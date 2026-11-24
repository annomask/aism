use std::{
    ffi::OsStr,
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, Write},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::{Buf, BufMut};
use flate2::{Compression, bufread::GzDecoder, write::GzEncoder};
use parking_lot::RwLock;
use snafu::{ResultExt, Snafu, ensure};

use super::{compact::COMPACTION_TEMP_EXTENSION, file_store::TombstoneStat};
pub const TOMBSTONE_FILE_EXTENSION: &str = "tombstone";
const HEADER_SIZE: u64 = 4;
const HEADER: u32 = 0x1504;

/// Error type for tsm reader.
#[derive(Debug, Snafu)]
pub enum Error {
    /// TSMClosed is returned when performing an operation against a closed TSM file.
    #[snafu(display("incompatible v4 version"))]
    IncompatibleVersion,

    /// IO error.
    #[snafu(display("tsm writer error: {reason}"))]
    IO { reason: std::io::Error },

    /// String error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },
}

/// Result type for tsm tombstone.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IO { reason: err }
    }
}

// Tombstoner records tombstones when entries are deleted.
pub struct Tombstoner {
    path: PathBuf,
    filter: Option<fn(&[u8], &[u8], &[u8]) -> bool>,
    inner: RwLock<Inner>,
}

#[derive(Default)]
struct Inner {
    // Tombstones that have been written but not flushed to disk yet.
    tombstones: Vec<Tombstone>,
    // cache of the stats for this tombstone
    tombstone_stats: TombstoneStat,
    // indicates that the stats may be out of sync with what is on disk and they
    // should be refreshed.
    stats_loaded: bool,

    tmp_filename: String,
    // These are references used for pending writes that have not been committed.  If
    // these are nil, then no pending writes are in progress.
    gz: Option<GzEncoder<BufWriter<File>>>,
    // pending_file:      Option<File>,  // gz contains pending file
    tmp: [u8; 8],
    last_applied_offset: u64,
}

impl Tombstoner {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            filter: Some(contains_key_filter),
            inner: RwLock::new(Inner::default()),
        }
    }

    pub fn with_filter(path: PathBuf, filter: fn(&[u8], &[u8], &[u8]) -> bool) -> Self {
        Self {
            path,
            filter: Some(filter),
            inner: RwLock::new(Inner::default()),
        }
    }

    /// Adds the all keys, across all timestamps, to the tombstone.
    pub fn add(&self, keys: &[&[u8]], min_key: &[u8], max_key: &[u8]) -> Result<()> {
        self.add_range(keys, min_key, max_key, i64::MIN, i64::MAX)
    }

    /// Adds all keys to the tombstone specifying only the data between min and max to be removed.
    pub fn add_range(
        &self,
        keys: &[&[u8]],
        min_key: &[u8],
        max_key: &[u8],
        min: i64,
        max: i64,
    ) -> Result<()> {
        let mut keys = keys;
        if let Some(filter) = self.filter {
            while !keys.is_empty() && !filter(keys[0], min_key, max_key) {
                keys = &keys[1..];
            }
        }

        if keys.is_empty() || (keys.len() == 1 && keys[0].is_empty()) {
            // &[&[]] is empty
            return Ok(());
        }

        let mut inner = self.inner.write();
        // If this TSMFile has not been written (mainly in tests), don't write a
        // tombstone because the keys will not be written when it's actually saved.
        if self.path.as_path().as_os_str().eq("") {
            return Ok(());
        }

        inner.stats_loaded = false;

        inner.prepare(self.path.as_path())?; // TODO: only support v4
        for k in keys {
            if !(self.filter.as_ref().unwrap())(k, min_key, max_key) {
                continue;
            }

            inner.write_tombstone(Tombstone {
                key: k.to_vec(),
                min,
                max,
            })?;
        }

        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        let mut inner = self.inner.write();

        if let Err(e) = inner.commit(&tombstone_path(self.path.as_path())) {
            // Reset our temp references and clean up.
            let _ = inner.rollback();
            return Err(e);
        }

        Ok(())
    }

    pub fn rollback(&self) -> Result<()> {
        let mut inner = self.inner.write();
        inner.rollback()
    }

    /// Removes all the tombstone files from disk.
    pub fn delete(&self) -> Result<()> {
        let mut inner = self.inner.write();
        std::fs::remove_file(tombstone_path(self.path.as_path()))?;

        inner.stats_loaded = false;
        inner.last_applied_offset = 0;

        Ok(())
    }

    /// Return true if there are any tombstone entries recorded.
    pub fn has_tombstones(&self) -> bool {
        let stats = self.tombstone_stats();
        if !stats.tombstone_exists {
            return false;
        }
        if stats.size > 0 {
            return true;
        }

        !self.inner.read().tombstones.is_empty()
    }

    /// Returns any tombstone files associated with Tombstoner's TSM file.
    pub fn tombstone_stats(&self) -> TombstoneStat {
        {
            let inner = self.inner.read();
            if inner.stats_loaded {
                return inner.tombstone_stats.clone();
            }
        }

        match std::fs::metadata(tombstone_path(self.path.as_path())) {
            Ok(stat) => {
                let mut inner = self.inner.write();
                inner.tombstone_stats = TombstoneStat {
                    tombstone_exists: true,
                    path: tombstone_path(self.path.as_path()).to_path_buf(),
                    last_modified: stat
                        .modified()
                        .unwrap()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as i64,
                    size: stat.size(),
                };
                inner.stats_loaded = true;
                inner.tombstone_stats.clone()
            }
            Err(e) => {
                let mut inner = self.inner.write();
                // The file doesn't exist so record that we tried to load it so
                // we don't continue to keep trying.  This is the common case.
                inner.stats_loaded = e.kind() == std::io::ErrorKind::NotFound;
                inner.tombstone_stats.tombstone_exists = false;
                inner.tombstone_stats.clone()
            }
        }
    }

    /// Calls fn for every Tombstone under the Tombstoner.
    // pub fn walk<F>(&self, f: F)
    // where
    //     F: FnMut(&[u8], &[u8]) -> bool,
    pub fn walk<Func>(&self, f: Func) -> Result<()>
    where
        Func: FnMut(&Tombstone), /* -> Result<()>*/
    {
        let mut inner = self.inner.write();

        if !tombstone_path(self.path.as_path()).exists() {
            return Ok(());
        }
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .open(tombstone_path(self.path.as_path()))?;
        let mut b = [0u8; 4];
        let _ = file.read(&mut b)?;

        file.seek(std::io::SeekFrom::Start(0))?;

        let header = (&b[..]).get_u32();
        ensure!(header == HEADER, IncompatibleVersionSnafu);

        inner.read_tombstone(file, f)
    }
}

impl Inner {
    fn prepare(&mut self, path: &Path) -> Result<()> {
        if self.gz.is_some() {
            return Ok(()); // exists pending file
        }

        let tmp_path = format!(
            "{}.{}",
            tombstone_path(path).to_str().unwrap(),
            COMPACTION_TEMP_EXTENSION
        );
        ensure!(
            !Path::new(&tmp_path).exists(),
            StringErrSnafu {
                reason: "the tombstone tmp file already exists".to_string(),
            }
        );
        let mut tsfile_exist = false;
        // Copy the existing v4 file if it exists
        match std::fs::OpenOptions::new()
            .read(true)
            // .write(true)
            // .create_new(true) // O_CREAT + O_EXCL
            .open(tombstone_path(path))  // open for read
        {
            Ok(mut f) => {
                // tombstone file exists
                tsfile_exist = true;

                let mut b = [0u8; 4];
                let n = f.read(&mut b)?;
                if n == 4 {
                    let header = (&b[..]).get_u32();
                    // There is an existing tombstone on disk and it's not a v3.  Just rewrite it as a v3
                    // version again.
                    ensure!(header == HEADER, IncompatibleVersionSnafu);

                    // // Seek back to the beginning we copy the header
                    // if let Err(e) = f.seek(std::io::SeekFrom::Start(0)) {
                    //     drop(tmp);
                    //     std::fs::remove_file(tmp_path);
                    //     return Err(Error::IO { reason: e });
                    // }

                    drop(f);
                    // Copy the while file
                    if let Err(e) = std::fs::copy(tombstone_path(path), tmp_path.as_str()) {
                        // drop(tmp);
                        let _ = std::fs::remove_file(tmp_path.as_str());
                        return Err(Error::IO { reason: e });
                    }
                }
            }
            Err(e) => {
                if e.kind() != std::io::ErrorKind::NotFound {
                    // tombstone file doesn't exist, but has other errors
                    return Err(Error::IO { reason: e });
                }
            }
        }

        // open tmp file or open
        let mut tmp = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&tmp_path)?;
        let _ = tmp.seek(std::io::SeekFrom::End(0));
        tmp.metadata()?.permissions().set_mode(0o666);
        self.tmp_filename = tmp_path;

        let mut b = [0u8; 8];
        let mut bw = BufWriter::with_capacity(64 * 1024, tmp);

        // Write the header only if the file is new
        if !tsfile_exist {
            // xxx.tombstone file not exist
            (&mut b[..4]).put_u32(HEADER);
            bw.write_all(&b[..4])?;
        }

        // Write the tombstones
        self.gz = Some(GzEncoder::new(bw, Compression::default()));

        Ok(())
    }

    fn write_tombstone(&mut self, ts: Tombstone) -> Result<()> {
        let dst = self.gz.as_mut().unwrap();

        (&mut self.tmp[..4]).put_u32(ts.key.len() as u32);
        dst.write_all(&self.tmp[..4])?;
        dst.write_all(ts.key.as_slice())?;

        (&mut self.tmp[..]).put_u64(ts.min as u64);
        dst.write_all(&self.tmp[..])?;

        (&mut self.tmp[..]).put_u64(ts.max as u64);
        dst.write_all(&self.tmp[..])?;

        Ok(())
    }

    fn commit(&mut self, tspath: &Path) -> Result<()> {
        // No pending writes
        if self.gz.is_none() {
            return Ok(());
        }

        // flush gzip encoder
        self.gz.as_mut().unwrap().flush()?;
        // flush bufwriter
        self.gz.as_mut().unwrap().get_mut().flush()?;
        // fsync file
        self.gz.as_mut().unwrap().get_mut().get_mut().sync_all()?;

        // .tombstone.tmp to .tombstone
        std::fs::rename(&self.tmp_filename, tspath)?;

        self.gz = None;

        Ok(())
    }

    pub fn rollback(&mut self) -> Result<()> {
        if self.gz.is_none() {
            return Ok(());
        }

        self.gz = None;
        std::fs::remove_file(&self.tmp_filename)?;

        Ok(())
    }

    /// Reads the fourth version of tombstone files that are capable
    /// of storing multiple v3 files appended together.
    fn read_tombstone<F>(&mut self, mut file: File, mut f: F) -> Result<()>
    where
        F: FnMut(&Tombstone), /* -> Result<()>*/
    {
        // Skip header, already checked earlier
        if self.last_applied_offset != 0 {
            file.seek(std::io::SeekFrom::Start(self.last_applied_offset))?;
        } else {
            file.seek(std::io::SeekFrom::Start(HEADER_SIZE))?;
        }

        let br = BufReader::with_capacity(64 * 1024, file);
        let mut gr = GzDecoder::new(br);

        let mut b = vec![0u8; 4096];
        loop {
            loop {
                match gr.read(&mut b[..4]) {
                    Ok(n) => {
                        if n == 0 {
                            // current read reach EOF
                            break Ok(());
                        }
                    }
                    Err(e) => {
                        if e.kind() == std::io::ErrorKind::UnexpectedEof {
                            break Ok(());
                        } else {
                            break Err(Error::IO { reason: e });
                        }
                    }
                }

                let key_len = (&b[0..4]).get_u32() as usize;
                if key_len + 16 > b.len() {
                    b = vec![0u8; key_len + 16];
                }

                if let Err(e) = gr.read_exact(&mut b[..key_len]) {
                    break Err(Error::IO { reason: e });
                }

                // Copy the key since b is re-used
                let key = b[..key_len].to_vec();

                if let Err(e) = gr.read_exact(&mut b[key_len..key_len + 8]) {
                    // min_buf
                    break Err(Error::IO { reason: e });
                }

                let min = (&b[key_len..key_len + 8]).get_u64() as i64; // min_buf
                if let Err(e) = gr.read_exact(&mut b[key_len + 8..key_len + 16]) {
                    // max_buf
                    break Err(Error::IO { reason: e });
                }

                let max = (&b[key_len + 8..key_len + 16]).get_u64() as i64; // max_buf

                let ts = Tombstone { key, min, max };
                f(&ts);
            }?;

            for t in &self.tombstones {
                f(t);
            }

            // reset gzip reader, check EOF and break loop
            gr = GzDecoder::new(gr.into_inner());
            if gr.header().is_none() {
                // the gzip reader reach EOF, nothing to read so break
                break;
            }
        } // outer loop

        // Save the position of tombstone file so we don't re-apply the same set again if there are
        // more deletes.
        let pos = gr.get_mut().stream_position()?;
        self.last_applied_offset = pos;

        Ok(())
    }
}

fn tombstone_path(path: &Path) -> PathBuf {
    if path.ends_with(TOMBSTONE_FILE_EXTENSION) {
        return path.to_path_buf();
    }

    // Filename is 0000001.tsm1, Strip off the tsm1
    let filename = path.file_stem();

    // Append the "tombstone" suffix to create a 0000001.tombstone file
    match path.parent() {
        Some(p) => {
            if let Some(filename) = filename {
                return p
                    .join(filename.to_str().unwrap().to_string() + "." + TOMBSTONE_FILE_EXTENSION);
            }
            p.join(".".to_string() + TOMBSTONE_FILE_EXTENSION)
        }
        None => {
            if let Some(filename) = filename {
                return PathBuf::from(
                    filename.to_str().unwrap().to_string() + "." + TOMBSTONE_FILE_EXTENSION,
                );
            }
            PathBuf::from(".".to_string() + TOMBSTONE_FILE_EXTENSION)
        }
    }
}

fn contains_key_filter(key: &[u8], min_key: &[u8], max_key: &[u8]) -> bool {
    key >= min_key && key <= max_key
}

#[derive(Default, Clone)]
pub struct Tombstone {
    // key is the tombstoned series key.
    pub key: Vec<u8>,

    // min and max are the min and max unix nanosecond time ranges of Key that are deleted.  If
    // the full range is deleted, both values are -1.
    pub min: i64,
    pub max: i64,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::common::to_str;

    use super::{Tombstone, Tombstoner};
    use crate::storage::tsm1::cache::tests::{must_temp_tsm_dir, must_temp_tsm_file};

    fn no_filter(key: &[u8], min_key: &[u8], max_key: &[u8]) -> bool {
        true
    }

    // FIXME: file size=44, but the golang file size=42
    #[test]
    fn test_tombstoner_add() {
        let dir = must_temp_tsm_dir();
        let (f, path) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut ts = Tombstoner::with_filter(path.clone(), no_filter);
        let mut entries = must_read_all(&ts);
        assert!(
            entries.is_empty(),
            "length mismatch: got {}, exp {}",
            entries.len(),
            0
        );

        let stats = ts.tombstone_stats();
        assert!(stats.tombstone_exists == false);

        // no_filter always always returns true
        ts.add(
            &["foo".as_bytes()],
            "min_key".as_bytes(),
            "max_key".as_bytes(),
        )
        .unwrap();

        ts.flush().unwrap();

        entries = must_read_all(&ts);
        let stats = ts.tombstone_stats();
        assert!(stats.tombstone_exists == true);
        assert!(stats.size != 0);
        assert!(stats.last_modified != 0);
        assert!(!stats.path.as_os_str().is_empty());

        assert_eq!(
            1,
            entries.len(),
            "length mismatch: exp {}, got {}",
            1,
            entries.len()
        );
        assert_eq!(
            "foo".as_bytes(),
            entries[0].key,
            "value mismatch: exp {}, got {}",
            "foo",
            to_str!(&entries[0].key),
        );

        // Use a new Tombstoner to verify values are persisted
        let ts = Tombstoner::new(path);
        let entries = must_read_all(&ts);
        assert_eq!(
            1,
            entries.len(),
            "length mismatch: exp {}, got {}",
            1,
            entries.len()
        );
        assert_eq!(
            "foo".as_bytes(),
            entries[0].key,
            "value mismatch: exp {}, got {}",
            "foo",
            to_str!(&entries[0].key),
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_tombstoner_add_large_key() {
        let dir = must_temp_tsm_dir();
        let (f, path) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut ts = Tombstoner::with_filter(path.clone(), no_filter);
        let mut entries = must_read_all(&ts);
        assert!(
            entries.is_empty(),
            "length mismatch: got {}, exp {}",
            entries.len(),
            0
        );

        let stats = ts.tombstone_stats();
        assert!(stats.tombstone_exists == false);

        let key = "a".repeat(4096);

        // no_filter always always returns true
        ts.add(
            &[key.as_bytes()],
            "min_key".as_bytes(),
            "max_key".as_bytes(),
        )
        .unwrap();

        ts.flush().unwrap();

        entries = must_read_all(&ts);
        let stats = ts.tombstone_stats();
        assert!(stats.tombstone_exists == true);
        assert!(stats.size != 0);
        assert!(stats.last_modified != 0);
        assert!(!stats.path.as_os_str().is_empty());

        assert_eq!(
            1,
            entries.len(),
            "length mismatch: exp {}, got {}",
            1,
            entries.len()
        );
        assert_eq!(
            key.as_bytes(),
            entries[0].key,
            "value mismatch: exp {}, got {}",
            &key,
            to_str!(&entries[0].key),
        );

        // Use a new Tombstoner to verify values are persisted
        let ts = Tombstoner::new(path);
        let entries = must_read_all(&ts);
        assert_eq!(
            1,
            entries.len(),
            "length mismatch: exp {}, got {}",
            1,
            entries.len()
        );
        assert_eq!(
            key.as_bytes(),
            entries[0].key,
            "value mismatch: exp {}, got {}",
            &key,
            to_str!(&entries[0].key),
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_tombstoner_add_multiple() {
        let dir = must_temp_tsm_dir();
        let (f, path) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut ts = Tombstoner::with_filter(path.clone(), no_filter);
        let mut entries = must_read_all(&ts);
        assert!(
            entries.is_empty(),
            "length mismatch: got {}, exp {}",
            entries.len(),
            0
        );

        let stats = ts.tombstone_stats();
        assert!(stats.tombstone_exists == false);

        // no_filter always always returns true
        ts.add(
            &["foo".as_bytes()],
            "min_key".as_bytes(),
            "max_key".as_bytes(),
        )
        .unwrap();

        ts.flush().unwrap();

        // no_filter always always returns true
        ts.add(
            &["bar".as_bytes()],
            "min_key".as_bytes(),
            "max_key".as_bytes(),
        )
        .unwrap();

        ts.flush().unwrap();

        entries = must_read_all(&ts);
        let stats = ts.tombstone_stats();
        assert!(stats.tombstone_exists == true);
        assert!(stats.size != 0);
        assert!(stats.last_modified != 0);
        assert!(!stats.path.as_os_str().is_empty());

        assert_eq!(
            2,
            entries.len(),
            "length mismatch: exp {}, got {}",
            2,
            entries.len()
        );

        assert_eq!(
            "foo".as_bytes(),
            entries[0].key,
            "value mismatch: exp {}, got {}",
            "foo",
            to_str!(&entries[0].key),
        );

        assert_eq!(
            "bar".as_bytes(),
            entries[1].key,
            "value mismatch: exp {}, got {}",
            "bar",
            to_str!(&entries[0].key),
        );

        // Use a new Tombstoner to verify values are persisted
        let ts = Tombstoner::new(path);
        let entries = must_read_all(&ts);
        assert_eq!(
            2,
            entries.len(),
            "length mismatch: exp {}, got {}",
            2,
            entries.len()
        );

        assert_eq!(
            "foo".as_bytes(),
            entries[0].key,
            "value mismatch: exp {}, got {}",
            "foo",
            to_str!(&entries[0].key),
        );

        assert_eq!(
            "bar".as_bytes(),
            entries[1].key,
            "value mismatch: exp {}, got {}",
            "bar",
            to_str!(&entries[0].key),
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_tombstoner_add_empty() {
        let dir = must_temp_tsm_dir();
        let (f, path) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut ts = Tombstoner::with_filter(path.clone(), no_filter);
        let entries = must_read_all(&ts);
        assert!(
            entries.is_empty(),
            "length mismatch: got {}, exp {}",
            entries.len(),
            0
        );

        let stats = ts.tombstone_stats();
        assert!(stats.tombstone_exists == false);

        // no_filter always always returns true
        ts.add(&[&[]], "min_key".as_bytes(), "max_key".as_bytes())
            .unwrap();

        ts.flush().unwrap();

        // Use a new Tombstoner to verify values are persisted
        let ts = Tombstoner::new(path);
        let entries = must_read_all(&ts);
        assert_eq!(
            0,
            entries.len(),
            "length mismatch: exp {}, got {}",
            0,
            entries.len()
        );
        let stats = ts.tombstone_stats();
        assert!(stats.tombstone_exists == false);

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_tombstoner_delete() {
        let dir = must_temp_tsm_dir();
        let (f, path) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut ts = Tombstoner::with_filter(path.clone(), no_filter);
        let mut entries = must_read_all(&ts);
        assert!(
            entries.is_empty(),
            "length mismatch: got {}, exp {}",
            entries.len(),
            0
        );

        // no_filter always always returns true
        ts.add(
            &["foo".as_bytes()],
            "min_key".as_bytes(),
            "max_key".as_bytes(),
        )
        .unwrap();

        ts.flush().unwrap();

        // Use a new Tombstoner to verify values are persisted
        let ts = Tombstoner::with_filter(path.clone(), no_filter);
        let entries = must_read_all(&ts);
        assert_eq!(
            1,
            entries.len(),
            "length mismatch: exp {}, got {}",
            1,
            entries.len()
        );
        assert_eq!(
            "foo".as_bytes(),
            entries[0].key,
            "value mismatch: exp {}, got {}",
            "foo",
            to_str!(&entries[0].key),
        );

        ts.delete().unwrap();

        let stats = ts.tombstone_stats();
        assert!(stats.tombstone_exists == false);

        let ts = Tombstoner::new(path);
        let entries = must_read_all(&ts);
        assert_eq!(
            0,
            entries.len(),
            "length mismatch: exp {}, got {}",
            0,
            entries.len()
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    fn must_read_all(t: &Tombstoner) -> Vec<Tombstone> {
        let mut tombstones = vec![];
        if let Err(e) = t.walk(|t: &Tombstone| {
            tombstones.push(Tombstone {
                min: t.min,
                max: t.max,
                key: t.key.clone(),
            });
        }) {
            panic!("{:?}", e);
        }

        tombstones
    }
}
