#[cfg(not(target_os = "windows"))]
use std::os::unix::fs::MetadataExt; // metadata.size
#[cfg(target_os = "windows")]
use std::os::windows::fs::MetadataExt;
use std::{
    fs::{self, File},
    io::{BufWriter, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::Arc,
};

use crate::common::mmap::{self, Mmap};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::{Buf, BufMut};
use parking_lot::RwLock;
use regex::Regex;
use snafu::{Snafu, ensure};

use crate::series::{series_file::read_series_key, series_index::SeriesIndex};

const SERIES_SEGMENT_VERSION: u8 = 1;
const SERIES_SEGMENT_MAGIC: &str = "SSEG";
const SERIES_SEGMENT_HEADER_SIZE: i32 = 4 + 1; // magic + version

// Series entry constants.
const SERIES_ENTRY_FLATG_SIZE: u8 = 1;
pub(crate) const SERIES_ENTRY_HEADER_SIZE: u8 = 1 + 8; // flag + id

pub(crate) const SERIES_ENTRY_INSERT_FLAG: u8 = 0x01;
pub(crate) const SERIES_ENTRY_TOMBSTONE_FLAG: u8 = 0x02;

/// Error type for SeriesSegment.
#[derive(Debug, Snafu)]
pub enum Error {
    /// IO error.
    #[snafu(display("io error: {}", reason))]
    IOError { reason: std::io::Error },

    /// File is empty.
    #[snafu(display("the file to mmap is empty"))]
    FileIsEmpty,

    /// Invalid series segment.
    #[snafu(display("invalid series segment"))]
    InvalidSeriesSegment,

    /// Invalid series segment version.
    #[snafu(display("invalid series segment version"))]
    InvalidSeriesSegmentVersion,

    /// Invalid series segment filename.
    #[snafu(display("invalid series segment filename"))]
    InvalidSeriesSegmentFilename,

    /// Series segment not writable.
    #[snafu(display("series segment not writable"))]
    SeriesSegmentNotWritable,

    /// Invalid series entry.
    #[snafu(display("{}", reason))]
    InvalidSeriesEntry { reason: String },
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IOError { reason: err }
    }
}

/// Result type for SeriesSegment.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// SeriesSegment represents a log of series entries.
pub struct SeriesSegment {
    id: u16,
    path: Arc<PathBuf>,

    mmap: Option<Mmap>,
    bw: Option<BufWriter<File>>, // bufferred file handle

    size: u32, // current file size
}

/// Open memory maps the data file at the file's path.
pub fn open_series_segment(id: u16, path: impl Into<PathBuf>) -> Result<SeriesSegment> {
    let path = path.into();

    // check if file is empty
    let stat = fs::metadata(&path)?;
    ensure!(stat.size() > 0, FileIsEmptySnafu);

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)?;

    // memory map file data
    let mmap = unsafe {
        mmap::MmapOptions::new()
            .len(series_segment_size(id).try_into().unwrap())
            .map(&file)
            .expect("read-only memory map failed")
    };

    // reader header
    let hdr = read_series_segment_header(&mmap)?;
    ensure!(
        hdr.version == SERIES_SEGMENT_VERSION,
        InvalidSeriesSegmentVersionSnafu
    );

    // calculate segment data size if writing
    let mut size = SERIES_SEGMENT_HEADER_SIZE as u32;
    let data_len = mmap.as_ref().len();
    while size < data_len as u32 {
        let (flag, _, _, sz) = read_series_entry(&mmap[size as usize..]);
        if !is_valid_series_entry_flag(flag) {
            break;
        }
        size += sz as u32;
    }
    // seek to end of data
    file.seek(SeekFrom::Start(size as u64))?;

    Ok(SeriesSegment {
        id,
        path: Arc::new(path),

        mmap: Some(mmap),
        bw: Some(BufWriter::with_capacity(32 * 1024, file)),
        size,
    })
}

/// generates an empty segment at path.
pub(crate) fn create_series_segment(id: u16, path: impl Into<PathBuf>) -> Result<SeriesSegment> {
    // create segment in temp location
    let path = path.into();
    let mut tmp_file = path.clone();
    tmp_file.set_extension("initialing"); // "xxx.initialing"
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp_file)?;

        // write header to file and close
        let hdr = new_series_segment_header();
        hdr.write_to(&mut f)?;
        f.set_len(series_segment_size(id) as u64)?; // truncate file size
        f.flush()?;
    }

    // swap with target path
    fs::rename(&tmp_file, &path)?;

    // open segment at new location
    let segment = open_series_segment(id, &path)?;

    Ok(segment)
}

impl SeriesSegment {
    /// Returns the directory the log was initialized with.
    pub fn path(&self) -> PathBuf {
        self.path.to_path_buf()
    }

    /// Unmaps the segment or free the malloced memory.
    pub fn close(&mut self) -> Result<()> {
        self.close_for_write()?;
        self.mmap = None;

        Ok(())
    }

    // used after create_segment or close unmmaps the segment
    pub fn close_for_write(&mut self) -> Result<()> {
        self.bw.as_mut().unwrap().flush()?;
        if let Some(ref f) = self.bw {
            f.get_ref().sync_all()?;
        }
        self.bw = None;

        Ok(())
    }

    /// Returns the raw data.
    pub fn data(&self) -> &[u8] {
        self.mmap.as_ref().unwrap().as_ref()
    }

    /// Returns the id and segment was initialized with.
    pub fn id(&self) -> u16 {
        self.id
    }

    /// Returns the size of the data in the segment, this only populated once init_for_write is called.
    pub fn size(&self) -> i64 {
        self.size as i64
    }

    // Returns a byte slice starting at pos
    pub fn slice(&self, pos: u32) -> &[u8] {
        &self.mmap.as_ref().unwrap().as_ref()[pos as usize..]
    }

    /// Writes entry data into the segment, returns the offset of the beginning of the entry.
    // pub fn write_log_entry(&mut self, data: &[u8]) -> (i64, Option<Error>) {
    pub fn write_log_entry(&mut self, data: &[u8]) -> Result<i64> {
        // if !self.can_write(data) {
        //     return (0, Some(Error::SeriesSegmentNotWritable));
        // }

        // let offset = join_series_offset(self.id, self.size);
        // if let Err(_e) = self.bw.as_mut().unwrap().write_all(data) {
        //     return (0, Some(Error::SeriesSegmentNotWritable));
        // }
        // self.bw.as_mut().unwrap().flush().unwrap();

        // self.size += data.len() as u32;

        // (offset, None)

        ensure!(self.can_write(data), SeriesSegmentNotWritableSnafu);

        let offset = join_series_offset(self.id, self.size);
        if self.bw.as_mut().unwrap().write_all(data).is_err() {
            return SeriesSegmentNotWritableSnafu.fail();
        }

        self.size += data.len() as u32;

        Ok(offset)
    }

    pub fn can_write(&self, data: &[u8]) -> bool {
        self.bw.is_some() && (self.size + (data.len() as u32) <= series_segment_size(self.id))
    }

    pub fn flush(&mut self) -> Result<()> {
        self.bw.as_mut().unwrap().flush()?;

        Ok(())
    }

    /// Appends all the segments ids to a slice. Returns the new slice.
    pub fn append_series_ids(&self, a: &[u64]) -> Vec<u64> {
        let mut out = a.to_vec();
        let _ = self.for_each_entry::<_, Error>(
            |flag: u8, id: u64, _offset: i64, _key: &[u8]| -> Result<()> {
                if flag == SERIES_ENTRY_INSERT_FLAG {
                    out.push(id);
                }
                Ok(())
            },
        );

        out
    }

    /// Returns the highest series id in the segment.
    pub fn max_series_id(&self) -> u64 {
        let mut max: u64 = 0;
        let _ = self.for_each_entry(
            |flag: u8, id: u64, _offset: i64, _key: &[u8]| -> Result<()> {
                if flag == SERIES_ENTRY_INSERT_FLAG && id > max {
                    max = id;
                }
                Ok(())
            },
        );

        max
    }

    /// Executes fn for every entry in the segment.
    pub fn for_each_entry<F, E>(&self, mut f: F) -> std::result::Result<(), E>
    where
        F: FnMut(u8, u64, i64, &[u8]) -> Result<(), E>, // flag, id, offset, key
    {
        let mut pos = SERIES_SEGMENT_HEADER_SIZE as usize;
        let data = self.mmap.as_ref().unwrap().as_ref();

        while pos < data.len() {
            let (flag, id, key, sz) = read_series_entry(&data[pos..]);
            if !is_valid_series_entry_flag(flag) {
                break;
            }

            let offset = join_series_offset(self.id, pos as u32);
            // when entry type is SERIES_ENTRY_TOMBSTONE_FLAG, the key is None.
            if let Some(k) = key {
                f(flag, id, offset, k)?
            } else {
                match f(flag, id, offset, &[] as &[u8]) {
                    Err(e) => {
                        return Err(e);
                    }
                    _ => {}
                }
            }

            pos += sz as usize;
        }

        Ok(())
    }

    /// Write each entry to current segment.
    fn write_each_entry(&self, index: &SeriesIndex, dst: &mut SeriesSegment) -> Result<()> {
        let mut pos = SERIES_SEGMENT_HEADER_SIZE as usize;
        let data = self.mmap.as_ref().unwrap().as_ref();

        while pos < data.len() {
            let (flag, id, key, sz) = read_series_entry(&data[pos..]);
            if !is_valid_series_entry_flag(flag) {
                break;
            }

            let offset = join_series_offset(self.id, pos as u32);
            // when entry type is SERIES_ENTRY_TOMBSTONE_FLAG, the key is None.
            if let Some(k) = key {
                SeriesSegment::write_one_entry(index, dst, flag, id, offset, k)?
            } else {
                SeriesSegment::write_one_entry(index, dst, flag, id, offset, &[] as &[u8])?
            }

            pos += sz as usize;
        }

        Ok(())
    }

    fn write_one_entry(
        index: &SeriesIndex,
        dst: &mut SeriesSegment,
        flag: u8,
        id: u64,
        _offset: i64,
        key: &[u8],
    ) -> Result<()> {
        if index.is_deleted(id) {
            return Ok(()); // series id has been deleted from index
        } else if flag == SERIES_ENTRY_TOMBSTONE_FLAG {
            return InvalidSeriesEntrySnafu {
                reason: format!("[series id {}]: tombstone entry but exists in index", id),
            }
            .fail();
        }

        // copy entry over to new segment
        let buf = append_series_entry(flag, id, Some(key));
        dst.write_log_entry(buf.as_slice())?;

        Ok(())
    }

    /// Returns a copy of the segment. Excludes the write handler, if set.
    pub fn clone_segment(&self) -> Self {
        Self {
            id: self.id,
            path: Arc::new(self.path.to_path_buf()),

            mmap: self.mmap.clone(),
            // excludes the writer handler
            bw: None,

            size: self.size,
        }
    }

    /// Rewrites the segment to a new file and removes tombstoned entries.
    pub fn compact_to_path(&self, path: impl Into<PathBuf>, index: &SeriesIndex) -> Result<()> {
        let mut dst = create_series_segment(self.id, path)?;
        // Iterate through the segment and write any entries to a new segment that exist in the index.
        self.write_each_entry(index, &mut dst)?;

        // Close the segment and truncate it to its maximum size.
        let size = dst.size();
        dst.close()?;

        // Truncate file size after compact
        std::fs::OpenOptions::new()
            .write(true)
            .open(dst.path())?
            .set_len(size as u64)?;

        Ok(())
    }
}

/// Returns a copy of a slice of segments.
pub(crate) fn clone_series_segments(
    segments: &[Arc<RwLock<SeriesSegment>>],
) -> Vec<Arc<RwLock<SeriesSegment>>> {
    let mut cloned = vec![];
    for seg in segments {
        cloned.push(Arc::clone(seg));
    }

    cloned
}

/// Returns a segment by id.
fn find_segment(
    segments: &[Arc<RwLock<SeriesSegment>>],
    id: u16,
) -> Option<Arc<RwLock<SeriesSegment>>> {
    for s in segments {
        if s.read().id == id {
            return Some(Arc::clone(s));
        }
    }
    None
}

/// Returns a series key from an offset within a set of segments.
pub(crate) fn read_series_key_from_segments(
    segments: &[Arc<RwLock<SeriesSegment>>],
    offset: i64,
) -> Option<Vec<u8>> {
    let (segment_id, pos) = split_series_offset(offset);
    if let Some(segment) = find_segment(segments, segment_id) {
        let segment = segment.read();
        let buf = segment.slice(pos);

        assert!(!buf.is_empty());
        if let Ok((key, _r)) = read_series_key(buf) {
            // (key, reminder)
            return Some(key.to_vec());
        } else {
            return None;
        }
    }

    None
}

fn join_series_offset(id: u16, pos: u32) -> i64 {
    ((id as i64) << 32) | (pos as i64)
}

pub(crate) fn split_series_offset(offset: i64) -> (u16, u32) {
    (
        ((offset >> 32) & 0xFFFF) as u16,
        (offset & 0xFFFFFFFF) as u32,
    ) // (id, pos)
}

fn is_valid_series_segment_file_name(filename: &str) -> bool {
    Regex::new(r"^[0-9a-f]{4}$").unwrap().is_match(filename)
}

pub(crate) fn parse_series_segment_filename(filename: &str) -> Result<u16> {
    let id = if let Ok(id) = u16::from_str_radix(filename, 16) {
        id
    } else {
        return InvalidSeriesSegmentFilenameSnafu.fail();
    };

    Ok(id)
}

fn series_segment_size(id: u16) -> u32 {
    const MIN: u16 = 22; // 4M
    const MAX: u16 = 28; // 256M

    let mut shift = id + MIN;
    if shift >= MAX {
        shift = MAX;
    }

    1 << shift
}

// SeriesSegmentHeader represents the header of a series segment.
struct SeriesSegmentHeader {
    version: u8,
}

fn new_series_segment_header() -> SeriesSegmentHeader {
    SeriesSegmentHeader {
        version: SERIES_SEGMENT_VERSION,
    }
}

fn read_series_segment_header(data: &[u8]) -> Result<SeriesSegmentHeader> {
    // read magic number
    let mut magic: [u8; SERIES_SEGMENT_MAGIC.len()] = [0; SERIES_SEGMENT_MAGIC.len()];
    magic[..].copy_from_slice(&data[..SERIES_SEGMENT_MAGIC.len()]);
    ensure!(
        SERIES_SEGMENT_MAGIC.as_bytes().eq(&magic),
        InvalidSeriesSegmentSnafu
    );

    // read version
    let mut hdr = new_series_segment_header();
    hdr.version = (&data[SERIES_SEGMENT_MAGIC.len()..SERIES_SEGMENT_MAGIC.len() + 1])
        .read_u8()
        .unwrap();

    Ok(hdr)
}

impl SeriesSegmentHeader {
    // writes the header to file
    fn write_to(&self, b: &mut impl Write) -> Result<usize> {
        let mut buf = [0; SERIES_SEGMENT_MAGIC.len() + 1];
        (&mut buf[..]).put_slice(SERIES_SEGMENT_MAGIC.as_bytes());
        let _ = (&mut buf[SERIES_SEGMENT_MAGIC.len()..]).write_u8(self.version); // BigEndian
        let n = b.write(&buf)?;

        Ok(n)
    }
}

fn read_series_entry(data: &[u8]) -> (u8, u64, Option<&[u8]>, i64) {
    // if flag byte is zero then no more enries exist
    let (flag, data) = (data[0], &data[1..]);
    if !is_valid_series_entry_flag(flag) {
        return (0, 0, None, 1);
    }

    let (id, data) = ((&data[..8]).read_u64::<BigEndian>().unwrap(), &data[8..]);
    if flag == SERIES_ENTRY_INSERT_FLAG {
        if let Ok((key, _r)) = read_series_key(data) {
            return (
                flag,
                id,
                Some(key),
                (SERIES_ENTRY_HEADER_SIZE as usize + key.len()) as i64,
            );
        }
    }

    (flag, id, None, SERIES_ENTRY_HEADER_SIZE as i64)
}

pub(crate) fn append_series_entry(flag: u8, id: u64, key: Option<&[u8]>) -> Vec<u8> {
    let mut dst: Vec<u8> = if let Some(k) = key {
        vec![0; 9 + k.len()] // 1 + 8 + len(key)
    } else {
        vec![0; 9]
    };

    dst[0] = flag;
    let _ = (&mut dst[1..9]).write_u64::<BigEndian>(id);

    match flag {
        SERIES_ENTRY_INSERT_FLAG => {
            dst[9..9 + key.as_ref().unwrap().len()].copy_from_slice(key.as_ref().unwrap())
        }
        SERIES_ENTRY_TOMBSTONE_FLAG => {}
        _ => unreachable!("unreachable: invalid flag: {}", flag),
    };

    dst
}

fn is_valid_series_entry_flag(flag: u8) -> bool {
    matches!(flag, SERIES_ENTRY_INSERT_FLAG | SERIES_ENTRY_TOMBSTONE_FLAG)
}

impl Drop for SeriesSegment {
    fn drop(&mut self) {}
}

#[cfg(test)]
mod tests {
    use crate::{
        db::tests::must_temp_reefsdb_dir,
        series::{series_file::encode_series_key, series_segment::*},
    };

    #[test]
    fn test_series_segment() {
        let dir = must_temp_reefsdb_dir();

        // Create a new initial segment (4mb) and initialize for writing.
        let mut segment = create_series_segment(0, dir.path().join("0000")).unwrap();

        // Write initial entry.
        let key1 = encode_series_key("m0".as_bytes(), &crate::models::points::Tags::new());
        let offset = segment
            .write_log_entry(
                append_series_entry(SERIES_ENTRY_INSERT_FLAG, 1, Some(key1.as_slice())).as_slice(),
            )
            .unwrap();
        assert_eq!(offset, SERIES_SEGMENT_HEADER_SIZE as i64);

        // Write a large entry (3mb).
        let name: String = vec!["m"; 3 * (1 << 20)].into_iter().collect();
        let key2 = encode_series_key(name.as_bytes(), &crate::models::points::Tags::new());
        let _ = segment
            .write_log_entry(
                append_series_entry(SERIES_ENTRY_INSERT_FLAG, 2, Some(key2.as_slice())).as_slice(),
            )
            .unwrap();
        assert_eq!(offset, SERIES_SEGMENT_HEADER_SIZE as i64);

        // Write another entry that is too large for the remaining segment space.
        let name: String = vec!["n"; 3 * (1 << 20)].into_iter().collect();
        let key3 = encode_series_key(name.as_bytes(), &crate::models::points::Tags::new());
        let res = segment.write_log_entry(
            append_series_entry(SERIES_ENTRY_INSERT_FLAG, 2, Some(key3.as_slice())).as_slice(),
        );
        assert!(matches!(
            res.err().unwrap(),
            Error::SeriesSegmentNotWritable
        ));

        // We must flush buffered file, so we can access data in mmaped data.
        assert!(segment.flush().is_ok());

        // Vertify two entries exist.
        let mut n = 0;
        let _ = segment.for_each_entry::<_, Error>(
            |flag: u8, id: u64, _offset: i64, key: &[u8]| -> Result<()> {
                match n {
                    0 => {
                        if flag != SERIES_ENTRY_INSERT_FLAG || id != 1 || key1.ne(key) {
                            panic!("unexpected entry(0): {}, {}, {:?}", flag, id, key);
                        }
                    }
                    1 => {
                        if flag != SERIES_ENTRY_INSERT_FLAG || id != 2 || key2.ne(key) {
                            panic!("unexpected entry(1): {}, {}, {:?}", flag, id, key);
                        }
                    }
                    _default => {
                        panic!("too many entries");
                    }
                }
                n += 1;

                Ok(())
            },
        );

        assert_eq!(n, 2, "unexpected entry count: {}", n);
    }

    #[test]
    fn test_series_segment_append_series_ids() {
        let dir = must_temp_reefsdb_dir();

        let mut segment = create_series_segment(0, dir.path().join("0000")).unwrap();

        // Write entries.
        let _offset = segment.write_log_entry(
            append_series_entry(
                SERIES_ENTRY_INSERT_FLAG,
                10,
                Some(
                    encode_series_key("m0".as_bytes(), &crate::models::points::Tags::new())
                        .as_slice(),
                ),
            )
            .as_slice(),
        );

        let _offset = segment.write_log_entry(
            append_series_entry(
                SERIES_ENTRY_INSERT_FLAG,
                11,
                Some(
                    encode_series_key("m1".as_bytes(), &crate::models::points::Tags::new())
                        .as_slice(),
                ),
            )
            .as_slice(),
        );

        // We must flush buffered file, so we can access data in mmaped data.
        assert!(segment.flush().is_ok());

        // Collect series ids with existing set.
        let a = segment.append_series_ids(&[1, 2]);
        assert_eq!(a, vec![1, 2, 10, 11]);
    }

    #[test]
    fn test_series_segment_max_series_id() {
        let dir = must_temp_reefsdb_dir();

        let mut segment = create_series_segment(0, dir.path().join("0000")).unwrap();

        // Write entries.
        let _offset = segment
            .write_log_entry(
                append_series_entry(
                    SERIES_ENTRY_INSERT_FLAG,
                    10,
                    Some(
                        encode_series_key("m0".as_bytes(), &crate::models::points::Tags::new())
                            .as_slice(),
                    ),
                )
                .as_slice(),
            )
            .unwrap();

        let _offset = segment
            .write_log_entry(
                append_series_entry(
                    SERIES_ENTRY_INSERT_FLAG,
                    11,
                    Some(
                        encode_series_key("m1".as_bytes(), &crate::models::points::Tags::new())
                            .as_slice(),
                    ),
                )
                .as_slice(),
            )
            .unwrap();

        // We must flush buffered file, so we can access data in mmaped data.
        assert!(segment.flush().is_ok());

        // Verify maximun.
        assert_eq!(segment.max_series_id(), 11);
    }

    #[test]
    fn test_series_segment_header() {
        // Verify header initializes correctly.
        let hdr = new_series_segment_header();
        assert_eq!(hdr.version, SERIES_SEGMENT_VERSION);

        // Marshal/unmarshal.
        let mut buf = Vec::<u8>::new();
        assert!(hdr.write_to(&mut buf).is_ok());
        let other = read_series_segment_header(buf.as_slice()).unwrap();
        assert_eq!(hdr.version, other.version);
    }

    #[test]
    fn test_series_segment_partial_write() {
        let dir = must_temp_reefsdb_dir();

        // Create a new initial segment (4mb) and initialize for writing.
        let mut segment = create_series_segment(0, dir.path().join("0000")).unwrap();

        // Write entries.
        let _offset = segment
            .write_log_entry(
                append_series_entry(
                    SERIES_ENTRY_INSERT_FLAG,
                    1,
                    Some(
                        encode_series_key("A".as_bytes(), &crate::models::points::Tags::new())
                            .as_slice(),
                    ),
                )
                .as_slice(),
            )
            .unwrap();

        let _offset = segment
            .write_log_entry(
                append_series_entry(
                    SERIES_ENTRY_INSERT_FLAG,
                    2,
                    Some(
                        encode_series_key("B".as_bytes(), &crate::models::points::Tags::new())
                            .as_slice(),
                    ),
                )
                .as_slice(),
            )
            .unwrap();

        // We must flush buffered file, so we can access data in mmaped data.
        assert!(segment.flush().is_ok());

        let sz = segment.size();
        let entry_size = append_series_entry(
            SERIES_ENTRY_INSERT_FLAG,
            2,
            Some(encode_series_key("B".as_bytes(), &crate::models::points::Tags::new()).as_slice()),
        )
        .len();

        // Close segment.
        segment.close().unwrap();

        // Truncate at each point and reopen.
        let mut i = entry_size;
        while i > 0 {
            {
                let f = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(dir.path().join("0000"))
                    .unwrap();

                f.set_len(sz as u64 - (entry_size - 1) as u64).unwrap();
            }
            let mut segment = open_series_segment(0, dir.path().join("0000")).unwrap();
            assert!(segment.close().is_ok());

            i -= 1;
        }
    }

    #[test]
    fn test_join_series_offset() {
        let offset = join_series_offset(0x1234, 0x56789ABC);
        assert_eq!(offset, 0x123456789ABC, "unexpected offset: {}", offset);
    }

    #[test]
    fn test_split_series_offset() {
        let (segment_id, pos) = split_series_offset(0x123456789ABC);
        assert!(
            matches!((segment_id, pos), (0x1234, 0x56789ABC)),
            "unexpected segmentid/pos: {}/{}",
            segment_id,
            pos
        );
    }

    #[test]
    fn test_is_valid_series_segment_file_name() {
        assert!(!is_valid_series_segment_file_name(""), "expected invalid");
        assert!(
            !is_valid_series_segment_file_name("0ab"),
            "expected invalid"
        );
        assert!(is_valid_series_segment_file_name("192a"), "expected valid");
    }

    #[test]
    fn test_parse_series_segment_filename() {
        match parse_series_segment_filename("a90b") {
            Ok(v) => {
                assert_eq!(v, 0xA90B, "unexpected value: {}", v);
            }
            Err(e) => panic!("{}", e),
        }

        match parse_series_segment_filename("0001") {
            Ok(v) => {
                assert_eq!(v, 1, "unexpected value: {}", v);
            }
            Err(e) => panic!("{}", e),
        }

        if parse_series_segment_filename("invalid").is_ok() {
            panic!("expected error");
        }
    }

    #[test]
    fn test_series_segment_size() {
        const MB: u32 = 1 << 20;

        let sz = series_segment_size(0);
        assert_eq!(sz, 4 * MB, "expected segment size: {}", sz);

        let sz = series_segment_size(1);
        assert_eq!(sz, 8 * MB, "expected segment size: {}", sz);

        let sz = series_segment_size(2);
        assert_eq!(sz, 16 * MB, "expected segment size: {}", sz);

        let sz = series_segment_size(3);
        assert_eq!(sz, 32 * MB, "expected segment size: {}", sz);

        let sz = series_segment_size(4);
        assert_eq!(sz, 64 * MB, "expected segment size: {}", sz);

        let sz = series_segment_size(5);
        assert_eq!(sz, 128 * MB, "expected segment size: {}", sz);

        let sz = series_segment_size(6);
        assert_eq!(sz, 256 * MB, "expected segment size: {}", sz);

        let sz = series_segment_size(7);
        assert_eq!(sz, 256 * MB, "expected segment size: {}", sz);
    }

    #[test]
    fn test_series_entry() {
        let series_key = encode_series_key("m0".as_bytes(), &crate::models::points::Tags::new());
        let buf = append_series_entry(1, 2, Some(series_key.as_slice()));
        let (flag, id, key, sz) = read_series_entry(buf.as_slice());
        assert_eq!(flag, 1, "unexpected flag: {}", flag);
        assert_eq!(id, 2, "unexpected id: {}", id);
        assert!(key.is_some());
        let key = key.unwrap();
        assert_eq!(key, series_key.as_slice(), "unexpected key: {:?}", key);
        assert_eq!(
            sz,
            (SERIES_ENTRY_HEADER_SIZE + key.len() as u8) as i64,
            "unexpected size: {}",
            sz
        );
    }
}
