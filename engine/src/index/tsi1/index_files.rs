use std::{
    collections::{HashMap, VecDeque},
    io::{BufWriter, Write},
    ops::{Deref, DerefMut},
    os::unix::fs::MetadataExt,
    sync::Arc,
};

use crate::common::{
    hll::HLL,
    time::{self, Time},
    to_str, to_string,
};
use parking_lot::RwLock;
use snafu::{ResultExt, Snafu};
use tokio_util::sync::CancellationToken;

use super::{
    file_set::{File, MeasurementIter, TagKeyIter},
    index_file::{self, IndexFile, IndexFileTrailer},
    measurement_block::{self, MeasurementBlockWriter},
    tag_block::{self, TagBlockEncoder},
};
use crate::{
    index::{
        SeriesIDIter, SeriesIDIterator,
        tsi1::{
            MeasurementElement, MeasurementMergeIter, TagKeyElement, TagKeyMergeIter,
            TagValueElement, merge_measurement_iters, merge_tag_key_iters,
        },
    },
    series::{
        series_file::SeriesFile,
        series_set::{self, SeriesIDSet},
    },
};

/// Error type for LogFiles.
#[derive(Debug, Snafu)]
pub enum Error {
    /// IO error.
    #[snafu(display("io error: {}", reason))]
    IOError { reason: tokio::io::Error },

    /// Compaction Interrupted.
    #[snafu(display("compaction interrupted"))]
    CompactionInterrupted,

    /// Index file error.
    #[snafu(display("index file error: {}", source))]
    IndexFile { source: index_file::Error },

    /// Series set error.
    #[snafu(display("series set error: {}", source))]
    SeriesSet { source: series_set::Error },

    /// Measurement block error.
    #[snafu(display("measurement block error: {}", source))]
    MeasurementBlock { source: measurement_block::Error },

    /// Tag block error.
    #[snafu(display("measurement block error: {}", source))]
    TagBlock { source: tag_block::Error },

    /// HLL error.
    #[snafu(display("hll error: {}", source))]
    HLL { source: crate::common::hll::Error },
}

impl From<tokio::io::Error> for Error {
    fn from(err: tokio::io::Error) -> Error {
        Error::IOError { reason: err }
    }
}

/// Result type for LogFiles.
pub type Result<T, E = Error> = std::result::Result<T, E>;

// IndexFiles represents a layered set of index files.
pub struct IndexFiles(pub VecDeque<Arc<RwLock<IndexFile>>>);

impl IndexFiles {
    /// Returns the ids for all index files.
    pub fn ids(&self) -> Vec<i64> {
        let mut a = Vec::with_capacity(self.len());
        for f in &self.0 {
            a.push(f.read().id());
        }
        a
    }

    /// Adds a reference count to all files.
    pub fn retain(&self) {
        for f in &self.0 {
            f.read().retain();
        }
    }

    // Release removes a reference count from all files.
    pub fn release(&self) {
        for f in &self.0 {
            f.read().release();
        }
    }

    // Files returns p as a list of File objects.
    pub fn files(&self) -> VecDeque<File> {
        self.0
            .iter()
            .map(|f| File::IndexFile(Arc::clone(f)))
            .collect::<VecDeque<File>>()
    }

    // Rteurns (series_id_set, tombstone_series_id_set)
    fn build_series_id_sets(&self) -> Result<(SeriesIDSet, SeriesIDSet)> {
        if self.is_empty() {
            return Ok((SeriesIDSet::new(&[]), SeriesIDSet::new(&[])));
        }

        // Start with sets from last file.
        let series_id_set = self
            .back()
            .unwrap()
            .read()
            .series_id_set()
            .context(IndexFileSnafu)?;
        let tomestone_series_id_set = self
            .back()
            .unwrap()
            .read()
            .tombstone_series_id_set()
            .context(IndexFileSnafu)?;

        // Build sets in reverse order.
        // This assumes that bits in both sets are mutually exclusive.
        for f in self.iter().rev().skip(1) {
            let ss = f.read().series_id_set().context(IndexFileSnafu)?;

            let ts = f.read().tombstone_series_id_set().context(IndexFileSnafu)?;

            // Add tombstones and remove from old series existence set.
            series_id_set.diff(&ts);
            tomestone_series_id_set.merge(&[&ts]);
            // Add new series and remove from old series tombstone set.
            tomestone_series_id_set.diff(&ss);
            series_id_set.merge(&[&ss]);
        }

        Ok((series_id_set, tomestone_series_id_set))
    }

    /// Returns a sorted list of all measurement names for all files.
    pub fn measurement_names(&self) -> Option<Vec<Vec<u8>>> {
        if let Some(itr) = self.measurement_iter() {
            let mut names = vec![vec![]];
            for e in itr {
                // e: Box<dyn MeasurementElement>
                names.push(e.name().unwrap().to_vec());
            }
            names.sort();
            return Some(names);
        }

        None
    }

    /// Returns an iterator that merges measurements across all files.
    pub fn measurement_iter(&self) -> Option<MeasurementMergeIter> {
        let mut a = Vec::with_capacity(self.len());
        for f in &self.0 {
            let itr = f.read().measurement_iterator();
            a.push(MeasurementIter::BlockMeasurementIter(itr));
        }
        merge_measurement_iters(a)
    }

    /// Returns an iterator that merges tag keys across all files.
    pub fn tag_key_iterator(&self, name: &[u8]) -> Option<TagKeyMergeIter> {
        let mut a = Vec::with_capacity(self.len());
        for f in &self.0 {
            match f.read().tag_key_iterator(name) {
                Some(itr) => {
                    a.push(TagKeyIter::TagBlockKeyIter(itr));
                }
                _ => {
                    continue;
                }
            }
        }
        merge_tag_key_iters(a)
    }

    /// Returns an iterator that merges series across all files.
    pub fn measurement_series_id_iterator(&self, name: &[u8]) -> Option<SeriesIDIter> {
        let mut a = Vec::with_capacity(self.len());
        for f in &self.0 {
            match f.read().measurement_series_id_iterator(name) {
                Some(itr) => {
                    a.push(itr);
                }
                _ => {
                    continue;
                }
            }
        }
        crate::index::merge_series_id_iterators(a)
    }

    /// Returns an iterator that merges series across all files.
    pub fn tag_value_series_id_set(
        &self,
        name: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<SeriesIDSet> {
        let ss = SeriesIDSet::new(&[]);
        for f in &self.0 {
            let fss = f
                .read()
                .tag_value_series_id_set(name, key, value)
                .context(IndexFileSnafu)?;
            if let Some(fss) = fss {
                ss.merge(&[&fss]);
            }
        }
        Ok(ss)
    }

    /// Merges all index files and writes them to w.
    pub fn compact_to(
        &self,
        w: &mut impl Write,
        sfile: Arc<SeriesFile>,
        _m: u64,
        _k: u64,
        cancel: CancellationToken,
    ) -> (Result<()>, i64) {
        let mut t = IndexFileTrailer::default();
        let mut n = 0i64;
        // Check for cancellation.
        if cancel.is_cancelled() {
            return (Err(Error::CompactionInterrupted), n);
        }

        // Wrap writer in buffered I/O.
        let mut bw = BufWriter::new(w);

        // Setup context object to track shared data for this compaction.
        let mut info = IndexCompactInfo {
            cancel,
            tag_sets: HashMap::new(),
        };

        // Write magic number.
        match bw.write(index_file::FILE_SIGNATURE.as_bytes()) {
            Ok(sz) => n += sz as i64,
            Err(e) => return (Err(Error::IOError { reason: e }), n),
        }

        // Flush buffer before re-mapping.
        if let Err(e) = bw.flush() {
            return (Err(Error::IOError { reason: e }), n);
        }

        // Write tagset blocks in measurement order.
        if let Err(e) = self.write_tag_sets_to(&mut bw, &mut info, &mut n) {
            return (Err(e), n);
        }

        // Ensure block is word aligned.
        // if offset := n % 8; offset != 0 {
        // 	if err := writeTo(bw, make([]byte, 8-offset), &n); err != nil {
        // 		return n, err
        // 	}
        // }

        // Write measurement block.
        t.measurement_block.offset = n;
        if let Err(e) = self.write_measurement_block_to(&mut bw, &mut info, &mut n) {
            return (Err(e), n);
        }
        t.measurement_block.size = n - t.measurement_block.offset;

        // Build series sets.
        let (series_id_set, tombstone_series_id_set) = match self.build_series_id_sets() {
            Ok((ss, tss)) => (ss, tss),
            Err(e) => return (Err(e), n),
        };

        // Generate sketches from series sets.
        let mut sketch = HLL::default();
        series_id_set.for_each(|id| {
            let key = sfile.series_key(id);
            if !key.is_empty() {
                sketch.add(&key);
            }
        });

        let mut tsketch = HLL::default();
        tombstone_series_id_set.for_each(|id| {
            let key = sfile.series_key(id);
            if !key.is_empty() {
                tsketch.add(&key);
            }
        });

        // Write series set.
        t.series_id_set.offset = n;
        let nn = match series_id_set.write_to(&mut bw) {
            Ok(nn) => nn,
            Err(e) => return (Err(Error::SeriesSet { source: e }), n),
        };
        n += nn as i64;
        t.series_id_set.size = n - t.series_id_set.offset;

        // Write tombstone series set.
        t.tombstone_series_id_set.offset = n;
        let nn = match tombstone_series_id_set.write_to(&mut bw) {
            Ok(nn) => nn,
            Err(e) => return (Err(Error::SeriesSet { source: e }), n),
        };
        n += nn as i64;
        t.tombstone_series_id_set.size = n - t.tombstone_series_id_set.offset;

        // Write series sketches. TODO(edd): Implement WriterTo on HLL++.
        t.series_sketch.offset = n;
        let data = match sketch.marshal_binary() {
            Ok(d) => d,
            Err(e) => {
                return (Err(Error::HLL { source: e }), n);
            }
        };
        if let Err(e) = bw.write(&data) {
            return (Err(Error::IOError { reason: e }), n);
        }
        t.series_sketch.size = data.len() as i64;
        n += t.series_sketch.size;

        t.tombstone_series_sketch.offset = n;
        let data = match tsketch.marshal_binary() {
            Ok(d) => d,
            Err(e) => {
                return (Err(Error::HLL { source: e }), n);
            }
        };
        if let Err(e) = bw.write(&data) {
            return (Err(Error::IOError { reason: e }), n);
        }
        t.tombstone_series_sketch.size = data.len() as i64;
        n += t.tombstone_series_sketch.size;

        // Write trailer.
        match t.write_to(&mut bw) {
            Ok(nn) => n += nn as i64,
            Err(e) => {
                return (Err(Error::IndexFile { source: e }), n);
            }
        }

        // Flush file.
        if let Err(e) = bw.flush() {
            return (Err(Error::IOError { reason: e }), n);
        }

        (Ok(()), n)
    }

    fn write_tag_sets_to<W>(
        &self,
        w: &mut BufWriter<W>,
        info: &mut IndexCompactInfo,
        n: &mut i64,
    ) -> Result<()>
    where
        W: Write,
    {
        if let Some(mitr) = self.measurement_iter() {
            for m in mitr {
                self.write_tag_set_to(w, m.name().unwrap(), info, n)?;
            }
        }

        Ok(())
    }

    /// writes a single tagset to w and saves the tagset offset.
    fn write_tag_set_to<W>(
        &self,
        w: &mut BufWriter<W>,
        name: &[u8],
        info: &mut IndexCompactInfo,
        n: &mut i64,
    ) -> Result<()>
    where
        W: Write,
    {
        // Check for cancellation.
        if info.cancel.is_cancelled() {
            return Err(Error::CompactionInterrupted);
        }

        // Ensure block is word aligned.
        // if offset := (*n) % 8; offset != 0 {
        // 	if err := writeTo(w, make([]byte, 8-offset), n); err != nil {
        // 		return err
        // 	}
        // }

        let kitr = self.tag_key_iterator(name);
        if kitr.is_none() {
            return Ok(());
        }
        let kitr = kitr.unwrap();
        let mut enc = TagBlockEncoder::new(w);
        for ke in kitr {
            // Encode key.
            enc.encode_key(ke.key(), ke.deleted())
                .context(TagBlockSnafu)?;

            // Iterate over tag values.
            let vitr = ke.tag_value_iterator().unwrap();
            for ve in vitr {
                // Merge all series together.
                let ss = self.tag_value_series_id_set(name, ke.key(), ve.value())?;
                enc.encode_value(ve.value(), ve.deleted(), &ss)
                    .context(TagBlockSnafu)?;
            }
        }

        // Save tagset offset to measurement.
        let mut pos = IndexTagSetPos {
            offset: *n,
            size: 0,
        };

        // Flush data to writer.
        enc.close().context(TagBlockSnafu)?;
        *n += enc.n();

        // Save tagset size to measurement.
        pos.size = *n - pos.offset;
        info.tag_sets.insert(to_string!(name), pos);

        Ok(())
    }

    fn write_measurement_block_to<W>(
        &self,
        w: &mut BufWriter<W>,
        info: &mut IndexCompactInfo,
        n: &mut i64,
    ) -> Result<()>
    where
        W: Write,
    {
        let mut mw: MeasurementBlockWriter = MeasurementBlockWriter::new();

        // Check for cancellation.
        if info.cancel.is_cancelled() {
            return Err(Error::CompactionInterrupted);
        }

        // Add measurement data & compute sketches.
        if let Some(mitr) = self.measurement_iter() {
            let mut series_n = 0i64;
            for m in mitr {
                let name = m.name().unwrap();

                // Look-up series ids.
                let mut itr = self.measurement_series_id_iterator(name).unwrap();

                let mut series_ids = vec![];
                for e in itr.by_ref() {
                    if e.series_id == 0 {
                        break;
                    }
                    series_ids.push(e.series_id);

                    // Check for cancellation periodically.
                    series_n += 1;
                    if series_n % 1000 == 0 && info.cancel.is_cancelled() {
                        return Err(Error::CompactionInterrupted);
                    }
                }
                series_ids.sort();

                // Add measurement to writer.
                let pos = info.tag_sets.get(to_str!(&name)).unwrap();
                mw.add(name, m.deleted(), pos.offset, pos.size, &series_ids);

                let _ = itr.close();
            }
        }

        // Flush data to writer.
        let nn = mw.write_to(w).context(MeasurementBlockSnafu)?;
        *n += nn;

        Ok(())
    }

    /// Returns the max index file size and the total file size for all index files.
    pub fn stat(&self) -> Result<IndexFilesInfo> {
        let mut info = IndexFilesInfo {
            max_size: 0,
            size: 0,
            mod_time: Time::from_timestamp_nanos(0),
        };
        for f in &self.0 {
            let fm = std::fs::metadata(f.read().path());
            if fm.is_err() {
                let e = fm.err().unwrap();
                // ensure!(e.kind() != std::io::ErrorKind::NotFound, Err(e.into()));
                if e.kind() == std::io::ErrorKind::NotFound {
                    continue;
                } else {
                    return Err(e.into());
                }
            }
            let fm = fm?;
            if fm.size() > info.max_size {
                info.max_size = fm.size();
            }
            if chrono::DateTime::<chrono::Utc>::from(fm.modified().unwrap())
                > info.mod_time.date_time()
            {
                info.mod_time = Time::from_date_time(fm.modified().unwrap().into());
            }
            info.size += fm.size();
        }

        Ok(info)
    }
}

impl Deref for IndexFiles {
    type Target = VecDeque<Arc<RwLock<IndexFile>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for IndexFiles {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
pub struct IndexFilesInfo {
    max_size: u64,        // largest file size
    size: u64,            // total file size
    mod_time: time::Time, // last modified
}

// IndexCompactInfo is a context object used for tracking position information
// during the compaction of index files.
struct IndexCompactInfo {
    cancel: CancellationToken,
    // Tracks offset/size for each measurement's tagset.
    tag_sets: HashMap<String, IndexTagSetPos>,
}

// IndexTagSetPos stores the offset/size of tagsets.
struct IndexTagSetPos {
    offset: i64,
    size: i64,
}

#[cfg(any(test, feature = "benchmarking"))]
pub mod tests {
    use std::{
        collections::{HashMap, VecDeque},
        sync::Arc,
    };

    use bytes::Bytes;
    use parking_lot::RwLock;
    use tokio_util::sync::CancellationToken;

    use super::IndexFiles;
    use crate::{
        index::tsi1::index_file::{
            IndexFile,
            tests::{K, M, create_index_file},
        },
        series::series_file::tests::{Series, must_open_series_file},
    };

    #[test]
    fn test_index_files_write_to() {
        let sfile = must_open_series_file();
        let sfile = Arc::new(sfile.into());

        // Write first file.
        let f0 = create_index_file(
            &sfile,
            vec![
                Series {
                    name: "cpu".as_bytes().to_vec(),
                    tags: crate::models::points::Tags::from(HashMap::from([(
                        "region".as_bytes().to_vec(),
                        "east".as_bytes().to_vec(),
                    )])),
                    delete: false,
                },
                Series {
                    name: "cpu".as_bytes().to_vec(),
                    tags: crate::models::points::Tags::from(HashMap::from([(
                        "region".as_bytes().to_vec(),
                        "west".as_bytes().to_vec(),
                    )])),
                    delete: false,
                },
                Series {
                    name: "mem".as_bytes().to_vec(),
                    tags: crate::models::points::Tags::from(HashMap::from([(
                        "region".as_bytes().to_vec(),
                        "east".as_bytes().to_vec(),
                    )])),
                    delete: false,
                },
            ],
        );

        // Write second file.
        let f1 = create_index_file(
            &sfile,
            vec![
                Series {
                    name: "cpu".as_bytes().to_vec(),
                    tags: crate::models::points::Tags::from(HashMap::from([(
                        "region".as_bytes().to_vec(),
                        "west".as_bytes().to_vec(),
                    )])),
                    delete: false,
                },
                Series {
                    name: "disk".as_bytes().to_vec(),
                    tags: crate::models::points::Tags::from(HashMap::from([(
                        "region".as_bytes().to_vec(),
                        "east".as_bytes().to_vec(),
                    )])),
                    delete: false,
                },
            ],
        );

        // Compact the two together and write out to a buffer.
        let mut buf = vec![];
        let a = IndexFiles(VecDeque::from([
            Arc::new(RwLock::new(f0)),
            Arc::new(RwLock::new(f1)),
        ]));
        let cancel = CancellationToken::new();
        let (res, n) = a.compact_to(&mut buf, Arc::clone(&sfile), M, K, cancel);
        assert!(res.is_ok(), "compact failed: {:?}", res);
        assert_ne!(n, 0, "expected data writted");

        // Unmarshal buffer into a new index file.
        let mut f = IndexFile::new(sfile);
        assert!(f.unmarshal_binary(Some(Bytes::from(buf))).is_ok());

        // Verify data in compacted file.
        let e = f.tag_value_elem("cpu".as_bytes(), "region".as_bytes(), "west".as_bytes());
        assert!(e.is_some());
        let n = e.unwrap().series_n();
        assert_eq!(n, 1, "unexpected series count: {}", n);
    }
}
