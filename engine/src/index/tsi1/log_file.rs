//! On-disk write-ahead log file for TSI.
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{self, BufWriter, Seek, SeekFrom, Write},
    ops::{Deref, DerefMut},
    os::unix::{fs::PermissionsExt, prelude::MetadataExt},
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use crate::common::{
    hll::HLL,
    mmap::{self, Mmap},
    time, to_str, to_string,
    wait_group::WaitGroup,
    xorfilter,
};
use crate::models::points::{self, make_key};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use integer_encoding::VarInt;
use parking_lot::RwLock;
use snafu::{ResultExt, Snafu, ensure};
use tokio_util::sync::CancellationToken;

use super::{
    measurement_block::{self, MeasurementBlockWriter},
    tag_block::{self, TagBlockEncoder},
};
use crate::{
    index::{
        self, SeriesIDIter, new_series_id_set_iter,
        tsi1::{self, decode_var, index_file},
    },
    series::{
        series_file,
        series_set::{self, SeriesIDSet},
    },
};

// Log entry flag constants.
const LOG_ENTRY_SERIES_TOMBSTONE_FLAG: u8 = 0x01;
const LOG_ENTRY_MEASUREMENT_TOMBSTONE_FLAG: u8 = 0x02;
const LOG_ENTRY_TAG_KEY_TOMBSTONE_FLAG: u8 = 0x03;
const LOG_ENTRY_TAG_VALUE_TOMBSTONE_FLAG: u8 = 0x04;

// DEFAULT_LOG_FILE_BUFFER_SIZE describes the size of the buffer that the LogFile's buffered
// writer uses. If the LogFile does not have an explicit buffer size set then
// this is the size of the buffer; it is equal to the default buffer size used
// by a BufWriter.
const DEFAULT_LOG_FILE_BUFFER_SIZE: usize = 8 * 1024;

// INDEX_FILE_BUFFER_SIZE is the buffer size used when compacting the LogFile down
// into a .tsi file.
const INDEX_FILE_BUFFER_SIZE: usize = 1 << 17; // 128K

/// Error type for LogFile.
#[derive(Debug, Snafu)]
pub enum Error {
    /// IO error.
    #[snafu(display("io error: {}", reason))]
    IOError { reason: io::Error },

    /// ChecksumMismatch.
    #[snafu(display("log entry checksum mismatch"))]
    LogEntryChecksumMismatch,

    /// ShortBuffer
    #[snafu(display("buffer too short"))]
    ShortBuffer,

    /// Unmarshal binary failed.
    #[snafu(display("tsdb: {}", source))]
    SeriesFileCreateSeriesFailed { source: series_file::Error },

    /// Encode TagBlock failed.
    #[snafu(display("tsdb: {}", source))]
    EncodeTagBlockFailed { source: tag_block::Error },

    /// Encode MeasurementBlock failed.
    #[snafu(display("tsdb: {}", source))]
    EncodeMeasurementBlockFailed { source: measurement_block::Error },

    /// Write series set failed.
    #[snafu(display("tsdb: {}", source))]
    WriteSeriesIDSetFailed { source: series_set::Error },

    /// Index file error.
    #[snafu(display("tsdb: {}", source))]
    IndexFile { source: index_file::Error },

    /// DecodeVarIntFailed.
    #[snafu(display("decode var integer failed"))]
    DecodeVarIntFailed,

    /// String error.
    #[snafu(display("tsi: {}", reason))]
    StringErr { reason: String },

    /// Compaction Interrupted.
    #[snafu(display("tsi1: compaction interrupted"))]
    CompactionInterrupted,
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IOError { reason: err }
    }
}

/// Result type for LogFile.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// LogFile represents an on-disk write-ahead log file.
#[derive(Default)]
pub struct LogFile {
    id: i64,       // file sequence identifier
    path: PathBuf, // Filepath to the log file

    mmap: Option<Mmap>, // mmap

    wg: WaitGroup,
    sfile: Arc<series_file::SeriesFile>, // series lookup

    pub buffer_size: usize, // The size of the buffer used by the buffered writer
    pub nosync: bool, // Disables buffer flushing and file syncing. Useful for offline tooling.

    inner: RwLock<Inner>,
}

struct Inner {
    // In-memory series existence sets.
    series_id_set: Arc<series_set::SeriesIDSet>,
    // In-memory series tombstone sets.
    tombstone_series_id_set: Arc<series_set::SeriesIDSet>,
    // In-memory index.
    mms: LogMeasurements, // measurement -> LogMeasurement

    size: i64,            // tracks current file size
    mod_time: time::Time, // tracks last time write occurred

    bw: Option<BufWriter<File>>, // buffered writer
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            series_id_set: Arc::new(series_set::SeriesIDSet::new(&[])),
            tombstone_series_id_set: Arc::new(series_set::SeriesIDSet::new(&[])),
            mms: LogMeasurements::default(),
            size: 0,
            mod_time: time::Time::from_timestamp_nanos(0),
            bw: None,
        }
    }
}

unsafe impl Sync for LogFile {}
unsafe impl Send for LogFile {}

impl LogFile {
    /// Returns a new instance of LogFile.
    pub fn new(path: impl Into<PathBuf>, sfile: Arc<series_file::SeriesFile>) -> Self {
        Self {
            id: 0i64,
            path: path.into(),

            mmap: None,

            wg: WaitGroup::new(),
            sfile,

            buffer_size: DEFAULT_LOG_FILE_BUFFER_SIZE,
            nosync: false,

            inner: RwLock::new(Inner::default()),
        }
    }

    /// Open reads the log from a file and validates all the checksums.
    pub fn open(&mut self, buffer_size: usize, nosync: bool) -> Result<()> {
        let inner = &mut *self.inner.write();

        let (id, _) = tsi1::partition::parse_file_name(self.path.to_str().unwrap());
        self.id = id;

        // open file for appending.
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.path)?;
        file.metadata()?.permissions().set_mode(0o666);

        let mut buffer_size = buffer_size;
        if buffer_size == 0 {
            buffer_size = DEFAULT_LOG_FILE_BUFFER_SIZE;
        }
        self.buffer_size = buffer_size;
        self.nosync = nosync;

        let fm = std::fs::metadata(&self.path)?;
        if fm.size() == 0 {
            let bw = Some(BufWriter::with_capacity(buffer_size, file));
            inner.bw = bw;
            // finish opening if file is empty.
            return Ok(());
        }

        // open a read-only memory map of the existing data.
        let mmap = unsafe {
            mmap::MmapOptions::new()
                .map(&file)
                .expect("read-only memory map failed")
        };
        self.mmap = Some(mmap);

        let bw = Some(BufWriter::with_capacity(self.buffer_size, file));
        inner.bw = bw;

        inner.size = fm.size() as i64;
        inner.mod_time = time::Time::from_timestamp_nanos(fm.mtime_nsec());

        // read log entries from mmap
        let mut buf = self.mmap.as_ref().unwrap().as_ref();
        let mut n = 0i64;
        while !buf.is_empty() {
            // read next entry, truncate partial writes
            let mut e = LogEntry::default();
            if let Err(e) = e.unmarshal_binary(buf) {
                match e {
                    Error::ShortBuffer | Error::LogEntryChecksumMismatch => break,
                    _ => return Err(e),
                }
            }

            // execute entry against in-memory index
            let esize = e.size;
            self.exec_entry(e, inner);

            // move buffer forward
            n += esize as i64;
            buf = &buf[esize as usize..];
        }

        // move to the end of the file
        inner
            .bw
            .as_mut()
            .unwrap()
            .get_mut()
            .seek(SeekFrom::Start(n as u64))?;

        inner.size = n;

        Ok(())
    }

    /// Shutdown the file handle amd mmap.
    pub fn close(&mut self) -> Result<()> {
        // wait until the file has no more references
        self.wg.wait();

        self.mmap = None;

        let inner = &mut *self.inner.write();
        if let Some(bw) = &mut inner.bw {
            bw.flush()?;
        }
        inner.bw = None;
        inner.mms = LogMeasurements::default();

        Ok(())
    }

    /// Estimates the memory footprint of this LogFile, in bytes.
    pub fn bytes(&self) -> i32 {
        unimplemented!()
    }

    /// Flushes buffered data to disk and then fsyncs the underlying file.
    /// If the LogFile has disabled flushing and syncing then flush_and_sync is a no-op.
    pub fn flush_and_sync(&self, w: Option<&mut BufWriter<File>>) -> Result<()> {
        if self.nosync {
            return Ok(());
        }

        if let Some(bw) = w {
            bw.flush()?;
            bw.get_ref().sync_all()?;
        }

        Ok(())
    }

    /// Returns the file sequence identifier.
    pub fn id(&self) -> i64 {
        self.id
    }

    /// Returns the file path.
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    /// Sets the log file's path.
    pub fn set_path(&mut self, path: &str) {
        self.path = PathBuf::from_str(path).unwrap();
    }

    /// Returns the log level of the file.
    pub fn level(&self) -> i64 {
        0i64
    }

    /// Returns the xor8 filter for the file.
    pub fn filter(&self) -> Option<xorfilter::Xor8Filter> {
        None
    }

    /// Adds a reference count to the file.
    pub fn retain(&self) {
        self.wg.add(1);
    }

    /// Removes a reference count from the file.
    pub fn release(&self) {
        self.wg.done();
    }

    pub(crate) fn wait_for_references(&self) {
        self.wg.wait();
    }

    /// Returns size and last modification time of the log file.
    pub fn stat(&self) -> (i64, time::Time) {
        let inner = self.inner.read();
        let size = inner.size;
        let mod_time = inner.mod_time;
        (size, mod_time)
    }

    pub fn series_file(&self) -> Arc<series_file::SeriesFile> {
        Arc::clone(&self.sfile)
    }

    /// Returns the series existence set.
    pub fn series_id_set(&self) -> Arc<series_set::SeriesIDSet> {
        let inner = self.inner.read();
        Arc::clone(&inner.series_id_set)
    }

    /// Returns the series tombstone set.
    pub fn tombstone_series_id_set(&self) -> Arc<series_set::SeriesIDSet> {
        let inner = self.inner.read();
        Arc::clone(&inner.tombstone_series_id_set)
    }

    /// Returns size of the file, in bytes.
    pub fn size(&self) -> i64 {
        let inner = self.inner.read();
        inner.size
    }

    /// Returns a measurement element.
    pub fn measurement(&self, name: &[u8]) -> Option<LogMeasurement> {
        let inner = self.inner.read();
        if let Some(mm) = inner.mms.get(to_str!(name)) {
            return Some(mm.clone());
        }
        None
    }

    pub fn measurement_has_series(&self, ss: &series_set::SeriesIDSet, name: &[u8]) -> bool {
        let inner = self.inner.read();
        if let Some(mm) = inner.mms.get(to_str!(name)) {
            return mm.has_series(ss);
        }
        false
    }

    pub fn measurement_names(mms: &LogMeasurements) -> Vec<String> {
        let mut a = Vec::with_capacity(mms.len());
        for k in mms.keys() {
            a.push(k.clone());
        }
        a.sort();
        a
    }

    /// Adds a tombstone for a measurement to the log file.
    pub fn delete_measurement(&self, name: &[u8]) -> Result<()> {
        let mut inner = self.inner.write();

        let mut e = LogEntry {
            flag: LOG_ENTRY_MEASUREMENT_TOMBSTONE_FLAG,
            mname: name.to_vec(), // measurement name
            ..Default::default()
        };
        self.append_entry(&mut inner, &mut e)?;
        self.exec_entry(e, &mut inner);
        // flush buffer and sync to disk.
        self.flush_and_sync(inner.bw.as_mut())
    }

    /// Returns a series iterator for a tag key.
    pub fn tag_key_series_id_iterator(&self, name: &[u8], key: &[u8]) -> Option<SeriesIDIter> {
        let inner = self.inner.read();
        let tk = {
            let mm = inner.mms.get(to_str!(name))?;
            mm.tag_set.get(to_str!(key))?
        };

        // combine iterators across all tag keys
        let mut itrs = Vec::with_capacity(tk.tag_values.len());
        for tv in tk.tag_values.values() {
            if tv.cardinality() == 0 {
                continue;
            }
            itrs.push(SeriesIDIter::SeriesIDSetIter(
                index::new_series_id_set_iter(tv.series_id_set()),
            ));
        }

        crate::index::merge_series_id_iterators(itrs)
    }

    /// Returns a value iterator for a measurement.
    pub fn tag_key_iterator(&self, name: &[u8]) -> Option<LogTagKeyIter> {
        let inner = self.inner.read();
        if let Some(mm) = inner.mms.get(to_str!(name)) {
            let mut a = Vec::<LogTagKey>::with_capacity(mm.tag_set.len());
            for (_, k) in mm.tag_set.iter() {
                a.push(k.clone());
            }
            return Some(new_log_tag_key_iterator(a));
        }

        None
    }

    /// Returns a tag key element.
    pub fn tag_key(
        &self,
        name: &[u8],
        key: &[u8],
    ) -> Option<impl tsi1::TagKeyElement + '_ + use<'_>> {
        let inner = self.inner.read();
        if let Some(mm) = inner.mms.get(to_str!(name)) {
            if let Some(tk) = mm.tag_set.get(to_str!(key)) {
                return Some(tk.clone());
            } else {
                return None;
            }
        }

        None
    }

    /// Returns a tag value element.
    pub fn tag_value(
        &self,
        name: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Option<impl tsi1::TagValueElement + use<>> {
        let inner = self.inner.read();
        if let Some(mm) = inner.mms.get(to_str!(name)) {
            if let Some(tk) = mm.tag_set.get(to_str!(key)) {
                if let Some(tv) = tk.tag_values.get(to_str!(value)) {
                    return Some(tv.clone());
                }
            } else {
                return None;
            }
        }
        None
    }

    /// Returns a value iterator for a tag key.
    pub fn tag_value_iter(&self, name: &[u8], key: &[u8]) -> Option<LogTagValueIter> {
        let inner = self.inner.read();
        if let Some(mm) = inner.mms.get(to_str!(name)) {
            if let Some(tk) = mm.tag_set.get(to_str!(key)) {
                return Some(tk.tag_value_iterator());
            } else {
                return None;
            }
        }

        None
    }

    /// Adds a tombstone for a tag key to the log file.
    pub fn delete_tag_key(&self, name: &[u8], key: &[u8]) -> Result<()> {
        let mut inner = self.inner.write();

        let mut e = LogEntry {
            flag: LOG_ENTRY_TAG_KEY_TOMBSTONE_FLAG,
            name: name.to_vec(),
            key: key.to_vec(),
            ..Default::default()
        };
        self.append_entry(&mut inner, &mut e)?;
        self.exec_entry(e, &mut inner);
        // flush buffer and sync to disk
        self.flush_and_sync(inner.bw.as_mut())
    }

    /// Returns a series iterator for a tag value.
    pub fn tag_value_series_id_set(
        &self,
        name: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Option<series_set::SeriesIDSet> {
        let inner = self.inner.read();
        if let Some(mm) = inner.mms.get(to_str!(name)) {
            if let Some(tk) = mm.tag_set.get(to_str!(key)) {
                if let Some(tv) = tk.tag_values.get(to_str!(value)) {
                    if tv.cardinality() == 0 {
                        return None;
                    }

                    return Some(tv.series_id_set());
                }
                return None;
            }
            return None;
        }

        None
    }

    /// Returns the total number of measurements.
    pub fn measurement_n(&self) -> u64 {
        self.inner.read().mms.len() as u64
    }

    /// Returns the total number of keys.
    pub fn tag_key_n(&self) -> u64 {
        let inner = self.inner.read();
        let mms = &(inner.mms);
        let mut n = 0usize;
        for (_, mm) in mms.iter() {
            n += mm.tag_set.len();
        }
        n as u64
    }

    /// Returns the total number of values.
    pub fn tag_value_n(&self) -> u64 {
        let inner = self.inner.read();
        let mms = &(inner.mms);
        let mut n = 0usize;
        for (_, mm) in mms.iter() {
            for (_, k) in mm.tag_set.iter() {
                n += k.tag_values.len()
            }
        }
        n as u64
    }

    ///Adds a tombstone for a tag value to the log file.
    pub fn delete_tag_value(&self, name: &[u8], key: &[u8], value: &[u8]) -> Result<()> {
        let mut inner = self.inner.write();

        let mut e = LogEntry {
            flag: LOG_ENTRY_TAG_VALUE_TOMBSTONE_FLAG,
            name: name.to_vec(),
            key: key.to_vec(),
            value: value.to_vec(),
            ..Default::default()
        };

        self.append_entry(&mut inner, &mut e)?;
        self.exec_entry(e, &mut inner);
        // flush buffer and sync to disk.
        self.flush_and_sync(inner.bw.as_mut())
    }

    /// Adds a list of series to the log file in bulk.
    pub fn add_series_list(
        &self,
        series_set: Arc<series_set::SeriesIDSet>,
        names: &[&[u8]],
        tags_slice: &[&crate::models::points::Tags],
    ) -> Result<Vec<u64>> {
        let mut series_ids = self
            .sfile
            .create_series_list_if_not_exists(names, tags_slice)
            .context(SeriesFileCreateSeriesFailedSnafu)?;

        let mut write_required = false;
        let mut entries = Vec::<LogEntry>::with_capacity(names.len());
        {
            let bitmap = series_set.bitmap.read();
            for (i, _name) in names.iter().enumerate() {
                if series_set.contains_no_lock(series_ids[i]) {
                    // We don't need to allocate anything for this series.
                    series_ids[i] = 0;
                    continue;
                }
                write_required = true;
                entries.push(LogEntry {
                    series_id: series_ids[i],
                    name: names[i].to_vec(),
                    tags: tags_slice[i].clone(),
                    cached: true,
                    batchidx: i as i32,
                    ..Default::default()
                });
            }
        }

        // Exit if all series already exist.
        if !write_required {
            return Ok(series_ids);
        }

        let mut inner = self.inner.write();
        let bitmap = series_set.bitmap.write();

        let mut i = 0usize;
        for mut entry in entries {
            // NB - this doesn't evaluate all series ids returned from series file.
            let series_id = entry.series_id;
            if series_set.contains_no_lock(series_id) {
                // We don't need to allocate anything for this series.
                series_ids[entry.batchidx as usize] = 0;
                i += 1usize;
                continue;
            }
            self.append_entry(&mut inner, &mut entry)?;
            self.exec_entry(entry, &mut inner);
            series_set.add_no_lock(series_id);
        }

        // Flush buffer and sync to disk
        self.flush_and_sync(inner.bw.as_mut())?;

        Ok(series_ids)
    }

    ///A dds a tombstone for a series id.
    pub fn delete_series_id(&self, id: u64) -> Result<()> {
        let mut inner = self.inner.write();

        let mut e = LogEntry {
            flag: LOG_ENTRY_SERIES_TOMBSTONE_FLAG,
            series_id: id,
            ..Default::default()
        };
        self.append_entry(&mut inner, &mut e)?;
        self.exec_entry(e, &mut inner);

        // flush buffer and sync to disk
        self.flush_and_sync(inner.bw.as_mut())
    }

    /// Returns the total number of series in the file.
    pub fn series_n(&self) -> u64 {
        let inner = self.inner.read();

        let mut n = 0u64;
        for (_, mm) in inner.mms.iter() {
            n += mm.cardinality() as u64;
        }
        n
    }

    /// Adds a log entry to the end of the file.
    fn append_entry(&self, f: &mut Inner, e: &mut LogEntry) -> Result<()> {
        // marshal entry to the local buffer
        let buf = append_log_entry(e);

        // save the size of the record
        e.size = buf.len() as i32;
        // write record to file
        if let Ok(n) = f.bw.as_mut().unwrap().write(buf.as_slice()) {
            // If an error is returned then no bytes in the buffer were written to this writer.

            // Update in-memory file size & modification time.
            f.size += n as i64;
            f.mod_time = time::Time::now();
        }

        Ok(())
    }

    /// Executes a log entry against the in-memory index.
    /// This is done after appending and on replay of the log.
    fn exec_entry(&self, e: LogEntry, inner: &mut Inner) {
        match e.flag {
            LOG_ENTRY_MEASUREMENT_TOMBSTONE_FLAG => self.exec_delete_measurement_entry(e, inner),
            LOG_ENTRY_TAG_KEY_TOMBSTONE_FLAG => self.exec_delete_tag_key_entry(e, inner),
            LOG_ENTRY_TAG_VALUE_TOMBSTONE_FLAG => self.exec_delete_tag_value_entry(e, inner),
            _ => self.exec_series_entry(e, inner),
        }
    }

    fn exec_delete_measurement_entry(&self, e: LogEntry, inner: &mut Inner) {
        let mm = self.create_measurement_if_not_exists(inner, &e.mname);
        mm.deleted = true;
        mm.tag_set = HashMap::<String, LogTagKey>::default();
        mm.series = HashSet::<u64>::new();
        mm.series_set = None;
    }

    fn exec_delete_tag_key_entry(&self, e: LogEntry, inner: &mut Inner) {
        let mm = self.create_measurement_if_not_exists(inner, &e.mname);
        let mut ts = mm.create_tag_set_if_not_exists(&e.key);

        ts.deleted = true;

        mm.tag_set.insert(to_string!(e.key), ts);
    }

    fn exec_delete_tag_value_entry(&self, e: LogEntry, inner: &mut Inner) {
        let mm = self.create_measurement_if_not_exists(inner, &e.mname);
        let mut ts = mm.create_tag_set_if_not_exists(&e.key);
        let mut tv = ts.create_tag_value_if_not_exists(&e.value);

        tv.deleted = true;
        ts.tag_values.insert(to_string!(e.value), tv);
        mm.tag_set.insert(to_string!(e.key), ts);
    }

    fn exec_series_entry(&self, e: LogEntry, inner: &mut Inner) {
        let series_key = match e.cached {
            true => series_file::encode_series_key(&e.name, &e.tags),
            false => self.sfile.series_key(e.series_id),
        };

        // Series keys can be removed if the series has been deleted from
        // the entire database and the server is restarted. This would cause
        // the log to replay its insert but the key cannot be found.
        //
        // https://github.com/influxdata/influxdb/issues/9444
        if series_key.is_empty() {
            return;
        }

        // let series_key = series_key.unwrap();

        // Check if deleted.
        let deleted = e.flag == LOG_ENTRY_SERIES_TOMBSTONE_FLAG;
        // Read key size.
        if let Ok((_, remainder)) = series_file::read_series_key_len(series_key.as_slice()) {
            // Read measurement name.
            let (name, remainder) = series_file::read_series_key_measurement(remainder);

            let mm = self.create_measurement_if_not_exists(inner, name);
            mm.deleted = false;
            if !deleted {
                mm.add_series_id(e.series_id);
            } else {
                mm.remove_series_id(e.series_id);
            }
            // Read tag count.
            if let Ok((tag_n, mut remainder)) = series_file::read_series_key_tag_n(remainder) {
                // Save tags.
                for _i in 0..tag_n {
                    let (k, v, remaining_data) = series_file::read_series_key_tag(remainder);
                    remainder = remaining_data;
                    let mut ts = mm.create_tag_set_if_not_exists(k);
                    let mut tv = ts.create_tag_value_if_not_exists(v);
                    // Add/remove a reference to the series on the tag value.
                    if !deleted {
                        tv.add_series_id(e.series_id);
                    } else {
                        tv.remove_series_id(e.series_id);
                    }
                    ts.tag_values.insert(to_string!(v), tv);

                    mm.tag_set.insert(to_string!(k), ts);
                }

                // Add/remove from appropriate series id sets.
                if !deleted {
                    inner.series_id_set.add(e.series_id);
                    inner.tombstone_series_id_set.remove(e.series_id);
                } else {
                    inner.series_id_set.remove(e.series_id);
                    inner.tombstone_series_id_set.add(e.series_id);
                }
            }
        }
    }

    // TODO:
    /// Returns an iterator over all series in the log file.
    // func (f *LogFile) SeriesIDIterator() tsdb.SeriesIDIterator {}
    pub fn series_id_iterator(&self) -> Option<SeriesIDIter> {
        let inner = self.inner.read();
        let ss = SeriesIDSet::new(&[]);
        let mut all_series_sets = Vec::<SeriesIDSet>::with_capacity(inner.mms.len());

        for (_, mm) in inner.mms.iter() {
            if mm.series_set.is_some() {
                all_series_sets.push(mm.series_set.as_ref().unwrap().clone());
                continue;
            }

            // measurement is not using seriesSet to store series IDs.
            mm.for_each(|series_id: u64| {
                ss.add_no_lock(series_id);
            })
        }

        // Fast merge all sereis sets.
        if !all_series_sets.is_empty() {
            ss.merge(
                all_series_sets
                    .iter()
                    .collect::<Vec<&SeriesIDSet>>()
                    .as_slice(),
            );
        }

        Some(SeriesIDIter::SeriesIDSetIter(new_series_id_set_iter(ss)))
    }

    // FIXME:  mutable borrow from immutable input
    /// Returns a measurement by name.
    fn create_measurement_if_not_exists<'a>(
        &self,
        inner: &'a mut Inner,
        name: &[u8],
    ) -> &'a mut LogMeasurement {
        if inner.mms.contains_key(to_str!(name)) {
            return inner.mms.get_mut(to_str!(name)).unwrap();
        }

        let mm: LogMeasurement = LogMeasurement {
            name: name.to_vec(),
            ..Default::default()
        };

        inner.mms.insert(to_str!(name).to_string(), mm);
        inner.mms.get_mut(to_str!(name)).unwrap()
    }

    /// Returns an iterator over all the measurements in the file.
    pub fn measurement_iterator(&self) -> LogMeasurementIter {
        let inner = self.inner.read();

        let mut itr = LogMeasurementIter::default();
        for (_, mm) in inner.mms.iter() {
            itr.mms.push(mm.clone());
        }

        itr.mms.sort_by(|a, b| a.name().cmp(b.name()));
        itr
    }

    /// Returns an iterator over all series for a measurement.
    pub fn measurement_series_id_iterator(&self, name: &[u8]) -> Option<SeriesIDIter> {
        let inner = self.inner.read();

        let mm = inner.mms.get(to_str!(name))?;
        if mm.cardinality() == 0 {
            return None;
        }

        Some(SeriesIDIter::SeriesIDSetIter(new_series_id_set_iter(
            mm.series_id_set(),
        )))
    }

    /// Compacts the log file and writes it to w.
    pub fn compact_to(
        &self,
        w: &mut impl Write,
        _m: u64,
        _k: u64,
        cancel: CancellationToken,
    ) -> Result<i64> {
        let inner = self.inner.read();
        // Check for cancellation.
        if cancel.is_cancelled() {
            return Err(Error::CompactionInterrupted);
        }

        // Wrap in bufferred writer with a buffer equivalent to the LogFile size.
        let mut bw = BufWriter::with_capacity(INDEX_FILE_BUFFER_SIZE, w);

        // Setup compaction offset tracking data.
        let mut t = index_file::IndexFileTrailer::default();
        let mut info = LogFileCompactInfo::new(cancel.clone());
        let mut n: usize = 0usize;
        // Write magic number.
        if let Ok(sz) = bw.write(index_file::FILE_SIGNATURE.as_bytes()) {
            n += sz;
        }

        // Retreve measurement names in order.
        let names = Self::measurement_names(&inner.mms);
        // Flush buffer & mmap series block.
        bw.flush()?;
        // Write tagset blocks in measurement order.
        self.write_tag_sets_to(&mut bw, &inner.mms, &names, &mut info, &mut n)?;
        // Write measurement block.
        t.measurement_block.offset = n as i64;
        self.write_measurement_block_to(&mut bw, &inner.mms, &names, &mut info, &mut n)?;
        t.measurement_block.size = n as i64 - t.measurement_block.offset;

        // Write series set.
        t.series_id_set.offset = n as i64;
        let nn = inner
            .series_id_set
            .write_to(&mut bw)
            .context(WriteSeriesIDSetFailedSnafu)?;
        n += nn;
        t.series_id_set.size = n as i64 - t.series_id_set.offset;

        // Write tombstone series set.
        t.tombstone_series_id_set.offset = n as i64;
        let nn = inner
            .tombstone_series_id_set
            .write_to(&mut bw)
            .context(WriteSeriesIDSetFailedSnafu)?;
        n += nn;
        t.tombstone_series_id_set.size = n as i64 - t.tombstone_series_id_set.offset;

        // Build series sketches.
        let (s_sketch, t_sketch) = self
            .series_sketches2(&inner.series_id_set, &inner.tombstone_series_id_set)
            .unwrap();

        // Write series sketches.
        t.series_sketch.offset = n as i64;
        // FIXME: 3271 -> 3275
        let data = s_sketch.marshal_binary().unwrap();
        let _ = bw.write(&data)?;
        t.series_sketch.size = data.len() as i64;
        n += t.series_sketch.size as usize;

        t.tombstone_series_sketch.offset = n as i64;
        let data = t_sketch.marshal_binary().unwrap();
        let _ = bw.write(&data)?;
        t.tombstone_series_sketch.size = data.len() as i64;
        n += t.tombstone_series_sketch.size as usize; // 58006 -> 58010

        // Write trailer.
        let nn = t.write_to(&mut bw).context(IndexFileSnafu)?;
        n += nn;
        // Flush buffer.
        bw.flush()?;

        Ok(n as i64)
    }

    fn write_tag_sets_to<W>(
        &self,
        w: &mut BufWriter<W>,
        mms: &LogMeasurements,
        names: &[String],
        info: &mut LogFileCompactInfo,
        n: &mut usize,
    ) -> Result<()>
    where
        W: Write,
    {
        for name in names {
            self.write_tag_set_to(w, mms, name.as_bytes(), info, n)?;
        }
        Ok(())
    }

    /// Writes a single tagset to w and saves the tagset offset.
    fn write_tag_set_to<W>(
        &self,
        w: &mut BufWriter<W>,
        mms: &LogMeasurements,
        name: &[u8],
        info: &mut LogFileCompactInfo,
        n: &mut usize,
    ) -> Result<()>
    where
        W: Write,
    {
        let mm = mms.get(to_str!(name)).unwrap();
        // Check for cancellation.
        if info.cancel.is_cancelled() {
            return Err(Error::CompactionInterrupted);
        }

        let mut enc = TagBlockEncoder::new(w);
        let mut value_n = 0;
        for k in &mm.keys() {
            let tag = &mm.tag_set[k];

            // Encode tag. Skip values if tag is deleted.
            enc.encode_key(&tag.name, tag.deleted)
                .context(EncodeTagBlockFailedSnafu)?;
            if tag.deleted {
                continue;
            }

            // Sort tag values.
            let mut values = Vec::<String>::with_capacity(tag.tag_values.len());
            for v in tag.tag_values.keys() {
                values.push(v.clone());
            }
            values.sort();

            // Add each value.
            for v in &values {
                let value = &tag.tag_values[v];
                enc.encode_value(&value.name, value.deleted, &value.series_id_set())
                    .context(EncodeTagBlockFailedSnafu)?;
                // Check for cancellation periodically.
                value_n += 1;
                if value_n % 1000 == 0 {
                    if info.cancel.is_cancelled() {
                        return Err(Error::CompactionInterrupted);
                    }
                }
            }
        }

        // Save tagset offset to measurement.
        let offset = *n;

        // Flush tag block.
        let res = enc.close().context(EncodeTagBlockFailedSnafu);
        *n += enc.n() as usize;
        res?;

        // Save tagset offset to measurement.
        let size = *n - offset;
        info.mms.insert(
            to_string!(name),
            LogFileMeasurementCompactInfo {
                offset: offset as i64,
                size: size as i64,
            },
        );

        Ok(())
    }

    fn write_measurement_block_to<W>(
        &self,
        w: &mut BufWriter<W>,
        mms: &LogMeasurements,
        names: &[String],
        info: &mut LogFileCompactInfo,
        n: &mut usize,
    ) -> Result<()>
    where
        W: Write,
    {
        let mut mw = MeasurementBlockWriter::new();

        // Check for cancallation.
        if info.cancel.is_cancelled() {
            return Err(Error::CompactionInterrupted);
        }

        // Add measurement data.
        for name in names {
            let mm = mms.get(name.as_str()).unwrap();
            let mm_info = info.mms.get(name.as_str());
            ensure!(
                mm_info.is_some(),
                StringErrSnafu {
                    reason: "measurement info not found".to_string(),
                }
            );
            let mm_info = mm_info.unwrap();
            mw.add(
                &mm.name,
                mm.deleted,
                mm_info.offset,
                mm_info.size,
                &mm.series_ids(),
            );
        }

        // Flush data to writer.
        let nn = mw
            .write_to(w)
            .context(EncodeMeasurementBlockFailedSnafu)
            .unwrap();
        *n += nn as usize;

        Ok(())
    }

    /// Returns sketches for existing and tombstoned measurement names.
    pub fn measurement_sketches(&self) -> Result<(HLL, HLL)> {
        let inner = self.inner.read();
        let (mut sketch, mut tsketch) = (HLL::default(), HLL::default());

        for (_, mm) in inner.mms.iter() {
            if mm.deleted {
                tsketch.add(&mm.name);
            } else {
                sketch.add(&mm.name);
            }
        }

        Ok((sketch, tsketch))
    }

    /// Returns sketches for existing and tombstoned series
    pub fn series_sketches(&self) -> Result<(HLL, HLL)> {
        let inner = self.inner.read();
        self.series_sketches2(&inner.series_id_set, &inner.tombstone_series_id_set)
    }

    /// Returns sketches for existing and tombstoned series
    pub fn series_sketches2(
        &self,
        series_set: &Arc<series_set::SeriesIDSet>,
        tombstone_series_set: &Arc<series_set::SeriesIDSet>,
    ) -> Result<(HLL, HLL)> {
        let mut sketch = HLL::default();
        series_set.for_each(|id| {
            let (name, keys) = self.sfile.series(id).unwrap();
            sketch.add(make_key(&name, &keys.unwrap()).as_slice());
        });

        let mut tsketch = HLL::default();
        tombstone_series_set.for_each(|id| {
            let (name, keys) = self.sfile.series(id).unwrap();
            tsketch.add(make_key(&name, &keys.unwrap()).as_slice());
        });

        Ok((sketch, tsketch))
    }
}

impl PartialEq for LogFile {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.path == other.path && self.mmap == other.mmap
    }
}

// LogFileCompactInfo is a context object to track compaction position info.
struct LogFileCompactInfo {
    cancel: CancellationToken,
    mms: HashMap<String, LogFileMeasurementCompactInfo>,
}

impl LogFileCompactInfo {
    pub fn new(cancel: CancellationToken) -> Self {
        Self {
            cancel,
            mms: HashMap::new(),
        }
    }
}

#[derive(Default)]
struct LogFileMeasurementCompactInfo {
    offset: i64,
    size: i64,
}

/// LogEntry represents a single log entry in the write-ahead log.
#[derive(Default)]
pub(crate) struct LogEntry {
    flag: u8,       // flag
    series_id: u64, // series id
    mname: Vec<u8>, // measurement name
    key: Vec<u8>,   // tag key
    value: Vec<u8>, // tag value
    check_sum: u32, // checksum of flag/name/tags.
    size: i32,      // total size of record, in bytes.

    cached: bool,       // Hint to LogFile that series data is already parsed
    name: Vec<u8>,      // series name, this is a cached copy of the parsed measurement name
    tags: points::Tags, // series tags, this is a cached copied of the parsed tags
    batchidx: i32,      // position of entry in batch.
}

impl LogEntry {
    /// Unmarshals data into e.
    pub fn unmarshal_binary(&mut self, mut data: &[u8]) -> Result<()> {
        let mut sz = 0u64;
        let mut n = 0usize;

        let orig = data;
        let start = data.len();

        // Parse flag data.
        ensure!(!data.is_empty(), ShortBufferSnafu);
        (self.flag, data) = (data[0], &data[1..]);

        // Parse series id.
        if let Some((id, nn)) = decode_var(data) {
            self.series_id = id;
            n = nn;
            data = &data[n..];
        } else {
            return Err(Error::DecodeVarIntFailed);
        }

        // Parse name length.
        if let Some((nsz, nn)) = decode_var(data) {
            sz = nsz;
            n = nn;
        } else {
            return Err(Error::DecodeVarIntFailed);
        }

        // Read name data.
        ensure!(data.len() >= n + sz as usize, ShortBufferSnafu);
        (self.mname, data) = (data[n..n + sz as usize].to_vec(), &data[n + sz as usize..]);

        // Parse key length.
        if let Some((ksz, nn)) = decode_var(data) {
            sz = ksz;
            n = nn;
        } else {
            return Err(Error::DecodeVarIntFailed);
        }

        // Read key data.
        ensure!(data.len() >= n + sz as usize, ShortBufferSnafu);
        (self.key, data) = (data[n..n + sz as usize].to_vec(), &data[n + sz as usize..]);

        // Parse value length.
        if let Some((vsz, nn)) = decode_var(data) {
            sz = vsz;
            n = nn;
        } else {
            return Err(Error::DecodeVarIntFailed);
        }

        // Read value data.
        ensure!(data.len() >= n + sz as usize, ShortBufferSnafu);
        (self.value, data) = (data[n..n + sz as usize].to_vec(), &data[n + sz as usize..]);

        // Compute checksum.
        let mut hasher = Hasher::new();
        hasher.update(&orig[..start - data.len()]);
        let chk = hasher.finalize();

        // Parse checksum.
        ensure!(data.len() >= 4, ShortBufferSnafu);
        (self.check_sum, data) = ((&data[..4]).read_u32::<BigEndian>().unwrap(), &data[4..]);

        // Verify checksum.
        ensure!(chk == self.check_sum, LogEntryChecksumMismatchSnafu);

        // Save length of elem.
        self.size = (start - data.len()) as i32;

        Ok(())
    }
}

/// Appends to dst and returns the new buffer.
/// This updates the checksum on the entry.
fn append_log_entry(e: &mut LogEntry) -> Vec<u8> {
    let mut dst: Vec<u8> = vec![];
    let mut buf = vec![0u8; i64::MAX.required_space()];

    // append flag
    dst.push(e.flag);

    let mut n = 0usize;
    // append series id
    n = e.series_id.encode_var(&mut buf);
    dst.extend_from_slice(&buf[..n]);

    // append measurement name
    n = (e.mname.len() as u64).encode_var(&mut buf);
    // dst[pos..pos + n].copy_from_slice(&buf[..n]);
    dst.extend_from_slice(&buf[..n]);
    dst.extend_from_slice(e.mname.as_slice());

    // append key
    n = (e.key.len() as u64).encode_var(&mut buf);
    // dst[pos..pos + n].copy_from_slice(&buf[..n]);
    dst.extend_from_slice(&buf[..n]);
    dst.extend_from_slice(e.key.as_slice());

    // append value
    n = (e.value.len() as u64).encode_var(&mut buf);
    // dst[pos..pos + n].copy_from_slice(&buf[..n]);
    dst.extend_from_slice(&buf[..n]);
    dst.extend_from_slice(e.value.as_slice());

    // calculate checksum
    let mut hasher = Hasher::new();
    hasher.update(&dst[..]);
    e.check_sum = hasher.finalize();

    // append checksum
    let _ = (&mut buf[..4]).write_u32::<BigEndian>(e.check_sum);
    dst.extend_from_slice(&buf[..4]);

    dst
}

// LogMeasurements represents a map of measurement names to measurements.
// type LogMeasurements map<string, Arc<LogMeasurement>>;
#[derive(Default, Debug)]
pub struct LogMeasurements(HashMap<String, LogMeasurement>);

impl LogMeasurements {
    /// Estimates the memory footprint of this logMeasurements, in bytes.
    pub fn bytes(&self) -> usize {
        let mut b = 0;
        for (k, v) in self.iter() {
            b += k.len();
            b += v.bytes();
        }
        // FIXME:
        b += std::mem::size_of_val(self);
        b
    }
}

impl Deref for LogMeasurements {
    type Target = HashMap<String, LogMeasurement>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for LogMeasurements {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Clone, Debug)]
pub struct LogMeasurement {
    name: Vec<u8>,
    tag_set: HashMap<String, LogTagKey>, // tag_key -> LogTagKey
    deleted: bool,
    series: HashSet<u64>,
    series_set: Option<series_set::SeriesIDSet>,
}

impl LogMeasurement {
    /// bytes estimates the memory footprint of this LogMeasurement, in bytes.
    pub fn bytes(&self) -> usize {
        let mut b = 0;
        b += self.name.len();
        for (k, v) in self.tag_set.iter() {
            b += k.len();
            b += v.bytes();
        }
        b += (self.cardinality() as usize) * 8;
        b += std::mem::size_of_val(self);
        b
    }

    fn add_series_id(&mut self, x: u64) {
        if let Some(series_set) = &self.series_set {
            series_set.add_no_lock(x);
            return;
        }

        self.series.insert(x);

        // If the map is getting too big it can be converted into a roaring seriesSet.
        if self.series.len() > 25 {
            let ss = SeriesIDSet::new(&[]);
            for id in &self.series {
                ss.add_no_lock(*id);
            }
            self.series_set = Some(ss);
            self.series = HashSet::<u64>::new();
        }
    }

    fn remove_series_id(&mut self, x: u64) {
        if let Some(ss) = &self.series_set {
            ss.remove_no_lock(x);
        }

        self.series.remove(&x);
    }

    fn cardinality(&self) -> i64 {
        if let Some(ss) = &self.series_set {
            return ss.cardinality() as i64;
        }

        self.series.len() as i64
    }

    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(u64),
    {
        if let Some(ss) = &self.series_set {
            ss.for_each_no_lock(f);
            return;
        }

        for id in self.series.iter() {
            f(*id);
        }
    }

    // returns a sorted set of SeriesIDs.
    fn series_ids(&self) -> Vec<u64> {
        let mut a = Vec::<u64>::with_capacity(self.cardinality() as usize);
        if let Some(ss) = &self.series_set {
            ss.for_each_no_lock(|id: u64| {
                a.push(id);
            });
            return a; // IDs are already sorted.
        }

        for id in self.series.iter() {
            a.push(*id);
        }
        a.sort();
        a
    }

    // returns a copy of the logMeasurement's seriesSet, or creates a new one.
    fn series_id_set(&self) -> series_set::SeriesIDSet {
        if let Some(ss) = &self.series_set {
            return ss.clone_no_lock();
        }

        let ss = SeriesIDSet::new(&[]);
        for id in self.series.iter() {
            ss.add_no_lock(*id);
        }
        ss
    }

    fn has_series(&self, ss: &series_set::SeriesIDSet) -> bool {
        if let Some(series_set) = &self.series_set {
            return series_set.intersects(ss);
        }

        for id in self.series.iter() {
            if ss.contains(*id) {
                return true;
            }
        }

        false
    }

    pub fn name(&self) -> &[u8] {
        &self.name
    }

    pub fn deleted(&self) -> bool {
        self.deleted
    }

    fn create_tag_set_if_not_exists(&self, key: &[u8]) -> LogTagKey {
        if let Some(ts) = self.tag_set.get(to_str!(key)) {
            return ts.clone();
        }
        LogTagKey {
            name: key.to_vec(),
            deleted: false,
            tag_values: HashMap::<String, LogTagValue>::default(),
        }
    }

    // returns a sorted list of tag keys.
    fn keys(&self) -> Vec<String> {
        let mut a = Vec::<String>::with_capacity(self.tag_set.len());
        for k in self.tag_set.keys() {
            a.push(k.clone());
        }
        a.sort();
        a
    }
}

impl tsi1::MeasurementElement for LogMeasurement {
    fn name(&self) -> Option<&[u8]> {
        if self.name.is_empty() {
            return None;
        }
        Some(&self.name)
    }

    fn deleted(&self) -> bool {
        self.deleted
    }
}

impl Default for LogMeasurement {
    fn default() -> Self {
        Self {
            name: vec![],
            tag_set: HashMap::<String, LogTagKey>::default(),
            deleted: false,
            series: HashSet::<u64>::new(),
            series_set: None,
        }
    }
}

/// Represents an iterator over a slice of measurements.
#[derive(Default, Clone)]
pub struct LogMeasurementIter {
    pub mms: Vec<LogMeasurement>,
}

impl Iterator for LogMeasurementIter {
    type Item = LogMeasurement;

    fn next(&mut self) -> Option<Self::Item> {
        if self.mms.is_empty() {
            return None;
        }

        Some(self.mms.remove(0))
    }
}

// impl Deref for LogMeasurementIter {
//     type Target = Vec<LogMeasurement>;

//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }

// impl DerefMut for LogMeasurementIter {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.0
//     }
// }

#[derive(Clone, Debug)]
pub struct LogTagKey {
    name: Vec<u8>,
    deleted: bool,
    tag_values: HashMap<String, LogTagValue>,
}

impl LogTagKey {
    // bytes estimates the memory footprint of this logTagKey, in bytes.
    pub fn bytes(&self) -> usize {
        let mut b = 0;
        b += self.name.len();
        for (k, v) in self.tag_values.iter() {
            b += k.len();
            b += v.bytes();
        }
        b += std::mem::size_of_val(self);
        b
    }

    pub fn tag_value_iterator(&self) -> LogTagValueIter {
        // The key owns this snapshot. Its source LogFile may have moved or
        // closed, and callers reading a live key already hold the file lock.
        new_log_tag_value_iterator(self.tag_values.values().cloned().collect())
    }

    fn create_tag_value_if_not_exists(&self, value: &[u8]) -> LogTagValue {
        if let Some(tv) = self.tag_values.get(to_str!(value)) {
            return tv.clone();
        }

        LogTagValue {
            name: value.to_vec(),
            deleted: false,
            series: HashSet::<u64>::new(),
            series_set: None,
        }
    }
}

// #[async_trait]
impl tsi1::TagKeyElement for LogTagKey {
    fn key(&self) -> &[u8] {
        self.name.as_slice()
    }

    fn deleted(&self) -> bool {
        self.deleted
    }

    // fn tag_value_iterator(&self) -> Option<Box<dyn TagValueIterator>> {
    //     // // FIXME: LogFile.tag_value_iterator has already hold LogFile.writer's read lock. now we needn't hold
    //     // // the read lock ?
    //     let mut a = Vec::<LogTagValue>::with_capacity(self.tag_values.len());
    //     {
    //         let f = unsafe { &*self.f };
    //         let _index_guard = f.inner.blocking_read();

    //         for v in self.tag_values.values() {
    //             a.push(v.clone());
    //         }
    //     }

    //     Some(Box::new(new_log_tag_value_iterator(a)))
    // }
}

#[derive(Clone, Default, Debug)]
pub struct LogTagValue {
    name: Vec<u8>,
    deleted: bool,
    series: HashSet<u64>,
    series_set: Option<series_set::SeriesIDSet>,
}

impl LogTagValue {
    // bytes estimates the memory footprint of this logTagValue, in bytes.
    fn bytes(&self) -> usize {
        let mut b = 0;
        b += self.name.len();
        b += std::mem::size_of_val(self);
        b += (self.cardinality() as usize) * 8;
        b
    }

    fn add_series_id(&mut self, x: u64) {
        if let Some(series_set) = &self.series_set {
            series_set.add_no_lock(x);
            return;
        }

        self.series.insert(x);

        // If the map is getting too big it can be converted into a roaring seriesSet.
        if self.series.len() > 25 {
            let ss = SeriesIDSet::new(&[]);
            for id in &self.series {
                ss.add_no_lock(*id);
            }
            self.series_set = Some(ss);
            self.series = HashSet::<u64>::new();
        }
    }

    fn remove_series_id(&mut self, x: u64) {
        if let Some(ss) = &self.series_set {
            ss.remove_no_lock(x);
        }

        self.series.remove(&x);
    }

    fn cardinality(&self) -> i64 {
        if let Some(ss) = &self.series_set {
            return ss.cardinality() as i64;
        }

        self.series.len() as i64
    }

    /// Returns a copy of the logMeasurement's seriesSet, or creates a new one
    fn series_id_set(&self) -> series_set::SeriesIDSet {
        if let Some(ss) = &self.series_set {
            return ss.clone_no_lock();
        }

        let ss = series_set::SeriesIDSet::new(&[]);
        for series_id in &self.series {
            ss.add_no_lock(*series_id);
        }
        ss
    }
}

impl tsi1::TagValueElement for LogTagValue {
    fn value(&self) -> &[u8] {
        self.name.as_slice()
    }

    fn deleted(&self) -> bool {
        self.deleted
    }
}

#[derive(Clone)]
pub struct LogTagKeyIter {
    // f: &'a LogFile, // TODO: remove ?
    a: Vec<LogTagKey>,
    index: usize,
}

impl Deref for LogTagKeyIter {
    type Target = Vec<LogTagKey>;

    fn deref(&self) -> &Self::Target {
        &self.a
    }
}

impl Iterator for LogTagKeyIter {
    type Item = LogTagKey;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.a.len() {
            return None;
        }

        let e = self.a[self.index].clone(); // FIXME: reutrn a reference
        self.index += 1;
        Some(e)
    }
}

/// Returns a new instance of LogTagKeyIter.
fn new_log_tag_key_iterator(mut a: Vec<LogTagKey>) -> LogTagKeyIter {
    a.sort_by(|a1, a2| a1.name.cmp(&a2.name));
    LogTagKeyIter { a, index: 0 }
}

/// LogTagValueIter represents an iterator over a slice of tag values.
#[derive(Clone)]
pub struct LogTagValueIter {
    a: Vec<LogTagValue>,
    index: usize,
    // phantom: std::marker::PhantomData<&'a ()>,
}

impl LogTagValueIter {
    pub fn new(mut a: Vec<LogTagValue>) -> Self {
        a.sort_by(|a, b| a.name.cmp(&b.name));
        LogTagValueIter {
            a,
            index: 0usize,
            // phantom: std::marker::PhantomData,
        }
    }

    // /// Returns an iterator over the LogTagValueIter.
    // fn iter(&self) -> impl Iterator<Item = &dyn tsi1::TagValueElement> {
    //     self.a.iter().map(|v| v as &dyn tsi1::TagValueElement)
    // }
}

impl Iterator for LogTagValueIter {
    type Item = LogTagValue;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.a.len() {
            // Get the next element and increment the index
            let item = self.a[self.index].clone(); // FIXME: return a reference
            self.index += 1;
            Some(item)
        } else {
            // No more elements to iterate
            None
        }
    }
}

// Returns a new instance of LogTagValueIter.
fn new_log_tag_value_iterator(mut a: Vec<LogTagValue>) -> LogTagValueIter {
    a.sort_by(|a1, a2| a1.name.cmp(&a2.name));

    LogTagValueIter {
        a,
        index: 0usize,
        // phantom: std::marker::PhantomData,
    }
}

/// generates a log filename for the given index.
pub fn format_log_file_name(id: i64) -> String {
    format!("L0-{:08}.{}", id, tsi1::partition::LOG_FILE_EXT) // .tsl
}

#[cfg(any(test, feature = "benchmarking"))]
pub mod tests {
    use std::{
        collections::{HashMap, HashSet},
        f32::consts::E,
        fmt::Debug,
        sync::Arc,
    };

    use crate::common::to_str;
    use chrono::format;
    // use tempfile;
    use rand::Rng;
    use snafu::ensure;
    use tokio_util::sync::CancellationToken;

    use crate::{
        index::tsi1::{MeasurementElement, log_file},
        series::{series_file, series_file::tests::*, series_set},
    };

    #[test]
    fn tag_value_snapshot_does_not_relock_source_file() {
        let sfile = Arc::new(must_open_series_file().into());
        let file = Arc::new(must_open_log_file(&sfile));
        let series_set = Arc::new(series_set::SeriesIDSet::new(&[]));
        file.add_series_list(series_set, &[b"cpu"], &[&crate::models::Tags::from(
            HashMap::from([(b"host".to_vec(), b"A".to_vec())]),
        )]).unwrap();
        let key = file.tag_key_iterator(b"cpu").unwrap().next().unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        let guard = file.inner.write();
        let worker = std::thread::spawn(move || {
            let values = key.tag_value_iterator().map(|v| v.name).collect::<Vec<_>>();
            sender.send(values).unwrap();
        });
        let result = receiver.recv_timeout(std::time::Duration::from_secs(2));
        drop(guard);
        worker.join().unwrap();
        assert_eq!(result.unwrap(), vec![b"A".to_vec()]);
        Arc::try_unwrap(file).ok().unwrap().close().unwrap();
    }

    // Ensure log file can append series.
    #[test]
    fn test_log_file_add_series_list() {
        let sfile = must_open_series_file();
        let sfile = Arc::new(sfile.into());
        let mut f = must_open_log_file(&sfile);

        let series_set = Arc::new(series_set::SeriesIDSet::new(&[]));

        // Add test data.
        let ids = f
            .add_series_list(
                Arc::clone(&series_set),
                &["cpu".as_bytes(), "mem".as_bytes()],
                &[
                    &crate::models::points::Tags::from(HashMap::from([(
                        "region".as_bytes().to_vec(),
                        "us-east".as_bytes().to_vec(),
                    )])),
                    &crate::models::points::Tags::from(HashMap::from([(
                        "host".as_bytes().to_vec(),
                        "serverA".as_bytes().to_vec(),
                    )])),
                ],
            )
            .unwrap();

        // Returned series ids should match those in the seriesSet.
        let other = series_set::SeriesIDSet::new(ids.as_slice());
        assert_eq!(
            other, *series_set,
            "got series ids {:?}, expected {:?}",
            other, series_set
        );

        // Add the same series again with a new one.
        let ids = f
            .add_series_list(
                Arc::clone(&series_set),
                &["cpu".as_bytes(), "mem".as_bytes()],
                &[
                    &crate::models::points::Tags::from(HashMap::from([(
                        "region".as_bytes().to_vec(),
                        "us-wast".as_bytes().to_vec(),
                    )])),
                    &crate::models::points::Tags::from(HashMap::from([(
                        "host".as_bytes().to_vec(),
                        "serverA".as_bytes().to_vec(),
                    )])),
                ],
            )
            .unwrap();

        assert_eq!(ids.len(), 2, "got {} series ids, expected {}", ids.len(), 2);
        assert_ne!(ids[0], 0, "series id was 0, expected it not to be");
        assert_eq!(ids[1], 0, "got series id {}, expected 0", ids[1]);

        // Add only the same series IDs.
        let ids = f
            .add_series_list(
                Arc::clone(&series_set),
                &["cpu".as_bytes(), "mem".as_bytes()],
                &[
                    &crate::models::points::Tags::from(HashMap::from([(
                        "region".as_bytes().to_vec(),
                        "us-wast".as_bytes().to_vec(),
                    )])),
                    &crate::models::points::Tags::from(HashMap::from([(
                        "host".as_bytes().to_vec(),
                        "serverA".as_bytes().to_vec(),
                    )])),
                ],
            )
            .unwrap();

        assert_eq!(
            ids,
            vec![0, 0],
            "got ids {:?}, expected {:?}",
            ids,
            vec![0, 0]
        );

        // Verify data.
        let mut itr = f.measurement_iterator();
        let e = itr.next();
        assert!(
            e.is_some() && e.as_ref().unwrap().name() == "cpu".as_bytes(),
            "unexpected measurement: {:?}",
            e
        );
        let e = itr.next();
        assert!(
            e.is_some() && e.as_ref().unwrap().name() == "mem".as_bytes(),
            "unexpected measurement: {:?}",
            e
        );
        let e = itr.next();
        assert!(e.is_none(), "expected eof, got: {:?}", e);

        // Reopen file and re-verify.
        f.reopen().unwrap();

        // Verify data.
        let mut itr = f.measurement_iterator();
        let e = itr.next();
        assert!(
            e.is_some() && e.as_ref().unwrap().name() == "cpu".as_bytes(),
            "unexpected measurement: {:?}",
            e
        );
        let e = itr.next();
        assert!(
            e.is_some() && e.as_ref().unwrap().name() == "mem".as_bytes(),
            "unexpected measurement: {:?}",
            e
        );
        let e = itr.next();
        assert!(e.is_none(), "expected eof, got: {:?}", e);
    }

    #[test]
    fn test_log_file_series_stored_in_order() {
        let sfile = must_open_series_file();
        let sfile = Arc::new(sfile.into());

        let mut f = must_open_log_file(&sfile);
        let series_set = Arc::new(series_set::SeriesIDSet::new(&[]));

        // Generate and add test data
        let mut tvm: HashSet<String> = HashSet::<String>::new();
        for _i in 0..100 {
            let tv = format!("server-{}", rand::thread_rng().gen_range(0..50));
            tvm.insert(tv.clone()); // Encourage adding duplicate series.

            assert!(
                f.add_series_list(
                    Arc::clone(&series_set),
                    &["mem".as_bytes(), "cpu".as_bytes()],
                    &[
                        &crate::models::points::Tags::from(HashMap::from([(
                            "host".as_bytes().to_vec(),
                            tv.as_bytes().to_vec(),
                        )])),
                        &crate::models::points::Tags::from(HashMap::from([(
                            "host".as_bytes().to_vec(),
                            tv.into_bytes(),
                        )])),
                    ],
                )
                .is_ok()
            );
        }

        // Sort the tag values so we know what order to expect.
        let mut tvs = vec![];
        for tv in tvm {
            tvs.push(tv);
        }
        tvs.sort();

        // Double the series values since we're adding them twice (two measurements)
        tvs.extend_from_slice(&tvs.clone());

        // When we pull the series out via an iterator they should be in order.
        let itr = f.series_id_iterator();
        assert!(itr.is_some());
        let mut itr = itr.unwrap();

        let mut prev_series_id = 0u64;
        for _i in 0..tvs.len() {
            let elem = itr.next();
            assert!(elem.is_some());
            let elem = elem.unwrap();
            assert_ne!(elem.series_id, 0, "got nil series");
            assert!(
                elem.series_id >= prev_series_id,
                "series out of order: {} !< {}",
                elem.series_id,
                prev_series_id
            );
            prev_series_id = elem.series_id;
        }
    }

    // Ensure log file can delete an existing measurement.
    #[test]
    fn test_log_file_delete_measurement() {
        let sfile = must_open_series_file();
        let sfile = Arc::new(sfile.into());

        let mut f = must_open_log_file(&sfile);
        let series_set = Arc::new(series_set::SeriesIDSet::new(&[]));

        // Add test data.
        assert!(
            f.add_series_list(
                Arc::clone(&series_set),
                &["mem".as_bytes(), "cpu".as_bytes(), "cpu".as_bytes()],
                &[
                    &crate::models::points::Tags::from(HashMap::from([(
                        "host".as_bytes().to_vec(),
                        "serverA".as_bytes().to_vec(),
                    )])),
                    &crate::models::points::Tags::from(HashMap::from([(
                        "region".as_bytes().to_vec(),
                        "us-east".as_bytes().to_vec(),
                    )])),
                    &crate::models::points::Tags::from(HashMap::from([(
                        "region".as_bytes().to_vec(),
                        "us-west".as_bytes().to_vec(),
                    )])),
                ],
            )
            .is_ok()
        );

        // Remove measurement.
        assert!(f.delete_measurement("cpu".as_bytes()).is_ok());

        // Verify data.
        let mut itr = f.measurement_iterator();
        // name:  cpu , deleted:  true
        // name:  mem , deleted:  false
        let e = itr.next();
        assert!(e.is_some());
        let e = e.unwrap();
        assert!(e.name() == "cpu".as_bytes() && e.deleted());

        let e = itr.next();
        assert!(e.is_some());
        let e = e.unwrap();
        assert!(e.name() == "mem".as_bytes() && !e.deleted());

        let e = itr.next();
        assert!(e.is_none());
    }

    // Ensure log file can recover correctly when data truncated.
    #[test]
    fn test_log_file_open_after_truncate() {
        let sfile = must_open_series_file();
        let sfile = Arc::new(sfile.into());

        let mut f = must_open_log_file(&sfile);
        let series_set = Arc::new(series_set::SeriesIDSet::new(&[]));

        // Add test data & close.
        assert!(
            f.add_series_list(
                Arc::clone(&series_set),
                &["cpu".as_bytes(), "mem".as_bytes()],
                &[
                    &crate::models::points::Tags::default(),
                    &crate::models::points::Tags::default(),
                ],
            )
            .is_ok()
        );
        assert!(f.close().is_ok());

        // Truncate data & reopen.
        {
            let fm = std::fs::metadata(f.path()).unwrap();
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(f.path())
                .unwrap();
            file.set_len(fm.len() - 1).unwrap();
        }
        assert!(f.open(0, false).is_ok());

        // Verify data.
        {
            let mut itr = f.series_id_iterator().unwrap();
            let e = itr.next();
            assert!(e.is_some());
            let e = e.unwrap();
            let (name, tags) = f.series_file().series(e.series_id).unwrap();
            let (name, tags) = (to_str!(&name), tags.unwrap());
            assert_eq!(
                name,
                "cpu",
                "unexpected series: {},{}",
                name,
                tags.to_string()
            );

            let e = itr.next();
            assert!(e.is_none(), "expected eof, got: {:?}", e);

            // Add more data & reopen.
            assert!(
                f.add_series_list(
                    Arc::clone(&series_set),
                    &["disk".as_bytes()],
                    &[&crate::models::points::Tags::default(),],
                )
                .is_ok()
            );
        }
        assert!(f.reopen().is_ok());

        // Verify new data.
        let mut names = vec![];
        let mut itr = f.series_id_iterator().unwrap();
        let e = itr.next();
        assert!(e.is_some());
        let e = e.unwrap();
        let (name, tags) = f.series_file().series(e.series_id).unwrap();
        let (name, tags) = (name, tags.unwrap());
        names.push(name);

        let e = itr.next();
        assert!(e.is_some());
        let e = e.unwrap();
        let (name, tags) = f.series_file().series(e.series_id).unwrap();
        let tags = tags.unwrap();
        names.push(name);

        // Note: log_file use HashMap to save names, so when iter all names in log file, the order is not guaranteed
        names.sort();
        assert_eq!(
            names,
            vec!["cpu".as_bytes().to_vec(), "disk".as_bytes().to_vec()]
        );

        let e = itr.next();
        assert!(e.is_none(), "expected eof, got: {:?}", e);
    }

    // Ensure log file can recover correctly when checksum mismatch.
    #[test]
    fn test_log_file_open_checksum_mismatch() {
        let sfile = must_open_series_file();
        let sfile = Arc::new(sfile.into());

        let mut f = must_open_log_file(&sfile);
        let series_set = Arc::new(series_set::SeriesIDSet::new(&[]));

        // Add test data & close.
        assert!(
            f.add_series_list(
                Arc::clone(&series_set),
                &["cpu".as_bytes(), "mem".as_bytes()],
                &[
                    &crate::models::points::Tags::default(),
                    &crate::models::points::Tags::default(),
                ],
            )
            .is_ok()
        );
        assert!(f.close().is_ok());

        // Corrupt last entry.
        let _file = std::fs::OpenOptions::new()
            .write(true)
            .open(f.path())
            .unwrap();
        let mut buf = std::fs::read(f.path()).unwrap();
        let buf_len = buf.len();
        buf[buf_len - 1] = 0u8;

        // Overwrite file with corrupt entry and reopen.
        std::fs::write(f.path(), buf.as_slice()).unwrap();
        assert!(f.open(0, false).is_ok());

        // Verify data.
        let mut itr = f.series_id_iterator().unwrap();
        let e = itr.next();
        assert!(e.is_some());
        let e = e.unwrap();
        let (name, tags) = f.series_file().series(e.series_id).unwrap();
        let (name, tags) = (to_str!(&name), tags.unwrap());
        assert_eq!(
            name,
            "cpu",
            "unexpected series: {},{}",
            name,
            tags.to_string()
        );

        let e = itr.next();
        assert!(e.is_none(), "expected eof, got: {:?}", e);
    }

    #[test]
    fn test_log_file_measurement_has_series() {
        let sfile = must_open_series_file();
        let sfile = Arc::new(sfile.into());

        let mut f = must_open_log_file(&sfile);

        let (measurement_n, series_value_n, series_key_n) = (3usize, 2usize, 5usize);
        let tag_value_n = series_value_n.pow(series_key_n as u32);

        let series_set = Arc::new(series_set::SeriesIDSet::new(&[])); //  all series in all measurements
        let mut series_ids = vec![]; // all series ids in measurement0

        // add series to all measurements
        for i in 0..measurement_n {
            let name = format!("measurement{}", i);

            let mut names = Vec::<String>::with_capacity(tag_value_n);
            let mut tags = Vec::<crate::models::points::Tags>::with_capacity(tag_value_n);
            for j in 0..tag_value_n {
                let mut tag = crate::models::points::Tags::default();

                for k in 0..series_key_n {
                    let key = format!("key{}", k);
                    let value = format!(
                        "value{}",
                        j / (series_value_n.pow(k as u32)) % series_value_n
                    );
                    tag.push(crate::models::points::Tag::new(
                        key.into_bytes(),
                        value.into_bytes(),
                    ));
                }

                names.push(name.clone()); // names[j] = name;
                tags.push(tag); // tags[j] = tag;
            }

            let tags_slice_of_refs: Vec<&crate::models::points::Tags> = tags.iter().collect();
            let tags = tags_slice_of_refs.as_slice();
            let names: Vec<&[u8]> = names.iter().map(|v| v.as_bytes()).collect();
            let ids = f
                .add_series_list(Arc::clone(&series_set), &names, tags)
                .unwrap();

            if i == 0 {
                series_ids = ids;
            }
        }

        // remove series from measurement 0
        let name = "measurement0";
        for i in 0..tag_value_n {
            // measurement0 has series before last one removed
            assert!(f.measurement_has_series(&*series_set, name.as_bytes()));
            assert!(f.delete_series_id(series_ids[i]).is_ok());
            series_set.remove(series_ids[i]);
        }

        // measurement0 has none series when last one removed
        assert!(!f.measurement_has_series(&*series_set, name.as_bytes()));
    }

    // #[test]
    // fn test_log_file_compact_to() {
    //     let sfile = must_open_series_file();
    //     let sfile = Arc::new(sfile.into());

    //     let mut f = must_open_log_file(&sfile);

    //     let series_set = Arc::new(series_set::SeriesIDSet::new(&[])); //  all series in all measurements

    //     let series_n = 1000;
    //     // Initialize log file with series data.
    //     for i in 0..series_n {
    //         let _ = f
    //             .add_series_list(Arc::clone(&series_set), &["cpu".as_bytes()], &[
    //                 &crate::models::points::Tags::from(HashMap::from([
    //                     ("host".to_owned(), format!("server-{}", i)),
    //                     ("location".to_owned(), "us-west".to_owned()),
    //                 ])),
    //             ]).await
    //             .unwrap();
    //     }

    //     let (m, k) = (8143, 6); // bloom::estimate(series_n as u64, 0.02)

    //     let mut buf = vec![];
    //     let n = f
    //         .compact_to(&mut buf, m, k, CancellationToken::new())
    //         .await
    //         .unwrap();
    //     // FIXME: add 1000 series 后，这里是 58092b，且 series_id 会超过 1000
    //     assert_eq!(n, 58088); // 10 series is 1086b, 1000 series is 58088b
    //     assert_eq!(buf.len(), 58088);
    // }

    impl Debug for dyn MeasurementElement {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "name: {:?}, deleted: {}", self.name(), self.deleted())
        }
    }

    // //////////// log file test util functions //////////////
    use std::ops::{Deref, DerefMut};

    use crate::series::series_file::tests::Series;
    pub struct LogFile(log_file::LogFile);

    impl LogFile {
        /// Closes the log file and removes it from disk.
        pub fn close(&mut self) -> log_file::Result<()> {
            self.0.close()
        }

        /// Reopen closes and reopens the file.
        pub fn reopen(&mut self) -> log_file::Result<()> {
            self.0.close().unwrap();
            self.0.open(0, false).unwrap();
            Ok(())
        }
    }

    /// Returns a new instance of LogFile with a temporary file path.
    pub fn new_log_file(sfile: &Arc<series_file::SeriesFile>) -> LogFile {
        let file = tempfile::NamedTempFile::with_prefix("tsi1-log-file-").unwrap();
        LogFile(log_file::LogFile {
            path: file.path().to_path_buf(),
            sfile: Arc::clone(sfile),
            ..Default::default()
        })
    }

    /// Returns a new, open instance of LogFile. Panic on error.
    pub fn must_open_log_file(sfie: &Arc<series_file::SeriesFile>) -> LogFile {
        let mut f = new_log_file(sfie);
        f.open(0, false).unwrap();
        f
    }

    /// Creates a new temporary log file and adds a list of series.
    pub fn create_log_file(sfile: &Arc<series_file::SeriesFile>, series: Vec<Series>) -> LogFile {
        let mut f = must_open_log_file(sfile);

        let series_set = Arc::new(series_set::SeriesIDSet::new(&[]));

        for serie in series {
            let _ = f
                .add_series_list(
                    Arc::clone(&series_set),
                    &[serie.name.as_slice()],
                    &[&serie.tags],
                )
                .unwrap();
        }

        f
    }

    /// Generates a log file from a set of series based on the count arguments.
    /// Total series returned will equal measurementN * tagN * valueN.
    pub fn generate_log_file(
        sfile: &Arc<series_file::SeriesFile>,
        measurement_n: i32,
        tag_n: i32,
        value_n: i32,
    ) -> LogFile {
        let tag_value_n = value_n.pow(tag_n as u32);
        let f = must_open_log_file(sfile);

        let series_set = Arc::new(series_set::SeriesIDSet::new(&[]));
        for i in 0..measurement_n {
            let name = format!("measurement{}", i);

            // Generate tag sets.
            for j in 0..tag_value_n {
                let mut tags = crate::models::points::Tags::default();

                for k in 0..tag_n {
                    let key = format!("key{}", k);
                    let value = format!("value{}", j / (value_n.pow(k as u32)) % value_n);
                    tags.push(crate::models::points::Tag::new(
                        key.into_bytes(),
                        value.into_bytes(),
                    ));
                }
                let _ = f
                    .add_series_list(Arc::clone(&series_set), &[name.as_bytes()], &[&tags])
                    .unwrap();
            }
        }

        f
    }

    impl Deref for LogFile {
        type Target = log_file::LogFile;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl DerefMut for LogFile {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }

    impl From<LogFile> for log_file::LogFile {
        fn from(lfile: LogFile) -> Self {
            lfile.0
        }
    }
}
