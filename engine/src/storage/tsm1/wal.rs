//! Write ahead log.

use std::{
    any::Any,
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Once,
        atomic::{AtomicI32, AtomicI64, Ordering},
    },
    time::Duration,
};

use crate::common::{time::Time, to_str, to_string};
use crate::models::points::*;
use crate::runtime::execution::{ExecutorRef, legacy_global_executor};
use bytes::{Buf, BufMut};
use chrono::{DateTime, TimeZone, Utc};
use crossbeam_channel::{Receiver as CrossbeamReceiver, Sender as CrossbeamSender, select, tick};
use parking_lot::{Mutex, RwLock};
use rand::distributions::weighted;
use snafu::{ResultExt, Snafu, ensure};
use tokio::sync::{
    mpsc,
    oneshot::{self, Receiver as OneshotReceiver, Sender as OneshotSender},
};
use tokio_util::sync::CancellationToken;

use super::wal_impl::{self, WAL as TSMWAL, WalSegmentWriter as TSMWALSegmentWriter};
use crate::storage::tsm1::vlog::vlog_wal::{
    self, WAL as VLogWAL, WalSegmentWriter as VLogWALSegmentWriter,
};

// Duration to buffer writes before flushing them to the wal.
pub const BUFFER_FLUSH_INTERVAL: Duration = Duration::from_millis(10);
// The maximum number of buffered writes that can be queued up before flushing to wal.
pub const BUFFER_CHANNEL_LIMIT: usize = 10_1000;
// Duration to wait before fsyncing writes. A value of 0 (default) will cause every write to be fsync'd,
pub const SYNC_DELAY: Duration = Duration::ZERO;

/// 10MB is the size at which segment files will be rolled over.
pub const DEFAULT_SEGMENT_SIZE: i64 = 10 * 1024 * 1024;
/// the file extension we expect for wal segments.
pub const WAL_FILE_EXTENSION: &str = "wal";

/// WAL_FILE_PREFIX is the prefix on all wal segment files.
pub const WAL_FILE_PREFIX: &str = "_";

/// the size of the wal entry encoding buffer
/// pub(crate) const WAL_ENCODE_BUF_SIZE: i32 = 4 * 1024 * 1024;

const FLOAT64_ENTRY_TYPE: u8 = 0;
const INTEGER_ENTRY_TYPE: u8 = 1;
const UNSIGNED_ENTRY_TYPE: u8 = 2;
const STRING_ENTRY_TYPE: u8 = 3;
const BOOL_ENTRY_TYPE: u8 = 4;

// WalEntryType is a u8 written to a wal segment file that indicates what the following compressed block contains.
pub type WalEntryType = u8;

// WRITE_WAL_ENTRY_TYPE indicates a write entry.
pub const WRITE_WAL_ENTRY_TYPE: WalEntryType = 0x01;
// DELETE_WAL_ENTRY_TYPE indicates a delete entry.
pub const DELETE_WAL_ENTRY_TYPE: WalEntryType = 0x02;
// DELETE_RANGE_WAL_ENTRY_TYPE indicates a delete range entry.
pub const DELETE_RANGE_WAL_ENTRY_TYPE: WalEntryType = 0x03;

// Statistics gathered by the WAL.
pub const STAT_WAL_OLD_BYTES: &str = "old_segments_disk_bytes";
pub const STAT_WAL_CURRENT_BYTES: &str = "current_segments_disk_bytes";
pub const STAT_WRITE_OK: &str = "write_ok";
pub const STAT_WRITE_ERR: &str = "write_err";

pub static CLOSE: Once = Once::new();

/// Error type for WAL.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    /// IO error.
    #[snafu(display("io error: {}", source))]
    IO { source: io::Error },

    /// ErrWALCorrupt is returned when reading a corrupt WAL entry.
    #[snafu(display("WAL entry corrupted"))]
    WALCorrupted,

    /// Incorrect value found.
    #[snafu(display("{}", reason))]
    IncorrectValue { reason: String },

    /// Unsupported value.
    #[snafu(display("{:?}", reason))]
    UnsupportedValue { reason: String },

    /// fsync error.
    #[snafu(display("snap compress error: {}", reason))]
    FsyncFailed { reason: String },

    /// buffered write error.
    #[snafu(display("buffered write failed: {}", reason))]
    BufferedWriteFailed { reason: String },

    /// TSM WAL error.
    #[snafu(display("{}", source))]
    TSMWALError { source: super::wal_impl::Error },

    /// VLog WAL error.
    #[snafu(display("{}", source))]
    VLogWALError {
        source: crate::storage::tsm1::vlog::vlog_wal::Error,
    },
}

impl From<io::Error> for Error {
    fn from(source: io::Error) -> Error {
        Error::IO { source }
    }
}

/// Result type for WAL.
pub type Result<T, E = Error> = std::result::Result<T, E>;

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
pub enum WAL {
    TSMWAL(TSMWAL),
    VLogWAL(VLogWAL),
}

impl WAL {
    pub fn new_tsm_wal(
        path: Arc<PathBuf>,
        segment_size: i64,
        // mut buffer_flush_interval: Duration,
        sync_delay: Duration,
    ) -> Result<Self> {
        Self::new_tsm_wal_with_executor(path, segment_size, sync_delay, legacy_global_executor())
    }

    pub fn new_tsm_wal_with_executor(
        path: Arc<PathBuf>,
        segment_size: i64,
        sync_delay: Duration,
        executor: ExecutorRef,
    ) -> Result<Self> {
        let wal = TSMWAL::new_with_executor(Arc::clone(&path), segment_size, sync_delay, executor)
            .context(TSMWALSnafu)?;
        tracing::info!(
            target: "reefsdb::tsdb::wal",
            path = %path.display(),
            wal_type = "tsm",
            "WAL opened"
        );
        Ok(WAL::TSMWAL(wal))
    }

    pub fn new_vlog_wal(
        path: Arc<PathBuf>,
        segment_size: i64,
        writer_num: i32,
        // mut buffer_flush_interval: Duration,
        sync_delay: Duration,
    ) -> Result<Self> {
        Self::new_vlog_wal_with_executor(
            path,
            segment_size,
            writer_num,
            sync_delay,
            legacy_global_executor(),
        )
    }

    pub fn new_vlog_wal_with_executor(
        path: Arc<PathBuf>,
        segment_size: i64,
        writer_num: i32,
        sync_delay: Duration,
        executor: ExecutorRef,
    ) -> Result<Self> {
        let wal = VLogWAL::new_with_executor(
            Arc::clone(&path),
            segment_size,
            writer_num,
            sync_delay,
            executor,
        )
        .context(VLogWALSnafu)?;
        tracing::info!(
            target: "reefsdb::tsdb::wal",
            path = %path.display(),
            wal_type = "vlog",
            writer_count = writer_num,
            "WAL opened"
        );
        Ok(WAL::VLogWAL(wal))
    }

    /// path returns the directory the log was initialized with.
    pub fn path(&self) -> &Path {
        match self {
            WAL::TSMWAL(wal) => wal.path(),
            WAL::VLogWAL(wal) => wal.path(),
        }
    }

    /// last_write_time is the last tme anything was written to the WAL.
    pub fn last_write_time(&self) -> Time {
        match self {
            WAL::TSMWAL(wal) => wal.last_write_time(),
            WAL::VLogWAL(wal) => wal.last_write_time(),
        }
    }

    /// Returns the size in bytes of all WAL segments on disk.
    pub fn disk_size_bytes(&self) -> i64 {
        match self {
            WAL::TSMWAL(wal) => wal.disk_size_bytes(),
            WAL::VLogWAL(wal) => wal.disk_size_bytes(),
        }
    }

    /// Returns  statistics for periodic monitoring
    pub fn statistics(&self, tags: HashMap<String, String>) -> crate::models::statistic::Statistic {
        match self {
            WAL::TSMWAL(wal) => wal.statistics(tags),
            WAL::VLogWAL(wal) => wal.statistics(tags),
        }
    }

    /// Writes the given values to the WAL. It returns the WAL segment ID to which the points were written.
    /// If an error is returned the segment ID should be ignored.
    pub async fn write_multi(&self, values: HashMap<String, Vec<Value>>) -> Result<()> {
        match self {
            WAL::TSMWAL(wal) => wal.write_multi(values).await.context(TSMWALSnafu),
            WAL::VLogWAL(wal) => wal.write_multi(values).await.context(VLogWALSnafu),
        }
    }

    pub async fn delete(&self, keys: Vec<Vec<u8>>) -> Result<()> {
        match self {
            WAL::TSMWAL(wal) => wal.delete(keys).await.context(TSMWALSnafu),
            WAL::VLogWAL(wal) => wal.delete(keys).await.context(VLogWALSnafu),
        }
    }

    // TODO: return the segment id for the operation ?
    /// Deletes the given keys within the given time range.
    pub async fn delete_range(
        &self,
        keys: Vec<Vec<u8>>,
        min_time: i64,
        max_time: i64,
    ) -> Result<()> {
        match self {
            WAL::TSMWAL(wal) => wal
                .delete_range(keys, min_time, max_time)
                .await
                .context(TSMWALSnafu),
            WAL::VLogWAL(wal) => wal
                .delete_range(keys, min_time, max_time)
                .await
                .context(VLogWALSnafu),
        }
    }

    /// Closes the current segment if it is non-empty and opens a new one.
    /// this method will be called when engine write snapshot.
    pub fn close_segment(&self) -> Result<()> {
        match self {
            WAL::TSMWAL(wal) => wal.close_segment().context(TSMWALSnafu),
            WAL::VLogWAL(wal) => wal.close_segment().context(VLogWALSnafu),
        }
    }

    /// Returns a slice of the names of the closed segment files.
    pub fn closed_segments(&self) -> Result<Vec<String>> {
        match self {
            WAL::TSMWAL(wal) => wal.closed_segments().context(TSMWALSnafu),
            WAL::VLogWAL(wal) => wal.closed_segments().context(VLogWALSnafu),
        }
    }

    /// Deletes the given segment file paths from disk and cleans up any associated objects.
    pub fn remove(&self, files: &[String]) -> Result<()> {
        let result = match self {
            WAL::TSMWAL(wal) => wal.remove(files).context(TSMWALSnafu),
            WAL::VLogWAL(wal) => wal.remove(files).context(VLogWALSnafu),
        };
        if result.is_ok() && !files.is_empty() {
            tracing::debug!(
                target: "reefsdb::tsdb::wal",
                path = %self.path().display(),
                segment_count = files.len(),
                "WAL segments removed"
            );
        }
        result
    }

    /// close will finish any flush that is currently in progress and close file handles.
    pub fn close(&self) {
        let path = self.path().to_path_buf();
        match self {
            WAL::TSMWAL(wal) => wal.close(),
            WAL::VLogWAL(wal) => wal.close(),
        }
        tracing::info!(
            target: "reefsdb::tsdb::wal",
            path = %path.display(),
            "WAL closed"
        );
    }

    #[cfg(test)]
    fn sync(&self) {
        match self {
            WAL::TSMWAL(wal) => wal.sync(),
            WAL::VLogWAL(wal) => wal.sync(),
        }
    }

    #[cfg(test)]
    fn flush(&self) {
        match self {
            WAL::TSMWAL(wal) => wal.flush(),
            WAL::VLogWAL(wal) => wal.flush(),
        }
    }
}

pub enum WALSegmentWriter {
    TSMWALSegmentWriter(TSMWALSegmentWriter),
    VLogWALSegmentWriter(VLogWALSegmentWriter),
}

impl WALSegmentWriter {
    pub fn new_tsm_wal_segment_writer(
        path: Arc<PathBuf>,
        segment_size: i64,
        id: i64,
        writer: BufWriter<File>,
        sync_waiters: Arc<(
            CrossbeamSender<OneshotSender<Option<String>>>,
            CrossbeamReceiver<OneshotSender<Option<String>>>,
        )>,
        pos: u64,
    ) -> Self {
        WALSegmentWriter::TSMWALSegmentWriter(TSMWALSegmentWriter::new(
            path,
            segment_size,
            id,
            writer,
            sync_waiters,
            pos,
        ))
    }

    pub fn new_tsm_vlog_segment_writer(
        path: Arc<PathBuf>,
        segment_size: i64,
        writer_id: i32,
        segment_id: i64,
        writer: BufWriter<File>,
        sync_waiters: Arc<(
            CrossbeamSender<OneshotSender<Option<String>>>,
            CrossbeamReceiver<OneshotSender<Option<String>>>,
        )>,
        pos: u64,
    ) -> Self {
        WALSegmentWriter::VLogWALSegmentWriter(VLogWALSegmentWriter::new(
            path,
            segment_size,
            writer_id,
            segment_id,
            writer,
            sync_waiters,
            pos,
        ))
    }

    pub fn current_segment_writer_path(&self) -> String {
        match self {
            WALSegmentWriter::TSMWALSegmentWriter(w) => w.current_segment_writer_path(),
            WALSegmentWriter::VLogWALSegmentWriter(w) => w.current_segment_writer_path(),
        }
    }

    /// write log entry to WAL
    pub fn write_entry(
        &mut self,
        entry_type: WalEntryType,
        compressed: &[u8],
        stats: Arc<WalStatistics>,
    ) -> Result<()> {
        match self {
            WALSegmentWriter::TSMWALSegmentWriter(w) => w
                .write_entry(entry_type, compressed, stats)
                .context(TSMWALSnafu),
            WALSegmentWriter::VLogWALSegmentWriter(w) => w
                .write_entry(entry_type, compressed, stats)
                .context(VLogWALSnafu),
        }
    }

    pub fn roll_segment(&mut self, stats: Arc<WalStatistics>) -> Result<()> {
        match self {
            WALSegmentWriter::TSMWALSegmentWriter(w) => w.roll_segment(stats).context(TSMWALSnafu),
            WALSegmentWriter::VLogWALSegmentWriter(w) => {
                w.roll_segment(stats).context(VLogWALSnafu)
            }
        }
    }

    pub fn new_segment_file(&mut self, stats: Arc<WalStatistics>) -> Result<()> {
        match self {
            WALSegmentWriter::TSMWALSegmentWriter(w) => {
                w.new_segment_file(stats).context(TSMWALSnafu)
            }
            WALSegmentWriter::VLogWALSegmentWriter(w) => {
                w.new_segment_file(stats).context(VLogWALSnafu)
            }
        }
    }

    pub fn sync(&mut self) {
        match self {
            WALSegmentWriter::TSMWALSegmentWriter(w) => w.sync(),
            WALSegmentWriter::VLogWALSegmentWriter(w) => w.sync(),
        }
    }

    pub fn flush(&mut self) -> Result<()> {
        match self {
            WALSegmentWriter::TSMWALSegmentWriter(w) => w.flush().context(TSMWALSnafu),
            WALSegmentWriter::VLogWALSegmentWriter(w) => w.flush().context(VLogWALSnafu),
        }
    }

    pub fn current_segment_size(&self) -> i64 {
        match self {
            WALSegmentWriter::TSMWALSegmentWriter(w) => w.current_segment_size(),
            WALSegmentWriter::VLogWALSegmentWriter(w) => w.current_segment_size(),
        }
    }
}

// /////////////////////////////////////////
// /////////// WAL Reader    ///////////////
// /////////////////////////////////////////

pub struct WalSegmentReader<'a> {
    segment: &'a File, // must be opened file
    pos: usize,
}

impl<'a> WalSegmentReader<'a> {
    pub fn new(f: &'a File) -> Self {
        Self { segment: f, pos: 0 }
    }

    pub fn reset(&mut self) -> Result<()> {
        // self.segment = None;
        self.segment.seek(SeekFrom::Start(0))?;
        self.pos = 0;

        Ok(())
    }

    /// Returns the total number of bytes read successfully from the segment, as
    /// of the last call to next(). The segment is guaranteed to be valid up to and
    /// including this number of bytes.
    pub fn read_bytes_count(&self) -> i64 {
        self.pos as i64
    }

    /// Close closes the underlying reader
    pub fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Iterator for WalSegmentReader<'_> {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        // if self.segment.is_none() {
        //     return None;
        // }

        let cur_pos = self.pos;
        // let mut segment_file = self.segment.as_ref().unwrap();
        if let Err(e) = self.segment.seek(SeekFrom::Start(cur_pos as u64)) {
            return Some(Err(e.into()));
        }

        // read the type and the length fo the entry.
        let mut read_ok = 0;
        let mut lv: [u8; 5] = [0; 5];
        let mut handle = self.segment.take(5);
        match handle.read(&mut lv[..]) {
            Ok(n) => {
                if n == 0 {
                    // EOF
                    return None;
                }
                read_ok += n;
            }
            Err(e) => {
                return Some(Err(e.into()));
            }
        }

        // read entry type and entry length
        let entry_type: WalEntryType = lv[0];
        let length = (&lv[1..5]).get_u32();

        let mut b = vec![0; length as usize];
        unsafe {
            b.set_len(length as usize);
        }
        // read the compressed block and decompress it
        let mut handle = self.segment.take(length as u64);
        match handle.read(b.as_mut_slice()) {
            Ok(n) => read_ok += n,
            Err(e) => {
                // We return true here because we want the client code to call read which
                // will return the this error to be handled.
                return Some(Err(e.into())); // TODO: read error
            }
        }

        let data = match snap::raw::Decoder::new().decompress_vec(&b) {
            Ok(v) => v,
            Err(_) => return None,
        };

        let entry = match entry_type {
            WRITE_WAL_ENTRY_TYPE => {
                let mut e = WriteWalEntry::new();
                if e.unmarshal_binary(&data).is_err() {
                    return None;
                }
                Entry::WriteWalEntry(e)
            }
            DELETE_WAL_ENTRY_TYPE => {
                let mut e = DeleteWalEntry::new();
                if let Err(e) = e.unmarshal_binary(&data) {
                    return Some(Err(e));
                }
                Entry::DeleteWalEntry(e)
            }
            DELETE_RANGE_WAL_ENTRY_TYPE => {
                let mut e = DeleteRangeWalEntry::new();
                if let Err(e) = e.unmarshal_binary(&data) {
                    return Some(Err(e));
                }
                Entry::DeleteRangeWalEntry(e)
            }
            _ => unreachable!(),
        };
        // Read and decode of this entry was successful.
        self.pos += read_ok;

        Some(Ok(entry))
    }
}

// pub enum WALSegmentReader<'a> {
//     TSMWALSegmentReader(TSMWALSegmentReader<'a>),
//     VLogWALSegmentReader(VLogWALSegmentReader<'a>),
// }

// impl<'a> WALSegmentReader<'a> {
//     pub fn new_tsm_wal_segment_reader(f: &'a File) -> Self {
//         WALSegmentReader::TSMWALSegmentReader(TSMWALSegmentReader::new(f))
//     }

//     pub fn new_vlog_wal_segment_reader(f: &'a File) -> Self {
//         WALSegmentReader::VLogWALSegmentReader(VLogWALSegmentReader::new(f))
//     }

//     pub fn reset(&mut self) -> Result<()> {
//         match self {
//             WALSegmentReader::TSMWALSegmentReader(r) => r.reset().context(TSMWALSnafu),
//             WALSegmentReader::VLogWALSegmentReader(r) => r.reset().context(VLogWALSnafu),
//         }
//     }

//     /// Returns the total number of bytes read successfully from the segment, as
//     /// of the last call to next(). The segment is guaranteed to be valid up to and
//     /// including this number of bytes.
//     pub fn read_bytes_count(&self) -> i64 {
//         match self {
//             WALSegmentReader::TSMWALSegmentReader(r) => r.read_bytes_count(),
//             WALSegmentReader::VLogWALSegmentReader(r) => r.read_bytes_count(),
//         }
//     }

//     /// Close closes the underlying reader
//     pub fn close(&mut self) -> Result<()> {
//         match self {
//             WALSegmentReader::TSMWALSegmentReader(r) => r.close().context(TSMWALSnafu),
//             WALSegmentReader::VLogWALSegmentReader(r) => r.close().context(VLogWALSnafu),
//         }
//     }
// }

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

pub struct WriteOp {
    pub entry: Box<dyn WalEntry + Send>,
    pub resp_tx: OneshotSender<Option<String>>,
}

#[derive(Default, Debug)]
pub struct WalStatistics {
    pub old_bytes: Arc<AtomicI64>,
    pub current_bytes: Arc<AtomicI64>,
    pub write_ok: Arc<AtomicI64>,
    pub write_err: Arc<AtomicI64>,
}

impl WalStatistics {}

pub trait WalEntry {
    fn entry_type(&self) -> WalEntryType;
    // fn entry_values(&self) -> Box
    fn marshal_size(&mut self) -> usize;
    fn encode(&mut self, dst: &mut Vec<u8>) -> Result<()>;
    fn marshal_binary(&mut self) -> Result<Vec<u8>>;
    fn unmarshal_binary(&mut self, b: &[u8]) -> Result<()>;
    /// Cast client to [`Any`], useful for downcasting.
    fn as_any(&self) -> &dyn Any;
}
/// WriteWalEntry represents a write of points.
#[derive(Debug, Clone)]
pub struct WriteWalEntry {
    pub values: HashMap<String, Vec<Value>>,
    pub size: usize,
}

impl WriteWalEntry {
    /// return empty WriteWalEntry
    pub fn new() -> Self {
        Self {
            values: HashMap::<String, Vec<Value>>::new(),
            size: 0,
        }
    }
}

impl WalEntry for WriteWalEntry {
    /// Returns WRITE_WAL_ENTRY_TYPE.
    fn entry_type(&self) -> WalEntryType {
        WRITE_WAL_ENTRY_TYPE
    }

    fn marshal_size(&mut self) -> usize {
        if self.size > 0 || self.values.is_empty() {
            return self.size;
        }

        let mut enc_len = 7 * self.values.len(); // Type (1), Key Length (2), and Count (4) for each key

        // determine required length
        for (k, v) in self.values.iter() {
            enc_len += k.len();
            if v.is_empty() {
                return 0;
            }

            enc_len += 8 * v.len(); // timestamps (8)

            match v[0].value_type() {
                BOOL => enc_len += v.len(), // 1 * v.len()
                INTEGER | UNSIGNED | FLOAT => enc_len += 8 * v.len(),
                STRING => {
                    for vv in v.iter() {
                        if let Some(sval) = vv.string_value() {
                            enc_len += 4 + sval.len();
                        } else {
                            return 0;
                        }
                    }
                }
                _ => return 0,
            }
        }
        self.size = enc_len;

        self.size
    }

    /// encode coverts the WriteWalEntry into a byte stream using dst if it is a large enough,
    /// if dst is too small, the slice will be grown to fit he encoded entry.
    fn encode(&mut self, dst: &mut Vec<u8>) -> Result<()> {
        // The entries values are encode as follows:
        //
        // For each key and slice of values, first a 1 byte type for the []Values
        // slice is written.  Following the type, the length and key bytes are written.
        // Following the key, a 4 byte count followed by each value as a 8 byte time
        // and N byte value.  The value is dependent on the type being encoded.  float64,
        // int64, use 8 bytes, boolean uses 1 byte, and string is similar to the key encoding,
        // except that string values have a 4-byte length, and keys only use 2 bytes.
        //
        // This structure is then repeated for each key an value slices.
        //
        // ┌────────────────────────────────────────────────────────────────────┐
        // │                           WriteWALEntry                            │
        // ├──────┬─────────┬────────┬───────┬─────────┬─────────┬───┬──────┬───┤
        // │ Type │ Key Len │   Key  │ Count │  Time   │  Value  │...│ Type │...│
        // │1 byte│ 2 bytes │ N bytes│4 bytes│ 8 bytes │ N bytes │   │1 byte│   │
        // └──────┴─────────┴────────┴───────┴─────────┴─────────┴───┴──────┴───┘

        let enc_len = self.marshal_size(); // Type (1), Key Length (2), and Count (4) for each key
        // allocate or resize slice to corrent size
        if dst.len() < enc_len {
            dst.resize(enc_len, 0u8);
        }

        // encode wal entry
        let mut n: usize = 0;
        let mut cur_type: u8;

        for (k, v) in self.values.iter() {
            match v[0].value_type() {
                BOOL => cur_type = BOOL_ENTRY_TYPE,
                INTEGER => cur_type = INTEGER_ENTRY_TYPE,
                UNSIGNED => cur_type = UNSIGNED_ENTRY_TYPE,
                FLOAT => cur_type = FLOAT64_ENTRY_TYPE,
                STRING => cur_type = STRING_ENTRY_TYPE,
                _ => {
                    return UnsupportedValueSnafu {
                        reason: format!("unsupported value type: {:?}", v[0]),
                    }
                    .fail();
                }
            }

            dst[n] = cur_type;
            n += 1;

            (&mut dst[n..n + 2]).put_u16(k.len() as u16); // BigEndian
            n += 2;
            dst[n..n + k.len()].copy_from_slice(k.as_bytes());
            n += k.len();

            (&mut dst[n..n + 4]).put_u32(v.len() as u32); // BigEndian
            n += 4;

            for vv in v.iter() {
                (&mut dst[n..n + 8]).put_u64(vv.unix_nano() as u64);
                n += 8;

                match vv.value_type() {
                    BOOL => {
                        ensure!(
                            cur_type == BOOL,
                            IncorrectValueSnafu {
                                reason: format!(
                                    "incorrect value found in {:?} slice: {:?}",
                                    v[0].bool_value(),
                                    vv
                                ),
                            }
                        );
                        if vv.bool_value().unwrap() {
                            dst[n] = 1;
                        } else {
                            dst[n] = 0;
                        }
                        n += 1;
                    }
                    INTEGER => {
                        ensure!(
                            cur_type == INTEGER,
                            IncorrectValueSnafu {
                                reason: format!(
                                    "incorrect value found in {:?} slice: {:?}",
                                    v[0].integer_value(),
                                    vv
                                ),
                            }
                        );
                        (&mut dst[n..n + 8]).put_u64(vv.integer_value().unwrap() as u64);
                        n += 8;
                    }
                    UNSIGNED => {
                        ensure!(
                            cur_type == UNSIGNED,
                            IncorrectValueSnafu {
                                reason: format!(
                                    "incorrect value found in {:?} slice: {:?}",
                                    v[0].unsigned_value(),
                                    vv
                                ),
                            }
                        );
                        (&mut dst[n..n + 8]).put_u64(vv.unsigned_value().unwrap());
                        n += 8;
                    }
                    FLOAT => {
                        ensure!(
                            cur_type == FLOAT,
                            IncorrectValueSnafu {
                                reason: format!(
                                    "incorrect value found in {:?} slice: {:?}",
                                    v[0].float_value(),
                                    vv
                                ),
                            }
                        );
                        (&mut dst[n..n + 8]).put_f64(vv.float_value().unwrap());
                        n += 8;
                    }
                    STRING => {
                        ensure!(
                            cur_type == STRING,
                            IncorrectValueSnafu {
                                reason: format!(
                                    "incorrect value found in {:?} slice: {:?}",
                                    v[0].string_value(),
                                    vv
                                ),
                            }
                        );
                        let sval_len = vv.string_value().unwrap().len();
                        (&mut dst[n..n + 4]).put_u32(sval_len as u32); // string value size
                        n += 4;
                        dst[n..n + sval_len].copy_from_slice(vv.string_value().unwrap().as_bytes());
                        n += sval_len;
                    }
                    _ => {
                        return UnsupportedValueSnafu {
                            reason: format!("unsupported value found in slice: {:?}", vv),
                        }
                        .fail();
                    }
                }
            }
        }
        dst.truncate(n);
        Ok(())
    }

    /// Return a binary representation of the entry in a new byte slice.
    fn marshal_binary(&mut self) -> Result<Vec<u8>> {
        // temp buffer to write encoded points into
        let mut b: Vec<u8> = Vec::with_capacity(self.marshal_size());
        match self.encode(&mut b) {
            Ok(_) => Ok(b),
            Err(e) => Err(e),
        }
    }

    /// Deserializes the byte slice into values.
    fn unmarshal_binary(&mut self, b: &[u8]) -> Result<()> {
        let mut i: usize = 0;
        while i < b.len() {
            let typ = b[i];
            i += 1;

            ensure!(i + 2 <= b.len(), WALCorruptedSnafu);

            let length = (&b[i..i + 2]).get_u16() as usize; // BigEndian
            i += 2;
            ensure!(i + length <= b.len(), WALCorruptedSnafu);

            let k = to_str!(&b[i..i + length]);
            i += length;

            ensure!(i + 4 <= b.len(), WALCorruptedSnafu);

            let nvals = (&b[i..i + 4]).get_u32() as usize; // BigEndian
            i += 4;

            ensure!(nvals > 0 && nvals <= b.len(), WALCorruptedSnafu);

            match typ {
                FLOAT64_ENTRY_TYPE => {
                    ensure!(i + 16 * nvals <= b.len(), WALCorruptedSnafu);

                    let mut values: Vec<Value> = Vec::with_capacity(nvals);
                    let mut j = 0;
                    while j < nvals {
                        let un = (&b[i..i + 8]).get_u64() as i64;
                        i += 8;
                        let mut vbuf: [u8; 8] = [0; 8];
                        vbuf.copy_from_slice(&b[i..i + 8]);
                        let v = f64::from_be_bytes(vbuf); // BigEndian
                        i += 8;
                        values.push(Value::FloatValue(FloatValue::new(un, v)));
                        j += 1;
                    }
                    self.values.insert(k.to_owned(), values);
                }
                INTEGER_ENTRY_TYPE => {
                    ensure!(i + 16 * nvals <= b.len(), WALCorruptedSnafu);

                    let mut values: Vec<Value> = Vec::with_capacity(nvals);
                    let mut j = 0;
                    while j < nvals {
                        let un = (&b[i..i + 8]).get_u64() as i64;
                        i += 8;
                        let v = (&b[i..i + 8]).get_u64() as i64; // BigEndian
                        i += 8;
                        values.push(Value::IntegerValue(IntegerValue::new(un, v)));
                        j += 1;
                    }
                    self.values.insert(k.to_owned(), values);
                }
                UNSIGNED_ENTRY_TYPE => {
                    ensure!(i + 16 * nvals <= b.len(), WALCorruptedSnafu);

                    let mut values: Vec<Value> = Vec::with_capacity(nvals);
                    let mut j = 0;
                    while j < nvals {
                        let un = (&b[i..i + 8]).get_u64() as i64;
                        i += 8;
                        let v = (&b[i..i + 8]).get_u64(); // BigEndian
                        i += 8;
                        values.push(Value::UnsignedValue(UnsignedValue::new(un, v)));
                        j += 1;
                    }
                    self.values.insert(k.to_owned(), values);
                }
                BOOL_ENTRY_TYPE => {
                    ensure!(i + 9 * nvals <= b.len(), WALCorruptedSnafu);

                    let mut values: Vec<Value> = Vec::with_capacity(nvals);
                    let mut j = 0;
                    while j < nvals {
                        let un = (&b[i..i + 8]).get_u64() as i64;
                        i += 8;
                        let v = b[i];
                        i += 1;
                        if v == 1 {
                            values.push(Value::BoolValue(BoolValue::new(un, true)));
                        } else {
                            values.push(Value::BoolValue(BoolValue::new(un, false)));
                        }
                        j += 1;
                    }
                    self.values.insert(k.to_owned(), values);
                }
                STRING_ENTRY_TYPE => {
                    let mut values: Vec<Value> = Vec::with_capacity(nvals);
                    let mut j = 0;
                    while j < nvals {
                        ensure!(i + 12 <= b.len(), WALCorruptedSnafu);

                        let un = (&b[i..i + 8]).get_u64() as i64;
                        i += 8;

                        let length = (&b[i..i + 4]).get_u32() as usize;
                        ensure!(i + length <= b.len(), WALCorruptedSnafu);

                        i += 4;

                        ensure!(i + length <= b.len(), WALCorruptedSnafu);

                        let v = to_string!(&b[i..i + length]);
                        i += length;

                        values.push(Value::StringValue(StringValue::new(un, v)));
                        j += 1;
                    }
                    self.values.insert(k.to_owned(), values); // TODO: value map should be ordered by key ?
                }
                _ => {
                    return UnsupportedValueSnafu {
                        reason: format!("unsupported value type: {}", typ),
                    }
                    .fail();
                }
            }
        }

        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// DeleteWalEntry represents a delete of points.
#[derive(Debug, Clone)]
pub struct DeleteWalEntry {
    pub keys: Vec<Vec<u8>>,
    pub size: usize,
}

impl DeleteWalEntry {
    /// return an empty DeleteEntry
    pub fn new() -> Self {
        Self {
            keys: Vec::<Vec<u8>>::new(),
            size: 0,
        }
    }
}

impl WalEntry for DeleteWalEntry {
    /// Returns DELETE_WAL_ENTRY_TYPE.
    fn entry_type(&self) -> WalEntryType {
        DELETE_WAL_ENTRY_TYPE
    }

    fn marshal_size(&mut self) -> usize {
        if self.size > 0 || self.keys.is_empty() {
            return self.size;
        }

        let mut enc_len = self.keys.len();
        for k in self.keys.iter() {
            enc_len += k.len();
        }

        self.size = enc_len;

        enc_len
    }

    /// converts the DeleteRangeWalEntry into a byte slice, appending to dst.
    fn encode(&mut self, dst: &mut Vec<u8>) -> Result<()> {
        let enc_len = self.marshal_size(); // keys count, key len for each key
        // allocate or resize slice to corrent size
        if dst.len() < enc_len {
            dst.resize(enc_len, 0u8);
        }

        let mut n: usize = 0;
        for k in self.keys.iter() {
            dst[n..n + k.len()].copy_from_slice(k.as_slice());
            n += k.len();
            dst[n] = b'\n';
            n += 1;
        }

        // truncate the vec to n-1 to strip off the last newline so that unmarshalling the value
        // does not produce an empty string
        dst.truncate(n - 1);
        Ok(())
    }

    // Return a binary representation of the entry in a new byte slice.
    fn marshal_binary(&mut self) -> Result<Vec<u8>> {
        let mut b: Vec<u8> = Vec::with_capacity(self.marshal_size());
        self.encode(&mut b)?;

        Ok(b)
    }

    // deserializes the byte slice into self.
    fn unmarshal_binary(&mut self, b: &[u8]) -> Result<()> {
        if b.is_empty() {
            return Ok(());
        }

        self.keys = to_str!(b)
            .split('\n')
            .map(|s| s.as_bytes().to_vec())
            .collect::<Vec<Vec<u8>>>();

        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// WriteRangeWalEntry represents a delete range of points.
#[derive(Debug, Clone)]
pub struct DeleteRangeWalEntry {
    pub keys: Vec<Vec<u8>>,
    pub min: i64, // min time
    pub max: i64,
    pub size: usize,
}

impl DeleteRangeWalEntry {
    /// return an empty DeleteRangeWalEntry
    pub fn new() -> Self {
        Self {
            keys: Vec::<Vec<u8>>::new(),
            min: 0,
            max: 0,
            size: 0,
        }
    }
}

impl WalEntry for DeleteRangeWalEntry {
    /// Returns DELETE_RANGE_WAL_ENTRY_TYPE.
    fn entry_type(&self) -> WalEntryType {
        DELETE_RANGE_WAL_ENTRY_TYPE
    }

    fn marshal_size(&mut self) -> usize {
        if self.size > 0 {
            return self.size;
        }

        let mut sz = 16 + self.keys.len() * 4;
        for k in self.keys.iter() {
            sz += k.len();
        }
        self.size = sz;

        sz
    }

    /// converts the DeleteRangeWalEntry into a byte slice, appending to dst.
    fn encode(&mut self, dst: &mut Vec<u8>) -> Result<()> {
        let sz = self.marshal_size();
        if dst.len() < sz {
            dst.resize(sz, 0u8);
        }

        (&mut dst[..8]).put_u64(self.min as u64); // BigEndian
        (&mut dst[8..16]).put_u64(self.max as u64);

        let mut i = 16;
        for k in self.keys.iter() {
            (&mut dst[i..i + 4]).put_u32(k.len() as u32);
            i += 4;
            dst[i..i + k.len()].copy_from_slice(k.as_slice());
            i += k.len();
        }

        dst.truncate(i);
        Ok(())
    }

    // Return a binary representation of the entry in a new byte slice.
    fn marshal_binary(&mut self) -> Result<Vec<u8>> {
        let mut b: Vec<u8> = Vec::with_capacity(self.marshal_size());
        let _ = self.encode(&mut b);

        Ok(b)
    }

    // deserializes the byte slice into self.
    fn unmarshal_binary(&mut self, b: &[u8]) -> Result<()> {
        ensure!(b.len() >= 16, WALCorruptedSnafu);

        self.min = (&b[0..8]).get_u64() as i64; // BigEndian
        self.max = (&b[8..16]).get_u64() as i64;

        let mut i = 16;
        while i < b.len() {
            ensure!(i + 4 <= b.len(), WALCorruptedSnafu);

            let sz = (&b[i..i + 4]).get_u32() as usize;
            i += 4;

            ensure!(i + sz <= b.len(), WALCorruptedSnafu);

            // b originates from a pool. Copy what needs to be retained.
            let mut buf = vec![0; sz];
            unsafe {
                buf.set_len(sz);
            }
            buf.copy_from_slice(&b[i..i + sz]);
            self.keys.push(buf);
            i += sz;
        }

        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
pub enum Entry {
    WriteWalEntry(WriteWalEntry),
    DeleteWalEntry(DeleteWalEntry),
    DeleteRangeWalEntry(DeleteRangeWalEntry),
}

impl Entry {
    pub fn entry_type(&self) -> WalEntryType {
        match self {
            Entry::WriteWalEntry(entry) => entry.entry_type(),
            Entry::DeleteWalEntry(entry) => entry.entry_type(),
            Entry::DeleteRangeWalEntry(entry) => entry.entry_type(),
        }
    }

    // fn entry_values(&self) -> Box
    pub fn marshal_size(&mut self) -> usize {
        match self {
            Entry::WriteWalEntry(entry) => entry.marshal_size(),
            Entry::DeleteWalEntry(entry) => entry.marshal_size(),
            Entry::DeleteRangeWalEntry(entry) => entry.marshal_size(),
        }
    }

    pub fn encode(&mut self, dst: &mut Vec<u8>) -> Result<()> {
        match self {
            Entry::WriteWalEntry(entry) => entry.encode(dst),
            Entry::DeleteWalEntry(entry) => entry.encode(dst),
            Entry::DeleteRangeWalEntry(entry) => entry.encode(dst),
        }
    }

    pub fn marshal_binary(&mut self) -> Result<Vec<u8>> {
        match self {
            Entry::WriteWalEntry(entry) => entry.marshal_binary(),
            Entry::DeleteWalEntry(entry) => entry.marshal_binary(),
            Entry::DeleteRangeWalEntry(entry) => entry.marshal_binary(),
        }
    }

    pub fn unmarshal_binary(&mut self, b: &[u8]) -> Result<()> {
        match self {
            Entry::WriteWalEntry(entry) => entry.unmarshal_binary(b),
            Entry::DeleteWalEntry(entry) => entry.unmarshal_binary(b),
            Entry::DeleteRangeWalEntry(entry) => entry.unmarshal_binary(b),
        }
    }
}

pub fn segment_file_names(path: &Path) -> Result<Vec<String>> {
    wal_impl::segment_file_names(path).context(TSMWALSnafu)
}

pub fn vlog_segment_file_names(path: &Path) -> Result<Vec<(i32, Vec<String>)>> {
    vlog_wal::segment_file_names(path).context(VLogWALSnafu)
}

#[cfg(any(test, feature = "benchmarking"))]
pub mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, mpsc},
    };

    use crate::runtime::global::block_on_global;
    use tokio::sync::oneshot;

    use super::{
        DeleteRangeWalEntry, DeleteWalEntry, Value, WAL, WalEntry, WalSegmentReader, WriteWalEntry,
        *,
    };

    #[test]
    fn test_wal_writer_write_multi_single() {
        // create wal test dir
        let dir = must_temp_wal_dir();

        // create point values
        let p1 = Value::FloatValue(FloatValue::new(1, 1.1));
        let p2 = Value::IntegerValue(IntegerValue::new(1, 1i64));
        let p3 = Value::BoolValue(BoolValue::new(1, true));
        let p4 = Value::StringValue(StringValue::new(1, "string".to_string()));
        let p5 = Value::UnsignedValue(UnsignedValue::new(1, 1u64));

        let values: HashMap<String, Vec<Value>> = HashMap::from([
            ("cpu,host=A#!~#float".to_string(), vec![p1]),
            ("cpu,host=A#!~#int".to_string(), vec![p2]),
            ("cpu,host=A#!~#bool".to_string(), vec![p3]),
            ("cpu,host=A#!~#string".to_string(), vec![p4]),
            ("cpu,host=A#!~#unsigned".to_string(), vec![p5]),
        ]);

        // create segment writer
        let f = must_write_temp_file(dir.path());
        let mut w = TSMWALSegmentWriter::new(
            // Arc::new(dir.path().into()),
            Arc::new(dir.path().into()),
            10 * 1024 * 1024,
            0,
            BufWriter::new(f),
            Arc::new(crossbeam_channel::bounded(1024)),
            0, // the new segment writer's current pos is 0
        );

        let mut entry = WriteWalEntry {
            values: values.clone(),
            size: 0, // size will be updated in marshal_size
        };

        let stats = Arc::new(WalStatistics::default());

        let compressed = must_marshal_entry(&mut entry);
        assert!(
            w.write_entry(
                WRITE_WAL_ENTRY_TYPE,
                compressed.as_slice(),
                Arc::clone(&stats)
            )
            .is_ok()
        );

        // don't test channel result, because we just test write/read from file.
        // assert!(matches!(sync_rx.recv().unwrap(), None));  // write success return Some(None)

        w.sync();
        drop(w);

        // create segment reader
        let f = must_read_temp_file(dir.path());
        let mut r = WalSegmentReader::new(&f);

        let e = r.next();
        assert!(e.is_some());

        // convert trait object to struct
        let e = e.unwrap();
        let e = match e {
            Ok(Entry::WriteWalEntry(e)) => e,
            _ => panic!("invalid wal entry: {:?}", e),
        };

        for (k, v) in &e.values {
            for (i, vv) in v.iter().enumerate() {
                assert_eq!(vv.to_string(), values[k][i].to_string());
            }
        }

        assert_eq!(
            must_read_file_size(&dir.path().join("00001.wal")),
            r.read_bytes_count() as i64
        );
    }

    #[test]
    fn test_wal_writer_write_multi_large_batch() {
        // create wal test dir
        let dir = must_temp_wal_dir();

        // create point values
        let mut points = Vec::<Value>::new();
        let mut i = 0;
        while i < 100000 {
            points.push(Value::IntegerValue(IntegerValue::new(i, 1i64)));
            i += 1;
        }
        let values: HashMap<String, Vec<Value>> = HashMap::from([
            (
                "cpu,host=A,server=01,foo=bar,tag=really-long#!~#float".to_string(),
                points.clone(),
            ),
            (
                "mem,host=A,server=01,foo=bar,tag=really-long#!~#float".to_string(),
                points.clone(),
            ),
        ]);

        // create segment writer
        let f = must_write_temp_file(dir.path());
        let mut w = TSMWALSegmentWriter::new(
            // Arc::new(dir.path().into()),
            Arc::new(dir.path().into()),
            10 * 1024 * 1024,
            0,
            BufWriter::new(f),
            Arc::new(crossbeam_channel::bounded(1024)),
            0, // the new segment writer's current pos is 0
        );

        let mut entry = WriteWalEntry {
            values: values.clone(),
            size: 0, // size will be updated in marshal_size
        };

        let stats = Arc::new(WalStatistics::default());
        let compressed = must_marshal_entry(&mut entry);
        assert!(
            w.write_entry(
                WRITE_WAL_ENTRY_TYPE,
                compressed.as_slice(),
                Arc::clone(&stats)
            )
            .is_ok()
        );

        // don't test channel result, because we just test write/read from file.
        // assert!(matches!(sync_rx.recv().unwrap(), None));  // write success return Some(None)

        w.sync();
        drop(w);

        // create segment reader
        let f = must_read_temp_file(dir.path());
        let mut r = WalSegmentReader::new(&f);

        let e = r.next();
        assert!(e.is_some());

        // convert trait object to struct
        let e = e.unwrap();
        let e = match e {
            Ok(Entry::WriteWalEntry(e)) => e,
            _ => panic!("invalid wal entry: {:?}", e),
        };

        for (k, v) in &e.values {
            for (i, vv) in v.iter().enumerate() {
                assert_eq!(vv.to_string(), values[k][i].to_string());
            }
        }

        assert_eq!(
            must_read_file_size(&dir.path().join("00001.wal")),
            r.read_bytes_count() as i64
        );
    }

    #[test]
    fn test_wal_writer_write_multi_multiple() {
        // create wal test dir
        let dir = must_temp_wal_dir();

        // create point values
        let p1 = Value::FloatValue(FloatValue::new(1, 1.1));
        let p2 = Value::IntegerValue(IntegerValue::new(1, 1i64));

        let exp: HashMap<String, Vec<Value>> = HashMap::from([
            (
                "cpu,host=A,server=01,foo=bar,tag=really-long#!~#float".to_string(),
                vec![p1],
            ),
            (
                "mem,host=A,server=01,foo=bar,tag=really-long#!~#float".to_string(),
                vec![p2],
            ),
        ]);

        // create segment writer
        let f = must_write_temp_file(dir.path());
        let mut w = TSMWALSegmentWriter::new(
            // Arc::new(dir.path().into()),
            Arc::new(dir.path().into()),
            10 * 1024 * 1024,
            0,
            BufWriter::new(f),
            Arc::new(crossbeam_channel::bounded(1024)),
            0, // the new segment writer's current pos is 0
        );

        let stats = Arc::new(WalStatistics::default());
        for (k, v) in exp.iter() {
            let mut entry = WriteWalEntry {
                values: HashMap::from([(k.clone(), v.clone())]),
                size: 0, // size will be updated in marshal_size
            };
            let compressed = must_marshal_entry(&mut entry);
            assert!(
                w.write_entry(
                    WRITE_WAL_ENTRY_TYPE,
                    compressed.as_slice(),
                    Arc::clone(&stats)
                )
                .is_ok()
            );

            w.sync();
        }

        drop(w);

        // create segment reader
        let f = must_read_temp_file(dir.path());
        let mut r = WalSegmentReader::new(&f);

        for (ep_k, ep) in exp.iter() {
            let e = r.next();
            assert!(e.is_some());

            // convert trait object to struct
            let e = e.unwrap();
            let e = match e {
                Ok(Entry::WriteWalEntry(e)) => e,
                _ => panic!("invalid wal entry: {:?}", e),
            };

            for (k, v) in &e.values {
                // assert key match
                assert_eq!(k, ep_k);
                // assert values length match
                assert_eq!(v.len(), ep.len());
                // assert value is equal
                for (i, vv) in v.iter().enumerate() {
                    assert_eq!(vv.to_string(), ep[i].to_string());
                }
            }
        }

        // check segment reader bytes count is equal to file read bytes count
        assert_eq!(
            must_read_file_size(&dir.path().join("00001.wal")),
            r.read_bytes_count() as i64
        );
    }

    #[test]
    fn test_wal_writer_write_delete_single() {
        // create wal test dir
        let dir = must_temp_wal_dir();

        let mut entry = DeleteWalEntry {
            keys: vec!["cpu".as_bytes().to_vec()],
            size: 0, // size will be updated in marshal_size
        };

        // create segment writer
        let f = must_write_temp_file(dir.path());
        let mut w = TSMWALSegmentWriter::new(
            // Arc::new(dir.path().into()),
            Arc::new(dir.path().into()),
            10 * 1024 * 1024,
            0,
            BufWriter::new(f),
            Arc::new(crossbeam_channel::bounded(1024)),
            0, // the new segment writer's current pos is 0
        );

        let stats = Arc::new(WalStatistics::default());
        let compressed = must_marshal_entry(&mut entry);
        assert!(
            w.write_entry(
                DELETE_WAL_ENTRY_TYPE,
                compressed.as_slice(),
                Arc::clone(&stats)
            )
            .is_ok()
        );

        // don't test channel result, because we just test write/read from file.
        // assert!(matches!(sync_rx.recv().unwrap(), None));  // write success return Some(None)

        w.sync();
        drop(w);

        // create segment reader
        let f = must_read_temp_file(dir.path());
        let mut r = WalSegmentReader::new(&f);

        let e = r.next();
        assert!(e.is_some());

        // convert trait object to struct
        let e = e.unwrap();
        let e = match e {
            Ok(Entry::DeleteWalEntry(e)) => e,
            _ => panic!("invalid wal entry: {:?}", e),
        };

        // check if keys len is equal
        assert_eq!(e.keys.len(), entry.keys.len());
        // check if key is match
        assert_eq!(e.keys[0], entry.keys[0]);

        assert_eq!(
            must_read_file_size(&dir.path().join("00001.wal")),
            r.read_bytes_count() as i64
        );
    }

    #[test]
    fn test_wal_writer_write_multi_delete_multiple() {
        // create wal test dir
        let dir = must_temp_wal_dir();

        // create point values
        let p1 = Value::BoolValue(BoolValue::new(1, true));

        let values: HashMap<String, Vec<Value>> =
            HashMap::from([("cpu,host=A#!~#value".to_string(), vec![p1])]);

        let mut write_entry = WriteWalEntry {
            values: values.clone(),
            size: 0, // size will be updated in marshal_size
        };

        // create segment writer
        let f = must_write_temp_file(dir.path());
        let mut w = TSMWALSegmentWriter::new(
            // Arc::new(dir.path().into()),
            Arc::new(dir.path().into()),
            10 * 1024 * 1024,
            0,
            BufWriter::new(f),
            Arc::new(crossbeam_channel::bounded(1024)),
            0, // the new segment writer's current pos is 0
        );

        let stats = Arc::new(WalStatistics::default());
        let compressed = must_marshal_entry(&mut write_entry);
        assert!(
            w.write_entry(
                WRITE_WAL_ENTRY_TYPE,
                compressed.as_slice(),
                Arc::clone(&stats)
            )
            .is_ok()
        );
        w.sync();

        // Write delete entry
        let mut delete_entry = DeleteWalEntry {
            keys: vec!["cpu,host=A#!~value".as_bytes().to_vec()],
            size: 0, // size will be updated in marshal_size
        };
        let compressed = must_marshal_entry(&mut delete_entry);
        assert!(
            w.write_entry(
                DELETE_WAL_ENTRY_TYPE,
                compressed.as_slice(),
                Arc::clone(&stats)
            )
            .is_ok()
        );
        w.sync();
        drop(w);

        // create segment reader
        let f = must_read_temp_file(dir.path());
        let mut r = WalSegmentReader::new(&f);

        // Read the write points first
        let e = r.next();
        assert!(e.is_some());

        // convert trait object to struct
        let e = e.unwrap();
        let e = match e {
            Ok(Entry::WriteWalEntry(e)) => e,
            _ => panic!("invalid wal entry: {:?}", e),
        };

        for (k, v) in &e.values {
            // assert values length match
            assert_eq!(v.len(), values[k].len());
            // assert value is equal
            for (i, vv) in v.iter().enumerate() {
                assert_eq!(vv.to_string(), values[k][i].to_string());
            }
        }

        // Read the delete points second
        let e = r.next();
        assert!(e.is_some());

        // convert trait object to struct
        let e = e.unwrap();
        let e = match e {
            Ok(Entry::DeleteWalEntry(e)) => e,
            _ => panic!("invalid wal entry: {:?}", e),
        };

        // check if keys len is equal
        assert_eq!(e.keys.len(), delete_entry.keys.len());
        // check if key is match
        assert_eq!(e.keys[0], delete_entry.keys[0]);

        // check segment reader bytes count is equal to file read bytes count
        assert_eq!(
            must_read_file_size(&dir.path().join("00001.wal")),
            r.read_bytes_count() as i64
        );
    }

    #[test]
    fn test_wal_writer_write_multi_delete_range_multiple() {
        // create wal test dir
        let dir = must_temp_wal_dir();

        // create point values
        let p1 = Value::FloatValue(FloatValue::new(1, 1.0));
        let p2 = Value::FloatValue(FloatValue::new(2, 2.0));
        let p3 = Value::FloatValue(FloatValue::new(3, 3.0));

        let values: HashMap<String, Vec<Value>> =
            HashMap::from([("cpu,host=A#!~#value".to_string(), vec![p1, p2, p3])]);

        // create segment writer
        let f = must_write_temp_file(dir.path());
        let mut w = TSMWALSegmentWriter::new(
            // Arc::new(dir.path().into()),
            Arc::new(dir.path().into()),
            10 * 1024 * 1024,
            0,
            BufWriter::new(f),
            Arc::new(crossbeam_channel::bounded(1024)),
            0, // the new segment writer's current pos is 0
        );

        let mut write_entry = WriteWalEntry {
            values: values.clone(),
            size: 0, // size will be updated in marshal_size
        };

        let stats = Arc::new(WalStatistics::default());
        let compressed = must_marshal_entry(&mut write_entry);
        assert!(
            w.write_entry(
                WRITE_WAL_ENTRY_TYPE,
                compressed.as_slice(),
                Arc::clone(&stats)
            )
            .is_ok()
        );
        w.sync();

        // Write delete range entry
        let mut delete_entry = DeleteRangeWalEntry {
            keys: vec!["cpu,host=A#!~value".as_bytes().to_vec()],
            min: 2,
            max: 3,
            size: 0, // size will be updated in marshal_size
        };
        let compressed = must_marshal_entry(&mut delete_entry);
        assert!(
            w.write_entry(
                DELETE_RANGE_WAL_ENTRY_TYPE,
                compressed.as_slice(),
                Arc::clone(&stats)
            )
            .is_ok()
        );
        w.sync();
        drop(w);

        // create segment reader
        let f = must_read_temp_file(dir.path());
        let mut r = WalSegmentReader::new(&f);

        // Read the write points first
        let e = r.next();
        assert!(e.is_some());

        // convert trait object to struct
        let e = e.unwrap();
        let e = match e {
            Ok(Entry::WriteWalEntry(e)) => e,
            _ => panic!("invalid wal entry: {:?}", e),
        };

        for (k, v) in &e.values {
            // assert values length match
            assert_eq!(v.len(), values[k].len());
            // assert value is equal
            for (i, vv) in v.iter().enumerate() {
                assert_eq!(vv.to_string(), values[k][i].to_string());
            }
        }

        // Read the delete points second
        let e = r.next();
        assert!(e.is_some());

        // convert trait object to struct
        let e = e.unwrap();
        let e = match e {
            Ok(Entry::DeleteRangeWalEntry(e)) => e,
            _ => panic!("invalid wal entry: {:?}", e),
        };

        // check if keys len is equal
        assert_eq!(e.keys.len(), delete_entry.keys.len());
        // check if key is match
        assert_eq!(e.keys[0], delete_entry.keys[0]);

        // check segment reader bytes count is equal to file read bytes count
        assert_eq!(
            must_read_file_size(&dir.path().join("00001.wal")),
            r.read_bytes_count() as i64
        );
    }

    #[test]
    fn test_wal_closed_segment() {
        // create wal test dir
        let dir = Arc::new(must_temp_wal_dir().into_path());

        let mut w = WAL::TSMWAL(
            wal_impl::WAL::new(Arc::clone(&dir), DEFAULT_SEGMENT_SIZE, Duration::ZERO).unwrap(),
        );

        let files = w.closed_segments().unwrap();
        assert!(
            files.is_empty(),
            "close segment length mismatch: got {}, exp {}",
            files.len(),
            0
        );

        block_on_global(w.write_multi(HashMap::from([(
            "cpu,host=A#!~#value".to_string(),
            vec![Value::FloatValue(FloatValue::new(1, 1.0))],
        )])))
        .unwrap();
        w.close();

        // Re-open the WAL
        w = WAL::TSMWAL(
            wal_impl::WAL::new(Arc::clone(&dir), DEFAULT_SEGMENT_SIZE, Duration::ZERO).unwrap(),
        );
        let files = w.closed_segments().unwrap();
        assert!(
            files.is_empty(),
            "close segment length mismatch: got {}, exp {}",
            files.len(),
            0
        );

        w.close();
    }

    #[test]
    fn test_wal_delete() {
        // create wal test dir
        let dir = Arc::new(must_temp_wal_dir().into_path());

        let mut w = WAL::TSMWAL(
            wal_impl::WAL::new(Arc::clone(&dir), DEFAULT_SEGMENT_SIZE, Duration::ZERO).unwrap(),
        );

        let files = w.closed_segments().unwrap();
        assert!(
            files.is_empty(),
            "close segment length mismatch: got {}, exp {}",
            files.len(),
            0
        );

        block_on_global(w.delete(vec!["cpu".as_bytes().to_vec()])).unwrap();

        w.close();

        // Re-open the WAL
        w = WAL::TSMWAL(
            wal_impl::WAL::new(Arc::clone(&dir), DEFAULT_SEGMENT_SIZE, Duration::ZERO).unwrap(),
        );
        let files = w.closed_segments().unwrap();
        assert!(
            files.is_empty(),
            "close segment length mismatch: got {}, exp {}",
            files.len(),
            0
        );

        w.close();
    }

    #[test]
    fn test_wal_writer_corrupt() {
        // create wal test dir
        let dir = must_temp_wal_dir();

        // create segment writer
        let f = must_write_temp_file(dir.path());
        let mut w = TSMWALSegmentWriter::new(
            // Arc::new(dir.path().into()),
            Arc::new(dir.path().into()),
            10 * 1024 * 1024,
            0,
            BufWriter::new(f),
            Arc::new(crossbeam_channel::bounded(1024)),
            0, // the new segment writer's current pos is 0
        );

        // Write correct points
        let p1 = Value::FloatValue(FloatValue::new(1, 1.1));
        let values: HashMap<String, Vec<Value>> =
            HashMap::from([("cpu,host=A#!~#float".to_string(), vec![p1])]);

        let mut entry = WriteWalEntry {
            values,
            size: 0, // size will be updated in marshal_size
        };

        let stats = Arc::new(WalStatistics::default());
        let compressed = must_marshal_entry(&mut entry);
        assert!(
            w.write_entry(
                WRITE_WAL_ENTRY_TYPE,
                compressed.as_slice(),
                Arc::clone(&stats)
            )
            .is_ok()
        );
        w.sync();
        drop(w);

        // Write some random bytes to the file to simulate corruption.
        let corruption: [u8; 5] = [1, 4, 0, 0, 0];
        // use File to write corruption
        let mut f = must_write_temp_file(dir.path());
        assert!(f.seek(SeekFrom::End(0)).is_ok());
        assert!(f.write_all(&corruption).is_ok());

        // create segment reader
        let f = must_read_temp_file(dir.path());
        let mut r = WalSegmentReader::new(&f);

        // Try to decode two entries.
        // decode the write points first
        let e = r.next();
        assert!(e.is_some());
        // convert trait object to struct
        let e = e.unwrap();
        let e = match e {
            Ok(Entry::WriteWalEntry(e)) => e,
            _ => panic!("invalid wal entry: {:?}", e),
        };

        // decode the corruption second
        let e = r.next();
        assert!(e.is_none()); // when try to decode the second entry, it will failed to decode,
        // because the second entry is really a bad entry

        // read_bytes_count should only return size of valid data.
        let read_bytes_count = r.read_bytes_count() as i64;
        let exp_bytes_count =
            must_read_file_size(&dir.path().join("00001.wal")) - corruption.len() as i64;
        assert_eq!(
            read_bytes_count, exp_bytes_count,
            "wrong count of bytes read, got {}, exp {}",
            read_bytes_count, exp_bytes_count
        );
    }

    #[test]
    fn test_wal_segment_reader_corrupt() {
        // create wal test dir
        let dir = must_temp_wal_dir();

        // create segment writer
        let f = must_write_temp_file(dir.path());
        let mut w = TSMWALSegmentWriter::new(
            // Arc::new(dir.path().into()),
            Arc::new(dir.path().into()),
            10 * 1024 * 1024,
            0,
            BufWriter::new(f),
            Arc::new(crossbeam_channel::bounded(1024)),
            0, // the new segment writer's current pos is 0
        );

        // Write correct points
        let p4 = Value::StringValue(StringValue::new(1, "string".to_string()));
        let values: HashMap<String, Vec<Value>> =
            HashMap::from([("cpu,host=A#!~#string".to_string(), vec![p4.clone(), p4])]);

        let mut entry = WriteWalEntry {
            values: values.clone(),
            size: 0, // size will be updated in marshal_size
        };

        let stats = Arc::new(WalStatistics::default());
        let mut compressed = must_marshal_entry(&mut entry);
        // This causes the nvals field to overflow on 32 bit systems which produces a
        // negative count and a panic when reading the segment.
        compressed[25] = 255;

        assert!(
            w.write_entry(
                WRITE_WAL_ENTRY_TYPE,
                compressed.as_slice(),
                Arc::clone(&stats)
            )
            .is_ok()
        );
        w.sync();
        drop(w);

        // Write some random bytes to the file to simulate corruption.
        let corruption: [u8; 5] = [1, 4, 0, 0, 0];
        // use File to write corruption
        let mut f = must_write_temp_file(dir.path());
        assert!(f.seek(SeekFrom::End(0)).is_ok());
        assert!(f.write_all(&corruption).is_ok());

        // create segment reader
        let f = must_read_temp_file(dir.path());
        let r = WalSegmentReader::new(&f);

        // Try to decode two entries.
        // when iter the first entry, snappy.decode will failed because of compressed[25] = 255; but the
        // next() will return None, so here will never come into the for loop.
        for e in r {
            // segment reader has Impl Iterator, so r is an iterator.
            // we can use for loop to enumerate the entries in the reader
            let e = match e {
                Ok(Entry::WriteWalEntry(e)) => e,
                _ => panic!("invalid wal entry: {:?}", e),
            };
            for (k, v) in &e.values {
                // assert values length match
                assert_eq!(v.len(), values[k].len());
                // assert value is equal
                for (i, vv) in v.iter().enumerate() {
                    assert_eq!(vv.to_string(), values[k][i].to_string());
                }
            }
        }
    }

    #[test]
    fn test_wal_roll_segment() {
        // create wal test dir
        let dir = Arc::new(must_temp_wal_dir().into_path());

        const SEG_SIZE: i64 = 1024;
        let w =
            WAL::TSMWAL(wal_impl::WAL::new(Arc::clone(&dir), SEG_SIZE, Duration::ZERO).unwrap());

        let values = HashMap::from([
            (
                "cpu,host=A#!~#value".to_string(),
                vec![Value::FloatValue(FloatValue::new(1, 1.0))],
            ),
            (
                "cpu,host=B#!~#value".to_string(),
                vec![Value::FloatValue(FloatValue::new(1, 1.0))],
            ),
            (
                "cpu,host=C#!~#value".to_string(),
                vec![Value::FloatValue(FloatValue::new(1, 1.0))],
            ),
        ]);

        block_on_global(w.write_multi(values.clone())).unwrap();
        w.flush();

        let mut files = fs::read_dir(w.path()).unwrap();
        let file_count = files.count();
        assert!(file_count == 1, "unexpected segments size {}", file_count);
        let mut files = fs::read_dir(w.path()).unwrap();
        let encode_size = files.next().unwrap().unwrap().metadata().unwrap().size();
        // write some values, the total size of these values will exceed SEG_SIZE(1024),
        // so roll_segment will be triggered
        for i in 0..100 {
            block_on_global(w.write_multi(values.clone())).unwrap();
        }
        let files = fs::read_dir(w.path()).unwrap();
        for f in files {
            let f = f.unwrap();
            let file_size = f.metadata().unwrap().size();
            assert!(
                file_size <= SEG_SIZE as u64 + encode_size,
                "unexpected segment size {}",
                file_size
            );
        }

        w.close();
    }

    // TODO:
    // func TestWAL_DiskSize(t *testing.T) {}

    #[test]
    fn test_wal_write_wal_segment_unmarshal_binary_write_wal_corrupt() {
        // create point values
        let p1 = Value::FloatValue(FloatValue::new(1, 1.1));
        let p2 = Value::IntegerValue(IntegerValue::new(1, 1i64));
        let p3 = Value::BoolValue(BoolValue::new(1, true));
        let p4 = Value::StringValue(StringValue::new(1, "string".to_string()));
        let p5 = Value::UnsignedValue(UnsignedValue::new(1, 1u64));

        let values: HashMap<String, Vec<Value>> = HashMap::from([
            ("cpu,host=A#!~#float".to_string(), vec![p1]),
            ("cpu,host=A#!~#int".to_string(), vec![p2]),
            ("cpu,host=A#!~#bool".to_string(), vec![p3]),
            ("cpu,host=A#!~#string".to_string(), vec![p4]),
            ("cpu,host=A#!~#unsigned".to_string(), vec![p5]),
        ]);

        let mut entry = WriteWalEntry {
            values,
            size: 0, // size will be updated in marshal_size
        };

        let b = entry.marshal_binary();
        assert!(b.is_ok(), "marshal binary error");

        // Test every possible truncation of a write WAL entry
        let mut i = 0;
        let b = b.unwrap();
        while i < b.len() {
            let mut truncated = vec![0u8; i];
            truncated.copy_from_slice(&b[..i]);

            // unmarshal binary failed, the error must be WALCorrupted
            if let Err(e) = entry.unmarshal_binary(&truncated) {
                assert!(matches!(e, Error::WALCorrupted));
            }

            i += 1;
        }
    }

    #[test]
    fn test_wal_delete_wal_entry_unmarshal_binary() {
        struct Example {
            input: Vec<Vec<u8>>,
            output: Vec<Vec<u8>>,
        }

        let examples = vec![
            Example {
                input: vec![vec![]],
                output: vec![],
            },
            Example {
                input: vec!["foo".as_bytes().to_vec()],
                output: vec!["foo".as_bytes().to_vec()],
            },
            Example {
                input: vec!["foo".as_bytes().to_vec(), "bar".as_bytes().to_vec()],
                output: vec!["foo".as_bytes().to_vec(), "bar".as_bytes().to_vec()],
            },
            Example {
                input: vec![
                    "foo".as_bytes().to_vec(),
                    "bar".as_bytes().to_vec(),
                    "z".as_bytes().to_vec(),
                    "abc".as_bytes().to_vec(),
                ],
                output: vec![
                    "foo".as_bytes().to_vec(),
                    "bar".as_bytes().to_vec(),
                    "z".as_bytes().to_vec(),
                    "abc".as_bytes().to_vec(),
                ],
            },
            Example {
                input: vec![
                    "foo".as_bytes().to_vec(),
                    "bar".as_bytes().to_vec(),
                    "z".as_bytes().to_vec(),
                    "a".as_bytes().to_vec(),
                ],
                output: vec![
                    "foo".as_bytes().to_vec(),
                    "bar".as_bytes().to_vec(),
                    "z".as_bytes().to_vec(),
                    "a".as_bytes().to_vec(),
                ],
            },
        ];

        for example in examples.iter() {
            let mut entry = DeleteWalEntry {
                keys: example.input.clone(),
                size: 0,
            };

            let b = entry.marshal_binary();
            assert!(b.is_ok());

            let b = b.unwrap();

            let mut out = DeleteWalEntry::new();
            assert!(out.unmarshal_binary(&b).is_ok());

            assert_eq!(out.keys, example.output);
        }
    }

    #[test]
    fn test_wal_write_wal_segment_unmarshal_binary_delete_wal_corrupt() {
        let mut entry = DeleteWalEntry {
            keys: vec!["foo".as_bytes().to_vec(), "bar".as_bytes().to_vec()],
            size: 0,
        };

        let b = entry.marshal_binary();
        assert!(b.is_ok());

        let b = b.unwrap();
        let mut i = 0;
        while i < b.len() {
            // re-allocated to ensure capacity would be exceed if slicing
            let mut truncated = vec![0u8; i];
            truncated.copy_from_slice(&b[..i]);

            // unmarshal binary failed, the error must be WALCorrupted
            if let Err(e) = entry.unmarshal_binary(&truncated) {
                matches!(e, Error::WALCorrupted);
            }

            i += 1;
        }
    }

    #[test]
    fn test_wal_write_wal_segment_unmarshal_binary_delete_range_wal_corrupt() {
        let mut entry = DeleteRangeWalEntry {
            keys: vec!["foo".as_bytes().to_vec(), "bar".as_bytes().to_vec()],
            min: 1,
            max: 2,
            size: 0,
        };

        let b = entry.marshal_binary();
        assert!(b.is_ok());

        let b = b.unwrap();
        let mut i = 0;
        while i < b.len() {
            // re-allocated to ensure capacity would be exceed if slicing
            let mut truncated = vec![0u8; i];
            truncated.copy_from_slice(&b[..i]);

            // unmarshal binary failed, the error must be WALCorrupted
            if let Err(e) = entry.unmarshal_binary(&truncated) {
                assert!(matches!(e, Error::WALCorrupted));
            }

            i += 1;
        }
    }

    // ////////// wal test helper functions ////////////
    #[cfg(not(target_os = "windows"))]
    use std::os::unix::fs::MetadataExt; // metadata.size
    #[cfg(target_os = "windows")]
    use std::os::windows::fs::MetadataExt;
    use std::{
        fs::{self, File, OpenOptions},
        io,
        path::Path,
    };

    use tempfile::TempDir;

    pub fn must_temp_wal_dir() -> TempDir {
        tempfile::TempDir::with_prefix("wal-test")
            .expect("unable to create wal temporary working directory")
    }

    pub fn must_files_from_dir(dir: &Path) -> io::Result<Vec<String>> {
        let mut files: Vec<String> = fs::read_dir(dir)?
            .flat_map(|res| -> io::Result<_> { Ok(res?.path()) })
            .filter(|path| path.is_file() && path.extension() == Some("wal".as_ref()))
            .flat_map(|path| path.into_os_string().into_string())
            .collect();

        files.sort();

        Ok(files)
    }

    pub fn must_write_temp_file(dir: &Path) -> File {
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(dir.join("00001.wal"))
            .expect("unable to create temporary file")
    }

    pub fn must_read_temp_file(dir: &Path) -> File {
        OpenOptions::new()
            .read(true)
            .open(dir.join("00001.wal"))
            .expect("unable to create temporary file")
    }

    pub fn must_read_file_size(path: &Path) -> i64 {
        let stat = fs::metadata(path)
            .unwrap_or_else(|_| panic!("read `{}` meta failed.", path.to_str().unwrap()));
        stat.size() as i64
    }

    pub fn must_marshal_entry(entry: &mut impl WalEntry) -> Vec<u8> {
        let marshal_size = entry.marshal_size();
        let mut bytes = Vec::<u8>::with_capacity(marshal_size);
        entry.encode(&mut bytes).unwrap(); // encode entry into bytes

        snap::raw::Encoder::new()
            .compress_vec(&bytes)
            .expect("snap compress failed")
    }
}
