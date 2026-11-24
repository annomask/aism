#[cfg(not(target_os = "windows"))]
use std::os::unix::fs::MetadataExt; // metadata.size
#[cfg(target_os = "windows")]
use std::os::windows::fs::MetadataExt;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    collections::HashMap,
    fs, io,
    io::{Seek, Write},
    ops::DerefMut,
    path::{Path, PathBuf},
    sync::{Arc, Once, mpsc},
};

use crate::common::{rhh, to_str, to_string};
use crate::runtime::execution::{ExecutorRef, RuntimeExecutorExt, SpawnError, accepted_task_guard};
use bytes::{Buf, BufMut};
use parking_lot::{MappedRwLockReadGuard, Mutex, RwLock, RwLockReadGuard};
use snafu::{ResultExt, Snafu, ensure};
use tokio_util::sync::CancellationToken;

use crate::{
    series::{series_file::*, series_index::*, series_segment::*},
    storage::limiter,
};

const COMPACTING_EXT: &str = "compacting";

// DEFAULT_SERIES_PARTITION_COMPACT_THREAD is the number of series IDs to hold in the in-memory
// series map before compacting and rebuilding the on-disk representation.
const DEFAULT_SERIES_PARTITION_COMPACT_THREAD: i32 = 1 << 17; // 128K

/// Error type for SeriesPartition.
#[derive(Debug, Snafu)]
pub enum Error {
    /// IO error.
    #[snafu(display("tsdb: io error: {}", reason))]
    IOError { reason: io::Error },

    /// Series partition closed.
    #[snafu(display("tsdb: series partition closed"))]
    SeriesPartitionClosed,

    /// Series partition compaction cancelled.
    #[snafu(display("tsdb: series partition compaction cancelled"))]
    SeriesPartitionCompactionCancelled,

    /// Open series partition error.
    #[snafu(display("tsdb: open series partition failed: {:?}", reason))]
    OpenSeriesPartitionFailed { reason: String },

    /// Compact series index done.
    #[snafu(display("compact series index done"))]
    CompactSeriesIndexDone,

    /// Unexpected series partition log entry flag.
    #[snafu(display("unexpected series partition log entry flag: {}", flag))]
    UnexpectedSeriesPartitionLogEntryFlag { flag: u8 },

    /// Series segment error.
    #[snafu(display("tsdb: {} {}", reason, source))]
    SeriesSegment {
        source: crate::series::series_segment::Error,
        reason: String,
    },

    /// Read series key from series segments failed.
    #[snafu(display("Read series key from series segments failed"))]
    ReadSeriesKeyFromSegmentsFailed,

    /// Series index error.
    #[snafu(display("tsdb: {} {}", reason, source))]
    SeriesIndex {
        source: crate::series::series_index::Error,
        reason: String,
    },

    #[snafu(display("tsdb: series partition task scheduling failed: {}", source))]
    Execution { source: SpawnError },
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IOError { reason: err }
    }
}

/// Result type for SeriesPartition.
pub type Result<T, E = Error> = std::result::Result<T, E>;

static CLOSE: Once = Once::new();
// SeriesPartition represents a subset of series file data.
pub struct SeriesPartition {
    id: i32,
    path: Arc<PathBuf>,

    closed: bool,
    closing: CancellationToken,

    // time series
    pub series: Arc<RwLock<Series>>,
    // compactor
    compactor: Arc<RwLock<Compactor>>,

    compact_threshold: i32,
    executor: ExecutorRef,

    #[cfg(test)]
    panic_compaction_body: Arc<AtomicBool>,
}

pub struct Series {
    segments: Vec<Arc<RwLock<SeriesSegment>>>,
    pub index: Option<SeriesIndex>,
    seq: u64, // series id sequence
}

struct Compactor {
    compacting: bool,
    compaction_limiter: limiter::fixed::Fixed, // limit concurrent series file compactions
    compactions_disabled: i32,
}

pub fn new_series_partition(
    id: i32,
    path: impl Into<PathBuf>,
    max_snapshot_concurrency: i32,
    executor: ExecutorRef,
) -> SeriesPartition {
    SeriesPartition {
        id,
        path: Arc::new(path.into()),

        closed: false,
        closing: CancellationToken::new(),

        series: Arc::new(RwLock::new(Series {
            segments: vec![],
            index: None,
            seq: (id as u64) + 1u64, // series id sequence
        })),

        compactor: Arc::new(RwLock::new(Compactor {
            compacting: false,
            compaction_limiter: limiter::fixed::Fixed::new(max_snapshot_concurrency), // TODO:
            compactions_disabled: 0,
        })),

        compact_threshold: DEFAULT_SERIES_PARTITION_COMPACT_THREAD,
        executor,
        #[cfg(test)]
        panic_compaction_body: Arc::new(AtomicBool::new(false)),
    }
}

impl SeriesPartition {
    fn finish_compaction(compactor: &Arc<RwLock<Compactor>>) {
        let mut compactor = compactor.write();
        compactor.compacting = false;
        compactor.compaction_limiter.release();
    }

    /// Open memory maps the data file at the partition's path.
    pub fn open(&mut self) -> Result<()> {
        ensure!(
            !self.closed,
            OpenSeriesPartitionFailedSnafu {
                reason: "cannot reopen series partition".to_string(),
            }
        );

        // Create path if it doesn't exist.
        fs::create_dir_all(&*self.path)?;

        // Open components.
        let e = {
            self.open_segments()?;
            // TODO: the last segment is ready for write ?

            let mut series_guard = self.series.write();
            let mut index = new_series_index(self.index_path());
            index.open().context(SeriesIndexSnafu {
                reason: "open series index failed".to_string(),
            })?;
            index
                .recover(series_guard.segments.as_slice())
                .context(SeriesIndexSnafu {
                    reason: "recover series index failed".to_string(),
                })?;
            series_guard.index = Some(index);

            Ok(())
        };

        if e.is_err() {
            let _ = self.close();
            return e;
        }

        Ok(())
    }

    fn open_segments(&self) -> Result<()> {
        let fis = fs::read_dir(&*self.path)?;
        let mut segments = Vec::<PathBuf>::new();
        for entry in fis {
            let entry = entry?;
            let path = entry.path();

            if entry.file_type()?.is_file() {
                segments.push(path);
            }
        }
        segments.sort();

        let mut series_guard = self.series.write();
        for s in segments {
            if let Ok(segment_id) =
                parse_series_segment_filename(s.file_name().unwrap().to_str().unwrap())
            {
                let segment =
                    open_series_segment(segment_id, self.path.join(s.file_name().unwrap()))
                        .context(SeriesSegmentSnafu {
                            reason: "open series segment failed".to_string(),
                        })?;
                series_guard.segments.push(Arc::new(RwLock::new(segment)));
            } else {
                continue; // this is index file
            }
        }

        // find max series id by searching segments inreverse order.
        let mut i = series_guard.segments.len() as i64 - 1;
        while i >= 0 {
            let seq = series_guard.segments[i as usize].read().max_series_id();
            if seq >= series_guard.seq {
                series_guard.seq = seq + SERIES_FILE_PARTITION_N as u64;
                break;
            }
            i -= 1;
        }

        // create initial segment if none exist
        if series_guard.segments.is_empty() {
            let segment =
                create_series_segment(0, self.path.join("0000")).context(SeriesSegmentSnafu {
                    reason: "create series segment `".to_string()
                        + self
                            .path
                            .join("0000")
                            .as_path()
                            .as_os_str()
                            .to_str()
                            .unwrap()
                        + "` failed",
                })?;
            series_guard.segments.push(Arc::new(RwLock::new(segment)));
        }

        Ok(())
    }

    /// Unmaps the data files.
    pub fn close(&mut self) -> Result<()> {
        CLOSE.call_once(|| {
            self.closing.cancel(); // TODO: or canceled() ?
        });

        let mut series_guard = self.series.write();

        self.closed = true;
        let mut err = Ok(());
        for s in series_guard.segments.iter() {
            let res = s.write().close();
            if res.is_err() && err.is_ok() {
                err = res.context(SeriesSegmentSnafu {
                    reason: "close series segment failed".to_string(),
                });
            }
        }
        series_guard.segments.clear(); // TODO: has no effect on the allocated capacity of the vector.

        if series_guard.index.is_some() {
            series_guard.index.as_mut().unwrap().close();
        }

        series_guard.index = None;

        err
    }

    /// Returns the partition id.
    pub fn id(&self) -> i32 {
        self.id
    }

    /// Returns the path to the partition.
    pub fn path(&self) -> PathBuf {
        self.path.to_path_buf()
    }

    /// Returns the path the series index.
    pub fn index_path(&self) -> PathBuf {
        self.path.join("index")
    }

    pub(crate) fn segment_paths(&self) -> Vec<PathBuf> {
        let segments = self.series.read().segments.clone();
        segments.iter().map(|s| s.read().path().to_path_buf()).collect()
    }

    #[cfg(any(test, feature = "benchmarking"))]
    /// Returns the partition's index.
    pub fn index(&self) -> MappedRwLockReadGuard<SeriesIndex> {
        let guard = self.series.read();
        RwLockReadGuard::map(guard, |s| s.index.as_ref().unwrap())
    }

    #[cfg(any(test, feature = "benchmarking"))]
    /// Returns a list of partition segments.
    pub fn segments(&self) -> Vec<Arc<RwLock<SeriesSegment>>> {
        self.series.read().segments.clone()
    }

    /// Returns the size of all partitions, in bytes.
    pub fn file_size(&self) -> Result<i64> {
        let series_guard = self.series.read();
        let mut n = 0;
        for ss in series_guard.segments.iter() {
            let stat = fs::metadata(ss.read().path())?;
            n += stat.size();
        }

        Ok(n as i64)
    }

    /// Set compact threshold.
    pub fn set_compact_threshold(&mut self, threshold: i32) {
        self.compact_threshold = threshold;
    }

    /// Creates a list of series in bulk if they don't exist. The ids parameter is modified
    /// to contain series IDs for all keys belonging to this partition.
    pub fn create_series_list_if_not_exists<T: AsRef<[u8]>>(
        &self,
        keys: &[T],
        key_partition_ids: &[i32],
        ids: Arc<Mutex<Vec<u64>>>,
    ) -> Result<()> {
        let mut write_required = false;
        {
            let series_guard = self.series.read();
            let mut ids = ids.lock();
            if self.closed {
                return SeriesPartitionClosedSnafu.fail();
            }

            for (i, key) in keys.iter().enumerate() {
                if key_partition_ids[i] != self.id {
                    continue;
                }
                let id: u64 = series_guard
                    .index
                    .as_ref()
                    .unwrap()
                    .find_id_by_series_key(series_guard.segments.as_slice(), key.as_ref());
                if id == 0 {
                    write_required = true;
                    continue;
                }
                ids[i] = id;
            }
        }

        // Exit if all series for this partition already exist.
        if !write_required {
            return Ok(());
        }

        #[derive(Default, Clone)]
        struct KeyRange {
            id: u64,
            offset: i64,
        }
        let mut new_key_ranges: Vec<KeyRange> = Vec::with_capacity(keys.len());

        // Obtain write lock to create new series.
        let mut series_guard = self.series.write();
        {
            let mut ids = ids.lock();
            if self.closed {
                return SeriesPartitionClosedSnafu.fail();
            }

            // Track offsets of duplicate series.
            let mut new_ids = HashMap::<String, u64>::with_capacity(ids.len());
            for (i, _k) in keys.iter().enumerate() {
                // Skip series that don't belong to the partition or have already been created.
                if key_partition_ids[i] != self.id || ids[i] != 0 {
                    continue;
                }

                // Re-attempt lookup under write lock.
                let key = keys[i].as_ref();
                if let Some(&id) = new_ids.get(to_str!(key)) {
                    ids[i] = id;
                    if ids[i] != 0 {
                        continue;
                    }
                }
                ids[i] = series_guard
                    .index
                    .as_ref()
                    .unwrap()
                    .find_id_by_series_key(series_guard.segments.as_slice(), key);
                if ids[i] != 0 {
                    continue;
                }

                // Write to series log and save offset.
                let (id, offset) = self.insert(series_guard.deref_mut(), key)?;
                // Append new key to be added to hash map after flush.
                ids[i] = id;
                new_ids.insert(to_string!(key), id);
                new_key_ranges.push(KeyRange { id, offset });
            }
        } // TODO: ?

        // FIXME: takes 20~30us in log_file_add_series_list_100_1_1, golang takes 2~3us
        // Flush active segment writes so we can access data in mmap.
        if let Some(segment) = Self::active_segment(series_guard.deref_mut()) {
            segment.write().flush().context(SeriesSegmentSnafu {
                reason: "segment flush failed".to_string(),
            })?;
        }

        // Add keys to hash map(s).
        for key_range in new_key_ranges.iter() {
            let key: Vec<u8>;
            {
                key = self.series_key_by_offset(key_range.offset, series_guard.segments.as_slice());
            }
            {
                series_guard
                    .index
                    .as_mut()
                    .unwrap()
                    .insert(key, key_range.id, key_range.offset);
            }
        }
        // }  // TODO: ?

        // Check if we've crossed the compaction threshold.
        let mut compactor_guard = self.compactor.write();
        if Self::compactions_enabled(compactor_guard.compactions_disabled)
            && !compactor_guard.compacting
            && self.compact_threshold != 0
            && series_guard.index.as_ref().unwrap().in_mem_count()
                >= (self.compact_threshold as u64)
            && compactor_guard.compaction_limiter.try_take()
        {
            // set compacting flag
            compactor_guard.compacting = true;

            drop(series_guard); // unlock write guard first

            let series = self.series.clone();
            let cancel = self.closing.clone();
            let compactor = self.compactor.clone();
            let task_compactor = Arc::clone(&compactor);
            let cancelled_compactor = Arc::clone(&compactor);
            #[cfg(test)]
            let panic_compaction_body = Arc::clone(&self.panic_compaction_body);
            let (registration, completion) = accepted_task_guard(move || {
                Self::finish_compaction(&cancelled_compactor);
            });
            match self.executor.spawn_blocking(move || {
                #[cfg(test)]
                if panic_compaction_body.swap(false, Ordering::SeqCst) {
                    panic!("injected series partition production-body panic");
                }

                let mut sp_compactor = new_series_partition_compactor();
                sp_compactor.cancel = cancel;
                if let Err(_e) = sp_compactor.compact(series) {
                    // TODO: log
                    // log.Error("series partition compaction failed", zap.Error(err))
                }
                // logEnd()

                // let _ = sync_tr.send(());

                completion.complete_with(|| Self::finish_compaction(&task_compactor));
            }) {
                Ok(_) => {
                    drop(compactor_guard);
                    registration.accepted();
                }
                Err(source) => {
                    registration.rejected();
                    compactor_guard.compacting = false;
                    compactor_guard.compaction_limiter.release();
                    tracing::error!(
                        target: "reefsdb::tsdb::series",
                        error = %source,
                        "series partition compaction task scheduling failed"
                    );
                }
            }
        }

        Ok(())
    }

    /// Returns if the SeriesPartition is currently compacting.
    pub fn compacting(&self) -> bool {
        self.compactor.read().compacting
    }

    /// Flags a series as permanently deleted. If the series is reintroduced later
    /// then it must create a new id.
    pub fn delete_series_id(&self, id: u64) -> Result<()> {
        let mut series_guard = self.series.write();
        ensure!(!self.closed, SeriesPartitionClosedSnafu);

        // Already tombstoned, ignore.
        if series_guard.index.as_ref().unwrap().is_deleted(id) {
            return Ok(());
        }

        // Write tombstone entry.
        self.write_log_entry(
            series_guard.deref_mut(),
            append_series_entry(SERIES_ENTRY_TOMBSTONE_FLAG, id, None).as_slice(),
        )?;

        // Flush active segment write.
        if let Some(segment) = Self::active_segment(series_guard.deref_mut()) {
            segment.write().flush().context(SeriesSegmentSnafu {
                reason: "segment flush failed".to_string(),
            })?;
        }

        // Mark tombstone in memory.
        series_guard.index.as_mut().unwrap().delete(id);

        Ok(())
    }

    /// Returns true if the ID has been deleted before.
    pub fn is_deleted(&self, id: u64) -> bool {
        let series = self.series.read();
        if self.closed {
            return false;
        }

        series.index.as_ref().unwrap().is_deleted(id)
    }

    /// Returns the series key for a given id.
    pub fn series_key(&self, id: u64) -> Vec<u8> {
        if id == 0 {
            return vec![];
        }

        let series = self.series.read();
        if self.closed {
            return vec![];
        }
        self.series_key_by_offset(
            series.index.as_ref().unwrap().find_offset_by_id(id),
            series.segments.as_slice(),
        )
    }

    /// Returns the parsed series name and tags for an offset.
    pub fn series(&self, id: u64) -> Result<(Vec<u8>, Option<crate::models::points::Tags>)> {
        let key = self.series_key(id);
        if key.is_empty() {
            return Ok((vec![], None));
        }

        let (name, tags) = parse_series_key(key.as_slice()).unwrap();
        Ok((name, Some(tags)))
    }

    /// Return the series id for the series key.
    pub fn find_id_by_series_key(&self, key: &[u8]) -> u64 {
        let series_guard = self.series.read();
        if self.closed {
            return 0;
        }
        series_guard
            .index
            .as_ref()
            .unwrap()
            .find_id_by_series_key(series_guard.segments.as_slice(), key)
    }

    /// Returns the number of series.
    pub fn series_count(&self) -> u64 {
        let series_guard = self.series.read();
        if self.closed {
            return 0;
        }
        series_guard.index.as_ref().unwrap().count()
    }

    pub fn disable_compactions(&self) {
        self.compactor.write().compactions_disabled += 1;
    }

    pub fn enable_compactions(&self) {
        let mut compactor_guard = self.compactor.write();
        if Self::compactions_enabled(compactor_guard.compactions_disabled) {
            return;
        }
        compactor_guard.compactions_disabled -= 1;
    }

    pub fn compactions_enabled(compactions_disabled: i32) -> bool {
        compactions_disabled == 0
    }

    /// Returns a list of all series ids.
    pub fn append_series_ids(&self, ids: &[u64]) -> Vec<u64> {
        // TODO: shouldn't access lock ?
        let series_guard = self.series.write();
        let mut new_ids = Vec::from(ids);
        for segment in series_guard.segments.iter() {
            new_ids = segment.read().append_series_ids(new_ids.as_slice());
        }

        new_ids
    }

    // Rreturns the last segment.
    fn active_segment(series: &mut Series) -> Option<Arc<RwLock<SeriesSegment>>> {
        let segments = &series.segments;
        if segments.is_empty() {
            return None;
        }

        Some(Arc::clone(&segments[segments.len() - 1]))
    }

    fn insert(&self, series: &mut Series, key: &[u8]) -> Result<(u64, i64)> {
        let id = series.seq;

        let offset = self.write_log_entry(
            series,
            append_series_entry(SERIES_ENTRY_INSERT_FLAG, id, Some(key)).as_slice(),
        )?;

        series.seq += SERIES_FILE_PARTITION_N as u64;
        Ok((id, offset))
    }

    // Appends an entry to the end of the active segment.
    // If there is no more room in the segment then a new segment is added.
    fn write_log_entry(&self, series: &mut Series, data: &[u8]) -> Result<i64> {
        let segment = Self::active_segment(series);

        let active_seg = if segment.is_none() || !segment.as_ref().unwrap().read().can_write(data) {
            self.create_segment(series)?
        } else {
            segment.unwrap()
        };

        let offset = active_seg
            .write()
            .write_log_entry(data)
            .context(SeriesSegmentSnafu {
                reason: "write series segment log entry failed".to_string(),
            })?;

        Ok(offset)
    }

    // Appends a new segment
    fn create_segment(&self, series: &mut Series) -> Result<Arc<RwLock<SeriesSegment>>> {
        // Close writer for active segment, if one exists.
        let segment = Self::active_segment(series);
        if let Some(seg) = segment {
            let _ = seg.write().close_for_write().context(SeriesSegmentSnafu {
                reason: "close series segment failed".to_string(),
            });
        }

        // Generate a new sequential segment identifier.
        let mut id = 0u16;
        if !series.segments.is_empty() {
            id = series.segments[series.segments.len() - 1].read().id() + 1;
        }
        let filename = format!("{:04}", id);

        // Generate new empty segment (already allow to write).
        let segment = Arc::new(RwLock::new(
            create_series_segment(id, self.path.join(filename)).context(SeriesSegmentSnafu {
                reason: "create series segment failed".to_string(),
            })?,
        ));
        series.segments.push(Arc::clone(&segment));
        Ok(segment)
    }

    fn series_key_by_offset(
        &self,
        offset: i64,
        segments: &[Arc<RwLock<SeriesSegment>>],
    ) -> Vec<u8> {
        if offset == 0 {
            return vec![];
        }

        let (segment_id, pos) = split_series_offset(offset);
        for segment in segments {
            let segment = segment.read();
            if segment.id() != segment_id {
                continue;
            }

            if let Ok((key, _r)) =
                read_series_key(segment.slice(pos + SERIES_ENTRY_HEADER_SIZE as u32))
            {
                // (key, reminder)
                return key.to_vec();
            }
        }

        vec![]
    }
}

// SeriesPartitionCompactor represents an object reindexes a series partition
// and optionally compacts segments.
pub struct SeriesPartitionCompactor {
    cancel: CancellationToken,
}

pub fn new_series_partition_compactor() -> SeriesPartitionCompactor {
    SeriesPartitionCompactor {
        cancel: CancellationToken::new(),
    }
}

impl SeriesPartitionCompactor {
    /// Rebuilds the series partition index.
    pub fn compact(&self, series: Arc<RwLock<Series>>) -> Result<()> {
        // Snapshot the partitions and index so we can check tombstones and replay at the end under lock.

        let series_r = series.read();
        let segments = clone_series_segments(series_r.segments.as_slice());
        let index = series_r
            .index
            .as_ref()
            .unwrap()
            .clone_series_index()
            .unwrap();
        let series_n = series_r.index.as_ref().unwrap().count();
        drop(series_r);

        // Compact index to a temporary location.
        let mut index_path = index.path();
        let _ = index_path.set_extension(COMPACTING_EXT);
        self.compact_index_to(&index, series_n, segments.as_slice(), &index_path)?;

        // Swap compacted index under lock & replay since compaction.
        {
            let mut series_w = series.write();
            // Reopen index with new file.
            series_w.index.as_mut().unwrap().close();
            fs::rename(index_path, index.path())?;
            series_w
                .index
                .as_mut()
                .unwrap()
                .open()
                .context(SeriesIndexSnafu {
                    reason: "open series index failed".to_string(),
                })?;

            // Replay new entries.
            let series = series_w.deref_mut();
            let segments = &series.segments;
            let index = series.index.as_mut();
            index
                .unwrap()
                .recover(segments.as_slice())
                .context(SeriesIndexSnafu {
                    reason: "recover series index failed".to_string(),
                })?;

            Ok(())
        }
    }

    fn compact_index_to(
        &self,
        index: &SeriesIndex,
        series_n: u64,
        segments: &[Arc<RwLock<SeriesSegment>>],
        path: &Path,
    ) -> Result<()> {
        let mut hdr = new_series_index_header();
        hdr.count = series_n;
        hdr.capacity = rhh::pow2(((hdr.count as i64) * 100) / (SERIES_INDEX_LOAD_FACTOR as i64));

        // Allocate space for maps.
        // let mut key_id_map = HashMap::<String, u64>::with_capacity(hdr.capacity);
        // let mut id_offset_map = HashMap::<u64, i64>::with_capacity(hdr.capacity);

        let mut key_id_map = vec![0u8; (hdr.capacity * (SERIES_INDEX_ELEM_SIZE as i64)) as usize];
        let mut id_offset_map =
            vec![0u8; (hdr.capacity * (SERIES_INDEX_ELEM_SIZE as i64)) as usize];

        // Rdindex all partitions.
        let mut entry_n = 0i32;
        for segment in segments {
            // CompactSeriesIndexDone
            // let e = segment.for_each_entry(
            let e = segment.read().for_each_entry::<_, Error>(
                |flag: u8, id: u64, offset: i64, key: &[u8]| -> Result<()> {
                    // Make sure we don't go past the offset where the compaction began.
                    if offset > index.max_offset {
                        return CompactSeriesIndexDoneSnafu.fail();
                    }

                    // Check for cancellation periodically.
                    entry_n += 1;
                    if entry_n % 1000 == 0 && self.cancel.is_cancelled() {
                        return SeriesPartitionCompactionCancelledSnafu.fail();
                    }

                    // Only process insert entries.
                    match flag {
                        SERIES_ENTRY_INSERT_FLAG => {}
                        SERIES_ENTRY_TOMBSTONE_FLAG => return Ok(()),
                        _ => return UnexpectedSeriesPartitionLogEntryFlagSnafu { flag }.fail(),
                    }

                    // Save max series identifier processed.
                    hdr.max_series_id = id;
                    hdr.max_offset = offset;

                    // Ignore entry if tombstoned.
                    if index.is_deleted(id) {
                        return Ok(());
                    }

                    // Insert into maps.
                    self.insert_id_offset_map(&mut id_offset_map, hdr.capacity, id, offset);
                    self.insert_key_id_map(&mut key_id_map, hdr.capacity, segments, key, offset, id)
                },
            );
            if e.is_err() {
                match e.err().unwrap() {
                    Error::CompactSeriesIndexDone => break,
                    e => return Err(e),
                }
            }
        }

        // Open file handler.
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;

        // Calculate map positions.
        (hdr.key_id_map.offset, hdr.key_id_map.size) =
            (SERIES_INDEX_HEADER_SIZE as i64, key_id_map.len() as i64);
        (hdr.id_offset_map.offset, hdr.id_offset_map.size) = (
            hdr.key_id_map.offset + hdr.key_id_map.size,
            id_offset_map.len() as i64,
        );

        // Write header.
        hdr.write_to(&mut f).context(SeriesIndexSnafu {
            reason: "write index header to index file failed".to_string(),
        })?;

        // Write maps.
        f.write_all(&key_id_map)?;
        f.write_all(&id_offset_map)?;

        // Sync & close.
        f.sync_all()?;

        Ok(())
    }

    fn insert_key_id_map(
        &self,
        dst: &mut [u8],
        capacity: i64,
        segments: &[Arc<RwLock<SeriesSegment>>],
        key: &[u8],
        offset: i64,
        id: u64,
    ) -> Result<()> {
        let mask = capacity - 1;
        let hash = rhh::hash_key(key);

        // Continue searching until we find an empty slot or lower probe distance.
        let mut offset = offset;
        let mut id = id;
        let (mut i, mut dist, mut pos) = (0i64, 0i64, hash & mask);
        loop {
            assert!(i <= capacity, "key/id map full");
            let elem = &mut dst[(pos * SERIES_INDEX_ELEM_SIZE as i64) as usize..];

            // If empty slot found or matching offset, insert and exit.
            let elem_offset = (&elem[..8]).get_u64() as i64; // big endian
            let elem_id = (&elem[8..]).get_u64();
            if elem_offset == 0 || elem_offset == offset {
                (&mut elem[..8]).put_u64(offset as u64);
                (&mut elem[8..]).put_u64(id);
                return Ok(());
            }

            // Read key at position & hash.
            let elem_key = read_series_key_from_segments(
                segments,
                elem_offset + SERIES_ENTRY_HEADER_SIZE as i64,
            )
            .ok_or(ReadSeriesKeyFromSegmentsFailedSnafu);

            ensure!(elem_key.is_ok(), ReadSeriesKeyFromSegmentsFailedSnafu);
            let elem_key = elem_key.unwrap();
            let elem_hash = rhh::hash_key(&elem_key);

            // If the existing elem has probed less than us, then swap places with
            // existing elem, and keep going to find another slot for that elem.
            let d = rhh::dist(elem_hash, pos, capacity);
            if d < dist {
                // Insert current values.
                (&mut elem[..8]).put_u64(offset as u64);
                (&mut elem[8..]).put_u64(id);

                // Swap with values in that position.
                (_, _, offset, id) = (elem_hash, elem_key, elem_offset, elem_id);

                // Update current distance.
                dist = d;
            }

            i += 1;
            dist += 1;
            pos = (pos + 1) & mask;
        }
    }

    fn insert_id_offset_map(&self, dst: &mut [u8], capacity: i64, id: u64, offset: i64) {
        let mask = capacity - 1;
        let hash = rhh::hash_u64(id);

        let mut offset = offset;
        let mut id = id;
        let (mut i, mut dist, mut pos) = (0i64, 0i64, hash & mask);
        loop {
            assert!(i <= capacity, "key/id map full");
            let elem = &mut dst[(pos * SERIES_INDEX_ELEM_SIZE as i64) as usize..];

            // If empty slot found or matching id, insert and exit.
            let elem_id = (&elem[..8]).get_u64();
            let elem_offset = (&elem[8..]).get_u64() as i64;
            if elem_offset == 0 || elem_offset == offset {
                (&mut elem[..8]).put_u64(id);
                (&mut elem[8..]).put_u64(offset as u64);
                return;
            }

            // Hash key.
            let elem_hash = rhh::hash_u64(elem_id);

            // If the existing elem has probed less than us, then swap places with
            // existing elem, and keep going to find another slot for that elem.
            let d = rhh::dist(elem_hash, pos, capacity);
            if d < dist {
                // Insert current values.
                (&mut elem[..8]).put_u64(id);
                (&mut elem[8..]).put_u64(offset as u64);

                // Swap with values in that position.
                (_, id, offset) = (elem_hash, elem_id, elem_offset);

                // Update current distance.
                dist = d;
            }

            i += 1;
            dist += 1;
            pos = (pos + 1) & mask;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use crate::runtime::execution::{
        BoxBlockingTask, BoxFutureTask, ExecutorRef, RuntimeExecutor, SpawnError, TaskController,
    };
    use parking_lot::Mutex;

    use super::{COMPACTING_EXT, SeriesPartition, new_series_partition};

    #[derive(Clone, Copy)]
    enum TaskDisposition {
        Run = 0,
        Refuse = 1,
        Drop = 2,
        Panic = 3,
    }

    struct CompactionLifecycleExecutor {
        disposition: AtomicUsize,
        submissions: Arc<AtomicUsize>,
    }

    impl CompactionLifecycleExecutor {
        fn new(disposition: TaskDisposition) -> Self {
            Self {
                disposition: AtomicUsize::new(disposition as usize),
                submissions: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn run_next(&self) {
            self.disposition
                .store(TaskDisposition::Run as usize, Ordering::SeqCst);
        }
    }

    impl RuntimeExecutor for CompactionLifecycleExecutor {
        fn schedule(&self, _task: BoxFutureTask) -> Result<TaskController, SpawnError> {
            Err(SpawnError::rejected("unused async lane"))
        }

        fn schedule_blocking(&self, task: BoxBlockingTask) -> Result<TaskController, SpawnError> {
            self.submissions.fetch_add(1, Ordering::SeqCst);
            match self
                .disposition
                .swap(TaskDisposition::Run as usize, Ordering::SeqCst)
            {
                value if value == TaskDisposition::Refuse as usize => {
                    Err(SpawnError::rejected("injected series compaction refusal"))
                }
                value if value == TaskDisposition::Drop as usize => {
                    drop(task);
                    Ok(TaskController::detached())
                }
                value if value == TaskDisposition::Panic as usize => {
                    std::thread::spawn(move || {
                        let _task = task;
                        panic!("injected accepted series compaction panic");
                    });
                    Ok(TaskController::detached())
                }
                _ => {
                    std::thread::spawn(task);
                    Ok(TaskController::detached())
                }
            }
        }

        fn schedule_compact(&self, _task: BoxFutureTask) -> Result<TaskController, SpawnError> {
            Err(SpawnError::rejected("unused compact async lane"))
        }

        fn schedule_blocking_compact(
            &self,
            _task: BoxBlockingTask,
        ) -> Result<TaskController, SpawnError> {
            Err(SpawnError::rejected("unused compact blocking lane"))
        }
    }

    fn wait_for_idle(partition: &SeriesPartition) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while partition.compacting() {
            assert!(
                Instant::now() < deadline,
                "series compaction did not become idle"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn assert_compaction_resources_restored(partition: &SeriesPartition) {
        let compactor = partition.compactor.read();
        assert!(!compactor.compacting);
        assert_eq!(
            compactor.compaction_limiter.available() as i64,
            compactor.compaction_limiter.capacity()
        );
    }

    fn insert_series(partition: &SeriesPartition, key: &[u8]) {
        partition
            .create_series_list_if_not_exists(
                &[key],
                &[partition.id()],
                Arc::new(Mutex::new(vec![0])),
            )
            .unwrap();
    }

    fn assert_terminal_cleanup_and_retry(
        disposition: TaskDisposition,
        force_operation_error: bool,
        panic_production_body: bool,
    ) {
        let root = tempfile::tempdir().unwrap();
        let concrete = Arc::new(CompactionLifecycleExecutor::new(disposition));
        let executor: ExecutorRef = concrete.clone();
        let mut partition = new_series_partition(0, root.path(), 1, executor);
        partition.set_compact_threshold(1);
        partition.open().unwrap();

        partition
            .panic_compaction_body
            .store(panic_production_body, Ordering::SeqCst);

        let compacting_path = partition.index_path().with_extension(COMPACTING_EXT);
        if force_operation_error {
            std::fs::write(&compacting_path, []).unwrap();
        }
        insert_series(&partition, b"cpu,host=first");
        wait_for_idle(&partition);
        assert_compaction_resources_restored(&partition);

        if force_operation_error {
            std::fs::remove_file(compacting_path).unwrap();
        }
        concrete.run_next();
        insert_series(&partition, b"cpu,host=retry");
        wait_for_idle(&partition);
        assert_compaction_resources_restored(&partition);
        assert!(concrete.submissions.load(Ordering::SeqCst) >= 2);

        partition.close().unwrap();
    }

    #[test]
    fn series_compaction_refusal_restores_state_and_retries() {
        assert_terminal_cleanup_and_retry(TaskDisposition::Refuse, false, false);
    }

    #[test]
    fn series_compaction_accepted_disposal_restores_state_and_retries() {
        assert_terminal_cleanup_and_retry(TaskDisposition::Drop, false, false);
    }

    #[test]
    fn series_compaction_executor_unwind_before_body_restores_state_and_retries() {
        assert_terminal_cleanup_and_retry(TaskDisposition::Panic, false, false);
    }

    #[test]
    fn series_compaction_production_body_panic_restores_state_and_retries() {
        assert_terminal_cleanup_and_retry(TaskDisposition::Run, false, true);
    }

    #[test]
    fn series_compaction_operation_error_restores_state_and_retries() {
        assert_terminal_cleanup_and_retry(TaskDisposition::Run, true, false);
    }

    #[test]
    fn series_compaction_success_restores_state_and_allows_later_work() {
        assert_terminal_cleanup_and_retry(TaskDisposition::Run, false, false);
    }
}

// use std::collections::hash_map::DefaultHasher;
// use std::hash::{Hash, Hasher};
// pub(crate) fn hash_key<T: Hash>(t: &T) -> u64
// {
//     let mut s = DefaultHasher::new();
//     t.hash(&mut s);
//     s.finish()
// }
