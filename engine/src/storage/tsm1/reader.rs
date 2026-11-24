use std::{
    cmp::Ordering::{Equal, Greater, Less},
    collections::{HashMap, VecDeque},
    fs::File,
    io::{Read, Seek, SeekFrom},
    ops::{Deref, DerefMut},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU64, Ordering},
    },
    time::UNIX_EPOCH,
};

use crate::common::{
    mmap::{self, Mmap},
    to_str, to_string,
    wait_group::WaitGroup,
};
use crate::models::points::{
    BoolValue, FloatValue, IntegerValue, StringValue, UnsignedValue, Value,
};
use crate::runtime::execution::{
    ExecutorRef, RuntimeExecutorExt, SpawnError, TaskAdmissionGate, accepted_task_guard,
    legacy_global_executor,
};
use bytes::{Buf, BufMut};
use parking_lot::{Mutex, RwLock, lock_api::RawMutex};
use snafu::{OptionExt, ResultExt, Snafu, ensure};

use super::{
    file_store::TombstoneStat,
    tombstone::{Tombstone, Tombstoner},
};
use crate::{
    common::slow_trace,
    storage::tsm1::{
        cursor::array_values::{BoolArray, FloatArray, IntegerArray, StringArray, UnsignedArray},
        data_encoding::{
            array_encoding::{
                decode_bool_array_block, decode_float_array_block, decode_integer_array_block,
                decode_string_array_block, decode_timestamp_array_block,
                decode_unsigned_array_block,
            },
            encoding::{
                self, decode_bool_block, decode_float_block, decode_integer_block,
                decode_string_block, decode_unsigned_block,
            },
            encoding_values::Values,
        },
        file_store::FileStat,
        writer::{
            INDEX_COUNT_SIZE, INDEX_ENTRY_SIZE, INDEX_TYPE_SIZE, IndexEntries, IndexEntry,
            MAGIC_NUMBER, VERSION,
        },
    },
};

const NONE_OFFSET: [u8; 4] = [255, 255, 255, 255];

/// Error type for tsm reader.
#[derive(Debug, Snafu)]
pub enum Error {
    /// FileInUse is returned when attempting to remove or close a TSM file that is still being used.
    #[snafu(display("file still in use"))]
    FileInUse,

    /// TSMClosed is returned when performing an operation against a closed TSM file.
    #[snafu(display("tsm file closed"))]
    TSMClosed,

    /// Encoding error.
    #[snafu(display("{}", source))]
    Encoding {
        source: crate::storage::tsm1::data_encoding::encoding::Error,
    },

    /// Array encoding error.
    #[snafu(display("{}", source))]
    ArrayEncoding {
        source: crate::storage::tsm1::data_encoding::array_encoding::Error,
    },

    // /// MaxKeyLengthExceeded is returned when attempting to write a key that is too long.
    // #[snafu(display("max key length exceeded"))]
    // MaxKeyLengthExceeded,

    // /// MaxBlocksExceeded is returned when attempting to write a block past the allowed number.
    // #[snafu(display("max blocks exceeded"))]
    // MaxBlocksExceeded,
    /// IO error.
    #[snafu(display("tsm writer error: {reason}"))]
    IO { reason: std::io::Error },

    /// Tombstone error.
    #[snafu(display("{}", source))]
    Tombstone {
        source: crate::storage::tsm1::tombstone::Error,
    },

    // /// Encoding error.
    // #[snafu(display("{}", source))]
    // Encoding {
    //     source: crate::storage::tsm1::data_encoding::encoding::Error,
    // },
    /// String error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },

    /// Required batch-delete work could not be scheduled.
    #[snafu(display("{}", source))]
    Execution { source: SpawnError },

    #[snafu(display("accepted batch-delete task did not complete"))]
    TaskTerminated,
}

/// Result type for tsm reader.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IO { reason: err }
    }
}

pub struct TSMReader {
    pub generation: i64,
    pub seq: i64,
    // refs is the count of active references to this reader.
    refs: AtomicI64,
    ref_wg: WaitGroup,

    pub inner: RwLock<Inner>,

    pub delete_mu: parking_lot::RawMutex,
}

pub struct Inner {
    // accessor provides access and decoding of blocks for the reader.
    accessor: MMapAccessor, // TODO: blockAccessor interface

    // index is the index of all blocks.
    index: Arc<IndirectIndex>,

    // tombstoner ensures tombstoned keys are not available by the index.
    pub tombstoner: Tombstoner,
    // size is the size of the file on disk.
    size: u64,

    // lastModified is the last time this file was modified on disk
    last_modified: i64,
}

impl TSMReader {
    pub fn new(f: File, path: &Path, generation: i64, seq: i64) -> Result<Self> {
        // TODO: madviseWillNeed
        let stat = std::fs::metadata(path)?;
        let mut accessor = MMapAccessor {
            access_count: AtomicU64::new(0),
            inner: RwLock::new(InnerAccessor {
                free_count: AtomicU64::default(),
                mmap_will_need: false,
                path: path.to_path_buf(),
                mmap: None,
                index: Arc::new(IndirectIndex::default()),
            }),
        };
        let index = accessor.init(f)?;
        let tombstoner = Tombstoner::new(path.to_path_buf());

        let last_modified = stat
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        let inner = Inner {
            accessor,
            index,
            tombstoner,
            size: stat.size(),
            last_modified,
        };

        let r = Self {
            generation,
            seq,
            refs: AtomicI64::new(0),
            ref_wg: WaitGroup::new(),
            inner: RwLock::new(inner),
            delete_mu: parking_lot::RawMutex::INIT,
        };

        r.apply_tomestones()?;

        Ok(r)
    }

    fn apply_tomestones(&self) -> Result<()> {
        let (mut cur, mut prev) = (Tombstone::default(), Tombstone::default());
        let mut batch = Vec::<Vec<u8>>::with_capacity(4096);

        let inner = self.inner.read();
        inner
            .tombstoner
            .walk(|ts: &Tombstone| {
                cur = ts.clone();
                if !batch.is_empty() && (prev.min != cur.min || prev.max != cur.max) {
                    let keys = &mut batch.iter().map(|v| v.as_slice()).collect::<Vec<&[u8]>>();
                    inner.index.delete_range(keys, prev.min, prev.max);
                    batch.clear();
                }

                // Copy the tombstone key and re-use the buffers to avoid allocations
                let n = batch.len();
                batch.resize(n + 1, vec![]);
                // batch.truncate(n + 1);
                if batch[n].capacity() < ts.key.len() {
                    // FIXME: len or capaity ?
                    batch[n] = vec![0u8; ts.key.len()];
                } else {
                    batch[n].truncate(ts.key.len());
                }
                batch[n].clone_from(&ts.key);

                if batch.len() >= 4096 {
                    let keys = &mut batch.iter().map(|v| v.as_slice()).collect::<Vec<&[u8]>>();
                    inner.index.delete_range(keys, prev.min, prev.max);
                    batch.clear();
                }

                prev = ts.clone();
            })
            .context(TombstoneSnafu)?;

        if !batch.is_empty() {
            let keys = &mut batch.iter().map(|v| v.as_slice()).collect::<Vec<&[u8]>>();
            inner.index.delete_range(keys, cur.min, cur.max);
        }

        Ok(())
    }

    pub fn free(&self) -> Result<()> {
        let inner = self.inner.read();
        inner.accessor.free()
    }

    /// Returns the path of the file the TSMReader was initialized with.
    pub fn path(&self) -> PathBuf {
        let inner = self.inner.read();
        inner.accessor.path()
    }

    /// Returns the key and the underlying entry at the numeric index.
    pub fn key(&self, index: usize, entries: &mut [IndexEntry]) -> (Vec<u8>, u8, Vec<IndexEntry>) {
        let inner = self.inner.read();
        inner.index.key(index, entries)
    }

    /// Returns the key and key type at position idx in the index.
    pub fn key_at(&self, idx: usize) -> (Vec<u8>, u8) {
        let inner = self.inner.read();
        inner.index.key_at(idx)
    }

    pub fn seek(&self, key: &[u8]) -> usize {
        let inner = self.inner.read();
        inner.index.seek(key)
    }

    /// Returns the values corresponding to the block at the given key and timestamp.
    pub fn read(&self, key: &[u8], timestamp: i64) -> Result<Vec<Value>> {
        let inner = self.inner.read();
        inner.accessor.read(key, timestamp)
    }

    /// Returns all values for a key in all blocks.
    pub fn read_all(&self, key: &[u8]) -> Result<Vec<Value>> {
        let inner = self.inner.read();
        inner.accessor.read_all(key)
    }

    pub fn read_bytes(&self, entry: &IndexEntry, b: &[u8]) -> Result<(u32, Vec<u8>)> {
        let inner = self.inner.read();
        inner.accessor.read_bytes(entry, b)
    }

    /// Returns only the timestamps corresponding to the given index entry.
    pub fn read_timestamp_array_block_at(
        &self,
        entry: &IndexEntry,
        expected_block_type: u8,
    ) -> Result<Vec<i64>> {
        let inner = self.inner.read();
        inner
            .accessor
            .read_timestamp_array_block(entry, expected_block_type)
    }

    /// Returns the integer values corresponding to the given index entry.
    pub fn read_integer_block_at(&self, entry: &IndexEntry) -> Result<Vec<IntegerValue>> {
        let lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait("tsm_reader.read", lock_start, "stage=query_tsm_lookup");
        inner.accessor.read_integer_block(entry)
    }

    /// Fills vals with the integer values corresponding to the given index entry.
    pub fn read_integer_array_block_at(
        &self,
        entry: &IndexEntry,
        vals: &mut IntegerArray,
    ) -> Result<()> {
        let inner = self.inner.read();
        inner.accessor.read_integer_array_block(entry, vals)
    }

    /// Returns the unsigned values corresponding to the given index entry.
    pub fn read_unsigned_block_at(&self, entry: &IndexEntry) -> Result<Vec<UnsignedValue>> {
        let lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait("tsm_reader.read", lock_start, "stage=query_tsm_lookup");
        inner.accessor.read_unsigned_block(entry)
    }

    /// Fills vals with the unsigned values corresponding to the given index entry.
    pub fn read_unsigned_array_block_at(
        &self,
        entry: &IndexEntry,
        vals: &mut UnsignedArray,
    ) -> Result<()> {
        let inner = self.inner.read();
        inner.accessor.read_unsigned_array_block(entry, vals)
    }

    /// Returns the float values corresponding to the given index entry.
    pub fn read_float_block_at(&self, entry: &IndexEntry) -> Result<Vec<FloatValue>> {
        let lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait("tsm_reader.read", lock_start, "stage=query_tsm_lookup");
        inner.accessor.read_float_block(entry)
    }

    /// Fills vals with the float values corresponding to the given index entry.
    pub fn read_float_array_block_at(
        &self,
        entry: &IndexEntry,
        vals: &mut FloatArray,
    ) -> Result<()> {
        let inner = self.inner.read();
        inner.accessor.read_float_array_block(entry, vals)
    }

    /// Returns the bool values corresponding to the given index entry.
    pub fn read_bool_block_at(&self, entry: &IndexEntry) -> Result<Vec<BoolValue>> {
        let lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait("tsm_reader.read", lock_start, "stage=query_tsm_lookup");
        inner.accessor.read_bool_block(entry)
    }

    /// Fills vals with the bool values corresponding to the given index entry.
    pub fn read_bool_array_block_at(&self, entry: &IndexEntry, vals: &mut BoolArray) -> Result<()> {
        let inner = self.inner.read();
        inner.accessor.read_bool_array_block(entry, vals)
    }

    /// Returns the string values corresponding to the given index entry.
    pub fn read_string_block_at(&self, entry: &IndexEntry) -> Result<Vec<StringValue>> {
        let lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait("tsm_reader.read", lock_start, "stage=query_tsm_lookup");
        inner.accessor.read_string_block(entry)
    }

    /// Fills vals with the string values corresponding to the given index entry.
    pub fn read_string_array_block_at(
        &self,
        entry: &IndexEntry,
        vals: &mut StringArray,
    ) -> Result<()> {
        let inner = self.inner.read();
        inner.accessor.read_string_array_block(entry, vals)
    }

    /// Returns the type of values stored at the given key.
    pub fn values_type(&self, key: &[u8]) -> Result<u8> {
        let inner = self.inner.read();
        inner.index.block_type(key)
    }

    /// Close the TSMReader.
    pub fn close(&self) -> Result<()> {
        self.ref_wg.wait();

        let inner = self.inner.write();
        inner.accessor.close()?;
        inner.index.close()
    }

    /// ref_reader records a usage of this TSMReader.  If there are active references
    /// when the reader is closed or removed, the reader will remain open until
    /// there are no more references.
    pub fn ref_reader(&self) {
        let _ = self.refs.fetch_add(1, Ordering::SeqCst);
        self.ref_wg.add(1);
    }

    /// unref_reader removes a usage record of this TSMReader.  If the Reader was closed
    /// by another goroutine while there were active references, the file will
    /// be closed and remove
    pub fn unref_reader(&self) {
        let _ = self.refs.fetch_sub(1, Ordering::SeqCst);
        self.ref_wg.done();
    }

    /// Returns whether the TSMReader currently has any active references.
    pub fn in_use(&self) -> bool {
        let refs = self.refs.load(Ordering::SeqCst);
        refs > 0
    }

    /// Removes any underlying files stored on disk for this reader.
    pub fn remove(&self) -> Result<()> {
        let inner = self.inner.write();

        let path = inner.accessor.path();
        ensure!(!self.in_use(), FileInUseSnafu);

        if path.exists() {
            std::fs::remove_file(path)?;
            return Ok(());
        }

        inner.tombstoner.delete().context(TombstoneSnafu)
    }

    /// Renames the underlying file to the new path.
    pub fn rename(&self, path: &Path) -> Result<()> {
        let inner = self.inner.write();
        inner.accessor.rename(path)
    }

    /// Returns whether the given key is present in the index.
    pub fn contains(&self, key: &[u8]) -> bool {
        let inner = self.inner.read();
        inner.index.contains(key)
    }

    /// Returns true if key and time might exists in this file.  This function could
    /// return true even though the actual point does not exist.  For example, the key may
    /// exist in this file, but not have a point exactly at time t.
    pub fn contains_value(&self, key: &[u8], ts: i64) -> bool {
        let inner = self.inner.read();
        inner.index.contains_value(key, ts)
    }

    /// Removes the given points for keys between minTime and maxTime.   The series
    // keys passed in must be sorted.
    pub fn delete_range(
        r: Arc<TSMReader>,
        keys: &[&[u8]],
        min_time: i64,
        max_time: i64,
    ) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        let batch = Self::batch_delete(r);

        if let Err(e) = batch.delete_range(keys, min_time, max_time) {
            let _ = batch.roll_back();
            return Err(e);
        }

        batch.commit()
    }

    // pub fn delete_range2(&self, keys: &[&[u8]], min_time: i64, max_time: i64) -> Result<()> {
    //     if keys.is_empty() {
    //         return Ok(());
    //     }

    //     // If the keys can't exist in this TSM file, skip it.
    //     let (min_key, max_key) = (keys[0], keys[keys.len() - 1]);
    //     let inner = self.inner.read(); // FIXME: read or write ?
    //     if !inner.index.overlaps_key_range(min_key, max_key) {
    //         return Ok(());
    //     }

    //     // If the timerange can't exist in this TSM file, skip it.
    //     if !inner.index.overlaps_time_range(min_time, max_time) {
    //         return Ok(());
    //     }

    //     let (min_key, max_key) = inner.index.key_range();
    //     inner
    //         .tombstoner
    //         .add_range(
    //             keys,
    //             min_key.as_slice(),
    //             max_key.as_slice(),
    //             min_time,
    //             max_time,
    //         )
    //         .context(TombstoneSnafu)
    // }

    /// Deletes blocks indicated by keys.
    pub fn delete(&self, keys: &mut [&[u8]]) -> Result<()> {
        let inner = self.inner.read();

        inner
            .tombstoner
            .add(
                keys,
                inner.index.min_key().as_slice(),
                inner.index.max_key().as_slice(),
            )
            .context(TombstoneSnafu)?;

        inner.tombstoner.flush().context(TombstoneSnafu)?;
        inner.index.delete(keys);

        Ok(())
    }

    // pub fn commit(&self) -> Result<()> {
    //     let inner = self.inner.read();  // FIXME:  read or write ?
    //     inner.tombstoner.flush()?;

    // }

    /// Returns true if the time range of the file intersect min and max.
    pub fn overlaps_time_range(&self, min: i64, max: i64) -> bool {
        self.inner.read().index.overlaps_time_range(min, max)
    }

    /// Returns true if the key range of the file intersect min and max.
    pub fn overlaps_key_range(&self, min: &[u8], max: &[u8]) -> bool {
        self.inner.read().index.overlaps_key_range(min, max)
    }

    /// Returns the min and max time across all keys in the file.
    pub fn time_range(&self) -> (i64, i64) {
        self.inner.read().index.time_range()
    }

    /// Returns the min and max key across all keys in the file.
    pub fn key_range(&self) -> (Vec<u8>, Vec<u8>) {
        self.inner.read().index.key_range()
    }

    /// Returns the count of unique keys in the TSMReader.
    pub fn key_count(&self) -> usize {
        self.inner.read().index.key_count()
    }

    /// Returns all index entries for key.
    pub fn entries(&self, key: &[u8]) -> Vec<IndexEntry> {
        self.inner.read().index.entries(key)
    }

    /// Reads the index entries for key into entries.
    pub fn read_entries(
        &self,
        key: &[u8],
        entries: Option<&mut Vec<IndexEntry>>,
    ) -> Vec<IndexEntry> {
        self.inner.read().index.read_entries(key, entries)
    }

    /// Returns the size of the index in bytes.
    pub fn index_size(&self) -> u32 {
        let inner = self.inner.read();
        inner.index.size()
    }

    /// Returns the size of the underlying file in bytes.
    pub fn size(&self) -> u32 {
        let inner = self.inner.read();
        inner.size as u32
    }

    /// Returns the last time the underlying file was modified.
    pub fn last_modified(&self) -> i64 {
        let inner = self.inner.read();
        let mut lm = inner.last_modified;
        let ts = inner.tombstoner.tombstone_stats();
        if ts.tombstone_exists && ts.last_modified > lm {
            lm = ts.last_modified;
        }
        lm
    }

    /// Return true if there are any tombstone entries recorded.
    pub fn has_tombstones(&self) -> bool {
        let inner = self.inner.read();
        inner.tombstoner.has_tombstones()
    }

    /// Returns any tombstone files associated with this TSM file.
    pub fn tombstone_stats(&self) -> TombstoneStat {
        let inner = self.inner.read();
        inner.tombstoner.tombstone_stats()
    }

    /// Returns ranges of time that are deleted for the given key.
    pub fn tombstone_range(&self, key: &[u8]) -> Vec<TimeRange> {
        let inner = self.inner.read();
        inner.index.tombstone_range(key)
    }

    /// Returns the FileStat for the TSMReader's underlying file.
    pub fn stats(&self) -> FileStat {
        let inner = self.inner.read();
        let (min_time, max_time) = inner.index.time_range();
        let (min_key, max_key) = inner.index.key_range();
        FileStat {
            path: self.path().to_path_buf(),
            has_tombstone: inner.tombstoner.has_tombstones(),
            size: self.size(),
            last_modified: self.last_modified(),
            min_time,
            max_time,
            min_key: min_key.clone(),
            max_key: max_key.clone(),
        }
    }

    /// Returns a BlockIter for the underlying TSM file.
    pub fn block_iter(r: Arc<TSMReader>) -> BlockIter {
        let n = r.inner.read().index.key_count();
        BlockIter {
            r,
            i: 0,
            n,
            key: vec![],
            cache: vec![],
            entries: VecDeque::new(),
            err: None,
            typ: 0,
        }
    }

    pub fn batch_delete(r: Arc<TSMReader>) -> BatchDelete {
        r.delete_mu.lock();
        BatchDelete::new(r)
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum BatchDeleteDisposition {
    Active,
    Committed,
    RolledBack,
}

struct BatchDeleteInner {
    reader: Arc<TSMReader>,
    disposition: Mutex<BatchDeleteDisposition>,
}

impl Drop for BatchDeleteInner {
    fn drop(&mut self) {
        let disposition = self.disposition.get_mut();
        if *disposition != BatchDeleteDisposition::Active {
            return;
        }
        let _ = self.reader.inner.read().tombstoner.rollback();
        unsafe { self.reader.delete_mu.unlock() };
        *disposition = BatchDeleteDisposition::RolledBack;
    }
}

#[derive(Clone)]
pub struct BatchDelete {
    inner: Arc<BatchDeleteInner>,
}

impl BatchDelete {
    pub fn new(r: Arc<TSMReader>) -> Self {
        Self {
            inner: Arc::new(BatchDeleteInner {
                reader: r,
                disposition: Mutex::new(BatchDeleteDisposition::Active),
            }),
        }
    }

    pub fn delete_range(&self, keys: &[&[u8]], min_time: i64, max_time: i64) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        let disposition = self.inner.disposition.lock();
        if *disposition != BatchDeleteDisposition::Active {
            return Ok(());
        }

        // If the keys can't exist in this TSM file, skip it.
        let (min_key, max_key) = (keys[0], keys[keys.len() - 1]);
        let inner = self.inner.reader.inner.read(); // FIXME: read or write ?
        if !inner.index.overlaps_key_range(min_key, max_key) {
            return Ok(());
        }

        // If the timerange can't exist in this TSM file, skip it.
        if !inner.index.overlaps_time_range(min_time, max_time) {
            return Ok(());
        }

        let (min_key, max_key) = inner.index.key_range();
        inner
            .tombstoner
            .add_range(
                keys,
                min_key.as_slice(),
                max_key.as_slice(),
                min_time,
                max_time,
            )
            .context(TombstoneSnafu)
    }

    pub fn commit(&self) -> Result<()> {
        let mut disposition = self.inner.disposition.lock();
        if *disposition != BatchDeleteDisposition::Active {
            return Ok(());
        }
        let inner = self.inner.reader.inner.read();

        if let Err(e) = inner.tombstoner.flush() {
            let _ = inner.tombstoner.rollback();
            unsafe { self.inner.reader.delete_mu.unlock() };
            *disposition = BatchDeleteDisposition::RolledBack;
            return Err(Error::Tombstone { source: e });
        }

        if let Err(e) = self.inner.reader.apply_tomestones() {
            let _ = inner.tombstoner.rollback();
            unsafe { self.inner.reader.delete_mu.unlock() };
            *disposition = BatchDeleteDisposition::RolledBack;
            return Err(e);
        }

        unsafe { self.inner.reader.delete_mu.unlock() };
        *disposition = BatchDeleteDisposition::Committed;

        Ok(())
    }

    pub fn roll_back(&self) -> Result<()> {
        let mut disposition = self.inner.disposition.lock();
        if *disposition != BatchDeleteDisposition::Active {
            return Ok(());
        }
        if let Err(e) = self.inner.reader.inner.read().tombstoner.rollback() {
            unsafe { self.inner.reader.delete_mu.unlock() };
            *disposition = BatchDeleteDisposition::RolledBack;
            return Err(Error::Tombstone { source: e });
        }

        unsafe { self.inner.reader.delete_mu.unlock() };
        *disposition = BatchDeleteDisposition::RolledBack;

        Ok(())
    }
}

pub struct BatchDeletes {
    batches: Vec<BatchDelete>,
    executor: ExecutorRef,
}

impl Default for BatchDeletes {
    fn default() -> Self {
        Self::new_with_executor(legacy_global_executor())
    }
}

impl BatchDeletes {
    pub fn new_with_executor(executor: ExecutorRef) -> Self {
        Self {
            batches: Vec::new(),
            executor,
        }
    }

    fn run_required<F>(&self, operation: F) -> Result<()>
    where
        F: Fn(BatchDelete) -> Result<()> + Clone + Send + 'static,
    {
        let admission = TaskAdmissionGate::pending();
        let (sender, receiver) = crossbeam_channel::bounded::<Result<()>>(self.len());
        let mut submitted = 0usize;
        let mut scheduling_error = None;

        for batch in self.iter().cloned() {
            let task_batch = batch.clone();
            let cancelled_batch = batch.clone();
            let task_operation = operation.clone();
            let task_sender = sender.clone();
            let cancelled = sender.clone();
            let rejected = sender.clone();
            let task_admission = admission.clone();
            let cancelled_admission = admission.clone();
            let (registration, completion) = accepted_task_guard(move || {
                cancelled_admission.cancel();
                let error = cancelled_batch
                    .roll_back()
                    .err()
                    .unwrap_or(Error::TaskTerminated);
                let _ = cancelled.send(Err(error));
            });
            submitted += 1;

            match self.executor.spawn_blocking(move || {
                if !task_admission.wait() {
                    let error = task_batch
                        .roll_back()
                        .err()
                        .unwrap_or(Error::TaskTerminated);
                    completion.complete_with(|| {
                        let _ = task_sender.send(Err(error));
                    });
                    return;
                }
                let result = task_operation(task_batch);
                completion.complete_with(|| {
                    let _ = task_sender.send(result);
                });
            }) {
                Ok(_) => registration.accepted(),
                Err(source) => {
                    let _ = batch.roll_back();
                    let _ = rejected.send(Err(Error::Execution {
                        source: source.clone(),
                    }));
                    registration.rejected();
                    scheduling_error = Some(source);
                    admission.cancel();
                    break;
                }
            }
        }

        if scheduling_error.is_none() {
            admission.run();
        }

        let mut task_error = None;
        for _ in 0..submitted {
            match receiver.recv() {
                Ok(Err(error)) if task_error.is_none() => task_error = Some(error),
                Ok(_) => {}
                Err(_) if task_error.is_none() => task_error = Some(Error::TaskTerminated),
                Err(_) => {}
            }
        }
        if let Some(source) = scheduling_error {
            return Err(Error::Execution { source });
        }
        if let Some(error) = task_error {
            return Err(error);
        }
        Ok(())
    }

    pub fn delete_range(&self, keys: Vec<Vec<u8>>, min_time: i64, max_time: i64) -> Result<()> {
        let keys = Arc::new(keys);
        let result = self.run_required(move |batch| {
            let keys = keys.iter().map(|key| key.as_slice()).collect::<Vec<_>>();
            batch.delete_range(&keys, min_time, max_time)
        });
        if result.is_err() {
            let _ = self.roll_back();
        }
        result
    }

    pub fn commit(&self) -> Result<()> {
        let result = self.run_required(|batch| batch.commit());
        if result.is_err() {
            let _ = self.roll_back();
        }
        result
    }

    pub fn roll_back(&self) -> Result<()> {
        let mut first_error = None;
        for batch in self.iter() {
            if let Err(error) = batch.roll_back()
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

impl Deref for BatchDeletes {
    type Target = Vec<BatchDelete>;

    fn deref(&self) -> &Self::Target {
        &self.batches
    }
}

impl DerefMut for BatchDeletes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.batches
    }
}

// BlockIter allows iterating over each block in a TSM file in order.  It provides
// raw access to the block bytes without decoding them.
pub struct BlockIter {
    pub r: Arc<TSMReader>,

    // i is the current key index
    i: usize,

    // n is the total number of keys
    n: usize,

    key: Vec<u8>,
    cache: Vec<IndexEntry>,
    entries: VecDeque<IndexEntry>,
    err: Option<String>,
    typ: u8,
}

/// One encoded TSM block and the index metadata describing it.
pub struct BlockIterItem {
    pub key: Vec<u8>,
    pub min_time: i64,
    pub max_time: i64,
    pub typ: u8,
    pub checksum: u32,
    pub block: Vec<u8>,
    pub statistics: Option<crate::storage::tsm1::block_statistics::BlockStatistics>,
    pub error: Option<String>,
}

impl BlockIter {
    fn read(&self) -> BlockIterItem {
        if let Ok((checksum, buf)) = self.r.read_bytes(&self.entries[0], &[]) {
            return BlockIterItem {
                key: self.key.clone(),
                min_time: self.entries[0].min_time,
                max_time: self.entries[0].max_time,
                typ: self.typ,
                checksum,
                block: buf,
                statistics: self.entries[0].statistics,
                error: self.err.clone(),
            };
        }
        BlockIterItem {
            key: vec![],
            min_time: 0,
            max_time: 0,
            typ: 0,
            checksum: 0,
            block: vec![],
            statistics: None,
            error: None,
        }
    }

    /// Returns the next key to be iterated or an empty string.
    pub fn peek_next(&self) -> Option<Vec<u8>> {
        if self.entries.len() > 1 {
            return Some(self.key.clone());
        } else if self.n - self.i > 1 {
            let (key, _) = self.r.key_at(self.i + 1);
            return Some(key);
        }

        None
    }

    /// Returns any errors encounter during iteration.
    pub fn err(&self) -> Option<String> {
        self.err.clone()
    }
}

impl Iterator for BlockIter {
    type Item = BlockIterItem;

    fn next(&mut self) -> Option<Self::Item> {
        if self.n - self.i == 0 && self.entries.is_empty() {
            return None;
        }

        if !self.entries.is_empty() {
            let _ = self.entries.pop_front();
            if !self.entries.is_empty() {
                return Some(self.read());
            }
        }

        if self.n - self.i > 0 {
            let (key, typ, entries) = self.r.key(self.i, &mut self.cache);
            self.key = key;
            self.typ = typ;
            self.entries = {
                if entries.is_empty() {
                    VecDeque::new()
                } else {
                    VecDeque::from(entries)
                }
            };

            self.i += 1;

            // If there were deletes on the TSMReader, then our index is now off and we
            // can't proceed.  What we just read may not actually the next block.
            if self.n != self.r.key_count() {
                self.err = Some("delete during iteration".to_string());
                return None;
            }

            if !self.entries.is_empty() {
                return Some(self.read());
            }
        }

        None
    }
}

/// Accessing blocks from a TSM file.
struct MMapAccessor {
    access_count: AtomicU64, // Counter incremented everytime the mmapAccessor is accessed

    inner: RwLock<InnerAccessor>,
}

impl MMapAccessor {
    pub fn new(path: &Path, mmap_will_need: bool) -> Self {
        let inner = InnerAccessor {
            free_count: AtomicU64::new(0),
            mmap_will_need,
            path: path.to_path_buf(),
            mmap: None,
            index: Arc::new(IndirectIndex::default()),
        };

        Self {
            access_count: AtomicU64::new(0),
            inner: RwLock::new(inner),
        }
    }

    fn init(&mut self, mut f: File) -> Result<Arc<IndirectIndex>> {
        let mut inner = self.inner.write();

        vertify_version(&mut f)?;

        f.seek(SeekFrom::Start(0))?;

        let stat = f.metadata()?;
        let mmap = unsafe {
            mmap::MmapOptions::new()
                .len(stat.size() as usize)
                .map(&f)
                .expect("read-only memory map failed")
        };
        inner.mmap = Some(mmap);

        let mmap_len = inner.mmap.as_ref().unwrap().len();
        ensure!(
            mmap_len >= 8,
            StringErrSnafu {
                reason: "MMapAccessor: byte slice too small for indirectIndex".to_string(),
            }
        );

        // TODO: implement me
        // Hint to the kernel that we will be reading the file.  It would be better to hint
        // that we will be reading the index section, but that's not been
        // implemented as yet.
        // if m.mmapWillNeed {
        //     if err := madviseWillNeed(m.b); err != nil {
        //         return nil, err
        //     }
        // }

        let index_ofs_pos = mmap_len - 8;
        let index_start =
            (&inner.mmap.as_ref().unwrap()[index_ofs_pos..index_ofs_pos + 8]).get_u64();
        ensure!(
            index_start < index_ofs_pos as u64,
            StringErrSnafu {
                reason: format!(
                    "MMapAccessor: invalid index_start, index_start={}, index_ofs_pos={}",
                    index_start, index_ofs_pos
                ),
            }
        );

        let idx = Arc::new(IndirectIndex::new());
        idx.unmarshal_binary(&inner.mmap.as_ref().unwrap()[index_start as usize..index_ofs_pos])?;

        inner.index = Arc::clone(&idx);

        // Allow resources to be freed immediately if requested
        self.inc_access();
        inner
            .free_count
            .store(1, std::sync::atomic::Ordering::SeqCst);

        Ok(idx)
    }

    fn free(&self) -> Result<()> {
        let inner = self.inner.read();
        let access_count = self.access_count.load(std::sync::atomic::Ordering::SeqCst);
        let free_count = inner.free_count.load(std::sync::atomic::Ordering::SeqCst);

        // Already freed everything.
        if access_count == 0 && free_count == 0 {
            return Ok(());
        }

        // Were there accesses after the last time we tried to free?
        // If so, don't free anything and record the access count that we
        // see now for the next check.
        if access_count != free_count {
            inner
                .free_count
                .store(access_count, std::sync::atomic::Ordering::SeqCst);
            return Ok(());
        }

        // Reset both counters to zero to indicate that we have freed everything.
        self.access_count
            .store(0, std::sync::atomic::Ordering::SeqCst);
        inner
            .free_count
            .store(0, std::sync::atomic::Ordering::SeqCst);

        // TODO: madviseDontNeed(m.b)  // NO need ?

        Ok(())
    }

    fn inc_access(&self) {
        self.access_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn rename(&self, path: &Path) -> Result<()> {
        self.inc_access();

        let mut inner = self.inner.write();
        inner.mmap = None; // unmmap

        std::fs::rename(&inner.path, path)?;

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;
        file.seek(SeekFrom::Start(0))?;

        let stat = file.metadata()?;
        let mmap = unsafe {
            mmap::MmapOptions::new()
                .len(stat.size() as usize)
                .map(&file)
                .expect("read-only memory map failed")
        };
        inner.mmap = Some(mmap);

        // TODO:
        // if m.mmapWillNeed {
        //     return madviseWillNeed(m.b)
        // }

        Ok(())
    }

    fn read(&self, key: &[u8], timestamp: i64) -> Result<Vec<Value>> {
        let inner = self.inner.read();
        if let Ok(Some(entry)) = inner.index.entry(key, timestamp) {
            return self.read_block(&entry);
        }
        Ok(vec![])
    }

    fn read_block(&self, entry: &IndexEntry) -> Result<Vec<Value>> {
        self.inc_access();

        let inner = self.inner.read();
        ensure!(
            inner.mmap.as_ref().unwrap().len() as i64 >= entry.offset + entry.size as i64,
            TSMClosedSnafu
        );

        //TODO: Validate checksum
        let values = encoding::decode_block(
            &inner.mmap.as_ref().unwrap()
                [entry.offset as usize + 4..entry.offset as usize + entry.size as usize],
        )
        .context(EncodingSnafu)?;

        Ok(values)
    }

    fn read_bytes(&self, entry: &IndexEntry, b: &[u8]) -> Result<(u32, Vec<u8>)> {
        self.inc_access();

        let lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait("tsm_accessor.read", lock_start, "stage=query_block_decode");
        let data = inner.mmap.as_ref().unwrap().as_ref();
        ensure!(
            data.len() as i64 >= entry.offset + entry.size as i64,
            TSMClosedSnafu
        );

        // return the bytes after the 4 byte checksum
        Ok((
            (&data[entry.offset as usize..entry.offset as usize + 4]).get_u32(),
            data[entry.offset as usize + 4..entry.offset as usize + entry.size as usize].to_vec(),
        ))
    }

    /// Returns all values for a key in all blocks.
    fn read_all(&self, key: &[u8]) -> Result<Vec<Value>> {
        self.inc_access();

        let inner = self.inner.read();

        let blocks = inner.index.entries(key);
        if blocks.is_empty() {
            return Ok(vec![]);
        }

        let tombstones = inner.index.tombstone_range(key);

        let mut values = vec![];
        for block in blocks {
            let mut skip = false;
            for t in tombstones.iter() {
                // Should we skip this block because it contains points that have been deleted
                if t.min <= block.min_time && t.max >= block.max_time {
                    skip = true;
                    break;
                }
            }

            if skip {
                continue;
            }

            //TODO: Validate checksum
            // The +4 is the 4 byte checksum length
            let temp = encoding::decode_block(
                &inner.mmap.as_ref().unwrap()
                    [block.offset as usize + 4..block.offset as usize + block.size as usize],
            )
            .context(EncodingSnafu)?;

            // Filter out any values that were deleted
            let mut temp_values = Values(temp);

            for t in tombstones.iter() {
                temp_values.exclude(t.min, t.max);
            }

            values.append(&mut *temp_values);
        }

        Ok(values)
    }

    fn read_integer_block(&self, entry: &IndexEntry) -> Result<Vec<IntegerValue>> {
        self.inc_access();

        let inner = self.inner.read();
        let data = inner.mmap.as_ref().unwrap().as_ref();
        ensure!(
            data.len() as i64 >= entry.offset + entry.size as i64,
            TSMClosedSnafu
        );

        let decode_start = slow_trace::now();
        let a = decode_integer_block(
            &data[entry.offset as usize + 4..entry.offset as usize + entry.size as usize],
        )
        .context(EncodingSnafu)?;
        slow_trace::log_query_stage("query_block_decode", decode_start, "source=tsm");

        Ok(a)
    }

    fn read_timestamp_array_block(
        &self,
        entry: &IndexEntry,
        expected_block_type: u8,
    ) -> Result<Vec<i64>> {
        self.inc_access();

        let lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait("tsm_accessor.read", lock_start, "stage=query_block_decode");
        let data = inner.mmap.as_ref().unwrap().as_ref();
        ensure!(
            data.len() as i64 >= entry.offset + entry.size as i64,
            TSMClosedSnafu
        );

        let decode_start = slow_trace::now();
        let timestamps = decode_timestamp_array_block(
            &data[entry.offset as usize + 4..entry.offset as usize + entry.size as usize],
            expected_block_type,
        )
        .context(ArrayEncodingSnafu)?;
        slow_trace::log_query_stage("query_block_decode", decode_start, "source=tsm");

        Ok(timestamps)
    }

    fn read_integer_array_block(&self, entry: &IndexEntry, value: &mut IntegerArray) -> Result<()> {
        self.inc_access();

        let lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait("tsm_accessor.read", lock_start, "stage=query_block_decode");
        let data = inner.mmap.as_ref().unwrap().as_ref();
        ensure!(
            data.len() as i64 >= entry.offset + entry.size as i64,
            TSMClosedSnafu
        );

        let decode_start = slow_trace::now();
        decode_integer_array_block(
            &data[entry.offset as usize + 4..entry.offset as usize + entry.size as usize],
            value,
        )
        .context(ArrayEncodingSnafu)?;
        slow_trace::log_query_stage("query_block_decode", decode_start, "source=tsm");

        Ok(())
    }

    fn read_unsigned_block(&self, entry: &IndexEntry) -> Result<Vec<UnsignedValue>> {
        self.inc_access();

        let lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait("tsm_accessor.read", lock_start, "stage=query_block_decode");
        let data = inner.mmap.as_ref().unwrap().as_ref();
        ensure!(
            data.len() as i64 >= entry.offset + entry.size as i64,
            TSMClosedSnafu
        );

        let decode_start = slow_trace::now();
        let a = decode_unsigned_block(
            &data[entry.offset as usize + 4..entry.offset as usize + entry.size as usize],
        )
        .context(EncodingSnafu)?;
        slow_trace::log_query_stage("query_block_decode", decode_start, "source=tsm");

        Ok(a)
    }

    fn read_unsigned_array_block(
        &self,
        entry: &IndexEntry,
        value: &mut UnsignedArray,
    ) -> Result<()> {
        self.inc_access();

        let lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait("tsm_accessor.read", lock_start, "stage=query_block_decode");
        let data = inner.mmap.as_ref().unwrap().as_ref();
        ensure!(
            data.len() as i64 >= entry.offset + entry.size as i64,
            TSMClosedSnafu
        );

        let decode_start = slow_trace::now();
        decode_unsigned_array_block(
            &data[entry.offset as usize + 4..entry.offset as usize + entry.size as usize],
            value,
        )
        .context(ArrayEncodingSnafu)?;
        slow_trace::log_query_stage("query_block_decode", decode_start, "source=tsm");

        Ok(())
    }

    fn read_float_block(&self, entry: &IndexEntry) -> Result<Vec<FloatValue>> {
        self.inc_access();

        let lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait("tsm_accessor.read", lock_start, "stage=query_block_decode");
        let data = inner.mmap.as_ref().unwrap().as_ref();
        ensure!(
            data.len() as i64 >= entry.offset + entry.size as i64,
            TSMClosedSnafu
        );

        let decode_start = slow_trace::now();
        let a = decode_float_block(
            &data[entry.offset as usize + 4..entry.offset as usize + entry.size as usize],
        )
        .context(EncodingSnafu)?;
        slow_trace::log_query_stage("query_block_decode", decode_start, "source=tsm");

        Ok(a)
    }

    fn read_float_array_block(&self, entry: &IndexEntry, value: &mut FloatArray) -> Result<()> {
        self.inc_access();

        let lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait("tsm_accessor.read", lock_start, "stage=query_block_decode");
        let data = inner.mmap.as_ref().unwrap().as_ref();
        ensure!(
            data.len() as i64 >= entry.offset + entry.size as i64,
            TSMClosedSnafu
        );

        let decode_start = slow_trace::now();
        decode_float_array_block(
            &data[entry.offset as usize + 4..entry.offset as usize + entry.size as usize],
            value,
        )
        .context(ArrayEncodingSnafu)?;
        slow_trace::log_query_stage("query_block_decode", decode_start, "source=tsm");

        Ok(())
    }

    fn read_bool_block(&self, entry: &IndexEntry) -> Result<Vec<BoolValue>> {
        self.inc_access();

        let lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait("tsm_accessor.read", lock_start, "stage=query_block_decode");
        let data = inner.mmap.as_ref().unwrap().as_ref();
        ensure!(
            data.len() as i64 >= entry.offset + entry.size as i64,
            TSMClosedSnafu
        );

        let decode_start = slow_trace::now();
        let a = decode_bool_block(
            &data[entry.offset as usize + 4..entry.offset as usize + entry.size as usize],
        )
        .context(EncodingSnafu)?;
        slow_trace::log_query_stage("query_block_decode", decode_start, "source=tsm");

        Ok(a)
    }

    fn read_bool_array_block(&self, entry: &IndexEntry, value: &mut BoolArray) -> Result<()> {
        self.inc_access();

        let lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait("tsm_accessor.read", lock_start, "stage=query_block_decode");
        let data = inner.mmap.as_ref().unwrap().as_ref();
        ensure!(
            data.len() as i64 >= entry.offset + entry.size as i64,
            TSMClosedSnafu
        );

        let decode_start = slow_trace::now();
        decode_bool_array_block(
            &data[entry.offset as usize + 4..entry.offset as usize + entry.size as usize],
            value,
        )
        .context(ArrayEncodingSnafu)?;
        slow_trace::log_query_stage("query_block_decode", decode_start, "source=tsm");

        Ok(())
    }

    fn read_string_block(&self, entry: &IndexEntry) -> Result<Vec<StringValue>> {
        self.inc_access();

        let lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait("tsm_accessor.read", lock_start, "stage=query_block_decode");
        let data = inner.mmap.as_ref().unwrap().as_ref();
        ensure!(
            data.len() as i64 >= entry.offset + entry.size as i64,
            TSMClosedSnafu
        );

        let decode_start = slow_trace::now();
        let a = decode_string_block(
            &data[entry.offset as usize + 4..entry.offset as usize + entry.size as usize],
        )
        .context(EncodingSnafu)?;
        slow_trace::log_query_stage("query_block_decode", decode_start, "source=tsm");

        Ok(a)
    }

    fn read_string_array_block(&self, entry: &IndexEntry, value: &mut StringArray) -> Result<()> {
        self.inc_access();

        let lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait("tsm_accessor.read", lock_start, "stage=query_block_decode");
        let data = inner.mmap.as_ref().unwrap().as_ref();
        ensure!(
            data.len() as i64 >= entry.offset + entry.size as i64,
            TSMClosedSnafu
        );

        let decode_start = slow_trace::now();
        decode_string_array_block(
            &data[entry.offset as usize + 4..entry.offset as usize + entry.size as usize],
            value,
        )
        .context(ArrayEncodingSnafu)?;
        slow_trace::log_query_stage("query_block_decode", decode_start, "source=tsm");

        Ok(())
    }

    fn path(&self) -> PathBuf {
        let inner = self.inner.read();
        inner.path.clone()
    }

    fn close(&self) -> Result<()> {
        let mut inner = self.inner.write();
        inner.mmap = None;
        Ok(())
    }
}

struct InnerAccessor {
    free_count: AtomicU64, // Counter to determine whether the accessor can free its resources
    mmap_will_need: bool, // If true then mmap advise value MADV_WILLNEED will be provided the kernel for b.

    path: PathBuf,
    mmap: Option<Mmap>,
    // b: Vec<u8>,
    // f: File,
    index: Arc<IndirectIndex>,
}

/// IndirectIndex is a TSMIndex that uses a raw byte slice representation of an index.  This
/// implementation can be used for indexes that may be MMAPed into memory.
#[derive(Default)]
pub struct IndirectIndex {
    // indirectIndex works a follows.  Assuming we have an index structure in memory as
    // the diagram below:
    //
    // ┌────────────────────────────────────────────────────────────────────┐
    // │                               Index                                │
    // ├─┬──────────────────────┬──┬───────────────────────┬───┬────────────┘
    // │0│                      │62│                       │145│
    // ├─┴───────┬─────────┬────┼──┴──────┬─────────┬──────┼───┴─────┬──────┐
    // │Key 1 Len│   Key   │... │Key 2 Len│  Key 2  │ ...  │  Key 3  │ ...  │
    // │ 2 bytes │ N bytes │    │ 2 bytes │ N bytes │      │ 2 bytes │      │
    // └─────────┴─────────┴────┴─────────┴─────────┴──────┴─────────┴──────┘

    // We would build an `offsets` slices where each element pointers to the byte location
    // for the first key in the index slice.

    // ┌────────────────────────────────────────────────────────────────────┐
    // │                              Offsets                               │
    // ├────┬────┬────┬─────────────────────────────────────────────────────┘
    // │ 0  │ 62 │145 │
    // └────┴────┴────┘

    // Using this offset slice we can find `Key 2` by doing a binary search
    // over the offsets slice.  Instead of comparing the value in the offsets
    // (e.g. `62`), we use that as an index into the underlying index to
    // retrieve the key at position `62` and perform our comparisons with that.

    // When we have identified the correct position in the index for a given
    // key, we could perform another binary search or a linear scan.  This
    // should be fast as well since each index entry is fixed-size and all
    // contiguous in memory.  The current implementation uses a linear scan since the
    // number of block entries is expected to be < 100 per key.
    inner: RwLock<InnerIndirectIndex>,
}

impl IndirectIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the size of the current index in bytes.
    pub fn size(&self) -> u32 {
        let inner = self.inner.read();
        inner.b.len() as u32
    }

    fn offset(&self, i: usize) -> usize {
        let inner = self.inner.read();
        if i < 0 || i + 4 > inner.offsets.len() {
            // return -1;
            panic!("TSMReader: invalid offset {}", i);
        }

        (&inner.offsets[i * 4..i * 4 + 4]).get_u32() as usize
    }

    pub fn seek(&self, key: &[u8]) -> usize {
        let inner = self.inner.read();
        inner.search_offset(key)
    }

    /// Returns all index entries for a key.
    pub fn entries(&self, key: &[u8]) -> Vec<IndexEntry> {
        self.read_entries(key, None)
    }

    /// Returns all index entries for a key.
    pub fn read_entries(
        &self,
        key: &[u8],
        entries: Option<&mut Vec<IndexEntry>>,
    ) -> Vec<IndexEntry> {
        let inner = self.inner.read();

        let ofs = inner.search(key);
        if ofs < inner.b.len() {
            let (k, entries) = inner.read_entries_at(ofs, entries);
            // The search may have returned an i == 0 which could indicated that the value
            // searched should be inserted at position 0.  Make sure the key in the index
            // matches the search value.
            if key.cmp(k) != Equal {
                return vec![];
            }

            return entries;
        }

        // The key is not in the index.  i is the index where it would be inserted.
        vec![]
    }

    /// Returns the index entry for the specified key and timestamp.
    /// If no entry matches the key an timestamp, nil is returned.
    pub fn entry(&self, key: &[u8], timestamp: i64) -> Result<Option<IndexEntry>> {
        let entries = self.entries(key);
        for entry in entries {
            if entry.contains(timestamp) {
                return Ok(Some(entry));
            }
        }

        Ok(None)
    }

    pub fn min_key(&self) -> Vec<u8> {
        let inner = self.inner.read();
        inner.min_key()
    }

    pub fn max_key(&self) -> Vec<u8> {
        let inner = self.inner.read();
        inner.max_key()
    }

    /// Returns the key in the index at the given position.
    pub fn key(&self, idx: usize, entries: &mut [IndexEntry]) -> (Vec<u8>, u8, Vec<IndexEntry>) {
        let inner = self.inner.read();
        inner.key(idx, entries)
    }

    /// Returns the key in the index at the given position.
    pub fn key_at(&self, idx: usize) -> (Vec<u8>, u8) {
        let inner = self.inner.read();
        inner.key_at(idx)
    }

    /// Returns the count of unique keys in the index
    pub fn key_count(&self) -> usize {
        let inner = self.inner.read();
        inner.key_count()
    }

    /// Removes the given keys from the index.
    pub fn delete(&self, keys: &mut [&[u8]]) {
        let mut keys = keys;
        if keys.is_empty() {
            return;
        }

        if !keys.is_sorted() {
            keys.sort();
        }

        // Both keys and offsets are sorted.  Walk both in order and skip
        // any keys that exist in both.
        let mut inner = self.inner.write();
        let start = inner.search_offset(keys[0]);
        let mut i = start * 4;
        while i + 4 <= inner.offsets.len() && !keys.is_empty() {
            let offset = (&inner.offsets[i..i + 4]).get_u32();
            let (_, index_key) = read_key(&inner.b.as_slice()[offset as usize..]);
            while !keys.is_empty() && keys[0] < index_key {
                keys = &mut keys[1..];
            }

            if !keys.is_empty() && keys[0] == index_key {
                keys = &mut keys[1..];
                inner.offsets[i..i + 4].copy_from_slice(NONE_OFFSET.as_slice());
            }
            i += 4;
        }

        pack(&mut inner.offsets, 4, 255);
    }

    /// Removes the given keys with data between minTime and maxTime from the index.
    pub fn delete_range(&self, keys: &mut [&[u8]], min_time: i64, max_time: i64) {
        let mut keys = keys;
        // No keys, nothing to do
        if keys.is_empty() {
            return;
        }

        if !keys.is_sorted() {
            keys.sort();
        }

        // If we're deleting the max time range, just use tombstoning to remove the
        // key from the offsets slice
        if min_time == i64::MIN && max_time == i64::MAX {
            self.delete(keys);
            return;
        }

        // Is the range passed in outside of the time range for the file?
        let (min, max) = self.time_range();
        if min_time > max || max_time < min {
            return;
        }

        let mut fullkeys = Vec::with_capacity(keys.len());
        let mut tombstones = HashMap::<String, Vec<TimeRange>>::new();

        let mut i = 0;
        while !keys.is_empty() && i < self.key_count() {
            let (k, entries) = {
                let inner = self.inner.read();
                let (k, entries) = inner.read_entries_at(self.offset(i), None);
                (k.to_vec(), entries)
            };
            let ie = entries;

            // Skip any keys that don't exist.  These are less than the current key.
            while !keys.is_empty() && keys[0] < k.as_slice() {
                keys = &mut keys[1..];
            }

            // No more keys to delete, we're done.
            if keys.is_empty() {
                break;
            }

            // If the current key is greater than the index one, continue to the next index key.
            if !keys.is_empty() && keys[0] > k.as_slice() {
                i += 1;
                continue;
            }

            // If multiple tombstones are saved for the same key
            if ie.is_empty() {
                i += 1;
                continue;
            }

            // Is the time range passed outside of the time range we've have stored for this key?
            let (min, max) = (ie[0].min_time, ie[ie.len() - 1].max_time);
            if min_time > max || max_time < min {
                i += 1;
                continue;
            }

            // Does the range passed in cover every value for the key?
            if min_time <= min && max_time >= max {
                fullkeys.push(keys[0]); // TODO: clone ?
                keys = &mut keys[1..];
                i += 1;
                continue;
            }

            let existing = {
                let inner = self.inner.read();
                // match inner.tombstones.get(to_str!(&k)) {
                //     Some(tms) => Some(tms.clone()),
                //     None => None,
                // }
                inner.tombstones.get(to_str!(&k)).cloned()
            };

            // Append the new tombonstes to the existing ones
            let mut new_ts = if let Some(mut existing) = existing {
                if let Some(tstone) = tombstones.get(to_str!(&k)) {
                    existing.extend_from_slice(tstone.as_slice());
                }
                existing.push(TimeRange {
                    min: min_time,
                    max: max_time,
                });
                existing
            } else {
                let mut a = vec![];
                if let Some(tstone) = tombstones.get(to_str!(&k)) {
                    a.extend_from_slice(tstone.as_slice());
                }
                a.push(TimeRange {
                    min: min_time,
                    max: max_time,
                });
                a
            };

            // Sort the updated tombstones if necessary
            if new_ts.len() > 1
                && new_ts.is_sorted_by(|a, b| {
                    if a.min == b.min {
                        return a.max <= b.max;
                    }
                    a.min < b.min
                })
            {
                new_ts.sort_by(|a, b| {
                    // if a.min == b.min {
                    //     return a.max <= b.max;
                    // }
                    // a.min < b.min
                    match a.min.cmp(&b.min) {
                        Less => Less,
                        Equal => match a.max.cmp(&b.max) {
                            Less | Equal => Less,
                            Greater => Greater,
                        },
                        Greater => Greater,
                    }
                });
            }

            tombstones.insert(to_string!(&k), new_ts.clone());

            // We need to see if all the tombstones end up deleting the entire series.  This
            // could happen if their is one tombstore with min,max time spanning all the block
            // time ranges or from multiple smaller tombstones the delete segments.  To detect
            // this cases, we use a window starting at the first tombstone and grow it be each
            // tombstone that is immediately adjacent to the current window or if it overlaps.
            // If there are any gaps, we abort.
            let (mut min_ts, mut max_ts) = (new_ts[0].min, new_ts[0].max);
            let mut j = 1;
            while j < new_ts.len() {
                let prev_ts = &new_ts[j - 1];
                let ts = &new_ts[j];

                // Make sure all the tombstone line up for a continuous range.  We don't
                // want to have two small deletes on each edges end up causing us to
                // remove the full key.
                if prev_ts.max != ts.min - 1 && !prev_ts.overlaps(ts.min, ts.max) {
                    (min_ts, max_ts) = (i64::MAX, i64::MIN);
                    break;
                }

                if ts.min < min_ts {
                    min_ts = ts.min;
                }
                if ts.max > max_ts {
                    max_ts = ts.max;
                }
                j += 1;
            }

            // If we have a fully deleted series, delete it all of it.
            if min_ts <= min && max_ts >= max {
                fullkeys.push(keys[0]); // TODO: clone ?
                keys = &mut keys[1..];
            }
            i += 1;
        }

        // Delete all the keys that fully deleted in bulk
        if !fullkeys.is_empty() {
            self.delete(fullkeys.as_mut_slice());
        }

        if tombstones.is_empty() {
            return;
        }

        let mut inner = self.inner.write();
        for (k, v) in tombstones {
            inner.tombstones.insert(k, v);
        }
    }

    /// Returns ranges of time that are deleted for the given key.
    pub fn tombstone_range(&self, key: &[u8]) -> Vec<TimeRange> {
        let inner = self.inner.read();
        if let Some(tm) = inner.tombstones.get(to_str!(key)) {
            return tm.clone();
        }
        vec![]
    }

    /// Return true if the given key exists in the index.
    pub fn contains(&self, key: &[u8]) -> bool {
        !self.entries(key).is_empty()
    }

    /// Returns true if key and time might exist in this file.
    pub fn contains_value(&self, key: &[u8], timestamp: i64) -> bool {
        if let Ok(Some(_entry)) = self.entry(key, timestamp) {
            let inner = self.inner.read();
            let tombstones = inner.tombstones.get(to_str!(key)).cloned();
            drop(inner);

            if let Some(tombstones) = tombstones {
                for t in tombstones.iter() {
                    if t.min <= timestamp && t.max >= timestamp {
                        return false;
                    }
                }
                return true;
            }
            return true;
        }

        false
    }

    /// Returns the block type of the values stored for the key.
    pub fn block_type(&self, key: &[u8]) -> Result<u8> {
        let inner = self.inner.read();

        let mut ofs = inner.search(key);
        if ofs < inner.b.len() {
            let (n, _) = read_key(&inner.b[ofs..]);
            ofs += n;
            return Ok(inner.b[ofs]);
        }
        Err(Error::StringErr {
            reason: format!("key does not exist: {}", to_str!(key)),
        })
    }

    /// Returns true if the time range of the file intersect min and max.
    pub fn overlaps_time_range(&self, min: i64, max: i64) -> bool {
        let inner = self.inner.read();
        inner.min_time <= max && inner.max_time >= min
    }

    /// Returns true if the min and max keys of the file overlap the arguments min and max.
    pub fn overlaps_key_range(&self, min: &[u8], max: &[u8]) -> bool {
        let inner = self.inner.read();
        inner.min_key.as_slice() <= max && inner.max_key.as_slice() >= min
    }

    /// Returns the min and max keys in the index.
    pub fn key_range(&self) -> (Vec<u8>, Vec<u8>) {
        let inner = self.inner.read();
        (inner.min_key.clone(), inner.max_key.clone())
    }

    /// Returns the min and max time across all keys in the index.
    pub fn time_range(&self) -> (i64, i64) {
        let inner = self.inner.read();
        (inner.min_time, inner.max_time)
    }

    /// Returns a byte slice encoded version of the index.
    pub fn marshal_binary(&self) -> Result<Vec<u8>> {
        let inner = self.inner.read();
        Ok(inner.b.clone()) // TODO: optimize me
    }

    /// Populates an index from an encoded byte slice representation of an index.
    pub fn unmarshal_binary(&self, b: &[u8]) -> Result<()> {
        let mut inner = self.inner.write();

        // Keep a reference to the actual index bytes
        inner.b = b.to_vec();
        if b.is_empty() {
            return Ok(());
        }

        let (mut min_time, mut max_time) = (i64::MAX, 0);

        // To create our "indirect" index, we need to find the location of all the keys in
        // the raw byte slice.  The keys are listed once each (in sorted order).  Following
        // each key is a time ordered list of index entry blocks for that key.  The loop below
        // basically skips across the slice keeping track of the counter when we are at a key
        // field.
        let mut i = 0;
        let mut offsets = vec![];
        let imax = b.len();
        while i < imax {
            offsets.push(i);

            // Skip to the start of the values
            // key length value (2) + type (1) + length of key
            ensure!(
                i.checked_add(3).is_some_and(|end| end <= imax),
                StringErrSnafu {
                    reason: "IndirectIndex: not enough data for key length value".to_string(),
                }
            );
            let key_len = (&b[i..i + 2]).get_u16() as usize;
            i = i
                .checked_add(3 + key_len)
                .filter(|end| *end <= imax)
                .context(StringErrSnafu {
                    reason: "IndirectIndex: not enough data for key".to_string(),
                })?;

            // count of index entries
            ensure!(
                i.checked_add(INDEX_COUNT_SIZE)
                    .is_some_and(|end| end <= imax),
                StringErrSnafu {
                    reason: "IndirectIndex: not enough data for index entries count".to_string(),
                }
            );
            let count = (&b[i..i + INDEX_COUNT_SIZE]).get_u16() as i32;
            i += INDEX_COUNT_SIZE;

            ensure!(
                count > 0,
                StringErrSnafu {
                    reason: "IndirectIndex: index entry count must be non-zero".to_string(),
                }
            );

            let entries_size =
                (count as usize)
                    .checked_mul(INDEX_ENTRY_SIZE)
                    .context(StringErrSnafu {
                        reason: "IndirectIndex: index entries size overflow".to_string(),
                    })?;
            let entries_end = i
                .checked_add(entries_size)
                .filter(|end| *end <= imax)
                .context(StringErrSnafu {
                    reason: "IndirectIndex: not enough data for index entries".to_string(),
                })?;
            let last_entry = entries_end - INDEX_ENTRY_SIZE;

            let min_t = (&b[i..i + 8]).get_u64() as i64;
            if min_t < min_time {
                min_time = min_t;
            }

            // Find the max time for the block
            let max_t = (&b[last_entry + 8..last_entry + 16]).get_u64() as i64;
            if max_t > max_time {
                max_time = max_t;
            }

            i = entries_end;
        }

        let first_ofs = offsets[0];
        let (_, key) = read_key(&b[first_ofs..]);
        inner.min_key = key.to_vec();

        let last_ofs = offsets[offsets.len() - 1];
        let (_, key) = read_key(&b[last_ofs..]);
        inner.max_key = key.to_vec();

        inner.min_time = min_time;
        inner.max_time = max_time;

        // TODO: golang use mmap as a regular memory allocater by setting nil to *os.File arg
        inner.offsets = vec![0u8; offsets.len() * 4];

        for (i, v) in offsets.iter().enumerate() {
            (&mut inner.offsets[i * 4..i * 4 + 4]).put_u32(*v as u32);
        }

        Ok(())
    }

    pub fn close(&self) -> Result<()> {
        // Windows doesn't use the anonymous map for the offsets index
        // if os == "windows" {
        //     return Ok(());
        // }

        // munmap(self.offsets[:cap(self.offsets)])  // FIXME: 这是什么意思
        Ok(())
    }
}

#[derive(Default)]
struct InnerIndirectIndex {
    // b is the underlying index byte slice.  This could be a copy on the heap or an MMAP
    // slice reference
    b: Vec<u8>, // []byte,

    // offsets contains the positions in b for each key.  It points to the 2 byte length of
    // key.
    offsets: Vec<u8>, //  []byte,

    // minKey, maxKey are the minium and maximum (lexicographically sorted) contained in the
    // file
    min_key: Vec<u8>, // []byte,
    max_key: Vec<u8>, // []byte,

    // minTime, maxTime are the minimum and maximum times contained in the file across all
    // series.
    min_time: i64,
    max_time: i64,

    // tombstones contains only the tombstoned keys with subset of time values deleted.  An
    // entry would exist here if a subset of the points for a key were deleted and the file
    // had not be re-compacted to remove the points on disk.
    tombstones: HashMap<String, Vec<TimeRange>>,
}

impl InnerIndirectIndex {
    // pub fn new() -> Self {
    //     InnerIndirectIndex::default()
    // }

    // fn offset(&self, i: usize) -> usize {
    //     if i < 0 || i+4 > self.offsets.len() {
    //         return -1;
    //     }

    //     (&self.offsets[i*4..i*4+4]).get_u32()
    // }

    // pub fn seek(&self, key: &[u8]) -> usize {
    //     let inner = self.
    // }

    /// searches the offsets slice for key and returns the position in
    /// offsets where key would exist.
    fn search_offset(&self, key: &[u8]) -> usize {
        // We use a binary search across our indirect offsets (pointers to all the keys
        // in the index slice).
        let i = search_bytes_fixed(&self.offsets, 4, &self.b, key);

        // See if we might have found the right index
        if i < self.offsets.len() {
            return (i / 4) as usize;
        }

        // The key is not in the index.  i is the index where it would be inserted so return
        // a value outside our offset range.
        self.offsets.len() / 4
    }

    /// Returns the byte position of key in the index.  If key is not in the index, len(index) is returned.
    fn search(&self, key: &[u8]) -> usize {
        if !self.contains_key(key) {
            return self.b.len();
        }

        // We use a binary search across our indirect offsets (pointers to all the keys
        // in the index slice).
        // TODO(sgc): this should be inlined to `indirectIndex` as it is only used here
        let i = search_bytes_fixed(&self.offsets, 4, &self.b, key);

        // See if we might have found the right index
        if i < self.offsets.len() {
            let ofs = (&self.offsets[i..i + 4]).get_u32() as usize;
            let (_, k) = read_key(&self.b[ofs..]);

            // The search may have returned an i == 0 which could indicated that the value
            // searched should be inserted at position 0.  Make sure the key in the index
            // matches the search value.
            match key.cmp(k) {
                std::cmp::Ordering::Equal => return ofs,
                _ => return self.b.len(),
            }
        }

        // The key is not in the index.  i is the index where it would be inserted so return
        // a value outside our offset range.
        self.b.len()
    }

    pub fn min_key(&self) -> Vec<u8> {
        self.min_key.clone()
    }

    pub fn max_key(&self) -> Vec<u8> {
        self.max_key.clone()
    }

    /// Returns true of key may exist in this index.
    fn contains_key(&self, key: &[u8]) -> bool {
        key.cmp(&self.min_key) != std::cmp::Ordering::Less
            && key.cmp(&self.max_key) != std::cmp::Ordering::Greater
    }

    fn read_entries_at(
        &self,
        mut ofs: usize,
        entries: Option<&mut Vec<IndexEntry>>,
    ) -> (&[u8], Vec<IndexEntry>) {
        let (n, k) = read_key(&self.b[ofs..]);

        // Read and return all the entries
        ofs += n;
        let mut ie = IndexEntries::default();
        match entries {
            Some(entries) => {
                ie.entries = entries.clone();

                if let Err(e) = read_entries(&self.b[ofs..], &mut ie) {
                    panic!("error reading entries: {:?}", e);
                }

                // for (idx, entry) in entries.iter_mut().enumerate() {
                //     *entry = ie.entries[idx].clone();
                // }

                *entries = ie.entries.clone();
            }
            None => {
                if let Err(e) = read_entries(&self.b[ofs..], &mut ie) {
                    panic!("error reading entries: {:?}", e);
                }
            }
        }

        (k, ie.entries)
    }

    /// Returns the key in the index at the given position.
    fn key(&self, idx: usize, entries: &mut [IndexEntry]) -> (Vec<u8>, u8, Vec<IndexEntry>) {
        if idx < 0 || idx * 4 + 4 > self.offsets.len() {
            return (vec![], 0, vec![]);
        }

        let ofs = (&self.offsets[idx * 4..idx * 4 + 4]).get_u32() as usize;
        let (n, key) = read_key(&self.b[ofs..]);

        let typ = self.b[ofs + n];

        let mut ie = IndexEntries::default();
        if !entries.is_empty() {
            ie.entries = entries.to_vec();
        }

        if let Err(e) = read_entries(&self.b[ofs + n..], &mut ie) {
            return (vec![], 0, vec![]);
        }

        if !entries.is_empty() {
            for (idx, entry) in entries.iter_mut().enumerate() {
                *entry = ie.entries[idx].clone();
            }
        }

        (key.to_vec(), typ, ie.entries)
    }

    /// Returns the key in the index at the given position.
    fn key_at(&self, idx: usize) -> (Vec<u8>, u8) {
        if idx < 0 || idx * 4 + 4 > self.offsets.len() {
            return (vec![], 0);
        }
        let mut ofs = (&self.offsets[idx * 4..idx * 4 + 4]).get_u32() as usize;
        let (n, key) = read_key(&self.b[ofs..]);

        ofs += n;
        let typ = self.b[ofs];

        (key.to_vec(), typ)
    }

    /// Returns the count of unique keys in the index.
    pub fn key_count(&self) -> usize {
        self.offsets.len() / 4
    }

    // /// Removes the given keys with data between minTime and maxTime from the index.
    // pub fn delete_range(&mut self, keys: &[&[u8]], min_time: i64, max_time: i64) {
    //     todo!("implement me");
    // }
}

#[derive(Clone, PartialEq, Eq)]
pub struct TimeRange {
    pub min: i64,
    pub max: i64,
}

impl TimeRange {
    pub fn overlaps(&self, min: i64, max: i64) -> bool {
        self.min <= max && self.max >= min
    }
}

fn search_bytes_fixed(a: &[u8], sz: usize, b: &[u8], key: &[u8]) -> usize {
    if a.len() % sz != 0 {
        panic!("x is not a multiple of a: {} {}", a.len(), sz);
    }

    if a.len() < sz {
        return 0;
    }

    let (mut i, mut j) = (0, a.len() - sz);
    while i < j {
        let mut h = (i + j) >> 1;
        h -= h % sz;
        let matched = {
            // i is the position in offsets we are at so get offset it points to
            let offset = (&a[h..h + sz]).get_u32() as usize;

            // It's pointing to the start of the key which is a 2 byte length
            let key_len = (&b[offset..offset + 2]).get_u16() as usize;

            // See if it matches
            b[offset + 2..offset + 2 + key_len].cmp(key) != std::cmp::Ordering::Less
        };

        if !matched {
            i = h + sz;
        } else {
            j = h;
        }
    }
    i
}

fn read_key(b: &[u8]) -> (usize, &[u8]) {
    // 2 byte size of key
    let (mut n, size) = (2, (&b[..2]).get_u16() as usize);

    // N byte key
    let key = &b[n..n + size];
    n += key.len();

    (n, key)
}

fn read_entries(b: &[u8], entries: &mut IndexEntries) -> Result<usize> {
    ensure!(
        b.len() >= 1 + INDEX_COUNT_SIZE,
        StringErrSnafu {
            reason: "read_entries: data too short for headers".to_string(),
        }
    );

    let mut n = 0;
    // 1 byte block type
    entries.typ = b[n];
    n += 1;

    // 2 byte count of index entries
    let count = (&b[n..n + INDEX_COUNT_SIZE]).get_u16() as i32;
    n += INDEX_COUNT_SIZE;

    if entries.entries.capacity() < count as usize {
        entries.entries = vec![IndexEntry::default(); count as usize];
    } else {
        entries.entries.truncate(count as usize);
    }

    let mut b = &b[INDEX_COUNT_SIZE + INDEX_TYPE_SIZE..];
    for i in 0..count {
        if let Err(e) = entries.entries[i as usize].unmarshal_binary(b, entries.typ) {
            return Err(Error::StringErr {
                reason: format!("read_entries: unmarshal error: {:?}", e),
            });
        }
        b = &b[INDEX_ENTRY_SIZE..];
    }

    n += count as usize * INDEX_ENTRY_SIZE;

    Ok(n)
}

// Pack converts a sparse array to a dense one.  It removes sections of a containing
// runs of val of length width.  The returned value is a subslice of a.
fn pack(a: &mut Vec<u8>, width: usize, val: u8) {
    let (mut i, mut j, mut j_start, mut end) = (0, 0, 0, 0);

    // Skip the first run that won't move
    while i < a.len() && a[i] != val {
        i += width;
    }
    end = i;

    while i < a.len() {
        // Find the next gap to remove
        while i < a.len() && a[i] == val {
            i += width
        }

        // Find the next non-gap to keep
        j_start = i;
        j = i;
        while j < a.len() && a[j] != val {
            j += width;
        }

        if j_start == a.len() {
            break;
        }

        // Move the non-gap over the section to remove.
        unsafe {
            let src = a.as_ptr().add(j_start);
            let dst = a.as_mut_ptr().add(end);
            std::ptr::copy(src, dst, j - j_start);
        }

        end += j - j_start;
        i = j;
    }

    a.truncate(end);
}

fn vertify_version(f: &mut File) -> Result<()> {
    f.seek(SeekFrom::Start(0))?;

    let mut buf = vec![0u8; 4];
    let _ = f.read(&mut buf[..4])?;

    let magic = buf.as_slice().get_u32();
    ensure!(
        magic == MAGIC_NUMBER,
        StringErrSnafu {
            reason: format!("vertify_version failed, incorrent maginc number: {}", magic),
        }
    );

    let _ = f.read(&mut buf[..1])?;
    ensure!(
        buf[0] == VERSION,
        StringErrSnafu {
            reason: format!("vertify_version failed, unsupported version: {}", buf[0]),
        }
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        fs::File,
        io::{Read, Seek, Write},
        path::Path,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use crate::common::to_string;
    use crate::models::points::{
        BoolValue, FloatValue, IntegerValue, StringValue, UnsignedValue, Value,
    };
    use crate::runtime::execution::{
        BoxBlockingTask, BoxFutureTask, ExecutorRef, RuntimeExecutor, SpawnError, TaskController,
    };
    use bytes::Buf;
    use parking_lot::lock_api::RawMutex as _;
    use tempfile::{NamedTempFile, TempDir};

    use super::{
        BatchDelete, BatchDeleteDisposition, BatchDeletes, BlockIterItem, IndirectIndex, TSMReader,
    };
    use crate::storage::tsm1::{
        cache::tests::{must_temp_tsm_dir, must_temp_tsm_file},
        data_encoding::{
            encoding::{BLOCK_FLOAT64, BLOCK_INTEGER, block_count},
            encoding_values::Values,
        },
        writer::{InmemIndex, MAGIC_NUMBER, TSMWriter},
    };

    struct RejectThirdBlockingSubmission {
        submissions: AtomicUsize,
    }

    struct DropThirdAcceptedSubmission {
        submissions: AtomicUsize,
    }

    struct PanicThirdAcceptedSubmission {
        submissions: AtomicUsize,
    }

    impl RejectThirdBlockingSubmission {
        fn new() -> Self {
            Self {
                submissions: AtomicUsize::new(0),
            }
        }
    }

    #[test]
    fn tsm_v1_is_rejected_explicitly() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(&MAGIC_NUMBER.to_be_bytes()).unwrap();
        file.write_all(&[1]).unwrap();
        file.flush().unwrap();

        let input = File::open(file.path()).unwrap();
        let error = match TSMReader::new(input, file.path(), 1, 1) {
            Ok(_) => panic!("TSM v1 should be rejected"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("unsupported version: 1"),
            "unexpected error: {error}"
        );
    }

    impl DropThirdAcceptedSubmission {
        fn new() -> Self {
            Self {
                submissions: AtomicUsize::new(0),
            }
        }
    }

    impl PanicThirdAcceptedSubmission {
        fn new() -> Self {
            Self {
                submissions: AtomicUsize::new(0),
            }
        }
    }

    impl RuntimeExecutor for RejectThirdBlockingSubmission {
        fn schedule(&self, _task: BoxFutureTask) -> Result<TaskController, SpawnError> {
            Err(SpawnError::rejected(
                "async lane unused by batch delete test",
            ))
        }

        fn schedule_blocking(&self, task: BoxBlockingTask) -> Result<TaskController, SpawnError> {
            let submission = self.submissions.fetch_add(1, Ordering::SeqCst) + 1;
            if submission == 3 {
                return Err(SpawnError::rejected("injected third batch refusal"));
            }
            std::thread::spawn(task);
            Ok(TaskController::detached())
        }

        fn schedule_compact(&self, _task: BoxFutureTask) -> Result<TaskController, SpawnError> {
            Err(SpawnError::rejected(
                "compact lane unused by batch delete test",
            ))
        }

        fn schedule_blocking_compact(
            &self,
            _task: BoxBlockingTask,
        ) -> Result<TaskController, SpawnError> {
            Err(SpawnError::rejected(
                "compact lane unused by batch delete test",
            ))
        }
    }

    impl RuntimeExecutor for DropThirdAcceptedSubmission {
        fn schedule(&self, _task: BoxFutureTask) -> Result<TaskController, SpawnError> {
            Err(SpawnError::rejected(
                "async lane unused by batch delete test",
            ))
        }

        fn schedule_blocking(&self, task: BoxBlockingTask) -> Result<TaskController, SpawnError> {
            let submission = self.submissions.fetch_add(1, Ordering::SeqCst) + 1;
            if submission == 3 {
                drop(task);
            } else {
                std::thread::spawn(task);
            }
            Ok(TaskController::detached())
        }

        fn schedule_compact(&self, _task: BoxFutureTask) -> Result<TaskController, SpawnError> {
            Err(SpawnError::rejected(
                "compact lane unused by batch delete test",
            ))
        }

        fn schedule_blocking_compact(
            &self,
            _task: BoxBlockingTask,
        ) -> Result<TaskController, SpawnError> {
            Err(SpawnError::rejected(
                "compact lane unused by batch delete test",
            ))
        }
    }

    impl RuntimeExecutor for PanicThirdAcceptedSubmission {
        fn schedule(&self, _task: BoxFutureTask) -> Result<TaskController, SpawnError> {
            Err(SpawnError::rejected(
                "async lane unused by batch delete test",
            ))
        }

        fn schedule_blocking(&self, task: BoxBlockingTask) -> Result<TaskController, SpawnError> {
            let submission = self.submissions.fetch_add(1, Ordering::SeqCst) + 1;
            if submission == 3 {
                std::thread::spawn(move || {
                    let _accepted_task = task;
                    panic!("injected accepted batch delete panic");
                });
            } else {
                std::thread::spawn(task);
            }
            Ok(TaskController::detached())
        }

        fn schedule_compact(&self, _task: BoxFutureTask) -> Result<TaskController, SpawnError> {
            Err(SpawnError::rejected(
                "compact lane unused by batch delete test",
            ))
        }

        fn schedule_blocking_compact(
            &self,
            _task: BoxBlockingTask,
        ) -> Result<TaskController, SpawnError> {
            Err(SpawnError::rejected(
                "compact lane unused by batch delete test",
            ))
        }
    }

    fn batch_delete_test_reader(dir: &Path, generation: i64) -> Arc<TSMReader> {
        let (file, path) = must_temp_tsm_file(dir).keep().unwrap();
        let mut writer = TSMWriter::new(file, generation, 1, "");
        writer
            .write(
                b"cpu",
                &[
                    Value::FloatValue(FloatValue::new(1, 1.0)),
                    Value::FloatValue(FloatValue::new(2, 2.0)),
                ],
            )
            .unwrap();
        writer.write_index().unwrap();
        writer.close().unwrap();
        let file = std::fs::OpenOptions::new().read(true).open(&path).unwrap();
        Arc::new(TSMReader::new(file, &path, generation, 1).unwrap())
    }

    fn batch_deletes_for(readers: &[Arc<TSMReader>], executor: ExecutorRef) -> BatchDeletes {
        let mut batches = BatchDeletes::new_with_executor(executor);
        for reader in readers {
            reader.delete_mu.lock();
            batches.push(BatchDelete::new(Arc::clone(reader)));
        }
        batches
    }

    #[test]
    fn batch_deletes_selective_refusal_rolls_back_unlocks_and_retries() {
        let dir = must_temp_tsm_dir();
        let readers = (1..=3)
            .map(|generation| batch_delete_test_reader(dir.path(), generation))
            .collect::<Vec<_>>();
        let executor: ExecutorRef = Arc::new(RejectThirdBlockingSubmission::new());

        let batches = batch_deletes_for(&readers, Arc::clone(&executor));
        let error = batches
            .delete_range(vec![b"cpu".to_vec()], 2, i64::MAX)
            .unwrap_err();
        assert!(matches!(error, super::Error::Execution { .. }));
        for (reader, batch) in readers.iter().zip(batches.iter()) {
            assert!(matches!(
                *batch.inner.disposition.lock(),
                BatchDeleteDisposition::RolledBack
            ));
            assert!(!reader.has_tombstones());
            assert_eq!(reader.read_all(b"cpu").unwrap().len(), 2);
            assert!(reader.delete_mu.try_lock());
            unsafe { reader.delete_mu.unlock() };
        }

        let retry = batch_deletes_for(&readers, executor);
        retry
            .delete_range(vec![b"cpu".to_vec()], 2, i64::MAX)
            .unwrap();
        retry.commit().unwrap();
        for reader in &readers {
            let values = reader.read_all(b"cpu").unwrap();
            assert_eq!(values.len(), 1);
            assert_eq!(values[0].unix_nano(), 1);
            assert!(reader.delete_mu.try_lock());
            unsafe { reader.delete_mu.unlock() };
        }
    }

    #[test]
    fn batch_deletes_accepted_disposal_before_admission_rolls_back_without_mutation() {
        let dir = must_temp_tsm_dir();
        let readers = (1..=3)
            .map(|generation| batch_delete_test_reader(dir.path(), generation))
            .collect::<Vec<_>>();
        let executor: ExecutorRef = Arc::new(DropThirdAcceptedSubmission::new());

        let batches = batch_deletes_for(&readers, executor);
        let error = batches
            .delete_range(vec![b"cpu".to_vec()], 2, i64::MAX)
            .unwrap_err();
        assert!(matches!(error, super::Error::TaskTerminated));
        for reader in &readers {
            assert!(!reader.has_tombstones());
            assert_eq!(reader.read_all(b"cpu").unwrap().len(), 2);
            assert!(reader.delete_mu.try_lock());
            unsafe { reader.delete_mu.unlock() };
        }
    }

    #[test]
    fn batch_deletes_accepted_panic_rolls_back_and_unlocks() {
        let dir = must_temp_tsm_dir();
        let readers = (1..=3)
            .map(|generation| batch_delete_test_reader(dir.path(), generation))
            .collect::<Vec<_>>();
        let executor: ExecutorRef = Arc::new(PanicThirdAcceptedSubmission::new());

        let batches = batch_deletes_for(&readers, executor);
        let error = batches
            .delete_range(vec![b"cpu".to_vec()], 2, i64::MAX)
            .unwrap_err();
        assert!(matches!(error, super::Error::TaskTerminated));
        for reader in &readers {
            assert!(!reader.has_tombstones());
            assert_eq!(reader.read_all(b"cpu").unwrap().len(), 2);
            assert!(reader.delete_mu.try_lock());
            unsafe { reader.delete_mu.unlock() };
        }
    }

    #[test]
    fn test_tsm_reader_type() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");
        w.write(
            "cpu".as_bytes(),
            &[Value::IntegerValue(IntegerValue::new(0, 1i64))],
        )
        .unwrap();

        w.write_index().unwrap();
        w.close().unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let r = TSMReader::new(f, &fpath, 1, 1).unwrap();

        let typ = r.values_type("cpu".as_bytes()).unwrap();
        assert_eq!(
            typ, BLOCK_INTEGER,
            "type mismatch: got {}, exp {}",
            typ, BLOCK_INTEGER
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_tsm_reader_mmap_read_all() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        let data = HashMap::from([
            ("float", vec![Value::FloatValue(FloatValue::new(1, 1.0))]),
            ("int", vec![Value::IntegerValue(IntegerValue::new(1, 1i64))]),
            (
                "uint",
                vec![Value::UnsignedValue(UnsignedValue::new(1, !0u64))],
            ),
            ("bool", vec![Value::BoolValue(BoolValue::new(1, true))]),
            (
                "string",
                vec![Value::StringValue(StringValue::new(1, "foo".to_string()))],
            ),
        ]);

        let mut keys = vec![];
        for (k, _) in data.iter() {
            keys.push(k.to_string())
        }
        keys.sort();

        for k in keys.iter() {
            w.write(k.as_bytes(), data.get(k.as_str()).unwrap())
                .unwrap();
        }
        w.write_index().unwrap();
        w.close().unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let r = TSMReader::new(f, &fpath, 1, 1).unwrap();

        let mut count = 0;
        for (k, vals) in data.iter() {
            let read_values = r.read_all(k.as_bytes()).unwrap();

            assert!(
                read_values.len() == vals.len(),
                "read values length mismatch: got {}, exp {}",
                read_values.len(),
                vals.len()
            );

            for (i, v) in vals.iter().enumerate() {
                if v.value() != read_values[i].value() {
                    panic!(
                        "read value mismatch({}): got {:?}, exp {:?}",
                        i,
                        v.value(),
                        read_values[i].value()
                    );
                }
            }
            count += 1;
        }

        assert_eq!(
            count,
            data.len(),
            "read values count mismatch: got {}, exp {}",
            count,
            data.len()
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_tsm_reader_mmap_read() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        let data = HashMap::from([
            ("float", vec![Value::FloatValue(FloatValue::new(1, 1.0))]),
            ("int", vec![Value::IntegerValue(IntegerValue::new(1, 1i64))]),
            (
                "uint",
                vec![Value::UnsignedValue(UnsignedValue::new(1, !0u64))],
            ),
            ("bool", vec![Value::BoolValue(BoolValue::new(1, true))]),
            (
                "string",
                vec![Value::StringValue(StringValue::new(1, "foo".to_string()))],
            ),
        ]);

        let mut keys = vec![];
        for (k, _) in data.iter() {
            keys.push(k.to_string())
        }
        keys.sort();

        for k in keys.iter() {
            w.write(k.as_bytes(), data.get(k.as_str()).unwrap())
                .unwrap();
        }
        w.write_index().unwrap();
        w.close().unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let r = TSMReader::new(f, &fpath, 1, 1).unwrap();

        let mut count = 0;
        for (k, vals) in data.iter() {
            let read_values = r.read(k.as_bytes(), vals[0].unix_nano()).unwrap();

            assert!(
                read_values.len() == vals.len(),
                "read values length mismatch: got {}, exp {}",
                read_values.len(),
                vals.len()
            );

            for (i, v) in vals.iter().enumerate() {
                if v.value() != read_values[i].value() {
                    panic!(
                        "read value mismatch({}): got {:?}, exp {:?}",
                        i,
                        v.value(),
                        read_values[i].value()
                    );
                }
            }
            count += 1;
        }

        assert_eq!(
            count,
            data.len(),
            "read values count mismatch: got {}, exp {}",
            count,
            data.len()
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_tsm_reader_mmap_tombstone() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        let values = &[Value::FloatValue(FloatValue::new(0, 1.0))];
        w.write("cpu".as_bytes(), values).unwrap();
        w.write("mem".as_bytes(), values).unwrap();

        w.write_index().unwrap();
        w.close().unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let r = TSMReader::new(f, &fpath, 1, 1).unwrap();

        r.delete(&mut ["mem".as_bytes()]).unwrap();
        drop(r);

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let r = TSMReader::new(f, &fpath, 1, 1).unwrap();
        let key_count = r.key_count();
        assert_eq!(
            key_count, 1,
            "key length mismatch: got {}, exp {}",
            key_count, 1
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_tsm_reader_mmap_tombstone_range() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        let exp_values = &[
            Value::FloatValue(FloatValue::new(1, 1.0)),
            Value::FloatValue(FloatValue::new(2, 2.0)),
            Value::FloatValue(FloatValue::new(3, 3.0)),
        ];
        w.write("cpu".as_bytes(), exp_values).unwrap();

        w.write_index().unwrap();
        w.close().unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let r = Arc::new(TSMReader::new(f, &fpath, 1, 1).unwrap());

        TSMReader::delete_range(Arc::clone(&r), &mut ["cpu".as_bytes()], 2, i64::MAX).unwrap();

        assert!(
            r.contains_value("cpu".as_bytes(), 1),
            "ContainsValue mismatch: got false, exp true"
        );
        assert!(
            !r.contains_value("cpu".as_bytes(), 3),
            "ContainsValue mismatch: got true, exp false"
        );

        let values = r.read_all("cpu".as_bytes()).unwrap();

        assert!(
            values.len() == 1,
            "values length mismatch: got {}, exp 1",
            values.len()
        );
        assert_eq!(
            values[0].to_string(),
            exp_values[0].to_string(),
            "value mismatch: got {}, exp {}",
            values[0].to_string(),
            exp_values[0].to_string()
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_tsm_reader_mmap_tombstone_outside_time_range() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        let exp_values = &[
            Value::FloatValue(FloatValue::new(1, 1.0)),
            Value::FloatValue(FloatValue::new(2, 2.0)),
            Value::FloatValue(FloatValue::new(3, 3.0)),
        ];
        w.write("cpu".as_bytes(), exp_values).unwrap();

        w.write_index().unwrap();
        w.close().unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();

        let r = Arc::new(TSMReader::new(f, &fpath, 1, 1).unwrap());

        TSMReader::delete_range(Arc::clone(&r), &mut ["cpu".as_bytes()], 0, 0).unwrap();

        assert!(
            r.contains_value("cpu".as_bytes(), 1),
            "ContainsValue mismatch: got false, exp true"
        );
        assert!(
            r.contains_value("cpu".as_bytes(), 2),
            "ContainsValue mismatch: got false, exp true"
        );
        assert!(
            r.contains_value("cpu".as_bytes(), 3),
            "ContainsValue mismatch: got false, exp true"
        );

        assert!(
            !r.has_tombstones(),
            "HasTombstones mismatch: got true, exp false"
        );

        assert!(
            !r.tombstone_stats().tombstone_exists,
            "tombstone_exists mismatch: got true, exp false"
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_tsm_reader_mmap_tombstone_outside_key_range() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        let exp_values = &[
            Value::FloatValue(FloatValue::new(1, 1.0)),
            Value::FloatValue(FloatValue::new(2, 2.0)),
            Value::FloatValue(FloatValue::new(3, 3.0)),
        ];
        w.write("cpu".as_bytes(), exp_values).unwrap();

        w.write_index().unwrap();
        w.close().unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let r = Arc::new(TSMReader::new(f, &fpath, 1, 1).unwrap());

        TSMReader::delete_range(Arc::clone(&r), &mut ["mem".as_bytes()], 0, 3).unwrap();

        assert!(
            r.contains_value("cpu".as_bytes(), 1),
            "ContainsValue mismatch: got false, exp true"
        );
        assert!(
            r.contains_value("cpu".as_bytes(), 2),
            "ContainsValue mismatch: got false, exp true"
        );
        assert!(
            r.contains_value("cpu".as_bytes(), 3),
            "ContainsValue mismatch: got false, exp true"
        );

        assert!(
            !r.has_tombstones(),
            "HasTombstones mismatch: got true, exp false"
        );

        assert!(
            !r.tombstone_stats().tombstone_exists,
            "tombstone_exists mismatch: got true, exp false"
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_tsm_reader_mmap_tombstone_overlap_key_range() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        let exp_values = &[
            Value::FloatValue(FloatValue::new(1, 1.0)),
            Value::FloatValue(FloatValue::new(2, 2.0)),
            Value::FloatValue(FloatValue::new(3, 3.0)),
        ];
        w.write("cpu,app=foo,host=server-0#!~#value".as_bytes(), exp_values)
            .unwrap();
        w.write(
            "cpu,app=foo,host=server-73379#!~#value".as_bytes(),
            exp_values,
        )
        .unwrap();

        w.write_index().unwrap();
        w.close().unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let r = Arc::new(TSMReader::new(f, &fpath, 1, 1).unwrap());

        TSMReader::delete_range(
            Arc::clone(&r),
            &mut [
                "cpu,app=foo,host=server-0#!~#value".as_bytes(),
                "cpu,app=foo,host=server-73379#!~#value".as_bytes(),
                "cpu,app=foo,host=server-99999#!~#value".as_bytes(),
            ],
            i64::MIN,
            i64::MAX,
        )
        .unwrap();

        assert!(
            !r.contains("cpu,app=foo,host=server-0#!~#value".as_bytes()),
            "ContainsValue mismatch: got false, exp true"
        );
        assert!(
            !r.contains("cpu,app=foo,host=server-73379#!~#value".as_bytes()),
            "ContainsValue mismatch: got false, exp true"
        );

        assert!(
            r.has_tombstones(),
            "HasTombstones mismatch: got true, exp false"
        );

        assert!(
            r.tombstone_stats().tombstone_exists,
            "tombstone_exists mismatch: got true, exp false"
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_tsm_reader_mmap_tombstone_full_range() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        let exp_values = &[
            Value::FloatValue(FloatValue::new(1, 1.0)),
            Value::FloatValue(FloatValue::new(2, 2.0)),
            Value::FloatValue(FloatValue::new(3, 3.0)),
        ];
        w.write("cpu".as_bytes(), exp_values).unwrap();

        w.write_index().unwrap();
        w.close().unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let r = Arc::new(TSMReader::new(f, &fpath, 1, 1).unwrap());

        TSMReader::delete_range(Arc::clone(&r), &mut ["cpu".as_bytes()], i64::MIN, i64::MAX)
            .unwrap();

        let values = r.read_all("cpu".as_bytes()).unwrap();
        assert!(
            values.is_empty(),
            "values length mismatch: got Some, exp None"
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_tsm_reader_mmap_tombstone_full_range_multi() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        let exp_values = &[
            Value::FloatValue(FloatValue::new(1, 1.0)),
            Value::FloatValue(FloatValue::new(2, 2.0)),
            Value::FloatValue(FloatValue::new(3, 3.0)),
        ];
        w.write("cpu".as_bytes(), exp_values).unwrap();

        let exp_values1 = &[
            Value::FloatValue(FloatValue::new(3, 1.0)),
            Value::FloatValue(FloatValue::new(4, 2.0)),
            Value::FloatValue(FloatValue::new(5, 3.0)),
        ];
        w.write("mem".as_bytes(), exp_values1).unwrap();

        w.write_index().unwrap();
        w.close().unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let r = Arc::new(TSMReader::new(f, &fpath, 1, 1).unwrap());

        TSMReader::delete_range(
            Arc::clone(&r),
            &mut ["mem".as_bytes(), "cpu".as_bytes()],
            0,
            3,
        )
        .unwrap();

        let values = r.read_all("cpu".as_bytes()).unwrap();
        assert!(
            values.is_empty(),
            "values length mismatch: got {}, exp {}",
            values.len(),
            0
        );

        let values = r.read_all("mem".as_bytes()).unwrap();
        assert!(
            values.len() == 2,
            "values length mismatch: got {}, exp {}",
            values.len(),
            2
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_tsm_reader_mmap_tombstone_multi_ranges() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        let exp_values = &[
            Value::FloatValue(FloatValue::new(1, 1.0)),
            Value::FloatValue(FloatValue::new(2, 2.0)),
            Value::FloatValue(FloatValue::new(3, 3.0)),
            Value::FloatValue(FloatValue::new(4, 4.0)),
            Value::FloatValue(FloatValue::new(5, 5.0)),
        ];
        w.write("cpu".as_bytes(), exp_values).unwrap();

        w.write_index().unwrap();
        w.close().unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let r = Arc::new(TSMReader::new(f, &fpath, 1, 1).unwrap());

        TSMReader::delete_range(Arc::clone(&r), &mut ["cpu".as_bytes()], 2, 2).unwrap();
        TSMReader::delete_range(Arc::clone(&r), &mut ["cpu".as_bytes()], 4, 4).unwrap();

        let values = r.read_all("cpu".as_bytes()).unwrap();
        assert!(
            values.len() == 3,
            "values length mismatch: got {}, exp {}",
            values.len(),
            3
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_tsm_reader_mmap_tombstone_multi_ranges_full() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        let exp_values = &[
            Value::FloatValue(FloatValue::new(1, 1.0)),
            Value::FloatValue(FloatValue::new(2, 2.0)),
        ];
        w.write("cpu".as_bytes(), exp_values).unwrap();

        w.write_index().unwrap();
        w.close().unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let r = Arc::new(TSMReader::new(f, &fpath, 1, 1).unwrap());

        TSMReader::delete_range(Arc::clone(&r), &mut ["cpu".as_bytes()], 1, 1).unwrap();
        TSMReader::delete_range(Arc::clone(&r), &mut ["cpu".as_bytes()], 2, 2).unwrap();

        let values = r.read_all("cpu".as_bytes()).unwrap();
        assert!(
            values.is_empty(),
            "values length mismatch: got Some, exp None"
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_tsm_reader_mmap_tombstone_multi_ranges_no_overlap() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        let exp_values = &[
            Value::FloatValue(FloatValue::new(1, 1.0)),
            Value::FloatValue(FloatValue::new(2, 2.0)),
            Value::FloatValue(FloatValue::new(3, 3.0)),
        ];
        w.write("cpu".as_bytes(), exp_values).unwrap();

        w.write_index().unwrap();
        w.close().unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let r = Arc::new(TSMReader::new(f, &fpath, 1, 1).unwrap());

        TSMReader::delete_range(Arc::clone(&r), &mut ["cpu".as_bytes()], 1, 1).unwrap();
        TSMReader::delete_range(Arc::clone(&r), &mut ["cpu".as_bytes()], 3, 3).unwrap();

        let key_count = r.key_count();
        assert_eq!(
            key_count, 1,
            "key count mismatch: got {}, exp {}",
            key_count, 1
        );

        let values = r.read_all("cpu".as_bytes()).unwrap();
        assert!(
            values.len() == 1,
            "values length mismatch: got {}, exp 1",
            values.len()
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_tsm_reader_mmap_tombstone_outside_range() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        let cpu_values = vec![
            Value::FloatValue(FloatValue::new(1, 1.0)),
            Value::FloatValue(FloatValue::new(2, 2.0)),
            Value::FloatValue(FloatValue::new(3, 3.0)),
        ];
        w.write("cpu".as_bytes(), &cpu_values).unwrap();

        let mem_values = vec![
            Value::FloatValue(FloatValue::new(1, 1.0)),
            Value::FloatValue(FloatValue::new(2, 2.0)),
            Value::FloatValue(FloatValue::new(30, 3.0)),
        ];
        w.write("mem".as_bytes(), &mem_values).unwrap();

        w.write_index().unwrap();
        w.close().unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let r = Arc::new(TSMReader::new(f, &fpath, 1, 1).unwrap());

        TSMReader::delete_range(
            Arc::clone(&r),
            &mut ["cpu".as_bytes(), "mem".as_bytes()],
            5,
            i64::MAX,
        )
        .unwrap();

        let key_count = r.key_count();
        assert_eq!(
            key_count, 2,
            "key count mismatch: got {}, exp {}",
            key_count, 2
        );

        let got = r.tombstone_range("cpu".as_bytes());
        assert!(
            got.is_empty(),
            "tombstone range mismatch: got Some, exp None"
        );

        let values = r.read_all("cpu".as_bytes()).unwrap();
        assert!(
            values.len() == cpu_values.len(),
            "values length mismatch: got {}, exp {}",
            values.len(),
            cpu_values.len()
        );

        let got = r.tombstone_range("mem".as_bytes());
        assert!(
            got.len() == 1,
            "tombstone range mismatch: got {}, exp 1",
            got.len()
        );

        let values = r.read_all("mem".as_bytes()).unwrap();
        assert!(
            values.len() == mem_values.as_slice()[..2].len(),
            "values length mismatch: got {}, exp {}",
            values.len(),
            mem_values.as_slice()[..2].len()
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_tsm_reader_mmap_stats() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        let values1 = vec![Value::FloatValue(FloatValue::new(0, 1.0))];
        w.write("cpu".as_bytes(), &values1).unwrap();

        let values2 = vec![Value::FloatValue(FloatValue::new(1, 1.0))];
        w.write("mem".as_bytes(), &values2).unwrap();

        w.write_index().unwrap();
        w.close().unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let r = TSMReader::new(f, &fpath, 1, 1).unwrap();

        let stats = r.stats();
        let min_key = to_string!(stats.min_key);
        assert_eq!(
            min_key,
            "cpu".to_string(),
            "min key mismatch: got {}, exp {}",
            min_key,
            "cpu".to_string()
        );

        let max_key = to_string!(stats.max_key);
        assert_eq!(
            max_key,
            "mem".to_string(),
            "max key mismatch: got {}, exp {}",
            max_key,
            "mem".to_string()
        );

        let min_time = stats.min_time;
        assert_eq!(
            min_time,
            values1[0].unix_nano(),
            "min time mismatch: got {}, exp {}",
            min_time,
            values1[0].unix_nano()
        );

        let max_time = stats.max_time;
        assert_eq!(
            max_time,
            values2[0].unix_nano(),
            "min time mismatch: got {}, exp {}",
            max_time,
            values2[0].unix_nano()
        );

        let key_count = r.key_count();
        assert_eq!(
            key_count, 2,
            "key length mismatch: got {}, exp {}",
            key_count, 2
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_tsm_reader_vertify_file_type() {
        let dir = must_temp_tsm_dir();
        let (mut f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        // write some garbage
        let buf = [
            0x23, 0xac, 0x99, 0x22, 0x77, 0x23, 0xac, 0x99, 0x22, 0x77, 0x23, 0xac, 0x99, 0x22,
            0x77, 0x23, 0xac, 0x99, 0x22, 0x77,
        ]
        .as_slice();
        let _ = f.write(buf);

        assert!(
            TSMReader::new(f, &fpath, 1, 1).is_err(),
            "expected error trying to open non-tsm file"
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_indirect_index_entries() {
        let mut index = InmemIndex::new();
        index.add("cpu".as_bytes(), BLOCK_FLOAT64, 0, 1, 10, 100, None);
        index.add("cpu".as_bytes(), BLOCK_FLOAT64, 2, 3, 20, 200, None);
        let exp = index.entries("cpu".as_bytes()).to_vec();

        index.add("mem".as_bytes(), BLOCK_FLOAT64, 0, 1, 10, 100, None);

        let b = index.marshal_binary().unwrap();

        let indirect = IndirectIndex::new();
        indirect.unmarshal_binary(b.as_slice()).unwrap();

        let entries = indirect.entries("cpu".as_bytes());

        let (got_len, exp_len) = (entries.len(), exp.len());
        assert_eq!(
            got_len, exp_len,
            "entries length mismatch: got {}, exp {}",
            got_len, exp_len
        );

        for (i, exp) in exp.iter().enumerate() {
            let got = &entries[i];
            assert_eq!(
                got.min_time, exp.min_time,
                "min_time mismatch: got {}, exp {}",
                got.min_time, exp.min_time
            );
            assert_eq!(
                got.max_time, exp.max_time,
                "max_time mismatch: got {}, exp {}",
                got.max_time, exp.max_time
            );
            assert_eq!(
                got.size, exp.size,
                "size mismatch: got {}, exp {}",
                got.size, exp.size
            );
            assert_eq!(
                got.offset, exp.offset,
                "offset mismatch: got {}, exp {}",
                got.offset, exp.offset
            );
        }
    }

    #[test]
    fn test_indirect_index_entries_non_existent() {
        let mut index = InmemIndex::new();
        index.add("cpu".as_bytes(), BLOCK_FLOAT64, 0, 1, 10, 100, None);
        index.add("cpu".as_bytes(), BLOCK_FLOAT64, 2, 3, 20, 200, None);

        let b = index.marshal_binary().unwrap();

        let indirect = IndirectIndex::new();
        indirect.unmarshal_binary(b.as_slice()).unwrap();

        // mem has not been added to the index so we should get no entries back for both
        let exp = index.entries("mem".as_bytes());
        let entries = indirect.entries("mem".as_bytes());

        assert!(exp.is_empty() && entries.is_empty());
    }

    #[test]
    fn indirect_index_rejects_truncated_statistics_tail() {
        let mut index = InmemIndex::new();
        index.add("cpu".as_bytes(), BLOCK_INTEGER, 0, 1, 10, 100, None);
        let mut bytes = index.marshal_binary().unwrap();
        bytes.pop();

        let indirect = IndirectIndex::new();
        let error = indirect.unmarshal_binary(&bytes).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("not enough data for index entries")
        );
    }

    #[test]
    #[ignore]
    fn test_indirect_index_max_blocks() {
        let mut index = InmemIndex::new();
        for _ in 0..1 << 16 {
            index.add("cpu".as_bytes(), BLOCK_FLOAT64, 0, 1, 10, 20, None);
        }

        match index.marshal_binary() {
            Ok(_) => panic!("expected max block count error. got Ok"),
            Err(_) => {}
        }
    }

    #[test]
    fn test_indirect_index_type() {
        let mut index = InmemIndex::new();
        index.add("cpu".as_bytes(), BLOCK_INTEGER, 0, 1, 10, 100, None);

        let b = index.marshal_binary().unwrap();

        let indirect = IndirectIndex::new();
        indirect.unmarshal_binary(b.as_slice()).unwrap();

        let typ = indirect.block_type("cpu".as_bytes()).unwrap();
        assert_eq!(
            typ, BLOCK_INTEGER,
            "type mismatch: got {}, exp {}",
            typ, BLOCK_INTEGER
        );
    }

    #[test]
    fn test_direct_index_key_count() {
        let mut index = InmemIndex::new();
        index.add("cpu".as_bytes(), BLOCK_FLOAT64, 0, 1, 10, 100, None);
        index.add("cpu".as_bytes(), BLOCK_FLOAT64, 1, 2, 20, 30, None);
        index.add("mem".as_bytes(), BLOCK_FLOAT64, 0, 1, 10, 100, None);

        // 2 distinct keys
        let got = index.key_count();
        assert_eq!(got, 2, "length mismatch: got {}, exp {}", got, 2);
    }

    #[test]
    fn test_block_iter_single() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        let values = vec![Value::IntegerValue(IntegerValue::new(0, 1i64))];
        w.write("cpu".as_bytes(), &values).unwrap();

        w.write_index().unwrap();
        w.close().unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let r = TSMReader::new(f, &fpath, 1, 1).unwrap();

        let mut count = 0;
        let itr = TSMReader::block_iter(Arc::new(r));
        for BlockIterItem {
            key,
            min_time,
            max_time,
            typ,
            block: buf,
            ..
        } in itr
        {
            let (got, exp) = (key, "cpu".as_bytes().to_vec());
            assert_eq!(got, exp, "key mismatch: got {:?}, exp: {:?}", got, exp);

            assert_eq!(
                min_time, 0,
                "min_time mismatch: got {}, exp: {}",
                min_time, 0
            );
            assert_eq!(
                max_time, 0,
                "max_time mismatch: got {}, exp: {}",
                max_time, 0
            );
            assert_eq!(
                typ, BLOCK_INTEGER,
                "type mismatch: got {}, exp: {}",
                typ, BLOCK_INTEGER
            );
            assert!(!buf.is_empty(), "buf is empty");

            count += 1;
        }

        assert_eq!(
            count,
            values.len(),
            "value count mismatch: got {}, exp {}",
            count,
            values.len()
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    // #[test]
    // fn test_block_iter_tombstone() {
    //     let dir = must_temp_tsm_dir();
    //     let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

    //     let mut w = TSMWriter::new(f, 1, 1, "");

    //     let values = Values(vec![
    //         Value::IntegerValue(IntegerValue::new(0, 1i64)),
    //     ]);
    //     w.write("cpu".as_bytes(), values.clone()).unwrap();
    //     w.write("mem".as_bytes(), values.clone()).unwrap();

    //     w.write_index().unwrap();
    //     w.close().unwrap();

    //     let f = std::fs::OpenOptions::new()
    //     .read(true)
    //     .open(fpath.clone())
    //     .unwrap();

    //     let r = Arc::new(TSMReader::new(f, &fpath, 1, 1).unwrap());

    //     let mut itr = TSMReader::block_iter(Arc::clone(&r)).peekable();
    //     while itr.peek().is_some() {
    //         r.delete(&mut ["cpu".as_bytes()]).unwrap();
    //     }
    //     // if iter.Err() == nil {
    //     //     t.Fatalf("expected error: got nil")
    //     // }
    // }

    #[test]
    fn test_block_iter_multi_blocks() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        let values1 = vec![Value::IntegerValue(IntegerValue::new(0, 1i64))];
        w.write("cpu".as_bytes(), &values1).unwrap();

        let values2 = vec![Value::IntegerValue(IntegerValue::new(1, 2i64))];
        w.write("cpu".as_bytes(), &values2).unwrap();

        w.write_index().unwrap();
        w.close().unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let r = TSMReader::new(f, &fpath, 1, 1).unwrap();

        let mut count = 0;
        let exp_data = vec![values1, values2];
        let mut i = 0;
        let itr = TSMReader::block_iter(Arc::new(r));
        for BlockIterItem {
            key,
            min_time,
            max_time,
            typ,
            block: buf,
            ..
        } in itr
        {
            let (got, exp) = (key, "cpu".as_bytes().to_vec());
            assert_eq!(got, exp, "key mismatch: got {:?}, exp: {:?}", got, exp);

            assert_eq!(
                min_time,
                exp_data[i][0].unix_nano(),
                "min_time mismatch: got {}, exp: {}",
                min_time,
                0
            );
            assert_eq!(
                max_time,
                exp_data[i][0].unix_nano(),
                "max_time mismatch: got {}, exp: {}",
                max_time,
                0
            );
            assert_eq!(
                typ, BLOCK_INTEGER,
                "type mismatch: got {}, exp: {}",
                typ, BLOCK_INTEGER
            );
            assert!(!buf.is_empty(), "buf is empty");

            count += 1;
            i += 1;
        }

        assert_eq!(count, 2, "value count mismatch: got {}, exp {}", count, 2);

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_block_iter_sorted() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        let values = HashMap::from([
            ("mem", vec![Value::IntegerValue(IntegerValue::new(0, 1i64))]),
            (
                "cycles",
                vec![Value::UnsignedValue(UnsignedValue::new(0, !0u64))],
            ),
            ("cpu", vec![Value::FloatValue(FloatValue::new(1, 2.0))]),
            ("disk", vec![Value::BoolValue(BoolValue::new(1, true))]),
            (
                "load",
                vec![Value::StringValue(StringValue::new(
                    1,
                    "string".to_string(),
                ))],
            ),
        ]);

        let mut keys = vec![];
        for (k, _) in values.iter() {
            keys.push(k.to_string())
        }
        keys.sort();

        for k in keys.iter() {
            w.write(k.as_bytes(), values.get(k.as_str()).unwrap())
                .unwrap();
        }

        w.write_index().unwrap();
        w.close().unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let r = TSMReader::new(f, &fpath, 1, 1).unwrap();

        let mut count = 0;
        let itr = TSMReader::block_iter(Arc::new(r));
        let mut last_key = "".to_string();
        for BlockIterItem {
            key, block: buf, ..
        } in itr
        {
            let key = to_string!(key);
            if key < last_key {
                panic!("keys not sorted: got {:?}, exp: {:?}", key, last_key);
            }

            last_key = key;

            assert!(!buf.is_empty(), "buf is empty");

            count += 1;
        }

        assert_eq!(
            count,
            values.len(),
            "value count mismatch: got {}, exp {}",
            count,
            values.len()
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_indirect_index_unmarshal_binary_block_count_overflow() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        for i in 0..3280 {
            w.write(
                "cpu".as_bytes(),
                &[Value::FloatValue(FloatValue::new(i as i64, i as f64))],
            )
            .unwrap();
        }

        w.write_index().unwrap();
        w.close().unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let r = TSMReader::new(f, &fpath, 1, 1).unwrap();

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    fn test_compacted_not_full() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        let values = &[Value::FloatValue(FloatValue::new(0, 1.0))];
        w.write("cpu".as_bytes(), values).unwrap();

        w.write_index().unwrap();
        w.close().unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let r = TSMReader::new(f, &fpath, 1, 1).unwrap();

        let mut itr = TSMReader::block_iter(Arc::new(r)).peekable();
        if itr.peek().is_none() {
            panic!("expected next, got none");
        }

        let buf = itr.next().unwrap().block;

        let cnt = block_count(buf.as_slice()).unwrap();
        assert_eq!(cnt, 1, "block counmt mismatch: got {}, exp 1", cnt);

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    // TODO: implement me
    // #[test]
    // fn test_tsm_reader_fuzz_crashes() {
    //     let cases = vec![
    //         "",
    //         "\x16\xd1\x16\xd1\x01\x10\x14X\xfb\x03\xac~\x80\xf0\x00\x00\x00I^K" +
    //             "_\xf0\x00\x00\x00D424259389w\xf0\x00\x00\x00" +
    //             "o\x93\bO\x10?\xf0\x00\x00\x00\x00\b\x00\xc2_\xff\xd8\x0fX^" +
    //             "/\xbf\xe8\x00\x00\x00\x00\x00\x01\x00\bctr#!~#n\x00" +
    //             "\x00\x01\x14X\xfb\xb0\x03\xac~\x80\x14X\xfb\xb1\x00\xd4ܥ\x00\x00" +
    //             "\x00\x00\x00\x00\x00\x05\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00E",
    //         "\x16\xd1\x16\xd1\x01\x80'Z\\\x00\v)\x00\x00\x00\x00;\x9a\xca\x00" +
    //             "\x01\x05\x10?\xf0\x00\x00\x00\x00\x00\x00\xc2_\xff\xd6\x1d\xd4&\xed\v" +
    //             "\xc5\xf7\xfb\xc0\x00\x00\x00\x00\x00 \x00\x06a#!~#v\x00\x00" +
    //             "\x01\x00\x00\x00\x00;\x9a\xca\x00\x00\x00\x00\x01*\x05\xf2\x00\x00\x00\x00" +
    //             "\x00\x00\x00\x00\x00\x00\x00\x00\x002",
    //         "\x16\xd1\x16\xd1\x01\x80\xf0\x00\x00\x00I^K_\xf0\x00\x00\x00D7" +
    //             "\nw\xf0\x00\x00\x00o\x93\bO\x10?\xf0\x00\x00\x00\x00\x00\x00\xc2" +
    //             "_\xff\x14X\xfb\xb0\x03\xac~\x80\x14X\xfb\xb1\x00\xd4ܥ\x00\x00" +
    //             "\x00\x00\x00\x00\x00\x05\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00E",
    //         "\x16\xd1\x16\xd1\x01000000000000000" +
    //             "00000000000000000000" +
    //             "0000000000\x00\x000\x00\x0100000" +
    //             "000\x00\x00\x00\x00\x00\x00\x002",
    //         "\x16\xd1\x16\xd1\x01",
    //         "\x16\xd1\x16\xd1\x01\x00\x00o\x93\bO\x10?\xf0\x00\x00\x00\x00X^" +
    //             "/\xbf\xe8\x00\x00\x00\x00\x00\x01\x00\bctr#!~#n\x00" +
    //             "\x00\x01\x14X\xfb\xb0\x03\xac~\x80\x14X\xfb\xb1\x00\xd4ܥ\x00\x00" +
    //             "\x00\x00\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00E",
    //     ];
    // }

    #[test]
    fn test_tsm_reader_references() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut w = TSMWriter::new(f, 1, 1, "");

        let data = HashMap::from([
            ("float", vec![Value::FloatValue(FloatValue::new(1, 1.0))]),
            ("int", vec![Value::IntegerValue(IntegerValue::new(1, 1i64))]),
            (
                "uint",
                vec![Value::UnsignedValue(UnsignedValue::new(1, !0u64))],
            ),
            ("bool", vec![Value::BoolValue(BoolValue::new(1, true))]),
            (
                "string",
                vec![Value::StringValue(StringValue::new(1, "foo".to_string()))],
            ),
        ]);

        let mut keys = vec![];
        for (k, _) in data.iter() {
            keys.push(k.to_string())
        }
        keys.sort();

        for k in keys.iter() {
            w.write(k.as_bytes(), data.get(k.as_str()).unwrap())
                .unwrap();
        }
        w.write_index().unwrap();
        w.close().unwrap();

        let f = std::fs::OpenOptions::new()
            .read(true)
            .open(fpath.clone())
            .unwrap();
        let r = TSMReader::new(f, &fpath, 1, 1).unwrap();

        r.ref_reader();

        let err = r.remove().err().unwrap();
        assert_eq!(
            err.to_string(),
            super::Error::FileInUse.to_string(),
            "expected error removing reader: {:?}",
            err
        );

        let mut count = 0;
        for (k, vals) in data.iter() {
            let read_values = r.read(k.as_bytes(), vals[0].unix_nano()).unwrap();

            assert!(
                read_values.len() == vals.len(),
                "read values length mismatch: got {}, exp {}",
                read_values.len(),
                vals.len()
            );

            for (i, v) in vals.iter().enumerate() {
                if v.value() != read_values[i].value() {
                    panic!(
                        "read value mismatch({}): got {:?}, exp {:?}",
                        i,
                        v.value(),
                        read_values[i].value()
                    );
                }
            }
            count += 1;
        }

        assert_eq!(
            count,
            data.len(),
            "read values count mismatch: got {}, exp {}",
            count,
            data.len()
        );

        r.unref_reader();
        r.close().unwrap();

        r.remove().unwrap();

        std::fs::remove_dir_all(dir.path()).unwrap();
    }
}
