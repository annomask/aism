use std::{
    cmp::Ordering::{Equal, Greater, Less},
    collections::{HashMap, VecDeque},
    fs::File,
    hash::Hash,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom},
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
    BOOL, BoolValue, FLOAT, FloatValue, INTEGER, IntegerValue, STRING, StringValue, UNSIGNED,
    UnsignedValue, Value,
};
use crate::runtime::execution::{
    ExecutorRef, RuntimeExecutorExt, SpawnError, TaskAdmissionGate, accepted_task_guard,
    legacy_global_executor,
};
use bytes::{Buf, BufMut};
use parking_lot::{Mutex, RwLock, lock_api::RawMutex};
use snafu::{ResultExt, Snafu, ensure};

use super::{vlog_index_cache::Cache, vlog_index_ring::VLogIndexes};
use crate::{
    common::slow_trace,
    storage::tsm1::{
        cursor::array_values::{BoolArray, FloatArray, IntegerArray, StringArray, UnsignedArray},
        data_encoding::{
            array_encoding::{
                decode_bool_array_block, decode_float_array_block, decode_integer_array_block,
                decode_string_array_block, decode_unsigned_array_block,
            },
            encoding::{
                self, decode_bool_block, decode_float_block, decode_integer_block,
                decode_string_block, decode_unsigned_block,
            },
            encoding_values::{
                BoolValues, FloatValues, IntegerValues, StringValues, UnsignedValues, Values,
            },
        },
        vlog::{
            VLogIndex,
            compact::Block,
            file_store::{FileStat, TombstoneStat, VLogFilePublishMeta},
            tombstone::{Tombstone, Tombstoner},
            writer::{MAGIC_NUMBER, VERSION},
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

    /// IO error.
    #[snafu(display("tsm writer error: {reason}"))]
    IO { reason: std::io::Error },

    /// Tombstone error.
    #[snafu(display("{}", source))]
    Tombstone { source: super::tombstone::Error },

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

impl Hash for VLogReader {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.generation.hash(state);
        self.seq.hash(state);
    }
}

impl PartialEq for VLogReader {
    fn eq(&self, other: &Self) -> bool {
        self.generation == other.generation && self.seq == other.seq
    }
}

impl Eq for VLogReader {}

impl PartialOrd for VLogReader {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VLogReader {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.generation
            .cmp(&other.generation)
            .then(self.seq.cmp(&other.seq))
    }
}

pub struct VLogReader {
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
    accessor: Accessor, // TODO: blockAccessor interface

    // index is the index of all blocks.
    index: Arc<IndirectIndex>,

    // tombstoner ensures tombstoned keys are not available by the index.
    pub tombstoner: Tombstoner,
    // size is the size of the file on disk.
    size: u64,

    // lastModified is the last time this file was modified on disk
    last_modified: i64,
}

impl VLogReader {
    /// Opens a data-only reader whose block locations are supplied by index files.
    pub(crate) fn new_bare(path: &Path, generation: i64, seq: i64) -> Result<Self> {
        let f = File::open(path)?;
        let stat = f.metadata()?;
        let accessor = Accessor::new(path, f);
        let index = {
            let mut inner = accessor.inner.write();
            vertify_version(&mut inner.f)?;
            inner.size = stat.len();
            inner.f.seek(SeekFrom::Start(0))?;
            Arc::clone(&inner.index)
        };
        // Deletes need index-backed key enumeration and are enabled separately.
        let tombstoner = Tombstoner::new(path.to_path_buf());
        Ok(Self {
            generation,
            seq,
            refs: AtomicI64::new(0),
            ref_wg: WaitGroup::new(),
            inner: RwLock::new(Inner {
                accessor,
                index,
                tombstoner,
                size: stat.len(),
                last_modified: stat.modified()?.duration_since(UNIX_EPOCH)
                    .map_err(|e| Error::StringErr { reason: e.to_string() })?.as_nanos() as i64,
            }),
            delete_mu: parking_lot::RawMutex::INIT,
        })
    }

    pub fn new(
        f: File,
        path: &Path,
        generation: i64,
        seq: i64,
        keys: Vec<(Vec<u8>, u8)>,
        vlog_index_cache: Arc<Cache>,
    ) -> Result<Self> {
        ensure!(!keys.is_empty(), TSMClosedSnafu);

        // TODO: madviseWillNeed
        let stat = std::fs::metadata(path)?;
        let mut accessor = Accessor {
            access_count: AtomicU64::new(0),
            inner: RwLock::new(InnerAccessor {
                free_count: AtomicU64::default(),
                path: path.to_path_buf(),
                f: BufReader::new(f),
                size: 0,
                index: Arc::new(IndirectIndex::default()),
            }),
        };

        let mut vlog_indexes: Vec<Vec<VLogIndex>> = vec![];
        for (key, _typ) in keys.iter() {
            vlog_indexes.push(
                vlog_index_cache
                    .values_by_gen_and_seq(key, generation, seq)
                    .into(),
            );
        }

        let index = accessor.init(keys, vlog_indexes)?;
        let tombstoner = Tombstoner::new(path.to_path_buf());

        let last_modified = stat
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        let inner = Inner {
            accessor,
            tombstoner,
            size: stat.size(),
            last_modified,
            index,
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

    pub fn from_publish_metadata(
        f: File,
        path: &Path,
        mut meta: VLogFilePublishMeta,
    ) -> Result<Self> {
        ensure!(!meta.keys.is_empty(), TSMClosedSnafu);

        #[cfg(any(test, debug_assertions))]
        validate_publish_metadata(&meta);

        let stat = std::fs::metadata(path)?;
        let mut accessor = Accessor {
            access_count: AtomicU64::new(0),
            inner: RwLock::new(InnerAccessor {
                free_count: AtomicU64::default(),
                path: path.to_path_buf(),
                f: BufReader::new(f),
                size: 0,
                index: Arc::new(IndirectIndex::default()),
            }),
        };

        let min_key = meta.min_key.take().ok_or_else(|| Error::StringErr {
            reason: "publish metadata missing min_key".to_string(),
        })?;
        let max_key = meta.max_key.take().ok_or_else(|| Error::StringErr {
            reason: "publish metadata missing max_key".to_string(),
        })?;
        let min_time = meta.min_time.ok_or_else(|| Error::StringErr {
            reason: "publish metadata missing min_time".to_string(),
        })?;
        let max_time = meta.max_time.ok_or_else(|| Error::StringErr {
            reason: "publish metadata missing max_time".to_string(),
        })?;

        let generation = meta.generation;
        let seq = meta.seq;
        let index = accessor.init_with_publish_metadata(
            std::mem::take(&mut meta.keys),
            std::mem::take(&mut meta.indexes),
            min_key,
            max_key,
            min_time,
            max_time,
        )?;
        let tombstoner = Tombstoner::new(path.to_path_buf());

        let last_modified = stat
            .modified()
            .unwrap()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64;
        let inner = Inner {
            accessor,
            tombstoner,
            size: stat.size(),
            last_modified,
            index,
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

                batch.push(ts.key.clone());
                if batch.len() >= 4096 {
                    let keys = &mut batch.iter().map(|v| v.as_slice()).collect::<Vec<&[u8]>>();
                    inner.index.delete_range(keys, cur.min, cur.max);
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

    /// Returns the path of the file the VLogReader was initialized with.
    pub fn path(&self) -> PathBuf {
        let inner = self.inner.read();
        inner.accessor.path()
    }

    // pub fn file_name(&self) -> String {
    //     let inner = self.inner.read();
    //     inner.accessor.file_name()
    // }

    /// Returns the key and the underlying entry at the numeric index.
    pub fn key(&self, index: usize, entries: &mut [VLogIndex]) -> (Vec<u8>, u8, Vec<VLogIndex>) {
        let inner = self.inner.read();
        inner.index.key(index, entries)
    }

    /// Returns the key and key type at position idx in the index.
    pub fn key_at(&self, idx: usize) -> (Vec<u8>, u8) {
        let inner = self.inner.read();
        inner.index.key_at(idx)
    }

    pub fn keys(&self) -> Vec<(Vec<u8>, u8)> {
        let inner = self.inner.read();
        inner.index.keys()
    }

    // pub fn seek(&self, key: &[u8]) -> usize {
    //     let inner = self.inner.read();
    //     inner.index.seek(key)
    // }

    /// Returns the values corresponding to the block at the given key and timestamp.
    pub fn read(&self, key: &[u8], timestamp: i64) -> Result<Vec<Value>> {
        let inner = self.inner.read();
        inner.accessor.read(key, timestamp)
    }

    /// Returns the values corresponding to the block at the given key and timestamp.
    pub fn read_block(&self, offset: u64, size: u64) -> Result<Vec<Value>> {
        let inner = self.inner.read();
        inner.accessor.read_block(offset, size)
    }

    /// Reads an aggregate-index block, verifying its checksum before decoding.
    /// The accessor's IO lock is released before checksum and decode work.
    pub(crate) fn read_checked_block(&self, offset: u64, size: u64) -> Result<Vec<Value>> {
        if size < 5 || offset.checked_add(size).is_none_or(|end| end > self.size()) {
            return Err(Error::StringErr { reason: "invalid VLog block range".into() });
        }
        let started = slow_trace::now();
        let (checksum, data) = self.read_bytes(offset, size, &[])?;
        slow_trace::log_query_stage("query_vlog_lookup", started, "checked VLog block IO");
        let started = slow_trace::now();
        if crc32fast::hash(&data) != checksum {
            return Err(Error::StringErr { reason: "VLog block checksum mismatch".into() });
        }
        let result = encoding::decode_block(&data).context(EncodingSnafu);
        slow_trace::log_query_stage("query_block_decode", started, "checked VLog block");
        result
    }

    /// Duplicates an immutable file handle under short guards, before query IO.
    pub(crate) fn aggregate_file_handle(&self) -> Result<(File, u64)> {
        #[cfg(unix)]
        {
            let inner = self.inner.read();
            let accessor = inner.accessor.inner.read();
            Ok((accessor.f.get_ref().try_clone()?, accessor.size))
        }
        #[cfg(windows)]
        {
            // A separate handle keeps positioned reads independent of BufReader's cursor.
            let (path, size) = {
                let inner = self.inner.read();
                let accessor = inner.accessor.inner.read();
                (accessor.path.clone(), accessor.size)
            };
            Ok((File::open(path)?, size))
        }
    }

    /// Returns all values for a key in all blocks.
    pub fn read_all(&self, indexes: &[VLogIndex]) -> Result<Vec<Value>> {
        let inner = self.inner.read();
        inner.accessor.read_all(indexes)
    }

    pub fn read_bytes(&self, offset: u64, size: u64, b: &[u8]) -> Result<(u32, Vec<u8>)> {
        let lock_started = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait("vlog_reader.read", lock_started, "read VLog bytes");
        inner.accessor.read_bytes(offset, size, b)
    }

    pub fn read_multi_bytes(&self, indexes: &[VLogIndex], b: &[u8]) -> Result<Vec<(u32, Vec<u8>)>> {
        let inner = self.inner.read();
        inner.accessor.read_multi_bytes(indexes, b)
    }

    pub fn read_multi_block(&self, typ: u8, indexes: &[VLogIndex], b: &[u8]) -> Result<Vec<Block>> {
        let inner = self.inner.read();
        inner.accessor.read_multi_block(typ, indexes, b)
    }

    /// Returns the integer values corresponding to the given index entry.
    pub fn read_integer_block(&self, offset: u64, size: u64) -> Result<Vec<IntegerValue>> {
        let inner = self.inner.read();
        inner.accessor.read_integer_block(offset, size)
    }

    pub fn read_integer_multi_block(&self, indexes: &[VLogIndex]) -> Result<Vec<IntegerValue>> {
        let inner = self.inner.read();
        inner.accessor.read_integer_multi_block(indexes)
    }

    pub fn read_integer_multi_block_and_exclude_tombstones(
        &self,
        r: &Arc<VLogReader>,
        key: &[u8],
        indexes: &[VLogIndex],
    ) -> Result<Vec<IntegerValue>> {
        let lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait(
            "vlog_reader.read",
            lock_start,
            "stage=query_wait_for_vlog_lock",
        );
        inner
            .accessor
            .read_integer_multi_block_and_exclude_tombstones(r, key, indexes)
    }

    /// Fills vals with the integer values corresponding to the given index entry.
    pub fn read_integer_array_block(
        &self,
        offset: u64,
        size: u64,
        vals: &mut IntegerArray,
    ) -> Result<()> {
        let inner = self.inner.read();
        inner.accessor.read_integer_array_block(offset, size, vals)
    }

    /// Returns the unsigned values corresponding to the given index entry.
    pub fn read_unsigned_block(&self, offset: u64, size: u64) -> Result<Vec<UnsignedValue>> {
        let inner = self.inner.read();
        inner.accessor.read_unsigned_block(offset, size)
    }

    pub fn read_unsigned_multi_block(&self, indexes: &[VLogIndex]) -> Result<Vec<UnsignedValue>> {
        let inner = self.inner.read();
        inner.accessor.read_unsigned_multi_block(indexes)
    }

    pub fn read_unsigned_multi_block_and_exclude_tombstones(
        &self,
        r: &Arc<VLogReader>,
        key: &[u8],
        indexes: &[VLogIndex],
    ) -> Result<Vec<UnsignedValue>> {
        let lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait(
            "vlog_reader.read",
            lock_start,
            "stage=query_wait_for_vlog_lock",
        );
        inner
            .accessor
            .read_unsigned_multi_block_and_exclude_tombstones(r, key, indexes)
    }

    /// Fills vals with the unsigned values corresponding to the given index entry.
    pub fn read_unsigned_array_block(
        &self,
        offset: u64,
        size: u64,
        vals: &mut UnsignedArray,
    ) -> Result<()> {
        let inner = self.inner.read();
        inner.accessor.read_unsigned_array_block(offset, size, vals)
    }

    /// Returns the float values corresponding to the given index entry.
    pub fn read_float_block(&self, offset: u64, size: u64) -> Result<Vec<FloatValue>> {
        let inner = self.inner.read();
        inner.accessor.read_float_block(offset, size)
    }

    pub fn read_float_multi_block(&self, indexes: &[VLogIndex]) -> Result<Vec<FloatValue>> {
        let inner = self.inner.read();
        inner.accessor.read_float_multi_block(indexes)
    }

    pub fn read_float_multi_block_and_exclude_tombstones(
        &self,
        r: &Arc<VLogReader>,
        key: &[u8],
        indexes: &[VLogIndex],
    ) -> Result<Vec<FloatValue>> {
        let lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait(
            "vlog_reader.read",
            lock_start,
            "stage=query_wait_for_vlog_lock",
        );
        inner
            .accessor
            .read_float_multi_block_and_exclude_tombstones(r, key, indexes)
    }

    /// Fills vals with the float values corresponding to the given index entry.
    pub fn read_float_array_block(
        &self,
        offset: u64,
        size: u64,
        vals: &mut FloatArray,
    ) -> Result<()> {
        let inner = self.inner.read();
        inner.accessor.read_float_array_block(offset, size, vals)
    }

    /// Returns the bool values corresponding to the given index entry.
    pub fn read_bool_block(&self, offset: u64, size: u64) -> Result<Vec<BoolValue>> {
        let inner = self.inner.read();
        inner.accessor.read_bool_block(offset, size)
    }

    pub fn read_bool_multi_block(&self, indexes: &[VLogIndex]) -> Result<Vec<BoolValue>> {
        let inner = self.inner.read();
        inner.accessor.read_bool_multi_block(indexes)
    }

    pub fn read_bool_multi_block_and_exclude_tombstones(
        &self,
        r: &Arc<VLogReader>,
        key: &[u8],
        indexes: &[VLogIndex],
    ) -> Result<Vec<BoolValue>> {
        let lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait(
            "vlog_reader.read",
            lock_start,
            "stage=query_wait_for_vlog_lock",
        );
        inner
            .accessor
            .read_bool_multi_block_and_exclude_tombstones(r, key, indexes)
    }

    /// Fills vals with the bool values corresponding to the given index entry.
    pub fn read_bool_array_block(
        &self,
        offset: u64,
        size: u64,
        vals: &mut BoolArray,
    ) -> Result<()> {
        let inner = self.inner.read();
        inner.accessor.read_bool_array_block(offset, size, vals)
    }

    /// Returns the string values corresponding to the given index entry.
    pub fn read_string_block(&self, offset: u64, size: u64) -> Result<Vec<StringValue>> {
        let inner = self.inner.read();
        inner.accessor.read_string_block(offset, size)
    }

    pub fn read_string_multi_block(&self, indexes: &[VLogIndex]) -> Result<Vec<StringValue>> {
        let inner = self.inner.read();
        inner.accessor.read_string_multi_block(indexes)
    }

    pub fn read_string_multi_block_and_exclude_tombstones(
        &self,
        r: &Arc<VLogReader>,
        key: &[u8],
        indexes: &[VLogIndex],
    ) -> Result<Vec<StringValue>> {
        let lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait(
            "vlog_reader.read",
            lock_start,
            "stage=query_wait_for_vlog_lock",
        );
        inner
            .accessor
            .read_string_multi_block_and_exclude_tombstones(r, key, indexes)
    }

    /// Fills vals with the string values corresponding to the given index entry.
    pub fn read_string_array_block(
        &self,
        offset: u64,
        size: u64,
        vals: &mut StringArray,
    ) -> Result<()> {
        let inner = self.inner.read();
        inner.accessor.read_string_array_block(offset, size, vals)
    }

    /// Returns the type of values stored at the given key.
    pub fn values_type(&self, key: &[u8]) -> Result<u8> {
        let inner = self.inner.read();
        inner.index.values_type(key)
    }

    /// Close the VLogReader.
    pub fn close(&self) -> Result<()> {
        self.ref_wg.wait();
        let inner = self.inner.write();
        inner.accessor.close()
        // inner.index.close()
    }

    /// ref_reader records a usage of this VLogReader.  If there are active references
    /// when the reader is closed or removed, the reader will remain open until
    /// there are no more references.
    pub fn ref_reader(&self) {
        let _ = self.refs.fetch_add(1, Ordering::SeqCst);
        self.ref_wg.add(1);
    }

    /// unref_reader removes a usage record of this VLogReader.  If the Reader was closed
    /// by another goroutine while there were active references, the file will
    /// be closed and remove
    pub fn unref_reader(&self) {
        let _ = self.refs.fetch_sub(1, Ordering::SeqCst);
        self.ref_wg.done();
    }

    /// Returns whether the VLogReader currently has any active references.
    pub fn in_use(&self) -> bool {
        let refs = self.refs.load(Ordering::SeqCst);
        refs > 0
    }

    pub fn use_count(&self) -> i64 {
        self.refs.load(Ordering::SeqCst)
    }

    /// Removes any underlying files stored on disk for this reader.
    pub fn remove(&self) -> Result<()> {
        let inner = self.inner.write();

        let path = inner.accessor.path();
        ensure!(!self.in_use(), FileInUseSnafu);

        // println!("\t\t\t VLogReader.remove, file: {:?}", path);

        if path.exists() {
            // println!("\t\t\t VLogReader.remove, file exists");
            std::fs::remove_file(path)?;
            // println!("\t\t\t VLogReader.remove, ok");
            return Ok(());
        }

        inner.tombstoner.delete().context(TombstoneSnafu)
    }

    /// Renames the underlying file to the new path.
    pub fn rename(&self, path: &Path) -> Result<()> {
        let inner = self.inner.write();
        inner.accessor.rename(path)
    }

    /// Renames the underlying file to the new path and update file name.
    pub fn rename_and_update_file_name(&self, path: &Path) -> Result<()> {
        let inner = self.inner.write();
        inner.accessor.rename_and_update_file_name(path)
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
        r: Arc<VLogReader>,
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

    // /// Returns true if the key range of the file intersect min and max.
    // pub fn overlaps_key_range(&self, min: &[u8], max: &[u8]) -> bool {
    //     self.inner.read().index.overlaps_key_range(min, max)
    // }

    // /// Returns the min and max time across all keys in the file.
    // pub fn time_range(&self) -> (i64, i64) {
    //     self.inner.read().index.time_range()
    // }

    /// Returns the min and max key across all keys in the file.
    pub fn key_range(&self) -> (Vec<u8>, Vec<u8>) {
        self.inner.read().index.key_range()
    }

    /// Returns the count of unique keys in the VLogReader.
    pub fn key_count(&self) -> usize {
        self.inner.read().index.key_count()
    }

    /// Returns all index entries for key.
    pub fn entries(&self, key: &[u8]) -> Vec<VLogIndex> {
        self.inner.read().index.entries(key)
    }

    /// Reads the index entries for key into entries.
    pub fn read_entries(&self, key: &[u8], entries: Option<&mut Vec<VLogIndex>>) -> Vec<VLogIndex> {
        self.inner.read().index.read_entries(key, entries)
    }

    // /// Returns the size of the index in bytes.
    // pub fn index_size(&self) -> u32 {
    //     let inner = self.inner.read();
    //     inner.index.size()
    // }

    /// Returns the size of the underlying file in bytes.
    pub fn size(&self) -> u64 {
        let inner = self.inner.read();
        inner.size as u64
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

    /// Returns the FileStat for the VLogReader's underlying file.
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
            merged: 0,
        }
    }

    // /// Returns a BlockIter for the underlying TSM file.
    // pub fn block_iter(r: Arc<VLogReader>) -> BlockIter {
    //     let n = r.inner.read().index.key_count();
    //     BlockIter {
    //         r,
    //         i: 0,
    //         n,
    //         key: vec![],
    //         cache: vec![],
    //         entries: VecDeque::new(),
    //         err: None,
    //         typ: 0,
    //     }
    // }

    pub fn batch_delete(r: Arc<VLogReader>) -> BatchDelete {
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
    reader: Arc<VLogReader>,
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
    pub fn new(r: Arc<VLogReader>) -> Self {
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

#[cfg(any(test, debug_assertions))]
fn validate_publish_metadata(meta: &VLogFilePublishMeta) {
    assert_eq!(
        meta.keys.len(),
        meta.indexes.len(),
        "publish metadata keys/indexes length mismatch"
    );
    let index_count = meta.indexes.iter().map(Vec::len).sum::<usize>();
    assert_eq!(
        index_count, meta.index_count,
        "publish metadata index_count mismatch"
    );
    assert!(
        meta.keys.windows(2).all(|w| w[0].0 <= w[1].0),
        "publish metadata keys must be sorted"
    );

    let mut min_time = i64::MAX;
    let mut max_time = i64::MIN;
    for ((key, typ), indexes) in meta.keys.iter().zip(meta.indexes.iter()) {
        assert!(
            indexes.windows(2).all(|w| w[0].min <= w[1].min),
            "publish metadata per-key indexes must be sorted"
        );
        for idx in indexes {
            assert_eq!(idx.generation, meta.generation);
            assert_eq!(idx.seq, meta.seq);
            assert_eq!(idx.typ, *typ);
            min_time = min_time.min(idx.min);
            max_time = max_time.max(idx.max);
        }
        assert!(!key.is_empty(), "publish metadata key must not be empty");
    }

    assert_eq!(
        meta.min_key.as_ref(),
        meta.keys.first().map(|(key, _)| key),
        "publish metadata min_key mismatch"
    );
    assert_eq!(
        meta.max_key.as_ref(),
        meta.keys.last().map(|(key, _)| key),
        "publish metadata max_key mismatch"
    );
    assert_eq!(meta.min_time, Some(min_time));
    assert_eq!(meta.max_time, Some(max_time));
}

// BlockIter allows iterating over each block in a TSM file in order.  It provides
// raw access to the block bytes without decoding them.
pub struct BlockIter {
    pub r: Arc<VLogReader>,

    indexes: Vec<VLogIndex>,

    pos: usize,

    err: Option<String>,
    typ: u8,
}

impl BlockIter {
    // fn read(&self) -> (Vec<u8>, i64, i64, u8, u32, Vec<u8>, Option<String>) {
    //     if let Ok((checksum, buf)) = self.r.read_bytes(&self.entries[0], &[]) {
    //         return (
    //             self.key.clone(),
    //             self.entries[0].min_time,
    //             self.entries[0].max_time,
    //             self.typ,
    //             checksum,
    //             buf,
    //             self.err.clone(),
    //         );
    //     }
    //     (vec![], 0, 0, 0, 0, vec![], None)
    // }

    // /// Returns the next key to be iterated or an empty string.
    // pub fn peek_next(&self) -> Option<Vec<u8>> {
    //     if self.entries.len() > 1 {
    //         return Some(self.key.clone());
    //     } else if self.n - self.i > 1 {
    //         let (key, _) = self.r.key_at(self.i + 1);
    //         return Some(key);
    //     }

    //     None
    // }

    /// Returns any errors encounter during iteration.
    pub fn err(&self) -> Option<String> {
        self.err.clone()
    }
}

impl Iterator for BlockIter {
    // (key, min_time, max_time, /*typ*/, checksum, buf, err)
    type Item = (i64, i64, /*u8,*/ u32, Vec<u8>, Option<String>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.indexes.is_empty() || self.pos == self.indexes.len() {
            return None;
        }

        let idx = &self.indexes[self.pos];
        match self.r.read_bytes(idx.offset as u64, idx.size as u64, &[]) {
            Ok((checksum, block)) => {
                self.pos += 1;
                return Some((idx.min, idx.max, checksum, block, None));
            }
            Err(e) => {
                self.err = Some(e.to_string());
                return None;
            }
        }
    }
}

/// Accessing blocks from a TSM file.
struct Accessor {
    access_count: AtomicU64, // Counter incremented everytime the mmapAccessor is accessed

    inner: RwLock<InnerAccessor>,
}

struct InnerAccessor {
    free_count: AtomicU64, // Counter to determine whether the accessor can free its resources

    path: PathBuf,
    // mmap: Option<Mmap>,
    // b: Vec<u8>,
    f: BufReader<File>,
    size: u64,
    index: Arc<IndirectIndex>,
}

impl Accessor {
    pub fn new(path: &Path, f: File) -> Self {
        let inner = InnerAccessor {
            free_count: AtomicU64::new(0),
            path: path.to_path_buf(),
            f: BufReader::with_capacity(64 * 1024, f),
            size: 0,
            index: Arc::new(IndirectIndex::default()),
        };

        Self {
            access_count: AtomicU64::new(0),
            inner: RwLock::new(inner),
        }
    }

    fn init(
        &mut self,
        keys: Vec<(Vec<u8>, u8)>,
        vlog_indexes: Vec<Vec<VLogIndex>>,
    ) -> Result<Arc<IndirectIndex>> {
        let mut inner = self.inner.write();

        vertify_version(&mut inner.f)?;

        let meta = std::fs::metadata(&inner.path)?;
        inner.size = meta.len();

        inner.f.seek(SeekFrom::Start(0))?;

        let (mut min, mut max) = (i64::MAX, i64::MIN);
        for index in vlog_indexes.iter() {
            for idx in index.iter() {
                if idx.min < min {
                    min = idx.min;
                }
                if idx.max > max {
                    max = idx.max;
                }
            }
        }

        let (min_key, max_key) = (keys[0].0.clone(), keys.last().unwrap().0.clone());
        let index = Arc::new(IndirectIndex {
            inner: RwLock::new(InnerIndirectIndex {
                keys,
                vlog_indexes,
                min_key,
                max_key,
                min_time: min,
                max_time: max,
                tombstones: HashMap::new(),
            }),
        });

        inner.index = Arc::clone(&index);

        // Allow resources to be freed immediately if requested
        self.inc_access();
        inner
            .free_count
            .store(1, std::sync::atomic::Ordering::SeqCst);

        Ok(index)
    }

    fn init_with_publish_metadata(
        &mut self,
        keys: Vec<(Vec<u8>, u8)>,
        vlog_indexes: Vec<Vec<VLogIndex>>,
        min_key: Vec<u8>,
        max_key: Vec<u8>,
        min_time: i64,
        max_time: i64,
    ) -> Result<Arc<IndirectIndex>> {
        let mut inner = self.inner.write();

        vertify_version(&mut inner.f)?;

        let meta = std::fs::metadata(&inner.path)?;
        inner.size = meta.len();

        inner.f.seek(SeekFrom::Start(0))?;

        let index = Arc::new(IndirectIndex {
            inner: RwLock::new(InnerIndirectIndex {
                keys,
                vlog_indexes,
                min_key,
                max_key,
                min_time,
                max_time,
                tombstones: HashMap::new(),
            }),
        });

        inner.index = Arc::clone(&index);

        self.inc_access();
        inner
            .free_count
            .store(1, std::sync::atomic::Ordering::SeqCst);

        Ok(index)
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

        Ok(())
    }

    fn inc_access(&self) {
        self.access_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn rename(&self, path: &Path) -> Result<()> {
        self.inc_access();

        let mut inner = self.inner.write();

        std::fs::rename(&inner.path, path)?; // FIXME: 应该先关闭再rename

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;
        file.seek(SeekFrom::Start(0))?;

        inner.f = BufReader::new(file);

        Ok(())
    }

    fn rename_and_update_file_name(&self, path: &Path) -> Result<()> {
        self.inc_access();

        let mut inner = self.inner.write();
        std::fs::rename(&inner.path, path)?; // FIXME: rename an opened file ?

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        file.seek(SeekFrom::Start(0))?;

        inner.f = BufReader::new(file);

        inner.path = path.to_path_buf();

        Ok(())
    }

    fn read(&self, key: &[u8], timestamp: i64) -> Result<Vec<Value>> {
        let inner = self.inner.read();
        if let Ok(Some(entry)) = inner.index.entry(key, timestamp) {
            return self.read_block(entry.offset as u64, entry.size as u64);
        }
        Ok(vec![])
    }

    fn read_block(&self, offset: u64, size: u64) -> Result<Vec<Value>> {
        self.inc_access();

        let mut inner = self.inner.write();
        ensure!(inner.size >= offset + size, TSMClosedSnafu);

        //TODO: Validate checksum
        let _ = inner.f.seek(SeekFrom::Start(offset))?;
        let mut data = vec![0u8; size as usize];
        inner.f.read_exact(&mut data)?;

        encoding::decode_block(&data[4..]).context(EncodingSnafu) // skip CRC32
    }

    fn read_bytes(&self, offset: u64, size: u64, b: &[u8]) -> Result<(u32, Vec<u8>)> {
        self.inc_access();

        let lock_started = slow_trace::now();
        let mut inner = self.inner.write();
        slow_trace::log_lock_wait("vlog_accessor.write", lock_started, "read VLog bytes");
        ensure!(inner.size >= offset + size, TSMClosedSnafu);

        // return the bytes after the 4 byte checksum
        let _ = inner.f.seek(SeekFrom::Start(offset))?;
        let mut checksum = vec![0u8; 4];
        inner.f.read_exact(&mut checksum)?;
        let mut data = vec![0u8; size as usize - 4]; // skip CRC32
        inner.f.read_exact(&mut data)?;
        Ok((checksum.as_slice().get_u32(), data))
    }

    fn read_multi_bytes(&self, indexes: &[VLogIndex], b: &[u8]) -> Result<Vec<(u32, Vec<u8>)>> {
        self.inc_access();

        let mut inner = self.inner.write();
        let mut bytes = vec![];

        for idx in indexes {
            match idx.typ {
                INTEGER | UNSIGNED | FLOAT | BOOL | STRING => {
                    ensure!(inner.size as i64 >= idx.offset + idx.size, TSMClosedSnafu);

                    let _ = inner.f.seek(SeekFrom::Start(idx.offset as u64))?;

                    let mut checksum = vec![0u8; 4];
                    inner.f.read_exact(&mut checksum)?;
                    let mut data = vec![0u8; idx.size as usize - 4];
                    inner.f.read_exact(&mut data)?;

                    bytes.push((checksum.as_slice().get_u32(), data));
                }
                _ => {
                    return Err(Error::StringErr {
                        reason: format!("invalid type: {}", idx.typ),
                    });
                }
            }
        }

        Ok(bytes)
    }

    pub fn read_multi_block(&self, typ: u8, indexes: &[VLogIndex], b: &[u8]) -> Result<Vec<Block>> {
        self.inc_access();

        let mut inner = self.inner.write();
        let mut blocks = vec![];
        for idx in indexes {
            match typ {
                INTEGER | UNSIGNED | FLOAT | BOOL | STRING => {
                    ensure!(inner.size as i64 >= idx.offset + idx.size, TSMClosedSnafu);

                    let _ = inner.f.seek(SeekFrom::Start(idx.offset as u64 + 4))?; // skip checksum
                    let mut b = vec![0u8; idx.size as usize - 4]; // skip checksum
                    inner.f.read_exact(&mut b)?;

                    let block: Block = Block {
                        aggregate: None,
                        aggregate_copied: false,
                        count: idx.count as usize,
                        key: vec![],
                        min_time: idx.min,
                        max_time: idx.max,
                        typ,
                        b,
                        tombstones: vec![],
                        read_min: i64::MAX,
                        read_max: i64::MIN,
                    };
                    blocks.push(block);
                }
                _ => {
                    return Err(Error::StringErr {
                        reason: format!("invalid type: {}", idx.typ),
                    });
                }
            }
        }

        Ok(blocks)
    }

    /// Returns all values for a key in all blocks.
    fn read_all(&self, indexes: &[VLogIndex]) -> Result<Vec<Value>> {
        self.inc_access();

        let mut inner = self.inner.write();
        let mut all = vec![];
        for idx in indexes {
            let mut values = self.read_block(idx.offset as u64, idx.size as u64)?;
            all.append(&mut values);
        }

        Ok(all)
    }

    fn read_integer_block(&self, offset: u64, size: u64) -> Result<Vec<IntegerValue>> {
        self.inc_access();

        let mut inner = self.inner.write();
        ensure!(inner.size >= offset + size, TSMClosedSnafu);

        let _ = inner.f.seek(SeekFrom::Start(offset))?;
        let mut data = vec![0u8; size as usize];
        inner.f.read_exact(&mut data)?;

        decode_integer_block(&data[4..]).context(EncodingSnafu) // skip CRC32
    }

    fn read_integer_multi_block(&self, indexes: &[VLogIndex]) -> Result<Vec<IntegerValue>> {
        self.inc_access();

        let mut inner = self.inner.write();

        let mut values = vec![];
        let mut data = vec![];
        for idx in indexes {
            ensure!(inner.size as i64 >= idx.offset + idx.size, TSMClosedSnafu);

            data.resize(idx.size as usize, 0u8);
            let _ = inner.f.seek(SeekFrom::Start(idx.offset as u64))?;
            inner.f.read_exact(&mut data)?;

            values.append(&mut decode_integer_block(&data[4..]).context(EncodingSnafu)?);
            // skip CRC32
        }

        Ok(values)
    }

    fn read_integer_multi_block_and_exclude_tombstones(
        &self,
        r: &Arc<VLogReader>,
        key: &[u8],
        indexes: &[VLogIndex],
    ) -> Result<Vec<IntegerValue>> {
        self.inc_access();

        let lock_start = slow_trace::now();
        let mut inner = self.inner.write();
        slow_trace::log_lock_wait(
            "vlog_accessor.write",
            lock_start,
            "stage=query_wait_for_vlog_lock",
        );

        let tombstones = r.tombstone_range(key);
        let mut values = vec![];
        let mut data = vec![];
        let decode_start = slow_trace::now();
        for idx in indexes {
            ensure!(inner.size as i64 >= idx.offset + idx.size, TSMClosedSnafu);

            data.resize(idx.size as usize, 0u8);
            let _ = inner.f.seek(SeekFrom::Start(idx.offset as u64))?;
            inner.f.read_exact(&mut data)?;

            let mut vals = IntegerValues(decode_integer_block(&data[4..]).context(EncodingSnafu)?);
            exclude_tombstones_integer_values(tombstones.as_slice(), &mut vals);
            if vals.is_empty() {
                continue;
            }
            values.append(&mut vals); // skip CRC32
        }
        slow_trace::log_query_stage("query_block_decode", decode_start, "source=vlog");

        Ok(values)
    }

    fn read_integer_array_block(
        &self,
        offset: u64,
        size: u64,
        value: &mut IntegerArray,
    ) -> Result<()> {
        self.inc_access();

        let inner = self.inner.read();
        let mut inner = self.inner.write();
        ensure!(inner.size >= offset + size, TSMClosedSnafu);

        let _ = inner.f.seek(SeekFrom::Start(offset))?;
        let mut data = vec![0u8; size as usize];
        inner.f.read_exact(&mut data)?;

        decode_integer_array_block(&data[4..], value).context(ArrayEncodingSnafu)
    }

    fn read_unsigned_block(&self, offset: u64, size: u64) -> Result<Vec<UnsignedValue>> {
        self.inc_access();

        let mut inner = self.inner.write();
        ensure!(inner.size >= offset + size, TSMClosedSnafu);

        let _ = inner.f.seek(SeekFrom::Start(offset))?;
        let mut data = vec![0u8; size as usize];
        inner.f.read_exact(&mut data)?;

        decode_unsigned_block(&data[4..]).context(EncodingSnafu) // skip CRC32
    }

    fn read_unsigned_multi_block(&self, indexes: &[VLogIndex]) -> Result<Vec<UnsignedValue>> {
        self.inc_access();

        let mut inner = self.inner.write();

        let mut values = vec![];
        let mut data = vec![];
        for idx in indexes {
            ensure!(inner.size as i64 >= idx.offset + idx.size, TSMClosedSnafu);

            data.resize(idx.size as usize, 0u8);
            let _ = inner.f.seek(SeekFrom::Start(idx.offset as u64))?;
            inner.f.read_exact(&mut data)?;

            values.append(&mut decode_unsigned_block(&data[4..]).context(EncodingSnafu)?);
            // skip CRC32
        }

        Ok(values)
    }

    fn read_unsigned_multi_block_and_exclude_tombstones(
        &self,
        r: &Arc<VLogReader>,
        key: &[u8],
        indexes: &[VLogIndex],
    ) -> Result<Vec<UnsignedValue>> {
        self.inc_access();

        let lock_start = slow_trace::now();
        let mut inner = self.inner.write();
        slow_trace::log_lock_wait(
            "vlog_accessor.write",
            lock_start,
            "stage=query_wait_for_vlog_lock",
        );

        let tombstones = r.tombstone_range(key);
        let mut values = vec![];
        let mut data = vec![];
        let decode_start = slow_trace::now();
        for idx in indexes {
            ensure!(inner.size as i64 >= idx.offset + idx.size, TSMClosedSnafu);

            data.resize(idx.size as usize, 0u8);
            let _ = inner.f.seek(SeekFrom::Start(idx.offset as u64))?;
            inner.f.read_exact(&mut data)?;

            let mut vals =
                UnsignedValues(decode_unsigned_block(&data[4..]).context(EncodingSnafu)?);
            exclude_tombstones_unsigned_values(tombstones.as_slice(), &mut vals);
            if vals.is_empty() {
                continue;
            }
            values.append(&mut vals); // skip CRC32
        }
        slow_trace::log_query_stage("query_block_decode", decode_start, "source=vlog");

        Ok(values)
    }

    fn read_unsigned_array_block(
        &self,
        offset: u64,
        size: u64,
        value: &mut UnsignedArray,
    ) -> Result<()> {
        self.inc_access();

        let inner = self.inner.read();
        let mut inner = self.inner.write();
        ensure!(inner.size >= offset + size, TSMClosedSnafu);

        let _ = inner.f.seek(SeekFrom::Start(offset))?;
        let mut data = vec![0u8; size as usize];
        inner.f.read_exact(&mut data)?;

        decode_unsigned_array_block(&data[4..], value).context(ArrayEncodingSnafu)
    }

    fn read_float_block(&self, offset: u64, size: u64) -> Result<Vec<FloatValue>> {
        self.inc_access();

        let mut inner = self.inner.write();
        ensure!(inner.size >= offset + size, TSMClosedSnafu);

        let _ = inner.f.seek(SeekFrom::Start(offset))?;
        let mut data = vec![0u8; size as usize];

        inner.f.read_exact(&mut data)?;

        decode_float_block(&data[4..]).context(EncodingSnafu) // skip CRC32
    }

    fn read_float_multi_block(&self, indexes: &[VLogIndex]) -> Result<Vec<FloatValue>> {
        self.inc_access();

        let mut inner = self.inner.write();

        let mut values = vec![];
        let mut data = vec![];
        for idx in indexes {
            ensure!(inner.size as i64 >= idx.offset + idx.size, TSMClosedSnafu);

            data.resize(idx.size as usize, 0u8);
            let _ = inner.f.seek(SeekFrom::Start(idx.offset as u64))?;
            inner.f.read_exact(&mut data)?;
            // skip CRC32
            values.append(&mut decode_float_block(&data[4..]).context(EncodingSnafu)?);
        }

        Ok(values)
    }

    fn read_float_multi_block_and_exclude_tombstones(
        &self,
        r: &Arc<VLogReader>,
        key: &[u8],
        indexes: &[VLogIndex],
    ) -> Result<Vec<FloatValue>> {
        self.inc_access();

        let lock_start = slow_trace::now();
        let mut inner = self.inner.write();
        slow_trace::log_lock_wait(
            "vlog_accessor.write",
            lock_start,
            "stage=query_wait_for_vlog_lock",
        );

        let tombstones = r.tombstone_range(key);
        let mut values = vec![];
        let mut data = vec![];
        let decode_start = slow_trace::now();
        for idx in indexes {
            ensure!(inner.size as i64 >= idx.offset + idx.size, TSMClosedSnafu);

            data.resize(idx.size as usize, 0u8);
            let _ = inner.f.seek(SeekFrom::Start(idx.offset as u64))?;
            inner.f.read_exact(&mut data)?;
            let mut vals = FloatValues(decode_float_block(&data[4..]).context(EncodingSnafu)?);
            exclude_tombstones_float_values(tombstones.as_slice(), &mut vals);
            if vals.is_empty() {
                continue;
            }
            values.append(&mut vals);
        }
        slow_trace::log_query_stage("query_block_decode", decode_start, "source=vlog");

        Ok(values)
    }

    fn read_float_array_block(&self, offset: u64, size: u64, value: &mut FloatArray) -> Result<()> {
        self.inc_access();

        let mut inner = self.inner.write();
        ensure!(inner.size >= offset + size, TSMClosedSnafu);

        let _ = inner.f.seek(SeekFrom::Start(offset))?;
        let mut data = vec![0u8; size as usize];
        inner.f.read_exact(&mut data)?;

        decode_float_array_block(&data[4..], value).context(ArrayEncodingSnafu)
    }

    fn read_bool_block(&self, offset: u64, size: u64) -> Result<Vec<BoolValue>> {
        self.inc_access();

        let mut inner = self.inner.write();
        ensure!(inner.size >= offset + size, TSMClosedSnafu);

        let _ = inner.f.seek(SeekFrom::Start(offset))?;
        let mut data = vec![0u8; size as usize];
        inner.f.read_exact(&mut data)?;

        decode_bool_block(&data[4..]).context(EncodingSnafu) // skip CRC32
    }

    fn read_bool_multi_block(&self, indexes: &[VLogIndex]) -> Result<Vec<BoolValue>> {
        self.inc_access();

        let mut inner = self.inner.write();

        let mut values = vec![];
        let mut data = vec![];
        for idx in indexes {
            ensure!(inner.size as i64 >= idx.offset + idx.size, TSMClosedSnafu);

            data.resize(idx.size as usize, 0u8);
            let _ = inner.f.seek(SeekFrom::Start(idx.offset as u64))?;
            inner.f.read_exact(&mut data)?;
            // skip CRC32
            values.append(&mut decode_bool_block(&data[4..]).context(EncodingSnafu)?);
        }

        Ok(values)
    }

    fn read_bool_multi_block_and_exclude_tombstones(
        &self,
        r: &Arc<VLogReader>,
        key: &[u8],
        indexes: &[VLogIndex],
    ) -> Result<Vec<BoolValue>> {
        self.inc_access();

        let lock_start = slow_trace::now();
        let mut inner = self.inner.write();
        slow_trace::log_lock_wait(
            "vlog_accessor.write",
            lock_start,
            "stage=query_wait_for_vlog_lock",
        );

        let tombstones = r.tombstone_range(key);
        let mut values = vec![];
        let mut data = vec![];
        let decode_start = slow_trace::now();
        for idx in indexes {
            ensure!(inner.size as i64 >= idx.offset + idx.size, TSMClosedSnafu);

            data.resize(idx.size as usize, 0u8);
            let _ = inner.f.seek(SeekFrom::Start(idx.offset as u64))?;
            inner.f.read_exact(&mut data)?;

            // skip CRC32
            let mut vals = BoolValues(decode_bool_block(&data[4..]).context(EncodingSnafu)?);
            exclude_tombstones_bool_values(tombstones.as_slice(), &mut vals);
            if vals.is_empty() {
                continue;
            }
            values.append(&mut vals);
        }
        slow_trace::log_query_stage("query_block_decode", decode_start, "source=vlog");

        Ok(values)
    }

    fn read_bool_array_block(&self, offset: u64, size: u64, value: &mut BoolArray) -> Result<()> {
        self.inc_access();

        let inner = self.inner.read();
        let mut inner = self.inner.write();
        ensure!(inner.size >= offset + size, TSMClosedSnafu);

        let _ = inner.f.seek(SeekFrom::Start(offset))?;
        let mut data = vec![0u8; size as usize];
        inner.f.read_exact(&mut data)?;

        // skip CRC32
        decode_bool_array_block(&data[4..], value).context(ArrayEncodingSnafu)
    }

    fn read_string_block(&self, offset: u64, size: u64) -> Result<Vec<StringValue>> {
        self.inc_access();

        let mut inner = self.inner.write();
        ensure!(inner.size >= offset + size, TSMClosedSnafu);

        let _ = inner.f.seek(SeekFrom::Start(offset))?;
        let mut data = vec![0u8; size as usize];
        inner.f.read_exact(&mut data)?;

        decode_string_block(&data[4..]).context(EncodingSnafu) // skip CRC32
    }

    fn read_string_multi_block(&self, indexes: &[VLogIndex]) -> Result<Vec<StringValue>> {
        self.inc_access();

        let mut inner = self.inner.write();

        let mut values = vec![];
        let mut data = vec![];
        for idx in indexes {
            ensure!(inner.size as i64 >= idx.offset + idx.size, TSMClosedSnafu);

            data.resize(idx.size as usize, 0u8);
            let _ = inner.f.seek(SeekFrom::Start(idx.offset as u64))?;
            inner.f.read_exact(&mut data)?;

            // skip CRC32
            values.append(&mut decode_string_block(&data[4..]).context(EncodingSnafu)?);
        }

        Ok(values)
    }

    fn read_string_multi_block_and_exclude_tombstones(
        &self,
        r: &Arc<VLogReader>,
        key: &[u8],
        indexes: &[VLogIndex],
    ) -> Result<Vec<StringValue>> {
        self.inc_access();

        let lock_start = slow_trace::now();
        let mut inner = self.inner.write();
        slow_trace::log_lock_wait(
            "vlog_accessor.write",
            lock_start,
            "stage=query_wait_for_vlog_lock",
        );

        let tombstones = r.tombstone_range(key);
        let mut values = vec![];
        let mut data = vec![];
        let decode_start = slow_trace::now();
        for idx in indexes {
            ensure!(inner.size as i64 >= idx.offset + idx.size, TSMClosedSnafu);

            data.resize(idx.size as usize, 0u8);
            let _ = inner.f.seek(SeekFrom::Start(idx.offset as u64))?;
            inner.f.read_exact(&mut data)?;

            // skip CRC32
            let mut vals = StringValues(decode_string_block(&data[4..]).context(EncodingSnafu)?);
            exclude_tombstones_string_values(tombstones.as_slice(), &mut vals);
            if vals.is_empty() {
                continue;
            }
            values.append(&mut vals);
        }
        slow_trace::log_query_stage("query_block_decode", decode_start, "source=vlog");

        Ok(values)
    }

    fn read_string_array_block(
        &self,
        offset: u64,
        size: u64,
        value: &mut StringArray,
    ) -> Result<()> {
        self.inc_access();

        let inner = self.inner.read();
        let mut inner = self.inner.write();
        ensure!(inner.size >= offset + size, TSMClosedSnafu);

        let _ = inner.f.seek(SeekFrom::Start(offset))?;
        let mut data = vec![0u8; size as usize];
        inner.f.read_exact(&mut data)?;

        // skip CRC32
        decode_string_array_block(&data[4..], value).context(ArrayEncodingSnafu)
    }

    fn path(&self) -> PathBuf {
        let inner = self.inner.read();
        inner.path.clone()
    }

    // fn file_name(&self) -> String {
    //     let inner = self.inner.read();
    //     inner.path.file_name().unwrap().to_os_string().into_string().unwrap()
    // }

    fn close(&self) -> Result<()> {
        // let mut inner = self.inner.write();
        // inner.mmap = None;
        Ok(())
    }
}

/// IndirectIndex is a TSMIndex that uses a raw byte slice representation of an index.  This
/// implementation can be used for indexes that may be MMAPed into memory.
#[derive(Default)]
pub struct IndirectIndex {
    // When we have identified the correct position in the index for a given
    // key, we could perform another binary search or a linear scan.  This
    // should be fast as well since each index entry is 28 bytes and all
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

        let mut n = 0;
        for (key, typ) in inner.keys.iter() {
            n += (key.len() + 1);
        }
        for idx in inner.vlog_indexes.iter() {
            n += idx.len() * VLogIndex::SIZE;
        }

        n as u32
    }

    // fn offset(&self, i: usize) -> usize {
    //     let inner = self.inner.read();
    //     if i < 0 || i + 4 > inner.offsets.len() {
    //         // return -1;
    //         panic!("TSMReader: invalid offset {}", i);
    //     }

    //     (&inner.offsets[i * 4..i * 4 + 4]).get_u32() as usize
    // }

    // pub fn seek(&self, key: &[u8]) -> usize {
    //     let inner = self.inner.read();
    //     inner.search_offset(key)
    // }

    /// Returns all index entries for a key.
    pub fn entries(&self, key: &[u8]) -> Vec<VLogIndex> {
        self.read_entries(key, None)
    }

    /// Returns all index entries for a key.
    pub fn read_entries(&self, key: &[u8], entries: Option<&mut Vec<VLogIndex>>) -> Vec<VLogIndex> {
        let inner = self.inner.read();
        for (pos, (k, _typ)) in inner.keys.iter().enumerate() {
            if k.eq(key) {
                return inner.vlog_indexes[pos].clone();
            }
        }

        // The key is not in the index.
        vec![]
    }

    /// Returns the index entry for the specified key and timestamp.
    /// If no entry matches the key an timestamp, nil is returned.
    pub fn entry(&self, key: &[u8], timestamp: i64) -> Result<Option<VLogIndex>> {
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
    pub fn key(&self, idx: usize, entries: &mut [VLogIndex]) -> (Vec<u8>, u8, Vec<VLogIndex>) {
        let inner = self.inner.read();
        inner.key(idx, entries)
    }

    /// Returns the key in the index at the given position.
    pub fn key_at(&self, idx: usize) -> (Vec<u8>, u8) {
        let inner = self.inner.read();
        inner.key_at(idx)
    }

    pub fn keys(&self) -> Vec<(Vec<u8>, u8)> {
        let inner = self.inner.read();
        inner.keys()
    }

    /// Returns the count of unique keys in the index
    pub fn key_count(&self) -> usize {
        let inner = self.inner.read();
        inner.key_count()
    }

    /// Removes the given keys from the index.
    pub fn delete(&self, keys: &mut [&[u8]]) {
        let keys = keys;
        if keys.is_empty() {
            return;
        }

        if !keys.is_sorted() {
            keys.sort();
        }

        let mut inner = self.inner.write();
        for key in keys {
            if let Some(pos) = inner.keys.iter().position(|(k, _typ)| k.eq(key)) {
                let _ = inner.keys.remove(pos);
            }
        }
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
            let tombstone_keys = keys.iter().map(|key| to_string!(key)).collect::<Vec<_>>();
            self.delete(keys);
            let mut inner = self.inner.write();
            for key in tombstone_keys {
                inner.tombstones.insert(
                    key,
                    vec![TimeRange {
                        min: min_time,
                        max: max_time,
                    }],
                );
            }
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
                let (k, entries) = inner.read_entries_at(i, None);
                (k.to_vec(), entries.to_vec())
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
            let (min, max) = (ie[0].min, ie[ie.len() - 1].max);
            if min_time > max || max_time < min {
                i += 1;
                continue;
            }

            // Does the range passed in cover every value for the key?
            if min_time <= min && max_time >= max {
                tombstones.insert(
                    to_string!(&k),
                    vec![TimeRange {
                        min: min_time,
                        max: max_time,
                    }],
                );
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
    pub fn values_type(&self, key: &[u8]) -> Result<u8> {
        let inner = self.inner.read();
        for (k, typ) in inner.keys.iter() {
            if k.as_slice() == key {
                return Ok(*typ);
            }
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
    // // b is the underlying index byte slice.  This could be a copy on the heap or an MMAP
    // // slice reference
    // b: Vec<u8>,
    keys: Vec<(Vec<u8>, u8)>,          // sorted
    vlog_indexes: Vec<Vec<VLogIndex>>, // keys[i] -> vlog_indexes[i]

    // offsets contains the positions in b for each key.  It points to the 2 byte length of
    // key.
    // offsets: Vec<usize>,

    // minKey, maxKey are the minium and maximum (lexicographically sorted) contained in the
    // file
    min_key: Vec<u8>,
    max_key: Vec<u8>,

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
    //     // if i < 0 || i+4 > self.offsets.len() {
    //     //     return -1;
    //     // }

    //     // (&self.offsets[i*4..i*4+4]).get_u32()
    //     self.offsets[i]
    // }

    // /// searches the offsets slice for key and returns the position in
    // /// offsets where key would exist.
    // fn search_offset(&self, key: &[u8]) -> usize {
    //     // // We use a binary search across our indirect offsets (pointers to all the keys
    //     // // in the index slice).
    //     // let i = search_bytes_fixed(&self.offsets, 4, &self.b, key);

    //     // // See if we might have found the right index
    //     // if i < self.offsets.len() {
    //     //     return (i / 4) as usize;
    //     // }

    //     // // The key is not in the index.  i is the index where it would be inserted so return
    //     // // a value outside our offset range.
    //     // self.offsets.len() / 4

    //     self.vlog_indexes.binary_search_by(|idx| {
    //         idx[0].key.cmp(key)
    //     })
    // }

    /// Returns the byte position of key in the index.  If key is not in the index, len(index) is returned.
    fn search(&self, key: &[u8]) -> usize {
        if !self.contains_key(key) {
            return self.keys.len();
        }

        for (pos, (k, _typ)) in self.keys.iter().enumerate() {
            if k == key {
                return pos;
            }
        }

        // The key is not in the index.  i is the index where it would be inserted so return
        // a value outside our offset range.
        self.keys.len()
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
        pos: usize,
        entries: Option<&mut Vec<VLogIndex>>,
    ) -> (&[u8], &[VLogIndex]) {
        (&self.keys[pos].0, &self.vlog_indexes[pos])
    }

    /// Returns the key in the index at the given position.
    fn key(&self, idx: usize, entries: &mut [VLogIndex]) -> (Vec<u8>, u8, Vec<VLogIndex>) {
        if idx < 0 || idx >= self.vlog_indexes.len() {
            return (vec![], 0, vec![]);
        }

        let (key, typ) = self.keys[idx].clone();
        let indexes = self.vlog_indexes[idx].clone();

        (key, typ, indexes)
    }

    /// Returns the key in the index at the given position.
    fn key_at(&self, idx: usize) -> (Vec<u8>, u8) {
        if idx < 0 || idx >= self.keys.len() {
            return (vec![], 0);
        }

        self.keys[idx].clone()
    }

    pub fn keys(&self) -> Vec<(Vec<u8>, u8)> {
        self.keys.clone()
    }

    /// Returns the count of unique keys in the index.
    pub fn key_count(&self) -> usize {
        self.keys.len()
    }

    // /// Removes the given keys with data between minTime and maxTime from the index.
    // pub fn delete_range(&mut self, keys: &[&[u8]], min_time: i64, max_time: i64) {
    //     todo!("implement me");
    // }
}

fn exclude_tombstones_integer_values(tr: &[TimeRange], values: &mut IntegerValues) {
    for t in tr.iter() {
        values.exclude(t.min, t.max);
    }
}

fn exclude_tombstones_unsigned_values(tr: &[TimeRange], values: &mut UnsignedValues) {
    for t in tr.iter() {
        values.exclude(t.min, t.max);
    }
}

fn exclude_tombstones_float_values(tr: &[TimeRange], values: &mut FloatValues) {
    for t in tr.iter() {
        values.exclude(t.min, t.max);
    }
}

fn exclude_tombstones_bool_values(tr: &[TimeRange], values: &mut BoolValues) {
    for t in tr.iter() {
        values.exclude(t.min, t.max);
    }
}

fn exclude_tombstones_string_values(tr: &[TimeRange], values: &mut StringValues) {
    for t in tr.iter() {
        values.exclude(t.min, t.max);
    }
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

// fn read_entries(b: &[u8], entries: &mut IndexEntries) -> Result<usize> {
//     ensure!(b.len() >= 1 + INDEX_COUNT_SIZE, StringErrSnafu {
//         reason: "read_entries: data too short for headers".to_string(),
//     });

//     let mut n = 0;
//     // 1 byte block type
//     entries.typ = b[n];
//     n += 1;

//     // 2 byte count of index entries
//     let count = (&b[n..n + INDEX_COUNT_SIZE]).get_u16() as i32;
//     n += INDEX_COUNT_SIZE;

//     if entries.entries.capacity() < count as usize {
//         entries.entries = vec![IndexEntry::default(); count as usize];
//     } else {
//         entries.entries.truncate(count as usize);
//     }

//     let mut b = &b[INDEX_COUNT_SIZE + INDEX_TYPE_SIZE..];
//     for i in 0..count {
//         if let Err(e) = entries.entries[i as usize].unmarshal_binary(b) {
//             return Err(Error::StringErr {
//                 reason: format!("read_entries: unmarshal error: {:?}", e),
//             });
//         }
//         b = &b[INDEX_ENTRY_SIZE..];
//     }

//     n += count as usize * INDEX_ENTRY_SIZE;

//     Ok(n)
// }

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

fn vertify_version(f: &mut BufReader<File>) -> Result<()> {
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
            reason: format!(
                "vertify_version failed, incorrent maginc number: {}",
                buf[0]
            ),
        }
    );

    Ok(())
}
