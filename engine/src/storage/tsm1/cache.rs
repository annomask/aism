//! TSM Cache.
use std::{
    collections::HashMap,
    ops::Sub,
    os::unix::fs::PermissionsExt,
    sync::{
        Arc,
        atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

use crate::common::{slow_trace, time::Time};
use crate::models::{
    FieldType,
    points::{FloatValue, Value},
};
use crate::runtime::execution::{
    ExecutorRef, RuntimeExecutorExt, SpawnError, legacy_global_executor,
};
use parking_lot::RwLock;
use snafu::{ResultExt, Snafu, ensure};

use super::{ring::Ring, wal::WalSegmentReader};
use crate::storage::tsm1::{
    data_encoding::encoding_values::{self, Values, ValuesRef},
    wal,
};
/// RING_SHARDS specifies the number of partitions that the hash ring used to
/// store the entry mappings contains. It must be a power of 2. From empirical
/// testing, a value above the number of cores on the machine does not provide
// any additional benefit. For now we'll set it to the number of cores on the
/// largest box we could imagine running influx.
pub const RING_SHARDS: i32 = 16;

/// Error type for Cache.
#[derive(Debug, Snafu)]
pub enum Error {
    /// IO error.
    #[snafu(display("open wal file failed: {}", reason))]
    IOError { reason: std::io::Error },

    /// Load entries from wal failed.
    #[snafu(display("{}", source))]
    LoadWALErr { source: wal::Error },

    /// ErrSnapshotInProgress is returned if a snapshot is attempted while one is already running.
    #[snafu(display("snapshot in progress"))]
    ErrSnapshotInProgress,

    /// ErrCacheMemorySizeLimitExceeded returns an error indicating an operation
    /// could not be completed due to exceeding the cache-max-memory-size setting.
    #[snafu(display("cache-max-memory-size exceeded: ({}/{})", n, limit))]
    ErrCacheMemorySizeLimitExceeded { n: u64, limit: u64 },

    // ErrFieldTypeConflict is returned when a new field already exists with a different type.
    #[snafu(display("field type conflict"))]
    ErrFieldTypeConflict,

    // ErrUnknownFieldType is returned when the type of a field cannot be determined.
    #[snafu(display("unknown field type"))]
    ErrUnknownFieldType,

    /// Encode values falied.
    #[snafu(display("{}", source))]
    EncodingValues { source: encoding_values::Error },

    /// String error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },

    /// The host executor refused required cache work.
    #[snafu(display("cache execution error: {}", source))]
    Execution { source: SpawnError },

    #[snafu(display("accepted cache task did not complete"))]
    TaskTerminated,
}

/// Result type for Cache.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IOError { reason: err }
    }
}

// TODO: 性能优化
// drop(RwLock<Values>)占用时间长
// A set of values and some metadata.
pub struct Entry {
    sequence: AtomicU64,
    pub values: RwLock<Values>, // All stored values.
    // The type of values stored. Read only so doesn't need to be protected by lock.
    pub vtype: RwLock<u8>,
}

impl Entry {
    /// Returns a new instance of entry with the given values.  If the values are not valid, an error is returned.
    pub fn new(values: Vec<Value>) -> Result<Self> {
        if values.is_empty() {
            let e = Self {
                sequence: AtomicU64::new(0),
                values: RwLock::new(Values(vec![])),
                vtype: RwLock::new(0),
            };
            return Ok(e);
        }

        let et = value_type(&values[0]);
        for v in values.iter() {
            // Make sure all the values are the same type
            ensure!(et == value_type(v), ErrFieldTypeConflictSnafu);
        }

        Ok(Self {
            sequence: AtomicU64::new(0),
            values: RwLock::new(Values(values)),
            vtype: RwLock::new(et),
        })
    }

    /// Adds the given values to the entry.
    pub fn add(&self, mut values: Vec<Value>) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        // Are any of the new values the wrong type?
        let vt = *self.vtype.read();
        if vt != 0 {
            for v in &values {
                ensure!(vt == value_type(v), ErrFieldTypeConflictSnafu);
            }
        }

        // entry currently has no values, so add the new ones and we're done.
        let mut vs = self.values.write();
        if vs.is_empty() {
            *self.vtype.write() = value_type(&values[0]);
            *vs = Values(values);
            return Ok(());
        }

        // Append the new values to the existing ones...
        vs.append(&mut values);
        Ok(())
    }

    /// deduplicate sorts and orders the entry's values. If values are already deduped and sorted,
    /// the function does no work and simply returns.
    pub fn deduplicate(&self) {
        let wait = slow_trace::now();
        let (acquired, finished) = {
            let mut values = self.values.write();
            let acquired = slow_trace::now();
            if values.len() > 1 {
                values.deduplicate();
            }
            (acquired, slow_trace::now())
        };
        slow_trace::log_lock_wait_until("cache_entry_deduplicate", wait, acquired, "cache values");
        slow_trace::log_query_stage_until("query_cache_entry_deduplicate", acquired, finished, "cache values");
    }

    /// Returns the number of values in this entry.
    pub fn count(&self) -> i64 {
        let values = self.values.read();
        values.len() as i64
    }

    /// Removes all values with timestamps between min and max inclusive.
    pub fn filter(&self, min: i64, max: i64) {
        let mut values = self.values.write();
        if values.len() > 1 {
            values.deduplicate();
        }
        values.exclude(min, max);
    }

    /// Returns the size of this entry in bytes.
    pub fn size(&self) -> i64 {
        let values = self.values.read();
        values.size()
    }

    /// Returns for the entry the data type of its values.
    pub fn reefs_ql_type(&self) -> Result<FieldType> {
        let values = self.values.read();
        values.reefs_ql_type().context(EncodingValuesSnafu)
    }
}

// Statistics gathered by the Cache.

// levels - point in time measures
const STAT_CACHE_MEMORY_BYTES: &str = "mem_bytes"; // level: Size of in-memory cache in bytes
const STAT_CACHE_DISK_BYTES: &str = "disk_bytes"; // level: Size of on-disk snapshots in bytes
const STAT_SNAPSHOTS: &str = "snapshot_count"; // level: Number of active snapshots.
const STAT_CACHE_AGE_MS: &str = "cache_age_ms"; // level: Number of milliseconds since cache was last snapshoted at sample time

// counters - accumulative measures
const STAT_CACHED_BYTES: &str = "cached_bytes"; // counter: Total number of bytes written into snapshots.
const STAT_WAL_COMPACTION_TIME_MS: &str = "wal_compaction_time_ms"; // counter: Total number of milliseconds spent compacting snapshots

const STAT_CACHE_WRITE_OK: &str = "write_ok";
const STAT_CACHE_WRITE_ERR: &str = "write_err";
const STAT_CACHE_WRITE_DROPPED: &str = "write_dropped";

const VALUE_TYPE_UNDEFINED: u8 = 0;
const VALUE_TYPE_INTEGER: u8 = 1;
const VALUE_TYPE_UNSIGNED: u8 = 2;
const VALUE_TYPE_FLOAT64: u8 = 3;
const VALUE_TYPE_BOOL: u8 = 4;
const VALUE_TYPE_STRING: u8 = 5;

fn value_type(v: &Value) -> u8 {
    match v {
        Value::FloatValue(_) => VALUE_TYPE_FLOAT64,
        Value::IntegerValue(_) => VALUE_TYPE_INTEGER,
        Value::UnsignedValue(_) => VALUE_TYPE_UNSIGNED,
        Value::BoolValue(_) => VALUE_TYPE_BOOL,
        Value::StringValue(_) => VALUE_TYPE_STRING,
        Value::EmptyValue(_) => VALUE_TYPE_UNDEFINED,
    }
}

/// Cache maintains an in-memory store of Values for a set of keys.
pub struct Cache {
    executor: ExecutorRef,
    // Due to a bug in atomic  size needs to be the first word in the struct, as
    // that's the only place where you're guaranteed to be 64-bit aligned on a
    // 32 bit system. See: https://golang.org/pkg/sync/atomic/#pkg-note-BUG
    size: AtomicU64,
    snapshot_size: AtomicU64,

    max_size: RwLock<u64>, // the maximum number of bytes the cache may consume.

    stats: CacheStatistics,

    inner: RwLock<Inner>,
    // TODO:
    // A one time synchronization used to initial the cache with a store.  Since the store can allocate a
    // a large amount memory across shards, we lazily create it.
    // initialize: tomic.Value,
    initialized_count: Arc<AtomicU32>,
}

impl Cache {
    pub fn new(max_size: u64) -> Self {
        Self::new_with_executor(max_size, legacy_global_executor())
    }

    pub fn new_with_executor(max_size: u64, executor: ExecutorRef) -> Self {
        let c = Self {
            executor,
            size: AtomicU64::new(0),
            snapshot_size: AtomicU64::new(0),
            max_size: RwLock::new(max_size),
            stats: CacheStatistics::default(),
            inner: RwLock::new(Inner {
                store: None,
                snapshot: None,
                snapshotting: false,
                snapshot_attempts: 0,
                last_snapshot: Time::from_timestamp(0, 0),
                last_write_time: Time::from_timestamp(0, 0),
            }),
            initialized_count: Arc::new(AtomicU32::new(0)),
        };
        c.update_age();
        c.update_compact_time(Duration::from_secs(0));
        c.update_cached_bytes(0);
        c.update_mem_size(0);
        c.update_snapshots(0);

        c
    }

    /// Returns statistics for periodic monitoring.
    pub fn statistics(&self, tags: HashMap<String, String>) -> crate::models::statistic::Statistic {
        crate::models::statistic::Statistic::new(
            "tsm1_cache".to_string(),
            tags,
            HashMap::from([
                (
                    STAT_CACHE_MEMORY_BYTES.to_string(),
                    self.stats.mem_size_bytes.load(Ordering::SeqCst),
                ),
                (
                    STAT_CACHE_DISK_BYTES.to_string(),
                    self.stats.disk_size_bytes.load(Ordering::SeqCst),
                ),
                (
                    STAT_SNAPSHOTS.to_string(),
                    self.stats.snapshot_count.load(Ordering::SeqCst),
                ),
                (
                    STAT_CACHE_AGE_MS.to_string(),
                    self.stats.cache_age_ms.load(Ordering::SeqCst),
                ),
                (
                    STAT_CACHED_BYTES.to_string(),
                    self.stats.cached_bytes.load(Ordering::SeqCst),
                ),
                (
                    STAT_WAL_COMPACTION_TIME_MS.to_string(),
                    self.stats.wal_compaction_time_ms.load(Ordering::SeqCst),
                ),
                (
                    STAT_CACHE_WRITE_OK.to_string(),
                    self.stats.write_ok.load(Ordering::SeqCst),
                ),
                (
                    STAT_CACHE_WRITE_ERR.to_string(),
                    self.stats.write_err.load(Ordering::SeqCst),
                ),
                (
                    STAT_CACHE_WRITE_DROPPED.to_string(),
                    self.stats.write_dropped.load(Ordering::SeqCst),
                ),
            ]),
        )
    }

    /// Initializes the cache and allocates the underlying store.  Once initialized,
    /// the store re-used until Freed.
    fn init(&self) {
        if self
            .initialized_count
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        let mut inner = self.inner.write();
        inner.store = Some(Arc::new(
            Ring::new_with_executor(RING_SHARDS, Arc::clone(&self.executor))
                .expect("init ring failed"),
        ));
    }

    /// Releases the underlying store and memory held by the Cache.
    pub fn free(&self) {
        if self
            .initialized_count
            .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        let mut inner = self.inner.write();
        inner.store = None;
    }

    // TODO: 性能优化
    // drop(String)
    // drop(Vec<Value>)
    // free, 1.40%
    // ValuesRef(v).size()

    /// WriteMulti writes the map of keys and associated values to the cache. This
    /// function is goroutine-safe. It returns an error if the cache will exceeded
    /// its max size by adding the new values.  The write attempts to write as many
    /// values as possible.  If one key fails, the others can still succeed and an
    /// error will be returned.
    pub fn write_multi(&self, values: HashMap<String, Vec<Value>>) -> Result<()> {
        self.init();

        let mut added_size = 0;
        for value in values.values() {
            // FIXME: takes too long to execute
            // added_size += ValuesRef(value).size() as u64;
            for val in value {
                added_size += match val {
                    Value::IntegerValue(_) => 16,
                    Value::UnsignedValue(_) => 16,
                    Value::FloatValue(_) => 16,
                    Value::BoolValue(_) => 9,
                    Value::StringValue(v) => 8 + (v.value.len() as u64),
                    Value::EmptyValue(_) => 0,
                };
            }
        }

        // Enough room in the cache?
        let limit = *self.max_size.read();
        let n = self.size() + added_size;
        if limit > 0 && n > limit {
            self.stats.write_err.fetch_add(1, Ordering::SeqCst);
            return Err(Error::ErrCacheMemorySizeLimitExceeded { n, limit });
        }

        let mut werr = Ok(());
        let inner = self.inner.read();
        let store = inner.store.clone().unwrap();
        drop(inner);
        // We'll optimistially set size here, and then decrement it for write errors.
        self.increase_size(added_size);
        for (k, v) in values {
            let key_len = k.len() as u64;
            let vsize = ValuesRef(&v).size();
            let (new_key, err) = store.write(k, Values(v));
            if let Err(e) = err {
                // The write failed, hold onto the error and adjust the size delta.
                werr = Err(e);
                added_size -= vsize as u64;
                self.decrease_size(vsize as u64);
            }
            if new_key {
                added_size += key_len;
                self.increase_size(key_len);
            }
        }

        // Some points in the batch were dropped.  An error is returned so
        // error stat is incremented as well.
        if werr.is_err() {
            self.stats.write_dropped.fetch_add(1, Ordering::SeqCst);
            self.stats.write_err.fetch_add(1, Ordering::SeqCst);
        }

        // Update the memory size stat
        self.update_mem_size(added_size as i64);
        self.stats.write_ok.fetch_add(1, Ordering::SeqCst);

        let mut inner = self.inner.write();
        inner.last_write_time = Time::now();

        werr
    }

    pub(crate) fn snapshot_in_progress(&self) -> bool {
        self.inner.read().snapshotting
    }

    pub(crate) fn has_pending_snapshot_values(&self) -> bool {
        self.inner.read().snapshot.as_ref().is_some_and(|snapshot| snapshot.size() != 0)
    }

    /// Takes a snapshot of the current cache, adds it to the slice of caches that
    /// are being flushed, and resets the current cache with new values.
    pub fn snapshot(&self) -> Result<Arc<Cache>> {
        self.init();

        let mut inner = self.inner.write();

        if inner.snapshotting {
            return Err(Error::ErrSnapshotInProgress);
        }

        inner.snapshotting = true;
        inner.snapshot_attempts += 1; // increment the number of times we tried to do this

        // If no snapshot exists, create a new one, otherwise update the existing snapshot
        match &mut inner.snapshot {
            Some(snapshot) => {
                // Did a prior snapshot exist that failed?  If so, return the existing
                // snapshot to retry.
                if snapshot.size() > 0 {
                    return Ok(Arc::clone(snapshot));
                }
            }
            None => {
                let store = Ring::new_with_executor(RING_SHARDS, Arc::clone(&self.executor))?;
                let snapshot = Cache {
                    executor: Arc::clone(&self.executor),
                    size: AtomicU64::new(0),
                    snapshot_size: AtomicU64::new(0),
                    max_size: RwLock::new(0),
                    stats: CacheStatistics::default(),
                    inner: RwLock::new(Inner {
                        store: Some(Arc::new(store)),
                        snapshot: None,
                        snapshotting: false,
                        snapshot_attempts: 0,
                        last_snapshot: Time::from_timestamp(0, 0),
                        last_write_time: Time::from_timestamp(0, 0),
                    }),
                    initialized_count: Arc::new(AtomicU32::new(0)),
                };
                inner.snapshot = Some(Arc::new(snapshot));
            }
        }
        let snapshot = Arc::clone(inner.snapshot.as_ref().unwrap());
        let mut snapshot_inner = snapshot.inner.write();
        std::mem::swap(&mut snapshot_inner.store, &mut inner.store);

        let snapshot_size = self.size();
        // Save the size of the snapshot on the snapshot cache
        inner
            .snapshot
            .as_ref()
            .unwrap()
            .size
            .store(snapshot_size, Ordering::SeqCst);
        // Save the size of the snapshot on the live cache
        self.snapshot_size.store(snapshot_size, Ordering::SeqCst);

        // Reset the cache's store.
        inner.store.as_ref().unwrap().reset();
        self.size.store(0, Ordering::SeqCst);
        inner.last_snapshot = Time::now();

        self.update_cached_bytes(snapshot_size);
        self.update_snapshots(inner.snapshot_attempts as i64);
        Ok(inner.snapshot.clone().unwrap())
    }

    /// Sorts the snapshot before returning it. The compactor and any queries
    /// coming in while it writes will need the values sorted.
    pub fn deduplicate(&self) {
        let inner = self.inner.read();
        let store = inner.store.clone();
        drop(inner);

        // Apply a function that simply calls deduplicate on each entry in the ring.
        // apply cannot return an error in this invocation.
        let _ = store
            .as_ref()
            .unwrap()
            .apply(|_key: &[u8], e: &Arc<Entry>| -> Result<()> {
                e.deduplicate();
                Ok(())
            });
    }

    /// Removes the snapshot cache from the list of flushing caches and adjusts the size.
    pub fn clear_snapshot(&self, success: bool) {
        self.init();

        let inner = self.inner.read();
        let snap_store = inner.snapshot.as_ref().unwrap().inner.read().store.clone();
        drop(inner);

        // reset the snapshot store outside of the write lock
        if success {
            snap_store.as_ref().unwrap().reset();
        }

        let mut inner = self.inner.write();

        inner.snapshotting = false;
        if success {
            inner.snapshot_attempts = 0;
            self.update_mem_size(-(self.snapshot_size.load(Ordering::SeqCst) as i64));

            // Reset the snapshot to a fresh Cache.
            let snapshot_store =
                std::mem::take(&mut inner.snapshot.as_ref().unwrap().inner.write().store);

            let snapshot = Cache {
                executor: Arc::clone(&self.executor),
                size: AtomicU64::new(0),
                snapshot_size: AtomicU64::new(0),
                max_size: RwLock::new(0),
                stats: CacheStatistics::default(),
                inner: RwLock::new(Inner {
                    store: snapshot_store,
                    snapshot: None,
                    snapshotting: false,
                    snapshot_attempts: 0,
                    last_snapshot: Time::from_timestamp(0, 0),
                    last_write_time: Time::from_timestamp(0, 0),
                }),
                initialized_count: Arc::new(AtomicU32::new(0)),
            };

            let defer_store = std::mem::replace(&mut inner.snapshot, Some(Arc::new(snapshot)));
            if let Err(error) = self.executor.spawn_blocking(move || {
                drop(defer_store);
            }) {
                tracing::debug!(
                    target: "reefsdb::tsdb::cache",
                    error = %error,
                    "deferred snapshot cache drop scheduling rejected"
                );
            }

            self.snapshot_size.store(0, Ordering::SeqCst);
            self.update_snapshots(inner.snapshot_attempts as i64);
        }
    }

    /// Returns the number of point-calcuated bytes the cache currently uses.
    pub fn size(&self) -> u64 {
        self.size.load(Ordering::SeqCst) + self.snapshot_size.load(Ordering::SeqCst)
    }

    /// Increases size by delta.
    fn increase_size(&self, delta: u64) {
        self.size.fetch_add(delta, Ordering::SeqCst);
    }

    /// Decreases size by delta.
    fn decrease_size(&self, delta: u64) {
        // Per sync/atomic docs, bit-flip delta minus one to perform subtraction within AtomicU64::fetch_add.
        self.size.fetch_sub(delta, Ordering::SeqCst);
    }

    /// Returns the maximum number of bytes the cache may consume.
    pub fn max_size(&self) -> u64 {
        *self.max_size.read()
    }

    /// Returns the number of values in the store.
    pub fn count(&self) -> i64 {
        let inner = self.inner.read();
        if let Some(store) = &inner.store {
            store.count()
        } else {
            0
        }
    }

    /// Returns a sorted slice of all keys under management by the cache.
    pub fn keys(&self) -> Vec<Vec<u8>> {
        let inner = self.inner.read();
        let store = inner.store.clone();
        drop(inner);
        if let Some(store) = store {
            return store.keys(true);
        }
        vec![]
    }

    /// Returns sorted keys from both the active store and any retained snapshot.
    ///
    /// The caller must serialize this with snapshot rotation and publication.
    pub(crate) fn keys_including_snapshot(&self) -> Vec<Vec<u8>> {
        let snapshot = self.inner.read().snapshot.clone();
        let mut keys = self.keys();
        if let Some(snapshot) = snapshot {
            keys.extend(snapshot.keys());
        }
        keys.sort();
        keys.dedup();
        keys
    }

    /// Returns the number of values in both the active and retained snapshot stores.
    ///
    /// The caller must serialize this with snapshot rotation and publication.
    pub(crate) fn count_including_snapshot(&self) -> i64 {
        let snapshot = self.inner.read().snapshot.clone();
        self.count() + snapshot.map_or(0, |snapshot| snapshot.count())
    }

    /// Records an already resolved sequence while the caller excludes rotation.
    pub(crate) fn set_sequence(&self, key: &[u8], sequence: u64) -> Result<()> {
        let store = self.inner.read().store.clone();
        let entry = store.and_then(|store| store.entry(key)).ok_or_else(|| Error::StringErr {
            reason: "cannot assign a sequence to a missing cache entry".into(),
        })?;
        match entry.sequence.compare_exchange(0, sequence, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => Ok(()),
            Err(existing) if existing == sequence => Ok(()),
            Err(_) => Err(Error::StringErr { reason: "cache sequence identity changed".into() }),
        }
    }

    pub(crate) fn sequence(&self, key: &[u8]) -> Option<u64> {
        let store = self.inner.read().store.clone()?;
        let sequence = store.entry(key)?.sequence.load(Ordering::Acquire);
        (sequence != 0).then_some(sequence)
    }

    pub fn split(cache: Arc<Cache>, n: i32) -> Vec<Arc<Cache>> {
        if n == 1 {
            return vec![cache];
        }

        let mut caches = vec![];
        let storers = if let Some(store) = &cache.inner.read().store {
            store.split(n)
        } else {
            vec![]
        };

        for r in storers {
            let c = Cache::new(0);
            c.inner.write().store = Some(Arc::new(r));
            caches.push(Arc::new(c));
        }

        caches
    }

    /// Returns the series type for a key.
    pub fn series_type(&self, key: &[u8]) -> Result<crate::models::points::FieldType> {
        let e = {
            let inner = self.inner.read();
            if inner.store.is_none() {
                if let Some(snapshot) = &inner.snapshot {
                    snapshot.inner.read().store.as_ref().unwrap().entry(key)
                } else {
                    None
                }
            } else {
                inner.store.as_ref().unwrap().entry(key)
            }
        };

        if let Some(e) = e {
            let typ = e.reefs_ql_type()?;
            match typ {
                VALUE_TYPE_INTEGER => return Ok(crate::models::points::INTEGER),
                VALUE_TYPE_UNSIGNED => return Ok(crate::models::points::UNSIGNED),
                VALUE_TYPE_FLOAT64 => return Ok(crate::models::points::FLOAT),
                VALUE_TYPE_BOOL => return Ok(crate::models::points::BOOL),
                VALUE_TYPE_STRING => return Ok(crate::models::points::STRING),
                _ => {}
            }
        }

        Err(Error::ErrUnknownFieldType)
    }

    fn value_entries(&self, key: &[u8]) -> (Option<Arc<Entry>>, Option<Arc<Entry>>) {
        let lookup = slow_trace::now();
        let (e, snapshot_entries, acquired) = {
            let inner = self.inner.read();
            let acquired = slow_trace::now();
            let e = if let Some(store) = &inner.store {
                store.entry(key)
            } else {
                None
            };

            let snapshot_entries = if let Some(snapshot) = &inner.snapshot {
                snapshot.inner.read().store.as_ref().unwrap().entry(key)
            } else {
                None
            };

            (e, snapshot_entries, acquired)
        };
        let finished = slow_trace::now();
        slow_trace::log_lock_wait_until("query_wait_for_cache_lock", lookup, acquired, "cache capture");
        slow_trace::log_query_stage_until("query_cache_entry_lookup", lookup, finished, "active and snapshot entries");
        (e, snapshot_entries)
    }

    /// Capture only the requested inclusive range, using shared reads for ordered selections.
    /// Snapshot values precede active values so the last write wins after deduplication.
    pub(crate) fn values_in_range(&self, key: &[u8], min: i64, max: i64) -> Values {
        if min > max {
            return Values::default();
        }
        let (active, snapshot) = self.value_entries(key);
        let mut values = Values::default();
        for entry in [snapshot, active].into_iter().flatten() {
            for ordered_only in [true, false] {
                let initial_len = values.len();
                let wait = slow_trace::now();
                let (acquired, finished, examined, complete) = {
                    let source = entry.values.read();
                    let acquired = slow_trace::now();
                    let mut last = None;
                    let mut examined = 0;
                    let mut complete = true;
                    // Writes can append out of order; do not use binary search here.
                    for value in source.iter() {
                        examined += 1;
                        let time = value.unix_nano();
                        if time < min || time > max { continue; }
                        if ordered_only && last.is_some_and(|previous| previous >= time) {
                            complete = false;
                            break;
                        }
                        last = Some(time);
                        values.push(value.clone());
                    }
                    (acquired, slow_trace::now(), examined, complete)
                };
                slow_trace::log_lock_wait_until("cache_entry_range_copy", wait, acquired, "aggregate cache range");
                slow_trace::log_query_stage_until("query_cache_range_copy", acquired, finished, "aggregate cache range");
                slow_trace::profile_work("query_cache_values", "examined_cache_points", examined);
                if complete { break; }
                values.truncate(initial_len);
                // Preserve the existing amortized normalization for unordered selections.
                // Release the read guard first. If a write races the second capture,
                // final deduplication of the private result still preserves precedence.
                entry.deduplicate();
            }
        }
        let merge = slow_trace::now();
        values.deduplicate();
        slow_trace::log_query_stage("query_cache_merge", merge, "captured range values");
        values
    }

    /// Returns a copy of all values, deduped and sorted, for the given key.
    pub fn values(&self, key: &[u8]) -> Values {
        let (e, snapshot_entries) = self.value_entries(key);

        if let Some(e) = &e {
            e.deduplicate();
        } else if snapshot_entries.is_none() {
            return Values::default();
        }

        // Build the sequence of entries that will be returned, in the correct order.
        // Calculate the required size of the destination buffer.
        let mut entries = vec![];
        let mut sz = 0;

        if let Some(snapshot_entries) = snapshot_entries {
            snapshot_entries.deduplicate(); // guarantee we are deduplicated
            sz += snapshot_entries.count();
            entries.push(snapshot_entries);
        }

        if let Some(e) = e {
            sz += e.count();
            entries.push(e);
        }

        // Any entries? If not, return.
        if sz == 0 {
            return Values::default();
        }

        // Create the buffer, and copy all hot values and snapshots. Individual
        // entries are sorted at this point, so now the code has to check if the
        // resultant buffer will be sorted from start to finish.
        let mut values = Values(Vec::with_capacity(sz as usize));
        for e in entries {
            let wait = slow_trace::now();
            let (acquired, finished) = {
                let entry = e.values.read();
                let acquired = slow_trace::now();
                values.extend_from_slice(&entry);
                (acquired, slow_trace::now())
            };
            slow_trace::log_lock_wait_until("cache_entry_copy", wait, acquired, "cache values");
            slow_trace::log_query_stage_until("query_cache_copy", acquired, finished, "cache values");
        }
        let merge = slow_trace::now();
        values.deduplicate();
        slow_trace::log_query_stage("query_cache_merge", merge, "active and snapshot values");

        values
    }

    /// Removes all values for the given keys from the cache.
    pub fn delete(&self, keys: &[&[u8]]) {
        self.delete_range(keys, i64::MIN, i64::MAX)
    }

    /// Removes the values for all keys containing points
    /// with timestamps between between min and max from the cache.
    ///
    /// TODO(edd): Lock usage could possibly be optimised if necessary.
    pub fn delete_range(&self, keys: &[&[u8]], min: i64, max: i64) {
        self.init();
        self.delete_range_initialized(keys, min, max);
    }

    fn delete_range_initialized(&self, keys: &[&[u8]], min: i64, max: i64) {
        let inner = self.inner.write();
        let Some(store) = &inner.store else {
            return;
        };
        self.delete_range_from_store(store, keys, min, max);
        self.stats
            .mem_size_bytes
            .store(self.size() as i64, Ordering::SeqCst);
    }

    fn delete_range_from_store(&self, store: &Ring, keys: &[&[u8]], min: i64, max: i64) {
        for k in keys {
            // Make sure key exist in the cache, skip if it does not
            if let Some(e) = store.entry(k) {
                let orig_size = e.size() as u64;
                if min == i64::MIN && max == i64::MAX {
                    self.decrease_size(orig_size + k.len() as u64);
                    store.remove(k);
                    continue;
                }

                e.filter(min, max);
                if e.count() == 0 {
                    store.remove(k);
                    self.decrease_size(orig_size + k.len() as u64);
                    continue;
                }

                self.decrease_size(orig_size - e.size() as u64);
            } else {
                continue;
            }
        }
    }

    /// Removes values from the active store and any retained snapshot store.
    ///
    /// The caller must serialize this with snapshot rotation and publication.
    pub(crate) fn delete_range_including_snapshot(&self, keys: &[&[u8]], min: i64, max: i64) {
        self.delete_range(keys, min, max);

        let snapshot = self.inner.read().snapshot.clone();
        if let Some(snapshot) = snapshot {
            let snapshot_inner = snapshot.inner.write();
            if let Some(store) = &snapshot_inner.store {
                snapshot.delete_range_from_store(store, keys, min, max);
            }
            snapshot
                .stats
                .mem_size_bytes
                .store(snapshot.size() as i64, Ordering::SeqCst);
            self.snapshot_size.store(snapshot.size(), Ordering::SeqCst);
        }

        self.stats
            .mem_size_bytes
            .store(self.size() as i64, Ordering::SeqCst);
    }

    /// The caller excludes cache rotation and waits for any snapshot writer.
    /// A failed snapshot remains readable and must receive the deletion too.
    pub(crate) fn delete_range_including_idle_snapshot(&self, keys: &[&[u8]], min: i64, max: i64) -> Result<()> {
        let snapshot = {
            let inner = self.inner.read();
            if inner.snapshotting { return Err(Error::ErrSnapshotInProgress); }
            inner.snapshot.clone()
        };
        self.delete_range(keys, min, max);
        if let Some(snapshot) = snapshot {
            let before = snapshot.size();
            // Snapshot wrappers already own a rotated ring even though their
            // lazy-init counter is zero. Initializing here would replace it.
            snapshot.delete_range_initialized(keys, min, max);
            let removed = before - snapshot.size();
            self.snapshot_size.fetch_sub(removed, Ordering::SeqCst);
            self.stats.mem_size_bytes.store(self.size() as i64, Ordering::SeqCst);
        }
        Ok(())
    }

    /// Updates the memory limit of the cache.
    pub fn set_max_size(&self, size: u64) {
        *self.max_size.write() = size;
    }

    /// Returns the values for the key. It assumes the data is already sorted.
    /// It doesn't lock the cache but it does read-lock the entry if there is one for the key.
    /// values should only be used in compact.go in the CacheKeyIterator.
    pub fn values2(&self, key: &[u8]) -> Values {
        let e = {
            let inner = self.inner.read();
            if let Some(store) = &inner.store {
                store.entry(key)
            } else {
                None
            }
        };

        if let Some(e) = e {
            return e.values.read().clone();
        }

        Values::default()
    }

    /// Applies the function f to each entry in the Cache.
    /// apply_entry_fn calls f on each entry in turn, within the same goroutine.
    /// It is safe for use by multiple goroutines.
    pub fn apply_entry_fn<F, E>(&self, f: F) -> Result<(), E>
    where
        F: FnMut(Vec<u8>, &Arc<Entry>) -> Result<(), E>,
    {
        let inner = self.inner.read();
        let store = inner.store.clone();
        drop(inner);
        if let Some(store) = store {
            store.apply_serial(f)
        } else {
            Ok(()) // emptyStore
        }
    }

    pub fn last_write_time(&self) -> Time {
        let inner = self.inner.read();
        inner.last_write_time
    }

    /// Updates the age statistic based on the current time.
    pub fn update_age(&self) {
        let inner = self.inner.read();
        let age_stat = Time::now().sub(inner.last_snapshot).as_millis() as i64;
        self.stats.cache_age_ms.store(age_stat, Ordering::SeqCst);
    }

    // /// Updates WAL compaction time statistic based on d.
    pub fn update_compact_time(&self, d: Duration) {
        let _ = self
            .stats
            .wal_compaction_time_ms
            .fetch_add(d.as_millis() as i64, Ordering::SeqCst);
    }

    /// Increases the cachedBytes counter by b.
    fn update_cached_bytes(&self, b: u64) {
        let _ = self
            .stats
            .cached_bytes
            .fetch_add(b as i64, Ordering::SeqCst);
    }

    /// Updates the memSize level by b.
    fn update_mem_size(&self, b: i64) {
        let _ = self.stats.mem_size_bytes.fetch_add(b, Ordering::SeqCst);
    }

    /// Updates the snapshotsCount and the diskSize levels.
    pub fn update_snapshots(&self, snapshot_attempts: i64) {
        self.stats.disk_size_bytes.store(
            self.snapshot_size.load(Ordering::SeqCst) as i64,
            Ordering::SeqCst,
        );
        self.stats
            .snapshot_count
            .store(snapshot_attempts, Ordering::SeqCst);
    }
}

struct Inner {
    store: Option<Arc<Ring>>, // storer is a ring, after free store is None, all methods should return None/false/0

    // snapshots are the cache objects that are currently being written to tsm files
    // they're kept in memory while flushing so they can be queried along with the cache.
    // they are read only and should never be modified
    snapshot: Option<Arc<Cache>>,
    snapshotting: bool,

    // This number is the number of pending or failed WriteSnaphot attempts since the last successful one.
    snapshot_attempts: i32,

    last_snapshot: Time,
    last_write_time: Time,
}

/// CacheStatistics hold statistics related to the cache.
#[derive(Default)]
pub struct CacheStatistics {
    mem_size_bytes: Arc<AtomicI64>,
    disk_size_bytes: Arc<AtomicI64>,
    snapshot_count: Arc<AtomicI64>,
    cache_age_ms: Arc<AtomicI64>,

    cached_bytes: Arc<AtomicI64>,
    wal_compaction_time_ms: Arc<AtomicI64>,
    write_ok: Arc<AtomicI64>,
    write_err: Arc<AtomicI64>,
    write_dropped: Arc<AtomicI64>,
}

/// CacheLoader processes a set of WAL segment files, and loads a cache with the data
/// contained within those files.
/// Processing of the supplied files take place in the order they exist in the files slice.
pub struct CacheLoader {
    files: Vec<String>,
}

impl CacheLoader {
    pub fn new(files: Vec<String>) -> Self {
        Self { files }
    }

    /// Returns a cache loaded with the data contained within the segment files.
    /// If, during reading of a segment file, corruption is encountered, that segment
    /// file is truncated up to and including the last valid byte, and processing
    /// continues with the next segment file.
    pub fn load(&self, cache: &Cache) -> Result<()> {
        for f in &self.files {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(f)?;
            let stat = file.metadata()?;
            stat.permissions().set_mode(0o666);

            // Log some information about the segments.
            // self.Logger.Info("Reading file", zap.String("path", f.Name()), zap.Int64("size", stat.Size()))

            // Nothing to read, skip it
            if stat.len() == 0 {
                continue;
            }

            let mut r = WalSegmentReader::new(&file);
            for entry in &mut r {
                match entry {
                    Err(e) => {
                        let n = r.read_bytes_count();
                        // self.Logger.Info("File corrupt", zap.Error(err), zap.String("path", f.Name()), zap.Int64("pos", n))
                        if let Err(e) = file.set_len(n as u64) {
                            let _ = r.close();
                            return Err(e.into());
                        }
                        break;
                    }
                    Ok(entry) => match entry {
                        wal::Entry::WriteWalEntry(entry) => {
                            if let Err(e) = cache.write_multi(entry.values) {
                                let _ = r.close();
                                return Err(e);
                            }
                        }
                        wal::Entry::DeleteWalEntry(entry) => cache.delete(
                            &entry
                                .keys
                                .iter()
                                .map(|k| k.as_slice())
                                .collect::<Vec<&[u8]>>(),
                        ),
                        wal::Entry::DeleteRangeWalEntry(entry) => cache.delete_range(
                            &entry
                                .keys
                                .iter()
                                .map(|k| k.as_slice())
                                .collect::<Vec<&[u8]>>(),
                            entry.min,
                            entry.max,
                        ),
                    },
                }
            }
            r.close().context(LoadWALErrSnafu)?;
        }

        Ok(())
    }
}

#[cfg(any(test, feature = "benchmarking"))]
pub mod tests {
    use std::{
        collections::HashMap,
        hash::Hash,
        io::{BufWriter, Write},
        sync::{Arc, atomic::Ordering, mpsc},
        time::Duration,
    };

    use crate::common::{
        time::{self, Time},
        to_string,
        wait_group::WaitGroup,
    };
    use crate::models::points::{BoolValue, FloatValue, IntegerValue, StringValue, Value};
    use crate::runtime::global::spawn_blocking_global;
    use parking_lot::Mutex;
    use tokio::sync::oneshot;

    use super::{Cache, Error, Result};
    use crate::storage::tsm1::{
        data_encoding::encoding_values::{Values, ValuesRef},
        wal_impl::WalSegmentWriter as TSMWALSegmentWriter,
    };

    // Convenience method for testing.
    impl Cache {
        pub fn write(&self, key: &[u8], values: Vec<Value>) -> Result<()> {
            self.write_multi(HashMap::from([(to_string!(key), values)]))
        }

        pub fn write_multi2(&self, values: HashMap<String, Vec<Value>>) -> Result<()> {
            self.init();

            let mut added_size = 0;
            for (k, v) in &values {
                added_size += ValuesRef(v).size() as u64;
            }

            // Enough room in the cache?
            let limit = *self.max_size.read();
            let n = self.size() + added_size;
            if limit > 0 && n > limit {
                self.stats.write_err.fetch_add(1, Ordering::SeqCst);
                return Err(Error::ErrCacheMemorySizeLimitExceeded { n, limit });
            }

            let mut werr = Ok(());
            let inner = self.inner.read();
            let store = inner.store.clone().unwrap();
            drop(inner);

            // We'll optimistially set size here, and then decrement it for write errors.
            self.increase_size(added_size);
            for (k, v) in values {
                let vsize = ValuesRef(&v).size();
                // mock out a Cache's storer implementation.
                let (new_key, err) = {
                    if k == "foo".to_string() {
                        (
                            false,
                            Err(Error::StringErr {
                                reason: "write failed".to_string(),
                            }),
                        )
                    } else {
                        (true, Ok(()))
                    }
                };

                if let Err(e) = err {
                    // The write failed, hold onto the error and adjust the size delta.
                    werr = Err(e);
                    added_size -= vsize as u64;
                    self.decrease_size(vsize as u64);
                }
                if new_key {
                    added_size += k.len() as u64;
                    self.increase_size(k.len() as u64);
                }
            }

            // Some points in the batch were dropped.  An error is returned so
            // error stat is incremented as well.
            if werr.is_err() {
                self.stats.write_dropped.fetch_add(1, Ordering::SeqCst);
                self.stats.write_err.fetch_add(1, Ordering::SeqCst);
            }

            // Update the memory size stat
            self.update_mem_size(added_size as i64);
            self.stats.write_ok.fetch_add(1, Ordering::SeqCst);

            let mut inner = self.inner.write();
            inner.last_write_time = Time::now();

            werr
        }
    }

    #[test]
    fn test_cache_new_cache() {
        let c = Cache::new(100);

        assert_eq!(c.max_size(), 100, "new cache max size not correct");
        assert_eq!(c.size(), 0, "new cache size not correct");
        assert!(
            c.keys().is_empty(),
            "new cache keys not correct:: {:?}",
            c.keys()
        );
    }

    #[test]
    fn test_cache_write_multi() {
        let v0 = Value::FloatValue(FloatValue::new(1, 1.0));
        let v1 = Value::FloatValue(FloatValue::new(2, 2.0));
        let v2 = Value::FloatValue(FloatValue::new(3, 3.0));

        let values_size = v0.size() + v1.size() + v2.size();
        let values = Values(vec![v0, v1, v2]);

        let c = Cache::new(30 * values_size as u64);
        c.write_multi(HashMap::from([
            ("foo".to_string(), values.clone().into()),
            ("bar".to_string(), values.clone().into()),
        ]))
        .unwrap();

        let (got, exp) = (c.size(), 2 * (values_size as u64) + 6);
        assert_eq!(
            got, exp,
            "cache size incorrect after 2 writes, got {}, exp {}",
            got, exp
        );

        let keys = c.keys();
        assert_eq!(
            vec!["bar".as_bytes().to_vec(), "foo".as_bytes().to_vec()],
            keys,
            "cache keys incorrect after 2 writes, exp {:?}, got {:?}",
            vec!["bar".as_bytes().to_vec(), "foo".as_bytes().to_vec()],
            keys
        );
    }

    // Tests that the cache stats and size are correctly maintained during writes.
    #[test]
    fn test_cache_write_multi_stats() {
        let limit = 1u64;
        let c = Cache::new(limit);

        // Not enough room in the cache.
        let v = Value::FloatValue(FloatValue::new(1, 1.0));
        let values = HashMap::from([("foo".to_string(), vec![v.clone(), v.clone()])]);
        let (got, exp) = (c.write_multi(values), ((v.size() * 2) as u64, limit));
        if let Some(e) = got.err() {
            match e {
                Error::ErrCacheMemorySizeLimitExceeded { n, limit } => {
                    assert_eq!((n, limit), exp, "got {:?}, expected: {:?}", (n, limit), exp)
                }
                _ => panic!("unexpected error: {:?}", e),
            }
        } else {
            panic!("expected error");
        }

        // Fail one of the values in the write.
        let c = Cache::new(50);
        c.init();
        let values = HashMap::from([
            ("foo".to_string(), vec![v.clone(), v.clone()]),
            ("bar".to_string(), vec![v.clone()]),
        ]);
        let (got, exp) = (c.write_multi2(values), "write failed".to_string()); // mock just write `bar`
        if let Some(e) = got.err() {
            match e {
                Error::StringErr { reason } => {
                    assert_eq!(reason, exp, "got {:?}, expected: {:?}", reason, exp)
                }
                _ => panic!("unexpected error: {:?}", e),
            }
        } else {
            panic!("expected error");
        }

        // Cache size decreased correctly.
        let (got, exp) = (c.size(), 16u64 + 3);
        assert_eq!(got, exp, "got {:?}, expected: {:?}", got, exp);

        // Write stats updated
        let (got, exp) = (c.stats.write_dropped.load(Ordering::Relaxed), 1i64);
        assert_eq!(got, exp, "got {:?}, expected: {:?}", got, exp);
        let (got, exp) = (c.stats.write_err.load(Ordering::Relaxed), 1i64);
        assert_eq!(got, exp, "got {:?}, expected: {:?}", got, exp);
    }

    #[test]
    fn test_cache_write_multi_type_conflict() {
        let v0 = Value::FloatValue(FloatValue::new(1, 1.0));
        let v1 = Value::FloatValue(FloatValue::new(2, 2.0));
        let v2 = Value::IntegerValue(IntegerValue::new(3, 3i64));

        let values_size = v0.size() + v1.size() + v2.size();
        let v0_size = v0.size();
        let values = Values(vec![v0, v1, v2]);

        let c = Cache::new(3 * values_size as u64);
        assert!(
            c.write_multi(HashMap::from([
                ("foo".to_string(), vec![values[1].clone()]),
                ("bar".to_string(), Vec::from(&values[1..])),
            ]))
            .is_err(),
            "expected field type conflict"
        );

        let (got, exp) = (v0_size as u64 + 3, c.size() as u64);
        assert_eq!(
            exp, got,
            "cache size incorrect after 2 writes, exp {}, got {}",
            exp, got
        );

        let (exp, keys) = (vec!["foo".as_bytes().to_vec()], c.keys());
        assert_eq!(
            keys, exp,
            "cache keys incorrect after 2 writes, exp {:?}, got {:?}",
            exp, keys
        );
    }

    #[test]
    fn test_cache_delete_range() {
        let v0 = Value::FloatValue(FloatValue::new(1, 1.0));
        let v1 = Value::FloatValue(FloatValue::new(2, 2.0));
        let v2 = Value::FloatValue(FloatValue::new(3, 3.0));

        let v0_size = v0.size();
        let values_size = v0.size() + v1.size() + v2.size();
        let values = Values(vec![v0, v1, v2]);

        let c = Cache::new(30 * values_size as u64);
        c.write_multi(HashMap::from([
            ("foo".to_string(), values.clone().into()),
            ("bar".to_string(), values.clone().into()),
        ]))
        .unwrap();

        let n = c.size();
        assert_eq!(
            n,
            (2 * values_size + 6) as u64,
            "cache size incorrect after 2 writes, exp {}, got {}",
            2 * values_size,
            n
        );

        let (exp, keys) = (
            vec!["bar".as_bytes().to_vec(), "foo".as_bytes().to_vec()],
            c.keys(),
        );
        assert_eq!(
            keys, exp,
            "cache keys incorrect after 2 writes, exp {:?}, got {:?}",
            exp, keys
        );

        c.delete_range(&["bar".as_bytes()], 2, i64::MAX);
        let (exp, keys) = (
            vec!["bar".as_bytes().to_vec(), "foo".as_bytes().to_vec()],
            c.keys(),
        );
        assert_eq!(
            keys, exp,
            "cache keys incorrect after 2 writes, exp {:?}, got {:?}",
            exp, keys
        );

        let (got, exp) = (c.size(), (values_size + v0_size + 6) as u64);
        assert_eq!(
            exp, got,
            "cache size incorrect after 2 writes, exp {}, got {}",
            exp, got
        );

        let (got, exp) = (c.values("bar".as_bytes()).len(), 1);
        assert_eq!(exp, got, "cache values mismatch: exp {}, got {}", exp, got);

        let (got, exp) = (c.values("foo".as_bytes()).len(), 3);
        assert_eq!(exp, got, "cache values mismatch: exp {}, got {}", exp, got);
    }

    #[test]
    fn test_cache_delete_range_no_values() {
        let v0 = Value::FloatValue(FloatValue::new(1, 1.0));
        let v1 = Value::FloatValue(FloatValue::new(2, 2.0));
        let v2 = Value::FloatValue(FloatValue::new(3, 3.0));

        let values_size = v0.size() + v1.size() + v2.size();
        let values = Values(vec![v0, v1, v2]);

        let c = Cache::new(3 * values_size as u64);
        c.write_multi(HashMap::from([("foo".to_string(), values.clone().into())]))
            .unwrap();

        let n = c.size();
        assert_eq!(
            n,
            (values_size + 3) as u64,
            "cache size incorrect after 2 writes, exp {}, got {}",
            2 * values_size,
            n
        );

        let (exp, keys) = (vec!["foo".as_bytes().to_vec()], c.keys());
        assert_eq!(
            keys, exp,
            "cache keys incorrect after 2 writes, exp {:?}, got {:?}",
            exp, keys
        );

        c.delete_range(&["foo".as_bytes()], i64::MIN, i64::MAX);

        let keys = c.keys();
        assert!(
            keys.is_empty(),
            "cache size incorrect after 2 writes, exp empty, got {:?}",
            keys
        );

        let (got, exp) = (c.size(), 0);
        assert_eq!(
            exp, got,
            "cache size incorrect after 2 writes, exp {}, got {}",
            exp, got
        );

        let values = c.values("foo".as_bytes());
        assert!(
            values.is_empty(),
            "cache values mismatch: exp empty, got {:?}",
            values
        );
    }

    #[test]
    fn test_cache_delete_range_not_sorted() {
        let v0 = Value::FloatValue(FloatValue::new(1, 1.0));
        let v1 = Value::FloatValue(FloatValue::new(3, 3.0));
        let v2 = Value::FloatValue(FloatValue::new(2, 2.0));

        let values_size = v0.size() + v1.size() + v2.size();
        let values = Values(vec![v0, v1, v2]);

        let c = Cache::new(3 * values_size as u64);
        c.write_multi(HashMap::from([("foo".to_string(), values.clone().into())]))
            .unwrap();

        let (exp, keys) = (vec!["foo".as_bytes().to_vec()], c.keys());
        assert_eq!(
            keys, exp,
            "cache keys incorrect after 2 writes, exp {:?}, got {:?}",
            exp, keys
        );

        c.delete_range(&["foo".as_bytes()], 1, 3);

        let keys = c.keys();
        assert!(
            keys.is_empty(),
            "cache size incorrect after 2 writes, exp empty, got {:?}",
            keys
        );

        let (got, exp) = (c.size(), 0);
        assert_eq!(
            exp, got,
            "cache size incorrect after 2 writes, exp {}, got {}",
            exp, got
        );

        let values = c.values("foo".as_bytes());
        assert!(
            values.is_empty(),
            "cache values mismatch: exp empty, got {:?}",
            values
        );
    }

    #[test]
    fn test_cache_delete() {
        let v0 = Value::FloatValue(FloatValue::new(1, 1.0));
        let v1 = Value::FloatValue(FloatValue::new(2, 2.0));
        let v2 = Value::FloatValue(FloatValue::new(3, 3.0));

        let values_size = v0.size() + v1.size() + v2.size();
        let values = Values(vec![v0, v1, v2]);

        let c = Cache::new(30 * values_size as u64);
        c.write_multi(HashMap::from([
            ("foo".to_string(), values.clone().into()),
            ("bar".to_string(), values.clone().into()),
        ]))
        .unwrap();

        let n = c.size();
        assert_eq!(
            n,
            (2 * values_size + 6) as u64,
            "cache size incorrect after 2 writes, exp {}, got {}",
            2 * values_size,
            n
        );

        let (exp, keys) = (
            vec!["bar".as_bytes().to_vec(), "foo".as_bytes().to_vec()],
            c.keys(),
        );
        assert_eq!(
            keys, exp,
            "cache keys incorrect after 2 writes, exp {:?}, got {:?}",
            exp, keys
        );

        c.delete(&["bar".as_bytes()]);

        let (exp, keys) = (vec!["foo".as_bytes().to_vec()], c.keys());
        assert_eq!(
            keys, exp,
            "cache keys incorrect after 2 writes, exp {:?}, got {:?}",
            exp, keys
        );

        let (got, exp) = (c.size(), values_size as u64 + 3);
        assert_eq!(
            exp, got,
            "cache size incorrect after 2 writes, exp {}, got {}",
            exp, got
        );

        let values = c.values("bar".as_bytes());
        assert!(
            values.is_empty(),
            "cache values mismatch: exp empty, got {:?}",
            values
        );

        let (exp, got) = (3, c.values("foo".as_bytes()).len() as u64);
        assert_eq!(exp, got, "cache values mismatch: exp {}, got {}", exp, got);
    }

    #[test]
    fn test_cache_delete_non_existent() {
        let c = Cache::new(1024);

        c.delete(&["bar".as_bytes()]);

        let (got, exp) = (c.size(), 0);
        assert_eq!(exp, got, "cache size incorrect exp {}, got {}", exp, got);
    }

    // This tests writing two batches to the same series.  The first batch
    // is sorted.  The second batch is also sorted but contains duplicates.
    #[test]
    fn test_cache_write_multi_duplicates() {
        let v0 = Value::FloatValue(FloatValue::new(2, 1.0));
        let v1 = Value::FloatValue(FloatValue::new(3, 1.0));
        let values0 = Values(vec![v0.clone(), v1.clone()]);

        let v3 = Value::FloatValue(FloatValue::new(4, 2.0));
        let v4 = Value::FloatValue(FloatValue::new(5, 3.0));
        let v5 = Value::FloatValue(FloatValue::new(5, 4.0));
        let values1 = Values(vec![v3.clone(), v4.clone(), v5.clone()]);

        let c = Cache::new(0);

        c.write_multi(HashMap::from([("foo".to_string(), values0.into())]))
            .unwrap();

        c.write_multi(HashMap::from([("foo".to_string(), values1.into())]))
            .unwrap();

        let (exp, keys) = (vec!["foo".as_bytes().to_vec()], c.keys());
        assert_eq!(
            keys, exp,
            "cache keys incorrect after 2 writes, exp {:?}, got {:?}",
            exp, keys
        );

        let exp_asc_values = Values(vec![v0, v1, v3, v5]);
        let (exp, got) = (exp_asc_values.len(), c.values("foo".as_bytes()).len());
        assert_eq!(exp, got, "value count mismatch: exp: {}, got {}", exp, got);

        let deduped = c.values("foo".as_bytes());
        assert_eq!(
            exp_asc_values, deduped,
            "deduped ascending values for foo incorrect, exp: {:?}, got {:?}",
            exp_asc_values, deduped
        );
    }

    #[test]
    fn range_capture_preserves_snapshot_precedence_and_unordered_overwrites() {
        for strings in [false, true] {
            for retry in [false, true] {
                for (min, max) in [(i64::MIN, i64::MAX), (4, 7), (8, 8), (0, 0), (9, 1)] {
                    let cache = Cache::new(0);
                    let value = |time, v: i64| if strings {
                        Value::StringValue(StringValue::new(time, format!("value-{v}")))
                    } else {
                        Value::IntegerValue(IntegerValue::new(time, v))
                    };
                    cache.write(b"key", [(8, 80), (1, 10), (4, 40), (1, 11)]
                        .into_iter().map(|(t, v)| value(t, v)).collect()).unwrap();
                    let snapshot = cache.snapshot().unwrap();
                    if retry { cache.clear_snapshot(false); }
                    cache.write(b"key", [(7, 70), (1, 100), (4, 400), (7, 71)]
                        .into_iter().map(|(t, v)| value(t, v)).collect()).unwrap();
                    let expected = [(1, 100), (4, 400), (7, 71), (8, 80)].into_iter()
                        .filter(|(t, _)| min <= *t && *t <= max)
                        .map(|(t, v)| value(t, v)).collect::<Vec<_>>();
                    assert_eq!(cache.values_in_range(b"key", min, max).0, expected);
                    assert!(cache.values_in_range(b"missing", min, max).is_empty());
                    // An empty or singleton selection must not normalize unrelated values.
                    if expected.len() <= 1 {
                        assert_eq!(cache.value_entries(b"key").0.unwrap().values.read().len(), 4);
                        assert_eq!(snapshot.value_entries(b"key").0.unwrap().values.read().len(), 4);
                    }
                }
            }
        }
    }

    #[test]
    fn range_capture_does_not_require_an_exclusive_entry_lock() {
        let cache = Cache::new(0);
        cache.write(b"key", vec![
            Value::IntegerValue(IntegerValue::new(2, 20)),
            Value::IntegerValue(IntegerValue::new(1, 10)),
        ]).unwrap();
        let entry = cache.value_entries(b"key").0.unwrap();
        let result = std::thread::scope(|scope| {
            let guard = entry.values.read();
            let (sender, receiver) = mpsc::channel();
            let cache = &cache;
            let worker = scope.spawn(move || {
                sender.send(cache.values_in_range(b"key", 1, 1)).unwrap();
            });
            let result = receiver.recv_timeout(Duration::from_secs(1));
            drop(guard);
            worker.join().unwrap();
            result
        }).expect("range capture waited for an unnecessary exclusive entry lock");
        assert_eq!(result.0, vec![Value::IntegerValue(IntegerValue::new(1, 10))]);
    }

    #[test]
    fn test_cache_values() {
        let v0 = Value::FloatValue(FloatValue::new(1, 0.0));
        let v1 = Value::FloatValue(FloatValue::new(2, 2.0));
        let v2 = Value::FloatValue(FloatValue::new(3, 3.0));
        let v3 = Value::FloatValue(FloatValue::new(1, 1.0));
        let v4 = Value::FloatValue(FloatValue::new(4, 4.0));

        let c = Cache::new(512);

        let deduped = c.values("no such key".as_bytes());
        assert!(deduped.is_empty(), "expected empty, got {:?}", deduped);

        c.write(
            "foo".as_bytes(),
            vec![v0.clone(), v1.clone(), v2.clone(), v3.clone()],
        )
        .unwrap();
        c.write("foo".as_bytes(), vec![v4.clone()]).unwrap();

        let exp_asc_values = Values(vec![v3, v1, v2, v4]);
        let (exp, got) = (exp_asc_values, c.values("foo".as_bytes()));
        assert_eq!(
            exp, got,
            "deduped ascending values for foo incorrect, exp: {:?}, got {:?}",
            exp, deduped
        );
    }

    #[test]
    fn test_cache_snapshot() {
        let v0 = Value::FloatValue(FloatValue::new(2, 0.0));
        let v1 = Value::FloatValue(FloatValue::new(3, 2.0));
        let v2 = Value::FloatValue(FloatValue::new(4, 3.0));
        let v3 = Value::FloatValue(FloatValue::new(5, 4.0));
        let v4 = Value::FloatValue(FloatValue::new(6, 5.0));
        let v5 = Value::FloatValue(FloatValue::new(1, 5.0));
        let v6 = Value::FloatValue(FloatValue::new(7, 5.0));
        let v7 = Value::FloatValue(FloatValue::new(2, 5.0));

        let c = Cache::new(512);
        c.write(
            "foo".as_bytes(),
            vec![v0.clone(), v1.clone(), v2.clone(), v3.clone()],
        )
        .unwrap();

        // Grab snapshot, and ensure it's as expected.
        let snapshot = c.snapshot().unwrap();

        let exp_values = Values(vec![v0.clone(), v1.clone(), v2.clone(), v3.clone()]);
        let deduped = snapshot.values("foo".as_bytes());
        assert_eq!(
            exp_values, deduped,
            "snapshotted values for foo incorrect, exp: {:?}, got {:?}",
            exp_values, deduped
        );

        // Ensure cache is still as expected.
        let deduped = c.values("foo".as_bytes());
        assert_eq!(
            exp_values, deduped,
            "post-snapshot values for foo incorrect, exp: {:?}, got {:?}",
            exp_values, deduped
        );

        // Write a new value to the cache.
        c.write("foo".as_bytes(), vec![v4.clone()]).unwrap();
        let exp_values = Values(vec![
            v0.clone(),
            v1.clone(),
            v2.clone(),
            v3.clone(),
            v4.clone(),
        ]);
        let deduped = c.values("foo".as_bytes());
        assert_eq!(
            exp_values, deduped,
            "post-snapshot write for foo incorrect, exp: {:?}, got {:?}",
            exp_values, deduped
        );

        // Write a new, out-of-order, value to the cache.
        c.write("foo".as_bytes(), vec![v5.clone()]).unwrap();
        let exp_values = Values(vec![
            v5.clone(),
            v0.clone(),
            v1.clone(),
            v2.clone(),
            v3.clone(),
            v4.clone(),
        ]);
        let deduped = c.values("foo".as_bytes());
        assert_eq!(
            exp_values, deduped,
            "post-snapshot out-of-order write values for foo incorrect, exp: {:?}, got {:?}",
            exp_values, deduped
        );

        // Clear snapshot, ensuring non-snapshot data untouched.
        c.clear_snapshot(true);

        let exp_values = Values(vec![v5.clone(), v4.clone()]);
        let deduped = c.values("foo".as_bytes());
        assert_eq!(
            exp_values, deduped,
            "post-clear values for foo incorrect, exp: {:?}, got {:?}",
            exp_values, deduped
        );

        // Create another snapshot
        let _ = c.snapshot().unwrap();

        c.write("foo".as_bytes(), vec![v4.clone(), v5.clone()])
            .unwrap();

        c.clear_snapshot(true);

        let _ = c.snapshot().unwrap();

        c.write("foo".as_bytes(), vec![v6.clone(), v7.clone()])
            .unwrap();

        let exp_values = Values(vec![v5.clone(), v7.clone(), v4.clone(), v6.clone()]);
        let deduped = c.values("foo".as_bytes());
        assert_eq!(
            exp_values, deduped,
            "post-snapshot out-of-order write values for foo incorrect, exp: {:?}, got {:?}",
            exp_values, deduped
        );
    }

    #[test]
    fn test_cache_delete_range_including_snapshot_preserves_unrelated_values() {
        let c = Cache::new(512);
        let foo = Value::FloatValue(FloatValue::new(1, 1.0));
        let bar = Value::FloatValue(FloatValue::new(2, 2.0));

        c.write("foo".as_bytes(), vec![foo]).unwrap();
        c.write("bar".as_bytes(), vec![bar.clone()]).unwrap();
        c.snapshot().unwrap();
        c.clear_snapshot(false);

        c.delete_range_including_snapshot(&["foo".as_bytes()], i64::MIN, i64::MAX);

        assert!(c.values("foo".as_bytes()).is_empty());
        assert_eq!(c.values("bar".as_bytes()), Values(vec![bar]));
        assert_eq!(c.keys_including_snapshot(), vec![b"bar".to_vec()]);
        assert_eq!(c.count_including_snapshot(), 1);
    }

    #[test]
    fn test_cache_snapshot_stats() {
        let limit = 16u64;
        let c = Cache::new(limit);

        let values = HashMap::from([(
            "foo".to_string(),
            vec![Value::FloatValue(FloatValue::new(1, 1.0))],
        )]);
        c.write_multi(values).unwrap();

        let (got, exp) = (c.stats.mem_size_bytes.load(Ordering::Relaxed), 16 + 3);
        assert_eq!(got, exp, "got {}, exp: {}", got, exp);

        let _ = c.snapshot().unwrap();

        // Store size should have been reset.
        let (got, exp) = (c.size(), 16 + 3);
        assert_eq!(got, exp, "got {}, exp: {}", got, exp);

        // Cached bytes should have been increased.
        let (got, exp) = (c.stats.cached_bytes.load(Ordering::Relaxed), 16 + 3);
        assert_eq!(got, exp, "got {}, exp: {}", got, exp);

        let (got, exp) = (c.stats.mem_size_bytes.load(Ordering::Relaxed), 16 + 3);
        assert_eq!(got, exp, "got {}, exp: {}", got, exp);
    }

    #[test]
    fn test_cache_empty_snapshot() {
        let c = Cache::new(512);

        // Grab snapshot, and ensure it's as expected.
        let snapshot = c.snapshot().unwrap();
        let deduped = snapshot.values("foo".as_bytes());
        assert!(
            deduped.is_empty(),
            "snapshotted values for foo incorrect, exp empty, got {:?}",
            deduped
        );

        // Ensure cache is still as expected.
        let deduped = c.values("foo".as_bytes());
        assert!(
            deduped.is_empty(),
            "post-snapshotted values for foo incorrect, exp empty, got {:?}",
            deduped
        );

        // Clear snapshot.
        c.clear_snapshot(true);
        let deduped = c.values("foo".as_bytes());
        assert!(
            deduped.is_empty(),
            "post-snapshot-clear values for foo incorrect, exp empty, got {:?}",
            deduped
        );
    }

    #[test]
    fn test_cache_write_memory_exceeded() {
        let v0 = Value::FloatValue(FloatValue::new(1, 1.0));
        let v1 = Value::FloatValue(FloatValue::new(2, 2.0));

        let c = Cache::new(v1.size() as u64);

        c.write("foo".as_bytes(), vec![v0.clone()]).unwrap();
        let keys = c.keys();
        assert_eq!(
            vec!["foo".as_bytes().to_vec()],
            keys,
            "cache keys incorrect after writes, exp {:?}, got {:?}",
            vec!["foo".as_bytes().to_vec()],
            keys
        );

        let err = c.write("bar".as_bytes(), vec![v1.clone()]);
        if let Some(e) = err.err() {
            match e {
                Error::ErrCacheMemorySizeLimitExceeded { n, limit } => {}
                _ => panic!("wrong error writing key bar to cache: {:?}", e),
            }
        } else {
            panic!("wrong error writing key bar to cache");
        }

        // Grab snapshot, write should still fail since we're still using the memory.
        let _ = c.snapshot().unwrap();
        let err = c.write("bar".as_bytes(), vec![v1.clone()]);
        if let Some(e) = err.err() {
            match e {
                Error::ErrCacheMemorySizeLimitExceeded { n, limit } => {}
                _ => panic!("wrong error writing key bar to cache: {:?}", e),
            }
        } else {
            panic!("wrong error writing key bar to cache");
        }

        // Clear the snapshot and the write should now succeed.
        c.clear_snapshot(true);
        c.write("bar".as_bytes(), vec![v1.clone()]).unwrap();

        let exp_asc_values = Values(vec![v1]);
        let deduped = c.values("bar".as_bytes());
        assert_eq!(
            exp_asc_values, deduped,
            "deduped ascending values for bar incorrect, exp: {:?}, got {:?}",
            exp_asc_values, deduped
        );
    }

    use rand::Rng;
    #[test]
    fn test_cache_deduplicated_concurrent() {
        let mut values = HashMap::new();

        for i in 0..1000 {
            for j in 0..100 {
                values.insert(
                    format!("cpu{}", i),
                    vec![Value::FloatValue(FloatValue::new(
                        (i + j) + rand::thread_rng().gen_range(0..10) as i64,
                        i as f64,
                    ))],
                );
            }
        }

        let wg = WaitGroup::new();
        let c = Arc::new(Cache::new(1000000));

        wg.add(1);
        let c1 = Arc::clone(&c);
        let wg1 = wg.clone();
        spawn_blocking_global(move || {
            let values = values;
            for i in 0..1000 {
                let _ = c1.write_multi(values.clone());
            }
            wg1.done();
        });

        std::thread::sleep(Duration::from_millis(10)); // wait write_multi to init cache.inner.store
        wg.add(1);
        let c2 = Arc::clone(&c);
        let wg2 = wg.clone();
        spawn_blocking_global(move || {
            for i in 0..1000 {
                let _ = c2.deduplicate();
            }
            wg2.done();
        });

        wg.wait();
    }

    use super::CacheLoader;
    // Ensure the CacheLoader can correctly load from a single segment, even if it's corrupted.
    #[test]
    fn test_cache_loader_single() {
        // Create a WAL segment.
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();
        let sync_waiters = Arc::new(crossbeam_channel::bounded(1024));
        let mut w = TSMWALSegmentWriter::new(
            Arc::new(fpath.clone()),
            wal::DEFAULT_SEGMENT_SIZE,
            0,
            BufWriter::new(f),
            sync_waiters,
            0,
        );

        let p1 = Value::FloatValue(FloatValue::new(1, 1.1));
        let p2 = Value::IntegerValue(IntegerValue::new(1, 1i64));
        let p3 = Value::BoolValue(BoolValue::new(1, true));

        let values: HashMap<String, Vec<Value>> = HashMap::from([
            ("foo".to_string(), vec![p1.clone()]),
            ("bar".to_string(), vec![p2.clone()]),
            ("baz".to_string(), vec![p3.clone()]),
        ]);

        let entry = WriteWalEntry {
            values: values.clone(),
            size: 0, // size will be updated in marshal_size
        };

        let stats = Arc::new(WalStatistics::default());
        let (entry_type, compressed) = must_marshal_entry(Entry::WriteWalEntry(entry));
        assert!(
            w.write_entry(entry_type, compressed.as_slice(), Arc::clone(&stats))
                .is_ok()
        );
        w.flush().unwrap();

        // Load the cache using the segment.
        let cache = Cache::new(1024);
        let loader = CacheLoader::new(vec![fpath.to_str().unwrap().to_string()]);
        loader.load(&cache).unwrap();

        // Check the cache.
        let (exp, got) = (Values(vec![p1.clone()]), cache.values("foo".as_bytes()));
        assert_eq!(
            exp, got,
            "cache key foo not as expected, got {:?}, exp {:?}",
            got, exp
        );

        let (exp, got) = (Values(vec![p2.clone()]), cache.values("bar".as_bytes()));
        assert_eq!(
            exp, got,
            "cache key bar not as expected, got {:?}, exp {:?}",
            exp, got
        );

        let (exp, got) = (Values(vec![p3.clone()]), cache.values("baz".as_bytes()));
        assert_eq!(
            exp, got,
            "cache key baz not as expected, got {:?}, exp {:?}",
            exp, got
        );

        // Corrupt the WAL segment.
        w.writer.write(&[1, 4, 0, 0, 0]).unwrap();

        // Reload the cache using the segment.
        let cache = Cache::new(1024);
        let loader = CacheLoader::new(vec![fpath.to_str().unwrap().to_string()]);
        loader.load(&cache).unwrap();

        // Check the cache.
        let (exp, got) = (Values(vec![p1.clone()]), cache.values("foo".as_bytes()));
        assert_eq!(
            exp, got,
            "cache key foo not as expected, got {:?}, exp {:?}",
            got, exp
        );

        let (exp, got) = (Values(vec![p2.clone()]), cache.values("bar".as_bytes()));
        assert_eq!(
            exp, got,
            "cache key bar not as expected, got {:?}, exp {:?}",
            exp, got
        );

        let (exp, got) = (Values(vec![p3.clone()]), cache.values("baz".as_bytes()));
        assert_eq!(
            exp, got,
            "cache key baz not as expected, got {:?}, exp {:?}",
            exp, got
        );

        std::fs::remove_file(fpath).unwrap();
    }

    // Ensure the CacheLoader can correctly load from two segments, even if one is corrupted.
    #[test]
    fn test_cache_loader_double() {
        // Create a WAL segment.
        let dir = must_temp_tsm_dir();
        let (f1, fpath1) = must_temp_tsm_file(dir.path()).keep().unwrap();
        let (f2, fpath2) = must_temp_tsm_file(dir.path()).keep().unwrap();
        let mut w1 = TSMWALSegmentWriter::new(
            Arc::new(fpath1.clone()),
            wal::DEFAULT_SEGMENT_SIZE,
            0,
            BufWriter::new(f1),
            Arc::new(crossbeam_channel::bounded(1024)),
            0,
        );
        let mut w2 = TSMWALSegmentWriter::new(
            Arc::new(fpath2.clone()),
            wal::DEFAULT_SEGMENT_SIZE,
            0,
            BufWriter::new(f2),
            Arc::new(crossbeam_channel::bounded(1024)),
            0,
        );

        let p1 = Value::FloatValue(FloatValue::new(1, 1.1));
        let p2 = Value::IntegerValue(IntegerValue::new(1, 1i64));
        let p3 = Value::BoolValue(BoolValue::new(1, true));
        let p4 = Value::StringValue(StringValue::new(1, "string".to_string()));

        // Write first segment
        let values: HashMap<String, Vec<Value>> = HashMap::from([
            ("foo".to_string(), vec![p1.clone()]),
            ("bar".to_string(), vec![p2.clone()]),
        ]);

        let entry = WriteWalEntry {
            values: values.clone(),
            size: 0, // size will be updated in marshal_size
        };

        let stats = Arc::new(WalStatistics::default());
        let (entry_type, compressed) = must_marshal_entry(Entry::WriteWalEntry(entry));
        assert!(
            w1.write_entry(entry_type, compressed.as_slice(), Arc::clone(&stats))
                .is_ok()
        );
        w1.flush().unwrap();

        // Write second segment
        let values: HashMap<String, Vec<Value>> = HashMap::from([
            ("baz".to_string(), vec![p3.clone()]),
            ("qux".to_string(), vec![p4.clone()]),
        ]);

        let entry = WriteWalEntry {
            values: values.clone(),
            size: 0, // size will be updated in marshal_size
        };

        let stats = Arc::new(WalStatistics::default());
        let (entry_type, compressed) = must_marshal_entry(Entry::WriteWalEntry(entry));
        assert!(
            w2.write_entry(entry_type, compressed.as_slice(), Arc::clone(&stats))
                .is_ok()
        );
        w2.flush().unwrap();

        // Corrupt the first WAL segment.
        w1.writer.write(&[1, 4, 0, 0, 0]).unwrap();

        // Reload the cache using the segment.
        let cache = Cache::new(1024);
        let loader = CacheLoader::new(vec![
            fpath1.to_str().unwrap().to_string(),
            fpath2.to_str().unwrap().to_string(),
        ]);
        loader.load(&cache).unwrap();

        // Check the cache.
        let (exp, got) = (Values(vec![p1.clone()]), cache.values("foo".as_bytes()));
        assert_eq!(
            exp, got,
            "cache key foo not as expected, got {:?}, exp {:?}",
            got, exp
        );

        let (exp, got) = (Values(vec![p2.clone()]), cache.values("bar".as_bytes()));
        assert_eq!(
            exp, got,
            "cache key bar not as expected, got {:?}, exp {:?}",
            exp, got
        );

        let (exp, got) = (Values(vec![p3.clone()]), cache.values("baz".as_bytes()));
        assert_eq!(
            exp, got,
            "cache key baz not as expected, got {:?}, exp {:?}",
            exp, got
        );

        let (exp, got) = (Values(vec![p4.clone()]), cache.values("qux".as_bytes()));
        assert_eq!(
            exp, got,
            "cache key baz not as expected, got {:?}, exp {:?}",
            exp, got
        );

        std::fs::remove_file(fpath1).unwrap();
        std::fs::remove_file(fpath2).unwrap();
    }

    // Ensure the CacheLoader can load deleted series
    #[test]
    fn test_cache_loader_deleted() {
        // Create a WAL segment.
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();
        let mut w = TSMWALSegmentWriter::new(
            Arc::new(fpath.clone()),
            wal::DEFAULT_SEGMENT_SIZE,
            0,
            BufWriter::new(f),
            Arc::new(crossbeam_channel::bounded(1024)),
            0,
        );

        let p1 = Value::FloatValue(FloatValue::new(1, 1.0));
        let p2 = Value::FloatValue(FloatValue::new(2, 2.0));
        let p3 = Value::FloatValue(FloatValue::new(3, 3.0));

        let values: HashMap<String, Vec<Value>> =
            HashMap::from([("foo".to_string(), vec![p1.clone(), p2.clone(), p3.clone()])]);

        let entry = WriteWalEntry {
            values: values.clone(),
            size: 0, // size will be updated in marshal_size
        };

        let stats = Arc::new(WalStatistics::default());
        let (entry_type, compressed) = must_marshal_entry(Entry::WriteWalEntry(entry));
        assert!(
            w.write_entry(entry_type, compressed.as_slice(), Arc::clone(&stats))
                .is_ok()
        );
        w.flush().unwrap();

        let dentry = DeleteRangeWalEntry {
            keys: vec!["foo".as_bytes().to_vec()],
            min: 2,
            max: 3,
            size: 0,
        };

        let (entry_type, compressed) = must_marshal_entry(Entry::DeleteRangeWalEntry(dentry));
        assert!(
            w.write_entry(entry_type, compressed.as_slice(), Arc::clone(&stats))
                .is_ok()
        );
        w.flush().unwrap();

        // Load the cache using the segment.
        let cache = Cache::new(1024);
        let loader = CacheLoader::new(vec![fpath.to_str().unwrap().to_string()]);
        loader.load(&cache).unwrap();

        // Check the cache.
        let (exp, got) = (Values(vec![p1.clone()]), cache.values("foo".as_bytes()));
        assert_eq!(
            exp, got,
            "cache key foo not as expected, got {:?}, exp {:?}",
            got, exp
        );

        // Reload the cache using the segment.
        let cache = Cache::new(1024);
        let loader = CacheLoader::new(vec![fpath.to_str().unwrap().to_string()]);
        loader.load(&cache).unwrap();

        // Check the cache.
        let (exp, got) = (Values(vec![p1.clone()]), cache.values("foo".as_bytes()));
        assert_eq!(
            exp, got,
            "cache key foo not as expected, got {:?}, exp {:?}",
            got, exp
        );

        std::fs::remove_file(fpath).unwrap();
    }

    #[test]
    fn test_cache_split() {
        let v0 = Value::FloatValue(FloatValue::new(1, 1.0));
        let v1 = Value::FloatValue(FloatValue::new(2, 2.0));
        let v2 = Value::FloatValue(FloatValue::new(3, 3.0));

        let values_size = v0.size() + v1.size() + v2.size();
        let values = Values(vec![v0, v1, v2]);

        let c = Arc::new(Cache::new(0));
        c.write("foo".as_bytes(), values.clone().into()).unwrap();
        c.write("bar".as_bytes(), values.clone().into()).unwrap();
        c.write("baz".as_bytes(), values.clone().into()).unwrap();

        let (got, exp) = (c.size(), 3 * (values_size as u64) + 9);
        assert_eq!(
            got, exp,
            "cache size incorrect after 3 writes, got {}, exp {}",
            got, exp
        );

        let splits = Cache::split(c, 3);
        let mut keys = HashMap::new();
        for s in splits.iter() {
            let skeys = s.keys();
            for k in &skeys {
                keys.insert(k.clone(), s.values(k).size());
            }
        }

        for key in vec!["foo", "bar", "baz"] {
            assert!(
                keys.contains_key(key.as_bytes()),
                "missing key, exp {}, got None",
                key
            );
        }
    }

    #[test]
    fn test_cache_check_concurrent_reads_are_safe() {
        let mut values = Values(Vec::with_capacity(1000));
        let mut timestamps = Vec::with_capacity(1000);
        let mut series = Vec::with_capacity(100);
        for i in 0..1000 {
            timestamps.push(rand::thread_rng().gen_range(0..1000) as i64);
        }

        for i in 0..1000 {
            values.push(Value::FloatValue(FloatValue::new(timestamps[i], i as f64)))
        }

        for i in 0..100 {
            series.push(format!("series{}", i));
        }

        let wg = WaitGroup::new();
        let c = Arc::new(Cache::new(1000000));

        for s in series {
            for (idx, v) in values.iter().enumerate() {
                let _ = c.write(s.as_bytes(), vec![v.clone()]); // maybe ErrCacheMemorySizeLimitExceeded, ignore
            }
            let wg = wg.clone();
            wg.add(3);
            for i in 0..3 {
                let c = Arc::clone(&c);
                let wg = wg.clone();
                let s = s.clone();
                std::thread::spawn(move || {
                    let _ = c.values(s.as_bytes());
                    wg.done();
                });
            }
        }

        wg.wait();
    }

    #[test]
    fn test_cache_race_2_compactors() {
        let mut values = Values(Vec::with_capacity(1000));
        let mut timestamps = Vec::with_capacity(1000);
        let mut series = Vec::with_capacity(100);
        for i in 0..1000 {
            timestamps.push(rand::thread_rng().gen_range(0..1000) as i64);
        }

        for i in 0..1000 {
            values.push(Value::FloatValue(FloatValue::new(timestamps[i], i as f64)))
        }

        for i in 0..100 {
            series.push(format!("series{}", i));
        }

        let wg = WaitGroup::new();
        let c = Arc::new(Cache::new(1000000));

        for s in series {
            for (idx, v) in values.iter().enumerate() {
                let _ = c.write(s.as_bytes(), vec![v.clone()]); // maybe ErrCacheMemorySizeLimitExceeded, ignore
            }
            let wg = wg.clone();
            wg.add(1);

            let c = Arc::clone(&c);
            let wg = wg.clone();
            let s = s.clone();
            std::thread::spawn(move || {
                let _ = c.values(s.as_bytes());
                wg.done();
            });
        }

        #[derive(Default)]
        struct CacheFile {
            file_counter: i32,
            map_files: HashMap<i32, bool>,
        }
        let cache_file = Arc::new(Mutex::new(CacheFile::default()));
        let (s, r) = crossbeam_channel::bounded::<Option<Error>>(2);
        for i in 0..2 {
            let wg = wg.clone();
            wg.add(1);
            let sender = s.clone();
            let cache_file = Arc::clone(&cache_file);
            let c = Arc::clone(&c);

            spawn_blocking_global(move || {
                let s = match c.snapshot() {
                    Ok(s) => s,
                    Err(e) => match e {
                        Error::ErrSnapshotInProgress => {
                            wg.done();
                            sender.send(None);
                            return;
                        }
                        e => {
                            sender.send(Some(Error::StringErr {
                                reason: format!("failed to snapshot cache: {:?}", e),
                            }));
                            wg.done();
                            return;
                        }
                    },
                };

                let mut my_files = HashMap::new();
                {
                    let mut cache_file = cache_file.lock();
                    let fcounter = cache_file.file_counter;
                    cache_file.map_files.insert(fcounter, true);
                    cache_file.file_counter += 1;

                    for (k, v) in &cache_file.map_files {
                        my_files.insert(*k, *v);
                    }
                }

                s.deduplicate();
                c.clear_snapshot(true);

                {
                    let mut cache_file = cache_file.lock();
                    for k in my_files.keys() {
                        if cache_file.map_files.contains_key(k) {
                            cache_file.map_files.remove(k);
                        } else {
                            sender.send(Some(Error::StringErr {
                                reason: "something else deleted one of my files".to_string(),
                            }));
                            wg.done();
                            return;
                        }
                    }
                }

                wg.done();
                sender.send(None);
            });
        }

        wg.wait(); // TODO: there is no need to use WaitGroup, because we have already use channel to collect errors ?

        // Check for error
        for _ in 0..2 {
            if let Ok(Some(e)) = r.recv() {
                eprintln!("{:?}", e);
            }
        }
    }

    #[cfg(test)]
    #[ctor::ctor]
    fn init_runtime() {
        let runtime_opt = crate::runtime::global::RuntimeOptions::default();
        crate::runtime::global::init_global_runtimes(&runtime_opt);
    }

    // /////////// test util functions ////////////////////
    use std::path::{Path, PathBuf};

    use tempfile::{NamedTempFile, TempDir};

    use crate::storage::tsm1::wal::{self, *};

    pub fn must_temp_tsm_dir() -> TempDir {
        tempfile::TempDir::with_prefix("tsm1-test")
            .expect("unable to create tsm temporary working directory")
    }

    pub fn must_temp_tsm_file(path: &Path) -> NamedTempFile {
        tempfile::Builder::new()
            .prefix("tsm1test")
            .tempfile_in(path)
            .unwrap()
    }

    fn must_marshal_entry(mut entry: Entry) -> (WalEntryType, Vec<u8>) {
        let mut bytes = vec![0u8; 1024 << 2];

        entry.encode(&mut bytes).unwrap();
        let compressed = snap::raw::Encoder::new()
            .compress_vec(bytes.as_slice())
            .expect("snappy encode failed");

        (entry.entry_type(), compressed)
    }
}
