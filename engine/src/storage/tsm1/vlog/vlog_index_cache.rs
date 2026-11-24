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

use crate::common::time::Time;
use crate::runtime::execution::{
    ExecutorRef, RuntimeExecutorExt, SpawnError, legacy_global_executor,
};
use parking_lot::RwLock;
use snafu::{ResultExt, Snafu, ensure};

use super::vlog_index_ring::{Ring, VLogIndexEntry, VLogIndexes};
use crate::{
    common::slow_trace,
    storage::tsm1::vlog::{
        VLogIndex,
        vlog_index_wal::{self, WalSegmentReader},
    },
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
    LoadWALErr { source: vlog_index_wal::Error },

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

    /// String error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },

    /// The host executor refused required index-cache work.
    #[snafu(display("index cache execution error: {}", source))]
    Execution { source: SpawnError },

    #[snafu(display("accepted index-cache task did not complete"))]
    TaskTerminated,
}

/// Result type for Cache.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IOError { reason: err }
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
    // Derived snapshots share this counter; unrelated cache instances do not.
    #[cfg(test)]
    keys_by_gen_and_seq_calls: Arc<std::sync::atomic::AtomicUsize>,
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
            #[cfg(test)]
            keys_by_gen_and_seq_calls: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
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
    pub fn write_multi(&self, values: HashMap<String, Vec<VLogIndex>>) -> Result<()> {
        self.init();

        let mut added_size = 0;
        for value in values.values() {
            added_size += value.iter().map(VLogIndex::memory_size).sum::<usize>() as u64;
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
            let vsize = v.iter().map(VLogIndex::memory_size).sum::<usize>();
            let (new_key, err) = store.write(&k, v);
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
                    #[cfg(test)]
                    keys_by_gen_and_seq_calls: Arc::clone(&self.keys_by_gen_and_seq_calls),
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
        let _ =
            store
                .as_ref()
                .unwrap()
                .apply(|_key: &[u8], e: &Arc<VLogIndexEntry>| -> Result<()> {
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
                #[cfg(test)]
                keys_by_gen_and_seq_calls: Arc::clone(&self.keys_by_gen_and_seq_calls),
            };

            let defer_store = std::mem::replace(&mut inner.snapshot, Some(Arc::new(snapshot)));
            if let Err(error) = self.executor.spawn_blocking(move || {
                drop(defer_store);
            }) {
                tracing::debug!(
                    target: "reefsdb::tsdb::vlog_index_cache",
                    error = %error,
                    "deferred index snapshot drop scheduling rejected"
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
    pub fn count(&self) -> usize {
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

    /// Returns a sorted slice of all keys by file generation and sequence under management by the cache.
    pub fn keys_by_gen_and_seq(&self, generation: i64, seq: i64) -> Vec<(Vec<u8>, u8)> {
        #[cfg(test)]
        self.keys_by_gen_and_seq_calls.fetch_add(1, Ordering::Relaxed);

        let inner = self.inner.read();
        let store = inner.store.clone();
        drop(inner);
        if let Some(store) = store {
            return store.keys_by_gen_and_seq(true, generation, seq);
        }
        vec![]
    }

    #[cfg(test)]
    pub(crate) fn keys_by_gen_and_seq_call_count(&self) -> usize {
        self.keys_by_gen_and_seq_calls.load(Ordering::Relaxed)
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
            #[cfg(test)]
            let c = Cache {
                keys_by_gen_and_seq_calls: Arc::clone(&cache.keys_by_gen_and_seq_calls),
                ..c
            };
            c.inner.write().store = Some(Arc::new(r));
            caches.push(Arc::new(c));
        }

        caches
    }

    /// Returns a copy of all values, deduped and sorted, for the given key from the cache.
    pub fn values(&self, key: &[u8]) -> VLogIndexes {
        let (e, snapshot_entries) = {
            let inner = self.inner.read();
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

            (e, snapshot_entries)
        };

        if let Some(e) = &e {
            e.deduplicate();
        } else if snapshot_entries.is_none() {
            return VLogIndexes::default();
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
            return VLogIndexes::default();
        }

        // Create the buffer, and copy all hot values and snapshots. Individual
        // entries are sorted at this point, so now the code has to check if the
        // resultant buffer will be sorted from start to finish.
        let mut values = VLogIndexes(Vec::with_capacity(sz as usize));
        for e in entries {
            values.extend_from_slice(&e.values.read());
        }
        values.deduplicate();

        // sort by generation
        values.sort_by(|a, b| {
            let order_by_vlog_id = a.generation.cmp(&b.generation);
            if order_by_vlog_id == std::cmp::Ordering::Equal {
                // If vlog_id.0 is equal, compare by min_time
                a.min.cmp(&b.min)
            } else {
                order_by_vlog_id
            }
        });

        values
    }

    /// Returns a copy of all values, deduped and sorted, for the given key with generation and
    /// sequence number from the cache.
    pub fn values_by_gen_and_seq(&self, key: &[u8], generation: i64, seq: i64) -> VLogIndexes {
        let (e, snapshot_entries) = {
            let inner = self.inner.read();
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

            (e, snapshot_entries)
        };

        let mut indexes = VLogIndexes::default();

        if let Some(e) = &e {
            indexes.append(&mut e.filter_by_gen_and_seq(generation, seq));
        } else if snapshot_entries.is_none() {
            return VLogIndexes::default();
        }

        if let Some(snapshot_entries) = snapshot_entries {
            indexes.append(&mut snapshot_entries.filter_by_gen_and_seq(generation, seq));
        }

        indexes.deduplicate_exact();

        // Keep distinct same-time-range indexes from the requested file. Lazy compaction
        // uses this API to account physical block bytes, so offset/size-distinct entries
        // must not be collapsed by the general cross-file deduplication logic.
        indexes.sort_by(|a, b| {
            let order_by_vlog_id = a.generation.cmp(&b.generation);
            if order_by_vlog_id == std::cmp::Ordering::Equal {
                // If vlog_id.0 is equal, compare by min_time
                a.min.cmp(&b.min)
            } else {
                order_by_vlog_id
            }
        });

        indexes
    }

    /// Returns a copy of all values, deduped and sorted, for the given key with timestamps
    /// between between min and max from the cache..
    pub(crate) fn aggregate_sources(&self, key: &[u8]) -> super::aggregate_record::AggregateSources {
        let entry = {
            let inner = self.inner.read();
            inner.store.as_ref().and_then(|store| store.entry(key))
        };
        entry.map_or_else(Default::default, |entry| entry.aggregate_sources())
    }

    pub(crate) fn aggregate_files(&self) -> std::collections::HashSet<u32> {
        let mut result = std::collections::HashSet::new();
        let _: Result<()> = self.apply_entry_fn(|_, entry| {
            result.extend(entry.aggregate_sources().keys().copied());
            Ok(())
        });
        result
    }

    pub fn values_range(&self, key: &[u8], min: i64, max: i64) -> VLogIndexes {
        let lookup_start = slow_trace::now();
        let (e, snapshot_entries) = {
            let lock_start = slow_trace::now();
            let inner = self.inner.read();
            slow_trace::log_lock_wait(
                "vlog_index_cache.read",
                lock_start,
                "stage=query_wait_for_vlog_lock",
            );
            let e = if let Some(store) = &inner.store {
                store.entry(key)
            } else {
                None
            };

            let snapshot_entries = if let Some(snapshot) = &inner.snapshot {
                let lock_start = slow_trace::now();
                let snapshot_inner = snapshot.inner.read();
                slow_trace::log_lock_wait(
                    "vlog_index_cache_snapshot.read",
                    lock_start,
                    "stage=query_wait_for_vlog_lock",
                );
                snapshot_inner.store.as_ref().unwrap().entry(key)
            } else {
                None
            };

            (e, snapshot_entries)
        };

        if e.is_none() && snapshot_entries.is_none() {
            slow_trace::log_query_stage(
                "query_vlog_lookup",
                lookup_start,
                "component=vlog_index_cache",
            );
            return VLogIndexes::default();
        }

        // Build the sequence of entries that will be returned, in the correct order.
        // Calculate the required size of the destination buffer.
        let mut entries = vec![];
        let mut sz = 0;

        if let Some(snapshot_entries) = snapshot_entries {
            sz += snapshot_entries.count();
            entries.push(snapshot_entries);
        }

        if let Some(e) = e {
            sz += e.count();
            entries.push(e);
        }

        // Any entries? If not, return.
        if sz == 0 {
            slow_trace::log_query_stage(
                "query_vlog_lookup",
                lookup_start,
                "component=vlog_index_cache",
            );
            return VLogIndexes::default();
        }

        // Create the buffer, and copy all hot values and snapshots. Individual
        // entries are sorted at this point, so now the code has to check if the
        // resultant buffer will be sorted from start to finish.
        let mut values = VLogIndexes(Vec::with_capacity(sz));
        for e in entries {
            let lock_start = slow_trace::now();
            let entry_values = e.values.read();
            slow_trace::log_lock_wait(
                "vlog_index_cache_entry.values.read",
                lock_start,
                "stage=query_wait_for_vlog_lock",
            );
            for entry in entry_values.iter() {
                if entry.min <= max && entry.max >= min {
                    values.push(entry.clone());
                }
            }
        }
        values.deduplicate_exact();

        // sort by generation
        values.sort_by(|a, b| {
            let order_by_vlog_id = a.generation.cmp(&b.generation);
            if order_by_vlog_id == std::cmp::Ordering::Equal {
                // If generation is equal, compare by min_time
                a.min.cmp(&b.min)
            } else {
                order_by_vlog_id
            }
        });

        slow_trace::log_query_stage(
            "query_vlog_lookup",
            lookup_start,
            "component=vlog_index_cache",
        );
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

        let inner = self.inner.write();
        for k in keys {
            // Make sure key exist in the cache, skip if it does not
            let store = if let Some(store) = &inner.store {
                store
            } else {
                continue;
            };

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
        self.stats
            .mem_size_bytes
            .store(self.size() as i64, Ordering::SeqCst);
    }

    /// Remove all the values by the keys and files.
    pub fn delete_multi(&self, indexes: &HashMap<String, Vec<VLogIndex>>) {
        self.init();

        let inner = self.inner.write();
        for (k, idxes) in indexes.iter() {
            // Make sure key exist in the cache, skip if it does not
            let store = if let Some(store) = &inner.store {
                store
            } else {
                continue;
            };

            if let Some(e) = store.entry(k.as_bytes()) {
                let orig_size = e.size() as u64;
                // if min == i64::MIN && max == i64::MAX {
                //     self.decrease_size(orig_size + k.len() as u64);
                //     store.remove(k);
                //     continue;
                // }

                e.delete_vlog_index(idxes);
                if e.count() == 0 {
                    store.remove(k.as_bytes());
                    self.decrease_size(orig_size + k.len() as u64);
                    continue;
                }

                self.decrease_size(orig_size - e.size() as u64);
            } else {
                continue;
            }
        }

        self.stats
            .mem_size_bytes
            .store(self.size() as i64, Ordering::SeqCst);
    }

    /// Updates the memory limit of the cache.
    pub fn set_max_size(&self, size: u64) {
        *self.max_size.write() = size;
    }

    /// Returns the values for the key. It assumes the data is already sorted.
    /// It doesn't lock the cache but it does read-lock the entry if there is one for the key.
    /// values should only be used in compact.go in the CacheKeyIterator.
    pub fn values2(&self, key: &[u8]) -> VLogIndexes {
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

        VLogIndexes::default()
    }

    /// Applies the function f to each entry in the Cache.
    /// apply_entry_fn calls f on each entry in turn, within the same goroutine.
    /// It is safe for use by multiple goroutines.
    pub fn apply_entry_fn<F, E>(&self, f: F) -> Result<(), E>
    where
        F: FnMut(Vec<u8>, &Arc<VLogIndexEntry>) -> Result<(), E>,
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

// /////////////////////////////////////////////////////////////////////////////////////////////////
// /////////////////////////////////////////////////////////////////////////////////////////////////
// /////////////////////////////////////////////////////////////////////////////////////////////////

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
                return Ok(());
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
                        vlog_index_wal::Entry::WriteWalEntry(entry) => {
                            if let Err(e) = cache.write_multi(entry.values) {
                                let _ = r.close();
                                return Err(e);
                            }
                        }
                        vlog_index_wal::Entry::DeleteWalEntry(entry) => {
                            // TODO: implement me
                            // cache.delete(
                            //     &entry
                            //         .keys
                            //         .iter()
                            //         .map(|k| k.as_slice())
                            //         .collect::<Vec<&[u8]>>(),
                            // );
                        }
                        vlog_index_wal::Entry::DeleteRangeWalEntry(entry) => {
                            // TODO: implement me
                            // cache.delete_range(
                            //     &entry
                            //         .keys
                            //         .iter()
                            //         .map(|k| k.as_slice())
                            //         .collect::<Vec<&[u8]>>(),
                            //     entry.min,
                            //     entry.max,
                            // );
                        }
                        vlog_index_wal::Entry::DeleteMultiWalEntry(entry) => {
                            cache.delete_multi(&entry.values);
                        }
                        vlog_index_wal::Entry::ReplaceMultiWalEntry(entry) => {
                            cache.delete_multi(&entry.old_indexes);
                            if let Err(e) = cache.write_multi(entry.new_indexes) {
                                let _ = r.close();
                                return Err(e);
                            }
                        }
                    },
                }
            }
            r.close().context(LoadWALErrSnafu)?;
        }

        Ok(())
    }
}

// /////////////////////////////////////////////////////////////////////////////////////////////////
// /////////////////////////////////////////////////////////////////////////////////////////////////
// /////////////////////////////////////////////////////////////////////////////////////////////////

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
    use crate::runtime::global::{block_on_global, spawn_blocking_global};
    use parking_lot::Mutex;
    use tokio::sync::oneshot;

    use super::{Cache, CacheLoader, Error, Result};

    #[test]
    fn file_keys_are_unique_without_dropping_block_locations() {
        for blocks in [1_i64, 2, 128] {
            let cache = Cache::new(0);
            let indexes = (0..blocks)
                .map(|offset| VLogIndex {
                    count: 1,
                    typ: 1,
                    min: offset,
                    max: offset,
                    generation: 3,
                    seq: 2,
                    offset,
                    size: 10,
                    ..Default::default()
                })
                .collect::<Vec<_>>();
            for key in [b"foo".as_slice(), b"bar".as_slice()] {
                cache.write(key, indexes.clone()).unwrap();
            }
            let mut other = indexes[0].clone();
            other.generation = 4;
            cache.write(b"foo", vec![other]).unwrap();
            let mut other = indexes[0].clone();
            other.seq = 3;
            cache.write(b"foo", vec![other]).unwrap();

            assert_eq!(
                cache.keys_by_gen_and_seq(3, 2),
                vec![(b"bar".to_vec(), 1), (b"foo".to_vec(), 1)]
            );
            assert!(cache.keys_by_gen_and_seq(9, 2).is_empty());
            assert_eq!(cache.keys_by_gen_and_seq(4, 2), vec![(b"foo".to_vec(), 1)]);
            assert_eq!(cache.keys_by_gen_and_seq(3, 3), vec![(b"foo".to_vec(), 1)]);
            for key in [b"foo".as_slice(), b"bar".as_slice()] {
                assert_eq!(cache.values_by_gen_and_seq(key, 3, 2).0, indexes);
            }
        }
    }

    #[test]
    fn key_scan_counter_isolated_between_caches_and_shared_with_workers() {
        let observed = Arc::new(Cache::new(0));
        let unrelated = Arc::new(Cache::new(0));
        let snapshot = observed.snapshot().unwrap();
        let snapshot_parts = Cache::split(snapshot, 2);
        assert!(!snapshot_parts.is_empty());
        let workers = snapshot_parts
            .into_iter()
            .chain([Arc::clone(&observed), Arc::clone(&unrelated)])
            .map(|cache| {
                std::thread::spawn(move || {
                    cache.keys_by_gen_and_seq(1, 1);
                })
            })
            .collect::<Vec<_>>();
        let observed_calls = workers.len() - 1;
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(observed.keys_by_gen_and_seq_call_count(), observed_calls);
        assert_eq!(unrelated.keys_by_gen_and_seq_call_count(), 1);

        observed.clear_snapshot(true);
        observed.snapshot().unwrap().keys_by_gen_and_seq(1, 1);
        assert_eq!(observed.keys_by_gen_and_seq_call_count(), observed_calls + 1);
        assert_eq!(unrelated.keys_by_gen_and_seq_call_count(), 1);
    }

    // Convenience method for testing.
    impl Cache {
        pub fn write(&self, key: &[u8], values: Vec<VLogIndex>) -> Result<()> {
            self.write_multi(HashMap::from([(to_string!(key), values)]))
        }

        pub fn write_multi2(&self, values: HashMap<String, Vec<VLogIndex>>) -> Result<()> {
            self.init();

            let mut added_size = 0;
            for value in values.values() {
                added_size += value.iter().map(VLogIndex::memory_size).sum::<usize>() as u64;
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
                let vsize = v.iter().map(VLogIndex::memory_size).sum::<usize>();

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
        let v0 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 1,
            max: 10,
            generation: 1,
            seq: 1,
            offset: 1,
            size: 10,
        };
        let v1 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 3,
            max: 30,
            generation: 1,
            seq: 1,
            offset: 3,
            size: 30,
        };
        let v2 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 5,
            max: 50,
            generation: 1,
            seq: 1,
            offset: 5,
            size: 50,
        };

        let values_size = VLogIndex::SIZE * 3;
        let values = VLogIndexes(vec![v0, v1, v2]);

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

    #[test]
    fn test_values_by_gen_and_seq_does_not_cross_file_dedup() {
        let c = Cache::new(0);
        let key = "foo";
        let old_file_index = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 10,
            max: 20,
            generation: 1,
            seq: 1,
            offset: 100,
            size: 111,
        };
        let target_file_index = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 10,
            max: 20,
            generation: 2,
            seq: 1,
            offset: 200,
            size: 222,
        };
        let newer_file_index = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 10,
            max: 20,
            generation: 3,
            seq: 1,
            offset: 300,
            size: 333,
        };

        c.write_multi(HashMap::from([(
            key.to_string(),
            vec![
                old_file_index.clone(),
                target_file_index.clone(),
                newer_file_index.clone(),
            ],
        )]))
        .unwrap();

        assert_eq!(
            c.values_by_gen_and_seq(key.as_bytes(), 2, 1),
            VLogIndexes(vec![target_file_index])
        );
        assert_eq!(
            c.values_by_gen_and_seq(key.as_bytes(), 1, 1),
            VLogIndexes(vec![old_file_index])
        );
        assert_eq!(
            c.values_by_gen_and_seq(key.as_bytes(), 3, 1),
            VLogIndexes(vec![newer_file_index])
        );
    }

    #[test]
    fn test_values_by_gen_and_seq_singleton_checks_requested_file() {
        let c = Cache::new(0);
        let key = "foo";
        let index = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 10,
            max: 20,
            generation: 2,
            seq: 1,
            offset: 100,
            size: 111,
        };

        c.write_multi(HashMap::from([(key.to_string(), vec![index.clone()])]))
            .unwrap();

        assert_eq!(
            c.values_by_gen_and_seq(key.as_bytes(), 1, 1),
            VLogIndexes::default()
        );
        assert_eq!(
            c.values_by_gen_and_seq(key.as_bytes(), 2, 1),
            VLogIndexes(vec![index])
        );
    }

    #[test]
    fn test_values_by_gen_and_seq_same_time_range_across_files() {
        let c = Cache::new(0);
        let key = "foo";
        let indexes = vec![
            VLogIndex {
                aggregate: None,
                count: 1,
                typ: 1,
                min: 10,
                max: 20,
                generation: 1,
                seq: 1,
                offset: 100,
                size: 111,
            },
            VLogIndex {
                aggregate: None,
                count: 1,
                typ: 1,
                min: 10,
                max: 20,
                generation: 1,
                seq: 2,
                offset: 200,
                size: 222,
            },
            VLogIndex {
                aggregate: None,
                count: 1,
                typ: 1,
                min: 10,
                max: 20,
                generation: 2,
                seq: 1,
                offset: 300,
                size: 333,
            },
        ];

        c.write_multi(HashMap::from([(key.to_string(), indexes.clone())]))
            .unwrap();

        for index in indexes {
            assert_eq!(
                c.values_by_gen_and_seq(key.as_bytes(), index.generation, index.seq),
                VLogIndexes(vec![index])
            );
        }
    }

    #[test]
    fn test_values_range_preserves_same_time_range_indexes_across_files() {
        let c = Cache::new(0);
        let key = "foo";
        let indexes = vec![
            VLogIndex {
                aggregate: None,
                count: 1,
                typ: 1,
                min: 10,
                max: 20,
                generation: 1,
                seq: 1,
                offset: 100,
                size: 111,
            },
            VLogIndex {
                aggregate: None,
                count: 1,
                typ: 1,
                min: 10,
                max: 20,
                generation: 2,
                seq: 1,
                offset: 200,
                size: 222,
            },
            VLogIndex {
                aggregate: None,
                count: 1,
                typ: 1,
                min: 10,
                max: 20,
                generation: 3,
                seq: 1,
                offset: 300,
                size: 333,
            },
        ];

        c.write_multi(HashMap::from([(key.to_string(), indexes.clone())]))
            .unwrap();

        assert_eq!(
            c.values_range(key.as_bytes(), 10, 20),
            VLogIndexes(indexes.clone())
        );
        assert_eq!(c.values2(key.as_bytes()), VLogIndexes(indexes));
    }

    // Tests that the cache stats and size are correctly maintained during writes.
    #[test]
    fn test_cache_write_multi_stats() {
        let limit = 1u64;
        let c = Cache::new(limit);

        // Not enough room in the cache.
        let v = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 1,
            max: 2,
            generation: 1,
            seq: 1,
            offset: 1,
            size: 2,
        };
        let values = HashMap::from([("foo".to_string(), vec![v.clone(), v.clone()])]);
        let (got, exp) = (c.write_multi(values), ((VLogIndex::SIZE * 2) as u64, limit));
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
        let c = Cache::new(500);
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
        let (got, exp) = (c.size(), (VLogIndex::SIZE + 3) as u64);
        assert_eq!(got, exp, "got {:?}, expected: {:?}", got, exp);

        // Write stats updated
        let (got, exp) = (c.stats.write_dropped.load(Ordering::Relaxed), 1i64);
        assert_eq!(got, exp, "got {:?}, expected: {:?}", got, exp);
        let (got, exp) = (c.stats.write_err.load(Ordering::Relaxed), 1i64);
        assert_eq!(got, exp, "got {:?}, expected: {:?}", got, exp);
    }

    // //////////////////////////////////////////////////////////////////////////////////
    // //////////////////////////////////////////////////////////////////////////////////
    // //////////////////////////////////////////////////////////////////////////////////

    // #[test]
    // fn test_cache_delete_range() {
    //     let v0 = Value::FloatValue(FloatValue::new(1, 1.0));
    //     let v1 = Value::FloatValue(FloatValue::new(2, 2.0));
    //     let v2 = Value::FloatValue(FloatValue::new(3, 3.0));

    //     let v0_size = v0.size();
    //     let values_size = v0.size() + v1.size() + v2.size();
    //     let values = Values(vec![v0, v1, v2]);

    //     let c = Cache::new(30 * values_size as u64);
    //     c.write_multi(HashMap::from([
    //         ("foo".to_string(), values.clone().into()),
    //         ("bar".to_string(), values.clone().into()),
    //     ]))
    //     .unwrap();

    //     let n = c.size();
    //     assert_eq!(
    //         n,
    //         (2 * values_size + 6) as u64,
    //         "cache size incorrect after 2 writes, exp {}, got {}",
    //         2 * values_size,
    //         n
    //     );

    //     let (exp, keys) = (
    //         vec!["bar".as_bytes().to_vec(), "foo".as_bytes().to_vec()],
    //         c.keys(),
    //     );
    //     assert_eq!(
    //         keys, exp,
    //         "cache keys incorrect after 2 writes, exp {:?}, got {:?}",
    //         exp, keys
    //     );

    //     c.delete_range(&["bar".as_bytes()], 2, i64::MAX);
    //     let (exp, keys) = (
    //         vec!["bar".as_bytes().to_vec(), "foo".as_bytes().to_vec()],
    //         c.keys(),
    //     );
    //     assert_eq!(
    //         keys, exp,
    //         "cache keys incorrect after 2 writes, exp {:?}, got {:?}",
    //         exp, keys
    //     );

    //     let (got, exp) = (c.size(), (values_size + v0_size + 6) as u64);
    //     assert_eq!(
    //         exp, got,
    //         "cache size incorrect after 2 writes, exp {}, got {}",
    //         exp, got
    //     );

    //     let (got, exp) = (c.values("bar".as_bytes()).len(), 1);
    //     assert_eq!(exp, got, "cache values mismatch: exp {}, got {}", exp, got);

    //     let (got, exp) = (c.values("foo".as_bytes()).len(), 3);
    //     assert_eq!(exp, got, "cache values mismatch: exp {}, got {}", exp, got);
    // }

    // #[test]
    // fn test_cache_delete_range_no_values() {
    //     let v0 = Value::FloatValue(FloatValue::new(1, 1.0));
    //     let v1 = Value::FloatValue(FloatValue::new(2, 2.0));
    //     let v2 = Value::FloatValue(FloatValue::new(3, 3.0));

    //     let values_size = v0.size() + v1.size() + v2.size();
    //     let values = Values(vec![v0, v1, v2]);

    //     let c = Cache::new(3 * values_size as u64);
    //     c.write_multi(HashMap::from([("foo".to_string(), values.clone().into())]))
    //         .unwrap();

    //     let n = c.size();
    //     assert_eq!(
    //         n,
    //         (values_size + 3) as u64,
    //         "cache size incorrect after 2 writes, exp {}, got {}",
    //         2 * values_size,
    //         n
    //     );

    //     let (exp, keys) = (vec!["foo".as_bytes().to_vec()], c.keys());
    //     assert_eq!(
    //         keys, exp,
    //         "cache keys incorrect after 2 writes, exp {:?}, got {:?}",
    //         exp, keys
    //     );

    //     c.delete_range(&["foo".as_bytes()], i64::MIN, i64::MAX);

    //     let keys = c.keys();
    //     assert!(
    //         keys.is_empty(),
    //         "cache size incorrect after 2 writes, exp empty, got {:?}",
    //         keys
    //     );

    //     let (got, exp) = (c.size(), 0);
    //     assert_eq!(
    //         exp, got,
    //         "cache size incorrect after 2 writes, exp {}, got {}",
    //         exp, got
    //     );

    //     let values = c.values("foo".as_bytes());
    //     assert!(
    //         values.is_empty(),
    //         "cache values mismatch: exp empty, got {:?}",
    //         values
    //     );
    // }

    // #[test]
    // fn test_cache_delete_range_not_sorted() {
    //     let v0 = Value::FloatValue(FloatValue::new(1, 1.0));
    //     let v1 = Value::FloatValue(FloatValue::new(3, 3.0));
    //     let v2 = Value::FloatValue(FloatValue::new(2, 2.0));

    //     let values_size = v0.size() + v1.size() + v2.size();
    //     let values = Values(vec![v0, v1, v2]);

    //     let c = Cache::new(3 * values_size as u64);
    //     c.write_multi(HashMap::from([("foo".to_string(), values.clone().into())]))
    //         .unwrap();

    //     let (exp, keys) = (vec!["foo".as_bytes().to_vec()], c.keys());
    //     assert_eq!(
    //         keys, exp,
    //         "cache keys incorrect after 2 writes, exp {:?}, got {:?}",
    //         exp, keys
    //     );

    //     c.delete_range(&["foo".as_bytes()], 1, 3);

    //     let keys = c.keys();
    //     assert!(
    //         keys.is_empty(),
    //         "cache size incorrect after 2 writes, exp empty, got {:?}",
    //         keys
    //     );

    //     let (got, exp) = (c.size(), 0);
    //     assert_eq!(
    //         exp, got,
    //         "cache size incorrect after 2 writes, exp {}, got {}",
    //         exp, got
    //     );

    //     let values = c.values("foo".as_bytes());
    //     assert!(
    //         values.is_empty(),
    //         "cache values mismatch: exp empty, got {:?}",
    //         values
    //     );
    // }

    // #[test]
    // fn test_cache_delete() {
    //     let v0 = Value::FloatValue(FloatValue::new(1, 1.0));
    //     let v1 = Value::FloatValue(FloatValue::new(2, 2.0));
    //     let v2 = Value::FloatValue(FloatValue::new(3, 3.0));

    //     let values_size = v0.size() + v1.size() + v2.size();
    //     let values = Values(vec![v0, v1, v2]);

    //     let c = Cache::new(30 * values_size as u64);
    //     c.write_multi(HashMap::from([
    //         ("foo".to_string(), values.clone().into()),
    //         ("bar".to_string(), values.clone().into()),
    //     ]))
    //     .unwrap();

    //     let n = c.size();
    //     assert_eq!(
    //         n,
    //         (2 * values_size + 6) as u64,
    //         "cache size incorrect after 2 writes, exp {}, got {}",
    //         2 * values_size,
    //         n
    //     );

    //     let (exp, keys) = (
    //         vec!["bar".as_bytes().to_vec(), "foo".as_bytes().to_vec()],
    //         c.keys(),
    //     );
    //     assert_eq!(
    //         keys, exp,
    //         "cache keys incorrect after 2 writes, exp {:?}, got {:?}",
    //         exp, keys
    //     );

    //     c.delete(&["bar".as_bytes()]);

    //     let (exp, keys) = (vec!["foo".as_bytes().to_vec()], c.keys());
    //     assert_eq!(
    //         keys, exp,
    //         "cache keys incorrect after 2 writes, exp {:?}, got {:?}",
    //         exp, keys
    //     );

    //     let (got, exp) = (c.size(), values_size as u64 + 3);
    //     assert_eq!(
    //         exp, got,
    //         "cache size incorrect after 2 writes, exp {}, got {}",
    //         exp, got
    //     );

    //     let values = c.values("bar".as_bytes());
    //     assert!(
    //         values.is_empty(),
    //         "cache values mismatch: exp empty, got {:?}",
    //         values
    //     );

    //     let (exp, got) = (3, c.values("foo".as_bytes()).len() as u64);
    //     assert_eq!(exp, got, "cache values mismatch: exp {}, got {}", exp, got);
    // }

    // #[test]
    // fn test_cache_delete_non_existent() {
    //     let c = Cache::new(1024);

    //     c.delete(&["bar".as_bytes()]);

    //     let (got, exp) = (c.size(), 0);
    //     assert_eq!(exp, got, "cache size incorrect exp {}, got {}", exp, got);
    // }

    // //////////////////////////////////////////////////////////////////////////////////
    // //////////////////////////////////////////////////////////////////////////////////
    // //////////////////////////////////////////////////////////////////////////////////

    // This tests writing two batches to the same series.  The first batch
    // is sorted.  The second batch is also sorted but contains duplicates.
    #[test]
    fn test_cache_write_multi_duplicates() {
        let v0 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 2,
            max: 20,
            generation: 1,
            seq: 1,
            offset: 2,
            size: 20,
        };
        let v1 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 3,
            max: 30,
            generation: 1,
            seq: 1,
            offset: 3,
            size: 30,
        };
        let values0 = VLogIndexes(vec![v0.clone(), v1.clone()]);

        let v3 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 4,
            max: 40,
            generation: 1,
            seq: 1,
            offset: 4,
            size: 40,
        };
        let v4 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 5,
            max: 50,
            generation: 1,
            seq: 1,
            offset: 5,
            size: 50,
        };
        let v5 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 5,
            max: 50,
            generation: 1,
            seq: 1,
            offset: 6,
            size: 60,
        };
        let values1 = VLogIndexes(vec![v3.clone(), v4.clone(), v5.clone()]);

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

        let exp_asc_values = VLogIndexes(vec![v0, v1, v3, v5]);
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
    fn test_cache_values() {
        let v0 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 1,
            max: 10,
            generation: 1,
            seq: 1,
            offset: 1,
            size: 10,
        };
        let v1 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 2,
            max: 20,
            generation: 1,
            seq: 1,
            offset: 2,
            size: 20,
        };
        let v2 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 3,
            max: 30,
            generation: 1,
            seq: 1,
            offset: 3,
            size: 30,
        };
        let v3 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 1,
            max: 10,
            generation: 1,
            seq: 1,
            offset: 2,
            size: 21,
        };
        let v4 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 4,
            max: 40,
            generation: 1,
            seq: 1,
            offset: 4,
            size: 40,
        };

        let c = Cache::new(512);

        let deduped = c.values("no such key".as_bytes());
        assert!(deduped.is_empty(), "expected empty, got {:?}", deduped);

        c.write(
            "foo".as_bytes(),
            vec![v0.clone(), v1.clone(), v2.clone(), v3.clone()],
        )
        .unwrap();
        c.write("foo".as_bytes(), vec![v4.clone()]).unwrap();

        let exp_asc_values = VLogIndexes(vec![v3, v1, v2, v4]);
        let (exp, got) = (exp_asc_values, c.values("foo".as_bytes()));
        assert_eq!(
            exp, got,
            "deduped ascending values for foo incorrect, exp: {:?}, got {:?}",
            exp, deduped
        );
    }

    #[test]
    fn test_cache_snapshot() {
        let v0 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 2,
            max: 20,
            generation: 1,
            seq: 1,
            offset: 0,
            size: 0,
        };
        let v1 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 3,
            max: 30,
            generation: 1,
            seq: 1,
            offset: 2,
            size: 20,
        };
        let v2 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 4,
            max: 40,
            generation: 1,
            seq: 1,
            offset: 3,
            size: 30,
        };
        let v3 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 5,
            max: 50,
            generation: 1,
            seq: 1,
            offset: 4,
            size: 40,
        };
        let v4 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 6,
            max: 60,
            generation: 1,
            seq: 1,
            offset: 5,
            size: 50,
        };
        let v5 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 1,
            max: 10,
            generation: 1,
            seq: 1,
            offset: 5,
            size: 50,
        };
        let v6 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 7,
            max: 70,
            generation: 1,
            seq: 1,
            offset: 5,
            size: 50,
        };
        let v7 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 2,
            max: 20,
            generation: 1,
            seq: 1,
            offset: 5,
            size: 50,
        };

        let c = Cache::new(512);
        c.write(
            "foo".as_bytes(),
            vec![v0.clone(), v1.clone(), v2.clone(), v3.clone()],
        )
        .unwrap();

        // Grab snapshot, and ensure it's as expected.
        let snapshot = c.snapshot().unwrap();

        let exp_values = VLogIndexes(vec![v0.clone(), v1.clone(), v2.clone(), v3.clone()]);
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
        let exp_values = VLogIndexes(vec![
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
        let exp_values = VLogIndexes(vec![
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

        let exp_values = VLogIndexes(vec![v5.clone(), v4.clone()]);
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

        let exp_values = VLogIndexes(vec![v5.clone(), v7.clone(), v4.clone(), v6.clone()]);
        let deduped = c.values("foo".as_bytes());
        assert_eq!(
            exp_values, deduped,
            "post-snapshot out-of-order write values for foo incorrect, exp: {:?}, got {:?}",
            exp_values, deduped
        );
    }

    #[test]
    fn test_cache_snapshot_stats() {
        let limit = VLogIndex::SIZE;
        let c = Cache::new(limit as u64);

        let values = HashMap::from([(
            "foo".to_string(),
            vec![VLogIndex {
                aggregate: None,
                count: 1,
                typ: 1,
                min: 1,
                max: 10,
                generation: 1,
                seq: 1,
                offset: 1,
                size: 10,
            }],
        )]);
        c.write_multi(values).unwrap();

        let (got, exp) = (
            c.stats.mem_size_bytes.load(Ordering::Relaxed),
            (VLogIndex::SIZE + 3) as i64,
        );
        assert_eq!(got, exp, "got {}, exp: {}", got, exp);

        let _ = c.snapshot().unwrap();

        // Store size should have been reset.
        let (got, exp) = (c.size(), (VLogIndex::SIZE + 3) as u64);
        assert_eq!(got, exp, "got {}, exp: {}", got, exp);

        // Cached bytes should have been increased.
        let (got, exp) = (
            c.stats.cached_bytes.load(Ordering::Relaxed),
            (VLogIndex::SIZE + 3) as i64,
        );
        assert_eq!(got, exp, "got {}, exp: {}", got, exp);

        let (got, exp) = (
            c.stats.mem_size_bytes.load(Ordering::Relaxed),
            (VLogIndex::SIZE + 3) as i64,
        );
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
        let v0 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 1,
            max: 10,
            generation: 1,
            seq: 1,
            offset: 1,
            size: 10,
        };
        let v1 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 2,
            max: 20,
            generation: 1,
            seq: 1,
            offset: 2,
            size: 20,
        };

        let c = Cache::new(VLogIndex::SIZE as u64);

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

        let exp_asc_values = VLogIndexes(vec![v1]);
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
                let min = (i + j) + rand::thread_rng().gen_range(0..10) as i64;
                values.insert(
                    format!("cpu{}", i),
                    vec![VLogIndex {
                        aggregate: None,
                        count: 1,
                        typ: 1,
                        min,
                        max: min + 10,
                        generation: i,
                        seq: i,
                        offset: i,
                        size: i,
                    }],
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

    #[test]
    fn test_vlog_index_cache_loader_legacy_write_multi_replay() {
        let dir = Arc::new(must_temp_tsm_dir().into_path());
        let wal = VLogIndexWAL::new(
            Arc::clone(&dir),
            VLOG_INDEX_WAL_DEFAULT_SEGMENT_SIZE,
            Duration::ZERO,
        )
        .unwrap();
        let key = "cpu,host=A#!~#field1".to_string();
        let index = test_vlog_index(1, 1, 100);
        let values = HashMap::from([(key.clone(), vec![index.clone()])]);

        block_on_global(wal.write_multi(values)).unwrap();
        drop(wal);

        let cache = Cache::new(0);
        let files = must_vlog_index_wal_files(&dir);
        CacheLoader::new(files).load(&cache).unwrap();

        assert_eq!(
            cache.values_by_gen_and_seq(key.as_bytes(), index.generation, index.seq),
            VLogIndexes(vec![index])
        );
    }

    #[test]
    fn test_vlog_index_cache_loader_legacy_delete_multi_replay() {
        let dir = Arc::new(must_temp_tsm_dir().into_path());
        let wal = VLogIndexWAL::new(
            Arc::clone(&dir),
            VLOG_INDEX_WAL_DEFAULT_SEGMENT_SIZE,
            Duration::ZERO,
        )
        .unwrap();
        let key = "cpu,host=A#!~#field1".to_string();
        let deleted_index = test_vlog_index(1, 1, 100);
        let retained_index = test_vlog_index(2, 1, 200);
        let deleted = HashMap::from([(key.clone(), vec![deleted_index.clone()])]);
        let retained = HashMap::from([(key.clone(), vec![retained_index.clone()])]);

        block_on_global(wal.write_multi(deleted.clone())).unwrap();
        block_on_global(wal.write_multi(retained)).unwrap();
        block_on_global(wal.delete_multi(deleted)).unwrap();
        drop(wal);

        let cache = Cache::new(0);
        let files = must_vlog_index_wal_files(&dir);
        CacheLoader::new(files).load(&cache).unwrap();

        assert_eq!(
            cache.values2(key.as_bytes()),
            VLogIndexes(vec![retained_index])
        );
    }

    #[test]
    fn test_vlog_index_cache_loader_replace_multi_replay() {
        let dir = Arc::new(must_temp_tsm_dir().into_path());
        let wal = VLogIndexWAL::new(
            Arc::clone(&dir),
            VLOG_INDEX_WAL_DEFAULT_SEGMENT_SIZE,
            Duration::ZERO,
        )
        .unwrap();
        let key = "cpu,host=A#!~#field1".to_string();
        let old_index = test_vlog_index(1, 1, 100);
        let new_index = test_vlog_index(2, 1, 200);
        let old_indexes = HashMap::from([(key.clone(), vec![old_index.clone()])]);
        let new_indexes = HashMap::from([(key.clone(), vec![new_index.clone()])]);

        block_on_global(wal.replace_multi(old_indexes, new_indexes)).unwrap();
        drop(wal);

        let cache = Cache::new(0);
        let files = must_vlog_index_wal_files(&dir);
        CacheLoader::new(files).load(&cache).unwrap();

        assert_eq!(cache.values2(key.as_bytes()), VLogIndexes(vec![new_index]));
    }

    #[test]
    fn test_vlog_index_cache_loader_replace_multi_replay_no_old_new_duplicates() {
        let dir = Arc::new(must_temp_tsm_dir().into_path());
        let wal = VLogIndexWAL::new(
            Arc::clone(&dir),
            VLOG_INDEX_WAL_DEFAULT_SEGMENT_SIZE,
            Duration::ZERO,
        )
        .unwrap();
        let key = "cpu,host=A#!~#field1".to_string();
        let old_index = test_vlog_index(1, 1, 100);
        let new_index = test_vlog_index(2, 1, 200);
        let old_indexes = HashMap::from([(key.clone(), vec![old_index.clone()])]);
        let new_indexes = HashMap::from([(key.clone(), vec![new_index.clone()])]);

        block_on_global(wal.write_multi(old_indexes.clone())).unwrap();
        block_on_global(wal.replace_multi(old_indexes, new_indexes)).unwrap();
        drop(wal);

        let cache = Cache::new(0);
        let files = must_vlog_index_wal_files(&dir);
        CacheLoader::new(files).load(&cache).unwrap();

        assert_eq!(cache.values2(key.as_bytes()), VLogIndexes(vec![new_index]));
    }

    #[test]
    fn test_vlog_index_cache_loader_corrupt_replace_multi_replay_keeps_valid_prefix() {
        let dir = Arc::new(must_temp_tsm_dir().into_path());
        let wal = VLogIndexWAL::new(
            Arc::clone(&dir),
            VLOG_INDEX_WAL_DEFAULT_SEGMENT_SIZE,
            Duration::ZERO,
        )
        .unwrap();
        let key = "cpu,host=A#!~#field1".to_string();
        let old_index = test_vlog_index(1, 1, 100);
        let old_indexes = HashMap::from([(key.clone(), vec![old_index.clone()])]);

        block_on_global(wal.write_multi(old_indexes)).unwrap();
        drop(wal);

        let files = must_vlog_index_wal_files(&dir);
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&files[0])
            .unwrap();
        f.write_all(&[
            vlog_index_wal::REPLACE_MULTI_WAL_ENTRY_TYPE,
            0,
            0,
            0,
            5,
            1,
            0,
        ])
        .unwrap();
        f.sync_all().unwrap();

        let cache = Cache::new(0);
        CacheLoader::new(files).load(&cache).unwrap();

        assert_eq!(
            cache.values_by_gen_and_seq(key.as_bytes(), old_index.generation, old_index.seq),
            VLogIndexes(vec![old_index])
        );
    }

    // use super::CacheLoader;
    // // Ensure the CacheLoader can correctly load from a single segment, even if it's corrupted.
    // #[test]
    // fn test_cache_loader_single() {
    //     // Create a WAL segment.
    //     let dir = must_temp_tsm_dir();
    //     let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();
    //     let sync_waiters = Arc::new(crossbeam_channel::bounded(1024));
    //     let mut w = WalSegmentWriter::new(
    //         Arc::new(fpath.clone()),
    //         wal::DEFAULT_SEGMENT_SIZE,
    //         0,
    //         BufWriter::new(f),
    //         sync_waiters,
    //         0,
    //     );

    //     let p1 = Value::FloatValue(FloatValue::new(1, 1.1));
    //     let p2 = Value::IntegerValue(IntegerValue::new(1, 1i64));
    //     let p3 = Value::BoolValue(BoolValue::new(1, true));

    //     let values: HashMap<String, Vec<Value>> = HashMap::from([
    //         ("foo".to_string(), vec![p1.clone()]),
    //         ("bar".to_string(), vec![p2.clone()]),
    //         ("baz".to_string(), vec![p3.clone()]),
    //     ]);

    //     let entry = WriteWalEntry {
    //         values: values.clone(),
    //         size: 0, // size will be updated in marshal_size
    //     };

    //     let stats = Arc::new(WalStatistics::default());
    //     let (entry_type, compressed) = must_marshal_entry(Entry::WriteWalEntry(entry));
    //     assert!(w
    //         .write_entry(entry_type, compressed.as_slice(), Arc::clone(&stats))
    //         .is_ok());
    //     w.flush().unwrap();

    //     // Load the cache using the segment.
    //     let cache = Cache::new(1024);
    //     let loader = CacheLoader::new(vec![fpath.to_str().unwrap().to_string()]);
    //     loader.load(&cache).unwrap();

    //     // Check the cache.
    //     let (exp, got) = (Values(vec![p1.clone()]), cache.values("foo".as_bytes()));
    //     assert_eq!(
    //         exp, got,
    //         "cache key foo not as expected, got {:?}, exp {:?}",
    //         got, exp
    //     );

    //     let (exp, got) = (Values(vec![p2.clone()]), cache.values("bar".as_bytes()));
    //     assert_eq!(
    //         exp, got,
    //         "cache key bar not as expected, got {:?}, exp {:?}",
    //         exp, got
    //     );

    //     let (exp, got) = (Values(vec![p3.clone()]), cache.values("baz".as_bytes()));
    //     assert_eq!(
    //         exp, got,
    //         "cache key baz not as expected, got {:?}, exp {:?}",
    //         exp, got
    //     );

    //     // Corrupt the WAL segment.
    //     w.writer.write(&[1, 4, 0, 0, 0]).unwrap();

    //     // Reload the cache using the segment.
    //     let cache = Cache::new(1024);
    //     let loader = CacheLoader::new(vec![fpath.to_str().unwrap().to_string()]);
    //     loader.load(&cache).unwrap();

    //     // Check the cache.
    //     let (exp, got) = (Values(vec![p1.clone()]), cache.values("foo".as_bytes()));
    //     assert_eq!(
    //         exp, got,
    //         "cache key foo not as expected, got {:?}, exp {:?}",
    //         got, exp
    //     );

    //     let (exp, got) = (Values(vec![p2.clone()]), cache.values("bar".as_bytes()));
    //     assert_eq!(
    //         exp, got,
    //         "cache key bar not as expected, got {:?}, exp {:?}",
    //         exp, got
    //     );

    //     let (exp, got) = (Values(vec![p3.clone()]), cache.values("baz".as_bytes()));
    //     assert_eq!(
    //         exp, got,
    //         "cache key baz not as expected, got {:?}, exp {:?}",
    //         exp, got
    //     );

    //     std::fs::remove_file(fpath).unwrap();
    // }

    // // Ensure the CacheLoader can correctly load from two segments, even if one is corrupted.
    // #[test]
    // fn test_cache_loader_double() {
    //     // Create a WAL segment.
    //     let dir = must_temp_tsm_dir();
    //     let (f1, fpath1) = must_temp_tsm_file(dir.path()).keep().unwrap();
    //     let (f2, fpath2) = must_temp_tsm_file(dir.path()).keep().unwrap();
    //     let mut w1 = WalSegmentWriter::new(
    //         Arc::new(fpath1.clone()),
    //         wal::DEFAULT_SEGMENT_SIZE,
    //         0,
    //         BufWriter::new(f1),
    //         Arc::new(crossbeam_channel::bounded(1024)),
    //         0,
    //     );
    //     let mut w2 = WalSegmentWriter::new(
    //         Arc::new(fpath2.clone()),
    //         wal::DEFAULT_SEGMENT_SIZE,
    //         0,
    //         BufWriter::new(f2),
    //         Arc::new(crossbeam_channel::bounded(1024)),
    //         0,
    //     );

    //     let p1 = Value::FloatValue(FloatValue::new(1, 1.1));
    //     let p2 = Value::IntegerValue(IntegerValue::new(1, 1i64));
    //     let p3 = Value::BoolValue(BoolValue::new(1, true));
    //     let p4 = Value::StringValue(StringValue::new(1, "string".to_string()));

    //     // Write first segment
    //     let values: HashMap<String, Vec<Value>> = HashMap::from([
    //         ("foo".to_string(), vec![p1.clone()]),
    //         ("bar".to_string(), vec![p2.clone()]),
    //     ]);

    //     let entry = WriteWalEntry {
    //         values: values.clone(),
    //         size: 0, // size will be updated in marshal_size
    //     };

    //     let stats = Arc::new(WalStatistics::default());
    //     let (entry_type, compressed) = must_marshal_entry(Entry::WriteWalEntry(entry));
    //     assert!(w1
    //         .write_entry(entry_type, compressed.as_slice(), Arc::clone(&stats))
    //         .is_ok());
    //     w1.flush().unwrap();

    //     // Write second segment
    //     let values: HashMap<String, Vec<Value>> = HashMap::from([
    //         ("baz".to_string(), vec![p3.clone()]),
    //         ("qux".to_string(), vec![p4.clone()]),
    //     ]);

    //     let entry = WriteWalEntry {
    //         values: values.clone(),
    //         size: 0, // size will be updated in marshal_size
    //     };

    //     let stats = Arc::new(WalStatistics::default());
    //     let (entry_type, compressed) = must_marshal_entry(Entry::WriteWalEntry(entry));
    //     assert!(w2
    //         .write_entry(entry_type, compressed.as_slice(), Arc::clone(&stats))
    //         .is_ok());
    //     w2.flush().unwrap();

    //     // Corrupt the first WAL segment.
    //     w1.writer.write(&[1, 4, 0, 0, 0]).unwrap();

    //     // Reload the cache using the segment.
    //     let cache = Cache::new(1024);
    //     let loader = CacheLoader::new(vec![
    //         fpath1.to_str().unwrap().to_string(),
    //         fpath2.to_str().unwrap().to_string(),
    //     ]);
    //     loader.load(&cache).unwrap();

    //     // Check the cache.
    //     let (exp, got) = (Values(vec![p1.clone()]), cache.values("foo".as_bytes()));
    //     assert_eq!(
    //         exp, got,
    //         "cache key foo not as expected, got {:?}, exp {:?}",
    //         got, exp
    //     );

    //     let (exp, got) = (Values(vec![p2.clone()]), cache.values("bar".as_bytes()));
    //     assert_eq!(
    //         exp, got,
    //         "cache key bar not as expected, got {:?}, exp {:?}",
    //         exp, got
    //     );

    //     let (exp, got) = (Values(vec![p3.clone()]), cache.values("baz".as_bytes()));
    //     assert_eq!(
    //         exp, got,
    //         "cache key baz not as expected, got {:?}, exp {:?}",
    //         exp, got
    //     );

    //     let (exp, got) = (Values(vec![p4.clone()]), cache.values("qux".as_bytes()));
    //     assert_eq!(
    //         exp, got,
    //         "cache key baz not as expected, got {:?}, exp {:?}",
    //         exp, got
    //     );

    //     std::fs::remove_file(fpath1).unwrap();
    //     std::fs::remove_file(fpath2).unwrap();
    // }

    // // Ensure the CacheLoader can load deleted series
    // #[test]
    // fn test_cache_loader_deleted() {
    //     // Create a WAL segment.
    //     let dir = must_temp_tsm_dir();
    //     let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();
    //     let mut w = WalSegmentWriter::new(
    //         Arc::new(fpath.clone()),
    //         wal::DEFAULT_SEGMENT_SIZE,
    //         0,
    //         BufWriter::new(f),
    //         Arc::new(crossbeam_channel::bounded(1024)),
    //         0,
    //     );

    //     let p1 = Value::FloatValue(FloatValue::new(1, 1.0));
    //     let p2 = Value::FloatValue(FloatValue::new(2, 2.0));
    //     let p3 = Value::FloatValue(FloatValue::new(3, 3.0));

    //     let values: HashMap<String, Vec<Value>> =
    //         HashMap::from([("foo".to_string(), vec![p1.clone(), p2.clone(), p3.clone()])]);

    //     let entry = WriteWalEntry {
    //         values: values.clone(),
    //         size: 0, // size will be updated in marshal_size
    //     };

    //     let stats = Arc::new(WalStatistics::default());
    //     let (entry_type, compressed) = must_marshal_entry(Entry::WriteWalEntry(entry));
    //     assert!(w
    //         .write_entry(entry_type, compressed.as_slice(), Arc::clone(&stats))
    //         .is_ok());
    //     w.flush().unwrap();

    //     let dentry = DeleteRangeWalEntry {
    //         keys: vec!["foo".as_bytes().to_vec()],
    //         min: 2,
    //         max: 3,
    //         size: 0,
    //     };

    //     let (entry_type, compressed) = must_marshal_entry(Entry::DeleteRangeWalEntry(dentry));
    //     assert!(w
    //         .write_entry(entry_type, compressed.as_slice(), Arc::clone(&stats))
    //         .is_ok());
    //     w.flush().unwrap();

    //     // Load the cache using the segment.
    //     let cache = Cache::new(1024);
    //     let loader = CacheLoader::new(vec![fpath.to_str().unwrap().to_string()]);
    //     loader.load(&cache).unwrap();

    //     // Check the cache.
    //     let (exp, got) = (Values(vec![p1.clone()]), cache.values("foo".as_bytes()));
    //     assert_eq!(
    //         exp, got,
    //         "cache key foo not as expected, got {:?}, exp {:?}",
    //         got, exp
    //     );

    //     // Reload the cache using the segment.
    //     let cache = Cache::new(1024);
    //     let loader = CacheLoader::new(vec![fpath.to_str().unwrap().to_string()]);
    //     loader.load(&cache).unwrap();

    //     // Check the cache.
    //     let (exp, got) = (Values(vec![p1.clone()]), cache.values("foo".as_bytes()));
    //     assert_eq!(
    //         exp, got,
    //         "cache key foo not as expected, got {:?}, exp {:?}",
    //         got, exp
    //     );

    //     std::fs::remove_file(fpath).unwrap();
    // }

    #[test]
    fn test_cache_split() {
        let v0 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 1,
            max: 10,
            generation: 1,
            seq: 1,
            offset: 1,
            size: 10,
        };
        let v1 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 2,
            max: 20,
            generation: 1,
            seq: 1,
            offset: 2,
            size: 20,
        };
        let v2 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 3,
            max: 3,
            generation: 1,
            seq: 1,
            offset: 3,
            size: 30,
        };

        let values_size = 3 * VLogIndex::SIZE;
        let values = VLogIndexes(vec![v0, v1, v2]);

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
        let mut values = VLogIndexes(Vec::with_capacity(1000));
        let mut timestamps = Vec::with_capacity(1000);
        let mut series = Vec::with_capacity(100);
        for _ in 0..1000 {
            timestamps.push(rand::thread_rng().gen_range(0..1000) as i64);
        }

        for i in 0..1000 {
            values.push(VLogIndex {
                aggregate: None,
                count: 1,
                typ: 1,
                min: timestamps[i],
                max: timestamps[i] + 10,
                generation: i as i64,
                seq: i as i64,
                offset: i as i64,
                size: i as i64,
            })
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
        let mut values = VLogIndexes(Vec::with_capacity(1000));
        let mut timestamps = Vec::with_capacity(1000);
        let mut series = Vec::with_capacity(100);
        for _ in 0..1000 {
            timestamps.push(rand::thread_rng().gen_range(0..1000) as i64);
        }

        for i in 0..1000 {
            values.push(VLogIndex {
                aggregate: None,
                count: 1,
                typ: 1,
                min: timestamps[i],
                max: timestamps[i] + 10,
                generation: i as i64,
                seq: i as i64,
                offset: i as i64,
                size: i as i64,
            })
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
                panic!("{:?}", e);
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

    use crate::storage::tsm1::{
        vlog::{
            VLogIndex,
            vlog_index_ring::VLogIndexes,
            vlog_index_wal::{
                self, DEFAULT_SEGMENT_SIZE as VLOG_INDEX_WAL_DEFAULT_SEGMENT_SIZE,
                WAL as VLogIndexWAL,
            },
        },
        wal::{self, *},
    };

    fn test_vlog_index(generation: i64, seq: i64, offset: i64) -> VLogIndex {
        VLogIndex {
            aggregate: None,
            count: 10,
            typ: 1,
            min: 1,
            max: 10,
            generation,
            seq,
            offset,
            size: 40,
        }
    }

    fn must_vlog_index_wal_files(dir: &Path) -> Vec<String> {
        let mut files: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.is_file() && path.extension() == Some("wal".as_ref()))
            .map(|path| path.into_os_string().into_string().unwrap())
            .collect();
        files.sort();
        files
    }

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
