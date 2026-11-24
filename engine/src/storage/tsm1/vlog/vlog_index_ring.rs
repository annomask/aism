//! ring is a structure that maps series keys to entries.
use std::{
    collections::HashMap,
    fmt::format,
    ops::{Deref, DerefMut},
    sync::Arc,
    time::Duration,
};

use crate::common::{hll::hash64, to_str};
use crate::runtime::execution::{
    ExecutorRef, RuntimeExecutorExt, accepted_task_guard, legacy_global_executor,
};
use parking_lot::RwLock;
use snafu::{ResultExt, Snafu, ensure};

use super::{
    VLogIndex,
    vlog_index_cache::{Error, Result},
};

// use super::cache::{VLogIndexEntry, Error, Result};
// use crate::storage::tsm1::data_encoding::encoding_values::Values;

/// INDEX_PARTITIONS is the number of partitions we used in the ring's continuum. It
/// basically defines the maximum number of partitions you can have in the ring.
/// If a smaller number of partitions are chosen when creating a ring, then
/// they're evenly spread across this many partitions in the ring.
pub const INDEX_PARTITIONS: i32 = 16;

// /// Error type for Index cache.
// #[derive(Debug, Snafu)]
// pub enum Error {
//     /// IO error.
//     #[snafu(display("open wal file failed: {}", reason))]
//     IOError { reason: std::io::Error },

//     // /// Load entries from wal failed.
//     // #[snafu(display("{}", source))]
//     // LoadIndexWALErr { source: wal::Error },
//     /// ErrSnapshotInProgress is returned if a snapshot is attempted while one is already running.
//     #[snafu(display("snapshot in progress"))]
//     ErrIndexSnapshotInProgress,

//     /// ErrCacheMemorySizeLimitExceeded returns an error indicating an operation
//     /// could not be completed due to exceeding the cache-max-memory-size setting.
//     #[snafu(display("cache-max-memory-size exceeded: ({}/{})", n, limit))]
//     ErrIndexCacheMemorySizeLimitExceeded { n: u64, limit: u64 },

//     /// String error.
//     #[snafu(display("{}", reason))]
//     StringErr { reason: String },
// }

// /// Result type for Cache.
// pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Ring is a structure that maps series keys to entries.
///
/// ring is implemented as a crude hash ring, in so much that you can have
/// variable numbers of members in the ring, and the appropriate member for a
/// given series key can always consistently be found. Unlike a true hash ring
/// though, this ring is not resizeable—there must be at most 16 members in the
/// ring, and the number of members must always be a power of 2.
///
/// ring works as follows: Each member of the ring contains a single store, which
/// contains a map of series keys to entries. A ring always has 16 partitions,
/// and a member takes up one or more of these partitions (depending on how many
/// members are specified to be in the ring)
///
/// To determine the partition that a series key should be added to, the series
/// key is hashed and the first 8 bits are used as an index to the ring.
pub struct Ring {
    executor: ExecutorRef,
    // The unique set of partitions in the ring.
    partitions: Vec<Arc<Partition>>,
}

impl Ring {
    pub fn new(n: i32) -> Result<Self> {
        Self::new_with_executor(n, legacy_global_executor())
    }

    pub fn new_with_executor(n: i32, executor: ExecutorRef) -> Result<Self> {
        if n <= 0 || n > INDEX_PARTITIONS {
            return Err(Error::StringErr {
                reason: format!("invalid number of partitions: {}", n),
            });
        }

        if n & (n - 1) != 0 {
            return Err(Error::StringErr {
                reason: format!("index partitions {} is not a power of two", n),
            });
        }

        let mut r = Ring {
            executor: Arc::clone(&executor),
            partitions: Vec::with_capacity(n as usize),
        };

        // The trick here is to map N partitions to all points on the continuum,
        // such that the first eight bits of a given hash will map directly to one
        // of the N partitions.
        for _ in 0..n {
            r.partitions
                .push(Arc::new(Partition::new(Arc::clone(&executor))))
        }

        Ok(r)
    }

    /// Resets the ring so it can be reused. Before removing references to entries
    /// within each partition it gathers sizing information to provide hints when
    /// reallocating entries in partition maps.
    ///
    /// reset is not safe for use by multiple threads.
    pub fn reset(&self) {
        for p in self.partitions.iter() {
            p.reset();
        }
    }

    /// Retrieves the hash ring partition associated with the provided key.
    pub fn get_partition(&self, key: &[u8]) -> &Arc<Partition> {
        &self.partitions[(hash64(key) % (self.partitions.len() as u64)) as usize]
    }

    /// Returns the entry for the given key.
    /// entry is safe for use by multiple threads.
    pub fn entry(&self, key: &[u8]) -> Option<Arc<VLogIndexEntry>> {
        self.get_partition(key).entry(key)
    }

    /// Writes values to the entry in the ring's partition associated with key.
    /// If no entry exists for the key then one will be created.
    /// write is safe for use by multiple threads.
    pub fn write(&self, key: &str, values: Vec<VLogIndex>) -> (bool, Result<(), Error>) {
        self.get_partition(key.as_bytes()).write(key, values)
    }

    /// Remove the entry for the given key.
    /// remove is safe for use by multiple threads.
    pub fn remove(&self, key: &[u8]) {
        self.get_partition(key).remove(key)
    }

    /// Returns all the keys from all partitions in the hash ring. The returned
    /// keys will be in order if sorted is true.
    pub fn keys(&self, sorted: bool) -> Vec<Vec<u8>> {
        let mut keys = vec![];
        for p in self.partitions.iter() {
            keys.append(&mut p.keys());
        }

        if sorted {
            keys.sort();
        }

        keys
    }

    pub fn keys_by_gen_and_seq(
        &self,
        sorted: bool,
        generation: i64,
        seq: i64,
    ) -> Vec<(Vec<u8>, u8)> {
        let mut keys = vec![];
        for p in self.partitions.iter() {
            keys.append(&mut p.keys_by_gen_and_seq(generation, seq));
        }

        if sorted {
            keys.sort();
        }

        keys
    }

    /// Returns the number of values in the ring
    pub fn count(&self) -> usize {
        let mut n = 0;
        for p in self.partitions.iter() {
            n += p.count();
        }
        n
    }

    /// Applies the provided function to every entry in the ring under a read
    /// lock using a separate goroutine for each partition. The provided function
    /// will be called with each key and the corresponding entry. The first error
    /// encountered will be returned, if any. apply is safe for use by multiple
    /// threads.
    pub fn apply<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &Arc<VLogIndexEntry>) -> Result<()> + Clone + Send + 'static,
    {
        let (s, r) = crossbeam_channel::unbounded::<Option<Error>>();

        for p in self.partitions.iter() {
            let s = s.clone();
            let p = Arc::clone(p);
            let mut f = f.clone();
            let rejected_s = s.clone();
            let cancelled_s = s.clone();
            let (registration, completion) = accepted_task_guard(move || {
                let _ = cancelled_s.send(Some(Error::TaskTerminated));
            });
            match self.executor.spawn_blocking(move || {
                let store = p.store.read();
                let result = store.iter().find_map(|(key, entry)| {
                    if let Err(error) = f(key.as_bytes(), entry) {
                        Some(error)
                    } else {
                        None
                    }
                });
                completion.complete_with(|| {
                    let _ = s.send(result);
                });
            }) {
                Ok(_) => registration.accepted(),
                Err(source) => {
                    let _ = rejected_s.send(Some(Error::Execution { source }));
                    registration.rejected();
                }
            }
        }

        // Collect results.
        for _ in 0..self.partitions.len() {
            if let Ok(Some(e)) = r.recv() {
                return Err(e);
            }
        }

        Ok(())
    }

    /// apply_serial is similar to apply, but invokes f on each partition in the same thread.
    /// apply is safe for use by multiple threads.
    pub fn apply_serial<F, E>(&self, mut f: F) -> Result<(), E>
    where
        F: FnMut(Vec<u8>, &Arc<VLogIndexEntry>) -> Result<(), E>,
    {
        for p in self.partitions.iter() {
            let store = p.store.read();
            for (k, e) in store.iter() {
                if e.count() == 0 {
                    continue;
                }

                f(k.as_bytes().to_vec(), e)?;
            }
        }

        Ok(())
    }

    pub fn split(&self, n: i32) -> Vec<Ring> {
        let mut storers = Vec::<Ring>::with_capacity(n as usize);
        for _ in 0..n {
            storers.push(
                Ring::new_with_executor(self.partitions.len() as i32, Arc::clone(&self.executor))
                    .unwrap(),
            );
        }

        for (i, p) in self.partitions.iter().enumerate() {
            let r = &mut storers[i % (n as usize)];
            r.partitions[i] = Arc::clone(p);
        }

        storers
    }
}

/// Partition provides safe access to a map of series keys to entries.
pub struct Partition {
    executor: ExecutorRef,
    store: RwLock<HashMap<String, Arc<VLogIndexEntry>>>,
}

// impl PartialEq for Partition {
//     fn eq(&self, other: &Self) -> bool {
//         let a = self.store.read();
//         let b = other.store.read();

//         for (k, v) in a.iter() {
//             if let Some(bv) = b.get(k.as_str()) {
//                 if !Arc::ptr_eq(v, bv) {
//                     return false;
//                 }
//             } else {
//                 return false;
//             }
//         }
//         true
//     }
// }

impl Partition {
    fn new(executor: ExecutorRef) -> Self {
        Self {
            executor,
            store: RwLock::new(HashMap::new()),
        }
    }

    /// entry returns the partition's entry for the provided key.
    /// It's safe for use by multiple goroutines.
    fn entry(&self, key: &[u8]) -> Option<Arc<VLogIndexEntry>> {
        self.store.read().get(to_str!(key)).cloned()
    }

    /// write writes the values to the entry in the partition, creating the entry
    /// if it does not exist.
    /// write is safe for use by multiple goroutines.
    fn write(&self, key: &str, values: Vec<VLogIndex>) -> (bool, Result<()>) {
        let store = self.store.read();

        if let Some(e) = store.get(key) {
            // Hot path.
            return (false, e.add(values));
        }
        drop(store);

        let mut store = self.store.write();
        // Check again.
        if let Some(e) = store.get(key) {
            return (false, e.add(values));
        }

        // Create a new entry using a preallocated size if we have a hint available.
        store.insert(key.to_string(), Arc::new(VLogIndexEntry::new(values)));
        (true, Ok(()))
    }

    /// remove deletes the entry associated with the provided key.
    /// remove is safe for use by multiple goroutines.
    fn remove(&self, key: &[u8]) {
        let mut store = self.store.write();
        let _ = store.remove(to_str!(key));
    }

    /// keys returns an unsorted slice of the keys in the partition.
    fn keys(&self) -> Vec<Vec<u8>> {
        let store = self.store.read();
        let mut keys = vec![];

        for (k, v) in store.iter() {
            if v.count() == 0 {
                continue;
            }
            keys.push(k.as_bytes().to_vec());
        }
        keys
    }

    fn keys_by_gen_and_seq(&self, generation: i64, seq: i64) -> Vec<(Vec<u8>, u8)> {
        let store = self.store.read();
        let mut keys = vec![];

        for (k, v) in store.iter() {
            let types = {
                let indexes = v.values.read();
                let mut types = Vec::new();
                for idx in indexes.iter() {
                    if idx.generation == generation && idx.seq == seq && !types.contains(&idx.typ) {
                        types.push(idx.typ);
                    }
                }
                types
            };
            // A reader loads all matching block locations for each key. Returning
            // the key once per block would retain a quadratic number of locations.
            for typ in types {
                keys.push((k.as_bytes().to_vec(), typ));
            }
        }
        keys
    }

    //// reset resets the partition by reinitialising the store. reset returns hints
    /// about sizes that the entries within the store could be reallocated with.
    fn reset(&self) {
        let sz = {
            let store = self.store.read();
            store.len()
        };
        // Drop store to a background thread.
        let new_store = HashMap::<String, Arc<VLogIndexEntry>>::with_capacity(sz);
        let mut store = self.store.write();

        let defer_store = std::mem::replace(&mut *store, new_store);
        if let Err(error) = self.executor.spawn_blocking(move || {
            drop(defer_store);
        }) {
            tracing::debug!(
                target: "reefsdb::tsdb::vlog_index_cache",
                error = %error,
                "deferred index ring store drop scheduling rejected"
            );
        }
    }

    fn count(&self) -> usize {
        let mut n = 0;
        let store = self.store.read();
        for (_k, v) in store.iter() {
            n += v.count();
        }
        n
    }
}

pub struct VLogIndexEntry {
    pub values: RwLock<VLogIndexes>, // All stored values.
    aggregate_sources: RwLock<super::aggregate_record::AggregateSources>,
}

impl VLogIndexEntry {
    /// Returns a new instance of entry with the given values.  If the values are not valid, an error is returned.
    pub fn new(values: Vec<VLogIndex>) -> Self {
        let mut sources = Default::default();
        for index in &values { super::aggregate_record::added(&mut sources, index); }
        Self {
            values: RwLock::new(VLogIndexes(values)),
            aggregate_sources: RwLock::new(sources),
        }
    }

    /// Adds the given values to the entry.
    pub fn add(&self, mut values: Vec<VLogIndex>) -> Result<()> {
        // entry currently has no values, so add the new ones and we're done.
        let mut vs = self.values.write();
        {
            let mut sources = self.aggregate_sources.write();
            for index in &values { super::aggregate_record::added(&mut sources, index); }
        }
        if vs.is_empty() {
            *vs = VLogIndexes(values);
            return Ok(());
        }

        // Append the new values to the existing ones...
        vs.append(&mut values);
        Ok(())
    }

    /// deduplicate sorts and orders the entry's values. If values are already deduped and sorted,
    /// the function does no work and simply returns.
    pub fn deduplicate(&self) {
        let mut values = self.values.write();
        if values.len() <= 1 {
            return;
        }

        let before = (!self.aggregate_sources.read().is_empty()).then(|| values.clone());
        values.deduplicate();
        if let Some(before) = before { self.remove_aggregate_members(&before, &values); }
    }

    pub(crate) fn aggregate_sources(&self) -> super::aggregate_record::AggregateSources {
        let _values = self.values.read();
        self.aggregate_sources.read().clone()
    }

    fn remove_aggregate_members(&self, before: &[VLogIndex], after: &[VLogIndex]) {
        let mut sources = self.aggregate_sources.write();
        let mut remaining = after.to_vec();
        for index in before {
            if let Some(i) = remaining.iter().position(|v| v == index && v.aggregate.as_ref().map(|a| a.file) == index.aggregate.as_ref().map(|a| a.file)) { remaining.remove(i); }
            else { super::aggregate_record::removed(&mut sources, index); }
        }
    }

    /// Returns a copy of filtered values by generation and sequence.
    pub fn filter_by_gen_and_seq(&self, generation: i64, seq: i64) -> VLogIndexes {
        let values = self.values.read();
        values.filter_by_gen_and_seq(generation, seq)
    }

    // FIXME: optimize me!!!
    /// Removes all indexes in the
    pub fn delete_vlog_index(&self, indexes: &[VLogIndex]) {
        let mut values = self.values.write();
        let mut sources = self.aggregate_sources.write();
        values.retain(|v| {
            if indexes.contains(v) { super::aggregate_record::removed(&mut sources, v); false }
            else { true }
        });
    }

    /// Returns the number of values in this entry.
    pub fn count(&self) -> usize {
        let values = self.values.read();
        values.len()
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
    pub fn size(&self) -> usize {
        let values = self.values.read();
        values.size()
    }
}

#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub struct VLogIndexes(pub Vec<VLogIndex>);

impl VLogIndexes {
    pub fn size(&self) -> usize {
        // min+max+time+generation+sequence+offset+size
        self.iter().map(|v| 48 + v.memory_size() - VLogIndex::SIZE).sum()
    }

    pub fn deduplicate(&mut self) {
        if self.len() <= 1 {
            return;
        }

        // See if we're already sorted and deduped
        let mut need_sort = false;
        for i in 1..self.len() {
            if self[i - 1].min >= self[i].min {
                need_sort = true;
                break;
            }
        }

        if !need_sort {
            return;
        }

        // 1. Stable sorts data in ascending order by min and max
        self.sort_by(|a, b| a.min.cmp(&b.min).then_with(|| a.max.cmp(&b.max)));

        // 2. remove the duplicated points(timestamps are same)
        let mut i = 0;
        for j in 1..self.len() {
            let v = &self[j];
            if (v.min != self[i].min) && (v.max != self[i].max) {
                i += 1;
            }
            self[i] = v.clone();
        }

        self.truncate(i + 1);
    }

    pub fn deduplicate_exact(&mut self) {
        if self.len() <= 1 {
            return;
        }

        self.sort_by(|a, b| {
            a.generation
                .cmp(&b.generation)
                .then_with(|| a.seq.cmp(&b.seq))
                .then_with(|| a.offset.cmp(&b.offset))
                .then_with(|| a.size.cmp(&b.size))
                .then_with(|| a.min.cmp(&b.min))
                .then_with(|| a.max.cmp(&b.max))
                .then_with(|| a.typ.cmp(&b.typ))
                .then_with(|| a.count.cmp(&b.count))
        });
        self.0.dedup();
    }

    /// Filter indexes by generation and sequence without mutating the source list.
    pub fn filter_by_gen_and_seq(&self, generation: i64, seq: i64) -> VLogIndexes {
        let mut filtered = VLogIndexes::default();
        for idx in self.iter() {
            if (idx.generation, idx.seq) == (generation, seq) {
                filtered.push(idx.clone());
            }
        }
        filtered
    }

    /// Exclude the values not in [min, max].  The values must be deduplicated and sorted
    /// before calling Exclude or the results are undefined.
    pub fn exclude(&mut self, min: i64, max: i64) {
        todo!("implementme");
    }

    /// Include returns the subset values between min and max inclusive. The values must
    /// be deduplicated and sorted before calling Exclude or the results are undefined.
    pub fn include(&mut self, min: i64, max: i64) {
        todo!("implementme");
    }

    /// search performs a binary search for unix_nano() v in a
    /// and returns the position, i, where v would be inserted.
    /// An additional check of self[i].unix_nano() == v is necessary
    /// to determine if the value v exists.
    pub fn search(&self, v: i64) -> usize {
        todo!("implementme");
    }

    /// Returns the positions where min and max would be
    /// inserted into the array. If self[0].unix_nano() > max or
    /// self[len-1].unix_nano() < min then find_range returns (-1, -1)
    /// indicating the array is outside the [min, max]. The values must
    /// be deduplicated and sorted before calling Exclude or the results
    /// are undefined.
    pub fn find_range(&self, min: i64, max: i64) -> (i64, i64) {
        todo!("implementme");
    }

    fn min(&self) -> i64 {
        self.first().expect("vlog index compactions id None").min
    }

    fn max(&self) -> i64 {
        self.last().expect("vlog index compactions id None").max
    }

    /// Merge overlays b to top of a.  If two values conflict with
    /// the same timestamp, b is used.  Both a and b must be sorted
    /// in ascending order.
    pub fn merge(&mut self, mut b: VLogIndexes) {
        todo!("implementme");
    }
}

impl Deref for VLogIndexes {
    type Target = Vec<VLogIndex>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for VLogIndexes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<VLogIndexes> for Vec<VLogIndex> {
    fn from(value: VLogIndexes) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use snafu::guide::examples;

    use super::Ring;

    #[test]
    fn test_ring_new() {
        struct Example {
            n: i32,
            return_err: bool,
        }

        let examples = vec![
            Example {
                n: 1,
                return_err: false,
            },
            Example {
                n: 2,
                return_err: false,
            },
            Example {
                n: 4,
                return_err: false,
            },
            Example {
                n: 8,
                return_err: false,
            },
            Example {
                n: 16,
                return_err: false,
            },
            Example {
                n: 32,
                return_err: true,
            },
            Example {
                n: 0,
                return_err: true,
            },
            Example {
                n: 3,
                return_err: true,
            },
        ];

        for e in examples.iter() {
            let r = Ring::new(e.n);
            if e.return_err {
                assert!(r.is_err());
                return;
            }
            assert!(r.is_ok());
            let r = r.unwrap();
            assert_eq!(e.n, r.partitions.len() as i32);

            // Check partitions distributed correctly
            let mut partitions = vec![];
            for (i, p) in r.partitions.iter().enumerate() {
                if i == 0 || partitions.is_empty() {
                    partitions.push(Arc::clone(p));
                } else {
                    let bp = &partitions[partitions.len() - 1];
                    if !Arc::ptr_eq(p, bp) {
                        partitions.push(Arc::clone(p));
                    }
                }
            }

            assert_eq!(
                e.n,
                partitions.len() as i32,
                "exp {}, got {}",
                e.n,
                partitions.len()
            );
        }
    }
}
