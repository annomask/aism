//! ring is a structure that maps series keys to entries.
use std::{collections::HashMap, fmt::format, sync::Arc, time::Duration};

use crate::common::{hll::hash64, to_str};
use crate::runtime::execution::{
    ExecutorRef, RuntimeExecutorExt, accepted_task_guard, legacy_global_executor,
};
use parking_lot::RwLock;
use snafu::{ResultExt, Snafu, ensure};

use super::cache::{Entry, Error, Result};
use crate::storage::tsm1::data_encoding::encoding_values::Values;

/// PARTITIONS is the number of partitions we used in the ring's continuum. It
/// basically defines the maximum number of partitions you can have in the ring.
/// If a smaller number of partitions are chosen when creating a ring, then
/// they're evenly spread across this many partitions in the ring.
pub const PARTITIONS: i32 = 16;

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
        if n <= 0 || n > PARTITIONS {
            return Err(Error::StringErr {
                reason: format!("invalid number of partitions: {}", n),
            });
        }

        if n & (n - 1) != 0 {
            return Err(Error::StringErr {
                reason: format!("partitions {} is not a power of two", n),
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
    pub fn entry(&self, key: &[u8]) -> Option<Arc<Entry>> {
        self.get_partition(key).entry(key)
    }

    /// Writes values to the entry in the ring's partition associated with key.
    /// If no entry exists for the key then one will be created.
    /// write is safe for use by multiple threads.
    pub fn write(&self, key: String, values: Values) -> (bool, Result<(), Error>) {
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

    /// Returns the number of values in the ring
    pub fn count(&self) -> i64 {
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
        F: FnMut(&[u8], &Arc<Entry>) -> Result<()> + Clone + Send + 'static,
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
        F: FnMut(Vec<u8>, &Arc<Entry>) -> Result<(), E>,
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
    store: RwLock<HashMap<String, Arc<Entry>>>,
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
    fn entry(&self, key: &[u8]) -> Option<Arc<Entry>> {
        self.store.read().get(to_str!(key)).cloned()
    }

    /// write writes the values to the entry in the partition, creating the entry
    /// if it does not exist.
    /// write is safe for use by multiple goroutines.
    fn write(&self, key: String, values: Values) -> (bool, Result<()>) {
        let store = self.store.read();

        if let Some(e) = store.get(&key) {
            // Hot path.
            return (false, e.add(values.into()));
        }
        drop(store);

        let mut store = self.store.write();
        // Check again.
        if let Some(e) = store.get(&key) {
            return (false, e.add(values.into()));
        }

        // Create a new entry using a preallocated size if we have a hint available.
        match Entry::new(values.into()) {
            Ok(e) => {
                store.insert(key, Arc::new(e));
                (true, Ok(()))
            }
            Err(e) => (false, Err(e)),
        }
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

    //// reset resets the partition by reinitialising the store. reset returns hints
    /// about sizes that the entries within the store could be reallocated with.
    fn reset(&self) {
        let sz = {
            let store = self.store.read();
            store.len()
        };
        // Drop store to a background thread.
        let new_store = HashMap::<String, Arc<Entry>>::with_capacity(sz);
        let mut store = self.store.write();

        let defer_store = std::mem::replace(&mut *store, new_store);
        if let Err(error) = self.executor.spawn_blocking(move || {
            drop(defer_store);
        }) {
            tracing::debug!(
                target: "reefsdb::tsdb::cache",
                error = %error,
                "deferred ring store drop scheduling rejected"
            );
        }
    }

    fn count(&self) -> i64 {
        let mut n = 0;
        let store = self.store.read();
        for (_k, v) in store.iter() {
            if v.count() > 0 {
                n += 1;
            }
        }
        n
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use snafu::guide::examples;

    use crate::storage::tsm1::ring::Ring;

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
