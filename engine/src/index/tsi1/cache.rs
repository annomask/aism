//! TagValueSeriesIDCache is an LRU cache for series id sets associated with
//! name -> key -> value mappings. The purpose of the cache is to provide
//! efficient means to get sets of series ids that would otherwise involve merging
//! many individual bitmaps at query time.
//!
//! When initialising a TagValueSeriesIDCache a capacity must be provided. When
//! more than c items are added to the cache, the least recently used item is
//! evicted from the cache.
use std::{
    collections::{HashMap, LinkedList},
    hash::Hash,
    sync::Arc,
    vec,
};

use crate::common::{to_str, to_string};
use parking_lot::RwLock;

use crate::series::series_set::SeriesIDSet;

/// A TagValueSeriesIDCache comprises a linked list implementation to track the
/// order by which items should be evicted from the cache, and a hashmap implementation
/// to provide constant time retrievals of items from the cache.
#[derive(Default)]
pub struct TagValueSeriesIDCache {
    cache: HashMap<String, HashMap<String, HashMap<String, Arc<RwLock<SeriesIDCacheElement>>>>>,
    evictor: LinkedList<Arc<RwLock<SeriesIDCacheElement>>>,
    capacity: i64,
}

impl TagValueSeriesIDCache {
    /// Returns a TagValueSeriesIDCache with capacity c.
    pub fn new(capacity: i64) -> Self {
        Self {
            capacity,
            ..Default::default()
        }
    }

    /// Returns the SeriesIDSet associated with the {name, key, value} tuple if it exists.
    /// This returns a cloned SeriesIDSet, because new_series_id_set_iter needs ownership.
    pub fn get(&mut self, name: &[u8], key: &[u8], value: &[u8]) -> Option<SeriesIDSet> {
        if let Some(mmap) = self.cache.get(to_str!(name)) {
            if let Some(tkmap) = mmap.get(to_str!(key)) {
                if let Some(ele) = tkmap.get(to_str!(value)) {
                    let e = &*ele.read();
                    // Move element to front, this now becomes most recently used.
                    // 1.search for index
                    let idx = self.evictor.iter().position(|x| *x.read() == *e).unwrap();
                    // 2.remove  the element at the given index
                    let mut tail = self.evictor.split_off(idx);
                    let _ = tail.pop_front();
                    self.evictor.append(&mut tail);
                    // 3. insert element to the front of list
                    self.evictor.push_front(Arc::clone(ele));

                    return e.series_set.clone();
                }
                return None;
            }
            return None;
        }
        None
    }

    // Note: all_cached_series_set is incorrect!
    // pub fn all_cached_series_set(&self) -> SeriesIDSet {
    //     let mut set = SeriesIDSet::new(&[]);
    //     for name in self.cache.keys() {
    //         if let Some(mmap) = self.cache.get(name.as_str()) {
    //             for (tkey, tkmap) in mmap {
    //                 for ele in tkmap.values() {
    //                     let e = &*ele.read();
    //                     if let Some(series_set) = &e.series_set {
    //                         set.merge_in_place(series_set);
    //                     }
    //                 }
    //             }
    //         }
    //     }

    //     set
    // }

    // Note: cached_tag_values is incorrect!
    // pub fn cached_tag_values(&self) -> HashMap<String, HashMap<String, Vec<String>>> {
    //     let mut measurement_tag_values = HashMap::<String, HashMap<String, Vec<String>>>::new();
    //     // measurement: cpu
    //     // tag_values: { (hostname: [(cpu,hostname,host_1), (cpu,hostname,host_2), ...]), (region...)}
    //     for (measurement, tag_values) in self.cache.iter() {
    //         let measurement_entry = measurement_tag_values
    //             .entry(measurement.to_string())
    //             .or_insert_with(HashMap::new);

    //         for (tag_key, tag_values) in tag_values {
    //             let measurement_tag_values_entry = measurement_entry
    //                 .entry(tag_key.clone())
    //                 .or_insert_with(Vec::new);
    //             for (_, ele) in tag_values {
    //                 let tv = ele.read().value.clone();
    //                 measurement_tag_values_entry.push(tv);
    //             }
    //         }
    //     }

    //     measurement_tag_values
    // }

    /// Adds the SeriesIDSet to the cache under the tuple {name, key, value}. If
    /// the cache is at its limit, then the least recently used item is evicted.
    pub fn put(&mut self, name: &[u8], key: &[u8], value: &[u8], ss: Option<SeriesIDSet>) {
        // Check under the write lock if the relevant item is now in the cache.
        let exists = {
            if let Some(mmap) = self.cache.get(to_str!(name)) {
                match mmap.get(to_str!(key)) {
                    Some(tkmap) => tkmap.contains_key(to_str!(value)),
                    None => false,
                };
            }

            false
        };
        if exists {
            return;
        }

        // Create list item, and add to the front of the eviction list.
        self.evictor
            .push_front(Arc::new(RwLock::new(SeriesIDCacheElement {
                name: to_string!(name),
                key: to_string!(key),
                value: to_string!(value),
                series_set: ss,
            })));
        let front = Arc::clone(self.evictor.front().unwrap());

        // Add the listElement to the set of items.
        if let Some(mmap) = self.cache.get_mut(to_str!(name)) {
            if let Some(tkmap) = mmap.get_mut(to_str!(key)) {
                if tkmap.contains_key(to_str!(value)) {
                    self.check_eviction();
                    return;
                }
                let _ = tkmap.insert(to_string!(value), Arc::clone(&front));
                self.check_eviction();
                return;
            }

            // No series set map for the tag key - first tag value for the tag key.
            mmap.insert(
                to_string!(key),
                HashMap::from([(to_string!(value), Arc::clone(&front))]),
            );
            self.check_eviction();
            return;
        }

        // No map for the measurement - first tag key for the measurement.
        self.cache.insert(
            to_string!(name),
            HashMap::from([(to_string!(key), HashMap::from([(to_string!(value), front)]))]),
        );

        self.check_eviction();
    }

    /// Checks if the cache is too big, and evicts the least recently used item if it is.
    fn check_eviction(&mut self) {
        if self.evictor.len() as i64 <= self.capacity {
            return;
        }

        let binding = self.evictor.pop_back().unwrap();
        let e = binding.read(); // Remove from evictor
        let name = &e.name;
        let key = &e.key;
        let value = &e.value;

        let _ = self
            .cache
            .get_mut(name)
            .unwrap()
            .get_mut(key)
            .unwrap()
            .remove(value); // Remove from hashmap of items.

        // Check if there are no more tag values for the tag key.
        if self.cache.get(name).unwrap().get(key).unwrap().is_empty() {
            let _ = self.cache.get_mut(name).unwrap().remove(key);
        }

        // Check there are no more tag keys for the measurement.
        if self.cache.get(name).unwrap().is_empty() {
            let _ = self.cache.remove(name);
        }
    }

    /// Removes x from the tuple {name, key, value} if it exists.
    /// This method takes a lock on the underlying SeriesIDSet.
    pub fn delete(&self, name: &[u8], key: &[u8], value: &[u8], x: u64) {
        if let Some(mmap) = self.cache.get(to_str!(name)) {
            if let Some(tkmap) = mmap.get(to_str!(key)) {
                if let Some(ele) = tkmap.get(to_str!(value)) {
                    let ele = ele.write();
                    if let Some(ss) = &ele.series_set {
                        ss.remove(x);
                    }
                }
            }
        }
    }

    /// Removes every cached tag-value series set for a measurement.
    pub(crate) fn remove_measurement(&mut self, name: &[u8]) {
        let name = to_str!(name);
        if self.cache.remove(name).is_none() {
            return;
        }
        self.evictor = std::mem::take(&mut self.evictor)
            .into_iter()
            .filter(|element| element.read().name != name)
            .collect();
    }

    /// Returns true if there are sets cached for the provided measurement.
    pub fn measurement_contains_sets(&self, name: &[u8]) -> bool {
        self.cache.contains_key(to_str!(name))
    }

    /// Adds x to the SeriesIDSet associated with the tuple {name, key, value}
    /// if it exists. This method takes a lock on the underlying SeriesIDSet.
    ///
    /// NB this does not count as an access on the set—therefore the set is not promoted
    /// within the LRU cache.
    pub fn add_to_set(&self, name: &[u8], key: &[u8], value: &[u8], x: u64) {
        if let Some(mmap) = self.cache.get(to_str!(name)) {
            if let Some(tkmap) = mmap.get(to_str!(key)) {
                if let Some(ele) = tkmap.get(to_str!(value)) {
                    let mut ele = ele.write();
                    if ele.series_set.is_none() {
                        ele.series_set = Some(SeriesIDSet::new(&[x]));
                        return;
                    }
                    ele.series_set.as_ref().unwrap().add(x);
                }
            }
        }
    }

    #[cfg(test)]
    /// Returns the evictor length.
    pub fn len(&self) -> usize {
        self.evictor.len()
    }

    #[cfg(test)]
    pub fn has_name(&self, name: &[u8]) -> bool {
        self.cache.contains_key(to_str!(name))
    }

    #[cfg(test)]
    pub fn has_key(&self, name: &[u8], key: &[u8]) -> bool {
        if let Some(mmap) = self.cache.get(to_str!(name)) {
            if let Some(tkmap) = mmap.get(to_str!(key)) {
                return mmap.contains_key(to_str!(key));
            }
            return false;
        }
        false
    }
}

/// SeriesIDCacheElement is an item stored within a cache.
#[derive(Default, PartialEq)]
struct SeriesIDCacheElement {
    name: String,
    key: String,
    value: String,
    series_set: Option<SeriesIDSet>,
}

#[cfg(test)]
mod tests {
    use std::ops::{Deref, DerefMut};

    use rand::Rng;

    use super::TagValueSeriesIDCache;
    use crate::series::series_set::SeriesIDSet;
    #[test]
    fn test_tag_value_series_id_cache() {
        let m0k0v0 = SeriesIDSet::new(&[1, 2, 3, 4, 5]);
        let m0k0v1 = SeriesIDSet::new(&[10, 20, 30, 40, 50]);
        let m0k1v2 = SeriesIDSet::new(&[]);
        let m1k3v0 = SeriesIDSet::new(&[900, 0, 929]);

        let mut cache = TestCache(TagValueSeriesIDCache::new(10));
        cache.has("m0".as_bytes(), "k0".as_bytes(), "v0".as_bytes(), None);

        // Putting something in the cache makes it retrievable.
        cache.put_by_string(
            "m0".as_bytes(),
            "k0".as_bytes(),
            "v0".as_bytes(),
            Some(m0k0v0.clone()),
        );
        cache.has(
            "m0".as_bytes(),
            "k0".as_bytes(),
            "v0".as_bytes(),
            Some(m0k0v0.clone()),
        );

        // Putting something else under the same key will not replace the original item.
        cache.put_by_string(
            "m0".as_bytes(),
            "k0".as_bytes(),
            "v0".as_bytes(),
            Some(SeriesIDSet::new(&[100, 200])),
        );
        cache.has(
            "m0".as_bytes(),
            "k0".as_bytes(),
            "v0".as_bytes(),
            Some(m0k0v0.clone()),
        );

        // Add another item to the cache.
        cache.put_by_string(
            "m0".as_bytes(),
            "k0".as_bytes(),
            "v1".as_bytes(),
            Some(m0k0v1.clone()),
        );
        cache.has(
            "m0".as_bytes(),
            "k0".as_bytes(),
            "v0".as_bytes(),
            Some(m0k0v0.clone()),
        );
        cache.has(
            "m0".as_bytes(),
            "k0".as_bytes(),
            "v1".as_bytes(),
            Some(m0k0v1.clone()),
        );

        // Add some more items
        cache.put_by_string(
            "m0".as_bytes(),
            "k1".as_bytes(),
            "v2".as_bytes(),
            Some(m0k1v2.clone()),
        );
        cache.put_by_string(
            "m1".as_bytes(),
            "k3".as_bytes(),
            "v0".as_bytes(),
            Some(m1k3v0.clone()),
        );
        cache.has(
            "m0".as_bytes(),
            "k0".as_bytes(),
            "v0".as_bytes(),
            Some(m0k0v0.clone()),
        );
        cache.has(
            "m0".as_bytes(),
            "k0".as_bytes(),
            "v1".as_bytes(),
            Some(m0k0v1.clone()),
        );
        cache.has(
            "m0".as_bytes(),
            "k1".as_bytes(),
            "v2".as_bytes(),
            Some(m0k1v2.clone()),
        );
        cache.has(
            "m1".as_bytes(),
            "k3".as_bytes(),
            "v0".as_bytes(),
            Some(m1k3v0.clone()),
        );
    }

    #[test]
    fn test_tag_value_series_id_cache_remove_measurement() {
        let mut cache = TagValueSeriesIDCache::new(10);
        cache.put(b"cpu", b"host", b"a", Some(SeriesIDSet::new(&[1])));
        cache.put(b"cpu", b"region", b"west", Some(SeriesIDSet::new(&[1])));
        cache.put(b"mem", b"host", b"a", Some(SeriesIDSet::new(&[2])));

        cache.remove_measurement(b"cpu");

        assert!(!cache.measurement_contains_sets(b"cpu"));
        assert!(cache.get(b"cpu", b"host", b"a").is_none());
        assert_eq!(cache.get(b"mem", b"host", b"a").unwrap().cardinality(), 1);
        assert_eq!(cache.len(), 1);

        cache.remove_measurement(b"cpu");
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_tag_value_series_id_cache_cviction() {
        let m0k0v0 = SeriesIDSet::new(&[1, 2, 3, 4, 5]);
        let m0k0v1 = SeriesIDSet::new(&[10, 20, 30, 40, 50]);
        let m0k1v2 = SeriesIDSet::new(&[]);
        let m1k3v0 = SeriesIDSet::new(&[900, 0, 929]);

        let mut cache = TestCache(TagValueSeriesIDCache::new(4));
        cache.put_by_string(
            "m0".as_bytes(),
            "k0".as_bytes(),
            "v0".as_bytes(),
            Some(m0k0v0.clone()),
        );
        cache.put_by_string(
            "m0".as_bytes(),
            "k0".as_bytes(),
            "v1".as_bytes(),
            Some(m0k0v1.clone()),
        );
        cache.put_by_string(
            "m0".as_bytes(),
            "k1".as_bytes(),
            "v2".as_bytes(),
            Some(m0k1v2.clone()),
        );
        cache.put_by_string(
            "m1".as_bytes(),
            "k3".as_bytes(),
            "v0".as_bytes(),
            Some(m1k3v0.clone()),
        );
        cache.has(
            "m0".as_bytes(),
            "k0".as_bytes(),
            "v0".as_bytes(),
            Some(m0k0v0.clone()),
        );
        cache.has(
            "m0".as_bytes(),
            "k0".as_bytes(),
            "v1".as_bytes(),
            Some(m0k0v1.clone()),
        );
        cache.has(
            "m0".as_bytes(),
            "k1".as_bytes(),
            "v2".as_bytes(),
            Some(m0k1v2.clone()),
        );
        cache.has(
            "m1".as_bytes(),
            "k3".as_bytes(),
            "v0".as_bytes(),
            Some(m1k3v0.clone()),
        );

        // Putting another item in the cache will evict m0k0v0
        let m2k0v0 = SeriesIDSet::new(&[8, 8, 8]);
        cache.put_by_string(
            "m2".as_bytes(),
            "k0".as_bytes(),
            "v0".as_bytes(),
            Some(m2k0v0.clone()),
        );
        let got = cache.len();
        assert_eq!(got, 4, "cache size was {}, expected {}", got, 4);

        cache.has_not("m0".as_bytes(), "k0".as_bytes(), "v0".as_bytes());
        cache.has(
            "m0".as_bytes(),
            "k0".as_bytes(),
            "v1".as_bytes(),
            Some(m0k0v1.clone()),
        );
        cache.has(
            "m0".as_bytes(),
            "k1".as_bytes(),
            "v2".as_bytes(),
            Some(m0k1v2.clone()),
        );
        cache.has(
            "m1".as_bytes(),
            "k3".as_bytes(),
            "v0".as_bytes(),
            Some(m1k3v0.clone()),
        );
        cache.has(
            "m2".as_bytes(),
            "k0".as_bytes(),
            "v0".as_bytes(),
            Some(m2k0v0.clone()),
        );

        // Putting another item in the cache will evict m0k0v1. That  will mean
        // there will be no values left under the tuple {m0, k0}
        assert!(
            cache.has_key("m0".as_bytes(), "k0".as_bytes()),
            "Map missing for key {}",
            "k0"
        );

        let m2k0v1 = SeriesIDSet::new(&[8, 8, 8]);
        cache.put_by_string(
            "m2".as_bytes(),
            "k0".as_bytes(),
            "v1".as_bytes(),
            Some(m2k0v1.clone()),
        );
        let got = cache.len();
        assert_eq!(got, 4, "cache size was {}, expected {}", got, 4);
        cache.has_not("m0".as_bytes(), "k0".as_bytes(), "v0".as_bytes());
        cache.has_not("m0".as_bytes(), "k0".as_bytes(), "v1".as_bytes());
        cache.has(
            "m0".as_bytes(),
            "k1".as_bytes(),
            "v2".as_bytes(),
            Some(m0k1v2.clone()),
        );
        cache.has(
            "m1".as_bytes(),
            "k3".as_bytes(),
            "v0".as_bytes(),
            Some(m1k3v0.clone()),
        );
        cache.has(
            "m2".as_bytes(),
            "k0".as_bytes(),
            "v0".as_bytes(),
            Some(m2k0v0.clone()),
        );
        cache.has(
            "m2".as_bytes(),
            "k0".as_bytes(),
            "v1".as_bytes(),
            Some(m2k0v1.clone()),
        );

        // Further, the map for all tag values for the tuple {m0, k0} should be removed.
        assert!(
            !cache.has_key("m0".as_bytes(), "k0".as_bytes()),
            "Map present for key {}, should be removed",
            "k0"
        );

        // Putting another item in the cache will evict m0k1v2. That  will mean
        // there will be no values left under the tuple {m0}
        assert!(
            cache.has_name("m0".as_bytes()),
            "Map missing for key {}",
            "k0"
        );
        let m2k0v2 = SeriesIDSet::new(&[8, 9, 9]);
        cache.put_by_string(
            "m2".as_bytes(),
            "k0".as_bytes(),
            "v2".as_bytes(),
            Some(m2k0v2.clone()),
        );
        cache.has_not("m0".as_bytes(), "k0".as_bytes(), "v0".as_bytes());
        cache.has_not("m0".as_bytes(), "k0".as_bytes(), "v1".as_bytes());
        cache.has_not("m0".as_bytes(), "k1".as_bytes(), "v2".as_bytes());
        cache.has(
            "m1".as_bytes(),
            "k3".as_bytes(),
            "v0".as_bytes(),
            Some(m1k3v0.clone()),
        );
        cache.has(
            "m2".as_bytes(),
            "k0".as_bytes(),
            "v0".as_bytes(),
            Some(m2k0v0.clone()),
        );
        cache.has(
            "m2".as_bytes(),
            "k0".as_bytes(),
            "v1".as_bytes(),
            Some(m2k0v1.clone()),
        );
        cache.has(
            "m2".as_bytes(),
            "k0".as_bytes(),
            "v2".as_bytes(),
            Some(m2k0v2.clone()),
        );

        // The map for all tag values for the tuple {m0} should be removed.
        assert!(
            !cache.has_name("m0".as_bytes()),
            "Map present for key {}, should be removed",
            "k0"
        );

        // Putting another item in the cache will evict m2k0v0 if we first get m1k3v0
        // because m2k0v0 will have been used less recently...
        let m3k0v0 = SeriesIDSet::new(&[1000]);
        cache.has(
            "m1".as_bytes(),
            "k3".as_bytes(),
            "v0".as_bytes(),
            Some(m1k3v0.clone()),
        ); // This makes it the most recently used rather than the least.
        cache.put_by_string(
            "m3".as_bytes(),
            "k0".as_bytes(),
            "v0".as_bytes(),
            Some(m3k0v0.clone()),
        );

        cache.has_not("m0".as_bytes(), "k0".as_bytes(), "v0".as_bytes());
        cache.has_not("m0".as_bytes(), "k0".as_bytes(), "v1".as_bytes());
        cache.has_not("m0".as_bytes(), "k1".as_bytes(), "v2".as_bytes());
        cache.has_not("m2".as_bytes(), "k0".as_bytes(), "v0".as_bytes()); // This got pushed to the back.

        cache.has(
            "m1".as_bytes(),
            "k3".as_bytes(),
            "v0".as_bytes(),
            Some(m1k3v0.clone()),
        ); // This got saved because we looked at it before we added to the cache
        cache.has(
            "m2".as_bytes(),
            "k0".as_bytes(),
            "v1".as_bytes(),
            Some(m2k0v1.clone()),
        );
        cache.has(
            "m2".as_bytes(),
            "k0".as_bytes(),
            "v2".as_bytes(),
            Some(m2k0v2.clone()),
        );
        cache.has(
            "m3".as_bytes(),
            "k0".as_bytes(),
            "v0".as_bytes(),
            Some(m3k0v0.clone()),
        );
    }

    #[test]
    fn test_tag_value_series_id_cache_add_to_set() {
        let mut cache = TestCache(TagValueSeriesIDCache::new(4));
        cache.put_by_string("m0".as_bytes(), "k0".as_bytes(), "v0".as_bytes(), None);
        let s2 = SeriesIDSet::new(&[100]);
        cache.put_by_string(
            "m0".as_bytes(),
            "k0".as_bytes(),
            "v1".as_bytes(),
            Some(s2.clone()),
        );
        cache.has("m0".as_bytes(), "k0".as_bytes(), "v0".as_bytes(), None);
        cache.has("m0".as_bytes(), "k0".as_bytes(), "v1".as_bytes(), Some(s2));

        {
            cache.add_to_set("m0".as_bytes(), "k0".as_bytes(), "v0".as_bytes(), 20); // No non-None set exists so one will be created
            cache.add_to_set("m0".as_bytes(), "k0".as_bytes(), "v1".as_bytes(), 101);
            // No non-None set exists so one will be created
        }
        cache.has(
            "m0".as_bytes(),
            "k0".as_bytes(),
            "v1".as_bytes(),
            Some(SeriesIDSet::new(&[100, 101])),
        );

        let ss = cache.get_by_string("m0".as_bytes(), "k0".as_bytes(), "v0".as_bytes());
        assert_eq!(Some(SeriesIDSet::new(&[20])), ss);
    }

    use std::sync::{Arc, Barrier};

    use parking_lot::RwLock;
    use rand::prelude::SliceRandom;
    #[test]
    fn test_tag_value_series_id_cache_concurrent_get_put() {
        let a = vec!["a", "b", "c", "d", "e"];
        let rnd = move || -> &'static str { *a.choose(&mut rand::thread_rng()).unwrap() };

        let barrier = Arc::new(Barrier::new(10));
        let mut handles = Vec::with_capacity(10);

        let mut cache = Arc::new(RwLock::new(TestCache(TagValueSeriesIDCache::new(100))));

        for _ in 0..5 {
            let rnd = rnd.clone();
            let c = Arc::clone(&barrier);
            let cache = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                c.wait();
                let _ = cache.write().put(
                    rnd().as_bytes(),
                    rnd().as_bytes(),
                    rnd().as_bytes(),
                    Some(SeriesIDSet::new(&[])),
                );
            }));
        }

        for _ in 0..5 {
            let rnd = rnd.clone();
            let c = Arc::clone(&barrier);
            let cache = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                c.wait();
                let _ = cache
                    .write()
                    .get(rnd().as_bytes(), rnd().as_bytes(), rnd().as_bytes());
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    struct TestCache(TagValueSeriesIDCache);
    impl TestCache {
        fn has(&mut self, name: &[u8], key: &[u8], value: &[u8], ss: Option<SeriesIDSet>) {
            let got = self.get(name, key, value);
            assert_eq!(got, ss, "got set: {:?}, expected: {:?}", got, ss);
        }

        fn has_not(&mut self, name: &[u8], key: &[u8], value: &[u8]) {
            let got = self.get(name, key, value);
            assert!(
                got.is_none(),
                "got non-None set {:?} for {{{:?}, {:?}, {:?}}}",
                got,
                name,
                key,
                value
            );
        }

        fn get_by_string(&mut self, name: &[u8], key: &[u8], value: &[u8]) -> Option<SeriesIDSet> {
            self.get(name, key, value)
        }

        fn put_by_string(
            &mut self,
            name: &[u8],
            key: &[u8],
            value: &[u8],
            ss: Option<SeriesIDSet>,
        ) {
            self.put(name, key, value, ss)
        }
    }

    impl Deref for TestCache {
        type Target = TagValueSeriesIDCache;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl DerefMut for TestCache {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }
}
