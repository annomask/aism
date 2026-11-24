//! HashMap represents a hash map that implements Robin Hood Hashing.
//! https://cs.uwaterloo.ca/research/tr/1986/CS-86-14.OpenSeriesPartitionFailed
use std::{cmp::Ordering, hash::Hash};

use bytes::BufMut;

use crate::common::hll::hash64;

#[derive(Debug, Default)]
pub struct HashMap<K, V> {
    hashes: Vec<i64>,
    elems: Vec<HashElem<K, V>>,

    n: i64,
    pub capacity: i64,
    threshold: i64,
    mask: i64,
    pub load_factor: i32,

    tmp_key: Vec<u8>,
}

impl<K: Eq + Hash, V: Clone> HashMap<K, V> {
    /// Initialises a new Hashmap with the provided options.
    pub fn new(opt: Options) -> Self {
        let mut m = Self {
            capacity: pow2(opt.capacity), // Limited to 2^64.
            load_factor: opt.load_factor,

            hashes: vec![],
            elems: vec![],
            n: 0i64,
            threshold: 0i64,
            mask: 0i64,
            tmp_key: vec![],
        };
        m.alloc();

        m
    }

    /// Clears the values in the map without deallocating the space.
    pub fn reset(&mut self) {
        for i in 0..self.capacity {
            self.hashes[i as usize] = 0;
            self.elems[i as usize].reset();
        }
        self.n = 0;
    }

    pub fn load_factor(&self) -> i32 {
        self.load_factor
    }

    /// Returns the value for a key from the Hashmap, or None if no key exists.
    pub fn get(&self, key: &[u8]) -> Option<&V> {
        let i = self.index(key);
        if i == -1 {
            return None;
        }
        self.elems[i as usize].value.as_ref()
    }

    /// Returns the i-th key/value pair of the hash map.
    pub fn elem(&self, i: i64) -> (Option<&[u8]>, Option<&V>) {
        if i >= self.elems.len() as i64 {
            return (None, None);
        }
        let e = &self.elems[i as usize];
        (Some(e.key.as_slice()), e.value.as_ref())
    }

    /// Returns the number of key/values set in map.
    pub fn len(&self) -> i64 {
        self.n
    }

    /// Returns true if the map has a length of 0.
    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    /// Returns the number of key/values set in map.
    pub fn capacity(&self) -> i64 {
        self.capacity
    }

    /// Stores the value at key in the Hashmap, overwriting an existing value if
    /// one exists. If the maximum load of the Hashmap is reached, the Hashmap will
    /// first resize itself.
    /// alloc elems according to currently set capacity.
    pub fn put(&mut self, key: Vec<u8>, val: V) {
        self.put2(key, val);
    }

    fn put2(&mut self, key: Vec<u8>, val: V) {
        // Grow the map if we've run out of slots.
        self.n += 1;
        if self.n > self.threshold {
            self.grow(self.capacity * 2);
        }

        // If the key was overwritten then decrement the size.
        let overwritten = self.insert(hash_key(key.as_slice()), key, val);
        if overwritten {
            self.n -= 1;
        }
    }

    fn insert(&mut self, mut hash: i64, key: Vec<u8>, mut val: V) -> bool {
        let mut pos = hash & self.mask;
        let mut d = 0i64;

        let mut copied = false;
        let mut search_key = key.clone();

        // Continue searching until we find an empty slot or lower probe distance.
        loop {
            let matched = self.elems[pos as usize].key == search_key;

            // Empty slot found or matching key, insert and exit.
            let e = &mut self.elems[pos as usize];
            if self.hashes[pos as usize] == 0 || matched {
                self.hashes[pos as usize] = hash;
                (e.hash, e.value) = (hash, Some(val));
                e.set_key(search_key.as_slice());
                return matched;
            }

            // If the existing elem has probed less than us, then swap places with
            // existing elem, and keep going to find another slot for that elem.
            let elem_dist = dist(self.hashes[pos as usize], pos, self.capacity);
            if elem_dist < d {
                // Swap with current position.
                (hash, self.hashes[pos as usize]) = (self.hashes[pos as usize], hash);
                (val, e.value) = (e.value.as_ref().unwrap().clone(), Some(val)); // TODO: how to get ownership other than clone ?

                // Copy e.key to self.tmp_key
                assign(&mut self.tmp_key, e.key.as_slice());

                e.set_key(search_key.as_slice());

                if !copied {
                    search_key = vec![0u8; key.len()];
                    search_key.copy_from_slice(key.as_slice());
                    copied = true;
                }

                // copy m.tmp_key to search_key
                assign(&mut search_key, self.tmp_key.as_slice());

                // Update current distance.
                d = elem_dist;
            }
            // Increment position, wrap around on overflow.
            pos = (pos + 1) & self.mask;
            d += 1;
        }
    }

    fn alloc(&mut self) {
        self.elems = vec![HashElem::default(); self.capacity as usize];
        self.hashes = vec![0i64; self.capacity as usize];
        self.threshold = (self.capacity * (self.load_factor as i64)) / 100;
        self.mask = self.capacity - 1;
    }

    /// Increases the capacity and reinserts all existing hashes & elements.
    fn grow(&mut self, sz: i64) {
        // Ensure new capacity is a power of two and greater than current capacity.
        let sz = pow2(sz);
        if sz <= self.capacity {
            return;
        }

        // Copy old elements and hashes.
        let (elems, hashes) = (self.elems.clone(), self.hashes.clone());
        let capacity = self.capacity;

        // Increase capacity & reallocate.
        self.capacity = sz;
        self.alloc();

        // Copy old elements to new hash/elem list.
        for i in 0..capacity {
            // TODO: how to get ownership other than clone ?
            let (elem, hash) = (elems[i as usize].clone(), hashes[i as usize]);
            if hash == 0 {
                continue;
            }
            self.insert(hash, elem.key, elem.value.unwrap());
        }
    }

    /// Index returns the position of key in the hash map.
    fn index(&self, key: &[u8]) -> i64 {
        let hash = hash_key(key);
        let mut pos = hash & self.mask;

        let mut d = 0i64;
        loop {
            if self.hashes[pos as usize] == 0
                || d > dist(self.hashes[pos as usize], pos, self.capacity)
            {
                return -1;
            } else if self.hashes[pos as usize] == hash && self.elems[pos as usize].key == key {
                return pos;
            }

            pos = (pos + 1) & self.mask;
            d += 1;
        }
    }
}

/// Represents initialization options that are passed to NewHashMap().
pub struct Options {
    pub capacity: i64,
    pub load_factor: i32,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            capacity: 256,
            load_factor: 90,
        }
    }
}

#[derive(Debug)]
struct HashElem<K, V> {
    key: Vec<u8>,
    value: Option<V>,
    hash: i64,
    _marker: std::marker::PhantomData<(K, V)>,
}

impl<K, V> HashElem<K, V> {
    // Clears the values in the element.
    fn reset(&mut self) {
        self.key = vec![];
        self.value = None;
        self.hash = 0;
    }

    // Copies v to a key on self.
    fn set_key(&mut self, v: &[u8]) {
        assign(&mut self.key, v);
    }
}

impl<K, V> Default for HashElem<K, V> {
    fn default() -> Self {
        Self {
            key: vec![],
            value: None,
            hash: 0i64,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<K, V: Clone> Clone for HashElem<K, V> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            value: self.value.clone(),
            hash: self.hash,
            _marker: std::marker::PhantomData,
        }
    }
}

/// Computes a hash of key. Hash is always non-zero.
pub fn hash_key(key: &[u8]) -> i64 {
    let mut h = hash64(key) as i64;
    match h.cmp(&0) {
        Ordering::Equal => h = 1,
        Ordering::Less => h = -h,
        _ => {}
    }
    h
}

/// Computes a hash of an int64. Hash is always non-zero.
pub fn hash_u64(key: u64) -> i64 {
    let mut buf = [0u8; 8];
    buf.as_mut_slice().put_u64(key);
    hash_key(&buf)
}

/// Returns the probe distance for a hash in a slot index.
// NOTE: Capacity must be a power of 2.
pub fn dist(hash: i64, i: i64, capacity: i64) -> i64 {
    let mask = capacity - 1;
    (i + capacity - (hash & mask)) & mask
}

// Returns the number that is the next highest power of 2.
// Returns v if it is a power of 2.
pub fn pow2(v: i64) -> i64 {
    let mut i = 2i64;
    while i < 1 << 62 {
        if i >= v {
            return i;
        }
        i *= 2;
    }
    unreachable!("pow2 should always return the next highest power of 2");
}

fn assign(x: &mut Vec<u8>, v: &[u8]) {
    match x.len().cmp(&v.len()) {
        Ordering::Greater => x.truncate(v.len()),
        Ordering::Less => *x = vec![0u8; v.len()],
        Ordering::Equal => {}
    }

    x.copy_from_slice(v)
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use rand_core::{OsRng, RngCore};

    use crate::common::rhh;

    // Ensure hash map can perform basic get/put operations.
    #[test]
    fn test_hash_map() {
        let mut m = rhh::HashMap::<Vec<u8>, Vec<u8>>::new(rhh::Options::default());
        m.put("foo".as_bytes().to_vec(), "bar".as_bytes().to_vec());
        m.put("baz".as_bytes().to_vec(), "bat".as_bytes().to_vec());

        // Verify values can be retrieved.
        let v = m.get("foo".as_bytes()).unwrap();
        assert_eq!(v, "bar".as_bytes(), "unexpected value: {:?}", v);
        let v = m.get("baz".as_bytes()).unwrap();
        assert_eq!(v, "bat".as_bytes(), "unexpected value: {:?}", v);

        // Overwrite field & verify.
        m.put("foo".as_bytes().to_vec(), "XXX".as_bytes().to_vec());
        let v = m.get("foo".as_bytes()).unwrap();
        assert_eq!(v, "XXX".as_bytes(), "unexpected value: {:?}", v);
    }

    // Ensure hash map can insert random data.
    #[test]
    fn test_hash_map_random() {
        // Iterate 100 times, check if hashmap can insert random data.
        for i in 0..100 {
            let mut m = rhh::HashMap::<Vec<u8>, Vec<u8>>::new(rhh::Options {
                capacity: 1000,
                load_factor: 90,
            });

            let n: i32 = rand::thread_rng().gen_range(0..10000);
            let keys = generate_byte_slices(n);
            let values = generate_byte_slices(n);

            let mut h = std::collections::HashMap::with_capacity(n as usize);
            for (i, k) in keys.iter().enumerate() {
                let v = &values[i];
                h.insert(k.clone(), v.clone());
                m.put(k.clone(), v.clone());
            }

            // Verify the maps are equal.
            for (i, (k, v)) in h.iter().enumerate() {
                let mv = m.get(k.as_slice());
                assert_eq!(
                    // if key is empty vec: [], then get returns empty vec too: Some(&[])
                    mv.unwrap(),
                    v.as_slice(),
                    "value mismatch:
key={:?}
got={:?}
exp={:?}

",
                    k.as_slice(),
                    mv.unwrap(),
                    v.as_slice()
                );
            }
        }
    }

    // GenerateByteSlices returns a random list of byte slices.
    pub fn generate_byte_slices(n: i32) -> Vec<Vec<u8>> {
        let mut out = Vec::<Vec<u8>>::with_capacity(n as usize);
        for _ in 0..n {
            let buf_len = rand::thread_rng().gen_range(0..50) as usize; // maximum length of arbitrary values
            let mut buf = vec![0u8; buf_len];
            // generate buf_len random bytes.
            OsRng.fill_bytes(&mut buf);
            out.push(buf.clone());
        }
        if out.len() != n as usize {
            panic!("wrong size slice: {}", n);
        }
        out
    }
}
