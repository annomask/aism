//! A lockable bitmap of series ids.
use std::{
    fmt::{self, Debug},
    io::{self, Write},
    iter::Iterator,
    mem, ptr,
    sync::Arc,
};

use crate::common::{hll::HLL, to_str};
use parking_lot::RwLock;
use roaring::RoaringBitmap;
use snafu::Snafu;

use crate::series::series_file::SeriesFile;

/// Error type for SeriesIDSet.
#[derive(Debug, Snafu)]
pub enum Error {
    /// IO error.
    #[snafu(display("io error: {}", reason))]
    IOErr { reason: io::Error },
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IOErr { reason: err }
    }
}

/// Result type for SeriesIndex.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// SeriesIDSet represens a lockable bitmap of series ids.
pub struct SeriesIDSet {
    pub bitmap: RwLock<RoaringBitmap>,
}

impl Default for SeriesIDSet {
    fn default() -> Self {
        Self::new(&[])
    }
}

impl PartialEq for SeriesIDSet {
    fn eq(&self, other: &Self) -> bool {
        let s = self.bitmap.data_ptr();
        let o = other.bitmap.data_ptr();
        unsafe { (*s).eq(&*o) }
    }
}

impl Debug for SeriesIDSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // f.debug_tuple("")
        //     .field(&self.longitude)
        //     .field(&self.latitude)
        //     .finish()
        // f.
        let bm = self.bitmap.data_ptr();
        unsafe { fmt::Debug::fmt(&*bm, f) }
    }
}

impl SeriesIDSet {
    /// Returns a new instance of SeriesIDSet.
    pub fn new(a: &[u64]) -> Self {
        let mut bm = RoaringBitmap::new();
        if !a.is_empty() {
            for id in a.iter() {
                bm.insert(*id as u32);
            }
        }

        Self {
            bitmap: RwLock::new(bm),
        }
    }

    /// Returns the number of distinct integers added to the set.
    pub fn len(&self) -> u64 {
        self.bitmap.read().len()
    }

    //// Returns `true` if there are no integers in this set.
    pub fn is_empty(&self) -> bool {
        self.bitmap.read().is_empty()
    }

    /// Estimates the memory footprint of this SeriesIDSet, in bytes.
    pub fn bytes(&self) -> i32 {
        let bm = self.bitmap.read();
        mem::size_of_val(self) as i32
    }

    /// Adds the series id to the set.
    /// Returns whether the value was absent from the set.
    pub fn add(&self, id: u64) -> bool {
        self.bitmap.write().insert(id as u32)
    }

    /// Adds the series id to the set. Add is not safe for use from multiple
    /// threads. Callers must manage synchronization.
    pub fn add_no_lock(&self, id: u64) -> bool {
        let bm = self.bitmap.data_ptr();
        unsafe { (*bm).insert(id as u32) }
    }

    /// Adds multiple ids to the SeriesIDSet. AddMany takes a lock, so may not be
    /// optimal to call many times with few ids.
    /// Returns the number of inserted values
    pub fn add_many(&self, ids: &[u64]) {
        if ids.is_empty() {
            return;
        }

        let mut bm = self.bitmap.write();
        for id in ids {
            bm.insert(*id as u32);
        }
    }

    /// Returns true if the id exists in the set.
    pub fn contains(&self, id: u64) -> bool {
        self.bitmap.read().contains(id as u32)
    }

    /// Returns true if the id exists in the set. ContainsNoLock is
    /// not safe for use from multiple goroutines. The caller must manage synchronization.
    pub fn contains_no_lock(&self, id: u64) -> bool {
        let bm = self.bitmap.data_ptr();
        unsafe { (*bm).contains(id as u32) }
    }

    /// Removs the id from the set.
    pub fn remove(&self, id: u64) -> bool {
        self.bitmap.write().remove(id as u32)
    }

    /// Removes the id from the set. RemoveNoLock is not safe for use
    /// from multiple threads. The caller must manage synchronization.
    pub fn remove_no_lock(&self, id: u64) -> bool {
        let bm = self.bitmap.data_ptr();
        unsafe { (*bm).remove(id as u32) }
    }

    /// Returns the cardinality(the number of distinct integers) of the SeriesIDSet.
    pub fn cardinality(&self) -> u64 {
        self.bitmap.read().len()
    }

    // TODO: optimize me!
    /// Merged the contents of others into s. The caller does not need to
    /// provide s as an argument, and the contents of s will always be present in s
    /// after Merge returns.
    pub fn merge(&self, others: &[&SeriesIDSet]) {
        let mut result = self.bitmap.read().clone();

        for other in others {
            let bm = other.bitmap.read();
            result |= &*bm;
        }

        *self.bitmap.write() = result;
    }

    // TODO: optimize me!
    /// Merges other into self, modifying self in the process.
    pub fn merge_in_place(&self, other: &SeriesIDSet) {
        if ptr::eq(self, other) {
            return;
        }

        let other = other.bitmap.read();
        *self.bitmap.write() |= &*other;
    }

    /// Returns true if other and self are the same set of ids.
    pub fn equals(&self, other: &SeriesIDSet) -> bool {
        if ptr::eq(self, other) {
            return true;
        }
        *self.bitmap.read() == *other.bitmap.read()
    }

    /// Returns a new SeriesIDSet containing elements that were present in self and other.
    pub fn and(&self, other: &SeriesIDSet) -> SeriesIDSet {
        let bm = &*self.bitmap.read() & &*other.bitmap.read();
        Self {
            bitmap: RwLock::new(bm),
        }
    }

    /// Returns a new SeriesIDSet containing elements that were present in self but not present in other.
    pub fn and_not(&self, other: &SeriesIDSet) -> SeriesIDSet {
        let bm = &*self.bitmap.read() - &*other.bitmap.read();
        Self {
            bitmap: RwLock::new(bm),
        }
    }

    /// Calls f for each id in the set. The function is applied to the IDs in ascending order.
    pub fn for_each<F>(&self, mut f: F)
    where
        F: FnMut(u64),
    {
        let bm = self.bitmap.read();
        for b in bm.iter() {
            f(b as u64);
        }
    }

    /// Calls f for each id in the set without taking a lock.
    pub fn for_each_no_lock<F>(&self, mut f: F)
    where
        F: FnMut(u64),
    {
        let bm = self.bitmap.data_ptr();
        unsafe {
            let i = (*bm).iter();
            for b in i {
                f(b as u64)
            }
        }
    }

    /// Removes from self any elements also present in other.
    pub fn diff(&self, other: &SeriesIDSet) {
        (*self.bitmap.write()) -= &*other.bitmap.read();
    }

    /// Checks whether two SeriesIDSet intersects, SeriesIDSet is not modified.
    pub fn intersects(&self, other: &SeriesIDSet) -> bool {
        self.bitmap.read().intersection_len(&other.bitmap.read()) != 0u64
    }

    /// Calls Clone without taking a lock.
    pub fn clone_no_lock(&self) -> SeriesIDSet {
        let bm = self.bitmap.data_ptr();
        unsafe {
            Self {
                bitmap: RwLock::new((*bm).clone()),
            }
        }
    }

    /// Rreturns an iterator to the underlying bitmap.
    /// This iterator is not protected by a lock.
    pub fn iterator_no_lock(&self) -> impl Iterator<Item = u32> + use<> {
        let bm = self.bitmap.data_ptr();
        unsafe { (*bm).iter() }
    }

    /// Unmarshals data into the set.
    pub fn unmarshal_binary(&self, data: &[u8]) -> Result<()> {
        let mut bm = self.bitmap.write();
        *bm = RoaringBitmap::deserialize_from(data)?;
        Ok(())
    }

    /// Unmarshals data into the set.
    /// References to the underlying data are used so data should not be reused by caller.
    pub fn unmarshal_binary_unsafe(&self, data: &[u8]) -> Result<()> {
        let mut bm = self.bitmap.write();
        *bm = RoaringBitmap::deserialize_unchecked_from(data)?;
        Ok(())
    }

    /// Writes a serialized version of this bitmap to w.
    /// Returning how many bytes were written.
    pub fn write_to(&self, w: &mut impl Write) -> Result<usize> {
        let mut bytes = vec![];
        self.bitmap.read().serialize_into(&mut bytes)?;
        Ok(w.write(&bytes[..])?)
    }

    /// Clears the underlying bitmap for re-use. Clear is safe for use by multiple goroutines.
    pub fn clear(&self) {
        self.bitmap.write().clear();
    }

    /// Clears the underlying bitmap for re-use without taking a lock.
    pub fn clear_no_lock(&self) {
        let bm = self.bitmap.data_ptr();
        unsafe {
            (*bm).clear();
        }
    }

    /// Returns a slice of series ids.
    pub fn slice(&self) -> Vec<u64> {
        let bm = self.bitmap.read();
        let mut a = Vec::<u64>::with_capacity(self.cardinality() as usize);
        for id in bm.iter() {
            a.push(id as u64);
        }
        a
    }
}

impl std::fmt::Display for SeriesIDSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut buf = vec![];
        let _ = buf.write(&b"{"[..]).unwrap();

        let mut counter = 0;
        let bm = self.bitmap.read();
        let mut i = bm.iter();
        if let Some(id) = i.next() {
            counter += 1;
            let _ = buf.write((id as i64).to_string().as_bytes()).unwrap();
        }
        // while let Some(id) = i.next() {
        for id in i {
            let _ = buf.write(&b","[..]).unwrap();
            counter += 1;
            // to avoid exhausting the memory
            if counter > 0x40000 {
                let _ = buf.write(&b"..."[..]).unwrap();
                break;
            }
            let _ = buf.write((id as i64).to_string().as_bytes()).unwrap();
        }
        let _ = buf.write(&b"}"[..]).unwrap();

        write!(f, "{}", to_str!(&buf))
    }
}

// impl Iterator for SeriesIDSet {
//     type Item = u32;

//     fn next(&mut self) -> Option<Self::Item> {
//         let bm = self.bitmap.data_ptr();
//         unsafe {
//             (&*bm).iter().next()
//         }
//     }
// }

// impl Iterator<'a> for SeriesIDSetIterator<'a> {
//     type Item = u32;

//     fn next(&mut self) -> Option<Self::Item> {
//         if self.bitmap.is_empty() || self.iter_i == self.bitmap.len() {
//             return None;
//         }
//         let curr_iter_i = self.iter_i;
//         self.iter_i += 1;

//         Some(self.ids[curr_iter_i])
//     }
// }

// pub struct SeriesIDSetIterator<'a> {
//     bitmap: &RoaringBitmap,
//     iter_i: usize,
// }

// impl SeriesIDSetIterator<'a> {
//     fn new(bitmap: &RoaringBitmap) -> SeriesIDSetIterator {
//         Self {
//             bitmap,
//             iter_i: 0usize,
//         }
//     }
// }

impl Clone for SeriesIDSet {
    /// Returns a new SeriesIDSet with a deep copy of the underlying bitmap under lock.
    fn clone(&self) -> Self {
        let bm = self.bitmap.read();
        Self {
            bitmap: RwLock::new(bm.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        thread,
    };

    use crate::series::{series_partition::Series, series_set::*};

    #[test]
    fn test_series_id_set_and_not() {
        let examples = vec![
            vec![vec![1, 10, 20, 30], vec![10, 12, 13, 14, 20], vec![1, 30]],
            vec![vec![], vec![10], vec![]],
            vec![vec![1, 10, 20, 30], vec![1, 10, 20, 30], vec![]],
            vec![vec![1, 10], vec![], vec![1, 10]],
        ];

        for example in &examples {
            let (a, b) = (
                SeriesIDSet::new(&[] as &[u64]),
                SeriesIDSet::new(&[] as &[u64]),
            );
            for v in &example[0] {
                a.add(*v);
            }
            for v in &example[1] {
                b.add(*v);
            }
            let expected = SeriesIDSet::new(&[] as &[u64]);
            for v in &example[2] {
                expected.add(*v);
            }
            let got = a.and_not(&b);
            let got_string = format!("{}", got);
            let expected_string = format!("{}", expected);
            assert_eq!(
                got_string, expected_string,
                "got {}, expected {}",
                got, expected
            );
        }
    }

    // Ensure that cloning in race-free
    #[test]
    fn test_series_id_set_clone_race() {
        let (main, total) = (
            SeriesIDSet::new(&[] as &[u64]),
            SeriesIDSet::new(&[] as &[u64]),
        );
        for i in 0u64..1024u64 {
            assert!(&main.add_no_lock(i));
            assert!(&total.add_no_lock(i));
        }

        // One test with a closure around the main SeriesIDSet,
        // so that we can run a subtest with and without COW.
        let n = 10 * (num_cpus::get() + 1);
        let mut clones = Vec::<SeriesIDSet>::new();
        let mut handles = Vec::new();

        let main = Arc::new(main);
        let total = Arc::new(total);
        for i in 1..=n {
            // let clones = clones.clone();
            let main = main.clone();
            let total = total.clone();
            handles.push(thread::spawn(move || {
                let cloned = main.as_ref().clone();
                for j in 0..1000 {
                    let id = (j + (100000 * i)) as u64;
                    assert!(total.add(id));
                    assert!(cloned.add_no_lock(id));
                }
                cloned
            }));
        }

        for h in handles {
            clones.push(h.join().unwrap());
        }

        for o in clones.iter() {
            let (got, exp) = (o.cardinality(), 2024u64);
            assert_eq!(got, exp, "got cardinality {}, expected {}", got, exp);
        }

        // The original set should be unaffected.
        let (got, exp) = (main.cardinality(), 1024u64);
        assert_eq!(got, exp, "got cardinality {}, expected {}", got, exp);

        // Merging the clones should result in only 1024 shared values.
        let union = SeriesIDSet::new(&[] as &[u64]);
        for o in clones.iter() {
            o.for_each_no_lock(|id: u64| {
                union.add_no_lock(id);
            });
        }

        assert!(union.equals(&total), "union not equal to total");
    }
}
