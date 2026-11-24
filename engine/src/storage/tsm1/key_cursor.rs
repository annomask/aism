#[cfg(feature = "slow-trace")]
use std::{collections::HashSet, time::Instant};
use std::{io::BufRead, sync::Arc};

use crate::models::points::{
    BoolValue, FloatValue, IntegerValue, StringValue, UnsignedValue, Value,
};
use parking_lot::RwLock;

use super::{
    reader::{Result, TSMReader, TimeRange},
    writer::IndexEntry,
};
#[cfg(feature = "slow-trace")]
use crate::query::storage_ir::{GroupedPhysicalBlockIdentity, StorageScanProfile};
use crate::storage::tsm1::{
    data_encoding::encoding_values::{
        BoolValues, FloatValues, IntegerValues, StringValues, UnsignedValues,
    },
    vlog::{VLogIndex, vlog_index_ring::VLogIndexes},
};

macro_rules! read_profiled_block {
    ($profile:ident, $location:expr, $method:ident) => {{
        #[cfg(feature = "slow-trace")]
        let started = Instant::now();
        let values = $location.r.$method(&$location.entry)?;
        #[cfg(feature = "slow-trace")]
        if let Some(profile) = &$profile {
            profile.record_block_decode(
                GroupedPhysicalBlockIdentity::new(
                    $location.r.path().to_string_lossy(),
                    $location.entry.offset,
                    $location.entry.min_time,
                    $location.entry.max_time,
                ),
                values.len() as u64,
                u64::from($location.entry.size),
                started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
                false,
            );
        }
        values
    }};
}

/// KeyCursor allows iteration through keys in a set of files within a FileStore.
pub struct KeyCursor {
    key: Vec<u8>,

    // seeks is all the file locations that we need to return during iteration.
    seeks: Vec<Arc<RwLock<Location>>>,

    // current is the set of blocks possibly containing the next set of points.
    // Normally this is just one entry, but there may be multiple if points have
    // been overwritten.
    current: Vec<Arc<RwLock<Location>>>,

    buf: Vec<Value>,

    // pos is the index within seeks. It is the data block index.
    // Based on ascending, it will increment or
    // decrement through the size of seeks slice.
    pub pos: usize,
    ascending: bool,
    #[cfg(feature = "slow-trace")]
    profile: Option<Arc<StorageScanProfile>>,
}

impl KeyCursor {
    /// Returns a new instance of KeyCursor.
    /// This function assumes the read-lock has been taken.
    pub fn new(files: &[Arc<TSMReader>], key: &[u8], t: i64, ascending: bool) -> Self {
        let mut c = Self {
            key: key.to_vec(),
            seeks: locations(files, key, t, ascending),
            current: vec![],
            buf: vec![],
            pos: 0,
            ascending,
            #[cfg(feature = "slow-trace")]
            profile: None,
        };

        if ascending {
            c.seeks.sort_unstable_by(|a, b| {
                let (a, b) = (a.read(), b.read());
                if a.entry
                    .overlaps_time_range(b.entry.min_time, b.entry.max_time)
                {
                    return a.r.path().cmp(&b.r.path());
                }
                a.entry.min_time.cmp(&b.entry.min_time)
            });
        } else {
            c.seeks.sort_unstable_by(|a, b| {
                let (a, b) = (a.read(), b.read());
                if a.entry
                    .overlaps_time_range(b.entry.min_time, b.entry.max_time)
                {
                    return a.r.path().cmp(&b.r.path());
                }
                a.entry.max_time.cmp(&b.entry.max_time)
            });
        }

        // Determine the distinct set of TSM files in use and mark then as in-use
        for f in c.seeks.iter() {
            f.read().r.ref_reader();
        }

        c.seek(t);

        c
    }

    #[cfg(feature = "slow-trace")]
    pub(crate) fn attach_profile(&mut self, profile: Arc<StorageScanProfile>) {
        let files = self
            .seeks
            .iter()
            .map(|location| {
                let location = location.read();
                Arc::as_ptr(&location.r) as usize
            })
            .collect::<HashSet<_>>()
            .len() as u64;
        profile.record_cursor(files, self.seeks.len() as u64);
        profile.record_cursor_seek();
        self.profile = Some(profile);
    }

    /// Removes all references on the cursor.
    pub fn close(&mut self) {
        // Remove all of our in-use references since we're done
        for f in self.seeks.iter() {
            f.read().r.unref_reader();
        }

        self.buf = vec![];
        self.seeks = vec![];
        self.current = vec![];
    }

    pub fn seek(&mut self, t: i64) {
        if self.seeks.is_empty() {
            return;
        }

        self.current = vec![];

        if self.ascending {
            self.seek_ascending(t);
        } else {
            self.seek_descending(t);
        }
    }

    fn seek_ascending(&mut self, t: i64) {
        for (i, e) in self.seeks.iter().enumerate() {
            let l = e.read();
            if t < l.entry.min_time || l.entry.contains(t) {
                // Record the position of the first block matching our seek time
                if self.current.is_empty() {
                    self.pos = i;
                }

                self.current.push(Arc::clone(e));
            }
        }
    }

    fn seek_descending(&mut self, t: i64) {
        for i in (0..self.seeks.len()).rev() {
            let l = &self.seeks[i].read();
            if t > l.entry.max_time || l.entry.contains(t) {
                // Record the position of the first block matching our seek time
                if self.current.is_empty() {
                    self.pos = i;
                }

                self.current.push(Arc::clone(&self.seeks[i]));
            }
        }
    }

    /// Moves the cursor to the next position.
    /// Data should be read by the ReadBlock functions.
    pub fn next(&mut self) {
        if self.current.is_empty() {
            return;
        }
        // Do we still have unread values in the current block
        if !self.current[0].read().read() {
            return;
        }
        self.current.clear();
        if self.ascending {
            self.next_ascending();
        } else {
            self.next_descending();
        }
    }

    fn next_ascending(&mut self) {
        loop {
            self.pos += 1;
            if self.pos >= self.seeks.len() {
                return;
            } else if !self.seeks[self.pos].read().read() {
                break;
            }
        }

        // Append the first matching block
        if self.current.is_empty() {
            self.current.push(Arc::clone(&self.seeks[self.pos]));
        } else {
            self.current.truncate(1);
            self.current[0] = Arc::clone(&self.seeks[self.pos]);
        }

        // If we have ovelapping blocks, append all their values so we can dedup
        // for l in self.seeks.iter().skip(self.pos + 1) {
        for i in self.pos + 1..self.seeks.len() {
            let l = &self.seeks[i];
            if l.read().read() {
                continue;
            }

            self.current.push(Arc::clone(l));
        }
    }

    fn next_descending(&mut self) {
        loop {
            if self.pos == 0 {
                return;
            }

            self.pos -= 1;
            if !self.seeks[self.pos].read().read() {
                break;
            }
        }

        // Append the first matching block
        if self.current.is_empty() {
            self.current.push(Arc::clone(&self.seeks[self.pos]));
        } else {
            self.current.truncate(1);
            self.current[0] = Arc::clone(&self.seeks[self.pos]);
        }

        // If we have ovelapping blocks, append all their values so we can dedup
        for l in self.seeks.as_slice()[..self.pos + 1].iter().rev() {
            if l.read().read() {
                continue;
            }

            self.current.push(Arc::clone(l));
        }
    }

    /// Reads the next block as a set of integer values.
    pub fn read_integer_block(&mut self) -> Result<Vec<IntegerValue>> {
        #[cfg(feature = "slow-trace")]
        let profile = self.profile.clone();
        loop {
            // No matching blocks to decode
            if self.current.is_empty() {
                return Ok(vec![]);
            }

            // First block is the oldest block containing the points we're searching for.
            let first = Arc::clone(&self.current[0]);
            let mut first = first.write();
            let mut values =
                IntegerValues(read_profiled_block!(profile, first, read_integer_block_at));

            // Remove values we already read
            values.exclude(first.read_min, first.read_max);

            // Remove any tombstones
            let tombstones = first.r.tombstone_range(&self.key);
            exclude_tombstones_integer_values(tombstones.as_slice(), &mut values);

            // If there are no values in this first block (all tombstoned or previously read) and
            // we have more potential blocks too search.  Try again.
            if values.is_empty() && !self.current.is_empty() {
                self.current.drain(..1);
                continue;
            }

            // Only one block with this key and time range so return it
            if self.current.len() == 1 {
                if !values.is_empty() {
                    first.mark_read(values.min_time(), values.max_time());
                }
                return Ok(values.into());
            }

            // Use the current block time range as our overlapping window
            let (mut min_t, mut max_t) = (first.read_min, first.read_max);

            if !values.is_empty() {
                min_t = values.min_time();
                max_t = values.max_time();
            }

            drop(first);

            if self.ascending {
                // Blocks are ordered by generation, we may have values in the past in later blocks, if so,
                // expand the window to include the min time range to ensure values are returned in ascending
                // order
                for cur in self.current.iter().skip(1) {
                    let cur = cur.read();
                    if cur.entry.min_time < min_t && !cur.read() {
                        min_t = cur.entry.min_time;
                    }
                }

                // Find first block that overlaps our window
                for cur in self.current.iter().skip(1) {
                    let cur = cur.read();
                    if cur.entry.overlaps_time_range(min_t, max_t) && !cur.read() {
                        // Shrink our window so it's the intersection of the first overlapping block and the
                        // first block.  We do this to minimize the region that overlaps and needs to
                        // be merged.
                        if cur.entry.max_time > max_t {
                            max_t = cur.entry.max_time;
                        }
                        values.include(min_t, max_t);
                        break;
                    }
                }

                // Search the remaining blocks that overlap our window and append their values so we can
                // merge them.
                for cur in self.current.iter_mut().skip(1) {
                    let mut cur = cur.write();
                    // Skip this block if it doesn't contain points we looking for or they have already been read
                    if !cur.entry.overlaps_time_range(min_t, max_t) || cur.read() {
                        cur.mark_read(min_t, max_t);
                        continue;
                    }

                    let mut v =
                        IntegerValues(read_profiled_block!(profile, cur, read_integer_block_at));
                    let tombstones = cur.r.tombstone_range(&self.key);
                    // Remove any tombstoned values
                    exclude_tombstones_integer_values(&tombstones, &mut v);

                    // Remove values we already read
                    v.exclude(cur.read_min, cur.read_max);

                    if !v.is_empty() {
                        // Only use values in the overlapping window
                        v.include(min_t, max_t);

                        // Merge the remaining values with the existing
                        values.merge(v);
                    }
                    cur.mark_read(min_t, max_t);
                }
            } else {
                // Blocks are ordered by generation, we may have values in the past in later blocks, if so,
                // expand the window to include the max time range to ensure values are returned in descending
                // order
                for cur in self.current.iter().skip(1) {
                    let cur = cur.read();
                    if cur.entry.max_time > max_t && !cur.read() {
                        max_t = cur.entry.max_time;
                    }
                }

                // Find first block that overlaps our window
                for cur in self.current.iter().skip(1) {
                    let cur = cur.read();
                    if cur.entry.overlaps_time_range(min_t, max_t) && !cur.read() {
                        // Shrink our window so it's the intersection of the first overlapping block and the
                        // first block.  We do this to minimize the region that overlaps and needs to
                        // be merged.
                        if cur.entry.min_time < min_t {
                            min_t = cur.entry.min_time;
                        }
                        values.include(min_t, max_t);
                        break;
                    }
                }

                // Search the remaining blocks that overlap our window and append their values so we can
                // merge them.
                for cur in self.current.iter_mut().skip(1) {
                    let mut cur = cur.write();
                    // Skip this block if it doesn't contain points we looking for or they have already been read
                    if !cur.entry.overlaps_time_range(min_t, max_t) || cur.read() {
                        cur.mark_read(min_t, max_t);
                        continue;
                    }

                    let mut v =
                        IntegerValues(read_profiled_block!(profile, cur, read_integer_block_at));
                    let tombstones = cur.r.tombstone_range(&self.key);
                    // Remove any tombstoned values
                    exclude_tombstones_integer_values(tombstones.as_slice(), &mut v);

                    // Remove values we already read
                    v.exclude(cur.read_min, cur.read_max);

                    // If the block we decoded should have all of it's values included, mark it as read so we
                    // don't use it again.
                    if !v.is_empty() {
                        v.include(min_t, max_t);
                        // Merge the remaining values with the existing
                        v.merge(values);
                        values = v;
                    }
                    cur.mark_read(min_t, max_t);
                }
            } // end dor if self.ascending

            let mut first = self.current[0].write();
            first.mark_read(min_t, max_t);

            return Ok(values.into());
        }
    }

    /// Reads the next block as a set of unsigned values.
    pub fn read_unsigned_block(&mut self) -> Result<Vec<UnsignedValue>> {
        #[cfg(feature = "slow-trace")]
        let profile = self.profile.clone();
        loop {
            // No matching blocks to decode
            if self.current.is_empty() {
                return Ok(vec![]);
            }

            // First block is the oldest block containing the points we're searching for.
            let first = Arc::clone(&self.current[0]);
            let mut first = first.write();
            let mut values =
                UnsignedValues(read_profiled_block!(profile, first, read_unsigned_block_at));

            // Remove values we already read
            values.exclude(first.read_min, first.read_max);

            // Remove any tombstones
            let tombstones = first.r.tombstone_range(&self.key);
            exclude_tombstones_unsigned_values(tombstones.as_slice(), &mut values);

            // If there are no values in this first block (all tombstoned or previously read) and
            // we have more potential blocks too search.  Try again.
            if values.is_empty() && !self.current.is_empty() {
                self.current.drain(..1);
                continue;
            }

            // Only one block with this key and time range so return it
            if self.current.len() == 1 {
                if !values.is_empty() {
                    first.mark_read(values.min_time(), values.max_time());
                }
                return Ok(values.into());
            }

            // Use the current block time range as our overlapping window
            let (mut min_t, mut max_t) = (first.read_min, first.read_max);

            if !values.is_empty() {
                min_t = values.min_time();
                max_t = values.max_time();
            }

            drop(first);

            if self.ascending {
                // Blocks are ordered by generation, we may have values in the past in later blocks, if so,
                // expand the window to include the min time range to ensure values are returned in ascending
                // order
                for cur in self.current.iter().skip(1) {
                    let cur = cur.read();
                    if cur.entry.min_time < min_t && !cur.read() {
                        min_t = cur.entry.min_time;
                    }
                }

                // Find first block that overlaps our window
                for cur in self.current.iter().skip(1) {
                    let cur = cur.read();
                    if cur.entry.overlaps_time_range(min_t, max_t) && !cur.read() {
                        // Shrink our window so it's the intersection of the first overlapping block and the
                        // first block.  We do this to minimize the region that overlaps and needs to
                        // be merged.
                        if cur.entry.max_time > max_t {
                            max_t = cur.entry.max_time;
                        }
                        values.include(min_t, max_t);
                        break;
                    }
                }

                // Search the remaining blocks that overlap our window and append their values so we can
                // merge them.
                for cur in self.current.iter_mut().skip(1) {
                    let mut cur = cur.write();
                    // Skip this block if it doesn't contain points we looking for or they have already been read
                    if !cur.entry.overlaps_time_range(min_t, max_t) || cur.read() {
                        cur.mark_read(min_t, max_t);
                        continue;
                    }

                    let mut v =
                        UnsignedValues(read_profiled_block!(profile, cur, read_unsigned_block_at));
                    let tombstones = cur.r.tombstone_range(&self.key);
                    // Remove any tombstoned values
                    exclude_tombstones_unsigned_values(&tombstones, &mut v);

                    // Remove values we already read
                    v.exclude(cur.read_min, cur.read_max);

                    if !v.is_empty() {
                        // Only use values in the overlapping window
                        v.include(min_t, max_t);

                        // Merge the remaining values with the existing
                        values.merge(v);
                    }
                    cur.mark_read(min_t, max_t);
                }
            } else {
                // Blocks are ordered by generation, we may have values in the past in later blocks, if so,
                // expand the window to include the max time range to ensure values are returned in descending
                // order
                for cur in self.current.iter().skip(1) {
                    let cur = cur.read();
                    if cur.entry.max_time > max_t && !cur.read() {
                        max_t = cur.entry.max_time;
                    }
                }

                // Find first block that overlaps our window
                for cur in self.current.iter().skip(1) {
                    let cur = cur.read();
                    if cur.entry.overlaps_time_range(min_t, max_t) && !cur.read() {
                        // Shrink our window so it's the intersection of the first overlapping block and the
                        // first block.  We do this to minimize the region that overlaps and needs to
                        // be merged.
                        if cur.entry.min_time < min_t {
                            min_t = cur.entry.min_time;
                        }
                        values.include(min_t, max_t);
                        break;
                    }
                }

                // Search the remaining blocks that overlap our window and append their values so we can
                // merge them.
                for cur in self.current.iter_mut().skip(1) {
                    let mut cur = cur.write();
                    // Skip this block if it doesn't contain points we looking for or they have already been read
                    if !cur.entry.overlaps_time_range(min_t, max_t) || cur.read() {
                        cur.mark_read(min_t, max_t);
                        continue;
                    }

                    let mut v =
                        UnsignedValues(read_profiled_block!(profile, cur, read_unsigned_block_at));
                    let tombstones = cur.r.tombstone_range(&self.key);
                    // Remove any tombstoned values
                    exclude_tombstones_unsigned_values(tombstones.as_slice(), &mut v);

                    // Remove values we already read
                    v.exclude(cur.read_min, cur.read_max);

                    // If the block we decoded should have all of it's values included, mark it as read so we
                    // don't use it again.
                    if !v.is_empty() {
                        v.include(min_t, max_t);
                        // Merge the remaining values with the existing
                        v.merge(values);
                        values = v;
                    }
                    cur.mark_read(min_t, max_t);
                }
            } // end dor if self.ascending

            let mut first = self.current[0].write();
            first.mark_read(min_t, max_t);

            return Ok(values.into());
        }
    }

    /// Reads the next block as a set of float values.
    pub fn read_float_block(&mut self) -> Result<Vec<FloatValue>> {
        #[cfg(feature = "slow-trace")]
        let profile = self.profile.clone();
        loop {
            // No matching blocks to decode
            if self.current.is_empty() {
                return Ok(vec![]);
            }

            // First block is the oldest block containing the points we're searching for.
            let first = Arc::clone(&self.current[0]);
            let mut first = first.write();
            let mut values = FloatValues(read_profiled_block!(profile, first, read_float_block_at));

            // Remove values we already read
            values.exclude(first.read_min, first.read_max);

            // Remove any tombstones
            let tombstones = first.r.tombstone_range(&self.key);
            exclude_tombstones_float_values(tombstones.as_slice(), &mut values);

            // If there are no values in this first block (all tombstoned or previously read) and
            // we have more potential blocks too search.  Try again.
            if values.is_empty() && !self.current.is_empty() {
                self.current.drain(..1);
                continue;
            }

            // Only one block with this key and time range so return it
            if self.current.len() == 1 {
                if !values.is_empty() {
                    first.mark_read(values.min_time(), values.max_time());
                }
                return Ok(values.into());
            }

            // Use the current block time range as our overlapping window
            let (mut min_t, mut max_t) = (first.read_min, first.read_max);

            if !values.is_empty() {
                min_t = values.min_time();
                max_t = values.max_time();
            }

            drop(first);

            if self.ascending {
                // Blocks are ordered by generation, we may have values in the past in later blocks, if so,
                // expand the window to include the min time range to ensure values are returned in ascending
                // order
                for cur in self.current.iter().skip(1) {
                    let cur = cur.read();
                    if cur.entry.min_time < min_t && !cur.read() {
                        min_t = cur.entry.min_time;
                    }
                }

                // Find first block that overlaps our window
                for cur in self.current.iter().skip(1) {
                    let cur = cur.read();
                    if cur.entry.overlaps_time_range(min_t, max_t) && !cur.read() {
                        // Shrink our window so it's the intersection of the first overlapping block and the
                        // first block.  We do this to minimize the region that overlaps and needs to
                        // be merged.
                        if cur.entry.max_time > max_t {
                            max_t = cur.entry.max_time;
                        }
                        values.include(min_t, max_t);
                        break;
                    }
                }

                // Search the remaining blocks that overlap our window and append their values so we can
                // merge them.
                for cur in self.current.iter_mut().skip(1) {
                    let mut cur = cur.write();
                    // Skip this block if it doesn't contain points we looking for or they have already been read
                    if !cur.entry.overlaps_time_range(min_t, max_t) || cur.read() {
                        cur.mark_read(min_t, max_t);
                        continue;
                    }

                    let mut v =
                        FloatValues(read_profiled_block!(profile, cur, read_float_block_at));
                    let tombstones = cur.r.tombstone_range(&self.key);
                    // Remove any tombstoned values
                    exclude_tombstones_float_values(&tombstones, &mut v);

                    // Remove values we already read
                    v.exclude(cur.read_min, cur.read_max);

                    if !v.is_empty() {
                        // Only use values in the overlapping window
                        v.include(min_t, max_t);

                        // Merge the remaining values with the existing
                        values.merge(v);
                    }
                    cur.mark_read(min_t, max_t);
                }
            } else {
                // Blocks are ordered by generation, we may have values in the past in later blocks, if so,
                // expand the window to include the max time range to ensure values are returned in descending
                // order
                for cur in self.current.iter().skip(1) {
                    let cur = cur.read();
                    if cur.entry.max_time > max_t && !cur.read() {
                        max_t = cur.entry.max_time;
                    }
                }

                // Find first block that overlaps our window
                for cur in self.current.iter().skip(1) {
                    let cur = cur.read();
                    if cur.entry.overlaps_time_range(min_t, max_t) && !cur.read() {
                        // Shrink our window so it's the intersection of the first overlapping block and the
                        // first block.  We do this to minimize the region that overlaps and needs to
                        // be merged.
                        if cur.entry.min_time < min_t {
                            min_t = cur.entry.min_time;
                        }
                        values.include(min_t, max_t);
                        break;
                    }
                }

                // Search the remaining blocks that overlap our window and append their values so we can
                // merge them.
                for cur in self.current.iter_mut().skip(1) {
                    let mut cur = cur.write();
                    // Skip this block if it doesn't contain points we looking for or they have already been read
                    if !cur.entry.overlaps_time_range(min_t, max_t) || cur.read() {
                        cur.mark_read(min_t, max_t);
                        continue;
                    }

                    let mut v =
                        FloatValues(read_profiled_block!(profile, cur, read_float_block_at));
                    let tombstones = cur.r.tombstone_range(&self.key);
                    // Remove any tombstoned values
                    exclude_tombstones_float_values(tombstones.as_slice(), &mut v);

                    // Remove values we already read
                    v.exclude(cur.read_min, cur.read_max);

                    // If the block we decoded should have all of it's values included, mark it as read so we
                    // don't use it again.
                    if !v.is_empty() {
                        v.include(min_t, max_t);
                        // Merge the remaining values with the existing
                        v.merge(values);
                        values = v;
                    }
                    cur.mark_read(min_t, max_t);
                }
            } // end dor if self.ascending

            let mut first = self.current[0].write();
            first.mark_read(min_t, max_t);

            return Ok(values.into());
        }
    }

    /// Reads the next block as a set of bool values.
    pub fn read_bool_block(&mut self) -> Result<Vec<BoolValue>> {
        #[cfg(feature = "slow-trace")]
        let profile = self.profile.clone();
        loop {
            // No matching blocks to decode
            if self.current.is_empty() {
                return Ok(vec![]);
            }

            // First block is the oldest block containing the points we're searching for.
            let first = Arc::clone(&self.current[0]);
            let mut first = first.write();
            let mut values = BoolValues(read_profiled_block!(profile, first, read_bool_block_at));

            // Remove values we already read
            values.exclude(first.read_min, first.read_max);

            // Remove any tombstones
            let tombstones = first.r.tombstone_range(&self.key);
            exclude_tombstones_bool_values(tombstones.as_slice(), &mut values);

            // If there are no values in this first block (all tombstoned or previously read) and
            // we have more potential blocks too search.  Try again.
            if values.is_empty() && !self.current.is_empty() {
                self.current.drain(..1);
                continue;
            }

            // Only one block with this key and time range so return it
            if self.current.len() == 1 {
                if !values.is_empty() {
                    first.mark_read(values.min_time(), values.max_time());
                }
                return Ok(values.into());
            }

            // Use the current block time range as our overlapping window
            let (mut min_t, mut max_t) = (first.read_min, first.read_max);

            if !values.is_empty() {
                min_t = values.min_time();
                max_t = values.max_time();
            }

            drop(first);

            if self.ascending {
                // Blocks are ordered by generation, we may have values in the past in later blocks, if so,
                // expand the window to include the min time range to ensure values are returned in ascending
                // order
                for cur in self.current.iter().skip(1) {
                    let cur = cur.read();
                    if cur.entry.min_time < min_t && !cur.read() {
                        min_t = cur.entry.min_time;
                    }
                }

                // Find first block that overlaps our window
                for cur in self.current.iter().skip(1) {
                    let cur = cur.read();
                    if cur.entry.overlaps_time_range(min_t, max_t) && !cur.read() {
                        // Shrink our window so it's the intersection of the first overlapping block and the
                        // first block.  We do this to minimize the region that overlaps and needs to
                        // be merged.
                        if cur.entry.max_time > max_t {
                            max_t = cur.entry.max_time;
                        }
                        values.include(min_t, max_t);
                        break;
                    }
                }

                // Search the remaining blocks that overlap our window and append their values so we can
                // merge them.
                for cur in self.current.iter_mut().skip(1) {
                    let mut cur = cur.write();
                    // Skip this block if it doesn't contain points we looking for or they have already been read
                    if !cur.entry.overlaps_time_range(min_t, max_t) || cur.read() {
                        cur.mark_read(min_t, max_t);
                        continue;
                    }

                    let mut v = BoolValues(read_profiled_block!(profile, cur, read_bool_block_at));
                    let tombstones = cur.r.tombstone_range(&self.key);
                    // Remove any tombstoned values
                    exclude_tombstones_bool_values(&tombstones, &mut v);

                    // Remove values we already read
                    v.exclude(cur.read_min, cur.read_max);

                    if !v.is_empty() {
                        // Only use values in the overlapping window
                        v.include(min_t, max_t);

                        // Merge the remaining values with the existing
                        values.merge(v);
                    }
                    cur.mark_read(min_t, max_t);
                }
            } else {
                // Blocks are ordered by generation, we may have values in the past in later blocks, if so,
                // expand the window to include the max time range to ensure values are returned in descending
                // order
                for cur in self.current.iter().skip(1) {
                    let cur = cur.read();
                    if cur.entry.max_time > max_t && !cur.read() {
                        max_t = cur.entry.max_time;
                    }
                }

                // Find first block that overlaps our window
                for cur in self.current.iter().skip(1) {
                    let cur = cur.read();
                    if cur.entry.overlaps_time_range(min_t, max_t) && !cur.read() {
                        // Shrink our window so it's the intersection of the first overlapping block and the
                        // first block.  We do this to minimize the region that overlaps and needs to
                        // be merged.
                        if cur.entry.min_time < min_t {
                            min_t = cur.entry.min_time;
                        }
                        values.include(min_t, max_t);
                        break;
                    }
                }

                // Search the remaining blocks that overlap our window and append their values so we can
                // merge them.
                for cur in self.current.iter_mut().skip(1) {
                    let mut cur = cur.write();
                    // Skip this block if it doesn't contain points we looking for or they have already been read
                    if !cur.entry.overlaps_time_range(min_t, max_t) || cur.read() {
                        cur.mark_read(min_t, max_t);
                        continue;
                    }

                    let mut v = BoolValues(read_profiled_block!(profile, cur, read_bool_block_at));
                    let tombstones = cur.r.tombstone_range(&self.key);
                    // Remove any tombstoned values
                    exclude_tombstones_bool_values(tombstones.as_slice(), &mut v);

                    // Remove values we already read
                    v.exclude(cur.read_min, cur.read_max);

                    // If the block we decoded should have all of it's values included, mark it as read so we
                    // don't use it again.
                    if !v.is_empty() {
                        v.include(min_t, max_t);
                        // Merge the remaining values with the existing
                        v.merge(values);
                        values = v;
                    }
                    cur.mark_read(min_t, max_t);
                }
            } // end dor if self.ascending

            let mut first = self.current[0].write();
            first.mark_read(min_t, max_t);

            return Ok(values.into());
        }
    }

    /// Reads the next block as a set of string values.
    pub fn read_string_block(&mut self) -> Result<Vec<StringValue>> {
        #[cfg(feature = "slow-trace")]
        let profile = self.profile.clone();
        loop {
            // No matching blocks to decode
            if self.current.is_empty() {
                return Ok(vec![]);
            }

            // First block is the oldest block containing the points we're searching for.
            let first = Arc::clone(&self.current[0]);
            let mut first = first.write();
            let mut values =
                StringValues(read_profiled_block!(profile, first, read_string_block_at));

            // Remove values we already read
            values.exclude(first.read_min, first.read_max);

            // Remove any tombstones
            let tombstones = first.r.tombstone_range(&self.key);
            exclude_tombstones_string_values(tombstones.as_slice(), &mut values);

            // If there are no values in this first block (all tombstoned or previously read) and
            // we have more potential blocks too search.  Try again.
            if values.is_empty() && !self.current.is_empty() {
                self.current.drain(..1);
                continue;
            }

            // Only one block with this key and time range so return it
            if self.current.len() == 1 {
                if !values.is_empty() {
                    first.mark_read(values.min_time(), values.max_time());
                }
                return Ok(values.into());
            }

            // Use the current block time range as our overlapping window
            let (mut min_t, mut max_t) = (first.read_min, first.read_max);

            if !values.is_empty() {
                min_t = values.min_time();
                max_t = values.max_time();
            }

            drop(first);

            if self.ascending {
                // Blocks are ordered by generation, we may have values in the past in later blocks, if so,
                // expand the window to include the min time range to ensure values are returned in ascending
                // order
                for cur in self.current.iter().skip(1) {
                    let cur = cur.read();
                    if cur.entry.min_time < min_t && !cur.read() {
                        min_t = cur.entry.min_time;
                    }
                }

                // Find first block that overlaps our window
                for cur in self.current.iter().skip(1) {
                    let cur = cur.read();
                    if cur.entry.overlaps_time_range(min_t, max_t) && !cur.read() {
                        // Shrink our window so it's the intersection of the first overlapping block and the
                        // first block.  We do this to minimize the region that overlaps and needs to
                        // be merged.
                        if cur.entry.max_time > max_t {
                            max_t = cur.entry.max_time;
                        }
                        values.include(min_t, max_t);
                        break;
                    }
                }

                // Search the remaining blocks that overlap our window and append their values so we can
                // merge them.
                for cur in self.current.iter_mut().skip(1) {
                    let mut cur = cur.write();
                    // Skip this block if it doesn't contain points we looking for or they have already been read
                    if !cur.entry.overlaps_time_range(min_t, max_t) || cur.read() {
                        cur.mark_read(min_t, max_t);
                        continue;
                    }

                    let mut v =
                        StringValues(read_profiled_block!(profile, cur, read_string_block_at));
                    let tombstones = cur.r.tombstone_range(&self.key);
                    // Remove any tombstoned values
                    exclude_tombstones_string_values(&tombstones, &mut v);

                    // Remove values we already read
                    v.exclude(cur.read_min, cur.read_max);

                    if !v.is_empty() {
                        // Only use values in the overlapping window
                        v.include(min_t, max_t);

                        // Merge the remaining values with the existing
                        values.merge(v);
                    }
                    cur.mark_read(min_t, max_t);
                }
            } else {
                // Blocks are ordered by generation, we may have values in the past in later blocks, if so,
                // expand the window to include the max time range to ensure values are returned in descending
                // order
                for cur in self.current.iter().skip(1) {
                    let cur = cur.read();
                    if cur.entry.max_time > max_t && !cur.read() {
                        max_t = cur.entry.max_time;
                    }
                }

                // Find first block that overlaps our window
                for cur in self.current.iter().skip(1) {
                    let cur = cur.read();
                    if cur.entry.overlaps_time_range(min_t, max_t) && !cur.read() {
                        // Shrink our window so it's the intersection of the first overlapping block and the
                        // first block.  We do this to minimize the region that overlaps and needs to
                        // be merged.
                        if cur.entry.min_time < min_t {
                            min_t = cur.entry.min_time;
                        }
                        values.include(min_t, max_t);
                        break;
                    }
                }

                // Search the remaining blocks that overlap our window and append their values so we can
                // merge them.
                for cur in self.current.iter_mut().skip(1) {
                    let mut cur = cur.write();
                    // Skip this block if it doesn't contain points we looking for or they have already been read
                    if !cur.entry.overlaps_time_range(min_t, max_t) || cur.read() {
                        cur.mark_read(min_t, max_t);
                        continue;
                    }

                    let mut v =
                        StringValues(read_profiled_block!(profile, cur, read_string_block_at));
                    let tombstones = cur.r.tombstone_range(&self.key);
                    // Remove any tombstoned values
                    exclude_tombstones_string_values(tombstones.as_slice(), &mut v);

                    // Remove values we already read
                    v.exclude(cur.read_min, cur.read_max);

                    // If the block we decoded should have all of it's values included, mark it as read so we
                    // don't use it again.
                    if !v.is_empty() {
                        v.include(min_t, max_t);
                        // Merge the remaining values with the existing
                        v.merge(values);
                        values = v;
                    }
                    cur.mark_read(min_t, max_t);
                }
            } // end dor if self.ascending

            let mut first = self.current[0].write();
            first.mark_read(min_t, max_t);

            return Ok(values.into());
        }
    }
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

#[derive(Clone)]
pub struct Location {
    pub r: Arc<TSMReader>,
    pub entry: IndexEntry,

    read_min: i64,
    read_max: i64,
}

impl Location {
    /// Check if all data in has been read.
    fn read(&self) -> bool {
        self.read_min <= self.entry.min_time && self.read_max >= self.entry.max_time
    }

    fn mark_read(&mut self, min: i64, max: i64) {
        if min < self.read_min {
            self.read_min = min;
        }

        if max >= self.read_max {
            self.read_max = max;
        }
    }
}

/// Returns the files and index blocks for a key and time.  ascending indicates
/// whether the key will be scan in ascending time order or descenging time order.
/// This function assumes the read-lock has been taken.
pub fn locations(
    files: &[Arc<TSMReader>],
    key: &[u8],
    t: i64,
    ascending: bool,
) -> Vec<Arc<RwLock<Location>>> {
    let mut cache = Vec::<IndexEntry>::new();
    let mut locations = vec![];
    for fd in files {
        let (min_time, max_time) = fd.time_range();

        // If we ascending and the max time of the file is before where we want to start
        // skip it.
        if ascending && max_time < t {
            continue;
            // If we are descending and the min time of the file is after where we want to start,
            // then skip it.
        } else if !ascending && min_time > t {
            continue;
        }
        let tombstones = fd.tombstone_range(key);

        // Read all index entries of the key in the file.
        // This file could potential contain points we are looking for so find the blocks for
        // the given key.
        let entries = fd.read_entries(key, Some(&mut cache));
        'outer: for ie in entries {
            // Skip any blocks only contain values that are tombstoned.
            for t in tombstones.iter() {
                if t.min <= ie.min_time && t.max >= ie.max_time {
                    continue 'outer;
                }
            }

            // If we ascending and the max time of a block is before where we are looking, skip
            // it since the data is out of our range
            if ascending && ie.max_time < t {
                continue;
                // If we descending and the min time of a block is after where we are looking, skip
                // it since the data is out of our range
            } else if !ascending && ie.min_time > t {
                continue;
            }

            let mut location = Location {
                r: Arc::clone(fd),
                entry: ie,
                read_min: 0,
                read_max: 0,
            };

            if ascending {
                // For an ascending cursor, mark everything before the seek time as read
                // so we can filter it out at query time
                location.read_min = i64::MIN;
                location.read_max = t - 1;
            } else {
                // For an ascending cursort, mark everything after the seek time as read
                // so we can filter it out at query time
                location.read_min = t + 1;
                location.read_max = i64::MAX;
            }

            // Otherwise, add this file and block location
            locations.push(Arc::new(RwLock::new(location)));
        }
    }

    locations
}

#[derive(Clone)]
pub struct VLogLocation {
    pub r: Arc<TSMReader>,
    pub vlog_index: VLogIndex,

    pub read_min: i64,
    pub read_max: i64,
}

impl VLogLocation {
    fn read(&self) -> bool {
        self.read_min <= self.vlog_index.min && self.read_max >= self.vlog_index.max
    }

    fn mark_read(&mut self, min: i64, max: i64) {
        if min < self.read_min {
            self.read_min = min;
        }

        if max >= self.read_max {
            self.read_max = max;
        }
    }
}

/// Returns the files and index blocks for a key and time.  ascending indicates
/// whether the key will be scan in ascending time order or descenging time order.
/// This function assumes the read-lock has been taken.
pub fn vlog_locations(
    files: &[Arc<TSMReader>],
    vlog_indexes: VLogIndexes,
    key: &[u8],
    t: i64,
    ascending: bool,
) -> Vec<Arc<RwLock<VLogLocation>>> {
    let mut locations = vec![];
    for f in files {
        // TODO: optimize me, iterator a vary large vlog_indexes mutli times.
        let tombstones = f.tombstone_range(key);

        'outer: for idx in vlog_indexes.iter() {
            if (idx.generation, idx.seq) == (f.generation, f.seq) {
                // Skip any blocks only contain values that are tombstoned.
                for t in tombstones.iter() {
                    if t.min <= idx.min && t.max >= idx.max {
                        continue 'outer;
                    }
                }

                // If we ascending and the max time of a block is before where we are looking, skip
                // it since the data is out of our range
                if ascending && idx.max < t {
                    continue;
                    // If we descending and the min time of a block is after where we are looking, skip
                    // it since the data is out of our range
                } else if !ascending && idx.min > t {
                    continue;
                }

                let mut location = VLogLocation {
                    r: Arc::clone(f),
                    vlog_index: idx.clone(),
                    read_min: 0,
                    read_max: 0,
                };

                if ascending {
                    // For an ascending cursor, mark everything before the seek time as read
                    // so we can filter it out at query time
                    location.read_min = i64::MIN;
                    location.read_max = t - 1;
                } else {
                    // For an ascending cursort, mark everything after the seek time as read
                    // so we can filter it out at query time
                    location.read_min = t + 1;
                    location.read_max = i64::MAX;
                }

                // Otherwise, add this file and block location
                locations.push(Arc::new(RwLock::new(location)));
            }
        }
    }
    locations
}
