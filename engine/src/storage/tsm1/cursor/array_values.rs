use std::cmp::Ordering::{Equal, Less};

use crate::storage::tsm1::vlog::VLogIndex;

#[derive(Default, Debug, PartialEq)]
pub struct IntegerArray {
    pub timestamps: Vec<i64>,
    pub values: Vec<i64>,
}

impl IntegerArray {
    pub fn new(sz: usize) -> Self {
        Self {
            timestamps: vec![0i64; sz],
            values: vec![0i64; sz],
        }
    }

    pub fn min_time(&self) -> i64 {
        self.timestamps[0]
    }

    pub fn max_time(&self) -> i64 {
        self.timestamps[self.timestamps.len() - 1]
    }

    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return bytes size.
    pub fn size(&self) -> usize {
        // size of timestamps + values
        self.timestamps.len() * 8 + self.values.len() * 8
    }

    /// search performs a binary search for unixnano() v in a
    /// and returns the position, i, where v would be inserted.
    /// An additional check of self.timestamps[i] == v is necessary
    /// to determine if the value v exists.
    pub fn search(&self, v: i64) -> usize {
        let mut lo = 0;
        let mut hi = self.len();
        while lo < hi {
            let mid = (lo + hi) >> 1;
            if self.timestamps[mid] < v {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        lo
    }

    /// Returns the positions where min and max would be
    /// inserted into the array. If a[0].UnixNano() > max or
    /// a[len-1].UnixNano() < min then find_range returns (-1, -1)
    /// indicating the array is outside the [min, max]. The values must
    /// be deduplicated and sorted before calling find_range or the results
    /// are undefined.
    pub fn find_range(&self, min: i64, max: i64) -> (i64, i64) {
        if self.is_empty() || min > max {
            return (-1, -1);
        }

        let min_val = self.min_time();
        let max_val = self.max_time();

        if max_val < min || min_val > max {
            return (-1, -1);
        }

        (self.search(min) as i64, self.search(max) as i64)
    }

    // Returns the subset of values not in [min, max].  The values must
    // be deduplicated and sorted before calling Exclude or the results are undefined.
    pub fn exclude(&mut self, min: i64, max: i64) {
        let (rmin, mut rmax) = self.find_range(min, max);
        if rmin == -1 && rmax == -1 {
            return;
        }

        // self[rmin].unix_nano() ≥ min
        // self[rmax].unix_nano() ≥ max

        if rmax < self.len() as i64 {
            if self.timestamps[rmax as usize] == max {
                rmax += 1;
            }
            let rest = self.len() - rmax as usize;
            if rest > 0 {
                let ptr = self.timestamps.as_mut_ptr();
                unsafe {
                    std::ptr::copy(ptr.offset(rmax as isize), ptr.offset(rmin as isize), rest);
                }
                self.timestamps.truncate(rmin as usize + rest);

                let ptr = self.values.as_mut_ptr();
                unsafe {
                    std::ptr::copy(ptr.offset(rmax as isize), ptr.offset(rmin as isize), rest);
                }
                self.values.truncate(rmin as usize + rest);

                return;
            }
        }
        self.timestamps.truncate(rmin as usize);
        self.values.truncate(rmin as usize);
    }

    /// Include returns the subset values between min and max inclusive. The values must
    /// be deduplicated and sorted before calling Exclude or the results are undefined.
    pub fn include(&mut self, min: i64, max: i64) {
        let (rmin, mut rmax) = self.find_range(min, max);
        if rmin == -1 && rmax == -1 {
            self.timestamps.clear();
            self.values.clear();
            return;
        }

        // self[rmin].UnixNano() ≥ min
        // self[rmax].UnixNano() ≥ max

        if (rmax as usize) < self.len() && self.timestamps[rmax as usize] == max {
            rmax += 1;
        }

        if rmin > -1 {
            let ptr = self.timestamps.as_mut_ptr();
            unsafe {
                std::ptr::copy(ptr.offset(rmin as isize), ptr, (rmax - rmin) as usize);
            }
            self.timestamps.truncate((rmax - rmin) as usize);

            let ptr = self.values.as_mut_ptr();
            unsafe {
                std::ptr::copy(ptr.offset(rmin as isize), ptr, (rmax - rmin) as usize);
            }
            self.values.truncate((rmax - rmin) as usize);

            return;
        }

        self.timestamps.truncate(rmax as usize);
        self.values.truncate(rmax as usize);
    }

    /// Merge overlays b to top of a.  If two values conflict with
    /// the same timestamp, b is used.  Both a and b must be sorted
    /// in ascending order.
    pub fn merge(&mut self, mut b: IntegerArray) {
        if self.is_empty() {
            *self = b;
            return;
        }

        if b.is_empty() {
            return;
        }

        // Normally, both a and b should not contain duplicates.  Due to a bug in older versions, it's
        // possible stored blocks might contain duplicate values.  Remove them if they exists before
        // merging.
        // self.deduplicate();
        // b.deduplicate();

        if self.max_time() < b.min_time() {
            self.timestamps.append(&mut b.timestamps);
            self.values.append(&mut b.values);
            return;
        }

        if b.max_time() < self.min_time() {
            let mut tmp = IntegerArray::default();
            tmp.timestamps.append(&mut b.timestamps);
            tmp.timestamps.append(&mut self.timestamps);
            tmp.values.append(&mut b.values);
            tmp.values.append(&mut self.values);

            *self = tmp;
            return;
        }

        let mut out = Self::new(self.len() + b.len());
        let (mut i, mut j, mut k) = (0, 0, 0);
        while i < self.timestamps.len() && j < b.timestamps.len() {
            match self.timestamps[i].cmp(&b.timestamps[j]) {
                Less => {
                    out.timestamps[k] = self.timestamps[i];
                    out.values[k] = self.values[i];
                    i += 1;
                }
                Equal => {
                    out.timestamps[k] = b.timestamps[j];
                    out.values[k] = b.values[j];
                    i += 1;
                    j += 1;
                }
                _ => {
                    out.timestamps[k] = b.timestamps[j];
                    out.values[k] = b.values[j];
                    j += 1;
                }
            }
            k += 1;
        }

        if i < self.timestamps.len() {
            let n = self.timestamps.len() - i;
            out.timestamps[k..k + n].copy_from_slice(&self.timestamps[i..]);
            out.values[k..k + n].copy_from_slice(&self.values[i..]);
            k += n;
        } else if j < b.timestamps.len() {
            let n = b.timestamps.len() - j;
            out.timestamps[k..k + n].copy_from_slice(&b.timestamps[j..]);
            out.values[k..k + n].copy_from_slice(&b.values[j..]);
            k += n;
        }

        out.timestamps.truncate(k);
        out.values.truncate(k);
        self.timestamps = out.timestamps;
        self.values = out.values;
    }
}

#[derive(Default, Debug, PartialEq)]
pub struct UnsignedArray {
    pub timestamps: Vec<i64>,
    pub values: Vec<u64>,
}

impl UnsignedArray {
    pub fn new(sz: usize) -> Self {
        Self {
            timestamps: vec![0i64; sz],
            values: vec![0u64; sz],
        }
    }

    pub fn min_time(&self) -> i64 {
        self.timestamps[0]
    }

    pub fn max_time(&self) -> i64 {
        self.timestamps[self.timestamps.len() - 1]
    }

    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return bytes size.
    pub fn size(&self) -> usize {
        // size of timestamps + values
        self.timestamps.len() * 8 + self.values.len() * 8
    }

    /// search performs a binary search for unixnano() v in a
    /// and returns the position, i, where v would be inserted.
    /// An additional check of self.timestamps[i] == v is necessary
    /// to determine if the value v exists.
    pub fn search(&self, v: i64) -> usize {
        let mut lo = 0;
        let mut hi = self.len();
        while lo < hi {
            let mid = (lo + hi) >> 1;
            if self.timestamps[mid] < v {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        lo
    }

    /// Returns the positions where min and max would be
    /// inserted into the array. If a[0].UnixNano() > max or
    /// a[len-1].UnixNano() < min then find_range returns (-1, -1)
    /// indicating the array is outside the [min, max]. The values must
    /// be deduplicated and sorted before calling find_range or the results
    /// are undefined.
    pub fn find_range(&self, min: i64, max: i64) -> (i64, i64) {
        if self.is_empty() || min > max {
            return (-1, -1);
        }

        let min_val = self.min_time();
        let max_val = self.max_time();

        if max_val < min || min_val > max {
            return (-1, -1);
        }

        (self.search(min) as i64, self.search(max) as i64)
    }

    // Returns the subset of values not in [min, max].  The values must
    // be deduplicated and sorted before calling Exclude or the results are undefined.
    pub fn exclude(&mut self, min: i64, max: i64) {
        let (rmin, mut rmax) = self.find_range(min, max);
        if rmin == -1 && rmax == -1 {
            return;
        }

        // self[rmin].unix_nano() ≥ min
        // self[rmax].unix_nano() ≥ max

        if rmax < self.len() as i64 {
            if self.timestamps[rmax as usize] == max {
                rmax += 1;
            }
            let rest = self.len() - rmax as usize;
            if rest > 0 {
                let ptr = self.timestamps.as_mut_ptr();
                unsafe {
                    std::ptr::copy(ptr.offset(rmax as isize), ptr.offset(rmin as isize), rest);
                }
                self.timestamps.truncate(rmin as usize + rest);

                let ptr = self.values.as_mut_ptr();
                unsafe {
                    std::ptr::copy(ptr.offset(rmax as isize), ptr.offset(rmin as isize), rest);
                }
                self.values.truncate(rmin as usize + rest);

                return;
            }
        }
        self.timestamps.truncate(rmin as usize);
        self.values.truncate(rmin as usize);
    }

    /// Include returns the subset values between min and max inclusive. The values must
    /// be deduplicated and sorted before calling Exclude or the results are undefined.
    pub fn include(&mut self, min: i64, max: i64) {
        let (rmin, mut rmax) = self.find_range(min, max);
        if rmin == -1 && rmax == -1 {
            self.timestamps.clear();
            self.values.clear();
            return;
        }

        // self[rmin].UnixNano() ≥ min
        // self[rmax].UnixNano() ≥ max

        if (rmax as usize) < self.len() && self.timestamps[rmax as usize] == max {
            rmax += 1;
        }

        if rmin > -1 {
            let ptr = self.timestamps.as_mut_ptr();
            unsafe {
                std::ptr::copy(ptr.offset(rmin as isize), ptr, (rmax - rmin) as usize);
            }
            self.timestamps.truncate((rmax - rmin) as usize);

            let ptr = self.values.as_mut_ptr();
            unsafe {
                std::ptr::copy(ptr.offset(rmin as isize), ptr, (rmax - rmin) as usize);
            }
            self.values.truncate((rmax - rmin) as usize);

            return;
        }

        self.timestamps.truncate(rmax as usize);
        self.values.truncate(rmax as usize);
    }

    /// Merge overlays b to top of a.  If two values conflict with
    /// the same timestamp, b is used.  Both a and b must be sorted
    /// in ascending order.
    pub fn merge(&mut self, mut b: UnsignedArray) {
        if self.is_empty() {
            *self = b;
            return;
        }

        if b.is_empty() {
            return;
        }

        // Normally, both a and b should not contain duplicates.  Due to a bug in older versions, it's
        // possible stored blocks might contain duplicate values.  Remove them if they exists before
        // merging.
        // self.deduplicate();
        // b.deduplicate();

        if self.max_time() < b.min_time() {
            self.timestamps.append(&mut b.timestamps);
            self.values.append(&mut b.values);
            return;
        }

        if b.max_time() < self.min_time() {
            let mut tmp = UnsignedArray::default();
            tmp.timestamps.append(&mut b.timestamps);
            tmp.timestamps.append(&mut self.timestamps);
            tmp.values.append(&mut b.values);
            tmp.values.append(&mut self.values);

            *self = tmp;
            return;
        }

        let mut out = Self::new(self.len() + b.len());
        let (mut i, mut j, mut k) = (0, 0, 0);
        while i < self.timestamps.len() && j < b.timestamps.len() {
            match self.timestamps[i].cmp(&b.timestamps[j]) {
                Less => {
                    out.timestamps[k] = self.timestamps[i];
                    out.values[k] = self.values[i];
                    i += 1;
                }
                Equal => {
                    out.timestamps[k] = b.timestamps[j];
                    out.values[k] = b.values[j];
                    i += 1;
                    j += 1;
                }
                _ => {
                    out.timestamps[k] = b.timestamps[j];
                    out.values[k] = b.values[j];
                    j += 1;
                }
            }

            k += 1;
        }

        if i < self.timestamps.len() {
            let n = self.timestamps.len() - i;
            out.timestamps[k..k + n].copy_from_slice(&self.timestamps[i..]);
            out.values[k..k + n].copy_from_slice(&self.values[i..]);
            k += n;
        } else if j < b.timestamps.len() {
            let n = b.timestamps.len() - j;
            out.timestamps[k..k + n].copy_from_slice(&b.timestamps[j..]);
            out.values[k..k + n].copy_from_slice(&b.values[j..]);
            k += n;
        }

        out.timestamps.truncate(k);
        out.values.truncate(k);
        self.timestamps = out.timestamps;
        self.values = out.values;
    }
}

#[derive(Default, Debug, PartialEq)]
pub struct FloatArray {
    pub timestamps: Vec<i64>,
    pub values: Vec<f64>,
}

impl FloatArray {
    pub fn new(sz: usize) -> Self {
        Self {
            timestamps: vec![0i64; sz],
            values: vec![0.0f64; sz],
        }
    }

    pub fn min_time(&self) -> i64 {
        self.timestamps[0]
    }

    pub fn max_time(&self) -> i64 {
        self.timestamps[self.timestamps.len() - 1]
    }

    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return bytes size.
    pub fn size(&self) -> usize {
        // size of timestamps + values
        self.timestamps.len() * 8 + self.values.len() * 8
    }

    /// search performs a binary search for unixnano() v in a
    /// and returns the position, i, where v would be inserted.
    /// An additional check of self.timestamps[i] == v is necessary
    /// to determine if the value v exists.
    pub fn search(&self, v: i64) -> usize {
        let mut lo = 0;
        let mut hi = self.len();
        while lo < hi {
            let mid = (lo + hi) >> 1;
            if self.timestamps[mid] < v {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        lo
    }

    /// Returns the positions where min and max would be
    /// inserted into the array. If a[0].UnixNano() > max or
    /// a[len-1].UnixNano() < min then find_range returns (-1, -1)
    /// indicating the array is outside the [min, max]. The values must
    /// be deduplicated and sorted before calling find_range or the results
    /// are undefined.
    pub fn find_range(&self, min: i64, max: i64) -> (i64, i64) {
        if self.is_empty() || min > max {
            return (-1, -1);
        }

        let min_val = self.min_time();
        let max_val = self.max_time();

        if max_val < min || min_val > max {
            return (-1, -1);
        }

        (self.search(min) as i64, self.search(max) as i64)
    }

    // Returns the subset of values not in [min, max].  The values must
    // be deduplicated and sorted before calling Exclude or the results are undefined.
    pub fn exclude(&mut self, min: i64, max: i64) {
        let (rmin, mut rmax) = self.find_range(min, max);
        if rmin == -1 && rmax == -1 {
            return;
        }

        // self[rmin].unix_nano() ≥ min
        // self[rmax].unix_nano() ≥ max

        if rmax < self.len() as i64 {
            if self.timestamps[rmax as usize] == max {
                rmax += 1;
            }
            let rest = self.len() - rmax as usize;
            if rest > 0 {
                let ptr = self.timestamps.as_mut_ptr();
                unsafe {
                    std::ptr::copy(ptr.offset(rmax as isize), ptr.offset(rmin as isize), rest);
                }
                self.timestamps.truncate(rmin as usize + rest);

                let ptr = self.values.as_mut_ptr();
                unsafe {
                    std::ptr::copy(ptr.offset(rmax as isize), ptr.offset(rmin as isize), rest);
                }
                self.values.truncate(rmin as usize + rest);

                return;
            }
        }
        self.timestamps.truncate(rmin as usize);
        self.values.truncate(rmin as usize);
    }

    /// Include returns the subset values between min and max inclusive. The values must
    /// be deduplicated and sorted before calling Exclude or the results are undefined.
    pub fn include(&mut self, min: i64, max: i64) {
        let (rmin, mut rmax) = self.find_range(min, max);
        if rmin == -1 && rmax == -1 {
            self.timestamps.clear();
            self.values.clear();
            return;
        }

        // self[rmin].UnixNano() ≥ min
        // self[rmax].UnixNano() ≥ max

        if (rmax as usize) < self.len() && self.timestamps[rmax as usize] == max {
            rmax += 1;
        }

        if rmin > -1 {
            let ptr = self.timestamps.as_mut_ptr();
            unsafe {
                std::ptr::copy(ptr.offset(rmin as isize), ptr, (rmax - rmin) as usize);
            }
            self.timestamps.truncate((rmax - rmin) as usize);

            let ptr = self.values.as_mut_ptr();
            unsafe {
                std::ptr::copy(ptr.offset(rmin as isize), ptr, (rmax - rmin) as usize);
            }
            self.values.truncate((rmax - rmin) as usize);

            return;
        }

        self.timestamps.truncate(rmax as usize);
        self.values.truncate(rmax as usize);
    }

    /// Merge overlays b to top of a.  If two values conflict with
    /// the same timestamp, b is used.  Both a and b must be sorted
    /// in ascending order.
    pub fn merge(&mut self, mut b: FloatArray) {
        if self.is_empty() {
            *self = b;
            return;
        }

        if b.is_empty() {
            return;
        }

        // Normally, both a and b should not contain duplicates.  Due to a bug in older versions, it's
        // possible stored blocks might contain duplicate values.  Remove them if they exists before
        // merging.
        // self.deduplicate();
        // b.deduplicate();

        if self.max_time() < b.min_time() {
            self.timestamps.append(&mut b.timestamps);
            self.values.append(&mut b.values);
            return;
        }

        if b.max_time() < self.min_time() {
            let mut tmp = FloatArray::default();
            tmp.timestamps.append(&mut b.timestamps);
            tmp.timestamps.append(&mut self.timestamps);
            tmp.values.append(&mut b.values);
            tmp.values.append(&mut self.values);

            *self = tmp;
            return;
        }

        let mut out = Self::new(self.len() + b.len());
        let (mut i, mut j, mut k) = (0, 0, 0);
        while i < self.timestamps.len() && j < b.timestamps.len() {
            match self.timestamps[i].cmp(&b.timestamps[j]) {
                Less => {
                    out.timestamps[k] = self.timestamps[i];
                    out.values[k] = self.values[i];
                    i += 1;
                }
                Equal => {
                    out.timestamps[k] = b.timestamps[j];
                    out.values[k] = b.values[j];
                    i += 1;
                    j += 1;
                }
                _ => {
                    out.timestamps[k] = b.timestamps[j];
                    out.values[k] = b.values[j];
                    j += 1;
                }
            }

            k += 1;
        }

        if i < self.timestamps.len() {
            let n = self.timestamps.len() - i;
            out.timestamps[k..k + n].copy_from_slice(&self.timestamps[i..]);
            out.values[k..k + n].copy_from_slice(&self.values[i..]);
            k += n;
        } else if j < b.timestamps.len() {
            let n = b.timestamps.len() - j;
            out.timestamps[k..k + n].copy_from_slice(&b.timestamps[j..]);
            out.values[k..k + n].copy_from_slice(&b.values[j..]);
            k += n;
        }

        out.timestamps.truncate(k);
        out.values.truncate(k);
        self.timestamps = out.timestamps;
        self.values = out.values;
    }
}

#[derive(Default, Debug, PartialEq)]
pub struct BoolArray {
    pub timestamps: Vec<i64>,
    pub values: Vec<bool>,
}

impl BoolArray {
    pub fn new(sz: usize) -> Self {
        Self {
            timestamps: vec![0i64; sz],
            values: vec![false; sz],
        }
    }

    pub fn min_time(&self) -> i64 {
        self.timestamps[0]
    }

    pub fn max_time(&self) -> i64 {
        self.timestamps[self.timestamps.len() - 1]
    }

    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return bytes size.
    pub fn size(&self) -> usize {
        // size of timestamps + values
        self.timestamps.len() * 8 + self.values.len()
    }

    /// search performs a binary search for unixnano() v in a
    /// and returns the position, i, where v would be inserted.
    /// An additional check of self.timestamps[i] == v is necessary
    /// to determine if the value v exists.
    pub fn search(&self, v: i64) -> usize {
        let mut lo = 0;
        let mut hi = self.len();
        while lo < hi {
            let mid = (lo + hi) >> 1;
            if self.timestamps[mid] < v {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        lo
    }

    /// Returns the positions where min and max would be
    /// inserted into the array. If a[0].UnixNano() > max or
    /// a[len-1].UnixNano() < min then find_range returns (-1, -1)
    /// indicating the array is outside the [min, max]. The values must
    /// be deduplicated and sorted before calling find_range or the results
    /// are undefined.
    pub fn find_range(&self, min: i64, max: i64) -> (i64, i64) {
        if self.is_empty() || min > max {
            return (-1, -1);
        }

        let min_val = self.min_time();
        let max_val = self.max_time();

        if max_val < min || min_val > max {
            return (-1, -1);
        }

        (self.search(min) as i64, self.search(max) as i64)
    }

    // Returns the subset of values not in [min, max].  The values must
    // be deduplicated and sorted before calling Exclude or the results are undefined.
    pub fn exclude(&mut self, min: i64, max: i64) {
        let (rmin, mut rmax) = self.find_range(min, max);
        if rmin == -1 && rmax == -1 {
            return;
        }

        // self[rmin].unix_nano() ≥ min
        // self[rmax].unix_nano() ≥ max

        if rmax < self.len() as i64 {
            if self.timestamps[rmax as usize] == max {
                rmax += 1;
            }
            let rest = self.len() - rmax as usize;
            if rest > 0 {
                let ptr = self.timestamps.as_mut_ptr();
                unsafe {
                    std::ptr::copy(ptr.offset(rmax as isize), ptr.offset(rmin as isize), rest);
                }
                self.timestamps.truncate(rmin as usize + rest);

                let ptr = self.values.as_mut_ptr();
                unsafe {
                    std::ptr::copy(ptr.offset(rmax as isize), ptr.offset(rmin as isize), rest);
                }
                self.values.truncate(rmin as usize + rest);

                return;
            }
        }
        self.timestamps.truncate(rmin as usize);
        self.values.truncate(rmin as usize);
    }

    /// Include returns the subset values between min and max inclusive. The values must
    /// be deduplicated and sorted before calling Exclude or the results are undefined.
    pub fn include(&mut self, min: i64, max: i64) {
        let (rmin, mut rmax) = self.find_range(min, max);
        if rmin == -1 && rmax == -1 {
            self.timestamps.clear();
            self.values.clear();
            return;
        }

        // self[rmin].UnixNano() ≥ min
        // self[rmax].UnixNano() ≥ max

        if (rmax as usize) < self.len() && self.timestamps[rmax as usize] == max {
            rmax += 1;
        }

        if rmin > -1 {
            let ptr = self.timestamps.as_mut_ptr();
            unsafe {
                std::ptr::copy(ptr.offset(rmin as isize), ptr, (rmax - rmin) as usize);
            }
            self.timestamps.truncate((rmax - rmin) as usize);

            let ptr = self.values.as_mut_ptr();
            unsafe {
                std::ptr::copy(ptr.offset(rmin as isize), ptr, (rmax - rmin) as usize);
            }
            self.values.truncate((rmax - rmin) as usize);

            return;
        }

        self.timestamps.truncate(rmax as usize);
        self.values.truncate(rmax as usize);
    }

    /// Merge overlays b to top of a.  If two values conflict with
    /// the same timestamp, b is used.  Both a and b must be sorted
    /// in ascending order.
    pub fn merge(&mut self, mut b: BoolArray) {
        if self.is_empty() {
            *self = b;
            return;
        }

        if b.is_empty() {
            return;
        }

        // Normally, both a and b should not contain duplicates.  Due to a bug in older versions, it's
        // possible stored blocks might contain duplicate values.  Remove them if they exists before
        // merging.
        // self.deduplicate();
        // b.deduplicate();

        if self.max_time() < b.min_time() {
            self.timestamps.append(&mut b.timestamps);
            self.values.append(&mut b.values);
            return;
        }

        if b.max_time() < self.min_time() {
            let mut tmp = BoolArray::default();
            tmp.timestamps.append(&mut b.timestamps);
            tmp.timestamps.append(&mut self.timestamps);
            tmp.values.append(&mut b.values);
            tmp.values.append(&mut self.values);

            *self = tmp;
            return;
        }

        let mut out = Self::new(self.len() + b.len());
        let (mut i, mut j, mut k) = (0, 0, 0);
        while i < self.timestamps.len() && j < b.timestamps.len() {
            match self.timestamps[i].cmp(&b.timestamps[j]) {
                Less => {
                    out.timestamps[k] = self.timestamps[i];
                    out.values[k] = self.values[i];
                    i += 1;
                }
                Equal => {
                    out.timestamps[k] = b.timestamps[j];
                    out.values[k] = b.values[j];
                    i += 1;
                    j += 1;
                }
                _ => {
                    out.timestamps[k] = b.timestamps[j];
                    out.values[k] = b.values[j];
                    j += 1;
                }
            }

            k += 1;
        }

        if i < self.timestamps.len() {
            let n = self.timestamps.len() - i;
            out.timestamps[k..k + n].copy_from_slice(&self.timestamps[i..]);
            out.values[k..k + n].copy_from_slice(&self.values[i..]);
            k += n;
        } else if j < b.timestamps.len() {
            let n = b.timestamps.len() - j;
            out.timestamps[k..k + n].copy_from_slice(&b.timestamps[j..]);
            out.values[k..k + n].copy_from_slice(&b.values[j..]);
            k += n;
        }

        out.timestamps.truncate(k);
        out.values.truncate(k);
        self.timestamps = out.timestamps;
        self.values = out.values;
    }
}

#[derive(Default, Debug, PartialEq)]
pub struct StringArray {
    pub timestamps: Vec<i64>,
    pub values: Vec<String>,
}

impl StringArray {
    pub fn new(sz: usize) -> Self {
        Self {
            timestamps: vec![0i64; sz],
            values: vec!["".to_string(); sz],
        }
    }

    pub fn min_time(&self) -> i64 {
        self.timestamps[0]
    }

    pub fn max_time(&self) -> i64 {
        self.timestamps[self.timestamps.len() - 1]
    }

    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return bytes size.
    pub fn size(&self) -> usize {
        let mut sz = self.timestamps.len() * 8;
        for s in self.values.iter() {
            sz += s.len();
        }

        sz
    }

    /// search performs a binary search for unixnano() v in a
    /// and returns the position, i, where v would be inserted.
    /// An additional check of self.timestamps[i] == v is necessary
    /// to determine if the value v exists.
    pub fn search(&self, v: i64) -> usize {
        let mut lo = 0;
        let mut hi = self.len();
        while lo < hi {
            let mid = (lo + hi) >> 1;
            if self.timestamps[mid] < v {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        lo
    }

    /// Returns the positions where min and max would be
    /// inserted into the array. If a[0].UnixNano() > max or
    /// a[len-1].UnixNano() < min then find_range returns (-1, -1)
    /// indicating the array is outside the [min, max]. The values must
    /// be deduplicated and sorted before calling find_range or the results
    /// are undefined.
    pub fn find_range(&self, min: i64, max: i64) -> (i64, i64) {
        if self.is_empty() || min > max {
            return (-1, -1);
        }

        let min_val = self.min_time();
        let max_val = self.max_time();

        if max_val < min || min_val > max {
            return (-1, -1);
        }

        (self.search(min) as i64, self.search(max) as i64)
    }

    // Returns the subset of values not in [min, max].  The values must
    // be deduplicated and sorted before calling Exclude or the results are undefined.
    pub fn exclude(&mut self, min: i64, max: i64) {
        let (rmin, mut rmax) = self.find_range(min, max);
        if rmin == -1 && rmax == -1 {
            return;
        }

        // self[rmin].unix_nano() ≥ min
        // self[rmax].unix_nano() ≥ max

        if rmax < self.len() as i64 {
            if self.timestamps[rmax as usize] == max {
                rmax += 1;
            }
            let rest = self.len() - rmax as usize;
            if rest > 0 {
                let ptr = self.timestamps.as_mut_ptr();
                unsafe {
                    std::ptr::copy(ptr.offset(rmax as isize), ptr.offset(rmin as isize), rest);
                }
                self.timestamps.truncate(rmin as usize + rest);

                let ptr = self.values.as_mut_ptr();
                unsafe {
                    std::ptr::copy(ptr.offset(rmax as isize), ptr.offset(rmin as isize), rest);
                }
                self.values.truncate(rmin as usize + rest);

                return;
            }
        }
        self.timestamps.truncate(rmin as usize);
        self.values.truncate(rmin as usize);
    }

    /// Include returns the subset values between min and max inclusive. The values must
    /// be deduplicated and sorted before calling Exclude or the results are undefined.
    pub fn include(&mut self, min: i64, max: i64) {
        let (rmin, mut rmax) = self.find_range(min, max);
        if rmin == -1 && rmax == -1 {
            self.timestamps.clear();
            self.values.clear();
            return;
        }

        // self[rmin].UnixNano() ≥ min
        // self[rmax].UnixNano() ≥ max

        if (rmax as usize) < self.len() && self.timestamps[rmax as usize] == max {
            rmax += 1;
        }

        if rmin > -1 {
            let ptr = self.timestamps.as_mut_ptr();
            unsafe {
                std::ptr::copy(ptr.offset(rmin as isize), ptr, (rmax - rmin) as usize);
            }
            self.timestamps.truncate((rmax - rmin) as usize);

            let ptr = self.values.as_mut_ptr();
            unsafe {
                std::ptr::copy(ptr.offset(rmin as isize), ptr, (rmax - rmin) as usize);
            }
            self.values.truncate((rmax - rmin) as usize);

            return;
        }

        self.timestamps.truncate(rmax as usize);
        self.values.truncate(rmax as usize);
    }

    /// Merge overlays b to top of a.  If two values conflict with
    /// the same timestamp, b is used.  Both a and b must be sorted
    /// in ascending order.
    pub fn merge(&mut self, mut b: StringArray) {
        if self.is_empty() {
            *self = b;
            return;
        }

        if b.is_empty() {
            return;
        }

        // Normally, both a and b should not contain duplicates.  Due to a bug in older versions, it's
        // possible stored blocks might contain duplicate values.  Remove them if they exists before
        // merging.
        // self.deduplicate();
        // b.deduplicate();

        if self.max_time() < b.min_time() {
            self.timestamps.append(&mut b.timestamps);
            self.values.append(&mut b.values);
            return;
        }

        if b.max_time() < self.min_time() {
            let mut tmp = StringArray::default();
            tmp.timestamps.append(&mut b.timestamps);
            tmp.timestamps.append(&mut self.timestamps);
            tmp.values.append(&mut b.values);
            tmp.values.append(&mut self.values);

            *self = tmp;
            return;
        }

        let mut out = Self::new(self.len() + b.len());
        let (mut i, mut j, mut k) = (0, 0, 0);
        while i < self.timestamps.len() && j < b.timestamps.len() {
            match self.timestamps[i].cmp(&b.timestamps[j]) {
                Less => {
                    out.timestamps[k] = self.timestamps[i];
                    out.values[k].clone_from(&self.values[i]);
                    i += 1;
                }
                Equal => {
                    out.timestamps[k] = b.timestamps[j];
                    out.values[k].clone_from(&b.values[j]);
                    i += 1;
                    j += 1;
                }
                _ => {
                    out.timestamps[k] = b.timestamps[j];
                    out.values[k].clone_from(&b.values[j]);
                    j += 1;
                }
            }

            k += 1;
        }

        if i < self.timestamps.len() {
            let n = self.timestamps.len() - i;
            out.timestamps[k..k + n].copy_from_slice(&self.timestamps[i..]);
            out.values[k..k + n].clone_from_slice(&self.values[i..]);
            k += n;
        } else if j < b.timestamps.len() {
            let n = b.timestamps.len() - j;
            out.timestamps[k..k + n].copy_from_slice(&b.timestamps[j..]);
            out.values[k..k + n].clone_from_slice(&b.values[j..]);
            k += n;
        }

        out.timestamps.truncate(k);
        out.values.truncate(k);
        self.timestamps = out.timestamps;
        self.values = out.values;
    }
}

pub struct TimestampArray {
    pub timestamps: Vec<i64>,
}

impl TimestampArray {
    pub fn new(sz: usize) -> Self {
        Self {
            timestamps: vec![0i64; sz],
        }
    }

    pub fn min_time(&self) -> i64 {
        self.timestamps[0]
    }

    pub fn max_time(&self) -> i64 {
        self.timestamps[self.timestamps.len() - 1]
    }

    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// search performs a binary search for unixnano() v in a
    /// and returns the position, i, where v would be inserted.
    /// An additional check of self.timestamps[i] == v is necessary
    /// to determine if the value v exists.
    pub fn search(&self, v: i64) -> usize {
        let mut lo = 0;
        let mut hi = self.len();
        while lo < hi {
            let mid = (lo + hi) >> 1;
            if self.timestamps[mid] < v {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        lo
    }

    /// Returns the positions where min and max would be
    /// inserted into the array. If a[0].UnixNano() > max or
    /// a[len-1].UnixNano() < min then find_range returns (-1, -1)
    /// indicating the array is outside the [min, max]. The values must
    /// be deduplicated and sorted before calling find_range or the results
    /// are undefined.
    pub fn find_range(&self, min: i64, max: i64) -> (i64, i64) {
        if self.is_empty() || min > max {
            return (-1, -1);
        }

        let min_val = self.min_time();
        let max_val = self.max_time();

        if max_val < min || min_val > max {
            return (-1, -1);
        }

        (self.search(min) as i64, self.search(max) as i64)
    }

    // Returns the subset of values not in [min, max].  The values must
    // be deduplicated and sorted before calling Exclude or the results are undefined.
    pub fn exclude(&mut self, min: i64, max: i64) {
        let (rmin, mut rmax) = self.find_range(min, max);
        if rmin == -1 && rmax == -1 {
            return;
        }

        // self[rmin].unix_nano() ≥ min
        // self[rmax].unix_nano() ≥ max

        if rmax < self.len() as i64 {
            if self.timestamps[rmax as usize] == max {
                rmax += 1;
            }
            let rest = self.len() - rmax as usize;
            if rest > 0 {
                let ptr = self.timestamps.as_mut_ptr();
                unsafe {
                    std::ptr::copy(ptr.offset(rmax as isize), ptr.offset(rmin as isize), rest);
                }
                self.timestamps.truncate(rmin as usize + rest);
                return;
            }
        }
        self.timestamps.truncate(rmin as usize);
    }

    /// Returns true if values exist between min and max inclusive. The
    /// values must be sorted before calling Contains or the results are undefined.
    pub fn contains(&self, min: i64, max: i64) -> bool {
        let (rmin, rmax) = self.find_range(min, max);
        if rmin == -1 && rmax == -1 {
            return false;
        }

        // self.Timestamps[rmin] ≥ min
        // self.Timestamps[rmax] ≥ max

        if self.timestamps[rmin as usize] == min {
            return true;
        }

        if rmax < self.len() as i64 && self.timestamps[rmax as usize] == max {
            return true;
        }

        rmax - rmin > 0
    }
}

#[cfg(any(test, feature = "benchmarking"))]
pub mod tests {

    use super::{BoolArray, FloatArray, IntegerArray, StringArray, TimestampArray, UnsignedArray};

    pub fn make_integer_array(count: usize, min: i64, max: i64) -> IntegerArray {
        let mut vals = IntegerArray::new(count);

        let mut ts = min;
        let inc = (max - min) / (count as i64);

        for i in 0..count {
            vals.timestamps[i as usize] = ts;
            ts += inc;
        }

        vals
    }

    fn make_integer_array_from_slice(ts: &[i64]) -> IntegerArray {
        let mut vals = IntegerArray::new(ts.len());

        for (i, t) in ts.iter().enumerate() {
            vals.timestamps[i] = *t;
        }

        vals
    }

    #[test]
    fn test_integer_array_find_range_no_values() {
        let vals = IntegerArray::new(0);

        let (l, r) = vals.find_range(0, 100);
        assert!(l == -1, "invalid l, exp=-1, got={}", l);
        assert!(r == -1, "invalid r, exp=-1, got={}", r);
    }

    #[test]
    fn test_integer_array_find_range() {
        let vals = make_integer_array_from_slice(&[10, 11, 13, 15, 17, 20, 21]);

        struct Case {
            min: i64,
            max: i64,
            l: i64,
            r: i64,
        }
        let cases = vec![
            Case {
                min: 12,
                max: 20,
                l: 2,
                r: 5,
            },
            Case {
                min: 22,
                max: 40,
                l: -1,
                r: -1,
            },
            Case {
                min: 1,
                max: 9,
                l: -1,
                r: -1,
            },
            Case {
                min: 1,
                max: 10,
                l: 0,
                r: 0,
            },
            Case {
                min: 1,
                max: 11,
                l: 0,
                r: 1,
            },
            Case {
                min: 15,
                max: 15,
                l: 3,
                r: 3,
            },
        ];

        for (i, tc) in cases.iter().enumerate() {
            let (l, r) = vals.find_range(tc.min, tc.max);
            assert!(l == tc.l, "test({}) invalid l, exp={}, got={}", i, tc.l, l);
            assert!(r == tc.r, "test({}) invalid r, exp={}, got={}", i, tc.r, r);
        }
    }

    #[test]
    fn test_integer_array_exclude() {
        struct Case {
            n: &'static str,
            min: i64,
            max: i64,
            exp: Vec<i64>,
        }
        let cases = vec![
            Case {
                n: "excl bad range",
                min: 18,
                max: 11,
                exp: vec![10, 12, 14, 16, 18],
            },
            Case {
                n: "excl none-lo",
                min: 0,
                max: 9,
                exp: vec![10, 12, 14, 16, 18],
            },
            Case {
                n: "excl none-hi",
                min: 19,
                max: 30,
                exp: vec![10, 12, 14, 16, 18],
            },
            Case {
                n: "excl first",
                min: 0,
                max: 10,
                exp: vec![12, 14, 16, 18],
            },
            Case {
                n: "excl last",
                min: 18,
                max: 20,
                exp: vec![10, 12, 14, 16],
            },
            Case {
                n: "excl all but first and last",
                min: 12,
                max: 16,
                exp: vec![10, 18],
            },
            Case {
                n: "excl none in middle",
                min: 13,
                max: 13,
                exp: vec![10, 12, 14, 16, 18],
            },
            Case {
                n: "excl middle",
                min: 14,
                max: 14,
                exp: vec![10, 12, 16, 18],
            },
            Case {
                n: "excl suffix",
                min: 14,
                max: 18,
                exp: vec![10, 12],
            },
        ];

        for (i, tc) in cases.iter().enumerate() {
            let mut vals = make_integer_array(5, 10, 20);
            vals.exclude(tc.min, tc.max);
            assert_eq!(
                tc.exp, vals.timestamps,
                "test({}) unexpected values, exp={:?}, got={:?}",
                i, tc.exp, vals.timestamps
            );
        }
    }

    #[test]
    fn test_integer_array_include() {
        struct Case {
            n: &'static str,
            min: i64,
            max: i64,
            exp: Vec<i64>,
        }
        let cases = vec![
            Case {
                n: "incl none-lo",
                min: 0,
                max: 9,
                exp: vec![],
            },
            Case {
                n: "incl none-hi",
                min: 19,
                max: 30,
                exp: vec![],
            },
            Case {
                n: "incl first",
                min: 0,
                max: 10,
                exp: vec![10],
            },
            Case {
                n: "incl last",
                min: 18,
                max: 20,
                exp: vec![18],
            },
            Case {
                n: "incl all but first and last",
                min: 12,
                max: 16,
                exp: vec![12, 14, 16],
            },
            Case {
                n: "incl none in middle",
                min: 13,
                max: 13,
                exp: vec![],
            },
            Case {
                n: "incl middle",
                min: 14,
                max: 14,
                exp: vec![14],
            },
        ];

        for (i, tc) in cases.iter().enumerate() {
            let mut vals = make_integer_array(5, 10, 20);
            vals.include(tc.min, tc.max);
            assert_eq!(
                tc.exp, vals.timestamps,
                "test({}) unexpected values, exp={:?}, got={:?}",
                i, tc.exp, vals.timestamps
            );
        }
    }

    fn make_timestamp_array(count: usize, min: i64, max: i64) -> TimestampArray {
        let mut vals = TimestampArray::new(count);

        let mut ts = min;
        let inc = (max - min) / (count as i64);

        for i in 0..count {
            vals.timestamps[i as usize] = ts;
            ts += inc;
        }

        vals
    }

    #[test]
    fn test_timestamp_array_contains() {
        struct Case {
            n: &'static str,
            min: i64,
            max: i64,
            exp: bool,
        }
        let cases = vec![
            Case {
                n: "no/lo",
                min: 0,
                max: 9,
                exp: false,
            },
            Case {
                n: "no/hi",
                min: 19,
                max: 30,
                exp: false,
            },
            Case {
                n: "no/middle",
                min: 13,
                max: 13,
                exp: false,
            },
            Case {
                n: "yes/first",
                min: 0,
                max: 10,
                exp: true,
            },
            Case {
                n: "yes/first-equal",
                min: 10,
                max: 10,
                exp: true,
            },
            Case {
                n: "yes/last",
                min: 18,
                max: 20,
                exp: true,
            },
            Case {
                n: "yes/last-eq",
                min: 18,
                max: 20,
                exp: true,
            },
            Case {
                n: "yes/all but first and last",
                min: 12,
                max: 16,
                exp: true,
            },
            Case {
                n: "yes/middle-eq",
                min: 14,
                max: 14,
                exp: true,
            },
            Case {
                n: "yes/middle-overleap",
                min: 13,
                max: 15,
                exp: true,
            },
            Case {
                n: "yes/covers",
                min: 8,
                max: 22,
                exp: true,
            },
        ];

        for (i, tc) in cases.iter().enumerate() {
            let mut vals = make_timestamp_array(5, 10, 20);
            let got = vals.contains(tc.min, tc.max);
            assert_eq!(
                tc.exp, got,
                "test({}) unexpected contains, exp={:?}, got={:?}",
                i, tc.exp, got
            );
        }
    }

    fn make_integer_array2(v: &[i64]) -> IntegerArray {
        if v.len() & 1 == 1 {
            panic!("invalid array length");
        }

        let mut a = IntegerArray::new(v.len() / 2);

        for i in 0..v.len() / 2 {
            a.timestamps[i] = v[i * 2];
            a.values[i] = v[i * 2 + 1];
        }

        a
    }

    fn make_unsigned_array2(v: &[i64]) -> UnsignedArray {
        if v.len() & 1 == 1 {
            panic!("invalid array length");
        }

        let mut a = UnsignedArray::new(v.len() / 2);

        for i in 0..v.len() / 2 {
            a.timestamps[i] = v[i * 2];
            a.values[i] = v[i * 2 + 1] as u64;
        }

        a
    }

    fn make_float_array2(v: &[f64]) -> FloatArray {
        if v.len() & 1 == 1 {
            panic!("invalid array length");
        }

        let mut a = FloatArray::new(v.len() / 2);
        for i in 0..v.len() / 2 {
            a.timestamps[i] = v[i * 2] as i64;
            a.values[i] = v[i * 2 + 1];
        }

        a
    }

    fn make_bool_array2(v: &[(i64, bool)]) -> BoolArray {
        let mut a = BoolArray::new(v.len());

        for i in 0..v.len() {
            a.timestamps[i] = v[i].0;
            a.values[i] = v[i].1;
        }

        a
    }

    fn make_string_array2(v: &[i64]) -> StringArray {
        if v.len() & 1 == 1 {
            panic!("invalid array length");
        }

        let mut a = StringArray::new(v.len() / 2);

        for i in 0..v.len() / 2 {
            a.timestamps[i] = v[i * 2];
            a.values[i] = v[i * 2 + 1].to_string();
        }

        a
    }

    #[test]
    fn test_integer_array_merge() {
        struct Case {
            name: &'static str,
            a: IntegerArray,
            b: IntegerArray,
            exp: IntegerArray,
        }

        let tests = vec![
            Case {
                name: "empty a",
                a: make_integer_array2(&[]),
                b: make_integer_array2(&[1, 11, 2, 21]),
                exp: make_integer_array2(&[1, 11, 2, 21]),
            },
            Case {
                name: "empty b",
                a: make_integer_array2(&[1, 10, 2, 20]),
                b: make_integer_array2(&[]),
                exp: make_integer_array2(&[1, 10, 2, 20]),
            },
            Case {
                name: "b replaces a",
                a: make_integer_array2(&[1, 10]),
                b: make_integer_array2(&[
                    0, 1, 1, 11, // overwrites a
                    2, 21, 3, 31, 4, 41,
                ]),
                exp: make_integer_array2(&[0, 1, 1, 11, 2, 21, 3, 31, 4, 41]),
            },
            Case {
                name: "b replaces partial a",
                a: make_integer_array2(&[1, 10, 2, 20, 3, 30, 4, 40]),
                b: make_integer_array2(&[
                    1, 11, // overwrites a
                    2, 21, // overwrites a
                ]),
                exp: make_integer_array2(&[
                    1, 11, // overwrites a
                    2, 21, // overwrites a
                    3, 30, 4, 40,
                ]),
            },
            Case {
                name: "b replaces all a",
                a: make_integer_array2(&[1, 10, 2, 20, 3, 30, 4, 40]),
                b: make_integer_array2(&[1, 11, 2, 21, 3, 31, 4, 41]),
                exp: make_integer_array2(&[1, 11, 2, 21, 3, 31, 4, 41]),
            },
            Case {
                name: "b before a",
                a: make_integer_array2(&[10, 100, 11, 110]),
                b: make_integer_array2(&[1, 10, 2, 20]),
                exp: make_integer_array2(&[1, 10, 2, 20, 10, 100, 11, 110]),
            },
            Case {
                name: "b replaces a interleaved",
                a: make_integer_array2(&[0, 0, 1, 10, 2, 20, 3, 30, 4, 40]),
                b: make_integer_array2(&[0, 1, 2, 21, 4, 41]),
                exp: make_integer_array2(&[0, 1, 1, 10, 2, 21, 3, 30, 4, 41]),
            },
            Case {
                name: "b merges a interleaved",
                a: make_integer_array2(&[0, 00, 2, 20, 4, 40]),
                b: make_integer_array2(&[1, 11, 3, 31, 5, 51]),
                exp: make_integer_array2(&[0, 00, 1, 11, 2, 20, 3, 31, 4, 40, 5, 51]),
            },
        ];

        for (i, mut t) in tests.into_iter().enumerate() {
            t.a.merge(t.b);
            assert_eq!(
                t.a, t.exp,
                "test({}) unexpected values, exp={:?}, got={:?}",
                i, t.a, t.exp
            );
        }
    }

    #[test]
    fn test_unsigned_array_merge() {
        struct Case {
            name: &'static str,
            a: UnsignedArray,
            b: UnsignedArray,
            exp: UnsignedArray,
        }

        let tests = vec![
            Case {
                name: "empty a",
                a: make_unsigned_array2(&[]),
                b: make_unsigned_array2(&[1, 11, 2, 21]),
                exp: make_unsigned_array2(&[1, 11, 2, 21]),
            },
            Case {
                name: "empty b",
                a: make_unsigned_array2(&[1, 10, 2, 20]),
                b: make_unsigned_array2(&[]),
                exp: make_unsigned_array2(&[1, 10, 2, 20]),
            },
            Case {
                name: "b replaces a",
                a: make_unsigned_array2(&[1, 10]),
                b: make_unsigned_array2(&[
                    0, 1, 1, 11, // overwrites a
                    2, 21, 3, 31, 4, 41,
                ]),
                exp: make_unsigned_array2(&[0, 1, 1, 11, 2, 21, 3, 31, 4, 41]),
            },
            Case {
                name: "b replaces partial a",
                a: make_unsigned_array2(&[1, 10, 2, 20, 3, 30, 4, 40]),
                b: make_unsigned_array2(&[
                    1, 11, // overwrites a
                    2, 21, // overwrites a
                ]),
                exp: make_unsigned_array2(&[
                    1, 11, // overwrites a
                    2, 21, // overwrites a
                    3, 30, 4, 40,
                ]),
            },
            Case {
                name: "b replaces all a",
                a: make_unsigned_array2(&[1, 10, 2, 20, 3, 30, 4, 40]),
                b: make_unsigned_array2(&[1, 11, 2, 21, 3, 31, 4, 41]),
                exp: make_unsigned_array2(&[1, 11, 2, 21, 3, 31, 4, 41]),
            },
            Case {
                name: "b before a",
                a: make_unsigned_array2(&[10, 100, 11, 110]),
                b: make_unsigned_array2(&[1, 10, 2, 20]),
                exp: make_unsigned_array2(&[1, 10, 2, 20, 10, 100, 11, 110]),
            },
            Case {
                name: "b replaces a interleaved",
                a: make_unsigned_array2(&[0, 0, 1, 10, 2, 20, 3, 30, 4, 40]),
                b: make_unsigned_array2(&[0, 1, 2, 21, 4, 41]),
                exp: make_unsigned_array2(&[0, 1, 1, 10, 2, 21, 3, 30, 4, 41]),
            },
            Case {
                name: "b merges a interleaved",
                a: make_unsigned_array2(&[0, 00, 2, 20, 4, 40]),
                b: make_unsigned_array2(&[1, 11, 3, 31, 5, 51]),
                exp: make_unsigned_array2(&[0, 00, 1, 11, 2, 20, 3, 31, 4, 40, 5, 51]),
            },
        ];

        for (i, mut t) in tests.into_iter().enumerate() {
            t.a.merge(t.b);
            assert_eq!(
                t.a, t.exp,
                "test({}) unexpected values, exp={:?}, got={:?}",
                i, t.a, t.exp
            );
        }
    }

    #[test]
    fn test_float_array_merge() {
        struct Case {
            name: &'static str,
            a: FloatArray,
            b: FloatArray,
            exp: FloatArray,
        }

        let tests = vec![
            Case {
                name: "empty a",
                a: make_float_array2(&[]),
                b: make_float_array2(&[1.0, 1.1, 2.0, 2.1]),
                exp: make_float_array2(&[1.0, 1.1, 2.0, 2.1]),
            },
            Case {
                name: "empty b",
                a: make_float_array2(&[1.0, 1.0, 2.0, 2.0]),
                b: make_float_array2(&[]),
                exp: make_float_array2(&[1.0, 1.0, 2.0, 2.0]),
            },
            Case {
                name: "b replaces a",
                a: make_float_array2(&[1.0, 1.0]),
                b: make_float_array2(&[
                    0.0, 0.1, 1.0, 1.1, // overwrites a
                    2.0, 2.1, 3.0, 3.1, 4.0, 4.1,
                ]),
                exp: make_float_array2(&[0.0, 0.1, 1.0, 1.1, 2.0, 2.1, 3.0, 3.1, 4.0, 4.1]),
            },
            Case {
                name: "b replaces partial a",
                a: make_float_array2(&[1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 4.0, 4.0]),
                b: make_float_array2(&[
                    1.0, 1.1, // overwrites a
                    2.0, 2.1, // overwrites a
                ]),
                exp: make_float_array2(&[
                    1.0, 1.1, // overwrites a
                    2.0, 2.1, // overwrites a
                    3.0, 3.0, 4.0, 4.0,
                ]),
            },
            Case {
                name: "b replaces all a",
                a: make_float_array2(&[1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 4.0, 4.0]),
                b: make_float_array2(&[1.0, 1.1, 2.0, 2.1, 3.0, 3.1, 4.0, 4.1]),
                exp: make_float_array2(&[1.0, 1.1, 2.0, 2.1, 3.0, 3.1, 4.0, 4.1]),
            },
            Case {
                name: "b before a",
                a: make_float_array2(&[10.0, 10.0, 11.0, 11.0]),
                b: make_float_array2(&[1.0, 1.0, 2.0, 2.0]),
                exp: make_float_array2(&[1.0, 1.0, 2.0, 2.0, 10.0, 10.0, 11.0, 11.0]),
            },
            Case {
                name: "b replaces a interleaved",
                a: make_float_array2(&[0.0, 0.0, 1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 4.0, 4.0]),
                b: make_float_array2(&[0.0, 1.0, 2.0, 2.1, 4.0, 4.1]),
                exp: make_float_array2(&[0.0, 1.0, 1.0, 1.0, 2.0, 2.1, 3.0, 3.0, 4.0, 4.1]),
            },
            Case {
                name: "b merges a interleaved",
                a: make_float_array2(&[0.0, 0.0, 2.0, 2.0, 4.0, 4.0]),
                b: make_float_array2(&[1.0, 1.1, 3.0, 3.1, 5.0, 5.1]),
                exp: make_float_array2(&[
                    0.0, 0.0, 1.0, 1.1, 2.0, 2.0, 3.0, 3.1, 4.0, 4.0, 5.0, 5.1,
                ]),
            },
        ];

        for (i, mut t) in tests.into_iter().enumerate() {
            t.a.merge(t.b);
            assert_eq!(
                t.a, t.exp,
                "test({}) unexpected values, exp={:?}, got={:?}",
                i, t.a, t.exp
            );
        }
    }

    #[test]
    fn test_bool_array_merge() {
        struct Case {
            name: &'static str,
            a: BoolArray,
            b: BoolArray,
            exp: BoolArray,
        }

        let tests = vec![
            Case {
                name: "empty a",
                a: make_bool_array2(&[]),
                b: make_bool_array2(&[(1, true), (2, true)]),
                exp: make_bool_array2(&[(1, true), (2, true)]),
            },
            Case {
                name: "empty b",
                a: make_bool_array2(&[(1, true), (2, true)]),
                b: make_bool_array2(&[]),
                exp: make_bool_array2(&[(1, true), (2, true)]),
            },
            Case {
                name: "b replaces a",
                a: make_bool_array2(&[(1, true)]),
                b: make_bool_array2(&[
                    (0, false),
                    (1, false), // overwrites a
                    (2, false),
                    (3, false),
                    (4, false),
                ]),
                exp: make_bool_array2(&[
                    (0, false),
                    (1, false),
                    (2, false),
                    (3, false),
                    (4, false),
                ]),
            },
            Case {
                name: "b replaces partial a",
                a: make_bool_array2(&[(1, true), (2, true), (3, true), (4, true)]),
                b: make_bool_array2(&[
                    (1, false), // overwrites a
                    (2, false), // overwrites a
                ]),
                exp: make_bool_array2(&[
                    (1, false), // overwrites a
                    (2, false), // overwrites a
                    (3, true),
                    (4, true),
                ]),
            },
            Case {
                name: "b replaces all a",
                a: make_bool_array2(&[(1, true), (2, true), (3, true), (4, true)]),
                b: make_bool_array2(&[(1, false), (2, false), (3, false), (4, false)]),
                exp: make_bool_array2(&[(1, false), (2, false), (3, false), (4, false)]),
            },
            Case {
                name: "b before a",
                a: make_bool_array2(&[(10, true), (11, true)]),
                b: make_bool_array2(&[(1, false), (2, false)]),
                exp: make_bool_array2(&[(1, false), (2, false), (10, true), (11, true)]),
            },
            Case {
                name: "b replaces a interleaved",
                a: make_bool_array2(&[(0, true), (1, true), (2, true), (3, true), (4, true)]),
                b: make_bool_array2(&[(0, false), (2, false), (4, false)]),
                exp: make_bool_array2(&[(0, false), (1, true), (2, false), (3, true), (4, false)]),
            },
            Case {
                name: "b merges a interleaved",
                a: make_bool_array2(&[(0, true), (2, true), (4, true)]),
                b: make_bool_array2(&[(1, false), (3, false), (5, false)]),
                exp: make_bool_array2(&[
                    (0, true),
                    (1, false),
                    (2, true),
                    (3, false),
                    (4, true),
                    (5, false),
                ]),
            },
        ];

        for (i, mut t) in tests.into_iter().enumerate() {
            t.a.merge(t.b);
            assert_eq!(
                t.a, t.exp,
                "test({}) unexpected values, exp={:?}, got={:?}",
                i, t.a, t.exp
            );
        }
    }

    #[test]
    fn test_string_array_merge() {
        struct Case {
            name: &'static str,
            a: StringArray,
            b: StringArray,
            exp: StringArray,
        }

        let tests = vec![
            Case {
                name: "b before a",
                a: make_string_array2(&[10, 100, 11, 110]),
                b: make_string_array2(&[1, 10, 2, 20]),
                exp: make_string_array2(&[1, 10, 2, 20, 10, 100, 11, 110]),
            },
            Case {
                name: "b after a",
                a: make_string_array2(&[1, 10, 2, 20]),
                b: make_string_array2(&[10, 100, 11, 110]),
                exp: make_string_array2(&[1, 10, 2, 20, 10, 100, 11, 110]),
            },
            Case {
                name: "b replaces a interleaved",
                a: make_string_array2(&[0, 0, 1, 10, 2, 20, 3, 30, 4, 40]),
                b: make_string_array2(&[0, 1, 2, 21, 4, 41]),
                exp: make_string_array2(&[0, 1, 1, 10, 2, 21, 3, 30, 4, 41]),
            },
            Case {
                name: "b merges a interleaved",
                a: make_string_array2(&[0, 0, 2, 20, 4, 40]),
                b: make_string_array2(&[1, 11, 3, 31, 5, 51]),
                exp: make_string_array2(&[0, 0, 1, 11, 2, 20, 3, 31, 4, 40, 5, 51]),
            },
        ];

        for (i, mut t) in tests.into_iter().enumerate() {
            t.a.merge(t.b);
            assert_eq!(
                t.a, t.exp,
                "test({}) {} unexpected values, exp={:?}, got={:?}",
                i, t.name, t.exp, t.a
            );
            assert_eq!(t.a.timestamps.len(), t.a.values.len(), "test({i})");
        }
    }
}
