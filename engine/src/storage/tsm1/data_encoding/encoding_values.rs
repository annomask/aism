//! encod multi type values.
use std::ops::{Deref, DerefMut};

use crate::models::{
    FieldType,
    points::{
        BOOL, BoolValue, FLOAT, FloatValue, INTEGER, IntegerValue, STRING, StringValue, UNSIGNED,
        UnsignedValue, Value,
    },
};
use snafu::{ResultExt, Snafu, ensure};

use super::{
    array_encoding::{BatchIntSnafu, BatchTimestampSnafu},
    batch_bool::bool_array_encode_all,
    batch_float::float_array_encode_all,
    batch_int::{integer_array_encode_all, unsigned_array_encode_all},
    batch_string::string_array_encode_all,
    batch_timestamp::time_array_encode_all,
    bool::BoolEncoder,
    encoding::{
        BLOCK_BOOL, BLOCK_FLOAT64, BLOCK_INTEGER, BLOCK_STRING, BLOCK_UNSIGNED, encode_bool_block,
        encode_float_block, encode_integer_block, encode_string_block, encode_unsigned_block,
        pack_block,
    },
    encoding_pools::{
        get_bool_encoder, get_float_encoder, get_integer_encoder, get_string_encoder,
        get_time_encoder, get_unsigned_encoder, put_bool_encoder, put_float_encoder,
        put_integer_encoder, put_string_encoder, put_time_encoder, put_unsigned_encoder,
    },
    float::FloatEncoder,
    int::IntegerEncoder,
    string::StringEncoder,
    timestamp::TimeEncoder,
};
use crate::storage::tsm1::cursor::array_values::{
    BoolArray, FloatArray, IntegerArray, StringArray, UnsignedArray,
};

/// Error type for Values.
#[derive(Debug, Snafu)]
pub enum Error {
    /// String error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },

    /// Encoding error.
    #[snafu(display("{}", source))]
    Encoding {
        source: crate::storage::tsm1::data_encoding::encoding::Error,
    },

    /// Integer encode/decod error.
    #[snafu(display("integer encode/decode error: {}", source))]
    Integer { source: super::int::Error },

    /// Bool encode/decod error.
    #[snafu(display("bool encode/decode error: {}", source))]
    Bool { source: super::bool::Error },

    /// Float encode/decod error.
    #[snafu(display("float encode/decode error: {}", source))]
    Float { source: super::float::Error },

    /// String encode/decod error.
    #[snafu(display("string encode/decode error: {}", source))]
    String { source: super::string::Error },

    /// Timestamp encode/decod error.
    #[snafu(display("timestamp encode/decode error: {}", source))]
    Timestamp { source: super::timestamp::Error },
}

/// Result type for Index.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Values represents a slice of  values.
#[derive(Clone, Default, Debug, PartialEq)]
pub struct Values(pub Vec<Value>);

impl Values {
    pub fn min_time(&self) -> i64 {
        self[0].unix_nano()
    }

    pub fn max_time(&self) -> i64 {
        self[self.len() - 1].unix_nano()
    }

    pub fn size(&self) -> i64 {
        let mut sz = 0;
        for v in &self.0 {
            sz += v.size();
        }
        sz
    }

    fn ordered(&self) -> bool {
        if self.len() <= 1 {
            return true;
        }

        for i in 1..self.len() {
            let (av, ab) = (self[i - 1].unix_nano(), self[i].unix_nano());
            if av >= ab {
                return false;
            }
        }
        true
    }

    fn assert_ordered(&self) {
        if self.len() <= 1 {
            return;
        }
        for i in 1..self.len() {
            let (av, ab) = (self[i - 1].unix_nano(), self[i].unix_nano());
            if av >= ab {
                panic!("not ordered: {} {} >= {}", i, av, ab);
            }
        }
    }

    /// Returns a new slice with any values that have the same timestamp removed.
    /// The Value that appears last in the slice is the one that is kept.  The returned
    /// Values are sorted if necessary.
    pub fn deduplicate(&mut self) {
        if self.len() <= 1 {
            return;
        }

        // See if we're already sorted and deduped
        let mut need_sort = false;
        for i in 1..self.len() {
            if self[i - 1].unix_nano() >= self[i].unix_nano() {
                need_sort = true;
                break;
            }
        }

        if !need_sort {
            return;
        }

        // 1. Stable sorts data in ascending order by timestamp
        self.sort_by_key(|a| a.unix_nano());

        // 2. remove the duplicated points(timestamps are same)
        let mut i = 0;
        for j in 1..self.len() {
            let v = &self[j];
            if v.unix_nano() != self[i].unix_nano() {
                i += 1;
            }
            self[i] = v.clone();
        }

        self.truncate(i + 1);
    }

    /// Exclude the values not in [min, max].  The values must be deduplicated and sorted
    /// before calling Exclude or the results are undefined.
    pub fn exclude(&mut self, min: i64, max: i64) {
        let (rmin, mut rmax) = self.find_range(min, max);
        if rmin == -1 && rmax == -1 {
            return;
        }

        // self[rmin].unix_nano() ≥ min
        // self[rmax].unix_nano() ≥ max

        if rmax < self.len() as i64 {
            if self[rmax as usize].unix_nano() == max {
                rmax += 1;
            }
            let rest = self.len() - rmax as usize;
            if rest > 0 {
                let ptr = self.as_mut_ptr();
                unsafe {
                    std::ptr::copy(ptr.offset(rmax as isize), ptr.offset(rmin as isize), rest);
                }

                self.truncate(rmin as usize + rest);
                return;
            }
        }
        self.truncate(rmin as usize);
    }

    /// Include returns the subset values between min and max inclusive. The values must
    /// be deduplicated and sorted before calling Exclude or the results are undefined.
    pub fn include(&mut self, min: i64, max: i64) {
        let (rmin, mut rmax) = self.find_range(min, max);
        if rmin == -1 && rmax == -1 {
            return;
        }

        // a[rmin].UnixNano() ≥ min
        // a[rmax].UnixNano() ≥ max

        if (rmax as usize) < self.len() && self[rmax as usize].unix_nano() == max {
            rmax += 1;
        }

        if rmin > -1 {
            let ptr = self.as_mut_ptr();
            unsafe {
                std::ptr::copy(ptr.offset(rmin as isize), ptr, (rmax - rmin) as usize);
            }

            self.truncate((rmax - rmin) as usize);
            return;
        }

        self.truncate(rmax as usize);
    }

    /// search performs a binary search for unix_nano() v in a
    /// and returns the position, i, where v would be inserted.
    /// An additional check of self[i].unix_nano() == v is necessary
    /// to determine if the value v exists.
    pub fn search(&self, v: i64) -> usize {
        let mut lo = 0;
        let mut hi = self.len();
        while lo < hi {
            let mid = (lo + hi) >> 1;
            if self[mid].unix_nano() < v {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        // lo == hi
        lo
    }

    /// Returns the positions where min and max would be
    /// inserted into the array. If self[0].unix_nano() > max or
    /// self[len-1].unix_nano() < min then find_range returns (-1, -1)
    /// indicating the array is outside the [min, max]. The values must
    /// be deduplicated and sorted before calling Exclude or the results
    /// are undefined.
    pub fn find_range(&self, min: i64, max: i64) -> (i64, i64) {
        if self.is_empty() || min > max {
            return (-1, -1);
        }

        let min_val = self[0].unix_nano();
        let max_val = self[self.len() - 1].unix_nano();

        if max_val < min || min_val > max {
            return (-1, -1);
        }

        (self.search(min) as i64, self.search(max) as i64)
    }

    /// Merge overlays b to top of a.  If two values conflict with
    /// the same timestamp, b is used.  Both a and b must be sorted
    /// in ascending order.
    pub fn merge(&mut self, mut b: Values) {
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
        self.deduplicate();
        b.deduplicate();

        if self[self.len() - 1].unix_nano() < b[0].unix_nano() {
            self.extend_from_slice(&b);
            return;
        }

        if b[b.len() - 1].unix_nano() < self[0].unix_nano() {
            b.extend_from_slice(self);
            *self = b;
            return;
        }

        let mut out = Vec::with_capacity(self.len() + b.len());
        let mut a = self.as_slice();
        let mut b = b.as_slice();
        while !a.is_empty() && !b.is_empty() {
            if a[0].unix_nano() < b[0].unix_nano() {
                out.push(a[0].clone());
                a = &a[1..];
            } else if !b.is_empty() && a[0].unix_nano() == b[0].unix_nano() {
                a = &a[1..];
            } else {
                out.push(b[0].clone());
                b = &b[1..];
            }
        }

        if !a.is_empty() {
            out.extend_from_slice(a);
            *self = Values(out);
            return;
        }
        out.extend_from_slice(b);
        *self = Values(out);
    }

    pub fn encode(values: &[Value]) -> Result<Vec<u8>> {
        if values.is_empty() {
            panic!("unable to encode block type");
        }

        let typ = values[0].value_type();
        match typ {
            INTEGER => encode_integer_block(values).context(EncodingSnafu),
            UNSIGNED => encode_unsigned_block(values).context(EncodingSnafu),
            FLOAT => encode_float_block(values).context(EncodingSnafu),
            BOOL => encode_bool_block(values).context(EncodingSnafu),
            STRING => encode_string_block(values).context(EncodingSnafu),
            _ => Err(Error::StringErr {
                reason: format!("unsupported value type {}", typ),
            }),
        }
    }

    pub fn reefs_ql_type(&self) -> Result<FieldType> {
        ensure!(
            !self.is_empty(),
            StringErrSnafu {
                reason: "no values to infer type".to_string(),
            }
        );

        match &self[0] {
            Value::IntegerValue(_) => Ok(BLOCK_INTEGER),
            Value::UnsignedValue(_) => Ok(BLOCK_UNSIGNED),
            Value::FloatValue(_) => Ok(BLOCK_FLOAT64),
            Value::BoolValue(_) => Ok(BLOCK_BOOL),
            Value::StringValue(_) => Ok(BLOCK_STRING),
            _ => Err(Error::StringErr {
                reason: "no values to infer type".to_string(),
            }),
        }
    }
}

impl Deref for Values {
    type Target = Vec<Value>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Values {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Values> for Vec<Value> {
    fn from(value: Values) -> Self {
        value.0
    }
}

pub struct ValuesRef<'a>(pub &'a [Value]);

impl ValuesRef<'_> {
    pub fn size(&self) -> i64 {
        let mut sz = 0;
        for v in self.0 {
            sz += v.size();
        }

        sz
    }
}

/// IntegerValues represents a slice of  IntegerValue.
#[derive(Debug, Default, PartialEq)]
pub struct IntegerValues(pub Vec<IntegerValue>);

impl IntegerValues {
    pub fn min_time(&self) -> i64 {
        self[0].unix_nano
    }

    pub fn max_time(&self) -> i64 {
        self[self.len() - 1].unix_nano
    }

    pub fn size(&self) -> i64 {
        let mut sz = 0;
        for v in self.iter() {
            sz += v.size();
        }
        sz
    }

    pub fn ordered(&self) -> bool {
        if self.len() <= 1 {
            return true;
        }

        for i in 1..self.len() {
            if self[i - 1].unix_nano >= self[i].unix_nano {
                return false;
            }
        }

        true
    }

    pub fn assert_ordered(&self) {
        if self.len() <= 1 {
            return;
        }

        for i in 1..self.len() {
            if self[i - 1].unix_nano >= self[i].unix_nano {
                panic!(
                    "not ordered: {} {} >= {}",
                    i,
                    self[i - 1].unix_nano,
                    self[i].unix_nano
                );
            }
        }
    }

    /// Returns a new slice with any values that have the same timestamp removed.
    /// The Value that appears last in the slice is the one that is kept.  The returned
    /// Values are sorted if necessary.
    pub fn deduplicate(&mut self) {
        if self.len() <= 1 {
            return;
        }

        // See if we're already sorted and deduped
        let mut need_sort = false;
        for i in 1..self.len() {
            if self[i - 1].unix_nano >= self[i].unix_nano {
                need_sort = true;
                break;
            }
        }

        if !need_sort {
            return;
        }

        // 1. Stable sorts data in ascending order by timestamp
        self.sort_by_key(|a| a.unix_nano);

        // 2. remove the duplicated points(timestamps are same)
        let mut i = 0;
        for j in 1..self.len() {
            let v = &self[j];
            if v.unix_nano != self[i].unix_nano {
                i += 1;
            }
            self[i] = v.clone(); // TODO: optimeze me, use Arc<IntegerValue> ?
        }

        self.truncate(i + 1);
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
            if self[rmax as usize].unix_nano == max {
                rmax += 1;
            }
            let rest = self.len() - rmax as usize;
            if rest > 0 {
                let ptr = self.as_mut_ptr();
                unsafe {
                    std::ptr::copy(ptr.offset(rmax as isize), ptr.offset(rmin as isize), rest);
                }

                self.truncate(rmin as usize + rest);
                return;
            }
        }
        self.truncate(rmin as usize);
    }

    /// Include returns the subset values between min and max inclusive. The values must
    /// be deduplicated and sorted before calling Exclude or the results are undefined.
    pub fn include(&mut self, min: i64, max: i64) {
        let (rmin, mut rmax) = self.find_range(min, max);
        if rmin == -1 && rmax == -1 {
            *self = IntegerValues(vec![]);
            return;
        }

        // a[rmin].UnixNano() ≥ min
        // a[rmax].UnixNano() ≥ max

        if (rmax as usize) < self.len() && self[rmax as usize].unix_nano == max {
            rmax += 1;
        }

        if rmin > -1 {
            let ptr = self.as_mut_ptr();
            unsafe {
                std::ptr::copy(ptr.offset(rmin as isize), ptr, (rmax - rmin) as usize);
            }

            self.truncate((rmax - rmin) as usize);
            return;
        }

        self.truncate(rmax as usize);
    }

    /// search performs a binary search for UnixNano() v in a
    /// and returns the position, i, where v would be inserted.
    /// An additional check of a[i].UnixNano() == v is necessary
    /// to determine if the value v exists.
    pub fn search(&self, v: i64) -> usize {
        let mut lo = 0;
        let mut hi = self.len();
        while lo < hi {
            let mid = (lo + hi) >> 1;
            if self[mid].unix_nano < v {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        // lo == hi
        lo
    }

    /// Returns the positions where min and max would be
    /// inserted into the array. If a[0].UnixNano() > max or
    /// a[len-1].UnixNano() < min then FindRange returns (-1, -1)
    /// indicating the array is outside the [min, max]. The values must
    /// be deduplicated and sorted before calling Exclude or the results
    /// are undefined.
    pub fn find_range(&self, min: i64, max: i64) -> (i64, i64) {
        if self.is_empty() || min > max {
            return (-1, -1);
        }

        let min_val = self[0].unix_nano;
        let max_val = self[self.len() - 1].unix_nano;

        if max_val < min || min_val > max {
            return (-1, -1);
        }

        (self.search(min) as i64, self.search(max) as i64)
    }

    /// Merge overlays b to top of a.  If two values conflict with
    /// the same timestamp, b is used.  Both a and b must be sorted
    /// in ascending order.
    pub fn merge(&mut self, mut b: IntegerValues) {
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
        self.deduplicate();
        b.deduplicate();

        if self[self.len() - 1].unix_nano < b[0].unix_nano {
            self.extend_from_slice(&b);
            return;
        }

        if b[b.len() - 1].unix_nano < self[0].unix_nano {
            b.extend_from_slice(self);
            *self = b;
            return;
        }

        let mut out = Vec::with_capacity(self.len() + b.len());
        let mut a = self.as_slice();
        let mut b = b.as_slice();
        while !a.is_empty() && !b.is_empty() {
            if a[0].unix_nano < b[0].unix_nano {
                out.push(a[0].clone());
                a = &a[1..];
            } else if !b.is_empty() && a[0].unix_nano == b[0].unix_nano {
                a = &a[1..];
            } else {
                out.push(b[0].clone());
                b = &b[1..];
            }
        }

        if !a.is_empty() {
            out.extend_from_slice(a);
            *self = IntegerValues(out);
            return;
        }
        out.extend_from_slice(b);
        *self = IntegerValues(out);
    }

    pub fn encode(&mut self, buf: &[u8]) -> Result<Vec<u8>> {
        encode_integer_values_block(buf, self.as_slice())
    }
}

pub fn encode_integer_array_block(a: &mut IntegerArray, b: &[u8]) -> Result<Vec<u8>> {
    if a.is_empty() {
        return Ok(vec![]);
    }

    // TODO: These need to be pooled.

    let vb = integer_array_encode_all(&mut a.values).context(IntegerSnafu)?;
    let tb = time_array_encode_all(&mut a.timestamps).context(TimestampSnafu)?;

    // Prepend the first timestamp of the block in the first 8 bytes and the block
    // in the next byte, followed by the block
    Ok(pack_block(BLOCK_INTEGER, &tb, &vb))
}

fn encode_integer_values_block(buf: &[u8], values: &[IntegerValue]) -> Result<Vec<u8>> {
    if values.is_empty() {
        return Ok(vec![]);
    }

    let mut tenc = get_time_encoder(values.len());
    let mut venc = get_integer_encoder(values.len());

    for v in values {
        tenc.write(v.unix_nano);
        venc.write(v.value);
    }
    venc.flush();

    // Encoded timestamp values
    let tb = tenc.bytes().context(TimestampSnafu)?;

    // Encoded int64 values
    let vb = venc.bytes().context(IntegerSnafu)?;

    // Prepend the first timestamp of the block in the first 8 bytes and the block
    // in the next byte, followed by the block
    let b = pack_block(BLOCK_INTEGER, tb, &vb);

    put_time_encoder(tenc);
    put_integer_encoder(venc);

    Ok(b)
}

impl Deref for IntegerValues {
    type Target = Vec<IntegerValue>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for IntegerValues {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<IntegerValues> for Vec<IntegerValue> {
    fn from(value: IntegerValues) -> Self {
        value.0
    }
}

/// UnsignedValues represents a slice of  UnsignedValue.
#[derive(Debug, Default)]
pub struct UnsignedValues(pub Vec<UnsignedValue>);

impl UnsignedValues {
    pub fn min_time(&self) -> i64 {
        self[0].unix_nano()
    }

    pub fn max_time(&self) -> i64 {
        self[self.len() - 1].unix_nano()
    }

    pub fn size(&self) -> i64 {
        let mut sz = 0;
        for v in self.iter() {
            sz += v.size();
        }
        sz
    }

    pub fn ordered(&self) -> bool {
        if self.len() <= 1 {
            return true;
        }

        for i in 1..self.len() {
            if self[i - 1].unix_nano >= self[i].unix_nano {
                return false;
            }
        }

        true
    }

    pub fn assert_ordered(&self) {
        if self.len() <= 1 {
            return;
        }

        for i in 1..self.len() {
            if self[i - 1].unix_nano >= self[i].unix_nano {
                panic!(
                    "not ordered: {} {} >= {}",
                    i,
                    self[i - 1].unix_nano,
                    self[i].unix_nano
                );
            }
        }
    }

    /// Returns a new slice with any values that have the same timestamp removed.
    /// The Value that appears last in the slice is the one that is kept.  The returned
    /// Values are sorted if necessary.
    pub fn deduplicate(&mut self) {
        if self.len() <= 1 {
            return;
        }

        // See if we're already sorted and deduped
        let mut need_sort = false;
        for i in 1..self.len() {
            if self[i - 1].unix_nano >= self[i].unix_nano {
                need_sort = true;
                break;
            }
        }

        if !need_sort {
            return;
        }

        // 1. Stable sorts data in ascending order by timestamp
        self.sort_by_key(|a| a.unix_nano);

        // 2. remove the duplicated points(timestamps are same)
        let mut i = 0;
        for j in 1..self.len() {
            let v = &self[j];
            if v.unix_nano != self[i].unix_nano {
                i += 1;
            }
            self[i] = v.clone(); // TODO: optimeze me, use Arc<IntegerValue> ?
        }

        self.truncate(i + 1);
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
            if self[rmax as usize].unix_nano == max {
                rmax += 1;
            }
            let rest = self.len() - rmax as usize;
            if rest > 0 {
                let ptr = self.as_mut_ptr();
                unsafe {
                    std::ptr::copy(ptr.offset(rmax as isize), ptr.offset(rmin as isize), rest);
                }

                self.truncate(rmin as usize + rest);
                return;
            }
        }
        self.truncate(rmin as usize);
    }

    /// Include returns the subset values between min and max inclusive. The values must
    /// be deduplicated and sorted before calling Exclude or the results are undefined.
    pub fn include(&mut self, min: i64, max: i64) {
        let (rmin, mut rmax) = self.find_range(min, max);
        if rmin == -1 && rmax == -1 {
            return;
        }

        // a[rmin].UnixNano() ≥ min
        // a[rmax].UnixNano() ≥ max

        if (rmax as usize) < self.len() && self[rmax as usize].unix_nano == max {
            rmax += 1;
        }

        if rmin > -1 {
            let ptr = self.as_mut_ptr();
            unsafe {
                std::ptr::copy(ptr.offset(rmin as isize), ptr, (rmax - rmin) as usize);
            }

            self.truncate((rmax - rmin) as usize);
            return;
        }

        self.truncate(rmax as usize);
    }

    /// search performs a binary search for UnixNano() v in a
    /// and returns the position, i, where v would be inserted.
    /// An additional check of a[i].UnixNano() == v is necessary
    /// to determine if the value v exists.
    pub fn search(&self, v: i64) -> usize {
        let mut lo = 0;
        let mut hi = self.len();
        while lo < hi {
            let mid = (lo + hi) >> 1;
            if self[mid].unix_nano < v {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        // lo == hi
        lo
    }

    /// Returns the positions where min and max would be
    /// inserted into the array. If a[0].UnixNano() > max or
    /// a[len-1].UnixNano() < min then FindRange returns (-1, -1)
    /// indicating the array is outside the [min, max]. The values must
    /// be deduplicated and sorted before calling Exclude or the results
    /// are undefined.
    pub fn find_range(&self, min: i64, max: i64) -> (i64, i64) {
        if self.is_empty() || min > max {
            return (-1, -1);
        }

        let min_val = self[0].unix_nano;
        let max_val = self[self.len() - 1].unix_nano;

        if max_val < min || min_val > max {
            return (-1, -1);
        }

        (self.search(min) as i64, self.search(max) as i64)
    }

    /// Merge overlays b to top of a.  If two values conflict with
    /// the same timestamp, b is used.  Both a and b must be sorted
    /// in ascending order.
    pub fn merge(&mut self, mut b: UnsignedValues) {
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
        self.deduplicate();
        b.deduplicate();

        if self[self.len() - 1].unix_nano < b[0].unix_nano {
            self.extend_from_slice(&b);
            return;
        }

        if b[b.len() - 1].unix_nano < self[0].unix_nano {
            b.extend_from_slice(self);
            *self = b;
            return;
        }

        let mut out = Vec::with_capacity(self.len() + b.len());
        let mut a = self.as_slice();
        let mut b = b.as_slice();
        while !a.is_empty() && !b.is_empty() {
            if a[0].unix_nano < b[0].unix_nano {
                out.push(a[0].clone());
                a = &a[1..];
            } else if !b.is_empty() && a[0].unix_nano == b[0].unix_nano {
                a = &a[1..];
            } else {
                out.push(b[0].clone());
                b = &b[1..];
            }
        }

        if !a.is_empty() {
            out.extend_from_slice(a);
            *self = UnsignedValues(out);
            return;
        }
        out.extend_from_slice(b);
        *self = UnsignedValues(out);
    }

    pub fn encode(&mut self, buf: &[u8]) -> Result<Vec<u8>> {
        encode_unsigned_values_block(buf, self.as_slice())
    }
}

pub fn encode_unsigned_array_block(a: &mut UnsignedArray, b: &[u8]) -> Result<Vec<u8>> {
    if a.is_empty() {
        return Ok(vec![]);
    }

    // TODO: These need to be pooled.

    let vb = unsigned_array_encode_all(&mut a.values).context(IntegerSnafu)?;
    let tb = time_array_encode_all(&mut a.timestamps).context(TimestampSnafu)?;

    // Prepend the first timestamp of the block in the first 8 bytes and the block
    // in the next byte, followed by the block
    Ok(pack_block(BLOCK_UNSIGNED, &tb, &vb))
}

fn encode_unsigned_values_block(buf: &[u8], values: &[UnsignedValue]) -> Result<Vec<u8>> {
    if values.is_empty() {
        return Ok(vec![]);
    }

    let mut tenc = get_time_encoder(values.len());
    let mut venc = get_unsigned_encoder(values.len());

    for v in values {
        tenc.write(v.unix_nano);
        venc.write(v.value as i64);
    }
    venc.flush();

    // Encoded timestamp values
    let tb = tenc.bytes().context(TimestampSnafu)?;

    // Encoded int64 values
    let vb = venc.bytes().context(IntegerSnafu)?;

    // Prepend the first timestamp of the block in the first 8 bytes and the block
    // in the next byte, followed by the block
    let b = pack_block(BLOCK_UNSIGNED, tb, &vb);

    put_time_encoder(tenc);
    put_unsigned_encoder(venc);

    Ok(b)
}

impl Deref for UnsignedValues {
    type Target = Vec<UnsignedValue>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for UnsignedValues {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<UnsignedValues> for Vec<UnsignedValue> {
    fn from(value: UnsignedValues) -> Self {
        value.0
    }
}

/// FloatValues represents a slice of  FloatValue.
#[derive(Debug, Default)]
pub struct FloatValues(pub Vec<FloatValue>);

impl FloatValues {
    pub fn min_time(&self) -> i64 {
        self[0].unix_nano
    }

    pub fn max_time(&self) -> i64 {
        self[self.len() - 1].unix_nano
    }

    pub fn size(&self) -> i64 {
        let mut sz = 0;
        for v in self.iter() {
            sz += v.size();
        }
        sz
    }

    pub fn ordered(&self) -> bool {
        if self.len() <= 1 {
            return true;
        }

        for i in 1..self.len() {
            if self[i - 1].unix_nano >= self[i].unix_nano {
                return false;
            }
        }

        true
    }

    pub fn assert_ordered(&self) {
        if self.len() <= 1 {
            return;
        }

        for i in 1..self.len() {
            if self[i - 1].unix_nano >= self[i].unix_nano {
                panic!(
                    "not ordered: {} {} >= {}",
                    i,
                    self[i - 1].unix_nano,
                    self[i].unix_nano
                );
            }
        }
    }

    /// Returns a new slice with any values that have the same timestamp removed.
    /// The Value that appears last in the slice is the one that is kept.  The returned
    /// Values are sorted if necessary.
    pub fn deduplicate(&mut self) {
        if self.len() <= 1 {
            return;
        }

        // See if we're already sorted and deduped
        let mut need_sort = false;
        for i in 1..self.len() {
            if self[i - 1].unix_nano >= self[i].unix_nano {
                need_sort = true;
                break;
            }
        }

        if !need_sort {
            return;
        }

        // 1. Stable sorts data in ascending order by timestamp
        // self.sort_by_key(|a| a.unix_nano);
        self.sort_by(|a, b| a.unix_nano.cmp(&b.unix_nano));

        // 2. remove the duplicated points(timestamps are same)
        let mut i = 0;
        for j in 1..self.len() {
            let v = &self[j];
            if v.unix_nano != self[i].unix_nano {
                i += 1;
            }
            self[i] = v.clone(); // TODO: optimeze me, use Arc<IntegerValue> ?
        }

        self.truncate(i + 1);
    }

    // Removes the subset of values not in [min, max].  The values must
    // be deduplicated and sorted before calling Exclude or the results are undefined.
    pub fn exclude(&mut self, min: i64, max: i64) {
        let (rmin, mut rmax) = self.find_range(min, max);
        if rmin == -1 && rmax == -1 {
            return;
        }

        // self[rmin].unix_nano() ≥ min
        // self[rmax].unix_nano() ≥ max

        if rmax < self.len() as i64 {
            if self[rmax as usize].unix_nano == max {
                rmax += 1;
            }
            let rest = self.len() - rmax as usize;
            if rest > 0 {
                let ptr = self.as_mut_ptr();
                unsafe {
                    std::ptr::copy(ptr.offset(rmax as isize), ptr.offset(rmin as isize), rest);
                }

                self.truncate(rmin as usize + rest);
                return;
            }
        }
        self.truncate(rmin as usize);
    }

    /// Include returns the subset values between min and max inclusive. The values must
    /// be deduplicated and sorted before calling Exclude or the results are undefined.
    pub fn include(&mut self, min: i64, max: i64) {
        let (rmin, mut rmax) = self.find_range(min, max);
        if rmin == -1 && rmax == -1 {
            self.clear();
            return;
        }

        // a[rmin].UnixNano() ≥ min
        // a[rmax].UnixNano() ≥ max

        if (rmax as usize) < self.len() && self[rmax as usize].unix_nano == max {
            rmax += 1;
        }

        if rmin > -1 {
            let ptr = self.as_mut_ptr();
            unsafe {
                std::ptr::copy(ptr.offset(rmin as isize), ptr, (rmax - rmin) as usize);
            }

            self.truncate((rmax - rmin) as usize);
            return;
        }

        self.truncate(rmax as usize);
    }

    /// search performs a binary search for UnixNano() v in a
    /// and returns the position, i, where v would be inserted.
    /// An additional check of a[i].UnixNano() == v is necessary
    /// to determine if the value v exists.
    pub fn search(&self, v: i64) -> usize {
        let mut lo = 0;
        let mut hi = self.len();
        while lo < hi {
            let mid = (lo + hi) >> 1;
            if self[mid].unix_nano < v {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        // lo == hi
        lo
    }

    /// Returns the positions where min and max would be
    /// inserted into the array. If a[0].UnixNano() > max or
    /// a[len-1].UnixNano() < min then FindRange returns (-1, -1)
    /// indicating the array is outside the [min, max]. The values must
    /// be deduplicated and sorted before calling Exclude or the results
    /// are undefined.
    pub fn find_range(&self, min: i64, max: i64) -> (i64, i64) {
        if self.is_empty() || min > max {
            return (-1, -1);
        }

        let min_val = self[0].unix_nano;
        let max_val = self[self.len() - 1].unix_nano;

        if max_val < min || min_val > max {
            return (-1, -1);
        }

        (self.search(min) as i64, self.search(max) as i64)
    }

    /// Merge overlays b to top of a.  If two values conflict with
    /// the same timestamp, b is used.  Both a and b must be sorted
    /// in ascending order.
    pub fn merge(&mut self, mut b: FloatValues) {
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
        self.deduplicate();
        b.deduplicate();

        if self[self.len() - 1].unix_nano < b[0].unix_nano {
            self.extend_from_slice(&b);
            return;
        }

        if b[b.len() - 1].unix_nano < self[0].unix_nano {
            b.extend_from_slice(self);
            *self = b;
            return;
        }

        let mut out = Vec::with_capacity(self.len() + b.len());
        let mut a = self.as_slice();
        let mut b = b.as_slice();
        while !a.is_empty() && !b.is_empty() {
            if a[0].unix_nano < b[0].unix_nano {
                out.push(a[0].clone());
                a = &a[1..];
            } else if !b.is_empty() && a[0].unix_nano == b[0].unix_nano {
                a = &a[1..];
            } else {
                out.push(b[0].clone());
                b = &b[1..];
            }
        }

        if !a.is_empty() {
            out.extend_from_slice(a);
            *self = FloatValues(out);
            return;
        }
        out.extend_from_slice(b);
        *self = FloatValues(out);
    }

    pub fn encode(&mut self, buf: &[u8]) -> Result<Vec<u8>> {
        encode_float_values_block(buf, self.as_slice())
    }
}

pub fn encode_float_array_block(a: &mut FloatArray, b: &[u8]) -> Result<Vec<u8>> {
    if a.is_empty() {
        return Ok(vec![]);
    }

    // TODO: These need to be pooled.

    let vb = float_array_encode_all(&a.values).context(FloatSnafu)?;
    let tb = time_array_encode_all(&mut a.timestamps).context(TimestampSnafu)?;

    // Prepend the first timestamp of the block in the first 8 bytes and the block
    // in the next byte, followed by the block
    Ok(pack_block(BLOCK_FLOAT64, &tb, &vb))
}

fn encode_float_values_block(buf: &[u8], values: &[FloatValue]) -> Result<Vec<u8>> {
    if values.is_empty() {
        return Ok(vec![]);
    }

    let mut tenc = get_time_encoder(values.len());
    let mut venc = get_float_encoder(values.len());

    for v in values {
        tenc.write(v.unix_nano);
        venc.write(v.value);
    }
    venc.flush();

    // Encoded timestamp values
    let tb = tenc.bytes().context(TimestampSnafu)?;

    // Encoded int64 values
    let vb = venc.bytes().context(FloatSnafu)?;

    // Prepend the first timestamp of the block in the first 8 bytes and the block
    // in the next byte, followed by the block
    let b = pack_block(BLOCK_FLOAT64, tb, vb);

    put_time_encoder(tenc);
    put_float_encoder(venc);

    Ok(b)
}

impl Deref for FloatValues {
    type Target = Vec<FloatValue>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for FloatValues {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<FloatValues> for Vec<FloatValue> {
    fn from(value: FloatValues) -> Self {
        value.0
    }
}

/// BoolValues represents a slice of  BoolValue.
#[derive(Debug, Default)]
pub struct BoolValues(pub Vec<BoolValue>);

impl BoolValues {
    pub fn min_time(&self) -> i64 {
        self[0].unix_nano
    }

    pub fn max_time(&self) -> i64 {
        self[self.len() - 1].unix_nano
    }

    pub fn size(&self) -> i64 {
        let mut sz = 0;
        for v in self.iter() {
            sz += v.size();
        }
        sz
    }

    pub fn ordered(&self) -> bool {
        if self.len() <= 1 {
            return true;
        }

        for i in 1..self.len() {
            if self[i - 1].unix_nano >= self[i].unix_nano {
                return false;
            }
        }

        true
    }

    pub fn assert_ordered(&self) {
        if self.len() <= 1 {
            return;
        }

        for i in 1..self.len() {
            if self[i - 1].unix_nano >= self[i].unix_nano {
                panic!(
                    "not ordered: {} {} >= {}",
                    i,
                    self[i - 1].unix_nano,
                    self[i].unix_nano
                );
            }
        }
    }

    /// Returns a new slice with any values that have the same timestamp removed.
    /// The Value that appears last in the slice is the one that is kept.  The returned
    /// Values are sorted if necessary.
    pub fn deduplicate(&mut self) {
        if self.len() <= 1 {
            return;
        }

        // See if we're already sorted and deduped
        let mut need_sort = false;
        for i in 1..self.len() {
            if self[i - 1].unix_nano >= self[i].unix_nano {
                need_sort = true;
                break;
            }
        }

        if !need_sort {
            return;
        }

        // 1. Stable sorts data in ascending order by timestamp
        self.sort_by_key(|a| a.unix_nano);

        // 2. remove the duplicated points(timestamps are same)
        let mut i = 0;
        for j in 1..self.len() {
            let v = &self[j];
            if v.unix_nano != self[i].unix_nano {
                i += 1;
            }
            self[i] = v.clone(); // TODO: optimeze me, use Arc<IntegerValue> ?
        }

        self.truncate(i + 1);
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
            if self[rmax as usize].unix_nano == max {
                rmax += 1;
            }
            let rest = self.len() - rmax as usize;
            if rest > 0 {
                let ptr = self.as_mut_ptr();
                unsafe {
                    std::ptr::copy(ptr.offset(rmax as isize), ptr.offset(rmin as isize), rest);
                }

                self.truncate(rmin as usize + rest);
                return;
            }
        }
        self.truncate(rmin as usize);
    }

    /// Include returns the subset values between min and max inclusive. The values must
    /// be deduplicated and sorted before calling Exclude or the results are undefined.
    pub fn include(&mut self, min: i64, max: i64) {
        let (rmin, mut rmax) = self.find_range(min, max);
        if rmin == -1 && rmax == -1 {
            return;
        }

        // a[rmin].UnixNano() ≥ min
        // a[rmax].UnixNano() ≥ max

        if (rmax as usize) < self.len() && self[rmax as usize].unix_nano == max {
            rmax += 1;
        }

        if rmin > -1 {
            let ptr = self.as_mut_ptr();
            unsafe {
                std::ptr::copy(ptr.offset(rmin as isize), ptr, (rmax - rmin) as usize);
            }

            self.truncate((rmax - rmin) as usize);
            return;
        }

        self.truncate(rmax as usize);
    }

    /// search performs a binary search for UnixNano() v in a
    /// and returns the position, i, where v would be inserted.
    /// An additional check of a[i].UnixNano() == v is necessary
    /// to determine if the value v exists.
    pub fn search(&self, v: i64) -> usize {
        let mut lo = 0;
        let mut hi = self.len();
        while lo < hi {
            let mid = (lo + hi) >> 1;
            if self[mid].unix_nano < v {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        // lo == hi
        lo
    }

    /// Returns the positions where min and max would be
    /// inserted into the array. If a[0].UnixNano() > max or
    /// a[len-1].UnixNano() < min then FindRange returns (-1, -1)
    /// indicating the array is outside the [min, max]. The values must
    /// be deduplicated and sorted before calling Exclude or the results
    /// are undefined.
    pub fn find_range(&self, min: i64, max: i64) -> (i64, i64) {
        if self.is_empty() || min > max {
            return (-1, -1);
        }

        let min_val = self[0].unix_nano;
        let max_val = self[self.len() - 1].unix_nano;

        if max_val < min || min_val > max {
            return (-1, -1);
        }

        (self.search(min) as i64, self.search(max) as i64)
    }

    /// Merge overlays b to top of a.  If two values conflict with
    /// the same timestamp, b is used.  Both a and b must be sorted
    /// in ascending order.
    pub fn merge(&mut self, mut b: BoolValues) {
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
        self.deduplicate();
        b.deduplicate();

        if self[self.len() - 1].unix_nano < b[0].unix_nano {
            self.extend_from_slice(&b);
            return;
        }

        if b[b.len() - 1].unix_nano < self[0].unix_nano {
            b.extend_from_slice(self);
            *self = b;
            return;
        }

        let mut out = Vec::with_capacity(self.len() + b.len());
        let mut a = self.as_slice();
        let mut b = b.as_slice();
        while !a.is_empty() && !b.is_empty() {
            if a[0].unix_nano < b[0].unix_nano {
                out.push(a[0].clone());
                a = &a[1..];
            } else if !b.is_empty() && a[0].unix_nano == b[0].unix_nano {
                a = &a[1..];
            } else {
                out.push(b[0].clone());
                b = &b[1..];
            }
        }

        if !a.is_empty() {
            out.extend_from_slice(a);
            *self = BoolValues(out);
            return;
        }
        out.extend_from_slice(b);
        *self = BoolValues(out);
    }

    pub fn encode(&mut self, buf: &[u8]) -> Result<Vec<u8>> {
        encode_bool_values_block(buf, self.as_slice())
    }
}

pub fn encode_bool_array_block(a: &mut BoolArray, b: &[u8]) -> Result<Vec<u8>> {
    if a.is_empty() {
        return Ok(vec![]);
    }

    // TODO: These need to be pooled.

    let vb = bool_array_encode_all(&a.values).context(BoolSnafu)?;
    let tb = time_array_encode_all(&mut a.timestamps).context(TimestampSnafu)?;

    // Prepend the first timestamp of the block in the first 8 bytes and the block
    // in the next byte, followed by the block
    Ok(pack_block(BLOCK_BOOL, &tb, &vb))
}

fn encode_bool_values_block(buf: &[u8], values: &[BoolValue]) -> Result<Vec<u8>> {
    if values.is_empty() {
        return Ok(vec![]);
    }

    let mut tenc = get_time_encoder(values.len());
    let mut venc = get_bool_encoder(values.len());

    for v in values {
        tenc.write(v.unix_nano);
        venc.write(v.value);
    }
    venc.flush();

    // Encoded timestamp values
    let tb = tenc.bytes().context(TimestampSnafu)?;

    // Encoded int64 values
    let vb = venc.bytes().context(BoolSnafu)?;

    // Prepend the first timestamp of the block in the first 8 bytes and the block
    // in the next byte, followed by the block
    let b = pack_block(BLOCK_BOOL, tb, &vb);

    put_time_encoder(tenc);
    put_bool_encoder(venc);

    Ok(b)
}

impl Deref for BoolValues {
    type Target = Vec<BoolValue>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for BoolValues {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<BoolValues> for Vec<BoolValue> {
    fn from(value: BoolValues) -> Self {
        value.0
    }
}
/// StringValues represents a slice of  StringValue.
#[derive(Debug, Default)]
pub struct StringValues(pub Vec<StringValue>);

impl StringValues {
    pub fn min_time(&self) -> i64 {
        self[0].unix_nano
    }

    pub fn max_time(&self) -> i64 {
        self[self.len() - 1].unix_nano
    }

    pub fn size(&self) -> i64 {
        let mut sz = 0;
        for v in self.iter() {
            sz += v.size();
        }
        sz
    }

    pub fn ordered(&self) -> bool {
        if self.len() <= 1 {
            return true;
        }

        for i in 1..self.len() {
            if self[i - 1].unix_nano >= self[i].unix_nano {
                return false;
            }
        }

        true
    }

    pub fn assert_ordered(&self) {
        if self.len() <= 1 {
            return;
        }

        for i in 1..self.len() {
            if self[i - 1].unix_nano >= self[i].unix_nano {
                panic!(
                    "not ordered: {} {} >= {}",
                    i,
                    self[i - 1].unix_nano,
                    self[i].unix_nano
                );
            }
        }
    }

    /// Returns a new slice with any values that have the same timestamp removed.
    /// The Value that appears last in the slice is the one that is kept.  The returned
    /// Values are sorted if necessary.
    pub fn deduplicate(&mut self) {
        if self.len() <= 1 {
            return;
        }

        // See if we're already sorted and deduped
        let mut need_sort = false;
        for i in 1..self.len() {
            if self[i - 1].unix_nano >= self[i].unix_nano {
                need_sort = true;
                break;
            }
        }

        if !need_sort {
            return;
        }

        // 1. Stable sorts data in ascending order by timestamp
        self.sort_by_key(|a| a.unix_nano);

        // 2. remove the duplicated points(timestamps are same)
        let mut i = 0;
        for j in 1..self.len() {
            let v = &self[j];
            if v.unix_nano != self[i].unix_nano {
                i += 1;
            }
            self[i] = v.clone(); // TODO: optimeze me, use Arc<IntegerValue> ?
        }

        self.truncate(i + 1);
    }

    /// Removes values in the inclusive range `[min, max]`. The values must be
    /// deduplicated and sorted before calling `exclude` or the results are undefined.
    pub fn exclude(&mut self, min: i64, max: i64) {
        let (rmin, mut rmax) = self.find_range(min, max);
        if rmin == -1 && rmax == -1 {
            return;
        }

        // self[rmin].unix_nano() ≥ min
        // self[rmax].unix_nano() ≥ max

        if rmax < self.len() as i64 {
            if self[rmax as usize].unix_nano == max {
                rmax += 1;
            }
        }
        self.drain(rmin as usize..rmax as usize);
    }

    /// Include returns the subset values between min and max inclusive. The values must
    /// be deduplicated and sorted before calling Exclude or the results are undefined.
    pub fn include(&mut self, min: i64, max: i64) {
        let (rmin, mut rmax) = self.find_range(min, max);
        if rmin == -1 && rmax == -1 {
            return;
        }

        // a[rmin].UnixNano() ≥ min
        // a[rmax].UnixNano() ≥ max

        if (rmax as usize) < self.len() && self[rmax as usize].unix_nano == max {
            rmax += 1;
        }

        if rmin > -1 {
            *self = StringValues(self.drain(rmin as usize..rmax as usize).collect::<Vec<_>>());
            return;
        }

        self.truncate(rmax as usize);
    }

    /// search performs a binary search for UnixNano() v in a
    /// and returns the position, i, where v would be inserted.
    /// An additional check of a[i].UnixNano() == v is necessary
    /// to determine if the value v exists.
    pub fn search(&self, v: i64) -> usize {
        let mut lo = 0;
        let mut hi = self.len();
        while lo < hi {
            let mid = (lo + hi) >> 1;
            if self[mid].unix_nano < v {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        // lo == hi
        lo
    }

    /// Returns the positions where min and max would be
    /// inserted into the array. If a[0].UnixNano() > max or
    /// a[len-1].UnixNano() < min then FindRange returns (-1, -1)
    /// indicating the array is outside the [min, max]. The values must
    /// be deduplicated and sorted before calling Exclude or the results
    /// are undefined.
    pub fn find_range(&self, min: i64, max: i64) -> (i64, i64) {
        if self.is_empty() || min > max {
            return (-1, -1);
        }

        let min_val = self[0].unix_nano;
        let max_val = self[self.len() - 1].unix_nano;

        if max_val < min || min_val > max {
            return (-1, -1);
        }

        (self.search(min) as i64, self.search(max) as i64)
    }

    /// Merge overlays b to top of a.  If two values conflict with
    /// the same timestamp, b is used.  Both a and b must be sorted
    /// in ascending order.
    pub fn merge(&mut self, mut b: StringValues) {
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
        self.deduplicate();
        b.deduplicate();

        if self[self.len() - 1].unix_nano < b[0].unix_nano {
            self.extend_from_slice(&b);
            return;
        }

        if b[b.len() - 1].unix_nano < self[0].unix_nano {
            b.extend_from_slice(self);
            *self = b;
            return;
        }

        let mut out = Vec::with_capacity(self.len() + b.len());
        let mut a = self.as_slice();
        let mut b = b.as_slice();
        while !a.is_empty() && !b.is_empty() {
            if a[0].unix_nano < b[0].unix_nano {
                out.push(a[0].clone());
                a = &a[1..];
            } else if !b.is_empty() && a[0].unix_nano == b[0].unix_nano {
                a = &a[1..];
            } else {
                out.push(b[0].clone());
                b = &b[1..];
            }
        }

        if !a.is_empty() {
            out.extend_from_slice(a);
            *self = StringValues(out);
            return;
        }
        out.extend_from_slice(b);
        *self = StringValues(out);
    }

    pub fn encode(&mut self, buf: &[u8]) -> Result<Vec<u8>> {
        encode_string_values_block(buf, self.as_slice())
    }
}

pub fn encode_string_array_block(a: &mut StringArray, b: &[u8]) -> Result<Vec<u8>> {
    if a.is_empty() {
        return Ok(vec![]);
    }

    // TODO: These need to be pooled.

    let vb = string_array_encode_all(&a.values).context(StringSnafu)?;
    let tb = time_array_encode_all(&mut a.timestamps).context(TimestampSnafu)?;

    // Prepend the first timestamp of the block in the first 8 bytes and the block
    // in the next byte, followed by the block
    Ok(pack_block(BLOCK_STRING, &tb, &vb))
}

fn encode_string_values_block(buf: &[u8], values: &[StringValue]) -> Result<Vec<u8>> {
    if values.is_empty() {
        return Ok(vec![]);
    }

    let mut tenc = get_time_encoder(values.len());
    let mut venc = get_string_encoder(values.len());

    for v in values {
        tenc.write(v.unix_nano);
        venc.write(v.value.as_bytes());
    }
    venc.flush();

    // Encoded timestamp values
    let tb = tenc.bytes().context(TimestampSnafu)?;

    // Encoded int64 values
    let vb = venc.bytes().context(StringSnafu)?;

    // Prepend the first timestamp of the block in the first 8 bytes and the block
    // in the next byte, followed by the block
    let b = pack_block(BLOCK_STRING, tb, &vb);

    put_time_encoder(tenc);
    put_string_encoder(venc);

    Ok(b)
}

impl Deref for StringValues {
    type Target = Vec<StringValue>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for StringValues {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<StringValues> for Vec<StringValue> {
    fn from(value: StringValues) -> Self {
        value.0
    }
}

#[cfg(any(test, feature = "benchmarking"))]
pub mod tests {

    use crate::models::points::{IntegerValue, StringValue};

    use super::{IntegerValues, StringValues};

    pub fn make_integer_values(count: i32, min: i64, max: i64) -> IntegerValues {
        let mut vals = IntegerValues(vec![]);

        let mut ts = min;
        let inc = (max - min) / (count as i64);

        for i in 0..count {
            vals.push(IntegerValue {
                unix_nano: ts,
                value: 0,
            });
            ts += inc;
        }

        vals
    }

    pub fn make_integer_values_from_slice(ts: &[i64]) -> IntegerValues {
        let mut vals = IntegerValues(vec![]);

        for t in ts {
            vals.push(IntegerValue {
                unix_nano: *t,
                value: 0,
            });
        }

        vals
    }

    fn make_string_values_from_slice(timestamps: &[i64]) -> StringValues {
        StringValues(
            timestamps
                .iter()
                .map(|timestamp| StringValue::new(*timestamp, format!("value_{timestamp}")))
                .collect(),
        )
    }

    fn string_value_timestamps(values: &StringValues) -> Vec<i64> {
        values.iter().map(|value| value.unix_nano).collect()
    }

    #[test]
    fn test_integer_values_find_range_no_values() {
        let vals = IntegerValues::default();
        let (l, r) = vals.find_range(0, 100);

        assert_eq!(-1, l, "invalid l, exp={}, got={}", -1, l);
        assert_eq!(-1, r, "invalid l, exp={}, got={}", -1, r);
    }

    #[test]
    fn test_integer_values_find_range() {
        let vals = make_integer_values_from_slice(&[10, 11, 13, 15, 17, 20, 21]);

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

        for c in cases {
            let (l, r) = vals.find_range(c.min, c.max);

            assert_eq!(c.l, l, "invalid l, exp={}, got={}", c.l, l);
            assert_eq!(c.r, r, "invalid l, exp={}, got={}", c.r, r);
        }
    }

    #[test]
    fn test_integer_values_exclude() {
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
                min: 16,
                max: 18,
                exp: vec![10, 12, 14],
            },
        ];

        for c in cases {
            let mut vals = make_integer_values(5, 10, 20);

            vals.exclude(c.min, c.max);

            let mut got = vec![];
            for v in vals.iter() {
                got.push(v.unix_nano);
            }

            assert_eq!(c.exp, got);
        }
    }

    #[test]
    fn test_integer_values_include() {
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

        for c in cases {
            let mut vals = make_integer_values(5, 10, 20);

            vals.include(c.min, c.max);

            let mut got = vec![];
            for v in vals.iter() {
                got.push(v.unix_nano);
            }

            assert_eq!(c.exp, got);
        }
    }

    #[test]
    fn test_string_values_exclude_sparse_prefix_regression() {
        let mut values = make_string_values_from_slice(&[1, 3, 7]);

        values.exclude(i64::MIN, 1);

        assert_eq!(string_value_timestamps(&values), vec![3, 7]);
        assert_eq!(
            values
                .iter()
                .map(|value| value.value.as_str())
                .collect::<Vec<_>>(),
            vec!["value_3", "value_7"]
        );
    }

    #[test]
    fn test_string_values_exclude_sparse_boundaries() {
        let cases = [
            (vec![], 0, 0, vec![]),
            (vec![10], 10, 10, vec![]),
            (vec![10], 0, 9, vec![10]),
            (vec![10], 11, 20, vec![10]),
            (vec![1, 3, 7, 10], i64::MIN, 1, vec![3, 7, 10]),
            (vec![1, 3, 7, 10], i64::MIN, 2, vec![3, 7, 10]),
            (vec![1, 3, 7, 10], 3, 7, vec![1, 10]),
            (vec![1, 3, 7, 10], 4, 6, vec![1, 3, 7, 10]),
            (vec![1, 3, 7, 10], 8, i64::MAX, vec![1, 3, 7]),
            (vec![1, 3, 7, 10], 10, i64::MAX, vec![1, 3, 7]),
            (vec![1, 3, 7, 10], i64::MIN, i64::MAX, vec![]),
        ];

        for (timestamps, min, max, expected) in cases {
            let mut values = make_string_values_from_slice(&timestamps);
            values.exclude(min, max);
            assert_eq!(
                string_value_timestamps(&values),
                expected,
                "excluded range [{min}, {max}] from {timestamps:?}"
            );
        }
    }

    #[test]
    fn test_string_values_exclude_sparse_randomized_differential() {
        let mut state = 0x5350_4152_5345_5255_u64;
        let mut timestamp = -100_i64;
        let mut timestamps = Vec::new();
        for _ in 0..32 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            timestamp += (state % 11 + 1) as i64;
            timestamps.push(timestamp);
        }

        for _ in 0..128 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let first = -120 + (state % 360) as i64;
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let second = -120 + (state % 360) as i64;
            let (min, max) = (first.min(second), first.max(second));
            let expected = timestamps
                .iter()
                .copied()
                .filter(|timestamp| *timestamp < min || *timestamp > max)
                .collect::<Vec<_>>();
            let mut values = make_string_values_from_slice(&timestamps);

            values.exclude(min, max);

            assert_eq!(
                string_value_timestamps(&values),
                expected,
                "excluded range [{min}, {max}]"
            );
        }
    }
}
