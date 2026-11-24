//! Crate hll contains a HyperLogLog++ with a LogLog-Beta bias correction implementation that is adapted (mostly
//! copied) from an implementation provided by Clark DuVall
//! github.com/clarkduvall/hyperloglog.
//!
//! The differences are that the implementation in this package:
//!
//!   * uses an AMD64 optimised xxhash algorithm instead of murmur;
//!   * uses some AMD64 optimisations for things like clz;
//!   * works with []byte rather than a Hash64 interface, to reduce allocations;
//!   * implements encoding.BinaryMarshaler and encoding.BinaryUnmarshaler
//!
//! Based on some rough benchmarking, this implementation of HyperLogLog++ is
//! around twice as fast as the github.com/clarkduvall/hyperloglog implementation.

use std::{collections::HashSet, hash::Hasher};

use bytes::Buf;
use snafu::{ResultExt, Snafu, ensure};
use twox_hash::XxHash64;

/// Package hll contains a HyperLogLog++ with a LogLog-Beta bias correction implementation that is adapted (mostly
/// copied) from an implementation provided by Clark DuVall
/// github.com/clarkduvall/hyperloglog.
///
/// The differences are that the implementation in this package:
///
///   * uses an AMD64 optimised xxhash algorithm instead of murmur;
///   * uses some AMD64 optimisations for things like clz;
///   * works with []byte rather than a Hash64 interface, to reduce allocations;
///   * implements encoding.BinaryMarshaler and encoding.BinaryUnmarshaler
///
/// Based on some rough benchmarking, this implementation of HyperLogLog++ is
/// around twice as fast as the github.com/clarkduvall/hyperloglog implementation.
// use core::hash::{BuildHasher, Hash, Hasher};
use crate::common::{bits, hll::compressed::*, to_string};

// Canonical ReefsDB byte-key hash compatibility path. The implementation stays
// in `rhh` so existing persisted-index callers retain identical behavior.
pub use crate::common::rhh::{hash_key, hash_u64};

mod compressed;

// Current VERSION of HLL implementation.
const VERSION: u8 = 2;

// DEFAULT_PRECISION is the default precision.
const DEFAULT_PRECISION: u8 = 16;

/// Error type for HLL.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    /// Precision is invalid.
    #[snafu(display("precision must be between 4 and 18"))]
    PrecisionInvalid,

    #[snafu(display("precisions must be equal"))]
    PrecisionNotEqual,

    #[snafu(display("wrong type for merging: {}", tye))]
    WrongTypeForMerge { tye: u8 },

    #[snafu(display("provided buffer {:?} too short for initializing HLL sketch", data))]
    DataTooShortForInitHLLSketch { data: String },

    /// Unmarshal binary failed.
    #[snafu(display("tsdb: {}", source))]
    UnmarshalBinaryFailed { source: compressed::Error },
}

/// Result type for HLL.
pub type Result<T, E = Error> = std::result::Result<T, E>;

// FIXME: unsupported float64 precision ?
fn beta(ez: f64) -> f64 {
    let zl = (ez + 1.0).ln();
    -0.37331876643753059_f64 * ez
        + -1.41704077448122989_f64 * zl
        + 0.40729184796612533_f64 * (zl.powi(2))
        + 1.56152033906584164_f64 * (zl.powi(3))
        + -0.99242233534286128_f64 * (zl.powi(4))
        + 0.26064681399483092_f64 * (zl.powi(5))
        + -0.03053811369682807_f64 * (zl.powi(6))
        + 0.00155770210179105_f64 * (zl.powi(7))
}

pub fn hash64(data: &[u8]) -> u64 {
    let mut hasher = XxHash64::default();
    hasher.write(data);
    hasher.finish()
}

// Plus implements the Hyperloglog++ algorithm, described in the following
// paper: http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf
//
// The HyperLogLog++ algorithm provides cardinality estimations.
#[derive(Clone)]
pub struct HLL {
    // hash function used to hash values to add to the sketch.
    // hash: Box<dyn Fn(&[u8]) -> u64>,
    hash: fn(&[u8]) -> u64,

    p: u8,  // precision.
    pp: u8, // p' (sparse) precision to be used when p ∈ [4..pp] and pp < 64.

    m: u32,  // Number of substream used for stochastic averaging of stream.
    mp: u32, // m' (sparse) number of substreams.

    alpha: f64, // alpha is used for bias correction.

    sparse: bool, // Should we use a sparse sketch representation.
    tmp_set: HashSet<u32>,

    dense_list: Vec<u8>,                 // The dense representation of the HLL.
    sparse_list: Option<CompressedList>, // values that can be stored in the sparse representation.
}

impl HLL {
    /// returns a new Plus with precision p. p must be between 4 and 18.
    pub fn new(p: u8, hash: fn(&[u8]) -> u64) -> Result<Self> {
        ensure!((4..=18).contains(&p), PrecisionInvalidSnafu);

        // p' = 25 is used in the Google paper.
        let pp = 25u8;

        let mut hll = HLL {
            hash, //: Box::new(hash),

            p,
            pp,

            m: 1 << p,
            mp: 1 << pp,

            alpha: 0f64,

            sparse: true,
            tmp_set: HashSet::new(),

            dense_list: vec![],
            sparse_list: Some(new_compressed_list(1 << p)),
        };

        // Determine alpha.
        match hll.m {
            16 => hll.alpha = 0.673,
            32 => hll.alpha = 0.697,
            64 => hll.alpha = 0.709,
            _ => hll.alpha = 0.7213 / (1.0 + 1.079 / (hll.m as f64)),
        }

        Ok(hll)
    }

    // FIXME:
    /// bytes estimates the memory footprint of this HLL, in bytes.
    pub fn bytes(&self) -> i32 {
        let mut b = 0;
        // Vec<T>:
        // The order of these fields is completely unspecified;
        // Vec will allocate if and only if mem::size_of::<T>() * capacity() > 0;
        b += self.tmp_set.capacity() * 4;
        b += self.dense_list.capacity();
        if self.sparse_list.is_some() {
            let sl = self.sparse_list.as_ref().unwrap();
            b += std::mem::size_of_val(sl);
            b += sl.b.capacity();
        }
        b += std::mem::size_of_val(self); // or std::mem::size_of::<Self>() ?
        b as i32
    }

    /// Add a new value to the HLL.
    pub fn add(&mut self, v: &[u8]) {
        let x = (self.hash)(v);
        if self.sparse {
            self.tmp_set.insert(self.encode_hash(x));

            if (self.tmp_set.len() * 100) as u32 > self.m {
                self.merge_sparse();
                if self.sparse_list.as_ref().unwrap().len() as u32 > self.m {
                    self.to_normal_mut();
                }
            }
        } else {
            let i = bextr(x, 64 - self.p, self.p); // {x63,...,x64-p}
            let w = x << self.p | 1 << (self.p - 1); // {x63-p,...,x0}

            let rho = (bits::leading_zeros_64(w) as u8) + 1;
            if rho > self.dense_list[i as usize] {
                self.dense_list[i as usize] = rho;
            }
        }
    }

    /// Returns a cardinality estimate.
    pub fn count(&mut self) -> u64 {
        if self.sparse {
            self.merge_sparse();
            return self.liner_count(self.mp, self.mp - self.sparse_list.as_ref().unwrap().count)
                as u64;
        }

        let mut sum = 0.0;
        let m = self.m as f64;
        let mut count = 0.0;
        for val in self.dense_list.iter() {
            // FIXME: test_hll_marshal_unmarshal_count failed: attempt to shift left with overflow
            // sum += 1.0 / ((1u32 << *val) as f64);
            // use unchecked_shl or unbounded_shl ?
            sum += 1.0 / (1u32.wrapping_shl(*val as u32) as f64);
            if *val == 0 {
                count += 1.0;
            }
        }
        // Use LogLog-Beta bias estimation.
        ((self.alpha * m * (m - count) / (beta(count) + sum)) + 0.5) as u64
    }

    ///merge takes another HyperLogLogPlus and combines it with HyperLogLogPlus h.
    /// If HyperLogLogPlus h is using the sparse representation, it will be converted
    /// to the normal representation.
    pub fn merge(&mut self, other: Option<&HLL>) -> Result<()> {
        if other.is_none() {
            // nothing to do
            return Ok(());
        }
        let other = other.as_ref().unwrap();
        ensure!(self.p == other.p, PrecisionInvalidSnafu);

        if self.sparse {
            self.to_normal_mut();
        }

        if other.sparse {
            for k in other.tmp_set.iter() {
                let (i, r) = other.decode_hash(*k);
                if self.dense_list[i as usize] < r {
                    self.dense_list[i as usize] = r;
                }
            }
            let iter = other.sparse_list.as_ref().unwrap().iter();
            for code in iter {
                let (i, r) = other.decode_hash(code);
                if self.dense_list[i as usize] < r {
                    self.dense_list[i as usize] = r;
                }
            }
        } else {
            for (i, v) in other.dense_list.iter().enumerate() {
                if *v > self.dense_list[i] {
                    self.dense_list[i] = *v;
                }
            }
        }
        Ok(())
    }

    /// Encodes the receiver into a binary form and returns the result.
    pub fn marshal_binary(&self) -> Result<Vec<u8>> {
        let mut data: Vec<u8> = vec![];
        // Encode a version marker
        data.push(VERSION);

        // Encode precision
        data.push(self.p);

        if self.sparse {
            // It's using the sparse representation.
            data.push(1u8);

            // Add the tmp_set
            let ts_data = self.tmp_set.marshal_binary()?;
            data.extend_from_slice(&ts_data[..]);

            // Add the sparse representation
            let sdata = self.sparse_list.as_ref().unwrap().marshal_binary().unwrap(); // FIXME: 1888->1892
            data.extend_from_slice(&sdata[..]);
            return Ok(data);
        }

        // It's using the dense representation.
        data.push(0u8);
        // Add the dense sketch representation.
        let sz = self.dense_list.len();
        data.extend_from_slice(&[
            (sz >> 24) as u8,
            (sz >> 16) as u8,
            (sz >> 8) as u8,
            sz as u8,
        ]);

        // Marshal each element in the list.
        for v in self.dense_list.iter() {
            data.push(*v);
        }

        Ok(data)
    }

    fn merge_sparse(&mut self) {
        if self.tmp_set.is_empty() {
            return;
        }

        let mut keys = vec![0u32; self.tmp_set.len()];
        for (i, v) in self.tmp_set.iter().enumerate() {
            keys[i] = *v;
        }
        keys.sort();

        let mut new_list = new_compressed_list(self.m as usize);
        let mut iter = self.sparse_list.as_ref().unwrap().iter().peekable();
        let mut i = 0;
        while iter.peek().is_some() || i < keys.len() {
            if iter.peek().is_none() {
                new_list.append(keys[i]);
                i += 1;
                continue;
            }

            if i >= keys.len() {
                new_list.append(iter.next().unwrap());
                continue;
            }

            let (x1, x2) = (*iter.peek().unwrap(), keys[i]);
            match x1.cmp(&x2) {
                std::cmp::Ordering::Equal => {
                    new_list.append(iter.next().unwrap());
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    new_list.append(x2);
                    i += 1;
                }
                std::cmp::Ordering::Less => {
                    new_list.append(iter.next().unwrap());
                }
            }
        }

        self.sparse_list = Some(new_list);
        self.tmp_set = HashSet::<u32>::new();
    }

    /// Convert from sparse representation to dense representation.
    fn to_normal_mut(&mut self) {
        if !self.tmp_set.is_empty() {
            self.merge_sparse();
        }

        self.dense_list = vec![0u8; self.m as usize];
        let iter = self.sparse_list.as_ref().unwrap().iter();
        for code in iter {
            let (i, r) = self.decode_hash(code);
            if self.dense_list[i as usize] < r {
                self.dense_list[i as usize] = r;
            }
        }

        self.sparse = false;
        self.tmp_set = HashSet::<u32>::new();
        self.sparse_list = None;
    }

    /// Encode a hash to be used in the sparse representation.
    fn encode_hash(&self, x: u64) -> u32 {
        let idx = bextr(x, 64 - self.pp, self.pp) as u32;
        if bextr(x, 64 - self.pp, self.pp - self.p) == 0 {
            let zeros = bits::leading_zeros_64(
                (bextr(x, 0, 64 - self.pp) << self.pp) | (1 << (self.pp - 1)),
            ) + 1;
            return idx << 7 | ((zeros << 1) as u32) | 1;
        }

        idx << 1
    }

    /// Decode the form generated by marshal_binary.
    /// unmarshal_binary must copy the data if it wishes to retain the data after returning.
    pub fn unmarshal_binary(&mut self, data: &[u8]) -> Result<()> {
        ensure!(
            data.len() >= 12,
            DataTooShortForInitHLLSketchSnafu {
                data: to_string!(data),
            }
        );

        // Decode version. We may need this in the future if we make non-compatible changes.
        let _ = data[0];

        // Decode precision
        let p = data[1];
        ensure!((4..=18).contains(&p), PrecisionInvalidSnafu);

        let new_hll = HLL::new(p, hash64)?;

        // update self
        let _ = std::mem::replace(self, new_hll);

        // Self is now initialised with the correct precision. We just need to fill the
        // rest of the details out.
        if data[2] == 1u8 {
            // Using the sparse representation
            self.sparse = true;

            // Decode the tmp_set
            let tssz = (&data[3..7]).get_u32();
            self.tmp_set = HashSet::<u32>::with_capacity(tssz as usize);

            // Decode tssz values in total, and each value requires us to read 4 bytes.
            let ts_last_byte = (tssz * 4 + 7) as usize;
            let mut i = 7usize;
            while i < ts_last_byte {
                let k = (&data[i..i + 4]).get_u32();
                self.tmp_set.insert(k);
                i += 4;
            }

            // Decode the sparse representation.
            return self
                .sparse_list
                .as_mut()
                .unwrap()
                .unmarshal_binary(&data[ts_last_byte..])
                .context(UnmarshalBinaryFailedSnafu);
        }

        // Using the dense representation.
        self.sparse = false;
        let dsz = (&data[3..7]).get_u32();
        self.dense_list = Vec::<u8>::with_capacity(dsz as usize);
        for i in 7..dsz + 7 {
            self.dense_list.push(data[i as usize]);
        }

        Ok(())
    }

    /// Decode a hash from the sparse representation.
    fn decode_hash(&self, k: u32) -> (u32, u8) {
        let r = if k & 1 == 1 {
            (bextr32(k, 1, 6) as u8) + self.pp - self.p
        } else {
            (bits::leading_zeros_32(k << (32 - self.pp + self.p - 1)) + 1) as u8
        };

        (self.get_index(k), r)
    }

    fn get_index(&self, k: u32) -> u32 {
        if k & 1 == 1 {
            return bextr32(k, 32 - self.p, self.p);
        }
        bextr32(k, self.pp - self.p + 1, self.p)
    }

    fn liner_count(&self, m: u32, v: u32) -> f64 {
        let fm = m as f64;
        fm * ((fm / (v as f64)).ln())
    }
}

impl Default for HLL {
    fn default() -> Self {
        // creates a new Plus with the default precision and default hasher.
        HLL::new(DEFAULT_PRECISION, hash64).unwrap()
    }
}

trait BinaryMarshaler {
    fn marshal_binary(&self) -> Result<Vec<u8>>;
    // fn unmarshal_binary(&self, data: &[u8]) -> Result<()>;
}

impl BinaryMarshaler for HashSet<u32> {
    fn marshal_binary(&self) -> Result<Vec<u8>> {
        // 4 bytes for the size of the set, and 4 bytes for each key list.
        let mut data: Vec<u8> = vec![];

        // Length of the set. We only need 32 bits because the size of the set
        // couldn't exceed that on 32 bit architectures.
        let sl = self.len();
        data.extend_from_slice(&[
            (sl >> 24) as u8,
            (sl >> 16) as u8,
            (sl >> 8) as u8,
            sl as u8,
        ]);

        // Encode each element in the set.
        for k in self {
            data.extend_from_slice(&[
                (*k >> 24) as u8,
                (*k >> 16) as u8,
                (*k >> 8) as u8,
                *k as u8,
            ]);
        }

        Ok(data)
    }
}

// bextr performs a bitfield extract on v. start should be the LSB of the field
// you wish to extract, and length the number of bits to extract.
//
// For example: start=0 and length=4 for the following 64-bit word would result
// in 1111 being returned.
//
// <snip 56 bits>00011110
// returns 1110
fn bextr(v: u64, start: u8, length: u8) -> u64 {
    (v >> start) & ((1 << length) - 1)
}

fn bextr32(v: u32, start: u8, length: u8) -> u32 {
    (v >> start) & ((1 << length) - 1)
}

#[cfg(test)]
mod tests {
    use bytes::BufMut;

    use crate::common::hll::*;

    #[test]
    fn existing_hash_key_golden_values_unchanged() {
        assert_eq!(hash_key(b""), 0x10b9_24c8_ae27_1667);
        assert_eq!(hash_key(&[42]), 0x0a9e_dece_beb0_3ae4);
        assert_eq!(hash_key(b"Hello, world!\0"), 0x7b06_c531_ea43_e89f);
    }

    #[test]
    fn existing_hash_u64_golden_values_unchanged() {
        assert_eq!(hash_u64(42), 0x5d3c_69dd_c074_2421);
        assert_eq!(hash_u64(0), 0x34c9_6acd_cadb_1bbb);
    }

    fn nop_hash(buf: &[u8]) -> u64 {
        assert_eq!(buf.len(), 8, "unexpected size buf: {}", buf.len());
        (&buf[..]).get_u64() // BigEndian
    }

    fn to_byte(v: u64) -> [u8; 8] {
        let mut buf = [0u8; 8];
        (&mut buf[..]).put_u64(v);
        buf
    }

    #[test]
    fn test_hll_bytes() {
        struct TestCase {
            p: u8,
            normal: bool,
        }
        let test_cases = [
            TestCase {
                p: 4,
                normal: false,
            },
            TestCase {
                p: 5,
                normal: false,
            },
            TestCase { p: 4, normal: true },
            TestCase { p: 5, normal: true },
        ];

        for (i, test_case) in test_cases.iter().enumerate() {
            let mut h = new_test_hll(test_case.p);

            let hll_struct_overhead = std::mem::size_of_val(&h) as i32;
            let compressed_list_overhead =
                std::mem::size_of_val(h.sparse_list.as_ref().unwrap()) as i32;

            let mut expected_dense_list_capacity = 0;
            let mut expected_sparse_list_capacity = 0;
            if test_case.normal {
                (&mut h).to_normal_mut();
                // dense_list has capacity for 2^p elements, one byte each
                expected_dense_list_capacity = 2.0_f64.powf(test_case.p as f64) as i32;
                assert_eq!(
                    expected_dense_list_capacity,
                    h.dense_list.capacity() as i32,
                    "dense_list capacity: want {} got {}",
                    expected_dense_list_capacity,
                    h.dense_list.capacity()
                );
            } else {
                // sparse_list has capacity for 2^p elements, one byte each
                expected_sparse_list_capacity = 2.0f64.powf(test_case.p as f64) as i32;
                assert_eq!(
                    expected_sparse_list_capacity,
                    h.sparse_list.as_ref().unwrap().capacity() as i32,
                    "dense_list capacity: want {} got {}",
                    expected_sparse_list_capacity,
                    h.sparse_list.as_ref().unwrap().capacity()
                );

                expected_sparse_list_capacity += compressed_list_overhead;
            }

            let expected_size =
                hll_struct_overhead + expected_dense_list_capacity + expected_sparse_list_capacity;
            assert_eq!(
                expected_size,
                h.bytes(),
                "Bytes(): want {} got {}",
                expected_size,
                h.bytes()
            );
        }
    }

    #[test]
    fn test_hll_add_no_sparse() {
        let mut h = new_test_hll(16u8);
        h.to_normal_mut();

        h.add(&to_byte(0x00010fffffffffff));
        let n = h.dense_list[1];
        assert_eq!(n, 5, "want 5, but get {}", n);

        h.add(&to_byte(0x0002ffffffffffff));
        let n = h.dense_list[2];
        assert_eq!(n, 1, "want 1, but get {}", n);

        h.add(&to_byte(0x0003000000000000));
        let n = h.dense_list[3];
        assert_eq!(n, 49, "want 49, but get {}", n);

        h.add(&to_byte(0x0003000000000001));
        let n = h.dense_list[3];
        assert_eq!(n, 49, "want 49, but get {}", n);

        h.add(&to_byte(0xff03700000000000));
        let n = h.dense_list[0xff03];
        assert_eq!(n, 2, "want 2, but get {}", n);

        h.add(&to_byte(0xff03080000000000));
        let n = h.dense_list[0xff03];
        assert_eq!(n, 5, "want 5, but get {}", n);
    }

    #[test]
    fn test_hll_precision_no_spare() {
        let mut h = new_test_hll(4u8);
        h.to_normal_mut();

        h.add(&to_byte(0x1fffffffffffffff));
        let n = h.dense_list[1];
        assert_eq!(n, 1, "want 1, but get {}", n);

        h.add(&to_byte(0xffffffffffffffff));
        let n = h.dense_list[0xf];
        assert_eq!(n, 1, "want 1, but get {}", n);

        h.add(&to_byte(0x00ffffffffffffff));
        let n = h.dense_list[0];
        assert_eq!(n, 5, "want 5, but get {}", n);
    }

    #[test]
    fn test_hll_to_normal() {
        let mut h = new_test_hll(16u8);
        h.add(&to_byte(0x00010fffffffffff));
        h.to_normal_mut();
        let c = h.count();
        assert_eq!(c, 1, "want 1, but get {}", c);

        if h.sparse {
            panic!("to_normal_mut should convert to normal");
        }

        h = new_test_hll(16u8);
        h.hash = nop_hash;
        h.add(&to_byte(0x00010fffffffffff));
        h.add(&to_byte(0x0002ffffffffffff));
        h.add(&to_byte(0x0003000000000000));
        h.add(&to_byte(0x0003000000000001));
        h.add(&to_byte(0xff03700000000000));
        h.add(&to_byte(0xff03080000000000));
        h.merge_sparse();
        h.to_normal_mut();

        let n = h.dense_list[1];
        assert_eq!(n, 5, "want 5, but get {}", n);

        let n = h.dense_list[2];
        assert_eq!(n, 1, "want 1, but get {}", n);

        let n = h.dense_list[3];
        assert_eq!(n, 49, "want 49, but get {}", n);

        let n = h.dense_list[0xff03];
        assert_eq!(n, 5, "want 5, but get {}", n);
    }

    #[test]
    fn test_hll_count() {
        let mut h = new_test_hll(16u8);
        let n = h.count();
        assert_eq!(n, 0, "want 0, but get {}", n);

        h.add(&to_byte(0x00010fffffffffff));
        h.add(&to_byte(0x00020fffffffffff));
        h.add(&to_byte(0x00030fffffffffff));
        h.add(&to_byte(0x00040fffffffffff));
        h.add(&to_byte(0x00050fffffffffff));
        h.add(&to_byte(0x00050fffffffffff));

        let n = h.count();
        assert_eq!(n, 5, "want 5, but get {}", n);

        // not mutated, still returns correct count
        let n = h.count();
        assert_eq!(n, 5, "want 5, but get {}", n);

        h.add(&to_byte(0x00060fffffffffff));

        // mutated
        let n = h.count();
        assert_eq!(n, 6, "want 6, but get {}", n);
    }

    #[test]
    fn test_hll_merge_error() {
        let mut h = new_test_hll(16u8);
        let h2 = new_test_hll(10u8);

        let err = h.merge(Some(&h2));
        assert!(err.is_err(), "different precision should return error");
    }

    #[test]
    fn test_hll_merge_sparse() {
        let mut h = new_test_hll(16u8);
        h.add(&to_byte(0x00010fffffffffff));
        h.add(&to_byte(0x00020fffffffffff));
        h.add(&to_byte(0x00030fffffffffff));
        h.add(&to_byte(0x00040fffffffffff));
        h.add(&to_byte(0x00050fffffffffff));
        h.add(&to_byte(0x00050fffffffffff));

        let mut h2 = new_test_hll(16u8);
        h2.merge(Some(&h));
        let n = h2.count();
        assert_eq!(n, 5, "want 5, but get {}", n);

        assert!(!h2.sparse, "merge should convert to normal");
        assert!(h.sparse, "merge should not modify argument");

        h2.merge(Some(&h));
        let n = h2.count();
        assert_eq!(n, 5, "want 5, but get {}", n);

        h.add(&to_byte(0x00060fffffffffff));
        h.add(&to_byte(0x00070fffffffffff));
        h.add(&to_byte(0x00080fffffffffff));
        h.add(&to_byte(0x00090fffffffffff));
        h.add(&to_byte(0x000a0fffffffffff));
        h.add(&to_byte(0x000a0fffffffffff));
        let n = h.count();
        assert_eq!(n, 10, "want 10, but get {}", n);

        h2.merge(Some(&h));
        let n = h2.count();
        assert_eq!(n, 10, "want 10, but get {}", n);
    }

    #[test]
    fn test_hll_merge_normal() {
        let mut h = new_test_hll(16u8);
        h.to_normal_mut();
        h.add(&to_byte(0x00010fffffffffff));
        h.add(&to_byte(0x00020fffffffffff));
        h.add(&to_byte(0x00030fffffffffff));
        h.add(&to_byte(0x00040fffffffffff));
        h.add(&to_byte(0x00050fffffffffff));
        h.add(&to_byte(0x00050fffffffffff));

        let mut h2 = new_test_hll(16u8);
        h2.to_normal_mut();
        h2.merge(Some(&h));
        let n = h2.count();
        assert_eq!(n, 5, "want 5, but get {}", n);

        h2.merge(Some(&h));
        let n = h2.count();
        assert_eq!(n, 5, "want 5, but get {}", n);

        h.add(&to_byte(0x00060fffffffffff));
        h.add(&to_byte(0x00070fffffffffff));
        h.add(&to_byte(0x00080fffffffffff));
        h.add(&to_byte(0x00090fffffffffff));
        h.add(&to_byte(0x000a0fffffffffff));
        h.add(&to_byte(0x000a0fffffffffff));
        let n = h.count();
        assert_eq!(n, 10, "want 10, but get {}", n);

        h2.merge(Some(&h));
        let n = h2.count();
        assert_eq!(n, 10, "want 10, but get {}", n);
    }

    #[test]
    fn test_hll_merge() {
        let mut h = new_test_hll(16u8);

        let k1 = 0xf000017000000000 as u64;
        h.add(&to_byte(k1));
        if !h.tmp_set.contains(&h.encode_hash(k1)) {
            panic!("key not in hash");
        }

        let k2 = 0x000fff8f00000000 as u64;
        h.add(&to_byte(k2));
        if !h.tmp_set.contains(&h.encode_hash(k2)) {
            panic!("key not in hash");
        }

        assert_eq!(h.tmp_set.len(), 2, "want 2, but get {}", h.tmp_set.len());

        h.merge_sparse();
        assert_eq!(h.tmp_set.len(), 0, "want 0, but get {}", h.tmp_set.len());
        assert_eq!(
            h.sparse_list.as_ref().unwrap().count,
            2,
            "want 2, but get {}",
            h.sparse_list.as_ref().unwrap().count
        );

        let mut iter = h.sparse_list.as_ref().unwrap().iter();
        let n = iter.next().unwrap();
        assert_eq!(
            n,
            h.encode_hash(k2),
            "want {}, but get {}",
            h.encode_hash(k2),
            n
        );

        let n = iter.next().unwrap();
        assert_eq!(
            n,
            h.encode_hash(k1),
            "want {}, but get {}",
            h.encode_hash(k1),
            n
        );

        let k3 = 0x0f00017000000000 as u64;
        h.add(&to_byte(k3));
        if !h.tmp_set.contains(&h.encode_hash(k3)) {
            panic!("key not in hash");
        }

        h.merge_sparse();
        assert_eq!(h.tmp_set.len(), 0, "want 0, but get {}", h.tmp_set.len());
        assert_eq!(
            h.sparse_list.as_ref().unwrap().count,
            3,
            "want 3, but get {}",
            h.sparse_list.as_ref().unwrap().count
        );

        let mut iter = h.sparse_list.as_ref().unwrap().iter();
        let n = iter.next().unwrap();
        assert_eq!(
            n,
            h.encode_hash(k2),
            "want {}, but get {}",
            h.encode_hash(k2),
            n
        );

        let n = iter.next().unwrap();
        assert_eq!(
            n,
            h.encode_hash(k3),
            "want {}, but get {}",
            h.encode_hash(k3),
            n
        );

        let n = iter.next().unwrap();
        assert_eq!(
            n,
            h.encode_hash(k1),
            "want {}, but get {}",
            h.encode_hash(k1),
            n
        );

        h.add(&to_byte(k1));
        if !h.tmp_set.contains(&h.encode_hash(k1)) {
            panic!("key not in hash");
        }

        h.merge_sparse();
        assert_eq!(h.tmp_set.len(), 0, "want 0, but get {}", h.tmp_set.len());
        assert_eq!(
            h.sparse_list.as_ref().unwrap().count,
            3,
            "want 3, but get {}",
            h.sparse_list.as_ref().unwrap().count
        );

        let mut iter = h.sparse_list.as_ref().unwrap().iter();
        let n = iter.next().unwrap();
        assert_eq!(
            n,
            h.encode_hash(k2),
            "want {}, but get {}",
            h.encode_hash(k2),
            n
        );

        let n = iter.next().unwrap();
        assert_eq!(
            n,
            h.encode_hash(k3),
            "want {}, but get {}",
            h.encode_hash(k3),
            n
        );

        let n = iter.next().unwrap();
        assert_eq!(
            n,
            h.encode_hash(k1),
            "want {}, but get {}",
            h.encode_hash(k1),
            n
        );
    }

    #[test]
    fn test_hll_encode_decode() {
        let h = new_test_hll(8u8);
        let (i, r) = h.decode_hash(h.encode_hash(0xffffff8000000000));
        assert_eq!(i, 0xff, "want 0xff, but get {}", i);
        assert_eq!(r, 1, "want 1, but get {}", r);

        let (i, r) = h.decode_hash(h.encode_hash(0xff00000000000000));
        assert_eq!(i, 0xff, "want 0xff, but get {}", i);
        assert_eq!(r, 57, "want 57, but get {}", r);

        let (i, r) = h.decode_hash(h.encode_hash(0xff30000000000000));
        assert_eq!(i, 0xff, "want 0xff, but get {}", i);
        assert_eq!(r, 3, "want 3, but get {}", r);

        let (i, r) = h.decode_hash(h.encode_hash(0xaa10000000000000));
        assert_eq!(i, 0xaa, "want 0xaa, but get {}", i);
        assert_eq!(r, 4, "want 4, but get {}", r);

        let (i, r) = h.decode_hash(h.encode_hash(0xaa0f000000000000));
        assert_eq!(i, 0xaa, "want 0xaa, but get {}", i);
        assert_eq!(r, 5, "want 5, but get {}", r);
    }

    #[test]
    fn test_hll_error() {
        let err = HLL::new(3u8, hash64);
        assert!(err.is_err(), "precision 3 should return error");

        let err = HLL::new(18u8, hash64);
        assert!(err.is_ok());

        let err = HLL::new(19u8, hash64);
        assert!(err.is_err(), "precision 19 should return error");
    }

    #[test]
    fn test_hll_marshal_unmarshal_sparse() {
        let mut h = HLL::new(4u8, hash64).unwrap();
        h.sparse = true;
        h.tmp_set = std::collections::HashSet::<u32>::from([26, 40]);

        // add a bounch of values to the sparse represention
        use rand::Rng;
        for _ in 0..10 {
            h.sparse_list
                .as_mut()
                .unwrap()
                .append(rand::thread_rng().gen_range(0..u32::MAX) as u32);
        }

        let data = h.marshal_binary().unwrap();

        // peeking at the first byte should reveal the version
        let (got, exp) = (data[0], 2u8);
        assert_eq!(got, exp, "got byte {}, expected {}", got, exp);

        let mut res = HLL::default();
        res.unmarshal_binary(&data).unwrap();

        assert!(deep_equal(&h, &res));
    }

    #[test]
    fn test_hll_marshal_unmarshal_dense() {
        let mut h = HLL::new(4u8, hash64).unwrap();
        h.sparse = false;

        // add a bunch of values to the dense representation
        use rand::Rng;
        for _ in 0..10 {
            h.dense_list.push(rand::thread_rng().gen_range(0..u8::MAX));
        }

        let data = h.marshal_binary().unwrap();

        // peeking at the first byte should reveal the version
        let (got, exp) = (data[0], 2u8);
        assert_eq!(got, exp, "got byte {}, expected {}", got, exp);

        let mut res = HLL::default();
        res.unmarshal_binary(&data).unwrap();

        assert!(deep_equal(&h, &res));
    }

    // Tests that a HLL can be serialised / unserialised and keep an accurate cardinality estimate.
    #[test]
    fn test_hll_marshal_unmarshal_count() {
        let mut h = HLL::new(16u8, hash64).unwrap();
        let mut count = std::collections::HashSet::<Vec<u8>>::new();

        use rand_core::{OsRng, RngCore};
        let mut buf = vec![0u8; 8];
        for _ in 0..1000000 {
            OsRng.fill_bytes(&mut buf);

            count.insert(buf.clone());
            // Add to the hll.
            h.add(&buf);
        }

        let got_c = h.count();
        let epsilon = 15000; // 1.5%
        let (got, exp) = (
            f64::abs(((got_c as isize) - (count.len() as isize)) as f64),
            epsilon,
        );
        assert!(
            got as i32 <= exp,
            "error was {} for estimation {} and true cardinality {}",
            got,
            got_c,
            count.len()
        );

        // Serialize the hll.
        let data = h.marshal_binary().unwrap();

        // Deserialize.
        let mut h = HLL::default();
        h.unmarshal_binary(&data).unwrap();

        // The count should be the same.
        let old_c = got_c;
        let (got, exp) = (h.count(), old_c);
        assert_eq!(got, exp, "got {}, expected {}", got, exp);

        // Add some more values.
        for _ in 0..1000000 {
            OsRng.fill_bytes(&mut buf);

            count.insert(buf.clone());
            // Add to the hll.
            h.add(&buf);
        }

        // The hll should still be working correctly.
        let got_c = h.count();
        let epsilon = 30000; // 1.5%
        let (got, exp) = (
            f64::abs(((got_c as isize) - (count.len() as isize)) as f64),
            epsilon,
        );
        assert!(
            got as i32 <= exp,
            "error was {} for estimation {} and true cardinality {}",
            got,
            got_c,
            count.len()
        );
    }

    fn new_test_hll(p: u8) -> HLL {
        HLL::new(p, nop_hash).unwrap()
    }

    fn deep_equal(h1: &HLL, h2: &HLL) -> bool {
        if h1.p != h2.p
            || h1.pp != h2.pp
            || h1.m != h2.m
            || h1.mp != h2.mp
            || h1.alpha != h2.alpha
            || h1.sparse != h2.sparse
            || h1.tmp_set != h2.tmp_set
            || h1.dense_list != h2.dense_list
            || h1.sparse_list != h2.sparse_list
        {
            return false;
        }
        true
    }
}
