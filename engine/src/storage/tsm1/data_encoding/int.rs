//! Integer encoding uses two different strategies depending on the range of values in
//! the uncompressed data.  Encoded values are first encoding used zig zag encoding.
//! This interleaves positive and negative integers across a range of positive integers.
//!
//! For example, [-2,-1,0,1] becomes [3,1,0,2]. See
//! https://developers.google.com/protocol-buffers/docs/encoding?hl=en#signed-integers
//! for more information.
//!
//! If all the zig zag encoded values are less than 1 << 60 - 1, they are compressed using
//! simple8b encoding.  If any value is larger than 1 << 60 - 1, the values are stored uncompressed.
//!
//! Each encoded byte slice contains a 1 byte header followed by multiple 8 byte packed integers
//! or 8 byte uncompressed integers.  The 4 high bits of the first byte indicate the encoding type
//! for the remaining bytes.
//!
//! There are currently two encoding types that can be used with room for 16 total.  These additional
//! encoding slots are reserved for future use.  One improvement to be made is to use a patched
//! encoding such as PFOR if only a small number of values exceed the max compressed value range.  This
//! should improve compression ratios with very large integers near the ends of the int64 range.
use bytes::{Buf, BufMut};
use integer_encoding::VarInt;
use snafu::{ResultExt, Snafu};

use super::{encoding::*, simple8b};

// INT_UNCOMPRESSED is an uncompressed format using 8 bytes per point
pub const INT_UNCOMPRESSED: u8 = 0;
// INT_COMPRESSED_SIMPLE is a bit-packed format using simple8b encoding
pub const INT_COMPRESSED_SIMPLE: u8 = 1;
// INT_COMPRESSED_RLE is a run-length encoding format
pub const INT_COMPRESSED_RLE: u8 = 2;

/// Error type for Integer.
#[derive(Debug, Snafu, Clone)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    /// Simple8b encode/decode error
    #[snafu(display(" {}", source))]
    Simple8b { source: simple8b::Error },

    /// String error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },
}

/// Result type for Integer Encode/Decode.
pub type Result<T, E = Error> = std::result::Result<T, E>;

// impl From<std::io::Error> for Error {
//     fn from(err: std::io::Error) -> Error {
//         Error::IOError { reason: err }
//     }
// }

/// IntegerEncoder encodes int64s into byte slices.
pub struct IntegerEncoder {
    prev: i64,
    rle: bool,
    values: Vec<u64>,
}

impl IntegerEncoder {
    /// Returns a new integer encoder with an initial buffer of values sized at sz.
    pub fn new(sz: i64) -> Self {
        Self {
            prev: 0,
            rle: true,
            values: Vec::<u64>::with_capacity(sz as usize),
        }
    }

    /// flush is no-op
    pub fn flush(&self) {}

    /// Sets the encoder back to its initial state.
    pub fn reset(&mut self) {
        self.prev = 0;
        self.rle = true;
        self.values.clear();
    }

    /// Encodes v to the underlying buffers.
    pub fn write(&mut self, v: i64) {
        // Delta-encode each value as it's written.  This happens before
        // ZigZagEncoding because the deltas could be negative.
        let delta = v.wrapping_sub(self.prev);
        self.prev = v;
        let enc = zig_zag_encode(delta);
        if self.values.len() > 1 {
            self.rle = self.rle && self.values[self.values.len() - 1] == enc;
        }

        self.values.push(enc);
    }

    /// Returns a copy of the underlying buffer.
    pub fn bytes(&mut self) -> Result<Vec<u8>> {
        // Only run-length encode if it could reduce storage size.
        if self.rle && self.values.len() > 2 {
            return self.encode_rle();
        }

        for v in &self.values {
            // Value is too large to encode using packed format
            if *v > simple8b::MAX_VALUE {
                return self.encode_uncompressed();
            }
        }

        self.encode_packed()
    }

    fn encode_rle(&self) -> Result<Vec<u8>> {
        // Large varints can take up to 10 bytes.  We're storing 3 + 1 type byte.
        let mut b = [0u8; 31];

        // 4 high bits used for the encoding type
        b[0] = INT_COMPRESSED_RLE << 4;

        let mut i = 1;
        // The first value
        (&mut b[i..i + 8]).put_u64(self.values[0]);
        i += 8;
        // The first delta
        i += (self.values[1]).encode_var(&mut b[i..]);

        // The number of times the delta is repeated
        i += ((self.values.len() - 1) as u64).encode_var(&mut b[i..]);

        Ok(b[..i].to_vec())
    }

    fn encode_packed(&mut self) -> Result<Vec<u8>> {
        if self.values.is_empty() {
            return Ok(vec![]);
        }

        // Encode all but the first value.  Fist value is written unencoded
        // using 8 bytes.
        let encoded_count =
            simple8b::encode_all(&mut self.values[1..]).context(Simple8bSnafu)? as usize;

        let mut b = vec![0u8; 1 + (encoded_count + 1) * 8];
        // 4 high bits of first byte store the encoding type for the block
        b[0] = INT_COMPRESSED_SIMPLE << 4;

        // Write the first value since it's not part of the encoded values
        (&mut b[1..9]).put_u64(self.values[0]);

        // Write the encoded values
        for i in 0..encoded_count {
            (&mut b[9 + i * 8..9 + i * 8 + 8]).put_u64(self.values[i + 1]); // encode_all from self.values[1..]
        }

        Ok(b)
    }

    fn encode_uncompressed(&self) -> Result<Vec<u8>> {
        if self.values.is_empty() {
            return Ok(vec![]);
        }

        let mut b = vec![0u8; 1 + self.values.len() * 8];
        // 4 high bits of first byte store the encoding type for the block
        b[0] = INT_UNCOMPRESSED << 4;

        for (i, v) in self.values.iter().enumerate() {
            (&mut b[1 + i * 8..1 + i * 8 + 8]).put_u64(*v);
        }

        Ok(b)
    }
}

/// IntegerDecoder decodes a byte slice into int64s.
pub struct IntegerDecoder {
    // 240 is the maximum number of values that can be encoded into a single u65 using simple8b
    values: [u64; 240],
    bytes: Vec<u8>,
    i: i64, // TODO: I64 ?
    n: i64, // TODO: I64 ?
    prev: i64,
    first: bool,

    // The first value for a run-length encoded byte slice
    rle_first: u64,

    // The delta value for a run-length encoded byte slice
    rle_delta: u64,
    encoding: u8,
    err: Result<()>,
}

impl IntegerDecoder {
    pub fn new() -> Self {
        Self {
            values: [0u64; 240],
            bytes: vec![],
            i: 0,
            n: 0,
            prev: 0,
            first: true,
            rle_first: 0,
            rle_delta: 0,
            encoding: 0,
            err: Ok(()),
        }
    }

    /// Sets the underlying byte slice of the decoder.
    pub fn set_bytes(&mut self, b: &[u8]) -> Result<()> {
        if !b.is_empty() {
            self.encoding = b[0] >> 4;
            self.bytes = b[1..].to_vec();
        } else {
            self.encoding = 0;
            self.bytes = vec![];
        }

        self.i = 0;
        self.n = 0;
        self.prev = 0;
        self.first = true;

        self.rle_first = 0;
        self.rle_delta = 0;
        self.err = Ok(());

        Ok(())
    }

    fn decode_rle(&mut self) {
        if self.bytes.is_empty() {
            return;
        }

        if self.bytes.len() < 8 {
            self.err = Err(Error::StringErr {
                reason: "IntegerDecoder: not enough data to decode RLE starting value".to_string(),
            });
            return;
        }

        let mut i = 0;

        // Next 8 bytes is the starting value
        let first = (&self.bytes[i..i + 8]).get_u64();
        i += 8;

        // Next 1-10 bytes is the delta value
        let (value, n) = match u64::decode_var(&self.bytes[i..]) {
            Some((sz, n)) => (sz, n),
            None => {
                self.err = Err(Error::StringErr {
                    reason: "IntegerDecoder: invalid RLE delta value".to_string(),
                });
                return;
            }
        };
        i += n;

        // Last 1-10 bytes is how many times the value repeats
        let (count, n) = match u64::decode_var(&self.bytes[i..]) {
            Some((sz, n)) => (sz, n),
            None => {
                self.err = Err(Error::StringErr {
                    reason: "IntegerDecoder: invalid RLE repeat value".to_string(),
                });
                return;
            }
        };

        // Store the first value and delta value so we do not need to allocate
        // a large values slice.  We can compute the value at position d.i on
        // demand.
        self.rle_first = first;
        self.rle_delta = value;
        self.n = count as i64 + 1;
        self.i = 0;

        // We've process all the bytes
        self.bytes = vec![];
    }

    fn decode_packed(&mut self) {
        if self.bytes.is_empty() {
            return;
        }

        if self.bytes.len() < 8 {
            self.err = Err(Error::StringErr {
                reason: "IntegerDecoder: not enough data to decode packed value".to_string(),
            });
            return;
        }

        let v = (&self.bytes[0..8]).get_u64();
        // The first value is always unencoded
        if self.first {
            self.first = false;
            self.n = 1;
            self.values[0] = v;
        } else {
            let n = match simple8b::decode(&mut self.values, v) {
                Ok(n) => n,
                Err(e) => {
                    // Should never happen, only error that could be returned is if the the value to be decoded was not
                    // actually encoded by simple8b encoder.
                    self.err = Err(Error::StringErr {
                        reason: format!("failed to decode value {}: {:?}", v, e),
                    });
                    return;
                }
            };
            self.n = n as i64;
        }
        self.i = 0;
        self.bytes = self.bytes.drain(8..).collect();
    }

    fn decode_uncompressed(&mut self) {
        if self.bytes.is_empty() {
            return;
        }

        if self.bytes.len() < 8 {
            self.err = Err(Error::StringErr {
                reason: "IntegerDecoder: not enough data to decode uncompressed value".to_string(),
            });
            return;
        }

        self.values[0] = (&self.bytes[..8]).get_u64();
        self.i = 0;
        self.n = 1;
        self.bytes = self.bytes.drain(8..).collect();
    }

    pub fn err(&self) -> Result<()> {
        self.err.clone()
    }
}

impl Default for IntegerDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Iterator for IntegerDecoder {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.n && self.bytes.is_empty() {
            return None;
        }

        self.i += 1;

        if self.i >= self.n {
            match self.encoding {
                INT_UNCOMPRESSED => self.decode_uncompressed(),
                INT_COMPRESSED_SIMPLE => self.decode_packed(),
                INT_COMPRESSED_RLE => self.decode_rle(),
                _ => {
                    self.err = Err(Error::StringErr {
                        reason: format!("unknown encoding {}", self.encoding),
                    })
                }
            }
        }

        if !(self.err.is_ok() && self.i < self.n) {
            return None;
        }

        match self.encoding {
            INT_COMPRESSED_RLE => {
                let first = zig_zag_decode(self.rle_first);
                let delta = zig_zag_decode(self.rle_delta);
                // InfluxDB evaluates both int64 operations modulo 2^64.
                Some(first.wrapping_add(self.i.wrapping_mul(delta)))
            }
            _ => {
                let mut v = zig_zag_decode(self.values[self.i as usize]);
                // v is the delta encoded value, we need to add the prior value to get the original
                v = v.wrapping_add(self.prev);
                self.prev = v;
                Some(v)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::IntegerEncoder;
    use crate::storage::tsm1::data_encoding::int::{
        INT_COMPRESSED_RLE, INT_COMPRESSED_SIMPLE, INT_UNCOMPRESSED, IntegerDecoder,
    };

    #[test]
    fn test_integer_encoder_no_values() {
        let mut enc = IntegerEncoder::new(0);
        let b = enc.bytes().unwrap();

        assert!(b.is_empty(), "unexpected length: exp 0, got {}", b.len());

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);
        if dec.next().is_some() {
            panic!("unexpected next value: got Some, exp None");
        }
    }

    #[test]
    fn test_integer_encoder_one() {
        let mut enc = IntegerEncoder::new(1);
        let v1 = 1i64;

        enc.write(1);
        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            got, INT_COMPRESSED_SIMPLE,
            "encoding type mismatch: exp simple compressed, got {}",
            got
        );

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);

        let got = dec.next().unwrap();
        assert_eq!(got, v1, "read value mismatch: got {}, exp {}", got, v1);
    }

    #[test]
    fn test_integer_encoder_two() {
        let mut enc = IntegerEncoder::new(2);
        let (v1, v2) = (1i64, 2i64);

        enc.write(1);
        enc.write(2);
        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            got, INT_COMPRESSED_SIMPLE,
            "encoding type mismatch: exp simple compressed, got {}",
            got
        );

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);

        let got = dec.next().unwrap();
        assert_eq!(got, v1, "read value mismatch: got {}, exp {}", got, v1);

        let got = dec.next().unwrap();
        assert_eq!(got, v2, "read value mismatch: got {}, exp {}", got, v2);
    }

    #[test]
    fn test_integer_encoder_negative() {
        let mut enc = IntegerEncoder::new(3);
        let (v1, v2, v3) = (-2i64, 0, 1i64);

        enc.write(v1);
        enc.write(v2);
        enc.write(v3);
        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            got, INT_COMPRESSED_SIMPLE,
            "encoding type mismatch: exp simple compressed, got {}",
            got
        );

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);

        let got = dec.next().unwrap();
        assert_eq!(got, v1, "read value mismatch: got {}, exp {}", got, v1);

        let got = dec.next().unwrap();
        assert_eq!(got, v2, "read value mismatch: got {}, exp {}", got, v2);

        let got = dec.next().unwrap();
        assert_eq!(got, v3, "read value mismatch: got {}, exp {}", got, v3);
    }

    #[test]
    fn test_integer_encoder_large_range() {
        let mut enc = IntegerEncoder::new(2);
        let (v1, v2) = (i64::MIN, i64::MAX);

        enc.write(v1);
        enc.write(v2);
        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            got, INT_UNCOMPRESSED,
            "encoding type mismatch: exp uncompressed, got {}",
            got
        );

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);

        let got = dec.next().unwrap();
        assert_eq!(got, v1, "read value mismatch: got {}, exp {}", got, v1);

        let got = dec.next().unwrap();
        assert_eq!(got, v2, "read value mismatch: got {}, exp {}", got, v2);
    }

    #[test]
    fn test_integer_encoder_uncompressed() {
        let mut enc = IntegerEncoder::new(3);
        let (v1, v2, v3) = (0, 1, 1 << 60);

        enc.write(v1);
        enc.write(v2);
        enc.write(v3);
        let b = enc.bytes().unwrap();

        // 1 byte header + 3 * 8 byte values
        assert_eq!(b.len(), 25, "length mismatch: got {}, exp {}", b.len(), 25);

        let got = b[0] >> 4;
        assert_eq!(
            got, INT_UNCOMPRESSED,
            "encoding type mismatch: exp uncompressed, got {}",
            got
        );

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);

        let got = dec.next().unwrap();
        assert_eq!(got, v1, "read value mismatch: got {}, exp {}", got, v1);

        let got = dec.next().unwrap();
        assert_eq!(got, v2, "read value mismatch: got {}, exp {}", got, v2);

        let got = dec.next().unwrap();
        assert_eq!(got, v3, "read value mismatch: got {}, exp {}", got, v3);
    }

    #[test]
    fn test_integer_encoder_negative_uncompressed() {
        let values = vec![
            -2352281900722994752,
            1438442655375607923,
            -4110452567888190110,
            -1221292455668011702,
            -1941700286034261841,
            -2836753127140407751,
            1432686216250034552,
            3663244026151507025,
            -3068113732684750258,
            -1949953187327444488,
            3713374280993588804,
            3226153669854871355,
            -2093273755080502606,
            1006087192578600616,
            -2272122301622271655,
            2533238229511593671,
            -4450454445568858273,
            2647789901083530435,
            2761419461769776844,
            -1324397441074946198,
            -680758138988210958,
            94468846694902125,
            -2394093124890745254,
            -2682139311758778198,
        ];
        let mut enc = IntegerEncoder::new(256);
        for v in &values {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            got, INT_UNCOMPRESSED,
            "encoding type mismatch: exp uncompressed, got {}",
            got
        );

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);

        let mut i = 0;
        while let Some(v) = dec.next() {
            assert!(
                i <= values.len(),
                "read too many values: got {}, exp {}",
                i,
                values.len()
            );
            assert_eq!(
                values[i], v,
                "read value {} mismatch: got {}, exp {}",
                i, v, values[i]
            );
            i += 1;
        }

        assert!(
            i == values.len(),
            "failed to read enough values: got {}, exp {}",
            i,
            values.len()
        );
    }

    #[test]
    fn test_integer_encoder_all_negative() {
        let values = vec![-10, -5, -1];
        let mut enc = IntegerEncoder::new(3);
        for v in &values {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            got, INT_COMPRESSED_SIMPLE,
            "encoding type mismatch: exp simple compressed, got {}",
            got
        );

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);

        let mut i = 0;
        while let Some(v) = dec.next() {
            assert!(
                i <= values.len(),
                "read too many values: got {}, exp {}",
                i,
                values.len()
            );
            assert_eq!(
                values[i], v,
                "read value {} mismatch: got {}, exp {}",
                i, v, values[i]
            );
            i += 1;
        }

        assert!(
            i == values.len(),
            "failed to read enough values: got {}, exp {}",
            i,
            values.len()
        );
    }

    #[test]
    fn test_integer_encoder_counter_packed() {
        let values = vec![
            1e15 as i64,
            1e15 as i64 + 1,
            1e15 as i64 + 2,
            1e15 as i64 + 3,
            1e15 as i64 + 4,
            1e15 as i64 + 6,
        ];
        let mut enc = IntegerEncoder::new(16);
        for v in &values {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            got, INT_COMPRESSED_SIMPLE,
            "encoding type mismatch: exp simple compressed, got {}",
            got
        );

        // Should use 1 header byte + 2, 8 byte words if delta-encoding is used based on
        // values sizes.  Without delta-encoding, we'd get 49 bytes.
        assert!(
            b.len() == 17,
            "encoded length mismatch: got {}, exp {}",
            b.len(),
            17
        );

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);

        let mut i = 0;
        while let Some(v) = dec.next() {
            assert!(
                i <= values.len(),
                "read too many values: got {}, exp {}",
                i,
                values.len()
            );
            assert_eq!(
                values[i], v,
                "read value {} mismatch: got {}, exp {}",
                i, v, values[i]
            );
            i += 1;
        }

        assert!(
            i == values.len(),
            "failed to read enough values: got {}, exp {}",
            i,
            values.len()
        );
    }

    #[test]
    fn test_integer_encoder_counter_rle() {
        let values = vec![
            1e15 as i64,
            1e15 as i64 + 1,
            1e15 as i64 + 2,
            1e15 as i64 + 3,
            1e15 as i64 + 4,
            1e15 as i64 + 5,
        ];
        let mut enc = IntegerEncoder::new(16);
        for v in &values {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            got, INT_COMPRESSED_RLE,
            "encoding type mismatch: exp rle compressed, got {}",
            got
        );

        // Should use 1 header byte, 8 byte first value, 1 var-byte for delta and 1 var-byte for
        // count of deltas in this particular RLE.
        assert!(
            b.len() == 11,
            "encoded length mismatch: got {}, exp {}",
            b.len(),
            11
        );

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);

        let mut i = 0;
        while let Some(v) = dec.next() {
            assert!(
                i <= values.len(),
                "read too many values: got {}, exp {}",
                i,
                values.len()
            );
            assert_eq!(
                values[i], v,
                "read value {} mismatch: got {}, exp {}",
                i, v, values[i]
            );
            i += 1;
        }

        assert!(
            i == values.len(),
            "failed to read enough values: got {}, exp {}",
            i,
            values.len()
        );
    }

    #[test]
    fn test_integer_encoder_descending() {
        let values = vec![7094, 4472, 1850];
        let mut enc = IntegerEncoder::new(16);
        for v in &values {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            got, INT_COMPRESSED_RLE,
            "encoding type mismatch: exp rle compressed, got {}",
            got
        );

        // Should use 1 header byte, 8 byte first value, 1 var-byte for delta and 1 var-byte for
        // count of deltas in this particular RLE.
        assert!(
            b.len() == 12,
            "encoded length mismatch: got {}, exp {}",
            b.len(),
            12
        );

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);

        let mut i = 0;
        while let Some(v) = dec.next() {
            assert!(
                i <= values.len(),
                "read too many values: got {}, exp {}",
                i,
                values.len()
            );
            assert_eq!(
                values[i], v,
                "read value {} mismatch: got {}, exp {}",
                i, v, values[i]
            );
            i += 1;
        }

        assert!(
            i == values.len(),
            "failed to read enough values: got {}, exp {}",
            i,
            values.len()
        );
    }

    #[test]
    fn test_integer_encoder_flat() {
        let values = vec![1, 1, 1, 1];
        let mut enc = IntegerEncoder::new(16);
        for v in &values {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            got, INT_COMPRESSED_RLE,
            "encoding type mismatch: exp rle compressed, got {}",
            got
        );

        // Should use 1 header byte, 8 byte first value, 1 var-byte for delta and 1 var-byte for
        // count of deltas in this particular RLE.
        assert!(
            b.len() == 11,
            "encoded length mismatch: got {}, exp {}",
            b.len(),
            11
        );

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);

        let mut i = 0;
        while let Some(v) = dec.next() {
            assert!(
                i <= values.len(),
                "read too many values: got {}, exp {}",
                i,
                values.len()
            );
            assert_eq!(
                values[i], v,
                "read value {} mismatch: got {}, exp {}",
                i, v, values[i]
            );
            i += 1;
        }

        assert!(
            i == values.len(),
            "failed to read enough values: got {}, exp {}",
            i,
            values.len()
        );
    }

    #[test]
    fn test_integer_encoder_min_max() {
        let values = vec![i64::MIN, i64::MAX];
        let mut enc = IntegerEncoder::new(2);
        for v in &values {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            got, INT_UNCOMPRESSED,
            "encoding type mismatch: exp uncompressed, got {}",
            got
        );

        // Should use 1 header byte, 8 byte first value, 1 var-byte for delta and 1 var-byte for
        // count of deltas in this particular RLE.
        assert!(
            b.len() == 17,
            "encoded length mismatch: got {}, exp {}",
            b.len(),
            17
        );

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);

        let mut i = 0;
        while let Some(v) = dec.next() {
            assert!(
                i <= values.len(),
                "read too many values: got {}, exp {}",
                i,
                values.len()
            );
            assert_eq!(
                values[i], v,
                "read value {} mismatch: got {}, exp {}",
                i, v, values[i]
            );
            i += 1;
        }

        assert!(
            i == values.len(),
            "failed to read enough values: got {}, exp {}",
            i,
            values.len()
        );
    }

    #[test]
    fn test_integer_encoder_rand() {
        let mut values = vec![0i64; 50];
        for v in values.iter_mut() {
            *v = rand::thread_rng().gen_range(i64::MIN..=i64::MAX);
        }

        // Write values to encoder.
        let mut enc = IntegerEncoder::new(2);
        for v in &values {
            enc.write(*v);
        }

        // Retrieve encoded bytes from encoder.
        let b = enc.bytes().unwrap();

        // Read values out of decoder.
        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);

        let mut got = vec![];
        while let Some(v) = dec.next() {
            if let Err(e) = dec.err() {
                panic!("{}", e);
            }

            got.push(v);
        }

        // Verify that input and output values match.
        assert_eq!(
            values, got,
            "mismatch:\n\nexp={:?}\n\ngot={:?}\n\n",
            values, got
        );
    }

    #[test]
    fn test_integer_encoder_corrupt() {
        let cases: &[&[u8]] = &[
            b"",                     // Empty
            b"\x00abc",              // Uncompressed: less than 8 bytes
            b"\x10abc",              // Packed: less than 8 bytes
            b"\x20abc",              // RLE: less than 8 bytes
            b"\x2012345678\x90",     // RLE: valid starting value but invalid delta value
            b"\x2012345678\x01\x90", // RLE: valid starting, valid delta value, invalid repeat value
        ];

        for c in cases {
            let mut dec = IntegerDecoder::new();
            dec.set_bytes(c);
            assert!(dec.next().is_none(), "exp next == None, got Some");
        }
    }
}
