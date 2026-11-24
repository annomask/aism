//! Timestamp encoding is adaptive and based on structure of the timestamps that are encoded.  It
//! uses a combination of delta encoding, scaling and compression using simple8b, run length encoding
//! as well as falling back to no compression if needed.
//!
//! Timestamp values to be encoded should be sorted before encoding.  When encoded, the values are
//! first delta-encoded.  The first value is the starting timestamp, subsequent values are the difference
//! from the prior value.
//!
//! Timestamp resolution can also be in the nanosecond.  Many timestamps are monotonically increasing
//! and fall on even boundaries of time such as every 10s.  When the timestamps have this structure,
//! they are scaled by the largest common divisor that is also a factor of 10.  This has the effect
//! of converting very large integer deltas into very small one that can be reversed by multiplying them
// by the scaling factor.
//!
//! Using these adjusted values, if all the deltas are the same, the time range is stored using run
//! length encoding.  If run length encoding is not possible and all values are less than 1 << 60 - 1
//! (~36.5 yrs in nanosecond resolution), then the timestamps are encoded using simple8b encoding.  If
//! any value exceeds the maximum values, the deltas are stored uncompressed using 8b each.
//!
//! Each compressed byte slice has a 1 byte header indicating the compression type.  The 4 high bits
//! indicate the encoding type.  The 4 low bits are used by the encoding type.
//!
//! For run-length encoding, the 4 low bits store the log10 of the scaling factor.  The next 8 bytes are
//! the starting timestamp, next 1-10 bytes is the delta value using variable-length encoding, finally the
//! next 1-10 bytes is the count of values.
//!
//! For simple8b encoding, the 4 low bits store the log10 of the scaling factor.  The next 8 bytes is the
//! first delta value stored uncompressed, the remaining bytes are 64bit words containing compressed delta
//! values.
//!
//! For uncompressed encoding, the delta values are stored using 8 bytes each.
use bytes::{Buf, BufMut};
use integer_encoding::VarInt;
use snafu::{ResultExt, Snafu};

use super::simple8b;

// timeUncompressed is a an uncompressed format using 8 bytes per timestamp
pub(crate) const TIME_UNCOMPRESSED: u8 = 0u8;
// timeCompressedPackedSimple is a bit-packed format using simple8b encoding
pub(crate) const TIME_COMPRESSED_PACKED_SIMPLE: u8 = 1u8;
// timeCompressedRLE is a run-length encoding format
pub(crate) const TIME_COMPRESSED_RLE: u8 = 2u8;

/// Error type for Timestamp.
#[derive(Debug, Snafu, Clone)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    // /// IO error.
    // #[snafu(display("timestamp encode/decode error: {}", reason))]
    // IOError { reason: std::io::Error },
    /// Simple8b encode/decode error
    #[snafu(display(" {}", source))]
    Simple8b { source: simple8b::Error },

    /// String error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },
}

/// Result type for Simple8b.
pub type Result<T, E = Error> = std::result::Result<T, E>;

// impl From<std::io::Error> for Error {
//     fn from(err: std::io::Error) -> Error {
//         Error::IOError { reason: err }
//     }
// }

pub struct TimeEncoder {
    ts: Vec<u64>,
    bytes: Vec<u8>,
    enc: simple8b::Encoder,
}

impl TimeEncoder {
    /// Returns a TimeEncoder with an initial buffer ready to hold sz bytes.
    pub fn new(sz: i64) -> Self {
        Self {
            ts: Vec::<u64>::with_capacity(sz as usize),
            bytes: vec![],
            enc: simple8b::Encoder::new(),
        }
    }

    /// Sets the encoder back to its initial state.
    pub fn reset(&mut self) {
        self.ts.clear();
        self.bytes.clear();
        self.enc.reset();
    }

    /// Adds a timestamp to the compressed stream.
    pub fn write(&mut self, t: i64) {
        self.ts.push(t as u64);
    }

    fn reduce(&mut self) -> (u64, u64, bool) {
        let deltas = &mut self.ts;
        let deltas_len = deltas.len();

        // Starting values for max and divisor
        let (mut max, mut divisor) = (0u64, 1e12 as u64);

        // Assume run-length encoding is possible
        let mut rle = true;

        // Use raw pointers to avoid bounds checking
        let deltas_ptr = deltas.as_mut_ptr();

        // Iterate in reverse and process deltas in place
        for i in (1..deltas_len).rev() {
            unsafe {
                // Differential encoding (in place)
                let curr = *deltas_ptr.add(i);
                let prev = *deltas_ptr.add(i - 1);
                let delta = curr.wrapping_sub(prev);
                *deltas_ptr.add(i) = delta;

                // Track maximum delta
                if delta > max {
                    max = delta;
                }

                // Reduce the divisor if necessary
                while divisor > 1 && delta % divisor != 0 {
                    divisor /= 10;
                }

                // Check if deltas can be run-length encoded (i.e., if all deltas are equal)
                if i < deltas_len - 1 && delta != *deltas_ptr.add(i + 1) {
                    rle = false;
                }
            }
        }

        (max, divisor, rle)
    }

    /// Returns the encoded bytes of all written times.
    pub fn bytes(&mut self) -> Result<&[u8]> {
        if self.ts.is_empty() {
            return Ok(&[]);
        }

        // Maximum and largest common divisor.  rle is true if dts (the delta timestamps),
        // are all the same.
        let (max, div, rle) = self.reduce();

        // The deltas are all the same, so we can run-length encode them
        if rle && self.ts.len() > 1 {
            return self.encode_rle(self.ts[0], self.ts[1], div, self.ts.len());
        }

        // We can't compress this time-range, the deltas exceed 1 << 60
        if max > simple8b::MAX_VALUE {
            return self.encode_raw();
        }

        self.encode_packed(div /*, &self.ts*/)
    }

    fn encode_packed(&mut self, div: u64 /*, dst: &[u64]*/) -> Result<&[u8]> {
        let dst = &self.ts;
        // Only apply the divisor if it's greater than 1 since division is expensive.
        if div > 1 {
            for v in &dst[1..] {
                self.enc.write(*v / div).context(Simple8bSnafu)?;
            }
        } else {
            for v in &dst[1..] {
                self.enc.write(*v).context(Simple8bSnafu)?;
            }
        }

        // The compressed deltas
        let deltas = self.enc.bytes().context(Simple8bSnafu)?;
        let sz = 8 + 1 + deltas.len();
        if self.bytes.len() < sz {
            self.bytes.resize(sz, 0u8)
        }
        let b = &mut self.bytes[..sz];

        // 4 high bits used for the encoding type
        b[0] = TIME_COMPRESSED_PACKED_SIMPLE << 4;
        // 4 low bits are the log10 divisor
        b[0] |= (div as f64).log10() as u8;

        // The first delta value
        (&mut b[1..9]).put_u64(dst[0]);
        b[9..9 + deltas.len()].copy_from_slice(&deltas);

        Ok(&b[..9 + deltas.len()])
    }

    fn encode_raw(&mut self) -> Result<&[u8]> {
        let sz = 1 + self.ts.len() * 8;
        if self.bytes.len() < sz {
            self.bytes.resize(sz, 0);
        }

        let b = &mut self.bytes[..sz];
        b[0] = TIME_UNCOMPRESSED << 4;
        for (i, v) in self.ts.iter().enumerate() {
            (&mut b[1 + i * 8..1 + i * 8 + 8]).put_u64(*v)
        }

        Ok(&self.bytes[..sz])
    }

    fn encode_rle(&mut self, first: u64, delta: u64, div: u64, n: usize) -> Result<&[u8]> {
        // Large varints can take up to 10 bytes, we're encoding 3 + 1 byte type
        let sz = 31;
        if self.bytes.len() < sz {
            self.bytes.resize(sz, 0);
        }

        let b = &mut self.bytes[..sz];
        // 4 high bits used for the encoding type
        b[0] = TIME_COMPRESSED_RLE << 4;
        // 4 low bits are the log10 divisor
        b[0] |= (div as f64).log10() as u8;

        let mut i = 1;
        // The first timestamp
        (&mut b[i..i + 8]).put_u64(first);
        i += 8;
        // The first delta
        i += (delta / div).encode_var(&mut b[i..]);
        // The number of times the delta is repeated
        i += (n as u64).encode_var(&mut b[i..]);

        Ok(&b[..i])
    }
}

/// TimeDecoder decodes a byte slice into timestamps.
pub struct TimeDecoder {
    v: i64,
    i: i64,
    n: i64,
    ts: Vec<u64>,
    dec: simple8b::Decoder,
    err: Result<()>,

    // The delta value for a run-length encoded byte slice
    rle_delta: i64,

    encoding: u8,
}

impl TimeDecoder {
    pub fn new() -> Self {
        Self {
            v: 0,
            i: 0,
            n: 0,
            ts: vec![],
            dec: simple8b::Decoder::new(vec![]),
            err: Ok(()),

            rle_delta: 0,
            encoding: 0,
        }
    }

    /// Initializes the decoder with bytes to read from.
    pub fn init(&mut self, b: &[u8]) {
        self.v = 0;
        self.i = 0;
        self.ts.clear();
        self.err = Ok(());
        if !b.is_empty() {
            // Encoding type is stored in the 4 high bits of the first byte
            self.encoding = b[0] >> 4;
        }
        self.decode(b);
    }

    pub fn err(&self) -> Result<()> {
        self.err.clone()
    }

    fn decode(&mut self, b: &[u8]) {
        if b.is_empty() {
            return;
        }

        match self.encoding {
            TIME_UNCOMPRESSED => self.decode_raw(&b[1..]),
            TIME_COMPRESSED_RLE => self.decode_rle(b),
            TIME_COMPRESSED_PACKED_SIMPLE => self.decode_packed(b),
            _ => {
                self.err = Err(Error::StringErr {
                    reason: format!("unknown encoding: {}", self.encoding),
                })
            }
        }
    }

    fn decode_packed(&mut self, b: &[u8]) {
        if b.len() < 9 {
            self.err = Err(Error::StringErr {
                reason: "TimeDecoder: not enough data to decode packed timestamps".to_string(),
            });
            return;
        }

        let div = 10u64.pow((b[0] as i64 & 0xF) as u32);
        let first = (&b[1..9]).get_u64();

        self.dec.set_bytes(b[9..].to_vec());

        self.i = 0;
        let deltas = &mut self.ts;
        deltas.push(first);

        for v in self.dec.by_ref() {
            deltas.push(v);
        }

        // Compute the prefix sum and scale the deltas back up
        let mut last = deltas[0];
        if div > 1 {
            for v in deltas.iter_mut().skip(1) {
                let dgap = *v * div;
                *v = last.wrapping_add(dgap);
                last = *v;
            }
        } else {
            for v in deltas.iter_mut().skip(1) {
                // Timestamp prefix sums use InfluxDB's uint64 modulo arithmetic.
                *v = v.wrapping_add(last);
                last = *v;
            }
        }

        self.i = 0;
        let deltas_len = deltas.len();
        self.ts.truncate(deltas_len);
    }

    fn decode_rle(&mut self, b: &[u8]) {
        if b.len() < 9 {
            self.err = Err(Error::StringErr {
                reason: "TimeDecoder: not enough data for initial RLE timestamp".to_string(),
            });
            return;
        }

        let mut i = 0;
        // Lower 4 bits hold the 10 based exponent so we can scale the values back up
        let vmod = 10u64.pow((b[i] as i64 & 0xF) as u32);
        i += 1;
        // Next 8 bytes is the starting timestamp
        let first = (&b[i..i + 8]).get_u64();
        i += 8;

        // Next 1-10 bytes is our (scaled down by factor of 10) run length values
        let (mut value, n) = match u64::decode_var(&b[i..]) {
            Some((sz, n)) => (sz, n),
            None => {
                self.err = Err(Error::StringErr {
                    reason: "TimeDecoder: invalid run length in decodeRLE".to_string(),
                });
                return;
            }
        };

        // Scale the value back up
        value *= vmod;
        i += n;

        // Last 1-10 bytes is how many times the value repeats
        let (count, n) = match u64::decode_var(&b[i..]) {
            Some((sz, n)) => (sz, n),
            None => {
                self.err = Err(Error::StringErr {
                    reason: "TimeDecoder: invalid repeat value in decodeRLE".to_string(),
                });
                return;
            }
        };

        // InfluxDB subtracts as uint64 before reinterpreting the result as int64.
        self.v = first.wrapping_sub(value) as i64;
        self.rle_delta = value as i64;

        self.i = -1;
        self.n = count as i64;
    }

    fn decode_raw(&mut self, b: &[u8]) {
        self.i = 0;
        self.ts = vec![0u64; b.len() / 8];
        let ts_len = self.ts.len();
        for i in 0..ts_len {
            self.ts[i] = (&b[i * 8..i * 8 + 8]).get_u64();
            let delta = self.ts[i];
            // Compute the prefix sum and scale the deltas back up
            if i > 0 {
                self.ts[i] = self.ts[i - 1].wrapping_add(delta);
            }
        }
    }
}

impl Default for TimeDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Iterator for TimeDecoder {
    type Item = i64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.err.is_err() {
            return None;
        }

        if self.encoding == TIME_COMPRESSED_RLE {
            if self.i >= self.n {
                return None;
            }
            self.i += 1;
            // RLE timestamp reconstruction follows Go int64 wraparound semantics.
            self.v = self.v.wrapping_add(self.rle_delta);
            if self.i >= self.n {
                return None;
            }
        } else {
            if self.i >= self.ts.len() as i64 {
                return None;
            }

            self.v = self.ts[self.i as usize] as i64;
            self.i += 1;
        }

        Some(self.v)
    }
}

pub fn count_timestamps(b: &[u8]) -> i32 {
    if b.is_empty() {
        return 0;
    }

    // Encoding type is stored in the 4 high bits of the first byte
    let encoding = b[0] >> 4;
    match encoding {
        TIME_UNCOMPRESSED => (b[1..].len() / 8) as i32, // Uncompressed timestamps are just 8 bytes each
        TIME_COMPRESSED_RLE => {
            // First 9 bytes are the starting timestamp and scaling factor, skip over them
            let mut i = 9;
            // Next 1-10 bytes is our (scaled down by factor of 10) run length values
            let (_, n) = match u64::decode_var(&b[9..]) {
                Some((sz, n)) => (sz, n),
                None => (0, 0),
            };
            i += n;
            // Last 1-10 bytes is how many times the value repeats
            let (count, _) = match u64::decode_var(&b[i..]) {
                Some((sz, n)) => (sz, n),
                None => (0, 0),
            };
            count as i32
        }
        TIME_COMPRESSED_PACKED_SIMPLE => {
            // First 9 bytes are the starting timestamp and scaling factor, skip over them
            let count = simple8b::count_bytes(&b[9..]).unwrap();
            count + 1 // +1 is for the first uncompressed timestamp, starting timestamp in b[1:9]
        }
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Add;

    use crate::common::time::Time;
    use rand::Rng;

    use super::{TIME_COMPRESSED_RLE, TimeEncoder};
    use crate::storage::tsm1::data_encoding::timestamp::{
        TIME_COMPRESSED_PACKED_SIMPLE, TIME_UNCOMPRESSED, TimeDecoder, count_timestamps,
    };

    #[test]
    fn test_time_encoder() {
        let mut enc = TimeEncoder::new(1);

        let mut x = vec![];
        let now = Time::from_timestamp(0, 0);
        x.push(now.timestamp_nanos());
        enc.write(now.timestamp_nanos());
        for i in 1..4 {
            x.push(now.add(std::time::Duration::from_secs(i)).timestamp_nanos());
            enc.write(x[i as usize]);
        }

        let b = enc.bytes().unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            got, TIME_COMPRESSED_RLE,
            "Wrong encoding used: expected rle, got: {}",
            got
        );

        let mut dec = TimeDecoder::new();
        dec.init(&b);
        for (i, v) in x.iter().enumerate() {
            let got = dec.next().unwrap();
            assert_eq!(*v, got, "Item {} mismatch, got {}, exp {}", i, got, *v);
        }
    }

    #[test]
    fn test_time_encoder_no_values() {
        let mut enc = TimeEncoder::new(0);
        let b = enc.bytes().unwrap();

        let mut dec = TimeDecoder::new();
        dec.init(&b);
        assert!(
            dec.next().is_none(),
            "unexpected next value: got Some, exp None"
        );
    }

    #[test]
    fn test_time_encoder_one() {
        let mut enc = TimeEncoder::new(1);
        let tm = 0i64;

        enc.write(tm);

        let b = enc.bytes().unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            got, TIME_COMPRESSED_PACKED_SIMPLE,
            "Wrong encoding used: expected simple compressed, got: {}",
            got
        );

        let mut dec = TimeDecoder::new();
        dec.init(&b);
        let got = dec.next().unwrap();
        assert_eq!(tm, got, "read value mismatch, exp {}, got {}", tm, got);
    }

    #[test]
    fn test_time_encoder_two() {
        let mut enc = TimeEncoder::new(2);
        let t1 = 0i64;
        let t2 = 1i64;

        enc.write(t1);
        enc.write(t2);

        let b = enc.bytes().unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            got, TIME_COMPRESSED_RLE,
            "Wrong encoding used: expected rle compressed, got: {}",
            got
        );

        let mut dec = TimeDecoder::new();
        dec.init(&b);
        let got = dec.next().unwrap();
        assert_eq!(t1, got, "read value mismatch, exp {}, got {}", t1, got);
        let got = dec.next().unwrap();
        assert_eq!(t2, got, "read value mismatch, exp {}, got {}", t2, got);
    }

    #[test]
    fn test_time_encoder_three() {
        let mut enc = TimeEncoder::new(3);
        let t1 = 0i64;
        let t2 = 1i64;
        let t3 = 3i64;

        enc.write(t1);
        enc.write(t2);
        enc.write(t3);

        let b = enc.bytes().unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            got, TIME_COMPRESSED_PACKED_SIMPLE,
            "Wrong encoding used: expected simple compressed, got: {}",
            got
        );

        let mut dec = TimeDecoder::new();
        dec.init(&b);
        let got = dec.next().unwrap();
        assert_eq!(t1, got, "read value mismatch, exp {}, got {}", t1, got);
        let got = dec.next().unwrap();
        assert_eq!(t2, got, "read value mismatch, exp {}, got {}", t2, got);
        let got = dec.next().unwrap();
        assert_eq!(t3, got, "read value mismatch, exp {}, got {}", t3, got);
    }

    #[test]
    fn test_time_encoder_large_range() {
        let mut enc = TimeEncoder::new(2);
        let t1 = 1442369134000000000i64;
        let t2 = 1442369135000000000i64;

        enc.write(t1);
        enc.write(t2);

        let b = enc.bytes().unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            got, TIME_COMPRESSED_RLE,
            "Wrong encoding used: expected rle compressed, got: {}",
            got
        );

        let mut dec = TimeDecoder::new();
        dec.init(&b);
        let got = dec.next().unwrap();
        assert_eq!(t1, got, "read value mismatch, exp {}, got {}", t1, got);
        let got = dec.next().unwrap();
        assert_eq!(t2, got, "read value mismatch, exp {}, got {}", t2, got);
    }

    #[test]
    fn test_time_encoder_uncompressed() {
        let mut enc = TimeEncoder::new(3);
        let t1 = Time::from_timestamp(0, 0).timestamp_nanos();
        let t2 = Time::from_timestamp(1, 0).timestamp_nanos();
        // about 36.5yrs in NS resolution is max range for compressed format
        // This should cause the encoding to fallback to raw points
        let t3 = Time::from_timestamp(2, 0).timestamp_nanos() + (2i64 << 59);

        enc.write(t1);
        enc.write(t2);
        enc.write(t3);

        let b = enc.bytes().unwrap();
        assert_eq!(25, b.len(), "length mismatch: exp {}, got {}", 25, b.len());
        let got = b[0] >> 4;
        assert_eq!(
            got, TIME_UNCOMPRESSED,
            "Wrong encoding used: expected uncompressed, got: {}",
            got
        );

        let mut dec = TimeDecoder::new();
        dec.init(&b);
        let got = dec.next().unwrap();
        assert_eq!(t1, got, "read value mismatch, exp {}, got {}", t1, got);
        let got = dec.next().unwrap();
        assert_eq!(t2, got, "read value mismatch, exp {}, got {}", t2, got);
        let got = dec.next().unwrap();
        assert_eq!(t3, got, "read value mismatch, exp {}, got {}", t3, got);
    }

    #[test]
    fn test_time_encoder_rle() {
        let mut enc = TimeEncoder::new(512);
        let mut ts = vec![];
        for i in 0..500i64 {
            ts.push(i);
            enc.write(i);
        }

        let b = enc.bytes().unwrap();
        assert_eq!(12, b.len(), "length mismatch: exp {}, got {}", 12, b.len());
        let got = b[0] >> 4;
        assert_eq!(
            got, TIME_COMPRESSED_RLE,
            "Wrong encoding used: expected rle compressed, got: {}",
            got
        );

        let mut dec = TimeDecoder::new();
        dec.init(&b);

        for v in ts {
            let got = dec.next().unwrap();
            assert_eq!(v, got, "read value mismatch, exp {}, got {}", v, got);
        }
        assert!(dec.next().is_none(), "unexpected extra values");
    }

    #[test]
    fn test_time_encoder_reverse() {
        let mut enc = TimeEncoder::new(3);
        let ts = vec![3i64, 2i64, 0i64];
        for v in &ts {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            got, TIME_UNCOMPRESSED,
            "Wrong encoding used: expected uncompressed, got: {}",
            got
        );

        let mut dec = TimeDecoder::new();
        dec.init(&b);

        for v in ts {
            let got = dec.next().unwrap();
            assert_eq!(v, got, "read value mismatch, exp {}, got {}", v, got);
        }
        assert!(dec.next().is_none(), "unexpected extra values");
    }

    #[test]
    fn test_time_encoder_220_second_delta() {
        let mut enc = TimeEncoder::new(256);
        let mut ts = vec![];
        let now = Time::now();
        for i in 0..220 {
            ts.push(
                now.add(std::time::Duration::from_secs(i * 60))
                    .timestamp_nanos(),
            );
        }
        for v in &ts {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        // Using RLE, should get 12 bytes
        assert_eq!(12, b.len(), "length mismatch: exp {}, got {}", 12, b.len());

        let got = b[0] >> 4;
        assert_eq!(
            got, TIME_COMPRESSED_RLE,
            "Wrong encoding used: expected rle compressed, got: {}",
            got
        );

        let mut dec = TimeDecoder::new();
        dec.init(&b);

        let mut i = 0;
        for v in dec.by_ref() {
            assert_eq!(ts[i], v, "read value mismatch, exp {}, got {}", ts[i], v);
            i += 1;
        }

        assert_eq!(
            ts.len(),
            i,
            "Read too few values: exp {}, got {}",
            ts.len(),
            i
        );
    }

    #[test]
    fn test_time_encoder_rand() {
        // Write values to encoder.
        let mut ts = vec![0i64; 50];
        for v in ts.iter_mut() {
            *v = rand::thread_rng().gen_range(i64::MIN..=i64::MAX);
        }

        let mut enc = TimeEncoder::new(1024);
        for v in &ts {
            enc.write(*v);
        }

        // Retrieve encoded bytes from encoder.
        let b = enc.bytes().unwrap();

        // Read values out of decoder.
        let mut got = Vec::with_capacity(ts.len());
        let mut dec = TimeDecoder::new();
        dec.init(&b);

        while let Some(v) = dec.next() {
            if let Err(e) = dec.err() {
                panic!("{}", e);
            }

            got.push(v);
        }

        // Verify that input and output values match.
        assert_eq!(ts, got, "mismatch:\n\nexp={:?}\n\ngot={:?}\n\n", ts, got);
    }

    #[test]
    fn test_time_encoder_rle_seconds() {
        let mut enc = TimeEncoder::new(6);
        let ts = vec![
            1444448158000000000,
            1444448168000000000,
            1444448178000000000,
            1444448188000000000,
            1444448198000000000,
            1444448208000000000,
        ];

        for v in &ts {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            got, TIME_COMPRESSED_RLE,
            "Wrong encoding used: expected rle compressed, got: {}",
            got
        );

        let mut dec = TimeDecoder::new();
        dec.init(&b);

        for v in ts {
            let got = dec.next().unwrap();
            assert_eq!(v, got, "read value mismatch, exp {}, got {}", v, got);
        }
        assert!(dec.next().is_none(), "unexpected extra values");
    }

    #[test]
    fn test_time_encoder_clunt_uncompressed() {
        let mut enc = TimeEncoder::new(2);
        let t1 = Time::from_timestamp(0, 0).timestamp_nanos();
        let t2 = Time::from_timestamp(1, 0).timestamp_nanos();
        // about 36.5yrs in NS resolution is max range for compressed format
        // This should cause the encoding to fallback to raw points
        let t3 = Time::from_timestamp(2, 0).timestamp_nanos() + (2i64 << 59);

        enc.write(t1);
        enc.write(t2);
        enc.write(t3);

        let b = enc.bytes().unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            got, TIME_UNCOMPRESSED,
            "Wrong encoding used: expected uncompressed, got: {}",
            got
        );

        let got = count_timestamps(&b);
        assert_eq!(3, got, "count mismatch: got {}, exp {}", 3, got);
    }

    #[test]
    fn test_time_encoder_count_rle() {
        let mut enc = TimeEncoder::new(5);
        let ts = vec![
            1444448158000000000,
            1444448168000000000,
            1444448178000000000,
            1444448188000000000,
            1444448198000000000,
            1444448208000000000,
        ];

        for v in &ts {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            got, TIME_COMPRESSED_RLE,
            "Wrong encoding used: expected rle compressed, got: {}",
            got
        );

        let got = count_timestamps(&b);
        assert_eq!(
            ts.len() as i32,
            got,
            "count mismatch: got {}, exp {}",
            ts.len() as i32,
            got
        );
    }

    #[test]
    fn test_time_encoder_count_simple() {
        let mut enc = TimeEncoder::new(3);
        let (t1, t2, t3) = (0i64, 1i64, 3i64);

        enc.write(t1);
        enc.write(t2);
        enc.write(t3);

        let b = enc.bytes().unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            got, TIME_COMPRESSED_PACKED_SIMPLE,
            "Wrong encoding used: expected simple compressed, got: {}",
            got
        );

        let got = count_timestamps(&b);
        assert_eq!(3, got, "count mismatch: got {}, exp {}", 3, got);
    }

    #[test]
    fn test_time_encoder_corrupt() {
        let cases: &[&[u8]] = &[
            b"",                 // Empty
            b"\x10\x14",         // Packed: not enough data
            b"\x20\x00",         // RLE: not enough data for starting timestamp
            b"\x2012345678\x90", // RLE: initial timestamp but invalid uvarint encoding
            b"\x2012345678\x7f", // RLE: timestamp, RLE but invalid repeat
            b"\x00123",          // Raw: data length not multiple of 8
        ];

        for c in cases {
            let mut dec = TimeDecoder::new();
            dec.init(*c);
            assert!(dec.next().is_none(), "exp next == None, got Some");
        }
    }

    #[test]
    fn bench_time_encoder() {
        let mut enc = TimeEncoder::new(1024);
        let mut x = vec![0i64; 1024];
        for v in x.iter_mut() {
            // Small amount of randomness prevents RLE from being used
            *v = Time::now().timestamp_nanos();
            enc.write(*v);
        }

        for i in 0..2 {
            enc.bytes();
            enc.reset();
            for v in &x {
                enc.write(*v)
            }
        }
    }
}
