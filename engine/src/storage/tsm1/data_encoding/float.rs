//! This code implements the float compression as presented in: http://www.vldb.org/pvldb/vol8/p1816-teller.pdf.
//! This implementation uses a sentinel value of NaN which means that float64 NaN cannot be stored using
//! this version.
use std::io::{BufWriter, Write};

use snafu::{ResultExt, Snafu};

use super::{
    bit_reader,
    bit_reader::BitReader,
    bit_stream::{self, BitWriter},
};

// Note: an uncompressed format is not yet implemented.
// floatCompressedGorilla is a compressed format using the gorilla paper encoding
pub const FLOAT_COMPRESSED_GORILLA: u8 = 1;

// uvnan is the constant returned from math.NaN().
pub const UVNAN: u64 = 0x7FF8000000000001;

pub fn nan() -> f64 {
    f64::from_bits(UVNAN)
}

/// Error type for Float.
#[derive(Debug, Snafu, Clone)]
pub enum Error {
    /// IO error.
    #[snafu(display("float encode/decode error: {}", reason))]
    IOError { reason: std::io::ErrorKind },

    /// String error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },

    /// BitStream error.
    #[snafu(display("float encode/decode error: {}", source))]
    BitReaderError { source: bit_reader::Error },

    /// NAN error.
    #[snafu(display("is nan eof"))]
    IsNAN,
}

/// Result type for Simple8b.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<std::io::ErrorKind> for Error {
    fn from(err: std::io::ErrorKind) -> Error {
        Error::IOError { reason: err }
    }
}

// FloatEncoder encodes multiple float64s into a byte slice.
pub struct FloatEncoder {
    val: f64,
    err: Result<()>,

    leading: u64,
    trailing: u64,

    // buf: std::io::BufWriter<Vec<u8>>,
    // bw: BitWriter<BufWriter<Vec<u8>>>,
    bw: BitWriter<Vec<u8>>,

    first: bool,
    finished: bool,
}

impl FloatEncoder {
    pub fn new() -> Self {
        let mut buf = Vec::with_capacity(64);
        buf.push(FLOAT_COMPRESSED_GORILLA << 4);
        Self {
            val: 0.0,
            err: Ok(()),
            leading: !0,
            trailing: 0,

            bw: BitWriter::new(buf),

            first: true,
            finished: false,
        }
    }

    /// Rets the encoder back to its initial state.
    pub fn reset(&mut self) {
        self.val = 0.0;
        self.err = Ok(());
        self.leading = !0;
        self.trailing = 0;
        self.bw.get_mut().clear();
        let _ = self.bw.get_mut().write(&[FLOAT_COMPRESSED_GORILLA << 4]);

        self.bw.resume(0x0, 8);

        self.finished = false;
        self.first = true;
    }

    /// Returns a copy of the underlying byte buffer used in the encoder.
    pub fn bytes(&self) -> Result<&[u8]> {
        if let Err(e) = &self.err {
            return Err(e.clone());
        }

        Ok(self.bw.get_ref().as_ref())
    }

    /// Indicates there are no more values to encode.
    pub fn flush(&mut self) {
        if !self.finished {
            // write an end-of-stream record
            self.finished = true;
            self.write(nan());
            let _ = self.bw.flush(bit_stream::ZERO);
        }
    }

    /// Encodes v to the underlying buffer.
    pub fn write(&mut self, v: f64) {
        // Only allow NaN as a sentinel value
        if v.is_nan() && !self.finished {
            self.err = Err(Error::StringErr {
                reason: "unsupported value: f64::NAN".to_string(),
            });
            return;
        }

        if self.first {
            // first point
            self.val = v;
            self.first = false;
            let _ = self.bw.write_bits(f64::to_bits(v), 64);
            return;
        }
        let v_delta = f64::to_bits(v) ^ f64::to_bits(self.val);

        if v_delta == 0 {
            let _ = self.bw.write_bit(bit_stream::ZERO);
        } else {
            let _ = self.bw.write_bit(bit_stream::ONE);

            let mut leading = v_delta.leading_zeros() as u64;
            let trailing = v_delta.trailing_zeros() as u64;

            // Clamp number of leading zeros to avoid overflow when encoding
            leading &= 0x1F;
            if leading >= 32 {
                leading = 31;
            }

            // TODO: check if it's 'cheaper' to reset the leading/trailing bits instead
            if self.leading != !0u64 && leading >= self.leading && trailing >= self.trailing {
                let _ = self.bw.write_bit(bit_stream::ZERO);
                let _ = self.bw.write_bits(
                    v_delta >> self.trailing,
                    64 - self.leading as i32 - self.trailing as i32,
                );
            } else {
                self.leading = leading;
                self.trailing = trailing;

                let _ = self.bw.write_bit(bit_stream::ONE);
                let _ = self.bw.write_bits(leading, 5);

                // Note that if leading == trailing == 0, then sigbits == 64.  But that
                // value doesn't actually fit into the 6 bits we have.
                // Luckily, we never need to encode 0 significant bits, since that would
                // put us in the other case (vdelta == 0).  So instead we write out a 0 and
                // adjust it back to 64 on unpacking.
                let sigbits = 64 - leading - trailing;
                let _ = self.bw.write_bits(sigbits, 6);
                let _ = self.bw.write_bits(v_delta >> trailing, sigbits as i32);
            }
        }

        self.val = v;
    }
}

impl Default for FloatEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// FloatDecoder decodes a byte slice into multiple float64 values.
pub struct FloatDecoder {
    val: u64,

    leading: u64,
    trailing: u64,

    br: BitReader,
    // b:  Vec<u8>,
    first: bool,
    finished: bool,

    err: Result<()>,
}

impl FloatDecoder {
    pub fn new() -> Self {
        FloatDecoder {
            val: 0,
            leading: 0,
            trailing: 0,
            br: BitReader::default(),
            // b: vec![],
            first: false,
            finished: false,
            err: Ok(()),
        }
    }

    /// Initializes the decoder with b. Must call before calling next().
    pub fn set_bytes(&mut self, b: &[u8]) -> Result<()> {
        let mut v = 0;
        if b.is_empty() {
            v = UVNAN;
        } else {
            // first byte is the compression type.
            // we currently just have gorilla compression.
            self.br.reset(b[1..].to_vec());

            v = self.br.read_bits(64).context(BitReaderSnafu)?;
        }

        // Reset all fields
        self.val = v;
        self.leading = 0;
        self.trailing = 0;
        self.first = true;
        self.finished = false;
        self.err = Ok(());

        Ok(())
    }

    /// Returns the current f64 value.
    pub fn values(&self) -> f64 {
        f64::from_bits(self.val)
    }

    /// Returns the current decoding error.
    pub fn err(&self) -> Result<()> {
        self.err.clone()
    }
}

impl Default for FloatDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Iterator for FloatDecoder {
    type Item = f64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.err.is_err() || self.finished {
            return None;
        }

        if self.first {
            self.first = false;

            // mark as finished if there were no values.
            if self.val == UVNAN {
                // is nan
                self.finished = true;
                return None;
            }

            return Some(f64::from_bits(self.val));
        }

        // read compressed value
        let mut bit = false;
        if self.br.can_read_bit_fast() {
            bit = self.br.read_bit_fast();
        } else {
            match self.br.read_bit() {
                Ok(v) => bit = v,
                Err(e) => {
                    self.err = Err(Error::BitReaderError { source: e });
                    return None;
                }
            }
        }

        if !bit {
            // self.val - self.val
        } else {
            let mut bit = false;
            if self.br.can_read_bit_fast() {
                bit = self.br.read_bit_fast();
            } else {
                match self.br.read_bit() {
                    Ok(v) => bit = v,
                    Err(e) => {
                        self.err = Err(Error::BitReaderError { source: e });
                        return None;
                    }
                }
            }

            if !bit {
                // reuse leading/trailing zero bits
                // self.leading, self.trailing = self.leading, self.trailing
            } else {
                match self.br.read_bits(5) {
                    Ok(bits) => self.leading = bits,
                    Err(e) => {
                        self.err = Err(Error::BitReaderError { source: e });
                        return None;
                    }
                }

                let mut mbits = match self.br.read_bits(6) {
                    Ok(bits) => bits,
                    Err(e) => {
                        self.err = Err(Error::BitReaderError { source: e });
                        return None;
                    }
                };

                // 0 significant bits here means we overflowed and we actually need 64; see comment in encoder
                if mbits == 0 {
                    mbits = 64;
                }
                self.trailing = 64 - self.leading - mbits;
            }

            let mbits = 64 - self.leading - self.trailing;
            let bits = match self.br.read_bits(mbits) {
                Ok(bits) => bits,
                Err(e) => {
                    self.err = Err(Error::BitReaderError { source: e });
                    return None;
                }
            };
            let mut vbits = self.val;
            vbits ^= bits << self.trailing;

            if vbits == UVNAN {
                self.finished = true;
                return None;
            }
            self.val = vbits;
        }

        Some(f64::from_bits(self.val))
    }
}

#[cfg(any(test, feature = "benchmarking"))]
pub mod tests {
    use rand::Rng;

    use super::{FloatDecoder, FloatEncoder, nan};

    #[test]
    fn test_float_encoder_simple() {
        // Example from the paper
        let mut enc = FloatEncoder::new();

        enc.write(12.0);
        enc.write(12.0);
        enc.write(24.0);

        // extra tests

        // floating point masking/shifting bug
        enc.write(13.0);
        enc.write(24.0);

        // delta-of-delta sizes
        enc.write(24.0);
        enc.write(24.0);
        enc.write(24.0);

        enc.flush();

        let b = enc.bytes().unwrap();

        let mut dec = FloatDecoder::new();
        dec.set_bytes(b).unwrap();

        let want = vec![12.0, 12.0, 24.0, 13.0, 24.0, 24.0, 24.0, 24.0];

        for w in want {
            let vv = dec.next().unwrap();
            assert_eq!(w, vv, "want={}, got={}", w, vv);
        }

        assert!(dec.next().is_none(), "want None, got Some");

        assert!(dec.err.is_ok(), "want Ok, got {:?}", dec.err);
    }

    #[test]
    fn test_float_encoder_similar_floats() {
        let mut enc = FloatEncoder::new();

        let want = vec![
            6.00065e+06,
            6.000656e+06,
            6.000657e+06,
            6.000659e+06,
            6.000661e+06,
        ];

        for v in &want {
            enc.write(*v);
        }

        enc.flush();

        let b = enc.bytes().unwrap();

        let mut dec = FloatDecoder::new();
        dec.set_bytes(b).unwrap();

        for w in want {
            let vv = dec.next().unwrap();
            assert_eq!(w, vv, "want={}, got={}", w, vv);
        }

        assert!(dec.next().is_none(), "want None, got Some");

        assert!(dec.err.is_ok(), "want Ok, got {:?}", dec.err);
    }

    pub const TWO_HOURS_DATA: [f64; 120] = [
        // 2h of data, rows of 10 values
        761.0, 727.0, 763.0, 706.0, 700.0, 679.0, 757.0, 708.0, 739.0, 707.0, 699.0, 740.0, 729.0,
        766.0, 730.0, 715.0, 705.0, 693.0, 765.0, 724.0, 799.0, 761.0, 737.0, 766.0, 756.0, 719.0,
        722.0, 801.0, 747.0, 731.0, 742.0, 744.0, 791.0, 750.0, 759.0, 809.0, 751.0, 705.0, 770.0,
        792.0, 727.0, 762.0, 772.0, 721.0, 748.0, 753.0, 744.0, 716.0, 776.0, 659.0, 789.0, 766.0,
        758.0, 690.0, 795.0, 770.0, 758.0, 723.0, 767.0, 765.0, 693.0, 706.0, 681.0, 727.0, 724.0,
        780.0, 678.0, 696.0, 758.0, 740.0, 735.0, 700.0, 742.0, 747.0, 752.0, 734.0, 743.0, 732.0,
        746.0, 770.0, 780.0, 710.0, 731.0, 712.0, 712.0, 741.0, 770.0, 770.0, 754.0, 718.0, 670.0,
        775.0, 749.0, 795.0, 756.0, 741.0, 787.0, 721.0, 745.0, 782.0, 765.0, 780.0, 811.0, 790.0,
        836.0, 743.0, 858.0, 739.0, 762.0, 770.0, 752.0, 763.0, 795.0, 792.0, 746.0, 786.0, 785.0,
        774.0, 786.0, 718.0,
    ];

    #[test]
    fn test_float_encoder_round_trip() {
        let mut enc = FloatEncoder::new();
        for p in TWO_HOURS_DATA.iter() {
            enc.write(*p);
        }
        enc.flush();

        let b = enc.bytes().unwrap();

        let mut dec = FloatDecoder::new();
        dec.set_bytes(b).unwrap();

        for w in TWO_HOURS_DATA.iter() {
            let vv = dec.next().unwrap();
            assert_eq!(*w, vv, "want={}, got={}", *w, vv);
        }

        assert!(dec.next().is_none(), "want None, got Some");

        assert!(dec.err.is_ok(), "want Ok, got {:?}", dec.err);
    }

    #[test]
    fn test_float_encoder_round_trip_nan() {
        let mut enc = FloatEncoder::new();
        enc.write(1.0);
        enc.write(nan());
        enc.write(2.0);
        enc.flush();

        assert!(enc.bytes().is_err(), "expected Err. got OK");
    }

    #[test]
    fn test_float_encoder_empty() {
        let mut enc = FloatEncoder::new();
        enc.flush();

        let b = enc.bytes().unwrap();

        let mut dec = FloatDecoder::new();
        dec.set_bytes(b).unwrap();

        let mut got = vec![];
        for v in dec {
            got.push(v);
        }

        assert!(got.is_empty(), "want Empty, got len={}", got.len());
    }

    #[test]
    fn test_float_encoder_rand() {
        let mut values = vec![0f64; 50];
        for i in 0..50 {
            values[i] = rand::random::<f64>();
        }

        // Write values to encoder.
        let mut enc = FloatEncoder::new();
        for v in values.iter() {
            enc.write(*v);
        }
        enc.flush();

        // Read values out of decoder.
        let b = enc.bytes().unwrap();

        let mut dec = FloatDecoder::new();
        dec.set_bytes(b).unwrap();

        let mut got = vec![];
        for v in dec {
            got.push(v);
        }

        // Verify that input and output values match.
        assert_eq!(
            values, got,
            "mismatch:

exp={:?}

got={:?}

",
            values, got
        );
    }

    #[test]
    fn test_float_decoder_empty() {
        let mut dec = FloatDecoder::new();
        dec.set_bytes(&[]).unwrap();

        assert!(dec.next().is_none(), "exp next=None, got Some");
    }
}
