//! bool encoding uses 1 bit per value.  Each compressed byte slice contains a 1 byte header
//! indicating the compression type, followed by a variable byte encoded length indicating
//! how many booleans are packed in the slice.  The remaining bytes contains 1 byte for every
//! 8 bool values encoded.
use integer_encoding::VarInt;
use snafu::{ResultExt, Snafu};

// Note: an uncompressed bool format is not yet implemented.
// booleanCompressedBitPacked is a bit packed format using 1 bit per bool
pub const BOOL_COMPRESSED_BIT_PACKET: u8 = 1;

/// Error type for bool encoder and decoder.
#[derive(Debug, Snafu, Clone)]
pub enum Error {
    /// String error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },
}

/// Result type for TSI.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// BooleanEncoder encodes a series of booleans to an in-memory buffer.
pub struct BoolEncoder {
    // The encoded bytes
    bytes: Vec<u8>,

    // The current byte being encoded
    b: u8,

    // The number of bools packed into b
    i: i32,

    // The total number of bools written
    n: i32,
}

impl BoolEncoder {
    /// Returns a new instance of BooleanEncoder.
    pub fn new(sz: i64) -> Self {
        Self {
            bytes: Vec::<u8>::with_capacity(((sz + 7) / 8) as usize),
            b: 0,
            i: 0,
            n: 0,
        }
    }

    /// Sets the encoder to its initial state.
    pub fn reset(&mut self) {
        self.bytes.clear();
        self.b = 0;
        self.i = 0;
        self.n = 0;
    }

    /// Encodes b to the underlying buffer.
    pub fn write(&mut self, b: bool) {
        // If we have filled the current byte, flush it
        if self.i >= 8 {
            self.flush2()
        }

        // Use 1 bit for each boolean value, shift the current byte
        // by 1 and set the least significant bit accordingly
        self.b <<= 1;
        if b {
            self.b |= 1;
        }

        // Increment the current boolean count
        self.i += 1;

        // Increment the total boolean count
        self.n += 1;
    }

    fn flush2(&mut self) {
        // Pad remaining byte w/ 0s
        while self.i < 8 {
            self.b <<= 1;
            self.i += 1;
        }

        // If we have bits set, append them to the byte slice
        if self.i > 0 {
            self.bytes.push(self.b);
            self.b = 0;
            self.i = 0;
        }
    }

    /// Flush is no-op
    pub fn flush(&self) {}

    /// Returns a new byte slice containing the encoded booleans from previous calls to Write.
    pub fn bytes(&mut self) -> Result<Vec<u8>> {
        // Ensure the current byte is flushed
        self.flush2();
        let mut b = vec![0u8; 10 + 1];

        // Store the encoding type in the 4 high bits of the first byte
        b[0] = BOOL_COMPRESSED_BIT_PACKET << 4;

        let mut i = 1;
        // Encode the number of booleans written
        i += (self.n as u64).encode_var(&mut b[i..]);

        // Append the packed booleans
        let bytes_len = self.bytes.len();
        if b.capacity() - i < bytes_len {
            b.resize(i + bytes_len, 0u8);
        }
        b[i..i + bytes_len].copy_from_slice(&self.bytes);
        b.truncate(i + bytes_len);
        Ok(b)
    }
}

/// Decodes a series of booleans from an in-memory buffer.
pub struct BoolDecoder {
    b: Vec<u8>,
    i: i32,
    n: i32,
    err: Result<()>,
}

impl BoolDecoder {
    pub fn new() -> Self {
        Self {
            b: vec![],
            i: 0,
            n: 0,
            err: Ok(()),
        }
    }

    /// Initializes the decoder with bytes to read from.
    /// This must be called before calling any other method.
    pub fn set_bytes(&mut self, b: &[u8]) -> Result<()> {
        if b.is_empty() {
            return Ok(());
        }

        // First byte stores the encoding type, only have 1 bit-packet format
        // currently ignore for now.
        let b = &b[1..];
        let (count, n) = match u64::decode_var(b) {
            Some((count, n)) => {
                if n == 0 {
                    return Err(Error::StringErr {
                        reason: "BoolDecoder: invalid count".to_string(),
                    });
                }
                (count, n)
            }
            _ => {
                return Err(Error::StringErr {
                    reason: "BoolDecoder: invalid count".to_string(),
                });
            }
        };

        self.b = b[n..].to_vec();
        self.i -= 1;
        self.n = count as i32;

        let min = (self.b.len() * 8) as i32;
        if min < self.n {
            // Shouldn't happen - TSM file was truncated/corrupted
            self.n = min;
        }
        self.err = Ok(());

        Ok(())
    }

    /// Returns the current decoding error.
    pub fn err(&self) -> Result<()> {
        self.err.clone()
    }
}

impl Default for BoolDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Iterator for BoolDecoder {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.err.is_err() {
            return None;
        }

        // check if there are any values remaining to be decoded.
        self.i += 1;
        if self.i >= self.n {
            return None;
        }

        // Index into the byte slice
        let idx = self.i >> 3; // integer division by 8

        // Bit position
        let pos = 7 - (self.i & 0x7);

        // The mask to select the bit
        let mask = (1 << (pos as u32)) as u8;

        // The packed byte
        let v = self.b[idx as usize];

        // Returns true if the bit is set
        Some(v & mask == mask)
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::{BoolDecoder, BoolEncoder};

    #[test]
    fn test_bool_encoder_no_values() {
        let mut enc = BoolEncoder::new(0);
        let b = enc.bytes().unwrap();

        let mut dec = BoolDecoder::new();
        dec.set_bytes(&b).unwrap();
        assert!(
            dec.next().is_none(),
            "unexpected next value: got Some, exp None"
        );
    }

    #[test]
    fn test_bool_encoder_single() {
        let mut enc = BoolEncoder::new(1);
        let v1 = true;
        enc.write(v1);
        let b = enc.bytes().unwrap();

        let mut dec = BoolDecoder::new();
        dec.set_bytes(&b).unwrap();
        let (exp, got) = (v1, dec.next().unwrap());
        assert!(
            exp == got,
            "unexpected next value: exp {}, got {}",
            exp,
            got
        );
    }

    #[test]
    fn test_bool_encoder_multi_compressed() {
        let mut enc = BoolEncoder::new(10);

        let mut values = vec![];
        for i in 0..10 {
            values.push(i % 2 == 0);
            enc.write(values[i]);
        }

        let b = enc.bytes().unwrap();
        assert_eq!(b.len(), 4, "unexpected length: got {}, exp {}", b.len(), 4);

        let mut dec = BoolDecoder::new();
        dec.set_bytes(&b).unwrap();
        for (i, v) in values.iter().enumerate() {
            let got = dec.next().unwrap();
            assert_eq!(
                got, *v,
                "unexpected value at pos {}: got {}, exp {}",
                i, got, *v
            );
        }

        assert!(
            dec.next().is_none(),
            "unexpected next value: got Some, exp None"
        );
    }

    #[test]
    fn test_bool_encoder_rand() {
        let n = rand::thread_rng().gen_range(0..1000);
        let mut values = vec![];
        for i in 0..n {
            values.push(rand::thread_rng().gen_range(0..1000) % 2 == 0)
        }

        // Write values to encoder.
        let mut enc = BoolEncoder::new(1024);
        for v in &values {
            enc.write(*v);
        }

        // Retrieve compressed bytes.
        let b = enc.bytes().unwrap();

        // Read values out of decoder.
        let mut got = Vec::with_capacity(values.len());
        let mut dec = BoolDecoder::new();
        dec.set_bytes(&b).unwrap();
        while let Some(v) = dec.next() {
            got.push(v);
        }

        // Verify that input and output values match.
        assert_eq!(
            values, got,
            "mismatch, expected:\n{:?}\n, got:{:?}\n",
            values, got
        );
    }

    // TODO: implement me
    // #[test]
    // fn test_bool_decoder_corrupt() {
    //     let cases = vec![
    //         "",
    //         "\x10\x90", // Packed: invalid count
    // 	    "\x10\x7f", // Packed: count greater than remaining bits, multiple bytes expected
    // 	    "\x10\x01", // Packed: count greater than remaining bits, one byte expected
    //     ];

    //     for c in cases {
    //         let mut dec = BoolDecoder::new();
    //         dec.set_bytes(c.as_bytes()).unwrap();
    //         assert!(dec.next().is_none(), "exp next == None, got Some for case {}", c);
    //     }
    // }
}
