//! String encoding uses snappy compression to compress each string.  Each string is
//! appended to byte slice prefixed with a variable byte length followed by the string
//! bytes.
//! The bytes are compressed using snappy compressor and a 1 byte header is used
//! to indicate the type of encoding.
use crate::common::to_string;
use integer_encoding::VarInt;
use snafu::{ResultExt, Snafu};

// Note: an uncompressed format is not yet implemented.

// stringCompressedSnappy is a compressed encoding using Snappy compression
pub const STRING_COMPRESSED_SNAPPY: u8 = 1;

/// Error type for string encoder and decoder.
#[derive(Debug, Snafu, Clone)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    // /// IO error.
    // #[snafu(display("io error: {}", reason))]
    // IOError { reason: std::io::Error },
    /// Snap encode/decode error.
    #[snafu(display("{}", source))]
    SnapErr { source: snap::Error },

    /// String error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },
}

/// Result type for TSI.
pub type Result<T, E = Error> = std::result::Result<T, E>;

// impl From<std::io::Error> for Error {
//     fn from(err: std::io::Error) -> Error {
//         Error::IOError { reason: err }
//     }
// }

/// StringEncoder encodes multiple strings into a byte slice.
pub struct StringEncoder {
    bytes: Vec<u8>,
}

impl StringEncoder {
    /// Returns a new StringEncoder with an initial buffer ready to hold sz bytes.
    pub fn new(sz: i64) -> Self {
        Self {
            bytes: Vec::<u8>::with_capacity(sz as usize),
        }
    }

    /// Flush is no-op
    pub fn flush(&self) {}

    /// Sets the encoder back to its initial state.
    pub fn reset(&mut self) {
        self.bytes.clear();
    }

    /// Encodes s to the underlying buffer.
    pub fn write(&mut self, s: &[u8]) {
        let mut b = vec![0u8; 10];
        // Append the length of the string using variable byte encoding
        let i = (s.len() as u64).encode_var(&mut b);
        self.bytes.extend_from_slice(&b[..i]);

        // Append the string bytes
        self.bytes.extend_from_slice(s);
    }

    /// Returns a copy of the underlying buffer.
    pub fn bytes(&self) -> Result<Vec<u8>> {
        // Compress the currently appended bytes using snappy and prefix with
        // a 1 byte header for future extension
        let mut compressed = snap::raw::Encoder::new()
            .compress_vec(self.bytes.as_slice())
            .context(SnapErrSnafu)?;

        let mut data = vec![STRING_COMPRESSED_SNAPPY << 4];
        data.append(&mut compressed);

        Ok(data)
    }
}

/// StringDecoder decodes a byte slice into strings.
pub struct StringDecoder {
    b: Vec<u8>,
    l: usize,
    i: usize,
    err: Result<()>,
}

impl StringDecoder {
    pub fn new() -> Self {
        Self {
            b: vec![],
            l: 0,
            i: 0,
            err: Ok(()),
        }
    }

    /// Initializes the decoder with bytes to read from.
    /// This must be called before calling any other method.
    pub fn set_bytes(&mut self, b: &[u8]) -> Result<()> {
        // First byte stores the encoding type, only have snappy format
        // currently so ignore for now.
        let data = if !b.is_empty() {
            snap::raw::Decoder::new()
                .decompress_vec(&b[1..])
                .context(SnapErrSnafu)?
        } else {
            vec![]
        };

        self.b = data;
        self.l = 0;
        self.i = 0;
        self.err = Ok(());

        Ok(())
    }

    /// Returns the last error encountered by the decoder.
    pub fn err(&self) -> Result<()> {
        self.err.clone()
    }
}

impl Default for StringDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Iterator for StringDecoder {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        if self.err.is_err() {
            return None;
        }

        // check if there are any values remaining to be decoded.
        self.i += self.l;
        if self.i >= self.b.len() {
            return None;
        }

        // Read the length of the string
        let (length, n) = match u64::decode_var(&self.b.as_slice()[self.i..]) {
            Some((sz, n)) => (sz, n),
            None => {
                self.err = Err(Error::StringErr {
                    reason: "StringDecoder: invalid encoded string length".to_string(),
                });
                return None;
            }
        };

        // The length of this string plus the length of the variable byte encoded length
        self.l = length as usize + n;

        let lower = self.i + n;
        let upper = lower + length as usize;
        if upper < lower {
            self.err = Err(Error::StringErr {
                reason: "StringDecoder: length overflow".to_string(),
            });
            return None;
        }

        if upper > self.b.len() {
            self.err = Err(Error::StringErr {
                reason: "StringDecoder: not enough data to represent encoded string".to_string(),
            });
            return None;
        }

        Some(to_string!(self.b.as_slice()[lower..upper]))
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::{STRING_COMPRESSED_SNAPPY, StringDecoder, StringEncoder};

    #[test]
    fn test_string_encoder_no_values() {
        let enc = StringEncoder::new(1024);
        let b = enc.bytes().unwrap();

        let mut dec = StringDecoder::new();
        let _ = dec.set_bytes(&b).unwrap();
        let e = dec.next();
        assert!(e.is_none(), "unexpected next value, got Some, exp None");
    }

    #[test]
    fn test_string_encoder_single() {
        let mut enc = StringEncoder::new(1024);
        let v1 = "v1";
        enc.write(v1.as_bytes());
        let b = enc.bytes().unwrap();

        let mut dec = StringDecoder::new();
        let _ = dec.set_bytes(&b).unwrap();

        let e = dec.next();
        assert!(e.is_some(), "unexpected next value: got None, exp Some");
        let e = e.unwrap();

        assert!(v1 == e.as_str(), "unexpected value: got {}, exp {}", e, v1);
    }

    #[test]
    fn test_string_encoder_multi_compressed() {
        let mut enc = StringEncoder::new(1024);

        let mut values = Vec::<String>::with_capacity(10);
        for i in 0..10 {
            let v = format!("value {}", i);
            enc.write(v.as_bytes());
            values.push(v);
        }

        let b = enc.bytes().unwrap();
        if b[0] >> 4 != STRING_COMPRESSED_SNAPPY {
            panic!(
                "unexpected encoding: got {}, exp {}",
                b[0], STRING_COMPRESSED_SNAPPY
            );
        }

        assert_eq!(
            b.len(),
            51,
            "unexpected length: got {}, exp {}",
            b.len(),
            51
        );

        let mut dec = StringDecoder::new();
        let _ = dec.set_bytes(&b).unwrap();

        for (i, v) in values.iter().enumerate() {
            let dv = dec.next().unwrap();
            assert_eq!(
                v, &dv,
                "unexpected value at pos {}: got {:?}, exp {:?}",
                i, dv, v
            );
        }

        assert!(
            dec.next().is_none(),
            "unexpected next value: got Some, exp None"
        );
    }

    #[test]
    fn test_string_encoder_quick() {
        // generate random values
        let mut input = vec![];
        for _ in 0..50 {
            let rndstr = rand::thread_rng()
                .sample_iter::<char, _>(rand::distributions::Standard)
                .take(50)
                .collect::<String>();
            input.push(rndstr);
        }

        let mut enc = StringEncoder::new(1024);
        for v in input.iter() {
            enc.write(v.as_bytes());
        }

        // Retrieve encoded bytes from encoder.
        let b = enc.bytes().unwrap();

        // Read values out of decoder.
        let mut got = vec![];
        let mut dec = StringDecoder::new();
        dec.set_bytes(&b).unwrap();

        for v in dec {
            got.push(v);
        }

        assert_eq!(input, got, "mismatch, exp: {:?}, got: {:?}", input, got);
    }

    #[test]
    fn test_string_decoder_empty() {
        let mut dec = StringDecoder::new();
        let _ = dec.set_bytes(&[]).unwrap();
        assert!(
            dec.next().is_none(),
            "unexpected next value: got Some, exp None"
        );
    }

    #[test]
    fn test_string_decoder_corrupt_read() {
        let cases: &[&[u8]] = &[
            b"\x10\x03\x08\x03Hi", // Higher length than actual data
            b"\x10\x1dp\x9c\x90\x90\x90\x90\x90\x90\x90\x90\x90length overflow----",
        ];

        for c in cases {
            let mut dec = StringDecoder::new();
            let _ = dec.set_bytes(*c).unwrap();
            assert!(
                dec.next().is_none(),
                "unexpected next value: got Some, exp None"
            );
            assert!(dec.err().is_err(), "unexpected an err, got Ok: {:?}", *c);
        }
    }

    #[test]
    fn test_string_decoder_corrupt_set_bytes() {
        let mut case = Vec::new();
        case.extend_from_slice(b"0t\x00\x01\x000\x00\x01\x000\x00\x01\x000\x00\x01\x000\x00\x01");
        case.extend_from_slice(
            b"\x000\x00\x01\x000\x00\x01\x000\x00\x00\x00\xff:\x01\x00\x01\x00\x01",
        );
        case.extend_from_slice(b"\x00\x01\x00\x01\x00\x01\x00\x010\x010\x000\x010\x010\x010\x01");
        case.extend_from_slice(b"0\x010\x010\x010\x010\x010\x010\x010\x010\x010\x010");

        let mut dec = StringDecoder::new();
        assert!(
            dec.set_bytes(&case).is_err(),
            "exp an Error, got Ok: {:?}",
            case
        );
    }
}
