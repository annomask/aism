use snafu::Snafu;

/// Error type for BitReader.
#[derive(Debug, Snafu, Clone)]
pub enum Error {
    /// IO error.
    #[snafu(display("bitreader error: {}", reason))]
    IOError { reason: std::io::ErrorKind },

    /// String error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },
}

/// Result type for BitStream.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<std::io::ErrorKind> for Error {
    fn from(err: std::io::ErrorKind) -> Error {
        Error::IOError { reason: err }
    }
}

/// BitReader reads bits from an io.Reader.
#[derive(Default)]
pub struct BitReader {
    data: Vec<u8>,
    buf: Buf,
}

#[derive(Default)]
struct Buf {
    v: u64, // bit buffer
    n: u64, // available bits
}

impl BitReader {
    /// Returns a new instance of BitReader that reads from data.
    pub fn new(data: Vec<u8>) -> Self {
        let mut b = BitReader::default();
        b.reset(data);
        b
    }

    /// Resets the underlying reader on b and reinitializes.
    pub fn reset(&mut self, data: Vec<u8>) {
        self.data = data;
        self.buf.v = 0;
        self.buf.n = 0;
        self.read_buf();
    }

    // CanReadBitFast returns true if calling ReadBitFast() is allowed.
    // Fast bit reads are allowed when at least 2 values are in the buffer.
    // This is because it is not required to refilled the buffer and the caller
    // can inline the calls.
    pub fn can_read_bit_fast(&self) -> bool {
        self.buf.n > 1
    }

    /// read_bit_fast is an optimized bit read.
    /// IMPORTANT: Only allowed if can_read_bit_fast() is true!
    pub fn read_bit_fast(&mut self) -> bool {
        let v = self.buf.v & (1 << 63) != 0;
        self.buf.v <<= 1;
        self.buf.n -= 1;
        v
    }

    /// Returns the next bit from the underlying data.
    pub fn read_bit(&mut self) -> Result<bool> {
        let v = self.read_bits(1)?;
        Ok(v != 0)
    }

    /// Reads nbits from the underlying data into a uint64.
    // nbits must be from 1 to 64, inclusive.
    pub fn read_bits(&mut self, nbits: u64) -> Result<u64> {
        // Return EOF if there is no more data.
        if self.buf.n == 0 {
            return Err(Error::IOError {
                reason: std::io::ErrorKind::UnexpectedEof,
            });
        }

        // Return bits from buffer if less than available bits.
        if nbits <= self.buf.n {
            // Return all bits, if requested.
            if nbits == 64 {
                let v = self.buf.v;
                self.buf.v = 0;
                self.buf.n = 0;
                self.read_buf();
                return Ok(v);
            }

            // Otherwise mask returned bits.
            let v = self.buf.v >> (64 - nbits);
            self.buf.v <<= nbits;
            self.buf.n -= nbits;

            if self.buf.n == 0 {
                self.read_buf();
            }
            return Ok(v);
        }

        // Otherwise read all available bits in current buffer.
        let (mut v, n) = (self.buf.v, self.buf.n);

        // Read new buffer.
        self.buf.v = 0;
        self.buf.n = 0;
        self.read_buf();

        // Append new buffer to previous buffer and shift to remove unnecessary bits.
        v |= self.buf.v >> n;
        v >>= 64 - nbits;

        // Remove used bits from new buffer.
        let mut buf_n = nbits - n;
        if buf_n > self.buf.n {
            buf_n = self.buf.n;
        }
        self.buf.v <<= buf_n;
        self.buf.n -= buf_n;

        if self.buf.n == 0 {
            self.read_buf();
        }

        Ok(v)
    }

    pub fn read_buf(&mut self) {
        // Determine number of bytes to read to fill buffer.
        let mut byte_n = 8 - (self.buf.n / 8);

        // Limit to the length of our data.
        let n = self.data.len() as u64;
        if byte_n > n {
            byte_n = n;
        }

        // Optimized 8-byte read.
        if byte_n == 8 {
            self.buf.v = (self.data[7] as u64)
                | (self.data[6] as u64) << 8
                | (self.data[5] as u64) << 16
                | (self.data[4] as u64) << 24
                | (self.data[3] as u64) << 32
                | (self.data[2] as u64) << 40
                | (self.data[1] as u64) << 48
                | (self.data[0] as u64) << 56;
            self.buf.n = 64;
            self.data = self.data.drain(8..).collect();
            return;
        }

        // Otherwise append bytes to buffer.
        for i in 0..byte_n {
            self.buf.n += 8;
            self.buf.v |= (self.data[i as usize] as u64) << (64 - self.buf.n)
        }

        // Move data forward.
        self.data = self.data.drain(byte_n as usize..).collect();
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufWriter, ErrorKind};

    use crate::common::to_str;
    use rand::Rng;

    use super::BitReader;
    use crate::storage::tsm1::data_encoding::bit_stream;

    #[test]
    fn test_bit_reader_bit_stream_eof() {
        let mut br: BitReader = BitReader::new("0".as_bytes().to_vec());

        let b = br.read_bits(8).unwrap();
        assert_eq!('0' as u64, b, "ReadBits(8) didn't return first byte");

        // 0 = 0b00110000
        let mut br = BitReader::new("0".as_bytes().to_vec());

        let mut bw = bit_stream::BitWriter::new(BufWriter::new(vec![]));

        for i in 0..4 {
            match br.read_bit() {
                Ok(b) => bw.write_bit(b as bit_stream::Bit).unwrap(),
                Err(super::Error::IOError { reason }) => {
                    if reason == ErrorKind::UnexpectedEof {
                        break;
                    }
                }
                Err(e) => panic!("GetBit returned error err={:?}", e),
            }
        }

        bw.flush(bit_stream::ONE).unwrap();

        bw.write_byte(0xAA).unwrap();

        let c = bw.get_ref().buffer();

        if c.len() != 2 || c[1] != 0xAA || c[0] != 0x3f {
            panic!("bad return from 4 read bytes");
        }

        match BitReader::new("".as_bytes().to_vec()).read_bit() {
            Err(super::Error::IOError { reason }) => {
                if reason != ErrorKind::UnexpectedEof {
                    panic!("ReadBit on empty string didn't return EOF");
                }
            }
            _ => panic!("ReadBit on empty string didn't return EOF"),
        }
    }

    #[test]
    fn test_bit_reader_bit_stream() {
        let mut br = BitReader::new("hello".as_bytes().to_vec());
        let mut bw = bit_stream::BitWriter::new(BufWriter::new(vec![]));

        loop {
            match br.read_bit() {
                Err(e) => {
                    match e {
                        super::Error::IOError { reason } => {
                            if reason == ErrorKind::UnexpectedEof {
                                break;
                            }
                        }
                        _ => panic!("GetBit returned error err={}", e),
                    }
                    // panic!("GetBit returned error err={}", e);
                }
                Ok(b) => bw.write_bit(b as bit_stream::Bit).unwrap(),
            }
        }

        let s = bw.get_ref().buffer();
        assert_eq!(
            s,
            "hello".as_bytes(),
            "expected 'hello', got={}",
            to_str!(s)
        );
    }

    #[test]
    fn test_bit_reader_byte_stream() {
        let mut br = BitReader::new("hello".as_bytes().to_vec());
        let mut bw = bit_stream::BitWriter::new(BufWriter::new(vec![]));

        for _ in 0..3 {
            match br.read_bit() {
                Err(e) => match e {
                    super::Error::IOError { reason } => {
                        if reason == ErrorKind::UnexpectedEof {
                            break;
                        }
                    }
                    _ => panic!("GetBit returned error err={}", e),
                },
                Ok(b) => bw.write_bit(b as bit_stream::Bit).unwrap(),
            }
        }

        for _ in 0..3 {
            match br.read_bits(8) {
                Err(e) => match e {
                    super::Error::IOError { reason } => {
                        if reason == ErrorKind::UnexpectedEof {
                            break;
                        }
                    }
                    _ => panic!("ReadBits(8) returned error err={}", e),
                },
                Ok(b) => bw.write_byte(b as u8).unwrap(),
            }
        }

        let u = br.read_bits(13).unwrap();

        bw.write_bits(u, 13).unwrap();
        bw.write_bits((('!' as u64) << 12) | (('.' as u64) << 4) | 0x02, 20)
            .unwrap();
        // 0x2f == '/'
        bw.flush(bit_stream::ONE).unwrap();

        let s = bw.get_ref().buffer();
        assert_eq!(
            "hello!./".as_bytes(),
            s,
            "expected 'hello!./', got={}",
            to_str!(s)
        );
    }

    // Ensure bit reader can read random bits written to a stream.
    #[test]
    fn test_bit_reader_rand() {
        let mut values = vec![0u64; 50];
        let mut nbits = vec![0u64; 50];

        // Limit nbits to 64.
        for i in 0..50 {
            nbits[i] = (rand::thread_rng().gen_range(u64::MIN..=u64::MAX) % 64 + 1);
            values[i] =
                (rand::thread_rng().gen_range(u64::MIN..=u64::MAX)) & (u64::MAX >> (64 - nbits[i]));
        }

        // Write bits to a buffer.
        let mut bw = bit_stream::BitWriter::new(BufWriter::new(vec![]));
        for i in 0..50 {
            bw.write_bits(values[i], nbits[i] as i32).unwrap();
        }
        bw.flush(bit_stream::ZERO).unwrap();

        let buf = bw.into_inner().into_inner().unwrap();
        // Read bits from the buffer.
        let mut br = BitReader::new(buf);
        for i in 0..50 {
            let v = br.read_bits(nbits[i]).unwrap();
            assert_eq!(
                values[i], v,
                "value mismatch({}): exp={}, got={} (nbits={})",
                i, v, values[i], nbits[i]
            );
        }
    }
}
