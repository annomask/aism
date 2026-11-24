//! Bitstream is a simple wrapper around a io.Reader and io.Writer to provide bit-level access to the stream.
use std::io::{Read, Write};

use snafu::{ResultExt, Snafu};

// A Bit is a zero or a one
pub type Bit = bool;

// Zero is our exported type for '0' bits
pub const ZERO: Bit = false;
// One is our exported type for '1' bits
pub const ONE: Bit = true;

/// Error type for Bitstream.
#[derive(Debug, Snafu)]
pub enum Error {
    /// IO error.
    #[snafu(display("bitstream encode/decode error: {}", reason))]
    IOError { reason: std::io::Error },

    /// String error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },
}

/// Result type for BitStream.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IOError { reason: err }
    }
}

/// A BitWriter writes bits to an io.Writer
pub struct BitWriter<W: Write> {
    w: W,
    b: [u8; 1],
    count: u8,
}

impl<W: Write> BitWriter<W> {
    /// Returns a BitWriter that buffers bits and write the resulting bytes to 'w'
    pub fn new(w: W) -> Self {
        Self {
            w,
            b: [0u8; 1],
            count: 8,
        }
    }

    pub fn pending(&self) -> (u8, u8) {
        (self.b[0], self.count)
    }

    pub fn resume(&mut self, data: u8, count: u8) {
        self.b[0] = data;
        self.count = count;
    }

    /// Writes a single bit to the stream, writing a new byte to 'w' if required.
    pub fn write_bit(&mut self, bit: Bit) -> Result<()> {
        if bit {
            self.b[0] |= 1 << (self.count - 1);
        }

        self.count -= 1;

        if self.count == 0 {
            let n = self.w.write(&self.b[..])?;
            if n != 1 {
                return Err(Error::StringErr {
                    reason: format!("write bit failed, expecte write 1, got {}", n),
                });
            }
            self.b[0] = 0;
            self.count = 8;
        }

        Ok(())
    }

    /// Writes a single byte to the stream, regardless of alignment
    pub fn write_byte(&mut self, byt: u8) -> Result<()> {
        // fill up self.b with self.count bits from byt
        self.b[0] |= byt >> (8 - self.count);

        let n = self.w.write(&self.b[..])?;
        if n != 1 {
            return Err(Error::StringErr {
                reason: format!("write byte failed, expecte write 1, got {}", n),
            });
        }
        self.b[0] = if self.count == 8 {
            0
        } else {
            byt << self.count
        };

        Ok(())
    }

    /// Writes the nbits least significant bits of u, most-significant-bit first.
    pub fn write_bits(&mut self, mut u: u64, mut nbits: i32) -> Result<()> {
        u <<= 64 - nbits as u64;
        while nbits >= 8 {
            let byt = (u >> 56) as u8;
            self.write_byte(byt)?;
            u <<= 8;
            nbits -= 8;
        }

        while nbits > 0 {
            self.write_bit((u >> 63) == 1)?;
            u <<= 1;
            nbits -= 1;
        }

        Ok(())
    }

    /// Empties the currently in-process byte by filling it with 'bit'.
    pub fn flush(&mut self, bit: Bit) -> Result<()> {
        while self.count != 8 {
            self.write_bit(bit)?;
        }

        Ok(())
    }

    /// Replaces the underlying io.Writer with the provided writer and resets
    /// all internal state to its initial value allowing for reuse of the BitWriter
    /// without additional allocations
    pub fn reset(&mut self, w: W) {
        self.w = w;
        self.b[0] = 0x00;
        self.count = 8;
    }

    pub fn into_inner(self) -> W {
        self.w
    }

    pub fn get_ref(&self) -> &W {
        &self.w
    }

    pub fn get_mut(&mut self) -> &mut W {
        &mut self.w
    }
}

impl<W: Write> Write for BitWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        todo!("implement me")
    }

    fn flush(&mut self) -> std::io::Result<()> {
        todo!("implement me")
    }
}

/// A BitReader reads bits from an io.Reader
pub struct BitReader<R: Read> {
    r: R,
    b: [u8; 1],
    count: u8,
}

impl<R: Read> BitReader<R> {
    pub fn new(r: R) -> Self {
        Self {
            r,
            b: [0u8; 1],
            count: 0,
        }
    }

    /// Returns the next bit from the stream, reading a new byte from the underlying reader if required.
    /// Returns the byte read and how many bytes were read, return 0 for EOF.
    pub fn read_bit(&mut self) -> Result<(Bit, usize)> {
        if self.count == 0 {
            match self.r.read(&mut self.b[..]) {
                Ok(n) => {
                    if n != 1 && n == 0 {
                        // n=0 is EOF: if read bit failed, but err is EOF
                        return Ok((ZERO, n));
                    }
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
            self.count = 8
        }
        self.count -= 1;
        let d = self.b[0] & 0x80;
        self.b[0] <<= 1;

        Ok((d != 0, 1))
    }

    /// Reads a single byte from the stream, regardless of alignment
    /// Returns the byte read and how many bytes were read, return 0 for EOF.
    pub fn read_byte(&mut self) -> Result<(u8, usize)> {
        if self.count == 0 {
            match self.r.read(&mut self.b[..]) {
                Ok(n) => {
                    if n != 1 && n == 0 {
                        // n=0 is EOF: if read bit failed, but err is EOF
                        self.b[0] = 0;
                        return Ok((self.b[0], n)); // return EOF
                    }
                    return Ok((self.b[0], n));
                }
                Err(e) => {
                    self.b[0] = 0;
                    return Err(e.into());
                }
            }
        }

        let mut byt = self.b[0];

        let n = match self.r.read(&mut self.b[..]) {
            Ok(n) => {
                if n != 1 && n == 0 {
                    return Ok((0, n)); // return EOF
                }
                n
            }
            Err(e) => {
                self.b[0] = 0;
                return Err(e.into());
            }
        };

        byt |= self.b[0] >> self.count;

        self.b[0] <<= 8 - self.count;

        Ok((byt, n))
    }

    /// Reads  nbits from the stream
    pub fn read_bits(&mut self, mut nbits: i32) -> Result<u64> {
        let mut u = 0;

        while nbits >= 8 {
            let (byt, n) = self.read_byte()?;

            u = (u << 8) | byt as u64;
            nbits -= 8;
        }

        let mut n = 2; // read bytes
        while nbits > 0 && (n != 0) {
            let (byt, sz) = match self.read_bit() {
                // sz=0 is EOF
                Ok((b, sz)) => (b, sz),
                Err(e) => return Err(e),
            };
            n = sz;
            // (let byt) = self.read_bit()?;
            u <<= 1;
            if byt {
                u |= 1;
            }
            nbits -= 1;
        }

        Ok(u)
    }

    /// Replaces the underlying io.Reader with the provided reader and resets
    /// all interal state to its zero value, allowing for reuse of the BitReader
    /// without additional allocations
    pub fn reset(&mut self, r: R) {
        self.r = r;
        self.b[0] = 0x00;
        self.count = 0;
    }
}

impl<R: Read> Read for BitReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        todo!("implement me")
    }
}

#[cfg(test)]
mod tests {
    use std::io::{ErrorKind, Read, Write};

    use crate::common::to_str;

    use super::{BitReader, BitWriter, ONE};

    #[test]
    fn test_bit_stream_eof() {
        let data = "0".to_string();
        let mut br = BitReader::new(data.as_bytes());
        let (b, n) = br.read_byte().unwrap(); // n=0 is EOF
        assert_eq!(b'0', b, "ReadByte didn't return first byte");

        let (b, n) = br.read_byte().unwrap(); // n=0 is EOF
        assert_eq!(0, n, "ReadByte on empty string didn't return EOF");

        let data = "0".to_string();
        let mut br = BitReader::new(data.as_bytes());
        let mut buf = vec![];
        let mut bw = BitWriter::new(buf);

        for i in 0..4 {
            let (bit, n) = br.read_bit().unwrap();
            if n == 0 {
                // EOF
                break;
            }
            bw.write_bit(bit).unwrap();
        }

        bw.flush(ONE);

        bw.write_byte(0xAA).unwrap();

        let buf = bw.into_inner();
        let c = buf.as_slice();

        assert!(
            c.len() == 2 && c[1] == 0xAA && c[0] == 0x3f,
            "bad return from 4 read bytes"
        );

        let data = "".to_string();
        let (b, n) = BitReader::new(data.as_bytes()).read_bit().unwrap(); // n=0 is EOF
        assert!(n == 0, "ReadBit on empty vec didn't return EOF");
    }

    // reader that returns a single byte with EOF
    struct EofReader {
        read_called: bool,
    }

    impl Read for EofReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.read_called {
                return Ok(0); // 0 indicates EOF
            }
            self.read_called = true;
            buf[0] = b'A';
            return Ok(1);
        }
    }

    #[test]
    fn test_bit_stream_read_eof() {
        let mut br = BitReader::new(EofReader { read_called: false });

        let r = br.read_bits(8).unwrap();
        assert_eq!(
            r, b'A' as u64,
            "failed to read single byte with EOF: {:?}",
            r
        );
    }

    #[test]
    fn test_bit_stream() {
        let mut buf = vec![];
        let mut data = "hello".to_string();
        let mut br = BitReader::new(data.as_bytes());
        let mut bw = BitWriter::new(buf);

        loop {
            let (bit, n) = br.read_bit().unwrap();
            if n == 0 {
                // EOF
                break;
            }

            bw.write_bit(bit).unwrap();
        }

        let buf = bw.into_inner();
        let s = to_str!(&buf);
        assert_eq!("hello", s, "expected 'hello', got={}", s);
    }

    #[test]
    fn test_byte_stream() {
        let mut buf = vec![];
        let mut data = "hello".to_string();
        let mut br = BitReader::new(data.as_bytes());
        let mut bw = BitWriter::new(buf);

        for i in 0..3 {
            let (bit, n) = br.read_bit().unwrap();
            if n == 0 {
                // EOF
                break;
            }

            bw.write_bit(bit).unwrap();
        }

        for i in 0..3 {
            let (byt, n) = br.read_byte().unwrap();
            if n == 0 {
                // EOF
                break;
            }
            bw.write_byte(byt).unwrap();
        }

        let u = br.read_bits(13).unwrap();

        bw.write_bits(u, 13).unwrap();
        bw.write_bits(((b'!' as u64) << 12) | ((b'.' as u64) << 4) | 0x02, 20)
            .unwrap();

        // 0x2f == '/'
        bw.flush(ONE);

        let buf = bw.into_inner();
        let s = to_str!(&buf);
        assert_eq!("hello!./", s, "expected 'hello', got={}", s);
    }

    struct BadWriter;

    impl Write for BadWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            Err(ErrorKind::Other.into())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_error_propagation() {
        // check WriteBit
        let w = BadWriter;
        let mut bw = BitWriter::new(w);
        for i in 0..7 {
            bw.write_bit(ONE).unwrap();
        }
        match bw.write_bit(ONE) {
            Ok(_) => panic!("expected error"),
            Err(super::Error::IOError { reason }) => {
                assert_eq!(ErrorKind::Other, reason.kind(), "failed to propagate error");
            }
            _ => panic!("failed to propagate error"),
        }

        // check WriteBits
        let w = BadWriter;
        let mut bw = BitWriter::new(w);
        match bw.write_bits(256, 8) {
            Ok(_) => panic!("expected error"),
            Err(super::Error::IOError { reason }) => {
                assert_eq!(ErrorKind::Other, reason.kind(), "failed to propagate error");
            }
            _ => panic!("failed to propagate error"),
        }
    }

    #[test]
    fn test_reset() {
        let data = "abc".to_string();
        let mut br = BitReader::new(data.as_bytes());
        let b = br.read_bits(8).unwrap();
        assert_eq!(
            'a' as u64,
            b,
            "expected 'a', got={}",
            char::from_u32(b as u32).unwrap()
        );

        let data = "def".to_string();
        br.reset(data.as_bytes());
        let b = br.read_bits(8).unwrap();
        assert_eq!(
            'd' as u64,
            b,
            "expected 'd', got={}",
            char::from_u32(b as u32).unwrap()
        );

        let mut first_writer = vec![];
        let mut w = BitWriter::new(first_writer);
        w.write_bits(97, 8).unwrap();
        first_writer = w.into_inner();

        let mut second_writer = vec![];
        let mut w = BitWriter::new(second_writer);
        w.write_bits(98, 8).unwrap();
        second_writer = w.into_inner();

        assert_eq!(
            "a",
            to_str!(first_writer.as_slice()),
            "expected 'a', got={}",
            to_str!(first_writer.as_slice())
        );
        assert_eq!(
            "b",
            to_str!(second_writer.as_slice()),
            "expected 'b', got={}",
            to_str!(second_writer.as_slice())
        );
    }
}
