//! Package simple8b implements the 64bit integer encoding algoritm as published
//! by Ann and Moffat in "Index compression using 64-bit words", Softw. Pract. Exper. 2010; 40:131–147
//!
//! It is capable of encoding multiple integers with values betweeen 0 and to 1^60 -1, in a single word.
//!
//! Simple8b is 64bit word-sized encoder that packs multiple integers into a single word using
//! a 4 bit selector values and up to 60 bits for the remaining values.  Integers are encoded using
//! the following table:
//!
//! ┌──────────────┬─────────────────────────────────────────────────────────────┐
//! │   Selector   │       0    1   2   3   4   5   6   7  8  9  0 11 12 13 14 15│
//! ├──────────────┼─────────────────────────────────────────────────────────────┤
//! │     Bits     │       0    0   1   2   3   4   5   6  7  8 10 12 15 20 30 60│
//! ├──────────────┼─────────────────────────────────────────────────────────────┤
//! │      N       │     240  120  60  30  20  15  12  10  8  7  6  5  4  3  2  1│
//! ├──────────────┼─────────────────────────────────────────────────────────────┤
//! │   Wasted Bits│      60   60   0   0   0   0  12   0  4  4  0  0  0  0  0  0│
//! └──────────────┴─────────────────────────────────────────────────────────────┘
//!
//! For example, when the number of values can be encoded using 4 bits, selected 5 is encoded in the
//! 4 most significant bits followed by 15 values encoded used 4 bits each in the remaing 60 bits.
use bytes::{Buf, BufMut};
use snafu::Snafu;

pub const MAX_VALUE: u64 = (1u64 << 60) - 1;

/// Error type for Simple8b.
#[derive(Debug, Snafu, Clone)]
pub enum Error {
    // /// IO error.
    // #[snafu(display("simple8b encode/decode failed: {}", reason))]
    // IOError { reason: std::io::Error },
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

/// Encoder converts a stream of unsigned 64bit integers to a compressed byte slice.
#[derive(Default)]
pub struct Encoder {
    // most recently written integers that have not been flushed
    buf: Vec<u64>,

    // index in buf of the head of the buf
    h: usize,

    // index in buf of the tail of the buf
    t: usize,

    // index into bytes of written bytes
    bp: usize,

    // current bytes written and flushed
    bytes: Vec<u8>,
    b: Vec<u8>,
}

impl Encoder {
    // NewEncoder returns an Encoder able to convert uint64s to compressed byte slices
    pub fn new() -> Self {
        Self {
            buf: vec![0u64; 240],
            b: vec![0u8; 8],
            bytes: vec![0u8; 128],
            ..Default::default()
        }
    }

    pub fn set_values(&mut self, v: Vec<u64>) {
        self.t = v.len();
        self.buf = v;
        self.h = 0;
        self.bytes.clear();
    }

    pub fn reset(&mut self) {
        self.t = 0;
        self.h = 0;
        self.bp = 0;

        self.buf.truncate(240);
        self.b.truncate(8);
        self.bytes.truncate(128);
    }

    pub fn write(&mut self, v: u64) -> Result<()> {
        if self.t >= self.buf.len() {
            self.flush()?;
        }

        // The buf is full but there is space at the front, just shift
        // the values down for now. TODO: use ring buffer
        if self.t >= self.buf.len() {
            let len = self.buf.len();
            unsafe {
                let src = self.buf[self.h..].as_ptr();
                let dst = self.buf.as_mut_ptr();
                std::ptr::copy(src, dst, len - self.h);
            }
            self.t -= self.h;
            self.h = 0;
        }
        self.buf[self.t] = v;
        self.t += 1;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.t == 0 {
            return Ok(());
        }

        // encode as many values into one as we can
        let (encoded, n) = encode(&self.buf[self.h..self.t])?;

        (&mut self.b[..8]).put_u64(encoded);
        if self.bp + 8 > self.bytes.len() {
            self.bytes.extend_from_slice(&self.b);
            self.bp = self.bytes.len()
        } else {
            unsafe {
                let src = self.b.as_ptr();
                let dst = self.bytes[self.bp..self.bp + 8].as_mut_ptr();
                std::ptr::copy(src, dst, 8);
            }
            self.bp += 8;
        }

        // Move the head forward since we encoded those values
        self.h += n as usize;

        // If we encoded them all, reset the head/tail pointers to the beginning
        if self.h == self.t {
            self.h = 0;
            self.t = 0;
        }

        Ok(())
    }

    pub fn bytes(&mut self) -> Result<&[u8]> {
        while self.t > 0 {
            self.flush()?;
        }

        Ok(&self.bytes[..self.bp])
    }
}

/// Decoder converts a compressed byte slice to a stream of unsigned 64bit integers.
pub struct Decoder {
    bytes: Vec<u8>,
    buf: [u64; 240],
    i: usize,
    n: usize,
}

impl Decoder {
    /// Returns a Decoder from a byte slice
    pub fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            buf: [0u64; 240],
            i: 0,
            n: 0,
        }
    }

    pub fn set_bytes(&mut self, b: Vec<u8>) {
        self.bytes = b;
        self.i = 0;
        self.n = 0;
    }
}

impl Iterator for Decoder {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        self.i += 1;
        if self.i >= self.n && self.bytes.len() >= 8 {
            let v = (&self.bytes[..8]).get_u64();
            self.bytes = self.bytes[8..].to_vec();
            self.n = match decode(&mut self.buf, v) {
                Ok(n) => n,
                Err(e) => 0,
            };
            self.i = 0;
        }

        if !(self.bytes.len() >= 8 || (self.i < self.n)) {
            return None;
        }

        Some(self.buf[self.i])
    }
}

struct Packing {
    n: usize,
    bit: usize,
    unpack: fn(u64, &mut [u64; 240]),
    pack: fn(&[u64]) -> u64,
}

const SELECTOR: [Packing; 16] = [
    Packing {
        n: 240,
        bit: 0,
        unpack: unpack240,
        pack: pack240,
    },
    Packing {
        n: 120,
        bit: 0,
        unpack: unpack120,
        pack: pack120,
    },
    Packing {
        n: 60,
        bit: 1,
        unpack: unpack60,
        pack: pack60,
    },
    Packing {
        n: 30,
        bit: 2,
        unpack: unpack30,
        pack: pack30,
    },
    Packing {
        n: 20,
        bit: 3,
        unpack: unpack20,
        pack: pack20,
    },
    Packing {
        n: 15,
        bit: 4,
        unpack: unpack15,
        pack: pack15,
    },
    Packing {
        n: 12,
        bit: 5,
        unpack: unpack12,
        pack: pack12,
    },
    Packing {
        n: 10,
        bit: 6,
        unpack: unpack10,
        pack: pack10,
    },
    Packing {
        n: 8,
        bit: 7,
        unpack: unpack8,
        pack: pack8,
    },
    Packing {
        n: 7,
        bit: 8,
        unpack: unpack7,
        pack: pack7,
    },
    Packing {
        n: 6,
        bit: 10,
        unpack: unpack6,
        pack: pack6,
    },
    Packing {
        n: 5,
        bit: 12,
        unpack: unpack5,
        pack: pack5,
    },
    Packing {
        n: 4,
        bit: 15,
        unpack: unpack4,
        pack: pack4,
    },
    Packing {
        n: 3,
        bit: 20,
        unpack: unpack3,
        pack: pack3,
    },
    Packing {
        n: 2,
        bit: 30,
        unpack: unpack2,
        pack: pack2,
    },
    Packing {
        n: 1,
        bit: 60,
        unpack: unpack1,
        pack: pack1,
    },
];

// Count returns the number of integers encoded in the byte slice
pub fn count_bytes(b: &[u8]) -> Result<i32> {
    let mut b = b;
    let mut count = 0;
    while b.len() >= 8 {
        let v = (&b[..8]).get_u64();
        b = &b[8..];

        let sel = v >> 60;
        if sel >= 16 {
            return Err(Error::StringErr {
                reason: format!("invalid selector value: {}", sel),
            });
        }
        count += SELECTOR[sel as usize].n;
    }

    if !b.is_empty() {
        return Err(Error::StringErr {
            reason: format!("invalid slice len remaining: {}", b.len()),
        });
    }

    Ok(count as i32)
}

// Count returns the number of integers encoded within an uint64
pub fn count(v: u64) -> Result<i32> {
    let sel = v >> 60;
    if sel >= 16 {
        return Err(Error::StringErr {
            reason: format!("invalid selector value: {}", sel),
        });
    }

    Ok(SELECTOR[sel as usize].n as i32)
}

pub fn for_each<F>(b: &[u8], mut f: F) -> Result<()>
where
    F: FnMut(u64) -> bool,
{
    let mut b = b;
    while b.len() >= 8 {
        let mut v = (&b[..8]).get_u64();
        b = &b[8..];

        let sel = v >> 60;
        if sel >= 16 {
            return Err(Error::StringErr {
                reason: format!("invalid selector value: {}", sel),
            });
        }

        let n = SELECTOR[sel as usize].n;
        let bits = SELECTOR[sel as usize].bit;
        let mask = !({ !0 } << bits) as u64;

        for _ in 0..n {
            let val = v & mask;
            if !f(val) {
                return Ok(());
            }
            v >>= bits;
        }
    }
    Ok(())
}

pub fn count_bytes_between(b: &[u8], min: u64, max: u64) -> Result<i32> {
    let mut b = b;
    let mut count = 0;
    while b.len() >= 8 {
        let mut v = (&b[..8]).get_u64();
        b = &b[8..];

        let sel = v >> 60;
        if sel >= 16 {
            return Err(Error::StringErr {
                reason: format!("invalid selector value: {}", sel),
            });
        }
        // If the max value that could be encoded by the uint64 is less than the min
        // skip the whole thing.
        let max_value = ((1i64 << (SELECTOR[sel as usize].bit as u64)) - 1) as u64;
        if max_value < min {
            continue;
        }

        let mask = !({ !0 } << SELECTOR[sel as usize].bit) as u64;

        for i in 0..SELECTOR[sel as usize].n {
            let val = v & mask;
            if val >= min && val < max {
                count += 1;
            } else if val > max {
                break;
            }

            v >>= SELECTOR[sel as usize].bit
        }
    }

    if !b.is_empty() {
        return Err(Error::StringErr {
            reason: format!("invalid slice len remaining: {}", b.len()),
        });
    }
    Ok(count)
}

// Encode packs as many values into a single uint64.  It returns the packed
// uint64, how many values from src were packed, or an error if the values exceed
// the maximum value range.
pub fn encode(src: &[u64]) -> Result<(u64, i32)> {
    if can_pack(src, 240, 0) {
        Ok((0u64, 240))
    } else if can_pack(src, 120, 0) {
        return Ok((1 << 60, 120));
    } else if can_pack(src, 60, 1) {
        return Ok((pack60(&src[..60]), 60));
    } else if can_pack(src, 30, 2) {
        return Ok((pack30(&src[..30]), 30));
    } else if can_pack(src, 20, 3) {
        return Ok((pack20(&src[..20]), 20));
    } else if can_pack(src, 15, 4) {
        return Ok((pack15(&src[..15]), 15));
    } else if can_pack(src, 12, 5) {
        return Ok((pack12(&src[..12]), 12));
    } else if can_pack(src, 10, 6) {
        return Ok((pack10(&src[..10]), 10));
    } else if can_pack(src, 8, 7) {
        return Ok((pack8(&src[..8]), 8));
    } else if can_pack(src, 7, 8) {
        return Ok((pack7(&src[..7]), 7));
    } else if can_pack(src, 6, 10) {
        return Ok((pack6(&src[..6]), 6));
    } else if can_pack(src, 5, 12) {
        return Ok((pack5(&src[..5]), 5));
    } else if can_pack(src, 4, 15) {
        return Ok((pack4(&src[..4]), 4));
    } else if can_pack(src, 3, 20) {
        return Ok((pack3(&src[..3]), 3));
    } else if can_pack(src, 2, 30) {
        return Ok((pack2(&src[..2]), 2));
    } else if can_pack(src, 1, 60) {
        return Ok((pack1(&src[..1]), 1));
    } else {
        if !src.is_empty() {
            return Err(Error::StringErr {
                reason: format!("value out of bounds: {:?}", src),
            });
        }
        return Ok((0, 0));
    }
}

// Encode returns the packed bytes size.  If a value is over
// 1 << 60, an error is returned.  The input src is modified to avoid extra
// allocations.
pub fn encode_all(src: &mut [u64]) -> Result<i32> {
    let mut i = 0;

    // Re-use the input slice and write encoded values back in place
    // let dst = &mut src;
    let mut j = 0;

    loop {
        if i >= src.len() {
            break;
        }
        let remaining = &src[i..];

        if can_pack(remaining, 240, 0) {
            src[j] = 0;
            i += 240;
        } else if can_pack(remaining, 120, 0) {
            src[j] = 1 << 60;
            i += 120;
        } else if can_pack(remaining, 60, 1) {
            src[j] = pack60(&src[i..i + 60]);
            i += 60;
        } else if can_pack(remaining, 30, 2) {
            src[j] = pack30(&src[i..i + 30]);
            i += 30;
        } else if can_pack(remaining, 20, 3) {
            src[j] = pack20(&src[i..i + 20]);
            i += 20;
        } else if can_pack(remaining, 15, 4) {
            src[j] = pack15(&src[i..i + 15]);
            i += 15;
        } else if can_pack(remaining, 12, 5) {
            src[j] = pack12(&src[i..i + 12]);
            i += 12;
        } else if can_pack(remaining, 10, 6) {
            src[j] = pack10(&src[i..i + 10]);
            i += 10;
        } else if can_pack(remaining, 8, 7) {
            src[j] = pack8(&src[i..i + 8]);
            i += 8;
        } else if can_pack(remaining, 7, 8) {
            src[j] = pack7(&src[i..i + 7]);
            i += 7;
        } else if can_pack(remaining, 6, 10) {
            src[j] = pack6(&src[i..i + 6]);
            i += 6;
        } else if can_pack(remaining, 5, 12) {
            src[j] = pack5(&src[i..i + 5]);
            i += 5;
        } else if can_pack(remaining, 4, 15) {
            src[j] = pack4(&src[i..i + 4]);
            i += 4;
        } else if can_pack(remaining, 3, 20) {
            src[j] = pack3(&src[i..i + 3]);
            i += 3;
        } else if can_pack(remaining, 2, 30) {
            src[j] = pack2(&src[i..i + 2]);
            i += 2;
        } else if can_pack(remaining, 1, 60) {
            src[j] = pack1(&src[i..i + 1]);
            i += 1;
        } else {
            return Err(Error::StringErr {
                reason: "value out of bounds".to_string(),
            });
        }
        j += 1
    }
    Ok(j as i32)
}

pub fn decode(dst: &mut [u64; 240], v: u64) -> Result<usize> {
    let sel = v >> 60;
    if sel >= 16 {
        return Err(Error::StringErr {
            reason: format!("invalid selector value: {}", sel),
        });
    }
    (SELECTOR[sel as usize].unpack)(v, dst);
    Ok(SELECTOR[sel as usize].n)
}

// decode_all writes the uncompressed values from src to dst.  It returns the number
// of values written or an error.
pub fn decode_all(dst: &mut [u64], src: &[u64]) -> Result<usize> {
    let mut j = 0;
    for v in src {
        let sel = *v >> 60;
        if sel >= 16 {
            return Err(Error::StringErr {
                reason: format!("invalid selector value: {}", sel),
            });
        }

        let dst = &mut dst[j..];
        let ptr = dst.as_mut_ptr() as *mut [u64; 240];
        let new_dst = unsafe { &mut *ptr };
        (SELECTOR[sel as usize].unpack)(*v, new_dst);
        j += SELECTOR[sel as usize].n;
    }

    Ok(j)
}

// DecodeBytesBigEndian writes the compressed, big-endian values from src to dst.  It returns the number
// of values written or an error.
pub fn decode_bytes(dst: &mut [u64], src: &[u8]) -> Result<usize> {
    if src.len() & 7 != 0 {
        return Err(Error::StringErr {
            reason: "src length is not multiple of 8".to_string(),
        });
    }

    let mut i = 0;
    let mut j = 0;
    while i < src.len() {
        let v = u64::from_be_bytes(src[i..i + 8].try_into().unwrap());
        let sel = ((v >> 60) & 0xf) as usize;
        let dst = &mut dst[j..];
        let ptr = dst.as_mut_ptr() as *mut [u64; 240];
        let new_dst = unsafe { &mut *ptr };
        (SELECTOR[sel].unpack)(v, new_dst);
        j += SELECTOR[sel].n;
        i += 8;
    }

    Ok(j)
}

// can_pack returs true if n elements from in can be stored using bits per element
fn can_pack(src: &[u64], n: usize, bits: i32) -> bool {
    if src.len() < n {
        return false;
    }

    let mut end = src.len();
    if n < end {
        end = n;
    }

    // Selector 0,1 are special and use 0 bits to encode runs of 1's
    if bits == 0 {
        for v in src {
            if *v != 1 {
                return false;
            }
        }
        return true;
    }

    let max = ((1i64 << (bits as u64)) - 1) as u64;

    for item in src.iter().take(end) {
        if *item > max {
            return false;
        }
    }

    true
}

// pack240 packs 240 ones from in using 1 bit each
fn pack240(src: &[u64]) -> u64 {
    0
}

// pack120 packs 120 ones from in using 1 bit each
fn pack120(src: &[u64]) -> u64 {
    0
}

// pack60 packs 60 values from in using 1 bit each
fn pack60(src: &[u64]) -> u64 {
    2 << 60
        | src[0]
        | src[1] << 1
        | src[2] << 2
        | src[3] << 3
        | src[4] << 4
        | src[5] << 5
        | src[6] << 6
        | src[7] << 7
        | src[8] << 8
        | src[9] << 9
        | src[10] << 10
        | src[11] << 11
        | src[12] << 12
        | src[13] << 13
        | src[14] << 14
        | src[15] << 15
        | src[16] << 16
        | src[17] << 17
        | src[18] << 18
        | src[19] << 19
        | src[20] << 20
        | src[21] << 21
        | src[22] << 22
        | src[23] << 23
        | src[24] << 24
        | src[25] << 25
        | src[26] << 26
        | src[27] << 27
        | src[28] << 28
        | src[29] << 29
        | src[30] << 30
        | src[31] << 31
        | src[32] << 32
        | src[33] << 33
        | src[34] << 34
        | src[35] << 35
        | src[36] << 36
        | src[37] << 37
        | src[38] << 38
        | src[39] << 39
        | src[40] << 40
        | src[41] << 41
        | src[42] << 42
        | src[43] << 43
        | src[44] << 44
        | src[45] << 45
        | src[46] << 46
        | src[47] << 47
        | src[48] << 48
        | src[49] << 49
        | src[50] << 50
        | src[51] << 51
        | src[52] << 52
        | src[53] << 53
        | src[54] << 54
        | src[55] << 55
        | src[56] << 56
        | src[57] << 57
        | src[58] << 58
        | src[59] << 59
}

// pack30 packs 30 values from in using 2 bits each
fn pack30(src: &[u64]) -> u64 {
    3 << 60
        | src[0]
        | src[1] << 2
        | src[2] << 4
        | src[3] << 6
        | src[4] << 8
        | src[5] << 10
        | src[6] << 12
        | src[7] << 14
        | src[8] << 16
        | src[9] << 18
        | src[10] << 20
        | src[11] << 22
        | src[12] << 24
        | src[13] << 26
        | src[14] << 28
        | src[15] << 30
        | src[16] << 32
        | src[17] << 34
        | src[18] << 36
        | src[19] << 38
        | src[20] << 40
        | src[21] << 42
        | src[22] << 44
        | src[23] << 46
        | src[24] << 48
        | src[25] << 50
        | src[26] << 52
        | src[27] << 54
        | src[28] << 56
        | src[29] << 58
}

// pack20 packs 20 values from in using 3 bits each
fn pack20(src: &[u64]) -> u64 {
    4 << 60
        | src[0]
        | src[1] << 3
        | src[2] << 6
        | src[3] << 9
        | src[4] << 12
        | src[5] << 15
        | src[6] << 18
        | src[7] << 21
        | src[8] << 24
        | src[9] << 27
        | src[10] << 30
        | src[11] << 33
        | src[12] << 36
        | src[13] << 39
        | src[14] << 42
        | src[15] << 45
        | src[16] << 48
        | src[17] << 51
        | src[18] << 54
        | src[19] << 57
}

// pack15 packs 15 values from in using 3 bits each
fn pack15(src: &[u64]) -> u64 {
    5 << 60
        | src[0]
        | src[1] << 4
        | src[2] << 8
        | src[3] << 12
        | src[4] << 16
        | src[5] << 20
        | src[6] << 24
        | src[7] << 28
        | src[8] << 32
        | src[9] << 36
        | src[10] << 40
        | src[11] << 44
        | src[12] << 48
        | src[13] << 52
        | src[14] << 56
}

// pack12 packs 12 values from in using 5 bits each
fn pack12(src: &[u64]) -> u64 {
    6 << 60
        | src[0]
        | src[1] << 5
        | src[2] << 10
        | src[3] << 15
        | src[4] << 20
        | src[5] << 25
        | src[6] << 30
        | src[7] << 35
        | src[8] << 40
        | src[9] << 45
        | src[10] << 50
        | src[11] << 55
}

// pack10 packs 10 values from in using 6 bits each
fn pack10(src: &[u64]) -> u64 {
    7 << 60
        | src[0]
        | src[1] << 6
        | src[2] << 12
        | src[3] << 18
        | src[4] << 24
        | src[5] << 30
        | src[6] << 36
        | src[7] << 42
        | src[8] << 48
        | src[9] << 54
}

// pack8 packs 8 values from in using 7 bits each
fn pack8(src: &[u64]) -> u64 {
    8 << 60
        | src[0]
        | src[1] << 7
        | src[2] << 14
        | src[3] << 21
        | src[4] << 28
        | src[5] << 35
        | src[6] << 42
        | src[7] << 49
}

// pack7 packs 7 values from in using 8 bits each
fn pack7(src: &[u64]) -> u64 {
    9 << 60
        | src[0]
        | src[1] << 8
        | src[2] << 16
        | src[3] << 24
        | src[4] << 32
        | src[5] << 40
        | src[6] << 48
}

// pack6 packs 6 values from in using 10 bits each
fn pack6(src: &[u64]) -> u64 {
    10 << 60 | src[0] | src[1] << 10 | src[2] << 20 | src[3] << 30 | src[4] << 40 | src[5] << 50
}

// pack5 packs 5 values from in using 12 bits each
fn pack5(src: &[u64]) -> u64 {
    11 << 60 | src[0] | src[1] << 12 | src[2] << 24 | src[3] << 36 | src[4] << 48
}

// pack4 packs 4 values from in using 15 bits each
fn pack4(src: &[u64]) -> u64 {
    12 << 60 | src[0] | src[1] << 15 | src[2] << 30 | src[3] << 45
}

// pack3 packs 3 values from in using 20 bits each
fn pack3(src: &[u64]) -> u64 {
    13 << 60 | src[0] | src[1] << 20 | src[2] << 40
}

// pack2 packs 2 values from in using 30 bits each
fn pack2(src: &[u64]) -> u64 {
    14 << 60 | src[0] | src[1] << 30
}

// pack1 packs 1 values from in using 60 bits each
fn pack1(src: &[u64]) -> u64 {
    15 << 60 | src[0]
}

fn unpack240(_v: u64, dst: &mut [u64; 240]) {
    for i in dst {
        *i = 1
    }
}

fn unpack120(_v: u64, dst: &mut [u64; 240]) {
    for i in &mut dst[..120] {
        *i = 1
    }
}

fn unpack60(v: u64, dst: &mut [u64; 240]) {
    dst[0] = v & 1;
    dst[1] = (v >> 1) & 1;
    dst[2] = (v >> 2) & 1;
    dst[3] = (v >> 3) & 1;
    dst[4] = (v >> 4) & 1;
    dst[5] = (v >> 5) & 1;
    dst[6] = (v >> 6) & 1;
    dst[7] = (v >> 7) & 1;
    dst[8] = (v >> 8) & 1;
    dst[9] = (v >> 9) & 1;
    dst[10] = (v >> 10) & 1;
    dst[11] = (v >> 11) & 1;
    dst[12] = (v >> 12) & 1;
    dst[13] = (v >> 13) & 1;
    dst[14] = (v >> 14) & 1;
    dst[15] = (v >> 15) & 1;
    dst[16] = (v >> 16) & 1;
    dst[17] = (v >> 17) & 1;
    dst[18] = (v >> 18) & 1;
    dst[19] = (v >> 19) & 1;
    dst[20] = (v >> 20) & 1;
    dst[21] = (v >> 21) & 1;
    dst[22] = (v >> 22) & 1;
    dst[23] = (v >> 23) & 1;
    dst[24] = (v >> 24) & 1;
    dst[25] = (v >> 25) & 1;
    dst[26] = (v >> 26) & 1;
    dst[27] = (v >> 27) & 1;
    dst[28] = (v >> 28) & 1;
    dst[29] = (v >> 29) & 1;
    dst[30] = (v >> 30) & 1;
    dst[31] = (v >> 31) & 1;
    dst[32] = (v >> 32) & 1;
    dst[33] = (v >> 33) & 1;
    dst[34] = (v >> 34) & 1;
    dst[35] = (v >> 35) & 1;
    dst[36] = (v >> 36) & 1;
    dst[37] = (v >> 37) & 1;
    dst[38] = (v >> 38) & 1;
    dst[39] = (v >> 39) & 1;
    dst[40] = (v >> 40) & 1;
    dst[41] = (v >> 41) & 1;
    dst[42] = (v >> 42) & 1;
    dst[43] = (v >> 43) & 1;
    dst[44] = (v >> 44) & 1;
    dst[45] = (v >> 45) & 1;
    dst[46] = (v >> 46) & 1;
    dst[47] = (v >> 47) & 1;
    dst[48] = (v >> 48) & 1;
    dst[49] = (v >> 49) & 1;
    dst[50] = (v >> 50) & 1;
    dst[51] = (v >> 51) & 1;
    dst[52] = (v >> 52) & 1;
    dst[53] = (v >> 53) & 1;
    dst[54] = (v >> 54) & 1;
    dst[55] = (v >> 55) & 1;
    dst[56] = (v >> 56) & 1;
    dst[57] = (v >> 57) & 1;
    dst[58] = (v >> 58) & 1;
    dst[59] = (v >> 59) & 1;
}

fn unpack30(v: u64, dst: &mut [u64; 240]) {
    dst[0] = v & 3;
    dst[1] = (v >> 2) & 3;
    dst[2] = (v >> 4) & 3;
    dst[3] = (v >> 6) & 3;
    dst[4] = (v >> 8) & 3;
    dst[5] = (v >> 10) & 3;
    dst[6] = (v >> 12) & 3;
    dst[7] = (v >> 14) & 3;
    dst[8] = (v >> 16) & 3;
    dst[9] = (v >> 18) & 3;
    dst[10] = (v >> 20) & 3;
    dst[11] = (v >> 22) & 3;
    dst[12] = (v >> 24) & 3;
    dst[13] = (v >> 26) & 3;
    dst[14] = (v >> 28) & 3;
    dst[15] = (v >> 30) & 3;
    dst[16] = (v >> 32) & 3;
    dst[17] = (v >> 34) & 3;
    dst[18] = (v >> 36) & 3;
    dst[19] = (v >> 38) & 3;
    dst[20] = (v >> 40) & 3;
    dst[21] = (v >> 42) & 3;
    dst[22] = (v >> 44) & 3;
    dst[23] = (v >> 46) & 3;
    dst[24] = (v >> 48) & 3;
    dst[25] = (v >> 50) & 3;
    dst[26] = (v >> 52) & 3;
    dst[27] = (v >> 54) & 3;
    dst[28] = (v >> 56) & 3;
    dst[29] = (v >> 58) & 3;
}

fn unpack20(v: u64, dst: &mut [u64; 240]) {
    dst[0] = v & 7;
    dst[1] = (v >> 3) & 7;
    dst[2] = (v >> 6) & 7;
    dst[3] = (v >> 9) & 7;
    dst[4] = (v >> 12) & 7;
    dst[5] = (v >> 15) & 7;
    dst[6] = (v >> 18) & 7;
    dst[7] = (v >> 21) & 7;
    dst[8] = (v >> 24) & 7;
    dst[9] = (v >> 27) & 7;
    dst[10] = (v >> 30) & 7;
    dst[11] = (v >> 33) & 7;
    dst[12] = (v >> 36) & 7;
    dst[13] = (v >> 39) & 7;
    dst[14] = (v >> 42) & 7;
    dst[15] = (v >> 45) & 7;
    dst[16] = (v >> 48) & 7;
    dst[17] = (v >> 51) & 7;
    dst[18] = (v >> 54) & 7;
    dst[19] = (v >> 57) & 7;
}

fn unpack15(v: u64, dst: &mut [u64; 240]) {
    dst[0] = v & 15;
    dst[1] = (v >> 4) & 15;
    dst[2] = (v >> 8) & 15;
    dst[3] = (v >> 12) & 15;
    dst[4] = (v >> 16) & 15;
    dst[5] = (v >> 20) & 15;
    dst[6] = (v >> 24) & 15;
    dst[7] = (v >> 28) & 15;
    dst[8] = (v >> 32) & 15;
    dst[9] = (v >> 36) & 15;
    dst[10] = (v >> 40) & 15;
    dst[11] = (v >> 44) & 15;
    dst[12] = (v >> 48) & 15;
    dst[13] = (v >> 52) & 15;
    dst[14] = (v >> 56) & 15;
}

fn unpack12(v: u64, dst: &mut [u64; 240]) {
    dst[0] = v & 31;
    dst[1] = (v >> 5) & 31;
    dst[2] = (v >> 10) & 31;
    dst[3] = (v >> 15) & 31;
    dst[4] = (v >> 20) & 31;
    dst[5] = (v >> 25) & 31;
    dst[6] = (v >> 30) & 31;
    dst[7] = (v >> 35) & 31;
    dst[8] = (v >> 40) & 31;
    dst[9] = (v >> 45) & 31;
    dst[10] = (v >> 50) & 31;
    dst[11] = (v >> 55) & 31;
}

fn unpack10(v: u64, dst: &mut [u64; 240]) {
    dst[0] = v & 63;
    dst[1] = (v >> 6) & 63;
    dst[2] = (v >> 12) & 63;
    dst[3] = (v >> 18) & 63;
    dst[4] = (v >> 24) & 63;
    dst[5] = (v >> 30) & 63;
    dst[6] = (v >> 36) & 63;
    dst[7] = (v >> 42) & 63;
    dst[8] = (v >> 48) & 63;
    dst[9] = (v >> 54) & 63;
}

fn unpack8(v: u64, dst: &mut [u64; 240]) {
    dst[0] = v & 127;
    dst[1] = (v >> 7) & 127;
    dst[2] = (v >> 14) & 127;
    dst[3] = (v >> 21) & 127;
    dst[4] = (v >> 28) & 127;
    dst[5] = (v >> 35) & 127;
    dst[6] = (v >> 42) & 127;
    dst[7] = (v >> 49) & 127;
}

fn unpack7(v: u64, dst: &mut [u64; 240]) {
    dst[0] = v & 255;
    dst[1] = (v >> 8) & 255;
    dst[2] = (v >> 16) & 255;
    dst[3] = (v >> 24) & 255;
    dst[4] = (v >> 32) & 255;
    dst[5] = (v >> 40) & 255;
    dst[6] = (v >> 48) & 255;
}

fn unpack6(v: u64, dst: &mut [u64; 240]) {
    dst[0] = v & 1023;
    dst[1] = (v >> 10) & 1023;
    dst[2] = (v >> 20) & 1023;
    dst[3] = (v >> 30) & 1023;
    dst[4] = (v >> 40) & 1023;
    dst[5] = (v >> 50) & 1023;
}

fn unpack5(v: u64, dst: &mut [u64; 240]) {
    dst[0] = v & 4095;
    dst[1] = (v >> 12) & 4095;
    dst[2] = (v >> 24) & 4095;
    dst[3] = (v >> 36) & 4095;
    dst[4] = (v >> 48) & 4095;
}

fn unpack4(v: u64, dst: &mut [u64; 240]) {
    dst[0] = v & 32767;
    dst[1] = (v >> 15) & 32767;
    dst[2] = (v >> 30) & 32767;
    dst[3] = (v >> 45) & 32767;
}

fn unpack3(v: u64, dst: &mut [u64; 240]) {
    dst[0] = v & 1048575;
    dst[1] = (v >> 20) & 1048575;
    dst[2] = (v >> 40) & 1048575;
}

fn unpack2(v: u64, dst: &mut [u64; 240]) {
    dst[0] = v & 1073741823;
    dst[1] = (v >> 30) & 1073741823;
}

fn unpack1(v: u64, dst: &mut [u64; 240]) {
    dst[0] = v & 1152921504606846975;
}

#[cfg(any(test, feature = "benchmarking"))]
pub mod tests {
    use std::cmp::Ordering;

    use rand::Rng;
    use roaring::MultiOps;

    use super::*;
    #[test]
    fn test_encode_no_values() {
        let mut src = vec![];
        let encoded = encode_all(&mut src).unwrap();

        let mut decoded = vec![0u64; src.len()];
        let n = decode_all(&mut decoded, &src[..encoded as usize]).unwrap();

        if src.len() != (&decoded[..]).len() {
            panic!("Len mismatch: got {}, exp {}", decoded.len(), src.len());
        }
    }

    fn ones(n: u64) -> impl Fn() -> Vec<u64> {
        move || {
            let mut in_vec = vec![0; n as usize];
            for i in in_vec.iter_mut() {
                *i = 1;
            }
            in_vec
        }
    }

    pub fn bits_n(b: i32) -> impl Fn(i32) -> Vec<u64> {
        move |n: i32| {
            let mut out = vec![0; n as usize];
            let max_val = 1u64 << (b as u8);
            for i in out.iter_mut() {
                let top_bit = ((*i & 1) << ((b - 1) as u8)) as u64;
                *i = rand::thread_rng().gen_range(0..max_val) as u64 | top_bit;
                assert!(*i < max_val, "max");
            }
            out
        }
    }

    // bits generates sequence of n numbers with max bits,
    // ensuring max bit is set for 50% of the values.
    fn bits(n: i32, bits: i32) -> impl Fn() -> Vec<u64> {
        move || {
            let mut out = vec![0; n as usize];
            let max_val = 1u64 << (bits as u8);
            for i in out.iter_mut() {
                let top_bit = ((*i & 1) << ((bits - 1) as u8)) as u64;
                *i = rand::thread_rng().gen_range(0..max_val) as u64 | top_bit;
                assert!(*i < max_val, "max");
            }
            out
        }
    }

    pub fn combile_n(bits: Vec<i32>) -> impl Fn(i32) -> Vec<u64> {
        let mut fns = vec![];
        for b in bits {
            let x = bits_n(b);
            fns.push(x);
        }

        move |n: i32| {
            let mut out = vec![];
            for f in &fns {
                out.extend(f(n));
            }
            out
        }
    }

    fn combile(fns: Vec<impl Fn() -> Vec<u64>>) -> impl Fn() -> Vec<u64> {
        move || {
            let mut out = vec![];
            for f in &fns {
                out.extend(f());
            }
            out
        }
    }

    // test_encode_all ensures 100% test coverage of simple8b.encode_all and
    // verifies all output by comparing the original input with the output of simple8b.encode_all
    #[test]
    fn test_encode_all() {
        struct Test {
            name: &'static str,
            input: Vec<u64>,
            // f: Option<impl Fn() -> Vec<u64>>,
            f: Option<Box<dyn Fn() -> Vec<u64>>>,
            err: Option<Error>,
        }

        let mut tests = vec![
            Test {
                name: "mo values",
                input: vec![],
                f: None,
                err: None,
            },
            Test {
                name: "mixed sizes",
                input: vec![7, 6, 256, 4, 3, 2, 1],
                f: None,
                err: None,
            },
            Test {
                name: "too big",
                input: vec![7, 6, 2 << 61 - 1, 4, 3, 2, 1],
                f: None,
                err: Some(Error::StringErr {
                    reason: format!("value out of bounds"),
                }),
            },
            Test {
                name: "1 bit",
                input: vec![],
                f: Some(Box::new(bits(100, 1))),
                err: None,
            },
            Test {
                name: "2 bit",
                input: vec![],
                f: Some(Box::new(bits(100, 2))),
                err: None,
            },
            Test {
                name: "3 bit",
                input: vec![],
                f: Some(Box::new(bits(100, 3))),
                err: None,
            },
            Test {
                name: "4 bit",
                input: vec![],
                f: Some(Box::new(bits(100, 4))),
                err: None,
            },
            Test {
                name: "5 bit",
                input: vec![],
                f: Some(Box::new(bits(100, 5))),
                err: None,
            },
            Test {
                name: "6 bit",
                input: vec![],
                f: Some(Box::new(bits(100, 6))),
                err: None,
            },
            Test {
                name: "7 bit",
                input: vec![],
                f: Some(Box::new(bits(100, 7))),
                err: None,
            },
            Test {
                name: "8 bit",
                input: vec![],
                f: Some(Box::new(bits(100, 8))),
                err: None,
            },
            Test {
                name: "10 bit",
                input: vec![],
                f: Some(Box::new(bits(100, 10))),
                err: None,
            },
            Test {
                name: "12 bit",
                input: vec![],
                f: Some(Box::new(bits(100, 12))),
                err: None,
            },
            Test {
                name: "15 bit",
                input: vec![],
                f: Some(Box::new(bits(100, 15))),
                err: None,
            },
            Test {
                name: "20 bit",
                input: vec![],
                f: Some(Box::new(bits(100, 20))),
                err: None,
            },
            Test {
                name: "30 bit",
                input: vec![],
                f: Some(Box::new(bits(100, 30))),
                err: None,
            },
            Test {
                name: "60 bit",
                input: vec![],
                f: Some(Box::new(bits(100, 60))),
                err: None,
            },
            Test {
                name: "combination",
                input: vec![],
                f: Some(Box::new(combile(vec![
                    bits(100, 1),
                    bits(100, 2),
                    bits(100, 3),
                    bits(100, 4),
                    bits(100, 5),
                    bits(100, 6),
                    bits(100, 7),
                    bits(100, 8),
                    bits(100, 10),
                    bits(100, 12),
                    bits(100, 15),
                    bits(100, 20),
                    bits(100, 30),
                    bits(100, 60),
                ]))),
                err: None,
            },
            Test {
                name: "240 ones",
                input: vec![],
                f: Some(Box::new(ones(240))),
                err: None,
            },
            Test {
                name: "120 ones",
                input: vec![],
                f: Some(Box::new(move || {
                    let mut v = ones(240)();
                    v[120] = 5;
                    v
                })),
                err: None,
            },
            Test {
                name: "119 ones",
                input: vec![],
                f: Some(Box::new(move || {
                    let mut v = ones(240)();
                    v[119] = 5;
                    v
                })),
                err: None,
            },
            Test {
                name: "239 ones",
                input: vec![],
                f: Some(Box::new(move || {
                    let mut v = ones(241)();
                    v[239] = 5;
                    v
                })),
                err: None,
            },
        ];

        for test in tests.iter_mut() {
            if test.f.is_some() {
                test.input = (*test.f.as_ref().unwrap())();
            }

            let mut src = test.input.clone();
            let encoded = match encode_all(&mut src) {
                Ok(encoded) => encoded,
                Err(e) => {
                    let got_err = if let Error::StringErr { ref reason } = e {
                        Some(reason)
                    } else {
                        None
                    };
                    let exp_err = if let Some(Error::StringErr { reason }) = &test.err {
                        Some(reason)
                    } else {
                        None
                    };
                    assert_eq!(got_err, exp_err, "expected encode error, got: {:?}", e);
                    continue;
                }
            };
            let mut decoded = vec![0u64; test.input.len()];
            let n = decode_all(&mut decoded, &src[..encoded as usize]).unwrap();
            assert_eq!(
                &decoded[..n],
                &test.input,
                "unexpected values; got:
{:?}, exp:
{:?}",
                &decoded[..n],
                &test.input
            );
        }
    }

    #[test]
    fn test_too_big() {
        let values = 1;
        let mut src = vec![0u64; values];
        for i in 0..values {
            src[i] = 2 << 61 - 1;
        }

        assert!(encode_all(&mut src).is_err(), "expected Err, got Ok");
    }

    #[test]
    fn test_few_values() {
        test_encode(20, 2)
    }
    #[test]
    fn test_encode_multiple_zeros() {
        test_encode(250, 0)
    }
    #[test]
    fn test_encode_multiple_ones() {
        test_encode(250, 1)
    }
    #[test]
    fn test_encode_multiple_large() {
        test_encode(250, 134)
    }
    #[test]
    fn test_encode_240_ones() {
        test_encode(240, 1)
    }
    #[test]
    fn test_encode_120_ones() {
        test_encode(120, 1)
    }
    #[test]
    fn test_encode_60() {
        test_encode(60, 1)
    }
    #[test]
    fn test_encode_30() {
        test_encode(30, 3)
    }
    #[test]
    fn test_encode_20() {
        test_encode(20, 7)
    }
    #[test]
    fn test_encode_15() {
        test_encode(15, 15)
    }
    #[test]
    fn test_encode_12() {
        test_encode(12, 31)
    }
    #[test]
    fn test_encode_10() {
        test_encode(10, 63)
    }
    #[test]
    fn test_encode_8() {
        test_encode(8, 127)
    }
    #[test]
    fn test_encode_7() {
        test_encode(7, 255)
    }
    #[test]
    fn test_encode_6() {
        test_encode(6, 1023)
    }
    #[test]
    fn test_encode_5() {
        test_encode(5, 4095)
    }
    #[test]
    fn test_encode_4() {
        test_encode(4, 32767)
    }
    #[test]
    fn test_encode_3() {
        test_encode(3, 1048575)
    }
    #[test]
    fn test_encode_2() {
        test_encode(2, 1073741823)
    }
    #[test]
    fn test_encode_1() {
        test_encode(1, 1152921504606846975)
    }

    fn test_encode(n: i32, val: u64) {
        let mut enc = Encoder::new();
        let mut src = vec![0u64; n as usize];
        for i in 0..n {
            src[i as usize] = val;
            enc.write(src[i as usize]).unwrap();
        }

        let encoded = enc.bytes().unwrap().to_vec();
        let count_bytes = count_bytes(&encoded).unwrap();

        let dec = Decoder::new(encoded);
        let mut i = 0;
        for v in dec {
            if i >= src.len() {
                panic!("Decoded too many values: got {}, exp {}", i, src.len());
            }

            assert_eq!(v, src[i], "Decoded[{}] != {}, got {}", i, src[i], v);
            i += 1;
        }

        assert_eq!(n, i as i32, "Decode len mismatch: exp {}, got {}", n, i);

        // let got = count_bytes(&encoded).unwrap();
        assert_eq!(
            count_bytes, n,
            "Count mismatch: got {}, exp {}",
            count_bytes, n
        );
    }

    // fn Test_Bytes() {
    //     enc = simple8b.NewEncoder()
    //     for i = 0; i < 30; i++ {
    //         enc.Write(uint64(i))
    //     }
    //     b, _ = enc.Bytes()

    //     dec = simple8b.NewDecoder(b)
    //     x = uint64(0)
    //     for dec.Next() {
    //         if x != dec.Read() {
    //             t.Fatalf("mismatch: got %v, exp %v", dec.Read(), x)
    //         }
    //         x += 1
    //     }
    // }

    // fn test_encode_ValueTooLarge() {
    //     enc = simple8b.NewEncoder()

    //     values = []uint64{
    //         1442369134000000000, 0,
    //     }

    //     for _, v = range values {
    //         enc.Write(v)
    //     }

    //     _, err = enc.Bytes()
    //     if err == nil {
    //         t.Fatalf("Expected error, got nil")

    //     }
    // }

    // fn Test_Decode_NotEnoughBytes() {
    //     dec = simple8b.NewDecoder([]byte{0})
    //     if dec.Next() {
    //         t.Fatalf("Expected Next to return false but it returned true")
    //     }
    // }

    // fn TestCountBytesBetween() {
    //     enc = simple8b.NewEncoder()
    //     in = make([]uint64, 8)
    //     for i = 0; i < len(in); i++ {
    //         in[i] = uint64(i)
    //         enc.Write(in[i])
    //     }

    //     encoded, err = enc.Bytes()
    //     if err != nil {
    //         t.Fatalf("Unexpected error: %v", err)
    //     }

    //     dec = simple8b.NewDecoder(encoded)
    //     i = 0
    //     for dec.Next() {
    //         if i >= len(in) {
    //             t.Fatalf("Decoded too many values: got %v, exp %v", i, len(in))
    //         }

    //         if dec.Read() != in[i] {
    //             t.Fatalf("Decoded[%d] != %v, got %v", i, in[i], dec.Read())
    //         }
    //         i += 1
    //     }

    //     if exp, got = len(in), i; got != exp {
    //         t.Fatalf("Decode len mismatch: exp %v, got %v", exp, got)
    //     }

    //     got, err = simple8b.CountBytesBetween(encoded, 2, 6)
    //     if err != nil {
    //         t.Fatalf("Unexpected error in Count: %v", err)
    //     }
    //     if got != 4 {
    //         t.Fatalf("Count mismatch: got %v, exp %v", got, 4)
    //     }
    // }

    // fn TestCountBytesBetween_SkipMin() {
    //     enc = simple8b.NewEncoder()
    //     in = make([]uint64, 8)
    //     for i = 0; i < len(in); i++ {
    //         in[i] = uint64(i)
    //         enc.Write(in[i])
    //     }
    //     in = append(in, 100000)
    //     enc.Write(100000)

    //     encoded, err = enc.Bytes()
    //     if err != nil {
    //         t.Fatalf("Unexpected error: %v", err)
    //     }

    //     dec = simple8b.NewDecoder(encoded)
    //     i = 0
    //     for dec.Next() {
    //         if i >= len(in) {
    //             t.Fatalf("Decoded too many values: got %v, exp %v", i, len(in))
    //         }

    //         if dec.Read() != in[i] {
    //             t.Fatalf("Decoded[%d] != %v, got %v", i, in[i], dec.Read())
    //         }
    //         i += 1
    //     }

    //     if exp, got = len(in), i; got != exp {
    //         t.Fatalf("Decode len mismatch: exp %v, got %v", exp, got)
    //     }

    //     got, err = simple8b.CountBytesBetween(encoded, 100000, 100001)
    //     if err != nil {
    //         t.Fatalf("Unexpected error in Count: %v", err)
    //     }
    //     if got != 1 {
    //         t.Fatalf("Count mismatch: got %v, exp %v", got, 1)
    //     }
    // }

    // fn BenchmarkEncode(b *testing.B) {
    //     total = 0
    //     x = make([]uint64, 1024)
    //     for i = 0; i < len(x); i++ {
    //         x[i] = uint64(15)
    //     }

    //     b.ResetTimer()
    //     for i = 0; i < b.N; i++ {
    //         simple8b.EncodeAll(x)
    //         b.SetBytes(int64(len(x) * 8))
    //         total += len(x)
    //     }
    // }

    // fn BenchmarkEncoder(b *testing.B) {
    //     x = make([]uint64, 1024)
    //     for i = 0; i < len(x); i++ {
    //         x[i] = uint64(15)
    //     }

    //     enc = simple8b.NewEncoder()
    //     b.ResetTimer()
    //     for i = 0; i < b.N; i++ {
    //         enc.SetValues(x)
    //         enc.Bytes()
    //         b.SetBytes(int64(len(x)) * 8)
    //     }
    // }
    // fn BenchmarkDecode(b *testing.B) {
    //     total = 0

    //     x = make([]uint64, 1024)
    //     for i = 0; i < len(x); i++ {
    //         x[i] = uint64(10)
    //     }
    //     y, _ = simple8b.EncodeAll(x)

    //     decoded = make([]uint64, len(x))

    //     b.ResetTimer()

    //     for i = 0; i < b.N; i++ {
    //         _, _ = simple8b.DecodeAll(decoded, y)
    //         b.SetBytes(int64(len(decoded) * 8))
    //         total += len(decoded)
    //     }
    // }

    // fn BenchmarkDecoder(b *testing.B) {
    //     enc = simple8b.NewEncoder()
    //     x = make([]uint64, 1024)
    //     for i = 0; i < len(x); i++ {
    //         x[i] = uint64(10)
    //         enc.Write(x[i])
    //     }
    //     y, _ = enc.Bytes()

    //     b.ResetTimer()

    //     dec = simple8b.NewDecoder(y)
    //     for i = 0; i < b.N; i++ {
    //         dec.SetBytes(y)
    //         j = 0
    //         for dec.Next() {
    //             j += 1
    //         }
    //         b.SetBytes(int64(j * 8))
    //     }
    // }
}
