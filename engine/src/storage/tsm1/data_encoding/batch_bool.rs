use bytes::{Buf, BufMut};
use integer_encoding::VarInt;
use snafu::{ResultExt, Snafu};

use crate::storage::tsm1::data_encoding::bool::*;

/// bool_array_encode_all encodes src into b, returning b and any error encountered.
/// The returned slice may be of a different length and capacity to b.
pub fn bool_array_encode_all(src: &[bool]) -> Result<Vec<u8>> {
    let sz = 1 + 8 + ((src.len() + 7) / 8); // Header + Num bools + bool data.

    let mut dst = vec![0u8; sz];

    // Store the encoding type in the 4 high bits of the first byte
    dst[0] = BOOL_COMPRESSED_BIT_PACKET << 4;
    let mut n = 8u64; // Current bit in current byte.

    // Encode the number of booleans written.
    let i = (src.len() as u64).encode_var(&mut dst[(n >> 3) as usize..]);
    n += (i * 8) as u64;

    for v in src {
        if *v {
            dst[(n >> 3) as usize] |= 128 >> (n & 7); // Set current bit on current byte.
        } else {
            dst[(n >> 3) as usize] &= !(128 >> (n & 7)); // Clear current bit on current byte.
        }
        n += 1;
    }

    let mut length = n >> 3;
    if n & 7 > 0 {
        length += 1; // Add an extra byte to capture overflowing bits.
    }

    dst.truncate(length as usize);

    Ok(dst)
}

pub fn bool_array_decode_all(b: &[u8]) -> Result<Vec<bool>> {
    if b.is_empty() {
        return Ok(vec![]);
    }

    // First byte stores the encoding type, only have 1 bit-packet format
    // currently ignore for now.
    let b = &b[1..];
    let (val, n) = match u64::decode_var(b) {
        Some((count, n)) => {
            if n == 0 {
                return Err(Error::StringErr {
                    reason: "bool_array_decode_all: invalid count".to_string(),
                });
            }
            (count, n)
        }
        _ => {
            return Err(Error::StringErr {
                reason: "bool_array_decode_all: invalid count".to_string(),
            });
        }
    };

    let mut count = val as usize;

    let b = &b[n..];
    let min = b.len() * 8;
    if min < count {
        // Shouldn't happen - TSM file was truncated/corrupted
        count = min;
    }

    let mut dst = vec![false; count];

    let mut j = 0;
    for v in b {
        let mut i = 128u8;
        while i > 0 && j < dst.len() {
            dst[j] = v & i != 0;
            j += 1;
            i >>= 1;
        }
    }

    Ok(dst)
}

#[cfg(test)]
mod tests {
    use rand::{Rng, RngCore, SeedableRng, rngs::StdRng};

    use super::{bool_array_decode_all, bool_array_encode_all};
    use crate::storage::tsm1::data_encoding::bool::{BoolDecoder, BoolEncoder};

    #[test]
    fn test_batch_bool_bool_array_encode_all_no_values() {
        let b = bool_array_encode_all(&[]).unwrap();

        let mut dec = BoolDecoder::new();
        dec.set_bytes(&b).unwrap();
        assert!(
            dec.next().is_none(),
            "unexpected next value: exp None, got Some"
        );
    }

    #[test]
    fn test_batch_bool_bool_array_encode_all_single() {
        let src = vec![true];

        let b = bool_array_encode_all(&src).unwrap();

        let mut dec = BoolDecoder::new();
        dec.set_bytes(&b).unwrap();
        let got = dec.next().unwrap();
        assert_eq!(src[0], got, "unexpected value: exp {}, got {}", src[0], got);
    }

    #[test]
    fn test_batch_bool_bool_array_encode_all_compare() {
        // generate random values
        let mut input = vec![];
        for i in 0..1000 {
            input.push(rand::thread_rng().gen_range(0..2) == 1);
        }

        let mut enc = BoolEncoder::new(1000);
        for v in &input {
            enc.write(*v);
        }
        enc.flush();

        let buf1 = enc.bytes().unwrap();

        let buf2 = bool_array_encode_all(&input).unwrap();

        let result = bool_array_decode_all(&buf2).unwrap();
        assert_eq!(
            input, result,
            "unexpected decode values: exp {:?}, got: {:?}",
            input, result
        );

        // Check that the encoders are byte for byte the same...
        assert_eq!(buf1, buf2, "Raw bytes differ for encoders");
    }

    #[test]
    fn test_batch_bool_bool_array_encode_all_multi_compressed() {
        // generate random values
        let mut src = vec![];
        for i in 0..10 {
            src.push(i % 2 == 0);
        }

        let mut enc = BoolEncoder::new(1000);
        for v in &src {
            enc.write(*v);
        }
        enc.flush();

        let b = enc.bytes().unwrap();

        assert_eq!(4, b.len(), "unexpected length: exp {} got {}", 4, b.len());

        let mut dec = BoolDecoder::new();
        dec.set_bytes(&b).unwrap();

        for (i, v) in src.iter().enumerate() {
            let got = dec.next().unwrap();
            assert_eq!(
                *v, got,
                "unexpected value at pos {}: exp {}, got {}",
                i, *v, got
            );
        }

        assert!(
            dec.next().is_none(),
            "unexpected next value: exp None, got Some"
        );
    }

    #[test]
    fn test_batch_bool_bool_array_encode_all_rand() {
        // generate random values
        let mut src = vec![];
        for _ in 0..50 {
            src.push(rand::thread_rng().gen_range(0..2) == 1);
        }

        let buf = bool_array_encode_all(&src).unwrap();

        let mut dec = BoolDecoder::new();
        dec.set_bytes(&buf);

        let mut got = vec![];
        for v in dec {
            got.push(v);
        }

        // Verify that input and output values match.
        assert_eq!(src, got, "mismatch: exp {:?}, got {:?}", src, got);
    }

    #[test]
    fn test_batch_bool_bool_array_decode_single() {
        let mut enc = BoolEncoder::new(1);
        let exp = true;
        enc.write(exp);

        let b = enc.bytes().unwrap();

        let got = bool_array_decode_all(&b).unwrap();
        assert!(got.len() == 1, "expeted 1 value");

        let got = got[0];
        assert_eq!(exp, got, "unexpected value: exp {}, got {}", exp, got);
    }

    #[test]
    fn test_batch_bool_bool_array_decode_multi_compressed() {
        let cases = vec![
            (10, 0.33), // (n, p) p is probability of a true value
            (100, 0.55),
            (1000, 0.68),
        ];

        for (n, p) in cases {
            let rng = StdRng::seed_from_u64(n * n);

            let mut enc = BoolEncoder::new(n as i64);
            let mut values = vec![];
            for i in 0..n {
                let v = rand::random::<f64>() < p;
                values.push(v);
                enc.write(v);
            }

            let b = enc.bytes().unwrap();

            let got = bool_array_decode_all(&b).unwrap();
            assert_eq!(values, got, "mismatch: exp {:?}, got {:?}", values, got);
        }
    }

    // TODO: implement me
    // func Test_BooleanBatchDecoder_Corrupt(t *testing.T) {
    //     cases := []struct {
    //         name string
    //         d    string
    //     }{
    //         {"empty", ""},
    //         {"invalid count", "\x10\x90"},
    //         {"count greater than remaining bits, multiple bytes expected", "\x10\x7f"},
    //         {"count greater than remaining bits, one byte expected", "\x10\x01"},
    //     }

    //     for _, c := range cases {
    //         t.Run(c.name, func(t *testing.T) {
    //             dst, _ := tsm1.BooleanArrayDecodeAll([]byte(c.d), nil)
    //             if len(dst) != 0 {
    //                 t.Fatalf("unexpected result -got/+want\n%s", cmp.Diff(dst, nil))
    //             }
    //         })
    //     }
    // }
}
