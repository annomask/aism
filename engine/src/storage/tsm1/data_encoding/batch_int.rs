use std::slice;

use bytes::{Buf, BufMut};
use integer_encoding::VarInt;
use snafu::{ResultExt, Snafu};

use crate::storage::tsm1::data_encoding::{
    encoding::{zig_zag_decode, zig_zag_encode},
    int::*,
    simple8b,
};

/// integer_array_encode_all encodes src into b, returning b and any error encountered.
/// The returned slice may be of a different length and capacity to b.
///
/// integer_array_encode_all implements batch oriented versions of the three integer
/// encoding types we support: uncompressed, simple8b and RLE.
///
/// Important: integer_array_encode_all modifies the contents of src by using it as
/// scratch space for delta encoded values. It is NOT SAFE to use src after
/// passing it into integer_array_encode_all.
pub fn integer_array_encode_all(src: &mut [i64]) -> Result<Vec<u8>> {
    if src.is_empty() {
        return Ok(vec![]); // Nothing to do
    }

    let mut max = 0u64;

    // To prevent an allocation of the entire block we're encoding reuse the
    // src slice to store the encoded deltas.
    let deltas = reinterpret_int64_to_uint64_slice(src);
    for i in (1..deltas.len()).rev() {
        deltas[i] = deltas[i].wrapping_sub(deltas[i - 1]);
        deltas[i] = zig_zag_encode(deltas[i] as i64);
        if deltas[i] > max {
            max = deltas[i];
        }
    }

    deltas[0] = zig_zag_encode(deltas[0] as i64);

    if deltas.len() > 2 {
        let mut rle = true;
        for i in 2..deltas.len() {
            if deltas[1] != deltas[i] {
                rle = false;
                break;
            }
        }

        if rle {
            // Large varints can take up to 10 bytes.  We're storing 3 + 1 type byte.
            // if dst.len() < 31 && dst.capacity() >= 31 {
            // 	b = b[:31]
            // } else if b.len() < 31 {
            // 	b = append(b, make([]byte, 31-b.len())...)
            // }

            let mut dst = vec![0u8; 31];
            let mut pos = 0;

            // 4 high bits used for the encoding type
            dst[0] = INT_COMPRESSED_RLE << 4;
            pos += 1;

            // The first value
            (&mut dst[pos..pos + 8]).put_u64(deltas[0]);
            pos += 8;
            // The first delta
            pos += deltas[1].encode_var(&mut dst[pos..]);

            // The number of times the delta is repeated
            pos += ((deltas.len() - 1) as u64).encode_var(&mut dst[pos..]);

            dst.truncate(pos);
            return Ok(dst);
        }
    }

    if max > simple8b::MAX_VALUE {
        // There is an encoded value that's too big to simple8b encode.
        // Encode uncompressed.
        let sz = 1 + deltas.len() * 8;
        let mut dst = vec![0u8; sz];

        // 4 high bits of first byte store the encoding type for the block
        dst[0] = INT_UNCOMPRESSED << 4;
        for (i, v) in deltas.iter().enumerate() {
            (&mut dst[1 + i * 8..1 + i * 8 + 8]).put_u64(*v);
        }
        dst.truncate(sz);
        return Ok(dst);
    }

    // Encode with simple8b - fist value is written unencoded using 8 bytes.
    let encoded_count = simple8b::encode_all(&mut deltas[1..]).context(Simple8bSnafu)? as usize;

    let sz = 1 + (encoded_count + 1) * 8;
    let mut dst = vec![0u8; sz];

    // 4 high bits of first byte store the encoding type for the block
    dst[0] = INT_COMPRESSED_SIMPLE << 4;

    // Write the first value since it's not part of the encoded values
    (&mut dst[1..9]).put_u64(deltas[0]);

    // Write the encoded values
    for i in 0..encoded_count {
        (&mut dst[9 + i * 8..9 + i * 8 + 8]).put_u64(deltas[1 + i]);
    }
    dst.truncate(sz);
    Ok(dst)
}

// unsigned_array_encode_all encodes src into b, returning b and any error encountered.
//
// unsigned_array_encode_all implements batch oriented versions of the three integer
// encoding types we support: uncompressed, simple8b and RLE.
//
// Important: integer_array_encode_all modifies the contents of src by using it as
// scratch space for delta encoded values. It is NOT SAFE to use src after
// passing it into integer_array_encode_all.
pub fn unsigned_array_encode_all(src: &mut [u64]) -> Result<Vec<u8>> {
    let srcint = reinterpret_uint64_to_int64_slice(src);
    integer_array_encode_all(srcint)
}

type IntBatchDecodeFn = fn(&[u8], &mut Vec<i64>) -> Result<()>;

const INT_BATCH_DECODER_FUNC: [IntBatchDecodeFn; 4] = [
    integer_batch_decode_all_uncompressed,
    integer_batch_decode_all_simple,
    integer_batch_decode_all_rle,
    integer_batch_decode_all_invalid,
];

pub fn integer_array_decode_all(b: &[u8]) -> Result<Vec<i64>> {
    let mut dst = Vec::new();
    integer_array_decode_all_into(b, &mut dst)?;
    Ok(dst)
}

pub(crate) fn integer_array_decode_all_into(b: &[u8], dst: &mut Vec<i64>) -> Result<()> {
    if b.is_empty() {
        dst.clear();
        return Ok(());
    }

    let mut encoding = b[0] >> 4;
    if encoding > INT_COMPRESSED_RLE {
        encoding = 3; // integerBatchDecodeAllInvalid
    }

    INT_BATCH_DECODER_FUNC[(encoding & 3) as usize](b, dst)
}

pub fn unsigned_array_decode_all(b: &[u8]) -> Result<Vec<u64>> {
    let mut dst = Vec::new();
    unsigned_array_decode_all_into(b, &mut dst)?;
    Ok(dst)
}

pub(crate) fn unsigned_array_decode_all_into(b: &[u8], dst: &mut Vec<u64>) -> Result<()> {
    let values = integer_array_decode_all(b)?;
    dst.clear();
    dst.extend(values.into_iter().map(|value| value as u64));
    Ok(())
}

fn integer_batch_decode_all_uncompressed(b: &[u8], dst: &mut Vec<i64>) -> Result<()> {
    let b = &b[1..];
    if b.len() & 0x7 != 0 {
        return Err(Error::StringErr {
            reason: "integer_array_decode_all: expected multiple of 8 bytes".to_string(),
        });
    }

    let count = b.len() / 8;
    dst.resize(count, 0);

    let mut prev = 0i64;
    for (i, v) in dst.iter_mut().enumerate() {
        prev = prev.wrapping_add(zig_zag_decode((&b[i * 8..i * 8 + 8]).get_u64()));
        *v = prev;
    }

    Ok(())
}

fn integer_batch_decode_all_simple(b: &[u8], dst: &mut Vec<i64>) -> Result<()> {
    let b = &b[1..];
    if b.len() < 8 {
        return Err(Error::StringErr {
            reason: "integer_array_decode_all: not enough data to decode packed value".to_string(),
        });
    }

    let mut count = simple8b::count_bytes(&b[8..]).context(Simple8bSnafu)? as usize;
    count += 1;

    dst.resize(count, 0);

    // first value
    dst[0] = zig_zag_decode((&b[..8]).get_u64());

    // decode compressed values
    let buf = reinterpret_int64_to_uint64_slice(dst);
    let n = simple8b::decode_bytes(&mut buf[1..], &b[8..]).context(Simple8bSnafu)?;
    if n != count - 1 {
        return Err(Error::StringErr {
            reason: format!(
                "integer_array_decode_all: unexpected number of values decoded; got={}, exp={}",
                n,
                count - 1
            ),
        });
    }

    // Go int64 reconstruction is modulo 2^64 when a delta crosses the signed boundary.
    let mut prev = dst[0];
    for v in &mut dst[1..] {
        prev = prev.wrapping_add(zig_zag_decode(*v as u64));
        *v = prev;
    }

    Ok(())
}

fn integer_batch_decode_all_rle(b: &[u8], dst: &mut Vec<i64>) -> Result<()> {
    let b = &b[1..];
    if b.len() < 8 {
        return Err(Error::StringErr {
            reason: "integer_array_decode_all: not enough data to decode RLE starting value"
                .to_string(),
        });
    }

    let mut k = 0;

    // Next 8 bytes is the starting value
    let first = zig_zag_decode((&b[k..k + 8]).get_u64());
    k += 8;

    // Next 1-10 bytes is the delta value
    let (value, n) = match u64::decode_var(&b[k..]) {
        Some((sz, n)) => (sz, n),
        None => {
            return Err(Error::StringErr {
                reason: "integer_array_decode_all: decode_var failed".to_string(),
            });
        }
    };
    if n == 0 {
        // n is usize
        return Err(Error::StringErr {
            reason: "integer_array_decode_all: invalid RLE delta value".to_string(),
        });
    }
    k += n;

    let delta = zig_zag_decode(value);

    // Last 1-10 bytes is how many times the value repeats
    let (mut count, n) = match u64::decode_var(&b[k..]) {
        Some((sz, n)) => (sz, n),
        None => {
            return Err(Error::StringErr {
                reason: "integer_array_decode_all: decode_var failed".to_string(),
            });
        }
    };
    if n == 0 {
        // n is usize
        return Err(Error::StringErr {
            reason: "integer_array_decode_all: invalid RLE repeat value".to_string(),
        });
    }

    count += 1;
    dst.resize(count as usize, 0);

    if delta == 0 {
        for v in dst.iter_mut() {
            *v = first;
        }
    } else {
        let mut acc = first;
        for v in dst.iter_mut() {
            *v = acc;
            // RLE deltas retain InfluxDB's modulo-2^64 int64 semantics.
            acc = acc.wrapping_add(delta);
        }
    }

    Ok(())
}

fn integer_batch_decode_all_invalid(b: &[u8], _dst: &mut Vec<i64>) -> Result<()> {
    Err(Error::StringErr {
        reason: format!("unknown encoding {}", b[0] >> 4),
    })
}

pub fn reinterpret_int64_to_uint64_slice(src: &mut [i64]) -> &mut [u64] {
    unsafe { slice::from_raw_parts_mut(src.as_ptr() as *mut u64, src.len()) }
}

pub fn reinterpret_uint64_to_int64_slice(src: &mut [u64]) -> &mut [i64] {
    unsafe { slice::from_raw_parts_mut(src.as_ptr() as *mut i64, src.len()) }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::{
        INT_COMPRESSED_RLE, INT_COMPRESSED_SIMPLE, INT_UNCOMPRESSED, IntegerDecoder,
        IntegerEncoder, integer_array_decode_all, integer_array_encode_all,
        unsigned_array_decode_all, unsigned_array_encode_all,
    };

    #[test]
    fn test_batch_int_int_array_encode_all_no_values() {
        let b = integer_array_encode_all(&mut []).unwrap();

        assert_eq!(0, b.len(), "unexpected length, exp 0, got {}", b.len());

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);
        assert!(
            dec.next().is_none(),
            "unexpected next value: exp None, got Some"
        );
    }

    #[test]
    fn test_batch_int_int_array_encode_all_compare() {
        // generate random values (should use simple8b)
        let mut input = vec![];
        for _ in 0..1000 {
            input.push(rand::thread_rng().gen_range(0..100000) - 50000);
        }
        input.sort();
        test_int_array_encode_all_compare(&mut input, INT_COMPRESSED_SIMPLE);

        // Generate same values (should use RLE)
        for i in 0..1000 {
            input[i] = 1232342341234;
        }
        test_int_array_encode_all_compare(&mut input, INT_COMPRESSED_RLE);

        // Generate large random values that are not sorted. The deltas will be large
        // and the values should be stored uncompressed.
        for i in 0..1000 {
            input[i] = rand::random::<u64>() as i64;
        }
        test_int_array_encode_all_compare(&mut input, INT_UNCOMPRESSED);
    }

    fn test_int_array_encode_all_compare(input: &mut [i64], encoding: u8) {
        let exp = Vec::from(&mut *input);

        let mut enc = IntegerEncoder::new(1000);
        for v in &mut *input {
            enc.write(*v);
        }

        let buf1 = enc.bytes().unwrap();
        let got = buf1[0] >> 4;
        assert_eq!(
            encoding, got,
            "unexpected encoding, exp {}, got {}",
            encoding, got
        );

        let buf2 = integer_array_encode_all(input).unwrap();
        let got = buf2[0] >> 4;
        assert_eq!(
            encoding, got,
            "unexpected encoding, exp {}, got {}",
            encoding, got
        );

        let result = integer_array_decode_all(&buf2).unwrap();

        assert_eq!(
            exp, result,
            "unexpected result, exp: {:?}
got: {:?}",
            exp, result
        );

        // Check that the encoders are byte for byte the same...
        assert_eq!(buf1, buf2, "Raw bytes differ for encoders");
    }

    #[test]
    fn test_batch_unsigned_unsigned_array_encode_all_compare() {
        // generate random values (should use simple8b)
        let mut input = vec![];
        for _ in 0..1000 {
            input.push(rand::thread_rng().gen_range(0..100000));
        }
        input.sort();
        test_unsigned_array_encode_all_compare(&mut input, INT_COMPRESSED_SIMPLE);

        // Generate same values (should use RLE)
        for i in 0..1000 {
            input[i] = 1232342341234u64;
        }
        test_unsigned_array_encode_all_compare(&mut input, INT_COMPRESSED_RLE);

        // Generate large random values that are not sorted. The deltas will be large
        // and the values should be stored uncompressed.
        for i in 0..1000 {
            input[i] = rand::random::<u64>();
        }
        test_unsigned_array_encode_all_compare(&mut input, INT_UNCOMPRESSED);
    }

    fn test_unsigned_array_encode_all_compare(input: &mut [u64], encoding: u8) {
        let exp = Vec::from(&mut *input);

        let mut enc = IntegerEncoder::new(1000);
        for v in &mut *input {
            enc.write(*v as i64);
        }

        let buf1 = enc.bytes().unwrap();
        let got = buf1[0] >> 4;
        assert_eq!(
            encoding, got,
            "unexpected encoding, exp {}, got {}",
            encoding, got
        );

        let buf2 = unsigned_array_encode_all(input).unwrap();
        let got = buf2[0] >> 4;
        assert_eq!(
            encoding, got,
            "unexpected encoding, exp {}, got {}",
            encoding, got
        );

        let result = unsigned_array_decode_all(&buf2).unwrap();

        assert_eq!(
            exp, result,
            "unexpected result, exp: {:?}
got: {:?}",
            exp, result
        );

        // Check that the encoders are byte for byte the same...
        assert_eq!(buf1, buf2, "Raw bytes differ for encoders");
    }

    #[test]
    fn test_batch_int_int_array_encode_all_one() {
        let v1 = 1i64;

        let mut src = vec![1i64];
        let b = integer_array_encode_all(&mut src).unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            INT_COMPRESSED_SIMPLE, got,
            "unexpected encoding, exp {}, got {}",
            INT_COMPRESSED_SIMPLE, got
        );

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);
        let got = dec.next().unwrap();
        assert_eq!(v1, got, "read value mismatch: got {}, exp {}", v1, got);
    }

    #[test]
    fn test_batch_int_int_array_encode_all_two() {
        let (v1, v2) = (1i64, 2i64);

        let mut src = vec![v1, v2];
        let b = integer_array_encode_all(&mut src).unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            INT_COMPRESSED_SIMPLE, got,
            "unexpected encoding, exp {}, got {}",
            INT_COMPRESSED_SIMPLE, got
        );

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);

        let got = dec.next().unwrap();
        assert_eq!(v1, got, "read value mismatch: got {}, exp {}", v1, got);

        let got = dec.next().unwrap();
        assert_eq!(v2, got, "read value mismatch: got {}, exp {}", v2, got);
    }

    #[test]
    fn test_batch_int_int_array_encode_all_negative() {
        let (v1, v2, v3) = (-2i64, 0i64, 1i64);

        let mut src = vec![v1, v2, v3];
        let b = integer_array_encode_all(&mut src).unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            INT_COMPRESSED_SIMPLE, got,
            "unexpected encoding, exp {}, got {}",
            INT_COMPRESSED_SIMPLE, got
        );

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);
        let got = dec.next().unwrap();
        assert_eq!(v1, got, "read value mismatch: exp {}, got {}", v1, got);

        let got = dec.next().unwrap();
        assert_eq!(v2, got, "read value mismatch: exp {}, got {}", v2, got);

        let got = dec.next().unwrap();
        assert_eq!(v3, got, "read value mismatch: exp {}, got {}", v3, got);
    }

    #[test]
    fn test_batch_int_int_array_encode_all_large_range() {
        let (v1, v2, v3) = (-i64::MAX, 0i64, i64::MIN);

        let mut src = vec![v1, v2, v3];
        let b = integer_array_encode_all(&mut src).unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            INT_UNCOMPRESSED, got,
            "unexpected encoding, exp {}, got {}",
            INT_UNCOMPRESSED, got
        );

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);

        let exp = vec![v1, v2, v3];
        let mut got = vec![];
        for v in dec {
            got.push(v);
        }

        assert_eq!(
            exp, got,
            "read value mismatch: exp {:?}, got {:?}",
            exp, got
        );
    }

    #[test]
    fn test_batch_int_int_array_encode_all_uncompressed() {
        let (v1, v2, v3) = (0i64, 1i64, 1i64 << 60);

        let mut src = vec![v1, v2, v3];
        let b = integer_array_encode_all(&mut src).unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            INT_UNCOMPRESSED, got,
            "unexpected encoding, exp {}, got {}",
            INT_UNCOMPRESSED, got
        );

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);

        let exp = vec![v1, v2, v3];
        let mut got = vec![];
        for v in dec {
            got.push(v);
        }

        assert_eq!(
            exp, got,
            "read value mismatch: exp {:?}, got {:?}",
            exp, got
        );
    }

    #[test]
    fn test_batch_int_int_array_encode_all_negative_uncompressed() {
        let mut src: Vec<i64> = vec![
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
        let exp = src.clone();

        let b = integer_array_encode_all(&mut src).unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            INT_UNCOMPRESSED, got,
            "unexpected encoding, exp {}, got {}",
            INT_UNCOMPRESSED, got
        );

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);

        let mut i = 0;
        for v in dec {
            if i > src.len() {
                panic!("read too many values: exp {}, got {}", src.len(), i);
            }
            assert_eq!(
                exp[i], v,
                "read value {} mismatch: exp {:?}, got {:?}",
                i, exp, got
            );
            i += 1;
        }
    }

    #[test]
    fn test_batch_int_int_array_encode_all_all_negative() {
        let mut src: Vec<i64> = vec![-10, -5, -1];
        let exp = src.clone();

        let b = integer_array_encode_all(&mut src).unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            INT_COMPRESSED_SIMPLE, got,
            "unexpected encoding, exp {}, got {}",
            INT_COMPRESSED_SIMPLE, got
        );

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);

        let mut i = 0;
        for v in dec {
            if i > src.len() {
                panic!("read too many values: exp {}, got {}", src.len(), i);
            }
            assert_eq!(
                exp[i], v,
                "read value {} mismatch: exp {:?}, got {:?}",
                i, exp, got
            );
            i += 1;
        }
    }

    #[test]
    fn test_batch_int_int_array_encode_all_counter_packed() {
        let mut src: Vec<i64> = vec![
            1e15 as i64,
            1e15 as i64 + 1,
            1e15 as i64 + 2,
            1e15 as i64 + 3,
            1e15 as i64 + 4,
            1e15 as i64 + 6,
        ];
        let exp = src.clone();

        let b = integer_array_encode_all(&mut src).unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            INT_COMPRESSED_SIMPLE, got,
            "unexpected encoding, exp {}, got {}",
            INT_COMPRESSED_SIMPLE, got
        );

        // Should use 1 header byte + 2, 8 byte words if delta-encoding is used based on
        // values sizes.  Without delta-encoding, we'd get 49 bytes.
        assert_eq!(
            17,
            b.len(),
            "encoded length mismatch: exp {}, got {}",
            17,
            b.len()
        );

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);

        let mut i = 0;
        for v in dec {
            if i > src.len() {
                panic!("read too many values: exp {}, got {}", src.len(), i);
            }
            assert_eq!(
                exp[i], v,
                "read value {} mismatch: exp {:?}, got {:?}",
                i, exp, got
            );
            i += 1;
        }
    }

    #[test]
    fn test_batch_int_int_array_encode_all_counter_rle() {
        let mut src: Vec<i64> = vec![
            1e15 as i64,
            1e15 as i64 + 1,
            1e15 as i64 + 2,
            1e15 as i64 + 3,
            1e15 as i64 + 4,
            1e15 as i64 + 5,
        ];
        let exp = src.clone();

        let b = integer_array_encode_all(&mut src).unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            INT_COMPRESSED_RLE, got,
            "unexpected encoding, exp {}, got {}",
            INT_COMPRESSED_RLE, got
        );

        // Should use 1 header byte, 8 byte first value, 1 var-byte for delta and 1 var-byte for
        // count of deltas in this particular RLE.
        assert_eq!(
            11,
            b.len(),
            "encoded length mismatch: exp {}, got {}",
            11,
            b.len()
        );

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);

        let mut i = 0;
        for v in dec {
            if i > src.len() {
                panic!("read too many values: exp {}, got {}", src.len(), i);
            }
            assert_eq!(
                exp[i], v,
                "read value {} mismatch: exp {:?}, got {:?}",
                i, exp, got
            );
            i += 1;
        }
    }

    #[test]
    fn test_batch_int_int_array_encode_all_descending() {
        let mut src: Vec<i64> = vec![7094, 4472, 1850];
        let exp = src.clone();

        let b = integer_array_encode_all(&mut src).unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            INT_COMPRESSED_RLE, got,
            "unexpected encoding, exp {}, got {}",
            INT_COMPRESSED_RLE, got
        );

        // Should use 1 header byte, 8 byte first value, 1 var-byte for delta and 1 var-byte for
        // count of deltas in this particular RLE.
        assert_eq!(
            12,
            b.len(),
            "encoded length mismatch: exp {}, got {}",
            12,
            b.len()
        );

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);

        let mut i = 0;
        for v in dec {
            if i > src.len() {
                panic!("read too many values: exp {}, got {}", src.len(), i);
            }
            assert_eq!(
                exp[i], v,
                "read value {} mismatch: exp {:?}, got {:?}",
                i, exp, got
            );
            i += 1;
        }
    }

    #[test]
    fn test_batch_int_int_array_encode_all_flat() {
        let mut src: Vec<i64> = vec![1, 1, 1, 1];
        let exp = src.clone();

        let b = integer_array_encode_all(&mut src).unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            INT_COMPRESSED_RLE, got,
            "unexpected encoding, exp {}, got {}",
            INT_COMPRESSED_RLE, got
        );

        // Should use 1 header byte, 8 byte first value, 1 var-byte for delta and 1 var-byte for
        // count of deltas in this particular RLE.
        assert_eq!(
            11,
            b.len(),
            "encoded length mismatch: exp {}, got {}",
            11,
            b.len()
        );

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);

        let mut i = 0;
        for v in dec {
            if i > src.len() {
                panic!("read too many values: exp {}, got {}", src.len(), i);
            }
            assert_eq!(
                exp[i], v,
                "read value {} mismatch: exp {:?}, got {:?}",
                i, exp, got
            );
            i += 1;
        }
    }

    #[test]
    fn test_batch_int_int_array_encode_all_min_max() {
        let mut src: Vec<i64> = vec![i64::MIN, i64::MAX];
        let exp = src.clone();

        let b = integer_array_encode_all(&mut src).unwrap();
        let got = b[0] >> 4;
        assert_eq!(
            INT_COMPRESSED_SIMPLE, got,
            "unexpected encoding, exp {}, got {}",
            INT_COMPRESSED_SIMPLE, got
        );

        assert_eq!(
            17,
            b.len(),
            "encoded length mismatch: exp {}, got {}",
            17,
            b.len()
        );

        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);

        let mut i = 0;
        for v in dec {
            if i > src.len() {
                panic!("read too many values: exp {}, got {}", src.len(), i);
            }
            assert_eq!(
                exp[i], v,
                "read value {} mismatch: exp {:?}, got {:?}",
                i, exp, got
            );
            i += 1;
        }
    }

    #[test]
    fn test_batch_int_int_array_encode_all_rand() {
        let mut src = vec![];
        for i in 0..50 {
            src.push(rand::random::<i64>());
        }
        let exp = src.clone();

        // Retrieve encoded bytes from encoder.
        let b = integer_array_encode_all(&mut src).unwrap();

        // Read values out of decoder.
        let mut dec = IntegerDecoder::new();
        dec.set_bytes(&b);

        let mut got = vec![];
        for v in dec {
            got.push(v);
        }

        assert_eq!(
            exp, got,
            "mismatch, exp: {:?}
got: {:?}",
            exp, got
        );
    }

    #[test]
    fn test_batch_int_int_array_decode_all_negative_uncompressed() {
        let src: Vec<i64> = vec![
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
        for v in &src {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            INT_UNCOMPRESSED, got,
            "unexpected encoding, exp {}, got {}",
            INT_UNCOMPRESSED, got
        );

        let got = integer_array_decode_all(&b).unwrap();

        assert_eq!(
            src, got,
            "mismatch, exp: {:?}
got: {:?}",
            src, got
        );
    }

    #[test]
    fn test_batch_int_int_array_decode_all_all_negative() {
        let src: Vec<i64> = vec![-10, -5, -1];

        let mut enc = IntegerEncoder::new(3);
        for v in &src {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            INT_COMPRESSED_SIMPLE, got,
            "unexpected encoding, exp {}, got {}",
            INT_COMPRESSED_SIMPLE, got
        );

        let got = integer_array_decode_all(&b).unwrap();

        assert_eq!(
            src, got,
            "mismatch, exp: {:?}
got: {:?}",
            src, got
        );
    }

    #[test]
    fn test_batch_int_int_array_decode_all_counter_packed() {
        let src: Vec<i64> = vec![
            1e15 as i64,
            1e15 as i64 + 1,
            1e15 as i64 + 2,
            1e15 as i64 + 3,
            1e15 as i64 + 4,
            1e15 as i64 + 6,
        ];

        let mut enc = IntegerEncoder::new(16);
        for v in &src {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            INT_COMPRESSED_SIMPLE, got,
            "unexpected encoding, exp {}, got {}",
            INT_COMPRESSED_SIMPLE, got
        );

        // Should use 1 header byte + 2, 8 byte words if delta-encoding is used based on
        // values sizes.  Without delta-encoding, we'd get 49 bytes.
        assert_eq!(
            17,
            b.len(),
            "encoded length mismatch: exp {}, got {}",
            17,
            b.len()
        );

        let got = integer_array_decode_all(&b).unwrap();

        assert_eq!(
            src, got,
            "mismatch, exp: {:?}
got: {:?}",
            src, got
        );
    }

    #[test]
    fn test_batch_int_int_array_decode_all_counter_rle() {
        let src: Vec<i64> = vec![
            1e15 as i64,
            1e15 as i64 + 1,
            1e15 as i64 + 2,
            1e15 as i64 + 3,
            1e15 as i64 + 4,
            1e15 as i64 + 5,
        ];

        let mut enc = IntegerEncoder::new(16);
        for v in &src {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            INT_COMPRESSED_RLE, got,
            "unexpected encoding, exp {}, got {}",
            INT_COMPRESSED_RLE, got
        );

        // Should use 1 header byte, 8 byte first value, 1 var-byte for delta and 1 var-byte for
        // count of deltas in this particular RLE.
        assert_eq!(
            11,
            b.len(),
            "encoded length mismatch: exp {}, got {}",
            11,
            b.len()
        );

        let got = integer_array_decode_all(&b).unwrap();

        assert_eq!(
            src, got,
            "mismatch, exp: {:?}
got: {:?}",
            src, got
        );
    }

    #[test]
    fn test_batch_int_int_array_decode_all_descending() {
        let src: Vec<i64> = vec![7094, 4472, 1850];

        let mut enc = IntegerEncoder::new(16);
        for v in &src {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            INT_COMPRESSED_RLE, got,
            "unexpected encoding, exp {}, got {}",
            INT_COMPRESSED_RLE, got
        );

        // Should use 1 header byte, 8 byte first value, 1 var-byte for delta and 1 var-byte for
        // count of deltas in this particular RLE.
        assert_eq!(
            12,
            b.len(),
            "encoded length mismatch: exp {}, got {}",
            12,
            b.len()
        );

        let got = integer_array_decode_all(&b).unwrap();

        assert_eq!(
            src, got,
            "mismatch, exp: {:?}
got: {:?}",
            src, got
        );
    }

    #[test]
    fn test_batch_int_int_array_decode_all_flat() {
        let src: Vec<i64> = vec![1, 1, 1, 1];

        let mut enc = IntegerEncoder::new(16);
        for v in &src {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            INT_COMPRESSED_RLE, got,
            "unexpected encoding, exp {}, got {}",
            INT_COMPRESSED_RLE, got
        );

        // Should use 1 header byte, 8 byte first value, 1 var-byte for delta and 1 var-byte for
        // count of deltas in this particular RLE.
        assert_eq!(
            11,
            b.len(),
            "encoded length mismatch: exp {}, got {}",
            11,
            b.len()
        );

        let got = integer_array_decode_all(&b).unwrap();

        assert_eq!(
            src, got,
            "mismatch, exp: {:?}
got: {:?}",
            src, got
        );
    }

    #[test]
    fn test_batch_int_int_array_decode_all_min_max() {
        let src: Vec<i64> = vec![i64::MIN, i64::MAX];

        let mut enc = IntegerEncoder::new(2);
        for v in &src {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            INT_UNCOMPRESSED, got,
            "unexpected encoding, exp {}, got {}",
            INT_UNCOMPRESSED, got
        );

        assert_eq!(
            17,
            b.len(),
            "encoded length mismatch: exp {}, got {}",
            17,
            b.len()
        );

        let got = integer_array_decode_all(&b).unwrap();

        assert_eq!(
            src, got,
            "mismatch, exp: {:?}
got: {:?}",
            src, got
        );
    }

    #[test]
    fn test_batch_int_int_array_decode_all_rand() {
        let mut src = vec![];
        for i in 0..50 {
            src.push(rand::random::<i64>());
        }
        let exp = src.clone();

        let mut enc = IntegerEncoder::new(1024);
        for v in &src {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        // Read values out of decoder.
        let got = integer_array_decode_all(&b).unwrap();
        assert_eq!(
            src, got,
            "mismatch, exp: {:?}
got: {:?}",
            src, got
        );
    }
}
