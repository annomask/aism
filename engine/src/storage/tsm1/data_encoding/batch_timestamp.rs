use bytes::{Buf, BufMut};
use integer_encoding::VarInt;
use snafu::ResultExt;

use super::{batch_int::reinterpret_int64_to_uint64_slice, simple8b, timestamp::*};

/// time_array_encode_all encodes src into b, returning b and any error encountered.
/// The returned slice may be of a different length and capacity to b.
///
/// time_array_encode_all implements batch oriented versions of the three integer
/// encoding types we support: uncompressed, simple8b and RLE.
///
/// Timestamp values to be encoded should be sorted before encoding.  When encoded,
/// the values are first delta-encoded.  The first value is the starting timestamp,
/// subsequent values are the difference from the prior value.
///
/// Important: time_array_encode_all modifies the contents of src by using it as
/// scratch space for delta encoded values. It is NOT SAFE to use src after
/// passing it into time_array_encode_all.
pub fn time_array_encode_all(src: &mut [i64]) -> Result<Vec<u8>> {
    if src.is_empty() {
        return Ok(vec![]); // Nothing to do
    }

    let (mut max, mut div) = (0u64, 1e12 as u64);

    // To prevent an allocation of the entire block we're encoding reuse the
    // src slice to store the encoded deltas.
    let deltas = reinterpret_int64_to_uint64_slice(src);
    if deltas.len() > 1 {
        for i in (1..deltas.len()).rev() {
            deltas[i] = deltas[i].wrapping_sub(deltas[i - 1]);
            if deltas[i] > max {
                max = deltas[i];
            }
        }

        let mut rle = true;
        for i in 2..deltas.len() {
            if deltas[1] != deltas[i] {
                rle = false;
                break;
            }
        }

        // Deltas are the same - encode with RLE
        if rle {
            // Large varints can take up to 10 bytes.  We're storing 3 + 1 type byte.
            let mut dst = vec![0u8; 31];
            let mut pos = 0;

            // 4 high bits used for the encoding type
            dst[0] = TIME_COMPRESSED_RLE << 4;
            pos += 1;

            // The first value
            (&mut dst[pos..pos + 8]).put_u64(deltas[0]);
            pos += 8;

            // The first delta, checking the divisor
            // given all deltas are the same, we can do a single check for the divisor
            let v = deltas[1];
            while div > 1 && v % div != 0 {
                div /= 10;
            }
            if div > 1 {
                // 4 low bits are the log10 divisor
                dst[0] |= (div as f64).log10() as u8;
                pos += (deltas[1] / div).encode_var(&mut dst[pos..]);
            } else {
                pos += deltas[1].encode_var(&mut dst[pos..]);
            }

            // The number of times the delta is repeated
            pos += (deltas.len() as u64).encode_var(&mut dst[pos..]);

            dst.truncate(pos);
            return Ok(dst);
        }
    }

    // We can't compress this time-range, the deltas exceed 1 << 60
    if max > simple8b::MAX_VALUE {
        // There is an encoded value that's too big to simple8b encode.
        // Encode uncompressed.
        let sz = 1 + deltas.len() * 8;
        let mut dst = vec![0u8; sz];

        // 4 high bits of first byte store the encoding type for the block
        dst[0] = TIME_UNCOMPRESSED << 4;
        for (i, v) in deltas.iter().enumerate() {
            (&mut dst[1 + i * 8..1 + i * 8 + 8]).put_u64(*v);
        }
        dst.truncate(sz);
        return Ok(dst);
    }

    // find divisor only if we're compressing with simple8b
    let mut i = 1;
    while i < deltas.len() && div > 1 {
        // If our value is divisible by 10, break.  Otherwise, try the next smallest divisor.
        let v = deltas[i];
        while div > 1 && v % div != 0 {
            div /= 10;
        }
        i += 1;
    }

    // Only apply the divisor if it's greater than 1 since division is expensive.
    if div > 1 {
        for v in deltas.iter_mut().skip(1) {
            *v /= div;
        }
    }

    // Encode with simple8b - fist value is written unencoded using 8 bytes.
    let encoded_count = simple8b::encode_all(&mut deltas[1..]).context(Simple8bSnafu)? as usize;

    let sz = 1 + (encoded_count + 1) * 8;
    let mut dst = vec![0u8; sz];

    // 4 high bits of first byte store the encoding type for the block
    dst[0] = TIME_COMPRESSED_PACKED_SIMPLE << 4;
    dst[0] |= (div as f64).log10() as u8;

    // Write the first value since it's not part of the encoded values
    (&mut dst[1..9]).put_u64(deltas[0]);

    // Write the encoded values
    for i in 0..encoded_count {
        (&mut dst[9 + i * 8..9 + i * 8 + 8]).put_u64(deltas[1 + i]);
    }
    dst.truncate(sz);

    Ok(dst)
}

type TimeBatchDecodeFn = fn(&[u8], &mut Vec<i64>) -> Result<()>;

const TIME_BATCH_DECODER_FUNC: [TimeBatchDecodeFn; 4] = [
    time_batch_decode_all_uncompressed,
    time_batch_decode_all_simple,
    time_batch_decode_all_rle,
    time_batch_decode_all_invalid,
];

pub fn time_array_decode_all(b: &[u8]) -> Result<Vec<i64>> {
    let mut dst = Vec::new();
    time_array_decode_all_into(b, &mut dst)?;
    Ok(dst)
}

pub(crate) fn time_array_decode_all_into(b: &[u8], dst: &mut Vec<i64>) -> Result<()> {
    if b.is_empty() {
        dst.clear();
        return Ok(());
    }

    let mut encoding = b[0] >> 4;
    if encoding > TIME_COMPRESSED_RLE {
        encoding = 3; // time_batch_decode_all_invalid
    }

    TIME_BATCH_DECODER_FUNC[(encoding & 3) as usize](b, dst)
}

fn time_batch_decode_all_uncompressed(b: &[u8], dst: &mut Vec<i64>) -> Result<()> {
    let b = &b[1..];
    if b.len() & 0x7 != 0 {
        return Err(Error::StringErr {
            reason: "time_array_decode_all: expected multiple of 8 bytes".to_string(),
        });
    }

    let count = b.len() / 8;
    dst.resize(count, 0);

    let mut prev = 0u64;
    for (i, v) in dst.iter_mut().enumerate() {
        prev = prev.wrapping_add((&b[i * 8..i * 8 + 8]).get_u64());
        *v = prev as i64;
    }

    Ok(())
}

fn time_batch_decode_all_simple(b: &[u8], dst: &mut Vec<i64>) -> Result<()> {
    if b.len() < 9 {
        return Err(Error::StringErr {
            reason: "time_array_decode_all: not enough data to decode packed timestamps"
                .to_string(),
        });
    }

    let div = 10u64.pow((b[0] & 0xF) as u32); // multiplier

    let mut count = simple8b::count_bytes(&b[9..]).context(Simple8bSnafu)? as usize;
    count += 1;

    dst.resize(count, 0);

    // decode compressed values
    let buf = reinterpret_int64_to_uint64_slice(dst);
    buf[0] = (&b[1..9]).get_u64();
    let n = simple8b::decode_bytes(&mut buf[1..], &b[9..]).context(Simple8bSnafu)?;
    if n != count - 1 {
        return Err(Error::StringErr {
            reason: format!(
                "time_array_decode_all: unexpected number of values decoded; got={}, exp={}",
                n,
                count - 1
            ),
        });
    }

    // Compute the prefix sum and scale the deltas back up
    let mut last = buf[0];
    if div > 1 {
        for v in buf.iter_mut().skip(1) {
            let dgap = *v * div;
            *v = last.wrapping_add(dgap);
            last = *v;
        }
    } else {
        for v in buf.iter_mut().skip(1) {
            *v = (*v).wrapping_add(last);
            last = *v;
        }
    }

    Ok(())
}

fn time_batch_decode_all_rle(b: &[u8], dst: &mut Vec<i64>) -> Result<()> {
    if b.len() < 9 {
        return Err(Error::StringErr {
            reason: "time_array_decode_all: not enough data to decode RLE starting value"
                .to_string(),
        });
    }

    let mut k = 0;

    // Lower 4 bits hold the 10 based exponent so we can scale the values back up
    let mod_n = 10i64.pow((b[k] & 0xF) as u32) as i64;
    k += 1;

    // Next 8 bytes is the starting timestamp
    let first = (&b[k..k + 8]).get_u64();
    k += 8;

    // Next 1-10 bytes is our (scaled down by factor of 10) run length delta
    let (mut delta, n) = match u64::decode_var(&b[k..]) {
        Some((sz, n)) => (sz, n),
        None => {
            return Err(Error::StringErr {
                reason: "time_array_decode_all: decode_var failed".to_string(),
            });
        }
    };
    if n == 0 {
        // n is usize
        return Err(Error::StringErr {
            reason: "time_array_decode_all: invalid run length in decode RLE".to_string(),
        });
    }
    k += n;

    // Scale the delta back up
    delta *= mod_n as u64;

    // Last 1-10 bytes is how many times the value repeats
    let (count, n) = match u64::decode_var(&b[k..]) {
        Some((sz, n)) => (sz, n),
        None => {
            return Err(Error::StringErr {
                reason: "time_array_decode_all: decode_var failed".to_string(),
            });
        }
    };
    if n == 0 {
        // n is usize
        return Err(Error::StringErr {
            reason: "time_array_decode_all: invalid run length in decode RLE".to_string(),
        });
    }

    dst.resize(count as usize, 0);
    let mut acc = first;
    for v in dst.iter_mut() {
        *v = acc as i64;
        // Timestamp deltas are uint64 and wrap exactly as in InfluxDB.
        acc = acc.wrapping_add(delta);
    }

    Ok(())
}

fn time_batch_decode_all_invalid(b: &[u8], _dst: &mut Vec<i64>) -> Result<()> {
    Err(Error::StringErr {
        reason: format!("unknown encoding {}", b[0] >> 4),
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::common::time::Time;
    use rand::Rng;

    use super::{TIME_COMPRESSED_PACKED_SIMPLE, TIME_UNCOMPRESSED, time_array_encode_all};
    use crate::storage::tsm1::data_encoding::batch_timestamp::{
        TIME_COMPRESSED_RLE, TimeDecoder, TimeEncoder, count_timestamps, time_array_decode_all,
    };

    #[test]
    fn test_batch_timestamp_time_array_encode_all() {
        let now = Time::now();
        let mut src = vec![now.timestamp_nanos()];

        for i in 1..4 {
            src.push(
                now.checked_add(Duration::from_secs(i))
                    .unwrap()
                    .timestamp_nanos(),
            );
        }

        let exp = src.clone();

        let b = time_array_encode_all(&mut src).unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            TIME_COMPRESSED_RLE, got,
            "wrong encoding used: expected rle, got {}",
            got
        );

        let mut dec = TimeDecoder::new();
        dec.init(&b);

        for (i, v) in exp.iter().enumerate() {
            let got = dec.next().unwrap();
            assert_eq!(*v, got, "Item {} mismatch, exp {}, got {}", i, *v, got);
        }
    }

    // This test compares the ArrayEncoder to the original iterator encoder, byte for byte.
    #[test]
    fn test_batch_timestamp_time_array_encode_all_compare() {
        // generate random values (should use simple8b)
        let mut input = vec![];
        for i in 0..1000 {
            input.push(rand::thread_rng().gen_range(0..100000) - 50000)
        }
        input.sort();

        test_time_array_encode_all_compare(&mut input, TIME_COMPRESSED_PACKED_SIMPLE);

        // Generate same values (should use RLE)
        for i in 0..1000 {
            input[i] = 1232342341234;
        }
        test_time_array_encode_all_compare(&mut input, TIME_COMPRESSED_RLE);

        // Generate large random values that are not sorted. The deltas will be large
        // and the values should be stored uncompressed.
        for i in 0..1000 {
            input[i] = rand::random::<u64>() as i64;
        }
        test_time_array_encode_all_compare(&mut input, TIME_UNCOMPRESSED);
    }

    fn test_time_array_encode_all_compare(input: &mut [i64], encoding: u8) {
        let exp = Vec::from(&mut *input);

        let mut enc = TimeEncoder::new(1000);
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

        let buf2 = time_array_encode_all(input).unwrap();
        let got = buf2[0] >> 4;
        assert_eq!(
            encoding, got,
            "unexpected encoding, exp {}, got {}",
            encoding, got
        );

        let result = time_array_decode_all(&buf2).unwrap();

        assert_eq!(
            exp, result,
            "unexpected result, exp: {:?}\ngot: {:?}",
            exp, result
        );

        // Check that the encoders are byte for byte the same...
        assert_eq!(buf1, buf2, "Raw bytes differ for encoders");
    }

    #[test]
    fn test_batch_timestamp_time_array_encode_all_no_values() {
        let b = time_array_encode_all(&mut []).unwrap();

        let mut dec = TimeDecoder::new();
        dec.init(&b);
        assert!(
            dec.next().is_none(),
            "unexpected next value: exp None, got Some"
        );
    }

    #[test]
    fn test_batch_timestamp_time_array_encode_all_one() {
        let mut src = vec![0i64];
        let exp = src.clone();

        let b = time_array_encode_all(&mut src).unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            TIME_COMPRESSED_PACKED_SIMPLE, got,
            "unexpected encoding, exp {}, got {}",
            TIME_COMPRESSED_PACKED_SIMPLE, got
        );

        let mut dec = TimeDecoder::new();
        dec.init(&b);

        let got = dec.next().unwrap();
        assert_eq!(
            exp[0], got,
            "read value mismatch: exp: {}, got: {}",
            exp[0], got
        );
    }

    #[test]
    fn test_batch_timestamp_time_array_encode_all_two() {
        let mut src = vec![0i64, 1i64];
        let exp = src.clone();

        let b = time_array_encode_all(&mut src).unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            TIME_COMPRESSED_RLE, got,
            "unexpected encoding, exp {}, got {}",
            TIME_COMPRESSED_RLE, got
        );

        let mut dec = TimeDecoder::new();
        dec.init(&b);

        let got = dec.next().unwrap();
        assert_eq!(
            exp[0], got,
            "read value mismatch: exp: {}, got: {}",
            exp[0], got
        );

        let got = dec.next().unwrap();
        assert_eq!(
            exp[1], got,
            "read value mismatch: exp: {}, got: {}",
            exp[1], got
        );
    }

    #[test]
    fn test_batch_timestamp_time_array_encode_all_three() {
        let mut src = vec![0i64, 1i64, 3i64];
        let exp = src.clone();

        let b = time_array_encode_all(&mut src).unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            TIME_COMPRESSED_PACKED_SIMPLE, got,
            "unexpected encoding, exp {}, got {}",
            TIME_COMPRESSED_PACKED_SIMPLE, got
        );

        let mut dec = TimeDecoder::new();
        dec.init(&b);

        let got = dec.next().unwrap();
        assert_eq!(
            exp[0], got,
            "read value mismatch: exp: {}, got: {}",
            exp[0], got
        );

        let got = dec.next().unwrap();
        assert_eq!(
            exp[1], got,
            "read value mismatch: exp: {}, got: {}",
            exp[1], got
        );

        let got = dec.next().unwrap();
        assert_eq!(
            exp[2], got,
            "read value mismatch: exp: {}, got: {}",
            exp[2], got
        );
    }

    #[test]
    fn test_batch_timestamp_time_array_encode_all_large_range() {
        let mut src: Vec<i64> = vec![1442369134000000000, 1442369135000000000];
        let exp = src.clone();

        let b = time_array_encode_all(&mut src).unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            TIME_COMPRESSED_RLE, got,
            "unexpected encoding, exp {}, got {}",
            TIME_COMPRESSED_RLE, got
        );

        let mut dec = TimeDecoder::new();
        dec.init(&b);

        let got = dec.next().unwrap();
        assert_eq!(
            exp[0], got,
            "read value mismatch: exp: {}, got: {}",
            exp[0], got
        );

        let got = dec.next().unwrap();
        assert_eq!(
            exp[1], got,
            "read value mismatch: exp: {}, got: {}",
            exp[1], got
        );
    }

    #[test]
    fn test_batch_timestamp_time_array_encode_all_uncompressed() {
        let mut src: Vec<i64> = vec![
            Time::from_timestamp(0, 0).timestamp_nanos(),
            Time::from_timestamp(1, 0).timestamp_nanos(),
        ];
        // about 36.5yrs in NS resolution is max range for compressed format
        // This should cause the encoding to fallback to raw points
        src.push(Time::from_timestamp(2, 0).timestamp_nanos() + (2i64 << 59));

        let exp = src.clone();

        let b = time_array_encode_all(&mut src).unwrap();

        assert_eq!(25, b.len(), "length mismatch: exp {}, got {}", 25, b.len());

        let got = b[0] >> 4;
        assert_eq!(
            TIME_UNCOMPRESSED, got,
            "unexpected encoding, exp {}, got {}",
            TIME_UNCOMPRESSED, got
        );

        let mut dec = TimeDecoder::new();
        dec.init(&b);

        let got = dec.next().unwrap();
        assert_eq!(
            exp[0], got,
            "read value mismatch: exp: {}, got: {}",
            exp[0], got
        );

        let got = dec.next().unwrap();
        assert_eq!(
            exp[1], got,
            "read value mismatch: exp: {}, got: {}",
            exp[1], got
        );

        let got = dec.next().unwrap();
        assert_eq!(
            exp[2], got,
            "read value mismatch: exp: {}, got: {}",
            exp[2], got
        );
    }

    #[test]
    fn test_batch_timestamp_time_array_encode_all_rle() {
        let mut src: Vec<i64> = vec![];
        for i in 0..500 {
            src.push(i);
        }
        let exp = src.clone();

        let b = time_array_encode_all(&mut src).unwrap();

        assert_eq!(12, b.len(), "length mismatch: exp {}, got {}", 12, b.len());

        let got = b[0] >> 4;
        assert_eq!(
            TIME_COMPRESSED_RLE, got,
            "unexpected encoding, exp {}, got {}",
            TIME_COMPRESSED_RLE, got
        );

        let mut dec = TimeDecoder::new();
        dec.init(&b);
        for (i, v) in exp.iter().enumerate() {
            let got = dec.next().unwrap();
            assert_eq!(*v, got, "Item {} mismatch, exp {}, got {}", i, *v, got);
        }

        assert!(dec.next().is_none(), "unexpected extra values");
    }

    #[test]
    fn test_batch_timestamp_time_array_encode_all_reverse() {
        let mut src: Vec<i64> = vec![3, 2, 0];
        let exp = src.clone();

        let b = time_array_encode_all(&mut src).unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            TIME_UNCOMPRESSED, got,
            "unexpected encoding, exp {}, got {}",
            TIME_UNCOMPRESSED, got
        );

        let mut dec = TimeDecoder::new();
        dec.init(&b);
        for (i, v) in exp.iter().enumerate() {
            let got = dec.next().unwrap();
            assert_eq!(*v, got, "Item {} mismatch, exp {}, got {}", i, *v, got);
        }

        assert!(dec.next().is_none(), "unexpected extra values");
    }

    #[test]
    fn test_batch_timestamp_time_array_encode_all_220second_delta() {
        let mut src: Vec<i64> = vec![];
        let now = Time::now();
        for i in 0..220 {
            src.push(
                now.checked_add(Duration::from_secs(i * 60))
                    .unwrap()
                    .timestamp_nanos(),
            );
        }

        let exp = src.clone();

        let b = time_array_encode_all(&mut src).unwrap();

        // Using RLE, should get 12 bytes
        assert_eq!(
            12,
            b.len(),
            "unexpected length: exp {}, got {}",
            12,
            b.len()
        );

        let got = b[0] >> 4;
        assert_eq!(
            TIME_COMPRESSED_RLE, got,
            "unexpected encoding, exp {}, got {}",
            TIME_COMPRESSED_RLE, got
        );

        let mut dec = TimeDecoder::new();
        dec.init(&b);
        for (i, v) in exp.iter().enumerate() {
            let got = dec.next().unwrap();
            assert_eq!(*v, got, "Item {} mismatch, exp {}, got {}", i, *v, got);
        }

        assert!(dec.next().is_none(), "unexpected extra values");
    }

    #[test]
    fn test_batch_timestamp_time_array_encode_all_rand() {
        // Write values to encoder.
        let mut ts = vec![0i64; 50];
        for v in ts.iter_mut() {
            *v = rand::random::<i64>();
        }

        let exp = ts.clone();

        // Retrieve encoded bytes from encoder.
        let b = time_array_encode_all(&mut ts).unwrap();

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
        assert_eq!(exp, got, "mismatch:\n\nexp={:?}\n\ngot={:?}\n\n", ts, got);
    }

    #[test]
    fn test_batch_timestamp_time_array_encode_all_rle_seconds() {
        // Write values to encoder.
        let mut ts: Vec<i64> = vec![
            1444448158000000000,
            1444448168000000000,
            1444448178000000000,
            1444448188000000000,
            1444448198000000000,
            1444448208000000000,
        ];

        let exp = ts.clone();

        // Retrieve encoded bytes from encoder.
        let b = time_array_encode_all(&mut ts).unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            TIME_COMPRESSED_RLE, got,
            "unexpected encoding, exp {}, got {}",
            TIME_COMPRESSED_RLE, got
        );

        let mut dec = TimeDecoder::new();
        dec.init(&b);
        for (i, v) in exp.iter().enumerate() {
            let got = dec.next().unwrap();
            assert_eq!(*v, got, "Item {} mismatch, exp {}, got {}", i, *v, got);
        }

        assert!(dec.next().is_none(), "unexpected extra values");
    }

    #[test]
    fn test_batch_timestamp_time_array_encode_all_count_uncompressed() {
        // Write values to encoder.
        let mut src: Vec<i64> = vec![
            Time::from_timestamp(0, 0).timestamp_nanos(),
            Time::from_timestamp(1, 0).timestamp_nanos(),
        ];
        src.push(Time::from_timestamp(2, 0).timestamp_nanos() + (2i64 << 59));

        // Retrieve encoded bytes from encoder.
        let b = time_array_encode_all(&mut src).unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            TIME_UNCOMPRESSED, got,
            "unexpected encoding, exp {}, got {}",
            TIME_UNCOMPRESSED, got
        );

        let got = count_timestamps(&b);
        assert_eq!(3, got, "count mismatch: exp: {}, got {}", 3, got);
    }

    #[test]
    fn test_batch_timestamp_time_array_encode_all_count_rle() {
        // Write values to encoder.
        let mut src: Vec<i64> = vec![
            1444448158000000000,
            1444448168000000000,
            1444448178000000000,
            1444448188000000000,
            1444448198000000000,
            1444448208000000000,
        ];

        let exp = src.clone();

        // Retrieve encoded bytes from encoder.
        let b = time_array_encode_all(&mut src).unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            TIME_COMPRESSED_RLE, got,
            "unexpected encoding, exp {}, got {}",
            TIME_COMPRESSED_RLE, got
        );

        let (exp, got) = (exp.len() as i32, count_timestamps(&b));
        assert_eq!(exp, got, "count mismatch: exp: {}, got {}", exp, got);
    }

    #[test]
    fn test_batch_timestamp_time_array_encode_all_count_simple8() {
        // Write values to encoder.
        let mut src: Vec<i64> = vec![
            1444448158000000000,
            1444448168000000000,
            1444448178000000000,
            1444448188000000000,
            1444448198000000000,
            1444448208000000000,
        ];

        let exp = src.clone();

        // Retrieve encoded bytes from encoder.
        let b = time_array_encode_all(&mut src).unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            TIME_COMPRESSED_RLE, got,
            "unexpected encoding, exp {}, got {}",
            TIME_COMPRESSED_RLE, got
        );

        let (exp, got) = (exp.len() as i32, count_timestamps(&b));
        assert_eq!(exp, got, "count mismatch: exp: {}, got {}", exp, got);
    }

    #[test]
    fn test_batch_timestamp_time_array_decode_all_no_values() {
        let mut enc = TimeEncoder::new(0);
        let b = enc.bytes().unwrap();

        let got = time_array_decode_all(&b).unwrap();
        let exp = Vec::<i64>::new();
        assert_eq!(
            exp, got,
            "unexpected values, exp: {:?}, got: {:?}",
            exp, got
        );
    }

    #[test]
    fn test_batch_timestamp_time_array_decode_all_one() {
        let mut enc = TimeEncoder::new(1);
        let exp: Vec<i64> = vec![0];

        for v in &exp {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            TIME_COMPRESSED_PACKED_SIMPLE, got,
            "unexpected encoding, exp {}, got {}",
            TIME_COMPRESSED_PACKED_SIMPLE, got
        );

        let got = time_array_decode_all(&b).unwrap();
        assert_eq!(exp, got, "unexpected values: exp {:?}, got {:?}", exp, got);
    }

    #[test]
    fn test_batch_timestamp_time_array_decode_all_two() {
        let mut enc = TimeEncoder::new(2);
        let exp: Vec<i64> = vec![0, 1];

        for v in &exp {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            TIME_COMPRESSED_RLE, got,
            "unexpected encoding, exp {}, got {}",
            TIME_COMPRESSED_RLE, got
        );

        let got = time_array_decode_all(&b).unwrap();
        assert_eq!(exp, got, "unexpected values: exp {:?}, got {:?}", exp, got);
    }

    #[test]
    fn test_batch_timestamp_time_array_decode_all_three() {
        let mut enc = TimeEncoder::new(3);
        let exp: Vec<i64> = vec![0, 1, 3];

        for v in &exp {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            TIME_COMPRESSED_PACKED_SIMPLE, got,
            "unexpected encoding, exp {}, got {}",
            TIME_COMPRESSED_PACKED_SIMPLE, got
        );

        let got = time_array_decode_all(&b).unwrap();
        assert_eq!(exp, got, "unexpected values: exp {:?}, got {:?}", exp, got);
    }

    #[test]
    fn test_batch_timestamp_time_array_decode_all_large_range() {
        let mut enc = TimeEncoder::new(2);
        let exp: Vec<i64> = vec![1442369134000000000, 1442369135000000000];

        for v in &exp {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            TIME_COMPRESSED_RLE, got,
            "unexpected encoding, exp {}, got {}",
            TIME_COMPRESSED_RLE, got
        );

        let got = time_array_decode_all(&b).unwrap();
        assert_eq!(exp, got, "unexpected values: exp {:?}, got {:?}", exp, got);
    }

    #[test]
    fn test_batch_timestamp_time_array_decode_all_uncompressed() {
        let mut enc = TimeEncoder::new(3);
        let exp: Vec<i64> = vec![
            Time::from_timestamp(0, 0).timestamp_nanos(),
            Time::from_timestamp(1, 0).timestamp_nanos(),
            // about 36.5yrs in NS resolution is max range for compressed format
            // This should cause the encoding to fallback to raw points
            Time::from_timestamp(2, 0).timestamp_nanos() + (2i64 << 59),
        ];

        for v in &exp {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        assert_eq!(25, b.len(), "length mismatch: exp {}, got {}", 25, b.len());

        let got = b[0] >> 4;
        assert_eq!(
            TIME_UNCOMPRESSED, got,
            "unexpected encoding, exp {}, got {}",
            TIME_UNCOMPRESSED, got
        );

        let got = time_array_decode_all(&b).unwrap();
        assert_eq!(exp, got, "unexpected values: exp {:?}, got {:?}", exp, got);
    }

    #[test]
    fn test_batch_timestamp_time_array_decode_all_rle() {
        let mut enc = TimeEncoder::new(512);
        let exp: Vec<i64> = (0..500).collect();

        for v in &exp {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        assert_eq!(12, b.len(), "length mismatch: exp {}, got {}", 12, b.len());

        let got = b[0] >> 4;
        assert_eq!(
            TIME_COMPRESSED_RLE, got,
            "unexpected encoding, exp {}, got {}",
            TIME_COMPRESSED_RLE, got
        );

        let got = time_array_decode_all(&b).unwrap();
        assert_eq!(exp, got, "unexpected values: exp {:?}, got {:?}", exp, got);
    }

    #[test]
    fn test_batch_timestamp_time_array_decode_all_reverse() {
        let mut enc = TimeEncoder::new(3);
        let exp: Vec<i64> = vec![3, 2, 0];

        for v in &exp {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            TIME_UNCOMPRESSED, got,
            "unexpected encoding, exp {}, got {}",
            TIME_UNCOMPRESSED, got
        );

        let got = time_array_decode_all(&b).unwrap();
        assert_eq!(exp, got, "unexpected values: exp {:?}, got {:?}", exp, got);
    }

    #[test]
    fn test_batch_timestamp_time_array_decode_all_negative() {
        let mut enc = TimeEncoder::new(3);
        let exp: Vec<i64> = vec![
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

        for v in &exp {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            TIME_UNCOMPRESSED, got,
            "unexpected encoding, exp {}, got {}",
            TIME_UNCOMPRESSED, got
        );

        let got = time_array_decode_all(&b).unwrap();
        assert_eq!(exp, got, "unexpected values: exp {:?}, got {:?}", exp, got);
    }

    #[test]
    fn test_batch_timestamp_time_array_decode_all_220second_delta() {
        let mut enc = TimeEncoder::new(256);
        let mut exp: Vec<i64> = vec![];

        let now = Time::now();
        for i in 0..220 {
            exp.push(
                now.checked_add(Duration::from_secs(i * 60))
                    .unwrap()
                    .timestamp_nanos(),
            );
        }

        for v in &exp {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        // Using RLE, should get 12 bytes
        assert_eq!(
            12,
            b.len(),
            "unexpected length: exp {}, got {}",
            12,
            b.len()
        );

        let got = b[0] >> 4;
        assert_eq!(
            TIME_COMPRESSED_RLE, got,
            "unexpected encoding, exp {}, got {}",
            TIME_COMPRESSED_RLE, got
        );

        let got = time_array_decode_all(&b).unwrap();
        assert_eq!(exp, got, "unexpected values: exp {:?}, got {:?}", exp, got);
    }

    #[test]
    fn test_batch_timestamp_time_array_decode_all_rand() {
        // Write values to encoder.
        let mut ts = vec![0i64; 50];
        for v in ts.iter_mut() {
            *v = rand::random::<i64>();
        }

        let exp = ts.clone();

        let mut enc = TimeEncoder::new(1024);
        for v in &exp {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        let got = time_array_decode_all(&b).unwrap();
        assert_eq!(exp, got, "unexpected values: exp {:?}, got {:?}", exp, got);
    }

    #[test]
    fn test_batch_timestamp_time_array_decode_all_rle_seconds() {
        let exp: Vec<i64> = vec![
            1444448158000000000,
            1444448168000000000,
            1444448178000000000,
            1444448188000000000,
            1444448198000000000,
            1444448208000000000,
        ];

        let mut enc = TimeEncoder::new(1024);
        for v in &exp {
            enc.write(*v);
        }

        let b = enc.bytes().unwrap();

        let got = b[0] >> 4;
        assert_eq!(
            TIME_COMPRESSED_RLE, got,
            "unexpected encoding, exp {}, got {}",
            TIME_COMPRESSED_RLE, got
        );

        let got = time_array_decode_all(&b).unwrap();
        assert_eq!(exp, got, "unexpected values: exp {:?}, got {:?}", exp, got);
    }

    // TODO: implement me
    // #[test]
    // fn test_batch_timestamp_time_array_decode_all_corrupt() {
    //     cases := []string{
    //         "\x10\x14",         // Packed: not enough data
    //         "\x20\x00",         // RLE: not enough data for starting timestamp
    //         "\x2012345678\x90", // RLE: initial timestamp but invalid uvarint encoding
    //         "\x2012345678\x7f", // RLE: timestamp, RLE but invalid repeat
    //         "\x00123",          // Raw: data length not multiple of 8
    //     }

    //     for _, c := range cases {
    //         t.Run(fmt.Sprintf("%q", c), func(t *testing.T) {
    //             got, err := TimeArrayDecodeAll([]byte(c), nil)
    //             if err == nil {
    //                 t.Fatal("exp an err, got nil")
    //             }

    //             exp := []int64{}
    //             if !cmp.Equal(got, exp) {
    //                 t.Fatalf("unexpected value: -got/+exp\n%s", cmp.Diff(got, exp))
    //             }
    //         })
    //     }
    // }
}
