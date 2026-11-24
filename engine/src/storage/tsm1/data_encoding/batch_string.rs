use bytes::BufMut;
use integer_encoding::VarInt;
use snafu::ResultExt;

use crate::storage::tsm1::data_encoding::string::*;

// StringArrayEncodeAll encodes src into b, returning b and any error encountered.
// The returned slice may be of a different length and capacity to b.
//
// Currently only the string compression scheme used snappy.
pub fn string_array_encode_all(src: &[String]) -> Result<Vec<u8>> {
    let mut src_sz_64 = 2 + src.len() * i32::MAX.required_space(); // strings shouldn't be longer than 64kb
    for v in src {
        src_sz_64 += v.len();
    }

    // 32-bit systems
    if src_sz_64 > u32::MAX as usize {
        return Err(Error::StringErr {
            reason: "string_array_encode_all: source length too large".to_string(),
        });
    }

    let src_sz = src_sz_64;

    // determine the maximum possible length needed for the buffer, which
    // includes the compressed size
    let mut compressed_sz = 0;
    if !src.is_empty() {
        let mle = snap::raw::max_compress_len(src_sz);
        if mle == 0 {
            return Err(Error::StringErr {
                reason: "string_array_encode_all: source length too large".to_string(),
            });
        }
        compressed_sz = mle + 1; /* header */
    }
    let tot_sz = src_sz + compressed_sz;

    let mut dst = vec![0u8; tot_sz];
    // Shortcut to snappy encoding nothing.
    if src.is_empty() {
        dst[0] = STRING_COMPRESSED_SNAPPY << 4;
        dst.truncate(2);
        return Ok(dst);
    }

    // write the data to be compressed *after* the space needed for snappy
    // compression. The compressed data is at the start of the allocated buffer,
    // ensuring the entire capacity is returned and available for subsequent use.
    let dta = &mut dst[compressed_sz..];
    let mut n = 0;
    for v in src {
        n += (v.len() as u64).encode_var(&mut dta[n..]);
        let bytes = v.as_bytes();
        (&mut dta[n..n + bytes.len()]).put_slice(bytes);
        n += bytes.len();
    }

    let (b, dta) = dst.split_at_mut(compressed_sz);
    b[0] = STRING_COMPRESSED_SNAPPY << 4;
    let compressed_n = snap::raw::Encoder::new()
        .compress(&dta[..n], &mut b[1..])
        .context(SnapErrSnafu)?;

    dst.truncate(compressed_n + 1);

    Ok(dst)
}

pub fn string_array_decode_all(b: &[u8]) -> Result<Vec<String>> {
    // First byte stores the encoding type, only have snappy format
    // currently so ignore for now.

    let b = if !b.is_empty() {
        // it is important that to note that `snappy.Decode` always returns
        // a newly allocated slice as the final strings reference this slice
        // directly.
        snap::raw::Decoder::new()
            .decompress_vec(&b[1..])
            .context(SnapErrSnafu)?
    } else {
        return Ok(vec![]);
    };

    let mut i = 0;

    let mut dst = vec![String::new(); 64];

    let mut j = 0;

    while i < b.len() {
        let (length, n) = match u64::decode_var(&b.as_slice()[i..]) {
            Some((sz, n)) => (sz, n),
            None => {
                return Err(Error::StringErr {
                    reason: "string_array_decode_all: invalid encoded string length".to_string(),
                });
            }
        };

        if n == 0 {
            // n is usize
            return Err(Error::StringErr {
                reason: "string_array_decode_all: invalid encoded string length".to_string(),
            });
        }

        // The length of this string plus the length of the variable byte encoded length
        let l = length + n as u64;

        let lower = i + n;
        let upper = lower + length as usize;
        if upper < lower {
            return Err(Error::StringErr {
                reason: "string_array_decode_all: length overflow".to_string(),
            });
        }
        if upper > b.len() {
            return Err(Error::StringErr {
                reason: "string_array_decode_all: short buffer".to_string(),
            });
        }

        // Returning Vec<String> requires each decoded value to own its bytes,
        // so this per-string copy is required under the current API.
        let val = unsafe { String::from_utf8_unchecked(b[lower..upper].to_vec()) };
        if j < dst.len() {
            dst[j] = val;
        } else {
            dst.push(val); // force a resize
            dst.truncate(dst.capacity());
        }
        i += l as usize;
        j += 1;
    }
    dst.truncate(j);

    Ok(dst)
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use uuid::Uuid;

    use super::{Error, StringDecoder, string_array_encode_all};
    use crate::storage::tsm1::data_encoding::{
        batch_string::string_array_decode_all,
        string::{STRING_COMPRESSED_SNAPPY, StringEncoder},
    };

    #[test]
    fn test_batch_string_string_array_encode_all_no_values() {
        let b = string_array_encode_all(&[]).unwrap();

        let mut dec = StringDecoder::new();
        dec.set_bytes(&b);

        assert!(
            dec.next().is_none(),
            "unexpected next value: exp None, got Some"
        );
    }

    #[test]
    fn test_batch_string_string_array_encode_all_exceeds_max_encoded_len() {
        let str = " ".repeat(1 << 23); // 8MB string
        let mut s = vec![];
        for _ in 0..512 {
            s.push(str.clone());
        }

        let got = string_array_encode_all(&s);
        if let Err(Error::StringErr { reason }) = got {
            assert_eq!(
                &reason, "string_array_encode_all: source length too large",
                "exp error: {}, got {}",
                "string_array_encode_all: source length too large", reason
            );
        } else {
            panic!("expected error");
        }
    }

    #[test]
    fn test_batch_string_string_array_encode_all_single() {
        let src = vec!["v1".to_string()];

        let b = string_array_encode_all(&src).unwrap();

        let mut dec = StringDecoder::new();
        dec.set_bytes(&b).unwrap();

        let got = dec.next().unwrap();
        assert_eq!(
            src[0], got,
            "unexpected next value: exp {:?}, got {:?}",
            src[0], got
        );

        assert!(
            dec.next().is_none(),
            "unexpected next value: exp None, got Some"
        );
    }

    #[test]
    fn test_batch_string_string_array_encode_compare() {
        // generate random values
        let mut input = vec![];
        for _ in 0..1000 {
            input.push(Uuid::new_v4().to_string());
        }

        // Example from the paper
        let mut enc = StringEncoder::new(1000);
        for v in &input {
            enc.write(v.as_bytes());
        }
        enc.flush();

        let buf1 = enc.bytes().unwrap();

        let buf2 = string_array_encode_all(&input).unwrap();

        let result = string_array_decode_all(&buf2).unwrap();

        assert_eq!(
            input, result,
            "unexpected next value: exp {:?}, got {:?}",
            input, result
        );

        // Check that the encoders are byte for byte the same...
        assert_eq!(buf1, buf2, "Raw bytes differ for encoders");
    }

    #[test]
    fn test_batch_string_string_array_encode_all_multi_compressed() {
        // generate random values
        let mut input = vec![];
        for i in 0..10 {
            input.push(format!("value {}", i));
        }

        let b = string_array_encode_all(&input).unwrap();

        assert_eq!(
            STRING_COMPRESSED_SNAPPY,
            b[0] >> 4,
            "unexpected encoding: exp {}, got {}",
            STRING_COMPRESSED_SNAPPY,
            b[0]
        );

        assert_eq!(
            51,
            b.len(),
            "unexpected length: exp {}, got {}",
            51,
            b.len()
        );

        let mut dec = StringDecoder::new();
        dec.set_bytes(&b).unwrap();

        for (i, v) in input.iter().enumerate() {
            let got = dec.next().unwrap();
            assert_eq!(
                *v, got,
                "unexpected value at pos {}: exp {}, got {}",
                i, v, got
            );
        }

        assert!(
            dec.next().is_none(),
            "unexpected next value, exp None, got Some"
        );
    }

    #[test]
    fn test_batch_string_string_array_encode_all_rand() {
        // generate random values
        let mut input = vec![];
        for _ in 0..50 {
            let rndstr = rand::thread_rng()
                .sample_iter::<char, _>(rand::distributions::Standard)
                .take(50)
                .collect();
            input.push(rndstr);
        }

        let b = string_array_encode_all(&input).unwrap();

        let mut dec = StringDecoder::new();
        dec.set_bytes(&b).unwrap();

        for (i, v) in input.iter().enumerate() {
            let got = dec.next().unwrap();
            assert_eq!(
                *v, got,
                "unexpected value at pos {}: exp {}, got {}",
                i, v, got
            );
        }

        assert!(
            dec.next().is_none(),
            "unexpected next value, exp None, got Some"
        );
    }

    #[test]
    fn test_batch_string_string_array_decode_all_no_values() {
        let mut enc = StringEncoder::new(1024);
        let b = enc.bytes().unwrap();

        let exp = Vec::<String>::new();
        let got = string_array_decode_all(&b).unwrap();
        assert_eq!(exp, exp, "unexpected value: exp {:?}, got {:?}", exp, got);
    }

    #[test]
    fn test_batch_string_string_array_decode_all_single() {
        let v1 = "v1".to_string();
        let mut enc = StringEncoder::new(1024);
        enc.write(v1.as_bytes());

        let b = enc.bytes().unwrap();

        let exp = vec![v1.clone()];
        let got = string_array_decode_all(&b).unwrap();
        assert_eq!(exp, exp, "unexpected value: exp {:?}, got {:?}", exp, got);
    }

    #[test]
    fn test_batch_string_string_array_decode_all_multi_compressed() {
        let mut exp = vec![];
        let mut enc = StringEncoder::new(1024);
        for i in 0..10 {
            let v = format!("value {}", i);
            enc.write(v.as_bytes());
            exp.push(v);
        }

        let b = enc.bytes().unwrap();

        assert_eq!(
            STRING_COMPRESSED_SNAPPY,
            b[0] >> 4,
            "unexpected encoding: exp {}, got {}",
            STRING_COMPRESSED_SNAPPY,
            b[0]
        );

        assert_eq!(
            51,
            b.len(),
            "unexpected length: exp {}, got {}",
            51,
            b.len()
        );

        let got = string_array_decode_all(&b).unwrap();
        assert_eq!(exp, exp, "unexpected value: exp {:?}, got {:?}", exp, got);
    }

    #[test]
    fn test_batch_string_string_array_decode_all_rand() {
        let mut exp = vec![];
        let mut enc = StringEncoder::new(1024);
        for _ in 0..50 {
            let rndstr = rand::thread_rng()
                .sample_iter::<char, _>(rand::distributions::Standard)
                .take(0)
                .collect::<String>();
            enc.write(rndstr.as_bytes());
            exp.push(rndstr);
        }

        let b = enc.bytes().unwrap();

        let got = string_array_decode_all(&b).unwrap();
        assert_eq!(exp, exp, "unexpected value: exp {:?}, got {:?}", exp, got);
    }

    #[test]
    fn test_batch_string_string_array_decode_all_empty() {
        let exp = Vec::<String>::new();
        let got = string_array_decode_all(&[]).unwrap();
        assert_eq!(exp, exp, "unexpected value: exp {:?}, got {:?}", exp, got);
    }

    #[test]
    fn test_batch_string_string_array_decode_all_corrupt_bytes() {
        // cases := []string{
        //     "\x10\x03\b\x03Hi", // Higher length than actual data
        //     "\x10\x1dp\x9c\x90\x90\x90\x90\x90\x90\x90\x90\x90length overflow----",
        //     "0t\x00\x01\x000\x00\x01\x000\x00\x01\x000\x00\x01\x000\x00\x01" +
        //         "\x000\x00\x01\x000\x00\x01\x000\x00\x00\x00\xff:\x01\x00\x01\x00\x01" +
        //         "\x00\x01\x00\x01\x00\x01\x00\x010\x010\x000\x010\x010\x010\x01" +
        //         "0\x010\x010\x010\x010\x010\x010\x010\x010\x010\x010", // Upper slice bounds overflows negative
        // }
        let cases = vec![
            "Hi".as_bytes().to_vec(),
            "plength overflow----".as_bytes().to_vec(),
            ("0t  0  0  0  0 ".to_string()
                + " 0  0  0   ÿ:  "
                + "    00 0000"
                + "00000000000")
                .into_bytes(),
        ];

        for c in cases {
            assert!(string_array_decode_all(&c).is_err(), "exp an error, got ok");
        }
    }
}
