//! Bounded integer value decoding without a block-sized output array.

use super::{
    encoding::zig_zag_decode,
    int::{Error, INT_COMPRESSED_RLE, INT_COMPRESSED_SIMPLE, INT_UNCOMPRESSED, Result},
    simple8b,
};
use integer_encoding::VarInt;

pub(crate) fn decode_with(bytes: &[u8], expected: usize, mut emit: impl FnMut(i64)) -> Result<()> {
    let Some((&header, payload)) = bytes.split_first() else {
        return Ok(());
    };
    let error = || Error::StringErr {
        reason: "invalid streaming integer block".into(),
    };
    let word = |bytes: &[u8]| u64::from_be_bytes(bytes.try_into().unwrap());
    match header >> 4 {
        INT_UNCOMPRESSED => {
            if payload.len() % 8 != 0 || payload.len() / 8 != expected {
                return Err(error());
            }
            let mut value = 0_i64;
            for delta in payload.chunks_exact(8) {
                value = value.wrapping_add(zig_zag_decode(word(delta)));
                emit(value);
            }
        }
        INT_COMPRESSED_SIMPLE => {
            if payload.len() < 8 || payload.len() % 8 != 0 {
                return Err(error());
            }
            let count = simple8b::count_bytes(&payload[8..])
                .map_err(|source| Error::Simple8b { source })? as usize;
            if count.checked_add(1) != Some(expected) {
                return Err(error());
            }
            let mut value = zig_zag_decode(word(&payload[..8]));
            emit(value);
            let mut scratch = [0_u64; 240];
            for packed in payload[8..].chunks_exact(8) {
                let n = simple8b::decode(&mut scratch, word(packed))
                    .map_err(|source| Error::Simple8b { source })?;
                for delta in &scratch[..n] {
                    value = value.wrapping_add(zig_zag_decode(*delta));
                    emit(value);
                }
            }
        }
        INT_COMPRESSED_RLE => {
            if payload.len() < 8 {
                return Err(error());
            }
            let first = zig_zag_decode(word(&payload[..8]));
            let (delta, n) = u64::decode_var(&payload[8..]).ok_or_else(error)?;
            let (repeats, _) = u64::decode_var(&payload[8 + n..]).ok_or_else(error)?;
            if repeats.checked_add(1) != Some(expected as u64) {
                return Err(error());
            }
            let delta = zig_zag_decode(delta);
            let mut value = first;
            for _ in 0..expected {
                emit(value);
                value = value.wrapping_add(delta);
            }
        }
        _ => return Err(error()),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::tsm1::data_encoding::batch_int::{
        integer_array_decode_all, integer_array_encode_all,
    };

    #[test]
    fn streaming_integer_preserves_all_codecs_signed_and_unsigned_wraps() {
        let inputs = [
            vec![5; 512],
            (0..512).map(|i| i * 3 - 17).collect(),
            vec![i64::MIN, i64::MAX, -1, 0, 1, i64::MIN + 1, i64::MAX - 1],
            (0..1024).map(|i| ((i * 7919) % 10007) - 5000).collect(),
        ];
        let mut encodings = std::collections::BTreeSet::new();
        for values in inputs {
            let encoded = integer_array_encode_all(&mut values.clone()).unwrap();
            encodings.insert(encoded[0] >> 4);
            let expected = integer_array_decode_all(&encoded).unwrap();
            let mut actual = Vec::new();
            decode_with(&encoded, expected.len(), |v| actual.push(v)).unwrap();
            assert_eq!(actual, expected);
            assert_eq!(
                actual.iter().map(|v| *v as u64).collect::<Vec<_>>(),
                expected.iter().map(|v| *v as u64).collect::<Vec<_>>()
            );
            assert!(decode_with(&encoded, expected.len() + 1, |_| {}).is_err());
        }
        assert_eq!(encodings.len(), 3);
    }
}
