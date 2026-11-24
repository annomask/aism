//! Bounded adapters for TSM column codecs. All integers on disk are big endian.

use super::{PAGE_GROUP, Result, invalid};
use crate::storage::tsm1::data_encoding::{
    batch_bool, batch_float, batch_int, batch_timestamp, float::UVNAN, simple8b,
};
use integer_encoding::VarInt;

pub(super) fn take<'a>(bytes: &mut &'a [u8], len: usize) -> Result<&'a [u8]> {
    if len > bytes.len() {
        return Err(invalid("truncated data"));
    }
    let (head, tail) = bytes.split_at(len);
    *bytes = tail;
    Ok(head)
}
pub(super) fn u16(bytes: &mut &[u8]) -> Result<u16> {
    Ok(u16::from_be_bytes(take(bytes, 2)?.try_into().unwrap()))
}
pub(super) fn u32(bytes: &mut &[u8]) -> Result<u32> {
    Ok(u32::from_be_bytes(take(bytes, 4)?.try_into().unwrap()))
}
pub(super) fn u64(bytes: &mut &[u8]) -> Result<u64> {
    Ok(u64::from_be_bytes(take(bytes, 8)?.try_into().unwrap()))
}
pub(super) fn i64(bytes: &mut &[u8]) -> Result<i64> {
    Ok(u64(bytes)? as i64)
}

pub(super) fn pack_parts(parts: &[Vec<u8>]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    for part in parts {
        out.extend_from_slice(
            &u16::try_from(part.len())
                .map_err(|_| invalid("column too large"))?
                .to_be_bytes(),
        );
    }
    for part in parts {
        out.extend_from_slice(part);
    }
    Ok(out)
}
pub(super) fn unpack_parts<const N: usize>(mut bytes: &[u8]) -> Result<[&[u8]; N]> {
    let mut lens = [0; N];
    for len in &mut lens {
        *len = u16(&mut bytes)? as usize;
    }
    let mut parts = [&[][..]; N];
    for (part, len) in parts.iter_mut().zip(lens) {
        *part = take(&mut bytes, len)?;
    }
    if !bytes.is_empty() {
        return Err(invalid("trailing column bytes"));
    }
    Ok(parts)
}
fn count_check(n: usize) -> Result<()> {
    if !(1..=PAGE_GROUP).contains(&n) {
        return Err(invalid("invalid column count"));
    }
    Ok(())
}
pub(super) fn ints(mut values: Vec<i64>) -> Result<Vec<u8>> {
    count_check(values.len())?;
    batch_int::integer_array_encode_all(&mut values).map_err(|_| invalid("integer encoding failed"))
}
pub(super) fn uints(values: Vec<u64>) -> Result<Vec<u8>> {
    ints(values.into_iter().map(|v| v as i64).collect())
}
pub(super) fn times(mut values: Vec<i64>) -> Result<Vec<u8>> {
    count_check(values.len())?;
    batch_timestamp::time_array_encode_all(&mut values)
        .map_err(|_| invalid("timestamp encoding failed"))
}
fn varint(bytes: &mut &[u8]) -> Result<u64> {
    let (v, n) = u64::decode_var(bytes).ok_or(invalid("invalid varint"))?;
    take(bytes, n)?;
    Ok(v)
}

// Inspect counts before calling the existing batch decoder: RLE otherwise trusts
// its on-disk count when allocating. Packed timestamp scaling is checked as well.
fn validate_integer(bytes: &[u8], n: usize, timestamp: bool) -> Result<()> {
    count_check(n)?;
    let mut input = bytes;
    let header = take(&mut input, 1)?[0];
    let exponent = header & 15;
    if (!timestamp && exponent != 0) || exponent > 12 {
        return Err(invalid("invalid codec exponent"));
    }
    let scale = 10u64.pow(exponent as u32);
    let actual = match header >> 4 {
        0 => {
            if exponent != 0 || input.len() % 8 != 0 {
                return Err(invalid("invalid raw column"));
            }
            input.len() / 8
        }
        1 => {
            take(&mut input, 8)?;
            if input.len() % 8 != 0 {
                return Err(invalid("invalid packed column"));
            }
            let count =
                simple8b::count_bytes(input).map_err(|_| invalid("invalid packed count"))? as usize;
            if count + 1 != n {
                return Err(invalid("column count mismatch"));
            }
            if timestamp && scale > 1 {
                let mut deltas = vec![0; count];
                simple8b::decode_bytes(&mut deltas, input)
                    .map_err(|_| invalid("invalid packed data"))?;
                for delta in deltas {
                    delta
                        .checked_mul(scale)
                        .ok_or(invalid("timestamp delta overflow"))?;
                }
            }
            count + 1
        }
        2 => {
            take(&mut input, 8)?;
            let delta = varint(&mut input)?;
            if timestamp {
                delta
                    .checked_mul(scale)
                    .ok_or(invalid("timestamp delta overflow"))?;
            }
            let count = varint(&mut input)?;
            if !input.is_empty() {
                return Err(invalid("trailing RLE bytes"));
            }
            let count = usize::try_from(count).map_err(|_| invalid("RLE count overflow"))?;
            if timestamp {
                count
            } else {
                count.checked_add(1).ok_or(invalid("RLE count overflow"))?
            }
        }
        _ => return Err(invalid("unknown integer codec")),
    };
    if actual != n {
        return Err(invalid("column count mismatch"));
    }
    Ok(())
}
pub(super) fn decode_ints(bytes: &[u8], n: usize) -> Result<Vec<i64>> {
    validate_integer(bytes, n, false)?;
    batch_int::integer_array_decode_all(bytes).map_err(|_| invalid("invalid integer column"))
}
pub(super) fn decode_uints(bytes: &[u8], n: usize) -> Result<Vec<u64>> {
    Ok(decode_ints(bytes, n)?
        .into_iter()
        .map(|v| v as u64)
        .collect())
}
pub(super) fn decode_times(bytes: &[u8], n: usize) -> Result<Vec<i64>> {
    validate_integer(bytes, n, true)?;
    batch_timestamp::time_array_decode_all(bytes).map_err(|_| invalid("invalid timestamp column"))
}
pub(super) fn floats(values: Vec<f64>) -> Result<Vec<u8>> {
    count_check(values.len())?;
    if let Ok(encoded) = batch_float::float_array_encode_all(&values) {
        return Ok(encoded);
    }
    // Aggregates may contain NaN (including inf + -inf); Gorilla reserves a NaN
    // sentinel. Codec 0 stores IEEE bits verbatim, preserving NaN payloads.
    let mut out = vec![0];
    for value in values {
        out.extend_from_slice(&value.to_bits().to_be_bytes());
    }
    Ok(out)
}

struct Bits<'a> {
    bytes: &'a [u8],
    pos: usize,
}
impl Bits<'_> {
    fn read(&mut self, n: usize) -> Result<u64> {
        if n > 64 || self.pos + n > self.bytes.len() * 8 {
            return Err(invalid("truncated float bits"));
        }
        let mut value = 0;
        for _ in 0..n {
            value = (value << 1) | u64::from((self.bytes[self.pos / 8] >> (7 - self.pos % 8)) & 1);
            self.pos += 1;
        }
        Ok(value)
    }
}
pub(super) fn decode_floats(bytes: &[u8], n: usize) -> Result<Vec<f64>> {
    count_check(n)?;
    let mut input = bytes;
    match take(&mut input, 1)?[0] {
        0 => {
            if input.len() != n * 8 {
                return Err(invalid("raw float length mismatch"));
            }
            (0..n)
                .map(|_| Ok(f64::from_bits(u64(&mut input)?)))
                .collect()
        }
        0x10 => {
            // The legacy batch reader assumes trusted bytes. Bound the Gorilla
            // bit stream and its output here before it reaches query execution.
            let mut bits = Bits {
                bytes: input,
                pos: 0,
            };
            let mut value = bits.read(64)?;
            let mut width = 64;
            let mut trailing = 0;
            let mut out = Vec::with_capacity(n);
            for i in 0..=n {
                if i != 0 && bits.read(1)? != 0 {
                    if bits.read(1)? != 0 {
                        let leading = bits.read(5)? as usize;
                        width = bits.read(6)? as usize;
                        if width == 0 {
                            width = 64;
                        }
                        trailing = 64usize
                            .checked_sub(leading + width)
                            .ok_or(invalid("invalid Gorilla window"))?;
                    }
                    value ^= bits.read(width)? << trailing;
                }
                if i == n {
                    if value != UVNAN {
                        return Err(invalid("missing Gorilla terminator"));
                    }
                } else {
                    if value == UVNAN {
                        return Err(invalid("premature Gorilla terminator"));
                    }
                    out.push(f64::from_bits(value));
                }
            }
            let remaining = input.len() * 8 - bits.pos;
            if remaining > 7 || bits.read(remaining)? != 0 {
                return Err(invalid("trailing Gorilla data"));
            }
            Ok(out)
        }
        _ => Err(invalid("unknown float codec")),
    }
}
pub(super) fn bools(values: &[bool]) -> Result<Vec<u8>> {
    count_check(values.len())?;
    batch_bool::bool_array_encode_all(values).map_err(|_| invalid("boolean encoding failed"))
}
pub(super) fn decode_bools(bytes: &[u8], n: usize) -> Result<Vec<bool>> {
    count_check(n)?;
    let mut input = bytes;
    if take(&mut input, 1)?[0] != 0x10
        || varint(&mut input)? != n as u64
        || input.len() != n.div_ceil(8)
    {
        return Err(invalid("invalid boolean column"));
    }
    batch_bool::bool_array_decode_all(bytes).map_err(|_| invalid("invalid boolean data"))
}
