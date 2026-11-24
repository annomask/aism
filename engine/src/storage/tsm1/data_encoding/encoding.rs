//! encoding time series data.
use crate::models::points::{
    BoolValue, FloatValue, IntegerValue, StringValue, UnsignedValue, Value,
};
use bytes::{Buf, BufMut};
use integer_encoding::VarInt;
use snafu::{ResultExt, Snafu, ensure};

use super::{
    bool::BoolEncoder,
    encoding_pools::{
        BOOL_DECODER_POOLS, BOOL_ENCODER_POOLS, FLOAT_DECODER_POOLS, FLOAT_ENCODER_POOLS,
        INTEGER_DECODER_POOLS, INTEGER_ENCODER_POOLS, STRING_DECODER_POOLS, STRING_ENCODER_POOLS,
        TIME_DECODER_POOLS, TIME_ENCODER_POOLS, get_bool_encoder, get_float_encoder,
        get_integer_encoder, get_string_encoder, get_time_encoder, get_unsigned_encoder,
        put_bool_encoder, put_float_encoder, put_integer_encoder, put_string_encoder,
        put_time_encoder, put_unsigned_encoder,
    },
    float::FloatEncoder,
    int::IntegerEncoder,
    string::StringEncoder,
    timestamp::TimeEncoder,
};
use crate::storage::tsm1::{
    data_encoding::{
        bool::BoolDecoder,
        float::FloatDecoder,
        int::IntegerDecoder,
        string::StringDecoder,
        timestamp::{TimeDecoder, count_timestamps},
    },
    vlog::VLogIndex,
};

// /////////////// Block types ////////////////
pub type BlockType = u8;
// BlockFloat64 designates a block encodes float64 values.
pub const BLOCK_FLOAT64: u8 = 0;
// BlockInteger designates a block encodes int64 values.
pub const BLOCK_INTEGER: u8 = 1;
// BlockUnsigned designates a block encodes uint64 values.
pub const BLOCK_UNSIGNED: u8 = 2;
// BlockString designates a block encodes string values.
pub const BLOCK_STRING: u8 = 3;
// BlockBoolean designates a block encodes boolean values.
pub const BLOCK_BOOL: u8 = 4;

// encodedBlockHeaderSize is the size of the header for an encoded block.  There is one
// byte encoding the type of the block.
pub const ENCODED_BLOCK_HEADER_SIZE: usize = 1;

/// Error type for Encoding.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    /// IO error.
    #[snafu(display("integer encode/decode error: {}", source))]
    IOError { source: std::io::Error },

    /// Integer encode/decod error.
    #[snafu(display("integer encode/decode error: {}", source))]
    Integer { source: super::int::Error },

    /// Bool encode/decod error.
    #[snafu(display("bool encode/decode error: {}", source))]
    Bool { source: super::bool::Error },

    /// Float encode/decod error.
    #[snafu(display("float encode/decode error: {}", source))]
    Float { source: super::float::Error },

    /// String encode/decod error.
    #[snafu(display("string encode/decode error: {}", source))]
    String { source: super::string::Error },

    /// Timestamp encode/decod error.
    #[snafu(display("timestamp encode/decode error: {}", source))]
    Timestamp { source: super::timestamp::Error },

    /// String error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },
}

/// Result type for Encoding.
pub type Result<T, E = Error> = std::result::Result<T, E>;

pub fn encode_integer_block(values: &[Value]) -> Result<Vec<u8>> {
    if values.is_empty() {
        return Ok(vec![]);
    }

    let mut tenc = get_time_encoder(values.len());
    let mut venc = get_integer_encoder(values.len());

    let b = encode_integer_block_using(values, &mut tenc, &mut venc)?;

    put_time_encoder(tenc);
    put_integer_encoder(venc);

    Ok(b)
}

pub fn encode_integer_block_using(
    values: &[Value],
    tenc: &mut TimeEncoder,
    venc: &mut IntegerEncoder,
) -> Result<Vec<u8>> {
    tenc.reset();
    venc.reset();

    // FIXME: write 1000 values cost 20.153µs
    for v in values {
        if let Value::IntegerValue(iv) = v {
            tenc.write(iv.unix_nano);
            venc.write(iv.value);
        }
    }

    // Encoded timestamp values
    let tb = tenc.bytes().context(TimestampSnafu)?; // FIXME:10 - 20µs
    // Encoded int64 values
    let vb = venc.bytes().context(IntegerSnafu)?;
    // Prepend the first timestamp of the block in the first 8 bytes
    Ok(pack_block(BLOCK_INTEGER, tb, &vb))
}

/// Decodes the integer block from the byte slice and appends the integer values to a.
pub fn decode_integer_block(block: &[u8]) -> Result<Vec<IntegerValue>> {
    let block_type = block[0];
    ensure!(
        block_type == BLOCK_INTEGER,
        StringErrSnafu {
            reason: format!(
                "invalid block type: exp {}, got {}",
                BLOCK_INTEGER, block_type
            ),
        }
    );

    let block = &block[1..];

    // The first 8 bytes is the minimum timestamp of the block
    let (tb, vb) = unpack_block(block)?;

    let sz = count_timestamps(tb);

    let mut dst = vec![IntegerValue::default(); sz as usize];

    // let mut tdec = TIME_DECODER_POOLS.get(0);
    // let mut vdec = INTEGER_DECODER_POOLS.get(0);
    let mut tdec = TimeDecoder::new();
    let mut vdec = IntegerDecoder::new();

    // Setup timestamp and value decoders
    tdec.init(tb);
    vdec.set_bytes(vb).context(IntegerSnafu)?;

    // Decode both a timestamp and value
    let mut i = 0;
    while i < dst.len() {
        if let (Some(t), Some(v)) = (tdec.next(), vdec.next()) {
            dst[i] = IntegerValue {
                unix_nano: t,
                value: v,
            };
        } else {
            break;
        }
        i += 1;
    }

    // Did timestamp decoding have an error?
    tdec.err().context(TimestampSnafu)?;

    // Did int64 decoding have an error?
    vdec.err().context(IntegerSnafu)?;

    // TIME_DECODER_POOLS.put(tdec);
    // INTEGER_DECODER_POOLS.put(vdec);

    dst.truncate(i);
    Ok(dst)
}

pub fn encode_unsigned_block(values: &[Value]) -> Result<Vec<u8>> {
    if values.is_empty() {
        return Ok(vec![]);
    }

    let mut tenc = get_time_encoder(values.len());
    let mut venc = get_unsigned_encoder(values.len());

    let b = encode_unsigned_block_using(values, &mut tenc, &mut venc)?;

    put_time_encoder(tenc);
    put_unsigned_encoder(venc);

    Ok(b)
}

pub fn encode_unsigned_block_using(
    values: &[Value],
    tenc: &mut TimeEncoder,
    venc: &mut IntegerEncoder,
) -> Result<Vec<u8>> {
    tenc.reset();
    venc.reset();

    for v in values {
        if let Value::UnsignedValue(uv) = v {
            tenc.write(uv.unix_nano);
            venc.write(uv.value as i64);
        }
    }

    // Encoded timestamp values
    let tb = tenc.bytes().context(TimestampSnafu)?;

    // Encoded int64 values
    let vb = venc.bytes().context(IntegerSnafu)?;

    // Prepend the first timestamp of the block in the first 8 bytes
    Ok(pack_block(BLOCK_UNSIGNED, tb, &vb))
}

/// Decodes the integer block from the byte slice and appends the integer values to a.
pub fn decode_unsigned_block(block: &[u8]) -> Result<Vec<UnsignedValue>> {
    let block_type = block[0];
    ensure!(
        block_type == BLOCK_UNSIGNED,
        StringErrSnafu {
            reason: format!(
                "invalid block type: exp {}, got {}",
                BLOCK_UNSIGNED, block_type
            ),
        }
    );

    let block = &block[1..];

    // The first 8 bytes is the minimum timestamp of the block
    let (tb, vb) = unpack_block(block)?;

    let sz = count_timestamps(tb);

    let mut dst = vec![UnsignedValue::default(); sz as usize];

    // let mut tdec = TIME_DECODER_POOLS.get(0);
    // let mut vdec = INTEGER_DECODER_POOLS.get(0);
    let mut tdec = TimeDecoder::new();
    let mut vdec = IntegerDecoder::new();

    // Setup timestamp and value decoders
    tdec.init(tb);
    vdec.set_bytes(vb).context(IntegerSnafu)?;

    // Decode both a timestamp and value
    let mut i = 0;
    while i < dst.len() {
        if let (Some(t), Some(v)) = (tdec.next(), vdec.next()) {
            dst[i] = UnsignedValue {
                unix_nano: t,
                value: v as u64,
            };
        } else {
            break;
        }
        i += 1;
    }

    // Did timestamp decoding have an error?
    tdec.err().context(TimestampSnafu)?;

    // Did int64 decoding have an error?
    vdec.err().context(IntegerSnafu)?;

    // TIME_DECODER_POOLS.put(tdec);
    // INTEGER_DECODER_POOLS.put(vdec);

    dst.truncate(i);
    Ok(dst)
}

pub fn encode_float_block(values: &[Value]) -> Result<Vec<u8>> {
    if values.is_empty() {
        return Ok(vec![]);
    }

    let mut tenc = get_time_encoder(values.len());
    let mut venc = get_float_encoder(values.len());

    let b = encode_float_block_using(values, &mut tenc, &mut venc)?;

    put_time_encoder(tenc);
    put_float_encoder(venc);

    Ok(b)
}

pub fn encode_float_block_using(
    values: &[Value],
    tenc: &mut TimeEncoder,
    venc: &mut FloatEncoder,
) -> Result<Vec<u8>> {
    tenc.reset();
    venc.reset();

    for v in values {
        if let Value::FloatValue(fv) = v {
            tenc.write(fv.unix_nano);
            venc.write(fv.value);
        }
    }
    venc.flush();

    // Encoded timestamp values
    let tb = tenc.bytes().context(TimestampSnafu)?;

    // Encoded int64 values
    let vb = venc.bytes().context(FloatSnafu)?;

    // Prepend the first timestamp of the block in the first 8 bytes
    Ok(pack_block(BLOCK_FLOAT64, tb, vb))
}

/// Decodes the float block from the byte slice.
pub fn decode_float_block(block: &[u8]) -> Result<Vec<FloatValue>> {
    let block_type = block[0];
    ensure!(
        block_type == BLOCK_FLOAT64,
        StringErrSnafu {
            reason: format!(
                "invalid block type: exp {}, got {}",
                BLOCK_FLOAT64, block_type
            ),
        }
    );

    let block = &block[1..];

    // The first 8 bytes is the minimum timestamp of the block
    let (tb, vb) = unpack_block(block)?;

    let sz = count_timestamps(tb);

    let mut dst = vec![FloatValue::default(); sz as usize];

    // let mut tdec = TIME_DECODER_POOLS.get(0);
    // let mut vdec = FLOAT_DECODER_POOLS.get(0);
    let mut tdec = TimeDecoder::new();
    let mut vdec = FloatDecoder::new();

    // Setup timestamp and value decoders
    tdec.init(tb);
    vdec.set_bytes(vb).context(FloatSnafu)?;

    // Decode both a timestamp and value
    let mut i = 0;
    while i < dst.len() {
        if let (Some(t), Some(v)) = (tdec.next(), vdec.next()) {
            dst[i] = FloatValue {
                unix_nano: t,
                value: v,
            };
        } else {
            break;
        }
        i += 1;
    }

    // Did timestamp decoding have an error?
    tdec.err().context(TimestampSnafu)?;

    // Did int64 decoding have an error?
    vdec.err().context(FloatSnafu)?;

    // TIME_DECODER_POOLS.put(tdec);
    // FLOAT_DECODER_POOLS.put(vdec);

    dst.truncate(i);
    Ok(dst)
}

pub fn encode_bool_block(values: &[Value]) -> Result<Vec<u8>> {
    if values.is_empty() {
        return Ok(vec![]);
    }

    let mut tenc = get_time_encoder(values.len());
    let mut venc = get_bool_encoder(values.len());

    let b = encode_bool_block_using(values, &mut tenc, &mut venc)?;

    put_time_encoder(tenc);
    put_bool_encoder(venc);

    Ok(b)
}

pub fn encode_bool_block_using(
    values: &[Value],
    tenc: &mut TimeEncoder,
    venc: &mut BoolEncoder,
) -> Result<Vec<u8>> {
    tenc.reset();
    venc.reset();

    for v in values {
        if let Value::BoolValue(bv) = v {
            tenc.write(bv.unix_nano);
            venc.write(bv.value);
        }
    }

    // Encoded timestamp values
    let tb = tenc.bytes().context(TimestampSnafu)?;

    // Encoded int64 values
    let vb = venc.bytes().context(BoolSnafu)?;

    // Prepend the first timestamp of the block in the first 8 bytes
    Ok(pack_block(BLOCK_BOOL, tb, &vb))
}

/// Decodes the bool block from the byte slice and appends the integer values to a.
pub fn decode_bool_block(block: &[u8]) -> Result<Vec<BoolValue>> {
    let block_type = block[0];
    ensure!(
        block_type == BLOCK_BOOL,
        StringErrSnafu {
            reason: format!("invalid block type: exp {}, got {}", BLOCK_BOOL, block_type),
        }
    );

    let block = &block[1..];

    // The first 8 bytes is the minimum timestamp of the block
    let (tb, vb) = unpack_block(block)?;

    let sz = count_timestamps(tb);

    let mut dst = vec![BoolValue::default(); sz as usize];

    // let mut tdec = TIME_DECODER_POOLS.get(0);
    // let mut vdec = BOOL_DECODER_POOLS.get(0);
    let mut tdec = TimeDecoder::new();
    let mut vdec = BoolDecoder::new();

    // Setup timestamp and value decoders
    tdec.init(tb);
    vdec.set_bytes(vb).context(BoolSnafu)?;

    // Decode both a timestamp and value
    let mut i = 0;
    while i < dst.len() {
        if let (Some(t), Some(v)) = (tdec.next(), vdec.next()) {
            dst[i] = BoolValue {
                unix_nano: t,
                value: v,
            };
        } else {
            break;
        }
        i += 1;
    }

    // Did timestamp decoding have an error?
    tdec.err().context(TimestampSnafu)?;

    // Did int64 decoding have an error?
    vdec.err().context(BoolSnafu)?;

    // TIME_DECODER_POOLS.put(tdec);
    // BOOL_DECODER_POOLS.put(vdec);

    dst.truncate(i);
    Ok(dst)
}

pub fn encode_string_block(values: &[Value]) -> Result<Vec<u8>> {
    if values.is_empty() {
        return Ok(vec![]);
    }

    let mut tenc = get_time_encoder(values.len());
    let mut venc = get_string_encoder(values.len());

    let b = encode_string_block_using(values, &mut tenc, &mut venc)?;

    put_time_encoder(tenc);
    put_string_encoder(venc);

    Ok(b)
}

pub fn encode_string_block_using(
    values: &[Value],
    tenc: &mut TimeEncoder,
    venc: &mut StringEncoder,
) -> Result<Vec<u8>> {
    tenc.reset();
    venc.reset();

    for v in values {
        if let Value::StringValue(sv) = v {
            tenc.write(sv.unix_nano);
            venc.write(sv.value.as_bytes());
        }
    }

    // Encoded timestamp values
    let tb = tenc.bytes().context(TimestampSnafu)?;

    // Encoded int64 values
    let vb = venc.bytes().context(StringSnafu)?;

    // Prepend the first timestamp of the block in the first 8 bytes
    Ok(pack_block(BLOCK_STRING, tb, &vb))
}

/// Decodes the bool block from the byte slice and appends the integer values to a.
pub fn decode_string_block(block: &[u8]) -> Result<Vec<StringValue>> {
    let block_type = block[0];
    ensure!(
        block_type == BLOCK_STRING,
        StringErrSnafu {
            reason: format!(
                "invalid block type: exp {}, got {}",
                BLOCK_STRING, block_type
            ),
        }
    );

    let block = &block[1..];

    // The first 8 bytes is the minimum timestamp of the block
    let (tb, vb) = unpack_block(block)?;

    let sz = count_timestamps(tb);

    let mut dst = vec![StringValue::default(); sz as usize];

    // let mut tdec = TIME_DECODER_POOLS.get(0);
    // let mut vdec = STRING_DECODER_POOLS.get(0);
    let mut tdec = TimeDecoder::new();
    let mut vdec = StringDecoder::new();

    // Setup timestamp and value decoders
    tdec.init(tb);
    vdec.set_bytes(vb).context(StringSnafu)?;

    // Decode both a timestamp and value
    let mut i = 0;
    while i < dst.len() {
        if let (Some(t), Some(v)) = (tdec.next(), vdec.next()) {
            dst[i] = StringValue {
                unix_nano: t,
                value: v,
            };
        } else {
            break;
        }
        i += 1;
    }

    // Did timestamp decoding have an error?
    tdec.err().context(TimestampSnafu)?;

    // Did int64 decoding have an error?
    vdec.err().context(StringSnafu)?;

    // TIME_DECODER_POOLS.put(tdec);
    // STRING_DECODER_POOLS.put(vdec);

    dst.truncate(i);
    Ok(dst)
}

/// Returns the type of value encoded in a block or an error if the block type is unknown.
pub fn block_type(block: &[u8]) -> Result<BlockType> {
    let block_type = block[0];
    match block_type {
        BLOCK_INTEGER | BLOCK_UNSIGNED | BLOCK_FLOAT64 | BLOCK_BOOL | BLOCK_STRING => {
            Ok(block_type)
        }
        _ => Err(Error::StringErr {
            reason: format!("unknown block type: {}", block_type),
        }),
    }
}

/// Returns the number of timestamps encoded in block.
pub fn block_count(block: &[u8]) -> Result<i32> {
    ensure!(
        block.len() > ENCODED_BLOCK_HEADER_SIZE,
        StringErrSnafu {
            reason: format!(
                "count of short block: got {}, exp {}",
                block.len(),
                ENCODED_BLOCK_HEADER_SIZE
            ),
        }
    );

    // first byte is the block type
    let (tb, _) = unpack_block(&block[1..])?;

    Ok(count_timestamps(tb))
}

/// Takes a byte slice and decodes it into values of the appropriate type based on the block.
pub fn decode_block(block: &[u8]) -> Result<Vec<Value>> {
    ensure!(
        block.len() > ENCODED_BLOCK_HEADER_SIZE,
        StringErrSnafu {
            reason: format!(
                "count of short block: got {}, exp {}",
                block.len(),
                ENCODED_BLOCK_HEADER_SIZE
            ),
        }
    );

    let block_type = block_type(block)?;
    match block_type {
        BLOCK_INTEGER => {
            let decode = decode_integer_block(block)?;
            Ok(decode
                .into_iter()
                .map(Value::IntegerValue)
                .collect::<Vec<Value>>())
        }
        BLOCK_UNSIGNED => {
            let decode = decode_unsigned_block(block)?;
            Ok(decode
                .into_iter()
                .map(Value::UnsignedValue)
                .collect::<Vec<Value>>())
        }
        BLOCK_FLOAT64 => {
            let decode = decode_float_block(block)?;
            Ok(decode
                .into_iter()
                .map(Value::FloatValue)
                .collect::<Vec<Value>>())
        }
        BLOCK_BOOL => {
            let decode = decode_bool_block(block)?;
            Ok(decode
                .into_iter()
                .map(Value::BoolValue)
                .collect::<Vec<Value>>())
        }
        BLOCK_STRING => {
            let decode = decode_string_block(block)?;
            Ok(decode
                .into_iter()
                .map(Value::StringValue)
                .collect::<Vec<Value>>())
        }
        _ => Err(Error::StringErr {
            reason: format!("unknown block type: {}", block_type),
        }),
    }
}

pub fn pack_block(typ: u8, ts: &[u8], values: &[u8]) -> Vec<u8> {
    // We encode the length of the timestamp block using a variable byte encoding.
    // This allows small byte slices to take up 1 byte while larger ones use 2 or more.
    let sz = 1 + i64::MAX.required_space() + ts.len() + values.len();

    let mut dst = vec![0u8; sz];
    dst[0] = typ;

    let mut i = (ts.len() as u64).encode_var(&mut dst[1..1 + i64::MAX.required_space()]);
    i += 1;

    // block is <len timestamp bytes>, <ts bytes>, <value bytes>
    dst[i..i + ts.len()].copy_from_slice(ts);
    // We don't encode the value length because we know it's the rest of the block after
    // the timestamp block.
    dst[i + ts.len()..i + ts.len() + values.len()].copy_from_slice(values);

    dst.truncate(i + ts.len() + values.len());

    dst
}

pub fn unpack_block(buf: &[u8]) -> Result<(&[u8], &[u8])> {
    // Unpack the timestamp block length
    let (ts_len, n) = match u64::decode_var(buf) {
        Some((sz, n)) => (sz, n),
        None => {
            return Err(Error::StringErr {
                reason: "unpack_block: unable to read timestamp block length".to_string(),
            });
        }
    };
    if n == 0 {
        return Err(Error::StringErr {
            reason: "unpack_block: unable to read timestamp block length".to_string(),
        });
    }

    // Unpack the timestamp bytes
    let ts_idx = n + ts_len as usize;
    if ts_idx > buf.len() {
        return Err(Error::StringErr {
            reason: "unpack_block: not enough data for timestamp".to_string(),
        });
    }
    let ts = &buf[n..ts_idx];

    // Unpack the value bytes
    let values = &buf[ts_idx..];
    Ok((ts, values))
}

// unpack_vlog_index upacks the encoded block.
// Returns min_time,max_time,generation,sequence,offset,size
pub fn unpack_vlog_index(buf: &[u8]) -> Result<(i64, i64, i64, i64, i64, i64)> {
    ensure!(
        buf.len() == 48,
        StringErrSnafu {
            reason: format!(
                "unpack_vlog_index: invalid block length, exp 48, got {}",
                buf.len()
            ),
        }
    );

    // Unpack the min_time
    let (min_time, n) = match i64::decode_var(&buf[..8]) {
        Some((sz, n)) => (sz, n),
        None => {
            return Err(Error::StringErr {
                reason: "unpack_block: unable to read min_time".to_string(),
            });
        }
    };
    if n == 0 {
        return Err(Error::StringErr {
            reason: "unpack_block: unable to read min_time".to_string(),
        });
    }

    // Unpack the max_time
    let (max_time, n) = match i64::decode_var(&buf[8..16]) {
        Some((sz, n)) => (sz, n),
        None => {
            return Err(Error::StringErr {
                reason: "unpack_block: unable to read max_time".to_string(),
            });
        }
    };
    if n == 0 {
        return Err(Error::StringErr {
            reason: "unpack_block: unable to read max_time".to_string(),
        });
    }

    // Unpack the vlog generation
    let (generation, n) = match i64::decode_var(&buf[16..24]) {
        Some((sz, n)) => (sz, n),
        None => {
            return Err(Error::StringErr {
                reason: "unpack_block: unable to read vlog generation".to_string(),
            });
        }
    };
    if n == 0 {
        return Err(Error::StringErr {
            reason: "unpack_block: unable to read vlog generation".to_string(),
        });
    }

    // Unpack the vlog sequence
    let (sequence, n) = match i64::decode_var(&buf[24..32]) {
        Some((sz, n)) => (sz, n),
        None => {
            return Err(Error::StringErr {
                reason: "unpack_block: unable to read vlog sequence".to_string(),
            });
        }
    };
    if n == 0 {
        return Err(Error::StringErr {
            reason: "unpack_block: unable to read vlog sequence".to_string(),
        });
    }

    // Unpack the vlog offset
    let (offset, n) = match i64::decode_var(&buf[32..40]) {
        Some((sz, n)) => (sz, n),
        None => {
            return Err(Error::StringErr {
                reason: "unpack_block: unable to read vlog offset".to_string(),
            });
        }
    };
    if n == 0 {
        return Err(Error::StringErr {
            reason: "unpack_block: unable to read vlog offset".to_string(),
        });
    }

    // Unpack the vlog size
    let (size, n) = match i64::decode_var(&buf[40..48]) {
        Some((sz, n)) => (sz, n),
        None => {
            return Err(Error::StringErr {
                reason: "unpack_block: unable to read vlog size".to_string(),
            });
        }
    };
    if n == 0 {
        return Err(Error::StringErr {
            reason: "unpack_block: unable to read vlog size".to_string(),
        });
    }

    Ok((min_time, max_time, generation, sequence, offset, size))
}

/// Converts a int64 to a uint64 by zig zagging negative and positive values
/// across even and odd numbers.  Eg. [0,-1,1,-2] becomes [0, 1, 2, 3].
pub fn zig_zag_encode(x: i64) -> u64 {
    ((x << 1) as u64) ^ ((x >> 63) as u64)
}

/// Converts a previously zigzag encoded uint64 back to a int64.
pub fn zig_zag_decode(v: u64) -> i64 {
    ((v >> 1) ^ ((((v & 1) as i64) << 63) >> 63) as u64) as i64
}

#[cfg(any(test, feature = "benchmarking"))]
pub mod tests {
    use std::time::Duration;

    use crate::common::time::Time;
    use crate::models::points::{BoolValue, FloatValue, IntegerValue, StringValue, UnsignedValue};
    use chrono::SubsecRound;

    use super::{Value, decode_block};
    use crate::storage::tsm1::data_encoding::{
        encoding::{
            BLOCK_BOOL, BLOCK_FLOAT64, BLOCK_INTEGER, BLOCK_STRING, BLOCK_UNSIGNED, block_count,
            block_type,
        },
        encoding_values::Values,
    };

    #[test]
    fn test_encoding_float_block() {
        let value_count = 1000;
        let times = get_times(value_count, 60, 1_000_000_000); // 1 second precision in nanoseconds
        let mut values = vec![];
        for (i, t) in times.iter().enumerate() {
            values.push(Value::FloatValue(FloatValue::new(*t, i as f64)))
        }

        let exp = values.clone();

        let b = Values::encode(&values).unwrap();
        let decoded_values = decode_block(&b).unwrap();

        assert_eq!(exp, decoded_values);
    }

    #[test]
    fn test_encoding_float_block_zero_time() {
        let mut values = vec![];
        for i in 0..3 {
            values.push(Value::FloatValue(FloatValue::new(0, i as f64)))
        }

        let exp = values.clone();

        let b = Values::encode(&values).unwrap();
        let decoded_values = decode_block(&b).unwrap();

        assert_eq!(exp, decoded_values);
    }

    #[test]
    fn test_encoding_float_block_similar_floats() {
        let mut values = vec![];
        values.push(Value::FloatValue(FloatValue::new(
            1444238178437870000,
            6.00065e+06,
        )));
        values.push(Value::FloatValue(FloatValue::new(
            1444238185286830000,
            6.000656e+06,
        )));
        values.push(Value::FloatValue(FloatValue::new(
            1444238188441501000,
            6.000657e+06,
        )));
        values.push(Value::FloatValue(FloatValue::new(
            1444238195286811000,
            6.000659e+06,
        )));
        values.push(Value::FloatValue(FloatValue::new(
            1444238198439917000,
            6.000661e+06,
        )));

        let exp = values.clone();

        let b = Values::encode(&values).unwrap();
        let decoded_values = decode_block(&b).unwrap();

        assert_eq!(exp, decoded_values);
    }

    #[test]
    fn test_encoding_int_block_baisc() {
        let value_count = 1000;
        let times = get_times(value_count, 60, 1_000_000_000); // 1 second precision in nanoseconds
        let mut values = vec![];
        for (i, t) in times.iter().enumerate() {
            values.push(Value::IntegerValue(IntegerValue::new(*t, i as i64)))
        }

        let exp = values.clone();

        let b = Values::encode(&values).unwrap();
        let decoded_values = decode_block(&b).unwrap();

        assert_eq!(exp, decoded_values);
    }

    #[test]
    fn test_encoding_int_block_negatives() {
        let value_count = 1000;
        let times = get_times(value_count, 60, 1_000_000_000); // 1 second precision in nanoseconds
        let mut values = vec![];
        for (i, t) in times.iter().enumerate() {
            let mut v = i as i64;
            if i % 2 == 0 {
                v = -v;
            }
            values.push(Value::IntegerValue(IntegerValue::new(*t, v)))
        }

        let exp = values.clone();

        let b = Values::encode(&values).unwrap();
        let decoded_values = decode_block(&b).unwrap();

        assert_eq!(exp, decoded_values);
    }

    #[test]
    fn test_encoding_unsigned_block_baisc() {
        let value_count = 1000;
        let times = get_times(value_count, 60, 1_000_000_000); // 1 second precision in nanoseconds
        let mut values = vec![];
        for (i, t) in times.iter().enumerate() {
            values.push(Value::UnsignedValue(UnsignedValue::new(*t, i as u64)));
        }

        let exp = values.clone();

        let b = Values::encode(&values).unwrap();
        let decoded_values = decode_block(&b).unwrap();

        assert_eq!(exp, decoded_values);
    }

    #[test]
    fn test_encoding_unsigned_block_max_values() {
        let value_count = 1000;
        let times = get_times(value_count, 60, 1_000_000_000); // 1 second precision in nanoseconds
        let mut values = vec![];
        for (i, t) in times.iter().enumerate() {
            values.push(Value::UnsignedValue(UnsignedValue::new(*t, !(i as u64))));
        }

        let exp = values.clone();

        let b = Values::encode(&values).unwrap();
        let decoded_values = decode_block(&b).unwrap();

        assert_eq!(exp, decoded_values);
    }

    #[test]
    fn test_encoding_bool_block_baisc() {
        let value_count = 1000;
        let times = get_times(value_count, 60, 1_000_000_000); // 1 second precision in nanoseconds
        let mut values = vec![];
        for (i, t) in times.iter().enumerate() {
            let mut v = true;
            if i % 2 == 0 {
                v = false;
            }
            values.push(Value::BoolValue(BoolValue::new(*t, v)));
        }

        let exp = values.clone();

        let b = Values::encode(&values).unwrap();
        let decoded_values = decode_block(&b).unwrap();

        assert_eq!(exp, decoded_values);
    }

    #[test]
    fn test_encoding_string_block_baisc() {
        let value_count = 1000;
        let times = get_times(value_count, 60, 1_000_000_000); // 1 second precision in nanoseconds
        let mut values = vec![];
        for (i, t) in times.iter().enumerate() {
            values.push(Value::StringValue(StringValue::new(
                *t,
                format!("value {}", i),
            )));
        }

        let exp = values.clone();

        let b = Values::encode(&values).unwrap();
        let decoded_values = decode_block(&b).unwrap();

        assert_eq!(exp, decoded_values);
    }

    #[test]
    fn test_encoding_block_type() {
        // test float
        let values = Values(vec![Value::FloatValue(FloatValue::new(0, 1.0))]);
        let b = Values::encode(&values).unwrap();
        let bt = block_type(&b).unwrap();
        assert_eq!(
            BLOCK_FLOAT64, bt,
            "block type mismatch: exp {}, got {}",
            BLOCK_FLOAT64, bt
        );

        // test integer
        let values = Values(vec![Value::IntegerValue(IntegerValue::new(0, 1i64))]);
        let b = Values::encode(&values).unwrap();
        let bt = block_type(&b).unwrap();
        assert_eq!(
            BLOCK_INTEGER, bt,
            "block type mismatch: exp {}, got {}",
            BLOCK_INTEGER, bt
        );

        // test unsigned
        let values = Values(vec![Value::UnsignedValue(UnsignedValue::new(0, 1u64))]);
        let b = Values::encode(&values).unwrap();
        let bt = block_type(&b).unwrap();
        assert_eq!(
            BLOCK_UNSIGNED, bt,
            "block type mismatch: exp {}, got {}",
            BLOCK_UNSIGNED, bt
        );

        // test bool
        let values = Values(vec![Value::BoolValue(BoolValue::new(0, true))]);
        let b = Values::encode(&values).unwrap();
        let bt = block_type(&b).unwrap();
        assert_eq!(
            BLOCK_BOOL, bt,
            "block type mismatch: exp {}, got {}",
            BLOCK_BOOL, bt
        );

        // test string
        let values = Values(vec![Value::StringValue(StringValue::new(
            0,
            "string".to_string(),
        ))]);
        let b = Values::encode(&values).unwrap();
        let bt = block_type(&b).unwrap();
        assert_eq!(
            BLOCK_STRING, bt,
            "block type mismatch: exp {}, got {}",
            BLOCK_STRING, bt
        );
    }

    #[test]
    fn test_encoding_count() {
        // test float
        let values = Values(vec![Value::FloatValue(FloatValue::new(0, 1.0))]);
        let b = Values::encode(&values).unwrap();
        let cnt = block_count(&b).unwrap();
        assert_eq!(1, cnt, "block count mismatch: exp {}, got {}", 1, cnt);

        // test integer
        let values = Values(vec![Value::IntegerValue(IntegerValue::new(0, 1i64))]);
        let b = Values::encode(&values).unwrap();
        let cnt = block_count(&b).unwrap();
        assert_eq!(1, cnt, "block count mismatch: exp {}, got {}", 1, cnt);

        // test unsigned
        let values = Values(vec![Value::UnsignedValue(UnsignedValue::new(0, 1u64))]);
        let b = Values::encode(&values).unwrap();
        let cnt = block_count(&b).unwrap();
        assert_eq!(1, cnt, "block count mismatch: exp {}, got {}", 1, cnt);

        // test bool
        let values = Values(vec![Value::BoolValue(BoolValue::new(0, true))]);
        let b = Values::encode(&values).unwrap();
        let cnt = block_count(&b).unwrap();
        assert_eq!(1, cnt, "block count mismatch: exp {}, got {}", 1, cnt);

        // test string
        let values = Values(vec![Value::StringValue(StringValue::new(
            0,
            "string".to_string(),
        ))]);
        let b = Values::encode(&values).unwrap();
        let cnt = block_count(&b).unwrap();
        assert_eq!(1, cnt, "block count mismatch: exp {}, got {}", 1, cnt);
    }

    #[test]
    fn test_encoding_merge_float() {
        struct Case {
            a: Vec<Value>,
            b: Vec<Value>,
            exp: Vec<Value>,
        }
        let mut tests = vec![
            Case {
                a: vec![],
                b: vec![
                    Value::FloatValue(FloatValue::new(1, 1.2)),
                    Value::FloatValue(FloatValue::new(2, 2.2)),
                ],
                exp: vec![
                    Value::FloatValue(FloatValue::new(1, 1.2)),
                    Value::FloatValue(FloatValue::new(2, 2.2)),
                ],
            },
            Case {
                a: vec![
                    Value::FloatValue(FloatValue::new(1, 1.1)),
                    Value::FloatValue(FloatValue::new(2, 2.1)),
                ],
                b: vec![],
                exp: vec![
                    Value::FloatValue(FloatValue::new(1, 1.1)),
                    Value::FloatValue(FloatValue::new(2, 2.1)),
                ],
            },
            Case {
                a: vec![
                    Value::FloatValue(FloatValue::new(0, 0.0)),
                    Value::FloatValue(FloatValue::new(1, 1.1)),
                    Value::FloatValue(FloatValue::new(2, 2.1)),
                ],
                b: vec![
                    Value::FloatValue(FloatValue::new(2, 2.2)),
                    Value::FloatValue(FloatValue::new(2, 2.2)), // duplicate data
                ],
                exp: vec![
                    Value::FloatValue(FloatValue::new(0, 0.0)),
                    Value::FloatValue(FloatValue::new(1, 1.1)),
                    Value::FloatValue(FloatValue::new(2, 2.2)),
                ],
            },
            Case {
                a: vec![
                    Value::FloatValue(FloatValue::new(0, 0.0)),
                    Value::FloatValue(FloatValue::new(1, 1.1)),
                    Value::FloatValue(FloatValue::new(1, 1.1)), // duplicate data
                    Value::FloatValue(FloatValue::new(2, 2.1)),
                ],
                b: vec![
                    Value::FloatValue(FloatValue::new(2, 2.2)),
                    Value::FloatValue(FloatValue::new(2, 2.2)), // duplicate data
                ],
                exp: vec![
                    Value::FloatValue(FloatValue::new(0, 0.0)),
                    Value::FloatValue(FloatValue::new(1, 1.1)),
                    Value::FloatValue(FloatValue::new(2, 2.2)),
                ],
            },
            Case {
                a: vec![Value::FloatValue(FloatValue::new(1, 1.1))],
                b: vec![
                    Value::FloatValue(FloatValue::new(0, 0.0)),
                    Value::FloatValue(FloatValue::new(1, 1.2)), // overwrites a
                    Value::FloatValue(FloatValue::new(2, 2.2)),
                    Value::FloatValue(FloatValue::new(3, 3.2)),
                    Value::FloatValue(FloatValue::new(4, 4.2)),
                ],
                exp: vec![
                    Value::FloatValue(FloatValue::new(0, 0.0)),
                    Value::FloatValue(FloatValue::new(1, 1.2)),
                    Value::FloatValue(FloatValue::new(2, 2.2)),
                    Value::FloatValue(FloatValue::new(3, 3.2)),
                    Value::FloatValue(FloatValue::new(4, 4.2)),
                ],
            },
            Case {
                a: vec![
                    Value::FloatValue(FloatValue::new(1, 1.1)),
                    Value::FloatValue(FloatValue::new(2, 2.1)),
                    Value::FloatValue(FloatValue::new(3, 3.1)),
                    Value::FloatValue(FloatValue::new(4, 4.1)),
                ],
                b: vec![
                    Value::FloatValue(FloatValue::new(1, 1.2)),
                    Value::FloatValue(FloatValue::new(2, 2.2)),
                ],
                exp: vec![
                    Value::FloatValue(FloatValue::new(1, 1.2)),
                    Value::FloatValue(FloatValue::new(2, 2.2)),
                    Value::FloatValue(FloatValue::new(3, 3.1)),
                    Value::FloatValue(FloatValue::new(4, 4.1)),
                ],
            },
            Case {
                a: vec![
                    Value::FloatValue(FloatValue::new(1, 1.1)),
                    Value::FloatValue(FloatValue::new(2, 2.1)),
                    Value::FloatValue(FloatValue::new(3, 3.1)),
                    Value::FloatValue(FloatValue::new(4, 4.1)),
                ],
                b: vec![
                    Value::FloatValue(FloatValue::new(1, 1.2)),
                    Value::FloatValue(FloatValue::new(2, 2.2)),
                    Value::FloatValue(FloatValue::new(3, 3.2)),
                    Value::FloatValue(FloatValue::new(4, 4.2)),
                ],
                exp: vec![
                    Value::FloatValue(FloatValue::new(1, 1.2)),
                    Value::FloatValue(FloatValue::new(2, 2.2)),
                    Value::FloatValue(FloatValue::new(3, 3.2)),
                    Value::FloatValue(FloatValue::new(4, 4.2)),
                ],
            },
            Case {
                a: vec![
                    Value::FloatValue(FloatValue::new(0, 0.0)),
                    Value::FloatValue(FloatValue::new(1, 1.1)),
                    Value::FloatValue(FloatValue::new(2, 2.1)),
                    Value::FloatValue(FloatValue::new(3, 3.1)),
                    Value::FloatValue(FloatValue::new(4, 4.1)),
                ],
                b: vec![
                    Value::FloatValue(FloatValue::new(0, 0.0)),
                    Value::FloatValue(FloatValue::new(2, 2.2)),
                    Value::FloatValue(FloatValue::new(4, 4.2)),
                ],
                exp: vec![
                    Value::FloatValue(FloatValue::new(0, 0.0)),
                    Value::FloatValue(FloatValue::new(1, 1.1)),
                    Value::FloatValue(FloatValue::new(2, 2.2)),
                    Value::FloatValue(FloatValue::new(3, 3.1)),
                    Value::FloatValue(FloatValue::new(4, 4.2)),
                ],
            },
            Case {
                a: vec![
                    Value::FloatValue(FloatValue::new(1462498658242869207, 0.0)),
                    Value::FloatValue(FloatValue::new(1462498658288956853, 1.1)),
                ],
                b: vec![
                    Value::FloatValue(FloatValue::new(1462498658242870810, 0.0)),
                    Value::FloatValue(FloatValue::new(1462498658262911238, 2.2)),
                    Value::FloatValue(FloatValue::new(1462498658282415038, 4.2)),
                    Value::FloatValue(FloatValue::new(1462498658282417760, 4.2)),
                ],
                exp: vec![
                    Value::FloatValue(FloatValue::new(1462498658242869207, 0.0)),
                    Value::FloatValue(FloatValue::new(1462498658242870810, 0.0)),
                    Value::FloatValue(FloatValue::new(1462498658262911238, 2.2)),
                    Value::FloatValue(FloatValue::new(1462498658282415038, 4.2)),
                    Value::FloatValue(FloatValue::new(1462498658282417760, 4.2)),
                    Value::FloatValue(FloatValue::new(1462498658288956853, 1.1)),
                ],
            },
            Case {
                a: vec![
                    Value::FloatValue(FloatValue::new(4, 4.0)),
                    Value::FloatValue(FloatValue::new(5, 5.0)),
                    Value::FloatValue(FloatValue::new(6, 6.0)),
                ],
                b: vec![
                    Value::FloatValue(FloatValue::new(1, 1.0)),
                    Value::FloatValue(FloatValue::new(2, 2.0)),
                    Value::FloatValue(FloatValue::new(3, 3.0)),
                ],
                exp: vec![
                    Value::FloatValue(FloatValue::new(1, 1.0)),
                    Value::FloatValue(FloatValue::new(2, 2.0)),
                    Value::FloatValue(FloatValue::new(3, 3.0)),
                    Value::FloatValue(FloatValue::new(4, 4.0)),
                    Value::FloatValue(FloatValue::new(5, 5.0)),
                    Value::FloatValue(FloatValue::new(6, 6.0)),
                ],
            },
            Case {
                a: vec![
                    Value::FloatValue(FloatValue::new(5, 5.0)),
                    Value::FloatValue(FloatValue::new(6, 6.0)),
                ],
                b: vec![
                    Value::FloatValue(FloatValue::new(1, 1.0)),
                    Value::FloatValue(FloatValue::new(2, 2.0)),
                    Value::FloatValue(FloatValue::new(3, 3.0)),
                    Value::FloatValue(FloatValue::new(4, 4.0)),
                    Value::FloatValue(FloatValue::new(7, 7.0)),
                    Value::FloatValue(FloatValue::new(8, 8.0)),
                ],
                exp: vec![
                    Value::FloatValue(FloatValue::new(1, 1.0)),
                    Value::FloatValue(FloatValue::new(2, 2.0)),
                    Value::FloatValue(FloatValue::new(3, 3.0)),
                    Value::FloatValue(FloatValue::new(4, 4.0)),
                    Value::FloatValue(FloatValue::new(5, 5.0)),
                    Value::FloatValue(FloatValue::new(6, 6.0)),
                    Value::FloatValue(FloatValue::new(7, 7.0)),
                    Value::FloatValue(FloatValue::new(8, 8.0)),
                ],
            },
            Case {
                a: vec![
                    Value::FloatValue(FloatValue::new(1, 1.0)),
                    Value::FloatValue(FloatValue::new(2, 2.0)),
                    Value::FloatValue(FloatValue::new(3, 3.0)),
                ],
                b: vec![
                    Value::FloatValue(FloatValue::new(4, 4.0)),
                    Value::FloatValue(FloatValue::new(5, 5.0)),
                    Value::FloatValue(FloatValue::new(6, 6.0)),
                ],
                exp: vec![
                    Value::FloatValue(FloatValue::new(1, 1.0)),
                    Value::FloatValue(FloatValue::new(2, 2.0)),
                    Value::FloatValue(FloatValue::new(3, 3.0)),
                    Value::FloatValue(FloatValue::new(4, 4.0)),
                    Value::FloatValue(FloatValue::new(5, 5.0)),
                    Value::FloatValue(FloatValue::new(6, 6.0)),
                ],
            },
        ];

        for (i, mut t) in tests.into_iter().enumerate() {
            let mut a = Values(t.a.clone());
            let b = Values(t.b.clone());
            a.merge(b); // modify t.a
            let got = a;
            assert_eq!(
                t.exp.len(),
                got.len(),
                "test({}): value length mismatch: exp {:?}, got {:?}",
                i,
                t.exp,
                got
            );

            let mut ab = Values(t.a);
            ab.append(&mut t.b);
            ab.deduplicate(); // remove any values that have the same timestamp from ab
            let dedup = ab;

            for (i, _) in t.exp.iter().enumerate() {
                let exp = t.exp[i].to_string();
                let got = got[i].to_string();
                assert_eq!(
                    exp, got,
                    "value mismatch:
 exp {}
 got {}",
                    exp, got
                );

                let ded = dedup[i].to_string();
                assert_eq!(
                    exp, ded,
                    "value mismatch:
 exp {}
 got {}",
                    exp, ded
                );
            }
        }
    }

    #[test]
    fn test_encoding_merge_integer() {
        struct Case {
            a: Vec<Value>,
            b: Vec<Value>,
            exp: Vec<Value>,
        }
        let mut tests = vec![
            Case {
                a: vec![],
                b: vec![
                    Value::IntegerValue(IntegerValue::new(1, 10)),
                    Value::IntegerValue(IntegerValue::new(2, 20)),
                ],
                exp: vec![
                    Value::IntegerValue(IntegerValue::new(1, 10)),
                    Value::IntegerValue(IntegerValue::new(2, 20)),
                ],
            },
            Case {
                a: vec![
                    Value::IntegerValue(IntegerValue::new(1, 1)),
                    Value::IntegerValue(IntegerValue::new(2, 2)),
                ],
                b: vec![],
                exp: vec![
                    Value::IntegerValue(IntegerValue::new(1, 1)),
                    Value::IntegerValue(IntegerValue::new(2, 2)),
                ],
            },
            Case {
                a: vec![Value::IntegerValue(IntegerValue::new(1, 1))],
                b: vec![
                    Value::IntegerValue(IntegerValue::new(0, 0)),
                    Value::IntegerValue(IntegerValue::new(1, 10)),
                    Value::IntegerValue(IntegerValue::new(2, 20)),
                    Value::IntegerValue(IntegerValue::new(3, 30)),
                    Value::IntegerValue(IntegerValue::new(4, 40)),
                ],
                exp: vec![
                    Value::IntegerValue(IntegerValue::new(0, 0)),
                    Value::IntegerValue(IntegerValue::new(1, 10)),
                    Value::IntegerValue(IntegerValue::new(2, 20)),
                    Value::IntegerValue(IntegerValue::new(3, 30)),
                    Value::IntegerValue(IntegerValue::new(4, 40)),
                ],
            },
            Case {
                a: vec![
                    Value::IntegerValue(IntegerValue::new(1, 1)),
                    Value::IntegerValue(IntegerValue::new(2, 2)),
                    Value::IntegerValue(IntegerValue::new(3, 3)),
                    Value::IntegerValue(IntegerValue::new(4, 4)),
                ],
                b: vec![
                    Value::IntegerValue(IntegerValue::new(1, 10)),
                    Value::IntegerValue(IntegerValue::new(2, 20)),
                ],
                exp: vec![
                    Value::IntegerValue(IntegerValue::new(1, 10)),
                    Value::IntegerValue(IntegerValue::new(2, 20)),
                    Value::IntegerValue(IntegerValue::new(3, 3)),
                    Value::IntegerValue(IntegerValue::new(4, 4)),
                ],
            },
            Case {
                a: vec![
                    Value::IntegerValue(IntegerValue::new(1, 1)),
                    Value::IntegerValue(IntegerValue::new(2, 2)),
                    Value::IntegerValue(IntegerValue::new(3, 3)),
                    Value::IntegerValue(IntegerValue::new(4, 4)),
                ],
                b: vec![
                    Value::IntegerValue(IntegerValue::new(1, 10)),
                    Value::IntegerValue(IntegerValue::new(2, 20)),
                    Value::IntegerValue(IntegerValue::new(3, 30)),
                    Value::IntegerValue(IntegerValue::new(4, 40)),
                ],
                exp: vec![
                    Value::IntegerValue(IntegerValue::new(1, 10)),
                    Value::IntegerValue(IntegerValue::new(2, 20)),
                    Value::IntegerValue(IntegerValue::new(3, 30)),
                    Value::IntegerValue(IntegerValue::new(4, 40)),
                ],
            },
            Case {
                a: vec![
                    Value::IntegerValue(IntegerValue::new(0, 0)),
                    Value::IntegerValue(IntegerValue::new(1, 1)),
                    Value::IntegerValue(IntegerValue::new(2, 2)),
                    Value::IntegerValue(IntegerValue::new(3, 3)),
                    Value::IntegerValue(IntegerValue::new(4, 4)),
                ],
                b: vec![
                    Value::IntegerValue(IntegerValue::new(0, 0)),
                    Value::IntegerValue(IntegerValue::new(2, 20)),
                    Value::IntegerValue(IntegerValue::new(4, 40)),
                ],
                exp: vec![
                    Value::IntegerValue(IntegerValue::new(0, 0)),
                    Value::IntegerValue(IntegerValue::new(1, 1)),
                    Value::IntegerValue(IntegerValue::new(2, 20)),
                    Value::IntegerValue(IntegerValue::new(3, 3)),
                    Value::IntegerValue(IntegerValue::new(4, 40)),
                ],
            },
        ];

        for (i, mut t) in tests.into_iter().enumerate() {
            let mut a = Values(t.a.clone());
            let b = Values(t.b.clone());
            a.merge(b); // modify t.a
            let got = a;
            assert_eq!(
                t.exp.len(),
                got.len(),
                "test({}): value length mismatch: exp {:?}, got {:?}",
                i,
                t.exp,
                got
            );

            let mut ab = Values(t.a);
            ab.append(&mut t.b);
            ab.deduplicate(); // remove any values that have the same timestamp from ab
            let dedup = ab;

            for (i, _) in t.exp.iter().enumerate() {
                let exp = t.exp[i].to_string();
                let got = got[i].to_string();
                assert_eq!(
                    exp, got,
                    "value mismatch:
 exp {}
 got {}",
                    exp, got
                );

                let ded = dedup[i].to_string();
                assert_eq!(
                    exp, ded,
                    "value mismatch:
 exp {}
 got {}",
                    exp, ded
                );
            }
        }
    }

    #[test]
    fn test_encoding_merge_unsigned() {
        struct Case {
            a: Vec<Value>,
            b: Vec<Value>,
            exp: Vec<Value>,
        }
        let mut tests = vec![
            Case {
                a: vec![],
                b: vec![
                    Value::UnsignedValue(UnsignedValue::new(1, 10)),
                    Value::UnsignedValue(UnsignedValue::new(2, 20)),
                ],
                exp: vec![
                    Value::UnsignedValue(UnsignedValue::new(1, 10)),
                    Value::UnsignedValue(UnsignedValue::new(2, 20)),
                ],
            },
            Case {
                a: vec![
                    Value::UnsignedValue(UnsignedValue::new(1, 1)),
                    Value::UnsignedValue(UnsignedValue::new(2, 2)),
                ],
                b: vec![],
                exp: vec![
                    Value::UnsignedValue(UnsignedValue::new(1, 1)),
                    Value::UnsignedValue(UnsignedValue::new(2, 2)),
                ],
            },
            Case {
                a: vec![Value::UnsignedValue(UnsignedValue::new(1, 1))],
                b: vec![
                    Value::UnsignedValue(UnsignedValue::new(0, 0)),
                    Value::UnsignedValue(UnsignedValue::new(1, 10)),
                    Value::UnsignedValue(UnsignedValue::new(2, 20)),
                    Value::UnsignedValue(UnsignedValue::new(3, 30)),
                    Value::UnsignedValue(UnsignedValue::new(4, 40)),
                ],
                exp: vec![
                    Value::UnsignedValue(UnsignedValue::new(0, 0)),
                    Value::UnsignedValue(UnsignedValue::new(1, 10)),
                    Value::UnsignedValue(UnsignedValue::new(2, 20)),
                    Value::UnsignedValue(UnsignedValue::new(3, 30)),
                    Value::UnsignedValue(UnsignedValue::new(4, 40)),
                ],
            },
            Case {
                a: vec![
                    Value::UnsignedValue(UnsignedValue::new(1, 1)),
                    Value::UnsignedValue(UnsignedValue::new(2, 2)),
                    Value::UnsignedValue(UnsignedValue::new(3, 3)),
                    Value::UnsignedValue(UnsignedValue::new(4, 4)),
                ],
                b: vec![
                    Value::UnsignedValue(UnsignedValue::new(1, !0u64)),
                    Value::UnsignedValue(UnsignedValue::new(2, 20)),
                ],
                exp: vec![
                    Value::UnsignedValue(UnsignedValue::new(1, !0u64)),
                    Value::UnsignedValue(UnsignedValue::new(2, 20)),
                    Value::UnsignedValue(UnsignedValue::new(3, 3)),
                    Value::UnsignedValue(UnsignedValue::new(4, 4)),
                ],
            },
            Case {
                a: vec![
                    Value::UnsignedValue(UnsignedValue::new(1, 1)),
                    Value::UnsignedValue(UnsignedValue::new(2, 2)),
                    Value::UnsignedValue(UnsignedValue::new(3, 3)),
                    Value::UnsignedValue(UnsignedValue::new(4, 4)),
                ],
                b: vec![
                    Value::UnsignedValue(UnsignedValue::new(1, 10)),
                    Value::UnsignedValue(UnsignedValue::new(2, 20)),
                    Value::UnsignedValue(UnsignedValue::new(3, 30)),
                    Value::UnsignedValue(UnsignedValue::new(4, 40)),
                ],
                exp: vec![
                    Value::UnsignedValue(UnsignedValue::new(1, 10)),
                    Value::UnsignedValue(UnsignedValue::new(2, 20)),
                    Value::UnsignedValue(UnsignedValue::new(3, 30)),
                    Value::UnsignedValue(UnsignedValue::new(4, 40)),
                ],
            },
            Case {
                a: vec![
                    Value::UnsignedValue(UnsignedValue::new(0, 0)),
                    Value::UnsignedValue(UnsignedValue::new(1, 1)),
                    Value::UnsignedValue(UnsignedValue::new(2, 2)),
                    Value::UnsignedValue(UnsignedValue::new(3, 3)),
                    Value::UnsignedValue(UnsignedValue::new(4, 4)),
                ],
                b: vec![
                    Value::UnsignedValue(UnsignedValue::new(0, 0)),
                    Value::UnsignedValue(UnsignedValue::new(2, 20)),
                    Value::UnsignedValue(UnsignedValue::new(4, 40)),
                ],
                exp: vec![
                    Value::UnsignedValue(UnsignedValue::new(0, 0)),
                    Value::UnsignedValue(UnsignedValue::new(1, 1)),
                    Value::UnsignedValue(UnsignedValue::new(2, 20)),
                    Value::UnsignedValue(UnsignedValue::new(3, 3)),
                    Value::UnsignedValue(UnsignedValue::new(4, 40)),
                ],
            },
        ];

        for (i, mut t) in tests.into_iter().enumerate() {
            let mut a = Values(t.a.clone());
            let b = Values(t.b.clone());
            a.merge(b); // modify t.a
            let got = a;
            assert_eq!(
                t.exp.len(),
                got.len(),
                "test({}): value length mismatch: exp {:?}, got {:?}",
                i,
                t.exp,
                got
            );

            let mut ab = Values(t.a);
            ab.append(&mut t.b);
            ab.deduplicate(); // remove any values that have the same timestamp from ab
            let dedup = ab;

            for (i, _) in t.exp.iter().enumerate() {
                let exp = t.exp[i].to_string();
                let got = got[i].to_string();
                assert_eq!(
                    exp, got,
                    "value mismatch:
 exp {}
 got {}",
                    exp, got
                );

                let ded = dedup[i].to_string();
                assert_eq!(
                    exp, ded,
                    "value mismatch:
 exp {}
 got {}",
                    exp, ded
                );
            }
        }
    }

    #[test]
    fn test_encoding_merge_bool() {
        struct Case {
            a: Vec<Value>,
            b: Vec<Value>,
            exp: Vec<Value>,
        }
        let mut tests = vec![
            Case {
                a: vec![],
                b: vec![
                    Value::BoolValue(BoolValue::new(1, true)),
                    Value::BoolValue(BoolValue::new(2, true)),
                ],
                exp: vec![
                    Value::BoolValue(BoolValue::new(1, true)),
                    Value::BoolValue(BoolValue::new(2, true)),
                ],
            },
            Case {
                a: vec![
                    Value::BoolValue(BoolValue::new(1, true)),
                    Value::BoolValue(BoolValue::new(2, true)),
                ],
                b: vec![],
                exp: vec![
                    Value::BoolValue(BoolValue::new(1, true)),
                    Value::BoolValue(BoolValue::new(2, true)),
                ],
            },
            Case {
                a: vec![Value::BoolValue(BoolValue::new(1, true))],
                b: vec![
                    Value::BoolValue(BoolValue::new(0, false)),
                    Value::BoolValue(BoolValue::new(1, false)),
                    Value::BoolValue(BoolValue::new(2, false)),
                    Value::BoolValue(BoolValue::new(3, false)),
                    Value::BoolValue(BoolValue::new(4, false)),
                ],
                exp: vec![
                    Value::BoolValue(BoolValue::new(0, false)),
                    Value::BoolValue(BoolValue::new(1, false)),
                    Value::BoolValue(BoolValue::new(2, false)),
                    Value::BoolValue(BoolValue::new(3, false)),
                    Value::BoolValue(BoolValue::new(4, false)),
                ],
            },
            Case {
                a: vec![
                    Value::BoolValue(BoolValue::new(1, true)),
                    Value::BoolValue(BoolValue::new(2, true)),
                    Value::BoolValue(BoolValue::new(3, true)),
                    Value::BoolValue(BoolValue::new(4, true)),
                ],
                b: vec![
                    Value::BoolValue(BoolValue::new(1, false)),
                    Value::BoolValue(BoolValue::new(2, false)),
                ],
                exp: vec![
                    Value::BoolValue(BoolValue::new(1, false)),
                    Value::BoolValue(BoolValue::new(2, false)),
                    Value::BoolValue(BoolValue::new(3, true)),
                    Value::BoolValue(BoolValue::new(4, true)),
                ],
            },
            Case {
                a: vec![
                    Value::BoolValue(BoolValue::new(1, true)),
                    Value::BoolValue(BoolValue::new(2, true)),
                    Value::BoolValue(BoolValue::new(3, true)),
                    Value::BoolValue(BoolValue::new(4, true)),
                ],
                b: vec![
                    Value::BoolValue(BoolValue::new(1, false)),
                    Value::BoolValue(BoolValue::new(2, false)),
                    Value::BoolValue(BoolValue::new(3, false)),
                    Value::BoolValue(BoolValue::new(4, false)),
                ],
                exp: vec![
                    Value::BoolValue(BoolValue::new(1, false)),
                    Value::BoolValue(BoolValue::new(2, false)),
                    Value::BoolValue(BoolValue::new(3, false)),
                    Value::BoolValue(BoolValue::new(4, false)),
                ],
            },
            Case {
                a: vec![
                    Value::BoolValue(BoolValue::new(0, true)),
                    Value::BoolValue(BoolValue::new(1, true)),
                    Value::BoolValue(BoolValue::new(2, true)),
                    Value::BoolValue(BoolValue::new(3, true)),
                    Value::BoolValue(BoolValue::new(4, true)),
                ],
                b: vec![
                    Value::BoolValue(BoolValue::new(0, false)),
                    Value::BoolValue(BoolValue::new(2, false)),
                    Value::BoolValue(BoolValue::new(4, false)),
                ],
                exp: vec![
                    Value::BoolValue(BoolValue::new(0, false)),
                    Value::BoolValue(BoolValue::new(1, true)),
                    Value::BoolValue(BoolValue::new(2, false)),
                    Value::BoolValue(BoolValue::new(3, true)),
                    Value::BoolValue(BoolValue::new(4, false)),
                ],
            },
        ];

        for (i, mut t) in tests.into_iter().enumerate() {
            let mut a = Values(t.a.clone());
            let b = Values(t.b.clone());
            a.merge(b); // modify t.a
            let got = a;
            assert_eq!(
                t.exp.len(),
                got.len(),
                "test({}): value length mismatch: exp {:?}, got {:?}",
                i,
                t.exp,
                got
            );

            let mut ab = Values(t.a);
            ab.append(&mut t.b);
            ab.deduplicate(); // remove any values that have the same timestamp from ab
            let dedup = ab;

            for (i, _) in t.exp.iter().enumerate() {
                let exp = t.exp[i].to_string();
                let got = got[i].to_string();
                assert_eq!(
                    exp, got,
                    "value mismatch:
 exp {}
 got {}",
                    exp, got
                );

                let ded = dedup[i].to_string();
                assert_eq!(
                    exp, ded,
                    "value mismatch:
 exp {}
 got {}",
                    exp, ded
                );
            }
        }
    }

    #[test]
    fn test_encoding_merge_string() {
        struct Case {
            a: Vec<Value>,
            b: Vec<Value>,
            exp: Vec<Value>,
        }
        let tests = vec![
            Case {
                a: vec![],
                b: vec![
                    Value::StringValue(StringValue::new(1, "10".to_string())),
                    Value::StringValue(StringValue::new(2, "20".to_string())),
                ],
                exp: vec![
                    Value::StringValue(StringValue::new(1, "10".to_string())),
                    Value::StringValue(StringValue::new(2, "20".to_string())),
                ],
            },
            Case {
                a: vec![
                    Value::StringValue(StringValue::new(1, "1".to_string())),
                    Value::StringValue(StringValue::new(2, "2".to_string())),
                ],
                b: vec![],
                exp: vec![
                    Value::StringValue(StringValue::new(1, "1".to_string())),
                    Value::StringValue(StringValue::new(2, "2".to_string())),
                ],
            },
            Case {
                a: vec![Value::StringValue(StringValue::new(1, "1".to_string()))],
                b: vec![
                    Value::StringValue(StringValue::new(0, "0".to_string())),
                    Value::StringValue(StringValue::new(1, "10".to_string())),
                    Value::StringValue(StringValue::new(2, "20".to_string())),
                    Value::StringValue(StringValue::new(3, "30".to_string())),
                    Value::StringValue(StringValue::new(4, "40".to_string())),
                ],
                exp: vec![
                    Value::StringValue(StringValue::new(0, "0".to_string())),
                    Value::StringValue(StringValue::new(1, "10".to_string())),
                    Value::StringValue(StringValue::new(2, "20".to_string())),
                    Value::StringValue(StringValue::new(3, "30".to_string())),
                    Value::StringValue(StringValue::new(4, "40".to_string())),
                ],
            },
            Case {
                a: vec![
                    Value::StringValue(StringValue::new(1, "1".to_string())),
                    Value::StringValue(StringValue::new(2, "2".to_string())),
                    Value::StringValue(StringValue::new(3, "3".to_string())),
                    Value::StringValue(StringValue::new(4, "4".to_string())),
                ],
                b: vec![
                    Value::StringValue(StringValue::new(1, "10".to_string())),
                    Value::StringValue(StringValue::new(2, "20".to_string())),
                ],
                exp: vec![
                    Value::StringValue(StringValue::new(1, "10".to_string())),
                    Value::StringValue(StringValue::new(2, "20".to_string())),
                    Value::StringValue(StringValue::new(3, "3".to_string())),
                    Value::StringValue(StringValue::new(4, "4".to_string())),
                ],
            },
            Case {
                a: vec![
                    Value::StringValue(StringValue::new(1, "1".to_string())),
                    Value::StringValue(StringValue::new(2, "2".to_string())),
                    Value::StringValue(StringValue::new(3, "3".to_string())),
                    Value::StringValue(StringValue::new(4, "4".to_string())),
                ],
                b: vec![
                    Value::StringValue(StringValue::new(1, "10".to_string())),
                    Value::StringValue(StringValue::new(2, "20".to_string())),
                    Value::StringValue(StringValue::new(3, "30".to_string())),
                    Value::StringValue(StringValue::new(4, "40".to_string())),
                ],
                exp: vec![
                    Value::StringValue(StringValue::new(1, "10".to_string())),
                    Value::StringValue(StringValue::new(2, "20".to_string())),
                    Value::StringValue(StringValue::new(3, "30".to_string())),
                    Value::StringValue(StringValue::new(4, "40".to_string())),
                ],
            },
            Case {
                a: vec![
                    Value::StringValue(StringValue::new(0, "0".to_string())),
                    Value::StringValue(StringValue::new(1, "1".to_string())),
                    Value::StringValue(StringValue::new(2, "2".to_string())),
                    Value::StringValue(StringValue::new(3, "3".to_string())),
                    Value::StringValue(StringValue::new(4, "4".to_string())),
                ],
                b: vec![
                    Value::StringValue(StringValue::new(0, "0".to_string())),
                    Value::StringValue(StringValue::new(2, "20".to_string())),
                    Value::StringValue(StringValue::new(4, "40".to_string())),
                ],
                exp: vec![
                    Value::StringValue(StringValue::new(0, "0".to_string())),
                    Value::StringValue(StringValue::new(1, "1".to_string())),
                    Value::StringValue(StringValue::new(2, "20".to_string())),
                    Value::StringValue(StringValue::new(3, "3".to_string())),
                    Value::StringValue(StringValue::new(4, "40".to_string())),
                ],
            },
        ];

        for (i, mut t) in tests.into_iter().enumerate() {
            let mut a = Values(t.a.clone());
            let b = Values(t.b.clone());
            a.merge(b); // modify t.a
            let got = a;
            assert_eq!(
                t.exp.len(),
                got.len(),
                "test({}): value length mismatch: exp {:?}, got {:?}",
                i,
                t.exp,
                got
            );

            let mut ab = Values(t.a);
            ab.append(&mut t.b);
            ab.deduplicate(); // remove any values that have the same timestamp from ab
            let dedup = ab;

            for (j, _) in t.exp.iter().enumerate() {
                let exp = t.exp[j].to_string();
                let got = got[j].to_string();
                assert_eq!(
                    exp, got,
                    "test({}/{}): value mismatch:
 exp {}
 got {}",
                    i, j, exp, got
                );

                let ded = dedup[j].to_string();
                assert_eq!(
                    exp, ded,
                    "test({}/{}): value mismatch:
 exp {}
 got {}",
                    i, j, exp, ded
                );
            }
        }
    }

    pub fn get_times(n: usize, step: i64, precision: i64) -> Vec<i64> {
        let t = Time::now().timestamp_nanos() / precision * precision;
        let mut a = Vec::with_capacity(n);
        for i in 0..n {
            a.push(t + (i as i64 * step * precision));
        }
        a
    }
}
