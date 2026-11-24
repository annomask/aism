use snafu::{ResultExt, Snafu, ensure};

use crate::storage::tsm1::{
    cursor::array_values::{BoolArray, FloatArray, IntegerArray, StringArray, UnsignedArray},
    data_encoding::{
        batch_bool::bool_array_decode_all,
        batch_float::float_array_decode_all_into,
        batch_int::{integer_array_decode_all_into, unsigned_array_decode_all_into},
        batch_string::string_array_decode_all,
        batch_timestamp::{time_array_decode_all, time_array_decode_all_into},
        encoding::{
            BLOCK_BOOL, BLOCK_FLOAT64, BLOCK_INTEGER, BLOCK_STRING, BLOCK_UNSIGNED, unpack_block,
            unpack_vlog_index,
        },
    },
    vlog::VLogIndex,
};

/// Error type for Integer.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    /// Encoding error.
    #[snafu(display("{}", source))]
    Encoding {
        source: crate::storage::tsm1::data_encoding::encoding::Error,
    },

    /// BatchTimestamp error.
    #[snafu(display("{}", source))]
    BatchTimestamp {
        source: crate::storage::tsm1::data_encoding::timestamp::Error,
    },

    /// BatchInt error.
    #[snafu(display("{}", source))]
    BatchInt {
        source: crate::storage::tsm1::data_encoding::int::Error,
    },

    /// BatchFloat error.
    #[snafu(display("{}", source))]
    BatchFloat {
        source: crate::storage::tsm1::data_encoding::float::Error,
    },

    /// BatchBool error.
    #[snafu(display("{}", source))]
    BatchBool {
        source: crate::storage::tsm1::data_encoding::bool::Error,
    },

    /// BatchString error.
    #[snafu(display("{}", source))]
    BatchString {
        source: crate::storage::tsm1::data_encoding::string::Error,
    },

    /// String error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Decodes only the timestamp payload from a typed TSM block.
pub fn decode_timestamp_array_block(block: &[u8], expected_block_type: u8) -> Result<Vec<i64>> {
    let mut times = Vec::new();
    decode_timestamp_array_block_into(block, expected_block_type, &mut times)?;
    Ok(times)
}

/// Reuses timestamp storage without decoding a field value payload.
pub(crate) fn decode_timestamp_array_block_into(
    block: &[u8],
    expected_block_type: u8,
    times: &mut Vec<i64>,
) -> Result<()> {
    let block_type = *block.first().ok_or_else(|| Error::StringErr {
        reason: "missing timestamp block type".into(),
    })?;
    ensure!(
        block_type == expected_block_type,
        StringErrSnafu {
            reason: format!(
                "invalid block type: exp {}, got {}",
                expected_block_type, block_type
            ),
        }
    );

    let (tb, _) = unpack_block(&block[1..]).context(EncodingSnafu)?;
    time_array_decode_all_into(tb, times).context(BatchTimestampSnafu)
}

// decode_integer_array_block decodes the integer block from the byte slice
// and writes the values to a.
pub fn decode_integer_array_block(block: &[u8], a: &mut IntegerArray) -> Result<()> {
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

    let (tb, vb) = unpack_block(&block[1..]).context(EncodingSnafu)?;

    let started = crate::common::slow_trace::now();
    time_array_decode_all_into(tb, &mut a.timestamps).context(BatchTimestampSnafu)?;
    crate::common::slow_trace::log_query_stage("query_timestamp_decode", started, "numeric block");
    let started = crate::common::slow_trace::now();
    integer_array_decode_all_into(vb, &mut a.values).context(BatchIntSnafu)?;
    crate::common::slow_trace::log_query_stage(
        "query_numeric_value_decode",
        started,
        "integer block",
    );

    Ok(())
}

// decode_unsigned_array_block decodes the unsigned block from the byte slice
// and writes the values to a.
pub fn decode_unsigned_array_block(block: &[u8], a: &mut UnsignedArray) -> Result<()> {
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

    let (tb, vb) = unpack_block(&block[1..]).context(EncodingSnafu)?;

    let started = crate::common::slow_trace::now();
    time_array_decode_all_into(tb, &mut a.timestamps).context(BatchTimestampSnafu)?;
    crate::common::slow_trace::log_query_stage("query_timestamp_decode", started, "numeric block");
    let started = crate::common::slow_trace::now();
    unsigned_array_decode_all_into(vb, &mut a.values).context(BatchIntSnafu)?;
    crate::common::slow_trace::log_query_stage(
        "query_numeric_value_decode",
        started,
        "unsigned block",
    );

    Ok(())
}

// decode_float_array_block decodes the float block from the byte slice
// and writes the values to a.
pub fn decode_float_array_block(block: &[u8], a: &mut FloatArray) -> Result<()> {
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

    let (tb, vb) = unpack_block(&block[1..]).context(EncodingSnafu)?;

    let started = crate::common::slow_trace::now();
    time_array_decode_all_into(tb, &mut a.timestamps).context(BatchTimestampSnafu)?;
    crate::common::slow_trace::log_query_stage("query_timestamp_decode", started, "numeric block");
    let started = crate::common::slow_trace::now();
    float_array_decode_all_into(vb, &mut a.values).context(BatchFloatSnafu)?;
    crate::common::slow_trace::log_query_stage(
        "query_numeric_value_decode",
        started,
        "float block",
    );

    Ok(())
}

// decode_bool_array_block decodes the bool block from the byte slice
// and writes the values to a.
pub fn decode_bool_array_block(block: &[u8], a: &mut BoolArray) -> Result<()> {
    let block_type = block[0];
    ensure!(
        block_type == BLOCK_BOOL,
        StringErrSnafu {
            reason: format!("invalid block type: exp {}, got {}", BLOCK_BOOL, block_type),
        }
    );

    let (tb, vb) = unpack_block(&block[1..]).context(EncodingSnafu)?;

    a.timestamps = time_array_decode_all(tb).context(BatchTimestampSnafu)?;
    a.values = bool_array_decode_all(vb).context(BatchBoolSnafu)?;

    Ok(())
}

// decode_string_array_block decodes the string block from the byte slice
// and writes the values to a.
pub fn decode_string_array_block(block: &[u8], a: &mut StringArray) -> Result<()> {
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

    let (tb, vb) = unpack_block(&block[1..]).context(EncodingSnafu)?;

    a.timestamps = time_array_decode_all(tb).context(BatchTimestampSnafu)?;
    a.values = string_array_decode_all(vb).context(BatchStringSnafu)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::models::points::FloatValue;

    use super::{
        decode_float_array_block, decode_string_array_block, decode_timestamp_array_block,
    };
    use crate::{
        storage::tsm1::cursor::array_values::{FloatArray, StringArray},
        storage::tsm1::data_encoding::{
            batch_timestamp::time_array_encode_all,
            encoding::{BLOCK_STRING, pack_block, tests::get_times},
            encoding_values::FloatValues,
        },
    };

    #[test]
    fn test_decode_timestamp_array_block_skips_invalid_string_values() {
        let expected = vec![10, 20, 30];
        let mut encoded_timestamps = expected.clone();
        let timestamp_payload = time_array_encode_all(&mut encoded_timestamps).unwrap();
        let block = pack_block(BLOCK_STRING, &timestamp_payload, &[0xff]);

        assert_eq!(
            decode_timestamp_array_block(&block, BLOCK_STRING).unwrap(),
            expected
        );
        assert!(decode_string_array_block(&block, &mut StringArray::default()).is_err());
    }

    #[test]
    fn timestamp_decode_reuses_storage_and_rejects_invalid_payloads() {
        let mut times = Vec::with_capacity(32);
        let pointer = times.as_ptr();
        for expected in [vec![-10, 0, 9], vec![4, 8]] {
            let mut input = expected.clone();
            let payload = time_array_encode_all(&mut input).unwrap();
            let block = pack_block(BLOCK_STRING, &payload, &[0xff]);
            super::decode_timestamp_array_block_into(&block, BLOCK_STRING, &mut times).unwrap();
            assert_eq!(times, expected);
            assert_eq!(times.as_ptr(), pointer);
            assert!(
                super::decode_timestamp_array_block_into(&block, BLOCK_STRING + 1, &mut times)
                    .is_err()
            );
        }
        for bytes in [&[][..], &[BLOCK_STRING][..], &[BLOCK_STRING, 255][..]] {
            assert!(
                super::decode_timestamp_array_block_into(bytes, BLOCK_STRING, &mut times).is_err()
            );
        }
    }

    #[test]
    fn test_decode_float_array_block() {
        let value_count = 1000;
        let times = get_times(value_count, 60, 1_000_000_000); // 1 second precision in nanoseconds

        let mut values = vec![];
        for (i, t) in times.iter().enumerate() {
            values.push(FloatValue::new(*t, i as f64));
        }

        let mut values = FloatValues(values);

        let exp = new_float_array_from_values(&values);

        let b = values.encode(&[]).unwrap();

        let mut got = FloatArray::new(exp.len());
        decode_float_array_block(&b, &mut got).unwrap();

        assert_eq!(
            exp, got,
            "unexpected values
exp={:?}
got={:?}",
            exp, got
        );
    }

    fn new_float_array_from_values(v: &FloatValues) -> FloatArray {
        let mut a = FloatArray::new(v.len());
        for (i, val) in v.iter().enumerate() {
            a.timestamps[i] = val.unix_nano;
            a.values[i] = val.value;
        }
        a
    }
}
