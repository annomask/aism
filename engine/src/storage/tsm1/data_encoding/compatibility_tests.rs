use crate::storage::tsm1::data_encoding::{
    batch_bool::{bool_array_decode_all, bool_array_encode_all},
    batch_float::{float_array_decode_all, float_array_encode_all},
    batch_int::{
        integer_array_decode_all, integer_array_encode_all, unsigned_array_decode_all,
        unsigned_array_encode_all,
    },
    batch_string::{string_array_decode_all, string_array_encode_all},
    batch_timestamp::{time_array_decode_all, time_array_encode_all},
    float::FloatEncoder,
    int::{IntegerDecoder, IntegerEncoder},
    simple8b,
    timestamp::{TimeDecoder, TimeEncoder},
};

fn hex_bytes(hex: &str) -> Vec<u8> {
    assert_eq!(hex.len() % 2, 0);
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).unwrap();
            u8::from_str_radix(pair, 16).unwrap()
        })
        .collect()
}

fn encode_integer(values: &[i64]) -> Vec<u8> {
    let mut values = values.to_vec();
    integer_array_encode_all(&mut values).unwrap()
}

fn encode_unsigned(values: &[u64]) -> Vec<u8> {
    let mut values = values.to_vec();
    unsigned_array_encode_all(&mut values).unwrap()
}

fn encode_time(values: &[i64]) -> Vec<u8> {
    let mut values = values.to_vec();
    time_array_encode_all(&mut values).unwrap()
}

fn decode_stateful_integer(bytes: &[u8]) -> Vec<i64> {
    let mut decoder = IntegerDecoder::new();
    decoder.set_bytes(bytes).unwrap();
    let values = decoder.by_ref().collect();
    decoder.err().unwrap();
    values
}

fn decode_stateful_time(bytes: &[u8]) -> Vec<i64> {
    let mut decoder = TimeDecoder::new();
    decoder.init(bytes);
    let values = decoder.by_ref().collect();
    decoder.err().unwrap();
    values
}

#[test]
fn batch_integer_simple_prefix_sum_wraps_like_influxdb() {
    let values = [i64::MAX, i64::MIN, i64::MIN + 2];
    let bytes = encode_integer(&values);

    assert_eq!(bytes, hex_bytes("10fffffffffffffffee000000100000002"));
    assert_eq!(integer_array_decode_all(&bytes).unwrap(), values);
}

#[test]
fn batch_integer_rle_accumulation_wraps_like_influxdb() {
    let add_values = [i64::MAX, i64::MIN, i64::MIN + 1];
    let add_bytes = encode_integer(&add_values);
    assert_eq!(add_bytes, hex_bytes("20fffffffffffffffe0202"));
    assert_eq!(integer_array_decode_all(&add_bytes).unwrap(), add_values);

    let repeated_delta = [0, 1 << 62, i64::MIN, -(1 << 62), 0];
    let repeated_bytes = encode_integer(&repeated_delta);
    assert_eq!(
        repeated_bytes,
        hex_bytes("2000000000000000008080808080808080800104")
    );
    assert_eq!(
        integer_array_decode_all(&repeated_bytes).unwrap(),
        repeated_delta
    );
}

#[test]
fn stateful_integer_rle_multiply_and_add_wrap_like_influxdb() {
    for values in [
        vec![i64::MAX, i64::MIN, i64::MIN + 1],
        vec![0, 1 << 62, i64::MIN, -(1 << 62), 0],
    ] {
        let mut encoder = IntegerEncoder::new(values.len() as i64);
        for value in &values {
            encoder.write(*value);
        }
        let bytes = encoder.bytes().unwrap();
        assert_eq!(decode_stateful_integer(&bytes), values);
    }
}

#[test]
fn batch_unsigned_reinterpretation_wraps_like_influxdb() {
    let rle_values = [i64::MAX as u64, i64::MAX as u64 + 1, i64::MAX as u64 + 2];
    let rle_bytes = encode_unsigned(&rle_values);
    assert_eq!(rle_bytes, hex_bytes("20fffffffffffffffe0202"));
    assert_eq!(unsigned_array_decode_all(&rle_bytes).unwrap(), rle_values);

    let simple_values = [i64::MAX as u64, i64::MAX as u64 + 1, i64::MAX as u64 + 3];
    let simple_bytes = encode_unsigned(&simple_values);
    assert_eq!(
        simple_bytes,
        hex_bytes("10fffffffffffffffee000000100000002")
    );
    assert_eq!(
        unsigned_array_decode_all(&simple_bytes).unwrap(),
        simple_values
    );

    let edge_values = [0, 1, i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX];
    let edge_bytes = encode_unsigned(&edge_values);
    assert_eq!(
        edge_bytes,
        hex_bytes(
            "0000000000000000000000000000000002fffffffffffffffc0000000000000002fffffffffffffffe"
        )
    );
    assert_eq!(unsigned_array_decode_all(&edge_bytes).unwrap(), edge_values);
}

#[test]
fn batch_timestamp_rle_accumulation_wraps_like_influxdb() {
    let values = [-30, -20, -10, 0, 10];
    let bytes = encode_time(&values);

    assert_eq!(bytes, hex_bytes("21ffffffffffffffe20105"));
    assert_eq!(time_array_decode_all(&bytes).unwrap(), values);
}

#[test]
fn stateful_timestamp_rle_accumulation_wraps_like_influxdb() {
    let values = [i64::MAX, i64::MIN, i64::MIN + 1];
    let mut encoder = TimeEncoder::new(values.len() as i64);
    for value in values {
        encoder.write(value);
    }
    let bytes = encoder.bytes().unwrap().to_vec();

    assert_eq!(bytes, hex_bytes("207fffffffffffffff0103"));
    assert_eq!(decode_stateful_time(&bytes), values);
}

#[test]
fn stateful_timestamp_packed_prefix_sum_wraps_like_influxdb() {
    let values = [-2, -1, 1];
    let mut encoder = TimeEncoder::new(values.len() as i64);
    for value in values {
        encoder.write(value);
    }
    let bytes = encoder.bytes().unwrap().to_vec();

    assert_eq!(bytes, hex_bytes("10fffffffffffffffee000000080000001"));
    assert_eq!(decode_stateful_time(&bytes), values);
}

#[test]
fn stateful_timestamp_rle_initial_value_wraps_like_influxdb() {
    let values = [i64::MIN, i64::MIN + 1, i64::MIN + 2];
    let mut encoder = TimeEncoder::new(values.len() as i64);
    for value in values {
        encoder.write(value);
    }
    let bytes = encoder.bytes().unwrap().to_vec();

    assert_eq!(bytes, hex_bytes("2080000000000000000103"));
    assert_eq!(decode_stateful_time(&bytes), values);
}

#[test]
fn simple8b_all_selectors_match_influxdb_golden_words() {
    let selectors = [
        (240, 1, 0x0000_0000_0000_0000, "0000000000000000"),
        (120, 1, 0x1000_0000_0000_0000, "1000000000000000"),
        (60, 1, 0x2fff_ffff_ffff_ffff, "2fffffffffffffff"),
        (30, 3, 0x3fff_ffff_ffff_ffff, "3fffffffffffffff"),
        (20, 7, 0x4fff_ffff_ffff_ffff, "4fffffffffffffff"),
        (15, 15, 0x5fff_ffff_ffff_ffff, "5fffffffffffffff"),
        (12, 31, 0x6fff_ffff_ffff_ffff, "6fffffffffffffff"),
        (10, 63, 0x7fff_ffff_ffff_ffff, "7fffffffffffffff"),
        (8, 127, 0x80ff_ffff_ffff_ffff, "80ffffffffffffff"),
        (7, 255, 0x90ff_ffff_ffff_ffff, "90ffffffffffffff"),
        (6, 1023, 0xafff_ffff_ffff_ffff, "afffffffffffffff"),
        (5, 4095, 0xbfff_ffff_ffff_ffff, "bfffffffffffffff"),
        (4, 32767, 0xcfff_ffff_ffff_ffff, "cfffffffffffffff"),
        (3, 1_048_575, 0xdfff_ffff_ffff_ffff, "dfffffffffffffff"),
        (2, 1_073_741_823, 0xefff_ffff_ffff_ffff, "efffffffffffffff"),
        (
            1,
            simple8b::MAX_VALUE,
            0xffff_ffff_ffff_ffff,
            "ffffffffffffffff",
        ),
    ];

    for (count, value, expected_word, expected_bytes) in selectors {
        let values = vec![value; count];
        let (word, encoded) = simple8b::encode(&values).unwrap();
        assert_eq!(encoded as usize, count);
        assert_eq!(word, expected_word);
        assert_eq!(word.to_be_bytes().to_vec(), hex_bytes(expected_bytes));

        let mut decoded = [0; 240];
        let decoded_count = simple8b::decode(&mut decoded, word).unwrap();
        assert_eq!(decoded_count, count);
        assert_eq!(&decoded[..decoded_count], values);
    }
}

#[test]
fn float_exact_bits_match_influxdb_golden_blocks() {
    let cases = [
        (
            vec![0.0, -0.0],
            "100000000000000000c00f001fff00000000000020",
        ),
        (
            vec![1.0, -1.0, f64::MIN_POSITIVE, f64::from_bits(1), f64::MAX],
            "103ff0000000000000c00f0177ff5eb0000000000001c3f7fefffffffffffff0017fffffffffffe0",
        ),
        (vec![f64::INFINITY], "107ff0000000000000d9a400000000000080"),
        (
            vec![f64::NEG_INFINITY],
            "10fff0000000000000c0040040000000000008",
        ),
    ];

    for (values, golden) in cases {
        let bytes = float_array_encode_all(&values).unwrap();
        assert_eq!(bytes, hex_bytes(golden));
        let decoded = float_array_decode_all(&bytes).unwrap();
        assert_eq!(
            decoded
                .iter()
                .map(|value| value.to_bits())
                .collect::<Vec<_>>(),
            values
                .iter()
                .map(|value| value.to_bits())
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn float_nan_policy_matches_influxdb() {
    for nan_bits in [0x7ff8_0000_0000_0001, 0x7ff8_0000_0000_0002] {
        let nan = f64::from_bits(nan_bits);
        for values in [vec![nan], vec![1.0, nan], vec![nan, 1.0]] {
            assert!(float_array_encode_all(&values).is_err());

            let mut encoder = FloatEncoder::new();
            for value in values {
                encoder.write(value);
            }
            encoder.flush();
            assert!(encoder.bytes().is_err());
        }
    }
}

#[test]
fn boolean_and_string_blocks_match_influxdb_golden_bytes() {
    for (bools, golden) in [
        (vec![], "1000"),
        (vec![false], "100100"),
        (vec![true], "100180"),
        ((0..7).map(|index| index % 2 == 0).collect(), "1007aa"),
        ((0..8).map(|index| index % 2 == 0).collect(), "1008aa"),
        ((0..9).map(|index| index % 2 == 0).collect(), "1009aa80"),
        ((0..16).map(|index| index % 2 == 0).collect(), "1010aaaa"),
    ] {
        let bytes = bool_array_encode_all(&bools).unwrap();
        assert_eq!(bytes, hex_bytes(golden));
        assert_eq!(bool_array_decode_all(&bytes).unwrap(), bools);
    }

    let string_cases = [
        (vec![], "1000"),
        (vec!["".to_string()], "10010000"),
        (
            vec!["hello".to_string(), "world".to_string()],
            "100c2c0568656c6c6f05776f726c64",
        ),
        (
            vec![
                "\u{4e2d}\u{6587}".to_string(),
                "caf\u{e9}".to_string(),
                "\u{1f30a}".to_string(),
            ],
            "10124406e4b8ade6968705636166c3a904f09f8c8a",
        ),
        (
            vec!["a\0b".to_string(), "\0".to_string()],
            "100614036100620100",
        ),
        (
            vec!["same".to_string(), "same".to_string(), "same".to_string()],
            "100f380473616d650473616d650473616d65",
        ),
        (
            vec!["\0".repeat(1024)],
            "10820808800800fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fe0100fa0100",
        ),
    ];
    for (strings, golden) in string_cases {
        let bytes = string_array_encode_all(&strings).unwrap();
        assert_eq!(bytes, hex_bytes(golden));
        assert_eq!(string_array_decode_all(&bytes).unwrap(), strings);
    }
}
