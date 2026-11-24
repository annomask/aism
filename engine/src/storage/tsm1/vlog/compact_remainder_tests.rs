use super::*;
use crate::models::points::{BoolValue, IntegerValue, StringValue, UnsignedValue};
use crate::storage::tsm1::vlog::vlog_index_cache::Cache as VLogIndexCache;
use std::ops::Range;

struct KeyCase {
    key: Vec<u8>,
    typ: u8,
    ordinal: usize,
}

fn key_cases(hosts: usize, types: &[u8]) -> Vec<KeyCase> {
    let mut cases = Vec::new();
    for host in 0..hosts {
        for (type_pos, typ) in types.iter().copied().enumerate() {
            for field in 0..2 {
                cases.push(KeyCase {
                    key: format!("cpu,host=host_{host}#!~#field_{type_pos}_{field}").into_bytes(),
                    typ,
                    ordinal: cases.len(),
                });
            }
        }
    }
    cases.sort_by(|left, right| left.key.cmp(&right.key));
    cases
}

fn value_for(case: &KeyCase, sample: usize) -> Value {
    let timestamp = 1_700_000_000_000_000_000 + sample as i64 * 10_000_000_000;
    let identity = case.ordinal as i64 * 1_000_000 + sample as i64;
    match case.typ {
        INTEGER => Value::IntegerValue(IntegerValue::new(timestamp, identity - 20_000_000)),
        UNSIGNED => Value::UnsignedValue(UnsignedValue::new(
            timestamp,
            (1_u64 << 54) + identity as u64,
        )),
        FLOAT => Value::FloatValue(FloatValue::new(timestamp, identity as f64 + 0.25)),
        BOOL => Value::BoolValue(BoolValue::new(
            timestamp,
            (sample + case.ordinal) % 2 == 0,
        )),
        STRING => Value::StringValue(StringValue::new(
            timestamp,
            format!("field-{}-sample-{sample}-\u{03bb}", case.ordinal),
        )),
        _ => panic!("unsupported fixture type"),
    }
}

fn assert_values_equal(actual: &[Value], expected: &[Value], key: &[u8]) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "wrong point count for {}",
        String::from_utf8_lossy(key)
    );
    for (position, (actual, expected)) in actual.iter().zip(expected).enumerate() {
        assert_eq!(
            actual,
            expected,
            "wrong value or field ownership at position {position} for {}",
            String::from_utf8_lossy(key)
        );
    }
}

fn assert_compaction_round_trip(
    cases: Vec<KeyCase>,
    input_ranges: &[Range<usize>],
    expected_sizes: Option<&[usize]>,
    tombstone: Option<Range<usize>>,
) {
    let dir = tempfile::tempdir().unwrap();
    let keys = cases
        .iter()
        .map(|case| (case.key.clone(), case.typ))
        .collect::<Vec<_>>();
    let mut readers = Vec::new();
    let mut indexes_by_key = HashMap::<String, Vec<VLogIndex>>::new();

    for (file_pos, range) in input_ranges.iter().enumerate() {
        let generation = file_pos as i64 + 1;
        let path = dir.path().join(format!(
            "{}.{}",
            default_format_file_name(generation, 1),
            VLOG_FILE_EXTENSION
        ));
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        let mut writer = VLogWriter::new(file, generation, 1, &path);
        let mut file_indexes = HashMap::<String, Vec<VLogIndex>>::new();
        for case in &cases {
            let values = range
                .clone()
                .map(|sample| value_for(case, sample))
                .collect::<Vec<_>>();
            let min = values.first().unwrap().unix_nano();
            let max = values.last().unwrap().unix_nano();
            let payload = Values::encode(&values).unwrap();
            let (offset, size) = writer.write_block(&case.key, min, max, &payload).unwrap();
            file_indexes.insert(
                String::from_utf8(case.key.clone()).unwrap(),
                vec![VLogIndex {
                    aggregate: None,
                    count: values.len() as i32,
                    typ: case.typ,
                    min,
                    max,
                    generation,
                    seq: 1,
                    offset,
                    size,
                }],
            );
        }
        writer.close().unwrap();
        let cache = Arc::new(VLogIndexCache::new(0));
        cache.write_multi(file_indexes.clone()).unwrap();
        let reader = Arc::new(
            VLogReader::new(
                fs::OpenOptions::new().read(true).open(&path).unwrap(),
                &path,
                generation,
                1,
                keys.clone(),
                cache,
            )
            .unwrap(),
        );
        if let Some(range) = &tombstone {
            let deleted_keys = cases.iter().map(|case| case.key.as_slice()).collect::<Vec<_>>();
            VLogReader::delete_range(
                Arc::clone(&reader),
                &deleted_keys,
                value_for(&cases[0], range.start).unix_nano(),
                value_for(&cases[0], range.end - 1).unix_nano(),
            )
            .unwrap();
        }
        for (key, mut indexes) in file_indexes {
            indexes_by_key.entry(key).or_default().append(&mut indexes);
        }
        readers.push(reader);
    }

    let info = VLogCompactInfo {
        keys: keys.clone(),
        indexes: cases
            .iter()
            .map(|case| indexes_by_key[std::str::from_utf8(&case.key).unwrap()].clone())
            .collect(),
        files: readers
            .iter()
            .map(|reader| reader.path().to_string_lossy().into_owned())
            .collect(),
        readers,
        aggregates: None,
    };
    // Force refill boundaries while retaining each key's complete native merge.
    let budget = info.indexes.iter().map(|indexes| {
        indexes.iter().map(|index| index.size as usize - 4).sum::<usize>()
    }).max().unwrap();
    let mut iter = futures::executor::block_on(VLogBatchKeyIter::new_with_prefetch_limit(
        4000, false, None, info, legacy_global_executor(), budget, None,
    )).unwrap();
    let mut output_blocks = Vec::new();
    let mut actual_by_key = BTreeMap::<Vec<u8>, Vec<Value>>::new();
    let mut sizes_by_key = BTreeMap::<Vec<u8>, Vec<usize>>::new();
    while let Some((count, typ, key, min, max, payload, error)) = iter.next() {
        assert!(error.is_none(), "unexpected compaction error: {error:?}");
        let values = encoding::decode_block(&payload).unwrap();
        assert!(!values.is_empty());
        assert_eq!(count as usize, values.len(), "incorrect emitted count for {key:?}");
        assert!(values.len() <= 4000, "fixture must exercise chunked output");
        assert_eq!(min, values.first().unwrap().unix_nano());
        assert_eq!(max, values.last().unwrap().unix_nano());
        assert!(values.iter().all(|value| value.value_type() == typ));
        assert!(values.windows(2).all(|pair| pair[0].unix_nano() < pair[1].unix_nano()));
        let key = (*key).clone();
        let previous = actual_by_key.entry(key.clone()).or_default();
        if let Some(last) = previous.last() {
            assert!(last.unix_nano() < min, "output blocks overlap for {key:?}");
        }
        previous.extend(values);
        sizes_by_key.entry(key.clone()).or_default().push(count as usize);
        output_blocks.push((key, typ, count, min, max, payload));
    }
    assert!(iter.err().is_none(), "unexpected final iterator error: {:?}", iter.err());
    assert!(iter.prefetch_peak_bytes <= budget);
    assert!(iter.prefetch_batches > 1);
    iter.close().unwrap();

    let mut sample_ids = input_ranges
        .iter()
        .cloned()
        .flatten()
        .collect::<Vec<_>>();
    sample_ids.sort_unstable();
    sample_ids.dedup();
    if let Some(range) = &tombstone {
        sample_ids.retain(|sample| !range.contains(sample));
    }
    assert_eq!(actual_by_key.len(), cases.len(), "compaction lost an entire key");
    for case in &cases {
        let expected = sample_ids
            .iter()
            .map(|sample| value_for(case, *sample))
            .collect::<Vec<_>>();
        assert_values_equal(&actual_by_key[&case.key], &expected, &case.key);
        if let Some(sizes) = expected_sizes {
            assert_eq!(sizes_by_key[&case.key], sizes, "incorrect chunk boundaries for {:?}", case.key);
        }
    }

    let output_path = dir.path().join(format!(
        "{}.{}",
        default_format_file_name(100, 1),
        VLOG_FILE_EXTENSION
    ));
    let output_file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&output_path)
        .unwrap();
    let mut writer = VLogWriter::new(output_file, 100, 1, &output_path);
    let mut output_indexes = HashMap::<String, Vec<VLogIndex>>::new();
    for (key, typ, count, min, max, payload) in output_blocks {
        let (offset, size) = writer.write_block(&key, min, max, &payload).unwrap();
        output_indexes
            .entry(String::from_utf8(key).unwrap())
            .or_default()
            .push(VLogIndex {
                aggregate: None,
                count,
                typ,
                min,
                max,
                generation: 100,
                seq: 1,
                offset,
                size,
            });
    }
    writer.close().unwrap();
    drop(writer);

    let reopened_cache = Arc::new(VLogIndexCache::new(0));
    reopened_cache.write_multi(output_indexes.clone()).unwrap();
    let reopened = VLogReader::new(
        fs::OpenOptions::new().read(true).open(&output_path).unwrap(),
        &output_path,
        100,
        1,
        keys,
        Arc::clone(&reopened_cache),
    )
    .unwrap();
    for case in &cases {
        let indexes = reopened_cache.values_by_gen_and_seq(&case.key, 100, 1);
        let mut values = Vec::new();
        for index in indexes.iter() {
            let (checksum, payload) = reopened
                .read_bytes(index.offset as u64, index.size as u64, &[])
                .unwrap();
            assert_eq!(checksum, crc32fast::hash(&payload));
            let block_values = encoding::decode_block(&payload).unwrap();
            assert_eq!(index.count as usize, block_values.len());
            assert_eq!(index.min, block_values.first().unwrap().unix_nano());
            assert_eq!(index.max, block_values.last().unwrap().unix_nano());
            assert_eq!(index.typ, case.typ);
            values.extend(block_values);
        }
        assert_values_equal(&values, &actual_by_key[&case.key], &case.key);
    }
    reopened.close().unwrap();
}

#[test]
fn compaction_drains_known_integer_remainder_before_the_next_field() {
    assert_compaction_round_trip(
        key_cases(1, &[INTEGER]),
        &[0..1321, 1321..2632, 2632..3957, 3957..5268],
        Some(&[4000, 1268]),
        None,
    );
}

#[test]
fn compaction_preserves_all_typed_tails_at_chunk_boundaries() {
    for (ranges, sizes) in [
        (vec![0..1999, 1999..4000], vec![4000]),
        (vec![0..2000, 2000..4001], vec![4000, 1]),
        (vec![0..2667, 2667..5334, 5334..8001], vec![4000, 4000, 1]),
    ] {
        assert_compaction_round_trip(
            key_cases(1, &[INTEGER, FLOAT, UNSIGNED, BOOL, STRING]),
            &ranges,
            Some(&sizes),
            None,
        );
    }
}

#[test]
fn compaction_drains_multiple_overflows_across_typed_fields_and_hosts() {
    assert_compaction_round_trip(
        key_cases(2, &[INTEGER, FLOAT, UNSIGNED, BOOL, STRING]),
        &[0..3500, 3500..7000, 7000..10500],
        Some(&[4000, 4000, 2500]),
        None,
    );
}

#[test]
fn compaction_preserves_overlap_deduplication_and_tombstones_with_remainders() {
    assert_compaction_round_trip(
        key_cases(1, &[INTEGER, FLOAT, UNSIGNED, BOOL, STRING]),
        &[0..3000, 2000..5000, 4000..8001],
        None,
        Some(3980..4021),
    );
}
