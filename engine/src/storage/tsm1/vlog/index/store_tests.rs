use super::super::{AggValues, Record, writer::IndexFileWriter};
use super::*;
use crate::models::{
    FLOAT, INTEGER,
    points::{IntegerValue, Value},
};
use crate::storage::tsm1::{data_encoding::encoding_values::Values, vlog::writer::VLogWriter};

#[test]
#[ignore = "diagnostic latency comparison; run without other workloads"]
fn aggregate_cache_conflict_benchmark() {
    use crate::models::points::FloatValue;
    use crate::query::storage_ir::{AggregateKind, AggregateProjection, TimeRange};
    use std::time::Instant;
    for typ in [INTEGER, FLOAT] {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("vlog");
        let store = IndexStore::open(&dir, &root.path().join("wal")).unwrap();
        let make = |time| {
            if typ == INTEGER {
                Value::IntegerValue(IntegerValue::new(time, 1))
            } else {
                Value::FloatValue(FloatValue::new(time, 1.0))
            }
        };
        super::super::raw_cursor_tests::install_values(
            &store,
            &dir,
            1,
            (0..64)
                .map(|block| (block * 160..(block + 1) * 160).map(make).collect())
                .collect(),
        );
        let projection = AggregateProjection::from_aggregates(&[AggregateKind::Avg]);
        for (name, times, range) in [
            ("clean", vec![], TimeRange::new(0, 10239)),
            ("outside_cache", vec![-10, 20000], TimeRange::new(0, 10239)),
            ("sparse_cache", vec![7, 10000], TimeRange::new(0, 10239)),
            ("boundary", vec![], TimeRange::new(3, 10237)),
        ] {
            let cache = times.into_iter().map(make).collect::<Vec<_>>();
            let mut samples = Vec::new();
            let mut decoded = 0;
            let mut summaries = 0;
            for sample in 0..524 {
                let start = Instant::now();
                let result = store
                    .aggregate_projected_windows(
                        SeqNo::new(1, 1).unwrap(),
                        typ,
                        &[range],
                        cache.clone(),
                        projection,
                    )
                    .unwrap();
                let elapsed = start.elapsed().as_secs_f64() * 1000.0;
                assert_eq!(
                    result.states[0].count,
                    Some((range.end_time - range.start_time + 1) as u64)
                );
                decoded = result.metrics.decoded_blocks;
                summaries = result.metrics.index_records;
                if sample >= 24 {
                    samples.push(elapsed);
                }
                std::hint::black_box(result);
            }
            samples.sort_by(f64::total_cmp);
            let percentile = |p: f64| {
                samples[((samples.len() as f64 * p).ceil() as usize - 1).min(samples.len() - 1)]
            };
            println!(
                "aggregate_bench type={typ} case={name} n={} avg_ms={:.6} p95_ms={:.6} p99_ms={:.6} p999_ms={:.6} p9999_ms={:.6} max_ms={:.6} decoded={decoded} summaries={summaries} points=10240 write_workers=0 query_workers=1 background=none warm=true",
                samples.len(),
                samples.iter().sum::<f64>() / samples.len() as f64,
                percentile(0.95),
                percentile(0.99),
                percentile(0.999),
                percentile(0.9999),
                samples.last().unwrap()
            );
        }
    }
}

#[test]
fn aggregate_integer_average_matches_wide_reference_across_storage_paths() {
    use crate::models::{UNSIGNED, points::UnsignedValue};
    use crate::query::storage_ir::{AggregateKind, AggregateProjection, AggregateState, TimeRange};
    for typ in [INTEGER, UNSIGNED] {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("vlog");
        let wal = root.path().join("wal");
        let store = IndexStore::open(&dir, &wal).unwrap();
        let make = |time: i64| {
            if typ == INTEGER {
                Value::IntegerValue(IntegerValue::new(time, i64::MAX - time))
            } else {
                Value::UnsignedValue(UnsignedValue::new(time, u64::MAX - time as u64))
            }
        };
        super::super::raw_cursor_tests::install_values(
            &store,
            &dir,
            1,
            (0..64)
                .map(|block| (block * 16..(block + 1) * 16).map(make).collect())
                .collect(),
        );
        drop(store);
        let store = IndexStore::open(&dir, &wal).unwrap();
        let seq = SeqNo::new(1, 1).unwrap();
        let projection = AggregateProjection::from_aggregates(&[AggregateKind::Avg]);
        for (ranges, overwritten) in [
            (vec![TimeRange::new(0, 1023)], false),
            (
                vec![TimeRange::new(0, 511), TimeRange::new(512, 1023)],
                false,
            ),
            (vec![TimeRange::new(3, 997)], false),
            (vec![TimeRange::new(3, 997)], true),
        ] {
            let cache = if overwritten {
                vec![if typ == INTEGER {
                    Value::IntegerValue(IntegerValue::new(7, i64::MIN))
                } else {
                    Value::UnsignedValue(UnsignedValue::new(7, 0))
                }]
            } else {
                vec![]
            };
            let result = store
                .aggregate_projected_windows(seq, typ, &ranges, cache, projection)
                .unwrap();
            for (actual, range) in result.states.iter().zip(&ranges) {
                let mut expected = AggregateState::empty(typ);
                for time in range.start_time..=range.end_time {
                    if typ == INTEGER {
                        expected.add_integer(if overwritten && time == 7 {
                            i64::MIN
                        } else {
                            i64::MAX - time
                        });
                    } else {
                        expected.add_unsigned(if overwritten && time == 7 {
                            0
                        } else {
                            u64::MAX - time as u64
                        });
                    }
                }
                assert_eq!(actual.wide_sum, expected.wide_sum);
                assert_eq!(actual.average(), expected.average());
            }
            assert!(result.metrics.index_records > 0);
            if ranges[0].start_time != 0 {
                assert!(result.metrics.decoded_blocks > 0);
            }
        }
        store
            .delete_ranges(vec![super::super::delete::DeleteRange {
                seq,
                min: 7,
                max: 7,
                cutoff: 0,
            }])
            .unwrap();
        store
            .compact_files(&store.index_readers(), 2, &|| false)
            .unwrap();
        drop(store);
        let store = IndexStore::open(&dir, &wal).unwrap();
        let result = store
            .aggregate_projected_windows(seq, typ, &[TimeRange::new(0, 1023)], vec![], projection)
            .unwrap();
        let mut expected = AggregateState::empty(typ);
        for time in (0..1024).filter(|time| *time != 7) {
            if typ == INTEGER {
                expected.add_integer(i64::MAX - time);
            } else {
                expected.add_unsigned(u64::MAX - time as u64);
            }
        }
        assert_eq!(result.states[0].wide_sum, expected.wide_sum);
        assert_eq!(result.states[0].average(), expected.average());
    }
}

#[test]
fn aggregate_cache_conflicts_use_actual_visible_timestamps() {
    use crate::query::storage_ir::{AggregateKind, AggregateProjection, TimeRange};
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("vlog");
    let store = IndexStore::open(&dir, &root.path().join("wal")).unwrap();
    super::super::raw_cursor_tests::install_blocks(&store, &dir, 1, 0, 64);
    let projection =
        AggregateProjection::from_aggregates(&[AggregateKind::Count, AggregateKind::Sum]);
    for (times, ranges, decoded) in [
        (vec![-10, 2000], vec![TimeRange::new(0, 1023)], 0),
        (vec![7, 1000], vec![TimeRange::new(0, 1023)], 2),
        (
            vec![500],
            vec![TimeRange::new(0, 15), TimeRange::new(1008, 1023)],
            0,
        ),
    ] {
        let cache = times
            .iter()
            .map(|t| Value::IntegerValue(IntegerValue::new(*t, 9)))
            .collect();
        let result = store
            .aggregate_projected_windows(
                SeqNo::new(1, 1).unwrap(),
                INTEGER,
                &ranges,
                cache,
                projection,
            )
            .unwrap();
        for (state, range) in result.states.iter().zip(&ranges) {
            let mut expected = crate::query::storage_ir::AggregateState::empty(INTEGER);
            for time in 0..1024 {
                if range.contains(time) {
                    expected.add_integer(if times.contains(&time) { 9 } else { 1 });
                }
            }
            assert_eq!(state.count, Some(expected.count));
            assert_eq!(state.wide_sum, expected.wide_sum);
            assert_eq!(state.average(), expected.average());
        }
        assert_eq!(result.metrics.decoded_blocks, decoded);
    }
}

#[test]
fn aggregate_directory_pruning_rejects_corrupt_time_and_type() {
    use crate::query::storage_ir::{AggregateKind, AggregateProjection, TimeRange};
    for (offset, replacement, time) in [
        (31, 11, 10),
        (39, 16, 20),
        (20, crate::models::UNSIGNED, 10),
    ] {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("vlog");
        let wal = root.path().join("wal");
        let store = IndexStore::open(&dir, &wal).unwrap();
        let values = vec![
            Value::IntegerValue(IntegerValue::new(10, 7)),
            Value::IntegerValue(IntegerValue::new(20, 8)),
        ];
        let (index, vlog) = batch_values(&dir, 1, 1, values);
        store.install(&[index], &[vlog], &[]).unwrap();
        let file = store.index_readers().remove(0);
        let path = file.path().to_path_buf();
        let byte = file.footer.dir_offset as usize + offset;
        drop(file);
        drop(store);
        let mut bytes = fs::read(&path).unwrap();
        bytes[byte] = replacement;
        fs::write(&path, bytes).unwrap();
        let store = IndexStore::open(&dir, &wal).unwrap();
        let projection = AggregateProjection::from_aggregates(&[AggregateKind::Count]);
        assert!(
            store
                .aggregate_projected_windows(
                    SeqNo::new(1, 1).unwrap(),
                    INTEGER,
                    &[TimeRange::new(time, time)],
                    vec![],
                    projection
                )
                .is_err()
        );
        assert!(
            store
                .raw_cursor(SeqNo::new(1, 1).unwrap(), INTEGER, time, time, true)
                .is_err()
        );
        assert!(
            store
                .delete_ranges(vec![super::super::delete::DeleteRange {
                    seq: SeqNo::new(1, 1).unwrap(),
                    min: time,
                    max: time,
                    cutoff: 0,
                }])
                .is_err()
        );
    }
}

pub(super) fn batch(
    dir: &Path,
    generation: i64,
    time: i64,
) -> (IndexFileMeta, (PathBuf, i64, i64)) {
    batch_at_sequence(dir, generation, 1, time)
}

fn batch_at_sequence(
    dir: &Path,
    generation: i64,
    sequence: i64,
    time: i64,
) -> (IndexFileMeta, (PathBuf, i64, i64)) {
    batch_values(
        dir,
        generation,
        sequence,
        vec![Value::IntegerValue(IntegerValue {
            unix_nano: time,
            value: generation,
        })],
    )
}

fn batch_values(
    dir: &Path,
    generation: i64,
    sequence: i64,
    values: Vec<Value>,
) -> (IndexFileMeta, (PathBuf, i64, i64)) {
    batch_values_with_format(
        dir,
        generation,
        sequence,
        values,
        super::super::directory::FORMAT_VERSION,
    )
}

fn batch_values_with_format(
    dir: &Path,
    generation: i64,
    sequence: i64,
    values: Vec<Value>,
    version: u16,
) -> (IndexFileMeta, (PathBuf, i64, i64)) {
    let path = dir.join(format!("{}.tmp", output_name(generation, sequence, "vlog")));
    let f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    let mut writer = VLogWriter::new(f, generation, sequence, &path);
    let min = values.first().unwrap().unix_nano();
    let max = values.last().unwrap().unix_nano();
    let block = Values::encode(&values).unwrap();
    let (offset, size) = writer
        .write_block(b"cpu#!~#value", min, max, &block)
        .unwrap();
    writer.close().unwrap();
    let record = Record::from_vlog_index(
        SeqNo::new(1, 1).unwrap(),
        &VLogIndex {
            aggregate: None,
            count: values.len() as i32,
            typ: INTEGER,
            min,
            max,
            generation,
            seq: sequence,
            offset,
            size,
        },
        AggValues::from_block(&values).unwrap(),
    )
    .unwrap();
    let idx = dir.join(format!("{}.tmp", output_name(generation, sequence, "idx")));
    let mut writer = if version < 4 {
        IndexFileWriter::create_legacy(&idx, generation, sequence, version)
    } else {
        IndexFileWriter::create(&idx, generation, sequence)
    }
    .unwrap();
    writer
        .write_segment(record.seq_no, INTEGER, &[(LEVEL_BLOCK, vec![record])])
        .unwrap();
    (writer.finish().unwrap(), (path, generation, sequence))
}

#[test]
fn window_aggregate_decodes_cross_bucket_block_once_and_skips_gaps() {
    use crate::query::storage_ir::{AggregateNumber, AggregateProjection, TimeRange};
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("data");
    let store = IndexStore::open(&path, &root.path().join("wal")).unwrap();
    let values = (0..30)
        .map(|t| Value::IntegerValue(IntegerValue::new(t, t)))
        .collect();
    let (idx, vlog) = batch_values(&path, 1, 1, values);
    store.install(&[idx], &[vlog], &[]).unwrap();
    let projection = AggregateProjection::complete(INTEGER, true);
    for ranges in [
        vec![
            TimeRange::new(0, 9),
            TimeRange::new(10, 19),
            TimeRange::new(20, 29),
        ],
        vec![
            TimeRange::new(2, 4),
            TimeRange::new(10, 12),
            TimeRange::new(27, 28),
        ],
    ] {
        let result = store
            .aggregate_projected_windows(
                SeqNo::new(1, 1).unwrap(),
                INTEGER,
                &ranges,
                vec![],
                projection,
            )
            .unwrap();
        assert_eq!(result.metrics.decoded_blocks, 1);
        assert_eq!(result.metrics.statistics_blocks, 0);
        for (state, range) in result.states.iter().zip(ranges) {
            assert_eq!(
                state.count,
                Some((range.end_time - range.start_time + 1) as u64)
            );
            assert_eq!(
                state.sum,
                Some(AggregateNumber::Integer(
                    (range.start_time..=range.end_time).sum()
                ))
            );
            assert_eq!(state.min, Some(AggregateNumber::Integer(range.start_time)));
            assert_eq!(state.max, Some(AggregateNumber::Integer(range.end_time)));
            assert_eq!(state.first.as_ref().unwrap().time, range.start_time);
            assert_eq!(state.last.as_ref().unwrap().time, range.end_time);
        }
    }
}

#[test]
fn query_read_ahead_is_bounded_and_reuses_immutable_bytes() {
    use crate::{
        common::slow_trace,
        storage::{AggregateQueryOptions, tsm1::vlog::aggregate_reader::QueryBlockReader},
    };
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("data");
    let store = IndexStore::open(&path, &root.path().join("wal")).unwrap();
    let values = (0..30)
        .map(|t| Value::IntegerValue(IntegerValue::new(t, t)))
        .collect();
    let (idx, vlog) = batch_values(&path, 1, 1, values);
    store.install(&[idx], &[vlog], &[]).unwrap();
    let (files, vlogs, _) = store.read_view();
    let entry = files[0].lookup(SeqNo::new(1, 1).unwrap()).unwrap().unwrap();
    let header = files[0].segment_header(&entry).unwrap();
    let record = files[0]
        .read_level_projected(
            &entry,
            &header,
            LEVEL_BLOCK,
            i64::MIN,
            i64::MAX,
            &[Column::Count],
            &mut Default::default(),
        )
        .unwrap()
        .remove(0);
    let mut reader = QueryBlockReader::new(AggregateQueryOptions {
        read_cache_bytes: 1024,
        read_ahead_bytes: 128,
        ..Default::default()
    });
    #[cfg(feature = "slow-trace")]
    let profile = slow_trace::QueryProfile::start().unwrap();
    for _ in 0..2 {
        let block = reader
            .read(
                &vlogs[0],
                u64::from(record.pos.block.offset),
                u64::from(record.pos.block_size),
                INTEGER,
            )
            .unwrap();
        assert!(block.valid_range(30, 0, 29, INTEGER));
        assert!(reader.retained_bytes() <= 1024);
    }
    assert!(reader.read(&vlogs[0], u64::MAX, 8, INTEGER).is_err());
    #[cfg(feature = "slow-trace")]
    {
        let profile = profile.finish();
        assert_eq!(profile.vlog_read_calls, 1);
        assert_eq!(profile.vlog_read_cache_hits, 1);
    }
}

#[test]
fn window_aggregate_preserves_overwrites_deletes_and_cache_after_compaction() {
    use crate::query::storage_ir::{
        AggregateNumber, AggregatePointValue, AggregateProjection, TimeRange,
    };
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("data");
    let wal = root.path().join("wal");
    let store = IndexStore::open(&path, &wal).unwrap();
    let seq = SeqNo::new(1, 1).unwrap();
    for generation in 1..=2 {
        let values = (0..30)
            .map(|t| Value::IntegerValue(IntegerValue::new(t, generation * 100 + t)))
            .collect();
        let (idx, vlog) = batch_values(&path, generation, 1, values);
        store.install(&[idx], &[vlog], &[]).unwrap();
    }
    store
        .delete_ranges(vec![DeleteRange {
            seq,
            min: 5,
            max: 7,
            cutoff: 0,
        }])
        .unwrap();
    let (idx, vlog) = batch_values(
        &path,
        3,
        1,
        vec![Value::IntegerValue(IntegerValue::new(6, 300))],
    );
    store.install(&[idx], &[vlog], &[]).unwrap();
    let ranges = [
        TimeRange::new(0, 9),
        TimeRange::new(10, 19),
        TimeRange::new(20, 29),
    ];
    let projection = AggregateProjection::complete(INTEGER, true);
    // Independent visible-point oracle: newest disk write wins, deletion is
    // file scoped, a later write resurrects time 6, and cache wins at time 15.
    let mut visible = (0..30)
        .map(|t| (t, 200 + t))
        .collect::<std::collections::BTreeMap<_, _>>();
    for t in 5..=7 {
        visible.remove(&t);
    }
    visible.insert(6, 300);
    visible.insert(15, 1000);
    let verify = |store: &IndexStore| {
        let result = store
            .aggregate_projected_windows(
                seq,
                INTEGER,
                &ranges,
                vec![Value::IntegerValue(IntegerValue::new(15, 1000))],
                projection,
            )
            .unwrap();
        for (state, range) in result.states.iter().zip(ranges) {
            let points = visible
                .range(range.start_time..=range.end_time)
                .collect::<Vec<_>>();
            assert_eq!(state.count, Some(points.len() as u64));
            assert_eq!(
                state.sum,
                Some(AggregateNumber::Integer(
                    points.iter().map(|(_, v)| **v).sum()
                ))
            );
            assert_eq!(
                state.max,
                Some(AggregateNumber::Integer(
                    points.iter().map(|(_, v)| **v).max().unwrap()
                ))
            );
            let first = state.first.as_ref().unwrap();
            let last = state.last.as_ref().unwrap();
            assert_eq!(
                (first.time, &first.value),
                (*points[0].0, &AggregatePointValue::Integer(*points[0].1))
            );
            assert_eq!(
                (last.time, &last.value),
                (
                    *points.last().unwrap().0,
                    &AggregatePointValue::Integer(*points.last().unwrap().1)
                )
            );
        }
    };
    verify(&store);
    store
        .compact_files(&store.index_readers(), 4, &|| false)
        .unwrap();
    verify(&store);
    drop(store);
    let store = IndexStore::open(&path, &wal).unwrap();
    verify(&store);
}

#[test]
fn projected_query_mixes_legacy_files_and_upgrades_columns_during_compaction() {
    use crate::query::storage_ir::{
        AggregateKind, AggregateNumber, AggregateProjection, TimeRange,
    };
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("data");
    let wal = root.path().join("wal");
    let store = IndexStore::open(&path, &wal).unwrap();
    let seq = SeqNo::new(1, 1).unwrap();
    let projection = AggregateProjection::from_aggregates(&[AggregateKind::Sum]);
    for (generation, version) in [(1, 2), (2, 3), (3, 4)] {
        let (idx, vlog) = batch_values_with_format(
            &path,
            generation,
            1,
            vec![Value::IntegerValue(IntegerValue::new(
                generation * 10,
                generation,
            ))],
            version,
        );
        store.install(&[idx], &[vlog], &[]).unwrap();
    }
    let before = store
        .aggregate_projected(seq, INTEGER, TimeRange::all(), vec![], projection)
        .unwrap();
    assert_eq!(before.state.sum, Some(AggregateNumber::Integer(6)));
    assert_eq!(before.state.count, None);
    assert_eq!(before.state.min, None);
    assert_eq!(before.index_reads.legacy_groups, 2);
    store
        .compact_files(&store.index_readers(), 4, &|| false)
        .unwrap();
    drop(store);
    let store = IndexStore::open(&path, &wal).unwrap();
    let after = store
        .aggregate_projected(seq, INTEGER, TimeRange::all(), vec![], projection)
        .unwrap();
    assert_eq!(after.state, before.state);
    assert_eq!(after.index_reads.legacy_groups, 0);
    assert_eq!(after.metrics.decoded_blocks, 0);
    assert!(after.index_reads.requested_bytes < after.index_reads.full_group_bytes);
}

#[test]
fn garbage_collection_preserves_split_generations_and_file_levels_after_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("data");
    let wal = root.path().join("wal");
    let store = IndexStore::open(&path, &wal).unwrap();
    let mut indexes = Vec::new();
    let mut vlogs = Vec::new();
    for (generation, sequence, time) in [(1, 2, 10), (1, 3, 11), (2, 4, 20)] {
        let (idx, vlog) = batch_at_sequence(&path, generation, sequence, time);
        let mut append = OpenOptions::new().append(true).open(&vlog.0).unwrap();
        append.write_all(&[0; 4096]).unwrap();
        append.sync_all().unwrap();
        indexes.push(idx);
        vlogs.push(vlog);
        if sequence == 3 || generation == 2 {
            store.install(&indexes, &vlogs, &[]).unwrap();
            indexes.clear();
            vlogs.clear();
        }
    }
    let stats = store
        .collect_garbage(
            super::super::allocation::OutputGenerations::isolated(3),
            u64::MAX,
            Default::default(),
            &|| false,
        )
        .unwrap();
    assert_eq!((stats.source_files, stats.copied_blocks), (2, 2));
    assert_eq!(
        store
            .index_readers()
            .iter()
            .map(|f| (f.generation, f.seq))
            .collect::<Vec<_>>(),
        [(2, 4), (3, 2), (3, 3)]
    );
    store.checkpoint(true).unwrap();
    drop(store);
    let store = IndexStore::open(&path, &wal).unwrap();
    assert_eq!(
        store
            .index_readers()
            .iter()
            .map(|f| (f.generation, f.seq))
            .collect::<Vec<_>>(),
        [(2, 4), (3, 2), (3, 3)]
    );
    assert_eq!(
        store
            .aggregate(
                SeqNo::new(1, 1).unwrap(),
                INTEGER,
                crate::query::storage_ir::TimeRange::all(),
                vec![],
                true
            )
            .unwrap()
            .state
            .count,
        3
    );
}

#[test]
fn higher_level_query_reads_summaries_without_decoding_vlog() {
    use crate::query::storage_ir::{AggregateValues, TimeRange};
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("vlog");
    let store = IndexStore::open(&path, &root.path().join("wal")).unwrap();
    for generation in 1..=70 {
        let (idx, vlog) = batch(&path, generation, generation);
        store.install(&[idx], &[vlog], &[]).unwrap();
    }
    let inputs = store.index_readers();
    store.compact_files(&inputs, 71, &|| false).unwrap();
    let result = store
        .aggregate(
            SeqNo::new(1, 1).unwrap(),
            INTEGER,
            TimeRange::all(),
            Vec::new(),
            true,
        )
        .unwrap();
    assert_eq!(result.state.count, 70);
    assert_eq!(
        result.state.values,
        Some(AggregateValues::Integer {
            sum: 2485,
            min: 1,
            max: 70
        })
    );
    assert_eq!(result.metrics.index_levels, 2);
    assert_eq!(result.metrics.index_records, 2);
    assert_eq!(result.metrics.decoded_blocks, 0);
    for range in [TimeRange::all(), TimeRange::new(2, 69)] {
        let mut expected = store
            .aggregate(SeqNo::new(1, 1).unwrap(), INTEGER, range, vec![], true)
            .unwrap();
        expected.state.first = None;
        expected.state.last = None;
        let projected = store
            .aggregate(SeqNo::new(1, 1).unwrap(), INTEGER, range, vec![], false)
            .unwrap();
        assert_eq!(projected.state, expected.state);
        assert_eq!(
            projected.metrics.index_records,
            expected.metrics.index_records
        );
        assert_eq!(projected.metrics.decoded_blocks, 0);
    }
    let part = store
        .aggregate(
            SeqNo::new(1, 1).unwrap(),
            INTEGER,
            TimeRange::new(2, 69),
            Vec::new(),
            true,
        )
        .unwrap();
    assert_eq!(part.state.count, 68);
    assert_eq!(part.metrics.decoded_blocks, 0);
    let before = store.cache.stats();
    store
        .aggregate(
            SeqNo::new(1, 1).unwrap(),
            INTEGER,
            TimeRange::all(),
            Vec::new(),
            true,
        )
        .unwrap();
    assert!(store.cache.stats().hits > before.hits);
    store.cache.set_budget(0);
    assert_eq!(
        store
            .aggregate(
                SeqNo::new(1, 1).unwrap(),
                INTEGER,
                TimeRange::all(),
                Vec::new(),
                true
            )
            .unwrap()
            .state,
        result.state
    );
    assert_eq!(store.cache.stats().entries, 0);
}

#[test]
fn garbage_collection_preserves_versions_and_keeps_pinned_readers_valid() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("vlog");
    let wal = root.path().join("wal");
    let store = IndexStore::open(&path, &wal).unwrap();
    let (idx, vlog) = batch(&path, 1, 10);
    let unused = Values::encode(
        &(0..500)
            .map(|i| Value::IntegerValue(IntegerValue::new(i, i * i)))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let mut append = OpenOptions::new().append(true).open(&vlog.0).unwrap();
    append
        .write_all(&crc32fast::hash(&unused).to_be_bytes())
        .unwrap();
    append.write_all(&unused).unwrap();
    append.sync_all().unwrap();
    drop(append);
    store.install(&[idx], &[vlog], &[]).unwrap();
    let pinned = store.read_view();
    let old_path = pinned.1[0].path();
    let stats = store
        .collect_garbage(
            super::super::allocation::OutputGenerations::isolated(2),
            u64::MAX,
            Default::default(),
            &|| false,
        )
        .unwrap();
    assert_eq!(stats.source_files, 1);
    assert_eq!(stats.copied_blocks, 1);
    let seq = SeqNo::new(1, 1).unwrap();
    let (locations, _, versions, _) = store
        .query_blocks(seq, INTEGER, i64::MIN, i64::MAX)
        .unwrap();
    assert_eq!(locations[0].generation, 2);
    assert_eq!(versions.values().copied().collect::<Vec<_>>(), [257]);
    // A subsequent deletion targets the current index file after relocation.
    store
        .delete_ranges(vec![DeleteRange {
            seq,
            min: 10,
            max: 10,
            cutoff: 0,
        }])
        .unwrap();
    assert_eq!(
        store
            .aggregate(
                seq,
                INTEGER,
                crate::query::storage_ir::TimeRange::all(),
                vec![],
                true
            )
            .unwrap()
            .state
            .count,
        0
    );
    store.checkpoint(true).unwrap();
    store.reap_retired().unwrap();
    assert!(old_path.exists());
    assert_eq!(
        pinned.1[0].read_block(5, stats.copied_bytes).unwrap().len(),
        1
    );
    drop(pinned);
    store.reap_retired().unwrap();
    assert!(!old_path.exists());
    drop(store);
    let store = IndexStore::open(&path, &wal).unwrap();
    assert_eq!(
        store
            .aggregate(
                seq,
                INTEGER,
                crate::query::storage_ir::TimeRange::all(),
                vec![],
                true
            )
            .unwrap()
            .state
            .count,
        0
    );
}

#[test]
fn checkpoint_reclaims_only_unreferenced_vlogs_after_readers_release() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("data");
    let wal = root.path().join("wal");
    let store = IndexStore::open(&path, &wal).unwrap();
    for generation in 1..=2 {
        let (index, vlog) = batch(&path, generation, 10);
        store.install(&[index], &[vlog], &[]).unwrap();
    }
    let old = store.readers();
    let inputs = store.index_readers();
    store.compact_files(&inputs, 3, &|| false).unwrap();
    assert_eq!(store.checkpoint(true).unwrap(), 2);
    drop(inputs);
    store.reap_retired().unwrap();
    assert!(old.iter().all(|r| r.path().exists()));
    assert_eq!(store.readers().len(), 1);
    drop(old);
    store.reap_retired().unwrap();
    assert!(!path.join(output_name(1, 1, "vlog")).exists());
    let metadata = fs::read_dir(&path)
        .unwrap()
        .map(|f| f.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert!(
        metadata
            .iter()
            .all(|n| !n.ends_with(".installed") && !n.ends_with(".replace"))
    );
    drop(store);
    let store = IndexStore::open(&path, &wal).unwrap();
    assert_eq!(store.index_readers().len(), 1);
    assert_eq!(store.readers().len(), 1);
    let (index, vlog) = batch(&path, 4, 20);
    store.install(&[index], &[vlog], &[]).unwrap();
    let inputs = store.index_readers();
    store.compact_files(&inputs, 5, &|| false).unwrap();
    drop(inputs);
    drop(store);
    let store = IndexStore::open(&path, &wal).unwrap();
    let result = store
        .aggregate(
            SeqNo::new(1, 1).unwrap(),
            INTEGER,
            crate::query::storage_ir::TimeRange::all(),
            Vec::new(),
            true,
        )
        .unwrap();
    assert_eq!(result.state.count, 2);
    assert_eq!(
        result.state.values,
        Some(crate::query::storage_ir::AggregateValues::Integer {
            sum: 6,
            min: 2,
            max: 4
        })
    );
}

#[test]
fn deletion_preparation_does_not_hold_the_publication_gate() {
    use super::super::delete::DeleteRange;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("data");
    let wal = root.path().join("wal");
    let store = IndexStore::open(&path, &wal).unwrap();
    let values = [0, 24000]
        .into_iter()
        .map(|t| Value::IntegerValue(IntegerValue::new(t, t)))
        .collect();
    let (index, vlog) = batch_values(&path, 1, 1, values);
    store.install(&[index], &[vlog], &[]).unwrap();
    let mut builder = store.delete_batch_builder();
    for series in (1..=12000).rev() {
        builder
            .push(DeleteRange {
                seq: SeqNo::new(1, 1).unwrap(),
                min: series * 2,
                max: series * 2,
                cutoff: 0,
            })
            .unwrap();
    }
    let batch = builder.finish().unwrap();
    let held = store.commit_gate.lock();
    let prepared = std::thread::scope(|scope| {
        let task = scope.spawn(|| store.install_delete_batch(&batch));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let mut prepared = false;
        while std::time::Instant::now() < deadline {
            prepared = std::fs::read_dir(&path).unwrap().any(|entry| {
                let entry = entry.unwrap();
                entry.path().extension().is_some_and(|ext| ext == "tmp")
                    && entry.metadata().unwrap().len() == 20 + 12000 * 28
            });
            if prepared {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        let unpublished = store.read_view().2.is_empty();
        // Release the intentional blocker before asserting, including on failure.
        drop(held);
        task.join().unwrap().unwrap();
        (prepared, unpublished)
    });
    assert!(
        prepared.0,
        "delete encoding waited for the publication gate"
    );
    assert!(prepared.1, "uncommitted deletion became visible");
    drop(store);
    let store = IndexStore::open(&path, &wal).unwrap();
    let files = store.read_view().2;
    assert_eq!(files.len(), 1);
    let rows = files[0].lookup(SeqNo::new(1, 1).unwrap()).unwrap();
    assert_eq!(rows.len(), 12000);
    let mut series = 0;
    for page in files[0].pages() {
        let page = page.unwrap();
        assert!(page.len() <= 256);
        for row in page {
            series += 1;
            assert_eq!(row.seq.series_id(), 1);
            assert_eq!((row.min, row.max), (series * 2, series * 2));
        }
    }
    assert_eq!(series, 12000);
}

#[test]
fn tombstones_are_file_scoped_and_included_in_backups() {
    use super::super::delete::DeleteRange;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("data");
    let wal = root.path().join("wal");
    let store = IndexStore::open(&path, &wal).unwrap();
    let seq = SeqNo::new(1, 1).unwrap();
    let (index, vlog) = batch(&path, 1, 10);
    store.install(&[index], &[vlog], &[]).unwrap();
    store
        .delete_ranges(vec![DeleteRange {
            seq,
            min: 10,
            max: 10,
            cutoff: 0,
        }])
        .unwrap();
    let backup = store.create_snapshot(root.path()).unwrap();
    let restored = IndexStore::open(&backup, &root.path().join("restore-wal")).unwrap();
    let result = restored
        .aggregate(
            seq,
            INTEGER,
            crate::query::storage_ir::TimeRange::all(),
            Vec::new(),
            true,
        )
        .unwrap();
    assert_eq!(result.state.count, 0);
    let (index, vlog) = batch(&path, 2, 10);
    store.install(&[index], &[vlog], &[]).unwrap();
    let result = store
        .aggregate(
            seq,
            INTEGER,
            crate::query::storage_ir::TimeRange::all(),
            Vec::new(),
            true,
        )
        .unwrap();
    assert_eq!(result.state.count, 1);
    assert_eq!(
        result.state.values,
        Some(crate::query::storage_ir::AggregateValues::Integer {
            sum: 2,
            min: 2,
            max: 2
        })
    );
}

#[test]
fn repair_data_and_index_recover_together_at_every_commit_boundary() {
    for stage in [
        "replacement_output_renamed",
        "replacement_outputs_durable",
        "replacement_durable",
        "replacement_published",
    ] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("vlog");
        let wal = root.path().join("wal");
        let store = IndexStore::open(&path, &wal).unwrap();
        for generation in 1..=2 {
            let (idx, vlog) = batch(&path, generation, 10);
            store.install(&[idx], &[vlog], &[]).unwrap();
        }
        let inputs = store.index_readers();
        let (indexes, _, vlogs) = super::super::compact::merge_files_with_repair(
            &path,
            3,
            &inputs,
            &store.readers(),
            &|| false,
        )
        .unwrap();
        assert_eq!(vlogs.len(), 1);
        assert!(
            store
                .replace_data_inner(&inputs, &indexes, &vlogs, &mut |point| {
                    if point == stage {
                        Err(invalid("injected repair interruption"))
                    } else {
                        Ok(())
                    }
                })
                .is_err()
        );
        drop(inputs);
        drop(store);
        let store = IndexStore::open(&path, &wal).unwrap();
        let committed = stage == "replacement_durable" || stage == "replacement_published";
        assert_eq!(store.index_readers().len(), if committed { 1 } else { 2 });
        let result = store
            .aggregate(
                SeqNo::new(1, 1).unwrap(),
                INTEGER,
                crate::query::storage_ir::TimeRange::all(),
                Vec::new(),
                true,
            )
            .unwrap();
        assert_eq!(result.state.count, 1);
        assert_eq!(
            result.state.values,
            Some(crate::query::storage_ir::AggregateValues::Integer {
                sum: 2,
                min: 2,
                max: 2
            })
        );
    }
}

#[test]
fn split_field_replacement_recovers_an_entire_generation_at_every_commit_boundary() {
    use crate::query::storage_ir::TimeRange;
    for stage in [
        "replacement_output_renamed",
        "replacement_outputs_durable",
        "replacement_durable",
        "replacement_published",
    ] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("vlog");
        let wal = root.path().join("wal");
        let store = IndexStore::open(&path, &wal).unwrap();
        for generation in 1..=3 {
            let (idx, vlog) = batch(&path, generation, generation * 10);
            store.install(&[idx], &[vlog], &[]).unwrap();
        }
        let inputs = store.index_readers();
        let (indexes, _) =
            super::super::compact::merge_files_with_target(&path, 4, &inputs, &|| false, 1)
                .unwrap();
        assert_eq!(indexes.len(), 3);
        assert!(
            store
                .replace_data_inner(&inputs, &indexes, &[], &mut |point| {
                    if point == stage {
                        Err(invalid("injected split replacement interruption"))
                    } else {
                        Ok(())
                    }
                })
                .is_err()
        );
        drop(inputs);
        drop(store);
        let store = IndexStore::open(&path, &wal).unwrap();
        let committed = stage == "replacement_durable" || stage == "replacement_published";
        let files = store.index_readers();
        assert_eq!(files.len(), 3);
        assert!(files.iter().all(|f| (f.generation == 4) == committed));
        let result = store
            .aggregate(
                SeqNo::new(1, 1).unwrap(),
                INTEGER,
                TimeRange::all(),
                vec![],
                true,
            )
            .unwrap();
        assert_eq!(result.state.count, 3);
        assert_eq!(
            result.state.values,
            Some(crate::query::storage_ir::AggregateValues::Integer {
                sum: 6,
                min: 1,
                max: 3
            })
        );
    }
}

#[test]
fn equal_file_count_compaction_commits_and_does_not_repeat() {
    use super::super::{allocation::OutputGenerations, planner::select_inputs};
    use crate::query::storage_ir::{AggregateValues, TimeRange};
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("vlog");
    let wal = root.path().join("wal");
    let store = IndexStore::open(&path, &wal).unwrap();
    for generation in 1..=8 {
        let (index, vlog) = batch(&path, generation, generation * 10);
        store.install(&[index], &[vlog], &[]).unwrap();
    }
    let inputs = select_inputs(&store).unwrap();
    assert_eq!(inputs.len(), 8);
    store
        .compact_files_controlled(
            &inputs,
            OutputGenerations::isolated(9),
            &|| false,
            1,
            Default::default(),
        )
        .unwrap();
    assert_eq!(store.index_readers().len(), 8);
    assert!(store.index_readers().iter().all(|f| f.generation == 9));
    assert!(select_inputs(&store).unwrap().is_empty());
    drop(inputs);
    drop(store);
    let store = IndexStore::open(&path, &wal).unwrap();
    assert!(select_inputs(&store).unwrap().is_empty());
    let seq = SeqNo::new(1, 1).unwrap();
    let result = store
        .aggregate(seq, INTEGER, TimeRange::all(), vec![], true)
        .unwrap();
    assert_eq!(result.state.count, 8);
    assert_eq!(
        result.state.values,
        Some(AggregateValues::Integer {
            sum: 36,
            min: 1,
            max: 8
        })
    );
    store
        .delete_ranges(vec![DeleteRange {
            seq,
            min: 10,
            max: 10,
            cutoff: 0,
        }])
        .unwrap();
    let inputs = select_inputs(&store).unwrap();
    assert_eq!(inputs.len(), 8);
    store
        .compact_files_controlled(
            &inputs,
            OutputGenerations::isolated(10),
            &|| false,
            1,
            Default::default(),
        )
        .unwrap();
    assert!(select_inputs(&store).unwrap().is_empty());
    assert_eq!(
        store
            .aggregate(seq, INTEGER, TimeRange::all(), vec![], true)
            .unwrap()
            .state
            .count,
        7
    );
}

#[test]
fn extended_sequences_recover_atomically_at_every_replacement_boundary() {
    for stage in [
        "replacement_output_renamed",
        "replacement_outputs_durable",
        "replacement_durable",
        "replacement_published",
    ] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("vlog");
        let wal = root.path().join("wal");
        let store = IndexStore::open(&path, &wal).unwrap();
        super::super::raw_cursor_tests::install_blocks(&store, &path, 1, 0, 300);
        let inputs = store.index_readers();
        let (indexes, _) =
            super::super::compact::merge_files_with_target(&path, 2, &inputs, &|| false, 1)
                .unwrap();
        assert_eq!(indexes.len(), 300);
        assert!(
            store
                .replace_data_inner(&inputs, &indexes, &[], &mut |point| {
                    if point == stage {
                        Err(invalid("injected extended output interruption"))
                    } else {
                        Ok(())
                    }
                })
                .is_err()
        );
        drop(inputs);
        drop(store);
        let store = IndexStore::open(&path, &wal).unwrap();
        let committed = stage == "replacement_durable" || stage == "replacement_published";
        assert_eq!(
            store.index_readers().len(),
            if committed { 300 } else { 1 },
            "{stage}"
        );
        assert_eq!(
            store.next_generation(),
            if committed { 4 } else { 2 },
            "{stage}"
        );
        let result = store
            .aggregate(
                SeqNo::new(1, 1).unwrap(),
                INTEGER,
                crate::query::storage_ir::TimeRange::all(),
                vec![],
                true,
            )
            .unwrap();
        assert_eq!(result.state.count, 4800, "{stage}");
        assert_eq!(
            result.state.values,
            Some(crate::query::storage_ir::AggregateValues::Integer {
                sum: 4800,
                min: 1,
                max: 1,
            })
        );
    }
}

#[test]
fn replacement_installs_value_files_from_multiple_reserved_generations() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("vlog");
    let wal = root.path().join("wal");
    let store = IndexStore::open(&path, &wal).unwrap();
    let (index, vlog) = batch(&path, 1, 0);
    store.install(&[index], &[vlog], &[]).unwrap();
    let mut vlogs = Vec::new();
    let mut records = Vec::new();
    let seq = SeqNo::new(1, 1).unwrap();
    for generation in [2, 3] {
        let time = generation * 10;
        let (unused_index, vlog) = batch(&path, generation, time);
        fs::remove_file(unused_index.path).unwrap();
        let mut record = Record::from_vlog_index(
            seq,
            &VLogIndex {
                aggregate: None,
                count: 1,
                typ: INTEGER,
                min: time,
                max: time,
                generation,
                seq: 1,
                offset: 5,
                size: fs::metadata(&vlog.0).unwrap().len() as i64 - 5,
            },
            AggValues::from_block(&[Value::IntegerValue(IntegerValue::new(time, generation))])
                .unwrap(),
        )
        .unwrap();
        record.pos.version = BlockLoc::from_vlog(1, 1, 5).unwrap().file_no;
        records.push(record);
        vlogs.push(vlog);
    }
    let mut writer =
        IndexFileWriter::create(&path.join("000000002-000000002.idx.tmp"), 2, 2).unwrap();
    writer
        .write_segment(seq, INTEGER, &[(LEVEL_BLOCK, records)])
        .unwrap();
    store
        .replace_data_inner(
            &store.index_readers(),
            &[writer.finish().unwrap()],
            &vlogs,
            &mut |_| Ok(()),
        )
        .unwrap();
    drop(store);
    let store = IndexStore::open(&path, &wal).unwrap();
    assert_eq!(store.next_generation(), 4);
    let result = store
        .aggregate(
            seq,
            INTEGER,
            crate::query::storage_ir::TimeRange::all(),
            vec![],
            false,
        )
        .unwrap();
    assert_eq!(result.state.count, 2);
    assert_eq!(
        result.state.values,
        Some(crate::query::storage_ir::AggregateValues::Integer {
            sum: 5,
            min: 2,
            max: 3
        })
    );
}

#[test]
fn commit_rejects_aliased_physical_index_identities() {
    let commit = Commit {
        version: 2,
        indexes: [(1, 256), (2, 0)]
            .into_iter()
            .map(|(generation, seq)| Output {
                name: output_name(generation, seq, "idx"),
                generation,
                seq,
                size: 100,
            })
            .collect(),
        vlogs: vec![],
        closed_wal: vec![],
    };
    assert!(commit.validate().is_err());
}

#[test]
fn installed_snapshots_reopen_without_replaying_reused_wal_names() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("vlog");
    let wal = root.path().join("wal");
    let store = IndexStore::open(&path, &wal).unwrap();
    let closed = wal.join("00001.wal");
    for generation in 1..=2 {
        fs::write(&closed, b"closed WAL").unwrap();
        let (idx, vlog) = batch(&path, generation, 10);
        store
            .install(&[idx], &[vlog], &[closed.to_str().unwrap().into()])
            .unwrap();
        assert!(!closed.exists());
    }
    fs::write(&closed, b"new WAL with a reused ID").unwrap();
    drop(store);
    let store = IndexStore::open(&path, &wal).unwrap();
    assert_eq!(fs::read(&closed).unwrap(), b"new WAL with a reused ID");
    assert_eq!(store.next_generation(), 3);
    let locations = store
        .block_locations(SeqNo::new(1, 1).unwrap(), INTEGER, 10, 10)
        .unwrap();
    assert_eq!(locations.len(), 2);
    for (reader, index) in store.readers().iter().zip(&locations) {
        let values = reader
            .read_integer_block(index.offset as u64, index.size as u64)
            .unwrap();
        assert_eq!(
            values,
            vec![IntegerValue {
                unix_nano: 10,
                value: reader.generation
            }]
        );
    }
    assert!(
        store
            .block_locations(SeqNo::new(1, 1).unwrap(), FLOAT, 10, 10)
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .block_locations(SeqNo::new(1, 1).unwrap(), INTEGER, 11, 20)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn interrupted_installations_recover_at_each_commit_boundary() {
    for stage in [
        "output_renamed",
        "outputs_durable",
        "commit_durable",
        "published",
    ] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("vlog");
        let wal = root.path().join("wal");
        let store = IndexStore::open(&path, &wal).unwrap();
        let closed = wal.join("00001.wal");
        fs::write(&closed, b"replay me unless committed").unwrap();
        let (idx, vlog) = batch(&path, 1, 10);
        let result = store.install_inner(
            &[idx],
            &[vlog],
            &[closed.to_str().unwrap().into()],
            &mut |point| {
                if point == stage {
                    Err(invalid("injected interruption"))
                } else {
                    Ok(())
                }
            },
        );
        assert!(result.is_err(), "{stage}");
        drop(store);
        let store = IndexStore::open(&path, &wal).unwrap();
        let committed = stage == "commit_durable" || stage == "published";
        assert_eq!(closed.exists(), !committed, "{stage}");
        assert_eq!(store.readers().len(), usize::from(committed), "{stage}");
        let outputs = fs::read_dir(&path)
            .unwrap()
            .filter(|entry| {
                let name = entry.as_ref().unwrap().file_name();
                let name = name.to_string_lossy();
                name.ends_with(".idx") || name.ends_with(".vlog") || name.ends_with(".tmp")
            })
            .count();
        assert_eq!(outputs, if committed { 2 } else { 0 }, "{stage}");
    }
}

#[test]
fn corrupt_or_missing_committed_files_preserve_pending_wal() {
    for corrupt_commit in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("vlog");
        let wal = root.path().join("wal");
        let store = IndexStore::open(&path, &wal).unwrap();
        let closed = wal.join("00001.wal");
        fs::write(&closed, b"keep me").unwrap();
        let (idx, vlog) = batch(&path, 1, 10);
        assert!(
            store
                .install_inner(
                    &[idx],
                    &[vlog],
                    &[closed.to_str().unwrap().into()],
                    &mut |stage| {
                        if stage == "commit_durable" {
                            Err(invalid("injected interruption"))
                        } else {
                            Ok(())
                        }
                    }
                )
                .is_err()
        );
        drop(store);
        if corrupt_commit {
            fs::write(path.join("snapshot-00000101.commit"), b"corrupt").unwrap();
        } else {
            fs::remove_file(path.join(output_name(1, 1, "vlog"))).unwrap();
        }
        assert!(IndexStore::open(&path, &wal).is_err());
        assert_eq!(fs::read(&closed).unwrap(), b"keep me");
    }
}

#[test]
fn mode_changes_and_unsafe_commit_paths_are_rejected() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("vlog");
    let wal = root.path().join("wal");
    let store = IndexStore::open(&path, &wal).unwrap();
    assert!(IndexStore::check_mode(&path, false).is_err());
    let (idx, vlog) = batch(&path, 1, 10);
    assert!(
        store
            .install(&[idx], &[vlog], &["../outside.wal".into()])
            .is_err()
    );
    assert!(store.readers().is_empty());
    let legacy = root.path().join("legacy");
    fs::create_dir(&legacy).unwrap();
    fs::write(legacy.join("000000001-000000001.vlog"), b"legacy").unwrap();
    assert!(IndexStore::check_mode(&legacy, true).is_err());
    assert!(IndexStore::check_mode(&legacy, false).is_ok());
    assert!(!legacy.join(MODE_FILE).exists());
}

fn installed_pair(path: &Path, wal: &Path) -> Arc<IndexStore> {
    let store = IndexStore::open(path, wal).unwrap();
    for generation in 1..=2 {
        let (idx, vlog) = batch(path, generation, generation * 10);
        store.install(&[idx], &[vlog], &[]).unwrap();
    }
    store
}

fn replacement_output(path: &Path, inputs: &[Arc<IndexFile>], generation: i64) -> IndexFileMeta {
    use super::super::compact::{SegmentSource, merge_ordered_segments};
    let sources = inputs
        .iter()
        .map(|file| {
            let entry = file.lookup(SeqNo::new(1, 1).unwrap()).unwrap().unwrap();
            let header = file.segment_header(&entry).unwrap();
            SegmentSource {
                file,
                entry,
                header,
            }
        })
        .collect::<Vec<_>>();
    let mut writer = IndexFileWriter::create(
        &path.join(format!("{}.tmp", output_name(generation, 0, "idx"))),
        generation,
        0,
    )
    .unwrap();
    merge_ordered_segments(&mut writer, &sources).unwrap();
    writer.finish().unwrap()
}

#[test]
fn replacement_pins_old_readers_and_backup_contains_only_live_indexes() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("data");
    let wal = root.path().join("wal");
    let store = installed_pair(&path, &wal);
    let inputs = store.index_readers();
    let old_paths = inputs
        .iter()
        .map(|f| f.path().to_path_buf())
        .collect::<Vec<_>>();
    let expected = store
        .block_locations(SeqNo::new(1, 1).unwrap(), INTEGER, 0, 100)
        .unwrap();
    let output = replacement_output(&path, &inputs, 3);
    store.replace(&inputs, &[output]).unwrap();
    assert_eq!(store.index_readers().len(), 1);
    assert_eq!(store.readers().len(), 2);
    assert_eq!(store.reap_retired().unwrap(), 0);
    for file in &inputs {
        assert!(file.lookup(SeqNo::new(1, 1).unwrap()).unwrap().is_some());
        assert!(file.path().exists());
    }
    let backup = store.create_snapshot(root.path()).unwrap();
    drop(inputs);
    assert_eq!(store.reap_retired().unwrap(), 2);
    assert!(old_paths.iter().all(|p| !p.exists()));
    drop(store);
    for data in [&path, &backup] {
        let restored = IndexStore::open(data, &root.path().join("restored-wal")).unwrap();
        assert_eq!(restored.index_readers().len(), 1);
        assert_eq!(restored.readers().len(), 2);
        assert_eq!(
            restored
                .block_locations(SeqNo::new(1, 1).unwrap(), INTEGER, 0, 100)
                .unwrap(),
            expected
        );
    }
}

#[test]
fn interrupted_replacements_recover_one_complete_file_set() {
    for stage in [
        "replacement_output_renamed",
        "replacement_outputs_durable",
        "replacement_durable",
        "replacement_published",
    ] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("data");
        let wal = root.path().join("wal");
        let store = installed_pair(&path, &wal);
        let inputs = store.index_readers();
        let output = replacement_output(&path, &inputs, 3);
        assert!(
            store
                .replace_inner(&inputs, &[output], &mut |point| {
                    if point == stage {
                        Err(invalid("injected replacement interruption"))
                    } else {
                        Ok(())
                    }
                })
                .is_err()
        );
        drop(inputs);
        if stage == "replacement_published" {
            assert_eq!(store.reap_retired().unwrap(), 2);
        }
        drop(store);
        let recovered = IndexStore::open(&path, &wal).unwrap();
        let committed = matches!(stage, "replacement_durable" | "replacement_published");
        assert_eq!(
            recovered.index_readers().len(),
            if committed { 1 } else { 2 },
            "{stage}"
        );
        assert_eq!(
            recovered
                .block_locations(SeqNo::new(1, 1).unwrap(), INTEGER, 0, 100)
                .unwrap()
                .len(),
            2
        );
        let indexes = fs::read_dir(&path)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|s| s == "idx")
            })
            .count();
        assert_eq!(indexes, if committed { 1 } else { 2 });
    }
}

#[test]
fn corrupt_replacement_or_missing_live_output_preserves_pending_wal() {
    for corrupt_manifest in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("data");
        let wal = root.path().join("wal");
        let store = installed_pair(&path, &wal);
        let inputs = store.index_readers();
        let output = replacement_output(&path, &inputs, 3);
        store.replace(&inputs, &[output]).unwrap();
        drop(inputs);
        store.reap_retired().unwrap();
        let closed = wal.join("pending.wal");
        fs::write(&closed, b"must survive failed recovery").unwrap();
        let (idx, vlog) = batch(&path, 4, 40);
        assert!(
            store
                .install_inner(
                    &[idx],
                    &[vlog],
                    &[closed.to_string_lossy().into_owned()],
                    &mut |stage| {
                        if stage == "commit_durable" {
                            Err(invalid("injected snapshot interruption"))
                        } else {
                            Ok(())
                        }
                    }
                )
                .is_err()
        );
        drop(store);
        if corrupt_manifest {
            fs::write(path.join("replace-00000300.replace"), b"corrupt").unwrap();
        } else {
            fs::remove_file(path.join(output_name(3, 0, "idx"))).unwrap();
        }
        assert!(IndexStore::open(&path, &wal).is_err());
        assert_eq!(fs::read(closed).unwrap(), b"must survive failed recovery");
    }
}

#[test]
fn chained_replacements_recover_without_retired_outputs_and_reject_stale_plans() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("data");
    let wal = root.path().join("wal");
    let store = installed_pair(&path, &wal);
    let old_inputs = store.index_readers();
    let output = replacement_output(&path, &old_inputs, 3);
    store.replace(&old_inputs, &[output.clone()]).unwrap();
    assert!(store.replace(&old_inputs, &[output]).is_err());
    store.ensure_ready().unwrap();
    drop(old_inputs);
    store.reap_retired().unwrap();
    let (idx, vlog) = batch(&path, 4, 40);
    store.install(&[idx], &[vlog], &[]).unwrap();
    let inputs = store.index_readers();
    let output = replacement_output(&path, &inputs, 5);
    store.replace(&inputs, &[output]).unwrap();
    drop(inputs);
    assert_eq!(store.reap_retired().unwrap(), 2);
    drop(store);
    let restored = IndexStore::open(&path, &wal).unwrap();
    assert_eq!(restored.index_readers().len(), 1);
    assert_eq!(restored.readers().len(), 3);
    assert_eq!(restored.next_generation(), 6);
    assert_eq!(
        restored
            .block_locations(SeqNo::new(1, 1).unwrap(), INTEGER, 0, 100)
            .unwrap()
            .len(),
        3
    );
}

#[test]
fn concurrent_flush_and_index_compaction_preserve_both_publications() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("data");
    let wal = root.path().join("wal");
    let store = installed_pair(&path, &wal);
    let inputs = store.index_readers();
    let output = replacement_output(&path, &inputs, 3);
    let (idx, vlog) = batch(&path, 4, 40);
    let start = std::sync::Barrier::new(3);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            start.wait();
            store.replace(&inputs, &[output]).unwrap();
        });
        scope.spawn(|| {
            start.wait();
            store.install(&[idx], &[vlog], &[]).unwrap();
        });
        start.wait();
    });
    assert_eq!(store.index_readers().len(), 2);
    assert_eq!(store.readers().len(), 3);
    drop(inputs);
    store.reap_retired().unwrap();
    drop(store);
    let store = IndexStore::open(&path, &wal).unwrap();
    assert_eq!(store.index_readers().len(), 2);
    assert_eq!(
        store
            .block_locations(SeqNo::new(1, 1).unwrap(), INTEGER, 0, 100)
            .unwrap()
            .len(),
        3
    );
}

#[test]
fn deletion_rejects_stale_compaction_and_filters_published_replacements() {
    for delete_first in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("data");
        let wal = root.path().join("wal");
        let store = IndexStore::open(&path, &wal).unwrap();
        for generation in 1..=2 {
            let (idx, vlog) = batch(&path, generation, 10);
            store.install(&[idx], &[vlog], &[]).unwrap();
        }
        let inputs = store.index_readers();
        // Build with the old deletion view, then publish the delete on either
        // side of replacement. Repair physically relocates the winning value.
        let (outputs, _, vlogs) = super::super::compact::merge_files_with_repair(
            &path,
            3,
            &inputs,
            &store.readers(),
            &|| false,
        )
        .unwrap();
        assert_eq!(vlogs.len(), 1);
        let delete = || {
            store
                .delete_ranges(vec![DeleteRange {
                    seq: SeqNo::new(1, 1).unwrap(),
                    min: 10,
                    max: 10,
                    cutoff: 0,
                }])
                .unwrap()
        };
        if delete_first {
            delete();
        }
        let result = store.replace_data_cancellable(&inputs, &outputs, &vlogs, &|| false);
        if delete_first {
            assert!(
                result.is_err(),
                "stale inputs must not resurrect deleted values"
            );
        } else {
            result.unwrap();
        }
        if !delete_first {
            delete();
        }
        let count = |store: &IndexStore| {
            store
                .aggregate(
                    SeqNo::new(1, 1).unwrap(),
                    INTEGER,
                    crate::query::storage_ir::TimeRange::all(),
                    vec![],
                    true,
                )
                .unwrap()
                .state
                .count
        };
        assert_eq!(count(&store), 0);
        store.checkpoint(true).unwrap();
        assert_eq!(store.read_view().2.len(), if delete_first { 2 } else { 1 });
        let (idx, vlog) = batch(&path, 4, 10);
        store.install(&[idx], &[vlog], &[]).unwrap();
        assert_eq!(count(&store), 1);
        drop(inputs);
        drop(store);
        let store = IndexStore::open(&path, &wal).unwrap();
        assert_eq!(count(&store), 1);
    }
}

#[test]
fn original_planner_rewrites_single_deleted_file_without_repeating_clean_output() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("data");
    let wal = root.path().join("wal");
    let store = IndexStore::open(&path, &wal).unwrap();
    let (idx, vlog) = batch(&path, 1, 10);
    store.install(&[idx], &[vlog], &[]).unwrap();
    store
        .delete_ranges(vec![DeleteRange {
            seq: SeqNo::new(1, 1).unwrap(),
            min: 10,
            max: 10,
            cutoff: 0,
        }])
        .unwrap();
    let selected = super::super::planner::select_inputs(&store).unwrap();
    assert_eq!(selected.len(), 1);
    store.compact_files(&selected, 2, &|| false).unwrap();
    assert!(
        super::super::planner::select_inputs(&store)
            .unwrap()
            .is_empty()
    );
    drop(selected);
    store.checkpoint(true).unwrap();
    drop(store);
    let store = IndexStore::open(&path, &wal).unwrap();
    assert!(
        super::super::planner::select_inputs(&store)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn replacement_durability_does_not_hold_the_query_file_list_lock() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("data");
    let wal = root.path().join("wal");
    let store = installed_pair(&path, &wal);
    let inputs = store.index_readers();
    let output = replacement_output(&path, &inputs, 3);
    let (reached_tx, reached_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (query_tx, query_rx) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let store_ref = &store;
        let inputs_ref = &inputs;
        scope.spawn(move || {
            let _commit = store_ref.commit_gate.lock();
            store_ref
                .replace_inner(inputs_ref, &[output], &mut |stage| {
                    if stage == "replacement_durable" {
                        reached_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                    }
                    Ok(())
                })
                .unwrap();
        });
        reached_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        scope.spawn(move || {
            query_tx
                .send(store_ref.block_locations(SeqNo::new(1, 1).unwrap(), INTEGER, 0, 100))
                .unwrap();
        });
        let query = query_rx.recv_timeout(std::time::Duration::from_secs(5));
        // Always release the publisher before asserting, so a regression cannot hang the test.
        release_tx.send(()).unwrap();
        assert_eq!(
            query
                .expect("query blocked by replacement publication")
                .unwrap()
                .len(),
            2
        );
    });
    assert_eq!(store.index_readers().len(), 1);
}

#[test]
fn append_only_mode_upgrades_after_validation_and_before_wal_cleanup() {
    for missing in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("data");
        let wal = root.path().join("wal");
        let store = installed_pair(&path, &wal);
        drop(store);
        fs::write(path.join(MODE_FILE), APPEND_ONLY_MODE_BYTES).unwrap();
        if missing {
            fs::remove_file(path.join(output_name(2, 1, "idx"))).unwrap();
        }
        let result = IndexStore::open(&path, &wal);
        assert_eq!(result.is_err(), missing);
        assert_eq!(
            fs::read(path.join(MODE_FILE)).unwrap(),
            if missing {
                APPEND_ONLY_MODE_BYTES
            } else {
                MODE_BYTES
            }
        );
    }
}

#[test]
fn replacement_cancelled_while_waiting_for_commit_does_not_publish() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("data");
    let wal = root.path().join("wal");
    let store = installed_pair(&path, &wal);
    let inputs = store.index_readers();
    let output = replacement_output(&path, &inputs, 3);
    let cancel = AtomicBool::new(false);
    let start = std::sync::Barrier::new(2);
    let gate = store.commit_gate.lock();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            start.wait();
            assert!(matches!(
                store.replace_cancellable(&inputs, &[output.clone()], &|| cancel
                    .load(Ordering::Acquire)),
                Err(super::super::Error::CompactionCancelled)
            ));
        });
        start.wait();
        cancel.store(true, Ordering::Release);
        drop(gate);
    });
    assert_eq!(store.index_readers().len(), 2);
    assert!(!path.join("replace-00000300.replace").exists());
    store.ensure_ready().unwrap();
    store.replace(&inputs, &[output]).unwrap();
    assert_eq!(store.index_readers().len(), 1);
}

#[test]
fn checkpoint_recovery_handles_every_durable_boundary() {
    for stage in [
        "checkpoint_prepared",
        "checkpoint_committed",
        "checkpoint_published",
        "history_removed",
    ] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("data");
        let wal = root.path().join("wal");
        let store = installed_pair(&path, &wal);
        let result = store.checkpoint_with_hook(true, &mut |here| {
            if here == stage {
                Err(invalid("injected checkpoint failure"))
            } else {
                Ok(())
            }
        });
        assert!(result.is_err(), "{stage}");
        drop(store);
        let store = IndexStore::open(&path, &wal).unwrap();
        assert_eq!(store.index_readers().len(), 2, "{stage}");
        assert_eq!(store.readers().len(), 2, "{stage}");
        store.ensure_ready().unwrap();
        store.checkpoint(true).unwrap();
    }
}

#[test]
fn checkpoint_abandons_stale_scan_and_preserves_wal_on_corruption() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("data");
    let wal = root.path().join("wal");
    let store = installed_pair(&path, &wal);
    store
        .checkpoint_with_hook(true, &mut |stage| {
            if stage == "references_scanned" {
                let (idx, vlog) = batch(&path, 3, 30);
                store.install(&[idx], &[vlog], &[])?;
            }
            Ok(())
        })
        .unwrap();
    assert!(!path.join("index.checkpoint").exists());
    store.checkpoint(true).unwrap();
    drop(store);
    fs::write(wal.join("keep.wal"), b"unreplayed WAL").unwrap();
    let checkpoint = path.join("index.checkpoint");
    let mut bytes = fs::read(&checkpoint).unwrap();
    bytes[0] ^= 1;
    fs::write(checkpoint, bytes).unwrap();
    assert!(IndexStore::open(&path, &wal).is_err());
    assert_eq!(fs::read(wal.join("keep.wal")).unwrap(), b"unreplayed WAL");
}
