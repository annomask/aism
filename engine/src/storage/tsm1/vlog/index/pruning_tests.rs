use super::super::raw_cursor_tests::install_values;
use super::*;
use crate::models::points::{FloatValue, IntegerValue, UnsignedValue};
use crate::query::storage_ir::AggregateKind;

fn value(typ: FieldType, time: i64, value: i64) -> Value {
    match typ {
        INTEGER => Value::IntegerValue(IntegerValue::new(time, value)),
        UNSIGNED => Value::UnsignedValue(UnsignedValue::new(time, value as u64)),
        FLOAT => Value::FloatValue(FloatValue::new(time, value as f64 / 8.0)),
        _ => unreachable!(),
    }
}

fn expected(
    values: &[Value],
    ranges: &[TimeRange],
    typ: FieldType,
    projection: AggregateProjection,
) -> Vec<ProjectedAggregateState> {
    ranges
        .iter()
        .map(|range| {
            let mut state = ProjectedAggregateState::empty(typ, projection);
            for value in values
                .iter()
                .filter(|value| range.contains(value.unix_nano()))
            {
                state.add_value(value.clone()).unwrap();
            }
            state
        })
        .collect()
}

fn query(
    store: &IndexStore,
    ranges: &[TimeRange],
    typ: FieldType,
    kinds: &[AggregateKind],
) -> ProjectedAggregateWindowsResult {
    store
        .aggregate_projected_windows(
            SeqNo::new(1, 1).unwrap(),
            typ,
            ranges,
            vec![],
            AggregateProjection::from_aggregates(kinds),
        )
        .unwrap()
}

#[test]
#[ignore = "single-client execution diagnostic; run in release with --ignored --nocapture"]
fn aggregate_execution_single_client_profile() {
    use std::time::Instant;
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("vlog");
    let store = IndexStore::open(&dir, &root.path().join("wal")).unwrap();
    let step = 10_000_000_000_i64;
    let values = (0..17_280)
        .map(|i| Value::FloatValue(FloatValue::new(i * step, ((i * 7919) % 10007) as f64 / 8.0)))
        .collect::<Vec<_>>();
    install_values(
        &store,
        &dir,
        1,
        values.chunks(160).map(<[Value]>::to_vec).collect(),
    );
    for (label, ranges) in [
        (
            "hourly_offset",
            (0..24)
                .map(|h| TimeRange::new((17 + h * 360) * step, (17 + (h + 1) * 360) * step - 1))
                .collect::<Vec<_>>(),
        ),
        (
            "daily_aligned",
            (0..2)
                .map(|d| TimeRange::new(d * 8640 * step, (d + 1) * 8640 * step - 1))
                .collect::<Vec<_>>(),
        ),
    ] {
        for kinds in [
            vec![AggregateKind::Count],
            vec![AggregateKind::Sum],
            vec![AggregateKind::Avg],
            vec![AggregateKind::Min],
            vec![AggregateKind::Max],
            vec![
                AggregateKind::Count,
                AggregateKind::Sum,
                AggregateKind::Avg,
                AggregateKind::Min,
                AggregateKind::Max,
            ],
        ] {
            let expected = expected(
                &values,
                &ranges,
                FLOAT,
                AggregateProjection::from_aggregates(&kinds),
            );
            for _ in 0..5 {
                assert_eq!(query(&store, &ranges, FLOAT, &kinds).states, expected);
            }
            #[cfg(feature = "slow-trace")]
            {
                let scope = crate::common::slow_trace::QueryProfile::start().unwrap();
                assert_eq!(query(&store, &ranges, FLOAT, &kinds).states, expected);
                println!(
                    "EXECUTION_PROFILE {}",
                    serde_json::json!({
                        "case":label, "operators":format!("{kinds:?}"), "profile":scope.finish(),
                    })
                );
            }
            let mut samples = Vec::new();
            for _ in 0..200 {
                let started = Instant::now();
                let result = query(&store, &ranges, FLOAT, &kinds);
                samples.push(started.elapsed().as_secs_f64() * 1000.0);
                assert_eq!(result.states, expected);
            }
            let probe = query(&store, &ranges, FLOAT, &kinds);
            samples.sort_by(f64::total_cmp);
            let percentile = |p: f64| {
                samples[((samples.len() as f64 * p).ceil() as usize - 1).min(samples.len() - 1)]
            };
            println!(
                "EXECUTION_BENCH {}",
                serde_json::json!({
                    "case":label, "operators":format!("{kinds:?}"), "samples":samples.len(),
                    "series":1,"fields":1,"points":values.len(),"query_workers":1,"write_workers":0,
                    "maintenance_running":false,"cache":"warm persisted", "block_points":160,
                    "average_ms":samples.iter().sum::<f64>() / samples.len() as f64,
                    "p95_ms":percentile(0.95),"p99_ms":percentile(0.99),
                    "p999_ms":percentile(0.999),"p9999_ms":percentile(0.9999),"max_ms":samples.last(),
                    "index_records":probe.metrics.index_records,"decoded_blocks":probe.metrics.decoded_blocks,
                })
            );
        }
    }
}

#[test]
fn count_boundaries_preserve_field_presence_and_visibility_conflicts() {
    use crate::models::{
        BOOL, STRING,
        points::{BoolValue, StringValue},
    };
    for typ in [INTEGER, UNSIGNED, FLOAT, BOOL, STRING] {
        let point = |time| match typ {
            BOOL => Value::BoolValue(BoolValue::new(time, false)),
            STRING => Value::StringValue(StringValue::new(time, String::new())),
            _ => value(typ, time, time),
        };
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("vlog");
        let store = IndexStore::open(&dir, &root.path().join("wal")).unwrap();
        let values: Vec<_> = [0, 2, 4, 6, 8].into_iter().map(point).collect();
        install_values(&store, &dir, 1, vec![values]);
        let ranges = [TimeRange::new(1, 3), TimeRange::new(4, 7)];
        let result = query(&store, &ranges, typ, &[AggregateKind::Count]);
        assert_eq!(
            result
                .states
                .iter()
                .map(|s| s.count.unwrap())
                .collect::<Vec<_>>(),
            [1, 2]
        );
        assert_eq!(result.metrics.decoded_blocks, 1);
        install_values(&store, &dir, 2, vec![vec![point(2), point(3)]]);
        let result = query(&store, &ranges, typ, &[AggregateKind::Count]);
        assert_eq!(
            result
                .states
                .iter()
                .map(|s| s.count.unwrap())
                .collect::<Vec<_>>(),
            [2, 2]
        );
        store
            .delete_ranges(vec![super::super::delete::DeleteRange {
                seq: SeqNo::new(1, 1).unwrap(),
                min: 2,
                max: 3,
                cutoff: 0,
            }])
            .unwrap();
        let result = query(&store, &ranges, typ, &[AggregateKind::Count]);
        assert_eq!(
            result
                .states
                .iter()
                .map(|s| s.count.unwrap())
                .collect::<Vec<_>>(),
            [0, 2]
        );
        let result = store
            .aggregate_projected_windows(
                SeqNo::new(1, 1).unwrap(),
                typ,
                &ranges,
                vec![point(3), point(4)],
                AggregateProjection::from_aggregates(&[AggregateKind::Count]),
            )
            .unwrap();
        assert_eq!(
            result
                .states
                .iter()
                .map(|s| s.count.unwrap())
                .collect::<Vec<_>>(),
            [1, 2]
        );
    }
}

#[test]
fn decoded_index_cache_preserves_projection_and_rechecks_deletions() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("vlog");
    let store = IndexStore::open(&dir, &root.path().join("wal")).unwrap();
    install_values(
        &store,
        &dir,
        1,
        vec![vec![value(INTEGER, 0, 10), value(INTEGER, 2, 20)]],
    );
    let ranges = [TimeRange::new(0, 2)];
    let files = store.index_readers();
    let entry = files[0].lookup(SeqNo::new(1, 1).unwrap()).unwrap().unwrap();
    let header = files[0].segment_header(&entry).unwrap();
    let mut cache = super::super::caches::QueryGroupCache::default();
    let mut metrics = Default::default();
    let records = files[0]
        .read_level_projected_cached(
            &entry,
            &header,
            LEVEL_BLOCK,
            0,
            2,
            &[Column::Sum],
            &mut metrics,
            &mut cache,
        )
        .unwrap();
    let mut cached_metrics = crate::query::storage_ir::AggregateIndexReadMetrics::default();
    let cached = files[0]
        .read_level_projected_cached(
            &entry,
            &header,
            LEVEL_BLOCK,
            0,
            2,
            &[Column::Sum],
            &mut cached_metrics,
            &mut cache,
        )
        .unwrap();
    assert_eq!(cached, records);
    assert_eq!(cached_metrics.decoded_columns, 0);
    let counts = files[0]
        .read_level_projected_cached(
            &entry,
            &header,
            LEVEL_BLOCK,
            0,
            2,
            &[Column::Count],
            &mut cached_metrics,
            &mut cache,
        )
        .unwrap();
    assert_eq!(cached_metrics.decoded_columns, 1);
    assert_eq!(counts[0].values[Column::Sum as usize], None);
    let first = query(&store, &ranges, INTEGER, &[AggregateKind::Sum]);
    let second = query(&store, &ranges, INTEGER, &[AggregateKind::Sum]);
    assert_eq!(first.states, second.states);
    assert!(second.index_reads.decoded_columns > 0);
    let count = query(&store, &ranges, INTEGER, &[AggregateKind::Count]);
    assert_eq!(count.states[0].count, Some(2));
    assert_eq!(count.states[0].sum, None);
    store
        .delete_ranges(vec![super::super::delete::DeleteRange {
            seq: SeqNo::new(1, 1).unwrap(),
            min: 2,
            max: 2,
            cutoff: 0,
        }])
        .unwrap();
    let result = query(&store, &ranges, INTEGER, &[AggregateKind::Sum]);
    assert_eq!(result.states[0].sum, Some(AggregateNumber::Integer(10)));
    assert_eq!(
        query(&store, &ranges, INTEGER, &[AggregateKind::Count]).states[0].count,
        Some(1)
    );
}

#[test]
fn existing_extrema_timestamps_answer_partial_numeric_windows() {
    for typ in [INTEGER, UNSIGNED, FLOAT] {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("vlog");
        let store = IndexStore::open(&dir, &root.path().join("wal")).unwrap();
        let values: Vec<_> = [(0, 10), (2, 100), (4, 1), (6, 60)]
            .into_iter()
            .map(|(t, v)| value(typ, t, v))
            .collect();
        install_values(&store, &dir, 1, vec![values.clone()]);
        for (range, kinds) in [
            (TimeRange::new(1, 3), vec![AggregateKind::Max]),
            (TimeRange::new(3, 5), vec![AggregateKind::Min]),
            (
                TimeRange::new(1, 5),
                vec![AggregateKind::Min, AggregateKind::Max],
            ),
        ] {
            let result = query(&store, &[range], typ, &kinds);
            assert_eq!(
                result.states,
                expected(
                    &values,
                    &[range],
                    typ,
                    AggregateProjection::from_aggregates(&kinds)
                )
            );
            assert_eq!(result.metrics.decoded_blocks, 0);
        }
    }
}

#[test]
fn extrema_pruning_requires_every_window_and_requested_component() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("vlog");
    let store = IndexStore::open(&dir, &root.path().join("wal")).unwrap();
    let values: Vec<_> = [(0, 10), (2, 100), (4, 1), (6, 60)]
        .into_iter()
        .map(|(t, v)| value(INTEGER, t, v))
        .collect();
    install_values(&store, &dir, 1, vec![values.clone()]);
    for kinds in [
        vec![AggregateKind::Max],
        vec![AggregateKind::Min, AggregateKind::Max],
        vec![AggregateKind::Count, AggregateKind::Max],
        vec![AggregateKind::Avg, AggregateKind::Max],
        vec![AggregateKind::First, AggregateKind::Max],
        vec![AggregateKind::Last, AggregateKind::Min],
    ] {
        let ranges = [TimeRange::new(1, 3), TimeRange::new(4, 5)];
        let result = query(&store, &ranges, INTEGER, &kinds);
        assert_eq!(
            result.states,
            expected(
                &values,
                &ranges,
                INTEGER,
                AggregateProjection::from_aggregates(&kinds)
            )
        );
        assert_eq!(result.metrics.decoded_blocks, 1, "{kinds:?}");
    }
}

#[test]
fn prior_extrema_are_window_scoped_and_remain_owned_by_the_caller() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("vlog");
    let store = IndexStore::open(&dir, &root.path().join("wal")).unwrap();
    let values: Vec<_> = [(0, 10), (2, 100), (4, 1), (6, 60)]
        .into_iter()
        .map(|(t, v)| value(INTEGER, t, v))
        .collect();
    install_values(&store, &dir, 1, vec![values.clone()]);
    let ranges = [TimeRange::new(1, 3), TimeRange::new(4, 5)];
    let projection = AggregateProjection::from_aggregates(&[AggregateKind::Max]);
    let mut prior = vec![ProjectedAggregateState::empty(INTEGER, projection); 2];
    prior[1].add_value(value(INTEGER, 4, 200)).unwrap();
    let result = store
        .aggregate_projected_windows_with_bounds(
            SeqNo::new(1, 1).unwrap(),
            INTEGER,
            &ranges,
            vec![],
            projection,
            &mut QueryBlockReader::new(Default::default()),
            Some(&prior),
        )
        .unwrap();
    assert_eq!(result.metrics.decoded_blocks, 0);
    assert_eq!(result.states[0].max, Some(AggregateNumber::Integer(100)));
    assert_eq!(result.states[1].max, None);
    assert_eq!(prior[1].max, Some(AggregateNumber::Integer(200)));
    for (output, part) in prior.iter_mut().zip(result.states) {
        output.merge(part).unwrap();
    }
    assert_eq!(prior[0].max, Some(AggregateNumber::Integer(100)));
    assert_eq!(prior[1].max, Some(AggregateNumber::Integer(200)));
    assert!(
        store
            .aggregate_projected_windows_with_bounds(
                SeqNo::new(1, 1).unwrap(),
                INTEGER,
                &ranges,
                vec![],
                projection,
                &mut QueryBlockReader::new(Default::default()),
                Some(&prior[..1])
            )
            .is_err()
    );
    prior[0].field_type = FLOAT;
    assert!(
        store
            .aggregate_projected_windows_with_bounds(
                SeqNo::new(1, 1).unwrap(),
                INTEGER,
                &ranges,
                vec![],
                projection,
                &mut QueryBlockReader::new(Default::default()),
                Some(&prior)
            )
            .is_err()
    );
}

#[test]
fn stale_extrema_never_bypass_overwrites_tombstones_or_cache() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("vlog");
    let store = IndexStore::open(&dir, &root.path().join("wal")).unwrap();
    install_values(
        &store,
        &dir,
        1,
        vec![vec![
            value(INTEGER, 0, 10),
            value(INTEGER, 2, 100),
            value(INTEGER, 4, 1),
            value(INTEGER, 6, 60),
        ]],
    );
    install_values(&store, &dir, 2, vec![vec![value(INTEGER, 2, 20)]]);
    let ranges = [TimeRange::new(1, 3)];
    let result = query(&store, &ranges, INTEGER, &[AggregateKind::Max]);
    assert_eq!(result.states[0].max, Some(AggregateNumber::Integer(20)));
    assert_eq!(result.metrics.decoded_blocks, 2);
    store
        .delete_ranges(vec![super::super::delete::DeleteRange {
            seq: SeqNo::new(1, 1).unwrap(),
            min: 2,
            max: 2,
            cutoff: 0,
        }])
        .unwrap();
    let result = query(&store, &ranges, INTEGER, &[AggregateKind::Max]);
    assert_eq!(result.states[0].max, None);
    assert_eq!(result.metrics.decoded_blocks, 1);
    let result = store
        .aggregate_projected_windows(
            SeqNo::new(1, 1).unwrap(),
            INTEGER,
            &ranges,
            vec![value(INTEGER, 2, 30)],
            AggregateProjection::from_aggregates(&[AggregateKind::Max]),
        )
        .unwrap();
    assert_eq!(result.states[0].max, Some(AggregateNumber::Integer(30)));
    assert_eq!(result.metrics.decoded_blocks, 1);
}

#[test]
fn extrema_match_raw_with_sparse_windows_and_multiple_tree_levels() {
    for typ in [INTEGER, UNSIGNED, FLOAT] {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("vlog");
        let store = IndexStore::open(&dir, &root.path().join("wal")).unwrap();
        let blocks: Vec<Vec<_>> = (0..72)
            .map(|b| {
                (0..13)
                    .map(|p| value(typ, b * 17 + p, (b * 17 + p * 11) % 101))
                    .collect()
            })
            .collect();
        let values: Vec<_> = blocks.iter().flatten().cloned().collect();
        install_values(&store, &dir, 1, blocks);
        for start in (0..18).map(|i| i * 61) {
            let ranges = [
                TimeRange::new(start + 1, start + 8),
                TimeRange::new(start + 11, start + 39),
            ];
            for kinds in [
                vec![AggregateKind::Min],
                vec![AggregateKind::Max],
                vec![AggregateKind::Min, AggregateKind::Max],
                vec![AggregateKind::Sum, AggregateKind::Max],
            ] {
                let result = query(&store, &ranges, typ, &kinds);
                assert_eq!(
                    result.states,
                    expected(
                        &values,
                        &ranges,
                        typ,
                        AggregateProjection::from_aggregates(&kinds)
                    ),
                    "{typ} {start} {kinds:?}"
                );
            }
        }
    }
}

#[test]
fn extrema_witness_times_must_stay_inside_their_record() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("vlog");
    let store = IndexStore::open(&dir, &root.path().join("wal")).unwrap();
    install_values(
        &store,
        &dir,
        1,
        vec![vec![value(INTEGER, 0, 10), value(INTEGER, 2, 100)]],
    );
    let files = store.index_readers();
    let entry = files[0].lookup(SeqNo::new(1, 1).unwrap()).unwrap().unwrap();
    let header = files[0].segment_header(&entry).unwrap();
    let sources = [SegmentSource {
        file: &files[0],
        entry,
        header,
    }];
    let projection = AggregateProjection::from_aggregates(&[AggregateKind::Max]);
    let columns = columns(INTEGER, projection).unwrap();
    let mut records = files[0]
        .read_level_projected(
            &sources[0].entry,
            &sources[0].header,
            LEVEL_BLOCK,
            0,
            2,
            &columns,
            &mut Default::default(),
        )
        .unwrap();
    records[0].values[Column::MaxAt as usize] = Some(ColumnValue::Time(3));
    let ranges = [TimeRange::new(1, 1)];
    let mut walk = Walk {
        sources: &sources,
        windows: AggregateWindows::new(&ranges).unwrap(),
        cache_times: &[],
        endpoints: false,
        typ: INTEGER,
        projection,
        columns: columns.clone(),
        leaf_columns: columns,
        allow_summaries: true,
        source_ranges: vec![vec![(sources[0].entry.min_start, sources[0].entry.max_end)]],
        result: ProjectedAggregateWindowsResult {
            states: vec![ProjectedAggregateState::empty(INTEGER, projection)],
            metrics: Default::default(),
            index_reads: Default::default(),
        },
        deletes: vec![files[0].tombstone_view(SeqNo::new(1, 1).unwrap())],
        prior_states: None,
        group_cache: Default::default(),
    };
    assert!(walk.seed_extrema(0, &records).is_err());
}
