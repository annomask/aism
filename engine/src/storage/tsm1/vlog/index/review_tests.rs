use super::super::raw_cursor_tests::{install_values, install_values_with_identity};
use super::*;
use crate::models::points::{FloatValue, IntegerValue};
use crate::query::storage_ir::AggregateKind;

#[test]
fn cache_overwrite_wins_over_maximum_disk_identity() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("vlog");
    let store = IndexStore::open(&dir, &root.path().join("wal")).unwrap();
    install_values_with_identity(
        &store,
        &dir,
        (1 << 24) - 1,
        255,
        vec![vec![Value::IntegerValue(IntegerValue::new(1, 10))]],
        0,
    );
    let projection = AggregateProjection::complete(INTEGER, true);
    let latest = Value::IntegerValue(IntegerValue::new(1, 20));
    let mut expected = ProjectedAggregateState::empty(INTEGER, projection);
    expected.add_value(latest.clone()).unwrap();
    let actual = store
        .aggregate_projected(
            SeqNo::new(1, 1).unwrap(),
            INTEGER,
            TimeRange::all(),
            vec![latest],
            projection,
        )
        .unwrap();
    assert_eq!(actual.state, expected);
}

#[test]
fn float_sum_preserves_cancellation_across_cache_and_snapshot() {
    for values in [vec![1e308, -1e308, -1e308, 1e308], vec![1e16, -1e16, 1.0]] {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("vlog");
        let store = IndexStore::open(&dir, &root.path().join("wal")).unwrap();
        let points = values
            .iter()
            .enumerate()
            .map(|(i, value)| Value::FloatValue(FloatValue::new(i as i64, *value)))
            .collect::<Vec<_>>();
        let projection = AggregateProjection::from_aggregates(&[AggregateKind::Avg]);
        install_values(&store, &dir, 1, vec![points[..1].to_vec()]);
        let before = store
            .aggregate_projected(
                SeqNo::new(1, 1).unwrap(),
                FLOAT,
                TimeRange::all(),
                points[1..].to_vec(),
                projection,
            )
            .unwrap();
        install_values(&store, &dir, 2, vec![points[1..].to_vec()]);
        let after = store
            .aggregate_projected(
                SeqNo::new(1, 1).unwrap(),
                FLOAT,
                TimeRange::all(),
                vec![],
                projection,
            )
            .unwrap();
        let expected = values.iter().fold(0.0, |sum, value| sum + value);
        assert_eq!(before.state.sum, Some(AggregateNumber::Float(expected)));
        assert_eq!(after.state.sum, before.state.sum);
        assert_eq!(
            after.state.average().unwrap(),
            before.state.average().unwrap()
        );
    }
}

#[test]
fn same_sign_float_sum_keeps_summary_acceleration() {
    for sign in [1.0, -1.0] {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("vlog");
        let store = IndexStore::open(&dir, &root.path().join("wal")).unwrap();
        for generation in 1..=2 {
            install_values(
                &store,
                &dir,
                generation,
                vec![
                    (0..2)
                        .map(|i| {
                            let value = generation * 2 + i;
                            Value::FloatValue(FloatValue::new(value, sign * value as f64))
                        })
                        .collect(),
                ],
            );
        }
        let result = store
            .aggregate_projected(
                SeqNo::new(1, 1).unwrap(),
                FLOAT,
                TimeRange::all(),
                vec![],
                AggregateProjection::from_aggregates(&[AggregateKind::Avg]),
            )
            .unwrap();
        assert_eq!(result.state.sum, Some(AggregateNumber::Float(sign * 14.0)));
        assert_eq!(result.state.count, Some(4));
        assert_eq!(result.metrics.decoded_blocks, 0);
    }
}

#[test]
fn conflict_refinement_preserves_actual_overwrites_and_sparse_gaps() {
    for overlap in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("vlog");
        let store = IndexStore::open(&dir, &root.path().join("wal")).unwrap();
        for generation in 1..=2 {
            install_values(
                &store,
                &dir,
                generation,
                (0..128)
                    .map(|i| {
                        let time = i * 4 + if overlap { 0 } else { generation - 1 };
                        vec![Value::IntegerValue(IntegerValue::new(time, generation))]
                    })
                    .collect(),
            );
        }
        let result = store
            .aggregate_projected(
                SeqNo::new(1, 1).unwrap(),
                INTEGER,
                TimeRange::all(),
                vec![],
                AggregateProjection::from_aggregates(&[AggregateKind::Avg]),
            )
            .unwrap();
        assert_eq!(result.state.count, Some(if overlap { 128 } else { 256 }));
        assert_eq!(
            result.state.sum,
            Some(AggregateNumber::Integer(if overlap { 256 } else { 384 }))
        );
        if !overlap {
            assert_eq!(result.metrics.decoded_blocks, 0);
        }
        let stats = store
            .compact_files(&store.index_readers(), 3, &|| false)
            .unwrap();
        if !overlap {
            assert_eq!(stats.repaired_blocks, 0);
            assert_eq!(stats.streamed_rebuild_fields, 0);
        }
        let compacted = store
            .aggregate_projected(
                SeqNo::new(1, 1).unwrap(),
                INTEGER,
                TimeRange::all(),
                vec![],
                AggregateProjection::from_aggregates(&[AggregateKind::Avg]),
            )
            .unwrap();
        assert_eq!(compacted.state, result.state);
    }
}

#[test]
fn sparse_disjoint_file_blocks_do_not_require_raw_conflict_reduction() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("vlog");
    let store = IndexStore::open(&dir, &root.path().join("wal")).unwrap();
    let block = |start| {
        (start..start + 10)
            .map(|t| Value::IntegerValue(IntegerValue::new(t, 1)))
            .collect::<Vec<_>>()
    };
    install_values(&store, &dir, 1, vec![block(0), block(100)]);
    install_values(&store, &dir, 2, vec![block(50)]);
    let result = store
        .aggregate_projected(
            SeqNo::new(1, 1).unwrap(),
            INTEGER,
            TimeRange::all(),
            vec![],
            AggregateProjection::from_aggregates(&[AggregateKind::Count]),
        )
        .unwrap();
    assert_eq!(result.state.count, Some(30));
    assert_eq!(result.metrics.decoded_blocks, 0);
    assert_eq!(result.metrics.overlap_raw_blocks, 0);
}

#[test]
#[ignore = "paired single-client measurement; run optimized with --ignored --nocapture"]
fn aggregate_review_query_costs() {
    use std::time::Instant;
    for sparse in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("vlog");
        let store = IndexStore::open(&dir, &root.path().join("wal")).unwrap();
        for generation in 1..=2 {
            install_values(
                &store,
                &dir,
                generation,
                (0..128)
                    .map(|b| {
                        let start = if sparse {
                            (b * 2 + generation - 1) * 64
                        } else {
                            ((generation - 1) * 128 + b) * 32
                        };
                        (start..start + 32)
                            .map(|t| Value::IntegerValue(IntegerValue::new(t, 1)))
                            .collect()
                    })
                    .collect(),
            );
        }
        let windows = if sparse {
            vec![TimeRange::new(0, 16383)]
        } else {
            (0..512)
                .map(|w| TimeRange::new(w * 16, w * 16 + 15))
                .collect()
        };
        let projection = AggregateProjection::from_aggregates(&[AggregateKind::Avg]);
        let query = || {
            store
                .aggregate_projected_windows(
                    SeqNo::new(1, 1).unwrap(),
                    INTEGER,
                    &windows,
                    vec![],
                    projection,
                )
                .unwrap()
        };
        for _ in 0..10 {
            std::hint::black_box(query());
        }
        #[cfg(feature = "slow-trace")]
        {
            let scope = slow_trace::QueryProfile::start().unwrap();
            std::hint::black_box(query());
            println!(
                "REVIEW_PROFILE {}",
                serde_json::json!({"sparse":sparse,"profile":scope.finish()})
            );
        }
        let mut samples = Vec::new();
        for _ in 0..200 {
            let started = Instant::now();
            let result = query();
            samples.push(started.elapsed().as_secs_f64() * 1000.0);
            assert_eq!(
                result.states.iter().map(|s| s.count.unwrap()).sum::<u64>(),
                8192
            );
        }
        samples.sort_by(f64::total_cmp);
        let percentile = |p: f64| {
            samples[((samples.len() as f64 * p).ceil() as usize - 1).min(samples.len() - 1)]
        };
        let probe = query();
        println!(
            "REVIEW_TIMING {}",
            serde_json::json!({"sparse":sparse,"points":8192,
            "windows":windows.len(),"writers":0,"query_workers":1,"background_maintenance":false,
            "mean_ms":samples.iter().sum::<f64>()/samples.len() as f64,
            "p95_ms":percentile(0.95),"p99_ms":percentile(0.99),"p999_ms":percentile(0.999),
            "p9999_ms":percentile(0.9999),"max_ms":samples.last(),
            "decoded_blocks":probe.metrics.decoded_blocks,"overlap_raw_blocks":probe.metrics.overlap_raw_blocks})
        );
    }
}
