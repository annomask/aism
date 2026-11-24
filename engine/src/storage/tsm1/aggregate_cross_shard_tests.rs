use super::*;
use crate::query::storage_ir::AggregateKind;

fn request(field: &str, range: TimeRange) -> AggregateRequest {
    let mut aggregates = vec![
        AggregateKind::Count,
        AggregateKind::First,
        AggregateKind::Last,
    ];
    if field != "b" {
        aggregates.extend([
            AggregateKind::Sum,
            AggregateKind::Avg,
            AggregateKind::Min,
            AggregateKind::Max,
        ]);
    }
    AggregateRequest {
        measurement: MeasurementRef::new("cpu"),
        field: field.into(),
        time_range: range,
        aggregates,
    }
}

fn load(engine: &super::super::tests::Engine, start: i64, batches: i64) {
    for (field, typ) in [("i", INTEGER), ("f", FLOAT), ("b", BOOL)] {
        engine
            .engine
            .measurement_fields(b"cpu")
            .create_field_if_not_exists(field.as_bytes(), typ)
            .unwrap();
    }
    for batch in 0..batches {
        let lines = (start + batch * 10..start + (batch + 1) * 10)
            .map(|t| {
                format!(
                    "cpu,host=A i={t}i,f={},b={} {t}",
                    t as f64 / 4.0,
                    t % 2 == 0
                )
            })
            .collect::<Vec<_>>();
        engine
            .write_points_string(&lines.iter().map(String::as_str).collect::<Vec<_>>())
            .unwrap();
        engine.must_write_snapshot();
    }
}

fn prepare(
    engines: &[&super::super::tests::Engine],
    req: &AggregateRequest,
) -> Vec<PreparedShardAggregate> {
    engines
        .iter()
        .zip([TimeRange::new(0, 999), TimeRange::new(1000, 1999)])
        .map(|(e, scope)| {
            e.engine
                .prepare_cross_shard_aggregate(req.clone(), StoragePredicate::Constant(true), scope)
                .unwrap()
                .unwrap()
        })
        .collect()
}

fn verify(
    engines: &[&super::super::tests::Engine],
    field: &str,
    windows: Vec<TimeRange>,
    cache: &Mutex<CrossShardCache>,
) -> CrossShardAggregateResult {
    let req = request(
        field,
        TimeRange::new(windows[0].start_time, windows.last().unwrap().end_time),
    );
    let shards = prepare(engines, &req);
    let got = aggregate_group(&shards, &req, &windows, cache)
        .unwrap()
        .unwrap();
    let projection = AggregateProjection::from_aggregates(&req.aggregates);
    for (w, range) in windows.into_iter().enumerate() {
        let mut want = ProjectedAggregateState::empty(got.result.states[w].field_type, projection);
        for shard in &shards {
            if let Some(part) = shard.part(Some("cpu,host=A"), range).unwrap() {
                want.merge(part.states[0].clone()).unwrap();
            }
        }
        assert_eq!(
            got.result.states[w], want,
            "field={field}, window={range:?}"
        );
    }
    got
}

#[test]
fn cross_shard_queries_survive_compaction_deletes_and_reopen() {
    let mut a = super::super::tests::Engine::with_aggregate_index();
    let mut b = super::super::tests::Engine::with_aggregate_index();
    load(&a, 0, 30);
    load(&b, 1000, 40);
    let cache = Mutex::new(CrossShardCache::default());
    let whole = vec![TimeRange::new(0, 1999)];
    let initial = verify(&[&a, &b], "i", whole.clone(), &cache);
    assert!(initial.cross_shard.cross_shard_records > 0);
    assert_eq!(initial.result.metrics.decoded_blocks, 0);
    assert_eq!(initial.result.states[0].count, Some(700));
    assert_eq!(
        verify(&[&a, &b], "i", whole.clone(), &cache)
            .cross_shard
            .reused_trees,
        1
    );
    for field in ["i", "f", "b"] {
        verify(
            &[&a, &b],
            field,
            vec![TimeRange::new(3, 1037), TimeRange::new(1040, 1398)],
            &cache,
        );
    }
    b.write_points_string(&["cpu,host=A i=9999i,f=7.5,b=false 1055"])
        .unwrap();
    let cached = verify(&[&a, &b], "i", whole.clone(), &cache);
    assert_eq!(cached.cross_shard.fallback_series, 1);
    assert_eq!(cached.result.states[0].count, Some(700));
    b.must_write_snapshot();
    a.engine
        .delete_series_range_by_measurement(b"cpu", Some(|_: &[u8], _: &Tags| (15, 24, true)))
        .unwrap();
    let deleted = verify(&[&a, &b], "i", whole.clone(), &cache);
    assert_eq!(deleted.cross_shard.fallback_series, 1);
    assert_eq!(deleted.result.states[0].count, Some(690));
    super::super::aggregate_native_tests::compact(&a);
    super::super::aggregate_native_tests::compact(&b);
    for reopened in [false, true] {
        if reopened {
            a.reopen();
            b.reopen();
        }
        for field in ["i", "f", "b"] {
            verify(&[&a, &b], field, whole.clone(), &cache);
            verify(
                &[&a, &b],
                field,
                vec![TimeRange::new(7, 1111), TimeRange::new(1112, 1397)],
                &cache,
            );
        }
    }
}

#[test]
fn cross_shard_revision_rejects_inflight_writes_and_deletes() {
    let a = super::super::tests::Engine::with_aggregate_index();
    let b = super::super::tests::Engine::with_aggregate_index();
    load(&a, 0, 1);
    load(&b, 1000, 1);
    let req = request("i", TimeRange::new(0, 1999));
    let shards = prepare(&[&a, &b], &req);
    let old = shards[0].revision().unwrap();
    shards[0].context.active_writers.store(1, Ordering::SeqCst);
    assert!(
        aggregate_group(
            &shards,
            &req,
            &[req.time_range],
            &Mutex::new(CrossShardCache::default())
        )
        .is_err()
    );
    shards[0].context.active_writers.store(0, Ordering::SeqCst);
    a.write_points_string(&["cpu,host=A i=999i 0"]).unwrap();
    assert!(
        aggregate_group(
            &shards,
            &req,
            &[req.time_range],
            &Mutex::new(CrossShardCache::default())
        )
        .is_err(),
        "a write after preparation must invalidate the captured field view"
    );
    a.engine
        .delete_series_range_by_measurement(b"cpu", Some(|_: &[u8], _: &Tags| (0, 1, true)))
        .unwrap();
    assert_ne!(shards[0].revision(), Some(old));
    shards[0].context.deleting.store(true, Ordering::SeqCst);
    assert_eq!(shards[0].revision(), None);
    shards[0].context.deleting.store(false, Ordering::SeqCst);
}

#[test]
fn cross_shard_float_sum_preserves_shard_then_series_reduction() {
    let a = super::super::tests::Engine::with_aggregate_index();
    let b = super::super::tests::Engine::with_aggregate_index();
    load(&a, 0, 0);
    load(&b, 1000, 0);
    a.write_points_string(&["cpu,host=A f=18014398509481984 0", "cpu,host=B f=1 0"])
        .unwrap();
    b.write_points_string(&[
        "cpu,host=A f=-18014398509481984 1000",
        "cpu,host=B f=1 1000",
    ])
    .unwrap();
    let mut req = request("f", TimeRange::new(0, 1999));
    req.aggregates = vec![AggregateKind::Sum];
    for snapshot in [false, true] {
        if snapshot {
            a.must_write_snapshot();
            b.must_write_snapshot();
        }
        let shards = prepare(&[&a, &b], &req);
        let got = aggregate_group(
            &shards,
            &req,
            &[req.time_range],
            &Mutex::new(CrossShardCache::default()),
        )
        .unwrap()
        .unwrap();
        let mut want = ProjectedAggregateState::empty(
            FLOAT,
            AggregateProjection::from_aggregates(&req.aggregates),
        );
        for shard in &shards {
            let part = shard.part(None, req.time_range).unwrap().unwrap();
            want.merge(part.states[0].clone()).unwrap();
        }
        assert_eq!(want.sum, Some(crate::AggregateNumber::Float(0.0)));
        assert_eq!(got.result.states[0], want);
        assert_eq!(got.cross_shard.cross_shard_records, 0);
    }
}
