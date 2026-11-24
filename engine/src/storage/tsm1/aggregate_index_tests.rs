use super::*;
use crate::query::storage_ir::{
    MeasurementRef, OutputOrder, SeriesProjection, StorageProjection, TimeOrder, TimeRange,
};

#[test]
fn aggregate_recovery_restores_cache_admission_limits() {
    let dir = tempfile::tempdir().unwrap();
    for limit in [0, 64] {
        let cache = Cache::new(limit);
        Engine::reload_cache(&cache, dir.path()).unwrap();
        assert_eq!(cache.max_size(), limit);
        Engine::reload_vlog_cache(&cache, dir.path()).unwrap();
        assert_eq!(cache.max_size(), limit);
        if limit != 0 {
            assert!(
                cache
                    .write(
                        b"cpu#!~#i",
                        (0..5)
                            .map(|time| Value::IntegerValue(IntegerValue::new(time, 1)))
                            .collect()
                    )
                    .is_err()
            );
        }
        let index_cache = IndexCache::new(limit);
        Engine::reload_index_cache(&index_cache, dir.path()).unwrap();
        assert_eq!(index_cache.max_size(), limit);
    }
}

pub(super) fn scan(e: &tests::Engine, field: &str, ascending: bool) -> Vec<(i64, Val)> {
    scan_series(e, "cpu,host=A", field, ascending)
}

fn scan_series(e: &tests::Engine, series: &str, field: &str, ascending: bool) -> Vec<(i64, Val)> {
    let Some(mut cursor) = series_cursor(e, series, field, ascending) else {
        return Vec::new();
    };
    let values = cursor.by_ref().collect();
    close_cursor(cursor);
    values
}

// Raw reads must retain the original VLog cursor's ordering and duplicate
// behavior. Aggregate-result checks below use a separate value oracle.
fn assert_native_rows(
    indexed: &tests::Engine,
    native: &tests::Engine,
    series: &str,
    field: &str,
) -> Vec<(i64, Val)> {
    let ascending = scan_series(native, series, field, true);
    assert_eq!(scan_series(indexed, series, field, true), ascending);
    assert_eq!(
        scan_series(indexed, series, field, false),
        scan_series(native, series, field, false)
    );
    ascending
}

pub(super) fn close_cursor(cursor: Cursor) {
    let mut cursor = super::super::iterator::BufCursor::new(Some(cursor), true);
    super::super::iterator::CursorAt::close(&mut cursor);
}

pub(super) fn cursor(e: &tests::Engine, field: &str, ascending: bool) -> Cursor {
    series_cursor(e, "cpu,host=A", field, ascending).expect("registered test field")
}

fn series_cursor(e: &tests::Engine, series: &str, field: &str, ascending: bool) -> Option<Cursor> {
    let column = ColumnRef::new(field, ColumnKind::AnyField);
    let scan = StorageScan {
        measurement: MeasurementRef::new("cpu"),
        projection: StorageProjection {
            primary_column: Some(column.clone()),
            returned_columns: vec![column.clone()],
            row_domain_columns: vec![],
            series: SeriesProjection::new(vec![], vec![]),
        },
        time_range: TimeRange::all(),
        predicate: None,
        time_order: if ascending {
            TimeOrder::Ascending
        } else {
            TimeOrder::Descending
        },
        output_order: OutputOrder::TimeOrdered,
        execution: Default::default(),
        cancellation: None,
    };
    let plan = Arc::new(EngineScanPlan {
        scan: Arc::new(scan),
        vlog_enabled: true,
    });
    let (cache, store, vlog) = {
        let inner = e.engine.inner.read();
        (
            Arc::clone(&inner.cache),
            Arc::clone(&inner.file_store),
            inner.vlog_inner.clone(),
        )
    };
    Engine::build_cursor(
        vlog,
        &cache,
        &e.engine.fieldset,
        &store,
        "cpu",
        series,
        crate::models::Tags::default(),
        &column,
        &plan,
    )
    .unwrap()
}

pub(super) fn aggregate(e: &tests::Engine, field: &str, range: TimeRange) -> AggregateResult {
    e.engine
        .aggregate_scan(AggregateRequest {
            measurement: MeasurementRef::new("cpu"),
            field: field.into(),
            time_range: range,
            aggregates: vec![
                AggregateKind::Count,
                AggregateKind::First,
                AggregateKind::Last,
            ],
        })
        .unwrap()
        .unwrap()
}

#[test]
fn aggregate_index_mixed_lifecycle_matches_reference_after_each_transition() {
    let mut e = tests::Engine::with_aggregate_index();
    let mut native = tests::Engine::with_native_vlog();
    for (field, typ) in [
        ("i", crate::models::INTEGER),
        ("u", crate::models::UNSIGNED),
    ] {
        for engine in [&e, &native] {
            engine
                .engine
                .measurement_fields(b"cpu")
                .create_field_if_not_exists(field.as_bytes(), typ)
                .unwrap();
        }
    }
    e.engine.fieldset.save().unwrap();
    native.engine.fieldset.save().unwrap();
    let mut expected = std::collections::BTreeMap::<(usize, usize, i64), i64>::new();
    let mut seed = 0x5eed_u64;
    for round in 0..24 {
        let mut lines = Vec::new();
        for n in 0..20 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let host = ((seed >> 32) % 3) as usize;
            let time = ((seed >> 8) % 64) as i64 * 3 + host as i64;
            let field = n % 2;
            let value = (round * 100 + n) as i64;
            let suffix = if field == 0 { "i" } else { "u" };
            let name = if field == 0 { "i" } else { "u" };
            lines.push(format!(
                "cpu,host={} {name}={value}{suffix} {time}",
                ["A", "B", "C"][host]
            ));
            expected.insert((host, field, time), value);
        }
        for engine in [&e, &native] {
            engine
                .write_points_string(&lines.iter().map(String::as_str).collect::<Vec<_>>())
                .unwrap();
        }
        if round % 2 == 0 {
            e.must_write_snapshot();
            native.must_write_snapshot();
        }
        if round % 3 == 0 {
            let host = (round / 3) % 3;
            let min = (round * 7 % 100) as i64;
            let max = min + 45;
            for engine in [&e, &native] {
                engine
                    .engine
                    .delete_series_range_by_measurement(
                        b"cpu",
                        Some(|_: &[u8], tags: &Tags| {
                            (
                                min,
                                max,
                                tags.iter().any(|t| t.value == [b"A", b"B", b"C"][host]),
                            )
                        }),
                    )
                    .unwrap();
            }
            expected.retain(|(h, _, t), _| *h != host || *t < min || *t > max);
        }
        if round % 4 == 0 {
            eprintln!("native lifecycle compaction: round={round}");
            aggregate_native_tests::compact_native_all(&native);
            aggregate_native_tests::compact(&e);
            // Original data compaction chooses duplicate winners. Reuse those
            // winners as the oracle instead of imposing an AISM merge policy.
            expected.clear();
            for host in 0..3 {
                let series = format!("cpu,host={}", ["A", "B", "C"][host]);
                for (field, name) in [(0, "i"), (1, "u")] {
                    for (time, value) in scan_series(&native, &series, name, true) {
                        let value = match value {
                            Val::I64(v) => v,
                            Val::U64(v) => v as i64,
                            _ => panic!("unexpected native field value"),
                        };
                        expected.insert((host, field, time), value);
                    }
                }
            }
        }
        if round % 6 == 0 {
            e.reopen();
            native.engine.fieldset.save().unwrap();
            native.reopen();
        }
        for (field, name, typ) in [
            (0, "i", crate::models::INTEGER),
            (1, "u", crate::models::UNSIGNED),
        ] {
            for host in 0..3 {
                let series = format!("cpu,host={}", ["A", "B", "C"][host]);
                assert_native_rows(&e, &native, &series, name);
            }
            for range in [TimeRange::all(), TimeRange::new(33, 147)] {
                let mut want = AggregateState::empty(typ);
                let mut rows = expected
                    .iter()
                    .filter(|((_, f, t), _)| *f == field && range.contains(*t))
                    .map(|((_, _, t), v)| (*t, *v))
                    .collect::<Vec<_>>();
                rows.sort_unstable();
                for (time, value) in rows {
                    if field == 0 {
                        want.add_integer_at(time, value);
                    } else {
                        want.add_unsigned_at(time, value as u64);
                    }
                }
                assert_eq!(
                    aggregate(&e, name, range).state,
                    want,
                    "round={round} field={name}"
                );
                for kinds in [
                    vec![AggregateKind::Sum],
                    vec![AggregateKind::Avg, AggregateKind::First, AggregateKind::Max],
                ] {
                    let projection =
                        crate::query::storage_ir::AggregateProjection::from_aggregates(&kinds);
                    let result = e
                        .engine
                        .aggregate_projected_scan(AggregateRequest {
                            measurement: MeasurementRef::new("cpu"),
                            field: name.into(),
                            time_range: range,
                            aggregates: kinds,
                        })
                        .unwrap()
                        .unwrap();
                    assert_eq!(
                        result.state,
                        crate::query::storage_ir::ProjectedAggregateState::from_complete(
                            want.clone(),
                            projection
                        ),
                        "projected lifecycle round={round} field={name}"
                    );
                }
            }
        }
        let keys = (0..3)
            .filter(|host| expected.keys().any(|(h, _, _)| h == host))
            .map(|host| format!("cpu,host={}", ["A", "B", "C"][host]).into_bytes())
            .collect::<Vec<_>>();
        assert_eq!(e.engine.measurement_series_keys(b"cpu").unwrap(), keys);
    }
    native.engine.delete_measurement(b"cpu").unwrap();
    aggregate_native_tests::compact_native_all(&native);
    e.engine.delete_measurement(b"cpu").unwrap();
    aggregate_native_tests::compact(&e);
    e.reopen();
    assert!(e.engine.measurement_names().unwrap().is_empty());
    assert_eq!(e.engine.series_n(), 0);
    e.close();
    native.close();
}

#[test]
fn aggregate_index_queries_match_raw_cache_boundaries_and_overwrites() {
    let mut e = tests::Engine::with_aggregate_index();
    let mut native = tests::Engine::with_native_vlog();
    for (name, typ) in [
        ("i", crate::models::INTEGER),
        ("u", crate::models::UNSIGNED),
        ("f", crate::models::FLOAT),
        ("b", crate::models::BOOL),
        ("s", crate::models::STRING),
    ] {
        for engine in [&e, &native] {
            engine
                .engine
                .measurement_fields(b"cpu")
                .create_field_if_not_exists(name.as_bytes(), typ)
                .unwrap();
        }
    }
    for batch in 0..3 {
        let lines = (1..=19)
            .map(|i| {
                let t = batch * 13 + i;
                format!(
                    "cpu,host=A i={}i,u={}u,f={}.5,b={},s=\"batch{}\" {}",
                    t + batch,
                    t + batch,
                    t + batch,
                    t % 2 == 0,
                    batch,
                    t
                )
            })
            .collect::<Vec<_>>();
        for engine in [&e, &native] {
            engine
                .write_points_string(&lines.iter().map(String::as_str).collect::<Vec<_>>())
                .unwrap();
        }
        if batch != 2 {
            e.must_write_snapshot();
            native.must_write_snapshot();
        }
    }
    for persisted in [false, true] {
        if persisted {
            e.must_write_snapshot();
            native.must_write_snapshot();
            e.reopen();
            native.engine.fieldset.save().unwrap();
            native.reopen();
        }
        for field in ["i", "u", "f", "b", "s"] {
            let typ = e
                .engine
                .measurement_fields(b"cpu")
                .field(field)
                .unwrap()
                .typ;
            let raw = assert_native_rows(&e, &native, "cpu,host=A", field);
            // Aggregation resolves timestamp overwrites even though the original
            // raw cursor can expose both immutable versions before compaction.
            let rows = raw
                .into_iter()
                .collect::<std::collections::BTreeMap<_, _>>();
            assert_eq!(rows.len(), 45, "{field} {persisted}");
            for (start, end) in [(i64::MIN, i64::MAX), (5, 31), (13, 13), (18, 19), (46, 90)] {
                let range = TimeRange::new(start, end);
                let mut expected = AggregateState::empty(typ);
                for (t, v) in &rows {
                    if !range.contains(*t) {
                        continue;
                    }
                    match v {
                        Val::I64(v) => expected.add_integer_at(*t, *v),
                        Val::U64(v) => expected.add_unsigned_at(*t, *v),
                        Val::F64(v) => expected.add_float_at(*t, *v),
                        Val::Bool(v) => expected.add_boolean_at(*t, *v),
                        Val::String(v) => expected.add_string_at(*t, v.clone()),
                        _ => panic!("unexpected field value"),
                    }
                }
                assert_eq!(
                    aggregate(&e, field, range).state,
                    expected,
                    "{field} {start} {end} {persisted}"
                );
                let kinds = if matches!(typ, crate::models::BOOL | crate::models::STRING) {
                    vec![
                        AggregateKind::Count,
                        AggregateKind::First,
                        AggregateKind::Last,
                    ]
                } else {
                    vec![
                        AggregateKind::Count,
                        AggregateKind::Sum,
                        AggregateKind::Avg,
                        AggregateKind::Min,
                        AggregateKind::Max,
                        AggregateKind::First,
                        AggregateKind::Last,
                    ]
                };
                for kind in kinds {
                    let request = AggregateRequest {
                        measurement: MeasurementRef::new("cpu"),
                        field: field.into(),
                        time_range: range,
                        aggregates: vec![kind],
                    };
                    let projection = crate::query::storage_ir::AggregateProjection::from_aggregates(
                        &request.aggregates,
                    );
                    let result = e.engine.aggregate_projected_scan(request).unwrap().unwrap();
                    assert_eq!(
                        result.state,
                        crate::query::storage_ir::ProjectedAggregateState::from_complete(
                            expected.clone(),
                            projection
                        ),
                        "projected {kind:?} {field} {start} {end} {persisted}"
                    );
                }
                // COUNT requests still return the complete public numeric state.
                // Only endpoint collection is optional in the existing API.
                expected.first = None;
                expected.last = None;
                let count_only = e
                    .engine
                    .aggregate_scan(AggregateRequest {
                        measurement: MeasurementRef::new("cpu"),
                        field: field.into(),
                        time_range: range,
                        aggregates: vec![AggregateKind::Count],
                    })
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    count_only.state, expected,
                    "count projection {field} {start} {end} {persisted}"
                );
            }
        }
    }
    e.close();
    native.close();
}

#[test]
fn aggregate_index_delete_reconciles_visible_metadata_and_reinsertion() {
    let mut e = tests::Engine::with_aggregate_index();
    e.engine
        .measurement_fields(b"cpu")
        .create_field_if_not_exists(b"i", crate::models::INTEGER)
        .unwrap();
    e.write_points_string(&["cpu,host=A i=1i 10", "cpu,host=B i=2i 20"])
        .unwrap();
    e.must_write_snapshot();
    let identity = e
        .engine
        .aggregate_index
        .as_ref()
        .unwrap()
        .sequence(b"cpu,host=A#!~#i")
        .unwrap();
    let mut keys = ["host".to_string()];
    assert_eq!(
        e.engine
            .measurement_tag_key_values(b"cpu", &mut keys, true)
            .unwrap(),
        [vec!["A".to_string(), "B".to_string()]]
    );
    e.engine
        .delete_series_range_by_measurement(
            b"cpu",
            Some(|_: &[u8], tags: &Tags| {
                (i64::MIN, i64::MAX, tags.iter().any(|t| t.value == b"A"))
            }),
        )
        .unwrap();
    assert_eq!(e.engine.series_n(), 1);
    assert_eq!(
        e.engine.measurement_series_keys(b"cpu").unwrap(),
        [b"cpu,host=B".to_vec()]
    );
    assert_eq!(
        e.engine
            .measurement_tag_key_values(b"cpu", &mut keys, true)
            .unwrap(),
        [vec!["B".to_string()]]
    );
    e.engine.delete_measurement(b"cpu").unwrap();
    assert_eq!(e.engine.series_n(), 0);
    assert!(e.engine.measurement_names().unwrap().is_empty());
    assert!(e.engine.measurement_field_keys(b"cpu").is_empty());
    assert!(e.engine.measurement_tag_keys(b"cpu").unwrap().is_empty());
    e.reopen();
    e.engine.reindex().unwrap();
    assert_eq!(e.engine.series_n(), 0);
    assert!(e.engine.measurement_names().unwrap().is_empty());
    e.write_points_string(&["cpu,host=A i=3i 30"]).unwrap();
    assert_eq!(
        e.engine
            .aggregate_index
            .as_ref()
            .unwrap()
            .sequence(b"cpu,host=A#!~#i")
            .unwrap(),
        identity
    );
    assert_eq!(e.engine.series_n(), 1);
    assert_eq!(scan(&e, "i", true), [(30, Val::I64(3))]);
    e.must_write_snapshot();
    aggregate_native_tests::compact(&e);
    e.reopen();
    assert_eq!(scan(&e, "i", false), [(30, Val::I64(3))]);
    e.close();
}

#[test]
fn aggregate_index_full_range_uses_summary_and_partial_range_decodes_leaves() {
    let mut e = tests::Engine::with_aggregate_index();
    e.engine
        .measurement_fields(b"cpu")
        .create_field_if_not_exists(b"i", crate::models::INTEGER)
        .unwrap();
    let lines = (1..=8100)
        .map(|i| format!("cpu,host=A i={i}i {i}"))
        .collect::<Vec<_>>();
    e.write_points_string(&lines.iter().map(String::as_str).collect::<Vec<_>>())
        .unwrap();
    e.must_write_snapshot();
    let full = aggregate(&e, "i", TimeRange::all());
    assert_eq!(full.state.count, 8100);
    assert_eq!(full.metrics.index_records, 3);
    assert_eq!(full.metrics.decoded_blocks, 0);
    let part = aggregate(&e, "i", TimeRange::new(2, 8099));
    assert_eq!(part.state.count, 8098);
    assert_eq!(part.metrics.index_records, 1);
    assert_eq!(part.metrics.boundary_blocks, 2);
    assert_eq!(part.metrics.decoded_blocks, 2);
    e.close();
}

#[test]
fn aggregate_index_partial_compaction_defers_overwrites_to_native_compactor() {
    let mut e = tests::Engine::with_aggregate_index();
    let mut native = tests::Engine::with_native_vlog();
    for engine in [&e, &native] {
        engine
            .engine
            .measurement_fields(b"cpu")
            .create_field_if_not_exists(b"i", crate::models::INTEGER)
            .unwrap();
    }
    for version in 1..=3 {
        for engine in [&e, &native] {
            engine
                .write_points_string(&[
                    &format!("cpu,host=A i={}i 10", version),
                    &format!("cpu,host=A i={}i {}", version, version * 10 + 10),
                ])
                .unwrap();
            engine.must_write_snapshot();
        }
    }
    let store = &e.engine.aggregate_index.as_ref().unwrap().store;
    let inputs = store.index_readers();
    let generation = e
        .engine
        .inner
        .read()
        .vlog_inner
        .as_ref()
        .unwrap()
        .read()
        .vlog_file_store
        .next_generation();
    let before = assert_native_rows(&e, &native, "cpu,host=A", "i");
    store
        .compact_files(&inputs[..2], generation, &|| false)
        .unwrap();
    // A hierarchy-only pass leaves overlapping native locators untouched.
    assert_eq!(store.index_readers().len(), inputs.len());
    assert_eq!(scan(&e, "i", true), before);
    assert_eq!(aggregate(&e, "i", TimeRange::all()).state.count, 4);
    drop(inputs);
    e.reopen();
    native.engine.fieldset.save().unwrap();
    native.reopen();
    assert_native_rows(&e, &native, "cpu,host=A", "i");
    aggregate_native_tests::compact(&e);
    aggregate_native_tests::compact_native_all(&native);
    assert_native_rows(&e, &native, "cpu,host=A", "i");
    assert_eq!(aggregate(&e, "i", TimeRange::all()).state.count, 4);
    e.close();
    native.close();
}

#[test]
fn aggregate_index_rebuilds_series_metadata_from_disk_and_wal() {
    let mut e = tests::Engine::with_aggregate_index();
    for field in ["i", "j"] {
        e.engine
            .measurement_fields(b"cpu")
            .create_field_if_not_exists(field.as_bytes(), crate::models::INTEGER)
            .unwrap();
    }
    e.write_points_string(&["cpu,host=A i=1i,j=2i 10", "cpu,host=B i=3i,j=4i 20"])
        .unwrap();
    e.must_write_snapshot();
    e.write_points_string(&["cpu,host=C i=5i,j=6i 30"]).unwrap();
    let seq = e
        .engine
        .aggregate_index
        .as_ref()
        .unwrap()
        .sequence(b"cpu,host=A#!~#i")
        .unwrap();
    let expected = ["i", "j"].map(|field| aggregate(&e, field, TimeRange::all()).state);
    for host in ["A", "B", "C"] {
        let composite = format!("cpu,host={host}#!~#i");
        let id = e
            .engine
            .aggregate_index
            .as_ref()
            .unwrap()
            .sequence(composite.as_bytes())
            .unwrap()
            .series_id();
        e.engine
            .index
            .drop_series(id, format!("cpu,host={host}").as_bytes(), false)
            .unwrap();
    }
    assert!(
        e.engine
            .index
            .drop_measurement_if_series_not_exist(b"cpu")
            .unwrap()
    );
    e.engine.reindex().unwrap();
    for (field, expected) in ["i", "j"].into_iter().zip(expected) {
        assert_eq!(aggregate(&e, field, TimeRange::all()).state, expected);
    }
    assert_eq!(
        e.engine
            .aggregate_index
            .as_ref()
            .unwrap()
            .sequence(b"cpu,host=A#!~#i")
            .unwrap(),
        seq
    );
    e.reopen();
    assert_eq!(aggregate(&e, "i", TimeRange::all()).state.count, 3);
    e.close();
}

#[test]
fn aggregate_index_reindex_rejects_retired_series_identity() {
    let mut e = tests::Engine::with_aggregate_index();
    e.engine
        .measurement_fields(b"cpu")
        .create_field_if_not_exists(b"i", crate::models::INTEGER)
        .unwrap();
    e.write_points_string(&["cpu,host=A i=1i 10"]).unwrap();
    e.must_write_snapshot();
    let seq = e
        .engine
        .aggregate_index
        .as_ref()
        .unwrap()
        .sequence(b"cpu,host=A#!~#i")
        .unwrap();
    e.sfile.delete_series_id(seq.series_id()).unwrap();
    let error = e.engine.reindex().unwrap_err();
    assert!(error.to_string().contains("series identity"));
    e.close();
}

#[test]
fn aggregate_index_deletes_many_cached_fields_without_forcing_flush() {
    let mut e = tests::Engine::with_aggregate_index();
    for field in 0..32 {
        e.engine
            .measurement_fields(b"cpu")
            .create_field_if_not_exists(format!("f{field}").as_bytes(), crate::models::INTEGER)
            .unwrap();
    }
    let mut lines = Vec::new();
    for host in 0..144 {
        for time in [10, 20] {
            let fields = (0..32)
                .map(|field| format!("f{field}={time}i"))
                .collect::<Vec<_>>()
                .join(",");
            lines.push(format!("cpu,host={host} {fields} {time}"));
        }
    }
    e.write_points_string(&lines.iter().map(String::as_str).collect::<Vec<_>>())
        .unwrap();
    assert!(
        e.engine
            .aggregate_index
            .as_ref()
            .unwrap()
            .store
            .index_readers()
            .is_empty()
    );
    e.engine
        .delete_series_range_by_measurement(
            b"cpu",
            Some(|_: &[u8], tags: &crate::models::Tags| {
                let host = std::str::from_utf8(tags.get(b"host").unwrap())
                    .unwrap()
                    .parse::<u64>()
                    .unwrap();
                let time = if host % 2 == 0 { 20 } else { 10 };
                (time, time, host != 0)
            }),
        )
        .unwrap();
    let verify = |e: &tests::Engine| {
        let context = e.engine.aggregate_index.as_ref().unwrap();
        for (host, expected) in [
            (0, vec![10, 20]),
            (1, vec![20]),
            (2, vec![10]),
            (143, vec![20]),
        ] {
            for field in ["f0", "f31"] {
                let key = format!("cpu,host={host}#!~#{field}");
                let seq = context.sequence(key.as_bytes()).unwrap();
                let result = context
                    .store
                    .aggregate(
                        seq,
                        crate::models::INTEGER,
                        TimeRange::all(),
                        e.engine.inner.read().cache.values(key.as_bytes()).0,
                        true,
                    )
                    .unwrap();
                assert_eq!(result.state.count, expected.len() as u64);
                assert_eq!(result.state.first.as_ref().unwrap().time, expected[0]);
                assert_eq!(
                    result.state.last.as_ref().unwrap().time,
                    *expected.last().unwrap()
                );
            }
        }
        assert_eq!(aggregate(e, "f0", TimeRange::all()).state.count, 145);
        assert_eq!(aggregate(e, "f31", TimeRange::all()).state.count, 145);
    };
    assert!(
        e.engine
            .aggregate_index
            .as_ref()
            .unwrap()
            .store
            .index_readers()
            .is_empty()
    );
    verify(&e);
    e.reopen();
    verify(&e);
    e.write_points_string(&["cpu,host=1 f0=99i 10"]).unwrap();
    e.must_write_snapshot();
    e.reopen();
    assert_eq!(aggregate(&e, "f0", TimeRange::all()).state.count, 146);
    assert_eq!(aggregate(&e, "f31", TimeRange::all()).state.count, 145);
    e.close();
}

#[test]
fn aggregate_index_deletes_disk_and_cache_but_preserves_subsequent_writes() {
    let mut e = tests::Engine::with_aggregate_index();
    let mut native = tests::Engine::with_native_vlog();
    for (field, typ) in [
        ("i", crate::models::INTEGER),
        ("u", crate::models::UNSIGNED),
        ("f", crate::models::FLOAT),
        ("b", crate::models::BOOL),
        ("s", crate::models::STRING),
    ] {
        for engine in [&e, &native] {
            engine
                .engine
                .measurement_fields(b"cpu")
                .create_field_if_not_exists(field.as_bytes(), typ)
                .unwrap();
        }
    }
    for time in 1..=3 {
        for engine in [&e, &native] {
            engine
                .write_points_string(&[&format!(
                    "cpu,host=A i={time}i,u=2u,f=3.5,b=true,s=\"old\" {time}"
                )])
                .unwrap();
            if time == 2 {
                engine.must_write_snapshot();
            }
        }
    }
    for engine in [&e, &native] {
        engine
            .engine
            .delete_series_range_by_measurement(
                b"cpu",
                Some(|_: &[u8], _: &crate::models::Tags| (2, 3, true)),
            )
            .unwrap();
    }
    for field in ["i", "u", "f", "b", "s"] {
        assert_eq!(scan(&e, field, true).len(), 1);
        assert_eq!(aggregate(&e, field, TimeRange::all()).state.count, 1);
    }
    // The engine-only fixture bypasses shard field validation on reinsertion.
    for engine in [&e, &native] {
        for (field, typ) in [
            ("i", crate::models::INTEGER),
            ("u", crate::models::UNSIGNED),
            ("f", crate::models::FLOAT),
            ("b", crate::models::BOOL),
            ("s", crate::models::STRING),
        ] {
            engine
                .engine
                .measurement_fields(b"cpu")
                .create_field_if_not_exists(field.as_bytes(), typ)
                .unwrap();
        }
    }
    native
        .write_points_string(&["cpu,host=A i=8i,u=8u,f=8.5,b=false,s=\"new\" 2"])
        .unwrap();
    native.must_write_snapshot();
    e.write_points_string(&["cpu,host=A i=8i,u=8u,f=8.5,b=false,s=\"new\" 2"])
        .unwrap();
    e.must_write_snapshot();
    e.engine.schedule_full_compaction().unwrap();
    e.engine.set_compactions_enabled(false);
    aggregate_native_tests::compact(&e);
    aggregate_native_tests::compact_native_all(&native);
    e.reopen();
    native.engine.fieldset.save().unwrap();
    native.reopen();
    for field in ["i", "u", "f", "b", "s"] {
        let rows = scan(&e, field, true);
        assert_eq!(rows.len(), 2, "{field}");
        assert_eq!(rows[1].0, 2);
        assert_native_rows(&e, &native, "cpu,host=A", field);
        assert_eq!(aggregate(&e, field, TimeRange::all()).state.count, 2);
    }
    e.engine.delete_measurement(b"cpu").unwrap();
    e.engine.schedule_full_compaction().unwrap();
    e.engine.set_compactions_enabled(false);
    aggregate_native_tests::compact(&e);
    assert!(
        e.engine
            .aggregate_index
            .as_ref()
            .unwrap()
            .store
            .readers()
            .is_empty()
    );
    e.reopen();
    assert!(scan(&e, "i", true).is_empty());
    e.write_points_string(&["cpu,host=A i=99i 2"]).unwrap();
    e.must_write_snapshot();
    assert_eq!(scan(&e, "i", true), vec![(2, Val::I64(99))]);
    e.close();
    native.close();
}

#[test]
fn aggregate_index_delete_waits_for_existing_snapshot_without_losing_values() {
    let mut e = tests::Engine::with_aggregate_index();
    e.engine
        .measurement_fields(b"cpu")
        .create_field_if_not_exists(b"i", crate::models::INTEGER)
        .unwrap();
    let cache = Arc::clone(&e.engine.inner.read().cache);
    let empty_snapshot = cache.snapshot().unwrap();
    assert_eq!(empty_snapshot.size(), 0);
    e.write_points_string(&["cpu,host=A i=1i 10", "cpu,host=A i=2i 20"])
        .unwrap();
    let engine = &e.engine;
    let started = std::sync::Barrier::new(2);
    std::thread::scope(|scope| {
        let handle = scope.spawn(|| {
            started.wait();
            engine
                .delete_series_range_by_measurement(
                    b"cpu",
                    Some(|_: &[u8], _: &crate::models::Tags| (10, 10, true)),
                )
                .unwrap();
        });
        started.wait();
        std::thread::sleep(Duration::from_millis(30));
        assert!(!handle.is_finished());
        cache.clear_snapshot(true);
        handle.join().unwrap();
    });
    assert_eq!(scan(&e, "i", true), [(20, Val::I64(2))]);
    e.reopen();
    assert_eq!(scan(&e, "i", true), [(20, Val::I64(2))]);
    e.close();
}

#[test]
fn aggregate_index_deletion_filters_failed_snapshot_without_flushing_active_cache() {
    let mut e = tests::Engine::with_aggregate_index();
    e.engine
        .measurement_fields(b"cpu")
        .create_field_if_not_exists(b"i", crate::models::INTEGER)
        .unwrap();
    e.write_points_string(&["cpu,host=A i=1i 10", "cpu,host=A i=2i 20"])
        .unwrap();
    let cache = Arc::clone(&e.engine.inner.read().cache);
    let compactor = Arc::clone(&e.engine.inner.read().compactor);
    compactor.disable_snapshots();
    assert!(Engine::write_snapshot(true, Arc::clone(&e.engine.inner)).is_err());
    compactor.enable_snapshots();
    assert!(cache.has_pending_snapshot_values());
    assert!(
        e.engine
            .aggregate_index
            .as_ref()
            .unwrap()
            .snapshot_wal
            .lock()
            .is_some()
    );
    e.write_points_string(&["cpu,host=A i=99i 10", "cpu,host=A i=3i 30"])
        .unwrap();
    e.engine
        .delete_series_range_by_measurement(b"cpu", Some(|_: &[u8], _: &Tags| (10, 10, true)))
        .unwrap();
    assert!(
        e.engine
            .aggregate_index
            .as_ref()
            .unwrap()
            .store
            .index_readers()
            .is_empty()
    );
    assert_eq!(scan(&e, "i", true), [(20, Val::I64(2)), (30, Val::I64(3))]);
    // Retrying the old snapshot must leave the newer cache's WAL intact.
    e.must_write_snapshot();
    e.reopen();
    assert_eq!(scan(&e, "i", true), [(20, Val::I64(2)), (30, Val::I64(3))]);
    e.write_points_string(&["cpu,host=A i=42i 10"]).unwrap();
    e.must_write_snapshot();
    e.reopen();
    assert_eq!(
        scan(&e, "i", true),
        [(10, Val::I64(42)), (20, Val::I64(2)), (30, Val::I64(3))]
    );
    e.close();
}

#[test]
fn aggregate_index_snapshots_all_types_reopen_and_merge_overwrites() {
    let mut e = tests::Engine::with_aggregate_index();
    let mut native = tests::Engine::with_native_vlog();
    let fields = e.engine.measurement_fields(b"cpu");
    for (name, typ) in [
        ("f", crate::models::FLOAT),
        ("i", crate::models::INTEGER),
        ("u", crate::models::UNSIGNED),
        ("b", crate::models::BOOL),
        ("s", crate::models::STRING),
    ] {
        fields
            .create_field_if_not_exists(name.as_bytes(), typ)
            .unwrap();
        native
            .engine
            .measurement_fields(b"cpu")
            .create_field_if_not_exists(name.as_bytes(), typ)
            .unwrap();
    }
    native
        .write_points_string(&[
            "cpu,host=A f=1.5,i=-2i,u=3u,b=true,s=\"old\" 10",
            "cpu,host=A f=2.5,i=-3i,u=4u,b=false,s=\"second\" 20",
        ])
        .unwrap();
    e.write_points_string(&[
        "cpu,host=A f=1.5,i=-2i,u=3u,b=true,s=\"old\" 10",
        "cpu,host=A f=2.5,i=-3i,u=4u,b=false,s=\"second\" 20",
    ])
    .unwrap();
    let first = ["f", "i", "u", "b", "s"].map(|field| scan(&e, field, true));
    e.must_write_snapshot();
    native.must_write_snapshot();
    for (field, expected) in ["f", "i", "u", "b", "s"].into_iter().zip(first) {
        assert_eq!(scan(&e, field, true), expected, "{field}");
    }
    native
        .write_points_string(&["cpu,host=A f=11.5,i=-12i,u=13u,b=false,s=\"new\" 10"])
        .unwrap();
    e.write_points_string(&["cpu,host=A f=11.5,i=-12i,u=13u,b=false,s=\"new\" 10"])
        .unwrap();
    e.must_write_snapshot();
    native.must_write_snapshot();
    native
        .write_points_string(&["cpu,host=A f=31.5,i=-32i,u=33u,b=true,s=\"wal\" 30"])
        .unwrap();
    e.write_points_string(&["cpu,host=A f=31.5,i=-32i,u=33u,b=true,s=\"wal\" 30"])
        .unwrap();
    let expected = ["f", "i", "u", "b", "s"].map(|field| scan(&e, field, true));
    assert_eq!(
        expected[1],
        vec![
            (10, Val::I64(-2)),
            (10, Val::I64(-12)),
            (20, Val::I64(-3)),
            (30, Val::I64(-32))
        ]
    );
    e.reopen();
    native.engine.fieldset.save().unwrap();
    native.reopen();
    for (field, expected) in ["f", "i", "u", "b", "s"].into_iter().zip(expected) {
        assert_eq!(scan(&e, field, true), expected, "{field}");
        assert_native_rows(&e, &native, "cpu,host=A", field);
        assert_eq!(aggregate(&e, field, TimeRange::all()).state.count, 3);
    }
    e.must_write_snapshot();
    native.must_write_snapshot();
    e.reopen();
    native.engine.fieldset.save().unwrap();
    native.reopen();
    assert_eq!(
        scan(&e, "s", true),
        vec![
            (10, Val::String("old".into())),
            (10, Val::String("new".into())),
            (20, Val::String("second".into())),
            (30, Val::String("wal".into()))
        ]
    );
    let inner = e.engine.inner.read();
    assert!(inner.cache.keys().is_empty());
    // Enriched locators remain in the original cache and are restored by its WAL.
    assert!(
        !inner
            .vlog_inner
            .as_ref()
            .unwrap()
            .read()
            .vlog_index_cache
            .keys()
            .is_empty()
    );
    assert!(
        inner
            .aggregate_index
            .as_ref()
            .unwrap()
            .store
            .readers()
            .len()
            >= 3
    );
    drop(inner);
    e.engine.delete_measurement(b"cpu").unwrap();
    assert!(scan(&e, "i", true).is_empty());
    e.close();
    native.close();
}

#[test]
fn aggregate_index_snapshot_crosses_block_boundaries_and_preserves_ids() {
    let mut e = tests::Engine::with_aggregate_index();
    e.engine
        .measurement_fields(b"cpu")
        .create_field_if_not_exists(b"i", crate::models::INTEGER)
        .unwrap();
    let lines = (1..=8100)
        .map(|i| format!("cpu,host=A i={i}i {i}"))
        .collect::<Vec<_>>();
    e.write_points_string(&lines.iter().map(String::as_str).collect::<Vec<_>>())
        .unwrap();
    let context = e.engine.aggregate_index.as_ref().unwrap();
    let id = context.sequence(b"cpu,host=A#!~#i").unwrap();
    e.must_write_snapshot();
    let locations = context
        .store
        .block_locations(id, crate::models::INTEGER, 1, 8100)
        .unwrap();
    assert_eq!(
        locations.iter().map(|b| b.count).collect::<Vec<_>>(),
        vec![4000, 4000, 100]
    );
    e.reopen();
    assert_eq!(
        e.engine
            .aggregate_index
            .as_ref()
            .unwrap()
            .sequence(b"cpu,host=A#!~#i")
            .unwrap(),
        id
    );
    assert_eq!(
        scan(&e, "i", true),
        (1..=8100).map(|i| (i, Val::I64(i))).collect::<Vec<_>>()
    );
    e.close();
}

#[test]
fn aggregate_index_full_compaction_preserves_all_types_and_restart_visibility() {
    let mut e = tests::Engine::with_aggregate_index();
    let mut native = tests::Engine::with_native_vlog();
    for (name, typ) in [
        ("f", crate::models::FLOAT),
        ("i", crate::models::INTEGER),
        ("u", crate::models::UNSIGNED),
        ("b", crate::models::BOOL),
        ("s", crate::models::STRING),
    ] {
        for engine in [&e, &native] {
            engine
                .engine
                .measurement_fields(b"cpu")
                .create_field_if_not_exists(name.as_bytes(), typ)
                .unwrap();
        }
    }
    for time in 1..=4 {
        for engine in [&e, &native] {
            engine
                .write_points_string(&[&format!(
                    "cpu,host=A f=1.5,i={time}i,u=3u,b=true,s=\"value\" {time}"
                )])
                .unwrap();
            engine.must_write_snapshot();
        }
    }
    let expected = ["f", "i", "u", "b", "s"].map(|name| scan(&e, name, true));
    let store = Arc::clone(&e.engine.aggregate_index.as_ref().unwrap().store);
    assert_eq!(store.index_readers().len(), 4);
    let pinned = store.index_readers();
    let old_paths = pinned
        .iter()
        .map(|f| f.path().to_path_buf())
        .collect::<Vec<_>>();
    e.engine.schedule_full_compaction().unwrap();
    e.engine.set_compactions_enabled(false);
    aggregate_native_tests::compact(&e);
    aggregate_native_tests::compact_native_all(&native);
    assert_eq!(store.index_readers().len(), 1);
    assert!(old_paths.iter().all(|p| p.exists()));
    for (name, values) in ["f", "i", "u", "b", "s"].into_iter().zip(&expected) {
        assert_eq!(scan(&e, name, true), *values);
        assert_native_rows(&e, &native, "cpu,host=A", name);
    }
    drop(pinned);
    store.reap_retired().unwrap();
    assert!(old_paths.iter().all(|p| !p.exists()));
    e.reopen();
    native.engine.fieldset.save().unwrap();
    native.reopen();
    for (name, values) in ["f", "i", "u", "b", "s"].into_iter().zip(expected) {
        assert_eq!(scan(&e, name, true), values);
    }
    e.write_points_string(&["cpu,host=A i=5i 5"]).unwrap();
    native.write_points_string(&["cpu,host=A i=5i 5"]).unwrap();
    native.must_write_snapshot();
    e.must_write_snapshot();
    e.reopen();
    native.engine.fieldset.save().unwrap();
    native.reopen();
    assert_eq!(
        scan(&e, "i", true),
        (1..=5).map(|i| (i, Val::I64(i))).collect::<Vec<_>>()
    );
    assert_native_rows(&e, &native, "cpu,host=A", "i");
    e.close();
    native.close();
}

#[test]
fn aggregate_index_background_compaction_stops_and_restarts_cleanly() {
    let mut e = tests::Engine::with_aggregate_index();
    e.engine
        .measurement_fields(b"cpu")
        .create_field_if_not_exists(b"i", crate::models::INTEGER)
        .unwrap();
    for round in 0..2 {
        for time in round * 8 + 1..=round * 8 + 8 {
            e.write_points_string(&[&format!("cpu,host=A i={time}i {time}")])
                .unwrap();
            e.must_write_snapshot();
        }
        let store = &e.engine.aggregate_index.as_ref().unwrap().store;
        let before = store.index_readers().len();
        e.engine.enable_level_compactions(false);
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while store.index_readers().len() >= before && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        e.engine.disable_level_compactions(false);
        assert!(store.index_readers().len() < before);
        assert!(e.engine.inner.read().done.is_none());
        assert_eq!(scan(&e, "i", true).len(), ((round + 1) * 8) as usize);
    }
    e.close();
}

#[test]
fn aggregate_index_compaction_repairs_overwrites_without_changing_visibility() {
    let mut e = tests::Engine::with_aggregate_index();
    let mut native = tests::Engine::with_native_vlog();
    for engine in [&e, &native] {
        engine
            .engine
            .measurement_fields(b"cpu")
            .create_field_if_not_exists(b"i", crate::models::INTEGER)
            .unwrap();
    }
    for value in 1..=4 {
        for engine in [&e, &native] {
            engine
                .write_points_string(&[&format!("cpu,host=A i={value}i 10")])
                .unwrap();
            engine.must_write_snapshot();
        }
    }
    e.engine.schedule_full_compaction().unwrap();
    e.engine.set_compactions_enabled(false);
    aggregate_native_tests::compact(&e);
    aggregate_native_tests::compact_native_all(&native);
    let store = &e.engine.aggregate_index.as_ref().unwrap().store;
    store.ensure_ready().unwrap();
    assert_eq!(store.index_readers().len(), 1);
    assert_eq!(scan(&e, "i", true), vec![(10, Val::I64(4))]);
    assert_eq!(aggregate(&e, "i", TimeRange::all()).state.count, 1);
    e.reopen();
    native.engine.fieldset.save().unwrap();
    native.reopen();
    assert_eq!(scan(&e, "i", false), vec![(10, Val::I64(4))]);
    e.write_points_string(&["cpu,host=A i=5i 10"]).unwrap();
    native.write_points_string(&["cpu,host=A i=5i 10"]).unwrap();
    native.must_write_snapshot();
    e.must_write_snapshot();
    assert_eq!(
        assert_native_rows(&e, &native, "cpu,host=A", "i"),
        vec![(10, Val::I64(4)), (10, Val::I64(5))]
    );
    assert_eq!(
        aggregate(&e, "i", TimeRange::all())
            .state
            .first
            .unwrap()
            .value,
        crate::query::storage_ir::AggregatePointValue::Integer(5)
    );
    e.close();
    native.close();
}

#[test]
fn aggregate_index_planner_does_not_repeat_deletes_inside_timestamp_gaps() {
    let mut e = tests::Engine::with_aggregate_index();
    e.engine
        .measurement_fields(b"cpu")
        .create_field_if_not_exists(b"i", crate::models::INTEGER)
        .unwrap();
    e.write_points_string(&[
        "cpu,host=A i=1i 1",
        "cpu,host=A i=5i 5",
        "cpu,host=A i=10i 10",
    ])
    .unwrap();
    e.must_write_snapshot();
    e.engine
        .delete_series_range_by_measurement(b"cpu", Some(|_: &[u8], _: &Tags| (5, 5, true)))
        .unwrap();
    let store = &e.engine.aggregate_index.as_ref().unwrap().store;
    // The original VLog tombstone drives data retirement; no second index
    // tombstone is written just to schedule a duplicate cleanup.
    assert!(
        vlog::index::planner::select_inputs(store)
            .unwrap()
            .is_empty()
    );
    assert_eq!(aggregate(&e, "i", TimeRange::all()).state.count, 2);
    e.engine.schedule_full_compaction().unwrap();
    e.engine.set_compactions_enabled(false);
    aggregate_native_tests::compact(&e);
    assert!(
        vlog::index::planner::select_inputs(store)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        scan(&e, "i", true),
        vec![(1, Val::I64(1)), (10, Val::I64(10))]
    );
    e.reopen();
    let store = &e.engine.aggregate_index.as_ref().unwrap().store;
    assert!(
        vlog::index::planner::select_inputs(store)
            .unwrap()
            .is_empty()
    );
    e.close();
}
