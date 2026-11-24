use super::*;
use crate::query::storage_ir::TimeRange;

fn load(engine: &tests::Engine, batches: i64) {
    for (field, typ) in [
        ("i", crate::models::INTEGER),
        ("f", crate::models::FLOAT),
        ("b", crate::models::BOOL),
    ] {
        engine
            .engine
            .measurement_fields(b"cpu")
            .create_field_if_not_exists(field.as_bytes(), typ)
            .unwrap();
    }
    for batch in 0..batches {
        let lines = (batch * 100..(batch + 1) * 100)
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

#[track_caller]
pub(super) fn compact_native_all(engine: &tests::Engine) {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_test_writer()
        .try_init();
    let (inner, compactor) = {
        let inner = engine.engine.inner.read();
        (
            Arc::clone(inner.vlog_inner.as_ref().unwrap()),
            Arc::clone(inner.vlog_compactor.as_ref().unwrap()),
        )
    };
    let readers = inner.read().vlog_file_store.reader_snapshot();
    if readers.is_empty() {
        return;
    }
    let success = Arc::new(AtomicI64::new(0));
    let errors = Arc::new(AtomicI64::new(0));
    compactor.enable_vlog_compactions();
    let strategy = VLogDefaultCompactionStrategy {
        group: VLogCompactionGroup(
            readers
                .iter()
                .map(|r| r.path().to_string_lossy().into_owned())
                .collect(),
        ),
        fast: false,
        level: 4,
        duration_stat: Arc::new(AtomicI64::new(0)),
        active_stat: Arc::new(AtomicI64::new(0)),
        success_stat: Arc::clone(&success),
        error_stat: Arc::clone(&errors),
        compactor,
        inner,
    };
    futures::executor::block_on(strategy.apply());
    assert_eq!(
        errors.load(Ordering::SeqCst),
        0,
        "native data compaction, aggregate_enabled={}",
        engine.engine.aggregate_index.is_some()
    );
    assert_eq!(success.load(Ordering::SeqCst), 1);
}

#[track_caller]
pub(super) fn compact(engine: &tests::Engine) {
    compact_native_all(engine);
    engine.engine.compact_aggregate_index().unwrap();
}

#[test]
fn native_aism_snapshot_recovers_original_locator_cache() {
    let mut engine = tests::Engine::with_aggregate_index();
    load(&engine, 3);
    let reference = ["i", "f", "b"].map(|field| aggregate_index_tests::scan(&engine, field, true));
    for values in &reference {
        assert_eq!(values.len(), 300);
    }
    let key = b"cpu,host=A#!~#i";
    let indexes = {
        let inner = engine.engine.inner.read();
        inner
            .vlog_inner
            .as_ref()
            .unwrap()
            .read()
            .vlog_index_cache
            .values(key)
    };
    assert!(
        !indexes.is_empty(),
        "AISM bypassed the original locator cache"
    );
    engine.reopen();
    for (field, expected) in ["i", "f", "b"].iter().zip(&reference) {
        assert_eq!(&aggregate_index_tests::scan(&engine, field, true), expected);
        assert_eq!(
            aggregate_index_tests::aggregate(&engine, field, TimeRange::all())
                .state
                .count,
            300
        );
    }
    let recovered = {
        let inner = engine.engine.inner.read();
        inner
            .vlog_inner
            .as_ref()
            .unwrap()
            .read()
            .vlog_index_cache
            .values(key)
    };
    assert_eq!(recovered, indexes);
}

#[test]
fn native_aism_compaction_appends_metadata_without_rewriting_old_indexes() {
    let mut engine = tests::Engine::with_aggregate_index();
    load(&engine, 8);
    let context = Arc::clone(engine.engine.aggregate_index.as_ref().unwrap());
    let old_metadata = context
        .store
        .index_readers()
        .iter()
        .map(|file| (file.path().to_path_buf(), fs::read(file.path()).unwrap()))
        .collect::<Vec<_>>();
    let reference = ["i", "f", "b"].map(|field| aggregate_index_tests::scan(&engine, field, true));
    let expected = ["i", "f", "b"]
        .map(|field| aggregate_index_tests::aggregate(&engine, field, TimeRange::all()).state);
    let mut pinned = aggregate_index_tests::cursor(&engine, "i", true);
    let (inner, compactor) = {
        let inner = engine.engine.inner.read();
        (
            Arc::clone(inner.vlog_inner.as_ref().unwrap()),
            Arc::clone(inner.vlog_compactor.as_ref().unwrap()),
        )
    };
    let readers = inner.read().vlog_file_store.reader_snapshot();
    let before = readers.len();
    assert!(before >= 8);
    let success = Arc::new(AtomicI64::new(0));
    let errors = Arc::new(AtomicI64::new(0));
    compactor.enable_vlog_compactions();
    let strategy = VLogDefaultCompactionStrategy {
        group: VLogCompactionGroup(
            readers
                .iter()
                .map(|r| r.path().to_string_lossy().into_owned())
                .collect(),
        ),
        fast: false,
        level: 4,
        duration_stat: Arc::new(AtomicI64::new(0)),
        active_stat: Arc::new(AtomicI64::new(0)),
        success_stat: Arc::clone(&success),
        error_stat: Arc::clone(&errors),
        compactor,
        inner: Arc::clone(&inner),
    };
    futures::executor::block_on(strategy.apply());
    assert_eq!(errors.load(Ordering::SeqCst), 0);
    assert_eq!(success.load(Ordering::SeqCst), 1);
    assert!(inner.read().vlog_file_store.count() < before);
    for (path, bytes) in &old_metadata {
        assert_eq!(&fs::read(path).unwrap(), bytes);
    }
    assert_eq!(pinned.by_ref().collect::<Vec<_>>(), reference[0]);
    aggregate_index_tests::close_cursor(pinned);
    for ((field, values), state) in ["i", "f", "b"].iter().zip(&reference).zip(&expected) {
        assert_eq!(&aggregate_index_tests::scan(&engine, field, true), values);
        assert_eq!(
            &aggregate_index_tests::aggregate(&engine, field, TimeRange::all()).state,
            state
        );
    }
    drop(readers);
    drop(inner);
    drop(context);
    engine.reopen();
    for ((field, values), state) in ["i", "f", "b"].iter().zip(&reference).zip(&expected) {
        assert_eq!(&aggregate_index_tests::scan(&engine, field, true), values);
        assert_eq!(
            &aggregate_index_tests::aggregate(&engine, field, TimeRange::all()).state,
            state
        );
    }
}

#[test]
fn native_aism_delete_compaction_and_reopen_keep_native_tombstones() {
    let mut engine = tests::Engine::with_aggregate_index();
    load(&engine, 4);
    engine
        .engine
        .delete_series_range_by_measurement(b"cpu", Some(|_: &[u8], _: &Tags| (50, 249, true)))
        .unwrap();
    engine
        .write_points_string(&["cpu,host=A i=999i,f=99.5,b=false 100"])
        .unwrap();
    engine.must_write_snapshot();
    let reference = ["i", "f", "b"].map(|f| aggregate_index_tests::scan(&engine, f, true));
    for values in &reference {
        assert_eq!(values.len(), 201);
    }
    let aggregates = ["i", "f", "b"]
        .map(|f| aggregate_index_tests::aggregate(&engine, f, TimeRange::all()).state);
    compact(&engine);
    for reopen in [false, true] {
        if reopen {
            engine.reopen();
        }
        for ((field, values), expected) in ["i", "f", "b"].iter().zip(&reference).zip(&aggregates) {
            assert_eq!(&aggregate_index_tests::scan(&engine, field, true), values);
            assert_eq!(
                &aggregate_index_tests::aggregate(&engine, field, TimeRange::all()).state,
                expected
            );
        }
    }
}

#[test]
fn native_aism_lazy_compaction_updates_aggregates_and_recovers_locations() {
    let mut engine = tests::Engine::with_aggregate_options(Default::default(), "lazy");
    // A forced full plan still uses the original CPU-dependent file threshold.
    load(
        &engine,
        EngineOptions::default()
            .vlog_config
            .vlog_compact_full_min_files,
    );
    let reference = ["i", "f", "b"].map(|f| aggregate_index_tests::scan(&engine, f, true));
    let expected = ["i", "f", "b"]
        .map(|f| aggregate_index_tests::aggregate(&engine, f, TimeRange::all()).state);
    let (inner, compactor, planner) = {
        let guard = engine.engine.inner.read();
        let Some(VLogCompactPlanner::VLogLazyPlanner(planner)) =
            &engine.engine.vlog_compaction_plan
        else {
            panic!("expected original lazy planner");
        };
        (
            Arc::clone(guard.vlog_inner.as_ref().unwrap()),
            Arc::clone(guard.vlog_compactor.as_ref().unwrap()),
            Arc::clone(planner),
        )
    };
    planner.force_full();
    let info = planner.plan(Time::from_timestamp(0, 0));
    assert!(!info.files.is_empty());
    let success = Arc::new(AtomicI64::new(0));
    let errors = Arc::new(AtomicI64::new(0));
    compactor.enable_vlog_compactions();
    let strategy = VLogLazyCompactionStrategy {
        compact_info: info,
        fast: false,
        level: 4,
        duration_stat: Arc::new(AtomicI64::new(0)),
        active_stat: Arc::new(AtomicI64::new(0)),
        success_stat: Arc::clone(&success),
        error_stat: Arc::clone(&errors),
        compactor,
        inner,
    };
    futures::executor::block_on(strategy.apply(&planner));
    assert_eq!(errors.load(Ordering::SeqCst), 0);
    assert_eq!(success.load(Ordering::SeqCst), 1);
    drop(planner);
    for reopen in [false, true] {
        if reopen {
            engine.reopen();
        }
        for ((field, values), aggregate) in ["i", "f", "b"].iter().zip(&reference).zip(&expected) {
            assert_eq!(&aggregate_index_tests::scan(&engine, field, true), values);
            assert_eq!(
                &aggregate_index_tests::aggregate(&engine, field, TimeRange::all()).state,
                aggregate
            );
        }
    }
}

#[test]
fn native_aism_prepared_metadata_does_not_replace_unpublished_locations_on_reopen() {
    let mut engine = tests::Engine::with_aggregate_index();
    load(&engine, 2);
    let expected = aggregate_index_tests::scan(&engine, "i", true);
    let aggregate = aggregate_index_tests::aggregate(&engine, "i", TimeRange::all()).state;
    {
        let context = engine.engine.aggregate_index.as_ref().unwrap();
        let key = "cpu,host=A#!~#i";
        let (files, mut blocks) = {
            let guard = engine.engine.inner.read();
            let native = guard.vlog_inner.as_ref().unwrap().read();
            (
                Arc::clone(&native.vlog_file_store),
                native.vlog_index_cache.values(key.as_bytes()).0,
            )
        };
        let summaries = context
            .store
            .compaction_aggregates(context.sequence(key.as_bytes()).unwrap(), &blocks)
            .unwrap();
        let values = blocks
            .iter()
            .map(|b| Arc::clone(&summaries[&(b.generation, b.seq, b.offset)]))
            .collect();
        for block in &mut blocks {
            block.generation += 100_000;
        }
        let mut locators = HashMap::from([(key.into(), blocks)]);
        let outputs = context
            .compaction_index_files(files, &locators, &HashMap::from([(key.into(), values)]))
            .unwrap();
        // Simulate a durable metadata prepare followed by interruption before
        // the original cache/WAL publication. No new native VLog is installed.
        let prepared = context
            .store
            .prepare_native(&outputs, &mut locators)
            .unwrap();
        drop(prepared);
        assert!(context.store.ensure_ready().is_ok());
    }
    engine.reopen();
    assert_eq!(aggregate_index_tests::scan(&engine, "i", true), expected);
    assert_eq!(
        aggregate_index_tests::aggregate(&engine, "i", TimeRange::all()).state,
        aggregate
    );
}

#[test]
fn native_aism_raw_cursor_matches_original_before_and_after_compaction() {
    let mut native = tests::Engine::with_native_vlog();
    let mut indexed = tests::Engine::with_aggregate_index();
    for engine in [&native, &indexed] {
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
        engine.engine.fieldset.save().unwrap();
        engine
            .write_points_string(&[
                "cpu,host=A i=1i,u=1u,f=1.5,b=true,s=\"one\" 10",
                "cpu,host=A i=2i,u=2u,f=2.5,b=false,s=\"two\" 20",
            ])
            .unwrap();
        engine.must_write_snapshot();
        engine
            .write_points_string(&["cpu,host=A i=3i,u=3u,f=3.5,b=false,s=\"new\" 10"])
            .unwrap();
        engine.must_write_snapshot();
    }
    let verify = |native: &tests::Engine, indexed: &tests::Engine| {
        for field in ["i", "u", "f", "b", "s"] {
            for ascending in [true, false] {
                assert_eq!(
                    aggregate_index_tests::scan(indexed, field, ascending),
                    aggregate_index_tests::scan(native, field, ascending),
                    "{field}, ascending={ascending}"
                );
            }
            let state = aggregate_index_tests::aggregate(indexed, field, TimeRange::all()).state;
            assert_eq!(state.count, 2);
            assert_eq!(state.first.as_ref().unwrap().time, 10);
            assert_eq!(state.last.as_ref().unwrap().time, 20);
        }
    };
    verify(&native, &indexed);
    // These are existing raw-cursor semantics, not AISM's aggregation result.
    // Keep this explicit so a metadata change cannot silently change the native path.
    assert_eq!(
        aggregate_index_tests::scan(&native, "i", true),
        [(10, Val::I64(1)), (10, Val::I64(3)), (20, Val::I64(2))]
    );
    assert_eq!(
        aggregate_index_tests::scan(&native, "i", false),
        [(10, Val::I64(3)), (10, Val::I64(1)), (20, Val::I64(2))]
    );
    compact_native_all(&native);
    compact(&indexed);
    verify(&native, &indexed);
    native.reopen();
    indexed.reopen();
    verify(&native, &indexed);
    // The original merger selects the older duplicate in this fixture. Preserve
    // that behavior here; AISM must not introduce its own overwrite policy.
    assert_eq!(
        aggregate_index_tests::scan(&native, "i", true),
        [(10, Val::I64(1)), (20, Val::I64(2))]
    );
}

#[test]
fn native_aism_key_compaction_preserves_native_locators_until_data_retirement() {
    let mut engine = tests::Engine::with_aggregate_index();
    load(&engine, 2);
    engine
        .engine
        .delete_series_range_by_measurement(
            b"cpu",
            Some(|_: &[u8], _: &Tags| (i64::MIN, i64::MAX, true)),
        )
        .unwrap();
    // Key-only maintenance must not remove native locators while the native
    // file store still owns the corresponding tombstoned data files.
    engine.engine.compact_aggregate_index().unwrap();
    for reopen in [false, true] {
        if reopen {
            engine.reopen();
        }
        for field in ["i", "f", "b"] {
            assert!(aggregate_index_tests::scan(&engine, field, true).is_empty());
            assert_eq!(
                aggregate_index_tests::aggregate(&engine, field, TimeRange::all())
                    .state
                    .count,
                0
            );
        }
    }
}

#[test]
fn native_aism_projected_aggregates_match_values_across_compaction_and_reopen() {
    use crate::query::storage_ir::{AggregateProjection, MeasurementRef, ProjectedAggregateState};
    let mut engine = tests::Engine::with_aggregate_index();
    load(&engine, 8);
    for phase in 0..3 {
        if phase == 1 {
            compact(&engine);
        }
        if phase == 2 {
            engine.reopen();
        }
        for (field, typ) in [
            ("i", crate::models::INTEGER),
            ("f", crate::models::FLOAT),
            ("b", crate::models::BOOL),
        ] {
            for (start, end) in [(0, 799), (30, 730), (200, 599), (501, 501), (900, 999)] {
                let mut expected = AggregateState::empty(typ);
                for time in start..=end.min(799) {
                    match field {
                        "i" => expected.add_integer_at(time, time),
                        "f" => expected.add_float_at(time, time as f64 / 4.0),
                        "b" => expected.add_boolean_at(time, time % 2 == 0),
                        _ => unreachable!(),
                    }
                }
                let kinds = if typ == crate::models::BOOL {
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
                    let projection = AggregateProjection::from_aggregates(&[kind]);
                    let actual = engine
                        .engine
                        .aggregate_projected_scan(AggregateRequest {
                            measurement: MeasurementRef::new("cpu"),
                            field: field.into(),
                            time_range: TimeRange::new(start, end),
                            aggregates: vec![kind],
                        })
                        .unwrap()
                        .unwrap();
                    assert_eq!(
                        actual.state,
                        ProjectedAggregateState::from_complete(expected.clone(), projection),
                        "phase={phase} field={field} range={start}..={end} kind={kind:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn native_aism_partial_move_uses_original_record_invalidation_and_replay() {
    let mut engine = tests::Engine::with_aggregate_index();
    load(&engine, 8);
    engine.engine.compact_aggregate_index().unwrap();
    let expected = ["i", "f", "b"]
        .map(|field| aggregate_index_tests::aggregate(&engine, field, TimeRange::all()).state);
    let (inner, compactor) = {
        let guard = engine.engine.inner.read();
        (
            Arc::clone(guard.vlog_inner.as_ref().unwrap()),
            Arc::clone(guard.vlog_compactor.as_ref().unwrap()),
        )
    };
    let readers = inner.read().vlog_file_store.reader_snapshot();
    assert!(readers.len() >= 8);
    let success = Arc::new(AtomicI64::new(0));
    let errors = Arc::new(AtomicI64::new(0));
    compactor.enable_vlog_compactions();
    let strategy = VLogDefaultCompactionStrategy {
        group: VLogCompactionGroup(
            readers[..4]
                .iter()
                .map(|r| r.path().to_string_lossy().into_owned())
                .collect(),
        ),
        fast: false,
        level: 4,
        duration_stat: Arc::new(AtomicI64::new(0)),
        active_stat: Arc::new(AtomicI64::new(0)),
        success_stat: Arc::clone(&success),
        error_stat: Arc::clone(&errors),
        compactor,
        inner,
    };
    futures::executor::block_on(strategy.apply());
    assert_eq!(errors.load(Ordering::SeqCst), 0);
    assert_eq!(success.load(Ordering::SeqCst), 1);
    drop(readers);
    for reopen in [false, true] {
        if reopen {
            engine.reopen();
        }
        for (field, state) in ["i", "f", "b"].iter().zip(&expected) {
            assert_eq!(
                &aggregate_index_tests::aggregate(&engine, field, TimeRange::all()).state,
                state
            );
            assert_eq!(aggregate_index_tests::scan(&engine, field, true).len(), 800);
        }
    }
}

#[test]
fn native_aism_replay_does_not_need_retired_hierarchy_files_or_a_second_journal() {
    let mut engine = tests::Engine::with_aggregate_index();
    load(&engine, 5);
    let context = Arc::clone(engine.engine.aggregate_index.as_ref().unwrap());
    let seq = context.sequence(b"cpu,host=A#!~#i").unwrap();
    let pinned = context.store.native_query_view(seq).unwrap();
    let paths = pinned
        .0
        .iter()
        .map(|f| f.path().to_path_buf())
        .collect::<Vec<_>>();
    let expected = aggregate_index_tests::aggregate(&engine, "i", TimeRange::all()).state;
    compact(&engine);
    assert!(
        paths.iter().all(|p| p.exists()),
        "a captured query still owns the old tree"
    );
    drop(pinned);
    context.store.reap_retired().unwrap();
    assert!(
        paths.iter().all(|p| !p.exists()),
        "unreferenced tree files should be retired"
    );
    for entry in fs::read_dir(paths[0].parent().unwrap()).unwrap() {
        let name = entry.unwrap().file_name().to_string_lossy().into_owned();
        assert!(
            ![".commit", ".installed", ".replace", ".idx.tombstone"]
                .iter()
                .any(|suffix| name.ends_with(suffix)),
            "unexpected independent aggregate journal: {name}"
        );
    }
    drop(context);
    engine.reopen();
    assert_eq!(
        aggregate_index_tests::aggregate(&engine, "i", TimeRange::all()).state,
        expected
    );
    assert_eq!(aggregate_index_tests::scan(&engine, "i", true).len(), 500);
}

#[test]
fn native_aism_continuous_snapshots_with_background_compaction_recover() {
    let mut engine = tests::Engine::with_aggregate_index();
    for (field, typ) in [
        ("i", crate::models::INTEGER),
        ("f", crate::models::FLOAT),
        ("b", crate::models::BOOL),
    ] {
        engine
            .engine
            .measurement_fields(b"cpu")
            .create_field_if_not_exists(field.as_bytes(), typ)
            .unwrap();
    }
    engine.engine.set_compactions_enabled(true);
    for batch in 0..48 {
        let lines = (batch * 100..(batch + 1) * 100)
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
        assert_eq!(
            aggregate_index_tests::aggregate(&engine, "i", TimeRange::all())
                .state
                .count,
            ((batch + 1) * 100) as u64
        );
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while engine.engine.stats.vlog_compactions[0].load(Ordering::SeqCst) == 0
        && std::time::Instant::now() < deadline
    {
        assert_eq!(
            engine.engine.stats.vlog_compaction_errors[0].load(Ordering::SeqCst),
            0
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        engine.engine.stats.vlog_compactions[0].load(Ordering::SeqCst) > 0,
        "the original VLog background worker must actually compact data"
    );
    engine.engine.set_compactions_enabled(false);
    compact(&engine);
    engine.reopen();
    for field in ["i", "f", "b"] {
        assert_eq!(
            aggregate_index_tests::aggregate(&engine, field, TimeRange::all())
                .state
                .count,
            4800
        );
        assert_eq!(
            aggregate_index_tests::scan(&engine, field, true).len(),
            4800
        );
    }
}

#[test]
fn native_aism_backup_restores_locators_and_native_deletes() {
    let mut engine = tests::Engine::with_aggregate_index();
    load(&engine, 5);
    compact(&engine);
    engine
        .engine
        .delete_series_range_by_measurement(b"cpu", Some(|_: &[u8], _: &Tags| (110, 189, true)))
        .unwrap();
    let expected = ["i", "f", "b"]
        .map(|field| aggregate_index_tests::aggregate(&engine, field, TimeRange::all()).state);
    let source = engine
        .engine
        .aggregate_index
        .as_ref()
        .unwrap()
        .store
        .index_readers()[0]
        .path()
        .parent()
        .unwrap()
        .to_path_buf();
    let root = source.parent().unwrap().to_path_buf();
    let snapshot = engine.engine.create_snapshot(false).unwrap();
    engine.engine.close().unwrap();
    fs::rename(&source, root.join("saved-vlog")).unwrap();
    fs::rename(root.join("vlog_index_wal"), root.join("saved-locator-wal")).unwrap();
    fs::rename(snapshot.join("vlog_index_wal"), root.join("vlog_index_wal")).unwrap();
    fs::rename(snapshot, &source).unwrap();
    engine.reopen();
    for (field, state) in ["i", "f", "b"].iter().zip(&expected) {
        assert_eq!(
            &aggregate_index_tests::aggregate(&engine, field, TimeRange::all()).state,
            state
        );
        assert_eq!(aggregate_index_tests::scan(&engine, field, true).len(), 420);
    }
}
