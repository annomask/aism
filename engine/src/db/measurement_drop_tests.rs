use std::{path::Path, sync::Arc, time::Duration};

use crate::{
    DefaultTokioExecutor, DefaultTokioExecutorOptions, EngineOptions, MEASUREMENT_SCHEMA_FILE_NAME,
    MeasurementSchemaRegistry, ReefsDb, SeriesFile, StorageOptions,
    query::{
        query_iterator,
        storage_ir::{
            ColumnKind, ColumnRef, MeasurementRef, OutputOrder, SeriesProjection,
            StorageProjection, StorageScan, TimeOrder, TimeRange,
        },
    },
    storage::active_cursor as iterator,
};

fn test_executor() -> crate::ExecutorRef {
    Arc::new(DefaultTokioExecutor::new(DefaultTokioExecutorOptions::default()).unwrap())
}

fn open_series_file(path: &Path, executor: crate::ExecutorRef) -> Arc<SeriesFile> {
    let mut series_file = SeriesFile::new_with_executor(path, executor);
    series_file.open().unwrap();
    Arc::new(series_file)
}

fn canonical_registry(series_file: &SeriesFile) -> Arc<MeasurementSchemaRegistry> {
    Arc::new(
        MeasurementSchemaRegistry::open(series_file.path().join(MEASUREMENT_SCHEMA_FILE_NAME))
            .unwrap(),
    )
}

fn open_shard(
    root: &Path,
    series_file: Arc<SeriesFile>,
    schemas: Arc<MeasurementSchemaRegistry>,
    executor: crate::ExecutorRef,
) -> Arc<ReefsDb> {
    open_shard_with_aggregate_index(root, series_file, schemas, executor, false)
}

fn open_shard_with_aggregate_index(
    root: &Path,
    series_file: Arc<SeriesFile>,
    schemas: Arc<MeasurementSchemaRegistry>,
    executor: crate::ExecutorRef,
    aggregate_index: bool,
) -> Arc<ReefsDb> {
    let mut options =
        EngineOptions::new_with_executor(StorageOptions::default(), Arc::clone(&executor));
    options.shard_id = 1;
    options.compaction_disabled = true;
    options.monitor_disabled = true;
    options.vlog_enabled = aggregate_index;
    options.agg_index_enabled = aggregate_index;
    options.vlog_config.max_vlog_writers = 1;
    let shard = Arc::new(
        ReefsDb::new_with_executor(
            root.join("data"),
            root.join("wal"),
            root.join("vlog"),
            root.join("vlog-index-wal"),
            series_file,
            schemas,
            options,
            executor,
        )
        .unwrap(),
    );
    shard.open().unwrap();
    shard
}

fn value_scan(measurement: &str) -> StorageScan {
    let value = ColumnRef::new("value", ColumnKind::Field(Some(crate::FLOAT)));
    StorageScan {
        measurement: MeasurementRef::new(measurement),
        projection: StorageProjection {
            primary_column: Some(value.clone()),
            returned_columns: vec![value],
            row_domain_columns: Vec::new(),
            series: SeriesProjection::new(vec!["host".to_string()], vec!["host".to_string()]),
        },
        time_range: TimeRange::all(),
        predicate: None,
        time_order: TimeOrder::Ascending,
        output_order: OutputOrder::TimeOrdered,
        execution: Default::default(),
        cancellation: None,
    }
}

macro_rules! try_float_iterator {
    ($iterator:expr, $ty:ty) => {
        match $iterator.try_downcast_float::<$ty>() {
            Ok(iterator) => return iterator,
            Err(iterator) => iterator,
        }
    };
}

fn float_iterator(iterator: super::LeasedQueryIterator) -> super::LeasedFloatIterator {
    let iterator = try_float_iterator!(iterator, query_iterator::FloatMergeIter);
    let iterator = try_float_iterator!(iterator, query_iterator::FloatInterruptIter);
    let iterator = try_float_iterator!(iterator, query_iterator::FloatLimitIter);
    let iterator = try_float_iterator!(iterator, query_iterator::FloatSortedMergeIter);
    let _iterator = try_float_iterator!(iterator, iterator::FloatIter);
    panic!("unexpected float iterator type")
}

fn scan_values(shard: &Arc<ReefsDb>, measurement: &str) -> Vec<(i64, f64)> {
    let Some(iterator) = shard
        .create_storage_iterator(value_scan(measurement))
        .unwrap()
    else {
        return Vec::new();
    };
    let mut iterator = float_iterator(iterator);
    let values = std::iter::from_fn(|| query_iterator::FloatIterator::next(&mut iterator))
        .map(|point| (point.time(), point.value()))
        .collect();
    query_iterator::QueryIterator::close(&mut iterator);
    values
}

fn local_metadata(shard: &ReefsDb, name: &[u8]) -> (bool, Vec<String>) {
    let inner = shard.inner.read();
    let engine = inner.engine().unwrap();
    (
        engine.measurement_exists(name).unwrap(),
        engine.measurement_field_keys(name),
    )
}

#[test]
fn aggregate_measurement_data_drop_preserves_retry_metadata_across_compaction_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();
    let series_path = root.path().join("_series");
    let series_file = open_series_file(&series_path, Arc::clone(&executor));
    let schemas = canonical_registry(&series_file);
    let shard = open_shard_with_aggregate_index(
        root.path(), Arc::clone(&series_file), Arc::clone(&schemas), Arc::clone(&executor), true,
    );
    shard.write_points_string(&[
        "cpu,host=a value=1.0 10", "memory,host=a value=5.0 10",
    ]).unwrap();
    shard.create_snapshot(false).unwrap();
    shard.write_points_string(&["cpu,host=a value=2.0 20"]).unwrap();
    let deleted = shard.delete_measurement_data(b"cpu").unwrap();
    assert_eq!(deleted.series_ids().len(), 1);
    assert!(scan_values(&shard, "cpu").is_empty());
    assert_eq!(local_metadata(&shard, b"cpu"), (true, vec!["value".into()]));
    assert_eq!(shard.delete_measurement_data(b"cpu").unwrap(), deleted);

    shard.schedule_full_compaction().unwrap();
    for _ in 0..100 {
        if shard.is_idle() { break; }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(shard.is_idle());
    assert_eq!(local_metadata(&shard, b"cpu"), (true, vec!["value".into()]));
    assert_eq!(scan_values(&shard, "memory"), vec![(10, 5.0)]);
    shard.close().unwrap();
    drop(shard);
    series_file.close().unwrap();
    drop(series_file);
    drop(schemas);

    let series_file = open_series_file(&series_path, Arc::clone(&executor));
    let schemas = canonical_registry(&series_file);
    let shard = open_shard_with_aggregate_index(
        root.path(), Arc::clone(&series_file), Arc::clone(&schemas), Arc::clone(&executor), true,
    );
    assert!(scan_values(&shard, "cpu").is_empty());
    assert_eq!(local_metadata(&shard, b"cpu"), (true, vec!["value".into()]));
    assert_eq!(shard.delete_measurement_data(b"cpu").unwrap(), deleted);
    assert!(!series_file.is_deleted(deleted.series_ids()[0]));
    assert!(schemas.measurement_schema("cpu").is_some());
    shard.finalize_measurement_drop_local(b"cpu").unwrap();
    for id in deleted.into_series_ids() { series_file.delete_series_id(id).unwrap(); }
    schemas.remove_measurement_schema("cpu").unwrap();
    shard.write_points_string(&["cpu,host=a value=9.0 30"]).unwrap();
    shard.create_snapshot(false).unwrap();
    assert_eq!(scan_values(&shard, "cpu"), vec![(30, 9.0)]);
    shard.close().unwrap();
    series_file.close().unwrap();
}

#[test]
fn measurement_drop_data_and_local_finalize_preserve_global_metadata() {
    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();
    let series_file = open_series_file(&root.path().join("_series"), Arc::clone(&executor));
    let schemas = canonical_registry(&series_file);
    let shard = open_shard(
        root.path(),
        Arc::clone(&series_file),
        Arc::clone(&schemas),
        executor,
    );
    shard
        .write_points_string(&[
            &format!("cpu,host=a value=1.0 {}", crate::MIN_NANO_TIME_I64),
            &format!("cpu,host=a value=2.0 {}", crate::MAX_NANO_TIME_I64),
            "memory,host=a value=3.0 10",
        ])
        .unwrap();

    let cpu_id = series_file.series_id(
        b"cpu",
        &crate::Tags::from(std::collections::HashMap::from([(
            b"host".to_vec(),
            b"a".to_vec(),
        )])),
    );
    assert_ne!(cpu_id, 0);
    assert_eq!(scan_values(&shard, "cpu").len(), 2);

    let first = shard.delete_measurement_data(b"cpu").unwrap();
    assert_eq!(first.series_ids(), &[cpu_id]);
    assert!(scan_values(&shard, "cpu").is_empty());
    assert_eq!(scan_values(&shard, "memory"), vec![(10, 3.0)]);
    assert_eq!(local_metadata(&shard, b"cpu"), (true, vec!["value".into()]));
    assert!(!series_file.is_deleted(cpu_id));
    assert!(schemas.measurement_schema("cpu").is_some());

    let retry = shard.delete_measurement_data(b"cpu").unwrap();
    assert_eq!(retry.series_ids(), &[cpu_id]);
    shard.finalize_measurement_drop_local(b"cpu").unwrap();
    shard.finalize_measurement_drop_local(b"cpu").unwrap();

    assert_eq!(local_metadata(&shard, b"cpu"), (false, Vec::new()));
    assert_eq!(shard.series_n(), 1);
    assert!(!series_file.is_deleted(cpu_id));
    assert!(schemas.measurement_schema("cpu").is_some());
    assert!(schemas.measurement_schema("memory").is_some());

    shard.close().unwrap();
    series_file.close().unwrap();
}

#[test]
fn measurement_drop_tsm_wal_restart_and_compaction_do_not_resurrect_values() {
    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();
    let series_path = root.path().join("_series");
    let series_file = open_series_file(&series_path, Arc::clone(&executor));
    let schemas = canonical_registry(&series_file);
    let shard = open_shard(
        root.path(),
        Arc::clone(&series_file),
        Arc::clone(&schemas),
        Arc::clone(&executor),
    );

    shard
        .write_points_string(&["cpu,host=a value=1.0 10", "memory,host=a value=5.0 10"])
        .unwrap();
    shard.create_snapshot(false).unwrap();
    shard
        .write_points_string(&["cpu,host=a value=2.0 20"])
        .unwrap();
    shard.create_snapshot(false).unwrap();

    shard.delete_measurement_data(b"cpu").unwrap();
    assert!(scan_values(&shard, "cpu").is_empty());
    shard.schedule_full_compaction().unwrap();
    for _ in 0..100 {
        if shard.is_idle() {
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(shard.is_idle(), "full compaction did not reach idle state");
    assert!(scan_values(&shard, "cpu").is_empty());

    shard.close().unwrap();
    drop(shard);
    drop(schemas);
    series_file.close().unwrap();
    drop(series_file);

    let reopened_series = open_series_file(&series_path, Arc::clone(&executor));
    let reopened_schemas = canonical_registry(&reopened_series);
    let reopened = open_shard(
        root.path(),
        Arc::clone(&reopened_series),
        Arc::clone(&reopened_schemas),
        executor,
    );
    assert!(scan_values(&reopened, "cpu").is_empty());
    assert_eq!(scan_values(&reopened, "memory"), vec![(10, 5.0)]);
    assert_eq!(local_metadata(&reopened, b"cpu").0, true);
    assert!(reopened_schemas.measurement_schema("cpu").is_some());

    reopened.delete_measurement_data(b"cpu").unwrap();
    reopened.finalize_measurement_drop_local(b"cpu").unwrap();
    reopened.close().unwrap();
    reopened_series.close().unwrap();
}

#[test]
fn measurement_drop_empty_and_missing_are_retryable_noops() {
    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();
    let series_file = open_series_file(&root.path().join("_series"), Arc::clone(&executor));
    let schemas = canonical_registry(&series_file);
    let shard = open_shard(
        root.path(),
        Arc::clone(&series_file),
        Arc::clone(&schemas),
        executor,
    );
    shard
        .create_measurement_schema(
            crate::MeasurementSchema::new("empty", Vec::new(), Vec::new()).unwrap(),
        )
        .unwrap();

    assert!(
        shard
            .delete_measurement_data(b"empty")
            .unwrap()
            .series_ids()
            .is_empty()
    );
    shard.finalize_measurement_drop_local(b"empty").unwrap();
    assert!(schemas.measurement_schema("empty").is_some());

    assert!(
        shard
            .delete_measurement_data(b"missing")
            .unwrap()
            .series_ids()
            .is_empty()
    );
    shard.finalize_measurement_drop_local(b"missing").unwrap();
    shard.finalize_measurement_drop_local(b"missing").unwrap();
    assert!(schemas.measurement_schema("empty").is_some());
    assert!(schemas.measurement_schema("missing").is_none());
    assert_eq!(series_file.series_count(), 0);

    shard.close().unwrap();
    series_file.close().unwrap();
}

#[test]
fn measurement_can_be_recreated_after_explicit_global_finalization() {
    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();
    let series_file = open_series_file(&root.path().join("_series"), Arc::clone(&executor));
    let schemas = canonical_registry(&series_file);
    let shard = open_shard(
        root.path(),
        Arc::clone(&series_file),
        Arc::clone(&schemas),
        executor,
    );
    shard
        .write_points_string(&["cpu,host=a value=1.0 10", "cpu,host=a value=2.0 20"])
        .unwrap();
    shard.create_snapshot(false).unwrap();

    let dropped = shard.delete_measurement_data(b"cpu").unwrap();
    let old_id = dropped.series_ids()[0];
    shard.finalize_measurement_drop_local(b"cpu").unwrap();
    for id in dropped.into_series_ids() {
        series_file.delete_series_id(id).unwrap();
    }
    assert!(schemas.remove_measurement_schema("cpu").unwrap());

    shard
        .write_points_string(&["cpu,host=a value=9.0 30"])
        .unwrap();
    let new_id = series_file.series_id(
        b"cpu",
        &crate::Tags::from(std::collections::HashMap::from([(
            b"host".to_vec(),
            b"a".to_vec(),
        )])),
    );
    assert_ne!(new_id, 0);
    assert_ne!(new_id, old_id);
    assert_eq!(scan_values(&shard, "cpu"), vec![(30, 9.0)]);

    shard.create_snapshot(false).unwrap();
    assert_eq!(scan_values(&shard, "cpu"), vec![(30, 9.0)]);
    shard.close().unwrap();
    series_file.close().unwrap();
}

#[test]
fn ordinary_partial_delete_keeps_measurement_and_unrelated_points() {
    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();
    let series_file = open_series_file(&root.path().join("_series"), Arc::clone(&executor));
    let schemas = canonical_registry(&series_file);
    let shard = open_shard(
        root.path(),
        Arc::clone(&series_file),
        Arc::clone(&schemas),
        executor,
    );
    shard
        .write_points_string(&[
            "cpu,host=a value=1.0 10",
            "cpu,host=a value=2.0 20",
            "cpu,host=a value=3.0 30",
        ])
        .unwrap();

    shard
        .delete_series_range_by_measurement(
            b"cpu",
            Some(|_: &[u8], _: &crate::Tags| (15, 25, true)),
        )
        .unwrap();

    assert_eq!(scan_values(&shard, "cpu"), vec![(10, 1.0), (30, 3.0)]);
    assert!(schemas.measurement_schema("cpu").is_some());
    assert_eq!(local_metadata(&shard, b"cpu").0, true);

    shard.close().unwrap();
    series_file.close().unwrap();
}
