use std::{path::Path, sync::Arc};

use crate::{
    db::{DrainWaitOutcome, LeasedFloatIterator, LeasedQueryIterator, ReefsDb},
    query::{
        query_iterator,
        storage_ir::{
            ColumnKind, ColumnRef, MeasurementRef, OutputOrder, SeriesProjection,
            StorageProjection, StorageScan, TimeOrder, TimeRange,
        },
    },
    runtime::execution::{DefaultTokioExecutor, DefaultTokioExecutorOptions, ExecutorRef},
    series::{MEASUREMENT_SCHEMA_FILE_NAME, MeasurementSchemaRegistry, series_file::SeriesFile},
    storage::{EngineOptions, StorageOptions, active_cursor as iterator},
};

fn test_executor() -> ExecutorRef {
    Arc::new(DefaultTokioExecutor::new(DefaultTokioExecutorOptions::default()).unwrap())
}

fn open_series_file(path: &Path, executor: ExecutorRef) -> Arc<SeriesFile> {
    let mut series_file = SeriesFile::new_with_executor(path, executor);
    series_file.open().unwrap();
    Arc::new(series_file)
}

fn open_reefsdb(root: &Path, series_file: Arc<SeriesFile>, executor: ExecutorRef) -> Arc<ReefsDb> {
    let measurement_schemas = Arc::new(
        MeasurementSchemaRegistry::open(series_file.path().join(MEASUREMENT_SCHEMA_FILE_NAME))
            .unwrap(),
    );
    let mut options =
        EngineOptions::new_with_executor(StorageOptions::default(), Arc::clone(&executor));
    options.shard_id = 7;
    options.compaction_disabled = true;
    options.monitor_disabled = true;
    options.config.wal_dir = root.join("wal").to_string_lossy().into_owned();
    let reefsdb = Arc::new(
        ReefsDb::new_with_executor(
            root.join("data"),
            root.join("wal"),
            root.join("vlog"),
            root.join("vlog_index_wal"),
            series_file,
            measurement_schemas,
            options,
            executor,
        )
        .unwrap(),
    );
    reefsdb.open().unwrap();
    reefsdb
}

fn value_scan() -> StorageScan {
    let value = ColumnRef::new("value", ColumnKind::Field(Some(crate::models::FLOAT)));
    StorageScan {
        measurement: MeasurementRef::new("cpu"),
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

fn float_iterator(iterator: LeasedQueryIterator) -> LeasedFloatIterator {
    let iterator = try_float_iterator!(iterator, query_iterator::FloatMergeIter);
    let iterator = try_float_iterator!(iterator, query_iterator::FloatInterruptIter);
    let iterator = try_float_iterator!(iterator, query_iterator::FloatLimitIter);
    let iterator = try_float_iterator!(iterator, query_iterator::FloatSortedMergeIter);
    let iterator = try_float_iterator!(iterator, iterator::FloatIter);
    let _iterator = try_float_iterator!(iterator, iterator::FloatLimitIter);
    panic!("unexpected float iterator type")
}

fn scan_values(reefsdb: &Arc<ReefsDb>) -> Vec<(i64, f64)> {
    let iterator = reefsdb
        .create_storage_iterator(value_scan())
        .unwrap()
        .unwrap();
    let mut iterator = float_iterator(iterator);
    let values = std::iter::from_fn(|| query_iterator::FloatIterator::next(&mut iterator))
        .map(|point| (point.time(), point.value()))
        .collect();
    query_iterator::QueryIterator::close(&mut iterator);
    values
}

#[test]
fn standalone_reefsdb_write_scan_snapshot_reopen_and_delete() {
    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();
    let series_path = root.path().join("_series");
    let series_file = open_series_file(&series_path, Arc::clone(&executor));
    let reefsdb = open_reefsdb(root.path(), Arc::clone(&series_file), Arc::clone(&executor));
    let (mut points, _) = crate::models::points::parse_points_string(
        "cpu,host=a value=1.0 10\ncpu,host=a value=2.0 20\ncpu,host=a value=3.0 30",
    )
    .unwrap();
    reefsdb.write_points(&mut points).unwrap();
    assert_eq!(reefsdb.measurement_names().unwrap(), vec![b"cpu".to_vec()]);
    let schema = reefsdb.measurement_schema("cpu").unwrap().unwrap();
    assert_eq!(
        schema.tag_keys().iter().cloned().collect::<Vec<_>>(),
        vec!["host"]
    );
    assert_eq!(schema.fields().get("value"), Some(&crate::models::FLOAT));
    assert_eq!(scan_values(&reefsdb), vec![(10, 1.0), (20, 2.0), (30, 3.0)]);

    let snapshot = reefsdb.create_snapshot(false).unwrap();
    assert!(snapshot.exists());
    reefsdb.close().unwrap();
    series_file.close().unwrap();

    let reopened_series_file = open_series_file(&series_path, Arc::clone(&executor));
    let reopened = open_reefsdb(root.path(), Arc::clone(&reopened_series_file), executor);
    assert_eq!(reopened.measurement_schema("cpu").unwrap(), Some(schema));
    assert_eq!(
        scan_values(&reopened),
        vec![(10, 1.0), (20, 2.0), (30, 3.0)]
    );

    reopened
        .delete_series_range_by_measurement(
            b"cpu",
            Some(|_name: &[u8], _tags: &crate::models::Tags| (15, 25, true)),
        )
        .unwrap();
    assert_eq!(scan_values(&reopened), vec![(10, 1.0), (30, 3.0)]);

    reopened.close().unwrap();
    reopened_series_file.close().unwrap();
}

#[test]
fn aggregate_index_recovers_queries_after_tag_index_directory_is_lost() {
    let check_projected = |db: &Arc<ReefsDb>, expected: f64| {
        use crate::{AggregateKind, AggregateNumber, AggregateRequest};
        let request = AggregateRequest { measurement: crate::MeasurementRef::new("cpu"),
            field: "value".into(), time_range: crate::TimeRange::all(), aggregates: vec![AggregateKind::Sum] };
        let result = db.aggregate_projected_scan(request.clone()).unwrap().unwrap();
        assert_eq!(result.state.sum, Some(AggregateNumber::Float(expected)));
        assert_eq!(result.state.count, None);
        assert_eq!(result.state.min, None);
        assert_eq!(result.state, db.acquire_operation().unwrap().aggregate_projected_scan(request).unwrap().unwrap().state);
        assert_eq!(db.active_operation_count(), 0);
    };
    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();
    let series_path = root.path().join("_series");
    let measurement_schemas = Arc::new(
        MeasurementSchemaRegistry::open(series_path.join(MEASUREMENT_SCHEMA_FILE_NAME)).unwrap(),
    );
    let open = |series_file: Arc<SeriesFile>| {
        let mut options =
            EngineOptions::new_with_executor(StorageOptions::default(), Arc::clone(&executor));
        options.vlog_enabled = true;
        options.agg_index_enabled = true;
        options.vlog_config.max_vlog_writers = 1;
        options.compaction_disabled = true;
        options.monitor_disabled = true;
        let db = Arc::new(
            ReefsDb::new(
                root.path().join("data"),
                root.path().join("wal"),
                root.path().join("vlog"),
                root.path().join("vlog_index_wal"),
                series_file,
                Arc::clone(&measurement_schemas),
                options,
            )
            .unwrap(),
        );
        db.open().unwrap();
        db
    };
    let series_file = open_series_file(&series_path, Arc::clone(&executor));
    let db = open(Arc::clone(&series_file));
    let (mut points, _) = crate::models::points::parse_points_string(
        "cpu,host=a value=1.0 10\ncpu,host=b value=2.0 20",
    )
    .unwrap();
    db.write_points(&mut points).unwrap();
    db.create_snapshot(false).unwrap();
    let (mut points, _) =
        crate::models::points::parse_points_string("cpu,host=c value=3.0 30").unwrap();
    db.write_points(&mut points).unwrap();
    let expected = scan_values(&db);
    assert_eq!(expected, vec![(10, 1.0), (20, 2.0), (30, 3.0)]);
    check_projected(&db, 6.0);
    db.delete_series_range_by_measurement(b"cpu", Some(|_: &[u8], tags: &crate::models::Tags| {
        (i64::MIN, i64::MAX, tags.iter().any(|t| t.value == b"b"))
    })).unwrap();
    let expected = vec![(10, 1.0), (30, 3.0)];
    check_projected(&db, 4.0);
    db.close().unwrap();
    series_file.close().unwrap();
    std::fs::rename(
        root.path().join("data/index"),
        root.path().join("retired-index"),
    )
    .unwrap();

    let series_file = open_series_file(&series_path, Arc::clone(&executor));
    let db = open(Arc::clone(&series_file));
    assert_eq!(db.measurement_series_keys(b"cpu").unwrap().len(), 2);
    assert_eq!(scan_values(&db), expected);
    check_projected(&db, 4.0);
    let dropped = db.delete_measurement_data(b"cpu").unwrap();
    assert!(scan_values(&db).is_empty());
    assert_eq!(db.measurement_series_keys(b"cpu").unwrap().len(), 2);
    assert!(db.measurement_fields(b"cpu").is_some());
    assert!(db.measurement_exists(b"cpu").unwrap());
    db.finalize_measurement_drop_local(b"cpu").unwrap();
    for id in dropped.into_series_ids() {
        series_file.delete_series_id(id).unwrap();
    }
    measurement_schemas.remove_measurement_schema("cpu").unwrap();
    assert!(!db.measurement_exists(b"cpu").unwrap());
    assert!(db.measurement_fields(b"cpu").is_none());
    assert!(db.acquire_operation().unwrap().field_keys_by_measurement(b"cpu").is_empty());
    assert_eq!(db.local_field_type(b"cpu", b"value").unwrap(), None);
    let (mut points, _) = crate::models::points::parse_points_string("cpu,host=a value=4.0 40").unwrap();
    db.write_points(&mut points).unwrap();
    assert!(db.measurement_fields(b"cpu").is_some());
    assert_eq!(scan_values(&db), vec![(40, 4.0)]);
    db.create_snapshot(false).unwrap();
    db.close().unwrap();
    series_file.close().unwrap();
    std::fs::remove_dir_all(root.path().join("data/index")).unwrap();
    let series_file = open_series_file(&series_path, Arc::clone(&executor));
    let db = open(Arc::clone(&series_file));
    assert_eq!(scan_values(&db), vec![(40, 4.0)]);
    check_projected(&db, 4.0);
    db.close().unwrap();
    series_file.close().unwrap();
}

#[test]
fn standalone_reefsdb_compaction_and_lifecycle_facades() {
    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();
    let series_file = open_series_file(&root.path().join("_series"), Arc::clone(&executor));
    let reefsdb = open_reefsdb(root.path(), Arc::clone(&series_file), executor);

    reefsdb.set_compactions_enabled(false);
    reefsdb.set_compactions_enabled(true);
    reefsdb.schedule_full_compaction().unwrap();

    let lease = reefsdb.acquire_operation().unwrap();
    let lease_clone = lease.clone();
    assert_eq!(reefsdb.active_operation_count(), 1);
    reefsdb.begin_retention_drain();
    assert!(reefsdb.acquire_operation().is_err());
    assert!(lease.measurement_names().unwrap().is_empty());
    drop(lease);
    assert_eq!(reefsdb.active_operation_count(), 1);
    drop(lease_clone);
    assert_eq!(reefsdb.active_operation_count(), 0);
    assert_eq!(
        reefsdb.wait_for_drain(None, || false),
        DrainWaitOutcome::Drained
    );
    reefsdb.close_after_drain().unwrap();
    series_file.close().unwrap();
}
