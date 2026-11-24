use reefsdb::{
    BoxBlockingTask, BoxFutureTask, ColumnKind, ColumnRef, EngineConfig, EngineOptions,
    EngineVLogConfig, ExecutorRef, MeasurementRef, OutputOrder, Point, ReefsDb, RuntimeExecutor,
    SERIES_FILE_DIRECTORY, SeriesFile, SeriesIDSet, SeriesProjection, SpawnError,
    StorageExecutionOptions, StoragePredicate, StorageProjection, StorageScan, TaskController,
    TimeOrder, TimeRange,
};

#[derive(Debug)]
struct ConsumerExecutor;

impl RuntimeExecutor for ConsumerExecutor {
    fn schedule(&self, _task: BoxFutureTask) -> Result<TaskController, SpawnError> {
        Ok(TaskController::detached())
    }

    fn schedule_blocking(&self, _task: BoxBlockingTask) -> Result<TaskController, SpawnError> {
        Ok(TaskController::detached())
    }

    fn schedule_compact(&self, _task: BoxFutureTask) -> Result<TaskController, SpawnError> {
        Ok(TaskController::detached())
    }

    fn schedule_blocking_compact(
        &self,
        _task: BoxBlockingTask,
    ) -> Result<TaskController, SpawnError> {
        Ok(TaskController::detached())
    }
}

#[test]
fn public_facade_supports_external_storage_construction_types() {
    let executor: ExecutorRef = std::sync::Arc::new(ConsumerExecutor);
    let options = EngineOptions::new_with_executor(
        reefsdb::StorageOptions {
            config: EngineConfig::default(),
            vlog_config: EngineVLogConfig::default(),
            vlog_enabled: false,
            vlog_compact_plan: "default".to_string(),
        },
        executor,
    );

    let scan = StorageScan {
        measurement: MeasurementRef::new("cpu"),
        projection: StorageProjection {
            primary_column: Some(ColumnRef::new("usage", ColumnKind::AnyField)),
            returned_columns: vec![ColumnRef::new("usage", ColumnKind::AnyField)],
            row_domain_columns: Vec::new(),
            series: SeriesProjection::default(),
        },
        time_range: TimeRange::new(0, 10),
        predicate: None,
        time_order: TimeOrder::Ascending,
        output_order: OutputOrder::TimeOrdered,
        execution: StorageExecutionOptions::default(),
        cancellation: None,
    };

    assert_eq!(options.index_version, "tsi1");
    assert!(scan.validate().is_ok());
    assert_eq!(SERIES_FILE_DIRECTORY, "_series");
    assert_eq!(std::any::type_name::<ReefsDb>(), "reefsdb::db::ReefsDb");
    assert_eq!(
        std::any::type_name::<SeriesFile>(),
        "reefsdb::series::series_file::SeriesFile"
    );
}

#[test]
fn public_facade_exposes_the_query_iterator_contract() {
    fn assert_public_type<T>() {}
    fn assert_query_iterator<T: reefsdb::query::QueryIterator + ?Sized>() {}
    fn assert_float_iterator<T: reefsdb::query::FloatIterator + ?Sized>() {}

    assert_public_type::<Point>();
    assert_public_type::<SeriesIDSet>();
    assert_public_type::<StoragePredicate>();
    assert_query_iterator::<dyn reefsdb::query::QueryIterator>();
    assert_float_iterator::<dyn reefsdb::query::FloatIterator>();
}

#[test]
fn public_facade_exposes_optional_aggregate_components() {
    use reefsdb::{AggregateKind, AggregateNumber, AggregateProjection, ProjectedAggregateState};
    let projection = AggregateProjection::from_aggregates(&[AggregateKind::Avg]);
    assert!(projection.count && projection.sum && !projection.min && !projection.max);
    let state = ProjectedAggregateState::empty(reefsdb::INTEGER, projection);
    assert_eq!(state.count, Some(0));
    assert_eq!(state.sum, None);
    let _ = AggregateNumber::Integer(1);
    let _ = ReefsDb::aggregate_projected_scan;
    let _ = reefsdb::ReefsDbOperationLease::aggregate_projected_scan;
    let _ = ReefsDb::aggregate_projected_scan_filtered;
    let _ = reefsdb::ReefsDbOperationLease::aggregate_projected_scan_filtered;
}

#[test]
fn public_aggregate_states_preserve_wide_average_contract() {
    use reefsdb::{
        AggregateKind, AggregateNumber, AggregateProjection, AggregateState, AggregateValues,
        AggregateWideSum, ProjectedAggregateState,
    };

    let mut complete = AggregateState::empty(reefsdb::INTEGER);
    complete.add_integer(i64::MAX);
    complete.add_integer(i64::MAX);
    assert_eq!(complete.average().unwrap(), Some(i64::MAX as f64));
    assert!(matches!(
        complete.values,
        Some(AggregateValues::Integer { sum: -2, .. })
    ));

    let mut projected = ProjectedAggregateState {
        field_type: reefsdb::INTEGER,
        projection: AggregateProjection::from_aggregates(&[AggregateKind::Avg]),
        count: Some(complete.count),
        sum: Some(AggregateNumber::Integer(-2)),
        wide_sum: complete.wide_sum,
        min: None,
        max: None,
        first: None,
        last: None,
    };
    assert_eq!(projected.average().unwrap(), complete.average().unwrap());

    projected.wide_sum = Some(AggregateWideSum::Integer(7));
    assert!(projected.average().is_err());
    projected.wide_sum = None;
    assert!(projected.average().is_err());
}
