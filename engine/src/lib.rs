//! Supported public API for the standalone ReefsDB time-series storage engine.
//!
//! External consumers should depend on and import from this crate. Internal
//! storage, index, model, and utility modules are not supported dependency
//! boundaries.

#[cfg(feature = "benchmarking")]
#[doc(hidden)]
pub mod common;
#[cfg(not(feature = "benchmarking"))]
mod common;
#[cfg(feature = "benchmarking")]
#[doc(hidden)]
pub mod models;
#[cfg(not(feature = "benchmarking"))]
mod models;
#[cfg(feature = "benchmarking")]
#[doc(hidden)]
pub mod runtime;
#[cfg(not(feature = "benchmarking"))]
mod runtime;

#[cfg(feature = "benchmarking")]
#[doc(hidden)]
pub mod config;
#[cfg(not(feature = "benchmarking"))]
mod config;
mod db;
#[cfg(feature = "benchmarking")]
#[doc(hidden)]
pub mod index;
#[cfg(not(feature = "benchmarking"))]
mod index;
pub mod query;
#[cfg(feature = "benchmarking")]
#[doc(hidden)]
pub mod series;
#[cfg(not(feature = "benchmarking"))]
mod series;
#[cfg(feature = "benchmarking")]
#[doc(hidden)]
pub mod storage;
#[cfg(not(feature = "benchmarking"))]
mod storage;

pub use crate::common::{
    hll::{Error as HllError, HLL, hash_key, hash_u64},
    time::Time,
};
pub use crate::models::{
    BOOL, DatabaseId, EMPTY, FLOAT, FieldType, Fields, INTEGER, InvalidDatabaseId,
    MAX_NANO_TIME_I64, MIN_NANO_TIME_I64, Point, RawVal, STRING, Tags, UNSIGNED, Val, ZERO_TIME,
    field_validator::validate_field_type,
    points::{
        Error as PointError, must_new_point, parse_points_string, parse_points_with_precision,
    },
};
pub use config::{Config as EngineConfig, VLogConfig as EngineVLogConfig};
pub use db::{
    AggregateShard, AggregateShardGroup, CrossShardAggregateMetrics, CrossShardAggregateResult,
    DrainWaitOutcome, Error as ReefsDbError, LeasedBoolIterator, LeasedFloatIterator,
    LeasedIntegerIterator, LeasedQueryIterator, LeasedStringIterator, LeasedUnsignedIterator,
    MeasurementDropLocalResult, ReefsDb, ReefsDbLifecycleState, ReefsDbOperationLease,
};
pub use query::metadata::{TIME_COLUMN_NAME, build_arrow_schema};
pub use query::record_batch::{
    RecordBatchError, RecordBatchMetrics, RecordBatchReader, RecordBatchRequest,
};
pub use query::storage_ir::{
    AggregateIndexReadMetrics, AggregateKind, AggregateNumber, AggregatePoint, AggregatePointValue,
    AggregateProjection, AggregateRequest, AggregateResult, ProjectedAggregateResult, ProjectedAggregateState,
    ProjectedAggregateWindowsResult,
    AggregateScanMetrics, AggregateState, AggregateValues, AggregateWideSum, ArithmeticOp, ColumnKind, ColumnRef,
    CompareOp, GroupedAggregateGroup, GroupedAggregateKey, GroupedAggregateMetrics,
    GroupedAggregateRequest, GroupedAggregateResult, GroupedAggregateTarget,
    GroupedAggregateWindow, GroupedTagFilter, LogicalOp, MeasurementRef, OutputOrder, RegexOp,
    ResidualColumnResolution, RowCountMetrics, RowCountRequest, RowCountResult, ScanCancellation,
    SeriesProjection, StorageExecutionOptions, StorageLimit, StoragePredicate, StorageProjection,
    StorageProjectionError, StorageScan, StorageValueExpr, StorageWindow, TimeOrder, TimeRange,
};
#[cfg(feature = "slow-trace")]
pub use query::storage_ir::{
    GroupedAggregateProfileMetrics, GroupedExecutionDiagnostics,
    GroupedExecutionDiagnosticsSnapshot, GroupedGeneralBlockingTaskTiming,
    GroupedPhysicalBlockIdentity, GroupedRawIntervalKind, GroupedRawIntervalSample,
    GroupedStorageJobTiming, StorageScanProfile, StorageScanProfileMetrics,
};
#[cfg(feature = "slow-trace")]
pub use runtime::execution::{
    GeneralBlockingClassSnapshot, GeneralBlockingExecutorSnapshot, GeneralBlockingTaskClass,
    GeneralBlockingTaskDiagnostics,
};
pub use runtime::{
    execution::{
        AcceptedTaskGuard, AcceptedTaskRegistration, BoxBlockingTask, BoxFutureTask,
        DefaultTokioExecutor, DefaultTokioExecutorOptions, ExecutorRef, RuntimeExecutor,
        RuntimeExecutorExt, SpawnError, StorageQueryCpuActivePermit, StorageQueryCpuBudget,
        StorageQueryCpuPermit, TaskController, TaskHandle, TaskJoinError, accepted_task_guard,
        legacy_global_executor,
    },
    global::{RuntimeOptions as GlobalRuntimeOptions, init_global_runtimes},
    runtime::Error as RuntimeBuildError,
};
pub use series::SERIES_FILE_DIRECTORY;
pub use series::series_file::{Error as SeriesFileError, SeriesFile};
pub use series::series_set::SeriesIDSet;
pub use series::{
    MEASUREMENT_SCHEMA_FILE_NAME, MeasurementSchema, MeasurementSchemaError,
    MeasurementSchemaRegistry,
};
pub use storage::{
    AggregateQueryOptions, CallbackSeriesIDSet, EngineError, EngineOptions, SeriesIDSets, StorageOptions,
    limiter::{
        Fixed as FixedLimiter,
        rate::{Limit as RateLimit, Limiter as RateLimiter},
    },
};
