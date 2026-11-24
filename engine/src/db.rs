//! ReefsDb represents a self-contained time series database.

// use std::{path::PathBuf, sync::Arc};

#[cfg(tests)]
use std::fmt::Result;
use std::{
    collections::{HashMap, HashSet},
    fs,
    io::Write,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};

#[cfg(feature = "slow-trace")]
use crate::ScanCancellation;
use crate::common::{hll::HLL, sync, time, to_str, to_string};
use crate::models::{BOOL, FLOAT, FieldType, INTEGER, STRING, UNSIGNED};
use crate::runtime::execution::{ExecutorRef, RuntimeExecutorExt};
use parking_lot::RwLock;
// use bytes::{Buf, BufMut};
// use integer_encoding::VarInt;
// use crate::models;
// // use parking_lot::{RwLock, RwLockWriteGuard, RwLockReadGuard};
use snafu::{ResultExt, Snafu, ensure};
use tokio::select;

use crate::{
    common::slow_trace,
    index,
    index::{ActiveIndex as Index, SeriesIterator},
    query::{
        metadata::TIME_COLUMN_NAME,
        query_iterator::{
            self, BoolIterator, FloatIterator, IntegerIterator, IteratorDataType, IteratorStats,
            QueryIterator, StringIterator, UnsignedIterator,
        },
        query_point,
        record_batch::{RecordBatchError, RecordBatchReader, RecordBatchRequest},
        storage_ir::{
            AggregateProjection, AggregateRequest, AggregateResult, GroupedAggregateRequest,
            GroupedAggregateResult, ProjectedAggregateResult, ProjectedAggregateWindowsResult,
            RowCountRequest, RowCountResult, StoragePredicate, StorageScan,
        },
    },
    series::{
        measurement_fields::MeasurementFields,
        measurement_schema::{
            Error as MeasurementSchemaError, MeasurementSchema, MeasurementSchemaRegistry,
            schemas_from_points,
        },
        series_file::{self, SeriesFile},
        series_set,
    },
    storage::{
        ActiveStorage as Engine, EngineOptions, VLogOptions, tsm1::engine::DirectTsmScanSet,
    },
};
// use tokio::{
//     fs, io,
//     sync::{RwLock, RwLockReadGuard, RwLockWriteGuard},
// };

// use crate::{series_file::*, series_partition::*};

const STAT_WRITE_REQ: &str = "write_req";
const STAT_WRITE_REQ_OK: &str = "write_req_ok";
const STAT_WRITE_REQ_ERR: &str = "write_req_err";
const STAT_SERIES_CREATE: &str = "series_create";
const STAT_FIELDS_CREATE: &str = "fields_create";
const STAT_WRITE_POINTS_ERR: &str = "write_points_err";
const STAT_WRITE_POINTS_DROPPED: &str = "write_points_dropped";
const STAT_WRITE_POINTS_OK: &str = "write_points_ok";
const STAT_WRITE_BYTES: &str = "write_bytes";
const STAT_DISK_BYTES: &str = "disk_bytes";

mod lifecycle;

#[cfg(test)]
mod measurement_drop_tests;
#[cfg(test)]
mod measurement_schema_tests;
#[cfg(test)]
mod phase5_tests;
#[doc(hidden)]
pub use lifecycle::DrainWaitOutcome;
use lifecycle::{
    BorrowedReefsDbOperationLease, ReefsDbLeaseError, ReefsDbLifecycle, ReefsDbOperationLeaseSet,
};
pub use lifecycle::{ReefsDbLifecycleState, ReefsDbOperationLease};
#[path = "db/aggregate_group.rs"]
mod aggregate_group;
pub use aggregate_group::{AggregateShard, AggregateShardGroup, CrossShardAggregateMetrics, CrossShardAggregateResult};

/// Error type for ReefsDb.
#[derive(Debug, Snafu)]
pub enum Error {
    /// IO error.
    #[snafu(display("integer encode/decode error: {}", source))]
    IOError { source: std::io::Error },

    #[snafu(display("Generic {}", source))]
    Generic {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// FieldTypeConflict is returned when a new field already exists with a different type.
    #[snafu(display("field type conflict"))]
    FieldTypeConflict,

    // ErrEngineClosed is returned when a caller attempts indirectly to
    // access the shard's underlying engine.
    #[snafu(display("engine is closed"))]
    EngineClosed,

    // ErrShardDisabled is returned when a the shard is not available for
    // queries or writes.
    #[snafu(display("shard is disabled"))]
    ShardDisabled,

    #[snafu(display("shard is draining"))]
    ShardDraining,

    #[snafu(display("shard has been deleted"))]
    ShardDeleted,

    #[snafu(display("shard operation lease was rejected: {reason}"))]
    ShardLeaseRejected { reason: String },

    #[snafu(display("iterator merge failed: {reason}"))]
    IteratorMergeFailed { reason: String },

    // ErrShardNotIdle is returned when an operation requiring the shard to be idle/cold is
    // attempted on a hot shard.
    #[snafu(display("shard not idle"))]
    ShardNotIdle,

    /// Index error.
    #[snafu(display("{}", source))]
    Index { source: crate::index::Error },

    /// Engine error.
    #[snafu(display("{}", source))]
    Engine {
        source: crate::storage::ActiveStorageError,
    },

    /// Measurement fields error.
    #[snafu(display("{}", source))]
    MeasurementFields {
        source: crate::series::MeasurementFieldsError,
    },

    /// Database-scoped measurement schema metadata error.
    #[snafu(display("{}", source))]
    MeasurementSchema { source: MeasurementSchemaError },

    /// Partial write error
    /// #[snafu(display("Partial write error"))]
    PartialWrite {
        reason: String,
        dropped: i32,

        // A sorted slice of series keys that were dropped.
        dropped_keys: Vec<Vec<u8>>,
    },

    /// String error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },
}

impl From<crate::index::Error> for Error {
    fn from(err: crate::index::Error) -> Error {
        Error::Index { source: err }
    }
}

impl From<crate::storage::ActiveStorageError> for Error {
    fn from(err: crate::storage::ActiveStorageError) -> Error {
        Error::Engine { source: err }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IOError { source: err }
    }
}

fn map_lease_error(error: ReefsDbLeaseError) -> Error {
    match error {
        ReefsDbLeaseError::Draining => Error::ShardDraining,
        ReefsDbLeaseError::Deleted => Error::ShardDeleted,
        ReefsDbLeaseError::Closed => Error::EngineClosed,
        ReefsDbLeaseError::CountOverflow => Error::ShardLeaseRejected {
            reason: "active operation count overflow".to_string(),
        },
    }
}

/// Result type for ReefsDb.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Shard-local output retained for later database-wide measurement cleanup.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MeasurementDropLocalResult {
    series_ids: Vec<u64>,
}

impl MeasurementDropLocalResult {
    pub fn series_ids(&self) -> &[u64] {
        &self.series_ids
    }

    pub fn into_series_ids(self) -> Vec<u64> {
        self.series_ids
    }
}

// #[derive(Clone, Debug, Default, PartialEq, Eq)]
// pub struct Measurement {
//     name: String,
//     system_iterator: Option<SystemIterator>,
// }

// impl Measurement {
//     pub fn new(name: impl Into<String>) -> Self {
//         Self {
//             name: name.into(),
//             system_iterator: None,
//         }
//     }

//     pub fn with_system_iterator(name: impl Into<String>, system_iterator: SystemIterator) -> Self {
//         Self {
//             name: name.into(),
//             system_iterator: Some(system_iterator),
//         }
//     }

//     pub fn name(&self) -> &str {
//         &self.name
//     }

//     pub fn system_iterator(&self) -> Option<SystemIterator> {
//         self.system_iterator.clone()
//     }
// }

// #[derive(Clone, Debug, PartialEq, Eq)]
// pub enum SystemIterator {
//     FieldKeys,
//     Series,
//     TagKeys,
// }

pub struct LeasedQueryIterator {
    inner: Option<Box<dyn QueryIterator + Send + Sync>>,
    leases: Option<ReefsDbOperationLeaseSet>,
}

impl LeasedQueryIterator {
    pub(crate) fn new(
        inner: Box<dyn QueryIterator + Send + Sync>,
        leases: ReefsDbOperationLeaseSet,
    ) -> Self {
        Self {
            inner: Some(inner),
            leases: Some(leases),
        }
    }

    /// Retains local operation leases for an iterator assembled by an upper
    /// multi-shard query owner.
    pub fn from_operation_leases(
        inner: Box<dyn QueryIterator + Send + Sync>,
        leases: Vec<ReefsDbOperationLease>,
    ) -> Self {
        Self::new(inner, ReefsDbOperationLeaseSet::new(leases))
    }

    pub fn data_type(&self) -> IteratorDataType {
        self.inner
            .as_ref()
            .map_or(IteratorDataType::Unknown, |inner| inner.data_type())
    }

    pub fn stats(&self) -> IteratorStats {
        self.inner
            .as_ref()
            .map_or_else(IteratorStats::default, |inner| inner.stats())
    }

    #[cfg(any(test, feature = "benchmarking"))]
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self.inner
            .as_mut()
            .expect("leased iterator must own an inner iterator")
            .as_any_mut()
    }

    pub fn close(&mut self) {
        if let Some(mut inner) = self.inner.take() {
            inner.close();
            drop(inner);
        }
        self.leases.take();
    }

    fn is<T: QueryIterator + 'static>(&self) -> bool {
        self.inner
            .as_ref()
            .is_some_and(|inner| inner.as_any().is::<T>())
    }

    fn take_downcast<T: QueryIterator + 'static>(&mut self) -> Box<T> {
        self.inner
            .take()
            .expect("leased iterator must own an inner iterator")
            .into_any()
            .downcast::<T>()
            .expect("iterator type was checked before downcast")
    }

    pub fn try_downcast_integer<T>(mut self) -> std::result::Result<LeasedIntegerIterator, Self>
    where
        T: IntegerIterator + Send + Sync + 'static,
    {
        if !self.is::<T>() {
            return Err(self);
        }
        let inner = self.take_downcast::<T>();
        let leases = self.leases.take().expect("leased iterator must own leases");
        Ok(LeasedIntegerIterator::new(inner, leases))
    }

    pub fn try_downcast_unsigned<T>(mut self) -> std::result::Result<LeasedUnsignedIterator, Self>
    where
        T: UnsignedIterator + Send + Sync + 'static,
    {
        if !self.is::<T>() {
            return Err(self);
        }
        let inner = self.take_downcast::<T>();
        let leases = self.leases.take().expect("leased iterator must own leases");
        Ok(LeasedUnsignedIterator::new(inner, leases))
    }

    pub fn try_downcast_float<T>(mut self) -> std::result::Result<LeasedFloatIterator, Self>
    where
        T: FloatIterator + Send + Sync + 'static,
    {
        if !self.is::<T>() {
            return Err(self);
        }
        let inner = self.take_downcast::<T>();
        let leases = self.leases.take().expect("leased iterator must own leases");
        Ok(LeasedFloatIterator::new(inner, leases))
    }

    pub fn try_downcast_bool<T>(mut self) -> std::result::Result<LeasedBoolIterator, Self>
    where
        T: BoolIterator + Send + Sync + 'static,
    {
        if !self.is::<T>() {
            return Err(self);
        }
        let inner = self.take_downcast::<T>();
        let leases = self.leases.take().expect("leased iterator must own leases");
        Ok(LeasedBoolIterator::new(inner, leases))
    }

    pub fn try_downcast_string<T>(mut self) -> std::result::Result<LeasedStringIterator, Self>
    where
        T: StringIterator + Send + Sync + 'static,
    {
        if !self.is::<T>() {
            return Err(self);
        }
        let inner = self.take_downcast::<T>();
        let leases = self.leases.take().expect("leased iterator must own leases");
        Ok(LeasedStringIterator::new(inner, leases))
    }
}

impl ReefsDbOperationLease {
    /// Creates one local storage child while this existing lease keeps the shard active.
    pub fn create_storage_query_iterator(
        &self,
        scan: StorageScan,
    ) -> Result<Option<Box<dyn QueryIterator + Sync + Send>>> {
        self.reefsdb().create_storage_iterator_unleased(scan)
    }

    /// Creates a local storage iterator while retaining this operation lease.
    pub fn create_storage_iterator(
        &self,
        scan: StorageScan,
    ) -> Result<Option<LeasedQueryIterator>> {
        Ok(self
            .reefsdb()
            .create_storage_iterator_unleased(scan)?
            .map(|inner| {
                LeasedQueryIterator::new(inner, ReefsDbOperationLeaseSet::new(vec![self.clone()]))
            }))
    }

    /// Creates a streaming Arrow reader while retaining this operation lease.
    pub fn create_record_batch_reader(
        &self,
        request: RecordBatchRequest,
    ) -> std::result::Result<RecordBatchReader, RecordBatchError> {
        if let Some(mut scan_set) = self
            .reefsdb()
            .create_direct_tsm_scans_unleased(request.scan())
            .map_err(RecordBatchError::storage)?
        {
            if scan_set.tag_sets.is_empty() {
                return RecordBatchReader::try_new(request, None);
            }
            if scan_set.tag_sets.len() > 1 || scan_set.tag_sets[0].series.len() > 1 {
                // The direct multi-series merger orders intermediate Arrow rows
                // by their projected timestamp. Aggregate-only projections may
                // omit `time`, so use the logical iterator merger in that case.
                if request.schema().index_of(TIME_COLUMN_NAME).is_ok() {
                    return RecordBatchReader::try_new_multi_series_direct(
                        request,
                        scan_set,
                        self.clone(),
                    );
                }
            } else {
                let mut scans = scan_set
                    .tag_sets
                    .pop()
                    .expect("single-series direct scan has one tag set")
                    .series
                    .pop()
                    .expect("single-series direct scan has one physical series")
                    .scans;
                if request.scan().projection.returned_columns.len() == 1
                    && request.scan().projection.row_domain_columns.is_empty()
                {
                    let scan = scans
                        .pop()
                        .expect("single-field direct scan has one field stream");
                    return RecordBatchReader::try_new_direct(request, scan, self.clone());
                }
                return RecordBatchReader::try_new_multi_direct(request, scans, self.clone());
            }
        }
        let iterator = self
            .create_storage_iterator(request.scan().clone())
            .map_err(RecordBatchError::storage)?;
        RecordBatchReader::try_new(request, iterator)
    }

    #[cfg(test)]
    fn create_record_batch_reader_with_full_row_domain_values(
        &self,
        request: RecordBatchRequest,
    ) -> std::result::Result<RecordBatchReader, RecordBatchError> {
        if let Some(mut scan_set) = self
            .reefsdb()
            .create_direct_tsm_scans_with_row_domain_values_unleased(request.scan())
            .map_err(RecordBatchError::storage)?
        {
            if scan_set.tag_sets.is_empty() {
                return RecordBatchReader::try_new(request, None);
            }
            if scan_set.tag_sets.len() > 1 || scan_set.tag_sets[0].series.len() > 1 {
                return RecordBatchReader::try_new_multi_series_direct(
                    request,
                    scan_set,
                    self.clone(),
                );
            }
            let scans = scan_set
                .tag_sets
                .pop()
                .expect("single-series direct scan has one tag set")
                .series
                .pop()
                .expect("single-series direct scan has one physical series")
                .scans;
            return RecordBatchReader::try_new_multi_direct(request, scans, self.clone());
        }
        let iterator = self
            .create_storage_iterator(request.scan().clone())
            .map_err(RecordBatchError::storage)?;
        RecordBatchReader::try_new(request, iterator)
    }

    /// Executes one local storage aggregate while this lease keeps the shard active.
    pub fn aggregate_scan(&self, request: AggregateRequest) -> Result<Option<AggregateResult>> {
        self.reefsdb().aggregate_scan_unleased(request)
    }

    /// Executes one local grouped storage aggregate while this lease keeps the shard active.
    pub fn grouped_aggregate_scan(
        &self,
        request: GroupedAggregateRequest,
    ) -> Result<Option<GroupedAggregateResult>> {
        self.reefsdb().grouped_aggregate_scan_unleased(request)
    }

    /// Aggregates exactly one field, returning only groups containing that field.
    ///
    /// Other fields do not establish empty groups and their values are not read.
    /// Use `grouped_aggregate_scan` when groups with no target values are required.
    pub fn grouped_aggregate_scan_target_only(
        &self,
        request: GroupedAggregateRequest,
    ) -> Result<Option<GroupedAggregateResult>> {
        let engine = {
            let inner = self.reefsdb().inner.read();
            inner.engine_arc()?
        };
        engine
            .grouped_aggregate_scan_target_only(request)
            .context(EngineSnafu)
    }

    /// Executes one local grouped storage aggregate through the injected general executor.
    pub async fn grouped_aggregate_scan_async(
        &self,
        request: GroupedAggregateRequest,
    ) -> Result<Option<GroupedAggregateResult>> {
        #[cfg(feature = "slow-trace")]
        let diagnostics = request
            .cancellation
            .as_ref()
            .and_then(ScanCancellation::grouped_diagnostics)
            .cloned();
        let (engine, executor) = {
            let inner = self.reefsdb().inner.read();
            (
                inner.engine_arc()?,
                Arc::clone(&self.reefsdb().options.executor),
            )
        };
        let lease = self.clone();
        #[cfg(feature = "slow-trace")]
        if let Some(diagnostics) = &diagnostics {
            diagnostics.record_general_async_submitted(std::time::Instant::now());
        }
        let task = executor
            .spawn(async move {
                #[cfg(feature = "slow-trace")]
                if let Some(diagnostics) = &diagnostics {
                    diagnostics.record_general_async_started(std::time::Instant::now());
                }
                let _lease = lease;
                engine.grouped_aggregate_scan_async(request).await
            })
            .map_err(|source| Error::Generic {
                source: Box::new(source),
            })?;
        task.await
            .map_err(|source| Error::Generic {
                source: Box::new(source),
            })?
            .context(EngineSnafu)
    }

    /// Counts exact local logical rows while this lease keeps the shard active.
    pub fn row_count(&self, request: RowCountRequest) -> Result<Option<RowCountResult>> {
        self.reefsdb().row_count_unleased(request)
    }

    /// Executes only the requested aggregate components, including AVG dependencies.
    pub fn aggregate_projected_scan(
        &self,
        request: AggregateRequest,
    ) -> Result<Option<ProjectedAggregateResult>> {
        self.reefsdb().aggregate_projected_scan_unleased(request)
    }

    /// Executes a projected aggregate after exact local series selection.
    /// Returns `None` when aggregate indexes or the requested field are absent.
    /// Predicates requiring row evaluation return an error; use a storage scan
    /// for those predicates. Time bounds remain inclusive in the request.
    pub fn aggregate_projected_scan_filtered(
        &self,
        request: AggregateRequest,
        predicate: StoragePredicate,
    ) -> Result<Option<ProjectedAggregateResult>> {
        self.reefsdb()
            .aggregate_projected_scan_filtered_unleased(request, predicate)
    }

    /// Reduces ordered disjoint windows for multiple physical fields, sharing
    /// exact series selection while this operation lease keeps the shard open.
    pub fn aggregate_projected_scan_windows_filtered(
        &self,
        requests: Vec<AggregateRequest>,
        windows: Vec<crate::TimeRange>,
        predicate: StoragePredicate,
    ) -> Result<Vec<Option<ProjectedAggregateWindowsResult>>> {
        self.reefsdb()
            .aggregate_projected_scan_windows_filtered_unleased(requests, windows, predicate)
    }

    /// Reads local measurement names without acquiring a second operation lease.
    pub fn measurement_names(&self) -> Result<Vec<Vec<u8>>> {
        self.reefsdb().measurement_names_unleased()
    }

    pub fn field_dimensions(
        &self,
        measurements: &[&[u8]],
    ) -> Result<(HashMap<String, FieldType>, HashSet<String>)> {
        self.reefsdb().field_dimensions_unleased(measurements)
    }

    pub fn field_keys_by_measurement(&self, name: &[u8]) -> Vec<String> {
        self.reefsdb()
            .measurement_schema_by_bytes(name)
            .map_or_else(
                |_| Vec::new(),
                |schema| {
                    schema.map_or_else(Vec::new, |schema| schema.fields().keys().cloned().collect())
                },
            )
    }

    pub fn tag_keys(&self, name: &[u8]) -> Result<Vec<String>> {
        let mut keys = self
            .reefsdb()
            .measurement_tag_keys_unleased(name)?
            .into_iter()
            .collect::<Vec<_>>();
        keys.sort();
        Ok(keys)
    }

    pub fn tag_values(&self, name: &[u8], keys: &[&str]) -> Result<Vec<(String, Vec<String>)>> {
        self.reefsdb().tag_values_unleased(name, keys)
    }

    /// Reads local series keys without acquiring a second operation lease.
    pub fn series_keys_by_storage_predicate(
        &self,
        name: &[u8],
        predicate: Option<&StoragePredicate>,
    ) -> Result<Vec<Vec<u8>>> {
        self.reefsdb()
            .series_keys_by_storage_predicate_unleased(name, predicate)
    }

    pub fn measurement_series_keys(&self, name: &[u8]) -> Result<Vec<Vec<u8>>> {
        self.reefsdb().measurement_series_keys_unleased(name)
    }

    pub fn measurement_names_by_storage_predicate(
        &self,
        predicate: Option<&StoragePredicate>,
    ) -> Result<Vec<Vec<u8>>> {
        self.reefsdb()
            .measurement_names_by_storage_predicate_unleased(predicate)
    }

    pub fn series_sketches(&self) -> Result<(HLL, HLL)> {
        self.reefsdb().series_sketches_unleased()
    }

    pub fn measurements_sketches(&self) -> Result<(HLL, HLL)> {
        self.reefsdb().measurements_sketches_unleased()
    }

    pub fn local_field_type(&self, measurement: &[u8], field: &[u8]) -> Result<Option<FieldType>> {
        self.reefsdb().local_field_type_unleased(measurement, field)
    }

    pub fn has_tag_key(&self, measurement: &[u8], key: &[u8]) -> Result<bool> {
        self.reefsdb().has_tag_key_unleased(measurement, key)
    }

    /// Deletes local points while this existing operation lease keeps the shard active.
    pub fn delete_series_range_by_measurement<F>(
        &self,
        name: &[u8],
        predicate: Option<F>,
    ) -> Result<()>
    where
        F: Fn(&[u8], &crate::models::Tags) -> (i64, i64, bool),
    {
        self.reefsdb()
            .delete_series_range_by_measurement_unleased(name, predicate)
    }
}

impl Drop for LeasedQueryIterator {
    fn drop(&mut self) {
        self.close();
    }
}

macro_rules! leased_typed_iterator {
    ($name:ident, $iterator_trait:ident, $point:ty, $data_type:expr) => {
        pub struct $name {
            inner: Option<Box<dyn $iterator_trait + Send + Sync>>,
            leases: Option<ReefsDbOperationLeaseSet>,
            stats: IteratorStats,
        }

        impl $name {
            fn new<T>(inner: Box<T>, leases: ReefsDbOperationLeaseSet) -> Self
            where
                T: $iterator_trait + Send + Sync + 'static,
            {
                let stats = inner.stats();
                Self {
                    inner: Some(inner),
                    leases: Some(leases),
                    stats,
                }
            }

            pub fn close(&mut self) {
                if let Some(mut inner) = self.inner.take() {
                    inner.close();
                    drop(inner);
                }
                self.leases.take();
            }
        }

        impl QueryIterator for $name {
            fn data_type(&self) -> IteratorDataType {
                $data_type
            }

            fn stats(&self) -> IteratorStats {
                self.stats.clone()
            }

            fn close(&mut self) {
                Self::close(self);
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
                self
            }

            fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
                self
            }
        }

        impl $iterator_trait for $name {
            fn next(&mut self) -> Option<$point> {
                let next = self.inner.as_mut().and_then(|inner| inner.next());
                if next.is_none() {
                    self.close();
                }
                next
            }
        }

        impl Drop for $name {
            fn drop(&mut self) {
                self.close();
            }
        }
    };
}

leased_typed_iterator!(
    LeasedIntegerIterator,
    IntegerIterator,
    query_point::IntegerPoint,
    IteratorDataType::Integer
);
leased_typed_iterator!(
    LeasedUnsignedIterator,
    UnsignedIterator,
    query_point::UnsignedPoint,
    IteratorDataType::Unsigned
);
leased_typed_iterator!(
    LeasedFloatIterator,
    FloatIterator,
    query_point::FloatPoint,
    IteratorDataType::Float
);
leased_typed_iterator!(
    LeasedBoolIterator,
    BoolIterator,
    query_point::BoolPoint,
    IteratorDataType::Boolean
);
leased_typed_iterator!(
    LeasedStringIterator,
    StringIterator,
    query_point::StringPoint,
    IteratorDataType::String
);

fn storage_field_type(field_type: crate::models::FieldType) -> Option<FieldType> {
    match field_type {
        FLOAT | INTEGER | UNSIGNED | STRING | BOOL => Some(field_type),
        _ => None,
    }
}

// struct StringPointsIterator {
//     points: Vec<query_point::StringPoint>,
//     idx: usize,
//     stats: query_iterator::IteratorStats,
// }

// impl QueryIterator for StringPointsIterator {
//     fn data_type(&self) -> VarRefDataType {
//         VarRefDataType::String
//     }

//     fn stats(&self) -> query_iterator::IteratorStats {
//         self.stats.clone()
//     }

//     fn close(&mut self) {
//         self.idx = self.points.len();
//     }

//     fn as_any(&self) -> &dyn std::any::Any {
//         self
//     }

//     fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
//         self
//     }

//     fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
//         self
//     }
// }

// impl query_iterator::StringIterator for StringPointsIterator {
//     fn next(&mut self) -> Option<query_point::StringPoint> {
//         if self.idx >= self.points.len() {
//             return None;
//         }

//         let point = self.points[self.idx].clone();
//         self.idx += 1;
//         Some(point)
//     }
// }

// fn new_string_points_iterator(values: Vec<String>, opt: Arc<IteratorOptions>) -> StringPointsIterator {
//     let mut values = values;
//     if !opt.ascending {
//         values.reverse();
//     }

//     let points = values
//         .into_iter()
//         .map(|value| query_point::StringPoint {
//             name: String::new(),
//             tags: query_point::Tags::default(),
//             time: 0,
//             value,
//             aux: vec![],
//             aggregated: 0,
//             none: false,
//         })
//         .collect::<Vec<_>>();

//     StringPointsIterator {
//         stats: query_iterator::IteratorStats {
//             series_n: points.len() as i32,
//             point_n: points.len() as i32,
//         },
//         points,
//         idx: 0,
//     }
// }
/// ReefsDb represents one self-contained local time-series storage instance.
/// It owns one engine and uses the database-scoped series file supplied by the
/// upper runtime. Cross-instance routing and result merging remain upper-layer
/// responsibilities.
pub struct ReefsDb {
    path: Arc<PathBuf>,
    wal_path: Arc<PathBuf>,
    index_wal_path: Arc<PathBuf>,
    vlog_path: Arc<PathBuf>,
    sfile: Arc<SeriesFile>,
    measurement_schemas: Arc<MeasurementSchemaRegistry>,
    options: EngineOptions,

    // inner: RwLock<Inner>,
    inner: Arc<RwLock<Inner>>,
    lifecycle: Arc<ReefsDbLifecycle>,
    #[cfg(feature = "test-support")]
    fail_next_index_close: std::sync::atomic::AtomicBool,

    enable_on_open: bool,
    // compaction_disabled specifies the shard should not schedule compactions.
    // This option is intended for offline tooling.
    compaction_disabled: bool,
    // TODO: implement me
    // expvar-based stats.
    stats: ReefsDbStatistics,
    default_tags: crate::models::StatisticTags,
}
impl std::fmt::Debug for ReefsDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReefsDb").finish()
    }
}
struct Inner {
    enabled: bool,
    engine: Option<Arc<Engine>>,
    index: Option<Arc<Index>>,
}

impl ReefsDb {
    pub fn new(
        path: impl Into<PathBuf>,
        wal_path: impl Into<PathBuf>,
        vlog_path: impl Into<PathBuf>,
        index_wal_path: impl Into<PathBuf>,
        sfile: Arc<SeriesFile>,
        measurement_schemas: Arc<MeasurementSchemaRegistry>,
        opt: EngineOptions,
    ) -> Result<Self> {
        let path: Arc<PathBuf> = Arc::new(path.into());
        let wal_path: Arc<PathBuf> = Arc::new(wal_path.into());
        let index_wal_path: Arc<PathBuf> = Arc::new(index_wal_path.into());
        let vlog_path: Arc<PathBuf> = Arc::new(vlog_path.into());
        Ok(Self {
            path,
            wal_path,
            index_wal_path,
            vlog_path,
            sfile,
            measurement_schemas,
            options: opt,

            inner: Arc::new(RwLock::new(Inner {
                enabled: false,
                engine: None,
                index: None,
            })),
            lifecycle: Arc::new(ReefsDbLifecycle::default()),
            #[cfg(feature = "test-support")]
            fail_next_index_close: std::sync::atomic::AtomicBool::new(false),

            enable_on_open: true,
            compaction_disabled: false,
            stats: ReefsDbStatistics::default(),
            default_tags: crate::models::StatisticTags::default(),
        })
    }

    /// Constructs a local storage instance with a host-supplied executor.
    pub fn new_with_executor(
        path: impl Into<PathBuf>,
        wal_path: impl Into<PathBuf>,
        vlog_path: impl Into<PathBuf>,
        index_wal_path: impl Into<PathBuf>,
        sfile: Arc<SeriesFile>,
        measurement_schemas: Arc<MeasurementSchemaRegistry>,
        mut opt: EngineOptions,
        executor: ExecutorRef,
    ) -> Result<Self> {
        opt.executor = executor;
        Self::new(
            path,
            wal_path,
            vlog_path,
            index_wal_path,
            sfile,
            measurement_schemas,
            opt,
        )
    }

    /// Enables the shard for queries and write.  When disabled, all
    /// writes and queries return an error and compactions are stopped for the shard.
    pub fn set_enabled(&self, enabled: bool) {
        let mut inner = self.inner.write();
        // Prevent writes and queries
        inner.enabled = enabled;
        if inner.engine.is_some() && !self.compaction_disabled {
            // Disable background compactions and snapshotting
            inner
                .engine
                .as_ref()
                .unwrap()
                .set_compactions_enabled(enabled);
        }
    }

    /// Forces a full compaction to be schedule on the shard.
    pub fn schedule_full_compaction(&self) -> Result<()> {
        let inner = self.inner.read();
        let engine = inner.engine()?;
        Ok(engine.schedule_full_compaction()?)
    }

    /// Returns the path set on the local ReefsDb when it was created.
    pub fn path(&self) -> &Path {
        &*self.path
    }

    pub fn vlog_enabled(&self) -> bool {
        self.options.vlog_enabled
    }

    pub fn open(&self) -> Result<()> {
        let started = std::time::Instant::now();
        tracing::info!(
            target: "reefsdb::tsdb::reefsdb",
            engine_id = self.options.shard_id,
            path = %self.path.display(),
            wal_path = %self.wal_path.display(),
            "opening ReefsDb"
        );
        let result = self.open_inner();
        match &result {
            Ok(()) => tracing::info!(
                target: "reefsdb::tsdb::reefsdb",
                engine_id = self.options.shard_id,
                duration_ms = started.elapsed().as_millis() as u64,
                "opened ReefsDb"
            ),
            Err(error) => tracing::error!(
                target: "reefsdb::tsdb::reefsdb",
                engine_id = self.options.shard_id,
                path = %self.path.display(),
                error = %error,
                duration_ms = started.elapsed().as_millis() as u64,
                "ReefsDb open failed"
            ),
        }
        result
    }

    fn open_inner(&self) -> Result<()> {
        self.lifecycle
            .check_open_allowed()
            .map_err(map_lease_error)?;
        {
            let mut inner = self.inner.write();

            // Return if the shard is already open
            if inner.engine.is_some() {
                return Ok(());
            }

            // Initialize underlying index.
            let ipath = self.path.join("index");
            let index = Arc::new(Index::new_with_executor(
                Arc::clone(&self.sfile),
                self.sfile.path().display().to_string(),
                &ipath,
                self.options.config.max_index_log_file_size,
                self.options.config.series_id_set_cache_size,
                Arc::clone(&self.options.executor),
            ));

            // Check if the index needs to be rebuilt before Open() initializes its file system layout.
            let mut should_reindex = false;
            if let Err(e) = fs::metadata(ipath) {
                if e.kind() == std::io::ErrorKind::NotFound {
                    should_reindex = true;
                }
            }

            // Open index.
            if let Err(source) = index.open() {
                let _ = index.close();
                return Err(source.into());
            }

            // Initialize underlying engine.
            let mut engine = match Engine::new(
                self.options.shard_id,
                Arc::clone(&index),
                Arc::clone(&self.path),
                Arc::clone(&self.wal_path),
                Arc::clone(&self.index_wal_path),
                Arc::clone(&self.vlog_path),
                Arc::clone(&self.sfile),
                self.options.clone(),
            ) {
                Ok(engine) => engine,
                Err(source) => {
                    let _ = index.close();
                    return Err(source.into());
                }
            };

            let open_result = (|| {
                engine.set_enabled(false);
                engine.open()?;

                if should_reindex {
                    engine.reindex()?;
                }

                engine.load_metadata_index(self.options.shard_id, Arc::clone(&index))?;
                Ok::<(), Error>(())
            })();
            if let Err(error) = open_result {
                let _ = engine.close();
                let _ = index.close();
                return Err(error);
            }

            inner.index = Some(index);
            inner.engine = Some(Arc::new(engine));
        }

        if let Err(error) = self.lifecycle.mark_open() {
            let _ = self.close_after_drain();
            return Err(map_lease_error(error));
        }

        // Enable engine.
        if self.enable_on_open {
            self.set_enabled(true);
        }

        Ok(())
    }

    /// Close shuts down this local storage instance.
    pub fn close(&self) -> Result<()> {
        self.lifecycle.begin_drain(false);
        let outcome = self.lifecycle.wait_for_drain(None, || false);
        debug_assert_eq!(outcome, DrainWaitOutcome::Drained);
        self.close_after_drain()
    }

    #[doc(hidden)]
    pub fn close_after_drain(&self) -> Result<()> {
        let started = std::time::Instant::now();
        tracing::info!(
            target: "reefsdb::tsdb::reefsdb",
            engine_id = self.options.shard_id,
            "closing ReefsDb"
        );
        let result = self.close_after_drain_inner();
        match &result {
            Ok(()) => tracing::info!(
                target: "reefsdb::tsdb::reefsdb",
                engine_id = self.options.shard_id,
                duration_ms = started.elapsed().as_millis() as u64,
                "closed ReefsDb"
            ),
            Err(error) => tracing::error!(
                target: "reefsdb::tsdb::reefsdb",
                engine_id = self.options.shard_id,
                error = %error,
                duration_ms = started.elapsed().as_millis() as u64,
                "ReefsDb close failed"
            ),
        }
        result
    }

    fn close_after_drain_inner(&self) -> Result<()> {
        self.set_enabled(false);
        let mut inner = self.inner.write();
        if let Some(engine) = inner.engine.as_ref() {
            engine.close()?;
            inner.engine = None;
        }
        #[cfg(feature = "test-support")]
        if self
            .fail_next_index_close
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            return Err(Error::StringErr {
                reason: "injected index close failure".to_string(),
            });
        }
        if let Some(index) = inner.index.as_ref() {
            index.close()?;
            inner.index = None;
        }
        drop(inner);
        self.lifecycle.mark_closed().map_err(map_lease_error)?;

        Ok(())
    }

    pub(crate) fn lifecycle(&self) -> &Arc<ReefsDbLifecycle> {
        &self.lifecycle
    }

    pub fn acquire_operation(self: &Arc<Self>) -> Result<ReefsDbOperationLease> {
        ReefsDbOperationLease::new(Arc::clone(self)).map_err(map_lease_error)
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn fail_next_index_close_for_test(&self) {
        self.fail_next_index_close
            .store(true, std::sync::atomic::Ordering::Release);
    }

    fn acquire_borrowed_operation(&self) -> Result<BorrowedReefsDbOperationLease<'_>> {
        BorrowedReefsDbOperationLease::new(self).map_err(map_lease_error)
    }

    #[doc(hidden)]
    pub fn begin_retention_drain(&self) {
        self.lifecycle.begin_drain(true);
    }

    #[doc(hidden)]
    pub fn wait_for_drain(
        &self,
        timeout: Option<std::time::Duration>,
        interrupted: impl Fn() -> bool,
    ) -> DrainWaitOutcome {
        self.lifecycle.wait_for_drain(timeout, interrupted)
    }

    #[doc(hidden)]
    pub fn notify_drain_waiters(&self) {
        self.lifecycle.notify_waiters();
    }

    pub fn lifecycle_state(&self) -> ReefsDbLifecycleState {
        self.lifecycle.state()
    }

    pub fn active_operation_count(&self) -> usize {
        self.lifecycle.active_operations()
    }

    pub fn is_retired(&self) -> bool {
        self.lifecycle.is_retired()
    }

    /// Returns the index version used by this local storage instance.
    ///
    /// index_type returns the empty string if it is called before the shard is opened,
    /// since it is only that point that the underlying index type is known.
    pub fn index_type(&self) -> String {
        let inner = self.inner.read();
        if inner.engine.is_none() || inner.index.is_none() {
            // ReefsDb not open yet.
            return String::new();
        }
        inner.index.as_ref().unwrap().index_type().to_string()
    }

    /// Ready determines if the ReefsDb is ready for queries or writes.
    /// It returns nil if ready, otherwise ErrShardClosed or ErrShardDisabled
    pub fn ready(&self) -> Result<()> {
        let inner = self.inner.read();
        let mut err = Ok(());
        if inner.engine.is_none() {
            err = Err(Error::EngineClosed);
        } else if !inner.enabled {
            err = Err(Error::ShardDisabled);
        }

        err
    }

    /// Returns the time when this shard was last modified.
    pub fn last_modified(&self) -> time::Time {
        let inner = self.inner.read();
        match &inner.engine {
            Some(e) => e.last_modified(),
            None => time::Time::from_timestamp(0, 0),
        }
    }

    /// Returns a reference the underlying series file.
    /// If return an error if the series file is None.
    pub fn series_file(&self) -> Result<Option<Arc<SeriesFile>>> {
        let inner = self.inner.read();
        if let Some(_e) = &inner.engine {
            self.ready()?;
            return Ok(Some(Arc::clone(&self.sfile)));
        }

        Ok(None)
    }

    /// Returns the database-scoped measurement schema registry shared by peer shards.
    pub fn measurement_schema_registry(&self) -> Arc<MeasurementSchemaRegistry> {
        Arc::clone(&self.measurement_schemas)
    }

    /// Reads shard-local legacy metadata for database-level bootstrap orchestration.
    #[doc(hidden)]
    pub fn legacy_measurement_schemas_for_bootstrap(&self) -> Result<Vec<MeasurementSchema>> {
        let _lease = self.acquire_borrowed_operation()?;
        let inner = self.inner.read();
        legacy_measurement_schemas(inner.engine()?)
    }

    /// Creates an explicit measurement schema without creating a concrete series.
    pub fn create_measurement_schema(&self, schema: MeasurementSchema) -> Result<bool> {
        let _lease = self.acquire_borrowed_operation()?;
        {
            let inner = self.inner.read();
            let _ = inner.engine()?;
        }
        self.measurement_schemas
            .create_measurement_schema(schema)
            .context(MeasurementSchemaSnafu)
    }

    /// Returns the canonical database-wide schema for one measurement.
    pub fn measurement_schema(&self, name: &str) -> Result<Option<MeasurementSchema>> {
        let _lease = self.acquire_borrowed_operation()?;
        {
            let inner = self.inner.read();
            let _ = inner.engine()?;
        }
        Ok(self.measurement_schemas.measurement_schema(name))
    }

    fn measurement_schema_by_bytes(&self, name: &[u8]) -> Result<Option<MeasurementSchema>> {
        Ok(self
            .measurement_schemas
            .measurement_schema(schema_name(name)?))
    }

    /// Return true if the shard is not receiving writes and is fully compacted.
    pub fn is_idle(&self) -> bool {
        let inner = self.inner.read();
        if let Some(_e) = &inner.engine {
            let _ = self.ready();
            return true;
        }

        false
    }

    pub fn free(&self) -> Result<()> {
        let inner = self.inner.read();
        if let Some(e) = &inner.engine {
            self.ready()?;
            // Disable compactions to stop background task.
            self.set_compactions_enabled(false);
            return e.free().context(EngineSnafu);
        }

        Ok(())
    }

    /// Enables or disable shard background compactions.
    pub fn set_compactions_enabled(&self, enabled: bool) {
        let inner = self.inner.read();
        if let Some(e) = &inner.engine {
            e.set_compactions_enabled(enabled);
        }
    }

    pub fn disk_size(&self) -> Result<i64> {
        let inner = self.inner.read();
        // We don't use engine() because we still want to report the shard's disk
        // size even if the shard has been disabled.
        ensure!(inner.engine.is_some(), EngineClosedSnafu);

        let size = inner.engine.as_ref().unwrap().disk_size();
        self.stats.disk_bytes.fetch_add(size, Ordering::SeqCst);

        Ok(size)
    }

    #[cfg(any(test, feature = "benchmarking"))]
    fn write_points_string(&self, ptstr: &[&str]) -> Result<()> {
        let (mut points, _) = crate::models::points::parse_points_string(&ptstr.join(
            "
",
        ))
        .unwrap();
        self.write_points(&mut points)
    }

    /// write_points will write the raw data points and any new metadata to the index in the shard.
    pub fn write_points(&self, points: &mut [crate::models::Point]) -> Result<()> {
        let _lease = self.acquire_borrowed_operation()?;
        self.write_points_unleased(points)
    }

    #[doc(hidden)]
    pub fn write_points_with_lease(
        &self,
        lease: &ReefsDbOperationLease,
        points: &mut [crate::models::Point],
    ) -> Result<()> {
        if !lease.belongs_to(self) {
            return Err(Error::ShardLeaseRejected {
                reason: "operation lease belongs to another shard".to_string(),
            });
        }
        self.write_points_unleased(points)
    }

    fn write_points_unleased(&self, points: &mut [crate::models::Point]) -> Result<()> {
        let write_total_start = slow_trace::now();
        let shard_lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait("shard.read", shard_lock_start, "stage=write_total");
        let engine = inner.engine()?;

        let mut write_err = Ok(());
        self.stats.write_req.fetch_add(1, Ordering::SeqCst);
        let validate_start = slow_trace::now();
        let (points, fields_to_create, err) = Self::validate_series_and_fields(
            self.options.config.validate_keys,
            points,
            engine,
            &self.measurement_schemas,
            &self.stats,
        );
        slow_trace::log_background_stage("write_validate_series_and_fields", validate_start, "");

        if let Err(e) = err {
            match &e {
                Error::PartialWrite {
                    reason,
                    dropped,
                    dropped_keys,
                } => {
                    // There was a partial write (points dropped), hold onto the error to return
                    // to the caller, but continue on writing the remaining points.
                    write_err = Err(e);
                }
                _ => {
                    return Err(e);
                }
            }
        }

        self.stats
            .fields_created
            .fetch_add(fields_to_create.len() as i64, Ordering::SeqCst);

        // add any new fields and keep track of what needs to be saved
        self.create_fields_and_measurements(&fields_to_create, engine)?;
        // Write to the engine.
        if let Err(e) = engine.write_points(points) {
            self.stats
                .write_points_err
                .fetch_add(points.len() as i64, Ordering::SeqCst);
            self.stats.write_req_err.fetch_add(1, Ordering::SeqCst);
            return Err(Error::StringErr {
                reason: format!("engine: {:?}", e),
            });
        }
        self.stats
            .write_points_ok
            .fetch_add(points.len() as i64, Ordering::SeqCst);
        self.stats.write_req_ok.fetch_add(1, Ordering::SeqCst);
        slow_trace::log_background_stage("write_total", write_total_start, "");
        write_err
    }

    fn validate_series_and_fields<'a>(
        validate_keys: bool,
        points: &'a mut [crate::models::Point],
        engine: &Engine,
        measurement_schemas: &MeasurementSchemaRegistry,
        stats: &ReefsDbStatistics,
    ) -> (&'a [crate::models::Point], Vec<FieldCreate>, Result<()>) {
        let mut fields_to_create = vec![];
        let mut err = Ok(());
        let mut points_dropped = 0i32;
        let mut err_reason = String::new();

        let mut j = 0;
        for i in 0..points.len() {
            let p = &mut points[i];
            let tags = p.tags();
            if tags.get(TIME_COLUMN_NAME.as_bytes()).is_some() {
                points_dropped += 1;
                if err_reason.is_empty() {
                    let point_name = p.name();
                    err_reason = format!(
                        "invalid tag key: input tag \"{}\" on measurement \"{}\" is invalid",
                        "time",
                        to_str!(point_name.as_slice())
                    );
                }
                continue;
            }

            if validate_keys && !crate::models::points::valid_key_tokens(&p.name(), &tags) {
                points_dropped += 1;
                if err_reason.is_empty() {
                    err_reason = format!("key contains invalid unicode: {:?}", to_str!(p.key()));
                }
                continue;
            }

            let valid_field = p
                .field_iter()
                .any(|field| field.field_key() != TIME_COLUMN_NAME.as_bytes());
            if !valid_field {
                if err_reason.is_empty() {
                    let point_name = p.name();
                    err_reason = format!(
                        "invalid field name: input field {} on measurement {} is invalid",
                        "time",
                        to_str!(point_name.as_slice()),
                    );
                }
                points_dropped += 1;
                continue;
            }

            points[j] = points[i].clone();
            j += 1;
        }
        let points = &mut points[..j];

        let mut j = 0;
        for i in 0..points.len() {
            let p = &points[i];
            let name = p.name();
            let mf = engine.measurement_fields(&name);
            if let Err(e) = validate_field(&mf, p) {
                match e {
                    Error::PartialWrite {
                        reason,
                        dropped,
                        dropped_keys,
                    } => {
                        if err_reason.is_empty() {
                            err_reason = reason;
                            points_dropped += dropped;
                            stats.write_points_dropped.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                    _ => {
                        return (&[], vec![], Err(e));
                    }
                }
                continue;
            }

            points[j] = points[i].clone();
            j += 1;

            for field in points[i].field_iter() {
                let field_key = field.field_key();
                if field_key == TIME_COLUMN_NAME.as_bytes() {
                    continue;
                }

                if mf.field_bytes(&field_key).is_some() {
                    continue;
                }

                let Some(data_type) = storage_field_type(field.field_type()) else {
                    continue;
                };

                fields_to_create.push(FieldCreate {
                    measurement: name.clone(),
                    field: Field {
                        id: 0,
                        name: to_string!(field_key.to_vec()),
                        typ: data_type,
                    },
                });
            }
        }
        let points = &mut points[..j];

        if !points.is_empty() {
            let schemas = match schemas_from_points(points) {
                Ok(schemas) => schemas,
                Err(source) => {
                    return (&[], vec![], Err(Error::MeasurementSchema { source }));
                }
            };
            if let Err(source) = measurement_schemas.ensure_measurement_schemas(schemas) {
                return (&[], vec![], Err(Error::MeasurementSchema { source }));
            }

            let mut keys = Vec::with_capacity(points.len());
            let mut names = Vec::with_capacity(points.len());
            let mut tags = Vec::with_capacity(points.len());
            for point in points.iter_mut() {
                keys.push(point.key.clone());
                names.push(point.name());
                tags.push(point.tags());
            }
            let key_refs = keys.iter().map(Vec::as_slice).collect::<Vec<_>>();
            let index_create_start = slow_trace::now();
            if let Err(source) = engine.create_series_list_if_not_exists(&key_refs, names, tags) {
                return (&[], vec![], Err(Error::Engine { source }));
            }
            slow_trace::log_background_stage("write_index_create_series", index_create_start, "");
        }

        if points_dropped > 0 {
            err = Err(Error::PartialWrite {
                reason: err_reason,
                dropped: points_dropped,
                dropped_keys: vec![],
            })
        }

        (points, fields_to_create, err)
    }

    /// Deletes all values from for seriesKeys between min and max (inclusive)
    /// Note, if this method fails, itr will be closed.
    pub fn delete_series_range(
        &self,
        itr: Box<dyn SeriesIterator>,
        min: i64,
        max: i64,
    ) -> Result<()> {
        let _lease = self.acquire_borrowed_operation()?;
        let inner = self.inner.read();
        let engine = inner.engine()?;
        engine
            .delete_series_range(itr, min, max)
            .context(EngineSnafu)
    }

    /// Deletes all values from for seriesKeys between min and max (inclusive)
    /// for which predicate() returns true. If predicate() is nil, then all values in range are deleted.
    /// Note, if this method fails, itr will be closed.
    pub fn delete_series_range_with_predicate<F>(
        &self,
        mut itr: Box<dyn SeriesIterator>,
        predicate: Option<F>,
    ) -> Result<()>
    where
        F: Fn(&[u8], &crate::models::Tags) -> (i64, i64, bool),
    {
        let _lease = self.acquire_borrowed_operation()?;
        let inner = self.inner.read();
        let engine = inner.engine()?;
        engine
            .delete_series_range_with_predicate(itr, predicate)
            .context(EngineSnafu)
    }

    /// Durably deletes this shard's values for `name` without deleting local
    /// enumeration metadata or database-wide identity/schema metadata.
    ///
    /// A future database-level coordinator must hold an exclusive DDL gate for
    /// this measurement across all shard data phases, local finalization,
    /// SeriesFile cleanup, and final MeasurementSchemaRegistry removal. The
    /// local storage coordination performed here does not exclude writes.
    pub fn delete_measurement_data(&self, name: &[u8]) -> Result<MeasurementDropLocalResult> {
        let _ = schema_name(name)?;
        let _lease = self.acquire_borrowed_operation()?;
        let inner = self.inner.read();
        let series_ids = inner
            .engine()?
            .delete_measurement_data(name)
            .context(EngineSnafu)?;
        Ok(MeasurementDropLocalResult { series_ids })
    }

    /// Removes this shard's TSI and `fields.idx` metadata after its measurement
    /// data phase has succeeded. This never mutates the database-wide SeriesFile
    /// or MeasurementSchemaRegistry and is safe to retry.
    ///
    /// The caller must hold the same database-level exclusive measurement DDL
    /// gate required by [`Self::delete_measurement_data`].
    pub fn finalize_measurement_drop_local(&self, name: &[u8]) -> Result<()> {
        let _ = schema_name(name)?;
        let _lease = self.acquire_borrowed_operation()?;
        let inner = self.inner.read();
        inner
            .engine()?
            .finalize_measurement_drop_local(name)
            .context(EngineSnafu)
    }

    // DROP MEASUREMENT <measurement_name>
    // The DROP MEASUREMENT query deletes all data and series from the specified measurement and
    // deletes the measurement from the index.
    // DeleteMeasurement deletes a measurement and all underlying series.
    pub fn delete_measurement(&self, name: &[u8]) -> Result<()> {
        let _lease = self.acquire_borrowed_operation()?;
        let inner = self.inner.read();
        let engine = inner.engine()?;
        engine.delete_measurement(name).context(EngineSnafu)
    }

    /// Deletes points of a single measurement matching `predicate` (per-series
    /// `(name, tags) -> (min, max, should_delete)`). The predicate carries both
    /// the time range and any tag filter, so this is the range/predicate
    /// counterpart of `delete_measurement`. A missing measurement is a no-op.
    pub fn delete_series_range_by_measurement<F>(
        &self,
        name: &[u8],
        predicate: Option<F>,
    ) -> Result<()>
    where
        F: Fn(&[u8], &crate::models::Tags) -> (i64, i64, bool),
    {
        let _lease = self.acquire_borrowed_operation()?;
        self.delete_series_range_by_measurement_unleased(name, predicate)
    }

    /// Internal mapped-operation path. The caller must already hold a shard
    /// operation lease for this shard.
    pub(crate) fn delete_series_range_by_measurement_unleased<F>(
        &self,
        name: &[u8],
        predicate: Option<F>,
    ) -> Result<()>
    where
        F: Fn(&[u8], &crate::models::Tags) -> (i64, i64, bool),
    {
        let inner = self.inner.read();
        let engine = inner.engine()?;
        engine
            .delete_series_range_by_measurement(name, predicate)
            .context(EngineSnafu)
    }

    /// Returns the unique number of series in the shard.
    pub fn series_n(&self) -> i64 {
        let inner = self.inner.read();
        match inner.engine() {
            Ok(engine) => engine.series_n(),
            Err(err) => 0,
        }
    }

    /// Returns fields for a measurement.
    pub fn measurement_fields(&self, name: &[u8]) -> Option<Arc<MeasurementFields>> {
        let _lease = self.acquire_borrowed_operation().ok()?;
        self.measurement_fields_unleased(name)
    }

    fn measurement_fields_unleased(&self, name: &[u8]) -> Option<Arc<MeasurementFields>> {
        let inner = self.inner.read();
        match inner.engine() {
            Ok(engine) => engine.visible_measurement_fields(name),
            Err(_) => None,
        }
    }

    // Returns true if the shard contains name.
    // TODO(edd): This method is currently only being called from tests; do we
    // really need it?
    pub fn measurement_exists(&self, name: &[u8]) -> Result<bool> {
        let _lease = self.acquire_borrowed_operation()?;
        {
            let inner = self.inner.read();
            let _ = inner.engine()?;
        }
        Ok(self.measurement_schema_by_bytes(name)?.is_some())
    }

    /// Returns one concrete local field type, if the field exists.
    pub fn local_field_type(&self, measurement: &[u8], field: &[u8]) -> Result<Option<FieldType>> {
        let _lease = self.acquire_borrowed_operation()?;
        self.local_field_type_unleased(measurement, field)
    }

    fn local_field_type_unleased(
        &self,
        measurement: &[u8],
        field: &[u8],
    ) -> Result<Option<FieldType>> {
        {
            let inner = self.inner.read();
            let _ = inner.engine()?;
        }
        let field = schema_name(field)?;
        Ok(self
            .measurement_schema_by_bytes(measurement)?
            .and_then(|schema| schema.fields().get(field).copied()))
    }

    /// Returns whether a local measurement contains the requested tag key.
    pub fn has_tag_key(&self, measurement: &[u8], key: &[u8]) -> Result<bool> {
        let _lease = self.acquire_borrowed_operation()?;
        self.has_tag_key_unleased(measurement, key)
    }

    fn has_tag_key_unleased(&self, measurement: &[u8], key: &[u8]) -> Result<bool> {
        {
            let inner = self.inner.read();
            let _ = inner.engine()?;
        }
        let key = schema_name(key)?;
        Ok(self
            .measurement_schema_by_bytes(measurement)?
            .is_some_and(|schema| schema.tag_keys().contains(key)))
    }

    fn create_fields_and_measurements(
        &self,
        fields_to_create: &[FieldCreate],
        engine: &Engine,
    ) -> Result<()> {
        if fields_to_create.is_empty() {
            return Ok(());
        }

        // add fields
        for f in fields_to_create {
            let mf = engine.measurement_fields(&f.measurement);

            mf.create_field_if_not_exists(f.field.name.as_bytes(), f.field.typ)
                .context(MeasurementFieldsSnafu)?;
        }

        engine
            .measurement_field_set()
            .save()
            .context(MeasurementFieldsSnafu)
    }

    /// Executes one local storage-native scan under a ReefsDb operation lease.
    pub fn create_storage_iterator(
        self: &Arc<Self>,
        scan: StorageScan,
    ) -> Result<Option<LeasedQueryIterator>> {
        let lease = self.acquire_operation()?;
        lease.create_storage_iterator(scan)
    }

    /// Executes one local storage-native scan as streaming Arrow batches.
    pub fn create_record_batch_reader(
        self: &Arc<Self>,
        request: RecordBatchRequest,
    ) -> std::result::Result<RecordBatchReader, RecordBatchError> {
        let lease = self
            .acquire_operation()
            .map_err(RecordBatchError::storage)?;
        lease.create_record_batch_reader(request)
    }

    /// Executes one local storage-native aggregate under a ReefsDb operation lease.
    pub fn aggregate_scan(
        self: &Arc<Self>,
        request: AggregateRequest,
    ) -> Result<Option<AggregateResult>> {
        let lease = self.acquire_operation()?;
        lease.aggregate_scan(request)
    }

    /// Executes one local grouped storage aggregate under a ReefsDb operation lease.
    pub fn grouped_aggregate_scan(
        self: &Arc<Self>,
        request: GroupedAggregateRequest,
    ) -> Result<Option<GroupedAggregateResult>> {
        let lease = self.acquire_operation()?;
        lease.grouped_aggregate_scan(request)
    }

    /// Aggregates one field without scanning other fields to add empty groups.
    ///
    /// A missing target field returns `None`; an empty range yields no groups.
    /// The request must contain exactly one target. The operation lease preserves
    /// the shard lifecycle while the engine executes outside the database lock.
    pub fn grouped_aggregate_scan_target_only(
        self: &Arc<Self>,
        request: GroupedAggregateRequest,
    ) -> Result<Option<GroupedAggregateResult>> {
        let lease = self.acquire_operation()?;
        lease.grouped_aggregate_scan_target_only(request)
    }

    /// Counts exact local logical rows under a ReefsDb operation lease.
    pub fn row_count(self: &Arc<Self>, request: RowCountRequest) -> Result<Option<RowCountResult>> {
        let lease = self.acquire_operation()?;
        lease.row_count(request)
    }

    pub(crate) fn aggregate_scan_unleased(
        &self,
        request: AggregateRequest,
    ) -> Result<Option<AggregateResult>> {
        let prepared = {
            let inner = self.inner.read();
            inner
                .engine()?
                .prepare_aggregate_complete_scan(request.clone())
                .context(EngineSnafu)?
        };
        if let Some(scan) = prepared {
            return scan.run_complete().map(Some).context(EngineSnafu);
        }
        let inner = self.inner.read();
        inner.engine()?.aggregate_scan(request).context(EngineSnafu)
    }

    pub(crate) fn grouped_aggregate_scan_unleased(
        &self,
        request: GroupedAggregateRequest,
    ) -> Result<Option<GroupedAggregateResult>> {
        let inner = self.inner.read();
        inner
            .engine()?
            .grouped_aggregate_scan(request)
            .context(EngineSnafu)
    }

    pub(crate) fn row_count_unleased(
        &self,
        request: RowCountRequest,
    ) -> Result<Option<RowCountResult>> {
        let inner = self.inner.read();
        inner.engine()?.row_count(request).context(EngineSnafu)
    }

    #[cfg(test)]
    fn delete_field_range_for_test(&self, key: Vec<u8>, min: i64, max: i64) -> Result<()> {
        let inner = self.inner.read();
        inner
            .engine()?
            .delete_field_range_for_test(key, min, max)
            .context(EngineSnafu)
    }

    /// Executes a request-projected aggregate under an operation lease. New
    /// aggregate indexes read independently checked columns; legacy engines use
    /// their existing aggregate path and project the result afterwards.
    pub fn aggregate_projected_scan(
        self: &Arc<Self>,
        request: AggregateRequest,
    ) -> Result<Option<ProjectedAggregateResult>> {
        self.acquire_operation()?.aggregate_projected_scan(request)
    }

    fn aggregate_projected_scan_unleased(
        &self,
        request: AggregateRequest,
    ) -> Result<Option<ProjectedAggregateResult>> {
        let prepared = {
            let inner = self.inner.read();
            inner
                .engine()?
                .prepare_aggregate_projected_scan(request.clone())
                .context(EngineSnafu)?
        };
        if let Some(scan) = prepared {
            return scan.run().map(Some).context(EngineSnafu);
        }
        let projection = AggregateProjection::from_aggregates(&request.aggregates);
        Ok(self
            .aggregate_scan_unleased(request)?
            .map(|r| ProjectedAggregateResult::from_complete(r, projection)))
    }

    /// Executes a projected aggregate after exact tag or measurement filtering.
    /// Returns `None` when aggregate indexes or the requested field are absent.
    /// Predicates requiring row evaluation return an error. Convert exclusive
    /// upper time bounds to inclusive bounds before constructing the request.
    pub fn aggregate_projected_scan_filtered(
        self: &Arc<Self>,
        request: AggregateRequest,
        predicate: StoragePredicate,
    ) -> Result<Option<ProjectedAggregateResult>> {
        self.acquire_operation()?
            .aggregate_projected_scan_filtered(request, predicate)
    }

    fn aggregate_projected_scan_filtered_unleased(
        &self,
        request: AggregateRequest,
        predicate: StoragePredicate,
    ) -> Result<Option<ProjectedAggregateResult>> {
        let prepared = {
            let lock_started = slow_trace::now();
            let inner = self.inner.read();
            slow_trace::log_lock_wait("shard.read", lock_started, "filtered aggregate prepare");
            inner
                .engine()?
                .prepare_aggregate_projected_scan_filtered(request, predicate)
                .context(EngineSnafu)?
        };
        prepared
            .map(|scan| scan.run().context(EngineSnafu))
            .transpose()
    }

    /// Executes multiple fields over ordered, disjoint inclusive time windows.
    /// Requests must share a measurement and time range equal to the windows'
    /// envelope. Results retain request/window order; a missing field or disabled
    /// aggregate index produces `None` for that field. Row predicates are rejected.
    /// Series selection is shared; cache/file visibility retains the existing
    /// per-series-field capture order, without promising transaction isolation.
    pub fn aggregate_projected_scan_windows_filtered(
        self: &Arc<Self>,
        requests: Vec<AggregateRequest>,
        windows: Vec<crate::TimeRange>,
        predicate: StoragePredicate,
    ) -> Result<Vec<Option<ProjectedAggregateWindowsResult>>> {
        self.acquire_operation()?
            .aggregate_projected_scan_windows_filtered(requests, windows, predicate)
    }

    fn aggregate_projected_scan_windows_filtered_unleased(
        &self,
        requests: Vec<AggregateRequest>,
        windows: Vec<crate::TimeRange>,
        predicate: StoragePredicate,
    ) -> Result<Vec<Option<ProjectedAggregateWindowsResult>>> {
        let prepared = {
            let started = slow_trace::now();
            let inner = self.inner.read();
            slow_trace::log_lock_wait("shard.read", started, "window aggregate prepare");
            inner
                .engine()?
                .prepare_aggregate_windows_scan_filtered(requests, windows, predicate)
                .context(EngineSnafu)?
        };
        prepared.run().context(EngineSnafu)
    }

    /// Internal mapped-operation path. The caller must already hold a ReefsDb
    /// operation lease for this storage scan.
    pub(crate) fn create_storage_iterator_unleased(
        &self,
        scan: StorageScan,
    ) -> Result<Option<Box<dyn QueryIterator + Sync + Send>>> {
        let shard_lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait(
            "shard.read",
            shard_lock_start,
            "stage=query_wait_for_shard_lock",
        );
        let engine = inner.engine()?;

        let query_start = slow_trace::now();
        let iter = engine.create_storage_iterator(scan)?;
        slow_trace::log_query_stage(
            "query_total",
            query_start,
            "component=shard_create_storage_iterator",
        );
        Ok(iter)
    }

    fn create_direct_tsm_scans_unleased(
        &self,
        scan: &StorageScan,
    ) -> Result<Option<DirectTsmScanSet>> {
        let inner = self.inner.read();
        inner
            .engine()?
            .create_direct_tsm_scans(scan)
            .context(EngineSnafu)
    }

    #[cfg(test)]
    fn create_direct_tsm_scans_with_row_domain_values_unleased(
        &self,
        scan: &StorageScan,
    ) -> Result<Option<DirectTsmScanSet>> {
        let inner = self.inner.read();
        inner
            .engine()?
            .create_direct_tsm_scans_with_row_domain_values(scan)
            .context(EngineSnafu)
    }

    /// Returns a list of measurement names for the shard.
    pub fn measurement_names(&self) -> Result<Vec<Vec<u8>>> {
        let _lease = self.acquire_borrowed_operation()?;
        self.measurement_names_unleased()
    }

    fn measurement_names_unleased(&self) -> Result<Vec<Vec<u8>>> {
        {
            let inner = self.inner.read();
            let _ = inner.engine()?;
        }
        Ok(self.measurement_schemas.measurement_names())
    }

    /// FieldDimensions returns unique sets of fields and dimensions across a
    /// list of measurement sources for this shard. `fields` maps each field key
    /// to its data type; `dimensions` is the set of tag keys. This is the
    /// primary metadata used to build a query/table schema.
    pub fn field_dimensions(
        &self,
        measurements: &[&[u8]],
    ) -> Result<(HashMap<String, FieldType>, HashSet<String>)> {
        let _lease = self.acquire_borrowed_operation()?;
        self.field_dimensions_unleased(measurements)
    }

    fn field_dimensions_unleased(
        &self,
        measurements: &[&[u8]],
    ) -> Result<(HashMap<String, FieldType>, HashSet<String>)> {
        {
            #[cfg(feature = "slow-trace")]
            let lock_started = slow_trace::now();
            let inner = self.inner.read();
            #[cfg(feature = "slow-trace")]
            let lock_wait = slow_trace::elapsed(lock_started);
            let _ = inner.engine()?;
            #[cfg(feature = "slow-trace")]
            {
                drop(inner);
                slow_trace::log_planner_lock_wait("reefsdb.inner.read", lock_wait);
            }
        }

        let mut fields: HashMap<String, FieldType> = HashMap::new();
        let mut dimensions: HashSet<String> = HashSet::new();

        for &name in measurements {
            if let Some(schema) = self.measurement_schema_by_bytes(name)? {
                fields.extend(schema.fields().iter().map(|(key, typ)| (key.clone(), *typ)));
                dimensions.extend(schema.tag_keys().iter().cloned());
            }
        }

        Ok((fields, dimensions))
    }

    /// Builds the Arrow schema for a measurement on this shard: a `time` column
    /// plus every tag key (as Utf8) and field key (typed), in one flat column
    /// space. Ready for a DataFusion `TableProvider`.
    pub fn arrow_schema(&self, measurement: &[u8]) -> Result<arrow_schema::SchemaRef> {
        let (fields, dimensions) = self.field_dimensions(&[measurement])?;
        Ok(crate::query::metadata::build_arrow_schema(
            &fields,
            &dimensions,
        ))
    }

    /// Returns the set of tag keys for a measurement on this shard.
    pub fn measurement_tag_keys(&self, name: &[u8]) -> Result<HashSet<String>> {
        let _lease = self.acquire_borrowed_operation()?;
        self.measurement_tag_keys_unleased(name)
    }

    fn measurement_tag_keys_unleased(&self, name: &[u8]) -> Result<HashSet<String>> {
        {
            let inner = self.inner.read();
            let _ = inner.engine()?;
        }
        Ok(self
            .measurement_schema_by_bytes(name)?
            .map_or_else(HashSet::new, |schema| {
                schema.tag_keys().iter().cloned().collect()
            }))
    }

    /// SHOW TAG KEYS FROM <measurement>: the sorted set of tag keys.
    pub fn tag_keys(&self, name: &[u8]) -> Result<Vec<String>> {
        let mut keys: Vec<String> = self.measurement_tag_keys(name)?.into_iter().collect();
        keys.sort();
        Ok(keys)
    }

    /// SHOW TAG VALUES FROM <measurement> WITH KEY IN (<keys>): the values for
    /// each requested tag key, returned as `(key, values)` pairs.
    pub fn tag_values(&self, name: &[u8], keys: &[&str]) -> Result<Vec<(String, Vec<String>)>> {
        let _lease = self.acquire_borrowed_operation()?;
        self.tag_values_unleased(name, keys)
    }

    fn tag_values_unleased(
        &self,
        name: &[u8],
        keys: &[&str],
    ) -> Result<Vec<(String, Vec<String>)>> {
        // The collector sorts `owned` in place and returns values aligned to it.
        let mut owned: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        let inner = self.inner.read();
        let values = inner
            .engine()?
            .measurement_tag_key_values(name, &mut owned, false)?;
        Ok(owned.into_iter().zip(values).collect())
    }

    /// SHOW SERIES FROM <measurement>: all series keys for a measurement.
    pub fn measurement_series_keys(&self, name: &[u8]) -> Result<Vec<Vec<u8>>> {
        let _lease = self.acquire_borrowed_operation()?;
        self.measurement_series_keys_unleased(name)
    }

    fn measurement_series_keys_unleased(&self, name: &[u8]) -> Result<Vec<Vec<u8>>> {
        let inner = self.inner.read();
        Ok(inner.engine()?.measurement_series_keys(name)?)
    }

    /// Returns measurement names selected by a storage-native predicate.
    pub fn measurement_names_by_storage_predicate(
        &self,
        predicate: Option<&StoragePredicate>,
    ) -> Result<Vec<Vec<u8>>> {
        let _lease = self.acquire_borrowed_operation()?;
        self.measurement_names_by_storage_predicate_unleased(predicate)
    }

    fn measurement_names_by_storage_predicate_unleased(
        &self,
        predicate: Option<&StoragePredicate>,
    ) -> Result<Vec<Vec<u8>>> {
        let inner = self.inner.read();
        Ok(inner
            .engine()?
            .measurement_names_by_storage_predicate(predicate)?)
    }

    /// Returns series keys selected by a storage-native predicate.
    pub fn series_keys_by_storage_predicate(
        &self,
        name: &[u8],
        predicate: Option<&StoragePredicate>,
    ) -> Result<Vec<Vec<u8>>> {
        let _lease = self.acquire_borrowed_operation()?;
        self.series_keys_by_storage_predicate_unleased(name, predicate)
    }

    fn series_keys_by_storage_predicate_unleased(
        &self,
        name: &[u8],
        predicate: Option<&StoragePredicate>,
    ) -> Result<Vec<Vec<u8>>> {
        let inner = self.inner.read();
        Ok(inner
            .engine()?
            .series_keys_by_storage_predicate(name, predicate)?)
    }

    /// Estimated cardinality of a single tag key within a measurement.
    pub fn tag_key_cardinality(&self, name: &[u8], key: &[u8]) -> i64 {
        let Ok(_lease) = self.acquire_borrowed_operation() else {
            return 0;
        };
        let inner = self.inner.read();
        match inner.engine() {
            Ok(engine) => engine.tag_key_cardinality(name, key),
            Err(_) => 0,
        }
    }

    /// HLL sketches (present, tombstoned) estimating series cardinality.
    pub fn series_sketches(&self) -> Result<(HLL, HLL)> {
        let _lease = self.acquire_borrowed_operation()?;
        self.series_sketches_unleased()
    }

    fn series_sketches_unleased(&self) -> Result<(HLL, HLL)> {
        let inner = self.inner.read();
        Ok(inner.engine()?.series_sketches()?)
    }

    /// HLL sketches (present, tombstoned) estimating measurement cardinality.
    pub fn measurements_sketches(&self) -> Result<(HLL, HLL)> {
        let _lease = self.acquire_borrowed_operation()?;
        self.measurements_sketches_unleased()
    }

    fn measurements_sketches_unleased(&self) -> Result<(HLL, HLL)> {
        let inner = self.inner.read();
        Ok(inner.engine()?.measurements_sketches()?)
    }

    /// create_snapshot will return a path to a temp directory
    /// containing hard links to the underlying shard files.
    pub fn create_snapshot(&self, skip_cache_ok: bool) -> Result<PathBuf> {
        let inner = self.inner.read();
        let engine = inner.engine()?;
        engine.create_snapshot(skip_cache_ok).context(EngineSnafu)
    }

    pub fn statistics(&self, tags: HashMap<String, String>) -> Vec<crate::models::Statistic> {
        let inner = self.inner.read();
        let engine = match inner.engine() {
            Ok(engine) => engine,
            Err(err) => {
                return vec![];
            }
        };

        // Refresh our disk size stat
        if self.disk_size().is_err() {
            return vec![];
        }
        let series_n = engine.series_n();

        let mut tags = self.default_tags.merge(tags);

        // Set the index type on the tags.  N.B this needs to be checked since it's
        // only set when the shard is opened.
        let index_type = self.index_type();
        if !index_type.is_empty() {
            tags.insert("index_type".to_string(), index_type);
        }

        let mut statistics: Vec<crate::models::Statistic> = vec![crate::models::Statistic {
            name: "shard".to_string(),
            tags: tags.clone(),
            values: HashMap::from([
                (
                    STAT_WRITE_REQ.to_string(),
                    self.stats.write_req.load(Ordering::SeqCst),
                ),
                (
                    STAT_WRITE_REQ_OK.to_string(),
                    self.stats.write_req_ok.load(Ordering::SeqCst),
                ),
                (
                    STAT_WRITE_REQ_ERR.to_string(),
                    self.stats.write_req_err.load(Ordering::SeqCst),
                ),
                (STAT_SERIES_CREATE.to_string(), series_n),
                (
                    STAT_FIELDS_CREATE.to_string(),
                    self.stats.fields_created.load(Ordering::SeqCst),
                ),
                (
                    STAT_WRITE_POINTS_ERR.to_string(),
                    self.stats.write_points_err.load(Ordering::SeqCst),
                ),
                (
                    STAT_WRITE_POINTS_DROPPED.to_string(),
                    self.stats.write_points_dropped.load(Ordering::SeqCst),
                ),
                (
                    STAT_WRITE_POINTS_OK.to_string(),
                    self.stats.write_points_ok.load(Ordering::SeqCst),
                ),
                (
                    STAT_WRITE_BYTES.to_string(),
                    self.stats.bytes_written.load(Ordering::SeqCst),
                ),
                (
                    STAT_DISK_BYTES.to_string(),
                    self.stats.disk_bytes.load(Ordering::SeqCst),
                ),
            ]),
        }];

        // Add the index and engine statistics.
        statistics.append(&mut engine.statistics(tags));

        statistics
    }
}

impl Inner {
    /// engine safely (under an RLock) returns a reference to the shard's Engine, or
    /// an error if the Engine is closed, or the shard is currently disabled.
    ///
    /// The shard's Engine should always be accessed via a call to inner.engine(), rather
    /// than directly referencing ReefsDb.engine.
    fn engine(&self) -> Result<&Engine> {
        self.ready()?;
        Ok(self.engine.as_ref().unwrap())
    }

    fn engine_arc(&self) -> Result<Arc<Engine>> {
        self.ready()?;
        Ok(Arc::clone(self.engine.as_ref().unwrap()))
    }

    fn ready(&self) -> Result<()> {
        let mut err = Ok(());
        if self.engine.is_none() {
            err = Err(Error::EngineClosed);
        } else if !self.enabled {
            err = Err(Error::ShardDisabled);
        }

        err
    }
}

// ReefsDbStatistics maintains statistics for a shard.
#[derive(Default)]
pub struct ReefsDbStatistics {
    write_req: AtomicI64,
    write_req_ok: AtomicI64,
    write_req_err: AtomicI64,
    fields_created: AtomicI64,
    write_points_err: AtomicI64,
    write_points_dropped: AtomicI64,
    write_points_ok: AtomicI64,
    bytes_written: AtomicI64,
    disk_bytes: AtomicI64,
}

pub struct FieldCreate {
    measurement: Vec<u8>,
    field: Field,
}

fn legacy_measurement_schemas(engine: &Engine) -> Result<Vec<MeasurementSchema>> {
    let mut schemas = Vec::new();
    for name in engine.measurement_names()? {
        let name_string = schema_name(&name)?.to_string();
        let fields = engine
            .measurement_fields(&name)
            .field_set()
            .into_iter()
            .collect();
        let tags = engine.measurement_tag_keys(&name)?.into_iter().collect();
        schemas.push(
            MeasurementSchema::new(name_string, tags, fields).context(MeasurementSchemaSnafu)?,
        );
    }
    Ok(schemas)
}

fn schema_name(name: &[u8]) -> Result<&str> {
    std::str::from_utf8(name).map_err(|source| Error::StringErr {
        reason: format!("measurement schema name is not valid UTF-8: {source}"),
    })
}

// Field represents a series field. All of the fields must be hashable.
pub struct Field {
    id: u8,
    name: String,
    typ: FieldType,
}

fn validate_field(mf: &Arc<MeasurementFields>, point: &crate::models::Point) -> Result<()> {
    let iter = point.field_iter();
    for k in iter {
        // Skip fields name "time", they are illegal.
        if k.field_key() == TIME_COLUMN_NAME.as_bytes() {
            continue;
        }

        // If the fields is not present, there cannot be a conflict.
        let f = mf.field_bytes(k.field_key());
        if f.is_none() {
            continue;
        }

        let f = f.unwrap();
        let Some(data_type) = storage_field_type(k.field_type()) else {
            continue;
        };
        // If the types are not the same, there is a conflict.
        if f.typ != data_type {
            let point_name = point.name();
            return Err(Error::PartialWrite {
                reason: format!(
                    "{}: input field \"{}\" on measurement \"{}\" is type {}, already exists as type {}",
                    Error::FieldTypeConflict,
                    to_str!(k.field_key()),
                    to_str!(&point_name),
                    data_type,
                    f.typ
                ),
                dropped: 1,
                dropped_keys: vec![],
            });
        }
    }

    Ok(())
}

#[cfg(any(test, feature = "benchmarking"))]
pub mod tests {
    #[cfg(test)]
    #[path = "aggregate_group_tests.rs"]
    mod aggregate_group_tests;
    use std::{
        collections::{HashMap, VecDeque},
        path::Path,
        sync::Arc,
        time::Instant,
    };

    use crate::common::{time, wait_group::WaitGroup};
    use crate::models::{self, Fields, RawVal, points::Point};
    use crate::runtime::global::spawn_blocking_global;
    use arrow_array::{
        Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
        TimestampNanosecondArray, UInt64Array,
    };
    use arrow_schema::{DataType as ArrowType, Field as ArrowField, Schema as ArrowSchema};
    use rand::{Rng, SeedableRng, rngs::StdRng};
    use scopeguard::defer;
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;

    use super::Error;
    use crate::{
        db::{LeasedQueryIterator, ReefsDb},
        query::{
            query_iterator::{self, FloatIterator},
            query_point,
            storage_ir::{
                ColumnKind, ColumnRef, CompareOp, MeasurementRef, OutputOrder, RowCountRequest,
                ScanCancellation, SeriesProjection, StoragePredicate, StorageProjection,
                StorageScan, StorageValueExpr, TimeOrder, TimeRange,
            },
        },
        series::{
            measurement_fields::MeasurementFieldSet,
            measurement_schema::{MEASUREMENT_SCHEMA_FILE_NAME, MeasurementSchemaRegistry},
            series_file::{self, SeriesFile, tests::must_open_series_file},
            series_set,
        },
        storage::{
            EngineOptions, active_cursor as iterator,
            tsm1::engine::tests::{TestSeriesIDSets, generate_data_with_tag_count},
        },
    };

    fn measurement_schemas_for(sfile: &SeriesFile) -> Arc<MeasurementSchemaRegistry> {
        Arc::new(
            MeasurementSchemaRegistry::open(sfile.path().join(MEASUREMENT_SCHEMA_FILE_NAME))
                .unwrap(),
        )
    }

    /// Create several shards all sharing the same
    fn new_test_reefsdb(
        id: u64,
        root: &Path,
        sfile: Arc<SeriesFile>,
        mut opt: EngineOptions,
    ) -> Arc<ReefsDb> {
        opt.shard_id = id;
        let shard_root = root.join(format!("shard_{id}"));
        let wal_root = root.join(format!("wal_{id}"));
        let vlog_root = root.join(format!("vlog_{id}"));
        let vlog_index_wal_root = root.join(format!("vlog_index_wal_{id}"));
        let measurement_schemas = measurement_schemas_for(&sfile);

        // let mut opt = EngineOptions::default();
        opt.config.wal_dir = wal_root.to_str().unwrap().to_string();
        // let series_ids = Arc::new(series_set::SeriesIDSet::new(&[]));
        // opt.series_id_sets = Some(Box::new(TestSeriesIDSets(vec![Arc::clone(&series_ids)])));

        let shard = Arc::new(
            ReefsDb::new(
                shard_root,
                wal_root,
                vlog_root,
                vlog_index_wal_root,
                sfile,
                measurement_schemas,
                opt,
            )
            .unwrap(),
        );
        shard.open().unwrap();
        shard
    }

    fn collect_float_points(mut iterator: LeasedQueryIterator) -> Vec<query_point::FloatPoint> {
        let iterator_any = iterator.as_any_mut();
        let float_iterator = if let Some(iterator) =
            iterator_any.downcast_mut::<query_iterator::FloatMergeIter>()
        {
            iterator as &mut dyn FloatIterator
        } else if let Some(iterator) =
            iterator_any.downcast_mut::<query_iterator::FloatInterruptIter>()
        {
            iterator as &mut dyn FloatIterator
        } else if let Some(iterator) = iterator_any.downcast_mut::<query_iterator::FloatLimitIter>()
        {
            iterator as &mut dyn FloatIterator
        } else if let Some(iterator) =
            iterator_any.downcast_mut::<query_iterator::FloatSortedMergeIter>()
        {
            iterator as &mut dyn FloatIterator
        } else if let Some(iterator) = iterator_any.downcast_mut::<iterator::FloatIter>() {
            iterator as &mut dyn FloatIterator
        } else {
            panic!("invalid float iterator")
        };
        let points = std::iter::from_fn(|| float_iterator.next()).collect();
        iterator.close();
        points
    }

    fn float_storage_scan(field: &str) -> StorageScan {
        let value = ColumnRef::new(field, ColumnKind::Field(Some(crate::models::FLOAT)));
        StorageScan {
            measurement: MeasurementRef::new("cpu"),
            projection: StorageProjection {
                primary_column: Some(value.clone()),
                returned_columns: vec![value],
                row_domain_columns: vec![],
                series: SeriesProjection::default(),
            },
            time_range: TimeRange::all(),
            predicate: None,
            time_order: TimeOrder::Ascending,
            output_order: OutputOrder::TimeOrdered,
            execution: Default::default(),
            cancellation: None,
        }
    }

    fn all_types_aux_storage_scan() -> StorageScan {
        StorageScan {
            measurement: MeasurementRef::new("cpu"),
            projection: StorageProjection {
                primary_column: None,
                returned_columns: vec![
                    ColumnRef::new("float", ColumnKind::Field(Some(crate::models::FLOAT))),
                    ColumnRef::new("integer", ColumnKind::Field(Some(crate::models::INTEGER))),
                    ColumnRef::new("unsigned", ColumnKind::Field(Some(crate::models::UNSIGNED))),
                    ColumnRef::new("boolean", ColumnKind::Field(Some(crate::models::BOOL))),
                    ColumnRef::new("string", ColumnKind::Field(Some(crate::models::STRING))),
                ],
                row_domain_columns: vec![],
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

    fn all_types_arrow_schema() -> arrow_schema::SchemaRef {
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                crate::query::metadata::TIME_COLUMN_NAME,
                crate::query::metadata::time_arrow_type(),
                false,
            ),
            ArrowField::new("host", ArrowType::Utf8, true),
            ArrowField::new("float", ArrowType::Float64, true),
            ArrowField::new("integer", ArrowType::Int64, true),
            ArrowField::new("unsigned", ArrowType::UInt64, true),
            ArrowField::new("boolean", ArrowType::Boolean, true),
            ArrowField::new("string", ArrowType::Utf8, true),
        ]))
    }

    fn selected_types_row_domain_scan(time_range: TimeRange) -> StorageScan {
        StorageScan {
            measurement: MeasurementRef::new("cpu"),
            projection: StorageProjection {
                primary_column: None,
                returned_columns: vec![
                    ColumnRef::new("float", ColumnKind::Field(Some(crate::models::FLOAT))),
                    ColumnRef::new("integer", ColumnKind::Field(Some(crate::models::INTEGER))),
                ],
                row_domain_columns: vec![
                    ColumnRef::new("unsigned", ColumnKind::Field(Some(crate::models::UNSIGNED))),
                    ColumnRef::new("boolean", ColumnKind::Field(Some(crate::models::BOOL))),
                    ColumnRef::new("string", ColumnKind::Field(Some(crate::models::STRING))),
                ],
                series: SeriesProjection::new(vec!["host".to_string()], vec!["host".to_string()]),
            },
            time_range,
            predicate: None,
            time_order: TimeOrder::Ascending,
            output_order: OutputOrder::TimeOrdered,
            execution: Default::default(),
            cancellation: None,
        }
    }

    fn selected_types_arrow_schema() -> arrow_schema::SchemaRef {
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                crate::query::metadata::TIME_COLUMN_NAME,
                crate::query::metadata::time_arrow_type(),
                false,
            ),
            ArrowField::new("host", ArrowType::Utf8, true),
            ArrowField::new("float", ArrowType::Float64, true),
            ArrowField::new("integer", ArrowType::Int64, true),
        ]))
    }

    fn row_count_request(measurement: &str, time_range: TimeRange) -> RowCountRequest {
        RowCountRequest {
            measurement: MeasurementRef::new(measurement),
            time_range,
            cancellation: None,
        }
    }

    fn raw_float_row_domain_count(shard: &Arc<ReefsDb>, time_range: TimeRange) -> u64 {
        let scan = StorageScan {
            measurement: MeasurementRef::new("cpu"),
            projection: StorageProjection {
                primary_column: None,
                returned_columns: vec![],
                row_domain_columns: vec![
                    ColumnRef::new("a", ColumnKind::Field(Some(crate::models::FLOAT))),
                    ColumnRef::new("b", ColumnKind::Field(Some(crate::models::FLOAT))),
                ],
                series: SeriesProjection::new(vec!["host".to_string()], vec![]),
            },
            time_range,
            predicate: None,
            time_order: TimeOrder::Ascending,
            output_order: OutputOrder::TimeOrdered,
            execution: Default::default(),
            cancellation: None,
        };
        let lease = shard.acquire_operation().unwrap();
        let iterator = lease
            .create_storage_query_iterator(scan)
            .unwrap()
            .expect("row-domain fixture must create an iterator");
        query_iterator::count_iterator_rows(iterator).unwrap()
    }

    #[test]
    fn exact_row_count_direct_path_matches_sparse_multi_series_row_domain_and_reopen() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let mut options = EngineOptions::default();
        options.compaction_disabled = true;
        let shard = new_test_reefsdb(1, tmp_dir.path(), Arc::clone(&sfile), options.clone());
        shard
            .write_points_string(&[
                "cpu,host=A a=1.0 10",
                "cpu,host=A b=2.0 20",
                "cpu,host=A a=3.0,b=4.0 30",
                "cpu,host=B a=5.0,b=6.0 10",
                "cpu,host=B b=7.0 20",
            ])
            .unwrap();
        shard.create_snapshot(false).unwrap();

        for (range, expected) in [
            (TimeRange::all(), 5),
            (TimeRange::new(10, 20), 4),
            (TimeRange::new(11, 29), 2),
            (TimeRange::new(100, 200), 0),
        ] {
            let result = shard
                .row_count(row_count_request("cpu", range))
                .unwrap()
                .unwrap();
            assert_eq!(result.count, expected);
            assert_eq!(result.count, raw_float_row_domain_count(&shard, range));
            assert!(!result.metrics.used_fallback);
            assert_eq!(result.metrics.raw_scans, 0);
            assert_eq!(result.metrics.fallback_rows, 0);
        }
        let full = shard
            .row_count(row_count_request("cpu", TimeRange::all()))
            .unwrap()
            .unwrap();
        assert_eq!(full.metrics.series_scanned, 2);
        assert!(full.metrics.timestamp_only_blocks > 0);
        assert_eq!(full.metrics.timestamp_only_points, 7);
        assert!(
            shard
                .row_count(row_count_request("missing", TimeRange::all()))
                .unwrap()
                .is_none()
        );

        shard.close().unwrap();
        let reopened = new_test_reefsdb(1, tmp_dir.path(), Arc::clone(&sfile), options);
        let reopened_result = reopened
            .row_count(row_count_request("cpu", TimeRange::all()))
            .unwrap()
            .unwrap();
        assert_eq!(reopened_result.count, 5);
        assert_eq!(
            reopened_result.count,
            raw_float_row_domain_count(&reopened, TimeRange::all())
        );
        assert!(!reopened_result.metrics.used_fallback);

        reopened.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn exact_row_count_visibility_fallback_matches_cache_and_tombstones() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let mut options = EngineOptions::default();
        options.compaction_disabled = true;
        let shard = new_test_reefsdb(1, tmp_dir.path(), Arc::clone(&sfile), options);
        shard
            .write_points_string(&["cpu,host=A a=1.0,b=10.0 10", "cpu,host=A a=2.0 20"])
            .unwrap();
        shard.create_snapshot(false).unwrap();
        shard
            .write_points_string(&["cpu,host=A b=20.0 20", "cpu,host=A a=3.0 30"])
            .unwrap();

        let cached = shard
            .row_count(row_count_request("cpu", TimeRange::all()))
            .unwrap()
            .unwrap();
        assert_eq!(cached.count, 3);
        assert_eq!(
            cached.count,
            raw_float_row_domain_count(&shard, TimeRange::all())
        );
        assert!(cached.metrics.used_fallback);
        assert_eq!(cached.metrics.raw_scans, 1);
        assert_eq!(cached.metrics.fallback_rows, 3);

        shard.create_snapshot(false).unwrap();
        shard
            .delete_field_range_for_test(
                crate::storage::tsm1::engine::series_field_key_bytes("cpu,host=A", "a"),
                10,
                10,
            )
            .unwrap();
        let one_field_remains = shard
            .row_count(row_count_request("cpu", TimeRange::all()))
            .unwrap()
            .unwrap();
        assert_eq!(one_field_remains.count, 3);
        assert_eq!(
            one_field_remains.count,
            raw_float_row_domain_count(&shard, TimeRange::all())
        );
        assert!(one_field_remains.metrics.used_fallback);

        shard
            .delete_field_range_for_test(
                crate::storage::tsm1::engine::series_field_key_bytes("cpu,host=A", "b"),
                10,
                10,
            )
            .unwrap();
        let final_field_deleted = shard
            .row_count(row_count_request("cpu", TimeRange::all()))
            .unwrap()
            .unwrap();
        assert_eq!(final_field_deleted.count, 2);
        assert_eq!(
            final_field_deleted.count,
            raw_float_row_domain_count(&shard, TimeRange::all())
        );
        assert!(final_field_deleted.metrics.used_fallback);

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn exact_row_count_fallback_matches_overlapping_versions_and_vlog() {
        {
            let tmp_dir = must_temp_reefsdb_dir();
            let sfile = Arc::new(must_open_series_file().0);
            let mut options = EngineOptions::default();
            options.compaction_disabled = true;
            let shard = new_test_reefsdb(1, tmp_dir.path(), Arc::clone(&sfile), options);
            shard
                .write_points_string(&["cpu,host=A a=1.0,b=2.0 10", "cpu,host=A b=3.0 20"])
                .unwrap();
            shard.create_snapshot(false).unwrap();
            shard.write_points_string(&["cpu,host=A a=9.0 10"]).unwrap();
            shard.create_snapshot(false).unwrap();

            let result = shard
                .row_count(row_count_request("cpu", TimeRange::all()))
                .unwrap()
                .unwrap();
            assert_eq!(result.count, 2);
            assert_eq!(
                result.count,
                raw_float_row_domain_count(&shard, TimeRange::all())
            );
            assert!(result.metrics.used_fallback);
            assert_eq!(result.metrics.raw_scans, 1);

            shard.close().unwrap();
            sfile.close().unwrap();
        }

        {
            let tmp_dir = must_temp_reefsdb_dir();
            let sfile = Arc::new(must_open_series_file().0);
            let mut options = EngineOptions::default();
            options.compaction_disabled = true;
            options.vlog_enabled = true;
            options.vlog_config.min_vlog_size = 0;
            let shard = new_test_reefsdb(1, tmp_dir.path(), Arc::clone(&sfile), options);
            shard
                .write_points_string(&[
                    "cpu,host=A a=1.0 10",
                    "cpu,host=A b=2.0 20",
                    "cpu,host=A a=3.0,b=4.0 30",
                ])
                .unwrap();
            shard.create_snapshot(false).unwrap();

            let result = shard
                .row_count(row_count_request("cpu", TimeRange::all()))
                .unwrap()
                .unwrap();
            assert_eq!(result.count, 3);
            assert_eq!(
                result.count,
                raw_float_row_domain_count(&shard, TimeRange::all())
            );
            assert!(result.metrics.used_fallback);
            assert_eq!(result.metrics.raw_scans, 1);

            shard.close().unwrap();
            sfile.close().unwrap();
        }
    }

    fn selected_types_direct_and_iterator_batches(
        shard: &Arc<ReefsDb>,
        time_range: TimeRange,
        batch_rows: usize,
    ) -> (
        Vec<RecordBatch>,
        Vec<RecordBatch>,
        crate::RecordBatchMetrics,
    ) {
        let scan = selected_types_row_domain_scan(time_range);
        selected_types_direct_and_iterator_batches_for_scan(shard, scan, batch_rows)
    }

    fn selected_types_direct_and_iterator_batches_for_scan(
        shard: &Arc<ReefsDb>,
        scan: StorageScan,
        batch_rows: usize,
    ) -> (
        Vec<RecordBatch>,
        Vec<RecordBatch>,
        crate::RecordBatchMetrics,
    ) {
        let request = crate::RecordBatchRequest::new(
            scan.clone(),
            selected_types_arrow_schema(),
            batch_rows,
            usize::MAX,
        );
        let iterator = shard.create_storage_iterator(scan).unwrap();
        let expected = crate::RecordBatchReader::try_new(request.clone(), iterator)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let mut direct = shard.create_record_batch_reader(request).unwrap();
        let actual = direct.by_ref().collect::<Result<Vec<_>, _>>().unwrap();
        (actual, expected, direct.metrics())
    }

    fn single_field_aux_scan(
        field: &str,
        field_type: crate::models::FieldType,
        time_range: TimeRange,
    ) -> StorageScan {
        StorageScan {
            measurement: MeasurementRef::new("cpu"),
            projection: StorageProjection {
                primary_column: None,
                returned_columns: vec![ColumnRef::new(field, ColumnKind::Field(Some(field_type)))],
                row_domain_columns: vec![],
                series: SeriesProjection::new(vec!["host".to_string()], vec!["host".to_string()]),
            },
            time_range,
            predicate: None,
            time_order: TimeOrder::Ascending,
            output_order: OutputOrder::TimeOrdered,
            execution: Default::default(),
            cancellation: None,
        }
    }

    fn single_field_schema(
        field: &str,
        field_type: crate::models::FieldType,
    ) -> arrow_schema::SchemaRef {
        let data_type = match field_type {
            crate::models::FLOAT => ArrowType::Float64,
            crate::models::INTEGER => ArrowType::Int64,
            crate::models::UNSIGNED => ArrowType::UInt64,
            crate::models::BOOL => ArrowType::Boolean,
            crate::models::STRING => ArrowType::Utf8,
            other => panic!("unsupported test field type {other}"),
        };
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                crate::query::metadata::TIME_COLUMN_NAME,
                crate::query::metadata::time_arrow_type(),
                false,
            ),
            ArrowField::new("host", ArrowType::Utf8, true),
            ArrowField::new(field, data_type, true),
        ]))
    }

    fn single_field_batch_rows(
        reader: &mut crate::RecordBatchReader,
        field: &str,
        field_type: crate::models::FieldType,
    ) -> Vec<(i64, Option<String>, crate::models::Val)> {
        let mut rows = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let time = batch
                .column_by_name(crate::query::metadata::TIME_COLUMN_NAME)
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let host = batch
                .column_by_name("host")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let value = match field_type {
                    crate::models::FLOAT => crate::models::Val::F64(
                        batch
                            .column_by_name(field)
                            .unwrap()
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap()
                            .value(row),
                    ),
                    crate::models::INTEGER => crate::models::Val::I64(
                        batch
                            .column_by_name(field)
                            .unwrap()
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(row),
                    ),
                    crate::models::UNSIGNED => crate::models::Val::U64(
                        batch
                            .column_by_name(field)
                            .unwrap()
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap()
                            .value(row),
                    ),
                    crate::models::BOOL => crate::models::Val::Bool(
                        batch
                            .column_by_name(field)
                            .unwrap()
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .unwrap()
                            .value(row),
                    ),
                    crate::models::STRING => crate::models::Val::String(
                        batch
                            .column_by_name(field)
                            .unwrap()
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(row)
                            .to_string(),
                    ),
                    other => panic!("unsupported test field type {other}"),
                };
                rows.push((
                    time.value(row),
                    (!host.is_null(row)).then(|| host.value(row).to_string()),
                    value,
                ));
            }
        }
        rows
    }

    fn assert_single_field_direct_matches_iterator(
        shard: &Arc<ReefsDb>,
        field: &str,
        field_type: crate::models::FieldType,
        time_range: TimeRange,
        expected_rows: usize,
        expected_partial_blocks: u64,
    ) {
        let scan = single_field_aux_scan(field, field_type, time_range);
        let request = crate::RecordBatchRequest::new(
            scan.clone(),
            single_field_schema(field, field_type),
            257,
            usize::MAX,
        );
        let iterator = shard.create_storage_iterator(scan).unwrap();
        let mut fallback = crate::RecordBatchReader::try_new(request.clone(), iterator).unwrap();
        let fallback_rows = single_field_batch_rows(&mut fallback, field, field_type);

        let mut direct = shard.create_record_batch_reader(request).unwrap();
        let direct_rows = single_field_batch_rows(&mut direct, field, field_type);
        assert_eq!(direct_rows, fallback_rows);
        assert_eq!(direct_rows.len(), expected_rows);
        assert_eq!(direct.metrics().direct_batch_rows, expected_rows as u64);
        assert_eq!(direct.metrics().iterator_fallback_rows, 0);
        assert!(direct.metrics().direct_blocks > 0 || expected_rows == 0);
        assert_eq!(direct.metrics().timestamp_only_blocks, 0);
        assert_eq!(
            direct.metrics().value_decoded_blocks,
            direct.metrics().direct_blocks
        );
        assert_eq!(
            direct.metrics().partial_direct_blocks,
            expected_partial_blocks
        );
    }

    fn legacy_single_field_rows(
        shard: &Arc<ReefsDb>,
        field: &str,
        field_type: crate::models::FieldType,
        time_range: TimeRange,
        time_order: TimeOrder,
    ) -> Vec<(i64, Option<String>, crate::models::Val)> {
        let mut scan = single_field_aux_scan(field, field_type, time_range);
        scan.time_order = time_order;
        let request = crate::RecordBatchRequest::new(
            scan.clone(),
            single_field_schema(field, field_type),
            16,
            usize::MAX,
        );
        let iterator = shard.create_storage_iterator(scan).unwrap();
        let mut fallback = crate::RecordBatchReader::try_new(request, iterator).unwrap();
        single_field_batch_rows(&mut fallback, field, field_type)
    }

    fn assert_single_field_partial_direct_matches_full_iterator(
        shard: &Arc<ReefsDb>,
        field: &str,
        field_type: crate::models::FieldType,
        time_range: TimeRange,
        expected_rows: usize,
    ) {
        let full_scan = single_field_aux_scan(field, field_type, TimeRange::all());
        let full_request = crate::RecordBatchRequest::new(
            full_scan.clone(),
            single_field_schema(field, field_type),
            257,
            usize::MAX,
        );
        let iterator = shard.create_storage_iterator(full_scan).unwrap();
        let mut fallback = crate::RecordBatchReader::try_new(full_request, iterator).unwrap();
        let expected = single_field_batch_rows(&mut fallback, field, field_type)
            .into_iter()
            .filter(|(time, _, _)| time_range.contains(*time))
            .collect::<Vec<_>>();

        let request = crate::RecordBatchRequest::new(
            single_field_aux_scan(field, field_type, time_range),
            single_field_schema(field, field_type),
            257,
            usize::MAX,
        );
        let mut direct = shard.create_record_batch_reader(request).unwrap();
        let actual = single_field_batch_rows(&mut direct, field, field_type);

        assert_eq!(actual, expected);
        assert_eq!(actual.len(), expected_rows);
        assert_eq!(direct.metrics().direct_batch_rows, expected_rows as u64);
        assert_eq!(direct.metrics().iterator_fallback_rows, 0);
        assert_eq!(direct.metrics().partial_direct_blocks, 1);
    }

    fn iterator_rows(
        points: Vec<query_point::FloatPoint>,
    ) -> Vec<(i64, Option<String>, Vec<crate::models::Val>)> {
        points
            .into_iter()
            .map(|point| (point.time(), point.tags().value("host"), point.aux))
            .collect()
    }

    fn record_batch_rows(
        reader: &mut crate::RecordBatchReader,
    ) -> Vec<(i64, Option<String>, Vec<crate::models::Val>)> {
        let mut rows = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let time = batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let host = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let float = batch
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            let integer = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let unsigned = batch
                .column(4)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let boolean = batch
                .column(5)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            let string = batch
                .column(6)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            for row in 0..batch.num_rows() {
                let value = |array: &dyn Array, value| {
                    if array.is_null(row) {
                        crate::models::Val::Empty(None)
                    } else {
                        value
                    }
                };
                rows.push((
                    time.value(row),
                    (!host.is_null(row)).then(|| host.value(row).to_string()),
                    vec![
                        value(float, crate::models::Val::F64(float.value(row))),
                        value(integer, crate::models::Val::I64(integer.value(row))),
                        value(unsigned, crate::models::Val::U64(unsigned.value(row))),
                        value(boolean, crate::models::Val::Bool(boolean.value(row))),
                        value(
                            string,
                            crate::models::Val::String(string.value(row).to_string()),
                        ),
                    ],
                ));
            }
        }
        rows
    }

    fn all_types_direct_and_iterator_rows(
        shard: &Arc<ReefsDb>,
        time_range: TimeRange,
        batch_rows: usize,
    ) -> (
        Vec<(i64, Option<String>, Vec<crate::models::Val>)>,
        Vec<(i64, Option<String>, Vec<crate::models::Val>)>,
        crate::RecordBatchMetrics,
    ) {
        let mut scan = all_types_aux_storage_scan();
        scan.time_range = time_range;
        let request = crate::RecordBatchRequest::new(
            scan.clone(),
            all_types_arrow_schema(),
            batch_rows,
            usize::MAX,
        );
        let iterator = shard.create_storage_iterator(scan).unwrap();
        let mut fallback = crate::RecordBatchReader::try_new(request.clone(), iterator).unwrap();
        let expected = record_batch_rows(&mut fallback);
        let (actual, metrics) = all_types_direct_rows(shard, time_range, batch_rows);
        (actual, expected, metrics)
    }

    fn all_types_direct_rows(
        shard: &Arc<ReefsDb>,
        time_range: TimeRange,
        batch_rows: usize,
    ) -> (
        Vec<(i64, Option<String>, Vec<crate::models::Val>)>,
        crate::RecordBatchMetrics,
    ) {
        let mut scan = all_types_aux_storage_scan();
        scan.time_range = time_range;
        let request =
            crate::RecordBatchRequest::new(scan, all_types_arrow_schema(), batch_rows, usize::MAX);
        let mut direct = shard.create_record_batch_reader(request).unwrap();
        let actual = record_batch_rows(&mut direct);
        (actual, direct.metrics())
    }

    fn assert_all_types_iterator_fallback(shard: &Arc<ReefsDb>, mut scan: StorageScan) {
        scan.projection = all_types_aux_storage_scan().projection;
        let request =
            crate::RecordBatchRequest::new(scan.clone(), all_types_arrow_schema(), 16, usize::MAX);
        let iterator = shard.create_storage_iterator(scan).unwrap();
        let mut forced = crate::RecordBatchReader::try_new(request.clone(), iterator).unwrap();
        let expected = record_batch_rows(&mut forced);
        let mut fallback = shard.create_record_batch_reader(request).unwrap();
        let actual = record_batch_rows(&mut fallback);
        assert_eq!(actual, expected);
        assert_eq!(fallback.metrics().multi_field_direct_rows, 0);
        assert_eq!(fallback.metrics().direct_batch_rows, 0);
        assert_eq!(
            fallback.metrics().iterator_fallback_rows,
            actual.len() as u64
        );
    }

    fn storage_column(name: &str, kind: ColumnKind) -> StorageValueExpr {
        StorageValueExpr::Column(ColumnRef::new(name, kind))
    }

    fn storage_literal(value: crate::models::Val) -> StorageValueExpr {
        StorageValueExpr::Literal(value)
    }

    // fn collect_string_points(
    //     mut itr: Box<dyn QueryIterator + Send + Sync>,
    // ) -> Vec<query_point::StringPoint> {
    //     let itr = itr.as_any_mut();
    //     let mut sitr = {
    //         if let Some(sitr) = itr.downcast_mut::<query_iterator::StringMergeIter>() {
    //             sitr as &mut dyn query_iterator::StringIterator
    //         } else if let Some(sitr) = itr.downcast_mut::<query_iterator::StringInterruptIter>() {
    //             sitr as &mut dyn query_iterator::StringIterator
    //         } else if let Some(sitr) = itr.downcast_mut::<query_iterator::StringLimitIter>() {
    //             sitr as &mut dyn query_iterator::StringIterator
    //         } else if let Some(sitr) = itr.downcast_mut::<query_iterator::StringSortedMergeIter>() {
    //             sitr as &mut dyn query_iterator::StringIterator
    //         } else if let Some(sitr) = itr.downcast_mut::<iterator::StringIter>() {
    //             sitr as &mut dyn query_iterator::StringIterator
    //         } else if let Some(sitr) = itr.downcast_mut::<super::StringPointsIterator>() {
    //             sitr as &mut dyn query_iterator::StringIterator
    //         } else {
    //             panic!("invalid iterator");
    //         }
    //     };

    //     let mut points = vec![];
    //     while let Some(p) = sitr.next() {
    //         points.push(p);
    //     }
    //     sitr.close();
    //     points
    // }

    fn assert_persisted_reefsdb_storage_scan() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let options = EngineOptions::default();
        let shard = new_test_reefsdb(1, tmp_dir.path(), Arc::clone(&sfile), options.clone());
        shard
            .write_points_string(&[
                "cpu,host=A value=1.0 10",
                "cpu,host=A value=2.0 20",
                "cpu,host=A value=3.0 30",
            ])
            .unwrap();
        let snapshot = shard.create_snapshot(false).unwrap();
        assert!(snapshot.exists());

        let persisted_root = tmp_dir.path().join("shard_1");
        assert!(
            std::fs::read_dir(&persisted_root)
                .unwrap()
                .filter_map(std::result::Result::ok)
                .any(|entry| entry
                    .path()
                    .extension()
                    .is_some_and(|extension| { extension == std::ffi::OsStr::new("tsm") })),
            "expected persisted TSM data under {}",
            persisted_root.display()
        );
        shard.close().unwrap();
        let query_shard = new_test_reefsdb(1, tmp_dir.path(), Arc::clone(&sfile), options);
        let mut scan = float_storage_scan("value");
        scan.projection.series =
            SeriesProjection::new(vec!["host".to_string()], vec!["host".to_string()]);
        scan.time_range = TimeRange::new(10, 20);
        scan.time_order = TimeOrder::Descending;
        let native_points =
            collect_float_points(query_shard.create_storage_iterator(scan).unwrap().unwrap());
        assert_eq!(
            native_points
                .iter()
                .map(|point| (point.time(), point.value()))
                .collect::<Vec<_>>(),
            vec![(20, 2.0), (10, 1.0)]
        );

        query_shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_reefsdb_storage_scan_reads_persisted_tsm_after_reopen() {
        assert_persisted_reefsdb_storage_scan();
    }

    #[test]
    fn test_filtered_projected_aggregate_selects_hosts_across_cache_snapshot_and_reopen() {
        use crate::{AggregateKind, AggregateNumber, AggregateRequest, LogicalOp};

        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let mut options = EngineOptions::default();
        options.vlog_enabled = true;
        options.agg_index_enabled = true;
        let shard = new_test_reefsdb(1, tmp_dir.path(), Arc::clone(&sfile), options.clone());
        shard
            .write_points_string(&[
                "cpu,host=A,region=east value=1i 10",
                "cpu,host=A,region=east value=3i 20",
                "cpu,host=A,region=east value=9000i 30",
                "cpu,host=A,region=west value=7i 10",
                "cpu,host=A,region=west value=8i 20",
                "cpu,host=A,region=west value=8000i 30",
                "cpu,host=B,region=east value=99999i 10",
                "cpu,host=C,region=east value=200000i 10",
            ])
            .unwrap();
        let request = AggregateRequest {
            measurement: MeasurementRef::new("cpu"),
            field: "value".into(),
            // The caller converts the SQL interval [10, 30) to this inclusive range.
            time_range: TimeRange::new(10, 29),
            aggregates: vec![AggregateKind::Count, AggregateKind::Sum, AggregateKind::Max],
        };
        let host = |name: &str| StoragePredicate::Compare {
            op: CompareOp::Equal,
            lhs: StorageValueExpr::Column(ColumnRef::new("host", ColumnKind::Tag)),
            rhs: StorageValueExpr::Literal(crate::models::Val::String(name.into())),
        };
        let verify = |shard: &Arc<ReefsDb>, count, sum, max| {
            let result = shard
                .aggregate_projected_scan_filtered(request.clone(), host("A"))
                .unwrap()
                .unwrap();
            assert_eq!(result.state.count, Some(count));
            assert_eq!(result.state.sum, Some(AggregateNumber::Integer(sum)));
            assert_eq!(result.state.max, Some(AggregateNumber::Integer(max)));
            assert_eq!(result.state.min, None);
        };
        verify(&shard, 4, 19, 8);
        shard.create_snapshot(false).unwrap();
        verify(&shard, 4, 19, 8);

        let selected_hosts = shard
            .aggregate_projected_scan_filtered(
                request.clone(),
                StoragePredicate::Logical {
                    op: LogicalOp::Or,
                    lhs: Box::new(host("A")),
                    rhs: Box::new(host("B")),
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(selected_hosts.state.count, Some(5));
        assert_eq!(selected_hosts.state.sum, Some(AggregateNumber::Integer(100018)));
        assert_eq!(selected_hosts.state.max, Some(AggregateNumber::Integer(99999)));

        let selected_region = shard
            .aggregate_projected_scan_filtered(
                request.clone(),
                StoragePredicate::Logical {
                    op: LogicalOp::And,
                    lhs: Box::new(host("A")),
                    rhs: Box::new(StoragePredicate::Compare {
                        op: CompareOp::Equal,
                        lhs: StorageValueExpr::Column(ColumnRef::new("region", ColumnKind::Tag)),
                        rhs: StorageValueExpr::Literal(crate::models::Val::String("west".into())),
                    }),
                },
            )
            .unwrap()
            .unwrap();
        assert_eq!(selected_region.state.count, Some(2));
        assert_eq!(selected_region.state.sum, Some(AggregateNumber::Integer(15)));
        assert_eq!(selected_region.state.max, Some(AggregateNumber::Integer(8)));

        let mut whole_range = request.clone();
        whole_range.time_range = TimeRange::all();
        let persisted = shard
            .acquire_operation()
            .unwrap()
            .aggregate_projected_scan_filtered(whole_range, host("A"))
            .unwrap()
            .unwrap();
        assert_eq!(persisted.state.count, Some(6));
        assert_eq!(persisted.state.max, Some(AggregateNumber::Integer(9000)));
        assert!(persisted.metrics.statistics_blocks > 0);
        assert_eq!(persisted.metrics.decoded_blocks, 0);

        let empty = shard
            .aggregate_projected_scan_filtered(request.clone(), host("missing"))
            .unwrap()
            .unwrap();
        assert_eq!(empty.state.count, Some(0));
        assert_eq!(empty.state.max, None);
        assert_eq!(empty.metrics.candidate_blocks, 0);
        let mut absent_field = request.clone();
        absent_field.field = "absent".into();
        assert!(
            shard
                .aggregate_projected_scan_filtered(absent_field, host("A"))
                .unwrap()
                .is_none()
        );

        shard
            .write_points_string(&["cpu,host=A,region=east value=11i 25"])
            .unwrap();
        verify(&shard, 5, 30, 11);
        shard.close().unwrap();
        let reopened = new_test_reefsdb(1, tmp_dir.path(), Arc::clone(&sfile), options);
        verify(&reopened, 5, 30, 11);
        reopened.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_filtered_window_aggregate_shares_series_and_preserves_visibility() {
        use crate::{AggregateKind, AggregateNumber, AggregateRequest};
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let mut options = EngineOptions::default();
        options.vlog_enabled = true;
        options.agg_index_enabled = true;
        let shard = new_test_reefsdb(1, tmp_dir.path(), Arc::clone(&sfile), options.clone());
        shard
            .write_points_string(&[
                "cpu,host=A,region=east value=1i,other=1.5 10",
                "cpu,host=A,region=east value=3i,other=3.5 20",
                "cpu,host=A,region=west value=7i 10",
                "cpu,host=A,region=west value=8i 20",
                "cpu,host=B,region=east value=99999i,other=99999.0 10",
            ])
            .unwrap();
        let windows = vec![TimeRange::new(10, 19), TimeRange::new(20, 29)];
        let requests = ["value", "other", "absent"]
            .map(|field| AggregateRequest {
                measurement: MeasurementRef::new("cpu"),
                field: field.into(),
                time_range: TimeRange::new(10, 29),
                aggregates: vec![AggregateKind::Count, AggregateKind::Sum, AggregateKind::Max],
            })
            .to_vec();
        let predicate = StoragePredicate::Compare {
            op: CompareOp::Equal,
            lhs: StorageValueExpr::Column(ColumnRef::new("host", ColumnKind::Tag)),
            rhs: StorageValueExpr::Literal(crate::models::Val::String("A".into())),
        };
        let verify = |shard: &Arc<ReefsDb>, second_sum, second_max| {
            #[cfg(feature = "slow-trace")]
            let profile = crate::common::slow_trace::QueryProfile::start().unwrap();
            let results = shard
                .acquire_operation()
                .unwrap()
                .aggregate_projected_scan_windows_filtered(
                    requests.clone(),
                    windows.clone(),
                    predicate.clone(),
                )
                .unwrap();
            #[cfg(feature = "slow-trace")]
            {
                let profile = profile.finish();
                assert_eq!(
                    profile
                        .stages
                        .iter()
                        .filter(|s| s.stage == "query_series_selection")
                        .map(|s| s.calls)
                        .sum::<u64>(),
                    1
                );
                assert!(
                    !profile
                        .stages
                        .iter()
                        .any(|s| s.stage == "query_tag_key_discovery")
                );
            }
            let integer = &results[0].as_ref().unwrap().states;
            assert_eq!(integer[0].count, Some(2));
            assert_eq!(integer[0].sum, Some(AggregateNumber::Integer(8)));
            assert_eq!(integer[0].max, Some(AggregateNumber::Integer(7)));
            assert_eq!(integer[1].count, Some(2));
            assert_eq!(integer[1].sum, Some(AggregateNumber::Integer(second_sum)));
            assert_eq!(integer[1].max, Some(AggregateNumber::Integer(second_max)));
            let float = &results[1].as_ref().unwrap().states;
            assert_eq!(float[0].count, Some(1));
            assert_eq!(float[0].max, Some(AggregateNumber::Float(1.5)));
            assert_eq!(float[1].max, Some(AggregateNumber::Float(3.5)));
            assert!(results[2].is_none());
        };
        verify(&shard, 11, 8);
        shard.create_snapshot(false).unwrap();
        verify(&shard, 11, 8);
        shard
            .write_points_string(&["cpu,host=A,region=east value=12i 20"])
            .unwrap();
        verify(&shard, 20, 12);
        for invalid in [vec![], vec![TimeRange::new(10, 20), TimeRange::new(20, 29)]] {
            assert!(
                shard
                    .aggregate_projected_scan_windows_filtered(
                        requests.clone(),
                        invalid,
                        predicate.clone()
                    )
                    .is_err()
            );
        }
        let mut mismatched = requests.clone();
        mismatched[1].measurement = MeasurementRef::new("other");
        assert!(
            shard
                .aggregate_projected_scan_windows_filtered(
                    mismatched,
                    windows.clone(),
                    predicate.clone()
                )
                .is_err()
        );
        shard.close().unwrap();
        let reopened = new_test_reefsdb(1, tmp_dir.path(), Arc::clone(&sfile), options);
        verify(&reopened, 20, 12);
        reopened.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn parallel_window_aggregate_preserves_numeric_results_after_snapshot() {
        use crate::{AggregateKind, AggregateNumber, AggregateRequest};
        use crate::runtime::execution::{DefaultTokioExecutor, DefaultTokioExecutorOptions, ExecutorRef, RuntimeExecutorExt};
        let tmp = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let mut options = EngineOptions::default();
        options.vlog_enabled = true;
        options.agg_index_enabled = true;
        options.agg_query.parallel_tasks = 3;
        options.agg_query.parallel_min_series = 2;
        let executor: ExecutorRef = Arc::new(DefaultTokioExecutor::new(DefaultTokioExecutorOptions::new(1, 1, 1, 1)).unwrap());
        options.executor = Arc::clone(&executor);
        let shard = new_test_reefsdb(1, tmp.path(), sfile, options);
        let lines: Vec<_> = (0..16).flat_map(|host| [0, 10, 20].map(move |time| format!("cpu,host=H{host} value={}i,other={}.5 {time}", host + time, host + time))).collect();
        shard.write_points_string(&lines.iter().map(String::as_str).collect::<Vec<_>>()).unwrap();
        shard.create_snapshot(false).unwrap();
        let windows = vec![TimeRange::new(0, 9), TimeRange::new(10, 29)];
        let requests = ["value", "other"].map(|field| AggregateRequest {
            measurement: MeasurementRef::new("cpu"), field: field.into(), time_range: TimeRange::new(0, 29),
            aggregates: vec![AggregateKind::Count, AggregateKind::Sum, AggregateKind::Min, AggregateKind::Max],
        }).to_vec();
        let query_shard = Arc::clone(&shard);
        let (sender, receiver) = std::sync::mpsc::channel();
        let task = executor.spawn_blocking(move || {
            let result = query_shard.aggregate_projected_scan_windows_filtered(requests, windows, StoragePredicate::Constant(true));
            let _ = sender.send(result);
        }).unwrap();
        let result = receiver.recv_timeout(std::time::Duration::from_secs(10)).expect("parallel query starved a single blocking worker").unwrap();
        futures::executor::block_on(task).unwrap();
        let integer = result[0].as_ref().unwrap();
        assert_eq!(integer.states[0].count, Some(16));
        assert_eq!(integer.states[0].sum, Some(AggregateNumber::Integer(120)));
        assert_eq!(integer.states[1].count, Some(32));
        assert_eq!(integer.states[1].sum, Some(AggregateNumber::Integer(720)));
        assert_eq!(integer.states[1].max, Some(AggregateNumber::Integer(35)));
        let floats = result[1].as_ref().unwrap();
        assert_eq!(floats.states[0].sum, Some(AggregateNumber::Float(128.0)));
        assert_eq!(floats.states[1].sum, Some(AggregateNumber::Float(736.0)));
        assert_eq!(floats.states[1].min, Some(AggregateNumber::Float(10.5)));
        shard.close().unwrap();
    }

    #[test]
    fn test_filtered_projected_aggregate_rejects_residual_row_predicates() {
        use crate::{AggregateKind, AggregateRequest, LogicalOp};

        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let mut options = EngineOptions::default();
        options.vlog_enabled = true;
        options.agg_index_enabled = true;
        let shard = new_test_reefsdb(1, tmp_dir.path(), Arc::clone(&sfile), options);
        shard
            .write_points_string(&["cpu,host=A value=1i 10", "cpu,host=B value=9i 20"])
            .unwrap();
        let request = AggregateRequest {
            measurement: MeasurementRef::new("cpu"),
            field: "value".into(),
            time_range: TimeRange::all(),
            aggregates: vec![AggregateKind::Max],
        };
        let field_filter = StoragePredicate::Compare {
            op: CompareOp::GreaterThan,
            lhs: StorageValueExpr::Column(ColumnRef::new("value", ColumnKind::AnyField)),
            rhs: StorageValueExpr::Literal(crate::models::Val::I64(2)),
        };
        let time_filter = StoragePredicate::Compare {
            op: CompareOp::GreaterThan,
            lhs: StorageValueExpr::Column(ColumnRef::new("time", ColumnKind::Time)),
            rhs: StorageValueExpr::Literal(crate::models::Val::I64(10)),
        };
        let mixed_filter = StoragePredicate::Logical {
            op: LogicalOp::Or,
            lhs: Box::new(StoragePredicate::Compare {
                op: CompareOp::Equal,
                lhs: StorageValueExpr::Column(ColumnRef::new("host", ColumnKind::Tag)),
                rhs: StorageValueExpr::Literal(crate::models::Val::String("A".into())),
            }),
            rhs: Box::new(field_filter.clone()),
        };
        for predicate in [field_filter, time_filter, mixed_filter] {
            let error = shard
                .aggregate_projected_scan_filtered(request.clone(), predicate.clone())
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("predicate fully resolved by series selection")
            );
            let error = shard
                .aggregate_projected_scan_windows_filtered(
                    vec![request.clone()], vec![TimeRange::all()], predicate,
                )
                .unwrap_err();
            assert!(error.to_string().contains("predicate fully resolved by series selection"));
        }
        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_filtered_projected_aggregate_declines_legacy_engine() {
        use crate::{AggregateKind, AggregateRequest};

        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let shard = new_test_reefsdb(
            1,
            tmp_dir.path(),
            Arc::clone(&sfile),
            EngineOptions::default(),
        );
        shard.write_points_string(&["cpu,host=A value=1i 10"]).unwrap();
        let request = AggregateRequest {
            measurement: MeasurementRef::new("cpu"),
            field: "value".into(),
            time_range: TimeRange::all(),
            aggregates: vec![AggregateKind::Max],
        };
        assert!(
            shard
                .aggregate_projected_scan_filtered(request, StoragePredicate::Constant(true))
                .unwrap()
                .is_none()
        );
        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_record_batch_reader_matches_iterator_for_tsm_and_cache_all_types() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let shard = new_test_reefsdb(
            1,
            tmp_dir.path(),
            Arc::clone(&sfile),
            EngineOptions::default(),
        );
        shard
            .write_points_string(&[
                "cpu,host=A float=1.5,integer=-2i,unsigned=3u,boolean=true,string=\"one\" 10",
                "cpu,host=B float=2.5,integer=4i,unsigned=5u,boolean=false,string=\"two\" 20",
            ])
            .unwrap();
        shard.create_snapshot(false).unwrap();
        shard
            .write_points_string(&[
                "cpu,host=A float=3.5,integer=6i,unsigned=7u,boolean=false,string=\"three\" 30",
                "cpu,host=B float=4.5,integer=8i,unsigned=9u,boolean=true,string=\"four\" 40",
            ])
            .unwrap();

        let scan = all_types_aux_storage_scan();
        let expected = iterator_rows(collect_float_points(
            shard
                .create_storage_iterator(scan.clone())
                .unwrap()
                .unwrap(),
        ));
        let request = crate::RecordBatchRequest::new(scan, all_types_arrow_schema(), 2, 256);
        let mut reader = shard.create_record_batch_reader(request).unwrap();
        let actual = record_batch_rows(&mut reader);

        assert_eq!(actual, expected);
        assert_eq!(actual.len(), 4);
        assert_eq!(reader.metrics().rows_emitted, 4);
        assert_eq!(reader.metrics().iterator_fallback_rows, 4);
        assert_eq!(reader.metrics().direct_batch_rows, 0);

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_record_batch_reader_multi_field_direct_sparse_mixed_types_and_empty_stream() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let shard = new_test_reefsdb(
            1,
            tmp_dir.path(),
            Arc::clone(&sfile),
            EngineOptions::default(),
        );
        shard
            .write_points_string(&[
                "cpu,host=A float=1.5,integer=10i,string=\"a\" 10",
                "cpu,host=A unsigned=20u,boolean=true 20",
                "cpu,host=A float=3.5,boolean=false,string=\"c\" 30",
                "cpu,host=A integer=40i,unsigned=41u 40",
                "cpu,host=A string=\"outside\" 100",
            ])
            .unwrap();
        shard.create_snapshot(false).unwrap();

        let (full_actual, full_expected, full_metrics) =
            all_types_direct_and_iterator_rows(&shard, TimeRange::all(), 2);
        assert_eq!(full_actual, full_expected);
        assert_eq!(full_metrics.iterator_fallback_rows, 0);
        assert_eq!(full_metrics.timestamp_only_blocks, 0);
        assert_eq!(
            full_metrics.value_decoded_blocks,
            full_metrics.direct_blocks
        );

        let (actual, metrics) = all_types_direct_rows(&shard, TimeRange::new(10, 40), 2);
        let expected = full_expected
            .iter()
            .filter(|(time, _, _)| (10..=40).contains(time))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        assert_eq!(actual.len(), 4);
        assert_eq!(
            actual.iter().map(|row| row.0).collect::<Vec<_>>(),
            vec![10, 20, 30, 40]
        );
        assert_eq!(actual[0].2[0], crate::models::Val::F64(1.5));
        assert_eq!(actual[0].2[2], crate::models::Val::Empty(None));
        assert_eq!(actual[0].2[4], crate::models::Val::String("a".to_string()));
        assert_eq!(actual[1].2[0], crate::models::Val::Empty(None));
        assert_eq!(actual[1].2[3], crate::models::Val::Bool(true));
        assert_eq!(actual[2].2[1], crate::models::Val::Empty(None));
        assert_eq!(actual[3].2[4], crate::models::Val::Empty(None));
        assert_eq!(metrics.direct_batch_rows, 4);
        assert_eq!(metrics.multi_field_direct_rows, 4);
        assert_eq!(metrics.multi_field_direct_batches, 2);
        assert_eq!(metrics.multi_field_streams, 5);
        assert_eq!(metrics.alignment_steps, 4);
        assert_eq!(metrics.null_values_emitted, 10);
        assert_eq!(metrics.iterator_fallback_rows, 0);

        let (actual, expected, metrics) =
            all_types_direct_and_iterator_rows(&shard, TimeRange::new(20, 20), 16);
        assert_eq!(actual, expected);
        assert_eq!(actual.len(), 1);
        assert!(
            actual
                .iter()
                .all(|row| row.2[4] == crate::models::Val::Empty(None))
        );
        assert_eq!(metrics.multi_field_direct_rows, 1);
        assert_eq!(metrics.iterator_fallback_rows, 0);

        let mut empty_scan = all_types_aux_storage_scan();
        empty_scan.time_range = TimeRange::new(200, 300);
        let empty_request =
            crate::RecordBatchRequest::new(empty_scan, all_types_arrow_schema(), 16, usize::MAX);
        let mut empty_reader = shard.create_record_batch_reader(empty_request).unwrap();
        let actual = record_batch_rows(&mut empty_reader);
        let metrics = empty_reader.metrics();
        assert!(actual.is_empty());
        assert_eq!(metrics.multi_field_streams, 5);
        assert_eq!(metrics.multi_field_direct_rows, 0);
        assert_eq!(metrics.batches_emitted, 0);

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_legacy_iterator_string_sparse_partial_range_boundaries() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let shard = new_test_reefsdb(
            1,
            tmp_dir.path(),
            Arc::clone(&sfile),
            EngineOptions::default(),
        );
        shard
            .write_points_string(&[
                "cpu,host=A string=\"one\" 1",
                "cpu,host=A string=\"three\" 3",
                "cpu,host=A string=\"seven\" 7",
                "cpu,host=A string=\"ten\" 10",
            ])
            .unwrap();
        shard.create_snapshot(false).unwrap();

        let cases = [
            (TimeRange::new(1, 1), vec![1]),
            (TimeRange::new(2, 7), vec![3, 7]),
            (TimeRange::new(3, 6), vec![3]),
            (TimeRange::new(10, 10), vec![10]),
            (TimeRange::new(0, 1), vec![1]),
            (TimeRange::new(0, 2), vec![1]),
            (TimeRange::new(0, 10), vec![1, 3, 7, 10]),
            (TimeRange::new(-10, 0), vec![]),
            (TimeRange::new(11, 20), vec![]),
            (TimeRange::new(-10, 20), vec![1, 3, 7, 10]),
            (TimeRange::new(5, 5), vec![]),
            (TimeRange::new(8, 20), vec![10]),
        ];

        for (range, expected) in cases {
            let ascending = legacy_single_field_rows(
                &shard,
                "string",
                crate::models::STRING,
                range,
                TimeOrder::Ascending,
            )
            .into_iter()
            .map(|row| row.0)
            .collect::<Vec<_>>();
            assert_eq!(ascending, expected, "ascending range {range:?}");

            let descending = legacy_single_field_rows(
                &shard,
                "string",
                crate::models::STRING,
                range,
                TimeOrder::Descending,
            )
            .into_iter()
            .map(|row| row.0)
            .collect::<Vec<_>>();
            assert_eq!(
                descending,
                expected.into_iter().rev().collect::<Vec<_>>(),
                "descending range {range:?}"
            );
        }

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_record_batch_reader_multi_field_direct_includes_row_domain_only_timestamps() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let shard = new_test_reefsdb(
            1,
            tmp_dir.path(),
            Arc::clone(&sfile),
            EngineOptions::default(),
        );
        shard
            .write_points_string(&[
                "cpu,host=A a=1i,b=1.0,d=10u 1",
                "cpu,host=A a=2i,d=20u 2",
                "cpu,host=A b=3.0 3",
                "cpu,host=A c=\"domain-only\" 4",
            ])
            .unwrap();
        shard.create_snapshot(false).unwrap();

        let scan = StorageScan {
            measurement: MeasurementRef::new("cpu"),
            projection: StorageProjection {
                primary_column: None,
                returned_columns: vec![
                    ColumnRef::new("a", ColumnKind::Field(Some(crate::models::INTEGER))),
                    ColumnRef::new("b", ColumnKind::Field(Some(crate::models::FLOAT))),
                ],
                row_domain_columns: vec![
                    ColumnRef::new("c", ColumnKind::Field(Some(crate::models::STRING))),
                    ColumnRef::new("d", ColumnKind::Field(Some(crate::models::UNSIGNED))),
                ],
                series: SeriesProjection::new(vec!["host".to_string()], vec!["host".to_string()]),
            },
            time_range: TimeRange::all(),
            predicate: None,
            time_order: TimeOrder::Ascending,
            output_order: OutputOrder::TimeOrdered,
            execution: Default::default(),
            cancellation: None,
        };
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                crate::query::metadata::TIME_COLUMN_NAME,
                crate::query::metadata::time_arrow_type(),
                false,
            ),
            ArrowField::new("host", ArrowType::Utf8, true),
            ArrowField::new("a", ArrowType::Int64, true),
            ArrowField::new("b", ArrowType::Float64, true),
        ]));
        let request = crate::RecordBatchRequest::new(scan.clone(), schema, 8, usize::MAX);
        let iterator = shard.create_storage_iterator(scan).unwrap();
        let expected = crate::RecordBatchReader::try_new(request.clone(), iterator)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let mut direct = shard.create_record_batch_reader(request.clone()).unwrap();
        let actual = direct.by_ref().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(actual, expected);

        let lease = shard.acquire_operation().unwrap();
        let mut forced_full = lease
            .create_record_batch_reader_with_full_row_domain_values(request)
            .unwrap();
        let forced_actual = forced_full.by_ref().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(forced_actual, actual);
        let forced_metrics = forced_full.metrics();
        drop(forced_full);
        drop(lease);

        let times = actual
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name(crate::query::metadata::TIME_COLUMN_NAME)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(times, vec![1, 2, 3, 4]);
        let batch = &actual[0];
        assert!(batch.column_by_name("a").unwrap().is_null(3));
        assert!(batch.column_by_name("b").unwrap().is_null(3));
        let metrics = direct.metrics();
        assert_eq!(metrics.multi_field_streams, 4);
        assert_eq!(metrics.multi_field_direct_rows, 4);
        assert_eq!(metrics.iterator_fallback_rows, 0);
        assert_eq!(metrics.value_decoded_blocks, 2);
        assert_eq!(metrics.timestamp_only_blocks, 2);
        assert_eq!(metrics.value_decoded_points, 4);
        assert_eq!(metrics.timestamp_only_points, 3);
        assert_eq!(
            metrics.direct_blocks,
            metrics.value_decoded_blocks + metrics.timestamp_only_blocks
        );
        assert_eq!(forced_metrics.timestamp_only_blocks, 0);
        assert_eq!(forced_metrics.value_decoded_blocks, 4);

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_record_batch_reader_multi_series_direct_preserves_sparse_row_domain_and_order() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let shard = new_test_reefsdb(
            1,
            tmp_dir.path(),
            Arc::clone(&sfile),
            EngineOptions::default(),
        );
        shard
            .write_points_string(&[
                "cpu,host=A float=1.0 1",
                "cpu,host=A integer=2i 2",
                "cpu,host=A string=\"domain-a\" 3",
                "cpu,host=B boolean=true 1",
                "cpu,host=B float=4.0,integer=40i 4",
                "cpu,host=B unsigned=5u 5",
            ])
            .unwrap();
        shard.create_snapshot(false).unwrap();

        let mut scan = selected_types_row_domain_scan(TimeRange::new(1, 5));
        scan.projection.series.identity_tag_keys.clear();
        let scan_set = shard
            .create_direct_tsm_scans_unleased(&scan)
            .unwrap()
            .expect("eligible multi-physical-series scan");
        assert_eq!(scan_set.tag_sets.len(), 1);
        assert_eq!(scan_set.tag_sets[0].series.len(), 2);
        drop(scan_set);

        let (actual, expected, metrics) =
            selected_types_direct_and_iterator_batches_for_scan(&shard, scan, 1);
        assert_eq!(actual, expected);
        let hosts_and_times = actual
            .iter()
            .map(|batch| {
                let time = batch
                    .column_by_name(crate::query::metadata::TIME_COLUMN_NAME)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .value(0);
                let host = batch
                    .column_by_name("host")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0)
                    .to_string();
                (host, time)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            hosts_and_times,
            vec![
                ("A".to_string(), 1),
                ("A".to_string(), 2),
                ("A".to_string(), 3),
                ("B".to_string(), 1),
                ("B".to_string(), 4),
                ("B".to_string(), 5),
            ]
        );
        assert_eq!(metrics.iterator_fallback_rows, 0);
        assert_eq!(metrics.direct_batch_rows, 6);
        assert_eq!(metrics.multi_field_direct_rows, 6);
        assert_eq!(metrics.multi_field_streams, 10);
        assert_eq!(metrics.value_decoded_blocks, 4);
        assert_eq!(metrics.timestamp_only_blocks, 3);
        assert_eq!(metrics.value_decoded_points, 4);
        assert_eq!(metrics.timestamp_only_points, 3);

        let mut single_scan = selected_types_row_domain_scan(TimeRange::new(1, 5));
        single_scan.projection.series.identity_tag_keys.clear();
        let integer = single_scan.projection.returned_columns.pop().unwrap();
        single_scan.projection.row_domain_columns.insert(0, integer);
        let single_request = crate::RecordBatchRequest::new(
            single_scan.clone(),
            single_field_schema("float", crate::models::FLOAT),
            2,
            usize::MAX,
        );
        let iterator = shard.create_storage_iterator(single_scan).unwrap();
        let mut single_fallback =
            crate::RecordBatchReader::try_new(single_request.clone(), iterator).unwrap();
        let single_expected =
            single_field_batch_rows(&mut single_fallback, "float", crate::models::FLOAT);
        let mut single_direct = shard.create_record_batch_reader(single_request).unwrap();
        let single_actual =
            single_field_batch_rows(&mut single_direct, "float", crate::models::FLOAT);
        assert_eq!(single_actual, single_expected);
        assert_eq!(single_direct.metrics().iterator_fallback_rows, 0);
        assert_eq!(single_direct.metrics().direct_batch_rows, 6);
        assert_eq!(single_direct.metrics().value_decoded_points, 2);
        assert_eq!(single_direct.metrics().timestamp_only_points, 5);

        let (all_actual, all_expected, all_metrics) =
            all_types_direct_and_iterator_rows(&shard, TimeRange::new(1, 5), 1);
        assert_eq!(all_actual, all_expected);
        assert_eq!(all_metrics.iterator_fallback_rows, 0);
        assert_eq!(all_metrics.direct_batch_rows, 6);
        assert_eq!(all_metrics.timestamp_only_blocks, 0);
        assert_eq!(all_metrics.value_decoded_blocks, 7);

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_record_batch_reader_direct_merges_equal_timestamps_within_one_tag_set() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let shard = new_test_reefsdb(
            1,
            tmp_dir.path(),
            Arc::clone(&sfile),
            EngineOptions::default(),
        );
        shard
            .write_points_string(&[
                "cpu,host=A value=11.0 1",
                "cpu,host=A value=12.0 2",
                "cpu,host=A value=14.0 4",
                "cpu,host=B value=21.0 1",
                "cpu,host=B value=23.0 3",
                "cpu,host=B value=24.0 4",
                "cpu,host=C value=31.0 1",
                "cpu,host=C value=34.0 4",
            ])
            .unwrap();
        shard.create_snapshot(false).unwrap();

        let mut scan = single_field_aux_scan("value", crate::models::FLOAT, TimeRange::all());
        scan.projection.series.identity_tag_keys.clear();
        scan.projection.series.output_tag_keys.clear();
        let scan_set = shard
            .create_direct_tsm_scans_unleased(&scan)
            .unwrap()
            .expect("eligible same-tag-set direct scan");
        assert_eq!(scan_set.tag_sets.len(), 1);
        assert_eq!(scan_set.tag_sets[0].series.len(), 3);
        drop(scan_set);

        let request = crate::RecordBatchRequest::new(
            scan.clone(),
            Arc::new(ArrowSchema::new(vec![
                ArrowField::new(
                    crate::query::metadata::TIME_COLUMN_NAME,
                    crate::query::metadata::time_arrow_type(),
                    false,
                ),
                ArrowField::new("value", ArrowType::Float64, true),
            ])),
            1,
            usize::MAX,
        );
        let iterator = shard.create_storage_iterator(scan).unwrap();
        let expected = crate::RecordBatchReader::try_new(request.clone(), iterator)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let mut direct = shard.create_record_batch_reader(request).unwrap();
        let actual = direct.by_ref().collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(actual, expected);
        let times = actual
            .iter()
            .map(|batch| {
                batch
                    .column_by_name(crate::query::metadata::TIME_COLUMN_NAME)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .value(0)
            })
            .collect::<Vec<_>>();
        assert_eq!(times, vec![1, 1, 1, 2, 3, 4, 4, 4]);
        assert_eq!(times.iter().filter(|time| **time == 1).count(), 3);
        assert_eq!(times.iter().filter(|time| **time == 4).count(), 3);
        assert_eq!(direct.metrics().direct_batch_rows, 8);
        assert_eq!(direct.metrics().iterator_fallback_rows, 0);

        let mut outer_scan = single_field_aux_scan("value", crate::models::FLOAT, TimeRange::all());
        outer_scan.projection.series.output_tag_keys.clear();
        let outer_scan_set = shard
            .create_direct_tsm_scans_unleased(&outer_scan)
            .unwrap()
            .expect("eligible multi-tag-set direct scan");
        assert_eq!(outer_scan_set.tag_sets.len(), 3);
        assert!(
            outer_scan_set
                .tag_sets
                .iter()
                .all(|tag_set| tag_set.series.len() == 1)
        );
        drop(outer_scan_set);
        let outer_request = crate::RecordBatchRequest::new(
            outer_scan.clone(),
            Arc::new(ArrowSchema::new(vec![
                ArrowField::new(
                    crate::query::metadata::TIME_COLUMN_NAME,
                    crate::query::metadata::time_arrow_type(),
                    false,
                ),
                ArrowField::new("value", ArrowType::Float64, true),
            ])),
            1,
            usize::MAX,
        );
        let iterator = shard.create_storage_iterator(outer_scan).unwrap();
        let outer_expected = crate::RecordBatchReader::try_new(outer_request.clone(), iterator)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let outer_actual = shard
            .create_record_batch_reader(outer_request)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(outer_actual, outer_expected);

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_record_batch_reader_direct_preserves_nested_tag_set_merge_order() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let shard = new_test_reefsdb(
            1,
            tmp_dir.path(),
            Arc::clone(&sfile),
            EngineOptions::default(),
        );
        shard
            .write_points_string(&[
                "cpu,region=east,host=A value=11.0 1",
                "cpu,region=east,host=A value=14.0 4",
                "cpu,region=east,host=B value=22.0 2",
                "cpu,region=east,host=B value=25.0 5",
                "cpu,region=west,host=C value=31.0 1",
                "cpu,region=west,host=C value=33.0 3",
                "cpu,region=west,host=D value=42.0 2",
                "cpu,region=west,host=D value=44.0 4",
            ])
            .unwrap();
        shard.create_snapshot(false).unwrap();

        let mut scan = single_field_aux_scan("value", crate::models::FLOAT, TimeRange::all());
        scan.projection.series.identity_tag_keys = vec!["region".to_string()];
        let scan_set = shard
            .create_direct_tsm_scans_unleased(&scan)
            .unwrap()
            .expect("eligible nested tag-set direct scan");
        assert_eq!(scan_set.tag_sets.len(), 2);
        assert!(
            scan_set
                .tag_sets
                .iter()
                .all(|tag_set| tag_set.series.len() == 2)
        );
        drop(scan_set);

        let request = crate::RecordBatchRequest::new(
            scan.clone(),
            single_field_schema("value", crate::models::FLOAT),
            1,
            usize::MAX,
        );
        let iterator = shard.create_storage_iterator(scan).unwrap();
        let mut generic = crate::RecordBatchReader::try_new(request.clone(), iterator).unwrap();
        let expected = single_field_batch_rows(&mut generic, "value", crate::models::FLOAT);
        let mut direct = shard.create_record_batch_reader(request).unwrap();
        let actual = single_field_batch_rows(&mut direct, "value", crate::models::FLOAT);

        assert_eq!(actual, expected);
        assert_eq!(actual.len(), 8);
        assert_eq!(direct.metrics().direct_batch_rows, 8);
        assert_eq!(direct.metrics().iterator_fallback_rows, 0);

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_record_batch_reader_r2_r4_shapes_use_one_tag_set_direct_merge() {
        for physical_series in [8_usize, 32] {
            let tmp_dir = must_temp_reefsdb_dir();
            let sfile = Arc::new(must_open_series_file().0);
            let shard = new_test_reefsdb(
                1,
                tmp_dir.path(),
                Arc::clone(&sfile),
                EngineOptions::default(),
            );
            let lines = (0..physical_series)
                .flat_map(|series| {
                    [
                        format!("cpu,host=H{series:02} value={}.0 1", series * 10 + 1),
                        format!("cpu,host=H{series:02} domain={}.0 2", series * 10 + 2),
                        format!("cpu,host=H{series:02} value={}.0 3", series * 10 + 3),
                    ]
                })
                .collect::<Vec<_>>();
            let refs = lines.iter().map(String::as_str).collect::<Vec<_>>();
            shard.write_points_string(&refs).unwrap();
            shard.create_snapshot(false).unwrap();

            let mut scan = single_field_aux_scan("value", crate::models::FLOAT, TimeRange::all());
            scan.projection.series.identity_tag_keys.clear();
            scan.projection.series.output_tag_keys.clear();
            scan.projection.row_domain_columns.push(ColumnRef::new(
                "domain",
                ColumnKind::Field(Some(crate::models::FLOAT)),
            ));
            let scan_set = shard
                .create_direct_tsm_scans_unleased(&scan)
                .unwrap()
                .expect("eligible R2/R4-shaped direct scan");
            assert_eq!(scan_set.tag_sets.len(), 1);
            assert_eq!(scan_set.tag_sets[0].series.len(), physical_series);
            drop(scan_set);

            let request = crate::RecordBatchRequest::new(
                scan.clone(),
                Arc::new(ArrowSchema::new(vec![
                    ArrowField::new(
                        crate::query::metadata::TIME_COLUMN_NAME,
                        crate::query::metadata::time_arrow_type(),
                        false,
                    ),
                    ArrowField::new("value", ArrowType::Float64, true),
                ])),
                7,
                usize::MAX,
            );
            let iterator = shard.create_storage_iterator(scan).unwrap();
            let expected = crate::RecordBatchReader::try_new(request.clone(), iterator)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let mut direct = shard.create_record_batch_reader(request).unwrap();
            let actual = direct.by_ref().collect::<Result<Vec<_>, _>>().unwrap();
            let metrics = direct.metrics();

            assert_eq!(actual, expected, "physical_series={physical_series}");
            assert_eq!(metrics.direct_batch_rows, (physical_series * 3) as u64);
            assert_eq!(metrics.iterator_fallback_rows, 0);
            assert_eq!(metrics.value_decoded_points, (physical_series * 2) as u64);
            assert_eq!(metrics.timestamp_only_points, physical_series as u64);
            assert_eq!(
                metrics.direct_blocks,
                metrics.value_decoded_blocks + metrics.timestamp_only_blocks
            );

            shard.close().unwrap();
            sfile.close().unwrap();
        }
    }

    #[test]
    fn test_record_batch_reader_r3_r5_shapes_use_multi_field_direct_merge() {
        for (physical_series, projected_fields) in [(8_usize, 5_usize), (32, 10)] {
            let tmp_dir = must_temp_reefsdb_dir();
            let sfile = Arc::new(must_open_series_file().0);
            let shard = new_test_reefsdb(
                1,
                tmp_dir.path(),
                Arc::clone(&sfile),
                EngineOptions::default(),
            );
            let lines = (0..physical_series)
                .flat_map(|series| {
                    [1_i64, 3].map(move |time| {
                        let fields = (0..10)
                            .map(|field| {
                                format!(
                                    "f{field}={}.0",
                                    series * 1_000 + field * 10 + time as usize
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(",");
                        format!("cpu,host=H{series:02} {fields} {time}")
                    })
                })
                .collect::<Vec<_>>();
            let refs = lines.iter().map(String::as_str).collect::<Vec<_>>();
            shard.write_points_string(&refs).unwrap();
            shard.create_snapshot(false).unwrap();

            let returned_columns = (0..projected_fields)
                .map(|field| {
                    ColumnRef::new(
                        format!("f{field}"),
                        ColumnKind::Field(Some(crate::models::FLOAT)),
                    )
                })
                .collect::<Vec<_>>();
            let scan = StorageScan {
                measurement: MeasurementRef::new("cpu"),
                projection: StorageProjection {
                    primary_column: None,
                    returned_columns,
                    row_domain_columns: vec![],
                    series: SeriesProjection::new(vec![], vec!["host".to_string()]),
                },
                time_range: TimeRange::all(),
                predicate: None,
                time_order: TimeOrder::Ascending,
                output_order: OutputOrder::TimeOrdered,
                execution: Default::default(),
                cancellation: None,
            };
            let mut schema_fields = vec![
                ArrowField::new(
                    crate::query::metadata::TIME_COLUMN_NAME,
                    crate::query::metadata::time_arrow_type(),
                    false,
                ),
                ArrowField::new("host", ArrowType::Utf8, true),
            ];
            schema_fields.extend(
                (0..projected_fields)
                    .map(|field| ArrowField::new(format!("f{field}"), ArrowType::Float64, true)),
            );
            let schema = Arc::new(ArrowSchema::new(schema_fields));

            let scan_set = shard
                .create_direct_tsm_scans_unleased(&scan)
                .unwrap()
                .expect("eligible R3/R5-shaped direct scan");
            assert_eq!(scan_set.tag_sets.len(), 1);
            assert_eq!(scan_set.tag_sets[0].series.len(), physical_series);
            drop(scan_set);

            let request = crate::RecordBatchRequest::new(scan.clone(), schema, 11, usize::MAX);
            let iterator = shard.create_storage_iterator(scan).unwrap();
            let expected = crate::RecordBatchReader::try_new(request.clone(), iterator)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let mut direct = shard.create_record_batch_reader(request).unwrap();
            let actual = direct.by_ref().collect::<Result<Vec<_>, _>>().unwrap();
            let metrics = direct.metrics();

            assert_eq!(actual, expected);
            assert_eq!(metrics.direct_batch_rows, (physical_series * 2) as u64);
            assert_eq!(
                metrics.multi_field_direct_rows,
                (physical_series * 2) as u64
            );
            assert_eq!(metrics.iterator_fallback_rows, 0);
            assert_eq!(
                metrics.value_decoded_points,
                (physical_series * projected_fields * 2) as u64
            );
            assert_eq!(metrics.timestamp_only_points, 0);

            shard.close().unwrap();
            sfile.close().unwrap();
        }
    }

    #[cfg(feature = "slow-trace")]
    #[test]
    fn test_record_batch_reader_multi_series_direct_reports_physical_work() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let shard = new_test_reefsdb(
            1,
            tmp_dir.path(),
            Arc::clone(&sfile),
            EngineOptions::default(),
        );
        shard
            .write_points_string(&[
                "cpu,host=A float=1.0 1",
                "cpu,host=A integer=2i 2",
                "cpu,host=A string=\"domain-a\" 3",
                "cpu,host=B boolean=true 1",
                "cpu,host=B float=4.0,integer=40i 4",
                "cpu,host=B unsigned=5u 5",
            ])
            .unwrap();
        shard.create_snapshot(false).unwrap();

        let profile = Arc::new(crate::query::storage_ir::StorageScanProfile::default());
        let mut scan = selected_types_row_domain_scan(TimeRange::new(1, 5));
        scan.projection.series.identity_tag_keys.clear();
        scan.execution.profile = Some(Arc::clone(&profile));
        let request =
            crate::RecordBatchRequest::new(scan, selected_types_arrow_schema(), 1, usize::MAX);
        let rows = shard
            .create_record_batch_reader(request)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .into_iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>();
        assert_eq!(rows, 6);

        let metrics = profile.raw_snapshot();
        assert_eq!(metrics.selected_series, 2);
        assert_eq!(metrics.selected_tag_sets, 1);
        assert_eq!(metrics.physical_series_keys_total, 2);
        assert_eq!(metrics.physical_series_keys_per_tagset_min, 2);
        assert_eq!(metrics.physical_series_keys_per_tagset_max, 2);
        assert_eq!(metrics.projected_field_count, 2);
        assert_eq!(metrics.row_domain_field_count, 3);
        assert_eq!(metrics.direct_scan_series_count, 2);
        assert_eq!(metrics.direct_physical_series_count, 2);
        assert_eq!(metrics.generic_fallback_series_count, 0);
        assert_eq!(metrics.generic_fallback_physical_series_count, 0);
        assert_eq!(metrics.field_value_streams, 4);
        assert_eq!(metrics.timestamp_only_streams, 6);
        assert_eq!(metrics.full_value_blocks_decoded, 4);
        assert_eq!(metrics.timestamp_only_blocks_decoded, 3);
        assert_eq!(metrics.full_value_points_decoded, 4);
        assert_eq!(metrics.timestamp_only_points_decoded, 3);
        assert_eq!(metrics.direct_merge_input_series, 2);
        assert_eq!(metrics.direct_merge_rows, 6);

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_record_batch_reader_row_domain_only_streams_supply_time_and_tags() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let shard = new_test_reefsdb(
            1,
            tmp_dir.path(),
            Arc::clone(&sfile),
            EngineOptions::default(),
        );
        shard
            .write_points_string(&[
                "cpu,host=A float=1.0 1",
                "cpu,host=A integer=2i 2",
                "cpu,host=A string=\"three\" 3",
                "cpu,host=A boolean=true 4",
            ])
            .unwrap();
        shard.create_snapshot(false).unwrap();

        let mut scan = selected_types_row_domain_scan(TimeRange::all());
        scan.projection
            .row_domain_columns
            .splice(0..0, scan.projection.returned_columns.drain(..));
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(
                crate::query::metadata::TIME_COLUMN_NAME,
                crate::query::metadata::time_arrow_type(),
                false,
            ),
            ArrowField::new("host", ArrowType::Utf8, true),
        ]));
        let request = crate::RecordBatchRequest::new(scan.clone(), schema, 2, usize::MAX);
        let iterator = shard.create_storage_iterator(scan).unwrap();
        let expected = crate::RecordBatchReader::try_new(request.clone(), iterator)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let mut direct = shard.create_record_batch_reader(request).unwrap();
        let actual = direct.by_ref().collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(actual, expected);
        let times = actual
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name(crate::query::metadata::TIME_COLUMN_NAME)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(times, vec![1, 2, 3, 4]);
        for batch in &actual {
            let host = batch
                .column_by_name("host")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert!((0..host.len()).all(|row| host.value(row) == "A"));
        }
        let metrics = direct.metrics();
        assert_eq!(metrics.value_decoded_blocks, 0);
        assert!(metrics.timestamp_only_blocks > 0);
        assert_eq!(metrics.direct_blocks, metrics.timestamp_only_blocks);
        assert_eq!(metrics.iterator_fallback_rows, 0);

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_record_batch_reader_timestamp_only_streams_preserve_partial_ranges() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let shard = new_test_reefsdb(
            1,
            tmp_dir.path(),
            Arc::clone(&sfile),
            EngineOptions::default(),
        );
        shard
            .write_points_string(&[
                "cpu,host=A float=1.0,integer=1i 1",
                "cpu,host=A string=\"two\" 2",
                "cpu,host=A string=\"four\" 4",
                "cpu,host=A float=5.0 5",
                "cpu,host=A string=\"six\" 6",
                "cpu,host=A string=\"eight\" 8",
                "cpu,host=A float=9.0 9",
            ])
            .unwrap();
        shard.create_snapshot(false).unwrap();

        for (range, expected_rows) in [
            (TimeRange::new(3, 7), 3_usize),
            (TimeRange::new(2, 2), 1),
            (TimeRange::new(10, 11), 0),
        ] {
            let (actual, expected, metrics) =
                selected_types_direct_and_iterator_batches(&shard, range, 16);
            assert_eq!(actual, expected, "range {range:?}");
            assert_eq!(
                actual.iter().map(RecordBatch::num_rows).sum::<usize>(),
                expected_rows
            );
            assert_eq!(metrics.iterator_fallback_rows, 0);
            if expected_rows == 0 {
                assert_eq!(metrics.direct_blocks, 0);
            } else {
                assert!(metrics.partial_direct_blocks > 0);
                assert!(metrics.timestamp_only_blocks > 0);
                assert_eq!(
                    metrics.direct_blocks,
                    metrics.value_decoded_blocks + metrics.timestamp_only_blocks
                );
            }
        }

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_record_batch_reader_multi_field_direct_crosses_different_block_boundaries() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let shard = new_test_reefsdb(
            1,
            tmp_dir.path(),
            Arc::clone(&sfile),
            EngineOptions::default(),
        );
        let block_rows = crate::config::DEFAULT_MAX_POINTS_PER_BLOCK;
        let total_rows = block_rows + block_rows / 2 + 101;
        let lines = (0..total_rows)
            .map(|time| {
                let mut fields = vec![format!("float={}.5", time)];
                if time % 4 != 1 {
                    fields.push(format!("integer={time}i"));
                }
                if time % 3 != 1 {
                    fields.push(format!("unsigned={}u", time as u64));
                }
                if time % 5 == 0 {
                    fields.push(format!("boolean={}", time % 10 == 0));
                }
                if time % 7 == 0 {
                    fields.push(format!("string=\"value_{time}\""));
                }
                format!("cpu,host=A {} {time}", fields.join(","))
            })
            .collect::<Vec<_>>();
        let refs = lines.iter().map(String::as_str).collect::<Vec<_>>();
        shard.write_points_string(&refs).unwrap();
        shard.create_snapshot(false).unwrap();

        let (actual, expected, metrics) =
            selected_types_direct_and_iterator_batches(&shard, TimeRange::all(), 257);
        assert_eq!(actual, expected);
        assert_eq!(
            actual.iter().map(RecordBatch::num_rows).sum::<usize>(),
            total_rows as usize
        );
        assert!(metrics.direct_blocks > metrics.multi_field_streams);
        assert!(metrics.value_decoded_blocks > 0);
        assert!(metrics.timestamp_only_blocks > 0);
        assert_eq!(
            metrics.direct_blocks,
            metrics.value_decoded_blocks + metrics.timestamp_only_blocks
        );
        assert_eq!(metrics.multi_field_direct_rows, total_rows as u64);
        assert_eq!(metrics.iterator_fallback_rows, 0);

        let range = TimeRange::new(37, total_rows - 29);
        let (actual, expected, metrics) =
            selected_types_direct_and_iterator_batches(&shard, range, 257);
        assert_eq!(actual, expected);
        assert_eq!(
            actual.iter().map(RecordBatch::num_rows).sum::<usize>(),
            (range.end_time - range.start_time + 1) as usize
        );
        assert!(metrics.direct_blocks > metrics.multi_field_streams);
        assert!(metrics.partial_direct_blocks >= 2);
        assert!(metrics.timestamp_only_blocks > 0);
        assert!(metrics.timestamp_only_points > 0);
        assert_eq!(
            metrics.multi_field_direct_rows,
            (range.end_time - range.start_time + 1) as u64
        );
        assert_eq!(metrics.iterator_fallback_rows, 0);

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_record_batch_reader_multi_field_direct_multiple_non_overlapping_files() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let mut options = EngineOptions::default();
        options.compaction_disabled = true;
        let shard = new_test_reefsdb(1, tmp_dir.path(), Arc::clone(&sfile), options);
        shard
            .write_points_string(&[
                "cpu,host=A float=1.0,integer=1i 10",
                "cpu,host=A unsigned=2u,boolean=true,string=\"a\" 20",
            ])
            .unwrap();
        shard.create_snapshot(false).unwrap();
        shard
            .write_points_string(&[
                "cpu,host=A float=3.0,unsigned=3u 30",
                "cpu,host=A integer=4i,boolean=false,string=\"b\" 40",
            ])
            .unwrap();
        shard.create_snapshot(false).unwrap();

        let (actual, expected, metrics) =
            selected_types_direct_and_iterator_batches(&shard, TimeRange::all(), 3);
        assert_eq!(actual, expected);
        assert_eq!(actual.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
        assert_eq!(metrics.multi_field_direct_rows, 4);
        assert_eq!(metrics.iterator_fallback_rows, 0);
        assert!(metrics.direct_blocks >= 8);
        assert!(metrics.timestamp_only_blocks > 0);

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_record_batch_reader_multi_field_direct_randomized_differential() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let shard = new_test_reefsdb(
            1,
            tmp_dir.path(),
            Arc::clone(&sfile),
            EngineOptions::default(),
        );
        let mut rng = StdRng::seed_from_u64(0x4d55_4c54_4946_4945);
        const ROWS: i64 = 193;
        let lines = (0..ROWS)
            .map(|time| {
                let mut fields = vec![format!("float={}.25", rng.gen_range(-10_000..10_000))];
                if rng.gen_bool(0.55) {
                    fields.push(format!("integer={}i", rng.gen_range(-50_000_i64..50_000)));
                }
                if rng.gen_bool(0.45) {
                    fields.push(format!("unsigned={}u", rng.gen_range(0_u64..100_000)));
                }
                if rng.gen_bool(0.35) {
                    fields.push(format!("boolean={}", rng.gen_bool(0.5)));
                }
                if rng.gen_bool(0.25) {
                    fields.push(format!("string=\"s{}\"", rng.gen_range(0_u8..32)));
                }
                format!("cpu,host=A {} {time}", fields.join(","))
            })
            .collect::<Vec<_>>();
        let refs = lines.iter().map(String::as_str).collect::<Vec<_>>();
        shard.write_points_string(&refs).unwrap();
        shard.create_snapshot(false).unwrap();

        let mut saw_timestamp_only_block = false;
        for _ in 0..10 {
            let start = rng.gen_range(0_i64..ROWS);
            let end = rng.gen_range(start..ROWS);
            let (actual, expected, metrics) = selected_types_direct_and_iterator_batches(
                &shard,
                TimeRange::new(start, end),
                rng.gen_range(37_usize..193),
            );
            assert_eq!(actual, expected, "range {start}..={end}");
            assert_eq!(
                actual.iter().map(RecordBatch::num_rows).sum::<usize>(),
                (end - start + 1) as usize
            );
            assert_eq!(metrics.multi_field_direct_rows, (end - start + 1) as u64);
            saw_timestamp_only_block |= metrics.timestamp_only_blocks > 0;
            assert_eq!(metrics.iterator_fallback_rows, 0);
        }
        assert!(saw_timestamp_only_block);

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_record_batch_reader_multi_field_unsafe_visibility_uses_iterator_fallback() {
        let persisted = [
            "cpu,host=A float=1.0,integer=1i,unsigned=1u,boolean=true,string=\"a\" 10",
            "cpu,host=A float=2.0,integer=2i,unsigned=2u,boolean=false,string=\"b\" 20",
        ];

        {
            let tmp_dir = must_temp_reefsdb_dir();
            let sfile = Arc::new(must_open_series_file().0);
            let shard = new_test_reefsdb(
                1,
                tmp_dir.path(),
                Arc::clone(&sfile),
                EngineOptions::default(),
            );
            shard.write_points_string(&persisted).unwrap();
            shard.create_snapshot(false).unwrap();
            shard
                .write_points_string(&[
                    "cpu,host=A float=3.0,integer=3i,unsigned=3u,boolean=true,string=\"c\" 30",
                ])
                .unwrap();
            assert_all_types_iterator_fallback(&shard, all_types_aux_storage_scan());
            shard.close().unwrap();
            sfile.close().unwrap();
        }

        {
            let tmp_dir = must_temp_reefsdb_dir();
            let sfile = Arc::new(must_open_series_file().0);
            let mut options = EngineOptions::default();
            options.compaction_disabled = true;
            let shard = new_test_reefsdb(1, tmp_dir.path(), Arc::clone(&sfile), options);
            shard.write_points_string(&persisted).unwrap();
            shard.create_snapshot(false).unwrap();
            shard
                .write_points_string(&[
                    "cpu,host=A float=9.0,integer=9i,unsigned=9u,boolean=false,string=\"new\" 10",
                ])
                .unwrap();
            shard.create_snapshot(false).unwrap();
            assert_all_types_iterator_fallback(&shard, all_types_aux_storage_scan());
            shard.close().unwrap();
            sfile.close().unwrap();
        }

        {
            let tmp_dir = must_temp_reefsdb_dir();
            let sfile = Arc::new(must_open_series_file().0);
            let shard = new_test_reefsdb(
                1,
                tmp_dir.path(),
                Arc::clone(&sfile),
                EngineOptions::default(),
            );
            shard.write_points_string(&persisted).unwrap();
            shard.create_snapshot(false).unwrap();
            shard
                .delete_series_range_by_measurement(
                    b"cpu",
                    Some(|_: &[u8], _: &crate::models::Tags| (10, 10, true)),
                )
                .unwrap();
            assert_all_types_iterator_fallback(&shard, all_types_aux_storage_scan());
            shard.close().unwrap();
            sfile.close().unwrap();
        }

        {
            let tmp_dir = must_temp_reefsdb_dir();
            let sfile = Arc::new(must_open_series_file().0);
            let shard = new_test_reefsdb(
                1,
                tmp_dir.path(),
                Arc::clone(&sfile),
                EngineOptions::default(),
            );
            shard.write_points_string(&persisted).unwrap();
            shard
                .write_points_string(&[
                    "cpu,host=B float=3.0,integer=3i,unsigned=3u,boolean=true,string=\"c\" 30",
                ])
                .unwrap();
            shard.create_snapshot(false).unwrap();
            let (actual, expected, metrics) =
                all_types_direct_and_iterator_rows(&shard, TimeRange::all(), 16);
            assert_eq!(actual, expected);
            assert_eq!(metrics.iterator_fallback_rows, 0);
            assert_eq!(metrics.direct_batch_rows, 3);
            shard.close().unwrap();
            sfile.close().unwrap();
        }

        {
            let tmp_dir = must_temp_reefsdb_dir();
            let sfile = Arc::new(must_open_series_file().0);
            let shard = new_test_reefsdb(
                1,
                tmp_dir.path(),
                Arc::clone(&sfile),
                EngineOptions::default(),
            );
            shard.write_points_string(&persisted).unwrap();
            shard.create_snapshot(false).unwrap();
            let mut scan = all_types_aux_storage_scan();
            scan.predicate = Some(StoragePredicate::Compare {
                op: CompareOp::GreaterThan,
                lhs: storage_column("integer", ColumnKind::Field(Some(crate::models::INTEGER))),
                rhs: storage_literal(crate::models::Val::I64(1)),
            });
            assert_all_types_iterator_fallback(&shard, scan);
            shard.close().unwrap();
            sfile.close().unwrap();
        }

        {
            let tmp_dir = must_temp_reefsdb_dir();
            let sfile = Arc::new(must_open_series_file().0);
            let mut options = EngineOptions::default();
            options.vlog_enabled = true;
            let shard = new_test_reefsdb(1, tmp_dir.path(), Arc::clone(&sfile), options);
            shard.write_points_string(&persisted).unwrap();
            shard.create_snapshot(false).unwrap();
            assert_all_types_iterator_fallback(&shard, all_types_aux_storage_scan());
            shard.close().unwrap();
            sfile.close().unwrap();
        }
    }

    #[test]
    fn test_record_batch_reader_direct_tsm_all_types_and_ranges() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let shard = new_test_reefsdb(
            1,
            tmp_dir.path(),
            Arc::clone(&sfile),
            EngineOptions::default(),
        );
        shard
            .write_points_string(&[
                "cpu,host=A float=1.5,integer=-2i,unsigned=3u,boolean=true,string=\"one\" 10",
                "cpu,host=A float=2.5,integer=4i,unsigned=5u,boolean=false,string=\"two\" 20",
                "cpu,host=A float=3.5,integer=6i,unsigned=7u,boolean=true,string=\"three\" 30",
                "cpu,host=A float=4.5,integer=8i,unsigned=9u,boolean=false,string=\"four\" 40",
                "cpu,host=A float=5.5,integer=10i,unsigned=11u,boolean=true,string=\"five\" 50",
            ])
            .unwrap();
        shard.create_snapshot(false).unwrap();

        for (field, field_type) in [
            ("float", crate::models::FLOAT),
            ("integer", crate::models::INTEGER),
            ("unsigned", crate::models::UNSIGNED),
            ("boolean", crate::models::BOOL),
            ("string", crate::models::STRING),
        ] {
            assert_single_field_direct_matches_iterator(
                &shard,
                field,
                field_type,
                TimeRange::new(10, 50),
                5,
                0,
            );
            assert_single_field_partial_direct_matches_full_iterator(
                &shard,
                field,
                field_type,
                TimeRange::new(20, 40),
                3,
            );
        }
        assert_single_field_direct_matches_iterator(
            &shard,
            "float",
            crate::models::FLOAT,
            TimeRange::new(20, 40),
            3,
            1,
        );
        assert_single_field_direct_matches_iterator(
            &shard,
            "float",
            crate::models::FLOAT,
            TimeRange::new(10, 30),
            3,
            1,
        );
        assert_single_field_direct_matches_iterator(
            &shard,
            "float",
            crate::models::FLOAT,
            TimeRange::new(30, 50),
            3,
            1,
        );
        assert_single_field_direct_matches_iterator(
            &shard,
            "float",
            crate::models::FLOAT,
            TimeRange::new(30, 30),
            1,
            1,
        );
        assert_single_field_direct_matches_iterator(
            &shard,
            "float",
            crate::models::FLOAT,
            TimeRange::new(60, 70),
            0,
            0,
        );

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_record_batch_reader_direct_tsm_honors_soft_byte_bound() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let shard = new_test_reefsdb(
            1,
            tmp_dir.path(),
            Arc::clone(&sfile),
            EngineOptions::default(),
        );
        shard
            .write_points_string(&[
                "cpu,host=A value=1.0 10",
                "cpu,host=A value=2.0 20",
                "cpu,host=A value=3.0 30",
            ])
            .unwrap();
        shard.create_snapshot(false).unwrap();

        let scan = single_field_aux_scan("value", crate::models::FLOAT, TimeRange::all());
        let request = crate::RecordBatchRequest::new(
            scan,
            single_field_schema("value", crate::models::FLOAT),
            1_024,
            1,
        );
        let mut reader = shard.create_record_batch_reader(request).unwrap();
        let rows = single_field_batch_rows(&mut reader, "value", crate::models::FLOAT);

        assert_eq!(rows.len(), 3);
        assert_eq!(reader.metrics().batches_emitted, 3);
        assert_eq!(reader.metrics().direct_batch_rows, 3);
        assert_eq!(reader.metrics().iterator_fallback_rows, 0);

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_record_batch_reader_direct_tsm_propagates_cancellation() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let shard = new_test_reefsdb(
            1,
            tmp_dir.path(),
            Arc::clone(&sfile),
            EngineOptions::default(),
        );
        shard
            .write_points_string(&["cpu,host=A value=1.0 10"])
            .unwrap();
        shard.create_snapshot(false).unwrap();

        let token = CancellationToken::new();
        let mut scan = single_field_aux_scan("value", crate::models::FLOAT, TimeRange::all());
        scan.projection.series.identity_tag_keys.clear();
        scan.cancellation = Some(ScanCancellation::new(token.clone()));
        let request = crate::RecordBatchRequest::new(
            scan,
            single_field_schema("value", crate::models::FLOAT),
            1_024,
            usize::MAX,
        );
        let mut reader = shard.create_record_batch_reader(request).unwrap();
        assert_eq!(shard.active_operation_count(), 1);
        token.cancel();

        let error = reader.next().unwrap().unwrap_err();
        assert!(error.to_string().contains("scan cancelled"));
        assert!(reader.next().is_none());
        assert_eq!(reader.metrics().direct_batch_rows, 0);
        assert_eq!(reader.metrics().iterator_fallback_rows, 0);
        assert_eq!(shard.active_operation_count(), 0);

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_record_batch_reader_multi_series_direct_cancels_workers_and_releases_lease() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let executor: crate::ExecutorRef = Arc::new(
            crate::DefaultTokioExecutor::new_with_storage_query_cpu_parallelism(
                crate::DefaultTokioExecutorOptions::default(),
                4,
            )
            .unwrap(),
        );
        let mut options = EngineOptions::default();
        options.executor = Arc::clone(&executor);
        let shard = new_test_reefsdb(1, tmp_dir.path(), Arc::clone(&sfile), options);
        let lines = (0..4)
            .flat_map(|host| {
                (0..128).map(move |time| {
                    format!("cpu,host=H{host} value={}.0 {time}", host * 1_000 + time)
                })
            })
            .collect::<Vec<_>>();
        let refs = lines.iter().map(String::as_str).collect::<Vec<_>>();
        shard.write_points_string(&refs).unwrap();
        shard.create_snapshot(false).unwrap();

        let token = CancellationToken::new();
        let mut scan = single_field_aux_scan("value", crate::models::FLOAT, TimeRange::all());
        scan.projection.series.identity_tag_keys.clear();
        scan.cancellation = Some(ScanCancellation::new(token.clone()));
        let request = crate::RecordBatchRequest::new(
            scan,
            single_field_schema("value", crate::models::FLOAT),
            8,
            usize::MAX,
        );
        let mut reader = shard.create_record_batch_reader(request).unwrap();
        assert_eq!(shard.active_operation_count(), 1);
        assert!(reader.next().unwrap().is_ok());
        token.cancel();

        let error = reader.next().unwrap().unwrap_err();
        assert!(error.to_string().contains("scan cancelled"));
        assert!(reader.next().is_none());
        drop(reader);
        assert_eq!(shard.active_operation_count(), 0);
        #[cfg(feature = "slow-trace")]
        assert_eq!(executor.storage_query_cpu_budget().active_jobs(), 0);

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_record_batch_reader_direct_tsm_batches_across_blocks() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let shard = new_test_reefsdb(
            1,
            tmp_dir.path(),
            Arc::clone(&sfile),
            EngineOptions::default(),
        );
        let block_rows = crate::config::DEFAULT_MAX_POINTS_PER_BLOCK;
        let total_rows = block_rows * 2 + 101;
        let lines = (0..total_rows)
            .map(|time| format!("cpu,host=A value={} {time}", time as f64 + 0.25))
            .collect::<Vec<_>>();
        let refs = lines.iter().map(String::as_str).collect::<Vec<_>>();
        shard.write_points_string(&refs).unwrap();
        shard.create_snapshot(false).unwrap();

        let scan = single_field_aux_scan("value", crate::models::FLOAT, TimeRange::all());
        let request = crate::RecordBatchRequest::new(
            scan.clone(),
            single_field_schema("value", crate::models::FLOAT),
            4_096,
            usize::MAX,
        );
        let iterator = shard.create_storage_iterator(scan).unwrap();
        let mut fallback = crate::RecordBatchReader::try_new(request.clone(), iterator).unwrap();
        let expected = single_field_batch_rows(&mut fallback, "value", crate::models::FLOAT);
        let mut direct = shard.create_record_batch_reader(request).unwrap();
        let actual = single_field_batch_rows(&mut direct, "value", crate::models::FLOAT);

        assert_eq!(actual, expected);
        assert_eq!(actual.len(), total_rows as usize);
        assert_eq!(direct.metrics().direct_batch_rows, total_rows as u64);
        assert_eq!(direct.metrics().iterator_fallback_rows, 0);
        assert_eq!(direct.metrics().direct_blocks, 3);
        assert_eq!(
            direct.metrics().batches_emitted,
            (total_rows as usize).div_ceil(4_096) as u64
        );

        let scan = single_field_aux_scan(
            "value",
            crate::models::FLOAT,
            TimeRange::new(50, block_rows * 2 + 50),
        );
        let request = crate::RecordBatchRequest::new(
            scan.clone(),
            single_field_schema("value", crate::models::FLOAT),
            4_096,
            usize::MAX,
        );
        let iterator = shard.create_storage_iterator(scan).unwrap();
        let mut fallback = crate::RecordBatchReader::try_new(request.clone(), iterator).unwrap();
        let expected = single_field_batch_rows(&mut fallback, "value", crate::models::FLOAT);
        let mut direct = shard.create_record_batch_reader(request).unwrap();
        let actual = single_field_batch_rows(&mut direct, "value", crate::models::FLOAT);

        assert_eq!(actual, expected);
        assert_eq!(actual.len(), (block_rows * 2 + 1) as usize);
        assert_eq!(direct.metrics().direct_blocks, 3);
        assert_eq!(direct.metrics().partial_direct_blocks, 2);

        assert_single_field_direct_matches_iterator(
            &shard,
            "value",
            crate::models::FLOAT,
            TimeRange::new(block_rows, block_rows * 2 - 1),
            block_rows as usize,
            0,
        );

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_record_batch_reader_direct_tsm_multiple_non_overlapping_files() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let mut options = EngineOptions::default();
        options.compaction_disabled = true;
        let shard = new_test_reefsdb(1, tmp_dir.path(), Arc::clone(&sfile), options);
        shard
            .write_points_string(&["cpu,host=A value=1.0 10", "cpu,host=A value=2.0 20"])
            .unwrap();
        shard.create_snapshot(false).unwrap();
        shard
            .write_points_string(&["cpu,host=A value=3.0 30", "cpu,host=A value=4.0 40"])
            .unwrap();
        shard.create_snapshot(false).unwrap();

        let scan = single_field_aux_scan("value", crate::models::FLOAT, TimeRange::all());
        let request = crate::RecordBatchRequest::new(
            scan.clone(),
            single_field_schema("value", crate::models::FLOAT),
            16,
            usize::MAX,
        );
        let iterator = shard.create_storage_iterator(scan).unwrap();
        let mut fallback = crate::RecordBatchReader::try_new(request.clone(), iterator).unwrap();
        let expected = single_field_batch_rows(&mut fallback, "value", crate::models::FLOAT);
        let mut direct = shard.create_record_batch_reader(request).unwrap();
        let actual = single_field_batch_rows(&mut direct, "value", crate::models::FLOAT);

        assert_eq!(actual, expected);
        assert_eq!(actual.len(), 4);
        assert_eq!(direct.metrics().direct_blocks, 2);
        assert_eq!(direct.metrics().direct_batch_rows, 4);
        assert_eq!(direct.metrics().iterator_fallback_rows, 0);

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_record_batch_reader_cache_falls_back_and_multiple_series_use_direct_scan() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let shard = new_test_reefsdb(
            1,
            tmp_dir.path(),
            Arc::clone(&sfile),
            EngineOptions::default(),
        );
        shard
            .write_points_string(&["cpu,host=A value=1.0 10"])
            .unwrap();
        shard.create_snapshot(false).unwrap();
        shard
            .write_points_string(&["cpu,host=A value=2.0 20"])
            .unwrap();

        let scan = single_field_aux_scan("value", crate::models::FLOAT, TimeRange::all());
        let request = crate::RecordBatchRequest::new(
            scan,
            single_field_schema("value", crate::models::FLOAT),
            16,
            usize::MAX,
        );
        let mut cache_reader = shard.create_record_batch_reader(request).unwrap();
        let cache_rows = single_field_batch_rows(&mut cache_reader, "value", crate::models::FLOAT);
        assert_eq!(cache_rows.len(), 2);
        assert_eq!(cache_reader.metrics().direct_batch_rows, 0);
        assert_eq!(cache_reader.metrics().iterator_fallback_rows, 2);

        shard.create_snapshot(false).unwrap();
        shard
            .write_points_string(&["cpu,host=B value=3.0 30"])
            .unwrap();
        shard.create_snapshot(false).unwrap();
        let scan = single_field_aux_scan("value", crate::models::FLOAT, TimeRange::all());
        let request = crate::RecordBatchRequest::new(
            scan,
            single_field_schema("value", crate::models::FLOAT),
            16,
            usize::MAX,
        );
        let mut series_reader = shard.create_record_batch_reader(request).unwrap();
        let series_rows =
            single_field_batch_rows(&mut series_reader, "value", crate::models::FLOAT);
        assert_eq!(series_rows.len(), 3);
        assert_eq!(series_reader.metrics().direct_batch_rows, 3);
        assert_eq!(series_reader.metrics().iterator_fallback_rows, 0);
        assert_eq!(series_reader.metrics().value_decoded_points, 3);

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_record_batch_reader_multi_series_direct_supports_exact_tag_set() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let shard = new_test_reefsdb(
            1,
            tmp_dir.path(),
            Arc::clone(&sfile),
            EngineOptions::default(),
        );
        shard
            .write_points_string(&[
                "cpu,host=A value=1.0 10",
                "cpu,host=B value=2.0 20",
                "cpu,host=C value=3.0 30",
            ])
            .unwrap();
        shard.create_snapshot(false).unwrap();

        let mut scan = single_field_aux_scan("value", crate::models::FLOAT, TimeRange::all());
        scan.predicate = Some(StoragePredicate::ExactTagSet {
            tag: ColumnRef::new("host", ColumnKind::Tag),
            accepted_values: vec!["C".to_string(), "A".to_string()],
        });
        let request = crate::RecordBatchRequest::new(
            scan.clone(),
            single_field_schema("value", crate::models::FLOAT),
            16,
            usize::MAX,
        );
        let iterator = shard.create_storage_iterator(scan).unwrap();
        let mut fallback = crate::RecordBatchReader::try_new(request.clone(), iterator).unwrap();
        let expected = single_field_batch_rows(&mut fallback, "value", crate::models::FLOAT);
        let mut direct = shard.create_record_batch_reader(request).unwrap();
        let actual = single_field_batch_rows(&mut direct, "value", crate::models::FLOAT);

        assert_eq!(actual, expected);
        assert_eq!(
            actual,
            vec![
                (10, Some("A".to_string()), crate::models::Val::F64(1.0)),
                (30, Some("C".to_string()), crate::models::Val::F64(3.0)),
            ]
        );
        assert_eq!(direct.metrics().iterator_fallback_rows, 0);
        assert_eq!(direct.metrics().direct_batch_rows, 2);

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_record_batch_reader_overlapping_tsm_uses_iterator_fallback() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let mut options = EngineOptions::default();
        options.compaction_disabled = true;
        let shard = new_test_reefsdb(1, tmp_dir.path(), Arc::clone(&sfile), options);
        shard
            .write_points_string(&["cpu,host=A value=1.0 10"])
            .unwrap();
        shard.create_snapshot(false).unwrap();
        shard
            .write_points_string(&["cpu,host=A value=2.0 10"])
            .unwrap();
        shard.create_snapshot(false).unwrap();

        let scan = single_field_aux_scan("value", crate::models::FLOAT, TimeRange::all());
        let request = crate::RecordBatchRequest::new(
            scan,
            single_field_schema("value", crate::models::FLOAT),
            16,
            usize::MAX,
        );
        let mut reader = shard.create_record_batch_reader(request).unwrap();
        let rows = single_field_batch_rows(&mut reader, "value", crate::models::FLOAT);
        assert_eq!(
            rows,
            vec![(10, Some("A".to_string()), crate::models::Val::F64(2.0))]
        );
        assert_eq!(reader.metrics().direct_batch_rows, 0);
        assert_eq!(reader.metrics().iterator_fallback_rows, 1);

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_record_batch_reader_tombstone_uses_iterator_fallback() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let shard = new_test_reefsdb(
            1,
            tmp_dir.path(),
            Arc::clone(&sfile),
            EngineOptions::default(),
        );
        shard
            .write_points_string(&["cpu,host=A value=1.0 10", "cpu,host=A value=2.0 20"])
            .unwrap();
        shard.create_snapshot(false).unwrap();
        shard
            .delete_series_range_by_measurement(
                b"cpu",
                Some(|_: &[u8], _: &crate::models::Tags| (10, 10, true)),
            )
            .unwrap();

        let scan = single_field_aux_scan("value", crate::models::FLOAT, TimeRange::all());
        let request = crate::RecordBatchRequest::new(
            scan,
            single_field_schema("value", crate::models::FLOAT),
            16,
            usize::MAX,
        );
        let mut reader = shard.create_record_batch_reader(request).unwrap();
        let rows = single_field_batch_rows(&mut reader, "value", crate::models::FLOAT);
        assert_eq!(
            rows,
            vec![(20, Some("A".to_string()), crate::models::Val::F64(2.0))]
        );
        assert_eq!(reader.metrics().direct_batch_rows, 0);
        assert_eq!(reader.metrics().iterator_fallback_rows, 1);

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_record_batch_reader_direct_tsm_randomized_differential_and_unsafe_fallback() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let mut options = EngineOptions::default();
        options.compaction_disabled = true;
        let shard = new_test_reefsdb(1, tmp_dir.path(), Arc::clone(&sfile), options);
        let mut rng = StdRng::seed_from_u64(0x5245_434f_5244_4241);
        let lines = (0..4_129_i64)
            .map(|time| {
                let float = rng.gen_range(-50_000_i64..50_000) as f64 / 10.0;
                let integer = rng.gen_range(-1_000_000_i64..1_000_000);
                let unsigned = rng.gen_range(0_u64..2_000_000);
                let boolean = rng.gen_bool(0.5);
                let string = format!("value_{}", rng.gen_range(0_u8..32));
                format!(
                    "cpu,host=A float={float},integer={integer}i,unsigned={unsigned}u,boolean={boolean},string=\"{string}\" {time}"
                )
            })
            .collect::<Vec<_>>();
        let refs = lines.iter().map(String::as_str).collect::<Vec<_>>();
        shard.write_points_string(&refs).unwrap();
        shard.create_snapshot(false).unwrap();

        for (field, field_type) in [
            ("float", crate::models::FLOAT),
            ("integer", crate::models::INTEGER),
            ("unsigned", crate::models::UNSIGNED),
            ("boolean", crate::models::BOOL),
            ("string", crate::models::STRING),
        ] {
            assert_single_field_direct_matches_iterator(
                &shard,
                field,
                field_type,
                TimeRange::all(),
                4_129,
                0,
            );
        }

        for _ in 0..12 {
            let start = rng.gen_range(0_i64..4_129);
            let end = rng.gen_range(start..4_129);
            let field_index = rng.gen_range(0_usize..4);
            let (field, field_type) = [
                ("float", crate::models::FLOAT),
                ("integer", crate::models::INTEGER),
                ("unsigned", crate::models::UNSIGNED),
                ("boolean", crate::models::BOOL),
            ][field_index];
            let scan = single_field_aux_scan(field, field_type, TimeRange::new(start, end));
            let request = crate::RecordBatchRequest::new(
                scan.clone(),
                single_field_schema(field, field_type),
                rng.gen_range(37_usize..257),
                usize::MAX,
            );
            let iterator = shard.create_storage_iterator(scan).unwrap();
            let mut fallback =
                crate::RecordBatchReader::try_new(request.clone(), iterator).unwrap();
            let expected = single_field_batch_rows(&mut fallback, field, field_type);
            let mut direct = shard.create_record_batch_reader(request).unwrap();
            let actual = single_field_batch_rows(&mut direct, field, field_type);

            assert_eq!(actual, expected);
            assert_eq!(actual.len(), (end - start + 1) as usize);
            assert_eq!(direct.metrics().direct_batch_rows, actual.len() as u64);
            assert_eq!(direct.metrics().iterator_fallback_rows, 0);
        }

        shard
            .write_points_string(&["cpu,host=B float=7.5 5000"])
            .unwrap();
        shard.create_snapshot(false).unwrap();
        let scan = single_field_aux_scan("float", crate::models::FLOAT, TimeRange::all());
        let request = crate::RecordBatchRequest::new(
            scan.clone(),
            single_field_schema("float", crate::models::FLOAT),
            257,
            usize::MAX,
        );
        let iterator = shard.create_storage_iterator(scan).unwrap();
        let mut forced = crate::RecordBatchReader::try_new(request.clone(), iterator).unwrap();
        let expected = single_field_batch_rows(&mut forced, "float", crate::models::FLOAT);
        let mut direct = shard.create_record_batch_reader(request).unwrap();
        let actual = single_field_batch_rows(&mut direct, "float", crate::models::FLOAT);

        assert_eq!(actual, expected);
        assert_eq!(direct.metrics().direct_batch_rows, actual.len() as u64);
        assert_eq!(direct.metrics().iterator_fallback_rows, 0);

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_reefsdb_storage_scan_validates_cancellation_and_lifecycle() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let shard = new_test_reefsdb(
            1,
            tmp_dir.path(),
            Arc::clone(&sfile),
            EngineOptions::default(),
        );
        shard
            .write_points_string(&["cpu,host=A value=1.0 10"])
            .unwrap();

        let mut malformed = float_storage_scan("value");
        malformed.projection.returned_columns.clear();
        assert!(matches!(
            shard.create_storage_iterator(malformed),
            Err(Error::Engine {
                source: crate::storage::ActiveStorageError::StorageProjection { .. }
            })
        ));
        assert_eq!(shard.active_operation_count(), 0);

        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let mut cancelled = float_storage_scan("value");
        cancelled.cancellation = Some(ScanCancellation::new(cancellation));
        assert!(matches!(
            shard.create_storage_iterator(cancelled),
            Err(Error::Engine {
                source: crate::storage::ActiveStorageError::ScanCancelled
            })
        ));
        assert_eq!(shard.active_operation_count(), 0);

        let leased_iterator = shard
            .create_storage_iterator(float_storage_scan("value"))
            .unwrap()
            .unwrap();
        let mut iterator = match leased_iterator.try_downcast_float::<iterator::FloatIter>() {
            Ok(iterator) => iterator,
            Err(mut iterator) => {
                iterator.close();
                panic!("single-series native scan must produce a FloatIter");
            }
        };
        assert_eq!(shard.active_operation_count(), 1);
        shard.begin_retention_drain();
        assert!(matches!(
            shard.create_storage_iterator(float_storage_scan("value")),
            Err(Error::ShardDeleted)
        ));
        assert_eq!(shard.active_operation_count(), 1);

        let point = iterator
            .next()
            .expect("an iterator acquired before drain must remain usable");
        assert_eq!((point.time(), point.value()), (10, 1.0));
        assert_eq!(shard.active_operation_count(), 1);
        assert!(iterator.next().is_none());
        assert_eq!(shard.active_operation_count(), 0);
        drop(iterator);
        assert_eq!(shard.active_operation_count(), 0);
        assert_eq!(
            shard.wait_for_drain(None, || false),
            super::DrainWaitOutcome::Drained
        );
        shard.close_after_drain().unwrap();
        sfile.close().unwrap();
    }

    // #[test]
    // fn test_shards_sort() {
    //     let tmp_dir = must_temp_shard_dir();
    //     let sfile = Arc::new(must_open_series_file().0);
    //     let sf = Arc::clone(&sfile);
    //     defer! {
    //         sf.close();
    //     }

    //     let mut opt = EngineOptions::default();
    //     let series_ids = Arc::new(series_set::SeriesIDSet::new(&[]));
    //     opt.series_id_sets = Some(Box::new(TestSeriesIDSets(vec![Arc::clone(&series_ids)])));

    //     let sh1 = new_test_shard(10, tmp_dir.path(), Arc::clone(&sfile), opt.clone());
    //     let sh2 = new_test_shard(2, tmp_dir.path(), Arc::clone(&sfile), opt.clone());
    //     let sh3 = new_test_shard(7, tmp_dir.path(), Arc::clone(&sfile), opt);

    //     let mut shards = Shards(vec![Arc::clone(&sh1), Arc::clone(&sh2), Arc::clone(&sh3)]);
    //     assert_eq!(shards.len(), 3);
    //     assert!(shards.less(1, 0));
    //     shards.swap(0, 1);
    //     assert_eq!(shards.0[0].id(), 2);
    //     assert_eq!(shards.0[1].id(), 10);

    //     shards.sort();
    //     assert_eq!(shards.0[0].id(), 2);
    //     assert_eq!(shards.0[1].id(), 7);
    //     assert_eq!(shards.0[2].id(), 10);

    //     sh1.close().unwrap();
    //     sh2.close().unwrap();
    //     sh3.close().unwrap();
    // }

    // #[test]
    // fn test_shards_field_keys_by_measurement() {
    //     let tmp_dir = must_temp_shard_dir();
    //     let sfile = Arc::new(must_open_series_file().0);
    //     let sf = Arc::clone(&sfile);
    //     defer! {
    //         sf.close();
    //     }

    //     let mut opt = EngineOptions::default();
    //     let series_ids = Arc::new(series_set::SeriesIDSet::new(&[]));
    //     opt.series_id_sets = Some(Box::new(TestSeriesIDSets(vec![Arc::clone(&series_ids)])));

    //     let sh1 = new_test_shard(1, tmp_dir.path(), Arc::clone(&sfile), opt.clone());
    //     let sh2 = new_test_shard(2, tmp_dir.path(), Arc::clone(&sfile), opt);

    //     let shards = Shards(vec![sh1, sh2]);

    //     shards[0]
    //         .write_points_string(&["cpu,host=serverA,region=uswest a=2.2,b=33.3,value=100 0"])
    //         .unwrap();
    //     shards[1]
    //         .write_points_string(&[
    //             "cpu,host=serverA,region=uswest a=2.2,c=12.3,value=100,z=\"hello\" 0",
    //             "disk q=100 0",
    //         ])
    //         .unwrap();

    //     let exp = vec!["a".to_string(), "b".to_string(), "value".to_string()];
    //     let got = Shards(vec![Arc::clone(&shards[0])]).field_keys_by_measurement("cpu".as_bytes());
    //     assert_eq!(exp, got, "exp={:?}, got={:?}", exp, got);

    //     let exp = vec![
    //         "a".to_string(),
    //         "b".to_string(),
    //         "c".to_string(),
    //         "value".to_string(),
    //         "z".to_string(),
    //     ];
    //     let got = shards.shards().field_keys_by_measurement("cpu".as_bytes());
    //     assert_eq!(exp, got, "exp={:?}, got={:?}", exp, got);

    //     shards.close().unwrap()
    // }

    // #[test]
    // fn test_shard_map_type() {
    //     let tmp_dir = must_temp_shard_dir();
    //     let sfile = Arc::new(must_open_series_file().0);
    //     let sf = Arc::clone(&sfile);
    //     defer! {
    //         sf.close();
    //     }

    //     let mut opt = EngineOptions::default();
    //     let series_ids = Arc::new(series_set::SeriesIDSet::new(&[]));
    //     opt.series_id_sets = Some(Box::new(TestSeriesIDSets(vec![Arc::clone(&series_ids)])));

    //     let sh1 = new_test_shard(1, tmp_dir.path(), Arc::clone(&sfile), opt.clone());
    //     sh1.write_points_string(&[
    //         "cpu,host=serverA,region=uswest value=100 0",
    //         "cpu,host=serverA,region=uswest value=50,val2=5  10",
    //         "cpu,host=serverB,region=uswest value=25  0",
    //     ])
    //     .unwrap();

    //     let sh2 = new_test_shard(2, tmp_dir.path(), Arc::clone(&sfile), opt.clone());
    //     sh2.write_points_string(&[
    //         "mem,host=serverA value=25i 0",
    //         "mem,host=serverB value=50i,val3=t 10",
    //         "_reserved,region=uswest value=\"foo\" 0",
    //     ])
    //     .unwrap();

    //     let sh = Shards(vec![sh1, sh2]);

    //     assert_eq!(sh.map_type("cpu", "value"), crate::models::FLOAT);
    //     assert_eq!(sh.map_type("cpu", "host"), ql::TAG);
    //     assert_eq!(sh.map_type("cpu", "region"), ql::TAG);
    //     assert_eq!(sh.map_type("cpu", "val2"), crate::models::FLOAT);
    //     assert_eq!(sh.map_type("cpu", "unknown"), ql::UNKNOWN);
    //     assert_eq!(sh.map_type("mem", "value"), crate::models::INTEGER);
    //     assert_eq!(sh.map_type("mem", "val3"), crate::models::BOOL);
    //     assert_eq!(sh.map_type("mem", "host"), ql::TAG);
    //     assert_eq!(sh.map_type("unknown", "unknown"), ql::UNKNOWN);
    //     assert_eq!(sh.map_type("_field_keys", "field_key"), crate::models::STRING);
    //     assert_eq!(sh.map_type("_field_keys", "field_type"), crate::models::STRING);
    //     assert_eq!(sh.map_type("_field_keys", "unknown"), ql::UNKNOWN);
    //     assert_eq!(sh.map_type("_series", "key"), crate::models::STRING);
    //     assert_eq!(sh.map_type("_series", "unknown"), ql::UNKNOWN);
    //     assert_eq!(sh.map_type("_tag_keys", "tag_key"), crate::models::STRING);
    //     assert_eq!(sh.map_type("_tag_keys", "unknown"), ql::UNKNOWN);
    //     assert_eq!(sh.map_type("_reserved", "value"), crate::models::STRING);
    //     assert_eq!(sh.map_type("_reserved", "region"), ql::TAG);

    //     sh.close().unwrap();
    // }

    // #[test]
    // fn test_shards_create_iterator_ascending() {
    //     let tmp_dir = must_temp_shard_dir();
    //     let sfile = Arc::new(must_open_series_file().0);
    //     let sf = Arc::clone(&sfile);
    //     defer! {
    //         sf.close();
    //     }

    //     let shards = new_test_shards(tmp_dir.path(), Arc::clone(&sfile), 2);
    //     shards[0]
    //         .write_points_string(&[
    //             "cpu,host=server01 value=1.0 1000000000",
    //             "cpu,host=server01 value=3.0 3000000000",
    //         ])
    //         .unwrap();
    //     shards[1]
    //         .write_points_string(&[
    //             "cpu,host=server02 value=2.0 2000000000",
    //             "cpu,host=server02 value=4.0 4000000000",
    //         ])
    //         .unwrap();

    //     let opt = Arc::new(IteratorOptions {
    //         expr: Some(Expr::VarRef(VarRef {
    //             name: "value".into(),
    //             data_type: Some(VarRefDataType::Float),
    //         })),
    //         interval: Some(TimeDimension {
    //             interval: Expr::Literal(Literal::Duration(0)),
    //             offset: None,
    //         }),
    //         condition: Some(host_condition("server01", "server02")),
    //         start_time: crate::models::time::MIN_NANO_TIME_I64,
    //         end_time: crate::models::time::MAX_NANO_TIME_I64,
    //         ascending: true,
    //         ordered: true,
    //         interrupt: Some(CancellationToken::new()),
    //         ..Default::default()
    //     });

    //     let measurement = Measurement {
    //         name: QualifiedMeasurementName {
    //             database: None,
    //             name: MeasurementName::Name(Identifier::new("cpu".to_string())),
    //         },
    //         system_iterator: None,
    //     };
    //     let itr = shards
    //         .create_iterator(&measurement, Arc::clone(&opt))
    //         .unwrap()
    //         .unwrap();
    //     let points = collect_float_points(itr);
    //     let got = points
    //         .iter()
    //         .map(|p| (p.time(), p.value()))
    //         .collect::<Vec<_>>();
    //     let exp = vec![
    //         (1_000_000_000, 1.0),
    //         (2_000_000_000, 2.0),
    //         (3_000_000_000, 3.0),
    //         (4_000_000_000, 4.0),
    //     ];
    //     assert_eq!(exp, got, "exp={:?}, got={:?}", exp, got);

    //     shards.close().unwrap();
    // }

    // #[test]
    // fn test_shards_create_iterator_descending() {
    //     let tmp_dir = must_temp_shard_dir();
    //     let sfile = Arc::new(must_open_series_file().0);
    //     let sf = Arc::clone(&sfile);
    //     defer! {
    //         sf.close();
    //     }

    //     let shards = new_test_shards(tmp_dir.path(), Arc::clone(&sfile), 2);
    //     shards[0]
    //         .write_points_string(&[
    //             "cpu,host=server01 value=1.0 1000000000",
    //             "cpu,host=server01 value=3.0 3000000000",
    //         ])
    //         .unwrap();
    //     shards[1]
    //         .write_points_string(&[
    //             "cpu,host=server02 value=2.0 2000000000",
    //             "cpu,host=server02 value=4.0 4000000000",
    //         ])
    //         .unwrap();

    //     let opt = Arc::new(IteratorOptions {
    //         expr: Some(Expr::VarRef(VarRef {
    //             name: "value".into(),
    //             data_type: Some(VarRefDataType::Float),
    //         })),
    //         interval: Some(TimeDimension {
    //             interval: Expr::Literal(Literal::Duration(0)),
    //             offset: None,
    //         }),
    //         condition: Some(host_condition("server01", "server02")),
    //         start_time: crate::models::time::MIN_NANO_TIME_I64,
    //         end_time: crate::models::time::MAX_NANO_TIME_I64,
    //         ascending: false,
    //         ordered: true,
    //         interrupt: Some(CancellationToken::new()),
    //         ..Default::default()
    //     });

    //             let measurement = Measurement {
    //         name: QualifiedMeasurementName {
    //             database: None,
    //             name: MeasurementName::Name(Identifier::new("cpu".to_string())),
    //         },
    //         system_iterator: None,
    //     };
    //     let itr = shards
    //         .create_iterator(&measurement, Arc::clone(&opt))
    //         .unwrap()
    //         .unwrap();
    //     let points = collect_float_points(itr);
    //     let got = points
    //         .iter()
    //         .map(|p| (p.time(), p.value()))
    //         .collect::<Vec<_>>();
    //     let exp = vec![
    //         (4_000_000_000, 4.0),
    //         (3_000_000_000, 3.0),
    //         (2_000_000_000, 2.0),
    //         (1_000_000_000, 1.0),
    //     ];
    //     assert_eq!(exp, got, "exp={:?}, got={:?}", exp, got);

    //     shards.close().unwrap();
    // }

    // // #[test]
    // // fn test_shards_create_iterator_series() {
    // //     let tmp_dir = must_temp_shard_dir();
    // //     let sfile = Arc::new(must_open_series_file().0);
    // //     let sf = Arc::clone(&sfile);
    // //     defer! {
    // //         sf.close();
    // //     }

    // //     let shards = new_test_shards(tmp_dir.path(), Arc::clone(&sfile), 2);
    // //     shards[0]
    // //         .write_points_string(&[
    // //             "cpu,host=server01,region=west value=1.0 0",
    // //             "cpu,host=server02,region=east value=2.0 0",
    // //         ])
    // //         .unwrap();
    // //     shards[1]
    // //         .write_points_string(&[
    // //             "mem,host=server03 value=3.0 0",
    // //             "disk,host=server04 value=4.0 0",
    // //         ])
    // //         .unwrap();

    // //     let opt = Arc::new(IteratorOptions {
    // //         ascending: true,
    // //         ordered: true,
    // //         interrupt: Some(CancellationToken::new()),
    // //         ..Default::default()
    // //     });

    // //     let measurement = Measurement::with_system_iterator("_series", SystemIterator::Series);
    // //     let itr = shards
    // //         .create_iterator(&measurement, Arc::clone(&opt))
    // //         .unwrap()
    // //         .unwrap();
    // //     let points = collect_string_points(itr);
    // //     let got = points.iter().map(|p| p.value()).collect::<Vec<_>>();
    // //     let exp = vec![
    // //         "cpu,host=server01,region=west".to_string(),
    // //         "cpu,host=server02,region=east".to_string(),
    // //         "disk,host=server04".to_string(),
    // //         "mem,host=server03".to_string(),
    // //     ];
    // //     assert_eq!(exp, got, "exp={:?}, got={:?}", exp, got);

    // //     shards.close().unwrap();
    // // }

    #[test]
    fn test_reefsdb_write_and_index() {
        let tmp_dir = must_temp_reefsdb_dir();
        let tmp_shard = tmp_dir.path().join("shard");
        let tmp_wal = tmp_dir.path().join("wal");
        let tmp_vlog = tmp_dir.path().join("vlog");
        let tmp_vlog_index_wal = tmp_dir.path().join("vlog_index_wal");

        let sfile = Arc::new(must_open_series_file().0);
        let sf = Arc::clone(&sfile);
        defer! {
            sf.close();
        }

        let mut opt = EngineOptions::default();
        opt.shard_id = 1;
        opt.config.wal_dir = tmp_wal.to_str().unwrap().to_string();

        let mut sh = ReefsDb::new(
            tmp_shard.clone(),
            tmp_wal.clone(),
            tmp_vlog.clone(),
            tmp_vlog_index_wal.clone(),
            Arc::clone(&sfile),
            measurement_schemas_for(&sfile),
            opt.clone(),
        )
        .unwrap();

        // Calling WritePoints when the engine is not open will return
        // ErrEngineClosed.
        let got = sh.write_points(&mut []);
        match got {
            Err(Error::EngineClosed) => {}
            _ => panic!("got: {:?}, exp: {:?}", got, Error::EngineClosed),
        }

        sh.open().unwrap();

        let mut pt = crate::models::points::must_new_point(
            "cpu".to_string(),
            crate::models::Tags::from(HashMap::from([(
                "host".as_bytes().to_vec(),
                "server".as_bytes().to_vec(),
            )])),
            Fields(HashMap::from([("value".to_string(), RawVal::F64(1.0))])),
            time::Time::from_timestamp(1, 2),
        );

        sh.write_points(&mut [pt.clone()]).unwrap();

        pt.set_time(time::Time::from_timestamp(2, 3));
        sh.write_points(&mut [pt.clone()]).unwrap();

        let cnt = sh.series_n();
        assert_eq!(cnt, 1, "got {}, exp {} series in index", cnt, 1);

        // ensure the index gets loaded after closing and opening the shard
        sh.close().unwrap();

        let measurement_schemas = measurement_schemas_for(&sfile);
        let mut sh = ReefsDb::new(
            tmp_shard,
            tmp_wal,
            tmp_vlog,
            tmp_vlog_index_wal,
            sfile,
            measurement_schemas,
            opt,
        )
        .unwrap();
        sh.open().unwrap();

        let cnt = sh.series_n();
        assert_eq!(cnt, 1, "got {}, exp {} series in index", cnt, 1);

        // and ensure that we can still write data
        pt.set_time(time::Time::from_timestamp(2, 6));
        sh.write_points(&mut [pt]).unwrap();
    }

    #[test]
    fn test_reefsdb_lifecycle_logging_includes_local_engine_context() {
        let tmp_dir = must_temp_reefsdb_dir();
        let tmp_shard = tmp_dir.path().join("shard");
        let tmp_wal = tmp_dir.path().join("wal");
        let tmp_vlog = tmp_dir.path().join("vlog");
        let tmp_vlog_index_wal = tmp_dir.path().join("vlog_index_wal");
        let sfile = Arc::new(must_open_series_file().0);
        let mut opt = EngineOptions::default();
        opt.shard_id = 17;
        opt.config.wal_dir = tmp_wal.to_str().unwrap().to_string();

        let output = crate::common::test_logging::capture("reefsdb::tsdb::reefsdb=info", || {
            let shard = ReefsDb::new(
                tmp_shard,
                tmp_wal,
                tmp_vlog,
                tmp_vlog_index_wal,
                Arc::clone(&sfile),
                measurement_schemas_for(&sfile),
                opt,
            )
            .unwrap();
            shard.open().unwrap();
            shard.close().unwrap();
        });
        sfile.close().unwrap();

        assert!(output.contains("opening ReefsDb"));
        assert!(output.contains("opened ReefsDb"));
        assert!(output.contains("closing ReefsDb"));
        assert!(output.contains("closed ReefsDb"));
        assert!(output.contains("engine_id=17"));
    }

    fn validate_index(sh: &ReefsDb, path: &Path) {
        let cnt = sh.series_n();
        assert_eq!(cnt, 1, "got {}, exp {} series in index", cnt, 1);

        match std::fs::metadata(path) {
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    panic!("index path {:?} does not exist", path);
                }
            }
            Ok(meta) => {
                if !meta.is_dir() {
                    panic!("index path {:?} is not a directory", path);
                }
            }
        }
    }

    #[test]
    fn test_reefsdb_rebuild_index() {
        let tmp_dir = must_temp_reefsdb_dir();
        let tmp_shard = tmp_dir.path().join("shard");
        let tmp_wal = tmp_dir.path().join("wal");
        let tmp_vlog = tmp_dir.path().join("vlog");
        let tmp_vlog_index_wal = tmp_dir.path().join("vlog_index_wal");

        let sfile = Arc::new(must_open_series_file().0);
        let sf = Arc::clone(&sfile);
        defer! {
            sf.close();
        }

        let mut opt = EngineOptions::default();
        opt.shard_id = 1;
        opt.config.wal_dir = tmp_wal.to_str().unwrap().to_string();

        let mut sh = ReefsDb::new(
            tmp_shard.clone(),
            tmp_wal.clone(),
            tmp_vlog.clone(),
            tmp_vlog_index_wal.clone(),
            Arc::clone(&sfile),
            measurement_schemas_for(&sfile),
            opt.clone(),
        )
        .unwrap();
        sh.open().unwrap();

        let mut pt = crate::models::points::must_new_point(
            "cpu".to_string(),
            crate::models::Tags::from(HashMap::from([(
                "host".as_bytes().to_vec(),
                "server".as_bytes().to_vec(),
            )])),
            Fields(HashMap::from([("value".to_string(), RawVal::F64(1.0))])),
            time::Time::from_timestamp(1, 2),
        );

        sh.write_points(&mut [pt.clone()]).unwrap();

        pt.set_time(time::Time::from_timestamp(2, 3));
        sh.write_points(&mut [pt.clone()]).unwrap();

        let index_path = tmp_shard.join("index");

        validate_index(&sh, &index_path);

        // ensure the index gets rebuilt after its directory is deleted and
        // the shard is reopened.
        sh.close().unwrap();

        std::fs::remove_dir_all(&index_path).unwrap();

        let measurement_schemas = measurement_schemas_for(&sfile);
        let mut sh = ReefsDb::new(
            tmp_shard,
            tmp_wal,
            tmp_vlog,
            tmp_vlog_index_wal,
            sfile,
            measurement_schemas,
            opt,
        )
        .unwrap();
        sh.open().unwrap();

        validate_index(&sh, &index_path);

        pt.set_time(time::Time::from_timestamp(2, 6));
        sh.write_points(&mut [pt]).unwrap();
    }

    #[test]
    fn test_reefsdb_open_corrupt_fields_index() {
        let tmp_dir = must_temp_reefsdb_dir();
        let tmp_shard = tmp_dir.path().join("shard");
        let tmp_wal = tmp_dir.path().join("wal");
        let tmp_vlog = tmp_dir.path().join("vlog");
        let tmp_vlog_index_wal = tmp_dir.path().join("vlog_index_wal");

        let sfile = Arc::new(must_open_series_file().0);
        let sf = Arc::clone(&sfile);
        defer! {
            sf.close();
        }

        let mut opt = EngineOptions::default();
        opt.shard_id = 1;
        opt.config.wal_dir = tmp_wal.to_str().unwrap().to_string();

        let mut sh = ReefsDb::new(
            tmp_shard.clone(),
            tmp_wal.clone(),
            tmp_vlog.clone(),
            tmp_vlog_index_wal.clone(),
            Arc::clone(&sfile),
            measurement_schemas_for(&sfile),
            opt.clone(),
        )
        .unwrap();

        // Calling WritePoints when the engine is not open will return
        // ErrEngineClosed.
        let got = sh.write_points(&mut []);
        match got {
            Err(Error::EngineClosed) => {}
            _ => panic!("got: {:?}, exp: {:?}", got, Error::EngineClosed),
        }

        sh.open().unwrap();

        let pt = crate::models::points::must_new_point(
            "cpu".to_string(),
            crate::models::Tags::from(HashMap::from([(
                "host".as_bytes().to_vec(),
                "server".as_bytes().to_vec(),
            )])),
            Fields(HashMap::from([("value".to_string(), RawVal::F64(1.0))])),
            time::Time::from_timestamp(1, 2),
        );

        sh.write_points(&mut [pt]).unwrap();

        sh.close().unwrap();

        let path = tmp_shard.join("fields.idx");
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();
        f.set_len(6).unwrap();

        let err = sh.open();
        if err.is_err() {
            eprintln!("error opening shard: {:?}", err);
        }
    }

    #[test]
    fn test_reefsdb_write_time_tag() {
        let tmp_dir = must_temp_reefsdb_dir();
        let tmp_shard = tmp_dir.path().join("shard");
        let tmp_wal = tmp_dir.path().join("wal");
        let tmp_vlog = tmp_dir.path().join("vlog");
        let tmp_vlog_index_wal = tmp_dir.path().join("vlog_index_wal");

        let sfile = Arc::new(must_open_series_file().0);
        let sf = Arc::clone(&sfile);
        defer! {
            sf.close();
        }

        let mut opt = EngineOptions::default();
        opt.shard_id = 1;
        opt.config.wal_dir = tmp_wal.to_str().unwrap().to_string();

        let mut sh = ReefsDb::new(
            tmp_shard.clone(),
            tmp_wal.clone(),
            tmp_vlog.clone(),
            tmp_vlog_index_wal.clone(),
            Arc::clone(&sfile),
            measurement_schemas_for(&sfile),
            opt.clone(),
        )
        .unwrap();
        sh.open().unwrap();

        // FIXME: append_field ERROR!!!! 应该是 "time=1"，而不是"time=1.0" !!!!!
        let pt = crate::models::points::must_new_point(
            "cpu".to_string(),
            crate::models::Tags::from(HashMap::new()),
            Fields(HashMap::from([("time".to_string(), RawVal::F64(1.0))])),
            time::Time::from_timestamp(1, 2),
        );

        assert!(
            sh.write_points(&mut [pt]).is_err(),
            "expected error: got Ok"
        );

        let pt = crate::models::points::must_new_point(
            "cpu".to_string(),
            crate::models::Tags::from(HashMap::new()),
            Fields(HashMap::from([
                ("value".to_string(), RawVal::F64(1.0)),
                ("time".to_string(), RawVal::F64(1.0)),
            ])),
            time::Time::from_timestamp(1, 2),
        );

        let err = sh.write_points(&mut [pt]);
        assert!(err.is_ok(), "unexpected error: {:?}", err);

        let mf = sh.measurement_fields("cpu".as_bytes());
        assert!(mf.is_some(), "expected cpu measurement fields");

        let field_n = mf.unwrap().field_n();
        assert_eq!(
            field_n, 1,
            "invalid numnber of field names, got={}, exp={}",
            field_n, 1
        );

        sh.close().unwrap();
    }

    #[test]
    fn test_reefsdb_write_time_field() {
        let tmp_dir = must_temp_reefsdb_dir();
        let tmp_shard = tmp_dir.path().join("shard");
        let tmp_wal = tmp_dir.path().join("wal");
        let tmp_vlog = tmp_dir.path().join("vlog");
        let tmp_vlog_index_wal = tmp_dir.path().join("vlog_index_wal");

        let sfile = Arc::new(must_open_series_file().0);
        let sf = Arc::clone(&sfile);
        defer! {
            sf.close();
        }

        let mut opt = EngineOptions::default();
        opt.shard_id = 1;
        opt.config.wal_dir = tmp_wal.to_str().unwrap().to_string();

        let mut sh = ReefsDb::new(
            tmp_shard.clone(),
            tmp_wal.clone(),
            tmp_vlog.clone(),
            tmp_vlog_index_wal.clone(),
            Arc::clone(&sfile),
            measurement_schemas_for(&sfile),
            opt.clone(),
        )
        .unwrap();
        sh.open().unwrap();

        let mut pt = crate::models::points::must_new_point(
            "cpu".to_string(),
            crate::models::Tags::from(HashMap::from([(
                "time".as_bytes().to_vec(),
                "now".as_bytes().to_vec(),
            )])),
            Fields(HashMap::from([("value".to_string(), RawVal::F64(1.0))])),
            time::Time::from_timestamp(1, 2),
        );

        assert!(
            sh.write_points(&mut [pt]).is_err(),
            "expected error: got Ok"
        );

        let key =
            crate::models::points::make_key("cpu".as_bytes(), &crate::models::Tags::default());
        assert_eq!(
            sh.measurement_exists(&key).unwrap(),
            false,
            "unexpected series"
        );

        sh.close().unwrap();
    }

    #[test]
    fn test_reefsdb_write_add_new_field() {
        let tmp_dir = must_temp_reefsdb_dir();
        let tmp_shard = tmp_dir.path().join("shard");
        let tmp_wal = tmp_dir.path().join("wal");
        let tmp_vlog = tmp_dir.path().join("vlog");
        let tmp_vlog_index_wal = tmp_dir.path().join("vlog_index_wal");

        let sfile = Arc::new(must_open_series_file().0);
        let sf = Arc::clone(&sfile);
        defer! {
            sf.close();
        }

        let mut opt = EngineOptions::default();
        opt.shard_id = 1;
        opt.config.wal_dir = tmp_wal.to_str().unwrap().to_string();

        let mut sh = ReefsDb::new(
            tmp_shard.clone(),
            tmp_wal.clone(),
            tmp_vlog.clone(),
            tmp_vlog_index_wal.clone(),
            Arc::clone(&sfile),
            measurement_schemas_for(&sfile),
            opt.clone(),
        )
        .unwrap();
        sh.open().unwrap();

        let mut pt = crate::models::points::must_new_point(
            "cpu".to_string(),
            crate::models::Tags::from(HashMap::from([(
                "host".as_bytes().to_vec(),
                "server".as_bytes().to_vec(),
            )])),
            Fields(HashMap::from([("value".to_string(), RawVal::F64(1.0))])),
            time::Time::from_timestamp(1, 2),
        );

        sh.write_points(&mut [pt]).unwrap();

        let mut pt = crate::models::points::must_new_point(
            "cpu".to_string(),
            crate::models::Tags::from(HashMap::from([(
                "host".as_bytes().to_vec(),
                "server".as_bytes().to_vec(),
            )])),
            Fields(HashMap::from([
                ("value".to_string(), RawVal::F64(1.0)),
                ("value2".to_string(), RawVal::F64(2.0)),
            ])),
            time::Time::from_timestamp(1, 2),
        );

        sh.write_points(&mut [pt]).unwrap();

        let series_n = sh.series_n();
        assert_eq!(
            series_n, 1,
            "got {} series, exp {} series in index",
            series_n, 1
        );

        sh.close().unwrap();
    }

    // Tests concurrently writing to the same shard with different field types which
    // can trigger a panic when the shard is snapshotted to TSM files.
    #[test]
    fn test_reefsdb_write_points_field_conflict_concurrent() {
        let tmp_dir = must_temp_reefsdb_dir();
        let tmp_shard = tmp_dir.path().join("shard");
        let tmp_wal = tmp_dir.path().join("wal");
        let tmp_vlog = tmp_dir.path().join("vlog");
        let tmp_vlog_index_wal = tmp_dir.path().join("vlog_index_wal");

        let sfile = Arc::new(must_open_series_file().0);
        let sf = Arc::clone(&sfile);
        defer! {
            sf.close();
        }

        let mut opt = EngineOptions::default();
        opt.shard_id = 1;
        opt.config.wal_dir = tmp_wal.to_str().unwrap().to_string();
        let series_ids = Arc::new(series_set::SeriesIDSet::new(&[]));
        opt.series_id_sets = Some(Box::new(TestSeriesIDSets(vec![Arc::clone(&series_ids)])));

        let mut sh = Arc::new(
            ReefsDb::new(
                tmp_shard.clone(),
                tmp_wal.clone(),
                tmp_vlog.clone(),
                tmp_vlog_index_wal.clone(),
                Arc::clone(&sfile),
                measurement_schemas_for(&sfile),
                opt.clone(),
            )
            .unwrap(),
        );
        sh.open().unwrap();

        let mut points = vec![];
        for i in 0..1000 {
            if i < 500 {
                points.push(crate::models::points::must_new_point(
                    "cpu".to_string(),
                    crate::models::Tags::from(HashMap::from([(
                        "host".as_bytes().to_vec(),
                        "server".as_bytes().to_vec(),
                    )])),
                    Fields(HashMap::from([("value".to_string(), RawVal::F64(1.0))])),
                    time::Time::from_timestamp(i, 0),
                ));
            } else {
                points.push(crate::models::points::must_new_point(
                    "cpu".to_string(),
                    crate::models::Tags::from(HashMap::from([(
                        "host".as_bytes().to_vec(),
                        "server".as_bytes().to_vec(),
                    )])),
                    Fields(HashMap::from([("value".to_string(), RawVal::I64(1))])),
                    time::Time::from_timestamp(i, 0),
                ));
            }
        }

        let mut ps1 = points.split_off(500);

        let wg = WaitGroup::new();
        wg.add(2);

        let wg1 = wg.clone();
        let sh1 = Arc::clone(&sh);
        let (tx1, rx1) = tokio::sync::oneshot::channel();
        spawn_blocking_global(move || {
            for _ in 0..50 {
                if let Err(e) = sh1.delete_measurement("cpu".as_bytes()) {
                    tx1.send(Err(e));
                    return;
                }

                let _ = sh1.write_points(&mut ps1);
                if let Ok(path) = sh1.create_snapshot(false) {
                    let _ = std::fs::remove_dir_all(&path);
                }
            }
            tx1.send(Ok(()));
            wg1.done();
        });

        let wg2 = wg.clone();
        let sh2 = Arc::clone(&sh);
        let (tx2, rx2) = tokio::sync::oneshot::channel();
        spawn_blocking_global(move || {
            for _ in 0..50 {
                if let Err(e) = sh2.delete_measurement("cpu".as_bytes()) {
                    tx2.send(Err(e));
                    return;
                }
                let _ = sh2.write_points(&mut points);
                if let Ok(path) = sh2.create_snapshot(false) {
                    let _ = std::fs::remove_dir_all(&path);
                }
            }
            tx2.send(Ok(()));
            wg2.done();
        });

        wg.wait();

        if let Err(e) = rx1.blocking_recv().unwrap() {
            panic!("{:?}", e);
        }
        if let Err(e) = rx2.blocking_recv().unwrap() {
            panic!("{:?}", e);
        }

        sh.close().unwrap();
    }

    /*
    1. ReefsDB, `engine.write_points`, enable vlog, use vlog_wal, 10G,2G,4000
    path: /tmp/tsm1-testDa8lsj
    size: 2.3G
    ### generate 320000000 points elapsed: 188.437981655s
    ### 32 tasks, each task write 10000000 points,  total elapsed 24.385845455s (12,800,000 points/s)
    ### query start_time: 1000000000, end_time: 20000001000000000, estimated total_count: 625001
    ### create iter elapsed: 11.108661ms
    ### query 625001 points(totoal 320000000 points), elapsed 428.63519ms

    2. ReefsDB, `shard.write_points`, enable vlog, use vlog_wal, 10G,2G,4000
    path: /tmp/shard-testFqDLrC/
    ### generate 320000000 points elapsed: 188.286746241s
    ### 32 tasks, each task write 10000000 points,  total elapsed 241.826136206s (1,323,264 points/s)

     */
    #[test]
    #[ignore]
    fn test_reefsdb_create_iter_with_large_write() {
        let tmp_dir = must_temp_reefsdb_dir().into_path();

        let db = "db0";
        let shard_id = "1";

        // auto create
        let tmp_shard = tmp_dir.as_path().join("data").join(db).join(shard_id);
        // auto create
        let tmp_series = tmp_dir.as_path().join("data").join(db).join("_series");
        // auto create
        let tmp_wal = tmp_dir.as_path().join("wal").join(db).join(shard_id);
        // auto create
        let tmp_vlog = tmp_shard.as_path().join("vlog");
        // auto create
        let tmp_vlog_index_wal = tmp_shard.as_path().join("vlog_index_wal");

        let mut sfile = series_file::SeriesFile::new(tmp_series);
        sfile.open().unwrap();
        let sfile = Arc::new(sfile);
        let sf = Arc::clone(&sfile);
        defer! {
            sf.close();
        }

        let mut opt = EngineOptions::default();
        opt.shard_id = 1;
        opt.config.wal_dir = tmp_wal.to_str().unwrap().to_string();
        let series_ids = Arc::new(series_set::SeriesIDSet::new(&[]));
        opt.series_id_sets = Some(Box::new(TestSeriesIDSets(vec![Arc::clone(&series_ids)])));

        let mut sh = Arc::new(
            ReefsDb::new(
                tmp_shard.clone(),
                tmp_wal.clone(),
                tmp_vlog.clone(),
                tmp_vlog_index_wal.clone(),
                Arc::clone(&sfile),
                measurement_schemas_for(&sfile),
                opt.clone(),
            )
            .unwrap(),
        );
        sh.open().unwrap();

        // let sz = 200000 * 50;
        // let num_cpus = num_cpus::get();
        // let tag_count = num_cpus;

        let sz = 200000 * 10;
        let num_cpus = 16;
        let tag_count = num_cpus;

        // ////////////////////////////////////////////////////////
        // ////////////////////////////////////////////////////
        // /////////// generate and write points //////////////

        let mut points = generate_data_with_tag_count(sz, num_cpus, tag_count);

        let mut points_to_write = VecDeque::new();
        for i in 0..num_cpus {
            points_to_write.push_back(points.drain(..sz).collect::<Vec<Point>>());
        }

        let wg = WaitGroup::new();
        for i in 0..num_cpus {
            let wg = wg.clone();
            wg.add(1);
            let sh = Arc::clone(&sh);
            let mut data = points_to_write.pop_front().unwrap();
            spawn_blocking_global(move || {
                let (mut start, end) = (0, sz);
                while start + 40000 < end {
                    sh.write_points(&mut data.as_mut_slice()[start..start + 40000])
                        .unwrap();
                    start += 40000;
                }
                if start < sz {
                    sh.write_points(&mut data.as_mut_slice()[start..end])
                        .unwrap();
                }
                wg.done();
            });
        }
        wg.wait();

        // std::thread::sleep(std::time::Duration::from_secs(60)); // wait for all compactions
        // let cache_size = e.engine.cache().size();
        // if cache_size > 0 {
        //     let now = std::time::Instant::now();
        //     super::Engine::write_snapshot(true, Arc::clone(&e.engine.inner)).unwrap();
        // }

        // ////////////////////////////////////////////////////
        // //////////// create iter ///////////////////////////

        // // let start_time = rand::thread_rng().gen_range(1..=sz as i64) * 1_000_000_000; // ns
        // // let end_time = start_time + 2 * (sz as i64) * 1_000_000_000;

        // let start_time = 1 * 1_000_000_000;
        // let end_time = start_time + (2 * sz * 1_000_000_000) as i64; // 10000000000000000

        // let opt = Arc::new(IteratorOptions {
        //     vlog_enabled: true,
        //     expr: Some(Box::new(ql::VarRef {
        //         // TODO: expr should be None
        //         val: "value".to_string(),
        //         typ: crate::models::FLOAT,
        //     })),
        //     // aux: vec![ql::VarRef {
        //     //     val: "value",
        //     //     typ: crate::models::FLOAT,
        //     // }],
        //     interval: query_iterator::Interval {
        //         duration: literal::Duration::new(0),
        //         offset: literal::Duration::new(0),
        //     },
        //     dimensions: vec![], // TODO: None
        //     group_by: std::collections::HashSet::new(),
        //     // condition: Some(Box::new(BinaryExpr {
        //     //     op: Token::OR,
        //     //     lhs: Some(Box::new(BinaryExpr {
        //     //         op: Token::EQ,
        //     //         lhs: Some(Box::new(VarRef {
        //     //             val: "host",
        //     //             typ: ql::ats::TAG,
        //     //         })),
        //     //         rhs: Some(Box::new(StringLiteral {
        //     //             val: "00000001".to_string(),
        //     //         })),
        //     //     })),
        //     //     rhs: Some(Box::new(BinaryExpr {
        //     //         op: Token::EQ,
        //     //         lhs: Some(Box::new(VarRef {
        //     //             val: "host",
        //     //             typ: ql::ats::TAG,
        //     //         })),
        //     //         rhs: Some(Box::new(StringLiteral {
        //     //             val: "00000002".to_string(),
        //     //         })),
        //     //     })),
        //     // })),
        //     condition: Some(Box::new(BinaryExpr {
        //         op: Token::EQ,
        //         lhs: Some(Box::new(VarRef {
        //             val: "host".to_string(),
        //             typ: ql::ats::TAG,
        //         })),
        //         rhs: Some(Box::new(StringLiteral {
        //             val: "00000001".to_string(),
        //         })),
        //     })),
        //     start_time,
        //     end_time, // include end_time
        //     ascending: true,
        //     // Always request an ordered output for the top level iterators.
        //     // The emitter will always emit points as ordered.
        //     ordered: true,
        //     interrupt: Some(CancellationToken::new()),
        //     // limit: 10,
        //     ..Default::default()
        // });

        // let total_count = (end_time - start_time) / 1_000_000_000 / (tag_count as i64) + 1;

        // let now = std::time::Instant::now();
        // let iter = e
        //     .engine
        //     .create_iterator("cpu", Arc::clone(&opt))
        //     .unwrap()
        //     .unwrap();

        // let now = std::time::Instant::now();

        // let query_count = drain_expr_iter(Arc::clone(&opt), iter, total_count, true);
        // // let query_count = drain_aux_iter(Arc::clone(&opt), iter, total_count, true);

        sh.close().unwrap();
    }

    #[test]
    fn test_measurement_field_set_concurrent_save() {
        let dir = must_temp_reefsdb_dir();
        let iterations = 200;
        let mt = vec!["cpu", "dpu", "epu", "fpu"];
        let mut ft = vec![vec![]; mt.len()];
        for (i, m) in mt.iter().enumerate() {
            ft[i] = vec![String::new(); iterations];
            for j in 0..iterations {
                ft[i][j] = format!("{}_{}_{}", m, "value", i);
            }
        }

        let path = dir.path().join("fields.idx");
        let mfs = Arc::new(MeasurementFieldSet::new(&path).unwrap());

        let wg = WaitGroup::new();
        wg.add(ft.len());
        for (i, fs) in ft.iter().enumerate() {
            let wg = wg.clone();
            let mfs = Arc::clone(&mfs);
            let measurement = mt[i].to_string();
            let field_names = fs.clone();
            spawn_blocking_global(move || {
                let fields = mfs.create_fields_if_not_exists(measurement.as_bytes());
                for field_name in field_names {
                    fields
                        .create_field_if_not_exists(field_name.as_bytes(), crate::models::FLOAT)
                        .unwrap();
                    mfs.save().unwrap();
                }
                wg.done();
            });
        }
        wg.wait();

        let mfs2 = MeasurementFieldSet::new(&path).unwrap();
        for (i, fs) in ft.iter().enumerate() {
            let mf = mfs.fields(mt[i].as_bytes()).unwrap();
            let mf2 = mfs2.fields(mt[i].as_bytes()).unwrap();
            for f in fs {
                assert!(
                    mf2.field(f).is_some(),
                    "Created field not found on reloaded MeasurementFieldSet: {}",
                    f
                );
                assert!(
                    mf.field(f).is_some(),
                    "Created field not found on original MeasurementFieldSet: {}",
                    f
                );
            }
        }
    }

    // //////////////////////////////////////////

    /// Benchmark-only same-version A/B for row-domain value decoding.
    #[test]
    #[ignore = "release-only timestamp-only row-domain direct TSM A/B benchmark"]
    fn benchmark_record_batch_timestamp_only_vs_forced_full_row_domain_values() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let mut options = EngineOptions::default();
        options.compaction_disabled = true;
        let shard = new_test_reefsdb(1, tmp_dir.path(), Arc::clone(&sfile), options);
        let rows = 172_800_i64;
        let lines = (0..rows)
            .map(|time| {
                let fields = (0..10)
                    .map(|field| format!("f{field:02}={}.0", time * 10 + field))
                    .collect::<Vec<_>>()
                    .join(",");
                format!("wide,host=A {fields} {time}")
            })
            .collect::<Vec<_>>();
        let refs = lines.iter().map(String::as_str).collect::<Vec<_>>();
        shard.write_points_string(&refs).unwrap();
        shard.create_snapshot(false).unwrap();

        let benchmark_cases = [
            (2, 86_400_i64, "1day"),
            (4, 3_600_i64, "1h"),
            (4, 18_000_i64, "5h"),
            (4, 86_400_i64, "1day"),
            (4, 172_800_i64, "2day"),
            (8, 86_400_i64, "1day"),
            (10, 86_400_i64, "1day"),
        ];
        for (width, seconds, label) in benchmark_cases {
            let returned_columns = (0..width)
                .map(|field| {
                    ColumnRef::new(
                        format!("f{field:02}"),
                        ColumnKind::Field(Some(crate::models::FLOAT)),
                    )
                })
                .collect::<Vec<_>>();
            let row_domain_columns = (width..10)
                .map(|field| {
                    ColumnRef::new(
                        format!("f{field:02}"),
                        ColumnKind::Field(Some(crate::models::FLOAT)),
                    )
                })
                .collect::<Vec<_>>();
            let mut arrow_fields = vec![
                ArrowField::new(
                    crate::query::metadata::TIME_COLUMN_NAME,
                    crate::query::metadata::time_arrow_type(),
                    false,
                ),
                ArrowField::new("host", ArrowType::Utf8, true),
            ];
            arrow_fields
                .extend((0..width).map(|field| {
                    ArrowField::new(format!("f{field:02}"), ArrowType::Float64, true)
                }));
            let scan = StorageScan {
                measurement: MeasurementRef::new("wide"),
                projection: StorageProjection {
                    primary_column: None,
                    returned_columns,
                    row_domain_columns,
                    series: SeriesProjection::new(
                        vec!["host".to_string()],
                        vec!["host".to_string()],
                    ),
                },
                time_range: TimeRange::new(0, seconds - 1),
                predicate: None,
                time_order: TimeOrder::Ascending,
                output_order: OutputOrder::TimeOrdered,
                execution: Default::default(),
                cancellation: None,
            };
            let request = crate::RecordBatchRequest::new(
                scan,
                Arc::new(ArrowSchema::new(arrow_fields)),
                8_192,
                usize::MAX,
            );
            let drain = |mut reader: crate::RecordBatchReader| {
                let started = Instant::now();
                let mut count = 0_u64;
                for batch in &mut reader {
                    count += batch.unwrap().num_rows() as u64;
                }
                (started.elapsed(), count, reader.metrics())
            };
            let forced_full_reader = || {
                let lease = shard.acquire_operation().unwrap();
                lease
                    .create_record_batch_reader_with_full_row_domain_values(request.clone())
                    .unwrap()
            };

            let _ = drain(shard.create_record_batch_reader(request.clone()).unwrap());
            let _ = drain(forced_full_reader());
            let mut timestamp_only = Vec::with_capacity(5);
            let mut full_value = Vec::with_capacity(5);
            for _ in 0..5 {
                timestamp_only.push(drain(
                    shard.create_record_batch_reader(request.clone()).unwrap(),
                ));
                full_value.push(drain(forced_full_reader()));
            }
            timestamp_only.sort_by_key(|item| item.0);
            full_value.sort_by_key(|item| item.0);
            let timestamp_only_middle = timestamp_only[2];
            let full_value_middle = full_value[2];
            assert_eq!(timestamp_only_middle.1, seconds as u64);
            assert_eq!(full_value_middle.1, seconds as u64);
            assert_eq!(timestamp_only_middle.2.multi_field_streams, 10);
            assert_eq!(full_value_middle.2.multi_field_streams, 10);
            assert_eq!(
                timestamp_only_middle.2.multi_field_direct_rows,
                seconds as u64
            );
            assert_eq!(full_value_middle.2.multi_field_direct_rows, seconds as u64);
            assert_eq!(timestamp_only_middle.2.iterator_fallback_rows, 0);
            assert_eq!(full_value_middle.2.iterator_fallback_rows, 0);
            assert_eq!(
                timestamp_only_middle.2.direct_blocks,
                timestamp_only_middle.2.value_decoded_blocks
                    + timestamp_only_middle.2.timestamp_only_blocks
            );
            assert_eq!(full_value_middle.2.timestamp_only_blocks, 0);
            assert_eq!(
                full_value_middle.2.value_decoded_blocks,
                full_value_middle.2.direct_blocks
            );
            if width < 10 {
                assert!(timestamp_only_middle.2.timestamp_only_blocks > 0);
                assert!(
                    timestamp_only_middle.2.value_decoded_blocks
                        < timestamp_only_middle.2.direct_blocks
                );
            } else {
                assert_eq!(timestamp_only_middle.2.timestamp_only_blocks, 0);
            }
            let timestamp_only_seconds = timestamp_only_middle.0.as_secs_f64();
            let full_value_seconds = full_value_middle.0.as_secs_f64();
            let decoded_values_avoided = full_value_middle
                .2
                .value_decoded_points
                .saturating_sub(timestamp_only_middle.2.value_decoded_points);
            eprintln!(
                "row_domain_timestamp_only_ab total_fields=10 returned_fields={width} row_domain_fields={} range={label} rows={seconds} direct_blocks={} value_decoded_blocks={} timestamp_only_blocks={} full_value_median_ms={:.3} timestamp_only_median_ms={:.3} speedup={:.3} decoded_values_avoided={decoded_values_avoided} batches={}",
                10 - width,
                timestamp_only_middle.2.direct_blocks,
                timestamp_only_middle.2.value_decoded_blocks,
                timestamp_only_middle.2.timestamp_only_blocks,
                full_value_seconds * 1_000.0,
                timestamp_only_seconds * 1_000.0,
                full_value_seconds / timestamp_only_seconds,
                timestamp_only_middle.2.batches_emitted,
            );
        }

        shard.close().unwrap();
        sfile.close().unwrap();
    }

    /// Benchmark-only same-version A/B: normal direct reader versus an
    /// explicitly iterator-backed reader. This is intentionally ignored and
    /// compiled only with the test module; it adds no production switch.
    #[test]
    #[ignore = "release-only direct TSM RecordBatch A/B benchmark"]
    fn benchmark_record_batch_direct_tsm_vs_forced_iterator() {
        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let mut options = EngineOptions::default();
        options.compaction_disabled = true;
        let shard = new_test_reefsdb(1, tmp_dir.path(), Arc::clone(&sfile), options);
        let rows = 172_800_i64;
        let lines = (0..rows)
            .map(|time| format!("cpu,host=A value={} {time}", time as f64 / 10.0))
            .collect::<Vec<_>>();
        let refs = lines.iter().map(String::as_str).collect::<Vec<_>>();
        shard.write_points_string(&refs).unwrap();
        shard.create_snapshot(false).unwrap();

        for (label, seconds) in [
            ("1h", 3_600_i64),
            ("5h", 18_000),
            ("1day", 86_400),
            ("2day", rows),
        ] {
            let request = crate::RecordBatchRequest::new(
                single_field_aux_scan(
                    "value",
                    crate::models::FLOAT,
                    TimeRange::new(0, seconds - 1),
                ),
                single_field_schema("value", crate::models::FLOAT),
                8_192,
                usize::MAX,
            );
            let drain = |mut reader: crate::RecordBatchReader| {
                let started = Instant::now();
                let mut count = 0_u64;
                for batch in &mut reader {
                    count += batch.unwrap().num_rows() as u64;
                }
                (started.elapsed(), count, reader.metrics())
            };
            // Warm both strategies before timing five full consumptions.
            let _ = drain(shard.create_record_batch_reader(request.clone()).unwrap());
            let iterator = shard
                .create_storage_iterator(request.scan().clone())
                .unwrap();
            let _ = drain(crate::RecordBatchReader::try_new(request.clone(), iterator).unwrap());
            let mut direct = Vec::new();
            let mut fallback = Vec::new();
            for _ in 0..5 {
                direct.push(drain(
                    shard.create_record_batch_reader(request.clone()).unwrap(),
                ));
                let iterator = shard
                    .create_storage_iterator(request.scan().clone())
                    .unwrap();
                fallback.push(drain(
                    crate::RecordBatchReader::try_new(request.clone(), iterator).unwrap(),
                ));
            }
            let median =
                |values: &mut Vec<(std::time::Duration, u64, crate::RecordBatchMetrics)>| {
                    values.sort_by_key(|item| item.0);
                    values[2]
                };
            let direct_middle = median(&mut direct);
            let fallback_middle = median(&mut fallback);
            let direct_min = direct.first().unwrap().0;
            let direct_max = direct.last().unwrap().0;
            let fallback_min = fallback.first().unwrap().0;
            let fallback_max = fallback.last().unwrap().0;
            assert_eq!(direct_middle.1, seconds as u64);
            assert_eq!(fallback_middle.1, seconds as u64);
            assert!(direct_middle.2.direct_batch_rows > 0);
            assert_eq!(direct_middle.2.iterator_fallback_rows, 0);
            assert_eq!(fallback_middle.2.direct_batch_rows, 0);
            assert_eq!(fallback_middle.2.iterator_fallback_rows, seconds as u64);
            eprintln!(
                "recordbatch_ab range={label} rows={seconds} forced_iterator_ms={} forced_iterator_min_ms={} forced_iterator_max_ms={} direct_ms={} direct_min_ms={} direct_max_ms={} speedup={:.3} direct_rows={} fallback_rows={} direct_blocks={} partial_blocks={} batches={}",
                fallback_middle.0.as_secs_f64() * 1_000.0,
                fallback_min.as_secs_f64() * 1_000.0,
                fallback_max.as_secs_f64() * 1_000.0,
                direct_middle.0.as_secs_f64() * 1_000.0,
                direct_min.as_secs_f64() * 1_000.0,
                direct_max.as_secs_f64() * 1_000.0,
                fallback_middle.0.as_secs_f64() / direct_middle.0.as_secs_f64(),
                direct_middle.2.direct_batch_rows,
                direct_middle.2.iterator_fallback_rows,
                direct_middle.2.direct_blocks,
                direct_middle.2.partial_direct_blocks,
                direct_middle.2.batches_emitted,
            );
        }
        shard.close().unwrap();
        sfile.close().unwrap();
    }

    /// Reduced-density local A/B using the frozen raw-range manifest's exact
    /// host sets, fields, and H1-01/H8-01 windows. The production fixture is
    /// intentionally not recreated in a developer VM; points are one minute
    /// apart instead of one second apart.
    #[cfg(feature = "slow-trace")]
    #[test]
    #[ignore = "release-only frozen raw-range reduced-fixture A/B benchmark"]
    fn benchmark_multi_series_direct_tsm_frozen_manifest_reduced_fixture() {
        const STEP_NS: i64 = 60_000_000_000;
        const H1_START: i64 = 1_768_630_582_000_000_000;
        const H1_STOP: i64 = 1_768_634_182_000_000_000;
        const H8_START: i64 = 1_768_673_782_000_000_000;
        const H8_STOP: i64 = 1_768_702_582_000_000_000;
        const FIELDS: [&str; 10] = [
            "usage_user",
            "usage_system",
            "usage_idle",
            "usage_nice",
            "usage_iowait",
            "usage_irq",
            "usage_softirq",
            "usage_steal",
            "usage_guest",
            "usage_guest_nice",
        ];
        const HOSTS_1: [&str; 1] = ["host_39"];
        const HOSTS_8: [&str; 8] = [
            "host_49", "host_3", "host_35", "host_39", "host_25", "host_15", "host_21", "host_11",
        ];
        const HOSTS_32: [&str; 32] = [
            "host_49", "host_3", "host_35", "host_39", "host_25", "host_15", "host_21", "host_11",
            "host_37", "host_2", "host_32", "host_48", "host_1", "host_22", "host_18", "host_4",
            "host_44", "host_7", "host_13", "host_26", "host_33", "host_8", "host_30", "host_36",
            "host_14", "host_34", "host_19", "host_24", "host_10", "host_16", "host_47", "host_27",
        ];

        #[derive(Clone, Copy)]
        struct Workload {
            name: &'static str,
            hosts: &'static [&'static str],
            field_count: usize,
            start: i64,
            stop: i64,
        }

        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        struct Digest {
            rows: u64,
            values: u64,
            hash: u64,
        }

        type Sample = (
            std::time::Duration,
            Digest,
            crate::RecordBatchMetrics,
            crate::StorageScanProfileMetrics,
        );

        fn request(
            workload: Workload,
            profile: Arc<crate::StorageScanProfile>,
        ) -> crate::RecordBatchRequest {
            let returned_columns = FIELDS[..workload.field_count]
                .iter()
                .map(|field| ColumnRef::new(*field, ColumnKind::Field(Some(crate::models::FLOAT))))
                .collect::<Vec<_>>();
            let row_domain_columns = FIELDS[workload.field_count..]
                .iter()
                .map(|field| ColumnRef::new(*field, ColumnKind::Field(Some(crate::models::FLOAT))))
                .collect::<Vec<_>>();
            let mut arrow_fields = vec![
                ArrowField::new(
                    crate::query::metadata::TIME_COLUMN_NAME,
                    crate::query::metadata::time_arrow_type(),
                    false,
                ),
                ArrowField::new("hostname", ArrowType::Utf8, true),
            ];
            arrow_fields.extend(
                FIELDS[..workload.field_count]
                    .iter()
                    .map(|field| ArrowField::new(*field, ArrowType::Float64, true)),
            );
            let scan = StorageScan {
                measurement: MeasurementRef::new("cpu"),
                projection: StorageProjection {
                    primary_column: None,
                    returned_columns,
                    row_domain_columns,
                    series: SeriesProjection::new(vec![], vec!["hostname".to_string()]),
                },
                time_range: TimeRange::new(workload.start, workload.stop - 1),
                predicate: Some(StoragePredicate::ExactTagSet {
                    tag: ColumnRef::new("hostname", ColumnKind::Tag),
                    accepted_values: workload
                        .hosts
                        .iter()
                        .map(|host| (*host).to_string())
                        .collect(),
                }),
                time_order: TimeOrder::Ascending,
                output_order: OutputOrder::TimeOrdered,
                execution: crate::StorageExecutionOptions {
                    profile: Some(profile),
                    ..Default::default()
                },
                cancellation: None,
            };
            crate::RecordBatchRequest::new(
                scan,
                Arc::new(ArrowSchema::new(arrow_fields)),
                1_024,
                usize::MAX,
            )
        }

        fn run(shard: &Arc<ReefsDb>, workload: Workload, direct: bool) -> Sample {
            use std::hash::Hasher as _;

            let profile = Arc::new(crate::StorageScanProfile::default());
            let request = request(workload, Arc::clone(&profile));
            let started = Instant::now();
            let mut reader = if direct {
                shard.create_record_batch_reader(request).unwrap()
            } else {
                let iterator = shard
                    .create_storage_iterator(request.scan().clone())
                    .unwrap();
                crate::RecordBatchReader::try_new(request, iterator).unwrap()
            };
            let mut rows = 0_u64;
            let mut values = 0_u64;
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            for batch in &mut reader {
                let batch = batch.unwrap();
                let times = batch
                    .column_by_name(crate::query::metadata::TIME_COLUMN_NAME)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                let hosts = batch
                    .column_by_name("hostname")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let columns = FIELDS[..workload.field_count]
                    .iter()
                    .map(|field| {
                        batch
                            .column_by_name(field)
                            .unwrap()
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .unwrap()
                    })
                    .collect::<Vec<_>>();
                for row in 0..batch.num_rows() {
                    hasher.write_i64(times.value(row));
                    hasher.write(hosts.value(row).as_bytes());
                    for column in &columns {
                        if column.is_null(row) {
                            hasher.write_u8(0);
                        } else {
                            hasher.write_u8(1);
                            hasher.write_u64(column.value(row).to_bits());
                            values = values.saturating_add(1);
                        }
                    }
                }
                rows = rows.saturating_add(batch.num_rows() as u64);
            }
            let elapsed = started.elapsed();
            let record_batch_metrics = reader.metrics();
            let profile_metrics = profile.raw_snapshot();
            (
                elapsed,
                Digest {
                    rows,
                    values,
                    hash: hasher.finish(),
                },
                record_batch_metrics,
                profile_metrics,
            )
        }

        fn median(mut samples: Vec<Sample>) -> Sample {
            samples.sort_by_key(|sample| sample.0);
            samples[samples.len() / 2]
        }

        let workloads = [
            Workload {
                name: "R1",
                hosts: &HOSTS_1,
                field_count: 1,
                start: H1_START,
                stop: H1_STOP,
            },
            Workload {
                name: "R2",
                hosts: &HOSTS_8,
                field_count: 1,
                start: H1_START,
                stop: H1_STOP,
            },
            Workload {
                name: "R3",
                hosts: &HOSTS_8,
                field_count: 5,
                start: H1_START,
                stop: H1_STOP,
            },
            Workload {
                name: "R4",
                hosts: &HOSTS_32,
                field_count: 1,
                start: H8_START,
                stop: H8_STOP,
            },
            Workload {
                name: "R5",
                hosts: &HOSTS_32,
                field_count: 10,
                start: H8_START,
                stop: H8_STOP,
            },
        ];

        let tmp_dir = must_temp_reefsdb_dir();
        let sfile = Arc::new(must_open_series_file().0);
        let mut options = EngineOptions::default();
        options.compaction_disabled = true;
        options.executor = Arc::new(
            crate::DefaultTokioExecutor::new_with_storage_query_cpu_parallelism(
                crate::DefaultTokioExecutorOptions::default(),
                4,
            )
            .unwrap(),
        );
        let shard = new_test_reefsdb(1, tmp_dir.path(), Arc::clone(&sfile), options);
        let mut lines = Vec::new();
        for (range_start, range_stop) in [(H1_START, H1_STOP), (H8_START, H8_STOP)] {
            for (host_index, host) in HOSTS_32.iter().enumerate() {
                for (point_index, timestamp) in (range_start..range_stop)
                    .step_by(STEP_NS as usize)
                    .enumerate()
                {
                    let fields = FIELDS
                        .iter()
                        .enumerate()
                        .map(|(field_index, field)| {
                            format!(
                                "{field}={}.0",
                                host_index * 1_000_000 + point_index * 10 + field_index
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(",");
                    lines.push(format!("cpu,hostname={host} {fields} {timestamp}"));
                }
            }
        }
        let refs = lines.iter().map(String::as_str).collect::<Vec<_>>();
        shard.write_points_string(&refs).unwrap();
        shard.create_snapshot(false).unwrap();

        for workload in workloads {
            let _ = run(&shard, workload, false);
            let _ = run(&shard, workload, true);
            let before = median((0..7).map(|_| run(&shard, workload, false)).collect());
            let after = median((0..7).map(|_| run(&shard, workload, true)).collect());
            let expected_rows =
                ((workload.stop - workload.start) / STEP_NS) as u64 * workload.hosts.len() as u64;
            assert_eq!(before.1, after.1);
            assert_eq!(after.1.rows, expected_rows);
            assert_eq!(
                after.1.values,
                expected_rows.saturating_mul(workload.field_count as u64)
            );
            assert_eq!(after.2.iterator_fallback_rows, 0);
            assert_eq!(
                after.3.direct_scan_series_count,
                workload.hosts.len() as u64
            );
            assert_eq!(after.3.generic_fallback_series_count, 0);
            assert_eq!(after.3.selected_tag_sets, 1);
            assert_eq!(
                after.3.physical_series_keys_per_tagset_max,
                workload.hosts.len() as u64
            );
            if workload.hosts.len() > 1 {
                assert_eq!(
                    after.3.direct_merge_input_series,
                    workload.hosts.len() as u64
                );
                assert_eq!(after.3.direct_merge_rows, expected_rows);
            }
            assert_eq!(before.3.direct_scan_series_count, 0);
            assert_eq!(
                before.3.generic_fallback_series_count,
                workload.hosts.len() as u64
            );
            assert_eq!(
                before.3.generic_fallback_physical_series_count,
                workload.hosts.len() as u64
            );
            if workload.field_count < FIELDS.len() {
                assert!(after.3.timestamp_only_points_decoded > 0);
            } else {
                assert_eq!(after.3.timestamp_only_points_decoded, 0);
            }
            eprintln!(
                "raw_manifest_reduced_ab workload={} manifest_sha256=e2cd60a74613aa63caf4285d9f1523e6a64c67818840fb43f99f04bb991ac0f8 interval_seconds=60 rows={} values={} forced_generic_ms={:.3} direct_ms={:.3} speedup={:.3} exact=true before_full_streams={} after_full_streams={} after_timestamp_streams={} before_full_blocks={} before_full_points={} before_timestamp_blocks={} before_timestamp_points={} after_full_blocks={} after_full_points={} after_timestamp_blocks={} after_timestamp_points={}",
                workload.name,
                after.1.rows,
                after.1.values,
                before.0.as_secs_f64() * 1_000.0,
                after.0.as_secs_f64() * 1_000.0,
                before.0.as_secs_f64() / after.0.as_secs_f64(),
                before.3.field_cursors,
                after.3.field_value_streams,
                after.3.timestamp_only_streams,
                before.3.full_value_blocks_decoded,
                before.3.full_value_points_decoded,
                before.3.timestamp_only_blocks_decoded,
                before.3.timestamp_only_points_decoded,
                after.3.full_value_blocks_decoded,
                after.3.full_value_points_decoded,
                after.3.timestamp_only_blocks_decoded,
                after.3.timestamp_only_points_decoded,
            );
        }
        shard.close().unwrap();
        drop(shard);

        let scaling_workload = workloads[3];
        for parallelism in [1_usize, 2, 4, 8] {
            let executor = Arc::new(
                crate::DefaultTokioExecutor::new_with_storage_query_cpu_parallelism(
                    crate::DefaultTokioExecutorOptions::default(),
                    parallelism,
                )
                .unwrap(),
            );
            let budget = crate::RuntimeExecutor::storage_query_cpu_budget(executor.as_ref());
            let mut options = EngineOptions::default();
            options.compaction_disabled = true;
            options.executor = executor;
            let shard = new_test_reefsdb(1, tmp_dir.path(), Arc::clone(&sfile), options);
            let _ = run(&shard, scaling_workload, true);
            budget.reset_active_jobs_peak();
            let sample = median(
                (0..7)
                    .map(|_| run(&shard, scaling_workload, true))
                    .collect(),
            );
            assert_eq!(budget.active_jobs(), 0);
            assert!((1..=parallelism).contains(&budget.active_jobs_peak()));
            eprintln!(
                "raw_manifest_reduced_scaling workload=R4 interval_seconds=60 parallelism={} median_ms={:.3} rows={} active_jobs_peak={}",
                parallelism,
                sample.0.as_secs_f64() * 1_000.0,
                sample.1.rows,
                budget.active_jobs_peak(),
            );
            shard.close().unwrap();
        }
        sfile.close().unwrap();
    }

    #[cfg(test)]
    #[ctor::ctor]
    fn init_runtime() {
        let runtime_opt = crate::runtime::global::RuntimeOptions::default();
        crate::runtime::global::init_global_runtimes(&runtime_opt);
    }

    pub fn must_temp_reefsdb_dir() -> TempDir {
        tempfile::TempDir::with_prefix("shard-test")
            .expect("unable to create shard temporary working directory")
    }
}
