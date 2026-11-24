//! Engine is a Time Structured Merge tree.
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs, i64,
    io::Read,
    os::unix::{ffi::OsStrExt, fs::PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, Instant},
};

#[cfg(feature = "slow-trace")]
use std::sync::atomic::AtomicU64;

#[cfg(feature = "slow-trace")]
use crate::ScanCancellation;
use crate::common::{
    bytesutil,
    hll::HLL,
    time::{self, Time},
    to_str, to_string,
    wait_group::WaitGroup,
};
use crate::models::{
    BOOL, FLOAT, INTEGER, STRING, Tags, UNSIGNED,
    points::{
        BoolValue, FieldType, FloatValue, IntegerValue, StringValue, UnsignedValue, Val, Value,
    },
    statistic::Statistic,
    time::{MAX_NANO_TIME_I64, MIN_NANO_TIME_I64},
};
use crate::runtime::execution::{
    ExecutorRef, RuntimeExecutorExt, SpawnError, StorageQueryCpuBudget, StorageQueryCpuPermit,
    accepted_task_guard,
};
#[cfg(feature = "slow-trace")]
use crate::runtime::execution::{
    GeneralBlockingTaskClass, GeneralBlockingTaskDiagnostics, StorageQueryCpuActivePermit,
};
use futures::{StreamExt, stream::FuturesUnordered};
use parking_lot::{Mutex, RwLock};
use scopeguard::defer;
use snafu::{ResultExt, Snafu, ensure};
use tokio_util::sync::CancellationToken;

use super::{
    cache::{self, Cache, CacheLoader},
    compact::{self, COMPACTION_TEMP_EXTENSION, Compactor, DefaultPlanner},
    cursor::array_values::{BoolArray, FloatArray, IntegerArray, StringArray, UnsignedArray},
    file_store::{self, BAD_TSM_FILE_EXTENSION, FileStore, TMP_TSM_FILE_EXTENSION},
    iterator::{
        self, BoolAscendingCursor, BoolCursor, BoolDescendingCursor, BoolIter, BufCursor, Cursor,
        CursorAt, FloatAscendingCursor, FloatCastIntegerCursor, FloatCastUnsignedCursor,
        FloatCursor, FloatDescendingCursor, FloatIter, IntegerAscendingCursor,
        IntegerCastFloatCursor, IntegerCastUnsignedCursor, IntegerCursor, IntegerDescendingCursor,
        IntegerIter, LiteralValueCursor, NoneBoolLiteralValueCursor, NoneFloatLiteralValueCursor,
        NoneIntegerLiteralValueCursor, NoneStringLiteralValueCursor,
        NoneUnsignedLiteralValueCursor, StringAscendingCursor, StringCursor,
        StringDescendingCursor, StringIter, UnsignedAscendingCursor, UnsignedCastFloatCursor,
        UnsignedCastIntegerCursor, UnsignedCursor, UnsignedDescendingCursor, UnsignedIter,
    },
    reader::TSMReader,
    scheduler::{Scheduler, VLogScheduler},
    wal::{DEFAULT_SEGMENT_SIZE, WAL, segment_file_names, vlog_segment_file_names},
    wal_impl,
    writer::IndexEntry,
};
#[cfg(feature = "slow-trace")]
use crate::query::storage_ir::{
    GroupedAggregateProfileMetrics, GroupedBlockEligibility, GroupedPhysicalBlockIdentity,
    GroupedPhysicalScanKind, GroupedRawIntervalKind,
};
#[cfg(feature = "slow-trace")]
use crate::storage::tsm1::block_statistics::DatafusionFloatMinMaxSafety;
use crate::{
    common::slow_trace,
    index::{
        self, ActiveIndex as Index, IndexDeleteGuard, SeriesIterator,
        new_filter_undeleted_series_iterator_adapter,
    },
    query::{
        metadata::TIME_COLUMN_NAME,
        predicate_eval::{ColumnResolution, PreparedStoragePredicate},
        query_iterator::{self, QueryIterator},
        query_point,
        storage_ir::{
            AggregateKind, AggregatePoint, AggregatePointValue, AggregateRequest, AggregateResult,
            AggregateScanMetrics, AggregateState, AggregateValues, ColumnKind, ColumnRef,
            CompareOp, GroupedAggregateGroup, GroupedAggregateKey, GroupedAggregateMetrics,
            GroupedAggregateRequest, GroupedAggregateResult, GroupedAggregateTarget,
            GroupedTagFilter, LogicalOp, MeasurementRef, OutputOrder, RowCountMetrics,
            RowCountRequest, RowCountResult, SeriesProjection, StorageExecutionOptions,
            StoragePredicate, StorageProjection, StorageScan, StorageValueExpr, TimeOrder,
            TimeRange,
        },
    },
    series::{
        measurement_fields::{MeasurementFieldSet, MeasurementFields},
        series_file::SeriesFile,
        series_set::SeriesIDSet,
    },
    storage::{
        EngineOptions,
        limiter::fixed::Fixed,
        tsm1::{
            block_statistics::{BlockStatisticValue, BlockStatistics},
            compact::CompactionGroup,
            data_encoding::{
                encoding::{
                    BLOCK_BOOL, BLOCK_FLOAT64, BLOCK_INTEGER, BLOCK_STRING, BLOCK_UNSIGNED,
                },
                encoding_pools::init_encoding_pools,
            },
            key_cursor::KeyCursor,
            vlog::{
                self, VLogIndex,
                compact::{
                    CompactionGroup as VLogCompactionGroup, Compactor as VLogCompactor,
                    PreparedVLogCompaction, VLogCompactInfo, VLogCompactPlanner,
                },
                default_compact_plan::VLogDefaultPlanner,
                file_store::{self as vlog_file_store, FileStore as VLogFileStore},
                key_cursor::VLogKeyCursor,
                lazy_compact_plan::{VLogLazyCompactConfig, VLogLazyPlanner},
                reader::VLogReader,
                vlog_index_cache::{self, Cache as IndexCache, CacheLoader as IndexCacheLoader},
                vlog_index_wal::WAL as IndexWAL,
            },
        },
    },
};

pub const KEY_FIELD_SEPARATOR_BYTES: &str = KEY_FIELD_SEPARATOR;
pub const EMPTY_BYTES: &str = "";

#[derive(Clone)]
struct EngineScanPlan {
    scan: Arc<StorageScan>,
    vlog_enabled: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeleteSeriesMode {
    Ordinary,
    MeasurementData,
}

impl DeleteSeriesMode {
    fn includes_pending_snapshot(self) -> bool {
        self == Self::MeasurementData
    }

    fn reconciles_metadata(self) -> bool {
        self == Self::Ordinary
    }
}

struct AggregateCandidate {
    reader: Arc<TSMReader>,
    entry: IndexEntry,
}

struct GroupedAggregateCandidate {
    reader: Arc<TSMReader>,
    entry: IndexEntry,
    tag_values: Vec<Option<String>>,
    #[cfg(feature = "slow-trace")]
    interval_scope: Arc<str>,
}

struct GroupedAggregateStorageKeyJob {
    target_index: usize,
    field_type: FieldType,
    candidates: Vec<GroupedAggregateCandidate>,
}

struct GroupedAggregateWorkerPartition {
    index: usize,
    jobs: Vec<GroupedAggregateStorageKeyJob>,
}

type GroupedWorkerGroups = Vec<BTreeMap<GroupedAggregateKey, AggregateState>>;
type GroupedPartitionWorkerResult = Result<(GroupedWorkerGroups, GroupedAggregateMetrics)>;

enum GroupedMultiTargetPartitionedResult {
    NotApplicable,
    Complete(Option<GroupedAggregateResult>),
}

#[cfg(feature = "slow-trace")]
struct GroupedPermitAdmission {
    requested: Instant,
    acquired: Instant,
    immediate: bool,
}

#[cfg(not(feature = "slow-trace"))]
type GroupedPermitAdmission = ();

#[cfg(feature = "slow-trace")]
#[derive(Default)]
struct GroupedParallelJobTracker {
    submitted: AtomicU64,
    started: AtomicU64,
    finished: AtomicU64,
    completed: AtomicU64,
    cancelled: AtomicU64,
    active: AtomicU64,
    active_peak: AtomicU64,
    queued: AtomicU64,
    queued_peak: AtomicU64,
    permit_immediate: AtomicU64,
    permit_wait_us: Mutex<Vec<u64>>,
    permit_hold_us: Mutex<Vec<u64>>,
    queue_wait_us: Mutex<Vec<u64>>,
    execution_us: Mutex<Vec<u64>>,
    finish_times: Mutex<Vec<Instant>>,
}

#[cfg(feature = "slow-trace")]
impl GroupedParallelJobTracker {
    fn submit(
        self: &Arc<Self>,
        diagnostics: Option<&Arc<crate::GroupedExecutionDiagnostics>>,
        partition: usize,
        permit_requested: Instant,
        permit_acquired: Instant,
        permit_immediate: bool,
    ) -> GroupedParallelJobSubmission {
        self.submitted.fetch_add(1, Ordering::Relaxed);
        let queued = self.queued.fetch_add(1, Ordering::Relaxed) + 1;
        self.queued_peak.fetch_max(queued, Ordering::Relaxed);
        let submitted = Instant::now();
        let diagnostics = diagnostics.map(|diagnostics| {
            let index = diagnostics.record_storage_job_submitted(
                partition,
                permit_requested,
                permit_acquired,
                submitted,
            );
            (Arc::clone(diagnostics), index)
        });
        GroupedParallelJobSubmission {
            tracker: Arc::clone(self),
            diagnostics,
            permit_requested,
            permit_acquired,
            submitted,
            permit_immediate,
            pending: true,
        }
    }

    fn record_permit(
        &self,
        permit_requested: Instant,
        permit_acquired: Instant,
        permit_immediate: bool,
    ) {
        if permit_immediate {
            self.permit_immediate.fetch_add(1, Ordering::Relaxed);
        }
        self.permit_wait_us.lock().push(
            permit_acquired
                .saturating_duration_since(permit_requested)
                .as_micros()
                .min(u128::from(u64::MAX)) as u64,
        );
        self.permit_hold_us.lock().push(
            permit_acquired
                .elapsed()
                .as_micros()
                .min(u128::from(u64::MAX)) as u64,
        );
    }
}

#[cfg(feature = "slow-trace")]
struct GroupedParallelJobSubmission {
    tracker: Arc<GroupedParallelJobTracker>,
    diagnostics: Option<(Arc<crate::GroupedExecutionDiagnostics>, usize)>,
    permit_requested: Instant,
    permit_acquired: Instant,
    submitted: Instant,
    permit_immediate: bool,
    pending: bool,
}

#[cfg(feature = "slow-trace")]
impl GroupedParallelJobSubmission {
    fn start(mut self, permit: StorageQueryCpuActivePermit) -> GroupedParallelJobGuard {
        self.pending = false;
        let started = Instant::now();
        if let Some((diagnostics, index)) = &self.diagnostics {
            diagnostics.record_storage_job_started(*index, started);
        }
        self.tracker.queued.fetch_sub(1, Ordering::Relaxed);
        self.tracker.queue_wait_us.lock().push(
            started
                .saturating_duration_since(self.submitted)
                .as_micros()
                .min(u128::from(u64::MAX)) as u64,
        );
        let tracker = Arc::clone(&self.tracker);
        tracker.started.fetch_add(1, Ordering::Relaxed);
        let active = tracker.active.fetch_add(1, Ordering::Relaxed) + 1;
        tracker.active_peak.fetch_max(active, Ordering::Relaxed);
        GroupedParallelJobGuard {
            tracker,
            diagnostics: self.diagnostics.take(),
            permit_requested: self.permit_requested,
            permit_acquired: self.permit_acquired,
            permit_immediate: self.permit_immediate,
            started,
            outcome: GroupedParallelJobOutcome::Failed,
            _permit: permit,
        }
    }
}

#[cfg(feature = "slow-trace")]
impl Drop for GroupedParallelJobSubmission {
    fn drop(&mut self) {
        if self.pending {
            self.tracker.queued.fetch_sub(1, Ordering::Relaxed);
            self.tracker.record_permit(
                self.permit_requested,
                self.permit_acquired,
                self.permit_immediate,
            );
        }
    }
}

#[cfg(feature = "slow-trace")]
enum GroupedParallelJobOutcome {
    Completed,
    Cancelled,
    Failed,
}

#[cfg(feature = "slow-trace")]
struct GroupedParallelJobGuard {
    tracker: Arc<GroupedParallelJobTracker>,
    diagnostics: Option<(Arc<crate::GroupedExecutionDiagnostics>, usize)>,
    permit_requested: Instant,
    permit_acquired: Instant,
    permit_immediate: bool,
    started: Instant,
    outcome: GroupedParallelJobOutcome,
    _permit: StorageQueryCpuActivePermit,
}

#[cfg(feature = "slow-trace")]
impl GroupedParallelJobGuard {
    fn finish(&mut self, result: &GroupedPartitionWorkerResult) {
        self.outcome = match result {
            Ok(_) => GroupedParallelJobOutcome::Completed,
            Err(Error::ScanCancelled) => GroupedParallelJobOutcome::Cancelled,
            Err(_) => GroupedParallelJobOutcome::Failed,
        };
    }
}

#[cfg(feature = "slow-trace")]
impl Drop for GroupedParallelJobGuard {
    fn drop(&mut self) {
        let finished = Instant::now();
        if let Some((diagnostics, index)) = &self.diagnostics {
            diagnostics.record_storage_job_finished(*index, finished);
        }
        self.tracker.active.fetch_sub(1, Ordering::Relaxed);
        self.tracker.finished.fetch_add(1, Ordering::Relaxed);
        self.tracker.execution_us.lock().push(
            finished
                .saturating_duration_since(self.started)
                .as_micros()
                .min(u128::from(u64::MAX)) as u64,
        );
        self.tracker.record_permit(
            self.permit_requested,
            self.permit_acquired,
            self.permit_immediate,
        );
        match self.outcome {
            GroupedParallelJobOutcome::Completed => {
                self.tracker.completed.fetch_add(1, Ordering::Relaxed);
            }
            GroupedParallelJobOutcome::Cancelled => {
                self.tracker.cancelled.fetch_add(1, Ordering::Relaxed);
            }
            GroupedParallelJobOutcome::Failed => {}
        }
        self.tracker.finish_times.lock().push(finished);
    }
}

struct GroupedRowDomainCoverage {
    total_fields: usize,
    covered_fields: usize,
    uncovered_fields: Vec<(String, FieldType)>,
}

impl GroupedRowDomainCoverage {
    fn new(
        row_domain_fields: &[(String, FieldType)],
        covered_target_fields: &HashSet<String>,
    ) -> Self {
        let uncovered_fields = row_domain_fields
            .iter()
            .filter(|(field, _)| !covered_target_fields.contains(field))
            .cloned()
            .collect::<Vec<_>>();
        let covered_fields = row_domain_fields
            .len()
            .saturating_sub(uncovered_fields.len());

        debug_assert_eq!(
            covered_fields + uncovered_fields.len(),
            row_domain_fields.len()
        );
        debug_assert!(
            uncovered_fields
                .iter()
                .all(|(field, _)| !covered_target_fields.contains(field))
        );

        Self {
            total_fields: row_domain_fields.len(),
            covered_fields,
            uncovered_fields,
        }
    }

    #[cfg(feature = "slow-trace")]
    fn record(
        &self,
        profile: &mut GroupedAggregateProfileMetrics,
        precheck_fields: usize,
        elided_fields: usize,
    ) {
        debug_assert_eq!(
            precheck_fields.saturating_add(elided_fields),
            self.uncovered_fields.len()
        );
        profile.row_domain_fields_total = profile
            .row_domain_fields_total
            .saturating_add(self.total_fields as u64);
        profile.row_domain_fields_covered = profile
            .row_domain_fields_covered
            .saturating_add(self.covered_fields as u64);
        profile.row_domain_fields_uncovered = profile
            .row_domain_fields_uncovered
            .saturating_add(self.uncovered_fields.len() as u64);
        profile.row_domain_precheck_fields = profile
            .row_domain_precheck_fields
            .saturating_add(precheck_fields as u64);
        profile.row_domain_precheck_elided_fields = profile
            .row_domain_precheck_elided_fields
            .saturating_add(elided_fields as u64);
        profile.row_domain_precheck_elision_target_group_coverage = profile
            .row_domain_precheck_elision_target_group_coverage
            .saturating_add(elided_fields as u64);
        if precheck_fields == 0 {
            profile.row_domain_precheck_skipped_full_coverage = profile
                .row_domain_precheck_skipped_full_coverage
                .saturating_add(1);
        }
    }
}

struct GroupedRowDomainPrecheck {
    fields: Vec<(String, FieldType)>,
    elided_fields: usize,
}

struct AggregateReaderSet {
    readers: Vec<Arc<TSMReader>>,
}

impl AggregateReaderSet {
    fn new(readers: Vec<Arc<TSMReader>>) -> Self {
        for reader in &readers {
            reader.ref_reader();
        }
        Self { readers }
    }

    fn as_slice(&self) -> &[Arc<TSMReader>] {
        &self.readers
    }
}

impl Drop for AggregateReaderSet {
    fn drop(&mut self) {
        for reader in &self.readers {
            reader.unref_reader();
        }
    }
}

pub(crate) enum DirectTsmBlockValues {
    Integer(Vec<i64>),
    Unsigned(Vec<u64>),
    Float(Vec<f64>),
    Boolean(Vec<bool>),
    String(Vec<String>),
    TimestampOnly,
}

pub(crate) struct DirectTsmBlock {
    pub(crate) timestamps: Vec<i64>,
    pub(crate) values: DirectTsmBlockValues,
    pub(crate) tags: Arc<query_point::Tags>,
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) partial: bool,
}

struct DirectTsmCandidate {
    reader: Arc<TSMReader>,
    entry: IndexEntry,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DirectTsmScanMode {
    Values,
    TimestampOnly,
}

/// A streaming, typed TSM source used only after conservative visibility checks.
pub(crate) struct DirectTsmScan {
    candidates: Vec<DirectTsmCandidate>,
    next_candidate: usize,
    field_type: FieldType,
    mode: DirectTsmScanMode,
    time_range: TimeRange,
    tags: Arc<query_point::Tags>,
    cancellation: Option<crate::query::storage_ir::ScanCancellation>,
    _readers: Arc<AggregateReaderSet>,
    #[cfg(feature = "slow-trace")]
    profile: Option<Arc<crate::query::storage_ir::StorageScanProfile>>,
}

/// The direct field streams for one selected series.
pub(crate) struct DirectTsmSeriesScan {
    pub(crate) scans: Vec<DirectTsmScan>,
    pub(crate) merge_identity: String,
    pub(crate) output_identity: String,
}

/// Physical series that share one canonical tag-set identity.
pub(crate) struct DirectTsmTagSetScan {
    pub(crate) series: Vec<DirectTsmSeriesScan>,
}

/// A stable tag-set and physical-series ordered direct TSM scan plan.
pub(crate) struct DirectTsmScanSet {
    pub(crate) tag_sets: Vec<DirectTsmTagSetScan>,
    pub(crate) executor: ExecutorRef,
}

impl DirectTsmScan {
    fn empty(
        field_type: FieldType,
        mode: DirectTsmScanMode,
        time_range: TimeRange,
        tags: Arc<query_point::Tags>,
        cancellation: Option<crate::query::storage_ir::ScanCancellation>,
        readers: Arc<AggregateReaderSet>,
        #[cfg(feature = "slow-trace")] profile: Option<
            Arc<crate::query::storage_ir::StorageScanProfile>,
        >,
    ) -> Self {
        #[cfg(feature = "slow-trace")]
        if let Some(profile) = &profile {
            profile.record_cursor(0, 0);
        }
        Self {
            candidates: Vec::new(),
            next_candidate: 0,
            field_type,
            mode,
            time_range,
            tags,
            cancellation,
            _readers: readers,
            #[cfg(feature = "slow-trace")]
            profile,
        }
    }

    pub(crate) fn tags(&self) -> Arc<query_point::Tags> {
        Arc::clone(&self.tags)
    }

    pub(crate) fn next_block(&mut self) -> Result<Option<DirectTsmBlock>> {
        while let Some(candidate) = self.candidates.get(self.next_candidate) {
            if self
                .cancellation
                .as_ref()
                .is_some_and(crate::query::storage_ir::ScanCancellation::is_cancelled)
            {
                return Err(Error::ScanCancelled);
            }
            self.next_candidate += 1;

            #[cfg(feature = "slow-trace")]
            let decode_started = Instant::now();

            let expected_block_type = match self.field_type {
                INTEGER => BLOCK_INTEGER,
                UNSIGNED => BLOCK_UNSIGNED,
                FLOAT => BLOCK_FLOAT64,
                BOOL => BLOCK_BOOL,
                STRING => BLOCK_STRING,
                other => {
                    return Err(Error::StringErr {
                        reason: format!("unsupported direct TSM field type {other}"),
                    });
                }
            };
            let (timestamps, values) = if self.mode == DirectTsmScanMode::TimestampOnly {
                (
                    candidate
                        .reader
                        .read_timestamp_array_block_at(&candidate.entry, expected_block_type)
                        .context(TSMReaderSnafu)?,
                    DirectTsmBlockValues::TimestampOnly,
                )
            } else {
                match self.field_type {
                    INTEGER => {
                        let mut array = IntegerArray::default();
                        candidate
                            .reader
                            .read_integer_array_block_at(&candidate.entry, &mut array)
                            .context(TSMReaderSnafu)?;
                        (
                            array.timestamps,
                            DirectTsmBlockValues::Integer(array.values),
                        )
                    }
                    UNSIGNED => {
                        let mut array = UnsignedArray::default();
                        candidate
                            .reader
                            .read_unsigned_array_block_at(&candidate.entry, &mut array)
                            .context(TSMReaderSnafu)?;
                        (
                            array.timestamps,
                            DirectTsmBlockValues::Unsigned(array.values),
                        )
                    }
                    FLOAT => {
                        let mut array = FloatArray::default();
                        candidate
                            .reader
                            .read_float_array_block_at(&candidate.entry, &mut array)
                            .context(TSMReaderSnafu)?;
                        (array.timestamps, DirectTsmBlockValues::Float(array.values))
                    }
                    BOOL => {
                        let mut array = BoolArray::default();
                        candidate
                            .reader
                            .read_bool_array_block_at(&candidate.entry, &mut array)
                            .context(TSMReaderSnafu)?;
                        (
                            array.timestamps,
                            DirectTsmBlockValues::Boolean(array.values),
                        )
                    }
                    STRING => {
                        let mut array = StringArray::default();
                        candidate
                            .reader
                            .read_string_array_block_at(&candidate.entry, &mut array)
                            .context(TSMReaderSnafu)?;
                        (array.timestamps, DirectTsmBlockValues::String(array.values))
                    }
                    other => {
                        return Err(Error::StringErr {
                            reason: format!("unsupported direct TSM field type {other}"),
                        });
                    }
                }
            };

            let value_len = match &values {
                DirectTsmBlockValues::Integer(values) => Some(values.len()),
                DirectTsmBlockValues::Unsigned(values) => Some(values.len()),
                DirectTsmBlockValues::Float(values) => Some(values.len()),
                DirectTsmBlockValues::Boolean(values) => Some(values.len()),
                DirectTsmBlockValues::String(values) => Some(values.len()),
                DirectTsmBlockValues::TimestampOnly => None,
            };
            if value_len.is_some_and(|value_len| timestamps.len() != value_len) {
                return Err(Error::StringErr {
                    reason: format!(
                        "direct TSM block has {} timestamps and {} values",
                        timestamps.len(),
                        value_len.unwrap()
                    ),
                });
            }
            #[cfg(feature = "slow-trace")]
            if let Some(profile) = &self.profile {
                profile.record_block_decode(
                    GroupedPhysicalBlockIdentity::new(
                        candidate.reader.path().to_string_lossy(),
                        candidate.entry.offset,
                        candidate.entry.min_time,
                        candidate.entry.max_time,
                    ),
                    timestamps.len() as u64,
                    u64::from(candidate.entry.size),
                    decode_started
                        .elapsed()
                        .as_micros()
                        .min(u128::from(u64::MAX)) as u64,
                    self.mode == DirectTsmScanMode::TimestampOnly,
                );
            }

            let partial = self.time_range.start_time > candidate.entry.min_time
                || self.time_range.end_time < candidate.entry.max_time;
            let (start, end) = if partial {
                (
                    timestamps.partition_point(|timestamp| *timestamp < self.time_range.start_time),
                    timestamps.partition_point(|timestamp| *timestamp <= self.time_range.end_time),
                )
            } else {
                (0, timestamps.len())
            };
            if start == end {
                continue;
            }

            return Ok(Some(DirectTsmBlock {
                timestamps,
                values,
                tags: Arc::clone(&self.tags),
                start,
                end,
                partial,
            }));
        }
        Ok(None)
    }
}

struct DirectTimestampCursor {
    scan: DirectTsmScan,
    timestamps: Vec<i64>,
    position: usize,
    end: usize,
}

impl DirectTimestampCursor {
    fn new(scan: DirectTsmScan) -> Self {
        Self {
            scan,
            timestamps: Vec::new(),
            position: 0,
            end: 0,
        }
    }

    fn next_timestamp(&mut self, metrics: &mut RowCountMetrics) -> Result<Option<i64>> {
        loop {
            if self.position < self.end {
                let timestamp = self.timestamps[self.position];
                self.position += 1;
                return Ok(Some(timestamp));
            }

            let Some(block) = self.scan.next_block()? else {
                return Ok(None);
            };
            if !matches!(block.values, DirectTsmBlockValues::TimestampOnly) {
                return Err(Error::StringErr {
                    reason: "row count direct scan decoded field values".to_string(),
                });
            }
            metrics.timestamp_only_blocks = metrics
                .timestamp_only_blocks
                .checked_add(1)
                .ok_or_else(|| Error::StringErr {
                    reason: "row count timestamp block metric overflow".to_string(),
                })?;
            metrics.timestamp_only_points = metrics
                .timestamp_only_points
                .checked_add((block.end - block.start) as u64)
                .ok_or_else(|| Error::StringErr {
                    reason: "row count timestamp point metric overflow".to_string(),
                })?;
            self.timestamps = block.timestamps;
            self.position = block.start;
            self.end = block.end;
        }
    }
}

fn count_direct_timestamp_union(
    scans: Vec<DirectTsmScan>,
    metrics: &mut RowCountMetrics,
) -> Result<u64> {
    visit_direct_timestamp_union(scans, metrics, |_| Ok(()))
}

fn visit_direct_timestamp_union(
    scans: Vec<DirectTsmScan>,
    metrics: &mut RowCountMetrics,
    mut visit: impl FnMut(i64) -> Result<()>,
) -> Result<u64> {
    let mut cursors = scans
        .into_iter()
        .map(DirectTimestampCursor::new)
        .collect::<Vec<_>>();
    let mut next = Vec::with_capacity(cursors.len());
    for cursor in &mut cursors {
        next.push(cursor.next_timestamp(metrics)?);
    }

    let mut count = 0_u64;
    loop {
        let Some(timestamp) = next.iter().flatten().copied().min() else {
            return Ok(count);
        };
        count = count.checked_add(1).ok_or_else(|| Error::StringErr {
            reason: "row count overflow".to_string(),
        })?;
        visit(timestamp)?;

        for (cursor, current) in cursors.iter_mut().zip(&mut next) {
            while *current == Some(timestamp) {
                *current = cursor.next_timestamp(metrics)?;
            }
        }
    }
}

fn direct_tsm_candidates_overlap(candidates: &[DirectTsmCandidate]) -> bool {
    let Some(first) = candidates.first() else {
        return false;
    };
    let mut max_time = first.entry.max_time;
    for candidate in &candidates[1..] {
        if candidate.entry.min_time <= max_time {
            return true;
        }
        max_time = max_time.max(candidate.entry.max_time);
    }
    false
}

fn merge_aggregate_intervals(mut intervals: Vec<TimeRange>) -> Vec<TimeRange> {
    intervals.sort_unstable_by_key(|interval| (interval.start_time, interval.end_time));
    let mut merged: Vec<TimeRange> = Vec::with_capacity(intervals.len());
    for interval in intervals {
        if let Some(last) = merged.last_mut()
            && interval.start_time <= last.end_time.saturating_add(1)
        {
            last.end_time = last.end_time.max(interval.end_time);
            continue;
        }
        merged.push(interval);
    }
    merged
}

fn aggregate_conflict_intervals(
    candidates: &[AggregateCandidate],
    query_range: TimeRange,
) -> Vec<TimeRange> {
    let mut ordered = candidates.iter().collect::<Vec<_>>();
    ordered.sort_unstable_by_key(|candidate| {
        (
            candidate.entry.min_time,
            candidate.entry.max_time,
            candidate.reader.generation,
            candidate.reader.seq,
        )
    });

    let mut conflicts = Vec::new();
    for (index, left) in ordered.iter().enumerate() {
        for right in ordered.iter().skip(index + 1) {
            if right.entry.min_time > left.entry.max_time {
                break;
            }
            let start_time = left
                .entry
                .min_time
                .max(right.entry.min_time)
                .max(query_range.start_time);
            let end_time = left
                .entry
                .max_time
                .min(right.entry.max_time)
                .min(query_range.end_time);
            if start_time <= end_time {
                conflicts.push(TimeRange::new(start_time, end_time));
            }
        }
    }
    merge_aggregate_intervals(conflicts)
}

fn aggregate_intervals_intersect(intervals: &[TimeRange], range: TimeRange) -> bool {
    let index = intervals.partition_point(|interval| interval.end_time < range.start_time);
    intervals
        .get(index)
        .is_some_and(|interval| interval.start_time <= range.end_time)
}

fn aggregate_intervals_cover(intervals: &[TimeRange], range: TimeRange) -> bool {
    let index = intervals.partition_point(|interval| interval.start_time <= range.start_time);
    index != 0 && intervals[index - 1].end_time >= range.end_time
}

fn aggregate_intervals_contain(intervals: &[TimeRange], timestamp: i64) -> bool {
    let index = intervals.partition_point(|interval| interval.start_time <= timestamp);
    index != 0 && timestamp <= intervals[index - 1].end_time
}

#[cfg(feature = "slow-trace")]
fn observe_grouped_candidate_eligibility(
    profile: &mut GroupedAggregateProfileMetrics,
    entry: &IndexEntry,
    request: &GroupedAggregateRequest,
    field_type: u8,
    conflict_intervals: &[TimeRange],
) -> Result<()> {
    let first_bucket = request
        .window
        .bucket_start(entry.min_time)
        .map_err(|reason| Error::StringErr {
            reason: reason.to_string(),
        })?;
    let last_bucket = request
        .window
        .bucket_start(entry.max_time)
        .map_err(|reason| Error::StringErr {
            reason: reason.to_string(),
        })?;
    let bucket_count = ((i128::from(last_bucket) - i128::from(first_bucket))
        / i128::from(request.window.duration_ns)
        + 1)
    .clamp(1, i128::from(u64::MAX)) as u64;
    let block_span_ns = (i128::from(entry.max_time) - i128::from(entry.min_time))
        .clamp(0, i128::from(u64::MAX)) as u64;
    profile.candidate_block_span_ns.push(block_span_ns);
    profile.candidate_bucket_count.push(bucket_count);

    let entry_range = TimeRange::new(
        entry.min_time.max(request.time_range.start_time),
        entry.max_time.min(request.time_range.end_time),
    );
    if aggregate_intervals_intersect(conflict_intervals, entry_range) {
        profile.observed_overlap = profile.observed_overlap.saturating_add(1);
    }

    if entry.min_time < request.time_range.start_time {
        profile.layout_query_clips_left = profile.layout_query_clips_left.saturating_add(1);
    }
    if entry.max_time > request.time_range.end_time {
        profile.layout_query_clips_right = profile.layout_query_clips_right.saturating_add(1);
    }
    match bucket_count {
        1 => {
            profile.layout_within_one_bucket = profile.layout_within_one_bucket.saturating_add(1);
        }
        2 => {
            profile.layout_crosses_two_buckets =
                profile.layout_crosses_two_buckets.saturating_add(1);
        }
        _ => {
            profile.layout_crosses_more_than_two_buckets = profile
                .layout_crosses_more_than_two_buckets
                .saturating_add(1);
        }
    }
    if bucket_count > 1 {
        if block_span_ns >= request.window.duration_ns as u64 {
            profile.layout_cross_bucket_span_ge_window =
                profile.layout_cross_bucket_span_ge_window.saturating_add(1);
        } else {
            profile.layout_cross_bucket_span_lt_window =
                profile.layout_cross_bucket_span_lt_window.saturating_add(1);
        }
    }

    let Some(statistics) = entry.statistics else {
        profile.observed_missing_statistics = profile.observed_missing_statistics.saturating_add(1);
        return Ok(());
    };
    if !statistics.supports_block_type(field_type) {
        profile.observed_unsupported_statistics =
            profile.observed_unsupported_statistics.saturating_add(1);
        return Ok(());
    }
    profile.observed_usable_statistics = profile.observed_usable_statistics.saturating_add(1);
    if field_type != crate::models::FLOAT {
        return Ok(());
    }
    if statistics.has_float_infinite_extreme() {
        profile.observed_float_infinity = profile.observed_float_infinity.saturating_add(1);
    }
    let counter = match statistics.datafusion_grouped_float_min_max_safety() {
        DatafusionFloatMinMaxSafety::Safe => &mut profile.observed_float_safe,
        DatafusionFloatMinMaxSafety::Nan => &mut profile.observed_float_nan,
        DatafusionFloatMinMaxSafety::SignedZero => &mut profile.observed_float_signed_zero,
        DatafusionFloatMinMaxSafety::ZeroCrossing => &mut profile.observed_float_zero_crossing,
        DatafusionFloatMinMaxSafety::UnsupportedStatistics => {
            &mut profile.observed_unsupported_statistics
        }
    };
    *counter = counter.saturating_add(1);
    Ok(())
}

#[cfg(feature = "slow-trace")]
fn grouped_float_candidate_reject_reason(
    entry: &IndexEntry,
    field_type: u8,
) -> GroupedBlockEligibility {
    let Some(statistics) = entry.statistics else {
        return GroupedBlockEligibility::MissingStatistics;
    };
    if !statistics.supports_block_type(field_type) {
        return GroupedBlockEligibility::UnsupportedStatistics;
    }
    match statistics.datafusion_grouped_float_min_max_safety() {
        DatafusionFloatMinMaxSafety::Safe => GroupedBlockEligibility::FloatRequestFallback,
        DatafusionFloatMinMaxSafety::UnsupportedStatistics => {
            GroupedBlockEligibility::UnsupportedStatistics
        }
        DatafusionFloatMinMaxSafety::Nan => GroupedBlockEligibility::FloatNan,
        DatafusionFloatMinMaxSafety::SignedZero => GroupedBlockEligibility::FloatSignedZero,
        DatafusionFloatMinMaxSafety::ZeroCrossing => GroupedBlockEligibility::FloatZeroCrossing,
    }
}

#[cfg(feature = "slow-trace")]
fn assert_grouped_eligibility_accounting(metrics: &GroupedAggregateMetrics) {
    debug_assert_eq!(
        metrics.profile.eligibility_stats_used,
        metrics.blocks_statistics_used
    );
    debug_assert_eq!(
        metrics.profile.candidate_block_span_ns.len() as u64,
        metrics.blocks_candidate,
        "every grouped candidate block must contribute one duration sample",
    );
    debug_assert_eq!(
        metrics.profile.candidate_bucket_count.len() as u64,
        metrics.blocks_candidate,
        "every grouped candidate block must contribute one bucket-count sample",
    );
    debug_assert!(
        metrics
            .profile
            .eligibility_accounting_matches(metrics.blocks_candidate),
        "grouped candidate blocks must have exactly one terminal eligibility decision: candidates={}, classified={}",
        metrics.blocks_candidate,
        metrics.profile.eligibility_terminal_total(),
    );
}

#[cfg(feature = "slow-trace")]
fn assert_grouped_raw_execution_accounting(metrics: &GroupedAggregateMetrics) {
    debug_assert!(
        metrics
            .profile
            .raw_point_accounting_matches(metrics.points_raw_decoded),
        "grouped raw points must split into target values and row-domain rows: raw_points={}, target_values={}, row_domain_rows={}",
        metrics.points_raw_decoded,
        metrics.profile.target_values_consumed,
        metrics.profile.row_domain_logical_rows_consumed,
    );
    debug_assert!(
        metrics.profile.physical_decode_accounting_matches(),
        "grouped physical decodes must have exactly one execution role",
    );
    debug_assert!(
        metrics.profile.physical_identity_accounting_matches(),
        "every grouped physical decode must have one stable block identity",
    );
}

impl EngineScanPlan {
    fn strip_name(&self) -> bool {
        self.scan.execution.strip_name
    }

    fn max_series_n(&self) -> i64 {
        self.scan.execution.max_series_n
    }

    fn limit_tag_sets(&self, tag_sets: Vec<index::TagSet>) -> Vec<index::TagSet> {
        let limit = self.scan.execution.series_limit;
        index::limit_tag_sets(tag_sets, limit.limit, limit.offset)
    }

    fn apply_pre_merge_limit(
        &self,
        inputs: Vec<Box<dyn QueryIterator + Sync + Send>>,
    ) -> Vec<Box<dyn QueryIterator + Sync + Send>> {
        let limit = self.scan.execution.point_limit;
        if (limit.limit > 0 || limit.offset > 0) && !self.scan.execution.grouping_matches_dimensions
        {
            return inputs
                .into_iter()
                .map(|input| iterator::new_limit_iter(input.into_any(), limit))
                .collect();
        }
        inputs
    }

    fn merge(
        &self,
        inputs: Vec<Box<dyn QueryIterator + Sync + Send>>,
        scan: Arc<StorageScan>,
    ) -> std::result::Result<Option<Box<dyn QueryIterator + Sync + Send>>, String> {
        query_iterator::try_merge_storage(inputs, scan)
    }

    fn apply_post_merge_limit(
        &self,
        iterator: Box<dyn QueryIterator + Sync + Send>,
    ) -> Box<dyn QueryIterator + Sync + Send> {
        let limit = self.scan.execution.point_limit;
        if limit.limit <= 0 || limit.offset <= 0 {
            return iterator;
        }
        if self.scan.execution.grouping_matches_dimensions {
            iterator::new_limit_iter(iterator.into_any(), limit)
        } else {
            query_iterator::new_limit_iter(iterator.into_any(), limit)
        }
    }
}

// KEY_FIELD_SEPARATOR separates the series key from the field name in the composite key
// that identifies a specific field in series
pub const KEY_FIELD_SEPARATOR: &str = "#!~#";

// DELETE_FLUSH_THRESHOLD is the size in bytes of a batch of series keys to delete.
pub const DELETE_FLUSH_THRESHOLD: usize = 50 * 1024 * 1024;

// Statistics gathered by the engine.
const STAT_CACHE_COMPACTIONS: &str = "CacheCompactions";
const STAT_CACHE_COMPACTIONS_ACTIVE: &str = "CacheCompactionsActive";
const STAT_CACHE_COMPACTION_ERROR: &str = "CacheCompactionErr";
const STAT_CACHE_COMPACTION_DURATION: &str = "CacheCompactionDuration";

const STAT_TSM_LEVEL1_COMPACTIONS: &str = "TsmLevel1Compactions";
const STAT_TSM_LEVEL1_COMPACTIONS_ACTIVE: &str = "TsmLevel1CompactionsActive";
const STAT_TSM_LEVEL1_COMPACTION_ERROR: &str = "TsmLevel1CompactionErr";
const STAT_TSM_LEVEL1_COMPACTION_DURATION: &str = "TsmLevel1CompactionDuration";
const STAT_TSM_LEVEL1_COMPACTION_QUEUE: &str = "TsmLevel1CompactionQueue";

const STAT_TSM_LEVEL2_COMPACTIONS: &str = "TsmLevel2Compactions";
const STAT_TSM_LEVEL2_COMPACTIONS_ACTIVE: &str = "TsmLevel2CompactionsActive";
const STAT_TSM_LEVEL2_COMPACTION_ERROR: &str = "TsmLevel2CompactionErr";
const STAT_TSM_LEVEL2_COMPACTION_DURATION: &str = "TsmLevel2CompactionDuration";
const STAT_TSM_LEVEL2_COMPACTION_QUEUE: &str = "TsmLevel2CompactionQueue";

const STAT_TSM_LEVEL3_COMPACTIONS: &str = "TsmLevel3Compactions";
const STAT_TSM_LEVEL3_COMPACTIONS_ACTIVE: &str = "TsmLevel3CompactionsActive";
const STAT_TSM_LEVEL3_COMPACTION_ERROR: &str = "TsmLevel3CompactionErr";
const STAT_TSM_LEVEL3_COMPACTION_DURATION: &str = "TsmLevel3CompactionDuration";
const STAT_TSM_LEVEL3_COMPACTION_QUEUE: &str = "TsmLevel3CompactionQueue";

const STAT_TSM_OPTIMIZE_COMPACTIONS: &str = "TsmOptimizeCompactions";
const STAT_TSM_OPTIMIZE_COMPACTIONS_ACTIVE: &str = "TsmOptimizeCompactionsActive";
const STAT_TSM_OPTIMIZE_COMPACTION_ERROR: &str = "TsmOptimizeCompactionErr";
const STAT_TSM_OPTIMIZE_COMPACTION_DURATION: &str = "TsmOptimizeCompactionDuration";
const STAT_TSM_OPTIMIZE_COMPACTION_QUEUE: &str = "TsmOptimizeCompactionQueue";

const STAT_TSM_FULL_COMPACTIONS: &str = "TsmFullCompactions";
const STAT_TSM_FULL_COMPACTIONS_ACTIVE: &str = "TsmFullCompactionsActive";
const STAT_TSM_FULL_COMPACTION_ERROR: &str = "TsmFullCompactionErr";
const STAT_TSM_FULL_COMPACTION_DURATION: &str = "TsmFullCompactionDuration";
const STAT_TSM_FULL_COMPACTION_QUEUE: &str = "TsmFullCompactionQueue";

#[derive(Clone)]
struct CompactionCancellation {
    token: CancellationToken,
    sender: Arc<Mutex<Option<crossbeam_channel::Sender<()>>>>,
    receiver: crossbeam_channel::Receiver<()>,
}

impl CompactionCancellation {
    fn new() -> Self {
        let (sender, receiver) = crossbeam_channel::unbounded();
        Self {
            token: CancellationToken::new(),
            sender: Arc::new(Mutex::new(Some(sender))),
            receiver,
        }
    }

    fn cancel(&self) {
        self.token.cancel();
        self.sender.lock().take();
    }

    fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }
}

fn wait_for_tick_or_cancellation<T>(
    ticker: &crossbeam_channel::Receiver<T>,
    quit: &CancellationToken,
    cancellation: &crossbeam_channel::Receiver<()>,
) -> bool {
    if quit.is_cancelled() {
        return false;
    }

    crossbeam_channel::select! {
        recv(cancellation) -> _ => false,
        recv(ticker) -> tick => tick.is_ok() && !quit.is_cancelled(),
    }
}

fn wait_for_compaction_tick<T>(
    ticker: &crossbeam_channel::Receiver<T>,
    quit: &CompactionCancellation,
) -> bool {
    wait_for_tick_or_cancellation(ticker, &quit.token, &quit.receiver)
}

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub fn wait_for_compaction_tick_for_test(
    ticker: &crossbeam_channel::Receiver<()>,
    quit: &CancellationToken,
    cancellation: &crossbeam_channel::Receiver<()>,
) -> bool {
    wait_for_tick_or_cancellation(ticker, quit, cancellation)
}

/// Error type for Engine.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    /// IO error.
    #[snafu(display("{reason}: {source}"))]
    IO {
        reason: String,
        source: std::io::Error,
    },

    /// Point error.
    #[snafu(display("engine point error: {}", source))]
    Point {
        source: crate::models::points::Error,
    },

    /// Index error.
    #[snafu(display("engine index error: {}", source))]
    Index { source: crate::index::Error },

    /// Index set error.
    #[snafu(display("engine index set error: {}", source))]
    IndexSet { source: crate::index::Error },

    /// Index error.
    #[snafu(display("engine series file error: {}", source))]
    SeriesFile {
        source: crate::series::series_file::Error,
    },

    /// WAL error.
    #[snafu(display("engine wal error: {}", source))]
    WAL {
        source: crate::storage::tsm1::wal::Error,
    },

    /// WAL error.
    #[snafu(display("engine index wal error: {}", source))]
    IndexWAL {
        source: crate::storage::tsm1::vlog::vlog_index_wal::Error,
    },

    /// Cache error.
    #[snafu(display("engine cache error: {}", source))]
    Cache {
        source: crate::storage::tsm1::cache::Error,
    },

    /// Cache error.
    #[snafu(display("engine index cache error: {}", source))]
    IndexCache {
        source: crate::storage::tsm1::vlog::vlog_index_cache::Error,
    },

    /// File store error.
    #[snafu(display("engine file store error: {}", source))]
    FileStore {
        source: crate::storage::tsm1::file_store::Error,
    },

    /// VLog file store error.
    #[snafu(display("engine file store error: {}", source))]
    VLogFileStore {
        source: crate::storage::tsm1::vlog::file_store::Error,
    },

    /// Measurement fields error.
    #[snafu(display("engine measurement fields error: {}", source))]
    MeasurementFields {
        source: crate::series::measurement_fields::Error,
    },

    /// TSM reader error.
    #[snafu(display("engine tsm reader error: {}", source))]
    TSMReader {
        source: crate::storage::tsm1::reader::Error,
    },

    // /// FieldSet error.
    // #[snafu(display("engine fieldset error: {}", source))]
    // FieldSet { source: crate::series::series_file::Error },
    /// Compact error.
    #[snafu(display("engine compact error: {}", source))]
    Compact {
        source: crate::storage::tsm1::compact::Error,
    },

    /// Query iterator error.
    #[snafu(display("engine compact error: {}", source))]
    QueryIterator {
        source: crate::query::query_iterator::Error,
    },

    /// Storage-native projection validation error.
    #[snafu(display("invalid storage projection: {reason}"))]
    StorageProjection { reason: String },

    /// Storage-native scan creation was cancelled.
    #[snafu(display("storage scan cancelled"))]
    ScanCancelled,

    // ErrUnknownFieldType is returned when the type of a field cannot be determined.
    #[snafu(display("unknown field type"))]
    ErrUnknownFieldType,

    /// String error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },

    /// The host executor refused required storage work.
    #[snafu(display("engine execution error: {}", source))]
    Execution { source: SpawnError },

    #[snafu(display("accepted engine task did not complete"))]
    TaskTerminated,
}

/// Result type for Engine.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<std::io::Error> for Error {
    fn from(source: std::io::Error) -> Error {
        Error::IO {
            reason: "".to_string(),
            source,
        }
    }
}

pub trait CallbackSeriesIDSet {
    fn for_each_series_id_set(&self, ids: &SeriesIDSet, id_set: &Arc<SeriesIDSet>);
}

pub trait SeriesIDSets {
    fn for_each(
        &self,
        ids: &SeriesIDSet,
        f: &dyn CallbackSeriesIDSet,
    ) -> Result<(), Box<dyn std::error::Error>>;

    fn clone_self(&self) -> Box<dyn SeriesIDSets + Sync + Send>;
}

/// Engine represents a storage engine with compressed blocks.
pub struct Engine {
    aggregate_index: Option<Arc<AggregateIndexContext>>,
    // Cache rotation swaps internal stores, not this handle. Aggregate queries
    // can capture values without waiting for the WAL lifecycle engine guard.
    aggregate_cache: Arc<Cache>,
    id: u64, // shard ID
    path: Arc<PathBuf>,

    index: Arc<Index>,
    sfile: Arc<SeriesFile>,
    fieldset: Arc<MeasurementFieldSet>,

    // wal_enabled determines whether writes to the WAL are enabled.  If this is false,
    // writes will only exist in the cache and can be lost if a snapshot has not occurred.
    wal_enabled: bool,

    vlog_enabled: bool,

    // Controls whether to enabled compactions when the engine is open
    enable_compactions_on_open: bool,

    stats: Arc<EngineStatistics>,

    executor: ExecutorRef,

    compaction_plan: Arc<DefaultPlanner>,

    // Limiter for concurrent compactions.
    compaction_limiter: Option<Arc<Fixed>>,

    vlog_compaction_plan: Option<VLogCompactPlanner>,
    vlog_compaction_limiter: Option<Arc<Fixed>>, // TODO: 和 level compaction 分开？

    // scheduler: Arc<RwLock<Scheduler>>,

    // provides access to the total set of series IDs
    series_id_sets: Option<Box<dyn SeriesIDSets + Sync + Send>>,

    // // cache_flush_memory_size_threshold specifies the minimum size threshold for
    // // the cache when the engine should write a snapshot to a TSM file
    // cache_flush_memory_size_threshold: u64,

    // // CacheFlushWriteColdDuration specifies the length of time after which if
    // // no writes have been committed to the WAL, the engine will write
    // // a snapshot of the cache to a TSM file
    // cache_flush_write_cold_duration: Duration,
    pub inner: Arc<RwLock<Inner>>,
}

pub struct Inner {
    aggregate_index: Option<Arc<AggregateIndexContext>>,
    engine_id: u64,
    executor: ExecutorRef,
    // cache_flush_memory_size_threshold specifies the minimum size threshold for
    // the cache when the engine should write a snapshot to a TSM file
    cache_flush_memory_size_threshold: u64,

    // CacheFlushWriteColdDuration specifies the length of time after which if
    // no writes have been committed to the WAL, the engine will write
    // a snapshot of the cache to a TSM file
    cache_flush_write_cold_duration: Duration,

    // The following group of fields is used to track the state of level compactions within the
    // Engine. The WaitGroup is used to monitor the compaction goroutines, the 'done' channel is
    // used to signal those goroutines to shutdown. Every request to disable level compactions will
    // call 'Wait' on 'wg', with the first goroutine to arrive (levelWorkers == 0 while holding the
    // lock) will close the done channel and re-assign 'nil' to the variable. Re-enabling will
    // decrease 'levelWorkers', and when it decreases to zero, level compactions will be started
    // back up again.
    wg: WaitGroup,                        // waitgroup for active level compaction tasks
    done: Option<CompactionCancellation>, // signal level compactions to stop
    level_workers: i32, // Number of "workers" that expect compactions to be in a disabled state

    snap_done: Option<CancellationToken>, // signal snapshot compactions to stop
    snap_wg: WaitGroup,                   // waitgroup for running snapshot compactions
    #[cfg(test)]
    panic_snapshot_supervisor_body: std::sync::atomic::AtomicBool,

    wal: Arc<WAL>,
    cache: Arc<Cache>,
    compactor: Arc<Compactor>,

    // Serializes snapshot rotation/publication with measurement-wide data deletion.
    snapshot_measurement_drop_gate: Arc<tokio::sync::Mutex<()>>,

    file_store: Arc<FileStore>,

    // vlog
    vlog_compactor: Option<Arc<VLogCompactor>>,
    vlog_inner: Option<Arc<RwLock<VLogInner>>>,

    // The following group of fields is used to track the state of vlog compactions within the
    // Engine.
    vlog_wg: WaitGroup, // waitgroup for active vlog compaction tasks
    vlog_done: Option<CompactionCancellation>, // signal vlog compactions to stop
    vlog_workers: i32,  // Number of "workers" that expect compactions to be in a disabled state
}

pub(super) struct VLogInner {
    aggregate_index: Option<Arc<AggregateIndexContext>>,
    engine_id: u64,
    pub(super) vlog_file_store: Arc<VLogFileStore>,
    pub(crate) vlog_index_wal: Arc<IndexWAL>,
    pub(super) vlog_index_cache: Arc<IndexCache>,
}

struct AggregateIndexContext {
    query_options: crate::storage::AggregateQueryOptions,
    query_tasks: Arc<std::sync::atomic::AtomicUsize>,
    write_epoch: std::sync::atomic::AtomicU64,
    active_writers: std::sync::atomic::AtomicUsize,
    cleanup_pending: std::sync::atomic::AtomicBool,
    cleanup_candidates: parking_lot::Mutex<Option<Arc<vlog::index::delete_batch::DeleteBatch>>>,
    cleanup_recovery: std::sync::atomic::AtomicBool,
    snapshot_wal: parking_lot::Mutex<Option<Vec<String>>>,
    deleting: std::sync::atomic::AtomicBool,
    delete_stripes: [parking_lot::RwLock<()>; 256],
    #[cfg(test)]
    metadata_probes: std::sync::atomic::AtomicUsize,
    io_limiter: Option<crate::storage::limiter::rate::Limiter>,
    max_compaction_input_bytes: u64,
    compaction_target_bytes: u64,
    compaction_write_cold_duration: Duration,
    executor: ExecutorRef,
    store: Arc<vlog::index::store::IndexStore>,
    sfile: Arc<SeriesFile>,
    fields: Arc<MeasurementFieldSet>,
    metadata_path: Arc<PathBuf>,
}

fn aggregate_index_error(error: vlog::index::Error) -> Error {
    Error::StringErr { reason: error.to_string() }
}

impl AggregateIndexContext {
    fn compaction_index_files(&self, files: Arc<VLogFileStore>,
        indexes: &HashMap<String, Vec<VLogIndex>>, aggregates: &vlog::compact::AggregateOutputs)
        -> Result<Vec<vlog::index::writer::IndexFileMeta>> {
        let mut fields = BTreeMap::new();
        for (key, blocks) in indexes {
            let seq = self.sequence(key.as_bytes())?;
            let summaries = aggregates.get(key).ok_or_else(|| Error::StringErr {
                reason: "native compaction did not supply block aggregates".into() })?;
            if summaries.len() != blocks.len() { return Err(Error::StringErr {
                reason: "native block/aggregate counts differ".into() }); }
            let records = blocks.iter().zip(summaries).map(|(block, summary)|
                vlog::index::Record::from_vlog_index(seq, block, **summary))
                .collect::<vlog::index::Result<Vec<_>>>().map_err(aggregate_index_error)?;
            fields.insert(seq, records);
        }
        let dir = files.path().to_path_buf();
        vlog::index::snapshot::write_compaction_files(&dir,
            vlog::index::allocation::OutputGenerations::reserve(files), fields,
            vlog::index::io_budget::IoBudget::new(self.io_limiter.clone(), || false),
            self.compaction_target_bytes).map_err(aggregate_index_error)
    }

    fn key(&self, seq: vlog::index::SeqNo) -> vlog::index::Result<Vec<u8>> {
        let (name, tags) = self.sfile.series(seq.series_id())
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let tags = tags.ok_or(vlog::index::Error::InvalidFormat { reason: "native index references missing series" })?;
        let fields = self.fields.fields(&name)
            .ok_or(vlog::index::Error::InvalidFormat { reason: "native index references missing fields" })?;
        let field = fields.field_keys().into_iter().find(|field|
            fields.ordinal(field.as_bytes()) == Some(seq.field_ordinal()))
            .ok_or(vlog::index::Error::InvalidFormat { reason: "native index references missing field ordinal" })?;
        let mut key = crate::models::points::make_key(&name, &tags);
        key.extend_from_slice(KEY_FIELD_SEPARATOR.as_bytes());
        key.extend_from_slice(field.as_bytes());
        Ok(key)
    }

    fn sequence(&self, key: &[u8]) -> Result<vlog::index::SeqNo> {
        let (series, field) = series_and_file_form_composite_key(key);
        let (name, tags) = crate::models::points::parse_key_bytes(series);
        let id = self.sfile.series_id(&name, &tags);
        let ordinal = self.fields.ordinal(&name, field).ok_or_else(|| Error::StringErr {
            reason: "aggregate index field identity is missing".into(),
        })?;
        vlog::index::SeqNo::new(id, ordinal).map_err(aggregate_index_error)
    }

    fn sync_metadata(&self) -> Result<()> {
        self.sfile.sync_ids()?;
        self.fields.save().context(MeasurementFieldsSnafu)?;
        std::fs::File::open(self.metadata_path.as_path())?.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
static FAIL_ENGINE_WRITE_WAL_AFTER_CACHE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

unsafe impl Send for Inner {}
unsafe impl Sync for Inner {}

impl Inner {
    // Returns true if the Cache is over its flush threshold
    // or if the passed in lastWriteTime is older than the write cold threshold.
    pub fn should_compact_cache(&self, t: Time) -> bool {
        let sz = self.cache.size();
        if sz == 0 {
            return false;
        }

        if sz > self.cache_flush_memory_size_threshold {
            // println!(
            //     "shuuld_compact_cache...2, cache_size: {}, threshold: {}, size-threshold={} MB",
            //     sz,
            //     self.cache_flush_memory_size_threshold,
            //     (sz - self.cache_flush_memory_size_threshold) / 1024 / 1024
            // );
            return true;
        }

        let last_write_time = self.cache.last_write_time();
        if t < last_write_time {
            // Only checked sub can be executed
            return false;
        }

        t - last_write_time > self.cache_flush_write_cold_duration
    }

    /// Returns the time when this shard was last modified.
    pub fn last_modified(&self, wal_enabled: bool) -> time::Time {
        let fs_time = self.file_store.last_modified();

        if wal_enabled && self.wal.last_write_time() > fs_time {
            return self.wal.last_write_time();
        }

        fs_time
    }

    /// Returns the time when this vlog was last modified.
    pub fn vlog_last_modified(&self) -> time::Time {
        self.vlog_inner
            .as_ref()
            .unwrap()
            .read()
            .vlog_file_store
            .last_modified()
    }
}

impl Engine {
    const REINDEX_BATCH_SIZE: usize = 10000;

    /// Return a new instance of Engine.
    pub fn new(
        id: u64,
        index: Arc<Index>,
        path: Arc<PathBuf>,
        wal_path: Arc<PathBuf>,
        index_wal_path: Arc<PathBuf>,
        vlog_path: Arc<PathBuf>,
        sfile: Arc<SeriesFile>,
        opt: EngineOptions,
        // vlog_opt: VLogOptions,
    ) -> Result<Self> {
        let executor = Arc::clone(&opt.executor);
        if opt.agg_index_enabled && !opt.vlog_enabled {
            return Err(Error::StringErr { reason: "aggregate index requires VLog mode".into() });
        }
        vlog::index::store::IndexStore::check_mode(&vlog_path, opt.agg_index_enabled)
            .map_err(aggregate_index_error)?;
        if opt.agg_index_enabled {
            for directory in [&path, &index_wal_path] {
                if !directory.exists() { continue; }
                for entry in fs::read_dir(directory.as_path())? {
                    let entry = entry?;
                    let p = entry.path();
                    if p.extension().is_some_and(|ext| ext == "tsm" || ext == "cmp")
                        || (p.extension().is_some_and(|ext| ext == "wal") && entry.metadata()?.len() > 0
                            && !vlog_path.join("agg-index.mode").exists()) {
                        return Err(Error::StringErr { reason: "legacy storage files cannot be opened in aggregate index mode".into() });
                    }
                }
            }
        }
        fs::create_dir_all(path.as_path())?;
        fs::create_dir_all(vlog_path.as_path())?;
        let permissions = fs::Permissions::from_mode(0o777); // 'o' signifies octal
        fs::set_permissions(path.as_path(), permissions)?;
        // ensure the directory creation is actually fsync'd so that when we create files there
        // we don't lose them (see: https://www.usenix.org/system/files/conference/osdi14/osdi14-paper-pillai.pdf)
        std::fs::File::open(path.as_path())
            .expect("should be able to open just-created wal directory")
            .sync_all()
            .expect("fsync failure");

        // Fill encode pools and decoder pools first.
        init_encoding_pools();

        // Finish pending WAL cleanup before a WAL writer can reuse segment IDs.
        let aggregate_index = if opt.agg_index_enabled {
            if opt.agg_index_compaction_max_input_bytes == 0
                || !(16384..=(2 * 1024 * 1024 * 1024 - 16 * 1024 * 1024))
                    .contains(&opt.agg_index_compaction_target_bytes) {
                return Err(Error::StringErr { reason: "invalid aggregate compaction byte budget".into() });
            }
            if fs::read_dir(vlog_path.as_path())?.any(|entry| entry.is_ok_and(|entry|
                entry.path().extension().is_some_and(|ext| ext == "commit" || ext == "installed" || ext == "replace" || ext == "checkpoint")))
                && !path.join("fields.idx").is_file() {
                return Err(Error::StringErr { reason: "committed aggregate index requires persisted field identities".into() });
            }
            let fields = Arc::new(MeasurementFieldSet::new(path.join("fields.idx"))
                .context(MeasurementFieldsSnafu)?);
            let store = vlog::index::store::IndexStore::open_native(&vlog_path, &wal_path).map_err(aggregate_index_error)?;
            store.cache.set_budget(opt.agg_index_cache_bytes);
            Some(Arc::new(AggregateIndexContext {
                query_options: opt.agg_query.clone(),
                query_tasks: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                write_epoch: std::sync::atomic::AtomicU64::new(0),
                active_writers: std::sync::atomic::AtomicUsize::new(0),
                cleanup_pending: std::sync::atomic::AtomicBool::new(true),
                cleanup_candidates: parking_lot::Mutex::new(None),
                cleanup_recovery: std::sync::atomic::AtomicBool::new(path.join("agg-metadata.cleanup").exists()),
                snapshot_wal: parking_lot::Mutex::new(None),
                deleting: std::sync::atomic::AtomicBool::new(false),
                delete_stripes: std::array::from_fn(|_| parking_lot::RwLock::new(())),
                #[cfg(test)]
                metadata_probes: std::sync::atomic::AtomicUsize::new(0),
                io_limiter: opt.vlog_compaction_throughput_limiter.clone()
                    .or_else(|| opt.compaction_throughput_limiter.clone()),
                max_compaction_input_bytes: opt.agg_index_compaction_max_input_bytes,
                compaction_target_bytes: opt.agg_index_compaction_target_bytes,
                compaction_write_cold_duration: opt.vlog_config.vlog_compact_full_write_cold_duration,
                executor: Arc::clone(&executor),
                store,
                sfile: Arc::clone(&sfile), fields, metadata_path: Arc::clone(&path),
            }))
        } else { None };

        let wal = if opt.vlog_enabled {
            Arc::new(
                WAL::new_vlog_wal_with_executor(
                    wal_path,
                    DEFAULT_SEGMENT_SIZE,
                    opt.vlog_config.max_wal_writers,
                    opt.vlog_config.wal_fsync_delay,
                    Arc::clone(&executor),
                )
                .context(WALSnafu)?,
            )
        } else {
            Arc::new(
                WAL::new_tsm_wal_with_executor(
                    wal_path,
                    DEFAULT_SEGMENT_SIZE,
                    opt.config.wal_fsync_delay,
                    Arc::clone(&executor),
                )
                .context(WALSnafu)?,
            )
        };

        let file_store = Arc::new(FileStore::new_with_executor(
            Arc::clone(&path),
            Some(opt.open_limiter.clone()),
            Arc::clone(&executor),
        ));

        let cache = Arc::new(Cache::new_with_executor(
            opt.config.cache_max_memory_size as u64,
            Arc::clone(&executor),
        ));
        let vlog_index_cache = Arc::new(IndexCache::new_with_executor(
            opt.vlog_config.index_cache_max_memory_size as u64,
            Arc::clone(&executor),
        ));
        let vlog_inner = if opt.vlog_enabled {
            Some(Arc::new(RwLock::new(VLogInner {
                aggregate_index: aggregate_index.clone(),
                engine_id: id,
                vlog_file_store: Arc::new(
                    VLogFileStore::new_with_executor(
                        Arc::clone(&vlog_path),
                        Some(opt.open_limiter),
                        Arc::clone(&vlog_index_cache),
                        Arc::clone(&executor),
                    )
                    .context(VLogFileStoreSnafu)?,
                ),
                vlog_index_wal: Arc::new(
                    IndexWAL::new_with_executor(
                        index_wal_path,
                        DEFAULT_SEGMENT_SIZE,
                        opt.vlog_config.wal_fsync_delay,
                        Arc::clone(&executor),
                    )
                    .context(IndexWALSnafu)?,
                ),
                vlog_index_cache: Arc::clone(&vlog_index_cache),
            })))
        } else {
            None
        };

        let compactor = if opt.vlog_enabled {
            if let Some(context) = &aggregate_index {
                let weak = Arc::downgrade(context);
                context.store.bind_native(vlog_inner.as_ref().unwrap(), Arc::new(move |seq| {
                    weak.upgrade().ok_or(vlog::index::Error::InvalidFormat { reason: "aggregate engine is closed" })?.key(seq)
                }));
            }
            let vlog_file_store = Some(Arc::clone(
                &vlog_inner.as_ref().unwrap().read().vlog_file_store,
            ));
            Arc::new(Compactor::new_with_executor(
                Arc::clone(&path),
                Arc::clone(&vlog_path),
                Arc::clone(&file_store),
                vlog_file_store,
                opt.compaction_throughput_limiter,
                opt.vlog_enabled,
                &if opt.agg_index_enabled {
                    let mut config = opt.vlog_config.clone();
                    config.min_vlog_size = 0;
                    config
                } else { opt.vlog_config.clone() },
                Arc::clone(&executor),
            ))
        } else {
            Arc::new(Compactor::new_with_executor(
                Arc::clone(&path),
                Arc::clone(&vlog_path),
                Arc::clone(&file_store),
                None,
                opt.compaction_throughput_limiter,
                opt.vlog_enabled,
                &opt.vlog_config,
                Arc::clone(&executor),
            ))
        };

        let vlog_compactor = if opt.vlog_enabled {
            let vlog_file_store = Some(Arc::clone(
                &vlog_inner.as_ref().unwrap().read().vlog_file_store,
            ));
            Some(Arc::new(VLogCompactor::new_with_executor(
                Arc::clone(&vlog_path),
                vlog_file_store,
                opt.vlog_compaction_throughput_limiter,
                &opt.vlog_config,
                Arc::clone(&executor),
            )))
        } else {
            None
        };

        let compaction_plan = Arc::new(DefaultPlanner::new(
            Arc::clone(&file_store),
            opt.config.compact_full_write_cold_duration,
        ));

        let stats = Arc::new(EngineStatistics::default());

        let vlog_compaction_plan = if opt.vlog_enabled {
            match opt.vlog_compact_plan.as_str() {
                "default" => {
                    let vlog_file_store =
                        Arc::clone(&vlog_inner.as_ref().unwrap().read().vlog_file_store);
                    Some(VLogCompactPlanner::VLogDefaultPlanner(Arc::new(
                        VLogDefaultPlanner::new(
                            vlog_file_store,
                            opt.vlog_config.vlog_compact_full_write_cold_duration,
                        ),
                    )))
                }
                "lazy" => {
                    let vlog_file_store =
                        Arc::clone(&vlog_inner.as_ref().unwrap().read().vlog_file_store);
                    let config = VLogLazyCompactConfig {
                        vlog_compact_level_1_min_files: opt
                            .vlog_config
                            .vlog_compact_level_1_min_files,
                        vlog_compact_level_2_min_files: opt
                            .vlog_config
                            .vlog_compact_level_2_min_files,
                        vlog_compact_level_3_min_files: opt
                            .vlog_config
                            .vlog_compact_level_3_min_files,
                        vlog_compact_full_min_files: opt.vlog_config.vlog_compact_full_min_files,
                        vlog_level_1_max_size_per_compact: opt
                            .vlog_config
                            .vlog_level_1_max_size_per_compact,
                        vlog_level_2_max_size_per_compact: opt
                            .vlog_config
                            .vlog_level_2_max_size_per_compact,
                        vlog_level_3_max_size_per_compact: opt
                            .vlog_config
                            .vlog_level_3_max_size_per_compact,
                        vlog_level_4_max_size_per_compact: opt
                            .vlog_config
                            .vlog_level_4_max_size_per_compact,
                        compact_full_write_cold_duration: opt
                            .vlog_config
                            .vlog_compact_full_write_cold_duration,
                    };
                    Some(VLogCompactPlanner::VLogLazyPlanner(Arc::new(
                        VLogLazyPlanner::new(
                            Arc::clone(&index),
                            vlog_file_store,
                            vlog_index_cache,
                            config,
                            Arc::clone(&stats),
                            opt.vlog_config.vlog_compact_prioritize_lru_keys,
                            opt.vlog_config.vlog_compact_uncached_keys,
                        ),
                    )))
                }
                _ => panic!("invalid vlog compact plan type"),
            }
        } else {
            None
        };

        let inner = Inner {
            aggregate_index: aggregate_index.clone(),
            engine_id: id,
            executor: Arc::clone(&executor),
            wg: WaitGroup::new(),
            done: None,
            level_workers: 0,
            snap_done: None,
            snap_wg: WaitGroup::new(),
            #[cfg(test)]
            panic_snapshot_supervisor_body: std::sync::atomic::AtomicBool::new(false),

            wal,
            cache,
            compactor,
            snapshot_measurement_drop_gate: Arc::new(tokio::sync::Mutex::new(())),
            vlog_compactor,

            cache_flush_memory_size_threshold: opt.config.cache_snapshot_memory_size as u64,
            cache_flush_write_cold_duration: opt.config.cache_snapshot_write_cold_duration,

            file_store,
            vlog_inner,

            vlog_wg: WaitGroup::new(),
            vlog_done: None,
            vlog_workers: 0,
        };

        Ok(Self {
            aggregate_index,
            aggregate_cache: Arc::clone(&inner.cache),
            id,
            path,

            index,
            sfile,
            fieldset: Arc::new(MeasurementFieldSet::default()),

            wal_enabled: opt.wal_enabled,
            vlog_enabled: opt.vlog_enabled,

            enable_compactions_on_open: true,

            stats,

            executor,

            compaction_plan,
            compaction_limiter: opt.compaction_limiter,

            vlog_compaction_plan,
            vlog_compaction_limiter: opt.vlog_compaction_limiter,

            series_id_sets: opt.series_id_sets,

            // writers,
            // scheduler,
            inner: Arc::new(RwLock::new(inner)),
        })
    }

    #[cfg(any(test, feature = "benchmarking"))]
    pub fn cache(&self) -> Arc<Cache> {
        Arc::clone(&self.inner.read().cache)
    }

    #[cfg(test)]
    pub fn file_store_keys(&self) -> HashMap<String, u8> {
        self.inner.read().file_store.keys()
    }

    // Open opens and initializes the engine.
    pub fn open(&mut self) -> Result<()> {
        let started = std::time::Instant::now();
        tracing::info!(
            target: "reefsdb::tsdb::engine",
            engine_id = self.id,
            shard_id = self.id,
            path = %self.path.display(),
            wal_enabled = self.wal_enabled,
            vlog_enabled = self.vlog_enabled,
            "engine opening"
        );
        let result = self.open_inner();
        match &result {
            Ok(()) => tracing::info!(
                target: "reefsdb::tsdb::engine",
                engine_id = self.id,
                shard_id = self.id,
                duration_ms = started.elapsed().as_millis() as u64,
                "engine opened"
            ),
            Err(error) => tracing::error!(
                target: "reefsdb::tsdb::engine",
                engine_id = self.id,
                shard_id = self.id,
                path = %self.path.display(),
                error = %error,
                duration_ms = started.elapsed().as_millis() as u64,
                "engine open failed"
            ),
        }
        result
    }

    fn open_inner(&mut self) -> Result<()> {
        // fs::create_dir_all(self.path.as_path())?;
        // let permissions = fs::Permissions::from_mode(0o777); // 'o' signifies octal
        // fs::set_permissions(self.path.as_path(), permissions)?;
        // // ensure the directory creation is actually fsync'd so that when we create files there
        // // we don't lose them (see: https://www.usenix.org/system/files/conference/osdi14/osdi14-paper-pillai.pdf)
        // std::fs::File::open(self.path.as_path())
        //     .expect("should be able to open just-created wal directory")
        //     .sync_all()
        //     .expect("fsync failure");

        self.cleanup()?;

        let fields = if let Some(context) = &self.aggregate_index {
            Arc::clone(&context.fields)
        } else {
            Arc::new(MeasurementFieldSet::new(self.path.join("fields.idx"))
                .context(MeasurementFieldsSnafu)?)
        };

        let inner = self.inner.write();
        self.fieldset = Arc::clone(&fields);
        self.index.set_field_set(fields);

        // Note: WAL has opened when create wal.

        inner.file_store.open().context(FileStoreSnafu)?;

        if self.wal_enabled {
            let recovery_started = std::time::Instant::now();
            tracing::info!(
                target: "reefsdb::tsdb::wal",
                engine_id = self.id,
                shard_id = self.id,
                path = %inner.wal.path().display(),
                "WAL recovery started"
            );
            if self.vlog_enabled {
                Self::reload_vlog_cache(&inner.cache, inner.wal.path())?;
            } else {
                Self::reload_cache(&inner.cache, inner.wal.path())?;
            }
            tracing::info!(
                target: "reefsdb::tsdb::wal",
                engine_id = self.id,
                shard_id = self.id,
                cache_bytes = inner.cache.size(),
                duration_ms = recovery_started.elapsed().as_millis() as u64,
                "WAL recovery complete"
            );
        }

        if self.vlog_enabled {
            let recovery_started = std::time::Instant::now();
            let vlog_inner = inner.vlog_inner.as_ref().unwrap().read();
            tracing::info!(
                target: "reefsdb::tsdb::vlog",
                engine_id = self.id,
                shard_id = self.id,
                path = %vlog_inner.vlog_index_wal.path().display(),
                "VLog index recovery started"
            );
            Self::reload_index_cache(
                &vlog_inner.vlog_index_cache,
                vlog_inner.vlog_index_wal.path(),
            )?;
            vlog_inner
                .vlog_file_store
                .open()
                .context(VLogFileStoreSnafu)?;
            tracing::info!(
                target: "reefsdb::tsdb::vlog",
                engine_id = self.id,
                shard_id = self.id,
                duration_ms = recovery_started.elapsed().as_millis() as u64,
                "VLog recovery complete"
            );
        }

        inner.compactor.open();
        drop(inner);

        if let Some(context) = &self.aggregate_index {
            let (cache, store) = {
                let inner = self.inner.read();
                (Arc::clone(&inner.cache), Arc::clone(&inner.vlog_inner.as_ref().unwrap().read().vlog_file_store))
            };
            context.store.recover_native().map_err(aggregate_index_error)?;
            store.restore_generation(context.store.next_generation());
            let mut recovered = Vec::new();
            cache.apply_entry_fn::<_, Error>(|key, entry| {
                recovered.push((key, entry.reefs_ql_type().context(CacheSnafu)?));
                Ok(())
            })?;
            for (key, typ) in recovered {
                self.add_to_index_from_key(&mut [key.as_slice()], &[typ])?;
                cache.set_sequence(&key, context.sequence(&key)?.get()).context(CacheSnafu)?;
            }
            context.sync_metadata()?;
            self.reconcile_aggregate_metadata()?;
        }

        if self.enable_compactions_on_open {
            self.set_compactions_enabled(true);
        }

        Ok(())
    }

    // SetEnabled sets whether the engine is enabled.
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enable_compactions_on_open = enabled;
        self.set_compactions_enabled(enabled)
    }

    // SetCompactionsEnabled enables compactions on the engine.  When disabled
    // all running compactions are aborted and new compactions stop running.
    pub fn set_compactions_enabled(&self, enabled: bool) {
        tracing::info!(
            target: "reefsdb::tsdb::compaction",
            engine_id = self.id,
            shard_id = self.id,
            enabled,
            "engine compactions changed"
        );
        if enabled {
            self.enable_snapshot_compactions();
            self.enable_level_compactions(false);
            if self.vlog_enabled {
                self.enable_vlog_compactions(false);
            }
        } else {
            self.disable_snapshot_compactions();
            self.disable_level_compactions(false);
            if self.vlog_enabled {
                self.disable_vlog_compactions(false);
            }
        }
    }

    // enable_level_compactions will request that level compactions start back up again
    //
    // 'wait' signifies that a corresponding call to disable_level_compactions(true) was made at some
    // point, and the associated task that required disabled compactions is now complete
    fn enable_level_compactions(&self, wait: bool) {
        if self.aggregate_index.is_some() {
            self.enable_aggregate_compactions(wait);
            return;
        }
        // If we don't need to wait, see if we're already enabled
        if !wait {
            let inner = self.inner.read();
            if inner.done.is_some() {
                // e.done != nil，表示还有compaction goruntine正在进行
                return;
            }
        }

        let mut inner = self.inner.write();
        if wait {
            inner.level_workers -= 1;
        }
        if inner.level_workers != 0 || inner.done.is_some() {
            // e.done != nil，表示还有compaction goruntine正在进行
            // still waiting on more workers or already enabled
            return;
        }

        // last one to enable, start things back up
        inner.compactor.enable_compactions();
        inner.done = Some(CompactionCancellation::new());
        let wg = WaitGroup::new();
        wg.add(1);
        inner.wg = wg;
        let wg = inner.wg.clone();
        drop(inner);

        let stats = Arc::clone(&self.stats);
        let inner = Arc::clone(&self.inner);
        let wal_enabled = self.wal_enabled;
        let compaction_plan = Arc::clone(&self.compaction_plan);
        let compaction_limiter = self.compaction_limiter.clone();
        let scheduler = match &self.compaction_limiter {
            Some(limiter) => Scheduler::new(Arc::clone(&self.stats), limiter.capacity()),
            None => Scheduler::new(Arc::clone(&self.stats), 0),
        };
        let task_wg = wg.clone();
        let cancelled_wg = wg.clone();
        let (registration, completion) = accepted_task_guard(move || cancelled_wg.done());
        match self.executor.spawn_blocking_compact(move || {
            Self::compact(
                compaction_plan,
                compaction_limiter,
                scheduler,
                wal_enabled,
                inner,
                stats,
                task_wg.clone(),
            );
            completion.complete_with(|| task_wg.done());
        }) {
            Ok(_) => registration.accepted(),
            Err(error) => {
                registration.rejected();
                wg.done();
                let mut inner = self.inner.write();
                inner.done = None;
                inner.compactor.disable_compactions();
                tracing::error!(
                    target: "reefsdb::tsdb::engine",
                    engine_id = self.id,
                    error = %error,
                    "level compaction worker scheduling failed"
                );
            }
        }
    }

    // disable_level_compactions will stop level compactions before returning.
    //
    // If 'wait' is set to true, then a corresponding call to enable_level_compactions(true) will be
    // required before level compactions will start back up again.
    fn disable_level_compactions(&self, wait: bool) {
        let mut inner = self.inner.write();

        let old = inner.level_workers;
        if wait {
            inner.level_workers += 1;
        }

        // Hold onto the current done channel so we can wait on it if necessary
        let wait = inner.done.clone();
        let wg = inner.wg.clone();

        if old == 0 && inner.done.is_some() {
            // It's possible we have closed the done channel and released the lock and another
            // goroutine has attempted to disable compactions.  We're current in the process of
            // disabling them so check for this and wait until the original completes.
            let done = inner.done.as_ref().unwrap();
            // select! {
            //     recv(done.receiver()) -> _ => return,
            //    default => {},
            // }
            if done.is_cancelled() {
                return;
            }

            // Prevent new compactions from starting
            inner.compactor.disable_compactions();

            // Stop all background compaction tasks
            done.cancel();
            drop(inner);
            wg.wait(); // all compaction tasks have done
            // Signal that all tasks have exited
            let mut inner = self.inner.write();
            inner.done = None; // note that all compaction tasks have done
            return;
        }
        drop(inner);
        // Compaction were already disabled.
        if wait.is_none() {
            return;
        }
        // We were not the first caller to disable compactions and they were in the process
        // of being disabled.  Wait for them to complete before returning.
        // let _ = wait.as_ref().unwrap().recv();
        loop {
            if wait.as_ref().unwrap().is_cancelled() {
                break;
            }
        }
        wg.wait();
    }

    fn enable_snapshot_compactions(&self) {
        // Check if already enabled under read lock
        {
            let inner = self.inner.read();
            if inner.snap_done.is_some() {
                return;
            }
        }

        // Check again under write lock
        let mut inner = self.inner.write();
        if inner.snap_done.is_some() {
            return;
        }

        inner.compactor.enable_snapshots();
        inner.snap_done = Some(CancellationToken::new());
        let wg = WaitGroup::new();
        wg.add(1);
        inner.snap_wg = wg;
        let wg = inner.snap_wg.clone();
        drop(inner);

        let stats = Arc::clone(&self.stats);
        let inner = Arc::clone(&self.inner);
        let wal_enabled = self.wal_enabled;
        let executor = Arc::clone(&self.executor);
        let task_wg = wg.clone();
        let cancelled_wg = wg.clone();
        let (registration, completion) = accepted_task_guard(move || cancelled_wg.done());
        match self.executor.spawn_compact(async move {
            #[cfg(test)]
            if inner
                .read()
                .panic_snapshot_supervisor_body
                .swap(false, Ordering::SeqCst)
            {
                panic!("injected snapshot supervisor production-body panic");
            }
            Self::compact_cache(wal_enabled, inner, stats, task_wg.clone(), executor).await;
            completion.complete_with(|| task_wg.done());
        }) {
            Ok(_) => registration.accepted(),
            Err(error) => {
                registration.rejected();
                wg.done();
                let mut inner = self.inner.write();
                inner.snap_done = None;
                inner.compactor.disable_snapshots();
                tracing::error!(
                    target: "reefsdb::tsdb::engine",
                    engine_id = self.id,
                    error = %error,
                    "snapshot worker scheduling failed"
                );
            }
        }
    }

    fn disable_snapshot_compactions(&self) {
        let inner = self.inner.write();
        if inner.snap_done.is_none() {
            return;
        }

        // We may be in the process of stopping snapshots.  See if the channel
        // was closed.
        let snap_done = inner.snap_done.as_ref().unwrap();
        // select! {
        //     recv(snap_done.receiver()) -> _ => return,
        //     default => {},
        // }
        if snap_done.is_cancelled() {
            return;
        }

        // first one here, disable and wait for completion
        snap_done.cancel();
        inner.compactor.disable_snapshots();
        let wg = inner.snap_wg.clone();
        drop(inner);

        // Wait for the snapshot goroutine to exit.
        wg.wait();

        // Signal that the tasks are exit and everything is stopped by setting snap_done to None.
        let mut inner = self.inner.write();
        inner.snap_done = None;
        drop(inner);

        // If the cache is empty, free up its resources as well.
        let inner = self.inner.read();
        if inner.cache.size() == 0 {
            inner.cache.free();
        }
    }

    // enable_vlog_compactions will request that vlog compactions start back up again
    //
    // 'wait' signifies that a corresponding call to disable_vlog_compactions(true) was made at some
    // point, and the associated task that required disabled compactions is now complete
    fn enable_vlog_compactions(&self, wait: bool) {
        // If we don't need to wait, see if we're already enabled
        if !wait {
            let inner = self.inner.read();
            if inner.vlog_done.is_some() {
                // e.done != nil，表示还有compaction goruntine正在进行
                return;
            }
        }

        let mut inner = self.inner.write();
        if wait {
            inner.vlog_workers -= 1;
        }
        if inner.vlog_workers != 0 || inner.vlog_done.is_some() {
            // e.done != nil，表示还有compaction goruntine正在进行
            // still waiting on more workers or already enabled
            return;
        }

        // last one to enable, start things back up
        inner
            .vlog_compactor
            .as_ref()
            .unwrap()
            .enable_vlog_compactions();
        inner.vlog_done = Some(CompactionCancellation::new());
        let wg = WaitGroup::new();
        wg.add(1);
        inner.vlog_wg = wg;
        let wg = inner.vlog_wg.clone();
        drop(inner);

        let stats = Arc::clone(&self.stats);
        let inner = Arc::clone(&self.inner);
        let compaction_plan = self.vlog_compaction_plan.as_ref().unwrap().clone();
        let compaction_limiter = self.vlog_compaction_limiter.clone();
        let scheduler = match &self.vlog_compaction_limiter {
            Some(limiter) => VLogScheduler::new(Arc::clone(&self.stats), limiter.capacity()),
            None => VLogScheduler::new(Arc::clone(&self.stats), 0),
        };
        let task_wg = wg.clone();
        let cancelled_wg = wg.clone();
        let (registration, completion) = accepted_task_guard(move || cancelled_wg.done());
        match self.executor.spawn_compact(async move {
            Self::compact_vlog(
                compaction_plan,
                compaction_limiter,
                scheduler,
                inner,
                stats,
                task_wg.clone(),
            )
            .await;
            completion.complete_with(|| task_wg.done());
        }) {
            Ok(_) => registration.accepted(),
            Err(error) => {
                registration.rejected();
                wg.done();
                let mut inner = self.inner.write();
                inner.vlog_done = None;
                inner
                    .vlog_compactor
                    .as_ref()
                    .unwrap()
                    .disable_vlog_compactions();
                tracing::error!(
                    target: "reefsdb::tsdb::engine",
                    engine_id = self.id,
                    error = %error,
                    "VLog compaction worker scheduling failed"
                );
            }
        }
    }

    // disable_vlog_compactions will stop vlog compactions before returning.
    //
    // If 'wait' is set to true, then a corresponding call to enable_vlog_compactions(true) will be
    // required before vlog compactions will start back up again.
    fn disable_vlog_compactions(&self, wait: bool) {
        let mut inner = self.inner.write();
        let old = inner.vlog_workers;
        if wait {
            inner.vlog_workers += 1;
        }

        // Hold onto the current done channel so we can wait on it if necessary
        let vlog_wait = inner.vlog_done.clone();
        let vlog_wg = inner.vlog_wg.clone();

        if old == 0 && inner.vlog_done.is_some() {
            // It's possible we have closed the done channel and released the lock and another
            // goroutine has attempted to disable compactions.  We're current in the process of
            // disabling them so check for this and wait until the original completes.
            let vlog_done = inner.vlog_done.as_ref().unwrap();
            // select! {
            //     recv(done.receiver()) -> _ => return,
            //    default => {},
            // }
            if vlog_done.is_cancelled() {
                return;
            }

            // Prevent new compactions from starting
            inner
                .vlog_compactor
                .as_ref()
                .unwrap()
                .disable_vlog_compactions();

            // Stop all background compaction tasks
            vlog_done.cancel();
            drop(inner);

            vlog_wg.wait(); // all compaction tasks have done
            // Signal that all tasks have exited
            let mut inner = self.inner.write();
            inner.vlog_done = None; // note that all compaction tasks have done

            return;
        }
        drop(inner);
        // Compaction were already disabled.
        if vlog_wait.is_none() {
            return;
        }
        // We were not the first caller to disable compactions and they were in the process
        // of being disabled.  Wait for them to complete before returning.
        // let _ = wait.as_ref().unwrap().recv();
        loop {
            if vlog_wait.as_ref().unwrap().is_cancelled() {
                break;
            }
        }
        vlog_wg.wait();
    }

    // schedule_full_compaction will force the engine to fully compact all data stored.
    // This will cancel and running compactions and snapshot any data in the cache to
    // TSM files.  This is an expensive operation.
    pub fn schedule_full_compaction(&self) -> Result<()> {
        // Snapshot any data in the cache
        Self::write_snapshot(self.wal_enabled, Arc::clone(&self.inner))?;

        // Cancel running compactions
        self.set_compactions_enabled(false);

        // Ensure compactions are restarted

        // Force the planner to only create a full plan.
        self.compaction_plan.force_full();

        self.set_compactions_enabled(true);

        Ok(())
    }

    // Returns true if the Cache is over its flush threshold
    // or if the passed in lastWriteTime is older than the write cold threshold.
    pub fn should_compact_cache(&self, t: Time) -> bool {
        let inner = self.inner.read();
        let sz = inner.cache.size();
        if sz == 0 {
            return false;
        }

        if sz > inner.cache_flush_memory_size_threshold {
            // println!(
            //     "shuuld_compact_cache, cache_size: {}, threshold: {}, size-threshold={} MB",
            //     sz,
            //     inner.cache_flush_memory_size_threshold,
            //     (sz - inner.cache_flush_memory_size_threshold) / 1024 / 1024
            // );
            return true;
        }

        let last_write_time = inner.cache.last_write_time();
        if t < last_write_time {
            // Only checked sub can be executed
            return false;
        }

        t - last_write_time > inner.cache_flush_write_cold_duration
    }

    pub fn should_compact_cache2(
        t: Time,
        cache: &Arc<Cache>,
        cache_flush_memory_size_threshold: u64,
        cache_flush_write_cold_duration: Duration,
    ) -> bool {
        // println!(
        //     "shuuld_compact_cache...3, time_now: {:?}",
        //     chrono::Local::now().to_string()
        // );
        let cache_size = cache.size();
        let last_write_time = cache.last_write_time();
        if cache_size == 0 {
            return false;
        }

        if cache_size > cache_flush_memory_size_threshold {
            // println!(
            //     "\t yes, need snapshot, cache_size: {}, threshold: {}, size-threshold={} MB",
            //     cache_size,
            //     cache_flush_memory_size_threshold,
            //     (cache_size - cache_flush_memory_size_threshold) / 1024 / 1024
            // );
            return true;
        }

        if t < last_write_time {
            // Only checked sub can be executed
            return false;
        }

        t - last_write_time > cache_flush_write_cold_duration
    }

    // path returns the path the engine was opened with.
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn measurement_exists(&self, name: &[u8]) -> Result<bool> {
        if self.aggregate_index.is_some() { return self.index.measurement_has_series(name).context(IndexSnafu); }
        self.index.measurement_exists(name).context(IndexSnafu)
    }

    // measurement_field_set returns the measurement field set.
    pub fn measurement_field_set(&self) -> Arc<MeasurementFieldSet> {
        Arc::clone(&self.fieldset)
    }

    /// Returns the measurement fields for a measurement.
    pub fn measurement_field_keys(&self, measurement: &[u8]) -> Vec<String> {
        if self.aggregate_index.is_some() && self.measurement_exists(measurement).is_ok_and(|exists| !exists) { return Vec::new(); }
        if let Some(mf) = self.fieldset.fields(measurement) {
            return mf.field_keys();
        }
        vec![]
    }

    /// Returns the measurement field keys for a measurement.
    pub fn measurement_fields(&self, measurement: &[u8]) -> Arc<MeasurementFields> {
        self.fieldset.create_fields_if_not_exists(measurement)
    }

    pub(crate) fn visible_measurement_fields(&self, measurement: &[u8]) -> Option<Arc<MeasurementFields>> {
        if self.aggregate_index.is_some() && !self.measurement_exists(measurement).ok()? { return None; }
        Some(self.measurement_fields(measurement))
    }

    /// Returns a list of measurement names in the engine.
    pub fn measurement_names(&self) -> Result<Vec<Vec<u8>>> {
        let mut names = vec![];
        self.for_each_measurement_name(|name| names.push(name.to_vec()))?;
        Ok(names)
    }

    /// Returns the tag values for the requested keys of a measurement, one inner
    /// vec per key (aligned to `keys` after sorting when `keys_sorted` is false).
    pub fn measurement_tag_key_values(
        &self,
        name: &[u8],
        keys: &mut [String],
        keys_sorted: bool,
    ) -> Result<Vec<Vec<String>>> {
        if self.aggregate_index.is_some() {
            if !keys_sorted { keys.sort(); }
            let tags = self.aggregate_visible_tags(name)?;
            return Ok(keys.iter().map(|key| tags.get(key).map(|v| v.iter().cloned().collect()).unwrap_or_default()).collect());
        }
        let index_set =
            index::IndexSet::new(Arc::clone(&self.index), Arc::clone(&self.sfile), None);
        index_set
            .measurement_tag_key_values(name, keys, keys_sorted)
            .context(IndexSetSnafu)
    }

    /// Returns all series keys (line-protocol encoded) for a measurement.
    pub fn measurement_series_keys(&self, name: &[u8]) -> Result<Vec<Vec<u8>>> {
        let index_set =
            index::IndexSet::new(Arc::clone(&self.index), Arc::clone(&self.sfile), None);
        index_set
            .measurement_series_keys(name)
            .context(IndexSetSnafu)
    }

    /// Returns the measurement names selected by a storage-native predicate.
    pub fn measurement_names_by_storage_predicate(
        &self,
        predicate: Option<&StoragePredicate>,
    ) -> Result<Vec<Vec<u8>>> {
        let index_set =
            index::IndexSet::new(Arc::clone(&self.index), Arc::clone(&self.sfile), None);
        let names = index_set
            .measurement_names_by_storage_predicate(predicate)
            .context(IndexSetSnafu)?;
        if self.aggregate_index.is_none() { return Ok(names); }
        let mut visible = Vec::new();
        for name in names { if self.measurement_exists(&name)? { visible.push(name); } }
        Ok(visible)
    }

    /// Returns the series keys selected by a storage-native predicate.
    pub fn series_keys_by_storage_predicate(
        &self,
        name: &[u8],
        predicate: Option<&StoragePredicate>,
    ) -> Result<Vec<Vec<u8>>> {
        let index_set =
            index::IndexSet::new(Arc::clone(&self.index), Arc::clone(&self.sfile), None);
        index_set
            .series_keys_by_storage_predicate(name, predicate)
            .context(IndexSetSnafu)
    }

    pub fn has_tag_key(&self, name: &[u8], key: &[u8]) -> Result<bool> {
        if self.aggregate_index.is_some() { return Ok(self.aggregate_visible_tags(name)?.contains_key(std::str::from_utf8(key).unwrap_or(""))); }
        self.index.has_tag_key(name, key).context(IndexSnafu)
    }

    /// Returns the set of tag keys for a measurement.
    pub fn measurement_tag_keys(&self, name: &[u8]) -> Result<HashSet<String>> {
        if self.aggregate_index.is_some() { return Ok(self.aggregate_visible_tags(name)?.into_keys().collect()); }
        self.index.measurement_tag_keys(name).context(IndexSnafu)
    }

    #[cfg(feature = "slow-trace")]
    fn measurement_tag_keys_profiled(
        &self,
        name: &[u8],
        diagnostics: &Arc<index::MeasurementTagKeysDiagnostics>,
    ) -> Result<HashSet<String>> {
        self.index
            .measurement_tag_keys_profiled(name, diagnostics)
            .context(IndexSnafu)
    }

    pub fn tag_key_cardinality(&self, name: &[u8], key: &[u8]) -> i64 {
        if self.aggregate_index.is_some() { return self.aggregate_visible_tags(name).ok().and_then(|t| t.get(std::str::from_utf8(key).ok()?).map(|v| v.len() as i64)).unwrap_or(0); }
        self.index.tag_key_cardinality(name, key)
    }

    // series_n returns the unique number of series in the index.
    pub fn series_n(&self) -> i64 {
        self.index.series_n()
    }

    // measurements_sketches returns sketches that describe the cardinality of the
    // measurements in this shard and measurements that were in this shard, but have
    // been tombstoned.
    pub fn measurements_sketches(&self) -> Result<(HLL, HLL)> {
        self.index.measurements_sketches().context(IndexSnafu)
    }

    // series_sketches returns sketches that describe the cardinality of the
    // series in this shard and series that were in this shard, but have
    // been tombstoned.
    pub fn series_sketches(&self) -> Result<(HLL, HLL)> {
        self.index.series_sketches().context(IndexSnafu)
    }

    /// Returns the time when this shard was last modified.
    pub fn last_modified(&self) -> time::Time {
        let inner = self.inner.read();
        inner.last_modified(self.wal_enabled)
    }

    // Statistics returns statistics for periodic monitoring.
    pub fn statistics(&self, tags: HashMap<String, String>) -> Vec<Statistic> {
        let inner = self.inner.read();

        let mut statistics = vec![];
        statistics.push(Statistic {
            name: "tsm1_engine".to_string(),
            tags: tags.clone(),
            values: HashMap::from([
                (
                    STAT_CACHE_COMPACTIONS.to_string(),
                    self.stats.cache_compactions.load(Ordering::SeqCst),
                ),
                (
                    STAT_CACHE_COMPACTIONS_ACTIVE.to_string(),
                    self.stats.cache_compactions_active.load(Ordering::SeqCst),
                ),
                (
                    STAT_CACHE_COMPACTION_ERROR.to_string(),
                    self.stats.cache_compaction_errors.load(Ordering::SeqCst),
                ),
                (
                    STAT_CACHE_COMPACTION_DURATION.to_string(),
                    self.stats.cache_compaction_duration.load(Ordering::SeqCst),
                ),
                (
                    STAT_TSM_LEVEL1_COMPACTIONS.to_string(),
                    self.stats.tsm_compactions[0].load(Ordering::SeqCst),
                ),
                (
                    STAT_TSM_LEVEL1_COMPACTIONS_ACTIVE.to_string(),
                    self.stats.tsm_compactions_active[0].load(Ordering::SeqCst),
                ),
                (
                    STAT_TSM_LEVEL1_COMPACTION_ERROR.to_string(),
                    self.stats.tsm_compaction_errors[0].load(Ordering::SeqCst),
                ),
                (
                    STAT_TSM_LEVEL1_COMPACTION_DURATION.to_string(),
                    self.stats.tsm_compaction_duration[0].load(Ordering::SeqCst),
                ),
                (
                    STAT_TSM_LEVEL1_COMPACTION_QUEUE.to_string(),
                    self.stats.tsm_compactions_queue[0].load(Ordering::SeqCst),
                ),
                (
                    STAT_TSM_LEVEL2_COMPACTIONS.to_string(),
                    self.stats.tsm_compactions[1].load(Ordering::SeqCst),
                ),
                (
                    STAT_TSM_LEVEL2_COMPACTIONS_ACTIVE.to_string(),
                    self.stats.tsm_compactions_active[1].load(Ordering::SeqCst),
                ),
                (
                    STAT_TSM_LEVEL2_COMPACTION_ERROR.to_string(),
                    self.stats.tsm_compaction_errors[1].load(Ordering::SeqCst),
                ),
                (
                    STAT_TSM_LEVEL2_COMPACTION_DURATION.to_string(),
                    self.stats.tsm_compaction_duration[1].load(Ordering::SeqCst),
                ),
                (
                    STAT_TSM_LEVEL2_COMPACTION_QUEUE.to_string(),
                    self.stats.tsm_compactions_queue[1].load(Ordering::SeqCst),
                ),
                (
                    STAT_TSM_LEVEL3_COMPACTIONS.to_string(),
                    self.stats.tsm_compactions[2].load(Ordering::SeqCst),
                ),
                (
                    STAT_TSM_LEVEL3_COMPACTIONS_ACTIVE.to_string(),
                    self.stats.tsm_compactions_active[2].load(Ordering::SeqCst),
                ),
                (
                    STAT_TSM_LEVEL3_COMPACTION_ERROR.to_string(),
                    self.stats.tsm_compaction_errors[2].load(Ordering::SeqCst),
                ),
                (
                    STAT_TSM_LEVEL3_COMPACTION_DURATION.to_string(),
                    self.stats.tsm_compaction_duration[2].load(Ordering::SeqCst),
                ),
                (
                    STAT_TSM_LEVEL3_COMPACTION_QUEUE.to_string(),
                    self.stats.tsm_compactions_queue[2].load(Ordering::SeqCst),
                ),
                (
                    STAT_TSM_OPTIMIZE_COMPACTIONS.to_string(),
                    self.stats.tsm_optimize_compactions.load(Ordering::SeqCst),
                ),
                (
                    STAT_TSM_OPTIMIZE_COMPACTIONS_ACTIVE.to_string(),
                    self.stats
                        .tsm_optimize_compactions_active
                        .load(Ordering::SeqCst),
                ),
                (
                    STAT_TSM_OPTIMIZE_COMPACTION_DURATION.to_string(),
                    self.stats
                        .tsm_optimize_compaction_duration
                        .load(Ordering::SeqCst),
                ),
                (
                    STAT_TSM_OPTIMIZE_COMPACTION_ERROR.to_string(),
                    self.stats
                        .tsm_optimize_compaction_errors
                        .load(Ordering::SeqCst),
                ),
                (
                    STAT_TSM_OPTIMIZE_COMPACTION_QUEUE.to_string(),
                    self.stats
                        .tsm_optimize_compactions_queue
                        .load(Ordering::SeqCst),
                ),
                (
                    STAT_TSM_FULL_COMPACTIONS.to_string(),
                    self.stats.tsm_full_compactions.load(Ordering::SeqCst),
                ),
                (
                    STAT_TSM_FULL_COMPACTIONS_ACTIVE.to_string(),
                    self.stats
                        .tsm_full_compactions_active
                        .load(Ordering::SeqCst),
                ),
                (
                    STAT_TSM_FULL_COMPACTION_ERROR.to_string(),
                    self.stats.tsm_full_compaction_errors.load(Ordering::SeqCst),
                ),
                (
                    STAT_TSM_FULL_COMPACTION_DURATION.to_string(),
                    self.stats
                        .tsm_full_compaction_duration
                        .load(Ordering::SeqCst),
                ),
                (
                    STAT_TSM_FULL_COMPACTION_QUEUE.to_string(),
                    self.stats.tsm_full_compactions_queue.load(Ordering::SeqCst),
                ),
            ]),
        });
        // TODO: add vlog_index_cache vlog_index_wal statistic
        statistics.push(inner.cache.statistics(tags.clone()));
        statistics.push(inner.file_store.statistics(tags.clone()));

        if self.wal_enabled {
            statistics.push(inner.wal.statistics(tags.clone()));
        }

        statistics
    }

    // DiskSize returns the total size in bytes of all TSM and WAL segments on disk.
    pub fn disk_size(&self) -> i64 {
        let mut wal_disk_size_bytes = 0;
        let mut index_wal_disk_size_bytes = 0;
        let mut vlog_file_store_disk_size_bytes = 0;
        let inner = self.inner.read();
        if self.wal_enabled {
            wal_disk_size_bytes = inner.wal.disk_size_bytes()
        }
        if self.vlog_enabled {
            index_wal_disk_size_bytes = inner
                .vlog_inner
                .as_ref()
                .unwrap()
                .read()
                .vlog_index_wal
                .disk_size_bytes();
            vlog_file_store_disk_size_bytes = inner.file_store.disk_size_bytes();
        }
        inner.file_store.disk_size_bytes()
            + wal_disk_size_bytes
            + index_wal_disk_size_bytes
            + vlog_file_store_disk_size_bytes
    }

    // Close closes the engine. Subsequent calls to Close are a nop.
    pub fn close(&self) -> Result<()> {
        let started = std::time::Instant::now();
        tracing::info!(
            target: "reefsdb::tsdb::engine",
            engine_id = self.id,
            shard_id = self.id,
            "engine closing"
        );
        let result = self.close_inner();
        match &result {
            Ok(()) => tracing::info!(
                target: "reefsdb::tsdb::engine",
                engine_id = self.id,
                shard_id = self.id,
                duration_ms = started.elapsed().as_millis() as u64,
                "engine closed"
            ),
            Err(error) => tracing::error!(
                target: "reefsdb::tsdb::engine",
                engine_id = self.id,
                shard_id = self.id,
                error = %error,
                duration_ms = started.elapsed().as_millis() as u64,
                "engine close failed"
            ),
        }
        result
    }

    fn close_inner(&self) -> Result<()> {
        self.set_compactions_enabled(false);
        // Lock now and close everything else down.
        let mut inner = self.inner.write();
        inner.done = None;
        inner.compactor.close();
        if let Some(ref vlog_compactor) = inner.vlog_compactor {
            vlog_compactor.close();
        }
        inner.file_store.close().context(FileStoreSnafu)?;
        if self.wal_enabled {
            inner.wal.close();
        }

        if self.vlog_enabled {
            let vlog_inner = inner.vlog_inner.as_ref().unwrap().write();
            vlog_inner.vlog_index_wal.close();
            vlog_inner
                .vlog_file_store
                .close()
                .context(VLogFileStoreSnafu)?;
        }

        Ok(())
    }

    // TODO: implement me ? 是否需要 load form vlog file store ?
    // load_metadata_index loads the shard metadata into memory.
    //
    // Note, it not safe to call load_metadata_index concurrently. load_metadata_index
    // should only be called when initialising a new Engine.
    pub fn load_metadata_index(&mut self, shard_id: u64, index: Arc<Index>) -> Result<()> {
        // Save reference to index for iterator creation.
        self.index = index;

        // If we have the cached fields index on disk, we can skip scanning all the TSM files.
        if !self.fieldset.is_empty() {
            return Ok(());
        }

        let mut keys = Vec::with_capacity(10000);
        let mut field_types = Vec::with_capacity(10000);

        let inner = self.inner.read();
        inner.file_store.walk_keys::<_, Error>(&[], |key, typ| {
            let field_type = block_type_to_field_type(typ).ok_or_else(|| Error::StringErr {
                reason: format!("unknown block type: {typ}"),
            })?;

            keys.push(key);
            field_types.push(field_type);
            if keys.len() == keys.capacity() {
                // Send batch of keys to the index.
                let mut keys2 = keys.iter().map(|k| k.as_slice()).collect::<Vec<&[u8]>>();
                self.add_to_index_from_key(&mut keys2, &field_types)?;

                // Reset buffers.
                keys.clear();
                field_types.clear();
            }

            Ok(())
        })?;

        if !keys.is_empty() {
            // Add remaining partial batch from FileStore.
            let mut keys2 = keys.iter().map(|k| k.as_slice()).collect::<Vec<&[u8]>>();
            let _ = self.add_to_index_from_key(&mut keys2, &field_types);
            keys.clear();
            field_types.clear();
        }

        // load metadata from the Cache
        inner
            .cache
            .apply_entry_fn::<_, cache::Error>(|key, entry| {
                let field_type = match entry.values.read().reefs_ql_type() {
                    Ok(typ) => typ,
                    Err(e) => return Err(cache::Error::EncodingValues { source: e }),
                };

                keys.push(key);
                field_types.push(field_type);
                if keys.len() == keys.capacity() {
                    // Send batch of keys to the index.
                    let mut keys2 = keys.iter().map(|k| k.as_slice()).collect::<Vec<&[u8]>>();
                    let _ = self.add_to_index_from_key(&mut keys2, &field_types);
                    // Reset buffers.
                    keys.clear();
                    field_types.clear();
                }

                Ok(())
            })
            .context(CacheSnafu)?;

        if !keys.is_empty() {
            // Add remaining partial batch from FileStore.
            let mut keys2 = keys.iter().map(|k| k.as_slice()).collect::<Vec<&[u8]>>();
            let _ = self.add_to_index_from_key(&mut keys2, &field_types);
        }

        // Save the field set index so we don't have to rebuild it next time
        self.fieldset.save().context(MeasurementFieldsSnafu)?;

        // e.traceLogger.Info("Meta data index for shard loaded", zap.Uint64("id", shardID), zap.Duration("duration", time.Since(now)))
        Ok(())
    }

    // IsIdle returns true if the cache is empty, there are no running compactions and the
    // shard is fully compacted.
    pub fn is_idle(&self) -> bool {
        let inner = self.inner.read();
        let cache_empty = inner.cache.size() == 0;

        let mut running_compactions = self.stats.cache_compactions_active.load(Ordering::SeqCst);
        running_compactions += self.stats.tsm_compactions_active[0].load(Ordering::SeqCst);
        running_compactions += self.stats.tsm_compactions_active[1].load(Ordering::SeqCst);
        running_compactions += self.stats.tsm_compactions_active[2].load(Ordering::SeqCst);
        running_compactions += self
            .stats
            .tsm_full_compactions_active
            .load(Ordering::SeqCst);
        running_compactions += self
            .stats
            .tsm_optimize_compactions_active
            .load(Ordering::SeqCst);

        cache_empty && running_compactions == 0 && self.compaction_plan.full_compacted()
    }

    // Free releases any resources held by the engine to free up memory or CPU.
    pub fn free(&self) -> Result<()> {
        let inner = self.inner.read();
        inner.cache.free();
        inner.file_store.free().context(FileStoreSnafu)?;
        if self.vlog_enabled {
            let vlog_inner = inner.vlog_inner.as_ref().unwrap().read();
            vlog_inner
                .vlog_file_store
                .free()
                .context(VLogFileStoreSnafu)?;
        }
        Ok(())
    }

    // add_to_index_from_key will pull the measurement names, series keys, and field
    // names from composite keys, and add them to the database index and measurement
    // fields.
    fn add_to_index_from_key(&self, keys: &mut [&[u8]], field_types: &[FieldType]) -> Result<()> {
        let mut field = &[] as &[u8];
        let mut names = vec![];
        let mut tags = vec![];

        for i in 0..keys.len() {
            // Replace tsm key format with index key format.
            (keys[i], field) = series_and_file_form_composite_key(keys[i]);
            let name = crate::models::points::parse_name(keys[i]);
            let mf = self.fieldset.create_fields_if_not_exists(&name);
            mf.create_field_if_not_exists(field, field_types[i])
                .context(MeasurementFieldsSnafu)?;

            names.push(name);
            tags.push(crate::models::points::parse_tags(keys[i], None));
        }

        self.index
            .create_series_list_if_not_exists(keys, names, tags)
            .context(IndexSnafu)
    }

    // TODO: validateSeriesAndFields and createFieldsAndMeasurements first:
    // 1. validateSeriesAndFields: checks which series and fields are new and whose metadata should
    //    be saved and indexed.
    // 2. createFieldsAndMeasurements: add any new fields and keep track of what needs to be saved

    /// Writes metadata and point data into the engine.
    /// It returns an error if new points are added to an existing key.
    pub fn write_points(&self, points: &[crate::models::Point]) -> Result<()> {
        let write_total_start = slow_trace::now();
        // Activity is an optimistic cleanup stamp, not a writer lock. Writers
        // remain parallel; cleanup only proceeds while this stamp is unchanged.
        let _activity = self.aggregate_index.as_ref().map(|context| {
            context.active_writers.fetch_add(1, Ordering::SeqCst);
            context.write_epoch.fetch_add(1, Ordering::SeqCst);
            scopeguard::guard(context, |context| {
                context.write_epoch.fetch_add(1, Ordering::SeqCst);
                context.active_writers.fetch_sub(1, Ordering::SeqCst);
            })
        });
        if self.aggregate_index.is_some() {
            // Validation may have preceded a concurrent metadata cleanup. Rejoin
            // TSI inside the activity lifetime before publishing cache values.
            self.index.create_series_list_if_not_exists(&points.iter().map(|p| p.key()).collect::<Vec<_>>(),
                points.iter().map(|p| p.name()).collect(), points.iter().map(|p| crate::models::points::parse_tags(p.key(), None)).collect()).context(IndexSnafu)?;
        }
        let mut values = HashMap::<String, Vec<Value>>::new();

        let mut key_buf = vec![];
        for p in points {
            key_buf.clear();
            key_buf.extend_from_slice(p.key());
            key_buf.extend_from_slice(KEY_FIELD_SEPARATOR.as_bytes());
            let base_len = key_buf.len();
            let iter = p.field_iter();
            let t = p.time().timestamp_nanos();
            for field in iter {
                // Skip fields name "time", they are illegal
                if field.field_key() == TIME_COLUMN_NAME.as_bytes() {
                    continue;
                }

                // key_buf: "measurement,tag_sets#!~#field_key"
                key_buf.truncate(base_len);
                key_buf.extend_from_slice(field.field_key());
                let v = match field.field_type() {
                    crate::models::points::INTEGER => {
                        let iv = field.integer_value().context(PointSnafu)?;
                        Value::IntegerValue(IntegerValue::new(t, iv))
                    }
                    crate::models::points::UNSIGNED => {
                        let uv = field.unsigned_value().context(PointSnafu)?;
                        Value::UnsignedValue(UnsignedValue::new(t, uv))
                    }
                    crate::models::points::FLOAT => {
                        let fv = field.float_value().context(PointSnafu)?;
                        Value::FloatValue(FloatValue::new(t, fv))
                    }
                    crate::models::points::BOOL => {
                        let bv = field.bool_value().context(PointSnafu)?;
                        Value::BoolValue(BoolValue::new(t, bv))
                    }
                    crate::models::points::STRING => {
                        let sv = field.string_value().context(PointSnafu)?;
                        Value::StringValue(StringValue::new(t, sv))
                    }
                    _ => {
                        return Err(Error::StringErr {
                            reason: format!(
                                "unknow field type for {}: {}",
                                to_str!(field.field_key()),
                                p
                            ),
                        });
                    }
                };
                let k = to_string!(&key_buf);
                match values.get_mut(&k) {
                    Some(vals) => {
                        vals.push(v);
                    }
                    None => {
                        values.insert(k, vec![v]);
                    }
                }
            }
        }

        // Hold the engine read guard across the cache-first/WAL-second write lifecycle.
        // Snapshot and close paths take inner.write() before rotating the cache or closing
        // resources, so they wait for this middle state to finish. Concurrent writers may
        // still proceed. If WAL fails after the cache write succeeds, the batch remains
        // cache-visible; callers may retry and later query/snapshot/compaction deduplicates.
        let identity_started = slow_trace::now();
        let identities = self.aggregate_index.as_ref().map(|context| {
            context.store.ensure_ready().map_err(aggregate_index_error)?;
            values.keys().map(|key| Ok((key.clone(), context.sequence(key.as_bytes())?.get())))
                .collect::<Result<Vec<_>>>()
        }).transpose()?;
        slow_trace::log_background_stage("write_aggregate_identity", identity_started, "field sequences");
        // Shared stripe guards preserve writer parallelism. A deletion excludes
        // only writers sharing its field stripe while ordering cache and WAL.
        let stripe_ids = self.aggregate_index.as_ref().map(|_| {
            let mut ids = values.keys().map(|key| crc32fast::hash(key.as_bytes()) as usize % 256).collect::<Vec<_>>();
            ids.sort_unstable(); ids.dedup(); ids
        });
        let stripe_started = slow_trace::now();
        let _delete_guards = self.aggregate_index.as_ref().zip(stripe_ids.as_ref()).map(|(context, ids)| {
            ids.iter().map(|i| context.delete_stripes[*i].read()).collect::<Vec<_>>()
        });
        slow_trace::log_lock_wait("write_delete_stripes", stripe_started, "shared field guards");
        let engine_lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait("engine.read", engine_lock_start, "stage=write_total");

        let cache_write_start = slow_trace::now();
        inner
            .cache
            .write_multi(values.clone())
            .context(CacheSnafu)?;
        if let Some(identities) = identities {
            for (key, seq) in identities {
                inner.cache.set_sequence(key.as_bytes(), seq).context(CacheSnafu)?;
            }
        }
        slow_trace::log_background_stage("write_cache_write", cache_write_start, "");

        if self.wal_enabled {
            #[cfg(test)]
            if FAIL_ENGINE_WRITE_WAL_AFTER_CACHE.swap(false, Ordering::SeqCst) {
                return Err(Error::WAL {
                    source: crate::storage::tsm1::wal::Error::BufferedWriteFailed {
                        reason: "injected engine WAL failure after cache write".to_string(),
                    },
                });
            }

            let wal_write_start = slow_trace::now();
            let wal_result =
                futures::executor::block_on(inner.wal.write_multi(values)).context(WALSnafu);
            if let Err(error) = &wal_result {
                tracing::error!(
                    target: "reefsdb::tsdb::wal",
                    engine_id = self.id,
                    shard_id = self.id,
                    error = %error,
                    "WAL write failed"
                );
            }
            wal_result?;
            slow_trace::log_background_stage("write_wal_write", wal_write_start, "");
        }

        slow_trace::log_background_stage("write_total", write_total_start, "component=engine");
        Ok(())
    }

    /// Removes the values between min and max (inclusive) from all series
    /// Note, if this method fails, itr will be closed.
    pub fn delete_series_range(
        &self,
        // mut itr: FilterUndeletedSeriesIterAdapter,
        itr: Box<dyn SeriesIterator>,
        min: i64,
        max: i64,
    ) -> Result<()> {
        self.delete_series_range_with_predicate(
            itr,
            Some(|name: &[u8], tags: &crate::models::Tags| (min, max, true)),
        )
    }

    fn delete_series_range_cleanup(
        disable_once: bool,
        sfile: Option<&Arc<SeriesFile>>,
        engine: Option<&Self>,
        index_guard: IndexDeleteGuard,
        itr: &Box<dyn SeriesIterator>,
    ) {
        if disable_once {
            if let Some(sfile) = sfile {
                sfile.enable_compactions();
            }

            if let Some(engine) = engine {
                engine.enable_level_compactions(true);
            }
        }

        index_guard.release();

        itr.close();
    }

    /// Removes the values between min and max (inclusive) from all series for which predicate()
    /// returns true. If predicate() is None, then all values in range are removed.
    pub fn delete_series_range_with_predicate<F>(
        &self,
        itr: Box<dyn SeriesIterator>,
        predicate: Option<F>,
    ) -> Result<()>
    where
        F: Fn(&[u8], &crate::models::Tags) -> (i64, i64, bool),
    {
        self.delete_series_range_with_predicate_and_threshold(
            itr,
            predicate,
            DELETE_FLUSH_THRESHOLD,
        )
    }

    fn delete_series_range_with_predicate_and_threshold<F>(
        &self,
        mut itr: Box<dyn SeriesIterator>,
        predicate: Option<F>,
        delete_flush_threshold: usize,
    ) -> Result<()>
    where
        F: Fn(&[u8], &crate::models::Tags) -> (i64, i64, bool),
    {
        if let Some(context) = &self.aggregate_index {
            let result = (|| {
                let prepare_start = slow_trace::now();
                let mut ranges = context.store.delete_batch_builder();
                for series in itr.by_ref() {
                    let (mut min,mut max,selected) = predicate.as_ref()
                        .map(|p| p(series.name(),series.tags())).unwrap_or((i64::MIN,i64::MAX,true));
                    if !selected || min > max { continue; }
                    if min == MIN_NANO_TIME_I64 { min = i64::MIN; }
                    if max == MAX_NANO_TIME_I64 { max = i64::MAX; }
                    let Some(fields) = self.fieldset.fields(series.name()) else { continue; };
                    let key = crate::models::points::make_key(series.name(),series.tags());
                    for field in fields.field_keys() {
                        let composite = series_field_key_bytes(std::str::from_utf8(&key).map_err(|_| Error::StringErr {
                            reason: "series key is not UTF-8".into()
                        })?,&field);
                        ranges.push(vlog::index::delete::DeleteRange {
                            seq: context.sequence(&composite)?, min,max,cutoff:0,
                        }).map_err(aggregate_index_error)?;
                    }
                }
                let ranges = Arc::new(ranges.finish().map_err(aggregate_index_error)?);
                slow_trace::log_background_stage("delete_prepare_ranges", prepare_start, "aggregate index");
                if ranges.len() == 0 { return Ok(()); }
                self.delete_aggregate_batch(&ranges, DeleteSeriesMode::Ordinary)
            })();
            itr.close();
            return result;
        }
        let mut disable_once = false;

        // Ensure that the index does not compact away the measurement or series we're
        // going to delete before we're done with them.
        let index_guard = match self.index.retain_for_delete().context(IndexSnafu) {
            Ok(index_guard) => index_guard,
            Err(e) => {
                itr.close();
                return Err(e);
            }
        };

        let mut sz = 0;
        let (mut min, mut max) = (i64::MIN, i64::MAX);
        // Indicator that the min/max time for the current batch has changed and
        // we need to flush the current batch before appending to it.
        let mut flush_batch = false;

        // These are reversed from min/max to ensure they are different the first time through.
        let (mut new_min, mut new_max) = (i64::MAX, i64::MIN);

        // There is no predicate, so setup newMin/newMax to delete the full time range.
        if predicate.is_none() {
            new_min = min;
            new_max = max;
        }
        let mut batch = vec![];
        for elem in itr.by_ref() {
            let (elem_name, elem_tags) = (elem.name(), elem.tags());

            // See if the series should be deleted and if so, what range of time.
            if let Some(ref predicate) = predicate {
                let mut should_delete = false;
                (new_min, new_max, should_delete) = predicate(elem_name, elem_tags);
                if !should_delete {
                    continue;
                }

                // If the min/max happens to change for the batch, we need to flush
                // the current batch and start a new one.
                flush_batch = (min != new_min || max != new_max) && !batch.is_empty();
            }
            if !disable_once {
                // Disable and abort running compactions so that tombstones added existing tsm
                // files don't get removed.  This would cause deleted measurements/series to
                // re-appear once the compaction completed.  We only disable the level compactions
                // so that snapshotting does not stop while writing out tombstones.  If it is stopped,
                // and writing tombstones takes a long time, writes can get rejected due to the cache
                // filling up.
                self.disable_level_compactions(true);
                self.sfile.disable_compactions();
                self.sfile.wait();
                disable_once = true;
            }
            if sz >= delete_flush_threshold || flush_batch {
                if let Err(e) = self.delete_series_range_ordinary_batch(&batch, min, max) {
                    // FIXME: in delete_measurement method, if delete_series failed, itr should be closed.
                    Self::delete_series_range_cleanup(
                        disable_once,
                        Some(&self.sfile),
                        Some(self),
                        index_guard,
                        &itr,
                    );

                    return Err(e);
                }

                batch.clear();
                sz = 0;
                flush_batch = false;
            }
            // Use the new min/max time for the next iteration
            min = new_min;
            max = new_max;

            let key = crate::models::points::make_key(elem_name, elem_tags);
            sz += key.len();
            batch.push(key);
        }

        if !batch.is_empty() {
            if let Err(e) = self.delete_series_range_ordinary_batch(&batch, min, max) {
                Self::delete_series_range_cleanup(
                    disable_once,
                    Some(&self.sfile),
                    Some(self),
                    index_guard,
                    &itr,
                );

                return Err(e);
            }
        }

        Self::delete_series_range_cleanup(
            disable_once,
            Some(&self.sfile),
            Some(self),
            index_guard,
            &itr,
        );

        Ok(())
    }

    fn delete_series_range_ordinary_batch(
        &self,
        series_keys: &[Vec<u8>],
        min: i64,
        max: i64,
    ) -> Result<()> {
        self.delete_series_range2(series_keys.to_vec(), min, max, DeleteSeriesMode::Ordinary)?;
        if self.vlog_enabled {
            self.delete_series_range_from_vlog(
                series_keys.to_vec(),
                min,
                max,
                DeleteSeriesMode::Ordinary,
            )?;
        }
        Ok(())
    }

    // Removes the values between min and max (inclusive) from all series.  This
    // does not update the index or disable compactions.  This should mainly be called by DeleteSeriesRange
    // and not directly.
    fn delete_series_range2(
        &self,
        mut series_keys: Vec<Vec<u8>>,
        mut min: i64,
        mut max: i64,
        mode: DeleteSeriesMode,
        // cache: &Arc<Cache>,
        // wal: &Arc<WAL>,
        // file_store: &Arc<FileStore>,
    ) -> Result<()> {
        if series_keys.is_empty() {
            return Ok(());
        }

        // Min and max time in the engine are slightly different from the query language values.
        if min == MIN_NANO_TIME_I64 {
            min = i64::MIN;
        }
        if max == MAX_NANO_TIME_I64 {
            max = i64::MAX;
        }

        let overlaps_time_range_min_max = Arc::new(Mutex::new(false));
        let overlaps_time_range_min_max2 = Arc::clone(&overlaps_time_range_min_max);
        let (cache, wal, file_store) = {
            let inner = self.inner.read();
            (
                Arc::clone(&inner.cache),
                Arc::clone(&inner.wal),
                Arc::clone(&inner.file_store),
            )
        };

        let _ = file_store.apply(move |r| {
            if r.overlaps_time_range(min, max) {
                *overlaps_time_range_min_max2.lock() = true;
            }
            Ok(())
        });

        let mut overlaps_time_range_min_max = *overlaps_time_range_min_max.lock();
        let cache_count = if mode.includes_pending_snapshot() {
            cache.count_including_snapshot()
        } else {
            cache.count()
        };
        if !overlaps_time_range_min_max && cache_count > 0 {
            overlaps_time_range_min_max = true;
        }

        if !overlaps_time_range_min_max {
            return Ok(());
        }

        // Ensure keys are sorted since lower layers require them to be.
        if !bytesutil::is_sorted(&series_keys) {
            bytesutil::sort(&mut series_keys);
        }

        let series_keys = Arc::new(RwLock::new(series_keys));
        let series_keys2 = Arc::clone(&series_keys);
        // Run the delete on each TSM file in parallel
        file_store
            .apply(move |r| {
                let series_keys = series_keys2.read();

                // See if this TSM file contains the keys and time range
                let (min_key, max_key) = (&series_keys[0], &series_keys[series_keys.len() - 1]);
                let (tsm_min, tsm_max) = r.key_range();

                let (tsm_min, _) = series_and_file_form_composite_key(&tsm_min);
                let (tsm_max, _) = series_and_file_form_composite_key(&tsm_max);

                let overlaps = tsm_min <= max_key.as_slice() && tsm_max >= min_key.as_slice();
                if !overlaps || !r.overlaps_time_range(min, max) {
                    return Ok(());
                }

                // Delete each key we find in the file.  We seek to the min key and walk from there.
                let batch = TSMReader::batch_delete(Arc::clone(&r));
                let n = r.key_count();
                let mut j = 0;
                for i in r.seek(min_key)..n {
                    let (index_key, _) = r.key_at(i);
                    let (series_key, _) = series_and_file_form_composite_key(&index_key);

                    while j < series_keys.len() && series_keys[j].as_slice() < series_key {
                        j += 1;
                    }

                    if j >= series_keys.len() {
                        break;
                    }

                    if series_keys[j] == series_key {
                        if let Err(e) = batch.delete_range(&[&index_key], min, max) {
                            let _ = batch.roll_back();
                            return Err(file_store::Error::Reader { source: e });
                        }
                    }
                }

                if let Err(e) = batch.commit() {
                    return Err(file_store::Error::Reader { source: e });
                };

                Ok(())
            })
            .context(FileStoreSnafu)?;

        // find the keys in the cache and remove them
        let mut delete_keys = vec![];
        let series_keys2 = Arc::clone(&series_keys);
        if mode.includes_pending_snapshot() {
            for k in cache.keys_including_snapshot() {
                let series_keys = series_keys2.read();
                let (series_key, _) = series_and_file_form_composite_key(&k);
                let i = bytesutil::search_bytes(&series_keys, series_key);
                if i < series_keys.len() && series_key == series_keys[i].as_slice() {
                    delete_keys.push(k);
                }
            }
        } else {
            // ApplySerialEntryFn cannot return an error in this invocation.
            cache.apply_entry_fn(|k, _| {
                let series_keys = series_keys2.read();

                let (series_key, _) = series_and_file_form_composite_key(&k);

                // Cache does not walk keys in sorted order, so search the sorted
                // series we need to delete to see if any of the cache keys match.

                let i = bytesutil::search_bytes(&series_keys, series_key);
                if i < series_keys.len() && series_key == series_keys[i].as_slice() {
                    // k is the measurement + tags + sep + field
                    delete_keys.push(k);
                }

                Ok::<_, Error>(())
            })?;
        }

        // Sort the series keys because ApplyEntryFn iterates over the keys randomly.
        bytesutil::sort(&mut delete_keys);

        let delete_keys2 = delete_keys
            .iter()
            .map(|k| k.as_slice())
            .collect::<Vec<&[u8]>>();
        if mode.includes_pending_snapshot() {
            cache.delete_range_including_snapshot(&delete_keys2, min, max);
        } else {
            cache.delete_range(&delete_keys2, min, max);
        }

        // delete from the WAL
        if self.wal_enabled {
            // block on the delete_range so that we don't hold the lock across await points
            let wal = Arc::clone(&wal);
            futures::executor::block_on(wal.delete_range(delete_keys.clone(), min, max))
                .context(WALSnafu)?;
        }

        if !mode.reconciles_metadata() {
            return Ok(());
        }

        // The series are deleted on disk, but the index may still say they exist.
        // Depending on the the min,max time passed in, the series may or not actually
        // exists now.  To reconcile the index, we walk the series keys that still exists
        // on disk and cross out any keys that match the passed in series.  Any series
        // left in the slice at the end do not exist and can be deleted from the index.
        // Note: this is inherently racy if writes are occurring to the same measurement/series are
        // being removed.  A write could occur and exist in the cache at this point, but we
        // would delete it from the index.
        let min_key = Arc::new(series_keys.read()[0].clone());

        let series_keys2 = Arc::clone(&series_keys);
        // Apply runs this func concurrently.  The seriesKeys slice is mutated concurrently
        // by different goroutines setting positions to nil.
        file_store
            .apply(move |r| {
                let mut series_keys = series_keys2.write();

                let n = r.key_count();
                let mut j = 0;

                // Start from the min deleted key that exists in this file.
                for i in r.seek(&min_key)..n {
                    if j >= series_keys.len() {
                        return Ok(());
                    }

                    let (index_key, _) = r.key_at(i);
                    let (series_key, _) = series_and_file_form_composite_key(&index_key);

                    // Skip over any deleted keys that are less than our tsm key
                    let series_keys_slice = series_keys
                        .iter()
                        .map(|k| k.as_slice())
                        .collect::<Vec<&[u8]>>();
                    let mut cmp = series_keys_slice[j].cmp(series_key);
                    while j < series_keys_slice.len() && cmp == std::cmp::Ordering::Less {
                        j += 1;
                        if j >= series_keys_slice.len() {
                            return Ok(());
                        }
                        cmp = series_keys_slice[j].cmp(series_key);
                    }

                    // We've found a matching key, cross it out so we do not remove it from the index.
                    if j < series_keys.len() && cmp == std::cmp::Ordering::Equal {
                        series_keys[j] = EMPTY_BYTES.as_bytes().to_vec();
                        j += 1;
                    }
                }
                Ok(())
            })
            .context(FileStoreSnafu)?;

        // let series_keys = Arc::into_inner(series_keys).unwrap().into_inner();
        let mut series_keys = series_keys.write();
        // The seriesKeys slice is mutated if they are still found in the cache.
        let cache_keys = cache.keys();
        for i in 0..series_keys.len() {
            let series_key = &series_keys[i];
            // Already crossed out
            if series_key.is_empty() {
                continue;
            }

            let j = bytesutil::search_bytes(&cache_keys, series_key);
            if j < cache_keys.len() {
                let (cache_series_key, _) = series_and_file_form_composite_key(&cache_keys[j]);
                if series_key == cache_series_key {
                    series_keys[i] = EMPTY_BYTES.as_bytes().to_vec();
                }
            }
        }

        // Have we deleted all values for the series? If so, we need to remove
        // the series from the index.
        let mut has_deleted = false;
        for k in series_keys.iter() {
            if !k.is_empty() {
                has_deleted = true;
                break;
            }
        }

        if has_deleted {
            let ids = SeriesIDSet::new(&[]);
            let mut measurements = HashSet::new();

            for k in series_keys.iter() {
                if k.is_empty() {
                    continue; // This key was wiped because it shouldn't be removed from index.
                }

                let (name, tags) = crate::models::points::parse_key_bytes(k.as_slice());
                let sid = self.sfile.series_id(&name, &tags);
                if sid == 0 {
                    continue;
                }

                // See if this series was found in the cache earlier
                let mut i = bytesutil::search_bytes(&delete_keys, k);

                let mut has_cache_values = false;
                // If there are multiple fields, they will have the same prefix.  If any field
                // has values, then we can't delete it from the index.
                while i < delete_keys.len() && delete_keys[i].as_slice().starts_with(k.as_slice()) {
                    if !cache.values(&delete_keys[i]).is_empty() {
                        has_cache_values = true;
                        break;
                    }
                    i += 1;
                }

                if has_cache_values {
                    continue;
                }

                measurements.insert(name);
                // Remove the series from the local index.
                self.index.drop_series(sid, k, false).context(IndexSnafu)?;

                // Add the id to the set of delete ids.
                ids.add(sid);
            }

            let mut fildset_changed = false;
            for k in measurements.iter() {
                if self
                    .index
                    .drop_measurement_if_series_not_exist(k.as_slice())
                    .context(IndexSnafu)?
                {
                    self.cleanup_measurement(k.as_slice(), &cache, &file_store)?;
                    fildset_changed = true;
                }
            }

            if fildset_changed {
                self.fieldset.save().context(MeasurementFieldsSnafu)?;
            }

            struct DeleteSeriesIDSets;
            impl CallbackSeriesIDSet for DeleteSeriesIDSets {
                fn for_each_series_id_set(&self, ids: &SeriesIDSet, id_set: &Arc<SeriesIDSet>) {
                    ids.and_not(id_set);
                }
            }

            let delete_series_id_sets = DeleteSeriesIDSets {};
            // `series_id_sets` is an optional in-memory index structure; in the
            // default engine configuration it is `None`, so there is nothing to
            // reconcile here — skip it rather than unwrapping a `None` (which
            // panicked whenever a delete emptied a whole series). The series-file
            // cleanup below still runs.
            if let Some(series_id_sets) = self.series_id_sets.as_ref() {
                if let Err(e) = series_id_sets.for_each(&ids, &delete_series_id_sets) {
                    return Err(Error::StringErr {
                        reason: e.to_string(),
                    });
                }
            }

            // Remove the remaining ids from the series file as they no longer exist
            // in any shard.
            let mut err = None;
            ids.for_each(|id| {
                if let Err(e) = self.sfile.delete_series_id(id) {
                    err = Some(e);
                }
            });

            if let Some(e) = err {
                return Err(Error::SeriesFile { source: e });
            }
        }

        Ok(())
    }

    fn delete_series_range_from_vlog(
        &self,
        mut series_keys: Vec<Vec<u8>>,
        mut min: i64,
        mut max: i64,
        mode: DeleteSeriesMode,
    ) -> Result<()> {
        if series_keys.is_empty() {
            return Ok(());
        }

        // Min and max time in the engine are slightly different from the query language values.
        if min == MIN_NANO_TIME_I64 {
            min = i64::MIN;
        }
        if max == MAX_NANO_TIME_I64 {
            max = i64::MAX;
        }

        let overlaps_time_range_min_max = Arc::new(Mutex::new(false));
        let overlaps_time_range_min_max2 = Arc::clone(&overlaps_time_range_min_max);

        let (cache, wal, file_store, vlog_file_store) = {
            let inner = self.inner.read();
            let vlog_inner = inner.vlog_inner.as_ref().unwrap().read();
            (
                Arc::clone(&inner.cache),
                Arc::clone(&inner.wal),
                Arc::clone(&inner.file_store),
                Arc::clone(&vlog_inner.vlog_file_store),
            )
        };

        let _ = vlog_file_store.apply(move |r| {
            if r.overlaps_time_range(min, max) {
                *overlaps_time_range_min_max2.lock() = true;
            }
            Ok(())
        });

        let mut overlaps_time_range_min_max = *overlaps_time_range_min_max.lock();
        let cache_count = if mode.includes_pending_snapshot() {
            cache.count_including_snapshot()
        } else {
            cache.count()
        };
        if !overlaps_time_range_min_max && cache_count > 0 {
            overlaps_time_range_min_max = true;
        }

        if !overlaps_time_range_min_max {
            return Ok(());
        }

        // Ensure keys are sorted since lower layers require them to be.
        if !bytesutil::is_sorted(&series_keys) {
            bytesutil::sort(&mut series_keys);
        }

        let series_keys = Arc::new(RwLock::new(series_keys));
        let series_keys2 = Arc::clone(&series_keys);
        // Run the delete on each TSM file in parallel
        vlog_file_store
            .apply(move |r| {
                let series_keys = series_keys2.read();

                // See if this TSM file contains the keys and time range
                let (min_key, max_key) = (&series_keys[0], &series_keys[series_keys.len() - 1]);
                let (tsm_min, tsm_max) = r.key_range();

                let (tsm_min, _) = series_and_file_form_composite_key(&tsm_min);
                let (tsm_max, _) = series_and_file_form_composite_key(&tsm_max);

                let overlaps = tsm_min <= max_key.as_slice() && tsm_max >= min_key.as_slice();
                if !overlaps || !r.overlaps_time_range(min, max) {
                    return Ok(());
                }

                // Delete each key we find in the file.  We seek to the min key and walk from there.
                let batch = VLogReader::batch_delete(Arc::clone(&r));
                let n = r.key_count();
                let mut j = 0;

                let keys = r.keys();
                for (index_key, typ) in keys {
                    let (series_key, _) = series_and_file_form_composite_key(&index_key);

                    while j < series_keys.len() && series_keys[j].as_slice() < series_key {
                        j += 1;
                    }

                    if j >= series_keys.len() {
                        break;
                    }

                    if series_keys[j] == series_key {
                        if let Err(e) = batch.delete_range(&[&index_key], min, max) {
                            let _ = batch.roll_back();
                            return Err(vlog_file_store::Error::Reader { source: e });
                        }
                    }
                }

                if let Err(e) = batch.commit() {
                    return Err(vlog_file_store::Error::Reader { source: e });
                };

                Ok(())
            })
            .context(VLogFileStoreSnafu)?;

        // find the keys in the cache and remove them
        let mut delete_keys = vec![];

        let series_keys2 = Arc::clone(&series_keys);
        if mode.includes_pending_snapshot() {
            for k in cache.keys_including_snapshot() {
                let series_keys = series_keys2.read();
                let (series_key, _) = series_and_file_form_composite_key(&k);
                let i = bytesutil::search_bytes(&series_keys, series_key);
                if i < series_keys.len() && series_key == series_keys[i].as_slice() {
                    delete_keys.push(k);
                }
            }
        } else {
            // ApplySerialEntryFn cannot return an error in this invocation.
            cache.apply_entry_fn(|k, _| {
                let series_keys = series_keys2.read();

                let (series_key, _) = series_and_file_form_composite_key(&k);

                // Cache does not walk keys in sorted order, so search the sorted
                // series we need to delete to see if any of the cache keys match.

                let i = bytesutil::search_bytes(&series_keys, series_key);
                if i < series_keys.len() && series_key == series_keys[i].as_slice() {
                    // k is the measurement + tags + sep + field
                    delete_keys.push(k);
                }

                Ok::<_, Error>(())
            })?;
        }

        // Sort the series keys because ApplyEntryFn iterates over the keys randomly.
        bytesutil::sort(&mut delete_keys);

        let delete_keys2 = delete_keys
            .iter()
            .map(|k| k.as_slice())
            .collect::<Vec<&[u8]>>();
        if mode.includes_pending_snapshot() {
            cache.delete_range_including_snapshot(&delete_keys2, min, max);
        } else {
            cache.delete_range(&delete_keys2, min, max);
        }

        // delete from the WAL
        if self.wal_enabled {
            // block on the delete_range so that we don't hold the lock across await points
            let wal = Arc::clone(&wal);
            futures::executor::block_on(wal.delete_range(delete_keys.clone(), min, max))
                .context(WALSnafu)?;
        }

        if !mode.reconciles_metadata() {
            return Ok(());
        }

        // The series are deleted on disk, but the index may still say they exist.
        // Depending on the the min,max time passed in, the series may or not actually
        // exists now.  To reconcile the index, we walk the series keys that still exists
        // on disk and cross out any keys that match the passed in series.  Any series
        // left in the slice at the end do not exist and can be deleted from the index.
        // Note: this is inherently racy if writes are occurring to the same measurement/series are
        // being removed.  A write could occur and exist in the cache at this point, but we
        // would delete it from the index.
        let min_key = Arc::new(series_keys.read()[0].clone());

        let series_keys2 = Arc::clone(&series_keys);
        // Apply runs this func concurrently.  The seriesKeys slice is mutated concurrently
        // by different goroutines setting positions to nil.
        vlog_file_store
            .apply(move |r| {
                let mut series_keys = series_keys2.write();

                let n = r.key_count();
                let mut j = 0;

                let keys = r.keys();
                // Start from the min deleted key that exists in this file.
                for (index_key, typ) in keys {
                    if j >= series_keys.len() {
                        return Ok(());
                    }

                    let (series_key, _) = series_and_file_form_composite_key(&index_key);

                    // Skip over any deleted keys that are less than our tsm key
                    let series_keys_slice = series_keys
                        .iter()
                        .map(|k| k.as_slice())
                        .collect::<Vec<&[u8]>>();
                    let mut cmp = series_keys_slice[j].cmp(series_key);
                    while j < series_keys_slice.len() && cmp == std::cmp::Ordering::Less {
                        j += 1;
                        if j >= series_keys_slice.len() {
                            return Ok(());
                        }
                        cmp = series_keys_slice[j].cmp(series_key);
                    }

                    // We've found a matching key, cross it out so we do not remove it from the index.
                    if j < series_keys.len() && cmp == std::cmp::Ordering::Equal {
                        series_keys[j] = EMPTY_BYTES.as_bytes().to_vec();
                        j += 1;
                    }
                }
                Ok(())
            })
            .context(VLogFileStoreSnafu)?;

        // let series_keys = Arc::into_inner(series_keys).unwrap().into_inner();
        let mut series_keys = series_keys.write();

        // The seriesKeys slice is mutated if they are still found in the cache.
        let cache_keys = cache.keys();
        for i in 0..series_keys.len() {
            let series_key = &series_keys[i];
            // Already crossed out
            if series_key.is_empty() {
                continue;
            }

            let j = bytesutil::search_bytes(&cache_keys, series_key);
            if j < cache_keys.len() {
                let (cache_series_key, _) = series_and_file_form_composite_key(&cache_keys[j]);
                if series_key == cache_series_key {
                    series_keys[i] = EMPTY_BYTES.as_bytes().to_vec();
                }
            }
        }

        // Have we deleted all values for the series? If so, we need to remove
        // the series from the index.
        let mut has_deleted = false;
        for k in series_keys.iter() {
            if !k.is_empty() {
                has_deleted = true;
                break;
            }
        }

        if has_deleted {
            let ids = SeriesIDSet::new(&[]);
            let mut measurements = HashSet::new();

            for k in series_keys.iter() {
                if k.is_empty() {
                    continue; // This key was wiped because it shouldn't be removed from index.
                }

                let (name, tags) = crate::models::points::parse_key_bytes(k.as_slice());
                let sid = self.sfile.series_id(&name, &tags);
                if sid == 0 {
                    continue;
                }

                // See if this series was found in the cache earlier
                let mut i = bytesutil::search_bytes(&delete_keys, k);

                let mut has_cache_values = false;
                // If there are multiple fields, they will have the same prefix.  If any field
                // has values, then we can't delete it from the index.
                while i < delete_keys.len() && delete_keys[i].as_slice().starts_with(k.as_slice()) {
                    if !cache.values(&delete_keys[i]).is_empty() {
                        has_cache_values = true;
                        break;
                    }
                    i += 1;
                }

                if has_cache_values {
                    continue;
                }

                measurements.insert(name);
                // Remove the series from the local index.
                self.index.drop_series(sid, k, false).context(IndexSnafu)?;

                // Add the id to the set of delete ids.
                ids.add(sid);
            }

            let mut fildset_changed = false;
            for k in measurements.iter() {
                if self
                    .index
                    .drop_measurement_if_series_not_exist(k.as_slice())
                    .context(IndexSnafu)?
                {
                    self.cleanup_measurement(k.as_slice(), &cache, &file_store)?;
                    fildset_changed = true;
                }
            }

            if fildset_changed {
                self.fieldset.save().context(MeasurementFieldsSnafu)?;
            }

            struct DeleteSeriesIDSets;
            impl CallbackSeriesIDSet for DeleteSeriesIDSets {
                fn for_each_series_id_set(&self, ids: &SeriesIDSet, id_set: &Arc<SeriesIDSet>) {
                    ids.and_not(id_set);
                }
            }

            let delete_series_id_sets = DeleteSeriesIDSets {};
            // `series_id_sets` is an optional in-memory index structure; in the
            // default engine configuration it is `None`, so there is nothing to
            // reconcile here — skip it rather than unwrapping a `None` (which
            // panicked whenever a delete emptied a whole series). The series-file
            // cleanup below still runs.
            if let Some(series_id_sets) = self.series_id_sets.as_ref() {
                if let Err(e) = series_id_sets.for_each(&ids, &delete_series_id_sets) {
                    return Err(Error::StringErr {
                        reason: e.to_string(),
                    });
                }
            }

            // Remove the remaining ids from the series file as they no longer exist
            // in any shard.
            let mut err = None;
            ids.for_each(|id| {
                if let Err(e) = self.sfile.delete_series_id(id) {
                    err = Some(e);
                }
            });

            if let Some(e) = err {
                return Err(Error::SeriesFile { source: e });
            }
        }

        Ok(())
    }

    fn cleanup_measurement(
        &self,
        name: &[u8],
        cache: &Arc<Cache>,
        file_store: &Arc<FileStore>,
    ) -> Result<()> {
        // Under write lock, delete the measurement if we no longer have any data stored for
        // the measurement.  If data exists, we can't delete the field set yet as there
        // were writes to the measurement while we are deleting it.
        match self.fieldset.delete_with_lock(name, || {
            let encoded_name = crate::models::points::escape_measurement(name);
            let sep = encoded_name.len();
            // First scan the cache to see if any series exists for this measurement.
            cache.apply_entry_fn(|k, _| {
                if k.starts_with(encoded_name.as_slice())
                    && (k[sep] == b',' || k[sep] == KEY_FIELD_SEPARATOR.as_bytes()[0])
                {
                    // A sentinel error message to cause delete_with_lock to not delete the measurement
                    return Err(Error::StringErr {
                        reason: "measurements still exist".to_string(),
                    });
                }
                Ok(())
            })?;

            // Check the filestore.
            file_store.walk_keys(name, |k, _| {
                if k.starts_with(encoded_name.as_slice())
                    && (k[sep] == b',' || k[sep] == KEY_FIELD_SEPARATOR.as_bytes()[0])
                {
                    // A sentinel error message to cause delete_with_lock to not delete the measurement
                    return Err(Error::StringErr {
                        reason: "measurements still exist".to_string(),
                    });
                }

                Ok(())
            })?;

            Ok(())
        }) {
            Ok(_) => Ok(()),
            Err(e) => match &e {
                Error::StringErr { reason } => {
                    if reason != "measurements still exist" {
                        return Err(e);
                    }
                    Ok(())
                }
                _ => Ok(()),
            },
        }
    }

    /// Durably deletes this shard's data for a measurement while retaining all
    /// local and database-wide metadata needed by a retry or later finalization.
    ///
    /// The database-level caller must hold its exclusive measurement DDL gate
    /// across every shard's data and metadata phases. This local gate only
    /// serializes the deletion with snapshot rotation and publication.
    pub fn delete_measurement_data(&self, name: &[u8]) -> Result<Vec<u64>> {
        let snapshot_gate = {
            let inner = self.inner.read();
            Arc::clone(&inner.snapshot_measurement_drop_gate)
        };
        let _snapshot_guard = futures::executor::block_on(snapshot_gate.lock_owned());

        let index_set =
            index::IndexSet::new(Arc::clone(&self.index), Arc::clone(&self.sfile), None);
        let series_keys = index_set
            .measurement_series_keys(name)
            .context(IndexSetSnafu)?;

        let mut series_ids = Vec::with_capacity(series_keys.len());
        for key in &series_keys {
            let (series_name, tags) = crate::models::points::parse_key_bytes(key);
            let series_id = self.sfile.series_id(&series_name, &tags);
            if series_id != 0 {
                series_ids.push(series_id);
            }
        }
        series_ids.sort_unstable();
        series_ids.dedup();

        if series_keys.is_empty() {
            return Ok(series_ids);
        }

        let (level_compactions_enabled, vlog_compactions_enabled) = {
            let inner = self.inner.read();
            (inner.done.is_some(), inner.vlog_done.is_some())
        };
        if level_compactions_enabled {
            self.disable_level_compactions(true);
        }
        if vlog_compactions_enabled {
            self.disable_vlog_compactions(true);
        }
        defer! {
            if vlog_compactions_enabled {
                self.enable_vlog_compactions(true);
            }
            if level_compactions_enabled {
                self.enable_level_compactions(true);
            }
        }

        let range = TimeRange::all();
        let mut batch = Vec::new();
        let mut batch_size = 0;
        for key in series_keys {
            batch_size += key.len();
            batch.push(key);
            if batch_size >= DELETE_FLUSH_THRESHOLD {
                self.delete_measurement_data_batch(std::mem::take(&mut batch), range)?;
                batch_size = 0;
            }
        }
        if !batch.is_empty() {
            self.delete_measurement_data_batch(batch, range)?;
        }

        Ok(series_ids)
    }

    fn delete_measurement_data_batch(
        &self,
        series_keys: Vec<Vec<u8>>,
        range: TimeRange,
    ) -> Result<()> {
        if let Some(context) = &self.aggregate_index {
            let mut ranges = context.store.delete_batch_builder();
            for key in series_keys {
                let (name, _) = crate::models::points::parse_key_bytes(&key);
                let Some(fields) = self.fieldset.fields(&name) else { continue; };
                for field in fields.field_keys() {
                    let mut composite = key.clone();
                    composite.extend_from_slice(KEY_FIELD_SEPARATOR.as_bytes());
                    composite.extend_from_slice(field.as_bytes());
                    ranges.push(vlog::index::delete::DeleteRange {
                        seq: context.sequence(&composite)?,
                        min: i64::MIN,
                        max: i64::MAX,
                        cutoff: 0,
                    }).map_err(aggregate_index_error)?;
                }
            }
            let ranges = ranges.finish().map_err(aggregate_index_error)?;
            if ranges.len() != 0 {
                self.delete_aggregate_batch(&ranges, DeleteSeriesMode::MeasurementData)?;
            }
            return Ok(());
        }
        self.delete_series_range2(
            series_keys.clone(),
            range.start_time,
            range.end_time,
            DeleteSeriesMode::MeasurementData,
        )?;
        if self.vlog_enabled {
            self.delete_series_range_from_vlog(
                series_keys,
                range.start_time,
                range.end_time,
                DeleteSeriesMode::MeasurementData,
            )?;
        }
        Ok(())
    }

    /// Removes only shard-local TSI and `fields.idx` metadata for a measurement.
    /// Database-wide SeriesFile and MeasurementSchemaRegistry cleanup belongs to
    /// the database-level coordinator after every shard data phase succeeds.
    pub fn finalize_measurement_drop_local(&self, name: &[u8]) -> Result<()> {
        let field_name = std::str::from_utf8(name).map_err(|source| Error::StringErr {
            reason: format!("measurement name is not valid UTF-8: {source}"),
        })?;
        self.index.drop_measurement(name).context(IndexSnafu)?;
        self.fieldset.delete(field_name);
        self.fieldset.save().context(MeasurementFieldsSnafu)
    }

    /// Deletes a measurement and all related series.
    /// Note, this will deletes all data and series from the specified measurement
    /// and deletes the measurement from the index.
    pub fn delete_measurement(&self, name: &[u8]) -> Result<()> {
        // Attempt to find the series keys.
        let index_set =
            index::IndexSet::new(Arc::clone(&self.index), Arc::clone(&self.sfile), None);
        let itr = index_set
            .measurement_series_iter(name)
            .context(IndexSetSnafu)?;
        if let Some(itr) = itr {
            self.delete_series_range(
                Box::new(new_filter_undeleted_series_iterator_adapter(
                    Arc::clone(&self.sfile),
                    itr,
                )),
                i64::MIN,
                i64::MAX,
            )
        } else {
            Ok(())
        }
    }

    /// Deletes points of a single measurement selected by `predicate`
    /// (per-series `(name, tags) -> (min, max, should_delete)`, where the tuple
    /// carries both the time range and any tag filter). This is the
    /// range/predicate counterpart of `delete_measurement`, which drops the
    /// whole measurement over the full range; here we reuse its
    /// `measurement_series_iter` construction and defer to
    /// `delete_series_range_with_predicate`. A missing measurement is a no-op,
    /// not an error.
    pub fn delete_series_range_by_measurement<F>(
        &self,
        name: &[u8],
        predicate: Option<F>,
    ) -> Result<()>
    where
        F: Fn(&[u8], &crate::models::Tags) -> (i64, i64, bool),
    {
        let index_set =
            index::IndexSet::new(Arc::clone(&self.index), Arc::clone(&self.sfile), None);
        let itr = index_set
            .measurement_series_iter(name)
            .context(IndexSetSnafu)?;
        match itr {
            Some(itr) => self.delete_series_range_with_predicate(
                Box::new(new_filter_undeleted_series_iterator_adapter(
                    Arc::clone(&self.sfile),
                    itr,
                )),
                predicate,
            ),
            None => Ok(()),
        }
    }

    // TODO: donot need to return error
    // Iterates over each measurement name in the engine.
    pub fn for_each_measurement_name<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]),
    {
        if self.aggregate_index.is_some() {
            let mut names = Vec::new();
            self.index.for_each_measurement_name(|name| names.push(name.to_vec())).context(IndexSnafu)?;
            for name in names { if self.measurement_exists(&name)? { f(&name); } }
            return Ok(());
        }
        self.index.for_each_measurement_name(f).context(IndexSnafu)
    }

    pub fn create_series_list_if_not_exists(
        &self,
        keys: &[&[u8]],
        names: Vec<Vec<u8>>,
        tags_slice: Vec<crate::models::points::Tags>,
    ) -> Result<()> {
        self.index
            .create_series_list_if_not_exists(keys, names, tags_slice)
            .context(IndexSnafu)
    }

    pub fn create_series_if_not_exists(
        &self,
        key: &[u8],
        name: &[u8],
        tags: crate::models::points::Tags,
    ) -> Result<()> {
        self.index
            .create_series_if_not_exists(key, name, tags)
            .context(IndexSnafu)
    }

    // // WriteTo is not implemented.
    // pub fn WriteTo(&self, w io.Writer) (n int64, err error) { panic("not implemented") }

    /// Snapshot the cache and write a new TSM file with its contents, releasing the snapshot when done.
    pub fn write_snapshot(wal_enabled: bool, inner: Arc<RwLock<Inner>>) -> Result<()> {
        futures::executor::block_on(Self::write_snapshot_async(wal_enabled, inner))
    }

    async fn write_snapshot_async(wal_enabled: bool, inner: Arc<RwLock<Inner>>) -> Result<()> {
        let start = std::time::Instant::now();
        let (executor, snapshot_gate) = {
            let inner = inner.read();
            (
                Arc::clone(&inner.executor),
                Arc::clone(&inner.snapshot_measurement_drop_gate),
            )
        };
        let _snapshot_guard = snapshot_gate
            .try_lock_owned()
            .map_err(|_| cache::Error::ErrSnapshotInProgress)
            .context(CacheSnafu)?;
        let prepare_inner = Arc::clone(&inner);
        let prepared = executor
            .spawn_blocking_compact(move || {
                let lock_started = slow_trace::now();
                let inner_guard = prepare_inner.write();
                slow_trace::log_lock_wait("snapshot_engine_write", lock_started, "cache and WAL rotation");
                if let Some(context) = &inner_guard.aggregate_index {
                    context.store.ensure_ready().map_err(aggregate_index_error)?;
                    if context.deleting.load(Ordering::SeqCst) || inner_guard.cache.snapshot_in_progress() {
                        return Err(Error::Cache { source: cache::Error::ErrSnapshotInProgress });
                    }
                }
                let retry_wal = inner_guard.aggregate_index.as_ref()
                    .filter(|_| inner_guard.cache.has_pending_snapshot_values())
                    .and_then(|context| context.snapshot_wal.lock().clone());
                let wal_started = slow_trace::now();
                let closed_files = if let Some(files) = retry_wal {
                    files
                } else {
                    if wal_enabled { inner_guard.wal.close_segment().context(WALSnafu)?; }
                    inner_guard.wal.closed_segments().context(WALSnafu)?
                };
                slow_trace::log_background_stage("snapshot_wal_rotate", wal_started, "close and enumerate segments");
                let cache_started = slow_trace::now();
                let snapshot = inner_guard.cache.snapshot().context(CacheSnafu)?;
                slow_trace::log_background_stage("snapshot_cache_rotate", cache_started, "capture snapshot");
                if let Some(context) = &inner_guard.aggregate_index {
                    *context.snapshot_wal.lock() = Some(closed_files.clone());
                }

                if snapshot.size() == 0 {
                    if let Some(context) = &inner_guard.aggregate_index { *context.snapshot_wal.lock() = None; }
                    inner_guard.cache.clear_snapshot(true);
                    return Ok::<_, Error>(None);
                }

                let prepared = (
                    closed_files,
                    snapshot,
                    Arc::clone(&inner_guard.cache),
                    Arc::clone(&inner_guard.compactor),
                    inner_guard.engine_id,
                );
                drop(inner_guard);
                prepared.1.deduplicate();
                Ok(Some(prepared))
            })
            .map_err(|source| Error::Execution { source })?
            .await
            .map_err(|_| Error::TaskTerminated)??;
        let Some((closed_files, snapshot, cache, compactor, engine_id)) = prepared else {
            return Ok(());
        };

        let cache_bytes = snapshot.size();
        tracing::info!(
            target: "reefsdb::tsdb::snapshot",
            engine_id,
            shard_id = engine_id,
            cache_bytes,
            wal_segment_count = closed_files.len(),
            "writing snapshot"
        );

        let outputs = match compactor.write_snapshot(Arc::clone(&snapshot)).await {
            Ok(outputs) => outputs,
            Err(source) => {
                cache.clear_snapshot(false);
                return Err(Error::Compact { source });
            }
        };
        let cleanup_cache = Arc::clone(&cache);
        let cleanup_files = outputs.0.clone();
        let refused_cache = Arc::clone(&cache);
        let refused_files = outputs.0.clone();
        let (registration, completion) = accepted_task_guard(move || {
            for file in cleanup_files {
                let _ = std::fs::remove_file(file);
            }
            cleanup_cache.clear_snapshot(false);
        });
        let commit_inner = Arc::clone(&inner);
        let commit_cache = Arc::clone(&cache);
        let task = match executor.spawn_blocking_compact(move || {
            let result = Self::commit_snapshot_outputs(
                wal_enabled,
                commit_inner,
                &closed_files,
                &commit_cache,
                &snapshot,
                outputs,
            );
            completion.complete();
            result
        }) {
            Ok(task) => {
                registration.accepted();
                task
            }
            Err(source) => {
                registration.rejected();
                for file in refused_files {
                    let _ = std::fs::remove_file(file);
                }
                refused_cache.clear_snapshot(false);
                return Err(Error::Execution { source });
            }
        };
        let res = task.await.map_err(|_| Error::TaskTerminated)?;
        cache.update_compact_time(start.elapsed());
        match &res {
            Ok(()) => tracing::info!(
                target: "reefsdb::tsdb::snapshot",
                engine_id,
                shard_id = engine_id,
                cache_bytes,
                duration_ms = start.elapsed().as_millis() as u64,
                "snapshot complete"
            ),
            Err(error) => tracing::error!(
                target: "reefsdb::tsdb::snapshot",
                engine_id,
                shard_id = engine_id,
                cache_bytes,
                error = %error,
                duration_ms = start.elapsed().as_millis() as u64,
                "snapshot failed"
            ),
        }
        res
    }

    // TODO: used for backup
    // Create a temp directory that holds
    // temporary hardlinks to the underlying shard files.
    // skipCacheOk controls whether it is permissible to fail writing out
    // in-memory cache data when a previous snapshot is in progress.
    pub fn create_snapshot(&self, skip_cache_ok: bool) -> Result<PathBuf> {
        // todo!("implement me");
        let inner = Arc::clone(&self.inner);
        let (mut err, in_progress) = Self::try_write_snapshot(self.wal_enabled, Arc::clone(&inner));
        if err.is_err() && in_progress {
            for i in 0..3 {
                let backoff = std::time::Duration::from_millis(32i32.pow(i) as u64);
                std::thread::sleep(backoff);
                let (err2, in_progress) =
                    Self::try_write_snapshot(self.wal_enabled, Arc::clone(&inner));
                if err.is_ok() {
                    break;
                } else if in_progress {
                    continue;
                } else {
                    err = err2;
                    break;
                }
            }
        }

        match err {
            Err(e) => {
                match &e {
                    Error::Compact { source } => {
                        match &source {
                            compact::Error::CompactionInProgress { reason } => {
                                if skip_cache_ok {
                                    // e.logger.Warn("Snapshotter busy: proceeding without cache contents")
                                }
                            }
                            _ => {
                                return Err(e);
                            }
                        }
                    }
                    _ => {
                        return Err(e);
                    }
                }
            }
            _ => {}
        }

        if let Some(context) = &self.aggregate_index {
            return context.store.create_snapshot(&self.path).map_err(aggregate_index_error);
        }
        let store = Arc::clone(&self.inner.read().file_store);
        Ok(store.create_snapshot().context(FileStoreSnafu)?)
    }

    fn try_write_snapshot(wal_enabled: bool, inner: Arc<RwLock<Inner>>) -> (Result<()>, bool) {
        match Self::write_snapshot(wal_enabled, inner) {
            Ok(()) => return (Ok(()), false),
            Err(e) => match &e {
                Error::Compact { source } => match &source {
                    compact::Error::CompactionInProgress { reason } => return (Err(e), true),
                    _ => return (Err(e), false),
                },
                _ => return (Err(e), false),
            },
        }
    }

    // Write the passed cache to a new TSM file and remove the closed WAL segments.
    // fn write_snapshot_and_commit(
    //     wal_enabled: bool,
    //     inner: Arc<RwLock<Inner>>,
    //     closed_files: &[String],
    //     snapshot: Arc<Cache>,
    //     cache: &Arc<Cache>,
    //     compactor: &Arc<Compactor>,
    // ) -> Result<()> {
    //     let mut start = std::time::Instant::now();

    //     // write the new snapshot files
    //     let (new_files, new_vlog_files, vlog_indexes, vlog_publish_metadata) =
    //         match compactor.write_snapshot(snapshot) {
    //             Ok((files, vlog_files, vlog_indexes, vlog_publish_metadata)) => {
    //                 (files, vlog_files, vlog_indexes, vlog_publish_metadata)
    //             }
    //             Err(e) => {
    //                 cache.clear_snapshot(false);
    //                 return Err(Error::Compact { source: e });
    //             }
    //         };

    //     println!("\t Compactor.write_snapshot elapsed: {:?}", start.elapsed());
    //     start = std::time::Instant::now();

    //     let inner_gaurd = inner.read();
    //     // update the file store with these new files
    //     if let Err(e) = inner_gaurd.file_store.replace(&[], &new_files) {
    //         // Remove the new snapshot files. We will try again.
    //         for f in new_files.iter() {
    //             let _ = std::fs::remove_file(f);
    //         }
    //         inner_gaurd.cache.clear_snapshot(false);
    //         return Err(Error::FileStore { source: e });
    //     }

    //     println!("\t file_store.replace elapsed: {:?}", start.elapsed());
    //     start = std::time::Instant::now();

    //     if let Some(vlog_inner) = inner_gaurd.vlog_inner.as_ref() {
    //         // vlog enabled
    //         let vlog_inner = vlog_inner.write();
    //         if !vlog_indexes.is_empty() {
    //             // Write all vlog indexes to wal and cache.
    //             vlog_inner
    //                 .vlog_index_cache
    //                 .write_multi(vlog_indexes.clone())
    //                 .context(IndexCacheSnafu)?;
    //             block_on_global(vlog_inner.vlog_index_wal.write_multi(vlog_indexes))
    //                 .context(IndexWALSnafu)?;
    //         }

    //         println!(
    //             "\t vlog_index_Wal.write_multi elapsed: {:?}",
    //             start.elapsed()
    //         );
    //         start = std::time::Instant::now();

    //         // update the vlog file store with these new files
    //         if let Err(e) = vlog_inner.vlog_file_store.replace_with_publish_metadata(
    //             &[],
    //             &new_vlog_files,
    //             &vlog_publish_metadata,
    //             false,
    //         ) {
    //             // Remove the new snapshot files. We will try again.
    //             for f in new_files.iter() {
    //                 let _ = std::fs::remove_file(f);
    //             }
    //             inner_gaurd.cache.clear_snapshot(false);
    //             return Err(Error::VLogFileStore { source: e });
    //         }

    //         println!("\t vlog_file_store.replace elapsed: {:?}", start.elapsed());
    //         start = std::time::Instant::now();
    //     }

    //     // clear the snapshot from the in-memory cache, then the old WAL files
    //     inner_gaurd.cache.clear_snapshot(true);
    //     if wal_enabled {
    //         if let Err(e) = inner_gaurd.wal.remove(closed_files) {
    //             inner_gaurd.cache.clear_snapshot(false);
    //             return Err(Error::WAL { source: e });
    //         }
    //     }

    //     println!("cache.clean_snapshot elapsed: {:?}", start.elapsed());
    //     start = std::time::Instant::now();

    //     Ok(())
    // }

    fn commit_snapshot_outputs(
        wal_enabled: bool,
        inner: Arc<RwLock<Inner>>,
        closed_files: &[String],
        cache: &Arc<Cache>,
        snapshot: &Arc<Cache>,
        outputs: (
            Vec<String>,
            Vec<String>,
            HashMap<String, Vec<vlog::VLogIndex>>,
            Vec<vlog_file_store::VLogFilePublishMeta>,
        ),
    ) -> Result<()> {
        let (new_files, new_vlog_files, mut vlog_indexes, vlog_publish_metadata) = outputs;

        let aggregate_index = inner.read().aggregate_index.clone();
        let index_outputs = if let Some(context) = &aggregate_index {
                context.sync_metadata()?;
                let vlogs = vlog_publish_metadata.iter().map(|f|
                    (f.path.clone(), f.generation, f.seq)).collect::<Vec<_>>();
                let generation = vlogs.iter().map(|(_, g, _)| *g).min().ok_or_else(|| Error::StringErr {
                    reason: "aggregate snapshot has no VLog outputs".into(),
                })?;
                let dir = vlogs[0].0.parent().unwrap();
                let budget = vlog::index::io_budget::IoBudget::new(context.io_limiter.clone(), || false);
                let vlog_files = {
                    let guard = inner.read();
                    Arc::clone(&guard.vlog_inner.as_ref().unwrap().read().vlog_file_store)
                };
                let generations = vlog::index::allocation::OutputGenerations::for_snapshot(generation, vlog_files);
                vlog::index::snapshot::write_index_files_with_budget(dir, generations, snapshot, &vlog_indexes, budget)
                    .map_err(aggregate_index_error)?
        } else { Vec::new() };
        let _aggregate_readers = aggregate_index.as_ref().map(|context|
            context.store.prepare_native(&index_outputs, &mut vlog_indexes)).transpose().map_err(aggregate_index_error)?;

        let inner_gaurd = inner.read();
        // update the file store with these new files
        if let Err(e) = inner_gaurd.file_store.replace(&[], &new_files) {
            // Remove the new snapshot files. We will try again.
            for f in new_files.iter() {
                let _ = std::fs::remove_file(f);
            }
            inner_gaurd.cache.clear_snapshot(false);
            return Err(Error::FileStore { source: e });
        }

        if let Some(vlog_inner) = inner_gaurd.vlog_inner.as_ref() {
            // vlog enabled
            let vlog_inner = vlog_inner.write();
            if !vlog_indexes.is_empty() {
                // Write all vlog indexes to wal and cache.
                vlog_inner
                    .vlog_index_cache
                    .write_multi(vlog_indexes.clone())
                    .context(IndexCacheSnafu)?;
                futures::executor::block_on(vlog_inner.vlog_index_wal.write_multi(vlog_indexes))
                    .context(IndexWALSnafu)?;
            }

            // update the vlog file store with these new files
            if let Err(e) = vlog_inner.vlog_file_store.replace_with_publish_metadata(
                &[],
                &new_vlog_files,
                &vlog_publish_metadata,
                false,
            ) {
                // Remove the new snapshot files. We will try again.
                for f in new_files.iter() {
                    let _ = std::fs::remove_file(f);
                }
                inner_gaurd.cache.clear_snapshot(false);
                return Err(Error::VLogFileStore { source: e });
            }
        }

        // clear the snapshot from the in-memory cache, then the old WAL files
        inner_gaurd.cache.clear_snapshot(true);
        if wal_enabled {
            if let Err(e) = inner_gaurd.wal.remove(closed_files) {
                inner_gaurd.cache.clear_snapshot(false);
                return Err(Error::WAL { source: e });
            }
        }

        drop(inner_gaurd);
        if let Some(context) = &aggregate_index { *context.snapshot_wal.lock() = None; }
        Ok(())
    }

    // Continually checks if the WAL cache should be written to disk.
    async fn compact_cache(
        wal_enabled: bool,
        inner: Arc<RwLock<Inner>>,
        stats: Arc<EngineStatistics>,
        wg: WaitGroup,
        _executor: ExecutorRef,
    ) {
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            let (
                quit,
                cache,
                engine_id,
                cache_size,
                // last_write_time,
                cache_flush_memory_size_threshold,
                cache_flush_write_cold_duration,
            ) = {
                let inner_guard = inner.read();
                (
                    inner_guard.snap_done.clone(),
                    Arc::clone(&inner_guard.cache),
                    inner_guard.engine_id,
                    inner_guard.cache.size(),
                    // inner_guard.cache.last_write_time(),
                    inner_guard.cache_flush_memory_size_threshold,
                    inner_guard.cache_flush_write_cold_duration,
                )
            };

            let inner = Arc::clone(&inner);
            let stats = Arc::clone(&stats);
            match quit {
                Some(quit) => {
                    tokio::select! {
                        _ = ticker.tick() => {
                            cache.update_age();
                            if Self::should_compact_cache2(
                                Time::now(),
                                &cache,
                                cache_flush_memory_size_threshold,
                                cache_flush_write_cold_duration,
                            ) {
                                let reason = if cache_size > cache_flush_memory_size_threshold {
                                    "memory_threshold"
                                } else {
                                    "write_cold"
                                };
                                tracing::debug!(
                                    target: "reefsdb::tsdb::cache",
                                    engine_id,
                                    shard_id = engine_id,
                                    reason,
                                    cache_bytes = cache_size,
                                    threshold_bytes = cache_flush_memory_size_threshold,
                                    "cache snapshot requested"
                                );
                                let start = tokio::time::Instant::now();
                                let snapshot_result =
                                    Self::write_snapshot_async(wal_enabled, inner).await;
                                match snapshot_result {
                                    Ok(_) => {
                                        let _ = stats.cache_compactions.fetch_add(1, Ordering::SeqCst);
                                    }
                                    Err(e) => match &e {
                                        Error::Compact {
                                            source: compact::Error::CompactionsDisabled,
                                        } => {
                                            let _ = stats.cache_compactions.fetch_add(1, Ordering::SeqCst);
                                        }
                                        _ => {
                                            let _ = stats.cache_compaction_errors.fetch_add(1, Ordering::SeqCst);
                                        }
                                    },
                                }
                                let _ = stats
                                    .cache_compaction_duration
                                    .fetch_add(start.elapsed().as_nanos() as i64, Ordering::SeqCst);
                            }
                        },
                        _ = quit.cancelled() => return,
                    }
                }
                None => return,
            }
        }
    }

    fn compact(
        compaction_plan: Arc<DefaultPlanner>,
        compaction_limiter: Option<Arc<Fixed>>,
        mut scheduler: Scheduler,
        wal_enabled: bool,
        inner: Arc<RwLock<Inner>>,
        stats: Arc<EngineStatistics>,
        wg: WaitGroup,
    ) {
        let ticker = crossbeam_channel::tick(Duration::from_secs(1));

        loop {
            let Some(quit) = inner.read().done.clone() else {
                return;
            };
            if !wait_for_compaction_tick(&ticker, &quit) {
                return;
            }
            let inner_guard = inner.read();
            if inner_guard.done.is_none() {
                return;
            }
            let compactor = Arc::clone(&inner_guard.compactor);
            let file_store = Arc::clone(&inner_guard.file_store);
            let last_modified = inner_guard.last_modified(wal_enabled);
            drop(inner_guard);

            Self::compact2(
                &compactor,
                &file_store,
                &compaction_plan,
                &compaction_limiter,
                &mut scheduler,
                wal_enabled,
                last_modified,
                &inner,
                &stats,
                &wg,
            );
        }
    }

    fn compact2(
        compactor: &Arc<Compactor>,
        file_store: &Arc<FileStore>,
        compaction_plan: &Arc<DefaultPlanner>,
        compaction_limiter: &Option<Arc<Fixed>>,
        scheduler: &mut Scheduler,
        wal_enabled: bool,
        last_modified: Time,
        inner: &Arc<RwLock<Inner>>,
        stats: &Arc<EngineStatistics>,
        wg: &WaitGroup,
    ) {
        // Find our compaction plans
        let (engine_id, executor) = {
            let inner = inner.read();
            (inner.engine_id, Arc::clone(&inner.executor))
        };
        let mut l1_groups = compaction_plan.plan_level(1);
        let mut l2_groups = compaction_plan.plan_level(2);
        let mut l3_groups = compaction_plan.plan_level(3);
        let mut l4_groups = compaction_plan.plan(last_modified);

        tracing::debug!(
            target: "reefsdb::tsdb::compaction",
            engine_id,
            shard_id = engine_id,
            level_1_group_count = l1_groups.len(),
            level_2_group_count = l2_groups.len(),
            level_3_group_count = l3_groups.len(),
            level_4_group_count = l4_groups.len(),
            "compaction plan selected"
        );
        stats
            .tsm_optimize_compactions_queue
            .store(l4_groups.len() as i64, Ordering::SeqCst);

        // If no full compactions are need, see if an optimize is needed
        if l4_groups.is_empty() {
            l4_groups = compaction_plan.plan_optimize();
            stats
                .tsm_optimize_compactions_queue
                .store(l4_groups.len() as i64, Ordering::SeqCst);
        }

        // Update the level plan queue stats
        stats.tsm_compactions_queue[0].store(l1_groups.len() as i64, Ordering::SeqCst);
        stats.tsm_compactions_queue[1].store(l2_groups.len() as i64, Ordering::SeqCst);
        stats.tsm_compactions_queue[2].store(l3_groups.len() as i64, Ordering::SeqCst);

        // Set the queue depths on the scheduler
        scheduler.set_depth(1, l1_groups.len() as i64);
        scheduler.set_depth(2, l2_groups.len() as i64);
        scheduler.set_depth(3, l3_groups.len() as i64);
        scheduler.set_depth(4, l4_groups.len() as i64);
        // Find the next compaction that can run and try to kick it off
        if let Some((level, runnable)) = scheduler.next() {
            if runnable {
                match level {
                    1 => {
                        if Self::compact_hi_priority_level(
                            Arc::clone(compactor),
                            Arc::clone(file_store),
                            Arc::clone(compaction_plan),
                            compaction_limiter.clone(),
                            l1_groups[0].clone().into(),
                            1,
                            false,
                            Arc::clone(stats),
                            wg.clone(),
                            engine_id,
                            Arc::clone(&executor),
                        ) {
                            l1_groups.drain(..1);
                        }
                    }
                    2 => {
                        if Self::compact_hi_priority_level(
                            Arc::clone(compactor),
                            Arc::clone(file_store),
                            Arc::clone(compaction_plan),
                            compaction_limiter.clone(),
                            l2_groups[0].clone().into(),
                            2,
                            false,
                            Arc::clone(stats),
                            wg.clone(),
                            engine_id,
                            Arc::clone(&executor),
                        ) {
                            l2_groups.drain(..1);
                        }
                    }
                    3 => {
                        if Self::compact_lo_priority_level(
                            Arc::clone(compactor),
                            Arc::clone(file_store),
                            Arc::clone(compaction_plan),
                            compaction_limiter.clone(),
                            l3_groups[0].clone().into(),
                            3,
                            true,
                            Arc::clone(stats),
                            wg.clone(),
                            engine_id,
                            Arc::clone(&executor),
                        ) {
                            l3_groups.drain(..1);
                        }
                    }
                    4 => {
                        if Self::compact_full(
                            Arc::clone(compactor),
                            Arc::clone(file_store),
                            Arc::clone(compaction_plan),
                            compaction_limiter.clone(),
                            l4_groups[0].clone().into(),
                            Arc::clone(stats),
                            wg.clone(),
                            engine_id,
                            Arc::clone(&executor),
                        ) {
                            l4_groups.drain(..1);
                        }
                    }
                    _ => {}
                }
            }
        }

        // Release all the plans we didn't start.
        compaction_plan.release(&l1_groups);
        compaction_plan.release(&l2_groups);
        compaction_plan.release(&l3_groups);
        compaction_plan.release(&l4_groups);
    }

    /// Kicks off compactions using the high priority policy. It returns
    /// true if ownership of the selected plan was consumed by this attempt.
    fn compact_hi_priority_level(
        compactor: Arc<Compactor>,
        file_store: Arc<FileStore>,
        compaction_plan: Arc<DefaultPlanner>,
        compaction_limiter: Option<Arc<Fixed>>,
        grp: CompactionGroup,
        level: i64,
        fast: bool,
        stats: Arc<EngineStatistics>,
        wg: WaitGroup,
        engine_id: u64,
        executor: ExecutorRef,
    ) -> bool {
        let mut s = Self::level_compaction_strategy(
            &stats, compactor, file_store, grp, fast, level, engine_id,
        );

        // Try hi priority limiter, otherwise steal a little from the low priority if we can.
        if let Some(compaction_limiter) = compaction_limiter {
            if compaction_limiter.try_take() {
                stats.tsm_compactions_active[level as usize - 1].fetch_add(1, Ordering::SeqCst);

                wg.add(1);
                let group: CompactionGroup = s.group.to_vec().into();
                let cleanup = scopeguard::guard((), move |_| {
                    compaction_plan.release(&[group]);
                    compaction_limiter.release();
                    stats.tsm_compactions_active[level as usize - 1].fetch_sub(1, Ordering::SeqCst);
                    wg.done();
                });
                if let Err(error) = executor.spawn_blocking_compact(move || {
                    let _cleanup = cleanup;
                    s.apply();
                }) {
                    tracing::error!(
                        target: "reefsdb::tsdb::compaction",
                        engine_id,
                        level,
                        error = %error,
                        "compaction task scheduling failed"
                    );
                    return true;
                }

                return true;
            }
        }

        // Return the unused plans
        false
    }

    /// Kicks off compactions using the lo priority policy. It returns
    /// true if ownership of the selected plan was consumed by this attempt.
    fn compact_lo_priority_level(
        compactor: Arc<Compactor>,
        file_store: Arc<FileStore>,
        compaction_plan: Arc<DefaultPlanner>,
        compaction_limiter: Option<Arc<Fixed>>,
        grp: CompactionGroup,
        level: i64,
        fast: bool,
        stats: Arc<EngineStatistics>,
        wg: WaitGroup,
        engine_id: u64,
        executor: ExecutorRef,
    ) -> bool {
        let mut s = Self::level_compaction_strategy(
            &stats, compactor, file_store, grp, fast, level, engine_id,
        );

        // Try the lo priority limiter, otherwise steal a little from the high priority if we can.
        if let Some(compaction_limiter) = compaction_limiter {
            if compaction_limiter.try_take() {
                stats.tsm_compactions_active[level as usize - 1].fetch_add(1, Ordering::SeqCst);

                wg.add(1);
                let group: CompactionGroup = s.group.to_vec().into();
                let cleanup = scopeguard::guard((), move |_| {
                    compaction_plan.release(&[group]);
                    compaction_limiter.release();
                    stats.tsm_compactions_active[level as usize - 1].fetch_sub(1, Ordering::SeqCst);
                    wg.done();
                });
                if let Err(error) = executor.spawn_blocking_compact(move || {
                    let _cleanup = cleanup;
                    s.apply();
                }) {
                    tracing::error!(
                        target: "reefsdb::tsdb::compaction",
                        engine_id,
                        level,
                        error = %error,
                        "compaction task scheduling failed"
                    );
                    return true;
                }

                return true;
            }
        }

        // Return the unused plans
        false
    }

    /// Kicks off full and optimize compactions using the lo priority policy. It returns
    /// true if ownership of the selected plan was consumed by this attempt.
    fn compact_full(
        compactor: Arc<Compactor>,
        file_store: Arc<FileStore>,
        compaction_plan: Arc<DefaultPlanner>,
        compaction_limiter: Option<Arc<Fixed>>,
        grp: CompactionGroup,
        stats: Arc<EngineStatistics>,
        wg: WaitGroup,
        engine_id: u64,
        executor: ExecutorRef,
    ) -> bool {
        let mut s =
            Self::full_compaction_strategy(compactor, file_store, grp, false, &stats, engine_id);

        // Try the lo priority limiter, otherwise steal a little from the high priority if we can.
        if let Some(compaction_limiter) = compaction_limiter {
            if compaction_limiter.try_take() {
                stats
                    .tsm_full_compactions_active
                    .fetch_add(1, Ordering::SeqCst);

                wg.add(1);
                let group: CompactionGroup = s.group.to_vec().into();
                let cleanup = scopeguard::guard((), move |_| {
                    compaction_plan.release(&[group]);
                    compaction_limiter.release();
                    stats
                        .tsm_full_compactions_active
                        .fetch_sub(1, Ordering::SeqCst);
                    wg.done();
                });
                if let Err(error) = executor.spawn_blocking_compact(move || {
                    let _cleanup = cleanup;
                    s.apply();
                }) {
                    tracing::error!(
                        target: "reefsdb::tsdb::compaction",
                        engine_id,
                        level = 4,
                        error = %error,
                        "full compaction task scheduling failed"
                    );
                    return true;
                }

                return true;
            }
        }

        false
    }

    /// Returns a CompactionStrategy for the given level.
    /// It returns nil if there are no TSM files to compact.
    fn level_compaction_strategy(
        // inner: Arc<RwLock<Inner>>,
        stats: &Arc<EngineStatistics>,
        compactor: Arc<Compactor>,
        file_store: Arc<FileStore>,
        group: CompactionGroup,
        fast: bool,
        level: i64,
        engine_id: u64,
    ) -> CompactionStrategy {
        CompactionStrategy {
            engine_id,
            group,
            fast,
            level,

            duration_stat: Arc::clone(&stats.tsm_compaction_duration[level as usize - 1]),
            active_stat: Arc::clone(&stats.tsm_compactions_active[level as usize - 1]),
            success_stat: Arc::clone(&stats.tsm_compactions[level as usize - 1]),
            error_stat: Arc::clone(&stats.tsm_compaction_errors[level as usize - 1]),

            // inner,
            compactor,
            file_store,
        }
    }

    /// Returns a CompactionStrategy for higher level generations of TSM files.
    /// It returns nil if there are no TSM files to compact.
    fn full_compaction_strategy(
        // inner: Arc<RwLock<Inner>>,
        compactor: Arc<Compactor>,
        file_store: Arc<FileStore>,
        group: CompactionGroup,
        optimize: bool,
        stats: &Arc<EngineStatistics>,
        engine_id: u64,
    ) -> CompactionStrategy {
        if optimize {
            CompactionStrategy {
                engine_id,
                group,
                fast: optimize,
                level: 4,

                active_stat: Arc::clone(&stats.tsm_optimize_compactions_active),
                success_stat: Arc::clone(&stats.tsm_optimize_compactions),
                error_stat: Arc::clone(&stats.tsm_optimize_compaction_errors),
                duration_stat: Arc::clone(&stats.tsm_optimize_compaction_duration),

                // inner,
                compactor,
                file_store,
            }
        } else {
            CompactionStrategy {
                engine_id,
                group,
                fast: optimize,
                level: 4,

                active_stat: Arc::clone(&stats.tsm_full_compactions_active),
                success_stat: Arc::clone(&stats.tsm_full_compactions),
                error_stat: Arc::clone(&stats.tsm_full_compaction_errors),
                duration_stat: Arc::clone(&stats.tsm_full_compaction_duration),

                // inner,
                compactor,
                file_store,
            }
        }
    }

    async fn compact_vlog(
        compaction_plan: VLogCompactPlanner,
        compaction_limiter: Option<Arc<Fixed>>,
        mut scheduler: VLogScheduler,
        inner: Arc<RwLock<Inner>>,
        stats: Arc<EngineStatistics>,
        wg: WaitGroup,
    ) {
        let start = tokio::time::Instant::now() + Duration::from_secs(1);
        let mut ticker = tokio::time::interval_at(start, Duration::from_secs(1));

        loop {
            let Some(quit) = inner.read().vlog_done.clone() else {
                return;
            };
            tokio::select! {
                _ = ticker.tick() => {}
                _ = quit.token.cancelled() => return,
            }
            let inner_guard = inner.read();
            if inner_guard.vlog_done.is_none() {
                return;
            }
            let vlog_inner = Arc::clone(inner_guard.vlog_inner.as_ref().unwrap());
            let compactor = Arc::clone(inner_guard.vlog_compactor.as_ref().unwrap());
            let last_modified = inner_guard.vlog_last_modified();
            drop(inner_guard);

            Self::compact_vlog2(
                &compactor,
                &vlog_inner,
                &compaction_plan,
                &compaction_limiter,
                &mut scheduler,
                last_modified,
                &inner,
                &stats,
                &wg,
            );
        }
    }

    fn compact_vlog2(
        compactor: &Arc<VLogCompactor>,
        vlog_inner: &Arc<RwLock<VLogInner>>,
        compaction_plan: &VLogCompactPlanner,
        compaction_limiter: &Option<Arc<Fixed>>,
        scheduler: &mut VLogScheduler,
        last_modified: Time,
        inner: &Arc<RwLock<Inner>>,
        stats: &Arc<EngineStatistics>,
        wg: &WaitGroup,
    ) {
        let executor = Arc::clone(&inner.read().executor);
        match compaction_plan {
            VLogCompactPlanner::VLogDefaultPlanner(vlog_default_planner) => {
                Self::compact_vlog_default(
                    compactor,
                    vlog_inner,
                    vlog_default_planner,
                    compaction_limiter,
                    scheduler,
                    last_modified,
                    inner,
                    stats,
                    wg,
                    &executor,
                )
            }
            VLogCompactPlanner::VLogLazyPlanner(vlog_lazy_planner) => Self::compact_vlog_lazy(
                compactor,
                vlog_inner,
                vlog_lazy_planner,
                compaction_limiter,
                scheduler,
                last_modified,
                inner,
                stats,
                wg,
                &executor,
            ),
        }
    }

    fn compact_vlog_default(
        compactor: &Arc<VLogCompactor>,
        vlog_inner: &Arc<RwLock<VLogInner>>,
        compaction_plan: &Arc<VLogDefaultPlanner>,
        compaction_limiter: &Option<Arc<Fixed>>,
        scheduler: &mut VLogScheduler,
        last_modified: Time,
        inner: &Arc<RwLock<Inner>>,
        stats: &Arc<EngineStatistics>,
        wg: &WaitGroup,
        executor: &ExecutorRef,
    ) {
        // Find our compaction plans
        let mut l1_groups = compaction_plan.plan_level(1);
        let mut l2_groups = compaction_plan.plan_level(2);
        let mut l3_groups = compaction_plan.plan_level(3);
        let mut l4_groups = compaction_plan.plan(last_modified);
        stats
            .vlog_optimize_compactions_queue
            .store(l4_groups.len() as i64, Ordering::SeqCst);

        // If no full compactions are need, see if an optimize is needed
        if l4_groups.is_empty() {
            l4_groups = compaction_plan.plan_optimize();
            stats
                .vlog_optimize_compactions_queue
                .store(l4_groups.len() as i64, Ordering::SeqCst);
        }

        // Update the level plan queue stats
        stats.vlog_compactions_queue[0].store(l1_groups.len() as i64, Ordering::SeqCst);
        stats.vlog_compactions_queue[1].store(l2_groups.len() as i64, Ordering::SeqCst);
        stats.vlog_compactions_queue[2].store(l3_groups.len() as i64, Ordering::SeqCst);

        // Set the queue depths on the scheduler
        scheduler.set_depth(1, l1_groups.len() as i64);
        scheduler.set_depth(2, l2_groups.len() as i64);
        scheduler.set_depth(3, l3_groups.len() as i64);
        scheduler.set_depth(4, l4_groups.len() as i64);
        // Find the next compaction that can run and try to kick it off
        if let Some((level, runnable)) = scheduler.next() {
            if runnable {
                match level {
                    1 => {
                        if Self::vlog_compact_hi_priority_level_default(
                            Arc::clone(compactor),
                            Arc::clone(vlog_inner),
                            Arc::clone(compaction_plan),
                            compaction_limiter.clone(),
                            l1_groups[0].clone().into(),
                            1,
                            false,
                            Arc::clone(stats),
                            wg.clone(),
                            Arc::clone(executor),
                        ) {
                            l1_groups.drain(..1);
                        }
                    }
                    2 => {
                        if Self::vlog_compact_hi_priority_level_default(
                            Arc::clone(compactor),
                            Arc::clone(vlog_inner),
                            Arc::clone(compaction_plan),
                            compaction_limiter.clone(),
                            l2_groups[0].clone().into(),
                            2,
                            false,
                            Arc::clone(stats),
                            wg.clone(),
                            Arc::clone(executor),
                        ) {
                            l2_groups.drain(..1);
                        }
                    }
                    3 => {
                        if Self::vlog_compact_lo_priority_level_default(
                            Arc::clone(compactor),
                            Arc::clone(vlog_inner),
                            Arc::clone(compaction_plan),
                            compaction_limiter.clone(),
                            l3_groups[0].clone().into(),
                            3,
                            true,
                            Arc::clone(stats),
                            wg.clone(),
                            Arc::clone(executor),
                        ) {
                            l3_groups.drain(..1);
                        }
                    }
                    4 => {
                        if Self::vlog_compact_full_default(
                            Arc::clone(compactor),
                            Arc::clone(vlog_inner),
                            Arc::clone(compaction_plan),
                            compaction_limiter.clone(),
                            l4_groups[0].clone().into(),
                            Arc::clone(stats),
                            wg.clone(),
                            Arc::clone(executor),
                        ) {
                            l4_groups.drain(..1);
                        }
                    }
                    _ => {}
                }
            }
        }

        // Release all the plans we didn't start.
        compaction_plan.release(&l1_groups);
        compaction_plan.release(&l2_groups);
        compaction_plan.release(&l3_groups);
        compaction_plan.release(&l4_groups);
    }

    /// Kicks off compactions using the high priority policy. It returns
    /// true if ownership of the selected plan was consumed by this attempt.
    fn vlog_compact_hi_priority_level_default(
        compactor: Arc<VLogCompactor>,
        vlog_inner: Arc<RwLock<VLogInner>>,
        compaction_plan: Arc<VLogDefaultPlanner>,
        compaction_limiter: Option<Arc<Fixed>>,
        grp: VLogCompactionGroup,
        level: i64,
        fast: bool,
        stats: Arc<EngineStatistics>,
        wg: WaitGroup,
        executor: ExecutorRef,
    ) -> bool {
        let mut s =
            Self::vlog_compaction_strategy_default(vlog_inner, &stats, compactor, grp, fast, level);

        // Try hi priority limiter, otherwise steal a little from the low priority if we can.
        if let Some(compaction_limiter) = compaction_limiter {
            if compaction_limiter.try_take() {
                stats.vlog_compactions_active[level as usize - 1].fetch_add(1, Ordering::SeqCst);

                wg.add(1);
                let group: VLogCompactionGroup = s.group.to_vec().into();
                let cleanup = scopeguard::guard((), move |_| {
                    compaction_plan.release(&[group]);
                    compaction_limiter.release();
                    stats.vlog_compactions_active[level as usize - 1]
                        .fetch_sub(1, Ordering::SeqCst);
                    wg.done();
                });
                if let Err(error) = executor.spawn_compact(async move {
                    let _cleanup = cleanup;
                    s.apply().await;
                }) {
                    tracing::error!(
                        target: "reefsdb::tsdb::vlog_compaction",
                        level,
                        error = %error,
                        "VLog compaction task scheduling failed"
                    );
                    return true;
                }

                return true;
            }
        }

        // Return the unused plans
        false
    }

    /// Kicks off compactions using the lo priority policy. It returns
    /// true if ownership of the selected plan was consumed by this attempt.
    fn vlog_compact_lo_priority_level_default(
        compactor: Arc<VLogCompactor>,
        vlog_inner: Arc<RwLock<VLogInner>>,
        compaction_plan: Arc<VLogDefaultPlanner>,
        compaction_limiter: Option<Arc<Fixed>>,
        grp: VLogCompactionGroup,
        level: i64,
        fast: bool,
        stats: Arc<EngineStatistics>,
        wg: WaitGroup,
        executor: ExecutorRef,
    ) -> bool {
        let mut s =
            Self::vlog_compaction_strategy_default(vlog_inner, &stats, compactor, grp, fast, level);

        // Try the lo priority limiter, otherwise steal a little from the high priority if we can.
        if let Some(compaction_limiter) = compaction_limiter {
            if compaction_limiter.try_take() {
                stats.vlog_compactions_active[level as usize - 1].fetch_add(1, Ordering::SeqCst);

                wg.add(1);
                let group: VLogCompactionGroup = s.group.to_vec().into();
                let cleanup = scopeguard::guard((), move |_| {
                    compaction_plan.release(&[group]);
                    compaction_limiter.release();
                    stats.vlog_compactions_active[level as usize - 1]
                        .fetch_sub(1, Ordering::SeqCst);
                    wg.done();
                });
                if let Err(error) = executor.spawn_compact(async move {
                    let _cleanup = cleanup;
                    s.apply().await;
                }) {
                    tracing::error!(
                        target: "reefsdb::tsdb::vlog_compaction",
                        level,
                        error = %error,
                        "VLog compaction task scheduling failed"
                    );
                    return true;
                }

                return true;
            }
        }

        // Return the unused plans
        false
    }

    /// Kicks off full and optimize compactions using the lo priority policy. It returns
    /// true if ownership of the selected plan was consumed by this attempt.
    fn vlog_compact_full_default(
        compactor: Arc<VLogCompactor>,
        vlog_inner: Arc<RwLock<VLogInner>>,
        compaction_plan: Arc<VLogDefaultPlanner>,
        compaction_limiter: Option<Arc<Fixed>>,
        grp: VLogCompactionGroup,
        stats: Arc<EngineStatistics>,
        wg: WaitGroup,
        executor: ExecutorRef,
    ) -> bool {
        let mut s =
            Self::vlog_full_compaction_strategy_default(vlog_inner, compactor, grp, false, &stats);

        // Try the lo priority limiter, otherwise steal a little from the high priority if we can.
        if let Some(compaction_limiter) = compaction_limiter {
            if compaction_limiter.try_take() {
                stats
                    .vlog_full_compactions_active
                    .fetch_add(1, Ordering::SeqCst);

                wg.add(1);
                let group: VLogCompactionGroup = s.group.to_vec().into();
                let cleanup = scopeguard::guard((), move |_| {
                    compaction_plan.release(&[group]);
                    compaction_limiter.release();
                    stats
                        .vlog_full_compactions_active
                        .fetch_sub(1, Ordering::SeqCst);
                    wg.done();
                });
                if let Err(error) = executor.spawn_compact(async move {
                    let _cleanup = cleanup;
                    s.apply().await;
                }) {
                    tracing::error!(
                        target: "reefsdb::tsdb::vlog_compaction",
                        level = 4,
                        error = %error,
                        "full VLog compaction task scheduling failed"
                    );
                    return true;
                }

                return true;
            }
        }

        false
    }

    /// Returns a CompactionStrategy for the given level.
    /// It returns nil if there are no TSM files to compact.
    fn vlog_compaction_strategy_default(
        inner: Arc<RwLock<VLogInner>>,
        stats: &Arc<EngineStatistics>,
        compactor: Arc<VLogCompactor>,
        group: VLogCompactionGroup,
        fast: bool,
        level: i64,
    ) -> VLogDefaultCompactionStrategy {
        VLogDefaultCompactionStrategy {
            group,
            fast,
            level,

            duration_stat: Arc::clone(&stats.vlog_compaction_duration[level as usize - 1]),
            active_stat: Arc::clone(&stats.vlog_compactions_active[level as usize - 1]),
            success_stat: Arc::clone(&stats.vlog_compactions[level as usize - 1]),
            error_stat: Arc::clone(&stats.vlog_compaction_errors[level as usize - 1]),

            compactor,
            inner,
        }
    }

    /// Returns a CompactionStrategy for higher level generations of TSM files.
    /// It returns nil if there are no TSM files to compact.
    fn vlog_full_compaction_strategy_default(
        inner: Arc<RwLock<VLogInner>>,
        compactor: Arc<VLogCompactor>,
        group: VLogCompactionGroup,
        optimize: bool,
        stats: &Arc<EngineStatistics>,
    ) -> VLogDefaultCompactionStrategy {
        if optimize {
            VLogDefaultCompactionStrategy {
                group,
                fast: optimize,
                level: 4,

                active_stat: Arc::clone(&stats.vlog_optimize_compactions_active),
                success_stat: Arc::clone(&stats.vlog_optimize_compactions),
                error_stat: Arc::clone(&stats.vlog_optimize_compaction_errors),
                duration_stat: Arc::clone(&stats.vlog_optimize_compaction_duration),

                compactor,
                inner,
            }
        } else {
            VLogDefaultCompactionStrategy {
                group,
                fast: optimize,
                level: 4,

                active_stat: Arc::clone(&stats.vlog_full_compactions_active),
                success_stat: Arc::clone(&stats.vlog_full_compactions),
                error_stat: Arc::clone(&stats.vlog_full_compaction_errors),
                duration_stat: Arc::clone(&stats.vlog_full_compaction_duration),

                compactor,
                inner,
            }
        }
    }

    // ////////////////////////////////////////////////////////////////
    // ////////////////////////////////////////////////////////////////
    // ////////////////////////////////////////////////////////////////
    // ////////////////////////////////////////////////////////////////

    fn compact_vlog_lazy(
        compactor: &Arc<VLogCompactor>,
        vlog_inner: &Arc<RwLock<VLogInner>>,
        compaction_plan: &Arc<VLogLazyPlanner>,
        compaction_limiter: &Option<Arc<Fixed>>,
        scheduler: &mut VLogScheduler,
        last_modified: Time,
        inner: &Arc<RwLock<Inner>>,
        stats: &Arc<EngineStatistics>,
        wg: &WaitGroup,
        executor: &ExecutorRef,
    ) {
        // Find our compaction plans
        let l1_compact_info = compaction_plan.plan_level(1);
        let l2_compact_info = compaction_plan.plan_level(2);
        let l3_compact_info = compaction_plan.plan_level(3);
        let mut l4_compact_info = compaction_plan.plan(last_modified);

        // let l1_len = if l1_compact_info.is_empty() { 0 } else { l1_compact_info.files.len() };
        // let l2_len = if l2_compact_info.is_empty() { 0 } else { l1_compact_info.files.len() };
        // let l3_len = if l3_compact_info.is_empty() { 0 } else { l1_compact_info.files.len() };
        // let mut l4_len = if l4_compact_info.is_empty() { 0 } else { l1_compact_info.files.len() };

        let l1_len = l1_compact_info.files_len() as i64;
        let l2_len = l2_compact_info.files_len() as i64;
        let l3_len = l3_compact_info.files_len() as i64;
        let mut l4_len = l4_compact_info.files_len() as i64;

        let mut l1_files = l1_compact_info.files.clone();
        let mut l2_files = l2_compact_info.files.clone();
        let mut l3_files = l3_compact_info.files.clone();
        let mut l4_files = l4_compact_info.files.clone();

        stats
            .vlog_optimize_compactions_queue
            .store(l4_len, Ordering::SeqCst);

        // If no full compactions are need, see if an optimize is needed
        if l4_compact_info.is_empty() {
            l4_compact_info = compaction_plan.plan_optimize();
            l4_files = l4_compact_info.files.clone();
            l4_len = if l4_compact_info.is_empty() { 0 } else { 1 };
            stats
                .vlog_optimize_compactions_queue
                .store(l4_len, Ordering::SeqCst);
        }

        // Update the level plan queue stats
        stats.vlog_compactions_queue[0].store(l1_len, Ordering::SeqCst);
        stats.vlog_compactions_queue[1].store(l2_len, Ordering::SeqCst);
        stats.vlog_compactions_queue[2].store(l3_len, Ordering::SeqCst);

        // Set the queue depths on the scheduler
        scheduler.set_depth(1, l1_len);
        scheduler.set_depth(2, l2_len);
        scheduler.set_depth(3, l3_len);
        scheduler.set_depth(4, l4_len);

        // Find the next compaction that can run and try to kick it off
        if let Some((level, runnable)) = scheduler.next() {
            if runnable {
                match level {
                    1 => {
                        // println!("====================== compact_vlog_lazy....1");
                        if Self::vlog_compact_hi_priority_level_lazy(
                            Arc::clone(compactor),
                            Arc::clone(vlog_inner),
                            Arc::clone(compaction_plan),
                            compaction_limiter.clone(),
                            l1_compact_info,
                            1,
                            false,
                            Arc::clone(stats),
                            wg.clone(),
                            Arc::clone(executor),
                        ) {
                            l1_files.clear();
                        } else {
                            // println!("====================== compact_vlog_lazy....1 failed");
                        }
                    }
                    2 => {
                        // println!("====================== compact_vlog_lazy....2");
                        if Self::vlog_compact_hi_priority_level_lazy(
                            Arc::clone(compactor),
                            Arc::clone(vlog_inner),
                            Arc::clone(compaction_plan),
                            compaction_limiter.clone(),
                            l2_compact_info,
                            2,
                            false,
                            Arc::clone(stats),
                            wg.clone(),
                            Arc::clone(executor),
                        ) {
                            l2_files.clear();
                            // println!("====================== compact_vlog_lazy....2 ok");
                        } else {
                            // println!("====================== compact_vlog_lazy....1 failed");
                        }
                    }
                    3 => {
                        // println!("====================== compact_vlog_lazy....3");
                        if Self::vlog_compact_lo_priority_level_lazy(
                            Arc::clone(compactor),
                            Arc::clone(vlog_inner),
                            Arc::clone(compaction_plan),
                            compaction_limiter.clone(),
                            l3_compact_info,
                            3,
                            true,
                            Arc::clone(stats),
                            wg.clone(),
                            Arc::clone(executor),
                        ) {
                            l3_files.clear();
                            // println!("====================== compact_vlog_lazy....3 ok");
                        } else {
                            // println!("====================== compact_vlog_lazy....3 failed");
                        }
                    }
                    4 => {
                        // println!("====================== compact_vlog_lazy....4");
                        if Self::vlog_compact_full_lazy(
                            Arc::clone(compactor),
                            Arc::clone(vlog_inner),
                            Arc::clone(compaction_plan),
                            compaction_limiter.clone(),
                            l4_compact_info,
                            Arc::clone(stats),
                            wg.clone(),
                            Arc::clone(executor),
                        ) {
                            l4_files.clear();
                            // println!("====================== compact_vlog_lazy....4 ok");
                        } else {
                            // println!("====================== compact_vlog_lazy....4 failed");
                        }
                    }
                    _ => {}
                }
            }
        }

        // Release all the plans we didn't start.
        compaction_plan.release(&l1_files);
        compaction_plan.release(&l2_files);
        compaction_plan.release(&l3_files);
        compaction_plan.release(&l4_files);
    }

    /// Kicks off compactions using the high priority policy. It returns
    /// true if ownership of the selected plan was consumed by this attempt.
    fn vlog_compact_hi_priority_level_lazy(
        compactor: Arc<VLogCompactor>,
        vlog_inner: Arc<RwLock<VLogInner>>,
        compaction_plan: Arc<VLogLazyPlanner>,
        compaction_limiter: Option<Arc<Fixed>>,
        vlog_compact_info: VLogCompactInfo,
        level: i64,
        fast: bool,
        stats: Arc<EngineStatistics>,
        wg: WaitGroup,
        executor: ExecutorRef,
    ) -> bool {
        let mut s = Self::vlog_compaction_strategy_lazy(
            // compaction_plan.compact_done.clone(),
            vlog_inner,
            &stats,
            compactor,
            vlog_compact_info,
            fast,
            level,
        );

        // Try hi priority limiter, otherwise steal a little from the low priority if we can.
        if let Some(compaction_limiter) = compaction_limiter {
            if compaction_limiter.try_take() {
                stats.vlog_compactions_active[level as usize - 1].fetch_add(1, Ordering::SeqCst);

                wg.add(1);
                let files = s.compact_info.files.clone();
                let apply_plan = Arc::clone(&compaction_plan);
                let cleanup = scopeguard::guard((), move |_| {
                    compaction_plan.release(&files);
                    compaction_limiter.release();
                    stats.vlog_compactions_active[level as usize - 1]
                        .fetch_sub(1, Ordering::SeqCst);
                    wg.done();
                });
                if let Err(error) = executor.spawn_compact(async move {
                    let _cleanup = cleanup;
                    s.apply(&apply_plan).await;
                }) {
                    tracing::error!(
                        target: "reefsdb::tsdb::vlog_compaction",
                        level,
                        error = %error,
                        "lazy VLog compaction task scheduling failed"
                    );
                    return true;
                }

                return true;
            }
        }

        // Return the unused plans
        false
    }

    /// Kicks off compactions using the lo priority policy. It returns
    /// true if ownership of the selected plan was consumed by this attempt.
    fn vlog_compact_lo_priority_level_lazy(
        compactor: Arc<VLogCompactor>,
        vlog_inner: Arc<RwLock<VLogInner>>,
        compaction_plan: Arc<VLogLazyPlanner>,
        compaction_limiter: Option<Arc<Fixed>>,
        vlog_compact_info: VLogCompactInfo,
        level: i64,
        fast: bool,
        stats: Arc<EngineStatistics>,
        wg: WaitGroup,
        executor: ExecutorRef,
    ) -> bool {
        let mut s = Self::vlog_compaction_strategy_lazy(
            // compaction_plan.compact_done.clone(),
            vlog_inner,
            &stats,
            compactor,
            vlog_compact_info,
            fast,
            level,
        );

        // Try the lo priority limiter, otherwise steal a little from the high priority if we can.
        if let Some(compaction_limiter) = compaction_limiter {
            if compaction_limiter.try_take() {
                stats.vlog_compactions_active[level as usize - 1].fetch_add(1, Ordering::SeqCst);

                wg.add(1);
                let files = s.compact_info.files.clone();
                let apply_plan = Arc::clone(&compaction_plan);
                let cleanup = scopeguard::guard((), move |_| {
                    compaction_plan.release(&files);
                    compaction_limiter.release();
                    stats.vlog_compactions_active[level as usize - 1]
                        .fetch_sub(1, Ordering::SeqCst);
                    wg.done();
                });
                if let Err(error) = executor.spawn_compact(async move {
                    let _cleanup = cleanup;
                    s.apply(&apply_plan).await;
                }) {
                    tracing::error!(
                        target: "reefsdb::tsdb::vlog_compaction",
                        level,
                        error = %error,
                        "lazy VLog compaction task scheduling failed"
                    );
                    return true;
                }

                return true;
            }
        }

        // Return the unused plans
        false
    }

    /// Kicks off full and optimize compactions using the lo priority policy. It returns
    /// true if ownership of the selected plan was consumed by this attempt.
    fn vlog_compact_full_lazy(
        compactor: Arc<VLogCompactor>,
        vlog_inner: Arc<RwLock<VLogInner>>,
        compaction_plan: Arc<VLogLazyPlanner>,
        compaction_limiter: Option<Arc<Fixed>>,
        vlog_compact_info: VLogCompactInfo,
        stats: Arc<EngineStatistics>,
        wg: WaitGroup,
        executor: ExecutorRef,
    ) -> bool {
        let mut s = Self::vlog_full_compaction_strategy_lazy(
            vlog_inner,
            compactor,
            vlog_compact_info,
            false,
            &stats,
        );

        // Try the lo priority limiter, otherwise steal a little from the high priority if we can.
        if let Some(compaction_limiter) = compaction_limiter {
            if compaction_limiter.try_take() {
                stats
                    .vlog_full_compactions_active
                    .fetch_add(1, Ordering::SeqCst);

                wg.add(1);
                let files = s.compact_info.files.clone();
                let apply_plan = Arc::clone(&compaction_plan);
                let cleanup = scopeguard::guard((), move |_| {
                    compaction_plan.release(&files);
                    compaction_limiter.release();
                    stats
                        .vlog_full_compactions_active
                        .fetch_sub(1, Ordering::SeqCst);
                    wg.done();
                });
                if let Err(error) = executor.spawn_compact(async move {
                    let _cleanup = cleanup;
                    s.apply(&apply_plan).await;
                }) {
                    tracing::error!(
                        target: "reefsdb::tsdb::vlog_compaction",
                        level = 4,
                        error = %error,
                        "full lazy VLog compaction task scheduling failed"
                    );
                    return true;
                }

                return true;
            }
        }

        false
    }

    /// Returns a CompactionStrategy for the given level.
    /// It returns nil if there are no TSM files to compact.
    fn vlog_compaction_strategy_lazy(
        inner: Arc<RwLock<VLogInner>>,
        stats: &Arc<EngineStatistics>,
        compactor: Arc<VLogCompactor>,
        compact_info: VLogCompactInfo,
        fast: bool,
        level: i64,
    ) -> VLogLazyCompactionStrategy {
        VLogLazyCompactionStrategy {
            compact_info,
            fast,
            level,

            duration_stat: Arc::clone(&stats.vlog_compaction_duration[level as usize - 1]),
            active_stat: Arc::clone(&stats.vlog_compactions_active[level as usize - 1]),
            success_stat: Arc::clone(&stats.vlog_compactions[level as usize - 1]),
            error_stat: Arc::clone(&stats.vlog_compaction_errors[level as usize - 1]),

            compactor,

            inner,
        }
    }

    /// Returns a CompactionStrategy for higher level generations of TSM files.
    /// It returns nil if there are no TSM files to compact.
    fn vlog_full_compaction_strategy_lazy(
        // compact_done: WaitGroup,
        inner: Arc<RwLock<VLogInner>>,
        compactor: Arc<VLogCompactor>,
        compact_info: VLogCompactInfo,
        optimize: bool,
        stats: &Arc<EngineStatistics>,
    ) -> VLogLazyCompactionStrategy {
        if optimize {
            VLogLazyCompactionStrategy {
                compact_info,
                fast: optimize,
                level: 4,

                active_stat: Arc::clone(&stats.vlog_optimize_compactions_active),
                success_stat: Arc::clone(&stats.vlog_optimize_compactions),
                error_stat: Arc::clone(&stats.vlog_optimize_compaction_errors),
                duration_stat: Arc::clone(&stats.vlog_optimize_compaction_duration),

                compactor,
                inner,
            }
        } else {
            VLogLazyCompactionStrategy {
                compact_info,
                fast: optimize,
                level: 4,

                active_stat: Arc::clone(&stats.vlog_full_compactions_active),
                success_stat: Arc::clone(&stats.vlog_full_compactions),
                error_stat: Arc::clone(&stats.vlog_full_compaction_errors),
                duration_stat: Arc::clone(&stats.vlog_full_compaction_duration),

                compactor,
                inner,
            }
        }
    }

    // ////////////////////////////////////////////////////////////////
    // ////////////////////////////////////////////////////////////////
    // ////////////////////////////////////////////////////////////////
    // ////////////////////////////////////////////////////////////////

    /// Reads the WAL segment files and loads them into the cache.
    fn reload_cache(cache: &Cache, wal_path: &Path) -> Result<()> {
        let files = segment_file_names(wal_path).context(WALSnafu)?;
        let limit = cache.max_size();

        // Disable the max size during loading
        cache.set_max_size(0);
        let _restore_limit = scopeguard::guard((), |_| cache.set_max_size(limit));

        let loader = CacheLoader::new(files);
        if let Err(e) = loader.load(cache) {
            return Err(Error::Cache { source: e });
        }

        Ok(())
    }

    /// Reads the VLog WAL segment files and loads them into the cache.
    fn reload_vlog_cache(cache: &Cache, wal_path: &Path) -> Result<()> {
        let files = vlog_segment_file_names(wal_path).context(WALSnafu)?;
        let mut names = vec![];
        for (_, mut fs) in files {
            names.append(&mut fs);
        }
        let limit = cache.max_size();

        // Disable the max size during loading
        cache.set_max_size(0);
        let _restore_limit = scopeguard::guard((), |_| cache.set_max_size(limit));

        let loader = CacheLoader::new(names);
        if let Err(e) = loader.load(cache) {
            return Err(Error::Cache { source: e });
        }

        Ok(())
    }

    /// Reads the Index WAL segment files and loads them into the cache.
    fn reload_index_cache(cache: &IndexCache, index_wal_path: &Path) -> Result<()> {
        let files = segment_file_names(index_wal_path).context(WALSnafu)?;
        let limit = cache.max_size();

        // Disable the max size during loading
        cache.set_max_size(0);
        let _restore_limit = scopeguard::guard((), |_| cache.set_max_size(limit));
        let loader = IndexCacheLoader::new(files);
        if let Err(e) = loader.load(cache) {
            return Err(Error::IndexCache { source: e });
        }

        Ok(())
    }

    /// Removes all temp files and dirs that exist on disk.  This is should only be run at startup to avoid
    /// removing tmp files that are still in use.
    fn cleanup(&self) -> Result<()> {
        if !Path::exists(&self.path) {
            return Ok(());
        }

        let all_files = std::fs::read_dir(self.path.as_path())?;

        for entry in all_files {
            let entry = entry?;
            // Check to see if there are any `.tmp` directories that were left over from failed shard snapshots
            let path = entry.path();
            if path.is_dir() {
                if let Some(ext) = path.extension() {
                    if ext == TMP_TSM_FILE_EXTENSION {
                        std::fs::remove_dir_all(self.path.join(path.file_name().unwrap()))?;
                    }
                }
            }
        }

        self.cleanup_temp_tsm_files()
    }

    fn cleanup_temp_tsm_files(&self) -> Result<()> {
        let _ = fs::read_dir(self.path.as_path())
            .context(IOSnafu {
                reason: "error getting compaction temp files".to_string(),
            })?
            .map(|entry| -> Result<()> {
                let path = entry.unwrap().path();
                match path.extension() {
                    Some(ext) => {
                        if ext == COMPACTION_TEMP_EXTENSION {
                            std::fs::remove_file(&path).context(IOSnafu {
                                reason: "error removing temp compaction files".to_string(),
                            })?;
                        }
                        Ok(())
                    }
                    _ => Ok(()),
                }
            });

        Ok(())
    }

    /// Returns a KeyCursor for the given key starting at time t.
    fn key_cursor(
        &self,
        key: &[u8],
        t: i64,
        ascending: bool,
    ) -> (KeyCursor, Option<VLogKeyCursor>) {
        let inner = self.inner.read();
        let c = FileStore::key_cursor(Arc::clone(&inner.file_store), key, t, ascending);
        let v = if self.vlog_enabled {
            let vlog_inner = inner.vlog_inner.as_ref().unwrap().read();

            let vlog_indexes = vlog_inner.vlog_index_cache.values_range(key, t, t);
            Some(VLogFileStore::vlog_key_cursor(
                Arc::clone(&vlog_inner.vlog_file_store),
                vlog_indexes,
                key,
                t,
                ascending,
            ))
        } else {
            None
        };

        (c, v)
    }

    // // //////////////////////////////////////////////////////////
    // // ///////////////////////// Iterator ///////////////////////
    // // //////////////////////////////////////////////////////////

    // agilor-cli query -tXXX -dmydb --format=column
    // "SELECT usage FROM cpu_usage WHERE "region"='beijing' AND "host"='server1' AND time >= '2021-10-01T00:00:00.00000000Z'"

    pub(crate) fn create_direct_tsm_scans(
        &self,
        scan: &StorageScan,
    ) -> Result<Option<DirectTsmScanSet>> {
        self.create_direct_tsm_scans_with_row_domain_mode(scan, DirectTsmScanMode::TimestampOnly)
    }

    #[cfg(test)]
    pub(crate) fn create_direct_tsm_scans_with_row_domain_values(
        &self,
        scan: &StorageScan,
    ) -> Result<Option<DirectTsmScanSet>> {
        self.create_direct_tsm_scans_with_row_domain_mode(scan, DirectTsmScanMode::Values)
    }

    fn create_direct_tsm_scans_with_row_domain_mode(
        &self,
        scan: &StorageScan,
        row_domain_mode: DirectTsmScanMode,
    ) -> Result<Option<DirectTsmScanSet>> {
        scan.validate().map_err(|error| Error::StorageProjection {
            reason: error.to_string(),
        })?;
        if self.vlog_enabled
            || scan.projection.primary_column.is_some()
            || scan.time_order != TimeOrder::Ascending
            || scan.execution.window.is_some()
            || scan.execution.point_limit != Default::default()
            || scan.execution.series_limit != Default::default()
            || scan.execution.max_series_n != 0
        {
            return Ok(None);
        }
        if scan
            .cancellation
            .as_ref()
            .is_some_and(crate::query::storage_ir::ScanCancellation::is_cancelled)
        {
            return Err(Error::ScanCancelled);
        }

        let columns = scan
            .projection
            .returned_columns
            .iter()
            .map(|column| (column, DirectTsmScanMode::Values))
            .chain(
                scan.projection
                    .row_domain_columns
                    .iter()
                    .map(|column| (column, row_domain_mode)),
            )
            .collect::<Vec<_>>();
        if columns.is_empty() {
            return Ok(None);
        }
        let mut typed_columns = Vec::with_capacity(columns.len());
        let mut names = HashSet::with_capacity(columns.len());
        for (column, mode) in columns {
            let ColumnKind::Field(Some(field_type)) = column.kind else {
                return Ok(None);
            };
            if !matches!(field_type, INTEGER | UNSIGNED | FLOAT | BOOL | STRING)
                || !names.insert(column.name.as_str())
            {
                return Ok(None);
            }
            typed_columns.push((column, field_type, mode));
        }

        let empty_scan_set = || {
            #[cfg(feature = "slow-trace")]
            if let Some(profile) = &scan.execution.profile {
                profile.record_direct_scan_layout(
                    0,
                    scan.projection.returned_columns.len() as u64,
                    scan.projection.row_domain_columns.len() as u64,
                );
                for _ in &typed_columns {
                    profile.record_cursor(0, 0);
                }
            }
            DirectTsmScanSet {
                tag_sets: Vec::new(),
                executor: Arc::clone(&self.executor),
            }
        };
        if !self
            .index
            .measurement_exists(scan.measurement.name.as_bytes())
            .context(IndexSnafu)?
        {
            return Ok(Some(empty_scan_set()));
        }

        let index_set =
            index::IndexSet::new(Arc::clone(&self.index), Arc::clone(&self.sfile), None);
        let tag_sets = index_set
            .tag_sets_for_storage_scan(Arc::clone(&self.sfile), scan, 0)
            .map_err(|source| match source {
                index::Error::StorageScanCancelled => Error::ScanCancelled,
                source => Error::IndexSet { source },
            })?;
        let physical_series_count = tag_sets
            .iter()
            .map(|tag_set| tag_set.series_keys.len())
            .sum::<usize>();
        if physical_series_count == 0 {
            return Ok(Some(empty_scan_set()));
        }
        if tag_sets
            .iter()
            .any(|tag_set| tag_set.storage_filters.iter().any(Option::is_some))
        {
            return Ok(None);
        }

        let (cache, file_store) = {
            let inner = self.inner.read();
            (Arc::clone(&inner.cache), Arc::clone(&inner.file_store))
        };
        let readers = Arc::new(AggregateReaderSet::new(file_store.readers()));
        let fields = self.fieldset.fields_by_string(&scan.measurement.name);
        let mut direct_tag_sets = Vec::with_capacity(tag_sets.len());

        for tag_set in tag_sets {
            let mut series_scans = Vec::with_capacity(tag_set.series_keys.len());
            for series_key in tag_set.series_keys {
                let (_, model_tags) = crate::models::points::parse_key(series_key.as_bytes());
                let tags = Arc::new(
                    query_point::Tags::new(model_tags.map())
                        .subset(&scan.projection.series.output_tag_keys),
                );
                let output_identity = tags.id();
                // The generic sorted merge compares the point's already-projected
                // tags against the identity dimensions. Preserve that exact key;
                // an identity tag absent from output tags is absent there too.
                let merge_identity = tags.subset(&scan.projection.series.identity_tag_keys).id();
                let mut scans = Vec::with_capacity(typed_columns.len());

                for (column, field_type, mode) in &typed_columns {
                    let Some(field) = fields
                        .as_ref()
                        .and_then(|fields| fields.field(&column.name))
                    else {
                        scans.push(DirectTsmScan::empty(
                            *field_type,
                            *mode,
                            scan.time_range,
                            Arc::clone(&tags),
                            scan.cancellation.clone(),
                            Arc::clone(&readers),
                            #[cfg(feature = "slow-trace")]
                            scan.execution.profile.clone(),
                        ));
                        continue;
                    };
                    if field.typ != *field_type {
                        return Ok(None);
                    }

                    let key = format!("{series_key}{KEY_FIELD_SEPARATOR}{}", column.name);
                    if cache
                        .values(key.as_bytes())
                        .iter()
                        .any(|value| scan.time_range.contains(value.unix_nano()))
                    {
                        return Ok(None);
                    }

                    let expected_block_type = match *field_type {
                        INTEGER => BLOCK_INTEGER,
                        UNSIGNED => BLOCK_UNSIGNED,
                        FLOAT => BLOCK_FLOAT64,
                        BOOL => BLOCK_BOOL,
                        STRING => BLOCK_STRING,
                        _ => unreachable!(),
                    };
                    let mut candidates = Vec::new();
                    #[cfg(feature = "slow-trace")]
                    let lookup_started = Instant::now();
                    for reader in readers.as_slice() {
                        if reader.tombstone_range(key.as_bytes()).iter().any(|range| {
                            range.max >= scan.time_range.start_time
                                && range.min <= scan.time_range.end_time
                        }) {
                            return Ok(None);
                        }
                        let entries = reader.entries(key.as_bytes());
                        if entries.is_empty() {
                            continue;
                        }
                        if reader.values_type(key.as_bytes()).context(TSMReaderSnafu)?
                            != expected_block_type
                        {
                            return Err(Error::StringErr {
                                reason: format!(
                                    "TSM block type does not match field type {field_type} for key {key}"
                                ),
                            });
                        }
                        for entry in entries {
                            if entry.max_time < scan.time_range.start_time
                                || entry.min_time > scan.time_range.end_time
                            {
                                continue;
                            }
                            candidates.push(DirectTsmCandidate {
                                reader: Arc::clone(reader),
                                entry,
                            });
                        }
                    }
                    #[cfg(feature = "slow-trace")]
                    if let Some(profile) = &scan.execution.profile {
                        profile.record_block_lookup(
                            lookup_started
                                .elapsed()
                                .as_micros()
                                .min(u128::from(u64::MAX)) as u64,
                        );
                    }

                    let overlap_candidates = candidates
                        .iter()
                        .map(|candidate| AggregateCandidate {
                            reader: Arc::clone(&candidate.reader),
                            entry: candidate.entry.clone(),
                        })
                        .collect::<Vec<_>>();
                    if !aggregate_conflict_intervals(&overlap_candidates, scan.time_range)
                        .is_empty()
                    {
                        return Ok(None);
                    }
                    candidates.sort_unstable_by_key(|candidate| {
                        (
                            candidate.entry.min_time,
                            candidate.entry.max_time,
                            candidate.reader.generation,
                            candidate.reader.seq,
                        )
                    });
                    #[cfg(feature = "slow-trace")]
                    if let Some(profile) = &scan.execution.profile {
                        let files = candidates
                            .iter()
                            .map(|candidate| Arc::as_ptr(&candidate.reader) as usize)
                            .collect::<HashSet<_>>()
                            .len() as u64;
                        profile.record_cursor(files, candidates.len() as u64);
                    }
                    scans.push(DirectTsmScan {
                        candidates,
                        next_candidate: 0,
                        field_type: *field_type,
                        mode: *mode,
                        time_range: scan.time_range,
                        tags: Arc::clone(&tags),
                        cancellation: scan.cancellation.clone(),
                        _readers: Arc::clone(&readers),
                        #[cfg(feature = "slow-trace")]
                        profile: scan.execution.profile.clone(),
                    });
                }
                series_scans.push(DirectTsmSeriesScan {
                    scans,
                    merge_identity,
                    output_identity,
                });
            }
            direct_tag_sets.push(DirectTsmTagSetScan {
                series: series_scans,
            });
        }

        #[cfg(feature = "slow-trace")]
        if let Some(profile) = &scan.execution.profile {
            profile.record_direct_scan_layout(
                physical_series_count as u64,
                scan.projection.returned_columns.len() as u64,
                scan.projection.row_domain_columns.len() as u64,
            );
        }

        Ok(Some(DirectTsmScanSet {
            tag_sets: direct_tag_sets,
            executor: Arc::clone(&self.executor),
        }))
    }

    pub(crate) fn create_storage_iterator(
        &self,
        scan: StorageScan,
    ) -> Result<Option<Box<dyn QueryIterator + Sync + Send>>> {
        scan.validate().map_err(|error| Error::StorageProjection {
            reason: error.to_string(),
        })?;
        let plan = Arc::new(EngineScanPlan {
            scan: Arc::new(scan),
            vlog_enabled: self.vlog_enabled,
        });

        self.create_iterator_from_scan(plan)
    }

    pub(crate) fn row_count(&self, request: RowCountRequest) -> Result<Option<RowCountResult>> {
        if request.time_range.start_time > request.time_range.end_time {
            return Err(Error::StringErr {
                reason: "row count request requires a valid time range".to_string(),
            });
        }

        let Some(measurement_fields) = self.fieldset.fields_by_string(&request.measurement.name)
        else {
            return Ok(None);
        };
        let mut fields = Vec::new();
        for name in measurement_fields.field_keys() {
            let field = measurement_fields
                .field(&name)
                .expect("field key snapshot must resolve");
            if !matches!(field.typ, INTEGER | UNSIGNED | FLOAT | BOOL | STRING) {
                return Err(Error::StringErr {
                    reason: format!("unsupported row-domain field type {}", field.typ),
                });
            }
            fields.push((name, field.typ));
        }
        if fields.is_empty() {
            return Ok(Some(RowCountResult::default()));
        }

        let scan = self.row_count_storage_scan(&request, &fields)?;
        let plan = Arc::new(EngineScanPlan {
            scan: Arc::new(scan.clone()),
            vlog_enabled: self.vlog_enabled,
        });
        let index_set =
            index::IndexSet::new(Arc::clone(&self.index), Arc::clone(&self.sfile), None);
        let tag_sets = index_set
            .tag_sets_for_storage_scan(Arc::clone(&self.sfile), &scan, plan.max_series_n())
            .map_err(|source| match source {
                index::Error::StorageScanCancelled => Error::ScanCancelled,
                source => Error::IndexSet { source },
            })?;

        let (cache, file_store) = {
            let inner = self.inner.read();
            (Arc::clone(&inner.cache), Arc::clone(&inner.file_store))
        };
        let readers = Arc::new(AggregateReaderSet::new(file_store.readers()));
        let mut metrics = RowCountMetrics::default();
        let mut count = 0_u64;

        if !self.vlog_enabled {
            let mut direct = true;
            'series: for tag_set in &tag_sets {
                for (series_key, residual) in
                    tag_set.series_keys.iter().zip(&tag_set.storage_filters)
                {
                    if residual.is_some() {
                        direct = false;
                        break 'series;
                    }
                    let Some(scans) = self.row_count_direct_scans_for_series(
                        series_key,
                        &fields,
                        &request,
                        &cache,
                        Arc::clone(&readers),
                        #[cfg(feature = "slow-trace")]
                        None,
                        #[cfg(feature = "slow-trace")]
                        None,
                    )?
                    else {
                        direct = false;
                        break 'series;
                    };
                    let series_count = count_direct_timestamp_union(scans, &mut metrics)?;
                    count = count
                        .checked_add(series_count)
                        .ok_or_else(|| Error::StringErr {
                            reason: "row count overflow".to_string(),
                        })?;
                    metrics.series_scanned =
                        metrics
                            .series_scanned
                            .checked_add(1)
                            .ok_or_else(|| Error::StringErr {
                                reason: "row count series metric overflow".to_string(),
                            })?;
                }
            }
            if direct {
                return Ok(Some(RowCountResult { count, metrics }));
            }
        }

        metrics.used_fallback = true;
        metrics.raw_scans = 1;
        count = match self.create_storage_iterator(scan)? {
            Some(iterator) => {
                query_iterator::count_iterator_rows(iterator).context(QueryIteratorSnafu)?
            }
            None => 0,
        };
        metrics.fallback_rows = count;
        Ok(Some(RowCountResult { count, metrics }))
    }

    #[cfg(test)]
    pub(crate) fn delete_field_range_for_test(
        &self,
        key: Vec<u8>,
        min: i64,
        max: i64,
    ) -> Result<()> {
        let file_store = {
            let inner = self.inner.read();
            Arc::clone(&inner.file_store)
        };
        file_store
            .delete_range(vec![key], min, max)
            .context(FileStoreSnafu)
    }

    fn row_count_direct_scans_for_series(
        &self,
        series_key: &str,
        fields: &[(String, FieldType)],
        request: &RowCountRequest,
        cache: &Arc<Cache>,
        readers: Arc<AggregateReaderSet>,
        #[cfg(feature = "slow-trace")] scan_profile: Option<
            &Arc<crate::query::storage_ir::StorageScanProfile>,
        >,
        #[cfg(feature = "slow-trace")] mut grouped_profile: Option<
            &mut GroupedAggregateProfileMetrics,
        >,
    ) -> Result<Option<Vec<DirectTsmScan>>> {
        let tags = Arc::new(query_point::Tags::default());
        let mut scans = Vec::with_capacity(fields.len());
        for (field_name, field_type) in fields {
            let key = format!("{series_key}{KEY_FIELD_SEPARATOR}{field_name}");
            if cache
                .values(key.as_bytes())
                .iter()
                .any(|value| request.time_range.contains(value.unix_nano()))
            {
                #[cfg(feature = "slow-trace")]
                if let Some(profile) = grouped_profile.as_deref_mut() {
                    profile.row_domain_direct_reject_cache =
                        profile.row_domain_direct_reject_cache.saturating_add(1);
                }
                return Ok(None);
            }

            let expected_block_type = match *field_type {
                INTEGER => BLOCK_INTEGER,
                UNSIGNED => BLOCK_UNSIGNED,
                FLOAT => BLOCK_FLOAT64,
                BOOL => BLOCK_BOOL,
                STRING => BLOCK_STRING,
                _ => unreachable!("validated row-domain field type"),
            };
            let mut candidates = Vec::new();
            for reader in readers.as_slice() {
                #[cfg(feature = "slow-trace")]
                if let Some(profile) = scan_profile {
                    profile.record_file_index_searches(1);
                }
                if reader.tombstone_range(key.as_bytes()).iter().any(|range| {
                    range.max >= request.time_range.start_time
                        && range.min <= request.time_range.end_time
                }) {
                    #[cfg(feature = "slow-trace")]
                    if let Some(profile) = grouped_profile.as_deref_mut() {
                        profile.row_domain_direct_reject_tombstone =
                            profile.row_domain_direct_reject_tombstone.saturating_add(1);
                    }
                    return Ok(None);
                }
                let entries = reader.entries(key.as_bytes());
                if entries.is_empty() {
                    continue;
                }
                if reader.values_type(key.as_bytes()).context(TSMReaderSnafu)?
                    != expected_block_type
                {
                    return Err(Error::StringErr {
                        reason: format!(
                            "TSM block type does not match field type {field_type} for key {key}"
                        ),
                    });
                }
                for entry in entries {
                    if entry.max_time < request.time_range.start_time
                        || entry.min_time > request.time_range.end_time
                    {
                        continue;
                    }
                    candidates.push(DirectTsmCandidate {
                        reader: Arc::clone(reader),
                        entry,
                    });
                }
            }
            candidates.sort_unstable_by_key(|candidate| {
                (
                    candidate.entry.min_time,
                    candidate.entry.max_time,
                    candidate.reader.generation,
                    candidate.reader.seq,
                )
            });
            if direct_tsm_candidates_overlap(&candidates) {
                #[cfg(feature = "slow-trace")]
                if let Some(profile) = grouped_profile.as_deref_mut() {
                    profile.row_domain_direct_reject_overlap =
                        profile.row_domain_direct_reject_overlap.saturating_add(1);
                }
                return Ok(None);
            }
            #[cfg(feature = "slow-trace")]
            if let Some(profile) = scan_profile {
                let files = candidates
                    .iter()
                    .map(|candidate| Arc::as_ptr(&candidate.reader) as usize)
                    .collect::<HashSet<_>>()
                    .len() as u64;
                profile.record_cursor(files, candidates.len() as u64);
                profile.record_cursor_seek();
            }
            scans.push(DirectTsmScan {
                candidates,
                next_candidate: 0,
                field_type: *field_type,
                mode: DirectTsmScanMode::TimestampOnly,
                time_range: request.time_range,
                tags: Arc::clone(&tags),
                cancellation: request.cancellation.clone(),
                _readers: Arc::clone(&readers),
                #[cfg(feature = "slow-trace")]
                profile: scan_profile.cloned(),
            });
        }
        Ok(Some(scans))
    }

    fn row_count_storage_scan(
        &self,
        request: &RowCountRequest,
        fields: &[(String, FieldType)],
    ) -> Result<StorageScan> {
        let mut identity_tag_keys = self
            .measurement_tag_keys(request.measurement.name.as_bytes())?
            .into_iter()
            .collect::<Vec<_>>();
        identity_tag_keys.sort();
        Ok(StorageScan {
            measurement: request.measurement.clone(),
            projection: StorageProjection {
                primary_column: None,
                returned_columns: vec![],
                row_domain_columns: fields
                    .iter()
                    .map(|(name, field_type)| {
                        ColumnRef::new(name, ColumnKind::Field(Some(*field_type)))
                    })
                    .collect(),
                series: SeriesProjection::new(identity_tag_keys, vec![]),
            },
            time_range: request.time_range,
            predicate: None,
            time_order: TimeOrder::Ascending,
            output_order: OutputOrder::TimeOrdered,
            execution: StorageExecutionOptions::default(),
            cancellation: request.cancellation.clone(),
        })
    }

    pub(crate) fn grouped_aggregate_scan(
        &self,
        request: GroupedAggregateRequest,
    ) -> Result<Option<GroupedAggregateResult>> {
        futures::executor::block_on(self.grouped_aggregate_scan_async(request))
    }

    pub(crate) async fn grouped_aggregate_scan_async(
        &self,
        request: GroupedAggregateRequest,
    ) -> Result<Option<GroupedAggregateResult>> {
        self.grouped_aggregate_scan_mode(request, true).await
    }

    pub(crate) fn grouped_aggregate_scan_target_only(
        &self,
        request: GroupedAggregateRequest,
    ) -> Result<Option<GroupedAggregateResult>> {
        if request.targets.len() != 1 {
            return Err(Error::StringErr {
                reason: "target-only grouped aggregation requires exactly one field".into(),
            });
        }
        futures::executor::block_on(self.grouped_aggregate_scan_mode(request, false))
    }

    async fn grouped_aggregate_scan_mode(
        &self,
        request: GroupedAggregateRequest,
        include_row_domain: bool,
    ) -> Result<Option<GroupedAggregateResult>> {
        #[cfg(feature = "slow-trace")]
        let diagnostics = request
            .cancellation
            .as_ref()
            .and_then(ScanCancellation::grouped_diagnostics)
            .cloned();
        #[cfg(feature = "slow-trace")]
        if let Some(diagnostics) = &diagnostics {
            diagnostics.record_storage_coordinator_started(Instant::now());
        }
        let result = self
            .grouped_aggregate_scan_with_row_domain(request, include_row_domain)
            .await;
        #[cfg(feature = "slow-trace")]
        if let Some(diagnostics) = &diagnostics {
            diagnostics.record_storage_result_ready(Instant::now());
        }
        result
    }

    async fn grouped_aggregate_scan_with_row_domain(
        &self,
        request: GroupedAggregateRequest,
        include_row_domain: bool,
    ) -> Result<Option<GroupedAggregateResult>> {
        let started = Instant::now();
        if request.targets.is_empty()
            || request.time_range.start_time > request.time_range.end_time
            || request.window.duration_ns <= 0
        {
            return Err(Error::StringErr {
                reason: "grouped aggregate request requires targets, a positive fixed window, and a valid time range"
                    .to_string(),
            });
        }
        if request.targets.iter().any(|target| {
            target.field.is_empty()
                || target.aggregates.is_empty()
                || target.aggregates.windows(2).any(|pair| pair[0] >= pair[1])
                || target.aggregates.iter().any(|kind| {
                    !matches!(
                        kind,
                        AggregateKind::Count
                            | AggregateKind::Sum
                            | AggregateKind::Min
                            | AggregateKind::Max
                            | AggregateKind::Avg
                    )
                })
        }) || request
            .targets
            .windows(2)
            .any(|pair| pair[0].field >= pair[1].field)
        {
            return Err(Error::StringErr {
                reason: "grouped aggregate targets must be sorted by unique non-empty field name and contain a non-empty sorted unique supported aggregate set"
                    .to_string(),
            });
        }
        if request.group_tags.iter().any(String::is_empty)
            || request.group_tags.iter().collect::<HashSet<_>>().len() != request.group_tags.len()
        {
            return Err(Error::StringErr {
                reason: "grouped aggregate group tags must be non-empty and unique".to_string(),
            });
        }
        if request.tag_filters.iter().any(|filter| {
            filter.tag.is_empty()
                || filter.accepted_values.is_empty()
                || filter
                    .accepted_values
                    .windows(2)
                    .any(|pair| pair[0] >= pair[1])
        }) || request
            .tag_filters
            .windows(2)
            .any(|pair| pair[0].tag >= pair[1].tag)
        {
            return Err(Error::StringErr {
                reason: "grouped aggregate tag filters must be sorted, unique, and contain a non-empty sorted unique value set"
                    .to_string(),
            });
        }

        let Some(fields) = self.fieldset.fields_by_string(&request.measurement.name) else {
            return Ok(None);
        };
        let row_domain_fields = fields
            .field_keys()
            .into_iter()
            .map(|name| {
                let field = fields
                    .field(&name)
                    .expect("field key snapshot must resolve");
                (name, field.typ)
            })
            .collect::<Vec<_>>();
        for target in &request.targets {
            if !matches!(
                target.expected_field_type,
                crate::models::INTEGER
                    | crate::models::UNSIGNED
                    | crate::models::FLOAT
                    | crate::models::BOOL
                    | crate::models::STRING
            ) {
                return Err(Error::StringErr {
                    reason: format!(
                        "unsupported grouped aggregate field type {}",
                        target.expected_field_type
                    ),
                });
            }
            if matches!(
                target.expected_field_type,
                crate::models::BOOL | crate::models::STRING
            ) && target
                .aggregates
                .iter()
                .any(|aggregate| *aggregate != AggregateKind::Count)
            {
                return Err(Error::StringErr {
                    reason:
                        "only COUNT is supported for grouped boolean and string aggregate fields"
                            .to_string(),
                });
            }
            if let Some(field) = fields.field(&target.field)
                && field.typ != target.expected_field_type
            {
                return Err(Error::StringErr {
                    reason: format!(
                        "grouped aggregate field {} has local type {} but request expects {}",
                        target.field, field.typ, target.expected_field_type
                    ),
                });
            }
        }
        if request.targets.len() > 1
            && request.targets.iter().any(|target| {
                target.expected_field_type == crate::models::FLOAT
                    && target
                        .aggregates
                        .iter()
                        .any(|kind| matches!(kind, AggregateKind::Min | AggregateKind::Max))
            })
        {
            return self
                .grouped_multi_target_float_min_max_states(&request, &row_domain_fields)
                .await;
        }
        if request.targets.len() > 1 {
            return self.grouped_multi_field_states(&request, &row_domain_fields, started);
        }

        let target = &request.targets[0];
        let requires_float_min_max_proof = target.expected_field_type == crate::models::FLOAT
            && target
                .aggregates
                .iter()
                .any(|kind| matches!(kind, AggregateKind::Min | AggregateKind::Max));
        let Some(field) = fields.field(&target.field) else {
            return if include_row_domain {
                self.grouped_missing_field_states(
                    &request,
                    &row_domain_fields,
                    target.expected_field_type,
                    started,
                )
            } else {
                Ok(None)
            };
        };
        let row_domain_coverage = include_row_domain.then(|| {
            let covered_target_fields = HashSet::from([target.field.clone()]);
            GroupedRowDomainCoverage::new(&row_domain_fields, &covered_target_fields)
        });

        #[cfg(feature = "slow-trace")]
        let measurement_tag_keys_diagnostics = Arc::new(index::MeasurementTagKeysDiagnostics::new(
            request
                .cancellation
                .as_ref()
                .and_then(ScanCancellation::grouped_diagnostics)
                .cloned(),
        ));
        #[cfg(feature = "slow-trace")]
        let scan = self.grouped_aggregate_storage_scan_profiled(
            &request,
            target,
            field.typ,
            &measurement_tag_keys_diagnostics,
        )?;
        #[cfg(not(feature = "slow-trace"))]
        let scan = self.grouped_aggregate_storage_scan(&request, target, field.typ)?;
        #[cfg(feature = "slow-trace")]
        let measurement_tag_keys_diagnostics = measurement_tag_keys_diagnostics.snapshot();
        let plan = Arc::new(EngineScanPlan {
            scan: Arc::new(scan.clone()),
            vlog_enabled: self.vlog_enabled,
        });
        let index_set =
            index::IndexSet::new(Arc::clone(&self.index), Arc::clone(&self.sfile), None);
        #[cfg(feature = "slow-trace")]
        let metadata_setup_wall_us = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        #[cfg(feature = "slow-trace")]
        let tsi_diagnostics = index::TsiSelectionDiagnostics::default();
        #[cfg(feature = "slow-trace")]
        let tsi_started = Instant::now();
        #[cfg(feature = "slow-trace")]
        let tag_sets_result = index_set.tag_sets_for_storage_scan_profiled(
            Arc::clone(&self.sfile),
            &scan,
            plan.max_series_n(),
            &tsi_diagnostics,
        );
        #[cfg(not(feature = "slow-trace"))]
        let tag_sets_result = index_set.tag_sets_for_storage_scan(
            Arc::clone(&self.sfile),
            &scan,
            plan.max_series_n(),
        );
        let tag_sets = tag_sets_result.map_err(|source| match source {
            index::Error::StorageScanCancelled => Error::ScanCancelled,
            source => Error::IndexSet { source },
        })?;
        #[cfg(feature = "slow-trace")]
        let tsi_series_selection_wall_us =
            tsi_started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;

        #[cfg(feature = "slow-trace")]
        let reader_snapshot_started = Instant::now();
        let (cache, file_store) = {
            let inner = self.inner.read();
            (Arc::clone(&inner.cache), Arc::clone(&inner.file_store))
        };
        let reader_list = file_store.readers();
        #[cfg(feature = "slow-trace")]
        let reader_snapshot_tsm_readers = reader_list.len() as u64;
        let readers = Arc::new(AggregateReaderSet::new(reader_list));
        #[cfg(feature = "slow-trace")]
        let reader_snapshot_wall_us = reader_snapshot_started
            .elapsed()
            .as_micros()
            .min(u128::from(u64::MAX)) as u64;
        let mut metrics = GroupedAggregateMetrics {
            fields_projected: 1,
            ..Default::default()
        };
        #[cfg(feature = "slow-trace")]
        {
            let tsi_diagnostics = tsi_diagnostics.snapshot();
            metrics.profile.shard_scans = 1;
            metrics.profile.metadata_setup_wall_us = metadata_setup_wall_us;
            metrics.profile.metadata_measurement_tag_keys_us =
                measurement_tag_keys_diagnostics.wall_us;
            metrics.profile.metadata_tag_key_partition_count =
                measurement_tag_keys_diagnostics.partition_count;
            metrics.profile.metadata_tag_key_tasks_submitted =
                measurement_tag_keys_diagnostics.tasks_submitted;
            metrics.profile.metadata_tag_key_tasks_started =
                measurement_tag_keys_diagnostics.tasks_started;
            metrics.profile.metadata_tag_key_tasks_completed =
                measurement_tag_keys_diagnostics.tasks_completed;
            metrics.profile.metadata_tag_key_queue_wait_us_total =
                measurement_tag_keys_diagnostics.queue_wait_us_total;
            metrics.profile.metadata_tag_key_queue_wait_us_max =
                measurement_tag_keys_diagnostics.queue_wait_us_max;
            metrics.profile.metadata_tag_key_execution_us_total =
                measurement_tag_keys_diagnostics.execution_us_total;
            metrics.profile.metadata_tag_key_execution_us_max =
                measurement_tag_keys_diagnostics.execution_us_max;
            metrics.profile.metadata_tag_key_caller_wait_all_us =
                measurement_tag_keys_diagnostics.caller_wait_all_us;
            metrics.profile.tsi_series_selection_wall_us = tsi_series_selection_wall_us;
            metrics.profile.reader_snapshot_wall_us = reader_snapshot_wall_us;
            metrics.profile.tsi_predicate_selection_us = tsi_diagnostics.predicate_selection_us;
            metrics.profile.tsi_series_key_materialization_us =
                tsi_diagnostics.series_key_materialization_us;
            metrics.profile.tsi_tag_value_cache_lock_acquisitions =
                tsi_diagnostics.tag_value_cache_lock_acquisitions;
            metrics.profile.tsi_tag_value_cache_lock_wait_us =
                tsi_diagnostics.tag_value_cache_lock_wait_us;
            metrics.profile.tsi_tag_value_cache_lock_hold_us =
                tsi_diagnostics.tag_value_cache_lock_hold_us;
            metrics.profile.metadata_field_count = row_domain_fields.len() as u64;
            metrics.profile.metadata_identity_tag_key_count =
                scan.projection.series.identity_tag_keys.len() as u64;
            metrics.profile.tag_predicates_evaluated = request.tag_filters.len() as u64;
            metrics.profile.tag_values_requested = request
                .tag_filters
                .iter()
                .map(|filter| filter.accepted_values.len() as u64)
                .sum();
            metrics.profile.tsi_selected_tag_sets = tag_sets.len() as u64;
            metrics.profile.tsi_selected_series_keys = tag_sets
                .iter()
                .map(|tag_set| tag_set.series_keys.len() as u64)
                .sum();
            metrics.profile.reader_snapshot_tsm_readers = reader_snapshot_tsm_readers;
        }
        let mut groups = BTreeMap::<GroupedAggregateKey, AggregateState>::new();
        let mut whole_request_fallback = self.vlog_enabled;
        let mut has_residual_filter = false;
        #[cfg(feature = "slow-trace")]
        let mut has_cache_values = false;
        #[cfg(feature = "slow-trace")]
        let mut has_tombstones = false;
        let mut series_jobs = Vec::new();
        let mut candidate_count = 0usize;
        let mut conflict_intervals = Vec::new();
        let mut matched_series_keys = Vec::new();
        #[cfg(feature = "slow-trace")]
        let candidate_discovery_started = Instant::now();

        for tag_set in &tag_sets {
            for (series_key, residual) in tag_set.series_keys.iter().zip(&tag_set.storage_filters) {
                metrics.series_matched = metrics.series_matched.saturating_add(1);
                matched_series_keys.push(series_key.clone());
                if residual.is_some() {
                    whole_request_fallback = true;
                    has_residual_filter = true;
                }
                let tag_values = Self::grouped_tag_values(series_key, &request.group_tags)?;
                let key = format!("{series_key}{KEY_FIELD_SEPARATOR}{}", target.field);
                #[cfg(feature = "slow-trace")]
                let interval_scope = Arc::<str>::from(key.as_str());
                if cache
                    .values(key.as_bytes())
                    .iter()
                    .any(|value| request.time_range.contains(value.unix_nano()))
                {
                    whole_request_fallback = true;
                    #[cfg(feature = "slow-trace")]
                    {
                        has_cache_values = true;
                    }
                }

                let mut series_candidates = Vec::new();
                for reader in readers.as_slice() {
                    let tombstones = reader.tombstone_range(key.as_bytes());
                    let entries = reader.entries(key.as_bytes());
                    #[cfg(feature = "slow-trace")]
                    {
                        metrics.profile.candidate_file_index_searches = metrics
                            .profile
                            .candidate_file_index_searches
                            .saturating_add(1);
                        metrics.profile.candidate_index_entries_inspected = metrics
                            .profile
                            .candidate_index_entries_inspected
                            .saturating_add(entries.len() as u64);
                        if !entries.is_empty() {
                            metrics.profile.candidate_files_with_entries = metrics
                                .profile
                                .candidate_files_with_entries
                                .saturating_add(1);
                        }
                    }
                    for entry in entries {
                        if entry.max_time < request.time_range.start_time
                            || entry.min_time > request.time_range.end_time
                        {
                            continue;
                        }
                        if tombstones
                            .iter()
                            .any(|range| range.max >= entry.min_time && range.min <= entry.max_time)
                        {
                            whole_request_fallback = true;
                            #[cfg(feature = "slow-trace")]
                            {
                                has_tombstones = true;
                            }
                        }
                        series_candidates.push(AggregateCandidate {
                            reader: Arc::clone(reader),
                            entry,
                        });
                    }
                }
                conflict_intervals.extend(aggregate_conflict_intervals(
                    &series_candidates,
                    request.time_range,
                ));
                let candidates = series_candidates
                    .into_iter()
                    .map(|candidate| GroupedAggregateCandidate {
                        reader: candidate.reader,
                        entry: candidate.entry,
                        tag_values: tag_values.clone(),
                        #[cfg(feature = "slow-trace")]
                        interval_scope: Arc::clone(&interval_scope),
                    })
                    .collect::<Vec<_>>();
                candidate_count = candidate_count.saturating_add(candidates.len());
                if !candidates.is_empty() {
                    series_jobs.push(GroupedAggregateStorageKeyJob {
                        target_index: 0,
                        field_type: field.typ,
                        candidates,
                    });
                }
            }
        }
        #[cfg(feature = "slow-trace")]
        {
            metrics.profile.candidate_discovery_wall_us = candidate_discovery_started
                .elapsed()
                .as_micros()
                .min(u128::from(u64::MAX))
                as u64;
        }
        metrics.blocks_candidate = candidate_count as u64;
        #[cfg(feature = "slow-trace")]
        let interval_construction_started = Instant::now();
        let conflict_intervals = merge_aggregate_intervals(conflict_intervals);
        #[cfg(feature = "slow-trace")]
        {
            metrics.profile.raw_interval_construction_wall_us = interval_construction_started
                .elapsed()
                .as_micros()
                .min(u128::from(u64::MAX))
                as u64;
        }

        #[cfg(feature = "slow-trace")]
        let diagnostic_candidate_observation_started = Instant::now();
        #[cfg(feature = "slow-trace")]
        for candidate in series_jobs.iter().flat_map(|job| &job.candidates) {
            observe_grouped_candidate_eligibility(
                &mut metrics.profile,
                &candidate.entry,
                &request,
                field.typ,
                &conflict_intervals,
            )?;
        }
        #[cfg(feature = "slow-trace")]
        {
            metrics.profile.diagnostic_candidate_observation_wall_us =
                diagnostic_candidate_observation_started
                    .elapsed()
                    .as_micros()
                    .min(u128::from(u64::MAX)) as u64;
        }
        #[cfg(feature = "slow-trace")]
        let request_visibility_reject = if self.vlog_enabled {
            Some(GroupedBlockEligibility::Vlog)
        } else if has_residual_filter {
            Some(GroupedBlockEligibility::ResidualFilter)
        } else if has_cache_values {
            Some(GroupedBlockEligibility::Cache)
        } else if has_tombstones {
            Some(GroupedBlockEligibility::Tombstone)
        } else {
            None
        };
        #[cfg(feature = "slow-trace")]
        {
            if self.vlog_enabled {
                metrics.profile.observed_vlog_visibility = metrics.blocks_candidate;
            }
            if has_residual_filter {
                metrics.profile.observed_residual_filter = metrics.blocks_candidate;
            }
            if has_cache_values {
                metrics.profile.observed_cache_visibility = metrics.blocks_candidate;
            }
            if has_tombstones {
                metrics.profile.observed_tombstone_visibility = metrics.blocks_candidate;
            }
        }

        if requires_float_min_max_proof
            && (whole_request_fallback
                || series_jobs
                    .iter()
                    .flat_map(|job| &job.candidates)
                    .any(|candidate| {
                        candidate.entry.statistics.is_none_or(|statistics| {
                            !statistics.is_datafusion_grouped_float_min_max_safe()
                        })
                    }))
        {
            metrics.requires_datafusion_fallback = true;
            #[cfg(feature = "slow-trace")]
            {
                if let Some(reason) = request_visibility_reject {
                    metrics
                        .profile
                        .record_eligibility_n(reason, metrics.blocks_candidate);
                } else {
                    for candidate in series_jobs.iter().flat_map(|job| &job.candidates) {
                        metrics
                            .profile
                            .record_eligibility(grouped_float_candidate_reject_reason(
                                &candidate.entry,
                                field.typ,
                            ));
                    }
                }
                assert_grouped_eligibility_accounting(&metrics);
            }
            metrics.storage_scan_us =
                started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
            return Ok(Some(GroupedAggregateResult {
                groups: Vec::new(),
                metrics,
            }));
        }

        if whole_request_fallback {
            metrics.used_raw_fallback = true;
            metrics.blocks_raw_decoded = metrics.blocks_candidate;
            #[cfg(feature = "slow-trace")]
            {
                metrics.profile.record_eligibility_n(
                    request_visibility_reject.unwrap_or(GroupedBlockEligibility::Other),
                    metrics.blocks_candidate,
                );
                for series_key in &matched_series_keys {
                    metrics.profile.record_raw_interval(
                        format!("{series_key}{KEY_FIELD_SEPARATOR}{}", target.field),
                        request.time_range,
                        GroupedRawIntervalKind::WholeRequestFallback,
                    );
                }
                assert_grouped_eligibility_accounting(&metrics);
            }
            self.grouped_aggregate_raw_states(
                scan,
                field.typ,
                &request,
                &mut groups,
                &mut metrics,
                #[cfg(feature = "slow-trace")]
                GroupedPhysicalScanKind::Target,
            )?;
            if include_row_domain {
                self.ensure_grouped_row_domain(
                    &matched_series_keys,
                    row_domain_coverage
                        .as_ref()
                        .expect("row-domain coverage must exist when requested"),
                    field.typ,
                    &request,
                    &cache,
                    Arc::clone(&readers),
                    &mut groups,
                    &mut metrics,
                    !has_residual_filter,
                )?;
            }
            #[cfg(feature = "slow-trace")]
            assert_grouped_raw_execution_accounting(&metrics);
            metrics.storage_scan_us =
                started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
            return Ok(Some(GroupedAggregateResult {
                groups: groups
                    .into_iter()
                    .map(|(key, state)| GroupedAggregateGroup {
                        key,
                        states: vec![state],
                    })
                    .collect(),
                metrics,
            }));
        }

        let candidates = if conflict_intervals.is_empty() {
            let (mut parallel_groups, parallel_metrics) = self
                .execute_grouped_storage_key_partitions(series_jobs, 1, Arc::new(request.clone()))
                .await?;
            Self::merge_grouped_single_target_groups(
                &mut groups,
                parallel_groups.pop().unwrap_or_default(),
            )?;
            metrics.merge(parallel_metrics);
            Vec::new()
        } else {
            series_jobs
                .into_iter()
                .flat_map(|job| job.candidates)
                .collect::<Vec<_>>()
        };

        for candidate in candidates {
            #[cfg(feature = "slow-trace")]
            let eligibility_started = Instant::now();
            let entry_range = TimeRange::new(
                candidate.entry.min_time.max(request.time_range.start_time),
                candidate.entry.max_time.min(request.time_range.end_time),
            );
            let intersects_conflict =
                aggregate_intervals_intersect(&conflict_intervals, entry_range);
            let fully_covered = request.time_range.start_time <= candidate.entry.min_time
                && request.time_range.end_time >= candidate.entry.max_time;
            let first_bucket = request
                .window
                .bucket_start(candidate.entry.min_time)
                .map_err(|reason| Error::StringErr {
                    reason: reason.to_string(),
                })?;
            let one_bucket = request
                .window
                .bucket_start(candidate.entry.max_time)
                .map_err(|reason| Error::StringErr {
                    reason: reason.to_string(),
                })?
                == first_bucket;
            if fully_covered
                && one_bucket
                && !intersects_conflict
                && let Some(state) =
                    Self::aggregate_state_from_entry(field.typ, &candidate.entry, false)?
            {
                let key = GroupedAggregateKey {
                    window_start: first_bucket,
                    tag_values: candidate.tag_values,
                };
                match groups.entry(key) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(state);
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        entry
                            .get_mut()
                            .merge(state)
                            .map_err(|reason| Error::StringErr {
                                reason: reason.to_string(),
                            })?;
                    }
                }
                metrics.blocks_statistics_used = metrics.blocks_statistics_used.saturating_add(1);
                #[cfg(feature = "slow-trace")]
                {
                    metrics
                        .profile
                        .record_eligibility(GroupedBlockEligibility::StatisticsUsed);
                    metrics
                        .profile
                        .record_statistics_block(GroupedPhysicalBlockIdentity::new(
                            candidate.reader.path().to_string_lossy(),
                            candidate.entry.offset,
                            candidate.entry.min_time,
                            candidate.entry.max_time,
                        ));
                    metrics.profile.block_eligibility_wall_us =
                        metrics.profile.block_eligibility_wall_us.saturating_add(
                            eligibility_started
                                .elapsed()
                                .as_micros()
                                .min(u128::from(u64::MAX)) as u64,
                        );
                }
                continue;
            }

            #[cfg(feature = "slow-trace")]
            {
                let reason = if !fully_covered {
                    GroupedBlockEligibility::PartialQueryRange
                } else if !one_bucket {
                    GroupedBlockEligibility::CrossBucket
                } else if intersects_conflict {
                    GroupedBlockEligibility::Overlap
                } else if candidate.entry.statistics.is_none() {
                    GroupedBlockEligibility::MissingStatistics
                } else {
                    GroupedBlockEligibility::Other
                };
                metrics.profile.record_eligibility(reason);
                metrics.profile.block_eligibility_wall_us =
                    metrics.profile.block_eligibility_wall_us.saturating_add(
                        eligibility_started
                            .elapsed()
                            .as_micros()
                            .min(u128::from(u64::MAX)) as u64,
                    );
            }

            if aggregate_intervals_cover(&conflict_intervals, entry_range) {
                continue;
            }

            metrics.blocks_raw_decoded = metrics.blocks_raw_decoded.saturating_add(1);
            #[cfg(feature = "slow-trace")]
            {
                let interval_kind = if !fully_covered {
                    GroupedRawIntervalKind::CandidatePartial
                } else if !one_bucket {
                    GroupedRawIntervalKind::CandidateCrossBucket
                } else {
                    GroupedRawIntervalKind::CandidateOther
                };
                metrics.profile.record_raw_interval(
                    candidate.interval_scope.to_string(),
                    entry_range,
                    interval_kind,
                );
            }
            let include_timestamp = |timestamp| {
                request.time_range.contains(timestamp)
                    && !aggregate_intervals_contain(&conflict_intervals, timestamp)
            };
            macro_rules! record_query_range_survivor {
                ($timestamp:expr) => {
                    #[cfg(feature = "slow-trace")]
                    if request.time_range.contains($timestamp) {
                        metrics.profile.target_values_surviving_query_range = metrics
                            .profile
                            .target_values_surviving_query_range
                            .saturating_add(1);
                    }
                };
            }
            macro_rules! read_candidate_values {
                ($values:expr) => {{
                    #[cfg(feature = "slow-trace")]
                    let decode_started = Instant::now();
                    let values = $values;
                    #[cfg(feature = "slow-trace")]
                    metrics.profile.record_direct_target_block_decode(
                        GroupedPhysicalBlockIdentity::new(
                            candidate.reader.path().to_string_lossy(),
                            candidate.entry.offset,
                            candidate.entry.min_time,
                            candidate.entry.max_time,
                        ),
                        values.len() as u64,
                        u64::from(candidate.entry.size),
                        decode_started
                            .elapsed()
                            .as_micros()
                            .min(u128::from(u64::MAX)) as u64,
                    );
                    values
                }};
            }
            macro_rules! add_values {
                ($values:expr, $add:ident) => {
                    for value in read_candidate_values!($values) {
                        record_query_range_survivor!(value.unix_nano);
                        if include_timestamp(value.unix_nano) {
                            let key = GroupedAggregateKey {
                                window_start: request
                                    .window
                                    .bucket_start(value.unix_nano)
                                    .map_err(|reason| Error::StringErr {
                                        reason: reason.to_string(),
                                    })?,
                                tag_values: candidate.tag_values.clone(),
                            };
                            groups
                                .entry(key)
                                .or_insert_with(|| AggregateState::empty(field.typ))
                                .$add(value.value);
                            metrics.points_raw_decoded =
                                metrics.points_raw_decoded.saturating_add(1);
                            #[cfg(feature = "slow-trace")]
                            {
                                metrics.profile.target_values_consumed =
                                    metrics.profile.target_values_consumed.saturating_add(1);
                            }
                        }
                    }
                };
            }
            #[cfg(feature = "slow-trace")]
            let direct_consume_started = Instant::now();
            match field.typ {
                crate::models::INTEGER => add_values!(
                    candidate
                        .reader
                        .read_integer_block_at(&candidate.entry)
                        .context(TSMReaderSnafu)?,
                    add_integer
                ),
                crate::models::UNSIGNED => add_values!(
                    candidate
                        .reader
                        .read_unsigned_block_at(&candidate.entry)
                        .context(TSMReaderSnafu)?,
                    add_unsigned
                ),
                crate::models::FLOAT => add_values!(
                    candidate
                        .reader
                        .read_float_block_at(&candidate.entry)
                        .context(TSMReaderSnafu)?,
                    add_float
                ),
                crate::models::BOOL => {
                    for value in read_candidate_values!(
                        candidate
                            .reader
                            .read_bool_block_at(&candidate.entry)
                            .context(TSMReaderSnafu)?
                    ) {
                        record_query_range_survivor!(value.unix_nano);
                        if include_timestamp(value.unix_nano) {
                            let key = GroupedAggregateKey {
                                window_start: request
                                    .window
                                    .bucket_start(value.unix_nano)
                                    .map_err(|reason| Error::StringErr {
                                        reason: reason.to_string(),
                                    })?,
                                tag_values: candidate.tag_values.clone(),
                            };
                            groups
                                .entry(key)
                                .or_insert_with(|| AggregateState::empty(field.typ))
                                .add_count(1);
                            metrics.points_raw_decoded =
                                metrics.points_raw_decoded.saturating_add(1);
                            #[cfg(feature = "slow-trace")]
                            {
                                metrics.profile.target_values_consumed =
                                    metrics.profile.target_values_consumed.saturating_add(1);
                            }
                        }
                    }
                }
                crate::models::STRING => {
                    for value in read_candidate_values!(
                        candidate
                            .reader
                            .read_string_block_at(&candidate.entry)
                            .context(TSMReaderSnafu)?
                    ) {
                        record_query_range_survivor!(value.unix_nano);
                        if include_timestamp(value.unix_nano) {
                            let key = GroupedAggregateKey {
                                window_start: request
                                    .window
                                    .bucket_start(value.unix_nano)
                                    .map_err(|reason| Error::StringErr {
                                        reason: reason.to_string(),
                                    })?,
                                tag_values: candidate.tag_values.clone(),
                            };
                            groups
                                .entry(key)
                                .or_insert_with(|| AggregateState::empty(field.typ))
                                .add_count(1);
                            metrics.points_raw_decoded =
                                metrics.points_raw_decoded.saturating_add(1);
                            #[cfg(feature = "slow-trace")]
                            {
                                metrics.profile.target_values_consumed =
                                    metrics.profile.target_values_consumed.saturating_add(1);
                            }
                        }
                    }
                }
                _ => unreachable!(),
            }
            #[cfg(feature = "slow-trace")]
            {
                metrics.profile.direct_raw_consume_wall_us =
                    metrics.profile.direct_raw_consume_wall_us.saturating_add(
                        direct_consume_started
                            .elapsed()
                            .as_micros()
                            .min(u128::from(u64::MAX)) as u64,
                    );
            }
        }

        #[cfg(feature = "slow-trace")]
        assert_grouped_eligibility_accounting(&metrics);

        for interval in conflict_intervals {
            metrics.blocks_raw_decoded = metrics.blocks_raw_decoded.saturating_add(
                readers
                    .as_slice()
                    .iter()
                    .map(|reader| {
                        tag_sets
                            .iter()
                            .flat_map(|tag_set| tag_set.series_keys.iter())
                            .map(|series_key| {
                                let key =
                                    format!("{series_key}{KEY_FIELD_SEPARATOR}{}", target.field);
                                reader
                                    .entries(key.as_bytes())
                                    .into_iter()
                                    .filter(|entry| {
                                        entry.min_time <= interval.end_time
                                            && interval.start_time <= entry.max_time
                                    })
                                    .count() as u64
                            })
                            .sum::<u64>()
                    })
                    .sum::<u64>(),
            );
            #[cfg(feature = "slow-trace")]
            for series_key in &matched_series_keys {
                metrics.profile.record_raw_interval(
                    format!("{series_key}{KEY_FIELD_SEPARATOR}{}", target.field),
                    interval,
                    GroupedRawIntervalKind::VisibilityOverlap,
                );
            }
            let mut overlap_scan = scan.clone();
            overlap_scan.time_range = interval;
            self.grouped_aggregate_raw_states(
                overlap_scan,
                field.typ,
                &request,
                &mut groups,
                &mut metrics,
                #[cfg(feature = "slow-trace")]
                GroupedPhysicalScanKind::VisibilityOverlap,
            )?;
        }

        if include_row_domain {
            self.ensure_grouped_row_domain(
                &matched_series_keys,
                row_domain_coverage
                    .as_ref()
                    .expect("row-domain coverage must exist when requested"),
                field.typ,
                &request,
                &cache,
                Arc::clone(&readers),
                &mut groups,
                &mut metrics,
                !has_residual_filter,
            )?;
        }
        #[cfg(feature = "slow-trace")]
        assert_grouped_raw_execution_accounting(&metrics);
        metrics.storage_scan_us = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        Ok(Some(GroupedAggregateResult {
            groups: groups
                .into_iter()
                .map(|(key, state)| GroupedAggregateGroup {
                    key,
                    states: vec![state],
                })
                .collect(),
            metrics,
        }))
    }

    fn grouped_request_cancelled(
        request: &GroupedAggregateRequest,
        sibling_cancellation: &CancellationToken,
    ) -> bool {
        sibling_cancellation.is_cancelled()
            || request
                .cancellation
                .as_ref()
                .is_some_and(|cancellation| cancellation.is_cancelled())
    }

    async fn acquire_grouped_cpu_permit(
        budget: &Arc<StorageQueryCpuBudget>,
        request: &GroupedAggregateRequest,
        sibling_cancellation: &CancellationToken,
    ) -> Result<(StorageQueryCpuPermit, GroupedPermitAdmission)> {
        if Self::grouped_request_cancelled(request, sibling_cancellation) {
            return Err(Error::ScanCancelled);
        }
        #[cfg(feature = "slow-trace")]
        let requested = Instant::now();
        if let Ok(permit) = budget.try_acquire() {
            #[cfg(feature = "slow-trace")]
            return Ok((
                permit,
                GroupedPermitAdmission {
                    requested,
                    acquired: Instant::now(),
                    immediate: true,
                },
            ));
            #[cfg(not(feature = "slow-trace"))]
            return Ok((permit, ()));
        }

        let request_cancellation = request
            .cancellation
            .as_ref()
            .map(|cancellation| cancellation.token());
        let sibling_cancellation = sibling_cancellation.clone();
        let permit = if let Some(request_cancellation) = request_cancellation {
            tokio::select! {
                permit = budget.acquire() => Ok(permit),
                _ = request_cancellation.cancelled() => Err(Error::ScanCancelled),
                _ = sibling_cancellation.cancelled() => Err(Error::ScanCancelled),
            }
        } else {
            tokio::select! {
                permit = budget.acquire() => Ok(permit),
                _ = sibling_cancellation.cancelled() => Err(Error::ScanCancelled),
            }
        }?;
        #[cfg(feature = "slow-trace")]
        return Ok((
            permit,
            GroupedPermitAdmission {
                requested,
                acquired: Instant::now(),
                immediate: false,
            },
        ));
        #[cfg(not(feature = "slow-trace"))]
        Ok((permit, ()))
    }

    fn merge_grouped_single_target_groups(
        groups: &mut BTreeMap<GroupedAggregateKey, AggregateState>,
        local: BTreeMap<GroupedAggregateKey, AggregateState>,
    ) -> Result<()> {
        for (key, state) in local {
            match groups.entry(key) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(state);
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    entry
                        .get_mut()
                        .merge(state)
                        .map_err(|reason| Error::StringErr {
                            reason: reason.to_string(),
                        })?;
                }
            }
        }
        Ok(())
    }

    /// Partitions storage keys exactly like InfluxDB v2.0.6 partitions series
    /// iterators: contiguous equal-sized prefixes, with the remainder assigned
    /// to the final partition.
    fn partition_grouped_storage_key_jobs(
        jobs: Vec<GroupedAggregateStorageKeyJob>,
        max_workers: usize,
    ) -> Vec<GroupedAggregateWorkerPartition> {
        if jobs.is_empty() {
            return Vec::new();
        }

        let partition_count = jobs.len().min(max_workers.max(1));
        let keys_per_partition = jobs.len() / partition_count;
        let mut jobs = jobs.into_iter();
        (0..partition_count)
            .map(|index| {
                let jobs = if index < partition_count - 1 {
                    jobs.by_ref().take(keys_per_partition).collect()
                } else {
                    jobs.by_ref().collect()
                };
                GroupedAggregateWorkerPartition { index, jobs }
            })
            .collect()
    }

    async fn execute_grouped_storage_key_partitions(
        &self,
        jobs: Vec<GroupedAggregateStorageKeyJob>,
        target_count: usize,
        request: Arc<GroupedAggregateRequest>,
    ) -> Result<(GroupedWorkerGroups, GroupedAggregateMetrics)> {
        #[cfg(feature = "slow-trace")]
        let coordinator_started = Instant::now();
        #[cfg(feature = "slow-trace")]
        let coordinator_start_unix_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros()
            .min(u128::from(u64::MAX)) as u64;
        let budget = self.executor.storage_query_cpu_budget();
        let storage_keys_total = jobs.len();
        let partitions = Self::partition_grouped_storage_key_jobs(jobs, budget.capacity());
        let partition_count = partitions.len();
        let sibling_cancellation = CancellationToken::new();
        let mut pending = partitions.into_iter();
        let mut handles = FuturesUnordered::new();
        let mut completed = BTreeMap::new();
        let mut groups = (0..target_count)
            .map(|_| BTreeMap::new())
            .collect::<GroupedWorkerGroups>();
        let mut metrics = GroupedAggregateMetrics::default();
        let mut next_merge = 0usize;
        let mut first_error = None;
        #[cfg(feature = "slow-trace")]
        let tracker = Arc::new(GroupedParallelJobTracker::default());
        #[cfg(feature = "slow-trace")]
        let diagnostics = request
            .cancellation
            .as_ref()
            .and_then(ScanCancellation::grouped_diagnostics)
            .cloned();
        #[cfg(feature = "slow-trace")]
        let mut permit_admission_done_us = 0u64;
        #[cfg(feature = "slow-trace")]
        let mut first_job_submitted = None;
        #[cfg(feature = "slow-trace")]
        let mut raw_blocks_per_partition = Vec::with_capacity(partition_count);
        #[cfg(feature = "slow-trace")]
        let mut merge_us = 0u64;
        #[cfg(feature = "slow-trace")]
        let partition_key_counts = pending
            .as_slice()
            .iter()
            .map(|partition| partition.jobs.len() as u64)
            .collect::<Vec<_>>();
        #[cfg(feature = "slow-trace")]
        let partition_candidate_counts = pending
            .as_slice()
            .iter()
            .map(|partition| {
                partition
                    .jobs
                    .iter()
                    .map(|job| job.candidates.len() as u64)
                    .sum::<u64>()
            })
            .collect::<Vec<_>>();

        while let Some(partition) = pending.next() {
            if first_error.is_some() {
                break;
            }
            let (permit, permit_admission) = match Self::acquire_grouped_cpu_permit(
                &budget,
                request.as_ref(),
                &sibling_cancellation,
            )
            .await
            {
                Ok(admission) => admission,
                Err(error) => {
                    first_error = Some(error);
                    sibling_cancellation.cancel();
                    break;
                }
            };
            #[cfg(feature = "slow-trace")]
            {
                permit_admission_done_us = coordinator_started
                    .elapsed()
                    .as_micros()
                    .min(u128::from(u64::MAX)) as u64;
            }
            #[cfg(not(feature = "slow-trace"))]
            let _ = permit_admission;

            let request = Arc::clone(&request);
            let worker_cancellation = sibling_cancellation.clone();
            #[cfg(feature = "slow-trace")]
            let submission = tracker.submit(
                diagnostics.as_ref(),
                partition.index,
                permit_admission.requested,
                permit_admission.acquired,
                permit_admission.immediate,
            );
            #[cfg(feature = "slow-trace")]
            first_job_submitted.get_or_insert_with(Instant::now);
            let partition_index = partition.index;
            #[cfg(feature = "slow-trace")]
            let blocking_diagnostics = Arc::new(GeneralBlockingTaskDiagnostics::new(
                GeneralBlockingTaskClass::GroupedStorageLeaf,
            ));
            #[cfg(feature = "slow-trace")]
            if let Some(diagnostics) = &diagnostics {
                diagnostics.record_general_blocking_task(
                    partition_index,
                    Arc::clone(&blocking_diagnostics),
                );
            }
            #[cfg(feature = "slow-trace")]
            let handle = self.executor.spawn_storage_query_cpu_with_diagnostics(
                blocking_diagnostics,
                move || {
                    let mut worker_guard = submission.start(permit.start());
                    let result = Self::execute_grouped_storage_key_partition(
                        partition,
                        target_count,
                        request.as_ref(),
                        &worker_cancellation,
                    );
                    worker_guard.finish(&result);
                    (partition_index, result)
                },
            );
            #[cfg(not(feature = "slow-trace"))]
            let handle = self.executor.spawn_storage_query_cpu(move || {
                #[cfg(feature = "slow-trace")]
                let mut worker_guard = submission.start(permit.start());
                #[cfg(not(feature = "slow-trace"))]
                let _permit = permit.start();
                let result = Self::execute_grouped_storage_key_partition(
                    partition,
                    target_count,
                    request.as_ref(),
                    &worker_cancellation,
                );
                #[cfg(feature = "slow-trace")]
                worker_guard.finish(&result);
                (partition_index, result)
            });
            match handle {
                Ok(handle) => {
                    handles.push(handle);
                }
                Err(source) => {
                    first_error = Some(Error::Execution { source });
                    sibling_cancellation.cancel();
                    break;
                }
            }
        }

        #[cfg(feature = "slow-trace")]
        let all_jobs_submitted_us = coordinator_started
            .elapsed()
            .as_micros()
            .min(u128::from(u64::MAX)) as u64;

        while let Some(joined) = handles.next().await {
            match joined {
                Ok((partition_index, Ok(result))) => {
                    #[cfg(feature = "slow-trace")]
                    raw_blocks_per_partition.push(result.1.blocks_raw_decoded);
                    completed.insert(partition_index, result);
                }
                Ok((_partition_index, Err(error))) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                    sibling_cancellation.cancel();
                }
                Err(_) => {
                    if first_error.is_none() {
                        first_error = Some(Error::TaskTerminated);
                    }
                    sibling_cancellation.cancel();
                }
            }

            while first_error.is_none() {
                let Some((local_groups, local_metrics)) = completed.remove(&next_merge) else {
                    break;
                };
                #[cfg(feature = "slow-trace")]
                let merge_started = Instant::now();
                for (target_groups, local_target_groups) in groups.iter_mut().zip(local_groups) {
                    if let Err(error) =
                        Self::merge_grouped_single_target_groups(target_groups, local_target_groups)
                    {
                        first_error = Some(error);
                        sibling_cancellation.cancel();
                        break;
                    }
                }
                if first_error.is_some() {
                    break;
                }
                metrics.merge(local_metrics);
                #[cfg(feature = "slow-trace")]
                {
                    merge_us = merge_us.saturating_add(
                        merge_started
                            .elapsed()
                            .as_micros()
                            .min(u128::from(u64::MAX)) as u64,
                    );
                }
                next_merge = next_merge.saturating_add(1);
            }
        }

        #[cfg(feature = "slow-trace")]
        {
            let submitted = tracker.submitted.load(Ordering::Relaxed);
            let started = tracker.started.load(Ordering::Relaxed);
            let finished = tracker.finished.load(Ordering::Relaxed);
            let completed = tracker.completed.load(Ordering::Relaxed);
            let cancelled = if first_error.is_some() {
                (partition_count as u64).saturating_sub(completed)
            } else {
                tracker.cancelled.load(Ordering::Relaxed)
            };
            let permit_wait_us = tracker.permit_wait_us.lock().clone();
            let permit_hold_us = tracker.permit_hold_us.lock().clone();
            let blocking_queue_wait_us = tracker.queue_wait_us.lock().clone();
            let worker_execution_us = tracker.execution_us.lock().clone();
            let all_jobs_completed_at = tracker.finish_times.lock().iter().copied().max();
            let all_jobs_completed_us = all_jobs_completed_at
                .map(|completed| {
                    completed
                        .saturating_duration_since(coordinator_started)
                        .as_micros()
                        .min(u128::from(u64::MAX)) as u64
                })
                .unwrap_or(all_jobs_submitted_us);
            raw_blocks_per_partition.sort_unstable();
            metrics.profile.storage_keys_total = storage_keys_total as u64;
            metrics.profile.partition_workers_total = partition_count as u64;
            metrics.profile.partition_workers_started = started;
            metrics.profile.partition_workers_completed = completed;
            metrics.profile.partition_workers_cancelled = cancelled;
            metrics.profile.worker_active_peak = tracker.active_peak.load(Ordering::Relaxed);
            metrics.profile.global_cpu_capacity = budget.capacity() as u64;
            metrics.profile.global_active_jobs_peak = budget.active_jobs_peak() as u64;
            metrics.profile.permit_immediate_count =
                tracker.permit_immediate.load(Ordering::Relaxed);
            metrics.profile.spawn_blocking_jobs = submitted;
            metrics.profile.blocking_jobs_started = started;
            metrics.profile.blocking_jobs_completed = finished;
            metrics.profile.blocking_active_current = tracker.active.load(Ordering::Relaxed);
            metrics.profile.blocking_active_peak = tracker.active_peak.load(Ordering::Relaxed);
            metrics.profile.blocking_queued_current = tracker.queued.load(Ordering::Relaxed);
            metrics.profile.blocking_queued_peak = tracker.queued_peak.load(Ordering::Relaxed);
            metrics.profile.keys_per_partition_min =
                partition_key_counts.iter().copied().min().unwrap_or(0);
            metrics.profile.keys_per_partition_max =
                partition_key_counts.iter().copied().max().unwrap_or(0);
            metrics.profile.candidates_per_partition_min = partition_candidate_counts
                .iter()
                .copied()
                .min()
                .unwrap_or(0);
            metrics.profile.candidates_per_partition_max = partition_candidate_counts
                .iter()
                .copied()
                .max()
                .unwrap_or(0);
            metrics.profile.raw_blocks_per_partition_min =
                raw_blocks_per_partition.first().copied().unwrap_or(0);
            metrics.profile.raw_blocks_per_partition_max =
                raw_blocks_per_partition.last().copied().unwrap_or(0);
            metrics.profile.merge_us = merge_us;
            metrics.profile.coordinator_start_unix_us = coordinator_start_unix_us;
            metrics.profile.coordinator_permit_admission_done_us = permit_admission_done_us;
            metrics.profile.coordinator_all_jobs_submitted_us = all_jobs_submitted_us;
            metrics.profile.coordinator_all_jobs_completed_us = all_jobs_completed_us;
            metrics.profile.coordinator_result_ready_us = coordinator_started
                .elapsed()
                .as_micros()
                .min(u128::from(u64::MAX))
                as u64;
            metrics.profile.coordinator_admission_wait_us = permit_wait_us
                .iter()
                .copied()
                .fold(0u64, u64::saturating_add);
            metrics.profile.coordinator_storage_job_wait_us = first_job_submitted
                .zip(all_jobs_completed_at)
                .map(|(submitted, completed)| {
                    completed
                        .saturating_duration_since(submitted)
                        .as_micros()
                        .min(u128::from(u64::MAX)) as u64
                })
                .unwrap_or(0);
            metrics.profile.coordinator_result_assembly_us = merge_us;
            metrics.profile.record_scheduler_samples(
                permit_wait_us,
                permit_hold_us,
                blocking_queue_wait_us,
                worker_execution_us,
            );
        }

        if let Some(error) = first_error {
            #[cfg(feature = "slow-trace")]
            slow_trace::log_grouped_storage_jobs(
                "error",
                &format!(
                    "storage_keys_total={} partition_workers_total={} partition_workers_started={} partition_workers_completed={} partition_workers_cancelled={} worker_active_peak={} global_cpu_capacity={} global_permit_wait_count={} global_permit_wait_us={} spawn_blocking_jobs={} keys_per_partition_min={} keys_per_partition_max={} candidates_per_partition_min={} candidates_per_partition_max={} raw_blocks_per_partition_min={} raw_blocks_per_partition_max={} worker_execution_us_min={} worker_execution_us_p50={} worker_execution_us_p95={} worker_execution_us_max={} merge_us={} error={error}",
                    metrics.profile.storage_keys_total,
                    metrics.profile.partition_workers_total,
                    metrics.profile.partition_workers_started,
                    metrics.profile.partition_workers_completed,
                    metrics.profile.partition_workers_cancelled,
                    metrics.profile.worker_active_peak,
                    metrics.profile.global_cpu_capacity,
                    metrics.profile.global_permit_wait_count,
                    metrics.profile.global_permit_wait_us,
                    metrics.profile.spawn_blocking_jobs,
                    metrics.profile.keys_per_partition_min,
                    metrics.profile.keys_per_partition_max,
                    metrics.profile.candidates_per_partition_min,
                    metrics.profile.candidates_per_partition_max,
                    metrics.profile.raw_blocks_per_partition_min,
                    metrics.profile.raw_blocks_per_partition_max,
                    metrics.profile.worker_execution_us_min,
                    metrics.profile.worker_execution_us_p50,
                    metrics.profile.worker_execution_us_p95,
                    metrics.profile.worker_execution_us_max,
                    metrics.profile.merge_us,
                ),
            );
            return Err(error);
        }
        Ok((groups, metrics))
    }

    fn execute_grouped_storage_key_partition(
        partition: GroupedAggregateWorkerPartition,
        target_count: usize,
        request: &GroupedAggregateRequest,
        sibling_cancellation: &CancellationToken,
    ) -> GroupedPartitionWorkerResult {
        if Self::grouped_request_cancelled(request, sibling_cancellation) {
            return Err(Error::ScanCancelled);
        }

        let mut groups = (0..target_count)
            .map(|_| BTreeMap::new())
            .collect::<GroupedWorkerGroups>();
        let mut metrics = GroupedAggregateMetrics::default();
        for job in partition.jobs {
            if Self::grouped_request_cancelled(request, sibling_cancellation) {
                return Err(Error::ScanCancelled);
            }
            let target_groups =
                groups
                    .get_mut(job.target_index)
                    .ok_or_else(|| Error::StringErr {
                        reason: "grouped aggregate storage key has an invalid target index"
                            .to_string(),
                    })?;
            Self::execute_grouped_storage_key_job(
                job,
                request,
                sibling_cancellation,
                target_groups,
                &mut metrics,
            )?;
        }
        Ok((groups, metrics))
    }

    fn execute_grouped_storage_key_job(
        job: GroupedAggregateStorageKeyJob,
        request: &GroupedAggregateRequest,
        sibling_cancellation: &CancellationToken,
        groups: &mut BTreeMap<GroupedAggregateKey, AggregateState>,
        metrics: &mut GroupedAggregateMetrics,
    ) -> Result<()> {
        let field_type = job.field_type;

        for candidate in job.candidates {
            if Self::grouped_request_cancelled(request, sibling_cancellation) {
                return Err(Error::ScanCancelled);
            }
            #[cfg(feature = "slow-trace")]
            let eligibility_started = Instant::now();
            let fully_covered = request.time_range.start_time <= candidate.entry.min_time
                && request.time_range.end_time >= candidate.entry.max_time;
            let first_bucket = request
                .window
                .bucket_start(candidate.entry.min_time)
                .map_err(|reason| Error::StringErr {
                    reason: reason.to_string(),
                })?;
            let one_bucket = request
                .window
                .bucket_start(candidate.entry.max_time)
                .map_err(|reason| Error::StringErr {
                    reason: reason.to_string(),
                })?
                == first_bucket;
            if fully_covered
                && one_bucket
                && let Some(state) =
                    Self::aggregate_state_from_entry(field_type, &candidate.entry, false)?
            {
                let key = GroupedAggregateKey {
                    window_start: first_bucket,
                    tag_values: candidate.tag_values,
                };
                Self::merge_grouped_single_target_groups(groups, BTreeMap::from([(key, state)]))?;
                metrics.blocks_statistics_used = metrics.blocks_statistics_used.saturating_add(1);
                #[cfg(feature = "slow-trace")]
                {
                    metrics
                        .profile
                        .record_eligibility(GroupedBlockEligibility::StatisticsUsed);
                    metrics
                        .profile
                        .record_statistics_block(GroupedPhysicalBlockIdentity::new(
                            candidate.reader.path().to_string_lossy(),
                            candidate.entry.offset,
                            candidate.entry.min_time,
                            candidate.entry.max_time,
                        ));
                    metrics.profile.block_eligibility_wall_us =
                        metrics.profile.block_eligibility_wall_us.saturating_add(
                            eligibility_started
                                .elapsed()
                                .as_micros()
                                .min(u128::from(u64::MAX)) as u64,
                        );
                }
                continue;
            }

            #[cfg(feature = "slow-trace")]
            {
                let reason = if !fully_covered {
                    GroupedBlockEligibility::PartialQueryRange
                } else if !one_bucket {
                    GroupedBlockEligibility::CrossBucket
                } else if candidate.entry.statistics.is_none() {
                    GroupedBlockEligibility::MissingStatistics
                } else {
                    GroupedBlockEligibility::Other
                };
                metrics.profile.record_eligibility(reason);
                metrics.profile.block_eligibility_wall_us =
                    metrics.profile.block_eligibility_wall_us.saturating_add(
                        eligibility_started
                            .elapsed()
                            .as_micros()
                            .min(u128::from(u64::MAX)) as u64,
                    );
            }

            metrics.blocks_raw_decoded = metrics.blocks_raw_decoded.saturating_add(1);
            let entry_range = TimeRange::new(
                candidate.entry.min_time.max(request.time_range.start_time),
                candidate.entry.max_time.min(request.time_range.end_time),
            );
            #[cfg(feature = "slow-trace")]
            {
                let interval_kind = if !fully_covered {
                    GroupedRawIntervalKind::CandidatePartial
                } else if !one_bucket {
                    GroupedRawIntervalKind::CandidateCrossBucket
                } else {
                    GroupedRawIntervalKind::CandidateOther
                };
                metrics.profile.record_raw_interval(
                    candidate.interval_scope.to_string(),
                    entry_range,
                    interval_kind,
                );
            }
            let include_timestamp = |timestamp| request.time_range.contains(timestamp);
            macro_rules! record_query_range_survivor {
                ($timestamp:expr) => {
                    #[cfg(feature = "slow-trace")]
                    if request.time_range.contains($timestamp) {
                        metrics.profile.target_values_surviving_query_range = metrics
                            .profile
                            .target_values_surviving_query_range
                            .saturating_add(1);
                    }
                };
            }
            macro_rules! read_candidate_values {
                ($values:expr) => {{
                    #[cfg(feature = "slow-trace")]
                    let decode_started = Instant::now();
                    let values = $values;
                    #[cfg(feature = "slow-trace")]
                    metrics.profile.record_direct_target_block_decode(
                        GroupedPhysicalBlockIdentity::new(
                            candidate.reader.path().to_string_lossy(),
                            candidate.entry.offset,
                            candidate.entry.min_time,
                            candidate.entry.max_time,
                        ),
                        values.len() as u64,
                        u64::from(candidate.entry.size),
                        decode_started
                            .elapsed()
                            .as_micros()
                            .min(u128::from(u64::MAX)) as u64,
                    );
                    values
                }};
            }
            macro_rules! check_value_cancellation {
                ($index:expr) => {
                    if $index % 256 == 0
                        && Self::grouped_request_cancelled(request, sibling_cancellation)
                    {
                        return Err(Error::ScanCancelled);
                    }
                };
            }
            macro_rules! add_values {
                ($values:expr, $add:ident) => {
                    for (value_index, value) in
                        read_candidate_values!($values).into_iter().enumerate()
                    {
                        check_value_cancellation!(value_index);
                        record_query_range_survivor!(value.unix_nano);
                        if include_timestamp(value.unix_nano) {
                            let key = GroupedAggregateKey {
                                window_start: request
                                    .window
                                    .bucket_start(value.unix_nano)
                                    .map_err(|reason| Error::StringErr {
                                        reason: reason.to_string(),
                                    })?,
                                tag_values: candidate.tag_values.clone(),
                            };
                            groups
                                .entry(key)
                                .or_insert_with(|| AggregateState::empty(field_type))
                                .$add(value.value);
                            metrics.points_raw_decoded =
                                metrics.points_raw_decoded.saturating_add(1);
                            #[cfg(feature = "slow-trace")]
                            {
                                metrics.profile.target_values_consumed =
                                    metrics.profile.target_values_consumed.saturating_add(1);
                            }
                        }
                    }
                };
            }
            #[cfg(feature = "slow-trace")]
            let direct_consume_started = Instant::now();
            match field_type {
                crate::models::INTEGER => add_values!(
                    candidate
                        .reader
                        .read_integer_block_at(&candidate.entry)
                        .context(TSMReaderSnafu)?,
                    add_integer
                ),
                crate::models::UNSIGNED => add_values!(
                    candidate
                        .reader
                        .read_unsigned_block_at(&candidate.entry)
                        .context(TSMReaderSnafu)?,
                    add_unsigned
                ),
                crate::models::FLOAT => add_values!(
                    candidate
                        .reader
                        .read_float_block_at(&candidate.entry)
                        .context(TSMReaderSnafu)?,
                    add_float
                ),
                crate::models::BOOL => {
                    for (value_index, value) in read_candidate_values!(
                        candidate
                            .reader
                            .read_bool_block_at(&candidate.entry)
                            .context(TSMReaderSnafu)?
                    )
                    .into_iter()
                    .enumerate()
                    {
                        check_value_cancellation!(value_index);
                        record_query_range_survivor!(value.unix_nano);
                        if include_timestamp(value.unix_nano) {
                            let key = GroupedAggregateKey {
                                window_start: request
                                    .window
                                    .bucket_start(value.unix_nano)
                                    .map_err(|reason| Error::StringErr {
                                        reason: reason.to_string(),
                                    })?,
                                tag_values: candidate.tag_values.clone(),
                            };
                            groups
                                .entry(key)
                                .or_insert_with(|| AggregateState::empty(field_type))
                                .add_count(1);
                            metrics.points_raw_decoded =
                                metrics.points_raw_decoded.saturating_add(1);
                            #[cfg(feature = "slow-trace")]
                            {
                                metrics.profile.target_values_consumed =
                                    metrics.profile.target_values_consumed.saturating_add(1);
                            }
                        }
                    }
                }
                crate::models::STRING => {
                    for (value_index, value) in read_candidate_values!(
                        candidate
                            .reader
                            .read_string_block_at(&candidate.entry)
                            .context(TSMReaderSnafu)?
                    )
                    .into_iter()
                    .enumerate()
                    {
                        check_value_cancellation!(value_index);
                        record_query_range_survivor!(value.unix_nano);
                        if include_timestamp(value.unix_nano) {
                            let key = GroupedAggregateKey {
                                window_start: request
                                    .window
                                    .bucket_start(value.unix_nano)
                                    .map_err(|reason| Error::StringErr {
                                        reason: reason.to_string(),
                                    })?,
                                tag_values: candidate.tag_values.clone(),
                            };
                            groups
                                .entry(key)
                                .or_insert_with(|| AggregateState::empty(field_type))
                                .add_count(1);
                            metrics.points_raw_decoded =
                                metrics.points_raw_decoded.saturating_add(1);
                            #[cfg(feature = "slow-trace")]
                            {
                                metrics.profile.target_values_consumed =
                                    metrics.profile.target_values_consumed.saturating_add(1);
                            }
                        }
                    }
                }
                _ => unreachable!(),
            }
            #[cfg(feature = "slow-trace")]
            {
                metrics.profile.direct_raw_consume_wall_us =
                    metrics.profile.direct_raw_consume_wall_us.saturating_add(
                        direct_consume_started
                            .elapsed()
                            .as_micros()
                            .min(u128::from(u64::MAX)) as u64,
                    );
            }
        }

        Ok(())
    }

    async fn try_grouped_multi_target_partitioned_states(
        &self,
        request: &GroupedAggregateRequest,
        row_domain_fields: &[(String, FieldType)],
    ) -> Result<GroupedMultiTargetPartitionedResult> {
        if self.vlog_enabled {
            return Ok(GroupedMultiTargetPartitionedResult::NotApplicable);
        }

        let started = Instant::now();
        let (cache, file_store) = {
            let inner = self.inner.read();
            (Arc::clone(&inner.cache), Arc::clone(&inner.file_store))
        };
        let readers = Arc::new(AggregateReaderSet::new(file_store.readers()));
        let mut jobs = Vec::new();
        let mut covered_target_fields = HashSet::with_capacity(request.targets.len());
        let mut metrics = GroupedAggregateMetrics {
            fields_projected: request.targets.len() as u64,
            ..Default::default()
        };

        for (target_index, target) in request.targets.iter().enumerate() {
            if request
                .cancellation
                .as_ref()
                .is_some_and(crate::query::storage_ir::ScanCancellation::is_cancelled)
            {
                return Err(Error::ScanCancelled);
            }
            let Some((_, field_type)) = row_domain_fields
                .iter()
                .find(|(field_name, _)| field_name == &target.field)
            else {
                return Ok(GroupedMultiTargetPartitionedResult::NotApplicable);
            };
            let field_type = *field_type;
            let scan = self.grouped_aggregate_storage_scan(request, target, field_type)?;
            let plan = Arc::new(EngineScanPlan {
                scan: Arc::new(scan.clone()),
                vlog_enabled: self.vlog_enabled,
            });
            let index_set =
                index::IndexSet::new(Arc::clone(&self.index), Arc::clone(&self.sfile), None);
            let tag_sets = index_set
                .tag_sets_for_storage_scan(Arc::clone(&self.sfile), &scan, plan.max_series_n())
                .map_err(|source| match source {
                    index::Error::StorageScanCancelled => Error::ScanCancelled,
                    source => Error::IndexSet { source },
                })?;
            let series_matched = tag_sets
                .iter()
                .map(|tag_set| tag_set.series_keys.len() as u64)
                .sum::<u64>();
            metrics.series_matched = metrics.series_matched.max(series_matched);
            #[cfg(feature = "slow-trace")]
            {
                metrics.profile.shard_scans = metrics.profile.shard_scans.saturating_add(1);
            }

            let mut target_jobs = Vec::new();
            let mut target_candidate_count = 0usize;
            let mut conflict_intervals = Vec::new();
            #[cfg(feature = "slow-trace")]
            let candidate_discovery_started = Instant::now();
            for tag_set in &tag_sets {
                for (series_key, residual) in
                    tag_set.series_keys.iter().zip(&tag_set.storage_filters)
                {
                    if request
                        .cancellation
                        .as_ref()
                        .is_some_and(crate::query::storage_ir::ScanCancellation::is_cancelled)
                    {
                        return Err(Error::ScanCancelled);
                    }
                    if residual.is_some() {
                        return Ok(GroupedMultiTargetPartitionedResult::NotApplicable);
                    }
                    let tag_values = Self::grouped_tag_values(series_key, &request.group_tags)?;
                    let key = format!("{series_key}{KEY_FIELD_SEPARATOR}{}", target.field);
                    if cache
                        .values(key.as_bytes())
                        .iter()
                        .any(|value| request.time_range.contains(value.unix_nano()))
                    {
                        return Ok(GroupedMultiTargetPartitionedResult::NotApplicable);
                    }
                    #[cfg(feature = "slow-trace")]
                    let interval_scope = Arc::<str>::from(key.as_str());

                    let mut series_candidates = Vec::new();
                    for reader in readers.as_slice() {
                        let tombstones = reader.tombstone_range(key.as_bytes());
                        let entries = reader.entries(key.as_bytes());
                        #[cfg(feature = "slow-trace")]
                        {
                            metrics.profile.candidate_file_index_searches = metrics
                                .profile
                                .candidate_file_index_searches
                                .saturating_add(1);
                            metrics.profile.candidate_index_entries_inspected = metrics
                                .profile
                                .candidate_index_entries_inspected
                                .saturating_add(entries.len() as u64);
                            if !entries.is_empty() {
                                metrics.profile.candidate_files_with_entries = metrics
                                    .profile
                                    .candidate_files_with_entries
                                    .saturating_add(1);
                            }
                        }
                        for entry in entries {
                            if entry.max_time < request.time_range.start_time
                                || entry.min_time > request.time_range.end_time
                            {
                                continue;
                            }
                            if tombstones.iter().any(|range| {
                                range.max >= entry.min_time && range.min <= entry.max_time
                            }) {
                                return Ok(GroupedMultiTargetPartitionedResult::NotApplicable);
                            }
                            series_candidates.push(AggregateCandidate {
                                reader: Arc::clone(reader),
                                entry,
                            });
                        }
                    }
                    conflict_intervals.extend(aggregate_conflict_intervals(
                        &series_candidates,
                        request.time_range,
                    ));
                    let candidates = series_candidates
                        .into_iter()
                        .map(|candidate| GroupedAggregateCandidate {
                            reader: candidate.reader,
                            entry: candidate.entry,
                            tag_values: tag_values.clone(),
                            #[cfg(feature = "slow-trace")]
                            interval_scope: Arc::clone(&interval_scope),
                        })
                        .collect::<Vec<_>>();
                    target_candidate_count =
                        target_candidate_count.saturating_add(candidates.len());
                    if !candidates.is_empty() {
                        target_jobs.push(GroupedAggregateStorageKeyJob {
                            target_index,
                            field_type,
                            candidates,
                        });
                    }
                }
            }
            #[cfg(feature = "slow-trace")]
            {
                metrics.profile.candidate_discovery_wall_us =
                    metrics.profile.candidate_discovery_wall_us.saturating_add(
                        candidate_discovery_started
                            .elapsed()
                            .as_micros()
                            .min(u128::from(u64::MAX)) as u64,
                    );
            }
            metrics.blocks_candidate = metrics
                .blocks_candidate
                .saturating_add(target_candidate_count as u64);

            #[cfg(feature = "slow-trace")]
            let interval_construction_started = Instant::now();
            let conflict_intervals = merge_aggregate_intervals(conflict_intervals);
            #[cfg(feature = "slow-trace")]
            {
                metrics.profile.raw_interval_construction_wall_us = metrics
                    .profile
                    .raw_interval_construction_wall_us
                    .saturating_add(
                        interval_construction_started
                            .elapsed()
                            .as_micros()
                            .min(u128::from(u64::MAX)) as u64,
                    );
            }
            if !conflict_intervals.is_empty() {
                return Ok(GroupedMultiTargetPartitionedResult::NotApplicable);
            }

            #[cfg(feature = "slow-trace")]
            for candidate in target_jobs.iter().flat_map(|job| &job.candidates) {
                observe_grouped_candidate_eligibility(
                    &mut metrics.profile,
                    &candidate.entry,
                    request,
                    field_type,
                    &conflict_intervals,
                )?;
            }
            let requires_float_min_max_proof = field_type == crate::models::FLOAT
                && target
                    .aggregates
                    .iter()
                    .any(|kind| matches!(kind, AggregateKind::Min | AggregateKind::Max));
            if requires_float_min_max_proof
                && target_jobs
                    .iter()
                    .flat_map(|job| &job.candidates)
                    .any(|candidate| {
                        candidate.entry.statistics.is_none_or(|statistics| {
                            !statistics.is_datafusion_grouped_float_min_max_safe()
                        })
                    })
            {
                return Ok(GroupedMultiTargetPartitionedResult::NotApplicable);
            }

            covered_target_fields.insert(target.field.clone());
            jobs.extend(target_jobs);
        }

        let (target_groups, partition_metrics) = self
            .execute_grouped_storage_key_partitions(
                jobs,
                request.targets.len(),
                Arc::new(request.clone()),
            )
            .await?;
        metrics.merge(partition_metrics);

        let mut groups = BTreeMap::<GroupedAggregateKey, Vec<AggregateState>>::new();
        for (target_index, target_groups) in target_groups.into_iter().enumerate() {
            for (key, state) in target_groups {
                let states = groups.entry(key).or_insert_with(|| {
                    request
                        .targets
                        .iter()
                        .map(|target| AggregateState::empty(target.expected_field_type))
                        .collect()
                });
                states[target_index] = state;
            }
        }

        let row_domain_coverage =
            GroupedRowDomainCoverage::new(row_domain_fields, &covered_target_fields);
        self.ensure_grouped_multi_target_row_domain(
            request,
            &row_domain_coverage,
            &mut groups,
            &mut metrics,
        )?;
        #[cfg(feature = "slow-trace")]
        {
            assert_grouped_raw_execution_accounting(&metrics);
        }
        metrics.storage_scan_us = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        if groups.is_empty() {
            return Ok(GroupedMultiTargetPartitionedResult::Complete(None));
        }
        Ok(GroupedMultiTargetPartitionedResult::Complete(Some(
            GroupedAggregateResult {
                groups: groups
                    .into_iter()
                    .map(|(key, states)| GroupedAggregateGroup { key, states })
                    .collect(),
                metrics,
            },
        )))
    }

    /// Executes a mixed or multi-field request containing Float64 MIN/MAX with
    /// one request-wide partition set when every target is directly eligible.
    /// Visibility or overlap fallbacks retain the established single-target
    /// correctness path.
    async fn grouped_multi_target_float_min_max_states(
        &self,
        request: &GroupedAggregateRequest,
        row_domain_fields: &[(String, FieldType)],
    ) -> Result<Option<GroupedAggregateResult>> {
        match self
            .try_grouped_multi_target_partitioned_states(request, row_domain_fields)
            .await?
        {
            GroupedMultiTargetPartitionedResult::NotApplicable => {}
            GroupedMultiTargetPartitionedResult::Complete(result) => return Ok(result),
        }

        let mut groups = BTreeMap::<GroupedAggregateKey, Vec<AggregateState>>::new();
        let mut metrics = GroupedAggregateMetrics::default();
        let mut covered_target_fields = HashSet::with_capacity(request.targets.len());

        for (target_index, target) in request.targets.iter().enumerate() {
            let mut single = request.clone();
            single.targets = vec![target.clone()];
            let Some(result) =
                Box::pin(self.grouped_aggregate_scan_with_row_domain(single, false)).await?
            else {
                continue;
            };

            let series_matched = metrics.series_matched.max(result.metrics.series_matched);
            let requires_datafusion_fallback = result.metrics.requires_datafusion_fallback;
            metrics.merge(result.metrics);
            // These scans visit the same logical series for different target
            // fields. Unlike block/value work, series cardinality is not
            // additive across targets.
            metrics.series_matched = series_matched;

            if requires_datafusion_fallback {
                metrics.fields_projected = request.targets.len() as u64;
                return Ok(Some(GroupedAggregateResult {
                    groups: Vec::new(),
                    metrics,
                }));
            }

            // The completed single-target execution has accounted for every
            // visible grouped key of this field, regardless of whether each
            // block used statistics or the canonical raw path.
            covered_target_fields.insert(target.field.clone());

            for group in result.groups {
                let [state] = group.states.as_slice() else {
                    return Err(Error::StringErr {
                        reason: "single-target grouped aggregate returned an invalid state layout"
                            .to_string(),
                    });
                };
                let states = groups.entry(group.key).or_insert_with(|| {
                    request
                        .targets
                        .iter()
                        .map(|target| AggregateState::empty(target.expected_field_type))
                        .collect()
                });
                states[target_index] = state.clone();
            }
        }

        let row_domain_coverage =
            GroupedRowDomainCoverage::new(row_domain_fields, &covered_target_fields);

        self.ensure_grouped_multi_target_row_domain(
            request,
            &row_domain_coverage,
            &mut groups,
            &mut metrics,
        )?;
        #[cfg(feature = "slow-trace")]
        {
            debug_assert!(
                metrics.profile.row_domain_fallback_scans <= 1,
                "one shard-local grouped request must perform at most one canonical row-domain fallback scan",
            );
            assert_grouped_raw_execution_accounting(&metrics);
        }
        if groups.is_empty() {
            return Ok(None);
        }
        metrics.fields_projected = request.targets.len() as u64;
        Ok(Some(GroupedAggregateResult {
            groups: groups
                .into_iter()
                .map(|(key, states)| GroupedAggregateGroup { key, states })
                .collect(),
            metrics,
        }))
    }

    fn grouped_row_domain_precheck_fields<T>(
        &self,
        series_keys: &[String],
        coverage: &GroupedRowDomainCoverage,
        request: &GroupedAggregateRequest,
        cache: &Arc<Cache>,
        readers: &Arc<AggregateReaderSet>,
        groups: &BTreeMap<GroupedAggregateKey, T>,
        allow_target_group_coverage_proof: bool,
    ) -> Result<GroupedRowDomainPrecheck> {
        if coverage.uncovered_fields.is_empty()
            || self.vlog_enabled
            || !allow_target_group_coverage_proof
            || groups.is_empty()
        {
            return Ok(GroupedRowDomainPrecheck {
                fields: coverage.uncovered_fields.clone(),
                elided_fields: 0,
            });
        }

        let mut target_windows =
            HashMap::<Vec<Option<String>>, Vec<i64>>::with_capacity(groups.len());
        for key in groups.keys() {
            target_windows
                .entry(key.tag_values.clone())
                .or_default()
                .push(key.window_start);
        }
        for windows in target_windows.values_mut() {
            windows.sort_unstable();
            windows.dedup();
        }

        let mut fields = Vec::new();
        let mut elided_fields = 0usize;
        'field: for (field_name, field_type) in &coverage.uncovered_fields {
            for series_key in series_keys {
                if request
                    .cancellation
                    .as_ref()
                    .is_some_and(crate::query::storage_ir::ScanCancellation::is_cancelled)
                {
                    return Err(Error::ScanCancelled);
                }

                let key = format!("{series_key}{KEY_FIELD_SEPARATOR}{field_name}");
                if cache
                    .values(key.as_bytes())
                    .iter()
                    .any(|value| request.time_range.contains(value.unix_nano()))
                {
                    fields.push((field_name.clone(), *field_type));
                    continue 'field;
                }

                let mut candidates = Vec::new();
                for reader in readers.as_slice() {
                    if reader.tombstone_range(key.as_bytes()).iter().any(|range| {
                        range.max >= request.time_range.start_time
                            && range.min <= request.time_range.end_time
                    }) {
                        fields.push((field_name.clone(), *field_type));
                        continue 'field;
                    }
                    for entry in reader.entries(key.as_bytes()) {
                        if entry.max_time < request.time_range.start_time
                            || entry.min_time > request.time_range.end_time
                        {
                            continue;
                        }
                        candidates.push(DirectTsmCandidate {
                            reader: Arc::clone(reader),
                            entry,
                        });
                    }
                }
                candidates.sort_unstable_by_key(|candidate| {
                    (
                        candidate.entry.min_time,
                        candidate.entry.max_time,
                        candidate.reader.generation,
                        candidate.reader.seq,
                    )
                });
                if direct_tsm_candidates_overlap(&candidates) {
                    fields.push((field_name.clone(), *field_type));
                    continue 'field;
                }

                let tag_values = Self::grouped_tag_values(series_key, &request.group_tags)?;
                let Some(windows) = target_windows.get(&tag_values) else {
                    if candidates.is_empty() {
                        continue;
                    }
                    fields.push((field_name.clone(), *field_type));
                    continue 'field;
                };
                for candidate in candidates {
                    let range = TimeRange::new(
                        candidate.entry.min_time.max(request.time_range.start_time),
                        candidate.entry.max_time.min(request.time_range.end_time),
                    );
                    if !Self::grouped_target_windows_cover_range(windows, request.window, range)? {
                        fields.push((field_name.clone(), *field_type));
                        continue 'field;
                    }
                }
            }
            elided_fields = elided_fields.saturating_add(1);
        }

        debug_assert_eq!(
            fields.len().saturating_add(elided_fields),
            coverage.uncovered_fields.len()
        );
        Ok(GroupedRowDomainPrecheck {
            fields,
            elided_fields,
        })
    }

    fn grouped_target_windows_cover_range(
        target_windows: &[i64],
        window: crate::query::storage_ir::GroupedAggregateWindow,
        range: TimeRange,
    ) -> Result<bool> {
        let first = window
            .bucket_start(range.start_time)
            .map_err(|reason| Error::StringErr {
                reason: reason.to_string(),
            })?;
        let last = window
            .bucket_start(range.end_time)
            .map_err(|reason| Error::StringErr {
                reason: reason.to_string(),
            })?;
        let Ok(first_index) = target_windows.binary_search(&first) else {
            return Ok(false);
        };
        let expected =
            (i128::from(last) - i128::from(first)).div_euclid(i128::from(window.duration_ns)) + 1;
        let Ok(expected) = usize::try_from(expected) else {
            return Ok(false);
        };
        let Some(last_index) = first_index
            .checked_add(expected)
            .and_then(|end| end.checked_sub(1))
        else {
            return Ok(false);
        };

        // Every target window uses the same fixed duration and origin. Exact
        // endpoints plus the exact number of unique aligned windows therefore
        // prove that no bucket inside this conservative entry range is absent.
        Ok(target_windows.get(last_index).copied() == Some(last))
    }

    fn ensure_grouped_multi_target_row_domain(
        &self,
        request: &GroupedAggregateRequest,
        coverage: &GroupedRowDomainCoverage,
        groups: &mut BTreeMap<GroupedAggregateKey, Vec<AggregateState>>,
        metrics: &mut GroupedAggregateMetrics,
    ) -> Result<()> {
        if coverage.uncovered_fields.is_empty() {
            #[cfg(feature = "slow-trace")]
            coverage.record(&mut metrics.profile, 0, 0);
            return Ok(());
        }

        let started = Instant::now();
        let scan = self.grouped_row_domain_scan(request, &coverage.uncovered_fields)?;
        let plan = Arc::new(EngineScanPlan {
            scan: Arc::new(scan.clone()),
            vlog_enabled: self.vlog_enabled,
        });
        let index_set =
            index::IndexSet::new(Arc::clone(&self.index), Arc::clone(&self.sfile), None);
        let tag_sets = index_set
            .tag_sets_for_storage_scan(Arc::clone(&self.sfile), &scan, plan.max_series_n())
            .map_err(|source| match source {
                index::Error::StorageScanCancelled => Error::ScanCancelled,
                source => Error::IndexSet { source },
            })?;
        let series_keys = tag_sets
            .iter()
            .flat_map(|tag_set| tag_set.series_keys.iter().cloned())
            .collect::<Vec<_>>();
        let has_residual_filter = tag_sets
            .iter()
            .flat_map(|tag_set| tag_set.storage_filters.iter())
            .any(Option::is_some);

        let (cache, file_store) = {
            let inner = self.inner.read();
            (Arc::clone(&inner.cache), Arc::clone(&inner.file_store))
        };
        let readers = Arc::new(AggregateReaderSet::new(file_store.readers()));
        #[cfg(feature = "slow-trace")]
        let row_domain_started = Instant::now();
        let precheck = self.grouped_row_domain_precheck_fields(
            &series_keys,
            coverage,
            request,
            &cache,
            &readers,
            groups,
            !has_residual_filter,
        )?;
        #[cfg(feature = "slow-trace")]
        coverage.record(
            &mut metrics.profile,
            precheck.fields.len(),
            precheck.elided_fields,
        );
        let fields = precheck.fields.as_slice();
        if fields.is_empty() {
            #[cfg(feature = "slow-trace")]
            {
                metrics.profile.row_domain_wall_us =
                    metrics.profile.row_domain_wall_us.saturating_add(
                        row_domain_started
                            .elapsed()
                            .as_micros()
                            .min(u128::from(u64::MAX)) as u64,
                    );
            }
            metrics.storage_scan_us = metrics
                .storage_scan_us
                .saturating_add(started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64);
            return Ok(());
        }
        #[cfg(feature = "slow-trace")]
        let precheck_profile = Arc::new(crate::query::storage_ir::StorageScanProfile::default());
        let row_request = RowCountRequest {
            measurement: request.measurement.clone(),
            time_range: request.time_range,
            cancellation: request.cancellation.clone(),
        };
        let mut row_metrics = RowCountMetrics::default();
        let mut direct = !self.vlog_enabled && !has_residual_filter;
        if direct {
            for series_key in &series_keys {
                let Some(scans) = self.row_count_direct_scans_for_series(
                    series_key,
                    fields,
                    &row_request,
                    &cache,
                    Arc::clone(&readers),
                    #[cfg(feature = "slow-trace")]
                    Some(&precheck_profile),
                    #[cfg(feature = "slow-trace")]
                    Some(&mut metrics.profile),
                )?
                else {
                    direct = false;
                    break;
                };
                #[cfg(feature = "slow-trace")]
                for (field_name, _) in fields {
                    metrics.profile.record_raw_interval(
                        format!("{series_key}{KEY_FIELD_SEPARATOR}{field_name}"),
                        request.time_range,
                        GroupedRawIntervalKind::RowDomainPrecheck,
                    );
                }
                let tag_values = Self::grouped_tag_values(series_key, &request.group_tags)?;
                // Materialize only final grouped keys; do not retain the per-row
                // timestamp domain after the direct proof consumes it.
                visit_direct_timestamp_union(scans, &mut row_metrics, |timestamp| {
                    let key = GroupedAggregateKey {
                        window_start: request.window.bucket_start(timestamp).map_err(|reason| {
                            Error::StringErr {
                                reason: reason.to_string(),
                            }
                        })?,
                        tag_values: tag_values.clone(),
                    };
                    groups.entry(key).or_insert_with(|| {
                        request
                            .targets
                            .iter()
                            .map(|target| AggregateState::empty(target.expected_field_type))
                            .collect()
                    });
                    Ok(())
                })?;
            }
        }
        #[cfg(feature = "slow-trace")]
        metrics.profile.merge_physical_scan(
            precheck_profile.snapshot(),
            GroupedPhysicalScanKind::RowDomainPrecheck,
        );

        if !direct {
            self.add_grouped_multi_row_domain_keys(
                request,
                fields,
                groups,
                &mut metrics.points_raw_decoded,
                #[cfg(feature = "slow-trace")]
                &series_keys,
                #[cfg(feature = "slow-trace")]
                Some(&mut metrics.profile),
            )?;
        }
        #[cfg(feature = "slow-trace")]
        {
            metrics.profile.row_domain_wall_us = metrics.profile.row_domain_wall_us.saturating_add(
                row_domain_started
                    .elapsed()
                    .as_micros()
                    .min(u128::from(u64::MAX)) as u64,
            );
        }
        metrics.storage_scan_us = metrics
            .storage_scan_us
            .saturating_add(started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64);
        Ok(())
    }

    fn add_grouped_multi_row_domain_keys(
        &self,
        request: &GroupedAggregateRequest,
        fields: &[(String, FieldType)],
        groups: &mut BTreeMap<GroupedAggregateKey, Vec<AggregateState>>,
        points_raw_decoded: &mut u64,
        #[cfg(feature = "slow-trace")] series_keys: &[String],
        #[cfg(feature = "slow-trace")] mut grouped_profile: Option<
            &mut GroupedAggregateProfileMetrics,
        >,
    ) -> Result<()> {
        let mut scan = self.grouped_row_domain_scan(request, fields)?;
        #[cfg(feature = "slow-trace")]
        let scan_profile = grouped_profile.as_ref().map(|_| {
            let profile = Arc::new(crate::query::storage_ir::StorageScanProfile::default());
            scan.execution.profile = Some(Arc::clone(&profile));
            profile
        });
        #[cfg(feature = "slow-trace")]
        if let Some(profile) = grouped_profile.as_deref_mut() {
            for series_key in series_keys {
                for (field_name, _) in fields {
                    profile.record_raw_interval(
                        format!("{series_key}{KEY_FIELD_SEPARATOR}{field_name}"),
                        request.time_range,
                        GroupedRawIntervalKind::RowDomainFallback,
                    );
                }
            }
        }
        #[cfg(feature = "slow-trace")]
        let points_before = *points_raw_decoded;
        if let Some(iterator) = self.create_storage_iterator(scan)? {
            query_iterator::fold_grouped_multi_row_keys(
                iterator,
                &request.targets,
                request.window,
                &request.group_tags,
                groups,
                points_raw_decoded,
            )
            .context(QueryIteratorSnafu)?;
        }
        #[cfg(feature = "slow-trace")]
        if let (Some(profile), Some(scan_profile)) = (grouped_profile.as_deref_mut(), scan_profile)
        {
            profile.row_domain_logical_rows_consumed = profile
                .row_domain_logical_rows_consumed
                .saturating_add((*points_raw_decoded).saturating_sub(points_before));
            profile.merge_physical_scan(
                scan_profile.snapshot(),
                GroupedPhysicalScanKind::RowDomainFallback,
            );
        }
        Ok(())
    }

    fn grouped_multi_field_states(
        &self,
        request: &GroupedAggregateRequest,
        fields: &[(String, FieldType)],
        started: Instant,
    ) -> Result<Option<GroupedAggregateResult>> {
        let mut scan = self.grouped_multi_field_storage_scan(request, fields)?;
        #[cfg(feature = "slow-trace")]
        let scan_profile = {
            let profile = Arc::new(crate::query::storage_ir::StorageScanProfile::default());
            scan.execution.profile = Some(Arc::clone(&profile));
            profile
        };
        let plan = Arc::new(EngineScanPlan {
            scan: Arc::new(scan.clone()),
            vlog_enabled: self.vlog_enabled,
        });
        let index_set =
            index::IndexSet::new(Arc::clone(&self.index), Arc::clone(&self.sfile), None);
        #[cfg(feature = "slow-trace")]
        let preflight_tsi_lookup_started = Instant::now();
        let tag_sets = index_set
            .tag_sets_for_storage_scan(Arc::clone(&self.sfile), &scan, plan.max_series_n())
            .map_err(|source| match source {
                index::Error::StorageScanCancelled => Error::ScanCancelled,
                source => Error::IndexSet { source },
            })?;
        #[cfg(feature = "slow-trace")]
        let preflight_tsi_lookup_us = preflight_tsi_lookup_started
            .elapsed()
            .as_micros()
            .min(u128::from(u64::MAX)) as u64;
        let series_matched = tag_sets
            .iter()
            .flat_map(|tag_set| tag_set.series_keys.iter())
            .collect::<HashSet<_>>()
            .len() as u64;
        let mut groups = BTreeMap::<GroupedAggregateKey, Vec<AggregateState>>::new();
        let mut metrics = GroupedAggregateMetrics {
            series_matched,
            fields_projected: request.targets.len() as u64,
            used_raw_fallback: true,
            ..Default::default()
        };
        #[cfg(feature = "slow-trace")]
        let iterator_construction_started = Instant::now();
        if let Some(iterator) = self.create_storage_iterator(scan)? {
            #[cfg(feature = "slow-trace")]
            {
                metrics.profile.iterator_construction_us = iterator_construction_started
                    .elapsed()
                    .as_micros()
                    .min(u128::from(u64::MAX))
                    as u64;
                metrics.profile = {
                    let mut profile = scan_profile.snapshot();
                    profile.shard_scans = 1;
                    profile.preflight_tsi_lookup_us = preflight_tsi_lookup_us;
                    profile.iterator_construction_us = metrics.profile.iterator_construction_us;
                    profile
                };
            }
            query_iterator::fold_grouped_multi_aggregate_iterator(
                iterator,
                &request.targets,
                request.window,
                &request.group_tags,
                &mut groups,
                &mut metrics.points_raw_decoded,
                #[cfg(feature = "slow-trace")]
                &mut metrics.profile,
            )
            .context(QueryIteratorSnafu)?;
            #[cfg(feature = "slow-trace")]
            {
                let mut physical = scan_profile.snapshot();
                physical.shard_scans = metrics.profile.shard_scans;
                physical.preflight_tsi_lookup_us = metrics.profile.preflight_tsi_lookup_us;
                physical.iterator_construction_us = metrics.profile.iterator_construction_us;
                physical.iterator_next_us = metrics.profile.iterator_next_us;
                physical.visibility_row_assembly_us = metrics
                    .profile
                    .iterator_next_us
                    .saturating_sub(physical.block_read_decode_us);
                physical.timestamp_domain_us = metrics.profile.timestamp_domain_us;
                physical.aggregate_update_us = metrics.profile.aggregate_update_us;
                physical.logical_rows = metrics.profile.logical_rows;
                physical.aggregate_updates = metrics.profile.aggregate_updates;
                physical.groups = metrics.profile.groups;
                metrics.profile = physical;
            }
        } else {
            #[cfg(feature = "slow-trace")]
            {
                metrics.profile = scan_profile.snapshot();
                metrics.profile.shard_scans = 1;
                metrics.profile.preflight_tsi_lookup_us = preflight_tsi_lookup_us;
                metrics.profile.iterator_construction_us = iterator_construction_started
                    .elapsed()
                    .as_micros()
                    .min(u128::from(u64::MAX))
                    as u64;
            }
        }
        metrics.storage_scan_us = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        if groups.is_empty() {
            return Ok(None);
        }
        #[cfg(feature = "slow-trace")]
        let finalization_started = Instant::now();
        let groups = groups
            .into_iter()
            .map(|(key, states)| GroupedAggregateGroup { key, states })
            .collect();
        #[cfg(feature = "slow-trace")]
        {
            metrics.profile.finalization_us = finalization_started
                .elapsed()
                .as_micros()
                .min(u128::from(u64::MAX)) as u64;
        }
        Ok(Some(GroupedAggregateResult { groups, metrics }))
    }

    fn grouped_missing_field_states(
        &self,
        request: &GroupedAggregateRequest,
        fields: &[(String, FieldType)],
        field_type: FieldType,
        started: Instant,
    ) -> Result<Option<GroupedAggregateResult>> {
        let mut groups = BTreeMap::<GroupedAggregateKey, AggregateState>::new();
        let mut metrics = GroupedAggregateMetrics {
            fields_projected: 1,
            ..Default::default()
        };
        #[cfg(feature = "slow-trace")]
        GroupedRowDomainCoverage::new(fields, &HashSet::new()).record(
            &mut metrics.profile,
            fields.len(),
            0,
        );
        // Row-domain decoding establishes group existence only. It must not be
        // reported as decoded target-field points because the target field is
        // absent from this shard.
        let mut row_domain_points = 0;
        self.add_grouped_row_domain_keys(
            request,
            fields,
            field_type,
            &mut groups,
            &mut row_domain_points,
            #[cfg(feature = "slow-trace")]
            &[],
            #[cfg(feature = "slow-trace")]
            Some(&mut metrics.profile),
        )?;
        if groups.is_empty() {
            return Ok(None);
        }
        metrics.storage_scan_us = started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        Ok(Some(GroupedAggregateResult {
            groups: groups
                .into_iter()
                .map(|(key, state)| GroupedAggregateGroup {
                    key,
                    states: vec![state],
                })
                .collect(),
            metrics,
        }))
    }

    fn ensure_grouped_row_domain(
        &self,
        series_keys: &[String],
        coverage: &GroupedRowDomainCoverage,
        field_type: FieldType,
        request: &GroupedAggregateRequest,
        cache: &Arc<Cache>,
        readers: Arc<AggregateReaderSet>,
        groups: &mut BTreeMap<GroupedAggregateKey, AggregateState>,
        metrics: &mut GroupedAggregateMetrics,
        allow_target_group_coverage_proof: bool,
    ) -> Result<()> {
        #[cfg(feature = "slow-trace")]
        let row_domain_started = Instant::now();
        let precheck = self.grouped_row_domain_precheck_fields(
            series_keys,
            coverage,
            request,
            cache,
            &readers,
            groups,
            allow_target_group_coverage_proof,
        )?;
        #[cfg(feature = "slow-trace")]
        coverage.record(
            &mut metrics.profile,
            precheck.fields.len(),
            precheck.elided_fields,
        );
        let fields = precheck.fields.as_slice();
        if fields.is_empty() {
            #[cfg(feature = "slow-trace")]
            {
                metrics.profile.row_domain_wall_us =
                    metrics.profile.row_domain_wall_us.saturating_add(
                        row_domain_started
                            .elapsed()
                            .as_micros()
                            .min(u128::from(u64::MAX)) as u64,
                    );
            }
            return Ok(());
        }
        #[cfg(feature = "slow-trace")]
        let precheck_profile = Arc::new(crate::query::storage_ir::StorageScanProfile::default());
        let row_request = RowCountRequest {
            measurement: request.measurement.clone(),
            time_range: request.time_range,
            cancellation: request.cancellation.clone(),
        };
        let mut row_metrics = RowCountMetrics::default();
        let mut direct = !self.vlog_enabled && allow_target_group_coverage_proof;
        if direct {
            for series_key in series_keys {
                let Some(scans) = self.row_count_direct_scans_for_series(
                    series_key,
                    fields,
                    &row_request,
                    cache,
                    Arc::clone(&readers),
                    #[cfg(feature = "slow-trace")]
                    Some(&precheck_profile),
                    #[cfg(feature = "slow-trace")]
                    Some(&mut metrics.profile),
                )?
                else {
                    direct = false;
                    break;
                };
                #[cfg(feature = "slow-trace")]
                for (field_name, _) in fields {
                    metrics.profile.record_raw_interval(
                        format!("{series_key}{KEY_FIELD_SEPARATOR}{field_name}"),
                        request.time_range,
                        GroupedRawIntervalKind::RowDomainPrecheck,
                    );
                }
                let tag_values = Self::grouped_tag_values(series_key, &request.group_tags)?;
                // Target execution already contributed every key from covered
                // fields. Merge exact keys from only the uncovered timestamp
                // union without retaining a per-point timestamp set.
                visit_direct_timestamp_union(scans, &mut row_metrics, |timestamp| {
                    let key = GroupedAggregateKey {
                        window_start: request.window.bucket_start(timestamp).map_err(|reason| {
                            Error::StringErr {
                                reason: reason.to_string(),
                            }
                        })?,
                        tag_values: tag_values.clone(),
                    };
                    groups
                        .entry(key)
                        .or_insert_with(|| AggregateState::empty(field_type));
                    Ok(())
                })?;
            }
        }
        #[cfg(feature = "slow-trace")]
        metrics.profile.merge_physical_scan(
            precheck_profile.snapshot(),
            GroupedPhysicalScanKind::RowDomainPrecheck,
        );

        if direct {
            #[cfg(feature = "slow-trace")]
            {
                metrics.profile.row_domain_wall_us =
                    metrics.profile.row_domain_wall_us.saturating_add(
                        row_domain_started
                            .elapsed()
                            .as_micros()
                            .min(u128::from(u64::MAX)) as u64,
                    );
            }
            return Ok(());
        }

        self.add_grouped_row_domain_keys(
            request,
            fields,
            field_type,
            groups,
            &mut metrics.points_raw_decoded,
            #[cfg(feature = "slow-trace")]
            series_keys,
            #[cfg(feature = "slow-trace")]
            Some(&mut metrics.profile),
        )?;
        #[cfg(feature = "slow-trace")]
        {
            metrics.profile.row_domain_wall_us = metrics.profile.row_domain_wall_us.saturating_add(
                row_domain_started
                    .elapsed()
                    .as_micros()
                    .min(u128::from(u64::MAX)) as u64,
            );
        }
        Ok(())
    }

    fn add_grouped_row_domain_keys(
        &self,
        request: &GroupedAggregateRequest,
        fields: &[(String, FieldType)],
        field_type: FieldType,
        groups: &mut BTreeMap<GroupedAggregateKey, AggregateState>,
        points_raw_decoded: &mut u64,
        #[cfg(feature = "slow-trace")] series_keys: &[String],
        #[cfg(feature = "slow-trace")] mut grouped_profile: Option<
            &mut GroupedAggregateProfileMetrics,
        >,
    ) -> Result<()> {
        let mut scan = self.grouped_row_domain_scan(request, fields)?;
        #[cfg(feature = "slow-trace")]
        let scan_profile = grouped_profile.as_ref().map(|_| {
            let profile = Arc::new(crate::query::storage_ir::StorageScanProfile::default());
            scan.execution.profile = Some(Arc::clone(&profile));
            profile
        });
        #[cfg(feature = "slow-trace")]
        if let Some(profile) = grouped_profile.as_deref_mut() {
            for series_key in series_keys {
                for (field_name, _) in fields {
                    profile.record_raw_interval(
                        format!("{series_key}{KEY_FIELD_SEPARATOR}{field_name}"),
                        request.time_range,
                        GroupedRawIntervalKind::RowDomainFallback,
                    );
                }
            }
        }
        #[cfg(feature = "slow-trace")]
        let points_before = *points_raw_decoded;
        if let Some(iterator) = self.create_storage_iterator(scan)? {
            query_iterator::fold_grouped_row_keys(
                iterator,
                field_type,
                request.window,
                &request.group_tags,
                groups,
                points_raw_decoded,
            )
            .context(QueryIteratorSnafu)?;
        }
        #[cfg(feature = "slow-trace")]
        if let (Some(profile), Some(scan_profile)) = (grouped_profile.as_deref_mut(), scan_profile)
        {
            profile.row_domain_logical_rows_consumed = profile
                .row_domain_logical_rows_consumed
                .saturating_add((*points_raw_decoded).saturating_sub(points_before));
            profile.merge_physical_scan(
                scan_profile.snapshot(),
                GroupedPhysicalScanKind::RowDomainFallback,
            );
        }
        Ok(())
    }

    fn grouped_aggregate_raw_states(
        &self,
        mut scan: StorageScan,
        field_type: FieldType,
        request: &GroupedAggregateRequest,
        groups: &mut BTreeMap<GroupedAggregateKey, AggregateState>,
        metrics: &mut GroupedAggregateMetrics,
        #[cfg(feature = "slow-trace")] scan_kind: GroupedPhysicalScanKind,
    ) -> Result<()> {
        #[cfg(feature = "slow-trace")]
        let scan_profile = {
            let profile = Arc::new(crate::query::storage_ir::StorageScanProfile::default());
            scan.execution.profile = Some(Arc::clone(&profile));
            profile
        };
        #[cfg(feature = "slow-trace")]
        let raw_started = Instant::now();
        #[cfg(feature = "slow-trace")]
        let points_before = metrics.points_raw_decoded;
        if let Some(iterator) = self.create_storage_iterator(scan)? {
            query_iterator::fold_grouped_aggregate_iterator(
                iterator,
                field_type,
                request.window,
                &request.group_tags,
                groups,
                &mut metrics.points_raw_decoded,
            )
            .context(QueryIteratorSnafu)?;
        }
        #[cfg(feature = "slow-trace")]
        {
            metrics.profile.target_values_consumed = metrics
                .profile
                .target_values_consumed
                .saturating_add(metrics.points_raw_decoded.saturating_sub(points_before));
            metrics.profile.target_values_surviving_query_range = metrics
                .profile
                .target_values_surviving_query_range
                .saturating_add(metrics.points_raw_decoded.saturating_sub(points_before));
            metrics.profile.raw_iterator_wall_us = metrics
                .profile
                .raw_iterator_wall_us
                .saturating_add(raw_started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64);
            metrics
                .profile
                .merge_physical_scan(scan_profile.snapshot(), scan_kind);
        }
        Ok(())
    }

    fn grouped_row_domain_scan(
        &self,
        request: &GroupedAggregateRequest,
        fields: &[(String, FieldType)],
    ) -> Result<StorageScan> {
        let mut identity_tag_keys = self
            .measurement_tag_keys(request.measurement.name.as_bytes())?
            .into_iter()
            .collect::<Vec<_>>();
        identity_tag_keys.sort();
        Ok(StorageScan {
            measurement: request.measurement.clone(),
            projection: StorageProjection {
                primary_column: None,
                returned_columns: vec![],
                row_domain_columns: fields
                    .iter()
                    .map(|(name, field_type)| {
                        ColumnRef::new(name, ColumnKind::Field(Some(*field_type)))
                    })
                    .collect(),
                series: SeriesProjection::new(identity_tag_keys, request.group_tags.clone()),
            },
            time_range: request.time_range,
            predicate: Self::grouped_tag_predicate(&request.tag_filters),
            time_order: TimeOrder::Ascending,
            output_order: OutputOrder::TimeOrdered,
            execution: StorageExecutionOptions::default(),
            cancellation: request.cancellation.clone(),
        })
    }

    fn grouped_aggregate_storage_scan(
        &self,
        request: &GroupedAggregateRequest,
        target: &GroupedAggregateTarget,
        field_type: FieldType,
    ) -> Result<StorageScan> {
        let mut identity_tag_keys = self
            .measurement_tag_keys(request.measurement.name.as_bytes())?
            .into_iter()
            .collect::<Vec<_>>();
        identity_tag_keys.sort();
        Ok(Self::grouped_aggregate_storage_scan_from_tag_keys(
            request,
            target,
            field_type,
            identity_tag_keys,
        ))
    }

    #[cfg(feature = "slow-trace")]
    fn grouped_aggregate_storage_scan_profiled(
        &self,
        request: &GroupedAggregateRequest,
        target: &GroupedAggregateTarget,
        field_type: FieldType,
        diagnostics: &Arc<index::MeasurementTagKeysDiagnostics>,
    ) -> Result<StorageScan> {
        let started = Instant::now();
        let identity_tag_keys =
            self.measurement_tag_keys_profiled(request.measurement.name.as_bytes(), diagnostics)?;
        let mut identity_tag_keys = identity_tag_keys.into_iter().collect::<Vec<_>>();
        identity_tag_keys.sort();
        diagnostics.record_wall(started);
        Ok(Self::grouped_aggregate_storage_scan_from_tag_keys(
            request,
            target,
            field_type,
            identity_tag_keys,
        ))
    }

    fn grouped_aggregate_storage_scan_from_tag_keys(
        request: &GroupedAggregateRequest,
        target: &GroupedAggregateTarget,
        field_type: FieldType,
        identity_tag_keys: Vec<String>,
    ) -> StorageScan {
        let primary = ColumnRef::new(&target.field, ColumnKind::Field(Some(field_type)));
        StorageScan {
            measurement: MeasurementRef::new(&request.measurement.name),
            projection: StorageProjection {
                primary_column: Some(primary.clone()),
                returned_columns: vec![primary],
                row_domain_columns: vec![],
                series: SeriesProjection::new(identity_tag_keys, request.group_tags.clone()),
            },
            time_range: request.time_range,
            predicate: Self::grouped_tag_predicate(&request.tag_filters),
            time_order: TimeOrder::Ascending,
            output_order: OutputOrder::TimeOrdered,
            execution: StorageExecutionOptions::default(),
            cancellation: request.cancellation.clone(),
        }
    }

    fn grouped_multi_field_storage_scan(
        &self,
        request: &GroupedAggregateRequest,
        fields: &[(String, FieldType)],
    ) -> Result<StorageScan> {
        let mut identity_tag_keys = self
            .measurement_tag_keys(request.measurement.name.as_bytes())?
            .into_iter()
            .collect::<Vec<_>>();
        identity_tag_keys.sort();
        let target_names = request
            .targets
            .iter()
            .map(|target| target.field.as_str())
            .collect::<HashSet<_>>();
        Ok(StorageScan {
            measurement: request.measurement.clone(),
            projection: StorageProjection {
                primary_column: None,
                returned_columns: request
                    .targets
                    .iter()
                    .map(|target| {
                        ColumnRef::new(
                            &target.field,
                            ColumnKind::Field(Some(target.expected_field_type)),
                        )
                    })
                    .collect(),
                row_domain_columns: fields
                    .iter()
                    .filter(|(name, _)| !target_names.contains(name.as_str()))
                    .map(|(name, field_type)| {
                        ColumnRef::new(name, ColumnKind::Field(Some(*field_type)))
                    })
                    .collect(),
                series: SeriesProjection::new(identity_tag_keys, request.group_tags.clone()),
            },
            time_range: request.time_range,
            predicate: Self::grouped_tag_predicate(&request.tag_filters),
            time_order: TimeOrder::Ascending,
            output_order: OutputOrder::TimeOrdered,
            execution: StorageExecutionOptions::default(),
            cancellation: request.cancellation.clone(),
        })
    }

    fn grouped_tag_predicate(filters: &[GroupedTagFilter]) -> Option<StoragePredicate> {
        filters.iter().cloned().fold(None, |predicate, filter| {
            let filter = StoragePredicate::ExactTagSet {
                tag: ColumnRef::new(filter.tag, ColumnKind::Tag),
                accepted_values: filter.accepted_values,
            };
            Some(match predicate {
                None => filter,
                Some(predicate) => StoragePredicate::Logical {
                    op: LogicalOp::And,
                    lhs: Box::new(predicate),
                    rhs: Box::new(filter),
                },
            })
        })
    }

    fn grouped_tag_values(series_key: &str, group_tags: &[String]) -> Result<Vec<Option<String>>> {
        let (_, tags) = crate::models::points::parse_key(series_key.as_bytes());
        group_tags
            .iter()
            .map(|group_tag| {
                tags.iter()
                    .find(|tag| tag.key == group_tag.as_bytes())
                    .map(|tag| {
                        std::str::from_utf8(&tag.value)
                            .map(str::to_owned)
                            .map_err(|source| Error::StringErr {
                                reason: format!(
                                    "group tag {group_tag} is not valid UTF-8: {source}"
                                ),
                            })
                    })
                    .transpose()
            })
            .collect()
    }

    fn aggregate_field(&self, request: &AggregateRequest) -> Result<Option<crate::series::measurement_fields::Field>> {
        if request.aggregates.is_empty()
            || request.time_range.start_time > request.time_range.end_time
        {
            return Err(Error::StringErr {
                reason: "aggregate request requires an operation and a valid time range"
                    .to_string(),
            });
        }

        let Some(fields) = self.fieldset.fields_by_string(&request.measurement.name) else {
            return Ok(None);
        };
        let Some(field) = fields.field(&request.field) else {
            return Ok(None);
        };
        if !matches!(
            field.typ,
            crate::models::INTEGER
                | crate::models::UNSIGNED
                | crate::models::FLOAT
                | crate::models::BOOL
                | crate::models::STRING
        ) {
            return Err(Error::StringErr {
                reason: format!("unsupported aggregate field type {}", field.typ),
            });
        }
        crate::query::storage_ir::AggregateProjection::from_aggregates(&request.aggregates)
            .validate_field_type(field.typ)
            .map_err(|reason| Error::StringErr { reason: reason.into() })?;
        Ok(Some(field))
    }

    pub(crate) fn aggregate_scan(
        &self,
        request: AggregateRequest,
    ) -> Result<Option<AggregateResult>> {
        if let Some(scan) = self.prepare_aggregate_complete_scan(request.clone())? {
            return scan.run_complete().map(Some);
        }
        let Some(field) = self.aggregate_field(&request)? else { return Ok(None); };
        let track_endpoints = request
            .aggregates
            .iter()
            .any(|aggregate| matches!(aggregate, AggregateKind::First | AggregateKind::Last));

        let scan = self.aggregate_storage_scan(&request, field.typ)?;
        let plan = Arc::new(EngineScanPlan {
            scan: Arc::new(scan.clone()),
            vlog_enabled: self.vlog_enabled,
        });
        let index_set =
            index::IndexSet::new(Arc::clone(&self.index), Arc::clone(&self.sfile), None);
        let tag_sets = index_set
            .tag_sets_for_storage_scan(Arc::clone(&self.sfile), &scan, plan.max_series_n())
            .map_err(|source| match source {
                index::Error::StorageScanCancelled => Error::ScanCancelled,
                source => Error::IndexSet { source },
            })?;

        let (cache, file_store) = {
            let inner = self.inner.read();
            (Arc::clone(&inner.cache), Arc::clone(&inner.file_store))
        };
        let readers = AggregateReaderSet::new(file_store.readers());
        let mut metrics = AggregateScanMetrics::default();
        let mut state = AggregateState::empty(field.typ);
        let mut whole_request_fallback = self.vlog_enabled;
        let mut candidates = Vec::new();
        let mut conflict_intervals = Vec::new();

        if !whole_request_fallback {
            'series: for series_key in tag_sets
                .iter()
                .flat_map(|tag_set| tag_set.series_keys.iter())
            {
                let key = format!("{series_key}{KEY_FIELD_SEPARATOR}{}", request.field);
                if cache
                    .values(key.as_bytes())
                    .iter()
                    .any(|value| request.time_range.contains(value.unix_nano()))
                {
                    whole_request_fallback = true;
                    break;
                }

                let mut series_candidates = Vec::new();
                for reader in readers.as_slice() {
                    let tombstones = reader.tombstone_range(key.as_bytes());
                    for entry in reader.entries(key.as_bytes()) {
                        if entry.max_time < request.time_range.start_time
                            || entry.min_time > request.time_range.end_time
                        {
                            continue;
                        }
                        if tombstones
                            .iter()
                            .any(|range| range.max >= entry.min_time && range.min <= entry.max_time)
                        {
                            whole_request_fallback = true;
                            break 'series;
                        }
                        series_candidates.push(AggregateCandidate {
                            reader: Arc::clone(reader),
                            entry,
                        });
                    }
                }
                conflict_intervals.extend(aggregate_conflict_intervals(
                    &series_candidates,
                    request.time_range,
                ));
                candidates.extend(series_candidates);
            }
        }

        if whole_request_fallback {
            metrics.used_fallback = true;
            state = self.aggregate_raw_state(
                scan,
                AggregateState::empty(field.typ),
                &mut metrics,
                track_endpoints,
            )?;
            return Ok(Some(AggregateResult { state, metrics }));
        }

        let conflict_intervals = merge_aggregate_intervals(conflict_intervals);
        metrics.candidate_blocks = candidates.len() as u64;
        metrics.overlap_intervals = conflict_intervals.len() as u64;
        metrics.overlap_raw_blocks = conflict_intervals.iter().fold(0_u64, |count, interval| {
            count.saturating_add(
                candidates
                    .iter()
                    .filter(|candidate| {
                        candidate.entry.min_time <= interval.end_time
                            && interval.start_time <= candidate.entry.max_time
                    })
                    .count() as u64,
            )
        });

        for candidate in candidates {
            let entry_range = TimeRange::new(
                candidate.entry.min_time.max(request.time_range.start_time),
                candidate.entry.max_time.min(request.time_range.end_time),
            );
            let intersects_conflict =
                aggregate_intervals_intersect(&conflict_intervals, entry_range);
            let fully_covered = request.time_range.start_time <= candidate.entry.min_time
                && request.time_range.end_time >= candidate.entry.max_time;
            if fully_covered
                && !intersects_conflict
                && let Some(partial) =
                    Self::aggregate_state_from_entry(field.typ, &candidate.entry, track_endpoints)?
            {
                state
                    .merge(partial)
                    .map_err(|reason| Error::StringErr {
                        reason: reason.to_string(),
                    })?;
                metrics.statistics_blocks += 1;
                continue;
            }

            if aggregate_intervals_cover(&conflict_intervals, entry_range) {
                continue;
            }

            metrics.decoded_blocks += 1;
            let include_timestamp = |timestamp| {
                request.time_range.contains(timestamp)
                    && !aggregate_intervals_contain(&conflict_intervals, timestamp)
            };
            match field.typ {
                crate::models::INTEGER => {
                    for value in candidate
                        .reader
                        .read_integer_block_at(&candidate.entry)
                        .context(TSMReaderSnafu)?
                    {
                        if include_timestamp(value.unix_nano) {
                            if track_endpoints {
                                state.add_integer_at(value.unix_nano, value.value);
                            } else {
                                state.add_integer(value.value);
                            }
                        }
                    }
                }
                crate::models::UNSIGNED => {
                    for value in candidate
                        .reader
                        .read_unsigned_block_at(&candidate.entry)
                        .context(TSMReaderSnafu)?
                    {
                        if include_timestamp(value.unix_nano) {
                            if track_endpoints {
                                state.add_unsigned_at(value.unix_nano, value.value);
                            } else {
                                state.add_unsigned(value.value);
                            }
                        }
                    }
                }
                crate::models::FLOAT => {
                    for value in candidate
                        .reader
                        .read_float_block_at(&candidate.entry)
                        .context(TSMReaderSnafu)?
                    {
                        if include_timestamp(value.unix_nano) {
                            if track_endpoints {
                                state.add_float_at(value.unix_nano, value.value);
                            } else {
                                state.add_float(value.value);
                            }
                        }
                    }
                }
                crate::models::BOOL => {
                    for value in candidate
                        .reader
                        .read_bool_block_at(&candidate.entry)
                        .context(TSMReaderSnafu)?
                        .into_iter()
                        .filter(|value| include_timestamp(value.unix_nano))
                    {
                        if track_endpoints {
                            state.add_boolean_at(value.unix_nano, value.value);
                        } else {
                            state.add_count(1);
                        }
                    }
                }
                crate::models::STRING => {
                    for value in candidate
                        .reader
                        .read_string_block_at(&candidate.entry)
                        .context(TSMReaderSnafu)?
                        .into_iter()
                        .filter(|value| include_timestamp(value.unix_nano))
                    {
                        if track_endpoints {
                            state.add_string_at(value.unix_nano, value.value);
                        } else {
                            state.add_count(1);
                        }
                    }
                }
                _ => unreachable!(),
            }
        }

        for interval in conflict_intervals {
            let mut overlap_scan = scan.clone();
            overlap_scan.time_range = interval;
            let overlap_state = self.aggregate_raw_state(
                overlap_scan,
                AggregateState::empty(field.typ),
                &mut metrics,
                track_endpoints,
            )?;
            state
                .merge(overlap_state)
                .map_err(|reason| Error::StringErr {
                    reason: reason.to_string(),
                })?;
        }

        Ok(Some(AggregateResult { state, metrics }))
    }

    fn aggregate_raw_state(
        &self,
        scan: StorageScan,
        mut state: AggregateState,
        metrics: &mut AggregateScanMetrics,
        track_endpoints: bool,
    ) -> Result<AggregateState> {
        metrics.raw_scans = metrics.raw_scans.saturating_add(1);
        if let Some(iterator) = self.create_storage_iterator(scan)? {
            state = query_iterator::fold_aggregate_iterator(iterator, state, track_endpoints)
                .context(QueryIteratorSnafu)?;
        }
        Ok(state)
    }

    fn aggregate_storage_scan(
        &self,
        request: &AggregateRequest,
        field_type: FieldType,
    ) -> Result<StorageScan> {
        let mut identity_tag_keys = self
            .measurement_tag_keys(request.measurement.name.as_bytes())?
            .into_iter()
            .collect::<Vec<_>>();
        identity_tag_keys.sort();
        let primary = ColumnRef::new(&request.field, ColumnKind::Field(Some(field_type)));
        Ok(StorageScan {
            measurement: MeasurementRef::new(&request.measurement.name),
            projection: StorageProjection {
                primary_column: Some(primary.clone()),
                returned_columns: vec![primary],
                row_domain_columns: vec![],
                series: SeriesProjection::new(identity_tag_keys, vec![]),
            },
            time_range: request.time_range,
            predicate: None,
            time_order: TimeOrder::Ascending,
            output_order: OutputOrder::TimeOrdered,
            execution: StorageExecutionOptions::default(),
            cancellation: None,
        })
    }

    /// Converts usable legacy summaries once, leaving visibility and window checks to callers.
    fn aggregate_state_from_entry(
        field_type: FieldType,
        entry: &IndexEntry,
        track_endpoints: bool,
    ) -> Result<Option<AggregateState>> {
        let Some(statistics) = entry.statistics else {
            return Ok(None);
        };
        if track_endpoints && !statistics.has_first_last() {
            return Ok(None);
        }
        let state = Self::aggregate_state_from_statistics(
            field_type,
            statistics,
            entry.min_time,
            entry.max_time,
            track_endpoints,
        )?;
        // A wrapped integer sum can be used only when its exact sum is recoverable.
        // Otherwise every caller must decode raw values, including non-AVG requests.
        if matches!(field_type, INTEGER | UNSIGNED) && state.wide_sum.is_none() {
            return Ok(None);
        }
        Ok(Some(state))
    }

    fn aggregate_state_from_statistics(
        field_type: FieldType,
        statistics: BlockStatistics,
        min_time: i64,
        max_time: i64,
        track_endpoints: bool,
    ) -> Result<AggregateState> {
        if !statistics.supports_block_type(field_type) {
            return Err(Error::StringErr {
                reason: format!("block statistics do not match field type {field_type}"),
            });
        }
        if track_endpoints && !statistics.has_first_last() {
            return Err(Error::StringErr {
                reason: "block statistics do not include endpoints".to_string(),
            });
        }
        let (values, first_value, last_value) = match statistics.value {
            BlockStatisticValue::Integer {
                sum,
                min,
                max,
                first,
                last,
            } => (
                AggregateValues::Integer { sum, min, max },
                Some(AggregatePointValue::Integer(first)),
                Some(AggregatePointValue::Integer(last)),
            ),
            BlockStatisticValue::Unsigned {
                sum,
                min,
                max,
                first,
                last,
            } => (
                AggregateValues::Unsigned { sum, min, max },
                Some(AggregatePointValue::Unsigned(first)),
                Some(AggregatePointValue::Unsigned(last)),
            ),
            BlockStatisticValue::Float {
                sum,
                min,
                max,
                first,
                last,
            } => (
                AggregateValues::Float { sum, min, max },
                Some(AggregatePointValue::Float(first)),
                Some(AggregatePointValue::Float(last)),
            ),
            BlockStatisticValue::Boolean { first, last } => (
                AggregateValues::CountOnly,
                Some(AggregatePointValue::Boolean(first)),
                Some(AggregatePointValue::Boolean(last)),
            ),
            BlockStatisticValue::Count => (AggregateValues::CountOnly, None, None),
        };
        let state = AggregateState {
            field_type,
            count: statistics.count,
            values: Some(values),
            wide_sum: crate::query::storage_ir::AggregateWideSum::from_legacy(values, statistics.count),
            first: track_endpoints.then(|| AggregatePoint {
                time: min_time,
                value: first_value.expect("endpoint-capable statistics"),
            }),
            last: track_endpoints.then(|| AggregatePoint {
                time: max_time,
                value: last_value.expect("endpoint-capable statistics"),
            }),
        };
        Ok(state)
    }

    fn create_iterator_from_scan(
        &self,
        plan: Arc<EngineScanPlan>,
    ) -> Result<Option<Box<dyn QueryIterator + Sync + Send>>> {
        let scan = Arc::clone(&plan.scan);
        #[cfg(feature = "slow-trace")]
        let profile = scan.execution.profile.clone();
        #[cfg(feature = "slow-trace")]
        if let Some(profile) = &profile {
            profile.record_shard_scan();
        }

        // Check measurement exists.
        #[cfg(feature = "slow-trace")]
        let measurement_lookup_started = Instant::now();
        let measurement_exists = self
            .index
            .measurement_exists(scan.measurement.name.as_bytes())
            .context(IndexSnafu)?;
        #[cfg(feature = "slow-trace")]
        if let Some(profile) = &profile {
            profile.record_measurement_lookup(
                measurement_lookup_started
                    .elapsed()
                    .as_micros()
                    .min(u128::from(u64::MAX)) as u64,
            );
        }
        if !measurement_exists {
            return Ok(None);
        }

        let index_set =
            index::IndexSet::new(Arc::clone(&self.index), Arc::clone(&self.sfile), None);
        #[cfg(feature = "slow-trace")]
        let tsi_lookup_started = Instant::now();
        let mut tag_sets = index_set
            .tag_sets_for_storage_scan(Arc::clone(&self.sfile), &scan, plan.max_series_n())
            .map_err(|source| match source {
                index::Error::StorageScanCancelled => Error::ScanCancelled,
                source => Error::IndexSet { source },
            })?;
        #[cfg(feature = "slow-trace")]
        if let Some(profile) = &profile {
            profile.record_tsi_lookup(
                tsi_lookup_started
                    .elapsed()
                    .as_micros()
                    .min(u128::from(u64::MAX)) as u64,
            );
        }
        #[cfg(feature = "slow-trace")]
        let iterator_setup_started = Instant::now();

        // Reverse the tag sets if we are ordering by descending.
        if !scan.ascending() {
            for t in tag_sets.iter_mut() {
                t.reverse();
            }
        }

        let (cache, fieldset, file_store) = {
            let inner = self.inner.read();
            (
                Arc::clone(&inner.cache),
                Arc::clone(&self.fieldset),
                Arc::clone(&inner.file_store),
                // inner.vlog_index_cache.clone(),
                // inner.vlog_file_store.clone(),
            )
        };

        // FIXME:
        // ref, _ := opt.Expr.(*VarRef)
        // Expr:       query IR VarRef {val: "value", typ: UNKNOWN}

        // Calculate tag sets and apply SLIMIT/SOFFSET.
        let tag_sets = plan.limit_tag_sets(tag_sets);
        #[cfg(feature = "slow-trace")]
        if let Some(profile) = &profile {
            let series_count = tag_sets
                .iter()
                .map(|tag_set| tag_set.series_keys.len() as u64)
                .sum();
            profile.record_series_iterators(series_count);
            profile.record_generic_fallback_series(series_count);
        }

        // FIXME:
        // ///////////////////////////////////////////////////
        // //Add series keys to lru_series_keys in index  ///
        // Look up fields for measurement.
        // let mf = fieldset.fields_by_string(measurement).unwrap();
        // // Find individual field.
        // let mut series_keys_with_field_key = vec![];
        // if let Some(vr) = &var_ref {
        //     if let Some(f) = mf.field(&vr.name) {
        //         for ts in tag_sets.iter() {
        //             for series_key in ts.series_keys.iter() {
        //                 let skf = series_key.clone() + "#!~#" + &f.name;
        //                 series_keys_with_field_key.push(skf);
        //             }
        //         }
        //     }
        // }

        // Add series keys to lru_series_keys in index.
        // self.index
        //     .add_lru_series_keys(series_keys_with_field_key.as_slice());
        // ///////////////////////////////////////////////////
        // ///////////////////////////////////////////////////

        let mut itrs = vec![];
        if let Err(e) = (|| {
            for t in tag_sets.iter() {
                let cache = Arc::clone(&cache);
                let fieldset = Arc::clone(&fieldset);
                let file_store = Arc::clone(&file_store);
                // [Integer/Unsigned/Bool/Float/String]Iter
                let mut inputs = self.create_tag_set_iters(
                    &plan, cache, fieldset, file_store,
                    // vlog_index_cache,
                    // vlog_file_store,
                    t,
                )?;
                if inputs.is_empty() {
                    continue;
                }

                // FIXME:
                // If we have a LIMIT or OFFSET and the grouping of the outer query
                // is different than the current grouping, we need to perform the
                // limit on each of the individual series keys instead to improve
                // performance.
                // if (opt.limit > 0 || opt.offset > 0) && len(opt.Dimensions) != len(opt.GroupBy) {

                inputs = plan.apply_pre_merge_limit(inputs);

                // inputs are:
                // [Integer/Unsigned/Float/Bool/String]Iter or
                // [Integer/Unsigned/Float/Bool/String]LimitIter
                let itr = plan.merge(inputs, Arc::clone(&scan)).map_err(|reason| {
                    Error::QueryIterator {
                        source: query_iterator::Error::StringErr { reason },
                    }
                })?;
                if let Some(itr) = itr {
                    // Apply a limit on the merged iterator.
                    let itr = plan.apply_post_merge_limit(itr);
                    // itr:
                    // [Integer/Unsigned/Float/Bool/String]SortedMergeIter or
                    // [Integer/Unsigned/Float/Bool/String]MergeIter or
                    // [Integer/Unsigned/Float/Bool/String]InterruptIter
                    // [Integer/Unsigned/Float/Bool/String]LimitIter
                    itrs.push(itr);
                }
            }
            Ok(())
        })() {
            query_iterator::Iterators(itrs).close();
            return Err(e);
        };

        // Ok(itrs)

        let merged = plan.merge(itrs, scan);
        #[cfg(feature = "slow-trace")]
        if let Some(profile) = &profile {
            profile.record_iterator_setup(
                iterator_setup_started
                    .elapsed()
                    .as_micros()
                    .min(u128::from(u64::MAX)) as u64,
            );
        }
        merged.map_err(|reason| Error::QueryIterator {
            source: query_iterator::Error::StringErr { reason },
        })
    }

    /// Creates a set of iterators for a tagset.
    fn create_tag_set_iters(
        &self,
        plan: &Arc<EngineScanPlan>,
        cache: Arc<Cache>,
        fieldset: Arc<MeasurementFieldSet>,
        file_store: Arc<FileStore>,
        // vlog_index_cache: Option<Arc<IndexCache>>,
        // vlog_file_store: Option<Arc<FileStore>>,
        t: &index::TagSet,
    ) -> Result<Vec<Box<dyn QueryIterator + Sync + Send>>> {
        // Set parallelism by number of logical cpus.
        let mut parallelism = num_cpus::get();
        if parallelism > t.series_keys.len() {
            parallelism = t.series_keys.len();
        }

        // Create series key groupings w/ return error.
        #[derive(Default)]
        struct Group {
            keys: Vec<String>,
            filters: Vec<Option<StoragePredicate>>,
            itrs: Vec<Box<dyn QueryIterator + Sync + Send>>,
            err: Option<Error>,
        }

        let mut groups = vec![];
        // Group series keys.
        let n = t.series_keys.len() / parallelism;
        for i in 0..parallelism {
            if i < parallelism - 1 {
                groups.push(Arc::new(Mutex::new(Group {
                    keys: t.series_keys[i * n..(i + 1) * n].to_vec(),
                    filters: t.storage_filters[i * n..(i + 1) * n].to_vec(),
                    itrs: vec![],
                    err: None,
                })));
            } else {
                groups.push(Arc::new(Mutex::new(Group {
                    keys: t.series_keys[i * n..].to_vec(),
                    filters: t.storage_filters[i * n..].to_vec(),
                    itrs: vec![],
                    err: None,
                })));
            }
        }
        #[cfg(feature = "slow-trace")]
        if let Some(profile) = &plan.scan.execution.profile {
            profile.record_iterator_tasks(groups.len() as u64);
        }

        let vlog_inner = self.inner.read().vlog_inner.clone();
        // Read series groups in parallel.
        let wg = WaitGroup::new();
        for group in groups.iter() {
            let cache = Arc::clone(&cache);
            let fieldset = Arc::clone(&fieldset);
            let file_store = Arc::clone(&file_store);
            let vlog_inner = vlog_inner.clone();

            let plan = Arc::clone(plan);
            let group = Arc::clone(group);
            let wg = wg.clone();
            wg.add(1);
            let rejected_group = Arc::clone(&group);
            let rejected_wg = wg.clone();
            let cancelled_group = Arc::clone(&group);
            let cancelled_wg = wg.clone();
            let (registration, completion) = accepted_task_guard(move || {
                cancelled_group.lock().err = Some(Error::TaskTerminated);
                cancelled_wg.done();
            });
            match self.executor.spawn_blocking(move || {
                {
                    let mut group = group.lock();
                    match Self::create_tag_set_group_iters(
                        vlog_inner,
                        cache,
                        fieldset,
                        file_store,
                        // vlog_index_cache,
                        // vlog_file_store,
                        plan,
                        &group.keys,
                        &group.filters,
                    ) {
                        Ok(itrs) => {
                            group.itrs = itrs;
                        }
                        Err(e) => {
                            group.err = Some(e);
                        }
                    }
                }

                drop(group); // must drop before done to ensure that all tasks release Arc before wg.done (notify)
                completion.complete_with(|| wg.done());
            }) {
                Ok(_) => registration.accepted(),
                Err(source) => {
                    rejected_group.lock().err = Some(Error::Execution { source });
                    rejected_wg.done();
                    registration.rejected();
                }
            }
        }
        wg.wait();

        // Determine total number of iterators so we can allocate only once.
        let mut itr_n = 0;
        for group in groups.iter() {
            itr_n += group.lock().itrs.len();
        }

        // Combine all iterators together and check for errors.
        let mut err: Option<Error> = None;
        let mut itrs = Vec::with_capacity(itr_n);
        for group in groups {
            let mut group = Arc::into_inner(group).unwrap().into_inner();
            if let Some(e) = group.err {
                err = Some(e);
            } else {
                // IntegerIter/UnsignedIter/FloatIter/BoolIter/StringIter
                itrs.append(&mut group.itrs);
            }
        }

        // If an error occurred, make sure we close all created iterators.
        if err.is_some() {
            for mut itr in itrs {
                itr.close();
            }
            return Err(err.unwrap());
        }

        Ok(itrs)
    }

    /// Creates a set of iterators for a subset of a tagset's series.
    fn create_tag_set_group_iters(
        vlog_inner: Option<Arc<RwLock<VLogInner>>>,
        cache: Arc<Cache>,
        fieldset: Arc<MeasurementFieldSet>,
        file_store: Arc<FileStore>,
        // vlog_index_cache: Option<Arc<IndexCache>>,
        // vlog_file_store: Option<Arc<FileStore>>,
        plan: Arc<EngineScanPlan>,
        series_keys: &[String],
        filters: &[Option<StoragePredicate>],
    ) -> Result<Vec<Box<dyn QueryIterator + Sync + Send>>> {
        let mut itrs = vec![];
        for (series_key, filter) in series_keys.iter().zip(filters) {
            // IntegerIter/UnsignedIter/FloatIter/BoolIter/StringIter
            let itr = Self::create_var_ref_series_iter(
                vlog_inner.clone(),
                &cache,
                &fieldset,
                &file_store,
                // &vlog_index_cache,
                // &vlog_file_store,
                &series_key,
                filter.clone(),
                &plan,
            )?;
            if itr.is_none() {
                continue;
            }

            itrs.push(itr.unwrap());

            // Abort native iterator creation if the translated scan was cancelled.
            if let Some(interrupt) = &plan.scan.cancellation {
                if interrupt.is_cancelled() {
                    query_iterator::Iterators(itrs).close();
                    return Err(Error::ScanCancelled);
                }
            }

            // Enforce series limit at creation time.
            if plan.max_series_n() > 0 && itrs.len() as i64 > plan.max_series_n() {
                let itrs_len = itrs.len();
                query_iterator::Iterators(itrs).close();
                return Err(Error::StringErr {
                    reason: format!(
                        "max-select-series limit exceeded: ({}/{})",
                        itrs_len,
                        plan.max_series_n()
                    ),
                });
            }
        }

        Ok(itrs)
    }

    /// Creates an iterator for a variable reference for a series.
    // fn create_var_ref_series_iter(cache: &Arc<Cache>, fieldset: &Arc<MeasurementFieldSet>, file_store: &Arc<FileStore>, var_ref: &Option<query_iterator::VarRef>, name: &str, series_key: &str, opt: &IteratorOptions) -> Result<Option<iterator::Iter>> {
    fn create_var_ref_series_iter(
        vlog_inner: Option<Arc<RwLock<VLogInner>>>,
        cache: &Arc<Cache>,
        fieldset: &Arc<MeasurementFieldSet>,
        file_store: &Arc<FileStore>,
        series_key: &str,
        filter: Option<StoragePredicate>,
        plan: &Arc<EngineScanPlan>,
    ) -> Result<Option<Box<dyn QueryIterator + Sync + Send>>> {
        let mut name = plan.scan.measurement.name.as_str();
        let (_, tfs) = crate::models::points::parse_key(series_key.as_bytes());
        let tags = query_point::Tags::new(tfs.map());

        let measurement_fields = fieldset.fields_by_string(name);
        let predicate = filter.as_ref().map(|filter| {
            PreparedStoragePredicate::prepare(filter, |column| {
                Self::resolve_predicate_column(column, name, measurement_fields.as_deref(), &tags)
            })
        });

        // Build main cursor.
        let mut main_cur = None;
        if let Some(column) = &plan.scan.projection.primary_column {
            main_cur = Self::build_cursor(
                vlog_inner.clone(),
                cache,
                fieldset,
                file_store,
                name,
                series_key,
                tfs.clone(),
                column,
                plan,
            )?;
            // If the field doesn't exist then don't build an iterator.
            if main_cur.is_none() {
                return Ok(None);
            }
        }

        // Build auxiliary cursors.
        // Tag values should be returned if the field doesn't exist.
        let mut aux: Vec<Box<dyn CursorAt + Sync + Send>> = vec![];
        let aux_start = usize::from(plan.scan.projection.primary_column.is_some());
        for column in &plan.scan.projection.returned_columns[aux_start..] {
            // Create cursor from field if a tag wasn't requested.
            if !matches!(column.kind, ColumnKind::Tag) {
                let cur = Self::build_cursor(
                    vlog_inner.clone(),
                    cache,
                    fieldset,
                    file_store,
                    name,
                    series_key,
                    tfs.clone(),
                    column,
                    plan,
                )?;
                if cur.is_some() {
                    aux.push(Box::new(BufCursor::new(cur, plan.scan.ascending())));
                    continue;
                }

                // 1) fieldset has no fields for the measurement;
                // 2) measurementfields has no filed for the fieldvalue
                // If a field was requested, use a nil cursor of the requested type.
                match column.kind {
                    ColumnKind::Field(Some(crate::models::FLOAT)) | ColumnKind::AnyField => {
                        aux.push(Box::new(NoneFloatLiteralValueCursor::default()));
                        continue;
                    }
                    ColumnKind::Field(Some(crate::models::INTEGER)) => {
                        aux.push(Box::new(NoneIntegerLiteralValueCursor::default()));
                        continue;
                    }
                    ColumnKind::Field(Some(crate::models::UNSIGNED)) => {
                        aux.push(Box::new(NoneUnsignedLiteralValueCursor::default()));
                        continue;
                    }
                    ColumnKind::Field(Some(crate::models::BOOL)) => {
                        aux.push(Box::new(NoneBoolLiteralValueCursor::default()));
                        continue;
                    }
                    ColumnKind::Field(Some(crate::models::STRING)) => {
                        aux.push(Box::new(NoneStringLiteralValueCursor::default()));
                        continue;
                    }
                    _ => {}
                }
            }

            // If field doesn't exist, use the tag value.
            match tags.value(&column.name) {
                Some(v) => aux.push(Box::new(LiteralValueCursor::new(Val::String(v)))),
                None => {
                    // However, if the tag value is blank then return a None.
                    aux.push(Box::new(NoneStringLiteralValueCursor::default()));
                }
            }
        }

        // Row-domain cursors contribute timestamps only for an aux-only scan;
        // a primary cursor exclusively owns timestamps when present. These
        // cursors never produce Point::aux values. Missing fields are skipped.
        for column in &plan.scan.projection.row_domain_columns {
            if !matches!(column.kind, ColumnKind::Tag)
                && let Some(cur) = Self::build_cursor(
                    vlog_inner.clone(),
                    cache,
                    fieldset,
                    file_store,
                    name,
                    series_key,
                    tfs.clone(),
                    column,
                    plan,
                )?
            {
                aux.push(Box::new(BufCursor::new(Some(cur), plan.scan.ascending())));
            }
        }

        let conds = Self::build_condition_cursors(
            vlog_inner.clone(),
            cache,
            fieldset,
            file_store,
            plan.scan.measurement.name.as_str(),
            series_key,
            tfs.clone(),
            predicate
                .as_ref()
                .map(PreparedStoragePredicate::field_dependencies)
                .unwrap_or_default(),
            plan,
        )?;

        let tags = tags.subset(&plan.scan.projection.series.output_tag_keys);

        // If it's only auxiliary fields then it doesn't matter what type of iterator we use.
        if plan.scan.projection.primary_column.is_none() {
            if plan.strip_name() {
                name = "";
            }
            return Ok(Some(Box::new(FloatIter::new_with_storage_predicate(
                name.to_string(),
                tags,
                Arc::clone(&plan.scan),
                None, // var_ref is none, so the main cursor is none, we just pass the aux cursor.
                aux,
                conds,
                predicate,
            ))));
        }

        // Remove name if requested.
        if plan.strip_name() {
            name = "";
        }

        // FIXME: we donot need to check if main_cur is None
        if let Some(cur) = main_cur {
            match cur {
                Cursor::IntegerCursor(c) => {
                    return Ok(Some(Box::new(IntegerIter::new_with_storage_predicate(
                        name.to_string(),
                        tags,
                        Arc::clone(&plan.scan),
                        Some(c),
                        aux,
                        conds,
                        predicate,
                    ))));
                }
                Cursor::UnsignedCursor(c) => {
                    return Ok(Some(Box::new(UnsignedIter::new_with_storage_predicate(
                        name.to_string(),
                        tags,
                        Arc::clone(&plan.scan),
                        Some(c),
                        aux,
                        conds,
                        predicate,
                    ))));
                }
                Cursor::FloatCursor(c) => {
                    return Ok(Some(Box::new(FloatIter::new_with_storage_predicate(
                        name.to_string(),
                        tags,
                        Arc::clone(&plan.scan),
                        Some(c),
                        aux,
                        conds,
                        predicate,
                    ))));
                }
                Cursor::BoolCursor(c) => {
                    return Ok(Some(Box::new(BoolIter::new_with_storage_predicate(
                        name.to_string(),
                        tags,
                        Arc::clone(&plan.scan),
                        Some(c),
                        aux,
                        conds,
                        predicate,
                    ))));
                }
                Cursor::StringCursor(c) => {
                    return Ok(Some(Box::new(StringIter::new_with_storage_predicate(
                        name.to_string(),
                        tags,
                        Arc::clone(&plan.scan),
                        Some(c),
                        aux,
                        conds,
                        predicate,
                    ))));
                }
            }
        }

        Ok(None)
    }

    fn build_condition_cursors(
        vlog_inner: Option<Arc<RwLock<VLogInner>>>,
        cache: &Arc<Cache>,
        fieldset: &Arc<MeasurementFieldSet>,
        file_store: &Arc<FileStore>,
        measurement: &str,
        series_key: &str,
        series_tags: crate::models::Tags,
        condition_fields: &[ColumnRef],
        plan: &Arc<EngineScanPlan>,
    ) -> Result<Vec<Box<dyn CursorAt + Sync + Send>>> {
        let mut cursors = Vec::<Box<dyn CursorAt + Sync + Send>>::new();

        for column in condition_fields {
            let cursor = Self::build_cursor(
                vlog_inner.clone(),
                cache,
                fieldset,
                file_store,
                measurement,
                series_key,
                series_tags.clone(),
                column,
                plan,
            )?;
            if let Some(cursor) = cursor {
                cursors.push(Box::new(BufCursor::new(
                    Some(cursor),
                    plan.scan.ascending(),
                )));
            } else {
                cursors.push(Self::missing_predicate_field_cursor(column));
            }
        }

        Ok(cursors)
    }

    fn resolve_predicate_column(
        column: &ColumnRef,
        measurement: &str,
        fields: Option<&MeasurementFields>,
        tags: &query_point::Tags,
    ) -> ColumnResolution {
        let tag_value = || ColumnResolution::Static(tags.value(&column.name).map(Val::String));
        match column.kind {
            ColumnKind::Time => ColumnResolution::Time,
            ColumnKind::Measurement => {
                ColumnResolution::Static(Some(Val::String(measurement.to_string())))
            }
            ColumnKind::LegacyName => ColumnResolution::Static(Some(Val::String(
                tags.value(&column.name)
                    .unwrap_or_else(|| measurement.to_string()),
            ))),
            ColumnKind::Tag | ColumnKind::Field(None) => tag_value(),
            ColumnKind::Field(Some(_)) | ColumnKind::AnyField => {
                if fields.is_some_and(|fields| fields.has_field(&column.name)) {
                    ColumnResolution::Field(column.clone())
                } else {
                    ColumnResolution::Static(None)
                }
            }
            ColumnKind::FieldOrTag => {
                if fields.is_some_and(|fields| fields.has_field(&column.name)) {
                    ColumnResolution::Field(ColumnRef::new(&column.name, ColumnKind::AnyField))
                } else {
                    tag_value()
                }
            }
        }
    }

    fn missing_predicate_field_cursor(column: &ColumnRef) -> Box<dyn CursorAt + Sync + Send> {
        match column.kind {
            ColumnKind::Field(Some(crate::models::INTEGER)) => {
                Box::new(NoneIntegerLiteralValueCursor::default())
            }
            ColumnKind::Field(Some(crate::models::UNSIGNED)) => {
                Box::new(NoneUnsignedLiteralValueCursor::default())
            }
            ColumnKind::Field(Some(crate::models::BOOL)) => {
                Box::new(NoneBoolLiteralValueCursor::default())
            }
            ColumnKind::Field(Some(crate::models::STRING)) => {
                Box::new(NoneStringLiteralValueCursor::default())
            }
            ColumnKind::Field(Some(_)) | ColumnKind::AnyField => {
                Box::new(NoneFloatLiteralValueCursor::default())
            }
            _ => unreachable!("prepared predicate dependencies are fields"),
        }
    }

    /// Creates an untyped cursor for a field.
    fn build_cursor(
        vlog_inner: Option<Arc<RwLock<VLogInner>>>,
        cache: &Arc<Cache>,
        fieldset: &Arc<MeasurementFieldSet>,
        file_store: &Arc<FileStore>,
        measurement: &str,
        series_key: &str,
        tags: crate::models::Tags,
        column: &ColumnRef,
        plan: &Arc<EngineScanPlan>,
    ) -> Result<Option<Cursor>> {
        // Check if this is a system field cursor.
        // switch ref.Val {
        // case "_name":
        //     return &stringSliceCursor{values: []string{measurement}}
        // case "_tagKey":
        //     return &stringSliceCursor{values: tags.Keys()}
        // case "_tagValue":
        //     return &stringSliceCursor{values: matchTagValues(tags, opt.Condition)}
        // case "_seriesKey":
        //     return &stringSliceCursor{values: []string{seriesKey}}
        // }

        // Look up fields for measurement.
        let Some(mf) = fieldset.fields_by_string(measurement) else { return Ok(None); };

        // Check for system field for field keys.
        // if ref.Val == "_fieldKey" {
        //     return &stringSliceCursor{values: mf.FieldKeys()}
        // }

        // Find individual field.
        let Some(f) = mf.field(&column.name) else { return Ok(None); };

        if matches!(
            column.kind,
            ColumnKind::Tag
                | ColumnKind::Time
                | ColumnKind::Measurement
                | ColumnKind::LegacyName
                | ColumnKind::Field(None)
        ) {
            return Ok(None);
        }

        // Check if we need to perform a cast. Performing a cast in the
        // engine (if it is possible) is much more efficient than an automatic cast.
        if let ColumnKind::Field(Some(requested_type)) = column.kind
            && requested_type != f.typ
        {
            return Ok(match (f.typ, requested_type) {
                (crate::models::INTEGER, crate::models::FLOAT) => {
                    let cursor = build_integer_cursor(
                        cache,
                        Arc::clone(file_store),
                        vlog_inner,
                        measurement,
                        series_key,
                        &column.name,
                        plan,
                    )?;
                    Some(Cursor::FloatCursor(FloatCursor::FloatCastIntegerCursor(
                        FloatCastIntegerCursor::new(cursor),
                    )))
                }
                (crate::models::UNSIGNED, crate::models::FLOAT) => {
                    let cursor = build_unsigned_cursor(
                        cache,
                        Arc::clone(file_store),
                        vlog_inner,
                        measurement,
                        series_key,
                        &column.name,
                        plan,
                    )?;
                    Some(Cursor::FloatCursor(FloatCursor::FloatCastUnsignedCursor(
                        FloatCastUnsignedCursor::new(cursor),
                    )))
                }
                (crate::models::FLOAT, crate::models::INTEGER) => {
                    let cursor = build_float_cursor(
                        cache,
                        Arc::clone(file_store),
                        vlog_inner,
                        measurement,
                        series_key,
                        &column.name,
                        plan,
                    )?;
                    Some(Cursor::IntegerCursor(
                        IntegerCursor::IntegerCastFloatCursor(IntegerCastFloatCursor::new(cursor)),
                    ))
                }
                (crate::models::UNSIGNED, crate::models::INTEGER) => {
                    let cursor = build_unsigned_cursor(
                        cache,
                        Arc::clone(file_store),
                        vlog_inner,
                        measurement,
                        series_key,
                        &column.name,
                        plan,
                    )?;
                    Some(Cursor::IntegerCursor(
                        IntegerCursor::IntegerCastUnsignedCursor(IntegerCastUnsignedCursor::new(
                            cursor,
                        )),
                    ))
                }
                (crate::models::FLOAT, crate::models::UNSIGNED) => {
                    let cursor = build_float_cursor(
                        cache,
                        Arc::clone(file_store),
                        vlog_inner,
                        measurement,
                        series_key,
                        &column.name,
                        plan,
                    )?;
                    Some(Cursor::UnsignedCursor(
                        UnsignedCursor::UnsignedCastFloatCursor(UnsignedCastFloatCursor::new(
                            cursor,
                        )),
                    ))
                }
                (crate::models::INTEGER, crate::models::UNSIGNED) => {
                    let cursor = build_integer_cursor(
                        cache,
                        Arc::clone(file_store),
                        vlog_inner,
                        measurement,
                        series_key,
                        &column.name,
                        plan,
                    )?;
                    Some(Cursor::UnsignedCursor(
                        UnsignedCursor::UnsignedCastIntegerCursor(UnsignedCastIntegerCursor::new(
                            cursor,
                        )),
                    ))
                }
                _ => None,
            });
        }

        // Return appropriate cursor based on type.
        Ok(match f.typ {
            crate::models::INTEGER => {
                let cur = build_integer_cursor(
                    cache,
                    Arc::clone(file_store),
                    vlog_inner,
                    measurement,
                    series_key,
                    &column.name,
                    plan,
                )?;
                Some(Cursor::IntegerCursor(cur))
            }
            crate::models::UNSIGNED => {
                let cur = build_unsigned_cursor(
                    cache,
                    Arc::clone(file_store),
                    vlog_inner,
                    measurement,
                    series_key,
                    &column.name,
                    plan,
                )?;
                Some(Cursor::UnsignedCursor(cur))
            }
            crate::models::FLOAT => {
                let cur = build_float_cursor(
                    cache,
                    Arc::clone(file_store),
                    vlog_inner,
                    measurement,
                    series_key,
                    &column.name,
                    plan,
                )?;
                Some(Cursor::FloatCursor(cur))
            }
            crate::models::BOOL => {
                let cur = build_bool_cursor(
                    cache,
                    Arc::clone(file_store),
                    vlog_inner,
                    measurement,
                    series_key,
                    &column.name,
                    plan,
                )?;
                Some(Cursor::BoolCursor(cur))
            }
            crate::models::STRING => {
                let cur = build_string_cursor(
                    cache,
                    Arc::clone(file_store),
                    vlog_inner,
                    measurement,
                    series_key,
                    &column.name,
                    plan,
                )?;
                Some(Cursor::StringCursor(cur))
            }
            _ => panic!("unreachable"),
        })
    }

    // IteratorCost produces the cost of an iterator.
    // 解析和规划查询，然后打印a summary of estimated costs。 许多 SQL 引擎使用 EXPLAIN 语句
    // 显示join order、join algorithms以及predicate and expression pushdown (谓词和表达式下推)。
    //  由于 QL 不支持joins，因此 QL 查询的成本通常是访问的the total series accessed、
    // iterator accesses to a TSM file次数以及the number of TSM blocks that need to be scanned
    // 的“函数”。
    // The elements of EXPLAIN query plan include:

    // expression
    // auxiliary fields
    // number of shards
    // number of series
    // cached values
    // number of files
    // number of blocks
    // size of blocks
    /// Returns FieldType for a series.  If the series does not exist, ErrUnknownFieldType is returned.
    pub fn field_type(&self, series: &[u8]) -> Result<FieldType> {
        let inner = self.inner.read();

        if let Ok(typ) = inner.cache.series_type(series) {
            return Ok(typ);
        }

        let typ = inner
            .file_store
            .value_type(series)
            .context(FileStoreSnafu)?;
        match typ {
            BLOCK_INTEGER => Ok(crate::models::INTEGER),
            BLOCK_UNSIGNED => Ok(crate::models::UNSIGNED),
            BLOCK_FLOAT64 => Ok(crate::models::FLOAT),
            BLOCK_BOOL => Ok(crate::models::BOOL),
            BLOCK_STRING => Ok(crate::models::STRING),
            _ => Err(Error::ErrUnknownFieldType),
        }
    }

    fn series_cost(
        &self,
        series_key: &str,
        field: &str,
        tmin: i64,
        tmax: i64,
    ) -> query_iterator::IteratorCost {
        let key = series_field_key_bytes(series_key, field);
        let inner = self.inner.read();
        let mut c = inner.file_store.cost(&key, tmin, tmax);

        // Retrieve the range of values within the cache.
        let mut cache_values = inner.cache.values(&key);
        cache_values.include(tmin, tmax);
        c.cached_values = cache_values.len() as i64;

        c
    }

    // TODO: add reindex vlog_index_wal
    /// When open a shard, if index path is not exist, we should reindex the index.
    pub fn reindex(&self) -> Result<()> {
        if self.aggregate_index.is_some() {
            return self.reindex_aggregate_index();
        }
        let mut keys = vec![vec![]; Self::REINDEX_BATCH_SIZE];
        let mut series_keys = vec![vec![]; Self::REINDEX_BATCH_SIZE];
        let mut names = vec![vec![]; Self::REINDEX_BATCH_SIZE];
        let mut tags = vec![Tags::default(); Self::REINDEX_BATCH_SIZE];

        let inner = self.inner.read();
        let mut n = 0;

        // Index data stored in TSM files.
        inner
            .file_store
            .walk_keys(&[], |key, _| {
                Self::reindex_key(
                    Arc::clone(&self.index),
                    key,
                    &mut n,
                    &mut keys,
                    &mut series_keys,
                    &mut names,
                    &mut tags,
                )
            })
            .context(IndexSnafu)?;

        // Index data stored in VLog files.
        if self.vlog_enabled {
            let vlog_file_store = &inner.vlog_inner.as_ref().unwrap().read().vlog_file_store;

            vlog_file_store
                .walk_keys(&[], |key, _| {
                    Self::reindex_key(
                        Arc::clone(&self.index),
                        key,
                        &mut n,
                        &mut keys,
                        &mut series_keys,
                        &mut names,
                        &mut tags,
                    )
                })
                .context(IndexSnafu)?;
        }

        // Make sure all TSM data is indexed.
        Self::reindex_batch(
            Arc::clone(&self.index),
            &mut n,
            &mut keys,
            &mut series_keys,
            &mut names,
            &mut tags,
        )
        .context(IndexSnafu)?;

        if !self.wal_enabled {
            // All done.
            return Ok(());
        }

        // Reindex data stored in the WAL cache.
        for key in inner.cache.keys() {
            Self::reindex_key(
                Arc::clone(&self.index),
                key,
                &mut n,
                &mut keys,
                &mut series_keys,
                &mut names,
                &mut tags,
            )
            .context(IndexSnafu)?;
        }

        // Make sure all WAL data is indexed.
        Self::reindex_batch(
            Arc::clone(&self.index),
            &mut n,
            &mut keys,
            &mut series_keys,
            &mut names,
            &mut tags,
        )
        .context(IndexSnafu)
    }

    fn reindex_batch(
        index: Arc<Index>,
        n: &mut usize,
        keys: &mut [Vec<u8>],
        series_keys: &mut [Vec<u8>],
        names: &mut [Vec<u8>],
        tags: &mut [Tags],
    ) -> index::Result<()> {
        if *n == 0 {
            return Ok(());
        }

        for (i, key) in keys[..*n].iter().enumerate() {
            let (series_key, _) = series_and_file_form_composite_key(key);
            series_keys[i] = series_key.to_vec();

            let (name, tag) = crate::models::points::parse_key_bytes(&series_keys[i]);
            names[i] = name;
            tags[i] = tag;
        }

        let keys = keys[..*n]
            .iter()
            .map(|k| k.as_slice())
            .collect::<Vec<&[u8]>>();
        index.create_series_list_if_not_exists(
            keys.as_slice(),
            names[..*n].to_vec(),
            tags[..*n].to_vec(),
        )?;

        *n = 0;

        Ok(())
    }

    fn reindex_key(
        index: Arc<Index>,
        key: Vec<u8>,
        n: &mut usize,
        keys: &mut [Vec<u8>],
        series_keys: &mut [Vec<u8>],
        names: &mut [Vec<u8>],
        tags: &mut [Tags],
    ) -> index::Result<()> {
        keys[*n] = key;
        *n += 1;

        if *n < Self::REINDEX_BATCH_SIZE {
            return Ok(());
        }

        Self::reindex_batch(index, n, keys, series_keys, names, tags)
    }
}

// //////////////////////////////////////
// ///// engine.generation /////////
// //////////////////////////////////////

fn build_key_cursor(file_store: Arc<FileStore>, key: &[u8], plan: &EngineScanPlan) -> KeyCursor {
    #[cfg(feature = "slow-trace")]
    let lookup_started = Instant::now();
    #[cfg(feature = "slow-trace")]
    let file_index_searches = file_store.count() as u64;
    let mut cursor = FileStore::key_cursor(
        file_store,
        key,
        plan.scan.seek_time(),
        plan.scan.ascending(),
    );
    #[cfg(feature = "slow-trace")]
    if let Some(profile) = &plan.scan.execution.profile {
        profile.record_file_index_searches(file_index_searches);
        profile.record_block_lookup(
            lookup_started
                .elapsed()
                .as_micros()
                .min(u128::from(u64::MAX)) as u64,
        );
        cursor.attach_profile(Arc::clone(profile));
    }
    cursor
}

fn build_vlog_cursor(
    vlog_inner: Option<Arc<RwLock<VLogInner>>>,
    key: &[u8],
    typ: FieldType,
    plan: &EngineScanPlan,
) -> Result<Option<vlog::key_cursor::VLogKeyCursor>> {
    if !plan.vlog_enabled { return Ok(None); }
    let inner = vlog_inner.ok_or_else(|| Error::StringErr { reason: "VLog state is missing".into() })?;
    let guard = inner.read();
    let indexes = guard.vlog_index_cache.values_range(key, plan.scan.time_range.start_time,
        plan.scan.time_range.end_time);
    Ok(Some(VLogFileStore::vlog_key_cursor(Arc::clone(&guard.vlog_file_store), indexes, key,
        plan.scan.seek_time(), plan.scan.ascending())))
}

/// Creates a cursor for a integer field.
fn build_integer_cursor(
    cache: &Arc<Cache>,
    file_store: Arc<FileStore>,
    vlog_inner: Option<Arc<RwLock<VLogInner>>>,
    measurement: &str,
    series_key: &str,
    field: &str,
    plan: &EngineScanPlan,
) -> Result<IntegerCursor> {
    let key = series_field_key_bytes(series_key, field);
    let cache_values = cache.values(&key);
    let key_cursor = build_key_cursor(file_store, &key, plan);
    let mut vlog_key_cursor = build_vlog_cursor(vlog_inner, &key, crate::models::INTEGER, plan)?;
    #[cfg(feature = "slow-trace")]
    if let (Some(cursor), Some(profile)) = (&mut vlog_key_cursor, &plan.scan.execution.profile) {
        cursor.attach_profile(Arc::clone(profile));
    }

    Ok(match plan.scan.ascending() {
        true => IntegerCursor::IntegerAscendingCursor(IntegerAscendingCursor::new(
            plan.vlog_enabled,
            plan.scan.seek_time(),
            cache_values,
            key_cursor,
            vlog_key_cursor,
        )),
        false => IntegerCursor::IntegerDescendingCursor(IntegerDescendingCursor::new(
            plan.vlog_enabled,
            plan.scan.seek_time(),
            cache_values,
            key_cursor,
            vlog_key_cursor,
        )),
    })
}
/// Creates a cursor for a unsigned field.
fn build_unsigned_cursor(
    cache: &Arc<Cache>,
    file_store: Arc<FileStore>,
    vlog_inner: Option<Arc<RwLock<VLogInner>>>,
    measurement: &str,
    series_key: &str,
    field: &str,
    plan: &EngineScanPlan,
) -> Result<UnsignedCursor> {
    let key = series_field_key_bytes(series_key, field);
    let cache_values = cache.values(&key);
    let key_cursor = build_key_cursor(file_store, &key, plan);
    let mut vlog_key_cursor = build_vlog_cursor(vlog_inner, &key, crate::models::UNSIGNED, plan)?;
    #[cfg(feature = "slow-trace")]
    if let (Some(cursor), Some(profile)) = (&mut vlog_key_cursor, &plan.scan.execution.profile) {
        cursor.attach_profile(Arc::clone(profile));
    }

    Ok(match plan.scan.ascending() {
        true => UnsignedCursor::UnsignedAscendingCursor(UnsignedAscendingCursor::new(
            plan.vlog_enabled,
            plan.scan.seek_time(),
            cache_values,
            key_cursor,
            vlog_key_cursor,
        )),
        false => UnsignedCursor::UnsignedDescendingCursor(UnsignedDescendingCursor::new(
            plan.vlog_enabled,
            plan.scan.seek_time(),
            cache_values,
            key_cursor,
            vlog_key_cursor,
        )),
    })
}

/// Creates a cursor for a float field.
fn build_float_cursor(
    cache: &Arc<Cache>,
    file_store: Arc<FileStore>,
    vlog_inner: Option<Arc<RwLock<VLogInner>>>,
    measurement: &str,
    series_key: &str,
    field: &str,
    plan: &EngineScanPlan,
) -> Result<FloatCursor> {
    let key = series_field_key_bytes(series_key, field);
    let cache_values = cache.values(&key);
    let key_cursor = build_key_cursor(file_store, &key, plan);
    let mut vlog_key_cursor = build_vlog_cursor(vlog_inner, &key, crate::models::FLOAT, plan)?;
    #[cfg(feature = "slow-trace")]
    if let (Some(cursor), Some(profile)) = (&mut vlog_key_cursor, &plan.scan.execution.profile) {
        cursor.attach_profile(Arc::clone(profile));
    }

    Ok(match plan.scan.ascending() {
        true => FloatCursor::FloatAscendingCursor(FloatAscendingCursor::new(
            plan.vlog_enabled,
            plan.scan.seek_time(),
            cache_values,
            key_cursor,
            vlog_key_cursor,
        )),
        false => FloatCursor::FloatDescendingCursor(FloatDescendingCursor::new(
            plan.vlog_enabled,
            plan.scan.seek_time(),
            cache_values,
            key_cursor,
            vlog_key_cursor,
        )),
    })
}

/// Creates a cursor for a bool field.
fn build_bool_cursor(
    cache: &Arc<Cache>,
    file_store: Arc<FileStore>,
    vlog_inner: Option<Arc<RwLock<VLogInner>>>,
    measurement: &str,
    series_key: &str,
    field: &str,
    plan: &EngineScanPlan,
) -> Result<BoolCursor> {
    let key = series_field_key_bytes(series_key, field);
    let cache_values = cache.values(&key);
    let key_cursor = build_key_cursor(file_store, &key, plan);
    let mut vlog_key_cursor = build_vlog_cursor(vlog_inner, &key, crate::models::BOOL, plan)?;
    #[cfg(feature = "slow-trace")]
    if let (Some(cursor), Some(profile)) = (&mut vlog_key_cursor, &plan.scan.execution.profile) {
        cursor.attach_profile(Arc::clone(profile));
    }

    Ok(match plan.scan.ascending() {
        true => BoolCursor::BoolAscendingCursor(BoolAscendingCursor::new(
            plan.vlog_enabled,
            plan.scan.seek_time(),
            cache_values,
            key_cursor,
            vlog_key_cursor,
        )),
        false => BoolCursor::BoolDescendingCursor(BoolDescendingCursor::new(
            plan.vlog_enabled,
            plan.scan.seek_time(),
            cache_values,
            key_cursor,
            vlog_key_cursor,
        )),
    })
}

/// Creates a cursor for a string field.
fn build_string_cursor(
    cache: &Arc<Cache>,
    file_store: Arc<FileStore>,
    vlog_inner: Option<Arc<RwLock<VLogInner>>>,
    measurement: &str,
    series_key: &str,
    field: &str,
    plan: &EngineScanPlan,
) -> Result<StringCursor> {
    let key = series_field_key_bytes(series_key, field);
    let cache_values = cache.values(&key);
    let key_cursor = build_key_cursor(file_store, &key, plan);
    let mut vlog_key_cursor = build_vlog_cursor(vlog_inner, &key, crate::models::STRING, plan)?;
    #[cfg(feature = "slow-trace")]
    if let (Some(cursor), Some(profile)) = (&mut vlog_key_cursor, &plan.scan.execution.profile) {
        cursor.attach_profile(Arc::clone(profile));
    }

    Ok(match plan.scan.ascending() {
        true => StringCursor::StringAscendingCursor(StringAscendingCursor::new(
            plan.vlog_enabled,
            plan.scan.seek_time(),
            cache_values,
            key_cursor,
            vlog_key_cursor,
        )),
        false => StringCursor::StringDescendingCursor(StringDescendingCursor::new(
            plan.vlog_enabled,
            plan.scan.seek_time(),
            cache_values,
            key_cursor,
            vlog_key_cursor,
        )),
    })
}

/// Combine a series key and field name for a unique: String to be hashed to a numeric ID.
pub fn series_field_key(series_key: String, field: String) -> String {
    series_key + KEY_FIELD_SEPARATOR + &field
}

pub fn series_field_key_bytes(series_key: &str, field: &str) -> Vec<u8> {
    let series_key_len = series_key.len();
    let key_field_separator_len = KEY_FIELD_SEPARATOR.len();
    let field_len = field.len();
    let mut b = vec![0u8; series_key_len + key_field_separator_len + field_len];
    b[..series_key_len].copy_from_slice(series_key.as_bytes());
    b[series_key_len..series_key_len + key_field_separator_len]
        .copy_from_slice(KEY_FIELD_SEPARATOR.as_bytes());
    b[series_key_len + key_field_separator_len
        ..series_key_len + key_field_separator_len + field_len]
        .copy_from_slice(field.as_bytes());

    b
}

/// EngineStatistics maintains statistics for the engine.
#[derive(Default)]
pub struct EngineStatistics {
    pub cache_compactions: AtomicI64, // Counter of cache compactions that have ever run.
    pub cache_compactions_active: AtomicI64, // Gauge of cache compactions currently running.
    pub cache_compaction_errors: AtomicI64, // Counter of cache compactions that have failed due to error.
    pub cache_compaction_duration: AtomicI64, // Counter of number of wall nanoseconds spent in cache compactions.

    pub tsm_compactions: [Arc<AtomicI64>; 3], // Counter of TSM compactions (by level) that have ever run.
    pub tsm_compactions_active: [Arc<AtomicI64>; 3], // Gauge of TSM compactions (by level) currently running.
    pub tsm_compaction_errors: [Arc<AtomicI64>; 3], // Counter of TSM compcations (by level) that have failed due to error.
    pub tsm_compaction_duration: [Arc<AtomicI64>; 3], // Counter of number of wall nanoseconds spent in TSM compactions (by level).
    pub tsm_compactions_queue: [AtomicI64; 3],        // Gauge of TSM compactions queues (by level).

    pub tsm_optimize_compactions: Arc<AtomicI64>, // Counter of optimize compactions that have ever run.
    pub tsm_optimize_compactions_active: Arc<AtomicI64>, // Gauge of optimize compactions currently running.
    pub tsm_optimize_compaction_errors: Arc<AtomicI64>, // Counter of optimize compactions that have failed due to error.
    pub tsm_optimize_compaction_duration: Arc<AtomicI64>, // Counter of number of wall nanoseconds spent in optimize compactions.
    pub tsm_optimize_compactions_queue: AtomicI64,        // Gauge of optimize compactions queue.

    pub tsm_full_compactions: Arc<AtomicI64>, // Counter of full compactions that have ever run.
    pub tsm_full_compactions_active: Arc<AtomicI64>, // Gauge of full compactions currently running.
    pub tsm_full_compaction_errors: Arc<AtomicI64>, // Counter of full compactions that have failed due to error.
    pub tsm_full_compaction_duration: Arc<AtomicI64>, // Counter of number of wall nanoseconds spent in full compactions.
    pub tsm_full_compactions_queue: AtomicI64,        // Gauge of full compactions queue.

    // The following group of fields is used to track the state of vlog compactions.
    pub vlog_compactions: [Arc<AtomicI64>; 3], // Counter of VLog compactions (by level) that have ever run.
    pub vlog_compactions_active: [Arc<AtomicI64>; 3], // Gauge of VLog compactions (by level) currently running.
    pub vlog_compaction_errors: [Arc<AtomicI64>; 3], // Counter of VLog compcations (by level) that have failed due to error.
    pub vlog_compaction_duration: [Arc<AtomicI64>; 3], // Counter of number of wall nanoseconds spent in VLog compactions (by level).
    pub vlog_compactions_queue: [AtomicI64; 3], // Gauge of VLog compactions queues (by level).

    pub vlog_optimize_compactions: Arc<AtomicI64>, // Counter of optimize compactions that have ever run.
    pub vlog_optimize_compactions_active: Arc<AtomicI64>, // Gauge of optimize compactions currently running.
    pub vlog_optimize_compaction_errors: Arc<AtomicI64>, // Counter of optimize compactions that have failed due to error.
    pub vlog_optimize_compaction_duration: Arc<AtomicI64>, // Counter of number of wall nanoseconds spent in optimize compactions.
    pub vlog_optimize_compactions_queue: AtomicI64,        // Gauge of optimize compactions queue.

    pub vlog_full_compactions: Arc<AtomicI64>, // Counter of full compactions that have ever run.
    pub vlog_full_compactions_active: Arc<AtomicI64>, // Gauge of full compactions currently running.
    pub vlog_full_compaction_errors: Arc<AtomicI64>, // Counter of full compactions that have failed due to error.
    pub vlog_full_compaction_duration: Arc<AtomicI64>, // Counter of number of wall nanoseconds spent in full compactions.
    pub vlog_full_compactions_queue: AtomicI64,        // Gauge of full compactions queue.
}

// CompactionStrategy holds the details of what to do in a compaction.
struct CompactionStrategy {
    engine_id: u64,
    group: CompactionGroup, // []string

    fast: bool,
    level: i64,

    duration_stat: Arc<AtomicI64>,
    active_stat: Arc<AtomicI64>,
    success_stat: Arc<AtomicI64>,
    error_stat: Arc<AtomicI64>,

    compactor: Arc<Compactor>,
    file_store: Arc<FileStore>,
    // inner: Arc<RwLock<Inner>>,
    // engine: Engine,
}

fn log_compaction_started(engine_id: u64, level: i64, fast: bool, input_file_count: usize) {
    tracing::info!(
        target: "reefsdb::tsdb::compaction",
        engine_id,
        shard_id = engine_id,
        level,
        compaction_type = if fast { "fast" } else { "full" },
        input_file_count,
        "starting compaction"
    );
}

fn log_compaction_completed(
    engine_id: u64,
    level: i64,
    input_file_count: usize,
    duration: Duration,
) {
    tracing::info!(
        target: "reefsdb::tsdb::compaction",
        engine_id,
        shard_id = engine_id,
        level,
        input_file_count,
        duration_ms = duration.as_millis() as u64,
        "compaction complete"
    );
}

impl CompactionStrategy {
    // Apply concurrently compacts all the groups in a compaction strategy.
    pub fn apply(&mut self) {
        let start = std::time::Instant::now();
        log_compaction_started(self.engine_id, self.level, self.fast, self.group.len());
        let completed = self.compact_group();
        self.duration_stat
            .fetch_add(start.elapsed().as_nanos() as i64, Ordering::SeqCst);
        if completed {
            log_compaction_completed(
                self.engine_id,
                self.level,
                self.group.len(),
                start.elapsed(),
            );
        }
    }

    // Executes the compaction strategy against a single CompactionGroup.
    fn compact_group(&mut self) -> bool {
        let group = &self.group;

        let files = if self.fast {
            self.compactor.compact_fast(group)
        } else {
            self.compactor.compact_full(group)
        };
        match files {
            Ok(files) => {
                if let Err(e) = self.file_store.replace(group, &files) {
                    tracing::error!(
                        target: "reefsdb::tsdb::compaction",
                        engine_id = self.engine_id,
                        shard_id = self.engine_id,
                        level = self.level,
                        error = %e,
                        "compaction file replacement failed"
                    );
                    self.error_stat.fetch_add(1, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_secs(1));

                    // Remove the new snapshot files. We will try again.
                    for file in files {
                        if let Err(e) = fs::remove_file(file) {
                            tracing::warn!(
                                target: "reefsdb::tsdb::compaction",
                                error = %e,
                                "compaction output cleanup deferred"
                            );
                        }
                    }
                    return false;
                }

                // TODO: log
                // for (i, f) in files.iter().enumerate() {
                //     log.Info("Compacted file", zap.Int("tsm1_index", i), zap.String("tsm1_file", f))
                // }
                // log.Info("Finished compacting files", zap.Int("tsm1_files_n", len(files)))
                self.success_stat.fetch_add(1, Ordering::SeqCst);
                true
            }
            Err(e) => {
                let in_progress = match &e {
                    compact::Error::CompactionInProgress { reason } => true,
                    _ => false,
                };
                let disabled = match &e {
                    compact::Error::CompactionsDisabled => true,
                    _ => false,
                };
                if disabled || in_progress {
                    tracing::debug!(
                        target: "reefsdb::tsdb::compaction",
                        engine_id = self.engine_id,
                        shard_id = self.engine_id,
                        level = self.level,
                        error = %e,
                        "compaction cancelled"
                    );
                    if in_progress {
                        std::thread::sleep(Duration::from_secs(1));
                    }
                    return false;
                }

                tracing::error!(
                    target: "reefsdb::tsdb::compaction",
                    engine_id = self.engine_id,
                    shard_id = self.engine_id,
                    level = self.level,
                    error = %e,
                    "compaction failed"
                );

                // We hit a bad TSM file - rename so the next compaction can proceed.
                if let compact::Error::BlockRead { file, reason } = &e {
                    // log.Info("Renaming a corrupt TSM file due to compaction error", zap.Error(err))
                    if let Err(e) = self.file_store.replace(std::slice::from_ref(file), &[]) {
                        tracing::warn!(
                            target: "reefsdb::tsdb::compaction",
                            path = %file,
                            error = %e,
                            "corrupt TSM file removal failed"
                        );
                    }
                    if let Err(e) = fs::rename(file, file.clone() + "." + BAD_TSM_FILE_EXTENSION) {
                        tracing::warn!(
                            target: "reefsdb::tsdb::compaction",
                            path = %file,
                            error = %e,
                            "corrupt TSM file quarantine failed"
                        );
                    }
                };

                self.error_stat.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(Duration::from_secs(1));
                false
            }
        }
    }
}

// VLogDefaultCompactionStrategy holds the details of what to do in a default vlog compaction.
struct VLogDefaultCompactionStrategy {
    group: VLogCompactionGroup, // []string

    fast: bool,
    level: i64,

    duration_stat: Arc<AtomicI64>,
    active_stat: Arc<AtomicI64>,
    success_stat: Arc<AtomicI64>,
    error_stat: Arc<AtomicI64>,

    compactor: Arc<VLogCompactor>,
    // file_store: Arc<FileStore>,
    inner: Arc<RwLock<VLogInner>>,
}

struct VLogDefaultCompactionInput {
    compact_info: VLogCompactInfo,
    old_indexes: HashMap<String, Vec<VLogIndex>>,
    old_file_merged_size: HashMap<String, u64>,
}

impl VLogDefaultCompactionStrategy {
    // Apply concurrently compacts all the groups in a compaction strategy.
    pub async fn apply(self) {
        let start = std::time::Instant::now();
        let successes = self.success_stat.load(Ordering::SeqCst);
        let engine_id = self.inner.read().engine_id;
        tracing::info!(
            target: "reefsdb::tsdb::vlog",
            engine_id,
            level = self.level,
            compaction_type = "default",
            input_file_count = self.group.len(),
            "starting VLog compaction"
        );

        let executor = self.compactor.executor();
        let prepare_task = match executor.spawn_blocking_compact(move || {
            let input = self.prepare_group();
            (self, input)
        }) {
            Ok(task) => task,
            Err(error) => {
                tracing::error!(
                    target: "reefsdb::tsdb::vlog",
                    engine_id,
                    error = %error,
                    "VLog compaction preparation scheduling failed"
                );
                return;
            }
        };

        let (mut strategy, mut input) = match prepare_task.await {
            Ok((strategy, Ok(input))) => (strategy, input),
            Ok((strategy, Err(error))) => {
                strategy
                    .duration_stat
                    .fetch_add(start.elapsed().as_nanos() as i64, Ordering::SeqCst);
                tracing::error!(
                    target: "reefsdb::tsdb::vlog",
                    engine_id,
                    level = strategy.level,
                    error = %error,
                    "VLog compaction planning failed"
                );
                return;
            }
            Err(error) => {
                tracing::error!(
                    target: "reefsdb::tsdb::vlog",
                    engine_id,
                    error = %error,
                    "VLog compaction preparation task terminated"
                );
                return;
            }
        };

        let compact_info = std::mem::take(&mut input.compact_info);
        let compact_res = strategy
            .compactor
            .prepare_vlog_compaction(strategy.fast, strategy.group.to_vec(), compact_info, false)
            .await;
        let compactor = Arc::clone(&strategy.compactor);
        let level = strategy.level;
        let final_task = match executor.spawn_blocking_compact(move || {
            let compact_res =
                compact_res.and_then(|prepared| compactor.finish_vlog_compaction_with_aggregates(prepared));
            strategy.compact_group(input, compact_res);
            strategy
                .duration_stat
                .fetch_add(start.elapsed().as_nanos() as i64, Ordering::SeqCst);
            if strategy.success_stat.load(Ordering::SeqCst) > successes {
                tracing::info!(
                    target: "reefsdb::tsdb::vlog",
                    engine_id,
                    level = strategy.level,
                    input_file_count = strategy.group.len(),
                    duration_ms = start.elapsed().as_millis() as u64,
                    "VLog compaction complete"
                );
            }
        }) {
            Ok(task) => task,
            Err(error) => {
                tracing::error!(
                    target: "reefsdb::tsdb::vlog",
                    engine_id,
                    level,
                    error = %error,
                    "VLog compaction apply scheduling failed"
                );
                return;
            }
        };

        if let Err(error) = final_task.await {
            tracing::error!(
                target: "reefsdb::tsdb::vlog",
                engine_id,
                level,
                error = %error,
                "VLog compaction apply task terminated"
            );
        }
    }

    fn prepare_group(&self) -> vlog::compact::Result<VLogDefaultCompactionInput> {
        let group = &self.group;
        // Get all keys and TSMReaders in the group.
        let (readers, keys) = self.compactor.vlog_readers_and_keys(group)?;

        let inner = self.inner.read();
        let mut old_indexes = HashMap::new();
        // keys 是待合并的所有files里的keys， readers和待合并的所有files一一对应
        let mut vlog_indexes = BTreeMap::new();
        let mut vlog_compact_info = VLogCompactInfo::default();
        for (key, typ) in keys.iter() {
            for r in readers.iter() {
                // key在该VLogReader里的index
                let mut indexes =
                    inner
                        .vlog_index_cache
                        .values_by_gen_and_seq(&key, r.generation, r.seq);
                if !indexes.is_empty() {
                    vlog_indexes
                        .entry(to_string!(key))
                        .or_insert_with(Vec::new)
                        .append(&mut indexes.clone());
                    // vlog_readers2
                    //     .entry(to_string!(key))
                    //     .or_insert_with(Vec::new)
                    //     .push(Arc::clone(r));

                    old_indexes
                        .entry(to_string!(key))
                        .or_insert_with(Vec::new)
                        .append(&mut indexes);
                }
            }
        }
        drop(inner);

        let vlog_indexes: Vec<_> = vlog_indexes.into_iter().map(|x| x.1).collect();

        vlog_compact_info.keys = keys; // Keys has been sorted
        vlog_compact_info.indexes = vlog_indexes;
        vlog_compact_info.readers = readers;
        let aggregate_context = { self.inner.read().aggregate_index.clone() };
        if let Some(context) = aggregate_context {
            let mut aggregates = HashMap::new();
            for (key, indexes) in &old_indexes {
                let seq = context.sequence(key.as_bytes()).map_err(|e|
                    vlog::compact::Error::StringErr { reason: e.to_string() })?;
                aggregates.extend(context.store.compaction_aggregates(seq, indexes)
                    .map_err(|e| vlog::compact::Error::StringErr { reason: e.to_string() })?);
            }
            vlog_compact_info.aggregates = Some(aggregates);
        }

        let mut old_file_merged_size = HashMap::<String, u64>::new();
        for indexes in vlog_compact_info.indexes.iter() {
            for idx in indexes.iter() {
                for r in vlog_compact_info.readers.iter() {
                    if (r.generation, r.seq) == (idx.generation, idx.seq) {
                        let path = r.path().to_str().unwrap().to_string();
                        old_file_merged_size
                            .entry(path)
                            .and_modify(|sz| *sz += idx.size as u64)
                            .or_insert(idx.size as u64);
                    }
                }
            }
        }

        Ok(VLogDefaultCompactionInput {
            compact_info: vlog_compact_info,
            old_indexes,
            old_file_merged_size,
        })
    }

    // Executes publication for one prepared VLogCompactionGroup.
    fn compact_group(
        &mut self,
        input: VLogDefaultCompactionInput,
        compact_res: vlog::compact::Result<(Vec<String>, HashMap<String, Vec<VLogIndex>>, vlog::compact::AggregateOutputs)>,
    ) {
        let engine_id = self.inner.read().engine_id;
        let group = &self.group;
        let VLogDefaultCompactionInput {
            compact_info: _,
            old_indexes,
            old_file_merged_size,
        } = input;

        let context = self.inner.read().aggregate_index.clone();
        let mut _aggregate_readers = Vec::new();
        let compact_res = compact_res.and_then(|(files, mut indexes, summaries)| {
            if let Some(context) = &context {
                let store = Arc::clone(&self.inner.read().vlog_file_store);
                let index_outputs = context.compaction_index_files(store, &indexes, &summaries)
                    .map_err(|e| vlog::compact::Error::StringErr { reason: e.to_string() })?;
                _aggregate_readers = context.store.prepare_native(&index_outputs, &mut indexes)
                    .map_err(|e| vlog::compact::Error::StringErr { reason: e.to_string() })?;
            }
            Ok((files, indexes))
        });
        let inner = self.inner.write();
        match compact_res {
            Ok((vlog_files, vlog_indexes)) => {
                // Get merged file data size.
                let mut merged_size = HashMap::<String, u64>::new();
                for (_, indexes) in vlog_indexes.iter() {
                    for idx in indexes {
                        let file_name = inner
                            .vlog_file_store
                            .path()
                            .join(vlog_file_store::default_format_file_name(
                                idx.generation,
                                idx.seq,
                            ))
                            .join(vlog::compact::VLOG_FILE_EXTENSION)
                            .to_str()
                            .unwrap()
                            .to_string();
                        merged_size
                            .entry(file_name)
                            .and_modify(|sz| *sz += idx.size as u64)
                            .or_insert(idx.size as u64);
                    }
                }

                // remove old indexes
                inner.vlog_index_cache.delete_multi(&old_indexes);
                // write new indexes to cache and WAL
                if let Err(e) = inner.vlog_index_cache.write_multi(vlog_indexes.clone()) {
                    tracing::error!(
                        target: "reefsdb::tsdb::vlog",
                        engine_id,
                        level = self.level,
                        error = %e,
                        "VLog index cache publish failed"
                    );
                    // FIXME: 上面 file_store.replace 已经完成，如何回滚？
                    self.error_stat.fetch_add(1, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_secs(1));

                    // Remove the new snapshot files. We will try again.
                    for file in &vlog_files {
                        if let Err(e) = fs::remove_file(file) {
                            // log.Error("Unable to remove file", zap.String("path", file), zap.Error(err))
                        }
                    }
                    return;
                }

                if let Err(e) =
                    futures::executor::block_on(inner.vlog_index_wal.delete_multi(old_indexes))
                {
                    tracing::error!(
                        target: "reefsdb::tsdb::vlog",
                        engine_id,
                        level = self.level,
                        error = %e,
                        "VLog index WAL delete failed"
                    );
                    // FIXME: 上面 file_store.replace 已经完成，如何回滚？
                    self.error_stat.fetch_add(1, Ordering::SeqCst);
                    // Remove the new snapshot files. We will try again.
                    for file in &vlog_files {
                        if let Err(e) = fs::remove_file(file) {
                            // log.Error("Unable to remove file", zap.String("path", file), zap.Error(err))
                        }
                    }
                    return;
                }

                if let Err(e) =
                    futures::executor::block_on(inner.vlog_index_wal.write_multi(vlog_indexes))
                {
                    tracing::error!(
                        target: "reefsdb::tsdb::vlog",
                        engine_id,
                        level = self.level,
                        error = %e,
                        "VLog index WAL publish failed"
                    );
                    // FIXME: 上面 file_store.replace 已经完成，如何回滚？
                    self.error_stat.fetch_add(1, Ordering::SeqCst);
                    // Remove the new snapshot files. We will try again.
                    for file in &vlog_files {
                        if let Err(e) = fs::remove_file(file) {
                            // log.Error("Unable to remove file", zap.String("path", file), zap.Error(err))
                        }
                    }
                    return;
                }

                if let Err(e) = inner.vlog_file_store.replace(
                    group,
                    &vlog_files,
                    &inner.vlog_index_cache,
                    false,
                ) {
                    tracing::error!(
                        target: "reefsdb::tsdb::vlog",
                        engine_id,
                        level = self.level,
                        error = %e,
                        "VLog file replacement failed"
                    );
                    // log.Info("Error replacing new TSM files", zap.Error(err))
                    self.error_stat.fetch_add(1, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_secs(1));

                    // Remove the new snapshot files. We will try again.
                    for file in &vlog_files {
                        if let Err(e) = fs::remove_file(file) {
                            // log.Error("Unable to remove file", zap.String("path", file), zap.Error(err))
                        }
                    }
                    return;
                }

                // for (i, f) in vlog_files.iter().enumerate() {
                //     log.Info("Compacted file", zap.Int("tsm1_index", i), zap.String("tsm1_file", f))
                // }
                // log.Info("Finished compacting files", zap.Int("tsm1_files_n", len(files)))
                self.success_stat.fetch_add(1, Ordering::SeqCst);
            }
            Err(e) => {
                let in_progress =
                    matches!(&e, vlog::compact::Error::CompactionInProgress { reason });
                let disabled = matches!(&e, vlog::compact::Error::CompactionsDisabled);
                if disabled || in_progress {
                    tracing::debug!(
                        target: "reefsdb::tsdb::vlog",
                        engine_id,
                        level = self.level,
                        error = %e,
                        "VLog compaction cancelled"
                    );
                    if in_progress {
                        std::thread::sleep(Duration::from_secs(1));
                    }
                    return;
                }

                tracing::error!(
                    target: "reefsdb::tsdb::vlog",
                    engine_id,
                    level = self.level,
                    error = %e,
                    "VLog compaction failed"
                );

                // We hit a bad TSM file - rename so the next compaction can proceed.
                if let vlog::compact::Error::BlockRead { file, reason } = &e {
                    // log.Info("Renaming a corrupt TSM file due to compaction error", zap.Error(err))
                    if let Err(e) = inner.vlog_file_store.replace(
                        std::slice::from_ref(&file),
                        &[],
                        &inner.vlog_index_cache,
                        false,
                    ) {
                        // log.Info("Error removing bad TSM file", zap.Error(err))
                    }
                    if let Err(e) = fs::rename(file, file.clone() + "." + BAD_TSM_FILE_EXTENSION) {
                        // log.Info("Error renaming corrupt TSM file", zap.Error((err)))
                    }
                };

                self.error_stat.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(Duration::from_secs(1));
            }
        }
        drop(inner);

    }
}

// VLogLazyCompactionStrategy holds the details of what to do in a lazy vlog compaction.
struct VLogLazyCompactionStrategy {
    compact_info: VLogCompactInfo,

    fast: bool,
    level: i64,

    duration_stat: Arc<AtomicI64>,
    active_stat: Arc<AtomicI64>,
    success_stat: Arc<AtomicI64>,
    error_stat: Arc<AtomicI64>,

    compactor: Arc<VLogCompactor>,

    inner: Arc<RwLock<VLogInner>>,
}

impl VLogLazyCompactionStrategy {
    // Apply concurrently compacts all the groups in a compaction strategy.
    pub async fn apply(mut self, compact_plan: &Arc<VLogLazyPlanner>) {
        let start = std::time::Instant::now();
        let successes = self.success_stat.load(Ordering::SeqCst);
        let engine_id = self.inner.read().engine_id;
        tracing::info!(
            target: "reefsdb::tsdb::vlog",
            engine_id,
            level = self.level,
            compaction_type = "lazy",
            input_file_count = self.compact_info.files.len(),
            "starting VLog compaction"
        );

        let executor = self.compactor.executor();
        let context = { self.inner.read().aggregate_index.clone() };
        if let Some(context) = context {
            let mut aggregates = HashMap::new();
            for ((key, _), indexes) in self.compact_info.keys.iter().zip(&self.compact_info.indexes) {
                let result = context.sequence(key).map_err(|e| e.to_string())
                    .and_then(|seq| context.store.compaction_aggregates(seq, indexes).map_err(|e| e.to_string()));
                match result {
                    Ok(values) => aggregates.extend(values),
                    Err(error) => {
                        self.error_stat.fetch_add(1, Ordering::SeqCst);
                        tracing::error!(target: "reefsdb::tsdb::vlog", engine_id, %error,
                            "native lazy compaction aggregate preparation failed");
                        return;
                    }
                }
            }
            self.compact_info.aggregates = Some(aggregates);
        }
        let prepared = self
            .compactor
            .prepare_vlog_compaction(
                self.fast,
                self.compact_info.files.clone(),
                self.compact_info.clone(),
                true,
            )
            .await;
        let compact_plan = Arc::clone(compact_plan);
        let level = self.level;
        let final_task = match executor.spawn_blocking_compact(move || {
            let mut strategy = self;
            strategy.compact_group(&compact_plan, prepared);
            strategy
                .duration_stat
                .fetch_add(start.elapsed().as_nanos() as i64, Ordering::SeqCst);
            if strategy.success_stat.load(Ordering::SeqCst) > successes {
                tracing::info!(
                    target: "reefsdb::tsdb::vlog",
                    engine_id,
                    level = strategy.level,
                    input_file_count = strategy.compact_info.files.len(),
                    duration_ms = start.elapsed().as_millis() as u64,
                    "VLog compaction complete"
                );
            }
        }) {
            Ok(task) => task,
            Err(error) => {
                tracing::error!(
                    target: "reefsdb::tsdb::vlog",
                    engine_id,
                    level,
                    error = %error,
                    "lazy VLog compaction apply scheduling failed"
                );
                return;
            }
        };

        if let Err(error) = final_task.await {
            tracing::error!(
                target: "reefsdb::tsdb::vlog",
                engine_id,
                level,
                error = %error,
                "lazy VLog compaction apply task terminated"
            );
        }
    }

    // Executes the compaction strategy against a single VLogCompactionGroup.
    fn compact_group(
        &mut self,
        compact_plan: &Arc<VLogLazyPlanner>,
        prepared: vlog::compact::Result<PreparedVLogCompaction>,
    ) -> Vec<String> {
        // println!("@@@ compact_group...1");
        let compact_group_start = std::time::Instant::now();
        let engine_id = self.inner.read().engine_id;
        let compaction_id = format!("{:p}", self);
        let mut old_indexes = HashMap::new();
        let mut old_file_merged_size = HashMap::<String, u64>::new();
        for (pos, indexes) in self.compact_info.indexes.iter().enumerate() {
            let (k, _) = &self.compact_info.keys[pos];
            old_indexes.insert(to_string!(k), indexes.clone());
            for idx in indexes.iter() {
                for r in self.compact_info.readers.iter() {
                    // TODO: 有些没有用到
                    if (r.generation, r.seq) == (idx.generation, idx.seq) {
                        let path = r.path();
                        // if path.extension().unwrap() == "cmp" {
                        //     println!("!!!! index file path: {:?}", path);
                        // }
                        old_file_merged_size
                            .entry(path.to_string_lossy().into_owned())
                            .and_modify(|sz| *sz += idx.size as u64)
                            .or_insert(idx.size as u64);
                    }
                }
            }
        }

        // compacted files
        // let mut i = 0;
        // for f in &self.compact_info.files {
        //     // println!("\t file_to_compacted: {:?}", f);
        //     // println!(
        //     //     "\t\t reader_path: {:?}",
        //     //     self.compact_info.readers[i].path()
        //     // );
        //     // println!("\t\t ");

        //     let mut sz = 0;
        //     for idx in &self.compact_info.indexes {
        //         for x in idx {
        //             if (x.generation, x.seq)
        //                 == (
        //                     self.compact_info.readers[i].generation,
        //                     self.compact_info.readers[i].seq,
        //                 )
        //             {
        //                 sz += x.size;
        //             }
        //         }
        //     }
        //     println!("\t file_comp_size: {}", sz);
        //     i += 1;
        // }

        // println!("@@@ compact_group...2");
        let compact_res = prepared.and_then(|prepared|
            self.compactor.finish_vlog_compaction_with_aggregates(prepared));
        let context = { self.inner.read().aggregate_index.clone() };
        let mut _aggregate_readers = Vec::new();
        let compact_res = compact_res.and_then(|(files, mut indexes, summaries)| {
            if let Some(context) = &context {
                let store = Arc::clone(&self.inner.read().vlog_file_store);
                let index_outputs = context.compaction_index_files(store, &indexes, &summaries)
                    .map_err(|e| vlog::compact::Error::StringErr { reason: e.to_string() })?;
                _aggregate_readers = context.store.prepare_native(&index_outputs, &mut indexes)
                    .map_err(|e| vlog::compact::Error::StringErr { reason: e.to_string() })?;
            }
            Ok((files, indexes))
        });
        // println!("@@@ compact_group...3");
        let inner = self.inner.write();
        // println!("@@@ compact_group...4");
        match compact_res {
            Ok((vlog_files, vlog_indexes)) => {
                // println!("@@@ compact_group...5, ok");
                // println!("\t @ old compacted files: ");
                // for vf in &self.compact_info.files {
                //     println!("\t{:?}", vf);
                // }
                // println!("\t @ new vlog files: ");
                // for vf in &vlog_files {
                //     println!("\t{:?}", vf);
                // }
                // println!("\t vlog_indexes.size: {}", vlog_indexes.len());

                let selected_index_count = self
                    .compact_info
                    .indexes
                    .iter()
                    .map(Vec::len)
                    .sum::<usize>();
                let output_index_count = vlog_indexes.values().map(Vec::len).sum::<usize>();
                slow_trace::log_vlog_compaction_event(
                    "vlog_lazy_compaction_before_publish",
                    &format!(
                        "compaction_id={} input_files={} output_tmp_files={} selected_index_count={}",
                        compaction_id,
                        self.compact_info.files.len(),
                        vlog_files.len(),
                        selected_index_count,
                    ),
                );
                // remove old indexes
                inner.vlog_index_cache.delete_multi(&old_indexes);
                // println!("@@@ compact_group...5.1... vlog_index_cache.delete_multi ok");
                // write new indexes to cache and WAL
                if let Err(e) = inner.vlog_index_cache.write_multi(vlog_indexes.clone()) {
                    tracing::error!(
                        target: "reefsdb::tsdb::vlog",
                        engine_id,
                        level = self.level,
                        error = %e,
                        "VLog index cache publish failed"
                    );
                    slow_trace::log_vlog_compaction_event(
                        "vlog_compaction_aborted",
                        &format!(
                            "compaction_id={} last_stage=vlog_index_cache_write error={:?} remaining_tmp_files={}",
                            compaction_id,
                            e,
                            vlog_files.len(),
                        ),
                    );
                    // FIXME: 上面 file_store.replace 已经完成，如何回滚？
                    self.error_stat.fetch_add(1, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_secs(1));

                    // Remove the new snapshot files. We will try again.
                    for file in &vlog_files {
                        if let Err(e) = fs::remove_file(file) {
                            // log.Error("Unable to remove file", zap.String("path", file), zap.Error(err))
                        }
                    }

                    return vec![];
                }
                // println!("@@@ compact_group...5.2... vlog_index_cache.write_multi ok");
                if let Err(e) =
                    futures::executor::block_on(inner.vlog_index_wal.delete_multi(old_indexes))
                {
                    tracing::error!(
                        target: "reefsdb::tsdb::vlog",
                        engine_id,
                        level = self.level,
                        error = %e,
                        "VLog index WAL delete failed"
                    );
                    slow_trace::log_vlog_compaction_event(
                        "vlog_compaction_aborted",
                        &format!(
                            "compaction_id={} last_stage=vlog_index_wal_delete error={:?} remaining_tmp_files={}",
                            compaction_id,
                            e,
                            vlog_files.len(),
                        ),
                    );
                    // FIXME: 上面 file_store.replace 已经完成，如何回滚？
                    self.error_stat.fetch_add(1, Ordering::SeqCst);
                    // Remove the new snapshot files. We will try again.
                    for file in &vlog_files {
                        if let Err(e) = fs::remove_file(file) {
                            // log.Error("Unable to remove file", zap.String("path", file), zap.Error(err))
                        }
                    }

                    return vec![];
                }
                // println!("@@@ compact_group...5.3... vlog_index_wal.delete_multi ok");
                if let Err(e) =
                    futures::executor::block_on(inner.vlog_index_wal.write_multi(vlog_indexes))
                {
                    tracing::error!(
                        target: "reefsdb::tsdb::vlog",
                        engine_id,
                        level = self.level,
                        error = %e,
                        "VLog index WAL publish failed"
                    );
                    slow_trace::log_vlog_compaction_event(
                        "vlog_compaction_aborted",
                        &format!(
                            "compaction_id={} last_stage=vlog_index_wal_write error={:?} remaining_tmp_files={}",
                            compaction_id,
                            e,
                            vlog_files.len(),
                        ),
                    );
                    // FIXME: 上面 file_store.replace 已经完成，如何回滚？
                    self.error_stat.fetch_add(1, Ordering::SeqCst);
                    // Remove the new snapshot files. We will try again.
                    for file in &vlog_files {
                        if let Err(e) = fs::remove_file(file) {
                            // log.Error("Unable to remove file", zap.String("path", file), zap.Error(err))
                        }
                    }

                    return vec![];
                }
                // println!("@@@ compact_group...5.4... vlog_index_wal.write_multi ok");

                // let old_merged_size = rename_merged_size_keys(old_file_merged_size, &need_to_rename);
                // Update merged file size.
                if let Err(e) = inner.vlog_file_store.add_file_size(old_file_merged_size) {
                    tracing::error!(
                        target: "reefsdb::tsdb::vlog",
                        engine_id,
                        level = self.level,
                        error = %e,
                        "VLog compaction progress update failed"
                    );
                    slow_trace::log_vlog_compaction_event(
                        "vlog_compaction_aborted",
                        &format!(
                            "compaction_id={} last_stage=file_size_properties_update error={:?} remaining_tmp_files={}",
                            compaction_id,
                            e,
                            vlog_files.len(),
                        ),
                    );
                    // FIXME: 上面 file_store.replace 已经完成，如何回滚？
                    self.error_stat.fetch_add(1, Ordering::SeqCst);
                    // Remove the new snapshot files. We will try again.
                    for file in &vlog_files {
                        if let Err(e) = fs::remove_file(file) {
                            // log.Error("Unable to remove file", zap.String("path", file), zap.Error(err))
                        }
                    }

                    return vec![];
                }
                // println!("@@@ compact_group...5.5... vlog_file_store.add_file_size ok");

                // TODO: 可以在这里精确记录目录的峰值大小

                let need_to_rename = match inner.vlog_file_store.replace(
                    &self.compact_info.files,
                    &vlog_files,
                    &inner.vlog_index_cache,
                    true,
                ) {
                    Ok(need_to_rename) => need_to_rename,
                    Err(e) => {
                        tracing::error!(
                            target: "reefsdb::tsdb::vlog",
                            engine_id,
                            level = self.level,
                            error = %e,
                            "VLog file replacement failed"
                        );
                        slow_trace::log_vlog_compaction_event(
                            "vlog_compaction_aborted",
                            &format!(
                                "compaction_id={} last_stage=vlog_file_store_replace error={:?} remaining_tmp_files={}",
                                compaction_id,
                                e,
                                vlog_files.len(),
                            ),
                        );
                        // log.Info("Error replacing new TSM files", zap.Error(err))
                        self.error_stat.fetch_add(1, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_secs(1));

                        // Remove the new snapshot files. We will try again.
                        for file in &vlog_files {
                            if let Err(e) = fs::remove_file(file) {
                                // log.Error("Unable to remove file", zap.String("path", file), zap.Error(err))
                            }
                        }

                        return vec![];
                    }
                };
                drop(inner);
                // println!("@@@ compact_group...5.6... vlog_file_store.replace ok");

                // for (i, f) in vlog_files.iter().enumerate() {
                //     log.Info("Compacted file", zap.Int("tsm1_index", i), zap.String("tsm1_file", f))
                // }
                // log.Info("Finished compacting files", zap.Int("tsm1_files_n", len(files)))
                self.success_stat.fetch_add(1, Ordering::SeqCst);
                slow_trace::log_vlog_compaction_event(
                    "vlog_compaction_success",
                    &format!(
                        "compaction_id={} input_count={} output_count={} output_index_count={} deleted_input_count={} elapsed_ms={}",
                        compaction_id,
                        self.compact_info.files.len(),
                        vlog_files.len(),
                        output_index_count,
                        self.compact_info
                            .files
                            .len()
                            .saturating_sub(need_to_rename.len()),
                        compact_group_start.elapsed().as_millis(),
                    ),
                );
                // println!(
                //     "@@@ compact_group...5.7... need_to_rename: {:?}",
                //     need_to_rename
                // );
                return need_to_rename;
            }
            Err(e) => {
                let in_progress =
                    matches!(&e, vlog::compact::Error::CompactionInProgress { reason });
                let disabled = matches!(&e, vlog::compact::Error::CompactionsDisabled);
                if disabled || in_progress {
                    tracing::debug!(
                        target: "reefsdb::tsdb::vlog",
                        engine_id,
                        level = self.level,
                        error = %e,
                        "VLog compaction cancelled"
                    );
                    if in_progress {
                        std::thread::sleep(Duration::from_secs(1));
                    }

                    // self.compact_done.done();
                    slow_trace::log_vlog_compaction_event(
                        "vlog_compaction_aborted",
                        &format!(
                            "compaction_id={} last_stage=compact error={:?} remaining_tmp_files=0",
                            compaction_id, e,
                        ),
                    );
                    return vec![];
                }

                tracing::error!(
                    target: "reefsdb::tsdb::vlog",
                    engine_id,
                    level = self.level,
                    error = %e,
                    "VLog compaction failed"
                );
                slow_trace::log_vlog_compaction_event(
                    "vlog_compaction_aborted",
                    &format!(
                        "compaction_id={} last_stage=compact error={:?} remaining_tmp_files=0",
                        compaction_id, e,
                    ),
                );

                // log.Warn("Error compacting TSM files", zap.Error(err))

                // We hit a bad TSM file - rename so the next compaction can proceed.
                if let vlog::compact::Error::BlockRead { file, reason } = &e {
                    // log.Info("Renaming a corrupt TSM file due to compaction error", zap.Error(err))
                    if let Err(e) = inner.vlog_file_store.replace(
                        std::slice::from_ref(file),
                        &[],
                        &inner.vlog_index_cache,
                        true,
                    ) {
                        // log.Info("Error removing bad TSM file", zap.Error(err))
                    }
                    if let Err(e) = fs::rename(file, file.clone() + "." + BAD_TSM_FILE_EXTENSION) {
                        // log.Info("Error renaming corrupt TSM file", zap.Error((err)))
                    }
                };

                self.error_stat.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(Duration::from_secs(1));

                vec![]
            }
        }
    }
}

fn rename_merged_size_keys(
    old_file_merged_size: HashMap<String, u64>,
    need_to_rename: &[String],
) -> HashMap<String, u64> {
    let need_to_rename: HashSet<&str> = need_to_rename.iter().map(String::as_str).collect();

    old_file_merged_size
        .into_iter()
        .map(|(file_name, size)| {
            if need_to_rename.contains(file_name.as_str()) {
                (format!("{}.cmp", file_name), size) // xxx.vlog -> xxx.vlog.cmp
            } else {
                (file_name, size)
            }
        })
        .collect()
}

pub fn block_type_to_field_type(typ: u8) -> Option<FieldType> {
    match typ {
        BLOCK_FLOAT64 => Some(crate::models::FLOAT),
        BLOCK_INTEGER => Some(crate::models::INTEGER),
        BLOCK_UNSIGNED => Some(crate::models::UNSIGNED),
        BLOCK_STRING => Some(crate::models::STRING),
        BLOCK_BOOL => Some(crate::models::BOOL),
        _ => None,
    }
}

/// Backward-compatible name for the transitional benchmark API.
/// Returns the series key and the field key extracted from the composite key.
pub fn series_and_file_form_composite_key(key: &[u8]) -> (&[u8], &[u8]) {
    match key
        .windows(KEY_FIELD_SEPARATOR_BYTES.len())
        .position(|w| w == KEY_FIELD_SEPARATOR_BYTES.as_bytes())
    {
        Some(idx) => (&key[..idx], &key[idx + KEY_FIELD_SEPARATOR.len()..]),
        None => {
            // No field???
            (key, &[])
        }
    }
}

#[cfg(any(test, feature = "benchmarking"))]
pub mod tests {
    use std::{
        any::Any,
        collections::HashMap,
        fs, i64,
        ops::{Deref, DerefMut, Range},
        os::unix::fs::PermissionsExt,
        path::{Path, PathBuf},
        sync::{
            Arc,
            atomic::{AtomicI32, Ordering},
        },
        time::Duration,
    };

    use crate::common::{time::Time, to_str, wait_group::WaitGroup};
    use crate::models::{Point, points::Val};
    use crate::runtime::{
        execution::{
            BoxBlockingTask, BoxFutureTask, DefaultTokioExecutor, DefaultTokioExecutorOptions,
            ExecutorRef, RuntimeExecutor, SpawnError, TaskController,
        },
        global::{block_on_global, spawn_blocking_global},
    };
    use parking_lot::RwLock;
    use rand::{Rng, RngCore, SeedableRng, distributions::Alphanumeric, rngs::StdRng};
    use tokio_util::sync::CancellationToken;

    use super::{GroupedAggregateStorageKeyJob, block_type_to_field_type, series_field_key_bytes};
    #[cfg(feature = "slow-trace")]
    use super::{
        GroupedBlockEligibility, assert_grouped_eligibility_accounting,
        grouped_float_candidate_reject_reason, observe_grouped_candidate_eligibility,
    };
    use crate::{
        index::{self, SeriesElem},
        query::{
            query_iterator::{
                self, FloatInterruptIter, FloatIterator, FloatLimitIter, FloatMergeIter,
                FloatSortedMergeIter, QueryIterator,
            },
            query_point,
            storage_ir::{
                AggregateKind, AggregatePoint, AggregatePointValue, AggregateRequest,
                AggregateState, AggregateValues, ColumnKind, ColumnRef, CompareOp,
                GroupedAggregateRequest, GroupedAggregateTarget, GroupedAggregateWindow, LogicalOp,
                MeasurementRef, OutputOrder, ScanCancellation, SeriesProjection, StoragePredicate,
                StorageProjection, StorageScan, StorageValueExpr, TimeOrder, TimeRange,
            },
        },
        series::SERIES_FILE_DIRECTORY,
        series::{
            series_file::{
                self, SeriesFile,
                tests::{must_open_series_file, new_series_file},
            },
            series_set::{self, SeriesIDSet},
        },
        storage::active_cursor::{self as iterator, FloatIter},
        storage::{
            EngineOptions, VLogOptions,
            limiter::rate::INF,
            tsm1::{
                block_statistics::BlockStatistics, cache::tests::must_temp_tsm_dir, engine,
                writer::IndexEntry,
            },
        },
    };

    #[test]
    fn grouped_storage_keys_use_influx_contiguous_partitions() {
        let jobs = (0..10)
            .map(|target_index| GroupedAggregateStorageKeyJob {
                target_index,
                field_type: crate::models::FLOAT,
                candidates: Vec::new(),
            })
            .collect();

        let partitions = super::Engine::partition_grouped_storage_key_jobs(jobs, 4);
        assert_eq!(
            partitions
                .iter()
                .map(|partition| partition.index)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
        assert_eq!(
            partitions
                .iter()
                .map(|partition| partition.jobs.len())
                .collect::<Vec<_>>(),
            vec![2, 2, 2, 4]
        );
        assert_eq!(
            partitions
                .into_iter()
                .flat_map(|partition| partition.jobs)
                .map(|job| job.target_index)
                .collect::<Vec<_>>(),
            (0..10).collect::<Vec<_>>()
        );
    }

    #[cfg(feature = "slow-trace")]
    #[test]
    fn grouped_eligibility_observes_missing_and_unsupported_statistics() {
        let request = GroupedAggregateRequest {
            measurement: MeasurementRef::new("diagnostic"),
            targets: vec![GroupedAggregateTarget {
                field: "value".to_string(),
                expected_field_type: crate::models::FLOAT,
                aggregates: vec![AggregateKind::Min, AggregateKind::Max],
            }],
            time_range: TimeRange::new(0, 59),
            window: GroupedAggregateWindow {
                duration_ns: 60,
                origin_ns: 0,
            },
            group_tags: vec![],
            tag_filters: vec![],
            cancellation: None,
        };

        for (statistics, expected_missing, expected_unsupported, expected_reason) in [
            (None, 1, 0, GroupedBlockEligibility::MissingStatistics),
            (
                Some(BlockStatistics::from_integer([1, 2]).unwrap()),
                0,
                1,
                GroupedBlockEligibility::UnsupportedStatistics,
            ),
            (
                Some(BlockStatistics::from_float([f64::NAN]).unwrap()),
                0,
                0,
                GroupedBlockEligibility::FloatNan,
            ),
            (
                Some(BlockStatistics::from_float([-0.0, 1.0]).unwrap()),
                0,
                0,
                GroupedBlockEligibility::FloatSignedZero,
            ),
            (
                Some(BlockStatistics::from_float([-1.0, 1.0]).unwrap()),
                0,
                0,
                GroupedBlockEligibility::FloatZeroCrossing,
            ),
        ] {
            let entry = IndexEntry {
                min_time: 0,
                max_time: 30,
                statistics,
                ..Default::default()
            };
            let mut profile = crate::query::storage_ir::GroupedAggregateProfileMetrics::default();
            observe_grouped_candidate_eligibility(
                &mut profile,
                &entry,
                &request,
                crate::models::FLOAT,
                &[],
            )
            .unwrap();
            assert_eq!(profile.observed_missing_statistics, expected_missing);
            assert_eq!(
                profile.observed_unsupported_statistics,
                expected_unsupported
            );
            let reason = grouped_float_candidate_reject_reason(&entry, crate::models::FLOAT);
            assert_eq!(reason, expected_reason);
            profile.record_eligibility(reason);
            let metrics = crate::query::storage_ir::GroupedAggregateMetrics {
                blocks_candidate: 1,
                profile,
                ..Default::default()
            };
            assert_grouped_eligibility_accounting(&metrics);
            assert!(metrics.profile.eligibility_accounting_matches(1));
        }
    }

    fn aggregate_partition_ranges(len: usize, partitions: usize) -> Vec<Range<usize>> {
        let mut ranges = Vec::with_capacity(partitions);
        let mut start = 0;
        for partition in 0..partitions {
            let remaining = len - start;
            let remaining_partitions = partitions - partition;
            let end = start + remaining.div_ceil(remaining_partitions);
            ranges.push(start..end);
            start = end;
        }
        ranges
    }

    fn aggregate_irregular_ranges(len: usize) -> Vec<Range<usize>> {
        let sizes = [1, 13, 2, 31, 5, 8, 21, 3, 55];
        let mut ranges = Vec::new();
        let mut start = 0;
        let mut index = 0;
        while start < len {
            let end = (start + sizes[index % sizes.len()]).min(len);
            ranges.push(start..end);
            start = end;
            index += 1;
        }
        ranges
    }

    fn aggregate_partition_layouts(len: usize) -> Vec<Vec<Range<usize>>> {
        let mut layouts = [1, 2, 3, 7]
            .into_iter()
            .map(|partitions| aggregate_partition_ranges(len, partitions))
            .collect::<Vec<_>>();
        layouts.push(aggregate_irregular_ranges(len));
        layouts
    }

    fn forced_raw_aggregate_state(
        engine: &super::Engine,
        request: &AggregateRequest,
        field_type: u8,
    ) -> AggregateState {
        let scan = engine.aggregate_storage_scan(request, field_type).unwrap();
        let mut state = AggregateState::empty(field_type);
        if let Some(iterator) = engine.create_storage_iterator(scan).unwrap() {
            let track_endpoints = request
                .aggregates
                .iter()
                .any(|kind| matches!(kind, AggregateKind::First | AggregateKind::Last));
            state =
                query_iterator::fold_aggregate_iterator(iterator, state, track_endpoints).unwrap();
        }
        state
    }

    fn all_aggregate_request(
        measurement: &str,
        field: &str,
        start_time: i64,
        end_time: i64,
    ) -> AggregateRequest {
        AggregateRequest {
            measurement: MeasurementRef::new(measurement),
            field: field.to_string(),
            time_range: TimeRange::new(start_time, end_time),
            aggregates: vec![
                AggregateKind::Count,
                AggregateKind::Sum,
                AggregateKind::Min,
                AggregateKind::Max,
                AggregateKind::Avg,
                AggregateKind::First,
                AggregateKind::Last,
            ],
        }
    }

    #[test]
    fn block_type_to_field_type_accepts_only_concrete_persisted_types() {
        for (typ, expected) in [
            (0, crate::models::FLOAT),
            (1, crate::models::INTEGER),
            (2, crate::models::UNSIGNED),
            (3, crate::models::STRING),
            (4, crate::models::BOOL),
        ] {
            assert_eq!(block_type_to_field_type(typ), Some(expected));
        }

        for typ in [5, 6, 7, 8, 9, u8::MAX] {
            assert_eq!(block_type_to_field_type(typ), None);
        }
    }

    #[test]
    fn block_statistics_are_partition_invariant_for_mergeable_aggregates() {
        fn merge_statistics(
            field_type: crate::models::FieldType,
            statistics: impl IntoIterator<Item = BlockStatistics>,
        ) -> AggregateState {
            let mut state = AggregateState::empty(field_type);
            for statistics in statistics {
                let partial = super::Engine::aggregate_state_from_statistics(
                    field_type, statistics, 0, 0, false,
                )
                .unwrap();
                let entry = super::IndexEntry {
                    statistics: Some(statistics),
                    ..Default::default()
                };
                let eligible =
                    super::Engine::aggregate_state_from_entry(field_type, &entry, false).unwrap();
                let exact_sum = !matches!(field_type, crate::models::INTEGER | crate::models::UNSIGNED)
                    || partial.wide_sum.is_some();
                assert_eq!(eligible.is_some(), exact_sum);
                state.merge(partial).unwrap();
            }
            state
        }

        fn assert_float_state(
            values: &[f64],
            ranges: &[Range<usize>],
            raw: AggregateState,
            merged: AggregateState,
        ) {
            assert_eq!(merged.count, raw.count);
            let Some(AggregateValues::Float {
                sum: raw_sum,
                min: raw_min,
                max: raw_max,
            }) = raw.values
            else {
                panic!("expected raw float state")
            };
            let Some(AggregateValues::Float {
                sum: merged_sum,
                min: merged_min,
                max: merged_max,
            }) = merged.values
            else {
                panic!("expected merged float state")
            };
            assert_eq!(merged_min.to_bits(), raw_min.to_bits());
            assert_eq!(merged_max.to_bits(), raw_max.to_bits());

            if raw_sum.is_nan() {
                assert!(merged_sum.is_nan());
                return;
            }
            if raw_sum.is_infinite() || merged_sum.is_infinite() {
                assert_eq!(merged_sum.to_bits(), raw_sum.to_bits());
                return;
            }

            // Both paths use ordinary f64 additions with different legal groupings.
            // The sum of their standard forward-error bounds is at most
            // 2 * gamma(n + p) * sum(abs(x)), where p is the partition count.
            let operations = (values.len() + ranges.len()) as f64;
            let gamma = operations * f64::EPSILON / (1.0 - operations * f64::EPSILON);
            let absolute_sum = values.iter().map(|value| value.abs()).sum::<f64>();
            let tolerance = 2.0 * gamma * absolute_sum;
            assert!(
                (merged_sum - raw_sum).abs() <= tolerance,
                "float SUM differs beyond forward-error bound: raw={raw_sum:?} \
                 merged={merged_sum:?} tolerance={tolerance:?}"
            );
            assert!(
                (merged_sum / merged.count as f64 - raw_sum / raw.count as f64).abs()
                    <= tolerance / raw.count as f64,
                "float AVG differs beyond inherited SUM error bound"
            );
        }

        let mut rng = StdRng::seed_from_u64(0x5245_4546_5344_4241);
        let mut integers = (0..257).map(|_| rng.next_u64() as i64).collect::<Vec<_>>();
        integers.extend([i64::MIN, i64::MAX, -1, 0, 1]);
        let mut raw_integer = AggregateState::empty(crate::models::INTEGER);
        for value in &integers {
            raw_integer.add_integer(*value);
        }
        for ranges in aggregate_partition_layouts(integers.len()) {
            let merged = merge_statistics(
                crate::models::INTEGER,
                ranges.iter().map(|range| {
                    BlockStatistics::from_integer(integers[range.clone()].iter().copied()).unwrap()
                }),
            );
            let mut expected = raw_integer.clone();
            if merged.wide_sum.is_none() {
                // Legacy summaries preserve wrapping SUM but may not prove AVG.
                assert!(merged.average().is_err());
                expected.wide_sum = None;
            }
            assert_eq!(merged, expected);
        }

        let mut unsigned = (0..257).map(|_| rng.next_u64()).collect::<Vec<_>>();
        unsigned.extend([0, 1, u64::MAX - 1, u64::MAX]);
        let mut raw_unsigned = AggregateState::empty(crate::models::UNSIGNED);
        for value in &unsigned {
            raw_unsigned.add_unsigned(*value);
        }
        for ranges in aggregate_partition_layouts(unsigned.len()) {
            let merged = merge_statistics(
                crate::models::UNSIGNED,
                ranges.iter().map(|range| {
                    BlockStatistics::from_unsigned(unsigned[range.clone()].iter().copied()).unwrap()
                }),
            );
            let mut expected = raw_unsigned.clone();
            if merged.wide_sum.is_none() {
                assert!(merged.average().is_err());
                expected.wide_sum = None;
            }
            assert_eq!(merged, expected);
        }

        let mut floats = (0..257)
            .map(|index| {
                let magnitude = (rng.next_u64() % 2_000_001) as f64 - 1_000_000.0;
                magnitude
                    * match index % 4 {
                        0 => 1.0e-100,
                        1 => 1.0e-10,
                        2 => 1.0e10,
                        _ => 1.0e100,
                    }
            })
            .collect::<Vec<_>>();
        floats.extend([-0.0, 0.0, f64::MIN_POSITIVE, -f64::MIN_POSITIVE]);
        let mut raw_float = AggregateState::empty(crate::models::FLOAT);
        for value in &floats {
            raw_float.add_float(*value);
        }
        for ranges in aggregate_partition_layouts(floats.len()) {
            let merged = merge_statistics(
                crate::models::FLOAT,
                ranges.iter().map(|range| {
                    BlockStatistics::from_float(floats[range.clone()].iter().copied()).unwrap()
                }),
            );
            assert_float_state(&floats, &ranges, raw_float.clone(), merged);
        }

        let special_floats = [
            f64::from_bits(0xfff8_0000_0000_0001),
            f64::NEG_INFINITY,
            -0.0,
            0.0,
            1.0,
            f64::INFINITY,
            f64::from_bits(0x7ff8_0000_0000_0001),
        ];
        let mut raw_special = AggregateState::empty(crate::models::FLOAT);
        for value in special_floats {
            raw_special.add_float(value);
        }
        for ranges in aggregate_partition_layouts(special_floats.len()) {
            let merged = merge_statistics(
                crate::models::FLOAT,
                ranges.iter().map(|range| {
                    BlockStatistics::from_float(special_floats[range.clone()].iter().copied())
                        .unwrap()
                }),
            );
            assert_float_state(&special_floats, &ranges, raw_special.clone(), merged);
        }

        for ranges in aggregate_partition_layouts(257) {
            let merged = merge_statistics(
                crate::models::BOOL,
                ranges.iter().map(|range| {
                    BlockStatistics::from_bool(std::iter::repeat_n(false, range.len())).unwrap()
                }),
            );
            assert_eq!(merged.count, 257);
            assert_eq!(merged.values, Some(AggregateValues::CountOnly));

            let merged = merge_statistics(
                crate::models::STRING,
                ranges
                    .iter()
                    .map(|range| BlockStatistics::count_only(range.len()).unwrap()),
            );
            assert_eq!(merged.count, 257);
            assert_eq!(merged.values, Some(AggregateValues::CountOnly));
        }
    }

    #[test]
    fn legacy_statistics_require_exact_sums_and_requested_endpoints() {
        use crate::models::{BOOL, FLOAT, INTEGER, STRING, UNSIGNED};
        use crate::query::storage_ir::AggregateWideSum;

        let entry = |statistics| super::IndexEntry {
            min_time: 10,
            max_time: 20,
            statistics,
            ..Default::default()
        };
        let exact = entry(BlockStatistics::from_integer([i64::MAX, i64::MAX]));
        let state = super::Engine::aggregate_state_from_entry(INTEGER, &exact, true)
            .unwrap()
            .unwrap();
        assert_eq!(
            state.wide_sum,
            Some(AggregateWideSum::Integer(i128::from(i64::MAX) * 2))
        );
        assert_eq!(state.first.as_ref().unwrap().time, 10);
        assert_eq!(state.last.as_ref().unwrap().time, 20);
        assert_eq!(state.average().unwrap(), Some(i64::MAX as f64));
        assert!(super::Engine::aggregate_state_from_entry(FLOAT, &exact, false).is_err());

        for (field_type, statistics) in [
            (INTEGER, BlockStatistics::from_integer([i64::MIN, 0, i64::MAX])),
            (UNSIGNED, BlockStatistics::from_unsigned([0, u64::MAX, u64::MAX])),
        ] {
            for track_endpoints in [false, true] {
                assert!(
                    super::Engine::aggregate_state_from_entry(
                        field_type, &entry(statistics), track_endpoints,
                    )
                    .unwrap()
                    .is_none()
                );
            }
        }
        let count = entry(BlockStatistics::count_only(2));
        assert!(
            super::Engine::aggregate_state_from_entry(STRING, &count, false)
                .unwrap()
                .is_some()
        );
        assert!(
            super::Engine::aggregate_state_from_entry(STRING, &count, true)
                .unwrap()
                .is_none()
        );
        assert!(
            super::Engine::aggregate_state_from_statistics(
                STRING, count.statistics.unwrap(), 10, 20, true,
            )
            .is_err()
        );
        assert!(
            super::Engine::aggregate_state_from_entry(INTEGER, &entry(None), false)
                .unwrap()
                .is_none()
        );
        let boolean = entry(BlockStatistics::from_bool([true, false]));
        assert!(
            super::Engine::aggregate_state_from_entry(BOOL, &boolean, true)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn aggregate_integer_average_falls_back_for_ambiguous_legacy_statistics() {
        for vlog in [false, true] {
            let mut engine = if vlog {
                must_open_vlog_engine("tsi1".into())
            } else {
                must_open_engine("tsi1".into())
            };
            for (field, typ) in [
                ("mixed", crate::models::INTEGER),
                ("steady", crate::models::INTEGER),
                ("unsigned", crate::models::UNSIGNED),
            ] {
                engine
                    .engine
                    .measurement_fields(b"average")
                    .create_field_if_not_exists(field.as_bytes(), typ)
                    .unwrap();
            }
            let mut signed = AggregateState::empty(crate::models::INTEGER);
            let mut unsigned = AggregateState::empty(crate::models::UNSIGNED);
            let mut steady = AggregateState::empty(crate::models::INTEGER);
            let lines = (0..4500)
                .map(|time| {
                    let i = if time % 3 == 0 { i64::MIN } else { i64::MAX };
                    let u = if time % 3 == 0 { 0 } else { u64::MAX };
                    signed.add_integer(i);
                    unsigned.add_unsigned(u);
                    steady.add_integer(i64::MAX);
                    format!(
                        "average,host=A mixed={i}i,steady={}i,unsigned={u}u {time}",
                        i64::MAX
                    )
                })
                .collect::<Vec<_>>();
            engine
                .write_points_string(&lines.iter().map(String::as_str).collect::<Vec<_>>())
                .unwrap();
            engine.must_write_snapshot();
            for (field, expected) in [
                ("mixed", signed),
                ("steady", steady),
                ("unsigned", unsigned),
            ] {
                let result = engine
                    .engine
                    .aggregate_scan(AggregateRequest {
                        measurement: MeasurementRef::new("average"),
                        field: field.into(),
                        time_range: TimeRange::new(0, 4499),
                        aggregates: vec![AggregateKind::Avg],
                    })
                    .unwrap()
                    .unwrap();
                assert_eq!(result.state, expected);
                assert_eq!(result.state.average(), expected.average());
                if !vlog {
                    if field == "steady" {
                        assert!(result.metrics.statistics_blocks > 0);
                        assert_eq!(result.metrics.decoded_blocks, 0);
                    } else {
                        assert_eq!(result.metrics.statistics_blocks, 0);
                        assert!(result.metrics.decoded_blocks > 0);
                    }
                }
                let grouped = engine
                    .engine
                    .grouped_aggregate_scan(GroupedAggregateRequest {
                        measurement: MeasurementRef::new("average"),
                        targets: vec![GroupedAggregateTarget {
                            field: field.into(),
                            expected_field_type: expected.field_type,
                            aggregates: vec![AggregateKind::Avg],
                        }],
                        time_range: TimeRange::new(0, 4499),
                        window: GroupedAggregateWindow {
                            duration_ns: 4500,
                            origin_ns: 0,
                        },
                        group_tags: vec![],
                        tag_filters: vec![],
                        cancellation: None,
                    })
                    .unwrap()
                    .unwrap();
                assert_eq!(grouped.groups.len(), 1);
                assert_eq!(grouped.groups[0].states[0].wide_sum, expected.wide_sum);
                assert_eq!(grouped.groups[0].states[0].average(), expected.average());
            }
            engine.close();
        }
    }

    #[test]
    fn aggregate_scan_uses_full_block_statistics_and_decodes_partial_blocks() {
        let mut engine = must_open_engine("tsi1".to_string());
        engine
            .engine
            .measurement_fields(b"cpu")
            .create_field_if_not_exists(b"value", crate::models::INTEGER)
            .unwrap();

        let lines = (0..9_500_i64)
            .map(|timestamp| format!("cpu,host=A value={}i {timestamp}", timestamp % 17 - 8))
            .collect::<Vec<_>>();
        let refs = lines.iter().map(String::as_str).collect::<Vec<_>>();
        engine.write_points_string(&refs).unwrap();
        engine.must_write_snapshot();

        let request = AggregateRequest {
            measurement: MeasurementRef::new("cpu"),
            field: "value".to_string(),
            time_range: TimeRange::new(1_000, 8_500),
            aggregates: vec![
                AggregateKind::Count,
                AggregateKind::Sum,
                AggregateKind::Min,
                AggregateKind::Max,
                AggregateKind::Avg,
            ],
        };
        let result = engine.engine.aggregate_scan(request).unwrap().unwrap();
        let expected = (1_000..=8_500_i64)
            .map(|timestamp| timestamp % 17 - 8)
            .collect::<Vec<_>>();
        assert_eq!(result.state.count, expected.len() as u64);
        assert_eq!(
            result.state.values,
            Some(AggregateValues::Integer {
                sum: expected.iter().copied().fold(0_i64, i64::wrapping_add),
                min: *expected.iter().min().unwrap(),
                max: *expected.iter().max().unwrap(),
            })
        );
        assert_eq!(result.metrics.candidate_blocks, 3);
        assert_eq!(result.metrics.statistics_blocks, 1);
        assert_eq!(result.metrics.decoded_blocks, 2);
        assert_eq!(result.metrics.raw_scans, 0);
        assert!(!result.metrics.used_fallback);

        let key = format!("cpu,host=A{}value", super::KEY_FIELD_SEPARATOR);
        let readers = {
            let inner = engine.engine.inner.read();
            inner.file_store.readers()
        };
        let mut entries = readers
            .iter()
            .flat_map(|reader| reader.entries(key.as_bytes()).into_iter())
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.min_time);
        assert_eq!(entries.len(), 3);
        let ranges = [
            (entries[0].min_time, entries[0].max_time, (1, 1, 0)),
            (entries[0].min_time + 1, entries[0].max_time - 1, (1, 0, 1)),
            (entries[0].min_time, entries[1].max_time, (2, 2, 0)),
            (entries[0].min_time + 1, entries[2].max_time - 1, (3, 1, 2)),
            (10_000, 11_000, (0, 0, 0)),
        ];
        for (start_time, end_time, expected_metrics) in ranges {
            let result = engine
                .engine
                .aggregate_scan(AggregateRequest {
                    measurement: MeasurementRef::new("cpu"),
                    field: "value".to_string(),
                    time_range: TimeRange::new(start_time, end_time),
                    aggregates: vec![
                        AggregateKind::Count,
                        AggregateKind::Sum,
                        AggregateKind::Min,
                        AggregateKind::Max,
                        AggregateKind::Avg,
                    ],
                })
                .unwrap()
                .unwrap();
            let expected = (0..9_500_i64)
                .filter(|timestamp| start_time <= *timestamp && *timestamp <= end_time)
                .map(|timestamp| timestamp % 17 - 8)
                .collect::<Vec<_>>();
            assert_eq!(result.state.count, expected.len() as u64);
            assert_eq!(
                result.state.values,
                (!expected.is_empty()).then(|| AggregateValues::Integer {
                    sum: expected.iter().copied().fold(0_i64, i64::wrapping_add),
                    min: *expected.iter().min().unwrap(),
                    max: *expected.iter().max().unwrap(),
                })
            );
            assert_eq!(
                (
                    result.metrics.candidate_blocks,
                    result.metrics.statistics_blocks,
                    result.metrics.decoded_blocks,
                ),
                expected_metrics
            );
            assert!(!result.metrics.used_fallback);
        }

        engine
            .write_points_string(&["cpu,host=A value=99i 10000"])
            .unwrap();
        let result = engine
            .engine
            .aggregate_scan(AggregateRequest {
                measurement: MeasurementRef::new("cpu"),
                field: "value".to_string(),
                time_range: TimeRange::new(1_000, 10_000),
                aggregates: vec![AggregateKind::Count, AggregateKind::Sum],
            })
            .unwrap()
            .unwrap();
        assert!(result.metrics.used_fallback);
        assert_eq!(result.metrics.raw_scans, 1);
        assert_eq!(result.state.count, 8_500 + 1);

        engine.close();
    }

    #[test]
    fn aggregate_scan_first_last_uses_statistics_and_decodes_only_boundaries() {
        let mut engine = must_open_engine("tsi1".to_string());
        engine
            .engine
            .measurement_fields(b"endpoint")
            .create_field_if_not_exists(b"value", crate::models::INTEGER)
            .unwrap();

        let lines = (0..9_500_i64)
            .map(|timestamp| format!("endpoint,host=A value={}i {timestamp}", timestamp % 97 - 48))
            .collect::<Vec<_>>();
        let refs = lines.iter().map(String::as_str).collect::<Vec<_>>();
        engine.write_points_string(&refs).unwrap();
        engine.must_write_snapshot();

        let request = AggregateRequest {
            measurement: MeasurementRef::new("endpoint"),
            field: "value".to_string(),
            time_range: TimeRange::new(500, 9_000),
            aggregates: vec![AggregateKind::First, AggregateKind::Last],
        };
        let raw = forced_raw_aggregate_state(&engine.engine, &request, crate::models::INTEGER);
        let result = engine.engine.aggregate_scan(request).unwrap().unwrap();
        assert_eq!(result.state, raw);
        assert_eq!(
            result.state.first,
            Some(AggregatePoint {
                time: 500,
                value: AggregatePointValue::Integer(500 % 97 - 48),
            })
        );
        assert_eq!(
            result.state.last,
            Some(AggregatePoint {
                time: 9_000,
                value: AggregatePointValue::Integer(9_000 % 97 - 48),
            })
        );
        assert_eq!(result.metrics.candidate_blocks, 3);
        assert_eq!(result.metrics.statistics_blocks, 1);
        assert_eq!(result.metrics.decoded_blocks, 2);
        assert_eq!(result.metrics.raw_scans, 0);

        let one = engine
            .engine
            .aggregate_scan(AggregateRequest {
                measurement: MeasurementRef::new("endpoint"),
                field: "value".to_string(),
                time_range: TimeRange::new(4_000, 4_000),
                aggregates: vec![AggregateKind::First, AggregateKind::Last],
            })
            .unwrap()
            .unwrap();
        assert_eq!(one.state.first, one.state.last);
        assert_eq!(one.state.first.as_ref().unwrap().time, 4_000);
        assert_eq!(one.metrics.statistics_blocks, 0);
        assert_eq!(one.metrics.decoded_blocks, 1);

        let empty = engine
            .engine
            .aggregate_scan(AggregateRequest {
                measurement: MeasurementRef::new("endpoint"),
                field: "value".to_string(),
                time_range: TimeRange::new(10_000, 11_000),
                aggregates: vec![AggregateKind::First, AggregateKind::Last],
            })
            .unwrap()
            .unwrap();
        assert_eq!(empty.state.count, 0);
        assert_eq!(empty.state.first, None);
        assert_eq!(empty.state.last, None);

        engine.close();
    }

    #[test]
    fn aggregate_scan_boolean_uses_endpoint_statistics_and_string_decodes() {
        let mut engine = must_open_engine("tsi1".to_string());
        let fields = engine.engine.measurement_fields(b"typed_endpoint");
        fields
            .create_field_if_not_exists(b"flag", crate::models::BOOL)
            .unwrap();
        fields
            .create_field_if_not_exists(b"label", crate::models::STRING)
            .unwrap();

        let lines = (0..4_500_i64)
            .map(|timestamp| {
                format!(
                    "typed_endpoint,host=A flag={},label=\"v{}\" {timestamp}",
                    timestamp % 2 == 0,
                    timestamp % 11
                )
            })
            .collect::<Vec<_>>();
        let refs = lines.iter().map(String::as_str).collect::<Vec<_>>();
        engine.write_points_string(&refs).unwrap();
        engine.must_write_snapshot();

        let boolean = engine
            .engine
            .aggregate_scan(AggregateRequest {
                measurement: MeasurementRef::new("typed_endpoint"),
                field: "flag".to_string(),
                time_range: TimeRange::new(0, 4_499),
                aggregates: vec![AggregateKind::First, AggregateKind::Last],
            })
            .unwrap()
            .unwrap();
        assert_eq!(
            boolean.state.first,
            Some(AggregatePoint {
                time: 0,
                value: AggregatePointValue::Boolean(true),
            })
        );
        assert_eq!(
            boolean.state.last,
            Some(AggregatePoint {
                time: 4_499,
                value: AggregatePointValue::Boolean(false),
            })
        );
        assert_eq!(boolean.metrics.statistics_blocks, 2);
        assert_eq!(boolean.metrics.decoded_blocks, 0);

        let string_request = AggregateRequest {
            measurement: MeasurementRef::new("typed_endpoint"),
            field: "label".to_string(),
            time_range: TimeRange::new(0, 4_499),
            aggregates: vec![AggregateKind::First, AggregateKind::Last],
        };
        let raw =
            forced_raw_aggregate_state(&engine.engine, &string_request, crate::models::STRING);
        let string = engine
            .engine
            .aggregate_scan(string_request)
            .unwrap()
            .unwrap();
        assert_eq!(string.state, raw);
        assert_eq!(string.metrics.statistics_blocks, 0);
        assert_eq!(string.metrics.decoded_blocks, 2);
        assert_eq!(
            string.state.first,
            Some(AggregatePoint {
                time: 0,
                value: AggregatePointValue::String("v0".to_string()),
            })
        );
        assert_eq!(
            string.state.last,
            Some(AggregatePoint {
                time: 4_499,
                value: AggregatePointValue::String("v0".to_string()),
            })
        );

        engine.close();
    }

    #[test]
    fn aggregate_scan_first_last_vlog_fallback_matches_raw_visibility() {
        let mut engine = must_open_vlog_engine("tsi1".to_string());
        engine
            .engine
            .measurement_fields(b"vlog_endpoints")
            .create_field_if_not_exists(b"value", crate::models::INTEGER)
            .unwrap();
        engine
            .write_points_string(&[
                "vlog_endpoints,host=A value=40i 10",
                "vlog_endpoints,host=A value=20i 20",
                "vlog_endpoints,host=A value=30i 30",
            ])
            .unwrap();
        engine.must_write_snapshot();

        let request = all_aggregate_request("vlog_endpoints", "value", 10, 30);
        let raw = forced_raw_aggregate_state(&engine.engine, &request, crate::models::INTEGER);
        let result = engine.engine.aggregate_scan(request).unwrap().unwrap();

        assert!(result.metrics.used_fallback);
        assert_eq!(result.metrics.raw_scans, 1);
        assert_eq!(result.state, raw);
        assert_eq!(
            result.state.first.as_ref().unwrap().value,
            AggregatePointValue::Integer(40)
        );
        assert_eq!(
            result.state.last.as_ref().unwrap().value,
            AggregatePointValue::Integer(30)
        );

        engine.close();
    }

    #[test]
    fn aggregate_scan_float_tsbs_shape_uses_sum_and_avg_statistics() {
        let mut engine = must_open_engine("tsi1".to_string());
        engine
            .engine
            .measurement_fields(b"tsbs_cpu")
            .create_field_if_not_exists(b"usage", crate::models::FLOAT)
            .unwrap();

        let point_count = 17_000_i64;
        let lines = ["A", "B"]
            .into_iter()
            .flat_map(|host| {
                (0..point_count).map(move |second| {
                    let timestamp_ns = second * 1_000_000_000;
                    let value = timestamp_ns as f64 / 1_000_000_000_000_000_f64;
                    format!("tsbs_cpu,host={host} usage={value} {timestamp_ns}")
                })
            })
            .collect::<Vec<_>>();
        let refs = lines.iter().map(String::as_str).collect::<Vec<_>>();
        engine.write_points_string(&refs).unwrap();
        engine.must_write_snapshot();

        let key = format!("tsbs_cpu,host=A{}usage", super::KEY_FIELD_SEPARATOR);
        let readers = {
            let inner = engine.engine.inner.read();
            inner.file_store.readers()
        };
        let mut entries = readers
            .iter()
            .flat_map(|reader| reader.entries(key.as_bytes()).into_iter())
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.min_time);
        assert_eq!(entries.len(), 5);

        let start_time = entries[0].min_time + 1;
        let end_time = entries[4].max_time - 1;
        let result = engine
            .engine
            .aggregate_scan(AggregateRequest {
                measurement: MeasurementRef::new("tsbs_cpu"),
                field: "usage".to_string(),
                time_range: TimeRange::new(start_time, end_time),
                aggregates: vec![
                    AggregateKind::Count,
                    AggregateKind::Sum,
                    AggregateKind::Min,
                    AggregateKind::Max,
                    AggregateKind::Avg,
                ],
            })
            .unwrap()
            .unwrap();

        let mut expected_count = 0_u64;
        let mut expected_sum = 0.0_f64;
        let mut absolute_sum = 0.0_f64;
        let mut expected_min = f64::INFINITY;
        let mut expected_max = f64::NEG_INFINITY;
        for _host in ["A", "B"] {
            for second in 0..point_count {
                let timestamp_ns = second * 1_000_000_000;
                if !(start_time..=end_time).contains(&timestamp_ns) {
                    continue;
                }
                let value = timestamp_ns as f64 / 1_000_000_000_000_000_f64;
                expected_count += 1;
                expected_sum += value;
                absolute_sum += value.abs();
                expected_min = expected_min.min(value);
                expected_max = expected_max.max(value);
            }
        }
        assert_eq!(result.state.count, expected_count);
        let Some(AggregateValues::Float { sum, min, max }) = result.state.values else {
            panic!("expected float aggregate state")
        };
        let tolerance = absolute_sum.max(1.0) * f64::EPSILON * expected_count.max(1) as f64 * 8.0;
        assert!(
            (sum - expected_sum).abs() <= tolerance,
            "float SUM differed: got={sum:?} expected={expected_sum:?} tolerance={tolerance:?}"
        );
        assert_eq!(min.to_bits(), expected_min.to_bits());
        assert_eq!(max.to_bits(), expected_max.to_bits());
        assert!(
            (sum / expected_count as f64 - expected_sum / expected_count as f64).abs()
                <= tolerance / expected_count as f64,
            "float AVG differed"
        );
        assert_eq!(result.metrics.candidate_blocks, 10);
        assert_eq!(result.metrics.statistics_blocks, 6);
        assert_eq!(result.metrics.decoded_blocks, 4);
        assert!(!result.metrics.used_fallback);

        engine.close();
    }

    #[test]
    fn aggregate_scan_float_preserves_multi_file_and_unsafe_fallback_rules() {
        let mut engine = must_open_engine("tsi1".to_string());
        engine
            .engine
            .measurement_fields(b"float_visibility")
            .create_field_if_not_exists(b"value", crate::models::FLOAT)
            .unwrap();

        let first = (0..1_000_i64)
            .map(|timestamp| format!("float_visibility,host=A value=1.5 {timestamp}"))
            .collect::<Vec<_>>();
        let refs = first.iter().map(String::as_str).collect::<Vec<_>>();
        engine.write_points_string(&refs).unwrap();
        engine.must_write_snapshot();

        engine
            .write_points_string(&["float_visibility,host=A value=2.5 1000"])
            .unwrap();
        let request = AggregateRequest {
            measurement: MeasurementRef::new("float_visibility"),
            field: "value".to_string(),
            time_range: TimeRange::new(0, 1_000),
            aggregates: vec![
                AggregateKind::Count,
                AggregateKind::Sum,
                AggregateKind::Avg,
                AggregateKind::First,
                AggregateKind::Last,
            ],
        };
        let cache_result = engine
            .engine
            .aggregate_scan(request.clone())
            .unwrap()
            .unwrap();
        assert!(cache_result.metrics.used_fallback);
        assert_eq!(cache_result.metrics.raw_scans, 1);
        assert_eq!(cache_result.state.count, 1_001);
        assert_eq!(cache_result.state.first.as_ref().unwrap().time, 0);
        assert_eq!(cache_result.state.last.as_ref().unwrap().time, 1_000);

        engine.must_write_snapshot();
        let multi_file = engine
            .engine
            .aggregate_scan(request.clone())
            .unwrap()
            .unwrap();
        assert!(!multi_file.metrics.used_fallback);
        assert_eq!(multi_file.metrics.candidate_blocks, 2);
        assert_eq!(multi_file.metrics.statistics_blocks, 2);
        assert_eq!(multi_file.metrics.decoded_blocks, 0);
        assert_eq!(multi_file.metrics.raw_scans, 0);
        assert_eq!(multi_file.state.count, 1_001);
        assert_eq!(multi_file.state.first.as_ref().unwrap().time, 0);
        assert_eq!(multi_file.state.last.as_ref().unwrap().time, 1_000);
        assert_eq!(
            multi_file.state.values,
            Some(AggregateValues::Float {
                sum: 1_502.5,
                min: 1.5,
                max: 2.5,
            })
        );

        engine
            .engine
            .delete_series_range_by_measurement(
                b"float_visibility",
                Some(|_: &[u8], _: &crate::models::Tags| (100, 199, true)),
            )
            .unwrap();
        let tombstoned = engine
            .engine
            .aggregate_scan(request.clone())
            .unwrap()
            .unwrap();
        assert!(tombstoned.metrics.used_fallback);
        assert_eq!(tombstoned.metrics.raw_scans, 1);
        assert_eq!(tombstoned.state.count, 901);
        assert_eq!(tombstoned.state.first.as_ref().unwrap().time, 0);
        assert_eq!(tombstoned.state.last.as_ref().unwrap().time, 1_000);
        assert_eq!(
            tombstoned.state.values,
            Some(AggregateValues::Float {
                sum: 1_352.5,
                min: 1.5,
                max: 2.5,
            })
        );

        let replacement = (0..100_i64)
            .map(|timestamp| format!("float_visibility,host=A value=3.0 {timestamp}"))
            .collect::<Vec<_>>();
        let refs = replacement.iter().map(String::as_str).collect::<Vec<_>>();
        engine.write_points_string(&refs).unwrap();
        engine.must_write_snapshot();
        let overlapping = engine.engine.aggregate_scan(request).unwrap().unwrap();
        assert!(overlapping.metrics.used_fallback);
        assert_eq!(overlapping.metrics.raw_scans, 1);
        assert_eq!(overlapping.state.count, 901);
        assert_eq!(overlapping.state.first.as_ref().unwrap().time, 0);
        assert_eq!(overlapping.state.last.as_ref().unwrap().time, 1_000);
        assert_eq!(
            overlapping.state.values,
            Some(AggregateValues::Float {
                sum: 1_502.5,
                min: 1.5,
                max: 3.0,
            })
        );

        engine.close();
    }

    #[test]
    fn aggregate_scan_localizes_overlap_and_falls_back_for_tombstones() {
        let mut engine = must_open_engine("tsi1".to_string());
        engine
            .engine
            .measurement_fields(b"cpu")
            .create_field_if_not_exists(b"value", crate::models::INTEGER)
            .unwrap();

        let first = (0..1_000_i64)
            .map(|timestamp| format!("cpu,host=A value=1i {timestamp}"))
            .collect::<Vec<_>>();
        let refs = first.iter().map(String::as_str).collect::<Vec<_>>();
        engine.write_points_string(&refs).unwrap();
        engine.must_write_snapshot();

        let replacement = (0..1_000_i64)
            .map(|timestamp| format!("cpu,host=A value=2i {timestamp}"))
            .collect::<Vec<_>>();
        let refs = replacement.iter().map(String::as_str).collect::<Vec<_>>();
        engine.write_points_string(&refs).unwrap();
        engine.must_write_snapshot();

        let request = all_aggregate_request("cpu", "value", 0, 999);
        let result = engine
            .engine
            .aggregate_scan(request.clone())
            .unwrap()
            .unwrap();
        assert!(!result.metrics.used_fallback);
        assert_eq!(result.metrics.candidate_blocks, 2);
        assert_eq!(result.metrics.statistics_blocks, 0);
        assert_eq!(result.metrics.decoded_blocks, 0);
        assert_eq!(result.metrics.overlap_raw_blocks, 2);
        assert_eq!(result.metrics.overlap_intervals, 1);
        assert_eq!(result.metrics.raw_scans, 1);
        assert_eq!(result.state.count, 1_000);
        assert_eq!(result.state.first.as_ref().unwrap().time, 0);
        assert_eq!(result.state.last.as_ref().unwrap().time, 999);
        assert_eq!(
            result.state.values,
            Some(AggregateValues::Integer {
                sum: 2_000,
                min: 2,
                max: 2,
            })
        );

        engine
            .engine
            .delete_series_range_by_measurement(
                b"cpu",
                Some(|_: &[u8], _: &crate::models::Tags| (100, 199, true)),
            )
            .unwrap();
        let result = engine.engine.aggregate_scan(request).unwrap().unwrap();
        assert!(result.metrics.used_fallback);
        assert_eq!(result.metrics.raw_scans, 1);
        assert_eq!(result.state.count, 900);
        assert_eq!(result.state.first.as_ref().unwrap().time, 0);
        assert_eq!(result.state.last.as_ref().unwrap().time, 999);
        assert_eq!(
            result.state.values,
            Some(AggregateValues::Integer {
                sum: 1_800,
                min: 2,
                max: 2,
            })
        );

        engine.close();
    }

    #[test]
    fn aggregate_scan_localizes_one_middle_overlap_with_partial_query_edges() {
        let mut engine = must_open_engine("tsi1".to_string());
        engine
            .engine
            .measurement_fields(b"localized")
            .create_field_if_not_exists(b"value", crate::models::INTEGER)
            .unwrap();

        let base = (0..20_000_i64)
            .map(|timestamp| format!("localized,host=A value=1i {timestamp}"))
            .collect::<Vec<_>>();
        let refs = base.iter().map(String::as_str).collect::<Vec<_>>();
        engine.write_points_string(&refs).unwrap();
        engine.must_write_snapshot();

        let replacement = (10_000..10_120_i64)
            .map(|timestamp| format!("localized,host=A value=9i {timestamp}"))
            .collect::<Vec<_>>();
        let refs = replacement.iter().map(String::as_str).collect::<Vec<_>>();
        engine.write_points_string(&refs).unwrap();
        engine.must_write_snapshot();

        let request = all_aggregate_request("localized", "value", 500, 19_500);
        let raw = forced_raw_aggregate_state(&engine.engine, &request, crate::models::INTEGER);
        let hybrid = engine.engine.aggregate_scan(request).unwrap().unwrap();

        assert_eq!(hybrid.state, raw);
        assert!(!hybrid.metrics.used_fallback);
        assert_eq!(hybrid.metrics.candidate_blocks, 6);
        assert_eq!(hybrid.metrics.statistics_blocks, 2);
        assert_eq!(hybrid.metrics.decoded_blocks, 3);
        assert_eq!(hybrid.metrics.overlap_intervals, 1);
        assert_eq!(hybrid.metrics.raw_scans, 1);
        assert_eq!(hybrid.metrics.overlap_raw_blocks, 2);
        assert!(hybrid.metrics.statistics_blocks > 0);
        assert!(hybrid.metrics.decoded_blocks > 0);

        engine.close();
    }

    #[test]
    fn aggregate_scan_handles_boundaries_multiple_intervals_and_spanning_overlap() {
        let mut engine = must_open_engine("tsi1".to_string());
        engine
            .engine
            .measurement_fields(b"overlap_boundaries")
            .create_field_if_not_exists(b"value", crate::models::INTEGER)
            .unwrap();

        let base = (0..28_000_i64)
            .map(|timestamp| {
                format!(
                    "overlap_boundaries,host=A value={}i {timestamp}",
                    timestamp % 31 - 15
                )
            })
            .collect::<Vec<_>>();
        let refs = base.iter().map(String::as_str).collect::<Vec<_>>();
        engine.write_points_string(&refs).unwrap();
        engine.must_write_snapshot();

        for (start, end, value) in [
            (4_000_i64, 4_499_i64, 101_i64),
            (7_500, 7_999, 102),
            (10_000, 10_499, 103),
            (12_500, 16_499, 104),
        ] {
            let replacement = (start..=end)
                .map(|timestamp| format!("overlap_boundaries,host=A value={value}i {timestamp}"))
                .collect::<Vec<_>>();
            let refs = replacement.iter().map(String::as_str).collect::<Vec<_>>();
            engine.write_points_string(&refs).unwrap();
            engine.must_write_snapshot();
        }

        let request = all_aggregate_request("overlap_boundaries", "value", 100, 27_500);
        let raw = forced_raw_aggregate_state(&engine.engine, &request, crate::models::INTEGER);
        let hybrid = engine.engine.aggregate_scan(request).unwrap().unwrap();

        assert_eq!(hybrid.state, raw);
        assert!(!hybrid.metrics.used_fallback);
        assert_eq!(hybrid.metrics.candidate_blocks, 11);
        assert_eq!(hybrid.metrics.statistics_blocks, 1);
        assert_eq!(hybrid.metrics.decoded_blocks, 6);
        assert_eq!(hybrid.metrics.overlap_intervals, 4);
        assert_eq!(hybrid.metrics.raw_scans, 4);
        assert_eq!(hybrid.metrics.overlap_raw_blocks, 9);

        engine.close();
    }

    #[test]
    fn aggregate_scan_empty_overlap_subrange_matches_raw_visibility() {
        let mut engine = must_open_engine("tsi1".to_string());
        engine
            .engine
            .measurement_fields(b"sparse_overlap")
            .create_field_if_not_exists(b"value", crate::models::INTEGER)
            .unwrap();

        engine
            .write_points_string(&[
                "sparse_overlap,host=A value=1i 0",
                "sparse_overlap,host=A value=1i 100",
            ])
            .unwrap();
        engine.must_write_snapshot();
        engine
            .write_points_string(&[
                "sparse_overlap,host=A value=2i 40",
                "sparse_overlap,host=A value=2i 60",
            ])
            .unwrap();
        engine.must_write_snapshot();

        let request = all_aggregate_request("sparse_overlap", "value", 41, 59);
        let raw = forced_raw_aggregate_state(&engine.engine, &request, crate::models::INTEGER);
        let hybrid = engine.engine.aggregate_scan(request).unwrap().unwrap();

        assert_eq!(hybrid.state, raw);
        assert_eq!(hybrid.state.count, 0);
        assert!(!hybrid.metrics.used_fallback);
        assert_eq!(hybrid.metrics.overlap_intervals, 1);
        assert_eq!(hybrid.metrics.raw_scans, 1);
        assert_eq!(hybrid.metrics.overlap_raw_blocks, 2);

        engine.close();
    }

    #[test]
    fn aggregate_scan_randomized_overlap_matches_forced_raw() {
        let mut engine = must_open_engine("tsi1".to_string());
        let fields = engine.engine.measurement_fields(b"random_overlap");
        fields
            .create_field_if_not_exists(b"integer_value", crate::models::INTEGER)
            .unwrap();
        fields
            .create_field_if_not_exists(b"float_value", crate::models::FLOAT)
            .unwrap();

        let mut rng = StdRng::seed_from_u64(0x4f56_4552_4c41_5053);
        let base = (0..9_000_i64)
            .map(|timestamp| {
                let integer_value = rng.next_u64() as i64 % 10_000;
                let float_value = integer_value as f64 / 17.0;
                format!(
                    "random_overlap,host=A integer_value={integer_value}i,float_value={float_value} {timestamp}"
                )
            })
            .collect::<Vec<_>>();
        let refs = base.iter().map(String::as_str).collect::<Vec<_>>();
        engine.write_points_string(&refs).unwrap();
        engine.must_write_snapshot();

        for _ in 0..4 {
            let start = rng.gen_range(250_i64..7_500_i64);
            let end = (start + rng.gen_range(100_i64..1_200_i64)).min(8_750);
            let replacement = (start..=end)
                .map(|timestamp| {
                    let integer_value = rng.next_u64() as i64 % 20_000;
                    let float_value = integer_value as f64 / 23.0;
                    format!(
                        "random_overlap,host=A integer_value={integer_value}i,float_value={float_value} {timestamp}"
                    )
                })
                .collect::<Vec<_>>();
            let refs = replacement.iter().map(String::as_str).collect::<Vec<_>>();
            engine.write_points_string(&refs).unwrap();
            engine.must_write_snapshot();
        }

        for _ in 0..12 {
            let start = rng.gen_range(0_i64..8_000_i64);
            let end = rng.gen_range(start..9_000_i64);
            let integer_request =
                all_aggregate_request("random_overlap", "integer_value", start, end);
            let raw_integer = forced_raw_aggregate_state(
                &engine.engine,
                &integer_request,
                crate::models::INTEGER,
            );
            let hybrid_integer = engine
                .engine
                .aggregate_scan(integer_request)
                .unwrap()
                .unwrap();
            assert_eq!(hybrid_integer.state, raw_integer);

            let float_request = all_aggregate_request("random_overlap", "float_value", start, end);
            let raw_float =
                forced_raw_aggregate_state(&engine.engine, &float_request, crate::models::FLOAT);
            let hybrid_float = engine
                .engine
                .aggregate_scan(float_request)
                .unwrap()
                .unwrap();
            assert_eq!(hybrid_float.state.count, raw_float.count);
            assert_eq!(hybrid_float.state.first, raw_float.first);
            assert_eq!(hybrid_float.state.last, raw_float.last);
            let (
                Some(AggregateValues::Float {
                    sum: hybrid_sum,
                    min: hybrid_min,
                    max: hybrid_max,
                }),
                Some(AggregateValues::Float {
                    sum: raw_sum,
                    min: raw_min,
                    max: raw_max,
                }),
            ) = (hybrid_float.state.values, raw_float.values)
            else {
                assert_eq!(hybrid_float.state.values, raw_float.values);
                continue;
            };
            assert_eq!(hybrid_min.to_bits(), raw_min.to_bits());
            assert_eq!(hybrid_max.to_bits(), raw_max.to_bits());
            let tolerance =
                raw_sum.abs().max(1.0) * f64::EPSILON * raw_float.count.max(1) as f64 * 16.0;
            assert!(
                (hybrid_sum - raw_sum).abs() <= tolerance,
                "float SUM differs beyond forward-error bound: hybrid={hybrid_sum:?} raw={raw_sum:?} tolerance={tolerance:?}"
            );
        }

        engine.close();
    }

    #[cfg(not(feature = "mock_compact_plan"))]
    #[test]
    fn rejected_compaction_plan_submission_restores_all_bookkeeping() {
        struct RejectingExecutor;

        impl RuntimeExecutor for RejectingExecutor {
            fn schedule(
                &self,
                _task: BoxFutureTask,
            ) -> std::result::Result<TaskController, SpawnError> {
                Err(SpawnError::rejected("stopping"))
            }

            fn schedule_blocking(
                &self,
                _task: BoxBlockingTask,
            ) -> std::result::Result<TaskController, SpawnError> {
                Err(SpawnError::rejected("stopping"))
            }

            fn schedule_compact(
                &self,
                _task: BoxFutureTask,
            ) -> std::result::Result<TaskController, SpawnError> {
                Err(SpawnError::rejected("stopping"))
            }

            fn schedule_blocking_compact(
                &self,
                _task: BoxBlockingTask,
            ) -> std::result::Result<TaskController, SpawnError> {
                Err(SpawnError::rejected("stopping"))
            }
        }

        let root = tempfile::tempdir().unwrap();
        let dir = Arc::new(root.path().to_path_buf());
        let executor: crate::runtime::execution::ExecutorRef = Arc::new(RejectingExecutor);
        let file_store = Arc::new(super::FileStore::new_with_executor(
            Arc::clone(&dir),
            None,
            Arc::clone(&executor),
        ));
        let planner = Arc::new(super::DefaultPlanner::new(
            Arc::clone(&file_store),
            Duration::from_secs(1),
        ));
        let group: super::CompactionGroup = vec!["000000001-000000001.tsm".to_string()].into();
        assert!(planner.acquire_for_test(std::slice::from_ref(&group)));

        let compactor = Arc::new(super::Compactor::new_with_executor(
            Arc::clone(&dir),
            Arc::new(root.path().join("vlog")),
            Arc::clone(&file_store),
            None,
            None,
            false,
            &VLogOptions::default().config,
            Arc::clone(&executor),
        ));
        let limiter = Arc::new(crate::storage::limiter::Fixed::new(1));
        let stats = Arc::new(super::EngineStatistics::default());
        let wg = WaitGroup::new();

        let consumed = super::Engine::compact_hi_priority_level(
            compactor,
            file_store,
            Arc::clone(&planner),
            Some(Arc::clone(&limiter)),
            group,
            1,
            false,
            Arc::clone(&stats),
            wg.clone(),
            1,
            executor,
        );

        assert!(consumed);
        assert!(!planner.file_in_use_for_test("000000001-000000001.tsm"));
        assert_eq!(limiter.available(), 1);
        assert_eq!(stats.tsm_compactions_active[0].load(Ordering::SeqCst), 0);
        assert_eq!(wg.waitings(), 1);
    }

    #[cfg(test)]
    static TEST_ENGINE_WRITE_FAILURE_HOOK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[cfg(test)]
    struct EngineWriteFailureHookGuard {
        _guard: std::sync::MutexGuard<'static, ()>,
    }

    #[cfg(test)]
    impl Drop for EngineWriteFailureHookGuard {
        fn drop(&mut self) {
            super::FAIL_ENGINE_WRITE_WAL_AFTER_CACHE
                .store(false, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[cfg(test)]
    fn engine_write_failure_hook_guard() -> EngineWriteFailureHookGuard {
        let guard = TEST_ENGINE_WRITE_FAILURE_HOOK_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        super::FAIL_ENGINE_WRITE_WAL_AFTER_CACHE.store(false, std::sync::atomic::Ordering::SeqCst);
        EngineWriteFailureHookGuard { _guard: guard }
    }

    #[test]
    fn test_engine_wal_failure_after_cache_write_leaves_cache_visible() {
        let _hook_guard = engine_write_failure_hook_guard();

        let mut e = Engine::new("tsi1".to_string());
        e.engine.open().unwrap();
        let key = series_field_key_bytes("cpu,host=A", "value");

        super::FAIL_ENGINE_WRITE_WAL_AFTER_CACHE.store(true, std::sync::atomic::Ordering::SeqCst);
        let err = e
            .write_points_string(&["cpu,host=A value=1.1 1000000000"])
            .unwrap_err();
        super::FAIL_ENGINE_WRITE_WAL_AFTER_CACHE.store(false, std::sync::atomic::Ordering::SeqCst);

        assert!(
            matches!(err, super::Error::WAL { .. }),
            "expected injected WAL error, got {err:?}"
        );
        assert_eq!(
            e.engine.cache().values(&key).len(),
            1,
            "failed WAL write remains cache-visible by design"
        );

        e.close();
    }

    #[test]
    fn test_engine_duplicate_write_overwrite_survives_snapshot_and_reopen() {
        let _hook_guard = engine_write_failure_hook_guard();
        let mut e = Engine::new("tsi1".to_string());
        e.engine.open().unwrap();
        e.engine
            .measurement_fields("cpu".as_bytes())
            .create_field_if_not_exists("value".as_bytes(), crate::models::FLOAT)
            .unwrap();

        e.write_points_string(&["cpu,host=A value=1.0 1000000000"])
            .unwrap();
        e.write_points_string(&["cpu,host=A value=9.0 1000000000"])
            .unwrap();

        let points = collect_float_points(&e.engine, "cpu", 1_000_000_000, 1_000_000_000);
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].value, 9.0);

        e.must_write_snapshot();
        e.reopen();
        e.engine
            .measurement_fields("cpu".as_bytes())
            .create_field_if_not_exists("value".as_bytes(), crate::models::FLOAT)
            .unwrap();

        let points = collect_float_points(&e.engine, "cpu", 1_000_000_000, 1_000_000_000);
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].value, 9.0);

        e.close();
    }

    #[test]
    fn test_engine_snapshot_logging_includes_engine_context() {
        let output = crate::common::test_logging::capture("reefsdb::tsdb::snapshot=info", || {
            let mut e = Engine::new("tsi1".to_string());
            e.engine.enable_compactions_on_open = false;
            e.engine.open().unwrap();
            e.write_points_string(&["cpu,host=A value=1.0 1000000000"])
                .unwrap();
            e.must_write_snapshot();
            e.close();
        });

        assert!(output.contains("writing snapshot"));
        assert!(output.contains("snapshot complete"));
        assert!(output.contains("engine_id=1"));
        assert!(output.contains("shard_id=1"));
        assert!(output.contains("cache_bytes="));
        assert!(output.contains("duration_ms="));
    }

    fn bounded_snapshot_engine(executor: ExecutorRef, max_vlog_writers: i32) -> Engine {
        let mut engine = Engine::with_path_vlog_executor(
            "tsi1".to_string(),
            "",
            true,
            max_vlog_writers,
            Some(executor),
        );
        engine.engine.enable_compactions_on_open = false;
        engine.engine.open().unwrap();
        engine
            .write_points_string(&["cpu,host=A value=1.0 1000000000"])
            .unwrap();
        engine
    }

    fn snapshot_with_watchdog(engine: &Engine) {
        let inner = Arc::clone(&engine.engine.inner);
        let (done, result) = std::sync::mpsc::sync_channel(1);
        let task = std::thread::spawn(move || {
            let _ = done.send(super::Engine::write_snapshot(true, inner));
        });
        result
            .recv_timeout(Duration::from_secs(10))
            .expect("snapshot stalled on bounded compact-blocking capacity")
            .unwrap();
        task.join().unwrap();
        assert_eq!(engine.engine.cache().size(), 0);
    }

    fn close_with_watchdog(mut engine: Engine) {
        let (done, closed) = std::sync::mpsc::sync_channel(1);
        let task = std::thread::spawn(move || {
            engine.close();
            let _ = done.send(());
        });
        closed
            .recv_timeout(Duration::from_secs(10))
            .expect("engine close stalled after bounded snapshot");
        task.join().unwrap();
    }

    #[test]
    fn snapshot_vlog_completes_with_one_compact_blocking_worker() {
        let executor: ExecutorRef = Arc::new(
            DefaultTokioExecutor::new(DefaultTokioExecutorOptions::new(2, 2, 1, 1)).unwrap(),
        );
        let engine = bounded_snapshot_engine(executor, 1);
        snapshot_with_watchdog(&engine);
        close_with_watchdog(engine);
    }

    #[test]
    fn snapshot_vlog_multi_writer_completes_below_writer_demand() {
        let executor: ExecutorRef = Arc::new(
            DefaultTokioExecutor::new(DefaultTokioExecutorOptions::new(2, 2, 1, 1)).unwrap(),
        );
        let engine = bounded_snapshot_engine(executor, 3);
        snapshot_with_watchdog(&engine);
        close_with_watchdog(engine);
    }

    #[test]
    fn concurrent_snapshot_parents_do_not_starve_shared_compact_lane() {
        let executor: ExecutorRef = Arc::new(
            DefaultTokioExecutor::new(DefaultTokioExecutorOptions::new(2, 2, 1, 1)).unwrap(),
        );
        let first = bounded_snapshot_engine(Arc::clone(&executor), 2);
        let second = bounded_snapshot_engine(executor, 2);
        let first_inner = Arc::clone(&first.engine.inner);
        let second_inner = Arc::clone(&second.engine.inner);
        let (done, result) = std::sync::mpsc::sync_channel(2);
        for inner in [first_inner, second_inner] {
            let done = done.clone();
            std::thread::spawn(move || {
                let _ = done.send(super::Engine::write_snapshot(true, inner));
            });
        }
        for _ in 0..2 {
            result
                .recv_timeout(Duration::from_secs(10))
                .expect("concurrent snapshots starved the shared compact lane")
                .unwrap();
        }
        close_with_watchdog(first);
        close_with_watchdog(second);
    }

    fn bounded_vlog_compaction_engine(
        executor: ExecutorRef,
        plan: &str,
        level_1_min_files: i64,
    ) -> Engine {
        let mut engine = Engine::with_path_vlog_executor_options(
            "tsi1".to_string(),
            "",
            true,
            1,
            Some(executor),
            Some((plan, level_1_min_files)),
        );
        engine.engine.enable_compactions_on_open = false;
        engine.engine.open().unwrap();

        let snapshots = if plan == "default" {
            8
        } else {
            level_1_min_files as usize
        };
        for i in 0..snapshots {
            let point = format!("cpu,host=A value={} {}", i + 1, (i + 1) * 1_000_000_000);
            engine.write_points_string(&[&point]).unwrap();
            engine.must_write_snapshot();
        }

        engine
    }

    fn wait_for_vlog_level_one_compaction(engine: &Engine) {
        engine.engine.enable_vlog_compactions(false);
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        while engine.engine.stats.vlog_compactions[0].load(Ordering::SeqCst) == 0
            && std::time::Instant::now() < deadline
        {
            assert_eq!(
                engine.engine.stats.vlog_compaction_errors[0].load(Ordering::SeqCst),
                0,
                "VLog level-one compaction failed"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(
            engine.engine.stats.vlog_compactions[0].load(Ordering::SeqCst) > 0,
            "VLog supervisor/strategy/read-child topology did not complete"
        );
    }

    #[test]
    fn vlog_compaction_supervisor_completes_with_one_compact_blocking_worker() {
        let executor: ExecutorRef = Arc::new(
            DefaultTokioExecutor::new(DefaultTokioExecutorOptions::new(2, 2, 1, 1)).unwrap(),
        );
        let engine = bounded_vlog_compaction_engine(executor, "lazy", 2);
        wait_for_vlog_level_one_compaction(&engine);
        close_with_watchdog(engine);
    }

    #[test]
    fn vlog_compaction_supervisor_strategy_reads_complete_with_two_workers() {
        let executor: ExecutorRef = Arc::new(
            DefaultTokioExecutor::new(DefaultTokioExecutorOptions::new(2, 2, 1, 2)).unwrap(),
        );
        let engine = bounded_vlog_compaction_engine(executor, "default", 8);
        wait_for_vlog_level_one_compaction(&engine);
        close_with_watchdog(engine);
    }

    #[test]
    fn snapshot_supervisor_production_body_panic_releases_wait_group() {
        let executor: ExecutorRef = Arc::new(
            DefaultTokioExecutor::new(DefaultTokioExecutorOptions::new(2, 2, 1, 1)).unwrap(),
        );
        let mut engine =
            Engine::with_path_vlog_executor("tsi1".to_string(), "", false, 1, Some(executor));
        engine.engine.enable_compactions_on_open = false;
        engine.engine.open().unwrap();
        engine
            .engine
            .inner
            .read()
            .panic_snapshot_supervisor_body
            .store(true, Ordering::SeqCst);

        engine.engine.enable_snapshot_compactions();
        engine.engine.disable_snapshot_compactions();

        let inner = engine.engine.inner.read();
        assert!(inner.snap_done.is_none());
        assert_eq!(inner.snap_wg.waitings(), 1);
        drop(inner);
        close_with_watchdog(engine);
    }

    #[test]
    fn test_compaction_lifecycle_logging_includes_plan_context() {
        let output = crate::common::test_logging::capture("reefsdb::tsdb::compaction=info", || {
            super::log_compaction_started(23, 2, false, 4);
            super::log_compaction_completed(23, 2, 4, std::time::Duration::from_millis(19));
        });

        assert!(output.contains("starting compaction"));
        assert!(output.contains("compaction complete"));
        assert!(output.contains("engine_id=23"));
        assert!(output.contains("shard_id=23"));
        assert!(output.contains("level=2"));
        assert!(output.contains("input_file_count=4"));
        assert!(output.contains("duration_ms=19"));
    }

    #[test]
    fn test_engine_concurrent_overlapping_write_points_preserve_duplicates() {
        let _hook_guard = engine_write_failure_hook_guard();
        let mut e = Engine::new("tsi1".to_string());
        e.engine.open().unwrap();
        let mut setup_points = must_parse_points_string("cpu,host=A value=1.1 1000000000");
        for p in setup_points.iter_mut() {
            let tags = p.tags();
            e.engine
                .create_series_if_not_exists(p.key(), &p.name(), tags)
                .unwrap();
        }

        let root = e.root.clone();
        let index = Arc::clone(&e.index);
        let sfile = Arc::clone(&e.sfile);
        let engine = Arc::new(e.engine);
        let key = series_field_key_bytes("cpu,host=A", "value");

        let mut writers = Vec::new();
        for writer_id in 0..2 {
            let writer_engine = Arc::clone(&engine);
            writers.push(std::thread::spawn(move || {
                for i in 0..50 {
                    let points = must_parse_points_string(&format!(
                        "cpu,host=A value=1.1 {}",
                        1_000_000_000i64 + writer_id * 100 + i
                    ));
                    writer_engine.write_points(&points).unwrap();
                }
            }));
        }

        for writer in writers {
            writer.join().expect("concurrent write_points panicked");
        }

        assert_eq!(
            engine.cache().values(&key).len(),
            100,
            "concurrent overlapping writes at distinct timestamps should all remain cache-visible"
        );

        engine.close().unwrap();
        index.close().unwrap();
        sfile.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn test_engine_close_concurrent_direct_write_points_does_not_panic() {
        let _hook_guard = engine_write_failure_hook_guard();
        let mut e = Engine::new("tsi1".to_string());
        e.engine.open().unwrap();
        let root = e.root.clone();
        let index = Arc::clone(&e.index);
        let sfile = Arc::clone(&e.sfile);
        let engine = Arc::new(e.engine);

        let writer_engine = Arc::clone(&engine);
        let writer = std::thread::spawn(move || {
            let points = must_parse_points_string("cpu,host=A value=1.1 1000000000");
            for _ in 0..200 {
                let _ = writer_engine.write_points(&points);
            }
        });

        let closer_engine = Arc::clone(&engine);
        let closer = std::thread::spawn(move || closer_engine.close());

        writer.join().expect("direct write_points panicked");
        closer.join().expect("engine close panicked").unwrap();
        index.close().unwrap();
        sfile.close().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    // Ensure that deletes only sent to the WAL will clear out the data from the cache on restart
    #[test]
    fn test_engine_delete_wal_load_metadata() {
        let mut e = must_open_engine("tsi1".to_string());

        e.write_points_string(&[
            "cpu,host=A value=1.1 1000000000",
            "cpu,host=B value=1.2 2000000000",
        ])
        .unwrap();

        // Remove series.
        let itr = Box::new(TestSeriesIter::new(vec!["cpu,host=A".as_bytes().to_vec()]));

        e.engine
            .delete_series_range(itr, i64::MIN, i64::MAX)
            .unwrap();

        // Ensure we can close and load index from the WAL
        e.reopen();

        let exp = 0;
        let got = e
            .engine
            .cache()
            .values(&series_field_key_bytes("cpu,host=A", "value"))
            .len();
        assert_eq!(
            exp, got,
            "unexpected number of values: got: {}. exp: {}",
            exp, got
        );

        let exp = 1;
        let got = e
            .engine
            .cache()
            .values(&series_field_key_bytes("cpu,host=B", "value"))
            .len();
        assert_eq!(
            exp, got,
            "unexpected number of values: got: {}. exp: {}",
            exp, got
        );

        // FIXME: if donot close engine, the test cannot return.
        e.close();
    }

    #[test]
    fn test_engine_measurement_drop_waits_for_snapshot_and_deletes_retained_snapshot() {
        let mut e = must_open_engine("tsi1".to_string());
        e.engine.set_compactions_enabled(false);
        for measurement in [b"cpu".as_slice(), b"memory".as_slice()] {
            e.engine
                .measurement_fields(measurement)
                .create_field_if_not_exists(b"value", crate::models::FLOAT)
                .unwrap();
        }
        e.engine.fieldset.save().unwrap();
        e.write_points_string(&["cpu,host=A value=1.0 10", "memory,host=A value=2.0 10"])
            .unwrap();

        let (cache, snapshot_gate) = {
            let inner = e.engine.inner.read();
            (
                Arc::clone(&inner.cache),
                Arc::clone(&inner.snapshot_measurement_drop_gate),
            )
        };
        let snapshot_guard = block_on_global(snapshot_gate.lock_owned());
        let retained = cache.snapshot().unwrap();
        assert!(retained.size() > 0);
        cache.clear_snapshot(false);
        assert_eq!(
            cache
                .values(&series_field_key_bytes("cpu,host=A", "value"))
                .len(),
            1
        );
        assert_eq!(
            cache
                .values(&series_field_key_bytes("memory,host=A", "value"))
                .len(),
            1
        );

        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            let engine = &e.engine;
            scope.spawn(move || {
                started_tx.send(()).unwrap();
                done_tx
                    .send(engine.delete_measurement_data(b"cpu"))
                    .unwrap();
            });
            started_rx.recv().unwrap();
            assert!(
                done_rx.recv_timeout(Duration::from_millis(50)).is_err(),
                "measurement deletion must wait while snapshot publication owns the gate"
            );
            drop(snapshot_guard);
            assert_eq!(done_rx.recv().unwrap().unwrap().len(), 1);
        });

        assert!(
            cache
                .values(&series_field_key_bytes("cpu,host=A", "value"))
                .is_empty()
        );
        assert_eq!(
            cache
                .values(&series_field_key_bytes("memory,host=A", "value"))
                .len(),
            1
        );

        e.engine.set_compactions_enabled(true);
        e.must_write_snapshot();
        assert!(
            collect_float_points(
                &e.engine,
                "cpu",
                crate::models::time::MIN_NANO_TIME_I64,
                crate::models::time::MAX_NANO_TIME_I64,
            )
            .is_empty()
        );
        assert_eq!(
            collect_float_points(
                &e.engine,
                "memory",
                crate::models::time::MIN_NANO_TIME_I64,
                crate::models::time::MAX_NANO_TIME_I64,
            )
            .len(),
            1
        );

        e.reopen();
        assert!(
            collect_float_points(
                &e.engine,
                "cpu",
                crate::models::time::MIN_NANO_TIME_I64,
                crate::models::time::MAX_NANO_TIME_I64,
            )
            .is_empty()
        );
        assert_eq!(
            collect_float_points(
                &e.engine,
                "memory",
                crate::models::time::MIN_NANO_TIME_I64,
                crate::models::time::MAX_NANO_TIME_I64,
            )
            .len(),
            1
        );
        e.close();
    }

    #[test]
    fn test_engine_measurement_drop_deletes_persisted_vlog_and_survives_reopen() {
        let mut e = must_open_vlog_engine("tsi1".to_string());
        for measurement in [b"cpu".as_slice(), b"memory".as_slice()] {
            e.engine
                .measurement_fields(measurement)
                .create_field_if_not_exists(b"value", crate::models::FLOAT)
                .unwrap();
        }
        e.engine.fieldset.save().unwrap();
        e.write_points_string(&[
            "cpu,host=A value=1.0 10",
            "cpu,host=A value=2.0 20",
            "memory,host=A value=3.0 10",
        ])
        .unwrap();
        e.must_write_snapshot();

        let vlog_files = {
            let inner = e.engine.inner.read();
            inner
                .vlog_inner
                .as_ref()
                .unwrap()
                .read()
                .vlog_file_store
                .count()
        };
        assert!(vlog_files > 0);

        let ids = e.engine.delete_measurement_data(b"cpu").unwrap();
        assert_eq!(ids.len(), 1);
        assert!(!e.sfile.is_deleted(ids[0]));
        assert!(e.engine.measurement_exists(b"cpu").unwrap());
        assert!(
            collect_float_points(
                &e.engine,
                "cpu",
                crate::models::time::MIN_NANO_TIME_I64,
                crate::models::time::MAX_NANO_TIME_I64,
            )
            .is_empty()
        );
        assert_eq!(
            collect_float_points(
                &e.engine,
                "memory",
                crate::models::time::MIN_NANO_TIME_I64,
                crate::models::time::MAX_NANO_TIME_I64,
            )
            .len(),
            1
        );

        e.reopen();
        assert!(
            collect_float_points(
                &e.engine,
                "cpu",
                crate::models::time::MIN_NANO_TIME_I64,
                crate::models::time::MAX_NANO_TIME_I64,
            )
            .is_empty()
        );
        assert_eq!(
            collect_float_points(
                &e.engine,
                "memory",
                crate::models::time::MIN_NANO_TIME_I64,
                crate::models::time::MAX_NANO_TIME_I64,
            )
            .len(),
            1
        );
        assert_eq!(e.engine.delete_measurement_data(b"cpu").unwrap(), ids);
        e.close();
    }

    #[test]
    fn test_engine_ordinary_delete_covers_every_persisted_vlog_batch_and_reopen() {
        fn visible_rows(engine: &super::Engine, measurement: &str) -> Vec<(i64, f64)> {
            let mut rows = collect_float_points(engine, measurement, 0, 40)
                .into_iter()
                .map(|point| (point.time, point.value))
                .collect::<Vec<_>>();
            rows.sort_by(|left, right| {
                left.0
                    .cmp(&right.0)
                    .then_with(|| left.1.total_cmp(&right.1))
            });
            rows
        }

        let mut e = must_open_vlog_engine("tsi1".to_string());
        for measurement in [b"cpu".as_slice(), b"memory".as_slice()] {
            e.engine
                .measurement_fields(measurement)
                .create_field_if_not_exists(b"value", crate::models::FLOAT)
                .unwrap();
        }
        e.engine.fieldset.save().unwrap();

        let mut lines = Vec::new();
        for (host, base) in [("a", 100), ("b", 200), ("c", 300), ("d", 400), ("z", 900)] {
            for timestamp in [10, 20, 30] {
                lines.push(format!(
                    "cpu,host={host} value={}.0 {timestamp}",
                    base + timestamp
                ));
            }
        }
        lines.extend([
            "memory,host=a value=1.0 10".to_string(),
            "memory,host=a value=2.0 20".to_string(),
            "memory,host=a value=3.0 30".to_string(),
        ]);
        let refs = lines.iter().map(String::as_str).collect::<Vec<_>>();
        e.write_points_string(&refs).unwrap();
        e.must_write_snapshot();

        let target_series = ["a", "b", "c", "d"]
            .into_iter()
            .map(|host| format!("cpu,host={host}").into_bytes())
            .collect::<Vec<_>>();
        let delete_flush_threshold = target_series[0].len();
        e.engine
            .delete_series_range_with_predicate_and_threshold(
                Box::new(TestSeriesIter::new(target_series)),
                Some(|_: &[u8], _: &crate::models::Tags| (20, 20, true)),
                delete_flush_threshold,
            )
            .unwrap();

        let expected_cpu = vec![
            (10, 110.0),
            (10, 210.0),
            (10, 310.0),
            (10, 410.0),
            (10, 910.0),
            (20, 920.0),
            (30, 130.0),
            (30, 230.0),
            (30, 330.0),
            (30, 430.0),
            (30, 930.0),
        ];
        assert_eq!(visible_rows(&e.engine, "cpu"), expected_cpu);
        assert_eq!(
            visible_rows(&e.engine, "memory"),
            vec![(10, 1.0), (20, 2.0), (30, 3.0)]
        );
        assert!(e.engine.measurement_exists(b"cpu").unwrap());

        e.reopen();
        assert_eq!(visible_rows(&e.engine, "cpu"), expected_cpu);
        assert_eq!(
            visible_rows(&e.engine, "memory"),
            vec![(10, 1.0), (20, 2.0), (30, 3.0)]
        );
        assert!(e.engine.measurement_exists(b"cpu").unwrap());
        e.close();
    }

    fn exercise_ordinary_delete_batch_case(
        vlog_enabled: bool,
        target_hosts: &[&str],
        delete: impl FnOnce(&super::Engine, Vec<Vec<u8>>),
    ) {
        let mut e = if vlog_enabled {
            must_open_vlog_engine("tsi1".to_string())
        } else {
            must_open_engine("tsi1".to_string())
        };
        e.engine
            .measurement_fields(b"cpu")
            .create_field_if_not_exists(b"value", crate::models::FLOAT)
            .unwrap();

        let mut lines = Vec::new();
        for (index, host) in target_hosts.iter().copied().chain(["z"]).enumerate() {
            let base = if host == "z" { 900 } else { (index + 1) * 100 };
            for timestamp in [10, 20, 30] {
                lines.push(format!(
                    "cpu,host={host} value={}.0 {timestamp}",
                    base + timestamp
                ));
            }
        }
        let refs = lines.iter().map(String::as_str).collect::<Vec<_>>();
        e.write_points_string(&refs).unwrap();
        e.must_write_snapshot();

        let target_series = target_hosts
            .iter()
            .map(|host| format!("cpu,host={host}").into_bytes())
            .collect::<Vec<_>>();
        delete(&e.engine, target_series);

        let mut rows = collect_float_points(&e.engine, "cpu", 0, 40)
            .into_iter()
            .map(|point| (point.time, point.value))
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| left.1.total_cmp(&right.1))
        });

        let mut expected = Vec::new();
        for (index, _) in target_hosts.iter().enumerate() {
            let base = (index + 1) * 100;
            expected.extend([(10, (base + 10) as f64), (30, (base + 30) as f64)]);
        }
        expected.extend([(10, 910.0), (20, 920.0), (30, 930.0)]);
        expected.sort_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| left.1.total_cmp(&right.1))
        });
        assert_eq!(rows, expected);
        assert!(e.engine.measurement_exists(b"cpu").unwrap());
        e.close();
    }

    #[test]
    fn test_engine_ordinary_delete_vlog_below_threshold_residual_batch() {
        exercise_ordinary_delete_batch_case(true, &["a"], |engine, target_series| {
            engine
                .delete_series_range_with_predicate_and_threshold(
                    Box::new(TestSeriesIter::new(target_series)),
                    Some(|_: &[u8], _: &crate::models::Tags| (20, 20, true)),
                    usize::MAX,
                )
                .unwrap();
        });
    }

    #[test]
    fn test_engine_ordinary_delete_vlog_one_completed_batch_plus_residual() {
        exercise_ordinary_delete_batch_case(true, &["a", "b"], |engine, target_series| {
            let threshold = target_series[0].len();
            engine
                .delete_series_range_with_predicate_and_threshold(
                    Box::new(TestSeriesIter::new(target_series)),
                    Some(|_: &[u8], _: &crate::models::Tags| (20, 20, true)),
                    threshold,
                )
                .unwrap();
        });
    }

    #[test]
    fn test_engine_ordinary_delete_complete_vlog_batch_without_residual() {
        exercise_ordinary_delete_batch_case(true, &["a", "b"], |engine, target_series| {
            engine
                .delete_series_range_ordinary_batch(&target_series, 20, 20)
                .unwrap();
        });
    }

    #[test]
    fn test_engine_ordinary_delete_small_batches_with_vlog_disabled() {
        exercise_ordinary_delete_batch_case(false, &["a", "b", "c"], |engine, target_series| {
            let threshold = target_series[0].len();
            engine
                .delete_series_range_with_predicate_and_threshold(
                    Box::new(TestSeriesIter::new(target_series)),
                    Some(|_: &[u8], _: &crate::models::Tags| (20, 20, true)),
                    threshold,
                )
                .unwrap();
        });
    }

    // See https://github.com/influxdata/influxdb/issues/14229
    #[test]
    fn test_engine_delete_series_after_cache_snapshot() {
        let mut e = must_open_engine("tsi1".to_string());
        e.write_points_string(&[
            "cpu,host=A value=1.1 1000000000",
            "cpu,host=B value=1.2 2000000000",
        ])
        .unwrap();

        e.engine
            .measurement_fields("cpu".as_bytes())
            .create_field_if_not_exists("value".as_bytes(), crate::models::FLOAT)
            .unwrap();
        e.engine
            .create_series_if_not_exists(
                "cpu,host=A".as_bytes(),
                "cpu".as_bytes(),
                crate::models::Tags::from(HashMap::from([(
                    "host".as_bytes().to_vec(),
                    "A".as_bytes().to_vec(),
                )])),
            )
            .unwrap();
        e.engine
            .create_series_if_not_exists(
                "cpu,host=B".as_bytes(),
                "cpu".as_bytes(),
                crate::models::Tags::from(HashMap::from([(
                    "host".as_bytes().to_vec(),
                    "B".as_bytes().to_vec(),
                )])),
            )
            .unwrap();

        // Verify series exist.
        let n = e.series_exist("cpu".to_string(), &["host".to_string()]);
        assert_eq!(n, 2, "got  {} points, expected {}", n, 2);

        // Simulate restart of server.
        e.reopen();

        // Snapshot the cache
        super::Engine::write_snapshot(true, Arc::clone(&e.engine.inner)).unwrap();
        // Verify series exist
        let n = e.series_exist("cpu".to_string(), &["host".to_string()]);
        assert_eq!(n, 2, "got  {} points, expected {}", n, 2);

        // Remove series.
        let itr = Box::new(TestSeriesIter::new(vec![
            "cpu,host=A".as_bytes().to_vec(),
            "cpu,host=B".as_bytes().to_vec(),
        ]));

        e.engine
            .delete_series_range(itr, i64::MIN, i64::MAX)
            .unwrap();

        // Verify series are no longer present
        let n = e.series_exist("cpu".to_string(), &["host".to_string()]);
        assert_eq!(n, 0, "got  {} points, expected {}", n, 0);

        // Simulate restart of server
        e.reopen();

        // Verify series are no longer present
        let n = e.series_exist("cpu".to_string(), &["host".to_string()]);
        assert_eq!(n, 0, "got  {} points, expected {}", n, 0);

        // FIXME: if donot close engine, the test cannot return.
        e.close();
    }

    fn write_vlog_float_points(e: &Engine, count: usize) {
        e.engine
            .measurement_fields("cpu".as_bytes())
            .create_field_if_not_exists("value".as_bytes(), crate::models::FLOAT)
            .unwrap();

        let lines = (0..count)
            .map(|i| {
                format!(
                    "cpu,host=h{} value={} {}",
                    i,
                    i as f64,
                    1_000_000_000i64 + i as i64
                )
            })
            .collect::<Vec<_>>();
        let refs = lines.iter().map(String::as_str).collect::<Vec<_>>();
        e.write_points_string(&refs).unwrap();
    }

    #[test]
    fn test_vlog_snapshot_publish_metadata_avoids_keys_by_gen_scan() {
        let mut e = must_open_vlog_engine("tsi1".to_string());
        write_vlog_float_points(&e, 64);

        let cache = {
            let inner = e.engine.inner.read();
            let vlog = inner.vlog_inner.as_ref().unwrap().read();
            Arc::clone(&vlog.vlog_index_cache)
        };
        let scans_before = cache.keys_by_gen_and_seq_call_count();
        e.must_write_snapshot();

        assert_eq!(
            cache.keys_by_gen_and_seq_call_count(),
            scans_before,
            "snapshot metadata publish must not scan vlog_index_cache keys"
        );

        e.close();
    }

    #[test]
    fn test_vlog_snapshot_publish_metadata_query_after_publish() {
        let mut e = must_open_vlog_engine("tsi1".to_string());
        write_vlog_float_points(&e, 3);

        e.must_write_snapshot();

        let mut points = collect_float_points(&e.engine, "cpu", 1_000_000_000, 1_000_000_002);
        points.sort_by_key(|point| point.time);
        assert_eq!(points.len(), 3);
        assert_eq!(points[0].value, 0.0);
        assert_eq!(points[1].value, 1.0);
        assert_eq!(points[2].value, 2.0);

        e.close();
    }

    #[test]
    fn test_vlog_snapshot_publish_metadata_restart_recovery() {
        let mut e = must_open_vlog_engine("tsi1".to_string());
        write_vlog_float_points(&e, 3);

        e.must_write_snapshot();
        e.reopen();
        e.engine
            .measurement_fields("cpu".as_bytes())
            .create_field_if_not_exists("value".as_bytes(), crate::models::FLOAT)
            .unwrap();

        let mut points = collect_float_points(&e.engine, "cpu", 1_000_000_000, 1_000_000_002);
        points.sort_by_key(|point| point.time);
        assert_eq!(points.len(), 3);
        assert_eq!(points[0].value, 0.0);
        assert_eq!(points[1].value, 1.0);
        assert_eq!(points[2].value, 2.0);

        e.close();
    }

    #[test]
    fn test_vlog_snapshot_publish_metadata_delete_series_after_publish() {
        let mut e = must_open_vlog_engine("tsi1".to_string());
        e.engine
            .measurement_fields("cpu".as_bytes())
            .create_field_if_not_exists("value".as_bytes(), crate::models::FLOAT)
            .unwrap();
        e.write_points_string(&[
            "cpu,host=h0 value=0 1000000000",
            "cpu,host=h0 value=1 1000000001",
            "cpu,host=h0 value=2 1000000002",
        ])
        .unwrap();

        e.must_write_snapshot();
        let vlog_file_store = {
            let inner = e.engine.inner.read();
            Arc::clone(&inner.vlog_inner.as_ref().unwrap().read().vlog_file_store)
        };
        let key = vlog_file_store
            .keys()
            .into_keys()
            .next()
            .expect("expected one published VLog key");
        vlog_file_store
            .delete_range(vec![key], 1_000_000_001, 1_000_000_001)
            .unwrap();
        vlog_file_store
            .apply(|r| {
                if !r.has_tombstones() {
                    return Err(crate::storage::tsm1::vlog::file_store::Error::StringErr {
                        reason: "metadata-built reader did not retain tombstone metadata"
                            .to_string(),
                    });
                }
                Ok(())
            })
            .unwrap();

        e.close();
    }

    #[ignore]
    #[test]
    fn test_vlog_snapshot_publish_metadata_high_cardinality_timing() {
        let mut e = must_open_vlog_engine("tsi1".to_string());
        write_vlog_float_points(&e, 10_000);

        let cache = {
            let inner = e.engine.inner.read();
            let vlog = inner.vlog_inner.as_ref().unwrap().read();
            Arc::clone(&vlog.vlog_index_cache)
        };
        let scans_before = cache.keys_by_gen_and_seq_call_count();
        let start = std::time::Instant::now();
        e.must_write_snapshot();
        let elapsed = start.elapsed();
        let scans = cache.keys_by_gen_and_seq_call_count() - scans_before;

        println!(
            "vlog metadata publish high-cardinality timing: keys=10000 elapsed={:?} keys_by_gen_and_seq_calls={}",
            elapsed, scans
        );
        assert_eq!(scans, 0);

        e.close();
    }

    #[test]
    fn test_engine_storage_scan_cancellation_error_boundary() {
        let mut e = must_open_engine("tsi1".to_string());
        e.engine
            .measurement_fields(b"cpu")
            .create_field_if_not_exists(b"value", crate::models::FLOAT)
            .unwrap();
        e.write_points_string(&["cpu,host=A value=1.0 10"]).unwrap();

        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let value = ColumnRef::new("value", ColumnKind::Field(Some(crate::models::FLOAT)));
        let scan = StorageScan {
            measurement: MeasurementRef::new("cpu"),
            projection: StorageProjection {
                primary_column: Some(value.clone()),
                returned_columns: vec![value],
                row_domain_columns: vec![],
                series: SeriesProjection::new(vec!["host".to_string()], vec!["host".to_string()]),
            },
            time_range: TimeRange::all(),
            predicate: None,
            time_order: TimeOrder::Ascending,
            output_order: OutputOrder::TimeOrdered,
            execution: Default::default(),
            cancellation: Some(ScanCancellation::new(cancellation.clone())),
        };

        assert!(matches!(
            e.engine.create_storage_iterator(scan.clone()),
            Err(super::Error::ScanCancelled)
        ));
        e.close();
    }

    #[test]
    fn test_engine_storage_scan_preserves_identity_with_narrower_output_tags() {
        let mut e = must_open_engine("tsi1".to_string());
        e.engine
            .measurement_fields(b"cpu")
            .create_field_if_not_exists(b"value", crate::models::FLOAT)
            .unwrap();
        e.write_points_string(&[
            "cpu,host=A,region=us value=1.0 100",
            "cpu,host=A,region=eu value=2.0 100",
        ])
        .unwrap();

        let value = ColumnRef::new("value", ColumnKind::Field(Some(crate::models::FLOAT)));
        let scan = StorageScan {
            measurement: MeasurementRef::new("cpu"),
            projection: StorageProjection {
                primary_column: Some(value.clone()),
                returned_columns: vec![value],
                row_domain_columns: vec![],
                series: SeriesProjection::new(
                    vec!["host".to_string(), "region".to_string()],
                    vec!["host".to_string()],
                ),
            },
            time_range: TimeRange::new(100, 100),
            predicate: None,
            time_order: TimeOrder::Ascending,
            output_order: OutputOrder::TimeOrdered,
            execution: Default::default(),
            cancellation: None,
        };
        let points =
            collect_float_iterator(e.engine.create_storage_iterator(scan).unwrap().unwrap());

        assert_eq!(points.len(), 2);
        let mut values = points.iter().map(|point| point.value).collect::<Vec<_>>();
        values.sort_by(f64::total_cmp);
        assert_eq!(values, vec![1.0, 2.0]);
        assert!(points.iter().all(|point| {
            point.time == 100
                && point.tags == parse_tags("host=A")
                && point.tags.value("region").is_none()
        }));
        e.close();
    }

    #[test]
    fn test_engine_storage_scan_rejects_malformed_primary_projection() {
        let mut e = must_open_engine("tsi1".to_string());
        let value = ColumnRef::new("value", ColumnKind::Field(Some(crate::models::FLOAT)));
        let scan = |projection| StorageScan {
            measurement: MeasurementRef::new("cpu"),
            projection,
            time_range: TimeRange::all(),
            predicate: None,
            time_order: TimeOrder::Ascending,
            output_order: OutputOrder::Unspecified,
            execution: Default::default(),
            cancellation: None,
        };

        let error = match e.engine.create_storage_iterator(scan(StorageProjection {
            primary_column: Some(value.clone()),
            returned_columns: vec![],
            row_domain_columns: vec![],
            series: SeriesProjection::default(),
        })) {
            Err(error) => error,
            Ok(_) => panic!("missing returned primary column must be rejected"),
        };
        assert!(matches!(
            error,
            super::Error::StorageProjection { ref reason }
                if reason == "primary column requires a non-empty returned column list"
        ));

        let host = ColumnRef::new("host", ColumnKind::Tag);
        let error = match e.engine.create_storage_iterator(scan(StorageProjection {
            primary_column: Some(value.clone()),
            returned_columns: vec![host.clone(), value.clone()],
            row_domain_columns: vec![],
            series: SeriesProjection::default(),
        })) {
            Err(error) => error,
            Ok(_) => panic!("primary column outside returned_columns[0] must be rejected"),
        };
        assert!(matches!(
            error,
            super::Error::StorageProjection { ref reason }
                if reason.contains("must be the first returned column")
        ));

        let result = e.engine.create_storage_iterator(scan(StorageProjection {
            primary_column: None,
            returned_columns: vec![value],
            row_domain_columns: vec![],
            series: SeriesProjection::default(),
        }));
        assert!(matches!(result, Ok(None)));
        e.close();
    }

    #[test]
    fn test_engine_native_numeric_casts_apply_to_primary_aux_and_predicate_cursors() {
        let mut e = must_open_engine("tsi1".to_string());
        let fields = e.engine.measurement_fields(b"cpu");
        fields
            .create_field_if_not_exists(b"integer", crate::models::INTEGER)
            .unwrap();
        fields
            .create_field_if_not_exists(b"unsigned", crate::models::UNSIGNED)
            .unwrap();
        fields
            .create_field_if_not_exists(b"float", crate::models::FLOAT)
            .unwrap();
        fields
            .create_field_if_not_exists(b"other", crate::models::FLOAT)
            .unwrap();
        fields
            .create_field_if_not_exists(b"string", crate::models::STRING)
            .unwrap();
        e.write_points_string(&[
            "cpu,host=A integer=-1i,unsigned=2u,float=3.5,other=10.0,string=\"4\" 10",
            "cpu,host=A integer=3i,unsigned=4u,float=5.5,other=30.0,string=\"6\" 30",
        ])
        .unwrap();

        let build = |field: &str, kind: ColumnKind| {
            let requested = ColumnRef::new(field, kind);
            let scan = Arc::new(StorageScan {
                measurement: MeasurementRef::new("cpu"),
                projection: StorageProjection {
                    primary_column: Some(requested.clone()),
                    returned_columns: vec![requested.clone()],
                    row_domain_columns: vec![],
                    series: SeriesProjection::default(),
                },
                time_range: TimeRange::all(),
                predicate: None,
                time_order: TimeOrder::Ascending,
                output_order: OutputOrder::Unspecified,
                execution: Default::default(),
                cancellation: None,
            });
            let plan = Arc::new(super::EngineScanPlan {
                scan,
                vlog_enabled: e.engine.vlog_enabled,
            });
            let (cache, file_store, vlog_inner) = {
                let inner = e.engine.inner.read();
                (
                    Arc::clone(&inner.cache),
                    Arc::clone(&inner.file_store),
                    inner.vlog_inner.clone(),
                )
            };
            super::Engine::build_cursor(
                vlog_inner,
                &cache,
                &e.engine.fieldset,
                &file_store,
                "cpu",
                "cpu,host=A",
                crate::models::Tags::default(),
                &requested,
                &plan,
            ).unwrap()
        };

        assert!(matches!(
            build("integer", ColumnKind::Field(Some(crate::models::FLOAT))),
            Some(iterator::Cursor::FloatCursor(
                iterator::FloatCursor::FloatCastIntegerCursor(_)
            ))
        ));
        assert!(matches!(
            build("unsigned", ColumnKind::Field(Some(crate::models::FLOAT))),
            Some(iterator::Cursor::FloatCursor(
                iterator::FloatCursor::FloatCastUnsignedCursor(_)
            ))
        ));
        assert!(matches!(
            build("float", ColumnKind::Field(Some(crate::models::INTEGER))),
            Some(iterator::Cursor::IntegerCursor(
                iterator::IntegerCursor::IntegerCastFloatCursor(_)
            ))
        ));
        assert!(matches!(
            build("unsigned", ColumnKind::Field(Some(crate::models::INTEGER))),
            Some(iterator::Cursor::IntegerCursor(
                iterator::IntegerCursor::IntegerCastUnsignedCursor(_)
            ))
        ));
        assert!(matches!(
            build("float", ColumnKind::Field(Some(crate::models::UNSIGNED))),
            Some(iterator::Cursor::UnsignedCursor(
                iterator::UnsignedCursor::UnsignedCastFloatCursor(_)
            ))
        ));
        assert!(matches!(
            build("integer", ColumnKind::Field(Some(crate::models::UNSIGNED))),
            Some(iterator::Cursor::UnsignedCursor(
                iterator::UnsignedCursor::UnsignedCastIntegerCursor(_)
            ))
        ));
        assert!(matches!(
            build("integer", ColumnKind::AnyField),
            Some(iterator::Cursor::IntegerCursor(
                iterator::IntegerCursor::IntegerAscendingCursor(_)
            ))
        ));
        assert!(build("string", ColumnKind::Field(Some(crate::models::FLOAT))).is_none());

        let integer_as_float =
            ColumnRef::new("integer", ColumnKind::Field(Some(crate::models::FLOAT)));
        let scan = StorageScan {
            measurement: MeasurementRef::new("cpu"),
            projection: StorageProjection {
                primary_column: Some(integer_as_float.clone()),
                returned_columns: vec![integer_as_float],
                row_domain_columns: vec![],
                series: SeriesProjection::new(vec!["host".to_string()], vec!["host".to_string()]),
            },
            time_range: TimeRange::all(),
            predicate: None,
            time_order: TimeOrder::Ascending,
            output_order: OutputOrder::TimeOrdered,
            execution: Default::default(),
            cancellation: None,
        };
        let points =
            collect_float_iterator(e.engine.create_storage_iterator(scan).unwrap().unwrap());
        assert_eq!(
            points
                .iter()
                .map(|point| (point.time, point.value))
                .collect::<Vec<_>>(),
            vec![(10, -1.0), (30, 3.0)]
        );

        let other = ColumnRef::new("other", ColumnKind::Field(Some(crate::models::FLOAT)));
        let integer_as_float =
            ColumnRef::new("integer", ColumnKind::Field(Some(crate::models::FLOAT)));
        let predicate = StoragePredicate::Compare {
            op: CompareOp::GreaterThan,
            lhs: StorageValueExpr::Column(integer_as_float.clone()),
            rhs: StorageValueExpr::Literal(Val::F64(2.0)),
        };
        let scan = StorageScan {
            measurement: MeasurementRef::new("cpu"),
            projection: StorageProjection {
                primary_column: Some(other.clone()),
                returned_columns: vec![other, integer_as_float],
                row_domain_columns: vec![],
                series: SeriesProjection::new(vec!["host".to_string()], vec!["host".to_string()]),
            },
            time_range: TimeRange::all(),
            predicate: Some(predicate),
            time_order: TimeOrder::Ascending,
            output_order: OutputOrder::TimeOrdered,
            execution: Default::default(),
            cancellation: None,
        };
        let points =
            collect_float_iterator(e.engine.create_storage_iterator(scan).unwrap().unwrap());
        assert_eq!(points.len(), 1);
        assert_eq!((points[0].time, points[0].value), (30, 30.0));
        assert_eq!(points[0].aux, vec![Val::F64(3.0)]);

        e.close();
    }

    #[test]
    fn test_engine_native_field_or_tag_name_and_nested_boolean_execution() {
        let mut e = must_open_engine("tsi1".to_string());
        let fields = e.engine.measurement_fields(b"cpu");
        for field in ["value", "temperature"] {
            fields
                .create_field_if_not_exists(field.as_bytes(), crate::models::FLOAT)
                .unwrap();
        }
        fields
            .create_field_if_not_exists(b"host", crate::models::STRING)
            .unwrap();
        e.write_points_string(&[
            "cpu,host=A,region=us value=1.0,temperature=5.0 10",
            "cpu,host=A,region=us value=2.0,temperature=15.0 11",
            "cpu,host=B,name=shadow,region=eu value=3.0,temperature=5.0 20",
            "cpu,host=C,name=cpu,region=us value=4.0,temperature=5.0 30",
            "cpu,host=D value=5.0 40",
        ])
        .unwrap();

        let value = ColumnRef::new("value", ColumnKind::Field(Some(crate::models::FLOAT)));
        let run = |predicate: StoragePredicate| {
            let scan = StorageScan {
                measurement: MeasurementRef::new("cpu"),
                projection: StorageProjection {
                    primary_column: Some(value.clone()),
                    returned_columns: vec![value.clone()],
                    row_domain_columns: vec![],
                    series: SeriesProjection::new(
                        vec!["host".to_string(), "name".to_string(), "region".to_string()],
                        vec!["host".to_string(), "name".to_string(), "region".to_string()],
                    ),
                },
                time_range: TimeRange::all(),
                predicate: Some(predicate),
                time_order: TimeOrder::Ascending,
                output_order: OutputOrder::TimeOrdered,
                execution: Default::default(),
                cancellation: None,
            };
            let mut times =
                collect_float_iterator(e.engine.create_storage_iterator(scan).unwrap().unwrap())
                    .into_iter()
                    .map(|point| point.time)
                    .collect::<Vec<_>>();
            times.sort_unstable();
            times
        };

        let field_greater_than_ten = |kind| StoragePredicate::Compare {
            op: CompareOp::GreaterThan,
            lhs: StorageValueExpr::Column(ColumnRef::new("temperature", kind)),
            rhs: StorageValueExpr::Literal(Val::F64(10.0)),
        };
        for kind in [
            ColumnKind::Field(Some(crate::models::FLOAT)),
            ColumnKind::AnyField,
            ColumnKind::FieldOrTag,
        ] {
            assert_eq!(run(field_greater_than_ten(kind)), vec![11]);
        }

        let region_us = StoragePredicate::Compare {
            op: CompareOp::Equal,
            lhs: StorageValueExpr::Column(ColumnRef::new("region", ColumnKind::FieldOrTag)),
            rhs: StorageValueExpr::Literal(Val::String("us".to_string())),
        };
        assert_eq!(run(region_us), vec![10, 11, 30]);

        let host_tag_shadowed_by_field = StoragePredicate::Compare {
            op: CompareOp::Equal,
            lhs: StorageValueExpr::Column(ColumnRef::new("host", ColumnKind::FieldOrTag)),
            rhs: StorageValueExpr::Literal(Val::String("A".to_string())),
        };
        assert!(run(host_tag_shadowed_by_field).is_empty());

        let measurement = StoragePredicate::Compare {
            op: CompareOp::Equal,
            lhs: StorageValueExpr::Column(ColumnRef::new("_name", ColumnKind::Measurement)),
            rhs: StorageValueExpr::Literal(Val::String("cpu".to_string())),
        };
        assert_eq!(run(measurement), vec![10, 11, 20, 30, 40]);

        let ordinary_name = StoragePredicate::Compare {
            op: CompareOp::Equal,
            lhs: StorageValueExpr::Column(ColumnRef::new("name", ColumnKind::FieldOrTag)),
            rhs: StorageValueExpr::Literal(Val::String("cpu".to_string())),
        };
        assert_eq!(run(ordinary_name.clone()), vec![30]);

        let legacy_name = StoragePredicate::Compare {
            op: CompareOp::Equal,
            lhs: StorageValueExpr::Column(ColumnRef::new("name", ColumnKind::LegacyName)),
            rhs: StorageValueExpr::Literal(Val::String("cpu".to_string())),
        };
        assert_eq!(run(legacy_name), vec![10, 11, 30, 40]);

        let nested = StoragePredicate::Logical {
            op: LogicalOp::Or,
            lhs: Box::new(ordinary_name),
            rhs: Box::new(field_greater_than_ten(ColumnKind::AnyField)),
        };
        assert_eq!(run(nested), vec![11, 30]);

        e.close();
    }

    #[test]
    fn test_engine_native_storage_scan_executes_predicate_over_persisted_vlog() {
        let mut e = must_open_vlog_engine("tsi1".to_string());
        let fields = e.engine.measurement_fields(b"cpu");
        fields
            .create_field_if_not_exists(b"value", crate::models::FLOAT)
            .unwrap();
        fields
            .create_field_if_not_exists(b"temperature", crate::models::FLOAT)
            .unwrap();
        e.write_points_string(&[
            "cpu,host=A value=1.0,temperature=5.0 10",
            "cpu,host=A value=2.0,temperature=15.0 20",
            "cpu,host=B value=3.0,temperature=20.0 20",
        ])
        .unwrap();
        e.must_write_snapshot();
        e.reopen();
        let fields = e.engine.measurement_fields(b"cpu");
        fields
            .create_field_if_not_exists(b"value", crate::models::FLOAT)
            .unwrap();
        fields
            .create_field_if_not_exists(b"temperature", crate::models::FLOAT)
            .unwrap();
        let vlog_keys = {
            let inner = e.engine.inner.read();
            inner
                .vlog_inner
                .as_ref()
                .unwrap()
                .read()
                .vlog_file_store
                .keys()
        };
        assert!(
            !vlog_keys.is_empty(),
            "reopened scan must read persisted VLog files"
        );

        let value = ColumnRef::new("value", ColumnKind::Field(Some(crate::models::FLOAT)));
        let predicate = StoragePredicate::Logical {
            op: LogicalOp::And,
            lhs: Box::new(StoragePredicate::Compare {
                op: CompareOp::Equal,
                lhs: StorageValueExpr::Column(ColumnRef::new("host", ColumnKind::Tag)),
                rhs: StorageValueExpr::Literal(Val::String("A".to_string())),
            }),
            rhs: Box::new(StoragePredicate::Compare {
                op: CompareOp::GreaterThan,
                lhs: StorageValueExpr::Column(ColumnRef::new(
                    "temperature",
                    ColumnKind::Field(Some(crate::models::FLOAT)),
                )),
                rhs: StorageValueExpr::Literal(Val::F64(10.0)),
            }),
        };
        let storage_scan = StorageScan {
            measurement: MeasurementRef::new("cpu"),
            projection: StorageProjection {
                primary_column: Some(value.clone()),
                returned_columns: vec![value],
                row_domain_columns: vec![],
                series: SeriesProjection::new(vec!["host".to_string()], vec!["host".to_string()]),
            },
            time_range: TimeRange::all(),
            predicate: Some(predicate),
            time_order: TimeOrder::Ascending,
            output_order: OutputOrder::TimeOrdered,
            execution: Default::default(),
            cancellation: None,
        };
        let storage_points = collect_float_iterator(
            e.engine
                .create_storage_iterator(storage_scan)
                .unwrap()
                .unwrap(),
        );

        assert_eq!(
            storage_points
                .iter()
                .map(|point| (point.time, point.value))
                .collect::<Vec<_>>(),
            vec![(20, 2.0)]
        );
        e.close();
    }

    #[test]
    fn test_engine_native_predicate_matches_cache_and_persisted_tsm() {
        let mut e = must_open_engine("tsi1".to_string());
        let fields = e.engine.measurement_fields(b"cpu");
        fields
            .create_field_if_not_exists(b"value", crate::models::FLOAT)
            .unwrap();
        fields
            .create_field_if_not_exists(b"temperature", crate::models::FLOAT)
            .unwrap();
        e.write_points_string(&[
            "cpu,host=A value=1.0,temperature=5.0 10",
            "cpu,host=A value=2.0,temperature=15.0 20",
            "cpu,host=B value=3.0,temperature=20.0 30",
        ])
        .unwrap();

        let value = ColumnRef::new("value", ColumnKind::Field(Some(crate::models::FLOAT)));
        let scan = StorageScan {
            measurement: MeasurementRef::new("cpu"),
            projection: StorageProjection {
                primary_column: Some(value.clone()),
                returned_columns: vec![value],
                row_domain_columns: vec![],
                series: SeriesProjection::new(vec!["host".to_string()], vec!["host".to_string()]),
            },
            time_range: TimeRange::all(),
            predicate: Some(StoragePredicate::Logical {
                op: LogicalOp::And,
                lhs: Box::new(StoragePredicate::Compare {
                    op: CompareOp::Equal,
                    lhs: StorageValueExpr::Column(ColumnRef::new("host", ColumnKind::Tag)),
                    rhs: StorageValueExpr::Literal(Val::String("A".to_string())),
                }),
                rhs: Box::new(StoragePredicate::Compare {
                    op: CompareOp::GreaterThan,
                    lhs: StorageValueExpr::Column(ColumnRef::new(
                        "temperature",
                        ColumnKind::Field(Some(crate::models::FLOAT)),
                    )),
                    rhs: StorageValueExpr::Literal(Val::F64(10.0)),
                }),
            }),
            time_order: TimeOrder::Ascending,
            output_order: OutputOrder::TimeOrdered,
            execution: Default::default(),
            cancellation: None,
        };
        let cached = collect_float_iterator(
            e.engine
                .create_storage_iterator(scan.clone())
                .unwrap()
                .unwrap(),
        );

        e.must_write_snapshot();
        e.reopen();
        let fields = e.engine.measurement_fields(b"cpu");
        fields
            .create_field_if_not_exists(b"value", crate::models::FLOAT)
            .unwrap();
        fields
            .create_field_if_not_exists(b"temperature", crate::models::FLOAT)
            .unwrap();
        assert!(!e.engine.file_store_keys().is_empty());
        let persisted =
            collect_float_iterator(e.engine.create_storage_iterator(scan).unwrap().unwrap());

        assert_eq!(cached, persisted);
        assert_eq!(
            persisted
                .iter()
                .map(|point| (point.time, point.value))
                .collect::<Vec<_>>(),
            vec![(20, 2.0)]
        );
        e.close();
    }

    #[test]
    fn test_engine_storage_scan_supports_sentinel_adjacent_ranges() {
        let mut e = must_open_engine("tsi1".to_string());
        e.engine
            .measurement_fields(b"cpu")
            .create_field_if_not_exists(b"value", crate::models::FLOAT)
            .unwrap();
        let min = crate::models::time::MIN_NANO_TIME_I64;
        let max = crate::models::time::MAX_NANO_TIME_I64;
        let lines = [
            format!("cpu,host=A value=1.0 {min}"),
            format!("cpu,host=A value=2.0 {}", min + 1),
            format!("cpu,host=A value=3.0 {}", max - 1),
            format!("cpu,host=A value=4.0 {max}"),
        ];
        let line_refs = lines.iter().map(String::as_str).collect::<Vec<_>>();
        e.write_points_string(&line_refs).unwrap();

        for (range, expected) in [
            (TimeRange::new(min, min + 1), vec![min, min + 1]),
            (TimeRange::new(max - 1, max), vec![max - 1, max]),
        ] {
            let value = ColumnRef::new("value", ColumnKind::Field(Some(crate::models::FLOAT)));
            let scan = StorageScan {
                measurement: MeasurementRef::new("cpu"),
                projection: StorageProjection {
                    primary_column: Some(value.clone()),
                    returned_columns: vec![value],
                    row_domain_columns: vec![],
                    series: SeriesProjection::new(
                        vec!["host".to_string()],
                        vec!["host".to_string()],
                    ),
                },
                time_range: range,
                predicate: None,
                time_order: TimeOrder::Ascending,
                output_order: OutputOrder::TimeOrdered,
                execution: Default::default(),
                cancellation: None,
            };
            let points =
                collect_float_iterator(e.engine.create_storage_iterator(scan).unwrap().unwrap());
            assert_eq!(
                points.iter().map(|point| point.time).collect::<Vec<_>>(),
                expected
            );
        }
        e.close();
    }

    // Test that series id set gets updated and returned appropriately.
    #[test]
    fn test_engine_index_series_id_set() {
        let mut engine = must_open_engine("tsi1".to_string());

        // Add some series.
        engine.must_add_series(
            "cpu".to_string(),
            HashMap::from([
                ("host".as_bytes().to_vec(), "a".as_bytes().to_vec()),
                ("region".as_bytes().to_vec(), "west".as_bytes().to_vec()),
            ]),
        );
        engine.must_add_series(
            "cpu".to_string(),
            HashMap::from([
                ("host".as_bytes().to_vec(), "b".as_bytes().to_vec()),
                ("region".as_bytes().to_vec(), "west".as_bytes().to_vec()),
            ]),
        );
        engine.must_add_series(
            "cpu".to_string(),
            HashMap::from([("host".as_bytes().to_vec(), "b".as_bytes().to_vec())]),
        );
        engine.must_add_series("gpu".to_string(), HashMap::new());
        engine.must_add_series(
            "gpu".to_string(),
            HashMap::from([("host".as_bytes().to_vec(), "b".as_bytes().to_vec())]),
        );
        engine.must_add_series(
            "mem".to_string(),
            HashMap::from([("host".as_bytes().to_vec(), "z".as_bytes().to_vec())]),
        );

        // Collect series IDs.
        let mut series_id_map = HashMap::<String, u64>::new();

        let itr = engine.sfile.series_id_iterator();
        for e in itr {
            if e.series_id == 0 {
                break;
            }

            let (name, tags) =
                series_file::parse_series_key(&engine.sfile.series_key(e.series_id)).unwrap();
            let tags_hash_key = tags.hash_key();
            let key = format!("{}{}", to_str!(&name), to_str!(&tags_hash_key));
            series_id_map.insert(key, e.series_id);
        }

        for (_, id) in series_id_map.iter() {
            if !engine.series_id_set().contains(*id) {
                panic!("bitmap does not contains ID: {}", *id);
            }
        }

        // Drop all the series for the gpu measurement and they should no longer
        // be in the series ID set.
        engine.engine.delete_measurement("gpu".as_bytes()).unwrap();

        if engine
            .series_id_set()
            .contains(*series_id_map.get("gpu").unwrap())
        {
            panic!(
                "bitmap does not contain ID: {} for key {}, but should",
                series_id_map.get("gpu").unwrap(),
                "gpu"
            );
        } else if engine
            .series_id_set()
            .contains(*series_id_map.get("gpu,host=b").unwrap())
        {
            panic!(
                "bitmap does not contain ID: {} for key {}, but should",
                series_id_map.get("gpu,host=b").unwrap(),
                "gpu"
            );
        }

        series_id_map.remove("gpu");
        series_id_map.remove("gpu,host=b");

        // Drop the specific mem series
        let ditr = TestSeriesIter {
            keys: vec!["mem,host=z".as_bytes().to_vec()],
        };
        engine
            .engine
            .delete_series_range(Box::new(ditr), i64::MIN, i64::MAX)
            .unwrap();

        if engine
            .series_id_set()
            .contains(*series_id_map.get("mem,host=z").unwrap())
        {
            panic!(
                "bitmap does not contain ID: {} for key {}, but should",
                series_id_map.get("mem,host=z").unwrap(),
                "mem,host=z"
            );
        }
        series_id_map.remove("mem,host=z");

        // The rest of the keys should still be in the set.
        for (key, id) in series_id_map.iter() {
            if !engine.series_id_set().contains(*id) {
                panic!("bitmap does not contains ID: {} for key {}", *id, key);
            }
        }

        // Reopen the engine, and the series should be re-added to the bitmap.
        engine.reopen();

        // Check bitset is expected.
        let mut exp = series_set::SeriesIDSet::new(&[]);
        for (_, id) in series_id_map.iter() {
            exp.add(*id);
        }

        if !engine.series_id_set().equals(&exp) {
            panic!(
                "got bitset: {:?}, exp: {:?}",
                engine.series_id_set().to_string(),
                exp.to_string()
            );
        }

        // FIXME: if donot close engine, the test cannot return.
        engine.close();
    }

    // Ensures that deleting series from TSM files with multiple fields removes all the series
    #[test]
    #[cfg(feature = "mock_compact_plan")] // mock the planner so compactions don't run during the test
    fn test_engine_delete_series() {
        // Create a few points.
        let mut p1 = must_parse_point_string("cpu,host=A value=1.1 1000000000");
        let mut p2 = must_parse_point_string("cpu,host=B value=1.2 2000000000");
        let mut p3 = must_parse_point_string("cpu,host=A sum=1.3 3000000000");

        let mut e = Engine::new("tsi1".to_string());
        e.engine.open().unwrap();

        e.write_points(&mut [p1, p2, p3]);
        super::Engine::write_snapshot(true, Arc::clone(&e.engine.inner)).unwrap();

        let keys = e.engine.file_store_keys();
        assert_eq!(
            keys.len(),
            3,
            "series count mismatch: got {}, exp {}",
            keys.len(),
            3
        );

        let itr = Box::new(TestSeriesIter::new(vec!["cpu,host=A".as_bytes().to_vec()]));

        e.engine
            .delete_series_range(itr, i64::MIN, i64::MAX)
            .unwrap();

        let keys = e.engine.file_store_keys();
        assert_eq!(
            keys.len(),
            1,
            "series count mismatch: got {}, exp {}",
            keys.len(),
            1
        );

        let exp = "cpu,host=B#!~#value";
        assert!(
            keys.contains_key(exp),
            "wrong series deleted: exp {}, got {:?}",
            exp,
            keys
        );

        // FIXME: if donot close engine, the test cannot return.
        e.close();
    }

    #[test]
    #[cfg(feature = "mock_compact_plan")]
    fn test_engine_delete_series_range() {
        // Create a few points.
        let p1 = must_parse_point_string("cpu,host=0 value=1.1 6000000000"); // Should not be deleted
        let p2 = must_parse_point_string("cpu,host=A value=1.2 2000000000"); // Should not be deleted
        let p3 = must_parse_point_string("cpu,host=A value=1.3 3000000000");
        let p4 = must_parse_point_string("cpu,host=B value=1.3 4000000000");
        let p5 = must_parse_point_string("cpu,host=B value=1.3 5000000000"); // Should not be deleted
        let p6 = must_parse_point_string("cpu,host=C value=1.3 1000000000");
        let p7 = must_parse_point_string("mem,host=C value=1.3 1000000000");
        let p8 = must_parse_point_string("disk,host=C value=1.3 1000000000"); // Should not be deleted

        let mut points = [p1, p2, p3, p4, p5, p6, p7, p8];

        let mut e = Engine::new("tsi1".to_string());
        e.engine.open().unwrap();

        for p in points.iter_mut() {
            let tags = p.tags();
            e.engine
                .create_series_if_not_exists(p.key(), &p.name(), tags)
                .unwrap();
        }

        e.engine.write_points(&points);

        super::Engine::write_snapshot(true, Arc::clone(&e.engine.inner)).unwrap();

        let keys = e.engine.file_store_keys(); // (key, type)
        assert_eq!(
            keys.len(),
            6,
            "series count mismatch: got {}, exp {}",
            keys.len(),
            6
        );

        let itr = Box::new(TestSeriesIter::new(vec![
            "cpu,host=0".as_bytes().to_vec(),
            "cpu,host=A".as_bytes().to_vec(),
            "cpu,host=B".as_bytes().to_vec(),
            "cpu,host=C".as_bytes().to_vec(),
        ]));

        e.engine.delete_series_range(itr, 0, 3000000000).unwrap();

        let keys = e.engine.file_store_keys();
        assert_eq!(
            keys.len(),
            4,
            "series count mismatch: got {}, exp {}",
            keys.len(),
            4
        );

        let exp = "cpu,host=B#!~#value";
        assert!(
            keys.contains_key(exp),
            "wrong series deleted: exp {}, got {:?}",
            exp,
            keys
        );

        // Check that the series still exists in the index
        let index_set = index::IndexSet::new(Arc::clone(&e.index), Arc::clone(&e.sfile), None);
        let mut iter = index_set
            .measurement_series_id_iter("cpu".as_bytes())
            .unwrap()
            .unwrap();

        let mut elem = iter.next();
        assert!(elem.is_some(), "series index mismatch: EOF, exp 2 series");
        let elem = elem.unwrap();
        assert!(elem.series_id != 0, "series id mismatch");

        // Lookup series.
        let (name, tags) = e.sfile.series(elem.series_id).unwrap();
        assert_eq!(
            &name,
            "cpu".as_bytes(),
            "series mismatch: got {:?}, exp {:?}",
            &name,
            "cpu".as_bytes()
        );

        let tags = tags.unwrap();
        if !tags.eq(&crate::models::points::Tags::from(HashMap::from([(
            "host".as_bytes().to_vec(),
            "0".as_bytes().to_vec(),
        )])))
            && !tags.eq(&crate::models::points::Tags::from(HashMap::from([(
                "host".as_bytes().to_vec(),
                "B".as_bytes().to_vec(),
            )])))
        {
            panic!(
                "series mismatch: got {:?}, exp either \"host=0\" or \"host=B\"",
                tags
            );
        }

        iter.close().unwrap();

        // Deleting remaining series should remove them from the series.
        let itr = Box::new(TestSeriesIter::new(vec![
            "cpu,host=0".as_bytes().to_vec(),
            "cpu,host=B".as_bytes().to_vec(),
        ]));

        e.engine.delete_series_range(itr, 0, 9000000000).unwrap();

        let index_set = index::IndexSet::new(Arc::clone(&e.index), Arc::clone(&e.sfile), None);
        let iter = index_set
            .measurement_series_id_iter("cpu".as_bytes())
            .unwrap();
        if iter.is_none() {
            e.close();
            return;
        }

        let mut iter = iter.unwrap();
        let elem = iter.next();
        assert!(
            elem.is_none(),
            "got an undeleted series id, but series should be dropped from index"
        );
        iter.close().unwrap();

        // FIXME: if donot close engine, the test cannot return.
        e.close();
    }

    #[test]
    #[cfg(feature = "mock_compact_plan")] // mock the planner so compactions don't run during the test
    fn test_engine_delete_series_range_with_predicate() {
        // Create a few points.
        let p1 = must_parse_point_string("cpu,host=A value=1.1 6000000000"); // Should not be deleted
        let p2 = must_parse_point_string("cpu,host=A value=1.2 2000000000"); // Should not be deleted
        let p3 = must_parse_point_string("cpu,host=B value=1.3 3000000000");
        let p4 = must_parse_point_string("cpu,host=B value=1.3 4000000000");
        let p5 = must_parse_point_string("cpu,host=C value=1.3 5000000000"); // Should not be deleted
        let p6 = must_parse_point_string("mem,host=B value=1.3 1000000000");
        let p7 = must_parse_point_string("mem,host=C value=1.3 1000000000");
        let p8 = must_parse_point_string("disk,host=C value=1.3 1000000000"); // Should not be deleted

        let mut points = [p1, p2, p3, p4, p5, p6, p7, p8];

        let mut e = Engine::new("tsi1".to_string());
        e.engine.open().unwrap();

        for p in points.iter_mut() {
            let tags = p.tags();
            e.engine
                .create_series_if_not_exists(p.key(), &p.name(), tags)
                .unwrap();
        }

        e.engine.write_points(&points);

        super::Engine::write_snapshot(true, Arc::clone(&e.engine.inner)).unwrap();

        let keys = e.engine.file_store_keys(); // (key, type)
        assert_eq!(
            keys.len(),
            6,
            "series count mismatch: got {}, exp {}",
            keys.len(),
            6
        );

        let itr = Box::new(TestSeriesIter::new(vec![
            "cpu,host=A".as_bytes().to_vec(),
            "cpu,host=B".as_bytes().to_vec(),
            "cpu,host=C".as_bytes().to_vec(),
            "mem,host=B".as_bytes().to_vec(),
            "mem,host=C".as_bytes().to_vec(),
        ]));

        e.engine
            .delete_series_range_with_predicate(
                itr,
                Some(
                    |name: &[u8], tags: &crate::models::Tags| -> (i64, i64, bool) {
                        if name == "mem".as_bytes() {
                            return (i64::MIN, i64::MAX, true);
                        }
                        if name == "cpu".as_bytes() {
                            for tag in tags.iter() {
                                if &tag.key == "host".as_bytes() && &tag.value == "B".as_bytes() {
                                    return (i64::MIN, i64::MAX, true);
                                }
                            }
                        }

                        (i64::MIN, i64::MAX, false)
                    },
                ),
            )
            .unwrap();

        let keys = e.engine.file_store_keys();
        assert_eq!(
            keys.len(),
            3,
            "series count mismatch: got {}, exp {}",
            keys.len(),
            3
        );

        let exps = [
            "cpu,host=A#!~#value",
            "cpu,host=C#!~#value",
            "disk,host=C#!~#value",
        ];
        for exp in exps.iter() {
            assert!(
                keys.contains_key(*exp),
                "wrong series deleted: exp {:?}, got {:?}",
                exps,
                keys
            );
        }

        // Check that the series still exists in the index
        let index_set = index::IndexSet::new(Arc::clone(&e.index), Arc::clone(&e.sfile), None);
        let mut iter = index_set
            .measurement_series_id_iter("cpu".as_bytes())
            .unwrap()
            .unwrap();

        let mut elem = iter.next();
        assert!(elem.is_some(), "series index mismatch: EOF, exp 2 series");
        let elem = elem.unwrap();
        assert!(elem.series_id != 0, "series id mismatch");

        // Lookup series.
        let (name, tags) = e.sfile.series(elem.series_id).unwrap();
        assert_eq!(
            &name,
            "cpu".as_bytes(),
            "series mismatch: got {:?}, exp {:?}",
            &name,
            "cpu".as_bytes()
        );

        let tags = tags.unwrap();
        if !tags.eq(&crate::models::points::Tags::from(HashMap::from([(
            "host".as_bytes().to_vec(),
            "A".as_bytes().to_vec(),
        )])))
            && !tags.eq(&crate::models::points::Tags::from(HashMap::from([(
                "host".as_bytes().to_vec(),
                "C".as_bytes().to_vec(),
            )])))
        {
            panic!(
                "series mismatch: got {:?}, exp either \"host=A\" or \"host=C\"",
                tags
            );
        }

        iter.close().unwrap();

        // Deleting remaining series should remove them from the series.
        let itr = Box::new(TestSeriesIter::new(vec![
            "cpu,host=A".as_bytes().to_vec(),
            "cpu,host=C".as_bytes().to_vec(),
        ]));

        e.engine.delete_series_range(itr, 0, 9000000000).unwrap();

        let index_set = index::IndexSet::new(Arc::clone(&e.index), Arc::clone(&e.sfile), None);
        let iter = index_set
            .measurement_series_id_iter("cpu".as_bytes())
            .unwrap();
        if iter.is_none() {
            e.close();
            return;
        }

        let mut iter = iter.unwrap();
        let elem = iter.next();
        assert!(
            elem.is_none(),
            "got an undeleted series id, but series should be dropped from index"
        );
        iter.close().unwrap();

        // FIXME: if donot close engine, the test cannot return.
        e.close();
    }

    // Tests that a None predicate deletes all values returned from the series iterator.
    #[test]
    #[cfg(feature = "mock_compact_plan")] // mock the planner so compactions don't run during the test
    fn test_engine_delete_series_range_with_predicate_none() {
        // Create a few points.
        let p1 = must_parse_point_string("cpu,host=A value=1.1 6000000000"); // Should not be deleted
        let p2 = must_parse_point_string("cpu,host=A value=1.2 2000000000"); // Should not be deleted
        let p3 = must_parse_point_string("cpu,host=B value=1.3 3000000000");
        let p4 = must_parse_point_string("cpu,host=B value=1.3 4000000000");
        let p5 = must_parse_point_string("cpu,host=C value=1.3 5000000000"); // Should not be deleted
        let p6 = must_parse_point_string("mem,host=B value=1.3 1000000000");
        let p7 = must_parse_point_string("mem,host=C value=1.3 1000000000");
        let p8 = must_parse_point_string("disk,host=C value=1.3 1000000000"); // Should not be deleted

        let mut points = [p1, p2, p3, p4, p5, p6, p7, p8];

        let mut e = Engine::new("tsi1".to_string());
        e.engine.open().unwrap();

        for p in points.iter_mut() {
            let tags = p.tags();
            e.engine
                .create_series_if_not_exists(p.key(), &p.name(), tags)
                .unwrap();
        }

        e.engine.write_points(&points);

        super::Engine::write_snapshot(true, Arc::clone(&e.engine.inner)).unwrap();

        let keys = e.engine.file_store_keys(); // (key, type)
        assert_eq!(
            keys.len(),
            6,
            "series count mismatch: got {}, exp {}",
            keys.len(),
            6
        );

        let itr = Box::new(TestSeriesIter::new(vec![
            "cpu,host=A".as_bytes().to_vec(),
            "cpu,host=B".as_bytes().to_vec(),
            "cpu,host=C".as_bytes().to_vec(),
            "mem,host=B".as_bytes().to_vec(),
            "mem,host=C".as_bytes().to_vec(),
        ]));

        let predicate: Option<fn(&[u8], &crate::models::Tags) -> (i64, i64, bool)> = None;
        e.engine
            .delete_series_range_with_predicate(itr, predicate)
            .unwrap();

        let keys = e.engine.file_store_keys();
        assert_eq!(
            keys.len(),
            1,
            "series count mismatch: got {}, exp {}",
            keys.len(),
            1
        );

        // Check that the series still exists in the index
        let index_set = index::IndexSet::new(Arc::clone(&e.index), Arc::clone(&e.sfile), None);
        let mut iter = index_set
            .measurement_series_id_iter("cpu".as_bytes())
            .unwrap();
        if iter.is_none() {
            e.close();
            return;
        }

        let mut iter = iter.unwrap();
        let mut elem = iter.next();
        assert!(elem.is_some(), "series index mismatch: EOF, exp 2 series");
        let elem = elem.unwrap();
        assert!(elem.series_id != 0, "series id mismatch");

        // Check that disk series still exists
        let mut iter = index_set
            .measurement_series_id_iter("disk".as_bytes())
            .unwrap()
            .unwrap();

        let elem = iter.next();
        assert!(
            elem.is_none(),
            "got an undeleted series id, but series should be dropped from index"
        );
        let elem = elem.unwrap();
        assert!(
            elem.series_id != 0,
            "got an undeleted series id, but series should be dropped from index"
        );

        iter.close().unwrap();

        // FIXME: if donot close engine, the test cannot return.
        e.close();
    }

    #[test]
    #[cfg(feature = "mock_compact_plan")] // mock the planner so compactions don't run during the test
    fn test_engine_delete_series_range_with_predicate_flush_batch() {
        // Create a few points.
        let p1 = must_parse_point_string("cpu,host=A value=1.1 6000000000"); // Should not be deleted
        let p2 = must_parse_point_string("cpu,host=A value=1.2 2000000000"); // Should not be deleted
        let p3 = must_parse_point_string("cpu,host=B value=1.3 3000000000");
        let p4 = must_parse_point_string("cpu,host=B value=1.3 4000000000");
        let p5 = must_parse_point_string("cpu,host=C value=1.3 5000000000"); // Should not be deleted
        let p6 = must_parse_point_string("mem,host=B value=1.3 1000000000");
        let p7 = must_parse_point_string("mem,host=C value=1.3 1000000000");
        let p8 = must_parse_point_string("disk,host=C value=1.3 1000000000"); // Should not be deleted

        let mut points = [p1, p2, p3, p4, p5, p6, p7, p8];

        let mut e = Engine::new("tsi1".to_string());
        e.engine.open().unwrap();

        for p in points.iter_mut() {
            let tags = p.tags();
            e.engine
                .create_series_if_not_exists(p.key(), &p.name(), tags)
                .unwrap();
        }

        e.engine.write_points(&points);

        super::Engine::write_snapshot(true, Arc::clone(&e.engine.inner)).unwrap();

        let keys = e.engine.file_store_keys(); // (key, type)
        assert_eq!(
            keys.len(),
            6,
            "series count mismatch: got {}, exp {}",
            keys.len(),
            6
        );

        let itr = Box::new(TestSeriesIter::new(vec![
            "cpu,host=A".as_bytes().to_vec(),
            "cpu,host=B".as_bytes().to_vec(),
            "cpu,host=C".as_bytes().to_vec(),
            "mem,host=B".as_bytes().to_vec(),
            "mem,host=C".as_bytes().to_vec(),
        ]));

        e.engine
            .delete_series_range_with_predicate(
                itr,
                Some(
                    |name: &[u8], tags: &crate::models::Tags| -> (i64, i64, bool) {
                        if name == "mem".as_bytes() {
                            return (1000000000, 1000000000, true);
                        }
                        if name == "cpu".as_bytes() {
                            for tag in tags.iter() {
                                if &tag.key == "host".as_bytes() && &tag.value == "B".as_bytes() {
                                    return (3000000000, 4000000000, true);
                                }
                            }
                        }

                        (i64::MIN, i64::MAX, false)
                    },
                ),
            )
            .unwrap();

        let keys = e.engine.file_store_keys();
        assert_eq!(
            keys.len(),
            3,
            "series count mismatch: got {}, exp {}",
            keys.len(),
            3
        );

        let exps = [
            "cpu,host=A#!~#value",
            "cpu,host=C#!~#value",
            "disk,host=C#!~#value",
        ];
        for exp in exps.iter() {
            assert!(
                keys.contains_key(*exp),
                "wrong series deleted: exp {:?}, got {:?}",
                exps,
                keys
            );
        }

        // Check that the series still exists in the index
        let index_set = index::IndexSet::new(Arc::clone(&e.index), Arc::clone(&e.sfile), None);
        let mut iter = index_set
            .measurement_series_id_iter("cpu".as_bytes())
            .unwrap()
            .unwrap();

        let mut elem = iter.next();
        assert!(elem.is_some(), "series index mismatch: EOF, exp 2 series");
        let elem = elem.unwrap();
        assert!(elem.series_id != 0, "series id mismatch");

        // Lookup series.
        let (name, tags) = e.sfile.series(elem.series_id).unwrap();
        assert_eq!(
            &name,
            "cpu".as_bytes(),
            "series mismatch: got {:?}, exp {:?}",
            &name,
            "cpu".as_bytes()
        );

        let tags = tags.unwrap();
        if !tags.eq(&crate::models::points::Tags::from(HashMap::from([(
            "host".as_bytes().to_vec(),
            "A".as_bytes().to_vec(),
        )])))
            && !tags.eq(&crate::models::points::Tags::from(HashMap::from([(
                "host".as_bytes().to_vec(),
                "C".as_bytes().to_vec(),
            )])))
        {
            panic!(
                "series mismatch: got {:?}, exp either \"host=A\" or \"host=C\"",
                tags
            );
        }

        iter.close().unwrap();

        // Deleting remaining series should remove them from the series.
        let itr = Box::new(TestSeriesIter::new(vec![
            "cpu,host=A".as_bytes().to_vec(),
            "cpu,host=C".as_bytes().to_vec(),
        ]));

        e.engine.delete_series_range(itr, 0, 9000000000).unwrap();

        let index_set = index::IndexSet::new(Arc::clone(&e.index), Arc::clone(&e.sfile), None);
        let iter = index_set
            .measurement_series_id_iter("cpu".as_bytes())
            .unwrap();
        if iter.is_none() {
            e.close();
            return;
        }

        let mut iter = iter.unwrap();
        let elem = iter.next();
        assert!(
            elem.is_none(),
            "got an undeleted series id, but series should be dropped from index"
        );

        iter.close().unwrap();

        // FIXME: if donot close engine, the test cannot return.
        e.close();
    }

    #[test]
    #[cfg(feature = "mock_compact_plan")] // mock the planner so compactions don't run during the test
    fn test_engine_delete_series_range_outside_time() {
        // Create a few points.
        let p1 = must_parse_point_string("cpu,host=A value=1.1 1000000000"); // Should not be deleted

        let mut points = [p1];

        let mut e = Engine::new("tsi1".to_string());
        e.engine.open().unwrap();

        for p in points.iter_mut() {
            let tags = p.tags();
            e.engine
                .create_series_if_not_exists(p.key(), &p.name(), tags)
                .unwrap();
        }

        e.engine.write_points(&points);

        super::Engine::write_snapshot(true, Arc::clone(&e.engine.inner)).unwrap();

        let keys = e.engine.file_store_keys(); // (key, type)
        assert_eq!(
            keys.len(),
            1,
            "series count mismatch: got {}, exp {}",
            keys.len(),
            1
        );

        let itr = Box::new(TestSeriesIter::new(vec!["cpu,host=A".as_bytes().to_vec()]));

        e.engine.delete_series_range(itr, 0, 0).unwrap();

        let keys = e.engine.file_store_keys();
        assert_eq!(
            keys.len(),
            1,
            "series count mismatch: got {}, exp {}",
            keys.len(),
            1
        );

        let exp = "cpu,host=A#!~#value";
        assert!(
            keys.contains_key(exp),
            "wrong series deleted: exp {:?}, got {:?}",
            exp,
            keys
        );

        // Check that the series still exists in the index
        let mut iter = e
            .index
            .measurement_series_id_iter("cpu".as_bytes())
            .unwrap()
            .unwrap();

        let mut elem = iter.next();
        assert!(elem.is_some(), "series index mismatch: EOF, exp 1 series");
        let elem = elem.unwrap();
        assert!(elem.series_id != 0, "series id mismatch");

        // Lookup series.
        let (name, tags) = e.sfile.series(elem.series_id).unwrap();
        assert_eq!(
            &name,
            "cpu".as_bytes(),
            "series mismatch: got {:?}, exp {:?}",
            &name,
            "cpu".as_bytes()
        );

        let tags = tags.unwrap();
        if !tags.eq(&crate::models::points::Tags::from(HashMap::from([(
            "host".as_bytes().to_vec(),
            "A".as_bytes().to_vec(),
        )]))) {
            panic!("series mismatch: got {:?}, exp either \"host=A\"", tags);
        }

        iter.close().unwrap();
        drop(iter); // must drop iter to release index.partition.file_set reference count

        // FIXME: if donot close engine, the test cannot return.
        e.close();
    }

    #[test]
    #[cfg(feature = "mock_compact_plan")] // mock the planner so compactions don't run during the test
    fn test_engine_last_modified() {
        // Create a few points.
        let p1 = must_parse_point_string("cpu,host=A value=1.1 1000000000");
        let p2 = must_parse_point_string("cpu,host=B value=1.2 2000000000");
        let p3 = must_parse_point_string("cpu,host=A sum=1.3 3000000000");

        let mut points = [p1, p2, p3];

        let mut e = Engine::new("tsi1".to_string());
        e.engine.set_enabled(false);
        e.engine.open().unwrap();

        e.engine.write_points(&points);

        let lm = e.engine.last_modified();
        if lm.is_zero() {
            panic!("expected non-zero time, got {:?}", lm);
        }

        e.engine.set_enabled(true);

        // Artificial sleep added due to filesystems caching the mod time
        // of files.  This prevents the WAL last modified time from being
        // returned and newer than the filestore's mod time.

        std::thread::sleep(std::time::Duration::from_secs(2)); // // Covers most filesystems.

        super::Engine::write_snapshot(true, Arc::clone(&e.engine.inner)).unwrap();

        let lm2 = e.engine.last_modified();
        if lm == lm2 {
            panic!(
                "expected time change, got true, exp false: {:?} == {:?}",
                lm, lm2
            );
        }

        let itr = Box::new(TestSeriesIter::new(vec!["cpu,host=A".as_bytes().to_vec()]));
        e.engine
            .delete_series_range(itr, i64::MIN, i64::MAX)
            .unwrap();

        let lm3 = e.engine.last_modified();
        if lm2 == lm3 {
            panic!(
                "expected time change, got true, exp false: {:?} == {:?}",
                lm, lm2
            );
        }

        // FIXME: if donot close engine, the test cannot return.
        e.close();
    }

    #[test]
    #[cfg(feature = "mock_compact_plan")] // mock the planner so compactions don't run during the test
    fn test_engine_snapshots_disabled() {
        let sfile = Arc::new(must_open_series_file().0);

        // Generate temporary file.
        let dir = Arc::new(must_temp_tsm_dir().into_path());
        let wal_path = Arc::new(dir.join("wal"));
        let index_wal_path = Arc::new(dir.join("index_wal_path"));
        let vlog_path = Arc::new(dir.join("vlog_path"));

        // Create a tsm1 engine.
        let db = dir.file_name().unwrap().to_str().unwrap().to_string();
        let mut opt = EngineOptions::default();
        opt.compaction_limiter = None; // close level compaction.
        let idx = Arc::new(crate::index::must_open_active_index(
            1,
            db,
            &dir.join("index"),
            Arc::new(SeriesIDSet::new(&[])),
            Arc::clone(&sfile),
            &opt,
        ));

        let mut e = super::Engine::new(
            1,
            Arc::clone(&idx),
            Arc::clone(&dir),
            wal_path,
            index_wal_path,
            vlog_path,
            Arc::clone(&sfile),
            opt,
        )
        .unwrap();

        e.set_enabled(false);
        e.open().unwrap();

        // Make sure Snapshots are disabled.
        e.set_compactions_enabled(false);
        e.inner.read().compactor.disable_snapshots();

        // Writing a snapshot should not fail when the snapshot is empty
        // even if snapshots are disabled.
        super::Engine::write_snapshot(true, Arc::clone(&e.inner)).unwrap();

        sfile.close().unwrap();
        idx.close().unwrap();

        e.close().unwrap();

        std::fs::remove_dir_all(dir.as_path());
    }

    #[test]
    #[cfg(feature = "mock_compact_plan")] // mock the planner so compactions don't run during the test
    fn test_engine_should_compact_cache() {
        let now = crate::common::time::Time::now();

        let mut e = Engine::new("tsi1".to_string());
        e.engine.set_enabled(false);
        e.engine.open().unwrap();

        {
            let mut inner = e.engine.inner.write();
            inner.cache_flush_memory_size_threshold = 1024;
            inner.cache_flush_write_cold_duration = std::time::Duration::from_mins(1);

            if inner.should_compact_cache(now) {
                panic!("nothing written to cache, so should not compact");
            }
        }

        e.write_points_string(&["m,k=v f=3i"]).unwrap();

        {
            let mut inner = e.engine.inner.write();

            if inner.should_compact_cache(now) {
                panic!(
                    "cache size < flush threshold and nothing written to FileStore, so should not compact"
                );
            }

            if !inner
                .should_compact_cache(now.checked_add(std::time::Duration::from_hours(1)).unwrap())
            {
                panic!(
                    "last compaction was longer than flush write cold threshold, so should compact"
                );
            }

            inner.cache_flush_memory_size_threshold = 1;
            if !inner.should_compact_cache(now) {
                panic!("cache size > flush threshold, so should compact");
            }
        }

        // FIXME: if donot close engine, the test cannot return.
        e.close();
    }

    // // Ensure engine can create an ascending cursor for cache and tsm values.
    // #[test]
    // fn test_engine_create_cursor_ascending() {
    //     let mut e = must_open_engine("tsi1".to_string());

    //     e.engine
    //         .measurement_fields("cpu".as_bytes())
    //         .create_field_if_not_exists("value".as_bytes(), crate::models::FLOAT)
    //         .unwrap();
    //     e.engine
    //     .create_series_if_not_exists(
    //         "cpu,host=A".as_bytes(),
    //         "cpu".as_bytes(),
    //         crate::models::Tags::from(HashMap::from([(
    //             "host".as_bytes().to_vec(),
    //             "A".as_bytes().to_vec(),
    //         )])),
    //     )
    //     .unwrap();

    //     e.write_points_string(&[
    //         "cpu,host=A value=1.1 1",
    //         "cpu,host=A value=1.2 2",
    //         "cpu,host=A value=1.3 3",
    //     ]).unwrap();

    //     e.must_write_snapshot();

    //     e.write_points_string(&[
    //         "cpu,host=A value=10.1 10",
    //         "cpu,host=A value=11.2 11",
    //         "cpu,host=A value=12.3 12",
    //     ]).unwrap();

    // }

    // // Ensure engine can create an ascending cursor for tsm values.
    // #[test]
    // fn test_engine_create_cursor_descending() {}

    // This test ensures that "sync: WaitGroup is reused before previous Wait has returned" is
    // is not raised.
    #[test]
    fn test_engine_disable_enable_compactions_concurrent() {
        let e = Arc::new(RwLock::new(must_open_engine("tsi1".to_string())));

        let wg = WaitGroup::new();
        wg.add(2);

        let wg1 = wg.clone();
        let e1 = Arc::clone(&e);
        spawn_blocking_global(move || {
            let e = e1.read();
            for i in 0..1000 {
                e.engine.set_compactions_enabled(true);
                e.engine.set_compactions_enabled(false);
            }
            wg1.done();
        });

        let wg2 = wg.clone();
        let e2 = Arc::clone(&e);
        spawn_blocking_global(move || {
            let e = e2.read();
            for i in 0..1000 {
                e.engine.set_compactions_enabled(false);
                e.engine.set_compactions_enabled(true);
            }
            wg2.done();
        });

        let done = tokio_util::sync::CancellationToken::new();
        let done2 = done.clone();
        spawn_blocking_global(move || {
            wg.wait();
            done2.cancel();
        });

        block_on_global(async {
            tokio::select! {
                _ = done.cancelled() => {}
                _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                    panic!("timed out after 30 seconds waiting for waitgroup")
                }
            }
        });

        // FIXME: if donot close engine, the test cannot return.
        e.write().close();
    }

    #[test]
    fn test_engine_reenable_compactions_uses_fresh_cancellation_generation() {
        let mut e = must_open_engine("tsi1".to_string());

        e.engine.set_compactions_enabled(false);
        e.engine.set_compactions_enabled(true);
        let first_generation = e.engine.inner.read().done.clone().unwrap();

        e.engine.set_compactions_enabled(false);
        assert!(first_generation.is_cancelled());

        e.engine.set_compactions_enabled(true);
        let second_generation = e.engine.inner.read().done.clone().unwrap();
        assert!(!second_generation.is_cancelled());

        e.close();
    }

    // TODO: use radix tree to check series type confilict
    // #[test]
    // fn test_engine_write_points_type_conflict() {
    //     // os.Setenv("INFLUXDB_SERIES_TYPE_CHECK_ENABLED", "1")
    //     // defer os.Unsetenv("INFLUXDB_SERIES_TYPE_CHECK_ENABLED")
    // }

    // TODO: use radix tree to check series type confilict
    // #[test]
    // fn test_engine_write_points_reload() {
    //     // t.Skip("Disabled until INFLUXDB_SERIES_TYPE_CHECK_ENABLED is enabled by default")

    //     let mut e = must_open_engine("tsi1".to_string());

    //     e.write_points_string(&["cpu,host=A value=1.1 1"]).unwrap();

    //     // Series type should be a float
    //     let typ = e
    //         .engine
    //         .field_type(super::series_field_key(
    //             "cpu,host=A".to_string(),
    //             "value".to_string(),
    //         ).as_bytes())
    //         .unwrap();

    //     assert_eq!(
    //         typ,
    //         crate::models::points::FLOAT,
    //         "field type mismatch: got {}, exp {}",
    //         typ,
    //         crate::models::points::FLOAT
    //     );

    //     super::Engine::write_snapshot(true, Arc::clone(&e.engine.inner)).unwrap();

    //     e.reopen();

    //     match e.write_points_string(&["cpu,host=A value=1i 1"]) {
    //         Ok(_) => panic!("expected field type conflict"),
    //         Err(e) => match &e {
    //             engine::Error::Cache { source } => match source {
    //                 crate::storage::tsm1::cache::Error::ErrFieldTypeConflict => {},
    //                 _ => panic!("expected field type conflict, got: {:?}", source),
    //             },
    //             engine::Error::MeasurementFields { source } => {
    //                 match source {
    //                     crate::series::measurement_fields::Error::FieldTypeConflict => {},
    //                     _ => panic!("expected field type conflict, got: {:?}", source),
    //                 }
    //             },
    //             _ => panic!("expected field type conflict, got: {:?}", e),
    //         }
    //     }

    //     e.close();
    // }

    #[test]
    #[cfg(feature = "mock_compact_plan")] // mock the planner so compactions don't run during the test
    fn test_engine_invalid_utf8() {
        let name: &[u8] = &[255, 112, 114, 111, 99]; // A known invalid UTF-8 string
        let field: &[u8] = &[255, 110, 101, 116]; // A known invalid UTF-8 string

        // Create a string from the invalid UTF-8 bytes
        let point_str = format!(
            "{:?},host=A {}=1.1 6000000000",
            String::from_utf8_lossy(name),
            String::from_utf8_lossy(field)
        );
        let mut p = must_parse_point_string(&point_str);

        let mut e = Engine::new("tsi1".to_string());
        e.engine.open().unwrap();

        let tags = p.tags();
        e.engine
            .create_series_if_not_exists(p.key(), &p.name(), tags)
            .unwrap();

        e.engine.write_points(&[p]);

        // Re-open the engine
        e.reopen();

        e.close();
    }

    #[test]
    #[ignore]
    fn test_engine_write_large_parallel() {
        let e = Arc::new(must_open_engine("tsi1".to_string()));
        println!("### path: {:?}", e.engine.path());

        e.engine
            .measurement_fields("cpu".as_bytes())
            .create_field_if_not_exists("value".as_bytes(), crate::models::FLOAT)
            .unwrap();

        let now = std::time::Instant::now();
        let sz = 200000 * 50;
        let num_cpus = 16;
        let points = Arc::new(generate_data_with_tag_count(sz, num_cpus, 10));
        println!(
            "### generate {} points elapsed: {:?}, now: {:?}",
            sz * num_cpus,
            now.elapsed(),
            crate::common::time::Time::now()
        );

        let now = std::time::Instant::now();
        let wg = WaitGroup::new();
        for i in 0..num_cpus {
            let wg = wg.clone();
            wg.add(1);
            let points = Arc::clone(&points);
            let e = Arc::clone(&e);
            let (from, to) = (i * sz, (i + 1) * sz);
            spawn_blocking_global(move || {
                let mut start = from;
                while start + 40000 < to {
                    e.engine
                        .write_points(&points.as_slice()[start..start + 40000])
                        .unwrap();
                    start += 40000;
                }
                if start < to {
                    e.engine
                        .write_points(&points.as_slice()[start..to])
                        .unwrap();
                }
                drop(e);
                wg.done();
            });
        }
        wg.wait();
        println!(
            "### {} tasks, each task write {} points,  total elapsed {:?}, now: {:?}",
            num_cpus,
            sz,
            now.elapsed(),
            crate::common::time::Time::now()
        );

        Arc::into_inner(e).unwrap().close_without_remove_file();
    }

    use std::io::{BufRead, BufReader};
    #[test]
    #[ignore]
    fn test_write_large_from_file() {
        let e = Arc::new(must_open_engine("tsi1".to_string()));
        println!("### path: {:?}", e.engine.path());

        // 创建索引
        e.engine
            .measurement_fields("cpu".as_bytes())
            .create_field_if_not_exists("usage_user".as_bytes(), crate::models::FLOAT)
            .unwrap();
        e.engine
            .measurement_fields("cpu".as_bytes())
            .create_field_if_not_exists("usage_system".as_bytes(), crate::models::FLOAT)
            .unwrap();
        e.engine
            .measurement_fields("cpu".as_bytes())
            .create_field_if_not_exists("usage_idle".as_bytes(), crate::models::FLOAT)
            .unwrap();
        e.engine
            .measurement_fields("cpu".as_bytes())
            .create_field_if_not_exists("usage_nice".as_bytes(), crate::models::FLOAT)
            .unwrap();
        e.engine
            .measurement_fields("cpu".as_bytes())
            .create_field_if_not_exists("usage_iowait".as_bytes(), crate::models::FLOAT)
            .unwrap();
        e.engine
            .measurement_fields("cpu".as_bytes())
            .create_field_if_not_exists("usage_irq".as_bytes(), crate::models::FLOAT)
            .unwrap();
        e.engine
            .measurement_fields("cpu".as_bytes())
            .create_field_if_not_exists("usage_softirq".as_bytes(), crate::models::FLOAT)
            .unwrap();
        e.engine
            .measurement_fields("cpu".as_bytes())
            .create_field_if_not_exists("usage_steal".as_bytes(), crate::models::FLOAT)
            .unwrap();
        e.engine
            .measurement_fields("cpu".as_bytes())
            .create_field_if_not_exists("usage_guest".as_bytes(), crate::models::FLOAT)
            .unwrap();
        e.engine
            .measurement_fields("cpu".as_bytes())
            .create_field_if_not_exists("usage_guest_nice".as_bytes(), crate::models::FLOAT)
            .unwrap();

        // read from file
        // let now: std::time::Instant = std::time::Instant::now();
        // let sz = 200000 * 50;
        // let num_cpus = 16;
        // let points = Arc::new(generate_data_with_tag_count(sz, num_cpus, 10));
        // println!(
        //     "### generate {} points elapsed: {:?}, now: {:?}",
        //     sz * num_cpus,
        //     now.elapsed(),
        //     crate::common::time::Time::now()
        // );

        // ///////////////////////////////////////////////////////////////
        // ///////////////////////////////////////////////////////////////
        // ///////////////////////////////////////////////////////////////

        // let sz = 200000 * 50;
        // let num_cpus = 16;

        const THREAD_COUNT: usize = 32;
        const CHUNK_SIZE: usize = 3000;

        let file =
            std::fs::File::open("testdata/tsbs.lp")
                .unwrap();
        let reader = BufReader::new(file);

        let now = std::time::Instant::now();

        let mut senders = Vec::with_capacity(THREAD_COUNT);
        // let mut handles = Vec::with_capacity(THREAD_COUNT);

        let wg = WaitGroup::new();
        for _ in 0..THREAD_COUNT {
            let wg = wg.clone();
            wg.add(1);

            let (tx, rx) = std::sync::mpsc::channel::<Vec<crate::models::Point>>();
            let e = Arc::clone(&e);

            // let handle = thread::spawn(move || {
            //     while let Ok(chunk) = rx.recv() {
            //         e.engine.write_points(&chunk);
            //     }
            // });

            spawn_blocking_global(move || {
                while let Ok(chunk) = rx.recv() {
                    e.engine
                        .write_points(&chunk)
                        .expect("### engine.write_points failed: ");
                }

                drop(e);
                wg.done();
            });

            senders.push(tx);
            // handles.push(handle);
        }

        let mut buffer = Vec::with_capacity(CHUNK_SIZE);
        let mut chunk_index = 0;

        for line in reader.lines() {
            let line = line.unwrap();

            let p = must_parse_point_string(&line);

            buffer.push(p);

            if buffer.len() >= CHUNK_SIZE {
                let target_thread = chunk_index % THREAD_COUNT;
                senders[target_thread]
                    .send(buffer)
                    .expect("Failed to send chunk");

                buffer = Vec::with_capacity(CHUNK_SIZE);
                chunk_index += 1;
            }
        }

        // send the last data
        if !buffer.is_empty() {
            let target_thread = chunk_index % THREAD_COUNT;
            senders[target_thread]
                .send(buffer)
                .expect("Failed to send last chunk");
        }

        // notify threads to exit
        drop(senders);

        // wait for all threads close
        // for handle in handles {
        //     handle.join().unwrap();
        // }

        wg.wait();

        println!(
            "### {} tasks, each task write {} points,  total elapsed {:?}, now: {:?}",
            THREAD_COUNT,
            CHUNK_SIZE,
            now.elapsed(),
            crate::common::time::Time::now()
        );

        println!("✅ All data written successfully, in ordered assignment.");

        // ////////////////////////////////////////
        // ////////////////////////////////////////
        // ////////////////////////////////////////

        // write concurrently
        // let now = std::time::Instant::now();
        // let wg = WaitGroup::new();
        // for i in 0..num_cpus {
        //     let wg = wg.clone();
        //     wg.add(1);
        //     let points = Arc::clone(&points);
        //     let e = Arc::clone(&e);
        //     let (from, to) = (i * sz, (i + 1) * sz);
        //     spawn_blocking_global(move || {
        //         let mut start = from;
        //         while start + 40000 < to {
        //             e.engine
        //                 .write_points(&points.as_slice()[start..start + 40000])
        //                 .unwrap();
        //             start += 40000;
        //         }
        //         if start < to {
        //             e.engine
        //                 .write_points(&points.as_slice()[start..to])
        //                 .unwrap();
        //         }
        //         drop(e);
        //         wg.done();
        //     });
        // }
        // wg.wait();
        // println!(
        //     "### {} tasks, each task write {} points,  total elapsed {:?}, now: {:?}",
        //     num_cpus,
        //     sz,
        //     now.elapsed(),
        //     crate::common::time::Time::now()
        // );

        Arc::into_inner(e).unwrap().close_without_remove_file();
    }

    /*
        1. ReefsDB, disable vlog, 10G,2G,1000
        path: /tmp/tsm1-testjWlZAK
        24.599105ms -> 305.002816ms
        29.102555ms -> 316.21383ms
        24.339926ms -> 306.971369ms

        2. ReefsDB, enable vlog, 10G,2G,4000
        path: /tmp/tsm1-testAx5qVu
        size: 2.3G
        ### generate 320000000 points elapsed: 239.276610515s
        ### 32 tasks, each task write 10000000 points,  total elapsed 45.069309636s (7,111,111 points/s)
        ### query start_time: 1000000000, end_time: 20000001000000000, estimated total_count: 625001
        ### create iter elapsed: 126.838715ms
        ### query 625001 points(totoal 320000000 points), elapsed 383.156702ms
        10.66485ms -> 430.228373ms
        11.653937ms -> 447.773934ms
        10.498957ms -> 429.088362ms

        3. ReefsDB, enable vlog, use vlog_wal, 10G,2G,4000
        path: /tmp/tsm1-testDa8lsj
        size: 2.3G
        ### generate 320000000 points elapsed: 188.437981655s
        ### 32 tasks, each task write 10000000 points,  total elapsed 24.385845455s (12,800,000 points/s)
        ### query start_time: 1000000000, end_time: 20000001000000000, estimated total_count: 625001
        ### create iter elapsed: 11.108661ms
        ### query 625001 points(totoal 320000000 points), elapsed 428.63519ms


        4. ReefsDB, enable vlog, disable wal, 10G,2G,4000
        path: /tmp/tsm1-testdNW1qj/
        size: 2.3G
        ### generate 320000000 points elapsed: 192.723803265s
        ### 32 tasks, each task write 10000000 points,  total elapsed 6.59475099s (48,523,439 points/s)
        ### query start_time: 1000000000, end_time: 20000001000000000, estimated total_count: 625001
        ### create iter elapsed: 16.12761ms
        ### query 625001 points(totoal 320000000 points), elapsed 434.706082ms

        5. influxdb, 10G,2G,1000
        path: /tmp/tsm1-3811352398
        size: 2.3G
        #### generate 320000000 points, elapsed: 5m37.867824024s
        ### 32 tasks, each task write 10000000 points,  total elapsed 50.654451633s (6,400,000 points/s)
        ### query startTime: 1000000000, endTime: 20000001000000000, estimated totalCount: 625001
        ### create iterator elapsed 666.058636ms
        ### query 625001 points(total 320000000 points) elapsed 215.802121ms

        6. influxdb, disable wal, 10G,2G,1000
        path: /tmp/tsm1-3811352398
        size: 2.3G
        #### generate 320000000 points, elapsed: 5m37.534473975s
        ### 32 tasks, each task write 10000000 points,  total elapsed 42.956595113s (7,449,379 points/s)
        ### create iterator elapsed 9.632774ms
        ### query 625001 points(total 320000000 points) elapsed 646.337886ms

        compact cache:
        1. influxdb: (190MB/s)
            2,063MB -> 11s
            693MB -> 3.7s
        2. ReefsDB: (190MB/s)
            2167 MB -> 21.4s
            2518 MB -> 9s
            196 MB -> 1s
        3. ReefsDB_enable_vlog: (482MB/s)
            3,619MB -> 7.5s
            1,263MB -> 3.3s
    */

    fn generate_data(sz: usize, num_cpus: usize) -> Vec<crate::models::points::Point> {
        let mut points = Vec::with_capacity(sz * num_cpus);
        for i in 0..sz * num_cpus {
            let p = must_parse_point_string(&format!("cpu,host={} value={}", i, i));
            points.push(p);
        }

        points
    }

    pub fn generate_data_with_tag_count(
        sz: usize,
        num_cpus: usize,
        tag_count: usize,
    ) -> Vec<crate::models::points::Point> {
        let mut points = Vec::with_capacity(sz * num_cpus);
        for i in 0..sz * num_cpus {
            // 1. sequence value
            // let mut p = Point::new(
            //     format!("cpu,host={:08}", i % tag_count).as_bytes(),
            //     format!("value={}", i).as_bytes(),
            //     &[],
            // );
            // p.time = Time::from_timestamp_nanos((i + 1) as i64 * 1_000_000_000);

            // 2. random value
            let mut p = Point::new(
                format!("cpu,host={:08}", i % tag_count + 1).as_bytes(),
                format!("value={}", rand::thread_rng().gen_range(-40.0..1.3e5)).as_bytes(),
                &[],
            );
            p.time = Time::from_timestamp_nanos((i + 1) as i64 * 1_000_000_000);
            points.push(p);

            if (i + 1) % 10000000 == 0 {
                println!("generate point count: {}", i + 1);
            }
        }

        points
    }

    #[cfg(test)]
    #[ctor::ctor]
    fn init_runtime() {
        let runtime_opt = crate::runtime::global::RuntimeOptions::default();
        crate::runtime::global::init_global_runtimes(&runtime_opt);
    }

    // Engine is a test wrapper for tsm1.Engine.
    pub struct Engine {
        pub engine: super::Engine,
        root: PathBuf,
        index_path: PathBuf,
        index_type: String,
        vlog_enabled: bool,
        pub index: Arc<crate::index::ActiveIndex>,
        pub sfile: Arc<crate::series::series_file::SeriesFile>,
    }

    unsafe impl Send for Engine {}

    impl Engine {
        /// Returns a new instance of Engine at a temporary location.
        fn new(index: String) -> Self {
            Self::with_path(index, "")
        }

        fn with_path(index: String, path: impl AsRef<Path>) -> Self {
            Self::with_path_and_vlog(index, path, false)
        }

        fn with_vlog(index: String) -> Self {
            Self::with_path_and_vlog(index, "", true)
        }

        pub(super) fn with_aggregate_index() -> Self {
            Self::with_aggregate_query_options(Default::default())
        }

        pub(super) fn with_native_vlog() -> Self {
            let mut engine = Self::with_path_vlog_executor("tsi1".into(), "", true, 2, None);
            engine.engine.enable_compactions_on_open = false;
            engine.engine.open().unwrap();
            engine
        }

        #[cfg(all(feature = "slow-trace", feature = "benchmarking"))]
        pub(super) fn with_performance_mode(aggregate: bool) -> Self {
            let mut e = Self::with_vlog("tsi1".into());
            e.engine.close().unwrap();
            let mut opt = EngineOptions::default();
            opt.vlog_enabled = true;
            opt.agg_index_enabled = aggregate;
            opt.vlog_config.min_vlog_size = 0;
            opt.vlog_config.max_vlog_writers = 2;
            e.engine = super::Engine::new(1, Arc::clone(&e.index),
                Arc::new(e.root.join("data")), Arc::new(e.root.join("wal")),
                Arc::new(e.root.join("vlog_index_wal")), Arc::new(e.root.join("vlog")),
                Arc::clone(&e.sfile), opt).unwrap();
            e.engine.enable_compactions_on_open = false;
            e.engine.open().unwrap();
            e
        }

        pub(super) fn with_aggregate_query_options(query_options: crate::storage::AggregateQueryOptions) -> Self {
            Self::with_aggregate_options(query_options, "default")
        }

        pub(super) fn with_aggregate_options(query_options: crate::storage::AggregateQueryOptions, planner: &str) -> Self {
            let mut e = Self::with_vlog("tsi1".into());
            e.engine.close().unwrap();
            let mut opt = EngineOptions::default();
            opt.vlog_enabled = true;
            opt.agg_index_enabled = true;
            opt.agg_query = query_options;
            opt.vlog_compact_plan = planner.into();
            opt.vlog_config.max_vlog_writers = 2;
            // Keep the default nonzero small-block threshold to verify it is ignored.
            e.engine = super::Engine::new(1, Arc::clone(&e.index),
                Arc::new(e.root.join("data")), Arc::new(e.root.join("wal")),
                Arc::new(e.root.join("vlog_index_wal")), Arc::new(e.root.join("vlog")),
                Arc::clone(&e.sfile), opt).unwrap();
            e.engine.enable_compactions_on_open = false;
            e.engine.open().unwrap();
            e
        }

        fn with_path_and_vlog(index: String, path: impl AsRef<Path>, vlog_enabled: bool) -> Self {
            Self::with_path_vlog_executor(index, path, vlog_enabled, 1, None)
        }

        fn with_path_vlog_executor(
            index: String,
            path: impl AsRef<Path>,
            vlog_enabled: bool,
            max_vlog_writers: i32,
            executor: Option<crate::runtime::execution::ExecutorRef>,
        ) -> Self {
            Self::with_path_vlog_executor_options(
                index,
                path,
                vlog_enabled,
                max_vlog_writers,
                executor,
                None,
            )
        }

        fn with_path_vlog_executor_options(
            index: String,
            path: impl AsRef<Path>,
            vlog_enabled: bool,
            max_vlog_writers: i32,
            executor: Option<crate::runtime::execution::ExecutorRef>,
            vlog_compaction: Option<(&str, i64)>,
        ) -> Self {
            let root = if path.as_ref().as_os_str().is_empty() {
                tempfile::TempDir::with_prefix("tsm1-test")
                    .expect("unable to create tsm temporary working directory")
                    .into_path()
            } else {
                path.as_ref().to_path_buf()
            };

            let db = "db0";
            let db_path = root.join("data").join(db);

            fs::create_dir_all(&db_path).unwrap();
            let permissions = fs::Permissions::from_mode(0o777);
            fs::set_permissions(&db_path, permissions).unwrap();

            // Setup series file.
            let mut sfile = SeriesFile::new(db_path.join(SERIES_FILE_DIRECTORY));
            sfile.open().unwrap();
            let sfile = Arc::new(sfile);

            let mut opt = EngineOptions::default();
            // opt.compaction_limiter = None; // close level compaction.
            // opt.compaction_throughput_limiter = None; // close write snapshot rate limit
            opt.index_version = index.clone();
            // opt.wal_enabled = false;
            // opt.vlog_enabled = true; // open vlog engine
            opt.vlog_enabled = vlog_enabled;
            if vlog_enabled {
                opt.vlog_config.min_vlog_size = 0;
                opt.vlog_config.max_vlog_writers = max_vlog_writers;
                if let Some((plan, level_1_min_files)) = vlog_compaction {
                    opt.vlog_compact_plan = plan.to_string();
                    opt.vlog_config.vlog_compact_level_1_min_files = level_1_min_files;
                }
            }
            if let Some(executor) = executor {
                opt.executor = executor;
            }

            // // Initialise series id sets. Need to do this as it's normally done at the store level.
            let series_ids = Arc::new(series_set::SeriesIDSet::new(&[]));
            opt.series_id_sets = Some(Box::new(TestSeriesIDSets(vec![Arc::clone(&series_ids)])));

            let index_path = db_path.join("index");
            let idx = Arc::new(index::must_open_active_index(
                1,
                db.to_string(),
                &index_path,
                series_ids,
                Arc::clone(&sfile),
                &opt,
            ));

            let tsm1_engine = engine::Engine::new(
                1,
                Arc::clone(&idx),
                Arc::new(root.join("data")),
                Arc::new(root.join("wal")),
                Arc::new(root.join("vlog_index_wal")),
                Arc::new(root.join("vlog")),
                Arc::clone(&sfile),
                opt,
            )
            .unwrap();

            Self {
                engine: tsm1_engine,
                root: root,
                index_path: index_path,
                index_type: index,
                vlog_enabled,
                index: idx,
                sfile,
            }
        }

        fn series_exist(&self, m: String, dims: &[String]) -> i64 {
            let value = ColumnRef::new("value", ColumnKind::FieldOrTag);
            let scan = StorageScan {
                measurement: MeasurementRef::new(m),
                projection: StorageProjection {
                    primary_column: Some(value.clone()),
                    returned_columns: vec![value],
                    row_domain_columns: Vec::new(),
                    series: SeriesProjection::new(dims.to_vec(), dims.to_vec()),
                },
                time_range: TimeRange::all(),
                predicate: None,
                time_order: TimeOrder::Descending,
                output_order: OutputOrder::Unspecified,
                execution: Default::default(),
                cancellation: None,
            };
            let mut itr = if let Some(itr) = self.engine.create_storage_iterator(scan).unwrap() {
                itr
            } else {
                return 0;
            };

            // let input = itr.unwrap().into_any();
            // let mut fitr = {
            //     if let Ok(fitr) = input.downcast::<FloatMergeIter>() {
            //         fitr as Box<dyn FloatIterator + Sync + Send>
            //     } else if let Ok(fitr) = input.downcast::<FloatInterruptIter>() {
            //         fitr as Box<dyn FloatIterator + Sync + Send>
            //     } else {
            //         panic!("invalid iterator");
            //     }
            // };

            // /////////////////////////////////////////////////////////////////////////////
            // /////////////////////////////////////////////////////////////////////////////
            // if (&*itr).type_id() == std::any::TypeId::of::<FloatIter>()

            let input = itr.as_any_mut();
            let mut fitr = {
                if let Some(fitr) = input.downcast_mut::<FloatMergeIter>() {
                    fitr as &mut dyn FloatIterator
                } else if let Some(fitr) = input.downcast_mut::<FloatInterruptIter>() {
                    fitr as &mut dyn FloatIterator
                } else {
                    panic!("invalid iterator");
                }
            };

            let mut n = 0;
            while let Some(p) = fitr.next() {
                n += 1;
            }

            // /////////////////////////////////////////////////////////////////////////////
            // /////////////////////////////////////////////////////////////////////////////
            itr.close();

            n
        }

        /// Close the engine and removes all underlying data.
        pub fn close(&mut self) {
            self.close2(true)
        }

        pub fn close_without_remove_file(&mut self) {
            self.close2(false)
        }

        fn close2(&mut self, cleanup: bool) {
            self.index.close().unwrap();
            self.sfile.close().unwrap();

            self.engine.close().unwrap(); // close file_store and wal

            if cleanup {
                fs::remove_dir_all(&self.root);
            }
        }

        /// Reopen closes and reopens the engine.
        pub(super) fn reopen(&mut self) {
            let enable_compactions_on_open = self.engine.enable_compactions_on_open;
            // Close engine without removing underlying engine data.
            self.close2(false);

            // Re-open series file. Must create a new series file using the same data.
            let mut sfile = SeriesFile::new(self.sfile.path());
            sfile.open().unwrap();
            let sfile = Arc::new(sfile);
            self.sfile = Arc::clone(&sfile);

            let db = self.root.file_name().unwrap().to_str().unwrap();
            let mut opt = EngineOptions::default();
            opt.compaction_limiter = None; // close level compaction.
            opt.compaction_throughput_limiter = None; // close write snapshot rate limit
            opt.index_version = self.index_type.clone();
            opt.vlog_enabled = self.vlog_enabled;
            opt.agg_index_enabled = self.engine.aggregate_index.is_some();
            if self.vlog_enabled {
                opt.vlog_config.min_vlog_size = 0;
                opt.vlog_config.max_vlog_writers = 1;
            }

            // Re-initialise the series id set
            let series_id_set = Arc::new(series_set::SeriesIDSet::new(&[]));
            opt.series_id_sets = Some(Box::new(TestSeriesIDSets(vec![Arc::clone(&series_id_set)])));

            // Re-open index.
            self.index = Arc::new(index::must_open_active_index(
                1,
                db.to_string(),
                &self.index_path,
                series_id_set,
                Arc::clone(&sfile),
                &opt,
            ));

            // Re-initialize engine.
            self.engine = super::Engine::new(
                1,
                Arc::clone(&self.index),
                Arc::new(self.root.join("data")),
                Arc::new(self.root.join("wal")),
                Arc::new(self.root.join("vlog_index_wal")),
                Arc::new(self.root.join("vlog")),
                Arc::clone(&self.sfile),
                opt,
            )
            .unwrap();

            // Reopen engine
            self.engine.enable_compactions_on_open = enable_compactions_on_open;
            self.engine.open().unwrap();

            // Reload series data into index (no-op on TSI).
            self.engine
                .load_metadata_index(1, Arc::clone(&self.index))
                .unwrap();
        }

        /// Rrovides access to the underlying series id bitset in the engine's index.
        /// It will panic if the underlying index does not have a SeriesIDSet method.
        fn series_id_set(&self) -> series_set::SeriesIDSet {
            self.index.series_id_set()
        }

        /// Adds the provided series data to the index and writes a point to
        /// the engine with default values for a field and a time of now.
        fn add_series(&mut self, name: String, tags: HashMap<Vec<u8>, Vec<u8>>) {
            let point = crate::models::points::new_point(
                name,
                crate::models::Tags::from(tags),
                crate::models::Fields(HashMap::from([(
                    "v".to_string(),
                    crate::models::RawVal::F64(1.0),
                )])),
                Time::now(),
            )
            .unwrap();

            self.write_points(&mut [point]);
        }

        /// Parse the string points, adds the associated series to the index and write points to the underlying engine.
        pub fn write_points_string(&self, ptstr: &[&str]) -> super::Result<()> {
            let (mut points, _) = crate::models::points::parse_points_string(&ptstr.join(
                "
",
            ))
            .unwrap();
            self.write_points(&mut points)
        }

        /// Adds the series for the provided points to the index, and writes the point data to the engine.
        pub fn write_points(&self, points: &mut [crate::models::Point]) -> super::Result<()> {
            for p in points.iter_mut() {
                // Write into the index.
                let tags = p.tags();
                self.engine
                    .create_series_if_not_exists(p.key(), &p.name(), tags)?;
            }

            // Write the points into the cache/wal.
            self.engine.write_points(points)
        }

        /// Calls AddSeries, panicking if there is an error.
        fn must_add_series(&mut self, name: String, tags: HashMap<Vec<u8>, Vec<u8>>) {
            self.add_series(name, tags)
        }

        /// Forces a snapshot of the engine. Panic on error.
        pub(super) fn must_write_snapshot(&self) {
            super::Engine::write_snapshot(true, Arc::clone(&self.engine.inner)).unwrap()
        }
    }

    /// Returns a new, open instance of Engine.
    pub fn must_open_engine(index: String) -> Engine {
        must_open_engine_with_path(index, "")
    }

    pub fn must_open_engine_with_path(index: String, path: impl AsRef<Path>) -> Engine {
        let mut e = Engine::with_path(index, path);
        e.engine.open().unwrap();
        e
    }

    pub fn must_open_vlog_engine(index: String) -> Engine {
        let mut e = Engine::with_vlog(index);
        e.engine.open().unwrap();
        e
    }

    fn collect_float_points(
        engine: &super::Engine,
        measurement: &str,
        start_time: i64,
        end_time: i64,
    ) -> Vec<query_point::FloatPoint> {
        let value = ColumnRef::new("value", ColumnKind::Field(Some(crate::models::FLOAT)));
        let scan = StorageScan {
            measurement: MeasurementRef::new(measurement),
            projection: StorageProjection {
                primary_column: Some(value.clone()),
                returned_columns: vec![value],
                row_domain_columns: Vec::new(),
                series: SeriesProjection::new(vec!["host".to_string()], vec!["host".to_string()]),
            },
            time_range: TimeRange::new(start_time, end_time),
            predicate: None,
            time_order: TimeOrder::Ascending,
            output_order: OutputOrder::Unspecified,
            execution: Default::default(),
            cancellation: None,
        };

        let itr = engine.create_storage_iterator(scan);
        let mut itr = match itr {
            Ok(Some(itr)) => itr,
            Ok(None) => return vec![],
            Err(e) => panic!("create_iterator failed: {:?}", e),
        };

        let itr_any = itr.as_any_mut();
        let mut fitr = if let Some(fitr) = itr_any.downcast_mut::<FloatMergeIter>() {
            fitr as &mut dyn FloatIterator
        } else if let Some(fitr) = itr_any.downcast_mut::<FloatInterruptIter>() {
            fitr as &mut dyn FloatIterator
        } else if let Some(fitr) = itr_any.downcast_mut::<FloatLimitIter>() {
            fitr as &mut dyn FloatIterator
        } else if let Some(fitr) = itr_any.downcast_mut::<FloatSortedMergeIter>() {
            fitr as &mut dyn FloatIterator
        } else if let Some(fitr) = itr_any.downcast_mut::<FloatIter>() {
            fitr as &mut dyn FloatIterator
        } else {
            panic!("invalid iterator");
        };

        let mut points = vec![];
        while let Some(point) = fitr.next() {
            points.push(point);
        }
        fitr.close();
        itr.close();
        points
    }

    fn collect_float_iterator(
        mut iterator: Box<dyn QueryIterator + Sync + Send>,
    ) -> Vec<query_point::FloatPoint> {
        let iterator_any = iterator.as_any_mut();
        let float_iterator = if let Some(iterator) = iterator_any.downcast_mut::<FloatMergeIter>() {
            iterator as &mut dyn FloatIterator
        } else if let Some(iterator) = iterator_any.downcast_mut::<FloatInterruptIter>() {
            iterator as &mut dyn FloatIterator
        } else if let Some(iterator) = iterator_any.downcast_mut::<FloatLimitIter>() {
            iterator as &mut dyn FloatIterator
        } else if let Some(iterator) = iterator_any.downcast_mut::<FloatSortedMergeIter>() {
            iterator as &mut dyn FloatIterator
        } else if let Some(iterator) = iterator_any.downcast_mut::<FloatIter>() {
            iterator as &mut dyn FloatIterator
        } else {
            panic!("invalid float iterator")
        };
        let points = std::iter::from_fn(|| float_iterator.next()).collect();
        iterator.close();
        points
    }

    // must_parse_points_string parses points from a string. Panic on error.
    pub fn must_parse_points_string(buf: &str) -> Vec<crate::models::Point> {
        let (a, _) = crate::models::points::parse_points_string(buf).unwrap();
        a
    }

    // parses the first point from a string. Panic on error.
    pub fn must_parse_point_string(buf: &str) -> crate::models::Point {
        let mut a = must_parse_points_string(buf);
        a.swap_remove(0)
    }

    /// Returns an instance of Tags for a comma-delimited list of key/values.
    fn parse_tags(s: &str) -> query_point::Tags {
        let mut m = HashMap::new();
        let split: Vec<&str> = s.split(",").collect();
        for kv in split {
            let a: Vec<&str> = kv.split("=").collect();
            m.insert(a[0].to_string(), a[1].to_string());
        }
        query_point::Tags::new(m)
    }

    struct TestSeriesIter {
        keys: Vec<Vec<u8>>,
    }

    impl TestSeriesIter {
        fn new(keys: Vec<Vec<u8>>) -> Self {
            Self { keys }
        }
    }

    impl crate::index::SeriesIterator for TestSeriesIter {
        fn close(&self) -> std::result::Result<(), Box<dyn std::error::Error>> {
            Ok(())
        }
    }

    impl Iterator for TestSeriesIter {
        type Item = Box<dyn SeriesElem>;

        fn next(&mut self) -> Option<Self::Item> {
            if self.keys.is_empty() {
                return None;
            }

            let (name, tags) = crate::models::points::parse_key_bytes(&self.keys[0]);
            let s = TestSeries {
                name,
                tags,
                deleted: false,
            };
            self.keys.drain(..1);

            Some(Box::new(s))
        }
    }

    struct TestSeries {
        name: Vec<u8>,
        tags: crate::models::Tags,
        deleted: bool,
    }

    impl crate::index::SeriesElem for TestSeries {
        fn name(&self) -> &[u8] {
            &self.name
        }

        fn tags(&self) -> &crate::models::points::Tags {
            &self.tags
        }

        fn deleted(&self) -> bool {
            self.deleted
        }
    }

    #[derive(Clone)]
    pub struct TestSeriesIDSets(pub Vec<Arc<series_set::SeriesIDSet>>);

    impl Deref for TestSeriesIDSets {
        type Target = Vec<Arc<series_set::SeriesIDSet>>;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl DerefMut for TestSeriesIDSets {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }

    // fn for_each_series_id_set(&self, ids: &SeriesIDSet, id_set: &Arc<SeriesIDSet>);
    impl super::SeriesIDSets for TestSeriesIDSets {
        // fn for_each(&self, f: &dyn engine::CallbackSeriesIDSet) -> super::Result<()> {
        fn for_each(
            &self,
            ids: &SeriesIDSet,
            f: &dyn engine::CallbackSeriesIDSet,
        ) -> Result<(), Box<dyn std::error::Error>> {
            for id_set in self.iter() {
                f.for_each_series_id_set(ids, id_set);
            }
            Ok(())
        }

        fn clone_self(&self) -> Box<dyn engine::SeriesIDSets + Sync + Send> {
            Box::new(self.clone())
        }
    }
}

#[cfg(test)]
#[path = "aggregate_index_tests.rs"]
mod aggregate_index_tests;

#[cfg(test)]
#[path = "aggregate_native_tests.rs"]
mod aggregate_native_tests;

#[cfg(test)]
#[path = "aggregate_memory_tests.rs"]
mod aggregate_memory_tests;

#[cfg(test)]
#[path = "aggregate_target_only_tests.rs"]
mod aggregate_target_only_tests;

#[cfg(test)]
#[path = "aggregate_index_benchmarks.rs"]
mod aggregate_index_benchmarks;

#[cfg(all(test, feature = "slow-trace", feature = "benchmarking"))]
#[path = "aggregate_performance_tests.rs"]
mod aggregate_performance_tests;

#[path = "aggregate_compaction.rs"]
mod aggregate_compaction;

#[path = "aggregate_metadata.rs"]
mod aggregate_metadata;
#[path = "aggregate_delete.rs"]
mod aggregate_delete;
#[path = "aggregate_projection.rs"]
pub(crate) mod aggregate_projection;
#[path = "aggregate_cross_shard.rs"]
pub(crate) mod aggregate_cross_shard;
