//! Query-language-independent types for one local storage scan.
//!
//! The types in this module form the storage-native scan contract exposed by
//! [`crate::ReefsDb`].

use super::numeric_aggregate;
use crate::models::{FieldType, MAX_NANO_TIME_I64, MIN_NANO_TIME_I64, points::Val};
#[cfg(feature = "slow-trace")]
use crate::runtime::execution::{
    GeneralBlockingExecutorSnapshot, GeneralBlockingTaskClass, GeneralBlockingTaskDiagnostics,
};
#[cfg(feature = "slow-trace")]
use parking_lot::Mutex;
#[cfg(any(test, feature = "slow-trace"))]
use std::sync::Arc;
#[cfg(feature = "slow-trace")]
use std::sync::atomic::AtomicU64;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
#[cfg(any(test, feature = "slow-trace"))]
use std::sync::atomic::Ordering;
use std::{collections::BTreeMap, fmt, time::Instant};
use tokio_util::sync::CancellationToken;

/// A concrete measurement selected by a local storage scan.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct MeasurementRef {
    pub name: String,
}

impl MeasurementRef {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

/// The storage meaning of a referenced column.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ColumnKind {
    /// A field, optionally constrained to one model field type.
    Field(Option<FieldType>),
    /// A field of any model field type.
    AnyField,
    Tag,
    Time,
    Measurement,
    /// Preserve the legacy Engine row-map alias for an unresolved ordinary
    /// `name` reference without changing TSI's FieldOrTag interpretation.
    LegacyName,
    /// Resolve against measurement field metadata first, then use the series tag.
    /// If neither exists, the column has a missing value for that series or row.
    FieldOrTag,
}

/// A field, tag, time, or measurement value referenced by a scan.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ColumnRef {
    pub name: String,
    pub kind: ColumnKind,
}

impl ColumnRef {
    pub fn new(name: impl Into<String>, kind: ColumnKind) -> Self {
        Self {
            name: name.into(),
            kind,
        }
    }
}

/// Tag requirements for local series identity and returned points.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SeriesProjection {
    /// Tags used to construct local tag sets and preserve distinct series while merging.
    pub identity_tag_keys: Vec<String>,
    /// Tags retained in returned `Point::tags` values.
    pub output_tag_keys: Vec<String>,
}

impl SeriesProjection {
    pub fn new(identity_tag_keys: Vec<String>, output_tag_keys: Vec<String>) -> Self {
        Self {
            identity_tag_keys,
            output_tag_keys,
        }
    }
}

/// Values, timestamp-domain columns, and series identity materialized by storage.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageProjection {
    /// The returned column that drives the typed primary value cursor.
    ///
    /// This remains separate from `returned_columns` so an aux-only scan can
    /// return columns without incorrectly promoting the first one to primary.
    pub primary_column: Option<ColumnRef>,
    /// Columns whose values are returned to the caller, in result order.
    pub returned_columns: Vec<ColumnRef>,
    /// Columns that establish the timestamp domain when there is no primary column.
    ///
    /// A primary column, when present, exclusively owns row timestamp generation.
    /// Otherwise returned auxiliary field cursors and these row-domain columns
    /// establish the union timestamp domain. Predicate-only fields never create rows.
    pub row_domain_columns: Vec<ColumnRef>,
    /// Local identity and returned tag requirements.
    pub series: SeriesProjection,
}

/// A malformed storage-native projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageProjectionError {
    PrimaryMissingFromReturnedColumns,
    PrimaryNotFirst {
        primary: ColumnRef,
        first: ColumnRef,
    },
}

impl fmt::Display for StorageProjectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PrimaryMissingFromReturnedColumns => {
                write!(
                    f,
                    "primary column requires a non-empty returned column list"
                )
            }
            Self::PrimaryNotFirst { primary, first } => write!(
                f,
                "primary column {:?} must be the first returned column, found {:?}",
                primary, first
            ),
        }
    }
}

impl std::error::Error for StorageProjectionError {}

impl StorageProjection {
    pub fn validate(&self) -> Result<(), StorageProjectionError> {
        let Some(primary) = self.primary_column.as_ref() else {
            return Ok(());
        };
        let Some(first) = self.returned_columns.first() else {
            return Err(StorageProjectionError::PrimaryMissingFromReturnedColumns);
        };
        if first != primary {
            return Err(StorageProjectionError::PrimaryNotFirst {
                primary: primary.clone(),
                first: first.clone(),
            });
        }
        Ok(())
    }
}

/// An inclusive nanosecond time range for one local scan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TimeRange {
    /// Inclusive lower bound.
    pub start_time: i64,
    /// Inclusive upper bound.
    pub end_time: i64,
}

impl TimeRange {
    pub const fn new(start_time: i64, end_time: i64) -> Self {
        Self {
            start_time,
            end_time,
        }
    }

    /// The full writable storage time range using the model-owned sentinels.
    pub const fn all() -> Self {
        Self::new(MIN_NANO_TIME_I64, MAX_NANO_TIME_I64)
    }

    pub const fn contains(self, timestamp: i64) -> bool {
        self.start_time <= timestamp && timestamp <= self.end_time
    }
}

/// The direction in which local cursors traverse time.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TimeOrder {
    #[default]
    Ascending,
    Descending,
}

/// The ordering guarantee required from merged storage output.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OutputOrder {
    #[default]
    Unspecified,
    TimeOrdered,
}

/// One fixed storage merge window expressed in nanoseconds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StorageWindow {
    pub duration: i64,
    pub offset: i64,
}

/// A storage-local limit and offset pair.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StorageLimit {
    pub limit: i64,
    pub offset: i64,
}

/// Storage execution behavior that is independent of query-language syntax.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageExecutionOptions {
    pub window: Option<StorageWindow>,
    pub point_limit: StorageLimit,
    pub series_limit: StorageLimit,
    pub strip_name: bool,
    pub max_series_n: i64,
    /// Whether query grouping and storage identity dimensions have the same shape.
    /// This preserves the existing placement of per-series point limits without
    /// exposing query-language grouping syntax to storage.
    pub grouping_matches_dimensions: bool,
    /// Controls storage-owned resolution of residual compatibility columns.
    pub residual_column_resolution: ResidualColumnResolution,
    /// Query-local physical-work counters for explicitly instrumented builds.
    #[cfg(feature = "slow-trace")]
    #[doc(hidden)]
    pub profile: Option<Arc<StorageScanProfile>>,
}

#[cfg(feature = "slow-trace")]
#[doc(hidden)]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GroupedPhysicalBlockIdentity {
    pub file_path: String,
    pub offset: i64,
    pub min_time: i64,
    pub max_time: i64,
}

#[cfg(feature = "slow-trace")]
impl GroupedPhysicalBlockIdentity {
    pub(crate) fn new(
        file_path: impl Into<String>,
        offset: i64,
        min_time: i64,
        max_time: i64,
    ) -> Self {
        Self {
            file_path: file_path.into(),
            offset,
            min_time,
            max_time,
        }
    }
}

#[cfg(feature = "slow-trace")]
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupedRawIntervalKind {
    CandidatePartial,
    CandidateCrossBucket,
    CandidateOther,
    VisibilityOverlap,
    WholeRequestFallback,
    RowDomainPrecheck,
    RowDomainFallback,
}

#[cfg(feature = "slow-trace")]
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupedRawIntervalSample {
    pub scope: String,
    pub start_time: i64,
    pub end_time: i64,
    pub kind: GroupedRawIntervalKind,
}

#[cfg(feature = "slow-trace")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupedPhysicalScanKind {
    Target,
    VisibilityOverlap,
    RowDomainPrecheck,
    RowDomainFallback,
}

#[cfg(feature = "slow-trace")]
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct StorageScanProfile {
    shard_scans: AtomicU64,
    total_measurement_series: AtomicU64,
    selected_series: AtomicU64,
    selected_tag_sets: AtomicU64,
    physical_series_keys_total: AtomicU64,
    physical_series_keys_per_tagset_min: AtomicU64,
    physical_series_keys_per_tagset_max: AtomicU64,
    projected_field_count: AtomicU64,
    row_domain_field_count: AtomicU64,
    direct_scan_series_count: AtomicU64,
    direct_physical_series_count: AtomicU64,
    generic_fallback_series_count: AtomicU64,
    generic_fallback_physical_series_count: AtomicU64,
    field_value_streams: AtomicU64,
    timestamp_only_streams: AtomicU64,
    tsi_exact_lookups: AtomicU64,
    series_iterators: AtomicU64,
    field_cursors: AtomicU64,
    file_index_searches: AtomicU64,
    cursor_seeks: AtomicU64,
    tsm_file_visits: AtomicU64,
    vlog_file_visits: AtomicU64,
    blocks_considered: AtomicU64,
    vlog_blocks_considered: AtomicU64,
    blocks_decoded: AtomicU64,
    full_value_blocks_decoded: AtomicU64,
    timestamp_only_blocks_decoded: AtomicU64,
    vlog_blocks_decoded: AtomicU64,
    values_decoded: AtomicU64,
    full_value_points_decoded: AtomicU64,
    timestamp_only_points_decoded: AtomicU64,
    vlog_values_decoded: AtomicU64,
    block_bytes: AtomicU64,
    vlog_block_bytes: AtomicU64,
    block_lookup_us: AtomicU64,
    block_read_decode_us: AtomicU64,
    vlog_read_decode_us: AtomicU64,
    vlog_sort_us: AtomicU64,
    iterator_tasks_spawned: AtomicU64,
    vlog_tasks_spawned: AtomicU64,
    measurement_lookup_us: AtomicU64,
    scan_metadata_us: AtomicU64,
    shard_mapping_us: AtomicU64,
    total_series_lookup_us: AtomicU64,
    tsi_lookup_us: AtomicU64,
    iterator_setup_us: AtomicU64,
    iterator_next_us: AtomicU64,
    arrow_append_us: AtomicU64,
    arrow_finish_us: AtomicU64,
    arrow_batches: AtomicU64,
    arrow_rows: AtomicU64,
    arrow_bytes: AtomicU64,
    row_domain_merge_us: AtomicU64,
    direct_merge_input_series: AtomicU64,
    direct_merge_rows: AtomicU64,
    direct_merge_us: AtomicU64,
    decoded_block_identities: Mutex<Vec<GroupedPhysicalBlockIdentity>>,
}

/// Raw storage work performed by one instrumented scan.
#[cfg(feature = "slow-trace")]
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StorageScanProfileMetrics {
    pub shard_scans: u64,
    pub total_measurement_series: u64,
    pub selected_series: u64,
    pub selected_tag_sets: u64,
    pub physical_series_keys_total: u64,
    pub physical_series_keys_per_tagset_min: u64,
    pub physical_series_keys_per_tagset_max: u64,
    pub projected_field_count: u64,
    pub row_domain_field_count: u64,
    pub direct_scan_series_count: u64,
    pub direct_physical_series_count: u64,
    pub generic_fallback_series_count: u64,
    pub generic_fallback_physical_series_count: u64,
    pub field_value_streams: u64,
    pub timestamp_only_streams: u64,
    pub tsi_exact_lookups: u64,
    pub series_iterators: u64,
    pub field_cursors: u64,
    pub file_index_searches: u64,
    pub cursor_seeks: u64,
    pub tsm_file_visits: u64,
    pub vlog_file_visits: u64,
    pub blocks_considered: u64,
    pub vlog_blocks_considered: u64,
    pub blocks_decoded: u64,
    pub full_value_blocks_decoded: u64,
    pub timestamp_only_blocks_decoded: u64,
    pub vlog_blocks_decoded: u64,
    pub values_decoded: u64,
    pub full_value_points_decoded: u64,
    pub timestamp_only_points_decoded: u64,
    pub vlog_values_decoded: u64,
    pub block_bytes: u64,
    pub vlog_block_bytes: u64,
    pub block_lookup_us: u64,
    pub block_read_decode_us: u64,
    pub vlog_read_decode_us: u64,
    pub vlog_sort_us: u64,
    pub iterator_tasks_spawned: u64,
    pub vlog_tasks_spawned: u64,
    pub measurement_lookup_us: u64,
    pub scan_metadata_us: u64,
    pub shard_mapping_us: u64,
    pub total_series_lookup_us: u64,
    pub tsi_lookup_us: u64,
    pub iterator_setup_us: u64,
    pub iterator_next_us: u64,
    pub arrow_append_us: u64,
    pub arrow_finish_us: u64,
    pub arrow_batches: u64,
    pub arrow_rows: u64,
    pub arrow_bytes: u64,
    pub row_domain_merge_us: u64,
    pub direct_merge_input_series: u64,
    pub direct_merge_rows: u64,
    pub direct_merge_us: u64,
}

#[cfg(feature = "slow-trace")]
impl PartialEq for StorageScanProfile {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}

#[cfg(feature = "slow-trace")]
impl Eq for StorageScanProfile {}

#[cfg(feature = "slow-trace")]
impl StorageScanProfile {
    pub(crate) fn record_shard_scan(&self) {
        self.shard_scans.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_total_measurement_series(&self, count: u64, elapsed_us: u64) {
        self.total_measurement_series
            .fetch_add(count, Ordering::Relaxed);
        self.total_series_lookup_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
    }

    pub(crate) fn record_selected_tag_set_layout(
        &self,
        tag_sets: u64,
        physical_series: u64,
        per_tagset_min: u64,
        per_tagset_max: u64,
    ) {
        self.selected_series
            .fetch_add(physical_series, Ordering::Relaxed);
        self.selected_tag_sets
            .fetch_add(tag_sets, Ordering::Relaxed);
        self.physical_series_keys_total
            .fetch_add(physical_series, Ordering::Relaxed);
        if per_tagset_min > 0 {
            let _ = self.physical_series_keys_per_tagset_min.fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |current| {
                    if current == 0 || per_tagset_min < current {
                        Some(per_tagset_min)
                    } else {
                        None
                    }
                },
            );
        }
        self.physical_series_keys_per_tagset_max
            .fetch_max(per_tagset_max, Ordering::Relaxed);
    }

    pub(crate) fn record_direct_scan_layout(
        &self,
        series: u64,
        projected_fields: u64,
        row_domain_fields: u64,
    ) {
        self.projected_field_count
            .fetch_add(projected_fields, Ordering::Relaxed);
        self.row_domain_field_count
            .fetch_add(row_domain_fields, Ordering::Relaxed);
        self.direct_scan_series_count
            .fetch_add(series, Ordering::Relaxed);
        self.direct_physical_series_count
            .fetch_add(series, Ordering::Relaxed);
        self.field_value_streams
            .fetch_add(series.saturating_mul(projected_fields), Ordering::Relaxed);
        self.timestamp_only_streams
            .fetch_add(series.saturating_mul(row_domain_fields), Ordering::Relaxed);
    }

    pub(crate) fn record_generic_fallback_series(&self, count: u64) {
        self.generic_fallback_series_count
            .fetch_add(count, Ordering::Relaxed);
        self.generic_fallback_physical_series_count
            .fetch_add(count, Ordering::Relaxed);
    }

    pub(crate) fn record_tsi_exact_lookup(&self) {
        self.tsi_exact_lookups.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_series_iterators(&self, count: u64) {
        self.series_iterators.fetch_add(count, Ordering::Relaxed);
    }

    pub(crate) fn record_cursor(&self, file_visits: u64, blocks_considered: u64) {
        self.field_cursors.fetch_add(1, Ordering::Relaxed);
        self.tsm_file_visits
            .fetch_add(file_visits, Ordering::Relaxed);
        self.blocks_considered
            .fetch_add(blocks_considered, Ordering::Relaxed);
    }

    pub(crate) fn record_file_index_searches(&self, count: u64) {
        self.file_index_searches.fetch_add(count, Ordering::Relaxed);
    }

    pub(crate) fn record_cursor_seek(&self) {
        self.cursor_seeks.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_block_decode(
        &self,
        identity: GroupedPhysicalBlockIdentity,
        values_decoded: u64,
        block_bytes: u64,
        elapsed_us: u64,
        timestamp_only: bool,
    ) {
        self.blocks_decoded.fetch_add(1, Ordering::Relaxed);
        self.values_decoded
            .fetch_add(values_decoded, Ordering::Relaxed);
        if timestamp_only {
            self.timestamp_only_blocks_decoded
                .fetch_add(1, Ordering::Relaxed);
            self.timestamp_only_points_decoded
                .fetch_add(values_decoded, Ordering::Relaxed);
        } else {
            self.full_value_blocks_decoded
                .fetch_add(1, Ordering::Relaxed);
            self.full_value_points_decoded
                .fetch_add(values_decoded, Ordering::Relaxed);
        }
        self.block_bytes.fetch_add(block_bytes, Ordering::Relaxed);
        self.block_read_decode_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
        self.decoded_block_identities.lock().push(identity);
    }

    pub(crate) fn record_block_lookup(&self, elapsed_us: u64) {
        self.block_lookup_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
    }

    pub(crate) fn record_vlog_cursor(&self, file_visits: u64, blocks_considered: u64) {
        self.field_cursors.fetch_add(1, Ordering::Relaxed);
        self.vlog_file_visits
            .fetch_add(file_visits, Ordering::Relaxed);
        self.vlog_blocks_considered
            .fetch_add(blocks_considered, Ordering::Relaxed);
    }

    pub(crate) fn record_vlog_read(
        &self,
        blocks_decoded: u64,
        values_decoded: u64,
        block_bytes: u64,
        tasks_spawned: u64,
        elapsed_us: u64,
    ) {
        self.vlog_blocks_decoded
            .fetch_add(blocks_decoded, Ordering::Relaxed);
        self.vlog_values_decoded
            .fetch_add(values_decoded, Ordering::Relaxed);
        self.vlog_block_bytes
            .fetch_add(block_bytes, Ordering::Relaxed);
        self.vlog_tasks_spawned
            .fetch_add(tasks_spawned, Ordering::Relaxed);
        self.vlog_read_decode_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
    }

    pub(crate) fn record_vlog_sort(&self, elapsed_us: u64) {
        self.vlog_sort_us.fetch_add(elapsed_us, Ordering::Relaxed);
    }

    pub(crate) fn record_iterator_tasks(&self, count: u64) {
        self.iterator_tasks_spawned
            .fetch_add(count, Ordering::Relaxed);
    }

    pub(crate) fn record_measurement_lookup(&self, elapsed_us: u64) {
        self.measurement_lookup_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
    }

    #[doc(hidden)]
    pub fn record_scan_metadata(&self, elapsed_us: u64) {
        self.scan_metadata_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
    }

    #[doc(hidden)]
    pub fn record_shard_mapping(&self, elapsed_us: u64) {
        self.shard_mapping_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
    }

    pub(crate) fn record_tsi_lookup(&self, elapsed_us: u64) {
        self.tsi_lookup_us.fetch_add(elapsed_us, Ordering::Relaxed);
    }

    pub(crate) fn record_iterator_setup(&self, elapsed_us: u64) {
        self.iterator_setup_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
    }

    pub(crate) fn record_iterator_next(&self, elapsed_us: u64) {
        self.iterator_next_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
    }

    pub(crate) fn record_arrow_batch(&self, rows: u64, bytes: u64, append_us: u64, finish_us: u64) {
        self.arrow_batches.fetch_add(1, Ordering::Relaxed);
        self.arrow_rows.fetch_add(rows, Ordering::Relaxed);
        self.arrow_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.arrow_append_us.fetch_add(append_us, Ordering::Relaxed);
        self.arrow_finish_us.fetch_add(finish_us, Ordering::Relaxed);
    }

    pub(crate) fn record_row_domain_merge(&self, elapsed_us: u64) {
        self.row_domain_merge_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
    }

    pub(crate) fn record_direct_merge_inputs(&self, series: u64) {
        self.direct_merge_input_series
            .fetch_add(series, Ordering::Relaxed);
    }

    pub(crate) fn record_direct_series_merge(&self, rows: u64, elapsed_us: u64) {
        self.direct_merge_rows.fetch_add(rows, Ordering::Relaxed);
        self.direct_merge_us
            .fetch_add(elapsed_us, Ordering::Relaxed);
    }

    pub fn raw_snapshot(&self) -> StorageScanProfileMetrics {
        StorageScanProfileMetrics {
            shard_scans: self.shard_scans.load(Ordering::Relaxed),
            total_measurement_series: self.total_measurement_series.load(Ordering::Relaxed),
            selected_series: self.selected_series.load(Ordering::Relaxed),
            selected_tag_sets: self.selected_tag_sets.load(Ordering::Relaxed),
            physical_series_keys_total: self.physical_series_keys_total.load(Ordering::Relaxed),
            physical_series_keys_per_tagset_min: self
                .physical_series_keys_per_tagset_min
                .load(Ordering::Relaxed),
            physical_series_keys_per_tagset_max: self
                .physical_series_keys_per_tagset_max
                .load(Ordering::Relaxed),
            projected_field_count: self.projected_field_count.load(Ordering::Relaxed),
            row_domain_field_count: self.row_domain_field_count.load(Ordering::Relaxed),
            direct_scan_series_count: self.direct_scan_series_count.load(Ordering::Relaxed),
            direct_physical_series_count: self.direct_physical_series_count.load(Ordering::Relaxed),
            generic_fallback_series_count: self
                .generic_fallback_series_count
                .load(Ordering::Relaxed),
            generic_fallback_physical_series_count: self
                .generic_fallback_physical_series_count
                .load(Ordering::Relaxed),
            field_value_streams: self.field_value_streams.load(Ordering::Relaxed),
            timestamp_only_streams: self.timestamp_only_streams.load(Ordering::Relaxed),
            tsi_exact_lookups: self.tsi_exact_lookups.load(Ordering::Relaxed),
            series_iterators: self.series_iterators.load(Ordering::Relaxed),
            field_cursors: self.field_cursors.load(Ordering::Relaxed),
            file_index_searches: self.file_index_searches.load(Ordering::Relaxed),
            cursor_seeks: self.cursor_seeks.load(Ordering::Relaxed),
            tsm_file_visits: self.tsm_file_visits.load(Ordering::Relaxed),
            vlog_file_visits: self.vlog_file_visits.load(Ordering::Relaxed),
            blocks_considered: self.blocks_considered.load(Ordering::Relaxed),
            vlog_blocks_considered: self.vlog_blocks_considered.load(Ordering::Relaxed),
            blocks_decoded: self.blocks_decoded.load(Ordering::Relaxed),
            full_value_blocks_decoded: self.full_value_blocks_decoded.load(Ordering::Relaxed),
            timestamp_only_blocks_decoded: self
                .timestamp_only_blocks_decoded
                .load(Ordering::Relaxed),
            vlog_blocks_decoded: self.vlog_blocks_decoded.load(Ordering::Relaxed),
            values_decoded: self.values_decoded.load(Ordering::Relaxed),
            full_value_points_decoded: self.full_value_points_decoded.load(Ordering::Relaxed),
            timestamp_only_points_decoded: self
                .timestamp_only_points_decoded
                .load(Ordering::Relaxed),
            vlog_values_decoded: self.vlog_values_decoded.load(Ordering::Relaxed),
            block_bytes: self.block_bytes.load(Ordering::Relaxed),
            vlog_block_bytes: self.vlog_block_bytes.load(Ordering::Relaxed),
            block_lookup_us: self.block_lookup_us.load(Ordering::Relaxed),
            block_read_decode_us: self.block_read_decode_us.load(Ordering::Relaxed),
            vlog_read_decode_us: self.vlog_read_decode_us.load(Ordering::Relaxed),
            vlog_sort_us: self.vlog_sort_us.load(Ordering::Relaxed),
            iterator_tasks_spawned: self.iterator_tasks_spawned.load(Ordering::Relaxed),
            vlog_tasks_spawned: self.vlog_tasks_spawned.load(Ordering::Relaxed),
            measurement_lookup_us: self.measurement_lookup_us.load(Ordering::Relaxed),
            scan_metadata_us: self.scan_metadata_us.load(Ordering::Relaxed),
            shard_mapping_us: self.shard_mapping_us.load(Ordering::Relaxed),
            total_series_lookup_us: self.total_series_lookup_us.load(Ordering::Relaxed),
            tsi_lookup_us: self.tsi_lookup_us.load(Ordering::Relaxed),
            iterator_setup_us: self.iterator_setup_us.load(Ordering::Relaxed),
            iterator_next_us: self.iterator_next_us.load(Ordering::Relaxed),
            arrow_append_us: self.arrow_append_us.load(Ordering::Relaxed),
            arrow_finish_us: self.arrow_finish_us.load(Ordering::Relaxed),
            arrow_batches: self.arrow_batches.load(Ordering::Relaxed),
            arrow_rows: self.arrow_rows.load(Ordering::Relaxed),
            arrow_bytes: self.arrow_bytes.load(Ordering::Relaxed),
            row_domain_merge_us: self.row_domain_merge_us.load(Ordering::Relaxed),
            direct_merge_input_series: self.direct_merge_input_series.load(Ordering::Relaxed),
            direct_merge_rows: self.direct_merge_rows.load(Ordering::Relaxed),
            direct_merge_us: self.direct_merge_us.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn snapshot(&self) -> GroupedAggregateProfileMetrics {
        GroupedAggregateProfileMetrics {
            field_cursors: self.field_cursors.load(Ordering::Relaxed),
            file_index_searches: self.file_index_searches.load(Ordering::Relaxed),
            cursor_seeks: self.cursor_seeks.load(Ordering::Relaxed),
            tsm_file_visits: self.tsm_file_visits.load(Ordering::Relaxed),
            blocks_considered: self.blocks_considered.load(Ordering::Relaxed),
            blocks_decoded: self.blocks_decoded.load(Ordering::Relaxed),
            values_decoded: self.values_decoded.load(Ordering::Relaxed),
            timestamp_only_points_decoded: self
                .timestamp_only_points_decoded
                .load(Ordering::Relaxed),
            block_bytes: self.block_bytes.load(Ordering::Relaxed),
            block_lookup_us: self.block_lookup_us.load(Ordering::Relaxed),
            block_read_decode_us: self.block_read_decode_us.load(Ordering::Relaxed),
            iterator_tasks_spawned: self.iterator_tasks_spawned.load(Ordering::Relaxed),
            measurement_lookup_us: self.measurement_lookup_us.load(Ordering::Relaxed),
            tsi_lookup_us: self.tsi_lookup_us.load(Ordering::Relaxed),
            iterator_setup_us: self.iterator_setup_us.load(Ordering::Relaxed),
            decoded_block_identities: self.decoded_block_identities.lock().clone(),
            ..Default::default()
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ResidualColumnResolution {
    #[default]
    Native,
    /// Ordinary `name` residuals use the historical measurement-name fallback.
    LegacyNameAlias,
}

/// Storage-supported arithmetic inside a row predicate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArithmeticOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    // Transitional parity with the current internal row evaluator. This is not
    // yet a public storage API commitment.
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
}

/// Storage-supported scalar comparisons inside a row predicate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompareOp {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

/// Storage-supported regular-expression comparisons.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegexOp {
    Match,
    NotMatch,
}

/// Storage-supported boolean composition inside a row predicate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalOp {
    And,
    Or,
}

/// A scalar expression evaluated while filtering a materialized row.
#[derive(Clone, Debug, PartialEq)]
pub enum StorageValueExpr {
    Column(ColumnRef),
    /// Reuses the model-owned scalar representation used by storage points.
    Literal(Val),
    /// A conditional expression used as a boolean scalar operand.
    Predicate(Box<StoragePredicate>),
    Arithmetic {
        op: ArithmeticOp,
        lhs: Box<StorageValueExpr>,
        rhs: Box<StorageValueExpr>,
    },
}

/// A structurally valid predicate pushed into local series and row filtering.
#[derive(Clone, Debug, PartialEq)]
pub enum StoragePredicate {
    /// A scalar expression used directly as a boolean predicate.
    Value(StorageValueExpr),
    /// Compatibility result for QL expressions that always evaluate false.
    Constant(bool),
    /// A legacy expression that produces no value. At the predicate root this
    /// filters the row; as a nested scalar it remains distinct from `false`.
    Missing,
    /// Exact membership of one present tag value in a canonical UTF-8 set.
    /// Missing tags never match, while a present empty value matches `""`.
    ExactTagSet {
        tag: ColumnRef,
        accepted_values: Vec<String>,
    },
    Compare {
        op: CompareOp,
        lhs: StorageValueExpr,
        rhs: StorageValueExpr,
    },
    Regex {
        op: RegexOp,
        value: StorageValueExpr,
        /// Syntax-independent raw pattern text.
        pattern: String,
    },
    Logical {
        op: LogicalOp,
        lhs: Box<StoragePredicate>,
        rhs: Box<StoragePredicate>,
    },
}

impl StoragePredicate {
    /// Rewrites ordinary unresolved `name` columns for legacy row evaluation.
    pub fn with_legacy_name_alias(self) -> Self {
        fn rewrite_predicate(source: StoragePredicate) -> StoragePredicate {
            match source {
                StoragePredicate::Value(value) => StoragePredicate::Value(rewrite_value(value)),
                StoragePredicate::Constant(value) => StoragePredicate::Constant(value),
                StoragePredicate::Missing => StoragePredicate::Missing,
                StoragePredicate::ExactTagSet {
                    tag,
                    accepted_values,
                } => StoragePredicate::ExactTagSet {
                    tag,
                    accepted_values,
                },
                StoragePredicate::Compare { op, lhs, rhs } => StoragePredicate::Compare {
                    op,
                    lhs: rewrite_value(lhs),
                    rhs: rewrite_value(rhs),
                },
                StoragePredicate::Regex { op, value, pattern } => StoragePredicate::Regex {
                    op,
                    value: rewrite_value(value),
                    pattern,
                },
                StoragePredicate::Logical { op, lhs, rhs } => StoragePredicate::Logical {
                    op,
                    lhs: Box::new(rewrite_predicate(*lhs)),
                    rhs: Box::new(rewrite_predicate(*rhs)),
                },
            }
        }

        fn rewrite_value(value: StorageValueExpr) -> StorageValueExpr {
            match value {
                StorageValueExpr::Column(mut column)
                    if column.name == "name" && column.kind == ColumnKind::FieldOrTag =>
                {
                    column.kind = ColumnKind::LegacyName;
                    StorageValueExpr::Column(column)
                }
                StorageValueExpr::Column(column) => StorageValueExpr::Column(column),
                StorageValueExpr::Literal(value) => StorageValueExpr::Literal(value),
                StorageValueExpr::Predicate(predicate) => {
                    StorageValueExpr::Predicate(Box::new(rewrite_predicate(*predicate)))
                }
                StorageValueExpr::Arithmetic { op, lhs, rhs } => StorageValueExpr::Arithmetic {
                    op,
                    lhs: Box::new(rewrite_value(*lhs)),
                    rhs: Box::new(rewrite_value(*rhs)),
                },
            }
        }

        rewrite_predicate(self)
    }
}

/// Storage-owned cancellation handle for a running scan.
#[derive(Clone, Debug)]
pub struct ScanCancellation {
    token: CancellationToken,
    #[cfg(feature = "slow-trace")]
    grouped_diagnostics: Option<Arc<GroupedExecutionDiagnostics>>,
    #[cfg(test)]
    checks_before_cancel: Option<Arc<AtomicUsize>>,
}

impl ScanCancellation {
    pub fn new(token: CancellationToken) -> Self {
        Self {
            token,
            #[cfg(feature = "slow-trace")]
            grouped_diagnostics: None,
            #[cfg(test)]
            checks_before_cancel: None,
        }
    }

    /// Attaches request-local grouped scheduling diagnostics to this
    /// cancellation handle. This API only exists in instrumented builds.
    #[cfg(feature = "slow-trace")]
    pub fn with_grouped_diagnostics(
        token: CancellationToken,
        diagnostics: Arc<GroupedExecutionDiagnostics>,
    ) -> Self {
        Self {
            token,
            grouped_diagnostics: Some(diagnostics),
            #[cfg(test)]
            checks_before_cancel: None,
        }
    }

    #[cfg(feature = "slow-trace")]
    pub(crate) fn grouped_diagnostics(&self) -> Option<&Arc<GroupedExecutionDiagnostics>> {
        self.grouped_diagnostics.as_ref()
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        #[cfg(test)]
        if let Some(checks) = &self.checks_before_cancel {
            let previous = checks
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                    Some(value.saturating_sub(1))
                })
                .expect("cancellation check counter update cannot fail");
            if previous <= 1 {
                self.token.cancel();
            }
        }
        self.token.is_cancelled()
    }

    #[cfg(test)]
    pub(crate) fn cancel_after_checks(token: CancellationToken, checks: usize) -> Self {
        assert!(checks > 0);
        Self {
            token,
            #[cfg(feature = "slow-trace")]
            grouped_diagnostics: None,
            checks_before_cancel: Some(Arc::new(AtomicUsize::new(checks))),
        }
    }

    pub fn cancel(&self) {
        self.token.cancel();
    }

    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }
}

/// Monotonic scheduling boundaries for one grouped query.
///
/// The HTTP-facing query timeline owns this object and passes it through the
/// existing cancellation handle. Storage only records timestamps and never
/// emits output from a measured section.
#[cfg(feature = "slow-trace")]
#[derive(Debug)]
pub struct GroupedExecutionDiagnostics {
    origin: Instant,
    general_async_submitted_us: AtomicU64,
    general_async_started_us: AtomicU64,
    storage_coordinator_started_us: AtomicU64,
    storage_result_ready_us: AtomicU64,
    storage_jobs: Mutex<Vec<GroupedStorageJobTiming>>,
    general_blocking_tasks: Mutex<Vec<GroupedGeneralBlockingTask>>,
}

#[cfg(feature = "slow-trace")]
#[derive(Debug)]
struct GroupedGeneralBlockingTask {
    partition: u64,
    diagnostics: Arc<GeneralBlockingTaskDiagnostics>,
}

/// One storage leaf job's monotonic scheduler timestamps, relative to the
/// owning query's origin.
#[cfg(feature = "slow-trace")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GroupedStorageJobTiming {
    pub partition: u64,
    pub permit_requested_us: u64,
    pub permit_acquired_us: u64,
    pub blocking_submitted_us: u64,
    pub blocking_started_us: u64,
    pub blocking_finished_us: u64,
}

/// One request-owned GENERAL blocking task and its executor queue snapshots.
#[cfg(feature = "slow-trace")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupedGeneralBlockingTaskTiming {
    pub task_class: GeneralBlockingTaskClass,
    pub partition: u64,
    pub blocking_submitted_us: u64,
    pub blocking_started_us: u64,
    pub blocking_finished_us: u64,
    pub queue_wait_us: u64,
    pub execution_us: u64,
    pub submit_state: GeneralBlockingExecutorSnapshot,
    pub start_state: GeneralBlockingExecutorSnapshot,
    pub finish_state: GeneralBlockingExecutorSnapshot,
}

/// Snapshot of grouped runtime scheduling boundaries.
#[cfg(feature = "slow-trace")]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GroupedExecutionDiagnosticsSnapshot {
    pub general_async_submitted_us: u64,
    pub general_async_started_us: u64,
    pub storage_coordinator_started_us: u64,
    pub storage_result_ready_us: u64,
    pub storage_jobs: Vec<GroupedStorageJobTiming>,
    pub general_blocking_tasks: Vec<GroupedGeneralBlockingTaskTiming>,
}

#[cfg(feature = "slow-trace")]
impl GroupedExecutionDiagnostics {
    pub fn new(origin: Instant) -> Self {
        Self {
            origin,
            general_async_submitted_us: AtomicU64::new(0),
            general_async_started_us: AtomicU64::new(0),
            storage_coordinator_started_us: AtomicU64::new(0),
            storage_result_ready_us: AtomicU64::new(0),
            storage_jobs: Mutex::new(Vec::new()),
            general_blocking_tasks: Mutex::new(Vec::new()),
        }
    }

    fn offset(&self, instant: Instant) -> u64 {
        instant
            .saturating_duration_since(self.origin)
            .as_micros()
            .min(u128::from(u64::MAX - 1)) as u64
    }

    fn encoded_offset(&self, instant: Instant) -> u64 {
        self.offset(instant) + 1
    }

    fn record_once(&self, target: &AtomicU64, instant: Instant) {
        let _ = target.compare_exchange(
            0,
            self.encoded_offset(instant),
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
    }

    pub(crate) fn record_general_async_submitted(&self, instant: Instant) {
        self.record_once(&self.general_async_submitted_us, instant);
    }

    pub(crate) fn record_general_async_started(&self, instant: Instant) {
        self.record_once(&self.general_async_started_us, instant);
    }

    pub(crate) fn record_storage_coordinator_started(&self, instant: Instant) {
        self.record_once(&self.storage_coordinator_started_us, instant);
    }

    pub(crate) fn record_storage_result_ready(&self, instant: Instant) {
        self.record_once(&self.storage_result_ready_us, instant);
    }

    pub(crate) fn record_storage_job_submitted(
        &self,
        partition: usize,
        permit_requested: Instant,
        permit_acquired: Instant,
        submitted: Instant,
    ) -> usize {
        let mut jobs = self.storage_jobs.lock();
        let index = jobs.len();
        jobs.push(GroupedStorageJobTiming {
            partition: partition as u64,
            permit_requested_us: self.offset(permit_requested),
            permit_acquired_us: self.offset(permit_acquired),
            blocking_submitted_us: self.offset(submitted),
            ..Default::default()
        });
        index
    }

    pub(crate) fn record_storage_job_started(&self, index: usize, instant: Instant) {
        if let Some(job) = self.storage_jobs.lock().get_mut(index) {
            job.blocking_started_us = self.offset(instant);
        }
    }

    pub(crate) fn record_storage_job_finished(&self, index: usize, instant: Instant) {
        if let Some(job) = self.storage_jobs.lock().get_mut(index) {
            job.blocking_finished_us = self.offset(instant);
        }
    }

    pub(crate) fn record_general_blocking_task(
        &self,
        partition: usize,
        diagnostics: Arc<GeneralBlockingTaskDiagnostics>,
    ) {
        self.general_blocking_tasks
            .lock()
            .push(GroupedGeneralBlockingTask {
                partition: partition as u64,
                diagnostics,
            });
    }

    pub fn snapshot(&self) -> GroupedExecutionDiagnosticsSnapshot {
        let decode = |value: &AtomicU64| value.load(Ordering::Relaxed).saturating_sub(1);
        let general_blocking_tasks = self
            .general_blocking_tasks
            .lock()
            .iter()
            .map(|task| {
                let (submitted, started, finished) = task.diagnostics.instants();
                let (submit_state, start_state, finish_state) = task.diagnostics.states();
                let blocking_submitted_us = submitted.map(|value| self.offset(value)).unwrap_or(0);
                let blocking_started_us = started.map(|value| self.offset(value)).unwrap_or(0);
                let blocking_finished_us = finished.map(|value| self.offset(value)).unwrap_or(0);
                GroupedGeneralBlockingTaskTiming {
                    task_class: task.diagnostics.task_class(),
                    partition: task.partition,
                    blocking_submitted_us,
                    blocking_started_us,
                    blocking_finished_us,
                    queue_wait_us: started
                        .zip(submitted)
                        .map(|(started, submitted)| {
                            started
                                .saturating_duration_since(submitted)
                                .as_micros()
                                .min(u128::from(u64::MAX)) as u64
                        })
                        .unwrap_or(0),
                    execution_us: finished
                        .zip(started)
                        .map(|(finished, started)| {
                            finished
                                .saturating_duration_since(started)
                                .as_micros()
                                .min(u128::from(u64::MAX)) as u64
                        })
                        .unwrap_or(0),
                    submit_state,
                    start_state,
                    finish_state,
                }
            })
            .collect();
        GroupedExecutionDiagnosticsSnapshot {
            general_async_submitted_us: decode(&self.general_async_submitted_us),
            general_async_started_us: decode(&self.general_async_started_us),
            storage_coordinator_started_us: decode(&self.storage_coordinator_started_us),
            storage_result_ready_us: decode(&self.storage_result_ready_us),
            storage_jobs: self.storage_jobs.lock().clone(),
            general_blocking_tasks,
        }
    }
}

/// A syntax-independent request for one local storage scan.
#[derive(Clone, Debug)]
pub struct StorageScan {
    pub measurement: MeasurementRef,
    pub projection: StorageProjection,
    pub time_range: TimeRange,
    pub predicate: Option<StoragePredicate>,
    pub time_order: TimeOrder,
    pub output_order: OutputOrder,
    pub execution: StorageExecutionOptions,
    /// Transitional execution state; move into a scan context before public use.
    pub cancellation: Option<ScanCancellation>,
}

pub use super::projected_aggregate::{AggregateIndexReadMetrics, AggregateNumber,
    AggregateProjection, ProjectedAggregateResult, ProjectedAggregateState, ProjectedAggregateWindowsResult};
pub use super::aggregate_sum::AggregateWideSum;

/// A simple aggregate that storage may evaluate without materializing rows.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum AggregateKind {
    Count,
    Sum,
    Min,
    Max,
    Avg,
    First,
    Last,
}

/// One storage-native aggregate request for a physical field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AggregateRequest {
    pub measurement: MeasurementRef,
    pub field: String,
    pub time_range: TimeRange,
    pub aggregates: Vec<AggregateKind>,
}

/// One fixed-width grouped aggregate window in nanoseconds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GroupedAggregateWindow {
    pub duration_ns: i64,
    pub origin_ns: i64,
}

impl GroupedAggregateWindow {
    /// Returns the fixed-width bucket containing `timestamp`.
    pub fn bucket_start(self, timestamp: i64) -> Result<i64, &'static str> {
        if self.duration_ns <= 0 {
            return Err("grouped aggregate window duration must be positive");
        }
        let duration = i128::from(self.duration_ns);
        let origin = i128::from(self.origin_ns);
        let timestamp = i128::from(timestamp);
        let bucket = origin + (timestamp - origin).div_euclid(duration) * duration;
        i64::try_from(bucket).map_err(|_| "grouped aggregate window start overflow")
    }
}

/// One exact tag-value set accepted by grouped aggregate pushdown.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct GroupedTagFilter {
    pub tag: String,
    /// Non-empty, sorted, and duplicate-free. An empty string is a valid value.
    pub accepted_values: Vec<String>,
}

/// One canonical physical field target in a grouped aggregate request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupedAggregateTarget {
    pub field: String,
    /// Planner-resolved physical type, including on shards where this field
    /// has no local values or metadata.
    pub expected_field_type: FieldType,
    /// Non-empty, sorted, and duplicate-free.
    pub aggregates: Vec<AggregateKind>,
}

/// One storage-native fixed-window aggregate request.
#[derive(Clone, Debug)]
pub struct GroupedAggregateRequest {
    pub measurement: MeasurementRef,
    /// Non-empty and sorted by unique physical field name.
    pub targets: Vec<GroupedAggregateTarget>,
    pub time_range: TimeRange,
    pub window: GroupedAggregateWindow,
    pub group_tags: Vec<String>,
    pub tag_filters: Vec<GroupedTagFilter>,
    /// Cooperative cancellation shared by queued and active storage-key jobs.
    pub cancellation: Option<ScanCancellation>,
}

/// Storage identity of one grouped aggregate state.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct GroupedAggregateKey {
    pub window_start: i64,
    /// Values are positional with `GroupedAggregateRequest::group_tags`.
    /// A missing series tag is represented by `None` and becomes SQL NULL.
    pub tag_values: Vec<Option<String>>,
}

/// One mergeable grouped aggregate state.
#[derive(Clone, Debug, PartialEq)]
pub struct GroupedAggregateGroup {
    pub key: GroupedAggregateKey,
    /// States are positional with `GroupedAggregateRequest::targets`.
    pub states: Vec<AggregateState>,
}

/// Exact local count of logical measurement rows in an inclusive time range.
///
/// Storage defines one logical row as one distinct visible timestamp within a
/// series, across the union of that series' field timestamp domains. Equal
/// timestamps in different series remain different rows.
#[derive(Clone, Debug)]
pub struct RowCountRequest {
    pub measurement: MeasurementRef,
    pub time_range: TimeRange,
    pub cancellation: Option<ScanCancellation>,
}

/// Lightweight evidence about which exact row-count path storage used.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RowCountMetrics {
    pub series_scanned: u64,
    pub timestamp_only_blocks: u64,
    pub timestamp_only_points: u64,
    pub raw_scans: u64,
    pub fallback_rows: u64,
    pub used_fallback: bool,
}

/// Result of one local exact row-domain count.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RowCountResult {
    pub count: u64,
    pub metrics: RowCountMetrics,
}

impl RowCountResult {
    /// Merges disjoint shard-local counts without silently wrapping.
    pub fn merge(&mut self, other: Self) -> Result<(), &'static str> {
        self.count = self
            .count
            .checked_add(other.count)
            .ok_or("row count overflow")?;
        self.metrics.series_scanned = self
            .metrics
            .series_scanned
            .checked_add(other.metrics.series_scanned)
            .ok_or("row-count series metric overflow")?;
        self.metrics.timestamp_only_blocks = self
            .metrics
            .timestamp_only_blocks
            .checked_add(other.metrics.timestamp_only_blocks)
            .ok_or("row-count block metric overflow")?;
        self.metrics.timestamp_only_points = self
            .metrics
            .timestamp_only_points
            .checked_add(other.metrics.timestamp_only_points)
            .ok_or("row-count point metric overflow")?;
        self.metrics.raw_scans = self
            .metrics
            .raw_scans
            .checked_add(other.metrics.raw_scans)
            .ok_or("row-count raw-scan metric overflow")?;
        self.metrics.fallback_rows = self
            .metrics
            .fallback_rows
            .checked_add(other.metrics.fallback_rows)
            .ok_or("row-count fallback metric overflow")?;
        self.metrics.used_fallback |= other.metrics.used_fallback;
        Ok(())
    }
}

/// Typed mergeable values accumulated for an aggregate request.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AggregateValues {
    Integer { sum: i64, min: i64, max: i64 },
    Unsigned { sum: u64, min: u64, max: u64 },
    Float { sum: f64, min: f64, max: f64 },
    CountOnly,
}

impl AggregateValues {
    pub(crate) fn numeric_components(self) -> Option<[AggregateNumber; 3]> {
        match self {
            Self::Integer { sum, min, max } => {
                Some([sum, min, max].map(numeric_aggregate::NumericValue::number))
            }
            Self::Unsigned { sum, min, max } => {
                Some([sum, min, max].map(numeric_aggregate::NumericValue::number))
            }
            Self::Float { sum, min, max } => {
                Some([sum, min, max].map(numeric_aggregate::NumericValue::number))
            }
            Self::CountOnly => None,
        }
    }
}

/// One typed value associated with a logical timestamp.
#[derive(Clone, Debug, PartialEq)]
pub enum AggregatePointValue {
    Integer(i64),
    Unsigned(u64),
    Float(f64),
    Boolean(bool),
    String(String),
}

impl AggregatePointValue {
    pub(crate) fn field_type(&self) -> FieldType {
        match self {
            Self::Integer(_) => crate::models::INTEGER,
            Self::Unsigned(_) => crate::models::UNSIGNED,
            Self::Float(_) => crate::models::FLOAT,
            Self::Boolean(_) => crate::models::BOOL,
            Self::String(_) => crate::models::STRING,
        }
    }
}

/// A visible endpoint used by time-ordered FIRST/LAST aggregation.
#[derive(Clone, Debug, PartialEq)]
pub struct AggregatePoint {
    pub time: i64,
    pub value: AggregatePointValue,
}

/// Mergeable aggregate state. Use `average()` to avoid wrapping integer sums.
#[derive(Clone, Debug, PartialEq)]
pub struct AggregateState {
    pub field_type: FieldType,
    pub count: u64,
    pub values: Option<AggregateValues>,
    /// Exact integer total for AVG; None for nonintegers or ambiguous legacy summaries.
    pub wide_sum: Option<AggregateWideSum>,
    pub first: Option<AggregatePoint>,
    pub last: Option<AggregatePoint>,
}

impl AggregateState {
    pub const fn empty(field_type: FieldType) -> Self {
        Self {
            field_type,
            count: 0,
            values: None,
            wide_sum: AggregateWideSum::zero(field_type),
            first: None,
            last: None,
        }
    }

    fn add_endpoints(&mut self, time: i64, value: AggregatePointValue) {
        let point = AggregatePoint { time, value };
        if self
            .first
            .as_ref()
            .is_none_or(|current| point.time < current.time)
        {
            self.first = Some(point.clone());
        }
        if self
            .last
            .as_ref()
            .is_none_or(|current| point.time > current.time)
        {
            self.last = Some(point);
        }
    }

    pub fn add_integer(&mut self, value: i64) {
        self.count += 1;
        self.wide_sum = AggregateWideSum::merge(
            self.wide_sum,
            numeric_aggregate::NumericValue::wide(value),
        )
        .expect("an integer sum with a u64 point count fits i128");
        self.values = Some(match self.values {
            Some(AggregateValues::Integer { sum, min, max }) => AggregateValues::Integer {
                sum: numeric_aggregate::NumericValue::add(sum, value),
                min: numeric_aggregate::NumericValue::min(min, value),
                max: numeric_aggregate::NumericValue::max(max, value),
            },
            _ => AggregateValues::Integer {
                sum: value,
                min: value,
                max: value,
            },
        });
    }

    pub fn add_integer_at(&mut self, time: i64, value: i64) {
        self.add_integer(value);
        self.add_endpoints(time, AggregatePointValue::Integer(value));
    }

    pub fn add_unsigned(&mut self, value: u64) {
        self.count += 1;
        self.wide_sum = AggregateWideSum::merge(
            self.wide_sum,
            numeric_aggregate::NumericValue::wide(value),
        )
        .expect("an unsigned sum with a u64 point count fits u128");
        self.values = Some(match self.values {
            Some(AggregateValues::Unsigned { sum, min, max }) => AggregateValues::Unsigned {
                sum: numeric_aggregate::NumericValue::add(sum, value),
                min: numeric_aggregate::NumericValue::min(min, value),
                max: numeric_aggregate::NumericValue::max(max, value),
            },
            _ => AggregateValues::Unsigned {
                sum: value,
                min: value,
                max: value,
            },
        });
    }

    pub fn add_unsigned_at(&mut self, time: i64, value: u64) {
        self.add_unsigned(value);
        self.add_endpoints(time, AggregatePointValue::Unsigned(value));
    }

    pub fn add_float(&mut self, value: f64) {
        self.count += 1;
        self.values = Some(match self.values {
            Some(AggregateValues::Float { sum, min, max }) => AggregateValues::Float {
                sum: numeric_aggregate::NumericValue::add(sum, value),
                min: numeric_aggregate::NumericValue::min(min, value),
                max: numeric_aggregate::NumericValue::max(max, value),
            },
            _ => AggregateValues::Float {
                // Match Arrow's floating SUM accumulator, which starts at +0.0.
                sum: numeric_aggregate::NumericValue::singleton_sum(value),
                min: value,
                max: value,
            },
        });
    }

    pub fn add_float_at(&mut self, time: i64, value: f64) {
        self.add_float(value);
        self.add_endpoints(time, AggregatePointValue::Float(value));
    }

    pub fn add_boolean_at(&mut self, time: i64, value: bool) {
        self.add_count(1);
        self.add_endpoints(time, AggregatePointValue::Boolean(value));
    }

    pub fn add_string_at(&mut self, time: i64, value: String) {
        self.add_count(1);
        self.add_endpoints(time, AggregatePointValue::String(value));
    }

    pub fn add_count(&mut self, count: u64) {
        self.count += count;
        if self.count != 0 && self.values.is_none() {
            self.values = Some(AggregateValues::CountOnly);
        }
    }

    pub fn merge(&mut self, other: Self) -> Result<(), &'static str> {
        let Self {
            field_type,
            count: other_count,
            values: other_values,
            wide_sum: other_wide_sum,
            first: other_first,
            last: other_last,
        } = other;
        if self.field_type != field_type {
            return Err("cannot merge aggregate states with different field types");
        }
        if other_count == 0 {
            return Ok(());
        }
        if self.count == 0 {
            *self = Self {
                field_type,
                count: other_count,
                values: other_values,
                wide_sum: other_wide_sum,
                first: other_first,
                last: other_last,
            };
            return Ok(());
        }
        let count = self.count.wrapping_add(other_count);
        let wide_sum = AggregateWideSum::merge(self.wide_sum, other_wide_sum)?;
        self.values = Some(match (self.values, other_values) {
            (
                Some(AggregateValues::Integer {
                    sum: left_sum,
                    min: left_min,
                    max: left_max,
                }),
                Some(AggregateValues::Integer {
                    sum: right_sum,
                    min: right_min,
                    max: right_max,
                }),
            ) => AggregateValues::Integer {
                sum: numeric_aggregate::NumericValue::add(left_sum, right_sum),
                min: numeric_aggregate::NumericValue::min(left_min, right_min),
                max: numeric_aggregate::NumericValue::max(left_max, right_max),
            },
            (
                Some(AggregateValues::Unsigned {
                    sum: left_sum,
                    min: left_min,
                    max: left_max,
                }),
                Some(AggregateValues::Unsigned {
                    sum: right_sum,
                    min: right_min,
                    max: right_max,
                }),
            ) => AggregateValues::Unsigned {
                sum: numeric_aggregate::NumericValue::add(left_sum, right_sum),
                min: numeric_aggregate::NumericValue::min(left_min, right_min),
                max: numeric_aggregate::NumericValue::max(left_max, right_max),
            },
            (
                Some(AggregateValues::Float {
                    sum: left_sum,
                    min,
                    max,
                }),
                Some(AggregateValues::Float {
                    sum: right_sum,
                    min: right_min,
                    max: right_max,
                }),
            ) => AggregateValues::Float {
                sum: numeric_aggregate::NumericValue::add(left_sum, right_sum),
                min: numeric_aggregate::NumericValue::min(min, right_min),
                max: numeric_aggregate::NumericValue::max(max, right_max),
            },
            (Some(AggregateValues::CountOnly), Some(AggregateValues::CountOnly)) => {
                AggregateValues::CountOnly
            }
            _ => return Err("cannot merge incompatible aggregate value states"),
        });
        self.count = count;
        self.wide_sum = wide_sum;
        if let Some(candidate) = other_first
            && self
                .first
                .as_ref()
                .is_none_or(|current| candidate.time < current.time)
        {
            self.first = Some(candidate);
        }
        if let Some(candidate) = other_last
            && self
                .last
                .as_ref()
                .is_none_or(|current| candidate.time > current.time)
        {
            self.last = Some(candidate);
        }
        Ok(())
    }

    /// Returns a floating-point mean, using the exact wide sum for integer fields.
    /// Empty input returns None; an ambiguous legacy summary returns an error.
    pub fn average(&self) -> Result<Option<f64>, &'static str> {
        super::aggregate_sum::average(
            self.field_type,
            self.count,
            self.values
                .and_then(AggregateValues::numeric_components)
                .map(|v| v[0]),
            self.wide_sum,
        )
    }
}

/// Lightweight evidence about which aggregate path storage used.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AggregateScanMetrics {
    /// Summary records consumed from the disk-resident aggregate index.
    pub index_records: u64,
    /// Highest aggregate level consumed (zero when no index summary was used).
    pub index_levels: u8,
    /// Leaf blocks decoded because a query only covers part of their time range.
    pub boundary_blocks: u64,
    pub candidate_blocks: u64,
    pub statistics_blocks: u64,
    pub decoded_blocks: u64,
    /// Physical TSM blocks read through visibility-aware overlap subrange scans.
    pub overlap_raw_blocks: u64,
    /// Disjoint TSM overlap intervals evaluated through the raw iterator.
    pub overlap_intervals: u64,
    /// Raw iterator executions, including localized overlap and whole-request fallback scans.
    pub raw_scans: u64,
    /// True only when the complete aggregate request used the raw iterator.
    pub used_fallback: bool,
}

impl AggregateScanMetrics {
    pub(crate) fn merge(&mut self, other: Self) {
        self.index_records += other.index_records;
        self.index_levels = self.index_levels.max(other.index_levels);
        self.boundary_blocks += other.boundary_blocks;
        self.candidate_blocks += other.candidate_blocks;
        self.statistics_blocks += other.statistics_blocks;
        self.decoded_blocks += other.decoded_blocks;
        self.overlap_raw_blocks += other.overlap_raw_blocks;
        self.overlap_intervals += other.overlap_intervals;
        self.raw_scans += other.raw_scans;
        self.used_fallback |= other.used_fallback;
    }
}

/// Result of one local storage aggregate scan.
#[derive(Clone, Debug, PartialEq)]
pub struct AggregateResult {
    pub state: AggregateState,
    pub metrics: AggregateScanMetrics,
}

/// Query-local evidence for one grouped aggregate storage request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(not(feature = "slow-trace"), derive(Copy))]
pub struct GroupedAggregateMetrics {
    pub series_matched: u64,
    pub fields_projected: u64,
    pub blocks_candidate: u64,
    pub blocks_statistics_used: u64,
    pub blocks_raw_decoded: u64,
    pub points_raw_decoded: u64,
    pub storage_scan_us: u64,
    pub aggregate_merge_us: u64,
    /// True when storage conservatively evaluated the complete request through
    /// the canonical visibility-aware raw iterator.
    pub used_raw_fallback: bool,
    /// True when storage cannot prove that its Float64 grouped MIN/MAX state is
    /// bitwise equivalent to DataFusion's order-dependent grouped accumulator.
    /// The query bridge must execute the original DataFusion plan instead of
    /// materializing the accompanying storage result.
    pub requires_datafusion_fallback: bool,
    #[cfg(feature = "slow-trace")]
    pub profile: GroupedAggregateProfileMetrics,
}

/// Detailed grouped-aggregate evidence emitted only by an instrumented build.
#[cfg(feature = "slow-trace")]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GroupedAggregateProfileMetrics {
    pub shard_scans: u64,
    pub preflight_tsi_lookup_us: u64,
    pub measurement_lookup_us: u64,
    pub tsi_lookup_us: u64,
    pub iterator_construction_us: u64,
    pub iterator_setup_us: u64,
    pub iterator_next_us: u64,
    pub timestamp_domain_us: u64,
    pub aggregate_update_us: u64,
    pub finalization_us: u64,
    pub field_cursors: u64,
    pub file_index_searches: u64,
    pub cursor_seeks: u64,
    pub tsm_file_visits: u64,
    pub blocks_considered: u64,
    pub blocks_decoded: u64,
    pub values_decoded: u64,
    pub timestamp_only_points_decoded: u64,
    pub block_bytes: u64,
    pub block_lookup_us: u64,
    pub block_read_decode_us: u64,
    pub visibility_row_assembly_us: u64,
    pub iterator_tasks_spawned: u64,
    pub logical_rows: u64,
    pub aggregate_updates: u64,
    pub groups: u64,
    /// Actual physical decode operations, partitioned by their execution role.
    pub physical_target_blocks_decoded: u64,
    pub physical_visibility_blocks_decoded: u64,
    pub physical_row_domain_precheck_blocks_decoded: u64,
    pub physical_row_domain_fallback_blocks_decoded: u64,
    pub physical_target_values_decoded: u64,
    pub physical_visibility_values_decoded: u64,
    pub physical_row_domain_precheck_points_decoded: u64,
    pub physical_row_domain_fallback_values_decoded: u64,
    /// Logical values consumed by target aggregation versus logical rows consumed
    /// solely to reconstruct the sparse row domain.
    pub target_values_surviving_query_range: u64,
    pub target_values_consumed: u64,
    pub row_domain_logical_rows_consumed: u64,
    pub target_raw_iterator_scans: u64,
    pub visibility_raw_iterator_scans: u64,
    pub row_domain_precheck_scans: u64,
    pub row_domain_fallback_scans: u64,
    pub row_domain_fields_total: u64,
    pub row_domain_fields_covered: u64,
    pub row_domain_fields_uncovered: u64,
    pub row_domain_precheck_fields: u64,
    /// Uncovered fields whose possible grouped-key domain was proven to be a
    /// subset of grouped keys already produced by completed target scans.
    pub row_domain_precheck_elided_fields: u64,
    /// Elisions justified by exact target-group coverage of every bucket in a
    /// conservative TSM-entry bucket superset.
    pub row_domain_precheck_elision_target_group_coverage: u64,
    pub row_domain_precheck_skipped_full_coverage: u64,
    pub row_domain_direct_reject_cache: u64,
    pub row_domain_direct_reject_tombstone: u64,
    pub row_domain_direct_reject_overlap: u64,
    pub storage_keys_total: u64,
    pub partition_workers_total: u64,
    pub partition_workers_started: u64,
    pub partition_workers_completed: u64,
    pub partition_workers_cancelled: u64,
    pub worker_active_peak: u64,
    pub global_cpu_capacity: u64,
    pub global_active_jobs_peak: u64,
    pub permit_requests: u64,
    pub permit_immediate_count: u64,
    pub global_permit_wait_count: u64,
    pub global_permit_wait_us: u64,
    pub permit_wait_us_min: u64,
    pub permit_wait_us_p50: u64,
    pub permit_wait_us_p95: u64,
    pub permit_wait_us_max: u64,
    pub permit_hold_us_total: u64,
    pub permit_hold_us_min: u64,
    pub permit_hold_us_p50: u64,
    pub permit_hold_us_p95: u64,
    pub permit_hold_us_max: u64,
    pub spawn_blocking_jobs: u64,
    pub blocking_jobs_started: u64,
    pub blocking_jobs_completed: u64,
    pub blocking_active_current: u64,
    pub blocking_active_peak: u64,
    pub blocking_queued_current: u64,
    pub blocking_queued_peak: u64,
    pub blocking_queue_wait_us_total: u64,
    pub blocking_queue_wait_us_min: u64,
    pub blocking_queue_wait_us_p50: u64,
    pub blocking_queue_wait_us_p95: u64,
    pub blocking_queue_wait_us_max: u64,
    pub keys_per_partition_min: u64,
    pub keys_per_partition_max: u64,
    pub candidates_per_partition_min: u64,
    pub candidates_per_partition_max: u64,
    pub raw_blocks_per_partition_min: u64,
    pub raw_blocks_per_partition_max: u64,
    pub worker_execution_us_min: u64,
    pub worker_execution_us_p50: u64,
    pub worker_execution_us_p95: u64,
    pub worker_execution_us_max: u64,
    pub worker_execution_us_total: u64,
    pub merge_us: u64,
    pub coordinator_start_unix_us: u64,
    pub coordinator_permit_admission_done_us: u64,
    pub coordinator_all_jobs_submitted_us: u64,
    pub coordinator_all_jobs_completed_us: u64,
    pub coordinator_result_ready_us: u64,
    pub coordinator_admission_wait_us: u64,
    pub coordinator_storage_job_wait_us: u64,
    pub coordinator_result_assembly_us: u64,
    pub candidate_file_index_searches: u64,
    pub candidate_files_with_entries: u64,
    pub candidate_index_entries_inspected: u64,
    /// Non-overlapping parent wall stages. Physical decode counters below are
    /// cumulative child work and must not be summed with these as wall time.
    pub metadata_setup_wall_us: u64,
    /// Nested wall time for complete tag-key discovery and caller normalization.
    pub metadata_measurement_tag_keys_us: u64,
    pub metadata_tag_key_partition_count: u64,
    pub metadata_tag_key_tasks_submitted: u64,
    pub metadata_tag_key_tasks_started: u64,
    pub metadata_tag_key_tasks_completed: u64,
    pub metadata_tag_key_queue_wait_us_total: u64,
    pub metadata_tag_key_queue_wait_us_max: u64,
    pub metadata_tag_key_execution_us_total: u64,
    pub metadata_tag_key_execution_us_max: u64,
    pub metadata_tag_key_caller_wait_all_us: u64,
    pub tsi_series_selection_wall_us: u64,
    pub reader_snapshot_wall_us: u64,
    pub diagnostic_candidate_observation_wall_us: u64,
    pub candidate_discovery_wall_us: u64,
    pub block_eligibility_wall_us: u64,
    pub raw_interval_construction_wall_us: u64,
    pub direct_raw_consume_wall_us: u64,
    pub raw_iterator_wall_us: u64,
    pub row_domain_wall_us: u64,
    /// Nested TSI diagnostics; do not add these to the non-overlapping wall total.
    pub tsi_predicate_selection_us: u64,
    pub tsi_series_key_materialization_us: u64,
    pub tsi_tag_value_cache_lock_acquisitions: u64,
    pub tsi_tag_value_cache_lock_wait_us: u64,
    pub tsi_tag_value_cache_lock_hold_us: u64,
    /// Cheap structural invariants for the pre-permit stages.
    pub metadata_field_count: u64,
    pub metadata_identity_tag_key_count: u64,
    pub tag_predicates_evaluated: u64,
    pub tag_values_requested: u64,
    pub tsi_selected_tag_sets: u64,
    pub tsi_selected_series_keys: u64,
    pub reader_snapshot_tsm_readers: u64,
    /// Mutually exclusive terminal decisions for every physical candidate block.
    pub eligibility_stats_used: u64,
    pub eligibility_reject_partial_query_range: u64,
    pub eligibility_reject_cross_bucket: u64,
    pub eligibility_reject_overlap: u64,
    pub eligibility_reject_missing_statistics: u64,
    pub eligibility_reject_unsupported_statistics: u64,
    pub eligibility_reject_float_nan: u64,
    pub eligibility_reject_float_signed_zero: u64,
    pub eligibility_reject_float_zero_crossing: u64,
    /// The block itself is Float64-safe, but another block forced the whole
    /// request onto DataFusion to preserve order-dependent MIN/MAX semantics.
    pub eligibility_reject_float_request_fallback: u64,
    pub eligibility_reject_cache: u64,
    pub eligibility_reject_vlog: u64,
    pub eligibility_reject_tombstone: u64,
    pub eligibility_reject_residual_filter: u64,
    pub eligibility_reject_other: u64,
    /// Non-terminal layout attributes. A block may increment more than one.
    pub layout_query_clips_left: u64,
    pub layout_query_clips_right: u64,
    pub layout_within_one_bucket: u64,
    pub layout_crosses_two_buckets: u64,
    pub layout_crosses_more_than_two_buckets: u64,
    pub layout_cross_bucket_span_ge_window: u64,
    pub layout_cross_bucket_span_lt_window: u64,
    /// Non-terminal statistics evidence collected before any request-level fallback.
    pub observed_usable_statistics: u64,
    pub observed_missing_statistics: u64,
    pub observed_unsupported_statistics: u64,
    pub observed_float_safe: u64,
    pub observed_float_nan: u64,
    pub observed_float_signed_zero: u64,
    pub observed_float_zero_crossing: u64,
    pub observed_float_infinity: u64,
    pub observed_overlap: u64,
    pub observed_cache_visibility: u64,
    pub observed_vlog_visibility: u64,
    pub observed_tombstone_visibility: u64,
    pub observed_residual_filter: u64,
    /// Exact per-query values retained only in slow-trace builds so merged shard
    /// results can report exact median and p95 distributions without per-block logs.
    pub candidate_block_span_ns: Vec<u64>,
    pub candidate_bucket_count: Vec<u64>,
    /// Exact slow-trace samples used to calculate query-level interval union
    /// and physical-block decode duplication without per-block log lines.
    pub raw_intervals: Vec<GroupedRawIntervalSample>,
    pub statistics_block_identities: Vec<GroupedPhysicalBlockIdentity>,
    pub decoded_block_identities: Vec<GroupedPhysicalBlockIdentity>,
    /// Exact scheduler samples retained to preserve query-level percentiles
    /// when local partition and shard results are merged.
    pub(crate) permit_wait_us_samples: Vec<u64>,
    pub(crate) permit_hold_us_samples: Vec<u64>,
    pub(crate) blocking_queue_wait_us_samples: Vec<u64>,
    pub(crate) blocking_execution_us_samples: Vec<u64>,
}

#[cfg(feature = "slow-trace")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupedBlockEligibility {
    StatisticsUsed,
    PartialQueryRange,
    CrossBucket,
    Overlap,
    MissingStatistics,
    UnsupportedStatistics,
    FloatNan,
    FloatSignedZero,
    FloatZeroCrossing,
    FloatRequestFallback,
    Cache,
    Vlog,
    Tombstone,
    ResidualFilter,
    Other,
}

impl GroupedAggregateMetrics {
    pub(crate) fn merge(&mut self, other: Self) {
        self.series_matched = self.series_matched.saturating_add(other.series_matched);
        self.fields_projected = self.fields_projected.max(other.fields_projected);
        self.blocks_candidate = self.blocks_candidate.saturating_add(other.blocks_candidate);
        self.blocks_statistics_used = self
            .blocks_statistics_used
            .saturating_add(other.blocks_statistics_used);
        self.blocks_raw_decoded = self
            .blocks_raw_decoded
            .saturating_add(other.blocks_raw_decoded);
        self.points_raw_decoded = self
            .points_raw_decoded
            .saturating_add(other.points_raw_decoded);
        self.storage_scan_us = self.storage_scan_us.saturating_add(other.storage_scan_us);
        self.aggregate_merge_us = self
            .aggregate_merge_us
            .saturating_add(other.aggregate_merge_us);
        self.used_raw_fallback |= other.used_raw_fallback;
        self.requires_datafusion_fallback |= other.requires_datafusion_fallback;
        #[cfg(feature = "slow-trace")]
        self.profile.merge(other.profile);
    }
}

#[cfg(feature = "slow-trace")]
impl GroupedAggregateProfileMetrics {
    fn merge(&mut self, mut other: Self) {
        let had_partition_workers = self.partition_workers_total > 0;
        let other_has_partition_workers = other.partition_workers_total > 0;
        macro_rules! add {
            ($field:ident) => {
                self.$field = self.$field.saturating_add(other.$field);
            };
        }
        macro_rules! partition_min {
            ($field:ident) => {
                if other_has_partition_workers
                    && (!had_partition_workers || other.$field < self.$field)
                {
                    self.$field = other.$field;
                }
            };
        }
        add!(shard_scans);
        add!(preflight_tsi_lookup_us);
        add!(measurement_lookup_us);
        add!(tsi_lookup_us);
        add!(iterator_construction_us);
        add!(iterator_setup_us);
        add!(iterator_next_us);
        add!(timestamp_domain_us);
        add!(aggregate_update_us);
        add!(finalization_us);
        add!(field_cursors);
        add!(file_index_searches);
        add!(cursor_seeks);
        add!(tsm_file_visits);
        add!(blocks_considered);
        add!(blocks_decoded);
        add!(values_decoded);
        add!(timestamp_only_points_decoded);
        add!(block_bytes);
        add!(block_lookup_us);
        add!(block_read_decode_us);
        add!(visibility_row_assembly_us);
        add!(iterator_tasks_spawned);
        add!(logical_rows);
        add!(aggregate_updates);
        add!(groups);
        add!(physical_target_blocks_decoded);
        add!(physical_visibility_blocks_decoded);
        add!(physical_row_domain_precheck_blocks_decoded);
        add!(physical_row_domain_fallback_blocks_decoded);
        add!(physical_target_values_decoded);
        add!(physical_visibility_values_decoded);
        add!(physical_row_domain_precheck_points_decoded);
        add!(physical_row_domain_fallback_values_decoded);
        add!(target_values_surviving_query_range);
        add!(target_values_consumed);
        add!(row_domain_logical_rows_consumed);
        add!(target_raw_iterator_scans);
        add!(visibility_raw_iterator_scans);
        add!(row_domain_precheck_scans);
        add!(row_domain_fallback_scans);
        add!(row_domain_fields_total);
        add!(row_domain_fields_covered);
        add!(row_domain_fields_uncovered);
        add!(row_domain_precheck_fields);
        add!(row_domain_precheck_elided_fields);
        add!(row_domain_precheck_elision_target_group_coverage);
        add!(row_domain_precheck_skipped_full_coverage);
        add!(row_domain_direct_reject_cache);
        add!(row_domain_direct_reject_tombstone);
        add!(row_domain_direct_reject_overlap);
        add!(storage_keys_total);
        add!(partition_workers_total);
        add!(partition_workers_started);
        add!(partition_workers_completed);
        add!(partition_workers_cancelled);
        self.worker_active_peak = self.worker_active_peak.max(other.worker_active_peak);
        self.global_cpu_capacity = self.global_cpu_capacity.max(other.global_cpu_capacity);
        self.global_active_jobs_peak = self
            .global_active_jobs_peak
            .max(other.global_active_jobs_peak);
        add!(permit_immediate_count);
        add!(spawn_blocking_jobs);
        add!(blocking_jobs_started);
        add!(blocking_jobs_completed);
        add!(blocking_active_current);
        self.blocking_active_peak = self.blocking_active_peak.max(other.blocking_active_peak);
        add!(blocking_queued_current);
        self.blocking_queued_peak = self.blocking_queued_peak.max(other.blocking_queued_peak);
        partition_min!(keys_per_partition_min);
        self.keys_per_partition_max = self
            .keys_per_partition_max
            .max(other.keys_per_partition_max);
        partition_min!(candidates_per_partition_min);
        self.candidates_per_partition_max = self
            .candidates_per_partition_max
            .max(other.candidates_per_partition_max);
        partition_min!(raw_blocks_per_partition_min);
        self.raw_blocks_per_partition_max = self
            .raw_blocks_per_partition_max
            .max(other.raw_blocks_per_partition_max);
        add!(merge_us);
        if self.coordinator_start_unix_us == 0
            || (other.coordinator_start_unix_us != 0
                && other.coordinator_start_unix_us < self.coordinator_start_unix_us)
        {
            self.coordinator_start_unix_us = other.coordinator_start_unix_us;
        }
        add!(coordinator_permit_admission_done_us);
        add!(coordinator_all_jobs_submitted_us);
        add!(coordinator_all_jobs_completed_us);
        add!(coordinator_result_ready_us);
        add!(coordinator_admission_wait_us);
        add!(coordinator_storage_job_wait_us);
        add!(coordinator_result_assembly_us);
        add!(candidate_file_index_searches);
        add!(candidate_files_with_entries);
        add!(candidate_index_entries_inspected);
        add!(metadata_setup_wall_us);
        add!(metadata_measurement_tag_keys_us);
        add!(metadata_tag_key_partition_count);
        add!(metadata_tag_key_tasks_submitted);
        add!(metadata_tag_key_tasks_started);
        add!(metadata_tag_key_tasks_completed);
        add!(metadata_tag_key_queue_wait_us_total);
        self.metadata_tag_key_queue_wait_us_max = self
            .metadata_tag_key_queue_wait_us_max
            .max(other.metadata_tag_key_queue_wait_us_max);
        add!(metadata_tag_key_execution_us_total);
        self.metadata_tag_key_execution_us_max = self
            .metadata_tag_key_execution_us_max
            .max(other.metadata_tag_key_execution_us_max);
        add!(metadata_tag_key_caller_wait_all_us);
        add!(tsi_series_selection_wall_us);
        add!(reader_snapshot_wall_us);
        add!(diagnostic_candidate_observation_wall_us);
        add!(candidate_discovery_wall_us);
        add!(block_eligibility_wall_us);
        add!(raw_interval_construction_wall_us);
        add!(direct_raw_consume_wall_us);
        add!(raw_iterator_wall_us);
        add!(row_domain_wall_us);
        add!(tsi_predicate_selection_us);
        add!(tsi_series_key_materialization_us);
        add!(tsi_tag_value_cache_lock_acquisitions);
        add!(tsi_tag_value_cache_lock_wait_us);
        add!(tsi_tag_value_cache_lock_hold_us);
        add!(metadata_field_count);
        add!(metadata_identity_tag_key_count);
        add!(tag_predicates_evaluated);
        add!(tag_values_requested);
        add!(tsi_selected_tag_sets);
        add!(tsi_selected_series_keys);
        add!(reader_snapshot_tsm_readers);
        add!(eligibility_stats_used);
        add!(eligibility_reject_partial_query_range);
        add!(eligibility_reject_cross_bucket);
        add!(eligibility_reject_overlap);
        add!(eligibility_reject_missing_statistics);
        add!(eligibility_reject_unsupported_statistics);
        add!(eligibility_reject_float_nan);
        add!(eligibility_reject_float_signed_zero);
        add!(eligibility_reject_float_zero_crossing);
        add!(eligibility_reject_float_request_fallback);
        add!(eligibility_reject_cache);
        add!(eligibility_reject_vlog);
        add!(eligibility_reject_tombstone);
        add!(eligibility_reject_residual_filter);
        add!(eligibility_reject_other);
        add!(layout_query_clips_left);
        add!(layout_query_clips_right);
        add!(layout_within_one_bucket);
        add!(layout_crosses_two_buckets);
        add!(layout_crosses_more_than_two_buckets);
        add!(layout_cross_bucket_span_ge_window);
        add!(layout_cross_bucket_span_lt_window);
        add!(observed_usable_statistics);
        add!(observed_missing_statistics);
        add!(observed_unsupported_statistics);
        add!(observed_float_safe);
        add!(observed_float_nan);
        add!(observed_float_signed_zero);
        add!(observed_float_zero_crossing);
        add!(observed_float_infinity);
        add!(observed_overlap);
        add!(observed_cache_visibility);
        add!(observed_vlog_visibility);
        add!(observed_tombstone_visibility);
        add!(observed_residual_filter);
        self.candidate_block_span_ns
            .append(&mut other.candidate_block_span_ns);
        self.candidate_bucket_count
            .append(&mut other.candidate_bucket_count);
        self.raw_intervals.append(&mut other.raw_intervals);
        self.statistics_block_identities
            .append(&mut other.statistics_block_identities);
        self.decoded_block_identities
            .append(&mut other.decoded_block_identities);
        self.permit_wait_us_samples
            .append(&mut other.permit_wait_us_samples);
        self.permit_hold_us_samples
            .append(&mut other.permit_hold_us_samples);
        self.blocking_queue_wait_us_samples
            .append(&mut other.blocking_queue_wait_us_samples);
        self.blocking_execution_us_samples
            .append(&mut other.blocking_execution_us_samples);
        self.recompute_scheduler_distributions();
    }

    pub(crate) fn record_scheduler_samples(
        &mut self,
        permit_wait_us: Vec<u64>,
        permit_hold_us: Vec<u64>,
        blocking_queue_wait_us: Vec<u64>,
        blocking_execution_us: Vec<u64>,
    ) {
        self.permit_wait_us_samples = permit_wait_us;
        self.permit_hold_us_samples = permit_hold_us;
        self.blocking_queue_wait_us_samples = blocking_queue_wait_us;
        self.blocking_execution_us_samples = blocking_execution_us;
        self.recompute_scheduler_distributions();
    }

    fn recompute_scheduler_distributions(&mut self) {
        fn summarize(samples: &mut [u64]) -> (u64, u64, u64, u64, u64) {
            if samples.is_empty() {
                return (0, 0, 0, 0, 0);
            }
            samples.sort_unstable();
            let percentile = |percentile: usize| {
                let rank = samples.len().saturating_mul(percentile).div_ceil(100);
                samples[rank.saturating_sub(1).min(samples.len() - 1)]
            };
            (
                samples.iter().copied().fold(0u64, u64::saturating_add),
                samples[0],
                percentile(50),
                percentile(95),
                samples[samples.len() - 1],
            )
        }

        self.permit_requests = self.permit_wait_us_samples.len() as u64;
        self.global_permit_wait_count = self
            .permit_requests
            .saturating_sub(self.permit_immediate_count);
        (
            self.global_permit_wait_us,
            self.permit_wait_us_min,
            self.permit_wait_us_p50,
            self.permit_wait_us_p95,
            self.permit_wait_us_max,
        ) = summarize(&mut self.permit_wait_us_samples);
        (
            self.permit_hold_us_total,
            self.permit_hold_us_min,
            self.permit_hold_us_p50,
            self.permit_hold_us_p95,
            self.permit_hold_us_max,
        ) = summarize(&mut self.permit_hold_us_samples);
        (
            self.blocking_queue_wait_us_total,
            self.blocking_queue_wait_us_min,
            self.blocking_queue_wait_us_p50,
            self.blocking_queue_wait_us_p95,
            self.blocking_queue_wait_us_max,
        ) = summarize(&mut self.blocking_queue_wait_us_samples);
        (
            self.worker_execution_us_total,
            self.worker_execution_us_min,
            self.worker_execution_us_p50,
            self.worker_execution_us_p95,
            self.worker_execution_us_max,
        ) = summarize(&mut self.blocking_execution_us_samples);
    }

    pub(crate) fn merge_physical_scan(&mut self, scan: Self, kind: GroupedPhysicalScanKind) {
        match kind {
            GroupedPhysicalScanKind::Target => {
                self.target_raw_iterator_scans = self.target_raw_iterator_scans.saturating_add(1);
                self.physical_target_blocks_decoded = self
                    .physical_target_blocks_decoded
                    .saturating_add(scan.blocks_decoded);
                self.physical_target_values_decoded = self
                    .physical_target_values_decoded
                    .saturating_add(scan.values_decoded);
            }
            GroupedPhysicalScanKind::VisibilityOverlap => {
                self.visibility_raw_iterator_scans =
                    self.visibility_raw_iterator_scans.saturating_add(1);
                self.physical_visibility_blocks_decoded = self
                    .physical_visibility_blocks_decoded
                    .saturating_add(scan.blocks_decoded);
                self.physical_visibility_values_decoded = self
                    .physical_visibility_values_decoded
                    .saturating_add(scan.values_decoded);
            }
            GroupedPhysicalScanKind::RowDomainPrecheck => {
                self.row_domain_precheck_scans = self.row_domain_precheck_scans.saturating_add(1);
                self.physical_row_domain_precheck_blocks_decoded = self
                    .physical_row_domain_precheck_blocks_decoded
                    .saturating_add(scan.blocks_decoded);
                self.physical_row_domain_precheck_points_decoded = self
                    .physical_row_domain_precheck_points_decoded
                    .saturating_add(scan.values_decoded);
            }
            GroupedPhysicalScanKind::RowDomainFallback => {
                self.row_domain_fallback_scans = self.row_domain_fallback_scans.saturating_add(1);
                self.physical_row_domain_fallback_blocks_decoded = self
                    .physical_row_domain_fallback_blocks_decoded
                    .saturating_add(scan.blocks_decoded);
                self.physical_row_domain_fallback_values_decoded = self
                    .physical_row_domain_fallback_values_decoded
                    .saturating_add(scan.values_decoded);
            }
        }
        self.merge(scan);
    }

    pub(crate) fn record_raw_interval(
        &mut self,
        scope: String,
        range: TimeRange,
        kind: GroupedRawIntervalKind,
    ) {
        self.raw_intervals.push(GroupedRawIntervalSample {
            scope,
            start_time: range.start_time,
            end_time: range.end_time,
            kind,
        });
    }

    pub(crate) fn record_statistics_block(&mut self, identity: GroupedPhysicalBlockIdentity) {
        self.statistics_block_identities.push(identity);
    }

    pub(crate) fn record_direct_target_block_decode(
        &mut self,
        identity: GroupedPhysicalBlockIdentity,
        values_decoded: u64,
        block_bytes: u64,
        elapsed_us: u64,
    ) {
        self.blocks_considered = self.blocks_considered.saturating_add(1);
        self.blocks_decoded = self.blocks_decoded.saturating_add(1);
        self.values_decoded = self.values_decoded.saturating_add(values_decoded);
        self.block_bytes = self.block_bytes.saturating_add(block_bytes);
        self.block_read_decode_us = self.block_read_decode_us.saturating_add(elapsed_us);
        self.physical_target_blocks_decoded = self.physical_target_blocks_decoded.saturating_add(1);
        self.physical_target_values_decoded = self
            .physical_target_values_decoded
            .saturating_add(values_decoded);
        self.decoded_block_identities.push(identity);
    }

    pub fn physical_decode_accounting_matches(&self) -> bool {
        self.blocks_decoded
            == self
                .physical_target_blocks_decoded
                .saturating_add(self.physical_visibility_blocks_decoded)
                .saturating_add(self.physical_row_domain_precheck_blocks_decoded)
                .saturating_add(self.physical_row_domain_fallback_blocks_decoded)
    }

    pub fn physical_identity_accounting_matches(&self) -> bool {
        self.decoded_block_identities.len() as u64 == self.blocks_decoded
    }

    pub fn raw_point_accounting_matches(&self, raw_points: u64) -> bool {
        raw_points
            == self
                .target_values_consumed
                .saturating_add(self.row_domain_logical_rows_consumed)
    }

    pub(crate) fn record_eligibility(&mut self, reason: GroupedBlockEligibility) {
        self.record_eligibility_n(reason, 1);
    }

    pub(crate) fn record_eligibility_n(&mut self, reason: GroupedBlockEligibility, count: u64) {
        let counter = match reason {
            GroupedBlockEligibility::StatisticsUsed => &mut self.eligibility_stats_used,
            GroupedBlockEligibility::PartialQueryRange => {
                &mut self.eligibility_reject_partial_query_range
            }
            GroupedBlockEligibility::CrossBucket => &mut self.eligibility_reject_cross_bucket,
            GroupedBlockEligibility::Overlap => &mut self.eligibility_reject_overlap,
            GroupedBlockEligibility::MissingStatistics => {
                &mut self.eligibility_reject_missing_statistics
            }
            GroupedBlockEligibility::UnsupportedStatistics => {
                &mut self.eligibility_reject_unsupported_statistics
            }
            GroupedBlockEligibility::FloatNan => &mut self.eligibility_reject_float_nan,
            GroupedBlockEligibility::FloatSignedZero => {
                &mut self.eligibility_reject_float_signed_zero
            }
            GroupedBlockEligibility::FloatZeroCrossing => {
                &mut self.eligibility_reject_float_zero_crossing
            }
            GroupedBlockEligibility::FloatRequestFallback => {
                &mut self.eligibility_reject_float_request_fallback
            }
            GroupedBlockEligibility::Cache => &mut self.eligibility_reject_cache,
            GroupedBlockEligibility::Vlog => &mut self.eligibility_reject_vlog,
            GroupedBlockEligibility::Tombstone => &mut self.eligibility_reject_tombstone,
            GroupedBlockEligibility::ResidualFilter => &mut self.eligibility_reject_residual_filter,
            GroupedBlockEligibility::Other => &mut self.eligibility_reject_other,
        };
        *counter = counter.saturating_add(count);
    }

    pub fn eligibility_terminal_total(&self) -> u64 {
        self.eligibility_stats_used
            .saturating_add(self.eligibility_reject_partial_query_range)
            .saturating_add(self.eligibility_reject_cross_bucket)
            .saturating_add(self.eligibility_reject_overlap)
            .saturating_add(self.eligibility_reject_missing_statistics)
            .saturating_add(self.eligibility_reject_unsupported_statistics)
            .saturating_add(self.eligibility_reject_float_nan)
            .saturating_add(self.eligibility_reject_float_signed_zero)
            .saturating_add(self.eligibility_reject_float_zero_crossing)
            .saturating_add(self.eligibility_reject_float_request_fallback)
            .saturating_add(self.eligibility_reject_cache)
            .saturating_add(self.eligibility_reject_vlog)
            .saturating_add(self.eligibility_reject_tombstone)
            .saturating_add(self.eligibility_reject_residual_filter)
            .saturating_add(self.eligibility_reject_other)
    }

    pub fn eligibility_accounting_matches(&self, candidate_blocks: u64) -> bool {
        self.eligibility_terminal_total() == candidate_blocks
    }
}

/// Result of one grouped aggregate scan. Groups are sorted by storage key.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct GroupedAggregateResult {
    pub groups: Vec<GroupedAggregateGroup>,
    pub metrics: GroupedAggregateMetrics,
}

impl GroupedAggregateResult {
    /// Merges disjoint shard-local results by logical group key.
    pub fn merge(&mut self, other: Self) -> Result<(), &'static str> {
        let started = Instant::now();
        let mut groups = self
            .groups
            .drain(..)
            .map(|group| (group.key, group.states))
            .collect::<BTreeMap<_, _>>();
        for group in other.groups {
            match groups.entry(group.key) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(group.states);
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let states = entry.get_mut();
                    if states.len() != group.states.len() {
                        return Err(
                            "cannot merge grouped aggregate results with different target counts",
                        );
                    }
                    for (state, other) in states.iter_mut().zip(group.states) {
                        state.merge(other)?;
                    }
                }
            }
        }
        self.groups = groups
            .into_iter()
            .map(|(key, states)| GroupedAggregateGroup { key, states })
            .collect();
        self.metrics.merge(other.metrics);
        self.metrics.aggregate_merge_us = self
            .metrics
            .aggregate_merge_us
            .saturating_add(started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64);
        Ok(())
    }
}

impl StorageScan {
    pub fn validate(&self) -> Result<(), StorageProjectionError> {
        self.projection.validate()
    }

    pub fn ascending(&self) -> bool {
        self.time_order == TimeOrder::Ascending
    }

    pub fn seek_time(&self) -> i64 {
        match self.time_order {
            TimeOrder::Ascending => self.time_range.start_time,
            TimeOrder::Descending => self.time_range.end_time,
        }
    }

    pub fn returned_aux_count(&self) -> usize {
        debug_assert!(self.projection.validate().is_ok());
        self.projection.returned_columns.len()
            - usize::from(self.projection.primary_column.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_range_is_inclusive_and_supports_full_storage_range() {
        let range = TimeRange::new(10, 20);
        assert!(range.contains(10));
        assert!(range.contains(20));
        assert!(!range.contains(9));
        assert!(!range.contains(21));

        assert_eq!(
            TimeRange::all(),
            TimeRange::new(MIN_NANO_TIME_I64, MAX_NANO_TIME_I64)
        );
    }

    #[test]
    fn float_aggregate_state_starts_sum_at_positive_zero() {
        let mut state = AggregateState::empty(crate::models::FLOAT);
        state.add_float(-0.0);
        let Some(AggregateValues::Float { sum, min, max }) = state.values else {
            panic!("expected float aggregate state")
        };
        assert_eq!(sum.to_bits(), 0.0_f64.to_bits());
        assert_eq!(min.to_bits(), (-0.0_f64).to_bits());
        assert_eq!(max.to_bits(), (-0.0_f64).to_bits());
    }

    #[test]
    fn float_aggregate_state_uses_total_order_for_extrema() {
        let positive_nan = f64::from_bits(0x7ff8_0000_0000_0001);
        let negative_nan = f64::from_bits(0xfff8_0000_0000_0002);
        let mut state = AggregateState::empty(crate::models::FLOAT);
        for value in [
            1.0,
            positive_nan,
            2.0,
            0.0,
            -0.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            negative_nan,
        ] {
            state.add_float(value);
        }
        let Some(AggregateValues::Float { min, max, .. }) = state.values else {
            panic!("expected float aggregate state")
        };
        assert_eq!(min.to_bits(), negative_nan.to_bits());
        assert_eq!(max.to_bits(), positive_nan.to_bits());

        let mut zeros = AggregateState::empty(crate::models::FLOAT);
        zeros.add_float(0.0);
        zeros.add_float(-0.0);
        let Some(AggregateValues::Float { min, max, .. }) = zeros.values else {
            panic!("expected float aggregate state")
        };
        assert_eq!(min.to_bits(), (-0.0_f64).to_bits());
        assert_eq!(max.to_bits(), 0.0_f64.to_bits());
    }

    #[test]
    fn grouped_result_merges_target_states_positionally() {
        let key = GroupedAggregateKey {
            window_start: 10,
            tag_values: vec![Some("host_a".to_string())],
        };
        let mut left_integer = AggregateState::empty(crate::models::INTEGER);
        left_integer.add_integer(1);
        let mut left_unsigned = AggregateState::empty(crate::models::UNSIGNED);
        left_unsigned.add_unsigned(2);
        let mut right_integer = AggregateState::empty(crate::models::INTEGER);
        right_integer.add_integer(3);
        let mut right_unsigned = AggregateState::empty(crate::models::UNSIGNED);
        right_unsigned.add_unsigned(4);
        let mut left = GroupedAggregateResult {
            groups: vec![GroupedAggregateGroup {
                key: key.clone(),
                states: vec![left_integer, left_unsigned],
            }],
            metrics: GroupedAggregateMetrics {
                series_matched: 1,
                fields_projected: 2,
                ..Default::default()
            },
        };
        left.merge(GroupedAggregateResult {
            groups: vec![GroupedAggregateGroup {
                key,
                states: vec![right_integer, right_unsigned],
            }],
            metrics: GroupedAggregateMetrics {
                series_matched: 1,
                fields_projected: 2,
                requires_datafusion_fallback: true,
                ..Default::default()
            },
        })
        .unwrap();

        assert_eq!(left.groups.len(), 1);
        assert_eq!(left.groups[0].states.len(), 2);
        assert_eq!(left.groups[0].states[0].count, 2);
        assert_eq!(left.groups[0].states[1].count, 2);
        assert_eq!(left.metrics.series_matched, 2);
        assert_eq!(left.metrics.fields_projected, 2);
        assert!(left.metrics.requires_datafusion_fallback);
    }

    #[test]
    fn grouped_result_rejects_different_target_counts() {
        let group = |count| GroupedAggregateGroup {
            key: GroupedAggregateKey {
                window_start: 0,
                tag_values: vec![],
            },
            states: (0..count)
                .map(|_| AggregateState::empty(crate::models::INTEGER))
                .collect(),
        };
        let mut left = GroupedAggregateResult {
            groups: vec![group(1)],
            ..Default::default()
        };
        let error = left
            .merge(GroupedAggregateResult {
                groups: vec![group(2)],
                ..Default::default()
            })
            .unwrap_err();
        assert!(error.contains("different target counts"));
    }

    #[cfg(feature = "slow-trace")]
    #[test]
    fn grouped_scheduler_distributions_merge_exact_samples() {
        let mut left = GroupedAggregateProfileMetrics {
            permit_immediate_count: 1,
            ..Default::default()
        };
        left.record_scheduler_samples(vec![0, 100], vec![10, 110], vec![1, 9], vec![3, 7]);
        let mut right = GroupedAggregateProfileMetrics::default();
        right.record_scheduler_samples(vec![50, 200], vec![60, 210], vec![2, 20], vec![4, 8]);

        left.merge(right);

        assert_eq!(left.permit_requests, 4);
        assert_eq!(left.permit_immediate_count, 1);
        assert_eq!(left.global_permit_wait_count, 3);
        assert_eq!(left.global_permit_wait_us, 350);
        assert_eq!(left.permit_wait_us_min, 0);
        assert_eq!(left.permit_wait_us_p50, 50);
        assert_eq!(left.permit_wait_us_p95, 200);
        assert_eq!(left.permit_wait_us_max, 200);
        assert_eq!(left.blocking_queue_wait_us_p50, 2);
        assert_eq!(left.blocking_queue_wait_us_p95, 20);
        assert_eq!(left.worker_execution_us_total, 22);
        assert_eq!(left.worker_execution_us_p50, 4);
        assert_eq!(left.worker_execution_us_p95, 8);
    }

    #[cfg(feature = "slow-trace")]
    #[test]
    fn grouped_execution_diagnostics_preserve_monotonic_job_boundaries() {
        use std::time::Duration;

        use crate::runtime::execution::{
            DefaultTokioExecutor, DefaultTokioExecutorOptions, GeneralBlockingTaskClass,
            GeneralBlockingTaskDiagnostics, RuntimeExecutorExt,
        };

        let origin = Instant::now();
        let diagnostics = GroupedExecutionDiagnostics::new(origin);
        diagnostics.record_general_async_submitted(origin + Duration::from_micros(10));
        diagnostics.record_general_async_started(origin + Duration::from_micros(14));
        diagnostics.record_storage_coordinator_started(origin + Duration::from_micros(15));
        let job = diagnostics.record_storage_job_submitted(
            3,
            origin + Duration::from_micros(20),
            origin + Duration::from_micros(23),
            origin + Duration::from_micros(24),
        );
        diagnostics.record_storage_job_started(job, origin + Duration::from_micros(29));
        diagnostics.record_storage_job_finished(job, origin + Duration::from_micros(41));
        let blocking = Arc::new(GeneralBlockingTaskDiagnostics::new(
            GeneralBlockingTaskClass::MeasurementTagKeysPartition,
        ));
        diagnostics.record_general_blocking_task(7, Arc::clone(&blocking));
        let executor = DefaultTokioExecutor::new(DefaultTokioExecutorOptions::new(1, 1, 1, 1))
            .expect("test executor");
        let task = executor
            .spawn_blocking_with_class(
                GeneralBlockingTaskClass::MeasurementTagKeysPartition,
                blocking,
                || 42,
            )
            .expect("schedule diagnostic task");
        assert_eq!(executor.block_on(task).expect("join diagnostic task"), 42);
        diagnostics.record_storage_result_ready(origin + Duration::from_micros(45));

        let snapshot = diagnostics.snapshot();
        assert_eq!(snapshot.general_async_submitted_us, 10);
        assert_eq!(snapshot.general_async_started_us, 14);
        assert_eq!(snapshot.storage_coordinator_started_us, 15);
        assert_eq!(snapshot.storage_result_ready_us, 45);
        assert_eq!(
            snapshot.storage_jobs,
            vec![GroupedStorageJobTiming {
                partition: 3,
                permit_requested_us: 20,
                permit_acquired_us: 23,
                blocking_submitted_us: 24,
                blocking_started_us: 29,
                blocking_finished_us: 41,
            }]
        );
        assert_eq!(snapshot.general_blocking_tasks.len(), 1);
        let blocking = snapshot.general_blocking_tasks[0];
        assert_eq!(
            blocking.task_class,
            GeneralBlockingTaskClass::MeasurementTagKeysPartition
        );
        assert_eq!(blocking.partition, 7);
        assert!(blocking.blocking_started_us >= blocking.blocking_submitted_us);
        assert!(blocking.blocking_finished_us >= blocking.blocking_started_us);
        assert_eq!(blocking.submit_state.submitted_total, 1);
        assert_eq!(blocking.start_state.started_total, 1);
        assert_eq!(blocking.finish_state.completed_total, 1);
        assert_eq!(blocking.finish_state.active_current, 0);
        assert_eq!(blocking.finish_state.queued_current, 0);
    }
}
