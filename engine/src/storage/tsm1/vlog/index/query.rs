//! Visibility-aware aggregation over immutable trees and a captured cache view.

#[cfg(test)]
#[path = "pruning_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "query_error_tests.rs"]
mod error_tests;

#[cfg(test)]
#[path = "review_tests.rs"]
mod review_tests;

use super::compact::SegmentSource;
use super::page_group::{Column, ColumnValue, SelectedRecord};
use super::projected_batch::SelectedBatch;
use super::store::IndexStore;
use super::{AggScalar, AggSum, BlockLoc, LEVEL_BLOCK, Level, Result, SeqNo, invalid};
use crate::common::slow_trace;
use crate::models::{FLOAT, FieldType, INTEGER, STRING, UNSIGNED, points::Value};
use crate::query::aggregate_windows::AggregateWindows;
#[cfg(test)]
use crate::query::storage_ir::AggregateResult;
use crate::query::storage_ir::{
    AggregateNumber, AggregatePoint, AggregatePointValue, AggregateProjection,
    AggregateScanMetrics, AggregateState, AggregateWideSum, ProjectedAggregateResult,
    ProjectedAggregateState, ProjectedAggregateWindowsResult, TimeRange,
};
use crate::storage::tsm1::vlog::aggregate_reader::{DecodedBlock, QueryBlockReader, MAX_PLANNED_BLOCKS};
use crate::storage::tsm1::vlog::reader::{Error as VLogError, VLogReader};
use std::{collections::BTreeMap, sync::Arc};

/// Process-immutable controls for isolated benchmark containers only.
/// Normal builds cannot change query behavior through these environment values.
#[derive(Clone, Copy)]
struct BenchmarkQueryPolicy {
    leaf_only: bool,
    summaries: bool,
    extrema: bool,
}

fn benchmark_query_policy() -> BenchmarkQueryPolicy {
    #[cfg(feature = "benchmarking")]
    {
        static POLICY: std::sync::OnceLock<BenchmarkQueryPolicy> = std::sync::OnceLock::new();
        *POLICY.get_or_init(|| {
            let level = std::env::var("REEFS_BENCH_SUMMARY_LEVEL").unwrap_or_default();
            BenchmarkQueryPolicy {
                leaf_only: matches!(level.as_str(), "leaf" | "raw"),
                summaries: level != "raw",
                extrema: std::env::var("REEFS_BENCH_EXTREMA").as_deref() != Ok("off"),
            }
        })
    }
    #[cfg(not(feature = "benchmarking"))]
    {
        BenchmarkQueryPolicy {
            leaf_only: false,
            summaries: true,
            extrema: true,
        }
    }
}

fn extrema_enabled(projection: AggregateProjection) -> bool {
    let policy = benchmark_query_policy();
    projection.extrema_only() && policy.extrema && !policy.leaf_only
}

fn intersects(a: (i64, i64), b: (i64, i64)) -> bool {
    a.0 <= b.1 && b.0 <= a.1
}

fn point(value: AggScalar) -> Result<AggregatePointValue> {
    Ok(match value {
        AggScalar::Int(v) => AggregatePointValue::Integer(v),
        AggScalar::Uint(v) => AggregatePointValue::Unsigned(v),
        AggScalar::Float(v) => AggregatePointValue::Float(v),
        AggScalar::Bool(v) => AggregatePointValue::Boolean(v),
        AggScalar::None => return Err(invalid("string endpoints require raw values")),
    })
}

fn columns(typ: FieldType, projection: AggregateProjection) -> Result<Vec<Column>> {
    projection.validate_field_type(typ).map_err(invalid)?;
    let mut columns = Vec::new();
    for (needed, column) in [
        (projection.count, Column::Count),
        (projection.sum, Column::Sum),
        (projection.min, Column::Min),
        (projection.max, Column::Max),
        (extrema_enabled(projection) && projection.min, Column::MinAt),
        (extrema_enabled(projection) && projection.max, Column::MaxAt),
        (projection.first && typ != STRING, Column::First),
        (projection.last && typ != STRING, Column::Last),
    ] {
        if needed {
            columns.push(column);
        }
    }
    Ok(columns)
}

fn count(record: &SelectedRecord) -> Result<u64> {
    match record.values[Column::Count as usize] {
        Some(ColumnValue::Count(count)) => Ok(count),
        _ => Err(invalid("aggregate projection requires count")),
    }
}

fn endpoint(record: &SelectedRecord, col: Column, time: i64) -> Result<AggregatePoint> {
    let Some(ColumnValue::Scalar(value)) = record.values[col as usize] else {
        return Err(invalid("aggregate projection requires endpoint"));
    };
    Ok(AggregatePoint {
        time,
        value: point(value)?,
    })
}

fn number(record: &SelectedRecord, col: Column, typ: FieldType) -> Result<AggregateNumber> {
    use ColumnValue::{Scalar, Sum};
    Ok(match (typ, record.values[col as usize]) {
        (INTEGER, Some(Sum(AggSum::Int(v)))) => AggregateNumber::Integer(v as i64),
        (UNSIGNED, Some(Sum(AggSum::Uint(v)))) => AggregateNumber::Unsigned(v as u64),
        (FLOAT, Some(Sum(AggSum::Float(v)))) => AggregateNumber::Float(v),
        (INTEGER, Some(Scalar(AggScalar::Int(v)))) => AggregateNumber::Integer(v),
        (UNSIGNED, Some(Scalar(AggScalar::Uint(v)))) => AggregateNumber::Unsigned(v),
        (FLOAT, Some(Scalar(AggScalar::Float(v)))) => AggregateNumber::Float(v),
        _ => {
            return Err(invalid(
                "aggregate projection requires a matching numeric column",
            ));
        }
    })
}

fn state(
    record: &SelectedRecord,
    typ: FieldType,
    projection: AggregateProjection,
) -> Result<ProjectedAggregateState> {
    Ok(ProjectedAggregateState {
        field_type: typ,
        projection,
        count: projection.count.then(|| count(record)).transpose()?,
        wide_sum: if projection.sum {
            match record.values[Column::Sum as usize] {
                Some(ColumnValue::Sum(AggSum::Int(value))) => Some(AggregateWideSum::Integer(value)),
                Some(ColumnValue::Sum(AggSum::Uint(value))) => Some(AggregateWideSum::Unsigned(value)),
                _ => None,
            }
        } else {
            None
        },
        sum: projection
            .sum
            .then(|| number(record, Column::Sum, typ))
            .transpose()?,
        min: projection
            .min
            .then(|| number(record, Column::Min, typ))
            .transpose()?,
        max: projection
            .max
            .then(|| number(record, Column::Max, typ))
            .transpose()?,
        first: projection
            .first
            .then(|| endpoint(record, Column::First, record.pos.start))
            .transpose()?,
        last: projection
            .last
            .then(|| endpoint(record, Column::Last, record.pos.end))
            .transpose()?,
    })
}
fn add_value(state: &mut AggregateState, value: Value, endpoints: bool) {
    match value {
        Value::IntegerValue(v) if endpoints => state.add_integer_at(v.unix_nano, v.value),
        Value::IntegerValue(v) => state.add_integer(v.value),
        Value::UnsignedValue(v) if endpoints => state.add_unsigned_at(v.unix_nano, v.value),
        Value::UnsignedValue(v) => state.add_unsigned(v.value),
        Value::FloatValue(v) if endpoints => state.add_float_at(v.unix_nano, v.value),
        Value::FloatValue(v) => state.add_float(v.value),
        Value::BoolValue(v) if endpoints => state.add_boolean_at(v.unix_nano, v.value),
        Value::StringValue(v) if endpoints => state.add_string_at(v.unix_nano, v.value),
        Value::BoolValue(_) | Value::StringValue(_) => state.add_count(1),
        Value::EmptyValue(_) => unreachable!("validated aggregate value"),
    }
}

fn add_projected_value(state: &mut ProjectedAggregateState, value: Value) -> Result<()> {
    state.add_value(value).map_err(invalid)
}

struct RawLeaf {
    record: SelectedRecord,
    requires_merge: bool,
}

impl RawLeaf {
    fn read_error(&self, operation: &'static str, source: VLogError) -> super::Error {
        self.record.pos.read_error(operation, source)
    }
}

/// Cache values always win, including over the largest valid disk identity.
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
enum ValueVersion {
    Disk(u32),
    Cache,
}

type PendingValues = BTreeMap<i64, (ValueVersion, Value)>;

fn find_vlog_reader(vlogs: &[Arc<VLogReader>], loc: BlockLoc) -> Result<&Arc<VLogReader>> {
    let lookup_started = slow_trace::now();
    #[cfg(feature = "slow-trace")]
    let mut reader_comparisons = 0;
    let reader = vlogs
        .iter()
        .find(|f| {
            #[cfg(feature = "slow-trace")]
            {
                reader_comparisons += 1;
            }
            f.generation == loc.generation() && f.seq == loc.seq()
        })
        .ok_or(invalid("aggregate references missing VLog"))?;
    slow_trace::log_query_stage(
        "query_vlog_reader_lookup",
        lookup_started,
        "raw block reader",
    );
    #[cfg(feature = "slow-trace")]
    slow_trace::profile_work(
        "query_vlog_reader_lookup",
        "reader comparisons",
        reader_comparisons,
    );
    Ok(reader)
}

struct Walk<'a> {
    sources: &'a [SegmentSource<'a>],
    windows: AggregateWindows<'a>,
    cache_times: &'a [i64],
    endpoints: bool,
    typ: FieldType,
    projection: AggregateProjection,
    columns: Vec<Column>,
    leaf_columns: Vec<Column>,
    allow_summaries: bool,
    source_ranges: super::conflict_ranges::SourceRanges,
    result: ProjectedAggregateWindowsResult,
    deletes: Vec<super::tombstone::TombstoneView>,
    prior_states: Option<&'a [ProjectedAggregateState]>,
    group_cache: super::caches::QueryGroupCache,
}

impl Walk<'_> {
    fn summaries_available(&self, source: usize, level: Level) -> bool {
        let policy = benchmark_query_policy();
        policy.summaries
            && self.allow_summaries
            && !(self.endpoints && self.typ == STRING)
            && (!policy.leaf_only || level == LEVEL_BLOCK)
    }

    fn seed_batches(&mut self, source: usize, batches: &[SelectedBatch]) -> Result<()> {
        if extrema_enabled(self.projection) {
            for batch in batches {
                self.seed_extrema(source, &batch.records().collect::<Vec<_>>())?;
            }
        }
        Ok(())
    }

    fn merge_batch(
        &mut self,
        source: usize,
        batch: &mut SelectedBatch,
        limit: Option<i64>,
        window: Option<usize>,
    ) -> Result<bool> {
        if !self.summaries_available(source, batch.group.level)
            || self.endpoints
            || extrema_enabled(self.projection)
        {
            return Ok(false);
        }
        let start = batch.rows.start;
        let Some(window) = window else {
            return Ok(false);
        };
        let mut end = start;
        for pos in &batch.group.positions[batch.rows.clone()] {
            if limit.is_some_and(|time| pos.start >= time)
                || pos.flags != 0
                || self.windows.containing(pos.start, pos.end) != Some(window)
                || self.has_conflict(source, pos.start, pos.end)
                || self.deletes[source].coverage(pos.start, pos.end)?.0
            {
                break;
            }
            end += 1;
        }
        if end - start < 2 {
            return Ok(false);
        }
        batch
            .group
            .merge_numeric(start..end, &mut self.result.states[window])?;
        for pos in &batch.group.positions[start..end] {
            slow_trace::profile_index_node(
                batch.group.level,
                pos.start,
                pos.end,
                true,
                true,
                false,
            );
        }
        let count = (end - start) as u64;
        self.result.metrics.index_records += count;
        self.result.metrics.index_levels = self.result.metrics.index_levels.max(batch.group.level);
        if batch.group.level == LEVEL_BLOCK {
            self.result.metrics.statistics_blocks += count;
        }
        batch.rows.start = end;
        Ok(true)
    }

    fn has_conflict(&self, source: usize, start: i64, end: i64) -> bool {
        self.cache_times
            .get(self.cache_times.partition_point(|time| *time < start))
            .is_some_and(|time| *time <= end)
            || super::conflict_ranges::overlaps(&self.source_ranges, source, start, end)
    }

    /// Seed only already decoded, clean records; retain chronological raw merging.
    fn seed_extrema(&mut self, source: usize, records: &[SelectedRecord]) -> Result<()> {
        if !extrema_enabled(self.projection) {
            return Ok(());
        }
        let started = slow_trace::now();
        for record in records {
            let pos = record.pos;
            if pos.flags != 0
                || !self.windows.intersects(pos.start, pos.end)
                || self.has_conflict(source, pos.start, pos.end)
                || self.deletes[source].coverage(pos.start, pos.end)?.0
            {
                continue;
            }
            for (needed, value_column, time_column) in [
                (self.projection.min, Column::Min, Column::MinAt),
                (self.projection.max, Column::Max, Column::MaxAt),
            ] {
                if !needed {
                    continue;
                }
                let Some(ColumnValue::Time(time)) = record.values[time_column as usize] else {
                    return Err(invalid("extrema witness timestamp is missing"));
                };
                if !(pos.start..=pos.end).contains(&time) {
                    return Err(invalid("extrema witness timestamp is outside its record"));
                }
                if let Some(window) = self.windows.for_time(time) {
                    self.result.states[window]
                        .add_extrema_witness(number(record, value_column, self.typ)?)
                        .map_err(invalid)?;
                    slow_trace::profile_extrema_witness();
                }
            }
        }
        slow_trace::log_query_stage("query_extrema_witness", started, "visible index extrema");
        Ok(())
    }

    fn dominated(&self, record: &SelectedRecord) -> Result<bool> {
        let min = self
            .projection
            .min
            .then(|| number(record, Column::Min, self.typ))
            .transpose()?;
        let max = self
            .projection
            .max
            .then(|| number(record, Column::Max, self.typ))
            .transpose()?;
        for window in self.windows.intersecting(record.pos.start, record.pos.end) {
            if !self.result.states[window]
                .dominates_extrema(min, max, self.prior_states.map(|states| &states[window]))
                .map_err(invalid)?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn visit(
        &mut self,
        source: usize,
        record: SelectedRecord,
        pending: &mut Vec<SelectedBatch>,
        window: Option<usize>,
    ) -> Result<Option<RawLeaf>> {
        let pos = record.pos;
        if !self.windows.intersects(pos.start, pos.end) {
            return Ok(None);
        }
        let (deleted, covered) = self.deletes[source].coverage(pos.start, pos.end)?;
        if covered {
            return Ok(None);
        }
        let conflict = deleted || self.has_conflict(source, pos.start, pos.end);
        let full = window.is_some();
        let summary = full
            && self.summaries_available(source, record.level)
            && !conflict
            && pos.flags == 0;
        slow_trace::profile_index_node(record.level, pos.start, pos.end, full, summary, conflict);
        if record.level == LEVEL_BLOCK {
            self.result.metrics.boundary_blocks += u64::from(!full);
        }
        if !full && !conflict && pos.flags == 0
            && extrema_enabled(self.projection) {
            let started = slow_trace::now();
            let dominated = self.dominated(&record)?;
            slow_trace::log_query_stage("query_extrema_pruning", started, "partial index bounds");
            if dominated {
                slow_trace::profile_extrema_pruned(record.level == LEVEL_BLOCK, pos.block_size);
                return Ok(None);
            }
        }
        if summary {
            let started = slow_trace::now();
            self.result.states[window.unwrap()]
                .merge(state(&record, self.typ, self.projection)?)
                .map_err(invalid)?;
            slow_trace::log_query_stage("query_index_summary_merge", started, "selected summary");
            self.result.metrics.index_records += 1;
            self.result.metrics.index_levels = self.result.metrics.index_levels.max(record.level);
            self.result.metrics.statistics_blocks += u64::from(record.level == LEVEL_BLOCK);
        } else if record.level == LEVEL_BLOCK {
            self.result.metrics.overlap_raw_blocks += u64::from(conflict);
            return Ok(Some(RawLeaf {
                record,
                requires_merge: conflict || pos.flags != 0,
            }));
        } else {
            let s = &self.sources[source];
            let started = slow_trace::now();
            let columns = if record.level - 1 == LEVEL_BLOCK {
                &self.leaf_columns
            } else {
                &self.columns
            };
            let children = s.file.read_level_batches(
                &s.entry,
                &s.header,
                record.level - 1,
                pos.start,
                pos.end,
                columns,
                &mut self.result.index_reads,
                &mut self.group_cache,
            )?;
            if children.iter().map(SelectedBatch::len).sum::<usize>() != pos.child_count as usize
                || children
                    .first()
                    .is_none_or(|c| c.first().start != pos.start)
                || children.last().is_none_or(|c| c.last().end != pos.end)
            {
                return Err(invalid("aggregate children do not cover their parent"));
            }
            self.seed_batches(source, &children)?;
            slow_trace::log_query_stage("query_index_level", started, "aggregate descent");
            for child in children.into_iter().rev() {
                pending.push(child);
            }
        }
        Ok(None)
    }

    fn root_cursors(&mut self) -> Result<Vec<Vec<SelectedBatch>>> {
        let sources = self.sources;
        let mut cursors = Vec::with_capacity(sources.len());
        for source in sources {
            self.result.metrics.candidate_blocks += u64::from(
                source
                    .header
                    .level(LEVEL_BLOCK)
                    .ok_or(invalid("field has no leaf level"))?
                    .count,
            );
            let roots_started = slow_trace::now();
            let level = source.root_level()?;
            let mut columns = if level == LEVEL_BLOCK {
                self.leaf_columns.clone()
            } else {
                self.columns.clone()
            };
            // Mixed-sign floating sums cannot safely compose block subtotals.
            // Inspect bounded roots before choosing any summaries for this key.
            if self.typ == FLOAT && self.projection.sum {
                for column in [Column::Min, Column::Max] {
                    if !columns.contains(&column) {
                        columns.push(column);
                    }
                }
            }
            let mut roots = source.file.read_level_batches(
                &source.entry,
                &source.header,
                level,
                i64::MIN,
                i64::MAX,
                &columns,
                &mut self.result.index_reads,
                &mut self.group_cache,
            )?;
            self.source_ranges.push(roots.iter().flat_map(|batch| {
                batch.group.positions[batch.rows.clone()].iter().map(|pos| (pos.start, pos.end))
            }).collect());
            slow_trace::log_query_stage("query_index_roots", roots_started, "aggregate roots");
            roots.reverse();
            cursors.push(roots);
        }
        self.refine_conflict_ranges()?;
        // All source ranges must be available before visibility-based seeding.
        for (source, roots) in cursors.iter().enumerate() {
            self.seed_batches(source, roots)?;
        }
        Ok(cursors)
    }

    fn refine_conflict_ranges(&mut self) -> Result<()> {
        let started = slow_trace::now();
        let range = self.windows.envelope();
        super::conflict_ranges::refine(self.sources, &mut self.source_ranges,
            Some((range.start_time, range.end_time)), |source| {
            let tree = &self.sources[source];
            let batches = tree.file.read_level_batches(&tree.entry, &tree.header, LEVEL_BLOCK,
                i64::MIN, i64::MAX, &[], &mut self.result.index_reads, &mut self.group_cache)?;
            Ok(batches.iter().flat_map(|batch| {
                batch.group.positions[batch.rows.clone()].iter().map(|pos| (pos.start, pos.end))
            }).collect())
        })?;
        slow_trace::log_query_stage("query_conflict_ranges", started, "bounded leaf position refinement");
        Ok(())
    }

    fn guard_float_summaries(&mut self, roots: &[Vec<SelectedBatch>], cache: &[Value]) -> Result<()> {
        if self.typ != FLOAT || !self.projection.sum {
            return Ok(());
        }
        let started = slow_trace::now();
        let (mut positive, mut negative, mut finite) = (false, false, true);
        let mut observe = |value: f64| {
            positive |= value > 0.0;
            negative |= value < 0.0;
            finite &= value.is_finite();
        };
        for record in roots.iter().flatten().flat_map(SelectedBatch::records) {
            if self.windows.intersects(record.pos.start, record.pos.end) {
                for column in [Column::Min, Column::Max, Column::Sum] {
                    let AggregateNumber::Float(value) = number(&record, column, FLOAT)? else {
                        unreachable!("validated floating summary");
                    };
                    observe(value);
                }
            }
        }
        for value in cache {
            if self.windows.for_time(value.unix_nano()).is_some() {
                if let Value::FloatValue(value) = value {
                    observe(value.value);
                }
            }
        }
        self.allow_summaries = finite && !(positive && negative);
        slow_trace::profile_work("float_summary_guard", "checked_keys", 1);
        slow_trace::profile_work("float_summary_guard", if !finite {
            "nonfinite_fallback_keys"
        } else if positive && negative {
            "mixed_sign_fallback_keys"
        } else {
            "summary_eligible_keys"
        }, 1);
        slow_trace::log_query_stage("query_float_summary_guard", started, "root and cache bounds");
        Ok(())
    }

    fn execute(
        mut self,
        vlogs: &[Arc<VLogReader>],
        cache: Vec<Value>,
        block_reader: &mut QueryBlockReader,
    ) -> Result<ProjectedAggregateWindowsResult> {
        let mut cursors = self.root_cursors()?;
        self.guard_float_summaries(&cursors, &cache)?;
        let mut pending: PendingValues = cache
            .into_iter()
            .filter(|v| self.windows.for_time(v.unix_nano()).is_some())
            .map(|v| (v.unix_nano(), (ValueVersion::Cache, v)))
            .collect();
        // Drain timestamps before the next block starts. Memory tracks concurrent
        // overlapping blocks and the captured cache, rather than the entire scan.
        // Each source keeps only its DFS frontier (at most fan-out times height).
        // Merge frontiers by start time instead of collecting every raw leaf.
        while let Some(source) = cursors
            .iter()
            .enumerate()
            .filter_map(|(i, c)| c.last().map(|r| (i, r.first().start)))
            .min_by_key(|(i, time)| (*time, *i))
            .map(|(i, _)| i)
        {
            let first = cursors[source].last().unwrap().first();
            let window = self.windows.containing(first.start, first.end);
            let limit = if window.is_some() && cursors[source].last().unwrap().len() >= 2 {
                cursors
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != source)
                    .filter_map(|(_, c)| c.last().map(|b| b.first().start))
                    .min()
            } else {
                None
            };
            let batch = cursors[source].last_mut().unwrap();
            let merged = self.merge_batch(source, batch, limit, window)?;
            if merged {
                if batch.len() == 0 {
                    cursors[source].pop();
                }
                continue;
            }
            let record = batch.pop_first();
            if batch.len() == 0 {
                cursors[source].pop();
            }
            let Some(raw) = self.visit(source, record, &mut cursors[source], window)? else {
                continue;
            };
            let reduction_started = slow_trace::now();
            while pending
                .first_key_value()
                .is_some_and(|(t, _)| *t < raw.record.pos.start)
            {
                let (_, (_, value)) = pending.pop_first().unwrap();
                self.add_value(value)?;
            }
            slow_trace::log_query_stage(
                "query_raw_aggregation",
                reduction_started,
                "drain raw points",
            );
            let reader = find_vlog_reader(vlogs, raw.record.pos.block)?;
            self.prefetch_raw(source, &raw, &cursors[source], reader, block_reader)?;
            self.reduce_raw_leaf(source, raw, reader, block_reader, &mut pending)?;
        }
        let reduction_started = slow_trace::now();
        for (_, (_, value)) in pending {
            self.add_value(value)?;
        }
        slow_trace::log_query_stage(
            "query_raw_aggregation",
            reduction_started,
            "final raw points",
        );
        #[cfg(debug_assertions)]
        for state in &self.result.states {
            state.validate().map_err(invalid)?;
        }
        Ok(self.result)
    }

    fn prefetch_raw(
        &mut self,
        source: usize,
        raw: &RawLeaf,
        frontier: &[SelectedBatch],
        reader: &Arc<VLogReader>,
        block_reader: &mut QueryBlockReader,
    ) -> Result<()> {
        let record = &raw.record;
        let loc = record.pos.block;
        if !self.projection.extrema_only()
            && !raw.requires_merge
            && block_reader.can_prefetch_blocks(
                reader, (u64::from(loc.offset), u64::from(record.pos.block_size)),
            )
        {
            let planning_started = slow_trace::now();
            #[cfg(feature = "slow-trace")]
            let mut inspected = 0;
            let mut candidates = Vec::new();
            for pos in frontier
                .iter()
                .rev()
                .filter(|batch| batch.group.level == LEVEL_BLOCK)
                .flat_map(SelectedBatch::positions)
            {
                #[cfg(feature = "slow-trace")]
                {
                    inspected += 1;
                }
                if pos.block.file_no != loc.file_no
                    || pos.flags != 0
                    || !self.windows.intersects(pos.start, pos.end)
                    || (self.windows.containing(pos.start, pos.end).is_some()
                        && self.summaries_available(source, LEVEL_BLOCK))
                    || self.has_conflict(source, pos.start, pos.end)
                    || self.deletes[source].coverage(pos.start, pos.end)?.0
                {
                    continue;
                }
                candidates.push((u64::from(pos.block.offset), u64::from(pos.block_size)));
                if candidates.len() == MAX_PLANNED_BLOCKS - 1 {
                    break;
                }
            }
            slow_trace::log_query_stage(
                "query_vlog_prefetch_frontier",
                planning_started,
                "required raw block candidates",
            );
            #[cfg(feature = "slow-trace")]
            {
                slow_trace::profile_work(
                    "query_vlog_prefetch_frontier",
                    "positions inspected",
                    inspected,
                );
                slow_trace::profile_work(
                    "query_vlog_prefetch_frontier",
                    "eligible candidates",
                    candidates.len() as u64,
                );
            }
            // Inspect the existing frontier only. Do not advance the walk or
            // merge future summaries: floating-point addition order stays intact.
            block_reader.prefetch_blocks(
                reader,
                (u64::from(loc.offset), u64::from(record.pos.block_size)),
                &mut candidates,
            ).map_err(|source| raw.read_error("prefetch", source))?;
        }
        Ok(())
    }

    fn expected_count(&mut self, source: usize, record: &SelectedRecord) -> Result<u64> {
        // Only raw fallback needs the count for block/index consistency.
        // SUM/MIN/MAX summaries do not need to fetch the count column.
        if record.values[Column::Count as usize].is_some() {
            count(record)
        } else {
            let count_started = slow_trace::now();
            let s = &self.sources[source];
            let counts = s.file.read_level_projected_cached(
                &s.entry,
                &s.header,
                LEVEL_BLOCK,
                record.pos.start,
                record.pos.end,
                &[Column::Count],
                &mut self.result.index_reads,
                &mut self.group_cache,
            )?;
            if counts.len() != 1 || counts[0].pos != record.pos {
                return Err(invalid("aggregate leaf changed during count lookup"));
            }
            slow_trace::log_query_stage(
                "query_boundary_count_lookup",
                count_started,
                "aggregate raw validation",
            );
            count(&counts[0])
        }
    }

    fn reduce_raw_leaf(
        &mut self,
        source: usize,
        raw: RawLeaf,
        reader: &Arc<VLogReader>,
        block_reader: &mut QueryBlockReader,
        pending: &mut PendingValues,
    ) -> Result<()> {
        let record = &raw.record;
        let loc = record.pos.block;
        let typ = self.typ;
        let started = slow_trace::now();
        if !self.allow_summaries {
            slow_trace::profile_work("float_summary_guard", "fallback_raw_blocks", 1);
        }
        let expected_count = self.expected_count(source, record)?;
        if self.projection.count_only() && !raw.requires_merge {
            return self.reduce_count_leaf(&raw, reader, block_reader, expected_count, started);
        }
        if !raw.requires_merge
            && block_reader
                .reduce_numeric(
                    reader,
                    u64::from(loc.offset),
                    u64::from(record.pos.block_size),
                    typ,
                    self.windows,
                    &mut self.result.states,
                    expected_count,
                    record.pos.start,
                    record.pos.end,
                )
                .map_err(|source| raw.read_error("streaming reduction", source))?
        {
            slow_trace::profile_decoded_block(
                loc.file_no,
                loc.offset,
                record.pos.block_size,
                expected_count as usize,
            );
            slow_trace::log_query_stage(
                "query_block_decode",
                started,
                "checked streaming boundary reduction",
            );
            self.result.metrics.decoded_blocks += 1;
            return Ok(());
        }
        let values = block_reader
            .read(
                reader,
                u64::from(loc.offset),
                u64::from(record.pos.block_size),
                typ,
            )
            .map_err(|source| raw.read_error("value decode", source))?;
        slow_trace::profile_decoded_block(
            loc.file_no,
            loc.offset,
            record.pos.block_size,
            values.len(),
        );
        if !values.valid_range(expected_count, record.pos.start, record.pos.end, typ) {
            return Err(invalid("aggregate VLog block does not match index"));
        }
        slow_trace::log_query_stage(
            "query_block_decode",
            started,
            "aggregate boundary or overlap",
        );
        self.result.metrics.decoded_blocks += 1;
        self.reduce_decoded_leaf(source, raw, values, block_reader, pending)
    }

    fn reduce_count_leaf(
        &mut self,
        raw: &RawLeaf,
        reader: &Arc<VLogReader>,
        block_reader: &mut QueryBlockReader,
        expected_count: u64,
        started: slow_trace::TraceInstant,
    ) -> Result<()> {
        let record = &raw.record;
        let loc = record.pos.block;
        let typ = self.typ;
        let times = block_reader
            .read_timestamps(
                reader,
                u64::from(loc.offset),
                u64::from(record.pos.block_size),
                typ,
            )
            .map_err(|source| raw.read_error("timestamp decode", source))?;
        if times.len() as u64 != expected_count
            || times.first() != Some(&record.pos.start)
            || times.last() != Some(&record.pos.end)
            || !times.windows(2).all(|pair| pair[0] < pair[1])
        {
            return Err(invalid("aggregate VLog timestamps do not match index"));
        }
        slow_trace::profile_decoded_block(
            loc.file_no,
            loc.offset,
            record.pos.block_size,
            times.len(),
        );
        slow_trace::log_query_stage(
            "query_block_decode",
            started,
            "aggregate timestamp-only boundary",
        );
        let reduction_started = slow_trace::now();
        crate::query::numeric_aggregate::count_windows(
            &times,
            typ,
            self.windows,
            &mut self.result.states,
        )
        .map_err(invalid)?;
        slow_trace::log_query_stage(
            "query_count_timestamp_reduction",
            reduction_started,
            "clean count boundary",
        );
        self.result.metrics.decoded_blocks += 1;
        block_reader.recycle_timestamps(times);
        Ok(())
    }

    fn reduce_decoded_leaf(
        &mut self,
        source: usize,
        raw: RawLeaf,
        values: DecodedBlock,
        block_reader: &mut QueryBlockReader,
        pending: &mut PendingValues,
    ) -> Result<()> {
        let record = raw.record;
        let merge_started = slow_trace::now();
        if !raw.requires_merge
            && values
                .reduce_windows(self.windows, &mut self.result.states)
                .map_err(invalid)?
        {
            slow_trace::log_query_stage(
                "query_raw_direct_reduction",
                merge_started,
                "filter and reduce raw points",
            );
            block_reader.recycle(values);
            return Ok(());
        }
        let mut window_cursor = self.windows.ordered_cursor();
        for value in values.into_values() {
            let Some(window) = window_cursor.for_time(value.unix_nano()) else {
                continue;
            };
            if raw.requires_merge && self.deletes[source].removes(value.unix_nano())? {
                continue;
            }
            if !raw.requires_merge {
                // With no overlapping source, cache value, tombstone or
                // dirty flag, the validated boundary can be reduced directly.
                add_projected_value(&mut self.result.states[window], value)?;
                continue;
            }
            let version = ValueVersion::Disk(record.pos.write_version());
            let entry = pending
                .entry(value.unix_nano())
                .or_insert_with(|| (version, value.clone()));
            if entry.0 <= version {
                *entry = (version, value);
            }
        }
        slow_trace::log_query_stage(
            if raw.requires_merge {
                "query_raw_point_merge"
            } else {
                "query_raw_direct_reduction"
            },
            merge_started,
            "filter and reduce raw points",
        );
        Ok(())
    }

    fn add_value(&mut self, value: Value) -> Result<()> {
        if let Some(window) = self.windows.for_time(value.unix_nano()) {
            add_projected_value(&mut self.result.states[window], value)?;
        }
        Ok(())
    }
}

impl IndexStore {
    #[cfg(test)]
    pub(crate) fn aggregate(
        &self,
        seq: SeqNo,
        typ: FieldType,
        range: TimeRange,
        cache: Vec<Value>,
        endpoints: bool,
    ) -> Result<AggregateResult> {
        let result = self.aggregate_projected(
            seq,
            typ,
            range,
            cache,
            AggregateProjection::complete(typ, endpoints),
        )?;
        result.into_complete().map_err(invalid)
    }

    /// Capture cache values before calling this method: a concurrent flush may
    /// then duplicate those values on disk, but cannot create a visibility gap.
    pub(crate) fn aggregate_projected(
        &self,
        seq: SeqNo,
        typ: FieldType,
        range: TimeRange,
        cache: Vec<Value>,
        projection: AggregateProjection,
    ) -> Result<ProjectedAggregateResult> {
        let mut result = self.aggregate_projected_windows(seq, typ, &[range], cache, projection)?;
        Ok(ProjectedAggregateResult {
            state: result.states.remove(0),
            metrics: result.metrics,
            index_reads: result.index_reads,
        })
    }

    /// Walk each field's trees once for all windows. Cache capture still precedes
    /// the file view so concurrent flush cannot create a visibility gap.
    pub(crate) fn aggregate_projected_windows(
        &self,
        seq: SeqNo,
        typ: FieldType,
        ranges: &[TimeRange],
        cache: Vec<Value>,
        projection: AggregateProjection,
    ) -> Result<ProjectedAggregateWindowsResult> {
        let mut reader = QueryBlockReader::new(Default::default());
        self.aggregate_projected_windows_with_reader(
            seq,
            typ,
            ranges,
            cache,
            projection,
            &mut reader,
        )
    }

    pub(crate) fn aggregate_projected_windows_with_reader(
        &self,
        seq: SeqNo,
        typ: FieldType,
        ranges: &[TimeRange],
        cache: Vec<Value>,
        projection: AggregateProjection,
        block_reader: &mut QueryBlockReader,
    ) -> Result<ProjectedAggregateWindowsResult> {
        self.aggregate_projected_windows_with_bounds(
            seq,
            typ,
            ranges,
            cache,
            projection,
            block_reader,
            None,
        )
    }

    /// Prior candidates belong to this query's field/windows, never a global cache.
    pub(crate) fn aggregate_projected_windows_with_bounds(
        &self,
        seq: SeqNo,
        typ: FieldType,
        ranges: &[TimeRange],
        cache: Vec<Value>,
        projection: AggregateProjection,
        block_reader: &mut QueryBlockReader,
        prior_states: Option<&[ProjectedAggregateState]>,
    ) -> Result<ProjectedAggregateWindowsResult> {
        let mut directory_cache = super::caches::QueryDirectoryCache::default();
        self.aggregate_projected_windows_with_bounds_cached(
            seq, typ, ranges, cache, projection, block_reader, prior_states,
            &mut directory_cache,
        )
    }

    pub(crate) fn aggregate_projected_windows_with_bounds_cached(
        &self,
        seq: SeqNo,
        typ: FieldType,
        ranges: &[TimeRange],
        cache: Vec<Value>,
        projection: AggregateProjection,
        block_reader: &mut QueryBlockReader,
        prior_states: Option<&[ProjectedAggregateState]>,
        directory_cache: &mut super::caches::QueryDirectoryCache,
    ) -> Result<ProjectedAggregateWindowsResult> {
        let windows = AggregateWindows::new(ranges).map_err(invalid)?;
        if let Some(prior) = prior_states
            && (!projection.extrema_only()
                || prior.len() != ranges.len()
                || prior
                    .iter()
                    .any(|state| state.field_type != typ || state.projection != projection))
        {
            return Err(invalid("extrema bounds do not match the aggregate request"));
        }
        let range = windows.envelope();
        let endpoints = projection.has_endpoints();
        let started = slow_trace::now();
        let (files, vlogs, native) = self.native_query_view(seq)?;
        let mut sources = Vec::new();
        for file in &files {
            if let Some(entry) = file.lookup_cached(seq, directory_cache)? {
                // Validate directory time/type metadata before using it to prune.
                let header = file.segment_header(&entry)?;
                if entry.typ != typ
                    || !intersects(
                        (entry.min_start, entry.max_end),
                        (range.start_time, range.end_time),
                    )
                {
                    continue;
                }
                if header.level(LEVEL_BLOCK).is_none() {
                    continue;
                }
                sources.push(SegmentSource {
                    file,
                    entry,
                    header,
                });
            }
        }
        slow_trace::log_query_stage("query_index_lookup", started, "aggregate directory");
        let cache_started = slow_trace::now();
        let cache_times = cache
            .iter()
            .map(Value::unix_nano)
            .filter(|time| windows.for_time(*time).is_some())
            .collect::<Vec<_>>();
        slow_trace::profile_work("query_cache_values", "captured_points", cache.len() as u64);
        slow_trace::profile_work("query_cache_values", "in_window_points", cache_times.len() as u64);
        slow_trace::log_query_stage(
            "query_cache_conflict_prepare", cache_started, "visible cache timestamps",
        );
        let columns = columns(typ, projection)?;
        let mut leaf_columns = columns.clone();
        // Multi-window leaves often cross buckets. Co-read their validation
        // count once with the requested statistics, rather than per raw leaf.
        if ranges.len() > 1 && !leaf_columns.contains(&Column::Count) {
            leaf_columns.push(Column::Count);
        }
        let walk = Walk {
            sources: &sources,
            windows,
            cache_times: &cache_times,
            endpoints,
            typ,
            projection,
            columns,
            leaf_columns,
            allow_summaries: true,
            source_ranges: Vec::with_capacity(sources.len()),
            result: ProjectedAggregateWindowsResult {
                states: ranges
                    .iter()
                    .map(|_| ProjectedAggregateState::empty(typ, projection))
                    .collect(),
                metrics: AggregateScanMetrics::default(),
                index_reads: Default::default(),
            },
            deletes: sources.iter().map(|s| native.as_ref().map_or_else(
                || s.file.tombstone_view(seq), |n| n.tombstones(s.file, seq))).collect(),
            prior_states,
            group_cache: Default::default(),
        };
        walk.execute(&vlogs, cache, block_reader)
    }
}
