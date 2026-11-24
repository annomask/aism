//! One prepared aggregate-index execution path for both public result contracts.
//! Callers release the database guard before running the owned scan.
#[cfg(test)]
#[path = "aggregate_execution_review_tests.rs"]
mod review_tests;

use super::vlog::aggregate_reader::QueryBlockReader;
use super::vlog::index::caches::QueryDirectoryCache;
use super::*;
use crate::query::aggregate_windows::AggregateWindows;
use crate::query::storage_ir::{
    AggregateProjection, ProjectedAggregateResult, ProjectedAggregateState,
    ProjectedAggregateWindowsResult, StoragePredicate,
};

#[derive(Clone)]
pub(crate) struct PreparedAggregateScan {
    context: Arc<AggregateIndexContext>,
    index: Arc<Index>,
    sfile: Arc<SeriesFile>,
    cache: Arc<Cache>,
    request: AggregateRequest,
    predicate: Option<StoragePredicate>,
    field_type: FieldType,
    projection: AggregateProjection,
    read_budget_divisor: usize,
}

enum AggregateOutput {
    Complete,
    Requested,
}

#[derive(Clone)]
pub(crate) struct PreparedAggregateWindowsScan {
    fields: Vec<Option<PreparedAggregateScan>>,
    windows: Vec<TimeRange>,
}

impl Engine {
    pub(crate) fn prepare_aggregate_windows_scan_filtered(
        &self,
        requests: Vec<AggregateRequest>,
        windows: Vec<TimeRange>,
        predicate: StoragePredicate,
    ) -> Result<PreparedAggregateWindowsScan> {
        let ranges = AggregateWindows::new(&windows).map_err(|reason| Error::StringErr {
            reason: reason.into(),
        })?;
        let first = requests.first().ok_or_else(|| Error::StringErr {
            reason: "aggregate windows require field requests".into(),
        })?;
        if first.time_range != ranges.envelope()
            || requests.iter().any(|request| {
                request.measurement != first.measurement || request.time_range != first.time_range
            })
        {
            return Err(Error::StringErr {
                reason: "window aggregates must share a measurement and the windows' envelope"
                    .into(),
            });
        }
        let fields = requests
            .into_iter()
            .map(|request| {
                self.prepare_aggregate_scan(
                    request,
                    Some(predicate.clone()),
                    AggregateOutput::Requested,
                )
            })
            .collect::<Result<_>>()?;
        Ok(PreparedAggregateWindowsScan { fields, windows })
    }

    pub(crate) fn prepare_aggregate_complete_scan(
        &self,
        request: AggregateRequest,
    ) -> Result<Option<PreparedAggregateScan>> {
        self.prepare_aggregate_scan(request, None, AggregateOutput::Complete)
    }

    pub(crate) fn prepare_aggregate_projected_scan(
        &self,
        request: AggregateRequest,
    ) -> Result<Option<PreparedAggregateScan>> {
        self.prepare_aggregate_scan(request, None, AggregateOutput::Requested)
    }

    pub(crate) fn prepare_aggregate_projected_scan_filtered(
        &self,
        request: AggregateRequest,
        predicate: StoragePredicate,
    ) -> Result<Option<PreparedAggregateScan>> {
        self.prepare_aggregate_scan(request, Some(predicate), AggregateOutput::Requested)
    }

    fn prepare_aggregate_scan(
        &self,
        request: AggregateRequest,
        predicate: Option<StoragePredicate>,
        output: AggregateOutput,
    ) -> Result<Option<PreparedAggregateScan>> {
        let prepare_started = slow_trace::now();
        let Some(context) = &self.aggregate_index else {
            return Ok(None);
        };
        let Some(field) = self.aggregate_field(&request)? else {
            return Ok(None);
        };
        let requested = AggregateProjection::from_aggregates(&request.aggregates);
        let projection = match output {
            AggregateOutput::Complete => {
                AggregateProjection::complete(field.typ, requested.has_endpoints())
            }
            AggregateOutput::Requested => requested,
        };
        let cache = Arc::clone(&self.aggregate_cache);
        slow_trace::log_query_stage("query_prepare", prepare_started, "aggregate request");
        Ok(Some(PreparedAggregateScan {
            context: Arc::clone(context),
            index: Arc::clone(&self.index),
            sfile: Arc::clone(&self.sfile),
            cache,
            request,
            predicate,
            field_type: field.typ,
            projection,
            read_budget_divisor: 1,
        }))
    }

    #[cfg(test)]
    pub(crate) fn aggregate_projected_scan(
        &self,
        request: AggregateRequest,
    ) -> Result<Option<ProjectedAggregateResult>> {
        if let Some(scan) = self.prepare_aggregate_projected_scan(request.clone())? {
            return scan.run().map(Some);
        }
        let projection = AggregateProjection::from_aggregates(&request.aggregates);
        Ok(self
            .aggregate_scan(request)?
            .map(|r| ProjectedAggregateResult::from_complete(r, projection)))
    }
}

impl PreparedAggregateScan {
    pub(super) fn cross_shard_type(&self) -> FieldType {
        self.field_type
    }

    pub(super) fn cross_shard_series(&self) -> Result<Vec<String>> {
        self.selected_series()
    }

    pub(super) fn cross_shard_leaves(
        &self,
        series: &str,
        scope: TimeRange,
    ) -> Result<Option<Vec<vlog::index::cross_shard::LogicalRecord>>> {
        let key = format!("{series}{KEY_FIELD_SEPARATOR}{}", self.request.field);
        let range = self.request.time_range;
        let start = range.start_time.max(scope.start_time);
        let end = range.end_time.min(scope.end_time);
        if start <= end
            && !self.cache.values_in_range(key.as_bytes(), start, end).0.is_empty()
        {
            return Ok(None);
        }
        let sequence = self.context.sequence(key.as_bytes())?;
        self.context.store.cross_shard_leaves(sequence, scope)
            .map_err(aggregate_index_error)
    }

    pub(super) fn cross_shard_windows(
        &self,
        series: Option<&str>,
        windows: &[TimeRange],
    ) -> Result<ProjectedAggregateWindowsResult> {
        let series = match series {
            Some(series) => vec![series.to_owned()],
            None => self.selected_series()?,
        };
        self.run_windows(
            windows,
            &series,
            &mut QueryBlockReader::new(self.context.query_options.clone()),
            None,
            &mut QueryDirectoryCache::default(),
        )
    }

    pub(crate) fn run_complete(self) -> Result<AggregateResult> {
        self.run()?
            .into_complete()
            .map_err(|reason| Error::StringErr {
                reason: reason.into(),
            })
    }

    fn selected_series(&self) -> Result<Vec<String>> {
        let started = slow_trace::now();
        let index_set =
            index::IndexSet::new(Arc::clone(&self.index), Arc::clone(&self.sfile), None);
        let column = ColumnRef::new(
            &self.request.field,
            ColumnKind::Field(Some(self.field_type)),
        );
        let scan = StorageScan {
            measurement: self.request.measurement.clone(),
            projection: StorageProjection {
                primary_column: Some(column.clone()),
                returned_columns: vec![column],
                row_domain_columns: vec![],
                // Global reductions do not group output by tags. TSI can resolve
                // the predicate without discovering every series' tag names.
                series: SeriesProjection::new(vec![], vec![]),
            },
            time_range: self.request.time_range,
            predicate: self.predicate.clone(),
            time_order: TimeOrder::Ascending,
            output_order: OutputOrder::TimeOrdered,
            execution: Default::default(),
            cancellation: None,
        };
        let plan = EngineScanPlan {
            scan: Arc::new(scan),
            vlog_enabled: true,
        };
        let selection_started = slow_trace::now();
        let tag_sets = index_set
            .tag_sets_for_storage_scan(Arc::clone(&self.sfile), &plan.scan, plan.max_series_n())
            .context(IndexSetSnafu)?;
        slow_trace::log_query_stage(
            "query_series_selection",
            selection_started,
            "aggregate predicate",
        );
        if tag_sets
            .iter()
            .flat_map(|set| &set.storage_filters)
            .any(Option::is_some)
        {
            return Err(Error::StringErr {
                reason: "aggregate index requires a predicate fully resolved by series selection; use a storage scan for row predicates".into(),
            });
        }
        slow_trace::log_query_stage("query_series_lookup", started, "projected aggregate");
        Ok(tag_sets
            .into_iter()
            .flat_map(|set| set.series_keys)
            .collect())
    }

    pub(crate) fn run(self) -> Result<ProjectedAggregateResult> {
        let series = self.selected_series()?;
        let range = self.request.time_range;
        let mut reader = QueryBlockReader::new(self.context.query_options.clone());
        let mut directory_cache = QueryDirectoryCache::default();
        let mut result = self.run_windows(&[range], &series, &mut reader, None, &mut directory_cache)?;
        let state = result.states.remove(0);
        state.validate().map_err(|reason| Error::StringErr {
            reason: reason.into(),
        })?;
        Ok(ProjectedAggregateResult {
            state,
            metrics: result.metrics,
            index_reads: result.index_reads,
        })
    }

    fn run_windows(
        &self,
        windows: &[TimeRange],
        series: &[String],
        reader: &mut QueryBlockReader,
        prior_states: Option<&[ProjectedAggregateState]>,
        directory_cache: &mut QueryDirectoryCache,
    ) -> Result<ProjectedAggregateWindowsResult> {
        let started = slow_trace::now();
        let projection = self.projection;
        let mut result = ProjectedAggregateWindowsResult {
            states: windows
                .iter()
                .map(|_| ProjectedAggregateState::empty(self.field_type, projection))
                .collect(),
            metrics: Default::default(),
            index_reads: Default::default(),
        };
        self.accumulate_windows(windows, series, reader, prior_states, directory_cache, &mut result)?;
        slow_trace::log_query_stage("query_aggregation", started, "projected aggregate");
        Ok(result)
    }

    fn accumulate_windows(
        &self,
        windows: &[TimeRange],
        series: &[String],
        reader: &mut QueryBlockReader,
        prior_states: Option<&[ProjectedAggregateState]>,
        directory_cache: &mut QueryDirectoryCache,
        result: &mut ProjectedAggregateWindowsResult,
    ) -> Result<()> {
        let projection = self.projection;
        for series_key in series {
            let key = format!("{series_key}{KEY_FIELD_SEPARATOR}{}", self.request.field);
            let cache_started = slow_trace::now();
            let range = self.request.time_range;
            let values = self.cache.values_in_range(
                key.as_bytes(), range.start_time, range.end_time,
            ).0;
            slow_trace::log_query_stage("query_cache_lookup", cache_started, "aggregate values");
            let sequence_started = slow_trace::now();
            let sequence = self.context.sequence(key.as_bytes())?;
            slow_trace::log_query_stage(
                "query_field_identity",
                sequence_started,
                "aggregate sequence",
            );
            let part = self
                .context
                .store
                .aggregate_projected_windows_with_bounds_cached(
                    sequence,
                    self.field_type,
                    windows,
                    values,
                    projection,
                    reader,
                    projection
                        .extrema_only()
                        .then_some(prior_states.unwrap_or(&result.states)),
                    directory_cache,
                )
                .map_err(aggregate_index_error)?;
            for (state, part) in result.states.iter_mut().zip(part.states) {
                state.merge(part).map_err(|reason| Error::StringErr {
                    reason: reason.into(),
                })?;
            }
            result.index_reads.merge(part.index_reads);
            result.metrics.merge(part.metrics);
        }
        Ok(())
    }
}

impl PreparedAggregateWindowsScan {
    pub(crate) fn run(self) -> Result<Vec<Option<ProjectedAggregateWindowsResult>>> {
        let results = self.execute()?;
        // Check the completed output once, after all series and worker states merge.
        for result in results.iter().flatten() {
            for state in &result.states {
                state.validate().map_err(|reason| Error::StringErr {
                    reason: reason.into(),
                })?;
            }
        }
        Ok(results)
    }

    fn execute(self) -> Result<Vec<Option<ProjectedAggregateWindowsResult>>> {
        let series = self
            .fields
            .iter()
            .flatten()
            .next()
            .map(PreparedAggregateScan::selected_series)
            .transpose()?
            .unwrap_or_default();
        let Some(field) = self.fields.iter().flatten().next() else {
            return self.run_series(&series);
        };
        let context = Arc::clone(&field.context);
        let options = &context.query_options;
        if options.parallel_tasks == 0
            || series.len() < options.parallel_min_series.max(2)
            || crate::common::slow_trace::profile_active()
        {
            return self.run_series(&series);
        }
        let workers = (options.parallel_tasks.saturating_add(1)).min(series.len());
        let state_bytes = self
            .fields
            .len()
            .checked_mul(self.windows.len())
            .and_then(|n| n.checked_mul(std::mem::size_of::<ProjectedAggregateState>()))
            .and_then(|n| n.checked_mul(workers));
        if state_bytes.is_none_or(|n| n > options.parallel_state_bytes)
            || self
                .fields
                .iter()
                .flatten()
                .any(|field| field.field_type == STRING)
        {
            // String payloads are variable-sized and are not covered by this budget.
            return self.run_series(&series);
        }
        // Admission precedes partitioning: saturation retains one serial scan
        // with its full read budget and cross-series extrema bounds.
        let mut permits = Vec::new();
        for _ in 1..workers {
            let Some(permit) = QueryTaskPermit::try_acquire(&context.query_tasks, options.parallel_tasks) else {
                break;
            };
            permits.push(permit);
        }
        if permits.is_empty() {
            return self.run_series(&series);
        }
        let chunk_size = series.len().div_ceil(1 + permits.len());
        let workers = series.len().div_ceil(chunk_size);
        permits.truncate(workers - 1);
        let mut scan = self.clone();
        // Split the per-query byte budget between admitted workers only.
        for field in scan.fields.iter_mut().flatten() {
            field.read_budget_divisor = workers;
        }
        let scan = Arc::new(scan);
        let mut jobs = Vec::new();
        let mut local = Vec::new();
        let mut permits = permits.into_iter();
        for (index, chunk) in series.chunks(chunk_size).enumerate() {
            if index == 0 {
                local.push((index, chunk.to_vec()));
                continue;
            }
            let permit = permits.next().expect("one permit per admitted worker");
            let owned = chunk.to_vec();
            let job = Arc::new(QueryJob::new(Arc::clone(&scan), owned, permit));
            let task_job = Arc::clone(&job);
            match context.executor.spawn_blocking(move || {
                task_job.execute();
            }) {
                Ok(task) => jobs.push((index, job, Some(task))),
                Err(_) => jobs.push((index, job, None)),
            }
        }
        let mut parts = Vec::new();
        let mut error = None;
        for (index, chunk) in local {
            match scan.run_series(&chunk) {
                Ok(part) => parts.push((index, part)),
                Err(e) => {
                    if error.is_none() {
                        error = Some(e);
                    }
                }
            }
        }
        // Help unstarted work before waiting, including on a one-thread executor.
        // Every job is consumed even if another chunk failed.
        for (index, job, task) in jobs {
            let part = job.finish();
            if let Some(task) = task {
                task.abort();
            }
            match part {
                Ok(part) => parts.push((index, part)),
                Err(e) => {
                    if error.is_none() {
                        error = Some(e);
                    }
                }
            }
        }
        if let Some(error) = error {
            return Err(error);
        }
        parts.sort_by_key(|(index, _)| *index);
        let mut parts = parts.into_iter();
        let (_, mut output) = parts.next().unwrap();
        for (_, part) in parts {
            for (output, part) in output.iter_mut().zip(part) {
                if let (Some(output), Some(part)) = (output, part) {
                    for (state, next) in output.states.iter_mut().zip(part.states) {
                        state.merge(next).map_err(|reason| Error::StringErr {
                            reason: reason.into(),
                        })?;
                    }
                    output.metrics.merge(part.metrics);
                    output.index_reads.merge(part.index_reads);
                }
            }
        }
        Ok(output)
    }

    fn run_series(
        &self,
        series: &[String],
    ) -> Result<Vec<Option<ProjectedAggregateWindowsResult>>> {
        let mut options = self
            .fields
            .iter()
            .flatten()
            .next()
            .map(|f| f.context.query_options.clone())
            .unwrap_or_default();
        options.read_cache_bytes /= self
            .fields
            .iter()
            .flatten()
            .next()
            .map(|f| f.read_budget_divisor)
            .unwrap_or(1);
        options.max_open_files /= self
            .fields
            .iter()
            .flatten()
            .next()
            .map(|f| f.read_budget_divisor)
            .unwrap_or(1);
        let mut reader = QueryBlockReader::new(options);
        let mut directory_cache = QueryDirectoryCache::default();
        let mut results: Vec<_> = self
            .fields
            .iter()
            .map(|field| {
                field.as_ref().map(|field| ProjectedAggregateWindowsResult {
                    states: self
                        .windows
                        .iter()
                        .map(|_| ProjectedAggregateState::empty(field.field_type, field.projection))
                        .collect(),
                    metrics: Default::default(),
                    index_reads: Default::default(),
                })
            })
            .collect();
        // Visit fields of one series together to reuse adjacent physical reads.
        // Each cache capture still precedes that field's publication snapshot.
        for key in series {
            for (field, output) in self.fields.iter().zip(&mut results) {
                if let (Some(field), Some(output)) = (field, output) {
                    field.accumulate_windows(
                        &self.windows,
                        std::slice::from_ref(key),
                        &mut reader,
                        None,
                        &mut directory_cache,
                        output,
                    )?;
                }
            }
        }
        Ok(results)
    }
}

type WindowScanResult = Vec<Option<ProjectedAggregateWindowsResult>>;

struct QueryWork {
    scan: Arc<PreparedAggregateWindowsScan>,
    series: Vec<String>,
    _permit: QueryTaskPermit,
}

struct QueryJob {
    work: parking_lot::Mutex<Option<QueryWork>>,
    result: parking_lot::Mutex<Option<Result<WindowScanResult>>>,
    ready: parking_lot::Condvar,
}

impl QueryJob {
    fn new(
        scan: Arc<PreparedAggregateWindowsScan>,
        series: Vec<String>,
        permit: QueryTaskPermit,
    ) -> Self {
        Self {
            work: parking_lot::Mutex::new(Some(QueryWork {
                scan,
                series,
                _permit: permit,
            })),
            result: parking_lot::Mutex::new(None),
            ready: parking_lot::Condvar::new(),
        }
    }

    fn execute(&self) {
        let work = { self.work.lock().take() };
        let Some(work) = work else {
            return;
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            work.scan.run_series(&work.series)
        }))
        .unwrap_or_else(|_| {
            Err(Error::StringErr {
                reason: "aggregate query worker panicked".into(),
            })
        });
        // Release the task permit and scan handles before waking the owner.
        drop(work);
        {
            *self.result.lock() = Some(result);
        }
        self.ready.notify_all();
    }

    fn finish(&self) -> Result<WindowScanResult> {
        self.execute();
        let mut result = self.result.lock();
        while result.is_none() {
            self.ready.wait(&mut result);
        }
        result.take().unwrap()
    }
}

struct QueryTaskPermit(Arc<std::sync::atomic::AtomicUsize>);
impl QueryTaskPermit {
    fn try_acquire(active: &Arc<std::sync::atomic::AtomicUsize>, limit: usize) -> Option<Self> {
        use std::sync::atomic::Ordering;
        active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < limit).then(|| n + 1)
            })
            .ok()
            .map(|_| Self(Arc::clone(active)))
    }
}
impl Drop for QueryTaskPermit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

#[cfg(test)]
mod execution_tests {
    use super::*;

    #[test]
    fn aggregate_index_extrema_bounds_cross_series_and_preserve_other_projections() {
        use crate::query::storage_ir::{AggregateKind, AggregateNumber, MeasurementRef};
        let mut engine = super::super::tests::Engine::with_aggregate_index();
        engine
            .engine
            .measurement_fields(b"cpu")
            .create_field_if_not_exists(b"value", crate::models::INTEGER)
            .unwrap();
        engine
            .write_points_string(&[
                "cpu,host=A value=200i 2",
                "cpu,host=B value=100i 0",
                "cpu,host=B value=10i 2",
                "cpu,host=B value=1i 4",
                "cpu,host=B value=60i 6",
            ])
            .unwrap();
        engine.must_write_snapshot();
        let request = |kinds| AggregateRequest {
            measurement: MeasurementRef::new("cpu"),
            field: "value".into(),
            time_range: TimeRange::new(1, 5),
            aggregates: kinds,
        };
        let result = engine
            .engine
            .aggregate_projected_scan(request(vec![AggregateKind::Max]))
            .unwrap()
            .unwrap();
        assert_eq!(result.state.max, Some(AggregateNumber::Integer(200)));
        assert_eq!(result.metrics.decoded_blocks, 0);
        let result = engine
            .engine
            .aggregate_projected_scan(request(vec![AggregateKind::Count, AggregateKind::Max]))
            .unwrap()
            .unwrap();
        assert_eq!(result.state.max, Some(AggregateNumber::Integer(200)));
        assert_eq!(result.state.count, Some(3));
        assert_eq!(result.metrics.decoded_blocks, 1);
        let windows = vec![TimeRange::new(1, 3), TimeRange::new(4, 5)];
        let result = engine
            .engine
            .prepare_aggregate_windows_scan_filtered(
                vec![request(vec![AggregateKind::Max])],
                windows,
                StoragePredicate::Constant(true),
            )
            .unwrap()
            .run()
            .unwrap();
        let result = result[0].as_ref().unwrap();
        assert_eq!(result.states[0].max, Some(AggregateNumber::Integer(200)));
        assert_eq!(result.states[1].max, Some(AggregateNumber::Integer(1)));
        assert_eq!(result.metrics.decoded_blocks, 1);
        engine.close();
    }
    #[test]
    fn query_task_permits_bound_admission_and_release_on_drop() {
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let first = QueryTaskPermit::try_acquire(&active, 2).unwrap();
        let second = QueryTaskPermit::try_acquire(&active, 2).unwrap();
        assert!(QueryTaskPermit::try_acquire(&active, 2).is_none());
        drop(first);
        assert!(QueryTaskPermit::try_acquire(&active, 2).is_some());
        drop(second);
        assert_eq!(active.load(std::sync::atomic::Ordering::Acquire), 0);
    }
}
