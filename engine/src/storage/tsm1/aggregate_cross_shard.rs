//! Cross-shard aggregation over logical ranges supplied by the database runtime.
#[cfg(test)]
#[path = "aggregate_cross_shard_tests.rs"]
mod tests;
use super::aggregate_projection::PreparedAggregateScan;
use super::*;
use crate::query::aggregate_windows::AggregateWindows;
use crate::query::storage_ir::{
    AggregateProjection, ProjectedAggregateState, ProjectedAggregateWindowsResult,
};
use vlog::index::cross_shard::{LogicalTree, MAX_CROSS_SHARD_LEAVES, project};

const GROUP_CACHE_BYTES: usize = 8 << 20;

/// Counters for the cross-shard layer, in addition to native scan counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CrossShardAggregateMetrics {
    pub logical_leaf_records: u64,
    pub cross_shard_records: u64,
    pub reused_trees: u64,
    pub fallback_series: u64,
    pub capture_retries: u64,
}

/// Projected states retain caller window order. Native scan metrics include
/// boundary reads and local fallbacks; logical metadata work is reported apart.
#[derive(Debug)]
pub struct CrossShardAggregateResult {
    pub result: ProjectedAggregateWindowsResult,
    pub cross_shard: CrossShardAggregateMetrics,
}

#[derive(Default)]
pub(crate) struct CrossShardCache {
    trees: HashMap<String, Arc<LogicalTree>>,
    bytes: usize,
}

pub(crate) struct PreparedShardAggregate {
    context: Arc<AggregateIndexContext>,
    scan: Option<PreparedAggregateScan>,
    scope: TimeRange,
    prepared_revision: Option<u64>,
}

impl Engine {
    pub(crate) fn prepare_cross_shard_aggregate(
        &self,
        request: AggregateRequest,
        predicate: StoragePredicate,
        scope: TimeRange,
    ) -> Result<Option<PreparedShardAggregate>> {
        let Some(context) = &self.aggregate_index else {
            return Ok(None);
        };
        // Include field discovery in the capture. A field may be created or
        // deleted before the remaining shards have finished preparation.
        let prepared_revision = stable_revision(context);
        Ok(Some(PreparedShardAggregate {
            context: Arc::clone(context),
            scan: self.prepare_aggregate_projected_scan_filtered(request, predicate)?,
            scope,
            prepared_revision,
        }))
    }
}

fn stable_revision(context: &AggregateIndexContext) -> Option<u64> {
    let epoch = context.write_epoch.load(Ordering::SeqCst);
    (context.active_writers.load(Ordering::SeqCst) == 0
        && !context.deleting.load(Ordering::SeqCst)
        && context.write_epoch.load(Ordering::SeqCst) == epoch)
        .then_some(epoch)
}

impl PreparedShardAggregate {
    fn revision(&self) -> Option<u64> {
        stable_revision(&self.context)
    }

    fn part(
        &self,
        series: Option<&str>,
        range: TimeRange,
    ) -> Result<Option<ProjectedAggregateWindowsResult>> {
        let Some(scan) = &self.scan else {
            return Ok(None);
        };
        let start = range.start_time.max(self.scope.start_time);
        let end = range.end_time.min(self.scope.end_time);
        if start > end {
            return Ok(None);
        }
        scan.cross_shard_windows(series, &[TimeRange::new(start, end)])
            .map(Some)
    }
}

fn merge_part(
    result: &mut ProjectedAggregateWindowsResult,
    window: usize,
    mut part: ProjectedAggregateWindowsResult,
) -> Result<()> {
    result.states[window]
        .merge(part.states.remove(0))
        .map_err(|reason| Error::StringErr {
            reason: reason.into(),
        })?;
    result.metrics.merge(part.metrics);
    result.index_reads.merge(part.index_reads);
    Ok(())
}

pub(crate) fn aggregate_group(
    shards: &[PreparedShardAggregate],
    request: &AggregateRequest,
    windows: &[TimeRange],
    cache: &Mutex<CrossShardCache>,
) -> Result<Option<CrossShardAggregateResult>> {
    let ranges = AggregateWindows::new(windows).map_err(|reason| Error::StringErr {
        reason: reason.into(),
    })?;
    if ranges.envelope() != request.time_range {
        return Err(Error::StringErr {
            reason: "cross-shard windows must match the request envelope".into(),
        });
    }
    if shards.iter().any(|shard| {
        shard.prepared_revision.is_none() || shard.revision() != shard.prepared_revision
    }) {
        return Err(Error::StringErr {
            reason: "cross-shard aggregate changed during preparation; retry the query".into(),
        });
    }
    let Some(first) = shards.iter().find_map(|s| s.scan.as_ref()) else {
        return Ok(None);
    };
    let typ = first.cross_shard_type();
    if shards
        .iter()
        .filter_map(|s| s.scan.as_ref())
        .any(|s| s.cross_shard_type() != typ)
    {
        return Err(Error::StringErr {
            reason: "cross-shard field type conflict".into(),
        });
    }
    let projection = AggregateProjection::from_aggregates(&request.aggregates);
    let started = slow_trace::now();
    for attempt in 0..2 {
        let Some(revisions) = shards
            .iter()
            .map(|shard| {
                shard
                    .revision()
                    .filter(|revision| Some(*revision) == shard.prepared_revision)
            })
            .collect::<Option<Vec<_>>>()
        else {
            continue;
        };
        let mut selections = Vec::with_capacity(shards.len());
        let mut series = std::collections::BTreeSet::new();
        for shard in shards {
            let selected = shard
                .scan
                .as_ref()
                .map(|s| s.cross_shard_series())
                .transpose()?
                .unwrap_or_default()
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>();
            series.extend(selected.iter().cloned());
            selections.push(selected);
        }
        let mut output = CrossShardAggregateResult {
            result: ProjectedAggregateWindowsResult {
                states: windows
                    .iter()
                    .map(|_| ProjectedAggregateState::empty(typ, projection))
                    .collect(),
                metrics: Default::default(),
                index_reads: Default::default(),
            },
            cross_shard: CrossShardAggregateMetrics {
                capture_retries: attempt,
                ..Default::default()
            },
        };
        if typ == FLOAT && projection.sum {
            // Finish each shard's native series reduction before merging shards.
            // Per-series then per-shard reduction can change cancellation.
            output.cross_shard.fallback_series = series.len() as u64;
            for (w, range) in windows.iter().enumerate() {
                for shard in shards {
                    if let Some(part) = shard.part(None, *range)? {
                        merge_part(&mut output.result, w, part)?;
                    }
                }
            }
        } else {
            for series_key in series {
                let mut leaves = Vec::new();
                // String endpoints use the existing local decoding path.
                let mut clean = !(typ == STRING && projection.has_endpoints());
                for (i, shard) in shards.iter().enumerate() {
                    if !clean {
                        break;
                    }
                    if !selections[i].contains(&series_key) {
                        continue;
                    }
                    let scan = shard.scan.as_ref().unwrap();
                    if let Some(mut part) = scan.cross_shard_leaves(&series_key, shard.scope)? {
                        output.cross_shard.logical_leaf_records += part.len() as u64;
                        if leaves.len() + part.len() > MAX_CROSS_SHARD_LEAVES {
                            clean = false;
                            break;
                        }
                        leaves.append(&mut part);
                    } else {
                        clean = false
                    }
                }
                if !clean {
                    output.cross_shard.fallback_series += 1;
                    for (w, range) in windows.iter().enumerate() {
                        for (i, shard) in shards.iter().enumerate() {
                            if selections[i].contains(&series_key) {
                                if let Some(part) = shard.part(Some(&series_key), *range)? {
                                    merge_part(&mut output.result, w, part)?
                                }
                            }
                        }
                    }
                    continue;
                }
                if leaves.is_empty() {
                    continue;
                }
                let key = format!("{series_key}{KEY_FIELD_SEPARATOR}{}", request.field);
                let cached = { cache.lock().trees.get(&key).cloned() };
                let tree = if let Some(tree) = cached.filter(|t| t.matches(&leaves)) {
                    output.cross_shard.reused_trees += 1;
                    tree
                } else {
                    let tree = Arc::new(LogicalTree::build(leaves).map_err(aggregate_index_error)?);
                    let size = tree.bytes();
                    if size <= GROUP_CACHE_BYTES {
                        let mut cache = cache.lock();
                        if let Some(old) = cache.trees.remove(&key) {
                            cache.bytes -= old.bytes()
                        }
                        if cache.bytes + size > GROUP_CACHE_BYTES {
                            cache.trees.clear();
                            cache.bytes = 0
                        }
                        cache.bytes += size;
                        cache.trees.insert(key, Arc::clone(&tree));
                    }
                    tree
                };
                for (w, range) in windows.iter().enumerate() {
                    let mut boundaries = Vec::new();
                    tree.visit(
                        *range,
                        |record| {
                            output.result.states[w]
                                .merge(project(record, projection)?)
                                .map_err(|reason| vlog::index::Error::InvalidFormat { reason })?;
                            output.result.metrics.index_records += 1;
                            output.result.metrics.index_levels =
                                output.result.metrics.index_levels.max(record.level);
                            output.result.metrics.statistics_blocks += u64::from(record.level == 1);
                            if !shards.iter().any(|s| {
                                s.scope.start_time <= record.pos.start
                                    && s.scope.end_time >= record.pos.end
                            }) {
                                output.cross_shard.cross_shard_records += 1;
                            }
                            Ok(())
                        },
                        |range| boundaries.push(range),
                    )
                    .map_err(aggregate_index_error)?;
                    for boundary in boundaries {
                        for (i, shard) in shards.iter().enumerate() {
                            if selections[i].contains(&series_key) {
                                if let Some(part) = shard.part(Some(&series_key), boundary)? {
                                    merge_part(&mut output.result, w, part)?
                                }
                            }
                        }
                    }
                }
            }
        }
        if shards
            .iter()
            .zip(revisions)
            .all(|(s, r)| s.revision() == Some(r))
        {
            for state in &output.result.states {
                state.validate().map_err(|reason| Error::StringErr {
                    reason: reason.into(),
                })?
            }
            slow_trace::log_query_stage(
                "query_cross_shard_aggregation",
                started,
                "logical range hierarchy",
            );
            return Ok(Some(output));
        }
    }
    Err(Error::StringErr {
        reason: "cross-shard aggregate changed during capture; retry the query".into(),
    })
}
