//! Aggregate-index workers use the existing engine cancellation and executor lifecycle.

use super::*;
use vlog::index::{
    Error as IndexError, allocation::OutputGenerations, io_budget::IoBudget, planner::IndexPlanner,
};

impl Engine {
    pub(super) fn enable_aggregate_compactions(&self, wait: bool) {
        let (quit, wg, vlog_files) = {
            let mut inner = self.inner.write();
            if wait {
                inner.level_workers -= 1;
            }
            if inner.level_workers != 0 || inner.done.is_some() {
                return;
            }
            inner.compactor.enable_compactions();
            let quit = CompactionCancellation::new();
            inner.done = Some(quit.clone());
            let wg = WaitGroup::new();
            wg.add(1);
            inner.wg = wg.clone();
            let vlog_files = Arc::clone(&inner.vlog_inner.as_ref().unwrap().read().vlog_file_store);
            (quit, wg, vlog_files)
        };
        let context = Arc::clone(self.aggregate_index.as_ref().unwrap());
        let index = Arc::clone(&self.index);
        let cache = { Arc::clone(&self.inner.read().cache) };
        let executor = Arc::clone(&self.executor);
        let limiter = self.vlog_compaction_limiter.clone();
        let stats = Arc::clone(&self.stats);
        let engine_id = self.id;
        let task_wg = wg.clone();
        let cancelled_wg = wg.clone();
        let (registration, completion) = accepted_task_guard(move || cancelled_wg.done());
        match self.executor.spawn_compact(async move {
            run_worker(
                context, vlog_files, executor, limiter, quit, engine_id, index, cache, stats,
            )
            .await;
            completion.complete_with(|| task_wg.done());
        }) {
            Ok(_) => registration.accepted(),
            Err(error) => {
                registration.rejected();
                wg.done();
                let mut inner = self.inner.write();
                inner.done = None;
                inner.compactor.disable_compactions();
                tracing::error!(target: "reefsdb::tsdb::compaction", engine_id,
                    error = %error, "aggregate compaction scheduling failed");
            }
        }
    }

    /// Explicit full compaction repairs conflicts and checkpoints the live file set.
    pub(super) fn compact_aggregate_index(&self) -> Result<()> {
        let context = self.aggregate_index.as_ref().unwrap();
        let store = &context.store;
        let inputs = store.index_readers();
        let vlog_files = {
            let inner = self.inner.read();
            Arc::clone(&inner.vlog_inner.as_ref().unwrap().read().vlog_file_store)
        };
        if !inputs.is_empty() {
            store
                .compact_files_controlled(
                    &inputs,
                    OutputGenerations::reserve(Arc::clone(&vlog_files)),
                    &|| false,
                    context.compaction_target_bytes,
                    IoBudget::new(context.io_limiter.clone(), || false),
                )
                .map_err(aggregate_index_error)?;
        }
        drop(inputs);
        store
            .collect_garbage(
                OutputGenerations::reserve(Arc::clone(&vlog_files)),
                u64::MAX,
                IoBudget::new(context.io_limiter.clone(), || false),
                &|| false,
            )
            .map_err(aggregate_index_error)?;
        store.checkpoint(true).map_err(aggregate_index_error)?;
        store.reap_retired().map_err(aggregate_index_error)?;
        self.reconcile_aggregate_metadata()?;
        Ok(())
    }
}

async fn run_worker(
    context: Arc<AggregateIndexContext>,
    vlog_files: Arc<VLogFileStore>,
    executor: ExecutorRef,
    limiter: Option<Arc<Fixed>>,
    quit: CompactionCancellation,
    engine_id: u64,
    index: Arc<Index>,
    cache: Arc<Cache>,
    stats: Arc<EngineStatistics>,
) {
    let store = &context.store;
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut deferred = HashSet::new();
    let mut published = 0;
    let mut planner = IndexPlanner::new(
        context.compaction_write_cold_duration,
        stats,
        limiter.as_ref().map_or(1, |limiter| limiter.capacity()),
    );
    loop {
        tokio::select! {
            _ = quit.token.cancelled() => break,
            _ = ticker.tick() => {},
        }
        if store.ensure_ready().is_err() {
            break;
        }
        if context.cleanup_pending.load(Ordering::SeqCst)
            && context.active_writers.load(Ordering::SeqCst) == 0
        {
            let job_context = Arc::clone(&context);
            let index = Arc::clone(&index);
            let cache = Arc::clone(&cache);
            let cleanup_quit = quit.clone();
            match executor.spawn_blocking_compact(move || {
                aggregate_metadata::reconcile_metadata(&job_context, &index, &cache, &|| {
                    cleanup_quit.is_cancelled()
                })
            }) {
                Ok(job) => match job.await {
                    Ok(Ok(())) => {}
                    other => {
                        tracing::warn!(target: "reefsdb::tsdb::compaction", engine_id, result = ?other, "aggregate metadata cleanup will be retried");
                        context.cleanup_pending.store(true, Ordering::SeqCst);
                    }
                },
                Err(error) => {
                    tracing::warn!(target: "reefsdb::tsdb::compaction", engine_id, error = %error, "aggregate metadata cleanup scheduling failed")
                }
            }
        }
        let files = store.index_readers();
        let live_ids = files.iter().map(|f| f.file_no).collect::<HashSet<_>>();
        deferred.retain(|id| live_ids.contains(id));
        let excluded = deferred.clone();
        let max_bytes = context.max_compaction_input_bytes;
        let last_write = cache.last_write_time();
        let planning_quit = quit.clone();
        let planning = executor.spawn_blocking_compact(move || {
            let result = planner.select(&files, &excluded, max_bytes, last_write, &|| {
                planning_quit.is_cancelled()
            });
            (planner, result)
        });
        let inputs = match planning {
            Ok(job) => match job.await {
                Ok((returned, result)) => {
                    planner = returned;
                    match result {
                        Ok(inputs) => inputs,
                        Err(IndexError::CompactionCancelled) => break,
                        Err(error) => {
                            tracing::warn!(target: "reefsdb::tsdb::compaction", engine_id,
                                error = %error, "aggregate compaction planning will be retried");
                            continue;
                        }
                    }
                }
                Err(error) => {
                    tracing::error!(target: "reefsdb::tsdb::compaction", engine_id,
                        error = ?error, "aggregate compaction planner terminated");
                    break;
                }
            },
            Err(error) => {
                tracing::error!(target: "reefsdb::tsdb::compaction", engine_id,
                    error = %error, "aggregate compaction planner scheduling failed");
                break;
            }
        };
        let target_bytes = inputs
            .iter()
            .map(|f| f.size())
            .sum::<u64>()
            .max(128 * 1024 * 1024)
            .min(context.compaction_target_bytes);
        let ids = inputs.iter().map(|f| f.file_no).collect::<Vec<_>>();
        let permit = if inputs.is_empty() {
            None
        } else if let Some(limiter) = &limiter {
            if !limiter.try_take() {
                continue;
            }
            Some(Arc::clone(limiter))
        } else {
            None
        };
        // A rejected or cancelled-before-start job must also release its permit.
        let permit = scopeguard::guard(permit, |limiter| {
            if let Some(limiter) = limiter {
                limiter.release();
            }
        });
        let job_store = Arc::clone(&store);
        let job_vlogs = Arc::clone(&vlog_files);
        let cancelled = quit.clone();
        let io_cancelled = quit.clone();
        let io_budget = IoBudget::new(context.io_limiter.clone(), move || {
            io_cancelled.is_cancelled()
        });
        let job = executor.spawn_blocking_compact(move || {
            let _permit = permit;
            if let Err(error) = job_store.reap_retired() {
                tracing::warn!(target: "reefsdb::tsdb::compaction", engine_id,
                    error = %error, "aggregate retired index cleanup will be retried");
            }
            if inputs.is_empty() || cancelled.is_cancelled() {
                return Ok(None);
            }
            let result = job_store.compact_files_controlled(
                &inputs,
                OutputGenerations::reserve(Arc::clone(&job_vlogs)),
                &|| cancelled.is_cancelled(),
                target_bytes,
                io_budget,
            );
            drop(inputs);
            if result.is_ok() {
                if let Err(error) = job_store.reap_retired() {
                    tracing::warn!(target: "reefsdb::tsdb::compaction", engine_id,
                        error = %error, "aggregate retired index cleanup will be retried");
                }
            }
            result.map(Some)
        });
        let result = match job {
            Ok(job) => match job.await {
                Ok(result) => result,
                Err(error) => {
                    tracing::error!(target: "reefsdb::tsdb::compaction", engine_id,
                        error = ?error, "aggregate compaction worker terminated");
                    break;
                }
            },
            Err(error) => {
                tracing::error!(target: "reefsdb::tsdb::compaction", engine_id,
                    error = %error, "aggregate compaction job scheduling failed");
                break;
            }
        };
        match result {
            Ok(Some(stats)) => {
                let cache = store.cache.stats();
                published += 1;
                if published % 32 == 0 {
                    let checkpoint_store = Arc::clone(&store);
                    let gc_vlogs = Arc::clone(&vlog_files);
                    let gc_cancel = quit.clone();
                    let io_cancel = quit.clone();
                    let gc_budget =
                        IoBudget::new(context.io_limiter.clone(), move || io_cancel.is_cancelled());
                    let max_bytes = context.max_compaction_input_bytes;
                    if let Ok(job) =
                        executor.spawn_blocking_compact(move || {
                            let stats = checkpoint_store.collect_garbage(OutputGenerations::reserve(Arc::clone(&gc_vlogs)), max_bytes,
                                gc_budget, &|| gc_cancel.is_cancelled())?;
                            tracing::debug!(target: "reefsdb::tsdb::compaction", engine_id,
                                source_files = stats.source_files, copied_blocks = stats.copied_blocks,
                                copied_bytes = stats.copied_bytes, "aggregate VLog garbage collection complete");
                            checkpoint_store.checkpoint(true)
                        })
                    {
                        if let Ok(Err(error)) = job.await {
                            tracing::warn!(target: "reefsdb::tsdb::compaction", engine_id,
                                error = %error, "aggregate checkpoint failed");
                        }
                    }
                }
                tracing::debug!(target: "reefsdb::tsdb::compaction", engine_id,
                input_files = ids.len(), copied_groups = stats.copied_groups,
                rewritten_groups = stats.rewritten_groups,
                coalesced_groups = stats.coalesced_groups,
                streamed_rebuild_fields = stats.streamed_rebuild_fields,
                repaired_blocks = stats.repaired_blocks,
                cache_bytes = cache.bytes, cache_hits = cache.hits, cache_misses = cache.misses,
                boundary_children_read = stats.boundary_children_read,
                "aggregate index compaction complete");
            }
            Ok(None) | Err(IndexError::CompactionBusy) => {}
            Err(IndexError::CompactionCancelled) => break,
            Err(error) => {
                // Defer these immutable identities until restart or replacement,
                // rather than rescanning an overlapping group on every tick.
                deferred.extend(ids);
                tracing::warn!(target: "reefsdb::tsdb::compaction", engine_id,
                    error = %error, "aggregate compaction inputs deferred");
            }
        }
    }
}
