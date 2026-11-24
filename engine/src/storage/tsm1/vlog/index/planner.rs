//! Adapt immutable index metadata to the existing VLog file-selection policy.

use super::{
    LEVEL_BLOCK, Result,
    page_group::{Column, ColumnValue},
    reader::IndexFile,
};
use crate::{
    common::time::Time,
    storage::tsm1::{
        engine::EngineStatistics,
        scheduler::VLogScheduler,
        vlog::{default_compact_plan::VLogDefaultPlanner, file_store::FileStat},
    },
};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

pub(crate) struct IndexPlanner {
    cold: Duration,
    files: Vec<Arc<IndexFile>>,
    excluded: HashSet<u32>,
    planner: Option<VLogDefaultPlanner>,
    paths: HashMap<String, Arc<IndexFile>>,
    scheduler: VLogScheduler,
    stats: Arc<EngineStatistics>,
}

impl IndexPlanner {
    pub(crate) fn new(cold: Duration, stats: Arc<EngineStatistics>, concurrency: i64) -> Self {
        Self {
            cold,
            files: Vec::new(),
            excluded: HashSet::new(),
            planner: None,
            paths: HashMap::new(),
            scheduler: VLogScheduler::new(Arc::clone(&stats), concurrency),
            stats,
        }
    }

    /// Call on a blocking worker. Publication locks are never held during metadata IO.
    pub(crate) fn select(
        &mut self,
        files: &[Arc<IndexFile>],
        excluded: &HashSet<u32>,
        max_bytes: u64,
        last_write: Time,
        cancelled: &impl Fn() -> bool,
    ) -> Result<Vec<Arc<IndexFile>>> {
        let started = crate::common::slow_trace::now();
        let _timing = scopeguard::guard((), |_| {
            crate::common::slow_trace::log_background_stage(
                "compaction_plan",
                started,
                "aggregate metadata adapter and original VLog planner",
            )
        });
        let unchanged = self.planner.is_some()
            && self.excluded == *excluded
            && self.files.len() == files.len()
            && self.files.iter().zip(files).all(|(a, b)| Arc::ptr_eq(a, b));
        if !unchanged {
            let excluded_generations: HashSet<_> = files
                .iter()
                .filter(|f| excluded.contains(&f.file_no))
                .map(|f| f.generation)
                .collect();
            let mut ordered = files.to_vec();
            ordered.sort_by_key(|f| (f.generation, f.seq));
            let mut stats = Vec::new();
            let mut counts = HashMap::new();
            let mut paths = HashMap::new();
            let mut shifts = HashMap::new();
            for file in &ordered {
                shifts
                    .entry(file.generation)
                    .or_insert(i64::from(file.seq == 0));
            }
            for file in &ordered {
                if cancelled() {
                    return Err(super::Error::CompactionCancelled);
                }
                if excluded_generations.contains(&file.generation) {
                    continue;
                }
                // Early experimental snapshots started at zero. Normalize the whole
                // generation without changing its durable identity or splitting it.
                let sequence = file.seq + shifts[&file.generation];
                let path = PathBuf::from(format!("{:09}-{:09}.idx", file.generation, sequence));
                let has_tombstone = file.tombstone.is_some();
                counts.insert(path.clone(), first_block_count(file)?);
                paths.insert(path.to_string_lossy().into_owned(), Arc::clone(file));
                stats.push(FileStat {
                    path,
                    size: file.size(),
                    has_tombstone,
                    ..Default::default()
                });
            }
            self.planner = Some(VLogDefaultPlanner::from_snapshot(stats, counts, self.cold));
            self.paths = paths;
            self.files = files.to_vec();
            self.excluded = excluded.clone();
        }
        let planner = self.planner.as_ref().unwrap();
        let mut groups = [
            planner.plan_level(1),
            planner.plan_level(2),
            planner.plan_level(3),
            planner.plan(last_write),
        ];
        if groups[3].is_empty() {
            groups[3] = planner.plan_optimize();
        }
        // Apply the configured IO cap to complete planner groups, never partial generations.
        for (level, groups) in groups.iter_mut().enumerate() {
            planner.release(groups);
            groups.retain(|group| {
                group.iter().map(|p| self.paths[p].size()).sum::<u64>() <= max_bytes
            });
            self.scheduler
                .set_depth(level as i64 + 1, groups.len() as i64);
            if level < 3 {
                self.stats.vlog_compactions_queue[level]
                    .store(groups.len() as i64, std::sync::atomic::Ordering::SeqCst);
            } else {
                self.stats
                    .vlog_optimize_compactions_queue
                    .store(groups.len() as i64, std::sync::atomic::Ordering::SeqCst);
            }
        }
        Ok(match self.scheduler.next() {
            Some((level, true)) => groups[level as usize - 1][0]
                .iter()
                .map(|p| Arc::clone(&self.paths[p]))
                .collect(),
            _ => Vec::new(),
        })
    }
}

fn first_block_count(file: &IndexFile) -> Result<i32> {
    let entries = file.directory_page(0)?;
    let entry = &entries[0];
    let header = file.segment_header(entry)?;
    if header.level(LEVEL_BLOCK).is_none() {
        return Ok(0);
    }
    let records = file.read_level(
        entry,
        &header,
        LEVEL_BLOCK,
        entry.min_start,
        entry.min_start,
        &[Column::Count],
    )?;
    Ok(
        match records
            .first()
            .and_then(|r| r.values[Column::Count as usize])
        {
            Some(ColumnValue::Count(count)) => count.min(i32::MAX as u64) as i32,
            _ => 0,
        },
    )
}

/// Preserve the original sequence-based promotion, capped at the highest file level.
/// Aggregate tree levels are independent of this file-compaction level.
pub(super) fn next_sequence(inputs: &[Arc<IndexFile>]) -> i64 {
    let newest = inputs.iter().map(|f| f.generation).max().unwrap();
    let mut sequences = inputs
        .iter()
        .filter(|f| f.generation == newest)
        .map(|f| f.seq);
    let first = sequences.next().unwrap();
    let (min, max) = sequences.fold((first, first), |(min, max), seq| {
        (min.min(seq), max.max(seq))
    });
    (max + i64::from(min == 0) + 1).min(4)
}

#[cfg(test)]
pub(crate) fn select_inputs(store: &super::store::IndexStore) -> Result<Vec<Arc<IndexFile>>> {
    let files = store.index_readers();
    IndexPlanner::new(Duration::ZERO, Arc::new(EngineStatistics::default()), 1).select(
        &files,
        &HashSet::new(),
        u64::MAX,
        Time::now(),
        &|| false,
    )
}

#[cfg(test)]
#[path = "planner_tests.rs"]
mod tests;
