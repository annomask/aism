use std::{
    collections::{HashMap, HashSet},
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use crate::common::time::Time;
use parking_lot::RwLock;

use super::{
    compact::{CompactionGroup, MAX_TSM_FILE_SIZE, parse_file_name},
    file_store::{FileStat, FileStore, default_parse_file_name},
};
use crate::config::DEFAULT_MAX_POINTS_PER_BLOCK;

/// VLogDefaultPlanner using a strategy to roll up multiple generations of
/// TSM files into larger files in stages.  It attempts to minimize the number of TSM
/// files on disk while rolling up a bounder number of files.
pub struct VLogDefaultPlanner {
    // compact_full_write_cold_duration specifies the length of time after
    // which if no writes have been committed to the WAL, the engine will
    // do a full compaction of the TSM files in this shard. This duration
    // should always be greater than the CACHE_FLUSH_WRITE_COLD_DURATION
    compact_full_write_cold_duration: Duration,

    inner: RwLock<InnerDefaultVLogPlanner>,
}

struct InnerDefaultVLogPlanner {
    // the last time Plan was called
    last_plan_check: Time,

    file_store: PlanningFiles,

    // the last time findGenerations was run
    last_find_generations: Time,

    // the last set of generations found by findGenerations
    last_generations: TSMGenerations,

    // force_full causes the next full plan requests to plan any files
    // that may need to be compacted.  Normally, these files are skipped and scheduled
    // infrequently as the plans are more expensive to run.
    force_full: bool,

    // files_in_use is the set of files that have been returned as part of a plan and might
    // be being compacted.  Two plans should not return the same file at any given time.
    files_in_use: HashSet<String>,
}

enum PlanningFiles {
    VLog(Arc<FileStore>),
    Snapshot {
        stats: Vec<FileStat>,
        block_counts: HashMap<PathBuf, i32>,
        modified: Time,
    },
}

impl PlanningFiles {
    fn stats(&self) -> Vec<FileStat> {
        match self {
            Self::VLog(store) => store.stats(),
            Self::Snapshot { stats, .. } => stats.clone(),
        }
    }

    fn last_modified(&self) -> Time {
        match self {
            Self::VLog(store) => store.last_modified(),
            Self::Snapshot { modified, .. } => *modified,
        }
    }

    fn block_count(&self, path: &Path, index: usize) -> i32 {
        match self {
            Self::VLog(store) => store.block_count(path, index),
            Self::Snapshot { block_counts, .. } => block_counts.get(path).copied().unwrap_or(0),
        }
    }
}

impl VLogDefaultPlanner {
    pub fn new(file_store: Arc<FileStore>, write_cold_duration: Duration) -> Self {
        Self::with_files(PlanningFiles::VLog(file_store), write_cold_duration)
    }

    /// Reuse the VLog policy with immutable metadata prepared outside planner locks.
    pub(crate) fn from_snapshot(
        stats: Vec<FileStat>,
        block_counts: HashMap<PathBuf, i32>,
        write_cold_duration: Duration,
    ) -> Self {
        Self::with_files(
            PlanningFiles::Snapshot {
                stats,
                block_counts,
                modified: Time::now(),
            },
            write_cold_duration,
        )
    }

    fn with_files(file_store: PlanningFiles, write_cold_duration: Duration) -> Self {
        Self {
            compact_full_write_cold_duration: write_cold_duration,
            inner: RwLock::new(InnerDefaultVLogPlanner {
                last_plan_check: Time::from_timestamp(0, 0),
                file_store,
                last_find_generations: Time::from_timestamp(0, 0),
                last_generations: TSMGenerations::default(),
                force_full: false,
                files_in_use: HashSet::new(),
            }),
        }
    }

    #[cfg(feature = "mock_compact_plan")]
    pub fn set_file_store(&self, fs: Arc<FileStore>) {}

    #[cfg(not(feature = "mock_compact_plan"))]
    pub fn set_file_store(&self, fs: Arc<FileStore>) {
        let mut inner = self.inner.write();
        inner.file_store = PlanningFiles::VLog(fs);
    }

    #[cfg(test)]
    #[cfg(feature = "mock_vlog_file_store")]
    fn set_last_modified(&self, t: Time) {
        let mut inner = self.inner.write();
        if let PlanningFiles::VLog(store) = &inner.file_store {
            store.set_last_modified(t);
        }
    }

    #[cfg(feature = "mock_compact_plan")]
    pub fn full_compacted(&self) -> bool {
        false
    }

    /// Returns true if the shard is fully compacted.
    #[cfg(not(feature = "mock_compact_plan"))]
    pub fn full_compacted(&self) -> bool {
        let gens = self.find_generations(false);
        gens.len() <= 1 && !gens.has_tombstones()
    }

    #[cfg(feature = "mock_compact_plan")]
    pub fn force_full(&self) {}

    /// force_full causes the planner to return a full compaction plan the next time
    /// a plan is requested.  When ForceFull is called, level and optimize plans will
    /// not return plans until a full plan is requested and released.
    #[cfg(not(feature = "mock_compact_plan"))]
    pub fn force_full(&self) {
        let mut inner = self.inner.write();
        inner.force_full = true;
    }

    #[cfg(feature = "mock_compact_plan")]
    pub fn plan_level(&self, level: i64) -> Vec<CompactionGroup> {
        vec![]
    }

    /// Returns a set of TSM files to rewrite for a specific level.
    #[cfg(not(feature = "mock_compact_plan"))]
    pub fn plan_level(&self, level: i64) -> Vec<CompactionGroup> {
        // If a full plan has been requested, don't plan any levels which will prevent
        // the full plan from acquiring them.
        {
            let inner = self.inner.read();
            if inner.force_full {
                return vec![];
            }
        }

        // Determine the generations from all files on disk.  We need to treat
        // a generation conceptually as a single file even though it may be
        // split across several files in sequence.
        let generations = self.find_generations(true);
        // If there is only one generation and no tombstones, then there's nothing to do.
        if generations.len() <= 1 && !generations.has_tombstones() {
            return vec![];
        }

        // Group each generation by level,  the same generation in two adjacent levels become part of the same group.
        // Groups are sorted by level in descending.
        let mut current_gen = TSMGenerations::default();
        let mut groups = vec![];
        for i in 0..generations.len() {
            let cur = generations[i].clone();

            // See if this generation is orphan'd which would prevent it from being further
            // compacted until a final full compaction runs.
            if i < generations.len() - 1 && cur.level() < generations[i + 1].level() {
                current_gen.push(cur);
                continue;
            }

            if current_gen.is_empty() || current_gen.level() == cur.level() {
                current_gen.push(cur);
                continue;
            }
            groups.push(current_gen);

            current_gen = TSMGenerations::default();
            current_gen.push(cur);
        }

        if !current_gen.is_empty() {
            groups.push(current_gen);
        }

        // Remove any groups in the wrong level
        let mut level_groups = vec![];
        for cur in groups {
            if cur.level() == level {
                level_groups.push(cur);
            }
        }

        // the minimum number of generations (files count in level) that will trigger compaction
        let mut min_generations = 4;
        if level == 1 {
            min_generations = 8;
        }

        // Group the level_groups files, and compaction merges the files of each group
        // The grouping condition is that the file count reaches min_generations or there are tombstones.
        let mut cgroups = vec![];
        for mut group in level_groups {
            // group: all generations in the group are same.
            for chunk in group.chunk(min_generations) {
                let mut cgroup = CompactionGroup::default();
                let mut has_tombstones = false;
                for generation in chunk.iter() {
                    if generation.has_tombstones() {
                        has_tombstones = true;
                    }
                    for file in generation.files.iter() {
                        cgroup.push(file.path.to_str().unwrap().to_string()); // file path are sorted
                    }
                }

                if (chunk.len() as i64) < min_generations && !has_tombstones {
                    continue;
                }

                cgroups.push(cgroup);
            }
        }

        // mark file in use
        if !self.acquire(&cgroups) {
            return vec![];
        }

        cgroups
    }

    #[cfg(feature = "mock_compact_plan")]
    pub fn plan_optimize(&self) -> Vec<CompactionGroup> {
        vec![]
    }

    /// Returns all TSM files if they are in different generations in order
    /// to optimize the index across TSM files.  Each returned compaction group can be
    /// compacted concurrently.
    #[cfg(not(feature = "mock_compact_plan"))]
    pub fn plan_optimize(&self) -> Vec<CompactionGroup> {
        // If a full plan has been requested, don't plan any levels which will prevent
        // the full plan from acquiring them.
        {
            let inner = self.inner.read();
            if inner.force_full {
                return vec![];
            }
        }

        // Determine the generations from all files on disk.  We need to treat
        // a generation conceptually as a single file even though it may be
        // split across several files in sequence.
        let generations = self.find_generations(true);
        // If there is only one generation and no tombstones, then there's nothing to do.
        if generations.len() <= 1 && !generations.has_tombstones() {
            return vec![];
        }

        // Group each generation by level such that two adjacent generations in the same
        // level become part of the same group.
        let mut current_gen = TSMGenerations::default();
        let mut groups = vec![];
        for i in 0..generations.len() {
            let cur = &generations[i];

            // Skip the file if it's over the max size and contains a full block and it does not have any tombstones
            if cur.count() > 2
                && cur.size() > MAX_TSM_FILE_SIZE as u64
                && self
                    .inner
                    .read()
                    .file_store
                    .block_count(&cur.files[0].path, 1)
                    == DEFAULT_MAX_POINTS_PER_BLOCK as i32
                && !cur.has_tombstones()
            {
                continue;
            }

            // See if this generation is orphan'd which would prevent it from being further
            // compacted until a final full compaction runs.
            if i < generations.len() - 1 && cur.level() < generations[i + 1].level() {
                current_gen.push(cur.clone());
                continue;
            }

            if current_gen.is_empty() || current_gen.level() == cur.level() {
                current_gen.push(cur.clone());
                continue;
            }
            groups.push(current_gen);

            current_gen = TSMGenerations::default();
            current_gen.push(cur.clone());
        }

        if !current_gen.is_empty() {
            groups.push(current_gen);
        }

        // Only optimize level 4 files since using lower-levels will collide
        // with the level planners
        let mut level_groups = vec![];
        for cur in groups {
            if cur.level() == 4 {
                level_groups.push(cur);
            }
        }

        let mut cgroups = vec![];
        for group in level_groups {
            // Skip the group if it's not worthwhile to optimize it
            if group.len() < 4 && !group.has_tombstones() {
                continue;
            }

            let mut cgroup = CompactionGroup::default();
            for generation in group {
                for file in generation.files {
                    cgroup.push(file.path.to_str().unwrap().to_string()); // 该 tsmGeneration 内的所有文件的path
                }
            }

            cgroups.push(cgroup);
        }

        // mark file in use
        if !self.acquire(&cgroups) {
            return vec![];
        }

        cgroups
    }

    #[cfg(feature = "mock_compact_plan")]
    pub fn plan(&self, last_write: Time) -> Vec<CompactionGroup> {
        vec![]
    }

    /// Returns a set of TSM files to rewrite for level 4 or higher.  The planning returns
    /// multiple groups if possible to allow compactions to run concurrently.
    #[cfg(not(feature = "mock_compact_plan"))]
    pub fn plan(&self, last_write: Time) -> Vec<CompactionGroup> {
        let generations = self.find_generations(true);

        let force_full = self.inner.read().force_full;

        // first check if we should be doing a full compaction because nothing has been written in a long time
        if force_full
            || self.compact_full_write_cold_duration > Duration::new(0, 0)
                && Time::now() - last_write > self.compact_full_write_cold_duration
                && generations.len() > 1
        {
            // Reset the full schedule if we planned because of it.
            if force_full {
                self.inner.write().force_full = false;
            }

            let mut tsm_files = vec![];
            let mut gen_count = 0;
            for (i, group) in generations.iter().enumerate() {
                let mut skip = false;

                // Skip the file if it's over the max size and contains a full block and it does not have any tombstones
                if generations.len() > 2
                    && group.size() > MAX_TSM_FILE_SIZE as u64
                    && self
                        .inner
                        .read()
                        .file_store
                        .block_count(&group.files[0].path, 1)
                        == DEFAULT_MAX_POINTS_PER_BLOCK as i32
                    && !group.has_tombstones()
                {
                    skip = true;
                }

                // We need to look at the level of the next file because it may need to be combined with this generation
                // but won't get picked up on it's own if this generation is skipped.  This allows the most recently
                // created files to get picked up by the full compaction planner and avoids having a few less optimally
                // compressed files.
                if i < generations.len() - 1 && generations[i + 1].level() <= 3 {
                    skip = false;
                }

                if skip {
                    continue;
                }

                for f in group.files.iter() {
                    tsm_files.push(f.path.to_str().unwrap().to_string());
                }
                gen_count += 1;
            }
            tsm_files.sort();

            // Make sure we have more than 1 file and more than 1 generation
            if tsm_files.len() <= 1 || gen_count <= 1 {
                return vec![];
            }

            let group = vec![CompactionGroup(tsm_files)];
            if !self.acquire(&group) {
                return vec![];
            }
            return group;
        }

        // don't plan if nothing has changed in the filestore
        {
            let inner = self.inner.read();
            if inner.last_plan_check > inner.file_store.last_modified()
                && !generations.has_tombstones()
            {
                // last_plan_check is after filestore's last_modified
                return vec![];
            }
        }

        self.inner.write().last_plan_check = Time::now();

        // If there is only one generation, return early to avoid re-compacting the same file
        // over and over again.
        if generations.len() <= 1 && !generations.has_tombstones() {
            return vec![];
        }

        // Need to find the ending point for level 4 files.  They will be the oldest files. We scan
        // each generation in descending break once we see a file less than 4.
        let (mut end, mut start) = (0, 0);
        for (i, g) in generations.iter().enumerate() {
            if g.level() <= 3 {
                break;
            }
            end = i + 1;
        }

        // As compactions run, the oldest files get bigger.  We don't want to re-compact them during
        // this planning if they are maxed out so skip over any we see.
        let mut has_tombstones = false;
        for (i, g) in generations.as_slice()[..end].iter().enumerate() {
            if g.has_tombstones() {
                has_tombstones = true;
            }

            if has_tombstones {
                continue;
            }

            // Skip the file if it's over the max size and contains a full block or the generation is split
            // over multiple files.  In the latter case, that would mean the data in the file spilled over
            // the 2GB limit.
            if g.size() > MAX_TSM_FILE_SIZE as u64
                && self
                    .inner
                    .read()
                    .file_store
                    .block_count(&g.files[0].path, 1)
                    == DEFAULT_MAX_POINTS_PER_BLOCK as i32
            {
                start = i + 1;
            }

            // This is an edge case that can happen after multiple compactions run.  The files at the beginning
            // can become larger faster than ones after them.  We want to skip those really big ones and just
            // compact the smaller ones until they are closer in size.
            if i > 0 && g.size() * 2 < generations[i - 1].size() {
                start = i;
                break;
            }
        }

        // step is how may files to compact in a group.  We want to clamp it at 4 but also stil
        // return groups smaller than 4.
        let step = { if end < 4 { end } else { 4 } };

        // slice off the generations that we'll examine
        let generations = &generations.as_slice()[start..end];

        // Loop through the generations in groups of size step and see if we can compact all (or
        // some of them as group)
        let mut groups = vec![];
        let mut i = 0;
        while i < generations.len() {
            let mut skip_group = false;
            let mut start_index = i;

            let mut j = i;
            while j < i + step && j < generations.len() {
                let generation = &generations[j];
                let lvl = generation.level();

                // Skip compacting this group if there happens to be any lower level files in the
                // middle.  These will get picked up by the level compactors.
                if lvl <= 3 {
                    skip_group = true;
                    break;
                }

                // Skip the file if it's over the max size and it contains a full block
                if generation.size() >= MAX_TSM_FILE_SIZE as u64
                    && self
                        .inner
                        .read()
                        .file_store
                        .block_count(&generation.files[0].path, 1)
                        == DEFAULT_MAX_POINTS_PER_BLOCK as i32
                    && !generation.has_tombstones()
                {
                    start_index += 1;
                    j += 1;
                    continue;
                }

                j += 1;
            }

            if skip_group {
                continue;
            }

            let end_index = {
                if i + step > generations.len() {
                    generations.len()
                } else {
                    i + step
                }
            };
            if end_index - start_index > 0 {
                let gens = TSMGenerations(generations[start_index..end_index].to_vec());
                groups.push(gens);
            }

            i += step;
        }

        if groups.is_empty() {
            return vec![];
        }

        // With the groups, we need to evaluate whether the group as a whole can be compacted
        let mut compactable = vec![];
        for g in groups {
            // if we don't have enough generations to compact, skip it
            if g.len() < 4 && !g.has_tombstones() {
                continue;
            }
            compactable.push(g);
        }

        // All the files to be compacted must be compacted in order.  We need to convert each
        // group to the actual set of files in that group to be compacted.
        let mut tsm_files = vec![];
        for c in compactable {
            let mut cgroup = CompactionGroup::default();
            for g in c.iter() {
                for f in &g.files {
                    cgroup.push(f.path.to_str().unwrap().to_string());
                }
            }
            cgroup.sort();
            tsm_files.push(cgroup);
        }

        if !self.acquire(&tsm_files) {
            return vec![];
        }

        tsm_files
    }

    /// Groups all the TSM files by generation based on their filename,
    /// then returns the generations in descending order (newest first).
    /// If skip_in_use is true, tsm files that are part of an existing compaction plan
    /// are not returned.
    fn find_generations(&self, skip_in_use: bool) -> TSMGenerations {
        let mut inner = self.inner.write();

        let last = inner.last_find_generations;
        let last_gen = inner.last_generations.clone();

        if last != Time::from_timestamp(0, 0) && inner.file_store.last_modified().eq(&last) {
            return last_gen;
        }

        let gen_time = inner.file_store.last_modified();
        let tsm_stats = inner.file_store.stats();
        let mut generations = HashMap::<i64, TSMGeneration>::new();
        for f in tsm_stats {
            let path = f.path.as_os_str().to_str().unwrap();
            let (generation, _) = parse_file_name(path).unwrap();

            // Skip any files that are assigned to a current compaction plan
            if inner.files_in_use.contains(path) && skip_in_use {
                continue;
            }

            generations
                .entry(generation)
                .or_insert_with(|| TSMGeneration::new(generation))
                .files
                .push(f);
        }

        let mut ordered_generations = TSMGenerations::default();
        for (_, g) in generations {
            ordered_generations.push(g);
        }

        if !ordered_generations.is_sorted() {
            ordered_generations.sort_by_key(|g| g.id);
        }

        inner.last_find_generations = gen_time;
        inner.last_generations = ordered_generations.clone();

        ordered_generations
    }

    fn acquire(&self, groups: &[CompactionGroup]) -> bool {
        let mut inner = self.inner.write();

        // See if the new files are already in use
        for g in groups {
            for f in g.iter() {
                if inner.files_in_use.contains(f) {
                    return false;
                }
            }
        }

        // Mark all the new files in use
        for g in groups {
            for f in g.iter() {
                inner.files_in_use.insert(f.clone());
            }
        }

        true
    }

    #[cfg(feature = "mock_compact_plan")]
    pub fn release(&self, groups: &[CompactionGroup]) {}

    /// Removes the files reference in each compaction group allowing new plans
    /// to be able to use them.
    #[cfg(not(feature = "mock_compact_plan"))]
    pub fn release(&self, groups: &[CompactionGroup]) {
        let mut inner = self.inner.write();
        for g in groups {
            for f in g.iter() {
                inner.files_in_use.remove(f);
            }
        }
    }
}

// TSMGeneration represents the TSM files within a generation.
// 000001-01.tsm, 000001-02.tsm would be in the same generation
// 000001 each with different sequence numbers.
#[derive(Clone, Debug)]
struct TSMGeneration {
    id: i64,              // generation
    files: Vec<FileStat>, // sorted by file name
}

impl TSMGeneration {
    fn new(id: i64) -> Self {
        Self { id, files: vec![] }
    }

    /// Returns the total size of the files in the generation.
    fn size(&self) -> u64 {
        let mut n = 0;
        for f in self.files.iter() {
            n += f.size as u64;
        }
        n
    }

    /// Returns the level of the files in this generation.
    fn level(&self) -> i64 {
        // Level 0 is always created from the result of a cache compaction.  It generates
        // 1 file with a sequence num of 1.  Level 2 is generated by compacting multiple
        // level 1 files.  Level 3 is generate by compacting multiple level 2 files.  Level
        // 4 is for anything else.
        let (_, seq) =
            default_parse_file_name(self.files[0].path.as_os_str().to_str().unwrap()).unwrap();
        if seq < 4 {
            return seq;
        }
        4
    }

    /// Returns the number of files in the generation.
    fn count(&self) -> usize {
        self.files.len()
    }

    /// Returns true if there are keys removed for any of the files.
    fn has_tombstones(&self) -> bool {
        for f in self.files.iter() {
            if f.has_tombstone {
                return true;
            }
        }

        false
    }
}

impl PartialEq for TSMGeneration {
    fn eq(&self, other: &Self) -> bool {
        self.id < other.id
    }
}

impl PartialOrd for TSMGeneration {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.id.partial_cmp(&other.id)
    }
}

#[derive(Default, Debug, Clone, PartialEq, PartialOrd)]
struct TSMGenerations(Vec<TSMGeneration>); // TODO: sort by id

impl TSMGenerations {
    fn has_tombstones(&self) -> bool {
        for g in self.iter() {
            if g.has_tombstones() {
                return true;
            }
        }

        false
    }

    fn level(&self) -> i64 {
        let mut level = 0;
        for g in self.iter() {
            let lev = g.level();
            if lev > level {
                level = lev;
            }
        }

        level
    }

    fn chunk(&mut self, size: i64) -> Vec<TSMGenerations> {
        let mut chunks = vec![];
        while !self.is_empty() {
            if self.len() >= size as usize {
                chunks.push(TSMGenerations(
                    self.drain(..size as usize).collect::<Vec<_>>(),
                ));
            } else {
                chunks.push(TSMGenerations(std::mem::take(self)));
            }
        }

        chunks
    }

    fn is_sorted(&self) -> bool {
        if self.len() == 1 {
            return true;
        }

        for i in 1..self.len() {
            if self[i] < self[i - 1] {
                return false;
            }
        }

        true
    }
}

impl Deref for TSMGenerations {
    type Target = Vec<TSMGeneration>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TSMGenerations {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl IntoIterator for TSMGenerations {
    type IntoIter = std::vec::IntoIter<Self::Item>;
    type Item = TSMGeneration;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
