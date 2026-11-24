use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ops::{Deref, DerefMut},
    path::Path,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

use crate::common::{time::Time, to_str, to_string, wait_group::WaitGroup};
use parking_lot::RwLock;
use snafu::{ResultExt, Snafu, ensure};

use super::{
    compact::{CompactionGroup, Error, Result, VLogCompactInfo, parse_file_name},
    file_store::{
        CMP_VLOG_FILE_EXTENSION, FileStat, FileStore, default_format_file_name,
        default_parse_file_name,
    },
    vlog_index_cache::Cache,
};
use crate::{
    index::ActiveIndex as Index,
    storage::tsm1::{engine::EngineStatistics, vlog::reader::VLogReader},
};

const COMPACT_LEVEL_1_MIN_FILES: usize = 32; // 建议=CPU CORES
const COMPACT_LEVEL_2_MIN_FILES: usize = 16;
const COMPACT_LEVEL_3_MIN_FILES: usize = 16;
const COMPACT_FULL_MIN_FILES: usize = 32; // level_1+level_2+level_3文件数据超过该参数时触发 full compact，建议=CPU CORES

const LEVEL_1_MAX_SIZE_PER_COMPACT: i64 = 2 * 1024 * 1024 * 1024; // max size per compact in level 1 vlog files.
const LEVEL_2_MAX_SIZE_PER_COMPACT: i64 = 4 * 1024 * 1024 * 1024; // max size per compact in level 2 vlog files.
const LEVEL_3_MAX_SIZE_PER_COMPACT: i64 = 8 * 1024 * 1024 * 1024; // max size per compact in level 3 vlog files.
const LEVEL_4_MAX_SIZE_PER_COMPACT: i64 = 16 * 1024 * 1024 * 1024; // max size per compact in level 4 vlog files.

pub struct VLogLazyPlanner {
    config: VLogLazyCompactConfig,
    compact_prioritize_lru_keys: bool,
    compact_uncached_keys: bool,

    // pub compact_done: WaitGroup,
    stats: Arc<EngineStatistics>,

    inner: RwLock<InnerLazyVLogPlanner>,
}

pub struct VLogLazyCompactConfig {
    pub vlog_compact_level_1_min_files: i64,
    pub vlog_compact_level_2_min_files: i64,
    pub vlog_compact_level_3_min_files: i64,
    pub vlog_compact_full_min_files: i64,

    pub vlog_level_1_max_size_per_compact: i64,
    pub vlog_level_2_max_size_per_compact: i64,
    pub vlog_level_3_max_size_per_compact: i64,
    pub vlog_level_4_max_size_per_compact: i64,

    // compact_full_write_cold_duration specifies the length of time after
    // which if no writes have been committed to the WAL, the engine will
    // do a full compaction of the TSM files in this shard. This duration
    // should always be greater than the CACHE_FLUSH_WRITE_COLD_DURATION
    pub compact_full_write_cold_duration: Duration,
}

struct InnerLazyVLogPlanner {
    // the last time Plan was called
    last_plan_check: Time,

    index: Arc<Index>,

    file_store: Arc<FileStore>,

    vlog_index_cache: Arc<Cache>,

    // the last set of levels found by find_levels
    last_levels: TSMLevels,

    // force_full causes the next full plan requests to plan any files
    // that may need to be compacted.  Normally, these files are skipped and scheduled
    // infrequently as the plans are more expensive to run.
    force_full: bool,

    // files_in_use is the set of files that have been returned as part of a plan and might
    // be being compacted.  Two plans should not return the same file at any given time.
    files_in_use: HashSet<String>,
}

impl VLogLazyPlanner {
    pub fn new(
        index: Arc<Index>,
        file_store: Arc<FileStore>,
        vlog_index_cache: Arc<Cache>,
        config: VLogLazyCompactConfig,
        stats: Arc<EngineStatistics>,
        compact_prioritize_lru_keys: bool,
        compact_uncached_keys: bool,
    ) -> Self {
        Self {
            config,
            compact_prioritize_lru_keys,
            compact_uncached_keys,
            // compact_done: WaitGroup::new(),
            stats,
            inner: RwLock::new(InnerLazyVLogPlanner {
                last_plan_check: Time::from_timestamp(0, 0),
                index,
                file_store,
                vlog_index_cache,
                last_levels: TSMLevels::default(),
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
        inner.file_store = fs;
    }

    #[cfg(test)]
    #[cfg(feature = "mock_vlog_file_store")]
    fn set_last_modified(&self, t: Time) {
        let mut inner = self.inner.write();
        inner.file_store.set_last_modified(t);
    }

    #[cfg(feature = "mock_compact_plan")]
    pub fn full_compacted(&self) -> bool {
        false
    }

    /// Returns true if the shard is fully compacted.
    #[cfg(not(feature = "mock_compact_plan"))]
    pub fn full_compacted(&self) -> bool {
        let levels = self.find_all_levels(false);
        levels.len() <= 1 && !levels.has_tombstones()
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
    pub fn plan_level(&self, level: i64) -> VLogCompactInfo {
        VLogCompactInfo::default()
    }

    /*
    1. find all vlogs by level
    2. find CompactionKeysAndIndexes: keys->VLogIndexes
    3. sort by vlog data size
    4. find frequently_queried_keys from LRU Cache
    5. split CompactionKeysAndIndexes:
       5.1 by frequently_queried_keys
       5.2 by vlog data size
    6. return CompactionKeysAndIndexes
     */
    /// Returns a set of TSM files to rewrite for a specific level.
    #[cfg(not(feature = "mock_compact_plan"))]
    pub fn plan_level(&self, level: i64) -> VLogCompactInfo {
        // Wait for all vlog compactions to finished to update vlog_index_cache
        let l1_running = self.stats.vlog_compactions_active[0].load(Ordering::SeqCst);
        let l2_running = self.stats.vlog_compactions_active[1].load(Ordering::SeqCst);
        let l3_running = self.stats.vlog_compactions_active[2].load(Ordering::SeqCst);
        let l4_running = self
            .stats
            .vlog_full_compactions_active
            .load(Ordering::SeqCst)
            + self
                .stats
                .vlog_optimize_compactions_active
                .load(Ordering::SeqCst);

        // Ensure that only one compaction can be running simultaneously at each level.
        if l1_running + l2_running + l3_running + l4_running >= 1 {
            return VLogCompactInfo::default();
        }

        // If a full plan has been requested, don't plan any levels which will prevent
        // the full plan from acquiring them.
        {
            let inner = self.inner.read();
            if inner.force_full
                || (!self.compact_uncached_keys && inner.index.lru_series_keys_is_empty())
            {
                return VLogCompactInfo::default();
            }
        }

        // 1. get all VLogReaders and keys from in the TSMLevel files.
        // let levels = self.find_level(level, true);
        let levels = match level {
            1 => self.find_levels(&[1], true),
            2 => self.find_levels(&[1, 2], true),
            3 => self.find_levels(&[1, 2, 3], true),
            _ => TSMLevels::default(),
        };

        // the min_levels number of levels (files count in level) that will trigger compaction
        let min_levels = match level {
            1 => self.config.vlog_compact_level_1_min_files,
            2 => self.config.vlog_compact_level_2_min_files,
            3 => self.config.vlog_compact_level_3_min_files,
            _ => unreachable!(),
        };

        if (levels.last().unwrap().len() as i64) < min_levels && !levels.has_tombstones() {
            return VLogCompactInfo::default();
        }

        let mut readers: Vec<Arc<VLogReader>> = vec![];
        let mut unique_keys = HashMap::new();
        let inner = self.inner.read();
        for lvls in levels {
            for file in lvls.files {
                // println!("$$$$$  plan_level file: {:?}", file.path);
                let tr = match inner.file_store.as_ref().vlog_reader_unref(&file.path) {
                    Some(r) => r,
                    None => {
                        return VLogCompactInfo::default();
                    }
                };
                // println!("$$$$$  plan_level file_reader: {:?}", tr.path());
                readers.push(tr);
                let keys = inner.file_store.as_ref().file_keys(&file.path);
                for (key, typ) in keys {
                    unique_keys.entry(key).or_insert(typ);
                }
            }
        }

        let mut keys = vec![];
        for (k, typ) in unique_keys {
            keys.push((k, typ));
        }
        keys.sort_by(|a, b| a.0.cmp(&b.0));
        // println!(
        //     "$$$$$  plan_level, level={}, keys_count={}",
        //     level,
        //     keys.len()
        // );

        // 2. get all vlog indexes
        let mut vlog_indexes = BTreeMap::new();
        for (key, typ) in keys.iter() {
            for r in readers.iter() {
                let mut indexes =
                    inner
                        .vlog_index_cache
                        .values_by_gen_and_seq(key, r.generation, r.seq);
                if !indexes.is_empty() {
                    vlog_indexes
                        .entry((key.clone(), *typ))
                        .or_insert_with(Vec::new)
                        .append(&mut indexes);
                }
            }
        }
        let index = Arc::clone(&inner.index);
        drop(inner);

        if vlog_indexes.is_empty()
            || (self.compact_prioritize_lru_keys
                && !self.compact_uncached_keys
                && index.lru_series_keys_is_empty())
        {
            return VLogCompactInfo::default();
        }

        // 3. sort by vlog data size
        let mut keys_indexes = vec![];
        for (k, indexes) in vlog_indexes {
            keys_indexes.push((k, indexes));
        }

        keys_indexes.sort_by(|a, b| {
            let mut asize = 0;
            for idx in a.1.iter() {
                asize += idx.size;
            }
            let mut bsize = 0;
            for idx in b.1.iter() {
                bsize += idx.size;
            }
            asize.cmp(&bsize)
        });

        let mut vlog_compact_info = VLogCompactInfo::default();

        // 4. Query frequently_queried_keys from LRU Cache
        let mut max_merged_size = self.config.vlog_level_1_max_size_per_compact;
        if level == 2 {
            max_merged_size = self.config.vlog_level_2_max_size_per_compact;
        } else if level == 3 {
            max_merged_size = self.config.vlog_level_3_max_size_per_compact;
        }

        // println!("$$$$$  plan_level, max_merged_size: {}", max_merged_size);

        let mut total_size = 0;
        let mut elapsed_size = 0;
        let mut unique_files = HashSet::new();
        let mut unique_readers = HashSet::new();
        let mut elapsed_keys = vec![];
        let mut elapsed_indexes = vec![];
        for (k, indexes) in keys_indexes {
            for idx in indexes.iter() {
                for r in readers.iter() {
                    if (idx.generation, idx.seq) == (r.generation, r.seq) {
                        unique_files.insert(r.path().to_str().unwrap().to_string());
                        unique_readers.insert(Arc::clone(r));
                    }
                }
            }

            if index.lru_series_keys_exists(to_str!(&k.0)) {
                total_size += indexes.iter().map(|x| x.size).sum::<i64>();
                vlog_compact_info.keys.push(k);
                vlog_compact_info.indexes.push(indexes);
            } else {
                elapsed_size += indexes.iter().map(|x| x.size).sum::<i64>();
                elapsed_keys.push(k);
                elapsed_indexes.push(indexes);
            }
        }

        // let mut elapsed_index_count = 0;
        // for idxes in elapsed_indexes.iter() {
        //     elapsed_index_count += idxes.len();
        // }
        // println!(
        //     "\t aaaaaaa plan_level, elapsed_keys.keys_count={}, elapsed_indexes.count: {}, total_size: {}, elapsed_size: {}",
        //     elapsed_keys.len(),
        //     elapsed_index_count,
        //     total_size,
        //     elapsed_size
        // );

        // Check if lru cached keys data is exceed or donot need to compact uncached keys.
        if total_size >= max_merged_size
            || (self.compact_prioritize_lru_keys && !self.compact_uncached_keys)
        {
            let unique_readers: Vec<_> = unique_readers.into_iter().collect();
            let unique_files: Vec<_> = unique_files.into_iter().collect();
            vlog_compact_info.readers = unique_readers;
            vlog_compact_info.files = unique_files;

            // mark file in use
            if !self.acquire(&vlog_compact_info.files) {
                return VLogCompactInfo::default();
            }

            return vlog_compact_info;
        }

        if level == 1 || level == 2 || level == 3 {
            // for (k, index) in keys_indexes {
            for (pos, k) in elapsed_keys.into_iter().enumerate() {
                let indexes = std::mem::take(&mut elapsed_indexes[pos]);
                total_size += indexes.iter().map(|x| x.size).sum::<i64>();

                for idx in indexes.iter() {
                    for r in readers.iter() {
                        if (idx.generation, idx.seq) == (r.generation, r.seq) {
                            unique_files.insert(r.path().to_str().unwrap().to_string());
                            unique_readers.insert(Arc::clone(r));
                        }
                    }
                }
                vlog_compact_info.keys.push(k);
                vlog_compact_info.indexes.push(indexes);
                if total_size >= max_merged_size {
                    break;
                }
            }
        }

        let unique_readers: Vec<_> = unique_readers.into_iter().collect();
        let unique_files: Vec<_> = unique_files.into_iter().collect();
        vlog_compact_info.readers = unique_readers;
        vlog_compact_info.files = unique_files;

        // let mut final_size = 0;
        // let mut info_index_count = 0;
        // for idxes in &vlog_compact_info.indexes {
        //     info_index_count += idxes.len();
        //     for idx in idxes {
        //         final_size += idx.size;
        //     }
        // }

        // println!(
        //     "\t aaaaaaa plan_level, vlog_compact_info.keys.keys_count={}, vlog_compact_info.indexes.count: {}, final_size: {}",
        //     vlog_compact_info.keys.len(),
        //     info_index_count,
        //     final_size
        // );

        // mark file in use
        if !self.acquire(&vlog_compact_info.files) {
            return VLogCompactInfo::default();
        }

        vlog_compact_info
    }

    #[cfg(feature = "mock_compact_plan")]
    pub fn plan_optimize(&self) -> VLogCompactInfo {
        VLogCompactInfo::default()
    }

    /// Returns all TSM files if they are in different generations in order
    /// to optimize the index across TSM files.  Each returned compaction group can be
    /// compacted concurrently.
    #[cfg(not(feature = "mock_compact_plan"))]
    pub fn plan_optimize(&self) -> VLogCompactInfo {
        VLogCompactInfo::default()
    }

    #[cfg(feature = "mock_compact_plan")]
    pub fn plan(&self, last_write: Time) -> VLogCompactInfo {
        VLogCompactInfo::default()
    }

    /// Returns a set of TSM files to rewrite for level 4 or higher.  The planning returns
    /// multiple groups if possible to allow compactions to run concurrently.
    #[cfg(not(feature = "mock_compact_plan"))]
    pub fn plan(&self, last_write: Time) -> VLogCompactInfo {
        // TODO: 读取所有文件，然后写入新文件，注意，写入到CPU_CORE个文件里

        // 注意：可能存在数据量不是那么大，导致level-2、level-3和level-4的文件数量
        // 达不到 COMPACT_LEVEL_x_MIN_FILES ，那么直接return，就不需要做了
        // 1.不存在level-3和level-4，return、
        // 2. level_1和level_2的文件数量 加起来，达不到 COMPACT_LEVEL_4_MIN_FILES ，return
        // 即，只有Level_1+Level_2+Level_3+Level_4的文件数量，超过了COMPACT_LEVEL_4_MIN_FILES，才进行
        // 这里面向level-4的full compact

        // Wait for all vlog compactions to finished to update vlog_index_cache
        let l1_running = self.stats.vlog_compactions_active[0].load(Ordering::SeqCst);
        let l2_running = self.stats.vlog_compactions_active[1].load(Ordering::SeqCst);
        let l3_running = self.stats.vlog_compactions_active[2].load(Ordering::SeqCst);
        let l4_running = self
            .stats
            .vlog_full_compactions_active
            .load(Ordering::SeqCst)
            + self
                .stats
                .vlog_optimize_compactions_active
                .load(Ordering::SeqCst);

        // Ensure that only one compaction can be running simultaneously at each level.
        if l1_running + l2_running + l3_running + l4_running >= 1 {
            return VLogCompactInfo::default();
        }

        let force_full = {
            let inner = self.inner.read();
            inner.force_full
        };

        if force_full
            || (self.config.compact_full_write_cold_duration > Duration::new(0, 0)
                && Time::now() - last_write > self.config.compact_full_write_cold_duration)
        {
            if force_full {
                let mut inner = self.inner.write();
                inner.force_full = false;
            }

            // FIXME:
            // full compact是否需要skip in use？按道理应该将所有 *.tsm.cmp 文件也进行compact？
            // 1. get all VLogReaders and keys from in the TSMLevel files.
            let levels = self.find_levels(&[1, 2, 3, 4], false);
            let mut total_files = 0;
            for lvl in levels.iter() {
                total_files += lvl.files.len();
            }

            if (total_files as i64) < self.config.vlog_compact_full_min_files {
                return VLogCompactInfo::default();
            }

            let mut readers: Vec<Arc<VLogReader>> = vec![];
            let mut unique_keys = HashMap::new();
            let inner = self.inner.read();
            for lvls in levels {
                for file in lvls.files {
                    let tr = match inner.file_store.as_ref().vlog_reader_unref(&file.path) {
                        Some(r) => r,
                        None => {
                            return VLogCompactInfo::default();
                        }
                    };
                    readers.push(tr);
                    let keys = inner.file_store.as_ref().file_keys(&file.path);
                    for (key, typ) in keys {
                        unique_keys.entry(key).or_insert(typ);
                    }
                }
            }

            let mut keys = vec![];
            for (k, typ) in unique_keys {
                keys.push((k, typ));
            }
            keys.sort_by(|a, b| a.0.cmp(&b.0));

            // 2. get all vlog indexes
            let mut vlog_indexes = BTreeMap::new();
            for (key, typ) in keys.iter() {
                for r in readers.iter() {
                    let mut indexes =
                        inner
                            .vlog_index_cache
                            .values_by_gen_and_seq(key, r.generation, r.seq);
                    if !indexes.is_empty() {
                        vlog_indexes
                            .entry((key.clone(), *typ))
                            .or_insert_with(Vec::new)
                            .append(&mut indexes);
                    }
                }
            }
            drop(inner);

            if vlog_indexes.is_empty() {
                return VLogCompactInfo::default();
            }

            // 2. get unique files and unique readers
            let mut vlog_compact_info = VLogCompactInfo::default();
            let mut unique_files = HashSet::new();
            let mut unique_readers = HashSet::new();
            for (k, indexes) in vlog_indexes {
                for idx in indexes.iter() {
                    for r in readers.iter() {
                        if (idx.generation, idx.seq) == (r.generation, r.seq) {
                            unique_files.insert(r.path().to_str().unwrap().to_string());
                            unique_readers.insert(Arc::clone(r));
                        }
                    }
                }

                vlog_compact_info.keys.push(k);
                vlog_compact_info.indexes.push(indexes);
            }

            let unique_readers: Vec<_> = unique_readers.into_iter().collect();
            let unique_files: Vec<_> = unique_files.into_iter().collect();

            vlog_compact_info.readers = unique_readers;
            vlog_compact_info.files = unique_files;
            return vlog_compact_info;
        }

        VLogCompactInfo::default()
    }

    fn find_level(&self, level: i64, skip_in_use: bool) -> TSMLevel {
        let mut inner = self.inner.write();

        let last = inner.last_levels[level as usize - 1].last_find_time();
        if last.eq(&Time::from_timestamp(0, 0)) && inner.file_store.last_modified().eq(last) {
            return inner.last_levels[level as usize - 1].clone();
        }

        let lev_time = inner.file_store.last_modified();
        let tsm_stats = inner.file_store.stats();
        let mut levels = TSMLevel::new(level);
        for f in tsm_stats {
            let (_, seq) = parse_file_name(&f.path).unwrap();
            if seq != level {
                continue;
            }

            // Skip any files that are assigned to a current compaction plan
            if inner
                .files_in_use
                .contains(f.path.as_os_str().to_str().unwrap())
                && skip_in_use
            {
                continue;
            }
            levels.files.push(f);
        }
        levels.last_find = lev_time;

        inner.last_levels[level as usize - 1] = levels.clone();

        levels
    }

    fn find_levels(&self, lvls: &[i64], skip_in_use: bool) -> TSMLevels {
        let mut levels = TSMLevels::default();
        for i in lvls {
            let level = self.find_level(*i, skip_in_use);
            // if !level.is_empty() {
            //     levels.push(level);
            // }
            levels.push(level);
        }
        levels
    }

    fn find_all_levels(&self, skip_in_use: bool) -> TSMLevels {
        let mut levels = TSMLevels::default();
        for i in 1..4 {
            let level = self.find_level(i, skip_in_use);
            if !level.is_empty() {
                levels.push(level);
            }
        }
        levels
    }

    fn acquire(&self, groups: &[String]) -> bool {
        let mut inner = self.inner.write();
        // See if the new files are already in use
        for f in groups.iter() {
            if inner.files_in_use.contains(f) {
                return false;
            }
        }

        // Mark all the new files in use
        for f in groups.iter() {
            inner.files_in_use.insert(f.clone());
        }

        true
    }

    #[cfg(feature = "mock_compact_plan")]
    pub fn release(&self, groups: &[String]) {}

    /// Removes the files reference in each compaction group allowing new plans
    /// to be able to use them.
    #[cfg(not(feature = "mock_compact_plan"))]
    pub fn release(&self, groups: &[String]) {
        let mut inner = self.inner.write();
        // for g in groups {
        for f in groups.iter() {
            inner.files_in_use.remove(f);
        }
        // }
    }
}

// TSMLevel represents the TSM files within a level.
// 000001-01.tsm, 000002-01.tsm would be in the same level
// 01 each with different generation numbers.
#[derive(Clone, Debug, Default)]
struct TSMLevel {
    level: i64, // level
    last_find: Time,
    files: Vec<FileStat>, // sorted by file name
}

impl TSMLevel {
    fn new(level: i64) -> Self {
        Self {
            level,
            last_find: Time::default(),
            files: vec![],
        }
    }

    /// Returns the total size of the files in the level.
    fn size(&self) -> u64 {
        let mut n = 0;
        for f in self.files.iter() {
            n += f.size as u64;
        }
        n
    }

    /// Returns the last find time in this level.
    pub fn last_find_time(&self) -> &Time {
        &self.last_find
    }

    /// Returns the level of the files in this level.
    fn level(&self) -> i64 {
        // Level 0 is always created from the result of a cache compaction.  It generates
        // 1 file with a sequence num of 1.  Level 2 is generated by compacting multiple
        // level 1 files.  Level 3 is generate by compacting multiple level 2 files.  Level
        // 4 is for anything else.
        if self.level < 4 {
            return self.level;
        }
        4
    }

    /// Returns the number of files in the level.
    fn count(&self) -> usize {
        self.files.len()
    }

    fn len(&self) -> usize {
        self.files.len()
    }

    fn is_empty(&self) -> bool {
        self.count() == 0
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

    /// Returns true if all files are full merged during a compaction.
    fn full_merged(&self) -> bool {
        for f in self.files.iter() {
            if !f.full_merged() {
                return false;
            }
        }
        true
    }

    /// Returns the remaining size to compact of all files.
    fn remaining_size(&self) -> u64 {
        let mut sz = 0;
        for f in self.files.iter() {
            sz += f.remaining_size();
        }
        sz
    }
}

impl PartialEq for TSMLevel {
    fn eq(&self, other: &Self) -> bool {
        self.level < other.level
    }
}

impl PartialOrd for TSMLevel {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.level.partial_cmp(&other.level)
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
struct TSMLevels(Vec<TSMLevel>); // TODO: sort by level

impl Default for TSMLevels {
    fn default() -> Self {
        Self(vec![TSMLevel::default(); 4])
    }
}

impl TSMLevels {
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

    // fn chunk(&mut self, size: i64) -> Vec<TSMGenerations> {
    //     let mut chunks = vec![];
    //     while !self.is_empty() {
    //         if self.len() >= size as usize {
    //             chunks.push(TSMGenerations(
    //                 self.drain(..size as usize).collect::<Vec<_>>(),
    //             ));
    //         } else {
    //             chunks.push(TSMGenerations(std::mem::take(self)));
    //         }
    //     }

    //     chunks
    // }

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

impl Deref for TSMLevels {
    type Target = Vec<TSMLevel>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TSMLevels {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl IntoIterator for TSMLevels {
    type IntoIter = std::vec::IntoIter<Self::Item>;
    type Item = TSMLevel;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
