//! Compactions are the process of creating read-optimized TSM files.
//! The files are created by converting write-optimized WAL entries
//! to read-optimized TSM format.  They can also be created from existing
//! TSM files when there are tombstone records that need to be removed, points
//! that were overwritten by later writes and need to updated, or multiple
//! smaller TSM files need to be merged to reduce file counts and improve
//! compression ratios.
//!
//! The compaction process is stream-oriented using multiple readers and
//! iterators.  The resulting stream is written sorted and chunked to allow for
//! one-pass writing of a new TSM file.

use std::{
    collections::{HashMap, HashSet},
    default,
    fs::{self, File},
    io::Write,
    ops::{Deref, DerefMut},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::common::{hll::hash64, sync::CancellationTokenBounded, time::Time, to_str, to_string};
use crate::models::points::{
    BOOL, EMPTY, FLOAT, FloatValue, INTEGER, STRING, UNSIGNED, Val, Value,
};
use crate::runtime::execution::{
    BoxBlockingTask, ExecutorRef, RuntimeExecutorExt, SpawnError, TaskHandle, accepted_task_guard,
    legacy_global_executor,
};
use parking_lot::{Mutex, RwLock};
use scopeguard::defer;
use snafu::{ResultExt, Snafu, ensure};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::{
    cache::Cache,
    file_store::{
        self, FileStat, FileStore, TMP_TSM_FILE_EXTENSION, default_format_file_name,
        default_parse_file_name,
    },
    reader::{self, BlockIter, TSMReader, TimeRange},
    writer::{self, TSMWriter},
};
use crate::{
    config::{DEFAULT_MAX_POINTS_PER_BLOCK, VLogConfig},
    storage::{
        limiter::{LimitWriter, SyncWrite, rate::Limiter},
        tsm1::{
            block_statistics::BlockStatistics,
            cache,
            cursor::array_values::{
                BoolArray, FloatArray, IntegerArray, StringArray, UnsignedArray,
            },
            data_encoding::{
                array_encoding::{
                    decode_bool_array_block, decode_float_array_block, decode_integer_array_block,
                    decode_string_array_block, decode_unsigned_array_block,
                },
                bool::BoolEncoder,
                encoding::{
                    self, BLOCK_BOOL, BLOCK_FLOAT64, BLOCK_INTEGER, BLOCK_STRING, BLOCK_UNSIGNED,
                    block_count, decode_bool_block, decode_float_block, decode_integer_block,
                    decode_string_block, decode_unsigned_block, encode_bool_block_using,
                    encode_float_block_using, encode_integer_block_using,
                    encode_string_block_using, encode_unsigned_block_using,
                },
                encoding_pools::{
                    BOOL_ENCODER_POOLS, FLOAT_DECODER_POOLS, FLOAT_ENCODER_POOLS,
                    INTEGER_DECODER_POOLS, INTEGER_ENCODER_POOLS, STRING_DECODER_POOLS,
                    STRING_ENCODER_POOLS, TIME_DECODER_POOLS, TIME_ENCODER_POOLS, get_bool_encoder,
                    get_float_encoder, get_integer_encoder, get_string_encoder, get_time_encoder,
                    get_unsigned_encoder, init_encoding_pools, put_bool_encoder, put_float_encoder,
                    put_integer_encoder, put_string_encoder, put_time_encoder,
                    put_unsigned_encoder,
                },
                encoding_values::{
                    self, BoolValues, FloatValues, IntegerValues, StringValues, UnsignedValues,
                    Values, encode_bool_array_block, encode_float_array_block,
                    encode_integer_array_block, encode_string_array_block,
                    encode_unsigned_array_block,
                },
                float::FloatEncoder,
                int::IntegerEncoder,
                string::StringEncoder,
                timestamp::TimeEncoder,
            },
            vlog::{
                self, VLogIndex,
                compact::VLOG_FILE_EXTENSION,
                file_store::{
                    self as vlog_file_store, FileStore as VLogFileStore, TMP_VLOG_FILE_EXTENSION,
                    VLogFilePublishMeta,
                },
                writer::VLogWriter,
            },
        },
    },
};

const DEFAULT_VLOG_WRITER_NUM: i32 = 1;
pub const MAX_TSM_FILE_SIZE: u32 = 2048 * 1024 * 1024; // 2GB

/// COMPACTION_TEMP_EXTENSION is the extension used for temporary files created during compaction.
pub const COMPACTION_TEMP_EXTENSION: &str = "tmp";
/// TSM_FILE_EXTENSION is the extension used for TSM files.
pub const TSM_FILE_EXTENSION: &str = "tsm";

static SNAPSHOT_SIZE: AtomicUsize = AtomicUsize::new(0);

/// Error type for compact.
#[derive(Debug, Snafu)]
pub enum Error {
    /// IO error.
    #[snafu(display("tsm writer error: {reason}"))]
    IO { reason: std::io::Error },

    /// Max file exceed.
    #[snafu(display("max file exceeded"))]
    MaxFileExceed,

    /// Snapshot disabled.
    #[snafu(display("snapshot disabled"))]
    SnapshotDisabled,

    /// Compactions disabled.
    #[snafu(display("compaction disabled"))]
    CompactionsDisabled,

    /// Compaction in progress.
    #[snafu(display("compaction in progress: {reason}"))]
    CompactionInProgress { reason: String },

    /// Compaction aborted.
    #[snafu(display("compaction aborted: {reason}"))]
    CompactionAborted { reason: String },

    /// Block read error.
    #[snafu(display("block read error on {file}: {reason}"))]
    BlockRead { file: String, reason: String },

    /// Encoding error.
    #[snafu(display("encoding error: {source}"))]
    Encoding { source: encoding::Error },

    /// Encoding values error.
    #[snafu(display("encoding error: {source}"))]
    EncodingValues { source: encoding_values::Error },

    /// TSM reader error.
    #[snafu(display("{}", source))]
    TSMReader { source: reader::Error },

    /// TSM Writer error.
    #[snafu(display("{}", source))]
    Writer { source: writer::Error },

    /// VLog reader error.
    #[snafu(display("{}", source))]
    VLogReader { source: vlog::reader::Error },

    /// VLog Writer error.
    #[snafu(display("{}", source))]
    VLogWriter { source: vlog::writer::Error },

    /// File store error.
    #[snafu(display("{}", source))]
    FileStore { source: file_store::Error },

    /// VLog file store error.
    #[snafu(display("{}", source))]
    VLogFileStore { source: file_store::Error },

    /// String error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },

    /// Required compaction work could not be scheduled.
    #[snafu(display("{}", source))]
    Execution { source: SpawnError },

    /// A required snapshot VLog writer ended without a normal operation result.
    #[snafu(display("snapshot VLog writer task terminated"))]
    VLogWriterTaskTerminated,

    /// A required snapshot encoding or file-writer task ended without a result.
    #[snafu(display("snapshot writer task terminated"))]
    SnapshotWriterTaskTerminated,
}

/// Result type for tsm reader.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IO { reason: err }
    }
}

impl From<file_store::Error> for Error {
    fn from(err: file_store::Error) -> Error {
        Error::FileStore { source: err }
    }
}

#[derive(Default, Debug)]
pub struct CompactionGroup(Vec<String>);

impl Deref for CompactionGroup {
    type Target = Vec<String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for CompactionGroup {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Vec<String>> for CompactionGroup {
    fn from(value: Vec<String>) -> Self {
        Self(value)
    }
}

/// DefaultPlanner using a strategy to roll up multiple generations of
/// TSM files into larger files in stages.  It attempts to minimize the number of TSM
/// files on disk while rolling up a bounder number of files.
pub struct DefaultPlanner {
    // compact_full_write_cold_duration specifies the length of time after
    // which if no writes have been committed to the WAL, the engine will
    // do a full compaction of the TSM files in this shard. This duration
    // should always be greater than the CACHE_FLUSH_WRITE_COLD_DURATION
    compact_full_write_cold_duration: Duration,

    inner: RwLock<InnerDefaultPlanner>,
}

struct InnerDefaultPlanner {
    // the last time Plan was called
    last_plan_check: Time,

    file_store: Arc<FileStore>,
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

impl DefaultPlanner {
    pub fn new(fs: Arc<FileStore>, write_cold_duration: Duration) -> Self {
        Self {
            compact_full_write_cold_duration: write_cold_duration,
            inner: RwLock::new(InnerDefaultPlanner {
                last_plan_check: Time::from_timestamp(0, 0),
                file_store: fs,
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
        inner.file_store = fs;
    }

    #[cfg(test)]
    #[cfg(feature = "mock_file_store")]
    fn set_last_modified(&self, t: Time) {
        let mut inner = self.inner.write();
        inner.file_store.set_last_modified(t);
    }

    /// Returns (generation, seq)
    pub fn parse_file_name(&self, path: &str) -> Result<(i64, i64)> {
        default_parse_file_name(path).context(FileStoreSnafu)
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

        // 000001-01.tsm, 000001-02.tsm would be in the same generation, each with different sequence numbers
        // generations = [
        //                  tsmGeneration{id=128,files=[000128-04.tsm, 00000128-05.tsm]},
        //                  tsmGeneration{id=160,files=[000160-03.tsm]},
        //                  tsmGeneration{id=192,files=[000192-03.tsm]},
        //                  tsmGeneration{id=224,files=[000224-03.tsm]},
        //                  tsmGeneration{id=288,files=[000288-02.tsm]},
        //                  tsmGeneration{id=313,files=[000313-01.tsm]},
        //                  tsmGeneration{id=314,files=[000314-01.tsm]},
        //                  tsmGeneration{id=315,files=[000315-01.tsm]},
        //               ]

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

        // groups:
        // [
        //     // group_1, all files in level-4, but the same generation in two adjacent levels are in the group
        //     TSMGenerations {
        //         TSMGeneration{id=128, files=[128-4, 128-5]},
        //     },
        //     TSMGenerations {  // group_2, all files in level-3
        //         TSMGeneration{id=160, files=[160-3]},
        //         TSMGeneration{id=192, files=[192-3]},
        //         TSMGeneration{id=224, files=[224-3]},
        //     },
        //     TSMGenerations {  // group_3, all files in level-2
        //         TSMGeneration{id=288, files=[288-2]},
        //     },
        //     TSMGenerations {  // group_4, all files in level-1
        //         TSMGeneration{id=313, files=[313-1]},
        //         TSMGeneration{id=314, files=[314-1]},
        //         TSMGeneration{id=315, files=[315-1]},
        //     },
        // ]

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
            // [tsmGeneration_1{id=1,files=[000001-03.tsm, 000002-03.tsm}, ...]
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
    /// then returns the generations in ascending order (oldest first).
    /// If skipInUse is true, tsm files that are part of an existing compaction plan
    /// are not returned.
    fn find_generations(&self, skip_in_use: bool) -> TSMGenerations {
        let mut inner = self.inner.write();

        let last = inner.last_find_generations;
        let last_gen = inner.last_generations.clone();

        if last != Time::from_timestamp(0, 0) && inner.file_store.last_modified().eq(&last) {
            return last_gen;
        }

        let gen_time = inner.file_store.last_modified();
        let tsm_stats = inner.file_store.stats(); // sorted by file name
        let mut generations = HashMap::<i64, TSMGeneration>::new();
        for f in tsm_stats {
            let path = f.path.as_os_str().to_str().unwrap();
            // Skip any files that are assigned to a current compaction plan
            if inner.files_in_use.contains(path) && skip_in_use {
                continue;
            }

            let (generation, _) = self.parse_file_name(path).unwrap();
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

    #[cfg(all(test, not(feature = "mock_compact_plan")))]
    pub(crate) fn acquire_for_test(&self, groups: &[CompactionGroup]) -> bool {
        self.acquire(groups)
    }

    #[cfg(all(test, not(feature = "mock_compact_plan")))]
    pub(crate) fn file_in_use_for_test(&self, file: &str) -> bool {
        self.inner.read().files_in_use.contains(file)
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

/// Compactor merges multiple TSM files into new files or
/// writes a Cache into 1 or more TSM files.
pub struct Compactor {
    dir: Arc<PathBuf>,
    size: i64,
    file_store: Arc<FileStore>,
    // RateLimit is the limit for disk writes for all concurrent compactions.
    rate_limit: Option<Limiter>,

    vlog_dir: Arc<PathBuf>,

    inner: Arc<RwLock<InnerCompactor>>,
    executor: ExecutorRef,
}

struct InnerCompactor {
    snapshots_enabled: bool,
    compactions_enabled: bool,

    // The following parameters are vlog associated
    vlog_enabled: bool,
    min_vlog_size: i64,
    max_vlog_writers: i32,
    // vlog_writers: Vec<Arc<RwLock<VLogWriter<File>>>>,
    vlog_file_store: Option<Arc<VLogFileStore>>,

    // the amount of time the last snapshot took to complete.
    last_snapshot_duration: Duration,

    snapshot_latencies: Latencies,

    // to signal that any in progress snapshots should be aborted.
    snapshot_interrupt: Option<CancellationToken>,
    // to signal that any in progress level compactions should be aborted.
    compactions_interrupt: Option<CancellationToken>,

    files: HashSet<String>,
}

impl Compactor {
    pub fn new(
        dir: Arc<PathBuf>,
        vlog_dir: Arc<PathBuf>,
        file_store: Arc<FileStore>,
        vlog_file_store: Option<Arc<VLogFileStore>>,
        rate_limit: Option<Limiter>,
        vlog_enabled: bool,
        // min_vlog_size: i64,
        vlog_config: &VLogConfig,
    ) -> Self {
        Self::new_with_executor(
            dir,
            vlog_dir,
            file_store,
            vlog_file_store,
            rate_limit,
            vlog_enabled,
            vlog_config,
            legacy_global_executor(),
        )
    }

    pub fn new_with_executor(
        dir: Arc<PathBuf>,
        vlog_dir: Arc<PathBuf>,
        file_store: Arc<FileStore>,
        vlog_file_store: Option<Arc<VLogFileStore>>,
        rate_limit: Option<Limiter>,
        vlog_enabled: bool,
        vlog_config: &VLogConfig,
        executor: ExecutorRef,
    ) -> Self {
        Self {
            dir,
            size: 0,
            file_store,
            rate_limit,

            vlog_dir,

            inner: Arc::new(RwLock::new(InnerCompactor {
                snapshots_enabled: false,
                compactions_enabled: false,

                vlog_enabled,
                min_vlog_size: vlog_config.min_vlog_size,
                max_vlog_writers: vlog_config.max_vlog_writers,
                // vlog_writers: vec![],
                vlog_file_store,

                last_snapshot_duration: Duration::from_secs(0),

                snapshot_latencies: Latencies::new(4),

                snapshot_interrupt: Some(CancellationToken::new()),
                compactions_interrupt: Some(CancellationToken::new()),

                files: HashSet::<String>::new(),
            })),
            executor,
        }
    }

    /// Open initializes the Compactor.
    pub fn open(&self) {
        let mut inner = self.inner.write();

        if inner.snapshots_enabled || inner.compactions_enabled {
            return;
        }

        inner.snapshots_enabled = true;
        inner.compactions_enabled = true;
        inner.snapshot_interrupt = Some(CancellationToken::new());
        inner.compactions_interrupt = Some(CancellationToken::new());
        inner.snapshot_latencies = Latencies::new(4);

        inner.files = HashSet::new();
    }

    fn open_vlog_writer(path: &Path, generation: i64) -> VLogWriter<File> {
        let file_name = path.join(
            default_format_file_name(generation, 1)
                + "."
                + VLOG_FILE_EXTENSION
                + "."
                + TMP_VLOG_FILE_EXTENSION,
        );
        let fd = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_name)
            .expect("create vlog writer failed");
        let stat = fd.metadata().expect("query vlog writer metadata  failed");
        stat.permissions().set_mode(0o666);

        // Arc::new(RwLock::new(VLogWriter::new(fd, generation, 1, file_name)))
        VLogWriter::new(fd, generation, 1, file_name)
    }

    /// Close disables the Compactor.
    pub fn close(&self) {
        let mut inner = self.inner.write();
        if !(inner.snapshots_enabled || inner.compactions_enabled) {
            // remove all empty vlog writers.
            // for w in inner.vlog_writers.iter() {
            //     let mut w = w.write();
            //     if w.size() == 0 {
            //         let _ = w.remove();
            //     }
            // }
            // return;
        }
        inner.snapshots_enabled = false;
        inner.compactions_enabled = false;
        if let Some(compactions_interrupt) = &inner.compactions_interrupt {
            compactions_interrupt.cancel(); // stop compactions in preogressing
        }
        if let Some(snapshot_interrupt) = &inner.snapshot_interrupt {
            snapshot_interrupt.cancel(); // stop snapshots in preogressing
        }
    }

    /// Disables the compactor from performing snapshots.
    pub fn disable_snapshots(&self) {
        let mut inner = self.inner.write();
        inner.snapshots_enabled = false;

        if inner.snapshot_interrupt.is_some() {
            inner.snapshot_interrupt.as_ref().unwrap().cancel(); // stop snapshots in preogressing
            inner.snapshot_interrupt = None;
        }
    }

    /// Allows the compactor to perform snapshots.
    pub fn enable_snapshots(&self) {
        let mut inner = self.inner.write();
        inner.snapshots_enabled = true;

        if inner.snapshot_interrupt.is_none() {
            inner.snapshot_interrupt = Some(CancellationToken::new());
        }
    }

    /// Disables the compactor from performing compactions.
    pub fn disable_compactions(&self) {
        let mut inner = self.inner.write();
        inner.compactions_enabled = false;

        if inner.compactions_interrupt.is_some() {
            inner.compactions_interrupt.as_ref().unwrap().cancel(); // stop compactions in preogressing
            inner.compactions_interrupt = None;
        }
    }

    /// Allows the compactor to perform compactions.
    pub fn enable_compactions(&self) {
        let mut inner: parking_lot::lock_api::RwLockWriteGuard<
            '_,
            parking_lot::RawRwLock,
            InnerCompactor,
        > = self.inner.write();
        inner.compactions_enabled = true;

        if inner.compactions_interrupt.is_none() {
            inner.compactions_interrupt = Some(CancellationToken::new());
        }
    }

    /// Writes a Cache snapshot to one or more new TSM files.
    // pub fn write_snapshot(
    //     &self,
    //     cache: Arc<Cache>,
    // ) -> Result<(
    //     Vec<String>,
    //     Vec<String>,
    //     HashMap<String, Vec<VLogIndex>>,
    //     Vec<VLogFilePublishMeta>,
    // )> {
    //     let (enabled, intc, vlog_enabled) = {
    //         let inner = self.inner.read();
    //         (
    //             inner.snapshots_enabled,
    //             inner.snapshot_interrupt.clone(),
    //             inner.vlog_enabled,
    //         )
    //     };

    //     ensure!(enabled, SnapshotDisabledSnafu);

    //     let start = std::time::Instant::now();
    //     let card = cache.count(); // number of series keys in cache

    //     // Enable throttling if we have lower cardinality or snapshots are going fast.
    //     let mut throttle = card < 3e6 as i64
    //         && self.inner.read().snapshot_latencies.avg() < Duration::from_secs(15);

    //     // Write snapshost concurrently if cardinality is relatively high.
    //     let mut concurrency = (card / (2e6 as i64)) as usize;
    //     if concurrency < 1 {
    //         concurrency = 1;
    //     }

    //     // See if we were disabled while writing a snapshot
    //     if card >= 3e6 as i64 {
    //         concurrency = 4;
    //         throttle = false;
    //     }

    //     let mut splits = cache::Cache::split(cache, concurrency as i32).into_iter(); // split 16 partitions in the ring using idx%concurrency

    //     println!(
    //         "-------------------------- write_snapshot concurrency: {}",
    //         concurrency
    //     );

    //     struct Res {
    //         tsm_files: Vec<String>,
    //         vlog_files: Vec<String>,
    //         vlog_indexes: HashMap<String, Vec<VLogIndex>>,
    //         err: Option<Error>,
    //     }

    //     // FIXME: 可以改成oneshot
    //     let (s, r) = crossbeam_channel::bounded::<Res>(concurrency);
    //     // Special case very high cardinality, use max concurrency and don't throttle writes.
    //     for _ in 0..concurrency {
    //         let s = s.clone();
    //         let cache = splits.next().unwrap();
    //         let intc = intc.clone();
    //         let dir = Arc::clone(&self.dir);
    //         let vlog_dir = Arc::clone(&self.vlog_dir);
    //         let inner = Arc::clone(&self.inner);
    //         let generation = self.file_store.next_generation();
    //         let rate_limit = self.rate_limit.clone();

    //         spawn_blocking_compact(move || {
    //             let (tx, rx) = tokio::sync::oneshot::channel();
    //             spawn_compact(async move {
    //                 let iter =
    //                     CacheKeyIter::new(cache, DEFAULT_MAX_POINTS_PER_BLOCK as usize, intc).await;
    //                 let _ = tx.send(iter);
    //             });
    //             let mut iter = block_on_compact(rx).expect("create cache key iterator failed");

    //             match Compactor::write_new_files(
    //                 inner,
    //                 dir,
    //                 vlog_dir,
    //                 generation,
    //                 0,
    //                 rate_limit,
    //                 &[],
    //                 &mut iter,
    //                 throttle,
    //                 true,
    //                 vlog_enabled,
    //             ) {
    //                 Ok((tsm_files, vlog_files, vlog_indexes)) => {
    //                     let _ = s.send(Res {
    //                         tsm_files,
    //                         vlog_files,
    //                         vlog_indexes,
    //                         err: None,
    //                     });
    //                 }
    //                 Err(e) => {
    //                     let _ = s.send(Res {
    //                         tsm_files: vec![],
    //                         vlog_files: vec![],
    //                         vlog_indexes: HashMap::new(),
    //                         err: Some(e),
    //                     });
    //                 }
    //             }
    //         });
    //     }

    //     let mut err: Option<Error> = None;
    //     let mut tsm_files = vec![];
    //     let mut vlog_files = vec![];
    //     let mut vlog_indexes = HashMap::new();
    //     // Check for error
    //     for _ in 0..concurrency {
    //         if let Ok(mut res) = r.recv() {
    //             if let Some(e) = res.err {
    //                 err = Some(e);
    //             }
    //             if !res.tsm_files.is_empty() {
    //                 tsm_files.append(&mut res.tsm_files);
    //             }
    //             if !res.vlog_files.is_empty() {
    //                 vlog_files.append(&mut res.vlog_files);
    //             }
    //             if !res.vlog_indexes.is_empty() {
    //                 for (k, mut indexes) in res.vlog_indexes {
    //                     vlog_indexes
    //                         .entry(k)
    //                         .or_insert_with(Vec::new)
    //                         .append(&mut indexes);
    //                 }
    //             }
    //         }
    //     }

    //     // FIXME:
    //     let dur = {
    //         let elapsed = start.elapsed();
    //         elapsed - elapsed / (Duration::from_secs(1).as_nanos() as u32)
    //     };

    //     let mut inner = self.inner.write();
    //     // See if we were disabled while writing a snapshot
    //     let enabled = inner.snapshots_enabled;
    //     inner.last_snapshot_duration = dur;
    //     inner.snapshot_latencies.add(start.elapsed());
    //     drop(inner);

    //     if !enabled {
    //         return Err(Error::SnapshotDisabled);
    //     }

    //     if let Some(err) = err {
    //         return Err(err);
    //     }

    //     println!("@@@@ vlog_files: {:?}", vlog_files);
    //     println!("@@@@ vlog_index.len: {}", vlog_indexes.len());

    //     let vlog_publish_metadata = if vlog_enabled {
    //         Self::build_vlog_publish_metadata(&vlog_files, &vlog_indexes)?
    //     } else {
    //         vec![]
    //     };

    //     Ok((tsm_files, vlog_files, vlog_indexes, vlog_publish_metadata))
    // }

    pub async fn write_snapshot(
        &self,
        cache: Arc<Cache>,
    ) -> Result<(
        Vec<String>,
        Vec<String>,
        HashMap<String, Vec<VLogIndex>>,
        Vec<VLogFilePublishMeta>,
    )> {
        let (enabled, intc, vlog_enabled) = {
            let inner = self.inner.read();
            (
                inner.snapshots_enabled,
                inner.snapshot_interrupt.clone(),
                inner.vlog_enabled,
            )
        };

        ensure!(enabled, SnapshotDisabledSnafu);

        let start = std::time::Instant::now();
        let card = cache.count(); // number of series keys in cache

        // Enable throttling if we have lower cardinality or snapshots are going fast.
        let mut throttle = card < 3e6 as i64
            && self.inner.read().snapshot_latencies.avg() < Duration::from_secs(15);

        // Write snapshost concurrently if cardinality is relatively high.
        let mut concurrency = (card / (2e6 as i64)) as usize;
        if concurrency < 1 {
            concurrency = 1;
        }

        // See if we were disabled while writing a snapshot
        if card >= 3e6 as i64 {
            concurrency = 4;
            throttle = false;
        }

        let splits = cache::Cache::split(cache, concurrency as i32);
        let mut iterators = Vec::with_capacity(concurrency);
        for cache in splits {
            iterators.push(
                CacheKeyIter::new_with_executor(
                    cache,
                    DEFAULT_MAX_POINTS_PER_BLOCK as usize,
                    intc.clone(),
                    Arc::clone(&self.executor),
                )
                .await?,
            );
        }

        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let mut producer_tasks = SnapshotTaskSet::default();
        let mut err = None;
        for mut iter in iterators {
            let dir = Arc::clone(&self.dir);
            let vlog_dir = Arc::clone(&self.vlog_dir);
            let inner = Arc::clone(&self.inner);
            let generation = self.file_store.next_generation();
            let rate_limit = self.rate_limit.clone();
            let event_tx = event_tx.clone();

            match self.executor.spawn_blocking_compact(move || {
                Compactor::write_new_files(
                    inner,
                    dir,
                    vlog_dir,
                    generation,
                    0,
                    rate_limit,
                    &[],
                    &mut iter,
                    throttle,
                    true,
                    vlog_enabled,
                    Some(event_tx),
                )
            }) {
                Ok(task) => producer_tasks.push(task),
                Err(source) => err = Some(Error::Execution { source }),
            }
        }
        drop(event_tx);

        let mut vlog_workers = SnapshotVLogWorkers::default();
        let mut vlog_results = Vec::new();
        while let Some(event) = event_rx.recv().await {
            match event {
                SnapshotVLogEvent::Start(batch) => {
                    if let Err(error) = vlog_workers.start(batch, &self.executor) {
                        err = Some(error);
                    }
                }
                SnapshotVLogEvent::Result(result) => vlog_results.push(result),
            }
        }

        let mut tsm_files = vec![];
        let mut vlog_files = HashSet::new();
        let mut vlog_indexes = HashMap::new();
        for task in producer_tasks.take() {
            match task.await {
                Ok(Ok((mut files, _, _))) => tsm_files.append(&mut files),
                Ok(Err(error)) => err = Some(error),
                Err(_) => err = Some(Error::SnapshotWriterTaskTerminated),
            }
        }

        vlog_workers.finish_input();
        if let Err(error) = vlog_workers.wait_ready().await {
            err = Some(error);
        }
        for result in vlog_results {
            match result.await {
                Ok(WriteVLogRes {
                    key,
                    write_vlog_res,
                    old_file,
                    vlog_index,
                    ..
                }) => {
                    if let Err(error) = write_vlog_res {
                        err = Some(error);
                    }
                    if let Some(file) = old_file {
                        vlog_files.insert(file);
                    }
                    if let Some(index) = vlog_index {
                        vlog_indexes
                            .entry(to_string!(key.to_vec()))
                            .or_insert_with(Vec::new)
                            .push(index);
                    }
                }
                Err(_) => err = Some(Error::VLogWriterTaskTerminated),
            }
        }
        if let Err(error) = vlog_workers.shutdown().await {
            err = Some(error);
        }
        let vlog_files = vlog_files.into_iter().collect::<Vec<_>>();

        // FIXME:
        let dur = {
            let elapsed = start.elapsed();
            elapsed - elapsed / (Duration::from_secs(1).as_nanos() as u32)
        };

        let mut inner = self.inner.write();
        // See if we were disabled while writing a snapshot
        let enabled = inner.snapshots_enabled;
        inner.last_snapshot_duration = dur;
        inner.snapshot_latencies.add(start.elapsed());
        drop(inner);

        if !enabled {
            return Err(Error::SnapshotDisabled);
        }

        if let Some(error) = err {
            return Err(error);
        }

        let vlog_publish_metadata = if vlog_enabled {
            Self::build_vlog_publish_metadata(&vlog_files, &vlog_indexes)?
        } else {
            vec![]
        };

        Ok((tsm_files, vlog_files, vlog_indexes, vlog_publish_metadata))
    }

    fn build_vlog_publish_metadata(
        vlog_files: &[String],
        vlog_indexes: &HashMap<String, Vec<VLogIndex>>,
    ) -> Result<Vec<VLogFilePublishMeta>> {
        let mut paths = HashMap::new();
        for file in vlog_files {
            let (generation, seq) =
                vlog_file_store::default_parse_file_name(file).map_err(|e| Error::StringErr {
                    reason: e.to_string(),
                })?;
            paths.insert((generation, seq), PathBuf::from(file));
        }

        let mut grouped = HashMap::<(i64, i64), HashMap<Vec<u8>, Vec<VLogIndex>>>::new();
        for (key, indexes) in vlog_indexes {
            for idx in indexes {
                grouped
                    .entry((idx.generation, idx.seq))
                    .or_default()
                    .entry(key.as_bytes().to_vec())
                    .or_default()
                    .push(idx.clone());
            }
        }

        let mut metadata = Vec::with_capacity(paths.len());
        for ((generation, seq), path) in paths {
            let Some(keys_for_file) = grouped.remove(&(generation, seq)) else {
                continue;
            };

            let mut entries = keys_for_file.into_iter().collect::<Vec<_>>();
            entries.sort_by(|a, b| a.0.cmp(&b.0));

            let mut keys = Vec::with_capacity(entries.len());
            let mut indexes = Vec::with_capacity(entries.len());
            let mut min_time = i64::MAX;
            let mut max_time = i64::MIN;
            let mut index_count = 0;

            for (key, mut key_indexes) in entries {
                key_indexes.sort_by(|a, b| a.min.cmp(&b.min).then_with(|| a.max.cmp(&b.max)));
                if let Some(first) = key_indexes.first() {
                    keys.push((key, first.typ));
                } else {
                    continue;
                }
                for idx in &key_indexes {
                    min_time = min_time.min(idx.min);
                    max_time = max_time.max(idx.max);
                }
                index_count += key_indexes.len();
                indexes.push(key_indexes);
            }

            if keys.is_empty() {
                continue;
            }

            metadata.push(VLogFilePublishMeta {
                path,
                generation,
                seq,
                min_key: keys.first().map(|(key, _)| key.clone()),
                max_key: keys.last().map(|(key, _)| key.clone()),
                keys,
                indexes,
                min_time: Some(min_time),
                max_time: Some(max_time),
                index_count,
            });
        }

        Ok(metadata)
    }

    /// compact writes multiple smaller TSM files into 1 or more larger files.
    fn compact(&self, fast: bool, tsm_files: &[String]) -> Result<Vec<String>> {
        let mut size = self.size;
        if size <= 0 {
            size = DEFAULT_MAX_POINTS_PER_BLOCK;
        }

        let intc = self.inner.read().compactions_interrupt.clone();

        // The new compacted files need to added to the max generation in the
        // set.  We need to find that max generation as well as the max sequence
        // number to ensure we write to the next unique location.
        let (mut max_generation, mut max_sequence) = (0, 0);
        for f in tsm_files {
            let (generation, seq) = default_parse_file_name(f)?;

            if generation > max_generation {
                max_generation = generation;
                max_sequence = seq;
            }

            if generation == max_generation && seq > max_sequence {
                max_sequence = seq;
            }
        }

        // For each TSM file, create a TSM reader
        let mut trs: Vec<Arc<TSMReader>> = vec![];
        for file in tsm_files {
            if let Some(intc) = &intc {
                // select! {
                //     recv(intc.receiver()) -> _ => return Err(Error::CompactionAborted { reason: String::new() }),
                //     default => {},
                // }
                if intc.is_cancelled() {
                    for tr in &trs {
                        tr.unref_reader();
                    }
                    return Err(Error::CompactionAborted {
                        reason: String::new(),
                    });
                }
            }

            let tr = {
                #[cfg(feature = "mock_file_store")]
                {
                    let (generation, seq) = default_parse_file_name(file)?;
                    self.file_store.tsm_reader(file, generation, seq)
                }

                #[cfg(not(feature = "mock_file_store"))]
                {
                    self.file_store.tsm_reader(file)
                }
            };

            let tr = match tr {
                Some(r) => r,
                None => {
                    for tr in &trs {
                        tr.unref_reader();
                    }
                    return Err(Error::CompactionAborted {
                        reason: format!("compact bad plan: {:?}", file),
                    });
                }
            };

            trs.push(tr);
        }

        if trs.is_empty() {
            return Ok(vec![]);
        }

        let mut tsm = match TSMBatchKeyIter::new(size, fast, intc, tsm_files.to_vec(), &trs) {
            Ok(itr) => itr,
            Err(e) => {
                for tr in &trs {
                    tr.unref_reader();
                }
                return Err(e);
            }
        };

        let inner = Arc::clone(&self.inner);
        match Compactor::write_new_files(
            inner,
            Arc::clone(&self.dir),
            Arc::clone(&self.vlog_dir),
            max_generation,
            max_sequence,
            self.rate_limit.clone(),
            tsm_files,
            &mut tsm,
            true,
            false,
            false,
            None,
        ) {
            Ok((new_files, _, _)) => {
                for tr in &trs {
                    tr.unref_reader();
                }
                Ok(new_files)
            }
            Err(e) => {
                for tr in &trs {
                    tr.unref_reader();
                }
                Err(e)
            }
        }
    }

    /// Writes multiple smaller TSM files into 1 or more larger files.
    pub fn compact_full(&self, tsm_files: &[String]) -> Result<Vec<String>> {
        let enabled = self.inner.read().compactions_enabled;

        if !enabled {
            return Err(Error::CompactionsDisabled);
        }

        if !self.add(tsm_files) {
            return Err(Error::CompactionInProgress {
                reason: "".to_string(),
            });
        }

        let files = self.compact(false, tsm_files)?;

        // See if we were disabled while writing a snapshot
        let enabled = self.inner.read().compactions_enabled;

        if !enabled {
            if let Err(e) = self.remove_tmp_files(&files) {
                self.remove(tsm_files);
                return Err(e);
            }

            self.remove(tsm_files);
            return Err(Error::CompactionsDisabled);
        }

        self.remove(tsm_files);

        Ok(files)
    }

    /// Writes multiple smaller TSM files into 1 or more larger files.
    pub fn compact_fast(&self, tsm_files: &[String]) -> Result<Vec<String>> {
        let enabled = self.inner.read().compactions_enabled;

        if !enabled {
            return Err(Error::CompactionsDisabled);
        }

        if !self.add(tsm_files) {
            return Err(Error::CompactionInProgress {
                reason: "".to_string(),
            });
        }

        let files = self.compact(true, tsm_files)?;

        // See if we were disabled while writing a snapshot
        let enabled = self.inner.read().compactions_enabled;

        if !enabled {
            if let Err(e) = self.remove_tmp_files(&files) {
                self.remove(tsm_files);
                return Err(e);
            }

            self.remove(tsm_files);
            return Err(Error::CompactionsDisabled);
        }

        self.remove(tsm_files);

        Ok(files)
    }

    // removeTmpFiles is responsible for cleaning up a compaction that
    // was started, but then abandoned before the temporary files were dealt with.
    fn remove_tmp_files(&self, files: &[String]) -> Result<()> {
        for f in files {
            fs::remove_file(f)?;
        }

        Ok(())
    }

    // Returns the new tsm compact files, vlog compact indexes, and vlog compact files.
    fn write_new_files(
        inner: Arc<RwLock<InnerCompactor>>,
        dir: Arc<PathBuf>,
        vlog_dir: Arc<PathBuf>,
        generation: i64,
        mut sequence: i64,
        rate_limit: Option<Limiter>,
        _src: &[String],
        iter: &mut impl KeyIterator,
        throttle: bool,
        is_write_snapshot: bool,
        vlog_enabled: bool,
        vlog_events: Option<mpsc::UnboundedSender<SnapshotVLogEvent>>,
    ) -> Result<(Vec<String>, Vec<String>, HashMap<String, Vec<VLogIndex>>)> {
        /*
            // 1. vlog_enabled && write_snapshot
            (tsm_files, Vec<old_vlog_file>, HashMap<key->vlog_indexes>)

            // 2. vlog_disabled && write_snapshot
            (tsm_files, HashMap::new())

            // 3. compact tsm
            (tsm_files, HashMap::new())
        */

        // These are the new TSM files written
        let mut new_tsm_files = vec![];
        let mut new_vlog_files = vec![];
        let mut new_vlog_indexes = HashMap::<String, Vec<VLogIndex>>::new();
        loop {
            sequence += 1;

            // New TSM files are written to a temp file and renamed when fully completed.
            let file_name = if vlog_enabled {
                dir.join(
                    default_format_file_name(generation, sequence)
                        + "."
                        + VLOG_FILE_EXTENSION
                        + "."
                        + TMP_VLOG_FILE_EXTENSION,
                )
            } else {
                dir.join(
                    default_format_file_name(generation, sequence)
                        + "."
                        + TSM_FILE_EXTENSION
                        + "."
                        + TMP_TSM_FILE_EXTENSION,
                )
            };

            match Self::write(
                Arc::clone(&vlog_dir),
                Arc::clone(&inner),
                generation,
                sequence,
                &file_name,
                rate_limit.clone(),
                iter,
                throttle,
                is_write_snapshot,
                vlog_enabled,
                vlog_events.clone(),
            ) {
                Ok(WriteFileRes {
                    write_tsm_res,
                    write_vlog_res,
                    mut vlog_files,
                    vlog_indexes,
                }) => {
                    match write_tsm_res {
                        Ok(_) => {
                            match write_vlog_res {
                                Ok(_) => {
                                    // tsm 和 vlog 都写成功
                                    if is_write_snapshot && vlog_enabled {
                                        // write snapshot to vlog
                                        new_tsm_files.push(file_name.to_str().unwrap().to_string());
                                        new_vlog_files.append(&mut vlog_files);
                                        for (key, mut indexes) in vlog_indexes {
                                            new_vlog_indexes
                                                .entry(key)
                                                .or_insert_with(Vec::new)
                                                .append(&mut indexes);
                                        }
                                    } else {
                                        // compact tsm
                                        new_tsm_files.push(file_name.to_str().unwrap().to_string());
                                    }

                                    break;
                                }
                                Err(e) => {
                                    // write tsm success, write vlog failed
                                    match e {
                                        // snapshot to vlog will never occur MaxFileExceed / MaxBlocksExceeded
                                        // but compact vlog will occur MaxFileExceed / MaxBlocksExceeded
                                        Error::MaxFileExceed => {
                                            // snapshot to tsm or compact tsm
                                            new_tsm_files
                                                .push(file_name.to_str().unwrap().to_string());
                                            continue;
                                        }
                                        Error::Writer {
                                            source:
                                                writer::Error::MaxBlocksExceeded { offset, size },
                                        } => {
                                            // write success, continue to next tsm file.
                                        }
                                        Error::Writer {
                                            source: writer::Error::NoValues,
                                        } => {
                                            // write tsm and vlog success, break

                                            // If the file only contained tombstoned entries, then it
                                            // would be a 0 length file that we can drop.
                                            if file_name.exists() {
                                                fs::remove_file(&file_name)?;
                                            }
                                            break;
                                        }
                                        Error::CompactionInProgress { reason: _ } => {
                                            // Don't clean up the file as another compaction is using it.
                                            // This should not happen as the planner keeps track of which
                                            // files are assigned to compaction plans now.
                                            return Ok((vec![], vec![], HashMap::new()));
                                        }
                                        _ => {
                                            new_vlog_files.append(&mut vlog_files);
                                            // Remove any tmp files we already completed
                                            for f in new_tsm_files {
                                                fs::remove_file(&f)?;
                                            }
                                            for f in new_vlog_files {
                                                fs::remove_file(&f)?;
                                            }
                                            // We hit an error and didn't finish the compaction.  Remove the temp file and abort.
                                            match std::fs::remove_file(file_name) {
                                                Ok(()) => {}
                                                Err(err)
                                                    if err.kind()
                                                        == std::io::ErrorKind::NotFound => {}
                                                Err(err) => return Err(err.into()),
                                            }

                                            // FIXME: Remove vlog file?
                                            return Err(e);
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            // write_tsm_res
                            match &e {
                                Error::MaxFileExceed => {
                                    // 写成功，需要continue下一个 tsm file
                                    if is_write_snapshot {
                                        // write snpshot to tsm
                                        new_tsm_files.push(file_name.to_str().unwrap().to_string());
                                        new_vlog_files.append(&mut vlog_files);
                                        for (key, mut indexes) in vlog_indexes {
                                            new_vlog_indexes
                                                .entry(key)
                                                .or_insert_with(Vec::new)
                                                .append(&mut indexes);
                                        }
                                    } else {
                                        // compact tsm
                                        new_tsm_files.push(file_name.to_str().unwrap().to_string());
                                    }
                                    continue;
                                }
                                Error::Writer { source } => {
                                    match source {
                                        writer::Error::MaxBlocksExceeded { offset, size } => {
                                            // 写成功，需要continue下一个 tsm file
                                            if is_write_snapshot {
                                                // write snpshot to tsm
                                                new_tsm_files
                                                    .push(file_name.to_str().unwrap().to_string());
                                                new_vlog_files.append(&mut vlog_files);
                                                for (key, mut indexes) in vlog_indexes {
                                                    new_vlog_indexes
                                                        .entry(key)
                                                        .or_insert_with(Vec::new)
                                                        .append(&mut indexes);
                                                }
                                            } else {
                                                // compact tsm
                                                new_tsm_files
                                                    .push(file_name.to_str().unwrap().to_string());
                                            }
                                            continue;
                                        }
                                        writer::Error::NoValues => {
                                            // write tsm and vlog success, break.

                                            // If the file only contained tombstoned entries, then it
                                            // would be a 0 length file that we can drop.
                                            if file_name.exists() {
                                                fs::remove_file(&file_name)?;
                                            }
                                            if is_write_snapshot && vlog_enabled {
                                                // write snpshot to tsm
                                                // new_tsm_files
                                                //     .push(file_name.to_str().unwrap().to_string());
                                                new_vlog_files.append(&mut vlog_files);
                                                for (key, mut indexes) in vlog_indexes {
                                                    new_vlog_indexes
                                                        .entry(key)
                                                        .or_insert_with(Vec::new)
                                                        .append(&mut indexes);
                                                }
                                            } else {
                                                // compact tsm
                                                // new_tsm_files
                                                //     .push(file_name.to_str().unwrap().to_string());
                                            }
                                            break;
                                        }
                                        _ => {
                                            // 写失败，清理文件，return error
                                            if is_write_snapshot {
                                                // write snpshot to tsm
                                                new_vlog_files.append(&mut vlog_files);
                                                // Remove any tmp files we already completed
                                                for f in new_tsm_files {
                                                    fs::remove_file(&f)?;
                                                }
                                                for f in new_vlog_files {
                                                    fs::remove_file(&f)?;
                                                }
                                            } else {
                                                // compact tsm
                                                for f in new_tsm_files {
                                                    fs::remove_file(&f)?;
                                                }
                                            }

                                            // We hit an error and didn't finish the compaction.  Remove the temp file and abort.
                                            match std::fs::remove_file(file_name) {
                                                Ok(()) => {}
                                                Err(err)
                                                    if err.kind()
                                                        == std::io::ErrorKind::NotFound => {}
                                                Err(err) => return Err(err.into()),
                                            }

                                            // FIXME: Remove vlog file?
                                            return Err(e);
                                        }
                                    }
                                }
                                Error::CompactionInProgress { reason: _ } => {
                                    // Don't clean up the file as another compaction is using it.  This should not happen as the
                                    // planner keeps track of which files are assigned to compaction plans now.
                                    return Ok((vec![], vec![], HashMap::new()));
                                }
                                _ => {
                                    // 写失败，清理文件，return error
                                    if is_write_snapshot {
                                        // write snpshot to tsm
                                        new_vlog_files.append(&mut vlog_files);
                                        // Remove any tmp files we already completed
                                        for f in new_tsm_files {
                                            fs::remove_file(&f)?;
                                        }
                                        for f in new_vlog_files {
                                            fs::remove_file(&f)?;
                                        }
                                    } else {
                                        // compact tsm
                                        for f in new_tsm_files {
                                            fs::remove_file(&f)?;
                                        }
                                    }

                                    // We hit an error and didn't finish the compaction.  Remove the temp file and abort.
                                    match std::fs::remove_file(file_name) {
                                        Ok(()) => {}
                                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                                        Err(err) => return Err(err.into()),
                                    }

                                    // FIXME: Remove vlog file?
                                    return Err(e);
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    // Self::write error
                    // Remove any tmp files we already completed
                    for f in new_tsm_files {
                        fs::remove_file(&f)?;
                    }
                    for f in new_vlog_files {
                        fs::remove_file(&f)?;
                    }
                    // We hit an error and didn't finish the compaction.  Remove the temp file and abort.
                    fs::remove_file(file_name)?;

                    // TODO: Remove vlog file?
                    return Err(e);
                }
            }
        }

        Ok((new_tsm_files, new_vlog_files, new_vlog_indexes))
    }

    fn add(&self, files: &[String]) -> bool {
        let mut inner = self.inner.write();

        // See if the new files are already in use
        for f in files {
            if inner.files.contains(f) {
                return false;
            }
        }

        // Mark all the new files in use
        for f in files {
            inner.files.insert(f.clone());
        }

        true
    }

    fn remove(&self, files: &[String]) {
        let mut inner = self.inner.write();
        for f in files {
            let _ = inner.files.remove(f);
        }
    }

    // (tsm_res, vlog_res)
    fn write(
        vlog_dir: Arc<PathBuf>,
        inner: Arc<RwLock<InnerCompactor>>,
        generation: i64,
        seq: i64,
        filename: &Path,
        rate_limit: Option<Limiter>,
        iter: &mut impl KeyIterator,
        throttle: bool,
        is_write_snapshot: bool,
        vlog_enabled: bool,
        vlog_events: Option<mpsc::UnboundedSender<SnapshotVLogEvent>>,
    ) -> Result<WriteFileRes> {
        let fd = match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename)
        {
            Ok(fd) => match fd.metadata() {
                Ok(stat) => {
                    stat.permissions().set_mode(0o666);
                    fd
                }
                Err(e) => {
                    return Err(Error::IO { reason: e });
                }
            },
            Err(e) => {
                return Err(Error::CompactionInProgress {
                    reason: e.to_string(),
                });
            }
        };

        let throttle = if cfg!(feature = "mock_file_store") {
            false
        } else {
            throttle
        };
        let rate_limit = if cfg!(feature = "mock_file_store") {
            None
        } else {
            rate_limit
        };

        // syncingWriter ensures that whatever we wrap the above file descriptor in
        // it will always be able to be synced by the tsm writer, since it does
        // type assertions to attempt to sync.
        if rate_limit.is_some() && throttle {
            let limiter_writer =
                LimitWriter::new_with_rate_limiter(filename.to_path_buf(), fd, rate_limit);
            let mut w = if iter.estimated_index_size() > 64 * 1024 * 1024 {
                TSMWriter::with_disk_buffer(limiter_writer, filename, generation, seq)
                    .context(WriterSnafu)?
            } else {
                TSMWriter::new(limiter_writer, generation, seq, filename)
            };

            Self::write_file(
                vlog_dir,
                inner,
                &mut w,
                iter,
                is_write_snapshot,
                vlog_enabled,
                vlog_events,
            )
        } else {
            let mut w = if iter.estimated_index_size() > 64 * 1024 * 1024 {
                TSMWriter::with_disk_buffer(fd, filename, generation, seq).context(WriterSnafu)?
            } else {
                TSMWriter::new(fd, generation, seq, filename)
            };
            Self::write_file(
                vlog_dir,
                inner,
                &mut w,
                iter,
                is_write_snapshot,
                vlog_enabled,
                vlog_events,
            )
        }
    }

    fn init_write_vlog(
        max_vlog_writers: i32,
        path: Arc<PathBuf>,
        vlog_file_store: &Arc<VLogFileStore>,
        events: &mpsc::UnboundedSender<SnapshotVLogEvent>,
    ) -> Result<SnapshotVLogDispatch> {
        let mut starts = Vec::new();
        let mut dispatch = SnapshotVLogDispatch::default();
        for _ in 0..max_vlog_writers {
            let generation = vlog_file_store.next_generation();
            let path = Arc::clone(&path);
            let (tx, rx) = crossbeam_channel::unbounded::<WriteVLogOp>();
            let shutdown = CancellationToken::new();
            let task_shutdown = shutdown.clone();
            let vlog_file_store = Arc::clone(vlog_file_store);
            let (ready_tx, ready_rx) = oneshot::channel();

            let task: BoxBlockingTask = Box::new(move || {
                let mut vw = Self::open_vlog_writer(path.as_path(), generation);
                if ready_tx.send(()).is_err() {
                    if vw.size() == 0 {
                        let _ = vw.remove();
                    }
                    return;
                }
                loop {
                    crossbeam_channel::select! {
                        recv(rx) -> op => {
                            match op {
                                Ok(op) => {
                                    #[cfg(test)]
                                    if op.panic_for_test {
                                        panic!("injected snapshot VLog writer panic");
                                    }
                                    // let mut vw = vw.write();
                                    // Write the key and value
                                    match vw.write_block(&op.key, op.min_time, op.max_time, &op.block) {
                                        Ok((offset, size)) => {
                                            let (generation, seq) = vw.get_id();
                                            let vlog_index = VLogIndex {
                                                aggregate: None,
                                                count: op.count,
                                                typ: op.typ,
                                                min: op.min_time,
                                                max: op.max_time,
                                                generation,
                                                seq,
                                                offset,
                                                size,
                                            };

                                            // If we have a max file size configured and we're over it,
                                            // close out the file and return the error.
                                            if vw.size() > MAX_TSM_FILE_SIZE {
                                                // if let Err(e) = vw.write_index() {
                                                //     op.notify.send(WriteVLogRes {
                                                //         key: op.key,
                                                //         write_vlog_res: close_file(
                                                //             &mut vw,
                                                //             Err(Error::Writer { source: e }),
                                                //         ),
                                                //         need_close_tsm_writer: true,
                                                //         old_file: None,
                                                //         vlog_index: None,
                                                //     }).expect("write vlog task channel closed");
                                                //     return;
                                                // }

                                                if let Err(e) = close_vlog_file(&mut vw, Ok(())) {
                                                    if op.notify.send(WriteVLogRes {
                                                        key: op.key,
                                                        write_vlog_res: Err(e),
                                                        need_close_tsm_writer: true,
                                                        old_file: None,
                                                        vlog_index: None,
                                                    }).is_err() {
                                                        return;
                                                    }
                                                    return;
                                                }

                                                // Create new file.
                                                let (generation, seq) = vw.get_id();
                                                match Self::close_and_open_new_vlog_writer(&mut vw, generation, seq, &path, Arc::clone(&vlog_file_store)) {
                                                    Ok((old_name, new_vw)) => {
                                                        vw = new_vw;

                                                        if op.notify.send( WriteVLogRes {
                                                            key: op.key,
                                                            write_vlog_res: Ok(()),
                                                            need_close_tsm_writer: false,
                                                            old_file: Some(old_name),
                                                            vlog_index: Some(vlog_index),
                                                        }).is_err() {
                                                            return;
                                                        }
                                                    }
                                                    Err(e) => {
                                                        // let _ = close_file(w, Ok(()));
                                                        // return WriteFileRes {
                                                        //     write_vlog_res: Err(e),
                                                        //     vlog_files,
                                                        //     ..Default::default()
                                                        // };

                                                        // FIXME: need check
                                                        // op.notify.send(WriteVLogRes {
                                                        //     key: op.key,
                                                        //     write_vlog_res:  Err(e),
                                                        //     need_close_tsm_writer: true,
                                                        //     old_file: None,
                                                        //     vlog_index: None,
                                                        // });
                                                        return;
                                                    }
                                                }
                                            } else {
                                                if op.notify.send( WriteVLogRes {
                                                    key: op.key,
                                                    write_vlog_res: Ok(()),
                                                    need_close_tsm_writer: false,
                                                    old_file: Some(vw.path().to_str().unwrap().to_string()),
                                                    vlog_index: Some(vlog_index),
                                                }).is_err() {
                                                    return;
                                                }
                                            };
                                        }
                                        Err(e) => match &e {
                                            vlog::writer::Error::MaxBlocksExceeded { offset, size } => {
                                                // Note: When write to vlog, MaxBlocksExceeded is not error, just write_index,
                                                // close file, save vlog file name and create new vlog writer
                                                // if let Err(e) = vw.write_index() {
                                                //     // let _ = close_file(w, Ok(()));
                                                //     // return WriteFileRes {
                                                //     //     write_vlog_res: Err(close_file(
                                                //     //         &mut vw,
                                                //     //         Err(Error::Writer { source: e }),
                                                //     //     )
                                                //     //     .err()
                                                //     //     .unwrap()),
                                                //     //     vlog_files,
                                                //     //     ..Default::default()
                                                //     // };

                                                //     op.notify.send(WriteVLogRes {
                                                //         key: op.key,
                                                //         write_vlog_res: close_file(
                                                //             &mut vw,
                                                //             Err(Error::Writer { source: e }),
                                                //         ),
                                                //         need_close_tsm_writer: true,
                                                //         old_file: None,
                                                //         vlog_index: None,
                                                //     }).expect("write vlog task channel closed");
                                                //     return;
                                                // }

                                                let (generation, seq) = vw.get_id();
                                                let vlog_index = VLogIndex {
                                                    aggregate: None,
                                                    count: op.count,
                                                    typ: op.typ,
                                                    min: op.min_time,
                                                    max: op.max_time,
                                                    generation,
                                                    seq,
                                                    offset: *offset,
                                                    size: *size,
                                                };

                                                match Self::close_and_open_new_vlog_writer(
                                                    &mut vw, generation, seq, &path, Arc::clone(&vlog_file_store),
                                                ) {
                                                    Ok((old_name, new_vw)) => {
                                                        vw = new_vw;

                                                        if op.notify.send(WriteVLogRes {
                                                            key: op.key,
                                                            write_vlog_res: Ok(()),
                                                            need_close_tsm_writer: false,
                                                            old_file: Some(old_name),
                                                            vlog_index: Some(vlog_index),
                                                        }).is_err() {
                                                            return;
                                                        }
                                                    }
                                                    Err(e) => {
                                                        // return WriteFileRes {
                                                        //     write_vlog_res: Err(e),
                                                        //     vlog_files,
                                                        //     ..Default::default()
                                                        // };

                                                        if op.notify.send(WriteVLogRes {
                                                            key: op.key,
                                                            write_vlog_res: Err(e),
                                                            need_close_tsm_writer: false,
                                                            old_file: None,
                                                            vlog_index: None,
                                                        }).is_err() {
                                                            return;
                                                        }
                                                        return;
                                                    }
                                                }
                                            }
                                            _ => {
                                                // IOError / MaxKeyLengthExceeded
                                                // let _ = close_file(w, Ok(()));
                                                // return WriteFileRes {
                                                //     write_vlog_res: Err(close_file(
                                                //         &mut vw,
                                                //         Err(Error::Writer { source: e }),
                                                //     )
                                                //     .err()
                                                //     .unwrap()),
                                                //     vlog_files,
                                                //     ..Default::default()
                                                // };

                                                if op.notify.send(WriteVLogRes {
                                                    key: op.key,
                                                    write_vlog_res: close_vlog_file(
                                                        &mut vw,
                                                        Err(Error::VLogWriter { source: e }),
                                                    ),
                                                    need_close_tsm_writer: true,
                                                    old_file: None,
                                                    vlog_index: None,
                                                }).is_err() {
                                                    return;
                                                }
                                                return;
                                            }
                                        },
                                    };
                                },
                                Err(_) => {
                                    if task_shutdown.is_cancelled() {
                                        // let mut vw = vw.write();
                                        if vw.size() == 0 {
                                            let _ = vw.remove();
                                        }
                                        return;
                                    }
                                }
                            }
                        },
                        default => {
                            if snapshot_vlog_input_finished(&task_shutdown, &rx) {
                                // let mut vw = vw.write();
                                if vw.size() == 0 {
                                    let _ = vw.remove();
                                }
                                return;
                            }
                        }
                    }
                } // loop
            });
            dispatch.senders.push(tx.clone());
            starts.push(SnapshotVLogWorkerStart {
                task,
                shutdown,
                sender: tx,
                ready: ready_rx,
            });
        }
        events
            .send(SnapshotVLogEvent::Start(starts))
            .map_err(|_| Error::VLogWriterTaskTerminated)?;
        Ok(dispatch)
    }

    fn write_file(
        vlog_dir: Arc<PathBuf>,
        inner: Arc<RwLock<InnerCompactor>>,
        w: &mut TSMWriter<impl SyncWrite>,
        iter: &mut impl KeyIterator,
        is_write_snapshot: bool,
        vlog_enabled: bool,
        vlog_events: Option<mpsc::UnboundedSender<SnapshotVLogEvent>>,
    ) -> Result<WriteFileRes> {
        let vlog_workers = if is_write_snapshot && vlog_enabled {
            let inner = inner.read();
            let events = vlog_events
                .as_ref()
                .ok_or(Error::SnapshotWriterTaskTerminated)?;
            Self::init_write_vlog(
                inner.max_vlog_writers,
                Arc::clone(&vlog_dir),
                &inner.vlog_file_store.as_ref().unwrap(),
                events,
            )?
        } else {
            SnapshotVLogDispatch::default()
        };

        // return vlog file names
        let err = Ok(());
        let mut vlog_files = HashSet::new();
        let mut vlog_indexes = HashMap::<String, Vec<VLogIndex>>::new();
        // chose min_seris_key in the blocks in tsm files which should be compacted, then merge
        // min_series_key's values, write to new tsm file, if the values are too large to write to only
        // one tsm file, then write them to the next level.
        // Each call to read returns the next sorted key (or the prior one if there are
        // more values to write).  The size of values will be less than or equal to our
        // chunk size (4000)
        let mut blk = 0;
        let mut kb_len = 0;
        let mut all_keys = HashSet::new();
        for encoded in iter.by_ref() {
            let EncodedBlock {
                typ,
                key,
                min_time,
                max_time,
                statistics,
                block,
                error: iter_err,
            } = encoded;
            let count = statistics.map_or(0, |statistics| statistics.count as i32);
            if is_write_snapshot {
                blk += 1;
                kb_len += key.len() + block.len();

                let kk = (*key).clone();
                all_keys.insert(kk);
            }

            let (enabled, min_vlog_size) = {
                let inner = inner.read();
                (
                    inner.snapshots_enabled || inner.compactions_enabled,
                    inner.min_vlog_size as usize,
                )
            };

            if !enabled {
                let _ = close_file(w, err);
                return Ok(WriteFileRes {
                    write_tsm_res: Err(Error::CompactionAborted {
                        reason: String::new(),
                    }),
                    vlog_files: vlog_files.into_iter().collect::<Vec<String>>(),
                    ..Default::default()
                });
            }

            if let Some(e) = iter_err {
                return Ok(WriteFileRes {
                    write_tsm_res: Err(close_file(w, Err(Error::StringErr { reason: e }))
                        .err()
                        .unwrap()),
                    vlog_files: vlog_files.into_iter().collect::<Vec<String>>(),
                    ..Default::default()
                });
            }

            if min_time > max_time {
                let _ = close_file(w, Ok(()));
                return Ok(WriteFileRes {
                    write_tsm_res: Err(Error::StringErr {
                        reason: format!(
                            "invalid index entry for block. min={}, max={}",
                            min_time, max_time
                        ),
                    }),
                    vlog_files: vlog_files.into_iter().collect::<Vec<String>>(),
                    ..Default::default()
                });
            }

            if is_write_snapshot && vlog_enabled {
                // If the encoded block is larger than min_vlog_size, write it to vlog
                if block.len() >= min_vlog_size {
                    let (tx, rx) = oneshot::channel();
                    let op = WriteVLogOp {
                        count,
                        typ,
                        key,
                        min_time,
                        max_time,
                        block,
                        notify: tx,
                        #[cfg(test)]
                        panic_for_test: false,
                    };
                    vlog_workers.send(op)?;
                    vlog_events
                        .as_ref()
                        .ok_or(Error::SnapshotWriterTaskTerminated)?
                        .send(SnapshotVLogEvent::Result(rx))
                        .map_err(|_| Error::VLogWriterTaskTerminated)?;
                } else {
                    // block.len() < min_vlog_size
                    // Write the key and value
                    if let Err(e) =
                        w.write_block_with_statistics(&key, min_time, max_time, &block, statistics)
                    {
                        match &e {
                            writer::Error::MaxBlocksExceeded { offset, size } => {
                                if let Err(e) = w.write_index() {
                                    return Ok(WriteFileRes {
                                        write_tsm_res: Err(close_file(
                                            w,
                                            Err(Error::Writer { source: e }),
                                        )
                                        .err()
                                        .unwrap()),
                                        vlog_files: vlog_files.into_iter().collect::<Vec<String>>(),
                                        ..Default::default()
                                    });
                                }
                                return Ok(WriteFileRes {
                                    write_tsm_res: Err(close_file(
                                        w,
                                        Err(Error::Writer { source: e }),
                                    )
                                    .err()
                                    .unwrap()),
                                    vlog_files: vlog_files.into_iter().collect::<Vec<String>>(),
                                    vlog_indexes,
                                    ..Default::default()
                                });
                            }
                            _ => {
                                return Ok(WriteFileRes {
                                    write_tsm_res: Err(close_file(
                                        w,
                                        Err(Error::Writer { source: e }),
                                    )
                                    .err()
                                    .unwrap()),
                                    vlog_files: vlog_files.into_iter().collect::<Vec<String>>(),
                                    ..Default::default()
                                });
                            }
                        }
                    }

                    // If we have a max file size configured and we're over it, close out the file
                    // and return the error.
                    if w.size() > MAX_TSM_FILE_SIZE {
                        if let Err(e) = w.write_index() {
                            return Ok(WriteFileRes {
                                write_tsm_res: Err(close_file(w, Err(Error::Writer { source: e }))
                                    .err()
                                    .unwrap()),
                                vlog_files: vlog_files.into_iter().collect::<Vec<String>>(),
                                ..Default::default()
                            });
                        }
                        return Ok(WriteFileRes {
                            write_tsm_res: Err(Error::MaxFileExceed),
                            vlog_files: vlog_files.into_iter().collect::<Vec<String>>(),
                            vlog_indexes,
                            ..Default::default()
                        });
                    }
                }
            } else if is_write_snapshot {
                // write snapshot to tsm

                // Write the key and value
                match w.write_block_with_statistics(&key, min_time, max_time, &block, statistics) {
                    Ok((offset, size)) => {}
                    Err(e) => match &e {
                        writer::Error::MaxBlocksExceeded { offset, size } => {
                            if let Err(e) = w.write_index() {
                                return Ok(WriteFileRes {
                                    write_tsm_res: Err(close_file(
                                        w,
                                        Err(Error::Writer { source: e }),
                                    )
                                    .err()
                                    .unwrap()),
                                    ..Default::default()
                                });
                            }
                            return Ok(WriteFileRes {
                                write_tsm_res: close_file(w, Err(Error::Writer { source: e })),
                                ..Default::default()
                            });
                        }
                        _ => {
                            return Ok(WriteFileRes {
                                write_tsm_res: Err(close_file(w, Err(Error::Writer { source: e }))
                                    .err()
                                    .unwrap()),
                                ..Default::default()
                            });
                        }
                    },
                };

                // If we have a max file size configured and we're over it, close out the file
                // and return the error.
                if w.size() > MAX_TSM_FILE_SIZE {
                    if let Err(e) = w.write_index() {
                        return Ok(WriteFileRes {
                            write_tsm_res: Err(close_file(w, Err(Error::Writer { source: e }))
                                .err()
                                .unwrap()),
                            ..Default::default()
                        });
                    }
                    return Ok(WriteFileRes {
                        write_tsm_res: Err(Error::MaxFileExceed),
                        ..Default::default()
                    });
                }
            } else {
                // compact tsm
                // Write the key and value

                match w.write_block_with_statistics(&key, min_time, max_time, &block, statistics) {
                    Ok((offset, size)) => {}
                    Err(e) => {
                        match e {
                            writer::Error::MaxBlocksExceeded { offset, size } => {
                                // compact tsm
                                if let Err(e) = w.write_index() {
                                    return Ok(WriteFileRes {
                                        write_tsm_res: Err(close_file(
                                            w,
                                            Err(Error::Writer { source: e }),
                                        )
                                        .err()
                                        .unwrap()),
                                        ..Default::default()
                                    });
                                }
                                return Ok(WriteFileRes {
                                    write_tsm_res: Err(close_file(
                                        w,
                                        Err(Error::Writer { source: e }),
                                    )
                                    .err()
                                    .unwrap()),
                                    ..Default::default()
                                });
                            }
                            _ => {
                                let mut res = WriteFileRes::default();
                                res.write_tsm_res =
                                    Err(close_file(w, Err(Error::Writer { source: e }))
                                        .err()
                                        .unwrap());
                                return Ok(res);
                            }
                        }
                    }
                };

                // If we have a max file size configured and we're over it, close out the file
                // and return the error.
                if w.size() > MAX_TSM_FILE_SIZE {
                    if let Err(e) = w.write_index() {
                        let mut res = WriteFileRes::default();
                        res.write_tsm_res = Err(close_file(w, Err(Error::Writer { source: e }))
                            .err()
                            .unwrap());
                        return Ok(res);
                    }

                    let mut res = WriteFileRes::default();
                    res.write_tsm_res = Err(Error::MaxFileExceed);
                    return Ok(res);
                }
            } // end for compact tsm or vlog
        } // while let

        if is_write_snapshot {
            SNAPSHOT_SIZE.fetch_add(kb_len, Ordering::Relaxed);

            tracing::debug!(
                target: "reefsdb::tsdb::snapshot",
                block_count = all_keys.len(),
                key_count = blk,
                output_bytes = kb_len,
                cumulative_output_bytes = SNAPSHOT_SIZE.load(Ordering::Relaxed),
                "snapshot output written"
            );
        }

        // Were there any errors encountered during iteration?
        if let Some(e) = iter.err() {
            // TODO: error 写入 write_tsm_res ?
            return Ok(WriteFileRes {
                write_tsm_res: Err(Error::StringErr { reason: e }),
                vlog_files: vlog_files.into_iter().collect::<Vec<String>>(),
                ..Default::default()
            });
        }

        if !vlog_enabled {
            // We're all done.  Close out the file.
            if let Err(e) = w.write_index() {
                let mut res = WriteFileRes {
                    vlog_files: vlog_files.into_iter().collect::<Vec<String>>(),
                    vlog_indexes,
                    ..Default::default()
                };

                res.write_tsm_res = Err(Error::Writer { source: e });
                return Ok(res);
            }
        }

        let mut res = WriteFileRes {
            vlog_indexes,
            ..Default::default()
        };
        if is_write_snapshot && vlog_enabled {
            res.vlog_files = vlog_files.into_iter().collect::<Vec<String>>();
        }
        Ok(res)
    }

    fn close_and_open_new_vlog_writer(
        mut w: &mut VLogWriter<impl SyncWrite>,
        generation: i64,
        seq: i64,
        path: &Path,
        vlog_file_store: Arc<VLogFileStore>,
    ) -> Result<(String, VLogWriter<File>)> {
        close_vlog_file(&mut w, Ok(()))?;

        // Create new file.
        let old_file = path
            .join(
                default_format_file_name(generation, seq)
                    + "."
                    + VLOG_FILE_EXTENSION
                    + "."
                    + TMP_VLOG_FILE_EXTENSION,
            )
            .to_str()
            .unwrap()
            .to_string();

        let next_generation = vlog_file_store.next_generation();
        let file_name = path.join(
            default_format_file_name(next_generation, seq)
                + "."
                + VLOG_FILE_EXTENSION
                + "."
                + TMP_VLOG_FILE_EXTENSION,
        );

        let fd = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_name)
            .expect("create file failed");
        let stat = fd.metadata().expect("get file metadata  failed");
        stat.permissions().set_mode(0o666);

        Ok((
            old_file,
            VLogWriter::new(fd, next_generation, seq, file_name),
        ))
    }
}

enum SnapshotVLogEvent {
    Start(Vec<SnapshotVLogWorkerStart>),
    Result(oneshot::Receiver<WriteVLogRes>),
}

fn snapshot_vlog_input_finished<T>(
    shutdown: &CancellationToken,
    input: &crossbeam_channel::Receiver<T>,
) -> bool {
    // A producer can enqueue its final blocks after select chose the idle
    // branch but before finish_input signals completion. Drain those blocks.
    shutdown.is_cancelled() && input.is_empty()
}

struct SnapshotVLogWorkerStart {
    task: BoxBlockingTask,
    shutdown: CancellationToken,
    sender: crossbeam_channel::Sender<WriteVLogOp>,
    ready: oneshot::Receiver<()>,
}

#[derive(Default)]
struct SnapshotVLogDispatch {
    senders: Vec<crossbeam_channel::Sender<WriteVLogOp>>,
}

impl SnapshotVLogDispatch {
    fn send(&self, op: WriteVLogOp) -> Result<()> {
        if self.senders.is_empty() {
            return Err(Error::VLogWriterTaskTerminated);
        }
        let index = (hash64(&op.key) % self.senders.len() as u64) as usize;
        self.senders[index]
            .send(op)
            .map_err(|_| Error::VLogWriterTaskTerminated)
    }
}

struct SnapshotTaskSet<T> {
    handles: Vec<TaskHandle<T>>,
}

impl<T> Default for SnapshotTaskSet<T> {
    fn default() -> Self {
        Self {
            handles: Vec::new(),
        }
    }
}

impl<T> SnapshotTaskSet<T> {
    fn push(&mut self, handle: TaskHandle<T>) {
        self.handles.push(handle);
    }

    fn take(&mut self) -> Vec<TaskHandle<T>> {
        std::mem::take(&mut self.handles)
    }
}

impl<T> Drop for SnapshotTaskSet<T> {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

#[derive(Default)]
struct SnapshotVLogWorkers {
    handles: Vec<TaskHandle<()>>,
    shutdowns: Vec<CancellationToken>,
    senders: Vec<crossbeam_channel::Sender<WriteVLogOp>>,
    ready: Vec<oneshot::Receiver<()>>,
}

impl SnapshotVLogWorkers {
    fn start(
        &mut self,
        starts: Vec<SnapshotVLogWorkerStart>,
        executor: &ExecutorRef,
    ) -> Result<()> {
        for start in starts {
            let SnapshotVLogWorkerStart {
                task,
                shutdown,
                sender,
                ready,
            } = start;
            match executor.spawn_blocking_compact(move || task()) {
                Ok(handle) => {
                    self.handles.push(handle);
                    self.shutdowns.push(shutdown);
                    self.senders.push(sender);
                    self.ready.push(ready);
                }
                Err(source) => return Err(Error::Execution { source }),
            }
        }
        Ok(())
    }

    fn finish_input(&mut self) {
        for shutdown in &self.shutdowns {
            shutdown.cancel();
        }
        self.senders.clear();
    }

    async fn wait_ready(&mut self) -> Result<()> {
        for ready in std::mem::take(&mut self.ready) {
            ready.await.map_err(|_| Error::VLogWriterTaskTerminated)?;
        }
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.finish_input();

        let mut task_terminated = false;
        for handle in std::mem::take(&mut self.handles) {
            if handle.await.is_err() {
                task_terminated = true;
            }
        }
        self.shutdowns.clear();
        self.ready.clear();

        if task_terminated {
            Err(Error::VLogWriterTaskTerminated)
        } else {
            Ok(())
        }
    }
}

impl Drop for SnapshotVLogWorkers {
    fn drop(&mut self) {
        self.finish_input();
        for handle in &self.handles {
            handle.abort();
        }
    }
}

struct WriteVLogOp {
    count: i32,
    typ: u8,
    key: Arc<Vec<u8>>,
    min_time: i64,
    max_time: i64,
    block: Vec<u8>,
    notify: oneshot::Sender<WriteVLogRes>,
    #[cfg(test)]
    panic_for_test: bool,
}

#[derive(Debug)]
struct WriteVLogRes {
    key: Arc<Vec<u8>>,
    // write_tsm_res: Result<()>,
    write_vlog_res: Result<()>,
    need_close_tsm_writer: bool,
    old_file: Option<String>,
    vlog_index: Option<VLogIndex>,
}

struct WriteFileRes {
    write_tsm_res: Result<()>,
    write_vlog_res: Result<()>,
    vlog_files: Vec<String>,
    vlog_indexes: HashMap<String, Vec<VLogIndex>>,
}

impl Default for WriteFileRes {
    fn default() -> Self {
        Self {
            write_tsm_res: Ok(()),
            write_vlog_res: Ok(()),
            vlog_files: vec![],
            vlog_indexes: HashMap::new(),
        }
    }
}

fn write_vlog(
    path: Arc<PathBuf>,
    w: Arc<RwLock<VLogWriter<File>>>,
    inner: Arc<RwLock<InnerCompactor>>,
    count: i32,
    typ: u8,
    key: Arc<Vec<u8>>,
    block: Vec<u8>,
    min: i64,
    max: i64,
) -> Result<VLogIndex> {
    let mut w = w.write();
    // Write the key and value
    let (offset, size) = match w.write_block(&key, min, max, &block) {
        Ok((offset, size)) => (offset, size),
        Err(e) => {
            match &e {
                vlog::writer::Error::MaxBlocksExceeded { offset, size } => {
                    // if let Err(e) = w.write_index() {
                    //     let _ = w.close();
                    //     // w.reset_index();
                    //     return Err(Error::Writer { source: e });
                    // }
                    return Err(Error::VLogWriter { source: e });
                }
                _ => {
                    let _ = w.close();
                    // w.reset_index();
                    return Err(Error::VLogWriter { source: e });
                }
            }
        }
    };

    let (generation, seq) = w.get_id();

    // w.reset_index();

    Ok(VLogIndex {
        aggregate: None,
        count,
        typ,
        min,
        max,
        generation,
        seq,
        offset,
        size,
    })
}

fn close_file(w: &mut TSMWriter<impl SyncWrite>, mut err: Result<()>) -> Result<()> {
    // 下面w.close最后调用DiskIndex.close导致"No such file or directory"
    if let Err(close_err) = w.close() {
        if err.is_ok() {
            err = Err(Error::Writer { source: close_err });
        }
    }

    // Check for errors where we should not remove the file
    if let Err(ref e) = err {
        let (mut in_progress, mut max_blocks, mut max_file_size) = (false, false, false);
        match &e {
            Error::CompactionInProgress { reason: _ } => in_progress = true,
            Error::MaxFileExceed => max_file_size = true,
            Error::Writer {
                source: writer::Error::MaxBlocksExceeded { offset, size },
            } => max_blocks = true,
            _ => {}
        }

        if in_progress || max_blocks || max_file_size {
            return err;
        }
        let _ = w.remove();
    }

    err
}

fn close_vlog_file(w: &mut VLogWriter<impl SyncWrite>, mut err: Result<()>) -> Result<()> {
    // 下面w.close最后调用DiskIndex.close导致"No such file or directory"
    if let Err(close_err) = w.close() {
        if err.is_ok() {
            err = Err(Error::VLogWriter { source: close_err });
        }
    }

    // Check for errors where we should not remove the file
    if let Err(ref e) = err {
        let (mut in_progress, mut max_blocks, mut max_file_size) = (false, false, false);
        match &e {
            Error::CompactionInProgress { reason: _ } => in_progress = true,
            Error::MaxFileExceed => max_file_size = true,
            Error::VLogWriter {
                source: vlog::writer::Error::MaxBlocksExceeded { offset, size },
            } => max_blocks = true,
            _ => {}
        }

        if in_progress || max_blocks || max_file_size {
            return err;
        }
        let _ = w.remove();
    }

    err
}

impl SyncWrite for std::fs::File {
    fn path(&self) -> &std::path::Path {
        std::path::Path::new("")
    }

    fn close(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn sync_all(&self) -> std::io::Result<()> {
        self.sync_all()
    }
}

#[derive(Clone, Debug)]
pub struct EncodedBlock {
    pub typ: u8,
    pub key: Arc<Vec<u8>>,
    pub min_time: i64,
    pub max_time: i64,
    pub statistics: Option<BlockStatistics>,
    pub block: Vec<u8>,
    pub error: Option<String>,
}

impl EncodedBlock {
    fn error(error: Option<String>) -> Self {
        Self {
            typ: EMPTY,
            key: Arc::new(vec![]),
            min_time: 0,
            max_time: 0,
            statistics: None,
            block: vec![],
            error,
        }
    }
}

pub trait KeyIterator: Iterator<Item = EncodedBlock> {
    /// Returns true if there are any values remaining in the iterator.
    fn has_next(&mut self) -> bool;

    /// Closes the iterator.
    fn close(&mut self) -> Result<()>;

    /// Returns any errors encountered during iteration.
    fn err(&self) -> Option<String>;

    /// Returns the estimated size of the index that would
    /// be required to store all the series and entries in the KeyIterator.
    fn estimated_index_size(&self) -> usize;
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

pub struct TSMErrors(Vec<Error>);
impl TSMErrors {
    pub fn err(&self) -> Option<String> {
        if self.is_empty() {
            return None;
        }

        let mut s = vec![];
        for e in &self.0 {
            s.push(e.to_string());
        }
        Some(s.join(", "))
    }
}

impl Deref for TSMErrors {
    type Target = Vec<Error>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TSMErrors {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// TSMKeyIter implements the KeyIterator for set of TSMReaders.  Iteration produces
// keys in sorted order and the values between the keys sorted and deduped.  If any of
// the readers have associated tombstone entries, they are returned as part of iteration.
struct TSMKeyIter {
    // readers is the set of readers it produce a sorted key run with
    readers: Vec<Arc<TSMReader>>,

    // values is the temporary buffers for each key that is returned by a reader
    values: HashMap<String, Value>,

    // pos is the current key position within the corresponding readers slice.  A value of
    // pos[0] = 1, means the reader[0] is currently at key 1 in its ordered index.
    pos: Vec<usize>,

    // TSMError wraps any error we received while iterating values.
    errs: TSMErrors,

    // indicates whether the iterator should choose a faster merging strategy over a more
    // optimally compressed one.  If fast is true, multiple blocks will just be added as is
    // and not combined.  In some cases, a slower path will need to be utilized even when
    // fast is true to prevent overlapping blocks of time for the same key.
    // If false, the blocks will be decoded and duplicated (if needed) and
    // then chunked into the maximally sized blocks.
    fast: bool,

    // size is the maximum number of values to encode in a single block
    size: i64,

    // key is the current key lowest key across all readers that has not be fully exhausted
    // of values.
    key: Vec<u8>,
    typ: u8,

    iterators: Vec<BlockIter>,
    blocks: Blocks,

    buf: Vec<Blocks>,

    // mergeValues are decoded blocks that have been combined
    merged_integer_values: IntegerValues,
    merged_unsigned_values: UnsignedValues,
    merged_float_values: FloatValues,
    merged_bool_values: BoolValues,
    merged_string_values: StringValues,

    // merged are encoded blocks that have been combined or used as is without decode.
    // each call to next will return block from merged.
    merged: Blocks,
    interrupt: CancellationToken,
}

impl TSMKeyIter {
    fn new(
        size: i64,
        fast: bool,
        interrupt: CancellationToken,
        readers: &[Arc<TSMReader>],
    ) -> Result<Self> {
        let mut iter = vec![];
        for r in readers {
            iter.push(TSMReader::block_iter(Arc::clone(r)));
        }

        let iter_len = iter.len();
        let tki = Self {
            readers: readers.to_vec(),
            values: HashMap::new(),
            pos: vec![0; readers.len()],
            errs: TSMErrors(vec![]),
            fast,
            size,
            key: vec![],
            typ: 0,
            iterators: iter,
            blocks: Blocks::default(),
            buf: vec![Blocks::default(); iter_len],

            merged_integer_values: IntegerValues::default(),
            merged_unsigned_values: UnsignedValues::default(),
            merged_float_values: FloatValues::default(),
            merged_bool_values: BoolValues::default(),
            merged_string_values: StringValues::default(),

            merged: Blocks::default(),
            interrupt,
        };

        Ok(tki)
    }

    fn append_error(&mut self, err: Error) {
        self.errs.push(err);
    }

    fn has_merged_values(&self) -> bool {
        !self.merged_integer_values.is_empty()
            || !self.merged_unsigned_values.is_empty()
            || !self.merged_float_values.is_empty()
            || !self.merged_bool_values.is_empty()
            || !self.merged_string_values.is_empty()
    }

    /// merge combines the next set of blocks into merged blocks.
    fn merge(&mut self) {
        match self.typ {
            BLOCK_INTEGER => self.merge_integer(), // TODO: compact_gen
            BLOCK_UNSIGNED => self.merge_unsigned(),
            BLOCK_FLOAT64 => self.merge_float(),
            BLOCK_BOOL => self.merge_bool(),
            BLOCK_STRING => self.merge_string(),
            _ => self.append_error(Error::StringErr {
                reason: format!("unknown block type: {}", self.typ),
            }),
        }
    }

    /// Combines the next set of blocks into merged blocks.
    fn merge_integer(&mut self) {
        // No blocks left, or pending merged values, we're done
        if self.blocks.is_empty() && self.merged.is_empty() && self.merged_integer_values.is_empty()
        {
            return;
        }

        self.blocks.sort(); // sort stable

        let mut dedup = !self.merged_integer_values.is_empty();
        if !self.blocks.is_empty() && !dedup {
            // If we have more than one block or any partially tombstoned blocks, we many need to dedup
            dedup = !self.blocks[0].tombstones.is_empty() || self.blocks[0].partially_read();

            // Quickly scan each block to see if any overlap with the prior block, if they overlap then
            // we need to dedup as there may be duplicate points now
            let mut i = 1;
            while i < self.blocks.len() && !dedup {
                dedup = self.blocks[i].partially_read()
                    || self.blocks[i].overlaps_time_range(
                        self.blocks[i - 1].min_time,
                        self.blocks[i - 1].max_time,
                    )
                    || !self.blocks[i].tombstones.is_empty();

                i += 1;
            }
        }

        self.merged = self.combine_integer(dedup);
    }

    // combine returns a new set of blocks using the current blocks in the buffers.  If dedup
    // is true, all the blocks will be decoded, dedup and sorted in in order.  If dedup is false,
    // only blocks that are smaller than the chunk size will be decoded and combined.
    fn combine_integer(&mut self, dedup: bool) -> Blocks {
        if dedup {
            while self.merged_integer_values.len() < self.size as usize && !self.blocks.is_empty() {
                while !self.blocks.is_empty() && self.blocks[0].read() {
                    self.blocks.drain(..1);
                }

                if self.blocks.is_empty() {
                    break;
                }
                let first = &self.blocks[0];
                let mut min_time = first.min_time;
                let mut max_time = first.max_time;

                // Adjust the min time to the start of any overlapping blocks.
                for b in self.blocks.iter_mut() {
                    if b.overlaps_time_range(min_time, max_time) && !b.read() {
                        if b.min_time < min_time {
                            min_time = b.min_time;
                        }
                        if b.max_time > min_time && b.max_time < max_time {
                            max_time = b.max_time;
                        }
                    }
                }

                // We have some overlapping blocks so decode all, append in order and then dedup
                for b in self.blocks.iter_mut() {
                    if !b.overlaps_time_range(min_time, max_time) || b.read() {
                        continue;
                    }

                    let mut v = IntegerValues(match decode_integer_block(&b.b) {
                        Ok(fv) => fv,
                        Err(e) => {
                            self.append_error(Error::StringErr {
                                reason: e.to_string(),
                            });
                            return Blocks::default();
                        }
                    });

                    // Remove values we already read

                    v.exclude(b.read_min, b.read_max);

                    // Filter out only the values for overlapping block
                    v.include(min_time, max_time);
                    if !v.is_empty() {
                        b.mark_read(v[0].unix_nano, v[v.len() - 1].unix_nano);
                    }

                    // Apply each tombstone to the block
                    for ts in b.tombstones.iter() {
                        v.exclude(ts.min, ts.max);
                    }
                    self.merged_integer_values.merge(v);
                }
            }

            // Since we combined multiple blocks, we could have more values than we should put into
            // a single block.  We need to chunk them up into groups and re-encode them.
            self.chunk_integer(true)
        } else {
            let mut i = 0;

            while i < self.blocks.len() {
                let cur = &self.blocks[i];

                // skip this block if it's values were already read
                if cur.read() {
                    i += 1;
                    continue;
                }

                // If this block is already full, just add it as is
                let count = match block_count(&cur.b) {
                    Ok(n) => n,
                    Err(e) => {
                        // accumulate all errors to tsmKeyIterator.err
                        self.append_error(Error::StringErr {
                            reason: e.to_string(),
                        });
                        i += 1;
                        continue;
                    }
                };

                if (count as i64) < self.size {
                    break;
                }

                self.merged.push(cur.clone());
                i += 1;
            }

            if self.fast {
                while i < self.blocks.len() {
                    // skip this block if it's values were already read
                    if self.blocks[i].read() {
                        i += 1;
                        continue;
                    }

                    self.merged.push(self.blocks[i].clone());
                    i += 1;
                }
            }

            // If we only have 1 blocks left, just append it as is and avoid decoding/recoding
            if i == self.blocks.len() - 1 {
                if !self.blocks[i].read() {
                    self.merged.push(self.blocks[i].clone());
                }
                i += 1;
            }

            // The remaining blocks can be combined and we know that they do not overlap and
            // so we can just append each, sort and re-encode.
            while i < self.blocks.len() && self.merged_integer_values.len() < self.size as usize {
                let cur = &mut self.blocks[i];
                if cur.read() {
                    i += 1;
                    continue;
                }

                let mut v = IntegerValues(match decode_integer_block(&cur.b) {
                    Ok(fv) => fv,
                    Err(e) => {
                        self.append_error(Error::StringErr {
                            reason: e.to_string(),
                        });
                        return Blocks::default();
                    }
                });

                // Apply each tombstone to the block
                for ts in cur.tombstones.iter() {
                    v.exclude(ts.min, ts.max);
                }

                cur.mark_read(cur.min_time, cur.max_time);

                self.merged_integer_values.merge(v);
                i += 1;
            }

            self.blocks.drain(..i);

            self.chunk_integer(false)
        }
    }

    fn chunk_integer(&mut self, dedup: bool) -> Blocks {
        let mut dst = {
            if dedup {
                Blocks::default()
            } else {
                self.merged.clone()
            }
        };

        if self.merged_integer_values.len() > self.size as usize {
            let values = self.merged_integer_values[..self.size as usize].to_vec();
            let min_time = values[0].unix_nano;
            let max_time = values[values.len() - 1].unix_nano;
            let statistics = BlockStatistics::from_integer(values.iter().map(|value| value.value));
            let cb = match IntegerValues(values).encode(&[]) {
                Ok(b) => b,
                Err(e) => {
                    self.append_error(Error::StringErr {
                        reason: e.to_string(),
                    });
                    return Blocks::default();
                }
            };

            dst.push(Block {
                min_time,
                max_time,
                key: self.key.clone(),
                statistics,
                b: cb,
                ..Default::default()
            });
            self.merged_integer_values = IntegerValues(
                self.merged_integer_values
                    .drain(self.size as usize..)
                    .collect(),
            );
            return dst;
        }

        // Re-encode the remaining values into the last block
        if !self.merged_integer_values.is_empty() {
            let statistics = BlockStatistics::from_integer(
                self.merged_integer_values.iter().map(|value| value.value),
            );
            let cb = match IntegerValues(self.merged_integer_values.clone()).encode(&[]) {
                Ok(b) => b,
                Err(e) => {
                    self.append_error(Error::StringErr {
                        reason: e.to_string(),
                    });
                    return Blocks::default();
                }
            };
            dst.push(Block {
                min_time: self.merged_integer_values[0].unix_nano,
                max_time: self.merged_integer_values[self.merged_integer_values.len() - 1]
                    .unix_nano,
                key: self.key.clone(),
                statistics,
                b: cb,
                ..Default::default()
            });
            self.merged_integer_values.clear();
        }

        dst
    }

    /// Combines the next set of blocks into merged blocks.
    fn merge_unsigned(&mut self) {
        // No blocks left, or pending merged values, we're done
        if self.blocks.is_empty()
            && self.merged.is_empty()
            && self.merged_unsigned_values.is_empty()
        {
            return;
        }

        self.blocks.sort(); // sort stable

        let mut dedup = !self.merged_unsigned_values.is_empty();
        if !self.blocks.is_empty() && !dedup {
            // If we have more than one block or any partially tombstoned blocks, we many need to dedup
            dedup = !self.blocks[0].tombstones.is_empty() || self.blocks[0].partially_read();

            // Quickly scan each block to see if any overlap with the prior block, if they overlap then
            // we need to dedup as there may be duplicate points now
            let mut i = 1;
            while i < self.blocks.len() && !dedup {
                dedup = self.blocks[i].partially_read()
                    || self.blocks[i].overlaps_time_range(
                        self.blocks[i - 1].min_time,
                        self.blocks[i - 1].max_time,
                    )
                    || !self.blocks[i].tombstones.is_empty();

                i += 1;
            }
        }

        self.merged = self.combine_unsigned(dedup);
    }

    // combine returns a new set of blocks using the current blocks in the buffers.  If dedup
    // is true, all the blocks will be decoded, dedup and sorted in in order.  If dedup is false,
    // only blocks that are smaller than the chunk size will be decoded and combined.
    fn combine_unsigned(&mut self, dedup: bool) -> Blocks {
        if dedup {
            while self.merged_unsigned_values.len() < self.size as usize && !self.blocks.is_empty()
            {
                while !self.blocks.is_empty() && self.blocks[0].read() {
                    self.blocks.drain(..1);
                }

                if self.blocks.is_empty() {
                    break;
                }
                let first = &self.blocks[0];
                let mut min_time = first.min_time;
                let mut max_time = first.max_time;

                // Adjust the min time to the start of any overlapping blocks.
                for b in self.blocks.iter_mut() {
                    if b.overlaps_time_range(min_time, max_time) && !b.read() {
                        if b.min_time < min_time {
                            min_time = b.min_time;
                        }
                        if b.max_time > min_time && b.max_time < max_time {
                            max_time = b.max_time;
                        }
                    }
                }

                // We have some overlapping blocks so decode all, append in order and then dedup
                for b in self.blocks.iter_mut() {
                    if !b.overlaps_time_range(min_time, max_time) || b.read() {
                        continue;
                    }

                    let mut v = UnsignedValues(match decode_unsigned_block(&b.b) {
                        Ok(fv) => fv,
                        Err(e) => {
                            self.append_error(Error::StringErr {
                                reason: e.to_string(),
                            });
                            return Blocks::default();
                        }
                    });

                    // Remove values we already read
                    v.exclude(b.read_min, b.read_max);

                    // Filter out only the values for overlapping block
                    v.include(min_time, max_time);
                    if !v.is_empty() {
                        b.mark_read(v[0].unix_nano, v[v.len() - 1].unix_nano);
                    }

                    // Apply each tombstone to the block
                    for ts in b.tombstones.iter() {
                        v.exclude(ts.min, ts.max);
                    }
                    self.merged_unsigned_values.merge(v);
                }
            }

            // Since we combined multiple blocks, we could have more values than we should put into
            // a single block.  We need to chunk them up into groups and re-encode them.
            self.chunk_unsigned(true)
        } else {
            let mut i = 0;

            while i < self.blocks.len() {
                let cur = &self.blocks[i];

                // skip this block if it's values were already read
                if cur.read() {
                    i += 1;
                    continue;
                }

                // If this block is already full, just add it as is
                let count = match block_count(&cur.b) {
                    Ok(n) => n,
                    Err(e) => {
                        // accumulate all errors to tsmKeyIterator.err
                        self.append_error(Error::StringErr {
                            reason: e.to_string(),
                        });
                        i += 1;
                        continue;
                    }
                };

                if (count as i64) < self.size {
                    break;
                }

                self.merged.push(cur.clone());
                i += 1;
            }

            if self.fast {
                while i < self.blocks.len() {
                    // skip this block if it's values were already read
                    if self.blocks[i].read() {
                        i += 1;
                        continue;
                    }

                    self.merged.push(self.blocks[i].clone());
                    i += 1;
                }
            }

            // If we only have 1 blocks left, just append it as is and avoid decoding/recoding
            if i == self.blocks.len() - 1 {
                if !self.blocks[i].read() {
                    self.merged.push(self.blocks[i].clone());
                }
                i += 1;
            }

            // The remaining blocks can be combined and we know that they do not overlap and
            // so we can just append each, sort and re-encode.
            while i < self.blocks.len() && self.merged_unsigned_values.len() < self.size as usize {
                let cur = &mut self.blocks[i];
                if cur.read() {
                    i += 1;
                    continue;
                }

                let mut v = UnsignedValues(match decode_unsigned_block(&cur.b) {
                    Ok(fv) => fv,
                    Err(e) => {
                        self.append_error(Error::StringErr {
                            reason: e.to_string(),
                        });
                        return Blocks::default();
                    }
                });

                // Apply each tombstone to the block
                for ts in cur.tombstones.iter() {
                    v.exclude(ts.min, ts.max);
                }

                cur.mark_read(cur.min_time, cur.max_time);

                self.merged_unsigned_values.merge(v);
                i += 1;
            }

            self.blocks.drain(..i);

            self.chunk_unsigned(false)
        }
    }

    fn chunk_unsigned(&mut self, dedup: bool) -> Blocks {
        let mut dst = {
            if dedup {
                Blocks::default()
            } else {
                self.merged.clone()
            }
        };

        if self.merged_unsigned_values.len() > self.size as usize {
            let values = self.merged_unsigned_values[..self.size as usize].to_vec();
            let min_time = values[0].unix_nano;
            let max_time = values[values.len() - 1].unix_nano;
            let statistics = BlockStatistics::from_unsigned(values.iter().map(|value| value.value));
            let cb = match UnsignedValues(values).encode(&[]) {
                Ok(b) => b,
                Err(e) => {
                    self.append_error(Error::StringErr {
                        reason: e.to_string(),
                    });
                    return Blocks::default();
                }
            };

            dst.push(Block {
                min_time,
                max_time,
                key: self.key.clone(),
                statistics,
                b: cb,
                ..Default::default()
            });
            self.merged_unsigned_values = UnsignedValues(
                self.merged_unsigned_values
                    .drain(self.size as usize..)
                    .collect(),
            );
            return dst;
        }

        // Re-encode the remaining values into the last block
        if !self.merged_unsigned_values.is_empty() {
            let statistics = BlockStatistics::from_unsigned(
                self.merged_unsigned_values.iter().map(|value| value.value),
            );
            let cb = match UnsignedValues(self.merged_unsigned_values.clone()).encode(&[]) {
                Ok(b) => b,
                Err(e) => {
                    self.append_error(Error::StringErr {
                        reason: e.to_string(),
                    });
                    return Blocks::default();
                }
            };
            dst.push(Block {
                min_time: self.merged_unsigned_values[0].unix_nano,
                max_time: self.merged_unsigned_values[self.merged_unsigned_values.len() - 1]
                    .unix_nano,
                key: self.key.clone(),
                statistics,
                b: cb,
                ..Default::default()
            });
            self.merged_unsigned_values.clear();
        }

        dst
    }

    /// Combines the next set of blocks into merged blocks.
    fn merge_float(&mut self) {
        // No blocks left, or pending merged values, we're done
        if self.blocks.is_empty() && self.merged.is_empty() && self.merged_float_values.is_empty() {
            return;
        }

        self.blocks.sort(); // sort stable

        let mut dedup = !self.merged_float_values.is_empty();
        if !self.blocks.is_empty() && !dedup {
            // If we have more than one block or any partially tombstoned blocks, we many need to dedup
            dedup = !self.blocks[0].tombstones.is_empty() || self.blocks[0].partially_read();

            // Quickly scan each block to see if any overlap with the prior block, if they overlap then
            // we need to dedup as there may be duplicate points now
            let mut i = 1;
            while i < self.blocks.len() && !dedup {
                dedup = self.blocks[i].partially_read()
                    || self.blocks[i].overlaps_time_range(
                        self.blocks[i - 1].min_time,
                        self.blocks[i - 1].max_time,
                    )
                    || !self.blocks[i].tombstones.is_empty();

                i += 1;
            }
        }

        self.merged = self.combine_float(dedup);
    }

    // combine returns a new set of blocks using the current blocks in the buffers.  If dedup
    // is true, all the blocks will be decoded, dedup and sorted in in order.  If dedup is false,
    // only blocks that are smaller than the chunk size will be decoded and combined.
    fn combine_float(&mut self, dedup: bool) -> Blocks {
        if dedup {
            while self.merged_float_values.len() < self.size as usize && !self.blocks.is_empty() {
                while !self.blocks.is_empty() && self.blocks[0].read() {
                    self.blocks.drain(..1);
                }

                if self.blocks.is_empty() {
                    break;
                }
                let first = &self.blocks[0];
                let mut min_time = first.min_time;
                let mut max_time = first.max_time;

                // Adjust the min time to the start of any overlapping blocks.
                for b in self.blocks.iter_mut() {
                    if b.overlaps_time_range(min_time, max_time) && !b.read() {
                        if b.min_time < min_time {
                            min_time = b.min_time;
                        }
                        if b.max_time > min_time && b.max_time < max_time {
                            max_time = b.max_time;
                        }
                    }
                }

                // We have some overlapping blocks so decode all, append in order and then dedup
                for b in self.blocks.iter_mut() {
                    if !b.overlaps_time_range(min_time, max_time) || b.read() {
                        continue;
                    }

                    let mut v = FloatValues(match decode_float_block(&b.b) {
                        Ok(fv) => fv,
                        Err(e) => {
                            self.append_error(Error::StringErr {
                                reason: e.to_string(),
                            });
                            return Blocks::default();
                        }
                    });

                    // Remove values we already read

                    v.exclude(b.read_min, b.read_max);

                    // Filter out only the values for overlapping block
                    v.include(min_time, max_time);
                    if !v.is_empty() {
                        b.mark_read(v[0].unix_nano, v[v.len() - 1].unix_nano);
                    }

                    // Apply each tombstone to the block
                    for ts in b.tombstones.iter() {
                        v.exclude(ts.min, ts.max);
                    }
                    self.merged_float_values.merge(v);
                }
            }

            // Since we combined multiple blocks, we could have more values than we should put into
            // a single block.  We need to chunk them up into groups and re-encode them.
            self.chunk_float(true)
        } else {
            let mut i = 0;

            while i < self.blocks.len() {
                let cur = &self.blocks[i];

                // skip this block if it's values were already read
                if cur.read() {
                    i += 1;
                    continue;
                }

                // If this block is already full, just add it as is
                let count = match block_count(&cur.b) {
                    Ok(n) => n,
                    Err(e) => {
                        // accumulate all errors to tsmKeyIterator.err
                        self.append_error(Error::StringErr {
                            reason: e.to_string(),
                        });
                        i += 1;
                        continue;
                    }
                };

                if (count as i64) < self.size {
                    break;
                }

                self.merged.push(cur.clone());
                i += 1;
            }

            if self.fast {
                while i < self.blocks.len() {
                    // skip this block if it's values were already read
                    if self.blocks[i].read() {
                        i += 1;
                        continue;
                    }

                    self.merged.push(self.blocks[i].clone());
                    i += 1;
                }
            }

            // If we only have 1 blocks left, just append it as is and avoid decoding/recoding
            if i == self.blocks.len() - 1 {
                if !self.blocks[i].read() {
                    self.merged.push(self.blocks[i].clone());
                }
                i += 1;
            }

            // The remaining blocks can be combined and we know that they do not overlap and
            // so we can just append each, sort and re-encode.
            while i < self.blocks.len() && self.merged_float_values.len() < self.size as usize {
                let cur = &mut self.blocks[i];
                if cur.read() {
                    i += 1;
                    continue;
                }

                let mut v = FloatValues(match decode_float_block(&cur.b) {
                    Ok(fv) => fv,
                    Err(e) => {
                        self.append_error(Error::StringErr {
                            reason: e.to_string(),
                        });
                        return Blocks::default();
                    }
                });

                // Apply each tombstone to the block
                for ts in cur.tombstones.iter() {
                    v.exclude(ts.min, ts.max);
                }

                cur.mark_read(cur.min_time, cur.max_time);

                self.merged_float_values.merge(v);
                i += 1;
            }

            self.blocks.drain(..i);

            self.chunk_float(false)
        }
    }

    fn chunk_float(&mut self, dedup: bool) -> Blocks {
        let mut dst = {
            if dedup {
                Blocks::default()
            } else {
                self.merged.clone()
            }
        };

        if self.merged_float_values.len() > self.size as usize {
            let values = self.merged_float_values[..self.size as usize].to_vec();
            let min_time = values[0].unix_nano;
            let max_time = values[values.len() - 1].unix_nano;
            let statistics = BlockStatistics::from_float(values.iter().map(|value| value.value));
            let cb = match FloatValues(values).encode(&[]) {
                Ok(b) => b,
                Err(e) => {
                    self.append_error(Error::StringErr {
                        reason: e.to_string(),
                    });
                    return Blocks::default();
                }
            };

            dst.push(Block {
                min_time,
                max_time,
                key: self.key.clone(),
                statistics,
                b: cb,
                ..Default::default()
            });
            self.merged_float_values = FloatValues(
                self.merged_float_values
                    .drain(self.size as usize..)
                    .collect(),
            );
            return dst;
        }

        // Re-encode the remaining values into the last block
        if !self.merged_float_values.is_empty() {
            let statistics = BlockStatistics::from_float(
                self.merged_float_values.iter().map(|value| value.value),
            );
            let cb = match FloatValues(self.merged_float_values.clone()).encode(&[]) {
                Ok(b) => b,
                Err(e) => {
                    self.append_error(Error::StringErr {
                        reason: e.to_string(),
                    });
                    return Blocks::default();
                }
            };
            dst.push(Block {
                min_time: self.merged_float_values[0].unix_nano,
                max_time: self.merged_float_values[self.merged_float_values.len() - 1].unix_nano,
                key: self.key.clone(),
                statistics,
                b: cb,
                ..Default::default()
            });
            self.merged_float_values.clear();
        }

        dst
    }

    /// Combines the next set of blocks into merged blocks.
    fn merge_bool(&mut self) {
        // No blocks left, or pending merged values, we're done
        if self.blocks.is_empty() && self.merged.is_empty() && self.merged_bool_values.is_empty() {
            return;
        }

        self.blocks.sort(); // sort stable

        let mut dedup = !self.merged_bool_values.is_empty();
        if !self.blocks.is_empty() && !dedup {
            // If we have more than one block or any partially tombstoned blocks, we many need to dedup
            dedup = !self.blocks[0].tombstones.is_empty() || self.blocks[0].partially_read();

            // Quickly scan each block to see if any overlap with the prior block, if they overlap then
            // we need to dedup as there may be duplicate points now
            let mut i = 1;
            while i < self.blocks.len() && !dedup {
                dedup = self.blocks[i].partially_read()
                    || self.blocks[i].overlaps_time_range(
                        self.blocks[i - 1].min_time,
                        self.blocks[i - 1].max_time,
                    )
                    || !self.blocks[i].tombstones.is_empty();

                i += 1;
            }
        }

        self.merged = self.combine_bool(dedup);
    }

    // combine returns a new set of blocks using the current blocks in the buffers.  If dedup
    // is true, all the blocks will be decoded, dedup and sorted in in order.  If dedup is false,
    // only blocks that are smaller than the chunk size will be decoded and combined.
    fn combine_bool(&mut self, dedup: bool) -> Blocks {
        if dedup {
            while self.merged_bool_values.len() < self.size as usize && !self.blocks.is_empty() {
                while !self.blocks.is_empty() && self.blocks[0].read() {
                    self.blocks.drain(..1);
                }

                if self.blocks.is_empty() {
                    break;
                }
                let first = &self.blocks[0];
                let mut min_time = first.min_time;
                let mut max_time = first.max_time;

                // Adjust the min time to the start of any overlapping blocks.
                for b in self.blocks.iter_mut() {
                    if b.overlaps_time_range(min_time, max_time) && !b.read() {
                        if b.min_time < min_time {
                            min_time = b.min_time;
                        }
                        if b.max_time > min_time && b.max_time < max_time {
                            max_time = b.max_time;
                        }
                    }
                }

                // We have some overlapping blocks so decode all, append in order and then dedup
                for b in self.blocks.iter_mut() {
                    if !b.overlaps_time_range(min_time, max_time) || b.read() {
                        continue;
                    }

                    let mut v = BoolValues(match decode_bool_block(&b.b) {
                        Ok(fv) => fv,
                        Err(e) => {
                            self.append_error(Error::StringErr {
                                reason: e.to_string(),
                            });
                            return Blocks::default();
                        }
                    });

                    // Remove values we already read

                    v.exclude(b.read_min, b.read_max);

                    // Filter out only the values for overlapping block
                    v.include(min_time, max_time);
                    if !v.is_empty() {
                        b.mark_read(v[0].unix_nano, v[v.len() - 1].unix_nano);
                    }

                    // Apply each tombstone to the block
                    for ts in b.tombstones.iter() {
                        v.exclude(ts.min, ts.max);
                    }
                    self.merged_bool_values.merge(v);
                }
            }

            // Since we combined multiple blocks, we could have more values than we should put into
            // a single block.  We need to chunk them up into groups and re-encode them.
            self.chunk_bool(true)
        } else {
            let mut i = 0;

            while i < self.blocks.len() {
                let cur = &self.blocks[i];

                // skip this block if it's values were already read
                if cur.read() {
                    i += 1;
                    continue;
                }

                // If this block is already full, just add it as is
                let count = match block_count(&cur.b) {
                    Ok(n) => n,
                    Err(e) => {
                        // accumulate all errors to tsmKeyIterator.err
                        self.append_error(Error::StringErr {
                            reason: e.to_string(),
                        });
                        i += 1;
                        continue;
                    }
                };

                if (count as i64) < self.size {
                    break;
                }

                self.merged.push(cur.clone());
                i += 1;
            }

            if self.fast {
                while i < self.blocks.len() {
                    // skip this block if it's values were already read
                    if self.blocks[i].read() {
                        i += 1;
                        continue;
                    }

                    self.merged.push(self.blocks[i].clone());
                    i += 1;
                }
            }

            // If we only have 1 blocks left, just append it as is and avoid decoding/recoding
            if i == self.blocks.len() - 1 {
                if !self.blocks[i].read() {
                    self.merged.push(self.blocks[i].clone());
                }
                i += 1;
            }

            // The remaining blocks can be combined and we know that they do not overlap and
            // so we can just append each, sort and re-encode.
            while i < self.blocks.len() && self.merged_bool_values.len() < self.size as usize {
                let cur = &mut self.blocks[i];
                if cur.read() {
                    i += 1;
                    continue;
                }

                let mut v = BoolValues(match decode_bool_block(&cur.b) {
                    Ok(fv) => fv,
                    Err(e) => {
                        self.append_error(Error::StringErr {
                            reason: e.to_string(),
                        });
                        return Blocks::default();
                    }
                });

                // Apply each tombstone to the block
                for ts in cur.tombstones.iter() {
                    v.exclude(ts.min, ts.max);
                }

                cur.mark_read(cur.min_time, cur.max_time);

                self.merged_bool_values.merge(v);
                i += 1;
            }

            self.blocks.drain(..i);

            self.chunk_bool(false)
        }
    }

    fn chunk_bool(&mut self, dedup: bool) -> Blocks {
        let mut dst = {
            if dedup {
                Blocks::default()
            } else {
                self.merged.clone()
            }
        };

        if self.merged_bool_values.len() > self.size as usize {
            let values = self.merged_bool_values[..self.size as usize].to_vec();
            let min_time = values[0].unix_nano;
            let max_time = values[values.len() - 1].unix_nano;
            let statistics = BlockStatistics::from_bool(values.iter().map(|value| value.value));
            let cb = match BoolValues(values).encode(&[]) {
                Ok(b) => b,
                Err(e) => {
                    self.append_error(Error::StringErr {
                        reason: e.to_string(),
                    });
                    return Blocks::default();
                }
            };

            dst.push(Block {
                min_time,
                max_time,
                key: self.key.clone(),
                statistics,
                b: cb,
                ..Default::default()
            });
            self.merged_bool_values = BoolValues(
                self.merged_bool_values
                    .drain(self.size as usize..)
                    .collect(),
            );
            return dst;
        }

        // Re-encode the remaining values into the last block
        if !self.merged_bool_values.is_empty() {
            let statistics =
                BlockStatistics::from_bool(self.merged_bool_values.iter().map(|value| value.value));
            let cb = match BoolValues(self.merged_bool_values.clone()).encode(&[]) {
                Ok(b) => b,
                Err(e) => {
                    self.append_error(Error::StringErr {
                        reason: e.to_string(),
                    });
                    return Blocks::default();
                }
            };
            dst.push(Block {
                min_time: self.merged_bool_values[0].unix_nano,
                max_time: self.merged_bool_values[self.merged_bool_values.len() - 1].unix_nano,
                key: self.key.clone(),
                statistics,
                b: cb,
                ..Default::default()
            });
            self.merged_bool_values.clear();
        }

        dst
    }

    /// Combines the next set of blocks into merged blocks.
    fn merge_string(&mut self) {
        // No blocks left, or pending merged values, we're done
        if self.blocks.is_empty() && self.merged.is_empty() && self.merged_string_values.is_empty()
        {
            return;
        }

        self.blocks.sort(); // sort stable

        let mut dedup = !self.merged_string_values.is_empty();
        if !self.blocks.is_empty() && !dedup {
            // If we have more than one block or any partially tombstoned blocks, we many need to dedup
            dedup = !self.blocks[0].tombstones.is_empty() || self.blocks[0].partially_read();

            // Quickly scan each block to see if any overlap with the prior block, if they overlap then
            // we need to dedup as there may be duplicate points now
            let mut i = 1;
            while i < self.blocks.len() && !dedup {
                dedup = self.blocks[i].partially_read()
                    || self.blocks[i].overlaps_time_range(
                        self.blocks[i - 1].min_time,
                        self.blocks[i - 1].max_time,
                    )
                    || !self.blocks[i].tombstones.is_empty();

                i += 1;
            }
        }

        self.merged = self.combine_string(dedup);
    }

    // combine returns a new set of blocks using the current blocks in the buffers.  If dedup
    // is true, all the blocks will be decoded, dedup and sorted in in order.  If dedup is false,
    // only blocks that are smaller than the chunk size will be decoded and combined.
    fn combine_string(&mut self, dedup: bool) -> Blocks {
        if dedup {
            while self.merged_string_values.len() < self.size as usize && !self.blocks.is_empty() {
                while !self.blocks.is_empty() && self.blocks[0].read() {
                    self.blocks.drain(..1);
                }

                if self.blocks.is_empty() {
                    break;
                }
                let first = &self.blocks[0];
                let mut min_time = first.min_time;
                let mut max_time = first.max_time;

                // Adjust the min time to the start of any overlapping blocks.
                for b in self.blocks.iter_mut() {
                    if b.overlaps_time_range(min_time, max_time) && !b.read() {
                        if b.min_time < min_time {
                            min_time = b.min_time;
                        }
                        if b.max_time > min_time && b.max_time < max_time {
                            max_time = b.max_time;
                        }
                    }
                }

                // We have some overlapping blocks so decode all, append in order and then dedup
                for b in self.blocks.iter_mut() {
                    if !b.overlaps_time_range(min_time, max_time) || b.read() {
                        continue;
                    }

                    let mut v = StringValues(match decode_string_block(&b.b) {
                        Ok(fv) => fv,
                        Err(e) => {
                            self.append_error(Error::StringErr {
                                reason: e.to_string(),
                            });
                            return Blocks::default();
                        }
                    });

                    // Remove values we already read
                    v.exclude(b.read_min, b.read_max);

                    // Filter out only the values for overlapping block
                    v.include(min_time, max_time);
                    if !v.is_empty() {
                        b.mark_read(v[0].unix_nano, v[v.len() - 1].unix_nano);
                    }

                    // Apply each tombstone to the block
                    for ts in b.tombstones.iter() {
                        v.exclude(ts.min, ts.max);
                    }
                    self.merged_string_values.merge(v);
                }
            }

            // Since we combined multiple blocks, we could have more values than we should put into
            // a single block.  We need to chunk them up into groups and re-encode them.
            self.chunk_string(true)
        } else {
            let mut i = 0;

            while i < self.blocks.len() {
                let cur = &self.blocks[i];

                // skip this block if it's values were already read
                if cur.read() {
                    i += 1;
                    continue;
                }

                // If this block is already full, just add it as is
                let count = match block_count(&cur.b) {
                    Ok(n) => n,
                    Err(e) => {
                        // accumulate all errors to tsmKeyIterator.err
                        self.append_error(Error::StringErr {
                            reason: e.to_string(),
                        });
                        i += 1;
                        continue;
                    }
                };

                if (count as i64) < self.size {
                    break;
                }

                self.merged.push(cur.clone());
                i += 1;
            }

            if self.fast {
                while i < self.blocks.len() {
                    // skip this block if it's values were already read
                    if self.blocks[i].read() {
                        i += 1;
                        continue;
                    }

                    self.merged.push(self.blocks[i].clone());
                    i += 1;
                }
            }

            // If we only have 1 blocks left, just append it as is and avoid decoding/recoding
            if i == self.blocks.len() - 1 {
                if !self.blocks[i].read() {
                    self.merged.push(self.blocks[i].clone());
                }
                i += 1;
            }

            // The remaining blocks can be combined and we know that they do not overlap and
            // so we can just append each, sort and re-encode.
            while i < self.blocks.len() && self.merged_string_values.len() < self.size as usize {
                let cur = &mut self.blocks[i];
                if cur.read() {
                    i += 1;
                    continue;
                }

                let mut v = StringValues(match decode_string_block(&cur.b) {
                    Ok(fv) => fv,
                    Err(e) => {
                        self.append_error(Error::StringErr {
                            reason: e.to_string(),
                        });
                        return Blocks::default();
                    }
                });

                // Apply each tombstone to the block
                for ts in cur.tombstones.iter() {
                    v.exclude(ts.min, ts.max);
                }

                cur.mark_read(cur.min_time, cur.max_time);

                self.merged_string_values.merge(v);
                i += 1;
            }

            self.blocks.drain(..i);

            self.chunk_string(false)
        }
    }

    fn chunk_string(&mut self, dedup: bool) -> Blocks {
        let mut dst = {
            if dedup {
                Blocks::default()
            } else {
                self.merged.clone()
            }
        };

        if self.merged_string_values.len() > self.size as usize {
            let values = self.merged_string_values[..self.size as usize].to_vec();
            let min_time = values[0].unix_nano;
            let max_time = values[values.len() - 1].unix_nano;
            let statistics = BlockStatistics::count_only(values.len());
            let cb = match StringValues(values).encode(&[]) {
                Ok(b) => b,
                Err(e) => {
                    self.append_error(Error::StringErr {
                        reason: e.to_string(),
                    });
                    return Blocks::default();
                }
            };

            dst.push(Block {
                min_time,
                max_time,
                key: self.key.clone(),
                statistics,
                b: cb,
                ..Default::default()
            });
            self.merged_string_values = StringValues(
                self.merged_string_values
                    .drain(self.size as usize..)
                    .collect(),
            );
            return dst;
        }

        // Re-encode the remaining values into the last block
        if !self.merged_string_values.is_empty() {
            let statistics = BlockStatistics::count_only(self.merged_string_values.len());
            let cb = match StringValues(self.merged_string_values.clone()).encode(&[]) {
                Ok(b) => b,
                Err(e) => {
                    self.append_error(Error::StringErr {
                        reason: e.to_string(),
                    });
                    return Blocks::default();
                }
            };
            dst.push(Block {
                min_time: self.merged_string_values[0].unix_nano,
                max_time: self.merged_string_values[self.merged_string_values.len() - 1].unix_nano,
                key: self.key.clone(),
                statistics,
                b: cb,
                ..Default::default()
            });
            self.merged_string_values.clear();
        }

        dst
    }

    //   /// Combines the next set of blocks into merged blocks.
    //   fn merge_vlog_index(&mut self) {

    //   }

    //    // combine returns a new set of blocks using the current blocks in the buffers.  If dedup
    // // is true, all the blocks will be decoded, dedup and sorted in in order.  If dedup is false,
    // // only blocks that are smaller than the chunk size will be decoded and combined.
    // fn combine_vlog_index(&mut self, dedup: bool) -> Blocks {

    // }

    // fn chunk_vlog_index(&mut self, dedup: bool) -> Blocks {

    // }
}

impl KeyIterator for TSMKeyIter {
    /// Returns true if there are any values remaining in the iterator.
    fn has_next(&mut self) -> bool {
        loop {
            // Any merged blocks pending?
            if !self.merged.is_empty() {
                // self.merged.drain(..1);
                // if !self.merged.is_empty() {
                //     return true;
                // }
                return true;
            }

            // Any merged values pending?
            if self.has_merged_values() {
                self.merge();
                if !self.merged.is_empty() || self.has_merged_values() {
                    return true;
                }
            }

            // If we still have blocks from the last read, merge them
            if !self.blocks.is_empty() {
                self.merge();
                if !self.merged.is_empty() || self.has_merged_values() {
                    return true;
                }
            }

            let mut errors = vec![];
            let mut iter_read_errors = vec![];
            // the outer for loop will iter all index blocks in tsm_i, add fill them to buf[i]
            for (i, v) in self.buf.iter_mut().enumerate() {
                if v.is_empty() {
                    let iter = &mut self.iterators[i]; // block iter in tsm_i file
                    if let Some(item) = iter.next() {
                        let reader::BlockIterItem {
                            key,
                            min_time,
                            max_time,
                            typ,
                            block: b,
                            statistics,
                            error: err,
                            ..
                        } = item;
                        if let Some(err) = err {
                            self.errs.push(Error::StringErr { reason: err });
                        }

                        // This block may have ranges of time removed from it that would
                        // reduce the block min and max time.
                        let tombstones = iter.r.tombstone_range(key.as_slice());

                        let mut vlen = v.len();
                        if v.capacity() > vlen {
                            let mut blk = Block {
                                min_time,
                                max_time,
                                typ,
                                statistics,
                                tombstones,
                                ..Default::default()
                            };
                            blk.key.clone_from(&key);
                            blk.b = b;
                            blk.read_min = i64::MAX;
                            blk.read_max = i64::MIN;

                            v.resize(vlen + 1, blk);
                            vlen += 1;
                        } else {
                            let mut blk = Block {
                                min_time,
                                max_time,
                                typ,
                                statistics,
                                tombstones,
                                ..Default::default()
                            };
                            blk.key.clone_from(&key);
                            blk.b = b;
                            blk.read_min = i64::MAX;
                            blk.read_max = i64::MIN;

                            v.push(blk);
                            vlen += 1;
                        }

                        let block_key = &key;
                        // iter all data blocks in tsm_i
                        while let Some(k) = iter.peek_next() {
                            if &k == block_key {
                                let reader::BlockIterItem {
                                    key,
                                    min_time,
                                    max_time,
                                    typ,
                                    block: b,
                                    statistics,
                                    error: err,
                                    ..
                                } = iter.next().unwrap();
                                if let Some(err) = err {
                                    iter_read_errors.push(err);
                                }

                                let tombstones = iter.r.tombstone_range(key.as_slice());

                                if v.capacity() > v.len() {
                                    let mut blk = Block {
                                        min_time,
                                        max_time,
                                        typ,
                                        statistics,
                                        tombstones,
                                        ..Default::default()
                                    };
                                    blk.key.clone_from(&key);
                                    blk.b = b;
                                    blk.read_min = i64::MAX;
                                    blk.read_max = i64::MIN;

                                    v.resize(vlen + 1, blk);
                                    vlen += 1;
                                } else {
                                    let mut blk = Block {
                                        min_time,
                                        max_time,
                                        typ,
                                        statistics,
                                        tombstones,
                                        ..Default::default()
                                    };
                                    blk.key.clone_from(&key);
                                    blk.b = b;
                                    blk.read_min = i64::MAX;
                                    blk.read_max = i64::MIN;

                                    v.push(blk);
                                    vlen += 1;
                                }
                            } else {
                                break;
                            }
                        }
                    }

                    if let Some(err) = iter.err() {
                        errors.push(err);
                    }
                }
            } // end for

            for err in iter_read_errors {
                self.append_error(Error::StringErr { reason: err });
            }

            for err in errors {
                self.append_error(Error::StringErr { reason: err });
            }

            // Each reader could have a different key that it's currently at, need to find
            // the next smallest one to keep the sort ordering.
            let (mut min_key, mut min_type) = (vec![], 0);
            for b in self.buf.iter() {
                // block could be nil if the iterator has been exhausted for that fil
                if b.is_empty() {
                    continue;
                }
                if min_key.is_empty() || b[0].key < min_key {
                    min_key.clone_from(&b[0].key);
                    min_type = b[0].typ;
                }
            }

            self.key = min_key;
            self.typ = min_type;

            // Now we need to find all blocks that match the min key so we can combine and dedupe
            // the blocks if necessary
            for b in self.buf.iter_mut() {
                if b.is_empty() {
                    continue;
                }

                if b[0].key == self.key {
                    self.blocks.extend_from_slice(b);
                    b.clear();
                }
            }

            if self.blocks.is_empty() {
                return false;
            }

            self.merge();

            // After merging all the values for this key, we might not have any.  (e.g. they were all deleted
            // through many tombstones).  In this case, move on to the next key instead of ending iteration.
            if self.merged.is_empty() {
                continue;
            }

            break;
        }

        !self.merged.is_empty()
    }

    fn close(&mut self) -> Result<()> {
        self.values = HashMap::new();
        self.pos = vec![];
        self.iterators = vec![];
        for r in self.readers.iter() {
            r.close().context(TSMReaderSnafu)?;
        }

        Ok(())
    }

    fn err(&self) -> Option<String> {
        self.errs.err()
    }

    fn estimated_index_size(&self) -> usize {
        let mut size = 0;
        for r in self.readers.iter() {
            size += r.index_size();
        }

        size as usize / self.readers.len()
    }
}

impl Iterator for TSMKeyIter {
    type Item = EncodedBlock;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.has_next() {
            return None;
        }

        // See if compactions were disabled while we were running.
        // select! {
        //     recv(self.interrupt.receiver()) -> _ => {
        //         self.append_error(Error::CompactionAborted {
        //             reason: "".to_string(),
        //         });
        //         return Some((vec![], 0,0,vec![], Some(Error::CompactionAborted { reason: "".to_string() }.to_string())));
        //     }
        //    default => {},
        // }
        if self.interrupt.is_cancelled() {
            self.append_error(Error::CompactionAborted {
                reason: "".to_string(),
            });
            return Some(EncodedBlock::error(Some(
                Error::CompactionAborted {
                    reason: "".to_string(),
                }
                .to_string(),
            )));
        }

        if self.merged.is_empty() {
            return Some(EncodedBlock::error(self.err()));
        }

        // TODO: optimize me, pop front from vec.
        let block = self.merged.drain(..1).next().unwrap();

        Some(EncodedBlock {
            typ: block.typ,
            key: Arc::new(block.key),
            min_time: block.min_time,
            max_time: block.max_time,
            statistics: block.statistics,
            block: block.b,
            error: self.err(),
        })
    }
}

/// TSMBatchKeyIter implements the KeyIter for set of TSMReaders.  Iteration produces
/// keys in sorted order and the values between the keys sorted and deduped.  If any of
/// the readers have associated tombstone entries, they are returned as part of iteration.
struct TSMBatchKeyIter {
    // readers is the set of readers it produce a sorted key run with
    readers: Vec<Arc<TSMReader>>,
    // values is the temporary buffers for each key that is returned by a reader
    values: HashMap<String, Value>,

    // pos is the current key position within the corresponding readers slice.  A value of
    // pos[0] = 1, means the reader[0] is currently at key 1 in its ordered index.
    pos: Vec<usize>,

    // errs is any error we received while iterating values.
    errs: TSMErrors,

    // indicates whether the iterator should choose a faster merging strategy over a more
    // optimally compressed one.  If fast is true, multiple blocks will just be added as is
    // and not combined.  In some cases, a slower path will need to be utilized even when
    // fast is true to prevent overlapping blocks of time for the same key.
    // If false, the blocks will be decoded and duplicated (if needed) and
    // then chunked into the maximally sized blocks.
    fast: bool,

    // size is the maximum number of values to encode in a single block
    size: i64,

    // key is the current key lowest key across all readers that has not be fully exhausted
    // of values.
    key: Vec<u8>,
    typ: u8,

    // tsmFiles are the string names of the files for use in tracking errors, ordered the same
    // as iterators and buf
    tsm_files: Vec<String>,
    // currentTsm is the current TSM file being iterated over
    current_tsm: String,
    // input, iterators[i] is all block iter in tsm_i file.
    iterators: Vec<BlockIter>,
    // output, all index blocks (index and data block content) in tsm files that equals to min_key
    blocks: Blocks,
    // output, buf[i] is all index blocks (index and data block content) in tsm_i file.
    buf: Vec<Blocks>,

    // mergeValues are decoded blocks that have been combined
    merged_float_values: FloatArray, // 对多个tsm文件某个seriesKey进行merge后得到的values
    merged_integer_values: IntegerArray,
    merged_unsigned_values: UnsignedArray,
    merged_bool_values: BoolArray,
    merged_string_values: StringArray,

    // merged are encoded blocks that have been combined or used as is without decode.
    // each call to next will return block from merged.
    merged: Blocks,
    interrupt: Option<CancellationToken>,
}

impl TSMBatchKeyIter {
    /// Returns a new TSM key iterator from readers.
    /// size indicates the maximum number of values to encode in a single block.
    pub fn new(
        size: i64,
        fast: bool,
        interrupt: Option<CancellationToken>,
        tsm_files: Vec<String>,
        readers: &[Arc<TSMReader>],
    ) -> Result<Self> {
        let mut iter = vec![];
        for r in readers {
            iter.push(TSMReader::block_iter(Arc::clone(r)));
        }

        let iter_len = iter.len();
        let tki = Self {
            readers: readers.to_vec(),
            values: HashMap::new(),
            pos: vec![0; readers.len()],
            errs: TSMErrors(vec![]),
            fast,
            tsm_files,
            current_tsm: String::new(),
            size,
            key: vec![],
            typ: 0,
            iterators: iter,
            blocks: Blocks::default(),
            buf: vec![Blocks::default(); iter_len],

            merged_integer_values: IntegerArray::default(),
            merged_unsigned_values: UnsignedArray::default(),
            merged_float_values: FloatArray::default(),
            merged_bool_values: BoolArray::default(),
            merged_string_values: StringArray::default(),

            merged: Blocks::default(),
            interrupt,
        };

        Ok(tki)
    }

    fn append_error(&mut self, err: Error) {
        self.errs.push(err);
    }

    fn has_merged_values(&self) -> bool {
        !self.merged_integer_values.is_empty()
            || !self.merged_unsigned_values.is_empty()
            || !self.merged_float_values.is_empty()
            || !self.merged_bool_values.is_empty()
            || !self.merged_string_values.is_empty()
    }

    /// merge combines the next set of blocks into merged blocks.
    fn merge(&mut self) {
        match self.typ {
            BLOCK_INTEGER => self.merge_integer(), // TODO: compact_gen
            BLOCK_UNSIGNED => self.merge_unsigned(),
            BLOCK_FLOAT64 => self.merge_float(),
            BLOCK_BOOL => self.merge_bool(),
            BLOCK_STRING => self.merge_string(),
            _ => self.append_error(Error::StringErr {
                reason: format!("unknown block type: {}", self.typ),
            }),
        }
    }

    fn handle_encode_error(&mut self, err: Error, typ: String) {
        self.append_error(Error::BlockRead {
            file: self.current_tsm.clone(),
            reason: format!(
                "encode error: unable to compress block type {} for key {:?}: {:?}",
                typ,
                self.key.clone(),
                err
            ),
        })
    }

    fn handle_decode_error(&mut self, err: Error, typ: String) {
        self.append_error(Error::BlockRead {
            file: self.current_tsm.clone(),
            reason: format!(
                "decode error: unable to decompress block type {} for key '{}': {}",
                typ,
                to_str!(&self.key),
                err
            ),
        })
    }

    /// Combines the next set of blocks into merged blocks.
    fn merge_integer(&mut self) {
        // No blocks left, or pending merged values, we're done
        if self.blocks.is_empty() && self.merged.is_empty() && self.merged_integer_values.is_empty()
        {
            return;
        }

        self.blocks.sort(); // sort stable

        let mut dedup = !self.merged_integer_values.is_empty();
        if !self.blocks.is_empty() && !dedup {
            // If we have more than one block or any partially tombstoned blocks, we many need to dedup
            dedup = !self.blocks[0].tombstones.is_empty() || self.blocks[0].partially_read();

            // Quickly scan each block to see if any overlap with the prior block, if they overlap then
            // we need to dedup as there may be duplicate points now
            let mut i = 1;
            while i < self.blocks.len() && !dedup {
                dedup = self.blocks[i].partially_read()
                    || self.blocks[i].overlaps_time_range(
                        self.blocks[i - 1].min_time,
                        self.blocks[i - 1].max_time,
                    )
                    || !self.blocks[i].tombstones.is_empty();

                i += 1;
            }
        }

        self.merged = self.combine_integer(dedup);
    }

    // combine returns a new set of blocks using the current blocks in the buffers.  If dedup
    // is true, all the blocks will be decoded, dedup and sorted in in order.  If dedup is false,
    // only blocks that are smaller than the chunk size will be decoded and combined.
    fn combine_integer(&mut self, dedup: bool) -> Blocks {
        if dedup {
            while self.merged_integer_values.len() < self.size as usize && !self.blocks.is_empty() {
                while !self.blocks.is_empty() && self.blocks[0].read() {
                    self.blocks.drain(..1);
                }

                if self.blocks.is_empty() {
                    break;
                }
                let first = &self.blocks[0];
                let mut min_time = first.min_time;
                let mut max_time = first.max_time;

                // Adjust the min time to the start of any overlapping blocks.
                for b in self.blocks.iter_mut() {
                    if b.overlaps_time_range(min_time, max_time) && !b.read() {
                        if b.min_time < min_time {
                            min_time = b.min_time;
                        }
                        if b.max_time > min_time && b.max_time < max_time {
                            max_time = b.max_time;
                        }
                    }
                }

                // We have some overlapping blocks so decode all, append in order and then dedup
                for b in self.blocks.iter_mut() {
                    if !b.overlaps_time_range(min_time, max_time) || b.read() {
                        continue;
                    }

                    let mut v = IntegerArray::default();
                    if let Err(e) = decode_integer_array_block(&b.b, &mut v) {
                        self.handle_decode_error(
                            Error::StringErr {
                                reason: e.to_string(),
                            },
                            "integer".to_string(),
                        );
                        return Blocks::default();
                    };

                    if b.max_time != v.max_time() {
                        if max_time == b.max_time {
                            max_time = v.max_time();
                        }
                        b.max_time = v.max_time();
                    }

                    // Remove values we already read
                    v.exclude(b.read_min, b.read_max);

                    // Filter out only the values for overlapping block
                    v.include(min_time, max_time);
                    if !v.is_empty() {
                        b.mark_read(v.min_time(), v.max_time());
                    }

                    // Apply each tombstone to the block
                    for ts in b.tombstones.iter() {
                        v.exclude(ts.min, ts.max);
                    }
                    self.merged_integer_values.merge(v);
                }
            }

            // Since we combined multiple blocks, we could have more values than we should put into
            // a single block.  We need to chunk them up into groups and re-encode them.
            self.chunk_integer(true)
        } else {
            let mut i = 0;

            while i < self.blocks.len() {
                let cur = &self.blocks[i];

                // skip this block if it's values were already read
                if cur.read() {
                    i += 1;
                    continue;
                }

                // If this block is already full, just add it as is
                let count = match block_count(&cur.b) {
                    Ok(n) => n,
                    Err(e) => {
                        // accumulate all errors to tsmKeyIterator.err
                        self.append_error(Error::StringErr {
                            reason: e.to_string(),
                        });
                        i += 1;
                        continue;
                    }
                };

                if (count as i64) < self.size {
                    break;
                }

                self.merged.push(cur.clone());
                i += 1;
            }

            if self.fast {
                while i < self.blocks.len() {
                    // skip this block if it's values were already read
                    if self.blocks[i].read() {
                        i += 1;
                        continue;
                    }

                    self.merged.push(self.blocks[i].clone());
                    i += 1;
                }
            }

            // If we only have 1 blocks left, just append it as is and avoid decoding/recoding
            if i == self.blocks.len() - 1 {
                if !self.blocks[i].read() {
                    self.merged.push(self.blocks[i].clone());
                }
                i += 1;
            }

            // The remaining blocks can be combined and we know that they do not overlap and
            // so we can just append each, sort and re-encode.
            while i < self.blocks.len() && self.merged_integer_values.len() < self.size as usize {
                let cur = &mut self.blocks[i];
                if cur.read() {
                    i += 1;
                    continue;
                }

                let mut v = IntegerArray::default();
                if let Err(e) = decode_integer_array_block(&cur.b, &mut v) {
                    self.handle_decode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "integer".to_string(),
                    );
                    return Blocks::default();
                }

                if cur.max_time != v.max_time() {
                    cur.max_time = v.max_time();
                }

                // Apply each tombstone to the block
                for ts in cur.tombstones.iter() {
                    v.exclude(ts.min, ts.max);
                }

                cur.mark_read(cur.min_time, cur.max_time);

                self.merged_integer_values.merge(v);
                i += 1;
            }

            self.blocks.drain(..i);

            self.chunk_integer(false)
        }
    }

    fn chunk_integer(&mut self, dedup: bool) -> Blocks {
        let mut dst = {
            if dedup {
                Blocks::default()
            } else {
                self.merged.clone()
            }
        };

        if self.merged_integer_values.len() > self.size as usize {
            let mut values = IntegerArray {
                timestamps: self.merged_integer_values.timestamps[..self.size as usize].to_vec(),
                ..Default::default()
            };
            let min_time = values.timestamps[0];
            let max_time = values.timestamps[values.len() - 1];
            values.values = self.merged_integer_values.values[..self.size as usize].to_vec();
            let statistics = BlockStatistics::from_integer(values.values.iter().copied());

            let cb = match encode_integer_array_block(&mut values, &[]) {
                // TODO: pool this buffer
                Ok(b) => b,
                Err(e) => {
                    self.handle_encode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "integer".to_string(),
                    );
                    return Blocks::default();
                }
            };

            dst.push(Block {
                min_time,
                max_time,
                key: self.key.clone(),
                statistics,
                b: cb,
                ..Default::default()
            });

            self.merged_integer_values.timestamps = self
                .merged_integer_values
                .timestamps
                .drain(self.size as usize..)
                .collect();
            self.merged_integer_values.values = self
                .merged_integer_values
                .values
                .drain(self.size as usize..)
                .collect();

            return dst;
        }

        // Re-encode the remaining values into the last block
        if !self.merged_integer_values.is_empty() {
            let (min_time, max_time) = (
                self.merged_integer_values.timestamps[0],
                self.merged_integer_values.timestamps
                    [self.merged_integer_values.timestamps.len() - 1],
            );
            let statistics =
                BlockStatistics::from_integer(self.merged_integer_values.values.iter().copied());
            let cb = match encode_integer_array_block(&mut self.merged_integer_values, &[]) {
                Ok(b) => b,
                Err(e) => {
                    self.handle_encode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "integer".to_string(),
                    );
                    return Blocks::default();
                }
            };
            dst.push(Block {
                min_time,
                max_time,
                key: self.key.clone(),
                statistics,
                b: cb,
                ..Default::default()
            });
            self.merged_integer_values.timestamps.clear();
            self.merged_integer_values.values.clear();
        }

        dst
    }

    /// Combines the next set of blocks into merged blocks.
    fn merge_unsigned(&mut self) {
        // No blocks left, or pending merged values, we're done
        if self.blocks.is_empty()
            && self.merged.is_empty()
            && self.merged_unsigned_values.is_empty()
        {
            return;
        }

        self.blocks.sort(); // sort stable

        let mut dedup = !self.merged_unsigned_values.is_empty();
        if !self.blocks.is_empty() && !dedup {
            // If we have more than one block or any partially tombstoned blocks, we many need to dedup
            dedup = !self.blocks[0].tombstones.is_empty() || self.blocks[0].partially_read();

            // Quickly scan each block to see if any overlap with the prior block, if they overlap then
            // we need to dedup as there may be duplicate points now
            let mut i = 1;
            while i < self.blocks.len() && !dedup {
                dedup = self.blocks[i].partially_read()
                    || self.blocks[i].overlaps_time_range(
                        self.blocks[i - 1].min_time,
                        self.blocks[i - 1].max_time,
                    )
                    || !self.blocks[i].tombstones.is_empty();

                i += 1;
            }
        }

        self.merged = self.combine_unsigned(dedup);
    }

    // combine returns a new set of blocks using the current blocks in the buffers.  If dedup
    // is true, all the blocks will be decoded, dedup and sorted in in order.  If dedup is false,
    // only blocks that are smaller than the chunk size will be decoded and combined.
    fn combine_unsigned(&mut self, dedup: bool) -> Blocks {
        if dedup {
            while self.merged_unsigned_values.len() < self.size as usize && !self.blocks.is_empty()
            {
                while !self.blocks.is_empty() && self.blocks[0].read() {
                    self.blocks.drain(..1);
                }

                if self.blocks.is_empty() {
                    break;
                }
                let first = &self.blocks[0];
                let mut min_time = first.min_time;
                let mut max_time = first.max_time;

                // Adjust the min time to the start of any overlapping blocks.
                for b in self.blocks.iter_mut() {
                    if b.overlaps_time_range(min_time, max_time) && !b.read() {
                        if b.min_time < min_time {
                            min_time = b.min_time;
                        }
                        if b.max_time > min_time && b.max_time < max_time {
                            max_time = b.max_time;
                        }
                    }
                }

                // We have some overlapping blocks so decode all, append in order and then dedup
                for b in self.blocks.iter_mut() {
                    if !b.overlaps_time_range(min_time, max_time) || b.read() {
                        continue;
                    }

                    let mut v = UnsignedArray::default();
                    if let Err(e) = decode_unsigned_array_block(&b.b, &mut v) {
                        self.handle_decode_error(
                            Error::StringErr {
                                reason: e.to_string(),
                            },
                            "unsigned".to_string(),
                        );
                        return Blocks::default();
                    }

                    if b.max_time != v.max_time() {
                        if max_time == b.max_time {
                            max_time = v.max_time();
                        }
                        b.max_time = v.max_time();
                    }

                    // Remove values we already read
                    v.exclude(b.read_min, b.read_max);

                    // Filter out only the values for overlapping block
                    v.include(min_time, max_time);
                    if !v.is_empty() {
                        b.mark_read(v.min_time(), v.max_time());
                    }

                    // Apply each tombstone to the block
                    for ts in b.tombstones.iter() {
                        v.exclude(ts.min, ts.max);
                    }
                    self.merged_unsigned_values.merge(v);
                }
            }

            // Since we combined multiple blocks, we could have more values than we should put into
            // a single block.  We need to chunk them up into groups and re-encode them.
            self.chunk_unsigned(true)
        } else {
            let mut i = 0;

            while i < self.blocks.len() {
                let cur = &self.blocks[i];

                // skip this block if it's values were already read
                if cur.read() {
                    i += 1;
                    continue;
                }

                // If this block is already full, just add it as is
                let count = match block_count(&cur.b) {
                    Ok(n) => n,
                    Err(e) => {
                        // accumulate all errors to tsmKeyIterator.err
                        self.append_error(Error::StringErr {
                            reason: e.to_string(),
                        });
                        i += 1;
                        continue;
                    }
                };

                if (count as i64) < self.size {
                    break;
                }

                self.merged.push(cur.clone());
                i += 1;
            }

            if self.fast {
                while i < self.blocks.len() {
                    // skip this block if it's values were already read
                    if self.blocks[i].read() {
                        i += 1;
                        continue;
                    }

                    self.merged.push(self.blocks[i].clone());
                    i += 1;
                }
            }

            // If we only have 1 blocks left, just append it as is and avoid decoding/recoding
            if i == self.blocks.len() - 1 {
                if !self.blocks[i].read() {
                    self.merged.push(self.blocks[i].clone());
                }
                i += 1;
            }

            // The remaining blocks can be combined and we know that they do not overlap and
            // so we can just append each, sort and re-encode.
            while i < self.blocks.len() && self.merged_unsigned_values.len() < self.size as usize {
                let cur = &mut self.blocks[i];
                if cur.read() {
                    i += 1;
                    continue;
                }

                let mut v = UnsignedArray::default();
                if let Err(e) = decode_unsigned_array_block(&cur.b, &mut v) {
                    self.handle_decode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "unsigned".to_string(),
                    );
                    return Blocks::default();
                }

                if cur.max_time != v.max_time() {
                    cur.max_time = v.max_time();
                }

                // Apply each tombstone to the block
                for ts in cur.tombstones.iter() {
                    v.exclude(ts.min, ts.max);
                }

                cur.mark_read(cur.min_time, cur.max_time);

                self.merged_unsigned_values.merge(v);
                i += 1;
            }

            self.blocks.drain(..i);

            self.chunk_unsigned(false)
        }
    }

    fn chunk_unsigned(&mut self, dedup: bool) -> Blocks {
        let mut dst = {
            if dedup {
                Blocks::default()
            } else {
                self.merged.clone()
            }
        };

        if self.merged_unsigned_values.len() > self.size as usize {
            let mut values = UnsignedArray {
                timestamps: self.merged_unsigned_values.timestamps[..self.size as usize].to_vec(),
                ..Default::default()
            };
            let min_time = values.timestamps[0];
            let max_time = values.timestamps[values.len() - 1];
            values.values = self.merged_unsigned_values.values[..self.size as usize].to_vec();
            let statistics = BlockStatistics::from_unsigned(values.values.iter().copied());

            let cb = match encode_unsigned_array_block(&mut values, &[]) {
                // TODO: pool this buffer
                Ok(b) => b,
                Err(e) => {
                    self.handle_encode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "unsigned".to_string(),
                    );
                    return Blocks::default();
                }
            };

            dst.push(Block {
                min_time,
                max_time,
                key: self.key.clone(),
                statistics,
                b: cb,
                ..Default::default()
            });

            self.merged_unsigned_values.timestamps = self
                .merged_unsigned_values
                .timestamps
                .drain(self.size as usize..)
                .collect();
            self.merged_unsigned_values.values = self
                .merged_unsigned_values
                .values
                .drain(self.size as usize..)
                .collect();

            return dst;
        }

        // Re-encode the remaining values into the last block
        if !self.merged_unsigned_values.is_empty() {
            let (min_time, max_time) = (
                self.merged_unsigned_values.timestamps[0],
                self.merged_unsigned_values.timestamps
                    [self.merged_unsigned_values.timestamps.len() - 1],
            );
            let statistics =
                BlockStatistics::from_unsigned(self.merged_unsigned_values.values.iter().copied());
            let cb = match encode_unsigned_array_block(&mut self.merged_unsigned_values, &[]) {
                Ok(b) => b,
                Err(e) => {
                    self.handle_encode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "unsigned".to_string(),
                    );
                    return Blocks::default();
                }
            };
            dst.push(Block {
                min_time,
                max_time,
                key: self.key.clone(),
                statistics,
                b: cb,
                ..Default::default()
            });
            self.merged_unsigned_values.timestamps.clear();
            self.merged_unsigned_values.values.clear();
        }

        dst
    }

    /// Combines the next set of blocks into merged blocks.
    fn merge_float(&mut self) {
        // No blocks left, or pending merged values, we're done
        if self.blocks.is_empty() && self.merged.is_empty() && self.merged_float_values.is_empty() {
            return;
        }

        self.blocks.sort(); // sort stable

        let mut dedup = !self.merged_float_values.is_empty();
        if !self.blocks.is_empty() && !dedup {
            // If we have more than one block or any partially tombstoned blocks, we many need to dedup
            dedup = !self.blocks[0].tombstones.is_empty() || self.blocks[0].partially_read();

            // Quickly scan each block to see if any overlap with the prior block, if they overlap then
            // we need to dedup as there may be duplicate points now
            let mut i = 1;
            while i < self.blocks.len() && !dedup {
                dedup = self.blocks[i].partially_read()
                    || self.blocks[i].overlaps_time_range(
                        self.blocks[i - 1].min_time,
                        self.blocks[i - 1].max_time,
                    )
                    || !self.blocks[i].tombstones.is_empty();

                i += 1;
            }
        }

        self.merged = self.combine_float(dedup);
    }

    // combine returns a new set of blocks using the current blocks in the buffers.  If dedup
    // is true, all the blocks will be decoded, dedup and sorted in in order.  If dedup is false,
    // only blocks that are smaller than the chunk size will be decoded and combined.
    fn combine_float(&mut self, dedup: bool) -> Blocks {
        if dedup {
            while self.merged_float_values.len() < self.size as usize && !self.blocks.is_empty() {
                while !self.blocks.is_empty() && self.blocks[0].read() {
                    self.blocks.drain(..1);
                }

                if self.blocks.is_empty() {
                    break;
                }
                let first = &self.blocks[0];
                let mut min_time = first.min_time;
                let mut max_time = first.max_time;

                // Adjust the min time to the start of any overlapping blocks.
                for b in self.blocks.iter_mut() {
                    if b.overlaps_time_range(min_time, max_time) && !b.read() {
                        if b.min_time < min_time {
                            min_time = b.min_time;
                        }
                        if b.max_time > min_time && b.max_time < max_time {
                            max_time = b.max_time;
                        }
                    }
                }

                // We have some overlapping blocks so decode all, append in order and then dedup
                for b in self.blocks.iter_mut() {
                    if !b.overlaps_time_range(min_time, max_time) || b.read() {
                        continue;
                    }

                    let mut v = FloatArray::default();
                    if let Err(e) = decode_float_array_block(&b.b, &mut v) {
                        self.handle_decode_error(
                            Error::StringErr {
                                reason: e.to_string(),
                            },
                            "float".to_string(),
                        );
                        return Blocks::default();
                    }

                    if b.max_time != v.max_time() {
                        if max_time == b.max_time {
                            max_time = v.max_time();
                        }
                        b.max_time = v.max_time();
                    }

                    // Remove values we already read
                    v.exclude(b.read_min, b.read_max);

                    // Filter out only the values for overlapping block
                    v.include(min_time, max_time);
                    if !v.is_empty() {
                        b.mark_read(v.min_time(), v.max_time());
                    }

                    // Apply each tombstone to the block
                    for ts in b.tombstones.iter() {
                        v.exclude(ts.min, ts.max);
                    }
                    self.merged_float_values.merge(v);
                }
            }

            // Since we combined multiple blocks, we could have more values than we should put into
            // a single block.  We need to chunk them up into groups and re-encode them.
            self.chunk_float(true)
        } else {
            let mut i = 0;

            while i < self.blocks.len() {
                let cur = &self.blocks[i];

                // skip this block if it's values were already read
                if cur.read() {
                    i += 1;
                    continue;
                }

                // If this block is already full, just add it as is
                let count = match block_count(&cur.b) {
                    Ok(n) => n,
                    Err(e) => {
                        // accumulate all errors to tsmKeyIterator.err
                        self.append_error(Error::StringErr {
                            reason: e.to_string(),
                        });
                        i += 1;
                        continue;
                    }
                };

                if (count as i64) < self.size {
                    break;
                }

                self.merged.push(cur.clone());
                i += 1;
            }

            if self.fast {
                while i < self.blocks.len() {
                    // skip this block if it's values were already read
                    if self.blocks[i].read() {
                        i += 1;
                        continue;
                    }

                    self.merged.push(self.blocks[i].clone());
                    i += 1;
                }
            }

            // If we only have 1 blocks left, just append it as is and avoid decoding/recoding
            if i == self.blocks.len() - 1 {
                if !self.blocks[i].read() {
                    self.merged.push(self.blocks[i].clone());
                }
                i += 1;
            }

            // The remaining blocks can be combined and we know that they do not overlap and
            // so we can just append each, sort and re-encode.
            while i < self.blocks.len() && self.merged_float_values.len() < self.size as usize {
                let cur = &mut self.blocks[i];
                if cur.read() {
                    i += 1;
                    continue;
                }

                let mut v = FloatArray::default();
                if let Err(e) = decode_float_array_block(&cur.b, &mut v) {
                    self.handle_decode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "float".to_string(),
                    );
                    return Blocks::default();
                };

                if cur.max_time != v.max_time() {
                    cur.max_time = v.max_time();
                }

                // Apply each tombstone to the block
                for ts in cur.tombstones.iter() {
                    v.exclude(ts.min, ts.max);
                }

                cur.mark_read(cur.min_time, cur.max_time);

                self.merged_float_values.merge(v);
                i += 1;
            }

            self.blocks.drain(..i);

            self.chunk_float(false)
        }
    }

    fn chunk_float(&mut self, dedup: bool) -> Blocks {
        let mut dst = {
            if dedup {
                Blocks::default()
            } else {
                self.merged.clone()
            }
        };

        if self.merged_float_values.len() > self.size as usize {
            let mut values = FloatArray {
                timestamps: self.merged_float_values.timestamps[..self.size as usize].to_vec(),
                ..Default::default()
            };
            let min_time = values.timestamps[0];
            let max_time = values.timestamps[values.len() - 1];
            values.values = self.merged_float_values.values[..self.size as usize].to_vec();
            let statistics = BlockStatistics::from_float(values.values.iter().copied());

            let cb = match encode_float_array_block(&mut values, &[]) {
                // TODO: pool this buffer
                Ok(b) => b,
                Err(e) => {
                    self.handle_encode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "float".to_string(),
                    );
                    return Blocks::default();
                }
            };

            dst.push(Block {
                min_time,
                max_time,
                key: self.key.clone(),
                statistics,
                b: cb,
                ..Default::default()
            });

            self.merged_float_values.timestamps = self
                .merged_float_values
                .timestamps
                .drain(self.size as usize..)
                .collect();
            self.merged_float_values.values = self
                .merged_float_values
                .values
                .drain(self.size as usize..)
                .collect();

            return dst;
        }

        // Re-encode the remaining values into the last block
        if !self.merged_float_values.is_empty() {
            let (min_time, max_time) = (
                self.merged_float_values.timestamps[0],
                self.merged_float_values.timestamps[self.merged_float_values.timestamps.len() - 1],
            );
            let statistics =
                BlockStatistics::from_float(self.merged_float_values.values.iter().copied());
            let cb = match encode_float_array_block(&mut self.merged_float_values, &[]) {
                Ok(b) => b,
                Err(e) => {
                    self.handle_encode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "float".to_string(),
                    );
                    return Blocks::default();
                }
            };
            dst.push(Block {
                min_time,
                max_time,
                key: self.key.clone(),
                statistics,
                b: cb,
                ..Default::default()
            });
            self.merged_float_values.timestamps.clear();
            self.merged_float_values.values.clear();
        }

        dst
    }

    /// Combines the next set of blocks into merged blocks.
    fn merge_bool(&mut self) {
        // No blocks left, or pending merged values, we're done
        if self.blocks.is_empty() && self.merged.is_empty() && self.merged_bool_values.is_empty() {
            return;
        }

        self.blocks.sort(); // sort stable

        let mut dedup = !self.merged_bool_values.is_empty();
        if !self.blocks.is_empty() && !dedup {
            // If we have more than one block or any partially tombstoned blocks, we many need to dedup
            dedup = !self.blocks[0].tombstones.is_empty() || self.blocks[0].partially_read();

            // Quickly scan each block to see if any overlap with the prior block, if they overlap then
            // we need to dedup as there may be duplicate points now
            let mut i = 1;
            while i < self.blocks.len() && !dedup {
                dedup = self.blocks[i].partially_read()
                    || self.blocks[i].overlaps_time_range(
                        self.blocks[i - 1].min_time,
                        self.blocks[i - 1].max_time,
                    )
                    || !self.blocks[i].tombstones.is_empty();

                i += 1;
            }
        }

        self.merged = self.combine_bool(dedup);
    }

    // combine returns a new set of blocks using the current blocks in the buffers.  If dedup
    // is true, all the blocks will be decoded, dedup and sorted in in order.  If dedup is false,
    // only blocks that are smaller than the chunk size will be decoded and combined.
    fn combine_bool(&mut self, dedup: bool) -> Blocks {
        if dedup {
            while self.merged_bool_values.len() < self.size as usize && !self.blocks.is_empty() {
                while !self.blocks.is_empty() && self.blocks[0].read() {
                    self.blocks.drain(..1);
                }

                if self.blocks.is_empty() {
                    break;
                }
                let first = &self.blocks[0];
                let mut min_time = first.min_time;
                let mut max_time = first.max_time;

                // Adjust the min time to the start of any overlapping blocks.
                for b in self.blocks.iter_mut() {
                    if b.overlaps_time_range(min_time, max_time) && !b.read() {
                        if b.min_time < min_time {
                            min_time = b.min_time;
                        }
                        if b.max_time > min_time && b.max_time < max_time {
                            max_time = b.max_time;
                        }
                    }
                }

                // We have some overlapping blocks so decode all, append in order and then dedup
                for b in self.blocks.iter_mut() {
                    if !b.overlaps_time_range(min_time, max_time) || b.read() {
                        continue;
                    }

                    let mut v = BoolArray::default();
                    if let Err(e) = decode_bool_array_block(&b.b, &mut v) {
                        self.handle_decode_error(
                            Error::StringErr {
                                reason: e.to_string(),
                            },
                            "bool".to_string(),
                        );
                        return Blocks::default();
                    };

                    if b.max_time != v.max_time() {
                        if max_time == b.max_time {
                            max_time = v.max_time();
                        }
                        b.max_time = v.max_time();
                    }

                    // Remove values we already read
                    v.exclude(b.read_min, b.read_max);

                    // Filter out only the values for overlapping block
                    v.include(min_time, max_time);
                    if !v.is_empty() {
                        b.mark_read(v.min_time(), v.max_time());
                    }

                    // Apply each tombstone to the block
                    for ts in b.tombstones.iter() {
                        v.exclude(ts.min, ts.max);
                    }
                    self.merged_bool_values.merge(v);
                }
            }

            // Since we combined multiple blocks, we could have more values than we should put into
            // a single block.  We need to chunk them up into groups and re-encode them.
            self.chunk_bool(true)
        } else {
            let mut i = 0;

            while i < self.blocks.len() {
                let cur = &self.blocks[i];

                // skip this block if it's values were already read
                if cur.read() {
                    i += 1;
                    continue;
                }

                // If this block is already full, just add it as is
                let count = match block_count(&cur.b) {
                    Ok(n) => n,
                    Err(e) => {
                        // accumulate all errors to tsmKeyIterator.err
                        self.append_error(Error::StringErr {
                            reason: e.to_string(),
                        });
                        i += 1;
                        continue;
                    }
                };

                if (count as i64) < self.size {
                    break;
                }

                self.merged.push(cur.clone());
                i += 1;
            }

            if self.fast {
                while i < self.blocks.len() {
                    // skip this block if it's values were already read
                    if self.blocks[i].read() {
                        i += 1;
                        continue;
                    }

                    self.merged.push(self.blocks[i].clone());
                    i += 1;
                }
            }

            // If we only have 1 blocks left, just append it as is and avoid decoding/recoding
            if i == self.blocks.len() - 1 {
                if !self.blocks[i].read() {
                    self.merged.push(self.blocks[i].clone());
                }
                i += 1;
            }

            // The remaining blocks can be combined and we know that they do not overlap and
            // so we can just append each, sort and re-encode.
            while i < self.blocks.len() && self.merged_bool_values.len() < self.size as usize {
                let cur = &mut self.blocks[i];
                if cur.read() {
                    i += 1;
                    continue;
                }

                let mut v = BoolArray::default();
                if let Err(e) = decode_bool_array_block(&cur.b, &mut v) {
                    self.handle_decode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "bool".to_string(),
                    );
                    return Blocks::default();
                };

                if cur.max_time != v.max_time() {
                    cur.max_time = v.max_time();
                }

                // Apply each tombstone to the block
                for ts in cur.tombstones.iter() {
                    v.exclude(ts.min, ts.max);
                }

                cur.mark_read(cur.min_time, cur.max_time);

                self.merged_bool_values.merge(v);
                i += 1;
            }

            self.blocks.drain(..i);

            self.chunk_bool(false)
        }
    }

    fn chunk_bool(&mut self, dedup: bool) -> Blocks {
        let mut dst = {
            if dedup {
                Blocks::default()
            } else {
                self.merged.clone()
            }
        };

        if self.merged_bool_values.len() > self.size as usize {
            let mut values = BoolArray {
                timestamps: self.merged_bool_values.timestamps[..self.size as usize].to_vec(),
                ..Default::default()
            };
            let min_time = values.timestamps[0];
            let max_time = values.timestamps[values.len() - 1];
            values.values = self.merged_bool_values.values[..self.size as usize].to_vec();
            let statistics = BlockStatistics::from_bool(values.values.iter().copied());

            let cb = match encode_bool_array_block(&mut values, &[]) {
                // TODO: pool this buffer
                Ok(b) => b,
                Err(e) => {
                    self.handle_encode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "bool".to_string(),
                    );
                    return Blocks::default();
                }
            };

            dst.push(Block {
                min_time,
                max_time,
                key: self.key.clone(),
                statistics,
                b: cb,
                ..Default::default()
            });

            self.merged_bool_values.timestamps = self
                .merged_bool_values
                .timestamps
                .drain(self.size as usize..)
                .collect();
            self.merged_bool_values.values = self
                .merged_bool_values
                .values
                .drain(self.size as usize..)
                .collect();

            return dst;
        }

        // Re-encode the remaining values into the last block
        if !self.merged_bool_values.is_empty() {
            let (min_time, max_time) = (
                self.merged_bool_values.timestamps[0],
                self.merged_bool_values.timestamps[self.merged_bool_values.timestamps.len() - 1],
            );
            let statistics =
                BlockStatistics::from_bool(self.merged_bool_values.values.iter().copied());
            let cb = match encode_bool_array_block(&mut self.merged_bool_values, &[]) {
                Ok(b) => b,
                Err(e) => {
                    self.handle_encode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "bool".to_string(),
                    );
                    return Blocks::default();
                }
            };
            dst.push(Block {
                min_time,
                max_time,
                key: self.key.clone(),
                statistics,
                b: cb,
                ..Default::default()
            });
            self.merged_bool_values.timestamps.clear();
            self.merged_bool_values.values.clear();
        }

        dst
    }

    /// Combines the next set of blocks into merged blocks.
    fn merge_string(&mut self) {
        // No blocks left, or pending merged values, we're done
        if self.blocks.is_empty() && self.merged.is_empty() && self.merged_string_values.is_empty()
        {
            return;
        }

        self.blocks.sort(); // sort stable

        let mut dedup = !self.merged_string_values.is_empty();
        if !self.blocks.is_empty() && !dedup {
            // If we have more than one block or any partially tombstoned blocks, we many need to dedup
            dedup = !self.blocks[0].tombstones.is_empty() || self.blocks[0].partially_read();

            // Quickly scan each block to see if any overlap with the prior block, if they overlap then
            // we need to dedup as there may be duplicate points now
            let mut i = 1;
            while i < self.blocks.len() && !dedup {
                dedup = self.blocks[i].partially_read()
                    || self.blocks[i].overlaps_time_range(
                        self.blocks[i - 1].min_time,
                        self.blocks[i - 1].max_time,
                    )
                    || !self.blocks[i].tombstones.is_empty();

                i += 1;
            }
        }

        self.merged = self.combine_string(dedup);
    }

    // combine returns a new set of blocks using the current blocks in the buffers.  If dedup
    // is true, all the blocks will be decoded, dedup and sorted in in order.  If dedup is false,
    // only blocks that are smaller than the chunk size will be decoded and combined.
    fn combine_string(&mut self, dedup: bool) -> Blocks {
        if dedup {
            while self.merged_string_values.len() < self.size as usize && !self.blocks.is_empty() {
                while !self.blocks.is_empty() && self.blocks[0].read() {
                    self.blocks.drain(..1);
                }

                if self.blocks.is_empty() {
                    break;
                }
                let first = &self.blocks[0];
                let mut min_time = first.min_time;
                let mut max_time = first.max_time;

                // Adjust the min time to the start of any overlapping blocks.
                for b in self.blocks.iter_mut() {
                    if b.overlaps_time_range(min_time, max_time) && !b.read() {
                        if b.min_time < min_time {
                            min_time = b.min_time;
                        }
                        if b.max_time > min_time && b.max_time < max_time {
                            max_time = b.max_time;
                        }
                    }
                }

                // We have some overlapping blocks so decode all, append in order and then dedup
                for b in self.blocks.iter_mut() {
                    if !b.overlaps_time_range(min_time, max_time) || b.read() {
                        continue;
                    }

                    let mut v = StringArray::default();
                    if let Err(e) = decode_string_array_block(&b.b, &mut v) {
                        self.handle_decode_error(
                            Error::StringErr {
                                reason: e.to_string(),
                            },
                            "string".to_string(),
                        );
                        return Blocks::default();
                    };

                    if b.max_time != v.max_time() {
                        if max_time == b.max_time {
                            max_time = v.max_time();
                        }
                        b.max_time = v.max_time();
                    }

                    // Remove values we already read
                    v.exclude(b.read_min, b.read_max);

                    // Filter out only the values for overlapping block
                    v.include(min_time, max_time);
                    if !v.is_empty() {
                        b.mark_read(v.min_time(), v.max_time());
                    }

                    // Apply each tombstone to the block
                    for ts in b.tombstones.iter() {
                        v.exclude(ts.min, ts.max);
                    }
                    self.merged_string_values.merge(v);
                }
            }

            // Since we combined multiple blocks, we could have more values than we should put into
            // a single block.  We need to chunk them up into groups and re-encode them.
            self.chunk_string(true)
        } else {
            let mut i = 0;

            while i < self.blocks.len() {
                let cur = &self.blocks[i];

                // skip this block if it's values were already read
                if cur.read() {
                    i += 1;
                    continue;
                }

                // If this block is already full, just add it as is
                let count = match block_count(&cur.b) {
                    Ok(n) => n,
                    Err(e) => {
                        // accumulate all errors to tsmKeyIterator.err
                        self.append_error(Error::StringErr {
                            reason: e.to_string(),
                        });
                        i += 1;
                        continue;
                    }
                };

                if (count as i64) < self.size {
                    break;
                }

                self.merged.push(cur.clone());
                i += 1;
            }

            if self.fast {
                while i < self.blocks.len() {
                    // skip this block if it's values were already read
                    if self.blocks[i].read() {
                        i += 1;
                        continue;
                    }

                    self.merged.push(self.blocks[i].clone());
                    i += 1;
                }
            }

            // If we only have 1 blocks left, just append it as is and avoid decoding/recoding
            if i == self.blocks.len() - 1 {
                if !self.blocks[i].read() {
                    self.merged.push(self.blocks[i].clone());
                }
                i += 1;
            }

            // The remaining blocks can be combined and we know that they do not overlap and
            // so we can just append each, sort and re-encode.
            while i < self.blocks.len() && self.merged_string_values.len() < self.size as usize {
                let cur = &mut self.blocks[i];
                if cur.read() {
                    i += 1;
                    continue;
                }

                let mut v = StringArray::default();
                if let Err(e) = decode_string_array_block(&cur.b, &mut v) {
                    self.handle_decode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "string".to_string(),
                    );
                    return Blocks::default();
                };

                if cur.max_time != v.max_time() {
                    cur.max_time = v.max_time();
                }

                // Apply each tombstone to the block
                for ts in cur.tombstones.iter() {
                    v.exclude(ts.min, ts.max);
                }

                cur.mark_read(cur.min_time, cur.max_time);

                self.merged_string_values.merge(v);
                i += 1;
            }

            self.blocks.drain(..i);

            self.chunk_string(false)
        }
    }

    fn chunk_string(&mut self, dedup: bool) -> Blocks {
        let mut dst = {
            if dedup {
                Blocks::default()
            } else {
                self.merged.clone()
            }
        };

        if self.merged_string_values.len() > self.size as usize {
            let mut values = StringArray {
                timestamps: self.merged_string_values.timestamps[..self.size as usize].to_vec(),
                ..Default::default()
            };
            let min_time = values.timestamps[0];
            let max_time = values.timestamps[values.len() - 1];
            values.values = self.merged_string_values.values[..self.size as usize].to_vec();
            let statistics = BlockStatistics::count_only(values.values.len());

            let cb = match encode_string_array_block(&mut values, &[]) {
                // TODO: pool this buffer
                Ok(b) => b,
                Err(e) => {
                    self.handle_encode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "string".to_string(),
                    );
                    return Blocks::default();
                }
            };

            dst.push(Block {
                min_time,
                max_time,
                key: self.key.clone(),
                statistics,
                b: cb,
                ..Default::default()
            });

            self.merged_string_values.timestamps = self
                .merged_string_values
                .timestamps
                .drain(self.size as usize..)
                .collect();
            self.merged_string_values.values = self
                .merged_string_values
                .values
                .drain(self.size as usize..)
                .collect();

            return dst;
        }

        // Re-encode the remaining values into the last block
        if !self.merged_string_values.is_empty() {
            let (min_time, max_time) = (
                self.merged_string_values.timestamps[0],
                self.merged_string_values.timestamps
                    [self.merged_string_values.timestamps.len() - 1],
            );
            let statistics = BlockStatistics::count_only(self.merged_string_values.values.len());
            let cb = match encode_string_array_block(&mut self.merged_string_values, &[]) {
                Ok(b) => b,
                Err(e) => {
                    self.handle_encode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "string".to_string(),
                    );
                    return Blocks::default();
                }
            };
            dst.push(Block {
                min_time,
                max_time,
                key: self.key.clone(),
                statistics,
                b: cb,
                ..Default::default()
            });
            self.merged_string_values.timestamps.clear();
            self.merged_string_values.values.clear();
        }

        dst
    }
}

impl KeyIterator for TSMBatchKeyIter {
    /// Returns true if there are any values remaining in the iterator.
    /// Every time has_next is called, wen will find all data of the smallest_key in all compact tsm files,
    /// merge and remove the tombstoned data, the smallest_key is the the smallest key in all compact tsm files.
    fn has_next(&mut self) -> bool {
        loop {
            // Any merged blocks pending?
            if !self.merged.is_empty() {
                // self.merged.drain(0..1);
                // if !self.merged.is_empty() {
                //     return true;
                // }
                return true;
            }

            // Any merged values pending?
            if self.has_merged_values() {
                self.merge();
                if !self.merged.is_empty() || self.has_merged_values() {
                    return true;
                }
            }

            // If we still have blocks from the last read, merge them
            if !self.blocks.is_empty() {
                self.merge();
                if !self.merged.is_empty() || self.has_merged_values() {
                    return true;
                }
            }

            let mut errors = vec![];
            let mut iter_read_errors = vec![];
            // Read the next block from each TSM iterator
            for (i, v) in self.buf.iter_mut().enumerate() {
                // v contains all IndexBlock for tsm_i file.
                if !v.is_empty() {
                    continue;
                }

                let iter = &mut self.iterators[i]; // block iter for tsm_i file
                self.current_tsm.clone_from(&self.tsm_files[i]);
                if let Some(item) = iter.next() {
                    let reader::BlockIterItem {
                        key,
                        min_time,
                        max_time,
                        typ,
                        block: b,
                        statistics,
                        error: err,
                        ..
                    } = item;
                    if let Some(err) = err {
                        self.errs.push(Error::BlockRead {
                            file: self.current_tsm.clone(),
                            reason: err,
                        });
                    }

                    // This block may have ranges of time removed from it that would
                    // reduce the block min and max time.
                    let tombstones = iter.r.tombstone_range(key.as_slice());

                    let mut vlen = v.len();
                    if v.capacity() > vlen {
                        let mut blk = Block {
                            min_time,
                            max_time,
                            typ,
                            statistics,
                            tombstones,
                            read_min: i64::MAX,
                            read_max: i64::MIN,
                            ..Default::default()
                        };
                        blk.key.clone_from(&key);
                        blk.b = b;
                        v.resize(vlen + 1, blk);
                        vlen += 1;
                    } else {
                        let mut blk = Block {
                            min_time,
                            max_time,
                            typ,
                            statistics,
                            tombstones,
                            read_min: i64::MAX,
                            read_max: i64::MIN,
                            ..Default::default()
                        };
                        blk.key.clone_from(&key);
                        blk.b = b;

                        v.push(blk);
                        vlen += 1;
                    }

                    let block_key = &key;
                    while let Some(k) = iter.peek_next() {
                        if &k == block_key {
                            let reader::BlockIterItem {
                                key,
                                min_time,
                                max_time,
                                typ,
                                block: b,
                                statistics,
                                error: err,
                                ..
                            } = iter.next().unwrap();
                            if let Some(err) = err {
                                iter_read_errors.push(Error::BlockRead {
                                    file: self.current_tsm.clone(),
                                    reason: err,
                                });
                            }

                            let tombstones = iter.r.tombstone_range(key.as_slice());

                            if v.capacity() > v.len() {
                                let mut blk = Block {
                                    min_time,
                                    max_time,
                                    typ,
                                    statistics,
                                    tombstones,
                                    read_min: i64::MAX,
                                    read_max: i64::MIN,
                                    ..Default::default()
                                };
                                blk.key.clone_from(&key);
                                blk.b = b;

                                v.resize(vlen + 1, blk);
                                vlen += 1;
                            } else {
                                let mut blk = Block {
                                    min_time,
                                    max_time,
                                    typ,
                                    statistics,
                                    tombstones,
                                    read_min: i64::MAX,
                                    read_max: i64::MIN,
                                    ..Default::default()
                                };
                                blk.key.clone_from(&key);
                                blk.b = b;

                                v.push(blk);
                                vlen += 1;
                            }
                        } else {
                            break;
                        }
                    }
                }

                if let Some(err) = iter.err() {
                    errors.push(Error::BlockRead {
                        file: self.current_tsm.clone(),
                        reason: err,
                    });
                }
            } // end for

            for err in iter_read_errors {
                self.append_error(err);
            }

            for err in errors {
                self.append_error(err);
            }

            // Each reader could have a different key that it's currently at, need to find
            // the next smallest one to keep the sort ordering.
            // min_key is the smallest key in all compact tsm files.
            let (mut min_key, mut min_type) = (vec![], 0);
            for b in self.buf.iter() {
                // block could be nil if the iterator has been exhausted for that fil
                if b.is_empty() {
                    continue;
                }
                if min_key.is_empty() || b[0].key < min_key {
                    min_key = b[0].key.clone();
                    min_type = b[0].typ;
                }
            }

            self.key = min_key;
            self.typ = min_type;

            // Now we need to find all blocks that match the min key so we can combine and dedupe
            // the blocks if necessary
            for b in self.buf.iter_mut() {
                if b.is_empty() {
                    continue;
                }

                if b[0].key == self.key {
                    self.blocks.extend_from_slice(b);
                    b.clear();
                }
            }

            if self.blocks.is_empty() {
                return false;
            }

            self.merge();

            // After merging all the values for this key, we might not have any.  (e.g. they were all deleted
            // through many tombstones).  In this case, move on to the next key instead of ending iteration.
            if self.merged.is_empty() {
                continue;
            }

            break;
        }
        !self.merged.is_empty()
    }

    fn close(&mut self) -> Result<()> {
        self.values = HashMap::new();
        self.pos = vec![];
        self.iterators = vec![];
        for r in self.readers.iter() {
            r.close().context(TSMReaderSnafu)?;
        }

        Ok(())
    }

    /// Returns any errors encountered during iteration.
    fn err(&self) -> Option<String> {
        self.errs.err()
    }

    fn estimated_index_size(&self) -> usize {
        let mut size = 0;
        for r in self.readers.iter() {
            size += r.index_size();
        }

        size as usize / self.readers.len()
    }
}

impl Iterator for TSMBatchKeyIter {
    type Item = EncodedBlock;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.has_next() {
            return None;
        }

        // See if compactions were disabled while we were running.
        if let Some(interrupt) = &self.interrupt {
            // select! {
            //     recv(interrupt.receiver()) -> _ => {
            //         self.append_error(Error::CompactionAborted {
            //             reason: "".to_string(),
            //         });
            //         return Some((vec![], 0,0,vec![], Some(Error::CompactionAborted { reason: "".to_string() }.to_string())));
            //     },
            //     default => {},
            // }
            if interrupt.is_cancelled() {
                self.append_error(Error::CompactionAborted {
                    reason: "".to_string(),
                });
                return Some(EncodedBlock::error(Some(
                    Error::CompactionAborted {
                        reason: "".to_string(),
                    }
                    .to_string(),
                )));
            }
        }

        if self.merged.is_empty() {
            return Some(EncodedBlock::error(self.err()));
        }

        // TODO: optimize me, pop front from vec.
        // let block = &self.merged[0];
        let block = self.merged.drain(..1).next().unwrap();
        Some(EncodedBlock {
            typ: block.typ,
            key: Arc::new(block.key),
            min_time: block.min_time,
            max_time: block.max_time,
            statistics: block.statistics,
            block: block.b,
            error: self.err(),
        })
    }
}

struct CacheKeyIter {
    cache: Arc<Cache>,
    size: usize, // the maximum number of points in an encoded block in a TSM file
    order: Arc<Vec<Vec<u8>>>, // all sorted sereis_keys in the cache ring

    n: usize, // keys count
    i: i64,
    blocks: Arc<Vec<Mutex<Vec<CacheBlock>>>>, // encoded values, the max encoded values is DefaultMaxPointsPerBlock
    ready: Arc<Mutex<Vec<CancellationTokenBounded>>>, // Notify the ith key is fully encoded or not
    interrupt: Option<CancellationToken>,
    err: Arc<RwLock<Option<Error>>>,
}

#[derive(Clone)]
struct CacheBlock {
    k: Arc<Vec<u8>>, // key
    typ: u8,
    min_time: i64,
    max_time: i64,
    statistics: Option<BlockStatistics>,
    b: Vec<u8>,
    err: Option<String>,
}

impl CacheKeyIter {
    pub async fn new(cache: Arc<Cache>, size: usize, interrupt: Option<CancellationToken>) -> Self {
        Self::new_with_executor(cache, size, interrupt, legacy_global_executor())
            .await
            .expect("legacy compaction executor rejected cache encoding")
    }

    async fn new_with_executor(
        cache: Arc<Cache>,
        size: usize,
        interrupt: Option<CancellationToken>,
        executor: ExecutorRef,
    ) -> Result<Self> {
        let keys = cache.keys();
        let keys_len = keys.len();
        let mut ready = Vec::with_capacity(keys_len);
        let mut blocks = Vec::with_capacity(keys_len);
        for _ in 0..keys_len {
            ready.push(CancellationTokenBounded::new(1));
            blocks.push(Mutex::new(vec![]));
        }

        let cki = Self {
            n: keys_len,
            i: -1,
            size, // DEFAULT_MAX_POINTS_PER_BLOCK
            cache,
            order: Arc::new(keys),
            ready: Arc::new(Mutex::new(ready)),
            blocks: Arc::new(blocks),
            interrupt,
            err: Arc::new(RwLock::new(None)),
        };

        let size = cki.size;
        let cache = Arc::clone(&cki.cache);
        let order = Arc::clone(&cki.order);
        let blocks = Arc::clone(&cki.blocks);
        let ready = Arc::clone(&cki.ready);
        let cerr = Arc::clone(&cki.err);

        Self::encode(size, keys_len, cache, order, blocks, ready, cerr, executor).await?;

        Ok(cki)
    }

    async fn encode(
        size: usize,
        n: usize,
        cache: Arc<Cache>,
        order: Arc<Vec<Vec<u8>>>,
        blocks: Arc<Vec<Mutex<Vec<CacheBlock>>>>,
        ready: Arc<Mutex<Vec<CancellationTokenBounded>>>,
        err: Arc<RwLock<Option<Error>>>,
        executor: ExecutorRef,
    ) -> Result<()> {
        let concurrency = num_cpus::get();
        // let n = ready.len();

        // Assign each key to exactly one worker so accepted-task cancellation
        // can release that worker's readiness tokens without double signaling.
        let terminals = Arc::new((0..n).map(|_| AtomicBool::new(false)).collect::<Vec<_>>());

        let mut handles = Vec::with_capacity(concurrency);
        for worker in 0..concurrency {
            let cache = Arc::clone(&cache);
            let order = Arc::clone(&order);
            let blocks = Arc::clone(&blocks);
            let ready = Arc::clone(&ready);
            let cerr = Arc::clone(&err);
            let cancelled_order = Arc::clone(&order);
            let cancelled_blocks = Arc::clone(&blocks);
            let cancelled_ready = Arc::clone(&ready);
            let cancelled_error = Arc::clone(&err);
            let task_terminals = Arc::clone(&terminals);
            let cancelled_terminals = Arc::clone(&terminals);
            let (registration, completion) = accepted_task_guard(move || {
                let reason = "cache encoder task terminated before completion".to_string();
                *cancelled_error.write() = Some(Error::StringErr {
                    reason: reason.clone(),
                });
                for i in (worker..n).step_by(concurrency) {
                    if cancelled_terminals[i].swap(true, Ordering::SeqCst) {
                        continue;
                    }
                    let mut encoded = cancelled_blocks[i].lock();
                    encoded.clear();
                    encoded.push(CacheBlock {
                        typ: EMPTY,
                        k: Arc::new(cancelled_order[i].clone()),
                        min_time: 0,
                        max_time: 0,
                        statistics: None,
                        b: vec![],
                        err: Some(reason.clone()),
                    });
                    drop(encoded);
                    unsafe {
                        let ready: &mut Vec<CancellationTokenBounded> =
                            &mut *cancelled_ready.data_ptr();
                        let _ = ready[i].send();
                    }
                }
            });
            // Run one task per CPU and encode a section of the key space concurrently
            match executor.spawn_blocking_compact(move || {
                // let mut tenc = TimeEncoder::new(DEFAULT_MAX_POINTS_PER_BLOCK);
                // let mut fenc = FloatEncoder::new();
                // let mut ienc = IntegerEncoder::new(DEFAULT_MAX_POINTS_PER_BLOCK);
                // let mut uenc = IntegerEncoder::new(DEFAULT_MAX_POINTS_PER_BLOCK);
                // let mut benc = BoolEncoder::new(DEFAULT_MAX_POINTS_PER_BLOCK);
                // let mut senc = StringEncoder::new(DEFAULT_MAX_POINTS_PER_BLOCK);

                let mut tenc = get_time_encoder(DEFAULT_MAX_POINTS_PER_BLOCK as usize);
                let mut fenc = get_float_encoder(DEFAULT_MAX_POINTS_PER_BLOCK as usize);
                let mut ienc = get_integer_encoder(DEFAULT_MAX_POINTS_PER_BLOCK as usize);
                let mut uenc = get_unsigned_encoder(DEFAULT_MAX_POINTS_PER_BLOCK as usize);
                let mut benc = get_bool_encoder(DEFAULT_MAX_POINTS_PER_BLOCK as usize);
                let mut senc = get_string_encoder(DEFAULT_MAX_POINTS_PER_BLOCK as usize);

                for i in (worker..n).step_by(concurrency) {
                    let key = Arc::new(order[i].clone());

                    let mut values = &cache.values(&key)[..];
                    while !values.is_empty() {
                        let mut end = values.len();
                        if end > size {
                            end = size;
                        }

                        let (min_time, max_time) =
                            (values[0].unix_nano(), values[end - 1].unix_nano());

                        let (typ, b, err) = match values[0].value_type() {
                            INTEGER => match encode_integer_block_using(
                                &values[..end],
                                &mut tenc,
                                &mut ienc,
                            ) {
                                Ok(b) => (INTEGER, b, None),
                                Err(e) => (EMPTY, vec![], Some(Error::Encoding { source: e })),
                            },
                            UNSIGNED => match encode_unsigned_block_using(
                                &values[..end],
                                &mut tenc,
                                &mut uenc,
                            ) {
                                Ok(b) => (UNSIGNED, b, None),
                                Err(e) => (EMPTY, vec![], Some(Error::Encoding { source: e })),
                            },
                            FLOAT => {
                                match encode_float_block_using(&values[..end], &mut tenc, &mut fenc)
                                {
                                    Ok(b) => (FLOAT, b, None),
                                    Err(e) => (EMPTY, vec![], Some(Error::Encoding { source: e })),
                                }
                            }
                            BOOL => {
                                match encode_bool_block_using(&values[..end], &mut tenc, &mut benc)
                                {
                                    Ok(b) => (BOOL, b, None),
                                    Err(e) => (EMPTY, vec![], Some(Error::Encoding { source: e })),
                                }
                            }
                            STRING => match encode_string_block_using(
                                &values[..end],
                                &mut tenc,
                                &mut senc,
                            ) {
                                Ok(b) => (STRING, b, None),
                                Err(e) => (EMPTY, vec![], Some(Error::Encoding { source: e })),
                            },
                            _ => {
                                unreachable!("CacheKeyIter.encode invalid value type");
                                // match Values::encode(&values[..end]) {
                                //     Ok(b) => (EMPTY, b, None),
                                //     Err(e) => (EMPTY, vec![], Some(Error::EncodingValues { source: e })),
                                // },
                            }
                        };

                        let statistics = BlockStatistics::from_values(&values[..end]);
                        values = &values[end..];
                        let err_str = err.as_ref().map(|e| e.to_string());
                        // Note: next can access blocks[i]
                        // Note: Only when we has finished put all encoded data to blocks[i],
                        //       Iterator can access blocks[i], so there is no race.
                        blocks[i].lock().push(CacheBlock {
                            typ,
                            k: Arc::clone(&key),
                            min_time,
                            max_time,
                            statistics,
                            b,
                            err: err_str,
                        });

                        if let Some(err) = err {
                            *cerr.write() = Some(err);
                        }
                    } // end while

                    // Notify this key is fully encoded
                    // let _ = ready[i].send();
                    if !task_terminals[i].swap(true, Ordering::SeqCst) {
                        unsafe {
                            let ready: &mut Vec<CancellationTokenBounded> = &mut *ready.data_ptr();
                            let _ = ready[i].send();
                        }
                    }
                }

                put_time_encoder(tenc);
                put_integer_encoder(ienc);
                put_unsigned_encoder(uenc);
                put_float_encoder(fenc);
                put_bool_encoder(benc);
                put_string_encoder(senc);
                completion.complete();
            }) {
                Ok(handle) => {
                    registration.accepted();
                    handles.push(handle);
                }
                Err(source) => {
                    registration.rejected();
                    for handle in &handles {
                        handle.abort();
                    }
                    return Err(Error::Execution { source });
                }
            }
        } // end for

        let mut terminated = false;
        for handle in handles {
            if handle.await.is_err() {
                terminated = true;
            }
        }
        if terminated {
            Err(Error::SnapshotWriterTaskTerminated)
        } else {
            Ok(())
        }
    }
}

impl KeyIterator for CacheKeyIter {
    // 这里是一边encode一边iter读取
    // read chans 的容量与keys相同
    // self.i >=0: 已经存在encoded
    // len(c.ready)：cache中已完成编码的seriesKey的数量
    // c.i < len(c.ready): 已完成encoded的数据还没有读取完
    // len(c.blocks[c.i])>0: c.blocks[c.i]的block数据还没有读取完
    // 注意：num_cpu个goroutine在编码时，是从idx=0开始一个一个往后编码的，因此c.i需要从0开始加1
    fn has_next(&mut self) -> bool {
        if self.i >= 0 && self.i < self.n as i64 {
            if !self.blocks[self.i as usize].lock().is_empty() {
                // the first block in blocks[i] has poped when call next
                return true;
            }
        }

        self.i += 1;
        if self.i >= self.n as i64 {
            // already read encoded data
            return false;
        }

        // let _ = self.ready[self.i as usize].recv(); // wait for encode complete
        unsafe {
            let ready: &mut Vec<CancellationTokenBounded> = &mut *self.ready.data_ptr();
            let _ = ready[self.i as usize].recv();
        }

        true
    }

    fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn err(&self) -> Option<String> {
        let e = self.err.read();

        (*e).as_ref().map(|e| e.to_string())
    }

    fn estimated_index_size(&self) -> usize {
        let mut n = 0;
        for v in self.order.iter() {
            n += v.len();
        }
        n
    }
}

impl Iterator for CacheKeyIter {
    type Item = EncodedBlock;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.has_next() {
            return None;
        }
        // See if snapshot compactions were disabled while we were running.
        if let Some(interrupt) = &self.interrupt {
            if interrupt.is_cancelled() {
                // Pop the first block first, so if blocks has one block, has_next returns false.
                self.blocks[self.i as usize].lock().drain(..1);

                self.err = Arc::new(RwLock::new(Some(Error::CompactionAborted {
                    reason: "".to_string(),
                })));
                return Some(EncodedBlock::error(Some(
                    Error::CompactionAborted {
                        reason: "".to_string(),
                    }
                    .to_string(),
                )));
            }
        }

        // Note: can access blocks[i] only after all data are encoded into blocks[i], so there is no race.
        // FIXME: 使用.drain获取vec的first是否合适，还是vec换成vecdeque？
        let blk = self.blocks[self.i as usize]
            .lock()
            .drain(..1)
            .next()
            .unwrap();
        Some(EncodedBlock {
            typ: blk.typ,
            key: blk.k,
            min_time: blk.min_time,
            max_time: blk.max_time,
            statistics: blk.statistics,
            block: blk.b,
            error: blk.err,
        })

        // // // (key, min_time, max_time, b, error)
        // let mut k = Vec::new();
        // k.clone_from(&blk.k);
        // let mut b = Vec::new();
        // b.clone_from(&blk.b);
        // let mut err = None;
        // err.clone_from(&blk.err);

        // Some((k, blk.min_time, blk.max_time, b, err))

        // Some((
        //     blk.k.clone_from();,
        //     blk.min_time,
        //     blk.max_time,
        //     blk.b.clone(),
        //     blk.err.clone(),
        // ))
    }
}

#[derive(Default, Clone)]
struct Block {
    key: Vec<u8>,
    min_time: i64,
    max_time: i64,
    typ: u8,
    statistics: Option<BlockStatistics>,
    b: Vec<u8>, // data block corresponding to cur index block  // TODO: Bytes ?
    tombstones: Vec<TimeRange>,

    // read_min, read_max are the timestamps range of values have been
    // read and encoded from this block.
    read_min: i64,
    read_max: i64,
}

impl Block {
    fn overlaps_time_range(&self, min: i64, max: i64) -> bool {
        self.min_time <= max && self.max_time >= min
    }

    fn read(&self) -> bool {
        self.read_min <= self.min_time && self.read_max >= self.max_time
    }

    fn mark_read(&mut self, min: i64, max: i64) {
        if min < self.read_min {
            self.read_min = min;
        }

        if max > self.read_max {
            self.read_max = max;
        }
    }

    fn partially_read(&self) -> bool {
        // If read_min and read_max are still the initial values, nothing has been read.
        if self.read_min == i64::MAX && self.read_max == i64::MIN {
            return false;
        }

        self.read_min != self.min_time || self.read_max != self.max_time
    }
}

// FIXME:
impl Ord for Block {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.key.cmp(&other.key) {
            std::cmp::Ordering::Equal => match self.min_time.cmp(&other.min_time) {
                std::cmp::Ordering::Equal => self.max_time.cmp(&other.max_time),
                other => other,
            },
            other => other,
        }
    }
}

impl PartialOrd for Block {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.min_time == other.min_time && self.max_time == other.max_time
    }
}

impl Eq for Block {}

#[derive(Default, Clone, PartialEq, Eq, Ord, PartialOrd)]
struct Blocks(Vec<Block>);

impl Deref for Blocks {
    type Target = Vec<Block>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Blocks {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Default)]
struct Latencies {
    i: usize,
    values: Vec<Duration>,
}

impl Latencies {
    fn new(sz: usize) -> Self {
        Self {
            i: 0,
            values: vec![Duration::default(); sz],
        }
    }

    fn add(&mut self, t: Duration) {
        self.values.insert(self.i % self.values.len(), t);
        self.i += 1;
    }

    fn avg(&self) -> Duration {
        let mut n = 0u64;
        let mut sum = 0u64;
        for v in self.values.iter() {
            if *v == Duration::from_secs(0) {
                continue;
            }
            sum += v.as_nanos() as u64;
            n += 1;
        }

        if n > 0 {
            return Duration::from_nanos(sum / n);
        }

        Duration::from_secs(0)
    }
}

#[cfg(test)]
pub mod tests {
    use std::{
        collections::HashMap,
        fs,
        io::{Seek, SeekFrom, Write},
        os::unix::fs::PermissionsExt,
        path::{Path, PathBuf},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use crate::common::{time::Time, to_str, to_string};
    use crate::models::{
        BOOL, FLOAT, INTEGER, STRING, UNSIGNED,
        points::{FloatValue, IntegerValue, Value},
    };
    use crate::runtime::{
        execution::{BoxBlockingTask, BoxFutureTask, RuntimeExecutor, SpawnError, TaskController},
        global::block_on_global,
    };
    use assert_cmd::assert;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use super::{
        Compactor, DefaultPlanner, EncodedBlock, Error, KeyIterator, SnapshotVLogEvent,
        SnapshotVLogWorkers, TSMKeyIter, WriteVLogOp, WriteVLogRes,
    };
    use crate::{
        config::{DEFAULT_COMPACT_FULL_WRITE_COLD_DURATION, VLogConfig},
        storage::{
            limiter::rate::{Limit, Limiter},
            tsm1::{
                block_statistics::{BlockStatisticValue, BlockStatistics},
                cache::{
                    Cache,
                    tests::{must_temp_tsm_dir, must_temp_tsm_file},
                },
                compact::CacheKeyIter,
                cursor::array_values::IntegerArray,
                data_encoding::{
                    array_encoding::decode_integer_array_block, encoding::decode_block,
                    encoding_values::Values,
                },
                file_store::{
                    FileStat, FileStore, default_format_file_name, default_parse_file_name,
                },
                reader::TSMReader,
                tombstone::Tombstoner,
                vlog::{
                    file_store::FileStore as VLogFileStore, vlog_index_cache::Cache as VLogCache,
                },
                writer::TSMWriter,
            },
        },
    };

    struct DisposeFirstCompactTaskExecutor {
        compact_tasks: AtomicUsize,
    }

    impl DisposeFirstCompactTaskExecutor {
        fn new() -> Self {
            Self {
                compact_tasks: AtomicUsize::new(0),
            }
        }
    }

    impl RuntimeExecutor for DisposeFirstCompactTaskExecutor {
        fn schedule(&self, _task: BoxFutureTask) -> Result<TaskController, SpawnError> {
            Err(SpawnError::rejected(
                "async scheduling is not used by this test",
            ))
        }

        fn schedule_blocking(&self, _task: BoxBlockingTask) -> Result<TaskController, SpawnError> {
            Err(SpawnError::rejected(
                "blocking scheduling is not used by this test",
            ))
        }

        fn schedule_compact(&self, _task: BoxFutureTask) -> Result<TaskController, SpawnError> {
            Err(SpawnError::rejected(
                "compact scheduling is not used by this test",
            ))
        }

        fn schedule_blocking_compact(
            &self,
            task: BoxBlockingTask,
        ) -> Result<TaskController, SpawnError> {
            if self.compact_tasks.fetch_add(1, Ordering::SeqCst) == 0 {
                drop(task);
            } else {
                std::thread::spawn(task);
            }
            Ok(TaskController::detached())
        }
    }

    #[derive(Clone, Copy)]
    enum WriterTaskDisposition {
        Run = 0,
        Drop = 1,
        Panic = 2,
    }

    struct WriterLifecycleExecutor {
        target: usize,
        disposition: AtomicUsize,
        submissions: AtomicUsize,
    }

    impl WriterLifecycleExecutor {
        fn new(target: usize, disposition: WriterTaskDisposition) -> Self {
            Self {
                target,
                disposition: AtomicUsize::new(disposition as usize),
                submissions: AtomicUsize::new(0),
            }
        }
    }

    impl RuntimeExecutor for WriterLifecycleExecutor {
        fn schedule(&self, _task: BoxFutureTask) -> Result<TaskController, SpawnError> {
            Err(SpawnError::rejected("unused async lane"))
        }

        fn schedule_blocking(&self, _task: BoxBlockingTask) -> Result<TaskController, SpawnError> {
            Err(SpawnError::rejected("unused blocking lane"))
        }

        fn schedule_compact(&self, _task: BoxFutureTask) -> Result<TaskController, SpawnError> {
            Err(SpawnError::rejected("unused compact async lane"))
        }

        fn schedule_blocking_compact(
            &self,
            task: BoxBlockingTask,
        ) -> Result<TaskController, SpawnError> {
            let submission = self.submissions.fetch_add(1, Ordering::SeqCst) + 1;
            let disposition = if submission == self.target {
                self.disposition
                    .swap(WriterTaskDisposition::Run as usize, Ordering::SeqCst)
            } else {
                WriterTaskDisposition::Run as usize
            };
            match disposition {
                value if value == WriterTaskDisposition::Drop as usize => {
                    drop(task);
                    Ok(TaskController::detached())
                }
                value if value == WriterTaskDisposition::Panic as usize => {
                    std::thread::spawn(move || {
                        let _task = task;
                        panic!("injected accepted snapshot VLog writer startup panic");
                    });
                    Ok(TaskController::detached())
                }
                _ => {
                    std::thread::spawn(task);
                    Ok(TaskController::detached())
                }
            }
        }
    }

    fn snapshot_vlog_file_store(path: &Path) -> Arc<VLogFileStore> {
        let cache = Arc::new(VLogCache::new(1 << 20));
        Arc::new(VLogFileStore::new(Arc::new(path.to_path_buf()), None, cache).unwrap())
    }

    fn snapshot_vlog_tmp_files(path: &Path) -> Vec<PathBuf> {
        fs::read_dir(path)
            .unwrap()
            .filter_map(|entry| {
                let path = entry.unwrap().path();
                path.to_string_lossy()
                    .ends_with(".vlog.tmp")
                    .then_some(path)
            })
            .collect()
    }

    fn assert_writer_startup_failure(
        writer_count: i32,
        target: usize,
        disposition: WriterTaskDisposition,
    ) {
        let root = tempfile::tempdir().unwrap();
        let store = snapshot_vlog_file_store(root.path());
        let executor: Arc<dyn RuntimeExecutor> =
            Arc::new(WriterLifecycleExecutor::new(target, disposition));
        let (events, mut event_rx) = mpsc::unbounded_channel();
        let dispatch = Compactor::init_write_vlog(
            writer_count,
            Arc::new(root.path().to_path_buf()),
            &store,
            &events,
        )
        .unwrap();
        drop(events);
        let SnapshotVLogEvent::Start(starts) = event_rx.blocking_recv().unwrap() else {
            panic!("expected snapshot VLog worker start event");
        };
        let mut workers = SnapshotVLogWorkers::default();
        workers.start(starts, &executor).unwrap();
        drop(dispatch);
        workers.finish_input();
        assert!(matches!(
            futures::executor::block_on(workers.wait_ready()),
            Err(Error::VLogWriterTaskTerminated)
        ));
        assert!(matches!(
            futures::executor::block_on(workers.shutdown()),
            Err(Error::VLogWriterTaskTerminated)
        ));
        assert!(snapshot_vlog_tmp_files(root.path()).is_empty());
    }

    #[test]
    fn snapshot_vlog_writer_accepted_disposal_fails_startup_without_leaks() {
        assert_writer_startup_failure(1, 1, WriterTaskDisposition::Drop);
    }

    #[test]
    fn snapshot_vlog_writer_executor_unwind_before_body_fails_startup_without_leaks() {
        assert_writer_startup_failure(1, 1, WriterTaskDisposition::Panic);
    }

    #[test]
    fn snapshot_vlog_writer_partial_multi_worker_failure_is_atomic() {
        assert_writer_startup_failure(3, 3, WriterTaskDisposition::Drop);
    }

    #[test]
    fn snapshot_vlog_writer_finishes_queued_work_after_idle_shutdown_race() {
        let (input, receiver) = crossbeam_channel::unbounded();
        let (idle, idle_rx) = std::sync::mpsc::channel();
        let resume = std::sync::Arc::new(std::sync::Barrier::new(2));
        let worker_resume = std::sync::Arc::clone(&resume);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let worker_shutdown = shutdown.clone();
        let worker = std::thread::spawn(move || {
            let mut values = Vec::new();
            let mut paused = false;
            loop {
                crossbeam_channel::select! {
                    recv(receiver) -> value => values.push(value.unwrap()),
                    default => {
                        if !paused {
                            paused = true;
                            idle.send(()).unwrap();
                            worker_resume.wait();
                        }
                        if super::snapshot_vlog_input_finished(&worker_shutdown, &receiver) {
                            return values;
                        }
                    }
                }
            }
        });
        // The worker already selected the empty-queue branch. Fill the queue
        // and signal producer completion before it checks the shutdown token.
        idle_rx.recv().unwrap();
        input.send(1).unwrap();
        input.send(2).unwrap();
        shutdown.cancel();
        resume.wait();
        assert_eq!(worker.join().unwrap(), vec![1, 2]);
    }

    #[test]
    fn snapshot_vlog_rollover_keeps_file_names_and_block_identities_consistent() {
        let root = tempfile::tempdir().unwrap();
        let store = snapshot_vlog_file_store(root.path());
        let first = store.next_generation();
        let mut writer = Compactor::open_vlog_writer(root.path(), first);
        for time in 0..260 {
            let (generation, sequence) = writer.get_id();
            let expected = root.path().join(format!("{generation:09}-{sequence:09}.vlog.tmp"));
            assert_eq!(writer.path(), expected.as_path());
            assert_eq!(sequence, 1);
            let values = vec![Value::IntegerValue(IntegerValue::new(time, 42))];
            let (offset, size) = writer.write_block(
                b"cpu#!~#value", time, time, &Values::encode(&values).unwrap(),
            ).unwrap();
            let (old, next) = Compactor::close_and_open_new_vlog_writer(
                &mut writer, generation, sequence, root.path(), Arc::clone(&store),
            ).unwrap();
            assert_eq!(Path::new(&old), expected.as_path());
            assert!(next.get_id().0 > generation);
            let reader = crate::storage::tsm1::vlog::reader::VLogReader::new_bare(
                Path::new(&old), generation, sequence,
            ).unwrap();
            assert_eq!(reader.read_integer_block(offset as u64, size as u64).unwrap(),
                vec![IntegerValue::new(time, 42)]);
            writer = next;
        }
        writer.close().unwrap();
    }

    #[test]
    fn snapshot_vlog_writer_runtime_panic_resolves_outstanding_request() {
        let root = tempfile::tempdir().unwrap();
        let store = snapshot_vlog_file_store(root.path());
        let executor: Arc<dyn RuntimeExecutor> = Arc::new(WriterLifecycleExecutor::new(
            usize::MAX,
            WriterTaskDisposition::Run,
        ));
        let (events, mut event_rx) = mpsc::unbounded_channel();
        let dispatch =
            Compactor::init_write_vlog(1, Arc::new(root.path().to_path_buf()), &store, &events)
                .unwrap();
        let SnapshotVLogEvent::Start(starts) = event_rx.blocking_recv().unwrap() else {
            panic!("expected snapshot VLog worker start event");
        };
        let mut workers = SnapshotVLogWorkers::default();
        workers.start(starts, &executor).unwrap();
        futures::executor::block_on(workers.wait_ready()).unwrap();
        let (notify, result) = tokio::sync::oneshot::channel();
        dispatch
            .send(WriteVLogOp {
                count: 1,
                typ: crate::models::INTEGER,
                key: Arc::new(b"cpu#!~#value".to_vec()),
                min_time: 1,
                max_time: 1,
                block: vec![1],
                notify,
                panic_for_test: true,
            })
            .unwrap();
        drop(dispatch);
        workers.finish_input();

        assert!(
            result.blocking_recv().is_err(),
            "worker panic must close the outstanding operation result"
        );
        assert!(matches!(
            futures::executor::block_on(workers.shutdown()),
            Err(Error::VLogWriterTaskTerminated)
        ));
    }

    #[test]
    fn snapshot_vlog_writer_closed_notification_does_not_panic_worker() {
        let root = tempfile::tempdir().unwrap();
        let store = snapshot_vlog_file_store(root.path());
        let executor: Arc<dyn RuntimeExecutor> = Arc::new(WriterLifecycleExecutor::new(
            usize::MAX,
            WriterTaskDisposition::Run,
        ));
        let (events, mut event_rx) = mpsc::unbounded_channel();
        let dispatch =
            Compactor::init_write_vlog(1, Arc::new(root.path().to_path_buf()), &store, &events)
                .unwrap();
        let SnapshotVLogEvent::Start(starts) = event_rx.blocking_recv().unwrap() else {
            panic!("expected snapshot VLog worker start event");
        };
        let mut workers = SnapshotVLogWorkers::default();
        workers.start(starts, &executor).unwrap();
        futures::executor::block_on(workers.wait_ready()).unwrap();
        let (notify, result) = tokio::sync::oneshot::channel::<WriteVLogRes>();
        drop(result);
        dispatch
            .send(WriteVLogOp {
                count: 1,
                typ: crate::models::INTEGER,
                key: Arc::new(b"cpu#!~#value".to_vec()),
                min_time: 1,
                max_time: 1,
                block: Vec::new(),
                notify,
                panic_for_test: false,
            })
            .unwrap();
        drop(dispatch);
        workers.finish_input();
        assert!(futures::executor::block_on(workers.shutdown()).is_ok());
    }

    //  Tests compacting a Cache snapshot into a single TSM file
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_snapshot() {
        let dir = must_temp_tsm_dir();

        let v1 = Value::FloatValue(FloatValue::new(1, 1.0));
        let v2 = Value::FloatValue(FloatValue::new(1, 1.0));
        let v3 = Value::FloatValue(FloatValue::new(2, 2.0));

        let points1 = HashMap::from([
            ("cpu,host=A#!~#value".to_string(), vec![v1.clone()]),
            (
                "cpu,host=B#!~#value".to_string(),
                vec![v2.clone(), v3.clone()],
            ),
        ]);

        let c1 = Arc::new(Cache::new(0));
        for (k, v) in points1.iter() {
            c1.write(k.as_bytes(), v.clone()).unwrap();
        }

        let c2 = Arc::new(Cache::new(0));
        for (k, v) in points1.iter() {
            c2.write(k.as_bytes(), v.clone()).unwrap();
        }

        let dir = Arc::new(dir.into_path());
        let vlog_dir = Arc::new(dir.join("vlog"));
        let file_store = Arc::new(FileStore::new(Arc::clone(&dir), None));
        let mut compactor = Compactor::new(
            Arc::clone(&dir),
            vlog_dir,
            file_store,
            None,
            Some(Limiter::new(
                Limit((48 * 1024 * 1024) as f64),
                48 * 1024 * 1024,
            )),
            false,
            &VLogConfig::default(),
        );

        let res = futures::executor::block_on(compactor.write_snapshot(c1));
        assert!(res.is_err(), "expected error writing snapshot");

        compactor.open();

        let files = futures::executor::block_on(compactor.write_snapshot(c2)).unwrap();

        assert_eq!(
            files.0.len(),
            1,
            "files length mismatch: got {}, exp {}",
            files.0.len(),
            1
        );

        let r = must_open_tsm_reader(files.0[0].as_str(), 0, 0);

        assert_eq!(
            r.key_count(),
            2,
            "keys length mismatch: got {}, exp {}",
            r.key_count(),
            2
        );

        struct Data {
            key: String,
            points: Vec<Value>,
        }
        let data = vec![
            Data {
                key: "cpu,host=A#!~#value".to_string(),
                points: vec![v1.clone()],
            },
            Data {
                key: "cpu,host=B#!~#value".to_string(),
                points: vec![v2.clone(), v3.clone()],
            },
        ];

        for p in data {
            assert_persisted_statistics_match_decoded(&r, p.key.as_bytes());
            let values = r.read_all(p.key.as_bytes()).unwrap();

            assert_eq!(
                values.len(),
                p.points.len(),
                "values length mismatch: got {}, exp {}",
                values.len(),
                p.points.len()
            );

            for (i, point) in p.points.iter().enumerate() {
                assert_eq!(
                    values[i],
                    point.clone(),
                    "point({}) mismatch: got {:?}, exp {:?}",
                    i,
                    values[i],
                    point
                );
            }
        }

        let _ = fs::remove_dir_all(dir.as_path());
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_compact_full_last_timestamp() {
        let dir = must_temp_tsm_dir();

        let mut vals = vec![];
        let mut ts = 1_000_000_000 as i64;
        for i in 0..120 {
            vals.push(Value::IntegerValue(IntegerValue::new(ts, 1)));
            ts += 1_000_000_000;
        }
        // 121st timestamp skips a second
        ts += 1_000_000_000;
        vals.push(Value::IntegerValue(IntegerValue::new(ts, 1)));

        let writes = HashMap::from([("cpu,host=A#!~#value".to_string(), vals[..100].to_vec())]);
        let (fpath1, f1) = must_write_tsm(dir.path(), 1, writes);

        let writes = HashMap::from([("cpu,host=A#!~#value".to_string(), vals[100..].to_vec())]);
        let (fpath2, f2) = must_write_tsm(dir.path(), 2, writes);

        let dir = Arc::new(dir.into_path());
        let vlog_dir = Arc::new(dir.join("vlog"));
        let file_store = Arc::new(FileStore::new(Arc::clone(&dir), None));
        let mut compactor = Compactor::new(
            Arc::clone(&dir),
            vlog_dir,
            file_store,
            None,
            Some(Limiter::new(
                crate::storage::limiter::rate::INF,
                48 * 1024 * 1024,
            )),
            false,
            &VLogConfig::default(),
        );

        compactor.open();

        let files = compactor.compact_full(&[f1, f2]).unwrap();

        let r = must_open_tsm_reader(files[0].as_str(), 0, 0);
        let entries = r.entries("cpu,host=A#!~#value".as_bytes());
        let (_, b) = r.read_bytes(&entries[0], &[]).unwrap();

        let mut a = IntegerArray::new(0);
        decode_integer_array_block(&b, &mut a).unwrap();

        assert_eq!(
            a.max_time(),
            entries[0].max_time,
            "expected a.max == entries[0].max_time, got: {}, exp: {}",
            a.max_time(),
            entries[0].max_time
        );

        let _ = fs::remove_dir_all(dir.as_path());
    }

    // Ensures that a compaction will properly merge multiple TSM files
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_compact_full() {
        let dir = must_temp_tsm_dir();

        // write 3 TSM files with different data and one new point
        let a1 = Value::FloatValue(FloatValue::new(1, 1.1));
        let writes = HashMap::from([("cpu,host=A#!~#value".to_string(), vec![a1.clone()])]);
        let (fpath1, f1) = must_write_tsm(dir.path(), 1, writes);

        let a2 = Value::FloatValue(FloatValue::new(2, 1.2));
        let b1 = Value::FloatValue(FloatValue::new(1, 2.1));
        let writes = HashMap::from([
            ("cpu,host=A#!~#value".to_string(), vec![a2.clone()]),
            ("cpu,host=B#!~#value".to_string(), vec![b1.clone()]),
        ]);
        let (fpath2, f2) = must_write_tsm(dir.path(), 2, writes);

        let a3 = Value::FloatValue(FloatValue::new(1, 1.3));
        let c1 = Value::FloatValue(FloatValue::new(1, 3.1));
        let writes = HashMap::from([
            ("cpu,host=A#!~#value".to_string(), vec![a3.clone()]),
            ("cpu,host=C#!~#value".to_string(), vec![c1.clone()]),
        ]);
        let (fpath3, f3) = must_write_tsm(dir.path(), 3, writes);

        let dir = Arc::new(dir.into_path());
        let vlog_dir = Arc::new(dir.join("vlog"));
        let file_store = Arc::new(FileStore::new(Arc::clone(&dir), None));
        let mut compactor = Compactor::new(
            Arc::clone(&dir),
            vlog_dir,
            file_store,
            None,
            Some(Limiter::new(
                Limit((48 * 1024 * 1024) as f64),
                48 * 1024 * 1024,
            )),
            false,
            &VLogConfig::default(),
        );

        let res = compactor.compact_full(&[f1.clone(), f2.clone(), f3.clone()]);
        assert!(res.is_err(), "expected error writing snapshot: {:?}", res);

        compactor.open();

        let files = compactor.compact_full(&[f1, f2, f3.clone()]).unwrap();

        assert_eq!(
            files.len(),
            1,
            "files length mismatch: got {}, exp {}",
            files.len(),
            1
        );

        let (exp_gen, mut exp_seq) = default_parse_file_name(&f3).unwrap();
        exp_seq += 1;

        let (got_gen, got_seq) = default_parse_file_name(&files[0]).unwrap();
        assert_eq!(
            got_gen, exp_gen,
            "wrong generation for new file: got {}, exp {}",
            got_gen, exp_gen
        );
        assert_eq!(
            got_seq, exp_seq,
            "wrong sequence for new file: got {}, exp {}",
            got_seq, exp_seq
        );

        let r = must_open_tsm_reader(files[0].as_str(), 0, 0);

        assert_eq!(
            r.key_count(),
            3,
            "keys length mismatch: got {}, exp {}",
            r.key_count(),
            3
        );

        let data = vec![
            KeyValues {
                key: "cpu,host=A#!~#value",
                points: vec![a3, a2],
            },
            KeyValues {
                key: "cpu,host=B#!~#value",
                points: vec![b1],
            },
            KeyValues {
                key: "cpu,host=C#!~#value",
                points: vec![c1],
            },
        ];

        for p in data {
            assert_persisted_statistics_match_decoded(&r, p.key.as_bytes());
            let values = r.read_all(p.key.as_bytes()).unwrap();

            assert_eq!(
                values.len(),
                p.points.len(),
                "values length mismatch %s: got {}, exp {}",
                values.len(),
                p.points.len()
            );

            for (i, point) in p.points.into_iter().enumerate() {
                assert_eq!(values[i], point);
            }
        }

        let _ = fs::remove_dir_all(dir.as_path());
    }

    // Ensures that a compaction will properly merge multiple TSM files
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_decode_error() {
        let dir = must_temp_tsm_dir();

        // write 3 TSM files with different data and one new point
        let a1 = Value::FloatValue(FloatValue::new(1, 1.1));
        let writes = HashMap::from([("cpu,host=A#!~#value".to_string(), vec![a1.clone()])]);
        let (fpath1, f1) = must_write_tsm(dir.path(), 1, writes);

        let a2 = Value::FloatValue(FloatValue::new(2, 1.2));
        let b1 = Value::FloatValue(FloatValue::new(1, 2.1));
        let writes = HashMap::from([
            ("cpu,host=A#!~#value".to_string(), vec![a2.clone()]),
            ("cpu,host=B#!~#value".to_string(), vec![b1.clone()]),
        ]);
        let (fpath2, f2) = must_write_tsm(dir.path(), 2, writes);

        let a3 = Value::FloatValue(FloatValue::new(1, 1.3));
        let c1 = Value::FloatValue(FloatValue::new(1, 3.1));
        let writes = HashMap::from([
            ("cpu,host=A#!~#value".to_string(), vec![a3.clone()]),
            ("cpu,host=C#!~#value".to_string(), vec![c1.clone()]),
        ]);
        let (fpath3, f3) = must_write_tsm(dir.path(), 3, writes);

        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&f3)
            .unwrap();
        f.metadata().unwrap().permissions().set_mode(0o777);

        let _ = f.seek(SeekFrom::Start(10)).unwrap();
        let _ = f.write("ffff".as_bytes()).unwrap();
        f.flush().unwrap();

        let dir = Arc::new(dir.into_path());
        let vlog_dir = Arc::new(dir.join("vlog"));
        let file_store = Arc::new(FileStore::new(Arc::clone(&dir), None));
        let mut compactor = Compactor::new(
            Arc::clone(&dir),
            vlog_dir,
            file_store,
            None,
            Some(Limiter::new(
                Limit((48 * 1024 * 1024) as f64),
                48 * 1024 * 1024,
            )),
            false,
            &VLogConfig::default(),
        );

        let res = compactor.compact_full(&[f1.clone(), f2.clone(), f3.clone()]);
        assert!(res.is_err(), "expected error writing snapshot: {:?}", res);

        compactor.open();

        let res = compactor.compact_full(&[f1, f2, f3.clone()]);
        if res.is_ok() {
            panic!("expected error writing snapshot: {:?}", res);
        } else {
            let err = res.err().unwrap().to_string();
            if !err.contains("decode error: unable to decompress block type float for key 'cpu,host=A#!~#value': unpack_block: not enough data for timestamp") {
                panic!("expected error writing snapshot: {:?}", err);
            }
        }

        let _ = fs::remove_dir_all(dir.as_path());
    }

    // Ensures that a compaction will properly merge multiple TSM files
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_compact_overlapping_blocks() {
        let dir = must_temp_tsm_dir();

        // write 3 TSM files with different data and one new point
        let a1 = Value::FloatValue(FloatValue::new(4, 1.1));
        let a2 = Value::FloatValue(FloatValue::new(5, 1.1));
        let a3 = Value::FloatValue(FloatValue::new(7, 1.1));
        let writes = HashMap::from([(
            "cpu,host=A#!~#value".to_string(),
            vec![a1.clone(), a2.clone(), a3.clone()],
        )]);
        let (fpath1, f1) = must_write_tsm(dir.path(), 1, writes);

        let c1 = Value::FloatValue(FloatValue::new(3, 1.2));
        let c2 = Value::FloatValue(FloatValue::new(8, 1.2));
        let c3 = Value::FloatValue(FloatValue::new(9, 1.2));
        let writes = HashMap::from([(
            "cpu,host=A#!~#value".to_string(),
            vec![c1.clone(), c2.clone(), c3.clone()],
        )]);
        let (fpath3, f3) = must_write_tsm(dir.path(), 3, writes);

        let dir = Arc::new(dir.into_path());
        let vlog_dir = Arc::new(dir.join("vlog"));
        let file_store = Arc::new(FileStore::new(Arc::clone(&dir), None));
        let mut compactor = Compactor::new(
            Arc::clone(&dir),
            vlog_dir,
            file_store,
            None,
            Some(Limiter::new(
                Limit((48 * 1024 * 1024) as f64),
                48 * 1024 * 1024,
            )),
            false,
            &VLogConfig::default(),
        );
        compactor.size = 2;

        compactor.open();

        let files = compactor.compact_fast(&[f1, f3]).unwrap();

        assert_eq!(
            files.len(),
            1,
            "files length mismatch: got {}, exp {}",
            files.len(),
            1
        );

        let r = must_open_tsm_reader(files[0].as_str(), 0, 0);

        assert_eq!(
            r.key_count(),
            1,
            "keys length mismatch: got {}, exp {}",
            r.key_count(),
            1
        );

        let data = vec![KeyValues {
            key: "cpu,host=A#!~#value",
            points: vec![c1, a1, a2, a3, c2, c3],
        }];

        for p in data {
            assert_persisted_statistics_match_decoded(&r, p.key.as_bytes());
            let values = r.read_all(p.key.as_bytes()).unwrap();

            assert_eq!(
                values.len(),
                p.points.len(),
                "values length mismatch %s: got {}, exp {}",
                values.len(),
                p.points.len()
            );

            for (i, point) in p.points.into_iter().enumerate() {
                assert_eq!(values[i], point);
            }
        }

        let _ = fs::remove_dir_all(dir.as_path());
    }

    // Ensures that a compaction will properly merge multiple TSM files
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_compact_overlapping_blocks_multiple() {
        let dir = must_temp_tsm_dir();

        // write 3 TSM files with different data and one new point
        let a1 = Value::FloatValue(FloatValue::new(4, 1.1));
        let a2 = Value::FloatValue(FloatValue::new(5, 1.1));
        let a3 = Value::FloatValue(FloatValue::new(7, 1.1));
        let writes = HashMap::from([(
            "cpu,host=A#!~#value".to_string(),
            vec![a1.clone(), a2.clone(), a3.clone()],
        )]);
        let (fpath1, f1) = must_write_tsm(dir.path(), 1, writes);

        let b1 = Value::FloatValue(FloatValue::new(1, 1.2));
        let b2 = Value::FloatValue(FloatValue::new(2, 1.2));
        let b3 = Value::FloatValue(FloatValue::new(6, 1.2));
        let writes = HashMap::from([(
            "cpu,host=A#!~#value".to_string(),
            vec![b1.clone(), b2.clone(), b3.clone()],
        )]);
        let (fpath2, f2) = must_write_tsm(dir.path(), 2, writes);

        let c1 = Value::FloatValue(FloatValue::new(3, 1.2));
        let c2 = Value::FloatValue(FloatValue::new(8, 1.2));
        let c3 = Value::FloatValue(FloatValue::new(9, 1.2));
        let writes = HashMap::from([(
            "cpu,host=A#!~#value".to_string(),
            vec![c1.clone(), c2.clone(), c3.clone()],
        )]);
        let (fpath3, f3) = must_write_tsm(dir.path(), 3, writes);

        let dir = Arc::new(dir.into_path());
        let vlog_dir = Arc::new(dir.join("vlog"));
        let file_store = Arc::new(FileStore::new(Arc::clone(&dir), None));
        let mut compactor = Compactor::new(
            Arc::clone(&dir),
            vlog_dir,
            file_store,
            None,
            Some(Limiter::new(
                Limit((48 * 1024 * 1024) as f64),
                48 * 1024 * 1024,
            )),
            false,
            &VLogConfig::default(),
        );
        compactor.size = 2;

        compactor.open();

        let files = compactor.compact_fast(&[f1, f2, f3]).unwrap();

        assert_eq!(
            files.len(),
            1,
            "files length mismatch: got {}, exp {}",
            files.len(),
            1
        );

        let r = must_open_tsm_reader(files[0].as_str(), 0, 0);

        assert_eq!(
            r.key_count(),
            1,
            "keys length mismatch: got {}, exp {}",
            r.key_count(),
            1
        );

        let data = vec![KeyValues {
            key: "cpu,host=A#!~#value",
            points: vec![b1, b2, c1, a1, a2, b3, a3, c2, c3],
        }];

        for p in data {
            assert_persisted_statistics_match_decoded(&r, p.key.as_bytes());
            let values = r.read_all(p.key.as_bytes()).unwrap();

            assert_eq!(
                values.len(),
                p.points.len(),
                "values length mismatch %s: got {}, exp {}",
                values.len(),
                p.points.len()
            );

            for (i, point) in p.points.into_iter().enumerate() {
                assert_eq!(values[i], point);
            }
        }

        let _ = fs::remove_dir_all(dir.as_path());
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_compact_unsorted_blocks() {
        let dir = must_temp_tsm_dir();

        // write 3 TSM files with different data and one new point
        let a1 = Value::FloatValue(FloatValue::new(4, 1.1));
        let a2 = Value::FloatValue(FloatValue::new(5, 1.1));
        let a3 = Value::FloatValue(FloatValue::new(6, 1.1));
        let writes = HashMap::from([(
            "cpu,host=A#!~#value".to_string(),
            vec![a1.clone(), a2.clone(), a3.clone()],
        )]);
        let (fpath1, f1) = must_write_tsm(dir.path(), 1, writes);

        let b1 = Value::FloatValue(FloatValue::new(1, 1.2));
        let b2 = Value::FloatValue(FloatValue::new(2, 1.2));
        let b3 = Value::FloatValue(FloatValue::new(3, 1.2));
        let writes = HashMap::from([(
            "cpu,host=A#!~#value".to_string(),
            vec![b1.clone(), b2.clone(), b3.clone()],
        )]);
        let (fpath2, f2) = must_write_tsm(dir.path(), 2, writes);

        let dir = Arc::new(dir.into_path());
        let vlog_dir = Arc::new(dir.join("vlog"));
        let file_store = Arc::new(FileStore::new(Arc::clone(&dir), None));
        let mut compactor = Compactor::new(
            Arc::clone(&dir),
            vlog_dir,
            file_store,
            None,
            Some(Limiter::new(
                Limit((48 * 1024 * 1024) as f64),
                48 * 1024 * 1024,
            )),
            false,
            &VLogConfig::default(),
        );
        compactor.size = 2;

        compactor.open();

        let files = compactor.compact_fast(&[f1, f2]).unwrap();

        assert_eq!(
            files.len(),
            1,
            "files length mismatch: got {}, exp {}",
            files.len(),
            1
        );

        let r = must_open_tsm_reader(files[0].as_str(), 0, 0);

        assert_eq!(
            r.key_count(),
            1,
            "keys length mismatch: got {}, exp {}",
            r.key_count(),
            1
        );

        let data = vec![KeyValues {
            key: "cpu,host=A#!~#value",
            points: vec![b1, b2, b3, a1, a2, a3],
        }];

        for p in data {
            assert_persisted_statistics_match_decoded(&r, p.key.as_bytes());
            let values = r.read_all(p.key.as_bytes()).unwrap();

            assert_eq!(
                values.len(),
                p.points.len(),
                "values length mismatch %s: got {}, exp {}",
                values.len(),
                p.points.len()
            );

            for (i, point) in p.points.into_iter().enumerate() {
                assert_eq!(values[i], point);
            }
        }

        let _ = fs::remove_dir_all(dir.as_path());
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_compact_unsorted_blocks_overlapping() {
        let dir = must_temp_tsm_dir();

        // write 3 TSM files with different data and one new point
        let a1 = Value::FloatValue(FloatValue::new(1, 1.1));
        let a2 = Value::FloatValue(FloatValue::new(2, 1.1));
        let writes = HashMap::from([(
            "cpu,host=A#!~#value".to_string(),
            vec![a1.clone(), a2.clone()],
        )]);
        let (fpath1, f1) = must_write_tsm(dir.path(), 1, writes);

        let b1 = Value::FloatValue(FloatValue::new(3, 1.2));
        let b2 = Value::FloatValue(FloatValue::new(4, 1.2));
        let writes = HashMap::from([(
            "cpu,host=A#!~#value".to_string(),
            vec![b1.clone(), b2.clone()],
        )]);
        let (fpath2, f2) = must_write_tsm(dir.path(), 2, writes);

        let c1 = Value::FloatValue(FloatValue::new(1, 1.1));
        let c2 = Value::FloatValue(FloatValue::new(2, 1.1));
        let writes = HashMap::from([(
            "cpu,host=A#!~#value".to_string(),
            vec![c1.clone(), c2.clone()],
        )]);
        let (fpath3, f3) = must_write_tsm(dir.path(), 3, writes);

        let dir = Arc::new(dir.into_path());
        let vlog_dir = Arc::new(dir.join("vlog"));
        let file_store = Arc::new(FileStore::new(Arc::clone(&dir), None));
        let mut compactor = Compactor::new(
            Arc::clone(&dir),
            vlog_dir,
            file_store,
            None,
            Some(Limiter::new(
                Limit((48 * 1024 * 1024) as f64),
                48 * 1024 * 1024,
            )),
            false,
            &VLogConfig::default(),
        );
        compactor.size = 2;

        compactor.open();

        let files = compactor.compact_fast(&[f1, f2, f3]).unwrap();

        assert_eq!(
            files.len(),
            1,
            "files length mismatch: got {}, exp {}",
            files.len(),
            1
        );

        let r = must_open_tsm_reader(files[0].as_str(), 0, 0);

        assert_eq!(
            r.key_count(),
            1,
            "keys length mismatch: got {}, exp {}",
            r.key_count(),
            1
        );

        let data = vec![KeyValues {
            key: "cpu,host=A#!~#value",
            points: vec![a1, a2, b1, b2],
        }];

        for p in data {
            assert_persisted_statistics_match_decoded(&r, p.key.as_bytes());
            let values = r.read_all(p.key.as_bytes()).unwrap();

            assert_eq!(
                values.len(),
                p.points.len(),
                "values length mismatch %s: got {}, exp {}",
                values.len(),
                p.points.len()
            );

            for (i, point) in p.points.into_iter().enumerate() {
                assert_eq!(values[i], point);
            }
        }

        let _ = fs::remove_dir_all(dir.as_path());
    }

    // Ensures that a compaction will properly merge multiple TSM files
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_compact_full_skip_full_blocks() {
        let dir = must_temp_tsm_dir();

        // write 3 TSM files with different data and one new point
        let a1 = Value::FloatValue(FloatValue::new(1, 1.1));
        let a2 = Value::FloatValue(FloatValue::new(2, 1.2));
        let writes = HashMap::from([(
            "cpu,host=A#!~#value".to_string(),
            vec![a1.clone(), a2.clone()],
        )]);
        let (fpath1, f1) = must_write_tsm(dir.path(), 1, writes);

        let a3 = Value::FloatValue(FloatValue::new(3, 1.3));
        let writes = HashMap::from([("cpu,host=A#!~#value".to_string(), vec![a3.clone()])]);
        let (fpath2, f2) = must_write_tsm(dir.path(), 2, writes);

        let a4 = Value::FloatValue(FloatValue::new(4, 1.4));
        let writes = HashMap::from([("cpu,host=A#!~#value".to_string(), vec![a4.clone()])]);
        let (fpath3, f3) = must_write_tsm(dir.path(), 3, writes);

        let dir = Arc::new(dir.into_path());
        let vlog_dir = Arc::new(dir.join("vlog"));
        let file_store = Arc::new(FileStore::new(Arc::clone(&dir), None));
        let mut compactor = Compactor::new(
            Arc::clone(&dir),
            vlog_dir,
            file_store,
            None,
            Some(Limiter::new(
                Limit((48 * 1024 * 1024) as f64),
                48 * 1024 * 1024,
            )),
            false,
            &VLogConfig::default(),
        );
        compactor.size = 2;

        compactor.open();

        let files = compactor.compact_full(&[f1, f2, f3.clone()]).unwrap();

        assert_eq!(
            files.len(),
            1,
            "files length mismatch: got {}, exp {}",
            files.len(),
            1
        );

        let (exp_gen, mut exp_seq) = default_parse_file_name(&f3).unwrap();
        exp_seq += 1;

        let (got_gen, got_seq) = default_parse_file_name(&files[0]).unwrap();
        assert_eq!(
            got_gen, exp_gen,
            "wrong generation for new file: got {}, exp {}",
            got_gen, exp_gen
        );
        assert_eq!(
            got_seq, exp_seq,
            "wrong sequence for new file: got {}, exp {}",
            got_seq, exp_seq
        );

        let r = must_open_tsm_reader(files[0].as_str(), 0, 0);

        assert_eq!(
            r.key_count(),
            1,
            "keys length mismatch: got {}, exp {}",
            r.key_count(),
            1
        );

        let data = vec![KeyValues {
            key: "cpu,host=A#!~#value",
            points: vec![a1, a2, a3, a4],
        }];

        for p in data {
            assert_persisted_statistics_match_decoded(&r, p.key.as_bytes());
            let values = r.read_all(p.key.as_bytes()).unwrap();

            assert_eq!(
                values.len(),
                p.points.len(),
                "values length mismatch %s: got {}, exp {}",
                values.len(),
                p.points.len()
            );

            for (i, point) in p.points.into_iter().enumerate() {
                assert_eq!(values[i], point);
            }
        }

        let _ = fs::remove_dir_all(dir.as_path());
    }

    // Ensures that a full compaction will skip over blocks that have the full
    // range of time contained in the block tombstoned
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_compact_full_tombstoned_skip_block() {
        let dir = must_temp_tsm_dir();

        // write 3 TSM files with different data and one new point
        let a1 = Value::FloatValue(FloatValue::new(1, 1.1));
        let a2 = Value::FloatValue(FloatValue::new(2, 1.2));
        let writes = HashMap::from([(
            "cpu,host=A#!~#value".to_string(),
            vec![a1.clone(), a2.clone()],
        )]);
        let (fpath1, f1) = must_write_tsm(dir.path(), 1, writes);

        let ts = Tombstoner::new(PathBuf::from(&f1));
        ts.add_range(
            &["cpu,host=A#!~#value".as_bytes()],
            "cpu,host=A#!~#value".as_bytes(),
            "cpu,host=A#!~#value".as_bytes(),
            i64::MIN,
            i64::MAX,
        );
        ts.flush().unwrap();

        let a3 = Value::FloatValue(FloatValue::new(3, 1.3));
        let writes = HashMap::from([("cpu,host=A#!~#value".to_string(), vec![a3.clone()])]);
        let (fpath2, f2) = must_write_tsm(dir.path(), 2, writes);

        let a4 = Value::FloatValue(FloatValue::new(4, 1.4));
        let writes = HashMap::from([("cpu,host=A#!~#value".to_string(), vec![a4.clone()])]);
        let (fpath3, f3) = must_write_tsm(dir.path(), 3, writes);

        let dir = Arc::new(dir.into_path());
        let vlog_dir = Arc::new(dir.join("vlog"));
        let file_store = Arc::new(FileStore::new(Arc::clone(&dir), None));
        let mut compactor = Compactor::new(
            Arc::clone(&dir),
            vlog_dir,
            file_store,
            None,
            Some(Limiter::new(
                Limit((48 * 1024 * 1024) as f64),
                48 * 1024 * 1024,
            )),
            false,
            &VLogConfig::default(),
        );
        compactor.size = 2;

        compactor.open();

        let files = compactor.compact_full(&[f1, f2, f3.clone()]).unwrap();

        assert_eq!(
            files.len(),
            1,
            "files length mismatch: got {}, exp {}",
            files.len(),
            1
        );

        let (exp_gen, mut exp_seq) = default_parse_file_name(&f3).unwrap();
        exp_seq += 1;

        let (got_gen, got_seq) = default_parse_file_name(&files[0]).unwrap();
        assert_eq!(
            got_gen, exp_gen,
            "wrong generation for new file: got {}, exp {}",
            got_gen, exp_gen
        );
        assert_eq!(
            got_seq, exp_seq,
            "wrong sequence for new file: got {}, exp {}",
            got_seq, exp_seq
        );

        let r = must_open_tsm_reader(files[0].as_str(), 0, 0);

        assert_eq!(
            r.key_count(),
            1,
            "keys length mismatch: got {}, exp {}",
            r.key_count(),
            1
        );

        let data = vec![KeyValues {
            key: "cpu,host=A#!~#value",
            points: vec![a3, a4],
        }];

        for p in data {
            let values = r.read_all(p.key.as_bytes()).unwrap();

            assert_eq!(
                values.len(),
                p.points.len(),
                "values length mismatch %s: got {}, exp {}",
                values.len(),
                p.points.len()
            );

            for (i, point) in p.points.into_iter().enumerate() {
                assert_eq!(values[i], point);
            }
        }

        let got = r.entries("cpu,host=A#!~#value".as_bytes()).len();
        assert_eq!(got, 1, "block count mismatch: got {}, exp {}", got, 1);

        let _ = fs::remove_dir_all(dir.as_path());
    }

    // Ensures that a full compaction will decode and combine blocks with
    // partial tombstoned values
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_compact_full_tombstoned_partial_block() {
        let dir = must_temp_tsm_dir();

        // write 3 TSM files with different data and one new point
        let a1 = Value::FloatValue(FloatValue::new(1, 1.1));
        let a2 = Value::FloatValue(FloatValue::new(2, 1.2));
        let writes = HashMap::from([(
            "cpu,host=A#!~#value".to_string(),
            vec![a1.clone(), a2.clone()],
        )]);
        let (fpath1, f1) = must_write_tsm(dir.path(), 1, writes);

        let ts = Tombstoner::new(PathBuf::from(&f1));
        ts.add_range(
            &["cpu,host=A#!~#value".as_bytes()],
            "cpu,host=A#!~#value".as_bytes(),
            "cpu,host=A#!~#value".as_bytes(),
            2,
            i64::MAX,
        );
        ts.flush().unwrap();

        let a3 = Value::FloatValue(FloatValue::new(3, 1.3));
        let writes = HashMap::from([("cpu,host=A#!~#value".to_string(), vec![a3.clone()])]);
        let (fpath2, f2) = must_write_tsm(dir.path(), 2, writes);

        let a4 = Value::FloatValue(FloatValue::new(4, 1.4));
        let writes = HashMap::from([("cpu,host=A#!~#value".to_string(), vec![a4.clone()])]);
        let (fpath3, f3) = must_write_tsm(dir.path(), 3, writes);

        let dir = Arc::new(dir.into_path());
        let vlog_dir = Arc::new(dir.join("vlog"));
        let file_store = Arc::new(FileStore::new(Arc::clone(&dir), None));
        let mut compactor = Compactor::new(
            Arc::clone(&dir),
            vlog_dir,
            file_store,
            None,
            Some(Limiter::new(
                Limit((48 * 1024 * 1024) as f64),
                48 * 1024 * 1024,
            )),
            false,
            &VLogConfig::default(),
        );
        compactor.size = 2;

        compactor.open();

        let files = compactor.compact_full(&[f1, f2, f3.clone()]).unwrap();

        assert_eq!(
            files.len(),
            1,
            "files length mismatch: got {}, exp {}",
            files.len(),
            1
        );

        let (exp_gen, mut exp_seq) = default_parse_file_name(&f3).unwrap();
        exp_seq += 1;

        let (got_gen, got_seq) = default_parse_file_name(&files[0]).unwrap();
        assert_eq!(
            got_gen, exp_gen,
            "wrong generation for new file: got {}, exp {}",
            got_gen, exp_gen
        );
        assert_eq!(
            got_seq, exp_seq,
            "wrong sequence for new file: got {}, exp {}",
            got_seq, exp_seq
        );

        let r = must_open_tsm_reader(files[0].as_str(), 0, 0);

        assert_eq!(
            r.key_count(),
            1,
            "keys length mismatch: got {}, exp {}",
            r.key_count(),
            1
        );

        let data = vec![KeyValues {
            key: "cpu,host=A#!~#value",
            points: vec![a1, a3, a4],
        }];

        for p in data {
            assert_persisted_statistics_match_decoded(&r, p.key.as_bytes());
            let values = r.read_all(p.key.as_bytes()).unwrap();

            assert_eq!(
                values.len(),
                p.points.len(),
                "values length mismatch %s: got {}, exp {}",
                values.len(),
                p.points.len()
            );

            for (i, point) in p.points.into_iter().enumerate() {
                assert_eq!(values[i], point);
            }
        }

        let got = r.entries("cpu,host=A#!~#value".as_bytes()).len();
        assert_eq!(got, 2, "block count mismatch: got {}, exp {}", got, 2);

        let _ = fs::remove_dir_all(dir.as_path());
    }

    // Ensures that a full compaction will decode and combine blocks with
    // multiple tombstoned ranges within the block e.g. (t1, t2, t3, t4)
    // having t2 and t3 removed
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_compact_full_tombstoned_multiple_ranges() {
        let dir = must_temp_tsm_dir();

        // write 3 TSM files with different data and one new point
        let a1 = Value::FloatValue(FloatValue::new(1, 1.1));
        let a2 = Value::FloatValue(FloatValue::new(2, 1.2));
        let a3 = Value::FloatValue(FloatValue::new(3, 1.3));
        let a4 = Value::FloatValue(FloatValue::new(4, 1.4));
        let writes = HashMap::from([(
            "cpu,host=A#!~#value".to_string(),
            vec![a1.clone(), a2.clone(), a3.clone(), a4.clone()],
        )]);
        let (fpath1, f1) = must_write_tsm(dir.path(), 1, writes);

        let ts = Tombstoner::new(PathBuf::from(&f1));
        // a1, a3 should remain after compaction
        ts.add_range(
            &["cpu,host=A#!~#value".as_bytes()],
            "cpu,host=A#!~#value".as_bytes(),
            "cpu,host=A#!~#value".as_bytes(),
            2,
            2,
        );
        ts.add_range(
            &["cpu,host=A#!~#value".as_bytes()],
            "cpu,host=A#!~#value".as_bytes(),
            "cpu,host=A#!~#value".as_bytes(),
            4,
            4,
        );
        ts.flush().unwrap();

        let a5 = Value::FloatValue(FloatValue::new(5, 1.5));
        let writes = HashMap::from([("cpu,host=A#!~#value".to_string(), vec![a5.clone()])]);
        let (fpath2, f2) = must_write_tsm(dir.path(), 2, writes);

        let a6 = Value::FloatValue(FloatValue::new(6, 1.6));
        let writes = HashMap::from([("cpu,host=A#!~#value".to_string(), vec![a6.clone()])]);
        let (fpath3, f3) = must_write_tsm(dir.path(), 3, writes);

        let dir = Arc::new(dir.into_path());
        let vlog_dir = Arc::new(dir.join("vlog"));
        let file_store = Arc::new(FileStore::new(Arc::clone(&dir), None));
        let mut compactor = Compactor::new(
            Arc::clone(&dir),
            vlog_dir,
            file_store,
            None,
            Some(Limiter::new(
                Limit((48 * 1024 * 1024) as f64),
                48 * 1024 * 1024,
            )),
            false,
            &VLogConfig::default(),
        );
        compactor.size = 2;

        compactor.open();

        let files = compactor.compact_full(&[f1, f2, f3.clone()]).unwrap();

        assert_eq!(
            files.len(),
            1,
            "files length mismatch: got {}, exp {}",
            files.len(),
            1
        );

        let (exp_gen, mut exp_seq) = default_parse_file_name(&f3).unwrap();
        exp_seq += 1;

        let (got_gen, got_seq) = default_parse_file_name(&files[0]).unwrap();
        assert_eq!(
            got_gen, exp_gen,
            "wrong generation for new file: got {}, exp {}",
            got_gen, exp_gen
        );
        assert_eq!(
            got_seq, exp_seq,
            "wrong sequence for new file: got {}, exp {}",
            got_seq, exp_seq
        );

        let r = must_open_tsm_reader(files[0].as_str(), 0, 0);

        assert_eq!(
            r.key_count(),
            1,
            "keys length mismatch: got {}, exp {}",
            r.key_count(),
            1
        );

        let data = vec![KeyValues {
            key: "cpu,host=A#!~#value",
            points: vec![a1, a3, a5, a6],
        }];

        for p in data {
            assert_persisted_statistics_match_decoded(&r, p.key.as_bytes());
            let values = r.read_all(p.key.as_bytes()).unwrap();

            assert_eq!(
                values.len(),
                p.points.len(),
                "values length mismatch %s: got {}, exp {}",
                values.len(),
                p.points.len()
            );

            for (i, point) in p.points.into_iter().enumerate() {
                assert_eq!(values[i], point);
            }
        }

        let got = r.entries("cpu,host=A#!~#value".as_bytes()).len();
        assert_eq!(got, 2, "block count mismatch: got {}, exp {}", got, 2);

        let _ = fs::remove_dir_all(dir.as_path());
    }

    // Ensures that a compaction will properly rollover to a new file when the
    // max keys per blocks is exceeded
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_compact_full_max_keys() {
        let dir = must_temp_tsm_dir();

        // write two files where the first contains a single key with the maximum
        // number of full blocks that can fit in a TSM file
        let (f1path, f1name, mut w1) = must_tsm_writer(dir.path(), 1);
        let mut values = Values::default();

        // FIXME: write 65534*1000 cost 2.5s, but golang cost 1.2s
        for i in 0..65534 {
            values.clear();
            for j in 0..1000 {
                values.push(Value::IntegerValue(IntegerValue::new(i * 1000 + j, 1i64)));
            }
            w1.write("cpu,host=A#!~#value".as_bytes(), &values).unwrap();
        }

        w1.write_index().unwrap();
        w1.close().unwrap();
        // Write a new file with 2 blocks that when compacted would exceed the max
        // blocks
        let (f2path, f2name, mut w2) = must_tsm_writer(dir.path(), 2);
        for i in 0..2 {
            let mut last_timestamp = values.last().as_ref().unwrap().unix_nano() + 1;
            values.clear();
            for j in last_timestamp..last_timestamp + 1000 {
                values.push(Value::IntegerValue(IntegerValue::new(j, 1i64)));
            }
            w2.write("cpu,host=A#!~#value".as_bytes(), &values).unwrap();
        }
        w2.write_index().unwrap();
        w2.close().unwrap();
        let dir = Arc::new(dir.into_path());
        let vlog_dir = Arc::new(dir.join("vlog"));
        let file_store = Arc::new(FileStore::new(Arc::clone(&dir), None));
        let mut compactor = Compactor::new(
            Arc::clone(&dir),
            vlog_dir,
            file_store,
            None,
            Some(Limiter::new(
                crate::storage::limiter::rate::INF,
                48 * 1024 * 1024,
            )),
            false,
            &VLogConfig::default(),
        );
        compactor.open();

        // FIXME: compact_full cost 7s, but golang cost 35ms
        // when compact tsm:
        // Compactor.write_file, iter.by_ref 65535 block, write to disk, elapsed 4.8s, write index elapsed 10ms
        // Compact both files, should get 2 files back
        let files = compactor.compact_full(&[f1name, f2name.clone()]).unwrap();
        assert_eq!(
            files.len(),
            2,
            "files length mismatch: got {}, exp {}",
            files.len(),
            2
        );
        let (exp_gen, mut exp_seq) = default_parse_file_name(&f2name).unwrap();
        exp_seq += 1;

        let (got_gen, got_seq) = default_parse_file_name(&files[0]).unwrap();
        assert_eq!(
            got_gen, exp_gen,
            "wrong generation for new file: got {}, exp {}",
            got_gen, exp_gen
        );
        assert_eq!(
            got_seq, exp_seq,
            "wrong sequence for new file: got {}, exp {}",
            got_seq, exp_seq
        );

        let _ = fs::remove_dir_all(dir.as_path());
    }

    // Tests that a single TSM file can be read and iterated over
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_tsm_key_iterator_single() {
        let dir = must_temp_tsm_dir();

        let v1 = Value::FloatValue(FloatValue::new(1, 1.1));
        let writes = HashMap::from([("cpu,host=A#!~#value".to_string(), vec![v1.clone()])]);

        let r = Arc::new(must_tsm_reader(dir.path(), 1, 1, writes.clone()));

        let mut iter = TSMKeyIter::new(1, false, CancellationToken::new(), &[r]).unwrap();

        let mut read_values = false;
        while let Some(EncodedBlock {
            key,
            block,
            statistics,
            error: err,
            ..
        }) = iter.next()
        {
            assert!(err.is_none(), "unexpected error read: {:?}", err.unwrap());

            let values = decode_block(&block).unwrap();
            let statistics = statistics.expect("compacted block statistics");
            assert_eq!(statistics.count, values.len() as u64);
            assert_eq!(
                statistics.value,
                BlockStatisticValue::Float {
                    sum: 1.1,
                    min: 1.1,
                    max: 1.1,
                    first: 1.1,
                    last: 1.1,
                }
            );

            let got = to_string!(Arc::into_inner(key).unwrap());
            let exp = "cpu,host=A#!~#value";
            assert_eq!(
                &got, "cpu,host=A#!~#value",
                "key mismatch: got {}, exp: {}",
                got, exp
            );

            assert_eq!(
                values.len(),
                writes.len(),
                "values length mismatch: got {}, exp {}",
                values.len(),
                writes.len()
            );

            for v in values {
                read_values = true;
                assert_eq!(v, v1);
            }
        }

        if !read_values {
            panic!("failed to read any values");
        }

        let _ = fs::remove_dir_all(dir.path());
    }

    // Tests that duplicate point values are merged.  There is only one case
    // where this could happen and that is when a compaction completed and we replace
    // the old TSM file with a new one and we crash just before deleting the old file.
    // No data is lost but the same point time/value would exist in two files until
    // compaction corrects it.
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_tsm_key_iterator_duplicate() {
        let dir = must_temp_tsm_dir();

        let v1 = Value::IntegerValue(IntegerValue::new(1, 1));
        let v2 = Value::IntegerValue(IntegerValue::new(1, 2));
        let writes1 = HashMap::from([("cpu,host=A#!~#value".to_string(), vec![v1.clone()])]);

        let r1 = Arc::new(must_tsm_reader(dir.path(), 1, 1, writes1.clone()));

        let writes2 = HashMap::from([("cpu,host=A#!~#value".to_string(), vec![v2.clone()])]);

        let r2 = Arc::new(must_tsm_reader(dir.path(), 2, 1, writes2.clone()));

        let mut iter = TSMKeyIter::new(1, false, CancellationToken::new(), &[r1, r2]).unwrap();

        let mut read_values = false;
        while let Some(EncodedBlock {
            key,
            block,
            statistics,
            error: err,
            ..
        }) = iter.next()
        {
            assert!(err.is_none(), "unexpected error read: {:?}", err.unwrap());

            let values = decode_block(&block).unwrap();
            assert_eq!(statistics.unwrap().count, values.len() as u64);

            let got = to_string!(Arc::into_inner(key).unwrap());
            let exp = "cpu,host=A#!~#value";
            assert_eq!(&got, exp, "key mismatch: got {}, exp: {}", got, exp);

            assert_eq!(
                values.len(),
                1,
                "values length mismatch: got {}, exp {}",
                values.len(),
                1
            );

            read_values = true;
            assert_eq!(values[0], v2);
        }

        if !read_values {
            panic!("failed to read any values");
        }

        let _ = fs::remove_dir_all(dir.path());
    }

    // Tests that deleted keys are not seen during iteration with
    // TSM files.
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_tsm_key_iterator_multiply_keys_deleted() {
        let dir = must_temp_tsm_dir();

        let v1 = Value::IntegerValue(IntegerValue::new(2, 1));
        let points1 = HashMap::from([("cpu,host=A#!~#value".to_string(), vec![v1.clone()])]);
        let r1 = Arc::new(must_tsm_reader(dir.path(), 1, 1, points1.clone()));
        r1.delete(&mut ["cpu,host=A#!~#value".as_bytes()]).unwrap();

        let v2 = Value::IntegerValue(IntegerValue::new(1, 1));
        let v3 = Value::IntegerValue(IntegerValue::new(1, 1));
        let points2 = HashMap::from([
            ("cpu,host=A#!~#count".to_string(), vec![v2.clone()]),
            ("cpu,host=B#!~#value".to_string(), vec![v3.clone()]),
        ]);
        let r2 = Arc::new(must_tsm_reader(dir.path(), 2, 1, points2.clone()));
        r2.delete(&mut ["cpu,host=A#!~#count".as_bytes()]).unwrap();

        let mut iter = TSMKeyIter::new(1, false, CancellationToken::new(), &[r1, r2]).unwrap();

        let mut read_values = false;
        while let Some(EncodedBlock {
            key,
            block,
            error: err,
            ..
        }) = iter.next()
        {
            assert!(err.is_none(), "unexpected error read: {:?}", err.unwrap());

            let values = decode_block(&block).unwrap();

            let got = to_string!(Arc::into_inner(key).unwrap());
            let exp = "cpu,host=B#!~#value";
            assert_eq!(&got, exp, "key mismatch: got {}, exp: {}", got, exp);

            assert_eq!(
                values.len(),
                1,
                "values length mismatch: got {}, exp {}",
                values.len(),
                1
            );

            read_values = true;
            assert_eq!(values[0], v3);
        }

        if !read_values {
            panic!("failed to read any values");
        }

        let _ = fs::remove_dir_all(dir.path());
    }

    // Tests that deleted keys are not seen during iteration with
    // TSM files.
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_tsm_key_iterator_single_deletes() {
        let dir = must_temp_tsm_dir();

        let v1 = Value::IntegerValue(IntegerValue::new(10, 1));
        let v2 = Value::IntegerValue(IntegerValue::new(20, 1));
        let v3 = Value::IntegerValue(IntegerValue::new(30, 1));
        let v4 = Value::IntegerValue(IntegerValue::new(40, 1));
        let v5 = Value::IntegerValue(IntegerValue::new(50, 1));
        let v6 = Value::IntegerValue(IntegerValue::new(60, 1));
        let points1 = HashMap::from([
            (
                "cpu,host=0#!~#value".to_string(),
                vec![v1.clone(), v2.clone()],
            ),
            (
                "cpu,host=A#!~#value".to_string(),
                vec![v5.clone(), v6.clone()],
            ),
            (
                "cpu,host=B#!~#value".to_string(),
                vec![v3.clone(), v4.clone()],
            ),
            (
                "cpu,host=C#!~#value".to_string(),
                vec![v1.clone(), v2.clone()],
            ),
            (
                "cpu,host=D#!~#value".to_string(),
                vec![v1.clone(), v2.clone()],
            ),
        ]);
        let r1 = Arc::new(must_tsm_reader(dir.path(), 1, 1, points1.clone()));
        TSMReader::delete_range(Arc::clone(&r1), &["cpu,host=A#!~#value".as_bytes()], 50, 50)
            .unwrap();
        TSMReader::delete_range(Arc::clone(&r1), &["cpu,host=A#!~#value".as_bytes()], 60, 60)
            .unwrap();
        TSMReader::delete_range(Arc::clone(&r1), &["cpu,host=C#!~#value".as_bytes()], 10, 10)
            .unwrap();
        TSMReader::delete_range(Arc::clone(&r1), &["cpu,host=C#!~#value".as_bytes()], 60, 60)
            .unwrap();
        TSMReader::delete_range(Arc::clone(&r1), &["cpu,host=C#!~#value".as_bytes()], 20, 20)
            .unwrap();

        let mut iter = TSMKeyIter::new(1, false, CancellationToken::new(), &[r1]).unwrap();

        struct Data {
            key: &'static str,
            value: Value,
        }

        let mut data = vec![
            Data {
                key: "cpu,host=0#!~#value",
                value: v1.clone(),
            },
            Data {
                key: "cpu,host=B#!~#value",
                value: v3,
            },
            Data {
                key: "cpu,host=D#!~#value",
                value: v1.clone(),
            },
        ];

        let mut read_values = 0;
        while let Some(EncodedBlock {
            key,
            block,
            error: err,
            ..
        }) = iter.next()
        {
            assert!(err.is_none(), "unexpected error read: {:?}", err.unwrap());

            let values = decode_block(&block).unwrap();

            let got = to_string!(Arc::into_inner(key).unwrap());
            assert_eq!(
                &got, data[0].key,
                "key mismatch: got {}, exp: {}",
                got, data[0].key
            );

            assert_eq!(
                values.len(),
                2,
                "values length mismatch: got {}, exp {}",
                values.len(),
                2
            );

            read_values += 1;
            assert_eq!(values[0], data[0].value);
            data.drain(..1);
        }

        assert_eq!(
            read_values, 3,
            "failed to read expected values: exp {}, got {}",
            read_values, 3
        );

        let _ = fs::remove_dir_all(dir.path());
    }

    // Tests that the TSMKeyIterator will abort if the interrupt channel is closed
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_tsm_key_iterator_abort() {
        let dir = must_temp_tsm_dir();

        let v1 = Value::FloatValue(FloatValue::new(1, 1.1));
        let points1 = HashMap::from([("cpu,host=A#!~#value".to_string(), vec![v1.clone()])]);
        let r = Arc::new(must_tsm_reader(dir.path(), 1, 1, points1.clone()));

        let intc = tokio_util::sync::CancellationToken::new();
        let mut iter = TSMKeyIter::new(1, false, intc.clone(), &[r]).unwrap();

        loop {
            // Abort
            intc.cancel();

            let err = iter.next().unwrap().error;
            assert!(err.is_some(), "expected abort error");

            break;
        }

        let _ = fs::remove_dir_all(dir.path());
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_cache_key_iterator_single() {
        let v0 = Value::FloatValue(FloatValue::new(1, 1.0));
        let writes = HashMap::from([("cpu,host=A#!~#value".to_string(), vec![v0.clone()])]);

        let c = Arc::new(Cache::new(0));

        for (k, v) in writes.iter() {
            c.write(k.as_bytes(), v.clone()).unwrap();
        }

        let mut iter = block_on_global(CacheKeyIter::new(c, 1, Some(CancellationToken::new())));

        let mut read_values = false;
        while let Some(EncodedBlock {
            key,
            block,
            statistics,
            error: err,
            ..
        }) = iter.next()
        {
            assert!(err.is_none(), "unexpected error read: {:?}", err.unwrap());

            let values = decode_block(&block).unwrap();
            assert_eq!(
                statistics.unwrap(),
                BlockStatistics::from_values(&values).unwrap()
            );

            let got = to_string!(Arc::into_inner(key).unwrap());
            let exp = "cpu,host=A#!~#value";
            assert_eq!(&got, exp, "key mismatch: got {}, exp: {}", got, exp);

            assert_eq!(
                values.len(),
                writes.len(),
                "values length mismatch: got {}, exp {}",
                values.len(),
                writes.len()
            );

            for v in values {
                read_values = true;
                assert_eq!(v, v0);
            }
        }

        if !read_values {
            panic!("failed to read any values");
        }
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_cache_key_iterator_chunked() {
        let v0 = Value::FloatValue(FloatValue::new(1, 1.0));
        let v1 = Value::FloatValue(FloatValue::new(2, 2.0));
        let writes = HashMap::from([(
            "cpu,host=A#!~#value".to_string(),
            vec![v0.clone(), v1.clone()],
        )]);

        let c = Arc::new(Cache::new(0));

        for (k, v) in writes.iter() {
            c.write(k.as_bytes(), v.clone()).unwrap();
        }

        let mut iter = block_on_global(CacheKeyIter::new(c, 1, Some(CancellationToken::new())));

        let mut read_values = false;
        let mut chunk = 0;
        while let Some(EncodedBlock {
            key,
            block,
            statistics,
            error: err,
            ..
        }) = iter.next()
        {
            assert!(err.is_none(), "unexpected error read: {:?}", err.unwrap());

            let values = decode_block(&block).unwrap();
            assert_eq!(
                statistics.unwrap(),
                BlockStatistics::from_values(&values).unwrap()
            );
            let got = to_str!(&key);
            let exp = "cpu,host=A#!~#value";
            assert_eq!(got, exp, "key mismatch: got {}, exp: {}", got, exp);

            assert_eq!(
                values.len(),
                1,
                "values length mismatch: got {}, exp {}",
                values.len(),
                1
            );

            for v in values {
                read_values = true;
                assert_eq!(v, writes.get("cpu,host=A#!~#value").unwrap()[chunk]);
            }
            chunk += 1;
        }

        if !read_values {
            panic!("failed to read any values");
        }
    }

    #[test]
    fn cache_key_iterator_accepted_encoder_disposal_is_terminal() {
        let cache = Arc::new(Cache::new(0));
        cache
            .write(
                b"cpu,host=A#!~#value",
                vec![Value::FloatValue(FloatValue::new(1, 1.0))],
            )
            .unwrap();

        let executor = Arc::new(DisposeFirstCompactTaskExecutor::new());
        let result =
            futures::executor::block_on(CacheKeyIter::new_with_executor(cache, 1, None, executor));

        assert!(matches!(result, Err(Error::SnapshotWriterTaskTerminated)));
    }

    // Tests that the CacheKeyIterator will abort if the interrupt channel is closed
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_cache_key_iterator_abort() {
        let v0 = Value::FloatValue(FloatValue::new(1, 1.0));
        let writes = HashMap::from([("cpu,host=A#!~#value".to_string(), vec![v0.clone()])]);

        let c = Arc::new(Cache::new(0));

        for (k, v) in writes.iter() {
            c.write(k.as_bytes(), v.clone()).unwrap();
        }

        let intc = tokio_util::sync::CancellationToken::new();
        let mut iter = block_on_global(CacheKeyIter::new(c, 1, Some(intc.clone())));
        let mut aborted = false;

        // Abort
        intc.cancel();
        while let Some(EncodedBlock { error: err, .. }) = iter.next() {
            assert!(err.is_some(), "expected abort error");
            aborted = err.is_some();
        }

        if !aborted {
            panic!("iterator not aborted");
        }
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_default_planner_plan_min() {
        let mut fs = FileStore::new(Arc::new(PathBuf::new()), None);
        fn paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "01-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "02-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "03-01.tsm1".into(),
                    size: 251 * 1024 * 1024,
                    ..Default::default()
                },
            ]
        }
        fs.paths_fn = Some(paths_fn);
        let mut cp = DefaultPlanner::new(Arc::new(fs), DEFAULT_COMPACT_FULL_WRITE_COLD_DURATION);

        let tsm = cp.plan(Time::now());
        assert_eq!(
            tsm.len(),
            0,
            "tsm file length mismatch: got {}, exp {}",
            tsm.len(),
            0
        );
    }

    // Ensure that if there are older files that can be compacted together but a newer
    // file that is in a larger step, the older ones will get compacted.
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_default_planner_plan_combine_sequence() {
        fn paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "01-04.tsm1".into(),
                    size: 128 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "02-04.tsm1".into(),
                    size: 128 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "03-04.tsm1".into(),
                    size: 128 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "04-04.tsm1".into(),
                    size: 128 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "06-02.tsm1".into(),
                    size: 67 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "07-02.tsm1".into(),
                    size: 128 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "08-01.tsm1".into(),
                    size: 251 * 1024 * 1024,
                    ..Default::default()
                },
            ]
        }

        let mut fs = FileStore::new(Arc::new(PathBuf::new()), None);
        fs.paths_fn = Some(paths_fn);
        let mut cp = DefaultPlanner::new(Arc::new(fs), DEFAULT_COMPACT_FULL_WRITE_COLD_DURATION);

        let data = paths_fn();
        let exp_files = vec![&data[0], &data[1], &data[2], &data[3]];

        let tsm = cp.plan(Time::now());

        assert_eq!(
            tsm[0].len(),
            exp_files.len(),
            "tsm file length mismatch: got {}, exp {}",
            tsm[0].len(),
            exp_files.len()
        );

        for (i, p) in exp_files.iter().enumerate() {
            let got = &tsm[0][i];
            let exp = p.path.to_str().unwrap();
            assert_eq!(got, exp, "tsm file mismatch: got {}, exp {}", got, exp);
        }
    }

    // Ensure that the planner grabs the smallest compaction step
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_default_planner_plan_multiple_groups() {
        fn paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "01-04.tsm1".into(),
                    size: 64 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "02-04.tsm1".into(),
                    size: 64 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "03-04.tsm1".into(),
                    size: 64 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "04-04.tsm1".into(),
                    size: 129 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "05-04.tsm1".into(),
                    size: 129 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "06-04.tsm1".into(),
                    size: 129 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "07-04.tsm1".into(),
                    size: 129 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "08-04.tsm1".into(),
                    size: 129 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "09-04.tsm1".into(), // should be skipped
                    size: 129 * 1024 * 1024,
                    ..Default::default()
                },
            ]
        }

        let mut fs = FileStore::new(Arc::new(PathBuf::new()), None);
        fs.paths_fn = Some(paths_fn);
        let mut cp = DefaultPlanner::new(Arc::new(fs), DEFAULT_COMPACT_FULL_WRITE_COLD_DURATION);

        let data = paths_fn();
        let exp_files = vec![
            &data[0], &data[1], &data[2], &data[3], &data[4], &data[5], &data[6], &data[7],
        ];

        let tsm = cp.plan(Time::now());

        assert_eq!(
            tsm.len(),
            2,
            "tsm file length mismatch: got {}, exp {}",
            tsm.len(),
            2
        );

        let got = tsm[0].len();
        let exp = exp_files[..4].len();
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );

        let got = tsm[1].len();
        let exp = exp_files[4..].len();
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );

        for (i, p) in exp_files[..4].iter().enumerate() {
            let got = &tsm[0][i];
            let exp = p.path.to_str().unwrap();
            assert_eq!(got, exp, "tsm file mismatch: got {}, exp {}", got, exp);
        }

        for (i, p) in exp_files[4..].iter().enumerate() {
            let got = &tsm[1][i];
            let exp = p.path.to_str().unwrap();
            assert_eq!(got, exp, "tsm file mismatch: got {}, exp {}", got, exp);
        }
    }

    // Ensure that the planner grabs the smallest compaction step
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_default_planner_plan_level_smallest_compaction_step() {
        fn paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "01-03.tsm1".into(),
                    size: 251 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "02-03.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "03-03.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "04-03.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "05-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "06-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "07-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "08-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "09-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "10-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "11-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "12-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
            ]
        }

        let mut fs = FileStore::new(Arc::new(PathBuf::new()), None);
        fs.paths_fn = Some(paths_fn);
        let cp = DefaultPlanner::new(Arc::new(fs), DEFAULT_COMPACT_FULL_WRITE_COLD_DURATION);

        let data = paths_fn();
        let exp_files = vec![
            &data[4], &data[5], &data[6], &data[7], &data[8], &data[9], &data[10], &data[11],
        ];

        let tsm = cp.plan_level(1);

        let got = tsm[0].len();
        let exp = exp_files.len();
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );

        for (i, p) in exp_files.iter().enumerate() {
            let got = &tsm[0][i];
            let exp = p.path.to_str().unwrap();
            assert_eq!(got, exp, "tsm file mismatch: got {}, exp {}", got, exp);
        }
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_default_planner_plan_level_split_file() {
        fn paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "01-03.tsm1".into(),
                    size: 251 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "02-03.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "03-03.tsm1".into(),
                    size: 2 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "03-04.tsm1".into(),
                    size: 10 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "04-03.tsm1".into(),
                    size: 10 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "05-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "06-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
            ]
        }

        let mut fs = FileStore::new(Arc::new(PathBuf::new()), None);
        fs.paths_fn = Some(paths_fn);
        let cp = DefaultPlanner::new(Arc::new(fs), DEFAULT_COMPACT_FULL_WRITE_COLD_DURATION);

        let data = paths_fn();
        let exp_files = vec![&data[0], &data[1], &data[2], &data[3], &data[4]];

        let tsm = cp.plan_level(3);

        let got = tsm[0].len();
        let exp = exp_files.len();
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );

        for (i, p) in exp_files.iter().enumerate() {
            let got = &tsm[0][i];
            let exp = p.path.to_str().unwrap();
            assert_eq!(got, exp, "tsm file mismatch: got {}, exp {}", got, exp);
        }
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_default_planner_plan_level_isolated_high_level() {
        fn paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "01-02.tsm1".into(),
                    size: 251 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "02-02.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "03-03.tsm1".into(),
                    size: 2 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "03-04.tsm1".into(),
                    size: 2 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "04-02.tsm1".into(),
                    size: 10 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "05-02.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "06-02.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
            ]
        }

        let mut fs = FileStore::new(Arc::new(PathBuf::new()), None);
        fs.paths_fn = Some(paths_fn);
        let cp = DefaultPlanner::new(Arc::new(fs), DEFAULT_COMPACT_FULL_WRITE_COLD_DURATION);

        let tsm = cp.plan_level(3);

        let got = tsm.len();
        let exp = 0;
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_default_planner_plan_level3_min_files() {
        fn paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "01-03.tsm1".into(),
                    size: 251 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "02-03.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "03-01.tsm1".into(),
                    size: 2 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "04-01.tsm1".into(),
                    size: 10 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "05-02.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "06-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
            ]
        }

        let mut fs = FileStore::new(Arc::new(PathBuf::new()), None);
        fs.paths_fn = Some(paths_fn);
        let cp = DefaultPlanner::new(Arc::new(fs), DEFAULT_COMPACT_FULL_WRITE_COLD_DURATION);

        let tsm = cp.plan_level(3);

        let got = tsm.len();
        let exp = 0;
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_default_planner_plan_level2_min_files() {
        fn paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "02-04.tsm1".into(),
                    size: 251 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "03-02.tsm1".into(),
                    size: 251 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "03-03.tsm1".into(),
                    size: 1 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
            ]
        }

        let mut fs = FileStore::new(Arc::new(PathBuf::new()), None);
        fs.paths_fn = Some(paths_fn);
        let cp = DefaultPlanner::new(Arc::new(fs), DEFAULT_COMPACT_FULL_WRITE_COLD_DURATION);

        let tsm = cp.plan_level(2);

        let got = tsm.len();
        let exp = 0;
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_default_planner_plan_level_tombstone() {
        fn paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "01-03.tsm1".into(),
                    size: 251 * 1024 * 1024,
                    has_tombstone: true,
                    ..Default::default()
                },
                FileStat {
                    path: "02-03.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "03-01.tsm1".into(),
                    size: 2 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "04-01.tsm1".into(),
                    size: 10 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "05-02.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "06-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
            ]
        }

        let mut fs = FileStore::new(Arc::new(PathBuf::new()), None);
        fs.paths_fn = Some(paths_fn);
        let cp = DefaultPlanner::new(Arc::new(fs), DEFAULT_COMPACT_FULL_WRITE_COLD_DURATION);

        let tsm = cp.plan_level(3);

        let data = paths_fn();
        let exp_files = vec![&data[0], &data[1]];
        let got = tsm[0].len();
        let exp = exp_files.len();
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );

        for (i, p) in exp_files.iter().enumerate() {
            let got = &tsm[0][i];
            let exp = p.path.to_str().unwrap();
            assert_eq!(got, exp, "tsm file mismatch: got {}, exp {}", got, exp);
        }
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_default_planner_plan_level_multiple() {
        fn paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "01-01.tsm1".into(),
                    size: 251 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "02-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "03-01.tsm1".into(),
                    size: 2 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "04-01.tsm1".into(),
                    size: 10 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "05-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "06-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "07-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "08-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
            ]
        }

        let mut fs = FileStore::new(Arc::new(PathBuf::new()), None);
        fs.paths_fn = Some(paths_fn);
        let cp = DefaultPlanner::new(Arc::new(fs), DEFAULT_COMPACT_FULL_WRITE_COLD_DURATION);

        let tsm = cp.plan_level(1);

        let data = paths_fn();
        let exp_files = vec![
            &data[0], &data[1], &data[2], &data[3], &data[4], &data[5], &data[6], &data[7],
        ];
        let got = tsm[0].len();
        let exp = exp_files.len();
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );

        for (i, p) in exp_files.iter().enumerate() {
            let got = &tsm[0][i];
            let exp = p.path.to_str().unwrap();
            assert_eq!(got, exp, "tsm file mismatch: got {}, exp {}", got, exp);
        }
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_default_planner_plan_level_in_use() {
        fn paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "01-01.tsm1".into(),
                    size: 251 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "02-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "03-01.tsm1".into(),
                    size: 2 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "04-01.tsm1".into(),
                    size: 10 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "05-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "06-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "07-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "08-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "09-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "10-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "11-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "12-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "13-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "14-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "15-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "16-01.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
            ]
        }

        let mut fs = FileStore::new(Arc::new(PathBuf::new()), None);
        fs.paths_fn = Some(paths_fn);
        let mut cp = DefaultPlanner::new(Arc::new(fs), DEFAULT_COMPACT_FULL_WRITE_COLD_DURATION);

        let tsm = cp.plan_level(1);

        let data = paths_fn();
        let exp_files1 = &data[..8];
        let exp_files2 = &data[8..];

        let got = tsm[0].len();
        let exp = exp_files1.len();
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );

        for (i, p) in exp_files1.iter().enumerate() {
            let got = &tsm[0][i];
            let exp = p.path.to_str().unwrap();
            assert_eq!(got, exp, "tsm file mismatch: got {}, exp {}", got, exp);
        }

        let got = tsm[1].len();
        let exp = exp_files2.len();
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );

        for (i, p) in exp_files2.iter().enumerate() {
            let got = &tsm[1][i];
            let exp = p.path.to_str().unwrap();
            assert_eq!(got, exp, "tsm file mismatch: got {}, exp {}", got, exp);
        }

        cp.release(&tsm[1..]);

        let tsm = cp.plan_level(1);

        let got = tsm[0].len();
        let exp = exp_files2.len();
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );

        for (i, p) in exp_files2.iter().enumerate() {
            let got = &tsm[0][i];
            let exp = p.path.to_str().unwrap();
            assert_eq!(got, exp, "tsm file mismatch: got {}, exp {}", got, exp);
        }
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_default_planner_plan_optimize_no_level4() {
        fn paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "01-03.tsm1".into(),
                    size: 251 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "02-03.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "03-03.tsm1".into(),
                    size: 2 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
            ]
        }

        let mut fs = FileStore::new(Arc::new(PathBuf::new()), None);
        fs.paths_fn = Some(paths_fn);
        let cp = DefaultPlanner::new(Arc::new(fs), DEFAULT_COMPACT_FULL_WRITE_COLD_DURATION);

        let tsm = cp.plan_optimize();

        assert_eq!(
            tsm.len(),
            0,
            "tsm file length mismatch: got {}, exp {}",
            tsm.len(),
            0
        );
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_default_planner_plan_optimize_level4() {
        fn paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "01-04.tsm1".into(),
                    size: 251 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "02-04.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "03-04.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "04-04.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "05-03.tsm1".into(),
                    size: 2 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "06-04.tsm1".into(),
                    size: 2 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "07-03.tsm1".into(),
                    size: 2 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
            ]
        }

        let mut fs = FileStore::new(Arc::new(PathBuf::new()), None);
        fs.paths_fn = Some(paths_fn);
        let cp = DefaultPlanner::new(Arc::new(fs), DEFAULT_COMPACT_FULL_WRITE_COLD_DURATION);

        let tsm = cp.plan_optimize();

        let data = paths_fn();
        let exp_files = vec![&data[0], &data[1], &data[2], &data[3], &data[4], &data[5]];
        let got = tsm[0].len();
        let exp = exp_files.len();
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );

        for (i, p) in exp_files.iter().enumerate() {
            let got = &tsm[0][i];
            let exp = p.path.to_str().unwrap();
            assert_eq!(got, exp, "tsm file mismatch: got {}, exp {}", got, exp);
        }
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_default_planner_plan_optimize_multiple() {
        fn paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "01-04.tsm1".into(),
                    size: 251 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "02-04.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "03-04.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "04-04.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "05-03.tsm1".into(),
                    size: 2 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "06-03.tsm1".into(),
                    size: 2 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "07-04.tsm1".into(),
                    size: 2 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "08-04.tsm1".into(),
                    size: 2 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "09-04.tsm1".into(),
                    size: 2 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "10-04.tsm1".into(),
                    size: 2 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
            ]
        }

        let mut fs = FileStore::new(Arc::new(PathBuf::new()), None);
        fs.paths_fn = Some(paths_fn);
        let cp = DefaultPlanner::new(Arc::new(fs), DEFAULT_COMPACT_FULL_WRITE_COLD_DURATION);

        let tsm = cp.plan_optimize();

        let data = paths_fn();
        let exp_files1 = vec![&data[0], &data[1], &data[2], &data[3]];
        let exp_files2 = vec![&data[6], &data[7], &data[8], &data[9]];

        let got = tsm.len();
        let exp = 2;
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );

        let got = tsm[0].len();
        let exp = exp_files1.len();
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );

        for (i, p) in exp_files1.iter().enumerate() {
            let got = &tsm[0][i];
            let exp = p.path.to_str().unwrap();
            assert_eq!(got, exp, "tsm file mismatch: got {}, exp {}", got, exp);
        }

        let got = tsm[1].len();
        let exp = exp_files2.len();
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );

        for (i, p) in exp_files2.iter().enumerate() {
            let got = &tsm[1][i];
            let exp = p.path.to_str().unwrap();
            assert_eq!(got, exp, "tsm file mismatch: got {}, exp {}", got, exp);
        }
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_default_planner_plan_optimized() {
        fn paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "01-01.tsm1".into(),
                    size: 251 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "01-04.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "01-05.tsm1".into(),
                    size: 2 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
            ]
        }

        let mut fs = FileStore::new(Arc::new(PathBuf::new()), None);
        fs.paths_fn = Some(paths_fn);
        let cp = DefaultPlanner::new(Arc::new(fs), DEFAULT_COMPACT_FULL_WRITE_COLD_DURATION);

        let tsm = cp.plan_optimize();

        let got = tsm.len();
        let exp = 0;
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_default_planner_plan_optimize_tombstone() {
        fn paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "01-04.tsm1".into(),
                    size: 251 * 1024 * 1024,
                    has_tombstone: true,
                    ..Default::default()
                },
                FileStat {
                    path: "01-05.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "02-06.tsm1".into(),
                    size: 2 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
            ]
        }

        let mut fs = FileStore::new(Arc::new(PathBuf::new()), None);
        fs.paths_fn = Some(paths_fn);
        let cp = DefaultPlanner::new(Arc::new(fs), DEFAULT_COMPACT_FULL_WRITE_COLD_DURATION);

        let tsm = cp.plan_optimize();

        let data = paths_fn();
        let exp_files = vec![&data[0], &data[1], &data[2]];

        let got = tsm[0].len();
        let exp = exp_files.len();
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );

        for (i, p) in exp_files.iter().enumerate() {
            let got = &tsm[0][i];
            let exp = p.path.to_str().unwrap();
            assert_eq!(got, exp, "tsm file mismatch: got {}, exp {}", got, exp);
        }
    }

    // Ensure that the planner will compact all files if no writes
    // have happened in some interval
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_default_planner_plan_full_on_cold() {
        fn paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "01-01.tsm1".into(),
                    size: 513 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "02-02.tsm1".into(),
                    size: 129 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "03-02.tsm1".into(),
                    size: 33 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "04-02.tsm1".into(),
                    size: 1 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "05-02.tsm1".into(),
                    size: 10 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "06-01.tsm1".into(),
                    size: 2 * 1024 * 1024,
                    ..Default::default()
                },
            ]
        }

        let mut fs = FileStore::new(Arc::new(PathBuf::new()), None);
        fs.paths_fn = Some(paths_fn);
        let mut cp = DefaultPlanner::new(Arc::new(fs), Duration::from_nanos(1));

        let tsm = cp.plan(Time::now().checked_sub(Duration::from_secs(1)).unwrap());

        let data = paths_fn();
        let got = tsm[0].len();
        let exp = data.len();
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );

        for (i, p) in data.iter().enumerate() {
            let got = &tsm[0][i];
            let exp = p.path.to_str().unwrap();
            assert_eq!(got, exp, "tsm file mismatch: got {}, exp {}", got, exp);
        }
    }

    // Ensure that the planner will not return files that are over the max
    // allowable size
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_default_planner_plan_skip_max_size_files() {
        fn paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "01-01.tsm1".into(),
                    size: 2049 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "02-02.tsm1".into(),
                    size: 2049 * 1024 * 1024,
                    ..Default::default()
                },
            ]
        }

        let mut fs = FileStore::new(Arc::new(PathBuf::new()), None);
        fs.paths_fn = Some(paths_fn);
        let mut cp = DefaultPlanner::new(Arc::new(fs), DEFAULT_COMPACT_FULL_WRITE_COLD_DURATION);

        let tsm = cp.plan(Time::now());

        let got = tsm.len();
        let exp = 0;
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );
    }

    // Ensure that the planner will not return files that are over the max
    // allowable size
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_default_planner_plan_skip_planning_after_full() {
        fn paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "01-05.tsm1".into(),
                    size: 256 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "02-05.tsm1".into(),
                    size: 256 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "03-05.tsm1".into(),
                    size: 256 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "04-04.tsm1".into(),
                    size: 256 * 1024 * 1024,
                    ..Default::default()
                },
            ]
        }

        let mut fs = FileStore::new(Arc::new(PathBuf::new()), None);
        fs.paths_fn = Some(paths_fn);
        fs.block_count = 1000;
        let mut cp = DefaultPlanner::new(Arc::new(fs), Duration::from_nanos(1));

        let tsm = cp.plan(Time::now().checked_sub(Duration::from_secs(1)).unwrap());

        // first verify that our test set would return files
        let got = tsm[0].len();
        let exp = 4;
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );
        cp.release(&tsm);

        // skip planning if all files are over the limit
        fn over_paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "01-05.tsm1".into(),
                    size: 2049 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "02-05.tsm1".into(),
                    size: 2049 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "03-05.tsm1".into(),
                    size: 2049 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "04-05.tsm1".into(),
                    size: 2049 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "05-05.tsm1".into(),
                    size: 2049 * 1024 * 1024,
                    ..Default::default()
                },
            ]
        }

        let mut over_fs = FileStore::new(Arc::new(PathBuf::new()), None);
        over_fs.paths_fn = Some(over_paths_fn);
        over_fs.block_count = 1000;
        cp.set_file_store(Arc::new(over_fs));

        let tsm = cp.plan(Time::now().checked_sub(Duration::from_secs(1)).unwrap());

        // first verify that our test set would return files
        let got = tsm.len();
        let exp = 0;
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );
        cp.release(&tsm);

        let tsm = cp.plan_optimize();
        // ensure the optimize planner would pick this up
        let got = tsm.len();
        let exp = 1;
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );
        cp.release(&tsm);

        let mut fs = FileStore::new(Arc::new(PathBuf::new()), None);
        fs.paths_fn = Some(paths_fn);
        // ensure that it will plan if last modified has changed
        fs.set_last_modified(Time::now());
        cp.set_file_store(Arc::new(fs));

        let tsm = cp.plan(Time::now());
        let got = tsm[0].len();
        let exp = 4;
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );
    }

    // Tests that 2 generations, each over 2 GB and the second in level 2 does
    // not return just the first generation.  This was a case where full planning
    // would get repeatedly plan the same files and never stop.
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_default_planner_plan_two_gen_level3() {
        fn paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "000002245-000001666.tsm".into(),
                    size: 2049 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "000002245-000001667.tsm".into(),
                    size: 2049 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "000002245-000001668.tsm".into(),
                    size: 2049 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "000002245-000001669.tsm".into(),
                    size: 2049 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "000002245-000001670.tsm".into(),
                    size: 2049 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "000002245-000001671.tsm".into(),
                    size: 2049 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "000002245-000001672.tsm".into(),
                    size: 2049 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "000002245-000001673.tsm".into(),
                    size: 2049 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "000002246-000000002.tsm".into(),
                    size: 2049 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "000002246-000000003.tsm".into(),
                    size: 192631258,
                    ..Default::default()
                },
            ]
        }

        let mut fs = FileStore::new(Arc::new(PathBuf::new()), None);
        fs.paths_fn = Some(paths_fn);
        fs.block_count = 1000;
        let mut cp = DefaultPlanner::new(Arc::new(fs), Duration::from_hours(1));

        let tsm = cp.plan(Time::now().checked_sub(Duration::from_hours(24)).unwrap());

        let got = tsm.len();
        let exp = 1;
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );
    }

    // Ensure that the planner will return files over the max file
    // size, but do not contain full blocks
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_default_planner_plan_not_full_over_max_size() {
        fn paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "01-05.tsm1".into(),
                    size: 256 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "02-05.tsm1".into(),
                    size: 256 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "03-05.tsm1".into(),
                    size: 256 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "04-04.tsm1".into(),
                    size: 256 * 1024 * 1024,
                    ..Default::default()
                },
            ]
        }

        let mut fs = FileStore::new(Arc::new(PathBuf::new()), None);
        fs.paths_fn = Some(paths_fn);
        fs.block_count = 100;
        let mut cp = DefaultPlanner::new(Arc::new(fs), Duration::from_nanos(1));

        let tsm = cp.plan(Time::now().checked_sub(Duration::from_secs(1)).unwrap());
        // first verify that our test set would return files
        let got = tsm[0].len();
        let exp = 4;
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );
        cp.release(&tsm);

        // skip planning if all files are over the limit
        fn over_paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "01-05.tsm1".into(),
                    size: 2049 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "02-05.tsm1".into(),
                    size: 2049 * 1024 * 1024,
                    ..Default::default()
                },
            ]
        }

        let mut over_fs = FileStore::new(Arc::new(PathBuf::new()), None);
        over_fs.paths_fn = Some(over_paths_fn);
        over_fs.block_count = 100;

        cp.set_file_store(Arc::new(over_fs));

        let tsm = cp.plan(Time::now().checked_sub(Duration::from_secs(1)).unwrap());
        // first verify that our test set would return files
        let got = tsm.len();
        let exp = 1;
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );
    }

    // Ensure that the planner will compact files that are past the smallest step
    // size even if there is a single file in the smaller step size
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_default_planner_plan_compacts_middle_steps() {
        fn paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "01-04.tsm1".into(),
                    size: 64 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "02-04.tsm1".into(),
                    size: 64 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "03-04.tsm1".into(),
                    size: 64 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "04-04.tsm1".into(),
                    size: 64 * 1024 * 1024,
                    ..Default::default()
                },
                FileStat {
                    path: "05-02.tsm1".into(),
                    size: 2 * 1024 * 1024,
                    ..Default::default()
                },
            ]
        }

        let mut fs = FileStore::new(Arc::new(PathBuf::new()), None);
        fs.paths_fn = Some(paths_fn);
        let mut cp = DefaultPlanner::new(Arc::new(fs), DEFAULT_COMPACT_FULL_WRITE_COLD_DURATION);

        let data = paths_fn();
        let exp_files = vec![&data[0], &data[1], &data[2], &data[3]];

        let tsm = cp.plan(Time::now());
        let got = tsm[0].len();
        let exp = exp_files.len();
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );

        for (i, p) in exp_files.iter().enumerate() {
            let got = &tsm[0][i];
            let exp = p.path.to_str().unwrap();
            assert_eq!(got, exp, "tsm file mismatch: got {}, exp {}", got, exp);
        }
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_default_planner_plan_large_generation() {
        fn paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "000000278-000000006.tsm".into(),
                    size: 2148340232,
                    ..Default::default()
                },
                FileStat {
                    path: "000000278-000000007.tsm".into(),
                    size: 2148356556,
                    ..Default::default()
                },
                FileStat {
                    path: "000000278-000000008.tsm".into(),
                    size: 167780181,
                    ..Default::default()
                },
                FileStat {
                    path: "000000278-000047040.tsm".into(),
                    size: 2148728539,
                    ..Default::default()
                },
                FileStat {
                    path: "000000278-000047041.tsm".into(),
                    size: 701863692,
                    ..Default::default()
                },
            ]
        }

        let mut fs = FileStore::new(Arc::new(PathBuf::new()), None);
        fs.paths_fn = Some(paths_fn);
        let mut cp = DefaultPlanner::new(Arc::new(fs), DEFAULT_COMPACT_FULL_WRITE_COLD_DURATION);

        let tsm = cp.plan(Time::now());
        let got = tsm.len();
        let exp = 0;
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_compactor_default_planner_plan_force_full() {
        fn paths_fn() -> Vec<FileStat> {
            vec![
                FileStat {
                    path: "000000001-000000001.tsm".into(),
                    size: 2148340232,
                    ..Default::default()
                },
                FileStat {
                    path: "000000002-000000001.tsm".into(),
                    size: 2148356556,
                    ..Default::default()
                },
                FileStat {
                    path: "000000003-000000001.tsm".into(),
                    size: 167780181,
                    ..Default::default()
                },
                FileStat {
                    path: "000000004-000000001.tsm".into(),
                    size: 2148728539,
                    ..Default::default()
                },
                FileStat {
                    path: "000000005-000000001.tsm".into(),
                    size: 2148340232,
                    ..Default::default()
                },
                FileStat {
                    path: "000000006-000000001.tsm".into(),
                    size: 2148356556,
                    ..Default::default()
                },
                FileStat {
                    path: "000000007-000000001.tsm".into(),
                    size: 167780181,
                    ..Default::default()
                },
                FileStat {
                    path: "000000008-000000001.tsm".into(),
                    size: 2148728539,
                    ..Default::default()
                },
                FileStat {
                    path: "000000009-000000002.tsm".into(),
                    size: 701863692,
                    ..Default::default()
                },
                FileStat {
                    path: "000000010-000000002.tsm".into(),
                    size: 701863692,
                    ..Default::default()
                },
                FileStat {
                    path: "000000011-000000002.tsm".into(),
                    size: 701863692,
                    ..Default::default()
                },
                FileStat {
                    path: "000000012-000000002.tsm".into(),
                    size: 701863692,
                    ..Default::default()
                },
                FileStat {
                    path: "000000013-000000002.tsm".into(),
                    size: 701863692,
                    ..Default::default()
                },
            ]
        }

        let mut fs = FileStore::new(Arc::new(PathBuf::new()), None);
        fs.paths_fn = Some(paths_fn);
        let mut cp = DefaultPlanner::new(Arc::new(fs), DEFAULT_COMPACT_FULL_WRITE_COLD_DURATION);

        let tsm = cp.plan_level(1);
        let got = tsm.len();
        let exp = 1;
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );
        cp.release(&tsm);

        let tsm = cp.plan_level(2);
        let got = tsm.len();
        let exp = 1;
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );
        cp.release(&tsm);

        cp.force_full();

        // Level plans should not return any plans
        let tsm = cp.plan_level(1);
        let got = tsm.len();
        let exp = 0;
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );
        cp.release(&tsm);

        let tsm = cp.plan_level(2);
        let got = tsm.len();
        let exp = 0;
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );
        cp.release(&tsm);

        let tsm = cp.plan(Time::now());
        let got = tsm.len();
        let exp = 1;
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );

        let got = tsm[0].len();
        let exp = 13;
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );
        cp.release(&tsm);

        // Level plans should return plans now that Plan has been called
        let tsm = cp.plan_level(1);
        let got = tsm.len();
        let exp = 1;
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );
        cp.release(&tsm);

        let tsm = cp.plan_level(2);
        let got = tsm.len();
        let exp = 1;
        assert_eq!(
            got, exp,
            "tsm file length mismatch: got {}, exp {}",
            got, exp
        );
        cp.release(&tsm);
    }

    #[cfg(test)]
    #[ctor::ctor]
    fn init_runtime() {
        let runtime_opt = crate::runtime::global::RuntimeOptions::default();
        crate::runtime::global::init_global_runtimes(&runtime_opt);
    }

    #[derive(Clone)]
    pub struct KeyValues {
        pub key: &'static str,
        pub points: Vec<Value>,
    }

    /// Return file dir, file name and tsm writer to prevent automatic drop the dir.
    pub fn must_tsm_writer(dir: &Path, generation: i64) -> (PathBuf, String, TSMWriter<fs::File>) {
        let (f, fpath) = must_temp_tsm_file(dir).keep().unwrap();

        // Windows can't rename a file while it's open.
        // TODO: Close first, rename and then re-open

        let new_name = dir.join(default_format_file_name(generation, 1) + ".tsm");
        fs::rename(fpath, &new_name).unwrap();

        let f = match fs::OpenOptions::new()
            .write(true)
            .read(true)
            .open(&new_name)
        {
            Ok(file) => file,
            Err(err) => panic!("error opening file {:?}: {:?}", &new_name, err),
        };
        f.metadata().unwrap().permissions().set_mode(0o666);

        (
            dir.into(),
            new_name.to_str().unwrap().to_string(),
            TSMWriter::new(f, generation, 1, new_name),
        )
    }

    pub fn must_write_tsm(
        dir: &Path,
        generation: i64,
        values: HashMap<String, Vec<Value>>,
    ) -> (PathBuf, String) {
        let (dir, name, mut w) = must_tsm_writer(dir, generation);

        let mut keys = vec![];
        for (k, v) in values.iter() {
            keys.push(k.clone());
        }
        keys.sort();

        for k in keys {
            w.write(k.as_bytes(), values.get(&k).unwrap()).unwrap();
        }

        w.write_index().unwrap();

        w.close().unwrap();

        (dir, name)
    }

    pub fn must_tsm_reader(
        dir: &Path,
        generation: i64,
        seq: i64,
        values: HashMap<String, Vec<Value>>,
    ) -> TSMReader {
        let (dir, name) = must_write_tsm(dir, generation, values);
        must_open_tsm_reader(&name, generation, seq)
    }

    pub fn must_open_tsm_reader(name: &str, generation: i64, seq: i64) -> TSMReader {
        let f = fs::OpenOptions::new().read(true).open(name).unwrap();
        TSMReader::new(f, Path::new(name), generation, seq).unwrap()
    }

    fn assert_persisted_statistics_match_decoded(reader: &TSMReader, key: &[u8]) {
        let block_type = reader.values_type(key).unwrap();
        for entry in reader.entries(key) {
            let expected = match block_type {
                INTEGER => BlockStatistics::from_integer(
                    reader
                        .read_integer_block_at(&entry)
                        .unwrap()
                        .into_iter()
                        .map(|value| value.value),
                ),
                UNSIGNED => BlockStatistics::from_unsigned(
                    reader
                        .read_unsigned_block_at(&entry)
                        .unwrap()
                        .into_iter()
                        .map(|value| value.value),
                ),
                FLOAT => BlockStatistics::from_float(
                    reader
                        .read_float_block_at(&entry)
                        .unwrap()
                        .into_iter()
                        .map(|value| value.value),
                ),
                BOOL => BlockStatistics::from_bool(
                    reader
                        .read_bool_block_at(&entry)
                        .unwrap()
                        .into_iter()
                        .map(|value| value.value),
                ),
                STRING => {
                    BlockStatistics::count_only(reader.read_string_block_at(&entry).unwrap().len())
                }
                other => panic!("unsupported block type {other}"),
            };
            assert_eq!(
                entry.statistics,
                expected,
                "persisted statistics differ from decoded output for key {} entry {:?}",
                String::from_utf8_lossy(key),
                entry
            );
        }
    }
}
