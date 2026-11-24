//! Default storage configuration.
use std::time::Duration;

use crate::common::time::Time;

use crate::index::TSI1_INDEX_NAME;

// DefaultEngine is the default engine for new shards
pub const DEFAULT_ENGINE: &str = "tsm1";
// DefaultIndex is the default index for new shards
pub const DEFAULT_INDEX: &str = TSI1_INDEX_NAME;

// tsdb/engine/wal configuration options

// Default settings for TSM
// DefaultCacheMaxMemorySize is the maximum size a shard's cache can
// reach before it starts rejecting writes.
pub const DEFAULT_CACHT_MAX_MEMORY_SIZE: i64 = 1024 * 1024 * 1024; // 1 GiB

// DefaultCacheSnapshotMemorySize is the size at which the engine will
// snapshot the cache and write it to a TSM file, freeing up memory
pub const DEFAULT_CACHE_SNAPSHOT_MEMORY_SIZE: i64 = 25 * 1024 * 1024; // 25 MiB

// DefaultCacheSnapshotWriteColdDuration is the length of time at which
// the engine will snapshot the cache and write it to a new TSM file if
// the shard hasn't received writes or deletes
pub const DEFAULT_CACHE_SNAPSHOT_WRITE_COLD_DURATION: Duration = Duration::from_secs(10 * 60);

// DefaultCompactFullWriteColdDuration is the duration at which the engine
// will compact all TSM files in a shard if it hasn't received a write or delete
pub const DEFAULT_COMPACT_FULL_WRITE_COLD_DURATION: Duration = Duration::from_secs(4 * 60 * 60);

// DefaultCompactThroughput is the rate limit in bytes per second that we
// will allow TSM compactions to write to disk. Not that short bursts are allowed
// to happen at a possibly larger value, set by DefaultCompactThroughputBurst.
// A value of 0 here will disable compaction rate limiting
pub const DEFAULT_COMPACT_THROUGHPUT: i64 = 48 * 1024 * 1024;

// DefaultCompactThroughputBurst is the rate limit in bytes per second that we
// will allow TSM compactions to write to disk. If this is not set, the burst value
// will be set to equal the normal throughput
pub const DEFAULT_COMPACT_THROUGHPUT_BURST: i64 = 48 * 1024 * 1024;

// DefaultMaxPointsPerBlock is the maximum number of points in an encoded
// block in a TSM file
pub const DEFAULT_MAX_POINTS_PER_BLOCK: i64 = 1000 * 4;
// pub const DEFAULT_MAX_POINTS_PER_BLOCK: i64 = 1000;

// DefaultMaxValuesPerTag is the maximum number of values a tag can have within a measurement.
pub const DEFAULT_MAX_VALUES_PER_TAG: i64 = 100000;

// DefaultMaxConcurrentCompactions is the maximum number of concurrent full and level compactions
// that can run at one time.  A value of 0 results in 50% of num_cpus::get() used at runtime.
pub const DEFAULT_MAX_CONCURRENT_COMPACTIONS: i64 = 0;

// DefaultMaxIndexLogFileSize is the default threshold, in bytes, when an index
// write-ahead log file will compact into an index file.
pub const DEFAULT_MAX_INDEX_LOG_FILE_SIZE: i64 = 1 * 1024 * 1024; // 1MB

// DefaultSeriesIDSetCacheSize is the default number of series ID sets to cache in the TSI index.
pub const DEFAULT_SERIES_ID_SET_CACHE_SIZE: i64 = 100;

// DefaultSeriesFileMaxConcurrentSnapshotCompactions is the maximum number of concurrent series
// partition snapshot compactions that can run at one time.
// A value of 0 results in num_cpus::get().
pub const DEFAULT_SERIES_FILE_MAX_CONCURRENT_SNAPSHOT_COMPACTIONS: i64 = 0;

// A value of 0 results in 50% of num_cpus::get() used at runtime.
pub const DEFAULT_VLOG_MAX_WAL_WRITERS: i64 = 0;
// A value of 0 results in 1.
pub const DEFAULT_VLOG_MAX_WRITERS: i64 = 1;

// TODO: 不管DEFAULT_CACHT_MAX_MEMORY_SIZE和DEFAULT_CACHE_SNAPSHOT_MEMORY_SIZE是多少，
//        32KB大部分数据落在tsm，28KB大部分数据落在vlog，找原因
// The following group of parameters is used to track the state of vlog compactions.
// DEFAULT_MIN_VLOG_SIZE is the default threshold, in bytes, when an encoded values will
// write to vlog.
pub const DEFAULT_MIN_VLOG_SIZE: i64 = 28 * 1024; // 28KB  // random float value
// pub const DEFAULT_MIN_VLOG_SIZE: i64 = 4 * 1024; // 28KB  // sequence float value

// Default settings for TSM
// DefaultCacheMaxMemorySize is the maximum size a shard's cache can
// reach before it starts rejecting writes.
pub const DEFAULT_VLOG_INDEX_CACHT_MAX_MEMORY_SIZE: i64 = 10 * 1024 * 1024 * 1024; // 10GB

// DefaultCacheSnapshotMemorySize is the size at which the engine will
// snapshot the cache and write it to a TSM file, freeing up memory
pub const DEFAULT_VLOG_INDEX_CACHE_SNAPSHOT_MEMORY_SIZE: i64 = 1 * 1024 * 1024 * 1024; // 1GB

// DefaultCacheSnapshotWriteColdDuration is the length of time at which
// the engine will snapshot the cache and write it to a new TSM file if
// the shard hasn't received writes or deletes
pub const DEFAULT_VLOG_INDEX_CACHE_SNAPSHOT_WRITE_COLD_DURATION: Duration =
    Duration::from_secs(12 * 60 * 60);

// DefaultCacheSnapshotWriteColdDuration is the length of time at which
// the engine will snapshot the cache and write it to a new TSM file if
// the shard hasn't received writes or deletes
pub const DEFAULT_VLOG_INDEX_COMPACT_FULL_WRITE_COLD_DURATION: Duration =
    Duration::from_secs(24 * 60 * 60);

// DefaultCompactFullWriteColdDuration is the duration at which the engine
// will compact all TSM files in a shard if it hasn't received a write or delete
pub const DEFAULT_VLOG_COMPACT_FULL_WRITE_COLD_DURATION: Duration =
    Duration::from_secs(4 * 60 * 60);

// DefaultMaxConcurrentCompactions is the maximum number of concurrent full and level compactions
// that can run at one time.  A value of 0 results in 50% of num_cpus::get() used at runtime.
pub const DEFAULT_VLOG_MAX_CONCURRENT_COMPACTIONS: i64 = 0;

pub const DEFAULT_VLOG_LEVEL_1_MAX_SIZE_PER_COMPACT: i64 = 512 * 1024 * 1024; // 512MB
pub const DEFAULT_VLOG_LEVEL_2_MAX_SIZE_PER_COMPACT: i64 = 768 * 1024 * 1024; // 768MB
pub const DEFAULT_VLOG_LEVEL_3_MAX_SIZE_PER_COMPACT: i64 = 1 * 1024 * 1024 * 1024; // 1GB
pub const DEFAULT_VLOG_LEVEL_4_MAX_SIZE_PER_COMPACT: i64 = 2 * 1024 * 1024 * 1024; // 2GB

// DefaultCompactThroughput is the rate limit in bytes per second that we
// will allow TSM compactions to write to disk. Not that short bursts are allowed
// to happen at a possibly larger value, set by DefaultCompactThroughputBurst.
// A value of 0 here will disable compaction rate limiting
pub const DEFAULT_VLOG_COMPACT_THROUGHPUT: i64 = 48 * 1024 * 1024;

// DefaultCompactThroughputBurst is the rate limit in bytes per second that we
// will allow TSM compactions to write to disk. If this is not set, the burst value
// will be set to equal the normal throughput
pub const DEFAULT_VLOG_COMPACT_THROUGHPUT_BURST: i64 = 48 * 1024 * 1024;

/// Config holds the configuration for the tsbd package.
#[derive(Clone)]
pub struct Config {
    pub dir: String,
    pub engine: String,
    pub index: String,

    pub wal_enabled: bool,
    // General WAL configuration options
    pub wal_dir: String,
    // wal_fsync_delay is the amount of time that a write will wait before fsyncing.  A duration
    // greater than 0 can be used to batch up multiple fsync calls.  This is useful for slower
    // disks or when WAL write contention is seen.  A value of 0 fsyncs every write to the WAL.
    pub wal_fsync_delay: Duration,

    // Enables unicode validation on series keys on write.
    pub validate_keys: bool,

    // Query logging
    pub query_log_enabled: bool,

    // Compaction options for tsm1 (descriptions above with defaults)
    pub cache_max_memory_size: i64,
    pub cache_snapshot_memory_size: i64,
    pub cache_snapshot_write_cold_duration: Duration,
    pub compact_full_write_cold_duration: Duration,
    pub compact_throughput: i64,
    pub compact_throughput_burst: i64,

    // Limits

    // MaxConcurrentCompactions is the maximum number of concurrent level and full compactions
    // that can be running at one time across all shards.  Compactions scheduled to run when the
    // limit is reached are blocked until a running compaction completes.  Snapshot compactions are
    // not affected by this limit.  A value of 0 limits compactions to num_cpus::get().
    pub max_concurrent_compactions: i64,

    // MaxIndexLogFileSize is the threshold, in bytes, when an index write-ahead log file will
    // compact into an index file. Lower sizes will cause log files to be compacted more quickly
    // and result in lower heap usage at the expense of write throughput. Higher sizes will
    // be compacted less frequently, store more series in-memory, and provide higher write throughput.
    pub max_index_log_file_size: i64,

    // SeriesIDSetCacheSize is the number items that can be cached within the TSI index. TSI caching can help
    // with query performance when the same tag key/value predicates are commonly used on queries.
    // Setting series-id-set-cache-size to 0 disables the cache.
    pub series_id_set_cache_size: i64,

    // SeriesFileMaxConcurrentSnapshotCompactions is the maximum number of concurrent snapshot compactions
    // that can be running at one time across all series partitions in a database. Snapshots scheduled
    // to run when the limit is reached are blocked until a running snapshot completes.  Only snapshot
    // compactions are affected by this limit. A value of 0 limits snapshot compactions to the lesser of
    // 8 (series file partition quantity) and num_cpus::get().
    pub series_file_max_concurrent_snapshot_compactions: i64,

    pub trace_logging_enabled: bool,

    // TSMWillNeed controls whether we hint to the kernel that we intend to
    // page in mmap'd sections of TSM files. This setting defaults to off, as it has
    // been found to be problematic in some cases. It may help users who have
    // slow disks.
    pub tsm_will_need: bool,
}

impl Config {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            engine: DEFAULT_ENGINE.to_string(),
            index: DEFAULT_INDEX.to_string(),

            query_log_enabled: true,

            cache_max_memory_size: DEFAULT_CACHT_MAX_MEMORY_SIZE,
            cache_snapshot_memory_size: DEFAULT_CACHE_SNAPSHOT_MEMORY_SIZE,
            cache_snapshot_write_cold_duration: DEFAULT_CACHE_SNAPSHOT_WRITE_COLD_DURATION,
            compact_full_write_cold_duration: DEFAULT_COMPACT_FULL_WRITE_COLD_DURATION,
            compact_throughput: DEFAULT_COMPACT_THROUGHPUT,
            compact_throughput_burst: DEFAULT_COMPACT_THROUGHPUT_BURST,

            max_concurrent_compactions: DEFAULT_MAX_CONCURRENT_COMPACTIONS,

            max_index_log_file_size: DEFAULT_MAX_INDEX_LOG_FILE_SIZE,
            series_id_set_cache_size: DEFAULT_SERIES_ID_SET_CACHE_SIZE,

            series_file_max_concurrent_snapshot_compactions:
                DEFAULT_SERIES_FILE_MAX_CONCURRENT_SNAPSHOT_COMPACTIONS,

            trace_logging_enabled: false,
            tsm_will_need: false,

            dir: String::new(),
            wal_enabled: true,
            wal_dir: String::new(),
            wal_fsync_delay: Duration::from_secs(0),
            validate_keys: false,
        }
    }
}

/// Config holds the configuration for the tsbd package.
#[derive(Clone)]
pub struct VLogConfig {
    pub dir: String,
    // pub engine: String,
    // pub index: String,

    // min_vlog_size is the minimum size of encoded values that should be written to vlog in writing snapshot.
    // If the encoded values's size if greater than min_vlog_size, the encoded values should be written to vlog,
    // otherwise, they will be written to the tsm file directly.
    // min_vlog_size is valid only when vlog_enabled is true.
    pub min_vlog_size: i64,

    // // General WAL configuration options
    // pub wal_dir: String,
    pub max_wal_writers: i32,
    pub max_vlog_writers: i32,

    // WALFsyncDelay is the amount of time that a write will wait before fsyncing.  A duration
    // greater than 0 can be used to batch up multiple fsync calls.  This is useful for slower
    // disks or when WAL write contention is seen.  A value of 0 fsyncs every write to the WAL.
    pub wal_fsync_delay: Duration,

    // // Enables unicode validation on series keys on write.
    // pub validate_keys: bool,

    // // Query logging
    // pub query_log_enabled: bool,

    // // Compaction options for tsm1 (descriptions above with defaults)
    pub index_cache_max_memory_size: i64,
    pub index_cache_snapshot_memory_size: i64,
    pub index_cache_snapshot_write_cold_duration: Duration,
    pub index_compact_full_write_cold_duration: Duration,

    // Limits

    // MaxConcurrentCompactions is the maximum number of concurrent level and full compactions
    // that can be running at one time across all shards.  Compactions scheduled to run when the
    // limit is reached are blocked until a running compaction completes.  Snapshot compactions are
    // not affected by this limit.  A value of 0 limits compactions to num_cpus::get().
    pub max_concurrent_compactions: i64,

    // // MaxIndexLogFileSize is the threshold, in bytes, when an index write-ahead log file will
    // // compact into an index file. Lower sizes will cause log files to be compacted more quickly
    // // and result in lower heap usage at the expense of write throughput. Higher sizes will
    // // be compacted less frequently, store more series in-memory, and provide higher write throughput.
    // pub max_index_log_file_size: i64,

    // // SeriesIDSetCacheSize is the number items that can be cached within the TSI index. TSI caching can help
    // // with query performance when the same tag key/value predicates are commonly used on queries.
    // // Setting series-id-set-cache-size to 0 disables the cache.
    // pub series_id_set_cache_size: i64,

    // // SeriesFileMaxConcurrentSnapshotCompactions is the maximum number of concurrent snapshot compactions
    // // that can be running at one time across all series partitions in a database. Snapshots scheduled
    // // to run when the limit is reached are blocked until a running snapshot completes.  Only snapshot
    // // compactions are affected by this limit. A value of 0 limits snapshot compactions to the lesser of
    // // 8 (series file partition quantity) and num_cpus::get().
    // pub series_file_max_concurrent_snapshot_compactions: i64,
    pub vlog_compact_level_1_min_files: i64,
    pub vlog_compact_level_2_min_files: i64,
    pub vlog_compact_level_3_min_files: i64,
    pub vlog_compact_full_min_files: i64,

    pub vlog_level_1_max_size_per_compact: i64,
    pub vlog_level_2_max_size_per_compact: i64,
    pub vlog_level_3_max_size_per_compact: i64,
    pub vlog_level_4_max_size_per_compact: i64,

    pub vlog_compact_full_write_cold_duration: Duration,
    pub vlog_compact_throughput: i64,
    pub vlog_compact_throughput_burst: i64,

    pub vlog_compact_prioritize_lru_keys: bool,
    pub vlog_compact_uncached_keys: bool,

    pub trace_logging_enabled: bool,
    // // TSMWillNeed controls whether we hint to the kernel that we intend to
    // // page in mmap'd sections of TSM files. This setting defaults to off, as it has
    // // been found to be problematic in some cases. It may help users who have
    // // slow disks.
    // pub tsm_will_need: bool,
}

impl VLogConfig {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for VLogConfig {
    fn default() -> Self {
        let cpus = num_cpus::get() as i32;
        Self {
            dir: String::new(),
            // engine: DEFAULT_ENGINE.to_string(),
            // index: DEFAULT_INDEX.to_string(),
            min_vlog_size: DEFAULT_MIN_VLOG_SIZE,

            // query_log_enabled: true,
            max_wal_writers: i32::max(cpus / 2, 1),
            max_vlog_writers: i32::max(cpus / 4, 1),
            wal_fsync_delay: Duration::from_secs(0),

            index_cache_max_memory_size: DEFAULT_VLOG_INDEX_CACHT_MAX_MEMORY_SIZE,
            index_cache_snapshot_memory_size: DEFAULT_VLOG_INDEX_CACHE_SNAPSHOT_MEMORY_SIZE,
            index_cache_snapshot_write_cold_duration:
                DEFAULT_VLOG_INDEX_CACHE_SNAPSHOT_WRITE_COLD_DURATION,
            index_compact_full_write_cold_duration:
                DEFAULT_VLOG_INDEX_COMPACT_FULL_WRITE_COLD_DURATION,
            max_concurrent_compactions: DEFAULT_MAX_CONCURRENT_COMPACTIONS,

            vlog_compact_level_1_min_files: (cpus as i64).clamp(8, 64),
            vlog_compact_level_2_min_files: ((cpus as i64) / 2).clamp(4, 32),
            vlog_compact_level_3_min_files: ((cpus as i64) / 4).clamp(4, 16),
            vlog_compact_full_min_files: ((cpus as i64) * 4).clamp(64, 256),

            vlog_level_1_max_size_per_compact: DEFAULT_VLOG_LEVEL_1_MAX_SIZE_PER_COMPACT,
            vlog_level_2_max_size_per_compact: DEFAULT_VLOG_LEVEL_2_MAX_SIZE_PER_COMPACT,
            vlog_level_3_max_size_per_compact: DEFAULT_VLOG_LEVEL_3_MAX_SIZE_PER_COMPACT,
            vlog_level_4_max_size_per_compact: DEFAULT_VLOG_LEVEL_4_MAX_SIZE_PER_COMPACT,

            // max_index_log_file_size: DEFAULT_MAX_INDEX_LOG_FILE_SIZE,
            // series_id_set_cache_size: DEFAULT_SERIES_ID_SET_CACHE_SIZE,

            // series_file_max_concurrent_snapshot_compactions:
            //     DEFAULT_SERIES_FILE_MAX_CONCURRENT_SNAPSHOT_COMPACTIONS,
            vlog_compact_full_write_cold_duration: DEFAULT_VLOG_COMPACT_FULL_WRITE_COLD_DURATION,
            vlog_compact_throughput: DEFAULT_VLOG_COMPACT_THROUGHPUT,
            vlog_compact_throughput_burst: DEFAULT_VLOG_COMPACT_THROUGHPUT_BURST,
            vlog_compact_prioritize_lru_keys: false,
            vlog_compact_uncached_keys: true,

            trace_logging_enabled: false,
            // tsm_will_need: false,

            // wal_dir: String::new(),
            // wal_fsync_delay: Duration::from_secs(0),
            // validate_keys: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_default_contract() {
        let config = Config::default();

        assert_eq!(config.cache_max_memory_size, 1_073_741_824);
        assert_eq!(config.cache_snapshot_memory_size, 26_214_400);
        assert_eq!(
            config.cache_snapshot_write_cold_duration,
            Duration::from_secs(10 * 60)
        );
        assert_eq!(
            config.compact_full_write_cold_duration,
            Duration::from_secs(4 * 60 * 60)
        );
        assert_eq!(config.compact_throughput, 50_331_648);
        assert_eq!(config.compact_throughput_burst, 50_331_648);
        assert_eq!(config.max_concurrent_compactions, 0);
        assert_eq!(config.max_index_log_file_size, 1_048_576);
        assert_eq!(config.series_id_set_cache_size, 100);
        assert!(!config.validate_keys);
        assert_eq!(config.wal_fsync_delay, Duration::ZERO);
    }
}
