//! TSM engine.
use std::sync::Arc;

use crate::runtime::execution::{ExecutorRef, legacy_global_executor};
use limiter::rate::{self, Limit};

use crate::config::{
    Config, DEFAULT_COMPACT_THROUGHPUT, DEFAULT_COMPACT_THROUGHPUT_BURST, DEFAULT_ENGINE,
    DEFAULT_INDEX, DEFAULT_MAX_CONCURRENT_COMPACTIONS, DEFAULT_VLOG_COMPACT_THROUGHPUT,
    DEFAULT_VLOG_COMPACT_THROUGHPUT_BURST, DEFAULT_VLOG_MAX_CONCURRENT_COMPACTIONS,
    VLogConfig as EngineVLogConfig,
};

pub mod limiter;
pub mod tsm1;

pub use tsm1::engine::{CallbackSeriesIDSet, Error as EngineError, SeriesIDSets};
pub(crate) use tsm1::engine::{Engine as ActiveStorage, Error as ActiveStorageError};
pub(crate) use tsm1::iterator as active_cursor;

/// Storage-owned configuration used to construct an [`EngineOptions`].
///
/// Product configuration must be translated into these storage semantics by
/// the caller before entering the standalone storage boundary.
#[derive(Clone)]
pub struct StorageOptions {
    pub config: Config,
    pub vlog_config: EngineVLogConfig,
    pub vlog_enabled: bool,
    pub vlog_compact_plan: String,
}

impl Default for StorageOptions {
    fn default() -> Self {
        Self {
            config: Config::default(),
            vlog_config: EngineVLogConfig::default(),
            vlog_enabled: false,
            vlog_compact_plan: "default".to_string(),
        }
    }
}

/// Bounded execution resources for aggregate-index queries.
#[derive(Clone, Debug)]
pub struct AggregateQueryOptions {
    /// Shared per-query budget for planned reads, read-ahead and idle decode buffers.
    /// Zero disables both kinds of retention; active blocks and IO scratch are separate.
    pub read_cache_bytes: usize,
    /// Maximum speculative aligned read-ahead span. Zero disables speculative reads.
    pub read_ahead_bytes: usize,
    /// Maximum duplicate immutable VLog handles retained by one query.
    pub max_open_files: usize,
    /// Maximum additional query tasks admitted across this shard.
    pub parallel_tasks: usize,
    /// Minimum selected series count before requesting parallel execution.
    pub parallel_min_series: usize,
    /// Fixed-size worker result-state budget; larger queries execute serially.
    pub parallel_state_bytes: usize,
}

impl Default for AggregateQueryOptions {
    fn default() -> Self {
        Self { read_cache_bytes: 4 * 1024 * 1024, read_ahead_bytes: 0, max_open_files: 128, parallel_tasks: 0, parallel_min_series: 8, parallel_state_bytes: 8 * 1024 * 1024 }
    }
}

/// EngineOptions represents the options used to initialize the engine.
pub struct EngineOptions {
    /// Host-supplied execution capability used by this engine and its workers.
    pub executor: ExecutorRef,

    pub engine_version: String,
    pub index_version: String,
    pub shard_id: u64,

    // Limits the concurrent number of TSM files that can be loaded at once.
    pub open_limiter: limiter::Fixed,

    // compaction_disabled specifies shards should not schedule compactions.
    // This option is intended for offline tooling.
    pub compaction_disabled: bool,
    // compaction_planner_creator    CompactionPlannerCreator  // nil
    // compaction_limiter limits the number of level compaction tasks can be run at once.
    pub compaction_limiter: Option<Arc<limiter::Fixed>>,
    // compaction_throughput_limiter limits the bytes of writes to disk for snapshot and level compaction.
    pub compaction_throughput_limiter: Option<rate::Limiter>,
    pub wal_enabled: bool,
    pub monitor_disabled: bool, // false

    // vlog_enabled controls whether the vlog is enabled, if true, the encoded values which size if greater
    // than min_vlog_size will be written to the vlog when write snapshot.
    pub vlog_enabled: bool,
    /// Use immutable aggregate index files. Requires VLog mode.
    pub agg_index_enabled: bool,
    /// Shard-wide budget for aggregate index pages and segment headers; zero disables caching.
    pub agg_index_cache_bytes: usize,
    /// Query-local read-ahead and shard-wide query task limits.
    pub agg_query: AggregateQueryOptions,
    /// Maximum index input bytes per automatic aggregate compaction.
    pub agg_index_compaction_max_input_bytes: u64,
    /// Maximum target output size; smaller tiers use their total input size.
    pub agg_index_compaction_target_bytes: u64,
    // vlog_compaction_limiter limits the number of vlog compaction tasks can be run at once.
    pub vlog_compaction_limiter: Option<Arc<limiter::Fixed>>,
    // vlog_compaction_throughput_limiter limits the bytes of writes to disk for snapshot and vlog compaction.
    pub vlog_compaction_throughput_limiter: Option<rate::Limiter>,
    pub vlog_compact_plan: String,

    // // DatabaseFilter is a predicate controlling which databases may be opened.
    // // If no function is set, all databases will be opened.
    // DatabaseFilter func(database string) bool  // nil

    // // ShardFilter controls which database shard IDs may be opened.
    // // nil will allow all combinations to pass.
    // ShardFilter func(database string, id uint64) bool  // nil
    pub config: Config,
    pub vlog_config: EngineVLogConfig,

    pub series_id_sets: Option<Box<dyn SeriesIDSets + Sync + Send>>,
    // TODO:
    // FieldValidator FieldValidator  // defaultFieldValidator

    // FileStoreObserver FileStoreObserver    // nil
}

impl EngineOptions {
    /// The default compaction_limiter is num cpus, and default compaction_throughput_limiter is None
    /// indicates there is no compact limit.
    pub fn new(storage_options: StorageOptions) -> Self {
        Self::new_with_executor(storage_options, legacy_global_executor())
    }

    /// Builds runtime construction options without consulting the legacy global executor.
    pub fn new_with_executor(storage_options: StorageOptions, executor: ExecutorRef) -> Self {
        let StorageOptions {
            config,
            vlog_config: mut engine_vlog_config,
            vlog_enabled,
            vlog_compact_plan,
        } = storage_options;

        let cpus = num_cpus::get();
        if engine_vlog_config.max_wal_writers == 0 {
            engine_vlog_config.max_wal_writers = i32::max(cpus as i32 / 2, 1);
        }
        if engine_vlog_config.max_vlog_writers == 0 {
            engine_vlog_config.max_vlog_writers = 1;
        }
        if engine_vlog_config.vlog_compact_level_1_min_files == 0 {
            engine_vlog_config.vlog_compact_level_1_min_files = ((cpus as i64) / 2).clamp(8, 64);
        }
        if engine_vlog_config.vlog_compact_level_2_min_files == 0 {
            engine_vlog_config.vlog_compact_level_2_min_files = ((cpus as i64) / 4).clamp(4, 32);
        }
        if engine_vlog_config.vlog_compact_level_3_min_files == 0 {
            engine_vlog_config.vlog_compact_level_3_min_files = ((cpus as i64) / 8).clamp(4, 16);
        }
        if engine_vlog_config.vlog_compact_full_min_files == 0 {
            engine_vlog_config.vlog_compact_full_min_files = ((cpus as i64) * 4).clamp(64, 256);
        }

        let mut lim = config.max_concurrent_compactions;
        if lim == 0 {
            lim = num_cpus::get() as i64 / 2;
            if lim < 1 {
                lim = 1;
            }
        }
        // Don't allow more compactions to run than cores.
        if lim > num_cpus::get() as i64 {
            lim = num_cpus::get() as i64;
        }
        let compaction_limiter = Some(Arc::new(limiter::Fixed::new(lim as i32)));

        let mut compaction_throughput_limiter = None;
        let throughput = config.compact_throughput;
        let mut throughput_burst = config.compact_throughput_burst;
        if throughput_burst > 0 {
            if throughput_burst < throughput {
                throughput_burst = throughput;
            }
            compaction_throughput_limiter = Some(rate::Limiter::new(
                Limit(throughput as f64),
                throughput_burst,
            ));
        }

        let mut lim = engine_vlog_config.max_concurrent_compactions;
        if lim == 0 {
            lim = num_cpus::get() as i64 / 2;
            if lim < 1 {
                lim = 1;
            }
        }
        // Don't allow more compactions to run than cores.
        if lim > num_cpus::get() as i64 {
            lim = num_cpus::get() as i64;
        }
        let vlog_compaction_limiter = Some(Arc::new(limiter::Fixed::new(lim as i32)));

        let mut vlog_compaction_throughput_limiter = None;
        let throughput = engine_vlog_config.vlog_compact_throughput;
        let mut throughput_burst = engine_vlog_config.vlog_compact_throughput_burst;
        if throughput_burst > 0 {
            if throughput_burst < throughput {
                throughput_burst = throughput;
            }
            vlog_compaction_throughput_limiter = Some(rate::Limiter::new(
                Limit(throughput as f64),
                throughput_burst,
            ));
        }
        Self {
            executor,
            engine_version: DEFAULT_ENGINE.to_string(),
            index_version: DEFAULT_INDEX.to_string(),
            shard_id: 0,

            open_limiter: limiter::Fixed::new(num_cpus::get() as i32),
            compaction_disabled: false,
            compaction_limiter,
            compaction_throughput_limiter,

            wal_enabled: config.wal_enabled,
            monitor_disabled: false,
            vlog_enabled,
            agg_index_enabled: false,
            agg_index_cache_bytes: 32 * 1024 * 1024,
            agg_query: AggregateQueryOptions::default(),
            agg_index_compaction_max_input_bytes: 8 * 1024 * 1024 * 1024,
            agg_index_compaction_target_bytes: 1024 * 1024 * 1024,
            vlog_compaction_limiter,
            vlog_compaction_throughput_limiter,
            vlog_compact_plan,

            config,
            vlog_config: engine_vlog_config,

            series_id_sets: None,
        }
    }
}

impl Default for EngineOptions {
    fn default() -> Self {
        let config = Config::default();
        let engine_vlog_config = EngineVLogConfig::default();
        let mut lim = DEFAULT_MAX_CONCURRENT_COMPACTIONS;
        if lim == 0 {
            lim = num_cpus::get() as i64 / 2;
            if lim < 1 {
                lim = 1;
            }
        }
        // Don't allow more compactions to run than cores.
        if lim > num_cpus::get() as i64 {
            lim = num_cpus::get() as i64;
        }
        let compaction_limiter = Some(Arc::new(limiter::Fixed::new(lim as i32)));

        let mut compaction_throughput_limiter = None;
        let throughput = DEFAULT_COMPACT_THROUGHPUT;
        let mut throughput_burst = DEFAULT_COMPACT_THROUGHPUT_BURST;
        if throughput_burst > 0 {
            if throughput_burst < throughput {
                throughput_burst = throughput;
            }
            compaction_throughput_limiter = Some(rate::Limiter::new(
                Limit(throughput as f64),
                throughput_burst,
            ));
        }

        let mut lim = DEFAULT_VLOG_MAX_CONCURRENT_COMPACTIONS;
        if lim == 0 {
            lim = num_cpus::get() as i64 / 2;
            if lim < 1 {
                lim = 1;
            }
        }
        // Don't allow more compactions to run than cores.
        if lim > num_cpus::get() as i64 {
            lim = num_cpus::get() as i64;
        }
        let vlog_compaction_limiter = Some(Arc::new(limiter::Fixed::new(lim as i32)));

        let mut vlog_compaction_throughput_limiter = None;
        let throughput = DEFAULT_VLOG_COMPACT_THROUGHPUT;
        let mut throughput_burst = DEFAULT_VLOG_COMPACT_THROUGHPUT_BURST;
        if throughput_burst > 0 {
            if throughput_burst < throughput {
                throughput_burst = throughput;
            }
            vlog_compaction_throughput_limiter = Some(rate::Limiter::new(
                Limit(throughput as f64),
                throughput_burst,
            ));
        }
        Self {
            executor: legacy_global_executor(),
            engine_version: DEFAULT_ENGINE.to_string(),
            index_version: DEFAULT_INDEX.to_string(),
            shard_id: 0,

            open_limiter: limiter::Fixed::new(num_cpus::get() as i32),
            compaction_disabled: false,
            compaction_limiter,
            compaction_throughput_limiter,

            wal_enabled: true,
            monitor_disabled: false,
            vlog_enabled: false,
            agg_index_enabled: false,
            agg_index_cache_bytes: 32 * 1024 * 1024,
            agg_query: AggregateQueryOptions::default(),
            agg_index_compaction_max_input_bytes: 8 * 1024 * 1024 * 1024,
            agg_index_compaction_target_bytes: 1024 * 1024 * 1024,
            vlog_compaction_limiter,
            vlog_compaction_throughput_limiter,
            vlog_compact_plan: "default".to_string(),

            config,
            vlog_config: engine_vlog_config,

            series_id_sets: None,
        }
    }
}

impl Clone for EngineOptions {
    fn clone(&self) -> Self {
        let series_id_sets = self.series_id_sets.as_ref().map(|ids| ids.clone_self());

        Self {
            executor: Arc::clone(&self.executor),
            engine_version: self.engine_version.clone(),
            index_version: self.index_version.clone(),
            shard_id: self.shard_id,

            open_limiter: self.open_limiter.clone(),

            compaction_disabled: self.compaction_disabled,
            compaction_limiter: self.compaction_limiter.clone(),
            compaction_throughput_limiter: self.compaction_throughput_limiter.clone(),

            wal_enabled: self.wal_enabled,

            vlog_enabled: self.vlog_enabled,
            agg_index_enabled: self.agg_index_enabled,
            agg_index_cache_bytes: self.agg_index_cache_bytes,
            agg_query: self.agg_query.clone(),
            agg_index_compaction_max_input_bytes: self.agg_index_compaction_max_input_bytes,
            agg_index_compaction_target_bytes: self.agg_index_compaction_target_bytes,
            vlog_compaction_limiter: self.vlog_compaction_limiter.clone(),
            vlog_compaction_throughput_limiter: self.vlog_compaction_throughput_limiter.clone(),
            vlog_compact_plan: self.vlog_compact_plan.clone(),

            monitor_disabled: self.monitor_disabled,

            config: self.config.clone(),
            vlog_config: self.vlog_config.clone(),

            series_id_sets,
        }
    }
}

/// VLogOptions represents the options used to initialize the vlog.
pub struct VLogOptions {
    pub engine_version: String,
    pub shard_id: u64,

    // Limits the concurrent number of TSM files that can be loaded at once.
    pub open_limiter: limiter::Fixed,

    // compaction_disabled specifies shards should not schedule compactions.
    // This option is intended for offline tooling.
    pub compaction_disabled: bool,
    // compaction_planner_creator    CompactionPlannerCreator  // nil
    // compaction_limiter limits the number of level compaction tasks can be run at once.
    pub compaction_limiter: Option<Arc<limiter::Fixed>>,
    // compaction_throughput_limiter limits the bytes of writes to disk for snapshot and level compaction.
    pub compaction_throughput_limiter: Option<rate::Limiter>,
    pub monitor_disabled: bool, // false

    // // DatabaseFilter is a predicate controlling which databases may be opened.
    // // If no function is set, all databases will be opened.
    // DatabaseFilter func(database string) bool  // nil

    // // ShardFilter controls which database shard IDs may be opened.
    // // nil will allow all combinations to pass.
    // ShardFilter func(database string, id uint64) bool  // nil
    pub config: EngineVLogConfig,
    // series_id_sets: Option<Box<dyn SeriesIDSets + Sync + Send>>,
    // TODO:
    // FieldValidator FieldValidator  // defaultFieldValidator

    // OnNewEngine func(Engine)  // nil

    // FileStoreObserver FileStoreObserver    // nil
}

impl VLogOptions {
    /// The default compaction_limiter is num cpus, and default compaction_throughput_limiter is None
    /// indicates there is no compact lmit.
    pub fn new(
        max_concurrent_compactions: i64,
        compact_throughput: i64,
        compact_throughput_burst: i64,
    ) -> Self {
        let mut lim = max_concurrent_compactions;
        if lim == 0 {
            lim = num_cpus::get() as i64 / 2;
            if lim < 1 {
                lim = 1;
            }
        }
        // Don't allow more compactions to run than cores.
        if lim > num_cpus::get() as i64 {
            lim = num_cpus::get() as i64;
        }

        let compaction_limiter = Some(Arc::new(limiter::Fixed::new(lim as i32)));

        let mut compaction_throughput_limiter = None;
        let throughput = compact_throughput;
        let mut throughput_burst = compact_throughput_burst;
        if throughput_burst > 0 {
            if throughput_burst < throughput {
                throughput_burst = throughput;
            }
            compaction_throughput_limiter = Some(rate::Limiter::new(
                Limit(throughput as f64),
                throughput_burst,
            ));
        }

        Self {
            engine_version: DEFAULT_ENGINE.to_string(),
            shard_id: 0,

            open_limiter: limiter::Fixed::new(num_cpus::get() as i32),
            compaction_disabled: false,
            compaction_limiter,
            compaction_throughput_limiter,

            monitor_disabled: false,

            config: EngineVLogConfig::default(),
        }
    }
}

impl Default for VLogOptions {
    fn default() -> Self {
        Self::new(
            DEFAULT_VLOG_MAX_CONCURRENT_COMPACTIONS,
            DEFAULT_VLOG_COMPACT_THROUGHPUT,
            DEFAULT_VLOG_COMPACT_THROUGHPUT_BURST,
        )
    }
}

impl Clone for VLogOptions {
    fn clone(&self) -> Self {
        Self {
            engine_version: self.engine_version.clone(),
            shard_id: self.shard_id,

            open_limiter: self.open_limiter.clone(),

            compaction_disabled: self.compaction_disabled,
            compaction_limiter: self.compaction_limiter.clone(),
            compaction_throughput_limiter: self.compaction_throughput_limiter.clone(),
            monitor_disabled: self.monitor_disabled,

            config: self.config.clone(),
        }
    }
}
