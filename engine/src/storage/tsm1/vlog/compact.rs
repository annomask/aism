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
    collections::{BTreeMap, HashMap, HashSet},
    default,
    fs::{self, File},
    io::Write,
    ops::{Deref, DerefMut},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicU64},
    time::Duration,
};

#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::common::{hll::hash64, sync::CancellationTokenBounded, time::Time, to_str, to_string};
use crate::models::points::{
    BOOL, EMPTY, FLOAT, FloatValue, INTEGER, STRING, UNSIGNED, Val, Value,
};
use crate::runtime::execution::{
    ExecutorRef, RuntimeExecutorExt, SpawnError, TaskAdmissionGate, TaskHandle, TaskJoinError,
    legacy_global_executor,
};
use parking_lot::{Mutex, RwLock};
use scopeguard::defer;
use snafu::{ResultExt, Snafu, ensure};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::{
    default_compact_plan::VLogDefaultPlanner,
    file_store::{
        self, FileStore, TMP_VLOG_FILE_EXTENSION, default_format_file_name, default_parse_file_name,
    },
    lazy_compact_plan::VLogLazyPlanner,
    reader::{self, BlockIter, TimeRange, VLogReader},
    writer::{self, VLogWriter},
};
use crate::{
    config::{DEFAULT_MAX_POINTS_PER_BLOCK, VLogConfig},
    storage::{
        limiter::{LimitWriter, SyncWrite, rate::Limiter},
        tsm1::{
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
            vlog::{self, VLogIndex},
        },
    },
};

const DEFAULT_VLOG_WRITER_NUM: i32 = 1;
pub const MAX_TSM_FILE_SIZE: u32 = 2048 * 1024 * 1024; // 2GB
pub const MAX_VLOG_FILE_SIZE: i64 = MAX_TSM_FILE_SIZE as i64;

/// COMPACTION_TEMP_EXTENSION is the extension used for temporary files created during compaction.
pub const COMPACTION_TEMP_EXTENSION: &str = "tmp";
/// VLOG_FILE_EXTENSION is the extension used for TSM files.
pub const VLOG_FILE_EXTENSION: &str = "vlog";

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

    /// VLog reader error.
    #[snafu(display("{}", source))]
    Reader { source: reader::Error },

    /// VLog Writer error.
    #[snafu(display("{}", source))]
    Writer { source: writer::Error },

    /// File store error.
    #[snafu(display("{}", source))]
    FileStore { source: file_store::Error },

    /// String error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },

    /// Required VLog compaction work could not be scheduled.
    #[snafu(display("{}", source))]
    Execution { source: SpawnError },

    /// An accepted VLog read task did not publish its result.
    #[snafu(display("VLog read task terminated: {}", source))]
    ReadTaskTerminated { source: TaskJoinError },
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
pub struct CompactionGroup(pub Vec<String>);

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

// pub trait VLogCompactPlan {
//     fn full_compacted(&self) -> bool;
//     fn force_full(&self);
//     // Default planner
//     fn plan_level(&self, level: i64) -> Vec<CompactionGroup>;
//     fn plan_optimize(&self) -> Vec<CompactionGroup>;
//     fn plan(&self, last_write: Time) -> Vec<CompactionGroup>;

//     // Lazy planner
//     fn plan_level(&self, level: i64) -> VLogCompactInfo;
//     fn plan_optimize(&self) -> VLogCompactInfo;
//     fn plan(&self, last_write: Time) -> VLogCompactInfo;

//     fn release(&self, groups: &[CompactionGroup]);
// }

#[derive(Clone)]
pub enum VLogCompactPlanner {
    VLogDefaultPlanner(Arc<VLogDefaultPlanner>),
    VLogLazyPlanner(Arc<VLogLazyPlanner>),
}

/// Returns (generation, seq)
pub fn parse_file_name(path: impl AsRef<Path>) -> Result<(i64, i64)> {
    default_parse_file_name(path).context(FileStoreSnafu)
}

/// Compactor merges multiple TSM files into new files or
/// writes a Cache into 1 or more TSM files.
pub struct Compactor {
    // dir: Arc<PathBuf>,
    size: i64,
    // file_store: Arc<FileStore>,
    // // RateLimit is the limit for disk writes for all concurrent compactions.
    // rate_limit: Option<Limiter>,
    vlog_dir: Arc<PathBuf>,
    // RateLimit is the limit for disk writes for all concurrent compactions.
    vlog_rate_limit: Option<Limiter>,
    max_vlog_writers: i32,

    inner: Arc<RwLock<InnerCompactor>>,
    executor: ExecutorRef,
    #[cfg(test)]
    read_test_hook: RwLock<Option<Arc<VLogReadTestHook>>>,
}

struct ActiveVLogCompaction {
    compactor: Arc<Compactor>,
    files: Vec<String>,
    readers: Vec<Arc<VLogReader>>,
}

struct VLogReadLease(Arc<VLogReader>);

impl VLogReadLease {
    fn new(reader: Arc<VLogReader>) -> Self {
        reader.ref_reader();
        Self(reader)
    }
}

impl Drop for VLogReadLease {
    fn drop(&mut self) {
        self.0.unref_reader();
    }
}

impl Drop for ActiveVLogCompaction {
    fn drop(&mut self) {
        for reader in &self.readers {
            reader.unref_reader();
        }
        self.compactor.remove_vlog(&self.files);
    }
}

pub(crate) struct PreparedVLogCompaction {
    _active: ActiveVLogCompaction,
    iter: VLogBatchKeyIter,
    max_generation: i64,
    max_sequence: i64,
    is_lazy_compact: bool,
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct VLogReadTestHook {
    panic_once: AtomicBool,
    delay_millis: AtomicU64,
    started: AtomicUsize,
    active: AtomicUsize,
    max_active: AtomicUsize,
}

#[cfg(test)]
impl VLogReadTestHook {
    fn enter(self: &Arc<Self>) -> VLogReadTestGuard {
        self.started.fetch_add(1, Ordering::SeqCst);
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        let guard = VLogReadTestGuard(Arc::clone(self));

        if self.panic_once.swap(false, Ordering::SeqCst) {
            panic!("injected VLog read child production-body panic");
        }
        let delay = self.delay_millis.load(Ordering::SeqCst);
        if delay != 0 {
            std::thread::sleep(Duration::from_millis(delay));
        }
        guard
    }
}

#[cfg(test)]
struct VLogReadTestGuard(Arc<VLogReadTestHook>);

#[cfg(test)]
impl Drop for VLogReadTestGuard {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}

struct InnerCompactor {
    // snapshots_enabled: bool,
    // compactions_enabled: bool,
    vlog_compactions_enabled: bool,

    // The following parameters are vlog associated
    min_vlog_size: i64,
    vlog_writers: Vec<Arc<RwLock<VLogWriter<File>>>>,
    vlog_file_store: Option<Arc<FileStore>>,

    // the amount of time the last snapshot took to complete.
    last_snapshot_duration: Duration,

    snapshot_latencies: Latencies,

    // // to signal that any in progress snapshots should be aborted.
    // snapshot_interrupt: Option<CancellationToken>,
    // // to signal that any in progress level compactions should be aborted.
    // compactions_interrupt: Option<CancellationToken>,
    // to signal that any in progress level compactions should be aborted.
    vlog_compactions_interrupt: Option<CancellationToken>,

    // files: HashSet<String>,
    vlog_files: HashSet<String>,
}

impl Compactor {
    pub fn new(
        // dir: Arc<PathBuf>,
        vlog_dir: Arc<PathBuf>,
        // file_store: Arc<FileStore>,
        vlog_file_store: Option<Arc<FileStore>>,
        // rate_limit: Option<Limiter>,
        vlog_rate_limit: Option<Limiter>,
        // min_vlog_size: i64,
        vlog_config: &VLogConfig,
    ) -> Self {
        Self::new_with_executor(
            vlog_dir,
            vlog_file_store,
            vlog_rate_limit,
            vlog_config,
            legacy_global_executor(),
        )
    }

    pub fn new_with_executor(
        vlog_dir: Arc<PathBuf>,
        vlog_file_store: Option<Arc<FileStore>>,
        vlog_rate_limit: Option<Limiter>,
        vlog_config: &VLogConfig,
        executor: ExecutorRef,
    ) -> Self {
        Self {
            // dir,
            size: 0,
            // file_store,
            // rate_limit,
            vlog_dir,
            vlog_rate_limit,
            max_vlog_writers: vlog_config.max_vlog_writers,

            inner: Arc::new(RwLock::new(InnerCompactor {
                // snapshots_enabled: false,
                // compactions_enabled: false,
                vlog_compactions_enabled: false,

                min_vlog_size: vlog_config.min_vlog_size,
                vlog_writers: vec![],

                vlog_file_store,

                last_snapshot_duration: Duration::from_secs(0),

                snapshot_latencies: Latencies::new(4),
                vlog_compactions_interrupt: Some(CancellationToken::new()),

                vlog_files: HashSet::<String>::new(),
            })),
            executor,
            #[cfg(test)]
            read_test_hook: RwLock::new(None),
        }
    }

    #[cfg(test)]
    pub(crate) fn set_read_test_hook(&self, hook: Option<Arc<VLogReadTestHook>>) {
        *self.read_test_hook.write() = hook;
    }

    /// Open initializes the Compactor.
    pub fn open(&self) {
        let mut inner = self.inner.write();

        let max_vlog_writes = if self.max_vlog_writers > 0 {
            self.max_vlog_writers
        } else {
            DEFAULT_VLOG_WRITER_NUM
        };

        let mut vlog_writers = vec![];
        for _ in 0..max_vlog_writes {
            let generation = inner.vlog_file_store.as_ref().unwrap().next_generation();
            vlog_writers.push(Self::open_vlog_writer(self.vlog_dir.as_path(), generation));
        }

        inner.vlog_writers = vlog_writers;

        inner.snapshot_latencies = Latencies::new(4);

        inner.vlog_files = HashSet::new();
    }

    fn open_vlog_writer(path: &Path, generation: i64) -> Arc<RwLock<VLogWriter<File>>> {
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

        Arc::new(RwLock::new(VLogWriter::new(fd, generation, 1, file_name)))
    }

    /// Close disables the Compactor.
    pub fn close(&self) {
        let mut inner = self.inner.write();
        if !inner.vlog_compactions_enabled {
            // remove all empty vlog writers.
            for w in inner.vlog_writers.iter() {
                let mut w = w.write();
                if w.size() == 0 {
                    let _ = w.remove();
                }
            }
            return;
        }

        inner.vlog_compactions_enabled = false;

        if let Some(vlog_compactions_interrupt) = &inner.vlog_compactions_interrupt {
            vlog_compactions_interrupt.cancel(); // stop snapshots in preogressing
        }
    }

    /// Disables the compactor from performing compactions.
    pub fn disable_vlog_compactions(&self) {
        let mut inner = self.inner.write();
        inner.vlog_compactions_enabled = false;
        if inner.vlog_compactions_interrupt.is_some() {
            inner.vlog_compactions_interrupt.as_ref().unwrap().cancel(); // stop compactions in preogressing
            inner.vlog_compactions_interrupt = None;
        }
    }

    /// Allows the compactor to perform compactions.
    pub fn enable_vlog_compactions(&self) {
        let mut inner = self.inner.write();
        inner.vlog_compactions_enabled = true;

        if inner.vlog_compactions_interrupt.is_none() {
            inner.vlog_compactions_interrupt = Some(CancellationToken::new());
        }
    }

    /// Returns the all VLogReaders for the file paths if one is currently managed by the FileStore.
    /// It also returns all sorted keys for the files.
    pub fn vlog_readers_and_keys(
        &self,
        tsm_files: &[String],
    ) -> Result<(Vec<Arc<VLogReader>>, Vec<(Vec<u8>, u8)>)> {
        let inner = self.inner.read();
        let mut trs: Vec<Arc<VLogReader>> = vec![];
        let mut unique_keys = HashMap::new();
        for file in tsm_files {
            if let Some(intc) = &inner.vlog_compactions_interrupt {
                // select! {
                //     recv(intc.receiver()) -> _ => return Err(Error::CompactionAborted { reason: String::new() }),
                //     default => {},
                // }
                if intc.is_cancelled() {
                    // for tr in &trs {
                    //     tr.unref_reader();
                    // }
                    return Err(Error::CompactionAborted {
                        reason: String::new(),
                    });
                }
            }

            let tr = match inner
                .vlog_file_store
                .as_ref()
                .unwrap()
                .vlog_reader_unref(file)
            {
                Some(r) => r,
                None => {
                    // for tr in &trs {
                    //     tr.unref_reader();
                    // }
                    return Err(Error::CompactionAborted {
                        reason: format!("compact vlog bad plan: {:?}", file),
                    });
                }
            };

            trs.push(tr);

            let keys = inner.vlog_file_store.as_ref().unwrap().file_keys(file);
            for (key, typ) in keys {
                unique_keys.entry(key).or_insert(typ);
            }
        }
        drop(inner);

        if trs.is_empty() {
            return Ok((vec![], vec![]));
        }

        let mut keys = vec![];
        for (k, typ) in unique_keys {
            keys.push((k, typ));
        }
        keys.sort_by(|a, b| a.0.cmp(&b.0));

        Ok((trs, keys))
    }

    pub(crate) fn executor(&self) -> ExecutorRef {
        Arc::clone(&self.executor)
    }

    /// Acquires source-file ownership and collects required read results while
    /// the compact-async caller remains outside compact-blocking capacity.
    pub(crate) async fn prepare_vlog_compaction(
        self: &Arc<Self>,
        fast: bool,
        tsm_files: Vec<String>,
        vlog_compact_info: VLogCompactInfo,
        is_lazy_compact: bool,
    ) -> Result<PreparedVLogCompaction> {
        if !self.inner.read().vlog_compactions_enabled {
            return Err(Error::CompactionsDisabled);
        }

        if !self.add_vlog(&tsm_files) {
            return Err(Error::CompactionInProgress {
                reason: String::new(),
            });
        }

        let readers = vlog_compact_info.readers.clone();
        for reader in &readers {
            reader.ref_reader();
        }
        let active = ActiveVLogCompaction {
            compactor: Arc::clone(self),
            files: tsm_files,
            readers,
        };

        let mut size = self.size;
        if size <= 0 {
            size = DEFAULT_MAX_POINTS_PER_BLOCK;
        }

        let intc = self.inner.read().vlog_compactions_interrupt.clone();

        // FIXME: 当 lazy compact时，[1-1.tsm.cmp, 2-1.tms.cmp, 4-1.tsm.cmp] 可能被第二次、第三次comapct，那么，第二次compact时，max_gen=4, max_seq=1,改成compact产生新文件：4-1.tsm；第三次compact时，max_gen=4, max_seq=1,改成compact产生新文件：4-1.tsm。这两次comapct生成的文件名重复，这里需要改正。
        // FIXME:
        // FIXME:

        // The new compacted files need to added to the max generation in the
        // set.  We need to find that max generation as well as the max sequence
        // number to ensure we write to the next unique location.
        let (mut max_generation, mut max_sequence) = (0, 0);
        if is_lazy_compact {
            for f in &active.files {
                let (_gen, seq) = default_parse_file_name(f)?;
                if seq > max_sequence {
                    max_sequence = seq;
                }
            }
        } else {
            for f in &active.files {
                let (generation, seq) = default_parse_file_name(f)?;
                if generation > max_generation {
                    max_generation = generation;
                    max_sequence = seq;
                }

                if generation == max_generation && seq > max_sequence {
                    max_sequence = seq;
                }
            }
        }

        #[cfg(test)]
        let read_test_hook = { self.read_test_hook.read().clone() };
        let iter = VLogBatchKeyIter::new_with_executor(
            size,
            fast,
            intc,
            vlog_compact_info,
            Arc::clone(&self.executor),
            #[cfg(test)]
            read_test_hook,
        )
        .await?;

        Ok(PreparedVLogCompaction {
            _active: active,
            iter,
            max_generation,
            max_sequence,
            is_lazy_compact,
        })
    }

    /// Runs the blocking merge/write stage after every required read child has
    /// reached a terminal state.
    pub(crate) fn finish_vlog_compaction(
        &self,
        prepared: PreparedVLogCompaction,
    ) -> Result<(Vec<String>, HashMap<String, Vec<VLogIndex>>)> {
        let (files, indexes, _) = self.finish_vlog_compaction_with_aggregates(prepared)?;
        Ok((files, indexes))
    }

    pub(crate) fn finish_vlog_compaction_with_aggregates(
        &self,
        mut prepared: PreparedVLogCompaction,
    ) -> Result<(Vec<String>, HashMap<String, Vec<VLogIndex>>, AggregateOutputs)> {
        let files = &prepared._active.files;

        let inner = Arc::clone(&self.inner);
        let (vlog_files, vlog_indexes) = Compactor::write_new_files(
            inner,
            Arc::clone(&self.vlog_dir),
            prepared.max_generation,
            prepared.max_sequence,
            self.vlog_rate_limit.clone(),
            files,
            &mut prepared.iter,
            true,
            false,
            true,
            prepared.is_lazy_compact,
        )?;

        let enabled = self.inner.read().vlog_compactions_enabled;
        if !enabled {
            self.remove_tmp_files(&vlog_files)?;
            return Err(Error::CompactionsDisabled);
        }

        Ok((vlog_files, vlog_indexes, std::mem::take(&mut prepared.iter.output_aggregates)))
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
        vlog_dir: Arc<PathBuf>,
        mut generation: i64,
        mut sequence: i64,
        rate_limit: Option<Limiter>,
        _src: &[String],
        iter: &mut impl KeyIterator,
        throttle: bool,
        is_write_snapshot: bool,
        is_compact_vlog: bool,
        is_lazy_compact: bool,
    ) -> Result<(Vec<String>, HashMap<String, Vec<VLogIndex>>)> {
        /*
            // 1. vlog_enabled && write_snapshot
            (tsm_files, Vec<old_vlog_file>, HashMap<key->vlog_indexes>)

            // 2. vlog_disabled && write_snapshot
            (tsm_files, HashMap::new())

            // 3. compact tsm
            (tsm_files, HashMap::new())

            // 4. compact vlog
            (vec![], Vec<old_vlog_file>, HashMap<key->vlog_indexes>)
        */

        // These are the new TSM files written
        let mut new_vlog_files = vec![];
        let mut new_vlog_indexes = HashMap::<String, Vec<VLogIndex>>::new();

        let mut first = true;
        loop {
            if is_lazy_compact {
                if first {
                    sequence += 1;
                    first = false;
                }
                generation = inner
                    .read()
                    .vlog_file_store
                    .as_ref()
                    .unwrap()
                    .next_generation();
            } else {
                sequence += 1;
            };

            // New TSM files are written to a temp file and renamed when fully completed.
            let file_name = vlog_dir.join(
                default_format_file_name(generation, sequence)
                    + "."
                    + VLOG_FILE_EXTENSION
                    + "."
                    + TMP_VLOG_FILE_EXTENSION,
            );

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
                is_compact_vlog,
            ) {
                Ok(WriteFileRes {
                    write_vlog_res,
                    mut vlog_files,
                    vlog_indexes,
                }) => {
                    match write_vlog_res {
                        Ok(_) => {
                            // Deleting every input value produces no locators.
                            // Retire the inputs without installing an empty VLog.
                            if vlog_indexes.is_empty() {
                                fs::remove_file(&file_name)?;
                                break;
                            }
                            // compact vlog
                            new_vlog_files.push(file_name.to_str().unwrap().to_string());
                            for (key, mut indexes) in vlog_indexes {
                                new_vlog_indexes
                                    .entry(key)
                                    .or_insert_with(Vec::new)
                                    .append(&mut indexes);
                            }
                            break;
                        }
                        Err(e) => {
                            match e {
                                Error::MaxFileExceed => {
                                    new_vlog_files.push(file_name.to_str().unwrap().to_string());
                                    for (key, mut indexes) in vlog_indexes {
                                        new_vlog_indexes
                                            .entry(key)
                                            .or_insert_with(Vec::new)
                                            .append(&mut indexes);
                                    }
                                    continue;
                                }
                                Error::Writer {
                                    source: writer::Error::MaxBlocksExceeded { offset, size },
                                } => {
                                    // FIXME:
                                    // FIXME:
                                    // FIXME:
                                    // FIXME:
                                    // FIXME:
                                    // FIXME:
                                    // FIXME:
                                    // FIXME:
                                    // FIXME: write success, continue to next tsm file.
                                    // if is_compact_vlog {
                                    new_vlog_files.push(file_name.to_str().unwrap().to_string());

                                    for (key, mut indexes) in vlog_indexes {
                                        new_vlog_indexes
                                            .entry(key)
                                            .or_insert_with(Vec::new)
                                            .append(&mut indexes);
                                    }
                                    continue;
                                    // }
                                }
                                Error::Writer {
                                    source: writer::Error::NoValues,
                                } => {
                                    // write vlog success, break

                                    // If the file only contained tombstoned entries, then it
                                    // would be a 0 length file that we can drop.
                                    if file_name.exists() {
                                        fs::remove_file(&file_name)?;
                                    }
                                    // FIXME:
                                    // 对于 /path/data/下面的tsm文件，如果这个文件 NoValue，这里会remove
                                    // 这个file(gen_seq.tsm)，那么下次再次写时fileName=generation+1_seq.tsm
                                    // 按道理应该还是gen_seq.tsm才对？
                                    new_vlog_files.push(file_name.to_str().unwrap().to_string());
                                    for (key, mut indexes) in vlog_indexes {
                                        new_vlog_indexes
                                            .entry(key)
                                            .or_default()
                                            .append(&mut indexes);
                                    }
                                    break;
                                }
                                Error::CompactionInProgress { reason: _ } => {
                                    // Don't clean up the file as another compaction is using it.
                                    // This should not happen as the planner keeps track of which
                                    // files are assigned to compaction plans now.
                                    return Ok((vec![], HashMap::new()));
                                }
                                _ => {
                                    new_vlog_files.append(&mut vlog_files);
                                    // Remove any tmp files we already completed
                                    for f in new_vlog_files {
                                        fs::remove_file(&f)?;
                                    }
                                    // We hit an error and didn't finish the compaction.  Remove the temp file and abort.
                                    fs::remove_file(file_name)?;

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

        Ok((new_vlog_files, new_vlog_indexes))
    }

    fn add_vlog(&self, files: &[String]) -> bool {
        let mut inner = self.inner.write();
        // See if the new files are already in use
        for f in files {
            if inner.vlog_files.contains(f) {
                return false;
            }
        }

        // Mark all the new files in use
        for f in files {
            inner.vlog_files.insert(f.clone());
        }

        true
    }

    fn remove_vlog(&self, files: &[String]) {
        let mut inner = self.inner.write();
        for f in files {
            let _ = inner.vlog_files.remove(f);
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
        is_compact_vlog: bool,
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
            let mut w = VLogWriter::new(limiter_writer, generation, seq, filename);

            Self::write_file(
                vlog_dir,
                inner,
                &mut w,
                iter,
                is_write_snapshot,
                is_compact_vlog,
            )
        } else {
            let mut w = VLogWriter::new(fd, generation, seq, filename);

            Self::write_file(
                vlog_dir,
                inner,
                &mut w,
                iter,
                is_write_snapshot,
                is_compact_vlog,
            )
        }
    }

    fn write_file(
        vlog_dir: Arc<PathBuf>,
        inner: Arc<RwLock<InnerCompactor>>,
        w: &mut VLogWriter<impl SyncWrite>,
        iter: &mut impl KeyIterator,
        is_write_snapshot: bool,
        is_compact_vlog: bool,
    ) -> Result<WriteFileRes> {
        // return vlog file names
        let err = Ok(());
        let mut vlog_files = vec![];
        let mut vlog_indexes = HashMap::<String, Vec<VLogIndex>>::new();
        // chose min_seris_key in the blocks in tsm files which should be compacted, then merge
        // min_series_key's values, write to new tsm file, if the values are too large to write to only
        // one tsm file, then write them to the next level.
        // Each call to read returns the next sorted key (or the prior one if there are
        // more values to write).  The size of values will be less than or equal to our
        // chunk size (4000)
        for (count, typ, key, min_time, max_time, block, iter_err) in iter.by_ref() {
            let enabled = inner.read().vlog_compactions_enabled;

            if !enabled {
                let _ = close_vlog_file(w, err);
                return Ok(WriteFileRes {
                    write_vlog_res: Err(Error::CompactionAborted {
                        reason: String::new(),
                    }),
                    vlog_files,
                    ..Default::default()
                });
            }

            if let Some(e) = iter_err {
                return Ok(WriteFileRes {
                    write_vlog_res: Err(close_vlog_file(w, Err(Error::StringErr { reason: e }))
                        .err()
                        .unwrap()),
                    vlog_files,
                    ..Default::default()
                });
            }

            if min_time > max_time {
                let _ = close_vlog_file(w, Ok(()));
                return Ok(WriteFileRes {
                    write_vlog_res: Err(Error::StringErr {
                        reason: format!(
                            "invalid vlog entry for block. min={}, max={}",
                            min_time, max_time
                        ),
                    }),
                    vlog_files,
                    ..Default::default()
                });
            }

            // Write the key and value
            match w.write_block(&key, min_time, max_time, &block) {
                Ok((offset, size)) => {
                    // if is_compact_vlog {
                    let (generation, seq) = w.get_id();
                    let vlog_index = VLogIndex {
                        aggregate: None,
                        count,
                        typ,
                        min: min_time,
                        max: max_time,
                        generation,
                        seq,
                        offset,
                        size,
                    };

                    vlog_indexes
                        .entry(to_string!(key.to_vec()))
                        .or_default()
                        .push(vlog_index);
                    // }
                }
                Err(e) => {
                    match e {
                        writer::Error::MaxBlocksExceeded { offset, size } => {
                            // compact vlog
                            let (generation, seq) = w.get_id();
                            let vlog_index = VLogIndex {
                                aggregate: None,
                                count,
                                typ,
                                min: min_time,
                                max: max_time,
                                generation,
                                seq,
                                offset,
                                size,
                            };

                            vlog_indexes
                                .entry(to_string!(key.to_vec()))
                                .or_default()
                                .push(vlog_index);
                            return Ok(WriteFileRes {
                                write_vlog_res: Err(close_vlog_file(
                                    w,
                                    Err(Error::Writer { source: e }),
                                )
                                .err()
                                .unwrap()),
                                vlog_indexes,
                                ..Default::default()
                            });
                        }
                        _ => {
                            let mut res = WriteFileRes::default();
                            if is_compact_vlog {
                                res.write_vlog_res =
                                    Err(close_file(w, Err(Error::Writer { source: e }))
                                        .err()
                                        .unwrap());
                                res.vlog_files = vlog_files;
                                res.vlog_indexes = vlog_indexes;
                            } else {
                                res.write_vlog_res =
                                    Err(close_vlog_file(w, Err(Error::Writer { source: e }))
                                        .err()
                                        .unwrap());
                            }
                            return Ok(res);
                        }
                    }
                }
            };

            // If we have a max file size configured and we're over it, close out the file
            // and return the error.
            if w.size() > MAX_TSM_FILE_SIZE {
                let mut res = WriteFileRes::default();
                res.write_vlog_res = Err(Error::MaxFileExceed);
                res.vlog_files = vlog_files;
                res.vlog_indexes = vlog_indexes;
                return Ok(res);
            }
        } // while let

        // Were there any errors encountered during iteration?
        if let Some(e) = iter.err() {
            return Ok(WriteFileRes {
                write_vlog_res: Err(Error::StringErr { reason: e }),
                vlog_files,
                ..Default::default()
            });
        }

        let mut res = WriteFileRes {
            vlog_indexes,
            ..Default::default()
        };

        Ok(res)
    }

    fn close_and_open_new_vlog_writer(
        mut w: &mut VLogWriter<impl SyncWrite>,
        generation: i64,
        seq: i64,
        path: &Path,
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

        let file_name = path.join(
            default_format_file_name(generation, seq + 1)
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
            VLogWriter::new(fd, generation, seq + 1, file_name),
        ))
    }
}

#[derive(Default, Clone)]
pub struct VLogCompactInfo {
    pub keys: Vec<(Vec<u8>, u8)>, // sorted by key name.
    // pub readers: Vec<Vec<Arc<VLogReader>>>,
    pub readers: Vec<Arc<VLogReader>>,
    pub indexes: Vec<Vec<VLogIndex>>,
    pub files: Vec<String>,
    pub(crate) aggregates: Option<AggregateInputs>,
}

pub(crate) type AggregateInputs = HashMap<(i64, i64, i64), Arc<super::index::AggValues>>;
pub(crate) type AggregateOutputs = HashMap<String, Vec<Arc<super::index::AggValues>>>;

impl VLogCompactInfo {
    pub fn keys_len(&self) -> usize {
        self.keys.len()
    }

    pub fn readers_len(&self) -> usize {
        self.readers.len()
    }

    pub fn files_len(&self) -> usize {
        self.files.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.len() == 0
    }
}

struct WriteFileRes {
    write_vlog_res: Result<()>,
    vlog_files: Vec<String>,
    vlog_indexes: HashMap<String, Vec<VLogIndex>>,
}

impl Default for WriteFileRes {
    fn default() -> Self {
        Self {
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
                    return Err(Error::Writer { source: e });
                }
                _ => {
                    let _ = w.close();
                    // w.reset_index();
                    return Err(Error::Writer { source: e });
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

fn close_file(w: &mut VLogWriter<impl SyncWrite>, mut err: Result<()>) -> Result<()> {
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

pub trait KeyIterator:
    Iterator<Item = (i32, u8, Arc<Vec<u8>>, i64, i64, Vec<u8>, Option<String>)>
{
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

pub struct VLogErrors(Vec<Error>);
impl VLogErrors {
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

impl Deref for VLogErrors {
    type Target = Vec<Error>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for VLogErrors {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// TSMBatchKeyIter implements the KeyIter for set of VLogReaders.  Iteration produces
/// keys in sorted order and the values between the keys sorted and deduped.  If any of
/// the readers have associated tombstone entries, they are returned as part of iteration.
// Bound encoded read-ahead per compaction, not the shard's total memory. Keep
// complete keys together so the native overlap/copy/merge decisions do not change.
const VLOG_PREFETCH_BYTES: usize = 64 * 1024 * 1024;

struct VLogBatchKeyIter {
    collect_aggregates: bool,
    aggregate_decodes: usize,
    input_aggregates: Option<AggregateInputs>,
    output_aggregates: AggregateOutputs,
    // readers is the set of readers it produce a sorted key run with
    readers: Vec<Arc<VLogReader>>,

    // vlog_indexes: HashMap<(i64, i64), HashMap<Vec<u8>, Arc<Vec<VLogIndex>>>>,
    vlog_indexes: Vec<Vec<VLogIndex>>,

    // all keys and types in readers, keys should be sorted.
    keys: Vec<(Vec<u8>, u8)>,
    // the next min key position in keys.
    pos: usize,

    // // values is the temporary buffers for each key that is returned by a reader
    // values: HashMap<String, Value>,

    // // pos is the current key position within the corresponding readers slice.  A value of
    // // pos[0] = 1, means the reader[0] is currently at key 1 in its ordered index.
    // pos: Vec<usize>,

    // errs is any error we received while iterating values.
    errs: VLogErrors,

    // // indicates whether the iterator should choose a faster merging strategy over a more
    // // optimally compressed one.  If fast is true, multiple blocks will just be added as is
    // // and not combined.  In some cases, a slower path will need to be utilized even when
    // // fast is true to prevent overlapping blocks of time for the same key.
    // // If false, the blocks will be decoded and duplicated (if needed) and
    // // then chunked into the maximally sized blocks.
    fast: bool, // TODO: need ?

    // size is the maximum number of values to encode in a single block
    size: i64,

    // // key is the current key lowest key across all readers that has not be fully exhausted
    // // of values.
    // key: Vec<u8>,
    // typ: u8,

    // tsmFiles are the string names of the files for use in tracking errors, ordered the same
    // as iterators and buf
    // tsm_files: Vec<String>,
    // currentTsm is the current TSM file being iterated over
    current_tsm: String,
    // input, iterators[i] is all block iter in tsm_i file.
    // iterators: Vec<BlockIter>,  // TODO: need ?
    // output, all index blocks (index and data block content) in tsm files that equals to min_key
    blocks: Blocks, //
    // // output, buf[i] is all index blocks (index and data block content) in tsm_i file.
    // buf: Vec<Blocks>,

    // // mergeValues are decoded blocks that have been combined from multi files.
    merged_float_values: FloatArray,
    merged_integer_values: IntegerArray,
    merged_unsigned_values: UnsignedArray,
    merged_bool_values: BoolArray,
    merged_string_values: StringArray,

    // merged are encoded blocks that have been combined or used as is without decode.
    // each call to next will return block from merged.
    merged: Blocks,
    interrupt: Option<CancellationToken>,
    prefetched_blocks: Option<Vec<Blocks>>,
    prefetch_start: usize,
    prefetch_end: usize,
    prefetch_limit: usize,
    prefetch_peak_bytes: usize,
    prefetch_batches: usize,
    total_spawned_tasks: usize,
    executor: ExecutorRef,
    #[cfg(test)]
    read_test_hook: Option<Arc<VLogReadTestHook>>,
}

#[derive(Clone)]
struct VLogReadRequest {
    key_pos: usize,
    key: Vec<u8>,
    typ: u8,
    index: VLogIndex,
    aggregate: Option<Arc<super::index::AggValues>>,
}

struct VLogReadGroup {
    reader: Arc<VLogReader>,
    requests: Vec<VLogReadRequest>,
}


type VLogReadTaskResult = Result<Vec<(usize, Block)>>;

struct VLogReadTaskSet {
    tasks: Vec<TaskHandle<VLogReadTaskResult>>,
    executor: ExecutorRef,
    completed: bool,
}

impl VLogReadTaskSet {
    fn new(tasks: Vec<TaskHandle<VLogReadTaskResult>>, executor: ExecutorRef) -> Self {
        Self {
            tasks,
            executor,
            completed: false,
        }
    }

    async fn wait(mut self) -> Result<Vec<Vec<(usize, Block)>>> {
        let results = futures::future::join_all(self.tasks.iter_mut()).await;
        self.completed = true;

        let mut outputs = Vec::with_capacity(results.len());
        for result in results {
            let output = result.map_err(|source| Error::ReadTaskTerminated { source })??;
            outputs.push(output);
        }
        Ok(outputs)
    }
}

impl Drop for VLogReadTaskSet {
    fn drop(&mut self) {
        if !self.completed {
            for task in &self.tasks {
                task.abort();
            }
            let mut tasks = std::mem::take(&mut self.tasks);
            let _ = self.executor.spawn_compact(async move {
                let _ = futures::future::join_all(tasks.iter_mut()).await;
            });
        }
    }
}

impl VLogBatchKeyIter {
    /// Returns a new TSM key iterator from readers.
    /// size indicates the maximum number of values to encode in a single block.
    pub async fn new(
        size: i64,
        fast: bool,
        interrupt: Option<CancellationToken>,
        // readers: &[Arc<VLogReader>],
        // unique_keys: HashMap<Vec<u8>, u8>,
        vlog_compact_info: VLogCompactInfo,
    ) -> Result<Self> {
        Self::new_with_executor(
            size,
            fast,
            interrupt,
            vlog_compact_info,
            legacy_global_executor(),
            #[cfg(test)]
            None,
        )
        .await
    }

    pub async fn new_with_executor(
        size: i64,
        fast: bool,
        interrupt: Option<CancellationToken>,
        vlog_compact_info: VLogCompactInfo,
        executor: ExecutorRef,
        #[cfg(test)] read_test_hook: Option<Arc<VLogReadTestHook>>,
    ) -> Result<Self> {
        Self::new_with_prefetch_limit(
            size, fast, interrupt, vlog_compact_info, executor, VLOG_PREFETCH_BYTES,
            #[cfg(test)]
            read_test_hook,
        ).await
    }

    async fn new_with_prefetch_limit(
        size: i64,
        fast: bool,
        interrupt: Option<CancellationToken>,
        vlog_compact_info: VLogCompactInfo,
        executor: ExecutorRef,
        prefetch_limit: usize,
        #[cfg(test)] read_test_hook: Option<Arc<VLogReadTestHook>>,
    ) -> Result<Self> {
        let mut iter = Self {
            collect_aggregates: vlog_compact_info.aggregates.is_some(),
            aggregate_decodes: 0,
            input_aggregates: vlog_compact_info.aggregates,
            output_aggregates: HashMap::new(),
            readers: vlog_compact_info.readers,
            vlog_indexes: vlog_compact_info.indexes,
            // values: HashMap::new(),
            keys: vlog_compact_info.keys,
            pos: 0,
            // typ: 0,
            errs: VLogErrors(vec![]),
            fast,
            current_tsm: String::new(),
            size,
            // key: vec![],
            // typ: 0,
            // iterators: iter,
            blocks: Blocks::default(),
            // buf: vec![Blocks::default(); iter_len],
            merged_integer_values: IntegerArray::default(),
            merged_unsigned_values: UnsignedArray::default(),
            merged_float_values: FloatArray::default(),
            merged_bool_values: BoolArray::default(),
            merged_string_values: StringArray::default(),

            merged: Blocks::default(),
            interrupt,
            prefetched_blocks: None,
            prefetch_start: 0,
            prefetch_end: 0,
            prefetch_limit,
            prefetch_peak_bytes: 0,
            prefetch_batches: 0,
            total_spawned_tasks: 0,
            executor,
            #[cfg(test)]
            read_test_hook,
        };

        iter.prefetch_blocks().await?;

        Ok(iter)
    }

    fn append_error(&mut self, err: Error) {
        self.errs.push(err);
    }

    fn prefetch_groups(&mut self) -> Result<BTreeMap<(i64, i64), VLogReadGroup>> {
        if self.interrupt.as_ref().is_some_and(CancellationToken::is_cancelled) {
            return Err(Error::CompactionAborted { reason: "VLog compaction read cancelled".into() });
        }
        if self.prefetched_blocks.as_ref().is_some_and(|batch| batch.iter().any(|b| !b.is_empty())) {
            return Err(Error::StringErr { reason: "VLog prefetch batch is still in use".into() });
        }
        // Never split a key: the native merger needs all overlapping blocks,
        // older versions and tombstones in their original order.
        let mut bytes = 0usize;
        let mut end = self.pos;
        for indexes in self.vlog_indexes.iter().skip(self.pos) {
            let mut key_bytes = 0usize;
            for index in indexes {
                let size = usize::try_from(index.size).ok().and_then(|n| n.checked_sub(4))
                    .ok_or_else(|| Error::StringErr { reason: "Invalid VLog block size in prefetch".into() })?;
                key_bytes = key_bytes.checked_add(size)
                    .ok_or_else(|| Error::StringErr { reason: "VLog prefetch size overflow".into() })?;
            }
            if key_bytes > self.prefetch_limit {
                return Err(Error::StringErr { reason: format!(
                    "VLog compaction key requires {key_bytes} encoded bytes, exceeding prefetch limit {}",
                    self.prefetch_limit) });
            }
            if key_bytes > self.prefetch_limit - bytes { break; }
            bytes += key_bytes;
            end += 1;
        }
        self.prefetched_blocks = None;
        self.prefetch_start = self.pos;
        self.prefetch_end = end;
        self.prefetch_peak_bytes = self.prefetch_peak_bytes.max(bytes);
        self.prefetch_batches += usize::from(end > self.pos);
        let mut reader_by_file = HashMap::<(i64, i64), Arc<VLogReader>>::new();
        for reader in &self.readers {
            reader_by_file.insert((reader.generation, reader.seq), Arc::clone(reader));
        }

        let mut groups = BTreeMap::<(i64, i64), VLogReadGroup>::new();
        for (key_pos, indexes) in self.vlog_indexes.iter().enumerate().take(end).skip(self.pos) {
            if indexes.is_empty() {
                continue;
            }

            let (key, typ) = &self.keys[key_pos];
            for index in indexes {
                let reader_key = (index.generation, index.seq);
                let reader = match reader_by_file.get(&reader_key) {
                    Some(reader) => Arc::clone(reader),
                    None => {
                        return Err(Error::StringErr {
                            reason: format!(
                                "missing VLog reader for generation={} seq={}",
                                index.generation, index.seq
                            ),
                        });
                    }
                };

                groups
                    .entry(reader_key)
                    .or_insert_with(|| VLogReadGroup {
                        reader,
                        requests: Vec::new(),
                    })
                    .requests
                    .push(VLogReadRequest {
                        key_pos,
                        key: key.clone(),
                        typ: *typ,
                        index: index.clone(),
                        aggregate: self.input_aggregates.as_ref().and_then(|values|
                            values.get(&(index.generation, index.seq, index.offset)).cloned()),
                    });
            }
        }

        Ok(groups)
    }

    fn read_group(
        group: VLogReadGroup,
        interrupt: Option<CancellationToken>,
        #[cfg(test)] read_test_hook: Option<Arc<VLogReadTestHook>>,
    ) -> VLogReadTaskResult {
        let reader = group.reader;
        let _reader_lease = VLogReadLease::new(Arc::clone(&reader));
        let requests = group.requests;
        let current_tsm = reader.path().to_string_lossy().into_owned();
        #[cfg(test)]
        let _read_test_guard = read_test_hook.as_ref().map(VLogReadTestHook::enter);
        let mut blocks = Vec::<(usize, Block)>::with_capacity(requests.len());
        let mut tombstones_by_key = HashMap::<Vec<u8>, Vec<TimeRange>>::new();

        for request in requests {
            if interrupt
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
            {
                return Err(Error::CompactionAborted {
                    reason: "VLog compaction read cancelled".to_string(),
                });
            }

            match request.typ {
                INTEGER | UNSIGNED | FLOAT | BOOL | STRING => {
                    let (_checksum, b) = match reader.read_bytes(
                        request.index.offset as u64,
                        request.index.size as u64,
                        &[],
                    ) {
                        Ok(read) => read,
                        Err(e) => {
                            return Err(Error::BlockRead {
                                file: current_tsm,
                                reason: e.to_string(),
                            });
                        }
                    };

                    let tombstones = tombstones_by_key
                        .entry(request.key.clone())
                        .or_insert_with(|| reader.tombstone_range(request.key.as_slice()))
                        .clone();
                    blocks.push((
                        request.key_pos,
                        Block {
                            aggregate: request.aggregate,
                            aggregate_copied: true,
                            count: request.index.count as usize,
                            key: request.key,
                            min_time: request.index.min,
                            max_time: request.index.max,
                            typ: request.typ,
                            b,
                            tombstones,
                            read_min: i64::MAX,
                            read_max: i64::MIN,
                        },
                    ));
                }
                _ => {
                    return Err(Error::StringErr {
                        reason: format!("invalid type: {}", request.typ),
                    });
                }
            }
        }

        Ok(blocks)
    }

    fn install_prefetched(&mut self, outputs: Vec<Vec<(usize, Block)>>) {
        let mut prefetched = vec![Blocks::default(); self.prefetch_end - self.prefetch_start];
        for blocks in outputs {
            for (key_pos, block) in blocks {
                prefetched[key_pos - self.prefetch_start].push(block);
            }
        }
        self.prefetched_blocks = Some(prefetched);
    }

    async fn prefetch_blocks(&mut self) -> Result<()> {
        let groups = self.prefetch_groups()?;
        self.total_spawned_tasks += groups.len();
        let admission = TaskAdmissionGate::pending();
        let mut tasks = Vec::with_capacity(groups.len());
        for group in groups.into_values() {
            // Keep the original child lease even if its parent is cancelled.
            let reader_lease = VLogReadLease::new(Arc::clone(&group.reader));
            let child_admission = admission.clone();
            let interrupt = self.interrupt.clone();
            #[cfg(test)]
            let read_test_hook = self.read_test_hook.clone();
            match self.executor.spawn_blocking_compact(move || {
                let _reader_lease = reader_lease;
                if !child_admission.wait() {
                    return Err(Error::CompactionAborted { reason: "VLog read fanout admission cancelled".into() });
                }
                Self::read_group(group, interrupt, #[cfg(test)] read_test_hook)
            }) {
                Ok(task) => tasks.push(task),
                Err(source) => {
                    admission.cancel();
                    let _ = VLogReadTaskSet::new(tasks, Arc::clone(&self.executor)).wait().await;
                    return Err(Error::Execution { source });
                }
            }
        }
        admission.run();
        let outputs = VLogReadTaskSet::new(tasks, Arc::clone(&self.executor)).wait().await?;
        self.install_prefetched(outputs);
        Ok(())
    }

    fn refill_prefetch(&mut self) -> Result<()> {
        let groups = self.prefetch_groups()?;
        let mut outputs = Vec::with_capacity(groups.len());
        // The writer is already on a blocking worker. Do not wait for children
        // on that same pool: a one-thread executor must also make progress.
        for group in groups.into_values() {
            outputs.push(Self::read_group(group, self.interrupt.clone(),
                #[cfg(test)] self.read_test_hook.clone())?);
        }
        self.install_prefetched(outputs);
        Ok(())
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
        let typ = self.keys[self.pos - 1].1;
        match typ {
            BLOCK_INTEGER => self.merge_integer(), // TODO: compact_gen
            BLOCK_UNSIGNED => self.merge_unsigned(),
            BLOCK_FLOAT64 => self.merge_float(),
            BLOCK_BOOL => self.merge_bool(),
            BLOCK_STRING => self.merge_string(),
            _ => self.append_error(Error::StringErr {
                reason: format!("unknown block type: {}", typ),
            }),
        }
    }

    fn handle_encode_error(&mut self, err: Error, typ: &str) {
        self.append_error(Error::BlockRead {
            file: self.current_tsm.clone(),
            reason: format!(
                "encode error: unable to compress block type {} for key {}: {}",
                typ,
                to_str!(&self.keys[self.pos - 1].0),
                err
            ),
        })
    }

    fn handle_decode_error(&mut self, err: Error, typ: &str) {
        self.append_error(Error::BlockRead {
            file: self.current_tsm.clone(),
            reason: format!(
                "decode error: unable to decompress block type {} for key '{}': {}",
                typ,
                to_str!(&self.keys[self.pos - 1].0),
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
                    self.aggregate_decodes += usize::from(self.collect_aggregates);
                    if let Err(e) = decode_integer_array_block(&b.b, &mut v) {
                        self.handle_decode_error(
                            Error::StringErr {
                                reason: e.to_string(),
                            },
                            "integer",
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
                self.aggregate_decodes += usize::from(self.collect_aggregates);
                if let Err(e) = decode_integer_array_block(&cur.b, &mut v) {
                    self.handle_decode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "integer",
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
            let count = values.timestamps.len();
            values.values = self.merged_integer_values.values[..self.size as usize].to_vec();

            let aggregate = match aggregate::summarize(self.collect_aggregates, &values) {
                Ok(value) => value,
                Err(error) => {
                    self.append_error(Error::StringErr {
                        reason: error.to_string(),
                    });
                    return Blocks::default();
                }
            };
            let cb = match encode_integer_array_block(&mut values, &[]) {
                // TODO: pool this buffer
                Ok(b) => b,
                Err(e) => {
                    self.handle_encode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "integer",
                    );
                    return Blocks::default();
                }
            };

            dst.push(Block {
                count,
                key: self.keys[self.pos - 1].0.clone(),
                min_time,
                max_time,
                typ: INTEGER,
                b: cb,
                aggregate,
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
            let (min_time, max_time, count) = (
                self.merged_integer_values.timestamps[0],
                self.merged_integer_values.timestamps
                    [self.merged_integer_values.timestamps.len() - 1],
                self.merged_integer_values.timestamps.len(),
            );
            let aggregate = match aggregate::summarize(
                self.collect_aggregates,
                &self.merged_integer_values,
            ) {
                Ok(value) => value,
                Err(error) => {
                    self.append_error(Error::StringErr {
                        reason: error.to_string(),
                    });
                    return Blocks::default();
                }
            };
            let cb = match encode_integer_array_block(&mut self.merged_integer_values, &[]) {
                Ok(b) => b,
                Err(e) => {
                    self.handle_encode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "integer",
                    );
                    return Blocks::default();
                }
            };
            dst.push(Block {
                count,
                key: self.keys[self.pos - 1].0.clone(),
                min_time,
                max_time,
                typ: INTEGER,
                b: cb,
                aggregate,
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
                    self.aggregate_decodes += usize::from(self.collect_aggregates);
                    if let Err(e) = decode_unsigned_array_block(&b.b, &mut v) {
                        self.handle_decode_error(
                            Error::StringErr {
                                reason: e.to_string(),
                            },
                            "unsigned",
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
                self.aggregate_decodes += usize::from(self.collect_aggregates);
                if let Err(e) = decode_unsigned_array_block(&cur.b, &mut v) {
                    self.handle_decode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "unsigned",
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
            let count = values.timestamps.len();
            values.values = self.merged_unsigned_values.values[..self.size as usize].to_vec();

            let aggregate = match aggregate::summarize(self.collect_aggregates, &values) {
                Ok(value) => value,
                Err(error) => {
                    self.append_error(Error::StringErr {
                        reason: error.to_string(),
                    });
                    return Blocks::default();
                }
            };
            let cb = match encode_unsigned_array_block(&mut values, &[]) {
                // TODO: pool this buffer
                Ok(b) => b,
                Err(e) => {
                    self.handle_encode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "unsigned",
                    );
                    return Blocks::default();
                }
            };

            dst.push(Block {
                count,
                key: self.keys[self.pos - 1].0.clone(),
                min_time,
                max_time,
                typ: UNSIGNED,
                b: cb,
                aggregate,
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
            let (min_time, max_time, count) = (
                self.merged_unsigned_values.timestamps[0],
                self.merged_unsigned_values.timestamps
                    [self.merged_unsigned_values.timestamps.len() - 1],
                self.merged_unsigned_values.timestamps.len(),
            );
            let aggregate = match aggregate::summarize(
                self.collect_aggregates,
                &self.merged_unsigned_values,
            ) {
                Ok(value) => value,
                Err(error) => {
                    self.append_error(Error::StringErr {
                        reason: error.to_string(),
                    });
                    return Blocks::default();
                }
            };
            let cb = match encode_unsigned_array_block(&mut self.merged_unsigned_values, &[]) {
                Ok(b) => b,
                Err(e) => {
                    self.handle_encode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "unsigned",
                    );
                    return Blocks::default();
                }
            };
            dst.push(Block {
                count,
                key: self.keys[self.pos - 1].0.clone(),
                min_time,
                max_time,
                typ: UNSIGNED,
                b: cb,
                aggregate,
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
                    self.aggregate_decodes += usize::from(self.collect_aggregates);
                    if let Err(e) = decode_float_array_block(&b.b, &mut v) {
                        self.handle_decode_error(
                            Error::StringErr {
                                reason: e.to_string(),
                            },
                            "float",
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
            return self.chunk_float(true);
        }

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

            self.merged.push(cur.clone()); // TODO: optimize me, use Arc
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
            self.aggregate_decodes += usize::from(self.collect_aggregates);
            if let Err(e) = decode_float_array_block(&cur.b, &mut v) {
                self.handle_decode_error(
                    Error::StringErr {
                        reason: e.to_string(),
                    },
                    "float",
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
            let max_time = *values.timestamps.last().unwrap();
            let count = values.timestamps.len();
            values.values = self.merged_float_values.values[..self.size as usize].to_vec();

            let aggregate = match aggregate::summarize(self.collect_aggregates, &values) {
                Ok(value) => value,
                Err(error) => {
                    self.append_error(Error::StringErr {
                        reason: error.to_string(),
                    });
                    return Blocks::default();
                }
            };
            let cb = match encode_float_array_block(&mut values, &[]) {
                // TODO: pool this buffer
                Ok(b) => b,
                Err(e) => {
                    self.handle_encode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "float",
                    );
                    return Blocks::default();
                }
            };

            dst.push(Block {
                count,
                key: self.keys[self.pos - 1].0.clone(),
                min_time,
                max_time,
                typ: FLOAT,
                b: cb,
                aggregate,
                ..Default::default()
            });

            self.merged_float_values
                .timestamps
                .drain(..self.size as usize);
            self.merged_float_values.values.drain(..self.size as usize);

            return dst;
        }

        // Re-encode the remaining values into the last block
        if !self.merged_float_values.is_empty() {
            let (min_time, max_time, count) = (
                self.merged_float_values.timestamps[0],
                *self.merged_float_values.timestamps.last().unwrap(),
                self.merged_float_values.timestamps.len(),
            );
            let aggregate = match aggregate::summarize(
                self.collect_aggregates,
                &self.merged_float_values,
            ) {
                Ok(value) => value,
                Err(error) => {
                    self.append_error(Error::StringErr {
                        reason: error.to_string(),
                    });
                    return Blocks::default();
                }
            };
            let cb = match encode_float_array_block(&mut self.merged_float_values, &[]) {
                Ok(b) => b,
                Err(e) => {
                    self.handle_encode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "float",
                    );
                    return Blocks::default();
                }
            };
            dst.push(Block {
                count,
                key: self.keys[self.pos - 1].0.clone(),
                min_time,
                max_time,
                typ: FLOAT,
                b: cb,
                aggregate,
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
                    self.aggregate_decodes += usize::from(self.collect_aggregates);
                    if let Err(e) = decode_bool_array_block(&b.b, &mut v) {
                        self.handle_decode_error(
                            Error::StringErr {
                                reason: e.to_string(),
                            },
                            "bool",
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
                self.aggregate_decodes += usize::from(self.collect_aggregates);
                if let Err(e) = decode_bool_array_block(&cur.b, &mut v) {
                    self.handle_decode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "bool",
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
            let count = values.timestamps.len();
            values.values = self.merged_bool_values.values[..self.size as usize].to_vec();

            let aggregate = match aggregate::summarize(self.collect_aggregates, &values) {
                Ok(value) => value,
                Err(error) => {
                    self.append_error(Error::StringErr {
                        reason: error.to_string(),
                    });
                    return Blocks::default();
                }
            };
            let cb = match encode_bool_array_block(&mut values, &[]) {
                // TODO: pool this buffer
                Ok(b) => b,
                Err(e) => {
                    self.handle_encode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "bool",
                    );
                    return Blocks::default();
                }
            };

            dst.push(Block {
                count,
                key: self.keys[self.pos - 1].0.clone(),
                min_time,
                max_time,
                typ: BOOL,
                b: cb,
                aggregate,
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
            let (min_time, max_time, count) = (
                self.merged_bool_values.timestamps[0],
                self.merged_bool_values.timestamps[self.merged_bool_values.timestamps.len() - 1],
                self.merged_bool_values.timestamps.len(),
            );
            let aggregate = match aggregate::summarize(
                self.collect_aggregates,
                &self.merged_bool_values,
            ) {
                Ok(value) => value,
                Err(error) => {
                    self.append_error(Error::StringErr {
                        reason: error.to_string(),
                    });
                    return Blocks::default();
                }
            };
            let cb = match encode_bool_array_block(&mut self.merged_bool_values, &[]) {
                Ok(b) => b,
                Err(e) => {
                    self.handle_encode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "bool",
                    );
                    return Blocks::default();
                }
            };
            dst.push(Block {
                count,
                key: self.keys[self.pos - 1].0.clone(),
                min_time,
                max_time,
                typ: BOOL,
                b: cb,
                aggregate,
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
                    self.aggregate_decodes += usize::from(self.collect_aggregates);
                    if let Err(e) = decode_string_array_block(&b.b, &mut v) {
                        self.handle_decode_error(
                            Error::StringErr {
                                reason: e.to_string(),
                            },
                            "string",
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
                    Self::retain_string_range(&mut v, b.read_min, b.read_max, false);

                    // Filter out only the values for overlapping block
                    Self::retain_string_range(&mut v, min_time, max_time, true);
                    if !v.is_empty() {
                        b.mark_read(v.min_time(), v.max_time());
                    }

                    // Apply each tombstone to the block
                    for ts in b.tombstones.iter() {
                        Self::retain_string_range(&mut v, ts.min, ts.max, false);
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
                self.aggregate_decodes += usize::from(self.collect_aggregates);
                if let Err(e) = decode_string_array_block(&cur.b, &mut v) {
                    self.handle_decode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "string",
                    );
                    return Blocks::default();
                };

                if cur.max_time != v.max_time() {
                    cur.max_time = v.max_time();
                }

                // Apply each tombstone to the block
                for ts in cur.tombstones.iter() {
                    Self::retain_string_range(&mut v, ts.min, ts.max, false);
                }

                cur.mark_read(cur.min_time, cur.max_time);

                self.merged_string_values.merge(v);
                i += 1;
            }

            self.blocks.drain(..i);

            self.chunk_string(false)
        }
    }

    fn retain_string_range(values: &mut StringArray, min: i64, max: i64, inside: bool) {
        // Move owned Strings safely instead of copying their allocation pointers.
        let timestamps = &values.timestamps;
        let mut position = 0;
        values.values.retain(|_| {
            let timestamp = timestamps[position];
            position += 1;
            (timestamp >= min && timestamp <= max) == inside
        });
        values
            .timestamps
            .retain(|timestamp| (*timestamp >= min && *timestamp <= max) == inside);
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
            let count = values.timestamps.len();
            values.values = self.merged_string_values.values[..self.size as usize].to_vec();

            let aggregate = match aggregate::summarize(self.collect_aggregates, &values) {
                Ok(value) => value,
                Err(error) => {
                    self.append_error(Error::StringErr {
                        reason: error.to_string(),
                    });
                    return Blocks::default();
                }
            };
            let cb = match encode_string_array_block(&mut values, &[]) {
                // TODO: pool this buffer
                Ok(b) => b,
                Err(e) => {
                    self.handle_encode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "string",
                    );
                    return Blocks::default();
                }
            };

            dst.push(Block {
                count,
                key: self.keys[self.pos - 1].0.clone(),
                min_time,
                max_time,
                typ: STRING,
                b: cb,
                aggregate,
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
            let (min_time, max_time, count) = (
                self.merged_string_values.timestamps[0],
                self.merged_string_values.timestamps
                    [self.merged_string_values.timestamps.len() - 1],
                self.merged_string_values.timestamps.len(),
            );
            let aggregate = match aggregate::summarize(
                self.collect_aggregates,
                &self.merged_string_values,
            ) {
                Ok(value) => value,
                Err(error) => {
                    self.append_error(Error::StringErr {
                        reason: error.to_string(),
                    });
                    return Blocks::default();
                }
            };
            let cb = match encode_string_array_block(&mut self.merged_string_values, &[]) {
                Ok(b) => b,
                Err(e) => {
                    self.handle_encode_error(
                        Error::StringErr {
                            reason: e.to_string(),
                        },
                        "string",
                    );
                    return Blocks::default();
                }
            };
            dst.push(Block {
                count,
                key: self.keys[self.pos - 1].0.clone(),
                min_time,
                max_time,
                typ: STRING,
                b: cb,
                aggregate,
                ..Default::default()
            });
            self.merged_string_values.timestamps.clear();
            self.merged_string_values.values.clear();
        }

        dst
    }
}

impl KeyIterator for VLogBatchKeyIter {
    /// Returns true if there are any values remaining in the iterator.
    /// Every time has_next is called, wen will find all data of the smallest_key in all compact tsm files,
    /// merge and remove the tombstoned data, the smallest_key is the the smallest key in all compact tsm files.
    fn has_next(&mut self) -> bool {
        // Any merged blocks pending?
        if !self.merged.is_empty() {
            return true;
        }

        // Drain the current key's input blocks and typed remainder before advancing.
        if !self.blocks.is_empty() || self.has_merged_values() {
            self.merge();
            if !self.merged.is_empty() {
                return true;
            }
        }

        // Read the blocks from each TSM iterator for tne next min_key.
        if self.pos == self.keys.len() {
            return false;
        }

        if self.pos == self.prefetch_end {
            if let Err(error) = self.refill_prefetch() {
                self.append_error(error);
                return false;
            }
        }
        if self.prefetched_blocks.is_some() {
            let key_pos = self.pos - self.prefetch_start;
            self.blocks = self
                .prefetched_blocks
                .as_mut()
                .and_then(|prefetched| prefetched.get_mut(key_pos))
                .map(std::mem::take)
                .unwrap_or_default();
            self.pos += 1;

            if self.blocks.is_empty() {
                return false;
            }
            self.merge();
            return !self.merged.is_empty();
        }

        self.append_error(Error::StringErr {
            reason: "VLog compaction iterator was not prefetched".to_string(),
        });
        false
    }

    fn close(&mut self) -> Result<()> {
        for r in self.readers.iter() {
            r.close().context(ReaderSnafu)?;
        }
        self.readers = vec![];

        self.keys = vec![];
        self.pos = 0;
        self.current_tsm.clear();
        self.blocks.clear();
        self.merged.clear();
        self.interrupt = None;
        self.prefetched_blocks = None;

        Ok(())
    }

    /// Returns any errors encountered during iteration.
    fn err(&self) -> Option<String> {
        self.errs.err()
    }

    fn estimated_index_size(&self) -> usize {
        todo!("no need to implement this method")
    }
}

impl Iterator for VLogBatchKeyIter {
    // FIXME: (&[U8], i64, i64, (&[U8], Err) ???
    type Item = (i32, u8, Arc<Vec<u8>>, i64, i64, Vec<u8>, Option<String>);

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
                return Some((
                    0,
                    EMPTY,
                    Arc::new(vec![]),
                    0,
                    0,
                    vec![],
                    Some(
                        Error::CompactionAborted {
                            reason: "".to_string(),
                        }
                        .to_string(),
                    ),
                ));
            }
        }

        if self.merged.is_empty() {
            return Some((0, EMPTY, Arc::new(vec![]), 0, 0, vec![], self.err()));
        }

        // TODO: optimize me, pop front from vec.
        // let block = &self.merged[0];
        let block = self.merged.drain(..1).next().unwrap();
        if self.collect_aggregates {
            let Some(aggregate) = block.aggregate else {
                self.append_error(Error::StringErr {
                    reason: "copied VLog block is missing its stored aggregate".into(),
                });
                return Some((0, EMPTY, Arc::new(Vec::new()), 0, 0, Vec::new(), self.err()));
            };
            self.output_aggregates.entry(to_string!(block.key.clone())).or_default().push(aggregate);
        }
        Some((
            block.count as i32,
            block.typ,
            Arc::new(block.key),
            block.min_time,
            block.max_time,
            block.b,
            self.err(),
        ))
    }
}

#[derive(Default, Clone)]
pub struct Block {
    pub(crate) aggregate: Option<Arc<super::index::AggValues>>,
    pub(crate) aggregate_copied: bool,
    pub count: usize,
    pub key: Vec<u8>,
    pub min_time: i64,
    pub max_time: i64,
    pub typ: u8,
    pub b: Vec<u8>, // data block corresponding to cur index block  // TODO: Bytes ?
    pub tombstones: Vec<TimeRange>,

    // read_min, read_max are the timestamps range of values have been
    // read and encoded from this block.
    pub read_min: i64,
    pub read_max: i64,
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
        self.count == other.count
            && self.key == other.key
            && self.min_time == other.min_time
            && self.max_time == other.max_time
            && self.typ == other.typ
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

#[cfg(test)]
#[path = "compact_remainder_tests.rs"]
mod remainder_tests;

#[cfg(test)]
#[path = "compact_prefetch_tests.rs"]
mod prefetch_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::execution::{
        BoxBlockingTask, BoxFutureTask, DefaultTokioExecutor, DefaultTokioExecutorOptions,
        RuntimeExecutor, TaskController,
    };
    use crate::storage::tsm1::vlog::vlog_index_cache::Cache as VLogIndexCache;

    fn encoded_float_block(timestamp: i64, value: f64) -> Vec<u8> {
        let mut values = FloatArray {
            timestamps: vec![timestamp],
            values: vec![value],
        };
        encode_float_array_block(&mut values, &[]).unwrap()
    }

    pub(super) fn write_test_vlog_reader(
        dir: &Path,
        generation: i64,
        seq: i64,
        blocks: &[(&[u8], i64, f64)],
    ) -> (Arc<VLogReader>, HashMap<String, Vec<VLogIndex>>) {
        let path = dir.join(format!(
            "{}.{}",
            default_format_file_name(generation, seq),
            VLOG_FILE_EXTENSION
        ));
        let fd = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        let mut writer = VLogWriter::new(fd, generation, seq, &path);
        let mut indexes_by_key = HashMap::<String, Vec<VLogIndex>>::new();

        for (key, timestamp, value) in blocks {
            let block = encoded_float_block(*timestamp, *value);
            let (offset, size) = writer
                .write_block(key, *timestamp, *timestamp, &block)
                .unwrap();
            indexes_by_key
                .entry(to_string!(key.to_vec()))
                .or_default()
                .push(VLogIndex {
                    aggregate: None,
                    count: 1,
                    typ: FLOAT,
                    min: *timestamp,
                    max: *timestamp,
                    generation,
                    seq,
                    offset,
                    size,
                });
        }
        writer.close().unwrap();

        let cache = Arc::new(VLogIndexCache::new(0));
        cache.write_multi(indexes_by_key.clone()).unwrap();
        let keys = blocks
            .iter()
            .map(|(key, _, _)| (key.to_vec(), FLOAT))
            .collect::<Vec<_>>();
        let reader = VLogReader::new(
            fs::OpenOptions::new().read(true).open(&path).unwrap(),
            &path,
            generation,
            seq,
            keys,
            cache,
        )
        .unwrap();

        (Arc::new(reader), indexes_by_key)
    }

    fn compact_info_fixture(dir: &Path, reader_count: usize) -> VLogCompactInfo {
        let key = b"cpu,host=A#!~#value";
        let mut readers = Vec::with_capacity(reader_count);
        let mut indexes_by_key = HashMap::<String, Vec<VLogIndex>>::new();

        for generation in 1..=reader_count as i64 {
            let (reader, indexes) =
                write_test_vlog_reader(dir, generation, 1, &[(key, generation, generation as f64)]);
            readers.push(reader);
            for (key, mut indexes) in indexes {
                indexes_by_key.entry(key).or_default().append(&mut indexes);
            }
        }

        let keys = vec![(key.to_vec(), FLOAT)];
        let indexes = vec![
            indexes_by_key
                .get(&to_string!(key.to_vec()))
                .cloned()
                .unwrap(),
        ];
        let files = readers
            .iter()
            .map(|reader| reader.path().to_string_lossy().into_owned())
            .collect();
        VLogCompactInfo {
            keys,
            readers,
            indexes,
            files,
            aggregates: None,
        }
    }

    pub(super) fn default_executor(compact_blocking_workers: usize) -> Arc<DefaultTokioExecutor> {
        Arc::new(
            DefaultTokioExecutor::new(DefaultTokioExecutorOptions::new(
                1,
                2,
                1,
                compact_blocking_workers,
            ))
            .unwrap(),
        )
    }

    fn test_compactor_with_output_dir(
        dir: &Path,
        output_dir: &Path,
        executor: ExecutorRef,
    ) -> Arc<Compactor> {
        let index_cache = Arc::new(VLogIndexCache::new_with_executor(0, Arc::clone(&executor)));
        let file_store = Arc::new(
            FileStore::new_with_executor(
                Arc::new(dir.to_path_buf()),
                None,
                index_cache,
                Arc::clone(&executor),
            )
            .unwrap(),
        );
        let compactor = Arc::new(Compactor::new_with_executor(
            Arc::new(output_dir.to_path_buf()),
            Some(file_store),
            None,
            &VLogConfig::default(),
            executor,
        ));
        compactor.enable_vlog_compactions();
        compactor
    }

    fn test_compactor(dir: &Path, executor: ExecutorRef) -> Arc<Compactor> {
        test_compactor_with_output_dir(dir, dir, executor)
    }

    fn run_real_compaction(
        runtime: Arc<DefaultTokioExecutor>,
        compact_info: VLogCompactInfo,
        lazy: bool,
    ) -> (Vec<String>, HashMap<String, Vec<VLogIndex>>) {
        let executor: ExecutorRef = runtime.clone();
        let compactor = test_compactor(
            compact_info.readers[0].path().parent().unwrap(),
            Arc::clone(&executor),
        );
        let files = compact_info.files.clone();
        let coordinator_executor = Arc::clone(&executor);
        let coordinator_compactor = Arc::clone(&compactor);
        let coordinator = executor
            .spawn_compact(async move {
                let prepared = coordinator_compactor
                    .prepare_vlog_compaction(true, files, compact_info, lazy)
                    .await
                    .unwrap();
                let finish_compactor = Arc::clone(&coordinator_compactor);
                coordinator_executor
                    .spawn_blocking_compact(move || {
                        finish_compactor.finish_vlog_compaction(prepared)
                    })
                    .unwrap()
                    .await
                    .unwrap()
                    .unwrap()
            })
            .unwrap();

        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(10), coordinator)
                .await
                .expect("VLog compaction stalled on compact-blocking capacity")
                .unwrap()
        })
    }

    struct SelectiveCompactExecutor {
        inner: DefaultTokioExecutor,
        blocking_submissions: AtomicUsize,
        reject_submission: AtomicUsize,
        dispose_submission: AtomicUsize,
    }

    impl SelectiveCompactExecutor {
        fn new() -> Self {
            Self {
                inner: DefaultTokioExecutor::new(DefaultTokioExecutorOptions::new(1, 2, 1, 4))
                    .unwrap(),
                blocking_submissions: AtomicUsize::new(0),
                reject_submission: AtomicUsize::new(0),
                dispose_submission: AtomicUsize::new(0),
            }
        }

        fn reset(&self) {
            self.blocking_submissions.store(0, Ordering::SeqCst);
            self.reject_submission.store(0, Ordering::SeqCst);
            self.dispose_submission.store(0, Ordering::SeqCst);
        }
    }

    impl RuntimeExecutor for SelectiveCompactExecutor {
        fn schedule(&self, task: BoxFutureTask) -> std::result::Result<TaskController, SpawnError> {
            self.inner.schedule(task)
        }

        fn schedule_blocking(
            &self,
            task: BoxBlockingTask,
        ) -> std::result::Result<TaskController, SpawnError> {
            self.inner.schedule_blocking(task)
        }

        fn schedule_compact(
            &self,
            task: BoxFutureTask,
        ) -> std::result::Result<TaskController, SpawnError> {
            self.inner.schedule_compact(task)
        }

        fn schedule_blocking_compact(
            &self,
            task: BoxBlockingTask,
        ) -> std::result::Result<TaskController, SpawnError> {
            let submission = self.blocking_submissions.fetch_add(1, Ordering::SeqCst) + 1;
            if self.reject_submission.load(Ordering::SeqCst) == submission {
                return Err(SpawnError::rejected("injected VLog read refusal"));
            }
            if self.dispose_submission.load(Ordering::SeqCst) == submission {
                drop(task);
                return Ok(TaskController::detached());
            }
            self.inner.schedule_blocking_compact(task)
        }
    }

    #[test]
    fn test_lazy_vlog_batch_iter_batches_by_reader_not_key_reader_pairs() {
        let dir = tempfile::tempdir().unwrap();
        let key_a = b"cpu,host=A#!~#value";
        let key_b = b"cpu,host=B#!~#value";
        let key_c = b"cpu,host=C#!~#value";

        let (reader_1, indexes_1) =
            write_test_vlog_reader(dir.path(), 1, 1, &[(key_a, 1, 1.0), (key_b, 2, 2.0)]);
        let (reader_2, indexes_2) =
            write_test_vlog_reader(dir.path(), 2, 1, &[(key_a, 3, 3.0), (key_c, 4, 4.0)]);

        let mut indexes_by_key = indexes_1;
        for (key, mut indexes) in indexes_2 {
            indexes_by_key.entry(key).or_default().append(&mut indexes);
        }

        let keys = vec![
            (key_a.to_vec(), FLOAT),
            (key_b.to_vec(), FLOAT),
            (key_c.to_vec(), FLOAT),
        ];
        let indexes = keys
            .iter()
            .map(|(key, _)| {
                indexes_by_key
                    .get(&to_string!(key.clone()))
                    .cloned()
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>();
        let compact_info = VLogCompactInfo {
            keys,
            readers: vec![reader_1, reader_2],
            indexes,
            files: vec![],
            aggregates: None,
        };
        let mut iter =
            futures::executor::block_on(VLogBatchKeyIter::new(1, true, None, compact_info))
                .unwrap();

        assert!(
            iter.prefetched_blocks.is_some(),
            "lazy compaction should prefetch selected blocks once"
        );
        assert_eq!(
            iter.total_spawned_tasks, 2,
            "lazy prefetch should spawn one task per selected reader"
        );

        let mut produced = Vec::new();
        while let Some((_count, _typ, key, min, max, _block, err)) = iter.next() {
            assert!(err.is_none(), "unexpected iterator error: {err:?}");
            if key.is_empty() {
                continue;
            }
            produced.push(((*key).clone(), min, max));
        }
        produced.sort();

        assert_eq!(
            produced,
            vec![
                (key_a.to_vec(), 1, 1),
                (key_a.to_vec(), 3, 3),
                (key_b.to_vec(), 2, 2),
                (key_c.to_vec(), 4, 4),
            ]
        );
    }

    #[test]
    fn non_lazy_vlog_compaction_completes_with_one_compact_blocking_worker() {
        let dir = tempfile::tempdir().unwrap();
        let compact_info = compact_info_fixture(dir.path(), 3);
        let (_files, indexes) = run_real_compaction(default_executor(1), compact_info, false);
        assert_eq!(indexes.values().map(Vec::len).sum::<usize>(), 3);
    }

    #[test]
    fn lazy_vlog_compaction_completes_with_one_compact_blocking_worker() {
        let dir = tempfile::tempdir().unwrap();
        let compact_info = compact_info_fixture(dir.path(), 3);
        let (_files, indexes) = run_real_compaction(default_executor(1), compact_info, true);
        assert_eq!(indexes.values().map(Vec::len).sum::<usize>(), 3);
    }

    #[test]
    fn concurrent_vlog_compaction_parents_do_not_starve_shared_lane() {
        let first_dir = tempfile::tempdir().unwrap();
        let second_dir = tempfile::tempdir().unwrap();
        let first_info = compact_info_fixture(first_dir.path(), 3);
        let second_info = compact_info_fixture(second_dir.path(), 3);
        let runtime = default_executor(1);
        let executor: ExecutorRef = runtime.clone();
        let first_compactor = test_compactor(first_dir.path(), Arc::clone(&executor));
        let second_compactor = test_compactor(second_dir.path(), Arc::clone(&executor));

        let first_executor = Arc::clone(&executor);
        let first_files = first_info.files.clone();
        let first_finish = Arc::clone(&first_compactor);
        let first = executor
            .spawn_compact(async move {
                let prepared = first_compactor
                    .prepare_vlog_compaction(true, first_files, first_info, false)
                    .await
                    .unwrap();
                first_executor
                    .spawn_blocking_compact(move || first_finish.finish_vlog_compaction(prepared))
                    .unwrap()
                    .await
                    .unwrap()
                    .unwrap();
            })
            .unwrap();

        let second_executor = Arc::clone(&executor);
        let second_files = second_info.files.clone();
        let second_finish = Arc::clone(&second_compactor);
        let second = executor
            .spawn_compact(async move {
                let prepared = second_compactor
                    .prepare_vlog_compaction(true, second_files, second_info, false)
                    .await
                    .unwrap();
                second_executor
                    .spawn_blocking_compact(move || second_finish.finish_vlog_compaction(prepared))
                    .unwrap()
                    .await
                    .unwrap()
                    .unwrap();
            })
            .unwrap();

        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(10), async {
                first.await.unwrap();
                second.await.unwrap();
            })
            .await
            .expect("concurrent VLog compactions starved the shared compact lane");
        });
    }

    #[test]
    fn vlog_read_fanout_remains_parallel_when_capacity_permits() {
        let dir = tempfile::tempdir().unwrap();
        let compact_info = compact_info_fixture(dir.path(), 3);
        let runtime = default_executor(3);
        let executor: ExecutorRef = runtime.clone();
        let hook = Arc::new(VLogReadTestHook::default());
        hook.delay_millis.store(100, Ordering::SeqCst);

        let iter = runtime.block_on(VLogBatchKeyIter::new_with_executor(
            1,
            true,
            None,
            compact_info,
            executor,
            Some(Arc::clone(&hook)),
        ));
        assert!(iter.is_ok());
        assert_eq!(hook.started.load(Ordering::SeqCst), 3);
        assert!(hook.max_active.load(Ordering::SeqCst) >= 2);
    }

    #[test]
    fn selective_vlog_read_refusal_terminates_siblings_and_retry_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let compact_info = compact_info_fixture(dir.path(), 3);
        let executor = Arc::new(SelectiveCompactExecutor::new());
        executor.reject_submission.store(2, Ordering::SeqCst);
        let executor_ref: ExecutorRef = executor.clone();

        let result = futures::executor::block_on(VLogBatchKeyIter::new_with_executor(
            1,
            true,
            None,
            compact_info.clone(),
            Arc::clone(&executor_ref),
            None,
        ));
        assert!(matches!(result, Err(Error::Execution { .. })));

        executor.reset();
        let retry = futures::executor::block_on(VLogBatchKeyIter::new_with_executor(
            1,
            true,
            None,
            compact_info,
            executor_ref,
            None,
        ));
        assert!(retry.is_ok());
    }

    #[test]
    fn accepted_vlog_read_disposal_is_observed() {
        let dir = tempfile::tempdir().unwrap();
        let compact_info = compact_info_fixture(dir.path(), 2);
        let executor = Arc::new(SelectiveCompactExecutor::new());
        executor.dispose_submission.store(1, Ordering::SeqCst);
        let executor_ref: ExecutorRef = executor;

        let result = futures::executor::block_on(VLogBatchKeyIter::new_with_executor(
            1,
            true,
            None,
            compact_info,
            executor_ref,
            None,
        ));
        assert!(matches!(result, Err(Error::ReadTaskTerminated { .. })));
    }

    #[test]
    fn vlog_read_production_body_panic_is_observed() {
        let dir = tempfile::tempdir().unwrap();
        let compact_info = compact_info_fixture(dir.path(), 2);
        let runtime = default_executor(2);
        let executor: ExecutorRef = runtime.clone();
        let hook = Arc::new(VLogReadTestHook::default());
        hook.panic_once.store(true, Ordering::SeqCst);

        let result = runtime.block_on(VLogBatchKeyIter::new_with_executor(
            1,
            true,
            None,
            compact_info,
            executor,
            Some(hook),
        ));
        assert!(matches!(result, Err(Error::ReadTaskTerminated { .. })));
    }

    #[test]
    fn later_vlog_apply_refusal_releases_file_ownership_for_retry() {
        let dir = tempfile::tempdir().unwrap();
        let compact_info = compact_info_fixture(dir.path(), 2);
        let executor = Arc::new(SelectiveCompactExecutor::new());
        executor.reject_submission.store(3, Ordering::SeqCst);
        let executor_ref: ExecutorRef = executor.clone();
        let compactor = test_compactor(dir.path(), Arc::clone(&executor_ref));

        let prepared = futures::executor::block_on(compactor.prepare_vlog_compaction(
            true,
            compact_info.files.clone(),
            compact_info.clone(),
            false,
        ))
        .unwrap();
        let finish_compactor = Arc::clone(&compactor);
        let error = executor_ref
            .spawn_blocking_compact(move || finish_compactor.finish_vlog_compaction(prepared))
            .unwrap_err();
        assert_eq!(error.reason(), "injected VLog read refusal");

        executor.reset();
        let retry = futures::executor::block_on(compactor.prepare_vlog_compaction(
            true,
            compact_info.files.clone(),
            compact_info,
            false,
        ));
        assert!(retry.is_ok(), "rejected apply leaked source-file ownership");
    }

    #[test]
    fn vlog_write_failure_after_reads_releases_file_ownership_for_retry() {
        let dir = tempfile::tempdir().unwrap();
        let compact_info = compact_info_fixture(dir.path(), 2);
        let runtime = default_executor(1);
        let executor: ExecutorRef = runtime.clone();
        let output_dir = dir.path().join("output");
        let compactor =
            test_compactor_with_output_dir(dir.path(), &output_dir, Arc::clone(&executor));

        let prepared = runtime
            .block_on(compactor.prepare_vlog_compaction(
                true,
                compact_info.files.clone(),
                compact_info.clone(),
                false,
            ))
            .unwrap();
        let finish_compactor = Arc::clone(&compactor);
        let result = runtime
            .block_on(
                executor
                    .spawn_blocking_compact(move || {
                        finish_compactor.finish_vlog_compaction(prepared)
                    })
                    .unwrap(),
            )
            .unwrap();
        assert!(
            result.is_err(),
            "missing output directory should fail write"
        );

        fs::create_dir(&output_dir).unwrap();
        let retry = runtime
            .block_on(compactor.prepare_vlog_compaction(
                true,
                compact_info.files.clone(),
                compact_info,
                false,
            ))
            .expect("write failure leaked source-file ownership");
        let finish_compactor = Arc::clone(&compactor);
        let retry_result = runtime
            .block_on(
                executor
                    .spawn_blocking_compact(move || finish_compactor.finish_vlog_compaction(retry))
                    .unwrap(),
            )
            .unwrap();
        assert!(retry_result.is_ok());
    }

    #[test]
    fn vlog_read_cancellation_terminates_outstanding_children() {
        let dir = tempfile::tempdir().unwrap();
        let compact_info = compact_info_fixture(dir.path(), 3);
        let runtime = default_executor(1);
        let executor: ExecutorRef = runtime.clone();
        let compactor = test_compactor(dir.path(), executor);
        let hook = Arc::new(VLogReadTestHook::default());
        hook.delay_millis.store(100, Ordering::SeqCst);
        compactor.set_read_test_hook(Some(Arc::clone(&hook)));
        let files = compact_info.files.clone();
        let prepare_compactor = Arc::clone(&compactor);
        let task = compactor
            .executor()
            .spawn_compact(async move {
                prepare_compactor
                    .prepare_vlog_compaction(true, files, compact_info, false)
                    .await
            })
            .unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while hook.started.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(hook.started.load(Ordering::SeqCst) > 0);
        compactor.disable_vlog_compactions();

        let result = runtime
            .block_on(async { tokio::time::timeout(Duration::from_secs(5), task).await })
            .expect("cancelled VLog reads did not terminate")
            .unwrap();
        assert!(matches!(result, Err(Error::CompactionAborted { .. })));
    }

    #[test]
    fn vlog_parent_abort_preserves_reader_lease_until_child_terminal() {
        let dir = tempfile::tempdir().unwrap();
        let compact_info = compact_info_fixture(dir.path(), 3);
        let first_reader = Arc::clone(&compact_info.readers[0]);
        let runtime = default_executor(1);
        let executor: ExecutorRef = runtime.clone();
        let compactor = test_compactor(dir.path(), executor);
        let hook = Arc::new(VLogReadTestHook::default());
        hook.delay_millis.store(250, Ordering::SeqCst);
        compactor.set_read_test_hook(Some(Arc::clone(&hook)));
        let files = compact_info.files.clone();
        let prepare_compactor = Arc::clone(&compactor);
        let task = compactor
            .executor()
            .spawn_compact(async move {
                prepare_compactor
                    .prepare_vlog_compaction(true, files, compact_info, false)
                    .await
            })
            .unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while hook.started.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(hook.started.load(Ordering::SeqCst) > 0);
        task.abort();
        assert!(
            runtime.block_on(task).is_err(),
            "aborted VLog parent unexpectedly completed"
        );
        assert!(
            first_reader.in_use(),
            "running read child lost its source-reader lease"
        );

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while first_reader.in_use() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            !first_reader.in_use(),
            "aborted VLog child did not release its source-reader lease"
        );
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

#[path = "compact_aggregate.rs"]
pub(crate) mod aggregate;
