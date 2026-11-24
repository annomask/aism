//! Time Series Index.

use std::{
    collections::{HashSet, VecDeque},
    hash::Hasher,
    io::Read,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
};

use crate::common::hll::HLL;
use crate::models::points::Tags;
use crate::runtime::execution::{
    ExecutorRef, RuntimeExecutorExt, SpawnError, TaskAdmissionGate, accepted_task_guard,
    legacy_global_executor,
};
use parking_lot::{Mutex, RwLock};
use snafu::{ResultExt, Snafu, ensure};
use twox_hash::XxHash64;

use crate::{
    common::slow_trace,
    index::tsi1::{
        cache::TagValueSeriesIDCache,
        file_set::FileSet,
        partition::{self, Partition},
    },
    series::{
        measurement_fields::MeasurementFieldSet,
        series_file::SeriesFile,
        series_set::{self, SeriesIDSet},
    },
};

#[cfg(feature = "slow-trace")]
use crate::index::{MeasurementTagKeysDiagnostics, TsiSelectionDiagnostics};
#[cfg(feature = "slow-trace")]
use crate::runtime::execution::{GeneralBlockingTaskClass, GeneralBlockingTaskDiagnostics};

// IndexName is the name of the index.
pub(crate) const INDEX_NAME: &str = crate::index::TSI1_INDEX_NAME;

// DefaultPartitionN determines how many shards the index will be partitioned into.
//
// NOTE: Currently, this must not be change once a database is created. Further,
// it must also be a power of 2.
//
pub(crate) const DEFAULT_PARTITION_N: u64 = 8;

/// Error type for Index.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    /// IO error.
    #[snafu(display("io error: {}", source))]
    IOError { source: std::io::Error },

    /// Compaction Interrupted.
    #[snafu(display("tsi1: compaction interrupted"))]
    CompactionInterrupted,

    /// Index partition error.
    #[snafu(display("{}", source))]
    Partition {
        source: crate::index::tsi1::partition::Error,
    },

    /// File set error.
    #[snafu(display("{}", source))]
    FileSet {
        source: crate::index::tsi1::file_set::Error,
    },

    /// HLL error.
    #[snafu(display("{}", source))]
    HLL { source: crate::common::hll::Error },

    /// String error.
    #[snafu(display("tsdb: {}", reason))]
    StringErr { reason: String },

    #[snafu(display("tsi1: task scheduling failed: {}", source))]
    Execution { source: SpawnError },

    #[snafu(display("tsi1: accepted partition task did not complete"))]
    TaskTerminated,
}

/// Result type for Index.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IOError { source: err }
    }
}

struct LRUSeriesKeys {
    keys: Vec<String>,
    capacity: usize,
    index: usize,
}

impl LRUSeriesKeys {
    pub fn with_capacity(cap: usize) -> Self {
        LRUSeriesKeys {
            keys: Vec::with_capacity(cap),
            capacity: cap,
            index: 0,
        }
    }

    pub fn add_keys(&mut self, keys: &[String]) {
        for key in keys {
            if self.keys.len() < self.capacity {
                self.keys.push(key.clone());
            } else {
                self.index = 0;
                self.keys[self.index] = key.clone();
            }
            self.index += 1;
        }
    }

    pub fn keys(&self) -> Vec<String> {
        self.keys.clone()
    }

    pub fn key_exists(&self, key: &str) -> bool {
        self.keys.iter().any(|k| k == key)
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_full(&self) -> bool {
        self.keys.len() == self.capacity
    }

    pub fn is_empty(&self) -> bool {
        self.keys.len() == 0
    }
}

pub struct Index {
    executor: ExecutorRef,
    tag_value_cache: Arc<RwLock<TagValueSeriesIDCache>>,
    tag_value_cache_size: i64,

    lru_series_keys: RwLock<LRUSeriesKeys>,

    // The following may be set when initializing an Index.
    path: PathBuf,             // Root directory of the index partitions.
    disable_compactions: bool, // Initially disables compactions on the index.
    max_log_file_size: i64,    // Maximum size of a LogFile before it's compacted.
    logfile_buffer_size: i64,  // The size of the buffer used by the LogFile.
    disable_fsync: bool, // Disables flushing buffers and fsyning files. Used when working with indexes offline.

    // The following must be set when initializing an Index.
    sfile: Arc<SeriesFile>, // series lookup file
    database: String,       // Name of database.

    // Index's version.
    version: i32,

    // Number of partitions used by the index.
    pub partition_n: u64,

    // FIXME:下面的 partitinos应该和m_sketch, s_sketch放到同一个mutex中
    // // partitions: Arc<RwLock<Vec<Partition>>>,
    // partitions: Vec<Arc<RwLock<Partition>>>,
    // m_sketch: RwLock<MSketch>,
    // s_sketch: RwLock<SSketch>,
    // opened: Mutex<bool>,
    inner: RwLock<Inner>,
}

#[derive(Default)]
struct Inner {
    opened: bool,

    partitions: Vec<Arc<RwLock<Partition>>>,

    m_sketch: HLL,  // measurement sketch
    mt_sketch: HLL, // measurement tombstone sketch
    s_sketch: HLL,  // series sketch
    st_sketch: HLL, // series tombstone sketch
}

// #[derive(Default)]
// struct MSketch {
//     m_sketch: HLL,  // measurement sketch
//     mt_sketch: HLL, // measurement tombstone sketch
// }
// #[derive(Default)]
// struct SSketch {
//     s_sketch: HLL,  // series sketch
//     st_sketch: HLL, // series tombstone sketch
// }
use std::io::BufRead;

type PartitionTask<T> = Box<dyn FnOnce() -> Result<T> + Send + 'static>;

impl Index {
    fn run_required_partition_tasks<T>(&self, tasks: Vec<PartitionTask<T>>) -> Result<Vec<T>>
    where
        T: Send + 'static,
    {
        if tasks.is_empty() {
            return Ok(Vec::new());
        }

        let task_count = tasks.len();
        let admission = TaskAdmissionGate::pending();
        let (sender, receiver) = crossbeam_channel::bounded::<(usize, Result<T>)>(task_count);
        let mut submitted = 0usize;
        let mut scheduling_error = None;

        for (task_index, task) in tasks.into_iter().enumerate() {
            let task_sender = sender.clone();
            let cancelled = sender.clone();
            let rejected = sender.clone();
            let task_admission = admission.clone();
            let cancelled_admission = admission.clone();
            let (registration, completion) = accepted_task_guard(move || {
                cancelled_admission.cancel();
                let _ = cancelled.send((task_index, Err(Error::TaskTerminated)));
            });
            submitted += 1;

            match self.executor.spawn_blocking(move || {
                if !task_admission.wait() {
                    completion.complete_with(|| {
                        let _ = task_sender.send((task_index, Err(Error::TaskTerminated)));
                    });
                    return;
                }
                let result = task();
                completion.complete_with(|| {
                    let _ = task_sender.send((task_index, result));
                });
            }) {
                Ok(_) => registration.accepted(),
                Err(source) => {
                    let _ = rejected.send((
                        task_index,
                        Err(Error::Execution {
                            source: source.clone(),
                        }),
                    ));
                    registration.rejected();
                    scheduling_error = Some(source);
                    admission.cancel();
                    break;
                }
            }
        }

        if scheduling_error.is_none() {
            admission.run();
        }

        let mut results = (0..task_count).map(|_| None).collect::<Vec<Option<T>>>();
        let mut task_error = None;
        for _ in 0..submitted {
            match receiver.recv() {
                Ok((task_index, Ok(value))) => results[task_index] = Some(value),
                Ok((_, Err(error))) if task_error.is_none() => task_error = Some(error),
                Ok(_) => {}
                Err(_) if task_error.is_none() => task_error = Some(Error::TaskTerminated),
                Err(_) => {}
            }
        }

        if let Some(source) = scheduling_error {
            return Err(Error::Execution { source });
        }
        if let Some(error) = task_error {
            return Err(error);
        }
        Ok(results
            .into_iter()
            .map(|result| result.expect("successful partition task must publish a result"))
            .collect())
    }

    #[cfg(feature = "slow-trace")]
    fn run_required_partition_tasks_profiled<T>(
        &self,
        tasks: Vec<PartitionTask<T>>,
        diagnostics: Arc<MeasurementTagKeysDiagnostics>,
    ) -> Result<Vec<T>>
    where
        T: Send + 'static,
    {
        if tasks.is_empty() {
            return Ok(Vec::new());
        }

        let task_count = tasks.len();
        let admission = TaskAdmissionGate::pending();
        let (sender, receiver) = crossbeam_channel::bounded::<(usize, Result<T>)>(task_count);
        let mut submitted = 0usize;
        let mut scheduling_error = None;

        for (task_index, task) in tasks.into_iter().enumerate() {
            let task_sender = sender.clone();
            let cancelled = sender.clone();
            let rejected = sender.clone();
            let task_admission = admission.clone();
            let cancelled_admission = admission.clone();
            let task_diagnostics = Arc::clone(&diagnostics);
            let submitted_at = std::time::Instant::now();
            let blocking_diagnostics = Arc::new(GeneralBlockingTaskDiagnostics::new(
                GeneralBlockingTaskClass::MeasurementTagKeysPartition,
            ));
            diagnostics.record_general_blocking_task(task_index, Arc::clone(&blocking_diagnostics));
            let (registration, completion) = accepted_task_guard(move || {
                cancelled_admission.cancel();
                let _ = cancelled.send((task_index, Err(Error::TaskTerminated)));
            });
            submitted += 1;

            match self.executor.spawn_blocking_with_class(
                GeneralBlockingTaskClass::MeasurementTagKeysPartition,
                blocking_diagnostics,
                move || {
                    task_diagnostics.record_task_started(submitted_at);
                    if !task_admission.wait() {
                        completion.complete_with(|| {
                            let _ = task_sender.send((task_index, Err(Error::TaskTerminated)));
                        });
                        return;
                    }
                    let execution_started = std::time::Instant::now();
                    let result = task();
                    task_diagnostics.record_task_completed(execution_started);
                    completion.complete_with(|| {
                        let _ = task_sender.send((task_index, result));
                    });
                },
            ) {
                Ok(_) => {
                    diagnostics.record_task_submitted();
                    registration.accepted();
                }
                Err(source) => {
                    let _ = rejected.send((
                        task_index,
                        Err(Error::Execution {
                            source: source.clone(),
                        }),
                    ));
                    registration.rejected();
                    scheduling_error = Some(source);
                    admission.cancel();
                    break;
                }
            }
        }

        if scheduling_error.is_none() {
            admission.run();
        }

        let caller_wait_started = std::time::Instant::now();
        let mut results = (0..task_count).map(|_| None).collect::<Vec<Option<T>>>();
        let mut task_error = None;
        for _ in 0..submitted {
            match receiver.recv() {
                Ok((task_index, Ok(value))) => results[task_index] = Some(value),
                Ok((_, Err(error))) if task_error.is_none() => task_error = Some(error),
                Ok(_) => {}
                Err(_) if task_error.is_none() => task_error = Some(Error::TaskTerminated),
                Err(_) => {}
            }
        }
        diagnostics.record_caller_wait_all(caller_wait_started);

        if let Some(source) = scheduling_error {
            return Err(Error::Execution { source });
        }
        if let Some(error) = task_error {
            return Err(error);
        }
        Ok(results
            .into_iter()
            .map(|result| result.expect("successful partition task must publish a result"))
            .collect())
    }

    pub fn new(
        sfile: Arc<SeriesFile>,
        database: String,
        path: impl Into<PathBuf>,
        max_index_log_file_size: i64,
        series_idset_cache_size: i64,
    ) -> Self {
        Self::new_with_executor(
            sfile,
            database,
            path,
            max_index_log_file_size,
            series_idset_cache_size,
            legacy_global_executor(),
        )
    }

    pub fn new_with_executor(
        sfile: Arc<SeriesFile>,
        database: String,
        path: impl Into<PathBuf>,
        max_index_log_file_size: i64,
        series_idset_cache_size: i64,
        executor: ExecutorRef,
    ) -> Self {
        // ////////////////////////////////////////////
        // ////////////////////////////////////////////
        // ////////////////////////////////////////////
        // let n = 1000; // 10%
        // let n = 2500;  // 25%
        // let n = 5000;  // 50%
        // let n = 7500;  // 75%
        // let n = 10000;  // 100%

        // let file = std::fs::File::open("testdata/series_keys.txt")
        //     .expect("open file failed");
        // let reader = std::io::BufReader::new(file);
        // let mut keys = Vec::new();

        // for (i, line) in reader.lines().take(n).enumerate() {
        //     match line {
        //         Ok(content) => keys.push(content),
        //         Err(e) => {}
        //     }
        // }

        // let mut series_keys = LRUSeriesKeys::with_capacity(10000);
        // series_keys.add_keys(&keys);

        // ////////////////////////////////////////////
        // ////////////////////////////////////////////
        // ////////////////////////////////////////////

        Self {
            executor,
            tag_value_cache: Arc::new(RwLock::new(TagValueSeriesIDCache::new(
                series_idset_cache_size,
            ))),
            tag_value_cache_size: series_idset_cache_size,

            lru_series_keys: RwLock::new(LRUSeriesKeys::with_capacity(1000)),
            // lru_series_keys: RwLock::new(series_keys),
            path: path.into(),
            disable_compactions: false,
            max_log_file_size: max_index_log_file_size,
            logfile_buffer_size: 0i64,
            disable_fsync: false,
            sfile,
            database,
            version: partition::VERSION,
            partition_n: DEFAULT_PARTITION_N,

            inner: RwLock::new(Inner::default()),
        }
    }

    // Estimates the memory footprint of this Index, in bytes.
    pub fn bytes(&self) -> i64 {
        todo!("implement me");
    }

    ///  Returns the name of the database the index was initialized with.
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Returns the type of Index this is.
    pub fn index_type(&self) -> &str {
        INDEX_NAME
    }

    /// Returns the series file attached to the index.
    pub fn series_file(&self) -> Arc<SeriesFile> {
        Arc::clone(&self.sfile)
    }

    /// Returns the set of series ids associated with series in this
    /// index. Any series IDs for series no longer present in the index are filtered out.
    pub fn series_id_set(&self) -> series_set::SeriesIDSet {
        let ss = series_set::SeriesIDSet::new(&[]);
        let mut others = Vec::with_capacity(self.partition_n as usize);
        let lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait(
            "index.read",
            lock_start,
            "stage=query_wait_for_index_lock component=measurement_exists",
        );
        for p in inner.partitions.iter() {
            let pss = Arc::clone(&p.read().inner.read().series_id_set);
            others.push(pss);
        }

        let others = others
            .iter()
            .map(|ss| &**ss)
            .collect::<Vec<&series_set::SeriesIDSet>>();
        ss.merge(others.as_ref());
        ss
    }

    /// Open the index.
    pub fn open(&self) -> Result<()> {
        let started = std::time::Instant::now();
        tracing::debug!(
            target: "reefsdb::tsdb::index",
            database_id = %self.database,
            path = %self.path.display(),
            "index opening"
        );
        let result = self.open_inner();
        match &result {
            Ok(()) => tracing::info!(
                target: "reefsdb::tsdb::index",
                database_id = %self.database,
                path = %self.path.display(),
                partition_count = self.partition_n,
                duration_ms = started.elapsed().as_millis() as u64,
                "index opened"
            ),
            Err(error) => tracing::error!(
                target: "reefsdb::tsdb::index",
                database_id = %self.database,
                path = %self.path.display(),
                error = %error,
                "index open failed"
            ),
        }
        result
    }

    fn open_inner(&self) -> Result<()> {
        let mut inner = self.inner.write();

        if inner.opened {
            return Err(Error::StringErr {
                reason: "index already open".to_string(),
            });
        }

        // Ensure root exists.
        std::fs::create_dir_all(&*self.path)?;
        std::fs::metadata(&self.path)?.permissions().set_mode(0o777);

        // Initialize index partitions locally so a refused open cannot publish
        // a partially initialized partition set.
        let mut partitions = Vec::with_capacity(self.partition_n as usize);
        for i in 0..self.partition_n {
            let mut p = Partition::new_with_executor(
                Arc::clone(&self.sfile),
                self.path.join(i.to_string()),
                Arc::clone(&self.executor),
            );
            p.max_log_file_size = self.max_log_file_size;
            p.nosync = self.disable_fsync;
            p.logbuffer_size = self.logfile_buffer_size;
            // p.logger = i.logger.With(zap.String("tsi1_partition", fmt.Sprint(j+1)))
            partitions.push(Arc::new(RwLock::new(p)));
        }

        let tasks = partitions
            .iter()
            .cloned()
            .map(|partition| {
                Box::new(move || partition.write().open().context(PartitionSnafu))
                    as PartitionTask<()>
            })
            .collect();
        self.run_required_partition_tasks(tasks)?;
        inner.partitions = partitions;

        // Refresh cached sketches.
        self.update_series_sketches(&mut inner)?;
        self.update_measurement_sketches(&mut inner)?;

        // Mark opened.
        inner.opened = true;

        Ok(())
    }

    pub fn enable_compactions(&self) {
        let inner = self.inner.read();
        for p in inner.partitions.iter() {
            let p = p.write();
            p.enable_compactions();
        }
    }

    pub fn disable_compactions(&self) {
        let inner = self.inner.read();
        for p in inner.partitions.iter() {
            let mut p = p.write();
            p.disable_compactions();
        }
    }

    /// Blocks until all outstanding compactions have completed.
    pub fn wait(&self) {
        let inner = self.inner.read();
        for p in inner.partitions.iter() {
            let p = p.read(); // FIXME: read or write
            p.wait();
        }
    }

    /// Close the index.
    pub fn close(&self) -> Result<()> {
        tracing::debug!(
            target: "reefsdb::tsdb::index",
            database_id = %self.database,
            path = %self.path.display(),
            "index closing"
        );
        // Lock index and close partitions.
        let mut inner = self.inner.write();
        for p in inner.partitions.iter() {
            let mut p = p.write();
            p.close().context(PartitionSnafu)?;
        }

        // Mark index as closed.
        inner.opened = false;
        drop(inner);

        tracing::info!(
            target: "reefsdb::tsdb::index",
            database_id = %self.database,
            path = %self.path.display(),
            "index closed"
        );

        Ok(())
    }

    /// Returns the path the index was opened with.
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[cfg(any(test, feature = "benchmarking"))]
    /// Returns the partition by index.
    pub fn partition_at(&self, index: usize) -> Arc<RwLock<Partition>> {
        let inner = self.inner.read();
        Arc::clone(&inner.partitions[index])
    }

    /// Returns the appropriate Partition for a provided series key.
    fn partition(&self, key: &[u8]) -> Arc<RwLock<Partition>> {
        let mut hasher = XxHash64::default();
        hasher.write(key);

        let inner = self.inner.read();
        Arc::clone(&inner.partitions[((hasher.finish()) & (self.partition_n - 1)) as usize])
    }

    /// Returns the index of the partition that key belongs in.
    fn partition_idx(&self, key: &[u8]) -> u64 {
        let mut hasher = XxHash64::default();
        hasher.write(key);
        (hasher.finish()) & (self.partition_n - 1)
    }

    /// Rebuilds the cached measurement sketches.
    fn update_measurement_sketches(&self, inner: &mut Inner) -> Result<()> {
        for p in inner.partitions.iter() {
            let (s, t) = p.read().measurement_sketches().context(PartitionSnafu)?;
            inner.m_sketch.merge(Some(&s)).context(HLLSnafu)?;
            inner.mt_sketch.merge(Some(&s)).context(HLLSnafu)?;
        }
        Ok(())
    }

    /// Rebuilds the cached series sketches.
    fn update_series_sketches(&self, inner: &mut Inner) -> Result<()> {
        for p in inner.partitions.iter() {
            let (s, t) = p.read().series_sketches().context(PartitionSnafu)?;
            inner.s_sketch.merge(Some(&s)).context(HLLSnafu)?;
            inner.st_sketch.merge(Some(&s)).context(HLLSnafu)?;
        }
        Ok(())
    }

    /// Sets a shared field set from the engine.
    pub fn set_field_set(&self, fs: Arc<MeasurementFieldSet>) {
        let inner = self.inner.read();
        for p in inner.partitions.iter() {
            p.write().set_field_set(Arc::clone(&fs));
        }
    }

    /// Returns the assigned fieldset.
    pub fn field_set(&self) -> Option<Arc<MeasurementFieldSet>> {
        let inner = self.inner.read();
        if inner.partitions.is_empty() {
            return None;
        }

        let p = inner.partitions[0].read();
        Some(p.field_set())
    }

    pub fn field_keys(&self, measurement: &[u8]) -> Vec<String> {
        let inner = self.inner.read();
        if inner.partitions.is_empty() {
            return vec![];
        }

        let p = inner.partitions[0].read();
        let fs = p.field_set();
        if let Some(fields) = fs.fields(measurement) {
            return fields.field_keys();
        }

        vec![]
    }

    /// for_each_measurement_name iterates over all measurement names in the index,
    /// applying fn. It returns the first error encountered, if any.
    ///
    /// for_each_measurement_name does not call fn on each partition concurrently so the
    /// call may provide a non-goroutine safe fn.
    pub fn for_each_measurement_name<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]),
    {
        let itr = self.measurement_iter()?;
        if itr.is_none() {
            return Ok(());
        }

        let mut itr = itr.unwrap();

        // Iterate over all measurements.
        while let Some(e) = itr.next() {
            // if let Err(e) = f(&e) {
            //     let _ = itr.close();
            //     return Err(e);
            // }
            f(&e)
        }
        // for e in itr {
        //     let e = &*e;
        //     f(&e)?;
        // }

        let _ = itr.close();

        Ok(())
    }

    /// Returns true if a measurement exists.
    pub fn measurement_exists(&self, name: &[u8]) -> Result<bool> {
        let measurement_exists_total_start = slow_trace::now();

        let index_lock_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait(
            "index.read",
            index_lock_start,
            "stage=query_wait_for_index_lock component=measurement_exists",
        );
        let partitions = inner.partitions.clone();
        drop(inner);
        slow_trace::log_query_stage(
            "query_index_lookup",
            index_lock_start,
            "component=measurement_exists_index_read",
        );

        let partition_scan_start = slow_trace::now();
        for partition in partitions {
            let lock_start = slow_trace::now();
            let partition = partition.read();
            slow_trace::log_lock_wait(
                "index_partition.read",
                lock_start,
                "stage=query_wait_for_index_lock component=measurement_exists",
            );

            let partition_lookup_start = slow_trace::now();
            let exists = partition
                .measurement_exists(name)
                .map_err(|e| crate::index::tsi1::index::Error::Partition { source: e })?;
            slow_trace::log_query_stage(
                "query_index_lookup",
                partition_lookup_start,
                "component=measurement_exists_partition",
            );
            if exists {
                slow_trace::log_query_stage(
                    "query_index_lookup",
                    partition_scan_start,
                    "component=measurement_exists_partition_scan",
                );
                slow_trace::log_query_stage(
                    "query_index_lookup",
                    measurement_exists_total_start,
                    "component=measurement_exists_total",
                );
                return Ok(true);
            }
        }

        slow_trace::log_query_stage(
            "query_index_lookup",
            partition_scan_start,
            "component=measurement_exists_partition_scan",
        );
        slow_trace::log_query_stage(
            "query_index_lookup",
            measurement_exists_total_start,
            "component=measurement_exists_total",
        );

        Ok(false)
    }

    /// Returns true if a measurement has non-tombstoned series.
    pub fn measurement_has_series(&self, name: &[u8]) -> Result<bool> {
        let lock_start = slow_trace::now();
        let partitions = { self.inner.read().partitions.clone() };
        slow_trace::log_lock_wait("index.read", lock_start, "stage=write_index_create_series");
        for p in &partitions {
            if p.read()
                .measurement_has_series(name)
                .context(PartitionSnafu)?
            {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Returns an iterator over all measurements in all index partitions.
    pub fn measurement_iter(&self) -> Result<Option<crate::index::MeasurementMergeIter>> {
        let inner = self.inner.read();
        let mut itrs = Vec::with_capacity(inner.partitions.len());
        for p in &inner.partitions {
            match p.read().measurement_iter().context(PartitionSnafu) {
                Ok(itr) => {
                    if let Some(itr) = itr {
                        itrs.push(itr);
                    }
                }
                Err(e) => {
                    for itr in itrs {
                        itr.close().context(FileSetSnafu)?;
                    }
                    return Err(e);
                }
            }
        }

        Ok(crate::index::merge_measurement_iters(itrs))
    }

    /// Returns an iterator over all series in a measurement.
    pub fn measurement_series_id_iter(
        &self,
        name: &[u8],
    ) -> Result<Option<crate::index::FileSetSeriesIDIter>> {
        let partitions = {
            let start = slow_trace::now();
            let inner = self.inner.read();
            slow_trace::log_lock_wait("index.read", start, "stage=query_index_lookup");
            inner.partitions.clone()
        };
        let mut itrs = Vec::with_capacity(partitions.len());
        for p in &partitions {
            let start = slow_trace::now();
            let partition = p.read();
            slow_trace::log_lock_wait("index_partition.read", start, "stage=query_index_lookup");
            if let Ok(Some(itr)) = partition.measurement_series_id_iter(name) {
                itrs.push(itr);
            }
        }

        Ok(crate::index::merge_file_set_series_id_iters(itrs))
    }

    /// Deletes a measurement from the index. It returns the first error encountered, if any.
    pub fn drop_measurement(&self, name: &[u8]) -> Result<()> {
        let inner = self.inner.read();
        let partitions = inner.partitions.clone();
        drop(inner);
        let name = name.to_vec();
        let tasks = partitions
            .into_iter()
            .map(|partition| {
                let name = name.clone();
                Box::new(move || {
                    partition
                        .read()
                        .drop_measurement(name.as_slice())
                        .context(PartitionSnafu)
                }) as PartitionTask<()>
            })
            .collect();
        self.run_required_partition_tasks(tasks)?;

        self.tag_value_cache
            .write()
            .remove_measurement(name.as_slice());

        let mut inner = self.inner.write();
        // Update sketches under lock.
        inner.mt_sketch.add(name.as_slice());

        self.update_series_sketches(&mut inner)
    }

    /// Creates a list of series if they doesn't exist in bulk.
    pub fn create_series_list_if_not_exists(
        &self,
        keys: &[&[u8]],
        names: Vec<Vec<u8>>,
        tags_slice: Vec<crate::models::points::Tags>,
    ) -> Result<()> {
        // All slices must be of equal length.
        ensure!(
            names.len() == tags_slice.len(),
            StringErrSnafu {
                reason: "names/tags length mismatch in index".to_string(),
            }
        );

        // We need to move different series into collections for each partition to process.
        let mut pkeys = vec![vec![]; self.partition_n as usize];
        let mut pnames = vec![vec![]; self.partition_n as usize];
        let mut ptags = vec![vec![]; self.partition_n as usize];

        // Determine partition for series using each series key.
        for (ki, key) in keys.iter().enumerate() {
            let pidx = self.partition_idx(key);
            pkeys[pidx as usize].push(key.to_vec()); // FIXME: don't clone
            pnames[pidx as usize].push(names[ki].clone()); // FIXME: don't clone
            ptags[pidx as usize].push(tags_slice[ki].clone()); // FIXME: don't clone
        }

        let pkeys = Arc::new(pkeys);
        let pnames = Arc::new(pnames);
        let ptags = Arc::new(ptags);

        let inner = self.inner.read();
        let partitions = inner.partitions.clone();
        drop(inner);
        let tasks = partitions
            .into_iter()
            .enumerate()
            .map(|(idx, partition)| {
                let pkeys = Arc::clone(&pkeys);
                let pnames = Arc::clone(&pnames);
                let ptags = Arc::clone(&ptags);
                let cache = Arc::clone(&self.tag_value_cache);
                Box::new(move || {
                    if pnames[idx].is_empty() {
                        return Ok((Vec::new(), Vec::new()));
                    }

                    let names = pnames[idx]
                        .iter()
                        .map(|name| name.as_slice())
                        .collect::<Vec<&[u8]>>();
                    let lock_start = slow_trace::now();
                    let partition = partition.read();
                    slow_trace::log_lock_wait(
                        "index_partition.read",
                        lock_start,
                        "stage=write_index_create_series",
                    );
                    let ids = partition
                        .create_series_list_if_not_exists(
                            names.as_slice(),
                            ptags[idx]
                                .iter()
                                .collect::<Vec<&crate::models::points::Tags>>()
                                .as_slice(),
                        )
                        .context(PartitionSnafu)?;

                    let mut created_keys = Vec::new();
                    let mut created_names = Vec::new();
                    let Some(ids) = ids else {
                        return Ok((created_keys, created_names));
                    };
                    if !ids.iter().any(|id| *id != 0) {
                        return Ok((created_keys, created_names));
                    }

                    let lock_start = slow_trace::now();
                    let cache = cache.read();
                    slow_trace::log_lock_wait(
                        "index_tag_value_cache.read",
                        lock_start,
                        "stage=write_index_create_series",
                    );
                    for (j, id) in ids.iter().enumerate() {
                        if *id == 0 {
                            continue;
                        }

                        let name = pnames[idx][j].clone();
                        let tags = ptags[idx][j].clone();
                        if cache.measurement_contains_sets(name.as_slice()) {
                            for pair in tags.iter() {
                                cache.add_to_set(
                                    name.as_slice(),
                                    pair.key.as_slice(),
                                    pair.value.as_slice(),
                                    *id,
                                );
                            }
                        }

                        created_keys.push(pkeys[idx][j].clone());
                        created_names.push(name);
                    }
                    Ok((created_keys, created_names))
                }) as PartitionTask<(Vec<Vec<u8>>, Vec<Vec<u8>>)>
            })
            .collect();

        let results = self.run_required_partition_tasks(tasks)?;
        let mut created_keys = Vec::new();
        let mut created_names = Vec::new();
        for (mut keys, mut names) in results {
            created_keys.append(&mut keys);
            created_names.append(&mut names);
        }

        if created_keys.is_empty() {
            return Ok(());
        }

        created_keys.sort_unstable();
        created_keys.dedup();
        created_names.sort_unstable();
        created_names.dedup();

        let sketch_update_start = slow_trace::now();
        let lock_start = slow_trace::now();
        let mut inner = self.inner.write();
        slow_trace::log_lock_wait("index.write", lock_start, "stage=write_index_create_series");
        // Update sketches under lock.
        for key in created_keys.iter() {
            inner.s_sketch.add(key.as_slice());
        }

        for name in created_names.iter() {
            inner.m_sketch.add(name.as_slice());
        }
        slow_trace::log_background_stage(
            "write_index_create_series",
            sketch_update_start,
            "component=sketch_update",
        );

        Ok(())
    }

    /// Creates a series if it doesn't exist or is deleted.
    pub fn create_series_if_not_exists(&self, key: &[u8], name: &[u8], tags: Tags) -> Result<()> {
        let ids = {
            let p = self.partition(key);
            let start = slow_trace::now();
            let partition = p.write();
            slow_trace::log_lock_wait("index_partition.write", start, "stage=write_index_create_series");
            partition.create_series_list_if_not_exists(&[name], &[&tags])
                .context(PartitionSnafu)?
        };
        // Partition operations must finish before acquiring index metadata.
        {
            let start = slow_trace::now();
            let mut inner = self.inner.write();
            slow_trace::log_lock_wait("index.write", start, "stage=write_index_create_series");
            inner.s_sketch.add(key);
            inner.m_sketch.add(name);
        }

        if let Some(ids) = &ids {
            if ids[0] == 0 {
                return Ok(()); // No new series, nothing further to update.
            }
        } else {
            return Ok(()); // No new series, nothing further to update.
        }

        // If there are cached sets for any of the tag pairs, they will need to be
        // updated with the series id.
        let cache = self.tag_value_cache.read();
        if cache.measurement_contains_sets(name) {
            let id = ids.unwrap()[0];
            for pair in tags.iter() {
                // TODO(edd): It's not clear to me yet whether it will be better to take a lock
                // on every series id set, or whether to gather them all up under the cache rlock
                // and then take the cache lock and update them all at once (without invoking a lock
                // on each series id set).
                //
                // Taking the cache lock will block all queries, but is one lock. Taking each series set
                // lock might be many lock/unlocks but will only block a query that needs that particular set.
                //
                // Need to think on it, but I think taking a lock on each series id set is the way to go.
                //
                // Note this will only add `id` to the set if it exists.
                cache.add_to_set(name, pair.key.as_slice(), pair.value.as_slice(), id);
                // Takes a lock on the series id set
            }
        }
        drop(cache);

        Ok(())
    }

    /// Drops the provided series from the index.  If cascade is true
    /// and this is the last series to the measurement, the measurement will also be dropped.
    pub fn drop_series(&self, series_id: u64, key: &[u8], cascade: bool) -> Result<()> {
        // Remove from partition.
        self.partition(key)
            .read()
            .drop_series(series_id)
            .context(PartitionSnafu)?;

        // Add sketch tombstone.
        {
            let mut inner = self.inner.write();
            inner.st_sketch.add(key);
        }

        if !cascade {
            return Ok(());
        }

        // Extract measurement name & tags.
        let (name, tags) = crate::models::points::parse_key_bytes(key);

        // If there are cached sets for any of the tag pairs, they will need to be
        // updated with the series id.
        {
            let cache = self.tag_value_cache.read();
            if cache.measurement_contains_sets(name.as_slice()) {
                for pair in tags.iter() {
                    cache.delete(
                        name.as_slice(),
                        pair.key.as_slice(),
                        pair.value.as_slice(),
                        series_id,
                    ); // Takes a lock on the series id set
                }
            }
        }

        // Check if that was the last series for the measurement in the entire index.
        if self.measurement_has_series(name.as_slice())? {
            return Ok(());
        }

        // If no more series exist in the measurement then delete the measurement.
        self.drop_measurement(name.as_slice())?;

        Ok(())
    }

    /// Rechecks a lock-free liveness stamp at the existing partition mutation
    /// boundary. The predicate must not perform IO or acquire other locks.
    pub(crate) fn drop_series_if(&self, series_id: u64, key: &[u8], unchanged: impl FnOnce() -> bool) -> Result<bool> {
        {
            let p = self.partition(key);
            let partition = p.write();
            if !unchanged() { return Ok(false); }
            if !partition.inner.read().series_id_set.contains(series_id) { return Ok(true); }
            partition.drop_series(series_id).context(PartitionSnafu)?;
            let (name, tags) = crate::models::points::parse_key_bytes(key);
            let cache = self.tag_value_cache.read();
            for pair in tags.iter() {
                cache.delete(&name, &pair.key, &pair.value, series_id);
            }
        }
        self.inner.write().st_sketch.add(key);
        Ok(true)
    }

    pub(crate) fn contains_series_id(&self, series_id: u64, key: &[u8]) -> bool {
        let p = self.partition(key);
        let partition = p.read();
        let present = partition.inner.read().series_id_set.contains(series_id);
        present
    }

    /// Drops a measurement only if there are no more series for the measurement.
    pub fn drop_measurement_if_series_not_exist(&self, name: &[u8]) -> Result<bool> {
        // Check if that was the last series for the measurement in the entire index.
        if self.measurement_has_series(name)? {
            return Ok(false);
        }

        // If no more series exist in the measurement then delete the measurement.
        self.drop_measurement(name)?;

        Ok(true)
    }

    /// Returns the two measurement sketches for the index.
    pub fn measurements_sketches(&self) -> Result<(HLL, HLL)> {
        // (sketch, tomestome_sketch)
        let inner = self.inner.read();
        Ok((inner.m_sketch.clone(), inner.mt_sketch.clone()))
    }

    /// Returns the two series sketches for the index.
    pub fn series_sketches(&self) -> Result<(HLL, HLL)> {
        // (sketch, tomestome_sketch)
        let inner = self.inner.read();
        Ok((inner.s_sketch.clone(), inner.st_sketch.clone()))
    }

    /// Since indexes are not shared across shards, the count returned by SeriesN
    /// cannot be combined with other shard's results. If you need to count series
    /// across indexes then use either the database-wide series file, or merge the
    /// index-level bitsets or sketches.
    pub fn series_n(&self) -> i64 {
        self.series_id_set().cardinality() as i64
    }

    /// Returns true if tag key exists. It returns the first error encountered if any.
    pub fn has_tag_key(&self, name: &[u8], key: &[u8]) -> Result<bool> {
        let found = Arc::new(AtomicU32::new(0)); // Use this to signal we found the measurement.
        let inner = self.inner.read();
        let partitions = inner.partitions.clone();
        drop(inner);
        let name = name.to_vec();
        let key = key.to_vec();
        let tasks = partitions
            .into_iter()
            .map(|partition| {
                let found = Arc::clone(&found);
                let name = name.clone();
                let key = key.clone();
                Box::new(move || {
                    if found.load(Ordering::SeqCst) == 1 {
                        return Ok(());
                    }

                    if partition
                        .read()
                        .has_tag_key(name.as_slice(), key.as_slice())
                        .context(PartitionSnafu)?
                    {
                        found.store(1, Ordering::SeqCst);
                    }
                    Ok(())
                }) as PartitionTask<()>
            })
            .collect();
        self.run_required_partition_tasks(tasks)?;

        // Check if we found the measurement.
        if found.load(Ordering::SeqCst) == 1 {
            return Ok(true);
        }

        Ok(false)
    }

    /// Returns true if tag value exists.
    pub fn hash_tag_value(&self, name: &[u8], key: &[u8], value: &[u8]) -> Result<bool> {
        let found = Arc::new(AtomicU32::new(0)); // Use this to signal we found the measurement.
        let inner = self.inner.read();
        let partitions = inner.partitions.clone();
        drop(inner);
        let name = name.to_vec();
        let key = key.to_vec();
        let value = value.to_vec();
        let tasks = partitions
            .into_iter()
            .map(|partition| {
                let found = Arc::clone(&found);
                let name = name.clone();
                let key = key.clone();
                let value = value.clone();
                Box::new(move || {
                    if found.load(Ordering::SeqCst) == 1 {
                        return Ok(());
                    }

                    partition
                        .read()
                        .has_tag_value(name.as_slice(), key.as_slice(), value.as_slice())
                        .context(PartitionSnafu)?;
                    found.store(1, Ordering::SeqCst);
                    Ok(())
                }) as PartitionTask<()>
            })
            .collect();
        self.run_required_partition_tasks(tasks)?;

        // Check if we found the measurement.
        if found.load(Ordering::SeqCst) == 1 {
            return Ok(true);
        }

        Ok(false)
    }

    /// Returns an iterator for all keys across a single measurement.
    pub fn tag_key_iter(&self, name: &[u8]) -> Result<Option<crate::index::TagKeyMergeIter>> {
        let inner = self.inner.read();
        let mut itrs = Vec::with_capacity(inner.partitions.len());
        for p in &inner.partitions {
            if let Ok(Some(itr)) = p.read().tag_key_iter(name).context(PartitionSnafu) {
                itrs.push(itr);
            }
        }

        Ok(crate::index::merge_tag_key_iters(itrs))
    }

    /// Returns an iterator for all values across a single key.
    pub fn tag_value_iter(
        &self,
        name: &[u8],
        key: &[u8],
    ) -> Result<Option<crate::index::TagValueMergeIter>> {
        let inner = self.inner.read();
        let mut itrs = Vec::with_capacity(inner.partitions.len());
        for p in &inner.partitions {
            if let Ok(Some(itr)) = p.read().tag_value_iter(name, key).context(PartitionSnafu) {
                itrs.push(itr);
            }
        }

        Ok(crate::index::merge_tag_value_iters(itrs))
    }

    /// Returns a series iterator for all values across a single key.
    pub fn tag_key_series_id_iter(
        &self,
        name: &[u8],
        key: &[u8],
    ) -> Result<Option<crate::index::FileSetSeriesIDIter>> {
        let inner = self.inner.read();
        let mut itrs = Vec::with_capacity(inner.partitions.len());
        for p in &inner.partitions {
            if let Ok(Some(itr)) = p
                .read()
                .tag_key_series_id_iter(name, key)
                .context(PartitionSnafu)
            {
                itrs.push(itr);
            }
        }

        Ok(crate::index::merge_file_set_series_id_iters(itrs))
    }

    // Note: lru_cached_series_set is incorrect!
    // pub fn lru_cached_series_set(&self) -> Option<SeriesIDSet> {
    //     if self.tag_value_cache_size > 0 {
    //         let cache = self.tag_value_cache.write();
    //         return Some(cache.all_cached_series_set());
    //     }
    //     None
    // }

    // Note: lru_cached_tag_keys is incorrect!
    // pub fn lru_cached_tag_keys(&self) -> Vec<String> {
    //     let mut cached_series_keys = vec![];
    //     if self.tag_value_cache_size > 0 {
    //         let cache = self.tag_value_cache.write();
    //         let tag_values = cache.cached_tag_values();
    //         for (name, tag_key_values) in tag_values {
    //             let fields_keys = self.field_keys(name.as_bytes());
    //             for (tk, tvalues) in tag_key_values {
    //                 for tv in tvalues {
    //                     for fk in fields_keys.iter() {
    //                         cached_series_keys.push(format!("{},{}={}#!~#{}", name, tk, tv, fk));
    //                     }
    //                 }
    //             }
    //         }
    //     }
    //     cached_series_keys
    // }

    /// Returns a series iterator for a single tag value.
    pub fn tag_value_series_id_iter(
        &self,
        name: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<crate::index::FileSetSeriesIDIter>> {
        self.tag_value_series_id_iter_inner(
            name,
            key,
            value,
            #[cfg(feature = "slow-trace")]
            None,
        )
    }

    #[cfg(feature = "slow-trace")]
    pub(crate) fn tag_value_series_id_iter_profiled(
        &self,
        name: &[u8],
        key: &[u8],
        value: &[u8],
        diagnostics: &TsiSelectionDiagnostics,
    ) -> Result<Option<crate::index::FileSetSeriesIDIter>> {
        self.tag_value_series_id_iter_inner(name, key, value, Some(diagnostics))
    }

    fn tag_value_series_id_iter_inner(
        &self,
        name: &[u8],
        key: &[u8],
        value: &[u8],
        #[cfg(feature = "slow-trace")] diagnostics: Option<&TsiSelectionDiagnostics>,
    ) -> Result<Option<crate::index::FileSetSeriesIDIter>> {
        // Check series ID set cache...
        if self.tag_value_cache_size > 0 {
            #[cfg(feature = "slow-trace")]
            let wait_started = std::time::Instant::now();
            let mut cache = self.tag_value_cache.write();
            #[cfg(feature = "slow-trace")]
            let wait_us = wait_started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
            #[cfg(feature = "slow-trace")]
            let hold_started = std::time::Instant::now();
            let cached = cache.get(name, key, value);
            drop(cache);
            #[cfg(feature = "slow-trace")]
            if let Some(diagnostics) = diagnostics {
                diagnostics.record_tag_value_cache_lock(
                    wait_us,
                    hold_started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
                );
            }
            if let Some(ss) = cached {
                // Return a clone because the set is mutable.
                return Ok(Some(crate::index::FileSetSeriesIDIter::SeriesIDSetIter(
                    crate::index::new_series_id_set_iter(ss),
                )));
            }
        }

        let inner = self.inner.read();
        let mut itrs = Vec::with_capacity(inner.partitions.len());
        for p in inner.partitions.iter() {
            if let Some(itr) = p
                .read()
                .tag_value_series_id_iter(name, key, value)
                .context(PartitionSnafu)?
            {
                itrs.push(itr);
            }
        }

        let itr = crate::index::merge_file_set_series_id_iters(itrs);
        if self.tag_value_cache_size == 0 {
            return Ok(itr);
        }

        // Check if the iterator contains only series id sets. Cache them...
        if let Some(crate::index::FileSetSeriesIDIter::OneFileSetSeriesIDSetIter(itr)) = &itr {
            let ss = itr.series_id_set().map(|ss| (*ss).clone());
            let mut cache = self.tag_value_cache.write();
            cache.put(name, key, value, ss)
        }
        if let Some(itr) = &itr {
            match itr {
                crate::index::FileSetSeriesIDIter::SeriesIDSetIter(itr) => {
                    let ss = itr.series_id_set();
                    let mut cache = self.tag_value_cache.write();
                    cache.put(name, key, value, Some((*ss).clone()))
                }
                crate::index::FileSetSeriesIDIter::OneFileSetSeriesIDSetIter(itr) => {
                    let ss = itr.series_id_set().map(|ss| (*ss).clone());
                    let mut cache = self.tag_value_cache.write();
                    cache.put(name, key, value, ss)
                }
                crate::index::FileSetSeriesIDIter::FileSetSeriesIDMergeIter(itr) => {
                    unreachable!();
                }
            }
        }

        Ok(itr)
    }

    /// Returns the tag keys for the measurement.
    pub fn measurement_tag_keys(&self, name: &[u8]) -> Result<HashSet<String>> {
        let inner = self.inner.read();
        let partitions = inner.partitions.clone();
        drop(inner);
        let name = name.to_vec();
        let tasks = partitions
            .into_iter()
            .map(|partition| {
                let name = name.clone();
                Box::new(move || {
                    partition
                        .read()
                        .measurement_tag_keys(&name)
                        .context(PartitionSnafu)
                }) as PartitionTask<HashSet<String>>
            })
            .collect();
        let keys = self.run_required_partition_tasks(tasks)?;

        // Merge into single set.
        let mut res = HashSet::new();
        for keys in keys {
            for key in keys {
                res.insert(key);
            }
        }

        Ok(res)
    }

    #[cfg(feature = "slow-trace")]
    pub(crate) fn measurement_tag_keys_profiled(
        &self,
        name: &[u8],
        diagnostics: Arc<MeasurementTagKeysDiagnostics>,
    ) -> Result<HashSet<String>> {
        let inner = self.inner.read();
        let partitions = inner.partitions.clone();
        drop(inner);
        diagnostics.record_partition_count(partitions.len());
        let name = name.to_vec();
        let tasks = partitions
            .into_iter()
            .map(|partition| {
                let name = name.clone();
                Box::new(move || {
                    partition
                        .read()
                        .measurement_tag_keys(&name)
                        .context(PartitionSnafu)
                }) as PartitionTask<HashSet<String>>
            })
            .collect();
        let keys = self.run_required_partition_tasks_profiled(tasks, Arc::clone(&diagnostics))?;

        let mut res = HashSet::new();
        for keys in keys {
            for key in keys {
                res.insert(key);
            }
        }

        Ok(res)
    }

    /// Returns the size of the index on disk.
    pub fn disk_size_bytes(&self) -> i64 {
        if let Ok(fs) = self.retain_file_set() {
            let mut manifest_size = 0i64;
            // Get MANIFEST sizes from each partition.
            let inner = self.inner.read();
            for p in inner.partitions.iter() {
                manifest_size += p.read().manifest_size();
            }

            fs.release();
            return fs.size() + manifest_size;
        }

        // self.logger.Warn("Index is closing down");
        0
    }

    /// TagKeyCardinality always returns zero.
    /// It is not possible to determine cardinality of tags across index files, and
    /// thus it cannot be done across partitions.
    pub fn tag_key_cardinality(&self, name: &[u8], key: &[u8]) -> i64 {
        0
    }

    /// Returns the set of all files across all partitions.
    /// This is only needed when all files need to be retained for an operation.
    pub fn retain_file_set(&self) -> Result<FileSet> {
        let mut fs = FileSet::new(vec![], Arc::clone(&self.sfile), VecDeque::new());
        let inner = self.inner.read();
        for p in inner.partitions.iter() {
            let p = p.read();
            match p.retain_file_set() {
                Ok(pfs) => {
                    if !pfs.files.is_empty() {
                        fs.files.extend(pfs.files.clone()); // clone just increase reference count
                    }
                }
                Err(e) => {
                    let _ = fs.close();
                    return Err(crate::index::tsi1::index::Error::Partition { source: e });
                }
            }
        }

        Ok(fs)
    }

    pub fn add_lru_series_keys(&self, keys: &[String]) {
        let lock_start = slow_trace::now();
        let mut lru_series_keys = self.lru_series_keys.write();
        slow_trace::log_lock_wait(
            "index_lru_series_keys.write",
            lock_start,
            "stage=query_wait_for_index_lock",
        );
        lru_series_keys.add_keys(keys);
    }

    pub fn lru_series_keys(&self) -> Vec<String> {
        self.lru_series_keys.read().keys()
    }

    pub fn lru_series_keys_exists(&self, key: &str) -> bool {
        self.lru_series_keys.read().key_exists(key)
    }

    pub fn lru_series_keys_len(&self) -> usize {
        self.lru_series_keys.read().keys.len()
    }

    pub fn lru_series_keys_is_empty(&self) -> bool {
        self.lru_series_keys.read().keys.is_empty()
    }
}

#[cfg(any(test, feature = "benchmarking"))]
pub mod tests {
    use core::panic;
    use std::{collections::HashMap, os::unix::fs::PermissionsExt, sync::Arc};

    use crate::common::{to_string, wait_group::WaitGroup};
    use crate::runtime::global::spawn_blocking_global;
    use snafu::ResultExt;
    use tempfile::TempDir;

    use super::DEFAULT_PARTITION_N;
    use crate::{
        config::{DEFAULT_MAX_INDEX_LOG_FILE_SIZE, DEFAULT_SERIES_ID_SET_CACHE_SIZE},
        index::{
            SeriesElem,
            tsi1::{
                self, TagKeyElement, file_set,
                partition::{Error::ErrIncompatibleVersion, MANIFEST_FILE_NAME, Manifest, VERSION},
            },
        },
        series::series_file::tests::Series,
    };
    // Ensure index can iterate over all measurement names.
    #[test]
    fn test_index_for_each_measurement_name() {
        let mut idx = must_open_default_index();

        // 1.  Add series to index.
        idx.create_series_slice_if_not_exists(vec![
            Series {
                name: "cpu".as_bytes().to_vec(),
                tags: crate::models::points::Tags::from(HashMap::from([(
                    "region".as_bytes().to_vec(),
                    "east".as_bytes().to_vec(),
                )])),
                delete: false,
            },
            Series {
                name: "cpu".as_bytes().to_vec(),
                tags: crate::models::points::Tags::from(HashMap::from([(
                    "region".as_bytes().to_vec(),
                    "west".as_bytes().to_vec(),
                )])),
                delete: false,
            },
            Series {
                name: "mem".as_bytes().to_vec(),
                tags: crate::models::points::Tags::from(HashMap::from([(
                    "region".as_bytes().to_vec(),
                    "east".as_bytes().to_vec(),
                )])),
                delete: false,
            },
        ])
        .unwrap();

        // 2. Verify measurements are returned.
        // 2.1 Invoke immediately.
        vertify_measurement_are_returned(
            &idx,
            vec!["cpu".as_bytes().to_vec(), "mem".as_bytes().to_vec()],
        );

        // 2.2 Reopen and invoke again.
        assert!(idx.reopen().is_ok(), "reopen failed");
        vertify_measurement_are_returned(
            &idx,
            vec!["cpu".as_bytes().to_vec(), "mem".as_bytes().to_vec()],
        );

        // TODO: 2.3 Request a compaction.

        // 2.4 Reopen and invoke again.
        assert!(idx.reopen().is_ok(), "post-compaction reopen failed");
        vertify_measurement_are_returned(
            &idx,
            vec!["cpu".as_bytes().to_vec(), "mem".as_bytes().to_vec()],
        );

        // 3. Add more series.
        idx.create_series_slice_if_not_exists(vec![
            Series {
                name: "disk".as_bytes().to_vec(),
                ..Default::default()
            },
            Series {
                name: "mem".as_bytes().to_vec(),
                ..Default::default()
            },
        ])
        .unwrap();

        // 4. Verify new measurements.
        // 4.1 Invoke immediately.
        vertify_measurement_are_returned(
            &idx,
            vec![
                "cpu".as_bytes().to_vec(),
                "disk".as_bytes().to_vec(),
                "mem".as_bytes().to_vec(),
            ],
        );

        // 4.2 Reopen and invoke again.
        assert!(idx.reopen().is_ok(), "reopen failed");
        vertify_measurement_are_returned(
            &idx,
            vec![
                "cpu".as_bytes().to_vec(),
                "disk".as_bytes().to_vec(),
                "mem".as_bytes().to_vec(),
            ],
        );

        // TODO: 4.3 Request a compaction.

        // 4.4 Reopen and invoke again.
        assert!(idx.reopen().is_ok(), "post-compaction reopen failed");
        vertify_measurement_are_returned(
            &idx,
            vec![
                "cpu".as_bytes().to_vec(),
                "disk".as_bytes().to_vec(),
                "mem".as_bytes().to_vec(),
            ],
        );
    }

    // Ensure index can return whether a measurement exists.
    #[test]
    fn test_index_measurement_exists() {
        let mut idx = must_open_default_index();
        // 1. Add series to index.
        idx.create_series_slice_if_not_exists(vec![
            Series {
                name: "cpu".as_bytes().to_vec(),
                tags: crate::models::points::Tags::from(HashMap::from([(
                    "region".as_bytes().to_vec(),
                    "east".as_bytes().to_vec(),
                )])),
                delete: false,
            },
            Series {
                name: "cpu".as_bytes().to_vec(),
                tags: crate::models::points::Tags::from(HashMap::from([(
                    "region".as_bytes().to_vec(),
                    "west".as_bytes().to_vec(),
                )])),
                delete: false,
            },
        ])
        .unwrap();

        // 2. Verify measurement exists.
        // 2.1 Invoke immediately.
        vertify_measurement_exists(&idx, "cpu".as_bytes(), true);

        // 2.2 Reopen and invoke again.
        assert!(idx.reopen().is_ok(), "reopen failed");
        vertify_measurement_exists(&idx, "cpu".as_bytes(), true);

        // TODO: 2.3 Request a compaction.

        // 2.4 Reopen and invoke again.
        assert!(idx.reopen().is_ok(), "reopen failed");
        vertify_measurement_exists(&idx, "cpu".as_bytes(), true);

        let (name, mut tags) = (
            "cpu".as_bytes(),
            crate::models::points::Tags::from(HashMap::from([(
                "region".as_bytes().to_vec(),
                "east".as_bytes().to_vec(),
            )])),
        );
        let sid = idx.idx.series_file().series_id(name, &tags);
        assert_ne!(sid, 0, "got 0 series id for {:?}/{:?}", name, tags);

        // 3. Delete one series.
        idx.idx
            .drop_series(sid, &crate::models::points::make_key(name, &tags), true)
            .unwrap();

        // 4. Verify measurement still exists.
        // 4.1 Invoke immediately.
        vertify_measurement_exists(&idx, "cpu".as_bytes(), true);

        // 4.2 Reopen and invoke again.
        assert!(idx.reopen().is_ok(), "reopen failed");
        vertify_measurement_exists(&idx, "cpu".as_bytes(), true);

        // TODO: 4.3 Request a compaction.

        // 4.4 Reopen and invoke again.
        assert!(idx.reopen().is_ok(), "reopen failed");
        vertify_measurement_exists(&idx, "cpu".as_bytes(), true);

        // 5. Delete second series.
        tags.set("region".as_bytes(), "west".as_bytes());
        let sid = idx.idx.series_file().series_id(name, &tags);
        assert_ne!(sid, 0, "got 0 series id for {:?}/{:?}", name, tags);

        idx.idx
            .drop_series(sid, &crate::models::points::make_key(name, &tags), true)
            .unwrap();

        // 6. Verify measurement is now deleted.
        // 6.1 Invoke immediately.
        vertify_measurement_exists(&idx, "cpu".as_bytes(), false);

        // 6.2 Reopen and invoke again.
        assert!(idx.reopen().is_ok(), "reopen failed");
        vertify_measurement_exists(&idx, "cpu".as_bytes(), false);

        // TODO: 6.3 Request a compaction.

        // 6.4 Reopen and invoke again.
        assert!(idx.reopen().is_ok(), "reopen failed");
        vertify_measurement_exists(&idx, "cpu".as_bytes(), false);
    }

    // Ensure index can delete a measurement and all related keys, values, & series.
    #[test]
    fn test_index_drop_measurement() {
        let mut idx = must_open_default_index();

        // 1. Add series to index.
        idx.create_series_slice_if_not_exists(vec![
            Series {
                name: "cpu".as_bytes().to_vec(),
                tags: crate::models::points::Tags::from(HashMap::from([(
                    "region".as_bytes().to_vec(),
                    "east".as_bytes().to_vec(),
                )])),
                delete: false,
            },
            Series {
                name: "cpu".as_bytes().to_vec(),
                tags: crate::models::points::Tags::from(HashMap::from([(
                    "region".as_bytes().to_vec(),
                    "west".as_bytes().to_vec(),
                )])),
                delete: false,
            },
            Series {
                name: "disk".as_bytes().to_vec(),
                tags: crate::models::points::Tags::from(HashMap::from([(
                    "region".as_bytes().to_vec(),
                    "north".as_bytes().to_vec(),
                )])),
                delete: false,
            },
            Series {
                name: "mem".as_bytes().to_vec(),
                tags: crate::models::points::Tags::from(HashMap::from([
                    ("region".as_bytes().to_vec(), "west".as_bytes().to_vec()),
                    ("country".as_bytes().to_vec(), "us".as_bytes().to_vec()),
                ])),
                delete: false,
            },
        ])
        .unwrap();

        // 2. Drop measurement.
        assert!(idx.idx.drop_measurement("cpu".as_bytes()).is_ok());

        // 3. Verify data is gone in each stage.
        // 3.1 Invoke immediately.
        vertify_data_is_gone_in_each_stage(&idx, "cpu".as_bytes(), "region".as_bytes());

        // 3.2 Reopen and invoke again.
        assert!(idx.reopen().is_ok(), "reopen failed");
        vertify_data_is_gone_in_each_stage(&idx, "cpu".as_bytes(), "region".as_bytes());

        // TODO: 3.3 Request a compaction.

        // 3.4 Reopen and invoke again.
        assert!(idx.reopen().is_ok(), "post-compaction reopen failed");
        vertify_data_is_gone_in_each_stage(&idx, "cpu".as_bytes(), "region".as_bytes());
    }

    #[test]
    fn test_index_open() {
        // Opening a fresh index should set the MANIFEST version to current version.
        let mut idx = new_default_index();

        // 1. open new index and check
        assert!(idx.open().is_ok());
        // Check version set appropriately.
        for i in 0..DEFAULT_PARTITION_N {
            let p = idx.idx.partition_at(i as usize);
            let p = p.read();
            let mut inner = p.inner.write();
            let v = p.manifest(&mut inner).version;
            assert_eq!(v, VERSION, "got index version {}, expected {}", v, VERSION);
        }

        // 2. Reopening an open index should return an error.
        if let Ok(_) = idx.open() {
            idx.close();
            panic!("didn't get an error on reopen, but expected one");
        }
        idx.close();

        // 3. Opening an incompatible index should return an error.
        let incompatible_versions = vec![-1, 0, 2];
        for v in incompatible_versions {
            let mut idx = new_default_index();
            // Manually create a MANIFEST file for an incompatible index version.
            let ppath = idx.idx.path().join("2");
            std::fs::create_dir_all(&ppath).expect("Failed to create partition dir");
            std::fs::metadata(&ppath)
                .unwrap()
                .permissions()
                .set_mode(0o777);

            let mpath = ppath.join(MANIFEST_FILE_NAME);
            let mut m = Manifest::new();
            m.path = mpath;
            m.levels = Vec::new();
            m.version = v; // Set example MANIFEST version.
            m.write().unwrap();

            // Log the MANIFEST file.
            let data = std::fs::read(&m.path).unwrap();
            eprintln!("Incompatible MANIFEST: {:?}", data);

            // Opening this index should return an error because the MANIFEST has an
            // incompatible version.
            if let Err(e) = idx.open() {
                assert!(
                    matches!(
                        e,
                        crate::index::tsi1::index::Error::Partition {
                            source: ErrIncompatibleVersion,
                        }
                    ),
                    "got error {}, expected {}",
                    e,
                    crate::index::tsi1::index::Error::Partition {
                        source: ErrIncompatibleVersion,
                    }
                );
            }
        }
    }

    #[test]
    fn test_index_manifest() {
        let mut idx = must_open_default_index();

        // Check version set appropriately.
        for i in 0..DEFAULT_PARTITION_N {
            let p = idx.idx.partition_at(i as usize);
            let p = p.read();
            let mut inner = p.inner.write();
            let v = p.manifest(&mut inner).version;
            assert_eq!(v, VERSION, "got index version {}, expected {}", v, VERSION);
        }
    }

    #[test]
    fn test_index_disk_size_bytes() {
        let mut idx = must_open_default_index();

        idx.create_series_slice_if_not_exists(vec![
            Series {
                name: "cpu".as_bytes().to_vec(),
                tags: crate::models::points::Tags::from(HashMap::from([(
                    "region".as_bytes().to_vec(),
                    "east".as_bytes().to_vec(),
                )])),
                delete: false,
            },
            Series {
                name: "cpu".as_bytes().to_vec(),
                tags: crate::models::points::Tags::from(HashMap::from([(
                    "region".as_bytes().to_vec(),
                    "west".as_bytes().to_vec(),
                )])),
                delete: false,
            },
            Series {
                name: "disk".as_bytes().to_vec(),
                tags: crate::models::points::Tags::from(HashMap::from([(
                    "region".as_bytes().to_vec(),
                    "north".as_bytes().to_vec(),
                )])),
                delete: false,
            },
            Series {
                name: "mem".as_bytes().to_vec(),
                tags: crate::models::points::Tags::from(HashMap::from([
                    ("region".as_bytes().to_vec(), "west".as_bytes().to_vec()),
                    ("country".as_bytes().to_vec(), "us".as_bytes().to_vec()),
                ])),
                delete: false,
            },
        ])
        .unwrap();

        // Verify on disk size is the same in each stage.
        // Each series stores flag(1) + series(uvarint(2)) + len(name)(1) + len(key)(1) + len(value)(1) + checksum(4).
        let mut exp_size: i64 = 4 * 9;

        // MANIFEST files include their absolute path, so their size depends on
        // the temporary index root generated for this test.
        let manifest_size = (0..DEFAULT_PARTITION_N)
            .map(|partition_id| {
                std::fs::metadata(
                    idx.idx
                        .path()
                        .join(partition_id.to_string())
                        .join(MANIFEST_FILE_NAME),
                )
                .unwrap()
                .len() as i64
            })
            .sum::<i64>();
        exp_size += manifest_size;

        // idx.run(|| {
        //     let (got, exp) = (idx.idx.disk_size_bytes(), exp_size);
        //     assert_eq!(got, exp, "got {} bytes, expected: {}", got, exp);
        // });

        // Invoke immediately.
        let (got, exp) = (idx.idx.disk_size_bytes(), exp_size);
        assert_eq!(got, exp, "got {} bytes, expected: {}", got, exp);

        // Reopen and invoke again.
        assert!(idx.reopen().is_ok(), "reopen failed");
        let (got, exp) = (idx.idx.disk_size_bytes(), exp_size);
        assert_eq!(got, exp, "got {} bytes, expected: {}", got, exp);

        // TODO: request a compaction
        // Reopen and invoke again.
        assert!(idx.reopen().is_ok(), "post-compaction  reopen failed");
        let (got, exp) = (idx.idx.disk_size_bytes(), exp_size);
        assert_eq!(got, exp, "got {} bytes, expected: {}", got, exp);
    }

    #[test]
    fn test_index_tag_value_series_id_iter() {
        let idx1 = must_open_default_index(); // Uses the single series creation method CreateSeriesIfNotExists
        let idx2 = must_open_default_index(); // Uses the batch series creation method CreateSeriesListIfNotExists

        struct Series {
            key: String,
            name: String,
            tags: HashMap<Vec<u8>, Vec<u8>>,
        }

        // 1. Add some series.
        let data = vec![
            Series {
                key: "cpu,region=west,server=a".to_string(),
                name: "cpu".to_string(),
                tags: HashMap::from([
                    ("region".as_bytes().to_vec(), "west".as_bytes().to_vec()),
                    ("server".as_bytes().to_vec(), "a".as_bytes().to_vec()),
                ]),
            },
            Series {
                key: "cpu,region=west,server=b".to_string(),
                name: "cpu".to_string(),
                tags: HashMap::from([
                    ("region".as_bytes().to_vec(), "west".as_bytes().to_vec()),
                    ("server".as_bytes().to_vec(), "b".as_bytes().to_vec()),
                ]),
            },
            Series {
                key: "cpu,region=east,server=a".to_string(),
                name: "cpu".to_string(),
                tags: HashMap::from([
                    ("region".as_bytes().to_vec(), "east".as_bytes().to_vec()),
                    ("server".as_bytes().to_vec(), "a".as_bytes().to_vec()),
                ]),
            },
            Series {
                key: "cpu,region=north,server=c".to_string(),
                name: "cpu".to_string(),
                tags: HashMap::from([
                    ("region".as_bytes().to_vec(), "north".as_bytes().to_vec()),
                    ("server".as_bytes().to_vec(), "c".as_bytes().to_vec()),
                ]),
            },
            Series {
                key: "cpu,region=south,server=s".to_string(),
                name: "cpu".to_string(),
                tags: HashMap::from([
                    ("region".as_bytes().to_vec(), "south".as_bytes().to_vec()),
                    ("server".as_bytes().to_vec(), "s".as_bytes().to_vec()),
                ]),
            },
            Series {
                key: "mem,region=west,server=a".to_string(),
                name: "mem".to_string(),
                tags: HashMap::from([
                    ("region".as_bytes().to_vec(), "west".as_bytes().to_vec()),
                    ("server".as_bytes().to_vec(), "a".as_bytes().to_vec()),
                ]),
            },
            Series {
                key: "mem,region=west,server=b".to_string(),
                name: "mem".to_string(),
                tags: HashMap::from([
                    ("region".as_bytes().to_vec(), "west".as_bytes().to_vec()),
                    ("server".as_bytes().to_vec(), "b".as_bytes().to_vec()),
                ]),
            },
            Series {
                key: "mem,region=west,server=c".to_string(),
                name: "mem".to_string(),
                tags: HashMap::from([
                    ("region".as_bytes().to_vec(), "west".as_bytes().to_vec()),
                    ("server".as_bytes().to_vec(), "c".as_bytes().to_vec()),
                ]),
            },
            Series {
                key: "disk,region=east,server=a".to_string(),
                name: "disk".to_string(),
                tags: HashMap::from([
                    ("region".as_bytes().to_vec(), "east".as_bytes().to_vec()),
                    ("server".as_bytes().to_vec(), "a".as_bytes().to_vec()),
                ]),
            },
            Series {
                key: "disk,region=east,server=a".to_string(),
                name: "disk".to_string(),
                tags: HashMap::from([
                    ("region".as_bytes().to_vec(), "east".as_bytes().to_vec()),
                    ("server".as_bytes().to_vec(), "a".as_bytes().to_vec()),
                ]),
            },
            Series {
                key: "disk,region=north,server=c".to_string(),
                name: "disk".to_string(),
                tags: HashMap::from([
                    ("region".as_bytes().to_vec(), "north".as_bytes().to_vec()),
                    ("server".as_bytes().to_vec(), "c".as_bytes().to_vec()),
                ]),
            },
        ];

        let mut batch_keys = vec![];
        let mut batch_names = vec![];
        let mut batch_tags = vec![];

        for pt in data {
            assert!(
                idx1.idx
                    .create_series_if_not_exists(
                        pt.key.as_bytes(),
                        pt.name.as_bytes(),
                        crate::models::points::Tags::from(pt.tags.clone())
                    )
                    .is_ok()
            );
            batch_keys.push(pt.key.as_bytes().to_vec());
            batch_names.push(pt.name.as_bytes().to_vec());
            batch_tags.push(crate::models::points::Tags::from(pt.tags));
        }

        let batch_keys = batch_keys
            .iter()
            .map(|k| k.as_slice())
            .collect::<Vec<&[u8]>>();
        assert!(
            idx2.idx
                .create_series_list_if_not_exists(&batch_keys, batch_names, batch_tags)
                .is_ok()
        );

        // 2. Test that correct series are initially returned
        test_tag_value_series_id_iter(
            &[&idx1, &idx2],
            "mem".as_bytes(),
            "region".as_bytes(),
            "west".as_bytes(),
            &[
                "mem,region=west,server=a".to_string(),
                "mem,region=west,server=b".to_string(),
                "mem,region=west,server=c".to_string(),
            ],
        );

        // 3. The result should now be cached, and the same result should be returned.
        test_tag_value_series_id_iter(
            &[&idx1, &idx2],
            "mem".as_bytes(),
            "region".as_bytes(),
            "west".as_bytes(),
            &[
                "mem,region=west,server=a".to_string(),
                "mem,region=west,server=b".to_string(),
                "mem,region=west,server=c".to_string(),
            ],
        );

        // 4. Adding a new series that would be referenced by some cached bitsets (in this case
        // the bitsets for mem->region->west and mem->server->c) should cause the cached
        // bitsets to be updated.
        assert!(
            idx1.idx
                .create_series_if_not_exists(
                    "mem,region=west,root=x,server=c".as_bytes(),
                    "mem".as_bytes(),
                    crate::models::points::Tags::from(HashMap::from([
                        ("region".as_bytes().to_vec(), "west".as_bytes().to_vec()),
                        ("root".as_bytes().to_vec(), "x".as_bytes().to_vec()),
                        ("server".as_bytes().to_vec(), "c".as_bytes().to_vec()),
                    ]))
                )
                .is_ok()
        );

        assert!(
            idx2.idx
                .create_series_if_not_exists(
                    "mem,region=west,root=x,server=c".as_bytes(),
                    "mem".as_bytes(),
                    crate::models::points::Tags::from(HashMap::from([
                        ("region".as_bytes().to_vec(), "west".as_bytes().to_vec()),
                        ("root".as_bytes().to_vec(), "x".as_bytes().to_vec()),
                        ("server".as_bytes().to_vec(), "c".as_bytes().to_vec()),
                    ]))
                )
                .is_ok()
        );

        // 4.1 test insert series
        test_tag_value_series_id_iter(
            &[&idx1, &idx2],
            "mem".as_bytes(),
            "region".as_bytes(),
            "west".as_bytes(),
            &[
                "mem,region=west,root=x,server=c".to_string(),
                "mem,region=west,server=a".to_string(),
                "mem,region=west,server=b".to_string(),
                "mem,region=west,server=c".to_string(),
            ],
        );

        assert!(
            idx1.idx
                .create_series_if_not_exists(
                    "mem,region=west,root=x,server=c".as_bytes(),
                    "mem".as_bytes(),
                    crate::models::points::Tags::from(HashMap::from([
                        ("region".as_bytes().to_vec(), "west".as_bytes().to_vec()),
                        ("root".as_bytes().to_vec(), "x".as_bytes().to_vec()),
                        ("server".as_bytes().to_vec(), "c".as_bytes().to_vec()),
                    ]))
                )
                .is_ok()
        );

        assert!(
            idx2.idx
                .create_series_if_not_exists(
                    "mem,region=west,root=x,server=c".as_bytes(),
                    "mem".as_bytes(),
                    crate::models::points::Tags::from(HashMap::from([
                        ("region".as_bytes().to_vec(), "west".as_bytes().to_vec()),
                        ("root".as_bytes().to_vec(), "x".as_bytes().to_vec()),
                        ("server".as_bytes().to_vec(), "c".as_bytes().to_vec()),
                    ]))
                )
                .is_ok()
        );

        // 4.2 insert same series
        test_tag_value_series_id_iter(
            &[&idx1, &idx2],
            "mem".as_bytes(),
            "region".as_bytes(),
            "west".as_bytes(),
            &[
                "mem,region=west,root=x,server=c".to_string(),
                "mem,region=west,server=a".to_string(),
                "mem,region=west,server=b".to_string(),
                "mem,region=west,server=c".to_string(),
            ],
        );

        // 4.3 no matching series
        test_tag_value_series_id_iter(
            &[&idx1, &idx2],
            "foo".as_bytes(),
            "bar".as_bytes(),
            "zoo".as_bytes(),
            &[] as &[String],
        );
    }

    #[test]
    #[ignore]
    fn test_index_concurrent_write_query() {
        let points = read_line_protocol_test_data();

        let mut keys = vec![vec![]; 1e6 as usize];
        let mut names = vec![vec![]; 1e6 as usize];
        let mut tags = vec![crate::models::Tags::default(); 1e6 as usize];

        for mut pt in points {
            keys.push(pt.key().to_vec());
            names.push(pt.name());
            tags.push(pt.tags());
        }

        let partitions = vec![1, 4, 8, 16];
        let query_n = 1e5 as i32;
        for partition_n in partitions {
            let keys = keys.clone();
            let names = names.clone();
            let tags = tags.clone();

            let wg = WaitGroup::new();

            let keys1 = keys.clone();
            let names1 = names.clone();
            let tags1 = tags.clone();
            let wg2 = wg.clone();
            wg2.add(1);
            spawn_blocking_global(move || {
                let keys = keys1.clone();
                run_concurrent_write_query(
                    query_n,
                    partition_n,
                    crate::config::DEFAULT_SERIES_ID_SET_CACHE_SIZE,
                    keys,
                    names1,
                    tags1,
                );
                wg2.done();
            });

            let wg2 = wg.clone();
            wg2.add(1);
            spawn_blocking_global(move || {
                run_concurrent_write_query(query_n, partition_n, 0, keys, names, tags);
                wg2.done();
            });

            wg.wait();
        }
    }

    use crate::index::SeriesIDIterator;

    fn run_iter_task(index: Arc<Index>, query_n: i32) {
        let keys = vec![
            vec!["m0".to_string(), "tag2".to_string(), "value4".to_string()],
            vec!["m1".to_string(), "tag3".to_string(), "value5".to_string()],
            vec!["m2".to_string(), "tag4".to_string(), "value6".to_string()],
            vec!["m3".to_string(), "tag0".to_string(), "value8".to_string()],
            vec!["m4".to_string(), "tag5".to_string(), "value0".to_string()],
        ];

        for _i in 0..query_n / 5 {
            for key in keys.iter() {
                let itr = index.idx.tag_value_series_id_iter(
                    key[0].as_bytes(),
                    key[1].as_bytes(),
                    key[2].as_bytes(),
                );
                assert!(itr.is_ok());
                let itr = itr.unwrap();
                assert!(itr.is_some(), "got None iter");
                assert!(itr.unwrap().close().is_ok())
            }
        }
    }

    fn run_concurrent_write_query(
        query_n: i32,
        partition: u64,
        cache_size: i64,
        keys: Vec<Vec<u8>>,
        names: Vec<Vec<u8>>,
        tags: Vec<crate::models::Tags>,
    ) -> std::time::Duration {
        let mut total_elapsed = std::time::Duration::ZERO;
        let start = std::time::Instant::now();

        let mut idx = Arc::new({
            let sfile: Arc<crate::series::series_file::SeriesFile> = new_open_series_file();
            let mut idx = super::Index::new(
                Arc::clone(&sfile),
                "foo".to_string(),
                tsi1::tests::must_temp_tsi_dir(),
                crate::config::DEFAULT_MAX_INDEX_LOG_FILE_SIZE,
                cache_size,
            );
            idx.partition_n = partition;

            let mut idx = Index { idx, sfile };

            assert!(idx.open().is_ok());
            idx
        });

        let wg = WaitGroup::new();
        wg.add(1);

        let worker = wg.clone();
        let idx2 = Arc::clone(&idx);
        spawn_blocking_global(move || {
            run_iter_task(idx2, query_n);
            worker.done();
        });

        let batch_size = 10000;

        total_elapsed += start.elapsed();
        let mut i = 0;
        while i < keys.len() {
            let start = std::time::Instant::now();
            let k = keys
                .get(i..i + batch_size)
                .unwrap()
                .iter()
                .map(|k| &k[..])
                .collect::<Vec<&[u8]>>();
            let n = names.get(i..i + batch_size).unwrap().to_vec();
            let t = tags.get(i..i + batch_size).unwrap().to_vec();

            if let Err(e) = idx.idx.create_series_list_if_not_exists(&k, n, t) {
                panic!("{}", e);
            }

            // Wait for queries to finish
            wg.wait();

            total_elapsed += start.elapsed();
            // Reset the index...
            Arc::get_mut(&mut idx).unwrap().close(); // query task has finished

            idx = Arc::new({
                let sfile = new_open_series_file();
                let mut idx = super::Index::new(
                    Arc::clone(&sfile),
                    "foo".to_string(),
                    tsi1::tests::must_temp_tsi_dir(),
                    crate::config::DEFAULT_MAX_INDEX_LOG_FILE_SIZE,
                    cache_size,
                );
                idx.partition_n = partition;

                let mut idx = Index { idx, sfile };

                assert!(idx.open().is_ok());
                idx
            });

            wg.add(1);

            let idx2 = Arc::clone(&idx);
            let worker = wg.clone();
            spawn_blocking_global(move || {
                run_iter_task(idx2, query_n);
                worker.done();
            });

            i += batch_size;
        }

        total_elapsed
    }

    use std::io::Read;
    fn read_line_protocol_test_data() -> Vec<crate::models::points::Point> {
        // Read line-protocol and coerce into tsdb format.
        // 1M series generated with:
        // $inch -b 10000 -c 1 -t 10,10,10,10,10,10 -f 1 -m 5 -p 1
        let path = std::env::current_dir()
            .unwrap()
            .join("src/index/tsi1/testdata/line-protocol-1M.txt.gz");
        let input = std::io::BufReader::new(std::fs::File::open(&path).unwrap());
        let mut decoder = flate2::bufread::GzDecoder::new(input);
        let mut data = vec![];
        let _n = decoder.read_to_end(&mut data);
        let (points, _) = crate::models::points::parse_points(&data).unwrap();
        points
    }

    fn vertify_measurement_are_returned(idx: &Index, expected: Vec<Vec<u8>>) {
        let mut names = vec![];
        idx.idx
            .for_each_measurement_name(|name| {
                names.push(name.to_vec());
            })
            .unwrap();

        assert_eq!(names, expected);
    }

    fn vertify_measurement_exists(idx: &Index, name: &[u8], exists: bool) {
        let b = idx.idx.measurement_exists(name).unwrap();
        assert_eq!(b, exists, "expected: {}, found: {}", exists, b);
    }

    fn vertify_data_is_gone_in_each_stage(idx: &Index, name: &[u8], tag_key: &[u8]) {
        // Verify measurement is gone.
        assert!(
            !idx.idx.measurement_exists(name).unwrap(),
            "expected no measurement"
        );

        // Obtain file set to perform lower level checks.
        let fs = idx.idx.partition_at(0).read().retain_file_set().unwrap();

        // Verify tags & values are gone.
        let e = fs.tag_key_iter(name).unwrap().next();
        if e.is_some() && !e.unwrap().deleted() {
            panic!("expected deleted tag key");
        }

        let itr = fs.tag_value_iter(name, tag_key);
        assert!(itr.is_none(), "expected nil tag value iterator");

        fs.release();
    }

    fn test_tag_value_series_id_iter(
        idxs: &[&Index],
        name: &[u8],
        key: &[u8],
        value: &[u8],
        exp_keys: &[String],
    ) {
        for (i, idx) in idxs.iter().enumerate() {
            let sitr = idx.idx.tag_value_series_id_iter(name, key, value).unwrap();
            let sitr = sitr.expect("series id iter is None");

            // Convert series ids to series keys.
            let mut itr = crate::index::new_series_iterator_adapter(Arc::clone(&idx.sfile), sitr);

            // Iterator was in series id order, which may not be series key order.
            let mut keys = vec![];
            for e in itr {
                keys.push(to_string!(crate::models::points::make_key(
                    e.name(),
                    e.tags()
                )));
            }

            // Iterator was in series id order, which may not be series key order.
            keys.sort();
            assert_eq!(
                keys, exp_keys,
                "[index{}] got {:?}, expected {:?}",
                i, keys, exp_keys
            );
        }
    }

    #[cfg(test)]
    #[ctor::ctor]
    fn init_runtime() {
        let runtime_opt = crate::runtime::global::RuntimeOptions::default();
        crate::runtime::global::init_global_runtimes(&runtime_opt);
    }

    // Index is a test wrapper for tsi1.Index.
    #[test]
    fn measurement_scan_releases_index_lock_before_waiting_for_partition() {
        let mut idx = must_open_index(1);
        idx.idx.create_series_if_not_exists(b"cpu", b"cpu", crate::models::Tags::default()).unwrap();
        let partition = idx.idx.partition(b"cpu");
        let held = partition.write();
        let (started, ready) = std::sync::mpsc::channel();
        let available = std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                started.send(()).unwrap();
                let result = idx.idx.measurement_series_id_iter(b"cpu").unwrap();
                if let Some(itr) = result { crate::index::SeriesIDIterator::close(&itr).unwrap(); }
            });
            ready.recv().unwrap();
            std::thread::sleep(std::time::Duration::from_millis(30));
            let available = idx.idx.inner.try_write_for(std::time::Duration::from_millis(100)).is_some();
            drop(held);
            worker.join().unwrap();
            available
        });
        idx.close();
        assert!(available, "query held index metadata while waiting for a partition");
    }


    #[test]
    fn single_series_write_releases_partition_before_index_metadata() {
        let mut idx = must_open_index(1);
        idx.idx.create_series_if_not_exists(b"cpu", b"cpu", crate::models::Tags::default()).unwrap();
        let partition = idx.idx.partition(b"cpu");
        let held = idx.idx.inner.read();
        let (started, ready) = std::sync::mpsc::channel();
        let available = std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                started.send(()).unwrap();
                idx.idx.create_series_if_not_exists(b"cpu", b"cpu", crate::models::Tags::default()).unwrap();
            });
            ready.recv().unwrap();
            std::thread::sleep(std::time::Duration::from_millis(30));
            let available = partition.try_read_for(std::time::Duration::from_millis(100)).is_some();
            drop(held);
            worker.join().unwrap();
            available
        });
        idx.close();
        assert!(available, "writer held partition while waiting for index metadata");
    }

    pub struct Index {
        pub idx: super::Index,
        pub sfile: Arc<crate::series::series_file::SeriesFile>,
    }

    impl Index {
        pub fn new(partition_n: u64) -> Self {
            let sfile = new_open_series_file();

            let mut idx = Self {
                idx: super::Index::new(
                    Arc::clone(&sfile),
                    "db0".to_string(),
                    tsi1::tests::must_temp_tsi_dir(),
                    DEFAULT_MAX_INDEX_LOG_FILE_SIZE,
                    DEFAULT_SERIES_ID_SET_CACHE_SIZE,
                ),
                sfile,
            };
            idx.idx.partition_n = partition_n;

            idx
        }

        /// Open the underlying tsi1.Index and tsdb.SeriesFile
        pub fn open(&mut self) -> Result<(), crate::index::tsi1::index::Error> {
            self.idx.open()
        }

        /// Close and removes the index directory.
        pub fn close(&mut self) {
            let _ = self.sfile.close();
            std::fs::remove_dir_all(self.sfile.path()).unwrap();

            let _ = self.idx.close();
            std::fs::remove_dir_all(self.idx.path()).unwrap();
        }

        /// Close and open the index.
        pub fn reopen(&mut self) -> Result<(), crate::index::tsi1::index::Error> {
            self.idx.close().unwrap();

            // Reopen the series file correctly, by initialising a new underlying series
            // file using the same disk data.
            // self.sfile.reopen().unwrap();
            let path = self.sfile.path();
            self.sfile.close().unwrap();

            let database = self.idx.database.clone();
            let index_path = self.idx.path.clone();
            let partition_n = self.idx.partition_n; // Remember how many partitions to use.
            let max_log_file_size = self.idx.max_log_file_size;
            let tag_value_cache_size = self.idx.tag_value_cache_size;
            let logfile_buffer_size = self.idx.logfile_buffer_size;
            let disable_fsync = self.idx.disable_fsync;
            let disable_compactions = self.idx.disable_compactions;

            let mut sfile = crate::series::series_file::SeriesFile::new(path);
            sfile.open().unwrap();
            self.sfile = Arc::new(sfile);

            self.idx = super::Index::new(
                Arc::clone(&self.sfile),
                database,
                index_path,
                max_log_file_size,
                tag_value_cache_size,
            );
            self.idx.partition_n = partition_n;
            self.idx.logfile_buffer_size = logfile_buffer_size;
            self.idx.disable_fsync = disable_fsync;
            self.idx.disable_compactions = disable_compactions;

            self.idx.open()
        }

        /// Creates multiple series at a time.
        pub fn create_series_slice_if_not_exists(
            &mut self,
            a: Vec<Series>,
        ) -> crate::index::tsi1::index::Result<()> {
            let mut keys = Vec::with_capacity(a.len());
            let mut names = Vec::with_capacity(a.len());
            let mut tags = Vec::with_capacity(a.len());

            for s in a {
                keys.push(crate::models::points::make_key(&s.name, &s.tags));
                names.push(s.name);
                tags.push(s.tags);
            }

            let keys = keys.iter().map(|k| k.as_slice()).collect::<Vec<&[u8]>>();
            self.idx
                .create_series_list_if_not_exists(&keys, names, tags)
        }

        // Executes a subtest for each of several different states:
        //
        // - Immediately
        // - After reopen
        // - After compaction
        // - After reopen again
        //
        // The index should always respond in the same fashion regardless of
        // how data is stored. This helper allows the index to be easily tested
        // in all major states.
        pub fn run<F>(&mut self, mut f: F)
        where
            F: FnMut(),
        {
            // Invoke immediately.
            f();

            // Reopen and invoke again.
            assert!(self.reopen().is_ok(), "reopen failed");
            f();

            // TODO: Request a compaction.
            // if err := idx.Compact(); err != nil {
            // 	t.Fatalf("compact error: %s", err)
            // }
            // t.Run("state=post-compaction", fn)

            // Reopen and invoke again.
            assert!(self.reopen().is_ok(), "post-compaction reopen failed");
            f();
        }
    }

    impl Default for Index {
        fn default() -> Self {
            Index::new(DEFAULT_PARTITION_N)
        }
    }

    /// Returns a new instance of Index with default number of partitions at a temporary path.
    pub fn new_default_index() -> Index {
        Index::new(DEFAULT_PARTITION_N)
    }

    fn new_open_series_file() -> Arc<crate::series::series_file::SeriesFile> {
        let mut sfile: crate::series::series_file::SeriesFile =
            crate::series::series_file::tests::new_series_file().into();
        sfile.open().unwrap();
        Arc::new(sfile)
    }

    /// Returns a new, open index. Panic on error.
    pub fn must_open_index(partition_n: u64) -> Index {
        let mut idx = Index::new(partition_n);
        assert!(idx.open().is_ok(), "open index failed");
        idx
    }

    /// Returns a new, open index with the default number of partitions.
    pub fn must_open_default_index() -> Index {
        let mut idx = Index::new(DEFAULT_PARTITION_N);
        assert!(idx.open().is_ok(), "open index failed");
        idx
    }
}
