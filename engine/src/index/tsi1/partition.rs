// //! TSI partition.
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    collections::{HashSet, VecDeque},
    io::{self, Write},
    os::unix::{ffi::OsStrExt, fs::PermissionsExt},
    panic,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::common::hll::HLL;
use crate::runtime::execution::{
    AcceptedTaskRegistration, ExecutorRef, RuntimeExecutorExt, SpawnError, accepted_task_guard,
    legacy_global_executor,
};
use crossbeam_channel::select;
use futures::{FutureExt, SinkExt, future::BoxFuture};
use parking_lot::RwLock;
use regex::Regex;
use scopeguard::defer;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu, ensure};
use tokio_util::sync::CancellationToken;

use super::{
    file_set::{
        self, File, FileSet, FileSetSeriesIDIter, OneFileSetMeasurementIter, OneFileSetTagKeyIter,
        OneFileSetTagValueIter, new_file_set_series_id_iter, new_one_file_set_measurement_iter,
        new_one_file_set_tag_key_iter, new_one_file_set_tag_value_iter,
    },
    index_file::{self, IndexFile, formant_index_file_name},
    index_files::IndexFiles,
    log_file::{self, LogFile, format_log_file_name},
};
use crate::{
    common::slow_trace,
    config::*,
    index::{
        SeriesIDIter, SeriesIDIterator,
        tsi1::{
            MeasurementElement, TSDBMeasurementIterAdapter, TSDBTagKeyIterAdapter,
            TSDBTagValueIterAdapter, TagKeyElement, TagValueElement,
        },
    },
    series::{measurement_fields::MeasurementFieldSet, series_file::SeriesFile, series_set},
};

/// VERSION is the current version of the IST index.
pub const VERSION: i32 = 1;

/// File extensions.
pub(crate) const LOG_FILE_EXT: &str = "tsl";
pub(crate) const INDEX_FILE_EXT: &str = "tsi";
pub(crate) const COMPACTING_EXT: &str = "compacting";

/// ManifestFileName is the name of the index manifest file.
pub(crate) const MANIFEST_FILE_NAME: &str = "MANIFEST";

#[cfg(test)]
static FAIL_COMPACT_LOG_FILE_MANIFEST_WRITE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static PANIC_COMPACT2_BODY: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static PANIC_CHECK_LOG_FILE_BODY: AtomicBool = AtomicBool::new(false);

/// Error type for Partition.
#[derive(Debug, Snafu)]
pub enum Error {
    /// ErrIncompatibleVersion is returned when attempting to read from an incompatible tsi1 manifest file.
    #[snafu(display("incompatible tsi1 index MANIFEST"))]
    ErrIncompatibleVersion,

    /// ErrIndexClosing can be returned to from an Index method if the index is currently closing.
    #[snafu(display("index is closing"))]
    ErrIndexClosing,

    /// IO error.
    #[snafu(display("io error: {}", source))]
    IOError { source: io::Error },

    /// Index file error.
    #[snafu(display("tsdb: {}", source))]
    IndexFile { source: index_file::Error },

    /// Lof file error.
    #[snafu(display("tsdb: {}", source))]
    LogFile { source: log_file::Error },

    /// File set error.
    #[snafu(display("tsdb: {}", source))]
    FileSet { source: file_set::Error },

    /// String error.
    #[snafu(display("tsdb: {}", reason))]
    StringErr { reason: String },

    #[snafu(display("tsi1 partition task scheduling failed: {}", source))]
    Execution { source: SpawnError },

    /// Cancel send error.
    #[snafu(display("tsdb: {}", source))]
    CancelSendErr {
        source: crossbeam_channel::SendError<()>,
    },

    /// Cancel recv error.
    #[snafu(display("tsdb: {}", source))]
    CancelRecvErr {
        source: crossbeam_channel::RecvError,
    },
}

/// Result type for Partition.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IOError { source: err }
    }
}

/// Partition represents a collection of layered index files and WAL.
pub struct Partition {
    // Index's version.
    version: i32,

    // Directory of the Partition's index files.
    path: PathBuf, // open时lock
    id: String,    // id portion of path.  // open时lock

    // Log file compaction thresholds.
    pub max_log_file_size: i64,
    pub nosync: bool, // when true, flushing and syncing of LogFile will be disabled.  // openLogFile 时
    pub logbuffer_size: i64, // the LogFile's buffer is set to this value.

    // Close management.
    once: std::sync::Once,      // FIXME:
    closing: CancellationToken, // closing is used to inform iterators the partition is closing.

    compaction_interrupt: CancellationToken, // FIXME: 需要lock？ 在获取之前需要先 lock inner

    sfile: Arc<SeriesFile>,
    executor: ExecutorRef,

    pub(crate) inner: Arc<RwLock<Inner>>,
}

// // compact2缺少
// closing
// compaction_interrupt

#[derive(Default)]
pub(crate) struct Inner {
    opened: bool,
    // Fast series lookup of series IDs in the series file that have been present
    // in this partition. This set tracks both insertions and deletions of a series.
    pub(crate) series_id_set: Arc<series_set::SeriesIDSet>, // Open时lock，DropSeries从seriesIDSet中Remove(seriesID)不需lock？

    fieldset: Arc<MeasurementFieldSet>, // Fieldset shared with engine.

    // Frequency of compaction checks.
    compactions_disabled: i32,

    active_log_file: Option<Arc<RwLock<LogFile>>>, //lock  // 需要在goroutine，同时需要&mut self  compact log file and index file
    pub(crate) file_set: Arc<FileSet>,             // current file set
    seq: i64,                                      // file id sequence

    // Compaction management
    levels: Vec<CompactionLevel>, // compaction levels
    level_compacting: Vec<bool>,  // level compaction status

    current_compaction_n: i64, // counter of in-progress compactions

    // Current size of MANIFEST. Used to determine partition size.
    manifest_size: i64,
}

impl Default for Partition {
    fn default() -> Self {
        Self {
            version: VERSION,
            path: PathBuf::new(),
            id: String::new(),

            max_log_file_size: 0i64,
            nosync: false,
            logbuffer_size: 0i64,

            once: std::sync::Once::new(),
            closing: CancellationToken::default(),

            compaction_interrupt: CancellationToken::default(),

            sfile: Arc::new(SeriesFile::default()),
            executor: legacy_global_executor(),
            inner: Arc::new(RwLock::new(Inner::default())),
        }
    }
}

impl Inner {
    /// Returns the path to the index's manifest file.
    pub fn manifest_path(&self, path: &Path) -> PathBuf {
        path.join(MANIFEST_FILE_NAME)
    }

    /// Returns a manifest for the index.
    pub fn manifest(&self, path: &Path, version: i32) -> Manifest {
        let mut m = Manifest {
            levels: self.levels.clone(),
            files: Vec::with_capacity(self.file_set.len()),
            version,
            path: self.manifest_path(path),
        };

        for f in self.file_set.files.iter() {
            m.files.push(
                f.path()
                    .file_name()
                    .unwrap()
                    .to_owned()
                    .into_string()
                    .unwrap(),
            );
        }

        m
    }

    fn compactions_enabled(&self) -> bool {
        self.compactions_disabled == 0
    }
}

// // compactLogFile
// compaction_interrupt
// sfile
// file_set
// manifest_size

// active_log_file

// // compact() -> compactToLevel
// compaction_interrupt
// levels
// sfile
// file_set
// manifest_size

// seq
// level_compacting
// current_compaction_n

impl Partition {
    /// Returns a new instance of Partition.
    pub fn new(sfile: Arc<SeriesFile>, path: impl Into<PathBuf>) -> Self {
        Self::new_with_executor(sfile, path, legacy_global_executor())
    }

    pub fn new_with_executor(
        sfile: Arc<SeriesFile>,
        path: impl Into<PathBuf>,
        executor: ExecutorRef,
    ) -> Self {
        Self {
            version: VERSION,
            path: path.into(),
            id: String::new(),

            max_log_file_size: DEFAULT_MAX_INDEX_LOG_FILE_SIZE,
            nosync: false,
            logbuffer_size: 0,

            once: std::sync::Once::new(),
            closing: CancellationToken::new(),
            compaction_interrupt: CancellationToken::new(),

            sfile,
            executor,
            inner: Arc::new(RwLock::new(Inner::default())),
        }
    }

    /// Eestimates the memory footprint of this Partition, in bytes.
    pub fn bytes(&self) -> i64 {
        todo!("implement me")
    }

    /// Open the partition.
    pub fn open(&mut self) -> Result<()> {
        let inner_arc = Arc::clone(&self.inner);

        let mut inner = self.inner.write();
        self.closing = CancellationToken::new();

        ensure!(
            !inner.opened,
            StringErrSnafu {
                reason: "index partition already open".to_string(),
            }
        );

        // Validate path is correct.
        self.id = self
            .path
            .file_name()
            .unwrap()
            .to_os_string()
            .into_string()
            .unwrap();

        ensure!(
            atoi_simd::parse::<i32>(self.id.as_bytes()).is_ok(),
            StringErrSnafu {
                reason: format!(
                    "invalid index partition base name: {:?}",
                    self.path.file_name().unwrap()
                ),
            }
        );

        // Create directory if it doesn't exist.
        if !Path::new(&self.path).exists() {
            std::fs::create_dir_all(&self.path).expect("Failed to create directory");
        }
        std::fs::metadata(&self.path)?.permissions().set_mode(0o777);

        // Read manifest file.
        let mut m: Manifest;
        let manifest_size;
        match read_manifest_file(&self.path.join(MANIFEST_FILE_NAME)) {
            Ok((mf, msize)) => {
                m = mf;
                manifest_size = msize;
            }
            Err(e) => {
                if let Error::IOError { source } = e {
                    manifest_size = 0;
                    if source.kind() == io::ErrorKind::NotFound {
                        m = Manifest::new();
                        m.path = self.manifest_path();
                    } else {
                        return Err(Error::IOError { source });
                    }
                } else {
                    return Err(e);
                }
            }
        }

        // Set manifest size on the partition
        inner.manifest_size = manifest_size;

        // Check to see if the MANIFEST file is compatible with the current Index.
        m.valid()?;

        // Copy compaction levels to the index.
        inner.levels = vec![];
        inner.levels.extend_from_slice(&m.levels);

        // Set up flags to track whether a level is compacting.
        inner.level_compacting = vec![false; m.levels.len()];

        // Open each file in the manifest.
        let mut files = VecDeque::new();
        for file_name in &m.files {
            match Path::new(file_name).extension().unwrap().to_str().unwrap() {
                LOG_FILE_EXT => {
                    let f = self.open_log_file(&self.path.join(file_name))?;
                    let (sz, _) = f.stat();
                    let f = Arc::new(RwLock::new(f));

                    files.push_back(File::LogFile(Arc::clone(&f)));
                    // Make first log file active, if within threshold.
                    if inner.active_log_file.is_none() && sz < self.max_log_file_size {
                        inner.active_log_file = Some(f);
                    }
                }
                INDEX_FILE_EXT => {
                    files.push_back(File::IndexFile(Arc::new(RwLock::new(
                        self.open_index_file(&self.path.join(file_name))?,
                    ))));
                }
                _ => {}
            }
        }
        inner.file_set = Arc::new(FileSet::new(
            inner.levels.clone(),
            Arc::clone(&self.sfile),
            files,
        ));

        // Set initial sequence number.
        inner.seq = inner.file_set.max_id();

        // Delete any files not in the manifest.
        self.delete_non_manifest_files(m)?;

        // Ensure a log file exists.
        if inner.active_log_file.is_none() {
            self.prepend_active_log_file(&mut inner)?;
        }

        // Build series existance set.
        self.build_series_set(&mut inner)?;

        // Mark opened.
        inner.opened = true;

        // Send a compaction request on start up.
        let compaction_registrations = Self::compact2(
            self.path.to_path_buf(),
            self.version,
            Arc::clone(&self.sfile),
            inner_arc,
            &mut inner,
            self.closing.clone(),
            self.compaction_interrupt.clone(),
            Arc::clone(&self.executor),
        );
        drop(inner);
        for registration in compaction_registrations {
            registration.accepted();
        }

        Ok(())
    }

    /// Open a log file and appends it to the index.
    fn open_log_file(&self, path: &Path) -> Result<LogFile> {
        let mut f = LogFile::new(path, Arc::clone(&self.sfile));
        f.open(self.logbuffer_size as usize, self.nosync)
            .context(LogFileSnafu)?;
        Ok(f)
    }

    /// Open a index file and appends it to the index.
    fn open_index_file(&self, path: &Path) -> Result<IndexFile> {
        let mut f = IndexFile::new(Arc::clone(&self.sfile));
        f.set_path(path);
        f.open().context(IndexFileSnafu)?;
        Ok(f)
    }

    /// Removes all files not in the manifest.
    fn delete_non_manifest_files(&self, m: Manifest) -> Result<()> {
        let entries = std::fs::read_dir(&self.path)?;
        for entry in entries {
            let file_path = entry?.path();

            let file_name = file_path.file_name().unwrap();
            if file_name == MANIFEST_FILE_NAME || m.has_file(file_name.as_bytes()) {
                continue;
            }

            std::fs::remove_file(file_name)?;
        }

        Ok(())
    }

    fn build_series_set(&self, inner: &mut Inner) -> Result<()> {
        let fs = self.retain_file_set3(inner)?;
        defer! {
            fs.release();
        }

        inner.series_id_set = Arc::new(series_set::SeriesIDSet::new(&[]));
        // Read series sets from files in reverse.
        for f in fs.files.iter().rev() {
            // Delete anything that's been tombstoned.
            let ts = f.tombstone_series_id_set().context(FileSetSnafu)?;
            inner.series_id_set.diff(&ts);

            // Add series created within the file.
            let ss = f.series_id_set().context(FileSetSnafu)?;
            inner.series_id_set.merge(&[&ss]);
        }

        Ok(())
    }

    /// Returns the number of compactions currently running.
    pub fn current_compaction_n(&self) -> i64 {
        self.inner.read().current_compaction_n
    }

    /// Wait will block until all compactions are finished.
    /// Must only be called while they are disabled.
    pub fn wait(&self) {
        let ticker = crossbeam_channel::tick(std::time::Duration::from_millis(10));
        loop {
            select! {
                recv(ticker) -> _ => {
                    if self.current_compaction_n() == 0 {
                        return;
                    }
                }
            }
        }
    }

    /// Close the index.
    pub fn close(&mut self) -> Result<()> {
        // Wait for goroutines to finish outstanding compactions.
        self.once.call_once(|| {
            self.closing.cancel();
            self.compaction_interrupt.cancel();
        });
        self.wait();

        // Lock index and close remaining
        let mut inner = self.inner.write();

        // Close log files.
        let mut res: Result<(), Error> = Ok(());
        for f in inner.file_set.files.iter() {
            if let Err(e) = f.close() {
                res = Err(Error::FileSet { source: e });
            }
        }
        // FIXME: just one reference ?
        Arc::get_mut(&mut inner.file_set).unwrap().files.clear();

        res
    }

    /// Returns true if the partition is currently closing. It does not require a lock so will always return to callers.
    fn is_closing(&self) -> bool {
        if self.closing.is_cancelled() {
            return true;
        }
        false
    }

    /// Returns the path to the partition.
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    /// Returns the attached series file.
    pub fn series_file(&self) -> Arc<SeriesFile> {
        Arc::clone(&self.sfile)
    }

    /// Returns the next file identifier.
    fn next_sequence(inner: &mut Inner) -> i64 {
        // when call this function, Arc<RwLock<Inner>> has been locked.
        inner.seq += 1;
        inner.seq
    }

    fn next_sequence2(inner: Arc<RwLock<Inner>>) -> i64 {
        let mut inner = inner.write();
        inner.seq += 1;
        inner.seq
    }

    /// Returns the path to the index's manifest file.
    pub fn manifest_path(&self) -> PathBuf {
        self.path.join(MANIFEST_FILE_NAME)
    }

    /// Returns the manifest size to the index's manifest file.
    pub fn manifest_size(&self) -> i64 {
        self.inner.read().manifest_size
    }

    /// Returns a manifest for the index.
    pub fn manifest(&self, inner: &mut Inner) -> Manifest {
        // let inner = self.inner.read();
        let mut m = Manifest {
            levels: inner.levels.clone(),
            files: Vec::with_capacity(inner.file_set.len()),
            version: self.version,
            path: self.manifest_path(),
        };

        for f in inner.file_set.files.iter() {
            m.files.push(
                f.path()
                    .file_name()
                    .unwrap()
                    .to_owned()
                    .into_string()
                    .unwrap(),
            );
        }

        m
    }

    /// Sets a shared field set from the engine.
    pub fn set_field_set(&mut self, fs: Arc<MeasurementFieldSet>) {
        self.inner.write().fieldset = fs;
    }

    /// Returns the fieldset.
    pub fn field_set(&self) -> Arc<MeasurementFieldSet> {
        Arc::clone(&self.inner.write().fieldset)
    }

    /// Returns the current fileset and adds a reference count.
    pub fn retain_file_set(&self) -> Result<Arc<FileSet>> {
        if self.closing.is_cancelled() {
            return Err(Error::ErrIndexClosing);
        }

        let inner_read_start = slow_trace::now();
        let inner = self.inner.read();
        slow_trace::log_lock_wait(
            "index_partition.read",
            inner_read_start,
            "stage=query_wait_for_index_lock component=partition_retain_file_set_inner_read",
        );
        let fs = Arc::clone(&inner.file_set);
        slow_trace::log_query_stage(
            "query_index_lookup",
            inner_read_start,
            "component=partition_retain_file_set_inner_read",
        );

        let retain_start = slow_trace::now();
        fs.retain_for_query_measurement_exists();
        slow_trace::log_query_stage(
            "query_index_lookup",
            retain_start,
            "component=partition_retain_file_set_retain",
        );
        drop(inner);
        Ok(fs)
    }

    pub fn retain_file_set3(&self, inner: &mut Inner) -> Result<Arc<FileSet>> {
        if self.closing.is_cancelled() {
            return Err(Error::ErrIndexClosing);
        }

        // let inner = self.inner.read();
        let fs = Arc::clone(&inner.file_set);
        fs.retain();
        Ok(fs)
    }

    fn retain_file_set2(file_set: &Arc<FileSet>) -> Arc<FileSet> {
        let fs = Arc::clone(file_set);
        fs.retain();

        fs
    }

    /// Returns the active files in the file set.
    pub fn len(&self) -> usize {
        self.inner.read().file_set.len()
    }

    /// Adds a new log file so that the current log file can be compacted.
    fn prepend_active_log_file(&self, inner: &mut Inner) -> Result<()> {
        // Open file and insert it into the first position.

        let log_file = self.open_log_file(
            &self
                .path
                .join(format_log_file_name(Self::next_sequence(inner))),
        )?;

        let f = Arc::new(RwLock::new(log_file));

        // Prepend and generate new fileset.
        let file_set = Arc::clone(&inner.file_set);
        inner.file_set = Arc::new(file_set.prepend_log_file(Arc::clone(&f)));

        inner.active_log_file = Some(f);

        // Write new manifest.
        let manifest_size = self.manifest(inner).write()?; // TODO: Close index if write fails.
        inner.manifest_size = manifest_size;

        Ok(())
    }

    /// Iterates over all measurement names in the index.
    pub fn for_each_measurement_name<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        let fs = self.retain_file_set()?;
        defer! {
            fs.release();
        }

        if let Some(itr) = fs.measurement_iter() {
            for e in itr {
                if let Some(name) = e.name() {
                    f(name)?;
                }
            }
        }

        Ok(())
    }

    /// Returns true if a measurement has at least one non-tombstoned series.
    pub fn measurement_has_series(&self, name: &[u8]) -> Result<bool> {
        let fs = self.retain_file_set()?;
        defer! {
            fs.release();
        }

        let inner = self.inner.read();

        for f in fs.files.iter() {
            if f.measurement_has_series(inner.series_id_set.as_ref(), name) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Returns an iterator over all measurement names.
    pub fn measurement_iter(&self) -> Result<Option<OneFileSetMeasurementIter>> {
        let fs = self.retain_file_set()?;
        let fs_clone = Arc::clone(&fs);

        match fs.measurement_iter() {
            Some(itr) => Ok(Some(new_one_file_set_measurement_iter(
                fs_clone,
                TSDBMeasurementIterAdapter::new(itr),
            ))),
            _ => {
                fs.release();
                Ok(None)
            }
        }
    }

    /// Returns true if a measurement exists.
    pub fn measurement_exists(&self, name: &[u8]) -> Result<bool> {
        let retain_start = slow_trace::now();
        let fs = self.retain_file_set()?;
        slow_trace::log_query_stage(
            "query_index_lookup",
            retain_start,
            "component=partition_measurement_exists_retain_file_set",
        );
        defer! {
            fs.release();
        }

        let lookup_start = slow_trace::now();
        let exists = fs.measurement_exists(name);
        slow_trace::log_query_stage(
            "query_index_lookup",
            lookup_start,
            "component=partition_measurement_exists_file_set",
        );

        Ok(exists)
    }

    pub fn measurement_series_id_iter(&self, name: &[u8]) -> Result<Option<FileSetSeriesIDIter>> {
        let fs = self.retain_file_set()?;
        let itr = fs.measurement_series_id_iter(name);
        Ok(new_file_set_series_id_iter(fs, itr))
    }

    /// Deletes a measurement from the index.
    /// drop_measurement does not remove any series from the index directly.
    pub fn drop_measurement(&self, name: &[u8]) -> Result<()> {
        let fs = self.retain_file_set()?;
        defer! {
            fs.release();
        }

        let (tag_keys, tag_values, series_ids) = {
            let mut tag_keys = Vec::<Vec<u8>>::new();
            let mut tag_values = Vec::<(Vec<u8>, Vec<u8>)>::new();
            let mut series_ids = Vec::<u64>::new();

            if let Some(kitr) = fs.tag_key_iter(name) {
                for k in kitr {
                    if !k.deleted() {
                        tag_keys.push(k.key().to_vec());
                    }

                    if let Some(vitr) = k.tag_value_iterator() {
                        for v in vitr {
                            if !v.deleted() {
                                tag_values.push((k.key().to_vec(), v.value().to_vec()));
                            }
                        }
                    }
                }
            }

            if let Some(mut itr) = fs.measurement_series_id_iter(name) {
                for elem in itr.by_ref() {
                    if elem.series_id == 0 {
                        break;
                    }
                    series_ids.push(elem.series_id);
                }
                itr.close().map_err(|e| Error::StringErr {
                    reason: e.to_string(),
                })?;
            }

            (tag_keys, tag_values, series_ids)
        };

        {
            let inner = self.inner.read();

            let active_log_file = inner.active_log_file.as_ref().unwrap();
            for key in &tag_keys {
                active_log_file
                    .read()
                    .delete_tag_key(name, key.as_slice())
                    .context(LogFileSnafu)?;
            }

            for (key, value) in &tag_values {
                active_log_file
                    .read()
                    .delete_tag_value(name, key.as_slice(), value.as_slice())
                    .context(LogFileSnafu)?;
            }

            for series_id in series_ids {
                active_log_file
                    .read()
                    .delete_series_id(series_id)
                    .context(LogFileSnafu)?;
                inner.series_id_set.remove(series_id);
            }

            active_log_file
                .read()
                .delete_measurement(name)
                .context(LogFileSnafu)?;
        }

        // Check if the log file needs to be swapped.
        self.check_log_file()
    }

    /// Creates a list of series if they doesn't exist in bulk.
    pub fn create_series_list_if_not_exists(
        &self,
        names: &[&[u8]],
        tags_slice: &[&crate::models::points::Tags],
    ) -> Result<Option<Vec<u64>>> {
        // Is there anything to do? The partition may have been sent an empty batch.
        if names.is_empty() {
            return Ok(None);
        }
        ensure!(
            names.len() == tags_slice.len(),
            StringErrSnafu {
                reason: format!(
                    "uneven batch, partition {} sent {} names and {} tags",
                    self.id,
                    names.len(),
                    tags_slice.len()
                ),
            }
        );

        // Maintain reference count on files in file set.
        let fs = self.retain_file_set()?;
        defer! {
            fs.release();
        }

        // Ensure fileset cannot change during insert.
        let ids;
        {
            let inner = self.inner.read();
            // Insert series into log file.
            ids = inner
                .active_log_file
                .as_ref()
                .unwrap()
                .read()
                .add_series_list(Arc::clone(&inner.series_id_set), names, tags_slice)
                .context(LogFileSnafu)?;
        }

        self.check_log_file()?;

        Ok(Some(ids))
    }

    pub fn drop_series(&self, series_id: u64) -> Result<()> {
        // Delete series from index.
        {
            let inner = self.inner.read();
            inner
                .active_log_file
                .as_ref()
                .unwrap()
                .read()
                .delete_series_id(series_id)
                .context(LogFileSnafu)?;

            // FIXME: we should unlock before remove series_id from self.series_id_set
            inner.series_id_set.remove(series_id);
        }

        // Swap log file, if necessary.
        self.check_log_file()
    }

    /// Returns the two sketches for the partition by merging all instances of
    /// the type sketch types in all the index files.
    pub fn measurement_sketches(&self) -> Result<(HLL, HLL)> {
        let fs = self.retain_file_set()?;
        defer! {
            fs.release();
        }

        fs.measurements_sketches().context(FileSetSnafu)
    }

    /// Returns the two sketches for the partition by merging all instances of
    /// the type sketch types in all the index files.
    pub fn series_sketches(&self) -> Result<(HLL, HLL)> {
        let fs = self.retain_file_set()?;
        defer! {
            fs.release();
        }

        fs.series_sketches().context(FileSetSnafu)
    }

    /// Returns true if tag key exists.
    pub fn has_tag_key(&self, name: &[u8], key: &[u8]) -> Result<bool> {
        let fs = self.retain_file_set()?;
        defer! {
            fs.release();
        }

        Ok(fs.has_tag_key(name, key))
    }

    /// Returns true if tag value exists.
    pub fn has_tag_value(&self, name: &[u8], key: &[u8], value: &[u8]) -> Result<bool> {
        let fs = self.retain_file_set()?;
        defer! {
            fs.release();
        }

        Ok(fs.has_tag_value(name, key, value))
    }

    /// Returns an iterator for all keys across a single measurement.
    pub fn tag_key_iter(&self, name: &[u8]) -> Result<Option<OneFileSetTagKeyIter>> {
        let fs = self.retain_file_set()?;

        match fs.tag_key_iter(name) {
            Some(itr) => Ok(Some(new_one_file_set_tag_key_iter(
                fs,
                TSDBTagKeyIterAdapter::new(itr),
            ))),
            _ => {
                fs.release();
                Ok(None)
            }
        }
    }

    /// Returns an iterator for all values across a single key.
    pub fn tag_value_iter(
        &self,
        name: &[u8],
        key: &[u8],
    ) -> Result<Option<OneFileSetTagValueIter>> {
        let fs = self.retain_file_set()?;

        match fs.tag_value_iter(name, key) {
            Some(itr) => Ok(Some(new_one_file_set_tag_value_iter(
                fs,
                TSDBTagValueIterAdapter::new(itr),
            ))),
            _ => {
                fs.release();
                Ok(None)
            }
        }
    }

    /// Returns a series iterator for all values across a single key.
    pub fn tag_key_series_id_iter(
        &self,
        name: &[u8],
        key: &[u8],
    ) -> Result<Option<FileSetSeriesIDIter>> {
        let fs = self.retain_file_set()?;

        match fs.tag_key_series_id_iter(name, key) {
            Ok(itr) => {
                if let Some(itr) = itr {
                    Ok(new_file_set_series_id_iter(fs, Some(itr)))
                } else {
                    fs.release();
                    Ok(None)
                }
            }
            Err(e) => {
                fs.release();
                Err(Error::FileSet { source: e })
            }
        }
    }

    /// Returns a series iterator for a single key value.
    pub fn tag_value_series_id_iter(
        &self,
        name: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<FileSetSeriesIDIter>> {
        let fs = self.retain_file_set()?;

        match fs.tag_value_series_id_iter(name, key, value) {
            Ok(itr) => Ok(new_file_set_series_id_iter(
                fs,
                Some(SeriesIDIter::SeriesIDSetIter(itr)),
            )),
            _ => {
                fs.release();
                Ok(None)
            }
        }
    }

    /// Returns the tag keys for the measurement.
    pub fn measurement_tag_keys(&self, name: &[u8]) -> Result<HashSet<String>> {
        let fs = self.retain_file_set()?;

        let m = fs.measurement_tag_keys(name);

        fs.release();

        Ok(m)
    }

    /// Iterates over all tag keys in a measurement.
    pub fn for_each_measurement_tag_key<F>(&self, name: &[u8], mut f: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        let fs = self.retain_file_set()?;
        defer! {
            fs.release();
        }

        if let Some(itr) = fs.tag_key_iter(name) {
            for e in itr {
                f(e.key())?;
            }
        }

        Ok(())
    }

    /// Always returns zero.
    /// It is not possible to determine cardinality of tags across index files.
    pub fn tag_key_cardinality(&self, _name: &[u8], _key: &[u8]) -> i32 {
        0
    }

    pub fn disable_compactions(&mut self) {
        let mut inner = self.inner.write();
        inner.compactions_disabled += 1;

        if self.closing.is_cancelled() {
            return;
        }

        if inner.compactions_disabled == 0 {
            self.compaction_interrupt.cancel();
            self.compaction_interrupt = CancellationToken::new();
        }
    }

    pub fn enable_compactions(&self) {
        let mut inner = self.inner.write();
        // Already enabled?
        if inner.compactions_disabled == 0 {
            return;
        }
        inner.compactions_disabled -= 1;
    }

    fn compactions_enabled(&self) -> bool {
        self.inner.read().compactions_enabled()
    }

    fn finish_level_compaction(
        inner: &Arc<RwLock<Inner>>,
        level: i64,
        retained_files: &RetainedIndexFiles,
    ) {
        retained_files.release();
        let mut inner = inner.write();
        inner.level_compacting[level as usize] = false;
        inner.current_compaction_n -= 1;
    }

    fn finish_log_compaction(inner: &Arc<RwLock<Inner>>) {
        inner.write().current_compaction_n -= 1;
    }

    fn compact2(
        path: PathBuf,
        version: i32,
        sfile: Arc<SeriesFile>,
        inner: Arc<RwLock<Inner>>,
        inner_ref: &mut Inner,
        closing: CancellationToken,
        compaction_interrupt: CancellationToken,
        executor: ExecutorRef,
        // ) -> BoxFuture<'static, ()> {
    ) -> Vec<AcceptedTaskRegistration> {
        let mut registrations = Vec::new();
        let inner_arc = Arc::clone(&inner);
        let inner = inner_ref;

        // check closing
        if closing.is_cancelled() {
            return registrations;
        }

        // check compactions enabled
        if !inner.compactions_enabled() {
            return registrations;
        }

        // let fs = self.retain_file_set().unwrap();
        let fs = Self::retain_file_set2(&inner.file_set);
        defer! {
            fs.release();
        }

        // Iterate over each level we are going to compact.
        // We skip the first level (0) because it is log files and they are compacted separately.
        // We skip the last level because the files have no higher level to compact into.
        let (min_level, max_level) = (1, inner.levels.len() as i64 - 2);
        for level in min_level..=max_level {
            // Skip level if it is currently compacting.
            if inner.level_compacting[level as usize] {
                continue;
            }

            // Collect contiguous files from the end of the level.
            match fs.last_contiguous_index_files_by_evel(level) {
                Some(mut files) => {
                    if files.len() < 2 {
                        continue;
                    } else if files.len() > MAX_INDEX_MERGE_COUNT as usize {
                        files.truncate(files.len() - MAX_INDEX_MERGE_COUNT as usize);

                        // Retain files during compaction.
                        let retained_files = Arc::new(RetainedIndexFiles::new(files.clone()));

                        // Mark the level as compacting.
                        inner.level_compacting[level as usize] = true;

                        // Execute in closure to save reference to the group within the loop.

                        // Start compacting in a separate goroutine.
                        inner.current_compaction_n += 1;
                        let compaction_interrupt = compaction_interrupt.clone();
                        let level_inner = Arc::clone(&inner_arc);
                        let lvl = inner.levels[level as usize].clone();
                        let closing = closing.clone();
                        let path = path.clone();
                        let sfile = Arc::clone(&sfile);

                        let next_executor = Arc::clone(&executor);
                        let cancelled_inner = Arc::clone(&level_inner);
                        let cancelled_files = Arc::clone(&retained_files);
                        let task_retained_files = Arc::clone(&retained_files);
                        let (registration, completion) = accepted_task_guard(move || {
                            Self::finish_level_compaction(
                                &cancelled_inner,
                                level,
                                &cancelled_files,
                            );
                        });
                        match executor.spawn_blocking_compact(move || {
                            #[cfg(test)]
                            if PANIC_COMPACT2_BODY.swap(false, Ordering::SeqCst) {
                                panic!("injected TSI compact2 production-body panic");
                            }

                            // Compact to a new level.
                            Partition::compact_to_level(
                                Arc::clone(&level_inner),
                                path.clone(),
                                version,
                                files,
                                Arc::clone(&task_retained_files),
                                level,
                                lvl,
                                Arc::clone(&sfile),
                                compaction_interrupt.clone(),
                            );

                            completion.complete_with(|| {
                                Self::finish_level_compaction(
                                    &level_inner,
                                    level,
                                    &task_retained_files,
                                );
                            });
                            // Check for new compactions
                            Self::compact(
                                path,
                                version,
                                sfile,
                                Arc::clone(&level_inner),
                                closing,
                                compaction_interrupt,
                                next_executor,
                            );
                        }) {
                            Ok(_) => registrations.push(registration),
                            Err(error) => {
                                registration.rejected();
                                retained_files.release();
                                inner.level_compacting[level as usize] = false;
                                inner.current_compaction_n -= 1;
                                tracing::error!(
                                    target: "reefsdb::tsdb::index",
                                    error = %error,
                                    "tsi1 level compaction task scheduling failed"
                                );
                            }
                        }
                    }
                }
                _ => {
                    continue;
                }
            }
        }
        // }
        // .boxed()
        registrations
    }

    fn compact(
        path: PathBuf,
        version: i32,
        sfile: Arc<SeriesFile>,
        inner: Arc<RwLock<Inner>>,
        closing: CancellationToken,
        compaction_interrupt: CancellationToken,
        executor: ExecutorRef,
        // ) -> BoxFuture<'static, ()> {
    ) {
        let inner_arc = Arc::clone(&inner);
        let mut inner = inner.write();
        let mut registrations = Vec::new();

        // async move {

        // check closing
        if closing.is_cancelled() {
            return;
        }

        // check compactions enabled
        if !inner.compactions_enabled() {
            return;
        }

        // let fs = self.retain_file_set().unwrap();
        let fs = Self::retain_file_set2(&inner.file_set);
        defer! {
            fs.release();
        }

        // Iterate over each level we are going to compact.
        // We skip the first level (0) because it is log files and they are compacted separately.
        // We skip the last level because the files have no higher level to compact into.
        let (min_level, max_level) = (1, inner.levels.len() as i64 - 2);
        for level in min_level..=max_level {
            // Skip level if it is currently compacting.
            if inner.level_compacting[level as usize] {
                continue;
            }

            // Collect contiguous files from the end of the level.
            match fs.last_contiguous_index_files_by_evel(level) {
                Some(mut files) => {
                    if files.len() < 2 {
                        continue;
                    } else if files.len() > MAX_INDEX_MERGE_COUNT as usize {
                        files.truncate(files.len() - MAX_INDEX_MERGE_COUNT as usize);

                        // Retain files during compaction.
                        let retained_files = Arc::new(RetainedIndexFiles::new(files.clone()));

                        // Mark the level as compacting.
                        inner.level_compacting[level as usize] = true;

                        // Execute in closure to save reference to the group within the loop.

                        // Start compacting in a separate goroutine.
                        inner.current_compaction_n += 1;
                        let compaction_interrupt = compaction_interrupt.clone();
                        let level_inner = Arc::clone(&inner_arc);
                        let lvl = inner.levels[level as usize].clone();
                        let closing = closing.clone();
                        let path = path.clone();
                        let sfile = Arc::clone(&sfile);

                        let next_executor = Arc::clone(&executor);
                        let cancelled_inner = Arc::clone(&level_inner);
                        let cancelled_files = Arc::clone(&retained_files);
                        let task_retained_files = Arc::clone(&retained_files);
                        let (registration, completion) = accepted_task_guard(move || {
                            Self::finish_level_compaction(
                                &cancelled_inner,
                                level,
                                &cancelled_files,
                            );
                        });
                        match executor.spawn_blocking_compact(move || {
                            // Compact to a new level.
                            Partition::compact_to_level(
                                Arc::clone(&level_inner),
                                path.clone(),
                                version,
                                files,
                                Arc::clone(&task_retained_files),
                                level,
                                lvl,
                                Arc::clone(&sfile),
                                compaction_interrupt.clone(),
                            );

                            completion.complete_with(|| {
                                Self::finish_level_compaction(
                                    &level_inner,
                                    level,
                                    &task_retained_files,
                                );
                            });
                            // Check for new compactions
                            Self::compact(
                                path,
                                version,
                                sfile,
                                Arc::clone(&level_inner),
                                closing,
                                compaction_interrupt,
                                next_executor,
                            );
                        }) {
                            Ok(_) => registrations.push(registration),
                            Err(error) => {
                                registration.rejected();
                                retained_files.release();
                                inner.level_compacting[level as usize] = false;
                                inner.current_compaction_n -= 1;
                                tracing::error!(
                                    target: "reefsdb::tsdb::index",
                                    error = %error,
                                    "tsi1 level compaction task scheduling failed"
                                );
                            }
                        }
                    }
                }
                _ => {
                    continue;
                }
            }
        }
        // }
        // .boxed()
        drop(inner);
        for registration in registrations {
            registration.accepted();
        }
    }

    // Compacts a set of files into a new file. Replaces old files with
    // compacted file on successful completion. This runs in a separate goroutine.
    fn compact_to_level(
        inner: Arc<RwLock<Inner>>,
        path: PathBuf,
        version: i32,
        files: VecDeque<Arc<RwLock<IndexFile>>>,
        retained_files: Arc<RetainedIndexFiles>,
        level: i64,
        lvl: CompactionLevel,
        sfile: Arc<SeriesFile>,
        interrupt: CancellationToken,
    ) {
        assert!(
            files.len() >= 2,
            "at least two index files are required for compaction"
        );
        assert!(level > 0, "cannot compact level zero");

        // Build a logger for this compaction.
        // TODO: implement me

        // Check for cancellation.
        if interrupt.is_cancelled() {
            return;
        }

        // Create new index file.
        let index_file_path = path.join(formant_index_file_name(
            Self::next_sequence2(Arc::clone(&inner)),
            level,
        ));
        let mut f = match std::fs::File::create(&index_file_path) {
            Ok(fs) => fs,
            Err(e) => {
                // log.Error("Cannot create compaction files", zap.Error(err))
                return;
            }
        };

        // log.Info("Performing full compaction",
        // zap.String("src", joinIntSlice(IndexFiles(files).IDs(), ",")),
        // zap.String("dst", path),
        // )

        // Compact all index files to new index file.
        let (res, _n) = IndexFiles(files.clone()).compact_to(
            &mut f,
            Arc::clone(&sfile),
            lvl.m,
            lvl.k,
            interrupt,
        );
        if let Err(e) = res {
            // log.Error("Cannot compact index files", zap.Error(err))
            return;
        }

        // Close file.
        drop(f);

        // Reopen as an index file.
        let mut file = IndexFile::new(sfile);
        file.set_path(&index_file_path);
        if let Err(e) = file.open() {
            // log.Error("Cannot open new index file", zap.Error(err))
            return;
        }

        // Obtain lock to swap in index file and write manifest.
        let res = (|| {
            let mut inner = inner.write();

            // Replace previous files with new index file.
            let file_set = Arc::clone(&inner.file_set);
            inner.file_set = Arc::new(file_set.must_replace(
                IndexFiles(files.clone()).files(),
                File::IndexFile(Arc::new(RwLock::new(file))),
            ));
            // Write new manifest.
            match inner.manifest(&path, version).write() {
                Ok(size) => {
                    inner.manifest_size = size;
                }
                Err(e) => {
                    // TODO: Close index if write fails.
                    return Err(e);
                }
            };
            Ok(())
        })();

        if let Err(e) = res {
            // log.Error("Cannot write manifest", zap.Error(err))
            return;
        }

        // log.Info("Full compaction complete",
        //     zap.String("path", path),
        //     logger.DurationLiteral("elapsed", elapsed),
        //     zap.Int64("bytes", n),
        //     zap.Int("kb_per_sec", int(float64(n)/elapsed.Seconds())/1024),
        // )

        // Release old files.
        retained_files.release();

        // Close and delete all old index files.
        for f in files {
            // log.Info("Removing index file", zap.String("path", f.Path()))
            let mut f = f.write();
            if let Err(e) = f.close() {
                // log.Error("Cannot close index file", zap.Error(err))
                return;
            }
            if let Err(e) = std::fs::remove_file(f.path()) {
                // log.Error("Cannot remove index file", zap.Error(err))
                return;
            }
        }
    }

    pub fn check_log_file(&self) -> Result<()> {
        // Check log file size under read lock.
        let size = {
            self.inner
                .read()
                .active_log_file
                .as_ref()
                .unwrap()
                .read()
                .size()
        };
        if size < self.max_log_file_size {
            return Ok(());
        }

        // If file size exceeded then recheck under write lock and swap files.
        let mut inner_guard = self.inner.write(); // TODO: WRITE

        if inner_guard.active_log_file.as_ref().unwrap().read().size() < self.max_log_file_size {
            return Ok(());
        }

        // Swap current log file.
        let log_file = Arc::clone(inner_guard.active_log_file.as_ref().unwrap());

        // Open new log file and insert it into the first position.
        self.prepend_active_log_file(&mut inner_guard)?;

        // Begin compacting in a background task.
        inner_guard.current_compaction_n += 1;

        let inner = Arc::clone(&self.inner);
        let path = self.path.clone();
        let lvl = inner_guard.levels[1].clone();
        let closing = self.closing.clone();
        let p = self.path.clone();
        let sfile = Arc::clone(&self.sfile);
        let interrupt = self.compaction_interrupt.clone();
        let version = self.version;
        let next_executor = Arc::clone(&self.executor);
        let cancelled_inner = Arc::clone(&inner);
        let task_inner = Arc::clone(&inner);
        let (registration, completion) = accepted_task_guard(move || {
            Self::finish_log_compaction(&cancelled_inner);
        });
        match self.executor.spawn_blocking_compact(move || {
            #[cfg(test)]
            if PANIC_CHECK_LOG_FILE_BODY.swap(false, Ordering::SeqCst) {
                panic!("injected TSI check_log_file production-body panic");
            }

            let _ = Self::compact_log_file(
                closing.clone(),
                Arc::clone(&inner),
                p,
                version,
                lvl,
                Arc::clone(&sfile),
                interrupt.clone(),
                log_file,
            );

            completion.complete_with(|| Self::finish_log_compaction(&task_inner));

            Self::compact(
                path,
                version,
                sfile,
                inner,
                closing,
                interrupt,
                next_executor,
            );
        }) {
            Ok(_) => {
                drop(inner_guard);
                registration.accepted();
            }
            Err(source) => {
                registration.rejected();
                inner_guard.current_compaction_n -= 1;
                tracing::error!(
                    target: "reefsdb::tsdb::index",
                    error = %source,
                    "tsi1 log compaction task scheduling failed"
                );
            }
        }

        Ok(())
    }

    // Compacts tsl file into a tsi file. The new file will share the
    // same identifier but will have a ".tsi" extension. Once the log file is
    // compacted then the manifest is updated and the log file is discarded.
    fn compact_log_file(
        closing: CancellationToken,
        inner: Arc<RwLock<Inner>>,
        path: PathBuf,
        version: i32,
        lvl: CompactionLevel,
        sfile: Arc<SeriesFile>,
        interrupt: CancellationToken,
        log_file: Arc<RwLock<LogFile>>,
    ) -> Result<()> {
        if closing.is_cancelled() {
            return Ok(());
        }

        let compaction_total_start = slow_trace::now();

        let (id, log_file_path) = {
            let lock_start = slow_trace::now();
            let log_file_guard = log_file.read();
            slow_trace::log_lock_wait(
                "log_file.read",
                lock_start,
                "stage=query_wait_for_index_lock component=index_log_compaction_prepare",
            );

            // Retrieve identifier from current path. Older LogFile::open paths
            // may leave id as zero because they parsed the full path; use the
            // basename here so log compaction does not panic on valid files.
            let mut id = log_file_guard.id(); // FIXME: batch_index_create_series_list_if_not_exists/batch_size_10000/partition_16
            if id == 0 {
                let (_, parsed_id) =
                    parse_file_name(log_file_guard.path().file_name().unwrap().to_str().unwrap());
                id = parsed_id;
            }
            assert!(
                id != 0,
                "cannot parse log file id: {:?}",
                log_file_guard.path()
            );
            (id, log_file_guard.path().to_path_buf())
        };

        // Build a logger for this compaction.
        // log, logEnd := logger.NewOperation(context.TODO(), p.logger, "TSI log compaction", "tsi1_compact_log_file", zap.Int("tsi1_log_file_id", id))
        // defer logEnd()

        // Create new index file.
        let index_file_path = path.join(formant_index_file_name(id, 1));
        let mut f = match std::fs::File::create(&index_file_path) {
            Ok(fs) => fs,
            Err(e) => {
                // log.Error("Cannot create index file", zap.Error(err))
                let _ = e;
                return Ok(());
            }
        };

        // Compact log file to new index file.

        let compact_to_start = slow_trace::now();
        let _n = {
            let lock_start = slow_trace::now();
            let log_file_guard = log_file.read();
            slow_trace::log_lock_wait(
                "log_file.read",
                lock_start,
                "stage=query_wait_for_index_lock component=index_log_compaction_compact_to",
            );

            match log_file_guard.compact_to(&mut f, lvl.m, lvl.k, interrupt) {
                Ok(n) => n,
                Err(e) => {
                    // log.Error("Cannot compact log file", zap.Error(err), zap.String("path", logFile.Path()))
                    let _ = e;
                    return Ok(());
                }
            }
        };
        slow_trace::log_background_stage(
            "index_log_compaction",
            compact_to_start,
            "component=compact_to",
        );

        // Close file.
        drop(f);

        // Reopen as an index file.
        let open_index_file_start = slow_trace::now();
        let mut file = IndexFile::new(sfile);
        file.set_path(&index_file_path);
        if let Err(e) = file.open() {
            // log.Error("Cannot open compacted index file", zap.Error(err), zap.String("path", file.Path()))
            let _ = e;
            return Ok(());
        }
        slow_trace::log_background_stage(
            "index_log_compaction",
            open_index_file_start,
            "component=open_index_file",
        );

        // Obtain lock to swap in index file and build the manifest snapshot.
        let install_start = slow_trace::now();
        let (old_file_set, published_file_set, manifest) = {
            let lock_start = slow_trace::now();
            let mut inner = inner.write();
            slow_trace::log_lock_wait(
                "index_partition.write",
                lock_start,
                "stage=query_wait_for_index_lock component=index_log_compaction_install",
            );
            // Publish a complete immutable FileSet, then write the manifest outside
            // this lock. If the durable write fails, the old FileSet is restored
            // before the old log file can be closed or removed.
            let file_set = Arc::clone(&inner.file_set);
            let new_file_set = Arc::new(file_set.must_replace(
                VecDeque::from([File::LogFile(Arc::clone(&log_file))]),
                File::IndexFile(Arc::new(RwLock::new(file))),
            ));
            inner.file_set = Arc::clone(&new_file_set);
            let manifest = inner.manifest(&path, version);
            (file_set, new_file_set, manifest)
        };
        slow_trace::log_background_stage(
            "index_log_compaction",
            install_start,
            "component=install_file_set",
        );

        // Write new manifest outside the partition lock.
        let manifest_write_start = slow_trace::now();
        let mut manifest = manifest;
        let rollback_published_file_set = || {
            let mut inner = inner.write();
            if Arc::ptr_eq(&inner.file_set, &published_file_set) {
                inner.file_set = Arc::clone(&old_file_set);
            }
            let _ = std::fs::remove_file(&index_file_path);
        };
        #[cfg(test)]
        if FAIL_COMPACT_LOG_FILE_MANIFEST_WRITE.swap(false, Ordering::SeqCst) {
            rollback_published_file_set();
            return Err(Error::StringErr {
                reason: "injected compact log file manifest write failure".to_string(),
            });
        }
        let manifest_size = match manifest.write() {
            Ok(size) => size,
            Err(e) => {
                rollback_published_file_set();
                return Err(e);
            }
        };
        slow_trace::log_background_stage(
            "index_log_compaction",
            manifest_write_start,
            "component=manifest_write",
        );

        let manifest_size_update_start = slow_trace::now();
        {
            let lock_start = slow_trace::now();
            let mut inner = inner.write();
            slow_trace::log_lock_wait(
                "index_partition.write",
                lock_start,
                "stage=query_wait_for_index_lock component=index_log_compaction_manifest_size",
            );
            inner.manifest_size = manifest_size;
        }
        slow_trace::log_background_stage(
            "index_log_compaction",
            manifest_size_update_start,
            "component=manifest_size_update",
        );

        // log.Info("Log file compacted",
        //     logger.DurationLiteral("elapsed", elapsed),
        //     zap.Int64("bytes", n),
        //     zap.Int("kb_per_sec", int(float64(n)/elapsed.Seconds())/1024),
        // )

        // Closing the log file will automatically wait until the ref count is zero.
        let close_wait_start = slow_trace::now();
        {
            let lock_start = slow_trace::now();
            let log_file_guard = log_file.read();
            slow_trace::log_lock_wait(
                "log_file.read",
                lock_start,
                "stage=query_wait_for_index_lock component=index_log_compaction_close_wait",
            );
            log_file_guard.wait_for_references();
        }
        slow_trace::log_background_stage(
            "index_log_compaction",
            close_wait_start,
            "component=close_wait",
        );

        let close_start = slow_trace::now();
        {
            let lock_start = slow_trace::now();
            let mut log_file_guard = log_file.write();
            slow_trace::log_lock_wait(
                "log_file.write",
                lock_start,
                "stage=query_wait_for_index_lock component=index_log_compaction_close",
            );
            if let Err(e) = log_file_guard.close() {
                // log.Error("Cannot close log file", zap.Error(err))
                let _ = e;
                return Ok(());
            }
        }

        if let Err(e) = std::fs::remove_file(&log_file_path) {
            // log.Error("Cannot remove log file", zap.Error(err))
            let _ = e;
        }
        slow_trace::log_background_stage("index_log_compaction", close_start, "component=close");
        slow_trace::log_background_stage("index_log_compaction", compaction_total_start, "");
        Ok(())
    }
}

/// ParseFilename extracts the numeric id from a log or index file path.
/// Returns 0 if it cannot be parsed.
pub fn parse_file_name(name: &str) -> (i64, i64) {
    // (level, id)
    match Regex::new(r"^L(\d+)-(\d+)\..+$").unwrap().captures(name) {
        Some(cap) => {
            let level = atoi_simd::parse::<i64>(cap[1].as_bytes()).unwrap();
            let id = atoi_simd::parse::<i64>(cap[2].as_bytes()).unwrap();
            (level, id)
        }
        _ => (0, 0),
    }
}

// Manifest represents the list of log & index files that make up the index.
// The files are listed in time order, not necessarily ID order.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Manifest {
    pub levels: Vec<CompactionLevel>,
    files: Vec<String>,

    // version should be updated whenever the TSI format has changed.
    pub version: i32,

    pub path: PathBuf, // location on disk of the manifest.
}

impl Manifest {
    /// Returns a new instance of Manifest with default compaction levels.
    pub fn new() -> Self {
        let mut m = Self {
            version: VERSION,
            ..Default::default()
        };

        for l in DEFAULT_COMPACTION_LEVELS {
            m.levels.push(l.clone());
        }

        m
    }

    /// Returns true if name is listed in the log files or index files.
    pub fn has_file(&self, name: &[u8]) -> bool {
        for file_name in &self.files {
            if file_name.as_bytes() == name {
                return true;
            }
        }
        false
    }

    /// Checks if the Manifest's version is compatible with this version of the tsi1 index.
    pub fn valid(&self) -> Result<()> {
        // If we don't have an explicit version in the manifest file then we know
        // it's not compatible with the latest tsi1 Index.
        ensure!(self.version == VERSION, ErrIncompatibleVersionSnafu);
        Ok(())
    }

    /// Writes the manifest file to the provided path, returning the number of
    /// bytes written and an error, if any.
    pub fn write(&mut self) -> Result<i64> {
        let mut buf = serde_json::to_vec_pretty(self).map_err(|e| Error::StringErr {
            reason: format!("Serialize manifest failed: {}", e),
        })?;

        buf.push(b'\n');

        std::fs::write(&self.path, &buf)?;
        std::fs::metadata(&self.path)?.permissions().set_mode(0o666);

        Ok(buf.len() as i64)
    }
}

// ReadManifestFile reads a manifest from a file path and returns the Manifest,
// the size of the manifest on disk, and any error if appropriate.
fn read_manifest_file(path: &Path) -> Result<(Manifest, i64)> {
    let buf = std::fs::read_to_string(path)?;

    // Decode manifest.
    if let Ok(mut m) = serde_json::from_slice::<Manifest>(buf.as_bytes()) {
        // Set the path of the manifest.
        m.path = path.to_path_buf();
        return Ok((m, buf.len() as i64));
    }

    Err(Error::StringErr {
        reason: "Deserialize data to manifest failed".to_string(),
    })
}

// CompactionLevel represents a grouping of index files based on bloom filter
// settings. By having the same bloom filter settings, the filters
// can be merged and evaluated at a higher level.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CompactionLevel {
    // Bloom filter bit size & hash count
    m: u64,
    k: u64,
}

struct RetainedIndexFiles {
    files: VecDeque<Arc<RwLock<IndexFile>>>,
    released: std::sync::Once,
}

impl RetainedIndexFiles {
    fn new(files: VecDeque<Arc<RwLock<IndexFile>>>) -> Self {
        IndexFiles(files.clone()).retain();
        Self {
            files,
            released: std::sync::Once::new(),
        }
    }

    fn release(&self) {
        self.released
            .call_once(|| IndexFiles(self.files.clone()).release());
    }
}

// DefaultCompactionLevels is the default settings used by the index.
const DEFAULT_COMPACTION_LEVELS: &[CompactionLevel; 8] = &[
    CompactionLevel { m: 0, k: 0 },       // L0: Log files, no filter.
    CompactionLevel { m: 1 << 25, k: 6 }, // L1: Initial compaction
    CompactionLevel { m: 1 << 25, k: 6 }, // L2
    CompactionLevel { m: 1 << 26, k: 6 }, // L3
    CompactionLevel { m: 1 << 27, k: 6 }, // L4
    CompactionLevel { m: 1 << 28, k: 6 }, // L5
    CompactionLevel { m: 1 << 29, k: 6 }, // L6
    CompactionLevel { m: 1 << 30, k: 6 }, // L7
];

// MaxIndexMergeCount is the maximum number of files that can be merged together at once.
pub const MAX_INDEX_MERGE_COUNT: i32 = 2;

// MaxIndexFileSize is the maximum expected size of an index file.
pub const MAX_INDEX_FILE_SIZE: i64 = 4 * (1 << 30);

#[cfg(test)]
mod tests {
    use core::panic;
    use std::{
        collections::HashMap,
        ops::{Deref, DerefMut},
        os::unix::fs::PermissionsExt,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, Instant},
    };

    use crate::runtime::execution::{
        BoxBlockingTask, BoxFutureTask, ExecutorRef, RuntimeExecutor, SpawnError, TaskController,
    };
    use parking_lot::RwLock;
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;

    use super::{
        File, FileSet, INDEX_FILE_EXT, MANIFEST_FILE_NAME, Manifest, formant_index_file_name,
    };
    use crate::{
        index::tsi1::{index_file::IndexFile, partition::Result, tests::must_temp_tsi_dir},
        series::series_file::{SeriesFile, tests::must_open_series_file},
    };

    static TEST_FAILURE_HOOK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct TestFailureHookGuard {
        _guard: std::sync::MutexGuard<'static, ()>,
    }

    fn reset_test_failure_hooks() {
        super::FAIL_COMPACT_LOG_FILE_MANIFEST_WRITE.store(false, Ordering::SeqCst);
        super::PANIC_COMPACT2_BODY.store(false, Ordering::SeqCst);
        super::PANIC_CHECK_LOG_FILE_BODY.store(false, Ordering::SeqCst);
    }

    fn test_failure_hook_guard() -> TestFailureHookGuard {
        let guard = TEST_FAILURE_HOOK_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        reset_test_failure_hooks();
        TestFailureHookGuard { _guard: guard }
    }

    #[derive(Clone, Copy)]
    enum CompactTaskDisposition {
        Run = 0,
        Refuse = 1,
        Drop = 2,
        Panic = 3,
    }

    struct CompactLifecycleExecutor {
        disposition: AtomicUsize,
        submissions: AtomicUsize,
    }

    impl CompactLifecycleExecutor {
        fn new(disposition: CompactTaskDisposition) -> Self {
            Self {
                disposition: AtomicUsize::new(disposition as usize),
                submissions: AtomicUsize::new(0),
            }
        }

        fn set(&self, disposition: CompactTaskDisposition) {
            self.disposition
                .store(disposition as usize, Ordering::SeqCst);
        }
    }

    impl RuntimeExecutor for CompactLifecycleExecutor {
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
            self.submissions.fetch_add(1, Ordering::SeqCst);
            match self
                .disposition
                .swap(CompactTaskDisposition::Run as usize, Ordering::SeqCst)
            {
                value if value == CompactTaskDisposition::Refuse as usize => {
                    Err(SpawnError::rejected("injected TSI compaction refusal"))
                }
                value if value == CompactTaskDisposition::Drop as usize => {
                    drop(task);
                    Ok(TaskController::detached())
                }
                value if value == CompactTaskDisposition::Panic as usize => {
                    std::thread::spawn(move || {
                        let _task = task;
                        panic!("injected accepted TSI compaction panic");
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

    fn wait_for_compactions(partition: &crate::index::tsi1::partition::Partition) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while partition.current_compaction_n() != 0 {
            assert!(
                Instant::now() < deadline,
                "TSI compaction accounting leaked"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    fn fake_index_files(
        root: &std::path::Path,
        sfile: &Arc<SeriesFile>,
    ) -> std::collections::VecDeque<Arc<RwLock<IndexFile>>> {
        (1..=3)
            .map(|id| {
                let mut file = IndexFile::new(Arc::clone(sfile));
                file.set_path(&root.join(formant_index_file_name(id, 1)));
                file.set_level_for_test(1);
                Arc::new(RwLock::new(file))
            })
            .collect()
    }

    fn install_index_files_for_compact2(
        partition: &crate::index::tsi1::partition::Partition,
        files: &std::collections::VecDeque<Arc<RwLock<IndexFile>>>,
    ) {
        let mut inner = partition.inner.write();
        let mut all_files = inner.file_set.files.clone();
        for file in files {
            all_files.push_back(File::IndexFile(Arc::clone(file)));
        }
        inner.file_set = Arc::new(FileSet::new(
            inner.levels.clone(),
            partition.series_file(),
            all_files,
        ));
    }

    fn invoke_production_compact2(partition: &crate::index::tsi1::partition::Partition) {
        let inner_arc = Arc::clone(&partition.inner);
        let mut inner = partition.inner.write();
        let registrations = crate::index::tsi1::partition::Partition::compact2(
            partition.path().to_path_buf(),
            partition.version,
            partition.series_file(),
            inner_arc,
            &mut inner,
            partition.closing.clone(),
            partition.compaction_interrupt.clone(),
            Arc::clone(&partition.executor),
        );
        drop(inner);
        for registration in registrations {
            registration.accepted();
        }
    }

    fn assert_files_released(files: std::collections::VecDeque<Arc<RwLock<IndexFile>>>) {
        let (sender, receiver) = crossbeam_channel::bounded(1);
        std::thread::spawn(move || {
            for file in files {
                file.write().close().unwrap();
            }
            let _ = sender.send(());
        });
        receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("TSI compaction retained index files after termination");
    }

    impl Drop for TestFailureHookGuard {
        fn drop(&mut self) {
            reset_test_failure_hooks();
        }
    }

    #[test]
    fn test_partition_open() {
        let sfile = must_open_series_file();
        let sfile = Arc::new(sfile.0);

        let mut p = Partition::new(Arc::clone(&sfile));
        // 1. Opening a fresh index should set the MANIFEST version to current version.
        p.open().unwrap();
        {
            // Check version set appropriately.
            let mut inner = p.inner.write();
            let m = p.manifest(&mut inner);
            assert_eq!(
                m.version, 1,
                "got index version {}, expected {}",
                m.version, 1
            );
        }

        // 2. Reopening an open index should return an error.
        if let Ok(_) = p.open() {
            p.close().unwrap();
            panic!("didn't get an error on reopen, but expected one");
        }
        p.close().unwrap();

        // 3. Opening an incompatible index should return an error.
        let incompatible_versions = vec![-1, 0, 2];
        for v in incompatible_versions {
            let mut p = Partition::new(Arc::clone(&sfile));
            // Manually create a MANIFEST file for an incompatible index version.
            let mpath = p.path().join(MANIFEST_FILE_NAME);
            let mut m = Manifest::new();
            m.path = mpath.clone();
            m.levels = Vec::new();
            m.version = v; // Set example MANIFEST version.
            m.write().unwrap();

            // Log the MANIFEST file.
            let data = std::fs::read(&m.path).unwrap();
            eprintln!("Incompatible MANIFEST: {:?}", data);

            // Opening this index should return an error because the MANIFEST has an
            // incompatible version.
            if let Err(e) = p.open() {
                assert!(
                    matches!(e, super::Error::ErrIncompatibleVersion),
                    "got error {}, expected {}",
                    e,
                    super::Error::ErrIncompatibleVersion
                );
            }
        }
    }

    #[test]
    fn test_partition_manifest() {
        let sfile = must_open_series_file();

        let p = must_open_partitions(Arc::new(sfile.into()));
        let mut inner = p.inner.write();
        let m = p.manifest(&mut inner);
        assert_eq!(
            m.version,
            crate::index::tsi1::partition::VERSION,
            "got MANIFEST version {}, expected {}",
            m.version,
            crate::index::tsi1::partition::VERSION
        );
    }

    #[test]
    fn test_compact_log_file_manifest_failure_rolls_back_file_set() {
        let _hook_guard = test_failure_hook_guard();

        let sfile = must_open_series_file();
        let mut p = must_open_partitions(Arc::new(sfile.into()));
        let tags = crate::models::points::Tags::from(HashMap::from([(
            "host".as_bytes().to_vec(),
            "a".as_bytes().to_vec(),
        )]));

        p.create_series_list_if_not_exists(&[b"cpu".as_slice()], &[&tags])
            .unwrap();
        assert!(p.measurement_exists(b"cpu").unwrap());

        let old_log_file = {
            let mut inner = p.0.inner.write();
            let old_log_file = Arc::clone(inner.active_log_file.as_ref().unwrap());
            p.prepend_active_log_file(&mut inner).unwrap();
            old_log_file
        };
        let old_log_path = old_log_file.read().path().to_path_buf();
        let (_, old_log_id) =
            super::parse_file_name(old_log_path.file_name().unwrap().to_str().unwrap());
        let failed_index_path =
            p.path()
                .join(crate::index::tsi1::index_file::formant_index_file_name(
                    old_log_id, 1,
                ));
        let old_file_set = Arc::clone(&p.0.inner.read().file_set);
        let old_file_paths = old_file_set
            .files
            .iter()
            .map(|file| file.path())
            .collect::<Vec<_>>();
        let (manifest_before, _) = super::read_manifest_file(&p.path().join(MANIFEST_FILE_NAME))
            .expect("manifest should exist before compaction");

        super::FAIL_COMPACT_LOG_FILE_MANIFEST_WRITE.store(true, Ordering::SeqCst);
        let level = p.0.inner.read().levels[1].clone();
        let result = crate::index::tsi1::partition::Partition::compact_log_file(
            CancellationToken::new(),
            Arc::clone(&p.0.inner),
            p.path().to_path_buf(),
            super::VERSION,
            level,
            p.series_file(),
            CancellationToken::new(),
            old_log_file,
        );
        super::FAIL_COMPACT_LOG_FILE_MANIFEST_WRITE.store(false, Ordering::SeqCst);

        assert!(result.is_err());
        assert!(!failed_index_path.exists());
        let restored_file_set = Arc::clone(&p.0.inner.read().file_set);
        assert!(Arc::ptr_eq(&restored_file_set, &old_file_set));
        assert_eq!(
            restored_file_set
                .files
                .iter()
                .map(|file| file.path())
                .collect::<Vec<_>>(),
            old_file_paths
        );
        assert!(old_log_path.exists());
        assert!(p.measurement_exists(b"cpu").unwrap());

        let (manifest_after_failure, _) =
            super::read_manifest_file(&p.path().join(MANIFEST_FILE_NAME))
                .expect("manifest should remain readable after failed compaction");
        assert_eq!(manifest_after_failure.files, manifest_before.files);

        let retry_log_file = Arc::clone(
            restored_file_set
                .files
                .iter()
                .find_map(|file| match file {
                    super::File::LogFile(log_file) if log_file.read().path() == old_log_path => {
                        Some(log_file)
                    }
                    _ => None,
                })
                .expect("old log file should still be in the restored file set"),
        );
        drop(restored_file_set);
        drop(old_file_set);

        let level = p.0.inner.read().levels[1].clone();
        let result = crate::index::tsi1::partition::Partition::compact_log_file(
            CancellationToken::new(),
            Arc::clone(&p.0.inner),
            p.path().to_path_buf(),
            super::VERSION,
            level,
            p.series_file(),
            CancellationToken::new(),
            retry_log_file,
        );
        assert!(result.is_ok());

        let (manifest_after_retry, _) =
            super::read_manifest_file(&p.path().join(MANIFEST_FILE_NAME))
                .expect("manifest should remain readable after retry");
        assert!(
            manifest_after_retry
                .files
                .iter()
                .any(|file| file.ends_with(super::INDEX_FILE_EXT))
        );

        p.close().unwrap();
    }

    fn assert_compact2_terminal_cleanup(
        disposition: CompactTaskDisposition,
        panic_production_body: bool,
    ) {
        let _failure_hooks = test_failure_hook_guard();
        let sfile = Arc::new(must_open_series_file().0);
        let executor = Arc::new(CompactLifecycleExecutor::new(disposition));
        let executor_ref: ExecutorRef = executor.clone();
        let mut partition = Partition::new_with_executor(Arc::clone(&sfile), executor_ref);
        partition.open().unwrap();

        let files = fake_index_files(&partition.path(), &sfile);
        install_index_files_for_compact2(&partition, &files);
        super::PANIC_COMPACT2_BODY.store(panic_production_body, Ordering::SeqCst);
        invoke_production_compact2(&partition);
        wait_for_compactions(&partition);
        {
            let inner = partition.inner.read();
            assert!(!inner.level_compacting[1]);
            assert_eq!(inner.current_compaction_n, 0);
        }
        assert_files_released(files);

        executor.set(CompactTaskDisposition::Drop);
        invoke_production_compact2(&partition);
        wait_for_compactions(&partition);
        assert!(!partition.inner.read().level_compacting[1]);
        assert!(executor.submissions.load(Ordering::SeqCst) >= 2);

        partition.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_compact2_accepted_disposal_restores_files_flags_and_counter() {
        assert_compact2_terminal_cleanup(CompactTaskDisposition::Drop, false);
    }

    #[test]
    fn test_compact2_executor_unwind_before_body_restores_files_flags_and_counter() {
        assert_compact2_terminal_cleanup(CompactTaskDisposition::Panic, false);
    }

    #[test]
    fn test_compact2_production_body_panic_restores_files_flags_and_counter() {
        assert_compact2_terminal_cleanup(CompactTaskDisposition::Run, true);
    }

    fn assert_check_log_file_terminal_cleanup(disposition: CompactTaskDisposition) {
        let _failure_hooks = test_failure_hook_guard();
        let sfile = Arc::new(must_open_series_file().0);
        let executor = Arc::new(CompactLifecycleExecutor::new(CompactTaskDisposition::Run));
        let executor_ref: ExecutorRef = executor.clone();
        let mut partition = Partition::new_with_executor(Arc::clone(&sfile), executor_ref);
        partition.max_log_file_size = 0;
        partition.open().unwrap();

        executor.set(disposition);
        partition.check_log_file().unwrap();
        wait_for_compactions(&partition);
        assert_eq!(partition.current_compaction_n(), 0);

        executor.set(CompactTaskDisposition::Run);
        partition.check_log_file().unwrap();
        wait_for_compactions(&partition);
        assert_eq!(partition.current_compaction_n(), 0);

        partition.close().unwrap();
        sfile.close().unwrap();
    }

    #[test]
    fn test_check_log_file_accepted_disposal_restores_counter_and_retries() {
        assert_check_log_file_terminal_cleanup(CompactTaskDisposition::Drop);
    }

    #[test]
    fn test_check_log_file_executor_unwind_before_body_restores_counter_and_retries() {
        assert_check_log_file_terminal_cleanup(CompactTaskDisposition::Panic);
    }

    #[test]
    fn test_check_log_file_production_body_panic_restores_counter_and_retries() {
        let _failure_hooks = test_failure_hook_guard();
        let sfile = Arc::new(must_open_series_file().0);
        let executor = Arc::new(CompactLifecycleExecutor::new(CompactTaskDisposition::Run));
        let executor_ref: ExecutorRef = executor.clone();
        let mut partition = Partition::new_with_executor(Arc::clone(&sfile), executor_ref);
        partition.max_log_file_size = 0;
        partition.open().unwrap();

        super::PANIC_CHECK_LOG_FILE_BODY.store(true, Ordering::SeqCst);
        partition.check_log_file().unwrap();
        wait_for_compactions(&partition);
        assert_eq!(partition.current_compaction_n(), 0);

        partition.check_log_file().unwrap();
        wait_for_compactions(&partition);
        assert_eq!(partition.current_compaction_n(), 0);

        partition.close().unwrap();
        sfile.close().unwrap();
    }

    // //////////// partition test util functions //////////////
    struct Partition(crate::index::tsi1::partition::Partition);

    impl Partition {
        fn new(sfile: Arc<SeriesFile>) -> Self {
            let path = must_temp_partition_dir();
            Partition(crate::index::tsi1::partition::Partition::new(sfile, path))
        }

        fn new_with_executor(sfile: Arc<SeriesFile>, executor: ExecutorRef) -> Self {
            let path = must_temp_partition_dir();
            Partition(crate::index::tsi1::partition::Partition::new_with_executor(
                sfile, path, executor,
            ))
        }

        // Closes and removes the index directory.
        fn close(&mut self) -> Result<()> {
            let res = self.0.close();
            std::fs::remove_dir_all(self.path()).unwrap();
            res
        }

        // Closes and opens the index.
        fn reopen(&mut self) -> Result<()> {
            self.0.close()?;

            let sfile = self.series_file();
            let path = self.path();
            self.0 = crate::index::tsi1::partition::Partition::new(sfile, path);
            self.open()
        }
    }

    impl Deref for Partition {
        type Target = crate::index::tsi1::partition::Partition;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl DerefMut for Partition {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }

    // Returns a new, open index. Panic on error.
    fn must_open_partitions(sfile: Arc<SeriesFile>) -> Partition {
        let mut p = Partition::new(sfile);
        p.open().unwrap();
        p
    }

    fn must_temp_partition_dir() -> PathBuf {
        let path = {
            let path = must_temp_tsi_dir();
            path.join("0")
        };

        std::fs::create_dir_all(&path).expect("Failed to create temp partition dir");
        std::fs::metadata(&path)
            .unwrap()
            .permissions()
            .set_mode(0o777);
        path
    }
}
