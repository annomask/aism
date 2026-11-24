use std::{
    collections::{BinaryHeap, HashMap, VecDeque},
    fs::{self},
    ops::{Deref, DerefMut},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
    time::Duration,
    vec,
};

use crate::common::{time::Time, to_string};
use crate::models::{points::Value, statistic::Statistic};
use crate::runtime::execution::{
    ExecutorRef, RuntimeExecutorExt, SpawnError, accepted_task_guard, legacy_global_executor,
};
use parking_lot::{Mutex, RwLock, lock_api::RawMutex};
use scopeguard::guard;
use snafu::{ResultExt, Snafu, ensure};

use super::{
    compact::TSM_FILE_EXTENSION,
    key_cursor::{KeyCursor, Location, VLogLocation, locations},
    reader::{BatchDelete, BatchDeletes, TSMReader, TimeRange},
    writer::IndexEntry,
};
use crate::{
    common::slow_trace,
    query::query_iterator,
    storage::{
        limiter::fixed::Fixed,
        tsm1::{data_encoding::encoding, vlog::vlog_index_ring::VLogIndexes},
    },
};
/// The extension used to describe temporary snapshot files.
pub const TMP_TSM_FILE_EXTENSION: &str = "tmp";
/// The extension used to describe corrupt snapshot files.
pub const BAD_TSM_FILE_EXTENSION: &str = "bad";

// Statistics gathered by the FileStore.
const STAT_FILE_STORE_BYTES: &str = "disk_bytes";
const STAT_FILE_STORE_COUNT: &str = "num_files";

/// Error type for tsm file store.
#[derive(Debug, Snafu)]
pub enum Error {
    /// IO error.
    #[snafu(display("tsm writer error: {reason}"))]
    IO { reason: std::io::Error },

    /// TSMReader error.
    #[snafu(display("{}", source))]
    Reader {
        source: crate::storage::tsm1::reader::Error,
    },

    /// Encoding error.
    #[snafu(display("{}", source))]
    Encoding {
        source: crate::storage::tsm1::data_encoding::encoding::Error,
    },

    /// Tombstone error.
    #[snafu(display("{}", source))]
    Tombstone {
        source: crate::storage::tsm1::tombstone::Error,
    },

    /// String error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },

    /// Required file-store work could not be scheduled.
    #[snafu(display("{}", source))]
    Execution { source: SpawnError },

    #[snafu(display("accepted file-store task did not complete"))]
    TaskTerminated,
}

/// Result type for tsm reader.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::IO { reason: err }
    }
}

/// FileStore is an abstraction around multiple TSM files.
pub struct FileStore {
    #[cfg(feature = "mock_file_store")]
    pub paths_fn: Option<fn() -> Vec<FileStat>>,
    #[cfg(feature = "mock_file_store")]
    pub block_count: i32,
    #[cfg(feature = "mock_file_store")]
    pub readers: Mutex<Vec<Arc<TSMReader>>>,

    stats: FileStoreStatistics,
    inner: RwLock<Inner>,
    executor: ExecutorRef,
}

struct Inner {
    last_modified: Time,
    // Most recently known file stats. If nil then stats will need to be recalculated
    last_file_stats: Vec<FileStat>,

    current_generation: i64,
    dir: Arc<PathBuf>,

    files: Vec<Arc<TSMReader>>,

    // tsm_mmap_will_need: bool,  // If true then the kernel will be advised MMAP_WILLNEED for TSM files.
    open_limiter: Fixed, // limit the number of concurrent opening TSM files.

    // stats: FileStoreStatistics,
    purger: Purger,

    current_temp_dir_id: i64,
}

impl FileStore {
    /// Returns a new instance of FileStore based on the given directory.
    pub fn new(dir: Arc<PathBuf>, open_limiter: Option<Fixed>) -> Self {
        Self::new_with_executor(dir, open_limiter, legacy_global_executor())
    }

    pub fn new_with_executor(
        dir: Arc<PathBuf>,
        open_limiter: Option<Fixed>,
        executor: ExecutorRef,
    ) -> Self {
        let inner = Inner {
            last_modified: Time::default(),
            last_file_stats: vec![],
            current_generation: 0,
            dir,
            files: vec![],
            // open_limiter: Fixed::new(num_cpus::get() as i32),
            open_limiter: match open_limiter {
                Some(limiter) => limiter,
                None => Fixed::new(num_cpus::get() as i32),
            },

            purger: Purger::new(Arc::clone(&executor)),
            current_temp_dir_id: 0,
        };

        Self {
            #[cfg(feature = "mock_file_store")]
            paths_fn: None,
            #[cfg(feature = "mock_file_store")]
            block_count: 0,
            #[cfg(feature = "mock_file_store")]
            readers: Mutex::new(vec![]), // use mutex to avoid &mut self

            stats: FileStoreStatistics::default(),
            inner: RwLock::new(inner),
            executor,
        }
    }

    /// Returns statistics for periodic monitoring.
    pub fn statistics(&self, tags: HashMap<String, String>) -> Statistic {
        Statistic {
            name: "tsm1_filestore".to_string(),
            tags,
            values: HashMap::from([
                (
                    STAT_FILE_STORE_BYTES.to_string(),
                    self.stats.disk_bytes.load(Ordering::SeqCst),
                ),
                (
                    STAT_FILE_STORE_COUNT.to_string(),
                    self.stats.file_count.load(Ordering::SeqCst),
                ),
            ]),
        }
    }

    /// Returns the number of TSM files currently loaded.
    pub fn count(&self) -> usize {
        let inner = self.inner.read();
        inner.files.len()
    }

    /// Clones the current reader list under the metadata lock. Reader I/O is
    /// performed after the lock is released.
    pub(crate) fn readers(&self) -> Vec<Arc<TSMReader>> {
        let inner = self.inner.read();
        inner.files.clone()
    }

    #[cfg(feature = "mock_file_store")]
    pub fn files(&self) -> Vec<Arc<TSMReader>> {
        self.readers()
    }

    /// Releases any resources held by the FileStore.  The resources will be re-acquired
    /// if necessary if they are needed after freeing them.
    pub fn free(&self) -> Result<()> {
        let inner = self.inner.read();
        for f in inner.files.iter() {
            f.free().context(ReaderSnafu)?;
        }

        Ok(())
    }

    /// Returns the current generation of the TSM files.
    pub fn current_generation(&self) -> i64 {
        let inner = self.inner.read();
        inner.current_generation
    }

    /// Increments the max file ID and returns the new value.
    pub fn next_generation(&self) -> i64 {
        if cfg!(feature = "mock_file_store") {
            return 1;
        }

        let mut inner = self.inner.write();
        inner.current_generation += 1;
        inner.current_generation
    }

    /// Calls fn for every key in every TSM file known to the FileStore.  If the key
    /// exists in multiple files, it will be invoked for each file.
    pub fn walk_keys<F, E>(&self, seek: &[u8], mut f: F) -> Result<(), E>
    where
        F: FnMut(Vec<u8>, u8) -> std::result::Result<(), E>, // (key, type)
    {
        let (ki, files) = {
            let inner = self.inner.read();
            if inner.files.is_empty() {
                return Ok(());
            }

            // Ensure files are not unmapped while we're iterating over them.
            let files = inner.files.clone();
            for r in files.iter() {
                r.ref_reader();
            }

            let ki = MergeKeyIter::new(files.clone(), seek);

            (ki, files)
        };

        for (key, typ) in ki {
            if let Err(e) = f(key, typ) {
                for r in files.iter() {
                    r.unref_reader();
                }
                return Err(e);
            }
        }

        for r in files.iter() {
            r.unref_reader();
        }

        Ok(())
    }

    /// Returns all keys and types for all files in the file store.
    pub fn keys(&self) -> HashMap<String, u8> {
        let _inner = self.inner.read();

        let mut unique_keys = HashMap::new();
        if self
            .walk_keys(&[], |key, typ| {
                let _ = unique_keys.insert(to_string!(key), typ);
                Ok::<(), Error>(())
            })
            .is_err()
        {
            return HashMap::new();
        }

        unique_keys
    }

    /// Returns all keys and types for the specified files in the file store.
    pub fn file_keys(&self, path: impl AsRef<Path>) -> HashMap<Vec<u8>, u8> {
        let _inner = self.inner.read();

        let mut unique_keys = HashMap::new();

        let mut ki: Option<MergeKeyIter> = None;
        let mut reader: Option<Arc<TSMReader>> = None;

        let inner = self.inner.read();
        if inner.files.is_empty() {
            return HashMap::new();
        }

        // Ensure files are not unmapped while we're iterating over them.
        for r in inner.files.iter() {
            if r.path().eq(path.as_ref()) {
                r.ref_reader();

                ki = Some(MergeKeyIter::new(vec![Arc::clone(r)], &[]));
                reader = Some(Arc::clone(r));
            }
        }
        drop(inner);

        if let Some(ki) = ki {
            for (key, typ) in ki {
                let _ = unique_keys.insert(key, typ);
            }

            reader.unwrap().unref_reader();
        }

        unique_keys
    }

    /// Returns the type of values store at the block for key.
    pub fn value_type(&self, key: &[u8]) -> Result<u8> {
        let inner = self.inner.read();

        for f in inner.files.iter() {
            if f.contains(key) {
                return f.values_type(key).context(ReaderSnafu);
            }
        }

        Err(Error::StringErr {
            reason: format!("unkonwn type for {:?}", key),
        })
    }

    /// Removes the keys from the set of keys available in this file.
    pub fn delete(&self, keys: Vec<Vec<u8>>) -> Result<()> {
        self.delete_range(keys, i64::MIN, i64::MAX)
    }

    pub fn apply<F>(&self, f: F) -> Result<()>
    where
        F: FnMut(Arc<TSMReader>) -> Result<()> + std::marker::Send + 'static + Clone,
    {
        // Limit apply fn to number of cores
        let limiter = Arc::new(Fixed::new(num_cpus::get() as i32));

        let inner = self.inner.read();

        let (s, r) = crossbeam_channel::bounded::<Option<Error>>(inner.files.len());
        for file in inner.files.iter() {
            let file = Arc::clone(file);
            let limiter = Arc::clone(&limiter);
            let mut f = f.clone();
            let s = s.clone();
            let cancelled = s.clone();
            let rejected = s.clone();
            let (registration, completion) = accepted_task_guard(move || {
                let _ = cancelled.send(Some(Error::TaskTerminated));
            });
            match self.executor.spawn_blocking(move || {
                limiter.take();
                let _limiter = guard((), move |_| limiter.release());
                file.ref_reader();
                let file2: Arc<TSMReader> = Arc::clone(&file);
                let _reader_ref = guard(file2, |file| file.unref_reader());

                let result = f(file).err();
                completion.complete_with(|| {
                    let _ = s.send(result);
                });
            }) {
                Ok(_) => registration.accepted(),
                Err(source) => {
                    let _ = rejected.send(Some(Error::Execution { source }));
                    registration.rejected();
                }
            }
        }

        let mut err: Result<()> = Ok(());
        for _ in 0..inner.files.len() {
            if let Ok(Some(e)) = r.recv() {
                err = Err(e);
            }
        }
        drop(inner);

        let mut inner = self.inner.write();
        inner.last_modified = Time::now();
        inner.last_file_stats = vec![];

        err
    }

    /// Removes the values for keys between timestamps min and max.  This should only
    /// be used with smaller batches of series keys.
    pub fn delete_range(&self, keys: Vec<Vec<u8>>, min: i64, max: i64) -> Result<()> {
        let mut batches = BatchDeletes::new_with_executor(Arc::clone(&self.executor));
        {
            let inner = self.inner.read();
            for f in inner.files.iter() {
                if f.overlaps_time_range(min, max) {
                    f.delete_mu.lock();
                    batches.push(BatchDelete::new(Arc::clone(f)));
                }
            }
        }

        if batches.is_empty() {
            return Ok(());
        }

        if let Err(e) = batches.delete_range(keys, min, max).context(ReaderSnafu) {
            let _ = batches.roll_back();
            return Err(e);
        }
        if let Err(e) = batches.commit().context(ReaderSnafu) {
            let _ = batches.roll_back();
            return Err(e);
        }

        let mut inner = self.inner.write();
        inner.last_modified = Time::now();
        inner.last_file_stats = vec![];

        Ok(())
    }

    /// Open loads all the TSM files in the configured directory.
    pub fn open(&self) -> Result<()> {
        let path = Arc::clone(&self.inner.read().dir);
        let started = std::time::Instant::now();
        tracing::debug!(
            target: "reefsdb::tsdb::filestore",
            path = %path.display(),
            "file store opening"
        );
        let result = self.open_inner();
        match &result {
            Ok(()) => tracing::info!(
                target: "reefsdb::tsdb::filestore",
                path = %path.display(),
                file_count = self.stats.file_count.load(Ordering::SeqCst),
                duration_ms = started.elapsed().as_millis() as u64,
                "file store opened"
            ),
            Err(error) => tracing::error!(
                target: "reefsdb::tsdb::filestore",
                path = %path.display(),
                error = %error,
                "file store open failed"
            ),
        }
        result
    }

    fn open_inner(&self) -> Result<()> {
        let mut inner = self.inner.write();

        // Not loading files from disk so nothing to do
        if inner.dir.as_path().eq(Path::new("")) {
            return Ok(());
        }

        // check inner.open_limiter.is_none first ?

        // find the current max ID for temp directories
        let mut tmp_files = fs::read_dir(&*inner.dir)?
            .map(|f| {
                let f = f.unwrap();
                f.path()
            })
            .collect::<Vec<PathBuf>>();

        tmp_files.sort();

        // ascertain the current temp directory number by examining the existing
        // directories and choosing the one with the higest basename when converted
        // to an integer.
        for fi in tmp_files.iter() {
            if !fi.is_dir() || !fi.ends_with(".".to_string() + TMP_TSM_FILE_EXTENSION) {
                continue;
            }

            let ss = fi
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .split('.')
                .collect::<Vec<&str>>();
            if ss.len() != 2 {
                continue;
            }

            let i = match atoi_simd::parse::<i64>(ss[0].as_bytes()) {
                Ok(i) => {
                    if i <= inner.current_temp_dir_id {
                        continue;
                    }
                    i
                }
                Err(_) => {
                    continue;
                }
            };

            // i must be a valid integer and greater than inner.current_temp_dir_id at this point
            inner.current_temp_dir_id = i;
        }

        let files = tmp_files
            .iter()
            .filter(|&entry| match entry.extension() {
                Some(ext) => ext == TSM_FILE_EXTENSION,
                None => false,
            })
            .cloned()
            .collect::<Vec<PathBuf>>();

        let (s, r) = crossbeam_channel::bounded::<Result<Option<TSMReader>>>(files.len());
        for f in files.iter() {
            let path = f.to_str().unwrap();

            // Keep track of the latest ID
            let (generation, seq) = default_parse_file_name(path)?;

            if generation >= inner.current_generation {
                inner.current_generation = generation + 1;
            }

            let file = match fs::OpenOptions::new().read(true).open(path) {
                Ok(file) => file,
                Err(err) => {
                    return Err(Error::StringErr {
                        reason: format!("error opening file {}: {:?}", path, err),
                    });
                }
            };
            file.metadata()?.permissions().set_mode(0o666);

            let open_limiter = inner.open_limiter.clone();
            let s = s.clone();
            let path = path.to_string();
            let cancelled = s.clone();
            let rejected = s.clone();
            let (registration, completion) = accepted_task_guard(move || {
                let _ = cancelled.send(Err(Error::TaskTerminated));
            });
            match self.executor.spawn_blocking(move || {
                // Ensure a limited number of TSM files are loaded at once.
                // Systems which have very large datasets (1TB+) can have thousands
                // of TSM files which can cause extremely long load times.
                open_limiter.take();
                let _open_limit = guard((), move |_| open_limiter.release());
                let file_path = Path::new(&path);
                let result = match TSMReader::new(file, file_path, generation, seq) {
                    Ok(reader) => Ok(Some(reader)),
                    Err(err) => {
                        // If we are unable to read a TSM file then log the error, rename
                        // the file, and continue loading the shard without it.
                        // TODO: drop file
                        match fs::rename(file_path, path.clone() + BAD_TSM_FILE_EXTENSION) {
                            Ok(()) => Err(Error::StringErr {
                                reason: format!("cannot read corrupt file {:?}: {}", path, err),
                            }),
                            Err(error) => Err(Error::StringErr {
                                reason: format!("cannot rename corrupt file {:?}: {}", path, error),
                            }),
                        }
                    }
                };
                completion.complete_with(|| {
                    let _ = s.send(result);
                });
            }) {
                Ok(_) => registration.accepted(),
                Err(source) => {
                    let _ = rejected.send(Err(Error::Execution { source }));
                    registration.rejected();
                }
            }
        }

        let mut lm = 0i64;
        for _ in 0..files.len() {
            match r.recv() {
                Ok(Ok(Some(r))) => {
                    // Accumulate file store size stats
                    self.stats
                        .disk_bytes
                        .fetch_add(r.size() as i64, Ordering::SeqCst);
                    let ts = r.tombstone_stats();
                    if ts.tombstone_exists {
                        self.stats
                            .disk_bytes
                            .fetch_add(ts.size as i64, Ordering::SeqCst);
                    }

                    // Re-initialize the lastModified time for the file store
                    if r.last_modified() > lm {
                        lm = r.last_modified();
                    }

                    inner.files.push(Arc::new(r));
                }
                Ok(Ok(None)) => {
                    continue;
                }
                Ok(Err(e)) => {
                    return Err(e);
                }
                _ => {}
            }
        }

        inner.last_modified = Time::from_timestamp_nanos(lm);
        inner.files.sort_by_key(|r| r.path());
        self.stats
            .file_count
            .store(inner.files.len() as i64, Ordering::SeqCst);

        Ok(())
    }

    #[cfg(feature = "mock_file_store")]
    pub fn close(&self) -> Result<()> {
        let mut readers = self.readers.lock();
        for r in readers.iter() {
            r.close();
        }
        *readers = vec![];
        Ok(())
    }

    /// Close the file store.
    #[cfg(not(feature = "mock_file_store"))]
    pub fn close(&self) -> Result<()> {
        let path = Arc::clone(&self.inner.read().dir);
        tracing::debug!(
            target: "reefsdb::tsdb::filestore",
            path = %path.display(),
            "file store closing"
        );
        // Make the object appear closed to other method calls.
        let mut inner = self.inner.write();

        let files = inner.files.clone();

        inner.last_file_stats = vec![];
        inner.files = vec![];

        self.stats.file_count.store(0, Ordering::SeqCst);

        // Let other methods access this closed object while we do the actual closing.
        drop(inner);

        for f in files {
            f.close().context(ReaderSnafu)?;
        }

        tracing::info!(
            target: "reefsdb::tsdb::filestore",
            path = %path.display(),
            "file store closed"
        );

        Ok(())
    }

    pub fn disk_size_bytes(&self) -> i64 {
        self.stats.disk_bytes.load(Ordering::SeqCst)
    }

    /// Returns the slice of values for the given key and the given timestamp,
    /// if any file matches those constraints.
    pub fn read(&self, key: &[u8], t: i64) -> Result<Vec<Value>> {
        let inner = self.inner.read();

        for f in inner.files.iter() {
            // Can this file possibly contain this key and timestamp?
            if !f.contains(key) {
                continue;
            }

            // May have the key and time we are looking for so try to find
            let v = f.read(key, t).context(ReaderSnafu)?;
            if !v.is_empty() {
                return Ok(v);
            }
        }

        Ok(vec![])
    }

    /// We need to determine the possible files that may be accessed by this query given
    /// the time range.
    pub fn cost(&self, key: &[u8], min: i64, max: i64) -> query_iterator::IteratorCost {
        let inner = self.inner.read();
        inner.cost(key, min, max)
    }

    /// Returns a TSMReader for path if one is currently managed by the FileStore.
    /// Otherwise it returns nil. If it returns a file, you must call Unref on it when
    /// you are done, and never use it after that.
    #[cfg(not(feature = "mock_file_store"))]
    pub fn tsm_reader(&self, path: impl AsRef<Path>) -> Option<Arc<TSMReader>> {
        let inner = self.inner.read();
        for f in inner.files.iter() {
            if f.path().as_path().eq(path.as_ref()) {
                let f = Arc::clone(f);
                f.ref_reader();
                return Some(f);
            }
        }

        None
    }

    #[cfg(feature = "mock_file_store")]
    pub fn tsm_reader(
        &self,
        path: impl AsRef<Path>,
        generation: i64,
        seq: i64,
    ) -> Option<Arc<TSMReader>> {
        let f = fs::OpenOptions::new().read(true).open(&path).unwrap();
        let r = Arc::new(TSMReader::new(f, path.as_ref(), generation, seq).unwrap());

        let mut readers = self.readers.lock();
        readers.push(Arc::clone(&r));

        r.ref_reader();
        Some(r)
    }

    /// Returns a KeyCursor for key and t across the files in the FileStore.
    pub fn key_cursor(fs: Arc<FileStore>, key: &[u8], t: i64, ascending: bool) -> KeyCursor {
        let lock_start = slow_trace::now();
        let inner = fs.inner.read();
        slow_trace::log_lock_wait(
            "file_store.read",
            lock_start,
            "stage=query_wait_for_file_store_lock",
        );
        let lookup_start = slow_trace::now();
        let cursor = inner.key_cursor(key, t, ascending);
        slow_trace::log_query_stage("query_tsm_lookup", lookup_start, "component=file_store");
        cursor
    }

    /// Returns the stats of the underlying files, preferring the cached version if it is still valid.
    #[cfg(not(feature = "mock_file_store"))]
    pub fn stats(&self) -> Vec<FileStat> {
        {
            let inner = self.inner.read();
            if !inner.last_file_stats.is_empty() {
                return inner.last_file_stats.clone();
            }
        }

        // The file stats cache is invalid due to changes to files. Need to recalculate.
        let lock_start = slow_trace::now();
        let mut inner = self.inner.write();
        slow_trace::log_lock_wait("file_store.write", lock_start, "stage=replace_files");
        if !inner.last_file_stats.is_empty() {
            return inner.last_file_stats.clone();
        }

        // If lastFileStats's capacity is far away from the number of entries
        // we need to add, then we'll reallocate.
        if inner.last_file_stats.capacity() < inner.files.len() / 2 {
            inner.last_file_stats = Vec::with_capacity(inner.files.len());
        }

        let mut stats = vec![];
        for fd in inner.files.iter() {
            stats.push(fd.stats());
        }
        inner.last_file_stats.append(&mut stats);

        inner.last_file_stats.clone()
    }

    #[cfg(feature = "mock_file_store")]
    pub fn stats(&self) -> Vec<FileStat> {
        //    self.paths_fn.as_ref().unwrap()()
        match self.paths_fn {
            Some(f) => f(),
            None => vec![],
        }
    }

    /// Replaces oldFiles with newFiles.
    pub fn replace(&self, old_files: &[String], new_files: &[String]) -> Result<()> {
        if old_files.is_empty() && new_files.is_empty() {
            return Ok(());
        }
        tracing::debug!(
            target: "reefsdb::tsdb::filestore",
            old_file_count = old_files.len(),
            new_file_count = new_files.len(),
            "replacing file store files"
        );

        let mut max_time = {
            let inner = self.inner.read();
            inner.last_modified
        };

        let mut updated = vec![];
        let tsm_tmp_ext = format!("{}.{}", TSM_FILE_EXTENSION, TMP_TSM_FILE_EXTENSION);
        // Rename all the new files to make them live on restart
        for file in new_files {
            if !file.ends_with(&tsm_tmp_ext) && !file.ends_with(TSM_FILE_EXTENSION) {
                // This isn't a .tsm or .tsm.tmp file.
                continue;
            }

            let (old_name, mut new_name) = (file.as_str(), file.as_str());
            if old_name.ends_with(&tsm_tmp_ext) {
                // The new TSM files have a tmp extension.  First rename them.
                new_name = &file[..file.len() - 4];
                fs::rename(old_name, new_name)?;
            }
            let (generation, seq) = default_parse_file_name(new_name)?;
            // Any error after this point should result in the file being bein named
            // back to the original name. The caller then has the opportunity to
            // remove it.
            let fd = match fs::File::open(new_name) {
                Ok(fd) => fd,
                Err(e) => {
                    if new_name != old_name {
                        fs::rename(new_name, old_name)?;
                    }
                    return Err(Error::IO { reason: e });
                }
            };

            // Keep track of the new mod time
            let stat = fs::metadata(new_name)?;
            if max_time.timestamp_nanos() == 0
                || chrono::DateTime::<chrono::Utc>::from(stat.modified().unwrap())
                    > max_time.date_time()
            {
                max_time = Time::from_date_time(stat.modified().unwrap().into());
            }

            let tsm = match TSMReader::new(fd, Path::new(&new_name), generation, seq) {
                Ok(r) => r,
                Err(e) => {
                    if new_name != old_name {
                        fs::rename(new_name, old_name)?;
                    }
                    return Err(Error::Reader { source: e });
                }
            };

            updated.push(Arc::new(tsm));
        }

        let mut inner = self.inner.write();

        // Copy the current set of active files while we rename
        // and load the new files.  We copy the pointers here to minimize
        // the time that locks are held as well as to ensure that the replacement
        // is atomic.©
        updated.extend_from_slice(&inner.files);

        // We need to prune our set of active files now
        let (mut active, mut inuse) = (vec![], vec![]);
        for file in updated.iter() {
            let mut keep = true;
            for remove in old_files.iter() {
                if file.path() == Path::new(remove) {
                    keep = false;

                    // give the observer a chance to process the file first.

                    // If queries are running against this file, then we need to move it out of the
                    // way and let them complete.  We'll then delete the original file to avoid
                    // blocking callers upstream.  If the process crashes, the temp file is
                    // cleaned up at startup automatically.
                    //
                    // In order to ensure that there are no races with this (file held externally calls Ref
                    // after we check InUse), we need to maintain the invariant that every handle to a file
                    // is handed out in use (Ref'd), and handlers only ever relinquish the file once (call Unref
                    // exactly once, and never use it again). InUse is only valid during a write lock, since
                    // we allow calls to Ref and Unref under the read lock and no lock at all respectively.
                    if file.in_use() {
                        // Copy all the tombstones related to this TSM file
                        let mut deletes = vec![];
                        let ts = file.tombstone_stats();
                        if ts.tombstone_exists {
                            deletes.push(ts.path);
                        }

                        // Rename the TSM file used by this reader
                        let tmp_path = format!(
                            "{}.{}",
                            file.path().to_str().unwrap(),
                            TMP_TSM_FILE_EXTENSION
                        );
                        file.rename(Path::new(&tmp_path)).context(ReaderSnafu)?;

                        // Remove the old file and tombstones.  We can't use the normal TSMReader.Remove()
                        // because it now refers to our temp file which we can't remove.
                        for f in deletes.iter() {
                            fs::remove_file(f)?;
                        }

                        inuse.push(Arc::clone(file));

                        continue;
                    }

                    file.close().context(ReaderSnafu)?;

                    file.remove().context(ReaderSnafu)?;

                    break;
                }
            }

            if keep {
                active.push(Arc::clone(file));
            }
        }

        // TODO: implement me
        // if err := file.SyncDir(f.dir); err != nil {
        //     return err
        // }

        // Tell the purger about our in-use files we need to remove
        inner.purger.add(&inuse);

        // If times didn't change (which can happen since file mod times are second level),
        // then add a ns to the time to ensure that lastModified changes since files on disk
        // actually did change
        if max_time <= inner.last_modified {
            // if max time equal or after last modified time
            max_time = inner
                .last_modified
                .checked_add(Duration::from_nanos(1))
                .unwrap();
        }

        inner.last_modified = max_time;

        inner.last_file_stats = vec![];
        inner.files = active;
        inner.files.sort_by_key(|r| r.path());
        self.stats
            .file_count
            .store(inner.files.len() as i64, Ordering::SeqCst);

        // Recalculate the disk size stat
        let mut total_size = 0;
        for f in inner.files.iter() {
            total_size += f.size() as i64;
            let ts = f.tombstone_stats();
            if ts.tombstone_exists {
                total_size += ts.size as i64;
            }
        }

        self.stats.disk_bytes.store(total_size, Ordering::SeqCst);

        tracing::debug!(
            target: "reefsdb::tsdb::filestore",
            old_file_count = old_files.len(),
            new_file_count = new_files.len(),
            active_file_count = inner.files.len(),
            disk_bytes = total_size,
            "file store files replaced"
        );

        Ok(())
    }

    /// Returns the last time the file store was updated with new TSM files or a delete.
    pub fn last_modified(&self) -> Time {
        let inner = self.inner.read();

        inner.last_modified
    }

    #[cfg(feature = "mock_file_store")]
    pub fn set_last_modified(&self, t: Time) {
        let mut inner = self.inner.write();
        inner.last_modified = t;
    }

    /// Returns number of values stored in the block at location idx
    /// in the file at path.  If path does not match any file in the store, 0 is
    /// returned.  If idx is out of range for the number of blocks in the file,
    /// 0 is returned.
    #[cfg(not(feature = "mock_file_store"))]
    pub fn block_count(&self, path: &Path, idx: usize) -> i32 {
        let inner = self.inner.read();

        if idx == 0 {
            return 0;
        }

        for fd in inner.files.iter() {
            if fd.path() == path {
                let mut itr = TSMReader::block_iter(Arc::clone(fd));
                for _ in 0..idx {
                    if itr.next().is_none() {
                        return 0;
                    }
                }
                let block = itr.next().unwrap().block;
                // on Error, BlockCount(block) returns 0 for cnt
                return encoding::block_count(block.as_slice()).expect("block_count error");
            }
        }

        0
    }

    #[cfg(feature = "mock_file_store")]
    pub fn block_count(&self, path: &Path, idx: usize) -> i32 {
        self.block_count
    }

    /// Creates hardlinks for all tsm and tombstone files in the path provided.
    pub fn create_snapshot(&self) -> Result<PathBuf> {
        let mut inner = self.inner.write();

        // create a copy of the files slice and ensure they aren't closed out from
        // under us, nor the slice mutated.
        let files = inner.files.clone();

        for tsmf in files.iter() {
            tsmf.ref_reader();
        }

        // increment and keep track of the current temp dir for when we drop the lock.
        // this ensures we are the only writer to the directory.
        inner.current_temp_dir_id += 1;
        let tmp_path = PathBuf::from(format!(
            "{}.{}",
            inner.current_temp_dir_id, TMP_TSM_FILE_EXTENSION
        ));
        let tmp_path = inner.dir.as_path().join(tmp_path);
        drop(inner);

        // create the tmp directory and add the hard links. there is no longer any shared
        // mutable state.
        if let Err(e) = fs::create_dir_all(&tmp_path) {
            for tsmf in files.iter() {
                tsmf.unref_reader();
            }
            return Err(e.into());
        }
        let stat = match fs::metadata(&tmp_path) {
            Ok(stat) => stat,
            Err(e) => {
                for tsmf in files.iter() {
                    tsmf.unref_reader();
                }
                return Err(e.into());
            }
        };
        stat.permissions().set_mode(0o777);

        for tsmf in files.iter() {
            let new_path = tmp_path.join(tsmf.path().file_name().unwrap());
            if let Err(e) = fs::hard_link(tsmf.path(), new_path) {
                for tsmf in files.iter() {
                    tsmf.unref_reader();
                }

                return Err(Error::StringErr {
                    reason: format!("error createing tsm hard link: {:?}", e),
                });
            }

            let ts = tsmf.tombstone_stats();
            if ts.tombstone_exists {
                let new_path = tmp_path.join(ts.path.file_name().unwrap());
                if let Err(e) = fs::hard_link(tsmf.path(), new_path) {
                    for tsmf in files.iter() {
                        tsmf.unref_reader();
                    }

                    return Err(Error::StringErr {
                        reason: format!("error createing tombstone hard link: {:?}", e),
                    });
                }
            }
        }

        for tsmf in files.iter() {
            tsmf.unref_reader();
        }

        Ok(tmp_path)
    }
}

pub fn default_format_file_name(generation: i64, sequence: i64) -> String {
    format!("{:09}-{:09}", generation, sequence)
}

pub fn default_parse_file_name(name: &str) -> Result<(i64, i64)> {
    let path = Path::new(name);
    let base = path.file_name().unwrap().to_str().unwrap().to_string();
    let idx = match base.find('.') {
        Some(idx) => idx,
        None => {
            return Err(Error::StringErr {
                reason: format!("find {} is named incorrectly", name),
            });
        }
    };

    let id = base.get(..idx).unwrap();
    let idx = match id.find('-') {
        Some(idx) => idx,
        None => {
            return Err(Error::StringErr {
                reason: format!("find {} is named incorrectly", name),
            });
        }
    };

    let generation = match atoi_simd::parse::<u32>(id.get(..idx).unwrap().as_bytes()) {
        Ok(generation) => generation,
        Err(_) => {
            return Err(Error::StringErr {
                reason: format!("find {} is named incorrectly", name),
            });
        }
    };

    let seq = match atoi_simd::parse::<u32>(id.get(idx + 1..).unwrap().as_bytes()) {
        Ok(seq) => seq as u64,
        Err(_) => {
            return Err(Error::StringErr {
                reason: format!("find {} is named incorrectly", name),
            });
        }
    };

    Ok((generation as i64, seq as i64))
}

impl Inner {
    fn key_cursor(&self, key: &[u8], t: i64, ascending: bool) -> KeyCursor {
        let files = self.files.as_slice();
        KeyCursor::new(files, key, t, ascending)
    }

    pub fn cost(&self, key: &[u8], min: i64, max: i64) -> query_iterator::IteratorCost {
        let mut cache = Vec::<IndexEntry>::new();
        let mut cost = query_iterator::IteratorCost::default();
        for fd in self.files.iter() {
            let (min_time, max_time) = fd.time_range();
            if !(max_time > min && min_time < max) {
                continue;
            }
            let mut skipped = true;
            let tombstones = fd.tombstone_range(key);

            let entries = fd.read_entries(key, Some(&mut cache));
            'inner: loop {
                for ie in entries.iter() {
                    if !(ie.max_time > min && ie.min_time < max) {
                        continue;
                    }

                    // Skip any blocks only contain values that are tombstoned.
                    for t in tombstones.iter() {
                        if t.min <= ie.min_time && t.max >= ie.max_time {
                            continue 'inner;
                        }
                    }

                    cost.blocks_read += 1;
                    cost.block_size += ie.size as i64;
                    skipped = false;
                }

                if !skipped {
                    cost.num_files += 1;
                }
            }
        }

        cost
    }
}

// FileStoreStatistics keeps statistics about the file store.
#[derive(Default)]
pub struct FileStoreStatistics {
    disk_bytes: AtomicI64,
    file_count: AtomicI64,
}

struct Purger {
    inner: Arc<RwLock<InnerPurger>>,
    executor: ExecutorRef,
}
#[derive(Default)]
struct InnerPurger {
    files: HashMap<String, Arc<TSMReader>>,
    running: bool,
}

impl Purger {
    fn new(executor: ExecutorRef) -> Self {
        Self {
            inner: Arc::new(RwLock::new(InnerPurger::default())),
            executor,
        }
    }

    pub fn add(&self, files: &[Arc<TSMReader>]) {
        {
            let mut inner = self.inner.write();
            for f in files {
                inner
                    .files
                    .insert(f.path().to_str().unwrap().to_owned(), Arc::clone(f));
            }
        }
        self.purge()
    }

    fn purge(&self) {
        {
            let mut inner = self.inner.write();
            if inner.running {
                return;
            }
            inner.running = true;
        }

        let inner = Arc::clone(&self.inner);
        let cancelled_inner = Arc::clone(&self.inner);
        let (registration, completion) = accepted_task_guard(move || {
            cancelled_inner.write().running = false;
        });
        match self.executor.spawn_blocking(move || {
            loop {
                {
                    let mut inner = inner.write();
                    inner.files.retain(|_, v| {
                        // In order to ensure that there are no races with this (file held externally calls Ref
                        // after we check InUse), we need to maintain the invariant that every handle to a file
                        // is handed out in use (Ref'd), and handlers only ever relinquish the file once (call Unref
                        // exactly once, and never use it again). InUse is only valid during a write lock, since
                        // we allow calls to Ref and Unref under the read lock and no lock at all respectively.
                        if !v.in_use() {
                            if v.close().is_err() {
                                // p.logger.Info("Purge: close file", zap.Error(err))
                                return false;
                            }

                            if v.remove().is_err() {
                                // p.logger.Info("Purge: remove file", zap.Error(err))
                                return false;
                            }
                            true
                        } else {
                            false
                        }
                    });

                    if inner.files.is_empty() {
                        inner.running = false;
                        completion.complete();
                        return;
                    }
                }
                std::thread::sleep(Duration::from_secs(1)); // FIXME: use async ???????
            }
        }) {
            Ok(_) => registration.accepted(),
            Err(error) => {
                self.inner.write().running = false;
                registration.rejected();
                tracing::warn!(
                    target: "reefsdb::tsdb::filestore",
                    %error,
                    "unable to schedule optional TSM file purge"
                );
            }
        }
    }
}

// TombstoneStat holds information about a possible tombstone file on disk.
#[derive(Default, Clone)]
pub struct TombstoneStat {
    pub tombstone_exists: bool,
    pub path: PathBuf,
    pub last_modified: i64,
    pub size: u64,
}

// FileStat holds information about a TSM file on disk.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct FileStat {
    pub path: PathBuf,
    pub has_tombstone: bool,
    pub size: u32,
    pub last_modified: i64,
    pub min_time: i64,
    pub max_time: i64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
}

impl FileStat {
    /// Returns true if the time range of the file intersect min and max.
    pub fn overlaps_time_range(&self, min: i64, max: i64) -> bool {
        self.min_time <= max && self.max_time >= min
    }

    /// Returns true if the min and max keys of the file overlap the arguments min and max.
    pub fn overlaps_key_range(&self, min: &[u8], max: &[u8]) -> bool {
        !min.is_empty()
            && !max.is_empty()
            && self.min_key.as_slice() <= max
            && self.max_key.as_slice() >= min
    }

    /// Returns true if the min and max keys of the file overlap the arguments min and max.
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.min_key.as_slice() >= key && key <= self.max_key.as_slice()
    }
}

// file store key iter
pub struct KeyIter {
    f: Arc<TSMReader>,
    c: usize, // current key index
    n: usize, // key count
    key: Vec<u8>,
    typ: u8,
}

impl KeyIter {
    pub fn new(f: Arc<TSMReader>, seek: &[u8]) -> Option<Self> {
        let (mut c, n) = (0, f.key_count());
        if !seek.is_empty() {
            c = f.seek(seek);
        }

        if c >= n {
            return None;
        }

        let mut k = Self {
            f,
            c,
            n,
            key: vec![],
            typ: 0,
        };
        let _ = k.has_next();

        Some(k)
    }

    fn has_next(&mut self) -> bool {
        if self.c < self.n {
            (self.key, self.typ) = self.f.key_at(self.c);
            self.c += 1;
            return true;
        }
        false
    }
}

struct KeyIters(Vec<KeyIter>);

impl KeyIters {
    /// Init establishes the heap.
    pub fn init(&mut self) {
        // heapify
        let n = self.len();
        if (n as i64 / 2) - 1 < 0 {
            return;
        }
        for i in (0..=((n / 2) - 1)).rev() {
            self.down(i, n);
        }
    }

    /// Push pushes the element x onto the heap.
    /// The complexity is O(log n) where n = h.Len().
    fn push(&mut self, x: KeyIter) {
        self.0.push(x);
        self.up(self.len() - 1);
    }

    /// Pop removes and returns the minimum element (according to Less) from the heap.
    /// The complexity is O(log n) where n = h.Len().
    /// Pop is equivalent to Remove(h, 0).
    fn pop(&mut self) -> Option<KeyIter> {
        let n = self.len() - 1;
        self.swap(0, n);
        self.down(0, n);
        self.0.pop()
    }

    /// Fix re-establishes the heap ordering after the element at index i has changed its value.
    /// Changing the value of the element at index i and then calling Fix is equivalent to,
    /// but less expensive than, calling Remove(h, i) followed by a Push of the new value.
    /// The complexity is O(log n) where n = h.Len().
    fn fix(&mut self, i: usize) {
        if !self.down(i, self.len()) {
            self.up(i);
        }
    }

    fn up(&mut self, j: usize) {
        let mut j = j;
        loop {
            let i = (j - 1) / 2; // parent
            if i == j || !self.less(j, i) {
                break;
            }
            self.swap(i, j);
            j = i;
        }
    }

    fn down(&mut self, i0: usize, n: usize) -> bool {
        let mut i = i0;
        loop {
            let j1 = 2 * i + 1;
            if j1 >= n || j1 < 0 {
                // j1 < 0 after int overflow  // FIXME: check overflow ?
                break;
            }
            let mut j = j1; // left child
            let j2 = j1 + 1;
            if j2 < n && self.less(j2, j1) {
                j = j2; // = 2*i + 2  // right child
            }
            if !self.less(j, i) {
                break;
            }

            // swap items
            self.swap(i, j);

            i = j;
        }
        i > i0
    }

    fn less(&self, i: usize, j: usize) -> bool {
        self[i].key < self[j].key
    }
}

impl Deref for KeyIters {
    type Target = Vec<KeyIter>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for KeyIters {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

struct MergeKeyIter {
    itrs: KeyIters,
    key: Vec<u8>,
    typ: u8,
}

impl MergeKeyIter {
    pub fn new(files: Vec<Arc<TSMReader>>, seek: &[u8]) -> Self {
        let mut itrs = Vec::with_capacity(files.len());
        for f in files {
            if let Some(ki) = KeyIter::new(f, seek) {
                itrs.push(ki);
            }
        }

        let mut m = Self {
            itrs: KeyIters(itrs),
            key: vec![],
            typ: 0,
        };

        // TODO: init to establish heap
        m.itrs.init();
        m
    }
}

impl Iterator for MergeKeyIter {
    type Item = (Vec<u8>, u8);

    // (key, type)

    fn next(&mut self) -> Option<Self::Item> {
        let merging = self.itrs.len() > 1;

        loop {
            if self.itrs.is_empty() {
                return None;
            }

            let (key, typ) = (self.itrs[0].key.clone(), self.itrs[0].typ);
            let more = self.itrs[0].has_next();

            match self.itrs.len().cmp(&1) {
                std::cmp::Ordering::Greater => {
                    if !more {
                        // remove the minimum element.
                        let _ = self.itrs.pop();
                    } else {
                        self.itrs.fix(0);
                    }
                }
                std::cmp::Ordering::Equal => {
                    if !more {
                        self.itrs = KeyIters(vec![]);
                    }
                }
                _ => {}
            }

            if merging && self.key.as_slice() == key.as_slice() {
                // same as previous key, keep iterating
                continue;
            }

            self.key = key;
            self.typ = typ;
            break;
        }

        Some((self.key.clone(), self.typ))
    }
}

#[cfg(test)]
pub mod tests {
    use std::{
        collections::HashMap,
        fs,
        os::unix::fs::{MetadataExt, PermissionsExt},
        path::{Path, PathBuf},
        sync::{
            Arc,
            atomic::{AtomicI64, Ordering},
        },
        time::Duration,
    };

    use crate::models::points::{
        BoolValue, FloatValue, IntegerValue, StringValue, UnsignedValue, Value,
    };

    use super::{FileStore, default_format_file_name};
    use crate::storage::tsm1::{
        cache::tests::{must_temp_tsm_dir, must_temp_tsm_file},
        compact::tests::must_open_tsm_reader,
        data_encoding::encoding_values::Values,
        file_store::TMP_TSM_FILE_EXTENSION,
        reader::TSMReader,
        writer::TSMWriter,
    };

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_read() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(1, 2.0))],
            },
            KeyValues {
                key: "mem",
                values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        // Search for an entry that exists in the second file
        let values = fs.read("cpu".as_bytes(), 1).unwrap();

        let exp = &data[1];
        assert_eq!(
            values.len(),
            exp.values.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.values.len()
        );

        for (i, v) in exp.values.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.value());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_asc_from_start() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(1, 2.0))],
            },
            KeyValues {
                key: "mem",
                values: vec![Value::FloatValue(FloatValue::new(2, 3.0))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 0, true);

        // Search for an entry that exists in the second file
        let values = c.read_float_block().unwrap();

        let exp = &data[0];
        assert_eq!(
            values.len(),
            exp.values.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.values.len()
        );

        for (i, v) in exp.values.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.float_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_asc_duplicate() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(0, 2.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(2, 3.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(2, 4.0))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 0, true);

        // Search for an entry that exists in the second file
        let values = c.read_float_block().unwrap();

        let exp = vec![data[1].values[0].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.float_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        // Check that calling Next will dedupe points
        c.next();
        let values = c.read_float_block().unwrap();

        let exp = vec![data[3].values[0].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.float_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_float_block().unwrap();

        assert_eq!(
            values.len(),
            0,
            "value length mismatch: got {}, exp 0",
            values.len()
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_asc_before_start() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(1, 1.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(2, 2.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(3, 3.0))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 0, true);

        // Search for an entry that exists in the second file
        let values = c.read_float_block().unwrap();

        let exp = &data[0];
        assert_eq!(
            values.len(),
            exp.values.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.values.len()
        );

        for (i, v) in exp.values.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.float_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_asc_before_start_overlap_float() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::FloatValue(FloatValue::new(0, 0.0)),
                    Value::FloatValue(FloatValue::new(1, 1.0)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(2, 2.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(3, 3.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::FloatValue(FloatValue::new(0, 4.0)),
                    Value::FloatValue(FloatValue::new(2, 7.0)),
                ],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 0, true);

        // Search for an entry that exists in the second file
        let values = c.read_float_block().unwrap();

        let exp = vec![
            data[3].values[0].clone(),
            data[0].values[1].clone(),
            data[3].values[1].clone(),
        ];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.float_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_float_block().unwrap();

        let exp = vec![data[2].values[0].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.float_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_asc_before_start_overlap_integer() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::IntegerValue(IntegerValue::new(0, 0)),
                    Value::IntegerValue(IntegerValue::new(1, 1)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::IntegerValue(IntegerValue::new(2, 2))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::IntegerValue(IntegerValue::new(3, 3))],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::IntegerValue(IntegerValue::new(0, 4)),
                    Value::IntegerValue(IntegerValue::new(2, 7)),
                ],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 0, true);

        // Search for an entry that exists in the second file
        let values = c.read_integer_block().unwrap();

        let exp = vec![
            data[3].values[0].clone(),
            data[0].values[1].clone(),
            data[3].values[1].clone(),
        ];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.integer_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_integer_block().unwrap();

        let exp = vec![data[2].values[0].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.integer_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_asc_before_start_overlap_unsigned() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::UnsignedValue(UnsignedValue::new(0, 0)),
                    Value::UnsignedValue(UnsignedValue::new(1, 1)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::UnsignedValue(UnsignedValue::new(2, 2))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::UnsignedValue(UnsignedValue::new(3, 3))],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::UnsignedValue(UnsignedValue::new(0, 4)),
                    Value::UnsignedValue(UnsignedValue::new(2, 7)),
                ],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 0, true);

        // Search for an entry that exists in the second file
        let values = c.read_unsigned_block().unwrap();

        let exp = vec![
            data[3].values[0].clone(),
            data[0].values[1].clone(),
            data[3].values[1].clone(),
        ];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.unsigned_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_unsigned_block().unwrap();

        let exp = vec![data[2].values[0].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.unsigned_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_asc_before_start_overlap_bool() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::BoolValue(BoolValue::new(0, true)),
                    Value::BoolValue(BoolValue::new(1, false)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::BoolValue(BoolValue::new(2, true))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::BoolValue(BoolValue::new(3, true))],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::BoolValue(BoolValue::new(0, false)),
                    Value::BoolValue(BoolValue::new(2, true)),
                ],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 0, true);

        // Search for an entry that exists in the second file
        let values = c.read_bool_block().unwrap();

        let exp = vec![
            data[3].values[0].clone(),
            data[0].values[1].clone(),
            data[3].values[1].clone(),
        ];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.bool_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_bool_block().unwrap();

        let exp = vec![data[2].values[0].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.bool_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_asc_before_start_overlap_string() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::StringValue(StringValue::new(0, "zero".to_string())),
                    Value::StringValue(StringValue::new(1, "one".to_string())),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::StringValue(StringValue::new(2, "two".to_string()))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::StringValue(StringValue::new(3, "three".to_string()))],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::StringValue(StringValue::new(0, "four".to_string())),
                    Value::StringValue(StringValue::new(2, "seven".to_string())),
                ],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 0, true);

        // Search for an entry that exists in the second file
        let values = c.read_string_block().unwrap();

        let exp = vec![
            data[3].values[0].clone(),
            data[0].values[1].clone(),
            data[3].values[1].clone(),
        ];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.string_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_string_block().unwrap();

        let exp = vec![data[2].values[0].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.string_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_asc_before_start_overlap_min_float() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::FloatValue(FloatValue::new(1, 1.0)),
                    Value::FloatValue(FloatValue::new(3, 3.0)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::FloatValue(FloatValue::new(2, 2.0)),
                    Value::FloatValue(FloatValue::new(4, 4.0)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::FloatValue(FloatValue::new(0, 0.0)),
                    Value::FloatValue(FloatValue::new(1, 1.0)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(2, 2.2))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 0, true);

        // Search for an entry that exists in the second file
        let values = c.read_float_block().unwrap();

        let exp = vec![
            data[2].values[0].clone(),
            data[2].values[1].clone(),
            data[3].values[0].clone(),
            data[0].values[1].clone(),
        ];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.float_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_float_block().unwrap();

        let exp = vec![data[1].values[1].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.float_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_float_block().unwrap();

        assert_eq!(
            values.len(),
            0,
            "value length mismatch: got {}, exp {}",
            values.len(),
            0
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_asc_before_start_overlap_min_integer() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::IntegerValue(IntegerValue::new(1, 1)),
                    Value::IntegerValue(IntegerValue::new(3, 3)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::IntegerValue(IntegerValue::new(2, 2)),
                    Value::IntegerValue(IntegerValue::new(4, 4)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::IntegerValue(IntegerValue::new(0, 0)),
                    Value::IntegerValue(IntegerValue::new(1, 10)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::IntegerValue(IntegerValue::new(2, 5))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 0, true);

        // Search for an entry that exists in the second file
        let values = c.read_integer_block().unwrap();

        let exp = vec![
            data[2].values[0].clone(),
            data[2].values[1].clone(),
            data[3].values[0].clone(),
            data[0].values[1].clone(),
        ];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.integer_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_integer_block().unwrap();

        let exp = vec![data[1].values[1].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.integer_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_integer_block().unwrap();

        assert_eq!(
            values.len(),
            0,
            "value length mismatch: got {}, exp {}",
            values.len(),
            0
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_asc_before_start_overlap_min_unsigned() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::UnsignedValue(UnsignedValue::new(1, 1)),
                    Value::UnsignedValue(UnsignedValue::new(3, 3)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::UnsignedValue(UnsignedValue::new(2, 2)),
                    Value::UnsignedValue(UnsignedValue::new(4, 4)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::UnsignedValue(UnsignedValue::new(0, 0)),
                    Value::UnsignedValue(UnsignedValue::new(1, 10)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::UnsignedValue(UnsignedValue::new(2, 5))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 0, true);

        // Search for an entry that exists in the second file
        let values = c.read_unsigned_block().unwrap();

        let exp = vec![
            data[2].values[0].clone(),
            data[2].values[1].clone(),
            data[3].values[0].clone(),
            data[0].values[1].clone(),
        ];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.unsigned_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_unsigned_block().unwrap();

        let exp = vec![data[1].values[1].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.unsigned_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_unsigned_block().unwrap();

        assert_eq!(
            values.len(),
            0,
            "value length mismatch: got {}, exp {}",
            values.len(),
            0
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_asc_before_start_overlap_min_bool() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::BoolValue(BoolValue::new(1, true)),
                    Value::BoolValue(BoolValue::new(3, true)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::BoolValue(BoolValue::new(2, true)),
                    Value::BoolValue(BoolValue::new(4, true)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::BoolValue(BoolValue::new(0, true)),
                    Value::BoolValue(BoolValue::new(1, false)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::BoolValue(BoolValue::new(2, false))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 0, true);

        // Search for an entry that exists in the second file
        let values = c.read_bool_block().unwrap();

        let exp = vec![
            data[2].values[0].clone(),
            data[2].values[1].clone(),
            data[3].values[0].clone(),
            data[0].values[1].clone(),
        ];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.bool_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_bool_block().unwrap();

        let exp = vec![data[1].values[1].clone()];

        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.bool_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_bool_block().unwrap();

        assert_eq!(
            values.len(),
            0,
            "value length mismatch: got {}, exp {}",
            values.len(),
            0
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_asc_before_start_overlap_min_string() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::StringValue(StringValue::new(1, "1.0".to_string())),
                    Value::StringValue(StringValue::new(3, "3.0".to_string())),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::StringValue(StringValue::new(2, "2.0".to_string())),
                    Value::StringValue(StringValue::new(4, "4.0".to_string())),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::StringValue(StringValue::new(0, "0.0".to_string())),
                    Value::StringValue(StringValue::new(1, "1.1".to_string())),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::StringValue(StringValue::new(2, "2.2".to_string()))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 0, true);

        // Search for an entry that exists in the second file
        let values = c.read_string_block().unwrap();
        let exp = vec![
            data[2].values[0].clone(),
            data[2].values[1].clone(),
            data[3].values[0].clone(),
            data[0].values[1].clone(),
        ];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.string_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        // Check that calling Next will dedupe points
        c.next();
        let values = c.read_string_block().unwrap();
        let exp = vec![data[1].values[1].clone()];

        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.string_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_string_block().unwrap();

        assert_eq!(
            values.len(),
            0,
            "value length mismatch: got {}, exp {}",
            values.len(),
            0
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_asc_middle() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::FloatValue(FloatValue::new(1, 1.0)),
                    Value::FloatValue(FloatValue::new(2, 2.0)),
                    Value::FloatValue(FloatValue::new(3, 3.0)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(4, 4.0))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 3, true);

        // Search for an entry that exists in the second file
        let values = c.read_float_block().unwrap();

        let exp = vec![data[0].values[2].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.float_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_float_block().unwrap();

        let exp = vec![data[1].values[0].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.float_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_asc_end() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(1, 2.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(2, 3.0))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 2, true);

        // Search for an entry that exists in the second file
        let values = c.read_float_block().unwrap();

        let exp = data[2].clone();
        assert_eq!(
            values.len(),
            exp.values.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.values.len()
        );

        for (i, v) in exp.values.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.float_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_desc_from_start() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(1, 2.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(2, 3.0))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 0, false);

        // Search for an entry that exists in the second file
        let values = c.read_float_block().unwrap();

        let exp = data[0].clone();
        assert_eq!(
            values.len(),
            exp.values.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.values.len()
        );

        for (i, v) in exp.values.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.float_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_desc_duplicate() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(0, 4.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(2, 2.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(2, 3.0))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 2, false);

        // Search for an entry that exists in the second file
        let values = c.read_float_block().unwrap();

        let exp = vec![data[3].values[0].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.float_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_float_block().unwrap();

        let exp = vec![data[1].values[0].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.float_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_desc_overlap_max_float() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::FloatValue(FloatValue::new(1, 1.0)),
                    Value::FloatValue(FloatValue::new(3, 3.0)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::FloatValue(FloatValue::new(2, 2.0)),
                    Value::FloatValue(FloatValue::new(4, 4.0)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::FloatValue(FloatValue::new(0, 0.0)),
                    Value::FloatValue(FloatValue::new(1, 1.0)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(2, 2.2))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 5, false);

        // Search for an entry that exists in the second file
        let values = c.read_float_block().unwrap();

        let exp = vec![
            data[3].values[0].clone(),
            data[0].values[1].clone(),
            data[1].values[1].clone(),
        ];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.float_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_float_block().unwrap();

        let exp = vec![data[2].values[0].clone(), data[2].values[1].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.float_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_desc_overlap_max_integer() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::IntegerValue(IntegerValue::new(1, 1)),
                    Value::IntegerValue(IntegerValue::new(3, 3)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::IntegerValue(IntegerValue::new(2, 2)),
                    Value::IntegerValue(IntegerValue::new(4, 4)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::IntegerValue(IntegerValue::new(0, 0)),
                    Value::IntegerValue(IntegerValue::new(1, 10)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::IntegerValue(IntegerValue::new(2, 5))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 5, false);

        // Search for an entry that exists in the second file
        let values = c.read_integer_block().unwrap();

        let exp = vec![
            data[3].values[0].clone(),
            data[0].values[1].clone(),
            data[1].values[1].clone(),
        ];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.integer_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_integer_block().unwrap();

        let exp = vec![data[2].values[0].clone(), data[2].values[1].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.integer_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_desc_overlap_max_unsigned() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::UnsignedValue(UnsignedValue::new(1, 1)),
                    Value::UnsignedValue(UnsignedValue::new(3, 3)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::UnsignedValue(UnsignedValue::new(2, 2)),
                    Value::UnsignedValue(UnsignedValue::new(4, 4)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::UnsignedValue(UnsignedValue::new(0, 0)),
                    Value::UnsignedValue(UnsignedValue::new(1, 10)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::UnsignedValue(UnsignedValue::new(2, 5))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 5, false);

        // Search for an entry that exists in the second file
        let values = c.read_unsigned_block().unwrap();

        let exp = vec![
            data[3].values[0].clone(),
            data[0].values[1].clone(),
            data[1].values[1].clone(),
        ];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.unsigned_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_unsigned_block().unwrap();

        let exp = vec![data[2].values[0].clone(), data[2].values[1].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.unsigned_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_desc_overlap_max_bool() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::BoolValue(BoolValue::new(1, true)),
                    Value::BoolValue(BoolValue::new(3, true)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::BoolValue(BoolValue::new(2, true)),
                    Value::BoolValue(BoolValue::new(4, true)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::BoolValue(BoolValue::new(0, true)),
                    Value::BoolValue(BoolValue::new(1, false)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::BoolValue(BoolValue::new(2, false))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 5, false);

        // Search for an entry that exists in the second file
        let values = c.read_bool_block().unwrap();

        let exp = vec![
            data[3].values[0].clone(),
            data[0].values[1].clone(),
            data[1].values[1].clone(),
        ];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.bool_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_bool_block().unwrap();

        let exp = vec![data[2].values[0].clone(), data[2].values[1].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.bool_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_desc_overlap_max_string() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::StringValue(StringValue::new(1, "1.0".to_string())),
                    Value::StringValue(StringValue::new(3, "3.0".to_string())),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::StringValue(StringValue::new(2, "2.0".to_string())),
                    Value::StringValue(StringValue::new(4, "4.0".to_string())),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::StringValue(StringValue::new(0, "0.0".to_string())),
                    Value::StringValue(StringValue::new(1, "1.1".to_string())),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::StringValue(StringValue::new(2, "2.2".to_string()))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 5, false);

        // Search for an entry that exists in the second file
        let values = c.read_string_block().unwrap();

        let exp = vec![
            data[3].values[0].clone(),
            data[0].values[1].clone(),
            data[1].values[1].clone(),
        ];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.string_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_string_block().unwrap();

        let exp = vec![data[2].values[0].clone(), data[2].values[1].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.string_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_desc_after_end() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(1, 1.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(2, 2.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(3, 3.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(2, 2.2))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 4, false);

        // Search for an entry that exists in the second file
        let values = c.read_float_block().unwrap();

        let exp = data[2].clone();
        assert_eq!(
            values.len(),
            exp.values.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.values.len()
        );

        for (i, v) in exp.values.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.float_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_desc_after_end_overlap_float() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::FloatValue(FloatValue::new(8, 0.0)),
                    Value::FloatValue(FloatValue::new(9, 1.0)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(2, 2.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(3, 3.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::FloatValue(FloatValue::new(3, 4.0)),
                    Value::FloatValue(FloatValue::new(7, 7.0)),
                ],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 10, false);

        // Search for an entry that exists in the second file
        let values = c.read_float_block().unwrap();

        let exp = vec![data[0].values[0].clone(), data[0].values[1].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.float_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_float_block().unwrap();

        let exp = vec![data[3].values[0].clone(), data[3].values[1].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.float_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_float_block().unwrap();

        let exp = vec![data[1].values[0].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.float_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_float_block().unwrap();
        assert_eq!(
            values.len(),
            0,
            "value length mismatch: got {}, exp {}",
            values.len(),
            0
        );
        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_desc_after_end_overlap_integer() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::IntegerValue(IntegerValue::new(8, 0)),
                    Value::IntegerValue(IntegerValue::new(9, 1)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::IntegerValue(IntegerValue::new(2, 2))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::IntegerValue(IntegerValue::new(3, 3))],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::IntegerValue(IntegerValue::new(3, 4)),
                    Value::IntegerValue(IntegerValue::new(10, 7)),
                ],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 11, false);

        // Search for an entry that exists in the second file
        let values = c.read_integer_block().unwrap();

        let exp = vec![
            data[3].values[0].clone(),
            data[0].values[0].clone(),
            data[0].values[1].clone(),
            data[3].values[1].clone(),
        ];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.integer_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_integer_block().unwrap();

        let exp = vec![data[1].values[0].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.integer_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_integer_block().unwrap();
        assert_eq!(
            values.len(),
            0,
            "value length mismatch: got {}, exp {}",
            values.len(),
            0
        );
        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_desc_after_end_overlap_unsigned() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::UnsignedValue(UnsignedValue::new(8, 0)),
                    Value::UnsignedValue(UnsignedValue::new(9, 1)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::UnsignedValue(UnsignedValue::new(2, 2))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::UnsignedValue(UnsignedValue::new(3, 3))],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::UnsignedValue(UnsignedValue::new(3, 4)),
                    Value::UnsignedValue(UnsignedValue::new(10, 7)),
                ],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 11, false);

        // Search for an entry that exists in the second file
        let values = c.read_unsigned_block().unwrap();

        let exp = vec![
            data[3].values[0].clone(),
            data[0].values[0].clone(),
            data[0].values[1].clone(),
            data[3].values[1].clone(),
        ];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.unsigned_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_unsigned_block().unwrap();

        let exp = vec![data[1].values[0].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.unsigned_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_unsigned_block().unwrap();
        assert_eq!(
            values.len(),
            0,
            "value length mismatch: got {}, exp {}",
            values.len(),
            0
        );
        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_desc_after_end_overlap_bool() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::BoolValue(BoolValue::new(8, true)),
                    Value::BoolValue(BoolValue::new(9, true)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::BoolValue(BoolValue::new(2, true))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::BoolValue(BoolValue::new(3, false))],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::BoolValue(BoolValue::new(3, true)),
                    Value::BoolValue(BoolValue::new(7, false)),
                ],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 11, false);

        // Search for an entry that exists in the second file
        let values = c.read_bool_block().unwrap();

        let exp = vec![data[0].values[0].clone(), data[0].values[1].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.bool_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_bool_block().unwrap();

        let exp = vec![data[3].values[0].clone(), data[3].values[1].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.bool_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_bool_block().unwrap();

        let exp = vec![data[1].values[0].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.bool_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_bool_block().unwrap();
        assert_eq!(
            values.len(),
            0,
            "value length mismatch: got {}, exp {}",
            values.len(),
            0
        );
        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_desc_after_end_overlap_string() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::StringValue(StringValue::new(8, "eight".to_string())),
                    Value::StringValue(StringValue::new(9, "nine".to_string())),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::StringValue(StringValue::new(2, "two".to_string()))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::StringValue(StringValue::new(3, "three".to_string()))],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::StringValue(StringValue::new(3, "four".to_string())),
                    Value::StringValue(StringValue::new(7, "seven".to_string())),
                ],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 11, false);

        // Search for an entry that exists in the second file
        let values = c.read_string_block().unwrap();

        let exp = vec![data[0].values[0].clone(), data[0].values[1].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.string_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_string_block().unwrap();

        let exp = vec![data[3].values[0].clone(), data[3].values[1].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.string_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_string_block().unwrap();

        let exp = vec![data[1].values[0].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.string_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_string_block().unwrap();
        assert_eq!(
            values.len(),
            0,
            "value length mismatch: got {}, exp {}",
            values.len(),
            0
        );
        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_desc_middle() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(1, 1.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::FloatValue(FloatValue::new(2, 2.0)),
                    Value::FloatValue(FloatValue::new(3, 3.0)),
                    Value::FloatValue(FloatValue::new(4, 4.0)),
                ],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 3, false);

        // Search for an entry that exists in the second file
        let values = c.read_float_block().unwrap();

        let exp = vec![data[1].values[0].clone(), data[1].values[1].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.float_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_float_block().unwrap();

        let exp = vec![data[0].values[0].clone()];
        assert_eq!(
            values.len(),
            exp.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.len()
        );

        for (i, v) in exp.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.float_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        c.next();
        let values = c.read_float_block().unwrap();
        assert_eq!(
            values.len(),
            0,
            "value length mismatch: got {}, exp {}",
            values.len(),
            0
        );

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_seek_to_desc_end() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(1, 2.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(2, 3.0))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 2, false);

        // Search for an entry that exists in the second file
        let values = c.read_float_block().unwrap();

        let exp = &data[2];
        assert_eq!(
            values.len(),
            exp.values.len(),
            "value length mismatch: got {}, exp {}",
            values.len(),
            exp.values.len()
        );

        for (i, v) in exp.values.iter().enumerate() {
            let (got, exp) = (values[i].value(), v.float_value().unwrap());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_key_cursor_tombstone_range() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(1, 2.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(2, 3.0))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        fs.delete_range(vec!["cpu".as_bytes().to_vec()], 1, 1)
            .unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 0, true);

        let exp_values = vec![0, 2];
        for v in exp_values {
            let values = c.read_float_block().unwrap();

            let exp = &data[v];
            assert_eq!(
                values.len(),
                1,
                "value length mismatch: got {}, exp {}",
                values.len(),
                1
            );

            let (got, exp) = (values[0].to_string(), exp.values[0].to_string());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                0, got, exp
            );

            c.next();
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_key_cursor_tombstone_range_partial_first() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![
                    Value::FloatValue(FloatValue::new(0, 0.0)),
                    Value::FloatValue(FloatValue::new(1, 1.0)),
                ],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(2, 2.0))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice());

        // Delete part of the block in the first file.
        let r = Arc::new(must_open_tsm_reader(&files[0], 0, 0));
        TSMReader::delete_range(r, &["cpu".as_bytes()], 1, 3).unwrap();

        fs.replace(&[], &files).unwrap();

        fs.delete_range(vec!["cpu".as_bytes().to_vec()], 1, 1)
            .unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 0, true);

        let exp_values = vec![
            Value::FloatValue(FloatValue::new(0, 0.0)),
            Value::FloatValue(FloatValue::new(2, 2.0)),
        ];

        for exp in exp_values {
            let values = c.read_float_block().unwrap();

            assert_eq!(
                values.len(),
                1,
                "value length mismatch: got {}, exp {}",
                values.len(),
                1
            );

            let (got, exp) = (values[0].to_string(), exp.to_string());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                0, got, exp
            );

            c.next();
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_key_cursor_tombstone_range_partial_float() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![KeyValues {
            key: "cpu",
            values: vec![
                Value::FloatValue(FloatValue::new(0, 1.0)),
                Value::FloatValue(FloatValue::new(1, 2.0)),
                Value::FloatValue(FloatValue::new(2, 3.0)),
            ],
        }];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        fs.delete_range(vec!["cpu".as_bytes().to_vec()], 1, 1)
            .unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 0, true);

        let values = c.read_float_block().unwrap();

        let exp_values = vec![&data[0].values[0], &data[0].values[2]];

        for (i, exp) in exp_values.iter().enumerate() {
            assert_eq!(
                values.len(),
                2,
                "value length mismatch: got {}, exp {}",
                values.len(),
                2
            );

            let (got, exp) = (values[i].to_string(), exp.to_string());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_key_cursor_tombstone_range_partial_integer() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![KeyValues {
            key: "cpu",
            values: vec![
                Value::IntegerValue(IntegerValue::new(0, 1)),
                Value::IntegerValue(IntegerValue::new(1, 2)),
                Value::IntegerValue(IntegerValue::new(2, 3)),
            ],
        }];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        fs.delete_range(vec!["cpu".as_bytes().to_vec()], 1, 1)
            .unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 0, true);

        let values = c.read_integer_block().unwrap();

        let exp_values = vec![&data[0].values[0], &data[0].values[2]];

        for (i, exp) in exp_values.iter().enumerate() {
            assert_eq!(
                values.len(),
                2,
                "value length mismatch: got {}, exp {}",
                values.len(),
                2
            );

            let (got, exp) = (values[i].to_string(), exp.to_string());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_key_cursor_tombstone_range_partial_unsigned() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![KeyValues {
            key: "cpu",
            values: vec![
                Value::UnsignedValue(UnsignedValue::new(0, 1)),
                Value::UnsignedValue(UnsignedValue::new(1, 2)),
                Value::UnsignedValue(UnsignedValue::new(2, 3)),
            ],
        }];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        fs.delete_range(vec!["cpu".as_bytes().to_vec()], 1, 1)
            .unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 0, true);

        let values = c.read_unsigned_block().unwrap();

        let exp_values = vec![&data[0].values[0], &data[0].values[2]];

        for (i, exp) in exp_values.iter().enumerate() {
            assert_eq!(
                values.len(),
                2,
                "value length mismatch: got {}, exp {}",
                values.len(),
                2
            );

            let (got, exp) = (values[i].to_string(), exp.to_string());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_key_cursor_tombstone_range_partial_bool() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![KeyValues {
            key: "cpu",
            values: vec![
                Value::BoolValue(BoolValue::new(0, true)),
                Value::BoolValue(BoolValue::new(1, false)),
                Value::BoolValue(BoolValue::new(2, true)),
            ],
        }];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        fs.delete_range(vec!["cpu".as_bytes().to_vec()], 1, 1)
            .unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 0, true);

        let values = c.read_bool_block().unwrap();

        let exp_values = vec![&data[0].values[0], &data[0].values[2]];

        for (i, exp) in exp_values.iter().enumerate() {
            assert_eq!(
                values.len(),
                2,
                "value length mismatch: got {}, exp {}",
                values.len(),
                2
            );

            let (got, exp) = (values[i].to_string(), exp.to_string());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_key_cursor_tombstone_range_partial_string() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let mut fs = FileStore::new(Arc::new(fpath), None);

        // Setup 3 files
        let data = vec![KeyValues {
            key: "cpu",
            values: vec![
                Value::StringValue(StringValue::new(0, "1".to_string())),
                Value::StringValue(StringValue::new(1, "2".to_string())),
                Value::StringValue(StringValue::new(2, "3".to_string())),
            ],
        }];

        let files = new_files(dir.path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        fs.delete_range(vec!["cpu".as_bytes().to_vec()], 1, 1)
            .unwrap();

        let mut c = FileStore::key_cursor(Arc::new(fs), "cpu".as_bytes(), 0, true);

        let values = c.read_string_block().unwrap();

        let exp_values = vec![&data[0].values[0], &data[0].values[2]];

        for (i, exp) in exp_values.iter().enumerate() {
            assert_eq!(
                values.len(),
                2,
                "value length mismatch: got {}, exp {}",
                values.len(),
                2
            );

            let (got, exp) = (values[i].to_string(), exp.to_string());
            assert_eq!(
                got, exp,
                "read value mismatch({}): got {:?}, exp {:?}",
                i, got, exp
            );
        }

        std::fs::remove_dir_all(dir.path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_open() {
        let dir = must_temp_tsm_dir();

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(1, 2.0))],
            },
            KeyValues {
                key: "mem",
                values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice()); // create test files

        let dir = Arc::new(dir.into_path());
        let mut fs = FileStore::new(Arc::clone(&dir), None);
        fs.open().unwrap();

        assert_eq!(
            fs.count(),
            3,
            "file count  mismatch: got {}, exp {}",
            fs.count(),
            3
        );

        assert_eq!(
            fs.current_generation(),
            4,
            "current ID mismatch: got {}, exp {}",
            fs.current_generation(),
            4
        );

        drop(fs);

        std::fs::remove_dir_all(dir.as_path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_remove() {
        let dir = must_temp_tsm_dir();

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(1, 2.0))],
            },
            KeyValues {
                key: "mem",
                values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice()); // create test files

        let dir = Arc::new(dir.into_path());
        let mut fs = FileStore::new(Arc::clone(&dir), None);
        fs.open().unwrap();

        assert_eq!(
            fs.count(),
            3,
            "file count  mismatch: got {}, exp {}",
            fs.count(),
            3
        );

        assert_eq!(
            fs.current_generation(),
            4,
            "current ID mismatch: got {}, exp {}",
            fs.current_generation(),
            4
        );

        fs.replace(&files[2..3], &[]).unwrap();

        assert_eq!(
            fs.count(),
            2,
            "file count  mismatch: got {}, exp {}",
            fs.count(),
            2
        );

        assert_eq!(
            fs.current_generation(),
            4,
            "current ID mismatch: got {}, exp {}",
            fs.current_generation(),
            4
        );

        drop(fs);

        std::fs::remove_dir_all(dir.as_path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_replace() {
        let dir = must_temp_tsm_dir();

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(1, 2.0))],
            },
            KeyValues {
                key: "mem",
                values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice()); // create test files

        // Replace requires assumes new files have a .tmp extension
        let replacement = format!("{}.{}", &files[2], TMP_TSM_FILE_EXTENSION);
        fs::rename(&files[2], &replacement).unwrap();

        let dir = Arc::new(dir.into_path());
        let fs = Arc::new(FileStore::new(Arc::clone(&dir), None));
        fs.open().unwrap();

        assert_eq!(
            fs.count(),
            2,
            "file count  mismatch: got {}, exp {}",
            fs.count(),
            2
        );

        // Should record references to the two existing TSM files
        let mut c = FileStore::key_cursor(Arc::clone(&fs), "cpu".as_bytes(), 0, true);

        // Should move the existing files out of the way, but allow query to complete
        fs.replace(&files[..2], &[replacement]).unwrap();

        assert_eq!(
            fs.count(),
            1,
            "file count  mismatch: got {}, exp {}",
            fs.count(),
            1
        );

        // There should be two blocks (1 in each file)
        c.next();
        let values = c.read_float_block().unwrap();
        assert_eq!(
            values.len(),
            1,
            "value length mismatch: got {}, exp {}",
            values.len(),
            1
        );

        c.next();
        let values = c.read_float_block().unwrap();
        assert_eq!(
            values.len(),
            1,
            "value length mismatch: got {}, exp {}",
            values.len(),
            1
        );

        // No more blocks for this cursor
        let values = c.read_float_block().unwrap();
        assert_eq!(
            values.len(),
            0,
            "value length mismatch: got {}, exp {}",
            values.len(),
            0
        );

        // Release the references (files should get evicted by purger shortly)
        c.close();

        std::thread::sleep(Duration::from_secs(1));
        // Make sure the two TSM files used by the cursor are gone
        assert!(
            !Path::new(&files[0]).exists(),
            "file {:?} should have been removed",
            &files[0]
        );
        assert!(
            !Path::new(&files[1]).exists(),
            "file {:?} should have been removed",
            &files[1]
        );

        // Make sure the new file exists
        assert!(
            Path::new(&files[2]).exists(),
            "file {:?} should be exist",
            &files[2]
        );

        std::fs::remove_dir_all(dir.as_path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_open_delete() {
        let dir = must_temp_tsm_dir();

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu,host=server2!~#!value",
                values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
            },
            KeyValues {
                key: "cpu,host=server1!~#!value",
                values: vec![Value::FloatValue(FloatValue::new(1, 2.0))],
            },
            KeyValues {
                key: "mem,host=server1!~#!value",
                values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
            },
        ];

        let files = new_files(dir.path(), data.as_slice()); // create test files

        let dir = Arc::new(dir.into_path());
        let fs: Arc<FileStore> = Arc::new(FileStore::new(Arc::clone(&dir), None));
        fs.open().unwrap();

        assert_eq!(
            fs.keys().len(),
            3,
            "keys count  mismatch: got {}, exp {}",
            fs.keys().len(),
            3
        );

        fs.delete(vec!["cpu,host=server2!~#!value".as_bytes().to_vec()])
            .unwrap();

        let fs2: Arc<FileStore> = Arc::new(FileStore::new(Arc::clone(&dir), None));
        fs2.open().unwrap();

        assert_eq!(
            fs2.keys().len(),
            2,
            "keys count  mismatch: got {}, exp {}",
            fs2.keys().len(),
            2
        );

        std::fs::remove_dir_all(dir.as_path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_delete() {
        let dir = must_temp_tsm_dir();

        let dir = Arc::new(dir.into_path());
        let fs: Arc<FileStore> = Arc::new(FileStore::new(Arc::clone(&dir), None));

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu,host=server2!~#!value",
                values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
            },
            KeyValues {
                key: "cpu,host=server1!~#!value",
                values: vec![Value::FloatValue(FloatValue::new(1, 2.0))],
            },
            KeyValues {
                key: "mem,host=server1!~#!value",
                values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
            },
        ];

        let files = new_files(dir.as_path(), data.as_slice()); // create test files

        fs.replace(&[], &files).unwrap();

        let keys = fs.keys();
        assert_eq!(
            keys.len(),
            3,
            "keys count  mismatch: got {}, exp {}",
            fs.keys().len(),
            3
        );

        fs.delete(vec!["cpu,host=server2!~#!value".as_bytes().to_vec()])
            .unwrap();

        assert_eq!(
            fs.keys().len(),
            2,
            "keys count  mismatch: got {}, exp {}",
            fs.keys().len(),
            2
        );

        std::fs::remove_dir_all(dir.as_path()).unwrap();
    }

    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_apply() {
        let dir = must_temp_tsm_dir();

        let dir = Arc::new(dir.into_path());
        let fs: Arc<FileStore> = Arc::new(FileStore::new(Arc::clone(&dir), None));

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu,host=server2!~#!value",
                values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
            },
            KeyValues {
                key: "cpu,host=server1!~#!value",
                values: vec![Value::FloatValue(FloatValue::new(1, 2.0))],
            },
            KeyValues {
                key: "mem,host=server1!~#!value",
                values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
            },
        ];

        let files = new_files(dir.as_path(), data.as_slice()); // create test files

        fs.replace(&[], &files).unwrap();

        let keys = fs.keys();
        assert_eq!(
            keys.len(),
            3,
            "keys count  mismatch: got {}, exp {}",
            fs.keys().len(),
            3
        );

        let n = Arc::new(AtomicI64::new(0));
        let nn = Arc::clone(&n);
        fs.apply(move |_r: Arc<TSMReader>| {
            nn.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();

        assert_eq!(
            n.load(Ordering::SeqCst),
            3,
            "apply mismatch: got {}, exp {}",
            n.load(Ordering::SeqCst),
            3
        );

        std::fs::remove_dir_all(dir.as_path()).unwrap();
    }

    // test_file_store_stats has been tested ok !
    // #[test]
    #[cfg(feature = "mock_file_store")]
    // fn test_file_store_stats() {
    //     let dir = must_temp_tsm_dir();

    //     // Setup 3 files
    //     let data = vec![
    //         KeyValues {
    //             key: "cpu,host=server2!~#!value",
    //             values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
    //         },
    //         KeyValues {
    //             key: "cpu,host=server1!~#!value",
    //             values: vec![Value::FloatValue(FloatValue::new(1, 2.0))],
    //         },
    //         KeyValues {
    //             key: "mem,host=server1!~#!value",
    //             values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
    //         },
    //     ];

    //     let files = new_files(dir.path(), data.as_slice()); // create test files

    //     let fs: Arc<FileStore> = Arc::new(FileStore::new(dir.path().to_str().unwrap().to_string()));
    //     fs.open().unwrap();

    //     let stats = fs.stats();

    //     assert_eq!(
    //         stats.len(),
    //         3,
    //         "file count mismatch: got {}, exp {}",
    //         stats.len(),
    //         3
    //     );

    //     // Another call should result in the same stats being returned.
    //     let stats2 = fs.stats();
    //     assert_eq!(stats, stats2, "got {:?}, exp {:?}", stats, stats2);

    //     // Removing one of the files should invalidate the cache.
    //     fs.replace(files[0..1].to_vec(), vec![]).unwrap();
    //     assert_eq!(
    //         fs.stats().len(),
    //         2,
    //         "file count mismatch: got {}, exp {}",
    //         fs.stats().len(),
    //         2
    //     );

    //     // Write a new TSM file that that is not open
    //     let (dir1, new_file_name) = must_write_tsm(
    //         dir.path(),
    //         4,
    //         HashMap::from([("mem".to_string(), vec![Value::FloatValue(FloatValue::new(
    //             0, 1.0,
    //         ))])]),
    //     );

    //     // Assumes new files have a .tmp extension
    //     let replacement = format!(
    //         "{}.{}.{}",
    //         &files[2], TMP_TSM_FILE_EXTENSION, TSM_FILE_EXTENSION
    //     );
    //     fs::rename(new_file_name, &replacement).unwrap();
    //     // Replace 3 w/ 1
    //     fs.replace(files, vec![replacement]).unwrap();

    //     let mut found = false;
    //     let stats = fs.stats();
    //     let suffix = format!(
    //         "{}.{}.{}",
    //         TSM_FILE_EXTENSION, TMP_TSM_FILE_EXTENSION, TSM_FILE_EXTENSION
    //     );
    //     for stat in stats.iter() {
    //         if stat.path.to_str().unwrap().ends_with(&suffix) {
    //             found = true;
    //         }
    //     }

    //     if !found {
    //         panic!("Didn't find {} in stats: {:?}", "foo", stats);
    //     }

    //     let (dir2, new_file_name) = must_write_tsm(
    //         dir.path(),
    //         5,
    //         HashMap::from([("mem".to_string(), vec![Value::FloatValue(FloatValue::new(
    //             0, 1.0,
    //         ))])]),
    //     );

    //     // Adding some files should invalidate the cache.
    //     fs.replace(vec![], vec![new_file_name]).unwrap();
    //     assert_eq!(
    //         fs.stats().len(),
    //         2,
    //         "file count mismatch: got {}, exp {}",
    //         fs.stats().len(),
    //         2
    //     );

    //     std::fs::remove_dir_all(dir.path()).unwrap();
    // }
    #[test]
    #[cfg(feature = "mock_file_store")]
    fn test_file_store_create_snapshot() {
        let dir = must_temp_tsm_dir();
        let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

        let dir = Arc::new(dir.into_path());
        let fs = FileStore::new(Arc::clone(&dir), None);

        // Setup 3 files
        let data = vec![
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(1, 2.0))],
            },
            KeyValues {
                key: "cpu",
                values: vec![Value::FloatValue(FloatValue::new(2, 3.0))],
            },
        ];

        let files = new_files(dir.as_path(), data.as_slice());

        fs.replace(&[], &files).unwrap();

        fs.delete_range(vec!["cpu".as_bytes().to_vec()], 1, 1)
            .unwrap();

        let s = fs.create_snapshot().unwrap();

        let tfs = fs::read_dir(&s)
            .unwrap()
            .map(|f| {
                let f = f.unwrap();
                f.path()
            })
            .collect::<Vec<PathBuf>>();

        assert!(!tfs.is_empty(), "no files found");

        for f in fs.files() {
            let p = s.join(f.path().file_name().unwrap());
            match fs::metadata(&p) {
                Ok(metadata) => {
                    // Check the link count
                    let link_count = metadata.nlink();

                    // If link count > 1, it's a hard link
                    if link_count <= 1 {
                        panic!("unable to find file: {:?}", &p);
                    }
                }
                Err(e) => {
                    panic!("unable to get file meta: {:?}", &p);
                }
            }

            let ts = f.tombstone_stats();
            if ts.tombstone_exists {
                let p = s.join(ts.path.file_name().unwrap());
                match fs::metadata(&p) {
                    Ok(metadata) => {
                        // Check the link count
                        let link_count = metadata.nlink();

                        // If link count > 1, it's a hard link
                        if link_count <= 1 {
                            panic!("unable to find file: {:?}", &p);
                        }
                    }
                    Err(e) => {
                        panic!("unable to get file meta: {:?}", &p);
                    }
                }
            }
        }

        std::fs::remove_dir_all(dir.as_path()).unwrap();
    }

    #[cfg(test)]
    #[ctor::ctor]
    fn init_runtime() {
        let runtime_opt = crate::runtime::global::RuntimeOptions::default();
        crate::runtime::global::init_global_runtimes(&runtime_opt);
    }

    // FIXME: cannot return
    // #[test]
    // #[bench]
    // fn bench_file_store_stats(b: &mut Bencher) {
    //     let dir = must_temp_tsm_dir();
    //     let (f, fpath) = must_temp_tsm_file(dir.path()).keep().unwrap();

    //     // Create some TSM files...
    //     let mut data = vec![];
    //     for i in 0..1000 {
    //         data.push(KeyValues {
    //             key: "cpu",
    //             values: vec![Value::FloatValue(FloatValue::new(0, 1.0))],
    //         });
    //     }

    //     let files = new_files(dir.path(), data.as_slice());

    //     let fs = FileStore::new(dir.path().to_str().unwrap().to_string());
    //     fs.open().unwrap();

    //     b.iter(|| {
    //         let _ = fs.stats();
    //     });

    //     std::fs::remove_dir_all(dir.path()).unwrap();
    // }

    pub fn new_files(dir: &Path, values: &[KeyValues]) -> Vec<String> {
        let mut files = vec![];

        let mut id = 1;
        for v in values {
            let (f, fpath) = must_temp_tsm_file(dir).keep().unwrap();
            let mut w = TSMWriter::new(f, 1, 1, "");

            w.write(v.key.as_bytes(), &v.values).unwrap();

            w.write_index().unwrap();

            w.close().unwrap();

            let new_name = dir.join(default_format_file_name(id, 1) + ".tsm");

            fs::rename(fpath, &new_name).unwrap();

            id += 1;

            files.push(new_name.to_str().unwrap().to_string());
        }

        files
    }

    pub fn new_file_dir(dir: &Path, values: &[KeyValues]) -> Vec<String> {
        let mut files = vec![];

        let mut id = 1;
        for v in values {
            let (f, fpath) = must_temp_tsm_file(dir).keep().unwrap();
            let mut w = TSMWriter::new(f, 1, 1, "");

            w.write(v.key.as_bytes(), &v.values).unwrap();

            w.write_index().unwrap();

            w.close().unwrap();

            let new_name = dir.join(default_format_file_name(id, 1) + ".tsm");

            fs::rename(fpath, &new_name).unwrap();

            id += 1;

            files.push(new_name.to_str().unwrap().to_string());
        }

        files
    }

    #[derive(Clone)]
    pub struct KeyValues {
        pub key: &'static str,
        pub values: Vec<Value>,
    }
}
