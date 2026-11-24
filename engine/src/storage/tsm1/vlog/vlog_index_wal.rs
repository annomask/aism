//! Write ahead log.
#[cfg(not(target_os = "windows"))]
use std::os::unix::fs::MetadataExt;
#[cfg(target_os = "windows")]
use std::os::windows::fs::MetadataExt;
use std::{
    any::Any,
    collections::{HashMap, binary_heap},
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Once, Weak,
        atomic::{AtomicI32, AtomicI64, Ordering},
    },
    time::Duration,
};

use crate::common::{time::Time, to_str};
use crate::models::points::*;
use crate::runtime::execution::{
    ExecutorRef, RuntimeExecutorExt, SpawnError, TaskHandle, accepted_task_guard,
    legacy_global_executor,
};
use bytes::{Buf, BufMut};
use chrono::{DateTime, TimeZone, Utc};
use crossbeam_channel::{Receiver as CrossbeamReceiver, Sender as CrossbeamSender, select, tick};
use parking_lot::{Mutex, RwLock};
use snafu::{ResultExt, Snafu, ensure};
use tokio::sync::{
    mpsc,
    oneshot::{self, Receiver as OneshotReceiver, Sender as OneshotSender},
};
use tokio_util::sync::CancellationToken;

use crate::storage::tsm1::vlog::VLogIndex;

// Duration to buffer writes before flushing them to the wal.
const BUFFER_FLUSH_INTERVAL: Duration = Duration::from_millis(10);
// The maximum number of buffered writes that can be queued up before flushing to wal.
const BUFFER_CHANNEL_LIMIT: usize = 10_1000;
// Duration to wait before fsyncing writes. A value of 0 (default) will cause every write to be fsync'd,
const SYNC_DELAY: Duration = Duration::ZERO;

/// 10MB is the size at which segment files will be rolled over.
pub const DEFAULT_SEGMENT_SIZE: i64 = 10 * 1024 * 1024;
/// the file extension we expect for wal segments.
const WAL_FILE_EXTENSION: &str = "wal";

/// WAL_FILE_PREFIX is the prefix on all wal segment files.
const WAL_FILE_PREFIX: &str = "_";

/// the size of the wal entry encoding buffer
/// pub(crate) const WAL_ENCODE_BUF_SIZE: i32 = 4 * 1024 * 1024;

const FLOAT64_ENTRY_TYPE: u8 = 0;
const INTEGER_ENTRY_TYPE: u8 = 1;
const UNSIGNED_ENTRY_TYPE: u8 = 2;
const STRING_ENTRY_TYPE: u8 = 3;
const BOOL_ENTRY_TYPE: u8 = 4;
const VLOG_INDEX_COMPACTION: u8 = 5;
const VLOG_INDEX_AGGREGATE: u8 = 6;

// WalEntryType is a u8 written to a wal segment file that indicates what the following compressed block contains.
pub type WalEntryType = u8;

// WRITE_WAL_ENTRY_TYPE indicates a write entry.
pub const WRITE_WAL_ENTRY_TYPE: WalEntryType = 0x01;
// DELETE_WAL_ENTRY_TYPE indicates a delete entry.
pub const DELETE_WAL_ENTRY_TYPE: WalEntryType = 0x02;
// DELETE_RANGE_WAL_ENTRY_TYPE indicates a delete range entry.
pub const DELETE_RANGE_WAL_ENTRY_TYPE: WalEntryType = 0x03;
// DELETE_MULTI_WAL_ENTRY_TYPE indicates a multi delete entry.
pub const DELETE_MULTI_WAL_ENTRY_TYPE: WalEntryType = 0x04;
// REPLACE_MULTI_WAL_ENTRY_TYPE indicates an atomic multi-index replacement entry.
pub const REPLACE_MULTI_WAL_ENTRY_TYPE: WalEntryType = 0x05;

const REPLACE_MULTI_WAL_ENTRY_VERSION: u8 = 1;
const REPLACE_MULTI_WAL_ENTRY_DEFAULT_MODE: u8 = 0;

// Statistics gathered by the WAL.
const STAT_WAL_OLD_BYTES: &str = "old_segments_disk_bytes";
const STAT_WAL_CURRENT_BYTES: &str = "current_segments_disk_bytes";
const STAT_WRITE_OK: &str = "write_ok";
const STAT_WRITE_ERR: &str = "write_err";

/// Error type for WAL.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    /// IO error.
    #[snafu(display("io error: {}", source))]
    IO { source: io::Error },

    /// WALClosed is returned when attempting to write to a closed WAL file.
    #[snafu(display("WAL closed"))]
    WALClosed,

    /// Invalid file name.
    #[snafu(display("Invalid file name: {:?}", reason))]
    InvalidFileName { reason: String },

    /// Open new wal segment file failed.
    #[snafu(display("Open WAL segment file failed: {:?}", reason))]
    OpenWALSegmentFileFailed { reason: String },

    /// ErrWALCorrupt is returned when reading a corrupt WAL entry.
    #[snafu(display("WAL entry corrupted"))]
    WALCorrupted,

    /// Incorrect value found.
    #[snafu(display("{}", reason))]
    IncorrectValue { reason: String },

    /// Unsupported value.
    #[snafu(display("{:?}", reason))]
    UnsupportedValue { reason: String },

    /// Snappy compress error.
    #[snafu(display("snap compress failed: {:?}", source))]
    SnapCompress { source: snap::Error },

    /// fsync error.
    #[snafu(display("snap compress error: {}", reason))]
    FsyncFailed { reason: String },

    /// buffered write error.
    #[snafu(display("buffered write failed: {}", reason))]
    BufferedWriteFailed { reason: String },

    /// string write error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },

    /// The host executor refused to start the index WAL worker.
    #[snafu(display("index WAL execution error: {}", source))]
    Execution { source: SpawnError },
}

impl From<io::Error> for Error {
    fn from(source: io::Error) -> Error {
        Error::IO { source }
    }
}

/// Result type for WAL.
pub type Result<T, E = Error> = std::result::Result<T, E>;

struct WriteOp {
    pub entry: Box<dyn WalEntry + Send>,
    pub resp_tx: OneshotSender<Option<String>>,
}

/// WAL represents the write-ahead log used for writing TSM(time series merge tree) files.
/// The wal IO is done in a native thread rather than a tokio task to avoid blocking the tokio runtime.
/// As referenced in this post, continuous long-running IO threads should be off the
/// tokio runtime: `<https://ryhl.io/blog/async-what-is-blocking/>`.
pub struct WAL {
    path: Arc<PathBuf>,

    executor: ExecutorRef,
    task_handle: Mutex<Option<TaskHandle<()>>>,
    close_once: Once,
    // io_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    buffer_tx: mpsc::Sender<WriteOp>,
    closing: CancellationToken,

    // buffer_flush_interval: Duration,
    current_segment_writer: Arc<RwLock<WalSegmentWriter>>,

    // Duration to wait before fsyncing writes. A value of 0 (default) will cause every write to be fsync'd,
    // This must be set before the WAL is opened if a non-default value is required.
    sync_delay: Duration,
    sync_count: Arc<AtomicI32>,
    sync_waiters: Arc<(
        CrossbeamSender<OneshotSender<Option<String>>>,
        CrossbeamReceiver<OneshotSender<Option<String>>>,
    )>,

    // file size at which a segment file will be rotated
    pub segment_size: i64,

    // statistics for WAL
    stats: Arc<WalStatistics>, // atomic variable
}

impl WAL {
    async fn submit_write_op(
        &self,
        write_op: WriteOp,
        resp_rx: OneshotReceiver<Option<String>>,
    ) -> Result<()> {
        if self.buffer_tx.send(write_op).await.is_err() {
            self.stats.write_err.fetch_add(1, Ordering::SeqCst);
            return Err(Error::WALClosed);
        }

        let response = match resp_rx.await {
            Ok(response) => response,
            Err(_) => {
                self.stats.write_err.fetch_add(1, Ordering::SeqCst);
                return Err(Error::WALClosed);
            }
        };
        match response {
            None => {
                self.stats.write_ok.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            Some(reason) => {
                self.stats.write_err.fetch_add(1, Ordering::SeqCst);
                Err(Error::StringErr { reason })
            }
        }
    }

    /// open WAL or new initializes a new WAL at the given directory, WAL::new opens
    /// and initializes the log, also open can recover from previous unclosed shutdowns.
    pub fn new(
        path: Arc<PathBuf>,
        segment_size: i64,
        // mut buffer_flush_interval: Duration,
        sync_delay: Duration,
    ) -> Result<Self> {
        Self::new_with_executor(path, segment_size, sync_delay, legacy_global_executor())
    }

    pub fn new_with_executor(
        path: Arc<PathBuf>,
        segment_size: i64,
        // mut buffer_flush_interval: Duration,
        mut sync_delay: Duration,
        executor: ExecutorRef,
    ) -> Result<Self> {
        fs::create_dir_all(&*path)?;
        // ensure the directory creation is actually fsync'd so that when we create files there
        // we don't lose them (see: https://www.usenix.org/system/files/conference/osdi14/osdi14-paper-pillai.pdf)
        File::open(&*path)
            .expect("should be able to open just-created wal directory")
            .sync_all()
            .expect("fsync failure");

        let segments = segment_file_names(&path)?;
        let stats = WalStatistics::default();
        let sync_waiters = Arc::new(crossbeam_channel::bounded(BUFFER_CHANNEL_LIMIT));
        // create current segment writer from last segment file
        let mut current_segment_writer;
        if !segments.is_empty() {
            let last_segment = &segments[segments.len() - 1];
            let id = id_from_file_name(last_segment)?;
            let stat = fs::metadata(last_segment)?;
            // if the last segment file is zero size, remove it
            if stat.size() == 0 {
                fs::remove_file(last_segment)?;
                // segments.remove(segments.len() - 1);
            }

            // open last_segment file, and seek to END
            let mut fd = OpenOptions::new()
                .create(true)
                .append(true)
                .open(last_segment)?;

            fd.seek(SeekFrom::End(0))?;

            current_segment_writer = new_segment_writer(
                &path,
                BufWriter::new(fd),
                id,
                segment_size,
                Arc::clone(&sync_waiters),
                stat.size(),
            );
            stats
                .current_bytes
                .store(stat.size() as i64, Ordering::SeqCst); // TODO: 0 or stat.size() ?
        } else {
            // the first write request arrives after open a new WAL.
            let segment_id = 1;

            stats.old_bytes.fetch_add(0, Ordering::SeqCst);

            // create new writer
            // self.writer = new_log_file(&self.path, self.current_segment_id)?;
            // self.pos = 0;
            let file_name = path.join(format!(
                "{}{:05}.{}",
                WAL_FILE_PREFIX, segment_id, WAL_FILE_EXTENSION
            ));
            let fd = OpenOptions::new()
                .create(true)
                .append(true)
                .open(file_name)?;

            current_segment_writer = WalSegmentWriter::new(
                Arc::clone(&path),
                segment_size,
                segment_id,
                BufWriter::new(fd),
                Arc::clone(&sync_waiters),
                0, // the new writer's current pos is 0
            );

            stats.current_bytes.store(0, Ordering::SeqCst);
        }

        let mut total_old_disk_size: i64 = 0;
        // update last write time from segments
        let mut last_write_time = Time::from_timestamp(0, 0);
        for segment in &segments {
            let stat = fs::metadata(segment)?;
            if stat.size() > 0 {
                total_old_disk_size += stat.size() as i64;
                if stat.mtime_nsec() > last_write_time.timestamp_nanos() {
                    last_write_time = Time::from_timestamp_nanos(stat.mtime_nsec());
                }
            }
        }
        current_segment_writer.last_write_time = last_write_time;
        total_old_disk_size -= current_segment_writer.current_segment_size();
        stats.old_bytes.store(total_old_disk_size, Ordering::SeqCst);

        if sync_delay < Duration::ZERO {
            sync_delay = Duration::ZERO;
        }
        // if buffer_flush_interval < Duration::ZERO {
        //     buffer_flush_interval = BUFFER_FLUSH_INTERVAL;
        // }
        let (buffer_tx, buffer_rx) = mpsc::channel(BUFFER_CHANNEL_LIMIT);
        // let (io_flush_tx, io_flush_rx) = crossbeam_channel::bounded(1);
        let (io_flush_notify_tx, io_flush_notify_rx) = crossbeam_channel::bounded(1);
        let wal = WAL {
            path,

            executor: Arc::clone(&executor),
            task_handle: Default::default(),
            close_once: Once::new(),
            // io_handle: Default::default(),
            buffer_tx,
            closing: CancellationToken::new(),

            // buffer_flush_interval,
            current_segment_writer: Arc::new(RwLock::new(current_segment_writer)),
            sync_delay,
            sync_count: Arc::new(AtomicI32::new(0)),
            sync_waiters,
            segment_size,

            stats: Arc::new(stats),
        };

        let segment_writer = Arc::clone(&wal.current_segment_writer);
        let shutdown = wal.closing.clone();
        let sync_count = Arc::clone(&wal.sync_count);
        let sync_waiters = Arc::clone(&wal.sync_waiters);
        let stats = Arc::clone(&wal.stats);
        let worker_executor = Arc::downgrade(&executor);
        let task_handle = executor
            .spawn(async move {
                run_wal_op_buffer_task(
                    segment_writer,
                    buffer_rx,
                    // Arc::new(io_flush_tx),
                    Arc::new(io_flush_notify_rx),
                    Arc::new(io_flush_notify_tx),
                    // wal.buffer_flush_interval,
                    wal.sync_delay,
                    sync_count,
                    sync_waiters,
                    stats,
                    shutdown,
                    worker_executor,
                )
                .await;
            })
            .map_err(|source| Error::Execution { source })?;
        *wal.task_handle.lock() = Some(task_handle);

        // let sync_waiters = Arc::clone(&wal.sync_waiters);
        // let segment_writer = Arc::clone(&wal.current_segment_writer);
        // let stats = Arc::clone(&wal.stats);
        // *wal.io_handle.lock() = Some(
        //     std::thread::Builder::new()
        //         .name("wal io thread".to_string())
        //         .spawn(move || {
        //             run_wal_io_flush_task(
        //                 segment_writer,
        //                 sync_waiters,
        //                 io_flush_rx,
        //                 io_flush_notify_tx,
        //                 stats,
        //             );
        //         })
        //         .expect("failed to spawn wal write thread"),
        // );

        Ok(wal)
    }

    /// path returns the directory the log was initialized with.
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    /// last_write_time is the last tme anything was written to the WAL.
    pub fn last_write_time(&self) -> Time {
        self.current_segment_writer.read().last_write_time
    }

    /// Returns the size in bytes of all WAL segments on disk.
    pub fn disk_size_bytes(&self) -> i64 {
        self.stats.old_bytes.load(Ordering::SeqCst)
            + self.stats.current_bytes.load(Ordering::SeqCst)
    }

    /// Returns  statistics for periodic monitoring
    pub fn statistics(&self, tags: HashMap<String, String>) -> crate::models::statistic::Statistic {
        crate::models::statistic::Statistic::new(
            "tsm1_wal".to_string(),
            tags,
            HashMap::from([
                (
                    STAT_WAL_OLD_BYTES.to_string(),
                    self.stats.old_bytes.load(Ordering::SeqCst),
                ),
                (
                    STAT_WAL_CURRENT_BYTES.to_string(),
                    self.stats.current_bytes.load(Ordering::SeqCst),
                ),
                (
                    STAT_WRITE_OK.to_string(),
                    self.stats.write_ok.load(Ordering::SeqCst),
                ),
                (
                    STAT_WRITE_ERR.to_string(),
                    self.stats.write_err.load(Ordering::SeqCst),
                ),
            ]),
        )
    }

    /// Writes the given values to the WAL. It returns the WAL segment ID to which the points were written.
    /// If an error is returned the segment ID should be ignored.
    pub async fn write_multi(&self, values: HashMap<String, Vec<VLogIndex>>) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let write_op = WriteOp {
            entry: Box::new(WriteWalEntry {
                values,
                size: 0, // size will be updated in marshal_size
            }),
            resp_tx,
        };

        self.submit_write_op(write_op, resp_rx).await
    }

    pub async fn delete(&self, keys: Vec<Vec<u8>>) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        let (resp_tx, resp_rx) = oneshot::channel();
        let write_op = WriteOp {
            entry: Box::new(DeleteWalEntry { keys, size: 0 }),
            resp_tx,
        };

        self.submit_write_op(write_op, resp_rx).await
    }

    pub async fn delete_multi(&self, values: HashMap<String, Vec<VLogIndex>>) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let write_op = WriteOp {
            entry: Box::new(DeleteMultiWalEntry {
                values,
                size: 0, // size will be updated in marshal_size
            }),
            resp_tx,
        };

        self.submit_write_op(write_op, resp_rx).await
    }

    pub async fn replace_multi(
        &self,
        old_indexes: HashMap<String, Vec<VLogIndex>>,
        new_indexes: HashMap<String, Vec<VLogIndex>>,
    ) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let write_op = WriteOp {
            entry: Box::new(ReplaceMultiWalEntry {
                old_indexes,
                new_indexes,
                version: REPLACE_MULTI_WAL_ENTRY_VERSION,
                mode: REPLACE_MULTI_WAL_ENTRY_DEFAULT_MODE,
                size: 0, // size will be updated in marshal_size
            }),
            resp_tx,
        };

        self.submit_write_op(write_op, resp_rx).await
    }

    /// Deletes the given keys within the given time range.
    pub async fn delete_range(&self, keys: Vec<Vec<u8>>, min: i64, max: i64) -> Result<()> {
        todo!("implement me");
    }

    /// Closes the current segment if it is non-empty and opens a new one.
    /// this method will be called when engine write snapshot.
    pub fn close_segment(&self) -> Result<()> {
        let mut guard_writer = self.current_segment_writer.write();
        if guard_writer.current_segment_size() > 0 {
            // if current segment writer is exists, create a new writer with current_segment_id+=1,
            // so all the files whose id<current_segment_id are regarede as closed segment files.
            // before create new segment writer, we must sync current segment file first.
            guard_writer.new_segment_file(Arc::clone(&self.stats))?;
        }

        Ok(())
    }

    /// Returns a slice of the names of the closed segment files.
    pub fn closed_segments(&self) -> Result<Vec<String>> {
        let guard_writer = self.current_segment_writer.read();

        if self.path.as_path().eq(Path::new("")) {
            return Ok(vec![]);
        }

        let current_file = guard_writer.current_segment_writer_path();

        let mut closed_files = Vec::<String>::new();
        let segments = segment_file_names(&self.path)?;
        for file in segments.iter() {
            // skip the current file
            if *file == current_file {
                continue;
            }
            closed_files.push(file.to_owned());
        }

        Ok(closed_files)
    }

    /// Deletes the given segment file paths from disk and cleans up any associated objects.
    pub fn remove(&self, files: &[String]) -> Result<()> {
        // TODO: check if segement writer is none first ?
        let guard_writer = self.current_segment_writer.write();
        for file in files {
            fs::remove_file(file)?;
        }

        // refresh the on-disk size stats
        let segments = segment_file_names(&self.path)?;
        let mut total_old_disk_size: i64 = 0;
        for segment in &segments {
            let stat = fs::metadata(segment)?;
            if stat.size() > 0 {
                total_old_disk_size += stat.size() as i64;
            }
        }

        total_old_disk_size -= guard_writer.current_segment_size();
        self.stats
            .old_bytes
            .store(total_old_disk_size, Ordering::SeqCst);

        Ok(())
    }

    /// close will finish any flush that is currently in progress and close file handles.
    pub fn close(&self) {
        self.close_once.call_once(|| {
            self.closing.cancel();

            if self.current_segment_writer.write().current_segment_size() > 0 {
                let mut guard_writer = self.current_segment_writer.write();
                guard_writer.sync();

                // if l.currentSegmentWriter != nil {
                //     l.sync()
                //     l.currentSegmentWriter.close()
                //     l.currentSegmentWriter = nil
                // }
                // FIXME:
                // self.current_segment_writer = None;
            }
        });
    }

    #[cfg(test)]
    fn sync(&self) {
        let mut w = self.current_segment_writer.write();
        w.sync();
    }

    #[cfg(test)]
    fn flush(&self) {
        let mut w = self.current_segment_writer.write();
        w.flush();
    }
}
pub fn add_resp_tx(
    resp_tx: OneshotSender<Option<String>>,
    sync_waiters: &Arc<(
        CrossbeamSender<OneshotSender<Option<String>>>,
        CrossbeamReceiver<OneshotSender<Option<String>>>,
    )>,
) {
    sync_waiters
        .0
        .send(resp_tx)
        .expect("sync_waiters channel is closed");
}

fn run_wal_io_flush_task(
    segment_writer: Arc<RwLock<WalSegmentWriter>>,
    sync_waiters: Arc<(
        CrossbeamSender<OneshotSender<Option<String>>>,
        CrossbeamReceiver<OneshotSender<Option<String>>>,
    )>,
    buffer_rx: CrossbeamReceiver<WriteOp>,
    buffer_notify: CrossbeamSender<(Option<Error>, Option<OneshotSender<Option<String>>>)>,
    stats: Arc<WalStatistics>,
    shutdown: CancellationToken,
) {
    // FIXME:
    // 瓶颈在这里！！！
    // 大量的wal.write_multi调用，run_wal_op_buffer_task向CrossbeamChannel（连接task和本std::thread）发送
    // 大量的WriteOP，但是这里每次取出来一个WriteOp，处理完了才能再次取下一个WriteOp，虽然将schedule sync
    // 放到了一个单独的tokio::task里，但是下面的snap encode是比较耗时的，无法提前进行snap compress再
    // 去等待segment_writer.write。只能每次先从buffer_rx.recv()，再snap compress，再等待获取
    // segment_writer.write，可以看出是一个一个的单线程处理，很耗时。
    // TODO:能否每次取出来以后，放到spawn_blocking里取做？
    loop {
        let (mut entry, resp_tx) = match buffer_rx.recv() {
            Ok(write_op) => (write_op.entry, write_op.resp_tx),
            Err(e) => {
                // the buffer channel has closed, it's shutdown
                return;
            }
        };
        // limit how many concurrent encodings can be in flight.  Since we can only
        // write one at a time to disk, a slow disk can cause the allocations below
        // to increase quickly.  If we're backed up, wait until others have completed.
        let bytes = match entry.encode() {
            Ok(b) => b,
            Err(e) => {
                // encode entry into bytes
                buffer_notify
                    .send((Some(e), Some(resp_tx)))
                    .expect("buffer flusher is dead");
                continue;
            }
        };

        // compress the encoded bytes by snappy.
        let compressed = match snap::raw::Encoder::new()
            .compress_vec(bytes.as_slice())
            .context(SnapCompressSnafu)
        {
            Ok(compressed) => compressed,
            Err(e) => {
                buffer_notify
                    .send((Some(e), Some(resp_tx)))
                    .expect("buffer flusher is dead");
                continue;
            }
        };

        {
            let mut segment_writer = segment_writer.write();
            // Make sure the log has not been closed
            if shutdown.is_cancelled() {
                buffer_notify
                    .send((Some(Error::WALClosed), Some(resp_tx)))
                    .expect("buffer flusher is dead");
                continue;
            }

            // write wal entry
            if let Err(e) = segment_writer.write_entry(
                entry.entry_type(),
                compressed.as_slice(),
                Arc::clone(&stats),
            ) {
                buffer_notify
                    .send((Some(e), Some(resp_tx)))
                    .expect("buffer flusher is dead");
                continue;
            }
            add_resp_tx(resp_tx, &sync_waiters);

            // update stats for current segment size
            stats
                .current_bytes
                .store(segment_writer.current_segment_size(), Ordering::SeqCst);

            // update last write time
            segment_writer.last_write_time = Time::now();
        }

        buffer_notify
            .send((None, None))
            .expect("buffer flusher is dead");
    }
}

async fn run_wal_op_buffer_task(
    segment_writer: Arc<RwLock<WalSegmentWriter>>,
    mut buffer_rx: mpsc::Receiver<WriteOp>, // get write op
    // io_flush_tx: Arc<CrossbeamSender<WriteOp>>, // send to wal_io thread
    io_flush_notify_rx: Arc<
        CrossbeamReceiver<(Option<Error>, Option<OneshotSender<Option<String>>>)>,
    >, // get resp from wal_io_thread
    buffer_notify: Arc<CrossbeamSender<(Option<Error>, Option<OneshotSender<Option<String>>>)>>, // send write log resp
    /*buffer_flush_interval: Duration,*/
    sync_delay: Duration,
    sync_count: Arc<AtomicI32>,
    sync_waiters: Arc<(
        CrossbeamSender<OneshotSender<Option<String>>>,
        CrossbeamReceiver<OneshotSender<Option<String>>>,
    )>,
    stats: Arc<WalStatistics>,
    shutdown: CancellationToken,
    executor: Weak<dyn crate::runtime::execution::RuntimeExecutor>,
) {
    loop {
        tokio::select! {
            Some(write_op) = buffer_rx.recv() => {  // buffer write ops
                let segment_writer = Arc::clone(&segment_writer);
                // let io_flush_tx = Arc::clone(&io_flush_tx);
                let io_flush_notify_rx = Arc::clone(&io_flush_notify_rx);
                let buffer_notify = Arc::clone(&buffer_notify);
                let sync_count = Arc::clone(&sync_count);
                let sync_waiters = Arc::clone(&sync_waiters);
                let stats = Arc::clone(&stats);
                let shutdown = shutdown.clone();
                let rejected_op = scopeguard::guard(write_op, |write_op| {
                    let _ = write_op.resp_tx.send(Some(
                        "VLog index WAL task scheduling rejected by host executor".to_string(),
                    ));
                });
                let Some(executor) = executor.upgrade() else {
                    drop(rejected_op);
                    continue;
                };
                let task_executor = Arc::clone(&executor);

                if let Err(error) = executor.spawn(async move {
                    let write_op = scopeguard::ScopeGuard::into_inner(rejected_op);
                    // io_flush_tx.send(write_op).expect("wal io thread is dead");
                    // if let (Some(e), Some(resp_tx)) = io_flush_notify_rx.recv().expect("wal io thread is dead") {
                    //     let _ = resp_tx.send(Some(e.to_string()));
                    //     return;
                    // }

                    let seg_writer = Arc::clone(&segment_writer);
                    let waiters = Arc::clone(&sync_waiters);
                    let shutdown2 = shutdown.clone();
                    let rejected_op = scopeguard::guard(write_op, |write_op| {
                        let _ = write_op.resp_tx.send(Some(
                            "VLog index WAL blocking task scheduling rejected by host executor"
                                .to_string(),
                        ));
                    });
                    let blocking_task = match task_executor.spawn_blocking(move || {
                        let write_op = scopeguard::ScopeGuard::into_inner(rejected_op);
                        let (mut entry, resp_tx) = (write_op.entry, write_op.resp_tx);
                        // limit how many concurrent encodings can be in flight.  Since we can only
                        // write one at a time to disk, a slow disk can cause the allocations below
                        // to increase quickly.  If we're backed up, wait until others have completed.
                        let bytes = match entry.encode() {
                            Ok(b) => b,
                            Err(e) => {
                                // encode entry into bytes
                                buffer_notify
                                    .send((Some(e), Some(resp_tx)))
                                    .expect("buffer flusher is dead");
                                return;
                            }
                        };


                        // compress the encoded bytes by snappy.
                        let compressed = match snap::raw::Encoder::new()
                            .compress_vec(bytes.as_slice())
                            .context(SnapCompressSnafu)
                        {
                            Ok(compressed) => compressed,
                            Err(e) => {
                                buffer_notify
                                    .send((Some(e), Some(resp_tx)))
                                    .expect("buffer flusher is dead");
                                return;
                            }
                        };

                        {
                            let mut seg_writer = seg_writer.write();
                            // Make sure the log has not been closed
                            if shutdown2.is_cancelled() {
                                buffer_notify
                                .send((Some(Error::WALClosed), Some(resp_tx)))
                                .expect("buffer flusher is dead");
                                return;
                            }

                            // write wal entry
                            if let Err(e) = seg_writer.write_entry(
                                entry.entry_type(),
                                compressed.as_slice(),
                                Arc::clone(&stats),
                            ) {
                                buffer_notify
                                    .send((Some(e), Some(resp_tx)))
                                    .expect("buffer flusher is dead");
                                return;
                            }
                            add_resp_tx(resp_tx, &waiters);

                            // update stats for current segment size
                            stats
                                .current_bytes
                                .store(seg_writer.current_segment_size(), Ordering::SeqCst);

                            // update last write time
                            seg_writer.last_write_time = Time::now();
                        }

                        buffer_notify
                            .send((None, None))
                            .expect("buffer flusher is dead");
                    }) {
                        Ok(task) => task,
                        Err(error) => {
                            tracing::error!(
                                target: "reefsdb::tsdb::wal",
                                error = %error,
                                "VLog index WAL blocking task scheduling failed"
                            );
                            return;
                        }
                    };

                    if blocking_task.await.is_err() {
                        return;
                    }

                    if let (Some(e), Some(resp_tx)) = io_flush_notify_rx.recv().expect("wal io thread is dead") {
                        let _ = resp_tx.send(Some(e.to_string()));
                        return;
                    }



                    let seg_writer = Arc::clone(&segment_writer);
                    let sync_count = Arc::clone(&sync_count);
                    let waiters = Arc::clone(&sync_waiters);
                    let shutdown = shutdown.clone();
                    let rejected_sync_count = Arc::clone(&sync_count);
                    let rejected_waiters = Arc::clone(&waiters);
                    let owns_sync = Arc::new(std::sync::atomic::AtomicBool::new(false));
                    let cancelled_owns_sync = Arc::clone(&owns_sync);
                    let cancelled_sync_count = Arc::clone(&sync_count);
                    let cancelled_waiters = Arc::clone(&waiters);
                    let (registration, completion) = accepted_task_guard(move || {
                        let owned = cancelled_owns_sync.swap(false, Ordering::SeqCst);
                        if owned || cancelled_sync_count.load(Ordering::SeqCst) == 0 {
                            cancelled_sync_count.store(0, Ordering::SeqCst);
                            while let Ok(waiter) = cancelled_waiters.1.try_recv() {
                                let _ = waiter.send(Some(
                                    "VLog index WAL sync task was cancelled by host executor"
                                        .to_string(),
                                ));
                            }
                        }
                    });
                    match task_executor.spawn(async move {
                        let sync_work = async {
                            // If we're not the first to sync, then another goroutine is fsyncing the wal for us.
                            if sync_count.compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst).is_err() {
                                return;
                            }
                            owns_sync.store(true, Ordering::SeqCst);

                            // Fsync the wal and notify all pending waiters
                            if sync_delay.is_zero() {
                                // let interval = tokio::time::sleep(std::time::Duration::ZERO);
                                loop {
                                    tokio::select! {
                                        _ = tokio::time::sleep(std::time::Duration::ZERO) => {
                                            let mut seg_writer = seg_writer.write();
                                            if waiters.1.is_empty() {
                                                sync_count.store(0, Ordering::SeqCst);
                                                return;
                                            }
                                            seg_writer.sync();
                                            drop(seg_writer);
                                        }
                                        _ = shutdown.cancelled() => {
                                            sync_count.store(0, Ordering::SeqCst);
                                            return;
                                        }
                                    }
                                }
                            } else {
                                let mut interval = tokio::time::interval(sync_delay);
                                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                                loop {
                                    tokio::select! {
                                        _ = interval.tick() => {
                                            let mut seg_writer = segment_writer.write();
                                            if waiters.1.is_empty() {
                                                sync_count.store(0, Ordering::SeqCst);
                                                return;
                                            }

                                            seg_writer.sync();
                                            drop(seg_writer);
                                        }
                                        _ = shutdown.cancelled() => {
                                            sync_count.store(0, Ordering::SeqCst);
                                            return;
                                        }
                                    }
                                }
                            }
                        };
                        sync_work.await;
                        completion.complete();
                    }) {
                        Ok(_) => registration.accepted(),
                        Err(error) => {
                            if rejected_sync_count.load(Ordering::SeqCst) == 0 {
                                while let Ok(waiter) = rejected_waiters.1.try_recv() {
                                    let _ = waiter.send(Some(error.to_string()));
                                }
                            }
                            registration.rejected();
                            tracing::error!(
                                target: "reefsdb::tsdb::wal",
                                error = %error,
                                "VLog index WAL sync task scheduling failed"
                            );
                        }
                    }
                }) {
                    tracing::error!(
                        target: "reefsdb::tsdb::wal",
                        error = %error,
                        "VLog index WAL operation task scheduling failed"
                    );
                }
            },
            _ = shutdown.cancelled() => {
                return;
            }
        }
    }
}

#[derive(Default, Debug)]
pub struct WalStatistics {
    pub old_bytes: Arc<AtomicI64>,
    pub current_bytes: Arc<AtomicI64>,
    pub write_ok: Arc<AtomicI64>,
    pub write_err: Arc<AtomicI64>,
}

impl WalStatistics {}

pub fn segment_file_names(path: &Path) -> Result<Vec<String>> {
    let mut names: Vec<String> = fs::read_dir(path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| {
            if !path.is_file() {
                return false;
            }
            let path = path.file_name().unwrap().to_str().unwrap();
            path.starts_with(WAL_FILE_PREFIX) && path.ends_with(&format!(".{}", WAL_FILE_EXTENSION))
        })
        .flat_map(|path| path.into_os_string().into_string())
        .collect();

    // sort log file names
    names.sort();

    Ok(names)
}

fn id_from_file_name(name: &str) -> Result<i64> {
    let file_name = PathBuf::from(name)
        .file_name()
        .unwrap()
        .to_os_string()
        .into_string()
        .unwrap();
    let parts: Vec<&str> = file_name.split('.').collect();
    ensure!(
        parts.len() == 2,
        InvalidFileNameSnafu {
            reason: format!("file {} has wrong wrong name format to have an id", name),
        }
    );

    match atoi_simd::parse::<i64>(parts[0].get(1..).unwrap().as_bytes()) {
        Ok(id) => Ok(id),
        Err(e) => Err(Error::InvalidFileName {
            reason: format!("parse id from {} failed: {}", name, e),
        }),
    }
}

pub fn new_segment_writer(
    path: &Path,
    writer: BufWriter<File>,
    segment_id: i64,
    segment_size: i64,
    sync_waiters: Arc<(
        CrossbeamSender<OneshotSender<Option<String>>>,
        CrossbeamReceiver<OneshotSender<Option<String>>>,
    )>,
    pos: u64,
) -> WalSegmentWriter {
    WalSegmentWriter::new(
        Arc::new(path.to_path_buf()),
        segment_size,
        segment_id,
        writer,
        sync_waiters,
        pos,
    )
}

// ///////////////////////////////////////////
// ///////////// WAL Entry      //////////////
// ///////////////////////////////////////////

pub trait WalEntry {
    fn entry_type(&self) -> WalEntryType;
    // fn entry_values(&self) -> Box
    fn marshal_size(&mut self) -> usize;
    fn encode(&mut self) -> Result<Vec<u8>>;
    fn marshal_binary(&mut self) -> Result<Vec<u8>>;
    fn unmarshal_binary(&mut self, b: &[u8]) -> Result<()>;
    /// Cast client to [`Any`], useful for downcasting.
    fn as_any(&self) -> &dyn Any;
}
/// WriteWalEntry represents a write of points.
#[derive(Debug, Clone)]
pub struct WriteWalEntry {
    pub values: HashMap<String, Vec<VLogIndex>>,
    pub size: usize,
}

impl WriteWalEntry {
    fn new() -> Self {
        Self {
            values: HashMap::<String, Vec<VLogIndex>>::new(),
            size: 0,
        }
    }
}

impl WalEntry for WriteWalEntry {
    /// Returns WRITE_WAL_ENTRY_TYPE.
    fn entry_type(&self) -> WalEntryType {
        WRITE_WAL_ENTRY_TYPE
    }

    fn marshal_size(&mut self) -> usize {
        if self.size > 0 || self.values.is_empty() {
            return self.size;
        }

        let mut enc_len = 7 * self.values.len(); // Type (1), Key Length (2), and Count (4) for each key

        // determine required length
        for (k, v) in self.values.iter() {
            enc_len += k.len();
            if v.is_empty() {
                return 0;
            }

            enc_len += VLogIndex::SIZE * v.len();
            if v.iter().any(|i| i.aggregate.is_some()) {
                enc_len += v.iter().map(|i| 1 + i.aggregate.as_ref().map_or(0, |_| super::aggregate_record::AggregateRecord::SIZE)).sum::<usize>();
            }
        }
        self.size = enc_len;

        self.size
    }

    /// encode coverts the WriteWalEntry into a byte stream using dst if it is a large enough,
    /// if dst is too small, the slice will be grown to fit he encoded entry.
    fn encode(&mut self) -> Result<Vec<u8>> {
        // The entries values are encode as follows:
        //
        // For each key and slice of values, first a 1 byte type for the []Values
        // slice is written.  Following the type, the length and key bytes are written.
        // Following the key, a 4 byte count followed by each value as a 8 byte time
        // and N byte value.  The value is dependent on the type being encoded.  float64,
        // int64, use 8 bytes, boolean uses 1 byte, and string is similar to the key encoding,
        // except that string values have a 4-byte length, and keys only use 2 bytes.
        //
        // This structure is then repeated for each key an value slices.
        //
        // ┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
        // │                           WriteWALEntry                                                         │
        // ├──────┬─────────┬────────┬───────┬───────┬───────────┬─────────┬─────────┬─────────┬─────────┬─────────┬───┤
        // │ Type │ Key Len │   Key  │ Idxes │ Vals  │   MinTime │ MaxTime │   Gen   │  Seq    │ Offset  │  Size   │...│
        // │      │         │        │ Count │ Count │           │         │
        // │1 byte│ 2 bytes │ N bytes│4 bytes│4 bytes│   8 bytes │ 8 bytes │ 8 bytes │ 8 bytes │ 8 bytes │ 8 bytes │   │
        // └──────┴─────────┴────────┴───────┴───────┴───────────┴─────────┴─────────┴─────────┴─────────┴─────────┴───┘

        // Type (1), Key Length (2), and Count (4) for each key allocate or resize slice to corrent size
        let mut b: Vec<u8> = vec![0u8; self.marshal_size()];
        // encode wal entry
        let mut n: usize = 0;
        for (k, v) in self.values.iter() {
            let enriched = v.iter().any(|i| i.aggregate.is_some());
            b[n] = if enriched { VLOG_INDEX_AGGREGATE } else { VLOG_INDEX_COMPACTION };
            n += 1;

            (&mut b[n..n + 2]).put_u16(k.len() as u16); // BigEndian
            n += 2;
            b[n..n + k.len()].copy_from_slice(k.as_bytes());
            n += k.len();

            (&mut b[n..n + 4]).put_u32(v.len() as u32); // vlog indexes count, BigEndian
            n += 4;

            for vv in v.iter() {
                (&mut b[n..n + 4]).put_u32(vv.count as u32); // values count in this vlog index
                n += 4;
                b[n] = vv.typ; // vlog index type
                n += 1;
                (&mut b[n..n + 8]).put_u64(vv.min as u64);
                n += 8;
                (&mut b[n..n + 8]).put_u64(vv.max as u64);
                n += 8;
                (&mut b[n..n + 8]).put_u64(vv.generation as u64); // generation
                n += 8;
                (&mut b[n..n + 8]).put_u64(vv.seq as u64); // sequence
                n += 8;
                (&mut b[n..n + 8]).put_u64(vv.offset as u64);
                n += 8;
                (&mut b[n..n + 8]).put_u64(vv.size as u64);
                n += 8;
                if enriched {
                    b[n] = u8::from(vv.aggregate.is_some());
                    n += 1;
                    if let Some(aggregate) = &vv.aggregate {
                        let mut encoded = Vec::with_capacity(super::aggregate_record::AggregateRecord::SIZE);
                        aggregate.encode(&mut encoded);
                        b[n..n + encoded.len()].copy_from_slice(&encoded);
                        n += encoded.len();
                    }
                }
            }
        }
        b.truncate(n);

        Ok(b)
    }

    /// Return a binary representation of the entry in a new byte slice.
    fn marshal_binary(&mut self) -> Result<Vec<u8>> {
        self.encode()
    }

    /// Deserializes the byte slice into values.
    fn unmarshal_binary(&mut self, b: &[u8]) -> Result<()> {
        let mut i: usize = 0;
        while i < b.len() {
            let typ = b[i];
            i += 1;

            ensure!(i + 2 <= b.len(), WALCorruptedSnafu);

            let length = (&b[i..i + 2]).get_u16() as usize; // BigEndian
            i += 2;
            ensure!(i + length <= b.len(), WALCorruptedSnafu);

            let k = to_str!(&b[i..i + length]);
            i += length;

            ensure!(i + 4 <= b.len(), WALCorruptedSnafu);

            let nvals = (&b[i..i + 4]).get_u32() as usize; // BigEndian
            i += 4;

            ensure!(nvals > 0 && nvals <= b.len(), WALCorruptedSnafu);
            match typ {
                VLOG_INDEX_COMPACTION | VLOG_INDEX_AGGREGATE => {
                    ensure!(i + VLogIndex::SIZE * nvals <= b.len(), WALCorruptedSnafu);

                    let mut values: Vec<VLogIndex> = Vec::with_capacity(nvals);
                    let mut j = 0;
                    while j < nvals {
                        ensure!(i + VLogIndex::SIZE <= b.len(), WALCorruptedSnafu);
                        let count = (&b[i..i + 4]).get_u32() as i32;
                        i += 4;
                        let value_type = b[i];
                        i += 1;
                        let min = (&b[i..i + 8]).get_u64() as i64;
                        i += 8;
                        let max = (&b[i..i + 8]).get_u64() as i64;
                        i += 8;
                        let generation = (&b[i..i + 8]).get_u64() as i64;
                        i += 8;
                        let seq = (&b[i..i + 8]).get_u64() as i64;
                        i += 8;
                        let offset = (&b[i..i + 8]).get_u64() as i64;
                        i += 8;
                        let size = (&b[i..i + 8]).get_u64() as i64;
                        i += 8;

                        let aggregate = if typ == VLOG_INDEX_AGGREGATE {
                            ensure!(i < b.len(), WALCorruptedSnafu);
                            let present = b[i]; i += 1;
                            match present {
                                0 => None,
                                1 => {
                                    let end = i + super::aggregate_record::AggregateRecord::SIZE;
                                    ensure!(end <= b.len(), WALCorruptedSnafu);
                                    let record = super::aggregate_record::AggregateRecord::decode(&b[i..end])
                                        .map_err(|_| Error::WALCorrupted)?;
                                    ensure!(record.values.count == count as u64, WALCorruptedSnafu);
                                    i = end;
                                    Some(std::sync::Arc::new(record))
                                },
                                _ => return WALCorruptedSnafu.fail(),
                            }
                        } else { None };
                        values.push(VLogIndex {
                            aggregate,
                            count,
                            typ: value_type,
                            min,
                            max,
                            generation,
                            seq,
                            offset,
                            size,
                        });
                        j += 1;
                    }
                    self.values.insert(k.to_owned(), values);
                }
                _ => {
                    return UnsupportedValueSnafu {
                        reason: format!("unsupported value type: {}", typ),
                    }
                    .fail();
                }
            }
        }

        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// DeleteWalEntry represents a delete of points.
#[derive(Debug, Clone)]
pub struct DeleteWalEntry {
    pub keys: Vec<Vec<u8>>,
    pub size: usize,
}

impl DeleteWalEntry {
    /// return an empty DeleteEntry
    fn new() -> Self {
        Self {
            keys: Vec::<Vec<u8>>::new(),
            size: 0,
        }
    }
}

impl WalEntry for DeleteWalEntry {
    /// Returns DELETE_WAL_ENTRY_TYPE.
    fn entry_type(&self) -> WalEntryType {
        DELETE_WAL_ENTRY_TYPE
    }

    fn marshal_size(&mut self) -> usize {
        if self.size > 0 || self.keys.is_empty() {
            return self.size;
        }

        let mut enc_len = self.keys.len();
        for k in self.keys.iter() {
            enc_len += k.len();
        }

        self.size = enc_len;

        enc_len
    }

    /// converts the DeleteRangeWalEntry into a byte slice, appending to dst.
    fn encode(&mut self) -> Result<Vec<u8>> {
        // keys count, key len for each key allocate or resize slice to corrent size
        let mut b: Vec<u8> = vec![0u8; self.marshal_size()];
        let mut n: usize = 0;
        for k in self.keys.iter() {
            b[n..n + k.len()].copy_from_slice(k.as_slice());
            n += k.len();
            b[n] = b'\n';
            n += 1;
        }

        // truncate the vec to n-1 to strip off the last newline so that unmarshalling the value
        // does not produce an empty string
        b.truncate(n - 1);
        Ok(b)
    }

    // Return a binary representation of the entry in a new byte slice.
    fn marshal_binary(&mut self) -> Result<Vec<u8>> {
        self.encode()
    }

    // deserializes the byte slice into self.
    fn unmarshal_binary(&mut self, b: &[u8]) -> Result<()> {
        if b.is_empty() {
            return Ok(());
        }

        self.keys = unsafe {
            std::str::from_utf8_unchecked(b)
                .split('\n')
                .map(|s| s.as_bytes().to_vec())
                .collect::<Vec<Vec<u8>>>()
        };

        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct DeleteMultiWalEntry {
    pub values: HashMap<String, Vec<VLogIndex>>,
    pub size: usize,
}

impl DeleteMultiWalEntry {
    fn new() -> Self {
        Self {
            values: HashMap::<String, Vec<VLogIndex>>::new(),
            size: 0,
        }
    }
}

impl WalEntry for DeleteMultiWalEntry {
    /// Returns WRITE_WAL_ENTRY_TYPE.
    fn entry_type(&self) -> WalEntryType {
        DELETE_MULTI_WAL_ENTRY_TYPE
    }

    fn marshal_size(&mut self) -> usize {
        if self.size > 0 || self.values.is_empty() {
            return self.size;
        }

        let mut enc_len = 7 * self.values.len(); // Type (1), Key Length (2), and Count (4) for each key

        // determine required length
        for (k, v) in self.values.iter() {
            enc_len += k.len();
            if v.is_empty() {
                return 0;
            }

            enc_len += VLogIndex::SIZE * v.len();
        }
        self.size = enc_len;

        self.size
    }

    /// encode coverts the WriteWalEntry into a byte stream using dst if it is a large enough,
    /// if dst is too small, the slice will be grown to fit he encoded entry.
    fn encode(&mut self) -> Result<Vec<u8>> {
        // The entries values are encode as follows:
        //
        // For each key and slice of values, first a 1 byte type for the []Values
        // slice is written.  Following the type, the length and key bytes are written.
        // Following the key, a 4 byte count followed by each value as a 8 byte time
        // and N byte value.  The value is dependent on the type being encoded.  float64,
        // int64, use 8 bytes, boolean uses 1 byte, and string is similar to the key encoding,
        // except that string values have a 4-byte length, and keys only use 2 bytes.
        //
        // This structure is then repeated for each key an value slices.
        //
        // ┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
        // │                           WriteWALEntry                                                         │
        // ├──────┬─────────┬────────┬───────┬─────────┬─────────┬─────────┬─────────┬─────────┬─────────┬───┤
        // │ Type │ Key Len │   Key  │ Count │ MinTime │ MaxTime │   Gen   │  Seq    │ Offset  │  Size   │...│
        // │1 byte│ 2 bytes │ N bytes│4 bytes│ 8 bytes │ 8 bytes │ 8 bytes │ 8 bytes │ 8 bytes │ 8 bytes │   │
        // └──────┴─────────┴────────┴───────┴─────────┴─────────┴─────────┴─────────┴─────────┴─────────┴───┘

        // Type (1), Key Length (2), and Count (4) for each key allocate or resize slice to corrent size
        let mut b: Vec<u8> = vec![0u8; self.marshal_size()];
        // encode wal entry
        let mut n: usize = 0;
        let cur_type = VLOG_INDEX_COMPACTION;
        for (k, v) in self.values.iter() {
            b[n] = cur_type;
            n += 1;

            (&mut b[n..n + 2]).put_u16(k.len() as u16); // BigEndian
            n += 2;
            b[n..n + k.len()].copy_from_slice(k.as_bytes());
            n += k.len();

            (&mut b[n..n + 4]).put_u32(v.len() as u32); // vlog indexes count, BigEndian
            n += 4;

            for vv in v.iter() {
                (&mut b[n..n + 4]).put_u32(vv.count as u32); // values count in this vlog index
                n += 4;
                b[n] = vv.typ;
                n += 1;
                (&mut b[n..n + 8]).put_u64(vv.min as u64);
                n += 8;
                (&mut b[n..n + 8]).put_u64(vv.max as u64);
                n += 8;
                (&mut b[n..n + 8]).put_u64(vv.generation as u64); // generation
                n += 8;
                (&mut b[n..n + 8]).put_u64(vv.seq as u64); // sequence
                n += 8;
                (&mut b[n..n + 8]).put_u64(vv.offset as u64);
                n += 8;
                (&mut b[n..n + 8]).put_u64(vv.size as u64);
                n += 8;
            }
        }
        b.truncate(n);

        Ok(b)
    }

    /// Return a binary representation of the entry in a new byte slice.
    fn marshal_binary(&mut self) -> Result<Vec<u8>> {
        self.encode()
    }

    /// Deserializes the byte slice into values.
    fn unmarshal_binary(&mut self, b: &[u8]) -> Result<()> {
        let mut i: usize = 0;
        while i < b.len() {
            let typ = b[i];
            i += 1;

            ensure!(i + 2 <= b.len(), WALCorruptedSnafu);

            let length = (&b[i..i + 2]).get_u16() as usize; // BigEndian
            i += 2;
            ensure!(i + length <= b.len(), WALCorruptedSnafu);

            let k = to_str!(&b[i..i + length]);
            i += length;

            ensure!(i + 4 <= b.len(), WALCorruptedSnafu);

            let nvals = (&b[i..i + 4]).get_u32() as usize; // BigEndian
            i += 4;

            ensure!(nvals > 0 && nvals <= b.len(), WALCorruptedSnafu);
            match typ {
                VLOG_INDEX_COMPACTION => {
                    ensure!(i + VLogIndex::SIZE * nvals <= b.len(), WALCorruptedSnafu);

                    let mut values: Vec<VLogIndex> = Vec::with_capacity(nvals);
                    let mut j = 0;
                    while j < nvals {
                        let count = (&b[i..i + 4]).get_u32() as i32;
                        i += 4;
                        let value_type = b[i];
                        i += 1;
                        let min = (&b[i..i + 8]).get_u64() as i64;
                        i += 8;
                        let max = (&b[i..i + 8]).get_u64() as i64;
                        i += 8;
                        let generation = (&b[i..i + 8]).get_u64() as i64;
                        i += 8;
                        let seq = (&b[i..i + 8]).get_u64() as i64;
                        i += 8;
                        let offset = (&b[i..i + 8]).get_u64() as i64;
                        i += 8;
                        let size = (&b[i..i + 8]).get_u64() as i64;
                        i += 8;

                        values.push(VLogIndex {
                            aggregate: None,
                            count,
                            typ: value_type,
                            min,
                            max,
                            generation,
                            seq,
                            offset,
                            size,
                        });
                        j += 1;
                    }
                    self.values.insert(k.to_owned(), values);
                }
                _ => {
                    return UnsupportedValueSnafu {
                        reason: format!("unsupported value type: {}", typ),
                    }
                    .fail();
                }
            }
        }

        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone)]
pub struct ReplaceMultiWalEntry {
    pub old_indexes: HashMap<String, Vec<VLogIndex>>,
    pub new_indexes: HashMap<String, Vec<VLogIndex>>,
    pub version: u8,
    pub mode: u8,
    pub size: usize,
}

impl ReplaceMultiWalEntry {
    fn new() -> Self {
        Self {
            old_indexes: HashMap::<String, Vec<VLogIndex>>::new(),
            new_indexes: HashMap::<String, Vec<VLogIndex>>::new(),
            version: REPLACE_MULTI_WAL_ENTRY_VERSION,
            mode: REPLACE_MULTI_WAL_ENTRY_DEFAULT_MODE,
            size: 0,
        }
    }

    fn encode_index_section(values: HashMap<String, Vec<VLogIndex>>) -> Result<Vec<u8>> {
        WriteWalEntry { values, size: 0 }.encode()
    }

    fn decode_index_section(b: &[u8]) -> Result<HashMap<String, Vec<VLogIndex>>> {
        let mut entry = WriteWalEntry::new();
        entry.unmarshal_binary(b)?;
        Ok(entry.values)
    }
}

impl WalEntry for ReplaceMultiWalEntry {
    /// Returns REPLACE_MULTI_WAL_ENTRY_TYPE.
    fn entry_type(&self) -> WalEntryType {
        REPLACE_MULTI_WAL_ENTRY_TYPE
    }

    fn marshal_size(&mut self) -> usize {
        if self.size > 0 {
            return self.size;
        }

        let mut old_entry = WriteWalEntry {
            values: self.old_indexes.clone(),
            size: 0,
        };
        let mut new_entry = WriteWalEntry {
            values: self.new_indexes.clone(),
            size: 0,
        };

        self.size = 1 + 1 + 4 + 4 + old_entry.marshal_size() + new_entry.marshal_size();
        self.size
    }

    fn encode(&mut self) -> Result<Vec<u8>> {
        let old_section = Self::encode_index_section(self.old_indexes.clone())?;
        let new_section = Self::encode_index_section(self.new_indexes.clone())?;

        self.size = 1 + 1 + 4 + 4 + old_section.len() + new_section.len();
        let mut b: Vec<u8> = vec![0u8; self.size];
        let mut n = 0;

        b[n] = self.version;
        n += 1;
        b[n] = self.mode;
        n += 1;

        (&mut b[n..n + 4]).put_u32(old_section.len() as u32);
        n += 4;
        (&mut b[n..n + 4]).put_u32(new_section.len() as u32);
        n += 4;

        b[n..n + old_section.len()].copy_from_slice(&old_section);
        n += old_section.len();
        b[n..n + new_section.len()].copy_from_slice(&new_section);
        n += new_section.len();
        b.truncate(n);

        Ok(b)
    }

    fn marshal_binary(&mut self) -> Result<Vec<u8>> {
        self.encode()
    }

    fn unmarshal_binary(&mut self, b: &[u8]) -> Result<()> {
        ensure!(b.len() >= 10, WALCorruptedSnafu);

        let version = b[0];
        ensure!(
            version == REPLACE_MULTI_WAL_ENTRY_VERSION,
            UnsupportedValueSnafu {
                reason: format!("unsupported replace_multi version: {}", version),
            }
        );
        let mode = b[1];

        let old_section_len = (&b[2..6]).get_u32() as usize;
        let new_section_len = (&b[6..10]).get_u32() as usize;
        ensure!(
            10 + old_section_len + new_section_len == b.len(),
            WALCorruptedSnafu
        );

        let old_section_start = 10;
        let old_section_end = old_section_start + old_section_len;
        let new_section_end = old_section_end + new_section_len;

        self.version = version;
        self.mode = mode;
        self.old_indexes = Self::decode_index_section(&b[old_section_start..old_section_end])?;
        self.new_indexes = Self::decode_index_section(&b[old_section_end..new_section_end])?;
        self.size = b.len();

        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// WriteRangeWalEntry represents a delete range of points.
#[derive(Debug, Clone)]
pub struct DeleteRangeWalEntry {
    pub keys: Vec<Vec<u8>>,
    pub min: i64, // min time
    pub max: i64,
    pub size: usize,
}

impl DeleteRangeWalEntry {
    /// return an empty DeleteRangeWalEntry
    fn new() -> Self {
        Self {
            keys: Vec::<Vec<u8>>::new(),
            min: 0,
            max: 0,
            size: 0,
        }
    }
}

impl WalEntry for DeleteRangeWalEntry {
    /// Returns DELETE_RANGE_WAL_ENTRY_TYPE.
    fn entry_type(&self) -> WalEntryType {
        DELETE_RANGE_WAL_ENTRY_TYPE
    }

    fn marshal_size(&mut self) -> usize {
        if self.size > 0 {
            return self.size;
        }

        let mut sz = 16 + self.keys.len() * 4;
        for k in self.keys.iter() {
            sz += k.len();
        }
        self.size = sz;

        sz
    }

    /// converts the DeleteRangeWalEntry into a byte slice, appending to dst.
    fn encode(&mut self) -> Result<Vec<u8>> {
        let mut b: Vec<u8> = vec![0u8; self.marshal_size()];
        (&mut b[..8]).put_u64(self.min as u64); // BigEndian
        (&mut b[8..16]).put_u64(self.max as u64);

        let mut i = 16;
        for k in self.keys.iter() {
            (&mut b[i..i + 4]).put_u32(k.len() as u32);
            i += 4;
            b[i..i + k.len()].copy_from_slice(k.as_slice());
            i += k.len();
        }

        b.truncate(i);
        Ok(b)
    }

    // Return a binary representation of the entry in a new byte slice.
    fn marshal_binary(&mut self) -> Result<Vec<u8>> {
        self.encode()
    }

    // deserializes the byte slice into self.
    fn unmarshal_binary(&mut self, b: &[u8]) -> Result<()> {
        ensure!(b.len() >= 16, WALCorruptedSnafu);

        self.min = (&b[0..8]).get_u64() as i64; // BigEndian
        self.max = (&b[8..16]).get_u64() as i64;

        let mut i = 16;
        while i < b.len() {
            ensure!(i + 4 <= b.len(), WALCorruptedSnafu);

            let sz = (&b[i..i + 4]).get_u32() as usize;
            i += 4;

            ensure!(i + sz <= b.len(), WALCorruptedSnafu);

            // b originates from a pool. Copy what needs to be retained.
            let mut buf = vec![0; sz];
            unsafe {
                buf.set_len(sz);
            }
            buf.copy_from_slice(&b[i..i + sz]);
            self.keys.push(buf);
            i += sz;
        }

        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
pub enum Entry {
    WriteWalEntry(WriteWalEntry),
    DeleteWalEntry(DeleteWalEntry),
    DeleteRangeWalEntry(DeleteRangeWalEntry),
    DeleteMultiWalEntry(DeleteMultiWalEntry),
    ReplaceMultiWalEntry(ReplaceMultiWalEntry),
}

impl Entry {
    pub fn entry_type(&self) -> WalEntryType {
        match self {
            Entry::WriteWalEntry(entry) => entry.entry_type(),
            Entry::DeleteWalEntry(entry) => entry.entry_type(),
            Entry::DeleteRangeWalEntry(entry) => entry.entry_type(),
            Entry::DeleteMultiWalEntry(entry) => entry.entry_type(),
            Entry::ReplaceMultiWalEntry(entry) => entry.entry_type(),
        }
    }

    // fn entry_values(&self) -> Box
    pub fn marshal_size(&mut self) -> usize {
        match self {
            Entry::WriteWalEntry(entry) => entry.marshal_size(),
            Entry::DeleteWalEntry(entry) => entry.marshal_size(),
            Entry::DeleteRangeWalEntry(entry) => entry.marshal_size(),
            Entry::DeleteMultiWalEntry(entry) => entry.marshal_size(),
            Entry::ReplaceMultiWalEntry(entry) => entry.marshal_size(),
        }
    }

    pub fn encode(&mut self) -> Result<Vec<u8>> {
        match self {
            Entry::WriteWalEntry(entry) => entry.encode(),
            Entry::DeleteWalEntry(entry) => entry.encode(),
            Entry::DeleteRangeWalEntry(entry) => entry.encode(),
            Entry::DeleteMultiWalEntry(entry) => entry.encode(),
            Entry::ReplaceMultiWalEntry(entry) => entry.encode(),
        }
    }

    pub fn marshal_binary(&mut self) -> Result<Vec<u8>> {
        match self {
            Entry::WriteWalEntry(entry) => entry.marshal_binary(),
            Entry::DeleteWalEntry(entry) => entry.marshal_binary(),
            Entry::DeleteRangeWalEntry(entry) => entry.marshal_binary(),
            Entry::DeleteMultiWalEntry(entry) => entry.marshal_binary(),
            Entry::ReplaceMultiWalEntry(entry) => entry.marshal_binary(),
        }
    }

    pub fn unmarshal_binary(&mut self, b: &[u8]) -> Result<()> {
        match self {
            Entry::WriteWalEntry(entry) => entry.unmarshal_binary(b),
            Entry::DeleteWalEntry(entry) => entry.unmarshal_binary(b),
            Entry::DeleteRangeWalEntry(entry) => entry.unmarshal_binary(b),
            Entry::DeleteMultiWalEntry(entry) => entry.unmarshal_binary(b),
            Entry::ReplaceMultiWalEntry(entry) => entry.unmarshal_binary(b),
        }
    }
}

// ///////////////////////////////////////////
// ///////// WAL Writer     //////////////////
// ///////////////////////////////////////////
#[derive(Debug)]
pub struct WalSegmentWriter {
    path: Arc<PathBuf>,
    segment_size: i64,
    current_segment_id: i64,
    pub sync_waiters: Arc<(
        CrossbeamSender<OneshotSender<Option<String>>>,
        CrossbeamReceiver<OneshotSender<Option<String>>>,
    )>,
    last_write_time: Time,
    pub writer: BufWriter<File>,
    pos: u64,
}

impl WalSegmentWriter {
    /// create new segment writer
    pub fn new(
        path: Arc<PathBuf>,
        segment_size: i64,
        id: i64,
        writer: BufWriter<File>,
        sync_waiters: Arc<(
            CrossbeamSender<OneshotSender<Option<String>>>,
            CrossbeamReceiver<OneshotSender<Option<String>>>,
        )>,
        pos: u64,
    ) -> Self {
        // maximun number of write log tasks in channel at once.
        const MAX_QUEUED_TASK: usize = 1000;

        WalSegmentWriter {
            path,
            segment_size,
            current_segment_id: id,
            last_write_time: Time::from_timestamp(0, 0),
            sync_waiters,
            writer,
            pos,
        }
    }

    pub fn current_segment_writer_path(&self) -> String {
        self.path
            .join(format!(
                "{}{:05}.{}",
                WAL_FILE_PREFIX, self.current_segment_id, WAL_FILE_EXTENSION
            ))
            .into_os_string()
            .into_string()
            .unwrap()
    }

    /// write log entry to WAL
    pub fn write_entry(
        &mut self,
        entry_type: WalEntryType,
        compressed: &[u8],
        stats: Arc<WalStatistics>,
    ) -> Result<()> {
        // checks if need to roll to a new segment file; and if so, opens a new segment file for future writes.
        self.roll_segment(stats)?;

        let mut b: [u8; 5] = [0; 5];
        b[0] = entry_type;
        (&mut b[1..5]).put_u32(compressed.len() as u32); // big endian

        if let Err(e) = self.writer.write(&b) {
            return Err(e.into());
        }

        if let Err(e) = self.writer.write(compressed) {
            return Err(e.into());
        }

        self.pos += (b.len() + compressed.len()) as u64;

        Ok(())
    }

    pub fn roll_segment(&mut self, stats: Arc<WalStatistics>) -> Result<()> {
        if self.pos > self.segment_size as u64 {
            if let Err(e) = self.new_segment_file(stats) {
                return OpenWALSegmentFileFailedSnafu {
                    reason: format!("opening new wal segment file failed (2): {}", e),
                }
                .fail();
            }
            return Ok(());
        }

        Ok(())
    }

    pub fn new_segment_file(&mut self, stats: Arc<WalStatistics>) -> Result<()> {
        self.current_segment_id += 1;
        // sync current writer first
        self.sync();
        // don't need to close segment writer, it will be closed
        // after the writer is dropped.
        // if let Err(e) = self.close(Arc::clone(&stats)) {
        //     return Err(e);
        // }
        stats
            .old_bytes
            .fetch_add(self.current_segment_size(), Ordering::SeqCst);

        // create new writer
        let file_name = self.path.join(format!(
            "{}{:05}.{}",
            WAL_FILE_PREFIX, self.current_segment_id, WAL_FILE_EXTENSION
        ));
        let fd = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_name)?;
        self.writer = BufWriter::new(fd);
        self.pos = 0;

        stats.current_bytes.store(0, Ordering::SeqCst);

        Ok(())
    }

    pub fn sync(&mut self) {
        let res = match self.sync2() {
            Ok(()) => None,
            Err(e) => Some(e.to_string()),
        };
        while let Ok(waiter) = self.sync_waiters.1.try_recv() {
            let _ = waiter.send(res.clone());
        }
    }

    pub fn sync2(&mut self) -> Result<()> {
        self.writer.flush().context(IOSnafu)?;
        self.writer.get_mut().sync_all().context(IOSnafu)
    }

    pub fn flush(&mut self) -> Result<()> {
        Ok(self.writer.flush()?)
    }

    // TODO:
    // pub fn close(&mut self /*, stats: Arc<WalStatistics>*/) -> Result<()> {
    //     // // TODO: lock first !!!
    //     // if self.writer.pos > 0 {
    //     //     if let Err(e) = self.new_segment_file(stats) {
    //     //         return Err(Error::ErrString {
    //     //             reason: format!("opening new wal segment file failed (1): {}", e),
    //     //         });
    //     //     }
    //     //     return Ok(())
    //     // }

    //     // return Ok(())

    //     if let Err(e) = self.writer.flush() {
    //         return Err(e.into())
    //     }
    //     // close file
    //     drop(self.writer);

    //     Ok(())
    // }

    // pub fn close(&mut self, stats: Arc<WalStatistics>) -> Result<()> {
    //     // TODO: lock first !!!
    //     if self.writer.pos > 0 {
    //         if let Err(e) = self.new_segment_file(stats) {
    //             return Err(Error::ErrString {
    //                 reason: format!("opening new wal segment file failed (1): {}", e),
    //             });
    //         }
    //         return Ok(())
    //     }

    //     return Ok(())
    // }

    pub fn current_segment_size(&self) -> i64 {
        self.pos as i64
    }
}

/// Create a new log file, returns the writer to the log.
pub(crate) fn new_log_file(path: &Path, segment_id: i64) -> Result<File> {
    let path = log_path(path, segment_id);
    let writer = OpenOptions::new().create(true).append(true).open(path)?;

    Ok(writer)
}

fn log_path(dir: &Path, segment_id: i64) -> PathBuf {
    dir.join(format!("{:05}.{}", segment_id, WAL_FILE_EXTENSION))
}

// /////////////////////////////////////////
// /////////// WAL Reader    ///////////////
// /////////////////////////////////////////

pub struct WalSegmentReader<'a> {
    segment: &'a File, // must be opened file
    pos: usize,
}

impl<'a> WalSegmentReader<'a> {
    pub fn new(f: &'a File) -> Self {
        Self { segment: f, pos: 0 }
    }

    pub fn reset(&mut self) -> Result<()> {
        // self.segment = None;
        self.segment.seek(SeekFrom::Start(0))?;
        self.pos = 0;

        Ok(())
    }

    /// Returns the total number of bytes read successfully from the segment, as
    /// of the last call to next(). The segment is guaranteed to be valid up to and
    /// including this number of bytes.
    pub fn read_bytes_count(&self) -> i64 {
        self.pos as i64
    }

    /// Close closes the underlying reader
    pub fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Iterator for WalSegmentReader<'_> {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        // if self.segment.is_none() {
        //     return None;
        // }

        let cur_pos = self.pos;
        // let mut segment_file = self.segment.as_ref().unwrap();
        if let Err(e) = self.segment.seek(SeekFrom::Start(cur_pos as u64)) {
            return Some(Err(e.into()));
        }

        // read the type and the length fo the entry.
        let mut read_ok = 0;
        let mut lv: [u8; 5] = [0; 5];
        let mut handle = self.segment.take(5);
        match handle.read(&mut lv[..]) {
            Ok(n) => {
                if n == 0 {
                    // EOF
                    return None;
                }
                read_ok += n;
            }
            Err(e) => {
                return Some(Err(e.into()));
            }
        }

        // read entry type and entry length
        let entry_type: WalEntryType = lv[0];
        let length = (&lv[1..5]).get_u32();

        let mut b = vec![0; length as usize];
        b.truncate(length as usize);

        // read the compressed block and decompress it
        let mut handle = self.segment.take(length as u64);
        match handle.read(b.as_mut_slice()) {
            Ok(n) => read_ok += n,
            Err(e) => {
                // We return true here because we want the client code to call read which
                // will return the this error to be handled.
                return Some(Err(e.into())); // TODO: read error
            }
        }

        let data = match snap::raw::Decoder::new().decompress_vec(&b) {
            Ok(v) => v,
            Err(_) => return None,
        };

        let entry = match entry_type {
            WRITE_WAL_ENTRY_TYPE => {
                let mut e = WriteWalEntry::new();
                if e.unmarshal_binary(&data).is_err() {
                    return None;
                }
                Entry::WriteWalEntry(e)
            }
            DELETE_WAL_ENTRY_TYPE => {
                let mut e = DeleteWalEntry::new();
                if let Err(e) = e.unmarshal_binary(&data) {
                    return Some(Err(e));
                }
                Entry::DeleteWalEntry(e)
            }
            DELETE_RANGE_WAL_ENTRY_TYPE => {
                let mut e = DeleteRangeWalEntry::new();
                if let Err(e) = e.unmarshal_binary(&data) {
                    return Some(Err(e));
                }
                Entry::DeleteRangeWalEntry(e)
            }
            DELETE_MULTI_WAL_ENTRY_TYPE => {
                let mut e = DeleteMultiWalEntry::new();
                if e.unmarshal_binary(&data).is_err() {
                    return None;
                }
                Entry::DeleteMultiWalEntry(e)
            }
            REPLACE_MULTI_WAL_ENTRY_TYPE => {
                let mut e = ReplaceMultiWalEntry::new();
                if e.unmarshal_binary(&data).is_err() {
                    return None;
                }
                Entry::ReplaceMultiWalEntry(e)
            }
            _ => unreachable!(),
        };
        // Read and decode of this entry was successful.
        self.pos += read_ok;

        Some(Ok(entry))
    }
}

#[cfg(any(test, feature = "benchmarking"))]
pub mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, mpsc},
    };

    use crate::runtime::global::block_on_global;
    use tokio::sync::oneshot;

    use super::{
        DeleteRangeWalEntry, DeleteWalEntry, VLogIndex, WalEntry, WalSegmentReader, WriteWalEntry,
        *,
    };

    #[test]
    fn test_wal_writer_write_multi_single() {
        // create wal test dir
        let dir = must_temp_wal_dir();

        // create point values
        let p1 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 1,
            max: 10,
            generation: 1,
            seq: 1,
            offset: 1,
            size: 10,
        };
        let p2 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 2,
            max: 20,
            generation: 1,
            seq: 1,
            offset: 2,
            size: 20,
        };
        let p3 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 3,
            max: 30,
            generation: 1,
            seq: 1,
            offset: 3,
            size: 30,
        };
        let p4 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 4,
            max: 40,
            generation: 1,
            seq: 1,
            offset: 4,
            size: 40,
        };
        let p5 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 5,
            max: 50,
            generation: 1,
            seq: 1,
            offset: 5,
            size: 50,
        };

        let values: HashMap<String, Vec<VLogIndex>> = HashMap::from([
            ("cpu,host=A#!~#field1".to_string(), vec![p1]),
            ("cpu,host=A#!~#field2".to_string(), vec![p2]),
            ("cpu,host=A#!~#field3".to_string(), vec![p3]),
            ("cpu,host=A#!~#field4".to_string(), vec![p4]),
            ("cpu,host=A#!~#field5".to_string(), vec![p5]),
        ]);

        // create segment writer
        let f = must_write_temp_file(dir.path());
        let mut w = WalSegmentWriter::new(
            // Arc::new(dir.path().into()),
            Arc::new(dir.path().into()),
            10 * 1024 * 1024,
            0,
            BufWriter::new(f),
            Arc::new(crossbeam_channel::bounded(1024)),
            0, // the new segment writer's current pos is 0
        );

        let mut entry = WriteWalEntry {
            values: values.clone(),
            size: 0, // size will be updated in marshal_size
        };

        let stats = Arc::new(WalStatistics::default());

        let compressed = must_marshal_entry(&mut entry);
        assert!(
            w.write_entry(
                WRITE_WAL_ENTRY_TYPE,
                compressed.as_slice(),
                Arc::clone(&stats)
            )
            .is_ok()
        );

        // don't test channel result, because we just test write/read from file.
        // assert!(matches!(sync_rx.recv().unwrap(), None));  // write success return Some(None)

        w.sync();
        drop(w);

        // create segment reader
        let f = must_read_temp_file(dir.path());
        let mut r = WalSegmentReader::new(&f);

        let e = r.next();
        assert!(e.is_some());

        // convert trait object to struct
        let e = e.unwrap();
        let e = match e {
            Ok(Entry::WriteWalEntry(e)) => e,
            _ => panic!("invalid wal entry: {:?}", e),
        };

        for (k, v) in &e.values {
            for (i, vv) in v.iter().enumerate() {
                assert_eq!(vv.to_string(), values[k][i].to_string());
            }
        }

        assert_eq!(
            must_read_file_size(&dir.path().join("00001.wal")),
            r.read_bytes_count() as i64
        );
    }

    #[test]
    fn test_wal_writer_write_multi_large_batch() {
        // create wal test dir
        let dir = must_temp_wal_dir();

        // create point values
        let mut points = Vec::<VLogIndex>::new();
        let mut i = 0;
        while i < 100000 {
            points.push(VLogIndex {
                aggregate: None,
                count: 1,
                typ: 1,
                min: i,
                max: i + 10,
                generation: i,
                seq: i,
                offset: i,
                size: i,
            });
            i += 1;
        }
        let values: HashMap<String, Vec<VLogIndex>> = HashMap::from([
            (
                "cpu,host=A,server=01,foo=bar,tag=really-long#!~#field1".to_string(),
                points.clone(),
            ),
            (
                "mem,host=A,server=01,foo=bar,tag=really-long#!~#field1".to_string(),
                points.clone(),
            ),
        ]);

        // create segment writer
        let f = must_write_temp_file(dir.path());
        let mut w = WalSegmentWriter::new(
            // Arc::new(dir.path().into()),
            Arc::new(dir.path().into()),
            10 * 1024 * 1024,
            0,
            BufWriter::new(f),
            Arc::new(crossbeam_channel::bounded(1024)),
            0, // the new segment writer's current pos is 0
        );

        let mut entry = WriteWalEntry {
            values: values.clone(),
            size: 0, // size will be updated in marshal_size
        };

        let stats = Arc::new(WalStatistics::default());
        let compressed = must_marshal_entry(&mut entry);
        assert!(
            w.write_entry(
                WRITE_WAL_ENTRY_TYPE,
                compressed.as_slice(),
                Arc::clone(&stats)
            )
            .is_ok()
        );

        // don't test channel result, because we just test write/read from file.
        // assert!(matches!(sync_rx.recv().unwrap(), None));  // write success return Some(None)

        w.sync();
        drop(w);

        // create segment reader
        let f = must_read_temp_file(dir.path());
        let mut r = WalSegmentReader::new(&f);

        let e = r.next();
        assert!(e.is_some());

        // convert trait object to struct
        let e = e.unwrap();
        let e = match e {
            Ok(Entry::WriteWalEntry(e)) => e,
            _ => panic!("invalid wal entry: {:?}", e),
        };

        for (k, v) in &e.values {
            for (i, vv) in v.iter().enumerate() {
                assert_eq!(vv.to_string(), values[k][i].to_string());
            }
        }

        assert_eq!(
            must_read_file_size(&dir.path().join("00001.wal")),
            r.read_bytes_count() as i64
        );
    }

    #[test]
    fn test_wal_writer_write_multi_multiple() {
        // create wal test dir
        let dir = must_temp_wal_dir();

        // create point values
        let p1 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 1,
            max: 10,
            generation: 1,
            seq: 1,
            offset: 1,
            size: 10,
        };
        let p2 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 1,
            max: 10,
            generation: 1,
            seq: 1,
            offset: 1,
            size: 10,
        };

        let exp: HashMap<String, Vec<VLogIndex>> = HashMap::from([
            (
                "cpu,host=A,server=01,foo=bar,tag=really-long#!~#field1".to_string(),
                vec![p1],
            ),
            (
                "mem,host=A,server=01,foo=bar,tag=really-long#!~#field1".to_string(),
                vec![p2],
            ),
        ]);

        // create segment writer
        let f = must_write_temp_file(dir.path());
        let mut w = WalSegmentWriter::new(
            // Arc::new(dir.path().into()),
            Arc::new(dir.path().into()),
            10 * 1024 * 1024,
            0,
            BufWriter::new(f),
            Arc::new(crossbeam_channel::bounded(1024)),
            0, // the new segment writer's current pos is 0
        );

        let stats = Arc::new(WalStatistics::default());
        for (k, v) in exp.iter() {
            let mut entry = WriteWalEntry {
                values: HashMap::from([(k.clone(), v.clone())]),
                size: 0, // size will be updated in marshal_size
            };
            let compressed = must_marshal_entry(&mut entry);
            assert!(
                w.write_entry(
                    WRITE_WAL_ENTRY_TYPE,
                    compressed.as_slice(),
                    Arc::clone(&stats)
                )
                .is_ok()
            );

            w.sync();
        }

        drop(w);

        // create segment reader
        let f = must_read_temp_file(dir.path());
        let mut r = WalSegmentReader::new(&f);

        for (ep_k, ep) in exp.iter() {
            let e = r.next();
            assert!(e.is_some());

            // convert trait object to struct
            let e = e.unwrap();
            let e = match e {
                Ok(Entry::WriteWalEntry(e)) => e,
                _ => panic!("invalid wal entry: {:?}", e),
            };

            for (k, v) in &e.values {
                // assert key match
                assert_eq!(k, ep_k);
                // assert values length match
                assert_eq!(v.len(), ep.len());
                // assert value is equal
                for (i, vv) in v.iter().enumerate() {
                    assert_eq!(vv.to_string(), ep[i].to_string());
                }
            }
        }

        // check segment reader bytes count is equal to file read bytes count
        assert_eq!(
            must_read_file_size(&dir.path().join("00001.wal")),
            r.read_bytes_count() as i64
        );
    }

    #[test]
    fn test_wal_writer_write_delete_single() {
        // create wal test dir
        let dir = must_temp_wal_dir();

        let mut entry = DeleteWalEntry {
            keys: vec!["cpu".as_bytes().to_vec()],
            size: 0, // size will be updated in marshal_size
        };

        // create segment writer
        let f = must_write_temp_file(dir.path());
        let mut w = WalSegmentWriter::new(
            // Arc::new(dir.path().into()),
            Arc::new(dir.path().into()),
            10 * 1024 * 1024,
            0,
            BufWriter::new(f),
            Arc::new(crossbeam_channel::bounded(1024)),
            0, // the new segment writer's current pos is 0
        );

        let stats = Arc::new(WalStatistics::default());
        let compressed = must_marshal_entry(&mut entry);
        assert!(
            w.write_entry(
                DELETE_WAL_ENTRY_TYPE,
                compressed.as_slice(),
                Arc::clone(&stats)
            )
            .is_ok()
        );

        // don't test channel result, because we just test write/read from file.
        // assert!(matches!(sync_rx.recv().unwrap(), None));  // write success return Some(None)

        w.sync();
        drop(w);

        // create segment reader
        let f = must_read_temp_file(dir.path());
        let mut r = WalSegmentReader::new(&f);

        let e = r.next();
        assert!(e.is_some());

        // convert trait object to struct
        let e = e.unwrap();
        let e = match e {
            Ok(Entry::DeleteWalEntry(e)) => e,
            _ => panic!("invalid wal entry: {:?}", e),
        };

        // check if keys len is equal
        assert_eq!(e.keys.len(), entry.keys.len());
        // check if key is match
        assert_eq!(e.keys[0], entry.keys[0]);

        assert_eq!(
            must_read_file_size(&dir.path().join("00001.wal")),
            r.read_bytes_count() as i64
        );
    }

    #[test]
    fn test_wal_writer_write_multi_delete_multiple() {
        // create wal test dir
        let dir = must_temp_wal_dir();

        // create point values
        let p1 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 1,
            max: 10,
            generation: 1,
            seq: 1,
            offset: 1,
            size: 10,
        };

        let values: HashMap<String, Vec<VLogIndex>> =
            HashMap::from([("cpu,host=A#!~#field1".to_string(), vec![p1])]);

        let mut write_entry = WriteWalEntry {
            values: values.clone(),
            size: 0, // size will be updated in marshal_size
        };

        // create segment writer
        let f = must_write_temp_file(dir.path());
        let mut w = WalSegmentWriter::new(
            // Arc::new(dir.path().into()),
            Arc::new(dir.path().into()),
            10 * 1024 * 1024,
            0,
            BufWriter::new(f),
            Arc::new(crossbeam_channel::bounded(1024)),
            0, // the new segment writer's current pos is 0
        );

        let stats = Arc::new(WalStatistics::default());
        let compressed = must_marshal_entry(&mut write_entry);
        assert!(
            w.write_entry(
                WRITE_WAL_ENTRY_TYPE,
                compressed.as_slice(),
                Arc::clone(&stats)
            )
            .is_ok()
        );
        w.sync();

        // Write delete entry
        let mut delete_entry = DeleteWalEntry {
            keys: vec!["cpu,host=A#!~value".as_bytes().to_vec()],
            size: 0, // size will be updated in marshal_size
        };
        let compressed = must_marshal_entry(&mut delete_entry);
        assert!(
            w.write_entry(
                DELETE_WAL_ENTRY_TYPE,
                compressed.as_slice(),
                Arc::clone(&stats)
            )
            .is_ok()
        );
        w.sync();
        drop(w);

        // create segment reader
        let f = must_read_temp_file(dir.path());
        let mut r = WalSegmentReader::new(&f);

        // Read the write points first
        let e = r.next();
        assert!(e.is_some());

        // convert trait object to struct
        let e = e.unwrap();
        let e = match e {
            Ok(Entry::WriteWalEntry(e)) => e,
            _ => panic!("invalid wal entry: {:?}", e),
        };

        for (k, v) in &e.values {
            // assert values length match
            assert_eq!(v.len(), values[k].len());
            // assert value is equal
            for (i, vv) in v.iter().enumerate() {
                assert_eq!(vv.to_string(), values[k][i].to_string());
            }
        }

        // Read the delete points second
        let e = r.next();
        assert!(e.is_some());

        // convert trait object to struct
        let e = e.unwrap();
        let e = match e {
            Ok(Entry::DeleteWalEntry(e)) => e,
            _ => panic!("invalid wal entry: {:?}", e),
        };

        // check if keys len is equal
        assert_eq!(e.keys.len(), delete_entry.keys.len());
        // check if key is match
        assert_eq!(e.keys[0], delete_entry.keys[0]);

        // check segment reader bytes count is equal to file read bytes count
        assert_eq!(
            must_read_file_size(&dir.path().join("00001.wal")),
            r.read_bytes_count() as i64
        );
    }

    #[test]
    fn test_wal_writer_write_multi_delete_range_multiple() {
        // create wal test dir
        let dir = must_temp_wal_dir();

        // create point values
        let p1 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 1,
            max: 10,
            generation: 1,
            seq: 1,
            offset: 1,
            size: 10,
        };
        let p2 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 2,
            max: 20,
            generation: 1,
            seq: 1,
            offset: 2,
            size: 20,
        };
        let p3 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 3,
            max: 30,
            generation: 1,
            seq: 1,
            offset: 3,
            size: 30,
        };

        let values: HashMap<String, Vec<VLogIndex>> =
            HashMap::from([("cpu,host=A#!~#field1".to_string(), vec![p1, p2, p3])]);

        // create segment writer
        let f = must_write_temp_file(dir.path());
        let mut w = WalSegmentWriter::new(
            // Arc::new(dir.path().into()),
            Arc::new(dir.path().into()),
            10 * 1024 * 1024,
            0,
            BufWriter::new(f),
            Arc::new(crossbeam_channel::bounded(1024)),
            0, // the new segment writer's current pos is 0
        );

        let mut write_entry = WriteWalEntry {
            values: values.clone(),
            size: 0, // size will be updated in marshal_size
        };

        let stats = Arc::new(WalStatistics::default());
        let compressed = must_marshal_entry(&mut write_entry);
        assert!(
            w.write_entry(
                WRITE_WAL_ENTRY_TYPE,
                compressed.as_slice(),
                Arc::clone(&stats)
            )
            .is_ok()
        );
        w.sync();

        // Write delete range entry
        let mut delete_entry = DeleteRangeWalEntry {
            keys: vec!["cpu,host=A#!~field1".as_bytes().to_vec()],
            min: 2,
            max: 3,
            size: 0, // size will be updated in marshal_size
        };
        let compressed = must_marshal_entry(&mut delete_entry);
        assert!(
            w.write_entry(
                DELETE_RANGE_WAL_ENTRY_TYPE,
                compressed.as_slice(),
                Arc::clone(&stats)
            )
            .is_ok()
        );
        w.sync();
        drop(w);

        // create segment reader
        let f = must_read_temp_file(dir.path());
        let mut r = WalSegmentReader::new(&f);

        // Read the write points first
        let e = r.next();
        assert!(e.is_some());

        // convert trait object to struct
        let e = e.unwrap();
        let e = match e {
            Ok(Entry::WriteWalEntry(e)) => e,
            _ => panic!("invalid wal entry: {:?}", e),
        };

        for (k, v) in &e.values {
            // assert values length match
            assert_eq!(v.len(), values[k].len());
            // assert value is equal
            for (i, vv) in v.iter().enumerate() {
                assert_eq!(vv.to_string(), values[k][i].to_string());
            }
        }

        // Read the delete points second
        let e = r.next();
        assert!(e.is_some());

        // convert trait object to struct
        let e = e.unwrap();
        let e = match e {
            Ok(Entry::DeleteRangeWalEntry(e)) => e,
            _ => panic!("invalid wal entry: {:?}", e),
        };

        // check if keys len is equal
        assert_eq!(e.keys.len(), delete_entry.keys.len());
        // check if key is match
        assert_eq!(e.keys[0], delete_entry.keys[0]);

        // check segment reader bytes count is equal to file read bytes count
        assert_eq!(
            must_read_file_size(&dir.path().join("00001.wal")),
            r.read_bytes_count() as i64
        );
    }

    #[test]
    fn test_wal_replace_multi_entry_marshal_binary() {
        let old_indexes = HashMap::from([(
            "cpu,host=A#!~#field1".to_string(),
            vec![VLogIndex {
                aggregate: None,
                count: 10,
                typ: 1,
                min: 1,
                max: 10,
                generation: 1,
                seq: 1,
                offset: 100,
                size: 40,
            }],
        )]);
        let new_indexes = HashMap::from([(
            "cpu,host=A#!~#field1".to_string(),
            vec![VLogIndex {
                aggregate: None,
                count: 10,
                typ: 1,
                min: 1,
                max: 10,
                generation: 2,
                seq: 1,
                offset: 200,
                size: 35,
            }],
        )]);

        let mut entry = ReplaceMultiWalEntry {
            old_indexes: old_indexes.clone(),
            new_indexes: new_indexes.clone(),
            version: REPLACE_MULTI_WAL_ENTRY_VERSION,
            mode: REPLACE_MULTI_WAL_ENTRY_DEFAULT_MODE,
            size: 0,
        };
        let b = entry.marshal_binary().unwrap();

        let mut got = ReplaceMultiWalEntry::new();
        got.unmarshal_binary(&b).unwrap();

        assert_eq!(got.version, REPLACE_MULTI_WAL_ENTRY_VERSION);
        assert_eq!(got.mode, REPLACE_MULTI_WAL_ENTRY_DEFAULT_MODE);
        assert_eq!(got.old_indexes, old_indexes);
        assert_eq!(got.new_indexes, new_indexes);
    }

    #[test]
    fn test_wal_writer_replace_multi() {
        let dir = must_temp_wal_dir();
        let old_indexes = HashMap::from([(
            "cpu,host=A#!~#field1".to_string(),
            vec![VLogIndex {
                aggregate: None,
                count: 10,
                typ: 1,
                min: 1,
                max: 10,
                generation: 1,
                seq: 1,
                offset: 100,
                size: 40,
            }],
        )]);
        let new_indexes = HashMap::from([(
            "cpu,host=A#!~#field1".to_string(),
            vec![VLogIndex {
                aggregate: None,
                count: 10,
                typ: 1,
                min: 1,
                max: 10,
                generation: 2,
                seq: 1,
                offset: 200,
                size: 35,
            }],
        )]);

        let f = must_write_temp_file(dir.path());
        let mut w = WalSegmentWriter::new(
            Arc::new(dir.path().into()),
            10 * 1024 * 1024,
            0,
            BufWriter::new(f),
            Arc::new(crossbeam_channel::bounded(1024)),
            0,
        );

        let mut entry = ReplaceMultiWalEntry {
            old_indexes: old_indexes.clone(),
            new_indexes: new_indexes.clone(),
            version: REPLACE_MULTI_WAL_ENTRY_VERSION,
            mode: REPLACE_MULTI_WAL_ENTRY_DEFAULT_MODE,
            size: 0,
        };

        let stats = Arc::new(WalStatistics::default());
        let compressed = must_marshal_entry(&mut entry);
        assert!(
            w.write_entry(
                REPLACE_MULTI_WAL_ENTRY_TYPE,
                compressed.as_slice(),
                Arc::clone(&stats)
            )
            .is_ok()
        );
        w.sync();
        drop(w);

        let f = must_read_temp_file(dir.path());
        let mut r = WalSegmentReader::new(&f);

        let e = r.next();
        assert!(e.is_some());
        let e = match e.unwrap() {
            Ok(Entry::ReplaceMultiWalEntry(e)) => e,
            other => panic!("invalid wal entry: {:?}", other),
        };

        assert_eq!(e.old_indexes, old_indexes);
        assert_eq!(e.new_indexes, new_indexes);
        assert_eq!(
            must_read_file_size(&dir.path().join("00001.wal")),
            r.read_bytes_count() as i64
        );
    }

    #[test]
    fn test_wal_replace_multi_api() {
        let dir = Arc::new(must_temp_wal_dir().into_path());
        let w = WAL::new(Arc::clone(&dir), DEFAULT_SEGMENT_SIZE, Duration::ZERO).unwrap();
        let old_indexes = HashMap::from([(
            "cpu,host=A#!~#field1".to_string(),
            vec![VLogIndex {
                aggregate: None,
                count: 10,
                typ: 1,
                min: 1,
                max: 10,
                generation: 1,
                seq: 1,
                offset: 100,
                size: 40,
            }],
        )]);
        let new_indexes = HashMap::from([(
            "cpu,host=A#!~#field1".to_string(),
            vec![VLogIndex {
                aggregate: None,
                count: 10,
                typ: 1,
                min: 1,
                max: 10,
                generation: 2,
                seq: 1,
                offset: 200,
                size: 35,
            }],
        )]);

        block_on_global(w.replace_multi(old_indexes.clone(), new_indexes.clone())).unwrap();
        w.flush();

        let files = must_files_from_dir(&dir).unwrap();
        assert_eq!(files.len(), 1);
        let f = OpenOptions::new().read(true).open(&files[0]).unwrap();
        let mut r = WalSegmentReader::new(&f);
        let e = match r.next().unwrap() {
            Ok(Entry::ReplaceMultiWalEntry(e)) => e,
            other => panic!("invalid wal entry: {:?}", other),
        };

        assert_eq!(e.old_indexes, old_indexes);
        assert_eq!(e.new_indexes, new_indexes);
        w.close();
    }

    #[test]
    fn test_wal_replace_multi_entry_unmarshal_binary_corrupt() {
        let old_indexes = HashMap::from([(
            "cpu,host=A#!~#field1".to_string(),
            vec![VLogIndex {
                aggregate: None,
                count: 10,
                typ: 1,
                min: 1,
                max: 10,
                generation: 1,
                seq: 1,
                offset: 100,
                size: 40,
            }],
        )]);
        let new_indexes = HashMap::from([(
            "cpu,host=A#!~#field1".to_string(),
            vec![VLogIndex {
                aggregate: None,
                count: 10,
                typ: 1,
                min: 1,
                max: 10,
                generation: 2,
                seq: 1,
                offset: 200,
                size: 35,
            }],
        )]);

        let mut entry = ReplaceMultiWalEntry {
            old_indexes,
            new_indexes,
            version: REPLACE_MULTI_WAL_ENTRY_VERSION,
            mode: REPLACE_MULTI_WAL_ENTRY_DEFAULT_MODE,
            size: 0,
        };
        let b = entry.marshal_binary().unwrap();

        for i in 0..b.len() {
            let truncated = b[..i].to_vec();
            let mut out = ReplaceMultiWalEntry::new();
            if let Err(e) = out.unmarshal_binary(&truncated) {
                assert!(matches!(e, Error::WALCorrupted));
            }
        }
    }

    #[test]
    fn test_wal_closed_segment() {
        // create wal test dir
        let dir = Arc::new(must_temp_wal_dir().into_path());

        let mut w = WAL::new(Arc::clone(&dir), DEFAULT_SEGMENT_SIZE, Duration::ZERO).unwrap();

        let files = w.closed_segments().unwrap();
        assert!(
            files.is_empty(),
            "close segment length mismatch: got {}, exp {}",
            files.len(),
            0
        );

        block_on_global(w.write_multi(HashMap::from([(
            "cpu,host=A#!~#value".to_string(),
            vec![VLogIndex {
                aggregate: None,
                count: 1,
                typ: 1,
                min: 1,
                max: 10,
                generation: 1,
                seq: 1,
                offset: 1,
                size: 1,
            }],
        )])))
        .unwrap();
        w.close();

        // Re-open the WAL
        w = WAL::new(Arc::clone(&dir), DEFAULT_SEGMENT_SIZE, Duration::ZERO).unwrap();
        let files = w.closed_segments().unwrap();
        assert!(
            files.is_empty(),
            "close segment length mismatch: got {}, exp {}",
            files.len(),
            0
        );

        w.close();
    }

    #[test]
    fn test_wal_delete() {
        // create wal test dir
        let dir = Arc::new(must_temp_wal_dir().into_path());

        let mut w = WAL::new(Arc::clone(&dir), DEFAULT_SEGMENT_SIZE, Duration::ZERO).unwrap();

        let files = w.closed_segments().unwrap();
        assert!(
            files.is_empty(),
            "close segment length mismatch: got {}, exp {}",
            files.len(),
            0
        );

        block_on_global(w.delete(vec!["cpu".as_bytes().to_vec()])).unwrap();

        w.close();

        // Re-open the WAL
        w = WAL::new(Arc::clone(&dir), DEFAULT_SEGMENT_SIZE, Duration::ZERO).unwrap();
        let files = w.closed_segments().unwrap();
        assert!(
            files.is_empty(),
            "close segment length mismatch: got {}, exp {}",
            files.len(),
            0
        );

        w.close();
    }

    #[test]
    fn test_wal_writer_corrupt() {
        // create wal test dir
        let dir = must_temp_wal_dir();

        // create segment writer
        let f = must_write_temp_file(dir.path());
        let mut w = WalSegmentWriter::new(
            // Arc::new(dir.path().into()),
            Arc::new(dir.path().into()),
            10 * 1024 * 1024,
            0,
            BufWriter::new(f),
            Arc::new(crossbeam_channel::bounded(1024)),
            0, // the new segment writer's current pos is 0
        );

        // Write correct points
        let p1 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 1,
            max: 10,
            generation: 1,
            seq: 1,
            offset: 1,
            size: 10,
        };
        let values: HashMap<String, Vec<VLogIndex>> =
            HashMap::from([("cpu,host=A#!~#float".to_string(), vec![p1])]);

        let mut entry = WriteWalEntry {
            values,
            size: 0, // size will be updated in marshal_size
        };

        let stats = Arc::new(WalStatistics::default());
        let compressed = must_marshal_entry(&mut entry);
        assert!(
            w.write_entry(
                WRITE_WAL_ENTRY_TYPE,
                compressed.as_slice(),
                Arc::clone(&stats)
            )
            .is_ok()
        );
        w.sync();
        drop(w);

        // Write some random bytes to the file to simulate corruption.
        let corruption: [u8; 5] = [1, 4, 0, 0, 0];
        // use File to write corruption
        let mut f = must_write_temp_file(dir.path());
        assert!(f.seek(SeekFrom::End(0)).is_ok());
        assert!(f.write_all(&corruption).is_ok());

        // create segment reader
        let f = must_read_temp_file(dir.path());
        let mut r = WalSegmentReader::new(&f);

        // Try to decode two entries.
        // decode the write points first
        let e = r.next();
        assert!(e.is_some());
        // convert trait object to struct
        let e = e.unwrap();
        let e = match e {
            Ok(Entry::WriteWalEntry(e)) => e,
            _ => panic!("invalid wal entry: {:?}", e),
        };

        // decode the corruption second
        let e = r.next();
        assert!(e.is_none()); // when try to decode the second entry, it will failed to decode,
        // because the second entry is really a bad entry

        // read_bytes_count should only return size of valid data.
        let read_bytes_count = r.read_bytes_count() as i64;
        let exp_bytes_count =
            must_read_file_size(&dir.path().join("00001.wal")) - corruption.len() as i64;
        assert_eq!(
            read_bytes_count, exp_bytes_count,
            "wrong count of bytes read, got {}, exp {}",
            read_bytes_count, exp_bytes_count
        );
    }

    #[test]
    fn test_wal_segment_reader_corrupt() {
        // create wal test dir
        let dir = must_temp_wal_dir();

        // create segment writer
        let f = must_write_temp_file(dir.path());
        let mut w = WalSegmentWriter::new(
            // Arc::new(dir.path().into()),
            Arc::new(dir.path().into()),
            10 * 1024 * 1024,
            0,
            BufWriter::new(f),
            Arc::new(crossbeam_channel::bounded(1024)),
            0, // the new segment writer's current pos is 0
        );

        // Write correct points
        let p4 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 1,
            max: 10,
            generation: 1,
            seq: 1,
            offset: 1,
            size: 10,
        };
        let values: HashMap<String, Vec<VLogIndex>> =
            HashMap::from([("cpu,host=A#!~#field".to_string(), vec![p4.clone(), p4])]);

        let mut entry = WriteWalEntry {
            values: values.clone(),
            size: 0, // size will be updated in marshal_size
        };

        let stats = Arc::new(WalStatistics::default());
        let mut compressed = must_marshal_entry(&mut entry);
        // This causes the nvals field to overflow on 32 bit systems which produces a
        // negative count and a panic when reading the segment.
        compressed[25] = 255;

        assert!(
            w.write_entry(
                WRITE_WAL_ENTRY_TYPE,
                compressed.as_slice(),
                Arc::clone(&stats)
            )
            .is_ok()
        );
        w.sync();
        drop(w);

        // Write some random bytes to the file to simulate corruption.
        let corruption: [u8; 5] = [1, 4, 0, 0, 0];
        // use File to write corruption
        let mut f = must_write_temp_file(dir.path());
        assert!(f.seek(SeekFrom::End(0)).is_ok());
        assert!(f.write_all(&corruption).is_ok());

        // create segment reader
        let f = must_read_temp_file(dir.path());
        let r = WalSegmentReader::new(&f);

        // Try to decode two entries.
        // when iter the first entry, snappy.decode will failed because of compressed[25] = 255; but the
        // next() will return None, so here will never come into the for loop.
        for e in r {
            // segment reader has Impl Iterator, so r is an iterator.
            // we can use for loop to enumerate the entries in the reader
            let e = match e {
                Ok(Entry::WriteWalEntry(e)) => e,
                _ => panic!("invalid wal entry: {:?}", e),
            };
            for (k, v) in &e.values {
                // assert values length match
                assert_eq!(v.len(), values[k].len());
                // assert value is equal
                for (i, vv) in v.iter().enumerate() {
                    assert_eq!(vv.to_string(), values[k][i].to_string());
                }
            }
        }
    }

    #[test]
    fn test_wal_roll_segment() {
        // create wal test dir
        let dir = Arc::new(must_temp_wal_dir().into_path());

        const SEG_SIZE: i64 = 1024;
        let w = WAL::new(Arc::clone(&dir), SEG_SIZE, Duration::ZERO).unwrap();

        let values = HashMap::from([
            (
                "cpu,host=A#!~#field1".to_string(),
                vec![VLogIndex {
                    aggregate: None,
                    count: 1,
                    typ: 1,
                    min: 1,
                    max: 10,
                    generation: 1,
                    seq: 1,
                    offset: 1,
                    size: 10,
                }],
            ),
            (
                "cpu,host=B#!~#field1".to_string(),
                vec![VLogIndex {
                    aggregate: None,
                    count: 1,
                    typ: 1,
                    min: 1,
                    max: 10,
                    generation: 1,
                    seq: 1,
                    offset: 1,
                    size: 10,
                }],
            ),
            (
                "cpu,host=C#!~#field1".to_string(),
                vec![VLogIndex {
                    aggregate: None,
                    count: 1,
                    typ: 1,
                    min: 1,
                    max: 10,
                    generation: 1,
                    seq: 1,
                    offset: 1,
                    size: 10,
                }],
            ),
        ]);

        block_on_global(w.write_multi(values.clone())).unwrap();
        w.flush();

        let mut files = fs::read_dir(w.path()).unwrap();
        let file_count = files.count();
        assert!(file_count == 1, "unexpected segments size {}", file_count);
        let mut files = fs::read_dir(w.path()).unwrap();
        let encode_size = files.next().unwrap().unwrap().metadata().unwrap().size();
        // write some values, the total size of these values will exceed SEG_SIZE(1024),
        // so roll_segment will be triggered
        for i in 0..100 {
            block_on_global(w.write_multi(values.clone())).unwrap();
        }
        let files = fs::read_dir(w.path()).unwrap();
        for f in files {
            let f = f.unwrap();
            let file_size = f.metadata().unwrap().size();
            assert!(
                file_size <= SEG_SIZE as u64 + encode_size,
                "unexpected segment size {}",
                file_size
            );
        }

        w.close();
    }

    // // TODO:
    // // func TestWAL_DiskSize(t *testing.T) {}

    #[test]
    fn test_wal_write_wal_segment_unmarshal_binary_write_wal_corrupt() {
        // create point values
        let p1 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 1,
            max: 10,
            generation: 1,
            seq: 1,
            offset: 1,
            size: 10,
        };
        let p2 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 1,
            max: 10,
            generation: 1,
            seq: 1,
            offset: 1,
            size: 10,
        };
        let p3 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 1,
            max: 10,
            generation: 1,
            seq: 1,
            offset: 1,
            size: 10,
        };
        let p4 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 1,
            max: 10,
            generation: 1,
            seq: 1,
            offset: 1,
            size: 10,
        };
        let p5 = VLogIndex {
            aggregate: None,
            count: 1,
            typ: 1,
            min: 1,
            max: 10,
            generation: 1,
            seq: 1,
            offset: 1,
            size: 10,
        };

        let values: HashMap<String, Vec<VLogIndex>> = HashMap::from([
            ("cpu,host=A#!~#field1".to_string(), vec![p1]),
            ("cpu,host=A#!~#field2".to_string(), vec![p2]),
            ("cpu,host=A#!~#field3".to_string(), vec![p3]),
            ("cpu,host=A#!~#field4".to_string(), vec![p4]),
            ("cpu,host=A#!~#field5".to_string(), vec![p5]),
        ]);

        let mut entry = WriteWalEntry {
            values,
            size: 0, // size will be updated in marshal_size
        };

        let b = entry.marshal_binary();
        assert!(b.is_ok(), "marshal binary error");

        // Test every possible truncation of a write WAL entry
        let mut i = 0;
        let b = b.unwrap();
        while i < b.len() {
            let mut truncated = vec![0u8; i];
            truncated.copy_from_slice(&b[..i]);

            // unmarshal binary failed, the error must be WALCorrupted
            if let Err(e) = entry.unmarshal_binary(&truncated) {
                assert!(matches!(e, Error::WALCorrupted));
            }

            i += 1;
        }
    }

    #[test]
    fn test_wal_delete_wal_entry_unmarshal_binary() {
        struct Example {
            input: Vec<Vec<u8>>,
            output: Vec<Vec<u8>>,
        }

        let examples = vec![
            Example {
                input: vec![vec![]],
                output: vec![],
            },
            Example {
                input: vec!["foo".as_bytes().to_vec()],
                output: vec!["foo".as_bytes().to_vec()],
            },
            Example {
                input: vec!["foo".as_bytes().to_vec(), "bar".as_bytes().to_vec()],
                output: vec!["foo".as_bytes().to_vec(), "bar".as_bytes().to_vec()],
            },
            Example {
                input: vec![
                    "foo".as_bytes().to_vec(),
                    "bar".as_bytes().to_vec(),
                    "z".as_bytes().to_vec(),
                    "abc".as_bytes().to_vec(),
                ],
                output: vec![
                    "foo".as_bytes().to_vec(),
                    "bar".as_bytes().to_vec(),
                    "z".as_bytes().to_vec(),
                    "abc".as_bytes().to_vec(),
                ],
            },
            Example {
                input: vec![
                    "foo".as_bytes().to_vec(),
                    "bar".as_bytes().to_vec(),
                    "z".as_bytes().to_vec(),
                    "a".as_bytes().to_vec(),
                ],
                output: vec![
                    "foo".as_bytes().to_vec(),
                    "bar".as_bytes().to_vec(),
                    "z".as_bytes().to_vec(),
                    "a".as_bytes().to_vec(),
                ],
            },
        ];

        for example in examples.iter() {
            let mut entry = DeleteWalEntry {
                keys: example.input.clone(),
                size: 0,
            };

            let b = entry.marshal_binary();
            assert!(b.is_ok());

            let b = b.unwrap();

            let mut out = DeleteWalEntry::new();
            assert!(out.unmarshal_binary(&b).is_ok());

            assert_eq!(out.keys, example.output);
        }
    }

    #[test]
    fn test_wal_write_wal_segment_unmarshal_binary_delete_wal_corrupt() {
        let mut entry = DeleteWalEntry {
            keys: vec!["foo".as_bytes().to_vec(), "bar".as_bytes().to_vec()],
            size: 0,
        };

        let b = entry.marshal_binary();
        assert!(b.is_ok());

        let b = b.unwrap();
        let mut i = 0;
        while i < b.len() {
            // re-allocated to ensure capacity would be exceed if slicing
            let mut truncated = vec![0u8; i];
            truncated.copy_from_slice(&b[..i]);

            // unmarshal binary failed, the error must be WALCorrupted
            if let Err(e) = entry.unmarshal_binary(&truncated) {
                matches!(e, Error::WALCorrupted);
            }

            i += 1;
        }
    }

    #[test]
    fn test_wal_write_wal_segment_unmarshal_binary_delete_range_wal_corrupt() {
        let mut entry = DeleteRangeWalEntry {
            keys: vec!["foo".as_bytes().to_vec(), "bar".as_bytes().to_vec()],
            min: 1,
            max: 2,
            size: 0,
        };

        let b = entry.marshal_binary();
        assert!(b.is_ok());

        let b = b.unwrap();
        let mut i = 0;
        while i < b.len() {
            // re-allocated to ensure capacity would be exceed if slicing
            let mut truncated = vec![0u8; i];
            truncated.copy_from_slice(&b[..i]);

            // unmarshal binary failed, the error must be WALCorrupted
            if let Err(e) = entry.unmarshal_binary(&truncated) {
                assert!(matches!(e, Error::WALCorrupted));
            }

            i += 1;
        }
    }

    // ////////// wal test helper functions ////////////
    #[cfg(not(target_os = "windows"))]
    use std::os::unix::fs::MetadataExt; // metadata.size
    #[cfg(target_os = "windows")]
    use std::os::windows::fs::MetadataExt;
    use std::{
        fs::{self, File, OpenOptions},
        io,
        path::Path,
    };

    use tempfile::TempDir;

    pub fn must_temp_wal_dir() -> TempDir {
        tempfile::TempDir::with_prefix("wal-test")
            .expect("unable to create wal temporary working directory")
    }

    pub fn must_files_from_dir(dir: &Path) -> io::Result<Vec<String>> {
        let mut files: Vec<String> = fs::read_dir(dir)?
            .flat_map(|res| -> io::Result<_> { Ok(res?.path()) })
            .filter(|path| path.is_file() && path.extension() == Some("wal".as_ref()))
            .flat_map(|path| path.into_os_string().into_string())
            .collect();

        files.sort();

        Ok(files)
    }

    pub fn must_write_temp_file(dir: &Path) -> File {
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(dir.join("00001.wal"))
            .expect("unable to create temporary file")
    }

    pub fn must_read_temp_file(dir: &Path) -> File {
        OpenOptions::new()
            .read(true)
            .open(dir.join("00001.wal"))
            .expect("unable to create temporary file")
    }

    pub fn must_read_file_size(path: &Path) -> i64 {
        let stat = fs::metadata(path)
            .unwrap_or_else(|_| panic!("read `{}` meta failed.", path.to_str().unwrap()));
        stat.size() as i64
    }

    pub fn must_marshal_entry(entry: &mut impl WalEntry) -> Vec<u8> {
        let bytes = entry.encode().unwrap(); // encode entry into bytes

        snap::raw::Encoder::new()
            .compress_vec(&bytes)
            .expect("snap compress failed")
    }
}

#[cfg(test)]
mod aggregate_locator_tests {
    use super::*;
    use crate::storage::tsm1::vlog::{aggregate_record::AggregateRecord, index::{AggValues, SeqNo}};
    use crate::models::points::{Value, IntegerValue, UnsignedValue, FloatValue, BoolValue, StringValue};

    #[test]
    fn native_aism_wal_preserves_every_aggregate_and_plain_locator_encoding() {
        let groups = vec![
            vec![Value::IntegerValue(IntegerValue::new(1, i64::MIN)), Value::IntegerValue(IntegerValue::new(2, i64::MAX))],
            vec![Value::UnsignedValue(UnsignedValue::new(1, u64::MAX)), Value::UnsignedValue(UnsignedValue::new(2, u64::MAX))],
            vec![Value::FloatValue(FloatValue::new(1, -0.0)), Value::FloatValue(FloatValue::new(2, f64::NAN))],
            vec![Value::BoolValue(BoolValue::new(1, true)), Value::BoolValue(BoolValue::new(2, false))],
            vec![Value::StringValue(StringValue::new(1, "a".into())), Value::StringValue(StringValue::new(2, "b".into()))],
        ];
        for values in groups {
            let summary = AggValues::from_block(&values).unwrap();
            let plain = VLogIndex { count: 2, typ: summary.field_type(), min: 1, max: 2,
                generation: 1, seq: 1, offset: 5, size: 30, aggregate: None };
            let mut enriched = plain.clone();
            enriched.aggregate = Some(Arc::new(AggregateRecord { file: 257,
                seq: SeqNo::new(1, 1).unwrap(), version: 17, values: summary }));
            let mut entry = WriteWalEntry { values: HashMap::from([("cpu#!~#v".into(), vec![plain.clone(), enriched])]), size: 0 };
            let encoded = entry.encode().unwrap();
            let mut decoded = WriteWalEntry::new();
            decoded.unmarshal_binary(&encoded).unwrap();
            assert_eq!(decoded.encode().unwrap(), encoded, "floating-point bits and wide sums must survive replay");
            assert!(decoded.values["cpu#!~#v"][0].aggregate.is_none());
            assert_eq!(decoded.values["cpu#!~#v"][1].aggregate.as_ref().unwrap().version, 17);
            for len in 1..encoded.len() { assert!(WriteWalEntry::new().unmarshal_binary(&encoded[..len]).is_err()); }
            let mut old = WriteWalEntry { values: HashMap::from([("cpu#!~#v".into(), vec![plain])]), size: 0 };
            let old_bytes = old.encode().unwrap();
            assert_eq!(old_bytes[0], VLOG_INDEX_COMPACTION);
            assert_eq!(old_bytes.len(), 7 + "cpu#!~#v".len() + VLogIndex::SIZE);
            let mut old_read = WriteWalEntry::new();
            old_read.unmarshal_binary(&old_bytes).unwrap();
            assert!(old_read.values["cpu#!~#v"][0].aggregate.is_none());
        }
    }
}
