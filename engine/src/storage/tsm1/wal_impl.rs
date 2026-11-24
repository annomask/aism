#[cfg(not(target_os = "windows"))]
use std::os::unix::fs::MetadataExt;
#[cfg(target_os = "windows")]
use std::os::windows::fs::MetadataExt;
use std::{
    any::Any,
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Once, Weak,
        atomic::{AtomicI32, AtomicI64, Ordering},
    },
    time::Duration,
};

use crate::common::time::Time;
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

use super::wal::{
    self, BUFFER_CHANNEL_LIMIT, DELETE_RANGE_WAL_ENTRY_TYPE, DELETE_WAL_ENTRY_TYPE,
    DeleteRangeWalEntry, DeleteWalEntry, Entry, Error as WALError, STAT_WAL_CURRENT_BYTES,
    STAT_WAL_OLD_BYTES, STAT_WRITE_ERR, STAT_WRITE_OK, WAL_FILE_EXTENSION, WAL_FILE_PREFIX,
    WRITE_WAL_ENTRY_TYPE, WalEntry, WalEntryType, WalStatistics, WriteOp, WriteWalEntry,
};

/// Error type for TSM WAL.
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

    /// Snappy compress error.
    #[snafu(display("snap compress failed: {:?}", source))]
    SnapCompress { source: snap::Error },

    /// string write error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },

    /// The host executor refused to start the WAL worker.
    #[snafu(display("WAL execution error: {}", source))]
    Execution { source: SpawnError },
}

impl From<io::Error> for Error {
    fn from(source: io::Error) -> Error {
        Error::IO { source }
    }
}

/// Result type for TSM WAL.
pub type Result<T, E = Error> = std::result::Result<T, E>;

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
    pub async fn write_multi(&self, values: HashMap<String, Vec<Value>>) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let write_op = WriteOp {
            entry: Box::new(WriteWalEntry {
                values,
                size: 0, // size will be updated in marshal_size
            }),
            resp_tx,
        };

        if self.buffer_tx.send(write_op).await.is_err() {
            self.stats.write_err.fetch_add(1, Ordering::SeqCst);
            return Err(Error::WALClosed);
        }

        match resp_rx.await {
            Err(_) => {
                self.stats.write_err.fetch_add(1, Ordering::SeqCst);
                return Err(Error::WALClosed);
            }
            Ok(None) => {
                let _ = self.stats.write_ok.fetch_add(1, Ordering::SeqCst);
            }
            Ok(Some(e)) => {
                self.stats.write_err.fetch_add(1, Ordering::SeqCst);
                return Err(Error::StringErr { reason: e });
            }
        }

        Ok(())
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

        if self.buffer_tx.send(write_op).await.is_err() {
            self.stats.write_err.fetch_add(1, Ordering::SeqCst);
            return Err(Error::WALClosed);
        }

        match resp_rx.await {
            Err(_) => {
                self.stats.write_err.fetch_add(1, Ordering::SeqCst);
                return Err(Error::WALClosed);
            }
            Ok(None) => {
                let _ = self.stats.write_ok.fetch_add(1, Ordering::SeqCst);
            }
            Ok(Some(e)) => {
                self.stats.write_err.fetch_add(1, Ordering::SeqCst);
                return Err(Error::StringErr { reason: e });
            }
        }

        Ok(())
    }

    // TODO: return the segment id for the operation ?
    /// Deletes the given keys within the given time range.
    pub async fn delete_range(
        &self,
        keys: Vec<Vec<u8>>,
        min_time: i64,
        max_time: i64,
    ) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        let (resp_tx, resp_rx) = oneshot::channel();
        let write_op = WriteOp {
            entry: Box::new(DeleteRangeWalEntry {
                keys,
                min: min_time,
                max: max_time,
                size: 0,
            }),
            resp_tx,
        };

        if self.buffer_tx.send(write_op).await.is_err() {
            self.stats.write_err.fetch_add(1, Ordering::SeqCst);
            return Err(Error::WALClosed);
        }

        match resp_rx.await {
            Err(_) => {
                self.stats.write_err.fetch_add(1, Ordering::SeqCst);
                return Err(Error::WALClosed);
            }
            Ok(None) => {
                let _ = self.stats.write_ok.fetch_add(1, Ordering::SeqCst);
            }
            Ok(Some(e)) => {
                self.stats.write_err.fetch_add(1, Ordering::SeqCst);
                return Err(Error::StringErr { reason: e });
            }
        }

        Ok(())
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

            let mut w = self.current_segment_writer.write();
            if w.current_segment_size() > 0 {
                w.sync();

                // if l.currentSegmentWriter != nil {
                //     l.sync()
                //     l.currentSegmentWriter.close()
                //     l.currentSegmentWriter = nil
                // }
                // FIXME:
                // self.current_segment_writer = None;
            } else {
                let _ = std::fs::remove_file(w.current_segment_writer_path_buf());
            }
        });
    }

    #[cfg(test)]
    pub fn sync(&self) {
        let mut w = self.current_segment_writer.write();
        w.sync();
    }

    #[cfg(test)]
    pub fn flush(&self) {
        let mut w = self.current_segment_writer.write();
        w.flush();
    }
}
fn add_resp_tx(
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
    buffer_notify: CrossbeamSender<(Option<WALError>, Option<OneshotSender<Option<String>>>)>,
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
        let marshal_size = entry.marshal_size();
        let mut bytes = Vec::<u8>::with_capacity(marshal_size);
        if let Err(e) = entry.encode(&mut bytes) {
            // encode entry into bytes
            buffer_notify
                .send((Some(e), Some(resp_tx)))
                .expect("buffer flusher is dead");
            continue;
        }
        // compress the encoded bytes by snappy.
        let compressed = match snap::raw::Encoder::new()
            .compress_vec(bytes.as_slice())
            .context(SnapCompressSnafu)
        {
            Ok(compressed) => compressed,
            Err(e) => {
                buffer_notify
                    .send((
                        Some(super::wal::Error::TSMWALError { source: e }),
                        Some(resp_tx),
                    ))
                    .expect("buffer flusher is dead");
                continue;
            }
        };

        {
            let mut segment_writer = segment_writer.write();
            // Make sure the log has not been closed
            if shutdown.is_cancelled() {
                buffer_notify
                    .send((
                        Some(super::wal::Error::TSMWALError {
                            source: Error::WALClosed,
                        }),
                        Some(resp_tx),
                    ))
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
                    .send((
                        Some(super::wal::Error::TSMWALError { source: e }),
                        Some(resp_tx),
                    ))
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
        CrossbeamReceiver<(Option<WALError>, Option<OneshotSender<Option<String>>>)>,
    >, // get resp from wal_io_thread
    buffer_notify: Arc<CrossbeamSender<(Option<WALError>, Option<OneshotSender<Option<String>>>)>>, // send write log resp
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
                        "WAL task scheduling rejected by host executor".to_string(),
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
                            "WAL blocking task scheduling rejected by host executor".to_string(),
                        ));
                    });
                    let blocking_task = match task_executor.spawn_blocking(move || {
                        let write_op = scopeguard::ScopeGuard::into_inner(rejected_op);
                        let (mut entry, resp_tx) = (write_op.entry, write_op.resp_tx);
                        // limit how many concurrent encodings can be in flight.  Since we can only
                        // write one at a time to disk, a slow disk can cause the allocations below
                        // to increase quickly.  If we're backed up, wait until others have completed.
                        let marshal_size = entry.marshal_size();
                        let mut bytes = Vec::<u8>::with_capacity(marshal_size);
                        if let Err(e) = entry.encode(&mut bytes) {
                            // encode entry into bytes
                            buffer_notify
                                .send((Some(e), Some(resp_tx)))
                                .expect("buffer flusher is dead");
                            return;
                        }
                        // compress the encoded bytes by snappy.
                        let compressed = match snap::raw::Encoder::new()
                            .compress_vec(bytes.as_slice())
                            .context(SnapCompressSnafu)
                        {
                            Ok(compressed) => compressed,
                            Err(e) => {
                                buffer_notify
                                    .send((Some(WALError::TSMWALError { source: e }), Some(resp_tx)))
                                    .expect("buffer flusher is dead");
                                return;
                            }
                        };

                        {
                            let mut seg_writer = seg_writer.write();
                            // Make sure the log has not been closed
                            if shutdown2.is_cancelled() {
                                buffer_notify
                                .send((Some(WALError::TSMWALError { source: Error::WALClosed }), Some(resp_tx)))
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
                                    .send((Some(WALError::TSMWALError { source: e }), Some(resp_tx)))
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
                                "WAL blocking task scheduling failed"
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
                                    "WAL sync task was cancelled by host executor".to_string(),
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
                                "WAL sync task scheduling failed"
                            );
                        }
                    }
                }) {
                    tracing::error!(
                        target: "reefsdb::tsdb::wal",
                        error = %error,
                        "WAL operation task scheduling failed"
                    );
                }
            },
            _ = shutdown.cancelled() => {
                return;
            }
        }
    }
}

pub(crate) fn segment_file_names(path: &Path) -> Result<Vec<String>> {
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

    fn current_segment_writer_path_buf(&self) -> PathBuf {
        self.path.join(format!(
            "{}{:05}.{}",
            WAL_FILE_PREFIX, self.current_segment_id, WAL_FILE_EXTENSION
        ))
    }

    pub fn current_segment_writer_path(&self) -> String {
        self.current_segment_writer_path_buf()
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
