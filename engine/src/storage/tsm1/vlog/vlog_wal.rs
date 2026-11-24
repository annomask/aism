//! Write ahead log.
#[cfg(not(target_os = "windows"))]
use std::os::unix::fs::MetadataExt;
#[cfg(target_os = "windows")]
use std::os::windows::fs::MetadataExt;
use std::{
    collections::{HashMap, HashSet},
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
use crossbeam_channel::{Receiver as CrossbeamReceiver, Sender as CrossbeamSender};
use parking_lot::RwLock;
use snafu::{ResultExt, Snafu, ensure};
use tokio::sync::{
    mpsc,
    oneshot::{self, Receiver as OneshotReceiver, Sender as OneshotSender},
};
use tokio_util::sync::CancellationToken;

use crate::storage::tsm1::wal::{
    self, DELETE_RANGE_WAL_ENTRY_TYPE, DELETE_WAL_ENTRY_TYPE, DeleteRangeWalEntry, DeleteWalEntry,
    Entry, Error as WALError, STAT_WAL_CURRENT_BYTES, STAT_WAL_OLD_BYTES, STAT_WRITE_ERR,
    STAT_WRITE_OK, WRITE_WAL_ENTRY_TYPE, WalEntry, WalEntryType, WalStatistics, WriteOp,
    WriteWalEntry,
};

const DEFAULT_WAL_WRITER_NUM: i32 = 8;

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

// const INTEGER_ENTRY_TYPE: u8 = 1;
// const UNSIGNED_ENTRY_TYPE: u8 = 2;
// const FLOAT64_ENTRY_TYPE: u8 = 3;
// const BOOL_ENTRY_TYPE: u8 = 4;
// const STRING_ENTRY_TYPE: u8 = 5;

// // WalEntryType is a u8 written to a wal segment file that indicates what the following compressed block contains.
// pub type WalEntryType = u8;

// // WRITE_WAL_ENTRY_TYPE indicates a write entry.
// pub const WRITE_WAL_ENTRY_TYPE: WalEntryType = 0x01;
// // DELETE_WAL_ENTRY_TYPE indicates a delete entry.
// pub const DELETE_WAL_ENTRY_TYPE: WalEntryType = 0x02;
// // DELETE_RANGE_WAL_ENTRY_TYPE indicates a delete range entry.
// pub const DELETE_RANGE_WAL_ENTRY_TYPE: WalEntryType = 0x03;

// // Statistics gathered by the WAL.
// const STAT_WAL_OLD_BYTES: &str = "old_segments_disk_bytes";
// const STAT_WAL_CURRENT_BYTES: &str = "current_segments_disk_bytes";
// const STAT_WRITE_OK: &str = "write_ok";
// const STAT_WRITE_ERR: &str = "write_err";

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

    /// Snappy compress error.
    #[snafu(display("snap compress failed: {:?}", source))]
    SnapCompress { source: snap::Error },

    /// string write error.
    #[snafu(display("{}", reason))]
    StringErr { reason: String },

    /// The host executor refused to start a WAL worker.
    #[snafu(display("WAL execution error: {}", source))]
    Execution { source: SpawnError },
}

impl From<io::Error> for Error {
    fn from(source: io::Error) -> Error {
        Error::IO { source }
    }
}

/// Result type for WAL.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// WAL represents the write-ahead log used for writing TSM(time series merge tree) files.
/// The wal IO is done in a native thread rather than a tokio task to avoid blocking the tokio runtime.
/// As referenced in this post, continuous long-running IO threads should be off the
/// tokio runtime: `<https://ryhl.io/blog/async-what-is-blocking/>`.
pub struct WAL {
    path: Arc<PathBuf>,
    executor: ExecutorRef,

    // task_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    // io_handle: Mutex<Option<std::thread::JoinHandle<()>>>,
    close_once: Once,
    buffer_tx: Vec<mpsc::Sender<WriteOp>>,
    closing: CancellationToken,

    // buffer_flush_interval: Duration,
    segment_writers: Vec<Arc<RwLock<WalSegmentWriter>>>,

    // Duration to wait before fsyncing writes. A value of 0 (default) will cause every write to be fsync'd,
    // This must be set before the WAL is opened if a non-default value is required.
    sync_delay: Duration,
    sync_count: Arc<AtomicI32>,
    // sync_waiters: Arc<(
    //     CrossbeamSender<OneshotSender<Option<String>>>,
    //     CrossbeamReceiver<OneshotSender<Option<String>>>,
    // )>,

    // file size at which a segment file will be rotated
    pub segment_size: i64,

    // statistics for WAL
    stats: Arc<WalStatistics>, // atomic variable

    count: AtomicI64,
}

impl WAL {
    async fn submit_write_op(
        &self,
        write_op: WriteOp,
        resp_rx: OneshotReceiver<Option<String>>,
    ) -> Result<()> {
        let count = self.count.fetch_add(1, Ordering::SeqCst) as usize;
        if self.buffer_tx[count % self.buffer_tx.len()]
            .send(write_op)
            .await
            .is_err()
        {
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
        writer_num: i32,
        // mut buffer_flush_interval: Duration,
        sync_delay: Duration,
    ) -> Result<Self> {
        Self::new_with_executor(
            path,
            segment_size,
            writer_num,
            sync_delay,
            legacy_global_executor(),
        )
    }

    pub fn new_with_executor(
        path: Arc<PathBuf>,
        segment_size: i64,
        mut writer_num: i32,
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

        let segments = segment_file_names(&path)?; // Vec<(writer_id, Vec<String>)>
        let stats = WalStatistics::default();
        // create current segment writer from last segment file

        if writer_num == 0 {
            writer_num = DEFAULT_WAL_WRITER_NUM;
        }

        let sync_waiters = Arc::new(crossbeam_channel::bounded(BUFFER_CHANNEL_LIMIT));

        let mut total_old_disk_size: i64 = 0;
        let mut segment_writers = vec![];
        if !segments.is_empty() {
            for (_, names) in segments.iter() {
                let sync_waiters = Arc::clone(&sync_waiters);
                let last_segment = names.last().unwrap();
                let (writer_id, segment_id) = id_from_file_name(last_segment)?;
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

                let mut writer = new_segment_writer(
                    &path,
                    BufWriter::new(fd),
                    writer_id,
                    segment_id,
                    segment_size,
                    sync_waiters,
                    stat.size(),
                );
                writer.last_write_time = Time::from_timestamp_nanos(stat.mtime_nsec());
                segment_writers.push(Arc::new(RwLock::new(writer)));
                total_old_disk_size += stat.size() as i64;
                stats
                    .current_bytes
                    .fetch_add(stat.size() as i64, Ordering::SeqCst);
            }
        } else {
            for i in 1..=writer_num {
                let sync_waiters = Arc::clone(&sync_waiters);

                // the first write request arrives after open a new WAL.
                let segment_id = 1;

                stats.old_bytes.fetch_add(0, Ordering::SeqCst);

                // create new writer
                // self.writer = new_log_file(&self.path, self.current_segment_id)?;
                // self.pos = 0;
                let file_name = path.join(format!(
                    "{}{:02}_{:05}.{}",
                    WAL_FILE_PREFIX, i, segment_id, WAL_FILE_EXTENSION
                ));
                let fd = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(file_name)?;

                let mut writer = WalSegmentWriter::new(
                    Arc::clone(&path),
                    segment_size,
                    i,
                    segment_id,
                    BufWriter::new(fd),
                    sync_waiters,
                    0, // the new writer's current pos is 0
                );
                writer.last_write_time = Time::from_timestamp(0, 0);
                segment_writers.push(Arc::new(RwLock::new(writer)));
            }
            stats.current_bytes.store(0, Ordering::SeqCst);
        }

        for writer in segment_writers.iter() {
            let sz = writer.read().current_segment_size();
            if sz > 0 {
                total_old_disk_size -= sz;
            }
        }

        stats.old_bytes.store(total_old_disk_size, Ordering::SeqCst);

        if sync_delay < Duration::ZERO {
            sync_delay = Duration::ZERO;
        }

        let stats = Arc::new(stats);

        let shutdown = CancellationToken::new();
        let sync_count = Arc::new(AtomicI32::new(0));
        let mut buffer_tx = vec![];
        let mut handles = Vec::<TaskHandle<()>>::new();
        for writer in segment_writers.iter() {
            let (io_flush_notify_tx, io_flush_notify_rx) = crossbeam_channel::bounded(1);
            let (tx, rx) = mpsc::channel(BUFFER_CHANNEL_LIMIT);
            buffer_tx.push(tx);

            let sync_waiters = { Arc::clone(&writer.read().sync_waiters) };

            let writer = Arc::clone(writer);
            let stats = Arc::clone(&stats);
            let shutdown = shutdown.clone();
            let sync_count = Arc::clone(&sync_count);
            let worker_executor = Arc::downgrade(&executor);
            let task = match executor.spawn(async move {
                run_wal_op_buffer_task(
                    writer,
                    rx,
                    Arc::new(io_flush_notify_rx),
                    Arc::new(io_flush_notify_tx),
                    sync_delay,
                    sync_count,
                    sync_waiters,
                    stats,
                    shutdown,
                    worker_executor,
                )
                .await;
            }) {
                Ok(task) => task,
                Err(source) => {
                    for task in &handles {
                        task.abort();
                    }
                    return Err(Error::Execution { source });
                }
            };
            handles.push(task);
        }

        Ok(WAL {
            path,
            executor,
            close_once: Once::new(),
            buffer_tx,
            closing: shutdown,
            segment_writers,
            sync_delay,
            sync_count,
            segment_size,
            stats,
            count: AtomicI64::new(0),
        })
    }

    /// path returns the directory the log was initialized with.
    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    /// last_write_time is the last tme anything was written to the WAL.
    pub fn last_write_time(&self) -> Time {
        let mut last_write_time = Time::from_timestamp(0, 0);
        for w in self.segment_writers.iter() {
            let writer = w.read();
            if writer.last_write_time > last_write_time {
                last_write_time = writer.last_write_time;
            }
        }
        last_write_time
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

        self.submit_write_op(write_op, resp_rx).await
    }

    /// Closes the current segment if it is non-empty and opens a new one.
    /// this method will be called when engine write snapshot.
    pub fn close_segment(&self) -> Result<()> {
        for w in self.segment_writers.iter() {
            let mut writer = w.write();
            if writer.current_segment_size() > 0 {
                // if current segment writer is exists, create a new writer with current_segment_id+=1,
                // so all the files whose id<current_segment_id are regarede as closed segment files.
                // before create new segment writer, we must sync current segment file first.
                writer.new_segment_file(Arc::clone(&self.stats))?;
            }
        }

        Ok(())
    }

    /// Returns a slice of the names of the closed segment files.
    pub fn closed_segments(&self) -> Result<Vec<String>> {
        // /////////////////////////////////
        if self.path.as_path().eq(Path::new("")) {
            return Ok(vec![]);
        }

        // Recovered writer IDs can be sparse after committed WAL files are removed.
        // Compare identities instead of treating a persisted ID as a vector index.
        let current_files = self
            .segment_writers
            .iter()
            .map(|writer| writer.read().current_segment_writer_path())
            .collect::<HashSet<_>>();
        let mut closed_files = Vec::<String>::new();
        let segments = segment_file_names(&self.path)?;
        for (_, files) in segments {
            for f in files {
                if current_files.contains(&f) {
                    continue;
                }
                closed_files.push(f);
            }
        }

        Ok(closed_files)
    }

    /// Deletes the given segment file paths from disk and cleans up any associated objects.
    pub fn remove(&self, files: &[String]) -> Result<()> {
        let mut writer_guards = vec![];
        for w in self.segment_writers.iter() {
            let w = w.write();
            writer_guards.push(w);
        }

        for file in files {
            fs::remove_file(file)?;
        }

        // refresh the on-disk size stats
        let segments = segment_file_names(&self.path)?;
        let mut total_old_disk_size: i64 = 0;
        for (_, segs) in &segments {
            for seg in segs {
                let stat = fs::metadata(seg)?;
                if stat.size() > 0 {
                    total_old_disk_size += stat.size() as i64;
                }
            }
        }

        for w in writer_guards.iter() {
            total_old_disk_size -= w.current_segment_size();
        }

        self.stats
            .old_bytes
            .store(total_old_disk_size, Ordering::SeqCst);

        Ok(())
    }

    /// close will finish any flush that is currently in progress and close file handles.
    pub fn close(&self) {
        self.close_once.call_once(|| {
            self.closing.cancel();
            for w in self.segment_writers.iter() {
                let mut w = w.write();
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
            }
        });
    }

    #[cfg(test)]
    pub fn sync(&self) {
        for w in self.segment_writers.iter() {
            let mut w = w.write();
            w.sync();
        }
    }

    #[cfg(test)]
    pub fn flush(&self) {
        for w in self.segment_writers.iter() {
            let mut w = w.write();
            w.flush();
        }
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
                    .send((Some(WALError::VLogWALError { source: e }), Some(resp_tx)))
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
                        Some(WALError::VLogWALError {
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
                    .send((Some(WALError::VLogWALError { source: e }), Some(resp_tx)))
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
                        "VLog WAL task scheduling rejected by host executor".to_string(),
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
                            "VLog WAL blocking task scheduling rejected by host executor".to_string(),
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
                                    .send((Some(WALError::VLogWALError { source: e }), Some(resp_tx)))
                                    .expect("buffer flusher is dead");
                                return;
                            }
                        };

                        {
                            let mut seg_writer = seg_writer.write();
                            // Make sure the log has not been closed
                            if shutdown2.is_cancelled() {
                                buffer_notify
                                .send((Some(WALError::VLogWALError { source: Error::WALClosed }), Some(resp_tx)))
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
                                    .send((Some(WALError::VLogWALError { source: e }), Some(resp_tx)))
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
                                "VLog WAL blocking task scheduling failed"
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
                                    "VLog WAL sync task was cancelled by host executor".to_string(),
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
                                "VLog WAL sync task scheduling failed"
                            );
                        }
                    }
                }) {
                    tracing::error!(
                        target: "reefsdb::tsdb::wal",
                        error = %error,
                        "VLog WAL operation task scheduling failed"
                    );
                }
            },
            _ = shutdown.cancelled() => {
                return;
            }
        }
    }
}

pub(crate) fn segment_file_names(path: &Path) -> Result<Vec<(i32, Vec<String>)>> {
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

    let mut segments = HashMap::<i32, Vec<String>>::new();
    for name in names {
        let (writer_id, _) = id_from_file_name(&name)?;
        segments.entry(writer_id).or_default().push(name);
    }

    let mut names = Vec::<(i32, Vec<String>)>::new();
    for (k, v) in segments {
        for vv in v {
            if let Some((_, segment_names)) = names.iter_mut().find(|(id, _)| *id == k) {
                segment_names.push(vv);
            } else {
                names.push((k, vec![vv]));
            }
        }
    }
    names.sort_by(|a, b| a.0.cmp(&b.0));

    Ok(names)
}

fn id_from_file_name(name: &str) -> Result<(i32, i64)> {
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

    let writer_id = match atoi_simd::parse::<i32>(parts[0].get(1..3).unwrap().as_bytes()) {
        Ok(id) => id,
        Err(e) => {
            return Err(Error::InvalidFileName {
                reason: format!("parse id from {} failed: {:?}", name, e),
            })?;
        }
    };

    let segment_id = match atoi_simd::parse::<i64>(parts[0].get(4..).unwrap().as_bytes()) {
        Ok(id) => id,
        Err(e) => {
            return Err(Error::InvalidFileName {
                reason: format!("parse id from {} failed: {:?}", name, e),
            })?;
        }
    };

    Ok((writer_id, segment_id))
}

pub fn new_segment_writer(
    path: &Path,
    writer: BufWriter<File>,
    writer_id: i32,
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
        writer_id,
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
    writer_id: i32,
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
        writer_id: i32,
        current_segment_id: i64,
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
            writer_id,
            current_segment_id,
            last_write_time: Time::from_timestamp(0, 0),
            sync_waiters,
            writer,
            pos,
        }
    }

    pub fn writer_id(&self) -> i32 {
        self.writer_id
    }

    fn current_segment_writer_path_buf(&self) -> PathBuf {
        self.path.join(format!(
            "{}{:02}_{:05}.{}",
            WAL_FILE_PREFIX, self.writer_id, self.current_segment_id, WAL_FILE_EXTENSION
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
            "{}{:02}_{:05}.{}",
            WAL_FILE_PREFIX, self.writer_id, self.current_segment_id, WAL_FILE_EXTENSION
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

        loop {
            match self.sync_waiters.1.try_recv() {
                Ok(waiter) => {
                    let _ = waiter.send(res.clone());
                    // count += 1;
                }
                Err(_) => break, // Empty or Disconnected
            }
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

#[cfg(any(test, feature = "benchmarking"))]
pub mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, mpsc},
    };

    use crate::runtime::global::block_on_global;
    use tokio::sync::oneshot;

    use super::{DeleteRangeWalEntry, DeleteWalEntry, Value, WalEntry, WriteWalEntry, *};
    use crate::storage::tsm1::wal::WalSegmentReader;

    #[test]
    fn test_wal_writer_write_multi_single() {
        // create wal test dir
        let dir = must_temp_wal_dir();

        // create point values
        let p1 = Value::FloatValue(FloatValue::new(1, 1.1));
        let p2 = Value::IntegerValue(IntegerValue::new(1, 1i64));
        let p3 = Value::BoolValue(BoolValue::new(1, true));
        let p4 = Value::StringValue(StringValue::new(1, "string".to_string()));
        let p5 = Value::UnsignedValue(UnsignedValue::new(1, 1u64));

        let values: HashMap<String, Vec<Value>> = HashMap::from([
            ("cpu,host=A#!~#float".to_string(), vec![p1]),
            ("cpu,host=A#!~#int".to_string(), vec![p2]),
            ("cpu,host=A#!~#bool".to_string(), vec![p3]),
            ("cpu,host=A#!~#string".to_string(), vec![p4]),
            ("cpu,host=A#!~#unsigned".to_string(), vec![p5]),
        ]);

        // create segment writer
        let f = must_write_temp_file(dir.path());
        let mut w = WalSegmentWriter::new(
            // Arc::new(dir.path().into()),
            Arc::new(dir.path().into()),
            10 * 1024 * 1024,
            1,
            1,
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
        let mut points = Vec::<Value>::new();
        let mut i = 0;
        while i < 100000 {
            points.push(Value::IntegerValue(IntegerValue::new(i, 1i64)));
            i += 1;
        }
        let values: HashMap<String, Vec<Value>> = HashMap::from([
            (
                "cpu,host=A,server=01,foo=bar,tag=really-long#!~#float".to_string(),
                points.clone(),
            ),
            (
                "mem,host=A,server=01,foo=bar,tag=really-long#!~#float".to_string(),
                points.clone(),
            ),
        ]);

        // create segment writer
        let f = must_write_temp_file(dir.path());
        let mut w = WalSegmentWriter::new(
            // Arc::new(dir.path().into()),
            Arc::new(dir.path().into()),
            10 * 1024 * 1024,
            1,
            1,
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
        let p1 = Value::FloatValue(FloatValue::new(1, 1.1));
        let p2 = Value::IntegerValue(IntegerValue::new(1, 1i64));

        let exp: HashMap<String, Vec<Value>> = HashMap::from([
            (
                "cpu,host=A,server=01,foo=bar,tag=really-long#!~#float".to_string(),
                vec![p1],
            ),
            (
                "mem,host=A,server=01,foo=bar,tag=really-long#!~#float".to_string(),
                vec![p2],
            ),
        ]);

        // create segment writer
        let f = must_write_temp_file(dir.path());
        let mut w = WalSegmentWriter::new(
            // Arc::new(dir.path().into()),
            Arc::new(dir.path().into()),
            10 * 1024 * 1024,
            1,
            1,
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

    // #[test]
    // fn test_wal_writer_write_delete_single() {
    //     // create wal test dir
    //     let dir = must_temp_wal_dir();

    //     let mut entry = DeleteWalEntry {
    //         keys: vec!["cpu".as_bytes().to_vec()],
    //         size: 0, // size will be updated in marshal_size
    //     };

    //     // create segment writer
    //     let f = must_write_temp_file(dir.path());
    //     let mut w = WalSegmentWriter::new(
    //         // Arc::new(dir.path().into()),
    //         Arc::new(dir.path().into()),
    //         10 * 1024 * 1024,
    //         0,
    //         1,
    //         BufWriter::new(f),
    //         Arc::new(crossbeam_channel::bounded(1024)),
    //         0, // the new segment writer's current pos is 0
    //     );

    //     let stats = Arc::new(WalStatistics::default());
    //     let compressed = must_marshal_entry(&mut entry);
    //     assert!(w
    //         .write_entry(
    //             DELETE_WAL_ENTRY_TYPE,
    //             compressed.as_slice(),
    //             Arc::clone(&stats)
    //         )
    //         .is_ok());

    //     // don't test channel result, because we just test write/read from file.
    //     // assert!(matches!(sync_rx.recv().unwrap(), None));  // write success return Some(None)

    //     w.sync();
    //     drop(w);

    //     // create segment reader
    //     let f = must_read_temp_file(dir.path());
    //     let mut r = WalSegmentReader::new(&f);

    //     let e = r.next();
    //     assert!(e.is_some());

    //     // convert trait object to struct
    //     let e = e.unwrap();
    //     let e = match e {
    //         Ok(Entry::DeleteWalEntry(e)) => e,
    //         _ => panic!("invalid wal entry: {:?}", e),
    //     };

    //     // check if keys len is equal
    //     assert_eq!(e.keys.len(), entry.keys.len());
    //     // check if key is match
    //     assert_eq!(e.keys[0], entry.keys[0]);

    //     assert_eq!(
    //         must_read_file_size(&dir.path().join("00001.wal")),
    //         r.read_bytes_count() as i64
    //     );
    // }

    // #[test]
    // fn test_wal_writer_write_multi_delete_multiple() {
    //     // create wal test dir
    //     let dir = must_temp_wal_dir();

    //     // create point values
    //     let p1 = Value::BoolValue(BoolValue::new(1, true));

    //     let values: HashMap<String, Vec<Value>> =
    //         HashMap::from([("cpu,host=A#!~#value".to_string(), vec![p1])]);

    //     let mut write_entry = WriteWalEntry {
    //         values: values.clone(),
    //         size: 0, // size will be updated in marshal_size
    //     };

    //     // create segment writer
    //     let f = must_write_temp_file(dir.path());
    //     let mut w = WalSegmentWriter::new(
    //         // Arc::new(dir.path().into()),
    //         Arc::new(dir.path().into()),
    //         10 * 1024 * 1024,
    //         0,
    //         1,
    //         BufWriter::new(f),
    //         Arc::new(crossbeam_channel::bounded(1024)),
    //         0, // the new segment writer's current pos is 0
    //     );

    //     let stats = Arc::new(WalStatistics::default());
    //     let compressed = must_marshal_entry(&mut write_entry);
    //     assert!(w
    //         .write_entry(
    //             WRITE_WAL_ENTRY_TYPE,
    //             compressed.as_slice(),
    //             Arc::clone(&stats)
    //         )
    //         .is_ok());
    //     w.sync();

    //     // Write delete entry
    //     let mut delete_entry = DeleteWalEntry {
    //         keys: vec!["cpu,host=A#!~value".as_bytes().to_vec()],
    //         size: 0, // size will be updated in marshal_size
    //     };
    //     let compressed = must_marshal_entry(&mut delete_entry);
    //     assert!(w
    //         .write_entry(
    //             DELETE_WAL_ENTRY_TYPE,
    //             compressed.as_slice(),
    //             Arc::clone(&stats)
    //         )
    //         .is_ok());
    //     w.sync();
    //     drop(w);

    //     // create segment reader
    //     let f = must_read_temp_file(dir.path());
    //     let mut r = WalSegmentReader::new(&f);

    //     // Read the write points first
    //     let e = r.next();
    //     assert!(e.is_some());

    //     // convert trait object to struct
    //     let e = e.unwrap();
    //     let e = match e {
    //         Ok(Entry::WriteWalEntry(e)) => e,
    //         _ => panic!("invalid wal entry: {:?}", e),
    //     };

    //     for (k, v) in &e.values {
    //         // assert values length match
    //         assert_eq!(v.len(), values[k].len());
    //         // assert value is equal
    //         for (i, vv) in v.iter().enumerate() {
    //             assert_eq!(vv.to_string(), values[k][i].to_string());
    //         }
    //     }

    //     // Read the delete points second
    //     let e = r.next();
    //     assert!(e.is_some());

    //     // convert trait object to struct
    //     let e = e.unwrap();
    //     let e = match e {
    //         Ok(Entry::DeleteWalEntry(e)) => e,
    //         _ => panic!("invalid wal entry: {:?}", e),
    //     };

    //     // check if keys len is equal
    //     assert_eq!(e.keys.len(), delete_entry.keys.len());
    //     // check if key is match
    //     assert_eq!(e.keys[0], delete_entry.keys[0]);

    //     // check segment reader bytes count is equal to file read bytes count
    //     assert_eq!(
    //         must_read_file_size(&dir.path().join("00001.wal")),
    //         r.read_bytes_count() as i64
    //     );
    // }

    // #[test]
    // fn test_wal_writer_write_multi_delete_range_multiple() {
    //     // create wal test dir
    //     let dir = must_temp_wal_dir();

    //     // create point values
    //     let p1 = Value::FloatValue(FloatValue::new(1, 1.0));
    //     let p2 = Value::FloatValue(FloatValue::new(2, 2.0));
    //     let p3 = Value::FloatValue(FloatValue::new(3, 3.0));

    //     let values: HashMap<String, Vec<Value>> =
    //         HashMap::from([("cpu,host=A#!~#value".to_string(), vec![p1, p2, p3])]);

    //     // create segment writer
    //     let f = must_write_temp_file(dir.path());
    //     let mut w = WalSegmentWriter::new(
    //         // Arc::new(dir.path().into()),
    //         Arc::new(dir.path().into()),
    //         10 * 1024 * 1024,
    //         0,
    //         1,
    //         BufWriter::new(f),
    //         Arc::new(crossbeam_channel::bounded(1024)),
    //         0, // the new segment writer's current pos is 0
    //     );

    //     let mut write_entry = WriteWalEntry {
    //         values: values.clone(),
    //         size: 0, // size will be updated in marshal_size
    //     };

    //     let stats = Arc::new(WalStatistics::default());
    //     let compressed = must_marshal_entry(&mut write_entry);
    //     assert!(w
    //         .write_entry(
    //             WRITE_WAL_ENTRY_TYPE,
    //             compressed.as_slice(),
    //             Arc::clone(&stats)
    //         )
    //         .is_ok());
    //     w.sync();

    //     // Write delete range entry
    //     let mut delete_entry = DeleteRangeWalEntry {
    //         keys: vec!["cpu,host=A#!~value".as_bytes().to_vec()],
    //         min: 2,
    //         max: 3,
    //         size: 0, // size will be updated in marshal_size
    //     };
    //     let compressed = must_marshal_entry(&mut delete_entry);
    //     assert!(w
    //         .write_entry(
    //             DELETE_RANGE_WAL_ENTRY_TYPE,
    //             compressed.as_slice(),
    //             Arc::clone(&stats)
    //         )
    //         .is_ok());
    //     w.sync();
    //     drop(w);

    //     // create segment reader
    //     let f = must_read_temp_file(dir.path());
    //     let mut r = WalSegmentReader::new(&f);

    //     // Read the write points first
    //     let e = r.next();
    //     assert!(e.is_some());

    //     // convert trait object to struct
    //     let e = e.unwrap();
    //     let e = match e {
    //         Ok(Entry::WriteWalEntry(e)) => e,
    //         _ => panic!("invalid wal entry: {:?}", e),
    //     };

    //     for (k, v) in &e.values {
    //         // assert values length match
    //         assert_eq!(v.len(), values[k].len());
    //         // assert value is equal
    //         for (i, vv) in v.iter().enumerate() {
    //             assert_eq!(vv.to_string(), values[k][i].to_string());
    //         }
    //     }

    //     // Read the delete points second
    //     let e = r.next();
    //     assert!(e.is_some());

    //     // convert trait object to struct
    //     let e = e.unwrap();
    //     let e = match e {
    //         Ok(Entry::DeleteRangeWalEntry(e)) => e,
    //         _ => panic!("invalid wal entry: {:?}", e),
    //     };

    //     // check if keys len is equal
    //     assert_eq!(e.keys.len(), delete_entry.keys.len());
    //     // check if key is match
    //     assert_eq!(e.keys[0], delete_entry.keys[0]);

    //     // check segment reader bytes count is equal to file read bytes count
    //     assert_eq!(
    //         must_read_file_size(&dir.path().join("00001.wal")),
    //         r.read_bytes_count() as i64
    //     );
    // }

    #[test]
    fn closed_segments_with_sparse_recovered_writer_ids() {
        let dir = must_temp_wal_dir();
        let old = dir.path().join("_03_00001.wal");
        let active = dir.path().join("_03_00002.wal");
        fs::write(&old, []).unwrap();
        fs::write(&active, []).unwrap();
        let wal = WAL::new(
            Arc::new(dir.path().to_path_buf()),
            DEFAULT_SEGMENT_SIZE,
            1,
            Duration::ZERO,
        )
        .unwrap();
        assert_eq!(
            wal.closed_segments().unwrap(),
            vec![old.to_string_lossy().to_string()]
        );
        wal.close();
    }

    #[test]
    fn test_wal_closed_segment() {
        // create wal test dir
        let dir = Arc::new(must_temp_wal_dir().into_path());

        let mut w = WAL::new(
            Arc::clone(&dir),
            DEFAULT_SEGMENT_SIZE,
            DEFAULT_WAL_WRITER_NUM,
            Duration::ZERO,
        )
        .unwrap();

        let files = w.closed_segments().unwrap();
        assert!(
            files.is_empty(),
            "close segment length mismatch: got {}, exp {}",
            files.len(),
            0
        );

        block_on_global(w.write_multi(HashMap::from([(
            "cpu,host=A#!~#value".to_string(),
            vec![Value::FloatValue(FloatValue::new(1, 1.0))],
        )])))
        .unwrap();
        w.close();

        // Re-open the WAL
        w = WAL::new(
            Arc::clone(&dir),
            DEFAULT_SEGMENT_SIZE,
            DEFAULT_WAL_WRITER_NUM,
            Duration::ZERO,
        )
        .unwrap();
        let files = w.closed_segments().unwrap();
        assert!(
            files.is_empty(),
            "close segment length mismatch: got {}, exp {}",
            files.len(),
            0
        );

        w.close();
    }

    // #[test]
    // fn test_wal_delete() {
    //     // create wal test dir
    //     let dir = Arc::new(must_temp_wal_dir().into_path());

    //     let mut w = WAL::new(Arc::clone(&dir), DEFAULT_SEGMENT_SIZE, DEFAULT_WAL_WRITER_NUM, Duration::ZERO).unwrap();

    //     let files = w.closed_segments().unwrap();
    //     assert!(
    //         files.is_empty(),
    //         "close segment length mismatch: got {}, exp {}",
    //         files.len(),
    //         0
    //     );

    //     block_on_global(w.delete(vec!["cpu".as_bytes().to_vec()])).unwrap();

    //     w.close();

    //     // Re-open the WAL
    //     w = WAL::new(Arc::clone(&dir), DEFAULT_SEGMENT_SIZE, DEFAULT_WAL_WRITER_NUM, Duration::ZERO).unwrap();
    //     let files = w.closed_segments().unwrap();
    //     assert!(
    //         files.is_empty(),
    //         "close segment length mismatch: got {}, exp {}",
    //         files.len(),
    //         0
    //     );

    //     w.close();
    // }

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
            1,
            1,
            BufWriter::new(f),
            Arc::new(crossbeam_channel::bounded(1024)),
            0, // the new segment writer's current pos is 0
        );

        // Write correct points
        let p1 = Value::FloatValue(FloatValue::new(1, 1.1));
        let values: HashMap<String, Vec<Value>> =
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
            1,
            1,
            BufWriter::new(f),
            Arc::new(crossbeam_channel::bounded(1024)),
            0, // the new segment writer's current pos is 0
        );

        // Write correct points
        let p4 = Value::StringValue(StringValue::new(1, "string".to_string()));
        let values: HashMap<String, Vec<Value>> =
            HashMap::from([("cpu,host=A#!~#string".to_string(), vec![p4.clone(), p4])]);

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

        const WRITER_NUM: i32 = 1;
        const SEG_SIZE: i64 = 1024;
        let w = WAL::new(Arc::clone(&dir), SEG_SIZE, WRITER_NUM, Duration::ZERO).unwrap();

        let values = HashMap::from([
            (
                "cpu,host=A#!~#value".to_string(),
                vec![Value::FloatValue(FloatValue::new(1, 1.0))],
            ),
            (
                "cpu,host=B#!~#value".to_string(),
                vec![Value::FloatValue(FloatValue::new(1, 1.0))],
            ),
            (
                "cpu,host=C#!~#value".to_string(),
                vec![Value::FloatValue(FloatValue::new(1, 1.0))],
            ),
        ]);

        block_on_global(w.write_multi(values.clone())).unwrap();
        w.flush();

        let files = fs::read_dir(w.path()).unwrap();
        let file_count = files.count();
        assert!(
            file_count as i32 == WRITER_NUM,
            "unexpected segments size {}",
            file_count
        );
        let mut files = fs::read_dir(w.path()).unwrap();
        let encode_size = files.next().unwrap().unwrap().metadata().unwrap().size();
        // write some values, the total size of these values will exceed SEG_SIZE(1024),
        // so roll_segment will be triggered
        for i in 0..100 {
            block_on_global(w.write_multi(values.clone())).unwrap();
        }
        let files = fs::read_dir(w.path()).unwrap();
        for f in files {
            let file_size = f.unwrap().metadata().unwrap().size();
            assert!(
                file_size <= SEG_SIZE as u64 + encode_size,
                "unexpected segment size {}",
                file_size
            );
        }

        w.close();
    }

    // TODO:
    // func TestWAL_DiskSize(t *testing.T) {}

    #[test]
    fn test_wal_write_wal_segment_unmarshal_binary_write_wal_corrupt() {
        // create point values
        let p1 = Value::FloatValue(FloatValue::new(1, 1.1));
        let p2 = Value::IntegerValue(IntegerValue::new(1, 1i64));
        let p3 = Value::BoolValue(BoolValue::new(1, true));
        let p4 = Value::StringValue(StringValue::new(1, "string".to_string()));
        let p5 = Value::UnsignedValue(UnsignedValue::new(1, 1u64));

        let values: HashMap<String, Vec<Value>> = HashMap::from([
            ("cpu,host=A#!~#float".to_string(), vec![p1]),
            ("cpu,host=A#!~#int".to_string(), vec![p2]),
            ("cpu,host=A#!~#bool".to_string(), vec![p3]),
            ("cpu,host=A#!~#string".to_string(), vec![p4]),
            ("cpu,host=A#!~#unsigned".to_string(), vec![p5]),
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
                assert!(matches!(e, wal::Error::WALCorrupted));
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
                matches!(e, wal::Error::WALCorrupted);
            }

            i += 1;
        }
    }

    // #[test]
    // fn test_wal_write_wal_segment_unmarshal_binary_delete_range_wal_corrupt() {
    //     let mut entry = DeleteRangeWalEntry {
    //         keys: vec!["foo".as_bytes().to_vec(), "bar".as_bytes().to_vec()],
    //         min: 1,
    //         max: 2,
    //         size: 0,
    //     };

    //     let b = entry.marshal_binary();
    //     assert!(b.is_ok());

    //     let b = b.unwrap();
    //     let mut i = 0;
    //     while i < b.len() {
    //         // re-allocated to ensure capacity would be exceed if slicing
    //         let mut truncated = vec![0u8; i];
    //         truncated.copy_from_slice(&b[..i]);

    //         // unmarshal binary failed, the error must be WALCorrupted
    //         if let Err(e) = entry.unmarshal_binary(&truncated) {
    //             assert!(matches!(e, Error::WALCorrupted));
    //         }

    //         i += 1;
    //     }
    // }

    use crate::common::wait_group::WaitGroup;
    use rand::{Rng, distributions::Alphanumeric};
    #[test]
    fn test_wal_write_multi_large() {
        let dir = Arc::new(must_temp_wal_dir().into_path());
        let mut w = Arc::new(
            WAL::new(
                Arc::clone(&dir),
                DEFAULT_SEGMENT_SIZE,
                DEFAULT_WAL_WRITER_NUM,
                Duration::ZERO,
                // Duration::from_millis(50),
            )
            .unwrap(),
        );

        let sz = 5000;
        let batch = 4000;

        let mut generated = vec![];
        for i in 1..=sz {
            let mut values = HashMap::new();

            let key = format!("cpu,host={:08}", i);
            let mut vals = vec![];
            for j in 1..=batch {
                let val = Value::FloatValue(FloatValue::new(
                    (i * batch + j) as i64 * 1_000_000_000,
                    rand::thread_rng().gen_range(-40.0..1.3e5),
                ));
                vals.push(val);
            }
            values.insert(key, vals);
            generated.push(values);
        }

        let wg = WaitGroup::new();
        for values in generated {
            let wg = wg.clone();
            wg.add(1);
            let w = Arc::clone(&w);
            legacy_global_executor()
                .spawn(async move {
                    w.write_multi(values).await;
                    drop(w);
                    wg.done();
                })
                .unwrap();
        }
        wg.wait();

        w.close();

        // Re-open the WAL
        w = Arc::new(
            WAL::new(
                Arc::clone(&dir),
                DEFAULT_SEGMENT_SIZE,
                DEFAULT_WAL_WRITER_NUM,
                Duration::ZERO,
            )
            .unwrap(),
        );
        let _files = w.closed_segments().unwrap();
        // assert!(
        //     files.is_empty(),
        //     "close segment length mismatch: got {}, exp {}",
        //     files.len(),
        //     0
        // );
        w.close();
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
        let marshal_size = entry.marshal_size();
        let mut bytes = Vec::<u8>::with_capacity(marshal_size);
        entry.encode(&mut bytes).unwrap(); // encode entry into bytes

        snap::raw::Encoder::new()
            .compress_vec(&bytes)
            .expect("snap compress failed")
    }
}
