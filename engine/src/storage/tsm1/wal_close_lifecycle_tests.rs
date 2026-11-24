use std::{
    collections::HashMap,
    env,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::{ffi::OsString, os::unix::ffi::OsStringExt};

use crate::models::points::{IntegerValue, Value};
use crate::runtime::execution::{
    BoxBlockingTask, BoxFutureTask, DefaultTokioExecutor, DefaultTokioExecutorOptions, ExecutorRef,
    RuntimeExecutor, SpawnError, TaskController,
};
use crate::storage::tsm1::{
    vlog::{VLogIndex, vlog_index_wal, vlog_wal},
    wal_impl,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(30);
const ISOLATED_TEST_ENV: &str = "REEFSDB_WAL_LIFECYCLE_ISOLATED_TEST";
static ISOLATED_TEST_LOCK: Mutex<()> = Mutex::new(());

fn run_in_isolated_process(test_name: &str) -> bool {
    if env::var(ISOLATED_TEST_ENV).as_deref() == Ok(test_name) {
        return false;
    }

    // These cases were integration tests before crate consolidation, so retain
    // their process-global isolation from the rest of the library test suite.
    let _guard = ISOLATED_TEST_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let module = module_path!()
        .strip_prefix("reefsdb::")
        .unwrap_or(module_path!());
    let status = Command::new(env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg(format!("{module}::{test_name}"))
        .arg("--test-threads=1")
        .env(ISOLATED_TEST_ENV, test_name)
        .status()
        .expect("run isolated WAL lifecycle test");
    assert!(status.success(), "isolated WAL lifecycle test failed");
    true
}

fn test_executor() -> Arc<DefaultTokioExecutor> {
    Arc::new(DefaultTokioExecutor::new(DefaultTokioExecutorOptions::new(4, 8, 2, 4)).unwrap())
}

fn tsm_values(time: i64) -> HashMap<String, Vec<Value>> {
    HashMap::from([(
        "cpu".to_string(),
        vec![Value::IntegerValue(IntegerValue::new(time, time))],
    )])
}

fn large_tsm_values() -> HashMap<String, Vec<Value>> {
    HashMap::from([(
        "cpu".to_string(),
        (0..10_000)
            .map(|time| Value::IntegerValue(IntegerValue::new(time, time)))
            .collect(),
    )])
}

fn vlog_index_values(generation: i64) -> HashMap<String, Vec<VLogIndex>> {
    HashMap::from([(
        "cpu".to_string(),
        vec![VLogIndex {
            count: 1,
            min: generation,
            max: generation,
            generation,
            seq: 1,
            offset: 5,
            size: 16,
            ..Default::default()
        }],
    )])
}

fn large_vlog_index_values() -> HashMap<String, Vec<VLogIndex>> {
    HashMap::from([(
        "cpu".to_string(),
        (0..10_000)
            .map(|generation| VLogIndex {
                count: 1,
                min: generation,
                max: generation,
                generation,
                seq: 1,
                offset: generation + 5,
                size: 16,
                ..Default::default()
            })
            .collect(),
    )])
}

fn new_tsm_wal(path: &Path, executor: &Arc<DefaultTokioExecutor>) -> Arc<wal_impl::WAL> {
    let executor: ExecutorRef = executor.clone();
    new_tsm_wal_with_executor(path, executor)
}

fn new_tsm_wal_with_executor(path: &Path, executor: ExecutorRef) -> Arc<wal_impl::WAL> {
    Arc::new(
        wal_impl::WAL::new_with_executor(
            Arc::new(path.to_path_buf()),
            1 << 20,
            Duration::ZERO,
            executor,
        )
        .unwrap(),
    )
}

fn new_vlog_wal(path: &Path, executor: &Arc<DefaultTokioExecutor>) -> Arc<vlog_wal::WAL> {
    let executor: ExecutorRef = executor.clone();
    new_vlog_wal_with_executor(path, executor)
}

fn new_vlog_wal_with_executor(path: &Path, executor: ExecutorRef) -> Arc<vlog_wal::WAL> {
    new_vlog_wal_with_writer_count(path, executor, 1)
}

fn new_vlog_wal_with_writer_count(
    path: &Path,
    executor: ExecutorRef,
    writer_count: i32,
) -> Arc<vlog_wal::WAL> {
    Arc::new(
        vlog_wal::WAL::new_with_executor(
            Arc::new(path.to_path_buf()),
            1 << 20,
            writer_count,
            Duration::ZERO,
            executor,
        )
        .unwrap(),
    )
}

fn new_vlog_index_wal(
    path: &Path,
    executor: &Arc<DefaultTokioExecutor>,
) -> Arc<vlog_index_wal::WAL> {
    let executor: ExecutorRef = executor.clone();
    new_vlog_index_wal_with_executor(path, executor)
}

fn new_vlog_index_wal_with_executor(
    path: &Path,
    executor: ExecutorRef,
) -> Arc<vlog_index_wal::WAL> {
    Arc::new(
        vlog_index_wal::WAL::new_with_executor(
            Arc::new(path.to_path_buf()),
            1 << 20,
            Duration::ZERO,
            executor,
        )
        .unwrap(),
    )
}

struct ActiveTask(Arc<AtomicUsize>);

impl ActiveTask {
    fn new(active: Arc<AtomicUsize>) -> Self {
        active.fetch_add(1, Ordering::SeqCst);
        Self(active)
    }
}

impl Drop for ActiveTask {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

struct CountingExecutor {
    inner: Arc<DefaultTokioExecutor>,
    active: Arc<AtomicUsize>,
}

impl CountingExecutor {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: test_executor(),
            active: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn wait_for_active(&self, expected: usize) {
        let deadline = Instant::now() + TEST_TIMEOUT;
        while self.active.load(Ordering::SeqCst) != expected {
            assert!(
                Instant::now() < deadline,
                "expected {expected} active WAL tasks, found {}",
                self.active.load(Ordering::SeqCst)
            );
            thread::sleep(Duration::from_millis(1));
        }
    }
}

impl RuntimeExecutor for CountingExecutor {
    fn schedule(&self, task: BoxFutureTask) -> Result<TaskController, SpawnError> {
        let active = ActiveTask::new(Arc::clone(&self.active));
        self.inner.schedule(Box::pin(async move {
            let _active = active;
            task.await;
        }))
    }

    fn schedule_blocking(&self, task: BoxBlockingTask) -> Result<TaskController, SpawnError> {
        let active = ActiveTask::new(Arc::clone(&self.active));
        self.inner.schedule_blocking(Box::new(move || {
            let _active = active;
            task();
        }))
    }

    fn schedule_compact(&self, task: BoxFutureTask) -> Result<TaskController, SpawnError> {
        let active = ActiveTask::new(Arc::clone(&self.active));
        self.inner.schedule_compact(Box::pin(async move {
            let _active = active;
            task.await;
        }))
    }

    fn schedule_blocking_compact(
        &self,
        task: BoxBlockingTask,
    ) -> Result<TaskController, SpawnError> {
        let active = ActiveTask::new(Arc::clone(&self.active));
        self.inner.schedule_blocking_compact(Box::new(move || {
            let _active = active;
            task();
        }))
    }
}

fn tsm_write(
    executor: &DefaultTokioExecutor,
    wal: &wal_impl::WAL,
    time: i64,
) -> wal_impl::Result<()> {
    executor.block_on(async {
        tokio::time::timeout(TEST_TIMEOUT, wal.write_multi(tsm_values(time)))
            .await
            .expect("TSM WAL write timed out")
    })
}

fn vlog_write(
    executor: &DefaultTokioExecutor,
    wal: &vlog_wal::WAL,
    time: i64,
) -> vlog_wal::Result<()> {
    executor.block_on(async {
        tokio::time::timeout(TEST_TIMEOUT, wal.write_multi(tsm_values(time)))
            .await
            .expect("VLog WAL write timed out")
    })
}

fn vlog_index_write(
    executor: &DefaultTokioExecutor,
    wal: &vlog_index_wal::WAL,
    generation: i64,
) -> vlog_index_wal::Result<()> {
    executor.block_on(async {
        tokio::time::timeout(TEST_TIMEOUT, wal.write_multi(vlog_index_values(generation)))
            .await
            .expect("VLog index WAL write timed out")
    })
}

fn close_concurrently<T>(first: Arc<T>, second: Arc<T>, close: fn(&T))
where
    T: Send + Sync + 'static,
{
    let start = Arc::new(Barrier::new(3));
    let (done_tx, done_rx) = mpsc::channel();
    let mut handles = Vec::new();
    for wal in [first, second] {
        let start = Arc::clone(&start);
        let done_tx = done_tx.clone();
        handles.push(thread::spawn(move || {
            start.wait();
            close(&wal);
            done_tx.send(()).unwrap();
        }));
    }

    start.wait();
    for _ in 0..2 {
        done_rx
            .recv_timeout(TEST_TIMEOUT)
            .expect("concurrent WAL close timed out");
    }
    for handle in handles {
        handle.join().unwrap();
    }
}

#[cfg(unix)]
fn non_utf8_wal_path(root: &Path, prefix: &[u8]) -> PathBuf {
    let mut name = prefix.to_vec();
    name.push(0xff);
    root.join(OsString::from_vec(name))
}

#[cfg(unix)]
fn segment_files(path: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.is_file())
        .collect()
}

#[cfg(unix)]
fn only_segment_file(path: &Path) -> PathBuf {
    let mut files = segment_files(path);
    assert_eq!(files.len(), 1, "expected one initial WAL segment");
    files.pop().unwrap()
}

#[cfg(unix)]
#[test]
fn tsm_empty_non_utf8_path_close_removes_segment_without_poisoning() {
    if run_in_isolated_process("tsm_empty_non_utf8_path_close_removes_segment_without_poisoning") {
        return;
    }

    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();
    let path = non_utf8_wal_path(root.path(), b"tsm-");
    let wal = new_tsm_wal(&path, &executor);
    let segment = only_segment_file(&path);

    wal.close();
    assert!(!segment.exists());
    wal.close();
    assert!(tsm_write(&executor, &wal, 1).is_err());
}

#[cfg(unix)]
#[test]
fn vlog_empty_non_utf8_path_close_removes_segment_without_poisoning() {
    if run_in_isolated_process("vlog_empty_non_utf8_path_close_removes_segment_without_poisoning") {
        return;
    }

    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();
    let path = non_utf8_wal_path(root.path(), b"vlog-");
    let executor_ref: ExecutorRef = executor.clone();
    let wal = new_vlog_wal_with_writer_count(&path, executor_ref, 3);
    let segments = segment_files(&path);
    assert_eq!(segments.len(), 3);

    wal.close();
    assert!(segments.iter().all(|segment| !segment.exists()));
    wal.close();
    assert!(vlog_write(&executor, &wal, 1).is_err());
}

#[cfg(unix)]
#[test]
fn same_instance_concurrent_close_is_safe_under_non_utf8_paths() {
    if run_in_isolated_process("same_instance_concurrent_close_is_safe_under_non_utf8_paths") {
        return;
    }

    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();

    let tsm_path = non_utf8_wal_path(root.path(), b"tsm-concurrent-");
    let tsm = new_tsm_wal(&tsm_path, &executor);
    let tsm_segment = only_segment_file(&tsm_path);
    close_concurrently(Arc::clone(&tsm), Arc::clone(&tsm), wal_impl::WAL::close);
    assert!(!tsm_segment.exists());
    assert!(tsm_write(&executor, &tsm, 1).is_err());

    let vlog_path = non_utf8_wal_path(root.path(), b"vlog-concurrent-");
    let vlog = new_vlog_wal(&vlog_path, &executor);
    let vlog_segment = only_segment_file(&vlog_path);
    close_concurrently(Arc::clone(&vlog), Arc::clone(&vlog), vlog_wal::WAL::close);
    assert!(!vlog_segment.exists());
    assert!(vlog_write(&executor, &vlog, 1).is_err());
}

#[cfg(unix)]
#[test]
fn tsm_non_utf8_instances_close_independently() {
    if run_in_isolated_process("tsm_non_utf8_instances_close_independently") {
        return;
    }

    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();
    let first_path = non_utf8_wal_path(root.path(), b"tsm-first-");
    let second_path = non_utf8_wal_path(root.path(), b"tsm-second-");
    let first = new_tsm_wal(&first_path, &executor);
    let second = new_tsm_wal(&second_path, &executor);
    let first_segment = only_segment_file(&first_path);

    first.close();
    assert!(!first_segment.exists());
    tsm_write(&executor, &second, 1).unwrap();
    second.close();
    assert!(tsm_write(&executor, &second, 2).is_err());
}

#[test]
fn tsm_wal_instances_close_independently_in_both_orders() {
    if run_in_isolated_process("tsm_wal_instances_close_independently_in_both_orders") {
        return;
    }

    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();
    let first = new_tsm_wal(&root.path().join("first"), &executor);
    let second = new_tsm_wal(&root.path().join("second"), &executor);

    tsm_write(&executor, &first, 1).unwrap();
    tsm_write(&executor, &second, 2).unwrap();
    first.close();
    tsm_write(&executor, &second, 3).unwrap();
    second.close();
    assert!(tsm_write(&executor, &second, 4).is_err());

    let third = new_tsm_wal(&root.path().join("third"), &executor);
    let fourth = new_tsm_wal(&root.path().join("fourth"), &executor);
    fourth.close();
    tsm_write(&executor, &third, 5).unwrap();
    third.close();
    assert!(tsm_write(&executor, &third, 6).is_err());
}

#[test]
fn vlog_wal_instances_close_independently_in_both_orders() {
    if run_in_isolated_process("vlog_wal_instances_close_independently_in_both_orders") {
        return;
    }

    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();
    let first = new_vlog_wal(&root.path().join("first"), &executor);
    let second = new_vlog_wal(&root.path().join("second"), &executor);

    vlog_write(&executor, &first, 1).unwrap();
    vlog_write(&executor, &second, 2).unwrap();
    first.close();
    vlog_write(&executor, &second, 3).unwrap();
    second.close();
    assert!(vlog_write(&executor, &second, 4).is_err());

    let third = new_vlog_wal(&root.path().join("third"), &executor);
    let fourth = new_vlog_wal(&root.path().join("fourth"), &executor);
    fourth.close();
    vlog_write(&executor, &third, 5).unwrap();
    third.close();
    assert!(vlog_write(&executor, &third, 6).is_err());
}

#[test]
fn vlog_index_wal_instances_close_independently_in_both_orders() {
    if run_in_isolated_process("vlog_index_wal_instances_close_independently_in_both_orders") {
        return;
    }

    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();
    let first = new_vlog_index_wal(&root.path().join("first"), &executor);
    let second = new_vlog_index_wal(&root.path().join("second"), &executor);

    vlog_index_write(&executor, &first, 1).unwrap();
    vlog_index_write(&executor, &second, 2).unwrap();
    first.close();
    vlog_index_write(&executor, &second, 3).unwrap();
    second.close();
    assert!(vlog_index_write(&executor, &second, 4).is_err());

    let third = new_vlog_index_wal(&root.path().join("third"), &executor);
    let fourth = new_vlog_index_wal(&root.path().join("fourth"), &executor);
    fourth.close();
    vlog_index_write(&executor, &third, 5).unwrap();
    third.close();
    assert!(vlog_index_write(&executor, &third, 6).is_err());
}

#[test]
fn repeated_close_is_idempotent_for_each_wal_type() {
    if run_in_isolated_process("repeated_close_is_idempotent_for_each_wal_type") {
        return;
    }

    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();
    let tsm = new_tsm_wal(&root.path().join("tsm"), &executor);
    let vlog = new_vlog_wal(&root.path().join("vlog"), &executor);
    let index = new_vlog_index_wal(&root.path().join("index"), &executor);

    tsm.close();
    tsm.close();
    vlog.close();
    vlog.close();
    index.close();
    index.close();

    assert!(tsm_write(&executor, &tsm, 1).is_err());
    assert!(vlog_write(&executor, &vlog, 1).is_err());
    assert!(vlog_index_write(&executor, &index, 1).is_err());
}

#[test]
fn same_instance_concurrent_close_is_idempotent_for_each_wal_type() {
    if run_in_isolated_process("same_instance_concurrent_close_is_idempotent_for_each_wal_type") {
        return;
    }

    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();
    let tsm = new_tsm_wal(&root.path().join("tsm"), &executor);
    let vlog = new_vlog_wal(&root.path().join("vlog"), &executor);
    let index = new_vlog_index_wal(&root.path().join("index"), &executor);

    close_concurrently(Arc::clone(&tsm), Arc::clone(&tsm), wal_impl::WAL::close);
    close_concurrently(Arc::clone(&vlog), Arc::clone(&vlog), vlog_wal::WAL::close);
    close_concurrently(
        Arc::clone(&index),
        Arc::clone(&index),
        vlog_index_wal::WAL::close,
    );

    assert!(tsm_write(&executor, &tsm, 1).is_err());
    assert!(vlog_write(&executor, &vlog, 1).is_err());
    assert!(vlog_index_write(&executor, &index, 1).is_err());
}

#[test]
fn different_instances_close_concurrently_for_each_wal_type() {
    if run_in_isolated_process("different_instances_close_concurrently_for_each_wal_type") {
        return;
    }

    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();
    let tsm_a = new_tsm_wal(&root.path().join("tsm-a"), &executor);
    let tsm_b = new_tsm_wal(&root.path().join("tsm-b"), &executor);
    let vlog_a = new_vlog_wal(&root.path().join("vlog-a"), &executor);
    let vlog_b = new_vlog_wal(&root.path().join("vlog-b"), &executor);
    let index_a = new_vlog_index_wal(&root.path().join("index-a"), &executor);
    let index_b = new_vlog_index_wal(&root.path().join("index-b"), &executor);

    close_concurrently(Arc::clone(&tsm_a), Arc::clone(&tsm_b), wal_impl::WAL::close);
    close_concurrently(
        Arc::clone(&vlog_a),
        Arc::clone(&vlog_b),
        vlog_wal::WAL::close,
    );
    close_concurrently(
        Arc::clone(&index_a),
        Arc::clone(&index_b),
        vlog_index_wal::WAL::close,
    );

    assert!(tsm_write(&executor, &tsm_a, 1).is_err());
    assert!(tsm_write(&executor, &tsm_b, 1).is_err());
    assert!(vlog_write(&executor, &vlog_a, 1).is_err());
    assert!(vlog_write(&executor, &vlog_b, 1).is_err());
    assert!(vlog_index_write(&executor, &index_a, 1).is_err());
    assert!(vlog_index_write(&executor, &index_b, 1).is_err());
}

#[test]
fn three_tsm_wal_instances_have_independent_close_state() {
    if run_in_isolated_process("three_tsm_wal_instances_have_independent_close_state") {
        return;
    }

    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();
    let first = new_tsm_wal(&root.path().join("first"), &executor);
    let second = new_tsm_wal(&root.path().join("second"), &executor);
    let third = new_tsm_wal(&root.path().join("third"), &executor);

    first.close();
    second.close();
    tsm_write(&executor, &third, 1).unwrap();
    third.close();
    assert!(tsm_write(&executor, &third, 2).is_err());
}

#[test]
fn reopened_wal_instances_receive_fresh_close_state() {
    if run_in_isolated_process("reopened_wal_instances_receive_fresh_close_state") {
        return;
    }

    let root = tempfile::tempdir().unwrap();
    let executor = test_executor();

    let tsm_path = root.path().join("tsm");
    let tsm = new_tsm_wal(&tsm_path, &executor);
    tsm_write(&executor, &tsm, 1).unwrap();
    tsm.close();
    drop(tsm);
    let reopened_tsm = new_tsm_wal(&tsm_path, &executor);
    tsm_write(&executor, &reopened_tsm, 2).unwrap();
    reopened_tsm.close();
    assert!(tsm_write(&executor, &reopened_tsm, 3).is_err());

    let vlog_path = root.path().join("vlog");
    let vlog = new_vlog_wal(&vlog_path, &executor);
    vlog_write(&executor, &vlog, 1).unwrap();
    vlog.close();
    drop(vlog);
    let reopened_vlog = new_vlog_wal(&vlog_path, &executor);
    vlog_write(&executor, &reopened_vlog, 2).unwrap();
    reopened_vlog.close();
    assert!(vlog_write(&executor, &reopened_vlog, 3).is_err());

    let index_path = root.path().join("index");
    let index = new_vlog_index_wal(&index_path, &executor);
    vlog_index_write(&executor, &index, 1).unwrap();
    index.close();
    drop(index);
    let reopened_index = new_vlog_index_wal(&index_path, &executor);
    vlog_index_write(&executor, &reopened_index, 2).unwrap();
    reopened_index.close();
    assert!(vlog_index_write(&executor, &reopened_index, 3).is_err());
}

#[test]
fn wal_workers_terminate_independently_for_each_wal_type() {
    if run_in_isolated_process("wal_workers_terminate_independently_for_each_wal_type") {
        return;
    }

    let root = tempfile::tempdir().unwrap();

    let tsm_executor = CountingExecutor::new();
    let executor: ExecutorRef = tsm_executor.clone();
    let tsm_a = new_tsm_wal_with_executor(&root.path().join("tsm-a"), Arc::clone(&executor));
    let tsm_b = new_tsm_wal_with_executor(&root.path().join("tsm-b"), executor);
    tsm_executor.wait_for_active(2);
    tsm_a.close();
    tsm_executor.wait_for_active(1);
    tsm_write(&tsm_executor.inner, &tsm_b, 1).unwrap();
    tsm_executor.wait_for_active(1);
    tsm_b.close();
    tsm_executor.wait_for_active(0);

    let vlog_executor = CountingExecutor::new();
    let executor: ExecutorRef = vlog_executor.clone();
    let vlog_a = new_vlog_wal_with_executor(&root.path().join("vlog-a"), Arc::clone(&executor));
    let vlog_b = new_vlog_wal_with_executor(&root.path().join("vlog-b"), executor);
    vlog_executor.wait_for_active(2);
    vlog_a.close();
    vlog_executor.wait_for_active(1);
    vlog_write(&vlog_executor.inner, &vlog_b, 1).unwrap();
    vlog_executor.wait_for_active(1);
    vlog_b.close();
    vlog_executor.wait_for_active(0);

    let index_executor = CountingExecutor::new();
    let executor: ExecutorRef = index_executor.clone();
    let index_a =
        new_vlog_index_wal_with_executor(&root.path().join("index-a"), Arc::clone(&executor));
    let index_b = new_vlog_index_wal_with_executor(&root.path().join("index-b"), executor);
    index_executor.wait_for_active(2);
    index_a.close();
    index_executor.wait_for_active(1);
    vlog_index_write(&index_executor.inner, &index_b, 1).unwrap();
    index_executor.wait_for_active(1);
    index_b.close();
    index_executor.wait_for_active(0);
}

#[test]
fn close_racing_with_pending_write_is_terminal_for_each_wal_type() {
    if run_in_isolated_process("close_racing_with_pending_write_is_terminal_for_each_wal_type") {
        return;
    }

    let root = tempfile::tempdir().unwrap();

    let executor = test_executor();
    let tsm = new_tsm_wal(&root.path().join("tsm"), &executor);
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let write_executor = Arc::clone(&executor);
    let write_wal = Arc::clone(&tsm);
    let handle = thread::spawn(move || {
        started_tx.send(()).unwrap();
        let result = write_executor.block_on(write_wal.write_multi(large_tsm_values()));
        done_tx.send(result).unwrap();
    });
    started_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    thread::sleep(Duration::from_millis(1));
    tsm.close();
    let _ = done_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("TSM WAL pending write did not terminate during close");
    handle.join().unwrap();
    assert!(tsm_write(&executor, &tsm, 1).is_err());

    let executor = test_executor();
    let vlog = new_vlog_wal(&root.path().join("vlog"), &executor);
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let write_executor = Arc::clone(&executor);
    let write_wal = Arc::clone(&vlog);
    let handle = thread::spawn(move || {
        started_tx.send(()).unwrap();
        let result = write_executor.block_on(write_wal.write_multi(large_tsm_values()));
        done_tx.send(result).unwrap();
    });
    started_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    thread::sleep(Duration::from_millis(1));
    vlog.close();
    let _ = done_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("VLog WAL pending write did not terminate during close");
    handle.join().unwrap();
    assert!(vlog_write(&executor, &vlog, 1).is_err());

    let executor = test_executor();
    let index = new_vlog_index_wal(&root.path().join("index"), &executor);
    let (started_tx, started_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let write_executor = Arc::clone(&executor);
    let write_wal = Arc::clone(&index);
    let handle = thread::spawn(move || {
        started_tx.send(()).unwrap();
        let result = write_executor.block_on(write_wal.write_multi(large_vlog_index_values()));
        done_tx.send(result).unwrap();
    });
    started_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    thread::sleep(Duration::from_millis(1));
    index.close();
    let _ = done_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("VLog index WAL pending write did not terminate during close");
    handle.join().unwrap();
    assert!(vlog_index_write(&executor, &index, 1).is_err());
}
