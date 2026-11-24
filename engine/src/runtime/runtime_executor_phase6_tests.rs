use std::{
    collections::HashMap,
    future::Future,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::{
    db::{LeasedFloatIterator, LeasedQueryIterator, ReefsDb},
    index::tsi1::index::Index,
    query::{
        query_iterator,
        storage_ir::{
            ColumnKind, ColumnRef, MeasurementRef, OutputOrder, SeriesProjection,
            StorageProjection, StorageScan, TimeOrder, TimeRange,
        },
    },
    runtime::execution::{
        BoxBlockingTask, BoxFutureTask, DefaultTokioExecutor, DefaultTokioExecutorOptions,
        ExecutorRef, RuntimeExecutor, RuntimeExecutorExt, SpawnError, TaskController,
    },
    series::series_file::SeriesFile,
    storage::{
        EngineOptions, StorageOptions, active_cursor as iterator,
        tsm1::{
            vlog::{VLogIndex, vlog_index_wal, vlog_wal},
            wal_impl,
        },
    },
};

const WAL_NESTED_RUN: usize = 0;
const WAL_NESTED_REJECT: usize = 1;
const WAL_NESTED_DROP: usize = 2;

struct WalTestExecutor {
    runtime: tokio::runtime::Runtime,
    async_submissions: AtomicUsize,
    drop_async_submission: AtomicUsize,
    nested_disposition: AtomicUsize,
}

impl WalTestExecutor {
    fn new() -> Self {
        Self {
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .max_blocking_threads(1)
                .thread_name("phase6_wal_executor")
                .enable_all()
                .build()
                .unwrap(),
            async_submissions: AtomicUsize::new(0),
            drop_async_submission: AtomicUsize::new(usize::MAX),
            nested_disposition: AtomicUsize::new(WAL_NESTED_RUN),
        }
    }

    fn reject_nested(&self) {
        self.nested_disposition
            .store(WAL_NESTED_REJECT, Ordering::SeqCst);
    }

    fn drop_nested(&self) {
        self.nested_disposition
            .store(WAL_NESTED_DROP, Ordering::SeqCst);
    }

    fn drop_async_at(&self, submission: usize) {
        self.drop_async_submission
            .store(submission, Ordering::SeqCst);
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }
}

impl RuntimeExecutor for WalTestExecutor {
    fn schedule(&self, task: BoxFutureTask) -> Result<TaskController, SpawnError> {
        let submission = self.async_submissions.fetch_add(1, Ordering::SeqCst) + 1;
        if submission == self.drop_async_submission.load(Ordering::SeqCst) {
            drop(task);
            return Ok(TaskController::detached());
        }
        Ok(ExternalExecutor::controller(self.runtime.spawn(task)))
    }

    fn schedule_blocking(&self, task: BoxBlockingTask) -> Result<TaskController, SpawnError> {
        match self.nested_disposition.load(Ordering::SeqCst) {
            WAL_NESTED_REJECT => Err(SpawnError::rejected("injected nested WAL refusal")),
            WAL_NESTED_DROP => {
                drop(task);
                Ok(TaskController::detached())
            }
            _ => Ok(ExternalExecutor::controller(
                self.runtime.spawn_blocking(task),
            )),
        }
    }

    fn schedule_compact(&self, task: BoxFutureTask) -> Result<TaskController, SpawnError> {
        self.schedule(task)
    }

    fn schedule_blocking_compact(
        &self,
        task: BoxBlockingTask,
    ) -> Result<TaskController, SpawnError> {
        self.schedule_blocking(task)
    }
}

struct ExternalExecutor {
    general: tokio::runtime::Runtime,
    compact: tokio::runtime::Runtime,
    submissions: AtomicUsize,
    reject_compact: bool,
    reject_nested_blocking: bool,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum AcceptedTaskDisposition {
    Run = 0,
    Drop = 1,
    Panic = 2,
}

#[derive(Clone, Copy)]
enum SupervisorClass {
    Level,
    Snapshot,
    VLog,
}

struct SupervisorLifecycleExecutor {
    general: tokio::runtime::Runtime,
    compact: tokio::runtime::Runtime,
    async_disposition: AtomicUsize,
    blocking_disposition: AtomicUsize,
    blocking_target: usize,
    blocking_submissions: AtomicUsize,
    terminal_sender: crossbeam_channel::Sender<()>,
    terminal_receiver: crossbeam_channel::Receiver<()>,
}

impl SupervisorLifecycleExecutor {
    fn new(class: SupervisorClass, disposition: AcceptedTaskDisposition) -> Self {
        let (terminal_sender, terminal_receiver) = crossbeam_channel::bounded(1);
        Self {
            general: ExternalExecutor::runtime("phase6_supervisor_general"),
            compact: ExternalExecutor::runtime("phase6_supervisor_compact"),
            async_disposition: AtomicUsize::new(
                if matches!(class, SupervisorClass::Snapshot | SupervisorClass::VLog) {
                    disposition as usize
                } else {
                    AcceptedTaskDisposition::Run as usize
                },
            ),
            blocking_disposition: AtomicUsize::new(if matches!(class, SupervisorClass::Level) {
                disposition as usize
            } else {
                AcceptedTaskDisposition::Run as usize
            }),
            blocking_target: 1,
            blocking_submissions: AtomicUsize::new(0),
            terminal_sender,
            terminal_receiver,
        }
    }

    fn disposition(value: usize) -> AcceptedTaskDisposition {
        match value {
            1 => AcceptedTaskDisposition::Drop,
            2 => AcceptedTaskDisposition::Panic,
            _ => AcceptedTaskDisposition::Run,
        }
    }

    fn wait_for_injected_terminal(&self) {
        self.terminal_receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("selected accepted supervisor did not reach its terminal disposition");
    }
}

impl RuntimeExecutor for SupervisorLifecycleExecutor {
    fn schedule(&self, task: BoxFutureTask) -> Result<TaskController, SpawnError> {
        Ok(ExternalExecutor::controller(self.general.spawn(task)))
    }

    fn schedule_blocking(&self, task: BoxBlockingTask) -> Result<TaskController, SpawnError> {
        Ok(ExternalExecutor::controller(
            self.general.spawn_blocking(task),
        ))
    }

    fn schedule_compact(&self, task: BoxFutureTask) -> Result<TaskController, SpawnError> {
        let disposition = Self::disposition(
            self.async_disposition
                .swap(AcceptedTaskDisposition::Run as usize, Ordering::SeqCst),
        );
        match disposition {
            AcceptedTaskDisposition::Run => {
                Ok(ExternalExecutor::controller(self.compact.spawn(task)))
            }
            AcceptedTaskDisposition::Drop => {
                drop(task);
                self.terminal_sender.send(()).unwrap();
                Ok(TaskController::detached())
            }
            AcceptedTaskDisposition::Panic => {
                let terminal_sender = self.terminal_sender.clone();
                Ok(ExternalExecutor::controller(self.compact.spawn(
                    async move {
                        terminal_sender.send(()).unwrap();
                        let _task = task;
                        panic!("injected accepted async supervisor panic");
                    },
                )))
            }
        }
    }

    fn schedule_blocking_compact(
        &self,
        task: BoxBlockingTask,
    ) -> Result<TaskController, SpawnError> {
        let submission = self.blocking_submissions.fetch_add(1, Ordering::SeqCst) + 1;
        let disposition = if submission == self.blocking_target {
            Self::disposition(
                self.blocking_disposition
                    .swap(AcceptedTaskDisposition::Run as usize, Ordering::SeqCst),
            )
        } else {
            AcceptedTaskDisposition::Run
        };
        match disposition {
            AcceptedTaskDisposition::Run => Ok(ExternalExecutor::controller(
                self.compact.spawn_blocking(task),
            )),
            AcceptedTaskDisposition::Drop => {
                drop(task);
                self.terminal_sender.send(()).unwrap();
                Ok(TaskController::detached())
            }
            AcceptedTaskDisposition::Panic => {
                let terminal_sender = self.terminal_sender.clone();
                Ok(ExternalExecutor::controller(self.compact.spawn_blocking(
                    move || {
                        terminal_sender.send(()).unwrap();
                        let _task = task;
                        panic!("injected accepted blocking supervisor panic");
                    },
                )))
            }
        }
    }
}

impl ExternalExecutor {
    fn new(reject_compact: bool, reject_nested_blocking: bool) -> Self {
        Self {
            general: Self::runtime("phase6_external_general"),
            compact: Self::runtime("phase6_external_compact"),
            submissions: AtomicUsize::new(0),
            reject_compact,
            reject_nested_blocking,
        }
    }

    fn runtime(thread_name: &str) -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(4)
            .thread_name(thread_name)
            .enable_all()
            .build()
            .unwrap()
    }

    fn controller(handle: tokio::task::JoinHandle<()>) -> TaskController {
        let abort = handle.abort_handle();
        TaskController::new(move || abort.abort())
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.general.block_on(future)
    }

    fn is_nested_storage_task() -> bool {
        std::thread::current()
            .name()
            .is_some_and(|name| name.starts_with("phase6_external_"))
    }
}

impl RuntimeExecutor for ExternalExecutor {
    fn schedule(&self, task: BoxFutureTask) -> Result<TaskController, SpawnError> {
        self.submissions.fetch_add(1, Ordering::SeqCst);
        Ok(Self::controller(self.general.spawn(task)))
    }

    fn schedule_blocking(&self, task: BoxBlockingTask) -> Result<TaskController, SpawnError> {
        if self.reject_nested_blocking && Self::is_nested_storage_task() {
            return Err(SpawnError::rejected("nested optional work rejected"));
        }
        self.submissions.fetch_add(1, Ordering::SeqCst);
        Ok(Self::controller(self.general.spawn_blocking(task)))
    }

    fn schedule_compact(&self, task: BoxFutureTask) -> Result<TaskController, SpawnError> {
        if self.reject_compact {
            return Err(SpawnError::rejected("compact lane stopping"));
        }
        self.submissions.fetch_add(1, Ordering::SeqCst);
        Ok(Self::controller(self.compact.spawn(task)))
    }

    fn schedule_blocking_compact(
        &self,
        task: BoxBlockingTask,
    ) -> Result<TaskController, SpawnError> {
        if self.reject_compact {
            return Err(SpawnError::rejected("compact lane stopping"));
        }
        self.submissions.fetch_add(1, Ordering::SeqCst);
        Ok(Self::controller(self.compact.spawn_blocking(task)))
    }
}

struct RejectingExecutor;

impl RuntimeExecutor for RejectingExecutor {
    fn schedule(&self, _task: BoxFutureTask) -> Result<TaskController, SpawnError> {
        Err(SpawnError::rejected("host stopping"))
    }

    fn schedule_blocking(&self, _task: BoxBlockingTask) -> Result<TaskController, SpawnError> {
        Err(SpawnError::rejected("host stopping"))
    }

    fn schedule_compact(&self, _task: BoxFutureTask) -> Result<TaskController, SpawnError> {
        Err(SpawnError::rejected("host stopping"))
    }

    fn schedule_blocking_compact(
        &self,
        _task: BoxBlockingTask,
    ) -> Result<TaskController, SpawnError> {
        Err(SpawnError::rejected("host stopping"))
    }
}

struct SelectiveRejectExecutor {
    runtime: tokio::runtime::Runtime,
    blocking_submissions: AtomicUsize,
    reject_submission: AtomicUsize,
    drop_submission: AtomicUsize,
    observe_parallel: Arc<AtomicBool>,
    active_tasks: Arc<AtomicUsize>,
    max_active_tasks: Arc<AtomicUsize>,
}

impl SelectiveRejectExecutor {
    fn new(reject_submission: usize) -> Self {
        Self {
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .max_blocking_threads(8)
                .thread_name("phase6_selective_reject")
                .enable_all()
                .build()
                .unwrap(),
            blocking_submissions: AtomicUsize::new(0),
            reject_submission: AtomicUsize::new(reject_submission),
            drop_submission: AtomicUsize::new(usize::MAX),
            observe_parallel: Arc::new(AtomicBool::new(false)),
            active_tasks: Arc::new(AtomicUsize::new(0)),
            max_active_tasks: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn reject_after(&self, additional_submissions: usize) {
        self.reject_submission.store(
            self.blocking_submissions.load(Ordering::SeqCst) + additional_submissions,
            Ordering::SeqCst,
        );
    }

    fn observe_parallelism(&self) {
        self.max_active_tasks.store(0, Ordering::SeqCst);
        self.observe_parallel.store(true, Ordering::SeqCst);
    }

    fn drop_after(&self, additional_submissions: usize) {
        self.drop_submission.store(
            self.blocking_submissions.load(Ordering::SeqCst) + additional_submissions,
            Ordering::SeqCst,
        );
    }

    fn max_active_tasks(&self) -> usize {
        self.observe_parallel.store(false, Ordering::SeqCst);
        self.max_active_tasks.load(Ordering::SeqCst)
    }
}

impl RuntimeExecutor for SelectiveRejectExecutor {
    fn schedule(&self, task: BoxFutureTask) -> Result<TaskController, SpawnError> {
        Ok(ExternalExecutor::controller(self.runtime.spawn(task)))
    }

    fn schedule_blocking(&self, task: BoxBlockingTask) -> Result<TaskController, SpawnError> {
        let submission = self.blocking_submissions.fetch_add(1, Ordering::SeqCst) + 1;
        if submission == self.reject_submission.load(Ordering::SeqCst) {
            return Err(SpawnError::rejected("selective refusal"));
        }
        if submission == self.drop_submission.load(Ordering::SeqCst) {
            drop(task);
            return Ok(TaskController::detached());
        }
        let observe_parallel = Arc::clone(&self.observe_parallel);
        let active_tasks = Arc::clone(&self.active_tasks);
        let max_active_tasks = Arc::clone(&self.max_active_tasks);
        Ok(ExternalExecutor::controller(self.runtime.spawn_blocking(
            move || {
                if observe_parallel.load(Ordering::SeqCst) {
                    let active = active_tasks.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active_tasks.fetch_max(active, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(25));
                    task();
                    active_tasks.fetch_sub(1, Ordering::SeqCst);
                } else {
                    task();
                }
            },
        )))
    }

    fn schedule_compact(&self, task: BoxFutureTask) -> Result<TaskController, SpawnError> {
        Ok(ExternalExecutor::controller(self.runtime.spawn(task)))
    }

    fn schedule_blocking_compact(
        &self,
        task: BoxBlockingTask,
    ) -> Result<TaskController, SpawnError> {
        Ok(ExternalExecutor::controller(
            self.runtime.spawn_blocking(task),
        ))
    }
}

fn open_series_file(path: &Path, executor: ExecutorRef) -> Arc<SeriesFile> {
    let mut series_file = SeriesFile::new_with_executor(path, executor);
    series_file.open().unwrap();
    Arc::new(series_file)
}

fn open_reefsdb(
    root: &Path,
    engine_id: u64,
    series_file: Arc<SeriesFile>,
    executor: ExecutorRef,
    compaction_disabled: bool,
) -> Arc<ReefsDb> {
    open_reefsdb_with_vlog(
        root,
        engine_id,
        series_file,
        executor,
        compaction_disabled,
        false,
    )
}

fn open_reefsdb_with_vlog(
    root: &Path,
    engine_id: u64,
    series_file: Arc<SeriesFile>,
    executor: ExecutorRef,
    compaction_disabled: bool,
    vlog_enabled: bool,
) -> Arc<ReefsDb> {
    let measurement_schemas = Arc::new(
        crate::series::MeasurementSchemaRegistry::open(
            series_file
                .path()
                .join(crate::series::MEASUREMENT_SCHEMA_FILE_NAME),
        )
        .unwrap(),
    );
    let mut options =
        EngineOptions::new_with_executor(StorageOptions::default(), Arc::clone(&executor));
    options.shard_id = engine_id;
    options.compaction_disabled = compaction_disabled;
    options.monitor_disabled = true;
    options.wal_enabled = false;
    options.config.wal_enabled = false;
    options.vlog_enabled = vlog_enabled;
    if vlog_enabled {
        options.vlog_config.max_vlog_writers = 1;
    }
    options.config.wal_dir = root.join("wal").to_string_lossy().into_owned();
    let reefsdb = Arc::new(
        ReefsDb::new_with_executor(
            root.join("data"),
            root.join("wal"),
            root.join("vlog"),
            root.join("vlog_index_wal"),
            series_file,
            measurement_schemas,
            options,
            executor,
        )
        .unwrap(),
    );
    reefsdb.open().unwrap();
    reefsdb
}

fn run_with_timeout(operation: impl FnOnce() + Send + 'static, message: &str) {
    let (sender, receiver) = crossbeam_channel::bounded(1);
    std::thread::spawn(move || {
        operation();
        let _ = sender.send(());
    });
    receiver
        .recv_timeout(Duration::from_secs(5))
        .unwrap_or_else(|_| panic!("{message}"));
}

fn assert_supervisor_terminal_cleanup(
    class: SupervisorClass,
    disposition: AcceptedTaskDisposition,
) {
    let root = tempfile::tempdir().unwrap();
    let concrete = Arc::new(SupervisorLifecycleExecutor::new(class, disposition));
    let executor: ExecutorRef = concrete.clone();
    let series_file = open_series_file(&root.path().join("_series"), Arc::clone(&executor));
    let reefsdb = open_reefsdb_with_vlog(
        &root.path().join("shard"),
        1,
        Arc::clone(&series_file),
        Arc::clone(&executor),
        true,
        matches!(class, SupervisorClass::VLog),
    );

    reefsdb.set_compactions_enabled(true);
    concrete.wait_for_injected_terminal();

    let disable_db = Arc::clone(&reefsdb);
    run_with_timeout(
        move || disable_db.set_compactions_enabled(false),
        "accepted supervisor terminal cleanup did not release disable waiters",
    );

    let retry_db = Arc::clone(&reefsdb);
    run_with_timeout(
        move || {
            retry_db.set_compactions_enabled(true);
            retry_db.set_compactions_enabled(false);
        },
        "supervisor enable/disable retry did not complete",
    );

    reefsdb.close().unwrap();
    series_file.close().unwrap();
}

fn value_scan() -> StorageScan {
    let value = ColumnRef::new("value", ColumnKind::Field(Some(crate::models::FLOAT)));
    StorageScan {
        measurement: MeasurementRef::new("cpu"),
        projection: StorageProjection {
            primary_column: Some(value.clone()),
            returned_columns: vec![value],
            row_domain_columns: Vec::new(),
            series: SeriesProjection::new(vec!["host".to_string()], vec!["host".to_string()]),
        },
        time_range: TimeRange::all(),
        predicate: None,
        time_order: TimeOrder::Ascending,
        output_order: OutputOrder::TimeOrdered,
        execution: Default::default(),
        cancellation: None,
    }
}

macro_rules! try_float_iterator {
    ($iterator:expr, $ty:ty) => {
        match $iterator.try_downcast_float::<$ty>() {
            Ok(iterator) => return iterator,
            Err(iterator) => iterator,
        }
    };
}

fn float_iterator(iterator: LeasedQueryIterator) -> LeasedFloatIterator {
    let iterator = try_float_iterator!(iterator, query_iterator::FloatMergeIter);
    let iterator = try_float_iterator!(iterator, query_iterator::FloatInterruptIter);
    let iterator = try_float_iterator!(iterator, query_iterator::FloatLimitIter);
    let iterator = try_float_iterator!(iterator, query_iterator::FloatSortedMergeIter);
    let iterator = try_float_iterator!(iterator, iterator::FloatIter);
    let _iterator = try_float_iterator!(iterator, iterator::FloatLimitIter);
    panic!("unexpected float iterator type")
}

fn scan_values(reefsdb: &Arc<ReefsDb>) -> Vec<(i64, f64)> {
    let iterator = reefsdb
        .create_storage_iterator(value_scan())
        .unwrap()
        .unwrap();
    let mut iterator = float_iterator(iterator);
    let values = std::iter::from_fn(|| query_iterator::FloatIterator::next(&mut iterator))
        .map(|point| (point.time(), point.value()))
        .collect();
    query_iterator::QueryIterator::close(&mut iterator);
    values
}

#[test]
fn default_executor_is_shared_by_series_file_and_peer_reefsdb_instances() {
    let root = tempfile::tempdir().unwrap();
    let default_executor =
        Arc::new(DefaultTokioExecutor::new(DefaultTokioExecutorOptions::new(2, 4, 1, 2)).unwrap());
    let executor: ExecutorRef = default_executor.clone();
    let series_file = open_series_file(&root.path().join("_series"), Arc::clone(&executor));
    let db1 = open_reefsdb(
        &root.path().join("shard-1"),
        1,
        Arc::clone(&series_file),
        Arc::clone(&executor),
        true,
    );
    let db2 = open_reefsdb(
        &root.path().join("shard-2"),
        2,
        Arc::clone(&series_file),
        Arc::clone(&executor),
        true,
    );

    assert!(Arc::ptr_eq(
        &series_file,
        &db1.series_file().unwrap().unwrap()
    ));
    assert!(Arc::ptr_eq(
        &series_file,
        &db2.series_file().unwrap().unwrap()
    ));

    let (mut first, _) =
        crate::models::points::parse_points_string("cpu,host=shared value=1.0 10").unwrap();
    db1.write_points(&mut first).unwrap();
    let (mut second, _) =
        crate::models::points::parse_points_string("cpu,host=shared value=2.0 20").unwrap();
    db2.write_points(&mut second).unwrap();
    let tags = crate::models::Tags::from(HashMap::from([(b"host".to_vec(), b"shared".to_vec())]));
    let database_wide_series_id = series_file.series_id(b"cpu", &tags);
    assert!(database_wide_series_id > 0);
    assert_eq!(
        db1.series_file().unwrap().unwrap().series_id(b"cpu", &tags),
        database_wide_series_id
    );
    assert_eq!(
        db2.series_file().unwrap().unwrap().series_id(b"cpu", &tags),
        database_wide_series_id
    );

    db1.close().unwrap();
    let task = executor.spawn_blocking(|| 42).unwrap();
    assert_eq!(default_executor.block_on(task).unwrap(), 42);

    let (mut after_close, _) =
        crate::models::points::parse_points_string("cpu,host=shared value=3.0 30").unwrap();
    db2.write_points(&mut after_close).unwrap();
    assert_eq!(scan_values(&db2), vec![(20, 2.0), (30, 3.0)]);

    db2.close().unwrap();
    series_file.close().unwrap();
    drop(executor);
    drop(default_executor);
}

#[test]
fn external_executor_drives_open_write_scan_and_close() {
    let root = tempfile::tempdir().unwrap();
    let external = Arc::new(ExternalExecutor::new(false, false));
    let executor: ExecutorRef = external.clone();
    let series_file = open_series_file(&root.path().join("_series"), Arc::clone(&executor));
    let reefsdb = open_reefsdb(
        &root.path().join("shard"),
        1,
        Arc::clone(&series_file),
        Arc::clone(&executor),
        true,
    );

    let (mut points, _) =
        crate::models::points::parse_points_string("cpu,host=external value=7.0 70").unwrap();
    reefsdb.write_points(&mut points).unwrap();
    assert_eq!(scan_values(&reefsdb), vec![(70, 7.0)]);
    reefsdb.close().unwrap();

    let task = executor.spawn(async { "still running" }).unwrap();
    assert_eq!(external.block_on(task).unwrap(), "still running");
    assert!(external.submissions.load(Ordering::SeqCst) > 0);
    series_file.close().unwrap();
}

#[test]
fn required_series_submission_refusal_does_not_publish_an_id() {
    let root = tempfile::tempdir().unwrap();
    let executor: ExecutorRef = Arc::new(RejectingExecutor);
    let series_file = open_series_file(&root.path().join("_series"), executor);
    let tags = crate::models::Tags::from(HashMap::from([(b"host".to_vec(), b"a".to_vec())]));

    let error = series_file
        .create_series_list_if_not_exists(&[b"cpu"], &[&tags])
        .unwrap_err();
    assert!(matches!(
        error,
        crate::series::series_file::Error::Execution { .. }
    ));
    assert_eq!(series_file.series_id(b"cpu", &tags), 0);
    series_file.close().unwrap();
}

#[test]
fn selective_required_series_refusal_does_not_publish_accepted_partition_work() {
    let root = tempfile::tempdir().unwrap();
    let executor: ExecutorRef = Arc::new(SelectiveRejectExecutor::new(4));
    let series_file = open_series_file(&root.path().join("_series"), executor);

    let tags = (0..10_000)
        .find_map(|candidate| {
            let tags = crate::models::Tags::from(HashMap::from([(
                b"host".to_vec(),
                format!("accepted-{candidate}").into_bytes(),
            )]));
            let key = crate::series::series_file::encode_series_key(b"cpu", &tags);
            (series_file.series_key_partition_id(&key) == 0).then_some(tags)
        })
        .expect("a series key should map to the first accepted partition");

    let error = series_file
        .create_series_list_if_not_exists(&[b"cpu"], &[&tags])
        .unwrap_err();
    assert!(matches!(
        error,
        crate::series::series_file::Error::Execution { .. }
    ));
    assert_eq!(series_file.series_id(b"cpu", &tags), 0);
    assert_eq!(series_file.series_count(), 0);
    series_file.close().unwrap();
}

#[test]
fn successful_series_file_fanout_remains_parallel() {
    let root = tempfile::tempdir().unwrap();
    let selective = Arc::new(SelectiveRejectExecutor::new(usize::MAX));
    let executor: ExecutorRef = selective.clone();
    let series_file = open_series_file(&root.path().join("_series"), executor);
    let tags = crate::models::Tags::from(HashMap::from([(b"host".to_vec(), b"parallel".to_vec())]));

    selective.observe_parallelism();
    let ids = series_file
        .create_series_list_if_not_exists(&[b"cpu"], &[&tags])
        .unwrap();
    assert!(ids[0] > 0);
    assert!(selective.max_active_tasks() >= 2);
    series_file.close().unwrap();
}

#[test]
fn tsi_open_selective_refusal_has_no_partial_partition_open_and_retries() {
    let root = tempfile::tempdir().unwrap();
    let selective = Arc::new(SelectiveRejectExecutor::new(usize::MAX));
    let executor: ExecutorRef = selective.clone();
    let series_file = open_series_file(&root.path().join("_series"), Arc::clone(&executor));
    let index_path = root.path().join("index");
    let index = Index::new_with_executor(
        Arc::clone(&series_file),
        "db".to_string(),
        &index_path,
        1 << 20,
        1024,
        executor,
    );

    selective.reject_after(3);
    let error = index.open().unwrap_err();
    assert!(matches!(
        error,
        crate::index::tsi1::index::Error::Execution { .. }
    ));
    for partition in 0..8 {
        assert!(!index_path.join(partition.to_string()).exists());
    }
    std::thread::sleep(Duration::from_millis(25));
    for partition in 0..8 {
        assert!(!index_path.join(partition.to_string()).exists());
    }

    selective.observe_parallelism();
    index.open().unwrap();
    assert!(selective.max_active_tasks() >= 2);
    index.close().unwrap();
    series_file.close().unwrap();
}

#[test]
fn tsi_open_accepted_task_disposal_cancels_before_mutation_and_retries() {
    let root = tempfile::tempdir().unwrap();
    let selective = Arc::new(SelectiveRejectExecutor::new(usize::MAX));
    let executor: ExecutorRef = selective.clone();
    let series_file = open_series_file(&root.path().join("_series"), Arc::clone(&executor));
    let index_path = root.path().join("index");
    let index = Index::new_with_executor(
        Arc::clone(&series_file),
        "db".to_string(),
        &index_path,
        1 << 20,
        1024,
        executor,
    );

    selective.drop_after(3);
    let error = index.open().unwrap_err();
    assert!(matches!(
        error,
        crate::index::tsi1::index::Error::TaskTerminated
    ));
    for partition in 0..8 {
        assert!(!index_path.join(partition.to_string()).exists());
    }

    index.open().unwrap();
    for partition in 0..8 {
        assert!(index_path.join(partition.to_string()).exists());
    }
    index.close().unwrap();
    series_file.close().unwrap();
}

#[test]
fn tsi_mutation_selective_refusal_is_atomic_and_retries() {
    let root = tempfile::tempdir().unwrap();
    let selective = Arc::new(SelectiveRejectExecutor::new(usize::MAX));
    let executor: ExecutorRef = selective.clone();
    let series_file = open_series_file(&root.path().join("_series"), Arc::clone(&executor));
    let index = Index::new_with_executor(
        Arc::clone(&series_file),
        "db".to_string(),
        root.path().join("index"),
        1 << 20,
        1024,
        executor,
    );
    index.open().unwrap();

    let tags = crate::models::Tags::from(HashMap::from([(b"host".to_vec(), b"tsi".to_vec())]));
    let key = crate::series::series_file::encode_series_key(b"cpu", &tags);
    selective.reject_after(3);
    let error = index
        .create_series_list_if_not_exists(
            &[key.as_slice()],
            vec![b"cpu".to_vec()],
            vec![tags.clone()],
        )
        .unwrap_err();
    assert!(matches!(
        error,
        crate::index::tsi1::index::Error::Execution { .. }
    ));
    assert_eq!(index.series_n(), 0);
    assert_eq!(series_file.series_id(b"cpu", &tags), 0);
    std::thread::sleep(Duration::from_millis(25));
    assert_eq!(index.series_n(), 0);
    assert_eq!(series_file.series_id(b"cpu", &tags), 0);

    index
        .create_series_list_if_not_exists(
            &[key.as_slice()],
            vec![b"cpu".to_vec()],
            vec![tags.clone()],
        )
        .unwrap();
    assert_eq!(index.series_n(), 1);
    assert!(index.measurement_exists(b"cpu").unwrap());

    selective.reject_after(3);
    let error = index.drop_measurement(b"cpu").unwrap_err();
    assert!(matches!(
        error,
        crate::index::tsi1::index::Error::Execution { .. }
    ));
    assert!(index.measurement_exists(b"cpu").unwrap());
    assert_eq!(index.series_n(), 1);
    std::thread::sleep(Duration::from_millis(25));
    assert!(index.measurement_exists(b"cpu").unwrap());
    assert_eq!(index.series_n(), 1);

    selective.observe_parallelism();
    assert!(index.measurement_tag_keys(b"cpu").unwrap().contains("host"));
    assert!(selective.max_active_tasks() >= 2);

    index.drop_measurement(b"cpu").unwrap();
    assert!(!index.measurement_exists(b"cpu").unwrap());
    index.close().unwrap();
    series_file.close().unwrap();
}

#[test]
fn optional_series_compaction_refusal_restores_state_and_preserves_write_success() {
    let root = tempfile::tempdir().unwrap();
    let external = Arc::new(ExternalExecutor::new(false, true));
    let executor: ExecutorRef = external.clone();
    let series_file = open_series_file(&root.path().join("_series"), executor);
    let first_tags = crate::models::Tags::from(HashMap::from([(b"host".to_vec(), b"a".to_vec())]));
    let first_key = b"cpu,host=a";
    let partition = series_file.series_key_partition(first_key).unwrap();
    partition.write().set_compact_threshold(1);

    let ids = series_file
        .create_series_list_if_not_exists(&[b"cpu"], &[&first_tags])
        .unwrap();
    assert!(ids[0] > 0);
    assert!(!partition.read().compacting());

    let mut second_host = None;
    for candidate in 0..10_000 {
        let host = format!("peer-{candidate}");
        let key = format!("cpu,host={host}");
        if Arc::ptr_eq(
            &partition,
            &series_file.series_key_partition(key.as_bytes()).unwrap(),
        ) {
            second_host = Some(host);
            break;
        }
    }
    let second_host = second_host.expect("a second key should map to the same series partition");
    let second_tags = crate::models::Tags::from(HashMap::from([(
        b"host".to_vec(),
        second_host.as_bytes().to_vec(),
    )]));
    let ids = series_file
        .create_series_list_if_not_exists(&[b"cpu"], &[&second_tags])
        .unwrap();
    assert!(ids[0] > 0);
    assert!(!partition.read().compacting());
    series_file.close().unwrap();
}

#[test]
fn rejected_background_compaction_workers_do_not_block_reefsdb_close() {
    let root = tempfile::tempdir().unwrap();
    let external = Arc::new(ExternalExecutor::new(true, false));
    let executor: ExecutorRef = external.clone();
    let series_file = open_series_file(&root.path().join("_series"), Arc::clone(&executor));
    let reefsdb = open_reefsdb(
        &root.path().join("shard"),
        1,
        Arc::clone(&series_file),
        Arc::clone(&executor),
        false,
    );

    let (mut points, _) =
        crate::models::points::parse_points_string("cpu,host=compact-refusal value=9.0 90")
            .unwrap();
    reefsdb.write_points(&mut points).unwrap();
    assert_eq!(scan_values(&reefsdb), vec![(90, 9.0)]);
    reefsdb.close().unwrap();

    let task = executor.spawn_blocking(|| 99).unwrap();
    assert_eq!(external.block_on(task).unwrap(), 99);
    series_file.close().unwrap();
}

#[test]
fn level_supervisor_accepted_disposal_releases_wait_group() {
    assert_supervisor_terminal_cleanup(SupervisorClass::Level, AcceptedTaskDisposition::Drop);
}

#[test]
fn level_supervisor_accepted_panic_releases_wait_group() {
    assert_supervisor_terminal_cleanup(SupervisorClass::Level, AcceptedTaskDisposition::Panic);
}

#[test]
fn snapshot_supervisor_accepted_disposal_releases_wait_group() {
    assert_supervisor_terminal_cleanup(SupervisorClass::Snapshot, AcceptedTaskDisposition::Drop);
}

#[test]
fn snapshot_supervisor_accepted_panic_releases_wait_group() {
    assert_supervisor_terminal_cleanup(SupervisorClass::Snapshot, AcceptedTaskDisposition::Panic);
}

#[test]
fn vlog_supervisor_accepted_disposal_releases_wait_group() {
    assert_supervisor_terminal_cleanup(SupervisorClass::VLog, AcceptedTaskDisposition::Drop);
}

#[test]
fn vlog_supervisor_accepted_panic_releases_wait_group() {
    assert_supervisor_terminal_cleanup(SupervisorClass::VLog, AcceptedTaskDisposition::Panic);
}

#[test]
fn reefsdb_close_completes_after_accepted_supervisor_disposal() {
    let root = tempfile::tempdir().unwrap();
    let concrete = Arc::new(SupervisorLifecycleExecutor::new(
        SupervisorClass::Snapshot,
        AcceptedTaskDisposition::Drop,
    ));
    let executor: ExecutorRef = concrete.clone();
    let series_file = open_series_file(&root.path().join("_series"), Arc::clone(&executor));
    let reefsdb = open_reefsdb(
        &root.path().join("shard"),
        1,
        Arc::clone(&series_file),
        executor,
        true,
    );

    reefsdb.set_compactions_enabled(true);
    concrete.wait_for_injected_terminal();
    run_with_timeout(
        move || reefsdb.close().unwrap(),
        "ReefsDb close hung after accepted supervisor disposal",
    );
    series_file.close().unwrap();
}

#[derive(Clone, Copy)]
enum WalFailureMode {
    NestedRefusal,
    NestedDisposal,
    SyncDisposal,
    WorkerDisposal,
}

fn configure_wal_executor_before_open(executor: &WalTestExecutor, mode: WalFailureMode) {
    if matches!(mode, WalFailureMode::WorkerDisposal) {
        executor.drop_async_at(1);
    }
}

fn configure_wal_executor_after_open(executor: &WalTestExecutor, mode: WalFailureMode) {
    match mode {
        WalFailureMode::NestedRefusal => executor.reject_nested(),
        WalFailureMode::NestedDisposal => executor.drop_nested(),
        WalFailureMode::SyncDisposal => executor.drop_async_at(3),
        WalFailureMode::WorkerDisposal => {}
    }
}

fn wal_values() -> HashMap<String, Vec<crate::models::points::Value>> {
    HashMap::from([(
        "cpu".to_string(),
        vec![crate::models::points::Value::IntegerValue(
            crate::models::points::IntegerValue::new(1, 1),
        )],
    )])
}

fn vlog_index_values() -> HashMap<String, Vec<VLogIndex>> {
    HashMap::from([(
        "cpu".to_string(),
        vec![VLogIndex {
            count: 1,
            min: 1,
            max: 1,
            generation: 1,
            seq: 1,
            offset: 5,
            size: 16,
            ..Default::default()
        }],
    )])
}

fn assert_tsm_wal_failure_is_terminal(root: &Path, mode: WalFailureMode) {
    let concrete = Arc::new(WalTestExecutor::new());
    configure_wal_executor_before_open(&concrete, mode);
    let executor: ExecutorRef = concrete.clone();
    let wal = wal_impl::WAL::new_with_executor(
        Arc::new(root.to_path_buf()),
        1 << 20,
        Duration::ZERO,
        executor,
    )
    .unwrap();
    configure_wal_executor_after_open(&concrete, mode);

    let result = concrete.block_on(async {
        tokio::time::timeout(Duration::from_secs(2), wal.write_multi(wal_values())).await
    });
    assert!(result.is_ok(), "TSM WAL request timed out");
    assert!(result.unwrap().is_err(), "TSM WAL reported false success");
}

fn assert_vlog_wal_failure_is_terminal(root: &Path, mode: WalFailureMode) {
    let concrete = Arc::new(WalTestExecutor::new());
    configure_wal_executor_before_open(&concrete, mode);
    let executor: ExecutorRef = concrete.clone();
    let wal = vlog_wal::WAL::new_with_executor(
        Arc::new(root.to_path_buf()),
        1 << 20,
        1,
        Duration::ZERO,
        executor,
    )
    .unwrap();
    configure_wal_executor_after_open(&concrete, mode);

    let result = concrete.block_on(async {
        tokio::time::timeout(Duration::from_secs(2), wal.write_multi(wal_values())).await
    });
    assert!(result.is_ok(), "VLog WAL request timed out");
    assert!(result.unwrap().is_err(), "VLog WAL reported false success");
}

fn assert_vlog_index_wal_failure_is_terminal(root: &Path, mode: WalFailureMode) {
    let concrete = Arc::new(WalTestExecutor::new());
    configure_wal_executor_before_open(&concrete, mode);
    let executor: ExecutorRef = concrete.clone();
    let wal = vlog_index_wal::WAL::new_with_executor(
        Arc::new(root.to_path_buf()),
        1 << 20,
        Duration::ZERO,
        executor,
    )
    .unwrap();
    configure_wal_executor_after_open(&concrete, mode);

    let result = concrete.block_on(async {
        tokio::time::timeout(Duration::from_secs(2), wal.write_multi(vlog_index_values())).await
    });
    assert!(result.is_ok(), "VLog index WAL request timed out");
    assert!(
        result.unwrap().is_err(),
        "VLog index WAL reported false success"
    );
}

#[test]
fn tsm_wal_executor_failures_are_terminal_without_hanging() {
    let root = tempfile::tempdir().unwrap();
    for (index, mode) in [
        WalFailureMode::NestedRefusal,
        WalFailureMode::NestedDisposal,
        WalFailureMode::SyncDisposal,
        WalFailureMode::WorkerDisposal,
    ]
    .into_iter()
    .enumerate()
    {
        assert_tsm_wal_failure_is_terminal(&root.path().join(index.to_string()), mode);
    }
}

#[test]
fn vlog_wal_executor_failures_are_terminal_without_hanging() {
    let root = tempfile::tempdir().unwrap();
    for (index, mode) in [
        WalFailureMode::NestedRefusal,
        WalFailureMode::NestedDisposal,
        WalFailureMode::SyncDisposal,
        WalFailureMode::WorkerDisposal,
    ]
    .into_iter()
    .enumerate()
    {
        assert_vlog_wal_failure_is_terminal(&root.path().join(index.to_string()), mode);
    }
}

#[test]
fn vlog_index_wal_executor_failures_are_terminal_without_hanging() {
    let root = tempfile::tempdir().unwrap();
    for (index, mode) in [
        WalFailureMode::NestedRefusal,
        WalFailureMode::NestedDisposal,
        WalFailureMode::SyncDisposal,
        WalFailureMode::WorkerDisposal,
    ]
    .into_iter()
    .enumerate()
    {
        assert_vlog_index_wal_failure_is_terminal(&root.path().join(index.to_string()), mode);
    }
}

#[test]
fn wal_workers_do_not_retain_the_host_executor() {
    let root = tempfile::tempdir().unwrap();
    let concrete = Arc::new(WalTestExecutor::new());
    let weak = Arc::downgrade(&concrete);
    let executor: ExecutorRef = concrete.clone();
    let tsm = wal_impl::WAL::new_with_executor(
        Arc::new(root.path().join("tsm")),
        1 << 20,
        Duration::ZERO,
        Arc::clone(&executor),
    )
    .unwrap();
    let vlog = vlog_wal::WAL::new_with_executor(
        Arc::new(root.path().join("vlog")),
        1 << 20,
        1,
        Duration::ZERO,
        Arc::clone(&executor),
    )
    .unwrap();
    let vlog_index = vlog_index_wal::WAL::new_with_executor(
        Arc::new(root.path().join("vlog-index")),
        1 << 20,
        Duration::ZERO,
        Arc::clone(&executor),
    )
    .unwrap();

    drop(tsm);
    drop(vlog);
    drop(vlog_index);
    drop(executor);
    drop(concrete);
    assert!(weak.upgrade().is_none());
}
