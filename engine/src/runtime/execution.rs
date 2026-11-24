//! Host-supplied task execution for storage and coordinator components.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, LazyLock},
    task::{Context, Poll},
};

#[cfg(feature = "slow-trace")]
use std::{
    array,
    sync::{
        OnceLock,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
};

use parking_lot::{Condvar, Mutex};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError, oneshot};

use crate::runtime::{
    global,
    runtime::{AsyncRuntime, Builder, DefaultRuntime, RuntimeBuilder},
};

/// A type-erased asynchronous task accepted by [`RuntimeExecutor`].
pub type BoxFutureTask = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// A type-erased blocking task accepted by [`RuntimeExecutor`].
pub type BoxBlockingTask = Box<dyn FnOnce() + Send + 'static>;

/// Shared execution capability injected into storage components.
pub type ExecutorRef = Arc<dyn RuntimeExecutor>;

/// Diagnostic classes for work submitted to the GENERAL blocking executor.
///
/// These labels do not influence scheduling. They only attribute queue state
/// in instrumented builds.
#[cfg(feature = "slow-trace")]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum GeneralBlockingTaskClass {
    MeasurementTagKeysPartition,
    GroupedStorageLeaf,
    OtherQueryBlocking,
    OtherGeneralBlocking,
}

#[cfg(feature = "slow-trace")]
impl GeneralBlockingTaskClass {
    pub const ALL: [Self; 4] = [
        Self::MeasurementTagKeysPartition,
        Self::GroupedStorageLeaf,
        Self::OtherQueryBlocking,
        Self::OtherGeneralBlocking,
    ];

    const fn index(self) -> usize {
        match self {
            Self::MeasurementTagKeysPartition => 0,
            Self::GroupedStorageLeaf => 1,
            Self::OtherQueryBlocking => 2,
            Self::OtherGeneralBlocking => 3,
        }
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::MeasurementTagKeysPartition => "measurement_tag_keys_partition",
            Self::GroupedStorageLeaf => "grouped_storage_leaf",
            Self::OtherQueryBlocking => "other_query_blocking",
            Self::OtherGeneralBlocking => "other_general_blocking",
        }
    }
}

/// One task class's cumulative and current GENERAL blocking state.
#[cfg(feature = "slow-trace")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GeneralBlockingClassSnapshot {
    pub submitted_total: u64,
    pub started_total: u64,
    pub completed_total: u64,
    pub active_current: u64,
    pub queued_current: u64,
    pub active_peak: u64,
    pub queued_peak: u64,
}

/// Observer-only snapshot of the GENERAL blocking executor.
#[cfg(feature = "slow-trace")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GeneralBlockingExecutorSnapshot {
    pub submitted_total: u64,
    pub started_total: u64,
    pub completed_total: u64,
    pub active_current: u64,
    pub queued_current: u64,
    pub active_peak: u64,
    pub queued_peak: u64,
    pub by_class: [GeneralBlockingClassSnapshot; 4],
}

#[cfg(feature = "slow-trace")]
impl GeneralBlockingExecutorSnapshot {
    pub fn class(&self, task_class: GeneralBlockingTaskClass) -> GeneralBlockingClassSnapshot {
        self.by_class[task_class.index()]
    }
}

#[cfg(feature = "slow-trace")]
#[derive(Debug, Default)]
struct GeneralBlockingClassCounters {
    submitted_total: AtomicU64,
    started_total: AtomicU64,
    completed_total: AtomicU64,
    active_current: AtomicUsize,
    queued_current: AtomicUsize,
    active_peak: AtomicUsize,
    queued_peak: AtomicUsize,
}

#[cfg(feature = "slow-trace")]
impl GeneralBlockingClassCounters {
    fn snapshot(&self) -> GeneralBlockingClassSnapshot {
        GeneralBlockingClassSnapshot {
            submitted_total: self.submitted_total.load(Ordering::Relaxed),
            started_total: self.started_total.load(Ordering::Relaxed),
            completed_total: self.completed_total.load(Ordering::Relaxed),
            active_current: self.active_current.load(Ordering::Relaxed) as u64,
            queued_current: self.queued_current.load(Ordering::Relaxed) as u64,
            active_peak: self.active_peak.load(Ordering::Relaxed) as u64,
            queued_peak: self.queued_peak.load(Ordering::Relaxed) as u64,
        }
    }
}

#[cfg(feature = "slow-trace")]
#[derive(Debug, Default)]
struct GeneralBlockingCounters {
    submitted_total: AtomicU64,
    started_total: AtomicU64,
    completed_total: AtomicU64,
    active_current: AtomicUsize,
    queued_current: AtomicUsize,
    active_peak: AtomicUsize,
    queued_peak: AtomicUsize,
    by_class: [GeneralBlockingClassCounters; 4],
}

#[cfg(feature = "slow-trace")]
impl GeneralBlockingCounters {
    fn snapshot(&self) -> GeneralBlockingExecutorSnapshot {
        GeneralBlockingExecutorSnapshot {
            submitted_total: self.submitted_total.load(Ordering::Relaxed),
            started_total: self.started_total.load(Ordering::Relaxed),
            completed_total: self.completed_total.load(Ordering::Relaxed),
            active_current: self.active_current.load(Ordering::Relaxed) as u64,
            queued_current: self.queued_current.load(Ordering::Relaxed) as u64,
            active_peak: self.active_peak.load(Ordering::Relaxed) as u64,
            queued_peak: self.queued_peak.load(Ordering::Relaxed) as u64,
            by_class: array::from_fn(|index| self.by_class[index].snapshot()),
        }
    }

    fn submitted(&self, task_class: GeneralBlockingTaskClass) -> GeneralBlockingExecutorSnapshot {
        let class = &self.by_class[task_class.index()];
        self.submitted_total.fetch_add(1, Ordering::Relaxed);
        class.submitted_total.fetch_add(1, Ordering::Relaxed);
        let queued = self.queued_current.fetch_add(1, Ordering::Relaxed) + 1;
        self.queued_peak.fetch_max(queued, Ordering::Relaxed);
        let class_queued = class.queued_current.fetch_add(1, Ordering::Relaxed) + 1;
        class.queued_peak.fetch_max(class_queued, Ordering::Relaxed);
        self.snapshot()
    }

    fn started(&self, task_class: GeneralBlockingTaskClass) -> GeneralBlockingExecutorSnapshot {
        let class = &self.by_class[task_class.index()];
        let queued = self.queued_current.fetch_sub(1, Ordering::Relaxed);
        let class_queued = class.queued_current.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(queued > 0, "GENERAL blocking queued counter underflow");
        debug_assert!(
            class_queued > 0,
            "GENERAL blocking class queued counter underflow"
        );
        self.started_total.fetch_add(1, Ordering::Relaxed);
        class.started_total.fetch_add(1, Ordering::Relaxed);
        let active = self.active_current.fetch_add(1, Ordering::Relaxed) + 1;
        self.active_peak.fetch_max(active, Ordering::Relaxed);
        let class_active = class.active_current.fetch_add(1, Ordering::Relaxed) + 1;
        class.active_peak.fetch_max(class_active, Ordering::Relaxed);
        self.snapshot()
    }

    fn completed(&self, task_class: GeneralBlockingTaskClass) -> GeneralBlockingExecutorSnapshot {
        let class = &self.by_class[task_class.index()];
        let active = self.active_current.fetch_sub(1, Ordering::Relaxed);
        let class_active = class.active_current.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(active > 0, "GENERAL blocking active counter underflow");
        debug_assert!(
            class_active > 0,
            "GENERAL blocking class active counter underflow"
        );
        self.completed_total.fetch_add(1, Ordering::Relaxed);
        class.completed_total.fetch_add(1, Ordering::Relaxed);
        self.snapshot()
    }

    fn dropped_while_queued(
        &self,
        task_class: GeneralBlockingTaskClass,
    ) -> GeneralBlockingExecutorSnapshot {
        let class = &self.by_class[task_class.index()];
        let queued = self.queued_current.fetch_sub(1, Ordering::Relaxed);
        let class_queued = class.queued_current.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(queued > 0, "GENERAL blocking queued counter underflow");
        debug_assert!(
            class_queued > 0,
            "GENERAL blocking class queued counter underflow"
        );
        self.snapshot()
    }
}

/// Request-owned timing and executor-state attribution for one blocking task.
#[cfg(feature = "slow-trace")]
#[derive(Debug)]
pub struct GeneralBlockingTaskDiagnostics {
    task_class: GeneralBlockingTaskClass,
    submitted_at: OnceLock<Instant>,
    started_at: OnceLock<Instant>,
    finished_at: OnceLock<Instant>,
    submit_state: OnceLock<GeneralBlockingExecutorSnapshot>,
    start_state: OnceLock<GeneralBlockingExecutorSnapshot>,
    finish_state: OnceLock<GeneralBlockingExecutorSnapshot>,
}

#[cfg(feature = "slow-trace")]
impl GeneralBlockingTaskDiagnostics {
    pub fn new(task_class: GeneralBlockingTaskClass) -> Self {
        Self {
            task_class,
            submitted_at: OnceLock::new(),
            started_at: OnceLock::new(),
            finished_at: OnceLock::new(),
            submit_state: OnceLock::new(),
            start_state: OnceLock::new(),
            finish_state: OnceLock::new(),
        }
    }

    pub fn task_class(&self) -> GeneralBlockingTaskClass {
        self.task_class
    }

    fn record_submitted(&self, instant: Instant, state: GeneralBlockingExecutorSnapshot) {
        let _ = self.submitted_at.set(instant);
        let _ = self.submit_state.set(state);
    }

    fn record_started(&self, instant: Instant, state: GeneralBlockingExecutorSnapshot) {
        let _ = self.started_at.set(instant);
        let _ = self.start_state.set(state);
    }

    fn record_finished(&self, instant: Instant, state: GeneralBlockingExecutorSnapshot) {
        let _ = self.finished_at.set(instant);
        let _ = self.finish_state.set(state);
    }

    pub(crate) fn instants(&self) -> (Option<Instant>, Option<Instant>, Option<Instant>) {
        (
            self.submitted_at.get().copied(),
            self.started_at.get().copied(),
            self.finished_at.get().copied(),
        )
    }

    pub(crate) fn states(
        &self,
    ) -> (
        GeneralBlockingExecutorSnapshot,
        GeneralBlockingExecutorSnapshot,
        GeneralBlockingExecutorSnapshot,
    ) {
        (
            self.submit_state.get().copied().unwrap_or_default(),
            self.start_state.get().copied().unwrap_or_default(),
            self.finish_state.get().copied().unwrap_or_default(),
        )
    }
}

/// Process-wide admission budget for CPU-heavy storage query jobs.
///
/// Tokio's blocking pool remains responsible for executing admitted work. This
/// budget is deliberately separate from the pool size: it bounds active query
/// decode/aggregate work across every query sharing the executor without
/// constraining unrelated storage or compaction tasks.
#[derive(Debug)]
pub struct StorageQueryCpuBudget {
    capacity: usize,
    permits: Arc<Semaphore>,
    #[cfg(feature = "slow-trace")]
    active_jobs: AtomicUsize,
    #[cfg(feature = "slow-trace")]
    active_jobs_peak: AtomicUsize,
}

impl StorageQueryCpuBudget {
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            capacity,
            permits: Arc::new(Semaphore::new(capacity)),
            #[cfg(feature = "slow-trace")]
            active_jobs: AtomicUsize::new(0),
            #[cfg(feature = "slow-trace")]
            active_jobs_peak: AtomicUsize::new(0),
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn try_acquire(self: &Arc<Self>) -> Result<StorageQueryCpuPermit, TryAcquireError> {
        Arc::clone(&self.permits)
            .try_acquire_owned()
            .map(|permit| StorageQueryCpuPermit {
                permit,
                #[cfg(feature = "slow-trace")]
                budget: Arc::clone(self),
            })
    }

    pub async fn acquire(self: &Arc<Self>) -> StorageQueryCpuPermit {
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .expect("storage query CPU budget is never closed");
        StorageQueryCpuPermit {
            permit,
            #[cfg(feature = "slow-trace")]
            budget: Arc::clone(self),
        }
    }

    #[cfg(feature = "slow-trace")]
    #[doc(hidden)]
    pub fn active_jobs(&self) -> usize {
        self.active_jobs.load(Ordering::SeqCst)
    }

    #[cfg(feature = "slow-trace")]
    #[doc(hidden)]
    pub fn active_jobs_peak(&self) -> usize {
        self.active_jobs_peak.load(Ordering::SeqCst)
    }

    #[cfg(feature = "slow-trace")]
    #[doc(hidden)]
    pub fn reset_active_jobs_peak(&self) {
        assert_eq!(self.active_jobs(), 0);
        self.active_jobs_peak.store(0, Ordering::SeqCst);
    }
}

/// RAII ownership of one admitted storage query CPU job.
#[derive(Debug)]
pub struct StorageQueryCpuPermit {
    permit: OwnedSemaphorePermit,
    #[cfg(feature = "slow-trace")]
    budget: Arc<StorageQueryCpuBudget>,
}

impl StorageQueryCpuPermit {
    /// Marks the admitted job as actively executing CPU-heavy storage work.
    pub fn start(self) -> StorageQueryCpuActivePermit {
        #[cfg(feature = "slow-trace")]
        {
            let active = self.budget.active_jobs.fetch_add(1, Ordering::SeqCst) + 1;
            self.budget
                .active_jobs_peak
                .fetch_max(active, Ordering::SeqCst);
        }
        StorageQueryCpuActivePermit {
            _permit: self.permit,
            #[cfg(feature = "slow-trace")]
            budget: self.budget,
        }
    }
}

/// One actively executing CPU-heavy storage query job.
#[derive(Debug)]
pub struct StorageQueryCpuActivePermit {
    _permit: OwnedSemaphorePermit,
    #[cfg(feature = "slow-trace")]
    budget: Arc<StorageQueryCpuBudget>,
}

#[cfg(feature = "slow-trace")]
impl Drop for StorageQueryCpuActivePermit {
    fn drop(&mut self) {
        self.budget.active_jobs.fetch_sub(1, Ordering::SeqCst);
    }
}

fn default_storage_query_cpu_capacity() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1)
}

fn default_storage_query_cpu_budget() -> Arc<StorageQueryCpuBudget> {
    static BUDGET: LazyLock<Arc<StorageQueryCpuBudget>> = LazyLock::new(|| {
        Arc::new(StorageQueryCpuBudget::new(
            default_storage_query_cpu_capacity(),
        ))
    });
    Arc::clone(&BUDGET)
}

/// The executor refused to schedule a task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpawnError {
    reason: String,
}

impl SpawnError {
    pub fn rejected(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl fmt::Display for SpawnError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "task scheduling rejected: {}", self.reason)
    }
}

impl std::error::Error for SpawnError {}

/// A scheduled task ended before publishing its typed result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaskJoinError;

impl fmt::Display for TaskJoinError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("scheduled task was cancelled or failed")
    }
}

impl std::error::Error for TaskJoinError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdmissionState {
    Pending,
    Run,
    Cancel,
}

/// Operation-local admission for required blocking-task fanouts.
///
/// Submitted workers wait while the gate is pending. The owner releases all
/// workers only after every required submission succeeds, or cancels them all
/// after the first refusal. Successful fanouts therefore remain parallel while
/// selective refusal cannot expose partial work.
#[derive(Clone, Debug)]
pub struct TaskAdmissionGate {
    inner: Arc<(Mutex<AdmissionState>, Condvar)>,
}

impl TaskAdmissionGate {
    pub fn pending() -> Self {
        Self {
            inner: Arc::new((Mutex::new(AdmissionState::Pending), Condvar::new())),
        }
    }

    /// Waits for the operation owner to decide whether admitted work may run.
    pub fn wait(&self) -> bool {
        let (admission, changed) = &*self.inner;
        let mut admission = admission.lock();
        changed.wait_while(&mut admission, |state| *state == AdmissionState::Pending);
        *admission == AdmissionState::Run
    }

    pub fn run(&self) {
        self.finish(AdmissionState::Run);
    }

    pub fn cancel(&self) {
        self.finish(AdmissionState::Cancel);
    }

    fn finish(&self, decision: AdmissionState) {
        let (admission, changed) = &*self.inner;
        let mut admission = admission.lock();
        if *admission == AdmissionState::Pending {
            *admission = decision;
        }
        changed.notify_all();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TaskDisposition {
    SubmissionPending,
    Accepted,
    Completed,
}

struct AcceptedTaskState {
    disposition: TaskDisposition,
    guard_dropped: bool,
    on_cancel: Option<Box<dyn FnOnce() + Send + 'static>>,
}

/// Caller-side registration for one task closure carrying an
/// [`AcceptedTaskGuard`].
///
/// The caller must consume this value with [`Self::accepted`] after successful
/// submission or [`Self::rejected`] after `SpawnError` handling.
pub struct AcceptedTaskRegistration {
    state: Arc<Mutex<AcceptedTaskState>>,
}

impl AcceptedTaskRegistration {
    /// Transfers ownership of the guarded closure to the executor.
    pub fn accepted(self) {
        let on_cancel = {
            let mut state = self.state.lock();
            if state.disposition != TaskDisposition::SubmissionPending {
                return;
            }
            if state.guard_dropped {
                state.disposition = TaskDisposition::Completed;
                state.on_cancel.take()
            } else {
                state.disposition = TaskDisposition::Accepted;
                None
            }
        };
        if let Some(on_cancel) = on_cancel {
            on_cancel();
        }
    }

    /// Marks the closure as refused after the caller publishes the typed
    /// scheduling error. Cancellation cleanup is not run for refused work.
    pub fn rejected(self) {
        let mut state = self.state.lock();
        state.disposition = TaskDisposition::Completed;
        state.on_cancel = None;
    }
}

/// Task-owned terminal-disposition guard.
///
/// If an accepted closure is dropped before execution, cancelled while
/// pending, or unwinds, dropping this guard runs the registered cancellation
/// callback exactly once. A normally finishing task must call
/// [`Self::complete`] or [`Self::complete_with`].
pub struct AcceptedTaskGuard {
    state: Option<Arc<Mutex<AcceptedTaskState>>>,
}

impl AcceptedTaskGuard {
    pub fn complete(mut self) {
        self.complete_inner();
    }

    pub fn complete_with(mut self, publish: impl FnOnce()) {
        self.complete_inner();
        publish();
    }

    fn complete_inner(&mut self) {
        if let Some(state) = self.state.take() {
            let mut state = state.lock();
            state.disposition = TaskDisposition::Completed;
            state.on_cancel = None;
        }
    }
}

impl Drop for AcceptedTaskGuard {
    fn drop(&mut self) {
        let Some(state) = self.state.take() else {
            return;
        };
        let on_cancel = {
            let mut state = state.lock();
            match state.disposition {
                TaskDisposition::SubmissionPending => {
                    state.guard_dropped = true;
                    None
                }
                TaskDisposition::Accepted => {
                    state.disposition = TaskDisposition::Completed;
                    state.on_cancel.take()
                }
                TaskDisposition::Completed => None,
            }
        };
        if let Some(on_cancel) = on_cancel {
            on_cancel();
        }
    }
}

/// Creates the caller registration and task guard for one scheduled closure.
pub fn accepted_task_guard(
    on_cancel: impl FnOnce() + Send + 'static,
) -> (AcceptedTaskRegistration, AcceptedTaskGuard) {
    let state = Arc::new(Mutex::new(AcceptedTaskState {
        disposition: TaskDisposition::SubmissionPending,
        guard_dropped: false,
        on_cancel: Some(Box::new(on_cancel)),
    }));
    (
        AcceptedTaskRegistration {
            state: Arc::clone(&state),
        },
        AcceptedTaskGuard { state: Some(state) },
    )
}

/// Runtime-neutral cancellation control for one scheduled task.
pub struct TaskController {
    abort: Arc<dyn Fn() + Send + Sync>,
}

impl TaskController {
    pub fn new(abort: impl Fn() + Send + Sync + 'static) -> Self {
        Self {
            abort: Arc::new(abort),
        }
    }

    pub fn detached() -> Self {
        Self::new(|| {})
    }

    pub fn abort(&self) {
        (self.abort)();
    }
}

impl Clone for TaskController {
    fn clone(&self) -> Self {
        Self {
            abort: Arc::clone(&self.abort),
        }
    }
}

impl fmt::Debug for TaskController {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskController")
            .finish_non_exhaustive()
    }
}

/// A runtime-neutral, typed handle for an injected executor task.
///
/// Dropping a handle detaches the task. Call [`TaskHandle::abort`] for
/// task-scoped cancellation; aborting a task never shuts down the host executor.
pub struct TaskHandle<T> {
    result: oneshot::Receiver<T>,
    controller: TaskController,
}

impl<T> TaskHandle<T> {
    pub fn abort(&self) {
        self.controller.abort();
    }
}

impl<T> fmt::Debug for TaskHandle<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("TaskHandle").finish_non_exhaustive()
    }
}

impl<T> Future for TaskHandle<T> {
    type Output = Result<T, TaskJoinError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.result)
            .poll(context)
            .map(|result| result.map_err(|_| TaskJoinError))
    }
}

/// Object-safe semantic scheduling boundary used by ReefsDB storage.
///
/// The host owns concrete runtime resources. Implementations may map the four
/// lanes to one or several pools, but storage never discovers a runtime
/// globally and never controls host shutdown through this interface.
///
/// The lanes preserve the existing execution roles:
/// - `schedule`: asynchronous coordination and worker loops;
/// - `schedule_blocking`: general storage and filesystem work;
/// - `schedule_compact`: asynchronous snapshot/compaction coordination;
/// - `schedule_blocking_compact`: CPU- or IO-heavy snapshot/compaction work.
///
/// Returning [`SpawnError`] means the task was not accepted and its closure was
/// dropped. After a successful scheduling call, ownership of the closure has
/// transferred to the executor. The executor must eventually execute or drop
/// that closure; accepted work must never be leaked or silently forgotten.
/// Dropping a closure is the executor-shutdown/cancellation disposition and runs
/// any task-owned RAII cleanup. An accepted scheduling call still returns
/// without waiting for task success. Implementations must make
/// [`TaskController::abort`] task-scoped.
pub trait RuntimeExecutor: Send + Sync + 'static {
    fn schedule(&self, task: BoxFutureTask) -> Result<TaskController, SpawnError>;

    fn schedule_blocking(&self, task: BoxBlockingTask) -> Result<TaskController, SpawnError>;

    /// Schedules GENERAL blocking work with observer-only task attribution.
    #[cfg(feature = "slow-trace")]
    fn schedule_blocking_with_class(
        &self,
        task_class: GeneralBlockingTaskClass,
        diagnostics: Arc<GeneralBlockingTaskDiagnostics>,
        task: BoxBlockingTask,
    ) -> Result<TaskController, SpawnError> {
        let submitted_at = Instant::now();
        diagnostics.record_submitted(submitted_at, GeneralBlockingExecutorSnapshot::default());
        self.schedule_blocking(Box::new(move || {
            diagnostics.record_started(Instant::now(), GeneralBlockingExecutorSnapshot::default());
            task();
            diagnostics.record_finished(Instant::now(), GeneralBlockingExecutorSnapshot::default());
            let _ = task_class;
        }))
    }

    /// Schedules one CPU-heavy storage query job on the normal blocking lane.
    ///
    /// The default deliberately reuses `schedule_blocking`; this is a semantic
    /// seam for focused admission tests and host observability, not a new pool.
    fn schedule_storage_query_cpu(
        &self,
        task: BoxBlockingTask,
    ) -> Result<TaskController, SpawnError> {
        self.schedule_blocking(task)
    }

    /// Instrumented form of [`Self::schedule_storage_query_cpu`].
    #[cfg(feature = "slow-trace")]
    fn schedule_storage_query_cpu_with_diagnostics(
        &self,
        diagnostics: Arc<GeneralBlockingTaskDiagnostics>,
        task: BoxBlockingTask,
    ) -> Result<TaskController, SpawnError> {
        self.schedule_blocking_with_class(
            GeneralBlockingTaskClass::GroupedStorageLeaf,
            diagnostics,
            task,
        )
    }

    fn schedule_compact(&self, task: BoxFutureTask) -> Result<TaskController, SpawnError>;

    fn schedule_blocking_compact(
        &self,
        task: BoxBlockingTask,
    ) -> Result<TaskController, SpawnError>;

    /// Global CPU admission budget shared by storage queries on this executor.
    fn storage_query_cpu_budget(&self) -> Arc<StorageQueryCpuBudget> {
        default_storage_query_cpu_budget()
    }
}

/// Typed helpers layered over the object-safe scheduling primitives.
pub trait RuntimeExecutorExt: RuntimeExecutor {
    fn spawn<F>(&self, future: F) -> Result<TaskHandle<F::Output>, SpawnError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (sender, result) = oneshot::channel();
        let controller = self.schedule(Box::pin(async move {
            let _ = sender.send(future.await);
        }))?;
        Ok(TaskHandle { result, controller })
    }

    fn spawn_blocking<F, T>(&self, task: F) -> Result<TaskHandle<T>, SpawnError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let (sender, result) = oneshot::channel();
        let controller = self.schedule_blocking(Box::new(move || {
            let _ = sender.send(task());
        }))?;
        Ok(TaskHandle { result, controller })
    }

    #[cfg(feature = "slow-trace")]
    fn spawn_blocking_with_class<F, T>(
        &self,
        task_class: GeneralBlockingTaskClass,
        diagnostics: Arc<GeneralBlockingTaskDiagnostics>,
        task: F,
    ) -> Result<TaskHandle<T>, SpawnError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let (sender, result) = oneshot::channel();
        let controller = self.schedule_blocking_with_class(
            task_class,
            diagnostics,
            Box::new(move || {
                let _ = sender.send(task());
            }),
        )?;
        Ok(TaskHandle { result, controller })
    }

    fn spawn_storage_query_cpu<F, T>(&self, task: F) -> Result<TaskHandle<T>, SpawnError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let (sender, result) = oneshot::channel();
        let controller = self.schedule_storage_query_cpu(Box::new(move || {
            let _ = sender.send(task());
        }))?;
        Ok(TaskHandle { result, controller })
    }

    #[cfg(feature = "slow-trace")]
    fn spawn_storage_query_cpu_with_diagnostics<F, T>(
        &self,
        diagnostics: Arc<GeneralBlockingTaskDiagnostics>,
        task: F,
    ) -> Result<TaskHandle<T>, SpawnError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let (sender, result) = oneshot::channel();
        let controller = self.schedule_storage_query_cpu_with_diagnostics(
            diagnostics,
            Box::new(move || {
                let _ = sender.send(task());
            }),
        )?;
        Ok(TaskHandle { result, controller })
    }

    fn spawn_compact<F>(&self, future: F) -> Result<TaskHandle<F::Output>, SpawnError>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (sender, result) = oneshot::channel();
        let controller = self.schedule_compact(Box::pin(async move {
            let _ = sender.send(future.await);
        }))?;
        Ok(TaskHandle { result, controller })
    }

    fn spawn_blocking_compact<F, T>(&self, task: F) -> Result<TaskHandle<T>, SpawnError>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let (sender, result) = oneshot::channel();
        let controller = self.schedule_blocking_compact(Box::new(move || {
            let _ = sender.send(task());
        }))?;
        Ok(TaskHandle { result, controller })
    }
}

impl<T: RuntimeExecutor + ?Sized> RuntimeExecutorExt for T {}

/// Resource sizing for [`DefaultTokioExecutor`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DefaultTokioExecutorOptions {
    pub general_workers: usize,
    pub general_blocking_workers: usize,
    pub compact_workers: usize,
    pub compact_blocking_workers: usize,
}

impl DefaultTokioExecutorOptions {
    pub fn new(
        general_workers: usize,
        general_blocking_workers: usize,
        compact_workers: usize,
        compact_blocking_workers: usize,
    ) -> Self {
        let legacy = global::RuntimeOptions::new(
            general_workers,
            general_blocking_workers,
            compact_workers,
            compact_blocking_workers,
        );
        Self::from(legacy)
    }
}

impl Default for DefaultTokioExecutorOptions {
    fn default() -> Self {
        Self::from(global::RuntimeOptions::default())
    }
}

impl From<global::RuntimeOptions> for DefaultTokioExecutorOptions {
    fn from(options: global::RuntimeOptions) -> Self {
        Self {
            general_workers: options.global_rt_size,
            general_blocking_workers: options.global_blocking_rt_size,
            compact_workers: options.compact_rt_size,
            compact_blocking_workers: options.compact_blocking_rt_size,
        }
    }
}

/// Explicit Tokio implementation for standalone programs and host applications.
#[derive(Clone, Debug)]
pub struct DefaultTokioExecutor {
    general: DefaultRuntime,
    compact: DefaultRuntime,
    storage_query_cpu_budget: Arc<StorageQueryCpuBudget>,
    #[cfg(feature = "slow-trace")]
    general_blocking_counters: Arc<GeneralBlockingCounters>,
}

#[cfg(feature = "slow-trace")]
struct GeneralBlockingTaskGuard {
    counters: Arc<GeneralBlockingCounters>,
    diagnostics: Arc<GeneralBlockingTaskDiagnostics>,
    task_class: GeneralBlockingTaskClass,
    started: bool,
}

#[cfg(feature = "slow-trace")]
impl GeneralBlockingTaskGuard {
    fn start(&mut self) {
        let started_at = Instant::now();
        let state = self.counters.started(self.task_class);
        self.started = true;
        self.diagnostics.record_started(started_at, state);
    }
}

#[cfg(feature = "slow-trace")]
impl Drop for GeneralBlockingTaskGuard {
    fn drop(&mut self) {
        let finished_at = Instant::now();
        let state = if self.started {
            self.counters.completed(self.task_class)
        } else {
            self.counters.dropped_while_queued(self.task_class)
        };
        self.diagnostics.record_finished(finished_at, state);
    }
}

impl DefaultTokioExecutor {
    pub fn new(options: DefaultTokioExecutorOptions) -> crate::runtime::runtime::Result<Self> {
        let mut executor = Self::new_with_storage_query_cpu_parallelism(
            options,
            default_storage_query_cpu_capacity(),
        )?;
        executor.storage_query_cpu_budget = default_storage_query_cpu_budget();
        Ok(executor)
    }

    /// Constructs an executor with an explicit storage query CPU limit.
    ///
    /// This is primarily a deterministic test/benchmark seam. Product startup
    /// uses [`Self::new`], whose default follows `available_parallelism()`.
    pub fn new_with_storage_query_cpu_parallelism(
        options: DefaultTokioExecutorOptions,
        storage_query_cpu_parallelism: usize,
    ) -> crate::runtime::runtime::Result<Self> {
        let general = Builder::default()
            .runtime_name("reefsdb_general")
            .thread_name("reefsdb_general_worker")
            .worker_threads(options.general_workers)
            .max_blocking_threads(options.general_blocking_workers)
            .build()?;
        let compact = Builder::default()
            .runtime_name("reefsdb_compact")
            .thread_name("reefsdb_compact_worker")
            .worker_threads(options.compact_workers)
            .max_blocking_threads(options.compact_blocking_workers)
            .build()?;
        Ok(Self {
            general,
            compact,
            storage_query_cpu_budget: Arc::new(StorageQueryCpuBudget::new(
                storage_query_cpu_parallelism,
            )),
            #[cfg(feature = "slow-trace")]
            general_blocking_counters: Arc::new(GeneralBlockingCounters::default()),
        })
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.general.block_on(future)
    }

    fn from_runtimes(general: DefaultRuntime, compact: DefaultRuntime) -> Self {
        Self {
            general,
            compact,
            storage_query_cpu_budget: default_storage_query_cpu_budget(),
            #[cfg(feature = "slow-trace")]
            general_blocking_counters: Arc::new(GeneralBlockingCounters::default()),
        }
    }

    fn task_controller(handle: tokio::task::JoinHandle<()>) -> TaskController {
        let abort = handle.abort_handle();
        TaskController::new(move || abort.abort())
    }

    #[cfg(feature = "slow-trace")]
    fn schedule_general_blocking(
        &self,
        task_class: GeneralBlockingTaskClass,
        diagnostics: Arc<GeneralBlockingTaskDiagnostics>,
        task: BoxBlockingTask,
    ) -> TaskController {
        let submitted_at = Instant::now();
        let state = self.general_blocking_counters.submitted(task_class);
        diagnostics.record_submitted(submitted_at, state);
        let mut guard = GeneralBlockingTaskGuard {
            counters: Arc::clone(&self.general_blocking_counters),
            diagnostics,
            task_class,
            started: false,
        };
        Self::task_controller(self.general.spawn_blocking(move || {
            guard.start();
            task();
        }))
    }
}

impl RuntimeExecutor for DefaultTokioExecutor {
    fn schedule(&self, task: BoxFutureTask) -> Result<TaskController, SpawnError> {
        Ok(Self::task_controller(self.general.spawn(task)))
    }

    fn schedule_blocking(&self, task: BoxBlockingTask) -> Result<TaskController, SpawnError> {
        #[cfg(feature = "slow-trace")]
        {
            let diagnostics = Arc::new(GeneralBlockingTaskDiagnostics::new(
                GeneralBlockingTaskClass::OtherGeneralBlocking,
            ));
            return Ok(self.schedule_general_blocking(
                GeneralBlockingTaskClass::OtherGeneralBlocking,
                diagnostics,
                task,
            ));
        }
        #[cfg(not(feature = "slow-trace"))]
        Ok(Self::task_controller(self.general.spawn_blocking(task)))
    }

    #[cfg(feature = "slow-trace")]
    fn schedule_blocking_with_class(
        &self,
        task_class: GeneralBlockingTaskClass,
        diagnostics: Arc<GeneralBlockingTaskDiagnostics>,
        task: BoxBlockingTask,
    ) -> Result<TaskController, SpawnError> {
        debug_assert_eq!(task_class, diagnostics.task_class());
        Ok(self.schedule_general_blocking(task_class, diagnostics, task))
    }

    fn schedule_storage_query_cpu(
        &self,
        task: BoxBlockingTask,
    ) -> Result<TaskController, SpawnError> {
        #[cfg(feature = "slow-trace")]
        {
            let diagnostics = Arc::new(GeneralBlockingTaskDiagnostics::new(
                GeneralBlockingTaskClass::GroupedStorageLeaf,
            ));
            return Ok(self.schedule_general_blocking(
                GeneralBlockingTaskClass::GroupedStorageLeaf,
                diagnostics,
                task,
            ));
        }
        #[cfg(not(feature = "slow-trace"))]
        Ok(Self::task_controller(self.general.spawn_blocking(task)))
    }

    #[cfg(feature = "slow-trace")]
    fn schedule_storage_query_cpu_with_diagnostics(
        &self,
        diagnostics: Arc<GeneralBlockingTaskDiagnostics>,
        task: BoxBlockingTask,
    ) -> Result<TaskController, SpawnError> {
        debug_assert_eq!(
            GeneralBlockingTaskClass::GroupedStorageLeaf,
            diagnostics.task_class()
        );
        Ok(self.schedule_general_blocking(
            GeneralBlockingTaskClass::GroupedStorageLeaf,
            diagnostics,
            task,
        ))
    }

    fn schedule_compact(&self, task: BoxFutureTask) -> Result<TaskController, SpawnError> {
        Ok(Self::task_controller(self.compact.spawn(task)))
    }

    fn schedule_blocking_compact(
        &self,
        task: BoxBlockingTask,
    ) -> Result<TaskController, SpawnError> {
        Ok(Self::task_controller(self.compact.spawn_blocking(task)))
    }

    fn storage_query_cpu_budget(&self) -> Arc<StorageQueryCpuBudget> {
        Arc::clone(&self.storage_query_cpu_budget)
    }
}

/// Compatibility adapter for low-level APIs that still expose their historical
/// constructors. Normal ReefsDb and coordinator paths inject an executor and do
/// not call this function.
pub fn legacy_global_executor() -> ExecutorRef {
    Arc::new(DefaultTokioExecutor::from_runtimes(
        global::global_runtime(),
        global::compact_runtime(),
    ))
}

#[cfg(test)]
mod tests {
    use std::{
        panic::{AssertUnwindSafe, catch_unwind},
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use super::*;

    struct RejectingExecutor;

    impl RuntimeExecutor for RejectingExecutor {
        fn schedule(&self, _task: BoxFutureTask) -> Result<TaskController, SpawnError> {
            Err(SpawnError::rejected("stopping"))
        }

        fn schedule_blocking(&self, _task: BoxBlockingTask) -> Result<TaskController, SpawnError> {
            Err(SpawnError::rejected("stopping"))
        }

        fn schedule_compact(&self, _task: BoxFutureTask) -> Result<TaskController, SpawnError> {
            Err(SpawnError::rejected("stopping"))
        }

        fn schedule_blocking_compact(
            &self,
            _task: BoxBlockingTask,
        ) -> Result<TaskController, SpawnError> {
            Err(SpawnError::rejected("stopping"))
        }
    }

    #[derive(Default)]
    struct ControlledExecutor {
        blocking: Mutex<Vec<BoxBlockingTask>>,
    }

    impl ControlledExecutor {
        fn run_next(&self) {
            let task = self.blocking.lock().pop().expect("accepted task");
            task();
        }

        fn shutdown(&self) {
            self.blocking.lock().clear();
        }
    }

    impl RuntimeExecutor for ControlledExecutor {
        fn schedule(&self, _task: BoxFutureTask) -> Result<TaskController, SpawnError> {
            Err(SpawnError::rejected("async lane unused by test"))
        }

        fn schedule_blocking(&self, task: BoxBlockingTask) -> Result<TaskController, SpawnError> {
            self.blocking.lock().push(task);
            Ok(TaskController::detached())
        }

        fn schedule_compact(&self, _task: BoxFutureTask) -> Result<TaskController, SpawnError> {
            Err(SpawnError::rejected("compact lane unused by test"))
        }

        fn schedule_blocking_compact(
            &self,
            _task: BoxBlockingTask,
        ) -> Result<TaskController, SpawnError> {
            Err(SpawnError::rejected("compact lane unused by test"))
        }
    }

    fn test_executor() -> DefaultTokioExecutor {
        DefaultTokioExecutor::new(DefaultTokioExecutorOptions::new(2, 4, 1, 2)).unwrap()
    }

    #[test]
    fn runtime_executor_is_object_safe() {
        let runtime = test_executor();
        let executor: ExecutorRef = Arc::new(runtime.clone());
        let handle = executor.spawn_blocking(|| 42).unwrap();
        assert_eq!(runtime.block_on(handle).unwrap(), 42);
    }

    #[cfg(feature = "slow-trace")]
    #[test]
    fn general_blocking_attribution_tracks_class_queue_state_and_drains() {
        let executor = DefaultTokioExecutor::new(DefaultTokioExecutorOptions::new(1, 1, 1, 1))
            .expect("test executor");
        let (started_sender, started_receiver) = std::sync::mpsc::channel();
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let blocker = executor
            .spawn_blocking(move || {
                started_sender.send(()).expect("publish blocker start");
                release_receiver.recv().expect("release blocker");
            })
            .expect("schedule blocker");
        started_receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("blocker starts");

        let tag = Arc::new(GeneralBlockingTaskDiagnostics::new(
            GeneralBlockingTaskClass::MeasurementTagKeysPartition,
        ));
        let tag_task = executor
            .spawn_blocking_with_class(
                GeneralBlockingTaskClass::MeasurementTagKeysPartition,
                Arc::clone(&tag),
                || 11,
            )
            .expect("schedule tag-key task");
        let storage = Arc::new(GeneralBlockingTaskDiagnostics::new(
            GeneralBlockingTaskClass::GroupedStorageLeaf,
        ));
        let storage_task = executor
            .spawn_storage_query_cpu_with_diagnostics(Arc::clone(&storage), || 22)
            .expect("schedule storage task");

        let (tag_submit, _, _) = tag.states();
        assert_eq!(tag_submit.active_current, 1);
        assert_eq!(tag_submit.queued_current, 1);
        assert_eq!(
            tag_submit
                .class(GeneralBlockingTaskClass::MeasurementTagKeysPartition)
                .queued_current,
            1
        );
        assert_eq!(
            tag_submit
                .class(GeneralBlockingTaskClass::OtherGeneralBlocking)
                .active_current,
            1
        );
        let (storage_submit, _, _) = storage.states();
        assert_eq!(storage_submit.active_current, 1);
        assert_eq!(storage_submit.queued_current, 2);
        assert_eq!(
            storage_submit
                .class(GeneralBlockingTaskClass::MeasurementTagKeysPartition)
                .queued_current,
            1
        );
        assert_eq!(
            storage_submit
                .class(GeneralBlockingTaskClass::GroupedStorageLeaf)
                .queued_current,
            1
        );

        release_sender.send(()).expect("release blocker");
        executor.block_on(async {
            blocker.await.expect("join blocker");
            assert_eq!(tag_task.await.expect("join tag-key task"), 11);
            assert_eq!(storage_task.await.expect("join storage task"), 22);
        });

        let final_state = executor.general_blocking_counters.snapshot();
        assert_eq!(final_state.submitted_total, 3);
        assert_eq!(final_state.started_total, 3);
        assert_eq!(final_state.completed_total, 3);
        assert_eq!(final_state.active_current, 0);
        assert_eq!(final_state.queued_current, 0);
        for task_class in [
            GeneralBlockingTaskClass::MeasurementTagKeysPartition,
            GeneralBlockingTaskClass::GroupedStorageLeaf,
            GeneralBlockingTaskClass::OtherGeneralBlocking,
        ] {
            let class = final_state.class(task_class);
            assert_eq!(class.submitted_total, 1);
            assert_eq!(class.started_total, 1);
            assert_eq!(class.completed_total, 1);
            assert_eq!(class.active_current, 0);
            assert_eq!(class.queued_current, 0);
        }
        let other_query = final_state.class(GeneralBlockingTaskClass::OtherQueryBlocking);
        assert_eq!(other_query.submitted_total, 0);
        assert_eq!(other_query.active_current, 0);
        assert_eq!(other_query.queued_current, 0);
    }

    #[test]
    fn storage_query_cpu_budget_defaults_to_host_parallelism_and_accepts_override() {
        let default = test_executor();
        let second_default = test_executor();
        assert_eq!(
            default.storage_query_cpu_budget().capacity(),
            default_storage_query_cpu_capacity()
        );
        assert!(Arc::ptr_eq(
            &default.storage_query_cpu_budget(),
            &second_default.storage_query_cpu_budget()
        ));

        let overridden = DefaultTokioExecutor::new_with_storage_query_cpu_parallelism(
            DefaultTokioExecutorOptions::new(2, 4, 1, 2),
            3,
        )
        .unwrap();
        assert_eq!(overridden.storage_query_cpu_budget().capacity(), 3);
    }

    #[test]
    fn storage_query_cpu_permit_is_released_during_worker_panic() {
        let budget = Arc::new(StorageQueryCpuBudget::new(1));
        let permit = budget.try_acquire().unwrap();
        assert!(budget.try_acquire().is_err());

        assert!(
            catch_unwind(AssertUnwindSafe(move || {
                let _active = permit.start();
                panic!("injected storage query CPU worker panic");
            }))
            .is_err()
        );
        assert!(budget.try_acquire().is_ok());
    }

    #[test]
    fn default_executor_preserves_all_execution_lanes() {
        let executor = test_executor();
        let total = Arc::new(AtomicUsize::new(0));
        let async_total = Arc::clone(&total);
        let blocking_total = Arc::clone(&total);
        let compact_total = Arc::clone(&total);
        let blocking_compact_total = Arc::clone(&total);

        let a = executor
            .spawn(async move { async_total.fetch_add(1, Ordering::SeqCst) })
            .unwrap();
        let b = executor
            .spawn_blocking(move || blocking_total.fetch_add(1, Ordering::SeqCst))
            .unwrap();
        let c = executor
            .spawn_compact(async move { compact_total.fetch_add(1, Ordering::SeqCst) })
            .unwrap();
        let d = executor
            .spawn_blocking_compact(move || blocking_compact_total.fetch_add(1, Ordering::SeqCst))
            .unwrap();

        executor.block_on(async {
            a.await.unwrap();
            b.await.unwrap();
            c.await.unwrap();
            d.await.unwrap();
        });
        assert_eq!(total.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn spawn_refusal_is_typed_and_does_not_panic() {
        let executor = RejectingExecutor;
        let error = executor.spawn(async {}).unwrap_err();
        assert_eq!(error.reason(), "stopping");
    }

    #[test]
    fn refused_task_uses_caller_error_and_not_accepted_cancellation() {
        let executor = RejectingExecutor;
        let (sender, receiver) = crossbeam_channel::bounded(2);
        let cancelled = sender.clone();
        let (registration, guard) = accepted_task_guard(move || {
            let _ = cancelled.send("cancelled");
        });

        let error = executor
            .schedule_blocking(Box::new(move || guard.complete()))
            .unwrap_err();
        sender.send("rejected").unwrap();
        registration.rejected();

        assert_eq!(error.reason(), "stopping");
        assert_eq!(receiver.recv().unwrap(), "rejected");
        assert!(receiver.recv_timeout(Duration::from_millis(50)).is_err());
    }

    #[test]
    fn accepted_task_executes_and_completes_once() {
        let executor = ControlledExecutor::default();
        let (sender, receiver) = crossbeam_channel::bounded(2);
        let cancelled = sender.clone();
        let completed = sender.clone();
        let (registration, guard) = accepted_task_guard(move || {
            let _ = cancelled.send("cancelled");
        });
        executor
            .schedule_blocking(Box::new(move || {
                guard.complete_with(|| {
                    let _ = completed.send("completed");
                });
            }))
            .unwrap();
        registration.accepted();

        executor.run_next();
        assert_eq!(receiver.recv().unwrap(), "completed");
        assert!(receiver.recv_timeout(Duration::from_millis(50)).is_err());
    }

    #[test]
    fn accepted_task_operation_error_completes_once_without_cancellation() {
        let executor = ControlledExecutor::default();
        let (sender, receiver) = crossbeam_channel::bounded(2);
        let cancelled = sender.clone();
        let failed = sender.clone();
        let (registration, guard) = accepted_task_guard(move || {
            let _ = cancelled.send("cancelled");
        });
        executor
            .schedule_blocking(Box::new(move || {
                let result: Result<(), &'static str> = Err("operation failed");
                guard.complete_with(|| {
                    let _ = failed.send(result.unwrap_err());
                });
            }))
            .unwrap();
        registration.accepted();

        executor.run_next();
        assert_eq!(receiver.recv().unwrap(), "operation failed");
        assert!(receiver.recv_timeout(Duration::from_millis(50)).is_err());
    }

    #[test]
    fn accepted_task_dropped_during_shutdown_is_cancelled_once() {
        let executor = ControlledExecutor::default();
        let (sender, receiver) = crossbeam_channel::bounded(2);
        let (registration, guard) = accepted_task_guard(move || {
            let _ = sender.send("cancelled");
        });
        executor
            .schedule_blocking(Box::new(move || guard.complete()))
            .unwrap();
        registration.accepted();

        executor.shutdown();
        assert_eq!(receiver.recv().unwrap(), "cancelled");
        assert!(receiver.recv_timeout(Duration::from_millis(50)).is_err());
    }

    #[test]
    fn accepted_task_panic_runs_terminal_cancellation_once() {
        let executor = ControlledExecutor::default();
        let (sender, receiver) = crossbeam_channel::bounded(2);
        let (registration, guard) = accepted_task_guard(move || {
            let _ = sender.send("cancelled");
        });
        executor
            .schedule_blocking(Box::new(move || {
                let _guard = guard;
                panic!("injected accepted task panic");
            }))
            .unwrap();
        registration.accepted();

        assert!(catch_unwind(AssertUnwindSafe(|| executor.run_next())).is_err());
        assert_eq!(receiver.recv().unwrap(), "cancelled");
        assert!(receiver.recv_timeout(Duration::from_millis(50)).is_err());
    }

    #[test]
    fn accepted_task_abort_runs_terminal_cancellation_once() {
        let executor = test_executor();
        let (sender, receiver) = crossbeam_channel::bounded(2);
        let (registration, guard) = accepted_task_guard(move || {
            let _ = sender.send("cancelled");
        });
        let task = executor
            .spawn(async move {
                let _guard = guard;
                std::future::pending::<()>().await;
            })
            .unwrap();
        registration.accepted();

        task.abort();
        assert!(executor.block_on(task).is_err());
        assert_eq!(receiver.recv().unwrap(), "cancelled");
        assert!(receiver.recv_timeout(Duration::from_millis(50)).is_err());
    }

    #[test]
    fn task_abort_is_scoped_to_one_task_and_keeps_executor_usable() {
        let executor = test_executor();
        let task = executor
            .spawn(async {
                std::future::pending::<()>().await;
            })
            .unwrap();
        task.abort();
        assert!(executor.block_on(task).is_err());

        let next = executor.spawn_blocking(|| 42).unwrap();
        assert_eq!(executor.block_on(next).unwrap(), 42);
    }
}
