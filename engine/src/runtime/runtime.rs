//! Tokio runtime wrapper

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use snafu::{ResultExt, Snafu};
pub use tokio::task::{JoinError, JoinHandle};
use tokio::{
    runtime::{Builder as TokioRuntimeBuilder, Handle},
    sync::oneshot,
};

// configurations
pub type Runtime = DefaultRuntime;

static RUNTIME_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Failed to build runtime: {}", source))]
    BuildRuntime { source: std::io::Error },
}

pub type Result<T> = std::result::Result<T, Error>;

/// Dropping the dropper will cause runtime to shutdown.
#[derive(Debug)]
pub struct Dropper {
    close: Option<oneshot::Sender<()>>,
}

impl Drop for Dropper {
    fn drop(&mut self) {
        // Send a signal to say i am dropping.
        let _ = self.close.take().map(|v| v.send(()));
    }
}

pub trait AsyncRuntime {
    /// Get a runtime builder
    fn builder() -> Builder {
        Builder::default()
    }

    /// Spawn a future and execute it in this thread pool
    ///
    /// Similar to tokio::runtime::Runtime::spawn()
    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;

    /// Run the provided function on an executor dedicated to blocking
    /// operations.
    fn spawn_blocking<F, R>(&self, func: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static;

    /// Run a future to complete, this is the runtime's entry point
    fn block_on<F: Future>(&self, future: F) -> F::Output;

    /// Get the name of the runtime
    fn name(&self) -> &str;
}

pub trait RuntimeBuilder<R: AsyncRuntime> {
    fn build(&mut self) -> Result<R>;
}

pub struct Builder {
    runtime_name: String,
    thread_name: String,
    builder: TokioRuntimeBuilder,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            runtime_name: format!("runtime-{}", RUNTIME_ID.fetch_add(1, Ordering::Relaxed)),
            thread_name: "default-thread-worker".to_string(),
            builder: TokioRuntimeBuilder::new_multi_thread(),
        }
    }
}

impl Builder {
    /// Sets the number of worker threads the Runtime will use.
    ///
    /// This can be any number above 0. The default value is the number of cores available to the system.
    pub fn worker_threads(&mut self, val: usize) -> &mut Self {
        let _ = self.builder.worker_threads(val);
        self
    }

    /// Specifies the limit for additional threads spawned by the Runtime.
    ///
    /// These threads are used for blocking operations like tasks spawned through spawn_blocking,
    /// they are not always active and will exit if left idle for too long, You can change this timeout duration
    /// with thread_keep_alive. The default value is 512.
    pub fn max_blocking_threads(&mut self, val: usize) -> &mut Self {
        let _ = self.builder.max_blocking_threads(val);
        self
    }

    /// Sets a custom timeout for a thread in the blocking pool.
    ///
    /// By default, the timeout for a thread is set to 10 seconds.
    pub fn thread_keep_alive(&mut self, duration: Duration) -> &mut Self {
        let _ = self.builder.thread_keep_alive(duration);
        self
    }

    pub fn runtime_name(&mut self, val: impl Into<String>) -> &mut Self {
        self.runtime_name = val.into();
        self
    }

    /// Sets name of threads spawned by the Runtime thread pool
    pub fn thread_name(&mut self, val: impl Into<String>) -> &mut Self {
        self.thread_name = val.into();
        self
    }
}

impl RuntimeBuilder<DefaultRuntime> for Builder {
    fn build(&mut self) -> Result<DefaultRuntime> {
        let runtime = self
            .builder
            .enable_all()
            .thread_name(self.thread_name.clone())
            .build()
            .context(BuildRuntimeSnafu)?;

        let name = self.runtime_name.clone();
        let handle = runtime.handle().clone();
        let (send_stop, recv_stop) = oneshot::channel();
        // Block the runtime to shutdown.
        let _ = std::thread::Builder::new()
            .name(format!("{}-blocker", self.thread_name))
            .spawn(move || runtime.block_on(recv_stop));

        Ok(DefaultRuntime::new(
            &name,
            handle,
            Arc::new(Dropper {
                close: Some(send_stop),
            }),
        ))
    }
}

/// A runtime to run future tasks
#[derive(Clone, Debug)]
pub struct DefaultRuntime {
    name: String,
    handle: Handle,
    // Used to receive a drop signal when dropper is dropped, inspired by databend
    _dropper: Arc<Dropper>, // Runtime被drop时，_dropper发送oneshot::Sender::send信号
}

impl DefaultRuntime {
    pub fn new(name: &str, handle: Handle, dropper: Arc<Dropper>) -> Self {
        Self {
            name: name.to_string(),
            handle,
            _dropper: dropper,
        }
    }
}

impl AsyncRuntime for DefaultRuntime {
    fn builder() -> Builder {
        Builder::default()
    }

    /// Spawn a future and execute it in this thread pool
    ///
    /// Similar to tokio::runtime::Runtime::spawn()
    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }

    /// Run the provided function on an executor dedicated to blocking
    /// operations.
    fn spawn_blocking<F, R>(&self, func: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.handle.spawn_blocking(func)
    }

    /// Run a future to complete, this is the runtime's entry point
    fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.handle.block_on(future)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {

    use std::{sync::Arc, thread, time::Duration};

    use tokio::sync::oneshot;
    use tokio_test::assert_ok;

    use super::*;

    fn runtime() -> Arc<Runtime> {
        let runtime = Builder::default()
            .worker_threads(2)
            .thread_name("test_spawn_join")
            .build();
        Arc::new(runtime.unwrap())
    }

    #[test]
    fn test_block_on_async() {
        let runtime = runtime();

        let out = runtime.block_on(async {
            let (tx, rx) = oneshot::channel();

            let _ = thread::spawn(move || {
                thread::sleep(Duration::from_millis(50));
                tx.send("ZOMG").unwrap();
            });

            assert_ok!(rx.await)
        });

        assert_eq!(out, "ZOMG");
    }

    #[test]
    fn test_spawn_from_blocking() {
        let runtime = runtime();
        let runtime1 = runtime.clone();
        let out = runtime.block_on(async move {
            let runtime2 = runtime1.clone();
            let inner = assert_ok!(
                runtime1
                    .spawn_blocking(move || { runtime2.spawn(async move { "hello" }) })
                    .await
            );

            assert_ok!(inner.await)
        });

        assert_eq!(out, "hello")
    }

    #[test]
    fn test_spawn_join() {
        let runtime = runtime();
        let handle = runtime.spawn(async { 1 + 1 });

        assert_eq!(2, runtime.block_on(handle).unwrap());
    }
}
