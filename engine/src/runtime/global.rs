//! Global runtimes.
use std::{future::Future, sync::Once};

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use paste::paste;
use tokio::task::JoinHandle;

use super::runtime::{AsyncRuntime, Builder, Runtime, RuntimeBuilder};

// const GLOBAL_WORKERS: usize = 8;
const GLOBAL_BLOCKING_WORKERS: usize = 512;
// const COMPACT_WORKERS: usize = 4;
const COMPACT_BLOCKING_WORKERS: usize = 512;

/// The options for the global runtimes.
#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeOptions {
    /// The number of threads to execute the runtime for global operations.
    pub global_rt_size: usize,
    /// The number of threads to execute the runtime for global blocking operations.
    pub global_blocking_rt_size: usize,
    /// The number of threads to execute the runtime for compact operations.
    pub compact_rt_size: usize,
    /// The number of threads to execute the runtime for compact blocking operations.
    pub compact_blocking_rt_size: usize,
}

impl RuntimeOptions {
    pub fn new(
        mut global_rt_size: usize,
        mut global_blocking_rt_size: usize,
        mut compact_rt_size: usize,
        mut compact_blocking_rt_size: usize,
    ) -> Self {
        let cpus = num_cpus::get();
        if global_rt_size == 0 {
            global_rt_size = cpus;
        }
        if global_blocking_rt_size == 0 {
            global_blocking_rt_size = GLOBAL_BLOCKING_WORKERS;
        }
        if compact_rt_size == 0 {
            compact_rt_size = usize::max(cpus / 2, 1);
        }
        if compact_blocking_rt_size == 0 {
            compact_blocking_rt_size = COMPACT_BLOCKING_WORKERS;
        }

        Self {
            global_rt_size,
            global_blocking_rt_size,
            compact_rt_size,
            compact_blocking_rt_size,
        }
    }
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        let cpus = num_cpus::get();
        Self {
            global_rt_size: cpus,
            global_blocking_rt_size: GLOBAL_BLOCKING_WORKERS,
            compact_rt_size: usize::max(cpus / 2, 1),
            compact_blocking_rt_size: COMPACT_BLOCKING_WORKERS,
        }
    }
}

fn create_runtime(
    runtime_name: &str,
    thread_name: &str,
    worker_threads: usize,
    blocking_threads: usize,
) -> Runtime {
    Builder::default()
        .runtime_name(runtime_name)
        .thread_name(thread_name)
        .worker_threads(worker_threads)
        .max_blocking_threads(blocking_threads)
        .build()
        .expect("Fail to create runtime")
}

struct GlobalRuntimes {
    global_runtime: Runtime,
    compact_runtime: Runtime,
}

macro_rules! define_spawn {
    ($type: ident) => {
        paste! {

            fn [<spawn_ $type>]<F>(&self, future: F) -> JoinHandle<F::Output>
            where
                F: Future + Send + 'static,
                F::Output: Send + 'static,
            {
                self.[<$type _runtime>].spawn(future)
            }

            fn [<spawn_blocking_ $type>]<F, R>(&self, future: F) ->  JoinHandle<R>
            where
                F: FnOnce() -> R + Send + 'static,
                R: Send + 'static,
            {
                self.[<$type _runtime>].spawn_blocking(future)
            }

            fn [<block_on_ $type>]<F: Future>(&self, future: F) -> F::Output {
                self.[<$type _runtime>].block_on(future)
            }
        }
    };
}

impl GlobalRuntimes {
    define_spawn!(global);

    define_spawn!(compact);

    fn new(global_runtime: Option<Runtime>, compact_runtime: Option<Runtime>) -> Self {
        Self {
            global_runtime: global_runtime.unwrap_or_else(|| {
                let cpus = num_cpus::get();
                create_runtime(
                    "global_runtime",
                    "global_thread_worker",
                    cpus,
                    GLOBAL_BLOCKING_WORKERS,
                )
            }),
            compact_runtime: compact_runtime.unwrap_or_else(|| {
                let cpus = num_cpus::get();
                create_runtime(
                    "compact_runtime",
                    "compact_thread_worker",
                    usize::max(cpus / 2, 1),
                    COMPACT_BLOCKING_WORKERS,
                )
            }),
        }
    }
}

#[derive(Default)]
struct ConfigRuntimes {
    global_runtime: Option<Runtime>,
    compact_runtime: Option<Runtime>,
    already_init: bool,
}

static GLOBAL_RUNTIMES: Lazy<GlobalRuntimes> = Lazy::new(|| {
    let mut c = CONFIG_RUNTIMES.lock();
    let global = c.global_runtime.take();
    let compact = c.compact_runtime.take();
    c.already_init = true;

    GlobalRuntimes::new(global, compact)
});

static CONFIG_RUNTIMES: Lazy<Mutex<ConfigRuntimes>> =
    Lazy::new(|| Mutex::new(ConfigRuntimes::default()));

/// Initialize the global runtimes
///
/// # Panics
/// Panics when the global runtimes are already initialized.
/// You should call this function before using any runtime functions.
pub fn init_global_runtimes(options: &RuntimeOptions) {
    static START: Once = Once::new();
    START.call_once(move || {
        let mut c = CONFIG_RUNTIMES.lock();
        assert!(!c.already_init, "Global runtimes already initialized");
        c.global_runtime = Some(create_runtime(
            "global_runtime",
            "global_thread_worker",
            options.global_rt_size,
            options.global_blocking_rt_size,
        ));
        c.compact_runtime = Some(create_runtime(
            "compact_runtime",
            "compact_thread_worker",
            options.compact_rt_size,
            options.compact_blocking_rt_size,
        ));
    });
}

macro_rules! define_global_runtime_spawn {
    ($type: ident) => {
        paste! {
            #[doc = "Returns the global `" $type "` thread pool."]
            pub fn [<$type _runtime>]() -> Runtime {
                GLOBAL_RUNTIMES.[<$type _runtime>].clone()
            }

            #[doc = "Spawn a future and execute it in `" $type "` thread pool."]
            pub fn [<spawn_ $type>]<F>(future: F) -> JoinHandle<F::Output>
            where
                F: Future + Send + 'static,
                F::Output: Send + 'static,
            {
                GLOBAL_RUNTIMES.[<spawn_ $type>](future)
            }

            #[doc = "Run the blocking operation in `" $type "` thread pool."]
            pub fn [<spawn_blocking_ $type>]<F, R>(future: F) ->  JoinHandle<R>
            where
                F: FnOnce() -> R + Send + 'static,
                R: Send + 'static,
            {
                GLOBAL_RUNTIMES.[<spawn_blocking_ $type>](future)
            }

            #[doc = "Run a future to complete in `" $type "` thread pool."]
            pub fn [<block_on_ $type>]<F: Future>(future: F) -> F::Output {
                GLOBAL_RUNTIMES.[<block_on_ $type>](future)
            }
        }
    };
}

define_global_runtime_spawn!(global);
define_global_runtime_spawn!(compact);

#[cfg(test)]
mod tests {
    use tokio_test::assert_ok;

    use super::*;

    #[test]
    fn test_spawn_block_on() {
        let handle = spawn_global(async { 1 + 1 });
        assert_eq!(2, block_on_global(handle).unwrap());

        let handle = spawn_compact(async { 2 + 2 });
        assert_eq!(4, block_on_compact(handle).unwrap());
    }

    macro_rules! define_spawn_blocking_test {
        ($type: ident) => {
            paste! {
                #[test]
                fn [<test_spawn_ $type _from_blocking>]() {
                    let runtime = [<$type _runtime>]();
                    let out = runtime.block_on(async move {
                        let inner = assert_ok!(
                            [<spawn_blocking_  $type>](move || {
                                [<spawn_ $type>](async move { "hello" })
                            }).await
                        );

                        assert_ok!(inner.await)
                    });

                    assert_eq!(out, "hello")
                }
            }
        };
    }

    define_spawn_blocking_test!(global);
    define_spawn_blocking_test!(compact);
}
