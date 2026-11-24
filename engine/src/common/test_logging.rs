use std::{
    io::{self, Write},
    sync::{Arc, Mutex},
};

use tracing::Dispatch;
use tracing_subscriber::{EnvFilter, fmt::MakeWriter};

#[derive(Clone, Default)]
struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

struct CaptureGuard(Arc<Mutex<Vec<u8>>>);

impl Write for CaptureGuard {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'writer> MakeWriter<'writer> for CaptureWriter {
    type Writer = CaptureGuard;

    fn make_writer(&'writer self) -> Self::Writer {
        CaptureGuard(Arc::clone(&self.0))
    }
}

pub(crate) fn capture(filter: &str, emit: impl FnOnce()) -> String {
    let writer = CaptureWriter::default();
    let subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_target(true)
        .with_env_filter(EnvFilter::try_new(filter).unwrap())
        .with_writer(writer.clone())
        .finish();
    let dispatch = Dispatch::new(subscriber);
    // Prevent another test thread from registering a new callsite through
    // tracing-core's single-dispatch fast path while this capture is active.
    let callsite_registration_guard = Dispatch::new(tracing_subscriber::registry());
    tracing::dispatcher::with_default(&dispatch, emit);
    drop(callsite_registration_guard);
    drop(dispatch);

    let bytes = writer
        .0
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    String::from_utf8(bytes).unwrap()
}

#[cfg(test)]
mod tests {
    use super::capture;

    fn emit_shared_callsite() {
        tracing::info!(
            target: "reefsdb::tsdb::test_logging",
            "captured test event"
        );
    }

    #[test]
    fn capture_includes_callsite_first_registered_on_another_thread() {
        let output = capture("reefsdb::tsdb::test_logging=info", || {
            std::thread::spawn(emit_shared_callsite).join().unwrap();
            emit_shared_callsite();
        });

        assert!(output.contains("captured test event"));
    }
}
