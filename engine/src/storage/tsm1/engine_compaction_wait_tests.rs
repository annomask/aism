#![cfg(feature = "test-support")]

use std::{
    sync::{Arc, Barrier},
    time::{Duration, Instant},
};

use crate::storage::tsm1::engine::wait_for_compaction_tick_for_test;
use tokio_util::sync::CancellationToken;

#[test]
fn compaction_worker_waits_for_tick_and_observes_cancellation() {
    let active = CancellationToken::new();
    let (_active_cancel, active_cancelled) = crossbeam_channel::unbounded();
    let (sender, tick) = crossbeam_channel::bounded(1);
    let worker = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(10));
        sender.send(()).unwrap();
    });
    assert!(wait_for_compaction_tick_for_test(
        &tick,
        &active,
        &active_cancelled
    ));
    worker.join().unwrap();

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let (_cancel, cancellation) = crossbeam_channel::unbounded();
    let no_tick = crossbeam_channel::never::<()>();
    assert!(!wait_for_compaction_tick_for_test(
        &no_tick,
        &cancelled,
        &cancellation
    ));
}

#[test]
fn compaction_cancellation_wakes_worker_without_polling_delay() {
    let quit = CancellationToken::new();
    let worker_quit = quit.clone();
    let (cancel, cancellation) = crossbeam_channel::unbounded();
    let no_tick = crossbeam_channel::never::<()>();
    let started = Arc::new(Barrier::new(2));
    let worker_started = Arc::clone(&started);

    let worker = std::thread::spawn(move || {
        worker_started.wait();
        wait_for_compaction_tick_for_test(&no_tick, &worker_quit, &cancellation)
    });

    started.wait();
    std::thread::sleep(Duration::from_millis(10));
    let cancel_started = Instant::now();
    quit.cancel();
    drop(cancel);

    assert!(!worker.join().unwrap());
    assert!(
        cancel_started.elapsed() < Duration::from_millis(75),
        "event-driven cancellation took {:?}",
        cancel_started.elapsed()
    );
}
