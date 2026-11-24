use super::*;
use crate::query::storage_ir::{AggregateKind, MeasurementRef};

fn request() -> AggregateRequest {
    AggregateRequest {
        measurement: MeasurementRef::new("cpu"),
        field: "value".into(),
        time_range: TimeRange::new(0, 8191),
        aggregates: vec![AggregateKind::Avg],
    }
}

#[test]
fn aggregate_prepare_does_not_wait_for_engine_write_guard() {
    let mut engine = super::super::tests::Engine::with_aggregate_index();
    engine
        .engine
        .measurement_fields(b"cpu")
        .create_field_if_not_exists(b"value", crate::models::INTEGER)
        .unwrap();
    engine.write_points_string(&["cpu value=10i 1"]).unwrap();
    let completed = std::thread::scope(|scope| {
        let guard = engine.engine.inner.write();
        let (send, receive) = std::sync::mpsc::channel();
        let database = &engine.engine;
        scope.spawn(move || {
            let result = database
                .prepare_aggregate_projected_scan(request())
                .and_then(|scan| scan.unwrap().run());
            send.send(result).unwrap();
        });
        let result = receive.recv_timeout(std::time::Duration::from_secs(1));
        // Always release before joining, including on failure, so the test cannot hang.
        drop(guard);
        match result {
            Ok(result) => {
                assert_eq!(result.unwrap().state.count, Some(1));
                true
            }
            Err(_) => {
                receive.recv().unwrap().unwrap();
                false
            }
        }
    });
    engine.close();
    assert!(
        completed,
        "aggregate query waited for an unrelated engine write guard"
    );
}

#[test]
fn saturated_parallel_admission_preserves_serial_extrema_reuse() {
    let mut options = crate::storage::AggregateQueryOptions::default();
    options.parallel_tasks = 3;
    options.parallel_min_series = 2;
    let mut engine = super::super::tests::Engine::with_aggregate_query_options(options);
    engine
        .engine
        .measurement_fields(b"cpu")
        .create_field_if_not_exists(b"value", crate::models::INTEGER)
        .unwrap();
    engine
        .write_points_string(&[
            "cpu,host=A value=200i 2",
            "cpu,host=B value=10i 0",
            "cpu,host=B value=20i 2",
            "cpu,host=B value=30i 6",
            "cpu,host=C value=11i 0",
            "cpu,host=C value=21i 2",
            "cpu,host=C value=31i 6",
            "cpu,host=D value=11i 0",
            "cpu,host=D value=21i 2",
            "cpu,host=D value=31i 6",
            "cpu,host=E value=11i 0",
            "cpu,host=E value=21i 2",
            "cpu,host=E value=31i 6",
        ])
        .unwrap();
    engine.must_write_snapshot();
    let mut request = request();
    request.time_range = TimeRange::new(1, 5);
    request.aggregates = vec![AggregateKind::Max];
    let scan = engine
        .engine
        .prepare_aggregate_windows_scan_filtered(
            vec![request],
            vec![TimeRange::new(1, 5)],
            StoragePredicate::Constant(true),
        )
        .unwrap();
    let context = Arc::clone(engine.engine.aggregate_index.as_ref().unwrap());
    let mut held = (0..3)
        .map(|_| QueryTaskPermit::try_acquire(&context.query_tasks, 3).unwrap())
        .collect::<Vec<_>>();
    let result = scan.clone().run().unwrap();
    let result = result[0].as_ref().unwrap();
    assert_eq!(
        result.states[0].max,
        Some(crate::query::storage_ir::AggregateNumber::Integer(200))
    );
    assert_eq!(
        result.metrics.decoded_blocks, 0,
        "saturated queries must keep cross-series bounds"
    );
    held.pop();
    let partial = scan.clone().run().unwrap();
    assert_eq!(partial[0].as_ref().unwrap().states, result.states);
    assert_eq!(
        context
            .query_tasks
            .load(std::sync::atomic::Ordering::Acquire),
        2
    );
    drop(held);
    let parallel = scan.run().unwrap();
    assert_eq!(parallel[0].as_ref().unwrap().states, result.states);
    assert_eq!(
        context
            .query_tasks
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );
    engine.close();
}

#[test]
#[ignore = "paired many-series window measurement; run optimized with --ignored --nocapture"]
fn aggregate_review_window_execution_costs() {
    let mut engine = super::super::tests::Engine::with_aggregate_index();
    engine
        .engine
        .measurement_fields(b"cpu")
        .create_field_if_not_exists(b"value", crate::models::INTEGER)
        .unwrap();
    let points = (0..64)
        .map(|i| format!("cpu,host=h{i:03} value=1i 1"))
        .collect::<Vec<_>>();
    engine
        .write_points_string(&points.iter().map(String::as_str).collect::<Vec<_>>())
        .unwrap();
    let windows = (0..512)
        .map(|w| TimeRange::new(w * 16, w * 16 + 15))
        .collect();
    let scan = engine
        .engine
        .prepare_aggregate_windows_scan_filtered(
            vec![request()],
            windows,
            StoragePredicate::Constant(true),
        )
        .unwrap();
    for _ in 0..10 {
        std::hint::black_box(scan.clone().run().unwrap());
    }
    let mut samples = Vec::new();
    for _ in 0..200 {
        let start = std::time::Instant::now();
        let result = scan.clone().run().unwrap();
        samples.push(start.elapsed().as_secs_f64() * 1000.0);
        let states = &result[0].as_ref().unwrap().states;
        assert_eq!(states.iter().map(|s| s.count.unwrap()).sum::<u64>(), 64);
    }
    samples.sort_by(f64::total_cmp);
    let p = |q: f64| samples[(samples.len() as f64 * q).ceil() as usize - 1];
    println!(
        "REVIEW_TIMING {}",
        serde_json::json!({"case":"many_series_windows",
        "series":64,"points":64,"windows":512,"range":[0,8191],"query_workers":1,
        "writers":0,"background_maintenance":false,"samples":samples.len(),
        "mean_ms":samples.iter().sum::<f64>()/samples.len() as f64,
        "p95_ms":p(0.95),"p99_ms":p(0.99),"p999_ms":p(0.999),
        "p9999_ms":p(0.9999),"max_ms":samples.last()})
    );
    engine.close();
}
