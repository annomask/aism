//! Opt-in measurements with independent fixtures, correctness checks, and bounded runs.

use super::*;
use crate::common::slow_trace::{QueryProfile, QueryProfileReport};
use crate::query::storage_ir::{AggregateKind, AggregateNumber, MeasurementRef};
use serde_json::{Value as Json, json};
use std::sync::{Barrier, atomic::AtomicUsize};
use std::time::{Duration, Instant};

fn setting(name: &str, default: usize) -> usize {
    std::env::var(name).ok().map(|v| v.parse().unwrap()).unwrap_or(default)
}

fn mode() -> String {
    std::env::var("REEFS_PERF_MODE").unwrap_or_else(|_| "index".into())
}

fn request(end: i64) -> AggregateRequest {
    AggregateRequest {
        measurement: MeasurementRef::new("cpu"),
        field: "value".into(),
        time_range: TimeRange::new(0, end),
        aggregates: vec![AggregateKind::Avg],
    }
}

fn latency(samples: &[f64]) -> Json {
    assert!(!samples.is_empty());
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let p = |q: f64| sorted[((sorted.len() as f64 * q).ceil() as usize - 1).min(sorted.len() - 1)];
    json!({"samples":sorted.len(), "mean_ms":sorted.iter().sum::<f64>() / sorted.len() as f64,
        "p95_ms":p(0.95), "p99_ms":p(0.99), "p999_ms":p(0.999),
        "p9999_ms":p(0.9999), "max_ms":sorted.last()})
}

fn check(result: &crate::query::storage_ir::ProjectedAggregateResult, count: u64, sum: f64) {
    assert_eq!(result.state.count, Some(count));
    let Some(AggregateNumber::Float(actual)) = result.state.sum else { panic!("missing float sum") };
    if sum.is_finite() {
        assert!((actual - sum).abs() <= sum.abs().max(1.0) * 1e-12,
            "incorrect sum: actual={actual} expected={sum}");
    } else {
        assert_eq!(actual.to_bits(), sum.to_bits());
    }
}

fn seed(engine: &tests::Engine, series: usize, points: usize, value: impl Fn(usize) -> f64) {
    engine.engine.measurement_fields(b"cpu")
        .create_field_if_not_exists(b"value", crate::models::FLOAT).unwrap();
    for start in (0..points).step_by(1024) {
        let mut lines = Vec::new();
        for s in 0..series {
            for t in start..(start + 1024).min(points) {
                lines.push(format!("cpu,host=h{s:03} value={:?} {t}", value(t)));
            }
        }
        engine.write_points_string(&lines.iter().map(String::as_str).collect::<Vec<_>>()).unwrap();
    }
    engine.must_write_snapshot();
}

#[test]
#[ignore = "controlled float fallback measurement; use an optimized build and --nocapture"]
fn aggregate_float_fallback_measurement() {
    let mode = mode();
    assert!(matches!(mode.as_str(), "index" | "index_raw" | "legacy"));
    if mode == "index_raw" {
        assert_eq!(std::env::var("REEFS_BENCH_SUMMARY_LEVEL").as_deref(), Ok("raw"));
    }
    let points = setting("REEFS_PERF_POINTS", 32768);
    let samples = setting("REEFS_PERF_SAMPLES", 1000);
    for shape in ["positive", "mixed_healthy", "cancellation", "outside_negative"] {
        let mut engine = tests::Engine::with_performance_mode(mode != "legacy");
        let value = |t: usize| match shape {
            "positive" => 2.0 + (t % 16) as f64 / 8.0,
            "mixed_healthy" => if t % 8 == 0 { -1.0 } else { 2.0 + (t % 16) as f64 / 8.0 },
            "cancellation" => [1e16, -1e16, 1.0][t % 3],
            "outside_negative" => if t < points / 2 { -1.0 } else { 2.0 },
            _ => unreachable!(),
        };
        seed(&engine, 1, points, value);
        let mut req = request(points as i64 - 1);
        if shape == "outside_negative" { req.time_range.start_time = (points * 3 / 4) as i64; }
        let expected = (req.time_range.start_time as usize..points).fold(0.0, |sum, t| sum + value(t));
        let count = points as u64 - req.time_range.start_time as u64;
        let query = || engine.engine.aggregate_projected_scan(req.clone()).unwrap().unwrap();
        for _ in 0..20 { check(&query(), count, expected); }
        let profile = QueryProfile::start().unwrap();
        let probe = query();
        check(&probe, count, expected);
        let profile = profile.finish();
        let mut times = Vec::with_capacity(samples);
        for _ in 0..samples {
            let start = Instant::now();
            let result = query();
            times.push(start.elapsed().as_secs_f64() * 1000.0);
            check(&result, count, expected);
        }
        println!("AGG_PERF {}", json!({"kind":"float", "mode":mode, "shape":shape,
            "points":points, "series":1, "query_range":[req.time_range.start_time,req.time_range.end_time],
            "query_workers":1, "write_workers":0, "snapshot":false, "compaction":false,
            "latency":latency(&times), "raw_samples_ms":times,
            "decoded_blocks":probe.metrics.decoded_blocks, "raw_scans":probe.metrics.raw_scans,
            "index_records":probe.metrics.index_records, "profile":profile}));
        engine.close();
    }
}

struct QuerySamples {
    times: Vec<f64>,
    profiles: Vec<(f64, QueryProfileReport)>,
    measured_queries: usize,
}

#[test]
#[ignore = "paired cache capture measurement for empty, narrow, and full ranges"]
fn aggregate_cache_range_measurement() {
    let points = 131072;
    let cache = Cache::new(0);
    cache.write(b"key", (0..points).map(|t| {
        Value::FloatValue(crate::models::points::FloatValue::new(t, t as f64))
    }).collect()).unwrap();
    for (case, min, max, count) in [
        ("outside", -100, -1, 0), ("narrow", 65536, 66559, 1024),
        ("full", 0, points - 1, points as usize),
    ] {
        let mut old_times = Vec::new();
        let mut range_times = Vec::new();
        for iteration in 0..1020 {
            for range_capture in if iteration % 2 == 0 { [true, false] } else { [false, true] } {
                let started = Instant::now();
                let values = if range_capture {
                    cache.values_in_range(b"key", min, max)
                } else {
                    crate::storage::tsm1::data_encoding::encoding_values::Values(
                        cache.values(b"key").0.into_iter().filter(|v| {
                            min <= v.unix_nano() && v.unix_nano() <= max
                        }).collect())
                };
                let elapsed = started.elapsed().as_secs_f64() * 1000.0;
                assert_eq!(values.len(), count);
                if iteration >= 20 {
                    if range_capture { range_times.push(elapsed); } else { old_times.push(elapsed); }
                }
            }
        }
        println!("CACHE_PERF {}", json!({"case":case, "points":points, "selected_points":count,
            "full_capture":latency(&old_times), "range_capture":latency(&range_times)}));
    }
    for (case, min, max) in [("unordered_narrow", 16000, 17023), ("unordered_full", 0, 32767)] {
        let old_cache = Cache::new(0);
        let new_cache = Cache::new(0);
        let points = (0..32768).map(|i| {
            let time = i * 7919 % 32768;
            Value::FloatValue(crate::models::points::FloatValue::new(time, time as f64))
        }).collect::<Vec<_>>();
        old_cache.write(b"key", points.clone()).unwrap();
        new_cache.write(b"key", points).unwrap();
        let mut old_times = Vec::new();
        let mut range_times = Vec::new();
        let mut first = [0.0; 2];
        for iteration in 0..1020 {
            for range_capture in if iteration % 2 == 0 { [true, false] } else { [false, true] } {
                let started = Instant::now();
                let values = if range_capture {
                    new_cache.values_in_range(b"key", min, max)
                } else {
                    crate::storage::tsm1::data_encoding::encoding_values::Values(
                        old_cache.values(b"key").0.into_iter().filter(|v| {
                            min <= v.unix_nano() && v.unix_nano() <= max
                        }).collect())
                };
                let elapsed = started.elapsed().as_secs_f64() * 1000.0;
                assert_eq!(values.len() as i64, max - min + 1);
                if iteration == 0 { first[usize::from(range_capture)] = elapsed; }
                if iteration >= 20 {
                    if range_capture { range_times.push(elapsed); } else { old_times.push(elapsed); }
                }
            }
        }
        println!("CACHE_PERF {}", json!({"case":case, "points":32768, "selected_points":max-min+1,
            "first_full_capture_ms":first[0], "first_range_capture_ms":first[1],
            "full_capture":latency(&old_times), "range_capture":latency(&range_times)}));
    }
}

#[test]
#[ignore = "fresh unordered caches with alternating first-call order and stage profiles"]
fn aggregate_cache_first_capture_measurement() {
    let points = (0..32768).map(|i| {
        let time = i * 7919 % 32768;
        Value::FloatValue(crate::models::points::FloatValue::new(time, time as f64))
    }).collect::<Vec<_>>();
    let mut records = Vec::new();
    for iteration in 0..40 {
        let old_cache = Cache::new(0);
        let new_cache = Cache::new(0);
        old_cache.write(b"key", points.clone()).unwrap();
        new_cache.write(b"key", points.clone()).unwrap();
        for range_capture in if iteration % 2 == 0 { [false, true] } else { [true, false] } {
            let profile = QueryProfile::start().unwrap();
            let started = Instant::now();
            let values = if range_capture {
                new_cache.values_in_range(b"key", 16000, 17023)
            } else {
                crate::storage::tsm1::data_encoding::encoding_values::Values(
                    old_cache.values(b"key").0.into_iter().filter(|v| {
                        (16000..=17023).contains(&v.unix_nano())
                    }).collect())
            };
            let elapsed = started.elapsed().as_secs_f64() * 1000.0;
            let profile = profile.finish();
            assert_eq!(values.len(), 1024);
            assert!(values.iter().enumerate().all(|(i, value)| value.unix_nano() == 16000 + i as i64));
            records.push(json!({"iteration":iteration, "range_capture":range_capture,
                "first":range_capture == (iteration % 2 != 0), "elapsed_ms":elapsed,
                "profile":profile}));
        }
    }
    println!("CACHE_COLD_PERF {}", json!({"case":"unordered_narrow_first_capture",
        "points":32768, "selected_points":1024, "pairs":40,
        "profiled":true, "records":records}));
}

#[test]
#[ignore = "one writer and five query workers with controlled maintenance; run optimized"]
fn aggregate_concurrent_tail_measurement() {
    let mode = mode();
    assert!(matches!(mode.as_str(), "index" | "legacy"));
    let scenario = std::env::var("REEFS_PERF_SCENARIO").unwrap_or_else(|_| "snapshot".into());
    assert!(matches!(scenario.as_str(), "idle" | "write" | "snapshot" | "compact"));
    assert!(scenario != "compact" || mode == "index", "explicit compaction fixture requires index mode");
    let seconds = setting("REEFS_PERF_SECONDS", 8).clamp(1, 60);
    let series = 8;
    let points = 2048;
    let query_workers = 5;
    let mut engine = tests::Engine::with_performance_mode(mode != "legacy");
    seed(&engine, series, points, |_| 1.0);
    let database = &engine.engine;
    let req = request(points as i64 - 1);
    let count = (series * points) as u64;
    for _ in 0..10 {
        check(&database.aggregate_projected_scan(req.clone()).unwrap().unwrap(), count, count as f64);
    }
    let barrier = Barrier::new(query_workers + 3);
    let written = AtomicUsize::new(0);
    let run_started = Instant::now();
    let (readers, writes, maintenance) = std::thread::scope(|scope| {
        let mut readers = Vec::new();
        for _ in 0..query_workers {
            let barrier = &barrier;
            let req = &req;
            readers.push(scope.spawn(move || {
                barrier.wait();
                let start = Instant::now();
                let mut result = QuerySamples { times: Vec::new(), profiles: Vec::new(), measured_queries: 0 };
                while start.elapsed() < Duration::from_secs(seconds as u64) {
                    // Exclude profiled queries from latency statistics. Keep the eight slowest
                    // diagnostic samples per worker; no global profiling lock is introduced.
                    let profile = (result.measured_queries % 64 == 0).then(|| QueryProfile::start().unwrap());
                    let query_start = Instant::now();
                    let answer = database.aggregate_projected_scan(req.clone()).unwrap().unwrap();
                    let elapsed = query_start.elapsed().as_secs_f64() * 1000.0;
                    check(&answer, count, count as f64);
                    if let Some(profile) = profile {
                        result.profiles.push((elapsed, profile.finish()));
                        result.profiles.sort_by(|a, b| b.0.total_cmp(&a.0));
                        result.profiles.truncate(8);
                    } else {
                        result.times.push(elapsed);
                    }
                    result.measured_queries += 1;
                }
                result
            }));
        }
        let write = scope.spawn(|| {
            barrier.wait();
            let start = Instant::now();
            let mut times = Vec::new();
            let mut profiles = Vec::new();
            let mut next = points;
            while scenario != "idle" && start.elapsed() < Duration::from_secs(seconds as u64) {
                let mut lines = Vec::new();
                for s in 0..series {
                    for t in next..next + 32 { lines.push(format!("cpu,host=h{s:03} value=1.0 {t}")); }
                }
                let (mut batch, _) = crate::models::points::parse_points_string(&lines.join("\n")).unwrap();
                let profile = (times.len() % 64 == 0).then(|| QueryProfile::start().unwrap());
                let write_start = Instant::now();
                database.write_points(&mut batch).unwrap();
                times.push(write_start.elapsed().as_secs_f64() * 1000.0);
                if let Some(profile) = profile { profiles.push(profile.finish()); }
                written.fetch_add(batch.len(), Ordering::Relaxed);
                next += 32;
                std::thread::sleep(Duration::from_millis(5));
            }
            (times, profiles)
        });
        let maintenance = scope.spawn(|| {
            barrier.wait();
            let start = Instant::now();
            let mut events = Vec::new();
            while matches!(scenario.as_str(), "snapshot" | "compact")
                && start.elapsed() < Duration::from_secs(seconds as u64) {
                std::thread::sleep(Duration::from_millis(200));
                let before = run_started.elapsed().as_secs_f64() * 1000.0;
                Engine::write_snapshot(true, Arc::clone(&database.inner)).unwrap();
                events.push(json!({"stage":"snapshot", "start_ms":before,
                    "end_ms":run_started.elapsed().as_secs_f64()*1000.0}));
                if scenario == "compact" && events.len() % 4 == 0 {
                    let before = run_started.elapsed().as_secs_f64() * 1000.0;
                    database.compact_aggregate_index().unwrap();
                    events.push(json!({"stage":"compaction", "start_ms":before,
                        "end_ms":run_started.elapsed().as_secs_f64()*1000.0}));
                }
            }
            events
        });
        barrier.wait();
        let readers = readers.into_iter().map(|r| r.join().unwrap()).collect::<Vec<_>>();
        (readers, write.join().unwrap(), maintenance.join().unwrap())
    });
    let mut times = Vec::new();
    let mut profiles = Vec::new();
    let mut query_count = 0;
    for reader in readers {
        times.extend(reader.times);
        query_count += reader.measured_queries;
        profiles.extend(reader.profiles);
    }
    let write_latency = if writes.0.is_empty() { Json::Null } else { latency(&writes.0) };
    println!("AGG_PERF {}", json!({"kind":"concurrent", "mode":mode, "scenario":scenario,
        "seconds":seconds, "query_workers":query_workers, "write_workers":usize::from(scenario != "idle"),
        "initial_points":count, "series":series, "query_range":[0,points-1],
        "write_batch_points":series*32, "write_pause_ms":5, "appended_points":written.load(Ordering::Relaxed),
        "query_count":query_count, "latency":latency(&times), "write_latency":write_latency,
        "raw_samples_ms":times, "slow_sampled_profiles":profiles, "write_profiles":writes.1,
        "maintenance_events":maintenance}));
    check(&database.aggregate_projected_scan(req).unwrap().unwrap(), count, count as f64);
    engine.close();
}
