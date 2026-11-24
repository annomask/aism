//! Reproducible opt-in workload; not a substitute for dedicated SSD experiments.

use super::aggregate_index_tests::{aggregate, cursor};
use super::*;
use crate::query::storage_ir::{AggregateState, AggregateValues, TimeRange};
use std::time::Instant;

fn raw(e: &tests::Engine) -> AggregateState {
    let mut state = AggregateState::empty(crate::models::INTEGER);
    for (time, value) in cursor(e, "i", true) {
        let Val::I64(value) = value else {
            panic!("unexpected benchmark type")
        };
        state.add_integer_at(time, value);
    }
    state
}

fn write_existing_series(e: &tests::Engine, rows: &[String]) {
    let (mut points, _) = crate::models::points::parse_points_string(&rows.join("\n")).unwrap();
    let point = &mut points[0];
    let tags = point.tags();
    e.engine
        .create_series_if_not_exists(point.key(), &point.name(), tags)
        .unwrap();
    e.engine.write_points(&mut points).unwrap();
}

fn option(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .map(|s| s.parse().expect("invalid benchmark setting"))
        .unwrap_or(default)
}

fn report(
    label: &str,
    samples: &mut [f64],
    points: usize,
    workers: usize,
    concurrent: bool,
    compactions: usize,
    metrics: AggregateScanMetrics,
) {
    samples.sort_by(f64::total_cmp);
    let percentile = |p: f64| {
        samples[((samples.len() as f64 * p).ceil() as usize)
            .saturating_sub(1)
            .min(samples.len() - 1)]
    };
    println!(
        "AGG_BENCH {}",
        serde_json::json!({
            "case": label, "points": points, "series": 1, "fields": 1,
            "query_workers": workers, "write_workers": usize::from(concurrent),
            "time_range": "all", "snapshots_running": concurrent,
            "completed_compactions": compactions, "samples": samples.len(),
            "average_ms": samples.iter().sum::<f64>() / samples.len() as f64,
            "p95_ms": percentile(0.95), "p99_ms": percentile(0.99),
            "p999_ms": percentile(0.999), "p9999_ms": percentile(0.9999),
            "max_ms": samples[samples.len() - 1],
        "post_run_index_probe": {
            "summary_records": metrics.index_records, "summary_level": metrics.index_levels,
            "decoded_blocks": metrics.decoded_blocks,
        },
        })
    );
}

#[test]
#[ignore = "research workload; run explicitly with --ignored --nocapture --test-threads=1"]
fn aggregate_index_research_latency_matrix() {
    let points = option("REEFSDB_AGG_BENCH_POINTS", 70000);
    let iterations = option("REEFSDB_AGG_BENCH_ITERATIONS", 30);
    let workers = option("REEFSDB_AGG_BENCH_WORKERS", 5);
    assert!(points >= 70 && iterations > 0 && workers > 0);
    for use_index in [false, true] {
        let mut e = tests::Engine::with_aggregate_index();
        e.engine
            .measurement_fields(b"cpu")
            .create_field_if_not_exists(b"i", crate::models::INTEGER)
            .unwrap();
        e.write_points_string(&["cpu,host=A i=0i 1"]).unwrap();
        let chunk = points.div_ceil(70);
        for start in (0..points).step_by(chunk) {
            let rows = (start..(start + chunk).min(points))
                .map(|i| {
                    format!(
                        "cpu,host=A i={}i {}",
                        ((i * 7919) % 10007) as i64 - 5000,
                        i + 1
                    )
                })
                .collect::<Vec<_>>();
            write_existing_series(&e, &rows);
            e.must_write_snapshot();
        }
        e.engine.schedule_full_compaction().unwrap();
        e.engine.set_compactions_enabled(false);
        assert_eq!(aggregate(&e, "i", TimeRange::all()).state, raw(&e));
        let query = || {
            if use_index {
                aggregate(&e, "i", TimeRange::all()).state
            } else {
                raw(&e)
            }
        };
        for _ in 0..5 {
            std::hint::black_box(query());
        }
        for concurrent in [false, true] {
            let writer_done = std::sync::atomic::AtomicBool::new(!concurrent);
            let gate = std::sync::Barrier::new(workers + usize::from(concurrent));
            let (mut samples, compactions) = std::thread::scope(|scope| {
                let writer = concurrent.then(|| {
                    scope.spawn(|| {
                        let _complete =
                            scopeguard::guard((), |_| writer_done.store(true, Ordering::Release));
                        gate.wait();
                        let mut compactions = 0;
                        for batch in 0..12 {
                            let rows = (0..64)
                                .map(|i| {
                                    format!(
                                        "cpu,host=A i={}i {}",
                                        100000 + batch * 64 + i,
                                        points / 2 + batch * 64 + i + 1
                                    )
                                })
                                .collect::<Vec<_>>();
                            write_existing_series(&e, &rows);
                            Engine::write_snapshot(
                                e.engine.wal_enabled,
                                Arc::clone(&e.engine.inner),
                            )
                            .unwrap();
                            let store = &e.engine.aggregate_index.as_ref().unwrap().store;
                            let selected = vlog::index::planner::select_inputs(store).unwrap();
                            if !selected.is_empty() {
                                let files = {
                                    let inner = e.engine.inner.read();
                                    Arc::clone(
                                        &inner.vlog_inner.as_ref().unwrap().read().vlog_file_store,
                                    )
                                };
                                store
                                    .compact_files(&selected, files.next_generation(), &|| false)
                                    .unwrap();
                                compactions += 1;
                            }
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        compactions
                    })
                });
                let queries = (0..workers)
                    .map(|_| {
                        scope.spawn(|| {
                            gate.wait();
                            let mut samples = Vec::new();
                            while samples.len() < iterations || !writer_done.load(Ordering::Acquire)
                            {
                                let start = Instant::now();
                                std::hint::black_box(query());
                                samples.push(start.elapsed().as_secs_f64() * 1000.0);
                            }
                            samples
                        })
                    })
                    .collect::<Vec<_>>();
                let samples = queries
                    .into_iter()
                    .flat_map(|h| h.join().unwrap())
                    .collect::<Vec<_>>();
                (samples, writer.map(|h| h.join().unwrap()).unwrap_or(0))
            });
            let result = aggregate(&e, "i", TimeRange::all());
            assert_eq!(result.state, raw(&e));
            let label = match (use_index, concurrent) {
                (false, false) => "raw_quiet",
                (false, true) => "raw_write_flush_compaction",
                (true, false) => "index_quiet",
                (true, true) => "index_write_flush_compaction",
            };
            report(
                label,
                &mut samples,
                points,
                workers,
                concurrent,
                compactions,
                result.metrics,
            );
        }
        e.close();
    }
}

fn latency_distribution(samples: &mut [f64]) -> serde_json::Value {
    samples.sort_by(f64::total_cmp);
    if samples.is_empty() {
        return serde_json::Value::Null;
    }
    let percentile = |p: f64| {
        samples[((samples.len() as f64 * p).ceil() as usize)
            .saturating_sub(1)
            .min(samples.len() - 1)]
    };
    serde_json::json!({"samples": samples.len(), "average_ms": samples.iter().sum::<f64>() / samples.len() as f64,
        "p95_ms": percentile(0.95), "p99_ms": percentile(0.99), "p999_ms": percentile(0.999),
        "p9999_ms": percentile(0.9999), "max_ms": samples[samples.len() - 1]})
}

#[test]
#[ignore = "research column IO workload; run explicitly with --ignored --nocapture --test-threads=1"]
fn aggregate_index_research_column_io() {
    use crate::query::storage_ir::{AggregateProjection, ProjectedAggregateState};
    let points = option("REEFSDB_AGG_BENCH_POINTS", 70000);
    let iterations = option("REEFSDB_AGG_BENCH_ITERATIONS", 30);
    let workers = option("REEFSDB_AGG_BENCH_WORKERS", 5);
    assert!(points >= 70 && iterations > 0 && workers > 0);
    let mut e = tests::Engine::with_aggregate_index();
    e.engine
        .measurement_fields(b"cpu")
        .create_field_if_not_exists(b"i", crate::models::INTEGER)
        .unwrap();
    e.write_points_string(&["cpu,host=A i=0i 1"]).unwrap();
    let chunk = points.div_ceil(70);
    for start in (0..points).step_by(chunk) {
        let rows = (start..(start + chunk).min(points))
            .map(|i| {
                format!(
                    "cpu,host=A i={}i {}",
                    ((i * 7919) % 10007) as i64 - 5000,
                    i + 1
                )
            })
            .collect::<Vec<_>>();
        write_existing_series(&e, &rows);
        e.must_write_snapshot();
    }
    e.engine.schedule_full_compaction().unwrap();
    e.engine.set_compactions_enabled(false);
    let expected = raw(&e);
    for kind in [
        AggregateKind::Count,
        AggregateKind::Sum,
        AggregateKind::Avg,
        AggregateKind::Min,
        AggregateKind::Max,
        AggregateKind::First,
        AggregateKind::Last,
    ] {
        let request = AggregateRequest {
            measurement: MeasurementRef::new("cpu"),
            field: "i".into(),
            time_range: TimeRange::all(),
            aggregates: vec![kind],
        };
        let projection = AggregateProjection::from_aggregates(&request.aggregates);
        let result = e
            .engine
            .aggregate_projected_scan(request.clone())
            .unwrap()
            .unwrap();
        assert_eq!(
            result.state,
            ProjectedAggregateState::from_complete(expected.clone(), projection)
        );
        assert_eq!(result.metrics.decoded_blocks, 0);
        assert!(result.index_reads.requested_bytes < result.index_reads.full_group_bytes);
        let context = e.engine.aggregate_index.as_ref().unwrap();
        let complete = context
            .store
            .aggregate_projected(
                context.sequence(b"cpu,host=A#!~#i").unwrap(),
                crate::models::INTEGER,
                TimeRange::all(),
                vec![],
                AggregateProjection::complete(
                    crate::models::INTEGER,
                    matches!(kind, AggregateKind::First | AggregateKind::Last),
                ),
            )
            .unwrap();
        let mut latency = Vec::new();
        for projected in [false, true] {
            let query = || {
                if projected {
                    std::hint::black_box(
                        e.engine.aggregate_projected_scan(request.clone()).unwrap(),
                    );
                } else {
                    std::hint::black_box(e.engine.aggregate_scan(request.clone()).unwrap());
                }
            };
            for _ in 0..5 {
                query();
            }
            let barrier = std::sync::Barrier::new(workers);
            let mut samples = std::thread::scope(|scope| {
                let handles = (0..workers)
                    .map(|_| {
                        scope.spawn(|| {
                            barrier.wait();
                            (0..iterations)
                                .map(|_| {
                                    let started = Instant::now();
                                    query();
                                    started.elapsed().as_secs_f64() * 1000.0
                                })
                                .collect::<Vec<_>>()
                        })
                    })
                    .collect::<Vec<_>>();
                handles
                    .into_iter()
                    .flat_map(|h| h.join().unwrap())
                    .collect::<Vec<_>>()
            });
            latency.push(latency_distribution(&mut samples));
        }
        println!(
            "AGG_COLUMN_IO {}",
            serde_json::json!({
                "aggregate": format!("{kind:?}"), "points": points, "series": 1, "fields": 1,
                "query_workers": workers, "write_workers": 0, "snapshots_running": false, "compactions_running": false,
                "time_range": "all", "format_version": vlog::index::directory::FORMAT_VERSION,
                "requested_payload_bytes": result.index_reads.requested_bytes,
                "complete_api_payload_bytes": complete.index_reads.requested_bytes,
                "full_payload_bytes": result.index_reads.full_group_bytes,
                "payload_read_requests": result.index_reads.read_requests,
                "decoded_columns": result.index_reads.decoded_columns,
                "decoded_vlog_blocks": result.metrics.decoded_blocks,
                "complete_api_latency": latency[0], "projected_api_latency": latency[1],
                "byte_scope": "logical index group payload including cache hits; excludes metadata, VLog and device IO"
            })
        );
    }
    e.close();
}

/// Counts committed data/index file lengths once per immutable filename. This
/// excludes WAL, TSI, manifests, temporary spools and device-level write traffic.
fn observe_outputs(path: &Path, seen: &mut HashSet<PathBuf>) -> (u64, u64) {
    let mut bytes = (0, 0);
    for entry in std::fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let target = match path.extension().and_then(|s| s.to_str()) {
            Some("idx") => &mut bytes.0,
            Some("vlog") => &mut bytes.1,
            _ => continue,
        };
        if seen.insert(path) {
            *target += entry.metadata().unwrap().len();
        }
    }
    bytes
}

#[test]
#[ignore = "research churn workload; run explicitly with --ignored --nocapture --test-threads=1"]
fn aggregate_index_research_compaction_churn() {
    let series = option("REEFSDB_AGG_CHURN_SERIES", 256);
    let rounds = option("REEFSDB_AGG_CHURN_ROUNDS", 24);
    let points = option("REEFSDB_AGG_CHURN_POINTS", 8);
    assert!(series > 0 && rounds >= 4 && points > 0);
    for mixed in [false, true] {
        let mut e = tests::Engine::with_aggregate_index();
        for field in [b"i", b"j"] {
            e.engine
                .measurement_fields(b"cpu")
                .create_field_if_not_exists(field, crate::models::INTEGER)
                .unwrap();
        }
        let files = {
            let inner = e.engine.inner.read();
            Arc::clone(&inner.vlog_inner.as_ref().unwrap().read().vlog_file_store)
        };
        let context = Arc::clone(e.engine.aggregate_index.as_ref().unwrap());
        let store = &context.store;
        let mut seen = HashSet::new();
        let mut output_bytes = (0_u64, 0_u64);
        let mut flush_bytes = (0_u64, 0_u64);
        let mut write_ms = Vec::new();
        let mut snapshot_ms = Vec::new();
        let mut compaction_ms = Vec::new();
        let mut delete_ms = Vec::new();
        let mut metrics = vlog::index::compact::MergeStats::default();
        let mut reference = std::collections::BTreeMap::new();
        for round in 0..rounds {
            let start = if mixed && round % 4 == 3 {
                round - 1
            } else {
                round
            } * points;
            for offset in 0..points {
                reference.insert(
                    (start + offset + 1) as i64,
                    (round * points + offset) as i64,
                );
            }
            let started = Instant::now();
            // Fixed-size ingestion batches keep the experiment driver independent
            // of total series cardinality. Series identities remain engine-owned.
            let mut batch = Vec::new();
            for host in 0..series {
                for offset in 0..points {
                    let value = round * points + offset;
                    batch.push(format!(
                        "cpu,host=S{host} i={value}i,j={}i {}",
                        value + 1,
                        start + offset + 1
                    ));
                    if batch.len() == 1024 {
                        let (mut parsed, _) =
                            crate::models::points::parse_points_string(&batch.join("\n")).unwrap();
                        e.engine.write_points(&mut parsed).unwrap();
                        batch.clear();
                    }
                }
            }
            if !batch.is_empty() {
                let (mut parsed, _) =
                    crate::models::points::parse_points_string(&batch.join("\n")).unwrap();
                e.engine.write_points(&mut parsed).unwrap();
            }
            write_ms.push(started.elapsed().as_secs_f64() * 1000.0);
            let started = Instant::now();
            e.must_write_snapshot();
            snapshot_ms.push(started.elapsed().as_secs_f64() * 1000.0);
            let bytes = observe_outputs(files.path(), &mut seen);
            flush_bytes.0 += bytes.0;
            flush_bytes.1 += bytes.1;
            output_bytes.0 += bytes.0;
            output_bytes.1 += bytes.1;
            if mixed && round % 6 == 5 {
                let cutoff = ((round - 3) * points) as i64;
                let started = Instant::now();
                e.engine
                    .delete_series_range_by_measurement(
                        b"cpu",
                        Some(|_: &[u8], _: &Tags| (i64::MIN, cutoff, true)),
                    )
                    .unwrap();
                delete_ms.push(started.elapsed().as_secs_f64() * 1000.0);
                reference.retain(|time, _| *time > cutoff);
                let bytes = observe_outputs(files.path(), &mut seen);
                output_bytes.0 += bytes.0;
                output_bytes.1 += bytes.1;
            }
            loop {
                let inputs = vlog::index::planner::select_inputs(store).unwrap();
                if inputs.is_empty() {
                    break;
                }
                let started = Instant::now();
                let stats = store
                    .compact_files(&inputs, files.next_generation(), &|| false)
                    .unwrap();
                compaction_ms.push(started.elapsed().as_secs_f64() * 1000.0);
                metrics.copied_groups += stats.copied_groups;
                metrics.rewritten_groups += stats.rewritten_groups;
                metrics.coalesced_groups += stats.coalesced_groups;
                metrics.repaired_blocks += stats.repaired_blocks;
                metrics.streamed_rebuild_fields += stats.streamed_rebuild_fields;
                metrics.boundary_children_read += stats.boundary_children_read;
                let bytes = observe_outputs(files.path(), &mut seen);
                output_bytes.0 += bytes.0;
                output_bytes.1 += bytes.1;
            }
            for (field, delta) in [("i", 0), ("j", 1)] {
                let actual = aggregate(&e, field, TimeRange::all()).state;
                let expected_sum =
                    reference.values().map(|value| *value + delta).sum::<i64>() * series as i64;
                assert_eq!(actual.count, (reference.len() * series) as u64);
                let AggregateValues::Integer { sum, .. } = actual.values.unwrap() else {
                    panic!("unexpected churn type");
                };
                assert_eq!(sum, expected_sum);
            }
        }
        // Measure fragmentation before optional end-of-run maintenance. Root
        // groups may legitimately be small, so leaf occupancy is separate.
        let mut leaf_groups = 0_u64;
        let mut leaf_rows = 0_u64;
        let mut small_leaf_groups = 0_u64;
        let indexes = store.index_readers();
        let live_index_bytes = indexes.iter().map(|f| f.size()).sum::<u64>();
        for file in &indexes {
            for page in 0..file.footer.directory_pages() {
                for entry in file.directory_page(page).unwrap() {
                    let header = file.segment_header(&entry).unwrap();
                    if header.level(vlog::index::LEVEL_BLOCK).is_none() {
                        continue;
                    }
                    let mut groups = file
                        .groups(&entry, &header, vlog::index::LEVEL_BLOCK)
                        .unwrap();
                    for i in 0..groups.len() {
                        let rows = u64::from(groups.entry(i).unwrap().row_count);
                        leaf_groups += 1;
                        leaf_rows += rows;
                        small_leaf_groups += u64::from(rows < 64);
                    }
                }
            }
        }
        let rss_kib = std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|text| {
                text.lines()
                    .find(|line| line.starts_with("VmRSS:"))
                    .and_then(|line| line.split_whitespace().nth(1)?.parse::<u64>().ok())
            });
        println!(
            "AGG_CHURN {}",
            serde_json::json!({
                "case": if mixed { "overwrite_delete" } else { "ordered_append" },
                "series": series, "fields_per_series": 2, "rounds": rounds, "points_per_series_per_round": points,
                "write_workers": 1, "concurrent_query_workers": 0, "query_time_range": "all", "automatic_background_tasks": false,
                "write": latency_distribution(&mut write_ms), "snapshot": latency_distribution(&mut snapshot_ms),
                "compaction": latency_distribution(&mut compaction_ms), "delete": latency_distribution(&mut delete_ms),
                "flush_index_bytes": flush_bytes.0, "flush_vlog_bytes": flush_bytes.1,
                "committed_index_bytes_written": output_bytes.0, "committed_vlog_bytes_written": output_bytes.1,
                "index_rewrite_ratio_to_flush": output_bytes.0 as f64 / flush_bytes.0.max(1) as f64,
                "file_byte_scope": "committed idx/vlog only; excludes WAL, TSI, manifests, spools and device traffic",
                "live_index_files": indexes.len(), "live_index_bytes": live_index_bytes,
                "leaf_groups": leaf_groups, "leaf_rows": leaf_rows, "leaf_slot_utilization": leaf_rows as f64 / (leaf_groups * 256).max(1) as f64,
                "small_leaf_groups": small_leaf_groups, "copied_groups": metrics.copied_groups,
                "rewritten_groups": metrics.rewritten_groups, "coalesced_groups": metrics.coalesced_groups,
                "repaired_blocks": metrics.repaired_blocks, "streamed_rebuild_fields": metrics.streamed_rebuild_fields,
                "boundary_children_read": metrics.boundary_children_read, "process_rss_kib": rss_kib,
                "rss_scope": "whole test process including TSI, identities, cache and experiment driver"
            })
        );
        drop(indexes);
        e.engine.compact_aggregate_index().unwrap();
        e.reopen();
        assert_eq!(
            aggregate(&e, "i", TimeRange::all()).state.count,
            (reference.len() * series) as u64
        );
        e.close();
    }
}
