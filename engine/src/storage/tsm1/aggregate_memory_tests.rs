//! Explicit small-data diagnostics; never part of the normal test suite.

use super::*;
use crate::query::storage_ir::TimeRange;

fn memory_sample(engine: &tests::Engine, stage: &str) {
    let (cache, files, raw_bytes) = {
        let inner = engine.engine.inner.read();
        let vlog = inner.vlog_inner.as_ref().unwrap().read();
        (
            Arc::clone(&vlog.vlog_index_cache),
            Arc::clone(&vlog.vlog_file_store),
            inner.cache.size(),
        )
    };
    let mut locators = 0_usize;
    let mut source_blocks = 0_usize;
    let mut removed_ranges = 0_usize;
    let mut reader_keys = 0_usize;
    let mut reader_locators = 0_usize;
    for reader in files.reader_snapshot() {
        for (key, _) in reader.keys() {
            reader_keys += 1;
            reader_locators += reader.entries(&key).len();
        }
    }
    cache.apply_entry_fn(|_, entry| -> std::result::Result<(), ()> {
        locators += entry.count();
        for source in entry.aggregate_sources().values() {
            source_blocks += source.blocks.len();
            removed_ranges += source.removed.len();
        }
        Ok(())
    }).unwrap();
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let resident = status.lines().filter(|line| {
        ["VmRSS:", "VmHWM:", "RssAnon:"].iter().any(|key| line.starts_with(key))
    }).collect::<Vec<_>>();
    eprintln!("compaction_memory {}", serde_json::json!({
        "stage": stage, "raw_cache_bytes": raw_bytes,
        "locator_accounted_bytes": cache.size(), "locators": locators,
        "source_blocks": source_blocks, "removed_ranges": removed_ranges,
        "reader_keys": reader_keys, "reader_locators": reader_locators,
        "vlog_files": files.reader_snapshot().len(),
        "hierarchy_readers": engine.engine.aggregate_index.as_ref().unwrap().store.index_readers().len(),
        "process": resident,
    }));
}

fn compact_native(engine: &tests::Engine, fast: bool) {
    let (inner, compactor) = {
        let inner = engine.engine.inner.read();
        (
            Arc::clone(inner.vlog_inner.as_ref().unwrap()),
            Arc::clone(inner.vlog_compactor.as_ref().unwrap()),
        )
    };
    let files = inner.read().vlog_file_store.reader_snapshot();
    let success = Arc::new(AtomicI64::new(0));
    let errors = Arc::new(AtomicI64::new(0));
    compactor.enable_vlog_compactions();
    let strategy = VLogDefaultCompactionStrategy {
        group: VLogCompactionGroup(files.iter().map(|r| r.path().to_string_lossy().into_owned()).collect()),
        fast,
        level: 4,
        duration_stat: Arc::new(AtomicI64::new(0)),
        active_stat: Arc::new(AtomicI64::new(0)),
        success_stat: Arc::clone(&success),
        error_stat: Arc::clone(&errors),
        compactor,
        inner,
    };
    futures::executor::block_on(strategy.apply());
    assert_eq!(errors.load(Ordering::SeqCst), 0);
    assert_eq!(success.load(Ordering::SeqCst), 1);
}

#[test]
#[ignore = "explicit compaction heap diagnostic on fresh small data"]
fn native_compaction_memory_churn() {
    let setting = |name: &str, default: usize, max: usize| {
        let n = std::env::var(name).ok().map(|s| s.parse::<usize>().unwrap()).unwrap_or(default);
        assert!((1..=max).contains(&n));
        n
    };
    let hosts = setting("REEFS_MEMORY_HOSTS", 64, 256);
    let batches = setting("REEFS_MEMORY_BATCHES", 32, 128);
    let points = setting("REEFS_MEMORY_POINTS", 49, 64);
    let interval = setting("REEFS_MEMORY_COMPACT_EVERY", 8, 128);
    let fast = std::env::var("REEFS_MEMORY_FAST").is_ok_and(|s| s == "1");
    let mut engine = tests::Engine::with_aggregate_index();
    for (field, typ) in [("i", crate::models::INTEGER), ("f", crate::models::FLOAT), ("b", crate::models::BOOL)] {
        engine.engine.measurement_fields(b"cpu").create_field_if_not_exists(field.as_bytes(), typ).unwrap();
    }
    memory_sample(&engine, "opened");
    for batch in 0..batches {
        {
            let mut lines = Vec::new();
            for t in batch * points..(batch + 1) * points {
                for host in 0..hosts {
                    let value = t + host;
                    lines.push(format!("cpu,host=h{host:04} i={value}i,f={},b={} {t}", value as f64 / 4.0, value % 2 == 0));
                }
            }
            engine.write_points_string(&lines.iter().map(String::as_str).collect::<Vec<_>>()).unwrap();
        }
        engine.must_write_snapshot();
        memory_sample(&engine, &format!("snapshot-{batch}"));
        if (batch + 1) % interval == 0 || batch + 1 == batches {
            let expected = ["i", "f", "b"].map(|field| aggregate_index_tests::aggregate(&engine, field, TimeRange::all()).state);
            for state in &expected { assert_eq!(state.count, ((batch + 1) * points * hosts) as u64); }
            compact_native(&engine, fast);
            memory_sample(&engine, &format!("data-compacted-{batch}"));
            engine.engine.compact_aggregate_index().unwrap();
            memory_sample(&engine, &format!("index-compacted-{batch}"));
            for (field, state) in ["i", "f", "b"].iter().zip(&expected) {
                assert_eq!(&aggregate_index_tests::aggregate(&engine, field, TimeRange::all()).state, state);
            }
        }
    }
    let expected = ["i", "f", "b"].map(|field| aggregate_index_tests::aggregate(&engine, field, TimeRange::all()).state);
    engine.reopen();
    memory_sample(&engine, "reopened");
    for (field, state) in ["i", "f", "b"].iter().zip(&expected) {
        assert_eq!(&aggregate_index_tests::aggregate(&engine, field, TimeRange::all()).state, state);
    }
    engine.close();
}
