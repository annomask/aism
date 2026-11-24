#[cfg(feature = "slow-trace")]
pub(crate) type TraceInstant = std::time::Instant;

#[cfg(not(feature = "slow-trace"))]
pub(crate) type TraceInstant = ();

#[cfg(feature = "slow-trace")]
pub(crate) type TraceDuration = std::time::Duration;

#[cfg(not(feature = "slow-trace"))]
pub(crate) type TraceDuration = ();

#[cfg(feature = "slow-trace")]
const QUERY_STAGE_THRESHOLD_MS: u64 = 20;
#[cfg(feature = "slow-trace")]
const QUERY_BREAKDOWN_THRESHOLD_MS: u128 = 50;
#[cfg(feature = "slow-trace")]
const LOCK_WAIT_THRESHOLD_MS: u64 = 5;
#[cfg(feature = "slow-trace")]
const BACKGROUND_STAGE_THRESHOLD_MS: u64 = 50;

#[cfg(feature = "slow-trace")]
fn deferred_query_timeline_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var_os("TZTSDB_DEFERRED_QUERY_TIMELINE").is_some_and(|value| value != "0")
            || std::env::var_os("TZTSDB_SUPPRESS_LEGACY_SLOW_TRACE")
                .is_some_and(|value| value != "0")
    })
}

#[cfg(feature = "slow-trace")]
#[inline]
pub(crate) fn now() -> TraceInstant {
    std::time::Instant::now()
}

#[cfg(not(feature = "slow-trace"))]
#[inline]
pub(crate) fn now() -> TraceInstant {}

#[cfg(feature = "slow-trace")]
#[inline]
pub(crate) fn elapsed(start: TraceInstant) -> TraceDuration {
    start.elapsed()
}

#[cfg(not(feature = "slow-trace"))]
#[inline]
pub(crate) fn elapsed(_start: TraceInstant) -> TraceDuration {}

#[cfg(feature = "slow-trace")]
#[inline]
pub(crate) fn log_query_stage(stage: &str, start: TraceInstant, context: &str) {
    log_query_stage_until(stage, start, std::time::Instant::now(), context);
}

#[cfg(feature = "slow-trace")]
#[inline]
pub(crate) fn log_query_stage_until(
    stage: &str,
    start: TraceInstant,
    end: TraceInstant,
    context: &str,
) {
    let elapsed = end.duration_since(start);
    profile::record(stage, context, elapsed);
    if deferred_query_timeline_enabled() {
        return;
    }
    log_slow_query_stage(stage, elapsed, context);
}

#[cfg(not(feature = "slow-trace"))]
#[inline]
pub(crate) fn log_query_stage(_stage: &str, _start: TraceInstant, _context: &str) {}

#[cfg(not(feature = "slow-trace"))]
#[inline]
pub(crate) fn log_query_stage_until(
    _stage: &str,
    _start: TraceInstant,
    _end: TraceInstant,
    _context: &str,
) {
}

#[cfg(feature = "slow-trace")]
#[inline]
pub(crate) fn elapsed_ms(start: TraceInstant) -> u128 {
    start.elapsed().as_millis()
}

#[cfg(not(feature = "slow-trace"))]
#[inline]
pub(crate) fn elapsed_ms(_start: TraceInstant) -> u128 {
    0
}

#[cfg(feature = "slow-trace")]
#[inline]
pub(crate) fn log_query_breakdown(total_ms: u128, context: &str) {
    log_query_breakdown_with_threshold(total_ms, QUERY_BREAKDOWN_THRESHOLD_MS, context);
}

#[cfg(not(feature = "slow-trace"))]
#[inline]
pub(crate) fn log_query_breakdown(_total_ms: u128, _context: &str) {}

#[cfg(feature = "slow-trace")]
#[inline]
pub(crate) fn log_query_breakdown_with_threshold(
    total_ms: u128,
    threshold_ms: u128,
    context: &str,
) {
    if deferred_query_timeline_enabled() {
        return;
    }
    if total_ms > threshold_ms {
        eprintln!(
            "reefsdb_slow_query_breakdown total_ms={}{}",
            total_ms,
            format_context(context)
        );
    }
}

#[cfg(not(feature = "slow-trace"))]
#[inline]
pub(crate) fn log_query_breakdown_with_threshold(
    _total_ms: u128,
    _threshold_ms: u128,
    _context: &str,
) {
}

#[cfg(feature = "slow-trace")]
#[inline]
pub(crate) fn log_lock_wait(lock: &str, start: TraceInstant, context: &str) {
    log_lock_wait_until(lock, start, std::time::Instant::now(), context);
}

/// Record acquisition time after the guard has been released, excluding lock work.
#[cfg(feature = "slow-trace")]
#[inline]
pub(crate) fn log_lock_wait_until(
    lock: &str,
    start: TraceInstant,
    acquired: TraceInstant,
    context: &str,
) {
    let elapsed = acquired.duration_since(start);
    profile::record("lock_wait", lock, elapsed);
    if deferred_query_timeline_enabled() {
        return;
    }
    if elapsed > std::time::Duration::from_millis(LOCK_WAIT_THRESHOLD_MS) {
        eprintln!(
            "reefsdb_slow_lock_wait lock={} elapsed_ms={}{}",
            lock,
            elapsed.as_millis(),
            format_context(context)
        );
    }
}

#[cfg(not(feature = "slow-trace"))]
#[inline]
pub(crate) fn log_lock_wait(_lock: &str, _start: TraceInstant, _context: &str) {}

#[cfg(not(feature = "slow-trace"))]
#[inline]
pub(crate) fn log_lock_wait_until(
    _lock: &str,
    _start: TraceInstant,
    _acquired: TraceInstant,
    _context: &str,
) {
}

#[inline]
pub(crate) fn profile_decoded_block(file: u32, offset: u32, bytes: u32, points: usize) {
    #[cfg(feature = "slow-trace")]
    profile::decoded_block(file, offset, bytes, points);
    #[cfg(not(feature = "slow-trace"))]
    let _ = (file, offset, bytes, points);
}

#[inline]
pub(crate) fn profile_extrema_witness() {
    #[cfg(feature = "slow-trace")]
    profile::extrema_witness();
}

#[inline]
pub(crate) fn profile_extrema_pruned(is_leaf: bool, bytes: u32) {
    #[cfg(feature = "slow-trace")]
    profile::extrema_pruned(is_leaf, bytes);
    #[cfg(not(feature = "slow-trace"))]
    let _ = (is_leaf, bytes);
}

#[cfg(feature = "slow-trace")]
#[inline]
pub(crate) fn log_planner_lock_wait(lock: &str, elapsed: TraceDuration) {
    if deferred_query_timeline_enabled() {
        return;
    }
    if elapsed > std::time::Duration::from_millis(LOCK_WAIT_THRESHOLD_MS) {
        eprintln!(
            "reefsdb_slow_planner_lock_wait lock={} elapsed_us={}",
            lock,
            elapsed.as_micros().min(u128::from(u64::MAX)),
        );
    }
}

#[cfg(not(feature = "slow-trace"))]
#[inline]
pub(crate) fn log_planner_lock_wait(_lock: &str, _elapsed: TraceDuration) {}

#[cfg(feature = "slow-trace")]
#[inline]
pub(crate) fn log_background_stage(stage: &str, start: TraceInstant, context: &str) {
    let elapsed = start.elapsed();
    profile::record(stage, context, elapsed);
    if elapsed > std::time::Duration::from_millis(BACKGROUND_STAGE_THRESHOLD_MS) {
        eprintln!(
            "reefsdb_slow_background_stage stage={} elapsed_ms={}{}",
            stage,
            elapsed.as_millis(),
            format_context(context)
        );
    }
}

#[cfg(not(feature = "slow-trace"))]
#[inline]
pub(crate) fn log_background_stage(_stage: &str, _start: TraceInstant, _context: &str) {}

#[cfg(feature = "slow-trace")]
#[inline]
pub(crate) fn log_background_error<E: std::fmt::Debug>(stage: &str, component: &str, error: &E) {
    eprintln!(
        "reefsdb_slow_background_error stage={} component={} error={:?}",
        stage, component, error
    );
}

#[cfg(not(feature = "slow-trace"))]
#[inline]
pub(crate) fn log_background_error<E>(_stage: &str, _component: &str, _error: &E) {}

#[cfg(feature = "slow-trace")]
#[inline]
pub(crate) fn log_grouped_storage_jobs(status: &str, context: &str) {
    if deferred_query_timeline_enabled() {
        return;
    }
    eprintln!(
        "reefsdb_slow_grouped_storage_jobs status={}{}",
        status,
        format_context(context)
    );
}

#[cfg(not(feature = "slow-trace"))]
#[inline]
pub(crate) fn log_grouped_storage_jobs(_status: &str, _context: &str) {}

#[cfg(feature = "slow-trace")]
#[inline]
pub(crate) fn log_vlog_file_store_replace_meta_begin(
    file_count: usize,
    key_count: usize,
    index_count: usize,
) {
    eprintln!(
        "vlog_file_store_replace_meta_begin file_count={} key_count={} index_count={}",
        file_count, key_count, index_count
    );
}

#[cfg(not(feature = "slow-trace"))]
#[inline]
pub(crate) fn log_vlog_file_store_replace_meta_begin(
    _file_count: usize,
    _key_count: usize,
    _index_count: usize,
) {
}

#[cfg(feature = "slow-trace")]
#[inline]
pub(crate) fn log_vlog_file_store_replace_meta_stage(
    stage: &str,
    start: TraceInstant,
    key_count: usize,
    index_count: usize,
) {
    eprintln!(
        "vlog_file_store_replace_meta_stage stage={} elapsed_ms={} key_count={} index_count={}",
        stage,
        start.elapsed().as_millis(),
        key_count,
        index_count
    );
}

#[cfg(not(feature = "slow-trace"))]
#[inline]
pub(crate) fn log_vlog_file_store_replace_meta_stage(
    _stage: &str,
    _start: TraceInstant,
    _key_count: usize,
    _index_count: usize,
) {
}

#[cfg(feature = "slow-trace")]
#[inline]
pub(crate) fn log_vlog_file_store_replace_meta_finish(
    start: TraceInstant,
    file_count: usize,
    key_count: usize,
    index_count: usize,
) {
    eprintln!(
        "vlog_file_store_replace_meta_finish elapsed_ms={} file_count={} key_count={} index_count={}",
        start.elapsed().as_millis(),
        file_count,
        key_count,
        index_count
    );
}

#[cfg(not(feature = "slow-trace"))]
#[inline]
pub(crate) fn log_vlog_file_store_replace_meta_finish(
    _start: TraceInstant,
    _file_count: usize,
    _key_count: usize,
    _index_count: usize,
) {
}

#[cfg(feature = "slow-trace")]
#[inline]
pub(crate) fn log_vlog_file_store_replace_old_stage(stage: &str, start: TraceInstant) {
    eprintln!(
        "vlog_file_store_replace_old_stage stage={} elapsed_ms={}",
        stage,
        start.elapsed().as_millis()
    );
}

#[cfg(not(feature = "slow-trace"))]
#[inline]
pub(crate) fn log_vlog_file_store_replace_old_stage(_stage: &str, _start: TraceInstant) {}

#[cfg(feature = "slow-trace")]
#[inline]
pub(crate) fn log_vlog_compaction_event(event: &str, context: &str) {
    eprintln!("{}{}", event, format_context(context));
}

#[cfg(not(feature = "slow-trace"))]
#[inline]
pub(crate) fn log_vlog_compaction_event(_event: &str, _context: &str) {}

#[cfg(feature = "slow-trace")]
#[inline]
pub(crate) fn log_engine_config(shard_id: u64, vlog_enabled: bool) {
    eprintln!(
        "reefsdb_slow_config component=engine_open shard_id={} vlog_enabled={}",
        shard_id, vlog_enabled
    );
}

#[cfg(not(feature = "slow-trace"))]
#[inline]
pub(crate) fn log_engine_config(_shard_id: u64, _vlog_enabled: bool) {}

#[cfg(feature = "slow-trace")]
#[inline]
pub(crate) fn log_metadata_commit(
    total_start: TraceInstant,
    build_ms: u128,
    encode_ms: u128,
    commit_ms: u128,
    context: &str,
) {
    eprintln!(
        "reefsdb_slow_metadata_commit total_ms={} build_ms={} encode_ms={} commit_ms={}{}",
        total_start.elapsed().as_millis(),
        build_ms,
        encode_ms,
        commit_ms,
        format_context(context)
    );
}

#[cfg(not(feature = "slow-trace"))]
#[allow(dead_code)]
#[inline]
pub(crate) fn log_metadata_commit(
    _total_start: TraceInstant,
    _build_ms: u128,
    _encode_ms: u128,
    _commit_ms: u128,
    _context: &str,
) {
}

#[cfg(feature = "slow-trace")]
#[inline]
fn log_slow_query_stage(stage: &str, elapsed: std::time::Duration, context: &str) {
    if elapsed > std::time::Duration::from_millis(QUERY_STAGE_THRESHOLD_MS) {
        eprintln!(
            "reefsdb_slow_query_stage stage={} elapsed_ms={}{}",
            stage,
            elapsed.as_millis(),
            format_context(context)
        );
    }
}

#[cfg(feature = "slow-trace")]
#[inline]
fn format_context(context: &str) -> String {
    if context.is_empty() {
        String::new()
    } else {
        format!(" {}", context)
    }
}
#[cfg(feature = "slow-trace")]
#[path = "query_profile.rs"]
mod profile;
#[cfg(feature = "slow-trace")]
pub use profile::{QueryProfile, QueryProfileReport};

pub(crate) fn profile_active() -> bool {
    #[cfg(feature = "slow-trace")]
    {
        profile::active()
    }
    #[cfg(not(feature = "slow-trace"))]
    {
        false
    }
}

#[inline]
pub(crate) fn profile_work(_stage: &str, _context: &str, _amount: u64) {
    #[cfg(feature = "slow-trace")]
    profile::work(_stage, _context, _amount);
}

#[inline]
pub(crate) fn profile_index_metadata(_kind: &str, _file: usize, _offset: u64, _rows: usize) {
    #[cfg(feature = "slow-trace")]
    profile::index_metadata(_kind, _file, _offset, _rows);
}

#[inline]
pub(crate) fn profile_vlog_io(_requested: u64, _bytes: u64, _calls: u64, _hit: bool) {
    #[cfg(feature = "slow-trace")]
    profile::vlog_io(_requested, _bytes, _calls, _hit);
}

#[inline]
pub(crate) fn profile_index_group(_file: u32, _offset: u64, _rows: u32) {
    #[cfg(feature = "slow-trace")]
    profile::decoded_index_group(_file, _offset, _rows);
}

#[inline]
pub(crate) fn profile_index_node(
    _level: u8,
    _start: i64,
    _end: i64,
    _full: bool,
    _summary: bool,
    _conflict: bool,
) {
    #[cfg(feature = "slow-trace")]
    profile::index_node(_level, _start, _end, _full, _summary, _conflict);
}

#[cfg(all(test, feature = "slow-trace"))]
mod measured_span_tests {
    use super::*;

    #[test]
    fn lock_wait_excludes_work_and_post_release_reporting() {
        let profile = QueryProfile::start().unwrap();
        let started = std::time::Instant::now();
        let acquired = started + std::time::Duration::from_millis(2);
        let released = acquired + std::time::Duration::from_millis(3);
        log_lock_wait_until("test_cache", started, acquired, "test");
        log_query_stage_until("cache_work", acquired, released, "test");
        let report = profile.finish();
        assert_eq!(report.stages.len(), 2);
        assert_eq!(
            report.stages.iter().find(|s| s.stage == "lock_wait").unwrap().total_ms,
            2.0
        );
        assert_eq!(
            report.stages.iter().find(|s| s.stage == "cache_work").unwrap().total_ms,
            3.0
        );
    }
}
