//! Scoped diagnostic accumulation. Disabled unless the caller starts a profile.
//! Stages can be nested; their totals must not be added as disjoint wall time.

use std::{cell::RefCell, collections::BTreeMap, marker::PhantomData, rc::Rc, time::Duration};

const MAX_TRACKED_BLOCKS: usize = 100_000;

#[derive(Default)]
struct Stage {
    calls: u64,
    total: Duration,
    maximum: Duration,
}

#[derive(Default)]
struct Metadata {
    decodes: u64,
    rows: u64,
    pages: BTreeMap<(usize, u64), u64>,
    untracked_decodes: u64,
}

#[derive(Default)]
struct State {
    extrema_witnesses: u64,
    extrema_pruned_records: u64,
    extrema_pruned_leaf_blocks: u64,
    extrema_pruned_leaf_bytes: u64,
    vlog_read_calls: u64,
    vlog_requested_bytes: u64,
    vlog_read_bytes: u64,
    vlog_read_cache_hits: u64,
    stages: BTreeMap<String, BTreeMap<String, Stage>>,
    work: BTreeMap<String, BTreeMap<String, u64>>,
    metadata: BTreeMap<String, Metadata>,
    blocks: BTreeMap<(u32, u32), (u32, usize)>,
    block_reads: u64,
    decoded_values: u64,
    block_bytes: u64,
    untracked_reads: u64,
    index_groups: BTreeMap<(u32, u64), u64>,
    index_group_decodes: u64,
    index_position_rows: u64,
    untracked_index_groups: u64,
    nodes: BTreeMap<u8, QueryNodeProfile>,
}

thread_local! {
    static ACTIVE: RefCell<Option<State>> = const { RefCell::new(None) };
}

#[derive(serde::Serialize)]
pub struct QueryStageProfile {
    pub stage: String,
    pub context: String,
    pub calls: u64,
    pub total_ms: f64,
    pub max_ms: f64,
}

#[derive(serde::Serialize)]
pub struct QueryWorkProfile {
    pub stage: String,
    pub context: String,
    pub amount: u64,
}

#[derive(serde::Serialize)]
pub struct QueryMetadataProfile {
    pub kind: String,
    pub decodes: u64,
    pub rows: u64,
    pub unique_pages: usize,
    pub unique_rows: u64,
    pub untracked_decodes: u64,
}

#[derive(serde::Serialize)]
pub struct QueryProfileReport {
    pub extrema_witnesses: u64,
    pub extrema_pruned_records: u64,
    pub extrema_pruned_leaf_blocks: u64,
    pub extrema_pruned_leaf_bytes: u64,
    pub vlog_read_calls: u64,
    pub vlog_requested_bytes: u64,
    pub vlog_read_bytes: u64,
    pub vlog_read_cache_hits: u64,
    pub stages: Vec<QueryStageProfile>,
    pub work: Vec<QueryWorkProfile>,
    pub metadata: Vec<QueryMetadataProfile>,
    pub raw_block_reads: u64,
    pub unique_raw_blocks: usize,
    pub decoded_values: u64,
    pub unique_decoded_values: u64,
    pub raw_block_bytes: u64,
    pub unique_raw_block_bytes: u64,
    pub untracked_block_reads: u64,
    pub index_group_decodes: u64,
    pub unique_index_groups: usize,
    pub index_position_rows: u64,
    pub unique_index_position_rows: u64,
    pub untracked_index_groups: u64,
    pub nodes: Vec<QueryNodeProfile>,
}

/// Visited, non-deleted records that intersect at least one requested window.
#[derive(Default, serde::Serialize)]
pub struct QueryNodeProfile {
    pub level: u8,
    pub visited_records: u64,
    pub wholly_in_window_records: u64,
    pub summary_records: u64,
    pub conflict_records: u64,
    pub min_span_ns: u128,
    pub max_span_ns: u128,
    pub total_span_ns: u128,
}

/// Benchmark-only, thread-bound scope. Drop clears a failed query's profile.
pub struct QueryProfile {
    active: bool,
    thread_bound: PhantomData<Rc<()>>,
}

impl QueryProfile {
    pub fn start() -> Result<Self, &'static str> {
        ACTIVE.with(|slot| {
            let mut state = slot.borrow_mut();
            if state.is_some() {
                return Err("query profiling scopes cannot be nested");
            }
            *state = Some(State::default());
            Ok(Self {
                active: true,
                thread_bound: PhantomData,
            })
        })
    }

    pub fn finish(mut self) -> QueryProfileReport {
        let state = ACTIVE.with(|slot| slot.borrow_mut().take()).unwrap();
        self.active = false;
        QueryProfileReport {
            extrema_witnesses: state.extrema_witnesses,
            extrema_pruned_records: state.extrema_pruned_records,
            extrema_pruned_leaf_blocks: state.extrema_pruned_leaf_blocks,
            extrema_pruned_leaf_bytes: state.extrema_pruned_leaf_bytes,
            vlog_read_calls: state.vlog_read_calls,
            vlog_requested_bytes: state.vlog_requested_bytes,
            vlog_read_bytes: state.vlog_read_bytes,
            vlog_read_cache_hits: state.vlog_read_cache_hits,
            stages: state
                .stages
                .into_iter()
                .flat_map(|(stage, contexts)| {
                    contexts
                        .into_iter()
                        .map(move |(context, value)| QueryStageProfile {
                            stage: stage.clone(),
                            context,
                            calls: value.calls,
                            total_ms: value.total.as_secs_f64() * 1000.0,
                            max_ms: value.maximum.as_secs_f64() * 1000.0,
                        })
                })
                .collect(),
            work: state.work.into_iter().flat_map(|(stage, contexts)| {
                contexts.into_iter().map(move |(context, amount)| QueryWorkProfile {
                    stage: stage.clone(), context, amount,
                })
            }).collect(),
            metadata: state.metadata.into_iter().map(|(kind, value)| QueryMetadataProfile {
                kind,
                decodes: value.decodes,
                rows: value.rows,
                unique_pages: value.pages.len(),
                unique_rows: value.pages.values().sum(),
                untracked_decodes: value.untracked_decodes,
            }).collect(),
            raw_block_reads: state.block_reads,
            unique_raw_blocks: state.blocks.len(),
            decoded_values: state.decoded_values,
            unique_decoded_values: state
                .blocks
                .values()
                .map(|(_, points)| *points as u64)
                .sum(),
            raw_block_bytes: state.block_bytes,
            unique_raw_block_bytes: state
                .blocks
                .values()
                .map(|(bytes, _)| u64::from(*bytes))
                .sum(),
            untracked_block_reads: state.untracked_reads,
            index_group_decodes: state.index_group_decodes,
            unique_index_groups: state.index_groups.len(),
            index_position_rows: state.index_position_rows,
            unique_index_position_rows: state.index_groups.values().sum(),
            untracked_index_groups: state.untracked_index_groups,
            nodes: state.nodes.into_values().collect(),
        }
    }
}

pub(super) fn vlog_io(requested: u64, bytes: u64, calls: u64, hit: bool) {
    ACTIVE.with(|slot| {
        if let Some(state) = slot.borrow_mut().as_mut() {
            state.vlog_requested_bytes += requested;
            state.vlog_read_bytes += bytes;
            state.vlog_read_calls += calls;
            state.vlog_read_cache_hits += u64::from(hit);
        }
    });
}

pub(super) fn active() -> bool {
    ACTIVE.with(|slot| slot.borrow().is_some())
}

pub(super) fn work(stage: &str, context: &str, amount: u64) {
    ACTIVE.with(|slot| {
        let mut active = slot.borrow_mut();
        let Some(state) = active.as_mut() else { return };
        let value = state.work.entry(stage.into()).or_default().entry(context.into()).or_default();
        *value = value.saturating_add(amount);
    });
}

pub(super) fn index_metadata(kind: &str, file: usize, offset: u64, rows: usize) {
    ACTIVE.with(|slot| {
        let mut active = slot.borrow_mut();
        let Some(state) = active.as_mut() else { return };
        let value = state.metadata.entry(kind.into()).or_default();
        value.decodes += 1;
        value.rows += rows as u64;
        let key = (file, offset);
        if value.pages.len() < MAX_TRACKED_BLOCKS || value.pages.contains_key(&key) {
            value.pages.insert(key, rows as u64);
        } else {
            value.untracked_decodes += 1;
        }
    });
}

pub(super) fn extrema_witness() {
    ACTIVE.with(|slot| {
        if let Some(state) = slot.borrow_mut().as_mut() {
            state.extrema_witnesses += 1;
        }
    });
}

pub(super) fn extrema_pruned(is_leaf: bool, bytes: u32) {
    ACTIVE.with(|slot| {
        if let Some(state) = slot.borrow_mut().as_mut() {
            state.extrema_pruned_records += 1;
            if is_leaf {
                state.extrema_pruned_leaf_blocks += 1;
                state.extrema_pruned_leaf_bytes += u64::from(bytes);
            }
        }
    });
}

impl Drop for QueryProfile {
    fn drop(&mut self) {
        if self.active {
            ACTIVE.with(|slot| *slot.borrow_mut() = None);
        }
    }
}

pub(super) fn record(stage: &str, context: &str, elapsed: Duration) {
    ACTIVE.with(|slot| {
        let mut active = slot.borrow_mut();
        let Some(state) = active.as_mut() else { return };
        if !state.stages.contains_key(stage) {
            state.stages.insert(stage.into(), BTreeMap::new());
        }
        let contexts = state.stages.get_mut(stage).unwrap();
        if !contexts.contains_key(context) {
            contexts.insert(context.into(), Stage::default());
        }
        let value = contexts.get_mut(context).unwrap();
        value.calls += 1;
        value.total += elapsed;
        value.maximum = value.maximum.max(elapsed);
    });
}

pub(super) fn decoded_block(file: u32, offset: u32, bytes: u32, points: usize) {
    ACTIVE.with(|slot| {
        let mut active = slot.borrow_mut();
        let Some(state) = active.as_mut() else { return };
        state.block_reads += 1;
        state.decoded_values += points as u64;
        state.block_bytes += u64::from(bytes);
        let key = (file, offset);
        if state.blocks.len() < MAX_TRACKED_BLOCKS || state.blocks.contains_key(&key) {
            state.blocks.insert(key, (bytes, points));
        } else {
            state.untracked_reads += 1;
        }
    });
}

pub(super) fn decoded_index_group(file: u32, offset: u64, rows: u32) {
    ACTIVE.with(|slot| {
        let mut active = slot.borrow_mut();
        let Some(state) = active.as_mut() else { return };
        state.index_group_decodes += 1;
        state.index_position_rows += u64::from(rows);
        let key = (file, offset);
        if state.index_groups.len() < MAX_TRACKED_BLOCKS || state.index_groups.contains_key(&key) {
            state.index_groups.insert(key, u64::from(rows));
        } else {
            state.untracked_index_groups += 1;
        }
    });
}

pub(super) fn index_node(
    level: u8,
    start: i64,
    end: i64,
    full: bool,
    summary: bool,
    conflict: bool,
) {
    ACTIVE.with(|slot| {
        let mut active = slot.borrow_mut();
        let Some(state) = active.as_mut() else { return };
        let span = (i128::from(end) - i128::from(start) + 1) as u128;
        let node = state.nodes.entry(level).or_default();
        node.level = level;
        if node.visited_records == 0 {
            node.min_span_ns = span;
        }
        node.visited_records += 1;
        node.wholly_in_window_records += u64::from(full);
        node.summary_records += u64::from(summary);
        node.conflict_records += u64::from(conflict);
        node.min_span_ns = node.min_span_ns.min(span);
        node.max_span_ns = node.max_span_ns.max(span);
        node.total_span_ns += span;
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_counts_work_and_repeated_metadata_without_per_row_stages() {
        let profile = QueryProfile::start().unwrap();
        work("prefetch", "scanned", 3);
        work("prefetch", "scanned", 7);
        index_metadata("directory", 1, 10, 100);
        index_metadata("directory", 1, 10, 100);
        index_metadata("directory", 2, 10, 50);
        let report = profile.finish();
        assert_eq!(report.work[0].amount, 10);
        let metadata = &report.metadata[0];
        assert_eq!((metadata.decodes, metadata.unique_pages), (3, 2));
        assert_eq!((metadata.rows, metadata.unique_rows), (250, 150));
        assert_eq!(metadata.untracked_decodes, 0);
        assert!(report.stages.is_empty());
    }

    #[test]
    fn profile_accumulates_short_stages_and_repeated_blocks() {
        let profile = QueryProfile::start().unwrap();
        record("lookup", "test", Duration::from_micros(3));
        record("lookup", "test", Duration::from_micros(5));
        decoded_block(1, 5, 20, 4);
        decoded_block(1, 5, 20, 4);
        extrema_witness();
        extrema_pruned(true, 20);
        extrema_pruned(false, 200);
        let report = profile.finish();
        assert_eq!(report.stages[0].calls, 2);
        assert_eq!(report.stages[0].total_ms, 0.008);
        assert_eq!((report.raw_block_reads, report.unique_raw_blocks), (2, 1));
        assert_eq!(
            (
                report.extrema_witnesses,
                report.extrema_pruned_records,
                report.extrema_pruned_leaf_blocks,
                report.extrema_pruned_leaf_bytes
            ),
            (1, 2, 1, 20)
        );
        assert_eq!(
            (report.decoded_values, report.unique_decoded_values),
            (8, 4)
        );
    }

    #[test]
    fn profile_tracks_repeated_groups_and_wide_node_spans() {
        let profile = QueryProfile::start().unwrap();
        decoded_index_group(1, 10, 4);
        decoded_index_group(1, 10, 4);
        decoded_index_group(2, 10, 3);
        index_node(2, i64::MIN, i64::MAX, false, false, false);
        index_node(1, -1, 1, true, true, false);
        let report = profile.finish();
        assert_eq!(
            (report.index_group_decodes, report.unique_index_groups),
            (3, 2)
        );
        assert_eq!(
            (
                report.index_position_rows,
                report.unique_index_position_rows
            ),
            (11, 7)
        );
        assert_eq!(report.nodes[0].summary_records, 1);
        assert_eq!(report.nodes[1].max_span_ns, 1_u128 << 64);
        assert_eq!(report.untracked_index_groups, 0);
    }

    #[test]
    fn profile_drop_and_other_threads_do_not_leak_state() {
        let profile = QueryProfile::start().unwrap();
        assert!(QueryProfile::start().is_err());
        std::thread::spawn(|| {
            let other = QueryProfile::start().unwrap();
            record("other", "", Duration::from_micros(1));
            assert_eq!(other.finish().stages.len(), 1);
        })
        .join()
        .unwrap();
        assert!(profile.finish().stages.is_empty());
        drop(QueryProfile::start().unwrap());
        assert!(QueryProfile::start().unwrap().finish().stages.is_empty());
    }
}
