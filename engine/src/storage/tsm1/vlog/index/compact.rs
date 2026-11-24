//! Ordered field compaction. Transactional replacement belongs to IndexStore;
//! this module writes an uncommitted segment and never removes input files.

use super::allocation::OutputGenerations;
use super::directory::DirEntry;
use super::output::{IndexOutput, SegmentSink};
use super::page_group::{ALL_COLUMNS, SelectedRecord, decode_group};
use super::reader::IndexFile;
use super::segment::SegmentHeader;
use super::spool::TreeSpool;
use super::tree::{Forest, NewNode, join_ordered};
use super::writer::IndexFileWriter;
use super::{Level, Result, invalid};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) struct SegmentSource<'a> {
    pub(crate) file: &'a IndexFile,
    pub(crate) entry: DirEntry,
    pub(crate) header: SegmentHeader,
}
impl SegmentSource<'_> {
    pub(crate) fn root_level(&self) -> Result<Level> {
        let level = self.entry.top_level;
        if !(1..=super::MAX_LEVELS).contains(&level) {
            return Err(invalid("unsupported tree root level"));
        }
        let h = self
            .header
            .level(level)
            .ok_or(invalid("missing field roots"))?;
        if h.count > super::FAN_OUT as u32
            || h.min_start != self.entry.min_start
            || h.max_end != self.entry.max_end
            || self.header.level_bits != (1 << level) - 1
        {
            return Err(invalid("incomplete field tree"));
        }
        Ok(level)
    }
    pub(crate) fn roots(&self) -> Result<Forest> {
        let level = self.root_level()?;
        Forest::new(
            self.file
                .read_level(
                    &self.entry,
                    &self.header,
                    level,
                    i64::MIN,
                    i64::MAX,
                    &ALL_COLUMNS,
                )?
                .into_iter()
                .map(SelectedRecord::into_record)
                .collect::<Result<_>>()?,
        )
    }
}

#[derive(Default, Debug)]
pub(crate) struct MergeStats {
    pub(crate) copied_groups: usize,
    pub(crate) rewritten_groups: usize,
    pub(crate) boundary_children_read: usize,
    pub(crate) removed_summaries: usize,
    pub(crate) new_summaries: usize,
    pub(crate) repaired_blocks: usize,
    pub(crate) streamed_rebuild_fields: usize,
    pub(crate) coalesced_groups: usize,
}
impl MergeStats {
    fn add(&mut self, other: Self) {
        self.copied_groups += other.copied_groups;
        self.rewritten_groups += other.rewritten_groups;
        self.boundary_children_read += other.boundary_children_read;
        self.removed_summaries += other.removed_summaries;
        self.new_summaries += other.new_summaries;
        self.repaired_blocks += other.repaired_blocks;
        self.streamed_rebuild_fields += other.streamed_rebuild_fields;
        self.coalesced_groups += other.coalesced_groups;
    }
}

pub(super) struct Fragment {
    pub(super) source: usize,
    pub(super) forest: Forest,
}

/// Input order is temporal, independent of file generation. Overlapping inputs
/// are rejected before writing; their statistics cannot safely be added.
pub(crate) fn merge_ordered_segments(
    writer: &mut IndexFileWriter,
    inputs: &[SegmentSource<'_>],
) -> Result<MergeStats> {
    merge_segments_cancellable(writer, inputs, &|| false)
}

fn merge_segments_cancellable(
    writer: &mut IndexFileWriter,
    inputs: &[SegmentSource<'_>],
    cancelled: &impl Fn() -> bool,
) -> Result<MergeStats> {
    check_cancelled(cancelled)?;
    let first = inputs.first().ok_or(invalid("empty compaction inputs"))?;
    for (i, source) in inputs.iter().enumerate() {
        if source.entry.seq_no != first.entry.seq_no
            || source.entry.typ != first.entry.typ
            || (i > 0 && inputs[i - 1].entry.max_end >= source.entry.min_start)
        {
            return Err(invalid(
                "compaction inputs require matching, disjoint ordered fields",
            ));
        }
    }
    let fragments = inputs
        .iter()
        .enumerate()
        .map(|(source, input)| {
            Ok(Fragment {
                source,
                forest: input.roots()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    merge_fragments(writer, inputs, &fragments, cancelled)
}

pub(super) fn merge_fragments(
    writer: &mut IndexFileWriter,
    inputs: &[SegmentSource<'_>],
    fragments: &[Fragment],
    cancelled: &impl Fn() -> bool,
) -> Result<MergeStats> {
    let first = inputs.first().ok_or(invalid("empty compaction sources"))?;
    writer.begin_segment(first.entry.seq_no, first.entry.typ)?;
    let stats = merge_fragment_groups(writer, inputs, fragments, cancelled)?;
    writer.end_segment()?;
    Ok(stats)
}

fn merge_fragment_groups(
    writer: &mut impl SegmentSink,
    inputs: &[SegmentSource<'_>],
    fragments: &[Fragment],
    cancelled: &impl Fn() -> bool,
) -> Result<MergeStats> {
    let mut stats = MergeStats::default();
    let mut forest = fragments
        .first()
        .ok_or(invalid("empty compaction fragments"))?
        .forest
        .clone();
    let mut added = BTreeMap::<(Level, i64), NewNode>::new();
    let mut removed = BTreeSet::new();
    for fragment in &fragments[1..] {
        check_cancelled(cancelled)?;
        let right = fragment.forest.clone();
        let plan = join_ordered(forest, right, |parent| {
            if let Some(node) = added.get(&(parent.level, parent.pos.start)) {
                return Ok(node.children.clone());
            }
            let fragment = fragments
                .iter()
                .find(|f| {
                    f.forest.level() >= parent.level
                        && f.forest.roots[0].pos.start <= parent.pos.start
                        && f.forest.roots.last().unwrap().pos.end >= parent.pos.end
                })
                .ok_or(invalid("parent source is missing"))?;
            let source = &inputs[fragment.source];
            let records = source.file.read_level(
                &source.entry,
                &source.header,
                parent.level - 1,
                parent.pos.start,
                parent.pos.end,
                &ALL_COLUMNS,
            )?;
            stats.boundary_children_read += records.len();
            records
                .into_iter()
                .map(SelectedRecord::into_record)
                .collect()
        })?;
        for parent in plan.removed {
            let key = (parent.level, parent.pos.start);
            if added.remove(&key).is_none() {
                removed.insert(key);
            }
        }
        for node in plan.added {
            added.insert((node.record.level, node.record.pos.start), node);
        }
        forest = plan.forest;
    }
    stats.removed_summaries = removed.len();
    stats.new_summaries = added.len();
    for level in (1..=forest.level()).rev() {
        let mut replacements = added
            .range((level, i64::MIN)..=(level, i64::MAX))
            .map(|(_, node)| node.record)
            .peekable();
        for fragment in fragments {
            let source = &inputs[fragment.source];
            if level > fragment.forest.level() {
                continue;
            }
            let min = fragment.forest.roots[0].pos.start;
            let max = fragment.forest.roots.last().unwrap().pos.end;
            let mut groups = source.file.groups(&source.entry, &source.header, level)?;
            for i in groups.predecessor(min)?..groups.len() {
                check_cancelled(cancelled)?;
                let entry = groups.entry(i)?;
                if entry.first_start > max {
                    break;
                }
                if entry.last_end < min {
                    continue;
                }
                while replacements
                    .peek()
                    .is_some_and(|r| r.pos.start < entry.first_start.max(min))
                {
                    writer.push(&replacements.next().unwrap())?;
                }
                let changed = entry.first_start < min
                    || entry.last_end > max
                    || removed
                        .range((level, entry.first_start)..=(level, entry.last_end))
                        .next()
                        .is_some()
                    || replacements
                        .peek()
                        .is_some_and(|r| r.pos.start <= entry.last_end);
                let group = groups.read(i)?;
                // Coalesce only adjacent undersized groups already inside one
                // retained fragment. Full groups retain their original bytes.
                let small = |e: &super::page_group::PageGroupEntry| {
                    e.row_count < 64 && e.first_start >= min && e.last_end <= max
                };
                let coalesce = !changed
                    && small(&entry)
                    && ((i > 0 && small(&groups.entry(i - 1)?))
                        || (i + 1 < groups.len() && small(&groups.entry(i + 1)?)));
                if !changed && !coalesce {
                    writer.append_group(group)?;
                    stats.copied_groups += 1;
                    continue;
                }
                let records = decode_group(
                    group.seq_no,
                    group.level,
                    group.typ,
                    &group.bytes,
                    &group.entry,
                    group.entry.row_count as usize,
                    &ALL_COLUMNS,
                )?;
                for record in records {
                    if record.pos.start < min || record.pos.end > max {
                        continue;
                    }
                    while replacements
                        .peek()
                        .is_some_and(|r| r.pos.start <= record.pos.start)
                    {
                        writer.push(&replacements.next().unwrap())?;
                    }
                    if !removed.contains(&(level, record.pos.start)) {
                        writer.push(&record.into_record()?)?;
                    }
                }
                while replacements
                    .peek()
                    .is_some_and(|r| r.pos.start <= entry.last_end.min(max))
                {
                    writer.push(&replacements.next().unwrap())?;
                }
                stats.rewritten_groups += 1;
                stats.coalesced_groups += usize::from(coalesce);
            }
        }
        for record in replacements {
            writer.push(&record)?;
        }
    }
    Ok(stats)
}

/// Retry only bounded metadata staging. Splits retain existing roots first;
/// descend into a root only when that subtree alone exceeds the budget.
pub(super) fn merge_fragment_outputs(
    output: &mut IndexOutput,
    inputs: &[SegmentSource<'_>],
    fragments: &[Fragment],
    cancelled: &impl Fn() -> bool,
) -> Result<MergeStats> {
    check_cancelled(cancelled)?;
    let first = inputs.first().ok_or(invalid("empty compaction sources"))?;
    let indivisible = fragments.len() == 1
        && fragments[0].forest.roots.len() == 1
        && fragments[0].forest.level() == super::LEVEL_BLOCK;
    let mut staged = TreeSpool::new(output.dir(), first.entry.seq_no, first.entry.typ)?;
    staged.set_limit(if indivisible {
        output.segment_limit()
    } else {
        output.segment_target()
    });
    let result =
        merge_fragment_groups(&mut staged, inputs, fragments, cancelled).and_then(|stats| {
            staged.encoded_size()?;
            Ok(stats)
        });
    match result {
        Ok(stats) => {
            check_cancelled(cancelled)?;
            output.append(first.entry.seq_no, first.entry.typ, &mut staged)?;
            return Ok(stats);
        }
        Err(super::Error::SegmentBudgetExceeded | super::Error::TreeCapacityExceeded)
            if !indivisible =>
        {
            ()
        }
        Err(super::Error::SegmentBudgetExceeded) => return Err(super::Error::MaxFileExceed),
        Err(error) => return Err(error),
    }
    drop(staged);
    let mut stats = MergeStats::default();
    if fragments.len() > 1 {
        let mid = fragments.len() / 2;
        stats.add(merge_fragment_outputs(
            output,
            inputs,
            &fragments[..mid],
            cancelled,
        )?);
        stats.add(merge_fragment_outputs(
            output,
            inputs,
            &fragments[mid..],
            cancelled,
        )?);
    } else {
        let fragment = &fragments[0];
        let mut roots = fragment.forest.roots.clone();
        if roots.len() == 1 {
            let parent = roots[0];
            let source = &inputs[fragment.source];
            roots = source
                .file
                .read_level(
                    &source.entry,
                    &source.header,
                    parent.level - 1,
                    parent.pos.start,
                    parent.pos.end,
                    &ALL_COLUMNS,
                )?
                .into_iter()
                .map(SelectedRecord::into_record)
                .collect::<Result<_>>()?;
            if roots.len() != parent.pos.child_count as usize
                || roots
                    .first()
                    .is_none_or(|r| r.pos.start != parent.pos.start)
                || roots.last().is_none_or(|r| r.pos.end != parent.pos.end)
            {
                return Err(invalid("split parent has incomplete children"));
            }
            stats.boundary_children_read += roots.len();
            stats.removed_summaries += 1;
        }
        let mid = roots.len() / 2;
        for roots in [&roots[..mid], &roots[mid..]] {
            let part = Fragment {
                source: fragment.source,
                forest: Forest::new(roots.to_vec())?,
            };
            stats.add(merge_fragment_outputs(output, inputs, &[part], cancelled)?);
        }
    }
    Ok(stats)
}

pub(super) fn check_cancelled(cancelled: &impl Fn() -> bool) -> Result<()> {
    if cancelled() {
        Err(super::Error::CompactionCancelled)
    } else {
        Ok(())
    }
}

struct DirectoryCursor<'a> {
    file: &'a IndexFile,
    page: usize,
    entries: std::collections::VecDeque<DirEntry>,
    previous: Option<DirEntry>,
}
impl<'a> DirectoryCursor<'a> {
    fn new(file: &'a IndexFile) -> Result<Self> {
        Ok(Self {
            file,
            page: 0,
            entries: file.directory_page(0)?.into(),
            previous: None,
        })
    }
    fn take(&mut self) -> Result<DirEntry> {
        let entry = self
            .entries
            .pop_front()
            .ok_or(invalid("directory cursor exhausted"))?;
        if self.previous.as_ref().is_some_and(|old| {
            old.seq_no >= entry.seq_no
                || old.seg_offset + u64::from(old.seg_len) != entry.seg_offset
        }) {
            return Err(invalid("invalid cross-page directory order"));
        }
        self.previous = Some(entry.clone());
        if self.entries.is_empty() && self.page + 1 < self.file.footer.directory_pages() {
            self.page += 1;
            self.entries = self.file.directory_page(self.page)?.into();
        }
        Ok(entry)
    }
}

pub(super) fn visit_fields(
    inputs: &[std::sync::Arc<IndexFile>],
    cancelled: &impl Fn() -> bool,
    mut visit: impl FnMut(Vec<(&IndexFile, DirEntry)>) -> Result<()>,
) -> Result<()> {
    let mut cursors = inputs
        .iter()
        .map(|file| DirectoryCursor::new(file))
        .collect::<Result<Vec<_>>>()?;
    while let Some(seq) = cursors
        .iter()
        .filter_map(|c| c.entries.front().map(|e| e.seq_no))
        .min()
    {
        check_cancelled(cancelled)?;
        let mut entries = Vec::new();
        for cursor in &mut cursors {
            if cursor.entries.front().is_some_and(|e| e.seq_no == seq) {
                let entry = cursor.take()?;
                entries.push((cursor.file, entry));
            }
        }
        entries.sort_by_key(|(_, entry)| entry.min_start);
        visit(entries)?;
    }
    Ok(())
}

/// Merges complete files while buffering one directory page per input. Outputs
/// split at field or retained-subtree boundaries before reaching the hard limit.
#[cfg(test)]
pub(crate) fn merge_files(
    dir: &std::path::Path,
    generation: i64,
    inputs: &[std::sync::Arc<IndexFile>],
    cancelled: &impl Fn() -> bool,
) -> Result<(Vec<super::writer::IndexFileMeta>, MergeStats)> {
    merge_files_with_target(dir, generation, inputs, cancelled, 128 * 1024 * 1024)
}

#[cfg(test)]
pub(super) fn merge_files_with_target(
    dir: &std::path::Path,
    generation: i64,
    inputs: &[std::sync::Arc<IndexFile>],
    cancelled: &impl Fn() -> bool,
    target_bytes: u64,
) -> Result<(Vec<super::writer::IndexFileMeta>, MergeStats)> {
    let (indexes, stats, _) = merge_files_impl(
        dir,
        OutputGenerations::isolated(generation),
        inputs,
        cancelled,
        target_bytes,
        None,
        Default::default(),
    )?;
    Ok((indexes, stats))
}

#[cfg(test)]
pub(super) fn merge_files_with_repair(
    dir: &std::path::Path,
    generation: i64,
    inputs: &[std::sync::Arc<IndexFile>],
    readers: &[std::sync::Arc<super::super::reader::VLogReader>],
    cancelled: &impl Fn() -> bool,
) -> Result<(
    Vec<super::writer::IndexFileMeta>,
    MergeStats,
    Vec<super::repair::DataOutput>,
)> {
    merge_files_controlled(
        dir,
        OutputGenerations::isolated(generation),
        inputs,
        readers,
        cancelled,
        128 * 1024 * 1024,
        Default::default(),
    )
}

pub(super) fn merge_files_controlled(
    dir: &std::path::Path,
    generations: OutputGenerations,
    inputs: &[std::sync::Arc<IndexFile>],
    readers: &[std::sync::Arc<super::super::reader::VLogReader>],
    cancelled: &impl Fn() -> bool,
    target_bytes: u64,
    budget: super::io_budget::IoBudget,
) -> Result<(
    Vec<super::writer::IndexFileMeta>,
    MergeStats,
    Vec<super::repair::DataOutput>,
)> {
    merge_files_impl(
        dir,
        generations,
        inputs,
        cancelled,
        target_bytes,
        Some(readers),
        budget,
    )
}

fn merge_files_impl(
    dir: &std::path::Path,
    generations: OutputGenerations,
    inputs: &[std::sync::Arc<IndexFile>],
    cancelled: &impl Fn() -> bool,
    target_bytes: u64,
    readers: Option<&[std::sync::Arc<super::super::reader::VLogReader>]>,
    budget: super::io_budget::IoBudget,
) -> Result<(
    Vec<super::writer::IndexFileMeta>,
    MergeStats,
    Vec<super::repair::DataOutput>,
)> {
    if inputs.is_empty()
        || inputs
            .iter()
            .any(|f| i64::from(f.file_no >> 8) >= generations.first)
    {
        return Err(invalid("invalid file compaction identities"));
    }
    let mut ids = BTreeSet::new();
    if inputs.iter().any(|f| !ids.insert(f.file_no)) {
        return Err(invalid("repeated compaction input"));
    }
    // Reject overlap before creating outputs. This pass reads directories only.
    let read_start = crate::common::slow_trace::now();
    visit_fields(inputs, cancelled, |entries| {
        if entries.windows(2).any(|p| p[0].1.typ != p[1].1.typ) {
            return Err(super::Error::TypeMismatch);
        }
        if readers.is_none()
            && entries
                .windows(2)
                .any(|p| p[0].1.max_end >= p[1].1.min_start)
        {
            return Err(super::Error::OverlappingInputs);
        }
        Ok(())
    })?;
    crate::common::slow_trace::log_background_stage(
        "compaction_read",
        read_start,
        "aggregate directory preflight",
    );
    let write_start = crate::common::slow_trace::now();
    let mut output = IndexOutput::new(
        dir,
        generations.clone(),
        super::planner::next_sequence(inputs),
        target_bytes,
        budget.clone(),
    );
    output.set_cancellation(cancelled);
    let mut total = MergeStats::default();
    let mut repair = super::repair::RepairWriter::new(dir, generations);
    repair.set_io_budget(budget.clone());
    visit_fields(inputs, cancelled, |entries| {
        let sources = entries
            .into_iter()
            .map(|(file, entry)| {
                let header = file.segment_header(&entry)?;
                Ok(SegmentSource {
                    file,
                    entry,
                    header,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let deleted = sources.iter().try_fold(false, |found, s| {
            Ok::<_, super::Error>(
                found
                    || s.file
                        .tombstone_view(s.entry.seq_no)
                        .coverage(s.entry.min_start, s.entry.max_end)?
                        .0,
            )
        })?;
        let stats = if deleted
            || sources.iter().any(|s| s.header.level(1).is_none())
            || sources
                .windows(2)
                .any(|p| p[0].entry.max_end >= p[1].entry.min_start)
        {
            super::repair::merge_overlapping(
                &mut output,
                &sources,
                readers.ok_or(super::Error::OverlappingInputs)?,
                &mut repair,
                cancelled,
            )?
        } else {
            let fragments = sources
                .iter()
                .enumerate()
                .map(|(source, input)| {
                    Ok(Fragment {
                        source,
                        forest: input.roots()?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            merge_fragment_outputs(&mut output, &sources, &fragments, cancelled)?
        };
        total.add(stats);
        Ok(())
    })?;
    check_cancelled(cancelled)?;
    // Keep index cleanup armed until the data writers have also closed.
    let vlogs = repair.finish()?;
    let cleanup = scopeguard::guard(vlogs, |files| {
        for (path, _, _) in files {
            let _ = std::fs::remove_file(path);
        }
    });
    let outputs = output.finish()?;
    let vlogs = scopeguard::ScopeGuard::into_inner(cleanup);
    crate::common::slow_trace::log_background_stage(
        "compaction_write",
        write_start,
        "aggregate index merge",
    );
    Ok((outputs, total, vlogs))
}
