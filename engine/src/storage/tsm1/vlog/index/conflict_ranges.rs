//! Bounded proofs of gaps shared by query and compaction visibility checks.

use super::{LEVEL_BLOCK, Result, compact::SegmentSource, invalid};

pub(super) type SourceRanges = Vec<Vec<(i64, i64)>>;

pub(super) fn overlaps(ranges: &SourceRanges, source: usize, start: i64, end: i64) -> bool {
    ranges.iter().enumerate().any(|(other, ranges)| {
        other != source
            && ranges
                .get(ranges.partition_point(|range| range.1 < start))
                .is_some_and(|range| range.0 <= end)
    })
}

pub(super) fn refine(
    sources: &[SegmentSource<'_>],
    ranges: &mut SourceRanges,
    query_bounds: Option<(i64, i64)>,
    mut read_leaves: impl FnMut(usize) -> Result<Vec<(i64, i64)>>,
) -> Result<()> {
    // Optional position-only reads are bounded independently of file count.
    // Large trees retain conservative roots and the normal raw fallback.
    let mut remaining = 4096;
    for (source, tree) in sources.iter().enumerate() {
        let Some(header) = tree.header.level(LEVEL_BLOCK) else {
            continue;
        };
        let leaves = header.count as usize;
        if tree.root_level()? == LEVEL_BLOCK
            // A narrow query must not pre-read the rest of a source just to
            // prove gaps. Its normal descent already limits metadata reads.
            || query_bounds.is_some_and(|(start, end)| start > header.min_start || end < header.max_end)
            || leaves > 512
            || leaves > remaining
            || !overlaps(ranges, source, tree.entry.min_start, tree.entry.max_end)
        {
            continue;
        }
        let leaf_ranges = read_leaves(source)?;
        if leaf_ranges.len() != leaves
            || leaf_ranges.first().map(|r| r.0) != Some(header.min_start)
            || leaf_ranges.last().map(|r| r.1) != Some(header.max_end)
            || leaf_ranges.iter().any(|r| r.0 > r.1)
            || leaf_ranges.windows(2).any(|r| r[0].1 >= r[1].0)
        {
            return Err(invalid("conflict ranges do not cover ordered leaf level"));
        }
        ranges[source] = leaf_ranges;
        remaining -= leaves;
    }
    Ok(())
}
