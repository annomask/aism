use super::allocation::OutputGenerations;
use super::output::{FieldTreeBuilder, IndexOutput};
use super::writer::IndexFileMeta;
use super::{AggValues, Record, Result, SeqNo, invalid};
use crate::storage::tsm1::{cache::Cache, vlog::VLogIndex};
use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
};

/// Builds a complete field tree from immutable snapshot values and completed VLog
/// writes. No values are decoded, and identities were resolved before rotation.
#[cfg(test)]
pub(crate) fn write_index_files(
    dir: &Path,
    generation: i64,
    cache: &Cache,
    indexes: &HashMap<String, Vec<VLogIndex>>,
) -> Result<Vec<IndexFileMeta>> {
    write_index_files_with_budget(
        dir,
        OutputGenerations::isolated(generation),
        cache,
        indexes,
        Default::default(),
    )
}

pub(crate) fn write_index_files_with_budget(
    dir: &Path,
    generations: OutputGenerations,
    cache: &Cache,
    indexes: &HashMap<String, Vec<VLogIndex>>,
    budget: super::io_budget::IoBudget,
) -> Result<Vec<IndexFileMeta>> {
    write_index_files_with_target(dir, generations, cache, indexes, budget, 128 * 1024 * 1024)
}

fn write_index_files_with_target(
    dir: &Path,
    generations: OutputGenerations,
    cache: &Cache,
    indexes: &HashMap<String, Vec<VLogIndex>>,
    budget: super::io_budget::IoBudget,
    target: u64,
) -> Result<Vec<IndexFileMeta>> {
    let mut ordered = BTreeMap::new();
    for (key, blocks) in indexes {
        let seq = SeqNo::from_raw(
            cache
                .sequence(key.as_bytes())
                .ok_or_else(|| invalid("snapshot cache sequence is missing"))?,
        )?;
        if ordered.insert(seq, (key, blocks)).is_some() {
            return Err(invalid("two snapshot keys have the same sequence"));
        }
    }
    if ordered.is_empty() || ordered.len() != cache.count() as usize {
        return Err(invalid("snapshot indexes do not cover the cache"));
    }
    let mut output = IndexOutput::new(dir, generations, 0, target, budget);
    for (seq_no, (key, blocks)) in ordered {
        let values = cache.values(key.as_bytes());
        let mut blocks = blocks.iter().collect::<Vec<_>>();
        blocks.sort_by_key(|b| b.min);
        let typ = blocks
            .first()
            .ok_or(invalid("snapshot field has no blocks"))?
            .typ;
        let mut builder = FieldTreeBuilder::new(dir, seq_no, typ)?;
        let mut consumed = 0;
        for block in blocks {
            let end = values.partition_point(|v| v.unix_nano() <= block.max);
            if end <= consumed
                || values[consumed].unix_nano() != block.min
                || values[end - 1].unix_nano() != block.max
            {
                return Err(invalid("snapshot block does not match cache values"));
            }
            builder.push(
                Record::from_vlog_index(
                    seq_no,
                    block,
                    AggValues::from_block(&values[consumed..end])?,
                )?,
                &mut output,
            )?;
            consumed = end;
        }
        if consumed != values.len() {
            return Err(invalid("snapshot block coverage is incomplete"));
        }
        builder.finish(&mut output)?;
    }
    output.finish()
}

/// Encoded payloads and block boundaries have already been chosen by the native
/// VLog writer. This builds only the added key-side aggregate representation.
pub(crate) fn write_compaction_files(
    dir: &Path,
    generations: OutputGenerations,
    fields: BTreeMap<SeqNo, Vec<Record>>,
    budget: super::io_budget::IoBudget,
    target: u64,
) -> Result<Vec<IndexFileMeta>> {
    let mut output = IndexOutput::new(dir, generations, 0, target, budget);
    for (seq, records) in fields {
        let first = records.first().ok_or(invalid("native compaction field has no blocks"))?;
        let mut builder = FieldTreeBuilder::new(dir, seq, first.values.field_type())?;
        for record in records { builder.push(record, &mut output)?; }
        builder.finish(&mut output)?;
    }
    output.finish()
}

#[cfg(test)]
mod tests {
    use super::super::{
        page_group::{ALL_COLUMNS, SelectedRecord},
        reader::IndexFile,
    };
    use super::*;
    use crate::models::{
        INTEGER,
        points::{IntegerValue, Value},
    };

    #[test]
    fn snapshot_builds_complete_field_local_trees_from_its_own_values() {
        let dir = tempfile::tempdir().unwrap();
        let cache = Cache::new(0);
        let seq = SeqNo::new(1, 1).unwrap();
        let key = "cpu,host=A#!~#i".to_string();
        cache
            .write_multi(HashMap::from([(
                key.clone(),
                (1..=210)
                    .map(|i| Value::IntegerValue(IntegerValue::new(i, i)))
                    .collect(),
            )]))
            .unwrap();
        cache.set_sequence(key.as_bytes(), seq.get()).unwrap();
        let snapshot = cache.snapshot().unwrap();
        // Each completed VLog block covers exactly three values in this snapshot.
        let indexes = HashMap::from([(
            key,
            (0..70)
                .map(|i| VLogIndex {
                    aggregate: None,
                    count: 3,
                    typ: INTEGER,
                    min: i * 3 + 1,
                    max: i * 3 + 3,
                    generation: 1,
                    seq: 1,
                    offset: 5 + i * 32,
                    size: 32,
                })
                .collect(),
        )]);
        let files = write_index_files(dir.path(), 1, &snapshot, &indexes).unwrap();
        assert_eq!(files.len(), 1);
        let file = IndexFile::from_meta(files.into_iter().next().unwrap()).unwrap();
        let entry = file.lookup(seq).unwrap().unwrap();
        let header = file.segment_header(&entry).unwrap();
        assert_eq!(header.level(1).unwrap().count, 70);
        let parents = file
            .read_level(&entry, &header, 2, i64::MIN, i64::MAX, &ALL_COLUMNS)
            .unwrap()
            .into_iter()
            .map(SelectedRecord::into_record)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            parents
                .iter()
                .map(|r| r.pos.child_count)
                .collect::<Vec<_>>(),
            [35, 35]
        );
        assert_eq!(
            parents.iter().map(|r| r.values.count).collect::<Vec<_>>(),
            [105, 105]
        );
        assert_eq!(
            parents[0].values.merge_ordered(parents[1].values).unwrap(),
            AggValues::from_block(&snapshot.values(b"cpu,host=A#!~#i")).unwrap()
        );
    }

    #[test]
    fn snapshot_splits_one_field_and_removes_all_fragments_on_late_failure() {
        let dir = tempfile::tempdir().unwrap();
        let cache = Cache::new(0);
        let seq = SeqNo::new(1, 1).unwrap();
        let key = "cpu#!~#value".to_string();
        let count = 300;
        cache
            .write_multi(HashMap::from([(
                key.clone(),
                (0..count)
                    .map(|t| Value::IntegerValue(IntegerValue::new(t, t)))
                    .collect(),
            )]))
            .unwrap();
        cache.set_sequence(key.as_bytes(), seq.get()).unwrap();
        let snapshot = cache.snapshot().unwrap();
        let mut indexes = HashMap::from([(
            key.clone(),
            (0..count)
                .map(|t| VLogIndex {
                    aggregate: None,
                    count: 1,
                    typ: INTEGER,
                    min: t,
                    max: t,
                    generation: 1,
                    seq: 0,
                    offset: 5 + t * 32,
                    size: 32,
                })
                .collect::<Vec<_>>(),
        )]);
        let files = write_index_files_with_target(
            dir.path(),
            OutputGenerations::isolated(1),
            &snapshot,
            &indexes,
            Default::default(),
            1,
        )
        .unwrap();
        assert_eq!(files.len(), count as usize);
        assert_eq!(files[256].generation, 1);
        assert_eq!(files[256].seq, 256);
        let mut times = Vec::new();
        for meta in &files {
            let file = IndexFile::from_meta(meta.clone()).unwrap();
            let entry = file.lookup(seq).unwrap().unwrap();
            let header = file.segment_header(&entry).unwrap();
            times.extend(
                file.read_level(&entry, &header, 1, i64::MIN, i64::MAX, &ALL_COLUMNS)
                    .unwrap()
                    .into_iter()
                    .map(|r| r.pos.start),
            );
        }
        assert_eq!(times, (0..count).collect::<Vec<_>>());
        indexes.get_mut(&key).unwrap().last_mut().unwrap().min = count - 2;
        assert!(
            write_index_files_with_target(
                dir.path(),
                OutputGenerations::isolated(3),
                &snapshot,
                &indexes,
                Default::default(),
                1
            )
            .is_err()
        );
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), files.len());
        assert!(files.iter().all(|f| f.path.exists()));
    }
}
