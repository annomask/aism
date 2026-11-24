use super::{AggValues, Record, SeqNo, store::IndexStore, writer::IndexFileWriter};
use crate::{
    models::{
        INTEGER,
        points::{IntegerValue, Value},
    },
    runtime::execution::legacy_global_executor,
    storage::tsm1::{
        data_encoding::encoding_values::Values,
        vlog::{VLogIndex, key_cursor::VLogKeyCursor, writer::VLogWriter},
    },
};
use std::fs::OpenOptions;

pub(super) fn install_blocks(
    store: &IndexStore,
    dir: &std::path::Path,
    generation: i64,
    start: i64,
    blocks: usize,
) {
    install_values(
        store,
        dir,
        generation,
        (0..blocks)
            .map(|block| {
                let min = start + block as i64 * 16;
                (min..=min + 15)
                    .map(|t| Value::IntegerValue(IntegerValue::new(t, generation)))
                    .collect()
            })
            .collect(),
    );
}

pub(super) fn install_values(
    store: &IndexStore,
    dir: &std::path::Path,
    generation: i64,
    blocks: Vec<Vec<Value>>,
) {
    install_values_with_padding(store, dir, generation, blocks, 0);
}

fn install_values_with_padding(
    store: &IndexStore,
    dir: &std::path::Path,
    generation: i64,
    blocks: Vec<Vec<Value>>,
    padding: usize,
) {
    install_values_with_identity(store, dir, generation, 1, blocks, padding);
}

pub(super) fn install_values_with_identity(
    store: &IndexStore,
    dir: &std::path::Path,
    generation: i64,
    file_sequence: i64,
    blocks: Vec<Vec<Value>>,
    padding: usize,
) {
    let path = dir.join(format!("{generation:09}-{file_sequence:09}.vlog.tmp"));
    let fd = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    let mut vlog = VLogWriter::new(fd, generation, file_sequence, &path);
    let seq = SeqNo::new(1, 1).unwrap();
    let typ = blocks[0][0].value_type();
    let mut builder = super::builder::TreeBuilder::new();
    let mut levels = std::collections::BTreeMap::<u8, Vec<Record>>::new();
    let mut emit = |record: Record| {
        levels.entry(record.level).or_default().push(record);
        Ok(())
    };
    for values in blocks {
        let min = values.first().unwrap().unix_nano();
        let max = values.last().unwrap().unix_nano();
        let encoded = Values::encode(&values).unwrap();
        let (offset, size) = vlog
            .write_block(b"cpu#!~#value", min, max, &encoded)
            .unwrap();
        builder
            .push(
                Record::from_vlog_index(
                    seq,
                    &VLogIndex {
                        aggregate: None,
                        count: values.len() as i32,
                        typ,
                        min,
                        max,
                        generation,
                        seq: file_sequence,
                        offset,
                        size,
                    },
                    AggValues::from_block(&values).unwrap(),
                )
                .unwrap(),
                &mut emit,
            )
            .unwrap();
    }
    builder.finish(&mut emit).unwrap();
    vlog.close().unwrap();
    if padding > 0 {
        use std::io::Write;
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&vec![0; padding]).unwrap();
        file.sync_all().unwrap();
    }
    let idx = dir.join(format!("{generation:09}-{file_sequence:09}.idx.tmp"));
    let mut writer = IndexFileWriter::create(&idx, generation, file_sequence).unwrap();
    writer
        .write_segment(seq, typ, &levels.into_iter().rev().collect::<Vec<_>>())
        .unwrap();
    store
        .install(&[writer.finish().unwrap()], &[(path, generation, file_sequence)], &[])
        .unwrap();
}

fn cursor(store: &IndexStore, ascending: bool, min: i64, max: i64) -> VLogKeyCursor {
    let stream = store
        .raw_cursor(SeqNo::new(1, 1).unwrap(), INTEGER, min, max, ascending)
        .unwrap();
    VLogKeyCursor::from_indexed_stream(stream, ascending, legacy_global_executor())
}

#[test]
fn raw_cursor_streams_connected_overlap_in_bounded_batches() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("vlog");
    let store = IndexStore::open(&dir, &root.path().join("wal")).unwrap();
    for generation in 1..=2 {
        install_blocks(&store, &dir, generation, (generation - 1) * 8, 1536);
    }
    let end = 1536 * 16 + 7;
    for ascending in [true, false] {
        let mut cursor = cursor(&store, ascending, 0, end);
        let mut actual = Vec::new();
        loop {
            let mut batch = cursor.read_integer_block().unwrap();
            if batch.is_empty() {
                break;
            }
            if actual.is_empty() {
                eprintln!(
                    "RAW_CURSOR_BUFFER first_batch_values={} input_blocks=3072 ascending={ascending}",
                    batch.len()
                );
            }
            assert!(
                batch.len() <= 4000,
                "connected overlap materialized {} values",
                batch.len()
            );
            if !ascending {
                batch.reverse();
            }
            actual.extend(batch.into_iter().map(|v| (v.unix_nano, v.value)));
            cursor.next();
        }
        cursor.close();
        let mut expected: Vec<_> = (0..=end).map(|t| (t, if t < 8 { 1 } else { 2 })).collect();
        if !ascending {
            expected.reverse();
        }
        assert_eq!(actual, expected);
    }
    for ascending in [true, false] {
        let mut stream = store
            .raw_cursor(SeqNo::new(1, 1).unwrap(), INTEGER, 0, end, ascending)
            .unwrap();
        assert_eq!(stream.buffer_metrics(), (0, 0, 0));
        assert_eq!(stream.read_batch().unwrap().len(), 4000);
        let metrics = stream.buffer_metrics();
        assert_eq!((metrics.0, metrics.1), (512, 32));
        assert!(metrics.2 < 3072);
        while !stream.read_batch().unwrap().is_empty() {
            stream.next();
        }
        assert_eq!(stream.buffer_metrics(), (512, 32, 3072));
        eprintln!(
            "RAW_CURSOR_BUFFER peak_leaf_records=512 peak_source_values=32 output_limit=4000 decoded_blocks=3072 ascending={ascending}"
        );
    }
}

fn integers(mut cursor: VLogKeyCursor, ascending: bool) -> Vec<(i64, i64)> {
    let mut result = Vec::new();
    loop {
        let mut values = cursor.read_integer_block().unwrap();
        if values.is_empty() {
            break;
        }
        if !ascending {
            values.reverse();
        }
        result.extend(values.into_iter().map(|v| (v.unix_nano, v.value)));
        cursor.next();
    }
    cursor.close();
    result
}

#[test]
fn raw_cursor_seeks_across_group_gaps_and_extreme_timestamps() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("vlog");
    let store = IndexStore::open(&dir, &root.path().join("wal")).unwrap();
    let times: Vec<i64> = std::iter::once(i64::MIN)
        .chain((0..600).map(|i| i * 4))
        .chain(std::iter::once(i64::MAX))
        .collect();
    install_values(
        &store,
        &dir,
        1,
        times
            .iter()
            .map(|t| vec![Value::IntegerValue(IntegerValue::new(*t, *t))])
            .collect(),
    );
    for (min, max) in [
        (i64::MIN, i64::MAX),
        (1017, 2087),
        (1017, 1019),
        (1017, 1020),
        (i64::MIN, i64::MIN),
        (i64::MAX, i64::MAX),
        (1, 0),
    ] {
        for ascending in [true, false] {
            let mut expected: Vec<_> = times
                .iter()
                .filter(|t| **t >= min && **t <= max)
                .map(|t| (*t, *t))
                .collect();
            if !ascending {
                expected.reverse();
            }
            assert_eq!(
                integers(cursor(&store, ascending, min, max), ascending),
                expected,
                "range={min}..{max} ascending={ascending}"
            );
        }
    }
}

#[test]
fn raw_cursor_pins_snapshot_and_preserves_versions_after_repair() {
    use super::delete::DeleteRange;
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("vlog");
    let wal = root.path().join("wal");
    let store = IndexStore::open(&dir, &wal).unwrap();
    install_blocks(&store, &dir, 1, 0, 600);
    install_blocks(&store, &dir, 2, 8, 600);
    let inputs = store.index_readers();
    install_blocks(&store, &dir, 3, 7990, 4);
    let old: Vec<_> = (0..=9607)
        .map(|t| {
            (
                t,
                if (7990..8054).contains(&t) {
                    3
                } else if t < 8 {
                    1
                } else {
                    2
                },
            )
        })
        .collect();
    let pinned = [
        cursor(&store, true, 0, 9607),
        cursor(&store, false, 0, 9607),
    ];
    let seq = SeqNo::new(1, 1).unwrap();
    store
        .delete_ranges(vec![
            DeleteRange {
                seq,
                min: 0,
                max: 8000,
                cutoff: 0,
            },
            DeleteRange {
                seq,
                min: 8500,
                max: 9607,
                cutoff: 0,
            },
        ])
        .unwrap();
    let expected: Vec<_> = old
        .iter()
        .copied()
        .filter(|(t, _)| (8001..8500).contains(t))
        .collect();
    for ascending in [true, false] {
        let want: Vec<_> = if ascending {
            expected.clone()
        } else {
            expected.iter().rev().copied().collect()
        };
        assert_eq!(
            integers(cursor(&store, ascending, 0, 9607), ascending),
            want
        );
        assert!(integers(cursor(&store, ascending, 0, 7989), ascending).is_empty());
    }
    assert!(store.compact_files(&inputs, 4, &|| false).is_err());
    let inputs = store
        .index_readers()
        .into_iter()
        .filter(|f| f.generation <= 2)
        .collect::<Vec<_>>();
    store.compact_files(&inputs, 4, &|| false).unwrap();
    store.checkpoint(true).unwrap();
    for (ascending, cursor) in [true, false].into_iter().zip(pinned) {
        let expected: Vec<_> = if ascending {
            old.clone()
        } else {
            old.iter().rev().copied().collect()
        };
        assert_eq!(integers(cursor, ascending), expected);
    }
    for ascending in [true, false] {
        let actual = integers(cursor(&store, ascending, 0, 9607), ascending);
        let want: Vec<_> = if ascending {
            expected.clone()
        } else {
            expected.iter().rev().copied().collect()
        };
        assert_eq!(actual, want);
    }
    // Materialized deletions and the unselected file's tombstone both survive reopening.
    let reopened = IndexStore::open(&dir, &wal).unwrap();
    assert_eq!(integers(cursor(&reopened, true, 0, 9607), true), expected);
}

#[test]
fn raw_cursor_reports_late_corruption_and_stays_failed() {
    use std::io::{Seek, SeekFrom, Write};
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("vlog");
    let store = IndexStore::open(&dir, &root.path().join("wal")).unwrap();
    install_blocks(&store, &dir, 1, 0, 600);
    let seq = SeqNo::new(1, 1).unwrap();
    let mut stream = store.raw_cursor(seq, INTEGER, 0, 9599, true).unwrap();
    let blocks = store.block_locations(seq, INTEGER, 4096, 4111).unwrap();
    let mut fd = OpenOptions::new()
        .write(true)
        .open(store.readers()[0].path())
        .unwrap();
    fd.seek(SeekFrom::Start(blocks[0].offset as u64)).unwrap();
    fd.write_all(&[0; 4]).unwrap();
    fd.sync_all().unwrap();
    let first = stream.read_batch().unwrap().to_vec();
    assert_eq!(first.len(), 4000);
    assert_eq!(stream.read_batch().unwrap(), first);
    stream.next();
    assert!(stream.read_batch().is_err());
    stream.next();
    assert!(stream.read_batch().is_err());
    assert!(
        store
            .aggregate(
                seq,
                INTEGER,
                crate::query::storage_ir::TimeRange::new(4097, 4110),
                Vec::new(),
                true
            )
            .is_err()
    );
    install_blocks(&store, &dir, 2, 4100, 1);
    let inputs = store.index_readers();
    assert!(store.compact_files(&inputs, 3, &|| false).is_err());
    assert_eq!(
        store
            .index_readers()
            .iter()
            .map(|f| f.file_no)
            .collect::<Vec<_>>(),
        inputs.iter().map(|f| f.file_no).collect::<Vec<_>>()
    );
    assert!(!std::fs::read_dir(&dir).unwrap().any(|entry| {
        entry
            .unwrap()
            .path()
            .extension()
            .is_some_and(|e| e == "tmp")
    }));
}

#[test]
fn repair_budget_streams_large_conflicts_and_cleans_cancelled_outputs() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("vlog");
    let store = IndexStore::open(&dir, &root.path().join("wal")).unwrap();
    install_blocks(&store, &dir, 1, 0, 600);
    install_blocks(&store, &dir, 2, 8, 600);
    let inputs = store.index_readers();
    install_blocks(&store, &dir, 3, 4000, 1);
    let calls = AtomicUsize::new(0);
    assert!(matches!(
        store.compact_files(&inputs, 4, &|| calls.fetch_add(1, Ordering::SeqCst) > 750),
        Err(super::Error::CompactionCancelled)
    ));
    assert!(!std::fs::read_dir(&dir).unwrap().any(|entry| {
        entry
            .unwrap()
            .path()
            .extension()
            .is_some_and(|e| e == "tmp")
    }));
    let stats = store.compact_files(&inputs, 5, &|| false).unwrap();
    assert_eq!(stats.streamed_rebuild_fields, 1);
    assert_eq!(stats.repaired_blocks, 1200);
    let expected: Vec<_> = (0..=9607)
        .map(|t| {
            (
                t,
                if (4000..4016).contains(&t) {
                    3
                } else if t < 8 {
                    1
                } else {
                    2
                },
            )
        })
        .collect();
    assert_eq!(integers(cursor(&store, true, 0, 9607), true), expected);
}

#[test]
fn extended_output_sequences_survive_gc_deletes_and_restart() {
    use super::allocation::OutputGenerations;
    use crate::query::storage_ir::{AggregateValues, TimeRange};
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("vlog");
    let wal = root.path().join("wal");
    let store = IndexStore::open(&dir, &wal).unwrap();
    install_values_with_padding(
        &store,
        &dir,
        1,
        (0..300)
            .map(|block| {
                (block * 16..block * 16 + 16)
                    .map(|t| Value::IntegerValue(IntegerValue::new(t, 1)))
                    .collect()
            })
            .collect(),
        128 * 1024,
    );
    store
        .compact_files_controlled(
            &store.index_readers(),
            OutputGenerations::isolated(2),
            &|| false,
            1,
            Default::default(),
        )
        .unwrap();
    let files = store.index_readers();
    assert_eq!(files.len(), 300);
    assert!(files.iter().all(|f| f.generation == 2));
    assert_eq!(
        files
            .iter()
            .map(|f| f.file_no)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        300
    );
    assert_eq!(files.iter().map(|f| f.seq).max(), Some(301));
    assert_eq!(store.next_generation(), 4);
    assert!(super::planner::select_inputs(&store).unwrap().is_empty());
    drop(files);
    drop(store);
    let store = IndexStore::open(&dir, &wal).unwrap();
    assert_eq!(store.next_generation(), 4);
    let pinned = cursor(&store, true, 0, 4799);
    let stats = store
        .collect_garbage(
            OutputGenerations::isolated(4),
            u64::MAX,
            Default::default(),
            &|| false,
        )
        .unwrap();
    assert_eq!(stats.copied_blocks, 300);
    assert_eq!(store.index_readers().len(), 300);
    assert!(store.index_readers().iter().all(|f| f.generation == 4));
    assert_eq!(store.next_generation(), 6);
    assert_eq!(
        integers(pinned, true),
        (0..4800).map(|t| (t, 1)).collect::<Vec<_>>()
    );
    let seq = SeqNo::new(1, 1).unwrap();
    store
        .delete_ranges(vec![super::delete::DeleteRange {
            seq,
            min: 4063,
            max: 4080,
            cutoff: 0,
        }])
        .unwrap();
    assert!(dir.join("000000004-000000256.idx.tombstone").exists());
    install_blocks(&store, &dir, 6, 4070, 1);
    let expected: Vec<_> = (0..4800)
        .filter_map(|t| {
            if (4070..4086).contains(&t) {
                Some((t, 6))
            } else if (4063..=4080).contains(&t) {
                None
            } else {
                Some((t, 1))
            }
        })
        .collect();
    store.checkpoint(true).unwrap();
    drop(store);
    let store = IndexStore::open(&dir, &wal).unwrap();
    assert_eq!(store.next_generation(), 7);
    for ascending in [true, false] {
        let want = if ascending {
            expected.clone()
        } else {
            expected.iter().rev().copied().collect()
        };
        assert_eq!(
            integers(cursor(&store, ascending, 0, 4799), ascending),
            want
        );
    }
    for indexed in [false, true] {
        let result = store
            .aggregate(seq, INTEGER, TimeRange::all(), vec![], indexed)
            .unwrap();
        assert_eq!(result.state.count, expected.len() as u64);
        assert_eq!(
            result.state.values,
            Some(AggregateValues::Integer {
                sum: expected.iter().map(|(_, v)| *v).sum(),
                min: 1,
                max: 6,
            })
        );
    }
}

#[test]
fn split_repair_outputs_preserve_versions_deletes_queries_and_recovery() {
    use crate::query::storage_ir::{
        AggregateKind, AggregateNumber, AggregateProjection, TimeRange,
    };
    for blocks in [8, 600] {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("vlog");
        let wal = root.path().join("wal");
        let store = IndexStore::open(&dir, &wal).unwrap();
        install_blocks(&store, &dir, 1, 0, blocks);
        install_blocks(&store, &dir, 2, 8, blocks);
        let seq = SeqNo::new(1, 1).unwrap();
        let max = blocks as i64 * 16 + 7;
        let pinned = cursor(&store, true, 0, max);
        let inputs = store.index_readers();
        let stats = store
            .compact_files_controlled(
                &inputs,
                super::allocation::OutputGenerations::isolated(3),
                &|| false,
                1,
                Default::default(),
            )
            .unwrap();
        assert_eq!(stats.streamed_rebuild_fields, usize::from(blocks == 600));
        assert!(store.index_readers().len() > 1);
        let original: Vec<_> = (0..=max).map(|t| (t, if t < 8 { 1 } else { 2 })).collect();
        assert_eq!(integers(pinned, true), original);
        assert_eq!(integers(cursor(&store, true, 0, max), true), original);
        let projection =
            AggregateProjection::from_aggregates(&[AggregateKind::Count, AggregateKind::Sum]);
        let result = store
            .aggregate_projected(seq, INTEGER, TimeRange::all(), vec![], projection)
            .unwrap();
        assert_eq!(result.state.count, Some((max + 1) as u64));
        assert_eq!(
            result.state.sum,
            Some(AggregateNumber::Integer(
                original.iter().map(|(_, v)| *v).sum()
            ))
        );
        store
            .delete_ranges(vec![super::delete::DeleteRange {
                seq,
                min: 4,
                max: 20,
                cutoff: 0,
            }])
            .unwrap();
        // A later write must survive the tombstones on every split output.
        install_blocks(&store, &dir, 4, 10, 1);
        let expected: Vec<_> = (0..=max)
            .filter_map(|t| {
                if (10..26).contains(&t) {
                    Some((t, 4))
                } else if (4..=20).contains(&t) {
                    None
                } else {
                    Some((t, if t < 8 { 1 } else { 2 }))
                }
            })
            .collect();
        store.checkpoint(true).unwrap();
        drop(store);
        let store = IndexStore::open(&dir, &wal).unwrap();
        for ascending in [true, false] {
            let want = if ascending {
                expected.clone()
            } else {
                expected.iter().rev().copied().collect()
            };
            assert_eq!(integers(cursor(&store, ascending, 0, max), ascending), want);
        }
        let result = store
            .aggregate_projected(seq, INTEGER, TimeRange::all(), vec![], projection)
            .unwrap();
        assert_eq!(result.state.count, Some(expected.len() as u64));
        assert_eq!(
            result.state.sum,
            Some(AggregateNumber::Integer(
                expected.iter().map(|(_, v)| *v).sum()
            ))
        );
        let raw = store
            .aggregate(seq, INTEGER, TimeRange::all(), vec![], false)
            .unwrap();
        let indexed = store
            .aggregate(seq, INTEGER, TimeRange::all(), vec![], true)
            .unwrap();
        assert_eq!(raw.state.count, indexed.state.count);
        assert_eq!(raw.state.values, indexed.state.values);
        assert_eq!(
            indexed.state.first.as_ref().map(|p| p.time),
            expected.first().map(|p| p.0)
        );
        assert_eq!(
            indexed.state.last.as_ref().map(|p| p.time),
            expected.last().map(|p| p.0)
        );
        store
            .delete_ranges(vec![super::delete::DeleteRange {
                seq,
                min: 0,
                max,
                cutoff: 0,
            }])
            .unwrap();
        store
            .compact_files_controlled(
                &store.index_readers(),
                super::allocation::OutputGenerations::isolated(5),
                &|| false,
                1,
                Default::default(),
            )
            .unwrap();
        assert_eq!(
            store
                .aggregate(seq, INTEGER, TimeRange::all(), vec![], true)
                .unwrap()
                .state
                .count,
            0
        );
        drop(store);
        let store = IndexStore::open(&dir, &wal).unwrap();
        assert!(integers(cursor(&store, true, 0, max), true).is_empty());
    }
}

#[test]
fn raw_cursor_all_types_merge_across_output_boundaries() {
    use crate::models::{
        BOOL, FLOAT, STRING, UNSIGNED,
        points::{BoolValue, FloatValue, StringValue, UnsignedValue},
    };
    for typ in [INTEGER, FLOAT, UNSIGNED, BOOL, STRING] {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("vlog");
        let store = IndexStore::open(&dir, &root.path().join("wal")).unwrap();
        let value = |time, generation| match typ {
            INTEGER => Value::IntegerValue(IntegerValue::new(time, generation)),
            FLOAT => Value::FloatValue(FloatValue::new(time, generation as f64 + 0.5)),
            UNSIGNED => Value::UnsignedValue(UnsignedValue::new(time, generation as u64)),
            BOOL => Value::BoolValue(BoolValue::new(time, generation == 2)),
            STRING => Value::StringValue(StringValue::new(time, format!("version-{generation}"))),
            _ => unreachable!(),
        };
        for generation in 1..=2 {
            install_values(
                &store,
                &dir,
                generation,
                (0..520)
                    .map(|block| {
                        let start = block * 16 + (generation - 1) * 8;
                        (start..start + 16).map(|t| value(t, generation)).collect()
                    })
                    .collect(),
            );
        }
        for ascending in [true, false] {
            let stream = store
                .raw_cursor(SeqNo::new(1, 1).unwrap(), typ, 3, 8323, ascending)
                .unwrap();
            let mut cursor =
                VLogKeyCursor::from_indexed_stream(stream, ascending, legacy_global_executor());
            let mut actual = Vec::new();
            loop {
                let mut batch: Vec<Value> = match typ {
                    INTEGER => cursor
                        .read_integer_block()
                        .unwrap()
                        .into_iter()
                        .map(Value::IntegerValue)
                        .collect(),
                    FLOAT => cursor
                        .read_float_block()
                        .unwrap()
                        .into_iter()
                        .map(Value::FloatValue)
                        .collect(),
                    UNSIGNED => cursor
                        .read_unsigned_block()
                        .unwrap()
                        .into_iter()
                        .map(Value::UnsignedValue)
                        .collect(),
                    BOOL => cursor
                        .read_bool_block()
                        .unwrap()
                        .into_iter()
                        .map(Value::BoolValue)
                        .collect(),
                    STRING => cursor
                        .read_string_block()
                        .unwrap()
                        .into_iter()
                        .map(Value::StringValue)
                        .collect(),
                    _ => unreachable!(),
                };
                if batch.is_empty() {
                    break;
                }
                assert!(batch.len() <= 4000);
                if !ascending {
                    batch.reverse();
                }
                actual.extend(batch);
                cursor.next();
            }
            cursor.close();
            let mut expected: Vec<_> = (3..=8323)
                .map(|t| value(t, if t < 8 { 1 } else { 2 }))
                .collect();
            if !ascending {
                expected.reverse();
            }
            assert_eq!(actual, expected, "type={typ} ascending={ascending}");
        }
    }
}
