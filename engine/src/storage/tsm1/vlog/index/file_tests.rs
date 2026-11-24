use super::super::page_group::{ColumnValue, encode_group};
use super::super::segment::LevelHeader;
use super::super::writer::IndexFileWriter;
use super::super::{AggScalar, AggSum, AggValues, LEVEL_BLOCK, LEVEL_DIRTY, Position};
use super::*;
use crate::models::{
    BOOL, FLOAT, FieldType, INTEGER, STRING, UNSIGNED,
    points::{BoolValue, FloatValue, IntegerValue, StringValue, UnsignedValue, Value},
};
use crate::storage::tsm1::vlog::index::BlockLoc;
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::{
    fs,
    io::{Seek, SeekFrom, Write},
};
use tempfile::tempdir;

fn record(seq_no: SeqNo, typ: FieldType, time: i64, seed: u64) -> Record {
    let value = match typ {
        INTEGER => Value::IntegerValue(IntegerValue {
            unix_nano: time,
            value: seed as i64,
        }),
        UNSIGNED => Value::UnsignedValue(UnsignedValue {
            unix_nano: time,
            value: seed,
        }),
        FLOAT => Value::FloatValue(FloatValue {
            unix_nano: time,
            value: (seed as i64) as f64 / 37.0,
        }),
        BOOL => Value::BoolValue(BoolValue {
            unix_nano: time,
            value: seed % 2 == 0,
        }),
        STRING => Value::StringValue(StringValue {
            unix_nano: time,
            value: format!("value-{seed}"),
        }),
        _ => unreachable!(),
    };
    Record {
        seq_no,
        level: LEVEL_BLOCK,
        pos: Position {
            start: time,
            end: time + 3,
            block: BlockLoc::from_vlog(41, 1, 5 + (seed % 100000) as i64).unwrap(),
            block_size: 32 + (seed % 1000) as u32,
            flags: 0,
            child_count: 0,
            version: 0,
        },
        values: AggValues::from_block(&[value]).unwrap(),
    }
}

#[test]
fn cached_index_bytes_are_shared_and_survive_eviction_during_decode() {
    let dir = tempdir().unwrap();
    let seq = SeqNo::new(1, 1).unwrap();
    let mut writer = IndexFileWriter::create(&dir.path().join("cached.idx"), 1, 1).unwrap();
    writer.begin_segment(seq, INTEGER).unwrap();
    writer.push(&record(seq, INTEGER, 0, 1)).unwrap();
    writer.end_segment().unwrap();
    let file = IndexFile::from_meta(writer.finish().unwrap()).unwrap();
    let cache = super::super::caches::ReadCache::new(4096);
    file.set_cache(Arc::clone(&cache));
    let original = file.read(0, HEADER_SIZE).unwrap().into_vec();
    let first = file.read(0, HEADER_SIZE).unwrap();
    let second = file.read(0, HEADER_SIZE).unwrap();
    match (&first, &second) {
        (ReadBytes::Shared(first), ReadBytes::Shared(second)) => {
            assert!(Arc::ptr_eq(first, second))
        }
        _ => panic!("cache hits must share bytes"),
    }
    cache.set_budget(0);
    assert_eq!(cache.stats().bytes, 0);
    assert_eq!(first.as_slice(), original.as_slice());
    assert_eq!(second.as_slice(), original.as_slice());
    assert!(matches!(
        file.read(0, HEADER_SIZE).unwrap(),
        ReadBytes::Owned(_)
    ));
}
fn assert_bits_equal(expected: &[Record], actual: &[Record]) {
    assert_eq!(expected.len(), actual.len());
    for (a, b) in expected.iter().zip(actual) {
        assert_eq!((a.seq_no, a.level, a.pos), (b.seq_no, b.level, b.pos));
        assert_eq!(a.values.count, b.values.count);
        assert_eq!(a.values.sumsq.to_bits(), b.values.sumsq.to_bits());
        assert_eq!(
            (a.values.min_at, a.values.max_at),
            (b.values.min_at, b.values.max_at)
        );
        match (a.values.sum, b.values.sum) {
            (AggSum::Float(x), AggSum::Float(y)) => assert_eq!(x.to_bits(), y.to_bits()),
            (x, y) => assert_eq!(x, y),
        }
        for (x, y) in [
            (a.values.min, b.values.min),
            (a.values.max, b.values.max),
            (a.values.first, b.values.first),
            (a.values.last, b.values.last),
        ] {
            match (x, y) {
                (AggScalar::Float(x), AggScalar::Float(y)) => assert_eq!(x.to_bits(), y.to_bits()),
                (x, y) => assert_eq!(x, y),
            }
        }
    }
}

#[test]
fn compaction_coalesces_adjacent_small_groups_without_realigning_full_groups() {
    use super::super::compact::{SegmentSource, merge_ordered_segments};
    let dir = tempdir().unwrap();
    let seq = SeqNo::new(1, 1).unwrap();
    let leaves: Vec<_> = (0..320)
        .map(|i| record(seq, INTEGER, i * 4, i as u64))
        .collect();
    let mut builder = super::super::builder::TreeBuilder::new();
    let mut levels = std::collections::BTreeMap::<u8, Vec<Record>>::new();
    let mut emit = |r: Record| {
        levels.entry(r.level).or_default().push(r);
        Ok(())
    };
    for r in &leaves {
        builder.push(*r, &mut emit).unwrap();
    }
    builder.finish(&mut emit).unwrap();
    let mut writer = IndexFileWriter::create(&dir.path().join("source.idx"), 1, 1).unwrap();
    writer.begin_segment(seq, INTEGER).unwrap();
    for (level, records) in levels.iter().rev() {
        if *level != 1 {
            for r in records {
                writer.push(r).unwrap();
            }
            continue;
        }
        for range in [0..32, 32..64, 64..320] {
            let (entry, bytes) = encode_group(INTEGER, &records[range], 0).unwrap();
            writer
                .append_group(super::super::page_group::EncodedPageGroup {
                    seq_no: seq,
                    level: 1,
                    typ: INTEGER,
                    entry,
                    bytes,
                })
                .unwrap();
        }
    }
    writer.end_segment().unwrap();
    let source = IndexFile::from_meta(writer.finish().unwrap()).unwrap();
    let entry = source.lookup(seq).unwrap().unwrap();
    let header = source.segment_header(&entry).unwrap();
    let original_full = source
        .groups(&entry, &header, 1)
        .unwrap()
        .read(2)
        .unwrap()
        .bytes;
    let mut writer = IndexFileWriter::create(&dir.path().join("merged.idx"), 2, 1).unwrap();
    let stats = merge_ordered_segments(
        &mut writer,
        &[SegmentSource {
            file: &source,
            entry,
            header,
        }],
    )
    .unwrap();
    assert_eq!(stats.coalesced_groups, 2);
    let merged = IndexFile::from_meta(writer.finish().unwrap()).unwrap();
    let entry = merged.lookup(seq).unwrap().unwrap();
    let header = merged.segment_header(&entry).unwrap();
    let mut groups = merged.groups(&entry, &header, 1).unwrap();
    assert_eq!(groups.len(), 2);
    assert_eq!(groups.entry(0).unwrap().row_count, 64);
    assert_eq!(groups.read(1).unwrap().bytes, original_full);
    let actual = merged
        .read_level(&entry, &header, 1, i64::MIN, i64::MAX, &ALL_COLUMNS)
        .unwrap()
        .into_iter()
        .map(|r| r.into_record().unwrap())
        .collect::<Vec<_>>();
    assert_bits_equal(&leaves, &actual);
}

#[test]
fn random_files_round_trip_all_types_and_projected_ranges() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("41-1.idx");
    let mut writer = IndexFileWriter::create(&path, 41, 1).unwrap();
    let mut rng = StdRng::seed_from_u64(20260927);
    let mut datasets = Vec::new();
    for (series, typ) in [INTEGER, UNSIGNED, FLOAT, BOOL, STRING]
        .into_iter()
        .enumerate()
    {
        let seq = SeqNo::new(series as u64 + 12, 1).unwrap();
        let records = (0..777)
            .map(|i| record(seq, typ, i * 10 - 4000, rng.r#gen()))
            .collect::<Vec<_>>();
        writer
            .write_segment(seq, typ, &[(1, records.clone())])
            .unwrap();
        datasets.push(records);
    }
    let file = IndexFile::from_meta(writer.finish().unwrap()).unwrap();
    for records in datasets {
        let entry = file.lookup(records[0].seq_no).unwrap().unwrap();
        let header = file.segment_header(&entry).unwrap();
        let all = file
            .read_level(&entry, &header, 1, i64::MIN, i64::MAX, &ALL_COLUMNS)
            .unwrap()
            .into_iter()
            .map(SelectedRecord::into_record)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_bits_equal(&records, &all);
        for _ in 0..60 {
            let a = rng.gen_range(-5000..5000);
            let b = rng.gen_range(a..6000);
            let expected = records
                .iter()
                .filter(|r| r.pos.start <= b && r.pos.end >= a)
                .collect::<Vec<_>>();
            let selected = file
                .read_level(&entry, &header, 1, a, b, &[Column::Count, Column::Sum])
                .unwrap();
            assert_eq!(selected.len(), expected.len());
            for (actual, expected) in selected.into_iter().zip(expected) {
                assert_eq!(actual.pos, expected.pos);
                assert_eq!(
                    actual.values[0],
                    Some(ColumnValue::Count(expected.values.count))
                );
                assert_eq!(
                    actual.values[1],
                    Some(ColumnValue::Sum(expected.values.sum))
                );
                assert!(actual.values[2..].iter().all(Option::is_none));
                assert!(actual.into_record().is_err());
            }
        }
        let tail = file
            .read_level_tail(&entry, &header, 1, records[512].pos.start)
            .unwrap();
        assert_bits_equal(&records[513..], &tail);
        assert!(
            file.read_level_tail(&entry, &header, 1, i64::MAX)
                .unwrap()
                .is_empty()
        );
    }
    assert!(file.lookup(SeqNo::new(1, 1).unwrap()).unwrap().is_none());
    assert!(file.lookup(SeqNo::new(12, 2).unwrap()).unwrap().is_none());
}

#[test]
fn batch_and_streaming_writers_preserve_multilevel_records() {
    let dir = tempdir().unwrap();
    let seq = SeqNo::new(12, 1).unwrap();
    let blocks = (0..4100)
        .map(|i| record(seq, INTEGER, i * 10, i as u64))
        .collect::<Vec<_>>();
    let second = blocks[..4096]
        .chunks(64)
        .map(|c| Record::summarize(c).unwrap())
        .collect::<Vec<_>>();
    let third = Record::summarize(&second).unwrap();
    let levels = vec![
        (3, vec![third]),
        (2, second[62..].to_vec()),
        (1, blocks[4000..].to_vec()),
    ];
    for stream in [false, true] {
        let path = dir.path().join(format!("{stream}.idx"));
        let mut writer = IndexFileWriter::create(&path, 41, 1).unwrap();
        if stream {
            writer.begin_segment(seq, INTEGER).unwrap();
            for (_, records) in &levels {
                for r in records {
                    writer.push(r).unwrap();
                }
            }
            writer.end_segment().unwrap();
        } else {
            writer.write_segment(seq, INTEGER, &levels).unwrap();
        }
        let other = record(SeqNo::new(13, 1).unwrap(), INTEGER, 10, 7);
        writer
            .write_segment(other.seq_no, INTEGER, &[(1, vec![other])])
            .unwrap();
        let file = IndexFile::from_meta(writer.finish().unwrap()).unwrap();
        let entry = file.lookup(seq).unwrap().unwrap();
        let h = file.segment_header(&entry).unwrap();
        assert_eq!(entry.seg_offset, 32);
        assert_eq!(h.size(), 104);
        assert_eq!(h.level_bits, 7);
        assert_eq!(h.level(1).unwrap().count, 100);
        assert_eq!(h.level(2).unwrap().count, 2);
        assert_eq!(h.level(3).unwrap().count, 1);
        assert_eq!(
            h.level(3).unwrap().level_offset,
            if stream { 200 } else { 104 }
        );
        assert_eq!(
            file.lookup(other.seq_no).unwrap().unwrap().seg_offset,
            entry.seg_offset + u64::from(entry.seg_len)
        );
        for (level, expected) in &levels {
            let actual = file
                .read_level(&entry, &h, *level, i64::MIN, i64::MAX, &ALL_COLUMNS)
                .unwrap()
                .into_iter()
                .map(SelectedRecord::into_record)
                .collect::<Result<Vec<_>>>()
                .unwrap();
            assert_bits_equal(expected, &actual);
        }
        let bytes = fs::read(&path).unwrap();
        assert_eq!(bytes.len() as u64, file.footer.top_index_offset + 8 + 80);
    }
}

#[test]
fn version_two_offsets_and_sum_projection_are_exact() {
    // The design's encoded lengths are illustrative; this fixture locks down
    // their address arithmetic independently of actual compression ratios.
    let header = SegmentHeader {
        typ: INTEGER,
        flags: 0,
        level_bits: 7,
        levels: vec![
            (
                3,
                LevelHeader {
                    count: 1,
                    level_offset: 104,
                    group_count: 1,
                    table_offset: 142,
                    min_start: 0,
                    max_end: 4095,
                },
            ),
            (
                2,
                LevelHeader {
                    count: 2,
                    level_offset: 310,
                    group_count: 1,
                    table_offset: 182,
                    min_start: 3968,
                    max_end: 4095,
                },
            ),
            (
                1,
                LevelHeader {
                    count: 100,
                    level_offset: 556,
                    group_count: 1,
                    table_offset: 2478,
                    min_start: 4000,
                    max_end: 4099,
                },
            ),
        ],
    };
    let mut bytes = Vec::new();
    header.encode(&mut bytes);
    assert_eq!(bytes.len(), 104);
    assert_eq!(SegmentHeader::decode(&bytes, 3098).unwrap(), header);
    let group = PageGroupEntry {
        first_start: 4000,
        last_end: 4099,
        group_offset: 0,
        first_row: 0,
        row_count: 100,
        position_len: 462,
        column_lens: [16, 400, 320, 320, 112, 320, 112, 208, 208],
        payload_crc: 0,
        column_checksums: false,
    };
    assert_eq!(
        group
            .read_range(32 + 556, &[Column::Count, Column::Sum])
            .unwrap(),
        (588, 878)
    );
    assert_eq!(
        588 + group.byte_len(&ALL_COLUMNS) + PageGroupEntry::SIZE,
        32 + 3098
    );
    let entry = DirEntry {
        seq_no: SeqNo::from_raw(3073).unwrap(),
        seg_offset: 32,
        seg_len: 3098,
        typ: INTEGER,
        top_level: 3,
        level_bits: 7,
        min_start: 0,
        max_end: 4099,
    };
    let mut bytes = Vec::new();
    entry.encode(&mut bytes);
    assert_eq!(bytes.len(), 40);
    assert_eq!(DirEntry::decode(&bytes, 3216).unwrap(), entry);
    let footer = Footer {
        index_offset: 32,
        page_index_offset: 3216,
        dir_offset: 3224,
        top_index_offset: 3304,
        min_seq_no: 3073,
        max_seq_no: 3329,
        series_count: 2,
        flags: 0,
    };
    let top = TopIndex(vec![3073]).encode();
    let bytes = footer.encode(&top);
    assert_eq!(Footer::decode(&bytes, 3392).unwrap(), footer);
    Footer::verify_checksum(&bytes, &top).unwrap();
}

#[test]
fn ieee_extremes_wide_sums_and_full_timestamp_span_round_trip() {
    let seq = SeqNo::new(1, 1).unwrap();
    let cases = [
        0.0,
        -0.0,
        f64::MAX,
        -f64::MAX,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::from_bits(0x7ff8_0000_0000_0023),
    ];
    let records = cases
        .iter()
        .enumerate()
        .map(|(i, &v)| {
            let mut r = record(seq, FLOAT, i as i64 * 10, i as u64);
            r.values.sum = AggSum::Float(v);
            r.values.sumsq = v;
            r.values.min = AggScalar::Float(v);
            r.values.max = AggScalar::Float(v);
            r.values.first = AggScalar::Float(v);
            r.values.last = AggScalar::Float(v);
            r
        })
        .collect::<Vec<_>>();
    let (entry, bytes) = encode_group(FLOAT, &records, 0).unwrap();
    let decoded = decode_group(seq, 1, FLOAT, &bytes, &entry, records.len(), &ALL_COLUMNS)
        .unwrap()
        .into_iter()
        .map(SelectedRecord::into_record)
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_bits_equal(&records, &decoded);
    for typ in [INTEGER, UNSIGNED] {
        let mut r = record(seq, typ, 0, 1);
        r.pos.start = i64::MIN;
        r.pos.end = i64::MAX;
        r.values.sum = if typ == INTEGER {
            AggSum::Int(i128::MIN + 17)
        } else {
            AggSum::Uint(u128::MAX - 17)
        };
        let (entry, bytes) = encode_group(typ, &[r], 0).unwrap();
        let actual = decode_group(seq, 1, typ, &bytes, &entry, 1, &ALL_COLUMNS)
            .unwrap()
            .remove(0)
            .into_record()
            .unwrap();
        assert_eq!(r, actual);
    }
}

#[test]
fn three_tier_directory_crosses_both_page_boundaries() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("many.idx");
    let mut writer = IndexFileWriter::create(&path, 41, 1).unwrap();
    let count = DIR_PER_PAGE * (INDEX_PER_PAGE + 1) + 3;
    for i in 1..=count {
        let r = record(SeqNo::new(i as u64 * 2, 1).unwrap(), INTEGER, 0, 1);
        writer
            .write_segment(r.seq_no, INTEGER, &[(1, vec![r])])
            .unwrap();
    }
    let file = IndexFile::from_meta(writer.finish().unwrap()).unwrap();
    assert_eq!(file.top_index.0.len(), 2);
    for i in [
        1,
        102,
        103,
        DIR_PER_PAGE * INDEX_PER_PAGE,
        DIR_PER_PAGE * INDEX_PER_PAGE + 1,
        count,
    ] {
        let seq = SeqNo::new(i as u64 * 2, 1).unwrap();
        assert_eq!(file.lookup(seq).unwrap().unwrap().seq_no, seq);
        assert!(
            file.lookup(SeqNo::new(i as u64 * 2 + 1, 1).unwrap())
                .unwrap()
                .is_none()
        );
    }
}

#[test]
fn large_group_tables_use_paged_search_with_the_same_predecessors() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("table");
    let entries = (0..GROUPS_PER_PAGE * 17 + 3)
        .map(|i| PageGroupEntry {
            first_start: i as i64 * 100 - 20000,
            last_end: i as i64 * 100 - 19999,
            group_offset: i as u32 * 19,
            first_row: i as u32,
            row_count: 1,
            position_len: 10,
            column_lens: [1; 9],
            column_checksums: false,
            payload_crc: 0,
        })
        .collect::<Vec<_>>();
    let mut bytes = Vec::new();
    for entry in &entries {
        entry.encode(&mut bytes);
    }
    fs::write(&path, &bytes).unwrap();
    let file = IndexFile {
        format: IndexFormat::CURRENT,
        tombstone: None,
        cache: parking_lot::RwLock::new(None),
        generation: 1,
        seq: 1,
        file_no: 257,
        path: path.clone(),
        fd: File::open(path).unwrap(),
        len: bytes.len() as u64,
        footer: Footer {
            index_offset: 32,
            page_index_offset: 32,
            dir_offset: 32,
            top_index_offset: 32,
            min_seq_no: 257,
            max_seq_no: 257,
            series_count: 1,
            flags: 0,
        },
        top_index: TopIndex(vec![257]),
    };
    let mut table = GroupTable::new(&file, 0, entries.len(), entries.len() as u64 * 19, 1, None).unwrap();
    assert!(table.whole.is_none());
    for time in [-21000, -20000, -19999, 0, 204700, 300000]
        .into_iter()
        .chain(
            (0..entries.len())
                .step_by(13)
                .map(|i| entries[i].first_start + 1),
        )
    {
        assert_eq!(
            table.predecessor(time).unwrap(),
            entries
                .partition_point(|e| e.first_start <= time)
                .saturating_sub(1)
        );
    }
}

#[test]
fn corruption_and_invalid_writer_states_return_errors() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("ok.idx");
    let r = record(SeqNo::new(1, 1).unwrap(), INTEGER, 0, 1);
    let mut writer = IndexFileWriter::create(&path, 41, 1).unwrap();
    writer
        .write_segment(r.seq_no, INTEGER, &[(1, vec![r])])
        .unwrap();
    assert!(
        writer
            .write_segment(r.seq_no, INTEGER, &[(1, vec![r])])
            .is_err()
    );
    writer.finish().unwrap();
    assert!(IndexFile::open(&path, 42, 1).is_err());
    let original = fs::read(&path).unwrap();
    for (case, index) in [
        0,
        4,
        8,
        original.len() - 1,
        original.len() - 8,
        original.len() - 81,
    ]
    .into_iter()
    .enumerate()
    {
        let corrupt = dir.path().join(format!("corrupt-{case}"));
        let mut bytes = original.clone();
        bytes[index] ^= 0x40;
        fs::write(&corrupt, bytes).unwrap();
        assert!(IndexFile::open(&corrupt, 41, 1).is_err());
    }
    for len in [0, 31, 80, original.len() - 1] {
        let truncated = dir.path().join(format!("short-{len}"));
        fs::write(&truncated, &original[..len]).unwrap();
        assert!(IndexFile::open(&truncated, 41, 1).is_err());
    }
    let incomplete = dir.path().join("incomplete.idx");
    let mut writer = IndexFileWriter::create(&incomplete, 41, 1).unwrap();
    writer.begin_segment(r.seq_no, INTEGER).unwrap();
    writer.push(&r).unwrap();
    assert!(writer.push(&r).is_err());
    assert!(writer.finish().is_err());
    let (entry, bytes) = encode_group(INTEGER, &[r], 0).unwrap();
    for len in 0..bytes.len() {
        assert!(
            decode_group(r.seq_no, 1, INTEGER, &bytes[..len], &entry, 1, &ALL_COLUMNS).is_err()
        );
    }
    let mut rng = StdRng::seed_from_u64(9);
    for _ in 0..200 {
        let mut corrupt = bytes.clone();
        let i = rng.gen_range(0..corrupt.len());
        corrupt[i] ^= rng.gen_range(1..=255);
        let _ = decode_group(r.seq_no, 1, INTEGER, &corrupt, &entry, 1, &ALL_COLUMNS);
    }
    // Corrupt a level count without changing the footer; validate on first use.
    let file = IndexFile::open(&path, 41, 1).unwrap();
    let entry = file.lookup(r.seq_no).unwrap().unwrap();
    drop(file);
    let mut fd = fs::OpenOptions::new().write(true).open(&path).unwrap();
    fd.seek(SeekFrom::Start(entry.seg_offset + 8)).unwrap();
    fd.write_all(&u32::MAX.to_be_bytes()).unwrap();
    drop(fd);
    let file = IndexFile::open(&path, 41, 1).unwrap();
    assert!(file.segment_header(&entry).is_err());
}

#[test]
fn dirty_level_uses_reserved_bitmap_bit_without_shift_overflow() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("dirty.idx");
    let mut r = record(SeqNo::new(1, 1).unwrap(), INTEGER, 0, 0);
    r.level = LEVEL_DIRTY;
    r.pos.block = BlockLoc::default();
    r.pos.block_size = 0;
    r.values.count = 0;
    let mut writer = IndexFileWriter::create(&path, 41, 1).unwrap();
    writer
        .write_segment(r.seq_no, INTEGER, &[(LEVEL_DIRTY, vec![r])])
        .unwrap();
    let file = IndexFile::from_meta(writer.finish().unwrap()).unwrap();
    let e = file.lookup(r.seq_no).unwrap().unwrap();
    assert_eq!(e.level_bits, 0x80);
    let h = file.segment_header(&e).unwrap();
    let actual = file
        .read_level(&e, &h, LEVEL_DIRTY, 0, 3, &ALL_COLUMNS)
        .unwrap()
        .remove(0)
        .into_record()
        .unwrap();
    assert_eq!(r, actual);
}

#[test]
fn bounded_codecs_reject_huge_runs_and_malformed_gorilla_windows() {
    let mut huge = vec![0x20];
    huge.extend_from_slice(&[0; 8]);
    huge.push(0);
    huge.extend_from_slice(&[0xff; 10]);
    assert!(enc::decode_ints(&huge, 1).is_err());
    assert!(enc::decode_times(&huge, 1).is_err());
    for len in 0..80 {
        for byte in [0, 0xff, 0x10, 0x20] {
            let mut bytes = vec![byte; len];
            if len > 0 {
                bytes[0] = 0x10;
            }
            let _ = enc::decode_floats(&bytes, 256);
        }
    }
    assert!(enc::decode_bools(&[0x10, 0xff, 0xff, 0x7f], 256).is_err());
}

#[test]
fn streaming_multiple_groups_and_sparse_projections_match_raw_positions() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("stream.idx");
    let seq = SeqNo::new(9, 1).unwrap();
    let records = (0..600)
        .map(|i| record(seq, INTEGER, i * 10, i as u64))
        .collect::<Vec<_>>();
    let mut writer = IndexFileWriter::create(&path, 41, 1).unwrap();
    writer.begin_segment(seq, INTEGER).unwrap();
    for r in &records {
        writer.push(r).unwrap();
    }
    writer.end_segment().unwrap();
    let file = IndexFile::from_meta(writer.finish().unwrap()).unwrap();
    let e = file.lookup(seq).unwrap().unwrap();
    let h = file.segment_header(&e).unwrap();
    for cols in [
        &[][..],
        &[Column::Max][..],
        &[Column::Last, Column::MinAt][..],
        &ALL_COLUMNS[..],
    ] {
        for (a, b) in [(2553, 2560), (5113, 5120), (2554, 2559), (-10, 8000)] {
            let actual = file.read_level(&e, &h, 1, a, b, cols).unwrap();
            assert_eq!(
                file.read_level_verified(&e, &h, 1, a, b, cols).unwrap(),
                actual,
            );
            let expected = records
                .iter()
                .filter(|r| r.pos.start <= b && r.pos.end >= a)
                .collect::<Vec<_>>();
            assert_eq!(actual.len(), expected.len());
            for (actual, expected) in actual.into_iter().zip(expected) {
                assert_eq!(actual.pos, expected.pos);
                for col in ALL_COLUMNS {
                    assert_eq!(actual.values[col as usize].is_some(), cols.contains(&col));
                }
                if cols.len() == 9 {
                    assert_eq!(actual.into_record().unwrap(), *expected);
                }
            }
        }
    }
}

#[test]
fn verified_projection_rejects_corruption_in_an_unselected_column() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("projection.idx");
    let seq = SeqNo::new(1, 1).unwrap();
    let mut writer = IndexFileWriter::create(&path, 41, 1).unwrap();
    writer
        .write_segment(seq, INTEGER, &[(1, vec![record(seq, INTEGER, 10, 7)])])
        .unwrap();
    let file = IndexFile::from_meta(writer.finish().unwrap()).unwrap();
    let entry = file.lookup(seq).unwrap().unwrap();
    let header = file.segment_header(&entry).unwrap();
    let mut groups = file.groups(&entry, &header, 1).unwrap();
    let group = groups.entry(0).unwrap();
    let level_offset = entry.seg_offset + u64::from(header.level(1).unwrap().level_offset);
    let (offset, len) = group.read_range(level_offset, &ALL_COLUMNS).unwrap();
    let mut bytes = fs::read(&path).unwrap();
    bytes[offset as usize + len - 1] ^= 1;
    fs::write(&path, bytes).unwrap();
    let error = file
        .read_level_verified(&entry, &header, 1, 0, 100, &[Column::Count])
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("page group payload checksum mismatch")
    );
}

#[test]
fn projected_reads_skip_unrequested_bytes_and_check_each_selected_part() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("columns.idx");
    let seq = SeqNo::new(1, 1).unwrap();
    let records: Vec<_> = (0..600)
        .map(|i| record(seq, INTEGER, i * 10, i as u64))
        .collect();
    let mut writer = IndexFileWriter::create(&path, 41, 1).unwrap();
    writer
        .write_segment(seq, INTEGER, &[(1, records.clone())])
        .unwrap();
    let file = IndexFile::from_meta(writer.finish().unwrap()).unwrap();
    let entry = file.lookup(seq).unwrap().unwrap();
    let header = file.segment_header(&entry).unwrap();
    for cols in [
        &[][..],
        &[Column::Count][..],
        &[Column::Sum][..],
        &[Column::Last, Column::Min][..],
        &ALL_COLUMNS[..],
    ] {
        let mut metrics = AggregateIndexReadMetrics::default();
        let rows = file
            .read_level_projected(&entry, &header, 1, i64::MIN, i64::MAX, cols, &mut metrics)
            .unwrap();
        assert_eq!(rows.len(), records.len());
        assert_eq!(
            rows,
            file.read_level_verified(&entry, &header, 1, i64::MIN, i64::MAX, cols)
                .unwrap()
        );
        let mut groups = file.groups(&entry, &header, 1).unwrap();
        let wanted: usize = (0..groups.len())
            .map(|i| {
                let e = groups.entry(i).unwrap();
                e.position_len as usize
                    + ALL_COLUMNS
                        .iter()
                        .filter(|c| cols.contains(c))
                        .map(|c| e.column_lens[*c as usize] as usize)
                        .sum::<usize>()
            })
            .sum();
        assert_eq!(metrics.requested_bytes, wanted as u64);
        assert_eq!(metrics.decoded_columns, 3 * cols.len() as u64);
        assert_eq!(metrics.legacy_groups, 0);
        if cols.len() < 9 {
            assert!(metrics.requested_bytes < metrics.full_group_bytes);
        } else {
            assert_eq!(metrics.requested_bytes, metrics.full_group_bytes);
            assert_eq!(metrics.read_requests, 3);
        }
    }
    let group = file.groups(&entry, &header, 1).unwrap().entry(0).unwrap();
    let base = entry.seg_offset as usize + header.level(1).unwrap().level_offset as usize;
    let original = fs::read(&path).unwrap();
    let mut corrupt = original.clone();
    corrupt[base + group.byte_len(&ALL_COLUMNS) - 1] ^= 1;
    fs::write(&path, &corrupt).unwrap();
    let mut metrics = Default::default();
    assert!(
        file.read_level_projected(&entry, &header, 1, 0, 9, &[Column::Sum], &mut metrics)
            .is_ok()
    );
    let error = file
        .read_level_projected(&entry, &header, 1, 0, 9, &[Column::Last], &mut metrics)
        .unwrap_err();
    assert!(error.to_string().contains("column checksum mismatch"));
    assert!(
        file.read_level_verified(&entry, &header, 1, 0, 9, &[Column::Sum])
            .is_err()
    );
    let mut corrupt = original;
    corrupt[base + 4] ^= 1;
    fs::write(&path, &corrupt).unwrap();
    assert!(
        file.read_level_projected(&entry, &header, 1, 0, 9, &[Column::Sum], &mut metrics)
            .is_err()
    );
}

fn write_tree(path: &std::path::Path, start: i64, count: usize, generation: i64) -> Arc<IndexFile> {
    use super::super::builder::TreeBuilder;
    let seq = SeqNo::new(1, 1).unwrap();
    let mut levels: Vec<_> = (1..=super::super::MAX_LEVELS)
        .rev()
        .map(|l| (l, Vec::new()))
        .collect();
    let mut emit = |r: Record| {
        levels[(super::super::MAX_LEVELS - r.level) as usize]
            .1
            .push(r);
        Ok(())
    };
    let mut builder = TreeBuilder::new();
    for i in start..start + count as i64 {
        builder
            .push(record(seq, INTEGER, i * 10, i as u64), &mut emit)
            .unwrap();
    }
    builder.finish(&mut emit).unwrap();
    levels.retain(|(_, records)| !records.is_empty());
    let mut writer = IndexFileWriter::create(path, generation, 1).unwrap();
    writer.write_segment(seq, INTEGER, &levels).unwrap();
    IndexFile::from_meta(writer.finish().unwrap()).unwrap()
}

#[test]
fn ordered_compaction_preserves_boundaries_and_copies_partial_groups_verbatim() {
    use super::super::compact::{SegmentSource, merge_ordered_segments};
    let dir = tempdir().unwrap();
    let seq = SeqNo::new(1, 1).unwrap();
    let files = [
        write_tree(&dir.path().join("a.idx"), 1, 100, 41),
        write_tree(&dir.path().join("b.idx"), 101, 100, 42),
    ];
    let sources = files
        .iter()
        .map(|file| {
            let entry = file.lookup(seq).unwrap().unwrap();
            let header = file.segment_header(&entry).unwrap();
            SegmentSource {
                file,
                entry,
                header,
            }
        })
        .collect::<Vec<_>>();
    let mut writer = IndexFileWriter::create(&dir.path().join("merged.idx"), 43, 1).unwrap();
    let stats = merge_ordered_segments(&mut writer, &sources).unwrap();
    assert_eq!(
        (
            stats.copied_groups,
            stats.rewritten_groups,
            stats.boundary_children_read,
            stats.removed_summaries,
            stats.new_summaries
        ),
        (4, 0, 0, 0, 0)
    );
    let merged = IndexFile::from_meta(writer.finish().unwrap()).unwrap();
    let entry = merged.lookup(seq).unwrap().unwrap();
    let header = merged.segment_header(&entry).unwrap();
    for level in [1, 2] {
        let mut output = merged.groups(&entry, &header, level).unwrap();
        assert_eq!(output.len(), 2);
        for (i, source) in sources.iter().enumerate() {
            let a = source
                .file
                .groups(&source.entry, &source.header, level)
                .unwrap()
                .read(0)
                .unwrap();
            let b = output.read(i).unwrap();
            assert_eq!(
                a.bytes, b.bytes,
                "compressed columns must be copied verbatim"
            );
            assert_eq!(a.entry.row_count, b.entry.row_count);
        }
    }
    let parents = merged
        .read_level(&entry, &header, 2, i64::MIN, i64::MAX, &ALL_COLUMNS)
        .unwrap();
    assert_eq!(
        parents
            .iter()
            .map(|r| (r.pos.start, r.pos.end))
            .collect::<Vec<_>>(),
        [(10, 643), (650, 1003), (1010, 1643), (1650, 2003)]
    );
    let leaves = merged
        .read_level(&entry, &header, 1, 990, 1023, &[Column::Count])
        .unwrap();
    assert_eq!(
        leaves.iter().map(|r| r.pos.start).collect::<Vec<_>>(),
        [990, 1000, 1010, 1020]
    );
}

#[test]
fn disk_compaction_repairs_only_boundary_groups_and_retains_every_level() {
    use super::super::compact::{SegmentSource, merge_ordered_segments};
    for counts in [
        vec![64, 6],
        vec![6, 64],
        vec![65, 6],
        vec![6, 65],
        vec![4096, 6],
        vec![6, 4096],
        vec![4096, 70, 5000],
        vec![1; 80],
        vec![100; 40],
    ] {
        let dir = tempdir().unwrap();
        let seq = SeqNo::new(1, 1).unwrap();
        let mut start = 1;
        let files = counts
            .iter()
            .enumerate()
            .map(|(i, &count)| {
                let file = write_tree(
                    &dir.path().join(format!("{i}.idx")),
                    start,
                    count,
                    i as i64 + 1,
                );
                start += count as i64;
                file
            })
            .collect::<Vec<_>>();
        let sources = files
            .iter()
            .map(|file| {
                let entry = file.lookup(seq).unwrap().unwrap();
                let header = file.segment_header(&entry).unwrap();
                SegmentSource {
                    file,
                    entry,
                    header,
                }
            })
            .collect::<Vec<_>>();
        let mut writer = IndexFileWriter::create(&dir.path().join("result.idx"), 100, 1).unwrap();
        let stats = merge_ordered_segments(&mut writer, &sources).unwrap();
        let expected = match counts.as_slice() {
            // Equal-height inputs retain their leaf groups without boundary reads.
            [64, 6] | [6, 64] => Some((2, 0, 0, 0, 2)),
            // A taller input opens only the parent facing the join boundary.
            [65, 6] => Some((2, 1, 33, 1, 1)),
            [6, 65] => Some((2, 1, 32, 1, 1)),
            _ => None,
        };
        if let Some(expected) = expected {
            assert_eq!(
                (
                    stats.copied_groups,
                    stats.rewritten_groups,
                    stats.boundary_children_read,
                    stats.removed_summaries,
                    stats.new_summaries
                ),
                expected,
                "{counts:?}"
            );
        }
        let file = IndexFile::from_meta(writer.finish().unwrap()).unwrap();
        let entry = file.lookup(seq).unwrap().unwrap();
        let header = file.segment_header(&entry).unwrap();
        assert!(header.level(entry.top_level).unwrap().count <= super::super::FAN_OUT as u32);
        let mut children = (1..start)
            .map(|i| record(seq, INTEGER, i * 10, i as u64))
            .collect::<Vec<_>>();
        for level in 1..=entry.top_level {
            let actual = file
                .read_level(&entry, &header, level, i64::MIN, i64::MAX, &ALL_COLUMNS)
                .unwrap()
                .into_iter()
                .map(SelectedRecord::into_record)
                .collect::<Result<Vec<_>>>()
                .unwrap();
            if level == 1 {
                assert_eq!(actual, children);
            } else {
                let mut used = 0;
                for parent in &actual {
                    let end = used + parent.pos.child_count as usize;
                    assert_eq!(*parent, Record::summarize(&children[used..end]).unwrap());
                    used = end;
                }
                assert_eq!(used, children.len());
            }
            assert_eq!(
                actual.iter().map(|r| r.values.count).sum::<u64>(),
                (start - 1) as u64
            );
            children = actual;
        }
    }
}

#[test]
fn copied_groups_reject_payload_corruption_and_old_formats_explicitly() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("source.idx");
    let file = write_tree(&path, 1, 100, 41);
    let entry = file.lookup(SeqNo::new(1, 1).unwrap()).unwrap().unwrap();
    let header = file.segment_header(&entry).unwrap();
    let original = fs::read(&path).unwrap();
    let data_offset =
        (entry.seg_offset + u64::from(header.level(1).unwrap().level_offset)) as usize;
    let mut corrupt = original.clone();
    corrupt[data_offset + 11] ^= 1;
    fs::write(&path, &corrupt).unwrap();
    assert!(file.groups(&entry, &header, 1).unwrap().read(0).is_err());
    assert!(
        file.read_level(&entry, &header, 1, 0, 2000, &ALL_COLUMNS)
            .is_err()
    );
    let mut corrupt = original.clone();
    let table = data_offset + header.level(1).unwrap().table_offset as usize;
    corrupt[table + 24] ^= 1;
    fs::write(&path, &corrupt).unwrap();
    assert!(file.groups(&entry, &header, 1).is_err());
    let mut old = original.clone();
    old[4..6].copy_from_slice(&2u16.to_be_bytes());
    fs::write(&path, &old).unwrap();
    let compatible = IndexFile::open(&path, 41, 1).unwrap();
    assert_eq!(compatible.lookup(entry.seq_no).unwrap().unwrap(), entry);
    assert!(
        compatible
            .read_level(&entry, &header, 1, 0, 2000, &ALL_COLUMNS)
            .is_err()
    );
    for version in [0, 1, FORMAT_VERSION + 1, u16::MAX] {
        let mut unsupported = original.clone();
        unsupported[4..6].copy_from_slice(&version.to_be_bytes());
        fs::write(&path, &unsupported).unwrap();
        match IndexFile::open(&path, 41, 1) {
            Err(e) => assert!(e.to_string().contains("explicit migration is required")),
            Ok(_) => panic!("unsupported version {version} must not be accepted"),
        }
    }
}

#[test]
fn overlapping_disk_compaction_is_rejected_before_output_changes() {
    use super::super::compact::{SegmentSource, merge_ordered_segments};
    let dir = tempdir().unwrap();
    let files = [
        write_tree(&dir.path().join("a.idx"), 1, 100, 41),
        write_tree(&dir.path().join("b.idx"), 100, 100, 42),
    ];
    let sources = files
        .iter()
        .map(|file| {
            let entry = file.lookup(SeqNo::new(1, 1).unwrap()).unwrap().unwrap();
            let header = file.segment_header(&entry).unwrap();
            SegmentSource {
                file,
                entry,
                header,
            }
        })
        .collect::<Vec<_>>();
    let mut writer = IndexFileWriter::create(&dir.path().join("out.idx"), 43, 1).unwrap();
    let before = writer.size();
    assert!(merge_ordered_segments(&mut writer, &sources).is_err());
    assert_eq!(writer.size(), before);
}

fn write_fields(
    path: &std::path::Path,
    generation: i64,
    fields: std::ops::RangeInclusive<u64>,
) -> Arc<IndexFile> {
    let mut writer = IndexFileWriter::create(path, generation, 0).unwrap();
    for id in fields {
        let seq = SeqNo::new(id, 1).unwrap();
        writer
            .write_segment(
                seq,
                INTEGER,
                &[(1, vec![record(seq, INTEGER, generation * 10, id)])],
            )
            .unwrap();
    }
    IndexFile::from_meta(writer.finish().unwrap()).unwrap()
}

#[test]
fn file_compaction_streams_directory_pages_and_splits_between_fields() {
    use super::super::compact::merge_files_with_target;
    let dir = tempdir().unwrap();
    let inputs = [
        write_fields(&dir.path().join("a.idx"), 1, 1..=220),
        write_fields(&dir.path().join("b.idx"), 2, 110..=329),
    ];
    let (outputs, _) = merge_files_with_target(dir.path(), 3, &inputs, &|| false, 4096).unwrap();
    assert!(outputs.len() > 1);
    let mut seen = Vec::new();
    for output in outputs {
        let file = IndexFile::from_meta(output).unwrap();
        for page in 0..file.footer.directory_pages() {
            for entry in file.directory_page(page).unwrap() {
                let id = entry.seq_no.series_id();
                seen.push(id);
                let header = file.segment_header(&entry).unwrap();
                let records = file
                    .read_level(&entry, &header, 1, i64::MIN, i64::MAX, &ALL_COLUMNS)
                    .unwrap();
                let expected = if id < 110 {
                    vec![10]
                } else if id > 220 {
                    vec![20]
                } else {
                    vec![10, 20]
                };
                assert_eq!(
                    records.iter().map(|r| r.pos.start).collect::<Vec<_>>(),
                    expected
                );
            }
        }
    }
    assert_eq!(seen, (1..=329).collect::<Vec<_>>());
}

#[test]
fn file_compaction_splits_one_field_at_subtrees_and_preserves_leaf_bytes() {
    use super::super::compact::{SegmentSource, merge_files_with_target};
    let dir = tempdir().unwrap();
    let input = write_tree(&dir.path().join("a.idx"), 1, 8192, 41);
    let seq = SeqNo::new(1, 1).unwrap();
    let entry = input.lookup(seq).unwrap().unwrap();
    let header = input.segment_header(&entry).unwrap();
    let expected = input
        .read_level(&entry, &header, 1, i64::MIN, i64::MAX, &ALL_COLUMNS)
        .unwrap()
        .into_iter()
        .map(SelectedRecord::into_record)
        .collect::<Result<Vec<_>>>()
        .unwrap();
    // This target also exercises descending into a single oversized root.
    let target = input.len / 4;
    let cancelled = || {
        fs::read_dir(dir.path()).unwrap().any(|entry| {
            entry
                .unwrap()
                .path()
                .extension()
                .is_some_and(|ext| ext == "tmp")
        })
    };
    assert!(matches!(
        merge_files_with_target(dir.path(), 42, &[input.clone()], &cancelled, target),
        Err(super::super::Error::CompactionCancelled)
    ));
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    let (metas, stats) =
        merge_files_with_target(dir.path(), 42, &[input.clone()], &|| false, target).unwrap();
    assert!(metas.len() > 1);
    assert!(stats.copied_groups > 0);
    assert_eq!(stats.repaired_blocks, 0);
    let mut actual = Vec::new();
    let mut files = Vec::new();
    for meta in metas {
        assert!(fs::metadata(&meta.path).unwrap().len() <= target);
        let file = IndexFile::from_meta(meta).unwrap();
        let entry = file.lookup(seq).unwrap().unwrap();
        let header = file.segment_header(&entry).unwrap();
        SegmentSource {
            file: &file,
            entry: entry.clone(),
            header: header.clone(),
        }
        .roots()
        .unwrap();
        actual.extend(
            file.read_level(&entry, &header, 1, i64::MIN, i64::MAX, &ALL_COLUMNS)
                .unwrap()
                .into_iter()
                .map(SelectedRecord::into_record)
                .collect::<Result<Vec<_>>>()
                .unwrap(),
        );
        files.push(file);
    }
    assert_bits_equal(&expected, &actual);
    // The fragments must remain valid input to another compaction.
    let (metas, _) = merge_files_with_target(dir.path(), 43, &files, &|| false, u64::MAX).unwrap();
    assert_eq!(metas.len(), 1);
    let merged = IndexFile::from_meta(metas[0].clone()).unwrap();
    let entry = merged.lookup(seq).unwrap().unwrap();
    let header = merged.segment_header(&entry).unwrap();
    let actual = merged
        .read_level(&entry, &header, 1, i64::MIN, i64::MAX, &ALL_COLUMNS)
        .unwrap()
        .into_iter()
        .map(SelectedRecord::into_record)
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_bits_equal(&expected, &actual);
}

#[test]
fn cancellation_during_file_merge_removes_only_its_temporary_outputs() {
    use super::super::compact::merge_files;
    use std::cell::Cell;
    let dir = tempdir().unwrap();
    let inputs = [
        write_fields(&dir.path().join("a.idx"), 1, 1..=110),
        write_fields(&dir.path().join("b.idx"), 2, 1..=110),
    ];
    let calls = Cell::new(0);
    let result = merge_files(dir.path(), 3, &inputs, &|| {
        let n = calls.get() + 1;
        calls.set(n);
        n > 130
    });
    assert!(matches!(
        result,
        Err(super::super::Error::CompactionCancelled)
    ));
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 2);
    assert!(inputs.iter().all(|f| f.path().exists()));
}

#[test]
fn automatic_file_selection_respects_fan_in_exclusions_and_byte_budget() {
    use super::super::planner::IndexPlanner;
    let select_inputs =
        |files: &[Arc<IndexFile>], excluded: &std::collections::HashSet<u32>, bytes| {
            IndexPlanner::new(
                std::time::Duration::ZERO,
                Arc::new(crate::storage::tsm1::engine::EngineStatistics::default()),
                1,
            )
            .select(
                files,
                excluded,
                bytes,
                crate::common::time::Time::now(),
                &|| false,
            )
            .unwrap()
        };
    const AUTO_INPUT_BYTES: u64 = 128 * 1024 * 1024;
    let dir = tempdir().unwrap();
    let mut files = (1..=8)
        .map(|i| write_fields(&dir.path().join(format!("{i}.idx")), i, 1..=3))
        .collect::<Vec<_>>();
    assert_eq!(
        select_inputs(&files, &Default::default(), AUTO_INPUT_BYTES).len(),
        8
    );
    assert!(
        select_inputs(
            &files,
            &std::collections::HashSet::from([files[0].file_no]),
            AUTO_INPUT_BYTES
        )
        .is_empty()
    );
    for file in &mut files {
        Arc::get_mut(file).unwrap().len = AUTO_INPUT_BYTES / 2;
    }
    assert!(select_inputs(&files, &Default::default(), AUTO_INPUT_BYTES).is_empty());
    assert_eq!(
        select_inputs(&files, &Default::default(), AUTO_INPUT_BYTES * 4).len(),
        8
    );
    Arc::get_mut(&mut files[3]).unwrap().len = files[3].fd.metadata().unwrap().len();
    assert_eq!(
        select_inputs(&files, &Default::default(), u64::MAX).len(),
        8
    );
}

#[test]
#[ignore = "synthetic directory experiment; creates up to one million field segments"]
fn aggregate_index_research_directory_memory() {
    use super::super::caches::ReadCache;
    let fields: u64 = std::env::var("REEFSDB_AGG_BENCH_FIELDS")
        .unwrap_or_else(|_| "100000".into())
        .parse()
        .unwrap();
    assert!((1..=1000000).contains(&fields));
    let dir = tempdir().unwrap();
    let file = write_fields(&dir.path().join("fields.idx"), 1, 1..=fields);
    let cache = ReadCache::new(1024 * 1024);
    file.set_cache(Arc::clone(&cache));
    for i in 0..10000 {
        let seq = SeqNo::new(1 + (i * 7919) % fields, 1).unwrap();
        let entry = file.lookup(seq).unwrap().unwrap();
        let header = file.segment_header(&entry).unwrap();
        assert_eq!(
            file.read_level(&entry, &header, 1, i64::MIN, i64::MAX, &[Column::Count])
                .unwrap()
                .len(),
            1
        );
    }
    let stats = cache.stats();
    assert!(stats.bytes <= 1024 * 1024);
    let rss = fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .filter(|line| line.starts_with("VmRSS:") || line.starts_with("VmHWM:"))
        .map(str::to_owned)
        .collect::<Vec<_>>();
    println!(
        "AGG_MEMORY {}",
        serde_json::json!({
            "fields": fields, "file_bytes": file.size(), "top_index_bytes": file.top_index.0.len() * 8,
            "cache_budget_bytes": 1024 * 1024, "cache_accounted_bytes": stats.bytes,
            "cache_entries": stats.entries, "hits": stats.hits, "misses": stats.misses,
            "process_memory": rss,
        })
    );
}
