use super::super::raw_cursor_tests::install_values;
use super::*;
use crate::models::points::IntegerValue;
use crate::query::storage_ir::AggregateKind;
use std::error::Error as _;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};

#[test]
fn corrupt_vlog_block_preserves_decode_error_and_location() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("vlog");
    let store = IndexStore::open(&dir, &root.path().join("wal")).unwrap();
    install_values(
        &store,
        &dir,
        1,
        vec![(0..4)
            .map(|time| Value::IntegerValue(IntegerValue::new(time, time + 10)))
            .collect()],
    );
    let seq = SeqNo::new(1, 1).unwrap();
    let files = store.index_readers();
    let entry = files[0].lookup(seq).unwrap().unwrap();
    let header = files[0].segment_header(&entry).unwrap();
    let record = files[0]
        .read_level_projected(
            &entry,
            &header,
            LEVEL_BLOCK,
            0,
            3,
            &[Column::Count],
            &mut Default::default(),
        )
        .unwrap()
        .remove(0);
    // Damage the stored checksum, leaving the index and block location intact.
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(store.readers()[0].path())
        .unwrap();
    let offset = u64::from(record.pos.block.offset);
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut checksum = [0];
    file.read_exact(&mut checksum).unwrap();
    checksum[0] ^= 0x80;
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&checksum).unwrap();
    file.sync_data().unwrap();

    let mut raw = store.raw_cursor(seq, INTEGER, 1, 2, true).unwrap();
    let error = raw.read_batch().unwrap_err();
    assert!(matches!(error, super::super::Error::VLogBlock { operation: "raw value decode", .. }));
    assert!(error.to_string().contains("checksum mismatch"));
    assert!(error.source().is_some());

    for (kind, cache, expected_operations) in [
        (AggregateKind::Count, vec![], &["timestamp decode"][..]),
        (
            AggregateKind::Sum,
            vec![],
            &["value decode", "streaming reduction"][..],
        ),
        (
            AggregateKind::Sum,
            vec![Value::IntegerValue(IntegerValue::new(1, 100))],
            &["value decode"][..],
        ),
    ] {
        let error = store
            .aggregate_projected(
                seq,
                INTEGER,
                TimeRange::new(1, 2),
                cache,
                AggregateProjection::from_aggregates(&[kind]),
            )
            .unwrap_err();
        let super::super::Error::VLogBlock {
            operation,
            file_no,
            offset,
            size,
            source: stored_source,
        } = &error
        else {
            panic!("expected a VLog block error, got {error:?}");
        };
        assert!(expected_operations.contains(operation));
        assert_eq!(*file_no, record.pos.block.file_no);
        assert_eq!(*offset, record.pos.block.offset);
        assert_eq!(*size, record.pos.block_size);
        // Snafu 0.7 exposes the stored Box through AsErrorSource without unboxing it.
        let source = error
            .source()
            .unwrap()
            .downcast_ref::<Box<VLogError>>()
            .expect("VLog block error must expose its original boxed reader error");
        assert!(std::ptr::eq(source.as_ref(), stored_source.as_ref()));
        assert!(source.to_string().contains("checksum mismatch"));
        let message = error.to_string();
        assert!(message.contains(*operation));
        assert!(message.contains(&format!("file {file_no}")));
        assert!(message.contains(&format!("offset {offset}")));
        assert!(message.contains(&format!("size {size}")));
        assert!(message.contains("checksum mismatch"));
    }
}
