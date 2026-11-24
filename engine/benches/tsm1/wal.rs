use std::{
    collections::HashMap,
    io::{BufWriter, Seek},
    sync::Arc,
    time::Duration,
};

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use reefsdb::models::points::{FloatValue, Value};
use reefsdb::storage::tsm1::{
    cache::tests::must_temp_tsm_dir,
    wal::{
        DEFAULT_SEGMENT_SIZE, WAL, WALSegmentWriter, WRITE_WAL_ENTRY_TYPE, WalSegmentReader,
        WalStatistics, WriteWalEntry,
        tests::{must_marshal_entry, must_read_temp_file, must_temp_wal_dir, must_write_temp_file},
    },
};

// golang: 413.929 µs/op
// rust:   275.37 µs
fn bench_wal_segment_writer(c: &mut Criterion) {
    let mut points = HashMap::new();
    for i in 0..5000 {
        let k = "cpu,host=A#!~#value";
        points
            .entry(k.to_string())
            .or_insert_with(Vec::new) // Insert a new Vec if the key doesn't exist
            .push(Value::FloatValue(FloatValue::new(i, 1.1))); // Push the new value into the Vec
    }

    let dir = must_temp_tsm_dir().into_path();
    let f = must_write_temp_file(dir.as_path());
    let mut w = WALSegmentWriter::new_tsm_wal_segment_writer(
        Arc::new(dir.as_path().into()),
        10 * 1024 * 1024,
        0,
        BufWriter::new(f),
        Arc::new(crossbeam_channel::bounded(1024)),
        0, // the new segment writer's current pos is 0
    );

    let mut write_entry = WriteWalEntry {
        values: points,
        size: 0, // size will be updated in marshal_size
    };

    let stats = Arc::new(WalStatistics::default());
    c.bench_function("bench_wal_segment_writer", |b| {
        b.iter(|| {
            assert!(
                w.write_entry(
                    WRITE_WAL_ENTRY_TYPE,
                    &must_marshal_entry(&mut write_entry),
                    Arc::clone(&stats)
                )
                .is_ok()
            );
        })
    });

    std::fs::remove_dir_all(dir.as_path()).unwrap();
}

// golang: 56.568439 ms/op
// rust:   79.024 µs
fn bench_wal_segment_reader(c: &mut Criterion) {
    let mut points = HashMap::new();
    for i in 0..5000 {
        let k = "cpu,host=A#!~#value";
        points
            .entry(k.to_string())
            .or_insert_with(Vec::new) // Insert a new Vec if the key doesn't exist
            .push(Value::FloatValue(FloatValue::new(i, 1.1))); // Push the new value into the Vec
    }

    let dir = must_temp_tsm_dir().into_path();
    let f = must_write_temp_file(dir.as_path());
    let mut w = WALSegmentWriter::new_tsm_wal_segment_writer(
        Arc::new(dir.as_path().into()),
        10 * 1024 * 1024,
        0,
        BufWriter::new(f),
        Arc::new(crossbeam_channel::bounded(1024)),
        0, // the new segment writer's current pos is 0
    );

    let mut write_entry = WriteWalEntry {
        values: points,
        size: 0, // size will be updated in marshal_size
    };

    // let (resp_tx, _resp_rx) = tokio::sync::oneshot::channel();
    let stats = Arc::new(WalStatistics::default());
    let compressed = must_marshal_entry(&mut write_entry);
    for _ in 0..100 {
        w.write_entry(WRITE_WAL_ENTRY_TYPE, &compressed, Arc::clone(&stats))
            .unwrap();
    }
    // w.add_resp_tx(resp_tx);
    drop(w);

    let path = dir.as_path();
    c.bench_function("bench_wal_segment_reader", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter_custom(|iters| async move {
                let mut total_elapsed = Duration::ZERO;
                let mut f = must_read_temp_file(path);
                for _i in 0..iters {
                    f.seek(std::io::SeekFrom::Start(0)).unwrap();
                    let mut r = WalSegmentReader::new(&f);
                    let now = std::time::Instant::now();
                    black_box({
                        for entry in r.by_ref() {
                            assert!(entry.is_ok(), "unexpected error reading entry: {:?}", entry);
                        }
                    });
                    total_elapsed += now.elapsed();
                }
                total_elapsed
            })
    });

    std::fs::remove_dir_all(dir.as_path()).unwrap();
}

// golang: 319.932765 ns/op
// rust:   302.75 ms
fn bench_wal_write_multi(c: &mut Criterion) {
    let mut points = HashMap::new();
    for i in 0..5000 {
        let k = "cpu,host=A#!~#value";
        points
            .entry(k.to_string())
            .or_insert_with(Vec::new) // Insert a new Vec if the key doesn't exist
            .push(Value::FloatValue(FloatValue::new(i, 1.1))); // Push the new value into the Vec
    }

    let rt = tokio::runtime::Runtime::new().unwrap();

    let dir = Arc::new(must_temp_wal_dir().into_path());
    let w = rt.block_on(async {
        Arc::new(WAL::new_tsm_wal(Arc::clone(&dir), DEFAULT_SEGMENT_SIZE, Duration::ZERO).unwrap())
    });

    let w2 = Arc::clone(&w);
    c.bench_function("bench_wal_write_multi", |b| {
        b.to_async(&rt).iter_custom(|iters| {
            let points = points.clone();
            let w = Arc::clone(&w2);
            async move {
                let mut total_elapsed = Duration::ZERO;
                for _i in 0..iters {
                    black_box({
                        for _ in 0..100 {
                            let points = points.clone();
                            let start = std::time::Instant::now();
                            assert!(w.write_multi(points).await.is_ok());
                            total_elapsed += start.elapsed();
                        }
                    });
                }
                total_elapsed
            }
        })
    });

    w.close();
    std::fs::remove_dir_all(dir.as_path()).unwrap();
}

criterion_group!(
    benches,
    bench_wal_segment_writer,
    bench_wal_segment_reader,
    bench_wal_write_multi,
);
criterion_main!(benches);
