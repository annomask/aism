use criterion::{Criterion, criterion_group, criterion_main};
use reefsdb::storage::tsm1::{
    data_encoding::encoding::BLOCK_FLOAT64, reader::IndirectIndex, writer::InmemIndex,
};

// golang: 1115.048 µs/op
// rust:   712.20 µs
fn bench_indirect_index_unmarshal_binary(c: &mut Criterion) {
    let mut index = InmemIndex::new(); // direct index
    for i in 0..100000 {
        index.add(
            format!("cpu-{:06}", i).as_bytes(),
            BLOCK_FLOAT64,
            i * 2,
            i * 2 + 1,
            10,
            100,
            None,
        )
    }

    let bytes = index.marshal_binary().unwrap();

    let indirect = IndirectIndex::new();

    c.bench_function("bench_indirect_index_unmarshal_binary", |b| {
        b.iter(|| {
            indirect.unmarshal_binary(bytes.as_slice()).unwrap();
        })
    });
}

// golang: 301.8 ns/op
// rust:   702.17 ns
fn bench_indirect_index_entries(c: &mut Criterion) {
    let indirect = must_make_index(1000, 100); // add 1000 keys and 100 blocks per key

    c.bench_function("bench_indirect_index_unmarshal_binary", |b| {
        b.iter_with_large_drop(|| indirect.entries("cpu-001".as_bytes()))
    });
}

// golang: 303.8 ns/op
// rust:   671.89 ns
fn bench_indirect_index_read_entries(c: &mut Criterion) {
    let indirect = must_make_index(1000, 100); // add 1000 keys and 100 blocks per key

    let mut cache = vec![];
    c.bench_function("bench_indirect_index_unmarshal_binary", |b| {
        b.iter_with_large_drop(|| indirect.read_entries("cpu-001".as_bytes(), Some(&mut cache)))
    });
}

// golang: 377.896 µs/op
// rust:
#[allow(dead_code)]
fn bench_block_iter_next(_c: &mut Criterion) {
    todo!("implement me");
}

fn must_make_index(keys: i64, blocks: i64) -> IndirectIndex {
    let mut index = InmemIndex::new(); // direct index
    for i in 0..keys {
        for j in 0..blocks {
            index.add(
                format!("cpu-{:03}", i).as_bytes(),
                BLOCK_FLOAT64,
                i * j,
                i * j * 2 + 1,
                10,
                100,
                None,
            )
        }
    }

    let bytes = index.marshal_binary().unwrap();

    let indirect = IndirectIndex::new();
    indirect.unmarshal_binary(bytes.as_slice()).unwrap();

    indirect
}

criterion_group!(
    benches,
    bench_indirect_index_unmarshal_binary,
    bench_indirect_index_entries,
    bench_indirect_index_read_entries,
    // bench_block_iter_next,
);
criterion_main!(benches);
