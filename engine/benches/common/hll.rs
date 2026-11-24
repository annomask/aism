use std::{hash::Hasher, time::Duration};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand_core::{OsRng, RngCore};
use reefsdb::common::hll::{HLL, hash64};
use twox_hash::XxHash64;

// Generate random data to add to the hll.
fn gen_data(n: usize) -> Vec<Vec<u8>> {
    let mut out = Vec::<Vec<u8>>::with_capacity(n);
    let mut buf = vec![0u8; 8];

    for _ in 0..n {
        // generate 8 random bytes.
        OsRng.fill_bytes(&mut buf);
        out.push(buf.clone());
    }
    if out.len() != n {
        panic!("wrong size slice: {}", n);
    }
    out
}

fn run_benchmarks(c: &mut Criterion, batch_size: BatchSize, data_size: usize) {
    // generate input data
    let bench_data = gen_data(data_size);
    let mut hll = HLL::new(16u8, hash64).unwrap();

    c.bench_function(&format!("hll_add_{}", data_size), move |b| {
        b.iter_batched(
            || bench_data.clone(),
            |mut bench_data| {
                for data in bench_data {
                    hll.add(&data);
                }
            },
            BatchSize::SmallInput,
        )
    });
}

// golang: 841.7 ns/op
// 1.4392 µs
fn hll_add_100(c: &mut Criterion) {
    run_benchmarks(c, BatchSize::LargeInput, 100);
}

// golang: 8.264 µs/op
// 67.369 µs
fn hll_add_1000(c: &mut Criterion) {
    run_benchmarks(c, BatchSize::LargeInput, 1000);
}

// goalng: 82.837 µs/op
// 1.2200 ms
fn hll_add_10000(c: &mut Criterion) {
    run_benchmarks(c, BatchSize::LargeInput, 10000);
}

// golang: 8.516505 ms/op
// 1.4825 ms
fn hll_add_100000(c: &mut Criterion) {
    run_benchmarks(c, BatchSize::LargeInput, 100000);
}

// golang: 8.533527 ms/op
// 13.935 ms
fn hll_add_1000000(c: &mut Criterion) {
    run_benchmarks(c, BatchSize::LargeInput, 1000000);
}

// golang: 88.292444 ms/op
// 135.81 ms
fn hll_add_10000000(c: &mut Criterion) {
    run_benchmarks(c, BatchSize::LargeInput, 10000000);
}

// golang: 873.733326 ms/op
// 1.3651 s
fn hll_add_100000000(c: &mut Criterion) {
    run_benchmarks(c, BatchSize::LargeInput, 100000000);
}

criterion_group!(
    benches,
    hll_add_100,
    hll_add_1000,
    hll_add_10000,
    hll_add_100000,
    hll_add_1000000,
    hll_add_10000000,
    hll_add_100000000,
);
criterion_main!(benches);
