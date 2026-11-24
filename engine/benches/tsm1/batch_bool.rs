use criterion::{Criterion, criterion_group, criterion_main};
use rand::Rng;
use reefsdb::storage::tsm1::data_encoding::{
    batch_bool::{bool_array_decode_all, bool_array_encode_all},
    bool::BoolEncoder,
};

// bench_batch_bool_encode_bools/10_random_iter
// golang: 83.01 ns/op
// rust:   63.575 ns
// bench_batch_bool_encode_bools/10_random_batch
// golang: 27.88 ns/op
// rust:   73.257 ns
// bench_batch_bool_encode_bools/100_random_iter
// golang: 358.9 ns/op
// rust:   293.20 ns
// bench_batch_bool_encode_bools/100_random_batch
// golang: 228.9 ns/op
// rust:   239.11 ns
// bench_batch_bool_encode_bools/1000_random_iter
// golang: 5.481 µs/op
// rust:   2.0465 µs
// bench_batch_bool_encode_bools/1000_random_batch
// golang: 2.162 µs/op
// rust:   1.9357 µs
fn bench_batch_bool_encode_bools(c: &mut Criterion) {
    let cases = vec![10, 100, 1000];
    let mut g = c.benchmark_group("bench_batch_bool_encode_bools");
    for n in cases {
        let mut input = vec![];
        for _ in 0..n {
            input.push(rand::thread_rng().gen_range(0..2) == 1);
        }

        let mut enc = BoolEncoder::new(n);
        g.bench_function(&format!("{}_random_iter", n), |b| {
            b.iter(|| {
                enc.reset();
                for v in &input {
                    enc.write(*v);
                }
                enc.flush();
                assert!(enc.bytes().is_ok());
            })
        });

        g.bench_function(&format!("{}_random_batch", n), |b| {
            b.iter(|| {
                assert!(bool_array_encode_all(&mut input).is_ok());
            })
        });
    }
}

// bench_batch_bool_decode_all/size_1
// golang: 6.301 ns/op
// rust:   37.166 ns
// bench_batch_bool_decode_all/size_55
// golang: 49.89 ns/op
// rust:   97.311 ns
// bench_batch_bool_decode_all/size_555
// golang: 417.3 ns/op
// rust:   591.24 ns
// bench_batch_bool_decode_all/size_1000
// golang: 761.0 ns/op
// rust:   1.0156 µs
fn bench_batch_bool_decode_all(c: &mut Criterion) {
    let benchmarks = vec![1, 55, 555, 1000];

    let mut g = c.benchmark_group("bench_batch_bool_decode_all");
    for size in benchmarks {
        let mut enc = BoolEncoder::new(size);
        for i in 0..size {
            enc.write(i & 1 == 1);
        }
        let bytes = enc.bytes().unwrap();

        g.bench_function(&format!("size_{}", size), |b| {
            b.iter(|| {
                let res = bool_array_decode_all(&bytes).unwrap();
                assert_eq!(
                    size,
                    res.len() as i64,
                    "expected to read {} booleans, but read {}",
                    size,
                    res.len()
                );
            })
        });
    }
}

criterion_group!(
    benches,
    bench_batch_bool_encode_bools,
    bench_batch_bool_decode_all,
);
criterion_main!(benches);
