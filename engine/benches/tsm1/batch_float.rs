use criterion::{Criterion, black_box, criterion_group, criterion_main};
use reefsdb::storage::tsm1::data_encoding::{
    batch_float::{float_array_decode_all, float_array_encode_all},
    float::{FloatEncoder, tests::TWO_HOURS_DATA},
};

// bench_batch_float_encode_floats/10_seq_iter
// golang: 467.5 ns/op
// rust:   123.30 ns
// bench_batch_float_encode_floats/10_seq_batch
// golang: 81.19 ns/op
// rust:   126.14 ns
// bench_batch_float_encode_floats/100_seq_iter
// golang: 2404 ns/op
// rust:   900.39 ns
// bench_batch_float_encode_floats/100_seq_batch
// golang: 555.8 ns/op
// rust:   594.54 ns
// bench_batch_float_encode_floats/1000_seq_iter
// golang: 16.664 µs/op
// rust:   6.9235 µs
// bench_batch_float_encode_floats/1000_seq_batch
// golang: 5.406 µs/op
// rust:   5.3153 µs
// bench_batch_float_encode_floats/10_random_iter
// golang: 587.9 ns/op
// rust:   192.27 ns
// bench_batch_float_encode_floats/10_random_batch
// golang: 78.78 ns/op
// rust:   164.20 ns
// bench_batch_float_encode_floats/100_random_iter
// golang: 4891 ns/op
// rust:   1.3047 µs
// bench_batch_float_encode_floats/100_random_batch
// golang: 652.0 ns/op
// rust:   825.55 ns
// bench_batch_float_encode_floats/1000_random_iter
// golang: 68.571 µs/op
// rust:   11.123 µs
// bench_batch_float_encode_floats/1000_random_batch
// golang: 5.717 µs/op
// rust:   6.2202 µs
fn bench_batch_float_encode_floats(c: &mut Criterion) {
    let cases = vec![10, 100, 1000];
    let mut g = c.benchmark_group("bench_batch_float_encode_floats");
    for n in cases {
        let mut seq_input = vec![];
        for i in 0..n {
            seq_input.push(i as f64);
        }

        let mut random_input = vec![];
        for _ in 0..n {
            random_input.push(rand::random::<f64>());
        }

        g.bench_function(&format!("{}_seq_iter", n), |b| {
            b.iter(|| {
                let mut enc = FloatEncoder::new();
                for x in &seq_input {
                    enc.write(*x);
                }
                enc.flush();

                black_box(enc.bytes().unwrap());
            })
        });

        g.bench_function(&format!("{}_seq_batch", n), |b| {
            b.iter(|| {
                black_box(float_array_encode_all(&seq_input).unwrap());
            })
        });

        g.bench_function(&format!("{}_random_iter", n), |b| {
            b.iter(|| {
                let mut enc = FloatEncoder::new();
                for x in &random_input {
                    enc.write(*x);
                }
                enc.flush();

                black_box(enc.bytes().unwrap());
            })
        });

        g.bench_function(&format!("{}_random_batch", n), |b| {
            b.iter(|| {
                black_box(float_array_encode_all(&random_input).unwrap());
            })
        });
    }
}

// bench_batch_float_decode_floats/1_seq
// golang: 7.000 ns/op
// rust:   14.277 ns
// bench_batch_float_decode_floats/1_random
// golang: 7.116 ns/op
// rust:   14.811 ns
// bench_batch_float_decode_floats/55_seq
// golang: 169.5 ns/op
// rust:   356.68 ns
// bench_batch_float_decode_floats/55_random
// golang: 206.5 ns/op
// rust:   394.19 ns
// bench_batch_float_decode_floats/550_seq
// golang: 1.513 µs/op
// rust:   2.8313 µs
// bench_batch_float_decode_floats/550_random
// golang: 1.987 µs/op
// rust:   3.0643 µs
// bench_batch_float_decode_floats/1000_seq
// golang: 2.751 µs/op
// rust:   4.9918 µs
// bench_batch_float_decode_floats/1000_random
// golang: 3.864 µs/op
// rust:   5.0259 µs
fn bench_batch_float_decode_floats(c: &mut Criterion) {
    let cases = vec![1, 55, 550, 1000];
    let mut g = c.benchmark_group("bench_batch_float_decode_floats");
    for n in cases {
        let mut enc1 = FloatEncoder::new();
        for i in 0..n {
            enc1.write(i as f64);
        }
        enc1.flush();
        let b1 = enc1.bytes().unwrap();

        let mut enc2 = FloatEncoder::new();
        for _ in 0..n {
            enc2.write(rand::random::<f64>() * 100.0);
        }
        enc2.flush();
        let b2 = enc2.bytes().unwrap();

        g.bench_function(&format!("{}_seq", n), |b| {
            b.iter(|| {
                let got = float_array_decode_all(b1).unwrap();
                assert_eq!(
                    n,
                    got.len(),
                    "unexpected length, exp {}, got {}",
                    n,
                    got.len()
                );
            })
        });

        g.bench_function(&format!("{}_random", n), |b| {
            b.iter(|| {
                let got = float_array_decode_all(b2).unwrap();
                assert_eq!(
                    n,
                    got.len(),
                    "unexpected length, exp {}, got {}",
                    n,
                    got.len()
                );
            })
        });
    }
}

// bench_batch_float_float_array_decode_all/size_1
// golang: 7.376 ns/op
// rust:   14.292 ns
// bench_batch_float_float_array_decode_all/size_55
// golang: 158.5 ns/op
// rust:   352.35 ns
// bench_batch_float_float_array_decode_all/size_550
// golang: 1.473 µs/op
// rust:   2.6493 µs
// bench_batch_float_float_array_decode_all/size_1000
// golang: 2.687 µs/op
// rust:   4.5939 µs
fn bench_batch_float_float_array_decode_all(c: &mut Criterion) {
    let cases = vec![1, 55, 550, 1000];
    for n in cases {
        let mut enc = FloatEncoder::new();
        for i in 0..n {
            enc.write(TWO_HOURS_DATA[i % TWO_HOURS_DATA.len()]);
        }
        enc.flush();
        let bytes = enc.bytes().unwrap();

        c.bench_function(
            &format!("bench_batch_float_float_array_decode_all/size_{}", n),
            |b| {
                b.iter(|| {
                    let got = float_array_decode_all(bytes).unwrap();
                    assert_eq!(
                        n,
                        got.len(),
                        "unexpected length, exp {}, got {}",
                        n,
                        got.len()
                    );
                })
            },
        );
    }
}

criterion_group!(
    benches,
    bench_batch_float_encode_floats,
    bench_batch_float_decode_floats,
    bench_batch_float_float_array_decode_all
);
criterion_main!(benches);
