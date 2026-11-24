use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rand::{Rng, RngCore, SeedableRng, rngs::StdRng};
use reefsdb::storage::tsm1::data_encoding::int::{IntegerDecoder, IntegerEncoder};

// golang: 18.33 ns/op
// rust:   13.720 ns
fn bench_integer_encoder_rle(c: &mut Criterion) {
    let mut enc = IntegerEncoder::new(1024);
    let mut x = vec![0i64; 1024];
    for (i, v) in x.iter_mut().enumerate() {
        // Small amount of randomness prevents RLE from being used
        *v = i as i64;
        enc.write(*v);
    }

    c.bench_function("bench_integer_encoder_rle", |b| {
        b.iter_with_large_drop(|| enc.bytes())
    });
}

// golang: 2.492 µs/op
// rust:   1.9924 µs
fn bench_integer_encoder_packed_simple(c: &mut Criterion) {
    let mut enc = IntegerEncoder::new(1024);
    let mut x = vec![0i64; 1024];
    for (i, v) in x.iter_mut().enumerate() {
        *v = i as i64 + rand::thread_rng().gen_range::<i64, _>(0..10);
        enc.write(*v);
    }

    c.bench_function("bench_integer_encoder_rle", |b| {
        b.iter(|| {
            black_box(enc.bytes().unwrap().len());
            enc.reset();
            for v in &x {
                enc.write(*v);
            }
        })
    });
}
// bench_integer_batch_decode_all_uncompressed/size: 5
// golang: 384.4 ns/op
// rust:   115.12 ns
// bench_integer_batch_decode_all_uncompressed/size: 55
// golang: 593.9 ns/op
// rust:   1.1050 µs
// bench_integer_batch_decode_all_uncompressed/size: 555
// golang: 2.599 µs/op
// rust:   31.241 µs
// bench_integer_batch_decode_all_uncompressed/size: 1000
// golang: 4.239 µs/op
// rust:   74.115 µs
fn bench_integer_batch_decode_all_uncompressed(c: &mut Criterion) {
    let benchmarks = vec![5, 55, 555, 1000];

    let values = vec![
        -2352281900722994752,
        1438442655375607923,
        -4110452567888190110,
        -1221292455668011702,
        -1941700286034261841,
        -2836753127140407751,
        1432686216250034552,
        3663244026151507025,
        -3068113732684750258,
        -1949953187327444488,
        3713374280993588804,
        3226153669854871355,
        -2093273755080502606,
        1006087192578600616,
        -2272122301622271655,
        2533238229511593671,
        -4450454445568858273,
        2647789901083530435,
        2761419461769776844,
        -1324397441074946198,
        -680758138988210958,
        94468846694902125,
        -2394093124890745254,
        -2682139311758778198,
    ];

    let mut g = c.benchmark_group("bench_integer_batch_decode_all_uncompressed");
    for size in benchmarks {
        let mut rng = StdRng::seed_from_u64((size * 1000) as u64);

        let mut enc = IntegerEncoder::new(size);
        for _ in 0..size {
            enc.write(values[rng.next_u64() as usize % values.len()]);
        }
        let bytes = enc.bytes().unwrap();

        g.bench_function(&format!("size: {}", size), |b| {
            b.iter(|| {
                let mut dec = IntegerDecoder::new();
                dec.set_bytes(bytes.as_slice()).unwrap();
                let mut count = 0;
                for _ in dec {
                    count += 1;
                }
                assert_eq!(
                    size, count,
                    "unexpected value count, exp {}, got {}",
                    size, count
                );
            })
        });
    }
}

// bench_integer_batch_decode_all_packed_simple/size: 5
// golang: 335.8 ns/op
// rust:   80.985 ns
// bench_integer_batch_decode_all_packed_simple/size: 55
// golang: 516.6 ns/op
// rust:   350.96  ns
// bench_integer_batch_decode_all_packed_simple/size: 555
// golang: 2.319 µs/op
// rust:   2.8740 µs
// bench_integer_batch_decode_all_packed_simple/size: 1000
// golang: 3.829 µs/op
// rust:   5.5625 µs
fn bench_integer_batch_decode_all_packed_simple(c: &mut Criterion) {
    let benchmarks = vec![5, 55, 555, 1000];

    let mut g = c.benchmark_group("bench_integer_batch_decode_all_packed_simple");
    for size in benchmarks {
        let mut rng = StdRng::seed_from_u64((size * 1000) as u64);

        let mut enc = IntegerEncoder::new(size);
        for i in 0..size {
            enc.write(i as i64 + rng.gen_range(0..10) as i64);
        }
        let bytes = enc.bytes().unwrap();

        g.bench_function(&format!("size: {}", size), |b| {
            b.iter(|| {
                let mut dec = IntegerDecoder::new();
                dec.set_bytes(bytes.as_slice()).unwrap();
                let mut count = 0;
                for _ in dec {
                    count += 1;
                }
                assert_eq!(
                    size, count,
                    "unexpected value count, exp {}, got {}",
                    size, count
                );
            })
        });
    }
}

// bench_integer_batch_decode_all_rle/5_delta_1
// golang: 327.9 ns/op
// rust:   170.19 ns
// bench_integer_batch_decode_all_rle/55_delta_1
// golang: 525.0 ns/op
// rust:   739.16 ns
// bench_integer_batch_decode_all_rle/555_delta_1
// golang: 1.583 µs/op
// rust:   8.8863 µs
// bench_integer_batch_decode_all_rle/1000_delta_1
// golang: 2.538 µs/op
// rust:   9.7773 µs
// bench_integer_batch_decode_all_rle/1000_delta_0
// golang: 2.574 µs/op
// rust:   10.743 µs
fn bench_integer_batch_decode_all_rle(c: &mut Criterion) {
    let benchmarks = vec![(5, 1), (55, 1), (555, 1), (1000, 1), (1000, 0)]; // (n, delta)

    let mut g = c.benchmark_group("bench_integer_batch_decode_all_rle");
    for (n, delta) in benchmarks {
        let mut enc = IntegerEncoder::new(n);
        let mut acc = 0i64;
        for _ in 0..n {
            enc.write(acc);
            acc += delta;
        }
        let bytes = enc.bytes().unwrap();

        g.bench_function(&format!("{}_delta_{}", n, delta), |b| {
            b.iter(|| {
                let mut dec = IntegerDecoder::new();
                dec.set_bytes(bytes.as_slice()).unwrap();
                let mut count = 0;
                for _ in dec {
                    count += 1;
                }
                assert_eq!(n, count, "unexpected value count, exp {}, got {}", n, count);
            })
        });
    }
}

criterion_group!(
    benches,
    bench_integer_encoder_rle,
    bench_integer_encoder_packed_simple,
    bench_integer_batch_decode_all_uncompressed,
    bench_integer_batch_decode_all_packed_simple,
    bench_integer_batch_decode_all_rle
);
criterion_main!(benches);
