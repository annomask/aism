use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rand::{Rng, RngCore, SeedableRng, rngs::StdRng};
use reefsdb::common::time::Time;
use reefsdb::storage::tsm1::data_encoding::timestamp::{TimeDecoder, TimeEncoder};

// golang: 7.547 µs/op
// rust:   4.0031 µs
fn bench_time_encoder(c: &mut Criterion) {
    let mut enc = TimeEncoder::new(1024);
    let mut x = vec![0i64; 1024];
    for v in x.iter_mut() {
        // Small amount of randomness prevents RLE from being used
        *v = Time::now().timestamp_nanos();
        enc.write(*v);
    }

    c.bench_function("bench_time_encoder", |b| {
        b.iter(|| {
            let encoded_len = enc.bytes().unwrap().len();
            black_box(encoded_len);
            enc.reset();
            for v in &x {
                enc.write(*v)
            }
        })
    });
}

// golang: 2936 ns/op
// rust:   91.625 ns
fn bench_time_decoder_packed(c: &mut Criterion) {
    let mut enc = TimeEncoder::new(1024);
    let mut x = vec![0i64; 1024];
    for v in x.iter_mut() {
        *v = Time::now().timestamp_nanos();
        enc.write(*v);
    }

    c.bench_function("bench_time_decoder_packed", |b| {
        b.iter_custom(|iters| {
            let mut exec_time = std::time::Duration::ZERO;
            for _i in 0..iters {
                let bytes = enc.bytes().unwrap();
                let mut dec = TimeDecoder::new();

                let start = std::time::Instant::now();

                dec.init(bytes);
                black_box(dec.by_ref().count());
                exec_time += start.elapsed();
            }
            exec_time
        })
    });
}

// golang: 586.1 ns/op
// rust:   85.919 ns
fn bench_time_decoder_rle(c: &mut Criterion) {
    let mut enc = TimeEncoder::new(1024);
    let mut x = vec![0i64; 1024];
    for (i, v) in x.iter_mut().enumerate() {
        *v = (i * 10) as i64;
        enc.write(*v);
    }

    c.bench_function("bench_time_decoder_rle", |b| {
        b.iter_custom(|iters| {
            let mut exec_time = std::time::Duration::ZERO;
            for _i in 0..iters {
                let bytes = enc.bytes().unwrap();
                let mut dec = TimeDecoder::new();

                let start = std::time::Instant::now();

                dec.init(bytes);
                black_box(dec.by_ref().count());
                exec_time += start.elapsed();
            }
            exec_time
        })
    });
}

// bench_time_batch_decode_all_uncompressed/size_5
// golang: 411.5 ns/op
// rust:   148.10 ns
// bench_time_batch_decode_all_uncompressed/size_55
// golang: 503.5 ns/op
// rust:   610.43 ns
// bench_time_batch_decode_all_uncompressed/size_555
// golang: 2142 ns/op
// rust:   4.2790 µs
// bench_time_batch_decode_all_uncompressed/size_1000
// golang: 3992 ns/op
// rust:   6.9214 µs
fn bench_time_batch_decode_all_uncompressed(c: &mut Criterion) {
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

    let mut g = c.benchmark_group("bench_time_batch_decode_all_uncompressed");
    for size in benchmarks {
        let mut rng = StdRng::seed_from_u64((size * 1000) as u64);

        let mut enc = TimeEncoder::new(size);
        for _ in 0..size {
            enc.write(values[rng.next_u64() as usize % values.len()]);
        }
        let bytes = enc.bytes().unwrap();

        let mut dst = Vec::with_capacity(size as usize);
        g.bench_function(&format!("size_{}", size), |b| {
            b.iter(|| {
                dst.clear();
                let mut dec = TimeDecoder::new();
                dec.init(bytes);
                for v in dec {
                    dst.push(v);
                }
                black_box(dst.len());
            })
        });
    }
}

// bench_time_batch_decode_all_packed_simple/size_5
// golang: 436.5 ns/op
// rust:   92.125 ns
// bench_time_batch_decode_all_packed_simple/size_55
// golang: 826.2 ns/op
// rust:   507.53 ns
// bench_time_batch_decode_all_packed_simple/size_555
// golang: 4.718 µs/op
// rust:   4.1744 µs
// bench_time_batch_decode_all_packed_simple/size_1000
// golang: 7.487 µs/op
// rust:   8.2134 µs
fn bench_time_batch_decode_all_packed_simple(c: &mut Criterion) {
    let benchmarks = vec![5, 55, 555, 1000];

    let mut g = c.benchmark_group("bench_time_batch_decode_all_packed_simple");
    for size in benchmarks {
        let mut rng = StdRng::seed_from_u64((size * 1000) as u64);

        let mut enc = TimeEncoder::new(size);
        for i in 0..size {
            enc.write(i as i64 * 1000 + rng.gen_range(0..10) as i64);
        }
        let bytes = enc.bytes().unwrap();

        let mut dst = Vec::with_capacity(size as usize);
        g.bench_function(&format!("size_{}", size), |b| {
            b.iter(|| {
                dst.clear();
                let mut dec = TimeDecoder::new();
                dec.init(bytes);
                for v in dec {
                    dst.push(v);
                }
                black_box(dst.len());
            })
        });
    }
}

// bench_time_batch_decode_all_rle/5_delta_10
// golang: 436.5 ns/op
// rust:   92.125 ns
// bench_time_batch_decode_all_rle/55_delta_10
// golang: 826.2 ns/op
// rust:   507.53 ns
// bench_time_batch_decode_all_rle/555_delta_10
// golang: 4.718 µs/op
// rust:   4.1744 µs
// bench_time_batch_decode_all_rle/1000_delta_10
// golang: 7.487 µs/op
// rust:   8.2134 µs
fn bench_time_batch_decode_all_rle(c: &mut Criterion) {
    struct Benchmark {
        n: i32,
        delta: i64,
    }
    let benchmarks = vec![
        Benchmark { n: 5, delta: 10 },
        Benchmark { n: 55, delta: 10 },
        Benchmark { n: 555, delta: 10 },
        Benchmark { n: 1000, delta: 10 },
    ];

    let mut g = c.benchmark_group("bench_time_batch_decode_all_rle");
    for bm in benchmarks {
        let mut enc = TimeEncoder::new(bm.n.into());
        let mut acc = 0i64;
        for _ in 0..bm.n {
            enc.write(acc);
            acc += bm.delta;
        }
        let bytes = enc.bytes().unwrap();

        let mut dst = Vec::with_capacity(bm.n as usize);
        g.bench_function(&format!("{}_delta_{}", bm.n, bm.delta), |b| {
            b.iter(|| {
                dst.clear();
                let mut dec = TimeDecoder::new();
                dec.init(bytes);
                for v in dec {
                    dst.push(v);
                }
                black_box(dst.len());
            })
        });
    }
}

criterion_group!(
    benches,
    bench_time_encoder,
    bench_time_decoder_packed,
    bench_time_decoder_rle,
    bench_time_batch_decode_all_uncompressed,
    bench_time_batch_decode_all_packed_simple,
    bench_time_batch_decode_all_rle,
);
criterion_main!(benches);
