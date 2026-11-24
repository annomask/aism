use criterion::{
    BenchmarkGroup, Criterion, criterion_group, criterion_main, measurement::WallTime,
};
use reefsdb::storage::tsm1::data_encoding::simple8b::{self, tests::*};

struct Benchmark {
    name: &'static str,
    f: Option<Box<dyn Fn(i32) -> Vec<u64>>>,
}

fn bench_simple8b_encode_all(c: &mut Criterion) {
    let mut benchmarks = vec![
        // golang: 15.638 µs/op
        // rust:   13.530 µs
        Benchmark {
            name: "1 bit",
            f: Some(Box::new(bits_n(1))),
        },
        // golang: 15.529 µs/op
        // rust:   13.734 µs
        Benchmark {
            name: "2 bit",
            f: Some(Box::new(bits_n(2))),
        },
        // golang: 16.122 µs/op
        // rust:   13.610 µs
        Benchmark {
            name: "3 bit",
            f: Some(Box::new(bits_n(3))),
        },
        // golang: 16.236 µs/op
        // rust:   13.888 µs
        Benchmark {
            name: "4 bit",
            f: Some(Box::new(bits_n(4))),
        },
        // golang: 16.105 µs/op
        // rust:   14.380 µs
        Benchmark {
            name: "5 bit",
            f: Some(Box::new(bits_n(5))),
        },
        // golang: 16.572 µs/op
        // rust:   14.358 µs
        Benchmark {
            name: "6 bit",
            f: Some(Box::new(bits_n(6))),
        },
        // golang: 17.549 µs/op
        // rust:   14.947 µs
        Benchmark {
            name: "7 bit",
            f: Some(Box::new(bits_n(7))),
        },
        // golang: 19.929 µs/op
        // rust:   15.093 µs
        Benchmark {
            name: "8 bit",
            f: Some(Box::new(bits_n(8))),
        },
        // golang: 19.609 µs/op
        // rust:   14.486 µs
        Benchmark {
            name: "10 bit",
            f: Some(Box::new(bits_n(10))),
        },
        // golang: 18.822 µs/op
        // rust:   14.730 µs
        Benchmark {
            name: "12 bit",
            f: Some(Box::new(bits_n(12))),
        },
        // golang: 19.640 µs/op
        // rust:   14.843 µs
        Benchmark {
            name: "15 bit",
            f: Some(Box::new(bits_n(15))),
        },
        // golang: 22.804 µs/op
        // rust:   15.224 µs
        Benchmark {
            name: "20 bit",
            f: Some(Box::new(bits_n(20))),
        },
        // golang: 26.045 µs/op
        // rust:   16.279 µs
        Benchmark {
            name: "30 bit",
            f: Some(Box::new(bits_n(30))),
        },
        // golang: 36.940 µs/op
        // rust:   18.833 µs
        Benchmark {
            name: "60 bit",
            f: Some(Box::new(bits_n(60))),
        },
        // golang: 35.9521 µs/op
        // rust:   212.16 µs
        Benchmark {
            name: "combination bit",
            f: Some(Box::new(combile_n(vec![
                1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60,
            ]))),
        },
    ];

    let mut g = c.benchmark_group("bench_simple8b_encode_all");
    for bm in &mut benchmarks {
        run_simple8b_encode_all_benchmarks(&mut g, bm);
    }
}

fn run_simple8b_encode_all_benchmarks(g: &mut BenchmarkGroup<WallTime>, bm: &mut Benchmark) {
    g.bench_function(&format!("{}", bm.name), |b| {
        b.iter(|| {
            let mut input = (*bm.f.as_mut().unwrap())(1000);
            let _ = simple8b::encode_all(&mut input);
        })
    });
}

// golang: 1140 ns/op
// rust:   726.04 ns
fn bench_simple8b_encode(c: &mut Criterion) {
    let mut x = vec![0u64; 1024];
    for i in x.iter_mut() {
        *i = 15;
    }

    let mut input = vec![0u64; 1024];
    c.bench_function("bench_simple8b_encode", |b| {
        b.iter(|| {
            unsafe {
                let src = x.as_ptr();
                let dst = input.as_mut_ptr();
                std::ptr::copy(src, dst, 1024);
            }
            let _ = simple8b::encode_all(&mut input);
        })
    });
}

// golang: 871.2 ns/op	9403.24 MB/s
// rust:   1.0683 µs
fn bench_simple8b_encoder(c: &mut Criterion) {
    let mut x = vec![0u64; 1024];
    for i in x.iter_mut() {
        *i = 15;
    }

    let mut enc = simple8b::Encoder::new();
    c.bench_function("bench_simple8b_encoder", |b| {
        b.iter(|| {
            enc.set_values(x.clone());
            enc.bytes().unwrap().len()
        })
    });
}

// golang: 233.3 ns/op
// rust:   178.06 ns
fn bench_simple8b_decode(c: &mut Criterion) {
    let mut x = vec![0u64; 1024];
    for i in x.iter_mut() {
        *i = 10;
    }

    let encoded_count = simple8b::encode_all(&mut x).unwrap();

    let mut decoded = vec![0u64; 1024];
    c.bench_function("bench_simple8b_decode", |b| {
        b.iter(|| {
            let _ = simple8b::decode_all(&mut decoded, &x[..encoded_count as usize]).unwrap();
        })
    });
}
// golang: 2007 ns/op
// rust:   679.08 ps
fn bench_simple8b_decoder(c: &mut Criterion) {
    let mut enc = simple8b::Encoder::new();
    let mut x = vec![0u64; 1024];
    for i in x.iter_mut() {
        *i = 10;
        enc.write(*i).unwrap();
    }

    let y = enc.bytes().unwrap().to_vec();

    let mut dec = simple8b::Decoder::new(Vec::new());
    c.bench_function("bench_simple8b_decoder", |b| {
        b.iter(|| {
            dec.set_bytes(y.clone());
            let mut j = 0;
            for _x in dec.by_ref() {
                j += 1;
            }
            j
        })
    });
}

criterion_group!(
    benches,
    bench_simple8b_encode_all,
    bench_simple8b_encode,
    bench_simple8b_encoder,
    bench_simple8b_decode,
    bench_simple8b_decoder,
);
criterion_main!(benches);
