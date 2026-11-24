use criterion::{Criterion, criterion_group, criterion_main};
use rand::{Rng, RngCore, SeedableRng, rngs::StdRng};
use reefsdb::storage::tsm1::data_encoding::{
    batch_int::{integer_array_decode_all, integer_array_encode_all},
    int::IntegerEncoder,
};

// bench_batch_integer_encode_integers/10_seq_iter
// golang: 32.56 ns/op
// rust:   27.439 ns
// bench_batch_integer_encode_integers/10_seq_batch
// golang: 13.96 ns/op
// rust:   40.141 ns
// bench_batch_integer_encode_integers/10_random_iter
// golang: 50.43 ns/op
// rust:   52.290 ns
// bench_batch_integer_encode_integers/10_random_batch
// golang: 33.46 ns/op
// rust:   38.366 ns
// bench_batch_integer_encode_integers/10_dup_iter
// golang: 32.18 ns/op
// rust:   46.659 ns
// bench_batch_integer_encode_integers/10_dup_batch
// golang: 12.88 ns/op
// rust:   38.260 ns
// bench_batch_integer_encode_integers/100_seq_iter
// golang: 159.0 ns/op
// rust:   106.36 ns
// bench_batch_integer_encode_integers/100_seq_batch
// golang: 104.8 ns/op
// rust:   145.28 ns
// bench_batch_integer_encode_integers/100_random_iter
// golang: 340.7 ns/op
// rust:   162.95 ns
// bench_batch_integer_encode_integers/100_random_batch
// golang: 292.5 ns/op
// rust:   146.11 ns
// bench_batch_integer_encode_integers/100_dup_iter
// golang: 151.3 ns/op
// rust:   176.38 ns
// bench_batch_integer_encode_integers/100_dup_batch
// golang: 93.59 ns/op
// rust:   147.84 ns
// bench_batch_integer_encode_integers/1000_seq_iter
// golang: 1.251 µs/op
// rust:   787.13 ns
// bench_batch_integer_encode_integers/1000_seq_batch
// golang: 777.9 ns/op
// rust:   1.2230 µs
// bench_batch_integer_encode_integers/1000_random_iter
// golang: 3.216 µs/op
// rust:   1.2716 µs
// bench_batch_integer_encode_integers/1000_random_batch
// golang: 3.061 µs/op
// rust:   1.2003 µs
// bench_batch_integer_encode_integers/1000_dup_iter
// golang: 1.233 µs/op
// rust:   1.3076 µs
// bench_batch_integer_encode_integers/1000_dup_batch
// golang: 791.6 ns/op
// rust:   1.1844 µs
fn bench_batch_integer_encode_integers(c: &mut Criterion) {
    let cases = vec![10, 100, 1000];
    let mut g = c.benchmark_group("bench_batch_integer_encode_integers");
    for n in cases {
        let mut seq_input = vec![];
        for i in 0..n {
            seq_input.push(i as i64);
        }

        let mut random_input = vec![];
        for _ in 0..n {
            random_input.push(rand::random::<i64>());
        }

        let dup_input = vec![1233242i64; n as usize];

        let mut input = vec![];

        g.bench_function(&format!("{}_seq_iter", n), |b| {
            b.iter(|| {
                let mut enc = IntegerEncoder::new(n);
                for x in &seq_input {
                    enc.write(*x);
                }
                enc.flush();

                let b = enc.bytes().unwrap();
                // Since the batch encoder needs to do a copy to reset the
                // input, we will add a copy here too.
                input = b.clone();
            })
        });

        g.bench_function(&format!("{}_seq_batch", n), |b| {
            b.iter(|| {
                let mut values = seq_input.clone();
                input = integer_array_encode_all(&mut values).unwrap();
            })
        });

        g.bench_function(&format!("{}_random_iter", n), |b| {
            b.iter(|| {
                let mut enc = IntegerEncoder::new(n);
                for x in &random_input {
                    enc.write(*x);
                }
                enc.flush();

                let b = enc.bytes().unwrap();
                // Since the batch encoder needs to do a copy to reset the
                // input, we will add a copy here too.
                input = b.clone();
            })
        });

        g.bench_function(&format!("{}_random_batch", n), |b| {
            b.iter(|| {
                let mut values = random_input.clone();
                input = integer_array_encode_all(&mut values).unwrap();
            })
        });

        g.bench_function(&format!("{}_dup_iter", n), |b| {
            b.iter(|| {
                let mut enc = IntegerEncoder::new(n);
                for x in &dup_input {
                    enc.write(*x);
                }
                enc.flush();

                let b = enc.bytes().unwrap();
                // Since the batch encoder needs to do a copy to reset the
                // input, we will add a copy here too.
                input = b.clone();
            })
        });

        g.bench_function(&format!("{}_dup_batch", n), |b| {
            b.iter(|| {
                let mut values = dup_input.clone();
                input = integer_array_encode_all(&mut values).unwrap();
            })
        });
    }
}

// bench_batch_integer_decode_all_uncompressed/size_1
// golang: 7.091 ns/op
// rust:   11.301 ns
// bench_batch_integer_decode_all_uncompressed/size_55
// golang: 65.92 ns/op
// rust:   51.783 ns
// bench_batch_integer_decode_all_uncompressed/size_555
// golang: 602.0 ns/op
// rust:   463.52 ns
// bench_batch_integer_decode_all_uncompressed/size_1000
// golang: 1078 ns/op
// rust:   811.12 ns
fn bench_batch_integer_decode_all_uncompressed(c: &mut Criterion) {
    let benchmarks = vec![5, 55, 555, 1000];
    let values: Vec<i64> = vec![
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

    let mut g = c.benchmark_group("bench_batch_integer_decode_all_uncompressed");
    for size in benchmarks {
        let mut rng = StdRng::seed_from_u64((size * 1000) as u64);

        let mut enc = IntegerEncoder::new(size);
        for _ in 0..size {
            enc.write(values[rng.next_u64() as usize % values.len()]);
        }
        let bytes = enc.bytes().unwrap();

        g.bench_function(&format!("size_{}", size), |b| {
            b.iter_with_large_drop(|| integer_array_decode_all(&bytes).unwrap())
        });
    }
}

// bench_batch_integer_decode_all_packed_simple/size_5
// golang: 10.41 ns/op
// rust:   15.147 ns
// bench_batch_integer_decode_all_packed_simple/size_55
// golang: 49.74 ns/op
// rust:   50.135 ns
// bench_batch_integer_decode_all_packed_simple/size_555
// golang: 412.8 ns/op
// rust:   478.10 ns
// bench_batch_integer_decode_all_packed_simple/size_1000
// golang: 731.0 ns/op
// rust:   888.72 ns
fn bench_batch_integer_decode_all_packed_simple(c: &mut Criterion) {
    let benchmarks = vec![5, 55, 555, 1000];

    let mut g = c.benchmark_group("bench_batch_integer_decode_all_packed_simple");
    for size in benchmarks {
        let mut rng = StdRng::seed_from_u64((size * 1000) as u64);

        let mut enc = IntegerEncoder::new(size);
        for i in 0..size {
            // Small amount of randomness prevents RLE from being used
            enc.write(i as i64 + rng.gen_range(0..10));
        }
        let bytes = enc.bytes().unwrap();

        g.bench_function(&format!("size_{}", size), |b| {
            b.iter_with_large_drop(|| integer_array_decode_all(&bytes).unwrap())
        });
    }
}

// bench_batch_integer_decode_all_rle/5_delta_1
// golang: 6.334 ns/op
// rust:   14.257 ns
// bench_batch_integer_decode_all_rle/55_delta_1
// golang: 19.75 ns/op
// rust:   30.922 ns
// bench_batch_integer_decode_all_rle/555_delta_1
// golang: 119.2 ns/op
// rust:   260.86 ns
// bench_batch_integer_decode_all_rle/1000_delta_1
// golang: 225.7 ns/op
// rust:   398.80 ns
// bench_batch_integer_decode_all_rle/1000_delta_0
// golang: 137.9 ns/op
// rust:   375.90 ns
fn bench_batch_integer_decode_all_rle(c: &mut Criterion) {
    let benchmarks = vec![(5, 1), (55, 1), (555, 1), (1000, 1), (1000, 0)]; // (n, delta)

    let mut g = c.benchmark_group("bench_batch_integer_decode_all_rle");
    for (n, delta) in benchmarks {
        let mut enc = IntegerEncoder::new(n);
        let mut acc = 0i64;
        for _ in 0..n {
            enc.write(acc);
            acc += delta;
        }
        let bytes = enc.bytes().unwrap();

        g.bench_function(&format!("{}_delta_{}", n, delta), |b| {
            b.iter_with_large_drop(|| integer_array_decode_all(&bytes))
        });
    }
}

criterion_group!(
    benches,
    bench_batch_integer_encode_integers,
    bench_batch_integer_decode_all_uncompressed,
    bench_batch_integer_decode_all_packed_simple,
    bench_batch_integer_decode_all_rle
);
criterion_main!(benches);
