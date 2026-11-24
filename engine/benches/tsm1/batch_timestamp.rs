use criterion::{Criterion, criterion_group, criterion_main};
use rand::{Rng, RngCore, SeedableRng, rngs::StdRng};
use reefsdb::storage::tsm1::data_encoding::{
    batch_timestamp::{time_array_decode_all, time_array_encode_all},
    timestamp::TimeEncoder,
};

//  bench_batch_timestamp_encode_ints/10_seq_iter
// golang: 50.91 ns/op
// rust:   166.75 ns
//  bench_batch_timestamp_encode_ints/10_seq_batch
// golang: 26.96 ns/op
// rust:   37.129 ns
//  bench_batch_timestamp_encode_ints/10_random_iter
// golang: 51.94 ns/op
// rust:   227.44 ns
//  bench_batch_timestamp_encode_ints/10_random_batch
// golang: 14.86 ns/op
// rust:   36.819 ns
//  bench_batch_timestamp_encode_ints/10_dup_iter
// golang: 51.03 ns/op
// rust:   202.05 ns
//  bench_batch_timestamp_encode_ints/10_dup_batch
// golang: 18.48 ns/op
// rust:   37.938 ns
//  bench_batch_timestamp_encode_ints/100_seq_iter
// golang: 292.0 ns/op
// rust:   360.73 ns
//  bench_batch_timestamp_encode_ints/100_seq_batch
// golang: 95.69 ns/op
// rust:   137.92 ns
//  bench_batch_timestamp_encode_ints/100_random_iter
// golang: 1682 ns/op
// rust:   412.78 ns
//  bench_batch_timestamp_encode_ints/100_random_batch
// golang: 2048 ns/op
// rust:   138.09 ns
//  bench_batch_timestamp_encode_ints/100_dup_iter
// golang: 374.1 ns/op
// rust:   400.13 ns
//  bench_batch_timestamp_encode_ints/100_dup_batch
// golang: 91.91 ns/op
// rust:   136.84 ns
//  bench_batch_timestamp_encode_ints/1000_seq_iter
// golang: 2544 ns/op
// rust:   1.9458 µs
//  bench_batch_timestamp_encode_ints/1000_seq_batch
// golang: 636.9 ns/op
// rust:   1.2238 µs
//  bench_batch_timestamp_encode_ints/1000_random_iter
// golang: 25937 ns/op
// rust:   2.3940 µs
//  bench_batch_timestamp_encode_ints/1000_random_batch
// golang: 25974 ns/op
// rust:   1.1789 µs
//  bench_batch_timestamp_encode_ints/1000_dup_iter
// golang: 3707 ns/op
// rust:   2.4455 µs
//  bench_batch_timestamp_encode_ints/1000_dup_batch
// golang: 646.8 ns/op
// rust:   1.1907 µs
fn bench_batch_timestamp_encode_ints(c: &mut Criterion) {
    let cases = vec![10, 100, 1000];
    let mut g = c.benchmark_group("bench_batch_timestamp_encode_ints");
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
                let mut enc = TimeEncoder::new(n);
                for x in &seq_input {
                    enc.write(*x);
                }

                let b = enc.bytes().unwrap();
                // Since the batch encoder needs to do a copy to reset the
                // input, we will add a copy here too.
                input = b.to_vec();
            })
        });

        g.bench_function(&format!("{}_seq_batch", n), |b| {
            b.iter(|| {
                let mut values = seq_input.clone();
                input = time_array_encode_all(&mut values).unwrap();
            })
        });

        g.bench_function(&format!("{}_random_iter", n), |b| {
            b.iter(|| {
                let mut enc = TimeEncoder::new(n);
                for x in &random_input {
                    enc.write(*x);
                }

                let b = enc.bytes().unwrap();
                // Since the batch encoder needs to do a copy to reset the
                // input, we will add a copy here too.
                input = b.to_vec();
            })
        });

        g.bench_function(&format!("{}_random_batch", n), |b| {
            b.iter(|| {
                let mut values = random_input.clone();
                input = time_array_encode_all(&mut values).unwrap();
            })
        });

        g.bench_function(&format!("{}_dup_iter", n), |b| {
            b.iter(|| {
                let mut enc = TimeEncoder::new(n);
                for x in &dup_input {
                    enc.write(*x);
                }

                let b = enc.bytes().unwrap();
                // Since the batch encoder needs to do a copy to reset the
                // input, we will add a copy here too.
                input = b.to_vec();
            })
        });

        g.bench_function(&format!("{}_dup_batch", n), |b| {
            b.iter(|| {
                let mut values = dup_input.clone();
                input = time_array_encode_all(&mut values).unwrap();
            })
        });
    }
}

//  bench_batch_timestamp_decode_all_uncompressed/size_1
// golang: 6.173 ns/op
// rust:   12.758 ns
//  bench_batch_timestamp_decode_all_uncompressed/size_55
// golang: 51.08 ns/op
// rust:   43.759 ns
//  bench_batch_timestamp_decode_all_uncompressed/size_555
// golang: 427.5 ns/op
// rust:   404.98 ns
//  bench_batch_timestamp_decode_all_uncompressed/size_1000
// golang: 770.3 ns/op
// rust:   708.05 ns
fn bench_batch_timestamp_decode_all_uncompressed(c: &mut Criterion) {
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

    let mut g = c.benchmark_group("bench_batch_timestamp_decode_all_uncompressed");
    for size in benchmarks {
        let mut rng = StdRng::seed_from_u64((size * 1000) as u64);

        let mut enc = TimeEncoder::new(size);
        for _ in 0..size {
            enc.write(values[rng.next_u64() as usize % values.len()]);
        }
        let bytes = enc.bytes().unwrap();

        g.bench_function(&format!("size_{}", size), |b| {
            b.iter_with_large_drop(|| time_array_decode_all(&bytes).unwrap())
        });
    }
}

//  bench_batch_timestamp_decode_all_packed_simple/size_5
// golang: 10.07 ns/op
// rust:   15.325 ns
//  bench_batch_timestamp_decode_all_packed_simple/size_55
// golang: 44.40 ns/op
// rust:   52.260 ns
//  bench_batch_timestamp_decode_all_packed_simple/size_555
// golang: 419.9 ns/op
// rust:   541.06 ns
//  bench_batch_timestamp_decode_all_packed_simple/size_1000
// golang: 750.6 ns/op
// rust:   940.72 ns
fn bench_batch_timestamp_decode_all_packed_simple(c: &mut Criterion) {
    let benchmarks = vec![5, 55, 555, 1000];

    let mut g = c.benchmark_group(" bench_batch_timestamp_decode_all_packed_simple");
    for size in benchmarks {
        let mut rng = StdRng::seed_from_u64((size * 1000) as u64);

        let mut enc = TimeEncoder::new(size);
        for i in 0..size {
            // Small amount of randomness prevents RLE from being used
            enc.write((i * 1000) as i64 + rng.gen_range(0..10));
        }
        let bytes = enc.bytes().unwrap();

        g.bench_function(&format!("size_{}", size), |b| {
            b.iter_with_large_drop(|| time_array_decode_all(&bytes).unwrap())
        });
    }
}

// bench_batch_timestamp_decode_all_rle/5_delta_10
// golang: 6.208 ns/op
// rust:   14.078 ns
// bench_batch_timestamp_decode_all_rle/55_delta_10
// golang: 19.47 ns/op
// rust:   34.779 ns
// bench_batch_timestamp_decode_all_rle/555_delta_10
// golang: 118.5 ns/op
// rust:   274.80 ns
// bench_batch_timestamp_decode_all_rle/1000_delta_10
// golang: 215.6 ns/op
// rust:   486.18 ns
fn bench_batch_timestamp_decode_all_rle(c: &mut Criterion) {
    let benchmarks = vec![(5, 10), (55, 10), (555, 10), (1000, 10)]; // (n, delta)

    let mut g = c.benchmark_group("bench_batch_timestamp_decode_all_rle");
    for (n, delta) in benchmarks {
        let mut enc = TimeEncoder::new(n);
        let mut acc = 0i64;
        for _ in 0..n {
            enc.write(acc);
            acc += delta;
        }
        let bytes = enc.bytes().unwrap();

        g.bench_function(&format!("{}_delta_{}", n, delta), |b| {
            b.iter_with_large_drop(|| time_array_decode_all(&bytes))
        });
    }
}

criterion_group!(
    benches,
    bench_batch_timestamp_encode_ints,
    bench_batch_timestamp_decode_all_uncompressed,
    bench_batch_timestamp_decode_all_packed_simple,
    bench_batch_timestamp_decode_all_rle
);
criterion_main!(benches);
