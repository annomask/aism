use std::sync::Arc;

use criterion::{
    BenchmarkGroup, Criterion, criterion_group, criterion_main, measurement::WallTime,
};
use rand::Rng;
use reefsdb::common::wait_group::WaitGroup;
use reefsdb::models::points::{IntegerValue, Value};
use reefsdb::storage::tsm1::{cache::Error, data_encoding::encoding_values::Values, ring::Ring};
struct BenchMark {
    n: i32, // the number of partitions
    k: i32, // keys
}

fn must_new_ring(n: i32) -> Ring {
    let r = Ring::new(n);
    assert!(r.is_ok());
    r.unwrap()
}

fn run_ring_keys_benchmarks(
    g: &mut BenchmarkGroup<WallTime>,
    ring: Arc<Ring>,
    partitions: i32,
    keys: i32,
) {
    // Add some keys
    for i in 0..keys {
        let (_, _) = ring.write(
            format!("cpu,host=server-{} value=1", i),
            Values(vec![Value::IntegerValue(IntegerValue::new(1, i as i64))]),
        );
    }

    g.bench_function(&format!("partitions={}/keys={}", partitions, keys), |b| {
        b.iter_with_large_drop(|| {
            ring.keys(false);
        })
    });
}

fn bench_ring_keys(c: &mut Criterion) {
    let benchmarks = vec![
        // golang: 6.111 µs/op
        // rust:   3.2564 µs
        BenchMark { n: 16, k: 100 },
        // golang: 55.886 µs/op
        // rust:   48.707 µs
        BenchMark { n: 16, k: 1000 },
        // golang: 741.354 µs/op
        // rust:   496.54 µs
        BenchMark { n: 16, k: 10000 },
        // golang: 10.466932 ms/op
        // rust:   11.289 ms
        BenchMark { n: 16, k: 100000 },
    ];

    let mut g = c.benchmark_group("bench_ring_keys");
    for bm in benchmarks {
        run_ring_keys_benchmarks(&mut g, Arc::new(must_new_ring(bm.n)), bm.n, bm.k);
    }
}

fn run_ring_get_partition_benchmarks(
    g: &mut BenchmarkGroup<WallTime>,
    ring: Arc<Ring>,
    partitions: i32,
    keys: i32,
) {
    let mut values = vec![];
    // Add some keys
    for i in 0..keys {
        values.push(format!("cpu,host=server-{} field1=value1,field2=value2,field4=value4,field5=value5,field6=value6,field7=value7,field8=value1,field9=value2,field10=value4,field11=value5,field12=value6,field13=value7", i));
        let (_, _) = ring.write(
            format!("cpu,host=server-{} value=1", i),
            Values(vec![Value::IntegerValue(IntegerValue::new(1, i as i64))]),
        );
    }

    g.bench_function(&format!("partitions={}/keys={}", partitions, keys), |b| {
        b.iter(|| {
            let n = rand::thread_rng().gen_range(0..keys);
            let _ = ring.get_partition(values[(n % keys) as usize].as_bytes());
        })
    });
}

fn bench_ring_get_partition(c: &mut Criterion) {
    let benchmarks = vec![
        // golang: 20.26 ns/op
        // rust:   36.443 ns
        BenchMark { n: 16, k: 100 },
        // golang: 22.52 ns/op
        // rust:   37.144 ns
        BenchMark { n: 16, k: 1000 },
    ];

    let mut g = c.benchmark_group("bench_ring_get_partition");
    for bm in benchmarks {
        run_ring_get_partition_benchmarks(&mut g, Arc::new(must_new_ring(bm.n)), bm.n, bm.k);
    }
}

fn bench_ring_write(c: &mut Criterion) {
    let benchmarks = vec![
        // golang: 70.096 µs/op
        // rust:   154.33 µs
        BenchMark { n: 1, k: 100 },
        // golang: 559.866 µs/op
        // rust:   152.02 µs
        BenchMark { n: 1, k: 1000 },
        // golang: 62.805073 ms/op
        // rust:   153.40 µs
        BenchMark { n: 1, k: 10000 },
        // golang: 70.096 µs/op
        // rust:   154.06 µs
        BenchMark { n: 1, k: 100000 },
        // golang: 70.829 µs/op
        // rust:   154.33 µs
        BenchMark { n: 4, k: 100 },
        // golang: 532.853 µs/op
        // rust:   161.79 µs
        BenchMark { n: 4, k: 1000 },
        // golang: 5.557738 ms/op
        // rust:   157.51 µs
        BenchMark { n: 4, k: 10000 },
        // golang: 61.871038 ms/op
        // rust:   218.31 µs
        BenchMark { n: 4, k: 100000 },
        // golang: 70.670 µs/op
        // rust:   181.93 µs
        BenchMark { n: 16, k: 100 },
        // golang: 560.008 µs/op
        // rust:   163.97 µs
        BenchMark { n: 16, k: 1000 },
        // golang: 5.551536 ms/op
        // rust:   194.98 µs
        BenchMark { n: 16, k: 10000 },
        // golang: 63.809827 ms/op
        // rust:   215.86 µs
        BenchMark { n: 16, k: 100000 },
    ];

    let mut g = c.benchmark_group("bench_ring_write");
    for bm in benchmarks {
        run_ring_write_benchmarks(&mut g, Arc::new(must_new_ring(bm.n)), bm.n, bm.k);
    }
}

fn run_ring_write_benchmarks(
    g: &mut BenchmarkGroup<WallTime>,
    ring: Arc<Ring>,
    partitions: i32,
    keys: i32,
) {
    g.bench_function(&format!("partitions={}/keys={}", partitions, keys), |b| {
        b.iter(|| {
            let n = num_cpus::get() as i32;
            let wg = WaitGroup::new();
            for _ in 0..n {
                let ring = Arc::clone(&ring);
                let (s, r) = crossbeam_channel::bounded::<Option<Error>>(n as usize);

                let wg1 = wg.clone();
                wg1.add(1);
                std::thread::spawn(move || {
                    for j in 0..n {
                        if let (_created, Err(e)) =
                            ring.write(format!("cpu,host=server-{} value=1", j), Values(vec![]))
                        {
                            s.send(Some(e)).unwrap();
                        } else {
                            let _ = s.send(None);
                        }
                    }
                    wg1.done();
                });

                let wg2 = wg.clone();
                std::thread::spawn(move || {
                    wg2.wait();
                    // TODO: need to drop sender ?
                    // drop(s);
                });

                for _ in 0..n {
                    if let Ok(Some(e)) = r.recv() {
                        panic!("{:?}", e);
                    }
                }
            }
        })
    });
}

criterion_group!(
    benches,
    bench_ring_keys,
    bench_ring_get_partition,
    bench_ring_write
);
criterion_main!(benches);
