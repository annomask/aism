use std::time::{Duration, Instant};

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use reefsdb::models::points::{FloatValue, Value};
use reefsdb::storage::tsm1::cache::Cache;

const FV_SIZE: u64 = 16u64; // 8 timestamp + 8 float value

// golang: 242.0 ns/op
// rust:   206.82 ns
fn bench_cache_float_entries(c: &mut Criterion) {
    let cache = Cache::new(50000000 * FV_SIZE + 4); // "test" -> 4bytes
    let mut values = vec![];
    for i in 0..50000000 {
        values.push(vec![Value::FloatValue(FloatValue::new(1, i as f64))]);
    }

    c.bench_function("bench_cache_float_entries", move |b| {
        b.iter_custom(|iters| {
            let mut exec_time = Duration::ZERO;
            for i in 0..iters {
                // if i >= 50000000 {
                //     break;
                // }
                let value = values[i as usize].clone();

                let start = Instant::now();
                let _ = black_box(cache.write("test".as_bytes(), value));
                exec_time += start.elapsed();
            }
            exec_time
        })
    });
}

// TODO: implement me
// fn bench_cache_parallel_float_entries(c: &mut Criterion) {
//     let n = num_cpus::get() as u64;
//     let c = 50000000 * n;
//     let cache = Arc::new(Cache::new(c * FV_SIZE * 10));
//     let mut values = vec![];
//     for i in 0..c {
//         let mut v = vec![];
//         for j in 0..10 {
//             v.push(Value::FloatValue(FloatValue::new(1, (i+j) as f64)));
//         }
//         values.push(Points {
//             key: format!("cpu{}", rand::thread_rng().gen_range(0..20)),
//             vals: v,
//         });
//     }

//     let values = Arc::new(values);
//     c.bench_function("bench_cache_float_entries", move |b| {
//         b.iter_custom(|iters| {
//             let wg = WaitGroup::new();
//             let i = Arc::new(std::sync::atomic::AtomicI32::new(-1));

//             let mut exec_time = Duration::ZERO;

//             for i in 0..iters {
//                 for j in 0..n {
//                     let values = Arc::clone(&values);
//                     let wg = wg.clone();
//                     wg.add(1);
//                     let i = Arc::clone(&i);
//                     let cache = Arc::clone(&cacle);
//                     std::thread::spawn(|| {

//                         loop {
//                             let k = i.fetch_add(1, Ordering::SeqCst) as usize;
//                             if k >= c {
//                                 break;
//                             }
//                             let v = values[k].clone();
//                             let start = Instant::now();
//                             let _ = black_box(cache.write("test".as_bytes(), value));
//                             exec_time += start.elapsed();
//                         }

//                         wg.done();
//                     })
//                 }
//             }

//             wg.wait();
//             exec_time
//         })
//     });
// }

criterion_group!(
    benches,
    bench_cache_float_entries,
    // bench_cache_parallel_float_entries,
    // bench_cache_entry_add
);
criterion_main!(benches);
