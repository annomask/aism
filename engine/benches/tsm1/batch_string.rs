use criterion::{Criterion, criterion_group, criterion_main};
use rand::{SeedableRng, rngs::StdRng};
use reefsdb::storage::tsm1::data_encoding::{
    batch_string::{string_array_decode_all, string_array_encode_all},
    string::StringEncoder,
};
use uuid::Uuid;

#[path = "../support/string.rs"]
mod string_support;

// bench_batch_string_encode_strings/10_iter
// golang: 403.2 ns/op
// rust:   367.17 ns
// bench_batch_string_encode_strings/10_batch
// golang: 199.7 ns/op
// rust:   167.25 ns
// bench_batch_string_encode_strings/100_iter
// golang: 3.313 µs/op
// rust:   3.3708 µs
// bench_batch_string_encode_strings/100_batch
// golang: 1.915 µs/op
// rust:   824.06 ns
// bench_batch_string_encode_strings/1000_iter
// golang: 33.399 µs/op
// rust:   29.723 µs
// bench_batch_string_encode_strings/1000_batch
// golang: 19.133 µs/op
// rust:   5.2343 µs
fn bench_batch_string_encode_strings(c: &mut Criterion) {
    let cases = vec![10, 100, 1000];
    let mut g = c.benchmark_group("bench_batch_string_encode_strings");
    for n in cases {
        let mut input = vec![];
        for _ in 0..n {
            input.push(Uuid::new_v4().to_string());
        }

        g.bench_function(&format!("{}_iter", n), |b| {
            b.iter(|| {
                let mut enc = StringEncoder::new(n);
                for x in &input {
                    enc.write(x.as_bytes());
                }
                enc.flush();

                assert!(enc.bytes().is_ok());
            })
        });

        g.bench_function(&format!("{}_batch", n), |b| {
            b.iter(|| {
                assert!(string_array_encode_all(&input).is_ok());
            })
        });
    }
}

// bench_batch_string_decode_all/size_1
// golang: 43.51 ns/op
// rust:   149.81 ns
// bench_batch_string_decode_all/size_55
// golang: 1.867 µs/op
// rust:   2.6013 µs
// bench_batch_string_decode_all/size_550
// golang: 15.825 µs/op
// rust:   33.047 µs
// bench_batch_string_decode_all/size_1000
// golang: 45.989 µs/op
// rust:   59.929 µs
fn bench_batch_string_decode_all(c: &mut Criterion) {
    let cases = vec![(1, 10), (55, 10), (550, 10), (1000, 10)];
    for (n, w) in cases {
        let mut rng = StdRng::seed_from_u64((n * 1000) as u64);

        let mut enc = StringEncoder::new(n);
        for _ in 0..n {
            enc.write(string_support::make_sentence(w, &mut rng).as_bytes());
        }
        enc.flush();
        let bytes = enc.bytes().unwrap();

        c.bench_function(&format!("bench_batch_string_decode_all/size_{}", n), |b| {
            b.iter(|| {
                let got = string_array_decode_all(&bytes).unwrap();
                assert_eq!(
                    n,
                    got.len() as i64,
                    "unexpected length, exp {}, got {}",
                    n,
                    got.len()
                );
            })
        });
    }
}

criterion_group!(
    benches,
    bench_batch_string_encode_strings,
    bench_batch_string_decode_all,
);
criterion_main!(benches);
