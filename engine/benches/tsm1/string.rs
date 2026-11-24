use criterion::{
    BenchmarkGroup, Criterion, criterion_group, criterion_main, measurement::WallTime,
};
use rand::{SeedableRng, rngs::StdRng};
use reefsdb::storage::tsm1::data_encoding::string::{StringDecoder, StringEncoder};

#[path = "../support/string.rs"]
mod string_support;

// bench_string_decoder_decode_all/1
// go:   73.33 ns/op
// rust: 1.9451 µs
// bench_string_decoder_decode_all/55
// go:   3213 ns/op
// rust: 5.6624 µs
// bench_string_decoder_decode_all/550
// go:   38536 ns/op
// rust: 42.666 µs
// bench_string_decoder_decode_all/1000
// go:   78211 ns/op
// rust: 78.050 µs
fn bench_string_decoder_decode_all(c: &mut Criterion) {
    struct BenchMark {
        n: i32,
        w: i32,
    }

    let benchmarks = vec![
        BenchMark { n: 1, w: 10 },
        BenchMark { n: 55, w: 10 },
        BenchMark { n: 550, w: 10 },
        BenchMark { n: 1000, w: 10 },
    ];

    let mut g = c.benchmark_group("string_decoder_decode_all");
    for bm in benchmarks {
        let mut rng = StdRng::seed_from_u64((bm.n * 1000) as u64);

        let mut enc = StringEncoder::new(bm.n.into());
        for _ in 0..bm.n {
            enc.write(string_support::make_sentence(bm.w, &mut rng).as_bytes());
        }
        enc.flush();

        let bytes = enc.bytes().unwrap();

        run_string_decoder_decode_all_benchmarks(&mut g, bm.n, &bytes);
    }
}

fn run_string_decoder_decode_all_benchmarks(
    g: &mut BenchmarkGroup<WallTime>,
    len: i32,
    bytes: &[u8],
) {
    g.bench_function(&format!("len: {}", len), |b| {
        b.iter(|| {
            let mut dst = Vec::<String>::with_capacity(len as usize);

            let mut dec = StringDecoder::new();
            let _ = dec.set_bytes(bytes).unwrap();

            while let Some(v) = dec.next() {
                dst.push(v);
            }

            assert_eq!(
                dst.len(),
                len as usize,
                "unexpected length, got {}, exp {}",
                dst.len(),
                len
            );
        })
    });
}

criterion_group!(benches, bench_string_decoder_decode_all);
criterion_main!(benches);
