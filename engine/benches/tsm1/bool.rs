use criterion::{
    BenchmarkGroup, Criterion, criterion_group, criterion_main, measurement::WallTime,
};
use reefsdb::storage::tsm1::data_encoding::bool::{BoolDecoder, BoolEncoder};

// bench_bool_decoder_decode_all/size=1
// golang: 4.552 ns/op
// rust:   10.130 ns
// bench_bool_decoder_decode_all/size=55
// golang: 73.15 ns/op
// rust:   71.499 ns
// bench_bool_decoder_decode_all/size=555
// golang: 648.6 ns/op
// rust:   704.03 ns
// bench_bool_decoder_decode_all/size=1000
// golang: 1.146 µs/op
// rust:   1.1372 µs
fn bench_bool_decoder_decode_all(c: &mut Criterion) {
    let benchmarks = vec![1, 55, 555, 1000];

    let mut g = c.benchmark_group("string_decoder_decode_all");
    for size in benchmarks {
        let mut enc = BoolEncoder::new(size);
        for i in 0..size {
            enc.write(i & 1 == 1);
        }
        let bytes = enc.bytes().unwrap();
        run_bool_decoder_decode_all_benchmarks(&mut g, size, bytes);
    }
}

fn run_bool_decoder_decode_all_benchmarks(
    g: &mut BenchmarkGroup<WallTime>,
    size: i64,
    bytes: Vec<u8>,
) {
    let mut dst = Vec::with_capacity(size as usize);
    g.bench_function(&format!("size: {}", size), |b| {
        b.iter(|| {
            let mut dec = BoolDecoder::new();
            dec.set_bytes(&bytes).unwrap();
            let mut n = 0;
            for v in dec {
                dst.push(v);
                n += 1;
            }
            assert_eq!(
                size, n,
                "expected to read {} booleans, but read {}",
                size, n
            );
        })
    });
}

criterion_group!(benches, bench_bool_decoder_decode_all);
criterion_main!(benches);
