use std::{collections::HashMap, sync::Arc};

use criterion::{Criterion, criterion_group, criterion_main};
use reefsdb::series::series_file::tests::must_open_series_file;

fn bench_series_file_compaction(c: &mut Criterion) {
    const N: i64 = 1_000_000;

    let rt = tokio::runtime::Runtime::new().unwrap();

    let sfile = rt.block_on(async {
        let sfile = must_open_series_file();

        // Generate a bounch of keys.
        let mut ids: Vec<u64> = vec![];
        for i in 0..N {
            let tmp = sfile
                .create_series_list_if_not_exists(
                    &["cpu".as_bytes()],
                    &[&reefsdb::models::points::Tags::from(HashMap::from([(
                        "region".as_bytes().to_vec(),
                        format!("r{}", i).into_bytes(),
                    )]))],
                )
                .unwrap();
            ids.extend_from_slice(&tmp);
        }

        // Delete a subset of keys.
        let mut i = 0;
        while i < ids.len() {
            assert!(sfile.delete_series_id(ids[i]).is_ok());
            i += 10;
        }

        sfile
    });

    let partitions = sfile.partitions().unwrap();
    c.bench_function("compact_series_file_with_1000000_series", move |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| async {
                // Compact all segments in parallel.
                let mut tasks = Vec::new();
                for p in &partitions {
                    let series = Arc::clone(&p.read().series);
                    let segments = p.read().segments();
                    for segment in segments {
                        let series = Arc::clone(&series);
                        tasks.push(tokio::task::spawn(async move {
                            let series = series.read();
                            let index = series.index.as_ref().unwrap();

                            let segment = segment.read();
                            let mut tmp_path = segment.path();
                            tmp_path.set_extension("tmp");
                            segment.compact_to_path(tmp_path, index)
                        }));
                    }
                }

                let errs = futures::future::join_all(tasks).await;
                errs.into_iter().for_each(|e| {
                    assert!(e.is_ok(), "{:?}", e); // task success
                    let e = e.unwrap();
                    assert!(e.is_ok(), "{:?}", e); // compact success
                });
            })
    });
}

// golang: 56 ms/op
// 49.503 ms
criterion_group!(benches, bench_series_file_compaction);
criterion_main!(benches);
