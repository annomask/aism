use super::*;

fn fixture(dir: &Path) -> VLogCompactInfo {
    let keys = (0..12).map(|n| format!("cpu,host={n:02}#!~#value").into_bytes())
        .collect::<Vec<_>>();
    let mut info = VLogCompactInfo::default();
    info.keys = keys.iter().map(|key| (key.clone(), FLOAT)).collect();
    info.indexes = vec![Vec::new(); keys.len()];
    for generation in 1..=2 {
        let blocks = keys.iter().map(|key| (key.as_slice(), generation, generation as f64))
            .collect::<Vec<_>>();
        let (reader, indexes) = tests::write_test_vlog_reader(dir, generation, 1, &blocks);
        info.files.push(reader.path().to_string_lossy().into_owned());
        info.readers.push(reader);
        for (position, key) in keys.iter().enumerate() {
            info.indexes[position].extend(indexes[std::str::from_utf8(key).unwrap()].clone());
        }
    }
    info
}

fn key_budget(info: &VLogCompactInfo) -> usize {
    info.indexes.iter().map(|indexes| {
        indexes.iter().map(|index| index.size as usize - 4).sum::<usize>()
    }).max().unwrap()
}

#[test]
fn bounded_prefetch_preserves_output_bytes_on_one_blocking_worker() {
    let dir = tempfile::tempdir().unwrap();
    let info = fixture(dir.path());
    let budget = key_budget(&info);
    let mut reference = futures::executor::block_on(VLogBatchKeyIter::new(
        1, true, None, info.clone(),
    )).unwrap();
    let expected = reference.by_ref().collect::<Vec<_>>();
    assert!(reference.err().is_none());
    assert_eq!(reference.prefetch_batches, 1);
    let runtime = tests::default_executor(1);
    let executor: ExecutorRef = runtime.clone();
    let mut iter = runtime.block_on(VLogBatchKeyIter::new_with_prefetch_limit(
        1, true, None, info, Arc::clone(&executor), budget, None,
    )).unwrap();
    let task = executor.spawn_blocking_compact(move || {
        let mut actual = Vec::new();
        while let Some(block) = iter.next() {
            let resident = iter.prefetched_blocks.as_ref().unwrap().iter()
                .flat_map(|blocks| blocks.iter()).map(|block| block.b.len()).sum::<usize>();
            assert!(resident <= budget);
            actual.push(block);
        }
        assert!(iter.err().is_none());
        assert_eq!(iter.prefetch_batches, 12);
        assert_eq!(iter.prefetch_peak_bytes, budget);
        actual
    }).unwrap();
    let actual = runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), task).await
    }).expect("refill must not wait on its own blocking pool").unwrap();
    assert_eq!(actual, expected);
}

#[test]
fn bounded_prefetch_rejects_oversized_key_before_reading() {
    let dir = tempfile::tempdir().unwrap();
    let info = fixture(dir.path());
    let budget = key_budget(&info) - 1;
    let hook = Arc::new(VLogReadTestHook::default());
    let result = futures::executor::block_on(VLogBatchKeyIter::new_with_prefetch_limit(
        1, true, None, info, legacy_global_executor(), budget, Some(Arc::clone(&hook)),
    ));
    assert!(matches!(result, Err(Error::StringErr { reason }) if reason.contains("exceeding prefetch limit")));
    assert_eq!(hook.started.load(Ordering::SeqCst), 0);
}

#[test]
fn bounded_prefetch_reports_later_read_failure() {
    let dir = tempfile::tempdir().unwrap();
    let info = fixture(dir.path());
    let budget = key_budget(&info);
    let path = info.files[0].clone();
    let mut iter = futures::executor::block_on(VLogBatchKeyIter::new_with_prefetch_limit(
        1, true, None, info, legacy_global_executor(), budget, None,
    )).unwrap();
    fs::OpenOptions::new().write(true).open(path).unwrap().set_len(0).unwrap();
    let blocks = iter.by_ref().collect::<Vec<_>>();
    assert_eq!(blocks.len(), 2, "only the already-prefetched key may be emitted");
    assert!(iter.err().is_some(), "late read failure must prevent output installation");
}

#[test]
fn bounded_prefetch_observes_cancellation_between_batches() {
    let dir = tempfile::tempdir().unwrap();
    let info = fixture(dir.path());
    let budget = key_budget(&info);
    let token = CancellationToken::new();
    let mut iter = futures::executor::block_on(VLogBatchKeyIter::new_with_prefetch_limit(
        1, true, Some(token.clone()), info, legacy_global_executor(), budget, None,
    )).unwrap();
    assert!(iter.next().is_some());
    assert!(iter.next().is_some());
    token.cancel();
    assert!(iter.next().is_none());
    assert!(iter.err().unwrap().contains("cancelled"));
}
