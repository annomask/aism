use super::*;
use super::super::{
    AggValues, BlockLoc, LEVEL_BLOCK, Position, Record, SeqNo,
    directory::{DIR_PER_PAGE, INDEX_PER_PAGE},
    reader::IndexFile,
    writer::IndexFileWriter,
};
use crate::models::{INTEGER, points::{IntegerValue, Value}};
use std::{fs::OpenOptions, io::{Seek, SeekFrom, Write}, path::Path};

fn seq(series: u64) -> SeqNo {
    SeqNo::new(series, 1).unwrap()
}

fn file(path: &Path, first: u64, count: usize, time: i64) -> Arc<IndexFile> {
    file_generation(path, first, count, time, 41)
}

fn file_generation(path: &Path, first: u64, count: usize, time: i64, generation: i64) -> Arc<IndexFile> {
    let mut writer = IndexFileWriter::create(path, generation, 1).unwrap();
    for series in first..first + count as u64 {
        let record = Record {
            seq_no: seq(series),
            level: LEVEL_BLOCK,
            pos: Position {
                start: time,
                end: time,
                block: BlockLoc::from_vlog(generation, 1, 5).unwrap(),
                block_size: 32,
                flags: 0,
                child_count: 0,
                version: 0,
            },
            values: AggValues::from_block(&[Value::IntegerValue(IntegerValue {
                unix_nano: time, value: series as i64,
            })]).unwrap(),
        };
        writer.write_segment(record.seq_no, INTEGER, &[(LEVEL_BLOCK, vec![record])]).unwrap();
    }
    IndexFile::from_meta(writer.finish().unwrap()).unwrap()
}

#[test]
fn alternating_files_reuse_bounded_directory_pages() {
    let dir = tempfile::tempdir().unwrap();
    let first = file_generation(&dir.path().join("first.idx"), 1, 1, 10, 41);
    let second = file_generation(&dir.path().join("second.idx"), 1, 1, 20, 42);
    let mut cache = QueryDirectoryCache::default();
    let mut decodes = 0;
    for _ in 0..8 {
        for source in [&first, &second] {
            let entry = source.lookup(seq(1)).unwrap().unwrap();
            cache.directory(source, 0, || { decodes += 1; Ok(vec![entry]) }).unwrap();
        }
    }
    assert_eq!(decodes, 2);
    assert!(cache.retained_bytes() < 128 * 1024);
}

#[test]
fn directory_file_lru_releases_old_views_and_bounds_full_pages() {
    let dir = tempfile::tempdir().unwrap();
    let mut cache = QueryDirectoryCache::default();
    let mut retired = Vec::new();
    for generation in 1..=20 {
        let source = file_generation(&dir.path().join(format!("{generation}.idx")), 1, 1, 10, generation);
        let entry = source.lookup(seq(1)).unwrap().unwrap();
        cache.directory(&source, 0, || Ok(vec![entry; DIR_PER_PAGE])).unwrap();
        cache.minima(&source, 0, || Ok(vec![1; INDEX_PER_PAGE])).unwrap();
        retired.push(Arc::downgrade(&source));
        assert!(cache.retained_bytes() < 128 * 1024);
    }
    assert_eq!(retired.iter().filter(|view| view.upgrade().is_some()).count(), 8);
    assert!(retired[0].upgrade().is_none());
}

#[test]
#[ignore = "paired metadata decode measurement; run optimized with --ignored --nocapture"]
fn aggregate_review_directory_costs() {
    let dir = tempfile::tempdir().unwrap();
    let files = [file_generation(&dir.path().join("a.idx"), 1, 64, 10, 41),
        file_generation(&dir.path().join("b.idx"), 1, 64, 20, 42)];
    let mut cache = QueryDirectoryCache::default();
    let start = std::time::Instant::now();
    #[cfg(feature = "slow-trace")]
    let scope = crate::common::slow_trace::QueryProfile::start().unwrap();
    for _ in 0..8 {
        for series in 1..=64 {
            for file in &files { std::hint::black_box(file.lookup_cached(seq(series), &mut cache).unwrap()); }
        }
    }
    println!("REVIEW_DIRECTORY elapsed_ms={} retained_bytes={}", start.elapsed().as_secs_f64()*1000.0, cache.retained_bytes());
    #[cfg(feature = "slow-trace")]
    println!("REVIEW_DIRECTORY_PROFILE {}", serde_json::to_string(&scope.finish()).unwrap());
}

#[test]
fn worker_directory_cache_reuses_adjacent_keys_and_crosses_pages() {
    let dir = tempfile::tempdir().unwrap();
    let file = file(&dir.path().join("adjacent.idx"), 1, DIR_PER_PAGE + 1, 10);
    let mut cache = QueryDirectoryCache::default();
    let first = file.lookup_cached(seq(1), &mut cache).unwrap();
    assert_eq!(first, file.lookup(seq(1)).unwrap());
    let page_address = cache.directory.as_ref().unwrap().1.as_ptr();
    for series in 2..=DIR_PER_PAGE as u64 {
        assert_eq!(file.lookup_cached(seq(series), &mut cache).unwrap(), file.lookup(seq(series)).unwrap());
        assert_eq!(cache.directory.as_ref().unwrap().1.as_ptr(), page_address);
    }
    let last = seq(DIR_PER_PAGE as u64 + 1);
    assert_eq!(file.lookup_cached(last, &mut cache).unwrap(), file.lookup(last).unwrap());
    assert_eq!(cache.directory.as_ref().unwrap().0, 1);
    assert!(cache.retained_bytes() < 16 * 1024);
}

#[test]
fn worker_directory_cache_uses_arc_identity_for_same_number_and_path() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("source.idx");
    let original = file(&path, 1, 1, 10);
    let reopened = IndexFile::open(&path, 41, 1).unwrap();
    assert_eq!(original.file_no, reopened.file_no);
    let mut cache = QueryDirectoryCache::default();
    original.lookup_cached(seq(1), &mut cache).unwrap();
    let retired = Arc::downgrade(&original);
    drop(original);
    assert!(retired.upgrade().is_some());
    reopened.lookup_cached(seq(1), &mut cache).unwrap();
    assert!(Arc::ptr_eq(cache.file.as_ref().unwrap(), &reopened));
    assert!(retired.upgrade().is_none());

    let replacement = file(&dir.path().join("replacement.idx"), 1, 1, 99);
    assert_eq!(reopened.file_no, replacement.file_no);
    assert_eq!(replacement.lookup_cached(seq(1), &mut cache).unwrap().unwrap().min_start, 99);
    assert!(Arc::ptr_eq(cache.file.as_ref().unwrap(), &replacement));
}

#[test]
fn out_of_range_source_does_not_evict_worker_directory_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let hot = file(&dir.path().join("hot.idx"), 1, 1, 10);
    let other = file(&dir.path().join("other.idx"), 500, 1, 20);
    let mut cache = QueryDirectoryCache::default();
    hot.lookup_cached(seq(1), &mut cache).unwrap();
    let page_address = cache.directory.as_ref().unwrap().1.as_ptr();
    assert!(other.lookup_cached(seq(1), &mut cache).unwrap().is_none());
    assert!(Arc::ptr_eq(cache.file.as_ref().unwrap(), &hot));
    assert_eq!(cache.directory.as_ref().unwrap().1.as_ptr(), page_address);
}

#[test]
fn worker_directory_cache_keeps_absolute_page_keys_and_bounded_boxes() {
    let dir = tempfile::tempdir().unwrap();
    let file = file(&dir.path().join("bounded.idx"), 1, 1, 10);
    let entry = file.lookup(seq(1)).unwrap().unwrap();
    let mut cache = QueryDirectoryCache::default();
    for page in [INDEX_PER_PAGE - 1, INDEX_PER_PAGE, INDEX_PER_PAGE * 2] {
        let mut loaded = false;
        cache.directory(&file, page, || { loaded = true; Ok(vec![entry.clone(); DIR_PER_PAGE]) }).unwrap();
        assert!(loaded);
        assert_eq!(cache.directory.as_ref().unwrap().0, page);
        cache.directory(&file, page, || panic!("directory page decoded twice")).unwrap();
    }
    for page in [0, 1] {
        cache.minima(&file, page, || Ok(vec![1; INDEX_PER_PAGE])).unwrap();
        cache.minima(&file, page, || panic!("directory minima decoded twice")).unwrap();
    }
    assert!(cache.retained_bytes() < 16 * 1024);
    assert_eq!(cache.minima.as_ref().unwrap().1.len(), INDEX_PER_PAGE);
    assert_eq!(cache.directory.as_ref().unwrap().1.len(), DIR_PER_PAGE);
}

#[test]
fn malformed_directory_first_load_is_rejected_without_admission() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("corrupt.idx");
    let file = file(&path, 1, 1, 10);
    let mut output = OpenOptions::new().write(true).open(&path).unwrap();
    output.seek(SeekFrom::Start(file.footer.dir_offset + 23)).unwrap();
    output.write_all(&[1]).unwrap();
    drop(output);
    let mut cache = QueryDirectoryCache::default();
    for _ in 0..2 {
        assert!(file.lookup_cached(seq(1), &mut cache).is_err());
        assert!(cache.directory.is_none());
    }
}

#[test]
fn cached_lookup_preserves_directory_minimum_relationship_validation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mismatch.idx");
    let file = file(&path, 1, DIR_PER_PAGE + 1, 10);
    let mut output = OpenOptions::new().write(true).open(&path).unwrap();
    output.seek(SeekFrom::Start(file.footer.page_index_offset + 8)).unwrap();
    output.write_all(&seq(DIR_PER_PAGE as u64).get().to_be_bytes()).unwrap();
    drop(output);
    let mut cache = QueryDirectoryCache::default();
    let last = seq(DIR_PER_PAGE as u64 + 1);
    let expected = file.lookup(last).unwrap_err().to_string();
    for _ in 0..2 {
        assert_eq!(file.lookup_cached(last, &mut cache).unwrap_err().to_string(), expected);
        assert!(cache.directory.is_none());
    }
}
