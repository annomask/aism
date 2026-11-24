use super::super::{AggValues, Record, SeqNo, writer::IndexFileWriter};
use super::*;
use crate::{
    models::{
        INTEGER,
        points::{IntegerValue, Value},
    },
    storage::tsm1::vlog::VLogIndex,
};

fn file(dir: &std::path::Path, generation: i64, sequence: i64) -> Arc<IndexFile> {
    let time = generation * 100 + sequence;
    let values = vec![Value::IntegerValue(IntegerValue::new(time, 1))];
    let record = Record::from_vlog_index(
        SeqNo::new(1, 1).unwrap(),
        &VLogIndex {
            aggregate: None,
            count: 1,
            typ: INTEGER,
            min: time,
            max: time,
            generation,
            seq: sequence,
            offset: 5,
            size: 30,
        },
        AggValues::from_block(&values).unwrap(),
    )
    .unwrap();
    let mut writer = IndexFileWriter::create(
        &dir.join(format!("{generation:09}-{sequence:09}.idx")),
        generation,
        sequence,
    )
    .unwrap();
    writer
        .write_segment(record.seq_no, INTEGER, &[(LEVEL_BLOCK, vec![record])])
        .unwrap();
    IndexFile::from_meta(writer.finish().unwrap()).unwrap()
}

fn planner(cold: Duration) -> IndexPlanner {
    IndexPlanner::new(cold, Arc::new(EngineStatistics::default()), 1)
}

#[test]
fn original_level_thresholds_count_generations_including_split_files() {
    for (level, count) in [(0, 8), (1, 8), (2, 4), (3, 4), (4, 4)] {
        let dir = tempfile::tempdir().unwrap();
        let mut files: Vec<_> = (1..count).map(|g| file(dir.path(), g, level)).collect();
        let mut planner = planner(Duration::ZERO);
        let now = Time::now();
        assert!(
            planner
                .select(&files, &HashSet::new(), u64::MAX, now, &|| false)
                .unwrap()
                .is_empty()
        );
        // More physical files within the same generation do not meet the threshold.
        files.push(file(dir.path(), 1, level + 1));
        assert!(
            planner
                .select(&files, &HashSet::new(), u64::MAX, now, &|| false)
                .unwrap()
                .is_empty()
        );
        files.push(file(dir.path(), count, level));
        let selected = planner
            .select(&files, &HashSet::new(), u64::MAX, now, &|| false)
            .unwrap();
        assert_eq!(selected.len(), count as usize + 1);
        let excluded = HashSet::from([files[count as usize - 1].file_no]);
        assert!(
            planner
                .select(&files, &excluded, u64::MAX, now, &|| false)
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn original_cold_policy_selects_below_threshold_and_budget_keeps_groups_whole() {
    let dir = tempfile::tempdir().unwrap();
    let files = vec![file(dir.path(), 1, 0), file(dir.path(), 2, 0)];
    let mut planner = planner(Duration::from_secs(1));
    let old = Time::from_timestamp(0, 0);
    let size = files.iter().map(|f| f.size()).sum::<u64>();
    assert!(
        planner
            .select(&files, &HashSet::new(), size - 1, old, &|| false)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        planner
            .select(&files, &HashSet::new(), size, old, &|| false)
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn promotion_survives_reopening_outputs_without_new_metadata_format() {
    let dir = tempfile::tempdir().unwrap();
    let mut files = vec![file(dir.path(), 1, 0), file(dir.path(), 2, 0)];
    for (generation, level) in [(3, 2), (4, 3), (5, 4), (6, 4)] {
        let (outputs, _) =
            super::super::compact::merge_files(dir.path(), generation, &files, &|| false).unwrap();
        assert_eq!(outputs[0].seq, level);
        files = outputs
            .into_iter()
            .map(|m| IndexFile::open(&m.path, m.generation, m.seq).unwrap())
            .collect();
    }
}

#[test]
fn original_planner_skips_max_size_blocks_without_spinning() {
    let mut stats = Vec::new();
    let mut counts = HashMap::new();
    for generation in 1..=4 {
        let path = PathBuf::from(format!("{generation:09}-000000004.idx"));
        stats.push(FileStat {
            path: path.clone(),
            size: super::super::super::compact::MAX_TSM_FILE_SIZE as u64,
            ..Default::default()
        });
        counts.insert(path, crate::config::DEFAULT_MAX_POINTS_PER_BLOCK as i32);
    }
    let planner = VLogDefaultPlanner::from_snapshot(stats, counts, Duration::ZERO);
    assert!(planner.plan(Time::now()).is_empty());
}
