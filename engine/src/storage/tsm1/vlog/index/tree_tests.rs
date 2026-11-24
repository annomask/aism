use super::builder::TreeBuilder;
use super::tree::{Forest, join_ordered};
use super::*;
use crate::models::{
    INTEGER,
    points::{IntegerValue, Value},
};
use std::collections::BTreeMap;

fn leaf(time: i64) -> Record {
    let index = super::super::VLogIndex {
        aggregate: None,
        count: 1,
        typ: INTEGER,
        min: time,
        max: time,
        generation: 1,
        seq: 1,
        offset: 5 + time * 32,
        size: 32,
    };
    Record::from_vlog_index(
        SeqNo::new(1, 1).unwrap(),
        &index,
        AggValues::from_block(&[Value::IntegerValue(IntegerValue::new(time, time))]).unwrap(),
    )
    .unwrap()
}

fn build(start: i64, count: usize) -> (Forest, BTreeMap<(Level, i64), Record>) {
    let mut records = BTreeMap::new();
    let mut emit = |record: Record| {
        records.insert((record.level, record.pos.start), record);
        Ok(())
    };
    let mut builder = TreeBuilder::new();
    for time in start..start + count as i64 {
        builder.push(leaf(time), &mut emit).unwrap();
    }
    let forest = builder.finish(&mut emit).unwrap();
    (forest, records)
}

fn assert_tree(
    forest: &Forest,
    records: &BTreeMap<(Level, i64), Record>,
    start: i64,
    count: usize,
) {
    assert!(forest.roots.len() <= FAN_OUT);
    assert_eq!(forest.roots[0].pos.start, start);
    assert_eq!(
        forest.roots.last().unwrap().pos.end,
        start + count as i64 - 1
    );
    let mut visited = std::collections::BTreeSet::new();
    let mut todo = forest.roots.clone();
    while let Some(parent) = todo.pop() {
        assert!(
            visited.insert((parent.level, parent.pos.start)),
            "duplicate coverage"
        );
        assert_eq!(records[&(parent.level, parent.pos.start)], parent);
        if parent.level == LEVEL_BLOCK {
            continue;
        }
        let children = records
            .range((parent.level - 1, parent.pos.start)..=(parent.level - 1, parent.pos.end))
            .map(|(_, r)| *r)
            .collect::<Vec<_>>();
        assert!((MIN_CHILDREN..=FAN_OUT).contains(&children.len()));
        assert_eq!(Record::summarize(&children).unwrap(), parent);
        todo.extend(children);
    }
    assert_eq!(visited.len(), records.len(), "orphaned or missing records");
    assert_eq!(records.values().filter(|r| r.level == 1).count(), count);
    assert_eq!(
        forest.roots.iter().map(|r| r.values.count).sum::<u64>(),
        count as u64
    );
}

#[test]
fn flush_builder_covers_every_child_and_rebalances_only_the_suffix() {
    for count in [
        1, 6, 31, 32, 33, 63, 64, 65, 70, 95, 96, 100, 127, 128, 129, 2047, 2048, 4095, 4096, 4097,
        10000,
    ] {
        let (forest, records) = build(1, count);
        assert_tree(&forest, &records, 1, count);
    }
    let (forest, _) = build(1, 70);
    assert_eq!(
        forest
            .roots
            .iter()
            .map(|r| r.pos.child_count)
            .collect::<Vec<_>>(),
        [35, 35]
    );
    let (forest, _) = build(1, 100);
    assert_eq!(
        forest
            .roots
            .iter()
            .map(|r| (r.pos.start, r.pos.end))
            .collect::<Vec<_>>(),
        [(1, 64), (65, 100)]
    );
}

#[test]
fn flush_builder_stops_once_the_top_level_fits_the_fan_out() {
    for (count, level, roots) in [
        (1, 1, 1),
        (31, 1, 31),
        (32, 1, 32),
        (40, 1, 40),
        (64, 1, 64),
        (65, 2, 2),
        (2112, 2, 33),
        (4096, 2, 64),
        (4097, 3, 2),
    ] {
        let (forest, records) = build(1, count);
        assert_tree(&forest, &records, 1, count);
        assert_eq!(
            (forest.level(), forest.roots.len()),
            (level, roots),
            "{count} leaves"
        );
    }
}

#[test]
fn joining_complete_roots_never_loads_their_children() {
    let (left, mut records) = build(1, 100);
    let (right, other) = build(101, 100);
    records.extend(other);
    let plan = join_ordered(left, right, |_| panic!("stable roots must not be expanded")).unwrap();
    assert!(plan.removed.is_empty());
    assert!(plan.added.is_empty());
    assert_eq!(
        plan.forest
            .roots
            .iter()
            .map(|r| (r.pos.start, r.pos.end))
            .collect::<Vec<_>>(),
        [(1, 64), (65, 100), (101, 164), (165, 200)]
    );
    assert_tree(&plan.forest, &records, 1, 200);
}

#[test]
fn joins_preserve_complete_coverage_across_heights_and_repeated_appends() {
    for counts in [
        vec![64, 6],
        vec![6, 64],
        vec![4096, 6],
        vec![6, 4096],
        vec![4096, 70, 5000],
        vec![1; 260],
        vec![100; 70],
        vec![31, 2048, 31, 1, 2048, 1],
    ] {
        let (mut forest, mut records) = build(1, counts[0]);
        let mut total = counts[0];
        for &count in &counts[1..] {
            let (right, extra) = build(total as i64 + 1, count);
            records.extend(extra);
            let old_height = forest.level().max(right.level());
            let mut loads = 0;
            let plan = join_ordered(forest, right, |parent| {
                loads += 1;
                Ok(records
                    .range(
                        (parent.level - 1, parent.pos.start)..=(parent.level - 1, parent.pos.end),
                    )
                    .map(|(_, r)| *r)
                    .collect())
            })
            .unwrap();
            assert!(loads < old_height as usize);
            for parent in plan.removed {
                records.remove(&(parent.level, parent.pos.start)).unwrap();
            }
            for node in plan.added {
                records.insert((node.record.level, node.record.pos.start), node.record);
            }
            forest = plan.forest;
            total += count;
            assert_tree(&forest, &records, 1, total);
        }
    }
}

#[test]
fn tree_join_rejects_overlaps_incomplete_children_and_capacity_overflow() {
    let (left, _) = build(1, 65);
    let (overlap, _) = build(65, 6);
    assert!(
        join_ordered(left.clone(), overlap, |_| panic!(
            "overlap must fail before reading"
        ))
        .is_err()
    );
    let (right, _) = build(66, 6);
    assert!(join_ordered(left, right, |_| Ok(vec![leaf(1)])).is_err());
    let mut roots = (1..=65).map(leaf).collect::<Vec<_>>();
    for root in &mut roots {
        root.level = MAX_LEVELS;
        root.pos.child_count = 32;
        root.pos.block = BlockLoc::default();
    }
    let right = Forest::new(roots.split_off(64)).unwrap();
    assert!(join_ordered(Forest::new(roots).unwrap(), right, |_| panic!()).is_err());
}

#[test]
fn builder_rejects_out_of_order_or_failed_emission() {
    let mut builder = TreeBuilder::new();
    builder.push(leaf(1), &mut |_| Ok(())).unwrap();
    assert!(builder.push(leaf(1), &mut |_| Ok(())).is_err());
    assert!(builder.finish(&mut |_| Ok(())).is_err());
    let mut builder = TreeBuilder::new();
    assert!(
        builder
            .push(leaf(1), &mut |_| Err(invalid("injected sink failure")))
            .is_err()
    );
    assert!(builder.finish(&mut |_| Ok(())).is_err());
}
