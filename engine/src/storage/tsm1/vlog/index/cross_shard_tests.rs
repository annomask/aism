use super::*;
use crate::models::points::{IntegerValue, Value};

fn leaves(generation: i64, offset: i64) -> Vec<LogicalRecord> {
    (0..70)
        .map(|i| {
            let points = [
                Value::IntegerValue(IntegerValue::new(i * 4, i)),
                Value::IntegerValue(IntegerValue::new(i * 4 + 2, i + 1)),
            ];
            let index = VLogIndex {
                aggregate: None,
                count: 2,
                typ: crate::INTEGER,
                min: i * 4,
                max: i * 4 + 2,
                generation,
                seq: 1,
                offset: offset + i * 32,
                size: 32,
            };
            logical(
                Record::from_vlog_index(
                    SeqNo::new(1, generation as u8).unwrap(),
                    &index,
                    AggValues::from_block(&points).unwrap(),
                )
                .unwrap(),
            )
        })
        .collect()
}

#[test]
fn cross_shard_logical_records_do_not_depend_on_block_locations() {
    let tree = LogicalTree::build(leaves(1, 5)).unwrap();
    assert!(tree.matches(&leaves(7, 99)));
    // Local field ordinals and physical locators are not global identities.
    assert_eq!(tree.levels[0], leaves(7, 99));
    let mut changed = leaves(7, 99);
    changed[69].values.count += 1;
    assert!(!tree.matches(&changed));
    let mut blocks = 0;
    let mut count = 0;
    tree.visit(
        TimeRange::new(0, 279),
        |r| {
            assert!(r.level > 1);
            count += r.values.count;
            Ok(())
        },
        |_| blocks += 1,
    )
    .unwrap();
    assert_eq!((count, blocks), (140, 0));
    let mut count = 0;
    let mut boundaries = Vec::new();
    tree.visit(
        TimeRange::new(1, 274),
        |r| {
            count += r.values.count;
            Ok(())
        },
        |r| boundaries.push(r),
    )
    .unwrap();
    assert_eq!(boundaries, vec![TimeRange::new(1, 2)]);
    assert_eq!(count, 136);
}
