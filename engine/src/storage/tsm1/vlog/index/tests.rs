use super::*;
use crate::models::points::{
    BoolValue, FloatValue, IntegerValue, StringValue, UnsignedValue, Value,
};

#[test]
fn sequence_and_location_packing_checks_boundaries() {
    for (id, ordinal) in [(1, 1), (12, 255), (u64::MAX >> 8, 255)] {
        let key = SeqNo::new(id, ordinal).unwrap();
        assert_eq!(key.series_id(), id);
        assert_eq!(key.field_ordinal(), ordinal);
        assert_eq!(SeqNo::from_raw(key.get()).unwrap(), key);
    }
    for (id, ordinal) in [(0, 1), (1, 0), (1 << 56, 1)] {
        assert!(SeqNo::new(id, ordinal).is_err());
    }
    let loc = BlockLoc::from_vlog((1 << 24) - 1, 255, i64::from(u32::MAX)).unwrap();
    assert_eq!(loc.generation(), (1 << 24) - 1);
    assert_eq!(loc.seq(), 255);
    for (generation, seq, offset) in [
        (0, 0, 5),
        (1 << 24, 0, 5),
        (1, 256, 5),
        (1, -1, 5),
        (1, 0, -1),
        (1, 0, 1 << 32),
    ] {
        assert!(BlockLoc::from_vlog(generation, seq, offset).is_err());
    }
}

fn integers(values: &[i64]) -> Vec<Value> {
    values
        .iter()
        .enumerate()
        .map(|(time, &value)| {
            Value::IntegerValue(IntegerValue {
                unix_nano: time as i64,
                value,
            })
        })
        .collect()
}

#[test]
fn aggregate_components_are_partition_invariant() {
    let data = integers(&[i64::MAX, i64::MAX, -1, i64::MIN, 7, 7, -1, 0]);
    let expected = AggValues::from_block(&data).unwrap();
    assert_eq!(expected.sum, AggSum::Int(i128::from(i64::MAX) - 1 + 12));
    for split in 1..data.len() {
        let merged = AggValues::from_block(&data[..split])
            .unwrap()
            .merge_ordered(AggValues::from_block(&data[split..]).unwrap())
            .unwrap();
        assert_eq!(merged, expected);
    }
    assert_eq!(expected.min_at, 3);
    assert_eq!(expected.max_at, 0);
}

#[test]
fn unsigned_boolean_and_string_aggregates_preserve_semantics() {
    let unsigned = [0, u64::MAX, u64::MAX]
        .into_iter()
        .enumerate()
        .map(|(time, value)| {
            Value::UnsignedValue(UnsignedValue {
                unix_nano: time as i64,
                value,
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(
        AggValues::from_block(&unsigned).unwrap().sum,
        AggSum::Uint(2 * u128::from(u64::MAX))
    );
    let boolean = [true, false, true]
        .into_iter()
        .enumerate()
        .map(|(time, value)| {
            Value::BoolValue(BoolValue {
                unix_nano: time as i64,
                value,
            })
        })
        .collect::<Vec<_>>();
    let values = AggValues::from_block(&boolean).unwrap();
    assert_eq!(values.sum, AggSum::Uint(2));
    assert_eq!(values.min_at, 1);
    assert_eq!(values.max_at, 0);
    let strings = [Value::StringValue(StringValue {
        unix_nano: 0,
        value: "hello".into(),
    })];
    let values = AggValues::from_block(&strings).unwrap();
    assert_eq!(values.count, 1);
    assert_eq!(values.first, AggScalar::None);
    assert_eq!(values.sum, AggSum::CountOnly);
}

#[test]
fn float_statistics_use_total_order_and_bounded_roundoff() {
    let data = (0..257)
        .map(|time| {
            Value::FloatValue(FloatValue {
                unix_nano: time,
                value: (time as f64 - 128.0) / 7.0,
            })
        })
        .collect::<Vec<_>>();
    let expected = AggValues::from_block(&data).unwrap();
    for split in 1..data.len() {
        let merged = AggValues::from_block(&data[..split])
            .unwrap()
            .merge_ordered(AggValues::from_block(&data[split..]).unwrap())
            .unwrap();
        assert_eq!(merged.count, expected.count);
        assert_eq!(merged.min, expected.min);
        assert_eq!(merged.max, expected.max);
        assert_eq!(merged.first, expected.first);
        assert_eq!(merged.last, expected.last);
        let (AggSum::Float(actual), AggSum::Float(want)) = (merged.sum, expected.sum) else {
            unreachable!()
        };
        assert!((actual - want).abs() < 1e-10);
        assert!((merged.sumsq - expected.sumsq).abs() < 1e-8);
    }
    let data = [-0.0, 0.0, f64::INFINITY, f64::NAN]
        .into_iter()
        .enumerate()
        .map(|(time, value)| {
            Value::FloatValue(FloatValue {
                unix_nano: time as i64,
                value,
            })
        })
        .collect::<Vec<_>>();
    let values = AggValues::from_block(&data).unwrap();
    assert_eq!(values.min_at, 0);
    assert_eq!(values.max_at, 3);
    assert!(matches!(values.sum, AggSum::Float(sum) if sum.is_nan()));
}

#[test]
fn malformed_inputs_and_arithmetic_overflow_are_rejected() {
    assert!(AggValues::from_block(&[]).is_err());
    let mut data = integers(&[1, 2]);
    data.reverse();
    assert!(matches!(
        AggValues::from_block(&data),
        Err(Error::InvalidTimeOrder)
    ));
    let mut data = integers(&[1]);
    data.push(Value::BoolValue(BoolValue {
        unix_nano: 1,
        value: true,
    }));
    assert!(matches!(
        AggValues::from_block(&data),
        Err(Error::TypeMismatch)
    ));
    let mut values = AggValues::from_block(&integers(&[1])).unwrap();
    values.sum = AggSum::Int(i128::MAX);
    assert!(matches!(
        values.merge_ordered(AggValues::from_block(&integers(&[1])).unwrap()),
        Err(Error::Overflow)
    ));
}

#[test]
fn vlog_records_round_trip_and_complete_groups_summarize() {
    let seq_no = SeqNo::new(12, 1).unwrap();
    let children = (0..64)
        .map(|time| {
            let data = [Value::IntegerValue(IntegerValue {
                unix_nano: time,
                value: time,
            })];
            let idx = VLogIndex {
                aggregate: None,
                count: 1,
                typ: crate::models::INTEGER,
                min: time,
                max: time,
                generation: 1,
                seq: 1,
                offset: 5 + time * 32,
                size: 32,
            };
            let record =
                Record::from_vlog_index(seq_no, &idx, AggValues::from_block(&data).unwrap())
                    .unwrap();
            assert_eq!(record.to_vlog_index().unwrap(), idx);
            record
        })
        .collect::<Vec<_>>();
    let summary = Record::summarize(&children).unwrap();
    assert_eq!(summary.level, 2);
    assert_eq!(summary.pos.start, 0);
    assert_eq!(summary.pos.end, 63);
    assert_eq!(summary.values.count, 64);
    assert_eq!(summary.values.sum, AggSum::Int(2016));
    assert_eq!(summary.pos.block, BlockLoc::default());
    assert!(summary.to_vlog_index().is_err());
    assert_eq!(summary.pos.child_count, 64);
    assert_eq!(
        Record::summarize(&children[..32]).unwrap().pos.child_count,
        32
    );
    assert_eq!(
        Record::summarize(&children[..63]).unwrap().pos.child_count,
        63
    );
    assert!(Record::summarize(&children[..31]).is_err());
    let mut invalid_first = children.clone();
    invalid_first[0].pos.end = -1;
    assert!(Record::summarize(&invalid_first).is_err());
    let mut overlap = children;
    overlap[1].pos.start = 0;
    assert!(Record::summarize(&overlap).is_err());
}
