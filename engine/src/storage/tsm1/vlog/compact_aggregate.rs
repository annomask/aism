//! AISM metadata on the existing VLog block merger. The copy, decode, dedup,
//! chunking and encoding decisions remain in VLogBatchKeyIter.

use super::super::index::{AggValues, Result as IndexResult};
use super::*;
use crate::models::points::{BoolValue, IntegerValue, StringValue, UnsignedValue};

pub(super) trait SummarizeArray {
    fn summarize(&self) -> IndexResult<AggValues>;
}

macro_rules! array_summary {
    ($array:ty, $value:ident) => {
        impl SummarizeArray for $array {
            fn summarize(&self) -> IndexResult<AggValues> {
                let values = self
                    .timestamps
                    .iter()
                    .zip(&self.values)
                    .map(|(&time, value)| Value::$value($value::new(time, value.clone())))
                    .collect::<Vec<_>>();
                AggValues::from_block(&values)
            }
        }
    };
}
array_summary!(IntegerArray, IntegerValue);
array_summary!(UnsignedArray, UnsignedValue);
array_summary!(FloatArray, FloatValue);
array_summary!(BoolArray, BoolValue);
array_summary!(StringArray, StringValue);

pub(super) fn summarize(
    enabled: bool,
    values: &impl SummarizeArray,
) -> IndexResult<Option<Arc<AggValues>>> {
    if enabled {
        values.summarize().map(Arc::new).map(Some)
    } else {
        Ok(None)
    }
}

#[cfg(test)]
fn block_iterator(blocks: Blocks, typ: u8, fast: bool, collect: bool) -> VLogBatchKeyIter {
    VLogBatchKeyIter {
        collect_aggregates: collect,
        aggregate_decodes: 0,
        input_aggregates: None,
        output_aggregates: HashMap::new(),
        readers: Vec::new(),
        vlog_indexes: Vec::new(),
        keys: vec![(Vec::new(), typ)],
        pos: 0,
        errs: VLogErrors(Vec::new()),
        fast,
        size: DEFAULT_MAX_POINTS_PER_BLOCK,
        current_tsm: String::new(),
        blocks: Blocks::default(),
        merged_float_values: FloatArray::default(),
        merged_integer_values: IntegerArray::default(),
        merged_unsigned_values: UnsignedArray::default(),
        merged_bool_values: BoolArray::default(),
        merged_string_values: StringArray::default(),
        merged: Blocks::default(),
        interrupt: None,
        prefetched_blocks: Some(vec![blocks]),
        prefetch_start: 0,
        prefetch_end: 1,
        prefetch_limit: VLOG_PREFETCH_BYTES,
        prefetch_peak_bytes: 0,
        prefetch_batches: 0,
        total_spawned_tasks: 0,
        executor: legacy_global_executor(),
        #[cfg(test)]
        read_test_hook: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::tsm1::{
        data_encoding::{encoding::decode_block, encoding_values::Values},
        vlog::{VLogIndex, index::{SeqNo, Record}},
    };

    fn points(typ: u8, start: i64, count: usize) -> Vec<Value> {
        (start..start + count as i64)
            .map(|t| match typ {
                INTEGER => Value::IntegerValue(IntegerValue::new(t, t)),
                UNSIGNED => Value::UnsignedValue(UnsignedValue::new(t, t as u64)),
                FLOAT => Value::FloatValue(FloatValue::new(t, t as f64 / 4.0)),
                BOOL => Value::BoolValue(BoolValue::new(t, t % 3 == 0)),
                STRING => Value::StringValue(StringValue::new(t, format!("v{t}"))),
                _ => unreachable!(),
            })
            .collect()
    }

    #[test]
    fn data_compaction_matches_original_block_bytes_and_decode_decisions() {
        // Full blocks and a lone tail copy unchanged. Only the original slow
        // merge combines small blocks; overlaps/deletes still decode in fast mode.
        let cases = [
            (vec![(0, 4000), (4000, 37)], false, false, 0),
            (vec![(0, 37)], false, false, 0),
            (vec![(0, 37), (37, 53)], true, false, 0),
            (vec![(0, 37), (37, 53)], false, false, 2),
            (vec![(0, 3000), (3000, 3000)], false, false, 2),
            (vec![(0, 37), (20, 53)], true, false, 3),
            (vec![(0, 37), (37, 53)], true, true, 2),
        ];
        for typ in [INTEGER, UNSIGNED, FLOAT, BOOL, STRING] {
            for (ranges, fast, deleting, decodes) in &cases {
                let mut inputs = Vec::new();
                let mut legacy = Blocks::default();
                for &(start, count) in ranges {
                    let values = points(typ, start, count);
                    let bytes = Values::encode(&values).unwrap();
                    let record = Record::from_vlog_index(
                        SeqNo::new(1, 1).unwrap(),
                        &VLogIndex {
                            aggregate: None,
                            count: count as i32,
                            typ,
                            min: start,
                            max: start + count as i64 - 1,
                            generation: 1,
                            seq: 1,
                            offset: 5,
                            size: bytes.len() as i64 + 4,
                        },
                        AggValues::from_block(&values).unwrap(),
                    )
                    .unwrap();
                    let tombstones = if *deleting {
                        vec![TimeRange { min: 10, max: 40 }]
                    } else {
                        Vec::new()
                    };
                    legacy.push(Block {
                        count,
                        key: Vec::new(),
                        min_time: record.pos.start,
                        max_time: record.pos.end,
                        typ,
                        b: bytes.clone(),
                        tombstones: tombstones.clone(),
                        read_min: i64::MAX,
                        read_max: i64::MIN,
                        ..Default::default()
                    });
                    inputs.push((record, bytes, tombstones));
                }
                let legacy: Vec<_> = block_iterator(legacy, typ, *fast, false)
                    .map(|(count, typ, _, min, max, bytes, error)| {
                        assert!(error.is_none(), "{error:?}");
                        (count, typ, min, max, bytes)
                    })
                    .collect();
                let mut actual = Vec::new();
                let mut blocks = Blocks::default();
                for (record, bytes, tombstones) in &inputs {
                    blocks.push(Block {
                        count: record.values.count as usize, typ,
                        min_time: record.pos.start, max_time: record.pos.end,
                        b: bytes.clone(), tombstones: tombstones.clone(),
                        read_min: i64::MAX, read_max: i64::MIN,
                        aggregate: Some(Arc::new(record.values)), aggregate_copied: true,
                        ..Default::default()
                    });
                }
                let mut iter = block_iterator(blocks, typ, *fast, true);
                let mut copied = 0;
                while iter.has_next() {
                    let block = iter.merged.remove(0);
                    let values = decode_block(&block.b).unwrap();
                    assert_eq!(*block.aggregate.unwrap(), AggValues::from_block(&values).unwrap());
                    copied += usize::from(block.aggregate_copied);
                    actual.push((
                        block.count as i32,
                        typ,
                        block.min_time,
                        block.max_time,
                        block.b,
                    ));
                }
                assert!(iter.err().is_none());
                assert_eq!(
                    actual, legacy,
                    "type {typ}, fast={fast}, deletes={deleting}"
                );
                assert_eq!(
                    iter.aggregate_decodes, *decodes,
                    "type {typ}, fast={fast}, deletes={deleting}"
                );
                if *decodes == 0 {
                    assert_eq!(copied, inputs.len());
                    assert_eq!(
                        actual.iter().map(|b| &b.4).collect::<Vec<_>>(),
                        inputs.iter().map(|b| &b.1).collect::<Vec<_>>()
                    );
                }
            }
        }
    }
}
