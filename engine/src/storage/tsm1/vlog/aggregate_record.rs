//! Optional aggregate fields of a native VLog locator. The locator WAL and
//! cache own their visibility; index files do not have a second commit log.

use super::index::{AggScalar, AggSum, AggValues, Result, SeqNo, invalid};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Clone, Debug)]
pub(crate) struct AggregateRecord {
    pub file: u32,
    pub seq: SeqNo,
    pub version: u32,
    pub values: AggValues,
}

impl AggregateRecord {
    pub const SIZE: usize = 101;

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend(self.file.to_be_bytes());
        out.extend(self.seq.get().to_be_bytes());
        out.extend(self.values.count.to_be_bytes());
        let (tag, value) = match self.values.sum {
            AggSum::Int(v) => (0, v as u128),
            AggSum::Uint(v) => (1, v),
            AggSum::Float(v) => (2, v.to_bits() as u128),
            AggSum::CountOnly => (3, 0),
        };
        out.push(tag);
        out.extend(value.to_be_bytes());
        out.extend(self.values.sumsq.to_bits().to_be_bytes());
        for scalar in [
            self.values.min,
            self.values.max,
            self.values.first,
            self.values.last,
        ] {
            let (tag, value) = match scalar {
                AggScalar::Int(v) => (0, v as u64),
                AggScalar::Uint(v) => (1, v),
                AggScalar::Float(v) => (2, v.to_bits()),
                AggScalar::Bool(v) => (3, u64::from(v)),
                AggScalar::None => (4, 0),
            };
            out.push(tag);
            out.extend(value.to_be_bytes());
        }
        out.extend(self.values.min_at.to_be_bytes());
        out.extend(self.values.max_at.to_be_bytes());
        out.extend(self.version.to_be_bytes());
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != Self::SIZE {
            return Err(invalid("invalid aggregate locator length"));
        }
        let u64_at = |i| u64::from_be_bytes(bytes[i..i + 8].try_into().unwrap());
        let value = u128::from_be_bytes(bytes[21..37].try_into().unwrap());
        let sum = match bytes[20] {
            0 => AggSum::Int(value as i128),
            1 => AggSum::Uint(value),
            2 if value <= u64::MAX as u128 => AggSum::Float(f64::from_bits(value as u64)),
            3 if value == 0 => AggSum::CountOnly,
            _ => return Err(invalid("invalid aggregate locator sum")),
        };
        let scalar = |i| -> Result<AggScalar> {
            let value = u64_at(i + 1);
            Ok(match bytes[i] {
                0 => AggScalar::Int(value as i64),
                1 => AggScalar::Uint(value),
                2 => AggScalar::Float(f64::from_bits(value)),
                3 if value <= 1 => AggScalar::Bool(value == 1),
                4 if value == 0 => AggScalar::None,
                _ => return Err(invalid("invalid aggregate locator scalar")),
            })
        };
        let file = u32::from_be_bytes(bytes[..4].try_into().unwrap());
        if file == 0 {
            return Err(invalid("invalid aggregate locator file"));
        }
        Ok(Self {
            file,
            seq: SeqNo::from_raw(u64_at(4))?,
            version: u32::from_be_bytes(bytes[97..101].try_into().unwrap()),
            values: AggValues {
                count: u64_at(12),
                sum,
                sumsq: f64::from_bits(u64_at(37)),
                min: scalar(45)?,
                max: scalar(54)?,
                first: scalar(63)?,
                last: scalar(72)?,
                min_at: u64_at(81) as i64,
                max_at: u64_at(89) as i64,
            },
        })
    }
}

/// Derived membership of immutable hierarchy files, updated inside the same
/// entry lock that changes native locators. Replay executes those same updates.
#[derive(Clone, Default, Debug)]
pub(crate) struct AggregateSource {
    pub live: usize,
    pub removed: Vec<(i64, i64)>,
    pub blocks: BlockMembership,
}
pub(crate) type AggregateSources = BTreeMap<u32, Arc<AggregateSource>>;

type BlockKey = (i64, i64, i64);
type BlockRange = (i64, i64, usize);

/// Snapshot files commonly contain one block per series field. Keep that
/// membership inline instead of allocating a mostly empty B-tree node.
#[derive(Clone, Default, Debug)]
pub(crate) enum BlockMembership {
    #[default]
    Empty,
    One(BlockKey, BlockRange),
    Many(BTreeMap<BlockKey, BlockRange>),
}

impl BlockMembership {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::One(..) => 1,
            Self::Many(blocks) => blocks.len(),
        }
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&BlockKey, &BlockRange)> {
        let (one, many) = match self {
            Self::Empty => (None, None),
            Self::One(key, range) => (Some((key, range)), None),
            Self::Many(blocks) => (None, Some(blocks)),
        };
        one.into_iter()
            .chain(many.into_iter().flat_map(|blocks| blocks.iter()))
    }

    fn add(&mut self, key: BlockKey, min: i64, max: i64) {
        match self {
            Self::Empty => *self = Self::One(key, (min, max, 1)),
            Self::One(current, range) if *current == key => range.2 += 1,
            Self::One(current, range) => {
                *self = Self::Many(BTreeMap::from([(*current, *range), (key, (min, max, 1))]));
            }
            Self::Many(blocks) => blocks.entry(key).or_insert((min, max, 0)).2 += 1,
        }
    }

    /// Return true only when the last native locator for this block is removed.
    fn remove(&mut self, key: BlockKey) -> bool {
        match self {
            Self::Empty => panic!("native aggregate block membership"),
            Self::One(current, range) => {
                assert_eq!(*current, key, "native aggregate block membership");
                range.2 -= 1;
                if range.2 != 0 {
                    return false;
                }
                *self = Self::Empty;
            }
            Self::Many(blocks) => {
                let range = blocks
                    .get_mut(&key)
                    .expect("native aggregate block membership");
                range.2 -= 1;
                if range.2 != 0 {
                    return false;
                }
                blocks.remove(&key);
                if blocks.len() == 1 {
                    let (&key, &range) = blocks.first_key_value().unwrap();
                    *self = Self::One(key, range);
                }
            }
        }
        true
    }
}

pub(crate) fn added(sources: &mut AggregateSources, index: &super::VLogIndex) {
    if let Some(record) = &index.aggregate {
        let source = Arc::make_mut(sources.entry(record.file).or_default());
        source.live += 1;
        source.blocks.add(
            (index.generation, index.seq, index.offset),
            index.min,
            index.max,
        );
    }
}

pub(crate) fn removed(sources: &mut AggregateSources, index: &super::VLogIndex) {
    let Some(record) = &index.aggregate else {
        return;
    };
    let source = sources
        .get_mut(&record.file)
        .expect("native aggregate membership");
    if source.live == 1 {
        sources.remove(&record.file);
    } else {
        let source = Arc::make_mut(source);
        source.live -= 1;
        let key = (index.generation, index.seq, index.offset);
        if !source.blocks.remove(key) {
            return;
        }
        source.removed.push((index.min, index.max));
        source.normalize_removed();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn membership_promotes_and_demotes_without_losing_duplicate_references() {
        let a = (1, 1, 5);
        let b = (2, 1, 5);
        let mut blocks = BlockMembership::default();
        blocks.add(a, 10, 20);
        blocks.add(a, 10, 20);
        assert!(matches!(blocks, BlockMembership::One(_, (_, _, 2))));
        blocks.add(b, 10, 20);
        assert_eq!(blocks.len(), 2);
        assert!(matches!(blocks, BlockMembership::Many(_)));
        assert!(!blocks.remove(a));
        assert_eq!(blocks.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>(),
                   vec![(a, (10, 20, 1)), (b, (10, 20, 1))]);
        assert!(blocks.remove(b));
        assert!(matches!(blocks, BlockMembership::One(_, (_, _, 1))));
        assert!(blocks.remove(a));
        assert_eq!(blocks.len(), 0);
    }

    #[test]
    fn membership_snapshot_survives_live_replacement() {
        let mut live = Arc::new(AggregateSource::default());
        Arc::make_mut(&mut live).blocks.add((1, 1, 5), 0, 10);
        let pinned = Arc::clone(&live);
        let current = Arc::make_mut(&mut live);
        current.blocks.add((2, 1, 5), 11, 20);
        assert!(current.blocks.remove((1, 1, 5)));
        assert_eq!(pinned.blocks.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>(),
                   vec![((1, 1, 5), (0, 10, 1))]);
        assert_eq!(live.blocks.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>(),
                   vec![((2, 1, 5), (11, 20, 1))]);
    }
}

impl AggregateSource {
    pub(crate) fn normalize_removed(&mut self) {
        self.removed.sort_unstable();
        let mut merged: Vec<(i64, i64)> = Vec::new();
        for &(min, max) in &self.removed {
            if let Some(last) = merged.last_mut() {
                if min <= last.1.saturating_add(1) {
                    last.1 = last.1.max(max);
                    continue;
                }
            }
            merged.push((min, max));
        }
        self.removed = merged;
    }
}
