//! Per-field streaming construction with a bounded unfinished suffix per level.

use super::page_group::validate_record;
use super::tree::Forest;
use super::{FAN_OUT, LEVEL_BLOCK, MAX_LEVELS, MIN_CHILDREN, Record, Result, invalid};

pub(crate) struct TreeBuilder {
    pending: [Vec<Record>; MAX_LEVELS as usize],
    previous: Option<Record>,
    failed: bool,
}
impl TreeBuilder {
    pub(crate) fn new() -> Self {
        Self {
            pending: std::array::from_fn(|_| Vec::new()),
            previous: None,
            failed: false,
        }
    }
    pub(crate) fn push(
        &mut self,
        record: Record,
        emit: &mut impl FnMut(Record) -> Result<()>,
    ) -> Result<()> {
        if self.failed {
            return Err(invalid("aggregate builder is failed"));
        }
        let result = (|| {
            validate_record(&record, record.values.field_type())?;
            if record.level != LEVEL_BLOCK
                || self.previous.is_some_and(|old| {
                    old.seq_no != record.seq_no
                        || old.values.field_type() != record.values.field_type()
                        || old.pos.end >= record.pos.start
                })
            {
                return Err(invalid("invalid aggregate builder input"));
            }
            emit(record)?;
            self.feed(record, emit)?;
            self.previous = Some(record);
            Ok(())
        })();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn feed(&mut self, record: Record, emit: &mut impl FnMut(Record) -> Result<()>) -> Result<()> {
        let index = record.level as usize - 1;
        // Most fields never reach every level. Allocate the original bounded
        // suffix once, only when this level actually receives a record.
        if self.pending[index].capacity() == 0 {
            self.pending[index].reserve_exact(2 * FAN_OUT);
        }
        self.pending[index].push(record);
        if record.level == MAX_LEVELS {
            if self.pending[index].len() > FAN_OUT {
                return Err(super::Error::TreeCapacityExceeded);
            }
        } else if self.pending[index].len() == 2 * FAN_OUT {
            let first = self.pending[index].drain(..FAN_OUT).collect::<Vec<_>>();
            let parent = Record::summarize(&first)?;
            emit(parent)?;
            self.feed(parent, emit)?;
        }
        Ok(())
    }
    pub(crate) fn finish(mut self, emit: &mut impl FnMut(Record) -> Result<()>) -> Result<Forest> {
        if self.failed || self.previous.is_none() {
            return Err(invalid("empty or failed aggregate builder"));
        }
        for index in 0..MAX_LEVELS as usize {
            let pending = std::mem::take(&mut self.pending[index]);
            // A parent above a level that already fits the root list would span
            // most of the tree, costing most queries a read they cannot use.
            if self.pending[index + 1..].iter().all(Vec::is_empty) && pending.len() <= FAN_OUT {
                return Forest::new(pending);
            }
            if pending.len() < MIN_CHILDREN || index + 1 == MAX_LEVELS as usize {
                return Err(invalid("incomplete aggregate level coverage"));
            }
            // Keep an entire final node until EOF. A 64+6 suffix becomes 35+35;
            // a 64+36 suffix keeps both groups intact.
            let split = if pending.len() <= FAN_OUT {
                pending.len()
            } else if pending.len() < FAN_OUT + MIN_CHILDREN {
                pending.len() / 2
            } else {
                FAN_OUT
            };
            for group in [&pending[..split], &pending[split..]] {
                if group.is_empty() {
                    continue;
                }
                let parent = Record::summarize(group)?;
                emit(parent)?;
                self.feed(parent, emit)?;
            }
        }
        Err(super::Error::TreeCapacityExceeded)
    }
}
