//! Reserve physical file identities without splitting a logical index generation.
//!
//! Index names retain the first generation of an output batch. Their sequence is
//! the distance from that generation's first physical ID. Existing sequences below
//! 256 keep exactly their old identity. Value-block locations remain unchanged.

use super::{BlockLoc, Error, Result, invalid};
use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};

const SLOTS: i64 = 256;

pub(super) fn index_file_no(generation: i64, sequence: i64) -> Result<u32> {
    if sequence < 0 {
        return Err(Error::InvalidBlockLocation);
    }
    let physical = generation
        .checked_add(sequence / SLOTS)
        .ok_or(Error::InvalidBlockLocation)?;
    BlockLoc::from_vlog(generation, 0, 5)?;
    Ok(BlockLoc::from_vlog(physical, sequence % SLOTS, 5)?.file_no)
}

pub(super) fn index_file_no_from_name(name: &str) -> Result<u32> {
    let (generation, sequence) = name
        .strip_suffix(".idx")
        .and_then(|s| s.split_once('-'))
        .ok_or(invalid("invalid index file name"))?;
    let generation = generation
        .parse::<i64>()
        .map_err(|_| invalid("invalid index generation"))?;
    let sequence = sequence
        .parse::<i64>()
        .map_err(|_| invalid("invalid index sequence"))?;
    if name != format!("{generation:09}-{sequence:09}.idx") {
        return Err(invalid("noncanonical index file name"));
    }
    index_file_no(generation, sequence)
}

#[derive(Clone)]
pub(crate) struct OutputGenerations {
    pub(super) first: i64,
    reserve: Arc<dyn Fn() -> i64 + Send + Sync>,
}
impl OutputGenerations {
    pub(crate) fn reserve(files: Arc<super::super::file_store::FileStore>) -> Self {
        Self::for_snapshot(files.next_generation(), files)
    }
    pub(crate) fn for_snapshot(
        first: i64,
        files: Arc<super::super::file_store::FileStore>,
    ) -> Self {
        Self {
            first,
            reserve: Arc::new(move || files.next_generation()),
        }
    }
    /// Only for a private scratch directory or isolated test fixtures. Persisted
    /// engine outputs must reserve from the shared VLog FileStore instead.
    pub(super) fn isolated(first: i64) -> Self {
        let next = AtomicI64::new(first + 1);
        Self {
            first,
            reserve: Arc::new(move || next.fetch_add(1, Ordering::SeqCst)),
        }
    }
    pub(super) fn next_batch(&self) -> Self {
        Self {
            first: (self.reserve)(),
            reserve: Arc::clone(&self.reserve),
        }
    }
}

pub(super) struct OutputIds {
    generations: OutputGenerations,
    physical: i64,
    next_sequence: i64,
}
impl OutputIds {
    pub(super) fn new(generations: OutputGenerations, first_sequence: i64) -> Self {
        Self {
            physical: generations.first,
            generations,
            next_sequence: first_sequence,
        }
    }
    pub(super) fn next_vlog(&mut self) -> Result<(i64, i64)> {
        if self.next_sequence == SLOTS {
            let next = (self.generations.reserve)();
            if next <= self.physical {
                return Err(invalid("file generation allocator did not advance"));
            }
            BlockLoc::from_vlog(next, 0, 5)?;
            self.physical = next;
            self.next_sequence = 0;
        }
        let sequence = self.next_sequence;
        BlockLoc::from_vlog(self.physical, sequence, 5)?;
        self.next_sequence += 1;
        Ok((self.physical, sequence))
    }
    pub(super) fn next_index(&mut self) -> Result<(i64, i64)> {
        let (physical, sequence) = self.next_vlog()?;
        let generation = self.generations.first;
        let sequence = (physical - generation) * SLOTS + sequence;
        index_file_no(generation, sequence)?;
        Ok((generation, sequence))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_batches_never_share_physical_index_identities() {
        let source = OutputGenerations::isolated(1);
        let workers: Vec<_> = (0..4)
            .map(|_| {
                let source = source.clone();
                std::thread::spawn(move || {
                    let batch = source.next_batch();
                    let first = batch.first;
                    let mut ids = OutputIds::new(batch, 0);
                    (0..600)
                        .map(|_| {
                            let (generation, sequence) = ids.next_index().unwrap();
                            assert_eq!(generation, first);
                            index_file_no(generation, sequence).unwrap()
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let ids: Vec<_> = workers
            .into_iter()
            .flat_map(|w| w.join().unwrap())
            .collect();
        assert_eq!(
            ids.iter().collect::<std::collections::HashSet<_>>().len(),
            2400
        );
    }

    #[test]
    fn shared_reservations_keep_index_groups_and_skip_concurrent_generations() {
        let source = OutputGenerations::isolated(7);
        let mut indexes = OutputIds::new(source.clone(), 255);
        let mut values = OutputIds::new(source.clone(), 255);
        assert_eq!(indexes.next_index().unwrap(), (7, 255));
        assert_eq!(values.next_vlog().unwrap(), (7, 255));
        assert_eq!(source.next_batch().first, 8);
        assert_eq!(indexes.next_index().unwrap(), (7, 512));
        assert_eq!(values.next_vlog().unwrap(), (10, 0));
        assert_eq!(index_file_no(7, 512).unwrap(), (9 << 8));
        assert_eq!(
            index_file_no_from_name("000000007-000000512.idx").unwrap(),
            9 << 8
        );
        for generation in [1, 7, (1 << 24) - 1] {
            for sequence in [0, 1, 255] {
                assert_eq!(
                    index_file_no(generation, sequence).unwrap(),
                    BlockLoc::from_vlog(generation, sequence, 5)
                        .unwrap()
                        .file_no
                );
            }
        }
        assert!(index_file_no((1 << 24) - 1, 256).is_err());
        assert!(index_file_no(0, 256).is_err());
        assert!(index_file_no(1, i64::MAX).is_err());
        assert!(index_file_no_from_name("../000000007-000000512.idx").is_err());
    }
}
