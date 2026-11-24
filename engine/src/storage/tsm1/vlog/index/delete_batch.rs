//! Bounded external sorting for file-owned deletion ranges.

use super::delete::{DeleteRange, ROW, decode_row, encode_row};
use super::reader::pread;
use super::{Result, invalid};
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

const BUFFERED_RANGES: usize = 4096;
const READ_ROWS: usize = 256;

struct Run {
    file: File,
    count: u64,
}

enum Storage {
    Memory(Vec<DeleteRange>),
    Disk(Run),
}

/// Immutable and replayable without sharing a seek cursor between callers.
pub(crate) struct DeleteBatch {
    storage: Storage,
}

impl DeleteBatch {
    pub(crate) fn len(&self) -> u64 {
        match &self.storage {
            Storage::Memory(rows) => rows.len() as u64,
            Storage::Disk(run) => run.count,
        }
    }

    pub(crate) fn rows(&self) -> impl Iterator<Item = Result<DeleteRange>> + '_ {
        let (memory, disk) = match &self.storage {
            Storage::Memory(rows) => (Some(rows.iter().copied().map(Ok)), None),
            Storage::Disk(run) => (None, Some(RunReader::new(run))),
        };
        memory
            .into_iter()
            .flatten()
            .chain(disk.into_iter().flatten())
    }

    /// Seek directly to one file's field interval in the sorted staging run.
    pub(crate) fn rows_between(
        &self,
        min: u64,
        max: u64,
    ) -> Result<impl Iterator<Item = Result<DeleteRange>> + '_> {
        let (memory, disk) = match &self.storage {
            Storage::Memory(rows) => {
                let start = rows.partition_point(|r| r.seq.get() < min);
                (Some(rows[start..].iter().copied().map(Ok)), None)
            }
            Storage::Disk(run) => {
                let (mut lo, mut hi) = (0, run.count);
                let mut bytes = [0; ROW];
                while lo < hi {
                    let mid = lo + (hi - lo) / 2;
                    pread(&run.file, mid * ROW as u64, &mut bytes)?;
                    if decode_row(&bytes)?.seq.get() < min {
                        lo = mid + 1;
                    } else {
                        hi = mid;
                    }
                }
                let mut reader = RunReader::new(run);
                reader.next = lo;
                (None, Some(reader))
            }
        };
        Ok(memory
            .into_iter()
            .flatten()
            .chain(disk.into_iter().flatten())
            .take_while(move |r| r.as_ref().map_or(true, |r| r.seq.get() <= max)))
    }
}

/// One sorted run per binary size class: at most 64 runs for a u64 row count.
/// Merging uses two fixed read buffers and one buffered output file.
pub(crate) struct DeleteBatchBuilder {
    dir: PathBuf,
    pending: Vec<DeleteRange>,
    runs: Vec<Option<Run>>,
}

impl DeleteBatchBuilder {
    pub(crate) fn new(dir: &Path) -> Self {
        Self {
            dir: dir.into(),
            pending: Vec::new(),
            runs: Vec::new(),
        }
    }

    pub(crate) fn push(&mut self, mut range: DeleteRange) -> Result<()> {
        if range.min > range.max {
            return Err(invalid("reversed delete range"));
        }
        // New deletions have file scope; the cutoff is legacy migration data only.
        range.cutoff = 0;
        self.pending.push(range);
        if self.pending.len() == BUFFERED_RANGES {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        self.pending.sort_unstable();
        self.pending.dedup();
        let file = tempfile::tempfile_in(&self.dir)?;
        let mut writer = BufWriter::new(&file);
        for row in &self.pending {
            writer.write_all(&encode_row(*row))?;
        }
        writer.flush()?;
        drop(writer);
        let mut run = Run {
            file,
            count: self.pending.len() as u64,
        };
        self.pending.clear();
        let mut level = 0;
        loop {
            if level == self.runs.len() {
                if level == 64 {
                    return Err(super::Error::Overflow);
                }
                self.runs.push(None);
            }
            match self.runs[level].take() {
                Some(previous) => run = self.merge(previous, run)?,
                None => {
                    self.runs[level] = Some(run);
                    return Ok(());
                }
            }
            level += 1;
        }
    }

    fn merge(&self, left: Run, right: Run) -> Result<Run> {
        let file = tempfile::tempfile_in(&self.dir)?;
        let mut writer = BufWriter::new(&file);
        let (mut a, mut b) = (RunReader::new(&left), RunReader::new(&right));
        let (mut av, mut bv) = (a.next().transpose()?, b.next().transpose()?);
        let mut count = 0u64;
        let mut previous = None;
        while av.is_some() || bv.is_some() {
            let row = if bv.is_none() || av.is_some_and(|v| v <= bv.unwrap()) {
                let row = av.take().unwrap();
                av = a.next().transpose()?;
                row
            } else {
                let row = bv.take().unwrap();
                bv = b.next().transpose()?;
                row
            };
            if previous != Some(row) {
                writer.write_all(&encode_row(row))?;
                count = count.checked_add(1).ok_or(super::Error::Overflow)?;
                previous = Some(row);
            }
        }
        writer.flush()?;
        drop(writer);
        Ok(Run { file, count })
    }

    pub(crate) fn finish(mut self) -> Result<DeleteBatch> {
        if self.runs.is_empty() {
            self.pending.sort_unstable();
            self.pending.dedup();
            return Ok(DeleteBatch {
                storage: Storage::Memory(self.pending),
            });
        }
        self.flush()?;
        let mut merged = None;
        for level in 0..self.runs.len() {
            if let Some(run) = self.runs[level].take() {
                merged = Some(match merged {
                    Some(previous) => self.merge(previous, run)?,
                    None => run,
                });
            }
        }
        Ok(DeleteBatch {
            storage: Storage::Disk(merged.ok_or(invalid("missing deletion run"))?),
        })
    }
}

struct RunReader<'a> {
    run: &'a Run,
    next: u64,
    buffer: [u8; ROW * READ_ROWS],
    loaded_page: Option<u64>,
}

impl<'a> RunReader<'a> {
    fn new(run: &'a Run) -> Self {
        Self {
            run,
            next: 0,
            buffer: [0; ROW * READ_ROWS],
            loaded_page: None,
        }
    }
}

impl Iterator for RunReader<'_> {
    type Item = Result<DeleteRange>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next == self.run.count {
            return None;
        }
        let slot = self.next as usize % READ_ROWS;
        let page = self.next / READ_ROWS as u64 * READ_ROWS as u64;
        if self.loaded_page != Some(page) {
            let count = (self.run.count - page).min(READ_ROWS as u64) as usize;
            if let Err(error) = pread(
                &self.run.file,
                page * ROW as u64,
                &mut self.buffer[..count * ROW],
            ) {
                self.next = self.run.count;
                return Some(Err(error.into()));
            }
            self.loaded_page = Some(page);
        }
        self.next += 1;
        Some(decode_row(&self.buffer[slot * ROW..(slot + 1) * ROW]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::tsm1::vlog::index::{SeqNo, delete::DeleteFile};

    fn range(series: u64) -> DeleteRange {
        DeleteRange {
            seq: SeqNo::new(series, 1).unwrap(),
            min: -10,
            max: 20,
            cutoff: 77,
        }
    }

    #[test]
    fn deletion_batches_sort_deduplicate_and_replay_from_bounded_runs() {
        let dir = tempfile::tempdir().unwrap();
        let mut builder = DeleteBatchBuilder::new(dir.path());
        let count = BUFFERED_RANGES as u64 * 17 + 13;
        for i in (1..=count).rev().chain(1..=count) {
            builder.push(range(i)).unwrap();
            assert!(builder.pending.capacity() <= BUFFERED_RANGES);
            assert!(builder.runs.len() <= 6);
        }
        let batch = builder.finish().unwrap();
        assert!(matches!(batch.storage, Storage::Disk(_)));
        assert_eq!(batch.len(), count);
        let verify = || {
            let mut rows = batch.rows();
            for i in 1..=count {
                let mut expected = range(i);
                expected.cutoff = 0;
                assert_eq!(rows.next().unwrap().unwrap(), expected);
            }
            assert!(rows.next().is_none());
        };
        std::thread::scope(|scope| {
            scope.spawn(verify);
            verify();
        });
        let slice = batch
            .rows_between(
                SeqNo::new(4099, 1).unwrap().get(),
                SeqNo::new(4111, 1).unwrap().get(),
            )
            .unwrap()
            .map(|r| r.unwrap().seq.series_id())
            .collect::<Vec<_>>();
        assert_eq!(slice, (4099..=4111).collect::<Vec<_>>());
        let path = dir.path().join("ranges.del");
        DeleteFile::write(&path, &batch, 513).unwrap();
        let file = DeleteFile::open(&path).unwrap();
        for i in [1, BUFFERED_RANGES as u64, count] {
            let mut expected = range(i);
            expected.cutoff = 513;
            assert_eq!(file.lookup(expected.seq).unwrap(), vec![expected]);
        }
        drop(batch);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn small_deletion_batches_need_no_temporary_file() {
        let dir = tempfile::tempdir().unwrap();
        let mut builder = DeleteBatchBuilder::new(&dir.path().join("missing"));
        for i in [3, 1, 2, 1] {
            builder.push(range(i)).unwrap();
        }
        let batch = builder.finish().unwrap();
        assert!(matches!(batch.storage, Storage::Memory(_)));
        assert_eq!(batch.len(), 3);
        assert_eq!(
            batch
                .rows()
                .map(|r| r.unwrap().seq.series_id())
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(
            DeleteBatchBuilder::new(dir.path()).finish().unwrap().len(),
            0
        );
    }

    #[test]
    fn corrupt_deletion_runs_fail_before_publication() {
        use std::os::unix::fs::FileExt;
        let dir = tempfile::tempdir().unwrap();
        let mut builder = DeleteBatchBuilder::new(dir.path());
        for i in 1..=BUFFERED_RANGES as u64 {
            builder.push(range(i)).unwrap();
        }
        let batch = builder.finish().unwrap();
        let Storage::Disk(run) = &batch.storage else {
            panic!("expected disk staging")
        };
        let mut byte = [0];
        pread(&run.file, ROW as u64 - 1, &mut byte).unwrap();
        byte[0] ^= 1;
        run.file.write_at(&byte, ROW as u64 - 1).unwrap();
        assert!(batch.rows().next().unwrap().is_err());
        let path = dir.path().join("data");
        let store = crate::storage::tsm1::vlog::index::store::IndexStore::open(
            &path,
            &dir.path().join("wal"),
        )
        .unwrap();
        assert!(store.install_delete_batch(&batch).is_err());
        store.ensure_ready().unwrap();
        assert!(store.read_view().2.is_empty());
        assert!(std::fs::read_dir(&path).unwrap().all(|entry| {
            let path = entry.unwrap().path();
            !matches!(
                path.extension().and_then(|s| s.to_str()),
                Some("tmp" | "del")
            )
        }));
    }

    #[test]
    fn failed_deletion_preparation_cleans_temporary_runs() {
        let dir = tempfile::tempdir().unwrap();
        let mut builder = DeleteBatchBuilder::new(dir.path());
        for i in 1..=BUFFERED_RANGES as u64 {
            builder.push(range(i)).unwrap();
        }
        assert!(
            builder
                .push(DeleteRange {
                    min: 21,
                    ..range(1)
                })
                .is_err()
        );
        drop(builder);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}
