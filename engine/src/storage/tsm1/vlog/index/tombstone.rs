//! File-owned, normalized ranges. Readers pin an immutable inode across replacement.

use super::{
    Result, SeqNo, delete::DeleteRange, delete_batch::DeleteBatch, invalid, reader::pread,
};
use std::{
    cell::RefCell,
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

const HEADER: usize = 20;
const ROW: usize = 28;
const PAGE_ROWS: u64 = 256;

pub(crate) fn path(index: &Path) -> PathBuf {
    let mut name = index.as_os_str().to_os_string();
    name.push(".tombstone");
    name.into()
}

pub(crate) struct Tombstone {
    pub(crate) path: PathBuf,
    file: File,
    pub(crate) owner: u32,
    count: u64,
}

fn encode(row: DeleteRange) -> [u8; ROW] {
    let mut bytes = [0; ROW];
    bytes[..8].copy_from_slice(&row.seq.get().to_be_bytes());
    bytes[8..16].copy_from_slice(&row.min.to_be_bytes());
    bytes[16..24].copy_from_slice(&row.max.to_be_bytes());
    let crc = crc32fast::hash(&bytes[..24]);
    bytes[24..].copy_from_slice(&crc.to_be_bytes());
    bytes
}

fn decode(bytes: &[u8]) -> Result<DeleteRange> {
    if bytes.len() != ROW || crc32fast::hash(&bytes[..24]).to_be_bytes() != bytes[24..] {
        return Err(invalid("tombstone row checksum mismatch"));
    }
    let row = DeleteRange {
        seq: SeqNo::from_raw(u64::from_be_bytes(bytes[..8].try_into().unwrap()))?,
        min: i64::from_be_bytes(bytes[8..16].try_into().unwrap()),
        max: i64::from_be_bytes(bytes[16..24].try_into().unwrap()),
        cutoff: u32::MAX,
    };
    if row.min > row.max {
        return Err(invalid("reversed tombstone range"));
    }
    Ok(row)
}

impl Tombstone {
    pub(crate) fn write(path: &Path, owner: u32, batch: &DeleteBatch) -> Result<()> {
        let mut file =
            std::io::BufWriter::new(OpenOptions::new().create_new(true).write(true).open(path)?);
        file.write_all(&[0; HEADER])?;
        let mut count = 0u64;
        let mut pending: Option<DeleteRange> = None;
        for row in batch.rows() {
            let row = row?;
            if let Some(last) = &mut pending {
                if (row.seq, row.min) < (last.seq, last.min) {
                    return Err(invalid("unsorted tombstone batch"));
                }
                if row.seq == last.seq && row.min <= last.max.saturating_add(1) {
                    last.max = last.max.max(row.max);
                    continue;
                }
                file.write_all(&encode(*last))?;
                count += 1;
            }
            pending = Some(row);
        }
        if let Some(last) = pending {
            file.write_all(&encode(last))?;
            count += 1;
        }
        if count == 0 {
            return Err(invalid("empty tombstone"));
        }
        file.seek(SeekFrom::Start(0))?;
        let mut header = Vec::from(*b"AGGT");
        header.extend_from_slice(&owner.to_be_bytes());
        header.extend_from_slice(&count.to_be_bytes());
        header.extend_from_slice(&crc32fast::hash(&header).to_be_bytes());
        file.write_all(&header)?;
        file.flush()?;
        file.get_ref().sync_all()?;
        Ok(())
    }

    pub(crate) fn open(path: &Path, owner: u32) -> Result<Arc<Self>> {
        let file = File::open(path)?;
        let mut header = [0; HEADER];
        pread(&file, 0, &mut header)?;
        if &header[..4] != b"AGGT"
            || header[4..8] != owner.to_be_bytes()
            || crc32fast::hash(&header[..16]).to_be_bytes() != header[16..]
        {
            return Err(invalid("invalid tombstone header or owner"));
        }
        let count = u64::from_be_bytes(header[8..16].try_into().unwrap());
        if count == 0
            || count
                .checked_mul(ROW as u64)
                .and_then(|n| n.checked_add(HEADER as u64))
                != Some(file.metadata()?.len())
        {
            return Err(invalid("invalid tombstone length"));
        }
        let result = Arc::new(Self {
            path: path.into(),
            file,
            owner,
            count,
        });
        let mut previous: Option<DeleteRange> = None;
        for page in result.pages() {
            for row in page? {
                if previous.is_some_and(|p| {
                    p.seq > row.seq || (p.seq == row.seq && p.max.saturating_add(1) >= row.min)
                }) {
                    return Err(invalid("tombstone ranges are not normalized"));
                }
                previous = Some(row);
            }
        }
        Ok(result)
    }

    fn row(&self, index: u64) -> Result<DeleteRange> {
        let mut bytes = [0; ROW];
        pread(&self.file, HEADER as u64 + index * ROW as u64, &mut bytes)?;
        decode(&bytes)
    }
    fn page(&self, start: u64) -> Result<Vec<DeleteRange>> {
        let count = (self.count - start).min(PAGE_ROWS) as usize;
        let mut bytes = vec![0; count * ROW];
        pread(&self.file, HEADER as u64 + start * ROW as u64, &mut bytes)?;
        bytes.chunks_exact(ROW).map(decode).collect()
    }
    pub(crate) fn pages(&self) -> impl Iterator<Item = Result<Vec<DeleteRange>>> + '_ {
        (0..self.count)
            .step_by(PAGE_ROWS as usize)
            .map(|start| self.page(start))
    }
    #[cfg(test)]
    pub(crate) fn lookup(&self, seq: SeqNo) -> Result<Vec<DeleteRange>> {
        let mut result = Vec::new();
        for page in self.pages() {
            result.extend(page?.into_iter().filter(|r| r.seq == seq));
        }
        Ok(result)
    }
}

/// One private page per queried input file; no shard-wide field/range map.
pub(crate) struct TombstoneView {
    removed: Option<Arc<super::super::aggregate_record::AggregateSource>>,
    file: Option<Arc<Tombstone>>,
    seq: SeqNo,
    page: RefCell<Option<(i64, Vec<DeleteRange>)>>,
}
impl TombstoneView {
    pub(crate) fn new(file: Option<Arc<Tombstone>>, seq: SeqNo) -> Self {
        Self {
            removed: None,
            file,
            seq,
            page: RefCell::new(None),
        }
    }
    pub(crate) fn with_removed(mut self, source: Arc<super::super::aggregate_record::AggregateSource>) -> Self {
        self.removed = Some(source);
        self
    }
    fn first(&self, min: i64) -> Result<Option<DeleteRange>> {
        let Some(file) = &self.file else {
            return Ok(None);
        };
        if let Some((start, rows)) = &*self.page.borrow() {
            if min >= *start {
                if rows.is_empty() {
                    return Ok(None);
                }
                let i = rows
                    .partition_point(|r| r.seq < self.seq || (r.seq == self.seq && r.max < min));
                if let Some(row) = rows.get(i) {
                    return Ok((row.seq == self.seq).then_some(*row));
                }
            }
        }
        let (mut lo, mut hi) = (0, file.count);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let row = file.row(mid)?;
            if (row.seq, row.max) < (self.seq, min) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let rows = file.page(lo)?;
        let result = rows.first().copied().filter(|r| r.seq == self.seq);
        *self.page.borrow_mut() = Some((min, rows));
        Ok(result)
    }
    /// Returns (intersects, fully_covered) for a block or subtree.
    pub(crate) fn coverage(&self, min: i64, max: i64) -> Result<(bool, bool)> {
        let started = crate::common::slow_trace::now();
        let mut result = self.first(min)?.map_or((false, false), |r| {
            (r.min <= max, r.min <= min && r.max >= max)
        });
        if let Some(source) = &self.removed {
            let i = source.removed.partition_point(|r| r.1 < min);
            if let Some(&(lo, hi)) = source.removed.get(i) {
                result.0 |= lo <= max;
                result.1 |= lo <= min && hi >= max;
            }
        }
        crate::common::slow_trace::log_query_stage(
            "query_tombstone_lookup",
            started,
            "file-owned ranges",
        );
        Ok(result)
    }
    pub(crate) fn removes(&self, time: i64) -> Result<bool> {
        Ok(self.coverage(time, time)?.0)
    }
}

#[cfg(test)]
mod tests {
    use super::super::delete_batch::DeleteBatchBuilder;
    use super::*;

    #[test]
    fn normalized_ranges_use_bounded_pages_in_both_directions() {
        let dir = tempfile::tempdir().unwrap();
        let seq = SeqNo::new(1, 1).unwrap();
        let mut builder = DeleteBatchBuilder::new(dir.path());
        for i in (0..20000i64).rev() {
            for (min, max) in [
                (i * 8, i * 8 + 1),
                (i * 8 + 1, i * 8 + 3),
                (i * 8 + 4, i * 8 + 4),
            ] {
                builder
                    .push(DeleteRange {
                        seq,
                        min,
                        max,
                        cutoff: 0,
                    })
                    .unwrap();
            }
        }
        for t in [i64::MIN, i64::MAX] {
            builder
                .push(DeleteRange {
                    seq,
                    min: t,
                    max: t,
                    cutoff: 0,
                })
                .unwrap();
        }
        let path = dir.path().join("test.idx.tombstone");
        Tombstone::write(&path, 257, &builder.finish().unwrap()).unwrap();
        let file = Tombstone::open(&path, 257).unwrap();
        assert_eq!(file.count, 20002);
        let view = TombstoneView::new(Some(file), seq);
        for i in (0..20000i64).chain((0..20000i64).rev()) {
            assert_eq!(view.coverage(i * 8, i * 8 + 4).unwrap(), (true, true));
            assert_eq!(view.coverage(i * 8 + 3, i * 8 + 7).unwrap(), (true, false));
            assert!(!view.removes(i * 8 + 7).unwrap());
            assert!(view.page.borrow().as_ref().unwrap().1.len() <= 256);
        }
        for t in [i64::MIN, i64::MAX] {
            assert!(view.removes(t).unwrap());
        }
        assert!(Tombstone::open(&path, 258).is_err());
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[HEADER + 7] ^= 1;
        std::fs::write(&path, bytes).unwrap();
        assert!(Tombstone::open(&path, 257).is_err());
    }
}
