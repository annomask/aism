use super::column_layout::PART_COUNT;
use super::directory::*;
use super::page_group::{
    ALL_COLUMNS, Column, EncodedPageGroup, PageGroupEntry, SelectedRecord, decode_columns,
    decode_group, decode_group_columns, decode_parts,
};
use super::segment::{LEVEL_HEADER_SIZE, SegmentHeader};
use super::writer::IndexFileMeta;
use super::{Level, Record, Result, SeqNo, encode as enc, invalid};
use crate::query::storage_ir::AggregateIndexReadMetrics;
use std::{
    fs::File,
    io,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};

const TABLE_WHOLE_PAGES: usize = 16;
const GROUPS_PER_PAGE: usize = PAGE / PageGroupEntry::SIZE;

enum ReadBytes {
    Owned(Vec<u8>),
    Shared(Arc<[u8]>),
}

impl Deref for ReadBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Self::Owned(bytes) => bytes,
            Self::Shared(bytes) => bytes,
        }
    }
}

impl ReadBytes {
    fn as_slice(&self) -> &[u8] {
        self
    }
    fn into_vec(self) -> Vec<u8> {
        match self {
            Self::Owned(bytes) => bytes,
            Self::Shared(bytes) => bytes.to_vec(),
        }
    }
}

pub(crate) struct IndexFile {
    pub(crate) generation: i64,
    pub(crate) seq: i64,
    pub(crate) file_no: u32,
    pub(crate) format: IndexFormat,
    path: PathBuf,
    fd: File,
    len: u64,
    pub(crate) footer: Footer,
    pub(crate) top_index: TopIndex,
    cache: parking_lot::RwLock<Option<Arc<super::caches::ReadCache>>>,
    pub(crate) tombstone: Option<Arc<super::tombstone::Tombstone>>,
}
pub(crate) fn pread(fd: &File, mut offset: u64, mut out: &mut [u8]) -> io::Result<()> {
    while !out.is_empty() {
        #[cfg(unix)]
        let result = {
            use std::os::unix::fs::FileExt;
            fd.read_at(out, offset)
        };
        #[cfg(windows)]
        let result = {
            use std::os::windows::fs::FileExt;
            fd.seek_read(out, offset)
        };
        match result {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
            Ok(n) => {
                offset += n as u64;
                out = &mut out[n..];
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
impl IndexFile {
    pub(crate) fn open(path: &Path, generation: i64, seq: i64) -> Result<Arc<Self>> {
        let file_no = super::allocation::index_file_no(generation, seq)?;
        let fd = File::open(path)?;
        let len = fd.metadata()?.len();
        if !(HEADER_SIZE as u64 + FOOTER_SIZE as u64..=MAX_INDEX_FILE_SIZE).contains(&len) {
            return Err(invalid("invalid index file length"));
        }
        let mut header = [0; HEADER_SIZE];
        pread(&fd, 0, &mut header)?;
        if header[..4] != MAGIC
            || header[6..8] != [0; 2]
            || header[8..12] != file_no.to_be_bytes()
            || header[12..].iter().any(|b| *b != 0)
        {
            return Err(invalid("invalid index file header or identity"));
        }
        let format =
            IndexFormat::from_version(u16::from_be_bytes(header[4..6].try_into().unwrap()))?;
        let mut footer_bytes = [0; FOOTER_SIZE];
        pread(&fd, len - FOOTER_SIZE as u64, &mut footer_bytes)?;
        let footer = Footer::decode(&footer_bytes, len)?;
        let top_len = (len - FOOTER_SIZE as u64 - footer.top_index_offset) as usize;
        let mut top = vec![0; top_len];
        pread(&fd, footer.top_index_offset, &mut top)?;
        Footer::verify_checksum(&footer_bytes, &top)?;
        let top_index = TopIndex::decode(&top, &footer)?;
        let tombstone_path = super::tombstone::path(path);
        let tombstone = if tombstone_path.exists() {
            Some(super::tombstone::Tombstone::open(&tombstone_path, file_no)?)
        } else {
            None
        };
        Ok(Arc::new(Self {
            generation,
            seq,
            file_no,
            format,
            path: path.to_path_buf(),
            fd,
            len,
            footer,
            top_index,
            cache: parking_lot::RwLock::new(None),
            tombstone,
        }))
    }
    pub(crate) fn from_meta(meta: IndexFileMeta) -> Result<Arc<Self>> {
        let file = Self::open(&meta.path, meta.generation, meta.seq)?;
        if file.footer != meta.footer || file.top_index != meta.top_index {
            return Err(invalid("installed index metadata mismatch"));
        }
        Ok(file)
    }
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
    pub(crate) fn tombstone_view(&self, seq: SeqNo) -> super::tombstone::TombstoneView {
        super::tombstone::TombstoneView::new(self.tombstone.clone(), seq)
    }
    pub(crate) fn size(&self) -> u64 {
        self.len
    }
    pub(super) fn set_cache(&self, cache: Arc<super::caches::ReadCache>) {
        *self.cache.write() = Some(cache);
    }
    pub(crate) fn seq_no_range(&self) -> (u64, u64) {
        (self.footer.min_seq_no, self.footer.max_seq_no)
    }
    fn read(&self, offset: u64, len: usize) -> Result<ReadBytes> {
        if offset
            .checked_add(len as u64)
            .is_none_or(|end| end > self.len)
        {
            return Err(invalid("read outside index file"));
        }
        let cache_started = crate::common::slow_trace::now();
        let cache = { self.cache.read().clone() };
        let key = (self.file_no, offset, len);
        if let Some(bytes) = cache.as_ref().and_then(|cache| cache.get(key)) {
            crate::common::slow_trace::log_query_stage(
                "query_index_cache_hit",
                cache_started,
                "index bytes",
            );
            return Ok(ReadBytes::Shared(bytes));
        }
        crate::common::slow_trace::log_query_stage(
            "query_index_cache_miss",
            cache_started,
            "index bytes",
        );
        let mut bytes = vec![0; len];
        let io_started = crate::common::slow_trace::now();
        pread(&self.fd, offset, &mut bytes)?;
        crate::common::slow_trace::log_query_stage("query_index_pread", io_started, "index bytes");
        if let Some(cache) = cache {
            cache.insert(key, &bytes);
        }
        Ok(ReadBytes::Owned(bytes))
    }
    pub(crate) fn directory_page(&self, page: usize) -> Result<Vec<DirEntry>> {
        if page >= self.footer.directory_pages() {
            return Err(invalid("directory page out of bounds"));
        }
        let count = (self.footer.series_count as usize - page * DIR_PER_PAGE).min(DIR_PER_PAGE);
        let bytes = self.read(
            self.footer.dir_offset + (page * PAGE) as u64,
            count * DIR_ENTRY_SIZE,
        )?;
        let decode_started = crate::common::slow_trace::now();
        let entries = bytes
            .chunks_exact(DIR_ENTRY_SIZE)
            .map(|b| DirEntry::decode(b, self.footer.page_index_offset))
            .collect::<Result<Vec<_>>>()?;
        if entries.windows(2).any(|p| {
            p[0].seq_no >= p[1].seq_no
                || p[0].seg_offset + u64::from(p[0].seg_len) != p[1].seg_offset
        }) || entries[0].seq_no.get() < self.footer.min_seq_no
            || entries.last().unwrap().seq_no.get() > self.footer.max_seq_no
        {
            return Err(invalid("invalid directory order"));
        }
        if page == 0
            && (entries[0].seq_no.get() != self.footer.min_seq_no
                || entries[0].seg_offset != HEADER_SIZE as u64)
        {
            return Err(invalid("invalid first directory entry"));
        }
        if page + 1 == self.footer.directory_pages() {
            let e = entries.last().unwrap();
            if e.seq_no.get() != self.footer.max_seq_no
                || e.seg_offset + u64::from(e.seg_len) != self.footer.page_index_offset
            {
                return Err(invalid("invalid final directory entry"));
            }
        }
        crate::common::slow_trace::log_query_stage(
            "query_index_directory_decode",
            decode_started,
            "validated directory page",
        );
        crate::common::slow_trace::profile_index_metadata(
            "directory", self as *const Self as usize,
            self.footer.dir_offset + (page * PAGE) as u64, count,
        );
        Ok(entries)
    }
    pub(crate) fn lookup(&self, seq_no: SeqNo) -> Result<Option<DirEntry>> {
        if seq_no.get() < self.footer.min_seq_no || seq_no.get() > self.footer.max_seq_no {
            return Ok(None);
        }
        let page = self
            .top_index
            .locate(seq_no)
            .ok_or(invalid("missing top index page"))?;
        let minima = self.page_minima(page)?;
        let local = minima
            .partition_point(|s| *s <= seq_no.get())
            .checked_sub(1)
            .ok_or(invalid("missing directory page"))?;
        let entries = self.directory_page(page * INDEX_PER_PAGE + local)?;
        Self::find_directory_entry(seq_no, minima[local], &entries)
    }

    pub(crate) fn lookup_cached(
        self: &Arc<Self>,
        seq_no: SeqNo,
        cache: &mut super::caches::QueryDirectoryCache,
    ) -> Result<Option<DirEntry>> {
        // A probe outside this source must not evict another source's hot page.
        if seq_no.get() < self.footer.min_seq_no || seq_no.get() > self.footer.max_seq_no {
            return Ok(None);
        }
        let page = self
            .top_index
            .locate(seq_no)
            .ok_or(invalid("missing top index page"))?;
        let (directory_page, minimum) = {
            let minima = cache.minima(self, page, || self.page_minima(page))?;
            let local = minima
                .partition_point(|s| *s <= seq_no.get())
                .checked_sub(1)
                .ok_or(invalid("missing directory page"))?;
            (page * INDEX_PER_PAGE + local, minima[local])
        };
        let entries = cache.directory(self, directory_page, || {
            let entries = self.directory_page(directory_page)?;
            if entries[0].seq_no.get() != minimum {
                return Err(invalid("directory page minimum mismatch"));
            }
            Ok(entries)
        })?;
        Self::find_directory_entry(seq_no, minimum, entries)
    }

    fn find_directory_entry(
        seq_no: SeqNo,
        minimum: u64,
        entries: &[DirEntry],
    ) -> Result<Option<DirEntry>> {
        if entries[0].seq_no.get() != minimum {
            return Err(invalid("directory page minimum mismatch"));
        }
        Ok(entries
            .binary_search_by_key(&seq_no, |e| e.seq_no)
            .ok()
            .map(|i| entries[i].clone()))
    }

    fn page_minima(&self, page: usize) -> Result<Vec<u64>> {
        let start = page * INDEX_PER_PAGE;
        let count = (self.footer.directory_pages() - start).min(INDEX_PER_PAGE);
        let bytes = self.read(
            self.footer.page_index_offset + (start * 8) as u64,
            count * 8,
        )?;
        let decode_started = crate::common::slow_trace::now();
        let mut input = bytes.as_slice();
        let mut minima = Vec::with_capacity(count);
        while !input.is_empty() {
            let no = enc::u64(&mut input)?;
            SeqNo::from_raw(no)?;
            minima.push(no);
        }
        if minima[0] != self.top_index.0[page]
            || minima.windows(2).any(|p| p[0] >= p[1])
            || minima.last().is_none_or(|s| *s > self.footer.max_seq_no)
            || self
                .top_index
                .0
                .get(page + 1)
                .is_some_and(|next| minima.last().unwrap() >= next)
        {
            return Err(invalid("invalid directory page index"));
        }
        crate::common::slow_trace::log_query_stage(
            "query_index_page_index_decode",
            decode_started,
            "validated directory minima",
        );
        crate::common::slow_trace::profile_index_metadata(
            "page_index", self as *const Self as usize,
            self.footer.page_index_offset + (start * 8) as u64, count,
        );
        Ok(minima)
    }
    pub(crate) fn segment_header(&self, entry: &DirEntry) -> Result<SegmentHeader> {
        let levels = super::segment::levels_from_bits(entry.level_bits)?;
        let size = 8 + LEVEL_HEADER_SIZE * levels.len();
        if size > entry.seg_len as usize {
            return Err(invalid("segment header exceeds segment"));
        }
        let bytes = self.read(entry.seg_offset, size)?;
        let decode_started = crate::common::slow_trace::now();
        let header = SegmentHeader::decode(&bytes, entry.seg_len)?;
        if header.typ != entry.typ
            || header.level_bits != entry.level_bits
            || header.levels.iter().map(|(_, h)| h.min_start).min() != Some(entry.min_start)
            || header.levels.iter().map(|(_, h)| h.max_end).max() != Some(entry.max_end)
        {
            return Err(invalid("segment directory mismatch"));
        }
        crate::common::slow_trace::log_query_stage(
            "query_index_segment_header_decode", decode_started, "validated segment header",
        );
        crate::common::slow_trace::profile_index_metadata(
            "segment_header", self as *const Self as usize, entry.seg_offset, 1,
        );
        Ok(header)
    }
    pub(crate) fn read_level(
        &self,
        entry: &DirEntry,
        header: &SegmentHeader,
        level: Level,
        t1: i64,
        t2: i64,
        cols: &[Column],
    ) -> Result<Vec<SelectedRecord>> {
        self.read_level_inner(entry, header, level, t1, t2, cols, false, None, None)
    }
    /// Decode only selected columns while retaining whole-group CRC validation.
    /// The current format has no independent column checksums.
    pub(crate) fn read_level_verified(
        &self,
        entry: &DirEntry,
        header: &SegmentHeader,
        level: Level,
        t1: i64,
        t2: i64,
        cols: &[Column],
    ) -> Result<Vec<SelectedRecord>> {
        self.read_level_inner(entry, header, level, t1, t2, cols, true, None, None)
    }
    /// Read only selected independently checked parts, with a full-group fallback
    /// for legacy groups that lack column checksums.
    pub(crate) fn read_level_projected(
        &self,
        entry: &DirEntry,
        header: &SegmentHeader,
        level: Level,
        t1: i64,
        t2: i64,
        cols: &[Column],
        metrics: &mut AggregateIndexReadMetrics,
    ) -> Result<Vec<SelectedRecord>> {
        self.read_level_inner(
            entry,
            header,
            level,
            t1,
            t2,
            cols,
            true,
            Some(metrics),
            None,
        )
    }
    pub(crate) fn read_level_projected_cached(
        &self,
        entry: &DirEntry,
        header: &SegmentHeader,
        level: Level,
        t1: i64,
        t2: i64,
        cols: &[Column],
        metrics: &mut AggregateIndexReadMetrics,
        cache: &mut super::caches::QueryGroupCache,
    ) -> Result<Vec<SelectedRecord>> {
        Ok(self
            .read_level_batches(entry, header, level, t1, t2, cols, metrics, cache)?
            .iter()
            .flat_map(super::projected_batch::SelectedBatch::records)
            .collect())
    }
    pub(crate) fn read_level_batches(
        &self,
        entry: &DirEntry,
        header: &SegmentHeader,
        level: Level,
        t1: i64,
        t2: i64,
        cols: &[Column],
        metrics: &mut AggregateIndexReadMetrics,
        cache: &mut super::caches::QueryGroupCache,
    ) -> Result<Vec<super::projected_batch::SelectedBatch>> {
        if t1 > t2 || header.typ != entry.typ || header.level_bits != entry.level_bits {
            return Err(invalid("invalid projected batch range or metadata"));
        }
        header.validate(entry.seg_len)?;
        let Some(h) = header.level(level) else {
            return Ok(Vec::new());
        };
        if t1 > h.max_end || t2 < h.min_start {
            return Ok(Vec::new());
        }
        let mut groups = GroupCursor::new_cached(self, entry, header, level, Some(cache))?;
        let begin = groups.table.predecessor(t1)?;
        let mask = cols.iter().fold(0_usize, |m, c| m | (1 << *c as usize));
        let mut result = Vec::new();
        let mut previous_end = None;
        for group in begin..groups.len() {
            let e = groups.entry(group)?;
            if e.first_start > t2 {
                break;
            }
            let key = (
                self.file_no,
                groups.level_offset + u64::from(e.group_offset),
                mask,
            );
            let (hit, records) = cache.group(level, key, || {
                groups.read_projected_group_columns(e, cols, metrics)
            })?;
            if hit {
                metrics.full_group_bytes += e.byte_len(&ALL_COLUMNS) as u64;
            }
            if records.positions[0].start < h.min_start
                || records.positions.last().unwrap().end > h.max_end
                || previous_end.is_some_and(|end| records.positions[0].start <= end)
            {
                return Err(invalid("page group exceeds level range"));
            }
            previous_end = Some(records.positions.last().unwrap().end);
            let start = records.positions.partition_point(|p| p.end < t1);
            let end = records.positions.partition_point(|p| p.start <= t2);
            if start < end {
                result.push(super::projected_batch::SelectedBatch {
                    group: Arc::clone(records),
                    rows: start..end,
                });
            }
        }
        Ok(result)
    }
    fn read_level_inner(
        &self,
        entry: &DirEntry,
        header: &SegmentHeader,
        level: Level,
        t1: i64,
        t2: i64,
        cols: &[Column],
        verify: bool,
        mut projection_reads: Option<&mut AggregateIndexReadMetrics>,
        mut cache: Option<&mut super::caches::QueryGroupCache>,
    ) -> Result<Vec<SelectedRecord>> {
        if t1 > t2 {
            return Err(invalid("reversed query time range"));
        }
        if header.typ != entry.typ || header.level_bits != entry.level_bits {
            return Err(invalid("level read uses mismatched segment metadata"));
        }
        let Some(i) = header.levels.iter().position(|(l, _)| *l == level) else {
            return Ok(Vec::new());
        };
        header.validate(entry.seg_len)?;
        let h = &header.levels[i].1;
        if t1 > h.max_end || t2 < h.min_start {
            return Ok(Vec::new());
        }
        let mut groups = self.groups(entry, header, level)?;
        let begin = groups.table.predecessor(t1)?;
        let mut result: Vec<SelectedRecord> = Vec::new();
        let mut previous_end = None;
        for group in begin..groups.len() {
            let e = groups.entry(group)?;
            if e.first_start > t2 {
                break;
            }
            let records: std::borrow::Cow<'_, [SelectedRecord]> =
                if let Some(metrics) = projection_reads.as_deref_mut() {
                    if let Some(cache) = cache.as_deref_mut() {
                        let mask = cols
                            .iter()
                            .fold(0_usize, |mask, column| mask | (1 << *column as usize));
                        let key = (
                            self.file_no,
                            groups.level_offset + u64::from(e.group_offset),
                            mask,
                        );
                        let (hit, records) = cache.group(level, key, || {
                            groups.read_projected_group_columns(e, cols, metrics)
                        })?;
                        if hit {
                            metrics.full_group_bytes += e.byte_len(&ALL_COLUMNS) as u64;
                        }
                        std::borrow::Cow::Owned(records.records())
                    } else {
                        std::borrow::Cow::Owned(groups.read_projected_group(e, cols, metrics)?)
                    }
                } else {
                    let n = e.row_count as usize;
                    let started = crate::common::slow_trace::now();
                    let (offset, len) = e.read_range(
                        groups.level_offset,
                        if verify { &ALL_COLUMNS } else { cols },
                    )?;
                    let bytes = self.read(offset, len)?;
                    let selected_len = e.byte_len(cols);
                    // decode_group verifies the CRC itself when its input is complete.
                    if verify && selected_len != bytes.len() {
                        e.verify_payload(&bytes)?;
                    }
                    crate::common::slow_trace::log_query_stage(
                        "query_index_group_read",
                        started,
                        "aggregate page group read and checksum",
                    );
                    let started = crate::common::slow_trace::now();
                    let records = decode_group(
                        entry.seq_no,
                        level,
                        entry.typ,
                        &bytes[..selected_len],
                        &e,
                        n,
                        cols,
                    )?;
                    crate::common::slow_trace::log_query_stage(
                        "query_index_column_decode",
                        started,
                        "aggregate selected columns",
                    );
                    std::borrow::Cow::Owned(records)
                };
            if records[0].pos.start < h.min_start
                || records.last().unwrap().pos.end > h.max_end
                || previous_end.is_some_and(|end| records[0].pos.start <= end)
            {
                return Err(invalid("page group exceeds level range"));
            }
            previous_end = Some(records.last().unwrap().pos.end);
            let started = crate::common::slow_trace::now();
            match records {
                std::borrow::Cow::Owned(records) => result.extend(
                    records
                        .into_iter()
                        .filter(|r| r.pos.start <= t2 && r.pos.end >= t1),
                ),
                std::borrow::Cow::Borrowed(records) => result.extend(
                    records
                        .iter()
                        .filter(|r| r.pos.start <= t2 && r.pos.end >= t1)
                        .cloned(),
                ),
            }
            crate::common::slow_trace::log_query_stage(
                "query_index_selected_materialization",
                started,
                "selected group records",
            );
        }
        Ok(result)
    }
    /// Opens a paged group directory without materializing a field's records.
    pub(crate) fn groups<'a>(
        &'a self,
        entry: &DirEntry,
        header: &SegmentHeader,
        level: Level,
    ) -> Result<GroupCursor<&'a IndexFile>> {
        GroupCursor::new(self, entry, header, level)
    }
    pub(crate) fn owned_groups(
        self: &Arc<Self>,
        entry: &DirEntry,
        header: &SegmentHeader,
        level: Level,
    ) -> Result<GroupCursor<Arc<IndexFile>>> {
        GroupCursor::new(Arc::clone(self), entry, header, level)
    }
    pub(crate) fn read_level_tail(
        &self,
        entry: &DirEntry,
        header: &SegmentHeader,
        level: Level,
        after: i64,
    ) -> Result<Vec<Record>> {
        let Some(start) = after.checked_add(1) else {
            return Ok(Vec::new());
        };
        self.read_level(entry, header, level, start, i64::MAX, &ALL_COLUMNS)?
            .into_iter()
            .filter(|r| r.pos.start > after)
            .map(SelectedRecord::into_record)
            .collect()
    }
}

pub(crate) struct GroupCursor<F> {
    seq_no: SeqNo,
    level: Level,
    typ: crate::models::FieldType,
    level_offset: u64,
    table: GroupTable<F>,
}
impl<F: Deref<Target = IndexFile>> GroupCursor<F> {
    fn read_projected_group(
        &self,
        entry: PageGroupEntry,
        cols: &[Column],
        metrics: &mut AggregateIndexReadMetrics,
    ) -> Result<Vec<SelectedRecord>> {
        Ok(self
            .read_projected_group_columns(entry, cols, metrics)?
            .records())
    }
    fn read_projected_group_columns(
        &self,
        entry: PageGroupEntry,
        cols: &[Column],
        metrics: &mut AggregateIndexReadMetrics,
    ) -> Result<super::projected_batch::ProjectedGroup> {
        let started = crate::common::slow_trace::now();
        crate::common::slow_trace::profile_index_group(
            self.table.file.file_no,
            self.level_offset + u64::from(entry.group_offset),
            u32::from(entry.row_count),
        );
        let full_len = entry.byte_len(&ALL_COLUMNS);
        metrics.full_group_bytes += full_len as u64;
        metrics.decoded_columns += ALL_COLUMNS.iter().filter(|c| cols.contains(c)).count() as u64;
        if !entry.column_checksums {
            let (offset, len) = entry.read_range(self.level_offset, &ALL_COLUMNS)?;
            let bytes = self.table.file.read(offset, len)?;
            entry.verify_payload(&bytes)?;
            metrics.requested_bytes += len as u64;
            metrics.read_requests += 1;
            metrics.legacy_groups += 1;
            crate::common::slow_trace::log_query_stage(
                "query_index_group_read",
                started,
                "legacy full group",
            );
            let started = crate::common::slow_trace::now();
            let result = decode_group_columns(
                self.seq_no,
                self.level,
                self.typ,
                &bytes[..entry.byte_len(cols)],
                &entry,
                cols,
            );
            crate::common::slow_trace::log_query_stage(
                "query_index_column_decode",
                started,
                "legacy projected decode",
            );
            return result;
        }
        let plan = entry.read_plan(cols);
        let base = self.level_offset + u64::from(entry.group_offset);
        let mut runs = Vec::new();
        for range in plan.ranges() {
            let len = range.bytes.len();
            let bytes = self.table.file.read(base + range.bytes.start as u64, len)?;
            metrics.requested_bytes += len as u64;
            metrics.read_requests += 1;
            runs.push((range, bytes));
        }
        if plan.is_complete() {
            entry.verify_payload(&runs[0].1)?;
        }
        crate::common::slow_trace::log_query_stage(
            "query_index_group_read",
            started,
            "projected column ranges",
        );
        let started = crate::common::slow_trace::now();
        let mut parts = [None; PART_COUNT];
        for (range, bytes) in &runs {
            for i in range.parts.clone() {
                let part = plan.part_range(i);
                let start = part.start - range.bytes.start;
                parts[i] = Some(&bytes[start..start + part.len()]);
            }
        }
        let result = decode_columns(
            self.seq_no,
            self.level,
            self.typ,
            &entry,
            parts[0].unwrap(),
            std::array::from_fn(|i| parts[i + 1]),
        );
        crate::common::slow_trace::log_query_stage(
            "query_index_column_decode",
            started,
            "projected columns and checksums",
        );
        result
    }
    fn new(file: F, entry: &DirEntry, header: &SegmentHeader, level: Level) -> Result<Self> {
        Self::new_cached(file, entry, header, level, None)
    }
    fn new_cached(
        file: F, entry: &DirEntry, header: &SegmentHeader, level: Level,
        cache: Option<&mut super::caches::QueryGroupCache>,
    ) -> Result<Self> {
        if header.typ != entry.typ || header.level_bits != entry.level_bits {
            return Err(invalid("group read uses mismatched segment metadata"));
        }
        header.validate(entry.seg_len)?;
        let h = header
            .level(level)
            .ok_or(invalid("missing aggregate level"))?;
        let level_offset = entry.seg_offset + u64::from(h.level_offset);
        let mut table = GroupTable::new(
            file,
            level_offset + u64::from(h.table_offset),
            h.group_count as usize,
            u64::from(h.table_offset),
            level,
            cache,
        )?;
        let first = table.get(0)?;
        let last = table.get(table.groups - 1)?;
        if first.first_start != h.min_start
            || first.first_row != 0
            || first.group_offset != 0
            || last.last_end != h.max_end
            || last.first_row.checked_add(u32::from(last.row_count)) != Some(h.count)
            || u64::from(last.group_offset) + last.byte_len(&ALL_COLUMNS) as u64
                != u64::from(h.table_offset)
        {
            return Err(invalid("page groups do not cover level bounds"));
        }
        Ok(Self {
            seq_no: entry.seq_no,
            level,
            typ: entry.typ,
            level_offset,
            table,
        })
    }
    pub(crate) fn predecessor(&mut self, time: i64) -> Result<usize> {
        self.table.predecessor(time)
    }
    pub(crate) fn len(&self) -> usize {
        self.table.groups
    }
    pub(crate) fn entry(&mut self, index: usize) -> Result<PageGroupEntry> {
        let entry = self.table.get(index)?;
        if index > 0 {
            contiguous_groups(&self.table.get(index - 1)?, &entry)?;
        }
        if index + 1 < self.len() {
            contiguous_groups(&entry, &self.table.get(index + 1)?)?;
        }
        Ok(entry)
    }
    pub(crate) fn read(&mut self, index: usize) -> Result<EncodedPageGroup> {
        let entry = self.entry(index)?;
        let (offset, len) = entry.read_range(self.level_offset, &ALL_COLUMNS)?;
        let bytes = self.table.file.read(offset, len)?;
        entry.verify_payload(&bytes)?;
        Ok(EncodedPageGroup {
            seq_no: self.seq_no,
            level: self.level,
            typ: self.typ,
            entry,
            bytes: bytes.into_vec(),
        })
    }
    pub(crate) fn read_selected(
        &mut self,
        index: usize,
        cols: &[Column],
    ) -> Result<Vec<SelectedRecord>> {
        let entry = self.entry(index)?;
        let (offset, len) = entry.read_range(self.level_offset, cols)?;
        let bytes = self.table.file.read(offset, len)?;
        decode_group(
            self.seq_no,
            self.level,
            self.typ,
            &bytes,
            &entry,
            entry.row_count as usize,
            cols,
        )
    }

    pub(crate) fn read_selected_measured(
        &mut self,
        index: usize,
        cols: &[Column],
        metrics: &mut AggregateIndexReadMetrics,
    ) -> Result<Vec<SelectedRecord>> {
        let entry = self.entry(index)?;
        self.read_projected_group(entry, cols, metrics)
    }
}

fn contiguous_groups(left: &PageGroupEntry, right: &PageGroupEntry) -> Result<()> {
    if left.last_end >= right.first_start
        || u64::from(left.group_offset) + left.byte_len(&ALL_COLUMNS) as u64
            != u64::from(right.group_offset)
        || left.first_row.checked_add(u32::from(left.row_count)) != Some(right.first_row)
    {
        return Err(invalid("noncontiguous or unordered page groups"));
    }
    Ok(())
}

struct GroupTable<F> {
    file: F,
    offset: u64,
    groups: usize,
    data_len: u64,
    whole: Option<Arc<[PageGroupEntry]>>,
    cached: Option<(usize, Vec<PageGroupEntry>)>,
}
impl<F: Deref<Target = IndexFile>> GroupTable<F> {
    fn new(file: F, offset: u64, groups: usize, data_len: u64, level: Level,
        cache: Option<&mut super::caches::QueryGroupCache>) -> Result<Self> {
        let mut table = Self {
            file,
            offset,
            groups,
            data_len,
            whole: None,
            cached: None,
        };
        if groups == 0 {
            return Err(invalid("empty group table"));
        }
        if groups * PageGroupEntry::SIZE <= TABLE_WHOLE_PAGES * PAGE {
            table.whole = Some(if let Some(cache) = cache {
                cache.table(level, (table.file.file_no, offset, groups, data_len),
                    || table.read_entries(0, groups))?
            } else {
                table.read_entries(0, groups)?.into()
            });
        }
        Ok(table)
    }
    fn read_entries(&self, start: usize, count: usize) -> Result<Vec<PageGroupEntry>> {
        let bytes = self.file.read(
            self.offset + (start * PageGroupEntry::SIZE) as u64,
            count * PageGroupEntry::SIZE,
        )?;
        let decode_started = crate::common::slow_trace::now();
        let entries = bytes
            .chunks_exact(PageGroupEntry::SIZE)
            .map(PageGroupEntry::decode)
            .collect::<Result<Vec<_>>>()?;
        if entries.iter().any(|e| {
            u64::from(e.group_offset) + e.byte_len(&ALL_COLUMNS) as u64 > self.data_len
                || (e.column_checksums && !self.file.format.supports_column_checksums())
        }) || entries
            .windows(2)
            .any(|p| contiguous_groups(&p[0], &p[1]).is_err())
        {
            return Err(invalid("invalid group table order or bounds"));
        }
        crate::common::slow_trace::log_query_stage(
            "query_index_group_table_decode", decode_started, "validated group table entries",
        );
        crate::common::slow_trace::profile_index_metadata(
            "group_table", &*self.file as *const IndexFile as usize,
            self.offset + (start * PageGroupEntry::SIZE) as u64, count,
        );
        Ok(entries)
    }
    fn get(&mut self, index: usize) -> Result<PageGroupEntry> {
        if index >= self.groups {
            return Err(invalid("group index out of bounds"));
        }
        if let Some(entries) = &self.whole {
            return Ok(entries[index]);
        }
        let page = index / GROUPS_PER_PAGE;
        if self.cached.as_ref().is_none_or(|(p, _)| *p != page) {
            let start = page * GROUPS_PER_PAGE;
            self.cached = Some((
                page,
                self.read_entries(start, (self.groups - start).min(GROUPS_PER_PAGE))?,
            ));
        }
        Ok(self.cached.as_ref().unwrap().1[index % GROUPS_PER_PAGE])
    }
    fn predecessor(&mut self, time: i64) -> Result<usize> {
        if let Some(entries) = &self.whole {
            return Ok(entries
                .partition_point(|e| e.first_start <= time)
                .saturating_sub(1));
        }
        let (mut lo, mut hi) = (0, self.groups.div_ceil(GROUPS_PER_PAGE));
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.get(mid * GROUPS_PER_PAGE)?.first_start <= time {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let page = lo.saturating_sub(1);
        let start = page * GROUPS_PER_PAGE;
        self.get(start)?;
        let entries = &self.cached.as_ref().unwrap().1;
        Ok(start
            + entries
                .partition_point(|e| e.first_start <= time)
                .saturating_sub(1))
    }
}

#[cfg(test)]
#[path = "file_tests.rs"]
mod tests;
