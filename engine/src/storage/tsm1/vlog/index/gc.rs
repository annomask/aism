//! Evacuate compressed live blocks from VLogs with at least half dead bytes.
//! Accounting is per physical file, never per series or field.

use super::super::{
    compact::check_cancelled,
    io_budget::IoBudget,
    page_group::{ALL_COLUMNS, Column, decode_group},
    repair::RepairWriter,
    writer::IndexFileWriter,
};
use super::*;
use std::collections::BTreeMap;

#[derive(Default, Debug)]
pub(crate) struct GcStats {
    pub(crate) source_files: usize,
    pub(crate) copied_blocks: usize,
    pub(crate) copied_bytes: u64,
}

fn visit_leaves(
    file: &IndexFile,
    cancelled: &impl Fn() -> bool,
    mut visit: impl FnMut(super::super::Position) -> Result<()>,
) -> Result<()> {
    for page in 0..file.footer.directory_pages() {
        for entry in file.directory_page(page)? {
            check_cancelled(cancelled)?;
            let header = file.segment_header(&entry)?;
            if header.level(LEVEL_BLOCK).is_none() {
                continue;
            }
            let mut groups = file.groups(&entry, &header, LEVEL_BLOCK)?;
            for group in 0..groups.len() {
                check_cancelled(cancelled)?;
                let range = groups.entry(group)?;
                for record in file.read_level(
                    &entry,
                    &header,
                    LEVEL_BLOCK,
                    range.first_start,
                    range.last_end,
                    &[Column::Count],
                )? {
                    visit(record.pos)?;
                }
            }
        }
    }
    Ok(())
}

impl IndexStore {
    /// Copy selected payloads without decoding points or rebuilding summaries.
    /// Publication uses the same durable replacement protocol as compaction.
    pub(crate) fn collect_garbage(
        &self,
        generations: super::super::allocation::OutputGenerations,
        max_source_bytes: u64,
        budget: IoBudget,
        cancelled: &impl Fn() -> bool,
    ) -> Result<GcStats> {
        if self.native_metadata {
            return Ok(GcStats::default());
        }
        if self
            .compacting
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(super::super::Error::CompactionBusy);
        }
        let _running = scopeguard::guard((), |_| self.compacting.store(false, Ordering::Release));
        self.ensure_ready()?;
        let started = slow_trace::now();
        let (indexes, readers, _) = self.read_view();
        if indexes
            .iter()
            .any(|f| i64::from(f.file_no >> 8) >= generations.first)
        {
            return Err(invalid(
                "garbage collection generation does not follow inputs",
            ));
        }
        let mut live_bytes = BTreeMap::<u32, u64>::new();
        for file in &indexes {
            visit_leaves(file, cancelled, |pos| {
                let bytes = live_bytes.entry(pos.block.file_no).or_default();
                *bytes = bytes
                    .checked_add(u64::from(pos.block_size))
                    .ok_or(super::super::Error::Overflow)?;
                Ok(())
            })?;
        }
        let mut selected = BTreeMap::new();
        let mut source_bytes = 0u64;
        for reader in &readers {
            let id = ((reader.generation as u32) << 8) | reader.seq as u32;
            let live = live_bytes.get(&id).copied().unwrap_or(0);
            // Fully unused files are handled by checkpoint without copying.
            if live == 0
                || live.saturating_mul(2) > reader.size().saturating_sub(5)
                || reader.size() > max_source_bytes.saturating_sub(source_bytes)
            {
                continue;
            }
            selected.insert(id, File::open(reader.path())?);
            source_bytes += reader.size();
        }
        if selected.is_empty() {
            return Ok(GcStats::default());
        }
        let mut stats = GcStats::default();
        let mut copied_sources = HashSet::new();
        let mut data = RepairWriter::new(&self.path, generations.clone());
        data.set_io_budget(budget.clone());
        let mut inputs = Vec::new();
        let mut outputs = Vec::new();
        let mut cleanup = scopeguard::guard(Vec::<PathBuf>::new(), |paths| {
            for path in paths {
                let _ = remove_if_exists(&path);
            }
        });
        let mut source_generation = None;
        for input in &indexes {
            let mut affected = false;
            visit_leaves(input, cancelled, |pos| {
                affected |= selected.contains_key(&pos.block.file_no);
                Ok(())
            })?;
            if affected {
                source_generation = Some(input.generation);
                break;
            }
        }
        let Some(source_generation) = source_generation else {
            return Ok(GcStats::default());
        };
        let mut generation_inputs = indexes
            .iter()
            .filter(|f| f.generation == source_generation)
            .collect::<Vec<_>>();
        generation_inputs.sort_by_key(|f| f.seq);
        let first_sequence = generation_inputs[0].seq.min(4);
        let mut identities = super::super::allocation::OutputIds::new(generations, first_sequence);
        // Relocation preserves the entire source generation and its starting level.
        for input in generation_inputs {
            let (generation, seq) = identities.next_index()?;
            let path = self.path.join(format!("{generation:09}-{seq:09}.idx.tmp"));
            let mut writer = IndexFileWriter::create(&path, generation, seq)?;
            cleanup.push(path);
            writer.set_io_budget(budget.clone());
            for page in 0..input.footer.directory_pages() {
                for entry in input.directory_page(page)? {
                    let header = input.segment_header(&entry)?;
                    writer.begin_segment(entry.seq_no, entry.typ)?;
                    for (level, _) in &header.levels {
                        let mut groups = input.groups(&entry, &header, *level)?;
                        for i in 0..groups.len() {
                            check_cancelled(cancelled)?;
                            let group = groups.read(i)?;
                            if *level != LEVEL_BLOCK {
                                writer.append_group(group)?;
                                continue;
                            }
                            let records = decode_group(
                                group.seq_no,
                                group.level,
                                group.typ,
                                &group.bytes,
                                &group.entry,
                                group.entry.row_count as usize,
                                &ALL_COLUMNS,
                            )?;
                            if !records
                                .iter()
                                .any(|r| selected.contains_key(&r.pos.block.file_no))
                            {
                                writer.append_group(group)?;
                                continue;
                            }
                            for record in records {
                                let mut record = record.into_record()?;
                                if let Some(fd) = selected.get(&record.pos.block.file_no) {
                                    copied_sources.insert(record.pos.block.file_no);
                                    let mut bytes = vec![0; record.pos.block_size as usize];
                                    super::super::reader::pread(
                                        fd,
                                        u64::from(record.pos.block.offset),
                                        &mut bytes,
                                    )?;
                                    if bytes.len() < 5
                                        || crc32fast::hash(&bytes[4..]).to_be_bytes() != bytes[..4]
                                    {
                                        return Err(invalid(
                                            "garbage collection VLog checksum mismatch",
                                        ));
                                    }
                                    stats.copied_blocks += 1;
                                    stats.copied_bytes += bytes.len() as u64;
                                    record = data.relocate(record, &bytes[4..])?;
                                }
                                writer.push(&record)?;
                            }
                        }
                    }
                    writer.end_segment()?;
                }
            }
            let output = writer.finish()?;
            if let Some(tombstone) = &input.tombstone {
                let mut builder = self.delete_batch_builder();
                for page in tombstone.pages() {
                    for range in page? {
                        builder.push(range)?;
                    }
                }
                let path = super::super::tombstone::path(&output.path);
                cleanup.push(path.clone());
                let owner = super::super::allocation::index_file_no(output.generation, output.seq)?;
                super::super::tombstone::Tombstone::write(&path, owner, &builder.finish()?)?;
            }
            outputs.push(output);
            inputs.push(Arc::clone(input));
        }
        let vlogs = data.finish()?;
        stats.source_files = copied_sources.len();
        cleanup.extend(vlogs.iter().map(|(path, _, _)| path.clone()));
        self.replace_data_cancellable(&inputs, &outputs, &vlogs, cancelled)?;
        slow_trace::log_background_stage(
            "compaction_vlog_gc",
            started,
            "aggregate live block evacuation",
        );
        Ok(stats)
    }
}
