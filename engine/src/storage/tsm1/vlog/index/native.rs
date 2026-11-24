//! Hierarchy files referenced by native locator records. Cache mutation and
//! locator-WAL replay are the only publication and invalidation authorities.

use super::super::{Record, SeqNo, writer::IndexFileMeta};
use super::*;
use crate::storage::tsm1::{
    engine::VLogInner,
    vlog::{
        VLogIndex,
        aggregate_record::{AggregateRecord, AggregateSources},
    },
};
use std::collections::{BTreeMap, HashMap};

type KeyLookup = dyn Fn(SeqNo) -> Result<Vec<u8>> + Send + Sync;
type Locators = HashMap<String, Vec<VLogIndex>>;

#[derive(Clone)]
pub(crate) struct NativeState {
    inner: Weak<RwLock<VLogInner>>,
    key: Arc<KeyLookup>,
}

/// The native reader reference protocol also protects aggregate queries.
pub(crate) struct NativeRead {
    readers: Vec<Arc<VLogReader>>,
    sources: AggregateSources,
    deleted: HashMap<(i64, i64), Vec<(i64, i64)>>,
}
impl Drop for NativeRead {
    fn drop(&mut self) {
        for reader in &self.readers {
            reader.unref_reader();
        }
    }
}
impl NativeRead {
    pub(crate) fn tombstones(
        &self,
        file: &IndexFile,
        seq: SeqNo,
    ) -> super::super::tombstone::TombstoneView {
        let source = self
            .sources
            .get(&file.file_no)
            .expect("captured native hierarchy source");
        if self.deleted.is_empty() {
            return file.tombstone_view(seq).with_removed(Arc::clone(source));
        }
        let mut source = source.as_ref().clone();
        for (&(g, s, _), &(min, max, _)) in source.blocks.iter() {
            if let Some(ranges) = self.deleted.get(&(g, s)) {
                for &(lo, hi) in ranges {
                    if lo <= max && hi >= min {
                        source.removed.push((lo.max(min), hi.min(max)));
                    }
                }
            }
        }
        source.normalize_removed();
        file.tombstone_view(seq).with_removed(Arc::new(source))
    }
}

impl IndexStore {
    pub(crate) fn open_locator_store(path: &Path, wal_path: &Path) -> Result<Arc<Self>> {
        const MODE: &[u8] = b"ReefsDB native aggregate locators 5\n";
        Self::check_mode(path, true)?;
        fs::create_dir_all(path)?;
        fs::create_dir_all(wal_path)?;
        let mode = path.join(MODE_FILE);
        if mode.exists() {
            if fs::read(&mode)? != MODE {
                return Err(invalid(
                    "aggregate locator format requires an explicit migration",
                ));
            }
        } else {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&mode)?;
            file.write_all(MODE)?;
            file.sync_all()?;
            sync_dir(path)?;
        }
        Ok(Arc::new(Self {
            cache: super::super::caches::ReadCache::new(super::super::caches::DEFAULT_CACHE_BYTES),
            path: path.into(),
            wal_path: wal_path.into(),
            files: RwLock::new(LiveFiles::default()),
            commit_gate: Mutex::new(()),
            installing: AtomicBool::new(false),
            recovery_required: AtomicBool::new(false),
            compacting: AtomicBool::new(false),
            native: RwLock::new(None),
            native_metadata: true,
            native_generation: std::sync::atomic::AtomicI64::new(0),
        }))
    }

    pub(crate) fn bind_native(&self, inner: &Arc<RwLock<VLogInner>>, key: Arc<KeyLookup>) {
        assert!(self.native_metadata);
        *self.native.write() = Some(NativeState {
            inner: Arc::downgrade(inner),
            key,
        });
    }

    fn native_binding(&self) -> Result<(NativeState, Arc<RwLock<VLogInner>>)> {
        let binding = self
            .native
            .read()
            .clone()
            .ok_or(invalid("native locator store is not bound"))?;
        let inner = binding
            .inner
            .upgrade()
            .ok_or(invalid("native VLog store is closed"))?;
        Ok((binding, inner))
    }

    pub(super) fn native_readers(&self) -> Option<Vec<Arc<VLogReader>>> {
        let (_, inner) = self.native_binding().ok()?;
        let readers = inner.read().vlog_file_store.reader_snapshot();
        Some(readers)
    }

    /// Called after the original locator WAL has been replayed. Obsolete WAL
    /// records contain their own summaries and do not need old hierarchy files.
    pub(crate) fn recover_native(&self) -> Result<()> {
        let (_, inner) = self.native_binding()?;
        let ids = inner.read().vlog_index_cache.aggregate_files();
        let mut names = HashMap::new();
        for item in fs::read_dir(&self.path)? {
            let item = item?;
            let name = item.file_name().to_string_lossy().into_owned();
            if name.ends_with(".idx") {
                let id = super::super::allocation::index_file_no_from_name(&name)?;
                self.native_generation
                    .fetch_max(i64::from(id >> 8), Ordering::Release);
                if names.insert(id, name).is_some() {
                    return Err(invalid("duplicate hierarchy file identity"));
                }
            }
        }
        let mut files = Vec::new();
        for id in ids {
            let name = names.get(&id).ok_or(invalid(
                "native locator references a missing hierarchy file",
            ))?;
            let (g, s) = name.strip_suffix(".idx").unwrap().split_once('-').unwrap();
            let file = IndexFile::open(
                &self.path.join(name),
                g.parse().unwrap(),
                s.parse().unwrap(),
            )?;
            file.set_cache(Arc::clone(&self.cache));
            files.push(file);
        }
        self.files.write().indexes = files;
        Ok(())
    }

    /// Durable file preparation only. The returned readers pin outputs until
    /// the caller writes enriched locators through the original cache and WAL.
    pub(crate) fn prepare_native(
        &self,
        outputs: &[IndexFileMeta],
        locators: &mut Locators,
    ) -> Result<Vec<Arc<IndexFile>>> {
        let (binding, _) = self.native_binding()?;
        let mut readers = Vec::new();
        let mut attached = 0;
        for output in outputs {
            let name = output_name(output.generation, output.seq, "idx");
            let path = self.path.join(&name);
            if output.path != self.path.join(format!("{name}.tmp")) || path.exists() {
                return Err(invalid(
                    "hierarchy output must have a fresh temporary identity",
                ));
            }
            File::open(&output.path)?.sync_all()?;
            fs::rename(&output.path, &path)?;
            let file = IndexFile::open(&path, output.generation, output.seq)?;
            file.set_cache(Arc::clone(&self.cache));
            self.native_generation
                .fetch_max(i64::from(file.file_no >> 8), Ordering::Release);
            super::super::compact::visit_fields(&[Arc::clone(&file)], &|| false, |entries| {
                let seq = entries[0].1.seq_no;
                let key = String::from_utf8((binding.key)(seq)?)
                    .map_err(|_| invalid("invalid native series key"))?;
                let indexes = locators
                    .get_mut(&key)
                    .ok_or(invalid("hierarchy has no native locator key"))?;
                let positions = indexes
                    .iter()
                    .enumerate()
                    .map(|(i, v)| ((v.generation, v.seq, v.offset), i))
                    .collect::<HashMap<_, _>>();
                for (source, entry) in entries {
                    let header = source.segment_header(&entry)?;
                    if header.level(LEVEL_BLOCK).is_none() {
                        continue;
                    }
                    let mut groups = source.groups(&entry, &header, LEVEL_BLOCK)?;
                    for group in 0..groups.len() {
                        for record in
                            groups.read_selected(group, &super::super::page_group::ALL_COLUMNS)?
                        {
                            let record = record.into_record()?;
                            let old = record.to_vlog_index()?;
                            let i = *positions
                                .get(&(old.generation, old.seq, old.offset))
                                .ok_or(invalid("hierarchy has no native locator"))?;
                            if old != indexes[i] {
                                return Err(invalid("aggregate and native locator disagree"));
                            }
                            indexes[i].aggregate = Some(Arc::new(AggregateRecord {
                                file: source.file_no,
                                seq,
                                version: record.pos.version,
                                values: record.values,
                            }));
                            attached += 1;
                        }
                    }
                }
                Ok(())
            })?;
            readers.push(file);
        }
        if attached != locators.values().map(Vec::len).sum::<usize>() {
            return Err(invalid("incomplete aggregate locator output"));
        }
        sync_dir(&self.path)?;
        // This is a reader cache, not a live-file publication. Queries select
        // only files referenced by their native cache entry.
        self.files.write().indexes.extend(readers.iter().cloned());
        Ok(readers)
    }

    pub(crate) fn compaction_aggregates(
        &self,
        _seq: SeqNo,
        indexes: &[VLogIndex],
    ) -> Result<crate::storage::tsm1::vlog::compact::AggregateInputs> {
        indexes
            .iter()
            .map(|index| {
                let record = index
                    .aggregate
                    .as_ref()
                    .ok_or(invalid("native locator is missing its aggregate"))?;
                Ok((
                    (index.generation, index.seq, index.offset),
                    Arc::new(record.values),
                ))
            })
            .collect()
    }

    pub(crate) fn native_query_view(
        &self,
        seq: SeqNo,
    ) -> Result<(
        Vec<Arc<IndexFile>>,
        Vec<Arc<VLogReader>>,
        Option<NativeRead>,
    )> {
        if !self.native_metadata {
            let (files, readers, _) = self.read_view();
            return Ok((files, readers, None));
        }
        let (binding, _) = self.native_binding()?;
        let key = (binding.key)(seq)?;
        self.native_query_view_key(&key)
    }

    fn native_query_view_key(
        &self,
        key: &[u8],
    ) -> Result<(
        Vec<Arc<IndexFile>>,
        Vec<Arc<VLogReader>>,
        Option<NativeRead>,
    )> {
        let (_, inner) = self.native_binding()?;
        let started = slow_trace::now();
        let (files, readers, sources) = {
            let guard = inner.read();
            let sources = guard.vlog_index_cache.aggregate_sources(&key);
            let files = self
                .files
                .read()
                .indexes
                .iter()
                .filter(|f| sources.contains_key(&f.file_no))
                .cloned()
                .collect::<Vec<_>>();
            if files.len() != sources.len() {
                return Err(invalid("native hierarchy reader is missing"));
            }
            (files, guard.vlog_file_store.pin_readers(), sources)
        };
        slow_trace::log_query_stage(
            "query_native_locator_lookup",
            started,
            "native aggregate entry",
        );
        let deleted = readers
            .iter()
            .filter(|r| r.has_tombstones())
            .filter_map(|r| {
                let ranges = r
                    .tombstone_range(&key)
                    .iter()
                    .map(|t| (t.min, t.max))
                    .collect::<Vec<_>>();
                (!ranges.is_empty()).then_some(((r.generation, r.seq), ranges))
            })
            .collect();
        let lease = NativeRead {
            readers: readers.clone(),
            sources,
            deleted,
        };
        Ok((files, readers, Some(lease)))
    }

    pub(super) fn native_index_readers(&self) -> Option<Vec<Arc<IndexFile>>> {
        if !self.native_metadata {
            return None;
        }
        let (_, inner) = self.native_binding().ok()?;
        let guard = inner.read();
        let ids = guard.vlog_index_cache.aggregate_files();
        Some(
            self.files
                .read()
                .indexes
                .iter()
                .filter(|f| ids.contains(&f.file_no))
                .cloned()
                .collect(),
        )
    }

    pub(super) fn retire_native_indexes(&self) -> Result<()> {
        if !self.native_metadata {
            return Ok(());
        }
        let (_, inner) = self.native_binding()?;
        let guard = inner.read();
        let live = guard.vlog_index_cache.aggregate_files();
        let mut files = self.files.write();
        let old = std::mem::take(&mut files.indexes);
        for file in old {
            if !live.contains(&file.file_no) && Arc::strong_count(&file) == 1 {
                files.retired.push(RetiredIndex {
                    path: file.path().into(),
                    file_no: file.file_no,
                    reader: Arc::downgrade(&file),
                });
            } else {
                files.indexes.push(file);
            }
        }
        Ok(())
    }

    /// Export the original locator WAL together with the files it currently
    /// references. Historical deletes in that WAL also reconstruct partially
    /// invalidated hierarchy files; no separate aggregate recovery log is used.
    pub(super) fn create_native_snapshot(&self, parent: &Path) -> Result<PathBuf> {
        let (_, inner) = self.native_binding()?;
        let (indexes, readers, segments) = {
            let guard = inner.read();
            // Rotation must share the native publication boundary. Only the
            // segment fsync occurs here; file linking happens after the lock.
            guard.vlog_index_wal.close_segment().map_err(|e| {
                std::io::Error::other(format!("locator WAL snapshot rotation failed: {e}"))
            })?;
            let segments = guard.vlog_index_wal.closed_segments().map_err(|e| {
                std::io::Error::other(format!("locator WAL snapshot enumeration failed: {e}"))
            })?;
            let ids = guard.vlog_index_cache.aggregate_files();
            let indexes = self
                .files
                .read()
                .indexes
                .iter()
                .filter(|f| ids.contains(&f.file_no))
                .cloned()
                .collect::<Vec<_>>();
            if indexes.len() != ids.len() {
                return Err(invalid("native hierarchy reader is missing"));
            }
            (indexes, guard.vlog_file_store.pin_readers(), segments)
        };
        let _pins = scopeguard::guard(readers, |files| {
            for reader in files {
                reader.unref_reader();
            }
        });
        let dir = tempfile::Builder::new()
            .prefix("agg-snapshot-")
            .suffix(".tmp")
            .tempdir_in(parent)?;
        fs::copy(self.path.join(MODE_FILE), dir.path().join(MODE_FILE))?;
        for file in &indexes {
            fs::hard_link(
                file.path(),
                dir.path().join(file.path().file_name().unwrap()),
            )?;
        }
        for file in _pins.iter() {
            let path = file.path();
            fs::hard_link(&path, dir.path().join(path.file_name().unwrap()))?;
            let tombstone = file.tombstone_stats();
            if tombstone.tombstone_exists {
                fs::hard_link(
                    &tombstone.path,
                    dir.path().join(tombstone.path.file_name().unwrap()),
                )?;
            }
        }
        let wal = dir.path().join("vlog_index_wal");
        fs::create_dir(&wal)?;
        for segment in segments {
            let path = Path::new(&segment);
            fs::hard_link(path, wal.join(path.file_name().unwrap()))?;
        }
        sync_dir(&wal)?;
        sync_dir(dir.path())?;
        Ok(dir.into_path())
    }

    /// Index-file compaction changes only the optional hierarchy reference of
    /// native records. It uses the existing locator replacement WAL operation.
    pub(super) fn compact_native_files(
        &self,
        inputs: &[Arc<IndexFile>],
        generations: super::super::allocation::OutputGenerations,
        cancelled: &impl Fn() -> bool,
        target: u64,
        budget: super::super::io_budget::IoBudget,
    ) -> Result<super::super::compact::MergeStats> {
        let (_, inner) = self.native_binding()?;
        let ids = inputs.iter().map(|f| f.file_no).collect::<HashSet<_>>();
        let mut old = Locators::new();
        {
            let guard = inner.read();
            guard
                .vlog_index_cache
                .apply_entry_fn(|key, entry| -> Result<()> {
                    let selected = entry
                        .values
                        .read()
                        .iter()
                        .filter(|v| v.aggregate.as_ref().is_some_and(|a| ids.contains(&a.file)))
                        .cloned()
                        .collect::<Vec<_>>();
                    if !selected.is_empty() {
                        old.insert(
                            String::from_utf8(key)
                                .map_err(|_| invalid("invalid native series key"))?,
                            selected,
                        );
                    }
                    Ok(())
                })?;
        }
        if old.is_empty() {
            return Err(super::super::Error::CompactionBusy);
        }
        // A key-only merge must not remove native locators for tombstoned
        // VLogs. Their retirement still belongs to the original data compactor.
        let mut eligible = Locators::new();
        let mut fields: BTreeMap<SeqNo, Vec<Record>> = BTreeMap::new();
        for (key, indexes) in old {
            let (_, _, lease) = self.native_query_view_key(key.as_bytes())?;
            let mut records = Vec::new();
            let mut deleted = false;
            for index in &indexes {
                let aggregate = index.aggregate.as_ref().unwrap();
                if !lease
                    .as_ref()
                    .unwrap()
                    .sources
                    .contains_key(&aggregate.file)
                {
                    // The native data compactor moved this key after capture.
                    return Err(super::super::Error::CompactionBusy);
                }
                let file = inputs.iter().find(|f| f.file_no == aggregate.file).unwrap();
                if lease
                    .as_ref()
                    .unwrap()
                    .tombstones(file, aggregate.seq)
                    .coverage(index.min, index.max)?
                    .0
                {
                    deleted = true;
                    break;
                }
                let mut record = Record::from_vlog_index(aggregate.seq, index, aggregate.values)?;
                record.pos.version = aggregate.version;
                records.push(record);
            }
            if deleted {
                continue;
            }
            records.sort_by_key(|r| (r.pos.start, r.pos.end));
            if records.windows(2).any(|p| p[0].pos.end >= p[1].pos.start) {
                continue;
            }
            fields.entry(records[0].seq_no).or_default().extend(records);
            eligible.insert(key, indexes);
        }
        let old = eligible;
        if old.is_empty() {
            return Ok(Default::default());
        }
        let mut new = old.clone();
        for records in fields.values_mut() {
            records.sort_by_key(|r| (r.pos.start, r.pos.end));
            if records.windows(2).any(|p| p[0].pos.end >= p[1].pos.start) {
                return Err(super::super::Error::CompactionBusy);
            }
        }
        if cancelled() {
            return Err(super::super::Error::CompactionCancelled);
        }
        let outputs = if fields.is_empty() {
            Vec::new()
        } else {
            super::super::snapshot::write_compaction_files(
                &self.path,
                generations,
                fields,
                budget,
                target,
            )?
        };
        let _pin = self.prepare_native(&outputs, &mut new)?;
        if cancelled() {
            return Err(super::super::Error::CompactionCancelled);
        }
        let guard = inner.write();
        // Data compaction may have replaced a captured locator during IO.
        for (key, expected) in &old {
            let current = guard
                .vlog_index_cache
                .values_range(key.as_bytes(), i64::MIN, i64::MAX);
            if expected.iter().any(|e| {
                !current.iter().any(|c| {
                    c == e
                        && c.aggregate.as_ref().map(|a| a.file)
                            == e.aggregate.as_ref().map(|a| a.file)
                })
            }) {
                return Err(super::super::Error::CompactionBusy);
            }
        }
        guard.vlog_index_cache.delete_multi(&old);
        guard
            .vlog_index_cache
            .write_multi(new.clone())
            .map_err(|_| invalid("native locator cache write failed"))?;
        futures::executor::block_on(guard.vlog_index_wal.replace_multi(old, new))
            .map_err(|_| invalid("native locator replacement WAL failed"))?;
        drop(guard);
        Ok(super::super::compact::MergeStats {
            streamed_rebuild_fields: 1,
            ..Default::default()
        })
    }
}
