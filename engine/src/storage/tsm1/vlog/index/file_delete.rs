//! File-scoped tombstone publication and one-time migration of legacy journals.
use super::super::{
    delete_batch::{DeleteBatch, DeleteBatchBuilder},
    page_group::Column,
    tombstone::{self, Tombstone},
};
use super::*;

const PENDING: &str = "index.delete.pending";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingFile {
    target: String,
    temp: String,
    owner: u32,
    crc: u32,
}

fn checksum(path: &Path) -> Result<u32> {
    use std::io::Read;
    let mut file = File::open(path)?;
    let mut hash = crc32fast::Hasher::new();
    let mut buffer = [0; 8192];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(hash.finalize())
}

/// An interrupted publication is rolled forward before any readers or WAL cleanup.
pub(super) fn recover(path: &Path) -> Result<()> {
    recover_with_hook(path, &mut |_| Ok(()))
}

fn recover_with_hook(path: &Path, hook: &mut impl FnMut(&str) -> Result<()>) -> Result<()> {
    let intent = path.join(PENDING);
    if !intent.exists() {
        return Ok(());
    }
    if fs::metadata(&intent)?.len() > MAX_COMMIT_BYTES {
        return Err(invalid("delete intent exceeds size limit"));
    }
    let bytes = fs::read(&intent)?;
    if bytes.len() < 4 {
        return Err(invalid("truncated delete intent"));
    }
    let (body, crc) = bytes.split_at(bytes.len() - 4);
    if crc32fast::hash(body).to_be_bytes() != crc {
        return Err(invalid("delete intent checksum mismatch"));
    }
    let files: Vec<PendingFile> =
        serde_json::from_slice(body).map_err(|_| invalid("invalid delete intent"))?;
    let mut names = HashSet::new();
    for file in &files {
        if !basename(&file.target)
            || !basename(&file.temp)
            || super::super::allocation::index_file_no_from_name(
                file.target
                    .strip_suffix(".tombstone")
                    .ok_or(invalid("invalid tombstone name"))?,
            )? != file.owner
            || !file.temp.starts_with(&format!("{}.", file.target))
            || !file.temp.ends_with(".tmp")
            || !names.insert(&file.target)
        {
            return Err(invalid("invalid delete intent member"));
        }
        let candidate = if path.join(&file.temp).exists() {
            path.join(&file.temp)
        } else {
            path.join(&file.target)
        };
        Tombstone::open(&candidate, file.owner)?;
        if checksum(&candidate)? != file.crc {
            return Err(invalid("delete intent member checksum mismatch"));
        }
    }
    for file in files {
        let temp = path.join(file.temp);
        if temp.exists() {
            fs::rename(temp, path.join(file.target))?;
            hook("sidecar_renamed")?;
        }
    }
    sync_dir(path)?;
    hook("sidecars_durable")?;
    fs::remove_file(intent)?;
    sync_dir(path)
}

/// Clip a legacy cutoff to this immutable file's leaves. New sidecars contain
/// only time ranges; original write versions remain solely overwrite metadata.
fn append_range(
    builder: &mut DeleteBatchBuilder,
    index: &IndexFile,
    range: super::super::delete::DeleteRange,
    cutoff: u32,
) -> Result<bool> {
    let Some(entry) = index.lookup(range.seq)? else {
        return Ok(false);
    };
    let header = index.segment_header(&entry)?;
    if entry.min_start > range.max
        || entry.max_end < range.min
        || entry.top_level == super::super::LEVEL_DIRTY
    {
        return Ok(false);
    }
    if cutoff == u32::MAX {
        builder.push(super::super::delete::DeleteRange {
            min: range.min.max(entry.min_start),
            max: range.max.min(entry.max_end),
            ..range
        })?;
        return Ok(true);
    }
    let mut groups = index.groups(&entry, &header, LEVEL_BLOCK)?;
    let mut added = false;
    for i in 0..groups.len() {
        let group = groups.entry(i)?;
        if group.first_start > range.max || group.last_end < range.min {
            continue;
        }
        for record in groups.read_selected(i, &[Column::Count])? {
            if record.pos.write_version() <= cutoff
                && record.pos.start <= range.max
                && record.pos.end >= range.min
            {
                builder.push(super::super::delete::DeleteRange {
                    min: range.min.max(record.pos.start),
                    max: range.max.min(record.pos.end),
                    ..range
                })?;
                added = true;
            }
        }
    }
    Ok(added)
}

fn prepare(
    path: &Path,
    index: &IndexFile,
    batch: &DeleteBatch,
    cutoff: u32,
) -> Result<Option<PendingFile>> {
    let mut builder = DeleteBatchBuilder::new(path);
    let mut added = false;
    let (min, max) = index.seq_no_range();
    for range in batch.rows_between(min, max)? {
        added |= append_range(&mut builder, index, range?, cutoff)?;
    }
    if !added {
        return Ok(None);
    }
    if let Some(old) = &index.tombstone {
        for page in old.pages() {
            for row in page? {
                builder.push(row)?;
            }
        }
    }
    let target = tombstone::path(index.path())
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let temp = format!("{target}.{:032x}.tmp", rand::random::<u128>());
    let temp_path = path.join(&temp);
    let cleanup = scopeguard::guard((), |_| {
        let _ = fs::remove_file(&temp_path);
    });
    Tombstone::write(&temp_path, index.file_no, &builder.finish()?)?;
    Tombstone::open(&temp_path, index.file_no)?;
    let crc = checksum(&temp_path)?;
    scopeguard::ScopeGuard::into_inner(cleanup);
    Ok(Some(PendingFile {
        target,
        temp,
        owner: index.file_no,
        crc,
    }))
}

fn publish(path: &Path, files: &[PendingFile]) -> Result<()> {
    publish_with_hook(path, files, &mut |_| Ok(()))
}

fn publish_with_hook(
    path: &Path,
    files: &[PendingFile],
    hook: &mut impl FnMut(&str) -> Result<()>,
) -> Result<()> {
    let mut bytes =
        serde_json::to_vec(files).map_err(|_| invalid("cannot encode delete intent"))?;
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_be_bytes());
    if bytes.len() as u64 > MAX_COMMIT_BYTES {
        return Err(invalid("delete intent exceeds size limit"));
    }
    let temp = path.join(format!("{PENDING}.tmp"));
    remove_if_exists(&temp)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    // Persist prepared inodes and the intent before replacing the first sidecar.
    sync_dir(path)?;
    fs::rename(temp, path.join(PENDING))?;
    sync_dir(path)?;
    hook("intent_durable")?;
    recover_with_hook(path, hook)
}

impl IndexStore {
    pub(crate) fn install_delete_batch(&self, batch: &DeleteBatch) -> Result<()> {
        if batch.len() == 0 {
            return Ok(());
        }
        // Validate staging even when there are no disk files.
        for row in batch.rows() {
            row?;
        }
        loop {
            self.ensure_ready()?;
            let indexes = self.index_readers();
            let mut prepared = Vec::new();
            let mut cleanup = scopeguard::guard(Vec::new(), |paths: Vec<PathBuf>| {
                for path in paths {
                    let _ = fs::remove_file(path);
                }
            });
            for index in &indexes {
                if let Some(file) = prepare(&self.path, index, batch, u32::MAX)? {
                    cleanup.push(self.path.join(&file.temp));
                    prepared.push(file);
                }
            }
            let _gate = self.commit_gate.lock();
            self.ensure_ready()?;
            let current = self.index_readers();
            if current.len() != indexes.len()
                || current
                    .iter()
                    .zip(&indexes)
                    .any(|(a, b)| !Arc::ptr_eq(a, b))
            {
                continue;
            }
            if prepared.is_empty() {
                return Ok(());
            }
            self.recovery_required.store(true, Ordering::Release);
            self.installing.store(true, Ordering::Release);
            let _installing =
                scopeguard::guard((), |_| self.installing.store(false, Ordering::Release));
            // Once an intent can exist, recovery owns these temporary files.
            scopeguard::ScopeGuard::into_inner(cleanup);
            publish(&self.path, &prepared)?;
            let mut updated = Vec::with_capacity(indexes.len());
            for index in indexes {
                if prepared.iter().any(|p| p.owner == index.file_no) {
                    let fresh = IndexFile::open(index.path(), index.generation, index.seq)?;
                    fresh.set_cache(Arc::clone(&self.cache));
                    updated.push(fresh);
                } else {
                    updated.push(index);
                }
            }
            let old = {
                let mut files = self.files.write();
                std::mem::replace(&mut files.indexes, updated)
            };
            drop(old);
            self.recovery_required.store(false, Ordering::Release);
            return Ok(());
        }
    }
}

pub(super) fn migrate(path: &Path, live: &mut LiveFiles, legacy: &[Arc<DeleteFile>]) -> Result<()> {
    if legacy.is_empty() {
        return Ok(());
    }
    // Repeating migration after a crash is safe: sidecar unions are idempotent.
    for index in &mut live.indexes {
        let mut builder = DeleteBatchBuilder::new(path);
        let mut added = false;
        for file in legacy {
            for page in file.pages() {
                for range in page? {
                    added |= append_range(&mut builder, index, range, range.cutoff)?;
                }
            }
        }
        if !added {
            continue;
        }
        let batch = builder.finish()?;
        if let Some(prepared) = prepare(path, index, &batch, u32::MAX)? {
            publish(path, &[prepared])?;
            *index = IndexFile::open(index.path(), index.generation, index.seq)?;
        }
    }
    for file in legacy {
        remove_if_exists(&file.path)?;
    }
    sync_dir(path)
}

#[cfg(test)]
mod tests {
    use super::super::super::delete::DeleteRange;
    use super::super::tests::batch;
    use super::*;
    use crate::{models::INTEGER, query::storage_ir::TimeRange};

    fn ranges(dir: &Path, min: i64, max: i64) -> DeleteBatch {
        let mut builder = DeleteBatchBuilder::new(dir);
        builder
            .push(DeleteRange {
                seq: super::super::super::SeqNo::new(1, 1).unwrap(),
                min,
                max,
                cutoff: 0,
            })
            .unwrap();
        builder.finish().unwrap()
    }
    fn count(store: &IndexStore) -> u64 {
        store
            .aggregate(
                super::super::super::SeqNo::new(1, 1).unwrap(),
                INTEGER,
                TimeRange::all(),
                vec![],
                true,
            )
            .unwrap()
            .state
            .count
    }

    #[test]
    fn interrupted_multi_file_tombstones_roll_forward_before_open() {
        for stage in [
            "prepared_only",
            "intent_durable",
            "sidecar_renamed",
            "sidecars_durable",
        ] {
            let root = tempfile::tempdir().unwrap();
            let dir = root.path().join("data");
            let wal = root.path().join("wal");
            let store = IndexStore::open(&dir, &wal).unwrap();
            for generation in 1..=2 {
                let (index, vlog) = batch(&dir, generation, generation * 10);
                store.install(&[index], &[vlog], &[]).unwrap();
            }
            let batch = ranges(&dir, 0, 30);
            let files = store
                .index_readers()
                .iter()
                .map(|f| prepare(&dir, f, &batch, u32::MAX).unwrap().unwrap())
                .collect::<Vec<_>>();
            if stage != "prepared_only" {
                assert!(
                    publish_with_hook(&dir, &files, &mut |point| if point == stage {
                        Err(invalid("injected tombstone interruption"))
                    } else {
                        Ok(())
                    })
                    .is_err()
                );
            }
            drop(store);
            let store = IndexStore::open(&dir, &wal).unwrap();
            assert_eq!(
                count(&store),
                if stage == "prepared_only" { 2 } else { 0 },
                "{stage}"
            );
            assert!(!dir.join(PENDING).exists());
            assert!(
                fs::read_dir(&dir).unwrap().all(|e| !e
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|e| e == "tmp"))
            );
        }
    }

    #[test]
    fn extended_sequence_tombstone_intents_recover_the_logical_file_name() {
        for stage in ["intent_durable", "sidecar_renamed", "sidecars_durable"] {
            let root = tempfile::tempdir().unwrap();
            let dir = root.path().join("data");
            let wal = root.path().join("wal");
            let store = IndexStore::open(&dir, &wal).unwrap();
            let (mut index, vlog) = batch(&dir, 4, 10);
            // Both names identify physical ID (4 << 8) | 1. Only the logical
            // generation changes; the header and tombstone owner stay valid.
            let path = dir.join("000000003-000000257.idx.tmp");
            fs::rename(&index.path, &path).unwrap();
            index.path = path;
            index.generation = 3;
            index.seq = 257;
            store.install(&[index], &[vlog], &[]).unwrap();
            let batch = ranges(&dir, 0, 30);
            let files = store
                .index_readers()
                .iter()
                .map(|f| prepare(&dir, f, &batch, u32::MAX).unwrap().unwrap())
                .collect::<Vec<_>>();
            assert!(
                publish_with_hook(&dir, &files, &mut |point| {
                    if point == stage {
                        Err(invalid("injected extended tombstone interruption"))
                    } else {
                        Ok(())
                    }
                })
                .is_err()
            );
            drop(store);
            let store = IndexStore::open(&dir, &wal).unwrap();
            assert_eq!(count(&store), 0, "{stage}");
            assert!(!dir.join(PENDING).exists());
            assert!(dir.join("000000003-000000257.idx.tombstone").exists());
        }
    }

    #[test]
    fn repeated_deletes_merge_and_unselected_files_keep_their_tombstones() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("data");
        let wal = root.path().join("wal");
        let store = IndexStore::open(&dir, &wal).unwrap();
        for generation in 1..=3 {
            let (index, vlog) = batch(&dir, generation, if generation == 3 { 100 } else { 10 });
            store.install(&[index], &[vlog], &[]).unwrap();
        }
        let mut pinned = store
            .raw_cursor(
                super::super::super::SeqNo::new(1, 1).unwrap(),
                INTEGER,
                i64::MIN,
                i64::MAX,
                true,
            )
            .unwrap();
        for _ in 0..8 {
            store.install_delete_batch(&ranges(&dir, 9, 11)).unwrap();
        }
        assert_eq!(count(&store), 1);
        assert_eq!(pinned.read_batch().unwrap().len(), 2);
        let inputs = store.index_readers();
        assert_eq!(store.read_view().2.len(), 2);
        for file in &inputs[..2] {
            assert_eq!(
                fs::metadata(tombstone::path(file.path())).unwrap().len(),
                48
            );
        }
        assert!(inputs[2].tombstone.is_none());
        let old_sidecar = tombstone::path(inputs[0].path());
        store.compact_files(&inputs[..1], 4, &|| false).unwrap();
        store.reap_retired().unwrap();
        assert!(old_sidecar.exists());
        assert_eq!(store.read_view().2.len(), 1);
        assert_eq!(count(&store), 1);
        let (index, vlog) = batch(&dir, 5, 10);
        store.install(&[index], &[vlog], &[]).unwrap();
        assert_eq!(count(&store), 2);
        drop(inputs);
        drop(pinned);
        store.reap_retired().unwrap();
        assert!(!old_sidecar.exists());
        drop(store);
        assert_eq!(count(&IndexStore::open(&dir, &wal).unwrap()), 2);
    }

    #[test]
    fn legacy_journals_migrate_by_original_leaf_version() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("data");
        let wal = root.path().join("wal");
        let store = IndexStore::open(&dir, &wal).unwrap();
        for generation in 1..=2 {
            let (index, vlog) = batch(&dir, generation, generation * 10);
            store.install(&[index], &[vlog], &[]).unwrap();
        }
        store
            .compact_files(&store.index_readers(), 3, &|| false)
            .unwrap();
        DeleteFile::write(&dir.join("legacy.del"), &ranges(&dir, 0, 30), 511).unwrap();
        fs::write(dir.join(MODE_FILE), JOURNAL_MODE_BYTES).unwrap();
        drop(store);
        let store = IndexStore::open(&dir, &wal).unwrap();
        assert_eq!(count(&store), 1);
        assert!(!dir.join("legacy.del").exists());
        let rows = store.read_view().2[0]
            .lookup(super::super::super::SeqNo::new(1, 1).unwrap())
            .unwrap();
        assert_eq!((rows[0].min, rows[0].max), (10, 10));
        assert_eq!(fs::read(dir.join(MODE_FILE)).unwrap(), MODE_BYTES);
        store
            .compact_files(&store.index_readers(), 4, &|| false)
            .unwrap();
        assert!(store.read_view().2.is_empty());
        drop(store);
        assert_eq!(count(&IndexStore::open(&dir, &wal).unwrap()), 1);
    }

    #[test]
    fn garbage_collection_carries_file_tombstones_to_new_owner() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("data");
        let wal = root.path().join("wal");
        let store = IndexStore::open(&dir, &wal).unwrap();
        let (index, vlog) = batch(&dir, 1, 10);
        let mut file = OpenOptions::new().append(true).open(&vlog.0).unwrap();
        file.write_all(&[0; 4096]).unwrap();
        file.sync_all().unwrap();
        store.install(&[index], &[vlog], &[]).unwrap();
        store.install_delete_batch(&ranges(&dir, 10, 10)).unwrap();
        let stats = store
            .collect_garbage(
                super::super::super::allocation::OutputGenerations::isolated(2),
                u64::MAX,
                Default::default(),
                &|| false,
            )
            .unwrap();
        assert_eq!(stats.copied_blocks, 1);
        assert_eq!(count(&store), 0);
        let view = store.read_view();
        assert_eq!(view.2.len(), 1);
        assert_eq!(view.2[0].owner, view.0[0].file_no);
        store.checkpoint(true).unwrap();
        let backup = store.create_snapshot(root.path()).unwrap();
        assert_eq!(
            count(&IndexStore::open(&backup, &root.path().join("backup-wal")).unwrap()),
            0
        );
        drop(store);
        assert_eq!(count(&IndexStore::open(&dir, &wal).unwrap()), 0);
    }
}
