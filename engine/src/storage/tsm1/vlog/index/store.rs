//! Snapshot and replacement installation. Recovery precedes opening any WAL writer.
//!
//! A durable commit makes all output files live together. Its rename to installed
//! records that WAL cleanup is durable, so reused WAL names are never replayed as
//! cleanup instructions. Replacement records retire index files, never VLog data.

#[cfg(test)]
use super::SeqNo;
use super::delete::DeleteFile;
#[cfg(test)]
use super::delete::DeleteRange;
#[cfg(test)]
use super::page_group::{Column, ColumnValue};
use super::reader::IndexFile;
use super::writer::IndexFileMeta;
use super::{BlockLoc, LEVEL_BLOCK, Result, invalid};
use crate::common::slow_trace;
#[cfg(test)]
use crate::models::FieldType;
#[cfg(test)]
use crate::storage::tsm1::vlog::VLogIndex;
use crate::storage::tsm1::vlog::reader::VLogReader;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

const MODE_FILE: &str = "agg-index.mode";
const MODE_BYTES: &[u8] = b"ReefsDB aggregate index 4\n";
const JOURNAL_MODE_BYTES: &[u8] = b"ReefsDB aggregate index 3\n";
const REPLACEMENT_MODE_BYTES: &[u8] = b"ReefsDB aggregate index 2\n";
const APPEND_ONLY_MODE_BYTES: &[u8] = b"ReefsDB aggregate index 1\n";
const MAX_COMMIT_BYTES: u64 = 16 * 1024 * 1024;

#[path = "checkpoint.rs"]
mod checkpoint;
#[path = "file_delete.rs"]
mod file_delete;
#[path = "gc.rs"]
mod gc;
#[path = "replacement.rs"]
mod replacement;
#[path = "native.rs"]
pub(crate) mod native;

struct RetiredIndex {
    path: PathBuf,
    file_no: u32,
    reader: Weak<IndexFile>,
}
struct RetiredVLog {
    path: PathBuf,
    reader: Weak<VLogReader>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Output {
    name: String,
    generation: i64,
    seq: i64,
    size: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Commit {
    version: u32,
    indexes: Vec<Output>,
    vlogs: Vec<Output>,
    closed_wal: Vec<String>,
}

#[derive(Default)]
struct LiveFiles {
    indexes: Vec<Arc<IndexFile>>,
    vlogs: Vec<Arc<VLogReader>>,
    retired: Vec<RetiredIndex>,
    retired_vlogs: Vec<RetiredVLog>,
}

pub(crate) struct IndexStore {
    pub(crate) cache: Arc<super::caches::ReadCache>,
    path: PathBuf,
    wal_path: PathBuf,
    files: RwLock<LiveFiles>,
    // Serializes durable publication only. Queries and compaction IO never take it.
    commit_gate: Mutex<()>,
    installing: AtomicBool,
    recovery_required: AtomicBool,
    compacting: AtomicBool,
    native: RwLock<Option<native::NativeState>>,
    native_metadata: bool,
    native_generation: std::sync::atomic::AtomicI64,
}

fn sync_dir(path: &Path) -> Result<()> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    {
        use std::os::windows::fs::OpenOptionsExt;
        OpenOptions::new()
            .read(true)
            .custom_flags(0x02000000)
            .open(path)?
            .sync_all()?;
    }
    Ok(())
}

fn basename(name: &str) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.contains(['/', '\\', ':'])
}

fn output_name(generation: i64, seq: i64, ext: &str) -> String {
    format!("{generation:09}-{seq:09}.{ext}")
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

fn write_mode(path: &Path) -> Result<()> {
    let temp = path.join(format!("{MODE_FILE}.tmp"));
    remove_if_exists(&temp)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)?;
    file.write_all(MODE_BYTES)?;
    file.sync_all()?;
    fs::rename(temp, path.join(MODE_FILE))?;
    sync_dir(path)
}

impl Commit {
    fn validate(&self) -> Result<()> {
        if !(1..=2).contains(&self.version)
            || self.indexes.is_empty()
            || (self.version == 1 && self.vlogs.is_empty())
            || (self.version == 2 && !self.closed_wal.is_empty())
        {
            return Err(invalid("invalid snapshot commit"));
        }
        let mut names = HashSet::new();
        let mut index_ids = HashSet::new();
        for (outputs, ext) in [(&self.indexes, "idx"), (&self.vlogs, "vlog")] {
            for f in outputs {
                if ext == "idx" {
                    if !index_ids.insert(super::allocation::index_file_no(f.generation, f.seq)?) {
                        return Err(invalid("snapshot repeats a physical index identity"));
                    }
                } else {
                    BlockLoc::from_vlog(f.generation, f.seq, 5)?;
                }
                if f.name != output_name(f.generation, f.seq, ext)
                    || f.size < 5
                    || !names.insert(&f.name)
                {
                    return Err(invalid("invalid or repeated snapshot output"));
                }
            }
        }
        let mut wal_names = HashSet::new();
        for name in &self.closed_wal {
            if !basename(name) || !name.ends_with(".wal") || !wal_names.insert(name) {
                return Err(invalid("invalid or repeated WAL name"));
            }
        }
        Ok(())
    }

    fn name(&self, ext: &str) -> String {
        format!(
            "snapshot-{:08x}.{ext}",
            super::allocation::index_file_no(self.indexes[0].generation, self.indexes[0].seq)
                .expect("validated snapshot identity")
        )
    }

    fn read(path: &Path) -> Result<Self> {
        if fs::metadata(path)?.len() > MAX_COMMIT_BYTES {
            return Err(invalid("snapshot commit exceeds size limit"));
        }
        let bytes = fs::read(path)?;
        if bytes.len() < 4 {
            return Err(invalid("truncated snapshot commit"));
        }
        let (body, crc) = bytes.split_at(bytes.len() - 4);
        if crc32fast::hash(body).to_be_bytes() != crc {
            return Err(invalid("snapshot commit checksum mismatch"));
        }
        let commit: Self =
            serde_json::from_slice(body).map_err(|_| invalid("invalid snapshot commit JSON"))?;
        commit.validate()?;
        Ok(commit)
    }

    fn write(&self, path: &Path) -> Result<()> {
        let mut bytes =
            serde_json::to_vec(self).map_err(|_| invalid("cannot encode snapshot commit"))?;
        bytes.extend_from_slice(&crc32fast::hash(&bytes).to_be_bytes());
        if bytes.len() as u64 > MAX_COMMIT_BYTES {
            return Err(invalid("snapshot commit exceeds size limit"));
        }
        let mut f = OpenOptions::new().write(true).create_new(true).open(path)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
        Ok(())
    }

    fn open_files(&self, path: &Path) -> Result<LiveFiles> {
        self.validate()?;
        for output in self.indexes.iter().chain(&self.vlogs) {
            let meta = fs::symlink_metadata(path.join(&output.name))?;
            if !meta.is_file() || meta.len() != output.size {
                return Err(invalid("snapshot output is missing or has changed size"));
            }
        }
        let indexes = self
            .indexes
            .iter()
            .map(|f| IndexFile::open(&path.join(&f.name), f.generation, f.seq))
            .collect::<Result<Vec<_>>>()?;
        let vlogs = self
            .vlogs
            .iter()
            .map(|f| {
                VLogReader::new_bare(&path.join(&f.name), f.generation, f.seq)
                    .map(Arc::new)
                    .map_err(|_| invalid("invalid committed VLog file"))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(LiveFiles {
            indexes,
            vlogs,
            retired: Vec::new(),
            retired_vlogs: Vec::new(),
        })
    }

    fn finish_wal_cleanup(&self, path: &Path, wal_path: &Path) -> Result<()> {
        for name in &self.closed_wal {
            remove_if_exists(&wal_path.join(name))?;
        }
        sync_dir(wal_path)?;
        fs::rename(
            path.join(self.name("commit")),
            path.join(self.name("installed")),
        )?;
        sync_dir(path)
    }
}

impl IndexStore {
    /// Reject mixed modes before either mode mutates files or opens WAL writers.
    pub(crate) fn check_mode(path: &Path, enabled: bool) -> Result<()> {
        let mut aggregate = false;
        let mut legacy = false;
        if path.exists() {
            for entry in fs::read_dir(path)? {
                let name = entry?.file_name();
                let name = name.to_string_lossy();
                aggregate |= name == MODE_FILE
                    || name.ends_with(".idx")
                    || name.ends_with(".commit")
                    || name.ends_with(".installed")
                    || name.ends_with(".replace")
                    || name.ends_with(".checkpoint")
                    || name.ends_with(".del");
                legacy |= name.ends_with(".vlog") || name.ends_with(".cmp");
            }
        }
        if (!enabled && aggregate) || (enabled && legacy && !path.join(MODE_FILE).exists()) {
            return Err(invalid("aggregate and legacy VLog modes cannot be mixed"));
        }
        if aggregate && !path.join(MODE_FILE).exists() {
            return Err(invalid("aggregate index mode marker is missing"));
        }
        Ok(())
    }

    /// Called once, with exclusive shard ownership and before constructing a WAL.
    pub(crate) fn open(path: &Path, wal_path: &Path) -> Result<Arc<Self>> {
        Self::open_inner(path, wal_path, false)
    }

    pub(crate) fn open_native(path: &Path, wal_path: &Path) -> Result<Arc<Self>> {
        Self::open_locator_store(path, wal_path)
    }

    fn open_inner(path: &Path, wal_path: &Path, native_metadata: bool) -> Result<Arc<Self>> {
        Self::check_mode(path, true)?;
        fs::create_dir_all(path)?;
        fs::create_dir_all(wal_path)?;
        if let Some(parent) = path.parent() {
            sync_dir(parent)?;
        }
        if let Some(parent) = wal_path.parent() {
            sync_dir(parent)?;
        }
        let mode = path.join(MODE_FILE);
        let upgrade = if mode.exists() {
            let bytes = fs::read(&mode)?;
            if bytes != MODE_BYTES
                && bytes != JOURNAL_MODE_BYTES
                && bytes != APPEND_ONLY_MODE_BYTES
                && bytes != REPLACEMENT_MODE_BYTES
            {
                return Err(invalid("unsupported aggregate index mode"));
            }
            bytes != MODE_BYTES
        } else {
            write_mode(path)?;
            false
        };
        let checkpoint = checkpoint::read(path)?;
        let mut commits = Vec::new();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if checkpoint
                .as_ref()
                .is_some_and(|c| c.covered.contains(&name))
            {
                continue;
            }
            if name.ends_with(".commit") || name.ends_with(".installed") {
                let commit = Commit::read(&entry.path())?;
                let pending = name.ends_with(".commit");
                if name != commit.name(if pending { "commit" } else { "installed" }) {
                    return Err(invalid("snapshot commit identity mismatch"));
                }
                commits.push((commit, pending));
            }
        }
        commits.sort_by_key(|(c, _)| (c.indexes[0].generation, c.indexes[0].seq));
        if native_metadata && (checkpoint.as_ref().is_some_and(|c| !c.baseline.vlogs.is_empty())
            || commits.iter().any(|(c, _)| !c.vlogs.is_empty())) {
            return Err(invalid("legacy aggregate-owned VLogs require an explicit migration"));
        }
        file_delete::recover(path)?;
        // Replay metadata first: retired indexes may already have been removed.
        // Validate the entire live set before any WAL or orphan cleanup.
        let (mut live, members) = replacement::recover(path, &commits, checkpoint.as_ref())?;
        if native_metadata && !live.vlogs.is_empty() {
            return Err(invalid("legacy aggregate-owned VLogs require an explicit migration"));
        }
        let mut legacy_deletes = Vec::new();
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            if entry.file_name().to_string_lossy().ends_with(".del") {
                if checkpoint.as_ref().is_some_and(|c| {
                    c.covered
                        .contains(entry.file_name().to_string_lossy().as_ref())
                }) {
                    continue;
                }
                legacy_deletes.push(DeleteFile::open(&entry.path())?);
            }
        }
        // Older append-only binaries must refuse a directory that may retire files.
        // Upgrade only after validating all live files, before WAL/orphan cleanup.
        if upgrade {
            write_mode(path)?;
        }
        file_delete::migrate(path, &mut live, &legacy_deletes)?;
        for (commit, pending) in &commits {
            if *pending {
                commit.finish_wal_cleanup(path, wal_path)?;
            }
        }
        if let Some(checkpoint) = checkpoint {
            for name in checkpoint.covered {
                remove_if_exists(&path.join(name))?;
            }
        }
        // Uncommitted outputs have no visibility; their data remains in the WAL.
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if entry.file_type()?.is_dir()
                && name.starts_with("agg-repair-")
                && name.ends_with(".tmp")
            {
                fs::remove_dir_all(entry.path())?;
                continue;
            }
            if entry.file_type()?.is_file()
                && !(native_metadata && (name.ends_with(".vlog") || name.ends_with(".vlog.tmp")
                    || name.ends_with(".vlog.tombstone")))
                && (name.ends_with(".tmp")
                    || name.ends_with(".idx.tmp.tombstone")
                    || (name.ends_with(".idx.tombstone")
                        && !members.contains(name.strip_suffix(".tombstone").unwrap()))
                    || ((name.ends_with(".idx") || name.ends_with(".vlog"))
                        && !members.contains(&name)))
            {
                fs::remove_file(entry.path())?;
            }
        }
        sync_dir(path)?;
        let cache = super::caches::ReadCache::new(super::caches::DEFAULT_CACHE_BYTES);
        for file in &live.indexes {
            file.set_cache(Arc::clone(&cache));
        }
        Ok(Arc::new(Self {
            cache,
            path: path.into(),
            wal_path: wal_path.into(),
            files: RwLock::new(live),
            commit_gate: Mutex::new(()),
            installing: AtomicBool::new(false),
            recovery_required: AtomicBool::new(false),
            compacting: AtomicBool::new(false),
            native: RwLock::new(None),
            native_metadata,
            native_generation: std::sync::atomic::AtomicI64::new(0),
        }))
    }

    /// Installs files, publishes one immutable batch, then checkpoints its WAL.
    /// Callers may clear the cache snapshot only after this succeeds. Any failure
    /// requires reopening the shard, since a durable commit may already exist.
    pub(crate) fn install(
        &self,
        indexes: &[IndexFileMeta],
        vlogs: &[(PathBuf, i64, i64)],
        closed_wal: &[String],
    ) -> Result<()> {
        if self.native_metadata {
            return Err(invalid("native VLog publication must use its original cache/WAL commit"));
        }
        let wait = slow_trace::now();
        let _commit = self.commit_gate.lock();
        slow_trace::log_lock_wait("snapshot_wait_for_commit", wait, "aggregate index snapshot");
        if self.recovery_required.load(Ordering::Acquire)
            || self
                .installing
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return Err(invalid(
                "snapshot installation requires recovery or is already running",
            ));
        }
        let guard = scopeguard::guard((), |_| self.installing.store(false, Ordering::Release));
        // A panic or ambiguous fsync failure must also prevent another commit.
        self.recovery_required.store(true, Ordering::Release);
        let result = self.install_inner(indexes, vlogs, closed_wal, &mut |_| Ok(()));
        if result.is_ok() {
            self.recovery_required.store(false, Ordering::Release);
        }
        drop(guard);
        result
    }

    fn install_inner(
        &self,
        indexes: &[IndexFileMeta],
        vlogs: &[(PathBuf, i64, i64)],
        closed_wal: &[String],
        checkpoint: &mut impl FnMut(&str) -> Result<()>,
    ) -> Result<()> {
        self.validate_new_index_ids(indexes)?;
        let make_output = |path: &Path, generation, seq, ext| -> Result<Output> {
            let name = output_name(generation, seq, ext);
            if path != self.path.join(format!("{name}.tmp")) || self.path.join(&name).exists() {
                return Err(invalid(
                    "snapshot output must be a new temporary file in the VLog directory",
                ));
            }
            let meta = fs::symlink_metadata(path)?;
            if !meta.is_file() {
                return Err(invalid("snapshot output is not a regular file"));
            }
            Ok(Output {
                name,
                generation,
                seq,
                size: meta.len(),
            })
        };
        let commit = Commit {
            version: 1,
            indexes: indexes
                .iter()
                .map(|f| make_output(&f.path, f.generation, f.seq, "idx"))
                .collect::<Result<_>>()?,
            vlogs: vlogs
                .iter()
                .map(|(p, g, s)| make_output(p, *g, *s, "vlog"))
                .collect::<Result<_>>()?,
            closed_wal: closed_wal
                .iter()
                .map(|name| {
                    let path = Path::new(name);
                    if path.parent() != Some(self.wal_path.as_path()) {
                        return Err(invalid("closed WAL is outside the shard WAL directory"));
                    }
                    path.file_name()
                        .and_then(|s| s.to_str())
                        .map(str::to_owned)
                        .ok_or_else(|| invalid("invalid closed WAL name"))
                })
                .collect::<Result<_>>()?,
        };
        commit.validate()?;
        if self.path.join(commit.name("commit")).exists()
            || self.path.join(commit.name("installed")).exists()
        {
            return Err(invalid("snapshot commit already exists"));
        }
        for f in commit.vlogs.iter().chain(&commit.indexes) {
            let temp = self.path.join(format!("{}.tmp", f.name));
            File::open(&temp)?.sync_all()?;
            fs::rename(temp, self.path.join(&f.name))?;
            checkpoint("output_renamed")?;
        }
        sync_dir(&self.path)?;
        let batch = commit.open_files(&self.path)?;
        for file in &batch.indexes {
            file.set_cache(Arc::clone(&self.cache));
        }
        checkpoint("outputs_durable")?;
        let temp_commit = self.path.join(commit.name("commit.tmp"));
        commit.write(&temp_commit)?;
        fs::rename(temp_commit, self.path.join(commit.name("commit")))?;
        sync_dir(&self.path)?;
        checkpoint("commit_durable")?;
        {
            let mut files = self.files.write();
            files.indexes.extend(batch.indexes);
            files.vlogs.extend(batch.vlogs);
        }
        checkpoint("published")?;
        commit.finish_wal_cleanup(&self.path, &self.wal_path)
    }

    fn validate_new_index_ids(&self, indexes: &[IndexFileMeta]) -> Result<()> {
        let mut ids = HashSet::new();
        for file in indexes {
            if !ids.insert(super::allocation::index_file_no(file.generation, file.seq)?) {
                return Err(invalid("output repeats a physical index identity"));
            }
        }
        let files = self.files.read();
        if files.indexes.iter().any(|file| ids.contains(&file.file_no))
            || files.retired.iter().any(|file| ids.contains(&file.file_no))
        {
            return Err(invalid("output reuses a physical index identity"));
        }
        Ok(())
    }

    pub(crate) fn next_generation(&self) -> i64 {
        let files = self.files.read();
        files
            .indexes
            .iter()
            .map(|f| i64::from(f.file_no >> 8))
            .chain(files.vlogs.iter().map(|f| f.generation))
            .max()
            .unwrap_or(0)
            .max(self.native_generation.load(Ordering::Acquire))
            + 1
    }

    pub(crate) fn readers(&self) -> Vec<Arc<VLogReader>> {
        if let Some(readers) = self.native_readers() { return readers; }
        self.files.read().vlogs.clone()
    }

    pub(crate) fn read_view(
        &self,
    ) -> (
        Vec<Arc<IndexFile>>,
        Vec<Arc<VLogReader>>,
        Vec<Arc<super::tombstone::Tombstone>>,
    ) {
        let wait = slow_trace::now();
        let files = self.files.read();
        slow_trace::log_lock_wait(
            "query_wait_for_file_store_lock",
            wait,
            "aggregate read view",
        );
        (
            files.indexes.clone(),
            files.vlogs.clone(),
            files
                .indexes
                .iter()
                .filter_map(|f| f.tombstone.clone())
                .collect::<Vec<_>>(),
        )
    }

    pub(crate) fn delete_batch_builder(&self) -> super::delete_batch::DeleteBatchBuilder {
        super::delete_batch::DeleteBatchBuilder::new(&self.path)
    }

    #[cfg(test)]
    pub(crate) fn delete_ranges(&self, ranges: Vec<DeleteRange>) -> Result<()> {
        let mut builder = self.delete_batch_builder();
        for range in ranges {
            builder.push(range)?;
        }
        self.install_delete_batch(&builder.finish()?)
    }

    pub(crate) fn ensure_ready(&self) -> Result<()> {
        if self.recovery_required.load(Ordering::Acquire)
            && !self.installing.load(Ordering::Acquire)
        {
            return Err(invalid(
                "aggregate index installation failed; reopen the shard before writing",
            ));
        }
        Ok(())
    }

    /// Pins a coherent data-file set. The caller still owns series/field metadata
    /// backup, as for legacy FileStore snapshots.
    pub(crate) fn create_snapshot(&self, parent: &Path) -> Result<PathBuf> {
        if self.native_metadata {
            return self.create_native_snapshot(parent);
        }
        let _gate = self.commit_gate.lock();
        self.ensure_ready()?;
        let (indexes, vlogs, deletes) = {
            let files = self.files.read();
            (
                files.indexes.clone(),
                files.vlogs.clone(),
                files
                    .indexes
                    .iter()
                    .filter_map(|f| f.tombstone.clone())
                    .collect::<Vec<_>>(),
            )
        };
        let dir = tempfile::Builder::new()
            .prefix("agg-snapshot-")
            .suffix(".tmp")
            .tempdir_in(parent)?;
        fs::write(dir.path().join(MODE_FILE), MODE_BYTES)?;
        for file in deletes {
            fs::hard_link(&file.path, dir.path().join(file.path.file_name().unwrap()))?;
        }
        if !indexes.is_empty() {
            // A self-contained baseline needs no historical replacement inputs.
            let output = |path: PathBuf, generation, seq| -> Result<Output> {
                Ok(Output {
                    name: path.file_name().unwrap().to_string_lossy().into_owned(),
                    generation,
                    seq,
                    size: fs::metadata(path)?.len(),
                })
            };
            let commit = Commit {
                version: 2,
                indexes: indexes
                    .iter()
                    .map(|f| output(f.path().to_path_buf(), f.generation, f.seq))
                    .collect::<Result<_>>()?,
                vlogs: vlogs
                    .iter()
                    .map(|f| output(f.path(), f.generation, f.seq))
                    .collect::<Result<_>>()?,
                closed_wal: Vec::new(),
            };
            commit.validate()?;
            for f in commit.indexes.iter().chain(&commit.vlogs) {
                fs::hard_link(self.path.join(&f.name), dir.path().join(&f.name))?;
            }
            commit.write(&dir.path().join(commit.name("installed")))?;
        }
        sync_dir(dir.path())?;
        Ok(dir.into_path())
    }

    /// Materialized location inspection for tests. Production scans use RawCursor.
    #[cfg(test)]
    pub(crate) fn block_locations(
        &self,
        seq_no: SeqNo,
        typ: FieldType,
        min: i64,
        max: i64,
    ) -> Result<Vec<VLogIndex>> {
        Ok(self.query_blocks(seq_no, typ, min, max)?.0)
    }

    #[cfg(test)]
    pub(crate) fn query_blocks(
        &self,
        seq_no: SeqNo,
        typ: FieldType,
        min: i64,
        max: i64,
    ) -> Result<(
        Vec<VLogIndex>,
        Vec<Arc<VLogReader>>,
        std::collections::HashMap<(i64, i64, i64), u32>,
        Vec<DeleteRange>,
    )> {
        if min > max {
            return Ok((Vec::new(), Vec::new(), Default::default(), Vec::new()));
        }
        let start = slow_trace::now();
        let (files, vlogs, native) = self.native_query_view(seq_no)?;
        let delete_files = files.iter().filter_map(|f| f.tombstone.clone()).collect::<Vec<_>>();
        let result = (|| {
            let mut locations = Vec::new();
            let mut versions = std::collections::HashMap::new();
            let mut deletes = Vec::new();
            for file in delete_files {
                deletes.extend(file.lookup(seq_no)?);
            }
            for file in files {
                let Some(entry) = file.lookup(seq_no)? else {
                    continue;
                };
                if entry.typ != typ || entry.min_start > max || entry.max_end < min {
                    continue;
                }
                let header = file.segment_header(&entry)?;
                for record in
                    file.read_level(&entry, &header, LEVEL_BLOCK, min, max, &[Column::Count])?
                {
                    if native.as_ref().map(|view| view.tombstones(&file, seq_no).coverage(record.pos.start, record.pos.end))
                        .transpose()?.is_some_and(|(_, full)| full) { continue; }
                    let Some(ColumnValue::Count(count)) = record.values[Column::Count as usize]
                    else {
                        return Err(invalid("block count missing"));
                    };
                    let location = record.pos.block;
                    if record.pos.version != 0 {
                        versions.insert(
                            (
                                location.generation(),
                                location.seq(),
                                i64::from(location.offset),
                            ),
                            record.pos.version,
                        );
                    }
                    let reader = vlogs
                        .iter()
                        .find(|f| f.generation == location.generation() && f.seq == location.seq())
                        .ok_or_else(|| invalid("index references a missing VLog file"))?;
                    if count == 0
                        || location.offset < 5
                        || record.pos.block_size < 5
                        || u64::from(location.offset) + u64::from(record.pos.block_size)
                            > reader.size()
                    {
                        return Err(invalid("index references an invalid VLog block"));
                    }
                    locations.push(VLogIndex {
                        aggregate: None,
                        count: i32::try_from(count).map_err(|_| super::Error::Overflow)?,
                        typ,
                        min: record.pos.start,
                        max: record.pos.end,
                        generation: record.pos.block.generation(),
                        seq: record.pos.block.seq(),
                        offset: i64::from(record.pos.block.offset),
                        size: i64::from(record.pos.block_size),
                    });
                }
            }
            Ok((locations, vlogs, versions, deletes))
        })();
        slow_trace::log_query_stage(
            "query_index_lookup",
            start,
            "aggregate index block locations",
        );
        result
    }
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;
