//! Live-file checkpoints cut recovery history and retire unreferenced VLogs.

use super::super::page_group::Column;
use super::*;

const NAME: &str = "index.checkpoint";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Checkpoint {
    version: u32,
    pub(super) baseline: Commit,
    pub(super) covered: HashSet<String>,
}
pub(super) fn read(path: &Path) -> Result<Option<Checkpoint>> {
    let path = path.join(NAME);
    if !path.exists() {
        return Ok(None);
    }
    if fs::metadata(&path)?.len() > MAX_COMMIT_BYTES {
        return Err(invalid("checkpoint exceeds size limit"));
    }
    let bytes = fs::read(path)?;
    if bytes.len() < 4 {
        return Err(invalid("truncated checkpoint"));
    }
    let (body, crc) = bytes.split_at(bytes.len() - 4);
    if crc32fast::hash(body).to_be_bytes() != crc {
        return Err(invalid("checkpoint checksum mismatch"));
    }
    let value: Checkpoint =
        serde_json::from_slice(body).map_err(|_| invalid("invalid checkpoint JSON"))?;
    if value.version != 1
        || !value.baseline.closed_wal.is_empty()
        || value.covered.iter().any(|name| {
            !basename(name)
                || !(name.ends_with(".installed")
                    || name.ends_with(".replace")
                    || name.ends_with(".del"))
        })
    {
        return Err(invalid("invalid checkpoint contents"));
    }
    value.baseline.validate()?;
    Ok(Some(value))
}

impl IndexStore {
    /// Scan references without publication locks. If a flush or compaction changes
    /// the file set meanwhile, the caller can retry with the next maintenance pass.
    pub(crate) fn checkpoint(&self, reclaim_unused: bool) -> Result<usize> {
        self.checkpoint_with_hook(reclaim_unused, &mut |_| Ok(()))
    }

    pub(super) fn checkpoint_with_hook(
        &self,
        reclaim_unused: bool,
        hook: &mut impl FnMut(&str) -> Result<()>,
    ) -> Result<usize> {
        if self.native_metadata { return self.reap_retired(); }
        let (indexes, vlogs, _) = self.read_view();
        if indexes.is_empty() {
            return Ok(0);
        }
        let mut used = HashSet::new();
        if reclaim_unused && !self.native_metadata {
            for file in &indexes {
                for page in 0..file.footer.directory_pages() {
                    for entry in file.directory_page(page)? {
                        let header = file.segment_header(&entry)?;
                        if header.level(LEVEL_BLOCK).is_none() {
                            continue;
                        }
                        let mut groups = file.groups(&entry, &header, LEVEL_BLOCK)?;
                        for group in 0..groups.len() {
                            let range = groups.entry(group)?;
                            for record in file.read_level(
                                &entry,
                                &header,
                                LEVEL_BLOCK,
                                range.first_start,
                                range.last_end,
                                &[Column::Count],
                            )? {
                                used.insert(record.pos.block.file_no);
                            }
                        }
                    }
                }
            }
            if used.iter().any(|id| {
                !vlogs
                    .iter()
                    .any(|v| ((*id >> 8) as i64, (*id & 255) as i64) == (v.generation, v.seq))
            }) {
                return Err(invalid("checkpoint references missing VLog"));
            }
        }
        hook("references_scanned")?;
        let _gate = self.commit_gate.lock();
        self.ensure_ready()?;
        {
            let live = self.files.read();
            if indexes.len() != live.indexes.len()
                || vlogs.len() != live.vlogs.len()
                || indexes
                    .iter()
                    .any(|f| !live.indexes.iter().any(|l| Arc::ptr_eq(f, l)))
            {
                return Ok(0);
            }
        }
        let (retained, retired): (Vec<_>, Vec<_>) = vlogs.into_iter().partition(|v| {
            !reclaim_unused || used.contains(&(((v.generation as u32) << 8) | v.seq as u32))
        });
        let output = |path: PathBuf, generation, seq, size| Output {
            name: path.file_name().unwrap().to_string_lossy().into_owned(),
            generation,
            seq,
            size,
        };
        let baseline = Commit {
            version: 2,
            indexes: indexes
                .iter()
                .map(|f| output(f.path().into(), f.generation, f.seq, f.size()))
                .collect(),
            vlogs: retained
                .iter()
                .map(|f| output(f.path(), f.generation, f.seq, f.size()))
                .collect(),
            closed_wal: Vec::new(),
        };
        baseline.validate()?;
        let mut covered = HashSet::new();
        for entry in fs::read_dir(&self.path)? {
            let name = entry?.file_name().to_string_lossy().into_owned();
            if name.ends_with(".installed") || name.ends_with(".replace") {
                covered.insert(name);
            }
        }
        let checkpoint = Checkpoint {
            version: 1,
            baseline,
            covered,
        };
        let mut bytes =
            serde_json::to_vec(&checkpoint).map_err(|_| invalid("cannot encode checkpoint"))?;
        bytes.extend_from_slice(&crc32fast::hash(&bytes).to_be_bytes());
        if bytes.len() as u64 > MAX_COMMIT_BYTES {
            return Err(invalid("checkpoint exceeds size limit"));
        }
        let temp = self.path.join(format!("{NAME}.tmp"));
        self.recovery_required.store(true, Ordering::Release);
        self.installing.store(true, Ordering::Release);
        let _installing =
            scopeguard::guard((), |_| self.installing.store(false, Ordering::Release));
        remove_if_exists(&temp)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        hook("checkpoint_prepared")?;
        fs::rename(temp, self.path.join(NAME))?;
        sync_dir(&self.path)?;
        hook("checkpoint_committed")?;
        let count = retired.len();
        {
            let mut live = self.files.write();
            live.vlogs = retained;
            live.retired_vlogs
                .extend(retired.iter().map(|f| RetiredVLog {
                    path: f.path(),
                    reader: Arc::downgrade(f),
                }));
        }
        hook("checkpoint_published")?;
        for name in checkpoint.covered {
            if name.ends_with(".del") {
                continue;
            }
            remove_if_exists(&self.path.join(name))?;
        }
        sync_dir(&self.path)?;
        hook("history_removed")?;
        self.recovery_required.store(false, Ordering::Release);
        Ok(count)
    }
}
