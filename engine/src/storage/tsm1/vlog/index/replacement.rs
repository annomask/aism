//! Durable index replacement and recovery of the final live file set.

use super::super::allocation::{OutputGenerations, index_file_no};
use super::*;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Replacement {
    version: u32,
    inputs: Vec<String>,
    outputs: Vec<Output>,
    #[serde(default)]
    vlogs: Vec<Output>,
}
impl Replacement {
    fn name(&self) -> String {
        format!(
            "replace-{:08x}.replace",
            index_file_no(self.outputs[0].generation, self.outputs[0].seq)
                .expect("validated replacement identity")
        )
    }
    fn validate(&self) -> Result<()> {
        if !(1..=2).contains(&self.version)
            || self.inputs.is_empty()
            || self.outputs.is_empty()
            || (self.version == 1 && !self.vlogs.is_empty())
        {
            return Err(invalid("invalid index replacement"));
        }
        let mut names = HashSet::new();
        for name in &self.inputs {
            if !basename(name) || !name.ends_with(".idx") || !names.insert(name.as_str()) {
                return Err(invalid("invalid replacement input"));
            }
        }
        for output in &self.outputs {
            index_file_no(output.generation, output.seq)?;
            if output.name != output_name(output.generation, output.seq, "idx")
                || output.size < 5
                || !names.insert(&output.name)
                || output.generation != self.outputs[0].generation
            {
                return Err(invalid("invalid replacement output"));
            }
        }
        for output in &self.vlogs {
            BlockLoc::from_vlog(output.generation, output.seq, 5)?;
            if output.name != output_name(output.generation, output.seq, "vlog")
                || output.size < 5
                || output.generation < self.outputs[0].generation
                || !names.insert(&output.name)
            {
                return Err(invalid("invalid replacement VLog output"));
            }
        }
        Ok(())
    }
    fn write(&self, path: &Path) -> Result<()> {
        self.validate()?;
        let mut bytes =
            serde_json::to_vec(self).map_err(|_| invalid("cannot encode replacement"))?;
        bytes.extend_from_slice(&crc32fast::hash(&bytes).to_be_bytes());
        if bytes.len() as u64 > MAX_COMMIT_BYTES {
            return Err(invalid("replacement exceeds size limit"));
        }
        let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        Ok(())
    }
    fn read(path: &Path) -> Result<Self> {
        if fs::metadata(path)?.len() > MAX_COMMIT_BYTES {
            return Err(invalid("replacement exceeds size limit"));
        }
        let bytes = fs::read(path)?;
        if bytes.len() < 4 {
            return Err(invalid("truncated replacement"));
        }
        let (body, crc) = bytes.split_at(bytes.len() - 4);
        if crc32fast::hash(body).to_be_bytes() != crc {
            return Err(invalid("replacement checksum mismatch"));
        }
        let record: Self =
            serde_json::from_slice(body).map_err(|_| invalid("invalid replacement JSON"))?;
        record.validate()?;
        if path.file_name().and_then(|s| s.to_str()) != Some(record.name().as_str()) {
            return Err(invalid("replacement identity mismatch"));
        }
        Ok(record)
    }
}

pub(super) fn recover(
    path: &Path,
    commits: &[(Commit, bool)],
    checkpoint: Option<&super::checkpoint::Checkpoint>,
) -> Result<(LiveFiles, HashSet<String>)> {
    enum Event<'a> {
        Snapshot(&'a Commit),
        Replace(Replacement),
    }
    let mut events = commits
        .iter()
        .map(|(c, _)| {
            (
                (c.indexes[0].generation, c.indexes[0].seq),
                Event::Snapshot(c),
            )
        })
        .collect::<Vec<_>>();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if checkpoint.is_some_and(|c| {
            c.covered
                .contains(entry.file_name().to_string_lossy().as_ref())
        }) {
            continue;
        }
        if entry.file_name().to_string_lossy().ends_with(".replace") {
            let replacement = Replacement::read(&entry.path())?;
            events.push((
                (
                    replacement.outputs[0].generation,
                    replacement.outputs[0].seq,
                ),
                Event::Replace(replacement),
            ));
        }
    }
    events.sort_by_key(|(id, _)| *id);
    let mut indexes = BTreeMap::new();
    let mut vlogs = BTreeMap::new();
    let mut ever_created = HashSet::new();
    if let Some(checkpoint) = checkpoint {
        indexes.extend(
            checkpoint
                .baseline
                .indexes
                .iter()
                .map(|o| (o.name.clone(), o.clone())),
        );
        vlogs.extend(
            checkpoint
                .baseline
                .vlogs
                .iter()
                .map(|o| (o.name.clone(), o.clone())),
        );
        ever_created.extend(indexes.keys().chain(vlogs.keys()).cloned());
    }
    for (_, event) in events {
        match event {
            Event::Snapshot(commit) => {
                for output in commit.indexes.iter().chain(&commit.vlogs) {
                    if !ever_created.insert(output.name.clone()) {
                        return Err(invalid("output belongs to multiple commits"));
                    }
                }
                indexes.extend(commit.indexes.iter().map(|o| (o.name.clone(), o.clone())));
                vlogs.extend(commit.vlogs.iter().map(|o| (o.name.clone(), o.clone())));
            }
            Event::Replace(replacement) => {
                for name in &replacement.inputs {
                    let old: Output = indexes
                        .remove(name)
                        .ok_or(invalid("replacement input is not live"))?;
                    if old.generation >= replacement.outputs[0].generation {
                        return Err(invalid("replacement generation does not follow inputs"));
                    }
                }
                for output in &replacement.vlogs {
                    if !ever_created.insert(output.name.clone()) {
                        return Err(invalid("replacement reuses VLog identity"));
                    }
                    vlogs.insert(output.name.clone(), output.clone());
                }
                for output in replacement.outputs {
                    if !ever_created.insert(output.name.clone()) {
                        return Err(invalid("replacement reuses output identity"));
                    }
                    indexes.insert(output.name.clone(), output);
                }
            }
        }
    }
    let members = indexes.keys().chain(vlogs.keys()).cloned().collect();
    let live = if indexes.is_empty() {
        if !vlogs.is_empty() {
            return Err(invalid("VLog files have no live index"));
        }
        LiveFiles::default()
    } else {
        // A baseline combines live outputs, not historical file dependencies.
        Commit {
            version: 2,
            indexes: indexes.into_values().collect(),
            vlogs: vlogs.into_values().collect(),
            closed_wal: Vec::new(),
        }
        .open_files(path)?
    };
    Ok((live, members))
}

impl IndexStore {
    pub(crate) fn index_readers(&self) -> Vec<Arc<IndexFile>> {
        if let Some(readers) = self.native_index_readers() { return readers; }
        self.files.read().indexes.clone()
    }

    pub(crate) fn replace(
        &self,
        inputs: &[Arc<IndexFile>],
        outputs: &[IndexFileMeta],
    ) -> Result<()> {
        self.replace_cancellable(inputs, outputs, &|| false)
    }

    pub(super) fn replace_cancellable(
        &self,
        inputs: &[Arc<IndexFile>],
        outputs: &[IndexFileMeta],
        cancelled: &impl Fn() -> bool,
    ) -> Result<()> {
        self.replace_data_cancellable(inputs, outputs, &[], cancelled)
    }

    pub(super) fn replace_data_cancellable(
        &self,
        inputs: &[Arc<IndexFile>],
        outputs: &[IndexFileMeta],
        vlogs: &[(PathBuf, i64, i64)],
        cancelled: &impl Fn() -> bool,
    ) -> Result<()> {
        let wait = slow_trace::now();
        let _commit = self.commit_gate.lock();
        slow_trace::log_lock_wait(
            "compaction_wait_for_commit",
            wait,
            "aggregate index replacement",
        );
        if cancelled() {
            return Err(super::super::Error::CompactionCancelled);
        }
        self.ensure_ready()?;
        self.validate_inputs(inputs)?;
        self.installing.store(true, Ordering::Release);
        let _installing =
            scopeguard::guard((), |_| self.installing.store(false, Ordering::Release));
        self.recovery_required.store(true, Ordering::Release);
        let start = slow_trace::now();
        let result = self.replace_data_inner(inputs, outputs, vlogs, &mut |_| Ok(()));
        slow_trace::log_background_stage(
            "compaction_install",
            start,
            "aggregate index replacement",
        );
        if result.is_ok() {
            self.recovery_required.store(false, Ordering::Release);
        }
        result
    }

    fn validate_inputs(&self, inputs: &[Arc<IndexFile>]) -> Result<()> {
        if inputs.is_empty() {
            return Err(invalid("replacement needs at least one input"));
        }
        let files = self.files.read();
        let mut ids = HashSet::new();
        for input in inputs {
            if !ids.insert(input.file_no) || !files.indexes.iter().any(|f| Arc::ptr_eq(f, input)) {
                return Err(invalid("replacement plan is stale or repeats an input"));
            }
        }
        Ok(())
    }

    pub(super) fn replace_inner(
        &self,
        inputs: &[Arc<IndexFile>],
        outputs: &[IndexFileMeta],
        checkpoint: &mut impl FnMut(&str) -> Result<()>,
    ) -> Result<()> {
        self.replace_data_inner(inputs, outputs, &[], checkpoint)
    }

    pub(super) fn replace_data_inner(
        &self,
        inputs: &[Arc<IndexFile>],
        outputs: &[IndexFileMeta],
        vlogs: &[(PathBuf, i64, i64)],
        checkpoint: &mut impl FnMut(&str) -> Result<()>,
    ) -> Result<()> {
        self.validate_inputs(inputs)?;
        self.validate_new_index_ids(outputs)?;
        let replacement = Replacement {
            version: 2,
            inputs: inputs
                .iter()
                .map(|f| f.path().file_name().unwrap().to_string_lossy().into_owned())
                .collect(),
            outputs: outputs
                .iter()
                .map(|f| {
                    let name = output_name(f.generation, f.seq, "idx");
                    if f.path != self.path.join(format!("{name}.tmp"))
                        || self.path.join(&name).exists()
                        || inputs.iter().any(|old| old.generation >= f.generation)
                    {
                        return Err(invalid(
                            "replacement output has an invalid path or generation",
                        ));
                    }
                    let meta = fs::symlink_metadata(&f.path)?;
                    if !meta.is_file() {
                        return Err(invalid("replacement output is not a regular file"));
                    }
                    Ok(Output {
                        name,
                        generation: f.generation,
                        seq: f.seq,
                        size: meta.len(),
                    })
                })
                .collect::<Result<_>>()?,
            vlogs: vlogs
                .iter()
                .map(|(path, generation, seq)| {
                    let name = output_name(*generation, *seq, "vlog");
                    if *path != self.path.join(format!("{name}.tmp"))
                        || self.path.join(&name).exists()
                        || !fs::symlink_metadata(path)?.is_file()
                    {
                        return Err(invalid("invalid repair VLog path"));
                    }
                    Ok(Output {
                        name,
                        generation: *generation,
                        seq: *seq,
                        size: fs::metadata(path)?.len(),
                    })
                })
                .collect::<Result<_>>()?,
        };
        replacement.validate()?;
        let record_path = self.path.join(replacement.name());
        if record_path.exists() {
            return Err(invalid("replacement already exists"));
        }
        for output in replacement.vlogs.iter().chain(&replacement.outputs) {
            let temp = self.path.join(format!("{}.tmp", output.name));
            if output.name.ends_with(".idx") {
                let sidecar = super::super::tombstone::path(&temp);
                if sidecar.exists() {
                    let owner = index_file_no(output.generation, output.seq)?;
                    super::super::tombstone::Tombstone::open(&sidecar, owner)?;
                    fs::rename(
                        sidecar,
                        super::super::tombstone::path(&self.path.join(&output.name)),
                    )?;
                }
            }
            File::open(&temp)?.sync_all()?;
            fs::rename(temp, self.path.join(&output.name))?;
            checkpoint("replacement_output_renamed")?;
        }
        sync_dir(&self.path)?;
        let new_files = replacement
            .outputs
            .iter()
            .map(|f| IndexFile::open(&self.path.join(&f.name), f.generation, f.seq))
            .collect::<Result<Vec<_>>>()?;
        let new_vlogs = replacement
            .vlogs
            .iter()
            .map(|f| {
                VLogReader::new_bare(&self.path.join(&f.name), f.generation, f.seq)
                    .map(Arc::new)
                    .map_err(|_| invalid("invalid replacement VLog"))
            })
            .collect::<Result<Vec<_>>>()?;
        for file in &new_files {
            file.set_cache(Arc::clone(&self.cache));
        }
        checkpoint("replacement_outputs_durable")?;
        let temp = self.path.join(format!("{}.tmp", replacement.name()));
        replacement.write(&temp)?;
        fs::rename(temp, record_path)?;
        sync_dir(&self.path)?;
        checkpoint("replacement_durable")?;
        // The commit gate excludes concurrent publication while this list is built.
        let old = self.files.read().indexes.clone();
        let (retired, mut retained): (Vec<_>, Vec<_>) = old
            .into_iter()
            .partition(|f| inputs.iter().any(|input| Arc::ptr_eq(f, input)));
        retained.extend(new_files);
        retained.sort_by_key(|f| (f.generation, f.seq));
        let retired = retired
            .iter()
            .map(|f| RetiredIndex {
                path: f.path().to_path_buf(),
                file_no: f.file_no,
                reader: Arc::downgrade(f),
            })
            .collect::<Vec<_>>();
        let old = {
            let mut files = self.files.write();
            files.vlogs.extend(new_vlogs);
            files.retired.extend(retired);
            std::mem::replace(&mut files.indexes, retained)
        };
        drop(old);
        checkpoint("replacement_published")?;
        Ok(())
    }

    /// Retired indexes remain on disk while queries or backups pin their readers.
    /// No metadata lock is held during unlink or directory synchronization.
    pub(crate) fn reap_retired(&self) -> Result<usize> {
        self.retire_native_indexes()?;
        let retired = std::mem::take(&mut self.files.write().retired);
        let mut deferred = Vec::new();
        let mut removed = 0;
        let mut error = None;
        for item in retired {
            if item.reader.upgrade().is_some() {
                deferred.push(item);
                continue;
            }
            match remove_if_exists(&super::super::tombstone::path(&item.path))
                .and_then(|()| remove_if_exists(&item.path))
            {
                Ok(()) => removed += 1,
                Err(e) => {
                    error = Some(e);
                    deferred.push(item);
                }
            }
        }
        self.files.write().retired.extend(deferred);
        let retired_vlogs = std::mem::take(&mut self.files.write().retired_vlogs);
        let mut deferred = Vec::new();
        for item in retired_vlogs {
            if item.reader.upgrade().is_some() {
                deferred.push(item);
                continue;
            }
            match remove_if_exists(&item.path) {
                Ok(()) => removed += 1,
                Err(e) => {
                    error = Some(e);
                    deferred.push(item);
                }
            }
        }
        self.files.write().retired_vlogs.extend(deferred);
        if removed > 0 {
            sync_dir(&self.path)?;
        }
        if let Some(error) = error {
            return Err(error);
        }
        Ok(removed)
    }

    #[cfg(test)]
    pub(crate) fn compact_files(
        &self,
        inputs: &[Arc<IndexFile>],
        generation: i64,
        cancelled: &impl Fn() -> bool,
    ) -> Result<super::super::compact::MergeStats> {
        self.compact_files_controlled(
            inputs,
            OutputGenerations::isolated(generation),
            cancelled,
            128 * 1024 * 1024,
            Default::default(),
        )
    }

    pub(crate) fn compact_files_controlled(
        &self,
        inputs: &[Arc<IndexFile>],
        generations: OutputGenerations,
        cancelled: &impl Fn() -> bool,
        target_bytes: u64,
        budget: super::super::io_budget::IoBudget,
    ) -> Result<super::super::compact::MergeStats> {
        if self
            .compacting
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(super::super::Error::CompactionBusy);
        }
        let _running = scopeguard::guard((), |_| self.compacting.store(false, Ordering::Release));
        if self.native_metadata {
            return self.compact_native_files(inputs, generations, cancelled, target_bytes, budget);
        }
        self.ensure_ready()?;
        self.validate_inputs(inputs)?;
        let started = slow_trace::now();
        let (_, readers, _) = self.read_view();
        let (outputs, stats, vlogs) = super::super::compact::merge_files_controlled(
            &self.path,
            generations,
            inputs,
            &readers,
            cancelled,
            target_bytes,
            budget,
        )?;
        let _cleanup = scopeguard::guard((), |_| {
            // Only temporary names are ours to remove after a failed/cancelled build.
            // Renamed outputs may already be committed and are left for recovery.
            for output in &outputs {
                let _ = remove_if_exists(&output.path);
            }
            for (path, _, _) in &vlogs {
                let _ = remove_if_exists(path);
            }
        });
        if cancelled() {
            return Err(super::super::Error::CompactionCancelled);
        }
        // Once publication begins, finish it even if cancellation arrives.
        self.replace_data_cancellable(inputs, &outputs, &vlogs, cancelled)?;
        slow_trace::log_background_stage(
            "compaction_total",
            started,
            "aggregate index replacement",
        );
        Ok(stats)
    }
}
