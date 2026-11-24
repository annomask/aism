//! Rebuild the tag index from persisted aggregate identities and recovered cache.

use super::*;
use crate::index::SeriesIDIterator;

impl AggregateIndexContext {
    /// Called with deletion admission held and the background worker stopped.
    /// This bounded TSI work queue never participates in data visibility.
    pub(super) fn remember_cleanup(
        &self,
        ranges: &vlog::index::delete_batch::DeleteBatch,
    ) -> Result<()> {
        let previous = { self.cleanup_candidates.lock().clone() };
        let mut builder = self.store.delete_batch_builder();
        if let Some(previous) = previous {
            for row in previous.rows() {
                builder
                    .push(row.map_err(aggregate_index_error)?)
                    .map_err(aggregate_index_error)?;
            }
        }
        for row in ranges.rows() {
            let id = row.map_err(aggregate_index_error)?.seq.series_id();
            builder
                .push(vlog::index::delete::DeleteRange {
                    seq: vlog::index::SeqNo::new(id, 1).map_err(aggregate_index_error)?,
                    min: 0,
                    max: 0,
                    cutoff: 0,
                })
                .map_err(aggregate_index_error)?;
        }
        let batch = Arc::new(builder.finish().map_err(aggregate_index_error)?);
        // A constant-size marker covers a crash after the source WAL is retired.
        // Recovery streams TSI once without a resident map of all series IDs.
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.metadata_path.join("agg-metadata.cleanup"))?
            .sync_all()?;
        std::fs::File::open(self.metadata_path.as_path())?.sync_all()?;
        let old = {
            let mut candidates = self.cleanup_candidates.lock();
            std::mem::replace(&mut *candidates, Some(batch))
        };
        drop(old);
        self.cleanup_pending.store(true, Ordering::SeqCst);
        Ok(())
    }
}

impl Engine {
    pub(super) fn reindex_aggregate_index(&self) -> Result<()> {
        let started = slow_trace::now();
        let context = self.aggregate_index.as_ref().unwrap();
        context
            .store
            .ensure_ready()
            .map_err(aggregate_index_error)?;
        let cache = { Arc::clone(&self.inner.read().cache) };
        let files = context.store.index_readers();
        let mut batch = SeriesBatch::new(&self.index);
        for file in files {
            let mut previous = None;
            for page in 0..file.footer.directory_pages() {
                for entry in file.directory_page(page).map_err(aggregate_index_error)? {
                    let id = entry.seq_no.series_id();
                    if previous == Some(id) {
                        continue;
                    }
                    previous = Some(id);
                    let (name, tags) = self.sfile.series(id).context(SeriesFileSnafu)?;
                    let tags = tags.ok_or_else(|| Error::StringErr {
                        reason: "aggregate index references a missing series identity".into(),
                    })?;
                    // Finalized database-wide DROP can leave fully tombstoned
                    // trees until compaction. Retired identities with visible
                    // values remain a recovery error, rather than losing data.
                    if self.sfile.is_deleted(id)
                        && !aggregate_series_has_values(context, &cache, id, &name, &tags, &|| false)?
                    {
                        continue;
                    }
                    // Recreating a deleted ID would disconnect the tag index
                    // from every persisted field tree using the original ID.
                    if self.sfile.series_id(&name, &tags) != id {
                        return Err(Error::StringErr {
                            reason: "aggregate index series identity is not active".into(),
                        });
                    }
                    if !aggregate_series_has_values(context, &cache, id, &name, &tags, &|| false)? {
                        continue;
                    }
                    batch.push(name, tags)?;
                }
            }
        }
        // The cache was populated by WAL recovery before reindexing. Its keys
        // may include series that have never appeared in an index file.
        let mut previous = Vec::new();
        for key in cache.keys() {
            let (series, _) = series_and_file_form_composite_key(&key);
            if previous == series {
                continue;
            }
            previous.clear();
            previous.extend_from_slice(series);
            let (name, tags) = crate::models::points::parse_key_bytes(series);
            batch.push(name, tags)?;
        }
        batch.flush()?;
        slow_trace::log_background_stage("aggregate_reindex", started, "series metadata");
        Ok(())
    }

    pub(super) fn reconcile_aggregate_series(&self, id: u64) -> Result<()> {
        let cache = { Arc::clone(&self.inner.read().cache) };
        reconcile_series(
            self.aggregate_index.as_ref().unwrap(),
            &self.index,
            &cache,
            id,
        )
    }

    pub(super) fn reconcile_aggregate_metadata(&self) -> Result<()> {
        let cache = { Arc::clone(&self.inner.read().cache) };
        reconcile_metadata(
            self.aggregate_index.as_ref().unwrap(),
            &self.index,
            &cache,
            &|| false,
        )
    }

    pub(super) fn aggregate_visible_tags(
        &self,
        name: &[u8],
    ) -> Result<std::collections::BTreeMap<String, std::collections::BTreeSet<String>>> {
        let index_set =
            index::IndexSet::new(Arc::clone(&self.index), Arc::clone(&self.sfile), None);
        let Some(iter) = index_set
            .measurement_series_iter(name)
            .context(IndexSetSnafu)?
        else {
            return Ok(Default::default());
        };
        let mut iter = new_filter_undeleted_series_iterator_adapter(Arc::clone(&self.sfile), iter);
        let mut tags =
            std::collections::BTreeMap::<String, std::collections::BTreeSet<String>>::new();
        for series in iter.by_ref() {
            for tag in series.tags().iter() {
                tags.entry(String::from_utf8_lossy(&tag.key).into_owned())
                    .or_default()
                    .insert(String::from_utf8_lossy(&tag.value).into_owned());
            }
        }
        iter.close();
        Ok(tags)
    }
}

fn aggregate_series_has_values(
    context: &AggregateIndexContext,
    cache: &Cache,
    id: u64,
    name: &[u8],
    tags: &Tags,
    cancelled: &impl Fn() -> bool,
) -> Result<bool> {
    #[cfg(test)]
    context.metadata_probes.fetch_add(1, Ordering::Relaxed);
    let Some(fields) = context.fields.fields(name) else {
        return Ok(false);
    };
    let key = crate::models::points::make_key(name, tags);
    for (field, typ) in fields.field_set() {
        check_cleanup_cancelled(cancelled)?;
        let mut composite = key.clone();
        composite.extend_from_slice(KEY_FIELD_SEPARATOR.as_bytes());
        composite.extend_from_slice(field.as_bytes());
        // Cache first, matching the query snapshot visibility rule.
        if !cache.values(&composite).is_empty() {
            return Ok(true);
        }
        let seq = vlog::index::SeqNo::new(
            id,
            fields
                .ordinal(field.as_bytes())
                .ok_or_else(|| Error::StringErr {
                    reason: "aggregate metadata field ordinal is missing".into(),
                })?,
        )
        .map_err(aggregate_index_error)?;
        if context
            .store
            .raw_cursor(seq, typ, i64::MIN, i64::MAX, true)
            .map_err(aggregate_index_error)?
            .has_values(cancelled)
            .map_err(aggregate_index_error)?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn reconcile_series(
    context: &AggregateIndexContext,
    index: &Index,
    cache: &Cache,
    id: u64,
) -> Result<()> {
    reconcile_series_with_hook(context, index, cache, id, &|| false, || {}).map(|_| ())
}

fn reconcile_series_with_hook(
    context: &AggregateIndexContext,
    index: &Index,
    cache: &Cache,
    id: u64,
    cancelled: &impl Fn() -> bool,
    before_install: impl FnOnce(),
) -> Result<bool> {
    let epoch = context.write_epoch.load(Ordering::SeqCst);
    if context.active_writers.load(Ordering::SeqCst) != 0 {
        context.cleanup_pending.store(true, Ordering::SeqCst);
        return Ok(false);
    }
    let (name, tags) = context.sfile.series(id).context(SeriesFileSnafu)?;
    let Some(tags) = tags else {
        return Ok(true);
    };
    let key = crate::models::points::make_key(&name, &tags);
    if !index.contains_series_id(id, &key) {
        return Ok(true);
    }
    if aggregate_series_has_values(context, cache, id, &name, &tags, cancelled)? {
        return Ok(true);
    }
    before_install();
    check_cleanup_cancelled(cancelled)?;
    if !index
        .drop_series_if(id, &key, || {
            context.active_writers.load(Ordering::SeqCst) == 0
                && context.write_epoch.load(Ordering::SeqCst) == epoch
        })
        .context(IndexSnafu)?
    {
        context.cleanup_pending.store(true, Ordering::SeqCst);
        return Ok(false);
    }
    Ok(true)
}

#[cfg(test)]
mod cleanup_tests {
    use super::*;

    #[test]
    fn aggregate_metadata_deferred_cache_delete_recovers_after_wal_retirement() {
        let mut e = super::super::tests::Engine::with_aggregate_index();
        e.engine
            .measurement_fields(b"cpu")
            .create_field_if_not_exists(b"i", crate::models::INTEGER)
            .unwrap();
        e.write_points_string(&["cpu,host=A i=1i 10"]).unwrap();
        let context = Arc::clone(e.engine.aggregate_index.as_ref().unwrap());
        context.active_writers.fetch_add(1, Ordering::SeqCst);
        e.engine.delete_measurement(b"cpu").unwrap();
        context.active_writers.fetch_sub(1, Ordering::SeqCst);
        assert_eq!(e.engine.series_n(), 1);
        assert!(context.metadata_path.join("agg-metadata.cleanup").exists());
        e.write_points_string(&["cpu,host=B i=2i 20"]).unwrap();
        e.must_write_snapshot();
        e.reopen();
        assert_eq!(
            e.engine.measurement_series_keys(b"cpu").unwrap(),
            [b"cpu,host=B".to_vec()]
        );
        assert!(!context.metadata_path.join("agg-metadata.cleanup").exists());
        e.close();
    }

    #[test]
    fn aggregate_metadata_cleanup_probes_only_deletion_candidates() {
        let mut e = super::super::tests::Engine::with_aggregate_index();
        e.engine
            .measurement_fields(b"cpu")
            .create_field_if_not_exists(b"i", crate::models::INTEGER)
            .unwrap();
        let lines = (0..20)
            .map(|host| format!("cpu,host=S{host} i=1i 10"))
            .collect::<Vec<_>>();
        e.write_points_string(&lines.iter().map(String::as_str).collect::<Vec<_>>())
            .unwrap();
        e.must_write_snapshot();
        let context = Arc::clone(e.engine.aggregate_index.as_ref().unwrap());
        context.active_writers.fetch_add(1, Ordering::SeqCst);
        e.engine
            .delete_series_range_by_measurement(
                b"cpu",
                Some(|_: &[u8], tags: &Tags| {
                    (i64::MIN, i64::MAX, tags.iter().any(|t| t.value == b"S0"))
                }),
            )
            .unwrap();
        context.active_writers.fetch_sub(1, Ordering::SeqCst);
        context.metadata_probes.store(0, Ordering::Relaxed);
        e.engine.reconcile_aggregate_metadata().unwrap();
        assert_eq!(context.metadata_probes.load(Ordering::Relaxed), 1);
        assert_eq!(e.engine.series_n(), 19);
        e.close();
    }

    #[test]
    fn aggregate_metadata_cleanup_rechecks_writer_after_disk_probe() {
        let mut e = super::super::tests::Engine::with_aggregate_index();
        e.engine
            .measurement_fields(b"cpu")
            .create_field_if_not_exists(b"i", crate::models::INTEGER)
            .unwrap();
        e.write_points_string(&["cpu,host=A i=1i 10"]).unwrap();
        let context = e.engine.aggregate_index.as_ref().unwrap();
        context.active_writers.fetch_add(1, Ordering::SeqCst);
        e.engine.delete_measurement(b"cpu").unwrap();
        context.active_writers.fetch_sub(1, Ordering::SeqCst);
        let id = context.sequence(b"cpu,host=A#!~#i").unwrap().series_id();
        let cache = { Arc::clone(&e.engine.inner.read().cache) };
        reconcile_series_with_hook(context, &e.engine.index, &cache, id, &|| false, || {
            e.write_points_string(&["cpu,host=A i=2i 20"]).unwrap();
        })
        .unwrap();
        assert_eq!(
            e.engine.measurement_series_keys(b"cpu").unwrap(),
            [b"cpu,host=A".to_vec()]
        );
        assert_eq!(
            super::super::aggregate_index_tests::aggregate(
                &e,
                "i",
                crate::query::storage_ir::TimeRange::all()
            )
            .state
            .count,
            1
        );
        e.close();
    }

    #[test]
    fn aggregate_metadata_cleanup_defers_while_writer_is_active() {
        let mut e = super::super::tests::Engine::with_aggregate_index();
        e.engine
            .measurement_fields(b"cpu")
            .create_field_if_not_exists(b"i", crate::models::INTEGER)
            .unwrap();
        e.write_points_string(&["cpu,host=A i=1i 10"]).unwrap();
        let context = Arc::clone(e.engine.aggregate_index.as_ref().unwrap());
        context.active_writers.fetch_add(1, Ordering::SeqCst);
        e.engine.delete_measurement(b"cpu").unwrap();
        assert_eq!(e.engine.series_n(), 1);
        assert!(context.cleanup_pending.load(Ordering::SeqCst));
        e.engine.compact_aggregate_index().unwrap();
        assert!(context.store.read_view().2.is_empty());
        assert_eq!(e.engine.series_n(), 1);
        context.active_writers.fetch_sub(1, Ordering::SeqCst);
        let cache = { Arc::clone(&e.engine.inner.read().cache) };
        assert!(reconcile_metadata(&context, &e.engine.index, &cache, &|| true).is_err());
        assert!(context.cleanup_pending.load(Ordering::SeqCst));
        assert_eq!(e.engine.series_n(), 1);
        e.engine.reconcile_aggregate_metadata().unwrap();
        assert_eq!(e.engine.series_n(), 0);
        assert!(!e.engine.measurement_exists(b"cpu").unwrap());
        e.close();
    }
}

pub(super) fn reconcile_metadata(
    context: &AggregateIndexContext,
    index: &Index,
    cache: &Cache,
    cancelled: &impl Fn() -> bool,
) -> Result<()> {
    let started = slow_trace::now();
    context.cleanup_pending.store(false, Ordering::SeqCst);
    let result = (|| {
        check_cleanup_cancelled(cancelled)?;
        let probe = |id| -> Result<bool> {
            check_cleanup_cancelled(cancelled)?;
            if !reconcile_series_with_hook(context, index, cache, id, cancelled, || {})? {
                context.cleanup_pending.store(true, Ordering::SeqCst);
                return Ok(false);
            }
            Ok(true)
        };
        let candidates = { context.cleanup_candidates.lock().clone() };
        if context.cleanup_recovery.load(Ordering::SeqCst) {
            if let Some(measurements) = index.measurement_iter().context(IndexSnafu)? {
                let mut measurements = scopeguard::guard(measurements, |m| {
                    let _ = m.close();
                });
                for measurement in measurements.by_ref() {
                    if let Some(mut series) = index
                        .measurement_series_id_iter(&measurement)
                        .context(IndexSnafu)?
                    {
                        let result: Result<bool> = (|| {
                            for item in series.by_ref() {
                                if !probe(item.series_id)? {
                                    return Ok(false);
                                }
                            }
                            Ok(true)
                        })();
                        let _ = series.close();
                        if !result? {
                            return Ok(());
                        }
                    }
                }
            }
            context.cleanup_recovery.store(false, Ordering::SeqCst);
        }
        if let Some(batch) = &candidates {
            for row in batch.rows() {
                if !probe(row.map_err(aggregate_index_error)?.seq.series_id())? {
                    return Ok(());
                }
            }
            let mut current = context.cleanup_candidates.lock();
            if current.as_ref().is_some_and(|b| Arc::ptr_eq(b, batch)) {
                *current = None;
            }
        }
        // Tombstones also serve data-only measurement deletion. Only ordinary
        // deletion queues metadata cleanup; compaction must not infer it from
        // an empty tree. Its durable marker handles interrupted queue recovery.
        let no_candidates = { context.cleanup_candidates.lock().is_none() };
        if !context.cleanup_pending.load(Ordering::SeqCst) && no_candidates {
            match std::fs::remove_file(context.metadata_path.join("agg-metadata.cleanup")) {
                Ok(()) => std::fs::File::open(context.metadata_path.as_path())?.sync_all()?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    })();
    if result.is_err() {
        context.cleanup_pending.store(true, Ordering::SeqCst);
    }
    slow_trace::log_background_stage(
        "aggregate_metadata_cleanup",
        started,
        "paged series liveness",
    );
    result
}

fn check_cleanup_cancelled(cancelled: &impl Fn() -> bool) -> Result<()> {
    if cancelled() {
        return Err(aggregate_index_error(
            vlog::index::Error::CompactionCancelled,
        ));
    }
    Ok(())
}

struct SeriesBatch<'a> {
    index: &'a Index,
    keys: Vec<Vec<u8>>,
    names: Vec<Vec<u8>>,
    tags: Vec<Tags>,
}

impl<'a> SeriesBatch<'a> {
    fn new(index: &'a Index) -> Self {
        Self {
            index,
            keys: Vec::new(),
            names: Vec::new(),
            tags: Vec::new(),
        }
    }

    fn push(&mut self, name: Vec<u8>, tags: Tags) -> Result<()> {
        self.keys
            .push(crate::models::points::make_key(&name, &tags));
        self.names.push(name);
        self.tags.push(tags);
        if self.keys.len() == Engine::REINDEX_BATCH_SIZE {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.keys.is_empty() {
            return Ok(());
        }
        let keys = self.keys.iter().map(Vec::as_slice).collect::<Vec<_>>();
        self.index
            .create_series_list_if_not_exists(
                &keys,
                std::mem::take(&mut self.names),
                std::mem::take(&mut self.tags),
            )
            .context(IndexSnafu)?;
        self.keys.clear();
        Ok(())
    }
}
