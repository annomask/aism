//! Delete coordination for the aggregate index; ordinary writes stay parallel.
use super::*;

impl Engine {
    pub(super) fn delete_aggregate_batch(
        &self,
        ranges: &vlog::index::delete_batch::DeleteBatch,
        mode: DeleteSeriesMode,
    ) -> Result<()> {
        let context = self.aggregate_index.as_ref().unwrap();
        let started = slow_trace::now();
        // Admission is changed under the same short engine lock as cache rotation.
        // Wait without holding an engine/cache/file-list lock: existing snapshots
        // must finish publication, including their failure cleanup.
        loop {
            let admitted = {
                let _inner = self.inner.write();
                context
                    .deleting
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            };
            if admitted {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        context.write_epoch.fetch_add(1, Ordering::SeqCst);
        let _admission = scopeguard::guard((), |_| {
            context.write_epoch.fetch_add(1, Ordering::SeqCst);
            context.deleting.store(false, Ordering::SeqCst);
        });
        self.disable_level_compactions(true);
        let _restart = scopeguard::guard((), |_| self.enable_level_compactions(true));
        let vlog_enabled = self.inner.read().vlog_done.is_some();
        if vlog_enabled { self.disable_vlog_compactions(true); }
        let _restart_vlog = scopeguard::guard((), |_| {
            if vlog_enabled { self.enable_vlog_compactions(true); }
        });
        let file_store = {
            let inner = self.inner.read();
            Arc::clone(&inner.vlog_inner.as_ref().unwrap().read().vlog_file_store)
        };
        let (cache, wal) = {
            let inner = self.inner.read();
            (Arc::clone(&inner.cache), Arc::clone(&inner.wal))
        };
        while cache.snapshot_in_progress() {
            std::thread::sleep(Duration::from_millis(10));
        }
        slow_trace::log_background_stage("delete_wait_for_snapshot", started, "aggregate index");
        context
            .store
            .ensure_ready()
            .map_err(aggregate_index_error)?;
        context.sync_metadata()?;
        if mode.reconciles_metadata() {
            context.remember_cleanup(ranges)?;
        }
        let flush = |keys: &mut Vec<Vec<u8>>, min, max| -> Result<()> {
            if keys.is_empty() {
                return Ok(());
            }
            // Native tombstone writers require composite keys in lexical order.
            // AISM delete batches are ordered by series/field ordinal instead.
            keys.sort_unstable();
            let mut stripes = keys
                .iter()
                .map(|key| crc32fast::hash(key) as usize % 256)
                .collect::<Vec<_>>();
            stripes.sort_unstable();
            stripes.dedup();
            let wait = slow_trace::now();
            let _guards = stripes
                .iter()
                .map(|i| context.delete_stripes[*i].write())
                .collect::<Vec<_>>();
            slow_trace::log_lock_wait("delete_field_stripe", wait, "cache and WAL ordering");
            // WAL precedes volatile removal; later writes remain visible after restart.
            if self.wal_enabled {
                futures::executor::block_on(wal.delete_range(keys.clone(), min, max))
                    .context(WALSnafu)?;
            }
            cache
                .delete_range_including_idle_snapshot(
                    &keys.iter().map(Vec::as_slice).collect::<Vec<_>>(),
                    min,
                    max,
                )
                .context(CacheSnafu)?;
            // Aggregate queries read these same native tombstones. There is
            // no second delete log or copy in the hierarchy files.
            file_store.delete_range(keys.clone(), min, max)
                .map_err(|e| Error::StringErr { reason: e.to_string() })?;
            keys.clear();
            Ok(())
        };
        let mut keys = Vec::new();
        let mut interval = (0, 0);
        for range in ranges.rows() {
            let range = range.map_err(aggregate_index_error)?;
            if interval != (range.min, range.max) || keys.len() == 128 {
                flush(&mut keys, interval.0, interval.1)?;
            }
            interval = (range.min, range.max);
            let (name, tags) = context
                .sfile
                .series(range.seq.series_id())
                .context(SeriesFileSnafu)?;
            let tags = tags.ok_or_else(|| Error::StringErr {
                reason: "delete references missing series identity".into(),
            })?;
            let fields = context
                .fields
                .fields(&name)
                .ok_or_else(|| Error::StringErr {
                    reason: "delete references missing measurement fields".into(),
                })?;
            let field = fields
                .field_keys()
                .into_iter()
                .find(|field| fields.ordinal(field.as_bytes()) == Some(range.seq.field_ordinal()))
                .ok_or_else(|| Error::StringErr {
                    reason: "delete references missing field ordinal".into(),
                })?;
            let mut key = crate::models::points::make_key(&name, &tags);
            key.extend_from_slice(KEY_FIELD_SEPARATOR.as_bytes());
            key.extend_from_slice(field.as_bytes());
            keys.push(key);
        }
        flush(&mut keys, interval.0, interval.1)?;
        if mode.reconciles_metadata() {
            context.cleanup_pending.store(true, Ordering::SeqCst);
            self.reconcile_aggregate_metadata()?;
        }
        slow_trace::log_background_stage(
            "delete_total",
            started,
            "file-owned aggregate tombstones",
        );
        Ok(())
    }
}
