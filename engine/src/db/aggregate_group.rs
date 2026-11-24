use super::*;
use crate::storage::tsm1::engine::aggregate_cross_shard::{CrossShardCache, aggregate_group};
use parking_lot::Mutex;

pub use crate::storage::tsm1::engine::aggregate_cross_shard::{
    CrossShardAggregateMetrics, CrossShardAggregateResult,
};

/// One local shard and the inclusive time range assigned to it by the runtime.
/// Values outside this range are excluded from the group query.
pub struct AggregateShard {
    pub database: Arc<ReefsDb>,
    pub time_range: crate::TimeRange,
}

/// A fixed set of disjoint time shards sharing one database's series identities.
/// Routing and SQL planning remain with the caller. The bounded logical index is
/// derived from native aggregate records; reopening rebuilds it without a second
/// WAL or changes to the native data/index formats.
pub struct AggregateShardGroup {
    members: Vec<AggregateShard>,
    cache: Mutex<CrossShardCache>,
}

impl AggregateShardGroup {
    pub fn new(mut members: Vec<AggregateShard>) -> Result<Self> {
        if members.is_empty() {
            return Err(Error::StringErr {
                reason: "aggregate shard group is empty".into(),
            });
        }
        members.sort_by_key(|m| m.time_range.start_time);
        for (i, member) in members.iter().enumerate() {
            if member.time_range.start_time > member.time_range.end_time
                || i > 0 && members[i - 1].time_range.end_time >= member.time_range.start_time
            {
                return Err(Error::StringErr {
                    reason: "aggregate shard ranges must be ordered and disjoint".into(),
                });
            }
            if !Arc::ptr_eq(&member.database.sfile, &members[0].database.sfile) {
                return Err(Error::StringErr {
                    reason: "aggregate shards must share a database series file".into(),
                });
            }
            if members[..i].iter().any(|m| {
                Arc::ptr_eq(&m.database, &member.database)
                    || m.database.path == member.database.path
                    || m.database.options.shard_id == member.database.options.shard_id
            }) {
                return Err(Error::StringErr {
                    reason: "duplicate aggregate shard".into(),
                });
            }
        }
        Ok(Self {
            members,
            cache: Mutex::new(CrossShardCache::default()),
        })
    }

    /// Reduces one selected field over disjoint windows and an exact series
    /// predicate. Acquire all lifecycle leases before preparing any read view.
    /// `None` means a member has no aggregate capability, or no member has the
    /// field. Row predicates are rejected by the native aggregate path.
    /// Floating SUM/AVG preserve the native per-shard reduction order.
    pub fn aggregate_projected_scan_windows_filtered(
        &self,
        request: AggregateRequest,
        windows: Vec<crate::TimeRange>,
        predicate: StoragePredicate,
    ) -> Result<Option<CrossShardAggregateResult>> {
        let _leases = self
            .members
            .iter()
            .map(|m| m.database.acquire_operation())
            .collect::<Result<Vec<_>>>()?;
        let mut prepared = Vec::with_capacity(self.members.len());
        for member in &self.members {
            let part = {
                let started = slow_trace::now();
                let inner = member.database.inner.read();
                slow_trace::log_lock_wait("shard.read", started, "cross-shard aggregate prepare");
                inner
                    .engine()?
                    .prepare_cross_shard_aggregate(
                        request.clone(),
                        predicate.clone(),
                        member.time_range,
                    )
                    .context(EngineSnafu)?
            };
            let Some(part) = part else { return Ok(None) };
            prepared.push(part);
        }
        aggregate_group(&prepared, &request, &windows, &self.cache).context(EngineSnafu)
    }
}
