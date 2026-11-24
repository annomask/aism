use std::sync::{Arc, atomic::Ordering};

use super::engine::EngineStatistics;

const DEFAULT_WEIGHTS: [f64; 4] = [0.4, 0.3, 0.2, 0.1];
pub struct Scheduler {
    max_concurrency: i64,
    stats: Arc<EngineStatistics>,

    // queues is the depth of work pending for each compaction level
    queues: [i64; 4],
    weights: [f64; 4],
}

impl Scheduler {
    pub fn new(stats: Arc<EngineStatistics>, max_concurrency: i64) -> Self {
        Self {
            max_concurrency,
            stats,
            queues: [0i64; 4],
            weights: DEFAULT_WEIGHTS,
        }
    }

    pub fn set_depth(&mut self, level: i64, depth: i64) {
        let level = level - 1;
        if level < 0 || level > self.queues.len() as i64 {
            return;
        }
        self.queues[level as usize] = depth;
    }

    fn limits(&self) -> (i64, i64) {
        let mut hi_limit = self.max_concurrency * 4 / 5;
        let mut lo_limit = (self.max_concurrency / 5) + 1;

        if hi_limit == 0 {
            hi_limit = 1;
        }

        if lo_limit == 0 {
            lo_limit = 1;
        }

        (lo_limit, hi_limit)
    }
}

impl Iterator for Scheduler {
    type Item = (i64, bool);

    fn next(&mut self) -> Option<Self::Item> {
        let l1_running = self.stats.tsm_compactions_active[0].load(Ordering::SeqCst);
        let l2_running = self.stats.tsm_compactions_active[1].load(Ordering::SeqCst);
        let l3_running = self.stats.tsm_compactions_active[2].load(Ordering::SeqCst);
        let l4_running = self
            .stats
            .tsm_full_compactions_active
            .load(Ordering::SeqCst)
            + self
                .stats
                .tsm_optimize_compactions_active
                .load(Ordering::SeqCst);

        if l1_running + l2_running + l3_running + l4_running >= self.max_concurrency {
            return None;
        }

        let (mut level, mut runnable) = (0, false);

        let (lo_limit, _) = self.limits();

        let mut end = self.queues.len();
        if l3_running + l4_running >= lo_limit
            && self.max_concurrency - (l1_running + l2_running) == 0
        {
            end = 2;
        }

        let mut weight = 0.0;
        for i in 0..end {
            if (self.queues[i] as f64) * self.weights[i] > weight {
                (level, runnable) = (i + 1, true);
                weight = (self.queues[i] as f64) * self.weights[i];
            }
        }

        Some((level as i64, runnable))
    }
}

pub struct VLogScheduler {
    max_concurrency: i64,
    stats: Arc<EngineStatistics>,

    // queues is the depth of work pending for each compaction level
    queues: [i64; 4],
    weights: [f64; 4],
}

impl VLogScheduler {
    pub fn new(stats: Arc<EngineStatistics>, max_concurrency: i64) -> Self {
        Self {
            max_concurrency,
            stats,
            queues: [0i64; 4],
            weights: DEFAULT_WEIGHTS,
        }
    }

    pub fn set_depth(&mut self, level: i64, depth: i64) {
        let level = level - 1;
        if level < 0 || level > self.queues.len() as i64 {
            return;
        }
        self.queues[level as usize] = depth;
    }

    fn limits(&self) -> (i64, i64) {
        let mut hi_limit = self.max_concurrency * 4 / 5;
        let mut lo_limit = (self.max_concurrency / 5) + 1;

        if hi_limit == 0 {
            hi_limit = 1;
        }

        if lo_limit == 0 {
            lo_limit = 1;
        }

        (lo_limit, hi_limit)
    }
}

impl Iterator for VLogScheduler {
    type Item = (i64, bool);

    fn next(&mut self) -> Option<Self::Item> {
        let l1_running = self.stats.vlog_compactions_active[0].load(Ordering::SeqCst);
        let l2_running = self.stats.vlog_compactions_active[1].load(Ordering::SeqCst);
        let l3_running = self.stats.vlog_compactions_active[2].load(Ordering::SeqCst);
        let l4_running = self
            .stats
            .vlog_full_compactions_active
            .load(Ordering::SeqCst)
            + self
                .stats
                .vlog_optimize_compactions_active
                .load(Ordering::SeqCst);

        if l1_running + l2_running + l3_running + l4_running >= self.max_concurrency {
            return None;
        }

        let (mut level, mut runnable) = (0, false);

        let (lo_limit, _) = self.limits();

        let mut end = self.queues.len();
        if l3_running + l4_running >= lo_limit
            && self.max_concurrency - (l1_running + l2_running) == 0
        {
            end = 2;
        }

        let mut weight = 0.0;
        for i in 0..end {
            if (self.queues[i] as f64) * self.weights[i] > weight {
                (level, runnable) = (i + 1, true);
                weight = (self.queues[i] as f64) * self.weights[i];
            }
        }

        Some((level as i64, runnable))
    }
}
