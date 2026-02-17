//! Cumulative compaction metrics (cycles, time).

use std::ops::AddAssign;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::info_sink::InfoSink;

/// Cumulative compaction metrics for the fulltext column family.
#[derive(Debug, Clone, Copy, Default)]
pub struct Metrics {
    /// Number of compaction cycles run.
    pub cycles: u64,
    /// Milliseconds spent in compaction.
    pub ms_run: u64,
}

impl AddAssign for Metrics {
    fn add_assign(&mut self, other: Self) {
        self.cycles += other.cycles;
        self.ms_run += other.ms_run;
    }
}

impl Metrics {
    /// Outputs the compaction metrics to an info sink.
    pub fn output_to_info_sink(&self, sink: &mut impl InfoSink) {
        sink.add_u64(c"compaction_total_cycles", self.cycles);
        sink.add_u64(c"compaction_total_ms_run", self.ms_run);
    }
}

/// Atomic version of compaction metrics for lock-free updates from `compact_full`.
#[derive(Debug, Default)]
pub struct AtomicMetrics {
    cycles: AtomicU64,
    ms_run: AtomicU64,
}

impl AtomicMetrics {
    /// Records one compaction run: increments cycles and adds elapsed ms.
    pub fn record(&self, metrics: Metrics) {
        self.cycles.fetch_add(metrics.cycles, Ordering::Relaxed);
        self.ms_run.fetch_add(metrics.ms_run, Ordering::Relaxed);
    }

    /// Loads the current metrics as a `Metrics` snapshot.
    pub fn load(&self) -> Metrics {
        Metrics {
            cycles: self.cycles.load(Ordering::Relaxed),
            ms_run: self.ms_run.load(Ordering::Relaxed),
        }
    }
}
