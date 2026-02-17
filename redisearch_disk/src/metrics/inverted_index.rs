//! Inverted index specific metrics.
//!
//! These metrics are specific to the fulltext inverted index and include
//! column family metrics plus cumulative compaction metrics.

use std::ops::AddAssign;

use super::{ColumnFamilyMetrics, CompactionMetrics};
use crate::info_sink::InfoSink;

#[derive(Debug, Default, Clone)]
pub struct Metrics {
    /// Column family metrics for the inverted index.
    pub column_family: ColumnFamilyMetrics,
    /// Cumulative compaction metrics (cycles, ms run).
    pub compaction: CompactionMetrics,
}

impl Metrics {
    /// Returns the total memory used by this inverted index's disk components.
    pub fn total_memory(&self) -> u64 {
        self.column_family.total_memory()
    }

    /// Outputs the inverted index metrics to an info sink.
    pub fn output_to_info_sink(&self, sink: &mut impl InfoSink) {
        self.column_family.output_to_info_sink(sink);
        self.compaction.output_to_info_sink(sink);
    }
}

impl AddAssign for Metrics {
    fn add_assign(&mut self, other: Self) {
        self.column_family += other.column_family;
        self.compaction += other.compaction;
    }
}
