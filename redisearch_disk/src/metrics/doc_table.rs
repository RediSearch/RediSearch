//! Doc table specific metrics.
//!
//! These metrics are specific to the document table and include
//! column family metrics plus memory-resident data like deleted IDs tracking.

use std::ops::AddAssign;

use super::ColumnFamilyMetrics;
use crate::info_sink::InfoSink;

#[derive(Debug, Default, Clone)]
pub struct Metrics {
    /// Column family metrics for the doc table.
    pub column_family: ColumnFamilyMetrics,
    /// Number of deleted document IDs currently tracked in the bitmap.
    pub deleted_ids_count: u64,
}

impl Metrics {
    /// Returns the total memory used by this doc table's disk components.
    pub fn total_memory(&self) -> u64 {
        self.column_family.total_memory()
    }

    /// Outputs the doc table metrics to an info sink.
    pub fn output_to_info_sink(&self, sink: &mut impl InfoSink) {
        self.column_family.output_to_info_sink(sink);
        sink.add_u64(c"deleted_ids_count", self.deleted_ids_count);
    }
}

impl AddAssign for Metrics {
    fn add_assign(&mut self, other: Self) {
        self.column_family += other.column_family;
        self.deleted_ids_count += other.deleted_ids_count;
    }
}
