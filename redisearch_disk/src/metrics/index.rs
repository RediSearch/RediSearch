//! Per-index aggregate metrics.

use super::{AsyncReadMetrics, ColumnFamilyMetrics};

/// Metrics for a single index, containing both doc_table and inverted_index metrics.
#[derive(Debug, Default, Clone)]
pub struct Metrics {
    pub doc_table: ColumnFamilyMetrics,
    pub inverted_index: ColumnFamilyMetrics,
    pub async_read: AsyncReadMetrics,
}

impl Metrics {
    /// Returns the total memory used by this index's disk components.
    pub fn total_memory(&self) -> u64 {
        self.doc_table.total_memory() + self.inverted_index.total_memory()
    }
}
