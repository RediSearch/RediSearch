//! Per-index aggregate metrics.

use super::{AsyncReadMetrics, DocTableMetrics, InvertedIndexMetrics};

/// Metrics for a single index, containing both doc_table and inverted_index metrics.
#[derive(Debug, Default, Clone)]
pub struct Metrics {
    pub doc_table: DocTableMetrics,
    pub term_inverted_index: InvertedIndexMetrics,
    pub tag_inverted_index: InvertedIndexMetrics,
    pub async_read: AsyncReadMetrics,
}

impl Metrics {
    /// Returns the total memory used by this index's disk components.
    pub fn total_memory(&self) -> u64 {
        self.doc_table.total_memory()
            + self.term_inverted_index.total_memory()
            + self.tag_inverted_index.total_memory()
    }
}
