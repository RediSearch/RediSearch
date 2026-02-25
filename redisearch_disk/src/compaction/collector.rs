//! Thread-safe collector for delta building during compaction.
//!
//! The `CompactionDeltaCollector` is used during SpeeDB compaction to collect
//! information about removed documents. It is shared with the merge operator
//! closure (via `Arc<Mutex<...>>`) and written to during merge operations.
//!
//! After compaction completes, the collected `CompactionDelta` is used to
//! update in-memory structures.

use std::sync::{Arc, Mutex};

use super::CompactionDelta;

/// Thread-safe collector for compaction delta data.
///
/// This struct wraps `CompactionDelta` in `Arc<Mutex<...>>` to allow
/// safe concurrent access from the merge operator (which runs on SpeeDB
/// threads during compaction).
///
/// # Usage Pattern
///
/// 1. Create the collector and pass a clone to the merge operator closure
/// 2. Before compaction: call `clear()` to reset state
/// 3. During compaction: merge operator calls `record_*` methods via `MergeCallback`
/// 4. After compaction: call `take()` to get the `CompactionDelta`
///
/// # Deduplication
///
/// The collector handles the case where the same `(term, doc_id)` pair appears
/// in multiple SST levels. Each unique pair is counted only once (handled by
/// `CompactionDelta` internally).
#[derive(Clone)]
pub struct CompactionDeltaCollector(Arc<Mutex<CompactionDelta>>);

impl Default for CompactionDeltaCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl CompactionDeltaCollector {
    /// Creates a new empty collector.
    pub fn new() -> Self {
        CompactionDeltaCollector(Arc::new(Mutex::new(CompactionDelta::new())))
    }

    /// Clears all collected data. Called before starting compaction.
    pub fn clear(&self) {
        self.0.lock().expect("collector lock poisoned").clear();
    }

    /// Takes the collected delta, replacing it with an empty delta.
    ///
    /// This is called after compaction completes to retrieve the collected
    /// delta for application.
    pub fn take(&self) -> CompactionDelta {
        std::mem::take(&mut *self.0.lock().expect("collector lock poisoned"))
    }

    /// Returns true if no data has been collected.
    pub fn is_empty(&self) -> bool {
        self.0.lock().expect("collector lock poisoned").is_empty()
    }
}

impl super::MergeCallbacks for CompactionDeltaCollector {
    /// Records that a document was removed from a term's posting list.
    ///
    /// # Arguments
    /// * `term` - The term whose posting list had a document removed
    /// * `doc_id` - The document ID that was removed
    fn on_doc_removed(&self, term: &str, doc_id: u64) {
        let mut guard = self.0.lock().expect("collector lock poisoned");
        guard.term_deltas.increment(term.as_bytes(), 1);
        guard.compacted_doc_ids.insert(doc_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compaction::MergeCallbacks;

    #[test]
    fn test_on_doc_removed() {
        let collector = CompactionDeltaCollector::new();

        collector.on_doc_removed("hello", 1);
        collector.on_doc_removed("hello", 2);
        collector.on_doc_removed("world", 3);

        let delta = collector.take();

        assert_eq!(delta.term_deltas.get(b"hello"), Some(2));
        assert_eq!(delta.term_deltas.get(b"world"), Some(1));
        assert_eq!(delta.compacted_doc_ids.len(), 3);
        assert!(delta.compacted_doc_ids.contains(&1));
        assert!(delta.compacted_doc_ids.contains(&2));
        assert!(delta.compacted_doc_ids.contains(&3));
    }

    #[test]
    fn test_on_doc_removed_accumulates_counts() {
        let collector = CompactionDeltaCollector::new();

        // Multiple calls for same term accumulate
        collector.on_doc_removed("hello", 1);
        collector.on_doc_removed("hello", 2);
        collector.on_doc_removed("hello", 3);

        let delta = collector.take();

        // Count should be 3 (one per call)
        assert_eq!(delta.term_deltas.get(b"hello"), Some(3));
        assert_eq!(delta.compacted_doc_ids.len(), 3);
    }
}
