//! Delta application logic for compaction.
//!
//! This module contains the logic for applying a `CompactionDelta` to in-memory
//! structures by calling FFI functions on the C IndexSpec.

use super::{CompactionDelta, IndexSpecUpdater};
use tracing::debug;

/// Applies a compaction delta to in-memory structures.
///
/// This function updates C structures via the provided updater and Rust-owned
/// DeletedIds (removes compacted doc IDs).
///
/// # Arguments
/// * `delta` - The compaction delta to apply
/// * `updater` - The updater to use for C structure updates (or a mock for testing)
pub fn apply_delta(delta: &CompactionDelta, updater: &impl IndexSpecUpdater) {
    debug!(
        term_count = delta.affected_term_count(),
        "Applying compaction delta"
    );

    // Update trie terms via FFI and track how many terms were completely emptied.
    // Term emptiness is detected when decrement_term_docs returns true, which
    // happens when the trie's numDocs for that term reaches 0 and the term is deleted.
    let mut num_terms_removed = 0u64;
    for (term_bytes, doc_count_decrement) in delta.term_deltas.iter() {
        debug!(
            term = ?term_bytes,
            doc_count_decrement = doc_count_decrement,
            "Calling decrement_term_docs FFI"
        );
        if updater.decrement_term_docs(&term_bytes, doc_count_decrement) {
            num_terms_removed += 1;
        }
    }

    // Decrement num_terms via FFI based on how many terms were deleted during trie updates
    if num_terms_removed > 0 {
        debug!(num_terms_removed, "Decrementing numTerms for deleted terms");
        updater.decrement_num_terms(num_terms_removed);
    }

    debug!("Compaction delta applied successfully");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    /// Mock implementation of IndexSpecUpdater for testing.
    struct MockIndexSpecUpdater {
        trie_updates: RefCell<Vec<(Vec<u8>, u64)>>,
        scoring_stats_updates: RefCell<Vec<u64>>,
        /// Terms that should be reported as "deleted" (emptied) when decremented.
        terms_to_delete: RefCell<std::collections::HashSet<Vec<u8>>>,
    }

    impl MockIndexSpecUpdater {
        fn new() -> Self {
            Self {
                trie_updates: RefCell::new(Vec::new()),
                scoring_stats_updates: RefCell::new(Vec::new()),
                terms_to_delete: RefCell::new(std::collections::HashSet::new()),
            }
        }

        /// Configure a term to be reported as deleted when `decrement_term_docs` is called.
        fn mark_term_as_will_be_deleted(&self, term: &[u8]) {
            self.terms_to_delete.borrow_mut().insert(term.to_vec());
        }
    }

    impl IndexSpecUpdater for MockIndexSpecUpdater {
        fn decrement_term_docs(&self, term: &[u8], doc_count_decrement: u64) -> bool {
            self.trie_updates
                .borrow_mut()
                .push((term.to_vec(), doc_count_decrement));
            // Return true if this term was marked as "will be deleted"
            self.terms_to_delete.borrow().contains(term)
        }

        fn decrement_num_terms(&self, num_terms_removed: u64) {
            self.scoring_stats_updates
                .borrow_mut()
                .push(num_terms_removed);
        }
    }

    #[test]
    fn test_apply_empty_delta() {
        let updater = MockIndexSpecUpdater::new();
        let delta = CompactionDelta::new();

        apply_delta(&delta, &updater);

        // Empty delta should not call any updater methods
        assert!(updater.trie_updates.borrow().is_empty());
        assert!(updater.scoring_stats_updates.borrow().is_empty());
    }

    #[test]
    fn test_apply_delta_with_term_updates() {
        let updater = MockIndexSpecUpdater::new();

        let mut delta = CompactionDelta::new();
        // Record 2 docs removed from "hello", 1 doc removed from "world"
        delta.term_deltas.increment(b"hello", 1);
        delta.term_deltas.increment(b"hello", 1);
        delta.term_deltas.increment(b"world", 1);

        apply_delta(&delta, &updater);

        // Check trie updates
        let updates = updater.trie_updates.borrow();
        assert_eq!(updates.len(), 2);
        assert!(updates.contains(&(b"hello".to_vec(), 2)));
        assert!(updates.contains(&(b"world".to_vec(), 1)));
    }

    /// Tests that `decrement_num_terms` is called with the correct count
    /// based on which terms report deletion via `decrement_term_docs`.
    #[test]
    fn test_apply_delta_tracks_deleted_terms_from_trie() {
        let updater = MockIndexSpecUpdater::new();

        // Mark "hello" and "world" as terms that will be deleted when decremented
        updater.mark_term_as_will_be_deleted(b"hello");
        updater.mark_term_as_will_be_deleted(b"world");
        // "foo" will NOT be deleted

        let mut delta = CompactionDelta::new();
        delta.term_deltas.increment(b"hello", 1);
        delta.term_deltas.increment(b"world", 1);
        delta.term_deltas.increment(b"foo", 1);

        apply_delta(&delta, &updater);

        // Check decrement_num_terms was called with 2 (hello and world deleted)
        let updates = updater.scoring_stats_updates.borrow();
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0], 2);

        // Check all 3 terms had their doc counts decremented
        let trie_updates = updater.trie_updates.borrow();
        assert_eq!(trie_updates.len(), 3);
    }
}
