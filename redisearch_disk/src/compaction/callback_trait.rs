//! Callback traits for merge operations during compaction.
//!
//! This module defines the `MergeCallback` trait used by the merge operator
//! to notify callers about document removals during compaction.

/// Callback trait for merge operations.
///
/// This trait allows the merge operator to notify callers about document removals
/// during compaction. Implementations can choose to record these events (for delta
/// tracking) or ignore them (no-op).
///
/// # Note on Term Emptiness
///
/// Term emptiness is NOT tracked at the merge operator level because a single term
/// can span multiple postings blocks. The merge operator only sees individual blocks,
/// so it cannot determine if a term is completely empty. Instead, term deletion is
/// detected when applying the compaction delta to the trie - if a term's doc count
/// reaches 0, the trie returns `TRIE_DECR_DELETED`.
pub trait MergeCallbacks: Clone + Send + Sync + 'static {
    /// Called when a document is removed from a term during merge.
    ///
    /// # Arguments
    /// * `term` - The term from which the document was removed
    /// * `doc_id` - The ID of the removed document
    fn on_doc_removed(&self, term: &str, doc_id: u64);
}

/// No-op callback implementation for cases where delta tracking is not needed.
///
/// This is used by default and for index types (like tags) that don't need
/// compaction delta collection.
#[derive(Clone, Copy, Default)]
pub struct NoOpCallbacks;

impl MergeCallbacks for NoOpCallbacks {
    #[inline(always)]
    fn on_doc_removed(&self, _term: &str, _doc_id: u64) {
        // No-op: intentionally empty
    }
}
