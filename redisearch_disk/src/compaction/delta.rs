//! Delta structures for tracking compaction changes.
//!
//! These structures capture the changes made during disk compaction that need
//! to be applied to in-memory structures (Serving Trie, DeletedIDs).
//!
//! # Important Note on Scoring Stats
//!
//! Document-level scoring stats (`num_docs`, `totalDocsLen`) are updated
//! at document delete time, NOT by GC/compaction. GC only updates:
//! - Term-level stats: `numDocs` per term in the trie
//! - Index-level: `numTerms` when terms become completely empty (detected via trie update)
//!
//! # Note on Term Emptiness
//!
//! Term emptiness is NOT tracked at the merge operator level because a single term
//! can span multiple postings blocks. Instead, when applying the delta to the trie,
//! if a term's doc count reaches 0, the trie returns `TRIE_DECR_DELETED`, and we
//! accumulate those to decrement `numTerms`.

use std::collections::HashSet;

use trie_rs::TrieCount;

/// Delta structure representing changes from a compaction run.
///
/// This structure is built by the compaction aggregator and contains all the
/// information needed to update in-memory structures after disk compaction completes.
#[derive(Clone, Default, Debug)]
pub struct CompactionDelta {
    /// Term -> document count to decrement.
    ///
    /// For each term affected by compaction, this tracks how many documents
    /// containing that term were removed. Values are always positive (counts).
    ///
    /// This is used to decrement the `numDocs` counter in the Serving Trie
    /// for each term.
    ///
    /// Uses a trie-based structure for memory efficiency when terms share
    /// common prefixes.
    pub term_deltas: TrieCount,

    /// Document IDs that were fully compacted from disk.
    ///
    /// These IDs should be removed from the DeletedIDs set since their
    /// data has been removed from disk and they no longer need to be tracked.
    pub compacted_doc_ids: HashSet<u64>,
}

impl CompactionDelta {
    /// Creates a new empty CompactionDelta.
    pub fn new() -> Self {
        Self::default()
    }

    /// Clears all collected data, resetting to empty state.
    pub fn clear(&mut self) {
        self.term_deltas.clear();
        self.compacted_doc_ids.clear();
    }

    /// Returns true if this delta contains no changes.
    ///
    /// An empty delta indicates that compaction completed but no documents
    /// were actually removed (e.g., no deleted IDs to process).
    pub fn is_empty(&self) -> bool {
        self.term_deltas.is_empty() && self.compacted_doc_ids.is_empty()
    }

    /// Returns the number of terms affected by this compaction.
    pub fn affected_term_count(&self) -> usize {
        self.term_deltas.n_unique_keys()
    }

    /// Returns the number of documents that were compacted.
    pub fn compacted_doc_count(&self) -> usize {
        self.compacted_doc_ids.len()
    }
}
