//! RAII guard for C IndexSpec write lock during compaction.
//!
//! This module provides a safe RAII wrapper that acquires the IndexSpec write lock
//! on creation and releases it on drop, ensuring the lock is always released.

use std::ffi::c_char;

/// Trait for updating IndexSpec in-memory structures during compaction.
///
/// This trait abstracts the FFI calls to C IndexSpec, allowing for
/// mock implementations in tests.
pub trait IndexSpecUpdater {
    /// Updates a term's document count in the Serving Trie.
    ///
    /// # Arguments
    /// * `term` - The term as raw bytes (typically UTF-8)
    /// * `doc_count_decrement` - Number of documents to decrement
    ///
    /// # Returns
    /// `true` if the term was completely emptied and deleted from the trie.
    fn decrement_term_docs(&self, term: &[u8], doc_count_decrement: u64) -> bool;

    /// Decrements the IndexScoringStats numTerms counter.
    ///
    /// # Arguments
    /// * `num_terms_removed` - Number of terms that became empty during compaction
    fn decrement_num_terms(&self, num_terms_removed: u64);
}

/// A guard that holds the IndexSpec write lock and releases it on drop.
///
/// This guard implements the RAII pattern: the lock is acquired when the guard
/// is created and released when the guard is dropped, ensuring the lock is always
/// released even on early returns or panics.
///
/// # Safety
///
/// The pointer passed to [`IndexSpecLockGuard::new`] must be a valid `IndexSpec*`
/// that remains valid for the lifetime of this guard.
pub struct IndexSpecLockGuard {
    ptr: *mut ffi::IndexSpec,
}

impl IndexSpecLockGuard {
    /// Creates a new guard, acquiring the write lock.
    ///
    /// Returns `None` if the pointer is null.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid pointer to a C `IndexSpec` struct that remains
    /// valid for the lifetime of this guard.
    pub unsafe fn new(ptr: *mut ffi::IndexSpec) -> Option<Self> {
        if ptr.is_null() {
            return None;
        }
        // SAFETY: Caller guarantees the pointer is valid.
        unsafe { ffi::IndexSpec_AcquireWriteLock(ptr) };
        Some(Self { ptr })
    }
}

impl IndexSpecUpdater for IndexSpecLockGuard {
    fn decrement_term_docs(&self, term: &[u8], doc_count_decrement: u64) -> bool {
        // SAFETY: We hold the write lock (enforced by having &self),
        // and the C side handles non-null-terminated strings with length.
        unsafe {
            ffi::IndexSpec_DecrementTrieTermCount(
                self.ptr,
                term.as_ptr() as *const c_char,
                term.len(),
                doc_count_decrement as usize,
            )
        }
    }

    fn decrement_num_terms(&self, num_terms_removed: u64) {
        // SAFETY: We hold the write lock (enforced by having &self).
        unsafe {
            ffi::IndexSpec_DecrementNumTerms(self.ptr, num_terms_removed);
        }
    }
}

impl Drop for IndexSpecLockGuard {
    fn drop(&mut self) {
        // SAFETY: We acquired the lock in new(), so we must release it.
        unsafe { ffi::IndexSpec_ReleaseWriteLock(self.ptr) };
    }
}
