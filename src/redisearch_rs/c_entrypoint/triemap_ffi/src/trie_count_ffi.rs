/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI bindings for TrieCount - a trie-based counter for accumulating term deletion counts.
//!
//! This module provides C-callable functions for:
//! 1. Creating and managing a TrieCount
//! 2. Incrementing counts for terms
//! 3. Applying accumulated deltas to a C Trie via `Trie_DecrementNumDocs`

use std::ffi::{c_char, c_void};
use trie_rs::TrieCount;

/// Opaque type for TrieCount. Can be instantiated with [`NewTrieCount`].
pub struct TrieCountHandle(TrieCount);

/// Result of applying deltas to a C Trie.
#[repr(C)]
pub struct TrieCountApplyResult {
    /// Number of terms that were successfully updated (numDocs decremented but still > 0).
    pub terms_updated: u64,
    /// Number of terms that were deleted (numDocs reached 0).
    pub terms_deleted: u64,
    /// Number of terms that were not found in the C Trie.
    pub terms_not_found: u64,
}

/// Create a new [`TrieCount`]. Returns an opaque pointer to the newly created counter.
///
/// To free the counter, use [`TrieCount_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn NewTrieCount() -> *mut TrieCountHandle {
    let counter = Box::new(TrieCountHandle(TrieCount::new()));
    Box::into_raw(counter)
}

/// Free a [`TrieCount`] and all its contents.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `tc` must point to a valid TrieCount obtained from [`NewTrieCount`] and cannot be NULL.
/// - After calling this function, `tc` must not be used again.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieCount_Free(tc: *mut TrieCountHandle) {
    if tc.is_null() {
        return;
    }
    // Reconstruct the Box which will free the memory on drop
    let _ = unsafe { Box::from_raw(tc) };
}

/// Increment the count for a term in the TrieCount.
///
/// If the term doesn't exist, it's created with the given delta.
/// If it exists, the delta is added to the existing count.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `tc` must point to a valid TrieCount obtained from [`NewTrieCount`] and cannot be NULL.
/// - `term` can be NULL only if `term_len == 0`. It is not necessarily NULL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieCount_Increment(
    tc: *mut TrieCountHandle,
    term: *const c_char,
    term_len: usize,
    delta: u64,
) {
    debug_assert!(!tc.is_null(), "tc cannot be NULL");

    let TrieCountHandle(counter) = unsafe { &mut *tc };

    let key: &[u8] = if term_len > 0 {
        debug_assert!(!term.is_null(), "term cannot be NULL if term_len > 0");
        unsafe { std::slice::from_raw_parts(term.cast(), term_len) }
    } else {
        &[]
    };

    counter.increment(key, delta);
}

/// Get the count for a term in the TrieCount.
///
/// Returns the count if the term exists, or 0 if it doesn't.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `tc` must point to a valid TrieCount obtained from [`NewTrieCount`] and cannot be NULL.
/// - `term` can be NULL only if `term_len == 0`. It is not necessarily NULL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieCount_Get(
    tc: *const TrieCountHandle,
    term: *const c_char,
    term_len: usize,
) -> u64 {
    debug_assert!(!tc.is_null(), "tc cannot be NULL");

    let TrieCountHandle(counter) = unsafe { &*tc };

    let key: &[u8] = if term_len > 0 {
        debug_assert!(!term.is_null(), "term cannot be NULL if term_len > 0");
        unsafe { std::slice::from_raw_parts(term.cast(), term_len) }
    } else {
        &[]
    };

    counter.get(key).unwrap_or(0)
}

/// Get the number of unique terms in the TrieCount.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `tc` must point to a valid TrieCount obtained from [`NewTrieCount`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieCount_Len(tc: *const TrieCountHandle) -> usize {
    debug_assert!(!tc.is_null(), "tc cannot be NULL");

    let TrieCountHandle(counter) = unsafe { &*tc };
    counter.len()
}

/// Apply all accumulated deltas from the TrieCount to a C Trie.
///
/// This function iterates over all terms in the TrieCount (in lexicographic order)
/// and calls `Trie_DecrementNumDocs` for each term with its accumulated delta.
///
/// Returns a [`TrieCountApplyResult`] containing:
/// - `terms_updated`: Number of terms where numDocs was decremented but still > 0
/// - `terms_deleted`: Number of terms where numDocs reached 0 (term was deleted)
/// - `terms_not_found`: Number of terms that were not found in the C Trie
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `tc` must point to a valid TrieCount obtained from [`NewTrieCount`] and cannot be NULL.
/// - `c_trie` must point to a valid C `Trie` struct and cannot be NULL.
/// - The C Trie must remain valid for the duration of this function call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieCount_ApplyToCTrie(
    tc: *const TrieCountHandle,
    c_trie: *mut c_void,
) -> TrieCountApplyResult {
    debug_assert!(!tc.is_null(), "tc cannot be NULL");
    debug_assert!(!c_trie.is_null(), "c_trie cannot be NULL");

    let TrieCountHandle(counter) = unsafe { &*tc };

    let mut result = TrieCountApplyResult {
        terms_updated: 0,
        terms_deleted: 0,
        terms_not_found: 0,
    };

    // Cast c_void pointer to the FFI Trie type
    let c_trie_ptr = c_trie as *mut ffi::Trie;

    // Iterate over all terms in lexicographic order
    for (term, delta) in counter.iter() {
        // Call the C function to decrement numDocs
        let decr_result = unsafe {
            ffi::Trie_DecrementNumDocs(
                c_trie_ptr,
                term.as_ptr() as *const c_char,
                term.len(),
                delta as usize,
            )
        };

        // Match on the result enum values
        match decr_result {
            ffi::TrieDecrResult_TRIE_DECR_NOT_FOUND => result.terms_not_found += 1,
            ffi::TrieDecrResult_TRIE_DECR_UPDATED => result.terms_updated += 1,
            ffi::TrieDecrResult_TRIE_DECR_DELETED => result.terms_deleted += 1,
            _ => {} // Unknown result, ignore
        }
    }

    result
}
