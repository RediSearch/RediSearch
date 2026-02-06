/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Test-only FFI function for verifying TrieCount integration with C Trie.
//!
//! This module provides a single C-callable function that:
//! 1. Creates a TrieCount internally
//! 2. Populates it with predefined test data
//! 3. Applies the deltas to a C Trie
//! 4. Returns the results
//!
//! This is used by C++ tests to verify the Rust -> C integration works correctly.

use std::ffi::c_void;

use c_trie::{CTrieDecrResult, CTrieRef};
use trie_rs::TrieCount;

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

/// Test function that creates a TrieCount with predefined test data and applies it to a C Trie.
///
/// This function:
/// 1. Creates a TrieCount internally
/// 2. Adds the following test deltas:
///    - "hello": 30 (expects 100 -> 70)
///    - "world": 20 (expects 50 -> 30)
///    - "delete_me": 30 (expects 30 -> 0, deleted)
///    - "not_found": 10 (term doesn't exist in C Trie)
/// 3. Applies all deltas to the C Trie
/// 4. Returns the results
///
/// Expected results when called with a properly prepared C Trie:
/// - terms_updated: 2 ("hello" and "world")
/// - terms_deleted: 1 ("delete_me")
/// - terms_not_found: 1 ("not_found")
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `c_trie` must point to a valid C `Trie` struct and cannot be NULL.
/// - The C Trie should contain: "hello" (numDocs=100), "world" (numDocs=50),
///   "delete_me" (numDocs=30), "partial" (numDocs=75)
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieCount_TestApplyToCTrie(c_trie: *mut c_void) -> TrieCountApplyResult {
    debug_assert!(!c_trie.is_null(), "c_trie cannot be NULL");

    // Create TrieCount and populate with test data
    let mut counter = TrieCount::new();
    counter.increment(b"hello", 30); // 100 -> 70
    counter.increment(b"world", 20); // 50 -> 30
    counter.increment(b"delete_me", 30); // 30 -> 0 (deleted)
    counter.increment(b"not_found", 10); // doesn't exist

    let mut result = TrieCountApplyResult {
        terms_updated: 0,
        terms_deleted: 0,
        terms_not_found: 0,
    };

    // Create a safe wrapper around the C Trie pointer
    // Safety: The caller guarantees c_trie is a valid pointer to a C Trie
    let c_trie_ref = unsafe { CTrieRef::from_raw(c_trie as *mut ffi::Trie) };

    // Iterate over all terms in lexicographic order and apply to C Trie
    for (term, delta) in counter.iter() {
        match c_trie_ref.decrement_num_docs(&term, delta) {
            CTrieDecrResult::NotFound => result.terms_not_found += 1,
            CTrieDecrResult::Updated => result.terms_updated += 1,
            CTrieDecrResult::Deleted => result.terms_deleted += 1,
        }
    }

    result
}
