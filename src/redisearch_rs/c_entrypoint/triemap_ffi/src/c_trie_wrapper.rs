/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Rust wrapper for the C Trie's `Trie_DecrementNumDocs` function.
//!
//! This module provides a safe Rust interface to decrement the `numDocs` count
//! for terms in a C Trie.

use std::ffi::c_char;

/// Result of a decrement operation on the C Trie.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum CTrieDecrResult {
    /// Term not found in the trie.
    NotFound = 0,
    /// numDocs decremented, term still has documents.
    Updated = 1,
    /// numDocs reached 0, term was deleted from the trie.
    Deleted = 2,
}

impl From<u32> for CTrieDecrResult {
    fn from(val: u32) -> Self {
        match val {
            0 => CTrieDecrResult::NotFound,
            1 => CTrieDecrResult::Updated,
            2 => CTrieDecrResult::Deleted,
            _ => CTrieDecrResult::NotFound, // Fallback for unexpected values
        }
    }
}

/// Wrapper around the C Trie pointer for safe FFI operations.
///
/// This struct does NOT own the C Trie - it's just a wrapper for
/// calling FFI functions. The caller is responsible for managing
/// the lifetime of the C Trie.
#[derive(Debug)]
pub struct CTrieRef {
    ptr: *mut ffi::Trie,
}

impl CTrieRef {
    /// Create a new wrapper around an existing C Trie pointer.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - `ptr` is a valid pointer to a C `Trie` struct
    /// - The C Trie outlives this wrapper
    /// - No other code frees the C Trie while this wrapper exists
    pub unsafe fn from_raw(ptr: *mut ffi::Trie) -> Self {
        debug_assert!(!ptr.is_null(), "C Trie pointer cannot be null");
        Self { ptr }
    }

    /// Decrement the numDocs count for a term in the C Trie.
    ///
    /// # Arguments
    ///
    /// * `term` - The UTF-8 encoded term bytes
    /// * `delta` - The amount to decrement numDocs by
    ///
    /// # Returns
    ///
    /// * `CTrieDecrResult::NotFound` - Term not found in trie
    /// * `CTrieDecrResult::Updated` - numDocs decremented, still > 0
    /// * `CTrieDecrResult::Deleted` - numDocs reached 0, term deleted
    ///
    /// # Safety
    ///
    /// This function is safe to call if the `CTrieRef` was created safely.
    /// The C function handles UTF-8 to rune conversion internally.
    pub fn decrement_num_docs(&self, term: &[u8], delta: u64) -> CTrieDecrResult {
        // Safety: We're calling the C function with valid parameters.
        // The term is passed as a UTF-8 byte slice, and the C function
        // handles the conversion to runes internally via runeBufFill.
        let result = unsafe {
            ffi::Trie_DecrementNumDocs(
                self.ptr,
                term.as_ptr() as *const c_char,
                term.len(),
                delta as usize,
            )
        };
        CTrieDecrResult::from(result)
    }

    /// Get the raw pointer to the C Trie.
    ///
    /// This is useful when you need to pass the pointer to other C functions.
    pub fn as_ptr(&self) -> *mut ffi::Trie {
        self.ptr
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_c_trie_decr_result_from() {
        assert_eq!(CTrieDecrResult::from(0), CTrieDecrResult::NotFound);
        assert_eq!(CTrieDecrResult::from(1), CTrieDecrResult::Updated);
        assert_eq!(CTrieDecrResult::from(2), CTrieDecrResult::Deleted);
        assert_eq!(CTrieDecrResult::from(99), CTrieDecrResult::NotFound); // Unknown
    }
}
