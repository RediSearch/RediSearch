/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Rust wrapper for the C Trie API.
//!
//! This crate provides a safe Rust interface to the C Trie implementation,

use std::ffi::c_char;

/// Result of a decrement operation on the C Trie.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::FromRepr)]
#[repr(u32)]
pub enum CTrieDecrResult {
    /// Term not found in the trie.
    NotFound = 0,
    /// numDocs decremented, term still has documents.
    Updated = 1,
    /// numDocs reached 0, term was deleted from the trie.
    Deleted = 2,
    /// Term too long/unconvertible for the trie; never inserted.
    Unsupported = 3,
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
    /// * `CTrieDecrResult::Unsupported` - term too long/unconvertible; never inserted
    ///
    /// # Safety
    ///
    /// This function is safe to call if the `CTrieRef` was created safely.
    /// The C function handles UTF-8 to rune conversion internally.
    pub fn decrement_num_docs(&mut self, term: &[u8], delta: u64) -> CTrieDecrResult {
        // SAFETY: We're calling the C function with valid parameters.
        // The term is passed as a UTF-8 byte slice, and the C function
        // handles the conversion to runes internally via runeBufFill.
        // The C function mutates the Trie by decrementing numDocs and
        // potentially deleting nodes.
        let result = unsafe {
            ffi::Trie_DecrementNumDocs(
                self.ptr,
                term.as_ptr() as *const c_char,
                term.len(),
                delta as usize,
            )
        };
        CTrieDecrResult::from_repr(result).unwrap_or(CTrieDecrResult::NotFound)
    }

    /// Number of documents indexed under `term`, or `0` if the term is absent
    /// from the trie.
    ///
    /// `term` is UTF-8 encoded; it is converted to runes internally and looked
    /// up as an exact match. Used to compute a term's inverse document
    /// frequency (IDF).
    ///
    /// Returns `0` for input that cannot correspond to a stored term — invalid
    /// UTF-8, or a term longer than the trie can hold — since such a term can
    /// never have been inserted.
    ///
    /// # Safety
    ///
    /// This function is safe to call if the `CTrieRef` was created safely.
    pub fn num_docs(&self, term: &[u8]) -> usize {
        // Terms longer than the trie can store are never present, so report zero
        // without a lookup (mirrors the C insertion/decrement guards). This also
        // bounds the rune count to `term.len()`, keeping it within `t_len` so the
        // narrowing cast below cannot wrap and match a shorter term by mistake.
        if term.len() > ffi::TRIE_INITIAL_STRING_LEN as usize * std::mem::size_of::<ffi::rune>() {
            return 0;
        }

        // The rune conversion decodes UTF-8 without bounds-checking multibyte
        // sequences against the slice end, so a truncated or invalid sequence
        // could read past `term`. Reject non-UTF-8 input up front; such a term
        // cannot match a stored (rune-decoded) term anyway.
        if std::str::from_utf8(term).is_err() {
            return 0;
        }

        // A UTF-8 string yields at most as many runes as bytes; the extra slot
        // leaves room for the conversion to write a trailing rune.
        let mut runes = vec![0 as ffi::rune; term.len() + 1];
        // SAFETY: `term` is valid UTF-8 of `term.len()` bytes, so the decode
        // stays within the slice, and `runes` has room for `term.len() + 1`
        // runes, so the conversion writes within bounds.
        let rlen = unsafe {
            ffi::strToRunesN(
                term.as_ptr() as *const c_char,
                term.len(),
                runes.as_mut_ptr(),
            )
        };
        // SAFETY: `self.ptr` is a valid `Trie` (`CTrieRef` invariant); `runes`/
        // `rlen` describe a valid rune slice, and `rlen <= term.len()` fits
        // `t_len` (guarded above).
        let node = unsafe {
            ffi::Trie_GetNode(
                self.ptr,
                runes.as_ptr(),
                rlen as ffi::t_len,
                true,
                std::ptr::null_mut(),
            )
        };

        if node.is_null() {
            0
        } else {
            // SAFETY: `node` is a valid, non-null `TrieNode` returned by the
            // lookup above.
            unsafe { ffi::TrieNode_NumDocs(node) }
        }
    }

    /// Get the raw pointer to the C Trie.
    ///
    /// This is useful when you need to pass the pointer to other C functions.
    pub const fn as_ptr(&self) -> *mut ffi::Trie {
        self.ptr
    }
}
