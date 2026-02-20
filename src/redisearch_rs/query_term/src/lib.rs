/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A query term being evaluated at query time.
//!
//! This crate defines [`RSQueryTerm`], a `#[repr(C)]` struct that is shared
//! between C and Rust across the FFI boundary. The C-callable lifecycle
//! functions (`NewQueryTerm`, `Term_Free`) are provided by the `query_term_ffi`
//! crate.

use std::ffi::c_char;
use std::fmt;

/// Flags associated with query tokens and terms.
///
/// Extension-set token flags — up to 31 bits are available for extensions,
/// since 1 bit is reserved for the `expanded` flag on [`RSToken`].
///
/// [`RSToken`]: https://github.com/RediSearch/RediSearch
pub type RSTokenFlags = u32;

/// A single term being evaluated at query time.
///
/// Each term carries scoring metadata ([`idf`](RSQueryTerm::idf),
/// [`bm25_idf`](RSQueryTerm::bm25_idf)) and a unique
/// [`id`](RSQueryTerm::id) assigned during query parsing.
///
/// # Memory layout
///
/// All fields are private and accessed via type-safe methods and FFI functions.
/// C code accesses fields via FFI accessor functions, not direct struct access.
/// cbindgen:field-names=[str, len, idf, id, flags, bm25_idf]
pub struct RSQueryTerm {
    /// The term string, always NULL-terminated.
    str_: *mut c_char,
    /// The term length in bytes.
    ///
    /// It doesn't count the null terminator.
    len: usize,
    /// Inverse document frequency of the term in the index.
    ///
    /// See <https://en.wikipedia.org/wiki/Tf%E2%80%93idf>.
    idf: f64,
    /// Each term in the query gets an incremental id.
    id: i32,
    /// Flags given by the engine or by the query expander.
    flags: RSTokenFlags,
    /// Inverse document frequency for BM25 scoring.
    bm25_idf: f64,
}

impl fmt::Debug for RSQueryTerm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let term_str = if let Some(bytes) = self.as_bytes() {
            // SAFETY: term strings originate from user queries and are treated
            // as valid UTF-8 throughout the C codebase.
            unsafe { std::str::from_utf8_unchecked(bytes) }
        } else {
            "<null>"
        };

        f.debug_struct("RSQueryTerm")
            .field("str", &term_str)
            .field("idf", &self.idf)
            .field("id", &self.id)
            .field("flags", &self.flags)
            .field("bm25_idf", &self.bm25_idf)
            .finish()
    }
}

impl RSQueryTerm {
    /// Create a new [`RSQueryTerm`], copying `s` into a null-terminated,
    /// Rust-owned allocation (`Box<[u8]>`).
    ///
    /// The resulting term has `idf = 1.0` and `bm25_idf = 0.0`.
    pub fn new(s: &[u8], id: i32, flags: RSTokenFlags) -> Box<Self> {
        let mut buf = Vec::with_capacity(s.len() + 1);
        buf.extend_from_slice(s);
        buf.push(0); // null terminator
        let str_copy = Box::into_raw(buf.into_boxed_slice()) as *mut c_char;
        Box::new(Self {
            str_: str_copy,
            len: s.len(),
            idf: 1.0,
            id,
            flags,
            bm25_idf: 0.0,
        })
    }

    /// Create a new [`RSQueryTerm`] with a null string pointer.
    ///
    /// This is used when creating terms from tokens that have null string pointers.
    pub fn new_null_str(id: i32, flags: RSTokenFlags) -> Box<Self> {
        Box::new(Self {
            str_: std::ptr::null_mut(),
            len: 0,
            idf: 1.0,
            id,
            flags,
            bm25_idf: 0.0,
        })
    }

    /// Get the inverse document frequency (IDF) for TF-IDF scoring.
    pub const fn idf(&self) -> f64 {
        self.idf
    }

    /// Set the inverse document frequency (IDF) for TF-IDF scoring.
    pub const fn set_idf(&mut self, value: f64) {
        self.idf = value;
    }

    /// Get the BM25 IDF value for BM25 scoring.
    pub const fn bm25_idf(&self) -> f64 {
        self.bm25_idf
    }

    /// Set the BM25 IDF value for BM25 scoring.
    pub const fn set_bm25_idf(&mut self, value: f64) {
        self.bm25_idf = value;
    }

    /// Get the term ID.
    ///
    /// Each term in the query gets an incremental ID assigned during parsing.
    pub const fn id(&self) -> i32 {
        self.id
    }

    /// Get the term string length in bytes (excluding null terminator).
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Check if the term string is empty (null pointer or zero length).
    pub const fn is_empty(&self) -> bool {
        self.str_.is_null() || self.len == 0
    }

    /// Get the raw string pointer (for FFI compatibility).
    ///
    /// # Safety
    ///
    /// Returned pointer is valid for the lifetime of this term and is null-terminated.
    pub const fn str_ptr(&self) -> *const c_char {
        self.str_ as *const c_char
    }

    /// Get the term as a byte slice, if the string pointer is non-null.
    ///
    /// Does NOT assume valid UTF-8.
    pub const fn as_bytes(&self) -> Option<&[u8]> {
        if self.str_.is_null() {
            None
        } else {
            // SAFETY: `str_` is valid for `len` bytes when non-null
            Some(unsafe { std::slice::from_raw_parts(self.str_ as *const u8, self.len) })
        }
    }
}

impl Drop for RSQueryTerm {
    fn drop(&mut self) {
        if !self.str_.is_null() {
            let slice_ptr = std::ptr::slice_from_raw_parts_mut(self.str_ as *mut u8, self.len + 1);
            // SAFETY: `str_` was allocated via `Box::into_raw` on a
            // `Box<[u8]>` of length `self.len + 1` in `new`.
            let _ = unsafe { Box::from_raw(slice_ptr) };
        }
    }
}

impl PartialEq for RSQueryTerm {
    fn eq(&self, other: &Self) -> bool {
        // Compare string contents, not pointer addresses.
        let self_str = self.as_bytes().unwrap_or(&[]);
        let other_str = other.as_bytes().unwrap_or(&[]);

        self_str == other_str
            && self.idf == other.idf
            && self.bm25_idf == other.bm25_idf
            && self.id == other.id
            && self.flags == other.flags
    }
}

impl Eq for RSQueryTerm {}

// `RSQueryTerm` contains a raw pointer (`*mut c_char`) which makes it `!Send`
// and `!Sync` by default. The pointer is an owned allocation that is only ever
// accessed through `&self` / `&mut self`, so sending across threads is safe.
//
// SAFETY: The `str_` pointer is an owned, exclusive allocation (created via
// `Box<[u8]>` in `RSQueryTerm::new`). No aliasing occurs.
unsafe impl Send for RSQueryTerm {}
// SAFETY: Shared references (`&RSQueryTerm`) only read from `str_`. The
// pointed-to data is never mutated after construction.
unsafe impl Sync for RSQueryTerm {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_output() {
        let term = RSQueryTerm::new(b"hello", 1, 0);
        let debug = format!("{term:?}");
        assert!(debug.contains("hello"));
        assert!(debug.contains("RSQueryTerm"));
    }

    #[test]
    fn debug_null_str() {
        let term = RSQueryTerm::new_null_str(0, 0);
        let debug = format!("{term:?}");
        assert!(debug.contains("<null>"));
    }

    #[test]
    fn partial_eq_same_content() {
        let a = RSQueryTerm::new(b"hello", 1, 0);
        let b = RSQueryTerm::new(b"hello", 1, 0);
        // Different allocations, same content.
        assert_ne!(a.str_ptr(), b.str_ptr());
        assert_eq!(*a, *b);
    }

    #[test]
    fn partial_eq_different_content() {
        let a = RSQueryTerm::new(b"hello", 1, 0);
        let b = RSQueryTerm::new(b"world", 1, 0);
        assert_ne!(*a, *b);
    }

    #[test]
    fn partial_eq_different_id() {
        let a = RSQueryTerm::new(b"hello", 1, 0);
        let b = RSQueryTerm::new(b"hello", 2, 0);
        assert_ne!(*a, *b);
    }
}
