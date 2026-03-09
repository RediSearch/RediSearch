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
//! This crate defines [`RSQueryTerm`], an opaque struct shared
//! between C and Rust across the FFI boundary. The C-callable lifecycle
//! functions (`NewQueryTerm`, `Term_Free`) are provided by the `query_term_ffi`
//! crate.

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
#[derive(PartialEq)]
pub struct RSQueryTerm {
    /// The term string as raw bytes, or `None` if the token had a null string pointer.
    str_: Option<Box<[u8]>>,
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

impl RSQueryTerm {
    /// Create a new [`RSQueryTerm`] from a UTF-8 string slice, copying it into
    /// a Rust-owned allocation (`Box<[u8]>`).
    ///
    /// The resulting term has `idf = 1.0` and `bm25_idf = 0.0`.
    pub fn new(s: &str, id: i32, flags: RSTokenFlags) -> Box<Self> {
        Box::new(Self {
            str_: Some(s.as_bytes().into()),
            idf: 1.0,
            id,
            flags,
            bm25_idf: 0.0,
        })
    }

    /// Create a new [`RSQueryTerm`] from a raw byte slice, copying it into a
    /// Rust-owned allocation (`Box<[u8]>`).
    ///
    /// Bytes are stored as-is without any UTF-8 validation or conversion.
    /// This is intended for the FFI path, where the C tokenizer may produce
    /// byte sequences that are not valid UTF-8 (e.g. after case-folding
    /// applied to some Unicode codepoints).
    pub fn new_bytes(s: &[u8], id: i32, flags: RSTokenFlags) -> Box<Self> {
        Box::new(Self {
            str_: Some(s.into()),
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
            str_: None,
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

    /// Get the term string length in bytes.
    pub fn len(&self) -> usize {
        self.str_.as_deref().map_or(0, <[u8]>::len)
    }

    /// Check if the term string is empty (null or zero length).
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the term as a byte slice, if the string is non-null.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        self.str_.as_deref()
    }
}

// `f64` does not implement `Eq` (NaN != NaN), but IDF values in a query term
// are never NaN in practice, so the reflexivity requirement holds.
impl Eq for RSQueryTerm {}

impl fmt::Debug for RSQueryTerm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RSQueryTerm")
            .field("str", &self.str_.as_deref().map(String::from_utf8_lossy))
            .field("idf", &self.idf)
            .field("id", &self.id)
            .field("flags", &self.flags)
            .field("bm25_idf", &self.bm25_idf)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_output() {
        let term = RSQueryTerm::new("hello", 1, 0);
        let debug = format!("{term:?}");
        assert!(debug.contains("hello"));
        assert!(debug.contains("RSQueryTerm"));
    }

    #[test]
    fn debug_null_str() {
        let term = RSQueryTerm::new_null_str(0, 0);
        let debug = format!("{term:?}");
        assert!(debug.contains("None"));
    }

    #[test]
    fn partial_eq_same_content() {
        let a = RSQueryTerm::new("hello", 1, 0);
        let b = RSQueryTerm::new("hello", 1, 0);
        assert_eq!(*a, *b);
    }

    #[test]
    fn partial_eq_different_content() {
        let a = RSQueryTerm::new("hello", 1, 0);
        let b = RSQueryTerm::new("world", 1, 0);
        assert_ne!(*a, *b);
    }

    #[test]
    fn partial_eq_different_id() {
        let a = RSQueryTerm::new("hello", 1, 0);
        let b = RSQueryTerm::new("hello", 2, 0);
        assert_ne!(*a, *b);
    }

    #[test]
    fn new_bytes_accepts_non_utf8() {
        // 0xFF and 0xFE are not valid UTF-8; new_bytes must not panic and
        // must store the bytes as-is without any replacement.
        let term = RSQueryTerm::new_bytes(&[0xFF, 0xFE], 1, 0);
        assert!(!term.is_empty());
        assert_eq!(term.as_bytes(), Some(&[0xFF, 0xFEu8][..]));
    }
}
