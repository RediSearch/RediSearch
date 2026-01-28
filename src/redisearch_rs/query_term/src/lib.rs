/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Query term representation for RediSearch.
//!
//! A query term represents a single term being evaluated during query time,
//! including its string representation, IDF scores, and flags.

use std::ptr::NonNull;

/// Flags associated with a token.
/// We support up to 30 user-given flags for each token; flags 1 and 2 are taken by the engine.
pub type TokenFlags = u32;

/// A token in the query.
///
/// The expanders receive query tokens and can expand the query with more query tokens.
///
/// Compared to C it does not pack expand and flags into 32 bits.
/// This is not considered relevant as it is not stored but only used to store query state.
#[derive(Debug, Clone)]
pub struct RSToken<'a> {
    /// The token string - which may or may not be NULL terminated.
    str: Option<&'a [u8]>,

    /// Is this token an expansion?
    pub expanded: bool,

    /// Extension set token flags
    pub flags: TokenFlags,
}

impl<'a> RSToken<'a> {
    /// Creates a new token from a byte slice.
    #[inline]
    pub const fn new(str: &'a [u8], expanded: bool, flags: TokenFlags) -> Self {
        Self {
            str: Some(str),
            expanded,
            flags,
        }
    }

    /// Creates a new token from a string slice.
    #[inline]
    pub const fn from_str(str: &'a str, expanded: bool, flags: TokenFlags) -> Self {
        Self::new(str.as_bytes(), expanded, flags)
    }

    /// Creates an empty token.
    #[inline]
    pub const fn empty() -> Self {
        Self {
            str: None,
            expanded: false,
            flags: 0,
        }
    }

    /// Returns the token string as a byte slice.
    #[inline]
    pub const fn as_bytes(&self) -> Option<&[u8]> {
        self.str
    }

    /// Returns the length of the token string.
    #[inline]
    pub const fn len(&self) -> usize {
        match self.str {
            Some(s) => s.len(),
            None => 0,
        }
    }

    /// Returns true if the token string is empty.
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A single term being evaluated in query time.
#[derive(Debug, Clone)]
pub struct RSQueryTerm {
    /// The term string, stored as owned bytes.
    str: Option<Vec<u8>>,

    /// Inverse document frequency of the term in the index.
    /// See <https://en.wikipedia.org/wiki/Tf%E2%80%93idf>
    pub idf: f64,

    /// Each term in the query gets an incremental id.
    pub id: i32,

    /// Flags given by the engine or by the query expander.
    pub flags: TokenFlags,

    /// Inverse document frequency of the term in the index for computing BM25.
    pub bm25_idf: f64,
}

impl RSQueryTerm {
    /// Creates a new query term from a token and an id.
    #[inline]
    pub fn new(tok: &RSToken<'_>, id: i32) -> Self {
        Self {
            str: tok.str.map(|s| s.to_vec()),
            idf: 1.0,
            id,
            flags: tok.flags,
            bm25_idf: 0.0,
        }
    }

    /// Returns the term string as a byte slice.
    #[inline]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        self.str.as_deref()
    }

    /// Returns a raw pointer to the term string, or null if empty.
    #[inline]
    pub fn as_ptr(&self) -> Option<NonNull<u8>> {
        self.str
            .as_ref()
            .map(|s| NonNull::new(s.as_ptr() as *mut u8).expect("boxed slice is never null"))
    }

    /// Returns the length of the term string.
    #[inline]
    pub fn len(&self) -> usize {
        self.str.as_ref().map_or(0, |s| s.len())
    }

    /// Returns true if the term string is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_creation() {
        let token = RSToken::from_str("hello", false, 0);
        assert_eq!(token.as_bytes(), Some(b"hello".as_slice()));
        assert_eq!(token.len(), 5);
        assert!(!token.is_empty());
        assert!(!token.expanded);
        assert_eq!(token.flags, 0);
    }

    #[test]
    fn test_empty_token() {
        let token = RSToken::empty();
        assert!(token.as_bytes().is_none());
        assert_eq!(token.len(), 0);
        assert!(token.is_empty());
    }

    #[test]
    fn test_query_term_creation() {
        let token = RSToken::from_str("world", true, 42);
        let term = RSQueryTerm::new(&token, 5);

        assert_eq!(term.as_bytes(), Some(b"world".as_slice()));
        assert_eq!(term.len(), 5);
        assert!(!term.is_empty());
        assert_eq!(term.id, 5);
        assert_eq!(term.flags, 42);
        assert!((term.idf - 1.0).abs() < f64::EPSILON);
        assert!((term.bm25_idf - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_query_term_from_empty_token() {
        let token = RSToken::empty();
        let term = RSQueryTerm::new(&token, 0);

        assert!(term.as_bytes().is_none());
        assert_eq!(term.len(), 0);
        assert!(term.is_empty());
    }
}
