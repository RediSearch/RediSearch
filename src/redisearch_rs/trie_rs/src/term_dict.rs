/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Term dictionary keyed by UTF-8 strings.
//!
//! Wraps a [`StrTrieMap<TermEntry>`] with the per-term bookkeeping used by
//! the FT.SEARCH terms trie (`sp->terms` in `src/spec.c`). The wrapper owns
//! the `numDocs` accounting and the "delete when the last doc disappears"
//! policy that lives in `Trie_DecrementNumDocs` (`src/trie/trie.c`).
//!
//! The trie itself stays value-agnostic; anything score/`numDocs`-shaped is
//! confined to this module so the underlying [`StrTrieMap`] remains a pure
//! data structure.

use crate::str::{StrTrieMap, iter::Iter};

/// Per-term metadata stored at each terminal in the term dictionary.
///
/// Mirrors the subset of `TrieNode` fields that `sp->terms` actually reads:
/// score (constant `1.0` in current C call sites) plus the number of
/// indexed documents containing this term. The C `payload` and
/// `maxChildScore` fields are unused by the terms trie and intentionally
/// absent here.
pub struct TermEntry {
    pub score: f32,
    pub num_docs: usize,
}

/// Outcome of [`TermDictionary::decrement_num_docs`].
///
/// Mirrors `TrieDecrResult` in `src/trie/trie.h`.
pub enum DecrResult {
    /// No terminal entry exists for the given term.
    NotFound,
    /// `num_docs` was decremented and is still `> 0`.
    Updated,
    /// `num_docs` reached `0`; the entry was removed.
    Deleted,
}

/// Term dictionary used by the FT.SEARCH index (`sp->terms`).
///
/// Maps each indexed term to its [`TermEntry`]. Insertion overwrites the
/// existing entry (ADD_REPLACE semantics); the ADD_INCR accumulation used
/// by `spec.c:1971` will land as a separate method once the relevant call
/// site is ported.
pub struct TermDictionary {
    inner: StrTrieMap<TermEntry>,
}

impl TermDictionary {
    pub fn new() -> Self {
        Self {
            inner: StrTrieMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.len() == 0
    }

    /// Insert or overwrite the entry for `term`.
    ///
    /// Returns the previous entry if one existed. Equivalent to the C
    /// `Trie_InsertStringBuffer(..., incr=0, ADD_REPLACE)` path.
    pub fn insert(&mut self, term: &str, entry: TermEntry) -> Option<TermEntry> {
        self.inner.insert(term, entry)
    }

    /// Remove the entry for `term`, returning it if present.
    pub fn remove(&mut self, term: &str) -> Option<TermEntry> {
        self.inner.remove(term)
    }

    pub fn get(&self, term: &str) -> Option<&TermEntry> {
        self.inner.get(term)
    }

    pub fn iter(&self) -> Iter<'_, TermEntry> {
        self.inner.iter()
    }

    /// Decrement the `num_docs` count for `term` by `delta`.
    ///
    /// Mirrors `Trie_DecrementNumDocs` in `src/trie/trie.c`: saturating
    /// subtract, and remove the entry when `num_docs` reaches zero.
    /// Returns [`DecrResult::NotFound`] if no terminal entry exists for
    /// `term`.
    //
    // TODO: use a `find_mut` on `StrTrieMap` once available, to avoid the
    // remove + reinsert round-trip on the still-alive path.
    pub fn decrement_num_docs(&mut self, term: &str, delta: usize) -> DecrResult {
        match self.inner.remove(term) {
            None => DecrResult::NotFound,
            Some(mut entry) if delta < entry.num_docs => {
                entry.num_docs -= delta;
                self.inner.insert(term, entry);
                DecrResult::Updated
            }
            Some(_) => DecrResult::Deleted,
        }
    }
}

impl Default for TermDictionary {
    fn default() -> Self {
        Self::new()
    }
}
