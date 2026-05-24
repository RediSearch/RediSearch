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

use crate::str::{
    StrTrieMap,
    iter::{ContainsIter, Iter, PrefixedIter, RangeIter, SuffixedIter, WildcardIter},
};

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

/// Outcome of [`TermDictionary::add_term`] / [`TermDictionary::replace_term`].
///
/// Mirrors the `TRIE_OK_NEW` / `TRIE_OK_UPDATED` return codes from
/// `src/trie/trie_node.h`.
pub enum InsertOutcome {
    /// No prior entry existed; a new terminal was created.
    New,
    /// An existing entry was modified in place.
    Updated,
}

/// Term dictionary used by the FT.SEARCH index (`sp->terms`).
///
/// Maps each indexed term to its [`TermEntry`]. Two production insert
/// modes are exposed: [`Self::add_term`] (ADD_INCR — accumulate `score`
/// and `num_docs`) and [`Self::replace_term`] (ADD_REPLACE — overwrite
/// `score`, still accumulate `num_docs`). The primitive [`Self::insert`]
/// stays available for bulk-seeding scenarios where neither accumulation
/// mode applies.
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

    /// Insert or overwrite the entry for `term`, returning the previous
    /// entry if one existed.
    ///
    /// Primitive overwrite — distinct from [`Self::replace_term`] in that
    /// it does NOT accumulate `num_docs`. Useful for bulk seeding and for
    /// re-installing a fully formed entry; production indexing paths
    /// should use [`Self::add_term`] / [`Self::replace_term`].
    pub fn insert(&mut self, term: &str, entry: TermEntry) -> Option<TermEntry> {
        self.inner.insert(term, entry)
    }

    /// ADD_INCR insert: accumulate both `score` and `num_docs` onto the
    /// existing entry, or create a fresh terminal if absent.
    ///
    /// Mirrors `Trie_InsertStringBuffer(..., incr=1)` in
    /// `src/trie/trie.c`, whose `__trieNode_Add` ADD_INCR branch runs
    /// `n->score += score; n->numDocs += numDocs`
    /// (`src/trie/trie_node.c:296`).
    //
    // TODO: use a `find_mut` on `StrTrieMap` once available, to avoid the
    // remove + reinsert round-trip on the still-alive path.
    pub fn add_term(&mut self, term: &str, score: f32, num_docs: usize) -> InsertOutcome {
        match self.inner.remove(term) {
            Some(prev) => {
                self.inner.insert(
                    term,
                    TermEntry {
                        score: prev.score + score,
                        num_docs: prev.num_docs + num_docs,
                    },
                );
                InsertOutcome::Updated
            }
            None => {
                self.inner.insert(term, TermEntry { score, num_docs });
                InsertOutcome::New
            }
        }
    }

    /// ADD_REPLACE insert: overwrite `score`, but still accumulate
    /// `num_docs` onto the existing count. Creates a fresh terminal if
    /// absent.
    ///
    /// Mirrors `Trie_InsertStringBuffer(..., incr=0)`: the C
    /// `__trieNode_Add` overwrites `n->score = score` but does
    /// `n->numDocs += numDocs` regardless of mode
    /// (`src/trie/trie_node.c:296`). The mode only gates the score path.
    //
    // TODO: use a `find_mut` on `StrTrieMap` once available, to avoid the
    // remove + reinsert round-trip on the still-alive path.
    pub fn replace_term(&mut self, term: &str, score: f32, num_docs: usize) -> InsertOutcome {
        match self.inner.remove(term) {
            Some(prev) => {
                self.inner.insert(
                    term,
                    TermEntry {
                        score,
                        num_docs: prev.num_docs + num_docs,
                    },
                );
                InsertOutcome::Updated
            }
            None => {
                self.inner.insert(term, TermEntry { score, num_docs });
                InsertOutcome::New
            }
        }
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

    /// Yield every term whose key starts with `prefix`.
    ///
    /// Proxies [`StrTrieMap::prefixed_iter`]; see that method for the
    /// underlying C contract (`Trie_IterateContains`, prefix branch).
    pub fn prefixed_iter<'tm>(&'tm self, prefix: &str) -> PrefixedIter<'tm, TermEntry> {
        self.inner.prefixed_iter(prefix)
    }

    /// Yield every term whose key ends with `suffix`.
    ///
    /// Proxies [`StrTrieMap::suffixed_iter`].
    pub fn suffixed_iter<'tm>(&'tm self, suffix: &str) -> SuffixedIter<'tm, TermEntry> {
        self.inner.suffixed_iter(suffix)
    }

    /// Yield every term whose key contains `target` as a substring.
    ///
    /// Proxies [`StrTrieMap::contains_iter`].
    pub fn contains_iter<'tm, 'p>(
        &'tm self,
        target: &'p str,
    ) -> ContainsIter<'tm, 'p, TermEntry> {
        self.inner.contains_iter(target)
    }

    /// Yield every term whose key falls within the lex range
    /// `[min, max]` (inclusivity controlled per-end).
    ///
    /// Proxies [`StrTrieMap::range_iter`]; `None` on either side disables
    /// that bound (matches the C `(NULL, -1)` sentinel for
    /// `Trie_IterateRange`).
    pub fn range_iter<'tm, 'p>(
        &'tm self,
        min: Option<&'p str>,
        include_min: bool,
        max: Option<&'p str>,
        include_max: bool,
    ) -> RangeIter<'tm, 'p, TermEntry> {
        self.inner.range_iter(min, include_min, max, include_max)
    }

    /// Yield every term matching the wildcard `pattern` (`?` = one byte,
    /// `*` = zero or more bytes).
    ///
    /// Proxies [`StrTrieMap::wildcard_iter`]; see that method for the
    /// byte-vs-codepoint caveat on non-ASCII patterns.
    pub fn wildcard_iter<'tm, 'p>(
        &'tm self,
        pattern: &'p str,
    ) -> WildcardIter<'tm, 'p, TermEntry> {
        self.inner.wildcard_iter(pattern)
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
