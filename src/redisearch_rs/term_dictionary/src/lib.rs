/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Term dictionary keyed by case-folded UTF-8 strings.
//!
//! [`TermDictionary`] wraps a [`StrTrieMap<TermEntry>`] with the per-term
//! bookkeeping used by the FT.SEARCH terms trie (`sp->terms`). The wrapper
//! owns the `numDocs` accounting and the "delete when the last doc
//! disappears" policy.
//!
//! ## Case-folding contract
//!
//! All keys and patterns are case-folded on the way in before reaching the
//! underlying [`StrTrieMap`], so the trie itself only ever holds folded
//! keys. (The one exception is [`TermDictionary::fuzzy_iter`], whose
//! underlying automaton applies the identical fold itself.) Callers never
//! fold themselves.
//!
//! Folding is [`unicode::tolower_cow`], which owns the equivalence to the
//! C fold. It is deliberately *not* Unicode default case folding: terms
//! reach the dictionary already lower-cased, so re-folding has to be
//! byte-identical or the same term lands under two keys — default folding
//! diverges on codepoints like `ß` (→ `ss`) and `ς` (→ `σ`).
//!
//! Iteration outputs are already folded by construction — the keys were
//! folded at insert — and are returned as-is.
//!
//! The underlying [`StrTrieMap`] stays byte-exact; case-folding is a
//! property of `TermDictionary` alone.
//!
//! ## Empty patterns
//!
//! [`TermDictionary::contains_iter`] and [`TermDictionary::suffixed_iter`]
//! yield nothing for an empty pattern, whereas the underlying [`StrTrieMap`]
//! yields every entry — the empty string is a substring and a suffix of every
//! key. The C walk this dictionary replaces (`TrieNode_IterateContains`)
//! yields nothing in both modes: its full-match test compares against the
//! pattern's last byte, which no position satisfies when the pattern is
//! empty. Matching that here keeps the two implementations
//! interchangeable, and matches the guard the existing C wrapper applies at
//! the same boundary (`TermsTrie::iterate_contains`).
//!
//! [`TermDictionary::prefixed_iter`] needs no such guard: C's prefix-only
//! mode also yields every term for an empty prefix, so the trie's own
//! semantics already agree.

use string_utils::unicode;
use trie_rs::str_trie_map::{
    StrTrieMap,
    iter::{
        ContainsIter as StrContainsIter, FuzzyIter, Iter, PrefixedIter, SuffixedIter,
        WildcardIter as StrWildcardIter,
    },
};

/// Lending iteration over a dictionary, implemented by every iterator the
/// `*_iter` methods return. Re-exported so a caller can name the protocol
/// without depending on [`trie_rs`] itself.
pub use trie_rs::str_trie_map::iter::LendingStrIter;

/// Per-term metadata stored at each terminal in the term dictionary.
///
/// Holds the subset of fields the FT.SEARCH terms trie actually reads.
#[derive(Debug, Clone, PartialEq)]
pub struct TermEntry {
    /// Sum of the per-document scores contributed for this term.
    pub score: f32,
    /// Number of indexed documents that contain this term. The entry is
    /// removed once this reaches zero — see
    /// [`TermDictionary::decrement_num_docs`].
    pub num_docs: usize,
}

/// Outcome of [`TermDictionary::decrement_num_docs`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecrResult {
    /// No terminal entry exists for the given term.
    NotFound,
    /// [`TermEntry::num_docs`] was decremented and is still `> 0`.
    Updated,
    /// [`TermEntry::num_docs`] reached `0`; the entry was removed.
    Deleted,
}

/// Outcome of [`TermDictionary::add_term`] / [`TermDictionary::replace_term`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    /// No prior entry existed; a new terminal was created.
    New,
    /// An existing entry was modified in place.
    Updated,
}

/// Term dictionary used by the FT.SEARCH index (`sp->terms`).
///
/// Maps each indexed term to its [`TermEntry`]. Inserts go through
/// [`Self::add_term`] or [`Self::replace_term`]; each documents its own
/// accumulation semantics.
///
/// All terms and lookup patterns are case-folded internally — see the
/// [module docs](self) for the case-folding contract.
pub struct TermDictionary {
    inner: StrTrieMap<TermEntry>,
}

impl Default for TermDictionary {
    fn default() -> Self {
        Self::new()
    }
}

impl TermDictionary {
    /// Create an empty dictionary.
    pub const fn new() -> Self {
        Self {
            inner: StrTrieMap::new(),
        }
    }

    /// The number of terms stored.
    pub const fn len(&self) -> usize {
        self.inner.len()
    }

    /// `true` when no terms are stored.
    pub const fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Estimated heap memory currently held by this dictionary.
    /// See [`StrTrieMap::mem_usage`] for what the cached counter covers.
    pub const fn mem_usage(&self) -> usize {
        self.inner.mem_usage()
    }

    /// Primitive overwrite — distinct from [`Self::replace_term`] in that
    /// it does NOT accumulate [`TermEntry::num_docs`]. Seeds a dictionary
    /// with fully formed entries; production indexing goes through
    /// [`Self::add_term`] / [`Self::replace_term`].
    #[cfg(any(test, feature = "test-utils"))]
    pub fn insert(&mut self, term: &str, entry: TermEntry) -> Option<TermEntry> {
        self.inner.insert(&unicode::tolower_cow(term), entry)
    }

    /// ADD_INCR insert: accumulate both [`TermEntry::score`] and
    /// [`TermEntry::num_docs`] onto the existing entry, or create a fresh
    /// terminal if absent.
    pub fn add_term(&mut self, term: &str, score: f32, num_docs: usize) -> InsertOutcome {
        if self
            .inner
            .insert_with(&unicode::tolower_cow(term), |prior| match prior {
                Some(mut entry) => {
                    entry.score += score;
                    entry.num_docs += num_docs;
                    entry
                }
                None => TermEntry { score, num_docs },
            })
        {
            InsertOutcome::New
        } else {
            InsertOutcome::Updated
        }
    }

    /// ADD_REPLACE insert: overwrite [`TermEntry::score`], but still
    /// accumulate [`TermEntry::num_docs`] onto the existing count. Creates
    /// a fresh terminal if absent.
    pub fn replace_term(&mut self, term: &str, score: f32, num_docs: usize) -> InsertOutcome {
        if self
            .inner
            .insert_with(&unicode::tolower_cow(term), |prior| {
                let prior_num_docs = prior.map_or(0, |entry| entry.num_docs);
                TermEntry {
                    score,
                    num_docs: prior_num_docs + num_docs,
                }
            })
        {
            InsertOutcome::New
        } else {
            InsertOutcome::Updated
        }
    }

    /// Removes the entry for `term`, returning the previous [`TermEntry`].
    pub fn remove(&mut self, term: &str) -> Option<TermEntry> {
        self.inner.remove(&unicode::tolower_cow(term))
    }

    /// Returns the [`TermEntry`] stored for `term`, if any.
    pub fn get(&self, term: &str) -> Option<&TermEntry> {
        self.inner.get(&unicode::tolower_cow(term))
    }

    /// Iterate over all entries in lexicographical key order.
    /// See [`StrTrieMap::iter`].
    pub fn iter(&self) -> Iter<'_, TermEntry> {
        self.inner.iter()
    }

    /// Yield every entry whose key contains the case-folded `target` as a
    /// substring. See [`StrTrieMap::contains_iter`]. The returned iterator
    /// owns the folded target, so it stays lazy whether or not folding
    /// allocated.
    ///
    /// An empty `target` yields nothing, unlike [`StrTrieMap::contains_iter`]
    /// — see the [module docs](self#empty-patterns).
    pub fn contains_iter(&self, target: &str) -> StrContainsIter<'_, 'static, TermEntry> {
        if target.is_empty() {
            return StrContainsIter::empty();
        }
        self.inner
            .contains_iter(&unicode::tolower_cow(target))
            .into_owned()
    }

    /// See [`StrTrieMap::prefixed_iter`].
    pub fn prefixed_iter(&self, prefix: &str) -> PrefixedIter<'_, TermEntry> {
        self.inner.prefixed_iter(&unicode::tolower_cow(prefix))
    }

    /// See [`StrTrieMap::suffixed_iter`].
    ///
    /// An empty `suffix` yields nothing, unlike [`StrTrieMap::suffixed_iter`]
    /// — see the [module docs](self#empty-patterns).
    pub fn suffixed_iter(&self, suffix: &str) -> SuffixedIter<'_, TermEntry> {
        if suffix.is_empty() {
            return SuffixedIter::empty();
        }
        self.inner.suffixed_iter(&unicode::tolower_cow(suffix))
    }

    /// See [`StrTrieMap::wildcard_iter`] for the codepoint matching model.
    /// `?` and `*` are ASCII so wildcard semantics survive folding. The
    /// returned iterator owns the parsed pattern, so it stays lazy
    /// regardless of whether folding allocated.
    pub fn wildcard_iter(&self, pattern: &str) -> StrWildcardIter<'_, TermEntry> {
        self.inner.wildcard_iter(&unicode::tolower_cow(pattern))
    }

    /// See [`StrTrieMap::fuzzy_iter`] for the matching model. The
    /// underlying automaton already folds the pattern (and each candidate
    /// key) with the same per-[`char`] lowering, so no fold is applied here.
    pub fn fuzzy_iter(&self, pattern: &str, max_dist: u32) -> FuzzyIter<'_, TermEntry> {
        self.inner.fuzzy_iter(pattern, max_dist)
    }

    /// Subtracts `delta` from [`TermEntry::num_docs`]; if `delta` meets or
    /// exceeds the count, the entry is removed and [`DecrResult::Deleted`]
    /// is returned. Returns [`DecrResult::NotFound`] if no terminal entry
    /// exists for `term`.
    pub fn decrement_num_docs(&mut self, term: &str, delta: usize) -> DecrResult {
        let term = unicode::tolower_cow(term);

        match self.inner.get_mut(&term) {
            Some(entry) => {
                if delta >= entry.num_docs {
                    self.inner.remove(&term);
                    DecrResult::Deleted
                } else {
                    entry.num_docs -= delta;
                    DecrResult::Updated
                }
            }
            None => DecrResult::NotFound,
        }
    }
}
