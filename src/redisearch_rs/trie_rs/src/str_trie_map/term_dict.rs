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
//! Wraps a [`StrTrieMap<TermEntry>`] with the per-term bookkeeping used by
//! the FT.SEARCH terms trie (`sp->terms`). The wrapper owns the `numDocs`
//! accounting and the "delete when the last doc disappears" policy.
//!
//! ## Case-folding contract
//!
//! All keys and patterns are case-folded on the way in via
//! `icu_casemap::CaseMapper::fold_string` before reaching the underlying
//! [`StrTrieMap`], so the trie itself only ever holds folded keys. Moving
//! the fold inside `TermDictionary` lets future C-to-Rust call sites stop
//! repeating the obligation.
//!
//! Iteration outputs are already folded by construction — the keys were
//! folded at insert — and are returned as-is.
//!
//! The underlying [`StrTrieMap`] stays byte-exact; case-folding is a
//! property of `TermDictionary` alone.
//!
//! ICU folding differs from C's `nu_tolower` on a few codepoints (e.g.
//! `ß` folds to `ss`, `ς` folds to `σ`), so the two paths can store
//! different keys for the same input.

use std::borrow::Cow;

use icu_casemap::CaseMapper;

use crate::str_trie_map::{
    StrTrieMap,
    iter::{ContainsIter, Iter, PrefixedIter, RangeIter, SuffixedIter, WildcardIter},
};

/// Per-term metadata stored at each terminal in the term dictionary.
///
/// Holds the subset of fields the FT.SEARCH terms trie actually reads:
/// score (constant `1.0` in current call sites) plus the number of
/// indexed documents containing this term.
pub struct TermEntry {
    pub score: f32,
    pub num_docs: usize,
}

/// Outcome of [`TermDictionary::decrement_num_docs`].
pub enum DecrResult {
    /// No terminal entry exists for the given term.
    NotFound,
    /// `num_docs` was decremented and is still `> 0`.
    Updated,
    /// `num_docs` reached `0`; the entry was removed.
    Deleted,
}

/// Outcome of [`TermDictionary::add_term`] / [`TermDictionary::replace_term`].
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
///
/// All terms and lookup patterns are case-folded internally via
/// [`icu_casemap::CaseMapper`] — see the module docs for the case-folding
/// contract.
pub struct TermDictionary {
    inner: StrTrieMap<TermEntry>,
}

/// Case-fold a term using Unicode default case folding. Borrows when
/// the input is already folded.
fn fold(term: &str) -> Cow<'_, str> {
    CaseMapper::new().fold_string(term)
}

impl TermDictionary {
    pub fn new() -> Self {
        Self {
            inner: StrTrieMap::new(),
        }
    }

    pub const fn len(&self) -> usize {
        self.inner.len()
    }

    pub const fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Estimated heap bytes held by the dictionary's internal trie.
    /// Backs `IndexSpec_TotalMemUsage`, which feeds `FT.INFO`'s per-spec
    /// terms-memory line. Counts trie node and key storage; the
    /// [`TermEntry`] payload is included only insofar as the underlying
    /// [`StrTrieMap`] already accounts for it.
    pub const fn mem_usage(&self) -> usize {
        self.inner.mem_usage()
    }

    /// Primitive overwrite — distinct from [`Self::replace_term`] in that
    /// it does NOT accumulate `num_docs`. Useful for bulk seeding and for
    /// re-installing a fully formed entry; production indexing paths
    /// should use [`Self::add_term`] / [`Self::replace_term`].
    pub fn insert(&mut self, term: &str, entry: TermEntry) -> Option<TermEntry> {
        self.inner.insert(&fold(term), entry)
    }

    /// ADD_INCR insert: accumulate both `score` and `num_docs` onto the
    /// existing entry, or create a fresh terminal if absent. `term` is
    /// case-folded before lookup.
    pub fn add_term(&mut self, term: &str, score: f32, num_docs: usize) -> InsertOutcome {
        let term = fold(term);
        if let Some(entry) = self.inner.get_mut(&term) {
            entry.score += score;
            entry.num_docs += num_docs;
            InsertOutcome::Updated
        } else {
            self.inner.insert(&term, TermEntry { score, num_docs });
            InsertOutcome::New
        }
    }

    /// ADD_REPLACE insert: overwrite `score`, but still accumulate
    /// `num_docs` onto the existing count. Creates a fresh terminal if
    /// absent. `term` is case-folded before lookup.
    pub fn replace_term(&mut self, term: &str, score: f32, num_docs: usize) -> InsertOutcome {
        let term = fold(term);
        if let Some(entry) = self.inner.get_mut(&term) {
            entry.score = score;
            entry.num_docs += num_docs;
            InsertOutcome::Updated
        } else {
            self.inner.insert(&term, TermEntry { score, num_docs });
            InsertOutcome::New
        }
    }

    pub fn remove(&mut self, term: &str) -> Option<TermEntry> {
        self.inner.remove(&fold(term))
    }

    pub fn get(&self, term: &str) -> Option<&TermEntry> {
        self.inner.get(&fold(term))
    }

    pub fn iter(&self) -> Iter<'_, TermEntry> {
        self.inner.iter()
    }

    /// Case-folds `prefix`; see [`StrTrieMap::prefixed_iter`].
    pub fn prefixed_iter(&self, prefix: &str) -> PrefixedIter<'_, TermEntry> {
        self.inner.prefixed_iter(&fold(prefix))
    }

    /// Case-folds `suffix`; see [`StrTrieMap::suffixed_iter`].
    pub fn suffixed_iter(&self, suffix: &str) -> SuffixedIter<'_, TermEntry> {
        self.inner.suffixed_iter(&fold(suffix))
    }

    /// Case-folds `target`; see [`StrTrieMap::contains_iter`]. When
    /// folding allocates, the iterator owns the folded buffer internally.
    pub fn contains_iter<'tm, 'p>(&'tm self, target: &'p str) -> ContainsIter<'tm, 'p, TermEntry> {
        ContainsIter::new_cow(self.inner.byte_trie(), fold(target))
    }

    /// Case-folds the bounds; see [`StrTrieMap::range_iter`]. `None` on
    /// either side disables that bound. When folding allocates, the
    /// iterator owns the folded buffers internally.
    pub fn range_iter<'tm, 'p>(
        &'tm self,
        min: Option<&'p str>,
        include_min: bool,
        max: Option<&'p str>,
        include_max: bool,
    ) -> RangeIter<'tm, 'p, TermEntry> {
        RangeIter::build_from_cow(
            self.inner.byte_trie(),
            min.map(fold),
            include_min,
            max.map(fold),
            include_max,
        )
    }

    /// Case-folds `pattern`; see [`StrTrieMap::wildcard_iter`]. `?` and
    /// `*` are ASCII so wildcard semantics survive folding. When folding
    /// allocates, the iterator owns the folded buffer internally.
    pub fn wildcard_iter<'tm, 'p>(&'tm self, pattern: &'p str) -> WildcardIter<'tm, 'p, TermEntry> {
        WildcardIter::new_cow(self.inner.byte_trie(), fold(pattern))
    }

    /// Case-folds `prefix` then runs [`StrTrieMap::iterate_dfa`]; see
    /// that method and [`crate::str_trie_map::dfa`] for the running-min distance
    /// and prefix-mode-freeze semantics.
    pub fn iterate_dfa<'tm>(
        &'tm self,
        prefix: &str,
        max_dist: u32,
        prefix_mode: bool,
    ) -> impl Iterator<Item = (String, &'tm TermEntry, u32)> + 'tm {
        self.inner.iterate_dfa(&fold(prefix), max_dist, prefix_mode)
    }

    /// Decrement the `num_docs` count for `term` by `delta`. Saturating
    /// subtract — when `num_docs` reaches zero the entry is removed.
    /// Returns [`DecrResult::NotFound`] if no terminal entry exists for
    /// `term`. `term` is case-folded before lookup.
    pub fn decrement_num_docs(&mut self, term: &str, delta: usize) -> DecrResult {
        let term = fold(term);
        let Some(entry) = self.inner.get_mut(&term) else {
            return DecrResult::NotFound;
        };
        if delta < entry.num_docs {
            entry.num_docs -= delta;
            DecrResult::Updated
        } else {
            self.inner.remove(&term);
            DecrResult::Deleted
        }
    }
}

impl Default for TermDictionary {
    fn default() -> Self {
        Self::new()
    }
}
