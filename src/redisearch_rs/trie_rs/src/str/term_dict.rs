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
//! the FT.SEARCH terms trie (`sp->terms` in `src/spec.c`). The wrapper owns
//! the `numDocs` accounting and the "delete when the last doc disappears"
//! policy that lives in `Trie_DecrementNumDocs` (`src/trie/trie.c`).
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
    ///
    /// Backs `IndexSpec_TotalMemUsage` (`src/spec.c`) which feeds
    /// `FT.INFO`'s per-spec terms-memory line. Counts trie node and key
    /// storage; the [`TermEntry`] payload is included only insofar as the
    /// underlying [`StrTrieMap`] already accounts for it.
    pub const fn mem_usage(&self) -> usize {
        self.inner.mem_usage()
    }

    /// Insert or overwrite the entry for `term`, returning the previous
    /// entry if one existed. `term` is case-folded before insertion.
    ///
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
    ///
    /// Mirrors `Trie_InsertStringBuffer(..., incr=1)` in
    /// `src/trie/trie.c`, whose `__trieNode_Add` ADD_INCR branch runs
    /// `n->score += score; n->numDocs += numDocs`
    /// (`src/trie/trie_node.c:296`).
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
    ///
    /// Mirrors `Trie_InsertStringBuffer(..., incr=0)`: the C
    /// `__trieNode_Add` overwrites `n->score = score` but does
    /// `n->numDocs += numDocs` regardless of mode
    /// (`src/trie/trie_node.c:296`). The mode only gates the score path.
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

    /// Remove the entry for `term`, returning it if present. `term` is
    /// case-folded before lookup.
    pub fn remove(&mut self, term: &str) -> Option<TermEntry> {
        self.inner.remove(&fold(term))
    }

    /// Look up an entry by `term`. `term` is case-folded before lookup.
    pub fn get(&self, term: &str) -> Option<&TermEntry> {
        self.inner.get(&fold(term))
    }

    pub fn iter(&self) -> Iter<'_, TermEntry> {
        self.inner.iter()
    }

    /// Yield every term whose key starts with `prefix`. `prefix` is
    /// case-folded before traversal.
    ///
    /// Proxies [`StrTrieMap::prefixed_iter`]; see that method for the
    /// underlying C contract (`Trie_IterateContains`, prefix branch).
    pub fn prefixed_iter(&self, prefix: &str) -> PrefixedIter<'_, TermEntry> {
        self.inner.prefixed_iter(&fold(prefix))
    }

    /// Yield every term whose key ends with `suffix`. `suffix` is
    /// case-folded before traversal.
    ///
    /// Proxies [`StrTrieMap::suffixed_iter`].
    pub fn suffixed_iter(&self, suffix: &str) -> SuffixedIter<'_, TermEntry> {
        self.inner.suffixed_iter(&fold(suffix))
    }

    /// Yield every term whose key contains `target` as a substring.
    /// `target` is case-folded before traversal; when folding allocates,
    /// the iterator owns the folded buffer internally.
    ///
    /// Proxies [`StrTrieMap::contains_iter`].
    pub fn contains_iter<'tm, 'p>(&'tm self, target: &'p str) -> ContainsIter<'tm, 'p, TermEntry> {
        ContainsIter::new_cow(self.inner.byte_trie(), fold(target))
    }

    /// Yield every term whose key falls within the lex range
    /// `[min, max]` (inclusivity controlled per-end). Bounds are
    /// case-folded before traversal; when folding allocates, the iterator
    /// owns the folded buffers internally.
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
        RangeIter::build_from_cow(
            self.inner.byte_trie(),
            min.map(fold),
            include_min,
            max.map(fold),
            include_max,
        )
    }

    /// Yield every term matching the wildcard `pattern` (`?` = one byte,
    /// `*` = zero or more bytes). `pattern` is case-folded before
    /// traversal; `?` and `*` are ASCII so the wildcard semantics are
    /// preserved. When folding allocates, the iterator owns the folded
    /// buffer internally.
    ///
    /// Proxies [`StrTrieMap::wildcard_iter`]; see that method for the
    /// byte-vs-codepoint caveat on non-ASCII patterns.
    pub fn wildcard_iter<'tm, 'p>(&'tm self, pattern: &'p str) -> WildcardIter<'tm, 'p, TermEntry> {
        WildcardIter::new_cow(self.inner.byte_trie(), fold(pattern))
    }

    /// Yield every term whose key lies within Levenshtein edit distance
    /// `max_dist` of `prefix`, optionally followed by any suffix when
    /// `prefix_mode` is true. `prefix` is case-folded before the DFA is
    /// built.
    ///
    /// Proxies [`StrTrieMap::iterate_dfa`]; see that method (and the
    /// `crate::str::dfa` module doc) for the running-min distance and
    /// prefix-mode-freeze semantics that the underlying DFA pins.
    pub fn iterate_dfa<'tm>(
        &'tm self,
        prefix: &str,
        max_dist: u32,
        prefix_mode: bool,
    ) -> impl Iterator<Item = (String, &'tm TermEntry, u32)> + 'tm {
        self.inner.iterate_dfa(&fold(prefix), max_dist, prefix_mode)
    }

    /// Decrement the `num_docs` count for `term` by `delta`. `term` is
    /// case-folded before lookup.
    ///
    /// Mirrors `Trie_DecrementNumDocs` in `src/trie/trie.c`: saturating
    /// subtract, and remove the entry when `num_docs` reaches zero.
    /// Returns [`DecrResult::NotFound`] if no terminal entry exists for
    /// `term`.
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
