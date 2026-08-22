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
//! underlying automaton applies the identical fold itself.) Moving the fold
//! inside `TermDictionary` lets future C-to-Rust call sites stop repeating
//! the obligation.
//!
//! Folding lower-cases each [`char`] independently via [`char::to_lowercase`],
//! matching RediSearch's C `unicode_tolower` (libnu-backed, context-free
//! per-codepoint); the equivalence is pinned by the differential tests in
//! `string_utils`. This is intentionally *not* Unicode default
//! case folding: terms enter the dictionary already lower-cased by the C
//! tokenizer, and re-folding must be byte-identical so the Rust and C paths
//! agree on the stored key. Default folding would diverge on codepoints like
//! `ß` (→ `ss`) or `ς` (→ `σ`), splitting a term across two keys.
//!
//! Iteration outputs are already folded by construction — the keys were
//! folded at insert — and are returned as-is.
//!
//! The underlying [`StrTrieMap`] stays byte-exact; case-folding is a
//! property of `TermDictionary` alone.

use std::borrow::Cow;

use trie_rs::str_trie_map::{
    StrTrieMap,
    iter::{
        ContainsIter as StrContainsIter, FuzzyIter, Iter, PrefixedIter, SuffixedIter,
        WildcardIter as StrWildcardIter,
    },
};

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
/// [`Self::add_term`], [`Self::replace_term`], or the primitive
/// [`Self::insert`]; each documents its own accumulation semantics.
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
    /// it does NOT accumulate [`TermEntry::num_docs`]. Useful for bulk seeding and for
    /// re-installing a fully formed entry; production indexing paths
    /// should use [`Self::add_term`] / [`Self::replace_term`].
    pub fn insert(&mut self, term: &str, entry: TermEntry) -> Option<TermEntry> {
        self.inner.insert(&fold(term), entry)
    }

    /// ADD_INCR insert: accumulate both [`TermEntry::score`] and
    /// [`TermEntry::num_docs`] onto the existing entry, or create a fresh
    /// terminal if absent.
    pub fn add_term(&mut self, term: &str, score: f32, num_docs: usize) -> InsertOutcome {
        let is_new = self.inner.insert_with(&fold(term), |prior| match prior {
            Some(mut entry) => {
                entry.score += score;
                entry.num_docs += num_docs;
                entry
            }
            None => TermEntry { score, num_docs },
        });
        if is_new {
            InsertOutcome::New
        } else {
            InsertOutcome::Updated
        }
    }

    /// ADD_REPLACE insert: overwrite [`TermEntry::score`], but still
    /// accumulate [`TermEntry::num_docs`] onto the existing count. Creates
    /// a fresh terminal if absent.
    pub fn replace_term(&mut self, term: &str, score: f32, num_docs: usize) -> InsertOutcome {
        let is_new = self.inner.insert_with(&fold(term), |prior| {
            let prior_num_docs = prior.map_or(0, |entry| entry.num_docs);
            TermEntry {
                score,
                num_docs: prior_num_docs + num_docs,
            }
        });
        if is_new {
            InsertOutcome::New
        } else {
            InsertOutcome::Updated
        }
    }

    /// Removes the entry for `term`, returning the previous [`TermEntry`].
    pub fn remove(&mut self, term: &str) -> Option<TermEntry> {
        self.inner.remove(&fold(term))
    }

    /// Returns the [`TermEntry`] stored for `term`, if any.
    pub fn get(&self, term: &str) -> Option<&TermEntry> {
        self.inner.get(&fold(term))
    }

    /// Iterate over all entries in lexicographical key order.
    /// See [`StrTrieMap::iter`].
    pub fn iter(&self) -> Iter<'_, TermEntry> {
        self.inner.iter()
    }

    /// Yield every entry whose key contains `target` as a substring. See
    /// [`ContainsIter`] for the lazy-vs-drained behaviour when folding
    /// allocates.
    pub fn contains_iter<'tm, 'p>(&'tm self, target: &'p str) -> ContainsIter<'tm, 'p> {
        match fold(target) {
            Cow::Borrowed(s) => ContainsIter::Lazy(Box::new(self.inner.contains_iter(s))),
            Cow::Owned(s) => {
                let drained: Vec<(String, &'tm TermEntry)> = self.inner.contains_iter(&s).collect();
                ContainsIter::Drained(drained.into_iter())
            }
        }
    }

    /// See [`StrTrieMap::prefixed_iter`].
    pub fn prefixed_iter(&self, prefix: &str) -> PrefixedIter<'_, TermEntry> {
        self.inner.prefixed_iter(&fold(prefix))
    }

    /// See [`StrTrieMap::suffixed_iter`].
    pub fn suffixed_iter(&self, suffix: &str) -> SuffixedIter<'_, TermEntry> {
        self.inner.suffixed_iter(&fold(suffix))
    }

    /// See [`StrTrieMap::wildcard_iter`] for the codepoint matching model.
    /// `?` and `*` are ASCII so wildcard semantics survive folding. The
    /// returned iterator owns the parsed pattern, so it stays lazy
    /// regardless of whether folding allocated.
    pub fn wildcard_iter(&self, pattern: &str) -> StrWildcardIter<'_, TermEntry> {
        self.inner.wildcard_iter(&fold(pattern))
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

/// Iterator returned by [`TermDictionary::contains_iter`].
///
/// Case-folding the target may allocate. When it does (mixed-case input),
/// the folded buffer cannot outlive this iterator, so the matches are
/// drained eagerly into a [`Vec`] at construction ([`Self::Drained`]). When
/// folding borrows (already-folded input) the iterator stays lazy
/// ([`Self::Lazy`]) and streams directly from the trie. Both yield the same
/// items in the same order.
pub enum ContainsIter<'tm, 'p> {
    /// Streams matches directly from the trie (folding borrowed).
    // Boxed: the streaming iterator's traversal stack dwarfs the drained
    // variant, and clippy::large_enum_variant rejects the imbalance.
    Lazy(Box<StrContainsIter<'tm, 'p, TermEntry>>),
    /// Matches collected at construction (folding allocated).
    Drained(std::vec::IntoIter<(String, &'tm TermEntry)>),
}

impl<'tm, 'p> Iterator for ContainsIter<'tm, 'p> {
    type Item = (String, &'tm TermEntry);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Lazy(it) => it.next(),
            Self::Drained(it) => it.next(),
        }
    }
}

/// Lower-case a term the way RediSearch's C tokenizer does: each [`char`]
/// independently via [`char::to_lowercase`], matching the libnu-backed C
/// `unicode_tolower`.
fn fold(term: &str) -> Cow<'_, str> {
    let unchanged = term.chars().all(|c| {
        let mut lower = c.to_lowercase();
        lower.next() == Some(c) && lower.next().is_none()
    });
    if unchanged {
        Cow::Borrowed(term)
    } else {
        Cow::Owned(term.chars().flat_map(char::to_lowercase).collect())
    }
}
