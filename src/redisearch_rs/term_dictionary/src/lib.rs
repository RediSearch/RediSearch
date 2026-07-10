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
//! All keys and patterns are case-folded on the way in before reaching the
//! underlying [`StrTrieMap`], so the trie itself only ever holds folded
//! keys. Moving the fold inside `TermDictionary` lets future C-to-Rust call
//! sites stop repeating the obligation.
//!
//! Folding lower-cases each [`char`] independently via [`char::to_lowercase`],
//! exactly matching RediSearch's C `unicode_tolower` (libnu-backed,
//! context-free per-codepoint). This is intentionally *not* Unicode default
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

use rdb_io::RdbIO;
use trie_rdb::{EntryFields, RdbError, RdbOpts, str as str_rdb};
use trie_rs::str_trie_map::{
    StrTrieMap,
    iter::{
        ContainsIter as StrContainsIter, FuzzyIter, Iter, PrefixedIter, RangeBoundary, RangeFilter,
        RangeIter as StrRangeIter, SuffixedIter, WildcardIter as StrWildcardIter,
    },
};

/// Wire options of the FT.SEARCH terms trie (`TrieType_GenericSave` with
/// `savePayloads = false, saveNumDocs = true`): per-entry score and
/// `num_docs`, no payloads.
const TERMS_RDB_OPTS: RdbOpts = RdbOpts {
    payloads: false,
    num_docs: true,
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
/// [`char::to_lowercase`] — see the module docs for the case-folding
/// contract.
pub struct TermDictionary {
    inner: StrTrieMap<TermEntry>,
}

impl Default for TermDictionary {
    fn default() -> Self {
        Self::new()
    }
}

impl TermDictionary {
    pub const fn new() -> Self {
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

    /// Estimated heap memory currently held by this index. Mirrors the cached
    /// counter on the underlying StrTrieMap — O(1). See [`StrTrieMap::mem_usage`].
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

    /// Case-folds `target`; see [`StrTrieMap::contains_iter`] and
    /// [`ContainsIter`] for the lazy-vs-drained behaviour when folding
    /// allocates.
    pub fn contains_iter<'tm, 'p>(&'tm self, target: &'p str) -> ContainsIter<'tm, 'p> {
        match fold(target) {
            Cow::Borrowed(s) => ContainsIter::Lazy(self.inner.contains_iter(s)),
            Cow::Owned(s) => {
                let drained: Vec<(String, &'tm TermEntry)> = self.inner.contains_iter(&s).collect();
                ContainsIter::Drained(drained.into_iter())
            }
        }
    }

    /// Case-folds `prefix`; see [`StrTrieMap::prefixed_iter`].
    pub fn prefixed_iter(&self, prefix: &str) -> PrefixedIter<'_, TermEntry> {
        self.inner.prefixed_iter(&fold(prefix))
    }

    /// Case-folds `suffix`; see [`StrTrieMap::suffixed_iter`].
    pub fn suffixed_iter(&self, suffix: &str) -> SuffixedIter<'_, TermEntry> {
        self.inner.suffixed_iter(&fold(suffix))
    }

    /// Case-folds the bounds; see [`StrTrieMap::range_iter`] and
    /// [`RangeIter`] for the lazy-vs-drained behaviour. `None` on either
    /// side disables that bound.
    pub fn range_iter<'tm, 'p>(
        &'tm self,
        min: Option<&'p str>,
        include_min: bool,
        max: Option<&'p str>,
        include_max: bool,
    ) -> RangeIter<'tm, 'p> {
        let min = min.map(fold);
        let max = max.map(fold);
        let min_owns = matches!(min, Some(Cow::Owned(_)));
        let max_owns = matches!(max, Some(Cow::Owned(_)));

        if !min_owns && !max_owns {
            // Both bounds absent or borrowed — stay lazy, carrying the
            // caller's `'p` lifetime rather than a borrow of the locals here.
            let min_ref: Option<&'p str> = match min {
                Some(Cow::Borrowed(s)) => Some(s),
                _ => None,
            };
            let max_ref: Option<&'p str> = match max {
                Some(Cow::Borrowed(s)) => Some(s),
                _ => None,
            };
            let filter = RangeFilter {
                min: min_ref.map(|value| RangeBoundary {
                    value,
                    is_included: include_min,
                }),
                max: max_ref.map(|value| RangeBoundary {
                    value,
                    is_included: include_max,
                }),
            };
            return RangeIter::Lazy(self.inner.range_iter(filter));
        }

        // A bound owns its folded buffer, which cannot outlive this call,
        // so drain the matches eagerly before the buffers drop.
        let min_buf: Option<String> = min.map(Cow::into_owned);
        let max_buf: Option<String> = max.map(Cow::into_owned);
        let filter = RangeFilter {
            min: min_buf.as_deref().map(|value| RangeBoundary {
                value,
                is_included: include_min,
            }),
            max: max_buf.as_deref().map(|value| RangeBoundary {
                value,
                is_included: include_max,
            }),
        };
        let drained: Vec<(String, &'tm TermEntry)> = self.inner.range_iter(filter).collect();
        RangeIter::Drained(drained.into_iter())
    }

    /// Case-folds `pattern`; see [`StrTrieMap::wildcard_iter`] for the
    /// codepoint matching model (`?` consumes one codepoint). `?` and `*`
    /// are ASCII so wildcard semantics survive folding. The returned
    /// iterator owns the parsed pattern, so it stays lazy regardless of
    /// whether folding allocated.
    pub fn wildcard_iter(&self, pattern: &str) -> StrWildcardIter<'_, TermEntry> {
        self.inner.wildcard_iter(&fold(pattern))
    }

    /// Case-folds `pattern`; see [`StrTrieMap::fuzzy_iter`] for the
    /// matching model — Levenshtein distance in codepoints under
    /// per-codepoint case folding. The returned iterator owns the folded
    /// needle, so it stays lazy regardless of whether folding allocated.
    pub fn fuzzy_iter(&self, pattern: &str, max_dist: u32) -> FuzzyIter<'_, TermEntry> {
        self.inner.fuzzy_iter(&fold(pattern), max_dist)
    }

    /// Serialize the dictionary to `writer` in the trie RDB wire format the
    /// FT.SEARCH terms trie has always used (per-entry score and `num_docs`,
    /// no payloads), so existing RDBs keep loading and new RDBs stay
    /// byte-compatible with readers that predate the Rust port.
    pub fn rdb_save<IO: RdbIO>(&self, writer: &mut IO) {
        str_rdb::save_with(&self.inner, writer, TERMS_RDB_OPTS, |entry| EntryFields {
            score: f64::from(entry.score),
            payload: None,
            num_docs: entry.num_docs as u64,
        });
    }

    /// Deserialize a dictionary from `reader`, accepting the wire format
    /// [`Self::rdb_save`] emits (which is also what `TrieType_GenericSave`
    /// wrote for the C terms trie).
    ///
    /// Keys must be valid UTF-8; a non-UTF-8 key aborts the load with
    /// [`RdbError::InvalidUtf8`]. Keys are re-folded on the way in: streams
    /// written by this module are folded already, but a legacy stream may
    /// carry codepoints whose old libnu fold differs from
    /// [`char::to_lowercase`], and the dictionary's folded-keys invariant
    /// must hold for lookups to find them.
    pub fn rdb_load<IO: RdbIO>(reader: &mut IO) -> Result<Self, RdbError> {
        let raw: StrTrieMap<TermEntry> =
            str_rdb::load_with(reader, TERMS_RDB_OPTS, |entry| TermEntry {
                score: entry.score as f32,
                num_docs: entry.num_docs as usize,
            })?;
        let mut dict = Self::new();
        for (key, entry) in raw.iter() {
            dict.insert(
                &key,
                TermEntry {
                    score: entry.score,
                    num_docs: entry.num_docs,
                },
            );
        }
        Ok(dict)
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

/// Iterator returned by [`TermDictionary::contains_iter`].
///
/// Case-folding the target may allocate. When it does (mixed-case input),
/// the folded buffer cannot outlive this iterator, so the matches are
/// drained eagerly into a `Vec` at construction ([`Self::Drained`]). When
/// folding borrows (already-folded input) the iterator stays lazy
/// ([`Self::Lazy`]) and streams directly from the trie. Both yield the same
/// items in the same order.
pub enum ContainsIter<'tm, 'p> {
    Lazy(StrContainsIter<'tm, 'p, TermEntry>),
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

/// Iterator returned by [`TermDictionary::range_iter`]. Drains eagerly when
/// either folded bound allocates; see [`ContainsIter`] for the rationale.
pub enum RangeIter<'tm, 'p> {
    Lazy(StrRangeIter<'tm, 'p, TermEntry>),
    Drained(std::vec::IntoIter<(String, &'tm TermEntry)>),
}

impl<'tm, 'p> Iterator for RangeIter<'tm, 'p> {
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
