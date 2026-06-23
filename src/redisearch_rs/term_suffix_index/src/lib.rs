/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A string set supporting substring and ends-with queries against
//! its members.
//!
//! Powers RediSearch's `WITHSUFFIXTRIE` field option, where each
//! member is a word-level term harvested from an indexed text field
//! by the tokenizer. The structure itself is content-agnostic — any
//! `&str` will do.
//!
//! # Why a suffix trie answers substring queries
//!
//! Every substring of a string is a prefix of *some* suffix of that
//! string. So indexing all suffixes and then prefix-searching the
//! trie for the needle surfaces every member containing it.
//! [`TermSuffixIndex::iter_contains`] is the prefix lookup;
//! [`TermSuffixIndex::iter_suffix`] is the exact lookup.

mod term_refs;

use std::{
    fmt::{self, Debug},
    rc::Rc,
};

use term_refs::{Outcome, TermRefs};
use trie_rs::str_trie_map::StrTrieMap;

#[derive(Default)]
pub struct TermSuffixIndex {
    inner: StrTrieMap<TermRefs>,
}

impl Debug for TermSuffixIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl TermSuffixIndex {
    /// Create a new, empty index.
    pub const fn new() -> Self {
        Self {
            inner: StrTrieMap::new(),
        }
    }

    /// Estimated heap memory currently held by this index. Mirrors the cached
    /// counter on the underlying [`StrTrieMap`] — O(1). See [`StrTrieMap::mem_usage`].
    pub const fn mem_usage(&self) -> usize {
        self.inner.mem_usage()
    }

    pub fn add(&mut self, term: &str) {
        debug_assert!(!term.is_empty(), "term must not be empty");

        if self.inner.get(term).is_some_and(TermRefs::has_full_term) {
            return;
        }

        let owner = Rc::from(term);
        self.inner.insert_with(term, |existing| {
            TermRefs::upsert_full_term(existing, Rc::clone(&owner))
        });

        for suffix in Self::suffixes_of(term) {
            self.inner.insert_with(suffix, |existing| {
                TermRefs::upsert_longer_term(existing, Rc::clone(&owner))
            });
        }
    }

    pub fn remove(&mut self, term: &str) {
        debug_assert!(!term.is_empty(), "term must not be empty");

        if let Some(data) = self.inner.get_mut(term)
            && data.has_full_term()
            && data.clear_full_term() == Outcome::Drained
        {
            self.inner.remove(term);
        }

        for suffix in Self::suffixes_of(term) {
            if let Some(data) = self.inner.get_mut(suffix)
                && data.remove_longer_term(term) == Outcome::Drained
            {
                self.inner.remove(suffix);
            }
        }
    }

    fn suffixes_of(term: &str) -> impl Iterator<Item = &str> {
        term.char_indices()
            .skip(1)
            .map(|(byte_idx, _)| &term[byte_idx..])
    }

    /// Yield every indexed term containing `needle`, in unspecified
    /// order. Empty `needle` yields nothing. A term may be yielded
    /// more than once; dedupe by [`Rc::as_ptr`] if needed.
    pub fn iter_contains(&self, needle: &str) -> impl Iterator<Item = Rc<str>> {
        (!needle.is_empty())
            .then_some(needle)
            .into_iter()
            .flat_map(|n| self.inner.prefixed_iter(n))
            .flat_map(|(_key, data)| data.terms().cloned())
    }

    /// Yield every indexed term that ends with `needle`, in unspecified
    /// order. Empty `needle` yields nothing.
    ///
    /// Each matching term is yielded exactly once.
    pub fn iter_suffix(&self, needle: &str) -> impl Iterator<Item = Rc<str>> {
        (!needle.is_empty())
            .then_some(needle)
            .into_iter()
            .flat_map(|n| self.inner.get(n))
            .flat_map(|data| data.terms().cloned())
    }
}
