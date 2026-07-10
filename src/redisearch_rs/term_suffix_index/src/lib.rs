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
//! # Case-insensitivity
//!
//! Every term and query is lowercased on the way in via
//! [`unicode_tolower_cow`], so stored terms and matches are always in
//! lowercase form and lookups are case-insensitive. Callers pass terms
//! verbatim; the index owns normalization.
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
    sync::Arc,
};

use string_utils::unicode_tolower_cow;
use term_refs::{Outcome, TermRefs};
use trie_rs::str_trie_map::StrTrieMap;
use trie_rs::str_trie_map::automaton::CodepointWildcard;

/// Score handicap for anchor tokens followed by `*`: matching them
/// scans a whole subtree instead of a single exact entry, so they
/// must out-length an exact token by this many codepoints to win.
const STARRED_ANCHOR_PENALTY: i32 = ffi::SUFFIX_STARRED_ANCHOR_PENALTY as i32;

/// Longest addable term, in bytes, after lowercasing. The underlying
/// trie stores node labels with `u16` lengths, so a longer key cannot
/// be represented and would panic on insert.
const MAX_TERM_BYTE_LEN: usize = u16::MAX as usize;

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

    /// Add a term to the set, registering it under its own key and every
    /// queryable suffix. [Lowercased on entry](crate#case-insensitivity);
    /// re-adding an existing term, or adding an empty one, is a no-op.
    /// So is adding a term whose lowercased form exceeds `u16::MAX`
    /// bytes — the trie cannot represent such a key.
    pub fn add(&mut self, term: &str) {
        if term.is_empty() {
            return;
        }

        let lowered = unicode_tolower_cow(term);
        let term: &str = &lowered;

        if term.len() > MAX_TERM_BYTE_LEN {
            return;
        }

        if self.inner.get(term).is_some_and(TermRefs::has_full_term) {
            return;
        }

        let owner = Arc::from(term);
        self.inner.insert_with(term, |existing| {
            TermRefs::upsert_full_term(existing, Arc::clone(&owner))
        });

        for suffix in Self::suffixes_of(term) {
            self.inner.insert_with(suffix, |existing| {
                TermRefs::upsert_longer_term(existing, Arc::clone(&owner))
            });
        }
    }

    /// Remove a term from the set, evicting its own key and any suffix entries
    /// it was the last referrer to. [Lowercased on entry](crate#case-insensitivity);
    /// removing an absent or empty term is a no-op.
    pub fn remove(&mut self, term: &str) {
        if term.is_empty() {
            return;
        }

        let lowered = unicode_tolower_cow(term);
        let term: &str = &lowered;

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

    /// Iterate over every key in the index — each member term plus every
    /// indexed proper suffix — in lexicographical order.
    pub fn keys(&self) -> impl Iterator<Item = String> + '_ {
        self.inner.iter().map(|(key, _)| key)
    }

    /// Iterate over the members that contain `needle` as a substring.
    /// Matching is [case-insensitive](crate#case-insensitivity). Empty
    /// `needle` yields nothing. A term may be yielded more than once
    /// (once per matching suffix entry).
    pub fn iter_contains(&self, needle: &str) -> impl Iterator<Item = &str> {
        let lowered = unicode_tolower_cow(needle);
        self.inner
            .prefixed_values(&lowered)
            .flat_map(|data| data.terms().map(|term| &**term))
    }

    /// Iterate over the members that end with `needle`.
    /// Matching is [case-insensitive](crate#case-insensitivity). Empty
    /// `needle` yields nothing.
    pub fn iter_suffix(&self, needle: &str) -> impl Iterator<Item = &str> {
        let lowered = unicode_tolower_cow(needle);
        let data = if lowered.is_empty() {
            None
        } else {
            self.inner.get(&lowered)
        };
        data.into_iter()
            .flat_map(|data| data.terms().map(|term| &**term))
    }

    /// Iterate over the members matching the glob `pattern`, where `*`
    /// matches any run of codepoints and `?` exactly one codepoint (`entr?`
    /// matches `entré`) — see [`CodepointWildcard`] for the matching model.
    /// Matching is [case-insensitive](crate#case-insensitivity). Returns
    /// `None` when no token in the pattern can seed the search (every token
    /// is empty or contains `?`).
    /// A term may be yielded more than once (once per matching suffix entry).
    pub fn iter_wildcard(&self, pattern: &str) -> Option<impl Iterator<Item = Arc<str>>> {
        let lowered = unicode_tolower_cow(pattern);
        let (token, followed_by_star) = Self::choose_token(&lowered)?;

        // A token followed by `*` can sit anywhere inside a match,
        // so every suffix entry starting with it is a candidate. A
        // token at the very end of the pattern must terminate the
        // match, so only terms ending with it — exactly its own
        // suffix entry — qualify.
        let (subtree, exact) = if followed_by_star {
            (Some(self.inner.prefixed_values(token)), None)
        } else {
            (None, self.inner.get(token))
        };

        let wildcard = CodepointWildcard::parse(&lowered);
        let matches: Vec<Arc<str>> = subtree
            .into_iter()
            .flatten()
            .chain(exact)
            .flat_map(|data| data.terms().cloned())
            .filter(|term| wildcard.matches(term))
            .collect();
        Some(matches.into_iter())
    }

    /// Returns the anchor token to look up for a wildcard `pattern`
    /// (e.g. `"ab*cd"`), paired with whether a `*` follows it.
    ///
    /// The anchor is the `*`-separated token expected to narrow the
    /// candidate set the most. Returns [`None`] if no token is
    /// eligible, i.e. every token is empty (a bare `*`) or contains
    /// `?` or `\`.
    fn choose_token(pattern: &str) -> Option<(&str, bool)> {
        pattern
            .split_inclusive('*')
            .map(|token| match token.strip_suffix('*') {
                Some(stripped) => (stripped, true),
                None => (token, false),
            })
            .filter(|(token, _)| !token.is_empty() && !token.contains(['?', '\\']))
            .max_by_key(|&(token, followed_by_star)| {
                token.chars().count() as i32
                    - if followed_by_star {
                        STARRED_ANCHOR_PENALTY
                    } else {
                        0
                    }
            })
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    /// Anchor choice affects performance, never the result set, so
    /// no test going through the public API can observe it; the
    /// scoring rules are pinned against the private heuristic.
    #[rstest]
    #[case::score_tie_prefers_later_token("ab*cd*", Some(("cd", true)))]
    #[case::starred_anchor_penalty_outweighs_small_length_lead("abcdef*gh", Some(("gh", false)))]
    #[case::length_lead_above_starred_anchor_penalty_wins("abcdefgh*ij", Some(("abcdefgh", true)))]
    #[case::single_char_tokens_are_eligible("a*b", Some(("b", false)))]
    #[case::all_empty_tokens_ineligible("*", None)]
    #[case::tokens_with_question_mark_or_backslash_ineligible("a?cd*\\ab*ef", Some(("ef", false)))]
    #[case::length_counts_codepoints_not_bytes("日本語*ab", Some(("ab", false)))]
    fn choose_token_test(#[case] pattern: &str, #[case] expected: Option<(&str, bool)>) {
        assert_eq!(TermSuffixIndex::choose_token(pattern), expected);
    }
}
