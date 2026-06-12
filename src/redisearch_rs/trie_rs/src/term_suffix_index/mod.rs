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
//!
//! Suffixes shorter than [`MIN_SUFFIX`] codepoints aren't indexed —
//! trades index size for a minimum supported needle length.

mod term_refs;

use std::rc::Rc;

use rqe_wildcard::{MatchOutcome, WildcardPattern};

use crate::str::StrTrieMap;
use term_refs::{Outcome, TermRefs};

const MIN_SUFFIX: usize = ffi::MIN_SUFFIX as usize;

pub struct TermSuffixIndex {
    inner: StrTrieMap<TermRefs>,
}

impl Default for TermSuffixIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl TermSuffixIndex {
    pub const fn new() -> Self {
        Self {
            inner: StrTrieMap::new(),
        }
    }

    /// Estimated heap memory currently held by the index — O(1).
    ///
    /// Counts the underlying trie structure only (see
    /// [`StrTrieMap::mem_usage`]); the shared term buffers and
    /// per-entry back-reference vectors are not included. The C
    /// counterpart (`TrieType_MemUsage`) likewise estimates from node
    /// count alone, ignoring payloads.
    pub const fn mem_usage(&self) -> usize {
        self.inner.mem_usage()
    }

    pub fn add(&mut self, term: &str) {
        if term.is_empty() {
            return;
        }

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
        if term.is_empty() {
            return;
        }

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
        let total_chars = term.chars().count();
        term.char_indices()
            .enumerate()
            .skip(1)
            .take_while(move |(char_idx, _)| total_chars - char_idx >= MIN_SUFFIX)
            .map(|(_, (byte_idx, _))| &term[byte_idx..])
    }

    /// Yield every indexed term containing `needle`, in unspecified
    /// order. Empty `needle` yields nothing. A term may be yielded
    /// more than once; duplicates share an allocation, so dedupe by
    /// [`str::as_ptr`] if needed.
    ///
    /// Non-empty `needle` must span at least [`MIN_SUFFIX`] codepoints.
    /// Shorter needles silently yield a subset of matching terms,
    /// since suffixes below the threshold aren't indexed; debug
    /// builds assert this. Production callers filter upstream via
    /// the query engine's `minTermPrefix` gate.
    pub fn iter_contains<'tm>(
        &'tm self,
        needle: &str,
    ) -> impl Iterator<Item = &'tm str> + use<'tm> {
        debug_assert!(
            needle.is_empty() || needle.chars().count() >= MIN_SUFFIX,
            "needle must span at least {MIN_SUFFIX} codepoints; caller must filter shorter needles (production gate: minTermPrefix)",
        );
        self.inner
            .prefixed_values(needle)
            .flat_map(|data| data.terms().map(|term| &**term))
    }

    /// Call `f` on every indexed term containing `needle`, with the same
    /// matching semantics, ordering, duplicate behavior, and needle-length
    /// requirements as [`Self::iter_contains`]. `f` returns `false` to stop
    /// the walk early.
    ///
    /// Allocation-free: prefer it over [`Self::iter_contains`] when the
    /// terms are consumed by a callback, where the iterator's per-call heap
    /// allocation would dominate short walks.
    pub fn visit_contains<F: FnMut(&str) -> bool>(&self, needle: &str, mut f: F) {
        debug_assert!(
            needle.is_empty() || needle.chars().count() >= MIN_SUFFIX,
            "needle must span at least {MIN_SUFFIX} codepoints; caller must filter shorter needles (production gate: minTermPrefix)",
        );
        self.inner.visit_prefixed_values(needle, &mut |data| {
            for term in data.terms() {
                if !f(term) {
                    return false;
                }
            }
            true
        });
    }

    /// Yield every indexed term that ends with `needle`, in unspecified
    /// order. Empty `needle` yields nothing.
    ///
    /// Each matching term is yielded exactly once.
    ///
    /// Non-empty `needle` must span at least [`MIN_SUFFIX`] codepoints.
    /// Shorter needles silently yield a subset of matching terms,
    /// since suffixes below the threshold aren't indexed; debug
    /// builds assert this. Production callers filter upstream via
    /// the query engine's `minTermPrefix` gate.
    pub fn iter_suffix<'tm>(&'tm self, needle: &str) -> impl Iterator<Item = &'tm str> + use<'tm> {
        debug_assert!(
            needle.is_empty() || needle.chars().count() >= MIN_SUFFIX,
            "needle must span at least {MIN_SUFFIX} codepoints; caller must filter shorter needles (production gate: minTermPrefix)",
        );
        let data = if needle.is_empty() {
            None
        } else {
            self.inner.get(needle)
        };
        data.into_iter()
            .flat_map(|data| data.terms().map(|term| &**term))
    }

    /// Yield every indexed term matching the wildcard `pattern`
    /// (`*` matches any run of characters, `?` exactly one), in
    /// unspecified order — or `None` when the pattern has no literal
    /// token that can anchor the search, in which case the caller
    /// must fall back to scanning the full term dictionary.
    ///
    /// A term may be yielded more than once; dedupe by
    /// [`Rc::as_ptr`] if needed.
    ///
    /// The search is anchored on the pattern's best literal token
    /// (see [`Self::choose_token`]): every term containing that
    /// token is pulled from the token's suffix entries, then
    /// filtered against the full pattern. Anchoring only narrows
    /// the candidate set — the filter alone decides membership — so
    /// `Some` and the fallback scan agree on the result set.
    pub fn iter_wildcard<'tm, 'p>(
        &'tm self,
        pattern: &'p str,
    ) -> Option<impl Iterator<Item = Rc<str>> + use<'tm, 'p>> {
        let (token, followed_by_star) = Self::choose_token(pattern)?;

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

        let pattern = WildcardPattern::parse(pattern.as_bytes());
        let iter = subtree
            .into_iter()
            .flatten()
            .chain(exact)
            .flat_map(|data| data.terms().cloned())
            .filter(move |term| pattern.matches(term.as_bytes()) == MatchOutcome::Match);
        Some(iter)
    }

    /// Pick the literal token of `pattern` (a maximal `*`-free run)
    /// that most narrows the candidate set, mirroring the scoring of
    /// the C `Suffix_ChooseToken_rune`: longer tokens win, a token
    /// that needs a subtree walk (followed by `*`) is docked 5
    /// points, and later tokens win ties.
    ///
    /// Tokens spanning fewer than [`MIN_SUFFIX`] codepoints are
    /// ineligible, as are tokens containing `?` or `\` — those are
    /// not literally matchable, so they cannot anchor a trie lookup.
    /// (The C heuristic could still anchor on a `?` token via a
    /// wildcard-aware trie walk; here such patterns report no anchor
    /// and take the fallback scan instead, which only affects speed,
    /// never the result set.)
    ///
    /// Returns the token and whether it is followed by `*` (when not,
    /// the token ends the pattern).
    fn choose_token(pattern: &str) -> Option<(&str, bool)> {
        let tokens: Vec<&str> = pattern.split('*').collect();
        let last = tokens.len() - 1;

        let mut best = None;
        let mut best_score = i32::MIN;
        for (i, token) in tokens.into_iter().enumerate() {
            if token.chars().count() < MIN_SUFFIX || token.contains(['?', '\\']) {
                continue;
            }
            let followed_by_star = i < last;
            let mut score = token.chars().count() as i32;
            if followed_by_star {
                score -= 5;
            }
            if score >= best_score {
                best_score = score;
                best = Some((token, followed_by_star));
            }
        }
        best
    }

    /// Yield every key stored in the underlying trie — each indexed
    /// term plus every proper suffix of length ≥ [`MIN_SUFFIX`] — in
    /// lexicographical order.
    ///
    /// Introspection aid (powers `FT.DEBUG DUMP_SUFFIX_TRIE`); query
    /// paths use [`Self::iter_contains`] / [`Self::iter_suffix`].
    pub fn keys<'tm>(&'tm self) -> impl Iterator<Item = String> + use<'tm> {
        self.inner.iter().map(|(key, _)| key)
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
    #[case::subtree_penalty_outweighs_small_length_lead("abcdef*gh", Some(("gh", false)))]
    #[case::length_lead_above_subtree_penalty_wins("abcdefgh*ij", Some(("abcdefgh", true)))]
    #[case::tokens_below_min_suffix_ineligible("a*b", None)]
    #[case::tokens_with_question_mark_or_backslash_ineligible("a?cd*\\ab*ef", Some(("ef", false)))]
    #[case::length_counts_codepoints_not_bytes("日本語*ab", Some(("ab", false)))]
    fn choose_token(#[case] pattern: &str, #[case] expected: Option<(&str, bool)>) {
        assert_eq!(TermSuffixIndex::choose_token(pattern), expected);
    }
}
