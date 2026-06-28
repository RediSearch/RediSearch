/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A case-preserving dictionary of spell-check terms, backed by a
//! [`StrTrieMap`].
//!
//! # Case model
//!
//! Terms are stored verbatim. [`SpellCheckDictionary::contains`] and
//! [`SpellCheckDictionary::fuzzy_matches`] are case-insensitive — the query
//! and each candidate are lowercased via [`unicode_tolower`] before comparison
//! — but [`SpellCheckDictionary::remove`] matches verbatim and is therefore
//! case-sensitive.
//!
//! Because matching lowercases verbatim-stored keys, those two queries scan
//! every stored term rather than exploiting the trie's prefix structure.

use std::fmt::{self, Debug};

use string_utils::{unicode_tolower_capped, unicode_tolower_cow};
use trie_rs::str_trie_map::StrTrieMap;

/// Maximum query length, in Unicode codepoints, that the dictionary will match
/// against. An over-long term yields no matches rather than
/// scanning every stored entry.
const TRIE_MAX_PREFIX: usize = ffi::TRIE_MAX_PREFIX as usize;

/// Upper bound on insertable term length, in runes (codepoints).
const TRIE_INITIAL_STRING_LEN: usize = ffi::TRIE_INITIAL_STRING_LEN as usize;

/// A case-preserving set of spell-check terms.
///
/// See the [crate-level case model](crate#case-model) for how case is handled
/// across queries and removal.
#[derive(Default)]
pub struct SpellCheckDictionary {
    trie: StrTrieMap<()>,
}

impl Debug for SpellCheckDictionary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.trie.fmt(f)
    }
}

impl SpellCheckDictionary {
    /// Create a new, empty dictionary.
    pub const fn new() -> Self {
        Self {
            trie: StrTrieMap::new(),
        }
    }

    /// Insert a term, stored verbatim (case-preserving). Empty or over-long
    /// terms are rejected.
    ///
    /// Returns `true` only if the term was newly added.
    pub fn add(&mut self, term: &str) -> bool {
        // C runes are `uint16_t`; the byte gate is `TRIE_INITIAL_STRING_LEN *
        // sizeof(rune)`, matching `Trie_InsertStringBuffer`.
        if term.is_empty()
            || term.len() > TRIE_INITIAL_STRING_LEN * size_of::<u16>()
            || term.chars().count() >= TRIE_INITIAL_STRING_LEN
        {
            return false;
        }
        self.trie.insert(term, ()).is_none()
    }

    /// Remove a term. Matched verbatim, so removal is case-sensitive.
    ///
    /// Returns `true` if the term was present.
    pub fn remove(&mut self, term: &str) -> bool {
        self.trie.remove(term).is_some()
    }

    /// The number of terms stored.
    pub const fn len(&self) -> usize {
        self.trie.len()
    }

    /// `true` when no terms are stored.
    pub const fn is_empty(&self) -> bool {
        self.trie.is_empty()
    }

    /// Iterate over every stored term, in its original case, in lexicographical
    /// order.
    pub fn dump(&self) -> impl Iterator<Item = String> {
        self.trie.iter().map(|(term, _)| term)
    }

    /// Check whether a stored term equals `term`, ignoring case. Lowercased on
    /// lookup; an over-long `term` never matches.
    ///
    /// Returns `true` if such a term exists.
    pub fn contains(&self, term: &str) -> bool {
        let Some(needle) = unicode_tolower_capped(term, TRIE_MAX_PREFIX) else {
            return false;
        };
        self.trie
            .iter()
            .any(|(key, _)| *unicode_tolower_cow(&key) == *needle)
    }

    /// Find stored terms within Levenshtein edit distance `max_dist`
    /// (in codepoints) of `term`. Lowercased on lookup, so matching ignores
    /// case; an over-long `term` matches nothing.
    ///
    /// Returns an iterator over the matching terms, each in its stored case.
    pub fn fuzzy_matches(&self, term: &str, max_dist: u32) -> impl Iterator<Item = String> + '_ {
        let needle = unicode_tolower_capped(term, TRIE_MAX_PREFIX);
        needle.into_iter().flat_map(move |needle| {
            self.trie.iter().filter_map(move |(key, _)| {
                let dist = strsim::levenshtein(&unicode_tolower_cow(&key), &needle) as u32;
                (dist <= max_dist).then_some(key)
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use std::collections::BTreeSet;
    use string_utils::unicode_tolower;

    #[rstest]
    #[case(&["Hello"], "Hello", true)]
    #[case(&["Hello"], "hello", true)]
    #[case(&["Hello"], "HELLO", true)]
    #[case(&["Hello"], "world", false)]
    #[case(&["Fußball"], "fußball", true)]
    #[case(&["И"], "и", true)]
    fn contains_is_case_insensitive(
        #[case] stored: &[&str],
        #[case] query: &str,
        #[case] expected: bool,
    ) {
        let mut sut = SpellCheckDictionary::new();
        for term in stored {
            sut.add(term);
        }

        assert_eq!(sut.contains(query), expected);
    }

    #[test]
    fn remove_is_case_sensitive() {
        let mut sut = SpellCheckDictionary::new();
        sut.add("Foo");

        // Wrong case is a no-op; the term survives (observed via contains).
        assert!(!sut.remove("foo"));
        assert!(sut.contains("foo"));

        // Exact case removes it.
        assert!(sut.remove("Foo"));
        assert!(!sut.contains("foo"));
    }

    #[test]
    fn add_rejects_empty_term() {
        let mut sut = SpellCheckDictionary::new();

        assert!(!sut.add(""));
        assert_eq!(sut.len(), 0);
        assert!(!sut.contains(""));
    }

    #[rstest]
    #[case(TRIE_INITIAL_STRING_LEN - 1, true)] // 255 codepoints: accepted
    #[case(TRIE_INITIAL_STRING_LEN, false)] // 256 codepoints: rejected, like C
    fn add_enforces_codepoint_limit(#[case] codepoints: usize, #[case] accepted: bool) {
        let term: String = "a".repeat(codepoints);
        let mut sut = SpellCheckDictionary::new();

        assert_eq!(sut.add(&term), accepted);
        assert_eq!(sut.len(), usize::from(accepted));
    }

    #[test]
    fn add_enforces_byte_limit_for_multibyte() {
        // 'あ' is 3 UTF-8 bytes / 1 codepoint. 200 of them = 600 bytes (over the
        // 512-byte gate) but only 200 codepoints (under 256): C rejects it via
        // the byte gate, before the codepoint gate is reached.
        let term: String = "あ".repeat(200);
        assert!(term.len() > TRIE_INITIAL_STRING_LEN * size_of::<u16>());
        assert!(term.chars().count() < TRIE_INITIAL_STRING_LEN);

        let mut sut = SpellCheckDictionary::new();

        assert!(!sut.add(&term));
        assert_eq!(sut.len(), 0);
    }

    fn fuzzy(dict: &SpellCheckDictionary, query: &str, max_dist: u32) -> BTreeSet<String> {
        dict.fuzzy_matches(query, max_dist).collect()
    }

    #[test]
    fn lowering_is_per_codepoint_like_c_nu_tolower() {
        // Lowering is per codepoint (like C's nu_tolower): a final-position Σ
        // always becomes σ, never the word-final ς. So an uppercase query
        // matches a stored trailing σ.
        let mut sut = SpellCheckDictionary::new();
        sut.add("οδοσ"); // stored verbatim, trailing non-final sigma (σ)

        assert!(sut.contains("ΟΔΟΣ"));
        assert_eq!(fuzzy(&sut, "ΟΔΟΣ", 0), BTreeSet::from(["οδοσ".into()]));
    }

    #[rstest]
    #[case(TRIE_MAX_PREFIX, true)] // at the limit: still matches
    #[case(TRIE_MAX_PREFIX + 1, false)] // one over: ignored, like C
    #[cfg_attr(miri, ignore = "This test runs too slowly under Miri.")]
    fn query_length_cutoff(#[case] query_len: usize, #[case] expected_match: bool) {
        let term: String = "a".repeat(query_len);
        let mut sut = SpellCheckDictionary::new();
        sut.add(&term);

        assert_eq!(sut.contains(&term), expected_match);
        assert_eq!(sut.fuzzy_matches(&term, 0).next().is_some(), expected_match);
    }

    #[test]
    fn cutoff_measures_lowercased_codepoints() {
        // 'İ' (U+0130) lowercases to two codepoints ("i̇"), so 51 of them
        // exceed the 100-codepoint limit only after lowercasing.
        let term: String = "İ".repeat(51);
        assert_eq!(term.chars().count(), 51);
        assert!(unicode_tolower(&term).chars().count() > TRIE_MAX_PREFIX);

        let mut sut = SpellCheckDictionary::new();
        sut.add(&term);

        assert!(!sut.contains(&term));
        assert!(sut.fuzzy_matches(&term, 0).next().is_none());
    }
}
