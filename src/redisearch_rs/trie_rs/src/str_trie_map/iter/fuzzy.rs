/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{
    TrieMap,
    iter::AutomatonIter,
    str_trie_map::{
        automaton::{CaseFoldLevenshtein, CaseFoldLevenshteinNfa},
        iter::unfiltered::key_to_string,
    },
};

/// Iterator over the entries of a
/// [`StrTrieMap`](crate::str_trie_map::StrTrieMap) whose case-folded key is
/// within a Levenshtein distance of a needle, in lexicographical key order.
///
/// See [`CaseFoldLevenshtein`] for the matching model. Backed by the
/// bit-parallel [`CaseFoldLevenshteinNfa`] whenever the needle and distance
/// fit its word width — the narrowest first — falling back to the DP-row
/// automaton beyond; all backends accept the same keys.
pub struct FuzzyIter<'tm, Data: 'tm>(Backend<'tm, Data>);

enum Backend<'tm, Data: 'tm> {
    /// `u64`-backed NFA — folded needle has ≤ 63 codepoints.
    Nfa64(AutomatonIter<'tm, Data, CaseFoldLevenshteinNfa<u64>>),
    /// `u128`-backed NFA — folded needle has 64..=127 codepoints.
    Nfa128(AutomatonIter<'tm, Data, CaseFoldLevenshteinNfa<u128>>),
    /// DP-row automaton — needle or distance past every NFA bound.
    Dp(AutomatonIter<'tm, Data, CaseFoldLevenshtein>),
}

impl<'tm, Data: 'tm> FuzzyIter<'tm, Data> {
    pub(crate) fn new(trie: &'tm TrieMap<Data>, needle: &str, max_dist: u32) -> Self {
        if let Some(nfa) = CaseFoldLevenshteinNfa::<u64>::new(needle, max_dist) {
            Self(Backend::Nfa64(trie.automaton_iter(nfa)))
        } else if let Some(nfa) = CaseFoldLevenshteinNfa::<u128>::new(needle, max_dist) {
            Self(Backend::Nfa128(trie.automaton_iter(nfa)))
        } else {
            Self(Backend::Dp(
                trie.automaton_iter(CaseFoldLevenshtein::new(needle, max_dist)),
            ))
        }
    }
}

impl<'tm, Data: 'tm> Iterator for FuzzyIter<'tm, Data> {
    type Item = (String, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.0 {
            Backend::Nfa64(it) => it.next().map(|(k, v)| (key_to_string(k), v)),
            Backend::Nfa128(it) => it.next().map(|(k, v)| (key_to_string(k), v)),
            Backend::Dp(it) => it.next().map(|(k, v)| (key_to_string(k), v)),
        }
    }
}
