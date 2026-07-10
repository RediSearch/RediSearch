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
    str_trie_map::{automaton::CaseFoldLevenshtein, iter::unfiltered::key_to_string},
};

/// Iterator over the entries of a
/// [`StrTrieMap`](crate::str_trie_map::StrTrieMap) whose case-folded key is
/// within a Levenshtein distance of a needle, in lexicographical key order.
///
/// See [`CaseFoldLevenshtein`] for the matching model.
pub struct FuzzyIter<'tm, Data: 'tm>(AutomatonIter<'tm, Data, CaseFoldLevenshtein>);

impl<'tm, Data: 'tm> FuzzyIter<'tm, Data> {
    pub(crate) fn new(trie: &'tm TrieMap<Data>, needle: &str, max_dist: u32) -> Self {
        Self(trie.automaton_iter(CaseFoldLevenshtein::new(needle, max_dist)))
    }
}

impl<'tm, Data: 'tm> Iterator for FuzzyIter<'tm, Data> {
    type Item = (String, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| (key_to_string(k), v))
    }
}
