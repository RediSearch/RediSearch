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
    automaton::CaseFoldExact,
    iter::AutomatonIter,
    str_trie_map::iter::{LendingStrIter, key_to_str},
};

/// Iterator over the entries of a
/// [`StrTrieMap`](crate::str_trie_map::StrTrieMap) whose key equals a needle
/// after per-codepoint case folding, in lexicographical key order.
///
/// See [`CaseFoldExact`] for the matching model.
pub struct CaseInsensitiveIter<'tm, Data: 'tm>(AutomatonIter<'tm, Data, CaseFoldExact>);

impl<'tm, Data: 'tm> CaseInsensitiveIter<'tm, Data> {
    pub(crate) fn new(trie: &'tm TrieMap<Data>, needle: &str) -> Self {
        Self(trie.automaton_iter(CaseFoldExact::new(needle)))
    }
}

impl<'tm, Data: 'tm> LendingStrIter<'tm> for CaseInsensitiveIter<'tm, Data> {
    type Data = Data;

    fn next_borrowed(&mut self) -> Option<(&str, &'tm Data)> {
        let data = self.0.advance()?;
        Some((key_to_str(self.0.key()), data))
    }
}

impl<'tm, Data: 'tm> Iterator for CaseInsensitiveIter<'tm, Data> {
    type Item = (String, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, data) = self.next_borrowed()?;
        Some((key.to_owned(), data))
    }
}
