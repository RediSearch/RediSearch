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
    iter::{self, filter},
    str_trie_map::iter::{LendingStrIter, key_to_str},
};

/// Lexicographical-order iterator over a
/// [`StrTrieMap`](crate::str_trie_map::StrTrieMap).
///
/// See [`crate::iter::Iter`] for the underlying traversal.
pub struct Iter<'a, Data>(iter::Iter<'a, Data, filter::VisitAll>);

impl<'a, Data> Iter<'a, Data> {
    pub(crate) fn new(trie: &'a TrieMap<Data>) -> Self {
        Self(trie.iter())
    }
}

impl<'a, Data: 'a> LendingStrIter<'a> for Iter<'a, Data> {
    type Data = Data;

    fn next_borrowed(&mut self) -> Option<(&str, &'a Data)> {
        let data = self.0.advance()?;
        Some((key_to_str(self.0.key()), data))
    }
}

impl<'a, Data: 'a> Iterator for Iter<'a, Data> {
    type Item = (String, &'a Data);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, data) = self.next_borrowed()?;
        Some((key.to_owned(), data))
    }
}
