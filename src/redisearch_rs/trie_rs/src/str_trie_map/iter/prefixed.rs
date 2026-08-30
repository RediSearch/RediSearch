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

/// Prefix-filtered iterator over a [`StrTrieMap`](crate::str_trie_map::StrTrieMap),
/// in lexicographical key order.
///
/// Empty `prefix` yields every entry, like [`TrieMap::prefixed_iter`] on
/// `&[]` — the empty string is a prefix of every key.
///
/// See [`crate::iter::Iter`] for the underlying traversal.
pub struct PrefixedIter<'tm, Data: 'tm>(iter::Iter<'tm, Data, filter::VisitAll>);

impl<'tm, Data: 'tm> PrefixedIter<'tm, Data> {
    pub(crate) fn new(trie: &'tm TrieMap<Data>, prefix: &str) -> Self {
        Self(trie.prefixed_iter(prefix.as_bytes()))
    }
}

impl<'tm, Data: 'tm> LendingStrIter<'tm> for PrefixedIter<'tm, Data> {
    type Data = Data;

    fn next_borrowed(&mut self) -> Option<(&str, &'tm Data)> {
        let data = self.0.advance()?;
        Some((key_to_str(self.0.key()), data))
    }
}

impl<'tm, Data: 'tm> Iterator for PrefixedIter<'tm, Data> {
    type Item = (String, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        let (key, data) = self.next_borrowed()?;
        Some((key.to_owned(), data))
    }
}
