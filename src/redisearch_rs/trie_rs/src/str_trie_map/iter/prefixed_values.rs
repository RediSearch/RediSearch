/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{TrieMap, iter::Values};

/// Prefix-filtered value iterator over a [`StrTrieMap`](crate::str_trie_map::StrTrieMap),
/// in lexicographical key order.
///
/// Empty `prefix` yields every value, like [`TrieMap::prefixed_values`] on
/// `&[]` — the empty string is a prefix of every key.
///
/// See [`crate::iter::Values`] for the underlying traversal.
pub struct PrefixedValues<'tm, Data: 'tm>(Values<'tm, Data>);

impl<'tm, Data: 'tm> PrefixedValues<'tm, Data> {
    pub(crate) fn new(trie: &'tm TrieMap<Data>, prefix: &str) -> Self {
        Self(trie.prefixed_values(prefix.as_bytes()))
    }
}

impl<'tm, Data: 'tm> Iterator for PrefixedValues<'tm, Data> {
    type Item = &'tm Data;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}
