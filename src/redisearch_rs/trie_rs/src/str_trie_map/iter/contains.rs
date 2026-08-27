/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{TrieMap, iter, str_trie_map::iter::unfiltered::key_to_string};

/// Substring-filtered iterator over a [`StrTrieMap`](crate::str_trie_map::StrTrieMap),
/// in lexicographical key order.
///
/// Empty `target` yields every entry — the empty string is a substring of
/// every key.
///
/// See [`crate::iter::ContainsIter`] for the underlying traversal.
pub struct ContainsIter<'tm, Data: 'tm>(iter::ContainsIter<'tm, Data>);

impl<'tm, Data: 'tm> ContainsIter<'tm, Data> {
    pub(crate) fn new(trie: &'tm TrieMap<Data>, target: &str) -> Self {
        Self(trie.contains_iter(target.as_bytes()))
    }
}

impl<'tm, Data: 'tm> Iterator for ContainsIter<'tm, Data> {
    type Item = (String, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| (key_to_string(k), v))
    }
}
