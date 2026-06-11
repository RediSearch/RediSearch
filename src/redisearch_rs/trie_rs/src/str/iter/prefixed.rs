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
    str::iter::iter_::key_to_string,
};

/// Prefix-filtered iterator over a [`StrTrieMap`](crate::str::StrTrieMap),
/// in lexicographical key order.
///
/// Empty `prefix` yields zero matches — differs from
/// [`TrieMap::prefixed_iter`] which yields every entry on `&[]`. The wrapped
/// `Option` carries the short-circuit: `None` when the prefix was empty.
///
/// See [`crate::iter::Iter`] for the underlying traversal.
pub struct PrefixedIter<'tm, Data: 'tm>(Option<iter::Iter<'tm, Data, filter::VisitAll>>);

impl<'tm, Data: 'tm> PrefixedIter<'tm, Data> {
    pub(crate) fn new(trie: &'tm TrieMap<Data>, prefix: &str) -> Self {
        if prefix.is_empty() {
            return Self(None);
        }
        Self(Some(trie.prefixed_iter(prefix.as_bytes())))
    }
}

impl<'tm, Data: 'tm> Iterator for PrefixedIter<'tm, Data> {
    type Item = (String, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.as_mut()?.next().map(|(k, v)| (key_to_string(k), v))
    }
}
