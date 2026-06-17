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

impl<'a, Data> Iterator for Iter<'a, Data> {
    type Item = (String, &'a Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| (key_to_string(k), v))
    }
}

/// Decode a trie byte key back to a `String`. Keys enter the [`crate::str_trie_map::StrTrieMap`]
/// exclusively via `&str` so they are UTF-8 by construction; the validating
/// `from_utf8` call here is cheap and protects against any future raw-byte
/// insertion at the lower layer.
pub(super) fn key_to_string(bytes: Vec<u8>) -> String {
    String::from_utf8(bytes).expect("StrTrieMap keys are UTF-8 by construction")
}
