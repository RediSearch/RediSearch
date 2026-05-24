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
    rune::{Rune, bytes_to_rune, rune_to_bytes},
};

pub struct PrefixedIter<'tm, Data: 'tm>(Option<iter::Iter<'tm, Data, filter::VisitAll>>);

impl<'tm, Data: 'tm> PrefixedIter<'tm, Data> {
    pub(crate) fn new(trie: &'tm TrieMap<Data>, prefix: &[Rune]) -> Self {
        if prefix.is_empty() {
            return Self(None);
        }
        Self(Some(trie.prefixed_iter(&rune_to_bytes(prefix))))
    }
}

impl<'tm, Data: 'tm> Iterator for PrefixedIter<'tm, Data> {
    type Item = (Vec<Rune>, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.as_mut()?.next().map(|(k, v)| (bytes_to_rune(&k), v))
    }
}
