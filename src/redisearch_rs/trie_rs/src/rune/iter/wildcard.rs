/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rqe_wildcard::WildcardPattern;

use crate::{
    TrieMap,
    iter::{self},
    rune::{Rune, bytes_to_rune, rune_to_bytes},
};

pub struct RuneTrieMapWildcardIter<'a, Data>(iter::WildcardIter<'a, Data>);

impl<'a, Data> RuneTrieMapWildcardIter<'a, Data> {
    pub(crate) fn new(trie: &'a TrieMap<Data>, buf: &[u16]) -> Self {
        let pattern = WildcardPattern::parse(&rune_to_bytes(buf));
        Self(trie.wildcard_iter(pattern))
    }
}

impl<'a, Data> Iterator for RuneTrieMapWildcardIter<'a, Data> {
    type Item = (Vec<Rune>, &'a Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| (bytes_to_rune(&k), v))
    }
}
