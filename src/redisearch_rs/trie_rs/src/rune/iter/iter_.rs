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
    rune::{Rune, bytes_to_rune},
};

pub struct Iter<'a, Data>(iter::Iter<'a, Data, filter::VisitAll>);

impl<'a, Data> Iter<'a, Data> {
    pub(crate) fn new(trie: &'a TrieMap<Data>) -> Self {
        Self(trie.iter())
    }
}

impl<'a, Data> Iterator for Iter<'a, Data> {
    type Item = (Vec<Rune>, &'a Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| (bytes_to_rune(&k), v))
    }
}
