/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rqe_wildcard::WildcardPattern;

use crate::{TrieMap, iter, str::iter::iter_::key_to_string};

pub struct WildcardIter<'tm, 'p, Data: 'tm>(iter::WildcardIter<'tm, 'p, Data>);

impl<'tm, 'p, Data: 'tm> WildcardIter<'tm, 'p, Data> {
    pub(crate) fn new(trie: &'tm TrieMap<Data>, pattern: &'p str) -> Self {
        Self(trie.wildcard_iter(WildcardPattern::parse(pattern.as_bytes())))
    }
}

impl<'tm, 'p, Data: 'tm> Iterator for WildcardIter<'tm, 'p, Data> {
    type Item = (String, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| (key_to_string(k), v))
    }
}
