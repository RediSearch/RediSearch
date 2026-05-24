/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{TrieMap, iter, str::iter::iter_::key_to_string};

pub struct ContainsIter<'tm, 'p, Data: 'tm>(Option<iter::ContainsIter<'tm, 'p, Data>>);

impl<'tm, 'p, Data: 'tm> ContainsIter<'tm, 'p, Data> {
    pub(crate) fn new(trie: &'tm TrieMap<Data>, target: &'p str) -> Self {
        if target.is_empty() {
            return Self(None);
        }
        Self(Some(trie.contains_iter(target.as_bytes())))
    }
}

impl<'tm, 'p, Data: 'tm> Iterator for ContainsIter<'tm, 'p, Data> {
    type Item = (String, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.as_mut()?.next().map(|(k, v)| (key_to_string(k), v))
    }
}
