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
    iter,
    rune::{Rune, bytes_to_rune, rune_to_bytes},
};

#[ouroboros::self_referencing]
struct ContainsIterInner<'tm, Data: 'tm> {
    target_bytes: Box<[u8]>,
    #[borrows(target_bytes)]
    #[covariant]
    inner: iter::ContainsIter<'tm, 'this, Data>,
}

pub struct ContainsIter<'tm, Data: 'tm>(Option<ContainsIterInner<'tm, Data>>);

impl<'tm, Data: 'tm> ContainsIter<'tm, Data> {
    pub(crate) fn new(trie: &'tm TrieMap<Data>, target: &[Rune]) -> Self {
        if target.is_empty() {
            return Self(None);
        }
        let target_bytes: Box<[u8]> = rune_to_bytes(target).into_boxed_slice();
        Self(Some(
            ContainsIterInnerBuilder {
                target_bytes,
                inner_builder: |t| trie.contains_iter(t.as_ref()),
            }
            .build(),
        ))
    }
}

impl<'tm, Data: 'tm> Iterator for ContainsIter<'tm, Data> {
    type Item = (Vec<Rune>, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .as_mut()?
            .with_inner_mut(|i| i.next())
            .map(|(k, v)| (bytes_to_rune(&k), v))
    }
}
