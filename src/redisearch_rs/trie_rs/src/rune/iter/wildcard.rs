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
    TrieMap, iter,
    rune::{Rune, bytes_to_rune, rune_to_bytes},
};

#[ouroboros::self_referencing]
struct WildcardIterInner<'tm, Data: 'tm> {
    pattern_bytes: Box<[u8]>,
    #[borrows(pattern_bytes)]
    #[covariant]
    inner: iter::WildcardIter<'tm, 'this, Data>,
}

pub struct WildcardIter<'tm, Data: 'tm>(WildcardIterInner<'tm, Data>);

impl<'tm, Data: 'tm> WildcardIter<'tm, Data> {
    pub(crate) fn new(trie: &'tm TrieMap<Data>, buf: &[u16]) -> Self {
        let pattern_bytes: Box<[u8]> = rune_to_bytes(buf).into_boxed_slice();
        Self(
            WildcardIterInnerBuilder {
                pattern_bytes,
                inner_builder: |p| trie.wildcard_iter(WildcardPattern::parse(p.as_ref())),
            }
            .build(),
        )
    }
}

impl<'tm, Data: 'tm> Iterator for WildcardIter<'tm, Data> {
    type Item = (Vec<Rune>, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .with_inner_mut(|i| i.next())
            .map(|(k, v)| (bytes_to_rune(&k), v))
    }
}
