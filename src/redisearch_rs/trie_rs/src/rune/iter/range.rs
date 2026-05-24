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
    iter::{self, RangeBoundary, RangeFilter},
    rune::{Rune, bytes_to_rune, rune_to_bytes},
};

#[ouroboros::self_referencing]
pub struct RangeIter<'tm, Data: 'tm> {
    bytes: (Option<Box<[u8]>>, Option<Box<[u8]>>),
    #[borrows(bytes)]
    #[covariant]
    inner: iter::RangeIter<'tm, 'this, Data>,
}

impl<'tm, Data: 'tm> RangeIter<'tm, Data> {
    pub(crate) fn build_from(
        trie: &'tm TrieMap<Data>,
        min: Option<&[Rune]>,
        include_min: bool,
        max: Option<&[Rune]>,
        include_max: bool,
    ) -> Self {
        let min_bytes: Option<Box<[u8]>> = min.map(|m| rune_to_bytes(m).into_boxed_slice());
        let max_bytes: Option<Box<[u8]>> = max.map(|m| rune_to_bytes(m).into_boxed_slice());

        RangeIterBuilder {
            bytes: (min_bytes, max_bytes),
            inner_builder: |(mn, mx)| {
                let filter = RangeFilter {
                    min: mn.as_ref().map(|b| RangeBoundary {
                        value: b.as_ref(),
                        is_included: include_min,
                    }),
                    max: mx.as_ref().map(|b| RangeBoundary {
                        value: b.as_ref(),
                        is_included: include_max,
                    }),
                };
                trie.range_iter(filter)
            },
        }
        .build()
    }
}

impl<'tm, Data: 'tm> Iterator for RangeIter<'tm, Data> {
    type Item = (Vec<Rune>, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.with_inner_mut(|inner| inner.next())
            .map(|(k, v)| (bytes_to_rune(&k), v))
    }
}
