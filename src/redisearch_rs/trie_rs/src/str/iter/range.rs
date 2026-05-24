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
    str::iter::iter_::key_to_string,
};

pub struct RangeIter<'tm, 'p, Data: 'tm>(iter::RangeIter<'tm, 'p, Data>);

impl<'tm, 'p, Data: 'tm> RangeIter<'tm, 'p, Data> {
    pub(crate) fn build_from(
        trie: &'tm TrieMap<Data>,
        min: Option<&'p str>,
        include_min: bool,
        max: Option<&'p str>,
        include_max: bool,
    ) -> Self {
        let filter = RangeFilter {
            min: min.map(|m| RangeBoundary {
                value: m.as_bytes(),
                is_included: include_min,
            }),
            max: max.map(|m| RangeBoundary {
                value: m.as_bytes(),
                is_included: include_max,
            }),
        };
        Self(trie.range_iter(filter))
    }
}

impl<'tm, 'p, Data: 'tm> Iterator for RangeIter<'tm, 'p, Data> {
    type Item = (String, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| (key_to_string(k), v))
    }
}
