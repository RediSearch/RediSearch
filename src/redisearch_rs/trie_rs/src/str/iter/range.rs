/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::borrow::Cow;

use crate::{
    TrieMap,
    iter::{self, RangeBoundary, RangeFilter},
    str::iter::iter_::key_to_string,
};

/// Range iterator over a [`StrTrieMap`](crate::str::StrTrieMap).
///
/// See [`WildcardIter`](super::WildcardIter) for the two-variant
/// borrowed-vs-drained dispatch contract; this iterator uses the same
/// pattern. `None` bounds disable the corresponding side of the range.
pub enum RangeIter<'tm, 'p, Data: 'tm> {
    Borrowed(iter::RangeIter<'tm, 'p, Data>),
    Drained(std::vec::IntoIter<(String, &'tm Data)>),
}

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
        Self::Borrowed(trie.range_iter(filter))
    }

    pub(crate) fn build_from_cow(
        trie: &'tm TrieMap<Data>,
        min: Option<Cow<'p, str>>,
        include_min: bool,
        max: Option<Cow<'p, str>>,
        include_max: bool,
    ) -> Self {
        let min_owns = matches!(min, Some(Cow::Owned(_)));
        let max_owns = matches!(max, Some(Cow::Owned(_)));

        if !min_owns && !max_owns {
            // Both bounds either absent or borrowed — stay lazy. Extract
            // the original `&'p str` borrows so the result carries the
            // caller's lifetime, not a borrow from the locals here.
            let min_ref: Option<&'p str> = match min {
                Some(Cow::Borrowed(s)) => Some(s),
                _ => None,
            };
            let max_ref: Option<&'p str> = match max {
                Some(Cow::Borrowed(s)) => Some(s),
                _ => None,
            };
            return Self::build_from(trie, min_ref, include_min, max_ref, include_max);
        }

        // Eagerly drain when any bound owns its bytes: see
        // [`WildcardIter`](super::WildcardIter) for the rationale.
        let min_buf: Option<Vec<u8>> = min.map(|m| m.into_owned().into_bytes());
        let max_buf: Option<Vec<u8>> = max.map(|m| m.into_owned().into_bytes());
        let filter = RangeFilter {
            min: min_buf.as_deref().map(|value| RangeBoundary {
                value,
                is_included: include_min,
            }),
            max: max_buf.as_deref().map(|value| RangeBoundary {
                value,
                is_included: include_max,
            }),
        };
        let drained: Vec<(String, &'tm Data)> = trie
            .range_iter(filter)
            .map(|(k, v)| (key_to_string(k), v))
            .collect();
        Self::Drained(drained.into_iter())
    }
}

impl<'tm, 'p, Data: 'tm> Iterator for RangeIter<'tm, 'p, Data> {
    type Item = (String, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Borrowed(inner) => inner.next().map(|(k, v)| (key_to_string(k), v)),
            Self::Drained(iter) => iter.next(),
        }
    }
}
