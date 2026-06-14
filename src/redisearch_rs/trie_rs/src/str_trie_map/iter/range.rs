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
    iter::{self, RangeBoundary as InnerBoundary, RangeFilter as InnerFilter},
    str_trie_map::iter::unfiltered::key_to_string,
};

/// One of the bounds for a [`RangeFilter`].
///
/// Mirrors [`crate::iter::RangeBoundary`] but carries a UTF-8 [`str`] value
/// instead of raw bytes, so the [`StrTrieMap`](crate::str_trie_map::StrTrieMap) invariant
/// extends through the range API.
#[derive(Clone, Copy, Debug)]
pub struct RangeBoundary<'f> {
    pub value: &'f str,
    pub is_included: bool,
}

impl<'f> RangeBoundary<'f> {
    /// Create a new range boundary that includes its boundary value.
    pub const fn included(value: &'f str) -> Self {
        Self {
            value,
            is_included: true,
        }
    }

    /// Create a new range boundary that doesn't include its boundary value.
    pub const fn excluded(value: &'f str) -> Self {
        Self {
            value,
            is_included: false,
        }
    }
}

/// Lower- and upper-bound filter for [`RangeIter`].
///
/// A `None` bound disables that side of the range; pairing inclusivity with
/// the boundary value in [`RangeBoundary`] makes "unbounded but inclusive"
/// unrepresentable.
#[derive(Clone, Copy, Debug, Default)]
pub struct RangeFilter<'f> {
    pub min: Option<RangeBoundary<'f>>,
    pub max: Option<RangeBoundary<'f>>,
}

impl RangeFilter<'_> {
    /// A filter that matches all entries.
    pub const fn all() -> Self {
        Self {
            min: None,
            max: None,
        }
    }
}

impl<'f> From<RangeBoundary<'f>> for InnerBoundary<'f> {
    fn from(b: RangeBoundary<'f>) -> Self {
        Self {
            value: b.value.as_bytes(),
            is_included: b.is_included,
        }
    }
}

impl<'f> From<RangeFilter<'f>> for InnerFilter<'f> {
    fn from(f: RangeFilter<'f>) -> Self {
        Self {
            min: f.min.map(InnerBoundary::from),
            max: f.max.map(InnerBoundary::from),
        }
    }
}

/// Range-filtered iterator over a [`StrTrieMap`](crate::str_trie_map::StrTrieMap),
/// in lexicographical key order.
///
/// See [`WildcardIter`](super::WildcardIter) for the two-variant
/// borrowed-vs-drained dispatch contract; this iterator uses the same
/// pattern. `None` bounds disable the corresponding side of the range.
///
/// See [`crate::iter::RangeIter`] for the underlying traversal.
pub enum RangeIter<'tm, 'p, Data: 'tm> {
    Borrowed(iter::RangeIter<'tm, 'p, Data>),
    Drained(std::vec::IntoIter<(String, &'tm Data)>),
}

impl<'tm, 'p, Data: 'tm> RangeIter<'tm, 'p, Data> {
    pub(crate) fn build_from(trie: &'tm TrieMap<Data>, filter: RangeFilter<'p>) -> Self {
        Self::Borrowed(trie.range_iter(InnerFilter::from(filter)))
    }

    /// Case-fold-aware constructor: each bound may borrow the caller's
    /// bytes ([`Cow::Borrowed`]) or own a folded buffer ([`Cow::Owned`]).
    /// When any bound owns its bytes the iterator is eagerly drained so the
    /// folded buffers need not outlive the call — see
    /// [`WildcardIter`](super::WildcardIter) for the rationale.
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
            let filter = RangeFilter {
                min: min_ref.map(|value| RangeBoundary {
                    value,
                    is_included: include_min,
                }),
                max: max_ref.map(|value| RangeBoundary {
                    value,
                    is_included: include_max,
                }),
            };
            return Self::build_from(trie, filter);
        }

        // Eagerly drain when any bound owns its bytes.
        let min_buf: Option<Vec<u8>> = min.map(|m| m.into_owned().into_bytes());
        let max_buf: Option<Vec<u8>> = max.map(|m| m.into_owned().into_bytes());
        let filter = InnerFilter {
            min: min_buf.as_deref().map(|value| InnerBoundary {
                value,
                is_included: include_min,
            }),
            max: max_buf.as_deref().map(|value| InnerBoundary {
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
