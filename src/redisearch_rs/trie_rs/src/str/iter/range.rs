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
    iter::{self, RangeBoundary as InnerBoundary, RangeFilter as InnerFilter},
    str::iter::iter_::key_to_string,
};

/// One of the bounds for a [`RangeFilter`].
///
/// Mirrors [`crate::iter::RangeBoundary`] but carries a UTF-8 [`str`] value
/// instead of raw bytes, so the [`StrTrieMap`](crate::str::StrTrieMap) invariant
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

/// Range-filtered iterator over a [`StrTrieMap`](crate::str::StrTrieMap),
/// in lexicographical key order.
///
/// See [`crate::iter::RangeIter`] for the underlying traversal.
pub struct RangeIter<'tm, 'p, Data: 'tm>(iter::RangeIter<'tm, 'p, Data>);

impl<'tm, 'p, Data: 'tm> RangeIter<'tm, 'p, Data> {
    pub(crate) fn build_from(trie: &'tm TrieMap<Data>, filter: RangeFilter<'p>) -> Self {
        Self(trie.range_iter(InnerFilter::from(filter)))
    }
}

impl<'tm, 'p, Data: 'tm> Iterator for RangeIter<'tm, 'p, Data> {
    type Item = (String, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| (key_to_string(k), v))
    }
}
