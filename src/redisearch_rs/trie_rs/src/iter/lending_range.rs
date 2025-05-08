/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use super::RangeIter;
use lending_iterator::prelude::*;

/// Iterates over the entries of a [`TrieMap`](crate::TrieMap) between the specified `min` and `max`,
/// in lexicographical order.
///
/// Unlike [`RangeIter`], this iterator lets you borrow the current key, rather than having to clone it.
pub struct RangeLendingIter<'tm, Data>(RangeIter<'tm, Data>);

impl<'tm, Data> From<RangeIter<'tm, Data>> for RangeLendingIter<'tm, Data> {
    fn from(iter: RangeIter<'tm, Data>) -> Self {
        RangeLendingIter(iter)
    }
}

// The [`LendingIterator`] trait allows us to obtain a reference to
// the key corresponding to the value.
// The [`Iterator`] trait does not allow for its `Item` to be a reference
// to the Iterator itself.
//
// Why do we need a crate? Well: <https://sabrinajewson.org/blog/the-better-alternative-to-lifetime-gats>
#[gat]
// The 'tm lifetime parameter is not actually needless.
#[allow(clippy::needless_lifetimes)]
impl<'tm, Data> LendingIterator for RangeLendingIter<'tm, Data> {
    type Item<'next>
    where
        Self: 'next,
    = (&'next [u8], &'tm Data);

    fn next(&mut self) -> Option<Self::Item<'_>> {
        let item = self.0.advance()?;
        Some((self.0.key(), item))
    }
}
