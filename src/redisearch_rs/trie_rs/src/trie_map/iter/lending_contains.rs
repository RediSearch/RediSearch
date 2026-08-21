/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::time::Instant;

use super::ContainsIter;
use lending_iterator::prelude::*;

/// Iterates over the entries of a [`TrieMap`](crate::TrieMap) that contain the target fragment,
/// in lexicographical order.
///
/// Unlike [`ContainsIter`], this iterator lets you borrow the current key, rather than having to clone it.
pub struct ContainsLendingIter<'tm, Data>(ContainsIter<'tm, Data>);

impl<'tm, Data> From<ContainsIter<'tm, Data>> for ContainsLendingIter<'tm, Data> {
    fn from(iter: ContainsIter<'tm, Data>) -> Self {
        ContainsLendingIter(iter)
    }
}

impl<'tm, Data> ContainsLendingIter<'tm, Data> {
    /// Set timeout
    pub fn set_timeout(&mut self, timeout: Option<Instant>) {
        self.0.set_timeout(timeout)
    }
}

// See `LendingIter` for why this is a `LendingIterator` rather than an `Iterator`.
#[gat]
impl<'tm, Data> LendingIterator for ContainsLendingIter<'tm, Data> {
    type Item<'next>
    where
        Self: 'next,
    = (&'next [u8], &'tm Data);

    fn next(&mut self) -> Option<Self::Item<'_>> {
        let item = self.0.advance()?;
        Some((self.0.key(), item))
    }
}
