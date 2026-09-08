/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::time::Instant;

use crate::automaton::wildcard::WildcardNfa;
use crate::trie_map::node::Node;

use super::{
    AutomatonIter, Iter, LendingIter,
    filter::{FilterOutcome, TraversalFilter},
};
use lending_iterator::prelude::*;
use rqe_wildcard::{MatchOutcome, WildcardPattern};

/// Per-key filter-based wildcard iterator. Public-named because it
/// is wrapped into a [`WildcardIter`]
/// (via its `From` impl) by the wildcard dispatcher.
pub struct WildcardFilterIter<'tm, 'p, Data>(Iter<'tm, Data, WildcardFilter<'p>>);

impl<'tm, 'p, Data> WildcardFilterIter<'tm, 'p, Data> {
    pub(crate) fn new(root: Option<&'tm Node<Data>>, pattern: WildcardPattern<'p>) -> Self {
        let iter = match root {
            Some(root) => {
                // If the first portion of the pattern is a literal, we can jump directly
                // to the subtree of the trie containing the terms under that prefix
                // (if there are any).
                if let Some(rqe_wildcard::Token::Literal(lit)) = pattern.tokens().first() {
                    match root.find_root_for_prefix(lit) {
                        Some((subroot, subroot_prefix)) => Iter::new(Some(subroot), subroot_prefix),
                        None => Iter::empty(),
                    }
                } else {
                    Iter::new(Some(root), vec![])
                }
            }
            None => Iter::empty(),
        }
        .traversal_filter(WildcardFilter(pattern));
        Self(iter)
    }

    /// Set timeout
    pub(crate) fn set_timeout(&mut self, timeout: Option<Instant>) {
        self.0.set_timeout(timeout);
    }

    /// Advance to the next matching entry; returns a reference to its data.
    ///
    /// Mirrors [`AutomatonIter::advance`] so that the auto-dispatching
    /// [`WildcardIter`] can plug a filter-based
    /// fallback into its `advance` / `key` interface without special-casing.
    pub(crate) fn advance(&mut self) -> Option<&'tm Data> {
        self.0.advance()
    }

    /// The current key — the concatenation of labels from the trie root to
    /// the node yielded by the most recent [`Self::advance`] call.
    pub(crate) fn key(&self) -> &[u8] {
        self.0.key()
    }
}

impl<'tm, 'p, Data> Iterator for WildcardFilterIter<'tm, 'p, Data> {
    type Item = (Vec<u8>, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl<'tm, 'p, Data> From<WildcardFilterIter<'tm, 'p, Data>>
    for LendingIter<'tm, Data, WildcardFilter<'p>>
{
    fn from(iter: WildcardFilterIter<'tm, 'p, Data>) -> Self {
        iter.0.into()
    }
}

/// Wildcard iterator that auto-selects the most efficient backend for a
/// given pattern, as picked by [`WildcardBackend::for_pattern`]:
///
/// - ≤ 63 atoms → [`WildcardNfa<u64>`].
/// - 64..=127 atoms → [`WildcardNfa<u128>`].
/// - ≥ 128 atoms → the filter-based [`WildcardFilterIter`] (a per-key matcher
///   that uses SIMD `memcmp` over each literal token). Wider bitsets and
///   a sparse-set automaton were prototyped for this range and lost on
///   every real workload — per-byte NFA overhead grows with state width
///   while the filter's per-key `memcmp` cost stays roughly flat in
///   literal length.
///
/// The dispatch is a single branch per [`WildcardIter`] method call.
pub enum WildcardIter<'tm, 'p, Data> {
    /// `u64`-backed NFA — pattern has ≤ 63 atoms.
    U64(AutomatonIter<'tm, Data, WildcardNfa<'p, u64>>),
    /// `u128`-backed NFA — pattern has 64..=127 atoms.
    U128(AutomatonIter<'tm, Data, WildcardNfa<'p, u128>>),
    /// Filter-based fallback — pattern has ≥ 128 atoms.
    Filter(WildcardFilterIter<'tm, 'p, Data>),
}

impl<'tm, 'p, Data> WildcardIter<'tm, 'p, Data> {
    pub(crate) fn advance(&mut self) -> Option<&'tm Data> {
        match self {
            Self::U64(it) => it.advance(),
            Self::U128(it) => it.advance(),
            Self::Filter(it) => it.advance(),
        }
    }

    pub(crate) fn key(&self) -> &[u8] {
        match self {
            Self::U64(it) => it.key(),
            Self::U128(it) => it.key(),
            Self::Filter(it) => it.key(),
        }
    }

    /// Set timeout
    pub fn set_timeout(&mut self, timeout: Option<Instant>) {
        match self {
            Self::U64(it) => it.set_timeout(timeout),
            Self::U128(it) => it.set_timeout(timeout),
            Self::Filter(it) => it.set_timeout(timeout),
        }
    }
}

/// The different supported backends for our wildcard iterator.
pub enum WildcardBackend {
    /// `u64` bitset — covers patterns with ≤ 63 atoms.
    U64,
    /// `u128` bitset — covers patterns with ≤ 127 atoms.
    U128,
    /// Filter-based fallback — patterns with ≥ 128 atoms. The NFA's
    /// per-byte overhead at wider state sizes outweighs the trie's
    /// prefix-sharing advantage versus the per-key filter, so we hand
    /// these patterns to [`WildcardFilterIter`] directly.
    Filter,
}

impl WildcardBackend {
    /// Pick the most efficient matching backend for a given pattern's atom
    /// count. The dispatcher uses this to route to a fully-monomorphized hot
    /// path for each variant.
    pub const fn for_pattern(pattern: &WildcardPattern<'_>) -> Self {
        // The state must reach position `accept = n_atoms`, so we need
        // capacity for `n_atoms + 1` distinct positions.
        let positions_needed = pattern.atom_count() + 1;
        if positions_needed <= 64 {
            Self::U64
        } else if positions_needed <= 128 {
            Self::U128
        } else {
            Self::Filter
        }
    }
}

impl<'tm, 'p, Data> Iterator for WildcardIter<'tm, 'p, Data> {
    type Item = (Vec<u8>, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        let data = self.advance()?;
        Some((self.key().to_vec(), data))
    }
}

/// Lending-iterator wrapper for [`WildcardIter`].
pub struct WildcardLendingIter<'tm, 'p, Data>(WildcardIter<'tm, 'p, Data>);

impl<'tm, 'p, Data> WildcardLendingIter<'tm, 'p, Data> {
    /// Set timeout
    pub fn set_timeout(&mut self, timeout: Option<Instant>) {
        self.0.set_timeout(timeout);
    }
}

impl<'tm, 'p, Data> From<WildcardIter<'tm, 'p, Data>> for WildcardLendingIter<'tm, 'p, Data> {
    fn from(iter: WildcardIter<'tm, 'p, Data>) -> Self {
        Self(iter)
    }
}

#[gat]
impl<'tm, 'p, Data> LendingIterator for WildcardLendingIter<'tm, 'p, Data> {
    type Item<'next>
    where
        Self: 'next,
    = (&'next [u8], &'tm Data);

    fn next(&mut self) -> Option<Self::Item<'_>> {
        let data = self.0.advance()?;
        Some((self.0.key(), data))
    }
}

/// A [`TraversalFilter`] that keeps only keys matching the given [`WildcardPattern`].
pub struct WildcardFilter<'p>(WildcardPattern<'p>);

impl TraversalFilter for WildcardFilter<'_> {
    fn filter(&self, key: &[u8]) -> FilterOutcome {
        match self.0.matches(key) {
            MatchOutcome::Match => FilterOutcome {
                yield_current: true,
                // If the pattern matches inputs of a given length,
                // and the current key is a match, it follows that
                // it won't match any of its descendants, since they'll be
                // at least one character longer.
                visit_descendants: self.0.expected_length().is_none(),
            },
            MatchOutcome::PartialMatch => FilterOutcome {
                yield_current: false,
                visit_descendants: true,
            },
            MatchOutcome::NoMatch => FilterOutcome {
                yield_current: false,
                visit_descendants: false,
            },
        }
    }
}
