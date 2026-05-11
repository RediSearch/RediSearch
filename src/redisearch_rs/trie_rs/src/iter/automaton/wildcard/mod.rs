/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Wildcard pattern matching as streaming automata.
//!
//! ## Layout
//!
//! - [`atoms`] — shared low-level building blocks: the [`NfaBitSet`] trait
//!   with implementations for `u64` and `u128`, the
//!   [`Atom`](atoms::Atom) enum, and the `flatten` helper that turns a
//!   parsed [`WildcardPattern`] into a flat atom slice.
//! - [`nfa`] — generic [`WildcardNfa<S>`] [`Automaton`](super::Automaton)
//!   implementation parameterized over the bitset.
//!
//! ## Auto-dispatching wrapper
//!
//! [`WildcardSpecializedIter`] picks the most efficient backend at NFA
//! compile time:
//!
//! - ≤ 63 atoms → NFA backed by `u64`.
//! - 64..=127 atoms → NFA backed by `u128`.
//! - ≥ 128 atoms → the filter-based [`WildcardIter`] (a per-key matcher
//!   that uses SIMD `memcmp` over each literal token). The wider bitset
//!   representations and a sparse-set automaton were tried for the
//!   ≥ 128 range, but neither beat the filter on real workloads.
//!
//! The runtime cost of the dispatch is one loop-invariant branch per
//! iterator call.
//!
//! ## Byte-level semantics
//!
//! Every implementation here matches at the byte level: `?` consumes exactly
//! one byte, not one Unicode codepoint. Codepoint-correct matching would
//! require a UTF-8 lifting pass on the underlying automata.

pub mod atoms;
pub mod nfa;

pub use atoms::NfaBitSet;
pub use nfa::WildcardNfa;

use super::AutomatonIter;
use crate::iter::wildcard::WildcardIter;
use atoms::count_atoms;
use lending_iterator::prelude::*;
use wildcard::WildcardPattern;

/// Wildcard iterator that auto-selects the most efficient backend at NFA
/// compile time. See the module documentation for the selection criteria.
pub enum WildcardSpecializedIter<'tm, Data> {
    /// `u64`-backed NFA — pattern has ≤ 63 atoms.
    GeneralU64(AutomatonIter<'tm, Data, WildcardNfa<u64>>),
    /// `u128`-backed NFA — pattern has 64..=127 atoms.
    GeneralU128(AutomatonIter<'tm, Data, WildcardNfa<u128>>),
    /// Filter-based fallback — pattern has ≥ 128 atoms.
    Filter(WildcardIter<'tm, Data>),
}

impl<'tm, Data> WildcardSpecializedIter<'tm, Data> {
    pub(crate) fn advance(&mut self) -> Option<&'tm Data> {
        match self {
            Self::GeneralU64(it) => it.advance(),
            Self::GeneralU128(it) => it.advance(),
            Self::Filter(it) => it.advance(),
        }
    }

    pub(crate) fn key(&self) -> &[u8] {
        match self {
            Self::GeneralU64(it) => it.key(),
            Self::GeneralU128(it) => it.key(),
            Self::Filter(it) => it.key(),
        }
    }
}

/// Pick the most efficient matching backend for a given pattern's atom
/// count. The dispatcher uses this to route to a fully-monomorphized hot
/// path for each variant.
pub enum WildcardBackend {
    /// `u64` bitset — covers patterns with ≤ 63 atoms.
    U64,
    /// `u128` bitset — covers patterns with ≤ 127 atoms.
    U128,
    /// Filter-based fallback — patterns with ≥ 128 atoms. The NFA's
    /// per-byte overhead at wider state sizes outweighs the trie's
    /// prefix-sharing advantage versus the per-key filter, so we hand
    /// these patterns to [`WildcardIter`] directly.
    Filter,
}

impl WildcardBackend {
    pub fn for_pattern(pattern: &WildcardPattern<'_>) -> Self {
        // The state must reach position `accept = n_atoms`, so we need
        // capacity for `n_atoms + 1` distinct positions.
        let positions_needed = count_atoms(pattern) + 1;
        if positions_needed <= 64 {
            Self::U64
        } else if positions_needed <= 128 {
            Self::U128
        } else {
            Self::Filter
        }
    }
}

impl<'tm, Data> Iterator for WildcardSpecializedIter<'tm, Data> {
    type Item = (Vec<u8>, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        let data = self.advance()?;
        Some((self.key().to_vec(), data))
    }
}

/// Lending-iterator wrapper for [`WildcardSpecializedIter`].
pub struct WildcardSpecializedLendingIter<'tm, Data>(WildcardSpecializedIter<'tm, Data>);

impl<'tm, Data> From<WildcardSpecializedIter<'tm, Data>>
    for WildcardSpecializedLendingIter<'tm, Data>
{
    fn from(iter: WildcardSpecializedIter<'tm, Data>) -> Self {
        Self(iter)
    }
}

#[gat]
impl<'tm, Data> LendingIterator for WildcardSpecializedLendingIter<'tm, Data> {
    type Item<'next>
    where
        Self: 'next,
    = (&'next [u8], &'tm Data);

    fn next(&mut self) -> Option<Self::Item<'_>> {
        let data = self.0.advance()?;
        Some((self.0.key(), data))
    }
}
