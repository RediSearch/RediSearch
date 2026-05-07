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
//!   with implementations for `u64`, `u128`, and [`InlineStateSet`]; the
//!   [`Atom`](atoms::Atom) enum; and the `flatten` helper that turns a
//!   parsed [`WildcardPattern`] into a flat atom slice.
//! - [`nfa`] — generic [`WildcardNfa<S>`] [`Automaton`](super::Automaton)
//!   implementation parameterized over the bitset; used for the small
//!   stack-resident bitset classes.
//! - [`sparse`] — [`WildcardSparseNfa`], a sparse-set automaton used
//!   instead of bitset variants whenever a bitset would have to live on
//!   the heap. Recycles two scratch buffers across `step_all` calls so
//!   per-byte work allocates nothing.
//! - [`fixed`] — [`FixedWildcardIter`], a concrete iterator that bypasses
//!   the [`Automaton`](super::Automaton) trait entirely for patterns
//!   with no `*`.
//!
//! ## Auto-dispatching wrapper
//!
//! [`WildcardSpecializedIter`] picks the most efficient backend at NFA
//! compile time:
//!
//! - No `*` → [`FixedWildcardIter`].
//! - `*` and ≤ 63 atoms → NFA backed by `u64`.
//! - `*` and 64..=127 atoms → NFA backed by `u128`.
//! - `*` and 128..=255 atoms → NFA backed by [`InlineStateSet`].
//! - `*` and ≥ 256 atoms → [`WildcardSparseNfa`].
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
pub mod fixed;
pub mod nfa;
pub mod sparse;

pub use atoms::{InlineStateSet, NfaBitSet};
pub use fixed::{FixedWildcardIter, FixedWildcardLendingIter};
pub use nfa::WildcardNfa;
pub use sparse::{SparseStateSet, WildcardSparseNfa};

use super::AutomatonIter;
use atoms::count_atoms;
use lending_iterator::prelude::*;
use wildcard::{Token, WildcardPattern};

/// Returns `true` if the parsed pattern contains any `*` token.
pub fn pattern_has_star(pattern: &WildcardPattern<'_>) -> bool {
    pattern.tokens().iter().any(|t| matches!(t, Token::Any))
}

/// Wildcard iterator that auto-selects the most efficient backend at NFA
/// compile time. See the module documentation for the selection criteria.
pub enum WildcardSpecializedIter<'tm, Data> {
    Fixed(FixedWildcardIter<'tm, Data>),
    GeneralU64(AutomatonIter<'tm, Data, WildcardNfa<u64>>),
    GeneralU128(AutomatonIter<'tm, Data, WildcardNfa<u128>>),
    GeneralInline(AutomatonIter<'tm, Data, WildcardNfa<InlineStateSet>>),
    GeneralSparse(AutomatonIter<'tm, Data, WildcardSparseNfa>),
}

impl<'tm, Data> WildcardSpecializedIter<'tm, Data> {
    pub(crate) fn advance(&mut self) -> Option<&'tm Data> {
        match self {
            Self::Fixed(it) => it.advance(),
            Self::GeneralU64(it) => it.advance(),
            Self::GeneralU128(it) => it.advance(),
            Self::GeneralInline(it) => it.advance(),
            Self::GeneralSparse(it) => it.advance(),
        }
    }

    pub(crate) fn key(&self) -> &[u8] {
        match self {
            Self::Fixed(it) => it.key(),
            Self::GeneralU64(it) => it.key(),
            Self::GeneralU128(it) => it.key(),
            Self::GeneralInline(it) => it.key(),
            Self::GeneralSparse(it) => it.key(),
        }
    }
}

/// Pick the most efficient state representation that fits the pattern's
/// atom count. The dispatcher uses this in conjunction with one of the
/// concrete NFA constructors so each variant gets a fully-monomorphized
/// hot path.
pub enum BitSetClass {
    /// `u64` bitset — covers patterns with ≤ 63 atoms.
    U64,
    /// `u128` bitset — covers patterns with ≤ 127 atoms.
    U128,
    /// [`InlineStateSet`] (`[u64; 4]` on the stack) — covers patterns with
    /// ≤ 255 atoms.
    Inline,
    /// [`WildcardSparseNfa`] — sparse-set automaton, used for any pattern
    /// past the largest stack-resident bitset class.
    Sparse,
}

impl BitSetClass {
    pub fn for_pattern(pattern: &WildcardPattern<'_>) -> Self {
        // The state must reach position `accept = n_atoms`, so we need
        // capacity for `n_atoms + 1` distinct positions.
        let positions_needed = count_atoms(pattern) + 1;
        if positions_needed <= 64 {
            Self::U64
        } else if positions_needed <= 128 {
            Self::U128
        } else if positions_needed <= 256 {
            Self::Inline
        } else {
            Self::Sparse
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
