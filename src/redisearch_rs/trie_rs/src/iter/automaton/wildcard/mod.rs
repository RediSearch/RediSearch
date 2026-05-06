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
//! - [`atoms`] — shared low-level building blocks: the [`StateSet`] bitset,
//!   the [`Atom`](atoms::Atom) enum, and the `flatten` helper that turns a
//!   parsed [`WildcardPattern`] into a flat atom slice.
//! - [`nfa`] — NFA primitives (ε-closure table, `nfa_step`) and the
//!   [`WildcardNfa`] [`Automaton`](super::Automaton) implementation.
//! - [`dfa`] — the [`WildcardDfa`] [`Automaton`](super::Automaton)
//!   implementation, built via subset construction over the NFA primitives.
//! - [`fixed`] — [`FixedWildcardIter`], a concrete iterator that bypasses
//!   the [`Automaton`](super::Automaton) trait entirely for patterns
//!   with no `*`.
//!
//! ## Auto-dispatching wrapper
//!
//! [`WildcardSpecializedIter`] picks [`FixedWildcardIter`] when the pattern
//! has no `*`, otherwise an
//! [`AutomatonIter`](super::AutomatonIter) driven by [`WildcardNfa`]. The
//! runtime cost of the dispatch is one loop-invariant branch per call.
//!
//! ## Byte-level semantics
//!
//! Every implementation here matches at the byte level: `?` consumes exactly
//! one byte, not one Unicode codepoint. Codepoint-correct matching would
//! require a UTF-8 lifting pass on the underlying automata.

pub mod atoms;
pub mod dfa;
pub mod fixed;
pub mod nfa;

pub use atoms::StateSet;
pub use dfa::WildcardDfa;
pub use fixed::{FixedWildcardIter, FixedWildcardLendingIter};
pub use nfa::WildcardNfa;

use super::AutomatonIter;
use crate::iter::wildcard::WildcardIter;
use lending_iterator::prelude::*;
use wildcard::{Token, WildcardPattern};

/// Returns `true` if the parsed pattern contains any `*` token.
pub fn pattern_has_star(pattern: &WildcardPattern<'_>) -> bool {
    pattern.tokens().iter().any(|t| matches!(t, Token::Any))
}

/// Wildcard iterator that auto-selects between a specialized fixed-length
/// path, the general NFA path, and a filter-based fallback.
///
/// The variant chosen depends on the pattern shape:
///
/// - [`Fixed`](Self::Fixed) — pattern has no `*` and fits in the bitset cap.
/// - [`General`](Self::General) — pattern has `*` and fits in the bitset cap.
/// - [`Fallback`](Self::Fallback) — pattern is too long for the streaming
///   automaton (more than [`atoms::MAX_ATOMS`] atoms after flattening); we
///   route through the existing filter-based [`WildcardIter`] to avoid
///   panicking on otherwise-valid input.
pub enum WildcardSpecializedIter<'tm, Data> {
    Fixed(FixedWildcardIter<'tm, Data>),
    General(AutomatonIter<'tm, Data, WildcardNfa>),
    Fallback(WildcardIter<'tm, Data>),
}

impl<'tm, Data> WildcardSpecializedIter<'tm, Data> {
    pub(crate) fn advance(&mut self) -> Option<&'tm Data> {
        match self {
            Self::Fixed(it) => it.advance(),
            Self::General(it) => it.advance(),
            Self::Fallback(it) => it.advance(),
        }
    }

    pub(crate) fn key(&self) -> &[u8] {
        match self {
            Self::Fixed(it) => it.key(),
            Self::General(it) => it.key(),
            Self::Fallback(it) => it.key(),
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
