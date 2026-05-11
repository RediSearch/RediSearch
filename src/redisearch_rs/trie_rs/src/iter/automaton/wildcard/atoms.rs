/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared low-level building blocks for wildcard automata.
//!
//! [`super::nfa`] consumes a flat sequence of [`Atom`]s produced by
//! [`flatten`] and tracks active positions in any type that implements the
//! [`NfaBitSet`] trait.
//!
//! Two stack-resident bitset implementations exist, sized for different
//! pattern lengths and selected at NFA compile time so every variant has a
//! fully monomorphized hot path:
//!
//! - [`u64`] — covers up to 63 atoms; single-register operations.
//! - [`u128`] — covers up to 127 atoms; two-register operations.
//!
//! Patterns beyond 127 atoms fall back to the per-key
//! [`crate::iter::wildcard::WildcardIter`] — its SIMD `memcmp` over each
//! literal token beats the wider NFA state representations once the
//! pattern's atom count outgrows a single register pair.
//!
//! Callers select the backend via [`super::WildcardBackend::for_pattern`].

use wildcard::{Token, WildcardPattern};

/// A bitset of NFA active positions.
///
/// Implementations track which atom positions are currently active during
/// NFA simulation. Positions are `0..=n_atoms` where `n_atoms` is the accept
/// position, so capacity must be at least `n_atoms + 1` bits.
pub trait NfaBitSet: Clone + Eq {
    /// Empty state pre-sized to hold positions `0..=n_atoms`.
    fn empty(n_atoms: usize) -> Self;

    /// State with only `pos` active, sized to hold positions `0..=n_atoms`.
    fn singleton(n_atoms: usize, pos: usize) -> Self;

    /// Reset every bit to zero, keeping the storage layout. Used to recycle
    /// a scratch buffer across calls to [`super::nfa::nfa_step_into`].
    fn clear(&mut self);

    /// Set bit `pos`.
    fn insert(&mut self, pos: usize);

    /// Whether bit `pos` is set.
    fn contains(&self, pos: usize) -> bool;

    /// Whether no bits are set.
    fn is_empty(&self) -> bool;

    /// Union `other` into `self` in place. Both must have the same width.
    fn union_in_place(&mut self, other: &Self);

    /// Position of the single set bit, assuming exactly one is set. Used by
    /// the fixed-length-pattern fast path.
    fn singleton_pos(&self) -> usize;

    /// Iterate over the positions of every set bit, in ascending order.
    fn iter(&self) -> impl Iterator<Item = usize> + '_;
}

impl NfaBitSet for u64 {
    #[inline]
    fn empty(_: usize) -> Self {
        0
    }
    #[inline]
    fn singleton(_: usize, pos: usize) -> Self {
        debug_assert!(pos < 64);
        1u64 << pos
    }
    #[inline]
    fn clear(&mut self) {
        *self = 0;
    }
    #[inline]
    fn insert(&mut self, pos: usize) {
        debug_assert!(pos < 64);
        *self |= 1u64 << pos;
    }
    #[inline]
    fn contains(&self, pos: usize) -> bool {
        debug_assert!(pos < 64);
        (*self >> pos) & 1 == 1
    }
    #[inline]
    fn is_empty(&self) -> bool {
        *self == 0
    }
    #[inline]
    fn union_in_place(&mut self, other: &Self) {
        *self |= *other;
    }
    #[inline]
    fn singleton_pos(&self) -> usize {
        self.trailing_zeros() as usize
    }
    #[inline]
    fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        let mut bits = *self;
        std::iter::from_fn(move || {
            if bits == 0 {
                None
            } else {
                let pos = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                Some(pos)
            }
        })
    }
}

impl NfaBitSet for u128 {
    #[inline]
    fn empty(_: usize) -> Self {
        0
    }
    #[inline]
    fn singleton(_: usize, pos: usize) -> Self {
        debug_assert!(pos < 128);
        1u128 << pos
    }
    #[inline]
    fn clear(&mut self) {
        *self = 0;
    }
    #[inline]
    fn insert(&mut self, pos: usize) {
        debug_assert!(pos < 128);
        *self |= 1u128 << pos;
    }
    #[inline]
    fn contains(&self, pos: usize) -> bool {
        debug_assert!(pos < 128);
        (*self >> pos) & 1 == 1
    }
    #[inline]
    fn is_empty(&self) -> bool {
        *self == 0
    }
    #[inline]
    fn union_in_place(&mut self, other: &Self) {
        *self |= *other;
    }
    #[inline]
    fn singleton_pos(&self) -> usize {
        self.trailing_zeros() as usize
    }
    #[inline]
    fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        let mut bits = *self;
        std::iter::from_fn(move || {
            if bits == 0 {
                None
            } else {
                let pos = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                Some(pos)
            }
        })
    }
}

/// Flattened pattern atom — one byte of input is consumed per atom (except
/// [`Atom::Any`] which can self-loop or skip).
#[derive(Clone, Copy, Debug)]
pub(super) enum Atom {
    /// Literal byte that must match exactly.
    Byte(u8),
    /// Any single byte (`?`).
    One,
    /// Zero-or-more bytes (`*`).
    Any,
}

/// Compile a parsed wildcard pattern to a flat atom sequence.
pub(super) fn flatten(pattern: &WildcardPattern<'_>) -> Vec<Atom> {
    let mut atoms = Vec::new();
    for token in pattern.tokens() {
        match token {
            Token::Literal(bytes) => atoms.extend(bytes.iter().copied().map(Atom::Byte)),
            Token::One => atoms.push(Atom::One),
            Token::Any => atoms.push(Atom::Any),
        }
    }
    atoms
}

/// Count atoms a pattern would produce without allocating — used by the
/// dispatcher to pick the right [`NfaBitSet`] size.
pub(super) fn count_atoms(pattern: &WildcardPattern<'_>) -> usize {
    let mut n = 0;
    for token in pattern.tokens() {
        n += match token {
            Token::Literal(bytes) => bytes.len(),
            Token::One | Token::Any => 1,
        };
    }
    n
}
