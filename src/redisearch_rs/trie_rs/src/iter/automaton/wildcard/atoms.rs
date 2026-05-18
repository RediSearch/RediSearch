/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Low-level building blocks for the wildcard NFA: the [`Atom`] enum, the
//! [`NfaBitSet`] trait, and the `flatten` helper that turns a parsed
//! pattern into an atom slice. See the [parent module
//! doc](super) for the NFA primer this fits into.
//!
//! The bitset is monomorphised twice — once for `u64` (≤ 63 atoms) and
//! once for `u128` (≤ 127 atoms). Each `NfaBitSet` impl is a few
//! single-register operations, so every method on
//! [`super::nfa::WildcardNfa<S>`] inlines into a tight bitwise hot loop.
//! [`super::WildcardBackend::for_pattern`] picks the width; patterns
//! past 127 atoms skip the NFA entirely and route to
//! [`crate::iter::WildcardIter`].

use wildcard::{Token, WildcardPattern};

/// Bitmask of active NFA positions, with bit *i* set iff position *i*
/// (in the sense documented on the [parent module](super)) is in the
/// current active set. The full set must fit in the type — implementers
/// reserve capacity for positions `0..=n_atoms` (the `+1` is the accept
/// position).
///
/// Methods mirror standard set operations. Width-specific implementations
/// are provided for `u64` and `u128`.
pub trait NfaBitSet: Clone + Eq {
    /// Empty set, sized for positions `0..=n_atoms`.
    fn empty(n_atoms: usize) -> Self;

    /// Set containing only `pos`, sized for positions `0..=n_atoms`.
    fn singleton(n_atoms: usize, pos: usize) -> Self;

    /// Reset every bit to zero. Used to recycle a scratch set across
    /// successive transitions inside [`super::nfa::nfa_step_into`].
    fn clear(&mut self);

    /// Add `pos` to the set.
    fn insert(&mut self, pos: usize);

    /// Whether `pos` is in the set.
    fn contains(&self, pos: usize) -> bool;

    /// Whether the set is empty (the NFA has died and the subtree can
    /// be pruned).
    fn is_empty(&self) -> bool;

    /// `self |= other` (set union). Both operands must be the same width.
    fn union_in_place(&mut self, other: &Self);

    /// Position of the lone set bit. Caller must guarantee the set has
    /// exactly one element; used by the no-`*` (fixed-length) fast path
    /// in [`super::nfa::WildcardNfa`], where the active set is always a
    /// singleton.
    fn singleton_pos(&self) -> usize;

    /// Iterate active positions in ascending order.
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

/// One position in the pattern. Each atom consumes exactly one input
/// byte, *except* [`Atom::Any`] which can self-loop on input or be
/// skipped via an ε-transition (see the [parent module
/// doc](super)). Atoms are laid out flat in a `Vec<Atom>`; position `i`
/// is "currently looking at atom `i`".
#[derive(Clone, Copy, Debug)]
pub(super) enum Atom {
    /// Literal byte; the input must match exactly to advance.
    Byte(u8),
    /// `?` — matches any single input byte.
    One,
    /// `*` — matches zero or more bytes; can self-loop or be skipped.
    Any,
}

/// Flatten a parsed pattern into a contiguous atom sequence.
///
/// A literal token expands to one [`Atom::Byte`] per byte (so the
/// per-byte hot loop just indexes `atoms[pos]` to know whether to
/// match); `?` and `*` become one atom each. The final length is
/// [`WildcardPattern::atom_count`], which we use to size the `Vec`
/// up front.
pub(super) fn flatten(pattern: &WildcardPattern<'_>) -> Vec<Atom> {
    let mut atoms = Vec::with_capacity(pattern.atom_count());
    for token in pattern.tokens() {
        match token {
            Token::Literal(bytes) => atoms.extend(bytes.iter().copied().map(Atom::Byte)),
            Token::One => atoms.push(Atom::One),
            Token::Any => atoms.push(Atom::Any),
        }
    }
    atoms
}
