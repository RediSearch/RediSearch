/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The [`NfaBitSet`] trait and its `u64`/`u128` implementations: the
//! bitmask encoding of the wildcard NFA's active position set. See the
//! [parent module doc](super) for the NFA primer this fits into.
//!
//! Each `NfaBitSet` impl is a few
//! single-register operations, so every method on
//! [`super::nfa::WildcardNfa<S>`] inlines into a tight bitwise hot loop.
//! [`super::WildcardBackend::for_pattern`] picks the width; patterns
//! past 127 atoms exceed `u128`, the widest bitset.

/// Bitmask of active NFA positions, with bit *i* set iff position *i*
/// is in the current active set. The full set must fit in the type — implementers
/// reserve capacity for positions `0..=n_atoms` (the `+1` is the accept
/// position).
///
/// Methods mirror standard set operations.
pub trait NfaBitSet: Clone + Eq {
    /// Number of distinct positions this bitset can hold. A pattern with
    /// `n` atoms uses positions `0..=n` (accept included), so the backend
    /// is only sound when `n < CAPACITY`. [`super::nfa::WildcardNfa::compile`]
    /// enforces this; the dispatcher in [`super::WildcardIter`]
    /// picks the narrowest width that fits.
    const CAPACITY: usize;

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

    /// Position of the lone set bit.
    ///
    /// If the set has more than one element, it returns the position
    /// of the highest set bit.
    fn singleton_pos(&self) -> usize;

    /// Iterate active positions in ascending order.
    fn iter(&self) -> impl Iterator<Item = usize> + '_;
}

impl NfaBitSet for u64 {
    const CAPACITY: usize = 64;

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
        debug_assert_eq!(
            self.count_ones(),
            1,
            "Invoked 'singleton_pos' on a set that contains more than one element."
        );
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
    const CAPACITY: usize = 128;

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
        debug_assert_eq!(
            self.count_ones(),
            1,
            "Invoked 'singleton_pos' on a set that contains more than one element."
        );
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
