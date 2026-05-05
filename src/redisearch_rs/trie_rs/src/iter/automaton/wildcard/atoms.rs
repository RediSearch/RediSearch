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
//! Both [`super::nfa`] and [`super::dfa`] consume a flat sequence of [`Atom`]s
//! produced by [`flatten`]; the NFA also tracks active positions in a
//! [`StateSet`] bitset.

use wildcard::{Token, WildcardPattern};

/// Maximum number of atoms supported by both automata.
///
/// One bit per position in [`StateSet`]. Patterns with more atoms will panic
/// during construction. For the typical Redis wildcard patterns this is
/// dramatically more than needed.
pub(super) const MAX_ATOMS: usize = 63;

/// A bitset tracking which NFA positions are currently active.
///
/// Positions are `0..=n_atoms` where `n_atoms` is the accept position.
///
/// Backed by a `u64`: ARM64/x86 single-instruction bit ops, and `Option<u64>`
/// fits in two registers — no stack roundtrip on
/// [`Automaton::step`](super::super::Automaton::step) returns.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Default, Debug)]
pub struct StateSet(pub(super) u64);

impl StateSet {
    pub(super) const fn empty() -> Self {
        Self(0)
    }

    pub(super) const fn singleton(pos: usize) -> Self {
        debug_assert!(pos <= MAX_ATOMS);
        Self(1u64 << pos)
    }

    pub(super) const fn insert(&mut self, pos: usize) {
        debug_assert!(pos <= MAX_ATOMS);
        self.0 |= 1u64 << pos;
    }

    pub(super) const fn contains(&self, pos: usize) -> bool {
        debug_assert!(pos <= MAX_ATOMS);
        (self.0 >> pos) & 1 == 1
    }

    pub(super) const fn is_empty(&self) -> bool {
        self.0 == 0
    }

    pub(super) const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub(super) fn iter(self) -> impl Iterator<Item = usize> {
        let mut bits = self.0;
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
    assert!(
        atoms.len() <= MAX_ATOMS,
        "wildcard pattern exceeds {MAX_ATOMS}-atom limit",
    );
    atoms
}
