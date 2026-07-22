/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Low-level building blocks for the wildcard NFA: the [`Atom`] enum and
//! the `flatten` helper that turns a parsed pattern into an atom slice.
//! See the [parent module doc](super) for the NFA primer this fits into.

use rqe_wildcard::{Token, WildcardPattern};

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
