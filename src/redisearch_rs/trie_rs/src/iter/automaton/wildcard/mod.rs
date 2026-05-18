/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Wildcard pattern matching as a streaming NFA over a trie.
//!
//! Read this module doc first if you're new to NFAs — every later doc
//! comment in this submodule assumes the vocabulary and worked example
//! below.
//!
//! # Why an NFA?
//!
//! A wildcard pattern like `*ab*` is *non-deterministic* to match: when
//! the matcher reads a byte from the input, it may be doing any of the
//! following at the same time —
//!
//! - still consuming bytes for the leading `*` (any prefix);
//! - about to start the literal `ab`;
//! - already partway through `ab`;
//! - already past `ab` and consuming bytes for the trailing `*`.
//!
//! A *deterministic* automaton (DFA) would have to flatten that
//! "what could have happened so far" set into a single state, which can
//! blow up exponentially in pattern size. A *non-*deterministic finite
//! automaton (NFA) instead lets all those possibilities coexist: the
//! active state is a **set** of pattern positions, each representing one
//! in-progress match attempt. Reading a byte advances some, kills others,
//! and possibly spawns new ones.
//!
//! # Positions, atoms, and ε-closure
//!
//! The parser breaks a pattern into a flat sequence of [`atoms::Atom`]s
//! — one of `Byte(b)`, `One` (`?`), or `Any` (`*`). With `N` atoms there
//! are `N + 1` numbered **positions** along the pattern:
//!
//! ```text
//!   pattern:  *  a  b  *
//!   atoms:   [Any][Byte a][Byte b][Any]
//!   pos:    0    1       2       3    4
//!                                     ^ accept (no atom; sink)
//! ```
//!
//! "Position `i` is active" means "we've successfully consumed the first
//! `i` atoms; we're about to try atom `i`". Position `N` is **accept** —
//! it has no atom and signals that the whole pattern matched.
//!
//! Most transitions consume exactly one input byte:
//!
//! - From position `i` with atom `Byte(b)` on byte `c`: advance to
//!   `i + 1` iff `b == c`; otherwise this branch dies.
//! - From position `i` with atom `One`: advance to `i + 1` on any byte.
//! - From position `i` with atom `Any`: stay at `i` (consume any byte
//!   while still inside the `*`).
//!
//! The `Any` atom has a second, special move: it can be **skipped
//! without consuming a byte** — it represents *zero* or more bytes. That
//! free move is called an **ε-transition** (ε = empty input). After any
//! regular transition we follow every reachable ε-move and add the
//! resulting positions to the active set; the closure of `{i}` under
//! these moves is the **ε-closure of position `i`**.
//!
//! For `*ab*`, the ε-closure of `{0}` is `{0, 1}`: you can either stay
//! at the leading `*` or skip past it onto position 1 (the `a` atom).
//! [`nfa::build_epsilon_table`] precomputes these closures once per
//! pattern so the per-byte loop just reads a row.
//!
//! # Worked example: matching `*ab*` against `xaab`
//!
//! State evolution (`{...}` = the active set of positions):
//!
//! ```text
//!   start:        {0, 1}    ε-closure of {0}; ready to read a byte
//!   read 'x':     {0, 1}    0 self-loops (and ε-closes back to {0, 1});
//!                           1 dies because 'x' ≠ 'a'
//!   read 'a':     {0, 1, 2} 0 self-loops; 1 advances to 2 (literal match)
//!   read 'a':     {0, 1, 2} 0 self-loops; 1 → 2 again; the previous 2
//!                           dies because 'a' ≠ 'b'
//!   read 'b':     {0, 1, 3} 0 self-loops; 1 dies; 2 advances to 3,
//!                           whose ε-closure {3, 4} pulls in accept
//!                           → final set is {0, 1, 3, 4}
//! ```
//!
//! Position 4 (accept) is in the set, so `xaab` matches `*ab*`.
//!
//! # Encoding the active set as a bitmask
//!
//! Because positions are dense integers `0..=N`, the natural
//! representation of the active set is a bitmask: bit `i` is set iff
//! position `i` is in the set. Set union (used to merge ε-closures and
//! transition targets) is a bitwise OR; emptiness is a single comparison;
//! "is position `i` active?" is a shift-and-mask. For `N ≤ 63` the whole
//! set fits in a `u64`; for `N ≤ 127`, a `u128`. [`nfa::WildcardNfa<S>`]
//! is generic over the bitset type via the [`NfaBitSet`] trait so the
//! same NFA code monomorphises against both widths.
//!
//! # How the trie iterator uses the NFA
//!
//! The driving loop lives in [`super::AutomatonIter`]: a DFS over the
//! trie that carries the NFA state on its stack and calls
//! [`Automaton::step_all`](super::Automaton::step_all) over each edge
//! label. At each node the driver consults
//! [`Automaton::classify`](super::Automaton::classify) (returning a
//! [`StateClass`](super::StateClass)) to decide whether to yield the key
//! and whether to recurse. Crucially, each byte stored in the trie is
//! processed *at most once per query*: shared prefixes are stepped once
//! at the branching node and the resulting state is reused for every
//! descendant.
//!
//! # Backend dispatch
//!
//! [`WildcardSpecializedIter`] picks the most efficient backend for the
//! pattern's atom count at iterator-construction time:
//!
//! - ≤ 63 atoms → [`WildcardNfa<u64>`].
//! - 64..=127 atoms → [`WildcardNfa<u128>`].
//! - ≥ 128 atoms → the filter-based [`WildcardIter`] (a per-key matcher
//!   that uses SIMD `memcmp` over each literal token). Wider bitsets and
//!   a sparse-set automaton were prototyped for this range and lost on
//!   every real workload — per-byte NFA overhead grows with state width
//!   while the filter's per-key `memcmp` cost stays roughly flat in
//!   literal length.
//!
//! The dispatch is a single branch per [`WildcardSpecializedIter`]
//! method call.
//!
//! # Layout
//!
//! - [`atoms`] — the [`Atom`](atoms::Atom) enum, the [`NfaBitSet`] trait
//!   with `u64`/`u128` impls, and the `flatten` helper that turns a
//!   parsed [`WildcardPattern`] into a flat atom slice.
//! - [`nfa`] — [`WildcardNfa<S>`](nfa::WildcardNfa), the NFA itself.
//!
//! # Byte-level semantics
//!
//! Every implementation here matches at the byte level: `?` consumes
//! exactly one byte, not one Unicode codepoint. Codepoint-correct
//! matching would require a UTF-8 lifting pass on the underlying
//! automaton.

pub mod atoms;
pub mod nfa;

pub use atoms::NfaBitSet;
pub use nfa::WildcardNfa;

use super::AutomatonIter;
use crate::iter::wildcard::WildcardIter;
use lending_iterator::prelude::*;
use wildcard::WildcardPattern;

/// Wildcard iterator that auto-selects the most efficient backend for a
/// given pattern. See the module documentation for the selection criteria.
pub enum WildcardSpecializedIter<'tm, Data> {
    /// `u64`-backed NFA — pattern has ≤ 63 atoms.
    U64(AutomatonIter<'tm, Data, WildcardNfa<u64>>),
    /// `u128`-backed NFA — pattern has 64..=127 atoms.
    U128(AutomatonIter<'tm, Data, WildcardNfa<u128>>),
    /// Filter-based fallback — pattern has ≥ 128 atoms.
    Filter(WildcardIter<'tm, Data>),
}

impl<'tm, Data> WildcardSpecializedIter<'tm, Data> {
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
