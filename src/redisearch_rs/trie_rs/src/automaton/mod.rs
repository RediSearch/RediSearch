/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Streaming byte automatons.
//!
//! An [`Automaton`] is a byte-streaming state machine: from a
//! [`start`](Automaton::start) state it is advanced one byte
//! ([`step`](Automaton::step)) or one byte slice
//! ([`step_all`](Automaton::step_all)) at a time, and every reached
//! state [`classify`](Automaton::classify)-es into a [`StateClass`]
//! that says whether the bytes consumed so far form an accepted key
//! and whether some extension of them still might.
//!
//! The contract is shaped for incremental traversal of a trie, where
//! the automaton state reached at a node is reused for every
//! descendant: input arrives as edge labels — chunks split at
//! arbitrary byte boundaries — states are cloned at branch points, and
//! the [`StateClass`] short-circuit variants and
//! [`literal_prefix`](Automaton::literal_prefix) allow whole subtrees
//! to be pruned or accepted without stepping through them.
//!
//! **Start with the [`wildcard`] module doc for a worked NFA primer** —
//! it explains positions, ε-closure, and the bitset state encoding with
//! a concrete `*ab*` against `xaab` trace.
//!
//! ## Byte-level and UTF-8-aware automatons
//!
//! The automatons in [`wildcard`] operate on raw bytes and know nothing
//! about key encoding. The remaining modules are Unicode-aware: they
//! reassemble codepoints from the byte stream (via
//! [`CodepointDecoder`](utf8::CodepointDecoder), handling chunks
//! that split a codepoint) and match under per-codepoint case folding,
//! which only makes sense for UTF-8 keys. Keys that are not valid
//! UTF-8 never match.
//!
//! ## Layout
//!
//! - This module: the [`Automaton`] trait and the [`StateClass`] enum.
//! - [`wildcard`]: the byte-level wildcard NFA, its atom encoding, and
//!   the [`NfaBitSet`] state representation.
//! - [`case_fold`]: [`CaseFoldExact`] — case-insensitive exact match.
//! - [`levenshtein`]: [`CaseFoldLevenshtein`] — case-insensitive
//!   Levenshtein distance in codepoints, as a DP row.
//! - [`levenshtein_nfa`]: [`CaseFoldLevenshteinNfa`] — the same matching
//!   model as a bit-parallel NFA, for needles and distances within its
//!   word-width bounds.
//! - [`codepoint_wildcard`]: [`CodepointWildcard`] — wildcard matching
//!   where `*` matches any run of codepoints and `?` consumes exactly
//!   one codepoint (rather than one byte), plus its automaton form
//!   ([`CodepointWildcardNfa`]).

pub mod case_fold;
pub mod codepoint_wildcard;
pub mod levenshtein;
pub mod levenshtein_nfa;
mod utf8;
pub mod wildcard;

pub use case_fold::CaseFoldExact;
pub use codepoint_wildcard::{CodepointWildcard, CodepointWildcardNfa};
pub use levenshtein::CaseFoldLevenshtein;
pub use levenshtein_nfa::CaseFoldLevenshteinNfa;
pub use wildcard::{NfaBitSet, WildcardNfa};

/// Tells a driver what to do at the current trie node.
///
/// Returned from [`Automaton::classify`] once per node visit. The two
/// non-`Live*` variants are *short-circuits*: they let the automaton
/// tell its driver "I already know what every descendant of this node
/// will look like, you don't need to step them through me."
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum StateClass {
    /// Not accepting (no full match here). Step each child's edge label
    /// through the automaton and recurse into the survivors; prune dead
    /// children.
    ///
    /// Example: a pattern like `a*y` doesn't match `an` but it matches
    /// `any`, one of its descendants. We shouldn't yield `an`,
    /// but we must continue the traversal to find all matches.
    Live,
    /// Accepting (the current key matches the pattern). Yield it, then
    /// continue stepping children normally — descendants may also match.
    ///
    /// Example: a pattern like `a*y` matches `any` but would also match
    /// `anyany`, one of its descendants. We can't stop the traversal at
    /// `any` if we want to find all matches.
    LiveAccepting,
    /// Accepting *and* every descendant is also guaranteed to match
    /// regardless of label content. The driver yields the current key
    /// and pushes every descendant without further `step_all` calls.
    ///
    /// Example: a wildcard pattern ending in `*`, after the trailing
    /// `*`'s position has become permanently active.
    Permanent,
    /// Accepting but no outgoing transition is live — the state is a
    /// sink. The driver yields the current key and prunes the whole
    /// subtree.
    ///
    /// Example: the accept state of a fixed-length pattern.
    Terminal,
}

impl StateClass {
    /// Whether this class represents an accepting state (a hit to yield).
    pub const fn is_accepting(self) -> bool {
        matches!(self, Self::LiveAccepting | Self::Permanent | Self::Terminal)
    }
}

/// A byte-streaming state machine.
///
/// Implementations expose a [`start`](Self::start) state and a
/// single-byte transition ([`step`](Self::step)); every reached state
/// [`classify`](Self::classify)-es into a [`StateClass`]. See the
/// module doc for how the contract is shaped for trie traversal.
///
/// `step` / `step_all` take `&mut self` so implementations can recycle
/// scratch buffers across transitions. A driver is expected to own the
/// automaton by value and never call these concurrently, so the mutable
/// receiver isn't a multi-borrow concern in practice.
pub trait Automaton {
    /// State carried across byte transitions and trie stack frames. The
    /// driver clones it at branch points, so keep it cheap to clone.
    type State: Clone;

    /// The starting state, before any bytes have been consumed.
    fn start(&self) -> Self::State;

    /// Advance the state by one byte. `None` means the transition died
    /// — the driver prunes the subtree.
    fn step(&mut self, state: &Self::State, byte: u8) -> Option<Self::State>;

    /// Tag `state` with how the driver should proceed at this node. See
    /// [`StateClass`].
    fn classify(&self, state: &Self::State) -> StateClass;

    /// Step through a slice of bytes (a trie edge label), returning the
    /// final state or `None` if any byte kills the transition.
    ///
    /// The default impl walks bytes through [`Self::step`]; implementers
    /// can override to recycle scratch buffers across the bytes of a
    /// multi-byte label.
    fn step_all(&mut self, state: &Self::State, bytes: &[u8]) -> Option<Self::State> {
        let mut s = state.clone();
        for &b in bytes {
            s = self.step(&s, b)?;
        }
        Some(s)
    }

    /// A byte prefix that every accepted key starts with, if the automaton
    /// knows one. It permits a driver to jump straight to the subtree
    /// holding the keys with that prefix instead of descending from the
    /// trie root.
    ///
    /// # Contract
    ///
    /// Every key the automaton accepts must start with the returned bytes —
    /// a driver may never visit keys outside the prefix subtree, so a
    /// too-long or wrong prefix silently drops matches. Return `None` to
    /// disable the jump.
    fn literal_prefix(&self) -> Option<&[u8]>;
}
