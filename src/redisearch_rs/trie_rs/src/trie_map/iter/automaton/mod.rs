/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Streaming-automaton trie traversal.
//!
//! Walks a trie under the control of a byte-streaming state machine —
//! the [`Automaton`] — instead of materialising each candidate key and
//! re-testing it against a predicate (the
//! [`TraversalFilter`](super::filter::TraversalFilter) approach). The
//! payoff is *shared-prefix reuse*: the automaton state at any trie
//! node is computed once, then reused for every descendant, so each
//! byte of stored content costs at most one transition per query.
//!
//! ## How it works
//!
//! The [`Automaton`] trait abstracts the state machine. An
//! implementation gives the driver three things —
//!
//! - [`start`](Automaton::start): the initial state.
//! - [`step`](Automaton::step) / [`step_all`](Automaton::step_all):
//!   advance the state by one byte or by a slice (a trie edge label).
//!   Returning `None` from `step_all` tells the driver the subtree is
//!   dead and can be pruned.
//! - [`classify`](Automaton::classify): tag the current state with a
//!   [`StateClass`] that tells the driver whether to yield the current
//!   key and whether to keep recursing.
//! - [`literal_prefix`](Automaton::literal_prefix): a byte prefix every
//!   accepted key starts with (or `None`), letting the traversal jump
//!   straight to that subtree instead of descending from the root.
//!
//! [`driver::AutomatonIter`] is the generic iterator that drives any
//! `Automaton` over a trie via a DFS that carries the post-edge state
//! on its stack.
//!
//! **Start there for a worked NFA primer** — the module
//! doc on [`wildcard`] explains positions, ε-closure, and the bitset
//! state encoding with a concrete `*ab*` against `xaab` trace.
//!
//! ## Layout
//!
//! - This module: the [`Automaton`] trait and the [`StateClass`] enum.
//! - [`driver`]: [`AutomatonIter`] and its lending variant.
//! - [`wildcard`]: the wildcard NFA, atom encoding, and the
//!   pattern-size-based backend dispatcher
//!   ([`WildcardIter`]).

pub mod driver;
pub mod wildcard;

pub use driver::{AutomatonIter, AutomatonLendingIter};
pub use wildcard::{NfaBitSet, WildcardBackend, WildcardIter, WildcardLendingIter, WildcardNfa};

/// Tells the driver what to do at the current trie node.
///
/// Returned from [`Automaton::classify`] once per node visit. The two
/// non-`Live*` variants are *short-circuits*: they let the automaton
/// tell the driver "I already know what every descendant of this node
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

/// A byte-streaming state machine that the trie driver can run.
///
/// Implementations expose a `start` state and a single-byte transition;
/// the driver advances the state along each trie edge and asks
/// [`Self::classify`] what to do at each node. See the module doc for
/// how this fits with [`AutomatonIter`].
///
/// `step` / `step_all` take `&mut self` so implementations can recycle
/// scratch buffers across transitions. The driver owns the automaton by
/// value and never calls these concurrently, so the mutable receiver
/// isn't a multi-borrow concern in practice.
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
    /// knows one. [`TrieMap::automaton_iter`](crate::TrieMap::automaton_iter)
    /// uses it to jump straight to the subtree holding the keys with that
    /// prefix instead of descending from the trie root.
    ///
    /// # Contract
    ///
    /// Every key the automaton accepts must start with the returned bytes —
    /// keys outside the prefix subtree are never visited, so a too-long or
    /// wrong prefix silently drops matches. Return `None` to disable the
    /// jump and descend from the trie root.
    fn literal_prefix(&self) -> Option<&[u8]>;
}
