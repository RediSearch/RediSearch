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
//! An alternative to [`TraversalFilter`](super::filter::TraversalFilter) that
//! consumes the trie one byte at a time, carrying state across edges instead
//! of re-evaluating a predicate against the full accumulated key at every
//! node. Each byte of stored content is processed at most once per query.
//!
//! ## Layout
//!
//! - This module defines the [`Automaton`] trait and the [`StateClass`]
//!   enum that drives iterator behavior.
//! - [`driver`] hosts [`AutomatonIter`] (and its lending variant), the
//!   generic iterator that drives any `Automaton` over a trie.
//! - [`wildcard`] hosts the wildcard NFA
//!   ([`WildcardNfa`](wildcard::WildcardNfa)), the specialized fixed-length
//!   iterator ([`FixedWildcardIter`](wildcard::FixedWildcardIter)), and the
//!   auto-dispatching wrapper
//!   ([`WildcardSpecializedIter`](wildcard::WildcardSpecializedIter)).

pub mod driver;
pub mod wildcard;

pub use driver::{AutomatonIter, AutomatonLendingIter};
pub use wildcard::{
    BitSetClass, FixedWildcardIter, FixedWildcardLendingIter, InlineStateSet, NfaBitSet,
    SparseStateSet, WildcardNfa, WildcardSparseNfa, WildcardSpecializedIter,
    WildcardSpecializedLendingIter, pattern_has_star,
};

/// What the iterator should do at the current state.
///
/// Returned from [`Automaton::classify`] once per node visit; the iterator
/// branches on the variant to decide whether to yield the current entry and
/// how to traverse the subtree.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum StateClass {
    /// Not accepting. Step children through the automaton; prune dead ones.
    Live,
    /// Accepting now. Step children normally — descendants may also match.
    LiveAccepting,
    /// Accepting now and stays accepting on every future input. Every
    /// descendant is guaranteed to match, so the iterator pushes them all
    /// without stepping. Example: a wildcard pattern ending in `*`.
    Permanent,
    /// Accepting now but no transition is live — no descendant can match.
    /// The iterator yields the current value and skips the subtree. Example:
    /// the accept state of a fixed-length pattern (a sink).
    Terminal,
}

impl StateClass {
    /// Whether this class represents an accepting state.
    pub const fn is_accepting(self) -> bool {
        matches!(self, Self::LiveAccepting | Self::Permanent | Self::Terminal)
    }
}

/// A byte-streaming state machine.
///
/// Implementations advance one byte at a time. The trie iterator carries the
/// resulting state on its traversal stack, so each byte of stored content is
/// processed at most once per query.
///
/// `step` and `step_all` take `&mut self` so implementations can hold
/// recycled scratch buffers. The trie iterator never calls these
/// concurrently for the same automaton (it owns the automaton by value),
/// so the mutable receiver is not a multi-borrow concern in practice.
pub trait Automaton {
    /// State carried across byte transitions and trie stack frames.
    type State: Clone;

    /// The starting state, before any bytes have been consumed.
    fn start(&self) -> Self::State;

    /// Advance the state by one byte.
    ///
    /// Returns `None` if the byte transitions into a dead state — the trie
    /// iterator will prune the corresponding subtree.
    fn step(&mut self, state: &Self::State, byte: u8) -> Option<Self::State>;

    /// Classify the current iterator state to determine whether:
    /// - The current entry should be yielded.
    /// - Its descendants should be examined.
    fn classify(&self, state: &Self::State) -> StateClass;

    /// Convenience: step through a slice of bytes, returning the final state
    /// or `None` if any byte leads to a dead state.
    fn step_all(&mut self, state: &Self::State, bytes: &[u8]) -> Option<Self::State> {
        let mut s = state.clone();
        for &b in bytes {
            s = self.step(&s, b)?;
        }
        Some(s)
    }
}
