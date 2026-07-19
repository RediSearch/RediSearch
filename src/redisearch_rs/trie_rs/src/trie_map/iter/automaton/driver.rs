/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Generic iterator that drives an [`Automaton`] over a trie.
//!
//! Depth-first walk that carries the post-edge automaton state on its
//! stack. At each node the driver consults [`Automaton::classify`] and
//! branches on the returned [`StateClass`]; see that enum's doc for the
//! four cases.

use super::{Automaton, StateClass};
use crate::trie_map::node::Node;
use lending_iterator::prelude::*;

/// Iterator that traverses a trie under the control of a streaming
/// [`Automaton`].
///
/// Every byte stored in the trie is run through the automaton
/// at most once per query, no matter how many keys share that byte.
pub struct AutomatonIter<'tm, Data, A: Automaton> {
    stack: Vec<Frame<'tm, Data, A::State>>,
    /// The automaton state is **cloned** once per child when the current
    /// stack frame is either in [`StateClass::Live`] or
    /// [`StateClass::LiveAccepting`].
    automaton: A,
    /// Concatenated labels from the trie root to the current node. The
    /// driver truncates and re-extends this in place as it descends and
    /// backtracks, instead of allocating a fresh `Vec` per yield.
    key: Vec<u8>,
}

/// One pending visit on the traversal stack.
///
/// `parent_key_len` is the length the iterator's `key` should have just
/// before this node is processed — popping a frame truncates `key` to
/// that length and then re-extends with `node.label()`. That folds DFS
/// backtracking into the same `pop` that pulls the next frame, so we
/// don't need separate "go back up" frames. (Stored as `u32` rather
/// than `usize` to keep the frame compact; caps key length at 4 GiB.)
///
/// Two variants encode whether the automaton is still consulted at
/// each descendant ([`Visit`]) or whether the subtree has already been
/// declared a universal match by [`StateClass::Permanent`] on some
/// ancestor ([`PermanentVisit`], no state carried — saves a clone per
/// descendant and skips redundant `classify` calls).
///
/// [`Visit`]: Frame::Visit
/// [`PermanentVisit`]: Frame::PermanentVisit
enum Frame<'tm, Data, S> {
    Visit {
        node: &'tm Node<Data>,
        state: S,
        parent_key_len: u32,
    },
    PermanentVisit {
        node: &'tm Node<Data>,
        parent_key_len: u32,
    },
}

impl<'tm, Data, A: Automaton> AutomatonIter<'tm, Data, A> {
    pub(crate) const fn empty(automaton: A) -> Self {
        Self {
            stack: Vec::new(),
            automaton,
            key: Vec::new(),
        }
    }

    /// Construct an iterator that starts at `start_node`, treating `key_prefix`
    /// as the trie path already consumed before reaching `start_node`'s parent.
    ///
    /// The automaton is advanced through `key_prefix + start_node.label()`
    /// before the first frame is pushed, so the state on the stack reflects
    /// the full path from the trie root.
    ///
    /// For a top-level traversal, pass the trie root with an empty prefix; for
    /// a subtree jump (e.g., the literal-prefix shortcut), pass the subroot
    /// with the path-to-its-parent.
    pub(crate) fn new(
        start_node: Option<&'tm Node<Data>>,
        key_prefix: Vec<u8>,
        automaton: A,
    ) -> Self {
        let mut iter = Self {
            stack: Vec::new(),
            automaton,
            key: key_prefix,
        };
        if let Some(node) = start_node {
            // Sequence the calls: `step_all` takes `&mut automaton`, so we
            // can't chain it with `automaton.start()` (which holds an `&`
            // borrow on the same field) on a single line.
            let start = iter.automaton.start();
            if let Some(pre_node) = iter.automaton.step_all(&start, &iter.key)
                && let Some(state) = iter.automaton.step_all(&pre_node, node.label())
            {
                // The first frame's `parent_key_len` is the prefix we
                // already have — popping it will truncate to that length
                // (a no-op the first time) and re-extend with `node.label()`.
                debug_assert!(iter.key.len() <= u32::MAX as usize);
                let parent_key_len = iter.key.len() as u32;
                iter.stack.push(Frame::Visit {
                    node,
                    state,
                    parent_key_len,
                });
            }
        }
        iter
    }

    /// Advance to the next matching entry, returning a reference to its data.
    pub(crate) fn advance(&mut self) -> Option<&'tm Data> {
        loop {
            match self.stack.pop()? {
                Frame::Visit {
                    node,
                    state,
                    parent_key_len,
                } => {
                    // Restore the key to this node's parent depth, then
                    // extend with its label. The truncate handles
                    // backtracking inline — we don't push a separate revisit
                    // frame for the way back up the stack.
                    self.key.truncate(parent_key_len as usize);
                    self.key.extend(node.label());
                    debug_assert!(self.key.len() <= u32::MAX as usize);
                    let key_len_after_node = self.key.len() as u32;

                    let class = self.automaton.classify(&state);
                    match class {
                        StateClass::Permanent => {
                            // Per `StateClass::Permanent`'s contract, the property is closed
                            // under descent: every descendant is guaranteed accepting regardless
                            // of label. We therefore never re-`classify` below this point — push
                            // children as `PermanentVisit`, skipping both `state.clone()` and the
                            // redundant `classify` call.
                            for child in node.children().iter().rev() {
                                self.stack.push(Frame::PermanentVisit {
                                    node: child,
                                    parent_key_len: key_len_after_node,
                                });
                            }
                        }
                        StateClass::Terminal => {
                            // Sink: no descendant can match. Skip children.
                        }
                        StateClass::Live | StateClass::LiveAccepting => {
                            // Standard path: step each child's label, push
                            // survivors.
                            for child in node.children().iter().rev() {
                                if let Some(child_state) =
                                    self.automaton.step_all(&state, child.label())
                                {
                                    self.stack.push(Frame::Visit {
                                        node: child,
                                        state: child_state,
                                        parent_key_len: key_len_after_node,
                                    });
                                }
                            }
                        }
                    }

                    if class.is_accepting()
                        && let Some(data) = node.data()
                    {
                        return Some(data);
                    }
                }
                // `StateClass::Permanent` is closed under descent by contract, so
                // every descendant inherits permanent-accepting status. Re-push as
                // `PermanentVisit` without consulting the automaton.
                Frame::PermanentVisit {
                    node,
                    parent_key_len,
                } => {
                    self.key.truncate(parent_key_len as usize);
                    self.key.extend(node.label());
                    debug_assert!(self.key.len() <= u32::MAX as usize);
                    let key_len_after_node = self.key.len() as u32;

                    // Every descendant is also in permanent mode — push as
                    // `PermanentVisit` too.
                    for child in node.children().iter().rev() {
                        self.stack.push(Frame::PermanentVisit {
                            node: child,
                            parent_key_len: key_len_after_node,
                        });
                    }

                    // In permanent mode the state is always accepting, so
                    // any node with data yields.
                    if let Some(data) = node.data() {
                        return Some(data);
                    }
                }
            }
        }
    }

    pub(crate) fn key(&self) -> &[u8] {
        &self.key
    }
}

impl<'tm, Data, A: Automaton> Iterator for AutomatonIter<'tm, Data, A> {
    type Item = (Vec<u8>, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.advance().map(|d| (self.key.clone(), d))
    }
}

/// Lending-iterator wrapper around [`AutomatonIter`] that borrows the current
/// key on each `next` call rather than cloning it.
pub struct AutomatonLendingIter<'tm, Data, A: Automaton>(AutomatonIter<'tm, Data, A>);

impl<'tm, Data, A: Automaton> From<AutomatonIter<'tm, Data, A>>
    for AutomatonLendingIter<'tm, Data, A>
{
    fn from(iter: AutomatonIter<'tm, Data, A>) -> Self {
        AutomatonLendingIter(iter)
    }
}

#[gat]
impl<'tm, Data, A: Automaton> LendingIterator for AutomatonLendingIter<'tm, Data, A> {
    type Item<'next>
    where
        Self: 'next,
    = (&'next [u8], &'tm Data);

    fn next(&mut self) -> Option<Self::Item<'_>> {
        let item = self.0.advance()?;
        Some((self.0.key(), item))
    }
}
