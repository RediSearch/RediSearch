/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The generic iterator that drives an [`Automaton`] over a trie.

use super::{Automaton, StateClass};
use crate::node::Node;
use lending_iterator::prelude::*;

/// Iterator that traverses a trie under the control of a streaming
/// [`Automaton`].
///
/// Unlike [`Iter`](crate::iter::Iter) +
/// [`TraversalFilter`](crate::iter::filter::TraversalFilter), the automaton's
/// state is advanced byte-by-byte as the iterator descends each edge. The trie
/// iterator carries the post-edge state on its stack, so shared prefixes are
/// processed once per query rather than re-walked at every descendant.
pub struct AutomatonIter<'tm, Data, A: Automaton> {
    stack: Vec<Frame<'tm, Data, A::State>>,
    automaton: A,
    /// Concatenated labels from root to the current node.
    key: Vec<u8>,
}

/// One pending visit.
///
/// `parent_key_len` is what the iterator's `key` length should be just
/// before this node is processed — popping a frame truncates `key` to that
/// length before extending it with `node.label()`. This replaces the
/// explicit "revisit" frames a tree DFS would otherwise need.
///
/// `parent_key_len` is `u32` rather than `usize` to keep the frame compact;
/// this caps total key length at 4 GiB, which is fine for any realistic
/// wildcard workload.
///
/// Two variants:
///
/// - [`Frame::Visit`] — the standard case: carries automaton state, classify
///   on pop to decide how to traverse children.
/// - [`Frame::PermanentVisit`] — once the automaton has reported
///   [`StateClass::Permanent`] for some ancestor, every descendant in the
///   subtree is guaranteed to match. We push these without state and skip
///   `classify` on pop, which avoids a `state.clone()` per child
///   (significant for the wider `InlineStateSet` / `HeapStateSet` bitsets)
///   and the redundant classify calls that would all return `Permanent`
///   anyway.
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
        if let Some(node) = start_node
            && let Some(pre_node) = iter.automaton.step_all(&iter.automaton.start(), &iter.key)
            && let Some(state) = iter.automaton.step_all(&pre_node, node.label())
        {
            // The first frame's `parent_key_len` is the prefix we already
            // have — popping it will truncate to that length (a no-op the
            // first time) and re-extend with `node.label()`.
            let parent_key_len = iter.key.len() as u32;
            iter.stack.push(Frame::Visit {
                node,
                state,
                parent_key_len,
            });
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
                    let key_len_after_node = self.key.len() as u32;

                    let class = self.automaton.classify(&state);
                    match class {
                        StateClass::Permanent => {
                            // Every descendant matches without further
                            // automaton work — push them as
                            // `PermanentVisit`, which doesn't carry state
                            // and skips `classify` on pop. Avoids
                            // `state.clone()` per child (a heap alloc for
                            // the spilled bitset).
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
                Frame::PermanentVisit {
                    node,
                    parent_key_len,
                } => {
                    self.key.truncate(parent_key_len as usize);
                    self.key.extend(node.label());
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
