/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Specialized iterator for wildcard patterns without `*`.
//!
//! Without `Any`, the NFA's active state is provably a singleton: a single
//! position in the atom list. We can replace the entire `Automaton` trait
//! machinery (bitset, ε-closure, `Option`-wrapped state) with a plain
//! `usize` counter and direct integer arithmetic.
//!
//! Concretely: each byte of input either advances the counter (a literal
//! byte that matches, or `?`) or kills it (literal mismatch, or running
//! past the end). At every node visit we check `pos == accept` to decide
//! whether to yield and `pos < accept` to decide whether descendants are
//! still reachable.

use super::atoms::{Atom, flatten};
use crate::node::Node;
use lending_iterator::prelude::*;
use wildcard::WildcardPattern;

/// Iterator over wildcard matches when the pattern contains no `*`.
///
/// State is a `usize` position in a flattened atom slice. No
/// [`Automaton`](super::super::Automaton) trait, no bitset, no ε-closure pass.
pub struct FixedWildcardIter<'tm, Data> {
    stack: Vec<Frame<'tm, Data>>,
    /// Pattern atoms, guaranteed not to contain [`Atom::Any`].
    atoms: Vec<Atom>,
    /// `atoms.len()` — cached so the hot loop doesn't reload it.
    accept: usize,
    /// Concatenated labels from root to the current node.
    key: Vec<u8>,
}

/// One pending visit. See [`AutomatonIter`](super::super::AutomatonIter)'s
/// `Frame` for the `parent_key_len` trick — same idea here.
struct Frame<'tm, Data> {
    node: &'tm Node<Data>,
    pos: usize,
    parent_key_len: u32,
}

impl<'tm, Data> FixedWildcardIter<'tm, Data> {
    pub(crate) const fn empty() -> Self {
        Self {
            stack: Vec::new(),
            atoms: Vec::new(),
            accept: 0,
            key: Vec::new(),
        }
    }

    /// Construct from `pattern` (which must contain no `*` — debug-asserted).
    /// See
    /// [`AutomatonIter::new`](super::super::AutomatonIter::new) for the
    /// `start_node` / `key_prefix` contract.
    pub(crate) fn new(
        start_node: Option<&'tm Node<Data>>,
        key_prefix: Vec<u8>,
        pattern: &WildcardPattern<'_>,
    ) -> Self {
        let atoms = flatten(pattern);
        debug_assert!(
            !atoms.iter().any(|a| matches!(a, Atom::Any)),
            "FixedWildcardIter requires a pattern with no `*` atoms",
        );
        let accept = atoms.len();
        let mut iter = Self {
            stack: Vec::new(),
            atoms,
            accept,
            key: key_prefix,
        };
        if let Some(node) = start_node
            && let Some(pos_after_prefix) = step_fixed(&iter.atoms, 0, &iter.key)
            && let Some(pos) = step_fixed(&iter.atoms, pos_after_prefix, node.label())
        {
            let parent_key_len = iter.key.len() as u32;
            iter.stack.push(Frame {
                node,
                pos,
                parent_key_len,
            });
        }
        iter
    }

    pub(crate) fn advance(&mut self) -> Option<&'tm Data> {
        loop {
            let Frame {
                node,
                pos,
                parent_key_len,
            } = self.stack.pop()?;

            self.key.truncate(parent_key_len as usize);
            self.key.extend(node.label());
            let key_len_after_node = self.key.len() as u32;

            // Children are reachable only while we still have atoms to consume.
            if pos < self.accept {
                for child in node.children().iter().rev() {
                    if let Some(child_pos) = step_fixed(&self.atoms, pos, child.label()) {
                        self.stack.push(Frame {
                            node: child,
                            pos: child_pos,
                            parent_key_len: key_len_after_node,
                        });
                    }
                }
            }

            if pos == self.accept
                && let Some(data) = node.data()
            {
                return Some(data);
            }
        }
    }

    pub(crate) fn key(&self) -> &[u8] {
        &self.key
    }
}

/// Advance the position counter through a slice of bytes.
///
/// Returns `None` if the slice can't be matched starting at `pos`.
#[inline]
fn step_fixed(atoms: &[Atom], mut pos: usize, bytes: &[u8]) -> Option<usize> {
    for &byte in bytes {
        if pos >= atoms.len() {
            return None;
        }
        // SAFETY: bounds-checked just above.
        let atom = unsafe { *atoms.get_unchecked(pos) };
        let advances = match atom {
            Atom::Byte(b) => b == byte,
            Atom::One => true,
            // Caller guarantees `atoms` contains no `Any`.
            Atom::Any => return None,
        };
        if !advances {
            return None;
        }
        pos += 1;
    }
    Some(pos)
}

impl<'tm, Data> Iterator for FixedWildcardIter<'tm, Data> {
    type Item = (Vec<u8>, &'tm Data);

    fn next(&mut self) -> Option<Self::Item> {
        self.advance().map(|d| (self.key.clone(), d))
    }
}

/// Lending-iterator wrapper around [`FixedWildcardIter`].
pub struct FixedWildcardLendingIter<'tm, Data>(FixedWildcardIter<'tm, Data>);

impl<'tm, Data> From<FixedWildcardIter<'tm, Data>> for FixedWildcardLendingIter<'tm, Data> {
    fn from(iter: FixedWildcardIter<'tm, Data>) -> Self {
        Self(iter)
    }
}

#[gat]
impl<'tm, Data> LendingIterator for FixedWildcardLendingIter<'tm, Data> {
    type Item<'next>
    where
        Self: 'next,
    = (&'next [u8], &'tm Data);

    fn next(&mut self) -> Option<Self::Item<'_>> {
        let item = self.0.advance()?;
        Some((self.0.key(), item))
    }
}
