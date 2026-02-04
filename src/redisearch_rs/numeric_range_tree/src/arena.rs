/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Arena storage for numeric range tree nodes.
//!
//! This module provides arena-based storage for tree nodes, offering better
//! cache locality and cheaper rotations (index swaps instead of allocation)
//! compared to boxed pointers.

use std::ops::{Index, IndexMut};

use slab::Slab;

use crate::NumericRangeNode;

/// Index into the node arena.
///
/// Wraps a `slab::Slab` key. This is a lightweight handle (single `u32`)
/// that is stable across mutations to other slots in the slab.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct NodeIndex(u32);

impl NodeIndex {
    /// Convert to a `usize` key for indexing into a [`slab::Slab`].
    const fn key(self) -> usize {
        self.0 as usize
    }
}

impl From<u32> for NodeIndex {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<NodeIndex> for u32 {
    fn from(idx: NodeIndex) -> Self {
        idx.0
    }
}

/// Arena storage for [`NumericRangeNode`]s.
///
/// This is a newtype wrapper around [`Slab<NumericRangeNode>`] that provides
/// type-safe indexing via [`NodeIndex`] instead of raw `usize`.
#[derive(Debug)]
pub(crate) struct NodeArena {
    nodes: Slab<NumericRangeNode>,
}

impl NodeArena {
    /// Create a new empty arena.
    pub const fn new() -> Self {
        Self { nodes: Slab::new() }
    }

    /// Insert a node into the arena, returning its index.
    ///
    /// # Panics
    ///
    /// Debug-asserts that the resulting key fits in `u32`.
    pub fn insert(&mut self, node: NumericRangeNode) -> NodeIndex {
        let key = self.nodes.insert(node);
        debug_assert!(key <= u32::MAX as usize);
        NodeIndex(key as u32)
    }

    /// Remove a node from the arena, returning it.
    ///
    /// # Panics
    ///
    /// Panics if the index is invalid.
    #[expect(dead_code, reason = "part of complete arena API, needed for future GC")]
    pub fn remove(&mut self, idx: NodeIndex) -> NumericRangeNode {
        self.nodes.remove(idx.key())
    }

    /// Iterate over all nodes in the arena.
    ///
    /// Yields `(NodeIndex, &NumericRangeNode)` pairs.
    #[cfg_attr(
        not(all(feature = "unittest", not(miri))),
        expect(dead_code, reason = "used by invariant checks in unittest feature")
    )]
    pub fn iter(&self) -> impl Iterator<Item = (NodeIndex, &NumericRangeNode)> {
        self.nodes
            .iter()
            .map(|(key, node)| (NodeIndex(key as u32), node))
    }

    /// Iterate over all nodes in the arena mutably.
    ///
    /// Yields `(NodeIndex, &mut NumericRangeNode)` pairs.
    #[expect(dead_code, reason = "part of complete arena API, needed for future GC")]
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (NodeIndex, &mut NumericRangeNode)> {
        self.nodes
            .iter_mut()
            .map(|(key, node)| (NodeIndex(key as u32), node))
    }
}

impl Default for NodeArena {
    fn default() -> Self {
        Self::new()
    }
}

impl Index<NodeIndex> for NodeArena {
    type Output = NumericRangeNode;

    fn index(&self, idx: NodeIndex) -> &Self::Output {
        &self.nodes[idx.key()]
    }
}

impl IndexMut<NodeIndex> for NodeArena {
    fn index_mut(&mut self, idx: NodeIndex) -> &mut Self::Output {
        &mut self.nodes[idx.key()]
    }
}
