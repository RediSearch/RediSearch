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

use generational_slab::{Key, Slab};

use crate::NumericRangeNode;

/// Index into the node arena.
///
/// Wraps a [`generational_slab::Key`]. This is a lightweight handle
/// that is stable across mutations to other slots in the slab.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct NodeIndex(Key);

impl NodeIndex {
    /// Return the underlying [`Key`] for indexing into a [`generational_slab::Slab`].
    pub const fn key(self) -> Key {
        self.0
    }

    /// Reconstruct a `NodeIndex` from the raw position and generation of
    /// the underlying [`Key`].
    ///
    /// Intended for FFI round-trips where the index was previously
    /// decomposed via [`Key::position`] and [`Key::generation`].
    pub const fn from_raw_parts(position: u32, generation: u32) -> Self {
        Self(Key::from_raw_parts(position, generation))
    }
}

impl From<Key> for NodeIndex {
    fn from(key: Key) -> Self {
        Self(key)
    }
}

/// Arena storage for [`NumericRangeNode`]s.
///
/// This is a newtype wrapper around [`Slab<NumericRangeNode>`] that provides
/// type-safe indexing via [`NodeIndex`] instead of raw [`Key`].
#[derive(Debug)]
pub(crate) struct NodeArena {
    nodes: Slab<NumericRangeNode>,
}

impl NodeArena {
    /// Create a new empty arena.
    pub const fn new() -> Self {
        Self { nodes: Slab::new() }
    }

    /// Get the number of nodes currently stored in the arena.
    ///
    /// This is not to be confused with the current _capacity_
    /// of the arena, i.e. the size of the underlying currently-allocated
    /// slab.
    pub const fn len(&self) -> u32 {
        // Safe to truncate because `Self::insert` ensures that the arena
        // never grows beyond `u32::MAX`.
        self.nodes.len() as u32
    }

    /// Get the capacity of the arena.
    ///
    /// This is the maximum number of nodes that can be stored in the arena
    /// without reallocating.
    pub const fn capacity(&self) -> u32 {
        // Safe to truncate because `Self::insert` ensures that the arena
        // never grows beyond `u32::MAX`.
        self.nodes.capacity() as u32
    }

    /// Get a mutable reference to a node in the arena, if it exists.
    pub fn get_mut(&mut self, idx: NodeIndex) -> Option<&mut NumericRangeNode> {
        self.nodes.get_mut(idx.key())
    }

    /// Insert a node into the arena, returning its index.
    ///
    /// # Panics
    ///
    /// Panics if the slab exceeds its maximum capacity (`u32::MAX` entries).
    pub fn insert(&mut self, node: NumericRangeNode) -> NodeIndex {
        let key = self.nodes.insert(node);
        NodeIndex(key)
    }

    /// Remove a node from the arena, returning it.
    ///
    /// # Panics
    ///
    /// Panics if the index is invalid.
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
        self.nodes.iter().map(|(key, node)| (NodeIndex(key), node))
    }

    /// Iterate over all nodes in the arena mutably.
    ///
    /// Yields `(NodeIndex, &mut NumericRangeNode)` pairs.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (NodeIndex, &mut NumericRangeNode)> {
        self.nodes
            .iter_mut()
            .map(|(key, node)| (NodeIndex(key), node))
    }

    /// Get the memory usage of the arena, in bytes.
    pub const fn mem_usage(&self) -> usize {
        self.nodes.mem_usage()
    }

    /// Compact the arena, moving nodes to fill gaps.
    ///
    /// The callback is called for each moved node with `(from_idx, to_idx)`.
    /// Return `true` from the callback to proceed with the move.
    pub fn compact(
        &mut self,
        mut callback: impl FnMut(&mut NumericRangeNode, NodeIndex, NodeIndex) -> bool,
    ) {
        self.nodes
            .compact(|node, from, to| callback(node, from.into(), to.into()))
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
