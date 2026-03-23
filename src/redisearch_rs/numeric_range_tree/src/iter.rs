/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Iterator for traversing the numeric range tree.
//!
//! Provides a depth-first traversal over all nodes in the tree, useful for
//! operations like serialization, debugging, or collecting statistics.

use crate::arena::NodeIndex;
use crate::{NumericRangeNode, NumericRangeTree};

/// An iterator that performs a depth-first traversal of the numeric range tree.
///
/// This iterator visits all nodes in the tree, yielding each node exactly once.
/// The traversal is done iteratively using an explicit stack to avoid recursion,
/// which is important for deeply nested trees that might overflow the call stack.
///
/// # Traversal Order
///
/// Nodes are visited in reverse pre-order (parent -> right child -> left child).
#[derive(Debug)]
pub struct ReversePreOrderDfsIterator<'a> {
    /// Reference to the tree (used to resolve node indices).
    tree: &'a NumericRangeTree,
    /// Stack of node indices to visit. Nodes are pushed right-first so left is
    /// processed first (LIFO order).
    stack: Vec<NodeIndex>,
}

impl<'a> ReversePreOrderDfsIterator<'a> {
    /// Create a new iterator starting from the root of the given tree.
    pub fn new(tree: &'a NumericRangeTree) -> Self {
        Self::from_node(tree, tree.root_index())
    }

    /// Create a new iterator starting from the given node index in the tree.
    fn from_node(tree: &'a NumericRangeTree, node_idx: NodeIndex) -> Self {
        let mut stack = Vec::with_capacity(tree.node(node_idx).max_depth() as usize + 1);
        stack.push(node_idx);
        Self { tree, stack }
    }
}

impl<'a> Iterator for ReversePreOrderDfsIterator<'a> {
    type Item = &'a NumericRangeNode;

    fn next(&mut self) -> Option<Self::Item> {
        let node_idx = self.stack.pop()?;
        let node = self.tree.node(node_idx);

        if let NumericRangeNode::Internal(internal) = node {
            // Push children onto stack (left first so right is processed first)
            self.stack.push(internal.left_index());
            self.stack.push(internal.right_index());
        }

        Some(node)
    }
}

impl<'a> IntoIterator for &'a NumericRangeTree {
    type Item = &'a NumericRangeNode;
    type IntoIter = ReversePreOrderDfsIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        ReversePreOrderDfsIterator::new(self)
    }
}

/// Same iteration logic as [`ReversePreOrderDfsIterator`], but it yields indices alongside each node.
#[derive(Debug)]
pub struct IndexedReversePreOrderDfsIterator<'a> {
    /// Reference to the tree (used to resolve node indices).
    tree: &'a NumericRangeTree,
    /// Stack of node indices to visit. Nodes are pushed right-first so left is
    /// processed first (LIFO order).
    stack: Vec<NodeIndex>,
}

impl<'a> IndexedReversePreOrderDfsIterator<'a> {
    /// Create a new iterator starting from the root of the given tree.
    pub fn new(tree: &'a NumericRangeTree) -> Self {
        Self::from_node(tree, tree.root_index())
    }

    /// Create a new iterator starting from the given node index in the tree.
    fn from_node(tree: &'a NumericRangeTree, node_idx: NodeIndex) -> Self {
        let mut stack = Vec::with_capacity(tree.node(node_idx).max_depth() as usize + 1);
        stack.push(node_idx);
        Self { tree, stack }
    }
}

impl<'a> Iterator for IndexedReversePreOrderDfsIterator<'a> {
    type Item = (NodeIndex, &'a NumericRangeNode);

    fn next(&mut self) -> Option<Self::Item> {
        let node_idx = self.stack.pop()?;
        let node = self.tree.node(node_idx);

        if let NumericRangeNode::Internal(internal) = node {
            // Push children onto stack (left first so right is processed first)
            self.stack.push(internal.left_index());
            self.stack.push(internal.right_index());
        }

        Some((node_idx, node))
    }
}
