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

use crate::{NumericRangeNode, NumericRangeTree};

/// An iterator that performs a depth-first traversal of the numeric range tree.
///
/// This iterator visits all nodes in the tree, yielding each node exactly once.
/// The traversal is done iteratively using an explicit stack to avoid recursion,
/// which is important for deeply nested trees that might overflow the call stack.
///
/// # Traversal Order
///
/// Nodes are visited in pre-order (parent before children), with left children
/// visited before right children. This matches the natural reading order of
/// the tree from left to right.
///
/// # Thread Safety
///
/// This iterator holds immutable references to tree nodes. If the tree is
/// modified during iteration (which would require mutable access), the
/// [`NumericRangeTree::revision_id`] mechanism should be used to detect and
/// abort stale iterations.
#[derive(Debug)]
pub struct DepthFirstNumericRangeTreeIterator<'a> {
    /// Stack of nodes to visit. Nodes are pushed right-first so left is
    /// processed first (LIFO order).
    stack: Vec<&'a NumericRangeNode>,
}

impl<'a> DepthFirstNumericRangeTreeIterator<'a> {
    /// Create a new iterator starting from the root of the given tree.
    pub fn new(tree: &'a NumericRangeTree) -> Self {
        let mut stack = Vec::with_capacity(4);
        stack.push(tree.root());
        Self { stack }
    }

    /// Create a new iterator starting from the given node.
    pub fn from_node(node: &'a NumericRangeNode) -> Self {
        let mut stack = Vec::with_capacity(4);
        stack.push(node);
        Self { stack }
    }
}

impl<'a> Iterator for DepthFirstNumericRangeTreeIterator<'a> {
    type Item = &'a NumericRangeNode;

    fn next(&mut self) -> Option<Self::Item> {
        let node = self.stack.pop()?;

        // Push children onto stack (right first so left is processed first)
        if let Some(right) = node.right() {
            self.stack.push(right);
        }
        if let Some(left) = node.left() {
            self.stack.push(left);
        }

        Some(node)
    }
}

impl<'a> IntoIterator for &'a NumericRangeTree {
    type Item = &'a NumericRangeNode;
    type IntoIter = DepthFirstNumericRangeTreeIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        DepthFirstNumericRangeTreeIterator::new(self)
    }
}
