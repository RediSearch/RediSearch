/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Iterator for traversing the numeric range tree.

use crate::{NumericRangeNode, NumericRangeTree};

/// An iterator that performs a depth-first traversal of the numeric range tree.
///
/// This iterator visits all nodes in the tree, yielding each node exactly once.
/// The traversal is done iteratively using an explicit stack to avoid recursion.
#[derive(Debug)]
pub struct NumericRangeTreeIterator<'a> {
    /// Stack of nodes to visit.
    stack: Vec<&'a NumericRangeNode>,
}

impl<'a> NumericRangeTreeIterator<'a> {
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

impl<'a> Iterator for NumericRangeTreeIterator<'a> {
    type Item = &'a NumericRangeNode;

    fn next(&mut self) -> Option<Self::Item> {
        let node = self.stack.pop()?;

        // Push children onto stack (right first so left is processed first)
        if !node.is_leaf() {
            if let Some(right) = node.right() {
                self.stack.push(right);
            }
            if let Some(left) = node.left() {
                self.stack.push(left);
            }
        }

        Some(node)
    }
}
