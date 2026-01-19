/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Iterator for traversing the numeric range tree.

use crate::node::NumericRangeNode;
use crate::tree::NumericRangeTree;

/// An iterator that traverses all nodes in a NumericRangeTree using depth-first
/// pre-order traversal.
pub struct NumericRangeTreeIterator<'a> {
    /// Stack of nodes to visit
    stack: Vec<&'a NumericRangeNode>,
}

impl<'a> NumericRangeTreeIterator<'a> {
    /// Creates a new iterator for the given tree.
    pub fn new(tree: &'a NumericRangeTree) -> Self {
        Self {
            stack: vec![tree.root()],
        }
    }
}

impl<'a> Iterator for NumericRangeTreeIterator<'a> {
    type Item = &'a NumericRangeNode;

    fn next(&mut self) -> Option<Self::Item> {
        let node = self.stack.pop()?;

        // Push children onto the stack (right first so left is processed first)
        if !node.is_leaf() {
            if let Some(ref right) = node.right {
                self.stack.push(right);
            }
            if let Some(ref left) = node.left {
                self.stack.push(left);
            }
        }

        Some(node)
    }
}
