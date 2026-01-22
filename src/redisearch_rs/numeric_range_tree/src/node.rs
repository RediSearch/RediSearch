/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Numeric range tree node implementation.

use crate::NumericRange;

/// A node in the numeric range tree.
///
/// Nodes can be either:
/// - Leaf nodes: Have a range but no children
/// - Internal nodes: Have children and optionally retain a range for query efficiency
#[derive(Debug)]
pub struct NumericRangeNode {
    /// The split value for this node. Values less than this go left, >= go right.
    /// Only meaningful for internal nodes.
    value: f64,
    /// Maximum depth of the subtree rooted at this node, used for balancing.
    max_depth: i32,
    /// Left child (values < split value).
    left: Option<Box<NumericRangeNode>>,
    /// Right child (values >= split value).
    right: Option<Box<NumericRangeNode>>,
    /// The numeric range stored at this node.
    /// - Always present for leaf nodes
    /// - May be present for internal nodes (retained ranges)
    /// - None for internal nodes that have been trimmed
    range: Option<NumericRange>,
}

impl NumericRangeNode {
    /// Create a new leaf node with an empty range.
    pub fn new_leaf() -> Self {
        Self {
            value: 0.0,
            max_depth: 0,
            left: None,
            right: None,
            range: Some(NumericRange::new()),
        }
    }

    /// Check if this node is a leaf (no children).
    pub const fn is_leaf(&self) -> bool {
        self.left.is_none() && self.right.is_none()
    }

    /// Get the split value.
    pub const fn split_value(&self) -> f64 {
        self.value
    }

    /// Set the split value.
    pub fn set_split_value(&mut self, value: f64) {
        self.value = value;
    }

    /// Get the maximum depth of the subtree rooted at this node.
    pub const fn max_depth(&self) -> i32 {
        self.max_depth
    }

    /// Set the maximum depth.
    pub fn set_max_depth(&mut self, depth: i32) {
        self.max_depth = depth;
    }

    /// Get a reference to the left child.
    pub fn left(&self) -> Option<&NumericRangeNode> {
        self.left.as_deref()
    }

    /// Get a mutable reference to the left child.
    pub fn left_mut(&mut self) -> Option<&mut NumericRangeNode> {
        self.left.as_deref_mut()
    }

    /// Get a reference to the right child.
    pub fn right(&self) -> Option<&NumericRangeNode> {
        self.right.as_deref()
    }

    /// Get a mutable reference to the right child.
    pub fn right_mut(&mut self) -> Option<&mut NumericRangeNode> {
        self.right.as_deref_mut()
    }

    /// Set the left child.
    pub fn set_left(&mut self, child: Option<Box<NumericRangeNode>>) {
        self.left = child;
    }

    /// Set the right child.
    pub fn set_right(&mut self, child: Option<Box<NumericRangeNode>>) {
        self.right = child;
    }

    /// Take the left child, leaving None in its place.
    pub fn take_left(&mut self) -> Option<Box<NumericRangeNode>> {
        self.left.take()
    }

    /// Take the right child, leaving None in its place.
    pub fn take_right(&mut self) -> Option<Box<NumericRangeNode>> {
        self.right.take()
    }

    /// Get a reference to the range, if present.
    pub const fn range(&self) -> Option<&NumericRange> {
        self.range.as_ref()
    }

    /// Get a mutable reference to the range, if present.
    pub fn range_mut(&mut self) -> Option<&mut NumericRange> {
        self.range.as_mut()
    }

    /// Take the range from this node, leaving None in its place.
    pub fn take_range(&mut self) -> Option<NumericRange> {
        self.range.take()
    }

    /// Set the range for this node.
    pub fn set_range(&mut self, range: Option<NumericRange>) {
        self.range = range;
    }

    /// Check if this node has a range.
    pub const fn has_range(&self) -> bool {
        self.range.is_some()
    }
}

impl Default for NumericRangeNode {
    fn default() -> Self {
        Self::new_leaf()
    }
}
