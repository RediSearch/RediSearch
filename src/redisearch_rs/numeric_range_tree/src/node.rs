/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Numeric range tree node implementation.
//!
//! This module defines the node structure used in the numeric range tree.
//! Nodes form a binary tree where internal nodes partition the value space
//! and leaf nodes store the actual document-value entries.

use crate::NumericRange;

/// A node in the numeric range tree.
///
/// Nodes can be either:
/// - **Leaf nodes**: Have a range but no children (`left` and `right` are `None`)
/// - **Internal nodes**: Have both children and optionally retain a range for query efficiency
///
/// # Invariants
///
/// - A node is a leaf if and only if both `left` and `right` are `None`.
/// - Internal nodes always have both children (never just one).
/// - Leaf nodes always have a range (`range.is_some()`).
/// - Internal nodes may or may not have a range, depending on depth trimming.
///
/// # Field Semantics by Node Type
///
/// | Field       | Leaf Node           | Internal Node                        |
/// |-------------|---------------------|--------------------------------------|
/// | `value`     | Unused (0.0)        | Split point for child routing        |
/// | `max_depth` | 0                   | Max depth of subtree (for balancing) |
/// | `left`      | `None`              | Always `Some`                        |
/// | `right`     | `None`              | Always `Some`                        |
/// | `range`     | Always `Some`       | `Some` if retained, else `None`      |
#[derive(Debug)]
pub struct NumericRangeNode {
    /// The split value for routing values to children.
    ///
    /// - **Internal nodes**: Values < `value` go to `left`, values >= `value` go to `right`.
    /// - **Leaf nodes**: This field is unused and set to 0.0.
    value: f64,

    /// Maximum depth of the subtree rooted at this node.
    ///
    /// Used for AVL-like balancing. For leaf nodes, this is always 0.
    /// For internal nodes, it equals `max(left.max_depth, right.max_depth) + 1`.
    ///
    /// Note: This is `i32` (not `usize`) to match the C implementation and allow
    /// signed arithmetic when computing depth differences for balance checks.
    max_depth: i32,

    /// Left child subtree (values < split value).
    ///
    /// - **Leaf nodes**: Always `None`.
    /// - **Internal nodes**: Always `Some` after splitting.
    left: Option<Box<NumericRangeNode>>,

    /// Right child subtree (values >= split value).
    ///
    /// - **Leaf nodes**: Always `None`.
    /// - **Internal nodes**: Always `Some` after splitting.
    right: Option<Box<NumericRangeNode>>,

    /// The numeric range containing document-value entries.
    ///
    /// - **Leaf nodes**: Always `Some`. Contains the actual indexed data for this
    ///   portion of the value space.
    /// - **Internal nodes**: `Some` if the range is retained for query optimization
    ///   (when `max_depth <= max_depth_range`), `None` if trimmed to save memory.
    ///
    /// When present on internal nodes, the range contains all entries from the
    /// entire subtree, enabling queries that span the full range to use this
    /// single range instead of unioning all descendant ranges.
    range: Option<NumericRange>,
}

impl NumericRangeNode {
    /// Create a new leaf node with an empty range.
    ///
    /// If `compress_floats` is true, the range will use float compression.
    pub fn new_leaf(compress_floats: bool) -> Self {
        Self {
            value: 0.0,
            max_depth: 0,
            left: None,
            right: None,
            range: Some(NumericRange::new(compress_floats)),
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
    pub const fn set_split_value(&mut self, value: f64) {
        self.value = value;
    }

    /// Get the maximum depth of the subtree rooted at this node.
    pub const fn max_depth(&self) -> i32 {
        self.max_depth
    }

    /// Set the maximum depth.
    pub const fn set_max_depth(&mut self, depth: i32) {
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
    pub const fn take_left(&mut self) -> Option<Box<NumericRangeNode>> {
        self.left.take()
    }

    /// Take the right child, leaving None in its place.
    pub const fn take_right(&mut self) -> Option<Box<NumericRangeNode>> {
        self.right.take()
    }

    /// Get a reference to the range, if present.
    pub const fn range(&self) -> Option<&NumericRange> {
        self.range.as_ref()
    }

    /// Get a mutable reference to the range, if present.
    pub const fn range_mut(&mut self) -> Option<&mut NumericRange> {
        self.range.as_mut()
    }

    /// Take the range from this node, leaving None in its place.
    pub const fn take_range(&mut self) -> Option<NumericRange> {
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
        Self::new_leaf(false)
    }
}
