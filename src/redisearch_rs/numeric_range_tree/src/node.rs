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
//! This module defines the node enum used in the numeric range tree.
//! Nodes form a binary tree where internal nodes partition the value space
//! and leaf nodes store the actual document-value entries.

use crate::NumericRange;

/// A leaf node containing a range of document-value entries.
#[derive(Debug)]
pub struct LeafNode {
    /// The numeric range containing document-value entries.
    /// Always present on leaf nodes.
    pub(crate) range: NumericRange,
}

/// An internal node that partitions the value space.
#[derive(Debug)]
pub struct InternalNode {
    /// The split value for routing values to children.
    /// Values < `value` go to `left`, values >= `value` go to `right`.
    pub(crate) value: f64,

    /// Maximum depth of the subtree rooted at this node.
    ///
    /// Used for AVL-like balancing. Equals `max(left.max_depth, right.max_depth) + 1`.
    ///
    /// Note: This is `i32` (not `usize`) to match the C implementation and allow
    /// signed arithmetic when computing depth differences for balance checks.
    pub(crate) max_depth: i32,

    /// Left child subtree (values < split value).
    pub(crate) left: Box<NumericRangeNode>,

    /// Right child subtree (values >= split value).
    pub(crate) right: Box<NumericRangeNode>,

    /// The numeric range containing document-value entries.
    ///
    /// `Some` if the range is retained for query optimization
    /// (when `max_depth <= max_depth_range`), `None` if trimmed to save memory.
    ///
    /// When present, the range contains all entries from the
    /// entire subtree, enabling queries that span the full range to use this
    /// single range instead of unioning all descendant ranges.
    pub(crate) range: Option<NumericRange>,
}

impl InternalNode {
    /// Get the left child subtree.
    pub fn left(&self) -> &NumericRangeNode {
        &self.left
    }

    /// Get the right child subtree.
    pub fn right(&self) -> &NumericRangeNode {
        &self.right
    }

    /// Perform a left rotation, returning the new root.
    ///
    /// The right child (`B`) is promoted to root and the old root (`A`) becomes
    /// `B`'s left child. `B`'s former left subtree (`y`) is re-parented as `A`'s
    /// new right child.
    ///
    /// ```text
    ///       A              B
    ///      / \            / \
    ///     x   B    =>   A   z
    ///        / \       / \
    ///       y   z     x   y
    /// ```
    ///
    /// If the right child is a [`Leaf`](NumericRangeNode::Leaf), rotation is
    /// impossible and the node is returned unchanged.
    pub(crate) fn rotate_left(self) -> NumericRangeNode {
        let InternalNode {
            value,
            left,
            right,
            range,
            max_depth,
        } = self;

        let NumericRangeNode::Internal(right_internal) = *right else {
            // Right child is a leaf — nothing to rotate, reconstruct unchanged.
            return NumericRangeNode::Internal(InternalNode {
                value,
                max_depth,
                left,
                right,
                range,
            });
        };

        let InternalNode {
            value: promoted_value,
            left: right_left,
            right: right_right,
            range: promoted_range,
            ..
        } = right_internal;

        // Build the demoted node (old root becomes left child of promoted node).
        let demoted_depth = left.max_depth().max(right_left.max_depth()) + 1;
        let demoted = Box::new(NumericRangeNode::Internal(InternalNode {
            value,
            max_depth: demoted_depth,
            left,
            right: right_left,
            range,
        }));

        // Build the promoted node (old right child becomes new root).
        let promoted_depth = demoted_depth.max(right_right.max_depth()) + 1;
        NumericRangeNode::Internal(InternalNode {
            value: promoted_value,
            max_depth: promoted_depth,
            left: demoted,
            right: right_right,
            range: promoted_range,
        })
    }

    /// Perform a right rotation, returning the new root.
    ///
    /// The left child (`B`) is promoted to root and the old root (`A`) becomes
    /// `B`'s right child. `B`'s former right subtree (`y`) is re-parented as
    /// `A`'s new left child.
    ///
    /// ```text
    ///       A            B
    ///      / \          / \
    ///     B   z   =>   x   A
    ///    / \               / \
    ///   x   y             y   z
    /// ```
    ///
    /// If the left child is a [`Leaf`](NumericRangeNode::Leaf), rotation is
    /// impossible and the node is returned unchanged.
    pub(crate) fn rotate_right(self) -> NumericRangeNode {
        let InternalNode {
            value,
            left,
            right,
            range,
            max_depth,
        } = self;

        let NumericRangeNode::Internal(left_internal) = *left else {
            // Left child is a leaf — nothing to rotate, reconstruct unchanged.
            return NumericRangeNode::Internal(InternalNode {
                value,
                max_depth,
                left,
                right,
                range,
            });
        };

        let InternalNode {
            value: promoted_value,
            left: left_left,
            right: left_right,
            range: promoted_range,
            ..
        } = left_internal;

        // Build the demoted node (old root becomes right child of promoted node).
        let demoted_depth = left_right.max_depth().max(right.max_depth()) + 1;
        let demoted = Box::new(NumericRangeNode::Internal(InternalNode {
            value,
            max_depth: demoted_depth,
            left: left_right,
            right,
            range,
        }));

        // Build the promoted node (old left child becomes new root).
        let promoted_depth = left_left.max_depth().max(demoted_depth) + 1;
        NumericRangeNode::Internal(InternalNode {
            value: promoted_value,
            max_depth: promoted_depth,
            left: left_left,
            right: demoted,
            range: promoted_range,
        })
    }
}

/// A node in the numeric range tree.
///
/// Nodes are either:
/// - **Leaf nodes**: Have a range but no children.
/// - **Internal nodes**: Have both children, a split value, depth tracking,
///   and optionally retain a range for query efficiency.
#[derive(Debug)]
pub enum NumericRangeNode {
    /// A leaf node containing a range of document-value entries.
    Leaf(LeafNode),
    /// An internal node that partitions the value space.
    Internal(InternalNode),
}

impl NumericRangeNode {
    /// Create a new leaf node with an empty range.
    ///
    /// If `compress_floats` is true, the range will use float compression.
    pub fn leaf(compress_floats: bool) -> Self {
        Self::Leaf(LeafNode {
            range: NumericRange::new(compress_floats),
        })
    }

    /// Create a new internal node with the given split value, children, and optional range.
    ///
    /// Computes `max_depth` automatically from the children's depths.
    pub fn internal(
        value: f64,
        left: NumericRangeNode,
        right: NumericRangeNode,
        range: Option<NumericRange>,
    ) -> Self {
        let max_depth = left.max_depth().max(right.max_depth()) + 1;
        Self::Internal(InternalNode {
            value,
            max_depth,
            left: Box::new(left),
            right: Box::new(right),
            range,
        })
    }

    /// Check if this node is a leaf (no children).
    pub const fn is_leaf(&self) -> bool {
        matches!(self, Self::Leaf(_))
    }

    /// Get the split value.
    ///
    /// Returns `0.0` for leaf nodes.
    pub const fn split_value(&self) -> f64 {
        match self {
            Self::Leaf(_) => 0.0,
            Self::Internal(internal) => internal.value,
        }
    }

    /// Get the maximum depth of the subtree rooted at this node.
    ///
    /// Returns `0` for leaf nodes.
    pub const fn max_depth(&self) -> i32 {
        match self {
            Self::Leaf(_) => 0,
            Self::Internal(internal) => internal.max_depth,
        }
    }

    /// Get a reference to the range, if present.
    ///
    /// Always `Some` for leaf nodes, optional for internal nodes.
    pub const fn range(&self) -> Option<&NumericRange> {
        match self {
            Self::Leaf(leaf) => Some(&leaf.range),
            Self::Internal(internal) => internal.range.as_ref(),
        }
    }

    /// Get a mutable reference to the range, if present.
    ///
    /// Always `Some` for leaf nodes, optional for internal nodes.
    pub(crate) const fn range_mut(&mut self) -> Option<&mut NumericRange> {
        match self {
            Self::Leaf(leaf) => Some(&mut leaf.range),
            Self::Internal(internal) => internal.range.as_mut(),
        }
    }

    /// Take the range from this node, leaving `None` in its place for internal nodes.
    ///
    /// For leaf nodes, this takes the range and replaces the node with a leaf
    /// containing a default (empty, uncompressed) range — callers that take a
    /// leaf's range typically replace the entire node immediately after.
    pub(crate) fn take_range(&mut self) -> Option<NumericRange> {
        match self {
            Self::Leaf(leaf) => {
                // Replace with a default range; callers are expected to replace the whole node.
                Some(std::mem::replace(&mut leaf.range, NumericRange::new(false)))
            }
            Self::Internal(internal) => internal.range.take(),
        }
    }

    /// Get the left and right children, if this is an internal node.
    ///
    /// Returns `None` for leaf nodes.
    pub const fn children(&self) -> Option<(&NumericRangeNode, &NumericRangeNode)> {
        match self {
            Self::Leaf(_) => None,
            Self::Internal(internal) => Some((&internal.left, &internal.right)),
        }
    }

    /// Get mutable references to the left and right children, if this is an internal node.
    ///
    /// Returns `None` for leaf nodes.
    pub const fn children_mut(&mut self) -> Option<(&mut NumericRangeNode, &mut NumericRangeNode)> {
        match self {
            Self::Leaf(_) => None,
            Self::Internal(internal) => Some((&mut internal.left, &mut internal.right)),
        }
    }

    /// Check if this node has a range.
    ///
    /// Always `true` for leaf nodes.
    pub const fn has_range(&self) -> bool {
        match self {
            Self::Leaf(_) => true,
            Self::Internal(internal) => internal.range.is_some(),
        }
    }
}

impl Default for NumericRangeNode {
    fn default() -> Self {
        Self::leaf(false)
    }
}
