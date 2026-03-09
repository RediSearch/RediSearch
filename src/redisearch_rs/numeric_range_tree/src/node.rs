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
//!
//! When stored in a [`NumericRangeTree`](crate::NumericRangeTree), nodes live
//! in a [`NodeArena`] and children are referenced by [`NodeIndex`].

use crate::NumericRange;
use crate::arena::{NodeArena, NodeIndex};

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
    pub(crate) max_depth: u32,

    /// Left child subtree (values < split value).
    pub(crate) left: NodeIndex,

    /// Right child subtree (values >= split value).
    pub(crate) right: NodeIndex,

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
    /// Get the left child index.
    pub const fn left_index(&self) -> NodeIndex {
        self.left
    }

    /// Get the right child index.
    pub const fn right_index(&self) -> NodeIndex {
        self.right
    }

    /// Perform a left rotation on the node at `node_idx`.
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
    /// With arena allocation, rotation is performed by swapping data between
    /// arena slots and reassigning indices. No allocation or deallocation occurs.
    ///
    /// Returns the dropped ranges from the promoted node and the demoted node (if any),
    /// so the caller can update tree statistics.
    ///
    /// If the right child is a [`Leaf`](NumericRangeNode::Leaf), rotation is
    /// impossible and the node is left unchanged.
    pub(crate) fn rotate_left(
        nodes: &mut NodeArena,
        node_idx: NodeIndex,
    ) -> Option<(Option<NumericRange>, Option<NumericRange>)> {
        let NumericRangeNode::Internal(node) = &nodes[node_idx] else {
            return None;
        };
        let right_idx = node.right;

        let NumericRangeNode::Internal(_) = &nodes[right_idx] else {
            // Right child is a leaf — nothing to rotate.
            return None;
        };

        // Extract data from the right child (B).
        let NumericRangeNode::Internal(right_node) = std::mem::take(&mut nodes[right_idx]) else {
            unreachable!()
        };
        let promoted_value = right_node.value;
        let promoted_range = right_node.range;
        let promoted_right_left = right_node.left; // y
        let promoted_right_right = right_node.right; // z

        // Extract data from the current node (A).
        let NumericRangeNode::Internal(current_node) = std::mem::take(&mut nodes[node_idx]) else {
            unreachable!()
        };
        let demoted_value = current_node.value;
        let demoted_range = current_node.range;
        let demoted_left = current_node.left; // x

        // Build demoted node (old A) in right_idx's slot.
        let demoted_depth = nodes[demoted_left]
            .max_depth()
            .max(nodes[promoted_right_left].max_depth())
            + 1;
        nodes[right_idx] = NumericRangeNode::Internal(InternalNode {
            value: demoted_value,
            max_depth: demoted_depth,
            left: demoted_left,
            right: promoted_right_left,
            range: None,
        });

        // Build promoted node (old B) in node_idx's slot.
        // The promoted node's range is discarded because it no longer covers
        // the full subtree. We must also discard the demoted node's range.
        // Example:
        //
        //   Before:                                 After:
        //         A [10,80]                               B [50,80] ← INVALID
        //        / \                                     / \
        //       x   B [50,80]        TOO BROAD->[10,80] A   z
        //    [10,30]  / \                              / \  [70,80]
        //            y   z                            x   y
        //         [50,60] [70,80]                  [10,30] [50,60]
        //
        // B's range [50,80] was for {y,z}. After rotation it governs {x,y,z},
        // so a query for [10,20] would see no overlap and skip x's results.
        // A's range [10,80] was for {B,y,z}. After rotation, it governs {x,y},
        // which is a smaller range than before.
        let promoted_depth = demoted_depth.max(nodes[promoted_right_right].max_depth()) + 1;
        nodes[node_idx] = NumericRangeNode::Internal(InternalNode {
            value: promoted_value,
            max_depth: promoted_depth,
            left: right_idx, // demoted node is now left child
            right: promoted_right_right,
            range: None,
        });

        Some((promoted_range, demoted_range))
    }

    /// Perform a right rotation on the node at `node_idx`.
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
    /// Returns the dropped range from the promoted node (if any), so the caller
    /// can update tree statistics.
    ///
    /// If the left child is a [`Leaf`](NumericRangeNode::Leaf), rotation is
    /// impossible and the node is left unchanged.
    pub(crate) fn rotate_right(
        nodes: &mut NodeArena,
        node_idx: NodeIndex,
    ) -> Option<(Option<NumericRange>, Option<NumericRange>)> {
        let NumericRangeNode::Internal(node) = &nodes[node_idx] else {
            return None;
        };
        let left_idx = node.left;

        let NumericRangeNode::Internal(_) = &nodes[left_idx] else {
            // Left child is a leaf — nothing to rotate.
            return None;
        };

        // Extract data from the left child (B).
        let NumericRangeNode::Internal(left_node) = std::mem::take(&mut nodes[left_idx]) else {
            unreachable!()
        };
        let promoted_value = left_node.value;
        let promoted_range = left_node.range;
        let promoted_left_left = left_node.left; // x
        let promoted_left_right = left_node.right; // y

        // Extract data from the current node (A).
        let NumericRangeNode::Internal(current_node) = std::mem::take(&mut nodes[node_idx]) else {
            unreachable!()
        };
        let demoted_value = current_node.value;
        let demoted_range = current_node.range;
        let demoted_right = current_node.right; // z

        // Build demoted node (old A) in left_idx's slot.
        let demoted_depth = nodes[promoted_left_right]
            .max_depth()
            .max(nodes[demoted_right].max_depth())
            + 1;
        nodes[left_idx] = NumericRangeNode::Internal(InternalNode {
            value: demoted_value,
            max_depth: demoted_depth,
            left: promoted_left_right,
            right: demoted_right,
            range: None,
        });

        // Build promoted node (old B) in node_idx's slot.
        // The promoted node's range is discarded because it no longer covers
        // the full subtree. Example:
        //
        //   Before:                         After:
        //             A [10,80]                      B [10,50] ← INVALID
        //            / \                            / \
        //   [10,50] B   z [60,80]         [10, 30] x   A [10,80] ← TOO BROAD
        //          / \                                / \
        //         x   y                              y   z
        //    [10,30] [40,50]                    [40,50] [60,80]
        //
        // B's range [10,50] was for {x,y}. After rotation it governs {x,y,z},
        // so a query for [70,80] would see no overlap and skip z's results.
        // A's range [10,80] was for {x,y,z}. After rotation it governs {y,z},
        // a smaller range.
        let promoted_depth = nodes[promoted_left_left].max_depth().max(demoted_depth) + 1;
        nodes[node_idx] = NumericRangeNode::Internal(InternalNode {
            value: promoted_value,
            max_depth: promoted_depth,
            left: promoted_left_left,
            right: left_idx, // demoted node is now right child
            range: None,
        });

        Some((promoted_range, demoted_range))
    }
}

/// A node in the numeric range tree.
///
/// Nodes are either:
/// - **Leaf nodes**: Have a range but no children.
/// - **Internal nodes**: Have both children, a split value, depth tracking,
///   and optionally retain a range for query efficiency.
///
/// When part of a [`NumericRangeTree`](crate::NumericRangeTree), nodes are
/// stored in a [`generational_slab::Slab`] arena and referenced by [`NodeIndex`].
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

    /// Create a new internal node with arena-indexed children.
    ///
    /// Computes `max_depth` automatically from the children's depths looked up in the arena.
    pub(crate) fn internal_indexed(
        value: f64,
        left: NodeIndex,
        right: NodeIndex,
        range: Option<NumericRange>,
        nodes: &NodeArena,
    ) -> Self {
        let max_depth = nodes[left].max_depth().max(nodes[right].max_depth()) + 1;
        Self::Internal(InternalNode {
            value,
            max_depth,
            left,
            right,
            range,
        })
    }

    /// Check if this node is a leaf (no children).
    pub const fn is_leaf(&self) -> bool {
        matches!(self, Self::Leaf(_))
    }

    /// Get the split value.
    ///
    /// Returns `None` for leaf nodes.
    pub const fn split_value(&self) -> Option<f64> {
        match self {
            Self::Leaf(_) => None,
            Self::Internal(internal) => Some(internal.value),
        }
    }

    /// Get the maximum depth of the subtree rooted at this node.
    ///
    /// Returns `0` for leaf nodes.
    pub const fn max_depth(&self) -> u32 {
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
    #[cfg(not(feature = "test-utils"))]
    pub(crate) const fn range_mut(&mut self) -> Option<&mut NumericRange> {
        match self {
            Self::Leaf(leaf) => Some(&mut leaf.range),
            Self::Internal(internal) => internal.range.as_mut(),
        }
    }

    /// Get a mutable reference to the range, if present.
    ///
    /// Always `Some` for leaf nodes, optional for internal nodes.
    // TODO: Used by `rqe_iterators_test_utils/src/test_context.rs` for testing purposes.
    //   Make it private again after we don't need it anymore as a workaround.
    #[cfg(feature = "test-utils")]
    pub const fn range_mut(&mut self) -> Option<&mut NumericRange> {
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
                Some(std::mem::take(&mut leaf.range))
            }
            Self::Internal(internal) => internal.range.take(),
        }
    }

    /// Get the child indices, if this is an internal node.
    ///
    /// Returns `None` for leaf nodes.
    pub const fn child_indices(&self) -> Option<(NodeIndex, NodeIndex)> {
        match self {
            Self::Leaf(_) => None,
            Self::Internal(internal) => Some((internal.left, internal.right)),
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
