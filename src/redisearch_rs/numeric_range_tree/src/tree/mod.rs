/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Numeric range tree implementation.
//!
//! This module contains the core tree structure and algorithms for the numeric
//! range tree, including insertion, splitting, balancing, and range queries.
//!
//! The implementation is split into sub-modules by concern:
//! - [`insert`]: Write path (add, split, balance)
//! - [`find`]: Read path (range queries)
//! - [`gc`][]: Maintenance (garbage collection, trimming, compaction)

mod find;
mod gc;
mod insert;
#[cfg(all(feature = "unittest", not(miri)))]
mod invariants;

pub use gc::{CompactIfSparseResult, NodeGcDelta, SingleNodeGcResult};

use ffi::t_docId;

use crate::NumericRangeNode;
use crate::arena::{NodeArena, NodeIndex};
use crate::unique_id::TreeUniqueId;

/// Result of adding a value to the tree.
///
/// This captures the changes that occurred during the add operation,
/// including memory growth and structural changes. The delta fields use
/// signed types to support both growth (positive) and shrinkage (negative)
/// during operations like trimming empty leaves.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(C)]
pub struct AddResult {
    /// The change in the tree's inverted index memory usage, in bytes.
    /// Positive values indicate growth, negative values indicate shrinkage.
    /// This tracks only inverted index memory, not node/range struct overhead.
    pub size_delta: i64,
    /// The net change in the number of records (document, value entries).
    /// When splitting, this counts re-added entries to child ranges.
    /// When trimming, this is negative for removed entries.
    pub num_records_delta: i32,
    /// Whether the tree structure changed (splits or rotations occurred).
    /// When true, the tree's `revision_id` should be incremented to
    /// invalidate any concurrent iterators.
    pub changed: bool,
    /// The net change in the number of ranges (nodes with inverted indexes).
    /// Splitting a leaf adds one or two new ranges. Trimming removes ranges.
    pub num_ranges_delta: i32,
    /// The net change in the number of leaf nodes.
    /// Splitting a leaf adds one new leaf. Trimming decreases this.
    pub num_leaves_delta: i32,
}

/// Result of trimming empty leaves from the tree.
///
/// Similar to [`AddResult`] but without `num_records_delta`, since trimming
/// only removes empty nodes and does not change the number of entries
/// (entries are removed by GC before trimming).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(C)]
pub struct TrimEmptyLeavesResult {
    /// The change in the tree's inverted index memory usage, in bytes.
    /// Positive values indicate growth, negative values indicate shrinkage.
    pub size_delta: i64,
    /// Whether the tree structure changed (nodes were removed or rotated).
    /// When true, the tree's `revision_id` should be incremented to
    /// invalidate any concurrent iterators.
    pub changed: bool,
    /// The net change in the number of ranges (nodes with inverted indexes).
    pub num_ranges_delta: i32,
    /// The net change in the number of leaf nodes.
    pub num_leaves_delta: i32,
}

/// Aggregate statistics for a [`NumericRangeTree`].
///
/// Tracks counts of ranges, leaves, entries, memory usage, and
/// empty leaves. Updated incrementally during insertions, splits,
/// GC, and trimming.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct TreeStats {
    /// Total number of ranges (nodes with inverted indexes).
    pub num_ranges: usize,
    /// Total number of leaf nodes.
    pub num_leaves: usize,
    /// Total number of (document, value) entries.
    pub num_entries: usize,
    /// Total memory used by inverted indexes in bytes.
    pub inverted_indexes_size: usize,
    /// Number of empty leaves (for GC tracking).
    pub empty_leaves: usize,
}

/// A numeric range tree for efficient range queries over numeric values.
///
/// The tree organizes documents by their numeric field values into a balanced
/// binary tree of ranges. Each leaf node contains a range of values, and
/// internal nodes may optionally retain their ranges for query efficiency.
///
/// # Arena Storage
///
/// All nodes are stored in a [`NodeArena`]. Children are referenced by
/// [`NodeIndex`] instead of `Box<NumericRangeNode>`. This provides better
/// cache locality, eliminates per-node heap allocation overhead, and makes
/// pruning cheaper (index swaps and a single `realloc` rather than a dealloc
/// for every deleted node).
///
/// # Splitting Strategy
///
/// Nodes split based on two conditions:
///
/// - **Cardinality threshold**: When the HyperLogLog-estimated cardinality exceeds
///   a depth-dependent limit. The threshold is [`Self::MINIMUM_RANGE_CARDINALITY`] at depth 0,
///   growing by a factor of [`Self::CARDINALITY_GROWTH_FACTOR`] per depth level until capped
///   at [`Self::MAXIMUM_RANGE_CARDINALITY`].
///
/// - **Size overflow**: When entry count exceeds [`Self::MAXIMUM_RANGE_SIZE`] and
///   cardinality > 1. This handles cases where many documents share few values.
///   The cardinality check prevents splitting single-value ranges indefinitely.
///
/// # Balancing
///
/// The tree uses AVL-like single rotations when depth imbalance exceeds
/// [`Self::MAXIMUM_DEPTH_IMBALANCE`].
#[derive(Debug)]
pub struct NumericRangeTree {
    /// The root node index.
    root: NodeIndex,
    /// Arena holding all tree nodes.
    nodes: NodeArena,
    /// Aggregate statistics for the tree.
    stats: TreeStats,
    /// The last document ID added to the tree.
    last_doc_id: t_docId,
    /// Revision ID, incremented when the tree structure changes (splits/rotations).
    ///
    /// When `revision_id != 0`, it indicates the tree nodes have changed and
    /// concurrent iteration may not be safe. Iterators like
    /// [`NumericRangeTreeIterator`](crate::NumericRangeTreeIterator) should check this value and abort if it
    /// changes during iteration, as the tree structure they were traversing
    /// may no longer be valid.
    revision_id: u32,
    /// Unique identifier for this tree instance.
    unique_id: TreeUniqueId,
    /// Whether to use float compression for numeric values.
    compress_floats: bool,
}

impl NumericRangeTree {
    /// Minimum cardinality before considering splitting (at depth 0).
    ///
    /// At depth 0, we require at least this many distinct values before splitting.
    /// This prevents excessive splitting for low-cardinality fields.
    pub const MINIMUM_RANGE_CARDINALITY: usize = 16;

    /// Maximum cardinality threshold for splitting.
    ///
    /// Once the split threshold reaches this value, it stays constant regardless
    /// of depth. This caps the maximum number of distinct values in any leaf range.
    pub const MAXIMUM_RANGE_CARDINALITY: usize = 2500;

    /// Maximum number of entries in a range before forcing a split (if cardinality > 1).
    ///
    /// Even if cardinality is below the threshold, we split if a range accumulates
    /// too many entries. This handles cases where many documents share few values.
    /// The cardinality > 1 check prevents splitting single-value ranges indefinitely.
    pub const MAXIMUM_RANGE_SIZE: usize = 10000;

    /// Maximum depth imbalance before rebalancing.
    ///
    /// We use AVL-like rotations when one subtree's depth exceeds the other by
    /// more than this value.
    pub const MAXIMUM_DEPTH_IMBALANCE: u32 = 2;

    /// Cardinality growth factor per depth level.
    ///
    /// The split cardinality threshold multiplies by this factor for each depth
    /// level, capped at [`Self::MAXIMUM_RANGE_CARDINALITY`].
    pub const CARDINALITY_GROWTH_FACTOR: usize = 4;

    /// Create a new empty numeric range tree.
    ///
    /// If `compress_floats` is true, the tree will use float compression.
    /// Check out [`NumericFloatCompression`][`inverted_index::numeric::NumericFloatCompression`] for more information.
    pub fn new(compress_floats: bool) -> Self {
        let mut nodes = NodeArena::new();
        let root_node = NumericRangeNode::leaf(compress_floats);
        let inverted_indexes_size = root_node.range().map(|r| r.memory_usage()).unwrap_or(0);
        let root_index = nodes.insert(root_node);

        Self {
            root: root_index,
            nodes,
            stats: TreeStats {
                num_ranges: 1,
                num_leaves: 1,
                num_entries: 0,
                inverted_indexes_size,
                empty_leaves: 1,
            },
            last_doc_id: 0,
            revision_id: 0,
            unique_id: TreeUniqueId::next(),
            compress_floats,
        }
    }

    /// Resolve a [`NodeIndex`] to a shared reference to the node.
    pub fn node(&self, idx: NodeIndex) -> &NumericRangeNode {
        &self.nodes[idx]
    }

    /// Resolve a [`NodeIndex`] to a mutable reference to the node.
    pub fn node_mut(&mut self, idx: NodeIndex) -> &mut NumericRangeNode {
        &mut self.nodes[idx]
    }

    /// Get a reference to the root node.
    pub fn root(&self) -> &NumericRangeNode {
        &self.nodes[self.root]
    }

    /// Get the root node index.
    pub const fn root_index(&self) -> NodeIndex {
        self.root
    }

    /// Get the total number of ranges in the tree.
    pub const fn num_ranges(&self) -> usize {
        self.stats.num_ranges
    }

    /// Get the total number of leaf nodes in the tree.
    pub const fn num_leaves(&self) -> usize {
        self.stats.num_leaves
    }

    /// Get the total number of entries in the tree.
    pub const fn num_entries(&self) -> usize {
        self.stats.num_entries
    }

    /// Get the total memory used by inverted indexes in bytes.
    pub const fn inverted_indexes_size(&self) -> usize {
        self.stats.inverted_indexes_size
    }

    /// Get the last document ID added to the tree.
    pub const fn last_doc_id(&self) -> t_docId {
        self.last_doc_id
    }

    /// Get the revision ID of the tree.
    pub const fn revision_id(&self) -> u32 {
        self.revision_id
    }

    /// Increment the revision ID.
    ///
    /// This method is never needed in production code: the tree
    /// revision ID is automatically incremented when the tree structure changes.
    ///
    /// This method is provided primarily for testing purposes—e.g. to force the invalidation
    /// of an iterator built on top of this tree in GC tests.
    pub const fn increment_revision(&mut self) -> u32 {
        self.revision_id = self.revision_id.wrapping_add(1);
        self.revision_id
    }

    /// Get the unique identifier for this tree.
    pub const fn unique_id(&self) -> TreeUniqueId {
        self.unique_id
    }

    /// Get the number of empty leaves (for GC tracking).
    pub const fn empty_leaves(&self) -> usize {
        self.stats.empty_leaves
    }

    /// Returns an iterator over all nodes in the tree (depth-first traversal).
    pub fn iter(&self) -> crate::ReversePreOrderDfsIterator<'_> {
        crate::ReversePreOrderDfsIterator::new(self)
    }

    /// Returns an iterator over all nodes in the tree, alongside their indices (depth-first traversal).
    pub fn indexed_iter(&self) -> crate::IndexedReversePreOrderDfsIterator<'_> {
        crate::IndexedReversePreOrderDfsIterator::new(self)
    }

    /// Calculate the total memory usage of the tree, in bytes.
    pub const fn mem_usage(&self) -> usize {
        std::mem::size_of::<Self>() + self.stats.inverted_indexes_size + self.nodes.mem_usage()
    }
}

impl Default for NumericRangeTree {
    fn default() -> Self {
        Self::new(false)
    }
}

/// Apply a signed delta to an unsigned value, panicking at bounds instead of wrapping.
const fn apply_signed_delta(value: usize, delta: i64) -> usize {
    if delta < 0 {
        value.checked_sub((-delta) as usize).expect("Underflow!")
    } else {
        value.checked_add(delta as usize).expect("Overflow!")
    }
}
