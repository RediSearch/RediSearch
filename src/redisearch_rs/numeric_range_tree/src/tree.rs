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

use ffi::t_docId;
use inverted_index::{GcApplyInfo, GcScanDelta, IndexReader, NumericFilter, RSIndexResult};

use crate::{NumericRange, NumericRangeNode, NumericRangeReader};

// ============================================================================
// Constants
// ============================================================================
//
// These constants control the tree's splitting and balancing behavior.
// They are calibrated to balance memory usage against query performance.

/// Minimum cardinality before considering splitting (at depth 0).
///
/// At depth 0, we require at least this many distinct values before splitting.
/// This prevents excessive splitting for low-cardinality fields.
const MINIMUM_RANGE_CARDINALITY: usize = 16;

/// Maximum cardinality threshold for splitting.
///
/// Once the split threshold reaches this value, it stays constant regardless
/// of depth. This caps the maximum number of distinct values in any leaf range.
const MAXIMUM_RANGE_CARDINALITY: usize = 2500;

/// Maximum number of entries in a range before forcing a split (if cardinality > 1).
///
/// Even if cardinality is below the threshold, we split if a range accumulates
/// too many entries. This handles cases where many documents share few values.
/// The cardinality > 1 check prevents splitting single-value ranges indefinitely.
const MAXIMUM_RANGE_SIZE: usize = 10000;

/// Maximum depth imbalance before rebalancing.
///
/// We use AVL-like rotations when one subtree's depth exceeds the other by
/// more than this value.
const MAXIMUM_DEPTH_IMBALANCE: i32 = 2;

/// Cardinality growth factor per depth level.
///
/// The split cardinality threshold multiplies by this factor for each depth
/// level, capped at [`MAXIMUM_RANGE_CARDINALITY`].
const CARDINALITY_GROWTH_FACTOR: usize = 4;

/// Last depth level that does NOT use the maximum split cardinality.
const LAST_DEPTH_WITHOUT_MAXIMUM_CARDINALITY: usize = {
    let ratio = MAXIMUM_RANGE_CARDINALITY / MINIMUM_RANGE_CARDINALITY;
    ratio.ilog2() as usize / CARDINALITY_GROWTH_FACTOR.ilog2() as usize
};

/// Calculate the split cardinality threshold for a given depth.
///
/// The threshold grows exponentially by a factor of [`CARDINALITY_GROWTH_FACTOR`]
/// per depth level until reaching [`MAXIMUM_RANGE_CARDINALITY`]. This allows
/// shallow nodes to hold fewer distinct values, pushing data down to leaves
/// for better query selectivity.
const fn get_split_cardinality(depth: usize) -> usize {
    if depth > LAST_DEPTH_WITHOUT_MAXIMUM_CARDINALITY {
        MAXIMUM_RANGE_CARDINALITY
    } else {
        MINIMUM_RANGE_CARDINALITY * CARDINALITY_GROWTH_FACTOR.pow(depth as u32)
    }
}

/// Result of adding a value to the tree.
///
/// This captures the changes that occurred during the add operation,
/// including memory growth and structural changes. The delta fields use
/// signed types to support both growth (positive) and shrinkage (negative)
/// during operations like trimming empty leaves.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AddResult {
    /// The change in the tree's inverted index memory usage, in bytes.
    /// Positive values indicate growth, negative values indicate shrinkage.
    /// This tracks only inverted index memory, not node/range struct overhead.
    pub size_delta: i64,
    /// The net change in the number of records (document, value entries).
    /// When splitting, this counts re-added entries to child ranges.
    /// When trimming, this is negative for removed entries.
    pub num_records: i32,
    /// Whether the tree structure changed (splits or rotations occurred).
    /// When true, the tree's `revision_id` should be incremented to
    /// invalidate any concurrent iterators.
    pub changed: bool,
    /// The net change in the number of ranges (nodes with inverted indexes).
    /// Splitting a leaf adds 2 new ranges. Trimming removes ranges.
    pub num_ranges_delta: i32,
    /// The net change in the number of leaf nodes.
    /// Splitting a leaf into two children adds 1 leaf (+2 new, -0 since parent
    /// becomes internal but was already counted). Trimming decreases this.
    pub num_leaves_delta: i32,
}

/// A numeric range tree for efficient range queries over numeric values.
///
/// The tree organizes documents by their numeric field values into a balanced
/// binary tree of ranges. Each leaf node contains a range of values, and
/// internal nodes may optionally retain their ranges for query efficiency.
///
/// # Splitting Strategy
///
/// Nodes split based on two conditions:
///
/// - **Cardinality threshold**: When the HyperLogLog-estimated cardinality exceeds
///   a depth-dependent limit. The threshold is [`MINIMUM_RANGE_CARDINALITY`] at depth 0,
///   growing by a factor of 4 per depth level until capped at [`MAXIMUM_RANGE_CARDINALITY`].
///
/// - **Size overflow**: When entry count exceeds [`MAXIMUM_RANGE_SIZE`] and
///   cardinality > 1. This handles cases where many documents share few values.
///   The cardinality check prevents splitting single-value ranges indefinitely.
///
/// # Balancing
///
/// The tree uses AVL-like single rotations when depth imbalance exceeds
/// [`MAXIMUM_DEPTH_IMBALANCE`]. To simplify rebalancing, we don't attempt to merge
/// or redistribute ranges during rotation.
#[derive(Debug)]
#[allow(rustdoc::private_intra_doc_links)]
pub struct NumericRangeTree {
    /// The root node of the tree.
    root: NumericRangeNode,
    /// Total number of ranges in the tree.
    num_ranges: usize,
    /// Total number of leaf nodes in the tree.
    num_leaves: usize,
    /// Total number of entries (document, value) pairs in the tree.
    num_entries: usize,
    /// Total memory used by inverted indexes in bytes.
    inverted_indexes_size: usize,
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
    unique_id: u32,
    /// Number of empty leaves (for GC tracking).
    empty_leaves: usize,
    /// Whether to use float compression for numeric values.
    compress_floats: bool,
}

/// Global counter for unique tree IDs.
static UNIQUE_ID_COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

impl NumericRangeTree {
    /// Create a new empty numeric range tree.
    ///
    /// If `compress_floats` is true, the tree will use float compression.
    /// Check out [`NumericFloatCompression`][`inverted_index::numeric::NumericFloatCompression`] for more information.
    pub fn new(compress_floats: bool) -> Self {
        let root = NumericRangeNode::leaf(compress_floats);
        let inverted_indexes_size = root.range().map(|r| r.memory_usage()).unwrap_or(0);

        Self {
            root,
            num_ranges: 1,
            num_leaves: 1,
            num_entries: 0,
            inverted_indexes_size,
            last_doc_id: 0,
            revision_id: 0,
            unique_id: UNIQUE_ID_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            empty_leaves: 0,
            compress_floats,
        }
    }

    /// Add a (docId, value) pair to the tree.
    ///
    /// If `allow_duplicates` is true, duplicate document IDs are allowed.
    /// Returns information about what changed during the add operation.
    pub fn add(
        &mut self,
        doc_id: t_docId,
        value: f64,
        allow_duplicates: bool,
        max_depth_range: usize,
    ) -> AddResult {
        if doc_id <= self.last_doc_id && !allow_duplicates {
            // Skip duplicate doc_id
            return AddResult::default();
        }
        self.last_doc_id = doc_id;

        let mut rv = AddResult::default();
        Self::node_add(
            &mut self.root,
            doc_id,
            value,
            &mut rv,
            0,
            max_depth_range,
            self.compress_floats,
        );

        // If the tree structure changed, increment the revision ID
        if rv.changed {
            self.revision_id = self.revision_id.wrapping_add(1);
        }

        self.num_ranges = (self.num_ranges as i64 + rv.num_ranges_delta as i64) as usize;
        self.num_leaves = (self.num_leaves as i64 + rv.num_leaves_delta as i64) as usize;
        self.num_entries += 1;
        self.inverted_indexes_size = (self.inverted_indexes_size as i64 + rv.size_delta) as usize;

        rv
    }

    /// Recursive add implementation.
    ///
    /// Descends the tree to find the appropriate leaf node for the value,
    /// adds the entry, and handles splitting and balancing on the way back up.
    ///
    /// # Algorithm
    ///
    /// 1. **Internal nodes**: Recurse into the appropriate child based on the
    ///    split value. If this node retains a range, also add the entry there
    ///    (without updating cardinality, since cardinality is only tracked at
    ///    leaves). Balance if the subtree changed.
    ///
    /// 2. **Leaf nodes**: Update cardinality via HyperLogLog, add the entry,
    ///    then check if splitting is needed based on cardinality threshold or
    ///    size overflow.
    ///
    /// # Cardinality Update Responsibility
    ///
    /// Cardinality is ONLY updated at leaf nodes. When adding to retained
    /// ranges in internal nodes, we call `add_without_cardinality` because
    /// the cardinality is already tracked by the leaf descendants.
    ///
    fn node_add(
        node: &mut NumericRangeNode,
        doc_id: t_docId,
        value: f64,
        rv: &mut AddResult,
        depth: usize,
        max_depth_range: usize,
        compress_floats: bool,
    ) {
        match node {
            NumericRangeNode::Internal(internal) => {
                // Internal node: recursively add to the appropriate child
                let child: &mut NumericRangeNode = if value < internal.value {
                    &mut internal.left
                } else {
                    &mut internal.right
                };

                Self::node_add(
                    child,
                    doc_id,
                    value,
                    rv,
                    depth + 1,
                    max_depth_range,
                    compress_floats,
                );

                // If this inner node retains a range, add the value without updating cardinality
                if let Some(range) = internal.range.as_mut() {
                    let size = range.add_without_cardinality(doc_id, value);
                    rv.size_delta += size as i64;
                    rv.num_records += 1;
                }

                // Balance the node if the tree structure changed
                if rv.changed {
                    Self::balance_node(node);

                    // Check if we're too high up to retain this node's range
                    if node.max_depth() > max_depth_range as i32 {
                        Self::remove_range(node, rv);
                    }
                }
            }
            NumericRangeNode::Leaf(leaf) => {
                // Leaf node: add and check if we need to split

                // Update cardinality for leaf nodes
                leaf.range.update_cardinality(value);

                let size = leaf.range.add_without_cardinality(doc_id, value);
                *rv = AddResult {
                    size_delta: size as i64,
                    num_records: 1,
                    changed: false,
                    num_ranges_delta: 0,
                    num_leaves_delta: 0,
                };

                let card = leaf.range.cardinality();
                let num_entries = leaf.range.num_entries();

                // Check if we need to split
                if card >= get_split_cardinality(depth)
                    || (num_entries > MAXIMUM_RANGE_SIZE && card > 1)
                {
                    Self::split_node(node, rv, compress_floats);

                    // Check if we're too high up to retain this node's range
                    if node.max_depth() > max_depth_range as i32 {
                        Self::remove_range(node, rv);
                    }
                }
            }
        }
    }

    /// Split a leaf node into two children at the median value.
    ///
    /// # Algorithm
    ///
    /// 1. Compute the median value from the range's entries.
    /// 2. If the median equals the minimum value, adjust it to the next
    ///    representable f64 (`nextafter(median, INFINITY)` equivalent) to
    ///    ensure at least one entry goes to the left child.
    /// 3. Create two new leaf children with empty ranges.
    /// 4. Redistribute all entries: values < split go left, values >= split go right.
    /// 5. Convert the current node into an internal node with the split value.
    ///
    /// # Split Value Adjustment
    ///
    /// The `f64::from_bits(split.to_bits() + 1)` adjustment is equivalent to
    /// C's `nextafter(split, INFINITY)`. This is necessary when all values in
    /// the range are identical—the median would equal the minimum, and without
    /// adjustment all entries would go to the right child, leaving the left empty.
    ///
    /// # Note
    ///
    /// The original range is retained in the node (now internal). It may be
    /// removed later by `remove_range` if the node's depth exceeds `max_depth_range`.
    fn split_node(node: &mut NumericRangeNode, rv: &mut AddResult, compress_floats: bool) {
        // First compute the split point and collect entries before mutating the node
        let (split, entries) = {
            let range = node.range().expect("node to split must have a range");

            // Compute the median to use as split point
            let split = Self::compute_median(range);

            // Adjust split if it equals the minimum value
            let split = if split == range.min_val() {
                // Make sure the split is not the same as the min value
                // Use next representable f64 greater than split
                f64::from_bits(split.to_bits() + 1)
            } else {
                split
            };

            // Collect all entries from the range
            let mut entries: Vec<(ffi::t_docId, f64)> = Vec::new();
            let reader = range.reader();
            let mut result = inverted_index::RSIndexResult::numeric(0.0);
            match reader {
                crate::NumericRangeReader::Uncompressed(mut r) => {
                    while inverted_index::IndexReader::next_record(&mut r, &mut result)
                        .unwrap_or(false)
                    {
                        // SAFETY: We know the result contains numeric data
                        let entry_value = unsafe { result.as_numeric_unchecked() };
                        entries.push((result.doc_id, entry_value));
                    }
                }
                crate::NumericRangeReader::Compressed(mut r) => {
                    while inverted_index::IndexReader::next_record(&mut r, &mut result)
                        .unwrap_or(false)
                    {
                        // SAFETY: We know the result contains numeric data
                        let entry_value = unsafe { result.as_numeric_unchecked() };
                        entries.push((result.doc_id, entry_value));
                    }
                }
            }

            (split, entries)
        };

        // Create new leaf children
        let mut left = NumericRangeNode::leaf(compress_floats);
        let mut right = NumericRangeNode::leaf(compress_floats);

        // Account for initial inverted index sizes
        rv.size_delta += left.range().map(|r| r.memory_usage() as i64).unwrap_or(0);
        rv.size_delta += right.range().map(|r| r.memory_usage() as i64).unwrap_or(0);

        // Redistribute entries to children
        for (doc_id, entry_value) in entries {
            let target = if entry_value < split {
                &mut left
            } else {
                &mut right
            };

            if let Some(target_range) = target.range_mut() {
                target_range.update_cardinality(entry_value);
                let size = target_range.add_without_cardinality(doc_id, entry_value);
                rv.size_delta += size as i64;
            }
            rv.num_records += 1;
        }

        // Take the existing range from the leaf and convert to an internal node.
        let old_range = node.take_range();
        *node = NumericRangeNode::internal(split, left, right, old_range);

        rv.changed = true;
        rv.num_ranges_delta += 2;
        rv.num_leaves_delta += 1; // Split one leaf into two = +1 leaf
    }

    /// Compute the median value from a range's entries.
    fn compute_median(range: &NumericRange) -> f64 {
        let num_entries = range.num_entries();
        if num_entries == 0 {
            return 0.0;
        }

        let mut values: Vec<f64> = Vec::with_capacity(num_entries);
        let reader = range.reader();
        let mut result = RSIndexResult::numeric(0.0);

        // Collect all values
        match reader {
            NumericRangeReader::Uncompressed(mut r) => {
                while r.next_record(&mut result).unwrap_or(false) {
                    // SAFETY: We know the result contains numeric data
                    values.push(unsafe { result.as_numeric_unchecked() });
                }
            }
            NumericRangeReader::Compressed(mut r) => {
                while r.next_record(&mut result).unwrap_or(false) {
                    // SAFETY: We know the result contains numeric data
                    values.push(unsafe { result.as_numeric_unchecked() });
                }
            }
        }

        let mid = values.len() / 2;
        values.select_nth_unstable_by(mid, f64::total_cmp);
        values[mid]
    }

    /// Remove the range from a node (for GC or depth trimming).
    fn remove_range(node: &mut NumericRangeNode, rv: &mut AddResult) {
        if let Some(range) = node.take_range() {
            rv.size_delta -= range.memory_usage() as i64;
            rv.num_records -= range.num_entries() as i32;
            rv.num_ranges_delta -= 1;
        }
    }

    /// Balance a node if one subtree is significantly deeper than the other.
    ///
    /// Uses AVL-like single rotations when the depth imbalance exceeds
    /// [`MAXIMUM_DEPTH_IMBALANCE`]. Unlike standard AVL trees, we don't perform
    /// double rotations—the simpler approach is sufficient for our use case.
    ///
    /// # Rotation Strategy
    ///
    /// - **Left rotation** (right-heavy): The right child becomes the new root,
    ///   and the old root becomes the left child of the new root.
    /// - **Right rotation** (left-heavy): The left child becomes the new root,
    ///   and the old root becomes the right child of the new root.
    ///
    /// # Note on Range Retention
    ///
    /// To simplify rebalancing, we don't attempt to merge or redistribute ranges
    /// during rotation. Nodes retain their existing ranges (if any) after rotation.
    /// This means a rotated node may have an outdated range that doesn't exactly
    /// match its new subtree, but this is acceptable for query correctness since
    /// ranges only need to be supersets of their subtree's values.
    fn balance_node(node: &mut NumericRangeNode) {
        let (left_depth, right_depth) = if let NumericRangeNode::Internal(internal) = node {
            (internal.left.max_depth(), internal.right.max_depth())
        } else {
            (0, 0)
        };

        if right_depth - left_depth > MAXIMUM_DEPTH_IMBALANCE {
            // Rotate to the left: the right child becomes the new root.
            //
            // We destructure the current internal node, detach the right child,
            // steal the right child's left subtree (right_left) and make it our
            // new right child, then make the demoted node the left child of the
            // promoted right node.
            let NumericRangeNode::Internal(ref mut current) = *node else {
                // balance_node is only called on internal nodes
                return;
            };

            // Take the right child's left subtree
            let NumericRangeNode::Internal(ref mut right_internal) = *current.right else {
                // Right child is a leaf — nothing to rotate
                return;
            };

            // Steal right_left: it becomes the current node's new right child.
            // We temporarily put a placeholder leaf in right_left's place.
            let right_left_subtree = std::mem::replace(
                &mut right_internal.left,
                Box::new(NumericRangeNode::default()),
            );

            // Now detach the entire right child from the current node.
            let NumericRangeNode::Internal(ref mut current) = *node else {
                unreachable!()
            };
            let promoted = std::mem::replace(&mut current.right, right_left_subtree);

            // Update the demoted node's max_depth before swapping
            let NumericRangeNode::Internal(ref mut current) = *node else {
                unreachable!()
            };
            current.max_depth = current.left.max_depth().max(current.right.max_depth()) + 1;

            // Swap: the old node becomes the left child of the promoted node
            let old_node = std::mem::replace(node, *promoted);
            let NumericRangeNode::Internal(ref mut current) = *node else {
                unreachable!()
            };
            current.left = Box::new(old_node);
        } else if left_depth - right_depth > MAXIMUM_DEPTH_IMBALANCE {
            // Rotate to the right: the left child becomes the new root.
            let NumericRangeNode::Internal(ref mut current) = *node else {
                return;
            };

            let NumericRangeNode::Internal(ref mut left_internal) = *current.left else {
                return;
            };

            let left_right_subtree = std::mem::replace(
                &mut left_internal.right,
                Box::new(NumericRangeNode::default()),
            );

            let NumericRangeNode::Internal(ref mut current) = *node else {
                unreachable!()
            };
            let promoted = std::mem::replace(&mut current.left, left_right_subtree);

            // Update the demoted node's max_depth before swapping
            let NumericRangeNode::Internal(ref mut current) = *node else {
                unreachable!()
            };
            current.max_depth = current.left.max_depth().max(current.right.max_depth()) + 1;

            // Swap: the old node becomes the right child of the promoted node
            let old_node = std::mem::replace(node, *promoted);
            let NumericRangeNode::Internal(ref mut current) = *node else {
                unreachable!()
            };
            current.right = Box::new(old_node);
        }

        // Update max_depth after potential rotation
        if let NumericRangeNode::Internal(ref mut internal) = *node {
            internal.max_depth = internal.left.max_depth().max(internal.right.max_depth()) + 1;
        }
    }

    /// Get a reference to the root node.
    pub const fn root(&self) -> &NumericRangeNode {
        &self.root
    }

    /// Get a mutable reference to the root node.
    pub const fn root_mut(&mut self) -> &mut NumericRangeNode {
        &mut self.root
    }

    /// Get the total number of ranges in the tree.
    pub const fn num_ranges(&self) -> usize {
        self.num_ranges
    }

    /// Get the total number of leaf nodes in the tree.
    pub const fn num_leaves(&self) -> usize {
        self.num_leaves
    }

    /// Get the total number of entries in the tree.
    pub const fn num_entries(&self) -> usize {
        self.num_entries
    }

    /// Get the total memory used by inverted indexes in bytes.
    pub const fn inverted_indexes_size(&self) -> usize {
        self.inverted_indexes_size
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
    pub const fn increment_revision_id(&mut self) -> u32 {
        self.revision_id = self.revision_id.wrapping_add(1);
        self.revision_id
    }

    /// Get the unique identifier for this tree.
    pub const fn unique_id(&self) -> u32 {
        self.unique_id
    }

    /// Get the number of empty leaves (for GC tracking).
    pub const fn empty_leaves(&self) -> usize {
        self.empty_leaves
    }

    /// Increment the revision ID. Call this when the tree structure changes.
    pub const fn increment_revision(&mut self) {
        self.revision_id = self.revision_id.wrapping_add(1);
    }

    /// Subtract from the number of entries (used by fork GC).
    pub(crate) const fn subtract_num_entries(&mut self, count: usize) {
        self.num_entries = self.num_entries.saturating_sub(count);
    }

    /// Update the inverted indexes size by a delta (can be negative).
    pub(crate) const fn update_inverted_indexes_size(&mut self, delta: isize) {
        if delta < 0 {
            self.inverted_indexes_size =
                self.inverted_indexes_size.saturating_sub((-delta) as usize);
        } else {
            self.inverted_indexes_size = self.inverted_indexes_size.saturating_add(delta as usize);
        }
    }

    /// Increment the empty leaves counter.
    pub(crate) const fn increment_empty_leaves(&mut self) {
        self.empty_leaves = self.empty_leaves.saturating_add(1);
    }

    /// Apply GC deltas to a specific node in the tree.
    ///
    /// This combines the per-node GC application logic:
    /// 1. Gets the mutable range from the node
    /// 2. Computes how many blocks were added since the fork
    /// 3. Applies the GC delta to the inverted index
    /// 4. Resets cardinality using HLL registers
    /// 5. Updates tree statistics (entries, index size, empty leaves)
    ///
    /// Returns a [`NodeGcResult`] with the GC results.
    /// If the node has no range, returns a result with `valid = false`.
    pub fn apply_node_gc(
        &mut self,
        node: &mut NumericRangeNode,
        delta: GcScanDelta,
        registers_with_last_block: &[u8; 64],
        registers_without_last_block: &[u8; 64],
    ) -> NodeGcResult {
        let Some(range) = node.range_mut() else {
            return NodeGcResult {
                entries_removed: 0,
                bytes_freed: 0,
                bytes_allocated: 0,
                blocks_ignored: 0,
                valid: false,
            };
        };

        // Get the number of blocks before applying GC to calculate blocks added since fork
        let blocks_before = range.entries().num_blocks();
        let last_block_idx = delta.last_block_idx();
        let blocks_since_fork = blocks_before.saturating_sub(last_block_idx + 1);

        // Apply GC delta to the index
        let info: GcApplyInfo = range.entries_mut().apply_gc(delta);

        // Reset cardinality with proper HLL recalculation
        range.reset_cardinality_after_gc(
            info.blocks_ignored,
            blocks_since_fork,
            registers_with_last_block,
            registers_without_last_block,
        );

        // Update tree stats
        self.subtract_num_entries(info.entries_removed);
        let size_delta = info.bytes_allocated as isize - info.bytes_freed as isize;
        self.update_inverted_indexes_size(size_delta);

        // Track empty ranges
        if range.entries().num_docs() == 0 {
            self.increment_empty_leaves();
        }

        NodeGcResult {
            entries_removed: info.entries_removed,
            bytes_freed: info.bytes_freed,
            bytes_allocated: info.bytes_allocated,
            blocks_ignored: info.blocks_ignored as u64,
            valid: true,
        }
    }

    /// Conditionally trim empty leaves from the tree.
    ///
    /// Checks if the number of empty leaves exceeds half the total number of
    /// leaves. If so, trims empty leaves and returns the number of bytes freed.
    /// Returns 0 if no trimming was needed.
    pub fn conditional_trim_empty_leaves(&mut self) -> usize {
        if self.empty_leaves < self.num_leaves / 2 {
            return 0;
        }

        let rv = self.trim_empty_leaves();
        // rv.size_delta is negative (bytes freed during trimming)
        (-rv.size_delta) as usize
    }

    /// Returns an iterator over all nodes in the tree (depth-first traversal).
    pub fn iter(&self) -> crate::DepthFirstNumericRangeTreeIterator<'_> {
        crate::DepthFirstNumericRangeTreeIterator::new(self)
    }

    /// Calculate the total memory usage of the tree.
    pub const fn mem_usage(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();
        let ranges_size = self.num_ranges * std::mem::size_of::<crate::NumericRange>();
        // Our tree is a full binary tree, so #nodes = 2 * #leaves - 1
        let nodes_count = 2 * self.num_leaves.saturating_sub(1) + 1;
        let nodes_size = nodes_count * std::mem::size_of::<NumericRangeNode>();
        // HLL memory: 64 registers per range
        let hll_size = self.num_ranges * 64;

        base_size + self.inverted_indexes_size + ranges_size + nodes_size + hll_size
    }

    /// Find all numeric ranges that match the given filter.
    ///
    /// Returns a vector of references to ranges that overlap with the filter's
    /// min/max bounds. The ranges are returned in order based on the filter's
    /// ascending/descending preference.
    ///
    /// # Optimization Goal
    ///
    /// We try to minimize the number of ranges returned, as each range will
    /// later need to be unioned during query iteration. When a node's range
    /// is completely contained within the filter bounds, we return that single
    /// range instead of descending to its children—this is why internal nodes
    /// retain their ranges up to `max_depth_range`.
    ///
    /// # Offset Handling
    ///
    /// The `filter.offset` and `filter.limit` parameters enable pagination.
    /// We track the running total of documents and skip ranges until we've
    /// passed the offset. See `recursive_find_ranges` for the special handling
    /// of the first overlapping leaf.
    pub fn find<'a>(&'a self, filter: &NumericFilter) -> Vec<&'a NumericRange> {
        let mut ranges = Vec::with_capacity(8);
        let mut total = 0usize;

        Self::recursive_find_ranges(&mut ranges, &self.root, filter, &mut total);

        ranges
    }

    /// Recursively find ranges that match the filter.
    ///
    /// # Containment vs Overlap
    ///
    /// - **Contained**: If the node's range is completely within [min, max],
    ///   add it and stop descending. The range covers all values we need from
    ///   this subtree.
    /// - **Overlaps**: If the range partially overlaps [min, max], we must
    ///   descend into children (for internal nodes) or add the range (for leaves)
    ///   since it contains some—but not all—of the values we need.
    /// - **No overlap**: Skip this subtree entirely.
    ///
    /// # Traversal Order
    ///
    /// Children are visited in ascending or descending order based on `filter.ascending`.
    /// This ensures ranges are returned in the correct order for sorted iteration.
    fn recursive_find_ranges<'a>(
        ranges: &mut Vec<&'a NumericRange>,
        node: &'a NumericRangeNode,
        filter: &NumericFilter,
        total: &mut usize,
    ) {
        // Check if we've reached the limit
        if filter.limit > 0 && *total >= filter.offset + filter.limit {
            return;
        }

        let min = filter.min;
        let max = filter.max;

        if let Some(range) = node.range() {
            let num_docs = range.num_docs();
            let contained = range.contained_in(min, max);
            let overlaps = range.overlaps(min, max);

            // If the range is completely contained in the search bounds, add it
            if contained {
                if filter.offset == 0 {
                    *total += num_docs as usize;
                    ranges.push(range);
                } else {
                    *total += num_docs as usize;
                    if *total > filter.offset {
                        ranges.push(range);
                    }
                }
                return;
            }

            // No overlap at all - nothing to do
            if !overlaps {
                return;
            }
        } else {
            // Node has no range
        }

        match node {
            NumericRangeNode::Internal(internal) => {
                if filter.ascending {
                    // Ascending: left first, then right
                    if min <= internal.value {
                        Self::recursive_find_ranges(ranges, &internal.left, filter, total);
                    }
                    if max >= internal.value {
                        Self::recursive_find_ranges(ranges, &internal.right, filter, total);
                    }
                } else {
                    // Descending: right first, then left
                    if max >= internal.value {
                        Self::recursive_find_ranges(ranges, &internal.right, filter, total);
                    }
                    if min <= internal.value {
                        Self::recursive_find_ranges(ranges, &internal.left, filter, total);
                    }
                }
            }
            NumericRangeNode::Leaf(leaf) => {
                // Leaf node with overlap
                if leaf.range.overlaps(min, max) {
                    // Special case for the first overlapping leaf with no offset:
                    // We count it as 1 to ensure it gets included (since total > offset
                    // means total > 0, and 1 > 0 is true). For all other cases, we use
                    // the actual document count to properly track pagination progress.
                    // This handles the edge case where the first range might have 0 docs
                    // but we still want to include it for completeness.
                    *total += if *total == 0 && filter.offset == 0 {
                        1
                    } else {
                        leaf.range.num_docs() as usize
                    };
                    if *total > filter.offset {
                        ranges.push(&leaf.range);
                    }
                }
            }
        }
    }

    /// Trim empty leaves from the tree (garbage collection).
    ///
    /// Removes leaf nodes that have no documents and prunes the tree structure
    /// accordingly. Returns information about what changed.
    pub fn trim_empty_leaves(&mut self) -> AddResult {
        let mut rv = AddResult::default();
        Self::remove_empty_children(&mut self.root, &mut rv);

        if rv.changed {
            // Update tree statistics
            self.revision_id = self.revision_id.wrapping_add(1);
            self.num_ranges = (self.num_ranges as i64 + rv.num_ranges_delta as i64) as usize;
            self.empty_leaves = (self.empty_leaves as i64 + rv.num_leaves_delta as i64) as usize;
            self.num_leaves = (self.num_leaves as i64 + rv.num_leaves_delta as i64) as usize;
            self.inverted_indexes_size =
                (self.inverted_indexes_size as i64 + rv.size_delta) as usize;
        }

        rv
    }

    /// Recursively remove empty children from a node.
    /// Returns true if this node is empty (CHILD_EMPTY), false otherwise (CHILD_NOT_EMPTY).
    fn remove_empty_children(node: &mut NumericRangeNode, rv: &mut AddResult) -> bool {
        // Stop condition: leaf node
        if let NumericRangeNode::Leaf(leaf) = node {
            return leaf.range.num_docs() == 0;
        }

        // Internal node: recursively check children
        let NumericRangeNode::Internal(internal) = node else {
            unreachable!()
        };

        let right_empty = Self::remove_empty_children(&mut internal.right, rv);
        let left_empty = Self::remove_empty_children(&mut internal.left, rv);

        // If both children are not empty, just balance if needed
        if !right_empty && !left_empty {
            if rv.changed {
                Self::balance_node(node);
            }
            return false;
        }

        // Check if this node has data we need to keep
        if let Some(r) = internal.range.as_ref()
            && r.num_docs() != 0
        {
            // We have data but some children are empty.
            // TODO: In the future, we should trim empty children but
            // for now we keep the node as is to avoid losing data.
            return false;
        }

        // At least one child is empty, and this node has no data worth keeping.
        rv.changed = true;

        // Take the whole node out and destructure it.
        let old = std::mem::take(node);
        let NumericRangeNode::Internal(old_internal) = old else {
            unreachable!()
        };

        // Free this node's range if any
        if let Some(r) = old_internal.range {
            rv.size_delta -= r.memory_usage() as i64;
            rv.num_records -= r.num_entries() as i32;
            rv.num_ranges_delta -= 1;
        }

        if right_empty {
            // Right is empty, keep left as the replacement
            Self::free_subtree(old_internal.right, rv);
            *node = *old_internal.left;
        } else {
            // Left is empty, keep right as the replacement
            Self::free_subtree(old_internal.left, rv);
            *node = *old_internal.right;
        }

        // Return whether the result is empty.
        // If right_empty, we replaced with left, so return left_empty.
        // If !right_empty, left_empty is true and we replaced with right, so return right_empty (false).
        right_empty && left_empty
    }

    /// Free an entire subtree, updating the result with freed resources.
    #[expect(
        clippy::boxed_local,
        reason = "recursive calls pass Box children from Internal variant"
    )]
    fn free_subtree(node: Box<NumericRangeNode>, rv: &mut AddResult) {
        match *node {
            NumericRangeNode::Leaf(leaf) => {
                rv.num_leaves_delta -= 1;
                rv.size_delta -= leaf.range.memory_usage() as i64;
                rv.num_records -= leaf.range.num_entries() as i32;
                rv.num_ranges_delta -= 1;
            }
            NumericRangeNode::Internal(internal) => {
                if let Some(range) = internal.range {
                    rv.size_delta -= range.memory_usage() as i64;
                    rv.num_records -= range.num_entries() as i32;
                    rv.num_ranges_delta -= 1;
                }
                Self::free_subtree(internal.left, rv);
                Self::free_subtree(internal.right, rv);
            }
        }
    }
}

/// Result of applying GC to a single node in the tree.
///
/// Returned by [`NumericRangeTree::apply_node_gc`].
#[derive(Debug, Clone, Copy)]
pub struct NodeGcResult {
    /// Number of entries removed from the index.
    pub entries_removed: usize,
    /// Number of bytes freed.
    pub bytes_freed: usize,
    /// Number of bytes allocated (for new compacted blocks).
    pub bytes_allocated: usize,
    /// Number of blocks that were skipped because the index changed since the scan.
    pub blocks_ignored: u64,
    /// Whether the GC was actually applied. `false` if the node had no range.
    pub valid: bool,
}

impl Default for NumericRangeTree {
    fn default() -> Self {
        Self::new(false)
    }
}
