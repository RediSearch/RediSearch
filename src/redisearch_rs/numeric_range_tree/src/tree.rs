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

use std::collections::HashMap;

use ffi::t_docId;
use inverted_index::{GcApplyInfo, GcScanDelta, IndexReader as _, NumericFilter, RSIndexResult};
use slab::Slab;

use crate::node::NodeIndex;
use crate::unique_id::TreeUniqueId;
use crate::{NumericRange, NumericRangeNode};

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
const MAXIMUM_DEPTH_IMBALANCE: u32 = 2;

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

/// Apply a signed delta to an unsigned value, saturating at bounds instead of wrapping.
const fn apply_signed_delta(value: usize, delta: i64) -> usize {
    if delta < 0 {
        value.saturating_sub((-delta) as usize)
    } else {
        value.saturating_add(delta as usize)
    }
}

/// Aggregate statistics for a [`NumericRangeTree`].
///
/// Tracks counts of ranges, leaves, entries, memory usage, and
/// empty leaves. Updated incrementally during insertions, splits,
/// GC, and trimming.
#[derive(Debug, Default)]
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
/// All nodes are stored in a [`Slab`] arena. Children are referenced by
/// [`NodeIndex`] instead of `Box<NumericRangeNode>`. This provides better
/// cache locality, eliminates per-node heap allocation overhead, and makes
/// rotations cheaper (index swaps instead of alloc/dealloc).
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
#[expect(
    rustdoc::private_intra_doc_links,
    reason = "internal docs reference private items for developer context"
)]
pub struct NumericRangeTree {
    /// The root node index.
    root: NodeIndex,
    /// Arena holding all tree nodes.
    nodes: Slab<NumericRangeNode>,
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
    /// Create a new empty numeric range tree.
    ///
    /// If `compress_floats` is true, the tree will use float compression.
    /// Check out [`NumericFloatCompression`][`inverted_index::numeric::NumericFloatCompression`] for more information.
    pub fn new(compress_floats: bool) -> Self {
        let mut nodes = Slab::new();
        let root_node = NumericRangeNode::leaf(compress_floats);
        let inverted_indexes_size = root_node.range().map(|r| r.memory_usage()).unwrap_or(0);
        let root = NodeIndex::from_slab(nodes.insert(root_node));

        Self {
            root,
            nodes,
            stats: TreeStats {
                num_ranges: 1,
                num_leaves: 1,
                num_entries: 0,
                inverted_indexes_size,
                empty_leaves: 0,
            },
            last_doc_id: 0,
            revision_id: 0,
            unique_id: TreeUniqueId::next(),
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
        // The underlying assumption is that insertions are ordered by doc_id.
        if doc_id <= self.last_doc_id && !allow_duplicates {
            // Skip duplicate doc_id
            return AddResult::default();
        }
        self.last_doc_id = doc_id;

        let mut rv = AddResult::default();
        Self::node_add(
            &mut self.nodes,
            self.root,
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

        self.stats.num_ranges =
            apply_signed_delta(self.stats.num_ranges, rv.num_ranges_delta as i64);
        self.stats.num_leaves =
            apply_signed_delta(self.stats.num_leaves, rv.num_leaves_delta as i64);
        self.stats.num_entries += 1;
        self.stats.inverted_indexes_size =
            apply_signed_delta(self.stats.inverted_indexes_size, rv.size_delta);

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
    #[expect(clippy::too_many_arguments)]
    fn node_add(
        nodes: &mut Slab<NumericRangeNode>,
        node_idx: NodeIndex,
        doc_id: t_docId,
        value: f64,
        rv: &mut AddResult,
        depth: usize,
        max_depth_range: usize,
        compress_floats: bool,
    ) {
        match &nodes[node_idx.key()] {
            NumericRangeNode::Internal(_) => {
                // Read the split value and child index.
                let NumericRangeNode::Internal(internal) = &nodes[node_idx.key()] else {
                    unreachable!()
                };
                let child_idx = if value < internal.value {
                    internal.left_index()
                } else {
                    internal.right_index()
                };

                Self::node_add(
                    nodes,
                    child_idx,
                    doc_id,
                    value,
                    rv,
                    depth + 1,
                    max_depth_range,
                    compress_floats,
                );

                // If this inner node retains a range, add the value without updating cardinality
                if let NumericRangeNode::Internal(internal) = &mut nodes[node_idx.key()]
                    && let Some(range) = internal.range.as_mut()
                {
                    let size = range.add_without_cardinality(doc_id, value);
                    rv.size_delta += size as i64;
                    rv.num_records += 1;
                }

                // Balance the node if the tree structure changed
                if rv.changed {
                    Self::balance_node(nodes, node_idx);

                    // Check if we're too high up to retain this node's range
                    if nodes[node_idx.key()].max_depth() > max_depth_range as u32 {
                        Self::remove_range(nodes, node_idx, rv);
                    }
                }
            }
            NumericRangeNode::Leaf(_) => {
                // Leaf node: add and check if we need to split
                let NumericRangeNode::Leaf(leaf) = &mut nodes[node_idx.key()] else {
                    unreachable!()
                };

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
                    Self::split_node(nodes, node_idx, rv, compress_floats);

                    // Check if we're too high up to retain this node's range
                    if nodes[node_idx.key()].max_depth() > max_depth_range as u32 {
                        Self::remove_range(nodes, node_idx, rv);
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
    /// 3. Create two new leaf children and insert them into the arena.
    /// 4. Redistribute all entries: values < split go left, values >= split go right.
    /// 5. Convert the current node into an internal node with the split value.
    ///
    /// # Split Value Adjustment
    ///
    /// The `f64::next_up()` adjustment produces the next representable f64
    /// greater than `split`. This is necessary when all values in the range
    /// are identical—the median would equal the minimum, and without adjustment
    /// all entries would go to the right child, leaving the left empty.
    ///
    /// # Note
    ///
    /// The original range is retained in the node (now internal). It may be
    /// removed later by `remove_range` if the node's depth exceeds `max_depth_range`.
    fn split_node(
        nodes: &mut Slab<NumericRangeNode>,
        node_idx: NodeIndex,
        rv: &mut AddResult,
        compress_floats: bool,
    ) {
        // First compute the split point and collect entries before mutating the node
        let (split, entries) = {
            let range = nodes[node_idx.key()]
                .range()
                .expect("node to split must have a range");

            // Compute the median to use as split point
            let split = Self::compute_median(range);

            // Adjust split if it equals the minimum value
            let split = if split == range.min_val() {
                // Make sure the split is not the same as the min value
                // Use next representable f64 greater than split
                split.next_up()
            } else {
                split
            };

            // Collect all entries from the range
            let mut entries: Vec<(ffi::t_docId, f64)> = Vec::new();
            let mut reader = range.reader();
            let mut result = inverted_index::RSIndexResult::numeric(0.0);
            while reader.next_record(&mut result).unwrap_or(false) {
                // SAFETY: We know the result contains numeric data
                let entry_value = unsafe { result.as_numeric_unchecked() };
                entries.push((result.doc_id, entry_value));
            }

            (split, entries)
        };

        // Create new leaf children and insert into the arena
        let left_node = NumericRangeNode::leaf(compress_floats);
        let right_node = NumericRangeNode::leaf(compress_floats);

        // Account for initial inverted index sizes
        rv.size_delta += left_node
            .range()
            .map(|r| r.memory_usage() as i64)
            .unwrap_or(0);
        rv.size_delta += right_node
            .range()
            .map(|r| r.memory_usage() as i64)
            .unwrap_or(0);

        let left_idx = NodeIndex::from_slab(nodes.insert(left_node));
        let right_idx = NodeIndex::from_slab(nodes.insert(right_node));

        // Redistribute entries to children
        for (doc_id, entry_value) in entries {
            let target_idx = if entry_value < split {
                left_idx
            } else {
                right_idx
            };

            if let Some(target_range) = nodes[target_idx.key()].range_mut() {
                target_range.update_cardinality(entry_value);
                let size = target_range.add_without_cardinality(doc_id, entry_value);
                rv.size_delta += size as i64;
            }
            rv.num_records += 1;
        }

        // Take the existing range from the leaf and convert to an internal node.
        let old_range = nodes[node_idx.key()].take_range();
        let new_node =
            NumericRangeNode::internal_indexed(split, left_idx, right_idx, old_range, nodes);
        nodes[node_idx.key()] = new_node;

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
        let mut reader = range.reader();
        let mut result = RSIndexResult::numeric(0.0);

        // Collect all values
        while reader.next_record(&mut result).unwrap_or(false) {
            // SAFETY: We know the result contains numeric data
            values.push(unsafe { result.as_numeric_unchecked() });
        }

        let mid = values.len() / 2;
        values.select_nth_unstable_by(mid, f64::total_cmp);
        values[mid]
    }

    /// Remove the range from a node (for GC or depth trimming).
    fn remove_range(nodes: &mut Slab<NumericRangeNode>, node_idx: NodeIndex, rv: &mut AddResult) {
        if let Some(range) = nodes[node_idx.key()].take_range() {
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
    ///   See [`InternalNode::rotate_left`].
    /// - **Right rotation** (left-heavy): The left child becomes the new root,
    ///   and the old root becomes the right child of the new root.
    ///   See [`InternalNode::rotate_right`].
    ///
    /// # Note on Range Retention
    ///
    /// To simplify rebalancing, we don't attempt to merge or redistribute ranges
    /// during rotation. Nodes retain their existing ranges (if any) after rotation.
    /// This means a rotated node may have an outdated range that doesn't exactly
    /// match its new subtree, but this is acceptable for query correctness since
    /// ranges only need to be supersets of their subtree's values.
    fn balance_node(nodes: &mut Slab<NumericRangeNode>, node_idx: NodeIndex) {
        let NumericRangeNode::Internal(internal) = &nodes[node_idx.key()] else {
            return;
        };
        let left_depth = nodes[internal.left_index().key()].max_depth();
        let right_depth = nodes[internal.right_index().key()].max_depth();

        if right_depth > left_depth + MAXIMUM_DEPTH_IMBALANCE {
            crate::InternalNode::rotate_left(nodes, node_idx);
        } else if left_depth > right_depth + MAXIMUM_DEPTH_IMBALANCE {
            crate::InternalNode::rotate_right(nodes, node_idx);
        } else {
            // No rotation needed — just update max_depth.
            let new_depth = left_depth.max(right_depth) + 1;
            if let NumericRangeNode::Internal(ref mut internal) = nodes[node_idx.key()] {
                internal.max_depth = new_depth;
            }
        }
    }

    /// Resolve a [`NodeIndex`] to a shared reference to the node.
    pub fn node(&self, idx: NodeIndex) -> &NumericRangeNode {
        &self.nodes[idx.key()]
    }

    /// Get a reference to the root node.
    pub fn root(&self) -> &NumericRangeNode {
        &self.nodes[self.root.key()]
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

    /// Apply a GC delta to a single node by index.
    ///
    /// Looks up the node in the arena, applies the delta to its range's
    /// inverted index, resets cardinality via HLL, and updates tree-level
    /// stats (`num_entries`, `inverted_indexes_size`, `empty_leaves`).
    ///
    /// Returns per-node GC statistics.
    ///
    /// # Panics
    ///
    /// Panics if `node_idx` does not refer to a valid node in the tree's arena.
    pub fn apply_gc_to_node(
        &mut self,
        node_idx: NodeIndex,
        delta: NodeGcDelta,
    ) -> SingleNodeGcResult {
        let Some(range) = self.nodes[node_idx.key()].range_mut() else {
            return SingleNodeGcResult::default();
        };

        // Compute blocks added since fork.
        let blocks_before = range.entries().num_blocks();
        let last_block_idx = delta.delta.last_block_idx();
        let blocks_since_fork = blocks_before.saturating_sub(last_block_idx + 1);

        // Apply GC delta to the index.
        let info: GcApplyInfo = range.entries_mut().apply_gc(delta.delta);

        // Reset cardinality with proper HLL recalculation.
        range.reset_cardinality_after_gc(
            info.blocks_ignored,
            blocks_since_fork,
            &delta.registers_with_last_block,
            &delta.registers_without_last_block,
        );

        // Track empty ranges.
        let became_empty = range.entries().num_docs() == 0;
        if became_empty {
            self.stats.empty_leaves = self.stats.empty_leaves.saturating_add(1);
        }

        // Update tree-level stats.
        self.stats.num_entries = self.stats.num_entries.saturating_sub(info.entries_removed);
        if info.bytes_freed > info.bytes_allocated {
            self.stats.inverted_indexes_size = self
                .stats
                .inverted_indexes_size
                .saturating_sub(info.bytes_freed - info.bytes_allocated);
        } else {
            self.stats.inverted_indexes_size = self
                .stats
                .inverted_indexes_size
                .saturating_add(info.bytes_allocated - info.bytes_freed);
        }

        SingleNodeGcResult {
            entries_removed: info.entries_removed,
            bytes_freed: info.bytes_freed,
            bytes_allocated: info.bytes_allocated,
            blocks_ignored: info.blocks_ignored as u64,
            became_empty,
        }
    }

    /// Conditionally trim empty leaves and compact the node slab.
    ///
    /// Checks if the number of empty leaves exceeds half the total number of
    /// leaves. If so, trims empty leaves, compacts the slab to reclaim freed
    /// slots, and returns the number of bytes freed. Returns 0 if no trimming
    /// was needed.
    pub fn compact_if_sparse(&mut self) -> usize {
        if self.stats.empty_leaves < self.stats.num_leaves / 2 {
            return 0;
        }

        let rv = self.trim_empty_leaves();
        let slab_freed = self.compact_slab();
        // rv.size_delta is negative (bytes freed during trimming)
        (-rv.size_delta) as usize + slab_freed
    }

    /// Compact the node slab so that live entries are contiguous,
    /// allowing the allocator to reclaim trailing free slots.
    fn compact_slab(&mut self) -> usize {
        let cap_before = self.nodes.capacity();

        let mut moves = Vec::with_capacity(self.nodes.capacity() - self.nodes.len());

        self.nodes.compact(|_, from, to| {
            moves.push((from, to));
            true
        });

        if moves.is_empty() {
            return 0;
        }

        let remap: HashMap<usize, usize> = moves.into_iter().collect();

        // Fix the root index.
        if let Some(&new_key) = remap.get(&self.root.key()) {
            self.root = NodeIndex::from_slab(new_key);
        }

        // Fix all child pointers.
        for (_, node) in self.nodes.iter_mut() {
            if let NumericRangeNode::Internal(internal) = node {
                if let Some(&new_key) = remap.get(&internal.left.key()) {
                    internal.left = NodeIndex::from_slab(new_key);
                }
                if let Some(&new_key) = remap.get(&internal.right.key()) {
                    internal.right = NodeIndex::from_slab(new_key);
                }
            }
        }

        let cap_after = self.nodes.capacity();
        let slots_freed = cap_before.saturating_sub(cap_after);
        slots_freed * std::mem::size_of::<NumericRangeNode>()
    }

    /// Returns an iterator over all nodes in the tree (depth-first traversal).
    pub fn iter(&self) -> crate::DepthFirstNumericRangeTreeIterator<'_> {
        crate::DepthFirstNumericRangeTreeIterator::new(self)
    }

    /// Calculate the total memory usage of the tree.
    pub const fn mem_usage(&self) -> usize {
        let base_size = std::mem::size_of::<Self>();
        let ranges_size = self.stats.num_ranges * std::mem::size_of::<crate::NumericRange>();
        // Our tree is a full binary tree, so #nodes = 2 * #leaves - 1
        let nodes_count = 2 * self.stats.num_leaves.saturating_sub(1) + 1;
        let nodes_size = nodes_count * std::mem::size_of::<NumericRangeNode>();
        // HLL memory: 64 registers per range
        let hll_size = self.stats.num_ranges * 64;

        base_size + self.stats.inverted_indexes_size + ranges_size + nodes_size + hll_size
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

        Self::recursive_find_ranges(&mut ranges, &self.nodes, self.root, filter, &mut total);

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
        nodes: &'a Slab<NumericRangeNode>,
        node_idx: NodeIndex,
        filter: &NumericFilter,
        total: &mut usize,
    ) {
        // Check if we've reached the limit
        if filter.limit > 0 && *total >= filter.offset + filter.limit {
            return;
        }

        let node = &nodes[node_idx.key()];
        let min = filter.min;
        let max = filter.max;

        if let Some(range) = node.range() {
            let num_docs = range.num_docs();
            let contained = range.contained_in(min, max);
            let overlaps = range.overlaps(min, max);

            // If the range is completely contained in the search bounds, add it
            if contained {
                *total += num_docs as usize;
                if filter.offset == 0 || *total > filter.offset {
                    ranges.push(range);
                }
                return;
            }

            // No overlap at all - nothing to do
            if !overlaps {
                return;
            }
        }

        match node {
            NumericRangeNode::Internal(internal) => {
                if filter.ascending {
                    // Ascending: left first, then right
                    if min <= internal.value {
                        Self::recursive_find_ranges(
                            ranges,
                            nodes,
                            internal.left_index(),
                            filter,
                            total,
                        );
                    }
                    if max >= internal.value {
                        Self::recursive_find_ranges(
                            ranges,
                            nodes,
                            internal.right_index(),
                            filter,
                            total,
                        );
                    }
                } else {
                    // Descending: right first, then left
                    if max >= internal.value {
                        Self::recursive_find_ranges(
                            ranges,
                            nodes,
                            internal.right_index(),
                            filter,
                            total,
                        );
                    }
                    if min <= internal.value {
                        Self::recursive_find_ranges(
                            ranges,
                            nodes,
                            internal.left_index(),
                            filter,
                            total,
                        );
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
        Self::remove_empty_children(&mut self.nodes, self.root, &mut rv);

        if rv.changed {
            // Update tree statistics
            self.revision_id = self.revision_id.wrapping_add(1);
            self.stats.num_ranges =
                apply_signed_delta(self.stats.num_ranges, rv.num_ranges_delta as i64);
            self.stats.empty_leaves =
                apply_signed_delta(self.stats.empty_leaves, rv.num_leaves_delta as i64);
            self.stats.num_leaves =
                apply_signed_delta(self.stats.num_leaves, rv.num_leaves_delta as i64);
            self.stats.inverted_indexes_size =
                apply_signed_delta(self.stats.inverted_indexes_size, rv.size_delta);
        }

        rv
    }

    /// Recursively remove empty children from a node.
    /// Returns true if this node is empty, false otherwise.
    fn remove_empty_children(
        nodes: &mut Slab<NumericRangeNode>,
        node_idx: NodeIndex,
        rv: &mut AddResult,
    ) -> bool {
        let Some((left_idx, right_idx)) = nodes[node_idx.key()].child_indices() else {
            // Leaf node — empty iff no docs.
            return nodes[node_idx.key()]
                .range()
                .is_some_and(|r| r.num_docs() == 0);
        };

        // Internal node: recursively check children
        let right_empty = Self::remove_empty_children(nodes, right_idx, rv);
        let left_empty = Self::remove_empty_children(nodes, left_idx, rv);

        // If both children are not empty, just balance if needed
        if !right_empty && !left_empty {
            if rv.changed {
                Self::balance_node(nodes, node_idx);
            }
            return false;
        }

        // Check if this node has data we need to keep
        if nodes[node_idx.key()]
            .range()
            .is_some_and(|r| r.num_docs() != 0)
        {
            // This node has surviving entries but some children are empty.
            // In practice this is unreachable: GC scans and applies deltas
            // to every node (leaves and retained internal ranges) before
            // trim runs, so if both children are empty the parent's range
            // will also be empty. Keep the subtree as-is for safety.
            return false;
        }

        // At least one child is empty, and this node has no data worth keeping.
        rv.changed = true;

        // Free this node's range if any
        if let Some(r) = nodes[node_idx.key()].take_range() {
            rv.size_delta -= r.memory_usage() as i64;
            rv.num_records -= r.num_entries() as i32;
            rv.num_ranges_delta -= 1;
        }

        if right_empty {
            // Right is empty, keep left as the replacement.
            // Free the right subtree.
            Self::free_subtree(nodes, right_idx, rv);
            // Move the left node's data into the current slot and remove the left slot.
            let left_data = nodes.remove(left_idx.key());
            nodes[node_idx.key()] = left_data;
        } else {
            // Left is empty, keep right as the replacement.
            // Free the left subtree.
            Self::free_subtree(nodes, left_idx, rv);
            // Move the right node's data into the current slot and remove the right slot.
            let right_data = nodes.remove(right_idx.key());
            nodes[node_idx.key()] = right_data;
        }

        // We promoted the non-empty child (or an arbitrary one if both
        // are empty). This node is empty only if both children were.
        right_empty && left_empty
    }

    /// Free an entire subtree, removing all slots from the slab.
    fn free_subtree(nodes: &mut Slab<NumericRangeNode>, node_idx: NodeIndex, rv: &mut AddResult) {
        // Read child indices before removing, to avoid borrow issues.
        let children = nodes[node_idx.key()].child_indices();

        match &nodes[node_idx.key()] {
            NumericRangeNode::Leaf(leaf) => {
                rv.num_leaves_delta -= 1;
                rv.size_delta -= leaf.range.memory_usage() as i64;
                rv.num_records -= leaf.range.num_entries() as i32;
                rv.num_ranges_delta -= 1;
            }
            NumericRangeNode::Internal(internal) => {
                if let Some(range) = internal.range.as_ref() {
                    rv.size_delta -= range.memory_usage() as i64;
                    rv.num_records -= range.num_entries() as i32;
                    rv.num_ranges_delta -= 1;
                }
            }
        }

        if let Some((left, right)) = children {
            Self::free_subtree(nodes, left, rv);
            Self::free_subtree(nodes, right, rv);
        }

        nodes.remove(node_idx.key());
    }
}

/// GC delta data for a single node, as computed by the child process.
///
/// Contains the inverted index GC delta plus the HyperLogLog registers
/// captured during the scan. One `NodeGcDelta` is produced per DFS node
/// that had GC work.
#[derive(Debug)]
pub struct NodeGcDelta {
    /// The inverted index GC scan delta.
    pub delta: GcScanDelta,
    /// HLL registers including the last scanned block's cardinality.
    pub registers_with_last_block: [u8; 64],
    /// HLL registers excluding the last scanned block's cardinality.
    pub registers_without_last_block: [u8; 64],
}

/// Result of applying GC to a single node.
///
/// Returned by [`NumericRangeTree::apply_gc_to_node`].
#[derive(Debug, Clone, Copy, Default)]
pub struct SingleNodeGcResult {
    /// Number of entries removed from this node.
    pub entries_removed: usize,
    /// Bytes freed from this node.
    pub bytes_freed: usize,
    /// Bytes allocated (for new compacted blocks) in this node.
    pub bytes_allocated: usize,
    /// Blocks ignored in this node.
    pub blocks_ignored: u64,
    /// Whether this node became empty after GC.
    pub became_empty: bool,
}

impl Default for NumericRangeTree {
    fn default() -> Self {
        Self::new(false)
    }
}
