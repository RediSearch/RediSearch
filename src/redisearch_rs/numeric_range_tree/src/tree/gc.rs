/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Maintenance: garbage collection, trimming, and compaction.
//!
//! This module implements the tree's maintenance operations that run outside
//! the normal insert/query hot paths. It handles applying GC deltas from
//! child processes, trimming empty leaves, and compacting the node slab.

use std::collections::HashMap;

use inverted_index::{GcApplyInfo, GcScanDelta};

use super::{AddResult, NumericRangeTree, apply_signed_delta};
use crate::NumericRangeNode;
use crate::arena::{NodeArena, NodeIndex};
use crate::range::Hll;

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
    pub registers_with_last_block: [u8; Hll::size()],
    /// HLL registers excluding the last scanned block's cardinality.
    pub registers_without_last_block: [u8; Hll::size()],
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
    /// Whether the last block in this node index was ignored.
    pub ignored_last_block: bool,
    /// Whether this node became empty after GC.
    pub became_empty: bool,
}

/// Returned by [`NumericRangeTree::compact_if_sparse`].
#[derive(Debug, Clone, Copy, Default)]
pub struct CompactIfSparseResult {
    /// The change in the tree's inverted index memory usage, in bytes.
    /// Positive values indicate growth, negative values indicate shrinkage.
    /// This tracks only inverted index memory, not node/range struct overhead.
    pub inverted_index_size_delta: i64,
    /// The change in the tree's node memory usage, in bytes.
    /// Positive values indicate growth, negative values indicate shrinkage.
    pub node_size_delta: i64,
}

impl NumericRangeTree {
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
        let node = &mut self.nodes[node_idx];
        let is_leaf = node.is_leaf();

        let Some(range) = node.range_mut() else {
            return SingleNodeGcResult::default();
        };
        let was_empty = range.entries().num_docs() == 0;

        // Compute blocks added since fork.
        let blocks_before = range.entries().num_blocks();
        let last_block_idx = delta.delta.last_block_idx();
        let blocks_since_fork = blocks_before.saturating_sub(last_block_idx + 1);

        // Apply GC delta to the index.
        let info: GcApplyInfo = range.entries_mut().apply_gc(delta.delta);

        // Reset cardinality with proper HLL recalculation.
        range.reset_cardinality_after_gc(
            info.ignored_last_block,
            blocks_since_fork,
            &delta.registers_with_last_block,
            &delta.registers_without_last_block,
        );

        // Track empty ranges (only count leaves, and only on transition to empty).
        let is_now_empty = range.entries().num_docs() == 0;
        let became_empty = !was_empty && is_now_empty;
        if is_leaf && became_empty {
            self.stats.empty_leaves = self.stats.empty_leaves.saturating_add(1);
        }

        // Update tree-level stats.
        // We only update num_entries for leaves to avoid double-counting:
        // when max_depth_range > 0, internal nodes retain ranges with duplicate
        // entries, so decrementing for every node would over-subtract.
        if is_leaf {
            self.stats.num_entries = self.stats.num_entries.saturating_sub(info.entries_removed);
        }
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

        #[cfg(all(feature = "unittest", not(miri)))]
        self.check_tree_invariants();

        SingleNodeGcResult {
            entries_removed: info.entries_removed,
            bytes_freed: info.bytes_freed,
            bytes_allocated: info.bytes_allocated,
            ignored_last_block: info.ignored_last_block,
            became_empty,
        }
    }

    /// Returns `true` if the tree has enough empty leaves to warrant compaction.
    ///
    /// The threshold is: at least half the leaves are empty.
    pub const fn is_sparse(&self) -> bool {
        self.stats.empty_leaves > 0 && self.stats.empty_leaves >= self.stats.num_leaves / 2
    }

    /// Conditionally trim empty leaves and compact the node slab.
    ///
    /// Checks if the number of empty leaves exceeds half the total number of
    /// leaves. If so, trims empty leaves, compacts the slab to reclaim freed
    /// slots, and returns the number of bytes freed. Returns 0 if no trimming
    /// was needed.
    pub fn compact_if_sparse(&mut self) -> CompactIfSparseResult {
        // Early return if no empty leaves, or fewer than half are empty.
        if !self.is_sparse() {
            return CompactIfSparseResult::default();
        }

        let rv = self._trim_empty_leaves();
        let slab_freed = self.compact_slab();
        CompactIfSparseResult {
            inverted_index_size_delta: rv.size_delta,
            node_size_delta: -(slab_freed as i64),
        }
    }

    /// Compact the node slab so that live entries are contiguous,
    /// allowing the allocator to reclaim trailing free slots.
    fn compact_slab(&mut self) -> usize {
        let mem_usage_before = self.nodes.mem_usage();

        let n_holes = self.nodes.capacity() - self.nodes.len();
        let mut remap = HashMap::with_capacity(n_holes as usize);

        self.nodes.compact(|_, from, to| {
            remap.insert(from, to);
            true
        });

        if !remap.is_empty() {
            // At this point, parent->children edges may be invalid.
            // We need to update them to reflect the new indices.
            if let Some(&new_idx) = remap.get(&self.root) {
                self.root = new_idx;
            }
            for (_, node) in self.nodes.iter_mut() {
                if let NumericRangeNode::Internal(internal) = node {
                    if let Some(&new_idx) = remap.get(&internal.left) {
                        internal.left = new_idx;
                    }
                    if let Some(&new_idx) = remap.get(&internal.right) {
                        internal.right = new_idx;
                    }
                }
            }
        }

        mem_usage_before.saturating_sub(self.nodes.mem_usage())
    }

    /// Trim empty leaves from the tree (garbage collection).
    ///
    /// Removes leaf nodes that have no documents and prunes the tree structure
    /// accordingly. Returns information about what changed.
    pub fn trim_empty_leaves(&mut self) -> AddResult {
        #[cfg(all(feature = "unittest", not(miri)))]
        let (stats_before, revision_id_before, total_records_before) =
            (self.stats, self.revision_id, self.total_records());

        let result = self._trim_empty_leaves();

        #[cfg(all(feature = "unittest", not(miri)))]
        {
            self.check_delta_invariants(
                stats_before,
                revision_id_before,
                total_records_before,
                &result,
            );
            self.check_tree_invariants();

            assert_eq!(
                stats_before.num_entries, self.stats.num_entries,
                "num_entries should be unchanged after trimming: before={}, after={}",
                stats_before.num_entries, self.stats.num_entries,
            );
        }
        result
    }

    fn _trim_empty_leaves(&mut self) -> AddResult {
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
        nodes: &mut NodeArena,
        node_idx: NodeIndex,
        rv: &mut AddResult,
    ) -> bool {
        let Some((left_idx, right_idx)) = nodes[node_idx].child_indices() else {
            // Leaf node — empty iff no docs.
            return nodes[node_idx].range().is_some_and(|r| r.num_docs() == 0);
        };

        // Internal node: recursively check children
        let right_empty = Self::remove_empty_children(nodes, right_idx, rv);
        let left_empty = Self::remove_empty_children(nodes, left_idx, rv);

        // If both children are not empty, just balance if needed and update depth.
        if !right_empty && !left_empty {
            if rv.changed {
                let new_depth = Self::balance_node(nodes, node_idx, rv);
                if let NumericRangeNode::Internal(internal) = &mut nodes[node_idx] {
                    internal.max_depth = new_depth;
                }
            }
            return false;
        }

        // Check if this node has data we need to keep
        if nodes[node_idx].range().is_some_and(|r| r.num_docs() != 0) {
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
        if let Some(r) = nodes[node_idx].take_range() {
            rv.size_delta -= r.memory_usage() as i64;
            rv.num_records_delta -= r.num_entries() as i32;
            rv.num_ranges_delta -= 1;
        }

        if right_empty {
            // Right is empty, keep left as the replacement.
            // Free the right subtree.
            Self::free_subtree(nodes, right_idx, rv);
            // Move the left node's data into the current slot and remove the left slot.
            let left_data = nodes.remove(left_idx);
            nodes[node_idx] = left_data;
        } else {
            // Left is empty, keep right as the replacement.
            // Free the left subtree.
            Self::free_subtree(nodes, left_idx, rv);
            // Move the right node's data into the current slot and remove the right slot.
            let right_data = nodes.remove(right_idx);
            nodes[node_idx] = right_data;
        }

        // We promoted the non-empty child (or an arbitrary one if both
        // are empty). This node is empty only if both children were.
        right_empty && left_empty
    }

    /// Free an entire subtree, removing all slots from the slab.
    fn free_subtree(nodes: &mut NodeArena, node_idx: NodeIndex, rv: &mut AddResult) {
        // Read child indices before removing, to avoid borrow issues.
        let children = nodes[node_idx].child_indices();

        match &nodes[node_idx] {
            NumericRangeNode::Leaf(leaf) => {
                rv.num_leaves_delta -= 1;
                rv.size_delta -= leaf.range.memory_usage() as i64;
                rv.num_records_delta -= leaf.range.num_entries() as i32;
                rv.num_ranges_delta -= 1;
            }
            NumericRangeNode::Internal(internal) => {
                if let Some(range) = internal.range.as_ref() {
                    rv.size_delta -= range.memory_usage() as i64;
                    rv.num_records_delta -= range.num_entries() as i32;
                    rv.num_ranges_delta -= 1;
                }
            }
        }

        if let Some((left, right)) = children {
            Self::free_subtree(nodes, left, rv);
            Self::free_subtree(nodes, right, rv);
        }

        nodes.remove(node_idx);
    }
}
