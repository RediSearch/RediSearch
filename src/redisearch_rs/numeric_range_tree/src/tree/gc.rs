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

use inverted_index::{GcApplyInfo, GcScanDelta, IndexBlock, RSIndexResult};

use super::{NumericRangeTree, TrimEmptyLeavesResult, apply_signed_delta};
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
#[repr(C)]
pub struct SingleNodeGcResult {
    /// Information about the outcome of garbage collection on
    /// the inverted index stored within this node.
    pub index_gc_info: GcApplyInfo,
    /// Whether this node became empty after GC.
    pub became_empty: bool,
}

/// Returned by [`NumericRangeTree::compact_if_sparse`].
#[derive(Debug, Clone, Copy, Default)]
#[repr(C)]
pub struct CompactIfSparseResult {
    /// The change in the tree's inverted index memory usage, in bytes.
    /// Positive values indicate growth, negative values indicate shrinkage.
    /// This tracks only inverted index memory, not node/range struct overhead.
    pub inverted_index_size_delta: i64,
    /// The change in the tree's node memory usage, in bytes.
    /// Positive values indicate growth, negative values indicate shrinkage.
    pub node_size_delta: i64,
}

impl NumericRangeNode {
    /// Scan a single node's inverted index for GC work.
    ///
    /// Scan the inverted index associated with its range for deleted
    /// documents, and computes HLL registers for cardinality re-estimation.
    ///
    /// Returns `Some(NodeGcDelta)` if the node had GC work, `None` otherwise
    /// (either the node has no range, or no documents were deleted).
    pub fn scan_gc(&self, doc_exists: &dyn Fn(ffi::t_docId) -> bool) -> Option<NodeGcDelta> {
        let range = self.range()?;

        // Pointer to the last block of the index. Used inside the repair
        // closure to route each entry's HLL contribution into the correct
        // accumulator.
        let last_block_ptr: *const IndexBlock = range
            .entries()
            .last_block()
            .map(|b| b as *const IndexBlock)
            .unwrap_or(std::ptr::null());

        // HLL tracking for cardinality (re)estimation.
        //
        // `majority_hll` accumulates all blocks except the last one.
        // `last_block_hll` accumulates only the last block.
        let mut majority_hll = Hll::new();
        let mut last_block_hll = Hll::new();

        let mut repair_fn = |res: &RSIndexResult<'_>, block: &IndexBlock| {
            // SAFETY: We know this is a numeric index result
            let value = unsafe { res.as_numeric_unchecked() };
            let target = if std::ptr::eq(block, last_block_ptr) {
                &mut last_block_hll
            } else {
                &mut majority_hll
            };
            crate::range::update_cardinality(target, value);
        };

        let delta = range
            .entries()
            .scan_gc(doc_exists, Some(&mut repair_fn))
            .ok()
            .flatten()?;

        // Merge majority into last_block to get "with last block" registers.
        last_block_hll.merge(&majority_hll);

        Some(NodeGcDelta {
            delta,
            registers_with_last_block: *last_block_hll.registers(),
            registers_without_last_block: *majority_hll.registers(),
        })
    }
}

impl NumericRangeTree {
    /// Apply a GC delta to a single node by index.
    ///
    /// Looks up the node in the arena, applies the delta to its range's
    /// inverted index, resets cardinality via HLL, and updates tree-level
    /// stats (`num_entries`, `inverted_indexes_size`, `empty_leaves`).
    ///
    /// Returns per-node GC statistics, if there is a node with the given index.
    /// Returns `None` if there isn't one.
    pub fn apply_gc_to_node(
        &mut self,
        node_idx: NodeIndex,
        delta: NodeGcDelta,
    ) -> Option<SingleNodeGcResult> {
        let node = self.nodes.get_mut(node_idx)?;
        let is_leaf = node.is_leaf();

        let Some(range) = node.range_mut() else {
            return Some(SingleNodeGcResult::default());
        };
        let was_empty = range.entries().num_docs() == 0;

        // Compute blocks added since fork.
        let n_current_blocks = range.entries().num_blocks();
        let last_block_idx_when_forked = delta.delta.last_block_idx();
        debug_assert!(
            n_current_blocks > last_block_idx_when_forked,
            "GC is the only routine that can remove blocks. The number of blocks should never decrease between two GC runs."
        );
        let n_new_blocks_since_fork = n_current_blocks - last_block_idx_when_forked - 1;

        // Apply GC delta to the index.
        let info: GcApplyInfo = range.entries_mut().apply_gc(delta.delta);

        // Reset cardinality with proper HLL recalculation.
        range.reset_cardinality_after_gc(
            info.ignored_last_block,
            n_new_blocks_since_fork,
            &delta.registers_with_last_block,
            &delta.registers_without_last_block,
        );

        // Track empty ranges (only count leaves, and only on transition to empty).
        let is_now_empty = range.entries().num_docs() == 0;
        let became_empty = !was_empty && is_now_empty;
        if is_leaf && became_empty {
            self.stats.empty_leaves = self.stats.empty_leaves.checked_add(1).expect("Overflow!");
        }

        // Update tree-level stats.
        // We only update num_entries for leaves to avoid double-counting:
        // when max_depth_range > 0, internal nodes retain ranges with duplicate
        // entries, so decrementing for every node would over-subtract.
        if is_leaf {
            self.stats.num_entries = self
                .stats
                .num_entries
                .checked_sub(info.entries_removed)
                .expect("Underflow!");
        }
        if info.bytes_freed > info.bytes_allocated {
            self.stats.inverted_indexes_size = self
                .stats
                .inverted_indexes_size
                .checked_sub(info.bytes_freed - info.bytes_allocated)
                .expect("Underflow!");
        } else {
            self.stats.inverted_indexes_size = self
                .stats
                .inverted_indexes_size
                .checked_add(info.bytes_allocated - info.bytes_freed)
                .expect("Overflow!");
        }

        #[cfg(all(feature = "unittest", not(miri)))]
        self.check_tree_invariants();

        Some(SingleNodeGcResult {
            index_gc_info: info,
            became_empty,
        })
    }

    /// Returns `true` if the tree has enough empty leaves to warrant compaction.
    ///
    /// The threshold is: at least half the leaves are empty.
    pub const fn is_sparse(&self) -> bool {
        self.stats.empty_leaves * 2 >= self.stats.num_leaves
    }

    /// Conditionally trim empty leaves and compact the node slab.
    ///
    /// Checks if the number of empty leaves exceeds half the total number of
    /// leaves. If so, trims empty leaves and compacts the slab to reclaim freed
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

        mem_usage_before
            .checked_sub(self.nodes.mem_usage())
            .expect("Underflow!")
    }

    /// Trim empty leaves from the tree (garbage collection).
    ///
    /// Removes leaf nodes that have no documents and prunes the tree structure
    /// accordingly. Returns information about what changed.
    pub fn trim_empty_leaves(&mut self) -> TrimEmptyLeavesResult {
        #[cfg(all(feature = "unittest", not(miri)))]
        let (stats_before, revision_id_before) = (self.stats, self.revision_id);

        let result = self._trim_empty_leaves();

        #[cfg(all(feature = "unittest", not(miri)))]
        {
            self.check_trim_delta_invariants(stats_before, revision_id_before, &result);
            self.check_tree_invariants();

            assert_eq!(
                stats_before.num_entries, self.stats.num_entries,
                "num_entries should be unchanged after trimming: before={}, after={}",
                stats_before.num_entries, self.stats.num_entries,
            );
        }
        result
    }

    fn _trim_empty_leaves(&mut self) -> TrimEmptyLeavesResult {
        let mut rv = TrimEmptyLeavesResult::default();
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
        rv: &mut TrimEmptyLeavesResult,
    ) -> bool {
        let Some((left_idx, right_idx)) = nodes[node_idx].child_indices() else {
            // Leaf node — empty if there are no docs.
            return nodes[node_idx].range().is_some_and(|r| r.num_docs() == 0);
        };

        // Internal node: recursively check children
        let right_empty = Self::remove_empty_children(nodes, right_idx, rv);
        let left_empty = Self::remove_empty_children(nodes, left_idx, rv);

        // If both children are not empty, just balance if needed and update depth.
        if !right_empty && !left_empty {
            if rv.changed {
                let br = Self::balance_node(nodes, node_idx);
                rv.size_delta += br.size_delta;
                rv.num_ranges_delta += br.num_ranges_delta;
                if let NumericRangeNode::Internal(internal) = &mut nodes[node_idx] {
                    internal.max_depth = br.new_depth;
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
    fn free_subtree(nodes: &mut NodeArena, node_idx: NodeIndex, rv: &mut TrimEmptyLeavesResult) {
        // Read child indices before removing, to avoid borrow issues.
        let children = nodes[node_idx].child_indices();

        match &nodes[node_idx] {
            NumericRangeNode::Leaf(leaf) => {
                rv.num_leaves_delta -= 1;
                rv.size_delta -= leaf.range.memory_usage() as i64;
                rv.num_ranges_delta -= 1;
            }
            NumericRangeNode::Internal(internal) => {
                if let Some(range) = internal.range.as_ref() {
                    rv.size_delta -= range.memory_usage() as i64;
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
