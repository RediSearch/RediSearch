/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Debug invariant checks for the numeric range tree.
//!
//! These checks are gated behind the `unittest` feature flag and run
//! after every mutation (`add`, `trim_empty_leaves`) to catch structural
//! violations early.

use super::{AddResult, NumericRangeTree, TreeStats, apply_signed_delta};
use crate::NumericRangeNode;
use crate::arena::NodeIndex;

impl NumericRangeTree {
    /// Sum `num_entries()` across every node that has a range.
    ///
    /// This walks the slab (not the tree) so it covers all nodes regardless
    /// of tree structure. Used to cross-check `AddResult::num_records`.
    pub(crate) fn total_records(&self) -> usize {
        self.nodes
            .iter()
            .filter_map(|(_, node)| node.range())
            .map(|range| range.num_entries())
            .sum()
    }

    /// Verify that the deltas in `result` are consistent with the stats change.
    ///
    /// Asserts that `before + delta == after` for every field that
    /// [`AddResult`] drives: `num_ranges`, `num_leaves`,
    /// `inverted_indexes_size`, `revision_id`, and `num_records`.
    pub(crate) fn check_delta_invariants(
        &self,
        before: TreeStats,
        revision_id_before: u32,
        total_records_before: usize,
        result: &AddResult,
    ) {
        assert_eq!(
            apply_signed_delta(before.num_ranges, result.num_ranges_delta as i64),
            self.stats.num_ranges,
            "num_ranges mismatch: before={}, delta={}, after={}",
            before.num_ranges,
            result.num_ranges_delta,
            self.stats.num_ranges,
        );
        assert_eq!(
            apply_signed_delta(before.num_leaves, result.num_leaves_delta as i64),
            self.stats.num_leaves,
            "num_leaves mismatch: before={}, delta={}, after={}",
            before.num_leaves,
            result.num_leaves_delta,
            self.stats.num_leaves,
        );
        assert_eq!(
            apply_signed_delta(before.inverted_indexes_size, result.size_delta),
            self.stats.inverted_indexes_size,
            "inverted_indexes_size mismatch: before={}, delta={}, after={}",
            before.inverted_indexes_size,
            result.size_delta,
            self.stats.inverted_indexes_size,
        );

        // revision_id / changed
        let expected_revision_id = if result.changed {
            revision_id_before.wrapping_add(1)
        } else {
            revision_id_before
        };
        assert_eq!(
            expected_revision_id, self.revision_id,
            "revision_id mismatch: before={revision_id_before}, changed={}, after={}",
            result.changed, self.revision_id,
        );

        // num_records cross-check
        let expected_total_records =
            apply_signed_delta(total_records_before, result.num_records_delta as i64);
        let actual_total_records = self.total_records();
        assert_eq!(
            expected_total_records, actual_total_records,
            "total_records mismatch: before={total_records_before}, delta={}, actual={actual_total_records}",
            result.num_records_delta,
        );
    }

    /// Walk the slab and independently compute all [`TreeStats`] fields.
    ///
    /// This is the ground-truth calculation used by [`check_memoized_stats`](Self::check_memoized_stats)
    /// to validate the incrementally maintained `self.stats`.
    fn compute_stats(&self) -> TreeStats {
        let mut stats = TreeStats::default();
        for (_, node) in self.nodes.iter() {
            if node.is_leaf() {
                stats.num_leaves += 1;
            }
            if let Some(range) = node.range() {
                stats.num_ranges += 1;
                stats.inverted_indexes_size += range.memory_usage();
                if node.is_leaf() {
                    stats.num_entries += range.num_entries();
                    if range.num_docs() == 0 {
                        stats.empty_leaves += 1;
                    }
                }
            }
        }
        stats
    }

    /// Assert that every field in `self.stats` matches the ground-truth
    /// value computed by walking the slab.
    ///
    /// Complements [`check_delta_invariants`](Self::check_delta_invariants),
    /// which checks that `before + delta == after`. This check verifies
    /// that `after` actually reflects reality.
    fn check_memoized_stats(&self) {
        let computed = self.compute_stats();
        assert_eq!(
            self.stats.num_ranges, computed.num_ranges,
            "num_ranges: memoized={}, computed={}",
            self.stats.num_ranges, computed.num_ranges,
        );
        assert_eq!(
            self.stats.num_leaves, computed.num_leaves,
            "num_leaves: memoized={}, computed={}",
            self.stats.num_leaves, computed.num_leaves,
        );
        assert_eq!(
            self.stats.num_entries, computed.num_entries,
            "num_entries: memoized={}, computed={}",
            self.stats.num_entries, computed.num_entries,
        );
        assert_eq!(
            self.stats.inverted_indexes_size, computed.inverted_indexes_size,
            "inverted_indexes_size: memoized={}, computed={}",
            self.stats.inverted_indexes_size, computed.inverted_indexes_size,
        );
        assert_eq!(
            self.stats.empty_leaves, computed.empty_leaves,
            "empty_leaves: memoized={}, computed={}",
            self.stats.empty_leaves, computed.empty_leaves,
        );
    }

    /// Verify all structural invariants of the tree.
    ///
    /// Panics with a descriptive message if any invariant is violated.
    /// Called automatically after mutations when the `unittest` feature is enabled.
    pub fn check_tree_invariants(&self) {
        self.check_node_invariants(self.root);
        self.check_memoized_stats();
    }

    /// Recursively check invariants for the subtree rooted at `node_idx`.
    ///
    /// Returns `(effective_min, effective_max)` — the tightest value bounds
    /// that cover all data in this subtree.
    ///
    /// For leaf nodes this is simply `(range.min_val(), range.max_val())`.
    /// For internal nodes it is the union of both children's bounds.
    fn check_node_invariants(&self, node_idx: NodeIndex) -> (f64, f64) {
        let node = self.node(node_idx);

        match node {
            NumericRangeNode::Leaf(leaf) => (leaf.range.min_val(), leaf.range.max_val()),
            NumericRangeNode::Internal(internal) => {
                // Recurse into children.
                let (left_min, left_max) = self.check_node_invariants(internal.left_index());
                let (right_min, right_max) = self.check_node_invariants(internal.right_index());

                // --- Invariant 1: max_depth ---
                let left_depth = self.node(internal.left_index()).max_depth();
                let right_depth = self.node(internal.right_index()).max_depth();
                let expected_depth = left_depth.max(right_depth) + 1;
                assert_eq!(
                    internal.max_depth, expected_depth,
                    "max_depth mismatch at node {node_idx:?}: \
                     stored {}, expected {} (left={left_depth}, right={right_depth})",
                    internal.max_depth, expected_depth,
                );

                // --- Invariant 2: depth imbalance ---
                let imbalance = left_depth.abs_diff(right_depth);
                assert!(
                    imbalance <= Self::MAXIMUM_DEPTH_IMBALANCE,
                    "depth imbalance ({imbalance}) exceeds MAXIMUM_DEPTH_IMBALANCE ({}) \
                     at node {node_idx:?} (left={left_depth}, right={right_depth})",
                    Self::MAXIMUM_DEPTH_IMBALANCE,
                );

                let split = internal.value;

                // A child is considered "empty" when its bounds are the
                // initial inverted values (min = +INF, max = -INF).
                let left_empty = left_min == f64::INFINITY && left_max == f64::NEG_INFINITY;
                let right_empty = right_min == f64::INFINITY && right_max == f64::NEG_INFINITY;

                // --- Invariant 3: split value ordering ---
                if !left_empty {
                    assert!(
                        left_max < split,
                        "left subtree max ({left_max}) must be < split ({split}) \
                         at node {node_idx:?}",
                    );
                }
                if !right_empty {
                    assert!(
                        right_min >= split,
                        "right subtree min ({right_min}) must be >= split ({split}) \
                         at node {node_idx:?}",
                    );
                }

                // Effective bounds for this subtree.
                let effective_min = if left_empty { right_min } else { left_min };
                let effective_max = if right_empty { left_max } else { right_max };

                // --- Invariant 4: range superset ---
                // If this internal node retains a range, it must cover all
                // values in both children (unless both children are empty).
                if let Some(range) = &internal.range
                    && !(left_empty && right_empty)
                {
                    assert!(
                        range.min_val() <= effective_min,
                        "internal range min ({}) must be <= effective subtree min \
                             ({effective_min}) at node {node_idx:?}",
                        range.min_val(),
                    );
                    assert!(
                        range.max_val() >= effective_max,
                        "internal range max ({}) must be >= effective subtree max \
                             ({effective_max}) at node {node_idx:?}",
                        range.max_val(),
                    );
                }

                (effective_min, effective_max)
            }
        }
    }
}
