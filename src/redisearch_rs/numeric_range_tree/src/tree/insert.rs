/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Write path: insertion, splitting, and balancing.
//!
//! This module implements the tree's write operations. Adding a value
//! descends to the appropriate leaf, inserts the entry, and may trigger
//! splitting and AVL-like rebalancing on the way back up.

use ffi::t_docId;
use inverted_index::{IndexReader as _, RSIndexResult};

use super::{AddResult, NumericRangeTree, apply_signed_delta};
use crate::arena::{NodeArena, NodeIndex};
use crate::{InternalNode, NumericRange, NumericRangeNode};

impl NumericRangeTree {
    /// Last depth level that does NOT use the maximum split cardinality.
    const LAST_DEPTH_WITHOUT_MAXIMUM_CARDINALITY: usize = {
        let ratio = Self::MAXIMUM_RANGE_CARDINALITY / Self::MINIMUM_RANGE_CARDINALITY;
        ratio.ilog2() as usize / Self::CARDINALITY_GROWTH_FACTOR.ilog2() as usize
    };

    /// Calculate the split cardinality threshold for a given depth.
    ///
    /// The threshold grows exponentially by a factor of [`Self::CARDINALITY_GROWTH_FACTOR`]
    /// per depth level until reaching [`Self::MAXIMUM_RANGE_CARDINALITY`]. This allows
    /// shallow nodes to hold fewer distinct values, pushing data down to leaves
    /// for better query selectivity.
    const fn get_split_cardinality(depth: usize) -> usize {
        if depth > Self::LAST_DEPTH_WITHOUT_MAXIMUM_CARDINALITY {
            Self::MAXIMUM_RANGE_CARDINALITY
        } else {
            Self::MINIMUM_RANGE_CARDINALITY * Self::CARDINALITY_GROWTH_FACTOR.pow(depth as u32)
        }
    }

    /// Add a (docId, value) pair to the tree.
    ///
    /// If `is_multivalued` is true, the same document ID can be provided multiple times
    /// in a row.
    /// Returns information about what changed during the add operation.
    pub fn add(
        &mut self,
        doc_id: t_docId,
        value: f64,
        is_multivalued: bool,
        max_depth_range: usize,
    ) -> AddResult {
        #[cfg(all(feature = "unittest", not(miri)))]
        let (stats_before, revision_id_before, total_records_before) =
            (self.stats, self.revision_id, self.total_records());

        let result = self._add(doc_id, value, is_multivalued, max_depth_range);

        #[cfg(all(feature = "unittest", not(miri)))]
        {
            self.check_delta_invariants(
                stats_before,
                revision_id_before,
                total_records_before,
                &result,
            );
            self.check_tree_invariants();
        }
        result
    }

    fn _add(
        &mut self,
        doc_id: t_docId,
        value: f64,
        is_multivalued: bool,
        max_depth_range: usize,
    ) -> AddResult {
        // The underlying assumption is that insertions are ordered by doc_id.
        // Skip if out of order or duplicate on a single-valued field
        if doc_id < self.last_doc_id || (doc_id == self.last_doc_id && !is_multivalued) {
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
            &mut self.stats.empty_leaves,
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
        nodes: &mut NodeArena,
        node_idx: NodeIndex,
        doc_id: t_docId,
        value: f64,
        rv: &mut AddResult,
        depth: usize,
        max_depth_range: usize,
        compress_floats: bool,
        empty_leaves: &mut usize,
    ) {
        match &nodes[node_idx] {
            NumericRangeNode::Internal(_) => {
                // Read the split value and child index.
                let NumericRangeNode::Internal(internal) = &nodes[node_idx] else {
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
                    empty_leaves,
                );

                // If this inner node retains a range, add the value without updating cardinality
                if let NumericRangeNode::Internal(internal) = &mut nodes[node_idx]
                    && let Some(range) = internal.range.as_mut()
                {
                    let size = range.add_without_cardinality(doc_id, value);
                    rv.size_delta += size as i64;
                    rv.num_records += 1;
                }

                // Balance the node if the tree structure changed, and update depth.
                if rv.changed {
                    let new_depth = Self::balance_node(nodes, node_idx, rv);
                    if let NumericRangeNode::Internal(internal) = &mut nodes[node_idx] {
                        internal.max_depth = new_depth;
                    }

                    // Check if we're too high up to retain this node's range
                    if new_depth > max_depth_range as u32 {
                        Self::remove_range(nodes, node_idx, rv);
                    }
                }
            }
            NumericRangeNode::Leaf(_) => {
                // Leaf node: add and check if we need to split
                let NumericRangeNode::Leaf(leaf) = &mut nodes[node_idx] else {
                    unreachable!()
                };

                // If this leaf was emptied (e.g. by the GC) and is about to be re-populated,
                // update the empty_leaves counter.
                if leaf.range.num_docs() == 0 {
                    *empty_leaves = empty_leaves.saturating_sub(1);
                }

                let size = leaf.range.add(doc_id, value);
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
                if card >= Self::get_split_cardinality(depth)
                    || (num_entries > Self::MAXIMUM_RANGE_SIZE && card > 1)
                {
                    Self::split_node(nodes, node_idx, rv, compress_floats);

                    // Check if we're too high up to retain this node's range
                    if nodes[node_idx].max_depth() > max_depth_range as u32 {
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
        nodes: &mut NodeArena,
        node_idx: NodeIndex,
        rv: &mut AddResult,
        compress_floats: bool,
    ) {
        // First compute the split point and collect entries before mutating the node
        let (split, entries) = {
            let range = nodes[node_idx]
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

        let left_idx = nodes.insert(left_node);
        let right_idx = nodes.insert(right_node);

        // Redistribute entries to children
        for (doc_id, entry_value) in entries {
            let target_idx = if entry_value < split {
                left_idx
            } else {
                right_idx
            };

            if let Some(target_range) = nodes[target_idx].range_mut() {
                let size = target_range.add(doc_id, entry_value);
                rv.size_delta += size as i64;
            }
            rv.num_records += 1;
        }

        // Take the existing range from the leaf and convert to an internal node.
        let old_range = nodes[node_idx].take_range();
        let new_node =
            NumericRangeNode::internal_indexed(split, left_idx, right_idx, old_range, nodes);
        nodes[node_idx] = new_node;

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
    fn remove_range(nodes: &mut NodeArena, node_idx: NodeIndex, rv: &mut AddResult) {
        if let Some(range) = nodes[node_idx].take_range() {
            rv.size_delta -= range.memory_usage() as i64;
            rv.num_records -= range.num_entries() as i32;
            rv.num_ranges_delta -= 1;
        }
    }

    /// Balance a node if one subtree is significantly deeper than the other.
    ///
    /// Uses AVL-like single rotations when the depth imbalance exceeds
    /// [`Self::MAXIMUM_DEPTH_IMBALANCE`]. Unlike standard AVL trees, we don't perform
    /// double rotations—the simpler approach is sufficient for our use case.
    ///
    /// Returns the new `max_depth` for the node at `node_idx`. The caller is
    /// responsible for updating the node's `max_depth` field.
    ///
    /// # Rotation Strategy
    ///
    /// - **Left rotation** (right-heavy): The right child becomes the new root,
    ///   and the old root becomes the left child of the new root.
    ///   See [`InternalNode::rotate_left`](crate::InternalNode::rotate_left).
    /// - **Right rotation** (left-heavy): The left child becomes the new root,
    ///   and the old root becomes the right child of the new root.
    ///   See [`InternalNode::rotate_right`](crate::InternalNode::rotate_right).
    #[must_use]
    pub(super) fn balance_node(
        nodes: &mut NodeArena,
        node_idx: NodeIndex,
        rv: &mut AddResult,
    ) -> u32 {
        let NumericRangeNode::Internal(internal) = &nodes[node_idx] else {
            return nodes[node_idx].max_depth();
        };
        let left_depth = nodes[internal.left_index()].max_depth();
        let right_depth = nodes[internal.right_index()].max_depth();

        let dropped_range = if right_depth > left_depth + Self::MAXIMUM_DEPTH_IMBALANCE {
            InternalNode::rotate_left(nodes, node_idx)
        } else if left_depth > right_depth + Self::MAXIMUM_DEPTH_IMBALANCE {
            InternalNode::rotate_right(nodes, node_idx)
        } else {
            return left_depth.max(right_depth) + 1;
        };

        // If a rotation dropped a range, update stats.
        if let Some(range) = dropped_range {
            rv.size_delta -= range.memory_usage() as i64;
            rv.num_records -= range.num_entries() as i32;
            rv.num_ranges_delta -= 1;
        }

        // Return the correct depth. After rotation, read from node (set by rotate).
        nodes[node_idx].max_depth()
    }
}
