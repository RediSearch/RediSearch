/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Read path: range queries.
//!
//! This module implements the tree's range query operations. Given a
//! [`NumericFilter`], it traverses the tree to find all matching ranges,
//! using containment checks to prune subtrees and avoid unnecessary descent.

use inverted_index::NumericFilter;

use super::NumericRangeTree;
use crate::arena::{NodeArena, NodeIndex};
use crate::{NumericRange, NumericRangeNode};

impl NumericRangeTree {
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

        #[cfg(all(feature = "unittest", not(miri)))]
        Self::check_find_invariants(&ranges, filter, total);

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
        nodes: &'a NodeArena,
        node_idx: NodeIndex,
        filter: &NumericFilter,
        total: &mut usize,
    ) {
        // Check if we've reached the limit
        if filter.limit > 0 && *total >= filter.offset.saturating_add(filter.limit) {
            return;
        }

        let node = &nodes[node_idx];
        let min = filter.min;
        let max = filter.max;

        if let Some(range) = node.range() {
            let num_docs = range.num_docs();
            if num_docs == 0 {
                // We don't care about empty ranges.
                return;
            }
            let contained = range.contained_in(min, max);
            let overlaps = range.overlaps(min, max);

            // If the range is completely contained in the search bounds, add it
            if contained {
                *total += num_docs as usize;
                if *total > filter.offset {
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
                let num_docs = leaf.range.num_docs();
                *total += if *total == 0 && filter.offset == 0 {
                    // This is the first range of the result set.
                    // It the range _overlaps_ with the filter range,
                    // but isn't contained within it, it might contain documents
                    // that sit outside the filter range.
                    // For example, if the filter range is [10, 20] and the leaf range is [5, 11],
                    // the leaf range contains documents that are outside the filter range.
                    //
                    // If we sum its `num_docs` to the running total, we may
                    // end up overcounting the number of documents that match
                    // the filter range, thus returning less documents than expected.
                    // We choose the opposite strategy: we potentially return _more_ documents
                    // than necessary, leaving it to the result set iterator to precisely filter
                    // out the ones that sit outside the filter range.
                    1
                } else {
                    num_docs as usize
                };
                if *total > filter.offset {
                    ranges.push(&leaf.range);
                }
            }
        }
    }
}
