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
        if filter.limit > 0 && *total >= filter.offset + filter.limit {
            return;
        }

        let node = &nodes[node_idx];
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
}
