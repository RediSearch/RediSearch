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
use crate::window::RangeWindow;
use crate::{NumericRange, NumericRangeNode};

impl NumericRangeTree {
    /// Find all numeric ranges that match the given filter.
    ///
    /// Returns a vector of references to ranges that overlap with the filter's
    /// min/max bounds. The ranges are returned in order based on the filter's
    /// ascending/descending preference, and are pairwise disjoint.
    ///
    /// # Optimization Goal
    ///
    /// We try to minimize the number of ranges returned, as each range will
    /// later need to be unioned during query iteration. When a node's range
    /// is completely contained within the filter bounds, we return that single
    /// range instead of descending to its children—this is why internal nodes
    /// retain their ranges up to `max_depth_range`.
    pub fn find<'a>(&'a self, filter: &NumericFilter) -> Vec<&'a NumericRange> {
        self.find_windowed(filter, RangeWindow::UNBOUNDED)
    }

    /// Like [`find`](Self::find), but returning only the ranges that cover
    /// `window`.
    ///
    /// Read [`RangeWindow`] before reaching for this: the window is measured in
    /// whole ranges, so it holds only for a caller that walks the stream
    /// end-to-end.
    pub fn find_windowed<'a>(
        &'a self,
        filter: &NumericFilter,
        window: RangeWindow,
    ) -> Vec<&'a NumericRange> {
        let mut ranges = Vec::with_capacity(8);
        let mut cursor = WindowCursor::new(window);

        Self::recursive_find_ranges(&mut ranges, &self.nodes, self.root, filter, &mut cursor);

        #[cfg(all(feature = "unittest", not(miri)))]
        Self::check_find_invariants(&ranges, filter, window, cursor.total);

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
        cursor: &mut WindowCursor,
    ) {
        if cursor.is_covered() {
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
                if cursor.admit(num_docs as usize) {
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
                            cursor,
                        );
                    }
                    if max >= internal.value {
                        Self::recursive_find_ranges(
                            ranges,
                            nodes,
                            internal.right_index(),
                            filter,
                            cursor,
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
                            cursor,
                        );
                    }
                    if min <= internal.value {
                        Self::recursive_find_ranges(
                            ranges,
                            nodes,
                            internal.left_index(),
                            filter,
                            cursor,
                        );
                    }
                }
            }
            NumericRangeNode::Leaf(leaf) => {
                if cursor.admit_partial(leaf.range.num_docs() as usize) {
                    ranges.push(&leaf.range);
                }
            }
        }
    }
}

/// Running position of a windowed walk over the tree's value-ordered stream.
///
/// Tracks how many documents the walk has passed so far, in traversal order,
/// and decides from that whether a range falls inside [`RangeWindow`]. An
/// unbounded window keeps every range and counts nothing.
struct WindowCursor {
    window: RangeWindow,
    /// Documents passed so far, the unit the window's bounds are measured in.
    total: usize,
}

impl WindowCursor {
    const fn new(window: RangeWindow) -> Self {
        Self { window, total: 0 }
    }

    /// Whether the walk has reached the end of the window and can stop.
    const fn is_covered(&self) -> bool {
        self.window.limit > 0 && self.total >= self.window.offset.saturating_add(self.window.limit)
    }

    /// Account a range lying entirely within the filter bounds, reporting
    /// whether it sits past `offset` and must be kept.
    const fn admit(&mut self, num_docs: usize) -> bool {
        if !self.window.is_bounded() {
            return true;
        }
        self.total += num_docs;
        self.total > self.window.offset
    }

    /// Account a leaf that overlaps the filter bounds without being contained in
    /// them, reporting whether it must be kept.
    ///
    /// Such a leaf holds an unknown mix of in-bounds and out-of-bounds
    /// documents. Counting all of them would overstate how much of the window is
    /// covered and cut the result short, so the leaf opening the window counts as
    /// a single document: the walk errs toward returning more ranges than needed
    /// and leaves exact filtering to the reader.
    const fn admit_partial(&mut self, num_docs: usize) -> bool {
        if !self.window.is_bounded() {
            return true;
        }
        self.total += if self.total == 0 && self.window.offset == 0 {
            1
        } else {
            num_docs
        };
        self.total > self.window.offset
    }
}
