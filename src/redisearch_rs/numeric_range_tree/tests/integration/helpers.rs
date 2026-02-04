/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared test helpers for the numeric range tree integration tests.

use numeric_range_tree::{NodeIndex, NumericRangeNode, NumericRangeTree};

/// Number of distinct values that reliably triggers a split at depth 0.
/// Accounts for HLL estimation error (~13%).
pub const SPLIT_TRIGGER: u64 = NumericRangeTree::MINIMUM_RANGE_CARDINALITY as u64 + 4;

/// Build a tree by inserting `n` entries with distinct sequential values `1..=n`.
pub fn build_tree(n: u64, compress_floats: bool, max_depth_range: usize) -> NumericRangeTree {
    let mut tree = NumericRangeTree::new(compress_floats);
    for i in 1..=n {
        tree.add(i, i as f64, false, max_depth_range);
    }
    tree
}

/// Walk the tree depth-first, calling `visitor(node, depth)` for each node.
pub fn walk_with_depth(tree: &NumericRangeTree, visitor: &mut dyn FnMut(&NumericRangeNode, usize)) {
    fn walk_inner(
        tree: &NumericRangeTree,
        node_idx: NodeIndex,
        depth: usize,
        visitor: &mut dyn FnMut(&NumericRangeNode, usize),
    ) {
        let node = tree.node(node_idx);
        visitor(node, depth);
        if let NumericRangeNode::Internal(internal) = node {
            walk_inner(tree, internal.left_index(), depth + 1, visitor);
            walk_inner(tree, internal.right_index(), depth + 1, visitor);
        }
    }
    walk_inner(tree, tree.root_index(), 0, visitor);
}
