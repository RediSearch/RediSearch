/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared test and benchmark helpers for the numeric range tree.
//!
//! Gated behind the `test_utils` feature so that production builds do not
//! include these utilities.

use inverted_index::{Encoder, IndexBlock, RSIndexResult, numeric::Numeric};

use crate::{NodeGcDelta, NodeIndex, NumericRangeNode, NumericRangeTree};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Number of distinct values that reliably triggers a split at depth 0.
/// Accounts for HLL estimation error (~13%).
pub const SPLIT_TRIGGER: u64 = NumericRangeTree::MINIMUM_RANGE_CARDINALITY as u64 + 4;

/// Number of entries per inverted index block before a new block is created.
pub const ENTRIES_PER_BLOCK: u64 = <Numeric as Encoder>::RECOMMENDED_BLOCK_ENTRIES as u64;

/// Enough distinct values to produce a tree with many leaves (>4) by
/// triggering splits up to depth 2. Used by tests that need deeper
/// tree structure (e.g. rebalancing, compaction).
pub const DEEP_TREE_ENTRIES: u64 = {
    let depth_2_threshold = NumericRangeTree::MINIMUM_RANGE_CARDINALITY as u64
        * NumericRangeTree::CARDINALITY_GROWTH_FACTOR as u64
        * NumericRangeTree::CARDINALITY_GROWTH_FACTOR as u64;
    (depth_2_threshold + 4) * 2
};

// ---------------------------------------------------------------------------
// Tree builders
// ---------------------------------------------------------------------------

/// Build a tree by inserting `n` entries with distinct sequential values `1..=n`.
pub fn build_tree(n: u64, compress_floats: bool, max_depth_range: usize) -> NumericRangeTree {
    let mut tree = NumericRangeTree::new(compress_floats);
    for i in 1..=n {
        tree.add(i, i as f64, false, max_depth_range);
    }
    tree
}

/// Build a single-leaf tree (no splits) with `count` entries.
///
/// Uses sequential doc IDs but only a few distinct values (cycling 1..=4) to
/// keep cardinality below the split threshold.
pub fn build_single_leaf_tree(count: u64) -> NumericRangeTree {
    let mut tree = NumericRangeTree::new(false);
    for i in 1..=count {
        let value = (i % 4 + 1) as f64;
        tree.add(i, value, false, 0);
    }
    assert!(
        tree.root().is_leaf(),
        "build_single_leaf_tree: expected single leaf but got {} leaves \
         (count={count} may be too high for 4 distinct values)",
        tree.num_leaves(),
    );
    tree
}

/// Build a large tree with 50k entries whose values cycle through 1..=5000.
pub fn build_large_tree() -> NumericRangeTree {
    let mut tree = NumericRangeTree::new(false);
    for i in 1..=50_000u64 {
        let value = ((i - 1) % 5000 + 1) as f64;
        tree.add(i, value, false, 0);
    }
    tree
}

/// Build a tree with the maximum number of distinct sequential values that
/// stays as a single leaf, plus the next value that would trigger the first split.
///
/// Returns `(tree, next_doc_id)` where `tree` has `num_leaves() == 1` and
/// `tree.add(next_doc_id, next_doc_id as f64, false, 0)` will trigger a split.
pub fn build_tree_at_split_edge() -> (NumericRangeTree, u64) {
    // Find the smallest n where build_tree(n) first causes a split.
    // SPLIT_TRIGGER is an upper bound, so this terminates quickly.
    let mut n = 1u64;
    loop {
        let tree = build_tree(n, false, 0);
        if tree.num_leaves() > 1 {
            let tree = build_tree(n - 1, false, 0);
            assert_eq!(
                tree.num_leaves(),
                1,
                "build_tree_at_split_edge: tree with {} entries should be single leaf",
                n - 1
            );
            return (tree, n);
        }
        n += 1;
        assert!(n <= SPLIT_TRIGGER + 10, "split threshold unexpectedly high");
    }
}

// ---------------------------------------------------------------------------
// GC scanning helpers
// ---------------------------------------------------------------------------

/// Scan a single node and produce its GC delta, if any.
///
/// Uses zeroed HLL registers. For accurate HLL values, use
/// [`NumericRangeNode::scan_gc`] directly or [`scan_node_delta_with_hll`].
pub fn scan_node_delta(
    tree: &NumericRangeTree,
    node_idx: NodeIndex,
    doc_exist: &dyn Fn(u64) -> bool,
) -> Option<NodeGcDelta> {
    scan_node_delta_with_hll(tree, node_idx, doc_exist, |_| ([0u8; 64], [0u8; 64]))
}

/// Like [`scan_node_delta`] but with custom HLL register values.
pub fn scan_node_delta_with_hll(
    tree: &NumericRangeTree,
    node_idx: NodeIndex,
    doc_exist: &dyn Fn(u64) -> bool,
    hll_fn: impl Fn(&inverted_index::GcScanDelta) -> ([u8; 64], [u8; 64]),
) -> Option<NodeGcDelta> {
    let node = tree.node(node_idx);
    node.range()
        .and_then(|range| -> Option<inverted_index::GcScanDelta> {
            range
                .entries()
                .scan_gc(
                    doc_exist,
                    None::<for<'index> fn(&RSIndexResult<'index>, &IndexBlock)>,
                )
                .expect("scan_gc should not fail")
        })
        .map(|delta| {
            let (hll_with, hll_without) = hll_fn(&delta);
            NodeGcDelta {
                delta,
                registers_with_last_block: hll_with,
                registers_without_last_block: hll_without,
            }
        })
}

/// Scan all nodes in the tree and collect GC deltas for nodes that have work.
///
/// Uses [`NumericRangeNode::scan_gc`] which computes accurate HLL registers.
pub fn scan_all_node_deltas(
    tree: &NumericRangeTree,
    doc_exist: &dyn Fn(u64) -> bool,
) -> Vec<(NodeIndex, NodeGcDelta)> {
    let mut deltas = Vec::new();
    scan_all_dfs(tree, tree.root_index(), doc_exist, &mut deltas);
    deltas
}

fn scan_all_dfs(
    tree: &NumericRangeTree,
    node_idx: NodeIndex,
    doc_exist: &dyn Fn(u64) -> bool,
    deltas: &mut Vec<(NodeIndex, NodeGcDelta)>,
) {
    if let Some(delta) = tree.node(node_idx).scan_gc(doc_exist) {
        deltas.push((node_idx, delta));
    }
    if let Some((left, right)) = tree.node(node_idx).child_indices() {
        scan_all_dfs(tree, left, doc_exist, deltas);
        scan_all_dfs(tree, right, doc_exist, deltas);
    }
}

// ---------------------------------------------------------------------------
// Tree walkers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// GC application helpers
// ---------------------------------------------------------------------------

/// Apply GC to every node in the tree that has a range with GC work.
///
/// This includes both leaf nodes and internal nodes with retained ranges.
pub fn gc_all_ranges(tree: &mut NumericRangeTree, doc_exist: &dyn Fn(u64) -> bool) {
    let deltas = scan_all_node_deltas(tree, doc_exist);
    for (node_idx, delta) in deltas {
        tree.apply_gc_to_node(node_idx, delta);
    }
}
