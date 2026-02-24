/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared test helpers for the numeric range tree integration tests.

use inverted_index::{Encoder, IndexBlock, RSIndexResult, numeric::Numeric};
use numeric_range_tree::{NodeGcDelta, NodeIndex, NumericRangeNode, NumericRangeTree};

/// Scan a single node and produce its GC delta, if any.
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

/// Scan all nodes in the tree and collect deltas for nodes that have GC work.
///
/// Returns a `Vec<(NodeIndex, NodeGcDelta)>` — only nodes with actual GC work.
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
    if let Some(delta) = scan_node_delta(tree, node_idx, doc_exist) {
        deltas.push((node_idx, delta));
    }

    if let Some((left, right)) = tree.node(node_idx).child_indices() {
        scan_all_dfs(tree, left, doc_exist, deltas);
        scan_all_dfs(tree, right, doc_exist, deltas);
    }
}

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

/// Apply GC to every node in the tree that has a range with GC work.
///
/// This includes both leaf nodes and internal nodes with retained ranges.
pub fn gc_all_ranges(tree: &mut NumericRangeTree, doc_exist: &dyn Fn(u64) -> bool) {
    let deltas = scan_all_node_deltas(tree, doc_exist);
    for (node_idx, delta) in deltas {
        tree.apply_gc_to_node(node_idx, delta);
    }
}
