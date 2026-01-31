/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for garbage collection in the numeric range tree.

use numeric_range_tree::{NodeGcDelta, NumericRangeNode, NumericRangeTree};
use rstest::rstest;

/// Build a single-leaf tree (no splits) with `count` entries.
///
/// Uses sequential doc IDs but only a few distinct values to keep cardinality
/// below the split threshold (16 at depth 0).
fn build_single_leaf_tree(count: u64, compress_floats: bool) -> NumericRangeTree {
    let mut tree = NumericRangeTree::new(compress_floats);
    for i in 1..=count {
        // Cycle through 4 distinct values to keep cardinality low.
        let value = (i % 4 + 1) as f64;
        tree.add(i, value, false, 0);
    }
    assert!(
        tree.root().is_leaf(),
        "tree should remain a single leaf (got {} leaves)",
        tree.num_leaves()
    );
    tree
}

// ============================================================================
// apply_gc_batch tests
// ============================================================================

#[test]
fn apply_gc_batch_single_leaf() {
    let mut tree = build_single_leaf_tree(10, false);
    let entries_before = tree.num_entries();

    // Scan and build batch deltas.
    let deltas = scan_batch_deltas(&tree, &|doc_id| doc_id > 5);

    let result = tree.apply_gc_batch(&mut deltas.into_iter());

    assert_eq!(result.nodes_applied, 1);
    assert_eq!(result.total_entries_removed, 5);
    assert_eq!(tree.num_entries(), entries_before - 5);
}

#[test]
fn apply_gc_batch_multi_node_mixed() {
    // Build a tree with multiple leaves.
    let mut tree = NumericRangeTree::new(false);
    for i in 1..=500u64 {
        tree.add(i, i as f64, false, 0);
    }
    assert!(tree.num_leaves() > 1);

    let entries_before = tree.num_entries();

    // Delete docs 1..=250 — some leaves will have work, some won't.
    let deltas = scan_batch_deltas(&tree, &|doc_id| doc_id > 250);

    let result = tree.apply_gc_batch(&mut deltas.into_iter());

    assert!(result.nodes_applied > 0);
    assert!(result.total_entries_removed > 0);
    assert!(tree.num_entries() < entries_before);
}

#[test]
fn apply_gc_batch_all_skip() {
    // No documents deleted — every node should be skipped.
    let mut tree = build_single_leaf_tree(10, false);
    let entries_before = tree.num_entries();

    let deltas = scan_batch_deltas(&tree, &|_| true);

    let result = tree.apply_gc_batch(&mut deltas.into_iter());

    assert_eq!(result.nodes_applied, 0);
    assert_eq!(result.total_entries_removed, 0);
    assert_eq!(tree.num_entries(), entries_before);
}

#[rstest]
#[case(false)]
#[case(true)]
fn apply_gc_batch_with_blocks_added_since_fork(#[case] compress_floats: bool) {
    let mut tree = NumericRangeTree::new(compress_floats);
    for i in 1..=2000 {
        tree.add(i, 42.0, false, 0);
    }
    assert!(tree.root().is_leaf());

    // Scan captures the block layout at fork time.
    let deltas = scan_batch_deltas(&tree, &|doc_id| doc_id > 500);

    // Simulate parent writes after fork.
    for i in 2001..=2500 {
        tree.add(i, 42.0, false, 0);
    }

    let result = tree.apply_gc_batch(&mut deltas.into_iter());

    assert_eq!(result.nodes_applied, 1);
    assert!(result.total_entries_removed <= 500);
}

#[test]
fn apply_gc_batch_empty_leaves_tracking() {
    let mut tree = build_single_leaf_tree(10, false);
    assert_eq!(tree.empty_leaves(), 0);

    // Delete all documents.
    let deltas = scan_batch_deltas(&tree, &|_| false);
    tree.apply_gc_batch(&mut deltas.into_iter());

    assert_eq!(tree.empty_leaves(), 1);
    assert_eq!(tree.num_entries(), 0);
}

#[test]
fn apply_gc_batch_removes_all_multi_leaf() {
    let mut tree = NumericRangeTree::new(false);
    for i in 1..=500u64 {
        tree.add(i, i as f64, false, 0);
    }
    let leaves = tree.num_leaves();
    assert!(leaves > 1);

    // Delete everything.
    let deltas = scan_batch_deltas(&tree, &|_| false);
    tree.apply_gc_batch(&mut deltas.into_iter());

    assert_eq!(tree.num_entries(), 0);
    assert!(tree.empty_leaves() > 0);
}

// ============================================================================
// conditional_trim_empty_leaves tests
// ============================================================================

#[test]
fn conditional_trim_below_threshold() {
    // No documents deleted — no empty leaves.
    let mut tree = build_single_leaf_tree(10, false);
    let freed = tree.conditional_trim_empty_leaves();
    assert_eq!(freed, 0);
}

#[test]
fn conditional_trim_above_threshold() {
    // Build a tree with many entries to force splits, then GC most leaves empty.
    let mut tree = NumericRangeTree::new(false);
    for i in 1..=500 {
        tree.add(i, i as f64, false, 0);
    }

    // GC every leaf, deleting nearly all docs.
    gc_all_leaves(&mut tree, &|doc_id| doc_id > 490);

    if tree.empty_leaves() >= tree.num_leaves() / 2 {
        let leaves_before = tree.num_leaves();
        let freed = tree.conditional_trim_empty_leaves();
        assert!(freed > 0);
        assert!(tree.num_leaves() < leaves_before);
    }
}

// ============================================================================
// trim_empty_leaves tests
// ============================================================================

#[test]
fn trim_single_empty_leaf() {
    let mut tree = NumericRangeTree::new(false);
    for i in 1..=500 {
        tree.add(i, i as f64, false, 0);
    }

    gc_all_leaves(&mut tree, &|doc_id| doc_id > 10);

    let ranges_before = tree.num_ranges();
    let leaves_before = tree.num_leaves();

    if tree.empty_leaves() > 0 {
        let rv = tree.trim_empty_leaves();
        assert!(rv.changed);
        assert!(tree.num_ranges() <= ranges_before);
        assert!(tree.num_leaves() <= leaves_before);
    }
}

#[test]
fn trim_preserves_non_empty_leaves() {
    let mut tree = NumericRangeTree::new(false);
    for i in 1..=500 {
        tree.add(i, i as f64, false, 0);
    }

    gc_all_leaves(&mut tree, &|doc_id| doc_id > 100);

    let entries_before = tree.num_entries();
    tree.trim_empty_leaves();

    // Trim should not change num_entries — only GC removes entries.
    assert_eq!(tree.num_entries(), entries_before);
}

#[test]
fn trim_updates_revision_id() {
    let mut tree = NumericRangeTree::new(false);
    for i in 1..=500 {
        tree.add(i, i as f64, false, 0);
    }

    gc_all_leaves(&mut tree, &|doc_id| doc_id > 100);

    let rev_before = tree.revision_id();
    let rv = tree.trim_empty_leaves();
    if rv.changed {
        assert_ne!(tree.revision_id(), rev_before);
    }
}

// ============================================================================
// Cardinality after GC
// ============================================================================

#[test]
fn cardinality_after_gc_no_new_blocks() {
    let mut tree = build_single_leaf_tree(15, false);

    let cardinality_before = tree.root().range().unwrap().cardinality();
    assert!(cardinality_before > 0);

    // Delete docs 1..=7, keeping 8..=15.
    // Use non-zero HLL registers so cardinality is non-zero after GC.
    let deltas = scan_batch_deltas_with_hll(&tree, &|doc_id| doc_id > 7, |_| {
        let mut hll_with = [0u8; 64];
        hll_with[0] = 5;
        hll_with[1] = 3;
        let hll_without = [0u8; 64];
        (hll_with, hll_without)
    });
    tree.apply_gc_batch(&mut deltas.into_iter());

    let cardinality_after = tree.root().range().unwrap().cardinality();
    assert!(cardinality_after > 0);
}

#[rstest]
#[case(false)]
#[case(true)]
fn cardinality_after_gc_with_new_blocks(#[case] compress_floats: bool) {
    // Single value to prevent splits, many docs for multiple blocks.
    let mut tree = NumericRangeTree::new(compress_floats);
    for i in 1..=2000 {
        tree.add(i, 42.0, false, 0);
    }
    assert!(tree.root().is_leaf());

    // Scan captures the block layout at fork time.
    let deltas = scan_batch_deltas(&tree, &|doc_id| doc_id > 500);

    // Simulate parent writes after fork.
    for i in 2001..=3000 {
        tree.add(i, 42.0, false, 0);
    }

    tree.apply_gc_batch(&mut deltas.into_iter());

    // New blocks added after fork should be rescanned for cardinality.
    // With a single value (42.0), cardinality should be 1 after rescan.
    let cardinality_after = tree.root().range().unwrap().cardinality();
    assert!(cardinality_after > 0);
}

// ============================================================================
// Helpers for batch GC
// ============================================================================

/// Scan the entire tree in DFS order and produce a batch of deltas.
///
/// Returns a `Vec<Option<NodeGcDelta>>` with one entry per DFS node.
/// `None` = skip (no range or no GC work), `Some` = apply.
fn scan_batch_deltas(
    tree: &NumericRangeTree,
    doc_exist: &dyn Fn(u64) -> bool,
) -> Vec<Option<NodeGcDelta>> {
    let mut deltas = Vec::new();
    scan_batch_dfs(tree.root(), doc_exist, &mut deltas);
    deltas
}

/// Like [`scan_batch_deltas`] but with custom HLL register values.
///
/// `hll_fn` receives the delta and returns `(hll_with, hll_without)`.
fn scan_batch_deltas_with_hll(
    tree: &NumericRangeTree,
    doc_exist: &dyn Fn(u64) -> bool,
    hll_fn: impl Fn(&inverted_index::GcScanDelta) -> ([u8; 64], [u8; 64]),
) -> Vec<Option<NodeGcDelta>> {
    let mut deltas = Vec::new();
    scan_batch_dfs_with_hll(tree.root(), doc_exist, &mut deltas, &hll_fn);
    deltas
}

fn scan_batch_dfs(
    node: &NumericRangeNode,
    doc_exist: &dyn Fn(u64) -> bool,
    deltas: &mut Vec<Option<NodeGcDelta>>,
) {
    // One delta per DFS node.
    let delta = node
        .range()
        .and_then(|range| {
            range
                .entries()
                .scan_gc(doc_exist)
                .expect("scan_gc should not fail")
        })
        .map(|delta| NodeGcDelta {
            delta,
            registers_with_last_block: [0u8; 64],
            registers_without_last_block: [0u8; 64],
        });
    deltas.push(delta);

    // Recurse: left then right (pre-order DFS).
    if let Some((left, right)) = node.children() {
        scan_batch_dfs(left, doc_exist, deltas);
        scan_batch_dfs(right, doc_exist, deltas);
    }
}

fn scan_batch_dfs_with_hll(
    node: &NumericRangeNode,
    doc_exist: &dyn Fn(u64) -> bool,
    deltas: &mut Vec<Option<NodeGcDelta>>,
    hll_fn: &dyn Fn(&inverted_index::GcScanDelta) -> ([u8; 64], [u8; 64]),
) {
    let delta = node
        .range()
        .and_then(|range| {
            range
                .entries()
                .scan_gc(doc_exist)
                .expect("scan_gc should not fail")
        })
        .map(|delta| {
            let (hll_with, hll_without) = hll_fn(&delta);
            NodeGcDelta {
                delta,
                registers_with_last_block: hll_with,
                registers_without_last_block: hll_without,
            }
        });
    deltas.push(delta);

    if let Some((left, right)) = node.children() {
        scan_batch_dfs_with_hll(left, doc_exist, deltas, hll_fn);
        scan_batch_dfs_with_hll(right, doc_exist, deltas, hll_fn);
    }
}

/// Apply GC to every leaf in the tree using `apply_gc_batch`.
fn gc_all_leaves(tree: &mut NumericRangeTree, doc_exist: &dyn Fn(u64) -> bool) {
    let deltas = scan_batch_deltas(tree, doc_exist);
    tree.apply_gc_batch(&mut deltas.into_iter());
}

// ============================================================================
// GC repopulation and intensive delete tests
// ============================================================================

#[rstest]
#[case(false)]
#[case(true)]
fn test_gc_delete_all_and_repopulate(#[case] compress_floats: bool) {
    // Mirror C `testNumericCompleteGCAndRepopulation`.
    let mut tree = NumericRangeTree::new(compress_floats);
    for i in 1..=100u64 {
        tree.add(i, (i % 10 + 1) as f64, false, 0);
    }
    assert_eq!(tree.num_entries(), 100);

    // GC-delete all docs.
    gc_all_leaves(&mut tree, &|_| false);
    tree.trim_empty_leaves();

    // Tree should be empty.
    assert_eq!(tree.num_entries(), 0);

    // Repopulate with new docs (IDs must be > last_doc_id = 100).
    for i in 101..=150u64 {
        tree.add(i, (i % 10 + 1) as f64, false, 0);
    }
    assert_eq!(tree.num_entries(), 50);

    // Verify find() works on the repopulated tree.
    let filter = inverted_index::NumericFilter {
        min: 1.0,
        max: 10.0,
        ..Default::default()
    };
    let ranges = tree.find(&filter);
    assert!(
        !ranges.is_empty(),
        "find() should return results after repopulation"
    );
}

#[rstest]
#[case(false)]
#[case(true)]
fn test_gc_intensive_alternating_deletes(#[case] compress_floats: bool) {
    // Mirror C `testNumericGCIntensive`: insert 1000 same-value docs,
    // delete every other one.
    let mut tree = NumericRangeTree::new(compress_floats);
    for i in 1..=1000u64 {
        tree.add(i, 42.0, false, 0);
    }
    assert_eq!(tree.num_entries(), 1000);
    assert!(tree.root().is_leaf(), "single value should not split");

    // Delete odd doc IDs via batch GC.
    let deltas = scan_batch_deltas(&tree, &|doc_id| doc_id % 2 == 0);
    let result = tree.apply_gc_batch(&mut deltas.into_iter());

    assert_eq!(result.total_entries_removed, 500);
    assert_eq!(tree.num_entries(), 500);

    // The remaining 500 docs should be the even ones.
    let root_range = tree.root().range().expect("root must have a range");
    assert_eq!(root_range.num_docs(), 500);
}

#[test]
fn test_trim_merges_tree() {
    // Mirror C `testNumericMergesTrees`: create enough ranges, delete to
    // empty half, then trim and verify range count decreases.
    let mut tree = NumericRangeTree::new(false);
    for i in 1..=500u64 {
        tree.add(i, i as f64, false, 0);
    }

    let ranges_before = tree.num_ranges();
    let leaves_before = tree.num_leaves();
    assert!(
        leaves_before > 1,
        "tree should have multiple leaves after 500 distinct values"
    );

    // Delete most docs to create empty leaves.
    gc_all_leaves(&mut tree, &|doc_id| doc_id > 400);

    // Trim.
    let rv = tree.trim_empty_leaves();

    if rv.changed {
        assert!(
            tree.num_ranges() < ranges_before,
            "trim should reduce range count: {} < {ranges_before}",
            tree.num_ranges()
        );
        assert!(
            tree.num_leaves() < leaves_before,
            "trim should reduce leaf count: {} < {leaves_before}",
            tree.num_leaves()
        );
    }
}
