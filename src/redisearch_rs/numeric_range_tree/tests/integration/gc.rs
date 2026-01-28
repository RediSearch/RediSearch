/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for garbage collection in the numeric range tree.

use numeric_range_tree::{NodeGcDelta, NumericRangeTree};
use rstest::rstest;

use super::helpers::{
    assert_tree_invariants, gc_all_leaves, scan_all_node_deltas, scan_node_delta,
    scan_node_delta_with_hll,
};

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
// apply_gc_to_node tests
// ============================================================================

#[rstest]
#[case(false)]
#[case(true)]
fn apply_gc_to_single_leaf(#[case] compress_floats: bool) {
    let mut tree = build_single_leaf_tree(10, compress_floats);
    let entries_before = tree.num_entries();

    // Scan root leaf.
    let delta = scan_node_delta(&tree, tree.root_index(), &|doc_id| doc_id > 5);

    let delta = delta.expect("should have GC work");
    let result = tree.apply_gc_to_node(tree.root_index(), delta);

    assert_eq!(result.entries_removed, 5);
    assert_eq!(tree.num_entries(), entries_before - 5);
}

#[rstest]
#[case(false)]
#[case(true)]
fn apply_gc_to_node_in_split_tree(#[case] compress_floats: bool) {
    // Build a tree with multiple leaves.
    let mut tree = NumericRangeTree::new(compress_floats);
    for i in 1..=500u64 {
        tree.add(i, i as f64, false, 0);
    }
    assert!(tree.num_leaves() > 1);

    let entries_before = tree.num_entries();

    // Delete docs 1..=250 — apply GC per node.
    let deltas = scan_all_node_deltas(&tree, &|doc_id| doc_id > 250);

    let mut total_removed = 0;
    for (node_idx, delta) in deltas {
        let result = tree.apply_gc_to_node(node_idx, delta);
        total_removed += result.entries_removed;
    }

    assert!(total_removed > 0);
    assert!(tree.num_entries() < entries_before);
}

#[rstest]
#[case(false)]
#[case(true)]
fn apply_gc_to_node_all_skip(#[case] compress_floats: bool) {
    // No documents deleted — every node should be skipped.
    let tree = build_single_leaf_tree(10, compress_floats);
    let entries_before = tree.num_entries();

    let deltas = scan_all_node_deltas(&tree, &|_| true);
    assert!(deltas.is_empty(), "no GC work expected");

    assert_eq!(tree.num_entries(), entries_before);
}

#[rstest]
#[case(false)]
#[case(true)]
fn apply_gc_to_node_with_blocks_since_fork(#[case] compress_floats: bool) {
    let mut tree = NumericRangeTree::new(compress_floats);
    for i in 1..=2000 {
        tree.add(i, 42.0, false, 0);
    }
    assert!(tree.root().is_leaf());

    // Scan captures the block layout at fork time.
    let delta = scan_node_delta(&tree, tree.root_index(), &|doc_id| doc_id > 500)
        .expect("should have GC work");

    // Simulate parent writes after fork.
    for i in 2001..=2500 {
        tree.add(i, 42.0, false, 0);
    }

    let result = tree.apply_gc_to_node(tree.root_index(), delta);

    assert!(result.entries_removed <= 500);
}

#[rstest]
#[case(false)]
#[case(true)]
fn apply_gc_to_node_empty_result(#[case] compress_floats: bool) {
    let mut tree = build_single_leaf_tree(10, compress_floats);
    assert_eq!(tree.empty_leaves(), 0);

    // Delete all documents.
    let delta = scan_node_delta(&tree, tree.root_index(), &|_| false).expect("should have GC work");
    let result = tree.apply_gc_to_node(tree.root_index(), delta);

    assert!(result.became_empty);
    assert_eq!(tree.empty_leaves(), 1);
    assert_eq!(tree.num_entries(), 0);
}

#[rstest]
#[case(false)]
#[case(true)]
fn apply_gc_removes_all_multi_leaf(#[case] compress_floats: bool) {
    let mut tree = NumericRangeTree::new(compress_floats);
    for i in 1..=500u64 {
        tree.add(i, i as f64, false, 0);
    }
    let leaves = tree.num_leaves();
    assert!(leaves > 1);

    // Delete everything.
    gc_all_leaves(&mut tree, &|_| false);

    assert_eq!(tree.num_entries(), 0);
    assert!(tree.empty_leaves() > 0);
}

// ============================================================================
// compact_if_sparse tests
// ============================================================================

#[rstest]
#[case(false)]
#[case(true)]
fn conditional_trim_below_threshold(#[case] compress_floats: bool) {
    // No documents deleted — no empty leaves.
    let mut tree = build_single_leaf_tree(10, compress_floats);
    let freed = tree.compact_if_sparse();
    assert_eq!(freed, 0);
}

#[rstest]
#[case(false)]
#[case(true)]
fn conditional_trim_above_threshold(#[case] compress_floats: bool) {
    // Build a tree with many entries to force splits, then GC most leaves empty.
    let mut tree = NumericRangeTree::new(compress_floats);
    for i in 1..=500 {
        tree.add(i, i as f64, false, 0);
    }

    // GC every leaf, deleting nearly all docs.
    gc_all_leaves(&mut tree, &|doc_id| doc_id > 490);

    if tree.empty_leaves() >= tree.num_leaves() / 2 {
        let leaves_before = tree.num_leaves();
        let freed = tree.compact_if_sparse();
        assert!(freed > 0);
        assert!(tree.num_leaves() < leaves_before);
    }
}

// ============================================================================
// trim_empty_leaves tests
// ============================================================================

#[rstest]
#[case(false)]
#[case(true)]
fn trim_single_empty_leaf(#[case] compress_floats: bool) {
    let mut tree = NumericRangeTree::new(compress_floats);
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

#[rstest]
#[case(false)]
#[case(true)]
fn trim_preserves_non_empty_leaves(#[case] compress_floats: bool) {
    let mut tree = NumericRangeTree::new(compress_floats);
    for i in 1..=500 {
        tree.add(i, i as f64, false, 0);
    }

    gc_all_leaves(&mut tree, &|doc_id| doc_id > 100);

    let entries_before = tree.num_entries();
    tree.trim_empty_leaves();

    // Trim should not change num_entries — only GC removes entries.
    assert_eq!(tree.num_entries(), entries_before);
}

#[rstest]
#[case(false)]
#[case(true)]
fn trim_updates_revision_id(#[case] compress_floats: bool) {
    let mut tree = NumericRangeTree::new(compress_floats);
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

#[rstest]
#[case(false)]
#[case(true)]
fn cardinality_after_gc_no_new_blocks(#[case] compress_floats: bool) {
    let mut tree = build_single_leaf_tree(15, compress_floats);

    let cardinality_before = tree.root().range().unwrap().cardinality();
    assert!(cardinality_before > 0);

    // Delete docs 1..=7, keeping 8..=15.
    // Use non-zero HLL registers so cardinality is non-zero after GC.
    let delta = scan_node_delta_with_hll(&tree, tree.root_index(), &|doc_id| doc_id > 7, |_| {
        let mut hll_with = [0u8; 64];
        hll_with[0] = 5;
        hll_with[1] = 3;
        let hll_without = [0u8; 64];
        (hll_with, hll_without)
    });
    let delta = delta.expect("should have GC work");
    tree.apply_gc_to_node(tree.root_index(), delta);

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
    let delta = scan_node_delta(&tree, tree.root_index(), &|doc_id| doc_id > 500)
        .expect("should have GC work");

    // Simulate parent writes after fork.
    for i in 2001..=3000 {
        tree.add(i, 42.0, false, 0);
    }

    tree.apply_gc_to_node(tree.root_index(), delta);

    // New blocks added after fork should be rescanned for cardinality.
    // With a single value (42.0), cardinality should be 1 after rescan.
    let cardinality_after = tree.root().range().unwrap().cardinality();
    assert!(cardinality_after > 0);
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

    // Delete odd doc IDs via per-node GC.
    let deltas = scan_all_node_deltas(&tree, &|doc_id| doc_id % 2 == 0);
    let mut total_removed = 0;
    for (node_idx, delta) in deltas {
        let result = tree.apply_gc_to_node(node_idx, delta);
        total_removed += result.entries_removed;
    }

    assert_eq!(total_removed, 500);
    assert_eq!(tree.num_entries(), 500);

    // The remaining 500 docs should be the even ones.
    let root_range = tree.root().range().expect("root must have a range");
    assert_eq!(root_range.num_docs(), 500);
}

#[rstest]
#[case(false)]
#[case(true)]
fn test_trim_merges_tree(#[case] compress_floats: bool) {
    // Mirror C `testNumericMergesTrees`: create enough ranges, delete to
    // empty half, then trim and verify range count decreases.
    let mut tree = NumericRangeTree::new(compress_floats);
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

// ============================================================================
// GC and trim with retained internal ranges (max_depth_range > 0)
// ============================================================================

#[test]
fn test_gc_on_node_without_range() {
    // Build a split tree with max_depth_range=0 so the root (internal) has no range.
    let mut tree = NumericRangeTree::new(false);
    for i in 1..=500u64 {
        tree.add(i, i as f64, false, 0);
    }
    assert!(!tree.root().is_leaf(), "tree should have split");
    assert!(
        tree.root().range().is_none(),
        "root should have no range with max_depth_range=0"
    );

    // Scan any leaf to get a valid delta.
    let deltas = scan_all_node_deltas(&tree, &|doc_id| doc_id > 5);
    assert!(!deltas.is_empty());
    let (leaf_node_idx, delta) = deltas.into_iter().next().unwrap();

    // Apply GC to the root (which has no range) — should early-return.
    let result = tree.apply_gc_to_node(
        tree.root_index(),
        NodeGcDelta {
            delta: delta.delta,
            registers_with_last_block: delta.registers_with_last_block,
            registers_without_last_block: delta.registers_without_last_block,
        },
    );
    assert_eq!(
        result.entries_removed, 0,
        "applying GC to a node without a range should be a no-op"
    );

    // Also apply the original delta to the correct leaf to verify it works.
    let leaf_delta = scan_node_delta(&tree, leaf_node_idx, &|doc_id| doc_id > 5);
    if let Some(d) = leaf_delta {
        tree.apply_gc_to_node(leaf_node_idx, d);
    }
}

#[rstest]
#[case(false)]
#[case(true)]
fn test_gc_and_trim_with_retained_internal_ranges(#[case] compress_floats: bool) {
    // Build tree with max_depth_range=2 so internal nodes retain ranges.
    let mut tree = NumericRangeTree::new(compress_floats);
    for i in 1..=500u64 {
        tree.add(i, i as f64, false, 2);
    }

    assert!(
        tree.num_ranges() > tree.num_leaves(),
        "with max_depth_range=2, internal nodes should retain ranges"
    );

    // GC-delete most docs (keep doc_id > 450).
    gc_all_leaves(&mut tree, &|doc_id| doc_id > 450);

    // Trim empty leaves.
    tree.trim_empty_leaves();

    // Tree invariants should still hold.
    assert_tree_invariants(&tree);
}

#[test]
fn test_trim_with_nonempty_internal_range_prevents_removal() {
    // Build tree with max_depth_range=2.
    let mut tree = NumericRangeTree::new(false);
    for i in 1..=500u64 {
        tree.add(i, i as f64, false, 2);
    }

    // GC-delete only docs 1–100 (keep 101–500).
    // This creates some empty leaves but parent internal nodes still have data.
    gc_all_leaves(&mut tree, &|doc_id| doc_id > 100);

    let entries_before = tree.num_entries();

    // Trim empty leaves.
    tree.trim_empty_leaves();

    // Surviving docs should be preserved — num_entries should not change from trim.
    assert_eq!(
        tree.num_entries(),
        entries_before,
        "trim should not change num_entries (surviving docs must be preserved)"
    );
    assert_tree_invariants(&tree);
}
