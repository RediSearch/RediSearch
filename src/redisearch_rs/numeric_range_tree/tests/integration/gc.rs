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
    DEEP_TREE_ENTRIES, ENTRIES_PER_BLOCK, SPLIT_TRIGGER, build_tree, gc_all_ranges,
    scan_all_node_deltas, scan_node_delta, scan_node_delta_with_hll,
};

/// Build a single-leaf tree (no splits) with `count` entries.
///
/// Uses sequential doc IDs but only a few distinct values to keep cardinality
/// below the split threshold (`MINIMUM_RANGE_CARDINALITY` at depth 0).
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
fn apply_gc_to_single_leaf(#[values(false, true)] compress_floats: bool) {
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
fn apply_gc_to_node_in_split_tree(
    #[values(false, true)] compress_floats: bool,
    #[values(0, 2)] max_depth_range: usize,
) {
    // Build a tree with multiple leaves.
    let n = SPLIT_TRIGGER * 2;
    let mut tree = build_tree(n, compress_floats, max_depth_range);
    assert!(tree.num_leaves() > 1);

    // Delete the lower half — apply GC per node.
    gc_all_ranges(&mut tree, &|doc_id| doc_id > SPLIT_TRIGGER);

    // num_entries should reflect exactly the surviving documents.
    assert_eq!(tree.num_entries(), (n - SPLIT_TRIGGER) as usize);
}

#[rstest]
fn apply_gc_to_node_all_skip(#[values(false, true)] compress_floats: bool) {
    // No documents deleted — every node should be skipped.
    let tree = build_single_leaf_tree(10, compress_floats);
    let entries_before = tree.num_entries();

    let deltas = scan_all_node_deltas(&tree, &|_| true);
    assert!(deltas.is_empty(), "no GC work expected");

    assert_eq!(tree.num_entries(), entries_before);
}

#[rstest]
fn apply_gc_to_node_with_blocks_since_fork(#[values(false, true)] compress_floats: bool) {
    let n = ENTRIES_PER_BLOCK * 2;
    let mut tree = NumericRangeTree::new(compress_floats);
    for i in 1..=n {
        tree.add(i, 42.0, false, 0);
    }
    assert!(tree.root().is_leaf());

    // Scan captures the block layout at fork time.
    let delta = scan_node_delta(&tree, tree.root_index(), &|doc_id| {
        doc_id > ENTRIES_PER_BLOCK
    })
    .expect("should have GC work");

    // Simulate parent writes after fork.
    for i in (n + 1)..=(n + ENTRIES_PER_BLOCK) {
        tree.add(i, 42.0, false, 0);
    }

    let result = tree.apply_gc_to_node(tree.root_index(), delta);

    assert!(result.entries_removed <= ENTRIES_PER_BLOCK as usize);
}

#[rstest]
fn apply_gc_to_node_empty_result(#[values(false, true)] compress_floats: bool) {
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
fn apply_gc_removes_all_multi_leaf(
    #[values(false, true)] compress_floats: bool,
    #[values(0, 2)] max_depth_range: usize,
) {
    let n = SPLIT_TRIGGER * 2;
    let mut tree = build_tree(n, compress_floats, max_depth_range);
    let leaves = tree.num_leaves();
    assert!(leaves > 1);

    // Delete everything.
    gc_all_ranges(&mut tree, &|_| false);

    assert_eq!(tree.num_entries(), 0);
    assert!(tree.empty_leaves() > 0);
}

// ============================================================================
// compact_if_sparse tests
// ============================================================================

#[rstest]
fn conditional_trim_below_threshold(#[values(false, true)] compress_floats: bool) {
    // No documents deleted — no empty leaves.
    let mut tree = build_single_leaf_tree(10, compress_floats);
    let freed = tree.compact_if_sparse();
    assert_eq!(freed, 0);
}

#[rstest]
fn conditional_trim_above_threshold(#[values(false, true)] compress_floats: bool) {
    // Build a tree with many entries to force splits, then GC most leaves empty.
    let n = SPLIT_TRIGGER * 2;
    let mut tree = build_tree(n, compress_floats, 0);

    // GC every leaf, deleting nearly all docs.
    gc_all_ranges(&mut tree, &|doc_id| doc_id > SPLIT_TRIGGER);

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
fn trim_single_empty_leaf(#[values(false, true)] compress_floats: bool) {
    let n = SPLIT_TRIGGER * 2;
    let mut tree = build_tree(n, compress_floats, 0);

    // Delete the lower half — at least one leaf should become empty.
    gc_all_ranges(&mut tree, &|doc_id| doc_id > SPLIT_TRIGGER);

    let ranges_before = tree.num_ranges();
    let leaves_before = tree.num_leaves();

    assert!(tree.empty_leaves() > 0);
    let rv = tree.trim_empty_leaves();
    assert!(rv.changed);
    assert!(tree.num_ranges() <= ranges_before);
    assert!(tree.num_leaves() <= leaves_before);
}

// ============================================================================
// Cardinality after GC
// ============================================================================

#[rstest]
fn cardinality_after_gc_no_new_blocks(#[values(false, true)] compress_floats: bool) {
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
fn cardinality_after_gc_with_new_blocks(#[values(false, true)] compress_floats: bool) {
    // Single value to prevent splits, many docs for multiple blocks.
    let n = ENTRIES_PER_BLOCK * 2;
    let mut tree = NumericRangeTree::new(compress_floats);
    for i in 1..=n {
        tree.add(i, 42.0, false, 0);
    }
    assert!(tree.root().is_leaf());

    // Scan captures the block layout at fork time.
    let delta = scan_node_delta(&tree, tree.root_index(), &|doc_id| {
        doc_id > ENTRIES_PER_BLOCK
    })
    .expect("should have GC work");

    // Simulate parent writes after fork.
    for i in (n + 1)..=(n + ENTRIES_PER_BLOCK) {
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
fn gc_delete_all_and_repopulate(#[values(false, true)] compress_floats: bool) {
    // Mirror C `testNumericCompleteGCAndRepopulation`.
    let mut tree = NumericRangeTree::new(compress_floats);
    for i in 1..=100u64 {
        tree.add(i, (i % 10 + 1) as f64, false, 0);
    }
    assert_eq!(tree.num_entries(), 100);

    // GC-delete all docs.
    gc_all_ranges(&mut tree, &|_| false);
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
fn gc_intensive_alternating_deletes(#[values(false, true)] compress_floats: bool) {
    // Mirror C `testNumericGCIntensive`: insert same-value docs,
    // delete every other one.
    let n = ENTRIES_PER_BLOCK * 2;
    let mut tree = NumericRangeTree::new(compress_floats);
    for i in 1..=n {
        tree.add(i, 42.0, false, 0);
    }
    assert_eq!(tree.num_entries(), n as usize);
    assert!(tree.root().is_leaf(), "single value should not split");

    // Delete odd doc IDs via per-node GC.
    let deltas = scan_all_node_deltas(&tree, &|doc_id| doc_id % 2 == 0);
    let mut total_removed = 0;
    for (node_idx, delta) in deltas {
        let result = tree.apply_gc_to_node(node_idx, delta);
        total_removed += result.entries_removed;
    }

    assert_eq!(total_removed, (n / 2) as usize);
    assert_eq!(tree.num_entries(), (n / 2) as usize);

    // The remaining docs should be the even ones.
    let root_range = tree.root().range().expect("root must have a range");
    assert_eq!(root_range.num_docs(), (n / 2) as u32);
}

#[rstest]
fn trim_merges_tree(
    #[values(false, true)] compress_floats: bool,
    #[values(0, 2)] max_depth_range: usize,
) {
    // Mirror C `testNumericMergesTrees`: create enough ranges, delete to
    // empty half, then trim and verify range count decreases.
    let n = SPLIT_TRIGGER * 2;
    let mut tree = build_tree(n, compress_floats, max_depth_range);

    let ranges_before = tree.num_ranges();
    let leaves_before = tree.num_leaves();
    assert!(
        leaves_before > 1,
        "tree should have multiple leaves after {n} distinct values"
    );

    // Delete most docs to create empty leaves, keeping only SPLIT_TRIGGER.
    gc_all_ranges(&mut tree, &|doc_id| doc_id > SPLIT_TRIGGER);

    // Verify num_entries is correct after GC.
    assert_eq!(tree.num_entries(), SPLIT_TRIGGER as usize);

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

    // num_entries should be unchanged by trim.
    assert_eq!(tree.num_entries(), SPLIT_TRIGGER as usize);
}

// ============================================================================
// GC and trim with retained internal ranges (max_depth_range > 0)
// ============================================================================

#[test]
fn gc_on_node_without_range() {
    // Build a split tree with max_depth_range=0 so the root (internal) has no range.
    let n = SPLIT_TRIGGER * 2;
    let mut tree = build_tree(n, false, 0);
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
fn gc_and_trim_with_retained_internal_ranges(#[values(false, true)] compress_floats: bool) {
    // Build tree with max_depth_range=2 so internal nodes retain ranges.
    let n = SPLIT_TRIGGER * 2;
    let mut tree = build_tree(n, compress_floats, 2);

    assert!(
        tree.num_ranges() > tree.num_leaves(),
        "with max_depth_range=2, internal nodes should retain ranges"
    );

    // GC-delete most docs (keep only the upper SPLIT_TRIGGER).
    gc_all_ranges(&mut tree, &|doc_id| doc_id > SPLIT_TRIGGER);

    // Trim empty leaves.
    tree.trim_empty_leaves();
}

// ============================================================================
// Compaction and targeted trim tests
// ============================================================================

/// Covers:
/// - `compact_slab`: slab holes from trim → compaction moves entries and remaps
///   parent/child pointers.
///
/// Strategy: build a deep tree with retained internal ranges (`max_depth_range=2`),
/// then delete all documents in the *left* subtree while keeping the *right*.
/// The left subtree's nodes were allocated first (low slab indices). Trimming them
/// creates slab holes *below* the surviving right-subtree nodes. `compact_slab`
/// then compacts those surviving entries downward and remaps all parent/child pointers.
#[rstest]
fn compact_slab_reclaims_memory(#[values(false, true)] compress_floats: bool) {
    let n = DEEP_TREE_ENTRIES;
    let mut tree = build_tree(n, compress_floats, 2);
    let leaves_before = tree.num_leaves();
    assert!(leaves_before > 1);
    assert!(
        tree.num_ranges() > tree.num_leaves(),
        "with max_depth_range=2, internal nodes should retain ranges"
    );

    // Delete the low-value half (left subtree). The root split is near the
    // median, so deleting the lower half empties the left subtree.
    // Keep the high-value half (right subtree) alive.
    gc_all_ranges(&mut tree, &|doc_id| doc_id > n / 2);
    assert_eq!(tree.num_entries(), (n - n / 2) as usize);

    // At this point many left-subtree leaves are empty but right-subtree leaves
    // are populated. Enough empty leaves should exceed the 50% threshold.
    assert!(
        tree.empty_leaves() >= tree.num_leaves() / 2,
        "empty_leaves ({}) should be >= half of num_leaves ({})",
        tree.empty_leaves(),
        tree.num_leaves()
    );

    // compact_if_sparse will: (1) _trim_empty_leaves → frees left-subtree nodes
    // at low slab indices, (2) compact_slab → moves surviving right-subtree nodes
    // down to fill gaps.
    let freed = tree.compact_if_sparse();
    assert!(freed > 0, "compaction should free memory");
    assert!(
        tree.num_leaves() < leaves_before,
        "trim should reduce leaf count: {} < {leaves_before}",
        tree.num_leaves()
    );
    // The surviving right subtree should still be queryable.
    assert_eq!(tree.num_entries(), (n - n / 2) as usize);
}

/// Covers internal range freeing: remove range on internal nodes
/// when both children are empty.
///
/// Strategy: build a tree with `max_depth_range=2` (internal nodes retain ranges),
/// then GC-delete *all* documents so every leaf is empty. When `trim_empty_leaves`
/// walks up the tree, it finds both children empty at every level, triggering
/// freeing internal nodes' own ranges.
#[rstest]
fn trim_frees_internal_ranges_when_all_empty(#[values(false, true)] compress_floats: bool) {
    let n = SPLIT_TRIGGER * 2;
    let mut tree = build_tree(n, compress_floats, 2);
    assert!(tree.num_leaves() > 1);
    assert!(
        tree.num_ranges() > tree.num_leaves(),
        "with max_depth_range=2, internal nodes should retain ranges"
    );

    // Delete everything — GC all ranges (leaves + internal).
    gc_all_ranges(&mut tree, &|_| false);
    assert_eq!(tree.num_entries(), 0);
    assert_eq!(tree.empty_leaves(), tree.num_leaves());

    // Trim: walks the tree, finds both children empty at each level,
    // frees internal ranges, and collapses to a single leaf.
    let rv = tree.trim_empty_leaves();
    assert!(rv.changed, "trim should have changed the tree");
    assert_eq!(tree.num_leaves(), 1);
    assert_eq!(tree.num_entries(), 0);
}

/// Covers:
/// - When only the right subtree is empty,
///   it is freed and the left child is promoted in place.
#[rstest]
fn trim_promotes_left_when_right_empty(#[values(false, true)] compress_floats: bool) {
    let n = DEEP_TREE_ENTRIES;
    let mut tree = build_tree(n, compress_floats, 0);
    assert!(!tree.root().is_leaf());

    let leaves_before = tree.num_leaves();

    // Keep only low values (left subtree). Delete high values (right subtree).
    // With sequential doc_id == value, keeping doc_id <= SPLIT_TRIGGER keeps
    // values that should all be in the leftmost leaf.
    gc_all_ranges(&mut tree, &|doc_id| doc_id <= SPLIT_TRIGGER);

    let rv = tree.trim_empty_leaves();
    assert!(rv.changed, "trim should have changed the tree");
    assert!(
        tree.num_leaves() < leaves_before,
        "trim should reduce leaf count: {} < {leaves_before}",
        tree.num_leaves()
    );
    assert_eq!(tree.num_entries(), SPLIT_TRIGGER as usize);
}

/// By deleting a band in the middle of the value range, we empty leaves deep
/// inside the tree. The ancestors of those leaves have both children survive
/// (left extremes and right extremes are populated), but the structural change
/// from trimming triggers the balance path.
#[rstest]
fn trim_rebalances_surviving_ancestors(#[values(false, true)] compress_floats: bool) {
    let n = DEEP_TREE_ENTRIES;
    let mut tree = build_tree(DEEP_TREE_ENTRIES, compress_floats, 0);
    let leaves_before = tree.num_leaves();
    assert!(
        leaves_before >= 4,
        "tree should have multiple leaves, got {leaves_before}"
    );

    // Delete docs in a band: keep only the extremes.
    // Low values → left subtree survives
    // High values → right subtree survives
    // Middle values → emptied → trimmed
    // This creates a scenario where deep subtrees are trimmed but both
    // children of the root (and potentially other ancestors) survive,
    // triggering the balance_node path at surviving ancestors.
    gc_all_ranges(&mut tree, &|doc_id| {
        doc_id <= SPLIT_TRIGGER || doc_id > n - SPLIT_TRIGGER
    });

    let depth_before = tree.root().max_depth();
    let rv = tree.trim_empty_leaves();
    assert!(rv.changed, "trim should have changed the tree");

    // After trim, depth should not increase since most middle nodes were removed.
    assert!(
        tree.root().max_depth() <= depth_before,
        "tree should not grow after trimming: {} <= {depth_before}",
        tree.root().max_depth()
    );
    assert_eq!(tree.num_entries(), (SPLIT_TRIGGER * 2) as usize);
}
