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

use numeric_range_tree::test_utils::{
    DEEP_TREE_ENTRIES, ENTRIES_PER_BLOCK, SPLIT_TRIGGER, build_tree, gc_all_ranges,
    scan_all_node_deltas, scan_node_delta, scan_node_delta_with_hll,
};

/// Build a single-leaf tree with post-fork writes for testing blocks-since-fork scenarios.
///
/// Creates a tree with `ENTRIES_PER_BLOCK * 2` entries (value 42.0), scans a GC
/// delta that deletes the first block, then simulates parent writes by adding
/// another block of entries after the scan.
///
/// Returns the tree (with post-fork writes applied) and the pre-fork GC delta.
fn build_tree_with_post_fork_writes(compress_floats: bool) -> (NumericRangeTree, NodeGcDelta) {
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

    (tree, delta)
}

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
    let size_before = tree.inverted_indexes_size();
    assert!(size_before > 0);

    // Scan root leaf.
    let delta = scan_node_delta(&tree, tree.root_index(), &|doc_id| doc_id > 5);

    let delta = delta.expect("should have GC work");
    let result = tree.apply_gc_to_node(tree.root_index(), delta).unwrap();

    assert_eq!(result.index_gc_info.entries_removed, 5);
    assert_eq!(
        tree.num_entries(),
        entries_before - result.index_gc_info.entries_removed
    );
    assert!(
        result.index_gc_info.bytes_freed > 0,
        "GC that removes entries should free bytes"
    );
    assert!(
        tree.inverted_indexes_size() < size_before,
        "inverted_indexes_size should decrease after GC: {} < {size_before}",
        tree.inverted_indexes_size()
    );
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
    let (mut tree, delta) = build_tree_with_post_fork_writes(compress_floats);

    let result = tree.apply_gc_to_node(tree.root_index(), delta).unwrap();

    assert!(
        result.index_gc_info.entries_removed > 0,
        "GC should have removed entries"
    );
    assert!(result.index_gc_info.entries_removed <= ENTRIES_PER_BLOCK as usize);

    // New blocks added after fork should be rescanned for cardinality.
    // With a single value (42.0), cardinality should be exactly 1 after rescan.
    let cardinality_after = tree.root().range().unwrap().cardinality();
    assert_eq!(cardinality_after, 1);
}

#[rstest]
fn apply_gc_to_node_empty_result(#[values(false, true)] compress_floats: bool) {
    let mut tree = build_single_leaf_tree(10, compress_floats);
    assert_eq!(tree.empty_leaves(), 0);

    // Delete all documents.
    let delta = scan_node_delta(&tree, tree.root_index(), &|_| false).expect("should have GC work");
    let result = tree.apply_gc_to_node(tree.root_index(), delta).unwrap();

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
    let free_stats = tree.compact_if_sparse();
    assert_eq!(free_stats.inverted_index_size_delta, 0);
    assert_eq!(free_stats.node_size_delta, 0);
}

#[rstest]
fn conditional_trim_above_threshold(#[values(false, true)] compress_floats: bool) {
    // Build a tree with many entries to force splits, then GC most leaves empty.
    let n = SPLIT_TRIGGER * 2;
    let mut tree = build_tree(n, compress_floats, 0);

    // GC every leaf, deleting nearly all docs.
    gc_all_ranges(&mut tree, &|doc_id| doc_id > SPLIT_TRIGGER);

    assert!(
        tree.is_sparse(),
        "empty_leaves ({}) should be >= half of num_leaves ({})",
        tree.empty_leaves(),
        tree.num_leaves()
    );

    let leaves_before = tree.num_leaves();
    let free_stats = tree.compact_if_sparse();
    assert!(free_stats.inverted_index_size_delta < 0);
    assert!(free_stats.node_size_delta < 0);
    assert!(tree.num_leaves() < leaves_before);
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

    // The HLL registers are synthetic (set manually above), so we can't
    // predict the exact cardinality estimate — only verify it's non-zero.
    let cardinality_after = tree.root().range().unwrap().cardinality();
    assert!(cardinality_after > 0);
}

#[rstest]
fn hll_cardinality_is_recomputed_correctly_when_last_block_fully_emptied(
    #[values(false, true)] compress_floats: bool,
) {
    // Build a single-leaf tree with 3 blocks:
    // - Block 0 (full): value 1.0
    // - Block 1 (full): value 2.0
    // - Block 2 (partial, half-full): value 3.0
    let partial = ENTRIES_PER_BLOCK / 2;
    let n = ENTRIES_PER_BLOCK * 2 + partial;
    let mut tree = NumericRangeTree::new(compress_floats);
    for i in 1..=n {
        let value = if i <= ENTRIES_PER_BLOCK {
            1.0
        } else if i <= ENTRIES_PER_BLOCK * 2 {
            2.0
        } else {
            3.0
        };
        tree.add(i, value, false, 0);
    }
    assert!(tree.root().is_leaf());
    let cardinality = tree.root().range().unwrap().cardinality();
    assert_eq!(
        cardinality, 3,
        "cardinality should reflect all 3 distinct values (1.0, 2.0, 3.0), got {cardinality}"
    );

    // Scan keeping blocks 0 and 1, deleting all of block 2.
    let delta = tree
        .node(tree.root_index())
        .scan_gc(&|doc_id| doc_id <= ENTRIES_PER_BLOCK * 2)
        .expect("should have GC work");

    // Simulate parent writes after scan: add 1 entry with value 4.0 to
    // the last block (block 2, still has room). This changes block 2's
    // num_entries, triggering ignored_last_block = true during apply.
    tree.add(n + 1, 4.0, false, 0);

    // Apply GC delta.
    let result = tree.apply_gc_to_node(tree.root_index(), delta).unwrap();

    assert!(
        result.index_gc_info.ignored_last_block,
        "last block should be ignored since its num_entries changed post-fork"
    );

    // After apply:
    // - Block 0: all entries survive (value 1.0)
    // - Block 1: all entries survive (value 2.0)
    // - Block 2: delta was ignored (entries not deleted), plus post-fork entry (value 4.0)
    // All four distinct values (1.0, 2.0, 3.0, 4.0) are present.
    let cardinality = tree.root().range().unwrap().cardinality();
    assert_eq!(
        cardinality, 4,
        "cardinality should reflect all 4 distinct values (1.0, 2.0, 3.0, 4.0), got {cardinality}"
    );
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
        let result = tree.apply_gc_to_node(node_idx, delta).unwrap();
        total_removed += result.index_gc_info.entries_removed;
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

    if max_depth_range != 0 {
        assert!(
            tree.num_ranges() > tree.num_leaves(),
            "with max_depth_range={max_depth_range}, internal nodes should retain ranges"
        );
    }

    // Delete most docs to create empty leaves, keeping only SPLIT_TRIGGER.
    gc_all_ranges(&mut tree, &|doc_id| doc_id > SPLIT_TRIGGER);

    // Verify num_entries is correct after GC.
    assert_eq!(tree.num_entries(), SPLIT_TRIGGER as usize);

    // Some leaves should now be empty.
    assert!(
        tree.empty_leaves() > 0,
        "GC should have created empty leaves"
    );

    // Trim.
    let rv = tree.trim_empty_leaves();

    if max_depth_range == 0 {
        // Without retained internal ranges, trim must restructure the tree.
        assert!(rv.changed, "trim should have changed the tree");
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
    } else {
        // With retained internal ranges (max_depth_range > 0), the internal
        // node still has surviving entries in its range, which blocks
        // `remove_empty_children` from restructuring.
        assert!(
            !rv.changed,
            "trim should not restructure when internal ranges have surviving docs"
        );
    }

    // num_entries should be unchanged by trim.
    assert_eq!(tree.num_entries(), SPLIT_TRIGGER as usize);
}

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
    let result = tree
        .apply_gc_to_node(
            tree.root_index(),
            NodeGcDelta {
                delta: delta.delta,
                registers_with_last_block: delta.registers_with_last_block,
                registers_without_last_block: delta.registers_without_last_block,
            },
        )
        .unwrap();
    assert_eq!(
        result.index_gc_info.entries_removed, 0,
        "applying GC to a node without a range should be a no-op"
    );

    // Also apply the original delta to the correct leaf to verify it works.
    let leaf_delta = scan_node_delta(&tree, leaf_node_idx, &|doc_id| doc_id > 5);
    let d = leaf_delta.expect("leaf should still have GC work");
    let leaf_result = tree.apply_gc_to_node(leaf_node_idx, d).unwrap();
    assert!(
        leaf_result.index_gc_info.entries_removed > 0,
        "applying GC to a leaf with deleted docs should remove entries"
    );
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
#[cfg_attr(miri, ignore = "Too slow to run under miri")]
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
        tree.is_sparse(),
        "empty_leaves ({}) should be >= half of num_leaves ({})",
        tree.empty_leaves(),
        tree.num_leaves()
    );

    // compact_if_sparse will: (1) _trim_empty_leaves → frees left-subtree nodes
    // at low slab indices, (2) compact_slab → moves surviving right-subtree nodes
    // down to fill gaps.
    let freed = tree.compact_if_sparse();
    assert!(
        freed.inverted_index_size_delta < 0,
        "compaction should free memory"
    );
    assert!(
        freed.node_size_delta < 0,
        "compaction should remove empty slots"
    );
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
#[cfg_attr(miri, ignore = "Skip miri because too slow")]
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
#[cfg_attr(miri, ignore = "Skip miri because too slow")]
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

// ============================================================================
// SingleNodeGcResult field coverage
// ============================================================================

#[rstest]
fn apply_gc_tracks_ignored_last_block(#[values(false, true)] compress_floats: bool) {
    // Build a tree where the last block is NOT full, scan a delta that
    // includes the last block, then add entries to that same block.
    // This changes last_block_num_entries, triggering ignored_last_block.
    let partial = ENTRIES_PER_BLOCK / 2;
    let n = ENTRIES_PER_BLOCK + partial;
    let mut tree = NumericRangeTree::new(compress_floats);
    for i in 1..=n {
        tree.add(i, 42.0, false, 0);
    }
    assert!(tree.root().is_leaf());
    // Block 0: full (ENTRIES_PER_BLOCK entries)
    // Block 1: half-full (partial entries)

    // Delete odd doc_ids from both blocks → delta covers both blocks.
    let delta = scan_node_delta(&tree, tree.root_index(), &|doc_id| doc_id % 2 == 0)
        .expect("should have GC work");

    // Add one entry after scan → goes to block 1 (still not full),
    // changing its num_entries vs scan time.
    tree.add(n + 1, 42.0, false, 0);

    let result = tree.apply_gc_to_node(tree.root_index(), delta).unwrap();

    assert!(
        result.index_gc_info.ignored_last_block,
        "last block should be ignored when its num_entries changed after scan"
    );
    // Block 0's deltas should still be applied.
    assert!(result.index_gc_info.entries_removed > 0);
}

#[rstest]
fn apply_gc_ignored_last_block_no_delta(#[values(false, true)] compress_floats: bool) {
    // Build a tree with 2 blocks where:
    // - Block 0 (full) has entries to delete (gets a delta)
    // - Block 1 (partially full) has NO deleted entries (no delta)
    // Then add entries to block 1 after the scan (simulating parent writes
    // post-fork). Block 1 must be partially full so post-fork entries land
    // in it rather than in a new block.
    //
    // This exercises the path where last_block_changed is true but no delta
    // exists for the last block — ignored_last_block must still be set, and
    // HLL cardinality must include the post-fork entries.
    let partial = ENTRIES_PER_BLOCK / 2;
    let n = ENTRIES_PER_BLOCK + partial;
    let mut tree = NumericRangeTree::new(compress_floats);
    for i in 1..=n {
        tree.add(i, 42.0, false, 0);
    }
    assert!(tree.root().is_leaf());
    // Block 0: ENTRIES_PER_BLOCK entries (doc IDs 1..=ENTRIES_PER_BLOCK) — full
    // Block 1: partial entries (doc IDs ENTRIES_PER_BLOCK+1..=n) — not full

    // Delete only entries in block 0 — block 1 has no deleted entries, so no
    // BlockGcScanResult is produced for it.
    let delta = scan_node_delta(&tree, tree.root_index(), &|doc_id| {
        doc_id > ENTRIES_PER_BLOCK
    })
    .expect("should have GC work");
    // Verify the delta only covers block 0.
    assert_eq!(
        delta.delta.last_block_idx(),
        1,
        "there should be 2 blocks (last index = 1)"
    );

    // Simulate parent writes after fork: add entries to block 1 (not full).
    // Use a different value (99.0) so cardinality should increase.
    tree.add(n + 1, 99.0, false, 0);

    let result = tree.apply_gc_to_node(tree.root_index(), delta).unwrap();

    assert!(
        result.index_gc_info.ignored_last_block,
        "last block should be ignored when its num_entries changed after scan, \
         even without a delta for the last block"
    );
    assert!(result.index_gc_info.entries_removed > 0);

    // Cardinality must reflect post-fork entries (value 99.0 is new).
    // With only value 42.0 surviving from block 1 plus value 99.0 from post-fork,
    // cardinality should be at least 2.
    let cardinality_after = tree.root().range().unwrap().cardinality();
    assert!(
        cardinality_after >= 2,
        "cardinality should include post-fork entries, got {cardinality_after}"
    );
}

#[test]
fn gc_on_empty_tree_is_noop() {
    let tree = NumericRangeTree::new(false);
    assert_eq!(tree.num_entries(), 0);

    let deltas = scan_all_node_deltas(&tree, &|_| true);
    assert!(deltas.is_empty(), "empty tree should have no GC work");
}

#[rstest]
fn compact_if_sparse_below_threshold_is_noop(#[values(false, true)] compress_floats: bool) {
    // Build a tree with multiple leaves, then GC just one leaf empty — not
    // enough to exceed the 50% sparse threshold.
    let n = SPLIT_TRIGGER * 2;
    let mut tree = build_tree(n, compress_floats, 0);
    let num_leaves = tree.num_leaves();
    assert!(num_leaves > 1);

    // Delete only a few docs from one leaf. With sequential values, deleting
    // doc_ids 1..=2 empties at most a small portion of the leftmost leaf.
    gc_all_ranges(&mut tree, &|doc_id| doc_id > 2);

    // Empty leaves should be less than half the total.
    assert!(
        !tree.is_sparse(),
        "only 2 deleted docs should not produce enough empty leaves: {} < {}",
        tree.empty_leaves(),
        tree.num_leaves() / 2
    );

    let result = tree.compact_if_sparse();
    assert_eq!(result.inverted_index_size_delta, 0);
    assert_eq!(result.node_size_delta, 0);
}
