/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for NumericRangeTree.

use numeric_range_tree::NumericRangeTree;
use rstest::rstest;

use numeric_range_tree::test_utils::{SPLIT_TRIGGER, build_tree, walk_with_depth};

#[test]
fn test_new_tree() {
    let tree = NumericRangeTree::new(false);
    assert_eq!(tree.num_ranges(), 1);
    assert_eq!(tree.num_leaves(), 1);
    assert_eq!(tree.num_entries(), 0);
    assert_eq!(tree.last_doc_id(), 0);
    assert_eq!(tree.revision_id(), 0);
}

#[test]
fn test_add_basic() {
    let mut tree = NumericRangeTree::new(false);

    let result = tree.add(1, 5.0, false, 0);
    assert_eq!(tree.num_entries(), 1);
    assert_eq!(tree.last_doc_id(), 1);
    assert!(result.size_delta > 0);

    let result = tree.add(2, 10.0, false, 0);
    assert_eq!(tree.num_entries(), 2);
    assert_eq!(tree.last_doc_id(), 2);
    assert!(result.size_delta > 0);
}

#[test]
fn test_duplicate_doc_id_rejected() {
    let mut tree = NumericRangeTree::new(false);

    tree.add(5, 10.0, false, 0);
    assert_eq!(tree.num_entries(), 1);

    // Duplicate should be rejected
    let result = tree.add(5, 20.0, false, 0);
    assert_eq!(result.size_delta, 0);
    assert_eq!(tree.num_entries(), 1);

    // Lower doc_id should also be rejected
    let result = tree.add(3, 15.0, false, 0);
    assert_eq!(result.size_delta, 0);
    assert_eq!(tree.num_entries(), 1);
}

#[test]
fn test_duplicate_doc_id_allowed_with_multi() {
    let mut tree = NumericRangeTree::new(false);

    tree.add(5, 10.0, true, 0);
    assert_eq!(tree.num_entries(), 1);

    // Duplicate allowed with is_multi=true
    let result = tree.add(5, 20.0, true, 0);
    assert!(result.size_delta > 0);
    assert_eq!(tree.num_entries(), 2);
}

#[test]
fn test_unique_ids() {
    let tree1 = NumericRangeTree::new(false);
    let tree2 = NumericRangeTree::new(false);
    assert_ne!(tree1.unique_id(), tree2.unique_id());
}

#[test]
fn test_default_impl() {
    let tree: NumericRangeTree = Default::default();
    assert_eq!(tree.num_ranges(), 1);
    assert_eq!(tree.num_leaves(), 1);
    assert_eq!(tree.num_entries(), 0);
    assert_eq!(tree.last_doc_id(), 0);
    assert_eq!(tree.revision_id(), 0);
}

#[test]
fn test_inverted_indexes_size() {
    let tree = NumericRangeTree::new(false);
    // A new tree has an empty inverted index
    let initial_size = tree.inverted_indexes_size();

    let mut tree2 = NumericRangeTree::new(false);
    tree2.add(1, 5.0, false, 0);
    let size_after_add = tree2.inverted_indexes_size();
    assert!(size_after_add > initial_size);
}

#[test]
fn test_empty_leaves() {
    let tree = NumericRangeTree::new(false);
    // A new tree starts with 1 empty leaf, the root
    assert_eq!(tree.empty_leaves(), 1);
}

#[test]
fn test_increment_revision() {
    let mut tree = NumericRangeTree::new(false);
    assert_eq!(tree.revision_id(), 0);

    tree.increment_revision();
    assert_eq!(tree.revision_id(), 1);

    tree.increment_revision();
    assert_eq!(tree.revision_id(), 2);
}

#[test]
fn test_mem_usage() {
    let tree = NumericRangeTree::new(false);
    let mem = tree.mem_usage();

    // Should include at least the base struct size
    assert!(mem >= std::mem::size_of::<NumericRangeTree>());

    // Add some entries and verify memory increases
    let mut tree = NumericRangeTree::new(false);
    let mem_before = tree.mem_usage();

    tree.add(1, 5.0, false, 0);
    tree.add(2, 10.0, false, 0);
    tree.add(3, 15.0, false, 0);

    let mem_after = tree.mem_usage();
    assert!(mem_after > mem_before);
}

#[test]
fn test_multiple_sequential_adds() {
    let mut tree = NumericRangeTree::new(false);

    for i in 1..=100 {
        let result = tree.add(i as u64, i as f64, false, 0);
        assert!(result.size_delta >= 0);
    }

    assert_eq!(tree.num_entries(), 100);
    assert_eq!(tree.last_doc_id(), 100);
}

#[test]
fn test_add_result_fields() {
    use numeric_range_tree::AddResult;

    let result = AddResult::default();
    assert_eq!(result.size_delta, 0);
    assert_eq!(result.num_records_delta, 0);
    assert!(!result.changed);
    assert_eq!(result.num_ranges_delta, 0);
    assert_eq!(result.num_leaves_delta, 0);
}

// ============================================================================
// Splitting and balancing tests
// ============================================================================

#[rstest]
fn test_split_triggers_at_cardinality_threshold(#[values(false, true)] compress_floats: bool) {
    // Insert enough distinct values to reliably exceed the depth-0 split
    // threshold, with margin for HLL estimation error (~13%).
    let tree = build_tree(SPLIT_TRIGGER, compress_floats, 0);

    // After enough distinct values, the tree should have split.
    assert!(
        tree.num_leaves() > 1,
        "tree should have split, but num_leaves = {}",
        tree.num_leaves()
    );
    assert!(tree.num_ranges() > 1);
    assert!(!tree.root().is_leaf());
}

#[test]
fn test_split_with_identical_values() {
    let mut tree = NumericRangeTree::new(false);

    // Insert many entries with the same value. Cardinality stays at 1,
    // so the size-overflow path (MAXIMUM_RANGE_SIZE) with card > 1 won't
    // trigger. The tree should remain a single leaf.
    for i in 1..=500u64 {
        tree.add(i, 42.0, false, 0);
    }

    assert_eq!(tree.num_entries(), 500);
    assert!(
        tree.root().is_leaf(),
        "identical values should not cause a split (cardinality = 1)"
    );
}

#[test]
#[cfg_attr(miri, ignore = "Too slow to run under miri")]
fn test_deep_tree_balancing() {
    let mut tree = NumericRangeTree::new(false);

    // Insert sorted increasing values to create depth imbalance.
    // The balancing logic (AVL rotations) should keep the tree bounded.
    // The depth imbalance invariant in `check_tree_invariants` (which runs
    // after every `add`) enforces the real bound.
    for i in 1..=5000u64 {
        tree.add(i, i as f64, false, 0);
    }
}

#[test]
#[cfg_attr(miri, ignore = "Too slow to run under miri")]
fn test_deep_tree_balancing_descending() {
    let mut tree = NumericRangeTree::new(false);

    // Insert sorted decreasing values to create right-to-left imbalance.
    // This triggers right rotations via `balance_node`, covering
    // `rotate_right` and the left-heavy branch in `balance_node`.
    // The depth imbalance invariant in `check_tree_invariants` (which runs
    // after every `add`) enforces the real bound.
    for i in (1..=5000u64).rev() {
        tree.add(5001 - i, i as f64, true, 0);
    }
}

#[test]
#[cfg_attr(miri, ignore = "Too slow to run under miri")]
fn test_deep_tree_balancing_mixed() {
    let mut tree = NumericRangeTree::new(false);

    // Insert values in alternating ascending/descending batches to exercise
    // both left and right rotations within a single tree.
    // The depth imbalance invariant in `check_tree_invariants` (which runs
    // after every `add`) enforces the real bound.
    let mut doc_id = 1u64;
    for batch in 0..10 {
        if batch % 2 == 0 {
            // Ascending batch
            for v in (batch * 500 + 1)..=(batch * 500 + 500) {
                tree.add(doc_id, v as f64, true, 0);
                doc_id += 1;
            }
        } else {
            // Descending batch
            for v in ((batch * 500 + 1)..=(batch * 500 + 500)).rev() {
                tree.add(doc_id, v as f64, true, 0);
                doc_id += 1;
            }
        }
    }
}

#[test]
fn test_max_depth_range_removes_inner_ranges() {
    // With max_depth_range = 0, only leaf nodes should retain ranges.
    // Internal nodes at depth > 0 should have their ranges removed.
    let tree = build_tree(100, false, 0);

    // Verify the tree has split.
    assert!(tree.num_leaves() > 1);

    // Internal nodes above max_depth_range=0 should not have ranges.
    // The root (if internal) should have no range because
    // max_depth > max_depth_range (0).
    if !tree.root().is_leaf() {
        assert!(
            tree.root().range().is_none(),
            "root internal node should not retain range with max_depth_range=0"
        );
    }
}

// ============================================================================
// max_depth_range > 0 tests
// ============================================================================

#[rstest]
fn test_max_depth_range_retains_internal_ranges(#[values(false, true)] compress_floats: bool) {
    // Insert 200 entries with max_depth_range=2 so internal nodes retain ranges.
    let tree = build_tree(200, compress_floats, 2);

    // Internal nodes should also have ranges, so num_ranges > num_leaves.
    assert!(
        tree.num_ranges() > tree.num_leaves(),
        "with max_depth_range=2, internal nodes should retain ranges: num_ranges={}, num_leaves={}",
        tree.num_ranges(),
        tree.num_leaves()
    );

    // Walk the tree: internal nodes at depth <= 2 should have ranges.
    walk_with_depth(&tree, &mut |node, depth| {
        if !node.is_leaf() && depth <= 2 && node.max_depth() <= 2 {
            assert!(
                node.range().is_some(),
                "internal node at depth {depth} with max_depth {} should retain range",
                node.max_depth()
            );
        }
    });
}

#[rstest]
#[cfg_attr(miri, ignore = "Too slow to run under miri")]
fn test_max_depth_range_removes_deep_ranges(#[values(false, true)] compress_floats: bool) {
    // Insert 5000 entries with max_depth_range=1.
    let tree = build_tree(5000, compress_floats, 1);

    // Walk the tree: nodes at depth > 1 should NOT have ranges
    // (only if they are internal nodes whose max_depth > 1).
    walk_with_depth(&tree, &mut |node, _depth| {
        if !node.is_leaf() && node.max_depth() > 1 {
            assert!(
                node.range().is_none(),
                "internal node with max_depth {} should not retain range with max_depth_range=1",
                node.max_depth()
            );
        }
    });
}
