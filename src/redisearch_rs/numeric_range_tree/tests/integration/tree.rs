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

use numeric_range_tree::test_utils::{SPLIT_TRIGGER, build_tree, gc_all_ranges, walk_with_depth};

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

/// The compression route into the MOD-16877 `empty_leaves` underflow crash (a
/// production crash on 8.6.6), now closed at the source.
///
/// It used to run: cardinality was estimated over the original f64 values while
/// entries were stored — and split-redistributed — as compressed f32. Enough
/// distinct f64s collapsing onto one f32 looked like a high-cardinality leaf, so a
/// split fired even though every stored value was identical; all entries went to
/// one child, the other was created empty and uncounted, and the next add routed
/// into it decremented `empty_leaves` below zero, aborting the process across the
/// non-unwinding FFI boundary.
///
/// Values are quantized on the way in now, so the collapsing values *are* one value
/// to the tree: cardinality stays at 1 and no split fires at all. The
/// `empty_leaves` accounting in `split_node` survives as a guard, but nothing in
/// this scenario reaches it.
#[test]
fn test_compression_collapse_does_not_inflate_cardinality() {
    let mut tree = NumericRangeTree::new(true); // float compression ON

    // Distinct f64 values tightly clustered within a single f32 rounding bucket
    // around 100.5 (exactly representable in f32). Each differs from the next by
    // 1e-7 — far above the f64 ULP near 100.5 (~1e-14), so they were distinct to
    // the HLL cardinality estimator, but far below the f32 ULP near 100.5
    // (~7.6e-6), so all round to the same stored f32 (100.5), well within the
    // 0.01 compression threshold.
    let value_at = |i: u64| 100.5 + (i - 1) as f64 * 1e-7;

    // Well past the point where the old cardinality estimate would have split.
    for i in 1..=(SPLIT_TRIGGER + 8) {
        tree.add(i, value_at(i), false, 0);
    }

    assert!(
        tree.root().is_leaf(),
        "collapsing values are one stored value, so no split should fire (got {} leaves)",
        tree.num_leaves()
    );

    let range = tree.root().range().unwrap();
    assert_eq!(
        range.cardinality(),
        1,
        "cardinality must count stored values, not the inputs that collapsed onto them"
    );
    assert_eq!(
        (range.min_val(), range.max_val()),
        (100.5, 100.5),
        "bounds must describe the stored value a reader will decode"
    );
    assert_eq!(tree.empty_leaves(), 0);

    // The add that used to hit the underflow. Nothing was ever stranded, so the
    // counter is not touched.
    let next = SPLIT_TRIGGER + 9;
    tree.add(next, value_at(next), false, 0);
    assert_eq!(tree.empty_leaves(), 0);
}

/// The compression-free route into the MOD-16877 `empty_leaves` underflow, now
/// closed at the source.
///
/// It used to run: GC removed entries without raising `min_val`, so a leaf whose
/// smallest-valued documents had all been collected kept reporting the old minimum.
/// `split_node`'s `split == min_val` guard then compared the median of the
/// *surviving* entries against a bound no survivor had, so it did not fire — and
/// with a majority of the survivors sharing the smallest surviving value, that value
/// became the split point, sending every entry to the right child and stranding the
/// left one empty.
///
/// GC now recomputes a leaf's bounds over the surviving entries, so the median
/// equals `min_val` again, the guard fires, and the split lands entries on both
/// sides. Its sibling `test_compression_collapse_does_not_inflate_cardinality`
/// covers the other route; both now pin the cause, not the symptom.
#[test]
fn test_split_after_gc_does_not_strand_empty_leaf() {
    let mut tree = NumericRangeTree::new(false); // float compression OFF

    /// Number of documents sharing the value 100.0. They must remain a majority of
    /// the leaf's entries once the split fires, so that the median lands on 100.0:
    /// the loop below adds at most `SPLIT_TRIGGER` further entries, and
    /// `MAJORITY > SPLIT_TRIGGER` keeps the median index within the block.
    const MAJORITY: u64 = 40;

    // Doc 1 is the only document below 100.0, so it alone sets `min_val` to 0.0.
    tree.add(1, 0.0, false, 0);

    for doc_id in 2..=(MAJORITY + 1) {
        tree.add(doc_id, 100.0, false, 0);
    }
    assert!(tree.root().is_leaf());
    assert_eq!(tree.empty_leaves(), 0, "the root leaf holds every document");

    // Delete doc 1 and collect it. Its entry is physically removed and both the HLL
    // and the bounds are recomputed over the survivors.
    gc_all_ranges(&mut tree, &|doc_id| doc_id != 1);
    assert_eq!(tree.num_entries(), MAJORITY as usize);
    assert_eq!(
        tree.empty_leaves(),
        0,
        "the leaf still holds the surviving documents"
    );
    assert_eq!(
        tree.root().range().unwrap().min_val(),
        100.0,
        "GC must raise the lower bound to the smallest surviving value"
    );

    // Push cardinality over the split threshold using distinct values strictly
    // greater than 100.0, so 100.0 stays the smallest surviving value.
    let mut doc_id = MAJORITY + 2;
    let mut split_fired = false;
    for i in 1..=SPLIT_TRIGGER {
        tree.add(doc_id, 100.0 + i as f64, false, 0);
        doc_id += 1;
        if tree.num_leaves() == 2 {
            split_fired = true;
            break;
        }
    }
    assert!(split_fired, "distinct values should trigger a split");

    // The median now equals the (tightened) `min_val`, so the split point is
    // `next_up(100.0)`: the block of 100.0s goes left, everything above goes right.
    assert_eq!(tree.root().split_value(), Some(100.0f64.next_up()));
    let (left_idx, right_idx) = tree.root().child_indices().unwrap();
    for (label, idx) in [("left", left_idx), ("right", right_idx)] {
        assert!(
            tree.node(idx).range().unwrap().num_docs() > 0,
            "the {label} child should have received entries"
        );
    }
    assert_eq!(
        tree.empty_leaves(),
        0,
        "the split should not strand an empty leaf at all"
    );

    // The add that used to hit the underflow: a value below the split point. It is
    // routed to a populated leaf now, so the counter is never decremented.
    tree.add(doc_id, 50.0, false, 0);
    assert_eq!(tree.empty_leaves(), 0);
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
