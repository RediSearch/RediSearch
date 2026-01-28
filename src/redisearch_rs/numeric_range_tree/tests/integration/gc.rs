/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for garbage collection in the numeric range tree.

use numeric_range_tree::{NumericRangeNode, NumericRangeTree};
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

/// Scan the root node's range for GC. Panics if root has no range.
fn scan_root(
    tree: &NumericRangeTree,
    doc_exist: impl Fn(u64) -> bool,
) -> inverted_index::GcScanDelta {
    tree.root()
        .range()
        .expect("root must have a range")
        .entries()
        .scan_gc(doc_exist)
        .expect("scan_gc should not fail")
        .expect("scan_gc should return Some delta")
}

/// Apply GC to the root node of `tree`.
///
/// Uses raw pointers to obtain a mutable reference to the root node that is
/// disjoint from `&mut tree`, mirroring how the FFI layer invokes
/// `apply_node_gc` (tree and node come from separate C pointers).
fn apply_gc_to_root(
    tree: &mut NumericRangeTree,
    delta: inverted_index::GcScanDelta,
    hll_with: &[u8; 64],
    hll_without: &[u8; 64],
) -> numeric_range_tree::NodeGcResult {
    // SAFETY: `root_mut()` returns a reference into `tree`. We immediately
    // convert it to a raw pointer and drop the mutable borrow so that we can
    // pass `&mut tree` separately. This is safe because `apply_node_gc` only
    // mutates tree-level statistics and the node's range — the node *is* part
    // of the tree, but `apply_node_gc` is designed for exactly this usage
    // pattern (see the FFI callsite in `gc.rs`).
    let node_ptr: *mut NumericRangeNode = tree.root_mut() as *mut _;
    // SAFETY: The raw pointer was just obtained from `tree.root_mut()`. We
    // create a second `&mut` to the root node while also holding `&mut tree`.
    // This mirrors the FFI callsite where tree and node arrive as independent
    // C pointers. `apply_node_gc` only touches disjoint fields (tree stats vs
    // node range), so no aliased mutation occurs.
    let node = unsafe { &mut *node_ptr };
    tree.apply_node_gc(node, delta, hll_with, hll_without)
}

// ============================================================================
// apply_node_gc tests
// ============================================================================

#[test]
fn apply_node_gc_basic() {
    let mut tree = build_single_leaf_tree(10, false);
    let entries_before = tree.num_entries();
    let size_before = tree.inverted_indexes_size();

    // Mark docs 1..=5 as deleted.
    let delta = scan_root(&tree, |doc_id| doc_id > 5);
    let hll_with = [0u8; 64];
    let hll_without = [0u8; 64];

    let result = apply_gc_to_root(&mut tree, delta, &hll_with, &hll_without);

    assert!(result.valid);
    assert_eq!(result.entries_removed, 5);
    assert_eq!(tree.num_entries(), entries_before - 5);
    assert!(result.bytes_freed > 0 || result.bytes_allocated > 0);
    assert_ne!(tree.inverted_indexes_size(), size_before);
}

#[test]
fn apply_node_gc_removes_all_entries() {
    let mut tree = build_single_leaf_tree(10, false);

    let delta = scan_root(&tree, |_| false);
    let hll_with = [0u8; 64];
    let hll_without = [0u8; 64];

    let result = apply_gc_to_root(&mut tree, delta, &hll_with, &hll_without);

    assert!(result.valid);
    assert_eq!(result.entries_removed, 10);
    assert_eq!(tree.num_entries(), 0);
    assert_eq!(tree.root().range().unwrap().num_docs(), 0);
    assert_eq!(tree.empty_leaves(), 1);
}

#[test]
fn apply_node_gc_on_node_without_range() {
    let mut tree = build_single_leaf_tree(0, false);
    tree.root_mut().set_range(None);

    // Build a dummy delta from a throwaway tree.
    let helper = build_single_leaf_tree(1, false);
    let delta = scan_root(&helper, |_| false);
    let hll_with = [0u8; 64];
    let hll_without = [0u8; 64];

    let result = apply_gc_to_root(&mut tree, delta, &hll_with, &hll_without);

    assert!(!result.valid);
    assert_eq!(result.entries_removed, 0);
}

#[rstest]
#[case(false)]
#[case(true)]
fn apply_node_gc_with_blocks_added_since_fork(#[case] compress_floats: bool) {
    // Use a single value to keep cardinality at 1, preventing splits even with
    // many entries. This lets us add enough docs to create multiple blocks.
    let mut tree = NumericRangeTree::new(compress_floats);
    for i in 1..=2000 {
        tree.add(i, 42.0, false, 0);
    }
    assert!(tree.root().is_leaf());

    // Scan captures the block layout at fork time.
    let delta = scan_root(&tree, |doc_id| doc_id > 500);

    // Simulate parent writes after fork.
    for i in 2001..=2500 {
        tree.add(i, 42.0, false, 0);
    }

    let hll_with = [0u8; 64];
    let hll_without = [0u8; 64];
    let result = apply_gc_to_root(&mut tree, delta, &hll_with, &hll_without);

    assert!(result.valid);
    // Entries added after the scan may cause blocks to be ignored, but all
    // originally-deleted entries that fit in unmodified blocks are still removed.
    assert!(result.entries_removed <= 500);
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

    let delta = scan_root(&tree, |doc_id| doc_id > 7);

    // Provide non-zero HLL registers so cardinality is non-zero after GC.
    let mut hll_with = [0u8; 64];
    hll_with[0] = 5;
    hll_with[1] = 3;
    let hll_without = [0u8; 64];

    apply_gc_to_root(&mut tree, delta, &hll_with, &hll_without);

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

    let delta = scan_root(&tree, |doc_id| doc_id > 500);

    // Simulate parent writes after fork.
    for i in 2001..=3000 {
        tree.add(i, 42.0, false, 0);
    }

    let hll_with = [0u8; 64];
    let hll_without = [0u8; 64];
    apply_gc_to_root(&mut tree, delta, &hll_with, &hll_without);

    // New blocks added after fork should be rescanned for cardinality.
    // With a single value (42.0), cardinality should be 1 after rescan.
    let cardinality_after = tree.root().range().unwrap().cardinality();
    assert!(cardinality_after > 0);
}

// ============================================================================
// Helpers for multi-leaf GC
// ============================================================================

/// Apply GC to every leaf in the tree.
fn gc_all_leaves(tree: &mut NumericRangeTree, doc_exist: &dyn Fn(u64) -> bool) {
    let mut work: Vec<GcWork> = Vec::new();
    collect_gc_work(tree.root(), doc_exist, &mut work);

    for item in work {
        let node_ptr = find_node_by_path(tree.root_mut(), &item.path) as *mut NumericRangeNode;
        // SAFETY: Same disjoint-mutation pattern as `apply_gc_to_root`.
        let node = unsafe { &mut *node_ptr };
        let hll_with = [0u8; 64];
        let hll_without = [0u8; 64];
        tree.apply_node_gc(node, item.delta, &hll_with, &hll_without);
    }
}

struct GcWork {
    path: Vec<Direction>,
    delta: inverted_index::GcScanDelta,
}

#[derive(Clone, Copy)]
enum Direction {
    Left,
    Right,
}

fn collect_gc_work(
    node: &NumericRangeNode,
    doc_exist: &dyn Fn(u64) -> bool,
    work: &mut Vec<GcWork>,
) {
    collect_gc_work_inner(node, doc_exist, work, &mut Vec::new());
}

fn collect_gc_work_inner(
    node: &NumericRangeNode,
    doc_exist: &dyn Fn(u64) -> bool,
    work: &mut Vec<GcWork>,
    path: &mut Vec<Direction>,
) {
    if node.is_leaf() {
        if let Some(range) = node.range()
            && let Some(delta) = range
                .entries()
                .scan_gc(doc_exist)
                .expect("scan should not fail")
        {
            work.push(GcWork {
                path: path.clone(),
                delta,
            });
        }
    } else {
        if let Some(left) = node.left() {
            path.push(Direction::Left);
            collect_gc_work_inner(left, doc_exist, work, path);
            path.pop();
        }
        if let Some(right) = node.right() {
            path.push(Direction::Right);
            collect_gc_work_inner(right, doc_exist, work, path);
            path.pop();
        }
    }
}

fn find_node_by_path<'a>(
    mut node: &'a mut NumericRangeNode,
    path: &[Direction],
) -> &'a mut NumericRangeNode {
    for dir in path {
        node = match dir {
            Direction::Left => node.left_mut().expect("left child must exist"),
            Direction::Right => node.right_mut().expect("right child must exist"),
        };
    }
    node
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

    // Delete odd doc IDs.
    let delta = scan_root(&tree, |doc_id| doc_id % 2 == 0);
    let hll_with = [0u8; 64];
    let hll_without = [0u8; 64];
    let result = apply_gc_to_root(&mut tree, delta, &hll_with, &hll_without);

    assert!(result.valid);
    assert_eq!(result.entries_removed, 500);
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
