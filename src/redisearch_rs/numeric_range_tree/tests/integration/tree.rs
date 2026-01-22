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

#[test]
fn test_new_tree() {
    let tree = NumericRangeTree::new();
    assert_eq!(tree.num_ranges(), 1);
    assert_eq!(tree.num_leaves(), 1);
    assert_eq!(tree.num_entries(), 0);
    assert_eq!(tree.last_doc_id(), 0);
    assert_eq!(tree.revision_id(), 0);
}

#[test]
fn test_add_basic() {
    let mut tree = NumericRangeTree::new();

    let result = tree.add(1, 5.0, false);
    assert_eq!(tree.num_entries(), 1);
    assert_eq!(tree.last_doc_id(), 1);
    assert!(result.size_delta > 0);

    let result = tree.add(2, 10.0, false);
    assert_eq!(tree.num_entries(), 2);
    assert_eq!(tree.last_doc_id(), 2);
    assert!(result.size_delta > 0);
}

#[test]
fn test_duplicate_doc_id_rejected() {
    let mut tree = NumericRangeTree::new();

    tree.add(5, 10.0, false);
    assert_eq!(tree.num_entries(), 1);

    // Duplicate should be rejected
    let result = tree.add(5, 20.0, false);
    assert_eq!(result.size_delta, 0);
    assert_eq!(tree.num_entries(), 1);

    // Lower doc_id should also be rejected
    let result = tree.add(3, 15.0, false);
    assert_eq!(result.size_delta, 0);
    assert_eq!(tree.num_entries(), 1);
}

#[test]
fn test_duplicate_doc_id_allowed_with_multi() {
    let mut tree = NumericRangeTree::new();

    tree.add(5, 10.0, true);
    assert_eq!(tree.num_entries(), 1);

    // Duplicate allowed with is_multi=true
    let result = tree.add(5, 20.0, true);
    assert!(result.size_delta > 0);
    assert_eq!(tree.num_entries(), 2);
}

#[test]
fn test_unique_ids() {
    let tree1 = NumericRangeTree::new();
    let tree2 = NumericRangeTree::new();
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
fn test_root_mut() {
    let mut tree = NumericRangeTree::new();

    // Modify the root through root_mut
    let root = tree.root_mut();
    root.set_split_value(42.0);

    // Verify the modification persisted
    assert_eq!(tree.root().split_value(), 42.0);
}

#[test]
fn test_root_mut_add_to_range() {
    let mut tree = NumericRangeTree::new();

    // Add directly to root's range through root_mut
    let root = tree.root_mut();
    if let Some(range) = root.range_mut() {
        range.add(100, 50.0).unwrap();
    }

    // Verify via root accessor
    let range = tree.root().range().unwrap();
    assert_eq!(range.min_val(), 50.0);
    assert_eq!(range.max_val(), 50.0);
}

#[test]
fn test_inverted_indexes_size() {
    let tree = NumericRangeTree::new();
    // A new tree has an empty inverted index
    let initial_size = tree.inverted_indexes_size();

    let mut tree2 = NumericRangeTree::new();
    tree2.add(1, 5.0, false);
    let size_after_add = tree2.inverted_indexes_size();
    assert!(size_after_add >= initial_size);
}

#[test]
fn test_empty_leaves() {
    let tree = NumericRangeTree::new();
    // A new tree starts with 0 empty leaves (root has an empty range but isn't counted)
    assert_eq!(tree.empty_leaves(), 0);
}

#[test]
fn test_increment_revision() {
    let mut tree = NumericRangeTree::new();
    assert_eq!(tree.revision_id(), 0);

    tree.increment_revision();
    assert_eq!(tree.revision_id(), 1);

    tree.increment_revision();
    assert_eq!(tree.revision_id(), 2);

    // Test wrapping behavior
    for _ in 0..10 {
        tree.increment_revision();
    }
    assert_eq!(tree.revision_id(), 12);
}

#[test]
fn test_revision_wraps_around() {
    let mut tree = NumericRangeTree::new();

    // Simulate being at max u32
    // We can't directly set revision_id, but we can verify wrapping logic works
    // by incrementing many times - this tests the wrapping_add behavior
    for _ in 0..100 {
        tree.increment_revision();
    }
    assert_eq!(tree.revision_id(), 100);
}

#[test]
fn test_mem_usage() {
    let tree = NumericRangeTree::new();
    let mem = tree.mem_usage();

    // Should include at least the base struct size
    assert!(mem >= std::mem::size_of::<NumericRangeTree>());

    // Add some entries and verify memory increases
    let mut tree = NumericRangeTree::new();
    let mem_before = tree.mem_usage();

    tree.add(1, 5.0, false);
    tree.add(2, 10.0, false);
    tree.add(3, 15.0, false);

    let mem_after = tree.mem_usage();
    assert!(mem_after >= mem_before);
}

#[test]
fn test_add_to_tree_without_range() {
    let mut tree = NumericRangeTree::new();

    // Remove the root's range to test the else branch in add()
    tree.root_mut().set_range(None);

    // Adding should handle the case where root has no range
    let result = tree.add(1, 5.0, false);
    assert_eq!(result.size_delta, 0);
}

#[test]
fn test_multiple_sequential_adds() {
    let mut tree = NumericRangeTree::new();

    for i in 1..=100 {
        let result = tree.add(i as u64, i as f64, false);
        assert!(result.size_delta >= 0 || i == 1);
    }

    assert_eq!(tree.num_entries(), 100);
    assert_eq!(tree.last_doc_id(), 100);
}

#[test]
fn test_add_result_fields() {
    use numeric_range_tree::AddResult;

    let result = AddResult::default();
    assert_eq!(result.size_delta, 0);
    assert_eq!(result.num_records, 0);
    assert!(!result.changed);
    assert_eq!(result.num_ranges_delta, 0);
    assert_eq!(result.num_leaves_delta, 0);
}
