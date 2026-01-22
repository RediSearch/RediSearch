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
