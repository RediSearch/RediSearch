/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use inverted_index::NumericFilter;
use numeric_range_tree::{NumericRange, NumericRangeNode, NumericRangeTree};

// =============================================================================
// NumericRange tests
// =============================================================================

#[test]
fn test_new_range() {
    let range = NumericRange::new();
    assert_eq!(range.min_val, f64::INFINITY);
    assert_eq!(range.max_val, f64::NEG_INFINITY);
    assert_eq!(range.num_entries(), 0);
    assert_eq!(range.num_docs(), 0);
}

#[test]
fn test_add_updates_bounds() {
    let mut range = NumericRange::new();
    range.add(1, 10.0).unwrap();
    assert_eq!(range.min_val, 10.0);
    assert_eq!(range.max_val, 10.0);

    range.add(2, 5.0).unwrap();
    assert_eq!(range.min_val, 5.0);
    assert_eq!(range.max_val, 10.0);

    range.add(3, 15.0).unwrap();
    assert_eq!(range.min_val, 5.0);
    assert_eq!(range.max_val, 15.0);
}

#[test]
fn test_cardinality_updates() {
    let mut range = NumericRange::new();
    range.update_cardinality(1.0);
    range.update_cardinality(2.0);
    range.update_cardinality(3.0);
    // Duplicate shouldn't affect cardinality much
    range.update_cardinality(1.0);

    let card = range.cardinality();
    // Should be approximately 3, allowing for HLL error
    assert!(card >= 1 && card <= 5, "cardinality={}", card);
}

#[test]
fn test_is_contained() {
    let mut range = NumericRange::new();
    range.add(1, 5.0).unwrap();
    range.add(2, 15.0).unwrap();

    // Range is [5, 15]
    assert!(range.is_contained(0.0, 20.0)); // Contains [0, 20]
    assert!(range.is_contained(5.0, 15.0)); // Exact bounds
    assert!(!range.is_contained(6.0, 20.0)); // Min too high
    assert!(!range.is_contained(0.0, 14.0)); // Max too low
}

#[test]
fn test_overlaps() {
    let mut range = NumericRange::new();
    range.add(1, 5.0).unwrap();
    range.add(2, 15.0).unwrap();

    // Range is [5, 15]
    assert!(range.overlaps(0.0, 20.0)); // Fully contains
    assert!(range.overlaps(10.0, 20.0)); // Partial overlap high
    assert!(range.overlaps(0.0, 10.0)); // Partial overlap low
    assert!(range.overlaps(5.0, 15.0)); // Exact bounds
    assert!(!range.overlaps(16.0, 20.0)); // No overlap high
    assert!(!range.overlaps(0.0, 4.0)); // No overlap low
}

// =============================================================================
// NumericRangeNode tests
// =============================================================================

#[test]
fn test_new_leaf() {
    let node = NumericRangeNode::new_leaf();
    assert!(node.is_leaf());
    assert!(node.range.is_some());
    assert_eq!(node.max_depth, 0);
}

#[test]
fn test_add_to_leaf() {
    let mut node = NumericRangeNode::new_leaf();
    let rv = node.add(1, 10.0, 0, 10).unwrap();
    assert!(rv.size_change > 0);
    assert_eq!(rv.num_records, 1);
    assert!(!rv.changed); // No split needed for single value

    let range = node.range.as_ref().unwrap();
    assert_eq!(range.num_entries(), 1);
}

// =============================================================================
// NumericRangeTree tests
// =============================================================================

#[test]
fn test_new_tree() {
    let tree = NumericRangeTree::new();
    assert_eq!(tree.num_ranges(), 1);
    assert_eq!(tree.num_leaves(), 1);
    assert_eq!(tree.num_entries(), 0);
}

#[test]
fn test_add_single_value() {
    let mut tree = NumericRangeTree::new();
    let rv = tree.add(1, 10.0, false).unwrap();

    assert!(rv.size_change > 0);
    assert_eq!(tree.num_entries(), 1);
    assert_eq!(tree.last_doc_id(), 1);
}

#[test]
fn test_add_rejects_duplicates() {
    let mut tree = NumericRangeTree::new();
    tree.add(1, 10.0, false).unwrap();
    let rv = tree.add(1, 20.0, false).unwrap();

    // Duplicate doc_id should be rejected
    assert_eq!(rv.size_change, 0);
    assert_eq!(tree.num_entries(), 1);
}

#[test]
fn test_add_allows_multi() {
    let mut tree = NumericRangeTree::new();
    tree.add(1, 10.0, true).unwrap();
    tree.add(1, 20.0, true).unwrap();

    // Multi-value should be allowed
    assert_eq!(tree.num_entries(), 2);
}

#[test]
fn test_find_basic() {
    let mut tree = NumericRangeTree::new();
    tree.add(1, 10.0, false).unwrap();
    tree.add(2, 20.0, false).unwrap();
    tree.add(3, 30.0, false).unwrap();

    let filter = NumericFilter {
        min: 15.0,
        max: 25.0,
        min_inclusive: true,
        max_inclusive: true,
        ..Default::default()
    };

    let ranges = tree.find(&filter);
    assert!(!ranges.is_empty());
}

#[test]
fn test_find_no_match() {
    let mut tree = NumericRangeTree::new();
    tree.add(1, 10.0, false).unwrap();
    tree.add(2, 20.0, false).unwrap();

    let filter = NumericFilter {
        min: 100.0,
        max: 200.0,
        min_inclusive: true,
        max_inclusive: true,
        ..Default::default()
    };

    let ranges = tree.find(&filter);
    assert!(ranges.is_empty());
}

#[test]
fn test_unique_ids() {
    let tree1 = NumericRangeTree::new();
    let tree2 = NumericRangeTree::new();

    assert_ne!(tree1.unique_id(), tree2.unique_id());
}

// =============================================================================
// Iterator tests
// =============================================================================

#[test]
fn test_iterator_single_node() {
    let tree = NumericRangeTree::new();
    let nodes: Vec<_> = tree.iter().collect();
    assert_eq!(nodes.len(), 1);
    assert!(nodes[0].is_leaf());
}

#[test]
fn test_iterator_visits_all_nodes() {
    let mut tree = NumericRangeTree::new();

    // Add enough values to potentially cause splits
    for i in 0..100 {
        tree.add(i as u64, i as f64, false).unwrap();
    }

    // Count nodes
    let count = tree.iter().count();
    assert!(count >= 1, "Iterator should visit at least the root");

    // All leaf nodes should have ranges
    let leaves: Vec<_> = tree.iter().filter(|n| n.is_leaf()).collect();
    for leaf in leaves {
        assert!(leaf.range.is_some(), "Leaf nodes should have ranges");
    }
}
