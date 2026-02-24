/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for NumericRangeTreeIterator.

use numeric_range_tree::{NumericRangeTree, ReversePreOrderDfsIterator};
use rstest::rstest;

use numeric_range_tree::test_utils::{SPLIT_TRIGGER, build_tree};

#[test]
fn test_iterator_single_node() {
    let tree = NumericRangeTree::new(false);
    let mut iter = ReversePreOrderDfsIterator::new(&tree);

    // Should yield exactly one node (the root leaf)
    assert!(iter.next().is_some());
    assert!(iter.next().is_none());
}

#[test]
fn test_iterator_empty_after_exhaustion() {
    let tree = NumericRangeTree::new(false);
    let mut iter = ReversePreOrderDfsIterator::new(&tree);

    // Exhaust the iterator
    while iter.next().is_some() {}

    // Should stay empty
    assert!(iter.next().is_none());
    assert!(iter.next().is_none());
}

#[test]
fn test_iterator_visits_all_leaves() {
    let tree = NumericRangeTree::new(false);
    let iter = ReversePreOrderDfsIterator::new(&tree);

    let leaf_count = iter.filter(|node| node.is_leaf()).count();
    assert_eq!(leaf_count, tree.num_leaves());
}

#[test]
fn test_from_node_single_leaf() {
    let tree = NumericRangeTree::new(false);
    let mut iter = tree.iter();

    // Should yield exactly one node
    let first = iter.next();
    assert!(first.is_some());
    assert!(first.unwrap().is_leaf());
    assert!(iter.next().is_none());
}

#[test]
fn test_from_node_with_children() {
    // Build a tree that has split (internal root + children)
    let tree = build_tree(SPLIT_TRIGGER, false, 0);
    // Should visit at least 3 nodes (root + 2 children)
    assert!(tree.iter().count() >= 3);
}

#[test]
fn test_iterator_traverses_multi_level_tree() {
    // Build a tree with enough entries to create multiple levels
    let tree = build_tree(100, false, 0);

    let nodes: Vec<_> = tree.iter().collect();

    // Should visit multiple nodes
    assert!(nodes.len() >= 5);

    // First node should be the root (internal)
    assert!(!nodes[0].is_leaf());

    // Verify depth-first order: root first, then descendants
    // The first node is internal, and eventually we should see leaves
    let has_leaves = nodes.iter().any(|n| n.is_leaf());
    assert!(has_leaves);
}

#[test]
fn test_iterator_counts_internal_and_leaf_nodes() {
    // Build tree with enough entries to have mixed internal and leaf nodes
    let tree = build_tree(SPLIT_TRIGGER, false, 0);
    let iter = tree.iter();

    let mut internal_count = 0;
    let mut leaf_count = 0;
    for node in iter {
        if node.is_leaf() {
            leaf_count += 1;
        } else {
            internal_count += 1;
        }
    }

    assert!(internal_count >= 1); // at least the root is internal
    assert!(leaf_count >= 2); // at least two leaves after a split
}

#[rstest]
fn test_into_iterator(#[values(false, true)] compress_floats: bool) {
    let tree = build_tree(100, compress_floats, 0);

    let mut count = 0;
    let mut n_leaves = 0;
    for node in &tree {
        count += 1;
        if node.is_leaf() {
            n_leaves += 1;
        }
    }
    assert!(count > 0, "iterator should yield at least one node");
    assert_eq!(n_leaves, tree.num_leaves());
}
