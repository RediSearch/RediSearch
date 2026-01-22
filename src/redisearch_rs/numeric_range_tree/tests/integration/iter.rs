/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for NumericRangeTreeIterator.

use numeric_range_tree::{NumericRangeNode, NumericRangeTree, NumericRangeTreeIterator};

#[test]
fn test_iterator_single_node() {
    let tree = NumericRangeTree::new();
    let mut iter = NumericRangeTreeIterator::new(&tree);

    // Should yield exactly one node (the root leaf)
    assert!(iter.next().is_some());
    assert!(iter.next().is_none());
}

#[test]
fn test_iterator_empty_after_exhaustion() {
    let tree = NumericRangeTree::new();
    let mut iter = NumericRangeTreeIterator::new(&tree);

    // Exhaust the iterator
    while iter.next().is_some() {}

    // Should stay empty
    assert!(iter.next().is_none());
    assert!(iter.next().is_none());
}

#[test]
fn test_iterator_visits_all_leaves() {
    let tree = NumericRangeTree::new();
    let iter = NumericRangeTreeIterator::new(&tree);

    let leaf_count = iter.filter(|node| node.is_leaf()).count();
    assert_eq!(leaf_count, tree.num_leaves());
}

#[test]
fn test_from_node_single_leaf() {
    let node = NumericRangeNode::new_leaf();
    let mut iter = NumericRangeTreeIterator::from_node(&node);

    // Should yield exactly one node
    let first = iter.next();
    assert!(first.is_some());
    assert!(first.unwrap().is_leaf());
    assert!(iter.next().is_none());
}

#[test]
fn test_from_node_with_children() {
    // Build a multi-level tree manually
    let mut root = NumericRangeNode::new_leaf();
    root.set_split_value(5.0);
    root.set_left(Some(Box::new(NumericRangeNode::new_leaf())));
    root.set_right(Some(Box::new(NumericRangeNode::new_leaf())));

    let iter = NumericRangeTreeIterator::from_node(&root);

    // Should visit all 3 nodes (root + 2 children)
    let count = iter.count();
    assert_eq!(count, 3);
}

#[test]
fn test_iterator_traverses_multi_level_tree() {
    // Build a 3-level tree:
    //        root
    //       /    \
    //     left   right
    //    /   \
    //  ll    lr
    let mut left_left = NumericRangeNode::new_leaf();
    left_left.set_split_value(1.0);

    let mut left_right = NumericRangeNode::new_leaf();
    left_right.set_split_value(3.0);

    let mut left = NumericRangeNode::new_leaf();
    left.set_split_value(2.0);
    left.set_left(Some(Box::new(left_left)));
    left.set_right(Some(Box::new(left_right)));

    let mut right = NumericRangeNode::new_leaf();
    right.set_split_value(7.0);

    let mut root = NumericRangeNode::new_leaf();
    root.set_split_value(5.0);
    root.set_left(Some(Box::new(left)));
    root.set_right(Some(Box::new(right)));

    let iter = NumericRangeTreeIterator::from_node(&root);
    let nodes: Vec<_> = iter.collect();

    // Should visit 5 nodes total
    assert_eq!(nodes.len(), 5);

    // Verify depth-first order: root, left, ll, lr, right
    assert_eq!(nodes[0].split_value(), 5.0); // root
    assert_eq!(nodes[1].split_value(), 2.0); // left
    assert_eq!(nodes[2].split_value(), 1.0); // left_left
    assert_eq!(nodes[3].split_value(), 3.0); // left_right
    assert_eq!(nodes[4].split_value(), 7.0); // right
}

#[test]
fn test_iterator_counts_internal_and_leaf_nodes() {
    // Build tree with mixed internal and leaf nodes
    let mut root = NumericRangeNode::new_leaf();
    root.set_left(Some(Box::new(NumericRangeNode::new_leaf())));
    root.set_right(Some(Box::new(NumericRangeNode::new_leaf())));

    let iter = NumericRangeTreeIterator::from_node(&root);

    let mut internal_count = 0;
    let mut leaf_count = 0;
    for node in iter {
        if node.is_leaf() {
            leaf_count += 1;
        } else {
            internal_count += 1;
        }
    }

    assert_eq!(internal_count, 1); // root is internal
    assert_eq!(leaf_count, 2); // left and right are leaves
}
