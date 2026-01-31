/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for NumericRangeTreeIterator.

use numeric_range_tree::{DepthFirstNumericRangeTreeIterator, NumericRangeNode, NumericRangeTree};
use rstest::rstest;

#[test]
fn test_iterator_single_node() {
    let tree = NumericRangeTree::new(false);
    let mut iter = DepthFirstNumericRangeTreeIterator::new(&tree);

    // Should yield exactly one node (the root leaf)
    assert!(iter.next().is_some());
    assert!(iter.next().is_none());
}

#[test]
fn test_iterator_empty_after_exhaustion() {
    let tree = NumericRangeTree::new(false);
    let mut iter = DepthFirstNumericRangeTreeIterator::new(&tree);

    // Exhaust the iterator
    while iter.next().is_some() {}

    // Should stay empty
    assert!(iter.next().is_none());
    assert!(iter.next().is_none());
}

#[test]
fn test_iterator_visits_all_leaves() {
    let tree = NumericRangeTree::new(false);
    let iter = DepthFirstNumericRangeTreeIterator::new(&tree);

    let leaf_count = iter.filter(|node| node.is_leaf()).count();
    assert_eq!(leaf_count, tree.num_leaves());
}

#[test]
fn test_from_node_single_leaf() {
    let node = NumericRangeNode::leaf(false);
    let mut iter = DepthFirstNumericRangeTreeIterator::from_node(&node);

    // Should yield exactly one node
    let first = iter.next();
    assert!(first.is_some());
    assert!(first.unwrap().is_leaf());
    assert!(iter.next().is_none());
}

#[test]
fn test_from_node_with_children() {
    // Build a multi-level tree manually
    let root = NumericRangeNode::internal(
        5.0,
        NumericRangeNode::leaf(false),
        NumericRangeNode::leaf(false),
        None,
    );

    let iter = DepthFirstNumericRangeTreeIterator::from_node(&root);

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
    let left = NumericRangeNode::internal(
        2.0,
        NumericRangeNode::leaf(false),
        NumericRangeNode::leaf(false),
        None,
    );

    let root = NumericRangeNode::internal(5.0, left, NumericRangeNode::leaf(false), None);

    let iter = DepthFirstNumericRangeTreeIterator::from_node(&root);
    let nodes: Vec<_> = iter.collect();

    // Should visit 5 nodes total
    assert_eq!(nodes.len(), 5);

    // Verify depth-first order: root, left, ll, lr, right
    assert_eq!(nodes[0].split_value(), 5.0); // root
    assert_eq!(nodes[1].split_value(), 2.0); // left
    assert!(nodes[2].is_leaf()); // left_left
    assert!(nodes[3].is_leaf()); // left_right
    assert!(nodes[4].is_leaf()); // right
}

#[test]
fn test_iterator_counts_internal_and_leaf_nodes() {
    // Build tree with mixed internal and leaf nodes
    let root = NumericRangeNode::internal(
        0.0,
        NumericRangeNode::leaf(false),
        NumericRangeNode::leaf(false),
        None,
    );

    let iter = DepthFirstNumericRangeTreeIterator::from_node(&root);

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

#[rstest]
#[case(false)]
#[case(true)]
fn test_into_iterator(#[case] compress_floats: bool) {
    let mut tree = NumericRangeTree::new(compress_floats);
    for i in 1..=100u64 {
        tree.add(i, i as f64, false, 0);
    }

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
