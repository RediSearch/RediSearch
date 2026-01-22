/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for NumericRangeTreeIterator.

use numeric_range_tree::{NumericRangeTree, NumericRangeTreeIterator};

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
