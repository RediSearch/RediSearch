/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for NumericRangeNode.

use numeric_range_tree::NumericRangeNode;

#[test]
fn test_new_leaf() {
    let node = NumericRangeNode::new_leaf();
    assert!(node.is_leaf());
    assert!(node.has_range());
    assert_eq!(node.max_depth(), 0);
    assert!(node.left().is_none());
    assert!(node.right().is_none());
}

#[test]
fn test_is_leaf() {
    let mut node = NumericRangeNode::new_leaf();
    assert!(node.is_leaf());

    // Adding a left child makes it not a leaf
    node.set_left(Some(Box::new(NumericRangeNode::new_leaf())));
    assert!(!node.is_leaf());

    // Even with only one child
    node.set_left(None);
    node.set_right(Some(Box::new(NumericRangeNode::new_leaf())));
    assert!(!node.is_leaf());
}

#[test]
fn test_take_range() {
    let mut node = NumericRangeNode::new_leaf();
    assert!(node.has_range());

    let range = node.take_range();
    assert!(range.is_some());
    assert!(!node.has_range());
}
