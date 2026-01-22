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

#[test]
fn test_default_impl() {
    let node: NumericRangeNode = Default::default();
    assert!(node.is_leaf());
    assert!(node.has_range());
    assert_eq!(node.max_depth(), 0);
}

#[test]
fn test_split_value_operations() {
    let mut node = NumericRangeNode::new_leaf();
    assert_eq!(node.split_value(), 0.0);

    node.set_split_value(5.0);
    assert_eq!(node.split_value(), 5.0);

    node.set_split_value(-10.5);
    assert_eq!(node.split_value(), -10.5);

    node.set_split_value(f64::INFINITY);
    assert_eq!(node.split_value(), f64::INFINITY);
}

#[test]
fn test_max_depth_operations() {
    let mut node = NumericRangeNode::new_leaf();
    assert_eq!(node.max_depth(), 0);

    node.set_max_depth(5);
    assert_eq!(node.max_depth(), 5);

    node.set_max_depth(100);
    assert_eq!(node.max_depth(), 100);

    node.set_max_depth(-1);
    assert_eq!(node.max_depth(), -1);
}

#[test]
fn test_left_mut() {
    let mut node = NumericRangeNode::new_leaf();
    node.set_left(Some(Box::new(NumericRangeNode::new_leaf())));

    // Access mutable reference to left child
    let left = node.left_mut().unwrap();
    left.set_split_value(42.0);

    // Verify the modification persisted
    assert_eq!(node.left().unwrap().split_value(), 42.0);
}

#[test]
fn test_right_mut() {
    let mut node = NumericRangeNode::new_leaf();
    node.set_right(Some(Box::new(NumericRangeNode::new_leaf())));

    // Access mutable reference to right child
    let right = node.right_mut().unwrap();
    right.set_split_value(99.0);

    // Verify the modification persisted
    assert_eq!(node.right().unwrap().split_value(), 99.0);
}

#[test]
fn test_take_left() {
    let mut node = NumericRangeNode::new_leaf();
    let mut child = NumericRangeNode::new_leaf();
    child.set_split_value(10.0);
    node.set_left(Some(Box::new(child)));

    assert!(node.left().is_some());

    // Take the left child
    let taken = node.take_left();
    assert!(taken.is_some());
    assert_eq!(taken.unwrap().split_value(), 10.0);

    // Now left should be None
    assert!(node.left().is_none());
    assert!(node.take_left().is_none());
}

#[test]
fn test_take_right() {
    let mut node = NumericRangeNode::new_leaf();
    let mut child = NumericRangeNode::new_leaf();
    child.set_split_value(20.0);
    node.set_right(Some(Box::new(child)));

    assert!(node.right().is_some());

    // Take the right child
    let taken = node.take_right();
    assert!(taken.is_some());
    assert_eq!(taken.unwrap().split_value(), 20.0);

    // Now right should be None
    assert!(node.right().is_none());
    assert!(node.take_right().is_none());
}

#[test]
fn test_set_range() {
    use numeric_range_tree::NumericRange;

    let mut node = NumericRangeNode::new_leaf();
    assert!(node.has_range());

    // Clear the range
    node.set_range(None);
    assert!(!node.has_range());
    assert!(node.range().is_none());

    // Set a new range
    let mut range = NumericRange::new();
    range.add(1, 5.0).unwrap();
    node.set_range(Some(range));
    assert!(node.has_range());
    assert_eq!(node.range().unwrap().min_val(), 5.0);
}

#[test]
fn test_range_mut() {
    let mut node = NumericRangeNode::new_leaf();

    // Modify the range through range_mut
    let range = node.range_mut().unwrap();
    range.add(1, 10.0).unwrap();
    range.add(2, 20.0).unwrap();

    // Verify modifications persisted
    let range = node.range().unwrap();
    assert_eq!(range.min_val(), 10.0);
    assert_eq!(range.max_val(), 20.0);
    assert_eq!(range.num_entries(), 2);
}

#[test]
fn test_take_children_returns_none_for_leaf() {
    let mut node = NumericRangeNode::new_leaf();
    assert!(node.take_left().is_none());
    assert!(node.take_right().is_none());
}

#[test]
fn test_mut_accessors_return_none_for_missing_children() {
    let mut node = NumericRangeNode::new_leaf();
    assert!(node.left_mut().is_none());
    assert!(node.right_mut().is_none());
}
