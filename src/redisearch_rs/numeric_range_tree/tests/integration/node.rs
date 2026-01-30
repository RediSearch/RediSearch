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
    let node = NumericRangeNode::leaf(false);
    assert!(node.has_range());
    assert_eq!(node.max_depth(), 0);
}

#[test]
fn test_is_leaf() {
    let leaf = NumericRangeNode::leaf(false);
    assert!(leaf.is_leaf());

    // An internal node is not a leaf
    let internal = NumericRangeNode::internal(
        5.0,
        NumericRangeNode::leaf(false),
        NumericRangeNode::leaf(false),
        None,
    );
    assert!(!internal.is_leaf());
}

#[test]
fn test_default_impl() {
    let node: NumericRangeNode = Default::default();
    assert!(node.is_leaf());
    assert!(node.has_range());
    assert_eq!(node.max_depth(), 0);
}

#[test]
fn test_split_value() {
    let leaf = NumericRangeNode::leaf(false);
    assert_eq!(leaf.split_value(), 0.0);

    let internal = NumericRangeNode::internal(
        5.0,
        NumericRangeNode::leaf(false),
        NumericRangeNode::leaf(false),
        None,
    );
    assert_eq!(internal.split_value(), 5.0);

    let internal_neg = NumericRangeNode::internal(
        -10.5,
        NumericRangeNode::leaf(false),
        NumericRangeNode::leaf(false),
        None,
    );
    assert_eq!(internal_neg.split_value(), -10.5);

    let internal_inf = NumericRangeNode::internal(
        f64::INFINITY,
        NumericRangeNode::leaf(false),
        NumericRangeNode::leaf(false),
        None,
    );
    assert_eq!(internal_inf.split_value(), f64::INFINITY);
}

#[test]
fn test_max_depth() {
    let leaf = NumericRangeNode::leaf(false);
    assert_eq!(leaf.max_depth(), 0);

    let internal = NumericRangeNode::internal(
        0.0,
        NumericRangeNode::leaf(false),
        NumericRangeNode::leaf(false),
        None,
    );
    // new_internal computes max_depth from children: max(0, 0) + 1 = 1
    assert_eq!(internal.max_depth(), 1);
}
