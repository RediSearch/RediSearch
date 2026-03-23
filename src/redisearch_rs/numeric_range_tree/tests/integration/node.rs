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

use numeric_range_tree::test_utils::{SPLIT_TRIGGER, build_tree};

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

    // Build a tree and trigger a split to get an internal node
    let tree = build_tree(SPLIT_TRIGGER, false, 0);
    assert!(!tree.root().is_leaf());
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
    assert_eq!(leaf.split_value(), None);

    // Build a tree with enough entries to split — the root becomes internal
    // and has a non-zero split value.
    let tree = build_tree(SPLIT_TRIGGER, false, 0);
    assert!(!tree.root().is_leaf());
    assert!(tree.root().split_value().unwrap() > 0.0);
}

#[test]
fn test_max_depth() {
    let leaf = NumericRangeNode::leaf(false);
    assert_eq!(leaf.max_depth(), 0);

    // After a split, the root should have max_depth >= 1
    let tree = build_tree(SPLIT_TRIGGER, false, 0);
    assert!(tree.root().max_depth() >= 1);
}
