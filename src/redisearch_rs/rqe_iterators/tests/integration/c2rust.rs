/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for [`CRQEIterator`], the shim that lets Rust code drive iterators
//! exposed through the C iterator API.

use query_types::QueryNodeType;
use rqe_iterators::{
    RQEIterator, c2rust::CRQEIterator, id_list::IdListSorted, interop::RQEIteratorWrapper,
    intersection::Intersection, union_opaque::build_union,
};

fn c_leaves(n: usize) -> Vec<CRQEIterator> {
    (0..n)
        .map(|_| CRQEIterator::from_rust_leaf(IdListSorted::new(vec![1, 2, 3])))
        .collect()
}

/// A C-wrapped `Intersection` is recognised through its header and weighted by
/// `1 / num_children`. Under miri this also checks that the header-to-wrapper
/// cast keeps provenance over the whole wrapper allocation.
#[test]
fn sort_weight_of_c_wrapped_intersection() {
    let intersection = Intersection::new(c_leaves(4), 1.0, false);
    let ptr = RQEIteratorWrapper::boxed_new_compound(intersection);
    // SAFETY: `boxed_new_compound` returns a uniquely-owned header with every
    // required callback populated.
    let it = unsafe { CRQEIterator::new(ptr) };

    assert_eq!(it.intersection_sort_weight(false), 0.25);
}

/// A C-wrapped union is weighted by its active child count, but only when
/// union children are prioritised.
#[test]
fn sort_weight_of_c_wrapped_union() {
    let ptr = build_union(c_leaves(3), false, usize::MAX, QueryNodeType::Union, 1.0);
    // SAFETY: `build_union` returns a uniquely-owned header with every
    // required callback populated.
    let it = unsafe { CRQEIterator::new(ptr) };

    assert_eq!(it.intersection_sort_weight(true), 3.0);
    assert_eq!(it.intersection_sort_weight(false), 1.0);
}
