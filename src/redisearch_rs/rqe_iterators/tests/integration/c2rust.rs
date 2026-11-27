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
    IteratorType, RQEIterator, TypeErasedRQEIterator, UnionFullFlat, UnionOpaque, UnionVariant,
    c2rust::CRQEIterator, id_list::IdListSorted, interop::RQEIteratorWrapper,
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

/// A union over type-erased children, lowered as a type-erased leaf: the shape
/// the disk backend gives multi-range numeric and geo clauses. It reports
/// [`IteratorType::Union`] without being a `UnionOpaque<CRQEIterator>`.
fn c_wrapped_type_erased_union(n: usize) -> CRQEIterator {
    let children: Vec<TypeErasedRQEIterator<'static>> = (0..n)
        .map(|_| TypeErasedRQEIterator::new(Box::new(IdListSorted::new(vec![1, 2, 3]))))
        .collect();
    let union = TypeErasedRQEIterator::new(Box::new(UnionOpaque {
        variant: UnionVariant::FlatFull(UnionFullFlat::new(children)),
        query_node_type: QueryNodeType::Union,
        query_string: None,
    }));
    let it = CRQEIterator::from_rust_leaf(union);
    assert_eq!(it.type_(), IteratorType::Union);
    it
}

/// The weight comes from the union itself, whatever its child type. Under
/// miri this also checks that nothing reads the wrapper as another type.
#[test]
fn sort_weight_of_c_wrapped_type_erased_union() {
    let it = c_wrapped_type_erased_union(3);

    assert_eq!(it.intersection_sort_weight(true), 3.0);
    assert_eq!(it.intersection_sort_weight(false), 1.0);
}

/// A header built outside this module may leave `IntersectionSortWeight` NULL;
/// such an iterator gets the neutral weight instead of its own.
#[test]
fn sort_weight_of_header_without_callback() {
    let ptr = build_union(c_leaves(3), false, usize::MAX, QueryNodeType::Union, 1.0);
    // SAFETY: `ptr` was just produced by a wrapper constructor and is not aliased.
    unsafe { rqe_iterators::interop::patch_vtable(ptr, |h| h.IntersectionSortWeight = None) };
    // SAFETY: `build_union` returns a uniquely-owned header with every
    // required callback populated, and `IntersectionSortWeight` is optional.
    let it = unsafe { CRQEIterator::new(ptr) };

    assert_eq!(it.intersection_sort_weight(true), 1.0);
}

/// With union children prioritised, a type-erased union is weighted by its
/// child count and so moves behind a leaf that estimates more documents.
#[rstest::rstest]
#[case::prioritized(true, IteratorType::IdListSorted)]
#[case::not_prioritized(false, IteratorType::Union)]
fn intersection_orders_c_wrapped_type_erased_union(
    #[case] prioritize_union_children: bool,
    #[case] first: IteratorType,
) {
    // The union estimates 9 documents, weighted 27 when prioritised; the leaf 12.
    let union = c_wrapped_type_erased_union(3);
    let leaf = CRQEIterator::from_rust_leaf(IdListSorted::new((1..=12).collect::<Vec<_>>()));

    let intersection = Intersection::new(vec![union, leaf], 1.0, prioritize_union_children);

    assert_eq!(intersection.child_at(0).type_(), first);
}
