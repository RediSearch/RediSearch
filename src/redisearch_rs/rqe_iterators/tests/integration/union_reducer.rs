/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for [`new_union_iterator`].

use rqe_iterators::{
    Empty, IteratorType, RQEIterator, Wildcard,
    union_reducer::{NewUnionIterator, new_union_iterator},
};

use crate::utils::Mock;

// ---------------------------------------------------------------------------
// Reducer: all empty children → Empty
// ---------------------------------------------------------------------------

#[test]
fn all_empty_children_reduces_to_empty() {
    let children: Vec<Empty> = vec![Empty, Empty, Empty];
    let result = new_union_iterator(children, false, false);

    let NewUnionIterator::ReducedEmpty(it) = result else {
        panic!("Expected ReducedEmpty");
    };
    assert_eq!(it.type_(), IteratorType::Empty);
}

#[test]
fn no_children_reduces_to_empty() {
    let children: Vec<Empty> = vec![];
    let result = new_union_iterator(children, false, false);

    let NewUnionIterator::ReducedEmpty(it) = result else {
        panic!("Expected ReducedEmpty");
    };
    assert_eq!(it.type_(), IteratorType::Empty);
}

// ---------------------------------------------------------------------------
// Reducer: single non-empty child → ReducedSingle
// ---------------------------------------------------------------------------

#[test]
fn single_child_reduces_to_single() {
    let children: Vec<Mock<3>> = vec![Mock::new([1, 2, 3])];
    let result = new_union_iterator(children, false, false);

    let NewUnionIterator::ReducedSingle(mut it) = result else {
        panic!("Expected ReducedSingle");
    };
    let mut seen = Vec::new();
    while let Ok(Some(doc)) = it.read() {
        seen.push(doc.doc_id);
    }
    assert_eq!(seen, vec![1, 2, 3]);
}

#[test]
fn single_non_empty_after_filtering_reduces_to_single() {
    let children: Vec<Box<dyn RQEIterator>> = vec![
        Box::new(Empty),
        Box::new(Mock::new([5, 10])),
        Box::new(Empty),
    ];
    let result = new_union_iterator(children, false, false);

    let NewUnionIterator::ReducedSingle(mut it) = result else {
        panic!("Expected ReducedSingle");
    };
    let mut seen = Vec::new();
    while let Ok(Some(doc)) = it.read() {
        seen.push(doc.doc_id);
    }
    assert_eq!(seen, vec![5, 10]);
}

// ---------------------------------------------------------------------------
// Reducer: quick exit + wildcard child → wildcard returned
// ---------------------------------------------------------------------------

#[test]
fn quick_exit_wildcard_reduces_to_single() {
    let children: Vec<Box<dyn RQEIterator>> = vec![
        Box::new(Mock::new([1, 2])),
        Box::new(Wildcard::new(10, 1.0)),
        Box::new(Mock::new([3, 4])),
    ];
    let result = new_union_iterator(children, true, false);

    let NewUnionIterator::ReducedSingle(it) = result else {
        panic!("Expected ReducedSingle (wildcard)");
    };
    assert!(matches!(
        it.type_(),
        IteratorType::Wildcard | IteratorType::InvIdxWildcard
    ));
}

#[test]
fn non_quick_exit_wildcard_not_reduced() {
    let children: Vec<Box<dyn RQEIterator>> = vec![
        Box::new(Mock::new([1, 2])),
        Box::new(Wildcard::new(10, 1.0)),
    ];
    // quick_exit = false, so wildcard should NOT cause reduction.
    let result = new_union_iterator(children, false, false);

    assert!(
        matches!(result, NewUnionIterator::Flat(_)),
        "Expected Flat union, not a reduced variant"
    );
}

// ---------------------------------------------------------------------------
// Flat vs Heap selection
// ---------------------------------------------------------------------------

#[test]
fn flat_selected_when_use_heap_false() {
    let children: Vec<Box<dyn RQEIterator>> =
        vec![Box::new(Mock::new([1, 3])), Box::new(Mock::new([2, 4]))];
    let result = new_union_iterator(children, false, false);

    assert!(matches!(result, NewUnionIterator::Flat(_)), "Expected Flat");
}

#[test]
fn heap_selected_when_use_heap_true() {
    let children: Vec<Box<dyn RQEIterator>> = vec![
        Box::new(Mock::new([1, 4])),
        Box::new(Mock::new([2, 5])),
        Box::new(Mock::new([3, 6])),
    ];
    let result = new_union_iterator(children, false, true);

    assert!(matches!(result, NewUnionIterator::Heap(_)), "Expected Heap");
}

#[test]
fn flat_quick_selected() {
    let children: Vec<Box<dyn RQEIterator>> =
        vec![Box::new(Mock::new([1, 3])), Box::new(Mock::new([2, 4]))];
    let result = new_union_iterator(children, true, false);

    assert!(
        matches!(result, NewUnionIterator::FlatQuick(_)),
        "Expected FlatQuick"
    );
}

#[test]
fn heap_quick_selected() {
    let children: Vec<Box<dyn RQEIterator>> = vec![
        Box::new(Mock::new([1, 4])),
        Box::new(Mock::new([2, 5])),
        Box::new(Mock::new([3, 6])),
    ];
    let result = new_union_iterator(children, true, true);

    assert!(
        matches!(result, NewUnionIterator::HeapQuick(_)),
        "Expected HeapQuick"
    );
}

// ---------------------------------------------------------------------------
// Functional: flat union produces correct results
// ---------------------------------------------------------------------------

#[test]
#[cfg_attr(miri, ignore = "Calls FFI function `RSYieldableMetric_Concat`")]
fn flat_union_produces_all_docs() {
    let children: Vec<Box<dyn RQEIterator>> = vec![
        Box::new(Mock::new([1, 3, 5])),
        Box::new(Mock::new([2, 3, 4])),
    ];
    let result = new_union_iterator(children, false, false);

    let NewUnionIterator::Flat(mut it) = result else {
        panic!("Expected Flat");
    };
    let mut seen = Vec::new();
    while let Ok(Some(doc)) = it.read() {
        seen.push(doc.doc_id);
    }
    assert_eq!(seen, vec![1, 2, 3, 4, 5]);
}

#[test]
#[cfg_attr(miri, ignore = "Calls FFI function `RSYieldableMetric_Concat`")]
fn heap_union_produces_all_docs() {
    let children: Vec<Box<dyn RQEIterator>> = vec![
        Box::new(Mock::new([1, 4, 7])),
        Box::new(Mock::new([2, 5, 8])),
        Box::new(Mock::new([3, 6, 9])),
    ];
    let result = new_union_iterator(children, false, true);

    let NewUnionIterator::Heap(mut it) = result else {
        panic!("Expected Heap");
    };
    let mut seen = Vec::new();
    while let Ok(Some(doc)) = it.read() {
        seen.push(doc.doc_id);
    }
    assert_eq!(seen, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
}
