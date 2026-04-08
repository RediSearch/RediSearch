/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for [`new_not_iterator`].

use std::time::Duration;

use ffi::t_docId;
use rqe_iterators::{
    Empty, IteratorType, RQEIterator, SkipToOutcome, Wildcard,
    not_reducer::{NewNotIterator, new_not_iterator},
};
use rqe_iterators_test_utils::MockContext;

use crate::utils::Mock;

/// Create a [`MockContext`] and call [`new_not_iterator`] with the given child,
/// returning the result alongside the context (to keep it alive).
fn call_new_not_iterator<'a, I>(
    child: I,
    max_doc_id: t_docId,
    ctx: &'a MockContext,
) -> NewNotIterator<'a, I>
where
    I: RQEIterator<'a> + 'a,
{
    // SAFETY: `MockContext` guarantees valid FFI structures for the lifetime
    // of the context.
    unsafe { new_not_iterator(child, max_doc_id, 1.0, Duration::ZERO, true, ctx.qctx()) }
}

// ---------------------------------------------------------------------------
// Reducer: empty child → wildcard (rule 1)
// ---------------------------------------------------------------------------

#[test]
fn empty_child_reduces_to_wildcard() {
    let ctx = MockContext::new(10, 10);
    let result = call_new_not_iterator(Empty, 10, &ctx);

    match result {
        NewNotIterator::ReducedWildcard(mut it) => {
            // NOT(empty) = everything → wildcard.
            assert!(
                matches!(
                    it.type_(),
                    IteratorType::Wildcard | IteratorType::InvIdxWildcard
                ),
                "expected a wildcard iterator, got {:?}",
                it.type_()
            );
            // The wildcard should yield docs 1..=10.
            let mut seen = Vec::new();
            while let Ok(Some(doc)) = it.read() {
                seen.push(doc.doc_id);
            }
            assert_eq!(seen, (1..=10).collect::<Vec<_>>());
        }
        _ => {
            panic!("Expected ReducedWildcard");
        }
    }
}

#[test]
fn empty_child_reduced_wildcard_has_zero_freq() {
    let ctx = MockContext::new(5, 5);
    let result = call_new_not_iterator(Empty, 5, &ctx);

    match result {
        NewNotIterator::ReducedWildcard(mut it) => {
            // The reducer sets freq = 0 on the current result.
            let current = it.current().expect("wildcard should have a current result");
            assert_eq!(current.freq, 0);
        }
        _ => {
            panic!("Expected ReducedWildcard");
        }
    }
}

// ---------------------------------------------------------------------------
// Reducer: wildcard child → empty (rule 2)
// ---------------------------------------------------------------------------

#[test]
fn wildcard_child_reduces_to_empty() {
    let ctx = MockContext::new(10, 10);
    let child = Wildcard::new(10, 1.0);
    let result = call_new_not_iterator(child, 10, &ctx);

    match result {
        NewNotIterator::ReducedEmpty(mut it) => {
            // NOT(wildcard) = nothing → empty.
            assert_eq!(it.type_(), IteratorType::Empty);
            assert!(it.read().unwrap().is_none());
        }
        _ => {
            panic!("Expected ReducedEmpty");
        }
    }
}

// ---------------------------------------------------------------------------
// Non-optimized path (index_all = false, no diskSpec)
// ---------------------------------------------------------------------------

#[test]
fn non_optimized_path_returns_not_iterator() {
    let ctx = MockContext::new(10, 10);
    // index_all defaults to false in MockContext.
    let child = Mock::new([2, 5, 8]);
    let result = call_new_not_iterator(child, 10, &ctx);

    let NewNotIterator::Not(mut it) = result else {
        panic!("Expected Not variant");
    };
    assert_eq!(it.type_(), IteratorType::Not);

    let mut seen = Vec::new();
    while let Ok(Some(doc)) = it.read() {
        seen.push(doc.doc_id);
    }
    // Complement of {2, 5, 8} in [1..=10].
    assert_eq!(seen, vec![1, 3, 4, 6, 7, 9, 10]);
}

#[test]
fn non_optimized_skip_to_works() {
    let ctx = MockContext::new(10, 10);
    let child = Mock::new([3, 7]);
    let result = call_new_not_iterator(child, 10, &ctx);

    let NewNotIterator::Not(mut it) = result else {
        panic!("Expected Not variant");
    };
    // skip_to a doc not in child → Found.
    let outcome = it.skip_to(5).unwrap().unwrap();
    assert!(matches!(outcome, SkipToOutcome::Found(doc) if doc.doc_id == 5));

    // skip_to a doc in child → NotFound (next valid).
    let outcome = it.skip_to(7).unwrap().unwrap();
    assert!(matches!(outcome, SkipToOutcome::NotFound(doc) if doc.doc_id == 8));
}

// ---------------------------------------------------------------------------
// Optimized path (index_all = true)
// ---------------------------------------------------------------------------

#[test]
fn optimized_path_returns_not_optimized_iterator() {
    let ctx = MockContext::new(10, 10);
    // SAFETY: No iterators have been created from this context yet.
    unsafe { ctx.set_index_all(true) };

    let child = Mock::new([2, 5, 8]);
    let result = call_new_not_iterator(child, 10, &ctx);

    let NewNotIterator::NotOptimized(it) = result else {
        panic!("Expected NotOptimized variant");
    };
    assert_eq!(it.type_(), IteratorType::NotOptimized);
    // With index_all=true but existingDocs=null, the wildcard is an
    // EmptyWildcard, so the NOT-optimized iterator produces nothing
    // (there are no "existing" documents to negate against).
}

// ---------------------------------------------------------------------------
// Child access
// ---------------------------------------------------------------------------

#[test]
fn not_child_access() {
    let ctx = MockContext::new(10, 10);
    let child = Mock::new([3, 6]);
    let result = call_new_not_iterator(child, 10, &ctx);

    let NewNotIterator::Not(mut it) = result else {
        panic!("Expected Not variant");
    };
    // child() should return Some.
    assert!(it.child().is_some());

    // take_child() should return Some and leave it unset.
    let taken = it.take_child();
    assert!(taken.is_some());
    assert!(it.child().is_none());

    // set_child() should re-set it.
    it.set_child(taken.unwrap());
    assert!(it.child().is_some());
}

#[test]
fn not_child_access_optimized() {
    let ctx = MockContext::new(10, 10);
    // SAFETY: No iterators have been created from this context yet.
    unsafe { ctx.set_index_all(true) };

    let child = Mock::new([3, 6]);
    let result = call_new_not_iterator(child, 10, &ctx);

    let NewNotIterator::NotOptimized(mut it) = result else {
        panic!("Expected NotOptimized variant");
    };
    assert_eq!(it.type_(), IteratorType::NotOptimized);
    assert!(it.child().is_some());

    let taken = it.take_child();
    assert!(taken.is_some());
    assert!(it.child().is_none());

    it.set_child(taken.unwrap());
    assert!(it.child().is_some());
}

// ---------------------------------------------------------------------------
// Weight propagation
// ---------------------------------------------------------------------------

#[test]
fn weight_is_propagated() {
    let ctx = MockContext::new(5, 5);
    let child = Mock::new([2]);
    let weight = 0.42;

    // SAFETY: MockContext guarantees valid FFI structures.
    let result = unsafe { new_not_iterator(child, 5, weight, Duration::ZERO, true, ctx.qctx()) };

    let NewNotIterator::Not(mut it) = result else {
        panic!("Expected Not variant");
    };
    let doc = it.read().unwrap().unwrap();
    assert_eq!(doc.doc_id, 1);
    assert!(
        (doc.weight - weight).abs() < f64::EPSILON,
        "weight should be {weight}, got {}",
        doc.weight
    );
}
