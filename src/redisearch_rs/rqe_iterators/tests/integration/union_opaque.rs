/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for [`UnionOpaque`]'s suspend/resume surface.
//!
//! [`UnionOpaque`] has no read/skip behaviour of its own — it forwards
//! everything to the variant it holds, and the variants are covered by
//! `union_flat`, `union_heap` and `union_trimmed`. What *is* its own is the
//! transition: a per-variant walk that rewrites the payload in place, followed
//! by a whole-box cast, plus a bespoke `dealloc` for the case where the payload
//! was consumed. Those are what these tests exercise.

use std::ffi::CStr;

use query_types::QueryNodeType;
use ref_mode::SharedPtr;
use rqe_iterators::{
    RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator, ResumeOutcome,
    TypeErasedRQEIterator, UnionFullFlat, UnionFullHeap, UnionOpaque, UnionQuickFlat,
    UnionQuickHeap, UnionVariant,
};
use rqe_iterators_test_utils::{MockContext, ResumeOutcomeExt, revalidate_via_resume};

use crate::utils::{Mock, MockIteratorError, MockRevalidateResult};

/// Which concrete union sits inside the [`UnionOpaque`] under test.
///
/// The transition rewrites the payload **per variant**, so each arm carries its
/// own layout obligation and covering one says nothing about the others.
/// `Trimmed` is absent because its resume is `unreachable!` by design — trimmed
/// unions are never revalidated — so there is no round trip to exercise.
#[derive(Clone, Copy, Debug)]
enum Which {
    FlatFull,
    FlatQuick,
    HeapFull,
    HeapQuick,
}

/// Wrap the mocks as the type-erased children a union really holds: their
/// active and suspended forms carry different vtables, which a concrete-child
/// test cannot exercise.
fn erased<'spec, const N0: usize, const N1: usize>(
    c0: Mock<'spec, N0>,
    c1: Mock<'spec, N1>,
) -> Vec<TypeErasedRQEIterator<'spec>> {
    vec![
        TypeErasedRQEIterator::new(Box::new(c0)),
        TypeErasedRQEIterator::new(Box::new(c1)),
    ]
}

fn variant_of<'spec>(
    which: Which,
    children: Vec<TypeErasedRQEIterator<'spec>>,
) -> UnionVariant<'spec, TypeErasedRQEIterator<'spec>> {
    match which {
        Which::FlatFull => UnionVariant::FlatFull(UnionFullFlat::new(children)),
        Which::FlatQuick => UnionVariant::FlatQuick(UnionQuickFlat::new(children)),
        Which::HeapFull => UnionVariant::HeapFull(UnionFullHeap::new(children)),
        Which::HeapQuick => UnionVariant::HeapQuick(UnionQuickHeap::new(children)),
    }
}

fn opaque<'spec>(
    variant: UnionVariant<'spec, TypeErasedRQEIterator<'spec>>,
) -> UnionOpaque<'spec, TypeErasedRQEIterator<'spec>> {
    UnionOpaque {
        variant,
        query_node_type: QueryNodeType::Union,
        query_string: None,
    }
}

/// A: the round trip, once per variant.
///
/// Guards the per-variant `ptr::write` of the suspended payload into a slot
/// sized for the active one — an oversize write is a heap overflow, an
/// undersize one leaves the tail of the slot holding stale active bytes that
/// the resume then reads back as suspended state.
#[rstest::rstest]
#[case::flat_full(Which::FlatFull)]
#[case::flat_quick(Which::FlatQuick)]
#[case::heap_full(Which::HeapFull)]
#[case::heap_quick(Which::HeapQuick)]
fn resume_round_trip_per_variant(#[case] which: Which) {
    let mock_ctx = MockContext::new(0, 0);
    let child0: Mock<'_, 3> = Mock::new([10, 30, 50]);
    let child1: Mock<'_, 3> = Mock::new([20, 40, 60]);

    let mut union = opaque(variant_of(which, erased(child0, child1)));
    assert_eq!(union.read().unwrap().unwrap().doc_id, 10);

    let guard = mock_ctx.spec_read();
    let mut union = revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(union)), &guard)
        .expect("resume failed")
        .expect_ok();

    assert_eq!(
        union.read().unwrap().unwrap().doc_id,
        20,
        "{which:?} must carry on where it was",
    );
}

/// B: the allocation — and the payload's address inside it — survive the cycle.
///
/// The FFI wrapper caches a raw pointer into the payload's result object, so a
/// transition that relocated either would dangle it. `suspend` in particular
/// must rewrite the variant *in place* rather than moving it out to a temporary
/// and back into a fresh box.
#[test]
fn resume_preserves_box_and_variant_addresses() {
    let mock_ctx = MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    let child0: Mock<'_, 2> = Mock::new([10, 30]);
    let child1: Mock<'_, 2> = Mock::new([20, 40]);

    let mut union = Box::new(opaque(UnionVariant::FlatFull(UnionFullFlat::new(erased(
        child0, child1,
    )))));
    assert_eq!(union.read().unwrap().unwrap().doc_id, 10);
    let box_addr = &*union as *const _ as usize;
    let variant_addr = &union.variant as *const _ as usize;

    let suspended = union.suspend();
    assert_eq!(
        &*suspended as *const _ as usize, box_addr,
        "suspend must reuse the allocation",
    );
    assert_eq!(
        &suspended.variant as *const _ as usize, variant_addr,
        "suspend must rewrite the variant in place",
    );

    let mut active = match suspended.resume(&guard).expect("resume failed") {
        ResumeOutcome::Ok(a) => a,
        ResumeOutcome::Moved(_) => panic!("expected Ok, got Moved"),
        ResumeOutcome::Aborted => panic!("expected Ok, got Aborted"),
    };
    assert_eq!(
        &*active as *const _ as usize, box_addr,
        "resume must reuse the allocation",
    );
    assert_eq!(
        &active.variant as *const _ as usize, variant_addr,
        "resume must rewrite the variant in place",
    );

    assert_eq!(active.read().unwrap().unwrap().doc_id, 20);
}

/// C1: every child aborting takes the payload down, which is the only path to
/// the bespoke `free_shell` deallocation.
///
/// The payload was consumed by its own resume, so the shell must be freed
/// *without* dropping it — with the layout the box was allocated with, which is
/// the active form's. Under miri a mismatched layout or a double drop of the
/// mocks' reference-counted state is caught here and nowhere else.
#[test]
fn resume_frees_the_shell_when_the_payload_aborts() {
    let mock_ctx = MockContext::new(0, 0);
    let child0: Mock<'_, 2> = Mock::new([10, 30]);
    let child1: Mock<'_, 2> = Mock::new([20, 40]);
    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Abort);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Abort);

    let mut union = opaque(UnionVariant::FlatFull(UnionFullFlat::new(erased(
        child0, child1,
    ))));
    assert_eq!(union.read().unwrap().unwrap().doc_id, 10);

    let guard = mock_ctx.spec_read();
    let outcome = revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(union)), &guard)
        .expect("resume must report the abort as an outcome, not an error");
    assert!(matches!(outcome, ResumeOutcome::Aborted));
}

/// C2: the same teardown reached through the `Err` arm instead.
///
/// A failing child leaves the payload consumed exactly as an abort does, so the
/// error path must free the shell the same way — and still surface the error.
#[test]
fn resume_frees_the_shell_when_the_payload_errors() {
    let mock_ctx = MockContext::new(0, 0);
    let child0: Mock<'_, 2> = Mock::new([10, 30]);
    let child1: Mock<'_, 2> = Mock::new([20, 40]);
    child1
        .data()
        .set_error_on_resume(Some(MockIteratorError::TimeoutError(None)));

    let mut union = opaque(UnionVariant::FlatFull(UnionFullFlat::new(erased(
        child0, child1,
    ))));
    assert_eq!(union.read().unwrap().unwrap().doc_id, 10);

    let guard = mock_ctx.spec_read();
    match revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(union)), &guard) {
        Err(e) => assert!(matches!(e, RQEIteratorError::TimedOut)),
        Ok(_) => panic!("the failing child's error must reach the caller"),
    }
}

/// D: the suspended form's own surface — the accessors callers use *without*
/// resuming, and the one `Rf`-parametrized field the transition re-types
/// without validating anything.
///
/// `query_string` borrows the query AST rather than the index, which is why the
/// suspend/resume cycle is allowed to hand it straight back; asserting the
/// pointer is bit-for-bit the same is what pins that down, since a wrong
/// payload offset would still produce a plausible-looking non-null pointer.
#[test]
fn suspended_accessors_and_query_string_survive_the_cycle() {
    let mock_ctx = MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    let q_str: &CStr = c"hello - world";
    let child0: Mock<'_, 2> = Mock::new([10, 30]);
    let child1: Mock<'_, 2> = Mock::new([20, 40]);

    let mut union = Box::new(UnionOpaque {
        variant: UnionVariant::HeapFull(UnionFullHeap::new(erased(child0, child1))),
        query_node_type: QueryNodeType::Prefix,
        query_string: Some(SharedPtr::from_ref(q_str)),
    });
    assert_eq!(union.read().unwrap().unwrap().doc_id, 10);
    let num_estimated = RQEIterator::num_estimated(&*union);

    let suspended = union.suspend();
    assert_eq!(
        RQESuspendedIterator::last_doc_id(&*suspended),
        10,
        "the suspended form reports the position without resuming",
    );
    assert_eq!(
        RQESuspendedIterator::num_estimated(&*suspended),
        num_estimated,
    );
    assert_eq!(
        suspended
            .query_string
            .expect("the query string survives suspend")
            .as_raw(),
        std::ptr::from_ref(q_str),
    );

    let active = match suspended.resume(&guard).expect("resume failed") {
        ResumeOutcome::Ok(a) => a,
        ResumeOutcome::Moved(_) => panic!("expected Ok, got Moved"),
        ResumeOutcome::Aborted => panic!("expected Ok, got Aborted"),
    };
    assert_eq!(
        active
            .query_string
            .expect("the query string survives resume")
            .as_raw(),
        std::ptr::from_ref(q_str),
    );
    assert_eq!(active.query_node_type, QueryNodeType::Prefix);
}
