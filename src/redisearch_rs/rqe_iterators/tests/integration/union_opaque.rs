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
//! by a whole-box cast, plus a bespoke `free_shell` deallocation for the case
//! where the payload was consumed. Those are what these tests exercise.

use std::ffi::CStr;

use query_types::QueryNodeType;
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
/// `Trimmed` is absent: it has no round trip to exercise, see
/// [`UnionTrimmed`](rqe_iterators::UnionTrimmed).
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

const fn opaque<'spec>(
    variant: UnionVariant<'spec, TypeErasedRQEIterator<'spec>>,
) -> UnionOpaque<'spec, TypeErasedRQEIterator<'spec>> {
    UnionOpaque {
        variant,
        query_node_type: QueryNodeType::Union,
        query_string: None,
    }
}

/// The round trip, once per variant, including the suspended form's accessors.
///
/// Payload size mismatches are already compile errors; what this guards is the
/// per-variant dispatch — each arm must transition its own payload and keep
/// the tag, so the resumed union carries on from the same position. The
/// accessors dispatch per variant too, so each arm is checked against the
/// active form's answer.
#[rstest::rstest]
#[case::flat_full(Which::FlatFull)]
#[case::flat_quick(Which::FlatQuick)]
#[case::heap_full(Which::HeapFull)]
#[case::heap_quick(Which::HeapQuick)]
fn resume_round_trip_per_variant(#[case] which: Which) {
    let mock_ctx = MockContext::new(0, 0);
    let child0: Mock<'_, 3> = Mock::new([10, 30, 50]);
    let child1: Mock<'_, 3> = Mock::new([20, 40, 60]);

    let mut union = Box::new(opaque(variant_of(which, erased(child0, child1))));
    assert_eq!(union.read().unwrap().unwrap().doc_id, 10);
    let num_estimated = RQEIterator::num_estimated(&*union);

    let suspended = union.suspend();
    assert_eq!(RQESuspendedIterator::last_doc_id(&*suspended), 10);
    assert_eq!(
        RQESuspendedIterator::num_estimated(&*suspended),
        num_estimated,
    );

    let guard = mock_ctx.spec_read();
    let ResumeOutcome::Ok(mut union) = suspended.resume(&guard).expect("resume failed") else {
        panic!("{which:?}: no child changed, so the union must resume unchanged");
    };

    assert_eq!(
        union.read().unwrap().unwrap().doc_id,
        20,
        "{which:?} must carry on where it was",
    );
}

/// A child that moved during the suspension surfaces as
/// [`ResumeOutcome::Moved`] on the opaque union, once per variant: the payload's
/// own `Moved` must not be flattened into `Ok` by the outer re-wrapping.
#[rstest::rstest]
#[case::flat_full(Which::FlatFull)]
#[case::flat_quick(Which::FlatQuick)]
#[case::heap_full(Which::HeapFull)]
#[case::heap_quick(Which::HeapQuick)]
fn resume_reports_moved_per_variant(#[case] which: Which) {
    let mock_ctx = MockContext::new(0, 0);
    let child0: Mock<'_, 3> = Mock::new([10, 30, 50]);
    let child1: Mock<'_, 3> = Mock::new([20, 40, 60]);
    child0
        .data()
        .set_revalidate_result(MockRevalidateResult::Move);
    child1
        .data()
        .set_revalidate_result(MockRevalidateResult::Move);

    let mut union = opaque(variant_of(which, erased(child0, child1)));
    assert_eq!(union.read().unwrap().unwrap().doc_id, 10);

    let guard = mock_ctx.spec_read();
    let union = revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(union)), &guard)
        .expect("resume failed")
        .expect_moved();
    // Each child steps to its next doc (30 and 40); the union lands on the lower.
    assert_eq!(
        union.last_doc_id(),
        30,
        "{which:?} must report the position its children moved it to",
    );
}

/// A trimmed union refuses to suspend with an ordinary panic, as its legacy
/// `revalidate` does, rather than aborting the process — see the comment in
/// [`UnionOpaque`]'s [`RQEIteratorBoxed::suspend`] for why that takes an early
/// refusal.
#[test]
#[should_panic(expected = "suspend is not supported on UnionTrimmed")]
fn suspend_of_a_trimmed_union_panics() {
    let children = (1..=3)
        .map(|doc_id| {
            let child: Mock<'_, 1> = Mock::new([doc_id]);
            TypeErasedRQEIterator::new(Box::new(child))
        })
        .collect();
    let mut union = opaque(UnionVariant::FlatFull(UnionFullFlat::new(children)));
    union.variant.trim(usize::MAX, true);

    let _ = Box::new(union).suspend();
}

/// The allocation — and the payload's address inside it — survive the cycle.
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

/// Every child aborting takes the payload down, which is the only path to
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

/// The same teardown reached through the `Err` arm instead.
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

/// The fields outside the variant come back bit-for-bit: a wrong offset would
/// still produce a plausible-looking non-null pointer.
#[test]
fn query_string_survives_the_cycle() {
    let mock_ctx = MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    let q_str: &CStr = c"hello - world";
    let child0: Mock<'_, 2> = Mock::new([10, 30]);
    let child1: Mock<'_, 2> = Mock::new([20, 40]);

    let mut union = Box::new(UnionOpaque {
        variant: UnionVariant::HeapFull(UnionFullHeap::new(erased(child0, child1))),
        query_node_type: QueryNodeType::Prefix,
        query_string: Some(q_str),
    });
    assert_eq!(union.read().unwrap().unwrap().doc_id, 10);

    let suspended = union.suspend();
    assert_eq!(
        suspended.query_string.map(std::ptr::from_ref),
        Some(std::ptr::from_ref(q_str)),
    );

    let active = match suspended.resume(&guard).expect("resume failed") {
        ResumeOutcome::Ok(a) => a,
        ResumeOutcome::Moved(_) => panic!("expected Ok, got Moved"),
        ResumeOutcome::Aborted => panic!("expected Ok, got Aborted"),
    };
    assert_eq!(
        active.query_string.map(std::ptr::from_ref),
        Some(std::ptr::from_ref(q_str)),
    );
    assert_eq!(active.query_node_type, QueryNodeType::Prefix);
}
