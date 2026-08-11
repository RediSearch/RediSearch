/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the revalidation status crossing the C ABI, in both directions.
//!
//! A revalidation that fails is unrecoverable either way, so C frees the iterator whichever error
//! it was. What the status has to preserve is the *report*: a timeout leaves the result set partial
//! and has to reach the client, while an abort lets the query end as if the index were exhausted.
//! Collapsing the two, as this boundary used to, turns a timed-out query into a successful one.

use ffi::{
    QueryIterator, ValidateStatus, ValidateStatus_VALIDATE_ABORTED, ValidateStatus_VALIDATE_OK,
    ValidateStatus_VALIDATE_TIMEOUT,
};
use rqe_core::DocId;
use rqe_iterators::{
    RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator, RQEValidateStatus,
    ResumeOutcome, TypeErasedRQEIterator, c2rust::CRQEIterator, interop::RQEIteratorWrapper,
    intersection::Intersection,
};
use rqe_iterators_test_utils::{MockContext, ResumeOutcomeExt, revalidate_via_resume};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::utils::{Mock, MockData, MockRevalidateResult, drain_doc_ids};

/// Call the C `Revalidate` callback on a Rust iterator lowered to the C ABI, then free it.
fn revalidate_through_c_abi(outcome: MockRevalidateResult) -> ValidateStatus {
    let ctx = MockContext::new(100, 10);
    let mock = Mock::new([1, 2, 3]);
    mock.data().set_revalidate_result(outcome);

    let it: *mut QueryIterator = RQEIteratorWrapper::boxed_new(mock);
    // SAFETY: `sctx` is a valid `RedisSearchCtx` owned by the mock context, and its `spec` is the
    // spec the iterator is revalidated against. `boxed_new` populates every callback.
    let status = unsafe {
        let spec = (*ctx.sctx().as_ptr()).spec;
        let revalidate = (*it).Revalidate.expect("Revalidate must be populated");
        revalidate(it, spec)
    };
    // SAFETY: `it` is the owning pointer returned by `boxed_new` and is not used afterwards.
    unsafe { ((*it).Free.expect("Free must be populated"))(it) };

    status
}

/// What a `resume` reported, flattened so it can be inspected once the read guard is gone.
///
/// [`CRQEIterator`] carries no lifetime, so the resumed iterator would outlive the guard it was
/// resumed under; the helpers below read everything a test needs off it and drop it while the
/// context is still alive.
#[derive(Debug, PartialEq, Eq)]
enum ResumeReport {
    Ok,
    /// A move publishes its new position in `current`, or `None` once it ran past the end.
    Moved(Option<DocId>),
    Aborted,
}

/// Drive a full `suspend` → `resume` cycle over a Rust mock lowered to the C ABI.
///
/// The suspended `CRQEIterator` has no Rust-side state of its own, so its `resume` is a plain call
/// into the C `Revalidate` vtable entry — the same callback `revalidate_through_c_abi` exercises.
/// Returning the mock's [`MockData`] lets a caller assert on the disposal that only this path
/// performs: `resume` consumes the suspended iterator, so an `Aborted` or `Err` has to free it.
fn resume_through_c_abi(
    outcome: MockRevalidateResult,
) -> (Result<ResumeReport, RQEIteratorError>, MockData) {
    let ctx = MockContext::new(100, 10);
    let guard = ctx.spec_read();

    let mock = Mock::new([1, 2, 3]);
    let data = mock.data();
    mock.data().set_revalidate_result(outcome);

    let suspended = RQEIteratorBoxed::suspend(Box::new(CRQEIterator::from_rust_leaf(mock)));
    let report = RQESuspendedIterator::resume(suspended, &guard).map(|outcome| match outcome {
        ResumeOutcome::Ok(_) => ResumeReport::Ok,
        ResumeOutcome::Moved(mut it) => {
            ResumeReport::Moved(RQEIterator::current(&mut *it).map(|r| r.doc_id))
        }
        ResumeOutcome::Aborted => ResumeReport::Aborted,
    });

    (report, data)
}

/// Drive the legacy [`RQEIterator::revalidate`] over the same lowered mock, reported the same way,
/// so the two revalidation paths can be diffed against each other.
fn revalidate_c_iterator(
    outcome: MockRevalidateResult,
) -> (Result<ResumeReport, RQEIteratorError>, MockData) {
    let ctx = MockContext::new(100, 10);
    let guard = ctx.spec_read();

    let mock = Mock::new([1, 2, 3]);
    let data = mock.data();
    mock.data().set_revalidate_result(outcome);

    let mut it = CRQEIterator::from_rust_leaf(mock);
    let report = it.revalidate(&guard).map(|status| match status {
        RQEValidateStatus::Ok => ResumeReport::Ok,
        RQEValidateStatus::Moved { current } => ResumeReport::Moved(current.map(|r| r.doc_id)),
        RQEValidateStatus::Aborted => ResumeReport::Aborted,
    });

    (report, data)
}

/// The two revalidation paths report failures as [`RQEIteratorError`], which is not comparable
/// (it wraps an [`std::io::Error`]). Fold both into one comparable value.
#[derive(Debug, PartialEq, Eq)]
enum FlatReport {
    Ok,
    Moved(Option<DocId>),
    Aborted,
    TimedOut,
    IoError,
}

fn flatten(report: Result<ResumeReport, RQEIteratorError>) -> FlatReport {
    match report {
        Ok(ResumeReport::Ok) => FlatReport::Ok,
        Ok(ResumeReport::Moved(position)) => FlatReport::Moved(position),
        Ok(ResumeReport::Aborted) => FlatReport::Aborted,
        Err(RQEIteratorError::TimedOut) => FlatReport::TimedOut,
        Err(RQEIteratorError::IoError(_)) => FlatReport::IoError,
    }
}

#[test]
fn a_resume_that_times_out_is_reported_as_an_error() {
    let (report, data) = resume_through_c_abi(MockRevalidateResult::TimedOut);

    let err = report.expect_err(
        "`VALIDATE_TIMEOUT` is a first-class status of the C `Revalidate` contract, which every \
         Rust iterator lowered to the C ABI reports and `FT.DEBUG MOCK_REVALIDATE_TIMEOUT` can \
         force: `resume` must map it to an error like `revalidate` does, not treat it as \
         unreachable",
    );
    assert!(
        matches!(err, RQEIteratorError::TimedOut),
        "expected a timeout, got {err:?}: reported as an abort instead, the query would end as if \
         the index were exhausted and hand the client a truncated result set labelled complete",
    );
    assert_eq!(
        data.drop_count(),
        1,
        "an `Err` consumes the suspended iterator, so its `Free` callback must run exactly once",
    );
}

#[test]
fn a_resume_that_fails_to_read_the_index_is_reported_as_an_abort() {
    let (report, data) = resume_through_c_abi(MockRevalidateResult::IoError);

    assert_eq!(
        report.expect("an I/O error has no C status of its own, so it arrives as an abort"),
        ResumeReport::Aborted,
        "the C `Revalidate` contract has no I/O status: the error is deliberately collapsed into \
         `VALIDATE_ABORTED` on the way out and cannot be recovered on the way back in",
    );
    assert_eq!(
        data.drop_count(),
        1,
        "`Aborted` materializes no active iterator, so the suspended one is freed here - exactly \
         once, since nothing else holds it",
    );
}

#[test]
fn an_aborted_resume_is_reported_as_an_abort() {
    let (report, data) = resume_through_c_abi(MockRevalidateResult::Abort);

    assert_eq!(
        report.expect("an abort is a status, not an error"),
        ResumeReport::Aborted
    );
    assert_eq!(
        data.drop_count(),
        1,
        "the C `Revalidate` reports the abort and leaves disposal to the owner, so `resume` has to \
         free the iterator it consumed - once, not twice",
    );
}

#[test]
fn a_successful_resume_hands_back_the_iterator() {
    let (report, data) = resume_through_c_abi(MockRevalidateResult::Ok);

    assert_eq!(
        report.expect("a successful resume is not an error"),
        ResumeReport::Ok
    );
    assert_eq!(
        data.drop_count(),
        1,
        "the resumed iterator is dropped by the helper, and only then: an outcome that hands one \
         back must not have freed it already",
    );
}

#[test]
fn a_resume_that_moves_publishes_its_new_position() {
    let (report, _) = resume_through_c_abi(MockRevalidateResult::Move);

    assert_eq!(
        report.expect("a move is a status, not an error"),
        // The mock was never read, so its move lands on the first document it holds.
        ResumeReport::Moved(Some(1)),
        "a `Moved` that does not publish the position it moved to hands its caller the stale \
         pre-suspend result instead, and a composite one level up cannot recover its own position \
         from it",
    );
}

// Relies on nextest's process-per-test isolation, as the legacy twin above does: the switch is
// process-global, so a shared-process runner could leak it into another test's resume.
#[cfg(not(miri))]
#[test]
fn the_debug_switch_makes_a_resume_time_out_without_consulting_the_iterator() {
    let ctx = MockContext::new(100, 10);
    let guard = ctx.spec_read();
    let mock = Mock::new([1, 2, 3]);
    let data = mock.data();
    let suspended = RQEIteratorBoxed::suspend(Box::new(CRQEIterator::from_rust_leaf(mock)));

    rqe_iterators::interop::set_mock_revalidate_timeout(true);
    let report = RQESuspendedIterator::resume(suspended, &guard);
    rqe_iterators::interop::set_mock_revalidate_timeout(false);

    let err = report.err().expect(
        "this switch is how the flow tests drive a revalidation timeout, so `resume` has to survive \
         being handed one",
    );
    assert!(
        matches!(err, RQEIteratorError::TimedOut),
        "expected a timeout, got {err:?}"
    );
    assert_eq!(
        data.revalidate_count(),
        0,
        "the switch stands in for an expired deadline, so it must short-circuit rather than let \
         the iterator revalidate and then discard the outcome",
    );
    assert_eq!(
        data.drop_count(),
        1,
        "the error consumed the suspended iterator"
    );
}

#[test]
fn a_resume_that_moves_past_the_end_reports_no_position() {
    let ctx = MockContext::new(100, 10);
    let guard = ctx.spec_read();

    let mock = Mock::new([1, 2, 3]);
    mock.data()
        .set_revalidate_result(MockRevalidateResult::Move);
    let mut it = Box::new(CRQEIterator::from_rust_leaf(mock));
    // Exhaust the mock first, so its move has nowhere left to go. `Moved` then carries no
    // position, which is a distinct outcome from `Moved(Some(_))`: it travels through the C ABI as
    // `atEOF` plus a NULL `current` rather than as a fresh result pointer.
    assert_eq!(drain_doc_ids(&mut *it), vec![1, 2, 3]);

    let suspended = RQEIteratorBoxed::suspend(it);
    let mut resumed = match RQESuspendedIterator::resume(suspended, &guard) {
        Ok(ResumeOutcome::Moved(it)) => it,
        other => panic!(
            "expected a move, got {:?}",
            other.map(|_| "a different outcome")
        ),
    };

    assert_eq!(
        RQEIterator::current(&mut *resumed).map(|r| r.doc_id),
        None,
        "a move past the end must not keep answering with the last result the iterator yielded: a \
         parent that reads a position here would re-emit a document the child no longer holds",
    );
    assert!(
        RQEIterator::at_eof(&*resumed),
        "the C side records the exhaustion in `atEOF`, which is what a parent checks before \
         reading the child again",
    );
}

#[test]
fn a_suspended_iterator_reports_the_position_it_was_suspended_at() {
    let mut it = Box::new(CRQEIterator::from_rust_leaf(Mock::new([1, 2, 3])));
    assert_eq!(
        RQEIterator::read(&mut *it).unwrap().map(|r| r.doc_id),
        Some(1)
    );

    let suspended = RQEIteratorBoxed::suspend(it);

    assert_eq!(
        RQESuspendedIterator::last_doc_id(&*suspended),
        1,
        "a composite decides where to re-seek its children from the positions they report while \
         suspended, so the suspended accessor has to carry the pre-suspend position over rather \
         than reset it",
    );
}

#[test]
fn suspend_and_resume_leave_the_iterator_where_c_cached_it() {
    let ctx = MockContext::new(100, 10);
    let guard = ctx.spec_read();

    let it = Box::new(CRQEIterator::from_rust_leaf(Mock::new([1, 2, 3])));
    let header = it.as_raw();
    let handle = std::ptr::from_ref(&*it);

    let suspended = RQEIteratorBoxed::suspend(it);
    assert_eq!(
        suspended.as_raw(),
        header,
        "the C side caches pointers into the `QueryIterator` allocation (`header.current` among \
         them), so suspend must hand the same allocation on rather than rebuild it",
    );

    let resumed = match RQESuspendedIterator::resume(suspended, &guard) {
        Ok(ResumeOutcome::Ok(it)) => it,
        other => panic!(
            "expected a plain resume, got {:?}",
            other.map(|_| "a different outcome")
        ),
    };
    assert_eq!(
        resumed.as_raw(),
        header,
        "resume must give the same allocation back"
    );
    assert_eq!(
        std::ptr::from_ref(&*resumed),
        handle,
        "the box holding the handle must not move either: a composite that suspended this child \
         in place expects to find it in the same slot",
    );
}

/// Number of times [`count_num_estimated_calls`] was invoked.
static NUM_ESTIMATED_CALLS: AtomicUsize = AtomicUsize::new(0);

/// Stand-in `NumEstimated` callback: records that it was called at all.
extern "C" fn count_num_estimated_calls(_it: *const QueryIterator) -> usize {
    NUM_ESTIMATED_CALLS.fetch_add(1, Ordering::Relaxed);
    usize::MAX
}

#[test]
fn a_suspended_iterator_serves_a_snapshot_rather_than_calling_into_c() {
    let mock = Mock::new([1, 2, 3]);
    let it = Box::new(CRQEIterator::from_rust_leaf(mock));
    let estimate = RQEIterator::num_estimated(&*it);

    let suspended = RQEIteratorBoxed::suspend(it);
    // Standing in for the hazard this guards against: while suspended, the spec read lock is
    // released, so the C `NumEstimated` callbacks may be reading index memory that GC has freed
    // since - `RQEIteratorWrapper`'s forwards to the active Rust iterator underneath, and the
    // hybrid and optimizer readers recurse into their children the same way. A callback that must
    // not be called is easier to observe than a use-after-free.
    // SAFETY: `suspended` owns the header and no reference into it is live across this call.
    unsafe {
        rqe_iterators::interop::patch_vtable(suspended.as_raw().as_ptr(), |h| {
            h.NumEstimated = Some(count_num_estimated_calls)
        })
    };

    assert_eq!(
        RQESuspendedIterator::num_estimated(&*suspended),
        estimate,
        "the suspended form has to answer from the snapshot `suspend` took while the lock was still \
         held",
    );
    assert_eq!(
        NUM_ESTIMATED_CALLS.load(Ordering::Relaxed),
        0,
        "asking the C side again is what makes this a use-after-free waiting to happen; the \
         estimate is display-only, so a snapshot is the whole point of the suspended accessor",
    );
}

#[test]
fn a_c_iterator_resumes_through_the_type_erased_bridge() {
    let ctx = MockContext::new(100, 10);
    let guard = ctx.spec_read();
    let mock = Mock::new([1, 2, 3]);
    let data = mock.data();

    // The erased bridge is the path composites take: their children are `TypeErasedRQEIterator`s,
    // whose suspended form is a *different* `Box<dyn …>` with a different vtable.
    let it = TypeErasedRQEIterator::new(Box::new(CRQEIterator::from_rust_leaf(mock)));
    let mut resumed = revalidate_via_resume(it, &guard)
        .expect("a successful resume is not an error")
        .expect_ok();

    assert_eq!(
        data.revalidate_count(),
        1,
        "the resume must reach the iterator underneath"
    );
    assert_eq!(
        drain_doc_ids(&mut resumed),
        vec![1, 2, 3],
        "the resumed iterator has to be usable: dispatching through the suspended vtable instead \
         of the active one is undefined behaviour that a mere round-trip would not notice",
    );
}

#[test]
fn a_timed_out_resume_through_the_type_erased_bridge_stays_a_timeout() {
    let ctx = MockContext::new(100, 10);
    let guard = ctx.spec_read();
    let mock = Mock::new([1, 2, 3]);
    let data = mock.data();
    mock.data()
        .set_revalidate_result(MockRevalidateResult::TimedOut);

    let it = TypeErasedRQEIterator::new(Box::new(CRQEIterator::from_rust_leaf(mock)));
    let err = revalidate_via_resume(it, &guard)
        .err()
        .expect("the erased bridge forwards the error rather than swallowing it");

    assert!(
        matches!(err, RQEIteratorError::TimedOut),
        "expected a timeout, got {err:?}"
    );
    assert_eq!(
        data.drop_count(),
        1,
        "the error consumed the erased suspended iterator, which owns the C one underneath",
    );
}

#[test]
fn revalidate_and_resume_report_the_same_outcome() {
    for outcome in [
        MockRevalidateResult::Ok,
        MockRevalidateResult::Move,
        MockRevalidateResult::Abort,
        MockRevalidateResult::TimedOut,
        MockRevalidateResult::IoError,
    ] {
        let (legacy, _) = revalidate_c_iterator(outcome);
        let (resumed, _) = resume_through_c_abi(outcome);

        assert_eq!(
            flatten(resumed),
            flatten(legacy),
            "`resume` supersedes `revalidate` on the same C `Revalidate` callback, so for {outcome:?} \
             the two have to agree: while both paths are live, any divergence is a bug in whichever \
             one the caller happens to take",
        );
    }
}

#[test]
fn timed_out_revalidation_is_reported_as_a_timeout() {
    assert_eq!(
        revalidate_through_c_abi(MockRevalidateResult::TimedOut),
        ValidateStatus_VALIDATE_TIMEOUT,
        "a deadline that expires mid-revalidation must reach C as a timeout: reported as an abort, \
         the query ends as if the index were exhausted and the client is handed a truncated result \
         set labelled complete",
    );
}

#[test]
fn failed_revalidation_is_reported_as_an_abort() {
    assert_eq!(
        revalidate_through_c_abi(MockRevalidateResult::IoError),
        ValidateStatus_VALIDATE_ABORTED,
        "an I/O error kills the subtree in a query that still has time left, so it stays an abort",
    );
}

#[test]
fn aborted_revalidation_is_reported_as_an_abort() {
    assert_eq!(
        revalidate_through_c_abi(MockRevalidateResult::Abort),
        ValidateStatus_VALIDATE_ABORTED,
    );
}

#[test]
fn successful_revalidation_is_reported_as_ok() {
    assert_eq!(
        revalidate_through_c_abi(MockRevalidateResult::Ok),
        ValidateStatus_VALIDATE_OK,
    );
}

#[test]
fn a_c_child_that_times_out_hands_its_rust_parent_an_error() {
    let ctx = MockContext::new(100, 10);
    let guard = ctx.spec_read();

    let mock = Mock::new([1, 2, 3]);
    mock.data()
        .set_revalidate_result(MockRevalidateResult::TimedOut);
    // Lowering the mock to the C ABI and adopting it back puts both mappings in the path, so this
    // covers the round trip: `Err(TimedOut)` -> `VALIDATE_TIMEOUT` -> `Err(TimedOut)`.
    let mut child = CRQEIterator::from_rust_leaf(mock);

    let err = child
        .revalidate(&guard)
        .expect_err("a timed-out child must surface as an error, not as a status");
    assert!(
        matches!(err, RQEIteratorError::TimedOut),
        "expected a timeout, got {err:?}: a Rust composite propagates this to the root of the \
         tree, exactly as it does for a child that times out during a read or a skip",
    );
}

#[test]
fn a_child_timeout_reaches_c_as_a_timeout_through_a_composite() {
    let ctx = MockContext::new(100, 10);
    let healthy = Mock::new([1, 2, 3]);
    let timing_out = Mock::new([1, 2, 3]);
    timing_out
        .data()
        .set_revalidate_result(MockRevalidateResult::TimedOut);

    // Children go through the C ABI, as they do in a real query plan, so the child's timeout is
    // mapped twice on its way up: to `VALIDATE_TIMEOUT` and back to an error for the parent.
    let intersection = Intersection::new(
        vec![
            CRQEIterator::from_rust_leaf(healthy),
            CRQEIterator::from_rust_leaf(timing_out),
        ],
        1.0,
        false,
    );
    let it: *mut QueryIterator = RQEIteratorWrapper::boxed_new_compound(intersection);

    // SAFETY: as in `revalidate_through_c_abi`.
    let status = unsafe {
        let spec = (*ctx.sctx().as_ptr()).spec;
        ((*it).Revalidate.expect("Revalidate must be populated"))(it, spec)
    };
    // SAFETY: `it` is the owning pointer returned by `boxed_new_compound` and is not used after.
    unsafe { ((*it).Free.expect("Free must be populated"))(it) };

    assert_eq!(
        status, ValidateStatus_VALIDATE_TIMEOUT,
        "a composite propagates a child's failure with `?`, so the timeout has to survive the whole \
         way up to the root of the tree - that is the one iterator C revalidates",
    );
}

// Relies on nextest's process-per-test isolation: the switch is process-global, so a shared-process
// runner could leak it into another test's revalidation.
#[cfg(not(miri))]
#[test]
fn the_debug_switch_reports_a_timeout_without_consulting_the_iterator() {
    let ctx = MockContext::new(100, 10);
    let mock = Mock::new([1, 2, 3]);
    let data = mock.data();
    let it: *mut QueryIterator = RQEIteratorWrapper::boxed_new(mock);

    rqe_iterators::interop::set_mock_revalidate_timeout(true);
    // SAFETY: as in `revalidate_through_c_abi`.
    let status = unsafe {
        let spec = (*ctx.sctx().as_ptr()).spec;
        ((*it).Revalidate.expect("Revalidate must be populated"))(it, spec)
    };
    rqe_iterators::interop::set_mock_revalidate_timeout(false);
    // SAFETY: `it` is the owning pointer returned by `boxed_new` and is not used afterwards.
    unsafe { ((*it).Free.expect("Free must be populated"))(it) };

    assert_eq!(status, ValidateStatus_VALIDATE_TIMEOUT);
    assert_eq!(
        data.revalidate_count(),
        0,
        "the switch stands in for an expired deadline, so it must short-circuit rather than let \
         the iterator revalidate and then discard the outcome",
    );
}
