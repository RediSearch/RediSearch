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
    RQEIterator, RQEIteratorError, RQESuspendedIterator, ResumeOutcome, TypeErasedRQEIterator,
    c2rust::CRQEIterator, interop::RQEIteratorWrapper, intersection::Intersection,
};
use rqe_iterators_test_utils::{MockContext, ResumeOutcomeExt, revalidate_via_resume};
use rstest::rstest;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::utils::{Mock, MockData, MockRevalidateResult, drain_doc_ids};

/// Drive the C lock-release protocol on `it`: `Suspend` before the lock is dropped, then
/// `Revalidate` once it is re-acquired.
///
/// # Safety
///
/// `it` is a live iterator built by an [`RQEIteratorWrapper`] constructor, and `spec` is the spec
/// it is revalidated against.
unsafe fn suspend_then_revalidate(
    it: *mut QueryIterator,
    spec: *mut ffi::IndexSpec,
) -> ValidateStatus {
    // SAFETY: guaranteed by the caller; every wrapper constructor populates both callbacks.
    unsafe {
        ((*it).Suspend.expect("Suspend must be populated"))(it);
        ((*it).Revalidate.expect("Revalidate must be populated"))(it, spec)
    }
}

/// Call the C `Revalidate` callback on a Rust iterator lowered to the C ABI, then free it.
fn revalidate_through_c_abi(outcome: MockRevalidateResult) -> ValidateStatus {
    let ctx = MockContext::new(100, 10);
    let mock = Mock::new([1, 2, 3]);
    mock.data().set_revalidate_result(outcome);

    let it: *mut QueryIterator = RQEIteratorWrapper::boxed_new(mock);
    // SAFETY: `sctx` is a valid `RedisSearchCtx` owned by the mock context, and its `spec` is the
    // spec the iterator is revalidated against. `boxed_new` populates every callback.
    let status = unsafe { suspend_then_revalidate(it, (*ctx.sctx().as_ptr()).spec) };
    // SAFETY: `it` is the owning pointer returned by `boxed_new` and is not used afterwards.
    unsafe { ((*it).Free.expect("Free must be populated"))(it) };

    status
}

/// What a `resume` reported, read off the resumed iterator before the read guard is dropped.
#[derive(Debug, PartialEq, Eq)]
enum ResumeReport {
    Ok,
    /// A move publishes its new position in `current`, or `None` once it ran past the end.
    Moved(Option<DocId>),
    Aborted,
}

/// Drive a full `suspend` → `resume` cycle over a Rust mock lowered to the C ABI, with its
/// `Revalidate` reporting `outcome`.
fn resume_through_c_abi(
    outcome: MockRevalidateResult,
) -> (Result<ResumeReport, RQEIteratorError>, MockData) {
    let ctx = MockContext::new(100, 10);
    let guard = ctx.spec_read();

    let mock = Mock::new([1, 2, 3]);
    let data = mock.data();
    mock.data().set_revalidate_result(outcome);

    let suspended = RQEIterator::suspend(Box::new(CRQEIterator::from_rust_leaf(mock)));
    let report = RQESuspendedIterator::resume(suspended, &guard).map(|outcome| match outcome {
        ResumeOutcome::Ok(_) => ResumeReport::Ok,
        ResumeOutcome::Moved(mut it) => {
            ResumeReport::Moved(RQEIterator::current(&mut *it).map(|r| r.doc_id))
        }
        ResumeOutcome::Aborted => ResumeReport::Aborted,
    });

    (report, data)
}

#[test]
fn a_resume_that_times_out_is_reported_as_an_error() {
    let (report, data) = resume_through_c_abi(MockRevalidateResult::TimedOut);

    let err = report.expect_err(
        "`VALIDATE_TIMEOUT` is a first-class status of the C `Revalidate` contract, which every \
         Rust iterator lowered to the C ABI reports and `FT.DEBUG MOCK_REVALIDATE_TIMEOUT` can \
         force: `resume` must map it to an error, not treat it as unreachable",
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
    let (report, data) = resume_through_c_abi(MockRevalidateResult::Move);

    assert_eq!(
        report.expect("a move is a status, not an error"),
        // The mock was never read, so its move lands on the first document it holds.
        ResumeReport::Moved(Some(1)),
        "a `Moved` that does not publish the position it moved to hands its caller the stale \
         pre-suspend result instead, and a composite one level up cannot recover its own position \
         from it",
    );
    assert_eq!(
        data.drop_count(),
        1,
        "the resumed iterator is dropped by the helper, and only then: a `Moved` hands it back, so \
         it must not have been freed already",
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
    // Exhausted first, so the move has nowhere left to go.
    assert_eq!(drain_doc_ids(&mut *it), vec![1, 2, 3]);

    let suspended = RQEIterator::suspend(it);
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

    let suspended = RQEIterator::suspend(it);

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

    let suspended = RQEIterator::suspend(it);
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

    let suspended = RQEIterator::suspend(it);
    // Calling into C while suspended may read freed index memory; a counter is easier to observe.
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

    // The erased suspended form dispatches through a different vtable from the active one.
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

/// A comparable [`ResumeReport`]: [`RQEIteratorError`] is not [`PartialEq`].
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

/// Drive a `suspend` → `resume` cycle over an [`Intersection`] of two lowered mocks, with the
/// first child's `Revalidate` reporting `outcome`.
///
/// Not type-erased, so each child is re-typed in place, as in production.
fn resume_c_intersection(
    outcome: MockRevalidateResult,
) -> (Result<ResumeReport, RQEIteratorError>, [MockData; 2]) {
    let ctx = MockContext::new(100, 10);
    let guard = ctx.spec_read();

    let first = Mock::new([1, 2, 3]);
    let second = Mock::new([1, 2, 3]);
    let data = [first.data(), second.data()];
    first.data().set_revalidate_result(outcome);

    let mut ii = Box::new(Intersection::new(
        vec![
            CRQEIterator::from_rust_leaf(first),
            CRQEIterator::from_rust_leaf(second),
        ],
        1.0,
        false,
    ));
    assert_eq!(
        RQEIterator::read(&mut *ii).unwrap().map(|r| r.doc_id),
        Some(1)
    );

    let suspended = RQEIterator::suspend(ii);
    let report = RQESuspendedIterator::resume(suspended, &guard).map(|outcome| match outcome {
        ResumeOutcome::Ok(_) => ResumeReport::Ok,
        ResumeOutcome::Moved(mut it) => {
            ResumeReport::Moved(RQEIterator::current(&mut *it).map(|r| r.doc_id))
        }
        ResumeOutcome::Aborted => ResumeReport::Aborted,
    });

    (report, data)
}

#[rstest]
#[case::ok(MockRevalidateResult::Ok, FlatReport::Ok)]
// The first child steps from 1 to 2; the intersection re-aligns the second child onto it.
#[case::moved(MockRevalidateResult::Move, FlatReport::Moved(Some(2)))]
#[case::aborted(MockRevalidateResult::Abort, FlatReport::Aborted)]
#[case::timed_out(MockRevalidateResult::TimedOut, FlatReport::TimedOut)]
fn an_intersection_of_c_iterators_resumes_with_its_childs_outcome(
    #[case] outcome: MockRevalidateResult,
    #[case] expected: FlatReport,
) {
    let (report, data) = resume_c_intersection(outcome);

    assert_eq!(flatten(report), expected);
    for (i, child) in data.iter().enumerate() {
        assert_eq!(
            child.drop_count(),
            1,
            "child {i} must be freed exactly once, whether the resume handed the intersection back \
             or consumed it",
        );
    }
}

/// Number of times [`count_child_num_estimated_calls`] was invoked.
static CHILD_NUM_ESTIMATED_CALLS: AtomicUsize = AtomicUsize::new(0);

/// Stand-in `NumEstimated` callback for the children of an intersection.
extern "C" fn count_child_num_estimated_calls(_it: *const QueryIterator) -> usize {
    CHILD_NUM_ESTIMATED_CALLS.fetch_add(1, Ordering::Relaxed);
    0
}

#[test]
fn suspending_an_intersection_snapshots_each_c_childs_estimate() {
    let first = CRQEIterator::from_rust_leaf(Mock::new([1, 2, 3]));
    let second = CRQEIterator::from_rust_leaf(Mock::new([1, 2, 3]));
    let headers = [first.as_raw(), second.as_raw()];
    let ii = Box::new(Intersection::new(vec![first, second], 1.0, false));
    // Patched after construction, which sorts the children by estimate.
    for header in headers {
        // SAFETY: the intersection owns the header and no reference into it is live across this
        // call.
        unsafe {
            rqe_iterators::interop::patch_vtable(header.as_ptr(), |h| {
                h.NumEstimated = Some(count_child_num_estimated_calls)
            })
        };
    }

    let suspended = RQEIterator::suspend(ii);
    let after_suspend = CHILD_NUM_ESTIMATED_CALLS.load(Ordering::Relaxed);
    let _ = RQESuspendedIterator::num_estimated(&*suspended);

    assert_eq!(
        after_suspend, 2,
        "each child has to snapshot its estimate while the lock is still held, which means the \
         in-place slot re-typing must dispatch to the child's own `suspend`",
    );
    assert_eq!(
        CHILD_NUM_ESTIMATED_CALLS.load(Ordering::Relaxed),
        after_suspend,
        "a suspended intersection must not ask its C children for an estimate",
    );
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
    let status = unsafe { suspend_then_revalidate(it, (*ctx.sctx().as_ptr()).spec) };
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
    let status = unsafe { suspend_then_revalidate(it, (*ctx.sctx().as_ptr()).spec) };
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

/// Print the profile of a Rust iterator through its C `PrintProfile` callback and capture the
/// single reply it writes.
///
/// # Safety
///
/// `it` is a live iterator built by an [`RQEIteratorWrapper`] constructor.
#[cfg(not(miri))]
unsafe fn print_profile_through_c_abi(it: *mut QueryIterator) -> redis_mock::reply::ReplyValue {
    redis_mock::init_redis_module_mock();
    // SAFETY: the mock replaces every `RedisModule_Reply*` function with one that records the reply
    // without dereferencing the context, so a dangling non-null context is never read.
    let mut replier = unsafe {
        redis_reply::Replier::new(std::ptr::dangling_mut::<redis_reply::RedisModuleCtx>())
    };
    let mut replies = redis_mock::reply::capture_replies(|| {
        let mut ctx = rqe_iterators::profile_print::ProfilePrintCtx::new(false, false);
        let mut map = replier.map();
        let it = std::ptr::NonNull::new(it).expect("the wrapper is non-null");
        // SAFETY: guaranteed by the caller; every wrapper constructor populates `PrintProfile`.
        unsafe { rqe_iterators::c2rust::call_print_profile(it, &mut map, &mut ctx) };
    });
    assert_eq!(replies.len(), 1, "a profile is a single map reply");
    replies.pop().unwrap()
}

/// The profile is printed after the query has released its read lock, so by then the iterator
/// tree is suspended. Printing it must not require resuming it, and must report the same profile
/// as the active iterator did.
#[cfg(not(miri))]
#[test]
fn a_suspended_wrapper_prints_the_profile_of_its_active_form() {
    let it: *mut QueryIterator = RQEIteratorWrapper::boxed_new(Mock::new([1, 2, 3]));

    // SAFETY: `it` was just built by `boxed_new`.
    let active = unsafe { print_profile_through_c_abi(it) };
    // SAFETY: as above; `boxed_new` populates `Suspend`.
    unsafe { ((*it).Suspend.expect("Suspend must be populated"))(it) };
    // SAFETY: as above.
    let suspended = unsafe { print_profile_through_c_abi(it) };
    // SAFETY: `it` is the owning pointer returned by `boxed_new` and is not used afterwards.
    unsafe { ((*it).Free.expect("Free must be populated"))(it) };

    assert_eq!(suspended, active);
    insta::assert_debug_snapshot!(suspended);
}

/// Pipelines may stack several unlock sites that each suspend the root iterator before releasing
/// the read lock, so a `Suspend` on an already suspended wrapper must leave it untouched.
#[test]
fn a_second_suspend_is_a_no_op() {
    let ctx = MockContext::new(100, 10);
    let mock = Mock::new([1, 2, 3]);
    let data = mock.data();
    let it: *mut QueryIterator = RQEIteratorWrapper::boxed_new(mock);

    // SAFETY: `it` was just built by `boxed_new`, which populates every callback, and `sctx`
    // carries the spec it is revalidated against.
    unsafe {
        ((*it).Suspend.expect("Suspend must be populated"))(it);
        assert_eq!(
            suspend_then_revalidate(it, (*ctx.sctx().as_ptr()).spec),
            ValidateStatus_VALIDATE_OK
        );
    }
    assert_eq!(
        data.revalidate_count(),
        1,
        "two suspends and one revalidate make a single cycle"
    );
    // SAFETY: `it` is the owning pointer returned by `boxed_new` and is not used afterwards.
    unsafe { ((*it).Free.expect("Free must be populated"))(it) };
}

/// Not every lock re-acquisition is preceded by a `Suspend`, and a freshly built iterator was never
/// suspended at all, so a `Revalidate` on an active wrapper has to run the whole cycle rather than
/// trust borrows that may have gone stale while the lock was released.
#[test]
fn revalidating_an_active_wrapper_runs_a_full_cycle() {
    let ctx = MockContext::new(100, 10);
    let mock = Mock::new([1, 2, 3]);
    let data = mock.data();
    let it: *mut QueryIterator = RQEIteratorWrapper::boxed_new(mock);

    // SAFETY: as in `a_second_suspend_is_a_no_op`.
    let status = unsafe {
        ((*it).Revalidate.expect("Revalidate must be populated"))(it, (*ctx.sctx().as_ptr()).spec)
    };

    assert_eq!(status, ValidateStatus_VALIDATE_OK);
    assert_eq!(
        data.revalidate_count(),
        1,
        "the iterator must have been resumed"
    );
    // SAFETY: `it` is the owning pointer returned by `boxed_new` and is not used afterwards.
    unsafe { ((*it).Free.expect("Free must be populated"))(it) };
}

/// Number of times [`count_suspend_calls`] was invoked.
static SUSPEND_CALLS: AtomicUsize = AtomicUsize::new(0);

/// Stand-in `Suspend` callback: records that it was called.
extern "C" fn count_suspend_calls(_it: *mut QueryIterator) {
    SUSPEND_CALLS.fetch_add(1, Ordering::Relaxed);
}

/// A C iterator may hold lock-dependent state of its own, and a Rust iterator lowered to the C ABI
/// only changes state through its `Suspend` callback, so suspending the handle has to reach it.
#[test]
fn suspending_a_c_iterator_forwards_to_its_suspend_callback() {
    let it = Box::new(CRQEIterator::from_rust_leaf(Mock::new([1, 2, 3])));
    // SAFETY: `it` owns the header and no reference into it is live across this call.
    unsafe {
        rqe_iterators::interop::patch_vtable(it.as_raw().as_ptr(), |h| {
            h.Suspend = Some(count_suspend_calls)
        })
    };

    let suspended = it.suspend();

    assert_eq!(SUSPEND_CALLS.load(Ordering::Relaxed), 1);
    drop(suspended);
}

/// `FT.PROFILE` asks for the estimate after the query has released its read lock, by which time
/// the iterator tree is suspended, so the callback has to answer in that state too.
#[test]
fn a_suspended_wrapper_still_reports_its_estimate() {
    let it: *mut QueryIterator = RQEIteratorWrapper::boxed_new(Mock::new([1, 2, 3]));

    // SAFETY: `it` was just built by `boxed_new`, which populates every callback.
    unsafe {
        let num_estimated = (*it).NumEstimated.expect("NumEstimated must be populated");
        let active = num_estimated(it);
        ((*it).Suspend.expect("Suspend must be populated"))(it);
        assert_eq!(num_estimated(it), active);
        ((*it).Free.expect("Free must be populated"))(it);
    }
}

/// An exhausted iterator publishes no result, and C tells it apart from one sitting on a document
/// by `current` being NULL with `atEOF` set. Revalidate re-derives the header from the resumed
/// iterator, so that has to survive the cycle, along with the last position.
#[test]
fn an_exhausted_wrapper_stays_exhausted_across_a_cycle() {
    let ctx = MockContext::new(100, 10);
    let it: *mut QueryIterator = RQEIteratorWrapper::boxed_new(Mock::new([1, 2]));

    // SAFETY: `it` was just built by `boxed_new`, which populates every callback, and `sctx`
    // carries the spec it is revalidated against.
    unsafe {
        let read = (*it).Read.expect("Read must be populated");
        while read(it) == ffi::IteratorStatus_ITERATOR_OK {}
        assert!((*it).current.is_null() && (*it).atEOF);

        assert_eq!(
            suspend_then_revalidate(it, (*ctx.sctx().as_ptr()).spec),
            ValidateStatus_VALIDATE_OK
        );
        assert!(
            (*it).current.is_null(),
            "an exhausted iterator publishes no result"
        );
        assert!((*it).atEOF);
        assert_eq!((*it).lastDocId, 2, "the last position survives the cycle");
        ((*it).Free.expect("Free must be populated"))(it);
    }
}
