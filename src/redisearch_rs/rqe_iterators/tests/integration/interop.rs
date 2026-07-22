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
use rqe_iterators::{
    RQEIterator, RQEIteratorError, c2rust::CRQEIterator, interop::RQEIteratorWrapper,
    intersection::Intersection,
};
use rqe_iterators_test_utils::MockContext;

use crate::utils::{Mock, MockRevalidateResult};

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
