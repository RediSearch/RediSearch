/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::time::Duration;

use timeout::{
    AnyTimeoutChecker, DeadlineTimeoutChecker, NoTimeoutChecker, TimeoutCheckResult, TimeoutChecker,
};

#[test]
fn no_timeout() {
    let mut ctx = NoTimeoutChecker;
    // Spin for a while, no timeout is reached
    for _ in 0..1_000 {
        assert!(matches!(ctx.check_timeout(), TimeoutCheckResult::Ok));
    }
}

#[test]
fn clock_context_does_not_time_out_within_deadline() {
    let mut ctx = DeadlineTimeoutChecker::new(Duration::from_secs(60), 1);
    for _ in 0..1_000 {
        assert!(matches!(ctx.check_timeout(), TimeoutCheckResult::Ok));
    }
}

#[test]
fn clock_context_times_out_after_deadline() {
    let mut ctx = DeadlineTimeoutChecker::new(Duration::from_nanos(1), 1);
    // Spin until the (very short) deadline passes; in practice the
    // first call already crosses it on every platform we run on.
    for _ in 0..1_000 {
        if matches!(ctx.check_timeout(), TimeoutCheckResult::TimedOut) {
            return;
        }
    }
    panic!("clock context never timed out");
}

#[test]
fn clock_context_amortizes_via_limit() {
    let mut ctx = DeadlineTimeoutChecker::new(Duration::from_nanos(1), 100);
    // With `limit = 100` the first 99 calls must not even probe the
    // clock, so they must all succeed regardless of the deadline.
    for _ in 0..99 {
        assert!(matches!(ctx.check_timeout(), TimeoutCheckResult::Ok));
    }
}

#[test]
fn clock_context_reset_counter_delays_next_check() {
    let mut ctx = DeadlineTimeoutChecker::new(Duration::from_nanos(1), 4);
    // Three increments bring the counter to 3 (below `limit`).
    for _ in 0..3 {
        assert!(matches!(ctx.check_timeout(), TimeoutCheckResult::Ok));
    }
    // Reset back to 0; the next three calls must again avoid the
    // clock check and report Ok.
    ctx.reset_counter();
    for _ in 0..3 {
        assert!(matches!(ctx.check_timeout(), TimeoutCheckResult::Ok));
    }
}

#[test]
fn any_timeout_context_dispatches_to_clock_variant() {
    let inner = DeadlineTimeoutChecker::new(Duration::from_secs(60), 1);
    let mut ctx = AnyTimeoutChecker::Deadline(inner);
    assert!(matches!(ctx.check_timeout(), TimeoutCheckResult::Ok));
    ctx.reset_counter();
    assert!(matches!(ctx.check_timeout(), TimeoutCheckResult::Ok));
}
