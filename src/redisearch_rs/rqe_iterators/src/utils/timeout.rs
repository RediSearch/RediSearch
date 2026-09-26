/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ffi::{QueryIterator_IsBlockedClientTimedOut, QueryRequestTimeout, RedisSearchCtx};
pub use timeout::{DeadlineTimeoutChecker, NoTimeoutChecker, TimeoutCheckResult, TimeoutChecker};

use crate::{RQEIteratorError, utils::timespec::deadline_passed};

/// Abstraction over the different ways a query iterator can detect that the
/// surrounding query has run out of time.
///
/// Concrete checkers include:
/// * [`NoTimeoutChecker`] — zero-sized no-op used when the query has no deadline.
/// * [`TimeoutContextDeadline`] — Clock Based Timeout: amortized clock check against the
///   deadline owned by the query's search context, used when no Blocked Client Timeout is in
///   play. ([`DeadlineTimeoutChecker`] is the same check against a deadline captured up front;
///   it is not what a query iterator gets, because a query's deadline moves — see
///   [`TimeoutContextDeadline`].)
/// * [`TimeoutContextRequest`] — reads the request's active timeout source on each probe,
///   including its blocked-client flag.
///
/// Iterators are generic over this trait so the dispatch is monomorphized
/// in the hot path.
pub trait TimeoutContext {
    /// Report whether the query has timed out.
    ///
    /// Returns [`RQEIteratorError::TimedOut`] when the deadline has been
    /// reached (or, for externally-signalled variants, when the signal has
    /// flipped). Otherwise returns `Ok(())`.
    ///
    /// Implementations are allowed (and encouraged) to amortize the actual
    /// check across many calls.
    fn check_timeout(&mut self) -> Result<(), RQEIteratorError>;

    /// Hook invoked by callers after a unit of useful work has been
    /// completed, so amortized implementations can reset their internal
    /// counter without losing accuracy.
    ///
    /// The default implementation is a no-op, which is the right behavior
    /// for variants that do not maintain any internal counter (such as
    /// [`NoTimeoutChecker`]).
    fn reset_counter(&mut self);
}

impl<TC: TimeoutChecker> TimeoutContext for TC {
    fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
        let res = TimeoutChecker::check_timeout(self);
        match res {
            TimeoutCheckResult::Ok => Ok(()),
            TimeoutCheckResult::TimedOut => Err(RQEIteratorError::TimedOut),
        }
    }

    fn reset_counter(&mut self) {
        TimeoutChecker::reset_counter(self)
    }
}

/// [`TimeoutContext`] backed by the request deadline borrowed through a query's
/// [`RedisSearchCtx`].
///
/// The deadline is *read through the pointer on every probe* rather than captured once. That
/// matters because the deadline moves: `runCursor` starts a new clock cycle before each cursor
/// read, giving the read its own budget. An iterator tree, by contrast, is built once and reused
/// for the whole life of the cursor, so a captured deadline is the one belonging to the *first*
/// read, and every later read starts out already expired against it — the iterators would report a
/// timeout for a deadline the pipeline around them had just extended.
///
/// Like [`TimeoutContextRequest`], the pointer carries no lifetime; keeping the search
/// context and its request timeout alive is a runtime invariant the caller upholds, documented on
/// [`new`](Self::new).
///
/// Probing still costs a clock read, so it is amortized the same way
/// [`DeadlineTimeoutChecker`] amortizes: only every `limit`-th call looks at the clock.
pub struct TimeoutContextDeadline {
    /// Deadline owned by the query request, re-read on every probe.
    deadline: NonNull<ffi::timespec>,
    /// Calls since the last clock probe.
    counter: u32,
    /// Probe the clock once every `limit` calls.
    limit: u32,
}

impl TimeoutContextDeadline {
    /// Build a context reading the deadline at `deadline`.
    ///
    /// # Safety
    ///
    /// * `deadline` must point to stable storage for a [`timespec`](ffi::timespec) that stays
    ///   [valid] for as long as this context (and any iterator holding it) is used. If the
    ///   storage is part of a union, callers may probe this checker only while the deadline
    ///   is the active member.
    /// * The deadline must not be written concurrently with a probe. C *does* write to it — that
    ///   is the point of reading it back — when a clock cycle begins, and directly from
    ///   `RPTimeoutAfterCount_SimulateTimeout`. Each of those either runs before the
    ///   pipeline for that read starts (`runCursor`, `buildPipelineAndExecute`, the hybrid and
    ///   coordinator entry points) or runs inside the pipeline on the thread that would be
    ///   probing, so no write overlaps a probe. A new write site has to preserve that.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    #[inline(always)]
    pub const unsafe fn new(deadline: NonNull<ffi::timespec>, limit: u32) -> Self {
        Self {
            deadline,
            counter: 0,
            limit,
        }
    }
}

impl TimeoutChecker for TimeoutContextDeadline {
    #[inline(always)]
    fn check_timeout(&mut self) -> TimeoutCheckResult {
        self.counter += 1;
        if self.counter < self.limit {
            return TimeoutCheckResult::Ok;
        }
        self.counter = 0;

        // SAFETY: the constructor contract guarantees `deadline` points to a valid `timespec` that
        // outlives this context, and that no write to it overlaps this read.
        let deadline = unsafe { self.deadline.read() };
        if deadline_passed(deadline) {
            TimeoutCheckResult::TimedOut
        } else {
            TimeoutCheckResult::Ok
        }
    }

    #[inline(always)]
    fn reset_counter(&mut self) {
        self.counter = 0;
    }
}

/// Request-owned timeout state retained across cursor reads.
///
/// The pointer is private because it may only be set by the unsafe constructor, which requires
/// the request to outlive every probe.
pub struct TimeoutContextRequest {
    timeout: NonNull<QueryRequestTimeout>,
    clock: TimeoutContextDeadline,
}

impl TimeoutContextRequest {
    /// Build a context that reads the request's active timeout source at each probe.
    ///
    /// # Safety
    ///
    /// `timeout` must stay [valid] at a stable address for as long as this context and any
    /// iterator built from it are used. Source changes and deadline writes must occur only
    /// between probes, after the previous execution cycle has stopped. Only the blocked-client
    /// flag may be updated concurrently.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub unsafe fn new(timeout: NonNull<QueryRequestTimeout>, granularity: u32) -> Self {
        // SAFETY: projecting a union field does not read it. The clock checker only reads
        // the field when a later probe sees CLOCK_DEADLINE as the active source.
        let deadline = unsafe { &raw mut (*timeout.as_ptr()).source.clock.deadline };
        let deadline = NonNull::new(deadline).expect("projected from a non-null request timeout");
        // SAFETY: the caller keeps the request timeout at a stable address and changes its
        // source only between execution cycles, when no probe can run.
        let clock = unsafe { TimeoutContextDeadline::new(deadline, granularity) };
        Self { timeout, clock }
    }
}

impl TimeoutContext for TimeoutContextRequest {
    #[inline(always)]
    fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
        // SAFETY: the constructor contract keeps the request valid and its kind stable
        // for this probe. Only the active union member is accessed below.
        match unsafe { (*self.timeout.as_ptr()).kind } {
            ffi::QueryRequestTimeoutKind_QUERY_REQUEST_TIMEOUT_UNARMED => Ok(()),
            ffi::QueryRequestTimeoutKind_QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE => {
                TimeoutContext::check_timeout(&mut self.clock)
            }
            ffi::QueryRequestTimeoutKind_QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT => {
                // SAFETY: the C bridge atomically reads the currently active marker.
                let timed_out =
                    unsafe { QueryIterator_IsBlockedClientTimedOut(self.timeout.as_ptr()) };
                if timed_out {
                    Err(RQEIteratorError::TimedOut)
                } else {
                    Ok(())
                }
            }
            kind => panic!("invalid query timeout kind: {kind}"),
        }
    }

    #[inline(always)]
    fn reset_counter(&mut self) {
        TimeoutContext::reset_counter(&mut self.clock);
    }
}

/// Optional request timeout retained by a query iterator.
///
/// A request-backed context reads the current source on every probe because cursor reads can
/// change it without rebuilding the iterator tree. A context without an owning request has no
/// timeout checks.
pub struct AnyTimeoutContext(Option<TimeoutContextRequest>);

impl AnyTimeoutContext {
    /// Build a context from a query's search context.
    ///
    /// # Safety
    ///
    /// `sctx` must stay [valid] at a stable address for as long as the returned context and any
    /// iterator built from it are used. If `sctx.timeout` is non-null, it must uphold the
    /// requirements of [`TimeoutContextRequest::new`].
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub unsafe fn from_sctx(sctx: NonNull<RedisSearchCtx>, granularity: u32) -> Self {
        // SAFETY: the caller guarantees `sctx` is valid throughout this call.
        let timeout = unsafe { (*sctx.as_ptr()).timeout };
        let request = NonNull::new(timeout).map(|timeout| {
            // SAFETY: the caller guarantees the retained request satisfies `new`'s contract.
            unsafe { TimeoutContextRequest::new(timeout, granularity) }
        });
        Self(request)
    }
}

impl TimeoutContext for AnyTimeoutContext {
    #[inline(always)]
    fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
        match &mut self.0 {
            Some(request) => request.check_timeout(),
            None => Ok(()),
        }
    }

    #[inline(always)]
    fn reset_counter(&mut self) {
        if let Some(request) = &mut self.0 {
            request.reset_counter();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn clock_context_does_not_time_out_within_deadline() {
        let mut checker = DeadlineTimeoutChecker::new(Duration::from_secs(60), 1);
        for _ in 0..1_000 {
            assert!(TimeoutContext::check_timeout(&mut checker).is_ok());
        }
    }

    #[test]
    fn clock_context_times_out_after_deadline() {
        let mut checker = DeadlineTimeoutChecker::new(Duration::from_nanos(1), 1);
        // Spin until the (very short) deadline passes; in practice the
        // first call already crosses it on every platform we run on.
        for _ in 0..1_000 {
            if TimeoutContext::check_timeout(&mut checker).is_err() {
                return;
            }
        }
        panic!("clock context never timed out");
    }

    #[test]
    fn clock_context_amortizes_via_limit() {
        let mut checker = DeadlineTimeoutChecker::new(Duration::from_nanos(1), 100);
        // With `limit = 100` the first 99 calls must not even probe the
        // clock, so they must all succeed regardless of the deadline.
        for _ in 0..99 {
            assert!(TimeoutContext::check_timeout(&mut checker).is_ok());
        }
    }

    #[test]
    fn clock_context_reset_counter_delays_next_check() {
        let mut checker = DeadlineTimeoutChecker::new(Duration::from_nanos(1), 4);
        // Three increments bring the counter to 3 (below `limit`).
        for _ in 0..3 {
            assert!(TimeoutContext::check_timeout(&mut checker).is_ok());
        }
        // Reset back to 0; the next three calls must again avoid the
        // clock check and report Ok.
        TimeoutContext::reset_counter(&mut checker);
        for _ in 0..3 {
            assert!(TimeoutContext::check_timeout(&mut checker).is_ok());
        }
    }

    /// A deadline `secs` from now, in the same monotonic clock the checker reads.
    fn deadline_in(secs: i64) -> ffi::timespec {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: `&mut ts` is a valid, writable `libc::timespec` and the clock id is valid.
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut ts) };
        ffi::timespec {
            tv_sec: ts.tv_sec + secs,
            tv_nsec: ts.tv_nsec,
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "miri has no clock_gettime(CLOCK_MONOTONIC_RAW)")]
    fn deadline_context_times_out_once_the_deadline_passes() {
        let mut deadline = deadline_in(-1);
        // SAFETY: as above.
        let mut checker = unsafe { TimeoutContextDeadline::new(NonNull::from(&mut deadline), 1) };
        assert!(matches!(
            TimeoutChecker::check_timeout(&mut checker),
            TimeoutCheckResult::TimedOut
        ));
    }

    #[test]
    #[cfg_attr(miri, ignore = "miri has no clock_gettime(CLOCK_MONOTONIC_RAW)")]
    fn deadline_context_follows_a_deadline_that_moves() {
        // The case this type exists for: a cursor read re-arms the deadline while the iterator
        // tree - and this context with it - lives on from the previous read. A context that
        // captured the deadline once would keep reporting the expired one.
        let mut deadline = deadline_in(-1);
        let ptr = NonNull::from(&mut deadline);
        // SAFETY: as above.
        let mut checker = unsafe { TimeoutContextDeadline::new(ptr, 1) };
        assert!(matches!(
            TimeoutChecker::check_timeout(&mut checker),
            TimeoutCheckResult::TimedOut
        ));

        // Stand in for the next clock cycle, which re-arms the deadline in place.
        // SAFETY: `ptr` points at `deadline`, still alive here, and no probe overlaps this write.
        unsafe { ptr.as_ptr().write(deadline_in(60)) };
        assert!(
            matches!(
                TimeoutChecker::check_timeout(&mut checker),
                TimeoutCheckResult::Ok
            ),
            "the extended deadline must be picked up, not the one from construction",
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "miri has no clock_gettime(CLOCK_MONOTONIC_RAW)")]
    fn deadline_context_amortizes_via_limit() {
        let mut deadline = deadline_in(-1);
        // SAFETY: as above.
        let mut checker = unsafe { TimeoutContextDeadline::new(NonNull::from(&mut deadline), 100) };
        // The first 99 calls must not probe the clock, so they report Ok despite the deadline.
        for _ in 0..99 {
            assert!(matches!(
                TimeoutChecker::check_timeout(&mut checker),
                TimeoutCheckResult::Ok
            ));
        }
        assert!(matches!(
            TimeoutChecker::check_timeout(&mut checker),
            TimeoutCheckResult::TimedOut
        ));
    }

    #[test]
    fn deadline_context_never_times_out_on_the_no_timeout_sentinel() {
        #[cfg_attr(target_env = "musl", expect(deprecated))]
        let mut deadline = ffi::timespec {
            tv_sec: libc::time_t::MAX,
            tv_nsec: 0,
        };
        // SAFETY: as above.
        let mut checker = unsafe { TimeoutContextDeadline::new(NonNull::from(&mut deadline), 1) };
        assert!(matches!(
            TimeoutChecker::check_timeout(&mut checker),
            TimeoutCheckResult::Ok
        ));
    }

    // The `BlockedClient` variant is a thin wrapper around the C symbol
    // `QueryIterator_IsBlockedClientTimedOut`; its
    // dispatch is covered end-to-end by `tests/pytests/test_blocked_client_timeout.py`
    // because exercising the debug sync point requires the C query pipeline.
}
