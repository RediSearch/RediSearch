/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ffi::{AREQ, AREQ_CheckTimedOut, RedisSearchCtx};
pub use timeout::{DeadlineTimeoutChecker, NoTimeoutChecker, TimeoutCheckResult, TimeoutChecker};

use crate::{
    RQEIteratorError,
    utils::{duration_from_redis_timespec, timespec::deadline_passed},
};

/// Abstraction over the different ways a query iterator can detect that the
/// surrounding query has run out of time.
///
/// Three implementations exist:
/// * [`NoTimeoutChecker`] — zero-sized no-op used when the query has no deadline.
/// * [`TimeoutContextDeadline`] — Clock Based Timeout: amortized clock check against the
///   deadline owned by the query's search context, used when no Blocked Client Timeout is in
///   play. ([`DeadlineTimeoutChecker`] is the same check against a deadline captured up front;
///   it is not what a query iterator gets, because a query's deadline moves — see
///   [`TimeoutContextDeadline`].)
/// * [`TimeoutContextBlockedClient`] — Blocked Client Timeout: reads the
///   AREQ atomic flag (set by the Blocked Client Timeout main-thread
///   callback) via the [`AREQ_CheckTimedOut`] C symbol.
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
    /// [`NoTimeoutChecker`] and [`TimeoutContextBlockedClient`]).
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

/// [`TimeoutContext`] backed by the deadline living inside a query's [`RedisSearchCtx`].
///
/// The deadline is *read through the pointer on every probe* rather than captured once. That
/// matters because the deadline moves: `runCursor` calls `SearchCtx_UpdateTime` before each cursor
/// read, giving the read its own budget. An iterator tree, by contrast, is built once and reused
/// for the whole life of the cursor, so a captured deadline is the one belonging to the *first*
/// read, and every later read starts out already expired against it — the iterators would report a
/// timeout for a deadline the pipeline around them had just extended.
///
/// Like [`TimeoutContextBlockedClient`], the pointer carries no lifetime; keeping the search
/// context alive is a runtime invariant the caller upholds, documented on [`new`](Self::new).
///
/// Probing still costs a clock read, so it is amortized the same way
/// [`DeadlineTimeoutChecker`] amortizes: only every `limit`-th call looks at the clock.
pub struct TimeoutContextDeadline {
    /// Deadline owned by the query's search context, re-read on every probe.
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
    /// * `deadline` must point to a valid [`timespec`](ffi::timespec) — in practice
    ///   `sctx.time.timeout` — that stays valid, and at a stable address, for as long as this
    ///   context (and any iterator holding it) is used. A request assigns its search context once,
    ///   in `AREQ_ApplyContext`, and later cursor reads update the deadline in place, so the
    ///   address holds for the whole request.
    /// * The deadline must not be written concurrently with a probe. C *does* write to it — that
    ///   is the point of reading it back — from every `SearchCtx_UpdateTime` call site, and
    ///   directly from `RPTimeoutAfterCount_SimulateTimeout`. Each of those either runs before the
    ///   pipeline for that read starts (`runCursor`, `buildPipelineAndExecute`, the hybrid and
    ///   coordinator entry points) or runs inside the pipeline on the thread that would be
    ///   probing, so no write overlaps a probe. A new write site has to preserve that.
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

/// [`TimeoutContext`] backed by the Blocked Client Timeout flag on an [`AREQ`].
///
/// The struct stores a pointer to the [`AREQ`] and on every
/// [`TimeoutContext::check_timeout`] call forwards directly to the
/// [`AREQ_CheckTimedOut`] C symbol.
///
/// Unlike [`DeadlineTimeoutChecker`] this variant does **not** amortize calls:
/// the cost of a relaxed atomic load through the named extern is already
/// in the same order of magnitude as a counter bump, and avoiding the
/// counter keeps the hot path branch-free.
///
/// The [`AREQ`] is held as a raw [`NonNull`] pointer with no lifetime: like the
/// rest of the query-iterator tree (see the "phantom `'index`" note on
/// `RQEIteratorWrapper`), the context does not model the borrow in the type
/// system. Keeping the request valid for as long as the context is used is a
/// runtime invariant the caller upholds, documented on [`new`](Self::new).
pub struct TimeoutContextBlockedClient {
    /// [`AREQ`] pointer forwarded verbatim to [`AREQ_CheckTimedOut`].
    areq: NonNull<AREQ>,
}

impl TimeoutContextBlockedClient {
    /// Build a new context wrapping `areq`.
    ///
    /// # Safety
    ///
    /// * `areq` must point to a valid [`AREQ`] (as defined in
    ///   `src/aggregate/aggregate.h`) for as long as this context (and any
    ///   iterator holding it) is used. The pointer is stored without a
    ///   lifetime, so the caller is fully responsible for not using the context
    ///   past the [`AREQ`]'s lifetime.
    /// * The `QueryRequestTimeout::timedOut` flag in the [`AREQ`]'s embedded
    ///   `QueryRequest` must be safe to read with relaxed semantics from any thread.
    #[inline(always)]
    pub const unsafe fn new(areq: NonNull<AREQ>) -> Self {
        Self { areq }
    }
}

impl TimeoutChecker for TimeoutContextBlockedClient {
    /// Probe the AREQ timed-out flag via [`AREQ_CheckTimedOut`] and translate
    /// its `bool` reply into the iterator-level [`Result`].
    #[inline(always)]
    fn check_timeout(&mut self) -> TimeoutCheckResult {
        // SAFETY: constructor contract guarantees `self.areq` is valid and
        // thread-safe to probe; `AREQ_CheckTimedOut` performs a relaxed
        // atomic load and does not unwind.
        let timed_out = unsafe { AREQ_CheckTimedOut(self.areq.as_ptr()) };
        if timed_out {
            TimeoutCheckResult::TimedOut
        } else {
            TimeoutCheckResult::Ok
        }
    }

    #[inline(always)]
    fn reset_counter(&mut self) {
        // Do nothing
    }
}

/// Type-erased [`TimeoutContext`] wrapping the concrete variants.
///
/// Used at the FFI boundary so the iterator type does not depend on which
/// timeout source the C side selected for a given query. The variant is
/// fixed at construction time: each call to [`check_timeout`] adds a single
/// well-predicted branch on top of the inner variant's own work.
///
/// [`check_timeout`]: TimeoutContext::check_timeout
///
/// Two of the three variants hold a raw pointer with no lifetime:
/// [`BlockedClient`](Self::BlockedClient) an [`AREQ`] (see
/// [`TimeoutContextBlockedClient`]), and [`Clock`](Self::Clock) the deadline inside a
/// [`RedisSearchCtx`] (see [`TimeoutContextDeadline`]). Only
/// [`NoTimeout`](Self::NoTimeout) borrows nothing. The type is therefore `'static`, and keeping
/// whatever a variant points at alive while the context is used is a runtime invariant its
/// constructor documents — that obligation applies to `Clock` just as much as to `BlockedClient`.
pub enum AnyTimeoutContext {
    /// No timeout source: every probe is a no-op.
    NoTimeout(NoTimeoutChecker),
    /// Clock Based Timeout: amortized clock check against the search context's live deadline.
    Clock(TimeoutContextDeadline),
    /// Blocked Client Timeout: relaxed atomic load against the AREQ flag.
    BlockedClient(TimeoutContextBlockedClient),
}

impl AnyTimeoutContext {
    /// Builds the timeout context from a search context's time settings.
    ///
    /// `skipTimeoutChecks` (or the absence of a deadline) opts out of timeout
    /// checks entirely, yielding [`NoTimeoutChecker`]; otherwise the context borrows the
    /// deadline out of `sctx` and probes the clock against it once every `granularity`
    /// checks. A search context carries no Blocked Client Timeout
    /// source, so the [`BlockedClient`](Self::BlockedClient) variant is never
    /// produced here.
    ///
    /// # Safety
    ///
    /// Both preconditions of [`TimeoutContextDeadline::new`] are forwarded to the caller:
    ///
    /// * `sctx` must stay valid, and at a stable address, for as long as the returned context and
    ///   any iterator built from it are used — not merely for this call. The deadline is read back
    ///   through a pointer on every probe.
    /// * No write to `sctx.time.timeout` may overlap a probe.
    ///
    /// Note that `skipTimeoutChecks` is read once, here, while the deadline is read live. A
    /// request that flips the flag after its iterators are built (only `TIMEOUT_AFTER_N` does,
    /// via `AREQ_SetSkipTimeoutChecks`) keeps whichever variant this chose.
    pub unsafe fn from_sctx(sctx: NonNull<RedisSearchCtx>, granularity: u32) -> Self {
        // Read the two construction-time inputs through short-lived raw reads rather than holding
        // a reference: C writes the deadline through this same location while the context lives.
        // SAFETY: the caller guarantees `sctx` is valid.
        let time = unsafe { &raw const (*sctx.as_ptr()).time };
        // SAFETY: `time` points into the valid `sctx`.
        if unsafe { (*time).skipTimeoutChecks } {
            return Self::NoTimeout(NoTimeoutChecker);
        }
        // A deadline that is absent *now* stays absent: `SearchCtx_UpdateTime` only ever re-arms a
        // timeout that the request was configured with, so a request without one never gains one.
        // SAFETY: as above.
        if duration_from_redis_timespec(unsafe { (*time).timeout }).is_none() {
            return Self::NoTimeout(NoTimeoutChecker);
        }
        // SAFETY: `time` points into the valid `sctx`, so projecting the field is in bounds. The
        // result stays valid for as long as the caller keeps `sctx` alive.
        let deadline = unsafe { &raw const (*time).timeout }.cast_mut();
        let deadline = NonNull::new(deadline).expect("projected from a non-null pointer");
        // SAFETY: forwarded to the caller by this method's own safety contract, both clauses.
        Self::Clock(unsafe { TimeoutContextDeadline::new(deadline, granularity) })
    }
}

impl TimeoutContext for AnyTimeoutContext {
    #[inline(always)]
    fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
        match self {
            Self::NoTimeout(c) => TimeoutContext::check_timeout(c),
            Self::Clock(c) => TimeoutContext::check_timeout(c),
            Self::BlockedClient(c) => TimeoutContext::check_timeout(c),
        }
    }

    #[inline(always)]
    fn reset_counter(&mut self) {
        match self {
            Self::NoTimeout(c) => TimeoutContext::reset_counter(c),
            Self::Clock(c) => TimeoutContext::reset_counter(c),
            Self::BlockedClient(c) => TimeoutContext::reset_counter(c),
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
    fn any_timeout_context_dispatches_to_clock_variant() {
        let mut deadline = deadline_in(60);
        // SAFETY: `deadline` outlives `checker`, and nothing writes to it concurrently.
        let inner = unsafe { TimeoutContextDeadline::new(NonNull::from(&mut deadline), 1) };
        let mut checker = AnyTimeoutContext::Clock(inner);
        assert!(TimeoutContext::check_timeout(&mut checker).is_ok());
        TimeoutContext::reset_counter(&mut checker);
        assert!(TimeoutContext::check_timeout(&mut checker).is_ok());
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

        // Stand in for `SearchCtx_UpdateTime`, which re-arms the deadline in place.
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
    // `AREQ_CheckTimedOut` (declared above as `unsafe extern "C"`); its
    // dispatch is covered end-to-end by `tests/pytests/test_blocked_client_timeout.py`
    // because exercising it from Rust requires a real AREQ.
}
