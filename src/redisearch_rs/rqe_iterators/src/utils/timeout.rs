/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    ptr::NonNull,
    time::{Duration, Instant},
};

use ffi::{AREQ, AREQ_CheckTimedOut, RedisSearchCtx};

use crate::{
    RQEIteratorError,
    utils::{duration_from_redis_timespec, timespec::deadline_passed},
};

/// Abstraction over the different ways a query iterator can detect that the
/// surrounding query has run out of time.
///
/// Three implementations exist:
/// * [`NoTimeout`] — zero-sized no-op used when the query has no deadline.
/// * [`TimeoutContextClock`] — Clock Based Timeout: amortized clock check
///   used when no Blocked Client Timeout is in play.
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
    /// [`NoTimeout`] and [`TimeoutContextBlockedClient`]).
    fn reset_counter(&mut self) {}
}

/// Amortized clock-based [`TimeoutContext`].
///
/// In "hot paths" (like index scanning or large iterations), calling the system clock
/// on every iteration is computationally expensive. This context uses a counter to
/// only perform a real clock check every `limit` iterations, significantly reducing
/// syscall overhead while still ensuring eventual termination.
pub struct TimeoutContextClock {
    /// The point in time after which the operation is considered timed out.
    deadline: ClockDeadline,
    /// The number of times `check_timeout` has been called since the last clock check.
    counter: u32,
    /// The threshold at which a real clock check is performed (the amortized frequency).
    limit: u32,
}

/// Where a [`TimeoutContextClock`] gets the deadline it compares the clock against.
enum ClockDeadline {
    /// A deadline computed once, when the context was built.
    ///
    /// Only appropriate when nothing can move the deadline afterwards — in practice, tests.
    Captured(Instant),
    /// A deadline owned elsewhere (in practice `sctx.time.timeout`) and read back on every probe.
    ///
    /// Query iterators use this: an iterator tree is built once and reused for the whole life of
    /// a cursor, while each cursor read re-arms the deadline via `SearchCtx_UpdateTime`. A
    /// captured deadline would keep answering with the first read's budget forever after.
    Live(NonNull<ffi::timespec>),
}

impl TimeoutContextClock {
    /// Creates a new [`TimeoutContextClock`] that expires after the given `duration`.
    ///
    /// The `limit` determines the granularity of the check. A higher limit
    /// improves performance but increases the potential delay between the
    /// actual timeout and when it is detected.
    ///
    /// To skip timeout checks entirely, use [`NoTimeout`] instead of
    /// constructing this context.
    #[inline(always)]
    pub fn new(duration: Duration, limit: u32) -> Self {
        Self {
            deadline: ClockDeadline::Captured(Instant::now() + duration),
            counter: 0,
            limit,
        }
    }

    /// Creates a [`TimeoutContextClock`] probing the deadline stored at `deadline`, reading it
    /// back on every clock check rather than capturing it now.
    ///
    /// `limit` plays the same role as in [`new`](Self::new).
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
    /// * `deadline` must be derived from a pointer that permits the C side to keep writing to it,
    ///   not from a shared reference: taking `&sctx.time.timeout` would freeze the location for
    ///   this context's lifetime and make those writes undefined behaviour.
    pub const unsafe fn from_deadline(deadline: NonNull<ffi::timespec>, limit: u32) -> Self {
        Self {
            deadline: ClockDeadline::Live(deadline),
            counter: 0,
            limit,
        }
    }
}

impl TimeoutContext for TimeoutContextClock {
    /// Increments the internal counter and, if the `limit` is reached, checks if
    /// the current time has passed the `deadline`.
    #[inline(always)]
    fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
        self.counter += 1;
        if self.counter >= self.limit {
            self.counter = 0;
            let passed = match self.deadline {
                ClockDeadline::Captured(deadline) => Instant::now() >= deadline,
                // SAFETY: guaranteed by the `from_deadline` constructor contract.
                ClockDeadline::Live(deadline) => deadline_passed(unsafe { deadline.read() }),
            };
            if passed {
                return Err(RQEIteratorError::TimedOut);
            }
        }

        Ok(())
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
/// Unlike [`TimeoutContextClock`] this variant does **not** amortize calls:
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
    /// * The `RequestSyncCtx::timedOut` flag inside the [`AREQ`] must be safe
    ///   to read with relaxed semantics from any thread.
    #[inline(always)]
    pub const unsafe fn new(areq: NonNull<AREQ>) -> Self {
        Self { areq }
    }
}

impl TimeoutContext for TimeoutContextBlockedClient {
    /// Probe the AREQ timed-out flag via [`AREQ_CheckTimedOut`] and translate
    /// its `bool` reply into the iterator-level [`Result`].
    #[inline(always)]
    fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
        // SAFETY: constructor contract guarantees `self.areq` is valid and
        // thread-safe to probe; `AREQ_CheckTimedOut` performs a relaxed
        // atomic load and does not unwind.
        let timed_out = unsafe { AREQ_CheckTimedOut(self.areq.as_ptr()) };
        if timed_out {
            Err(RQEIteratorError::TimedOut)
        } else {
            Ok(())
        }
    }
}

/// Zero-sized no-op [`TimeoutContext`].
///
/// Used by callers that want to opt out of timeout checks entirely without
/// having to wrap the context in an [`Option`]. Because the type has no
/// fields and every method is a no-op, monomorphizing an iterator over
/// `NoTimeout` collapses the entire timeout machinery to dead code that
/// the optimizer removes from the hot path.
pub struct NoTimeout;

impl TimeoutContext for NoTimeout {
    #[inline(always)]
    fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
        Ok(())
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
/// The [`BlockedClient`](Self::BlockedClient) variant holds its [`AREQ`] as a
/// raw pointer with no lifetime (see [`TimeoutContextBlockedClient`]); the other
/// two borrow nothing. The type is therefore `'static`, and keeping the request
/// alive while the context is used is a runtime invariant its constructor
/// documents.
pub enum AnyTimeoutContext {
    /// No timeout source: every probe is a no-op.
    NoTimeout(NoTimeout),
    /// Clock Based Timeout: amortized clock check.
    Clock(TimeoutContextClock),
    /// Blocked Client Timeout: relaxed atomic load against the AREQ flag.
    BlockedClient(TimeoutContextBlockedClient),
}

impl AnyTimeoutContext {
    /// Builds the timeout context from a search context's time settings.
    ///
    /// `skipTimeoutChecks` (or the absence of a deadline) opts out of timeout
    /// checks entirely, yielding [`NoTimeout`]; otherwise the deadline drives an
    /// amortized [`TimeoutContextClock`] that probes the clock once every
    /// `granularity` checks. A search context carries no Blocked Client Timeout
    /// source, so the [`BlockedClient`](Self::BlockedClient) variant is never
    /// produced here.
    ///
    /// # Safety
    ///
    /// Both preconditions of [`TimeoutContextClock::from_deadline`] are forwarded to the caller:
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
        // SAFETY: the caller guarantees `sctx` remains valid.
        let time = unsafe { &raw const (*sctx.as_ptr()).time };
        // SAFETY: `time` points inside the valid search context.
        if unsafe { (*time).skipTimeoutChecks } {
            return Self::NoTimeout(NoTimeout);
        }
        // SAFETY: as above.
        if duration_from_redis_timespec(unsafe { (*time).timeout }).is_none() {
            return Self::NoTimeout(NoTimeout);
        }
        // SAFETY: this field projection remains valid as long as `sctx` does.
        let deadline = unsafe { &raw const (*time).timeout }.cast_mut();
        let deadline = NonNull::new(deadline).expect("projected from a non-null pointer");
        // SAFETY: this method forwards the constructor's lifetime and synchronization contract.
        Self::Clock(unsafe { TimeoutContextClock::from_deadline(deadline, granularity) })
    }
}

impl TimeoutContext for AnyTimeoutContext {
    #[inline(always)]
    fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
        match self {
            Self::NoTimeout(c) => c.check_timeout(),
            Self::Clock(c) => c.check_timeout(),
            Self::BlockedClient(c) => c.check_timeout(),
        }
    }

    #[inline(always)]
    fn reset_counter(&mut self) {
        match self {
            Self::NoTimeout(c) => c.reset_counter(),
            Self::Clock(c) => c.reset_counter(),
            Self::BlockedClient(c) => c.reset_counter(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A deadline `secs` from now, on the clock the live variant reads.
    fn deadline_in(secs: i64) -> ffi::timespec {
        let mut now = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: `now` is writable and the clock id is valid.
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut now) };
        ffi::timespec {
            tv_sec: now.tv_sec + secs,
            tv_nsec: now.tv_nsec,
        }
    }

    #[test]
    fn clock_context_does_not_time_out_within_deadline() {
        let mut ctx = TimeoutContextClock::new(Duration::from_secs(60), 1);
        for _ in 0..1_000 {
            assert!(ctx.check_timeout().is_ok());
        }
    }

    #[test]
    fn clock_context_times_out_after_deadline() {
        let mut ctx = TimeoutContextClock::new(Duration::from_nanos(1), 1);
        // Spin until the (very short) deadline passes; in practice the
        // first call already crosses it on every platform we run on.
        for _ in 0..1_000 {
            if ctx.check_timeout().is_err() {
                return;
            }
        }
        panic!("clock context never timed out");
    }

    #[test]
    fn clock_context_amortizes_via_limit() {
        let mut ctx = TimeoutContextClock::new(Duration::from_nanos(1), 100);
        // With `limit = 100` the first 99 calls must not even probe the
        // clock, so they must all succeed regardless of the deadline.
        for _ in 0..99 {
            assert!(ctx.check_timeout().is_ok());
        }
    }

    #[test]
    fn clock_context_reset_counter_delays_next_check() {
        let mut ctx = TimeoutContextClock::new(Duration::from_nanos(1), 4);
        // Three increments bring the counter to 3 (below `limit`).
        for _ in 0..3 {
            assert!(ctx.check_timeout().is_ok());
        }
        // Reset back to 0; the next three calls must again avoid the
        // clock check and report Ok.
        ctx.reset_counter();
        for _ in 0..3 {
            assert!(ctx.check_timeout().is_ok());
        }
    }

    #[test]
    fn any_timeout_context_dispatches_to_clock_variant() {
        let inner = TimeoutContextClock::new(Duration::from_secs(60), 1);
        let mut ctx = AnyTimeoutContext::Clock(inner);
        assert!(ctx.check_timeout().is_ok());
        ctx.reset_counter();
        assert!(ctx.check_timeout().is_ok());
    }

    /// The bug the live variant exists to prevent.
    ///
    /// An iterator tree is built once and reused for the whole life of a cursor, while every
    /// cursor read calls `SearchCtx_UpdateTime` to give that read its own budget. A context that
    /// captured the deadline at construction would keep measuring against the *first* read's
    /// deadline, long past, and report a timeout for a budget just extended.
    #[test]
    #[cfg_attr(miri, ignore = "miri has no clock_gettime(CLOCK_MONOTONIC_RAW)")]
    fn clock_context_follows_a_deadline_that_moves() {
        let mut deadline = deadline_in(-1);
        // Derive the pointer once and drive both the checker and the re-arm through it, the way C
        // writes to the very location the iterator reads back.
        let ptr = NonNull::from(&mut deadline);
        // SAFETY: `deadline` outlives the checker and is not written during a probe.
        let mut ctx = unsafe { TimeoutContextClock::from_deadline(ptr, 1) };
        assert!(
            ctx.check_timeout().is_err(),
            "a deadline already in the past must be reported as timed out",
        );
        // Re-arm, as `SearchCtx_UpdateTime` does at the start of the next cursor read.
        // SAFETY: `ptr` still points to `deadline`, and no probe overlaps the write.
        unsafe { ptr.as_ptr().write(deadline_in(60)) };
        assert!(
            ctx.check_timeout().is_ok(),
            "the re-armed deadline must be picked up, not the one seen at construction",
        );
    }

    // The `BlockedClient` variant is a thin wrapper around the C symbol
    // `AREQ_CheckTimedOut` (declared above as `unsafe extern "C"`); its
    // dispatch is covered end-to-end by `tests/pytests/test_blocked_client_timeout.py`
    // because exercising it from Rust requires a real AREQ.
}
