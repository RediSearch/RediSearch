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

use ffi::{AREQ, AREQ_CheckTimedOut};

use crate::RQEIteratorError;

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
    /// The absolute point in time after which the operation is considered timed out.
    deadline: Instant,
    /// The number of times `check_timeout` has been called since the last clock check.
    counter: u32,
    /// The threshold at which a real clock check is performed (the amortized frequency).
    limit: u32,
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
            deadline: Instant::now() + duration,
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
            if Instant::now() >= self.deadline {
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
    ///   `src/aggregate/aggregate.h`).
    /// * The pointee must outlive every iterator that holds this context.
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
pub enum AnyTimeoutContext {
    /// No timeout source: every probe is a no-op.
    NoTimeout(NoTimeout),
    /// Clock Based Timeout: amortized clock check.
    Clock(TimeoutContextClock),
    /// Blocked Client Timeout: relaxed atomic load against the AREQ flag.
    BlockedClient(TimeoutContextBlockedClient),
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

    // The `BlockedClient` variant is a thin wrapper around the C symbol
    // `AREQ_CheckTimedOut` (declared above as `unsafe extern "C"`); its
    // dispatch is covered end-to-end by `tests/pytests/test_blocked_client_timeout.py`
    // because exercising it from Rust requires a real AREQ.
}
