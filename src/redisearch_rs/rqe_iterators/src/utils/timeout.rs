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

use crate::{RQEIteratorError, utils::duration_from_redis_timespec};

pub trait TimeoutContext {
    fn check_timeout(&mut self) -> Result<(), RQEIteratorError>;

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
    /// * The `RequestSyncState::timedOut` flag inside the [`AREQ`] must be safe
    ///   to read with relaxed semantics from any thread.
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
/// The [`BlockedClient`](Self::BlockedClient) variant holds its [`AREQ`] as a
/// raw pointer with no lifetime (see [`TimeoutContextBlockedClient`]); the other
/// two borrow nothing. The type is therefore `'static`, and keeping the request
/// alive while the context is used is a runtime invariant its constructor
/// documents.
pub enum AnyTimeoutContext {
    /// No timeout source: every probe is a no-op.
    NoTimeout(NoTimeoutChecker),
    /// Clock Based Timeout: amortized clock check.
    Clock(DeadlineTimeoutChecker),
    /// Blocked Client Timeout: relaxed atomic load against the AREQ flag.
    BlockedClient(TimeoutContextBlockedClient),
}

impl AnyTimeoutContext {
    /// Builds the timeout context from a search context's time settings.
    ///
    /// `skipTimeoutChecks` (or the absence of a deadline) opts out of timeout
    /// checks entirely, yielding [`NoTimeoutChecker`]; otherwise the deadline drives an
    /// amortized [`DeadlineTimeoutChecker`] that probes the clock once every
    /// `granularity` checks. A search context carries no Blocked Client Timeout
    /// source, so the [`BlockedClient`](Self::BlockedClient) variant is never
    /// produced here.
    pub fn from_sctx(sctx: &RedisSearchCtx, granularity: u32) -> Self {
        if sctx.time.skipTimeoutChecks {
            return Self::NoTimeout(NoTimeoutChecker);
        }
        match duration_from_redis_timespec(sctx.time.timeout) {
            Some(duration) => Self::Clock(DeadlineTimeoutChecker::new(duration, granularity)),
            None => Self::NoTimeout(NoTimeoutChecker),
        }
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

    #[test]
    fn any_timeout_context_dispatches_to_clock_variant() {
        let inner = DeadlineTimeoutChecker::new(Duration::from_secs(60), 1);
        let mut checker = AnyTimeoutContext::Clock(inner);
        assert!(TimeoutContext::check_timeout(&mut checker).is_ok());
        TimeoutContext::reset_counter(&mut checker);
        assert!(TimeoutContext::check_timeout(&mut checker).is_ok());
    }

    // The `BlockedClient` variant is a thin wrapper around the C symbol
    // `AREQ_CheckTimedOut` (declared above as `unsafe extern "C"`); its
    // dispatch is covered end-to-end by `tests/pytests/test_blocked_client_timeout.py`
    // because exercising it from Rust requires a real AREQ.
}
