/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::c_void;
use std::time::{Duration, Instant};

use crate::RQEIteratorError;

/// Abstraction over the different ways a query iterator can detect that the
/// surrounding query has run out of time.
///
/// Two implementations exist (see `MOD-15397-design.md`):
/// * [`TimeoutContextClock`] — the original amortized clock-based check used
///   when no Redis blocked-client timeout is in play.
/// * [`TimeoutContextBlockedClientCallback`] — delegates the decision to a C
///   callback that reads the AREQ atomic flag set by the blocked-client
///   timeout main-thread callback.
///
/// Iterators are generic over this trait so the dispatch is monomorphized
/// in the hot path.
pub trait TimeoutContext {
    /// Report whether the query has timed out.
    ///
    /// Returns [`RQEIteratorError::TimedOut`] when the deadline has been
    /// reached (or, for callback-based variants, when the external signal
    /// has flipped). Otherwise returns `Ok(())`.
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
    /// callback-based ones).
    #[inline(always)]
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
    /// To skip timeout checks entirely, do not construct a context: callers
    /// hold the timeout context as `Option<TC>` and pass `None` instead.
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

/// Function pointer signature exposed by the C side to drive
/// [`TimeoutContextBlockedClientCallback`].
///
/// The callback is invoked by Rust on every [`TimeoutContext::check_timeout`]
/// call and must return `true` when the surrounding query has timed out,
/// `false` otherwise.
///
/// # Safety contract
///
/// Callers constructing a [`TimeoutContextBlockedClientCallback`] guarantee
/// that the callback they install satisfies all of the following:
///
/// * It is safe to call from any thread, including the worker threads that
///   drive [`crate::RQEIterator`] hot paths.
/// * It must not unwind — any failure to honor that turns the FFI call into
///   undefined behavior.
/// * It treats `user_data` as an opaque pointer and must not retain it past
///   the call.
/// * The pointer kept in `user_data` (typically the AREQ pointer set up by
///   MOD-15396 alongside `RPQueryIterator->areq`) outlives every iterator
///   that holds the context.
///
/// The skip-flag logic intentionally lives **inside the C callback**: the
/// Rust side only sees a `bool`. This keeps the iterators agnostic of which
/// timeout path is in effect.
pub type TimeoutCallback = unsafe extern "C" fn(user_data: *mut c_void) -> bool;

/// Callback-driven [`TimeoutContext`] used when a Redis blocked-client
/// timeout is in play.
///
/// The struct stores a C function pointer plus an opaque user-data pointer
/// (typically the AREQ established by MOD-15396). Each call to
/// [`TimeoutContext::check_timeout`] forwards directly to the callback and
/// returns [`RQEIteratorError::TimedOut`] when it reports a timeout.
///
/// Unlike [`TimeoutContextClock`] this variant does **not** amortize calls:
/// the cost of probing the AREQ atomic flag through the callback is already
/// in the same order of magnitude as a counter bump, and the indirection
/// avoids any extra hot-path arithmetic.
pub struct TimeoutContextBlockedClientCallback {
    /// C function pointer invoked on every timeout probe.
    callback: TimeoutCallback,
    /// Opaque pointer forwarded verbatim to `callback`.
    user_data: *mut c_void,
}

impl TimeoutContextBlockedClientCallback {
    /// Build a new context wrapping `callback` and `user_data`.
    ///
    /// # Safety
    ///
    /// The caller must uphold every clause of the [`TimeoutCallback`] safety
    /// contract for the lifetime of the returned value.
    #[inline(always)]
    pub unsafe fn new(callback: TimeoutCallback, user_data: *mut c_void) -> Self {
        Self {
            callback,
            user_data,
        }
    }
}

impl TimeoutContext for TimeoutContextBlockedClientCallback {
    /// Invoke the C callback and translate its `bool` reply into the
    /// iterator-level [`Result`].
    #[inline(always)]
    fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
        // SAFETY: the constructor's safety contract guarantees the callback
        // is safe to invoke from this thread, does not unwind, and that
        // `self.user_data` is still valid.
        let timed_out = unsafe { (self.callback)(self.user_data) };
        if timed_out {
            Err(RQEIteratorError::TimedOut)
        } else {
            Ok(())
        }
    }
}

/// Type-erased [`TimeoutContext`] wrapping the two concrete variants.
///
/// Used at the FFI boundary so the iterator type does not depend on which
/// timeout source the C side selected for a given query. The variant is
/// fixed at construction time: each call to [`check_timeout`] adds a single
/// well-predicted branch on top of the inner variant's own work.
///
/// [`check_timeout`]: TimeoutContext::check_timeout
pub enum AnyTimeoutContext {
    /// Amortized clock-based check (default for the legacy timeout path).
    Clock(TimeoutContextClock),
    /// Blocked-client callback check (new MOD-15397 path).
    BlockedClient(TimeoutContextBlockedClientCallback),
}

impl TimeoutContext for AnyTimeoutContext {
    #[inline(always)]
    fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
        match self {
            Self::Clock(c) => c.check_timeout(),
            Self::BlockedClient(c) => c.check_timeout(),
        }
    }

    #[inline(always)]
    fn reset_counter(&mut self) {
        match self {
            Self::Clock(c) => c.reset_counter(),
            Self::BlockedClient(c) => c.reset_counter(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Callback that always reports "not timed out".
    unsafe extern "C" fn never_timeout(_: *mut c_void) -> bool {
        false
    }

    /// Callback that always reports "timed out".
    unsafe extern "C" fn always_timeout(_: *mut c_void) -> bool {
        true
    }

    /// Callback that flips to "timed out" once the counter reaches its
    /// configured trigger. The counter lives in the user-data pointer.
    unsafe extern "C" fn flip_after_n(user_data: *mut c_void) -> bool {
        // SAFETY: the test passes a `&FlipAfterN` cast to `*mut c_void`.
        let state = unsafe { &*(user_data as *const FlipAfterN) };
        let prev = state.calls.fetch_add(1, Ordering::Relaxed);
        prev + 1 >= state.trigger
    }

    struct FlipAfterN {
        calls: AtomicU32,
        trigger: u32,
    }

    #[test]
    fn never_timeout_callback_returns_ok() {
        // SAFETY: the static fn satisfies the callback contract trivially.
        let mut ctx = unsafe {
            TimeoutContextBlockedClientCallback::new(never_timeout, std::ptr::null_mut())
        };
        for _ in 0..1_000 {
            assert!(ctx.check_timeout().is_ok());
        }
    }

    #[test]
    fn always_timeout_callback_returns_timed_out() {
        // SAFETY: the static fn satisfies the callback contract trivially.
        let mut ctx = unsafe {
            TimeoutContextBlockedClientCallback::new(always_timeout, std::ptr::null_mut())
        };
        assert!(matches!(
            ctx.check_timeout(),
            Err(RQEIteratorError::TimedOut)
        ));
    }

    #[test]
    fn flip_after_n_callback_flips_on_trigger() {
        let state = FlipAfterN {
            calls: AtomicU32::new(0),
            trigger: 5,
        };
        // SAFETY: `state` outlives `ctx` since both are owned by this test.
        let mut ctx = unsafe {
            TimeoutContextBlockedClientCallback::new(
                flip_after_n,
                &state as *const FlipAfterN as *mut c_void,
            )
        };

        for _ in 0..(state.trigger - 1) {
            assert!(ctx.check_timeout().is_ok());
        }
        assert!(matches!(
            ctx.check_timeout(),
            Err(RQEIteratorError::TimedOut)
        ));
    }

    #[test]
    fn reset_counter_is_a_noop() {
        // SAFETY: the static fn satisfies the callback contract trivially.
        let mut ctx = unsafe {
            TimeoutContextBlockedClientCallback::new(never_timeout, std::ptr::null_mut())
        };
        ctx.reset_counter();
        assert!(ctx.check_timeout().is_ok());
    }

    #[test]
    fn any_timeout_context_dispatches_to_clock_variant() {
        let inner = TimeoutContextClock::new(Duration::from_secs(60), 1);
        let mut ctx = AnyTimeoutContext::Clock(inner);
        // The clock checks once per `limit` calls; with `limit = 1` and a
        // 60s deadline the call must succeed immediately.
        assert!(ctx.check_timeout().is_ok());
        ctx.reset_counter();
        assert!(ctx.check_timeout().is_ok());
    }

    #[test]
    fn any_timeout_context_dispatches_to_callback_variant() {
        // SAFETY: the static fn satisfies the callback contract trivially.
        let inner = unsafe {
            TimeoutContextBlockedClientCallback::new(always_timeout, std::ptr::null_mut())
        };
        let mut ctx = AnyTimeoutContext::BlockedClient(inner);
        assert!(matches!(
            ctx.check_timeout(),
            Err(RQEIteratorError::TimedOut)
        ));
        // `reset_counter` must not panic on the callback variant.
        ctx.reset_counter();
    }
}
