/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::time::{Duration, Instant};

/// Identify the result of [`TimeoutChecker::check_timeout`].
pub enum TimeoutCheckResult {
    /// The checker timed out.
    TimedOut,
    /// The checker is still ok.
    Ok,
}

/// Abstraction over the timeout checker.
pub trait TimeoutChecker {
    /// Report whether the query has timed out.
    ///
    /// Returns [`TimeoutCheckResult::TimedOut`] when the deadline has been
    /// reached. Otherwise returns [`TimeoutCheckResult::Ok`].
    fn check_timeout(&mut self) -> TimeoutCheckResult;

    /// The caller invokes this method after a unit of useful work has been
    /// completed, so amortized implementations can reset their internal
    /// counter without losing accuracy.
    fn reset_counter(&mut self);
}

/// Amortized clock [`TimeoutChecker`].
///
/// In "hot paths" (like index scanning or large iterations), calling the system clock
/// on every iteration is computationally expensive. This context uses a counter to
/// only perform a real clock check every `limit` iterations, significantly reducing
/// syscall overhead while still ensuring eventual termination.
pub struct DeadlineTimeoutChecker {
    /// The absolute point in time after which the operation is considered timed out.
    deadline: Instant,
    /// The number of times `check_timeout` has been called since the last clock check.
    counter: u32,
    /// The threshold at which a real clock check is performed (the amortized frequency).
    limit: u32,
}

impl DeadlineTimeoutChecker {
    /// Creates a new [`TimeoutChecker`] that expires after the given `duration`.
    ///
    /// The `limit` determines the granularity of the check. A higher limit
    /// improves performance but increases the potential delay between the
    /// actual timeout and when it is detected.
    ///
    /// To skip timeout checks entirely, use [`NoTimeoutChecker`] instead of
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

impl TimeoutChecker for DeadlineTimeoutChecker {
    /// Increments the internal counter and, if the `limit` is reached, checks if
    /// the current time has passed the `deadline`.
    #[inline(always)]
    fn check_timeout(&mut self) -> TimeoutCheckResult {
        self.counter += 1;
        // For optimized builds, we only check the deadline
        // once every `self.limit` iterations. In development,
        // we're checking each iteration.
        if self.counter >= self.limit || cfg!(debug_assertions) {
            // Reset the counter whenever we probe the clock so the next
            // `limit` calls are amortized again. Without this reset the
            // counter would stay `>= limit` forever, turning every
            // subsequent call into a real clock check.
            self.counter = 0;
            if Instant::now() >= self.deadline {
                return TimeoutCheckResult::TimedOut;
            }
        }

        TimeoutCheckResult::Ok
    }

    #[inline(always)]
    fn reset_counter(&mut self) {
        self.counter = 0;
    }
}

/// Zero-sized no-op [`TimeoutChecker`].
///
/// Used by callers that want to opt out of timeout checks entirely without
/// having to wrap the context in an [`Option`]. Because the type has no
/// fields and every method is a no-op, monomorphizing an iterator over
/// `NoTimeoutChecker` collapses the entire timeout machinery to dead code that
/// the optimizer removes from the hot path.
pub struct NoTimeoutChecker;

impl TimeoutChecker for NoTimeoutChecker {
    /// Increments the internal counter and, if the `limit` is reached, checks if
    /// the current time has passed the `deadline`.
    #[inline(always)]
    fn check_timeout(&mut self) -> TimeoutCheckResult {
        TimeoutCheckResult::Ok
    }

    #[inline(always)]
    fn reset_counter(&mut self) {
    }
}

/// Type-erased [`TimeoutChecker`] wrapping the concrete variants.
pub enum AnyTimeoutChecker {
    /// No timeout source: every probe is a no-op.
    NoTimeout(NoTimeoutChecker),
    /// Clock Based Timeout: amortized clock check.
    Deadline(DeadlineTimeoutChecker),
}

impl TimeoutChecker for AnyTimeoutChecker {
    #[inline(always)]
    fn check_timeout(&mut self) -> TimeoutCheckResult {
        
        match self {
            Self::NoTimeout(c) => {
                c.check_timeout()
            },
            Self::Deadline(c) => {
                c.check_timeout()
            },
        }
    }

    #[inline(always)]
    fn reset_counter(&mut self) {
        match self {
            Self::NoTimeout(c) => c.reset_counter(),
            Self::Deadline(c) => c.reset_counter(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // NOTE: behavioral tests that only exercise the public API live in
    // `tests/tests.rs`. This module keeps only the tests that must inspect
    // private internals (e.g. the amortization `counter`), which an
    // integration test cannot reach.

    #[test]
    fn clock_context_amortizes_repeatedly_after_first_probe() {
        // Deadline is effectively unreachable, so the checker must always
        // report Ok. With `limit = 100`, the clock is probed on calls
        // 100, 200, 300, ... and the counter must reset each time. The
        // calls in between (never a multiple of `limit`) must not probe
        // the clock. Regression test for a counter that was left `>= limit`
        // after the first probe, which turned every later call into a
        // clock check.
        let limit = 100;
        let mut ctx = DeadlineTimeoutChecker::new(Duration::from_secs(3600), limit);
        for _ in 0..(limit as usize * 5) {
            assert!(matches!(ctx.check_timeout(), TimeoutCheckResult::Ok));
        }
        // After probing on the last multiple of `limit`, the counter must
        // have been reset rather than left at `limit`.
        assert!(ctx.counter < limit);
    }
}