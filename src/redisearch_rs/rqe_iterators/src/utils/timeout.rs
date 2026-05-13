/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::time::{Duration, Instant};

use crate::RQEIteratorError;

/// Abstraction over the different ways a query iterator can detect that the
/// surrounding query has run out of time.
///
/// Two implementations are planned (see `MOD-15397-design.md`):
/// * [`TimeoutContextClock`] — the original amortized clock-based check used
///   when no Redis blocked-client timeout is in play.
/// * `TimeoutContextBlockedClientCallback` (added in a follow-up step) —
///   delegates the decision to a C callback that reads the AREQ atomic flag
///   set by the blocked-client timeout main-thread callback.
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
