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

use crate::RQEIteratorError;

/// A utility for performing amortized timeout checks in high-frequency loops.
///
/// In "hot paths" (like index scanning or large iterations), calling the system clock
/// on every iteration is computationally expensive. This context uses a counter to
/// only perform a real clock check every `limit` iterations, significantly reducing
/// syscall overhead while still ensuring eventual termination.
pub struct TimeoutContext {
    /// The absolute point in time after which the operation is considered timed out.
    deadline: Deadline,
    /// The number of times `check_timeout` has been called since the last clock check.
    counter: u32,
    /// The threshold at which a real clock check is performed (the amortized frequency).
    /// When set to `u32::MAX`, timeout checks are effectively skipped.
    limit: u32,
}

enum Deadline {
    Captured(Instant),
    Live(NonNull<ffi::timespec>),
}

impl TimeoutContext {
    /// Creates a new [`TimeoutContext`] that expires after the given `duration`.
    ///
    /// The `limit` determines the granularity of the check. A higher limit
    /// improves performance but increases the potential delay between the
    /// actual timeout and when it is detected.
    ///
    /// If `skip_timeout_checks` is `true`, `limit` is set to `u32::MAX` to effectively
    /// skip timeout checks (the counter will never reach the limit in practice).
    #[inline(always)]
    pub fn new(duration: Duration, limit: u32, skip_timeout_checks: bool) -> Self {
        Self {
            deadline: Deadline::Captured(Instant::now() + duration),
            counter: 0,
            // Use u32::MAX to effectively skip timeout checks
            limit: if skip_timeout_checks { u32::MAX } else { limit },
        }
    }

    /// # Safety
    ///
    /// `deadline` must remain valid and stable for this context's lifetime, and no write may
    /// overlap a timeout probe.
    pub const unsafe fn from_deadline(
        deadline: NonNull<ffi::timespec>,
        limit: u32,
        skip_timeout_checks: bool,
    ) -> Self {
        Self {
            deadline: Deadline::Live(deadline),
            counter: 0,
            limit: if skip_timeout_checks { u32::MAX } else { limit },
        }
    }

    /// Increments the internal counter and, if the `limit` is reached, checks if
    /// the current time has passed the `deadline`.
    ///
    /// Returns error [`RQEIteratorError::TimedOut`] if the deadline has been reached or exceeded.
    #[inline(always)]
    pub fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
        self.counter += 1;
        if self.counter >= self.limit {
            self.counter = 0;
            let timed_out = match self.deadline {
                Deadline::Captured(deadline) => Instant::now() >= deadline,
                // SAFETY: guaranteed by the `from_deadline` constructor contract.
                Deadline::Live(deadline) => {
                    let deadline = unsafe { deadline.read() };
                    let mut now = libc::timespec { tv_sec: 0, tv_nsec: 0 };
                    // SAFETY: `now` is writable and the clock id is valid.
                    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut now) };
                    now.tv_sec > deadline.tv_sec
                        || (now.tv_sec == deadline.tv_sec && now.tv_nsec >= deadline.tv_nsec)
                }
            };
            if timed_out {
                return Err(RQEIteratorError::TimedOut);
            }
        }

        Ok(())
    }

    /// Reset the internal counter.
    #[inline(always)]
    pub const fn reset_counter(&mut self) {
        self.counter = 0;
    }
}
