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
    /// A deadline computed once, when the context was built.
    Captured(Instant),
    /// A deadline owned by someone else, re-read on every probe so that re-arming it in place is
    /// observed. Upheld by [`TimeoutContext::from_deadline`]'s safety contract.
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

    /// Creates a [`TimeoutContext`] that probes a deadline it does not own, re-reading it through
    /// `deadline` on every clock check rather than capturing it now.
    ///
    /// This is the difference that makes a re-armed deadline visible: whoever owns the `timespec`
    /// can move it, and the next probe measures against the new value. `limit` and
    /// `skip_timeout_checks` behave as in [`new`](Self::new).
    ///
    /// # Safety
    ///
    /// * `deadline` must point to an initialized `timespec` that stays alive, and at this same
    ///   address, for as long as this context is used - not merely for this call.
    /// * No write to `*deadline` may overlap a [`check_timeout`](Self::check_timeout) call.
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
                Deadline::Live(deadline) => {
                    // SAFETY: `from_deadline`'s contract guarantees the pointee is alive,
                    // initialized, and not being written while this probe runs.
                    let deadline = unsafe { deadline.read() };
                    let mut now = libc::timespec {
                        tv_sec: 0,
                        tv_nsec: 0,
                    };
                    // SAFETY: `now` is a writable `libc::timespec` and the clock id is valid.
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Read the clock `check_timeout` compares against.
    fn monotonic_now() -> ffi::timespec {
        let mut now = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: `now` is a writable `libc::timespec` and the clock id is valid.
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut now) };
        ffi::timespec {
            tv_sec: now.tv_sec,
            tv_nsec: now.tv_nsec,
        }
    }

    /// A deadline `secs` away from now, on that same clock.
    fn deadline_in(secs: i64) -> ffi::timespec {
        let now = monotonic_now();
        ffi::timespec {
            tv_sec: now.tv_sec + secs,
            tv_nsec: now.tv_nsec,
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "miri has no clock_gettime(CLOCK_MONOTONIC_RAW)")]
    fn a_future_live_deadline_does_not_time_out() {
        let mut deadline = deadline_in(60);
        let ptr = NonNull::from(&mut deadline);
        // SAFETY: `deadline` outlives `ctx`, and nothing writes to it while a probe runs.
        let mut ctx = unsafe { TimeoutContext::from_deadline(ptr, 1, false) };

        assert!(ctx.check_timeout().is_ok());
    }

    #[test]
    #[cfg_attr(miri, ignore = "miri has no clock_gettime(CLOCK_MONOTONIC_RAW)")]
    fn a_past_live_deadline_times_out() {
        let mut deadline = deadline_in(-1);
        let ptr = NonNull::from(&mut deadline);
        // SAFETY: as above.
        let mut ctx = unsafe { TimeoutContext::from_deadline(ptr, 1, false) };

        assert!(matches!(
            ctx.check_timeout(),
            Err(RQEIteratorError::TimedOut)
        ));
    }

    /// The reason the live variant exists (MOD-17489): a cursor read re-arms the deadline in place
    /// between reads, and the next probe must measure against the new value. A captured deadline
    /// would keep reporting the first read's budget forever.
    #[test]
    #[cfg_attr(miri, ignore = "miri has no clock_gettime(CLOCK_MONOTONIC_RAW)")]
    fn re_arming_the_deadline_in_place_is_observed() {
        let mut deadline = deadline_in(-1);
        let ptr = NonNull::from(&mut deadline);
        // SAFETY: as above.
        let mut ctx = unsafe { TimeoutContext::from_deadline(ptr, 1, false) };

        assert!(
            matches!(ctx.check_timeout(), Err(RQEIteratorError::TimedOut)),
            "the deadline starts in the past"
        );

        // Write through the same pointer the context holds, so this stays a single borrow chain.
        // SAFETY: `ptr` points at `deadline`, still alive here, and no probe overlaps this write.
        unsafe { ptr.write(deadline_in(60)) };

        assert!(
            ctx.check_timeout().is_ok(),
            "the extended deadline must be picked up, not the one read before"
        );
    }

    #[test]
    fn skipping_timeout_checks_never_probes_the_deadline() {
        // Already expired, so any probe that happened would report a timeout.
        let mut deadline = ffi::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        let ptr = NonNull::from(&mut deadline);
        // SAFETY: as above.
        let mut ctx = unsafe { TimeoutContext::from_deadline(ptr, 1, true) };

        for _ in 0..10_000 {
            assert!(ctx.check_timeout().is_ok());
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "miri has no clock_gettime(CLOCK_MONOTONIC_RAW)")]
    fn the_deadline_is_only_probed_once_every_limit_calls() {
        let mut deadline = deadline_in(-1);
        let ptr = NonNull::from(&mut deadline);
        // SAFETY: as above.
        let mut ctx = unsafe { TimeoutContext::from_deadline(ptr, 3, false) };

        assert!(ctx.check_timeout().is_ok());
        assert!(ctx.check_timeout().is_ok());
        assert!(
            matches!(ctx.check_timeout(), Err(RQEIteratorError::TimedOut)),
            "the third call is the one that reaches the limit and reads the clock"
        );
    }
}
