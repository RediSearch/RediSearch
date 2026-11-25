/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::time::Instant;

use timeout::{
    AnyTimeoutChecker, DeadlineTimeoutChecker, NoTimeoutChecker, TimeoutCheckResult, TimeoutChecker,
};

/// Number of traversal steps between two consecutive clock probes.
const TIMEOUT_CHECK_GRANULARITY: u32 = 100;

/// Deadline enforcement for a trie iterator.
///
/// Wraps an [`AnyTimeoutChecker`] so each iterator can carry a timeout without
/// naming the concrete checker type. Construct it from an optional deadline
/// [`Instant`] via its [`From`] impl, then call [`check`](Self::check) once per
/// traversal step.
pub struct IteratorTimeoutState<'walk> {
    checker: AnyTimeoutChecker,
    stop: Option<&'walk dyn Fn() -> bool>,
    until_stop_check: u32,
}

impl<'walk> IteratorTimeoutState<'walk> {
    /// A state that never times out (used when no deadline is set).
    pub const fn no_timeout() -> Self {
        Self {
            checker: AnyTimeoutChecker::NoTimeout(NoTimeoutChecker),
            stop: None,
            until_stop_check: TIMEOUT_CHECK_GRANULARITY,
        }
    }

    pub fn set_stop_check(&mut self, stop: &'walk dyn Fn() -> bool) {
        self.stop = Some(stop);
    }

    /// Advance the amortized checker by one step and report whether the
    /// deadline has been reached.
    pub fn check(&mut self) -> TimeoutCheckResult {
        if let Some(stop) = self.stop {
            self.until_stop_check -= 1;
            if self.until_stop_check == 0 {
                self.until_stop_check = TIMEOUT_CHECK_GRANULARITY;
                if stop() {
                    return TimeoutCheckResult::TimedOut;
                }
            }
        }
        self.checker.check_timeout()
    }
}

impl From<Option<Instant>> for IteratorTimeoutState<'_> {
    fn from(value: Option<Instant>) -> Self {
        match value {
            None => Self::no_timeout(),
            Some(deadline) => {
                let duration = deadline.saturating_duration_since(Instant::now());
                Self {
                    checker: AnyTimeoutChecker::Deadline(DeadlineTimeoutChecker::new(
                        duration,
                        TIMEOUT_CHECK_GRANULARITY,
                    )),
                    ..Self::no_timeout()
                }
            }
        }
    }
}
