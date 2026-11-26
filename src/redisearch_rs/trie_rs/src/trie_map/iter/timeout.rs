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
/// naming the concrete checker type. Set a deadline with [`Self::set_timeout`]
/// and call [`Self::check`] once per traversal step.
pub struct IteratorTimeoutState<'walk> {
    checker: AnyTimeoutChecker,
    stop: Option<&'walk (dyn Fn() -> bool + Sync)>,
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

    pub fn set_timeout(&mut self, timeout: Option<Instant>) {
        self.checker = match timeout {
            None => AnyTimeoutChecker::NoTimeout(NoTimeoutChecker),
            Some(deadline) => AnyTimeoutChecker::Deadline(DeadlineTimeoutChecker::new(
                deadline.saturating_duration_since(Instant::now()),
                TIMEOUT_CHECK_GRANULARITY,
            )),
        };
    }

    pub fn set_stop_check(&mut self, stop: &'walk (dyn Fn() -> bool + Sync)) {
        self.stop = Some(stop);
    }

    /// Advance the amortized checker by one step and report whether the
    /// deadline has been reached.
    pub fn check(&mut self) -> TimeoutCheckResult {
        if let Some(stop) = self.stop {
            if self.until_stop_check == 1 {
                if stop() {
                    return TimeoutCheckResult::TimedOut;
                }
                self.until_stop_check = TIMEOUT_CHECK_GRANULARITY;
            } else {
                self.until_stop_check -= 1;
            }
        }
        self.checker.check_timeout()
    }
}
