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

/// A utility for performing amortized timeout checks in high-frequency loops.
///
/// In "hot paths" (like index scanning or large iterations), calling the system clock
/// on every iteration is computationally expensive. This context uses a counter to
/// only perform a real clock check every `limit` iterations, significantly reducing
/// syscall overhead while still ensuring eventual termination.
pub struct TimeoutContext {
    /// The absolute point in time after which the operation is considered timed out.
    deadline: Instant,
    /// The number of times `check_timeout` has been called since the last clock check.
    counter: u32,
    /// The threshold at which a real clock check is performed (the amortized frequency).
    limit: u32,
}

impl TimeoutContext {
    /// Creates a new [`TimeoutContext`] that expires after the given `duration`.
    ///
    /// The `limit` determines the granularity of the check. A higher limit
    /// improves performance but increases the potential delay between the
    /// actual timeout and when it is detected.
    #[inline(always)]
    pub fn new(duration: Duration, limit: u32) -> Self {
        Self {
            deadline: Instant::now() + duration,
            counter: 0,
            limit,
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
            if Instant::now() >= self.deadline {
                return Err(RQEIteratorError::TimedOut);
            }
        }

        Ok(())
    }

    /// Reset the internal counter.
    #[inline(always)]
    pub fn reset_counter(&mut self) {
        self.counter = 0;
    }
}
