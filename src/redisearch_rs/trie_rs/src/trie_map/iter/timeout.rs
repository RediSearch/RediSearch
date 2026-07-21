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

pub struct IteratorTimeoutState(AnyTimeoutChecker);

impl IteratorTimeoutState {
    pub const fn no_timeout() -> Self {
        Self(AnyTimeoutChecker::NoTimeout(NoTimeoutChecker))
    }

    pub fn check(&mut self) -> TimeoutCheckResult {
        self.0.check_timeout()
    }
}

impl From<Option<Instant>> for IteratorTimeoutState {
    fn from(value: Option<Instant>) -> Self {
        match value {
            None => Self::no_timeout(),
            Some(deadline) => {
                let duration = deadline.saturating_duration_since(Instant::now());
                Self(AnyTimeoutChecker::Deadline(DeadlineTimeoutChecker::new(
                    duration, 100,
                )))
            }
        }
    }
}
