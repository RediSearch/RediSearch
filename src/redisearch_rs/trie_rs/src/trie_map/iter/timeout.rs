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

/// Number of traversal steps between two consecutive deadline probes —
/// clock reads for a deadline set from an [`Instant`], calls to the
/// predicate for one set via
/// [`from_should_stop`](IteratorTimeoutState::from_should_stop).
///
/// This is the polling contract every deadline-carrying trie iterator
/// obeys: the deadline source is probed once per this many *traversal
/// steps*, not once per yielded entry, so it fires even on a sparse walk
/// that visits many nodes without yielding any. Once it reports a stop,
/// the iterator is exhausted and stays exhausted.
///
/// Mirrors `TIMEOUT_COUNTER_LIMIT` in `src/util/timeout.h`, which paces
/// the equivalent C traversals. The value is duplicated rather than
/// imported because this crate is pure Rust with no `ffi` dependency.
pub const TIMEOUT_CHECK_GRANULARITY: u32 = 100;

/// Deadline enforcement for a trie iterator.
///
/// Carries one of two amortized deadline sources, probed once per
/// [`TIMEOUT_CHECK_GRANULARITY`] traversal steps: a clock deadline
/// (constructed from an optional [`Instant`] via the [`From`] impl) or a
/// caller-supplied `should_stop` predicate (via
/// [`from_should_stop`](Self::from_should_stop)), which keeps the clock —
/// or whatever else signals cancellation — on the caller's side. Call
/// [`check`](Self::check) once per traversal step.
pub struct IteratorTimeoutState<'a>(Inner<'a>);

enum Inner<'a> {
    /// Clock-based deadline (or the no-op checker when no deadline is set).
    Clock(AnyTimeoutChecker),
    /// Caller-supplied stop signal, polled with the same amortization as
    /// the clock checker.
    Callback {
        /// Traversal steps since `should_stop` was last polled.
        counter: u32,
        /// Latched once `should_stop` returns `true`, so the iterator stays
        /// exhausted instead of walking another amortization window —
        /// matching an expired clock deadline, which stays expired.
        stopped: bool,
        should_stop: Box<dyn FnMut() -> bool + 'a>,
    },
}

impl IteratorTimeoutState<'_> {
    /// A state that never times out (used when no deadline is set).
    pub const fn no_timeout() -> Self {
        Self(Inner::Clock(AnyTimeoutChecker::NoTimeout(NoTimeoutChecker)))
    }

    /// Advance the amortized checker by one step and report whether the
    /// deadline has been reached.
    pub fn check(&mut self) -> TimeoutCheckResult {
        match &mut self.0 {
            Inner::Clock(checker) => checker.check_timeout(),
            Inner::Callback {
                counter,
                stopped,
                should_stop,
            } => {
                if *stopped {
                    return TimeoutCheckResult::TimedOut;
                }
                *counter += 1;
                if *counter >= TIMEOUT_CHECK_GRANULARITY {
                    *counter = 0;
                    if should_stop() {
                        *stopped = true;
                        return TimeoutCheckResult::TimedOut;
                    }
                }
                TimeoutCheckResult::Ok
            }
        }
    }
}

impl<'a> IteratorTimeoutState<'a> {
    /// A state driven by a caller-supplied stop signal instead of the clock.
    pub fn from_should_stop(should_stop: impl FnMut() -> bool + 'a) -> Self {
        Self(Inner::Callback {
            counter: 0,
            stopped: false,
            should_stop: Box::new(should_stop),
        })
    }
}

impl From<Option<Instant>> for IteratorTimeoutState<'_> {
    fn from(value: Option<Instant>) -> Self {
        match value {
            None => Self::no_timeout(),
            Some(deadline) => {
                let duration = deadline.saturating_duration_since(Instant::now());
                Self(Inner::Clock(AnyTimeoutChecker::Deadline(
                    DeadlineTimeoutChecker::new(duration, TIMEOUT_CHECK_GRANULARITY),
                )))
            }
        }
    }
}
