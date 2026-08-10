/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use crate::{RQEIteratorError, utils::deadline_passed};

/// A utility for performing amortized timeout checks in high-frequency loops.
///
/// In "hot paths" (like index scanning or large iterations), calling the system clock
/// on every iteration is computationally expensive. This context uses a counter to
/// only perform a real clock check every `limit` iterations, significantly reducing
/// syscall overhead while still ensuring eventual termination.
///
/// The deadline is *borrowed*, not copied: it is read back through a pointer on every probe.
/// An iterator tree is built once and reused for the whole life of a cursor, while each cursor
/// read re-arms the deadline via `SearchCtx_UpdateTime`. A context that captured the deadline at
/// construction would keep answering with the first read's budget forever after.
pub struct TimeoutContext {
    /// The absolute point in time after which the operation is considered timed out.
    ///
    /// Owned by the query's `RedisSearchCtx` and read back on every clock check.
    deadline: NonNull<ffi::timespec>,
    /// The number of times `check_timeout` has been called since the last clock check.
    counter: u32,
    /// The threshold at which a real clock check is performed (the amortized frequency).
    limit: u32,
}

impl TimeoutContext {
    /// Creates a new [`TimeoutContext`] probing the deadline stored at `deadline`.
    ///
    /// The `limit` determines the granularity of the check. A higher limit
    /// improves performance but increases the potential delay between the
    /// actual timeout and when it is detected.
    ///
    /// # Safety
    ///
    /// * `deadline` must point to a valid [`timespec`](ffi::timespec) — in practice
    ///   `sctx.time.timeout` — that stays valid, and at a stable address, for as long as this
    ///   context (and any iterator holding it) is used. A request assigns its search context once,
    ///   in `AREQ_ApplyContext`, and later cursor reads update the deadline in place, so the
    ///   address holds for the whole request.
    /// * The deadline must not be written concurrently with a probe. C *does* write to it — that
    ///   is the point of reading it back — from every `SearchCtx_UpdateTime` call site, and
    ///   directly from `RPTimeoutAfterCount_SimulateTimeout`. Each of those either runs before the
    ///   pipeline for that read starts (`runCursor`, `buildPipelineAndExecute`, the hybrid and
    ///   coordinator entry points) or runs inside the pipeline on the thread that would be
    ///   probing, so no write overlaps a probe. A new write site has to preserve that.
    /// * `deadline` must be derived from a pointer that permits the C side to keep writing to it,
    ///   not from a shared reference: taking `&sctx.time.timeout` would freeze the location for
    ///   this context's lifetime and make those writes undefined behaviour.
    #[inline(always)]
    pub const unsafe fn new(deadline: NonNull<ffi::timespec>, limit: u32) -> Self {
        Self {
            deadline,
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
            // SAFETY: the constructor contract guarantees `deadline` points to a valid `timespec`
            // that outlives this context, and that no write to it overlaps this read.
            let deadline = unsafe { self.deadline.read() };
            if deadline_passed(deadline) {
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
