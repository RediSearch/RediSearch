/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A test-owned stand-in for the deadline an iterator normally borrows out of a
//! `RedisSearchCtx`.

use std::{ptr::NonNull, time::Duration};

use rqe_iterators::utils::TimeoutContext;

/// Now, on the clock a [`TimeoutContext`] reads.
fn monotonic_now() -> ffi::timespec {
    let mut now = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `&mut now` is a valid, properly aligned, writable pointer to a `libc::timespec`,
    // and `CLOCK_MONOTONIC_RAW` is a valid clock id.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut now) };
    assert_eq!(rc, 0, "clock_gettime(CLOCK_MONOTONIC_RAW) failed");

    ffi::timespec {
        tv_sec: now.tv_sec,
        tv_nsec: now.tv_nsec,
    }
}

/// A deadline `after` from now, on the clock a [`TimeoutContext`] reads.
pub(crate) fn deadline_after(after: Duration) -> ffi::timespec {
    let now = monotonic_now();
    let nsec = now.tv_nsec + after.subsec_nanos() as i64;
    ffi::timespec {
        tv_sec: now.tv_sec + after.as_secs() as i64 + nsec / 1_000_000_000,
        tv_nsec: nsec % 1_000_000_000,
    }
}

/// A deadline `secs_from_now` seconds from now; negative values are already in the past.
pub(crate) fn deadline_in_secs(secs_from_now: i64) -> ffi::timespec {
    let now = monotonic_now();
    ffi::timespec {
        tv_sec: now.tv_sec + secs_from_now,
        tv_nsec: now.tv_nsec,
    }
}

/// A deadline the test owns, so an iterator can borrow it the way it would borrow
/// `sctx.time.timeout` in production.
///
/// Keep the value bound for at least as long as any [`TimeoutContext`] built from it: the context
/// holds a pointer to this deadline and reads it back on every probe.
pub(crate) struct TestDeadline(ffi::timespec);

impl TestDeadline {
    /// A deadline `after` from now.
    pub(crate) fn in_(after: Duration) -> Self {
        Self(deadline_after(after))
    }

    /// A timeout context probing this deadline every `limit` checks.
    ///
    /// Do not re-arm the deadline through `self` afterwards — a fresh `&mut` would invalidate the
    /// pointer the returned context holds. Tests that need to re-arm should derive a single raw
    /// pointer and use it for both, the way C does.
    pub(crate) fn timeout_ctx(&mut self, limit: u32) -> TimeoutContext {
        // SAFETY: the deadline lives in `self`, which the caller keeps alive for as long as the
        // returned context is used, and nothing writes to it in the meantime.
        unsafe { TimeoutContext::new(NonNull::from(&mut self.0), limit) }
    }
}
