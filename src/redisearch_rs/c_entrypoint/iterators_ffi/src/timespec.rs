/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! timespec utilities when going between C and Rust

use std::time::Duration;

pub(crate) fn duration_from_redis_timespec(deadline: ffi::timespec) -> Option<Duration> {
    // Redis sentinel for no timeout
    if deadline.tv_sec >= libc::time_t::MAX - 1 {
        return None;
    }

    let now = monotonic_now_timespec();

    // If deadline is already in the past, expire immediately
    if timespec_le(deadline, now) {
        return Some(Duration::ZERO);
    }

    Some(timespec_sub_to_duration(deadline, now))
}

const fn timespec_le(a: ffi::timespec, b: ffi::timespec) -> bool {
    a.tv_sec < b.tv_sec || (a.tv_sec == b.tv_sec && a.tv_nsec <= b.tv_nsec)
}

fn timespec_sub_to_duration(a: ffi::timespec, b: ffi::timespec) -> Duration {
    // Computes (a - b) where a > b, returning a positive Duration.

    // Clamp nanos into a sane range similar to your existing helper
    let a_nsec = a.tv_nsec.clamp(0, 999_999_999);
    let b_nsec = b.tv_nsec.clamp(0, 999_999_999);

    let mut sec = (a.tv_sec - b.tv_sec) as u64;

    // Do a borrow if needed for nanoseconds
    let nsec: u32 = if a_nsec >= b_nsec {
        (a_nsec - b_nsec) as u32
    } else {
        // Borrow 1 second
        sec = sec.saturating_sub(1);
        (1_000_000_000 + a_nsec - b_nsec) as u32
    };

    Duration::new(sec, nsec)
}

fn monotonic_now_timespec() -> ffi::timespec {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };

    // SAFETY: `&mut ts` is a valid, properly aligned, writable pointer to
    // `libc::timespec`, and `CLOCK_MONOTONIC_RAW` is a valid clock id.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut ts) };
    debug_assert_eq!(rc, 0);

    ffi::timespec {
        tv_sec: ts.tv_sec,
        tv_nsec: ts.tv_nsec,
    }
}
