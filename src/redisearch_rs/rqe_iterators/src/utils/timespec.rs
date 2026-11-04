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

/// Converts an absolute Redis deadline into a [`Duration`] from now.
///
/// Returns `None` when `deadline` is the Redis sentinel meaning "no timeout",
/// and `Some(Duration::ZERO)` when the deadline has already passed.
pub fn duration_from_redis_timespec(deadline: ffi::timespec) -> Option<Duration> {
    // Redis sentinel for no timeout
    // `libc::time_t` is deprecated on musl (musl 1.2 changed it to 64-bit,
    // and the libc crate will follow suit — see libc#1848). Suppress the
    // warning since we just need the MAX sentinel value.
    #[cfg_attr(target_env = "musl", expect(deprecated))]
    let time_t_max = libc::time_t::MAX;
    if deadline.tv_sec >= time_t_max - 1 {
        return None;
    }

    let now = monotonic_now_timespec();

    // If deadline is already in the past, expire immediately
    if timespec_le(deadline, now) {
        return Some(Duration::ZERO);
    }

    Some(timespec_sub_to_duration(deadline, now))
}

/// Report whether `deadline` has been reached, reading the clock now.
///
/// The Redis sentinel meaning "no timeout" never counts as reached, so a deadline that is cleared
/// between two probes stops expiring rather than expiring forever.
pub fn deadline_passed(deadline: ffi::timespec) -> bool {
    // Same sentinel check as `duration_from_redis_timespec`; see the note there on musl.
    #[cfg_attr(target_env = "musl", expect(deprecated))]
    let time_t_max = libc::time_t::MAX;
    if deadline.tv_sec >= time_t_max - 1 {
        return false;
    }

    timespec_le(deadline, monotonic_now_timespec())
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A deadline `secs` away from now, on the clock `deadline_passed` reads.
    fn deadline_in(secs: i64) -> ffi::timespec {
        let now = monotonic_now_timespec();
        ffi::timespec {
            tv_sec: now.tv_sec + secs,
            tv_nsec: now.tv_nsec,
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "miri has no clock_gettime(CLOCK_MONOTONIC_RAW)")]
    fn a_future_deadline_has_not_passed() {
        assert!(!deadline_passed(deadline_in(60)));
    }

    #[test]
    #[cfg_attr(miri, ignore = "miri has no clock_gettime(CLOCK_MONOTONIC_RAW)")]
    fn a_past_deadline_has_passed() {
        assert!(deadline_passed(deadline_in(-1)));
    }

    #[test]
    fn the_no_timeout_sentinel_never_passes() {
        // A request without a deadline must never be reported as out of time, however long it
        // runs — the sentinel is not a very distant deadline, it is the absence of one.
        #[cfg_attr(target_env = "musl", expect(deprecated))]
        let sentinel = ffi::timespec {
            tv_sec: libc::time_t::MAX,
            tv_nsec: 0,
        };
        assert!(!deadline_passed(sentinel));

        #[cfg_attr(target_env = "musl", expect(deprecated))]
        let off_by_one = ffi::timespec {
            tv_sec: libc::time_t::MAX - 1,
            tv_nsec: 0,
        };
        assert!(
            !deadline_passed(off_by_one),
            "the sentinel check is `>= MAX - 1`, so this value is a sentinel too",
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "miri has no clock_gettime(CLOCK_MONOTONIC_RAW)")]
    fn agrees_with_duration_from_redis_timespec() {
        // The two share a sentinel rule; a deadline that converts to a zero remaining duration is
        // exactly one that has passed.
        assert_eq!(
            duration_from_redis_timespec(deadline_in(-1)),
            Some(std::time::Duration::ZERO)
        );
        assert!(deadline_passed(deadline_in(-1)));
    }
}
