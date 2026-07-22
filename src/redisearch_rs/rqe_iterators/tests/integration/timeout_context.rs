/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the timeout context an iterator gets from a search context.
//!
//! These build the context the way production does — through
//! [`AnyTimeoutContext::from_sctx`] against a real [`RedisSearchCtx`] — rather than constructing
//! the checker directly, so they cover the pointer shape the FFI callers actually produce.

use std::ptr::NonNull;

use rqe_iterators::utils::{
    AnyTimeoutContext, TimeoutCheckResult, TimeoutChecker, TimeoutContext, TimeoutContextDeadline,
};
use rqe_iterators_test_utils::MockContext;

/// Overwrite the deadline in `ctx`'s search context, as `SearchCtx_UpdateTime` does.
fn set_deadline(ctx: &MockContext, secs_from_now: i64) {
    let mut now = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `&mut now` is a valid, writable `libc::timespec`; the clock id is valid.
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut now) };
    // SAFETY: the mock owns a valid `RedisSearchCtx` for as long as it is alive.
    unsafe {
        (*ctx.sctx().as_ptr()).time.timeout = ffi::timespec {
            tv_sec: now.tv_sec + secs_from_now,
            tv_nsec: now.tv_nsec,
        };
    }
}

/// Probe `granularity` times, so an amortized checker reaches its clock read.
fn probe(checker: &mut AnyTimeoutContext, granularity: u32) -> Result<(), String> {
    for _ in 0..granularity {
        checker.check_timeout().map_err(|e| e.to_string())?;
    }
    Ok(())
}

#[cfg_attr(miri, ignore = "miri has no clock_gettime(CLOCK_MONOTONIC_RAW)")]
#[test]
fn tracks_a_deadline_that_moves_after_construction() {
    // The regression this exists for. An iterator tree is built once and reused for the whole life
    // of a cursor, while each cursor read re-arms the deadline. A context that captured the
    // deadline at construction reports every later read as timed out.
    let ctx = MockContext::new(100, 10);
    set_deadline(&ctx, -1);

    // SAFETY: `ctx` outlives `checker`, and nothing writes the deadline while a probe runs.
    let mut checker = unsafe { AnyTimeoutContext::from_sctx(ctx.sctx(), 1) };
    assert!(
        probe(&mut checker, 1).is_err(),
        "an expired deadline must be reported",
    );

    set_deadline(&ctx, 60);
    assert!(
        probe(&mut checker, 1).is_ok(),
        "the re-armed deadline must be picked up; a captured one would keep reporting the \
         expired deadline for the rest of the cursor's life",
    );
}

#[cfg_attr(miri, ignore = "miri has no clock_gettime(CLOCK_MONOTONIC_RAW)")]
#[test]
fn a_live_deadline_that_expires_is_reported() {
    let ctx = MockContext::new(100, 10);
    set_deadline(&ctx, 60);

    // SAFETY: as above.
    let mut checker = unsafe { AnyTimeoutContext::from_sctx(ctx.sctx(), 1) };
    assert!(probe(&mut checker, 1).is_ok());

    set_deadline(&ctx, -1);
    assert!(
        probe(&mut checker, 1).is_err(),
        "reading the deadline live must catch an expiry as well as an extension",
    );
}

#[cfg_attr(miri, ignore = "miri has no clock_gettime(CLOCK_MONOTONIC_RAW)")]
#[test]
fn probes_are_amortized_across_the_granularity() {
    let ctx = MockContext::new(100, 10);
    set_deadline(&ctx, -1);

    // SAFETY: as above.
    let mut checker = unsafe { AnyTimeoutContext::from_sctx(ctx.sctx(), 100) };
    for _ in 0..99 {
        assert!(
            checker.check_timeout().is_ok(),
            "the first 99 probes must not read the clock at all",
        );
    }
    assert!(checker.check_timeout().is_err());
}

#[cfg_attr(miri, ignore = "miri has no clock_gettime(CLOCK_MONOTONIC_RAW)")]
#[test]
fn skip_timeout_checks_opts_out_entirely() {
    let ctx = MockContext::new(100, 10);
    set_deadline(&ctx, -1);
    // SAFETY: the mock owns a valid `RedisSearchCtx`.
    unsafe { (*ctx.sctx().as_ptr()).time.skipTimeoutChecks = true };

    // SAFETY: as above.
    let mut checker = unsafe { AnyTimeoutContext::from_sctx(ctx.sctx(), 1) };
    assert!(
        matches!(checker, AnyTimeoutContext::NoTimeout(_)),
        "skipTimeoutChecks must win over an expired deadline",
    );
    assert!(probe(&mut checker, 1).is_ok());
}

#[test]
fn the_no_timeout_sentinel_yields_no_checker() {
    let ctx = MockContext::new(100, 10);
    // SAFETY: the mock owns a valid `RedisSearchCtx`.
    #[cfg_attr(target_env = "musl", expect(deprecated))]
    unsafe {
        (*ctx.sctx().as_ptr()).time.timeout = ffi::timespec {
            tv_sec: libc::time_t::MAX,
            tv_nsec: 0,
        };
    }

    // SAFETY: as above.
    let checker = unsafe { AnyTimeoutContext::from_sctx(ctx.sctx(), 1) };
    assert!(
        matches!(checker, AnyTimeoutContext::NoTimeout(_)),
        "a request configured without a deadline must not pay for clock probes",
    );
}

#[test]
fn a_probe_reads_the_deadline_out_of_the_search_context() {
    // Deliberately *not* miri-ignored. The sentinel short-circuits before any clock read, so this
    // exercises the part miri can actually check: the probe dereferencing a pointer projected out
    // of a live `RedisSearchCtx`, the same way `from_sctx` projects it. The clock-reading tests
    // above have to sit out under miri, which would otherwise leave that read uncovered there.
    let ctx = MockContext::new(100, 10);
    // SAFETY: the mock owns a valid `RedisSearchCtx` for as long as it is alive.
    let deadline = unsafe { &raw mut (*ctx.sctx().as_ptr()).time.timeout };
    #[cfg_attr(target_env = "musl", expect(deprecated))]
    // SAFETY: `deadline` was just projected out of the valid search context.
    unsafe {
        deadline.write(ffi::timespec {
            tv_sec: libc::time_t::MAX,
            tv_nsec: 0,
        })
    };

    let ptr = NonNull::new(deadline).expect("projected from a non-null pointer");
    // SAFETY: `ctx` outlives `checker`, and nothing writes the deadline while the probe runs.
    let mut checker = unsafe { TimeoutContextDeadline::new(ptr, 1) };
    assert!(matches!(
        TimeoutChecker::check_timeout(&mut checker),
        TimeoutCheckResult::Ok
    ));
}
