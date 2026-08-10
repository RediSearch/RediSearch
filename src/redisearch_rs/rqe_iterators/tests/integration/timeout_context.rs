/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for [`TimeoutContext`], which borrows its deadline rather than capturing it.

use std::ptr::NonNull;

use rqe_iterators::utils::TimeoutContext;

use crate::utils::deadline_in_secs;

/// Probe `granularity` times, so an amortized context reaches its clock read.
fn probe(
    ctx: &mut TimeoutContext,
    granularity: u32,
) -> Result<(), rqe_iterators::RQEIteratorError> {
    for _ in 0..granularity {
        ctx.check_timeout()?;
    }
    Ok(())
}

/// The bug this borrowing behaviour exists to prevent.
///
/// An iterator tree is built once and reused for the whole life of a cursor, while every cursor
/// read calls `SearchCtx_UpdateTime` to give that read its own budget. A context that captured
/// the deadline at construction would keep measuring against the *first* read's deadline, long
/// past, and report a timeout for a budget the pipeline had just extended.
#[test]
#[cfg_attr(miri, ignore = "miri has no clock_gettime(CLOCK_MONOTONIC_RAW)")]
fn tracks_a_deadline_that_moves_after_construction() {
    let mut deadline = deadline_in_secs(-1);
    // Derive the pointer once and drive both the context and the re-arm through it, the way C
    // writes to the very location the iterator reads back.
    let ptr = NonNull::from(&mut deadline);

    // SAFETY: `deadline` outlives `ctx` and nothing writes to it concurrently with a probe.
    let mut ctx = unsafe { TimeoutContext::new(ptr, 1) };

    assert!(
        probe(&mut ctx, 1).is_err(),
        "a deadline already in the past must be reported as timed out",
    );

    // Re-arm, as `SearchCtx_UpdateTime` does at the start of the next cursor read.
    // SAFETY: `ptr` points at the live `deadline`, and no probe is in flight.
    unsafe { ptr.write(deadline_in_secs(60)) };

    assert!(
        probe(&mut ctx, 1).is_ok(),
        "the re-armed deadline must be picked up, not the one captured at construction",
    );
}

/// The mirror image: a deadline that is live at construction and expires later.
#[test]
#[cfg_attr(miri, ignore = "miri has no clock_gettime(CLOCK_MONOTONIC_RAW)")]
fn a_live_deadline_that_expires_is_reported() {
    let mut deadline = deadline_in_secs(60);
    let ptr = NonNull::from(&mut deadline);

    // SAFETY: `deadline` outlives `ctx` and nothing writes to it concurrently with a probe.
    let mut ctx = unsafe { TimeoutContext::new(ptr, 1) };

    assert!(probe(&mut ctx, 1).is_ok());

    // SAFETY: `ptr` points at the live `deadline`, and no probe is in flight.
    unsafe { ptr.write(deadline_in_secs(-1)) };

    assert!(
        probe(&mut ctx, 1).is_err(),
        "an expired deadline must be reported on the next probe",
    );
}

/// Reading the clock is amortized: only every `limit`-th probe pays for it.
#[test]
#[cfg_attr(miri, ignore = "miri has no clock_gettime(CLOCK_MONOTONIC_RAW)")]
fn probes_are_amortized_across_the_granularity() {
    const GRANULARITY: u32 = 100;

    let mut deadline = deadline_in_secs(-1);
    let ptr = NonNull::from(&mut deadline);

    // SAFETY: `deadline` outlives `ctx` and nothing writes to it concurrently with a probe.
    let mut ctx = unsafe { TimeoutContext::new(ptr, GRANULARITY) };

    for i in 1..GRANULARITY {
        assert!(
            ctx.check_timeout().is_ok(),
            "probe #{i} must not read the clock yet, the limit is {GRANULARITY}",
        );
    }
    assert!(
        ctx.check_timeout().is_err(),
        "probe #{GRANULARITY} reaches the limit and must observe the expired deadline",
    );
}

/// The counter restarts after a clock read, so the next probe is amortized again.
#[test]
#[cfg_attr(miri, ignore = "miri has no clock_gettime(CLOCK_MONOTONIC_RAW)")]
fn reset_counter_restarts_the_amortization_window() {
    const GRANULARITY: u32 = 10;

    let mut deadline = deadline_in_secs(-1);
    let ptr = NonNull::from(&mut deadline);

    // SAFETY: `deadline` outlives `ctx` and nothing writes to it concurrently with a probe.
    let mut ctx = unsafe { TimeoutContext::new(ptr, GRANULARITY) };

    for _ in 1..GRANULARITY {
        assert!(ctx.check_timeout().is_ok());
    }
    ctx.reset_counter();

    for i in 1..GRANULARITY {
        assert!(
            ctx.check_timeout().is_ok(),
            "probe #{i} after the reset must not read the clock yet",
        );
    }
    assert!(ctx.check_timeout().is_err());
}
