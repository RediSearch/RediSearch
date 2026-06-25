/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Timeout-propagation tests for [`VectorScoreSource`], ported from
//! `test_vecsim.py::TestTimeoutReached`.
//!
//! These live in their **own test binary** (hence their own process) on
//! purpose. They install a *process-global* VecSim timeout callback
//! (`VecSim_SetTimeoutCallbackFunction`) set to always-timed-out, which any
//! concurrent VecSim query in the same process would observe. The rest of the
//! suite (`tests/integration/*`) is full of VecSim query tests; keeping the
//! timeout tests out of that binary means the global mutation can never reach a
//! query test, regardless of the harness — `cargo nextest` (process-per-test),
//! `cargo test`, or the coverage lane's `cargo llvm-cov test` (multithreaded
//! libtest, one process). See MOD-16948 / PR #10493: relying on nextest's
//! process isolation alone left the coverage lane racy.
//!
//! Within this binary the two tests are still serialized against each other,
//! because one test's teardown (restoring the no-op callback) would otherwise
//! clear the always-timed-out callback another test is still asserting on.

use std::{
    ffi::c_void,
    num::NonZeroUsize,
    sync::{Mutex, MutexGuard, PoisonError},
};

use ffi::VecSimIndex_Free;
use rqe_core::DocId;
use rqe_iterators::{RQEIterator, RQEIteratorError};
use top_k::{ScoreSource as _, TopKIterator, TopKMode};
use vector_score_source::new_vector_top_k_unfiltered;
use vector_score_source::test_utils::{
    asc, build_hnsw_index, make_child, make_source, uniform_blob,
};

// Provide stubs for C symbols that the linked C archive references but that
// these tests never exercise (Redis module API surface).
redis_mock::mock_or_stub_missing_redis_c_symbols!();
extern crate redisearch_rs;

unsafe extern "C" {
    fn VecSim_SetTimeoutCallbackFunction(
        cb: Option<unsafe extern "C" fn(*mut c_void) -> std::ffi::c_int>,
    );
}

unsafe extern "C" fn always_timed_out(_ctx: *mut c_void) -> std::ffi::c_int {
    1
}

unsafe extern "C" fn never_timed_out(_ctx: *mut c_void) -> std::ffi::c_int {
    0
}

/// Serializes the two timeout tests against each other: both install a
/// process-global always-timed-out callback and restore the no-op on drop, so
/// running them concurrently could let one test's teardown clear the callback
/// while the other is still asserting on it.
static TIMEOUT_TEST_LOCK: Mutex<()> = Mutex::new(());

/// Acquire [`TIMEOUT_TEST_LOCK`] for the caller's scope, recovering from a
/// poisoned lock so a genuinely failing test surfaces its own panic instead of
/// cascading into the sibling test.
#[must_use]
fn serialize() -> MutexGuard<'static, ()> {
    TIMEOUT_TEST_LOCK
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// Installs the always-timeout callback; restores the no-op on drop so a
/// panicking assertion cannot leak timeout state to the sibling test.
struct MockTimeout;
impl MockTimeout {
    fn enable() -> Self {
        // SAFETY: the fn pointer is valid for the whole program.
        unsafe { VecSim_SetTimeoutCallbackFunction(Some(always_timed_out)) };
        MockTimeout
    }
}
impl Drop for MockTimeout {
    fn drop(&mut self) {
        // SAFETY: the fn pointer is valid for the whole program.
        unsafe { VecSim_SetTimeoutCallbackFunction(Some(never_timed_out)) };
    }
}

/// From `test_vecsim.py::TestTimeoutReached` (KNN branch).
#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn unfiltered_propagates_timeout() {
    // Acquire before enable() so the global-callback mutation stays exclusive;
    // released after `_mock` restores the callback (reverse drop order).
    let _serial = serialize();
    let (n, k, dim) = (100, 10, 4);
    let index = build_hnsw_index(n, dim);
    let _mock = MockTimeout::enable();

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, uniform_blob(n as f32, dim), n, k, n) };
    let mut it = new_vector_top_k_unfiltered(source, NonZeroUsize::new(k).unwrap());

    assert!(matches!(it.read(), Err(RQEIteratorError::TimedOut)));

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

/// From `test_vecsim.py::TestTimeoutReached` (hybrid BATCHES branch).
#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn batches_propagates_timeout() {
    // Acquire before enable() so the global-callback mutation stays exclusive;
    // released after `_mock` restores the callback (reverse drop order).
    let _serial = serialize();
    let (n, k, dim) = (100, 10, 4);
    let index = build_hnsw_index(n, dim);
    let _mock = MockTimeout::enable();

    // SAFETY: index outlives the iterator (freed at end of scope).
    let source = unsafe { make_source(index, uniform_blob(n as f32, dim), n, k, n) };
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(make_child((1..=n as DocId).collect())),
        NonZeroUsize::new(k).unwrap(),
        asc,
        TopKMode::Batches,
    );

    assert!(matches!(it.read(), Err(RQEIteratorError::TimedOut)));

    drop(it);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}

/// A timed-out unfiltered query must not consume the single-shot budget: the
/// source is probed directly (no iterator rewind in between) so that re-issuing
/// the query after the timeout clears returns the real top-k rather than EOF.
#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn unfiltered_timeout_does_not_mark_consumed() {
    let (n, k, dim) = (100, 10, 4);
    let index = build_hnsw_index(n, dim);

    // SAFETY: index outlives the source (freed at end of scope).
    let mut source = unsafe { make_source(index, uniform_blob(n as f32, dim), n, k, n) };

    {
        let _mock = MockTimeout::enable();
        assert!(matches!(
            source.all_results_unfiltered_batch(),
            Err(RQEIteratorError::TimedOut)
        ));
    }

    // The timeout left the single-shot flag clear, so this re-issues the query.
    assert!(
        source.all_results_unfiltered_batch().unwrap().is_some(),
        "retry after a timed-out query must re-run it, not short-circuit to EOF"
    );

    drop(source);
    // SAFETY: no live references to the index remain.
    unsafe { VecSimIndex_Free(index.as_ptr()) };
}
