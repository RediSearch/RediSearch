/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The memory check between the batches of a filtered vector top-k, in a test binary of its own
//! because the forced memory ratio is process-global.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};

use rqe_core::DocId;
use rqe_iterators::{NoOpChecker, RQEIterator, RQEIteratorError};
use top_k::{Ascending, ScoreSource, TopKIterator, TopKMode};
use vector_score_source::VectorScoreSource;
use vector_score_source::test_utils::{
    ForcedMemoryRatio, TestIndex, asc, collect_ids, make_child, uniform_blob,
};

// Provide stubs for C symbols that the linked C archive references but that
// these tests never exercise (Redis module API surface).
redis_mock::mock_or_stub_missing_redis_c_symbols!();
extern crate redisearch_rs;

const N: usize = 100;
const K: usize = 10;
const DIM: usize = 4;
/// The filter keeps the docs farthest from the query, so the first batch, sized for the filter's
/// selectivity, holds none of them and the top k takes further batches.
const FILTERED: DocId = 20;

type FarFilterTopK<'index> = TopKIterator<
    'index,
    VectorScoreSource<'index, NoOpChecker>,
    Box<dyn RQEIterator<'index> + 'index>,
    Ascending,
>;

/// A top-k nearest to `[N; DIM]` over the docs `1..=FILTERED`, kept in batches rather than
/// switched to an adhoc scan, which a filter this selective would otherwise choose.
fn far_filter_top_k(index: &TestIndex) -> FarFilterTopK<'_> {
    let source = index.source(uniform_blob(N as f32, DIM), N, K, FILTERED as usize);
    TopKIterator::new_with_mode(
        source,
        Some(make_child((1..=FILTERED).collect())),
        NonZeroUsize::new(K).unwrap(),
        asc(),
        TopKMode::ForcedBatches,
    )
}

/// Whether the fixture's request has a memory stop requested.
fn memory_stopped(index: &TestIndex) -> bool {
    // SAFETY: the fixture's request limits outlive the call; projecting the field reads nothing.
    let flag = unsafe { &raw mut (*index.timeout_ptr()).memory.stopped };
    // SAFETY: the flag is aligned and valid, and every access to it is atomic.
    unsafe { AtomicBool::from_ptr(flag) }.load(Ordering::Relaxed)
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn batches_under_memory_pressure_stop_for_oom_before_the_second_batch() {
    for index in [TestIndex::flat(N, DIM), TestIndex::hnsw(N, DIM)] {
        let _pressure = ForcedMemoryRatio::set(1.0);
        let mut it = far_filter_top_k(&index);

        assert!(matches!(it.read(), Err(RQEIteratorError::Stopped)));
        assert_eq!(it.metrics().num_batches, 1);
        // The second batch was never requested from VecSim.
        assert_eq!(it.source().num_iterations, 1);
        assert!(memory_stopped(&index));
    }
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn batches_below_the_memory_limit_run_to_the_end() {
    let index = TestIndex::flat(N, DIM);
    let _pressure = ForcedMemoryRatio::set(0.99);
    let mut it = far_filter_top_k(&index);

    // Doc i is [i; DIM], so the filtered docs nearest to [N; DIM] are the highest of them.
    let expected: Vec<DocId> = (FILTERED - K as DocId + 1..=FILTERED).rev().collect();
    assert_eq!(collect_ids(&mut it), expected);
    assert!(it.metrics().num_batches > 1);
    assert!(!memory_stopped(&index));
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn a_top_k_filled_by_its_first_batch_is_not_stopped() {
    let index = TestIndex::flat(N, DIM);
    let _pressure = ForcedMemoryRatio::set(1.0);
    // The even docs are spread evenly, so the first batch fills the top k.
    let source = index.source(uniform_blob(N as f32, DIM), N, K, N / 2);
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(make_child((2..=N as DocId).step_by(2).collect())),
        NonZeroUsize::new(K).unwrap(),
        asc(),
        TopKMode::Batches,
    );

    let expected: Vec<DocId> = (0..K as DocId).map(|i| N as DocId - 2 * i).collect();
    assert_eq!(collect_ids(&mut it), expected);
    assert_eq!(it.metrics().num_batches, 1);
    assert!(!memory_stopped(&index));
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn a_filter_exhausted_by_its_first_batch_is_not_stopped() {
    let index = TestIndex::flat(N, DIM);
    let _pressure = ForcedMemoryRatio::set(1.0);
    // Fewer docs pass than K, and the first batch, sized for that selectivity, covers the index.
    let few: DocId = 5;
    let source = index.source(uniform_blob(N as f32, DIM), N, K, few as usize);
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(make_child((1..=few).collect())),
        NonZeroUsize::new(K).unwrap(),
        asc(),
        TopKMode::ForcedBatches,
    );

    assert_eq!(collect_ids(&mut it), (1..=few).rev().collect::<Vec<_>>());
    assert_eq!(it.metrics().num_batches, 1);
    assert!(!memory_stopped(&index));
}

/// An adhoc scan over the docs `1..=FILTERED` with a k that exceeds them, so it ends by exhausting
/// the filter: every match is retained with its captured child record.
fn adhoc_top_k(index: &TestIndex) -> FarFilterTopK<'_> {
    let k = FILTERED as usize + 5;
    let source = index.source(uniform_blob(N as f32, DIM), N, k, FILTERED as usize);
    TopKIterator::new_with_mode(
        source,
        Some(make_child((1..=FILTERED).collect())),
        NonZeroUsize::new(k).unwrap(),
        asc(),
        TopKMode::AdhocBF,
    )
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn an_adhoc_scan_shorter_than_the_read_interval_stops_for_oom_at_its_end() {
    for index in [TestIndex::flat(N, DIM), TestIndex::hnsw(N, DIM)] {
        let _pressure = ForcedMemoryRatio::set(1.0);
        let mut it = adhoc_top_k(&index);

        assert!(matches!(it.read(), Err(RQEIteratorError::Stopped)));
        assert!(memory_stopped(&index));
    }
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn an_adhoc_scan_below_the_memory_limit_runs_to_the_end() {
    let index = TestIndex::flat(N, DIM);
    let _pressure = ForcedMemoryRatio::set(0.99);
    let mut it = adhoc_top_k(&index);

    let expected: Vec<DocId> = (1..=FILTERED).rev().collect();
    assert_eq!(collect_ids(&mut it), expected);
    assert!(!memory_stopped(&index));
}

#[test]
#[cfg_attr(miri, ignore = "requires C FFI (VecSim)")]
fn the_adhoc_checkpoint_reads_memory_once_per_interval() {
    let index = TestIndex::flat(N, DIM);
    let _pressure = ForcedMemoryRatio::set(1.0);
    let mut source = index.source(uniform_blob(N as f32, DIM), N, K, N);

    // The stop comes with the amortized read, not with the first call. A scan that replaced
    // `check_stop` with an unamortized read would stop on call 1.
    assert!(source.check_stop().is_ok());
    assert!(!memory_stopped(&index));
    let stopped_at = (2..=4096).find(|_| source.check_stop().is_err());
    assert!(stopped_at.is_some(), "the interval read never happened");
    assert!(memory_stopped(&index));
}
