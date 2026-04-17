/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for [`TopKIterator`].
//!
//! These tests live here (rather than inline in `src/iterator.rs`) because
//! they use [`IdList`] and [`RSIndexResult`], which transitively call C symbols
//! (`RedisModule_Free`, `ResultMetrics_Free`) at drop time.  Those symbols are
//! stubbed out by `redis_mock::mock_or_stub_missing_redis_c_symbols!()` in
//! `main.rs`.

use std::{cmp::Ordering, num::NonZeroUsize};

use rqe_iterators::{IdList, RQEIterator};
use top_k::{CollectionStrategy, TopKIterator, mock::MockScoreSource};

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Ascending comparator: lower score is better (e.g. vector distance).
fn asc(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

fn make_child<'a>(ids: Vec<ffi::t_docId>) -> Box<dyn RQEIterator<'a> + 'a> {
    Box::new(IdList::<true>::new(ids))
}

// ── State machine ─────────────────────────────────────────────────────────────

#[test]
fn read_triggers_collection_on_first_call() {
    let source = MockScoreSource::new(vec![vec![(1, 1.0), (2, 2.0)]], |_, _| {
        CollectionStrategy::Continue
    });
    let mut it = TopKIterator::new(source, None, NonZeroUsize::new(5).unwrap(), asc);

    assert!(!it.at_eof());
    let result = it.read().unwrap().expect("should have a result");
    assert_eq!(result.doc_id, 1);
}

#[test]
fn rewind_resets_to_not_started() {
    let source = MockScoreSource::new(vec![vec![(1, 1.0), (2, 2.0)]], |_, _| {
        CollectionStrategy::Continue
    });
    let mut it = TopKIterator::new(source, None, NonZeroUsize::new(5).unwrap(), asc);
    it.read().unwrap();
    it.read().unwrap();
    it.read().unwrap(); // EOF

    assert!(it.at_eof());
    it.rewind();
    assert!(!it.at_eof());

    let result = it
        .read()
        .unwrap()
        .expect("should have a result after rewind");
    assert_eq!(result.doc_id, 1);
}

#[test]
fn eof_set_after_results_exhausted() {
    let source = MockScoreSource::new(vec![vec![(1, 1.0)]], |_, _| CollectionStrategy::Continue);
    let mut it = TopKIterator::new(source, None, NonZeroUsize::new(5).unwrap(), asc);

    it.read().unwrap();
    let eof = it.read().unwrap();
    assert!(eof.is_none());
    assert!(it.at_eof());
}

#[test]
fn num_estimated_capped_at_k() {
    let source = MockScoreSource::new(vec![vec![(1, 1.0), (2, 2.0), (3, 3.0)]], |_, _| {
        CollectionStrategy::Continue
    })
    .with_num_estimated(100);
    let it = TopKIterator::new(source, None, NonZeroUsize::new(3).unwrap(), asc);

    assert_eq!(it.num_estimated(), 3);
}

// ── Unfiltered path ───────────────────────────────────────────────────────────

#[test]
fn unfiltered_yields_batch_in_source_order() {
    // Unfiltered mode streams directly from the batch without sorting.
    let source = MockScoreSource::new(vec![vec![(3, 0.1), (1, 0.5), (2, 0.9)]], |_, _| {
        CollectionStrategy::Continue
    });
    let mut it = TopKIterator::new(source, None, NonZeroUsize::new(10).unwrap(), asc);
    let ids: Vec<_> = std::iter::from_fn(|| it.read().unwrap().map(|r| r.doc_id)).collect();
    assert_eq!(ids, vec![3, 1, 2]);
}

#[test]
fn unfiltered_empty_source_is_immediate_eof() {
    let source = MockScoreSource::new(vec![], |_, _| CollectionStrategy::Continue);
    let mut it = TopKIterator::new(source, None, NonZeroUsize::new(5).unwrap(), asc);
    assert!(it.read().unwrap().is_none());
    assert!(it.at_eof());
}

#[test]
fn unfiltered_timeout_propagated() {
    use rqe_iterators::RQEIteratorError;
    use top_k::{ScoreSource, mock::MockScoreBatch};

    struct TimingOutSource;
    impl<'index> ScoreSource<'index> for TimingOutSource {
        type Batch = MockScoreBatch;
        fn next_batch(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError> {
            Err(RQEIteratorError::TimedOut)
        }
        fn num_estimated(&self) -> usize {
            0
        }
        fn rewind(&mut self) {}
        fn build_result(
            &self,
            doc_id: ffi::t_docId,
            _: f64,
        ) -> inverted_index::RSIndexResult<'index> {
            inverted_index::RSIndexResult::build_virt()
                .doc_id(doc_id)
                .build()
        }
        fn collection_strategy(&mut self, _: usize, _: usize) -> CollectionStrategy {
            CollectionStrategy::Continue
        }

        fn iterator_type(&self) -> rqe_iterator_type::IteratorType {
            rqe_iterator_type::IteratorType::Mock
        }
    }

    let mut it = TopKIterator::new(TimingOutSource, None, NonZeroUsize::new(5).unwrap(), asc);
    assert!(matches!(
        it.read().unwrap_err(),
        rqe_iterators::RQEIteratorError::TimedOut
    ));
}

// ── Batches intersection ──────────────────────────────────────────────────

#[test]
fn batches_overlap_intersection() {
    // Child: [1,3,5], Batch: [(1,0.9),(2,0.8),(3,0.7),(4,0.6),(5,0.5)]
    // Matches: 1→0.9, 3→0.7, 5→0.5  → best-first ASC: 5, 3, 1
    let source = MockScoreSource::new(
        vec![vec![(1, 0.9), (2, 0.8), (3, 0.7), (4, 0.6), (5, 0.5)]],
        |_, _| CollectionStrategy::Continue,
    );
    let mut it = TopKIterator::new(
        source,
        Some(make_child(vec![1, 3, 5])),
        NonZeroUsize::new(10).unwrap(),
        asc,
    );
    let doc_ids: Vec<_> = std::iter::from_fn(|| it.read().unwrap().map(|r| r.doc_id)).collect();
    assert_eq!(doc_ids, vec![5, 3, 1]);
}

#[test]
fn batches_disjoint_yields_nothing() {
    let source = MockScoreSource::new(vec![vec![(1, 1.0), (2, 2.0), (3, 3.0)]], |_, _| {
        CollectionStrategy::Continue
    });
    let mut it = TopKIterator::new(
        source,
        Some(make_child(vec![10, 20])),
        NonZeroUsize::new(10).unwrap(),
        asc,
    );
    assert!(it.read().unwrap().is_none());
    assert!(it.at_eof());
}

#[test]
fn batches_empty_child_yields_nothing() {
    let source = MockScoreSource::new(vec![vec![(1, 1.0), (2, 2.0)]], |_, _| {
        CollectionStrategy::Continue
    });
    let mut it = TopKIterator::new(
        source,
        Some(make_child(vec![])),
        NonZeroUsize::new(10).unwrap(),
        asc,
    );
    assert!(it.read().unwrap().is_none());
    assert!(it.at_eof());
}

#[test]
fn batches_multiple_batches() {
    // Batch 1: [(1,3.0),(2,4.0)], Batch 2: [(3,1.0),(4,2.0)]
    // Child: [1,3,4]  Matches: 1→3.0, 3→1.0, 4→2.0  best-first → 3,4,1
    let source = MockScoreSource::new(
        vec![vec![(1, 3.0), (2, 4.0)], vec![(3, 1.0), (4, 2.0)]],
        |_, _| CollectionStrategy::Continue,
    );
    let mut it = TopKIterator::new(
        source,
        Some(make_child(vec![1, 3, 4])),
        NonZeroUsize::new(10).unwrap(),
        asc,
    );
    let doc_ids: Vec<_> = std::iter::from_fn(|| it.read().unwrap().map(|r| r.doc_id)).collect();
    assert_eq!(doc_ids, vec![3, 4, 1]);
}

#[test]
fn strategy_stop_stops_after_first_batch() {
    let source = MockScoreSource::new(
        vec![
            vec![(1, 1.0), (2, 2.0)],
            vec![(3, 0.5)], // NOT fetched
            vec![(4, 0.1)], // NOT fetched
        ],
        |_, _| CollectionStrategy::Stop,
    );
    let mut it = TopKIterator::new(
        source,
        Some(make_child(vec![1, 2, 3, 4])),
        NonZeroUsize::new(10).unwrap(),
        asc,
    );
    let doc_ids: Vec<_> = std::iter::from_fn(|| it.read().unwrap().map(|r| r.doc_id)).collect();
    assert_eq!(it.metrics.num_batches, 1);
    assert_eq!(doc_ids.len(), 2);
}

#[test]
fn strategy_switch_to_batches_rewinds() {
    // Call 0 → SwitchToBatches (rewinds source+child, restarts loop).
    // Call 1 → Stop (to exit on the second pass).
    let call_count = std::cell::Cell::new(0u32);
    let strategy = move |_: usize, _: usize| {
        let n = call_count.get();
        call_count.set(n + 1);
        if n == 0 {
            CollectionStrategy::SwitchToBatches
        } else {
            CollectionStrategy::Stop
        }
    };
    let source = MockScoreSource::new(vec![vec![(1, 1.0), (2, 2.0)]], strategy);
    let mut it = TopKIterator::new(
        source,
        Some(make_child(vec![1, 2])),
        NonZeroUsize::new(10).unwrap(),
        asc,
    );
    let doc_ids: Vec<_> = std::iter::from_fn(|| it.read().unwrap().map(|r| r.doc_id)).collect();
    assert_eq!(doc_ids, vec![1, 2]);
    assert_eq!(it.metrics.strategy_switches, 1);
}
