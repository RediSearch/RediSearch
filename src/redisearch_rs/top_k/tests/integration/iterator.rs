/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for [`TopKIterator`].

use std::{cmp::Ordering, num::NonZeroUsize};

use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use rqe_iterator_type::IteratorType;
use rqe_iterators::{
    IdList, RQEIterator, RQEIteratorError,
    c2rust::CRQEIterator,
    interop::{ProfileChildren, RQEIteratorWrapper},
};
use top_k::{
    BatchStrategy, ScoreSource, TopKIterator, TopKMode, mock::MockScoreBatch, mock::MockScoreSource,
};

// ── Error path stubs ─────────────────────────────────────────────────────────────

/// Child iterator whose `revalidate` unconditionally returns `Aborted`.
///
/// The `Ok` delegation path is covered by [`rqe_iterators::Empty`], which
/// already returns `Ok` from `revalidate`.  This stub only exists for the
/// case that cannot be expressed with any existing public iterator type.
struct AbortOnRevalidate;

impl<'index> rqe_iterators::RQEIterator<'index> for AbortOnRevalidate {
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        None
    }

    fn read(
        &mut self,
    ) -> Result<Option<&mut RSIndexResult<'index>>, rqe_iterators::RQEIteratorError> {
        Ok(None)
    }

    fn skip_to(
        &mut self,
        _doc_id: ffi::t_docId,
    ) -> Result<Option<rqe_iterators::SkipToOutcome<'_, 'index>>, rqe_iterators::RQEIteratorError>
    {
        unimplemented!()
    }

    fn revalidate(
        &mut self,
        _spec: &IndexSpecReadGuard,
    ) -> Result<rqe_iterators::RQEValidateStatus<'_, 'index>, rqe_iterators::RQEIteratorError> {
        Ok(rqe_iterators::RQEValidateStatus::Aborted)
    }

    fn rewind(&mut self) {}

    fn num_estimated(&self) -> usize {
        0
    }

    fn last_doc_id(&self) -> ffi::t_docId {
        0
    }

    fn at_eof(&self) -> bool {
        true
    }

    fn type_(&self) -> rqe_iterator_type::IteratorType {
        rqe_iterator_type::IteratorType::Mock
    }

    fn intersection_sort_weight(&self, _: bool) -> f64 {
        1.0
    }
}

/// [`ScoreSource`] whose [`ScoreSource::next_batch`] unconditionally returns [`RQEIteratorError::TimedOut`].
///
/// Used to verify that timeout errors propagate correctly through [`TopKIterator`].
struct TimingOutSource;

impl ScoreSource for TimingOutSource {
    type Batch = MockScoreBatch;

    fn next_batch(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError> {
        Err(RQEIteratorError::TimedOut)
    }

    fn lookup_score(&mut self, _: ffi::t_docId) -> Option<f64> {
        None
    }

    fn num_estimated(&self) -> usize {
        0
    }

    fn rewind(&mut self) {}

    fn build_result<'r>(&self, doc_id: ffi::t_docId, _: f64) -> RSIndexResult<'r>
    where
        Self: 'r,
    {
        RSIndexResult::build_virt().doc_id(doc_id).build()
    }

    fn batch_strategy(&mut self, _: usize, _: usize) -> BatchStrategy {
        BatchStrategy::Continue
    }

    fn iterator_type(&self) -> rqe_iterator_type::IteratorType {
        rqe_iterator_type::IteratorType::Mock
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Ascending comparator: lower score is better (e.g. vector distance).
fn asc(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

fn make_child<'a>(ids: Vec<ffi::t_docId>) -> Box<dyn RQEIterator<'a> + 'a> {
    Box::new(IdList::<true>::new(ids))
}

/// Wrap an [`IdList`] in a [`CRQEIterator`] so the typed-child `TopKIterator`
/// variant (which supports [`ProfileChildren`]) can be exercised in tests.
fn make_crqe_child(ids: Vec<ffi::t_docId>) -> CRQEIterator {
    let it = IdList::<true>::new(ids);
    let ptr = RQEIteratorWrapper::boxed_new(it);
    // SAFETY: `boxed_new` returns a non-null `Box::into_raw` pointer.
    let ptr = unsafe { std::ptr::NonNull::new_unchecked(ptr) };
    // SAFETY: `ptr` is a valid, owning, non-null `QueryIterator` pointer produced
    // by `RQEIteratorWrapper::boxed_new`, satisfying all `CRQEIterator::new` preconditions.
    unsafe { CRQEIterator::new(ptr) }
}

// ── State machine ─────────────────────────────────────────────────────────────

#[test]
fn read_triggers_collection_on_first_call() {
    let source = MockScoreSource::new(vec![vec![(1, 1.0), (2, 2.0)]], vec![], |_, _| {
        BatchStrategy::Continue
    });
    let mut it = TopKIterator::new_unfiltered(source, NonZeroUsize::new(5).unwrap(), asc);

    assert!(!it.at_eof());
    let result = it.read().unwrap().expect("should have a result");
    assert_eq!(result.doc_id, 1);
}

#[test]
fn rewind_resets_to_not_started() {
    let source = MockScoreSource::new(vec![vec![(1, 1.0), (2, 2.0)]], vec![], |_, _| {
        BatchStrategy::Continue
    });
    let mut it = TopKIterator::new_unfiltered(source, NonZeroUsize::new(5).unwrap(), asc);
    it.read().unwrap();
    it.read().unwrap();
    let eof = it.read().unwrap();

    assert!(eof.is_none());
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
    let source = MockScoreSource::new(vec![vec![(1, 1.0)]], vec![], |_, _| BatchStrategy::Continue);
    let mut it = TopKIterator::new_unfiltered(source, NonZeroUsize::new(5).unwrap(), asc);
    it.read().unwrap();
    let eof = it.read().unwrap();
    assert!(eof.is_none());
    assert!(it.at_eof());
}

#[test]
fn last_doc_id_starts_at_zero_tracks_reads_and_resets_on_rewind() {
    let source = MockScoreSource::new(vec![vec![(1, 1.0), (2, 2.0)]], vec![], |_, _| {
        BatchStrategy::Continue
    });
    let mut it = TopKIterator::new_unfiltered(source, NonZeroUsize::new(5).unwrap(), asc);

    assert_eq!(it.last_doc_id(), 0);

    it.read().unwrap();
    assert_eq!(it.last_doc_id(), 1);
    it.read().unwrap();
    assert_eq!(it.last_doc_id(), 2);

    it.rewind();
    assert_eq!(it.last_doc_id(), 0);
}

#[test]
fn num_estimated_capped_at_k() {
    let source = MockScoreSource::new(vec![vec![(1, 1.0), (2, 2.0), (3, 3.0)]], vec![], |_, _| {
        BatchStrategy::Continue
    })
    .with_num_estimated(100);
    let it = TopKIterator::new_unfiltered(source, NonZeroUsize::new(3).unwrap(), asc);

    assert_eq!(it.num_estimated(), 3);
}

// ── Unfiltered path ───────────────────────────────────────────────────────────

#[test]
fn unfiltered_yields_batch_in_source_order() {
    // Scores are descending (0.9 → 0.5 → 0.1) while doc IDs are ascending.
    // If the iterator re-sorted by score it would yield [3, 2, 1]; source order gives [1, 2, 3].
    let source = MockScoreSource::new(vec![vec![(1, 0.9), (2, 0.5), (3, 0.1)]], vec![], |_, _| {
        BatchStrategy::Continue
    });
    let mut it = TopKIterator::new_unfiltered(source, NonZeroUsize::new(10).unwrap(), asc);
    let ids: Vec<_> = std::iter::from_fn(|| it.read().unwrap().map(|r| r.doc_id)).collect();
    assert_eq!(ids, vec![1, 2, 3]);
}

#[test]
fn unfiltered_empty_source_is_immediate_eof() {
    let source = MockScoreSource::new(vec![], vec![], |_, _| BatchStrategy::Continue);
    let mut it = TopKIterator::new_unfiltered(source, NonZeroUsize::new(5).unwrap(), asc);
    assert!(it.read().unwrap().is_none());
    assert!(it.at_eof());
}

// ── Revalidation ──────────────────────────────────────────────────────────────

#[test]
fn revalidate_without_child_returns_ok() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let source = MockScoreSource::new(vec![vec![(1, 1.0)]], vec![], |_, _| BatchStrategy::Continue);
    let mut it = TopKIterator::new_unfiltered(source, NonZeroUsize::new(5).unwrap(), asc);
    let status = it.revalidate(&mock_ctx.spec_read()).unwrap();
    assert_eq!(status, rqe_iterators::RQEValidateStatus::Ok);
}

#[test]
fn revalidate_with_child_delegates_ok() {
    // rqe_iterators::Empty::revalidate returns Ok, so it is the natural stand-in
    // for any child iterator that leaves the parent in a valid state.
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let source = MockScoreSource::new(vec![vec![(1, 1.0)]], vec![], |_, _| BatchStrategy::Continue);
    let child: Box<dyn rqe_iterators::RQEIterator<'_>> = Box::new(rqe_iterators::Empty::default());
    let mut it = TopKIterator::new(source, child, NonZeroUsize::new(5).unwrap(), asc);
    let status = it.revalidate(&mock_ctx.spec_read()).unwrap();

    assert_eq!(status, rqe_iterators::RQEValidateStatus::Ok);
}

#[test]
fn revalidate_with_child_delegates_aborted() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let source = MockScoreSource::new(vec![vec![(1, 1.0)]], vec![], |_, _| BatchStrategy::Continue);
    let child: Box<dyn rqe_iterators::RQEIterator<'_>> = Box::new(AbortOnRevalidate);
    let mut it = TopKIterator::new(source, child, NonZeroUsize::new(5).unwrap(), asc);
    let status = it.revalidate(&mock_ctx.spec_read()).unwrap();
    assert_eq!(status, rqe_iterators::RQEValidateStatus::Aborted);
}

#[test]
fn unfiltered_timeout_propagated() {
    let mut it = TopKIterator::new_unfiltered(TimingOutSource, NonZeroUsize::new(5).unwrap(), asc);
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
        vec![],
        |_, _| BatchStrategy::Continue,
    );
    let mut it = TopKIterator::new(
        source,
        make_child(vec![1, 3, 5]),
        NonZeroUsize::new(10).unwrap(),
        asc,
    );
    let doc_ids: Vec<_> = std::iter::from_fn(|| it.read().unwrap().map(|r| r.doc_id)).collect();
    assert_eq!(doc_ids, vec![5, 3, 1]);
}

#[test]
fn batches_disjoint_yields_nothing() {
    let source = MockScoreSource::new(vec![vec![(1, 1.0), (2, 2.0), (3, 3.0)]], vec![], |_, _| {
        BatchStrategy::Continue
    });
    let mut it = TopKIterator::new(
        source,
        make_child(vec![10, 20]),
        NonZeroUsize::new(10).unwrap(),
        asc,
    );
    assert!(it.read().unwrap().is_none());
    assert!(it.at_eof());
}

#[test]
fn batches_empty_child_yields_nothing() {
    let source = MockScoreSource::new(vec![vec![(1, 1.0), (2, 2.0)]], vec![], |_, _| {
        BatchStrategy::Continue
    });
    let mut it = TopKIterator::new(
        source,
        make_child(vec![]),
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
        vec![],
        |_, _| BatchStrategy::Continue,
    );
    let mut it = TopKIterator::new(
        source,
        make_child(vec![1, 3, 4]),
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
        vec![],
        |_, _| BatchStrategy::Stop,
    );
    let mut it = TopKIterator::new(
        source,
        make_child(vec![1, 2, 3, 4]),
        NonZeroUsize::new(10).unwrap(),
        asc,
    );
    let doc_ids: Vec<_> = std::iter::from_fn(|| it.read().unwrap().map(|r| r.doc_id)).collect();
    assert_eq!(doc_ids.len(), 2);
}

/// When collection fails mid-batch, the heap holds partial results.
/// A subsequent rewind() followed by read() must not produce duplicate doc IDs.
#[test]
fn rewind_after_mid_collect_error_does_not_retain_stale_heap() {
    use rqe_iterators::RQEIteratorError;
    use top_k::{BatchStrategy, ScoreSource, mock::MockScoreBatch};

    struct OnceErrorSource {
        batch_call: u32,
        rewind_count: u32,
    }

    impl ScoreSource for OnceErrorSource {
        type Batch = MockScoreBatch;

        fn next_batch(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError> {
            let n = self.batch_call;
            self.batch_call += 1;
            if self.rewind_count == 0 {
                // Session 1: return one batch then error, leaving the heap dirty.
                match n {
                    0 => Ok(Some(MockScoreBatch::new(vec![(1, 1.0), (3, 3.0)]))),
                    _ => Err(RQEIteratorError::TimedOut),
                }
            } else {
                // Session 2 (after rewind): return the same batch then stop cleanly.
                match n {
                    0 => Ok(Some(MockScoreBatch::new(vec![(1, 1.0), (3, 3.0)]))),
                    _ => Ok(None),
                }
            }
        }

        fn num_estimated(&self) -> usize {
            10
        }

        fn rewind(&mut self) {
            self.batch_call = 0;
            self.rewind_count += 1;
        }

        fn lookup_score(&mut self, _: ffi::t_docId) -> Option<f64> {
            Some(10f64)
        }

        fn build_result<'r>(&self, doc_id: ffi::t_docId, _: f64) -> RSIndexResult<'r>
        where
            Self: 'r,
        {
            RSIndexResult::build_virt().doc_id(doc_id).build()
        }

        fn batch_strategy(&mut self, _heap_count: usize, _k: usize) -> BatchStrategy {
            BatchStrategy::Continue
        }

        fn iterator_type(&self) -> rqe_iterator_type::IteratorType {
            rqe_iterator_type::IteratorType::Mock
        }
    }

    let source = OnceErrorSource {
        batch_call: 0,
        rewind_count: 0,
    };
    let mut it = TopKIterator::new(
        source,
        make_child(vec![1, 3]),
        NonZeroUsize::new(10).unwrap(),
        asc,
    );

    // Session 1: the source returns one batch (doc 1, doc 3 land in the heap)
    // then errors before finalize_collection() can drain it.
    assert!(matches!(it.read().unwrap_err(), RQEIteratorError::TimedOut));

    it.rewind();

    // Session 2: the source replays the same batch.  If the heap was not cleared
    // by rewind(), doc 1 and doc 3 are added a second time and appear twice.
    let doc_ids: Vec<_> = std::iter::from_fn(|| it.read().unwrap().map(|r| r.doc_id)).collect();
    assert_eq!(doc_ids, vec![1, 3]);
}

#[test]
fn strategy_switch_to_adhoc() {
    // First call to batch_strategy → SwitchToAdhoc; subsequent → Continue.
    let mut call_count = 0u32;
    let strategy = move |_heap: usize, _k: usize| {
        call_count += 1;
        if call_count == 1 {
            BatchStrategy::SwitchToAdhoc
        } else {
            BatchStrategy::Continue
        }
    };
    // Batch finds doc 1 (score 3.0). Adhoc finds docs 2 (1.0) and 3 (2.0).
    // Doc 1 is not in the adhoc scores map, so it survives only from the batch phase.
    let source = MockScoreSource::new(vec![vec![(1, 3.0)]], vec![(2, 1.0), (3, 2.0)], strategy);
    let mut it = TopKIterator::new(
        source,
        make_child(vec![1, 2, 3]),
        NonZeroUsize::new(10).unwrap(),
        asc,
    );
    let doc_ids: Vec<_> = std::iter::from_fn(|| it.read().unwrap().map(|r| r.doc_id)).collect();
    // Best-first ASC: 2(1.0), 3(2.0), 1(3.0)
    assert_eq!(doc_ids, vec![2, 3, 1]);
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
            BatchStrategy::SwitchToBatches
        } else {
            BatchStrategy::Stop
        }
    };
    let source = MockScoreSource::new(vec![vec![(1, 1.0), (2, 2.0)]], vec![], strategy);
    let mut it = TopKIterator::new(
        source,
        make_child(vec![1, 2]),
        NonZeroUsize::new(10).unwrap(),
        asc,
    );
    let doc_ids: Vec<_> = std::iter::from_fn(|| it.read().unwrap().map(|r| r.doc_id)).collect();
    assert_eq!(doc_ids, vec![1, 2]);
}

// ── Adhoc-BF ──────────────────────────────────────────────────────────────

#[test]
fn adhoc_scores_known_docs_only() {
    // Child: [1,2,3,4,5], source has scores for [2,4] only → yields [2,4].
    let source = MockScoreSource::new(vec![], vec![(2, 1.0), (4, 2.0)], |_, _| {
        BatchStrategy::Continue
    });
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(make_child(vec![1, 2, 3, 4, 5])),
        NonZeroUsize::new(10).unwrap(),
        asc,
        TopKMode::AdhocBF,
    );
    let doc_ids: Vec<_> = std::iter::from_fn(|| it.read().unwrap().map(|r| r.doc_id)).collect();
    assert_eq!(doc_ids, vec![2, 4]);
}

#[test]
fn adhoc_early_stop_when_heap_full() {
    // k=2; strategy signals Stop when heap reaches capacity.
    let source = MockScoreSource::new(vec![], vec![(1, 1.0), (2, 2.0), (3, 0.5)], |heap, k| {
        if heap >= k {
            BatchStrategy::Stop
        } else {
            BatchStrategy::Continue
        }
    });
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(make_child(vec![1, 2, 3])),
        NonZeroUsize::new(2).unwrap(),
        asc,
        TopKMode::AdhocBF,
    );
    let doc_ids: Vec<_> = std::iter::from_fn(|| it.read().unwrap().map(|r| r.doc_id)).collect();
    assert_eq!(doc_ids.len(), 2);
}

#[test]
fn adhoc_child_eof_returns_what_was_found() {
    let source = MockScoreSource::new(vec![], vec![(1, 0.1), (2, 0.2)], |_, _| {
        BatchStrategy::Continue
    });
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(make_child(vec![1, 2])),
        NonZeroUsize::new(10).unwrap(),
        asc,
        TopKMode::AdhocBF,
    );
    let doc_ids: Vec<_> = std::iter::from_fn(|| it.read().unwrap().map(|r| r.doc_id)).collect();
    assert_eq!(doc_ids, vec![1, 2]);
}

#[test]
fn adhoc_empty_child_is_eof() {
    let source = MockScoreSource::new(vec![], vec![], |_, _| BatchStrategy::Continue);
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(make_child(vec![])),
        NonZeroUsize::new(10).unwrap(),
        asc,
        TopKMode::AdhocBF,
    );
    assert!(it.read().unwrap().is_none());
    assert!(it.at_eof());
}

// ── ProfileChildren ───────────────────────────────────────────────────────────

#[test]
fn profile_children_wraps_child_in_profile_node() {
    // Verify that `profile_children()` wraps the filter child in a Profile
    // iterator without changing the results produced by the TopKIterator.
    let source = MockScoreSource::new(vec![vec![(1, 1.0), (2, 2.0), (3, 0.5)]], vec![], |_, _| {
        BatchStrategy::Continue
    });
    let child = make_crqe_child(vec![1, 2, 3]);
    let it = TopKIterator::new(source, child, NonZeroUsize::new(10).unwrap(), asc);

    let mut profiled = it.profile_children();

    // Child must now be a Profile wrapper.
    assert_eq!(
        profiled.child().map(|c| c.type_),
        Some(IteratorType::Profile),
        "filter child should be wrapped in a Profile node after profile_children()"
    );

    // Results must be unchanged: all three docs, sorted best-first (asc).
    let doc_ids: Vec<_> =
        std::iter::from_fn(|| profiled.read().unwrap().map(|r| r.doc_id)).collect();
    assert_eq!(doc_ids, vec![3, 1, 2]);
}

#[test]
fn profile_children_with_no_child_is_identity() {
    // Unfiltered path: profile_children() is a no-op on the child (there is none).
    let source = MockScoreSource::new(vec![vec![(1, 1.0), (2, 2.0)]], vec![], |_, _| {
        BatchStrategy::Continue
    });
    let it = TopKIterator::<_, CRQEIterator>::new_with_mode(
        source,
        None,
        NonZeroUsize::new(10).unwrap(),
        asc,
        TopKMode::Unfiltered,
    );

    let mut profiled = it.profile_children();

    assert!(profiled.child().is_none());
    let doc_ids: Vec<_> =
        std::iter::from_fn(|| profiled.read().unwrap().map(|r| r.doc_id)).collect();
    assert_eq!(doc_ids, vec![1, 2]);
}
