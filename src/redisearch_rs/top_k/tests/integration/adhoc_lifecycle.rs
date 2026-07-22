/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the `begin_adhoc` / `end_adhoc` lifecycle hooks on
//! [`top_k::ScoreSource`]: verifies the iterator brackets every adhoc scan
//! with exactly one begin and one end, including when the child errors
//! mid-loop.

use std::{cmp::Ordering, marker::PhantomData, num::NonZeroUsize};

use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use rqe_core::DocId;
use rqe_iterators::{IdList, RQEIterator, RQEIteratorError};
use top_k::{
    BatchStrategy, ScoreSource, ScoredResult, TopKIterator, TopKMode, mock::MockScoreBatch,
    mock::MockScoreSource,
};

fn asc(a: &f64, b: &f64) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

fn make_child<'a>(ids: Vec<DocId>) -> Box<dyn RQEIterator<'a> + 'a> {
    Box::new(IdList::<true>::new(ids))
}

/// A `ScoreSource` that remembers how it was called, so tests can check it.
#[derive(Default)]
struct CallCountingScoreSource {
    begin_calls: u32,
    end_calls: u32,
    lookups_after_begin_before_end: u32,
    lookups_outside_scan: u32,
    rerank: bool,
    rerank_calls: u32,
}

impl ScoreSource for CallCountingScoreSource {
    type Batch = MockScoreBatch;

    fn next_batch(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError> {
        Ok(None)
    }

    fn lookup_score(&mut self, _: DocId) -> Option<f64> {
        if self.begin_calls > self.end_calls {
            self.lookups_after_begin_before_end += 1;
        } else {
            self.lookups_outside_scan += 1;
        }
        Some(1.0)
    }

    fn num_estimated(&self) -> usize {
        0
    }

    fn rewind(&mut self) {}

    fn build_result<'r>(&self, doc_id: DocId, _: f64) -> RSIndexResult<'r>
    where
        Self: 'r,
    {
        RSIndexResult::build_virt().doc_id(doc_id).build()
    }

    fn attach_score_metric<'r>(&self, _result: &mut RSIndexResult<'r>, _score: f64)
    where
        Self: 'r,
    {
    }

    fn batch_strategy(&mut self, _: usize, _: usize) -> BatchStrategy {
        BatchStrategy::Continue
    }

    fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
        Ok(())
    }

    fn begin_adhoc(&mut self) {
        self.begin_calls += 1;
    }

    fn end_adhoc(&mut self) {
        self.end_calls += 1;
    }

    fn should_rerank(&self) -> bool {
        self.rerank
    }

    fn rerank(&mut self, _results: &mut [ScoredResult]) {
        assert!(
            self.begin_calls > self.end_calls,
            "Rerank must run inside the scan window, before `end_adhoc`."
        );
        self.rerank_calls += 1;
    }

    fn iterator_type(&self) -> rqe_iterator_type::IteratorType {
        rqe_iterator_type::IteratorType::Mock
    }
}

#[test]
fn hooks_called_once_per_scan_with_lookups_in_between() {
    let mut it = TopKIterator::new_with_mode(
        CallCountingScoreSource::default(),
        Some(make_child(vec![1, 2, 3])),
        NonZeroUsize::new(10).unwrap(),
        asc,
        TopKMode::AdhocBF,
    );
    while it.read().unwrap().is_some() {}

    let source = it.source();
    assert_eq!(source.begin_calls, 1, "begin_adhoc must run exactly once");
    assert_eq!(source.end_calls, 1, "end_adhoc must run exactly once");
    assert_eq!(
        source.lookups_after_begin_before_end, 3,
        "every lookup_score must occur inside the begin/end window"
    );
    assert_eq!(source.lookups_outside_scan, 0);
}

/// Child that yields one doc, then returns `TimedOut`. Used to verify that
/// `?` propagation out of the scan loop still triggers `end_adhoc` via the
/// `AdhocScope` RAII guard.
struct ErrOnSecondRead<'index> {
    n: u32,
    result: RSIndexResult<'index>,
    _marker: PhantomData<&'index ()>,
}

impl<'index> ErrOnSecondRead<'index> {
    fn new() -> Self {
        Self {
            n: 0,
            result: RSIndexResult::build_virt().build(),
            _marker: PhantomData,
        }
    }
}

impl<'index> RQEIterator<'index> for ErrOnSecondRead<'index> {
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        None
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.n += 1;
        if self.n == 1 {
            self.result.doc_id = 1;
            Ok(Some(&mut self.result))
        } else {
            Err(RQEIteratorError::TimedOut)
        }
    }

    fn skip_to(
        &mut self,
        _: DocId,
    ) -> Result<Option<rqe_iterators::SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        unimplemented!()
    }

    fn revalidate(
        &mut self,
        _: &IndexSpecReadGuard,
    ) -> Result<rqe_iterators::RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        Ok(rqe_iterators::RQEValidateStatus::Ok)
    }

    fn rewind(&mut self) {
        self.n = 0;
    }

    fn num_estimated(&self) -> usize {
        2
    }

    fn last_doc_id(&self) -> DocId {
        0
    }

    fn at_eof(&self) -> bool {
        false
    }

    fn type_(&self) -> rqe_iterator_type::IteratorType {
        rqe_iterator_type::IteratorType::Mock
    }

    fn intersection_sort_weight(&self, _: bool) -> f64 {
        1.0
    }
}

#[test]
fn end_adhoc_runs_when_child_errors_midscan() {
    let child: Box<dyn RQEIterator<'_>> = Box::new(ErrOnSecondRead::new());
    let mut it = TopKIterator::new_with_mode(
        CallCountingScoreSource::default(),
        Some(child),
        NonZeroUsize::new(10).unwrap(),
        asc,
        TopKMode::AdhocBF,
    );
    assert!(matches!(it.read().unwrap_err(), RQEIteratorError::TimedOut));

    let source = it.source();
    assert_eq!(source.begin_calls, 1);
    assert_eq!(
        source.end_calls, 1,
        "end_adhoc must run when child.read()? bails out"
    );
}

#[test]
fn rerank_runs_once_after_clean_scan() {
    let source = CallCountingScoreSource {
        rerank: true,
        ..Default::default()
    };
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(make_child(vec![1, 2, 3])),
        NonZeroUsize::new(10).unwrap(),
        asc,
        TopKMode::AdhocBF,
    );
    while it.read().unwrap().is_some() {}

    let source = it.source();
    assert_eq!(
        source.rerank_calls, 1,
        "rerank must run once after a clean adhoc scan"
    );
    assert_eq!(source.end_calls, 1);
}

#[test]
fn rerank_skipped_on_timeout() {
    let source = CallCountingScoreSource {
        rerank: true,
        ..Default::default()
    };
    let child: Box<dyn RQEIterator<'_>> = Box::new(ErrOnSecondRead::new());
    let mut it = TopKIterator::new_with_mode(
        source,
        Some(child),
        NonZeroUsize::new(10).unwrap(),
        asc,
        TopKMode::AdhocBF,
    );
    assert!(matches!(it.read().unwrap_err(), RQEIteratorError::TimedOut));

    let source = it.source();
    assert_eq!(
        source.rerank_calls, 0,
        "rerank must not run when the scan times out"
    );
    assert_eq!(
        source.end_calls, 1,
        "end_adhoc still runs on the error path"
    );
}

#[test]
fn rerank_reorders_topk_by_exact_scores() {
    // Adhoc (approximate) scores order the docs 1 < 2 < 3 (ascending = better).
    // The exact rerank scores invert that to 3 < 2 < 1, so the final yield
    // order must follow the reranked scores, not the adhoc ones.
    let source = MockScoreSource::new(vec![], vec![(1, 0.1), (2, 0.2), (3, 0.3)], |_, _| {
        BatchStrategy::Continue
    })
    .with_rerank(vec![(1, 0.30), (2, 0.20), (3, 0.10)]);

    let mut it = TopKIterator::new_with_mode(
        source,
        Some(make_child(vec![1, 2, 3])),
        NonZeroUsize::new(3).unwrap(),
        asc,
        TopKMode::AdhocBF,
    );

    let mut ids = Vec::new();
    while let Some(r) = it.read().unwrap() {
        ids.push(r.doc_id);
    }
    assert_eq!(ids, vec![3, 2, 1], "yield order must follow exact scores");
}

#[test]
fn rerank_keeps_adhoc_score_for_unmapped_doc() {
    // Only doc 1 has an exact score; docs 2 and 3 keep their adhoc scores,
    // mirroring the disk path's handling of labels with no exact distance.
    // Adhoc: 1→0.5, 2→0.1, 3→0.9. After rerank doc 1 becomes the best (0.05),
    // so the order is 1 < 2 < 3.
    let source = MockScoreSource::new(vec![], vec![(1, 0.5), (2, 0.1), (3, 0.9)], |_, _| {
        BatchStrategy::Continue
    })
    .with_rerank(vec![(1, 0.05)]);

    let mut it = TopKIterator::new_with_mode(
        source,
        Some(make_child(vec![1, 2, 3])),
        NonZeroUsize::new(3).unwrap(),
        asc,
        TopKMode::AdhocBF,
    );

    let mut ids = Vec::new();
    while let Some(r) = it.read().unwrap() {
        ids.push(r.doc_id);
    }
    assert_eq!(ids, vec![1, 2, 3], "unmapped docs keep their adhoc score");
}
