/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for [`TopKIterator`].

use std::num::NonZeroUsize;

use rqe_iterators::RQEIterator;
use top_k::{TopKIterator, mock::MockScoreSource};

// ── State machine ─────────────────────────────────────────────────────────────

#[test]
fn read_triggers_collection_on_first_call() {
    let source = MockScoreSource::new(vec![vec![(1, 1.0), (2, 2.0)]]);
    let mut it = TopKIterator::new(source, None, NonZeroUsize::new(5).unwrap());
    assert!(!it.at_eof());
    let result = it.read().unwrap().expect("should have a result");
    assert_eq!(result.doc_id, 1);
}

#[test]
fn rewind_resets_to_not_started() {
    let source = MockScoreSource::new(vec![vec![(1, 1.0), (2, 2.0)]]);
    let mut it = TopKIterator::new(source, None, NonZeroUsize::new(5).unwrap());
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
    let source = MockScoreSource::new(vec![vec![(1, 1.0)]]);
    let mut it = TopKIterator::new(source, None, NonZeroUsize::new(5).unwrap());
    it.read().unwrap();
    let eof = it.read().unwrap();
    assert!(eof.is_none());
    assert!(it.at_eof());
}

#[test]
fn last_doc_id_starts_at_zero_tracks_reads_and_resets_on_rewind() {
    let source = MockScoreSource::new(vec![vec![(1, 1.0), (2, 2.0)]]);
    let mut it = TopKIterator::new(source, None, NonZeroUsize::new(5).unwrap());

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
    let source =
        MockScoreSource::new(vec![vec![(1, 1.0), (2, 2.0), (3, 3.0)]]).with_num_estimated(100);
    let it = TopKIterator::new(source, None, NonZeroUsize::new(3).unwrap());
    assert_eq!(it.num_estimated(), 3);
}

// ── Unfiltered path ───────────────────────────────────────────────────────────

#[test]
fn unfiltered_yields_batch_in_source_order() {
    // Unfiltered mode streams directly from the batch without sorting.
    let source = MockScoreSource::new(vec![vec![(3, 0.1), (1, 0.5), (2, 0.9)]]);
    let mut it = TopKIterator::new(source, None, NonZeroUsize::new(10).unwrap());
    let ids: Vec<_> = std::iter::from_fn(|| it.read().unwrap().map(|r| r.doc_id)).collect();
    assert_eq!(ids, vec![3, 1, 2]);
}

#[test]
fn unfiltered_empty_source_is_immediate_eof() {
    let source = MockScoreSource::new(vec![]);
    let mut it = TopKIterator::new(source, None, NonZeroUsize::new(5).unwrap());
    assert!(it.read().unwrap().is_none());
    assert!(it.at_eof());
}

// ── Revalidation ──────────────────────────────────────────────────────────────

/// Child iterator whose `revalidate` unconditionally returns `Aborted`.
///
/// The `Ok` delegation path is covered by [`rqe_iterators::Empty`], which
/// already returns `Ok` from `revalidate`.  This stub only exists for the
/// case that cannot be expressed with any existing public iterator type.
struct AbortOnRevalidate;

impl<'index> rqe_iterators::RQEIterator<'index> for AbortOnRevalidate {
    fn current(&mut self) -> Option<&mut inverted_index::RSIndexResult<'index>> {
        None
    }

    fn read(
        &mut self,
    ) -> Result<Option<&mut inverted_index::RSIndexResult<'index>>, rqe_iterators::RQEIteratorError>
    {
        Ok(None)
    }

    fn skip_to(
        &mut self,
        _doc_id: ffi::t_docId,
    ) -> Result<Option<rqe_iterators::SkipToOutcome<'_, 'index>>, rqe_iterators::RQEIteratorError>
    {
        unimplemented!()
    }

    unsafe fn revalidate(
        &mut self,
        _spec: std::ptr::NonNull<ffi::IndexSpec>,
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

#[test]
fn revalidate_without_child_returns_ok() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let source = MockScoreSource::new(vec![vec![(1, 1.0)]]);
    let mut it = TopKIterator::new(source, None, NonZeroUsize::new(5).unwrap());
    // SAFETY: child-less path returns Ok unconditionally; spec is never read.
    let status = unsafe { it.revalidate(mock_ctx.spec()) }.unwrap();
    assert_eq!(status, rqe_iterators::RQEValidateStatus::Ok);
}

#[test]
fn revalidate_with_child_delegates_ok() {
    // rqe_iterators::Empty::revalidate returns Ok, so it is the natural stand-in
    // for any child iterator that leaves the parent in a valid state.
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let source = MockScoreSource::new(vec![vec![(1, 1.0)]]);
    let child: Box<dyn rqe_iterators::RQEIterator<'_>> = Box::new(rqe_iterators::Empty::default());
    let mut it = TopKIterator::new(source, Some(child), NonZeroUsize::new(5).unwrap());
    // SAFETY: Empty::revalidate ignores spec; nothing is dereferenced.
    let status = unsafe { it.revalidate(mock_ctx.spec()) }.unwrap();
    assert_eq!(status, rqe_iterators::RQEValidateStatus::Ok);
}

#[test]
fn revalidate_with_child_delegates_aborted() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let source = MockScoreSource::new(vec![vec![(1, 1.0)]]);
    let child: Box<dyn rqe_iterators::RQEIterator<'_>> = Box::new(AbortOnRevalidate);
    let mut it = TopKIterator::new(source, Some(child), NonZeroUsize::new(5).unwrap());
    // SAFETY: AbortOnRevalidate::revalidate ignores spec; nothing is dereferenced.
    let status = unsafe { it.revalidate(mock_ctx.spec()) }.unwrap();
    assert_eq!(status, rqe_iterators::RQEValidateStatus::Aborted);
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

        fn iterator_type(&self) -> rqe_iterator_type::IteratorType {
            rqe_iterator_type::IteratorType::Mock
        }
    }

    let mut it = TopKIterator::new(TimingOutSource, None, NonZeroUsize::new(5).unwrap());
    assert!(matches!(
        it.read().unwrap_err(),
        rqe_iterators::RQEIteratorError::TimedOut
    ));
}
