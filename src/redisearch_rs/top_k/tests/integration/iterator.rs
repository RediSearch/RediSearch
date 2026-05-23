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

use index_result::RSIndexResult;
use rqe_iterators::{RQEIterator, RQEIteratorError};
use top_k::{ScoreSource, TopKIterator, mock::MockScoreBatch, mock::MockScoreSource};

// ── Error path stubs ─────────────────────────────────────────────────────────────

/// [`ScoreSource`] whose [`ScoreSource::next_batch`] unconditionally returns [`RQEIteratorError::TimedOut`].
///
/// Used to verify that timeout errors propagate correctly through [`TopKIterator`].
struct TimingOutSource;

impl ScoreSource for TimingOutSource {
    type Batch = MockScoreBatch;

    fn next_batch(&mut self) -> Result<Option<Self::Batch>, RQEIteratorError> {
        Err(RQEIteratorError::TimedOut)
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

    fn iterator_type(&self) -> rqe_iterator_type::IteratorType {
        rqe_iterator_type::IteratorType::Mock
    }
}

// ── State machine ─────────────────────────────────────────────────────────────

#[test]
fn read_triggers_collection_on_first_call() {
    let source = MockScoreSource::new(vec![vec![(1, 1.0), (2, 2.0)]]);
    let mut it = TopKIterator::<_, rqe_iterators::Empty>::new(source, None, NonZeroUsize::new(5).unwrap());
    assert!(!it.at_eof());
    let result = it.read().unwrap().expect("should have a result");
    assert_eq!(result.doc_id, 1);
}

#[test]
fn rewind_resets_to_not_started() {
    let source = MockScoreSource::new(vec![vec![(1, 1.0), (2, 2.0)]]);
    let mut it = TopKIterator::<_, rqe_iterators::Empty>::new(source, None, NonZeroUsize::new(5).unwrap());
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
    let source = MockScoreSource::new(vec![vec![(1, 1.0)]]);
    let mut it = TopKIterator::<_, rqe_iterators::Empty>::new(source, None, NonZeroUsize::new(5).unwrap());
    it.read().unwrap();
    let eof = it.read().unwrap();
    assert!(eof.is_none());
    assert!(it.at_eof());
}

#[test]
fn last_doc_id_starts_at_zero_tracks_reads_and_resets_on_rewind() {
    let source = MockScoreSource::new(vec![vec![(1, 1.0), (2, 2.0)]]);
    let mut it = TopKIterator::<_, rqe_iterators::Empty>::new(source, None, NonZeroUsize::new(5).unwrap());

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
    let it = TopKIterator::<_, rqe_iterators::Empty>::new(
        source,
        None,
        NonZeroUsize::new(3).unwrap(),
    );
    assert_eq!(it.num_estimated(), 3);
}

// ── Unfiltered path ───────────────────────────────────────────────────────────

#[test]
fn unfiltered_yields_batch_in_source_order() {
    let source = MockScoreSource::new(vec![vec![(1, 0.9), (2, 0.5), (3, 0.1)]]);
    let mut it = TopKIterator::<_, rqe_iterators::Empty>::new(
        source,
        None,
        NonZeroUsize::new(10).unwrap(),
    );
    let ids: Vec<_> = std::iter::from_fn(|| it.read().unwrap().map(|r| r.doc_id)).collect();
    assert_eq!(ids, vec![1, 2, 3]);
}

#[test]
fn unfiltered_empty_source_is_immediate_eof() {
    let source = MockScoreSource::new(vec![]);
    let mut it = TopKIterator::<_, rqe_iterators::Empty>::new(source, None, NonZeroUsize::new(5).unwrap());
    assert!(it.read().unwrap().is_none());
    assert!(it.at_eof());
}

#[test]
fn unfiltered_timeout_propagated() {
    let mut it = TopKIterator::<_, rqe_iterators::Empty>::new(
        TimingOutSource,
        None,
        NonZeroUsize::new(5).unwrap(),
    );
    assert!(matches!(
        it.read().unwrap_err(),
        rqe_iterators::RQEIteratorError::TimedOut
    ));
}
