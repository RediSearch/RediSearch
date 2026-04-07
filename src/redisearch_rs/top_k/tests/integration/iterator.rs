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

use rqe_iterators::RQEIterator;
use top_k::{TopKIterator, mock::MockScoreSource};

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Ascending comparator: lower score is better (e.g. vector distance).
fn asc(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

// ── State machine ─────────────────────────────────────────────────────────────

#[test]
fn read_triggers_collection_on_first_call() {
    let source = MockScoreSource::new(vec![vec![(1, 1.0), (2, 2.0)]]);
    let mut it = TopKIterator::new(source, None, NonZeroUsize::new(5).unwrap(), asc);
    assert!(!it.at_eof());
    let result = it.read().unwrap().expect("should have a result");
    assert_eq!(result.doc_id, 1);
}

#[test]
fn rewind_resets_to_not_started() {
    let source = MockScoreSource::new(vec![vec![(1, 1.0), (2, 2.0)]]);
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
    let source = MockScoreSource::new(vec![vec![(1, 1.0)]]);
    let mut it = TopKIterator::new(source, None, NonZeroUsize::new(5).unwrap(), asc);
    it.read().unwrap();
    let eof = it.read().unwrap();
    assert!(eof.is_none());
    assert!(it.at_eof());
}

#[test]
fn num_estimated_capped_at_k() {
    let source =
        MockScoreSource::new(vec![vec![(1, 1.0), (2, 2.0), (3, 3.0)]]).with_num_estimated(100);
    let it = TopKIterator::new(source, None, NonZeroUsize::new(3).unwrap(), asc);
    assert_eq!(it.num_estimated(), 3);
}

// ── Unfiltered path ───────────────────────────────────────────────────────────

#[test]
fn unfiltered_yields_batch_in_source_order() {
    // Unfiltered mode streams directly from the batch without sorting.
    let source = MockScoreSource::new(vec![vec![(3, 0.1), (1, 0.5), (2, 0.9)]]);
    let mut it = TopKIterator::new(source, None, NonZeroUsize::new(10).unwrap(), asc);
    let ids: Vec<_> = std::iter::from_fn(|| it.read().unwrap().map(|r| r.doc_id)).collect();
    assert_eq!(ids, vec![3, 1, 2]);
}

#[test]
fn unfiltered_empty_source_is_immediate_eof() {
    let source = MockScoreSource::new(vec![]);
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
