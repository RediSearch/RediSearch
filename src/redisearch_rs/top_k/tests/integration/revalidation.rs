/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for [`TopKIterator::revalidate`].

use std::{cmp::Ordering, num::NonZeroUsize};

use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use rqe_core::DocId;
use rqe_iterators::{RQEIterator, RQEValidateStatus};
use top_k::{BatchStrategy, TopKIterator, mock::MockScoreSource};

/// Ascending comparator: lower score is better (e.g. vector distance).
const fn asc() -> fn(a: &f64, b: &f64) -> Ordering {
    f64::total_cmp
}

/// Child iterator whose `revalidate` unconditionally returns `Aborted`.
///
/// The `Ok` delegation path is covered by [`rqe_iterators::Empty`], which
/// already returns `Ok` from `revalidate`.  This stub only exists for the
/// case that cannot be expressed with any existing public iterator type.
struct AbortOnRevalidate;

impl<'index> RQEIterator<'index> for AbortOnRevalidate {
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
        _doc_id: DocId,
    ) -> Result<Option<rqe_iterators::SkipToOutcome<'_, 'index>>, rqe_iterators::RQEIteratorError>
    {
        unimplemented!()
    }

    fn revalidate(
        &mut self,
        _spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, rqe_iterators::RQEIteratorError> {
        Ok(RQEValidateStatus::Aborted)
    }

    fn rewind(&mut self) {}

    fn num_estimated(&self) -> usize {
        0
    }

    fn last_doc_id(&self) -> DocId {
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

/// Child iterator whose `revalidate` reports `Moved` to a new current document.
///
/// Used to verify the parent collapses a moved child to `Ok` rather than
/// surfacing the child's reposition as its own.
struct MovedOnRevalidate<'index> {
    current: RSIndexResult<'index>,
}

impl<'index> MovedOnRevalidate<'index> {
    fn new() -> Self {
        Self {
            current: RSIndexResult::build_virt().doc_id(7).build(),
        }
    }
}

impl<'index> RQEIterator<'index> for MovedOnRevalidate<'index> {
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
        _doc_id: DocId,
    ) -> Result<Option<rqe_iterators::SkipToOutcome<'_, 'index>>, rqe_iterators::RQEIteratorError>
    {
        unimplemented!()
    }

    fn revalidate(
        &mut self,
        _spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, rqe_iterators::RQEIteratorError> {
        Ok(RQEValidateStatus::Moved {
            current: Some(&mut self.current),
        })
    }

    fn rewind(&mut self) {}

    fn num_estimated(&self) -> usize {
        0
    }

    fn last_doc_id(&self) -> DocId {
        7
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
fn without_child_returns_ok() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let source = MockScoreSource::new(vec![vec![(1, 1.0)]], vec![], |_, _| BatchStrategy::Continue);
    let mut it = TopKIterator::new_unfiltered(source, NonZeroUsize::new(5).unwrap(), asc());
    let status = it.revalidate(&mock_ctx.spec_read()).unwrap();
    assert_eq!(status, RQEValidateStatus::Ok);
}

#[test]
fn with_child_delegates_ok() {
    // rqe_iterators::Empty::revalidate returns Ok, so it is the natural stand-in
    // for any child iterator that leaves the parent in a valid state.
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let source = MockScoreSource::new(vec![vec![(1, 1.0)]], vec![], |_, _| BatchStrategy::Continue);
    let child: Box<dyn RQEIterator<'_>> = Box::new(rqe_iterators::Empty::default());
    let mut it = TopKIterator::new(source, child, NonZeroUsize::new(5).unwrap(), asc());
    let status = it.revalidate(&mock_ctx.spec_read()).unwrap();
    assert_eq!(status, RQEValidateStatus::Ok);
}

#[test]
fn with_child_delegates_aborted() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let source = MockScoreSource::new(vec![vec![(1, 1.0)]], vec![], |_, _| BatchStrategy::Continue);
    let child: Box<dyn RQEIterator<'_>> = Box::new(AbortOnRevalidate);
    let mut it = TopKIterator::new(source, child, NonZeroUsize::new(5).unwrap(), asc());
    let status = it.revalidate(&mock_ctx.spec_read()).unwrap();
    assert_eq!(status, RQEValidateStatus::Aborted);
}

#[test]
fn moved_child_collapses_to_ok() {
    // We yield from our own score-ordered buffer, so a child that repositions
    // does not move our cursor: the parent must report Ok, not Moved.
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let source = MockScoreSource::new(vec![vec![(1, 1.0)]], vec![], |_, _| BatchStrategy::Continue);
    let child: Box<dyn RQEIterator<'_>> = Box::new(MovedOnRevalidate::new());
    let mut it = TopKIterator::new(source, child, NonZeroUsize::new(5).unwrap(), asc());
    let status = it.revalidate(&mock_ctx.spec_read()).unwrap();
    assert_eq!(status, RQEValidateStatus::Ok);
}
