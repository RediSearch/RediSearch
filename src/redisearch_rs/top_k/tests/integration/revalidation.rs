/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for [`TopKIterator::revalidate`] and its suspend/resume
//! successor ([`RQEIteratorBoxed::suspend`] / [`RQESuspendedIterator::resume`]).

use std::num::NonZeroUsize;

use index_result::{RSIndexResult, RSOffsetSlice, RSQueryTerm};
use index_spec::IndexSpecReadGuard;
use rqe_core::DocId;
use rqe_iterators::{
    IdList, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator, ResumeOutcome,
    SkipToOutcome, TypeErasedRQEIterator,
};
use rqe_iterators_test_utils::{ResumeOutcomeExt, revalidate_via_resume};
use top_k::{Ascending, BatchStrategy, TopKIterator, TopKMode, mock::MockScoreSource};

// ── suspend / resume ──────────────────────────────────────────────────────────

/// Which outcome [`SteerableChild`] reports from `resume`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChildOutcome {
    Ok,
    Moved,
    Aborted,
    Failed,
}

/// Child iterator over a fixed doc-id list whose `resume` reports a
/// caller-chosen [`ChildOutcome`].
///
/// The public iterators cover neither `Aborted` nor `Failed` on the resume path.
struct SteerableChild<'index> {
    docs: Vec<DocId>,
    /// Index of the next doc to yield.
    pos: usize,
    /// The record handed out by `read`/`current`; a virtual sentinel whose
    /// `doc_id` is rewritten in place, so it borrows nothing.
    current: RSIndexResult<'index>,
    /// Whether `current` holds a document the caller has been handed.
    positioned: bool,
    outcome: ChildOutcome,
}

impl<'index> SteerableChild<'index> {
    fn new(docs: impl Into<Vec<DocId>>, outcome: ChildOutcome) -> Self {
        Self {
            docs: docs.into(),
            pos: 0,
            current: RSIndexResult::build_virt().build(),
            positioned: false,
            outcome,
        }
    }

    /// Yield term records that own a query term, as a leaf term iterator does.
    fn with_query_term(mut self) -> Self {
        self.current = RSIndexResult::build_term()
            .borrowed_record(
                Some(RSQueryTerm::new("term", 1, 0)),
                RSOffsetSlice::from_slice(&[]),
            )
            .build();
        self
    }

    /// Position on `docs[pos]` and hand it out, or report depletion.
    fn yield_at_pos(&mut self) -> Option<&mut RSIndexResult<'index>> {
        match self.docs.get(self.pos) {
            Some(&doc_id) => {
                self.pos += 1;
                self.current.doc_id = doc_id;
                self.positioned = true;
                Some(&mut self.current)
            }
            None => {
                self.positioned = false;
                None
            }
        }
    }
}

impl<'index> RQEIterator<'index> for SteerableChild<'index> {
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        if self.positioned {
            Some(&mut self.current)
        } else {
            None
        }
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        Ok(self.yield_at_pos())
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        self.pos += self.docs[self.pos..].partition_point(|&id| id < doc_id);
        let Some(found) = self.yield_at_pos() else {
            return Ok(None);
        };
        Ok(Some(if found.doc_id == doc_id {
            SkipToOutcome::Found(found)
        } else {
            SkipToOutcome::NotFound(found)
        }))
    }

    fn rewind(&mut self) {
        self.pos = 0;
        self.positioned = false;
    }

    fn num_estimated(&self) -> usize {
        self.docs.len()
    }

    fn last_doc_id(&self) -> DocId {
        if self.positioned {
            self.current.doc_id
        } else {
            0
        }
    }

    fn at_eof(&self) -> bool {
        self.pos >= self.docs.len()
    }

    fn type_(&self) -> rqe_iterator_type::IteratorType {
        rqe_iterator_type::IteratorType::Mock
    }

    fn intersection_sort_weight(&self, _: bool) -> f64 {
        1.0
    }
}

impl<'index> RQEIteratorBoxed<'index> for SteerableChild<'index> {
    type Suspended = Self;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        // Nothing to weaken: the held record is a virtual sentinel.
        self
    }
}

impl<'query> RQESuspendedIterator<'query> for SteerableChild<'query> {
    type Resumed<'index>
        = SteerableChild<'index>
    where
        'query: 'index;

    fn resume<'index>(
        self: Box<Self>,
        _guard: &IndexSpecReadGuard<'index>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'index>>>, RQEIteratorError>
    where
        'query: 'index,
    {
        let outcome = self.outcome;
        if outcome == ChildOutcome::Failed {
            return Err(RQEIteratorError::TimedOut);
        }
        if outcome == ChildOutcome::Aborted {
            // Consumed and dropped, as an aborted resume must be.
            return Ok(ResumeOutcome::Aborted);
        }
        // SAFETY: the only lifetime-carrying field is a virtual sentinel that
        // borrows nothing, so re-labelling `'query` as the shorter `'index`
        // narrows a claim nothing relies on. The allocation is reused, as every
        // resume must.
        let resumed =
            unsafe { Box::from_raw(Box::into_raw(self).cast::<SteerableChild<'index>>()) };
        Ok(if outcome == ChildOutcome::Moved {
            ResumeOutcome::Moved(resumed)
        } else {
            ResumeOutcome::Ok(resumed)
        })
    }

    fn last_doc_id(&self) -> DocId {
        RQEIterator::last_doc_id(self)
    }

    fn num_estimated(&self) -> usize {
        self.docs.len()
    }
}

/// A source emitting one batch of `(doc_id, score)` pairs, ascending in score so
/// the yield order matches the doc order.
fn source_over(docs: &[DocId]) -> MockScoreSource {
    let batch: Vec<_> = docs
        .iter()
        .enumerate()
        .map(|(i, &doc_id)| (doc_id, i as f64))
        .collect();
    MockScoreSource::new(vec![batch], vec![], |_, _| BatchStrategy::Continue)
}

/// Unwrapping a [`ResumeOutcome`] that still carries its *concrete* iterator.
///
/// [`ResumeOutcomeExt`] covers only the type-erased form; the tests below resume
/// a concrete [`TopKIterator`] and keep reading through it.
trait ExpectOkConcrete<T> {
    /// Unwrap the resumed iterator, panicking unless the outcome is
    /// [`ResumeOutcome::Ok`].
    fn expect_ok_concrete(self) -> T;
}

impl<T> ExpectOkConcrete<T> for ResumeOutcome<T> {
    #[track_caller]
    fn expect_ok_concrete(self) -> T {
        match self {
            ResumeOutcome::Ok(it) => it,
            ResumeOutcome::Moved(_) => panic!("expected ResumeOutcome::Ok, got Moved"),
            ResumeOutcome::Aborted => panic!("expected ResumeOutcome::Ok, got Aborted"),
        }
    }
}

/// Read every remaining result, collecting the doc ids.
fn drain<'index>(it: &mut impl RQEIterator<'index>) -> Vec<DocId> {
    let mut seen = Vec::new();
    while let Some(result) = it.read().expect("read failed") {
        seen.push(result.doc_id);
    }
    seen
}

/// A top-k iterator resumed mid-yield must carry on from where it stopped.
///
/// Its results come from its own score-ordered buffer, which the suspension
/// leaves untouched — so restarting collection would re-hand the caller
/// documents it already received, and truncating would silently lose the tail.
#[test]
fn resume_mid_yield_continues_the_sequence() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    let child = IdList::<true>::new(vec![1, 2, 3, 4]);
    let mut it = Box::new(TopKIterator::new(
        source_over(&[1, 2, 3, 4]),
        child,
        NonZeroUsize::new(4).unwrap(),
        Ascending,
    ));

    let mut seen = vec![
        it.read().unwrap().expect("expected doc").doc_id,
        it.read().unwrap().expect("expected doc").doc_id,
    ];
    assert_eq!(seen, vec![1, 2]);

    let mut active = revalidate_via_resume(TypeErasedRQEIterator::new(it), &guard)
        .expect("resume failed")
        .expect_ok();

    assert_eq!(
        active.current().map(|r| r.doc_id),
        Some(2),
        "resume must leave the caller on the document it last read",
    );
    seen.extend(drain(&mut active));
    assert_eq!(
        seen,
        vec![1, 2, 3, 4],
        "the resumed iterator must neither restart nor repeat",
    );
}

/// The suspend/resume cycle must reuse the allocation: the FFI wrapper caches a
/// raw pointer into the iterator, and a rebuilt box would dangle it.
#[test]
fn resume_preserves_box_address() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    let mut it = Box::new(TopKIterator::new(
        source_over(&[1, 2]),
        IdList::<true>::new(vec![1, 2]),
        NonZeroUsize::new(2).unwrap(),
        Ascending,
    ));
    assert_eq!(it.read().unwrap().expect("expected doc").doc_id, 1);
    let addr_before = &*it as *const _ as usize;

    let suspended = it.suspend();
    assert_eq!(
        &*suspended as *const _ as usize, addr_before,
        "suspend must reuse the allocation",
    );
    let active = suspended
        .resume(&guard)
        .expect("resume failed")
        .expect_ok_concrete();
    assert_eq!(
        &*active as *const _ as usize, addr_before,
        "resume must reuse the allocation",
    );
}

/// A type-erased child crosses the suspend boundary through a vtable swap, not a
/// byte cast — the active and suspended erased forms are different `dyn` types.
/// A concrete-child round trip cannot catch a wrong-vtable bug; this one (also
/// run under miri) can. The whole iterator is erased too, as the production FFI
/// path erases it.
#[test]
fn resume_with_a_type_erased_child_survives() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    let child = TypeErasedRQEIterator::new(Box::new(IdList::<true>::new(vec![1, 2, 3])));
    let mut it = TopKIterator::new(
        source_over(&[1, 2, 3]),
        child,
        NonZeroUsize::new(3).unwrap(),
        Ascending,
    );
    assert_eq!(it.read().unwrap().expect("expected doc").doc_id, 1);

    let mut active = revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(it)), &guard)
        .expect("resume failed")
        .expect_ok();

    assert_eq!(drain(&mut active), vec![2, 3]);
}

/// An unfiltered top-k has no child to transition, so its resume is the
/// wrapper's own round trip — and it keeps streaming its batch.
#[test]
fn resume_without_a_child_keeps_streaming_the_batch() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    let mut it = Box::new(TopKIterator::<_, SteerableChild<'_>>::new_with_mode(
        source_over(&[1, 2, 3]),
        None,
        NonZeroUsize::new(3).unwrap(),
        Ascending,
        TopKMode::Unfiltered,
    ));
    assert_eq!(it.read().unwrap().expect("expected doc").doc_id, 1);

    let mut active = revalidate_via_resume(TypeErasedRQEIterator::new(it), &guard)
        .expect("resume failed")
        .expect_ok();
    assert_eq!(drain(&mut active), vec![2, 3]);
}

/// An aborted child aborts the whole top-k — the filter it applied is
/// unrecoverable, so the collected set can no longer be trusted. The reused box
/// is torn down without dropping the consumed child slot (miri covers that).
#[test]
fn resume_with_an_aborting_child_aborts() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    let mut it = Box::new(TopKIterator::new(
        source_over(&[1, 2]),
        SteerableChild::new([1, 2], ChildOutcome::Aborted),
        NonZeroUsize::new(2).unwrap(),
        Ascending,
    ));
    assert_eq!(it.read().unwrap().expect("expected doc").doc_id, 1);

    let outcome = it
        .suspend()
        .resume(&guard)
        .expect("an aborted child is not an error");
    assert!(matches!(outcome, ResumeOutcome::Aborted));
}

/// A child that fails to resume surfaces its error, and the reused box is torn
/// down the same way as on `Aborted`.
#[test]
fn resume_propagates_a_child_error() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    let mut it = Box::new(TopKIterator::new(
        source_over(&[1, 2]),
        SteerableChild::new([1, 2], ChildOutcome::Failed),
        NonZeroUsize::new(2).unwrap(),
        Ascending,
    ));
    assert_eq!(it.read().unwrap().expect("expected doc").doc_id, 1);

    assert!(matches!(
        it.suspend().resume(&guard),
        Err(RQEIteratorError::TimedOut)
    ));
}

/// A parked record borrows its query term from the leaf that produced it, and
/// the child's resume may drop that leaf without reporting a move (a union
/// discards an aborted child and can still settle on the same document). The
/// resume must not hand such a record back.
#[test]
fn resume_aborts_when_a_parked_record_borrows_a_query_term() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    let mut it = Box::new(TopKIterator::new(
        source_over(&[1, 2, 3]),
        SteerableChild::new([1, 2, 3], ChildOutcome::Ok).with_query_term(),
        NonZeroUsize::new(3).unwrap(),
        Ascending,
    ));
    assert_eq!(it.read().unwrap().expect("expected doc").doc_id, 1);

    let outcome = it
        .suspend()
        .resume(&guard)
        .expect("an unsafe-to-resume state is recoverable, not an error");
    assert!(matches!(outcome, ResumeOutcome::Aborted));
}

/// Trimmed records carry only the child's metrics, never a query term, so the
/// same child leaves a trimmed top-k resumable.
#[test]
fn resume_keeps_a_trimmed_top_k_whose_child_yields_terms() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    let mut it = Box::new(
        TopKIterator::new(
            source_over(&[1, 2, 3]),
            SteerableChild::new([1, 2, 3], ChildOutcome::Ok).with_query_term(),
            NonZeroUsize::new(3).unwrap(),
            Ascending,
        )
        .with_trim_deep_results(true),
    );
    assert_eq!(it.read().unwrap().expect("expected doc").doc_id, 1);

    let mut active = revalidate_via_resume(TypeErasedRQEIterator::new(it), &guard)
        .expect("resume failed")
        .expect_ok();
    assert_eq!(drain(&mut active), vec![2, 3]);
}

/// Collection reads index state the source kept from before the suspension,
/// which a concurrent writer or GC may have invalidated, so a top-k that has
/// not collected yet cannot resume.
#[test]
fn resume_before_the_first_read_aborts() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    let it = Box::new(TopKIterator::new(
        source_over(&[1, 2, 3]),
        IdList::<true>::new(vec![1, 2, 3]),
        NonZeroUsize::new(3).unwrap(),
        Ascending,
    ));

    let outcome = it
        .suspend()
        .resume(&guard)
        .expect("an unresumable state is recoverable, not an error");
    assert!(matches!(outcome, ResumeOutcome::Aborted));
}

/// A selective filter caps the estimate in both modes.
#[test]
fn suspended_estimate_is_capped_by_the_child() {
    let it = Box::new(TopKIterator::new(
        source_over(&[1, 2, 3, 4]),
        IdList::<true>::new(vec![2, 4]),
        NonZeroUsize::new(4).unwrap(),
        Ascending,
    ));
    assert_eq!(RQEIterator::num_estimated(&*it), 2);

    let suspended = it.suspend();
    assert_eq!(RQESuspendedIterator::num_estimated(&*suspended), 2);
}

/// A rewind after crossing a lock release would start a collection over the
/// source's stale view of the index, so the next read reports EOF instead.
#[test]
fn rewind_after_resume_reports_eof() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    let mut it = Box::new(TopKIterator::new(
        source_over(&[1, 2]),
        IdList::<true>::new(vec![1, 2]),
        NonZeroUsize::new(2).unwrap(),
        Ascending,
    ));
    assert_eq!(it.read().unwrap().expect("expected doc").doc_id, 1);

    let mut active = it
        .suspend()
        .resume(&guard)
        .expect("resume failed")
        .expect_ok_concrete();
    active.rewind();
    assert!(active.read().unwrap().is_none());
    assert!(active.at_eof());
}

/// A rewind without any lock release in between still re-collects.
#[test]
fn rewind_without_a_lock_release_recollects() {
    let mut it = TopKIterator::new(
        source_over(&[1, 2]),
        IdList::<true>::new(vec![1, 2]),
        NonZeroUsize::new(2).unwrap(),
        Ascending,
    );
    assert_eq!(it.read().unwrap().expect("expected doc").doc_id, 1);
    it.rewind();
    assert_eq!(drain(&mut it), vec![1, 2]);
}

/// A drained top-k resumes still at EOF; the harness asserts exhaustion
/// survives the cycle.
#[test]
fn resume_at_eof_stays_at_eof() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    let mut it = TopKIterator::new(
        source_over(&[1, 2]),
        IdList::<true>::new(vec![1, 2]),
        NonZeroUsize::new(2).unwrap(),
        Ascending,
    );
    assert_eq!(drain(&mut it), vec![1, 2]);
    assert!(it.at_eof());

    let mut active = revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(it)), &guard)
        .expect("resume failed")
        .expect_ok();
    assert!(active.read().unwrap().is_none());
}

/// A source that builds its own results never yields the child's records, so
/// collection keeps none, and with them no borrowed query term that would make
/// the resume abort.
#[test]
fn resume_keeps_a_top_k_whose_source_builds_its_own_results() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    let mut it = Box::new(TopKIterator::new(
        source_over(&[1, 2, 3]).without_child_records(),
        SteerableChild::new([1, 2, 3], ChildOutcome::Ok).with_query_term(),
        NonZeroUsize::new(3).unwrap(),
        Ascending,
    ));
    assert_eq!(it.read().unwrap().expect("expected doc").doc_id, 1);

    let mut active = revalidate_via_resume(TypeErasedRQEIterator::new(it), &guard)
        .expect("resume failed")
        .expect_ok();
    assert_eq!(drain(&mut active), vec![2, 3]);
}

/// A source whose collection survives a lock release lets a top-k that has not
/// collected yet through a resume, and collect afterwards.
#[test]
fn source_that_survives_a_lock_release_collects_afterwards() {
    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    let build = || {
        TopKIterator::new(
            source_over(&[1, 2]).collectable_after_lock_release(),
            IdList::<true>::new(vec![1, 2]),
            NonZeroUsize::new(2).unwrap(),
            Ascending,
        )
    };

    let mut resumed = revalidate_via_resume(TypeErasedRQEIterator::new(Box::new(build())), &guard)
        .expect("resume failed")
        .expect_ok();
    assert_eq!(drain(&mut resumed), vec![1, 2]);

    // Nor does a rewind after the lock release stop it re-collecting.
    resumed.rewind();
    assert_eq!(drain(&mut resumed), vec![1, 2]);
}
