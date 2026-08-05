/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for [`ContractChecker`] itself: a well-behaved iterator passes every
//! check transparently, and each way an iterator (or its driver) can break the
//! [`RQEIterator`] contract is caught at the operation that commits it.
//!
//! The violations are staged through [`Misbehaving`], a minimal sorted-id-list
//! iterator that misbehaves in exactly one configurable way per instance.

use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use rqe_core::DocId;
use rqe_iterators::{
    IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome,
    id_list::{IdListSorted, IdListUnsorted},
};
use rqe_iterators_test_utils::{
    ContractChecker, MockContext, assert_current_contract, assert_current_contract_via_skip_to,
};

#[test]
fn well_behaved_iterator_passes_transparently() {
    let mut it = ContractChecker::new(IdListSorted::new(vec![10, 20, 30, 50, 80]));

    // The fixed contract drains compose with the checker.
    assert_eq!(assert_current_contract(&mut it), [10, 20, 30, 50, 80]);
    assert_current_contract_via_skip_to(&mut it, 81);

    // A mixed read/skip drive passes every per-operation check.
    assert_eq!(it.read().unwrap().unwrap().doc_id, 10);
    assert!(matches!(it.skip_to(30), Ok(Some(SkipToOutcome::Found(_)))));
    assert!(matches!(
        it.skip_to(40),
        Ok(Some(SkipToOutcome::NotFound(_)))
    ));
    assert_eq!(it.last_doc_id(), 50);
    assert_eq!(it.current().unwrap().doc_id, 50);
    assert!(matches!(it.skip_to(90), Ok(None)));
    assert!(it.at_eof());
}

#[test]
fn unordered_checker_accepts_out_of_order_ids() {
    let mut it = ContractChecker::new_unordered(IdListUnsorted::new(vec![5, 3, 8, 1]));
    assert_eq!(assert_current_contract(&mut it), [5, 3, 8, 1]);
}

#[test]
#[should_panic(expected = "doc ids must strictly ascend")]
fn catches_out_of_order_ids() {
    let mut it = ContractChecker::new(IdListUnsorted::new(vec![5, 3, 8, 1]));
    let _ = assert_current_contract(&mut it);
}

#[test]
#[should_panic(expected = "broke the precondition")]
fn catches_caller_skipping_backwards() {
    let mut it = ContractChecker::new(IdListSorted::new(vec![10, 20, 30]));
    assert!(matches!(it.skip_to(30), Ok(Some(_))));
    // The bug under test sits in the *driver*: `skip_to` requires
    // `last_doc_id() < doc_id`, and the checker enforces it on the caller's
    // behalf before forwarding.
    let _ = it.skip_to(20);
}

#[test]
fn revalidate_ok_passes() {
    let mock_ctx = MockContext::new(0, 0);
    let mut it = ContractChecker::new(IdListSorted::new(vec![1, 2, 3]));
    assert_eq!(it.read().unwrap().unwrap().doc_id, 1);
    let status = it
        .revalidate(&*mock_ctx.spec_read())
        .expect("revalidate failed");
    assert_eq!(status, RQEValidateStatus::Ok);
    // The drive continues cleanly after an in-place revalidation.
    assert_eq!(it.read().unwrap().unwrap().doc_id, 2);
}

/// A single misbehaviour for [`Misbehaving`] to commit; everything else about
/// the iterator stays correct so the checker's panic can be pinned to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    /// Behave correctly, except that `revalidate` answers `Moved` in place —
    /// benign, used to exercise the checker's `Moved` agreement checks.
    MovedOnRevalidate,
    /// `current()` keeps handing out the stale result after running past the
    /// end.
    StaleCurrentPastEnd,
    /// `at_eof()` never turns true.
    NeverAtEof,
    /// `last_doc_id()` lags behind the result `read` just yielded.
    LaggingLastDocId,
    /// An exhausted iterator starts yielding results again.
    YieldsAfterExhaustion,
    /// A `skip_to` that found nothing records the probed id as its position.
    ClaimsProbedId,
    /// `skip_to` answers `NotFound` even on an exact match.
    NotFoundOnExactMatch,
    /// `rewind` leaves the exhausted state latched.
    LatchedRewind,
    /// `current()` returns a different result object than `read` yielded.
    ForeignCurrent,
    /// `num_estimated()` under-reports the number of results.
    UnderEstimates,
    /// `revalidate` aborts the iterator.
    AbortsOnRevalidate,
}

/// A minimal sorted-id-list iterator committing exactly one [`Fault`].
///
/// Position is encoded like `IdList`'s: `offset` is the index of the next id
/// to yield, so `offset == ids.len()` is still a live position (on the last
/// id) and one step further means the iterator ran past its end.
struct Misbehaving<'index> {
    ids: Vec<DocId>,
    offset: usize,
    result: RSIndexResult<'index>,
    /// A second result object, handed out by [`Fault::ForeignCurrent`].
    spare: RSIndexResult<'index>,
    fault: Fault,
}

impl Misbehaving<'_> {
    fn new(ids: Vec<DocId>, fault: Fault) -> Self {
        Self {
            ids,
            offset: 0,
            result: RSIndexResult::build_virt().build(),
            spare: RSIndexResult::build_virt().build(),
            fault,
        }
    }

    fn past_end(&self) -> bool {
        self.offset > self.ids.len()
    }
}

impl<'index> RQEIterator<'index> for Misbehaving<'index> {
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        if self.past_end() {
            return (self.fault == Fault::StaleCurrentPastEnd).then_some(&mut self.result);
        }
        if self.fault == Fault::ForeignCurrent {
            self.spare.doc_id = self.result.doc_id;
            return Some(&mut self.spare);
        }
        Some(&mut self.result)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.past_end() {
            if self.fault == Fault::YieldsAfterExhaustion {
                self.result.doc_id = self.ids[0];
                return Ok(Some(&mut self.result));
            }
            return Ok(None);
        }
        let Some(&id) = self.ids.get(self.offset) else {
            self.offset = self.ids.len() + 1;
            return Ok(None);
        };
        self.offset += 1;
        self.result.doc_id = id;
        Ok(Some(&mut self.result))
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        while let Some(&id) = self.ids.get(self.offset) {
            self.offset += 1;
            if id >= doc_id {
                self.result.doc_id = id;
                let found = id == doc_id && self.fault != Fault::NotFoundOnExactMatch;
                return Ok(Some(if found {
                    SkipToOutcome::Found(&mut self.result)
                } else {
                    SkipToOutcome::NotFound(&mut self.result)
                }));
            }
        }
        self.offset = self.ids.len() + 1;
        if self.fault == Fault::ClaimsProbedId {
            self.result.doc_id = doc_id;
        }
        Ok(None)
    }

    fn revalidate(
        &mut self,
        _spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        Ok(match self.fault {
            Fault::AbortsOnRevalidate => RQEValidateStatus::Aborted,
            Fault::MovedOnRevalidate => RQEValidateStatus::Moved {
                current: self.current(),
            },
            _ => RQEValidateStatus::Ok,
        })
    }

    fn rewind(&mut self) {
        if self.fault == Fault::LatchedRewind && self.past_end() {
            return;
        }
        self.offset = 0;
        self.result.doc_id = 0;
    }

    fn num_estimated(&self) -> usize {
        if self.fault == Fault::UnderEstimates {
            self.ids.len() - 1
        } else {
            self.ids.len()
        }
    }

    fn last_doc_id(&self) -> DocId {
        if self.fault == Fault::LaggingLastDocId && self.offset > 0 {
            self.result.doc_id.saturating_sub(1)
        } else {
            self.result.doc_id
        }
    }

    fn at_eof(&self) -> bool {
        self.fault != Fault::NeverAtEof && self.past_end()
    }

    fn type_(&self) -> IteratorType {
        IteratorType::IdListSorted
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

/// Wrap a [`Misbehaving`] iterator and drain it through the checker.
fn drain(fault: Fault) -> ContractChecker<Misbehaving<'static>> {
    let mut it = ContractChecker::new(Misbehaving::new(vec![1, 2], fault));
    while it.read().expect("read must not fail").is_some() {}
    it
}

#[test]
fn misbehaving_without_fault_passes() {
    // `MovedOnRevalidate` is the benign configuration: it proves the mock
    // itself upholds the contract, so the `catches_*` panics below can only
    // come from each test's single fault.
    let mut it = ContractChecker::new(Misbehaving::new(vec![1, 2], Fault::MovedOnRevalidate));
    assert_eq!(assert_current_contract(&mut it), [1, 2]);
    assert_current_contract_via_skip_to(&mut it, 3);
}

#[test]
fn revalidate_moved_agreement_is_verified() {
    let mock_ctx = MockContext::new(0, 0);
    let mut it = ContractChecker::new(Misbehaving::new(vec![1, 2], Fault::MovedOnRevalidate));

    // Moved onto a concrete document: the checker verifies the accessors
    // agree with the reported position, then the drive continues.
    assert_eq!(it.read().unwrap().unwrap().doc_id, 1);
    let status = it.revalidate(&*mock_ctx.spec_read()).unwrap();
    assert!(matches!(
        status,
        RQEValidateStatus::Moved { current: Some(_) }
    ));
    assert_eq!(it.read().unwrap().unwrap().doc_id, 2);

    // Moved while past the end: the `current` carried by the status must be
    // `None`, matching the exhausted position.
    assert!(it.read().unwrap().is_none());
    let status = it.revalidate(&*mock_ctx.spec_read()).unwrap();
    assert!(matches!(status, RQEValidateStatus::Moved { current: None }));
}

#[test]
#[should_panic(expected = "current() must be None")]
fn catches_stale_current_past_end() {
    drain(Fault::StaleCurrentPastEnd);
}

#[test]
#[should_panic(expected = "at_eof() must be true")]
fn catches_eof_never_reported() {
    drain(Fault::NeverAtEof);
}

#[test]
#[should_panic(expected = "last_doc_id() must track")]
fn catches_lagging_last_doc_id() {
    let mut it = ContractChecker::new(Misbehaving::new(vec![1, 2], Fault::LaggingLastDocId));
    let _ = it.read();
}

#[test]
#[should_panic(expected = "must keep returning None")]
fn catches_yield_after_exhaustion() {
    let mut it = drain(Fault::YieldsAfterExhaustion);
    let _ = it.read();
}

#[test]
#[should_panic(expected = "must not claim the probed id")]
fn catches_skip_claiming_probed_id() {
    let mut it = ContractChecker::new(Misbehaving::new(vec![1, 2], Fault::ClaimsProbedId));
    let _ = it.skip_to(5);
}

#[test]
#[should_panic(expected = "NotFound promises")]
fn catches_not_found_on_exact_match() {
    let mut it = ContractChecker::new(Misbehaving::new(vec![1, 2], Fault::NotFoundOnExactMatch));
    let _ = it.skip_to(2);
}

#[test]
#[should_panic(expected = "an exhausted state must not latch")]
fn catches_latched_rewind() {
    let mut it = drain(Fault::LatchedRewind);
    it.rewind();
}

#[test]
#[should_panic(expected = "same result object")]
fn catches_foreign_current() {
    let mut it = ContractChecker::new(Misbehaving::new(vec![1, 2], Fault::ForeignCurrent));
    let _ = it.read();
}

#[test]
#[should_panic(expected = "must be an upper bound")]
fn catches_underestimating_num_estimated() {
    let mut it = ContractChecker::new(Misbehaving::new(vec![1, 2, 3], Fault::UnderEstimates));
    for _ in 0..3 {
        let _ = it.read();
    }
}

#[test]
#[should_panic(expected = "must be dropped, not used")]
fn catches_use_after_abort() {
    let mock_ctx = MockContext::new(0, 0);
    let mut it = ContractChecker::new(Misbehaving::new(vec![1, 2], Fault::AbortsOnRevalidate));
    let status = it.revalidate(&*mock_ctx.spec_read()).unwrap();
    assert!(matches!(status, RQEValidateStatus::Aborted));
    let _ = it.read();
}
