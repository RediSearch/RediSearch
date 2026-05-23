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

use std::cell::Cell;

use index_result::RSIndexResult;
use rqe_core::DocId;
use rqe_iterators::{
    IteratorType, RQEIterator, RQEIteratorError, SkipToOutcome,
    id_list::{IdListSorted, IdListUnsorted},
};
use rqe_iterators_test_utils::{
    ContractChecker, assert_current_contract, assert_current_contract_via_skip_to,
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
#[should_panic(expected = "doc ids must strictly ascend")]
fn catches_repeated_ids_unless_they_are_opted_into() {
    // A repeat is out of contract for the iterators that promise strict ascent,
    // which is all of them bar the ones wrapped with `new_with_duplicates`.
    let mut it = ContractChecker::new(Misbehaving::new(vec![1, 1, 2], Fault::MovedOnRevalidate));
    while it.read().expect("read must not fail").is_some() {}
}

#[test]
fn duplicates_checker_accepts_repeated_ids() {
    let mut it = ContractChecker::new_with_duplicates(Misbehaving::new(
        vec![1, 1, 2],
        Fault::MovedOnRevalidate,
    ));
    assert_eq!(assert_current_contract(&mut it), [1, 1, 2]);
}

#[test]
#[should_panic(expected = "must not descend")]
fn catches_descending_ids_even_when_duplicates_are_allowed() {
    // Allowing a repeat does not allow going backwards.
    let mut it = ContractChecker::new_with_duplicates(Misbehaving::new(
        vec![2, 1],
        Fault::MovedOnRevalidate,
    ));
    while it.read().expect("read must not fail").is_some() {}
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

/// A single misbehaviour for [`Misbehaving`] to commit; everything else about
/// the iterator stays correct so the checker's panic can be pinned to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    /// Behave correctly, except that `revalidate` answers `Moved` in place —
    /// benign, used to exercise the checker's `Moved` agreement checks.
    MovedOnRevalidate,
    /// Every fallible operation reports a timeout — benign, and the checker
    /// must hand the error back untouched rather than read it as a violation.
    TimesOut,
    /// `current()` keeps handing out the stale result after running past the
    /// end.
    StaleCurrentPastEnd,
    /// `current()` is `None` while the iterator is positioned on a result.
    NoCurrentWhilePositioned,
    /// `current()` hands back the result once after each yield and answers
    /// `None` from then on.
    ForgetfulCurrent,
    /// `at_eof()` never turns true.
    NeverAtEof,
    /// `last_doc_id()` lags behind the result `read` just yielded.
    LaggingLastDocId,
    /// `last_doc_id()` answers truthfully once after each yield and reports 0
    /// from then on — a stale self-report that must not be able to license a
    /// backward `skip_to`.
    ForgetfulLastDocId,
    /// `at_eof()` is true while unread, which claims the iterator can yield
    /// nothing whatever the data, yet it yields all the same.
    EofWhileUnread,
    /// An exhausted iterator starts yielding results again.
    YieldsAfterExhaustion,
    /// A `skip_to` that found nothing records the probed id as its position.
    ClaimsProbedId,
    /// A `skip_to` that failed records the probed id as its position.
    ClaimsProbedIdOnTimeout,
    /// `skip_to` answers `NotFound` even on an exact match.
    NotFoundOnExactMatch,
    /// `rewind` leaves the exhausted state latched.
    LatchedRewind,
    /// `current()` returns a different result object than `read` yielded.
    ForeignCurrent,
    /// `num_estimated()` under-reports the number of results.
    UnderEstimates,
    /// `num_estimated()` collapses to 0 once the iterator has run dry, below
    /// the number of results it already handed out.
    ShrinkingEstimate,
    /// `revalidate` republishes through `current()` the result the iterator
    /// yielded before it ran dry, while reporting `Ok`.
    StaleCurrentAfterOkRevalidate,
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
    /// Set by every yield, cleared by the next query that consumes it — the
    /// window in which [`Fault::ForgetfulLastDocId`] and
    /// [`Fault::ForgetfulCurrent`] still tell the truth.
    just_yielded: Cell<bool>,
    /// Set by the `revalidate` of [`Fault::StaleCurrentAfterOkRevalidate`],
    /// from which point `current()` republishes the stale result.
    resurrected: bool,
    fault: Fault,
}

impl Misbehaving<'_> {
    fn new(ids: Vec<DocId>, fault: Fault) -> Self {
        Self {
            ids,
            offset: 0,
            result: RSIndexResult::build_virt().build(),
            spare: RSIndexResult::build_virt().build(),
            just_yielded: Cell::new(false),
            resurrected: false,
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
            let stale = self.fault == Fault::StaleCurrentPastEnd
                || (self.fault == Fault::StaleCurrentAfterOkRevalidate && self.resurrected);
            return stale.then_some(&mut self.result);
        }
        match self.fault {
            Fault::NoCurrentWhilePositioned => None,
            Fault::ForgetfulCurrent if !self.just_yielded.replace(false) => None,
            Fault::ForeignCurrent => {
                self.spare.doc_id = self.result.doc_id;
                Some(&mut self.spare)
            }
            _ => Some(&mut self.result),
        }
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.fault == Fault::TimesOut {
            return Err(RQEIteratorError::TimedOut);
        }
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
        self.just_yielded.set(true);
        Ok(Some(&mut self.result))
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        if self.fault == Fault::TimesOut {
            return Err(RQEIteratorError::TimedOut);
        }
        if self.fault == Fault::ClaimsProbedIdOnTimeout {
            self.result.doc_id = doc_id;
            return Err(RQEIteratorError::TimedOut);
        }
        while let Some(&id) = self.ids.get(self.offset) {
            self.offset += 1;
            if id >= doc_id {
                self.result.doc_id = id;
                self.just_yielded.set(true);
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

    fn rewind(&mut self) {
        if self.fault == Fault::LatchedRewind && self.past_end() {
            return;
        }
        self.offset = 0;
        self.result.doc_id = 0;
    }

    fn num_estimated(&self) -> usize {
        match self.fault {
            Fault::UnderEstimates => self.ids.len() - 1,
            Fault::ShrinkingEstimate if self.past_end() => 0,
            _ => self.ids.len(),
        }
    }

    fn last_doc_id(&self) -> DocId {
        match self.fault {
            Fault::LaggingLastDocId if self.offset > 0 => self.result.doc_id.saturating_sub(1),
            Fault::ForgetfulLastDocId if !self.just_yielded.replace(false) => 0,
            _ => self.result.doc_id,
        }
    }

    fn at_eof(&self) -> bool {
        match self.fault {
            Fault::NeverAtEof => false,
            Fault::EofWhileUnread => self.offset == 0 || self.past_end(),
            _ => self.past_end(),
        }
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
#[should_panic(expected = "broke the precondition")]
fn catches_backward_skip_licensed_by_a_stale_last_doc_id() {
    let mut it = ContractChecker::new(Misbehaving::new(
        vec![10, 20, 30],
        Fault::ForgetfulLastDocId,
    ));
    assert!(matches!(it.skip_to(30), Ok(Some(SkipToOutcome::Found(_)))));
    // The iterator now under-reports `last_doc_id()` as 0, which would license
    // this backward probe if the checker took its word for where it stands. It
    // tracks doc 30 itself, so the probe is rejected regardless.
    let _ = it.skip_to(20);
}

#[test]
#[should_panic(expected = "last_doc_id() must still report")]
fn catches_stale_last_doc_id_before_a_forward_skip() {
    let mut it = ContractChecker::new(Misbehaving::new(
        vec![10, 20, 30],
        Fault::ForgetfulLastDocId,
    ));
    assert!(matches!(it.skip_to(30), Ok(Some(SkipToOutcome::Found(_)))));
    // The probe itself is well-formed; the iterator having forgotten its
    // position is the violation.
    let _ = it.skip_to(40);
}

#[test]
#[should_panic(expected = "cannot yield anything by construction")]
fn catches_read_from_an_iterator_that_reports_eof_while_unread() {
    let mut it = ContractChecker::new(Misbehaving::new(vec![1, 2], Fault::EofWhileUnread));
    let _ = it.read();
}

#[test]
#[should_panic(expected = "cannot yield anything by construction")]
fn catches_skip_from_an_iterator_that_reports_eof_while_unread() {
    let mut it = ContractChecker::new(Misbehaving::new(vec![1, 2], Fault::EofWhileUnread));
    let _ = it.skip_to(2);
}

#[test]
#[should_panic(expected = "current: must be None on an iterator that reports at_eof()")]
fn catches_current_on_an_iterator_that_reports_eof_while_unread() {
    let mut it = ContractChecker::new(Misbehaving::new(vec![1, 2], Fault::EofWhileUnread));
    // Nothing has been read yet, so the only result the iterator could be
    // holding is one it promised it does not have.
    let _ = it.current();
}

#[test]
#[should_panic(expected = "must not claim the probed id")]
fn catches_skip_claiming_probed_id() {
    let mut it = ContractChecker::new(Misbehaving::new(vec![1, 2], Fault::ClaimsProbedId));
    let _ = it.skip_to(5);
}

#[test]
#[should_panic(expected = "must not claim the probed id")]
fn catches_failed_skip_claiming_probed_id() {
    let mut it = ContractChecker::new(Misbehaving::new(vec![1, 2], Fault::ClaimsProbedIdOnTimeout));
    // A skip that fails carries no result either, so it is under the same
    // obligation not to leave the probed id behind as its position.
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
#[should_panic(expected = "read: current() must be Some")]
fn catches_missing_current_on_a_yield() {
    let mut it = ContractChecker::new(Misbehaving::new(
        vec![1, 2],
        Fault::NoCurrentWhilePositioned,
    ));
    let _ = it.read();
}

#[test]
#[should_panic(expected = "current: must be Some")]
fn catches_current_dropped_after_a_yield() {
    let mut it = ContractChecker::new(Misbehaving::new(vec![1, 2], Fault::ForgetfulCurrent));
    // The result was there when `read` handed it out, so it takes a second
    // query to catch the iterator letting go of it.
    assert_eq!(it.read().unwrap().unwrap().doc_id, 1);
    let _ = it.current();
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
#[should_panic(expected = "must be an upper bound")]
fn catches_estimate_shrinking_below_the_results_already_yielded() {
    // The estimate is a valid upper bound at every yield and only collapses
    // afterwards, so it takes reading it to catch this.
    let it = drain(Fault::ShrinkingEstimate);
    let _ = it.num_estimated();
}
