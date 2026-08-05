/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A wrapper that verifies the [`RQEIterator`] contract on every operation.
//!
//! [`assert_current_contract`](crate::assert_current_contract) and friends
//! drive an iterator through a *fixed* scenario and assert the contract along
//! the way. [`ContractChecker`] turns that inside out: it sits between the
//! test and the iterator under test, so *whatever* scenario the test drives —
//! including scenarios the fixed drains never reach — is checked on every
//! single operation, at the operation that commits the violation.

use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use rqe_core::DocId;
use rqe_iterators::{
    IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome, c2rust,
};

/// Where the checker believes the wrapped iterator stands, updated on every
/// operation that can move it.
///
/// This is the checker's independent copy of the one piece of state the
/// [`RQEIterator`] contract revolves around; each subsequent operation is
/// checked against it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Position {
    /// No [`read`](RQEIterator::read)/[`skip_to`](RQEIterator::skip_to)
    /// outcome observed since construction or the last
    /// [`rewind`](RQEIterator::rewind).
    Unread,
    /// The last operation yielded a result with this doc id.
    On(DocId),
    /// An operation found nothing: the iterator ran past its last result.
    PastEnd,
}

/// [`RQEValidateStatus`] with the borrowed result reduced to its doc id and
/// address, so the checker can release the borrow, interrogate the iterator,
/// and only then rebuild the status to hand out.
enum RevalidateSummary<'index> {
    Ok,
    Moved(Option<(DocId, *const RSIndexResult<'index>)>),
    Aborted,
}

/// An [`RQEIterator`] wrapper that forwards every operation to the wrapped
/// iterator and panics on the first contract violation.
///
/// Wrap the iterator under test right after constructing it, then drive the
/// wrapper exactly as you would have driven the iterator — the checks are a
/// side effect of normal use, so the test's assertions stay about *behaviour*
/// (which doc ids come out) while the wrapper owns the *contract* (how the
/// accessors must agree along the way).
///
/// # What is checked
///
/// Every rule below comes from the [`RQEIterator`] method contracts; the
/// wrapper holds its own [`Position`] record and checks each operation against
/// it.
///
/// - [`read`](RQEIterator::read)/[`skip_to`](RQEIterator::skip_to) yielding a
///   result: [`last_doc_id`](RQEIterator::last_doc_id) tracks it,
///   [`at_eof`](RQEIterator::at_eof) is `false`, and
///   [`current`](RQEIterator::current) returns the *same* result object.
/// - [`read`](RQEIterator::read)/[`skip_to`](RQEIterator::skip_to) finding
///   nothing: [`at_eof`](RQEIterator::at_eof) is `true`,
///   [`current`](RQEIterator::current) is `None` (not the stale last result),
///   and the exhausted state holds until [`rewind`](RQEIterator::rewind) — an
///   exhausted iterator must keep returning `None`.
/// - An iterator that reports [`at_eof`](RQEIterator::at_eof) while unread
///   cannot yield anything *by construction*, so it must never produce a
///   result before a [`rewind`](RQEIterator::rewind) — or after one.
/// - [`skip_to`](RQEIterator::skip_to): `Found` carries the requested id,
///   `NotFound` carries a strictly greater one, and a skip that found nothing
///   must not claim the probed id as its position. The *caller* precondition
///   `last_doc_id() < doc_id` is asserted too, so a misbehaving driver (e.g. a
///   parent iterator skipping backwards) is caught as well — against the
///   checker's own [`Position`] record, so an iterator that under-reports
///   [`last_doc_id`](RQEIterator::last_doc_id) cannot license a backward probe.
/// - [`rewind`](RQEIterator::rewind) restores the
///   [`at_eof`](RQEIterator::at_eof) answer observed at construction: the
///   exhausted state must not latch.
/// - [`revalidate`](RQEIterator::revalidate): `Ok` leaves every accessor
///   answering exactly as it did before the call — it promises the position did
///   not change; `Moved` lands on a position the accessors agree on, never
///   behind the previous one and never resurrecting an exhausted iterator; and
///   any use after `Aborted` panics — an aborted iterator must be dropped.
/// - Doc ids strictly ascend between rewinds, unless constructed with
///   [`new_unordered`](Self::new_unordered).
/// - The number of results yielded between rewinds never exceeds
///   [`num_estimated`](RQEIterator::num_estimated), which is documented as an
///   upper bound — checked both as results come out and whenever a caller reads
///   the estimate, since an estimate that shrinks below the count already
///   handed out breaks the bound just as much.
///
/// # Assumptions
///
/// - The iterator is wrapped *before* its first operation; the checker
///   calibrates its unread-state expectations at construction.
/// - The test does not change `doc_id` through the `&mut RSIndexResult`
///   references handed out; the checker records yielded ids and would
///   misattribute such a mutation to the iterator.
///
/// # Limitations
///
/// The checker verifies the [`RQEIterator`] surface only. It does not
/// implement the suspend/resume machinery
/// ([`RQEIteratorBoxed`](rqe_iterators::RQEIteratorBoxed)), so tests
/// exercising that path still drive the unwrapped iterator.
#[expect(rustdoc::private_intra_doc_links)]
pub struct ContractChecker<I> {
    inner: I,
    position: Position,
    /// The [`at_eof`](RQEIterator::at_eof) answer observed at construction.
    /// [`rewind`](RQEIterator::rewind) must restore it, and it must not drift
    /// while [`Position::Unread`]: `false` for almost every iterator, `true`
    /// only for those that cannot yield anything *by construction* (e.g.
    /// [`Empty`](rqe_iterators::Empty)).
    at_eof_when_unread: bool,
    /// Results yielded since construction or the last
    /// [`rewind`](RQEIterator::rewind), to check against
    /// [`num_estimated`](RQEIterator::num_estimated).
    yielded: usize,
    /// Whether yielded doc ids must strictly ascend between rewinds.
    ordered: bool,
    /// Set once [`revalidate`](RQEIterator::revalidate) returns
    /// [`Aborted`](RQEValidateStatus::Aborted); every operation afterwards is
    /// a contract violation.
    aborted: bool,
}

impl<'index, I: RQEIterator<'index>> ContractChecker<I> {
    /// Wrap a freshly-constructed iterator whose doc ids ascend strictly
    /// between rewinds — the normal case, relied upon by every composite.
    pub fn new(inner: I) -> Self {
        Self::with_ordering(inner, true)
    }

    /// Wrap a freshly-constructed iterator that legitimately yields doc ids
    /// out of order (e.g.
    /// [`IdListUnsorted`](rqe_iterators::id_list::IdListUnsorted)), skipping
    /// the ascending-ids check.
    pub fn new_unordered(inner: I) -> Self {
        Self::with_ordering(inner, false)
    }

    fn with_ordering(inner: I, ordered: bool) -> Self {
        let at_eof_when_unread = inner.at_eof();
        Self {
            inner,
            position: Position::Unread,
            at_eof_when_unread,
            yielded: 0,
            ordered,
            aborted: false,
        }
    }

    /// Consume the checker, returning the wrapped iterator.
    pub fn into_inner(self) -> I {
        self.inner
    }

    /// Panic if the iterator was aborted by a
    /// [`revalidate`](RQEIterator::revalidate): the contract demands it be
    /// dropped, not used.
    #[track_caller]
    fn assert_usable(&self, op: &str) {
        assert!(
            !self.aborted,
            "{op}: the iterator reported Aborted from revalidate — it must be dropped, not used",
        );
    }

    /// Checks shared by every operation that lands the iterator on a result:
    /// the position accessors must agree with the doc id just handed out, and
    /// [`current`](RQEIterator::current) must return the very same result
    /// object (`yielded` is its address). Returns that result, re-borrowed,
    /// for the caller to hand out.
    #[track_caller]
    fn assert_positioned_on(
        &mut self,
        op: &str,
        id: DocId,
        yielded: *const RSIndexResult<'index>,
    ) -> &mut RSIndexResult<'index> {
        assert_eq!(
            self.inner.last_doc_id(),
            id,
            "{op}: last_doc_id() must track the result just handed out (doc {id})",
        );
        assert!(
            !self.inner.at_eof(),
            "{op}: at_eof() must be false while positioned on doc {id}, including on the last \
             result",
        );
        self.position = Position::On(id);
        let current = self
            .inner
            .current()
            .unwrap_or_else(|| panic!("{op}: current() must be Some while positioned on doc {id}"));
        assert_eq!(
            current.doc_id, id,
            "{op}: current() must agree with the result just handed out (doc {id})",
        );
        assert!(
            std::ptr::eq(yielded, current),
            "{op}: current() must hand back the same result object the operation returned, not a \
             different one",
        );
        current
    }

    /// [`assert_positioned_on`](Self::assert_positioned_on) plus the
    /// yield-count bound, for operations that yield a *new* result rather
    /// than re-report the current one.
    #[track_caller]
    fn after_yield(
        &mut self,
        op: &str,
        id: DocId,
        yielded: *const RSIndexResult<'index>,
    ) -> &mut RSIndexResult<'index> {
        self.yielded += 1;
        self.assert_estimate_bounds_yields(op);
        self.assert_positioned_on(op, id, yielded)
    }

    /// The yield-count bound: results handed out since the last
    /// [`rewind`](RQEIterator::rewind) may never exceed
    /// [`num_estimated`](RQEIterator::num_estimated), documented as an upper
    /// bound. Checked as each result comes out *and* whenever a caller reads
    /// the estimate — an estimate that shrinks below the count already handed
    /// out breaks the bound wherever it is observed.
    #[track_caller]
    fn assert_estimate_bounds_yields(&self, op: &str) -> usize {
        let estimated = self.inner.num_estimated();
        assert!(
            self.yielded <= estimated,
            "{op}: {} results yielded since the last rewind exceeds num_estimated() = \
             {estimated}, which must be an upper bound",
            self.yielded,
        );
        estimated
    }

    /// Panic if an iterator that reported [`at_eof`](RQEIterator::at_eof) while
    /// unread produced a result anyway. That answer is reserved for iterators
    /// that cannot yield anything *by construction*, whatever the data, so it
    /// is a promise about every position — not a state a read can talk its way
    /// out of.
    #[track_caller]
    fn assert_may_yield_while_unread(&self, op: &str, previous: Position, id: DocId) {
        if previous == Position::Unread {
            assert!(
                !self.at_eof_when_unread,
                "{op}: an iterator that reports at_eof() while unread cannot yield anything by \
                 construction, but it produced doc {id}",
            );
        }
    }

    /// Re-check that every accessor still answers as the tracked
    /// [`Position`] says, for an operation that promised not to move the
    /// iterator.
    #[track_caller]
    fn assert_position_unchanged(&mut self, op: &str) {
        match self.position {
            Position::Unread => {
                let at_eof = self.inner.at_eof();
                assert_eq!(
                    at_eof, self.at_eof_when_unread,
                    "{op}: at_eof() changed from {} to {at_eof}, but the iterator has not moved",
                    self.at_eof_when_unread,
                );
                if at_eof {
                    assert!(
                        self.inner.current().is_none(),
                        "{op}: current() must be None on an iterator that reports at_eof() while \
                         unread",
                    );
                }
            }
            Position::On(id) => {
                assert_eq!(
                    self.inner.last_doc_id(),
                    id,
                    "{op}: last_doc_id() must still report the result last yielded (doc {id})",
                );
                assert!(
                    !self.inner.at_eof(),
                    "{op}: at_eof() must be false while positioned on doc {id}",
                );
                let current = self.inner.current().unwrap_or_else(|| {
                    panic!("{op}: current() must be Some while positioned on doc {id}")
                });
                assert_eq!(
                    current.doc_id, id,
                    "{op}: current() must still return the result last yielded (doc {id})",
                );
            }
            Position::PastEnd => {
                assert!(
                    self.inner.at_eof(),
                    "{op}: at_eof() must be true once the iterator has run past its last result",
                );
                assert!(
                    self.inner.current().is_none(),
                    "{op}: current() must be None once the iterator has run past its last result \
                     — not the stale last result",
                );
            }
        }
    }

    /// Checks shared by every operation that ran the iterator past its last
    /// result: both EOF oracles must report it.
    #[track_caller]
    fn after_exhaustion(&mut self, op: &str) {
        assert!(
            self.inner.at_eof(),
            "{op}: at_eof() must be true once the operation has found nothing",
        );
        assert!(
            self.inner.current().is_none(),
            "{op}: current() must be None once the iterator has run past its last result — not \
             the stale last result",
        );
        self.position = Position::PastEnd;
    }
}

impl<'index, I: RQEIterator<'index>> RQEIterator<'index> for ContractChecker<I> {
    #[track_caller]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.assert_usable("current");
        match self.position {
            Position::Unread => {
                let at_eof = self.inner.at_eof();
                assert_eq!(
                    at_eof, self.at_eof_when_unread,
                    "current: at_eof() changed from {} to {at_eof} without any read or skip_to",
                    self.at_eof_when_unread,
                );
                let current = self.inner.current();
                if at_eof {
                    // Only an iterator that is empty by construction is at EOF
                    // while unread, and such an iterator has nothing current.
                    assert!(
                        current.is_none(),
                        "current: must be None on an iterator that reports at_eof() while unread",
                    );
                }
                current
            }
            Position::On(id) => {
                assert!(
                    !self.inner.at_eof(),
                    "current: at_eof() must be false while positioned on doc {id}",
                );
                let current = self.inner.current().unwrap_or_else(|| {
                    panic!("current: must be Some while positioned on doc {id}")
                });
                assert_eq!(
                    current.doc_id, id,
                    "current: must still return the result last yielded (doc {id})",
                );
                Some(current)
            }
            Position::PastEnd => {
                assert!(
                    self.inner.at_eof(),
                    "current: at_eof() must be true once the iterator has run past its last \
                     result",
                );
                assert!(
                    self.inner.current().is_none(),
                    "current: must be None once the iterator has run past its last result — not \
                     the stale last result",
                );
                None
            }
        }
    }

    #[track_caller]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.assert_usable("read");
        let previous = self.position;
        let outcome = self
            .inner
            .read()?
            .map(|result| (result.doc_id, result as *const RSIndexResult<'index>));
        match outcome {
            None => {
                self.after_exhaustion("read");
                Ok(None)
            }
            Some((id, yielded)) => {
                assert_ne!(
                    previous,
                    Position::PastEnd,
                    "read: an exhausted iterator must keep returning None, but it yielded doc \
                     {id}",
                );
                self.assert_may_yield_while_unread("read", previous, id);
                if self.ordered
                    && let Position::On(previous_id) = previous
                {
                    assert!(
                        id > previous_id,
                        "read: doc ids must strictly ascend between rewinds, but {id} follows \
                         {previous_id} — wrap deliberately unordered iterators with \
                         `ContractChecker::new_unordered`",
                    );
                }
                Ok(Some(self.after_yield("read", id, yielded)))
            }
        }
    }

    #[track_caller]
    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        self.assert_usable("skip_to");
        let previous = self.position;
        // The precondition is checked against the checker's own record of where
        // the iterator stands, not the iterator's self-report: an implementation
        // that under-reports `last_doc_id()` must not be able to license a probe
        // that walks the position backwards, which would then let a lower
        // document through every downstream check.
        let last = match previous {
            Position::On(id) => id,
            Position::Unread | Position::PastEnd => self.inner.last_doc_id(),
        };
        assert!(
            last < doc_id,
            "skip_to({doc_id}): the caller broke the precondition last_doc_id() < doc_id — \
             last_doc_id() is {last}",
        );
        if let Position::On(id) = previous {
            assert_eq!(
                self.inner.last_doc_id(),
                id,
                "skip_to({doc_id}): last_doc_id() must still report the result last yielded (doc \
                 {id})",
            );
        }
        let outcome = match self.inner.skip_to(doc_id) {
            Ok(outcome) => outcome,
            Err(error) => {
                assert_ne!(
                    self.inner.last_doc_id(),
                    doc_id,
                    "skip_to({doc_id}): a skip that carries no result must not claim the probed \
                     id as its position",
                );
                return Err(error);
            }
        };
        let summary = outcome.map(|outcome| match outcome {
            SkipToOutcome::Found(result) => {
                (true, result.doc_id, result as *const RSIndexResult<'index>)
            }
            SkipToOutcome::NotFound(result) => {
                (false, result.doc_id, result as *const RSIndexResult<'index>)
            }
        });
        match summary {
            None => {
                assert_ne!(
                    self.inner.last_doc_id(),
                    doc_id,
                    "skip_to({doc_id}): a skip that found nothing must not claim the probed id \
                     as its position — a parent reads that as \"the child holds this document\"",
                );
                self.after_exhaustion("skip_to");
                Ok(None)
            }
            Some((found, id, yielded)) => {
                assert_ne!(
                    previous,
                    Position::PastEnd,
                    "skip_to({doc_id}): an iterator that had run past its last result must keep \
                     finding nothing, but it produced doc {id}",
                );
                self.assert_may_yield_while_unread("skip_to", previous, id);
                if found {
                    assert_eq!(
                        id, doc_id,
                        "skip_to({doc_id}): Found promises a result at the requested id, got \
                         {id}",
                    );
                } else {
                    assert!(
                        id > doc_id,
                        "skip_to({doc_id}): NotFound promises the first result *greater* than \
                         the requested id, got {id}",
                    );
                }
                let current = self.after_yield("skip_to", id, yielded);
                Ok(Some(if found {
                    SkipToOutcome::Found(current)
                } else {
                    SkipToOutcome::NotFound(current)
                }))
            }
        }
    }

    #[track_caller]
    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        self.assert_usable("revalidate");
        let previous = self.position;
        let previous_last = self.inner.last_doc_id();
        let summary = match self.inner.revalidate(spec)? {
            RQEValidateStatus::Ok => RevalidateSummary::Ok,
            RQEValidateStatus::Moved { current } => RevalidateSummary::Moved(
                current.map(|result| (result.doc_id, result as *const RSIndexResult<'index>)),
            ),
            RQEValidateStatus::Aborted => RevalidateSummary::Aborted,
        };
        match summary {
            RevalidateSummary::Ok => {
                assert_eq!(
                    self.inner.last_doc_id(),
                    previous_last,
                    "revalidate: Ok promises the same position, but last_doc_id() changed",
                );
                // `Ok` is a promise about the whole position, not just the id:
                // an iterator that quietly dropped to EOF (or swapped its
                // current result) while leaving `last_doc_id()` alone reports
                // `Moved`, or it lies here.
                self.assert_position_unchanged("revalidate (ok)");
                Ok(RQEValidateStatus::Ok)
            }
            RevalidateSummary::Moved(Some((id, yielded))) => {
                // Moved onto a concrete document: the same agreement rules as
                // any other yield apply, but it is a reposition rather than a
                // new result, so it does not count against `num_estimated`.
                //
                // `Moved` also promises the position did not move *back*
                // (`iterator_api.h` documents it as moving forward), and a
                // caller emits `current` in place of a read before resuming
                // from there — so a move backwards replays documents, and one
                // out of the exhausted state resurrects an iterator that owes
                // nothing until a `rewind`. Landing on the same document is
                // accepted: an iterator may report `Moved` purely to hand back
                // a republished result object for the position it kept.
                self.assert_may_yield_while_unread("revalidate (moved)", previous, id);
                match previous {
                    Position::PastEnd => panic!(
                        "revalidate: an iterator that had run past its last result cannot move \
                         back onto a document without a rewind, but Moved reported doc {id}",
                    ),
                    Position::On(previous_id) if self.ordered => assert!(
                        id >= previous_id,
                        "revalidate: Moved must not move the position backwards, but doc {id} \
                         comes before doc {previous_id}",
                    ),
                    Position::Unread | Position::On(_) => {}
                }
                let current = self.assert_positioned_on("revalidate (moved)", id, yielded);
                Ok(RQEValidateStatus::Moved {
                    current: Some(current),
                })
            }
            RevalidateSummary::Moved(None) => {
                self.after_exhaustion("revalidate (moved to EOF)");
                Ok(RQEValidateStatus::Moved { current: None })
            }
            RevalidateSummary::Aborted => {
                self.aborted = true;
                Ok(RQEValidateStatus::Aborted)
            }
        }
    }

    #[track_caller]
    fn rewind(&mut self) {
        self.assert_usable("rewind");
        self.inner.rewind();
        self.position = Position::Unread;
        self.yielded = 0;
        assert_eq!(
            self.inner.at_eof(),
            self.at_eof_when_unread,
            "rewind: must restore the at_eof() answer the iterator gave when freshly constructed \
             ({}) — an exhausted state must not latch",
            self.at_eof_when_unread,
        );
    }

    #[track_caller]
    fn num_estimated(&self) -> usize {
        self.assert_usable("num_estimated");
        self.assert_estimate_bounds_yields("num_estimated")
    }

    #[track_caller]
    fn last_doc_id(&self) -> DocId {
        self.assert_usable("last_doc_id");
        let last = self.inner.last_doc_id();
        if let Position::On(id) = self.position {
            assert_eq!(
                last, id,
                "last_doc_id: must report the id of the result last yielded",
            );
        }
        last
    }

    #[track_caller]
    fn at_eof(&self) -> bool {
        self.assert_usable("at_eof");
        let at_eof = self.inner.at_eof();
        match self.position {
            Position::Unread => assert_eq!(
                at_eof, self.at_eof_when_unread,
                "at_eof: changed from {} to {at_eof} without any read or skip_to",
                self.at_eof_when_unread,
            ),
            Position::On(id) => assert!(
                !at_eof,
                "at_eof: must be false while positioned on doc {id}, including on the last \
                 result",
            ),
            Position::PastEnd => assert!(
                at_eof,
                "at_eof: must be true once the iterator has run past its last result",
            ),
        }
        at_eof
    }

    fn type_(&self) -> IteratorType {
        self.inner.type_()
    }

    fn as_c_iterator(&self) -> Option<&c2rust::CRQEIterator> {
        self.inner.as_c_iterator()
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        self.inner
            .intersection_sort_weight(prioritize_union_children)
    }
}
