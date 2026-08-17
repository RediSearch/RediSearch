/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Test utilities for rqe_iterators.
//!
//! This module provides utilities for testing iterators, including contexts
//! for setting up test environments.

pub mod contract_checker;
#[expect(clippy::undocumented_unsafe_blocks)]
#[expect(clippy::multiple_unsafe_ops_per_block)]
pub mod mock_context;
pub mod mock_expiration;
#[expect(clippy::undocumented_unsafe_blocks)]
#[expect(clippy::multiple_unsafe_ops_per_block)]
pub mod test_context;

pub use contract_checker::ContractChecker;
use index_spec::IndexSpecReadGuard;
pub use mock_context::MockContext;
pub use mock_expiration::MockExpirationChecker;
use rqe_core::DocId;
use rqe_iterators::{
    RQEIterator, ResumeOutcome, TypeErasedRQEIterator, TypeErasedRQESuspendedIterator,
};
pub use test_context::{GlobalGuard, TestContext};

/// Drive `it` from its current position to exhaustion, asserting that it
/// upholds the contract on [`current`](RQEIterator::current) and
/// [`at_eof`](RQEIterator::at_eof), in both directions.
///
/// Asserted, in order:
///
/// 1. after each successful `read`, `current()` is `Some` and agrees with both
///    the returned result's `doc_id` and [`last_doc_id`](RQEIterator::last_doc_id),
///    and `at_eof()` is `false` — *including* on the last result;
/// 2. once `read` returns `Ok(None)`, `current()` is `None` and `at_eof()` is
///    `true`;
/// 3. the iterator stays exhausted, still agreeing on both;
/// 4. after [`rewind`](RQEIterator::rewind) the iterator replays the identical
///    doc-id sequence — which also proves the exhausted state is cleared rather
///    than latched.
///
/// Nothing is asserted before the first `read`: an iterator that has not been
/// read is neither positioned on a result nor past the end, so `current()` is
/// unconstrained there — `None` for `TopKIterator`, a meaningless default for
/// everything else.
///
/// Returns the doc ids yielded, so callers can additionally assert on them.
///
/// # Panics
///
/// Panics on the first contract violation, or if `it` errors while draining.
/// Not suitable for iterators that panic on `rewind` (e.g. `UnionTrimmed`).
#[track_caller]
pub fn assert_current_contract<'index, I: RQEIterator<'index>>(it: &mut I) -> Vec<DocId> {
    let first_pass = drain_checking_current(it, "first pass");

    it.rewind();
    let second_pass = drain_checking_current(it, "after rewind");
    assert_eq!(
        first_pass, second_pass,
        "rewind must replay the same doc ids; a latched exhausted-state \
         truncates the second pass",
    );

    first_pass
}

/// Assert that a `skip_to` running past the last result reports EOF the same way
/// a `read` running past it does, without claiming the id it failed to reach as
/// its position.
///
/// `it` is rewound first, so the skip runs from a known position rather than
/// from wherever a previous drain left it — reaching the past-the-end state
/// *through* `skip_to` is the case an iterator can get wrong while still passing
/// [`assert_current_contract`], which only ever reaches it through `read`.
/// `past_end` must exceed every doc id `it` can yield.
///
/// Leaves `it` rewound.
///
/// # Panics
///
/// Panics on the first contract violation, if the skip errors, or if it
/// unexpectedly finds a result. Not suitable for iterators that panic on
/// `skip_to` or `rewind` (e.g. `UnionTrimmed`).
#[track_caller]
pub fn assert_current_contract_via_skip_to<'index, I: RQEIterator<'index>>(
    it: &mut I,
    past_end: DocId,
) {
    it.rewind();

    let ran_past_end = it
        .skip_to(past_end)
        .expect("skip_to must not fail")
        .is_none();
    assert!(
        ran_past_end,
        "skip_to({past_end}) must run past the end: {past_end} is meant to be \
         beyond every doc id this iterator can yield",
    );

    assert_ne!(
        it.last_doc_id(),
        past_end,
        "a skip_to returning None produced no result, so it must not claim the \
         probed id as its position: a parent reads that as \"the child holds this \
         document\" and unwraps current()",
    );
    assert!(
        it.current().is_none(),
        "current() must be None once a skip_to has run past the last result — \
         recording that step is not `read`'s job alone",
    );
    assert!(
        it.at_eof(),
        "at_eof() must be true once a skip_to has returned None",
    );

    it.rewind();
    assert_eof_agrees_with_current(it, "after rewind", "following a skip past the end");
}

/// Drain `it`, asserting the EOF contract at every step. `phase` names the pass
/// in assertion messages.
#[track_caller]
fn drain_checking_current<'index, I: RQEIterator<'index>>(it: &mut I, phase: &str) -> Vec<DocId> {
    let mut doc_ids = Vec::new();

    while let Some(result) = it.read().expect("read must not fail while draining") {
        let doc_id = result.doc_id;
        doc_ids.push(doc_id);

        assert_eq!(
            it.last_doc_id(),
            doc_id,
            "{phase}: last_doc_id() must track the result just returned by read()",
        );
        let current = it.current().unwrap_or_else(|| {
            panic!("{phase}: current() must be Some right after a successful read of doc {doc_id}")
        });
        assert_eq!(
            current.doc_id, doc_id,
            "{phase}: current() must return the result read() just yielded",
        );
        assert!(
            !it.at_eof(),
            "{phase}: at_eof() must be false while positioned on doc {doc_id}, \
             including when it is the last one",
        );
    }

    assert!(
        it.current().is_none(),
        "{phase}: current() must be None once the iterator has run past its \
         last result — it must not keep returning the stale last result",
    );
    assert!(
        it.at_eof(),
        "{phase}: at_eof() must be true once read() has returned None",
    );

    assert!(
        it.read()
            .expect("read must not fail once exhausted")
            .is_none(),
        "{phase}: an exhausted iterator must keep returning None",
    );
    assert_eof_agrees_with_current(it, phase, "after a read on an already-exhausted iterator");

    doc_ids
}

/// Assert that the two EOF answers agree at a single observation point. `at`
/// names the point in the assertion message.
#[track_caller]
fn assert_eof_agrees_with_current<'index, I: RQEIterator<'index>>(
    it: &mut I,
    phase: &str,
    at: &str,
) {
    let has_current = it.current().is_some();
    assert_eq!(
        it.at_eof(),
        !has_current,
        "{phase}: at_eof() and current() disagree {at} — at_eof() is {}, while \
         current() is {}",
        it.at_eof(),
        if has_current { "Some" } else { "None" },
    );
}

/// Drive a suspend/resume cycle on `it` under the given lock guard.
///
/// Mirrors the production FFI `revalidate` callback (see
/// `rqe_iterators::interop::revalidate`): box-suspend → resume. Tests use this
/// to exercise the canonical suspend/resume path during the in-progress
/// migration away from `RQEIterator::revalidate`.
///
/// See [`ResumeOutcomeExt`] for `expect_ok` / `expect_moved`.
///
/// # Contract checks
///
/// [`ContractChecker`] verifies the [`RQEIterator`] surface but not the
/// suspend/resume machinery, so the cross-cycle guarantees are asserted here
/// instead — this is the single funnel every resume test goes through. On any
/// outcome that hands an iterator back, it checks that
///
/// - exhaustion survived the cycle: an iterator that was at
///   [`at_eof`](RQEIterator::at_eof) is still at it afterwards. A composite drops
///   children that report EOF, so one that comes back live re-enters a parent
///   that has already moved on without it;
/// - the position did not move backwards, which would replay documents; and
/// - [`ResumeOutcome::Ok`] means exactly that — same position, same EOF answer —
///   matching the promise its [`RQEValidateStatus::Ok`] counterpart makes on the
///   `revalidate` side.
///
/// [`ContractChecker`]: crate::contract_checker::ContractChecker
/// [`RQEValidateStatus::Ok`]: rqe_iterators::RQEValidateStatus::Ok
pub fn revalidate_via_resume<'borrow, 'index>(
    it: TypeErasedRQEIterator<'index>,
    spec: &'borrow IndexSpecReadGuard<'index>,
) -> Result<ResumeOutcome<TypeErasedRQEIterator<'index>>, rqe_iterators::RQEIteratorError> {
    let was_at_eof = it.at_eof();
    let previous_last_doc_id = it.last_doc_id();

    let suspended =
        <TypeErasedRQEIterator<'index> as rqe_iterators::RQEIteratorBoxed<'index>>::suspend(
            Box::new(it),
        );
    // Resume via the dyn path: it yields a single `TypeErasedRQEIterator`,
    // whereas the concrete `RQESuspendedIterator::resume` on the already-erased
    // suspended type would double-box. `resume` re-reads/seeks the index to
    // restore position and can fail with an `RQEIteratorError` (e.g. timeout);
    // propagate it like the production path.
    let TypeErasedRQESuspendedIterator(inner) = *suspended;
    let outcome = inner.resume(spec)?;

    // `Aborted` yields no iterator to inspect; every other outcome does.
    if let ResumeOutcome::Ok(it) | ResumeOutcome::Moved(it) = &outcome {
        if was_at_eof {
            assert!(
                it.at_eof(),
                "resume: an iterator that had run past its last result cannot come back \
                 without a rewind",
            );
        }
        assert!(
            it.last_doc_id() >= previous_last_doc_id,
            "resume: must not move the position backwards, but doc {} comes before doc {}",
            it.last_doc_id(),
            previous_last_doc_id,
        );
        if matches!(outcome, ResumeOutcome::Ok(_)) {
            assert_eq!(
                it.last_doc_id(),
                previous_last_doc_id,
                "resume: Ok promises the same position, but last_doc_id() changed",
            );
            assert_eq!(
                it.at_eof(),
                was_at_eof,
                "resume: Ok promises the same position, but at_eof() changed",
            );
        }
    }

    Ok(outcome)
}

/// Test-only ergonomic accessors on [`ResumeOutcome`].
pub trait ResumeOutcomeExt<'a> {
    /// Unwrap the resumed iterator, panicking unless the outcome is
    /// [`Ok`](rqe_iterators::ResumeOutcome::Ok).
    fn expect_ok(self) -> TypeErasedRQEIterator<'a>;

    /// Unwrap the resumed iterator, panicking unless the outcome is
    /// [`Moved`](rqe_iterators::ResumeOutcome::Moved).
    fn expect_moved(self) -> TypeErasedRQEIterator<'a>;
}

impl<'a> ResumeOutcomeExt<'a> for ResumeOutcome<TypeErasedRQEIterator<'a>> {
    #[track_caller]
    fn expect_ok(self) -> TypeErasedRQEIterator<'a> {
        match self {
            ResumeOutcome::Ok(it) => it,
            ResumeOutcome::Moved(_) => {
                panic!("expected ResumeOutcome::Ok, got Moved")
            }
            ResumeOutcome::Aborted => {
                panic!("expected ResumeOutcome::Ok, got Aborted")
            }
        }
    }

    #[track_caller]
    fn expect_moved(self) -> TypeErasedRQEIterator<'a> {
        match self {
            ResumeOutcome::Moved(it) => it,
            ResumeOutcome::Ok(_) => {
                panic!("expected ResumeOutcome::Moved, got Ok")
            }
            ResumeOutcome::Aborted => {
                panic!("expected ResumeOutcome::Moved, got Aborted")
            }
        }
    }
}
