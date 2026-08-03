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

#[expect(clippy::undocumented_unsafe_blocks)]
#[expect(clippy::multiple_unsafe_ops_per_block)]
pub mod mock_context;
pub mod mock_expiration;
#[expect(clippy::undocumented_unsafe_blocks)]
#[expect(clippy::multiple_unsafe_ops_per_block)]
pub mod test_context;

use index_spec::IndexSpecReadGuard;
pub use mock_context::MockContext;
pub use mock_expiration::MockExpirationChecker;
use rqe_core::DocId;
use rqe_iterators::{
    RQEIterator, ResumeOutcome, TypeErasedRQEIterator, TypeErasedRQESuspendedIterator,
};
pub use test_context::{GlobalGuard, TestContext};

/// Drive `it` from its current position to exhaustion, asserting that it
/// upholds the [`current`](RQEIterator::current) has-current contract.
///
/// The contract is what makes [`ResumeOutcome::Moved`] actionable: a composite
/// recovers "moved to a new document" vs. "moved off the end" by asking its
/// child for a current result. A child that keeps handing out its last result
/// after exhaustion silently turns the second case into the first, which is
/// exactly the stale-position bug the suspend/resume machinery exists to avoid.
///
/// Asserted, in order:
///
/// 1. after each successful `read`, `current()` is `Some` and agrees with both
///    the returned result's `doc_id` and [`last_doc_id`](RQEIterator::last_doc_id);
/// 2. once `read` returns `Ok(None)`, [`at_eof`](RQEIterator::at_eof) is `true`
///    and `current()` is `None`;
/// 3. the iterator stays exhausted, and keeps reporting no current;
/// 4. after [`rewind`](RQEIterator::rewind) the iterator replays the identical
///    doc-id sequence — which also proves the exhausted state is cleared rather
///    than latched.
///
/// Returns the doc ids yielded, so callers can additionally assert on them.
///
/// Note that `at_eof()` is deliberately *not* asserted to be `false` while
/// results remain: it is a look-ahead and is allowed to go `true` while the
/// iterator still sits on its last result.
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

/// Drain `it`, asserting the has-current contract at every step. `phase` names
/// the pass in assertion messages.
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
    }

    assert!(
        it.at_eof(),
        "{phase}: at_eof() must be true once read() has returned None",
    );
    assert!(
        it.current().is_none(),
        "{phase}: current() must be None once the iterator has run past its \
         last result — it must not keep returning the stale last result",
    );

    assert!(
        it.read()
            .expect("read must not fail once exhausted")
            .is_none(),
        "{phase}: an exhausted iterator must keep returning None",
    );
    assert!(
        it.current().is_none(),
        "{phase}: current() must stay None once exhausted",
    );

    doc_ids
}

/// Drive a suspend/resume cycle on `it` under the given lock guard.
///
/// Mirrors the production FFI `revalidate` callback (see
/// `rqe_iterators::interop::revalidate`): box-suspend → resume. Tests use this
/// to exercise the canonical suspend/resume path during the in-progress
/// migration away from `RQEIterator::revalidate`.
///
/// See [`ResumeOutcomeExt`] for `expect_ok` / `expect_moved`.
pub fn revalidate_via_resume<'borrow, 'index>(
    it: TypeErasedRQEIterator<'index>,
    spec: &'borrow IndexSpecReadGuard<'index>,
) -> Result<ResumeOutcome<TypeErasedRQEIterator<'index>>, rqe_iterators::RQEIteratorError> {
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
    inner.resume(spec)
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
