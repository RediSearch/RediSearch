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
#[expect(clippy::undocumented_unsafe_blocks)]
#[expect(clippy::multiple_unsafe_ops_per_block)]
pub mod test_context;

use index_spec::IndexSpecReadGuard;
pub use mock_context::MockContext;
use rqe_iterators::{ResumeOutcome, TypeErasedRQEIterator, TypeErasedRQESuspendedIterator};
pub use test_context::{GlobalGuard, TestContext};

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

/// Test-only ergonomic accessors on
/// [`ResumeOutcome`](rqe_iterators::ResumeOutcome).
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
