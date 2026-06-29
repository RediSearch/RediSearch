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

pub use mock_context::MockContext;
pub use test_context::{GlobalGuard, TestContext};

/// Drive a suspend/resume cycle on `it` under the given lock guard.
///
/// Mirrors the production FFI `revalidate` callback (see
/// `rqe_iterators::interop::revalidate`): box-suspend → resume. Tests use this
/// to exercise the canonical suspend/resume path during the in-progress
/// migration away from `RQEIterator::revalidate`.
///
/// The resumed iterator comes back type-erased as a
/// [`BoxedRQEIterator`](rqe_iterators::BoxedRQEIterator) inside the returned
/// [`ResumeOutcome`](rqe_iterators::ResumeOutcome) — exactly what `resume`
/// yields — so callers keep driving it through the
/// [`RQEIterator`](rqe_iterators::RQEIterator) surface.
/// See [`ResumeOutcomeExt`] for `expect_ok` / `expect_moved`.
///
/// # Safety contract
///
/// `resume` hands the resumed iterator back at the guard's borrow lifetime;
/// this helper relabels it to the index lifetime `'spec` — the same relabel the
/// production FFI wrapper performs (see `rqe_iterators::interop::revalidate`).
/// Lifetimes are erased at runtime, so the relabel never changes the
/// representation.
pub fn revalidate_via_resume<'borrow, 'spec>(
    it: rqe_iterators::BoxedRQEIterator<'spec>,
    spec: &'borrow index_spec::IndexSpecReadGuard<'spec>,
) -> Result<rqe_iterators::ResumeOutcome<'spec>, rqe_iterators::RQEIteratorError> {
    let suspended = <rqe_iterators::BoxedRQEIterator<'spec> as rqe_iterators::RQEIteratorBoxed<
        'spec,
    >>::suspend(Box::new(it));
    // `resume` re-reads/seeks the index to restore position and can fail with an
    // `RQEIteratorError` (e.g. timeout); propagate it like the production path.
    let outcome =
        <rqe_iterators::BoxedRQESuspendedIterator as rqe_iterators::RQESuspendedIterator>::resume(
            suspended, spec,
        )?;
    // SAFETY: relabel the resumed iterator from the guard's borrow lifetime to
    // the index lifetime `'spec` — the same relabel the production FFI wrapper
    // performs. Only the (erased) lifetime changes; the representation is
    // identical, so the transmute is a no-op at runtime.
    Ok(unsafe {
        std::mem::transmute::<rqe_iterators::ResumeOutcome<'_>, rqe_iterators::ResumeOutcome<'spec>>(
            outcome,
        )
    })
}

/// Test-only ergonomic accessors on
/// [`ResumeOutcome`](rqe_iterators::ResumeOutcome).
pub trait ResumeOutcomeExt<'a> {
    /// Unwrap the resumed iterator, panicking unless the outcome is
    /// [`Ok`](rqe_iterators::ResumeOutcome::Ok).
    fn expect_ok(self) -> rqe_iterators::BoxedRQEIterator<'a>;

    /// Unwrap the resumed iterator, panicking unless the outcome is
    /// [`Moved`](rqe_iterators::ResumeOutcome::Moved).
    fn expect_moved(self) -> rqe_iterators::BoxedRQEIterator<'a>;
}

impl<'a> ResumeOutcomeExt<'a> for rqe_iterators::ResumeOutcome<'a> {
    #[track_caller]
    fn expect_ok(self) -> rqe_iterators::BoxedRQEIterator<'a> {
        match self {
            rqe_iterators::ResumeOutcome::Ok(it) => it,
            rqe_iterators::ResumeOutcome::Moved(_) => {
                panic!("expected ResumeOutcome::Ok, got Moved")
            }
            rqe_iterators::ResumeOutcome::Aborted => {
                panic!("expected ResumeOutcome::Ok, got Aborted")
            }
        }
    }

    #[track_caller]
    fn expect_moved(self) -> rqe_iterators::BoxedRQEIterator<'a> {
        match self {
            rqe_iterators::ResumeOutcome::Moved(it) => it,
            rqe_iterators::ResumeOutcome::Ok(_) => {
                panic!("expected ResumeOutcome::Moved, got Ok")
            }
            rqe_iterators::ResumeOutcome::Aborted => {
                panic!("expected ResumeOutcome::Moved, got Aborted")
            }
        }
    }
}
