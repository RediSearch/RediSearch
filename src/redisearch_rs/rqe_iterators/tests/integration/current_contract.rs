/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Cross-iterator conformance tests for the [`RQEIterator::current`]
//! has-current contract, and for [`RQEIterator::at_eof`] being exactly its
//! negation.
//!
//! Every iterator must report `None` from `current()` — and `true` from
//! `at_eof()` — once it has run past its last result, and neither before that.
//! It is the signal a composite parent uses to tell "moved to a new document"
//! from "moved off the end" when its child resumes (see `ResumeOutcome::Moved`),
//! and the two ways of getting it wrong are symmetric. An iterator that keeps
//! handing out its last result collapses the second case into the first,
//! reinstating the stale position the resume was meant to repair. One that
//! reports EOF while still sitting on its last result collapses the first into
//! the second, and a parent rebuilding its live-child set drops a document that
//! was still there.
//!
//! Per-iterator suites test behaviour; this file tests that they all agree on
//! the shared contract. Iterators that do not yet uphold it are listed here as
//! `#[ignore]`d tests rather than omitted, so the gap stays visible.
//!
//! [`RQEIterator::current`]: rqe_iterators::RQEIterator::current
//! [`RQEIterator::at_eof`]: rqe_iterators::RQEIterator::at_eof

use rqe_core::DocId;
use rqe_iterators::{Empty, IdList};
use rqe_iterators_test_utils::{assert_current_contract, assert_current_contract_via_skip_to};
use timeout::NoTimeoutChecker;

use crate::utils;

const DOCS: [DocId; 5] = [10, 20, 30, 50, 80];

/// A target beyond every id in [`DOCS`], for the `skip_to` half of the contract.
const PAST_DOCS: DocId = 81;

// ---------------------------------------------------------------------------
// Conforming
// ---------------------------------------------------------------------------

#[test]
fn mock_upholds_current_contract() {
    let mut it = utils::Mock::new(DOCS);
    assert_eq!(assert_current_contract(&mut it), DOCS);
    assert_current_contract_via_skip_to(&mut it, PAST_DOCS);
}

#[test]
fn mock_vec_upholds_current_contract() {
    let mut it = utils::MockVec::new(DOCS.to_vec());
    assert_eq!(assert_current_contract(&mut it), DOCS);
    assert_current_contract_via_skip_to(&mut it, PAST_DOCS);
}

#[test]
fn empty_upholds_current_contract() {
    let mut it = Empty;
    assert!(assert_current_contract(&mut it).is_empty());
    assert_current_contract_via_skip_to(&mut it, PAST_DOCS);
}

// ---------------------------------------------------------------------------
// Not yet conforming
//
// Each of these clamps on its last result instead of recording the step past it,
// so it fails the contract twice over: `current()` keeps handing that result out
// after `read()` has returned `None`, and `at_eof()` — computed from the same
// position — is already `true` while the result is still live. A parent asking
// "did my child move to a new doc, or off the end?" cannot get a straight answer
// either way round.
//
// `Wildcard` and `IdList` already implement `RQEIteratorBoxed`, so that is
// reachable from a resume today. `Not`/`NotOptimized` do not yet — for them this
// is a latent violation, which becomes reachable when they are migrated.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "Wildcard clamps on its last result; needs a past-the-end state like \
            the inverted-index iterators have"]
fn wildcard_upholds_current_contract() {
    let mut it = rqe_iterators::Wildcard::new(5, 1.0);
    assert_eq!(assert_current_contract(&mut it), [1, 2, 3, 4, 5]);
}

#[test]
#[ignore = "IdList clamps on its last result; `offset` could encode past-the-end \
            the way the mocks' `next_index` does"]
fn id_list_upholds_current_contract() {
    let mut it: IdList<'_, true> = IdList::new(DOCS.to_vec());
    assert_eq!(assert_current_contract(&mut it), DOCS);
}

#[test]
#[ignore = "Not clamps on its last result; latent until Not is migrated to \
            RQEIteratorBoxed"]
fn not_upholds_current_contract() {
    // `Not` over [2, 4] within 1..=5 yields the complement.
    let mut it =
        rqe_iterators::not::Not::new(utils::Mock::new([2u64, 4]), 5, 1.0, NoTimeoutChecker);
    assert_eq!(assert_current_contract(&mut it), [1, 3, 5]);
}

#[test]
#[ignore = "NotOptimized clamps on its last result; latent until it is migrated \
            to RQEIteratorBoxed"]
fn not_optimized_upholds_current_contract() {
    let mut it = rqe_iterators::not_optimized::NotOptimized::new(
        utils::Mock::new([1u64, 2, 3, 4, 5]),
        utils::Mock::new([2u64, 4]),
        5,
        1.0,
        NoTimeoutChecker,
    );
    assert_eq!(assert_current_contract(&mut it), [1, 3, 5]);
}

#[test]
#[ignore = "fixed in the following change"]
fn optional_upholds_current_contract() {
    // Yields every id in 1..=5, real at 2 and 4, virtual elsewhere.
    let mut it = rqe_iterators::optional::Optional::new(5, 2.0, utils::Mock::new([2u64, 4]));
    assert_eq!(assert_current_contract(&mut it), [1, 2, 3, 4, 5]);
}

#[test]
#[ignore = "OptionalOptimized clamps on its last result; fixed on the branch that \
            reworks it (iter-reval-C3a-prof-opt), which is based elsewhere"]
fn optional_optimized_upholds_current_contract() {
    let mut it = rqe_iterators::optional_optimized::OptionalOptimized::new(
        utils::Mock::new([1u64, 2, 3, 4, 5]),
        utils::Mock::new([2u64, 4]),
        5,
        2.0,
    );
    assert_eq!(assert_current_contract(&mut it), [1, 2, 3, 4, 5]);
}

#[test]
#[ignore = "GeoShape clamps on its last id; `offset` could encode past-the-end \
            the way IdList's does"]
fn geo_shape_upholds_current_contract() {
    let mut it = rqe_iterators::GeoShape::new(
        DOCS.to_vec(),
        NoTimeoutChecker,
        rqe_iterators::NoOpChecker,
        rqe_iterators::NoTracker,
    );
    assert_eq!(assert_current_contract(&mut it), DOCS);
}
