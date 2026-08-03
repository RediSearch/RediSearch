/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Cross-iterator conformance tests for the [`RQEIterator::current`]
//! has-current contract.
//!
//! Every iterator that participates in suspend/resume must report `None` from
//! `current()` once it has run past its last result — that is the signal a
//! composite parent uses to tell "moved to a new document" from "moved off the
//! end" when its child resumes (see `ResumeOutcome::Moved`). An iterator that
//! keeps handing out its last result silently collapses those two cases into
//! the first, reinstating exactly the stale position the resume was meant to
//! repair.
//!
//! Per-iterator suites test behaviour; this file tests that they all agree on
//! the shared contract. Iterators that do not yet uphold it are listed here as
//! `#[ignore]`d tests rather than omitted, so the gap stays visible.
//!
//! [`RQEIterator::current`]: rqe_iterators::RQEIterator::current

use rqe_core::DocId;
use rqe_iterators::{Empty, IdList};
use rqe_iterators_test_utils::assert_current_contract;
use timeout::NoTimeoutChecker;

use crate::utils;

const DOCS: [DocId; 5] = [10, 20, 30, 50, 80];

// ---------------------------------------------------------------------------
// Conforming
// ---------------------------------------------------------------------------

#[test]
fn mock_upholds_current_contract() {
    let mut it = utils::Mock::new(DOCS);
    assert_eq!(assert_current_contract(&mut it), DOCS);
}

#[test]
fn mock_vec_upholds_current_contract() {
    let mut it = utils::MockVec::new(DOCS.to_vec());
    assert_eq!(assert_current_contract(&mut it), DOCS);
}

#[test]
fn empty_upholds_current_contract() {
    let mut it = Empty;
    assert!(assert_current_contract(&mut it).is_empty());
}

#[test]
fn optional_upholds_current_contract() {
    // Yields every id in 1..=5, real at 2 and 4, virtual elsewhere.
    let mut it = rqe_iterators::optional::Optional::new(5, 2.0, utils::Mock::new([2u64, 4]));
    assert_eq!(assert_current_contract(&mut it), [1, 2, 3, 4, 5]);
}

#[test]
fn optional_optimized_upholds_current_contract() {
    let mut it = rqe_iterators::optional_optimized::OptionalOptimized::new(
        utils::Mock::new([1u64, 2, 3, 4, 5]),
        utils::Mock::new([2u64, 4]),
        5,
        2.0,
    );
    assert_eq!(assert_current_contract(&mut it), [1, 2, 3, 4, 5]);
}

// ---------------------------------------------------------------------------
// Not yet conforming
//
// Each keeps returning its last result after `read()` has returned `None`, so a
// parent asking "did my child move to a new doc, or off the end?" is told "new
// doc" either way.
//
// `Wildcard` and `IdList` already implement `RQEIteratorBoxed`, so that is
// reachable from a resume today. `Not` does not yet — for it this is a latent
// violation, which becomes reachable when it is migrated.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "Wildcard::current() clamps on its last result; needs a past-the-end \
            state like the inverted-index iterators have"]
fn wildcard_upholds_current_contract() {
    let mut it = rqe_iterators::Wildcard::new(5, 1.0);
    assert_eq!(assert_current_contract(&mut it), [1, 2, 3, 4, 5]);
}

#[test]
#[ignore = "IdList::current() clamps on its last result; `offset` could encode \
            past-the-end the way the mocks' `next_index` does"]
fn id_list_upholds_current_contract() {
    let mut it: IdList<'_, true> = IdList::new(DOCS.to_vec());
    assert_eq!(assert_current_contract(&mut it), DOCS);
}

#[test]
#[ignore = "Not::current() clamps on its last result; latent until Not is \
            migrated to RQEIteratorBoxed"]
fn not_upholds_current_contract() {
    // `Not` over [2, 4] within 1..=5 yields the complement.
    let mut it =
        rqe_iterators::not::Not::new(utils::Mock::new([2u64, 4]), 5, 1.0, NoTimeoutChecker);
    assert_eq!(assert_current_contract(&mut it), [1, 3, 5]);
}

