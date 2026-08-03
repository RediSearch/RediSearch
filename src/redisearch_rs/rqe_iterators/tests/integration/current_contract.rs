/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Cross-iterator conformance tests for the contract on
//! [`RQEIterator::current`] and [`RQEIterator::at_eof`], whose definition lives
//! on those methods.
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
// Test doubles
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

// ---------------------------------------------------------------------------
// Leaves
// ---------------------------------------------------------------------------

#[test]
fn empty_upholds_current_contract() {
    let mut it = Empty;
    assert!(assert_current_contract(&mut it).is_empty());
    assert_current_contract_via_skip_to(&mut it, PAST_DOCS);
}

#[test]
fn id_list_sorted_upholds_current_contract() {
    let mut it: IdList<'_, true> = IdList::new(DOCS.to_vec());
    assert_eq!(assert_current_contract(&mut it), DOCS);
    assert_current_contract_via_skip_to(&mut it, PAST_DOCS);
}

#[test]
fn id_list_unsorted_upholds_current_contract() {
    let mut it: IdList<'_, false> = IdList::new(DOCS.to_vec());
    assert_eq!(assert_current_contract(&mut it), DOCS);
}

#[test]
fn metric_upholds_current_contract() {
    // `Metric` delegates `current()` to an inner `IdList`, as do `MetricLazy`
    // and `IdListLazy`.
    let mut it =
        rqe_iterators::metric::MetricSortedById::new(vec![1u64, 3, 5], vec![0.1, 0.3, 0.5]);
    assert_eq!(assert_current_contract(&mut it), [1, 3, 5]);
}

#[test]
fn wildcard_upholds_current_contract() {
    let mut it = rqe_iterators::Wildcard::new(5, 1.0);
    assert_eq!(assert_current_contract(&mut it), [1, 2, 3, 4, 5]);
    assert_current_contract_via_skip_to(&mut it, 6);
}

#[test]
fn geo_shape_upholds_current_contract() {
    let mut it = rqe_iterators::GeoShape::new(
        DOCS.to_vec(),
        NoTimeoutChecker,
        rqe_iterators::NoOpChecker,
        rqe_iterators::NoTracker,
    );
    assert_eq!(assert_current_contract(&mut it), DOCS);
    assert_current_contract_via_skip_to(&mut it, PAST_DOCS);
}

// ---------------------------------------------------------------------------
// Composites — these already delegated correctly; asserted so they stay that way
// ---------------------------------------------------------------------------

#[test]
fn optional_upholds_current_contract() {
    // Yields every id in 1..=5, real at 2 and 4, virtual elsewhere.
    let mut it = rqe_iterators::optional::Optional::new(5, 2.0, utils::Mock::new([2u64, 4]));
    assert_eq!(assert_current_contract(&mut it), [1, 2, 3, 4, 5]);
    assert_current_contract_via_skip_to(&mut it, 6);
}

#[test]
fn intersection_upholds_current_contract() {
    let children = vec![
        utils::Mock::new([1u64, 2, 3]),
        utils::Mock::new([2u64, 3, 9]),
    ];
    let mut it = rqe_iterators::Intersection::new(children, 1.0, false);
    assert_eq!(assert_current_contract(&mut it), [2, 3]);
}

#[test]
fn union_full_flat_upholds_current_contract() {
    let children: Vec<Box<dyn rqe_iterators::RQEIterator<'static>>> = vec![
        Box::new(utils::Mock::new([1u64, 3])),
        Box::new(utils::Mock::new([2u64, 4])),
    ];
    let mut it = rqe_iterators::UnionFullFlat::new(children);
    assert_eq!(assert_current_contract(&mut it), [1, 2, 3, 4]);
}

#[test]
fn union_full_heap_upholds_current_contract() {
    let children: Vec<Box<dyn rqe_iterators::RQEIterator<'static>>> = vec![
        Box::new(utils::Mock::new([1u64, 3])),
        Box::new(utils::Mock::new([2u64, 4])),
    ];
    let mut it = rqe_iterators::UnionFullHeap::new(children);
    assert_eq!(assert_current_contract(&mut it), [1, 2, 3, 4]);
}

#[test]
fn profile_upholds_current_contract() {
    let mut it = rqe_iterators::profile::Profile::new(utils::Mock::new(DOCS));
    assert_eq!(assert_current_contract(&mut it), DOCS);
}

#[test]
fn maybe_empty_upholds_current_contract() {
    let mut it = rqe_iterators::maybe_empty::MaybeEmpty::new(utils::Mock::new(DOCS));
    assert_eq!(assert_current_contract(&mut it), DOCS);
}

#[test]
fn not_upholds_current_contract() {
    // `Not` over [2, 4] within 1..=5 yields the complement.
    let mut it =
        rqe_iterators::not::Not::new(utils::Mock::new([2u64, 4]), 5, 1.0, NoTimeoutChecker);
    assert_eq!(assert_current_contract(&mut it), [1, 3, 5]);
    assert_current_contract_via_skip_to(&mut it, 6);
}

#[test]
fn not_optimized_upholds_current_contract() {
    let mut it = rqe_iterators::not_optimized::NotOptimized::new(
        utils::Mock::new([1u64, 2, 3, 4, 5]),
        utils::Mock::new([2u64, 4]),
        5,
        1.0,
        NoTimeoutChecker,
    );
    assert_eq!(assert_current_contract(&mut it), [1, 3, 5]);
    assert_current_contract_via_skip_to(&mut it, 6);
}

// ---------------------------------------------------------------------------
// Not yet conforming
//
// Each of these clamps on its last result instead of recording the step past it,
// so it fails both halves at once: `current()` keeps handing that result out
// after `read()` has returned `None`, and `at_eof()` — derived from the same
// position — reports EOF while the result is still live.
// ---------------------------------------------------------------------------

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
