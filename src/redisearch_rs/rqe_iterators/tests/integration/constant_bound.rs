/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`ConstantBound`] reports one bound for every document and ends a thresholded read it cannot
//! reach.

use rqe_core::DocId;
use rqe_iterators::{
    ConstantBound, Intersection, RQEIterator, UnionFullFlat,
    id_list::IdListSorted,
    score_bound::{BlockBound, MinCompetitiveScore},
};
use rqe_iterators_test_utils::ContractChecker;

use crate::utils::blocked::{Block, Blocked, drain_above};

fn threshold(value: f64) -> MinCompetitiveScore {
    MinCompetitiveScore::new(value).expect("a positive threshold")
}

fn constant(
    ids: &[DocId],
    max_score: f64,
) -> ContractChecker<ConstantBound<IdListSorted<'static>>> {
    ContractChecker::new(ConstantBound::new(
        IdListSorted::new(ids.to_vec()),
        max_score,
    ))
}

fn read_all<'index>(it: &mut impl RQEIterator<'index>) -> Vec<DocId> {
    std::iter::from_fn(|| it.read().expect("read failed").map(|r| r.doc_id)).collect()
}

#[test]
fn reports_its_bound_while_positioned() {
    let mut it = constant(&[3, 7], 1.5);
    assert!(it.reports_score_bounds());
    assert_eq!(
        it.current_block_bound(),
        None,
        "not positioned before the first read"
    );

    it.read().expect("read failed").expect("a document");
    assert_eq!(it.current_max_score(), 1.5);
    assert_eq!(
        it.current_block_bound(),
        Some(BlockBound {
            last_doc_id: DocId::MAX,
            max_score: 1.5,
        }),
    );

    assert_eq!(read_all(&mut it), [7]);
    assert_eq!(it.current_block_bound(), None, "not positioned at EOF");
}

#[test]
fn plain_reads_and_skips_pass_through() {
    let mut it = constant(&[2, 4, 6, 8], 1.0);
    assert_eq!(it.num_estimated(), 4);
    it.skip_to(5).expect("skip failed").expect("a document");
    assert_eq!(it.last_doc_id(), 6);
    assert_eq!(read_all(&mut it), [8]);
    it.rewind();
    assert_eq!(read_all(&mut it), [2, 4, 6, 8]);
}

#[test]
fn thresholded_read_that_the_bound_reaches_is_a_plain_read() {
    let mut it = constant(&[1, 2, 3], 2.0);
    assert_eq!(drain_above(&mut it, threshold(2.0)), [1, 2, 3]);
}

#[test]
fn thresholded_read_below_the_bound_ends_the_iterator_until_rewind() {
    let mut it = constant(&[1, 2, 3], 0.5);
    it.read().expect("read failed").expect("a document");

    assert_eq!(drain_above(&mut it, threshold(1.0)), Vec::<DocId>::new());
    assert!(it.at_eof());
    assert!(it.read().expect("read failed").is_none());
    assert!(it.skip_to(3).expect("skip failed").is_none());

    it.rewind();
    assert_eq!(read_all(&mut it), [1, 2, 3]);
}

#[test]
fn a_zero_bound_ends_any_thresholded_read() {
    let mut it = constant(&[1, 2], 0.0);
    assert_eq!(
        drain_above(&mut it, threshold(f64::MIN_POSITIVE)),
        Vec::<DocId>::new()
    );
}

/// The composite case the wrapper exists for: a term bounded per block, intersected with a
/// filter that adds the same amount to every document.
#[test]
fn intersection_skips_the_windows_the_constant_cannot_lift_over_the_threshold() {
    let term = Blocked::new(vec![
        Block::uniform([1, 2, 3], 0.5),
        Block::uniform([4, 5, 6], 2.0),
        Block::uniform([7, 8, 9], 0.5),
    ]);
    let log = term.log();
    let filter = ConstantBound::new(IdListSorted::new((1..=9).collect::<Vec<_>>()), 1.0);
    let mut it = ContractChecker::new(Intersection::new(
        vec![
            Box::new(term) as Box<dyn RQEIterator<'static>>,
            Box::new(filter),
        ],
        1.0,
        false,
    ));

    assert_eq!(drain_above(&mut it, threshold(2.0)), [4, 5, 6]);
    // The weak term blocks plus the filter's 1.0 stay below 2.0, so the intersection jumps out
    // of each of them past the document it landed on.
    let visited = &log.borrow().visited;
    assert!(
        ![3, 8, 9].iter().any(|doc| visited.contains(doc)),
        "visited {visited:?}"
    );
}

#[test]
fn union_sums_the_constant_only_on_documents_it_holds() {
    let mut it = UnionFullFlat::new(vec![
        Box::new(ConstantBound::new(IdListSorted::new(vec![1, 2]), 1.0))
            as Box<dyn RQEIterator<'static>>,
        Box::new(ConstantBound::new(IdListSorted::new(vec![2]), 3.0)),
    ]);
    it.read().expect("read failed").expect("a document");
    assert_eq!(it.current_max_score(), 1.0);
    it.read().expect("read failed").expect("a document");
    assert_eq!(it.current_max_score(), 4.0);
}

#[test]
#[should_panic(expected = "a constant bound must be at least 0")]
fn a_negative_bound_is_rejected() {
    ConstantBound::new(IdListSorted::new(vec![1]), -1.0);
}

#[test]
#[should_panic(expected = "a constant bound must be at least 0")]
fn a_nan_bound_is_rejected() {
    ConstantBound::new(IdListSorted::new(vec![1]), f64::NAN);
}
