/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::collections::HashMap;

use index_result::RSResultKind;
use inverted_index::NumericFilter;
use rqe_core::DocId;
use rqe_iterators::utils::{DeadlineTimeoutChecker, NoTimeoutChecker};
use rqe_iterators::{
    Candidates, DocumentElementValues, ElementFilter, ElementPredicate, ElementValues, RQEIterator,
    RQEIteratorError, RQEValidateStatus, SkipToOutcome, UnionFullFlat, intersection::Intersection,
};
use sorting_vector::RSSortingVector;
use value::{ElementNumbers, SharedValue};

use crate::utils::{Mock, MockRevalidateResult};

/// The values of one document: `fields[slot][element]`.
#[derive(Clone)]
struct Fields(Vec<Vec<Option<f64>>>);

impl DocumentElementValues for Fields {
    fn value(&self, slot: usize, element: u32) -> Option<f64> {
        *self.0.get(slot)?.get(element as usize)?
    }

    fn num_elements(&self, slot: usize) -> u32 {
        self.0.get(slot).map_or(0, |values| values.len() as u32)
    }
}

/// The documents of an index that has one per-element field, in slot 0.
struct OneField(HashMap<DocId, Fields>);

impl OneField {
    /// `docs` pairs each document with its values by element number.
    fn new<const N: usize>(docs: [(DocId, &[Option<f64>]); N]) -> Self {
        Self(
            docs.into_iter()
                .map(|(doc_id, values)| (doc_id, Fields(vec![values.to_vec()])))
                .collect(),
        )
    }
}

impl ElementValues for OneField {
    type Document = Fields;

    fn document(&self, doc_id: DocId) -> Option<Fields> {
        self.0.get(&doc_id).cloned()
    }
}

/// A predicate accepting values of `slot` that are strictly greater than `min`.
fn greater_than(slot: usize, min: f64) -> ElementPredicate {
    ElementPredicate {
        filter: NumericFilter {
            min,
            max: f64::INFINITY,
            min_inclusive: false,
            max_inclusive: true,
            ..Default::default()
        },
        slot,
    }
}

/// A predicate accepting values of `slot` that are strictly less than `max`.
fn less_than(slot: usize, max: f64) -> ElementPredicate {
    ElementPredicate {
        filter: NumericFilter {
            min: f64::NEG_INFINITY,
            max,
            min_inclusive: true,
            max_inclusive: false,
            ..Default::default()
        },
        slot,
    }
}

/// The ids of every document `it` still has to yield.
fn read_all<'index>(it: &mut impl RQEIterator<'index>) -> Vec<DocId> {
    let mut doc_ids = Vec::new();
    while let Some(result) = it.read().expect("read failed") {
        doc_ids.push(result.doc_id);
    }
    doc_ids
}

/// Documents 1 to 4, each with element 1 as its candidate. The value of that element is
/// above 100 in documents 1 and 4 only.
const CANDIDATE_DOCS: [DocId; 4] = [1, 2, 3, 4];
const CANDIDATE_ELEMENTS: [u8; 4] = [1, 1, 1, 1];

fn candidate_values() -> OneField {
    OneField::new([
        (1, &[Some(500.0), Some(150.0)]),
        (2, &[Some(500.0), Some(90.0)]),
        (3, &[Some(500.0), Some(100.0)]),
        (4, &[None, Some(101.0)]),
    ])
}

/// A filter keeping the [`CANDIDATE_DOCS`] whose candidate element is above 100.
fn above_100<'index>() -> ElementFilter<'index, Mock<'index, 4>, OneField, NoTimeoutChecker> {
    let child = Mock::new_with_positions(CANDIDATE_DOCS, CANDIDATE_ELEMENTS);
    ElementFilter::new(
        child,
        1.0,
        Candidates::CommonPositions,
        vec![greater_than(0, 100.0)],
        candidate_values(),
        NoTimeoutChecker,
    )
}

/// A filter of `child` with weight 1 and no timeout.
fn filter<'index, I>(
    child: I,
    candidates: Candidates,
    predicates: Vec<ElementPredicate>,
    values: OneField,
) -> ElementFilter<'index, I, OneField, NoTimeoutChecker> {
    ElementFilter::new(child, 1.0, candidates, predicates, values, NoTimeoutChecker)
}

/// A filter keeping the documents of `child` whose value in a common position is above 100.
fn common_position_above_100<'index, I>(
    child: I,
    values: OneField,
) -> ElementFilter<'index, I, OneField, NoTimeoutChecker> {
    filter(
        child,
        Candidates::CommonPositions,
        vec![greater_than(0, 100.0)],
        values,
    )
}

#[test]
fn read_skips_documents_whose_candidate_fails_the_predicate() {
    let mut it = above_100();

    assert_eq!(read_all(&mut it), [1, 4]);
    assert!(it.at_eof());
}

#[test]
fn candidate_without_a_value_is_rejected() {
    let child = Mock::new_with_positions([1, 2], [1, 1]);
    let values = OneField::new([
        (1, &[Some(500.0), None]),
        // The array is shorter than the candidate's element number.
        (2, &[Some(500.0)]),
    ]);
    let mut it = common_position_above_100(child, values);

    assert!(read_all(&mut it).is_empty());
}

#[test]
fn document_the_index_no_longer_has_is_rejected() {
    let child = Mock::new_with_positions([1, 2], [1, 1]);
    let values = OneField::new([(2, &[None, Some(150.0)])]);
    let mut it = common_position_above_100(child, values);

    assert_eq!(read_all(&mut it), [2]);
}

#[test]
fn child_without_positions_names_no_candidate() {
    let child = Mock::new([1]);
    let values = OneField::new([(1, &[Some(150.0), Some(150.0)])]);
    let mut it = common_position_above_100(child, values);

    assert!(read_all(&mut it).is_empty());
}

#[test]
fn any_passing_candidate_keeps_the_document() {
    // The union names elements 1 and 2 of each document as candidates.
    let first: Mock<'_, 2> = Mock::new_with_positions([1, 2], [1, 1]);
    let second: Mock<'_, 2> = Mock::new_with_positions([1, 2], [2, 2]);
    let child = UnionFullFlat::new(vec![first, second]);
    let values = OneField::new([
        // Only the second candidate passes.
        (1, &[None, Some(90.0), Some(150.0)]),
        (2, &[Some(500.0), Some(90.0), Some(80.0)]),
    ]);
    let mut it = common_position_above_100(child, values);

    assert_eq!(read_all(&mut it), [1]);
}

#[test]
fn candidates_of_an_intersection_are_the_elements_its_children_share() {
    // Document 1: both children match element 1. Document 2: elements 2 and 1.
    let first: Mock<'_, 2> = Mock::new_with_positions([1, 2], [1, 2]);
    let second: Mock<'_, 2> = Mock::new_with_positions([1, 2], [1, 1]);
    let child = Intersection::new(vec![first, second], 1.0, false);
    let values = OneField::new([
        (1, &[None, Some(150.0)]),
        (2, &[None, Some(150.0), Some(150.0)]),
    ]);
    let mut it = common_position_above_100(child, values);

    assert_eq!(read_all(&mut it), [1]);
}

#[test]
fn every_predicate_must_hold_on_one_element() {
    let first: Mock<'_, 2> = Mock::new_with_positions([1, 2], [1, 1]);
    let second: Mock<'_, 2> = Mock::new_with_positions([1, 2], [2, 2]);
    let child = UnionFullFlat::new(vec![first, second]);
    let two_fields =
        |slot0: &[Option<f64>], slot1: &[Option<f64>]| Fields(vec![slot0.to_vec(), slot1.to_vec()]);
    let values = OneField(HashMap::from([
        // Each predicate holds, but on a different element.
        (
            1,
            two_fields(
                &[None, Some(150.0), Some(50.0)],
                &[None, Some(50.0), Some(5.0)],
            ),
        ),
        (
            2,
            two_fields(
                &[None, Some(50.0), Some(150.0)],
                &[None, Some(50.0), Some(5.0)],
            ),
        ),
    ]));
    let predicates = vec![greater_than(0, 100.0), less_than(1, 10.0)];
    let mut it = filter(child, Candidates::CommonPositions, predicates, values);

    assert_eq!(read_all(&mut it), [2]);
}

#[test]
fn all_elements_are_candidates_when_the_child_only_names_documents() {
    // The child records no positions. In document 1 no single element is both above 100
    // and below 200; in document 2 the last element is.
    let child = Mock::new([1, 2]);
    let values = OneField::new([
        (1, &[Some(50.0), None, Some(500.0)]),
        (2, &[Some(50.0), None, Some(150.0)]),
    ]);
    let predicates = vec![greater_than(0, 100.0), less_than(0, 200.0)];
    let mut it = filter(child, Candidates::AllElements, predicates, values);

    assert_eq!(read_all(&mut it), [2]);
}

#[test]
fn all_elements_ignores_the_positions_of_the_child() {
    // The child records element 0, whose value fails; element 1 passes.
    let child = Mock::new_with_positions([1], [1]);
    let values = OneField::new([(1, &[None, Some(90.0), Some(150.0)])]);
    let predicates = vec![greater_than(0, 100.0)];
    let mut it = filter(child, Candidates::AllElements, predicates, values);

    assert_eq!(read_all(&mut it), [1]);
}

#[test]
fn without_predicates_a_document_needs_one_candidate_element() {
    // Document 1: both children match element 1. Document 2: elements 2 and 1.
    let first: Mock<'_, 2> = Mock::new_with_positions([1, 2], [1, 2]);
    let second: Mock<'_, 2> = Mock::new_with_positions([1, 2], [1, 1]);
    let child = Intersection::new(vec![first, second], 1.0, false);
    let values = OneField(HashMap::new());
    let mut it = filter(child, Candidates::CommonPositions, vec![], values);

    assert_eq!(read_all(&mut it), [1]);
}

#[test]
#[should_panic(expected = "the elements of a document are those of the first predicate's field")]
fn all_elements_needs_a_predicate() {
    let child: Mock<'_, 1> = Mock::new([1]);
    let values = OneField::new([(1, &[Some(150.0)])]);
    let _ = filter(child, Candidates::AllElements, vec![], values);
}

/// More candidates than the checker lets pass between two looks at the clock.
const REJECTED: usize = 6_000;
const CLOCK_CHECK_GRANULARITY: u32 = 5_000;

/// A filter whose child names [`REJECTED`] documents, none of which has a value.
fn rejecting_all<'index, TC>(
    timeout_ctx: TC,
) -> ElementFilter<'index, Mock<'index, REJECTED>, OneField, TC> {
    let mut doc_ids = [0; REJECTED];
    for (i, doc_id) in doc_ids.iter_mut().enumerate() {
        *doc_id = i as DocId + 1;
    }
    let child = Mock::new_with_positions(doc_ids, [1; REJECTED]);
    child
        .data()
        .add_delay_since_index(1, std::time::Duration::from_micros(100));
    ElementFilter::new(
        child,
        1.0,
        Candidates::CommonPositions,
        vec![greater_than(0, 100.0)],
        OneField(HashMap::new()),
        timeout_ctx,
    )
}

#[test]
#[cfg_attr(miri, ignore = "Too slow to be run under miri.")]
fn read_over_rejected_candidates_times_out() {
    let mut it = rejecting_all(DeadlineTimeoutChecker::new(
        std::time::Duration::from_micros(50),
        CLOCK_CHECK_GRANULARITY,
    ));

    let result = it.read();
    assert!(
        matches!(result, Err(RQEIteratorError::TimedOut)),
        "expected a timeout, got {result:?}"
    );
}

#[test]
#[cfg_attr(miri, ignore = "Too slow to be run under miri.")]
fn skip_to_over_rejected_candidates_times_out() {
    let mut it = rejecting_all(DeadlineTimeoutChecker::new(
        std::time::Duration::from_micros(50),
        CLOCK_CHECK_GRANULARITY,
    ));

    let result = it.skip_to(1);
    assert!(
        matches!(result, Err(RQEIteratorError::TimedOut)),
        "expected a timeout, got {:?}",
        result.map(|outcome| outcome.is_some())
    );
}

#[test]
#[cfg_attr(miri, ignore = "Too slow to be run under miri.")]
fn read_over_rejected_candidates_without_a_deadline_reaches_eof() {
    let mut it = rejecting_all(NoTimeoutChecker);

    assert!(it.read().expect("read failed").is_none());
}

#[test]
fn yielded_result_has_the_weight_every_field_and_nothing_of_the_child() {
    let child = Mock::new_with_positions([1], [1]);
    let values = OneField::new([(1, &[None, Some(150.0)])]);
    let mut it = ElementFilter::new(
        child,
        3.0,
        Candidates::CommonPositions,
        vec![greater_than(0, 100.0)],
        values,
        NoTimeoutChecker,
    );

    let result = it.read().expect("read failed").expect("document 1");
    assert_eq!(result.doc_id, 1);
    assert_eq!(result.kind(), RSResultKind::Virtual);
    assert_eq!(result.weight, 3.0);
    assert_eq!(result.field_mask, rqe_core::RS_FIELDMASK_ALL);
}

#[test]
fn skip_to_a_passing_document_finds_it() {
    let mut it = above_100();

    match it.skip_to(4).expect("skip failed") {
        Some(SkipToOutcome::Found(result)) => assert_eq!(result.doc_id, 4),
        other => panic!("expected document 4 to be found, got {other:?}"),
    }
    assert_eq!(it.last_doc_id(), 4);
}

#[test]
fn skip_to_a_rejected_document_lands_on_the_next_passing_one() {
    let mut it = above_100();

    match it.skip_to(2).expect("skip failed") {
        Some(SkipToOutcome::NotFound(result)) => assert_eq!(result.doc_id, 4),
        other => panic!("expected to land on document 4, got {other:?}"),
    }
    assert_eq!(it.current().expect("positioned").doc_id, 4);
}

#[test]
fn skip_to_past_the_last_passing_document_reaches_eof() {
    let child = Mock::new_with_positions([1, 2], [1, 1]);
    let values = OneField::new([(1, &[None, Some(150.0)]), (2, &[None, Some(90.0)])]);
    let mut it = common_position_above_100(child, values);

    assert!(it.skip_to(2).expect("skip failed").is_none());
    assert!(it.at_eof());
}

#[test]
fn rewind_yields_the_same_documents_again() {
    let mut it = above_100();
    assert_eq!(read_all(&mut it), [1, 4]);

    it.rewind();

    assert!(!it.at_eof());
    assert_eq!(read_all(&mut it), [1, 4]);
}

#[test]
fn revalidate_that_moves_onto_a_rejected_document_goes_on_to_a_passing_one() {
    let mut it = above_100();
    it.child()
        .data()
        .set_revalidate_result(MockRevalidateResult::Move);
    assert_eq!(it.read().expect("read failed").unwrap().doc_id, 1);

    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    // The child moves to document 2, whose candidate fails the predicate.
    match it.revalidate(&guard).expect("revalidate failed") {
        RQEValidateStatus::Moved {
            current: Some(current),
        } => assert_eq!(current.doc_id, 4),
        other => panic!("expected a move to document 4, got {other:?}"),
    }
}

#[test]
fn revalidate_forwards_an_unmoved_child() {
    let mut it = above_100();
    assert_eq!(it.read().expect("read failed").unwrap().doc_id, 1);

    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    assert_eq!(
        it.revalidate(&guard).expect("revalidate failed"),
        RQEValidateStatus::Ok
    );
    assert_eq!(it.current().expect("positioned").doc_id, 1);
}

#[test]
fn revalidate_forwards_an_aborted_child() {
    let mut it = above_100();
    it.child()
        .data()
        .set_revalidate_result(MockRevalidateResult::Abort);
    assert_eq!(it.read().expect("read failed").unwrap().doc_id, 1);

    let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
    let guard = mock_ctx.spec_read();
    assert_eq!(
        it.revalidate(&guard).expect("revalidate failed"),
        RQEValidateStatus::Aborted
    );
}

#[test]
fn sorting_vector_slot_is_read_as_stored_element_numbers() {
    // What the value shows is not read here.
    let shown = SharedValue::new_num(90.0);
    let stored = ElementNumbers::store(&[0, 2], &[90.0, 150.0], 3, shown);
    let mut sorting_vector = RSSortingVector::new(2);
    sorting_vector
        .try_insert_val(1, stored)
        .expect("slot 1 exists");

    assert_eq!(sorting_vector.num_elements(1), 3);
    assert_eq!(sorting_vector.value(1, 0), Some(90.0));
    assert_eq!(
        sorting_vector.value(1, 1),
        None,
        "an element without a number"
    );
    assert_eq!(sorting_vector.value(1, 2), Some(150.0));
}

#[test]
fn sorting_vector_slot_without_a_value_has_no_elements() {
    let sorting_vector = RSSortingVector::new(1);

    for slot in [0, 1] {
        assert_eq!(sorting_vector.num_elements(slot), 0);
        assert_eq!(sorting_vector.value(slot, 0), None);
    }
}

mod via_resume {
    use super::*;
    use crate::utils::MockIteratorError;
    use rqe_iterators::{RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator, ResumeOutcome};

    /// Read document 1, then make the child resume as `result` says.
    fn parked_on_first_match(
        result: MockRevalidateResult,
    ) -> Box<ElementFilter<'static, Mock<'static, 4>, OneField, NoTimeoutChecker>> {
        let mut it = Box::new(above_100());
        it.child().data().set_revalidate_result(result);
        assert_eq!(it.read().expect("read failed").unwrap().doc_id, 1);
        it
    }

    #[test]
    fn resume_keeps_an_unmoved_child_in_place() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let it = parked_on_first_match(MockRevalidateResult::Ok);
        let address = &raw const *it as usize;

        let mut active = match it.suspend().resume(&guard).expect("resume failed") {
            ResumeOutcome::Ok(active) => active,
            _ => panic!("an unmoved child must resume as Ok"),
        };

        // Results are the child's, so the box must not move for cached pointers to hold.
        assert_eq!(&raw const *active as usize, address);
        assert_eq!(active.current().expect("positioned").doc_id, 1);
        assert_eq!(read_all(&mut *active), [4]);
    }

    #[test]
    fn resume_that_moves_onto_a_rejected_document_goes_on_to_a_passing_one() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let it = parked_on_first_match(MockRevalidateResult::Move);

        // The child moves to document 2, whose candidate fails the predicate.
        let mut active = match it.suspend().resume(&guard).expect("resume failed") {
            ResumeOutcome::Moved(active) => active,
            _ => panic!("a moved child must resume as Moved"),
        };

        assert_eq!(active.current().expect("positioned").doc_id, 4);
    }

    #[test]
    fn resume_forwards_an_aborted_child() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let it = parked_on_first_match(MockRevalidateResult::Abort);

        let outcome = it.suspend().resume(&guard).expect("resume failed");

        assert!(matches!(outcome, ResumeOutcome::Aborted));
    }

    #[test]
    fn resume_forwards_a_child_error() {
        let mock_ctx = rqe_iterators_test_utils::MockContext::new(0, 0);
        let guard = mock_ctx.spec_read();
        let it = parked_on_first_match(MockRevalidateResult::Ok);
        it.child()
            .data()
            .set_error_on_resume(Some(MockIteratorError::TimeoutError(None)));

        let result = it.suspend().resume(&guard);

        assert!(matches!(result, Err(RQEIteratorError::TimedOut)));
    }
}
