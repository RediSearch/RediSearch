/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for [`rederive_aggregate_entries`] and [`clear_aggregate_entries`],
//! the resume-shaped wrapper a composite calls between transitioning its
//! children and reinterpreting its own allocation.
//!
//! `index_result`'s own suite covers the pointer-level primitive underneath;
//! what is under test here is the wrapper's *decision*: which children are
//! allowed to answer for an entry, what it reports back, and what survives the
//! path where nothing can.
//!
//! Worth running under miri, for the same reason those tests are: the property
//! the re-derivation exists for is a Stacked Borrows one, and outside miri an
//! entry with dead provenance reads back perfectly well.
//!
//! One case is deliberately absent here: offering the same child twice, which a
//! running total would let cover for an entry no child answers for. The wrapper
//! takes each child as a `&mut`, so no caller can express it; the property is
//! pinned one layer down instead, in `index_result`'s
//! `a_child_offered_twice_cannot_cover_for_an_unanswered_entry`.

use index_result::{MetricsVec, RSIndexResult, SuspendedIndexResult};
use rqe_iterators::{
    RQEIterator,
    boxed::{RederiveOutcome, clear_aggregate_entries, rederive_aggregate_entries},
};

use crate::utils::Mock;

/// The document every fixture below is built on.
const DOC_ID: u64 = 10;

/// Hands `child`'s allocation through a by-value [`Box`], reproducing what
/// transitioning a child does to it: the function-entry retag invalidates every
/// borrow taken from that allocation beforehand, which is precisely how a
/// composite's aggregate entries lose their provenance across a suspend/resume
/// cycle.
#[inline(never)]
fn retag<T>(child: Box<T>) -> Box<T> {
    child
}

/// A child positioned on [`DOC_ID`], boxed so that [`retag`] can reach it, and
/// carrying `freq` so the scalars the aggregate accumulates are non-trivial.
fn child_on_doc<'index, const N: usize>(doc_ids: [u64; N], freq: u32) -> Box<Mock<'index, N>> {
    let mut child = Box::new(Mock::new(doc_ids));
    let current = child
        .read()
        .expect("the mock reads without error")
        .expect("a mock over a non-empty list serves its first document");
    assert_eq!(
        current.doc_id, DOC_ID,
        "every fixture starts on the same document"
    );
    current.freq = freq;
    child
}

/// Borrows `child` into `parent`'s aggregate the way a composite iterator does:
/// through a raw pointer, because the child is owned by a sibling field and the
/// borrow checker cannot see that it outlives the aggregate.
fn push_borrowed<'a>(parent: &mut RSIndexResult<'a>, child: &RSIndexResult<'a>, metric: f64) {
    let mut metrics = MetricsVec::new();
    metrics.push_without_key(metric);

    let child: *const RSIndexResult<'a> = child;
    // SAFETY: every caller below keeps the child alive, and at the same address,
    // for as long as the parent.
    let child = unsafe { &*child };
    parent.push_borrowed(child, metrics);
}

/// The scalars a resume must hand back untouched whatever it decides about the
/// entries: they are the composite's own accumulated state, not the children's.
#[derive(Debug, PartialEq)]
struct Scalars {
    doc_id: u64,
    freq: u32,
    field_mask: inverted_index::FieldMask,
    metrics: Vec<f64>,
}

impl Scalars {
    fn of(result: &SuspendedIndexResult<'_>) -> Self {
        Self {
            doc_id: result.doc_id,
            freq: result.freq,
            field_mask: result.field_mask,
            metrics: result.metrics.iter().map(|m| m.value()).collect(),
        }
    }
}

/// A union result over two children sitting on [`DOC_ID`], suspended and ready
/// for the wrapper — the shape every test starts from.
fn suspended_union<'index>(
    child0: &mut Mock<'index, 2>,
    child1: &mut Mock<'index, 2>,
) -> SuspendedIndexResult<'index> {
    let mut parent = RSIndexResult::build_union(2).build();
    push_borrowed(&mut parent, child0.current().expect("positioned"), 0.25);
    push_borrowed(&mut parent, child1.current().expect("positioned"), 0.75);
    assert_eq!(parent.doc_id, DOC_ID);
    parent.into_suspended()
}

/// The happy path: every entry has a live child on the aggregate's document, so
/// the entries are re-derived, the wrapper reports as much, and the result is
/// readable again through the cast the wrapper exists to license.
#[test]
fn every_entry_is_rederived_from_its_child() {
    let mut child0 = child_on_doc([DOC_ID, 30], 3);
    let mut child1 = child_on_doc([DOC_ID, 40], 5);

    let mut suspended = suspended_union(&mut child0, &mut child1);
    let before = Scalars::of(&suspended);
    assert_eq!(
        before.freq, 8,
        "the frequencies of both children accumulated"
    );

    let mut child0 = retag(child0);
    let mut child1 = retag(child1);

    assert_eq!(
        rederive_aggregate_entries(&mut suspended, [&mut *child0, &mut *child1]),
        RederiveOutcome::Rederived,
    );
    assert_eq!(
        Scalars::of(&suspended),
        before,
        "re-derivation touches the entries and nothing else",
    );

    // SAFETY: both entries were just re-derived from live children that are
    // still alive and at the same address, and the result holds nothing else
    // that suspension weakened.
    let parent = unsafe { suspended.into_active() };
    assert_eq!(
        (
            parent.get(0).expect("first entry").doc_id,
            parent.get(1).expect("second entry").doc_id,
        ),
        (DOC_ID, DOC_ID),
        "both entries reach their child",
    );
}

/// A result with no borrowed entries is the early return: it reports success
/// without consulting a child, and a child at EOF among them cannot drag it
/// into the clearing path.
#[test]
fn an_aggregate_that_borrows_nothing_needs_no_child() {
    let mut child = child_on_doc([DOC_ID, 30], 3);
    // Run it past its last document, so `current()` answers `None`. Were the
    // children consulted at all, that would be a shortfall.
    while child
        .read()
        .expect("the mock reads without error")
        .is_some()
    {}
    assert!(child.current().is_none(), "the mock is past its end");

    let mut suspended = RSIndexResult::build_union(2)
        .doc_id(DOC_ID)
        .build()
        .into_suspended();
    let before = Scalars::of(&suspended);

    assert_eq!(
        rederive_aggregate_entries(&mut suspended, [&mut *child]),
        RederiveOutcome::Rederived,
    );
    assert_eq!(Scalars::of(&suspended), before);
}

/// A result that is not an aggregate borrows nothing either, and takes the same
/// early return.
#[test]
fn a_non_aggregate_result_is_left_alone() {
    let mut child = child_on_doc([DOC_ID, 30], 3);

    let mut suspended = RSIndexResult::build_virt()
        .doc_id(DOC_ID)
        .build()
        .into_suspended();
    let before = Scalars::of(&suspended);

    assert_eq!(
        rederive_aggregate_entries(&mut suspended, [&mut *child]),
        RederiveOutcome::Rederived,
    );
    assert_eq!(Scalars::of(&suspended), before);
}

/// A child that has run past its end hides its result, so nothing can answer
/// for the entry it left behind. The whole aggregate goes — and nothing else
/// does: the composite's own position and accumulated scalars are what it hands
/// back if it decides it can still publish anything at all.
#[test]
fn a_child_that_stopped_publishing_a_current_clears_the_aggregate() {
    let mut child0 = child_on_doc([DOC_ID, 30], 3);
    let mut child1 = child_on_doc([DOC_ID, 40], 5);

    let mut suspended = suspended_union(&mut child0, &mut child1);
    let before = Scalars::of(&suspended);

    let mut child0 = retag(child0);
    let mut child1 = retag(child1);
    // `child1` runs out where `child0` does not: one live, on-document child is
    // not enough, which is the whole point of deciding per entry.
    while child1
        .read()
        .expect("the mock reads without error")
        .is_some()
    {}
    assert!(child1.current().is_none(), "the mock is past its end");

    assert_eq!(
        rederive_aggregate_entries(&mut suspended, [&mut *child0, &mut *child1]),
        RederiveOutcome::Cleared,
    );
    assert_eq!(
        suspended
            .as_aggregate()
            .expect("still an aggregate")
            .num_borrowed(),
        0,
        "clearing leaves nothing for the cast to re-narrow",
    );
    assert_eq!(
        Scalars::of(&suspended),
        before,
        "the position, the frequency, the field mask and the metrics are the \
         composite's own state — only the entries were unusable",
    );
}

/// A child whose own resume carried it onto a later document still sits at the
/// same address, so matching on address alone would keep an entry that no
/// longer belongs to the document the aggregate describes. The legacy
/// `revalidate` rebuilds the result in that situation; here it is cleared, and
/// the caller is told so it can do the same.
#[test]
fn a_child_that_moved_off_the_document_clears_the_aggregate() {
    let mut child0 = child_on_doc([DOC_ID, 30], 3);
    let mut child1 = child_on_doc([DOC_ID, 40], 5);

    let mut suspended = suspended_union(&mut child0, &mut child1);
    let before = Scalars::of(&suspended);

    let mut child0 = retag(child0);
    let mut child1 = retag(child1);
    let moved = child1
        .read()
        .expect("the mock reads without error")
        .expect("a second document");
    assert_eq!(moved.doc_id, 40, "the child left the aggregate's document");

    assert_eq!(
        rederive_aggregate_entries(&mut suspended, [&mut *child0, &mut *child1]),
        RederiveOutcome::Cleared,
    );
    assert_eq!(
        suspended
            .as_aggregate()
            .expect("still an aggregate")
            .num_borrowed(),
        0,
    );
    assert_eq!(Scalars::of(&suspended), before);
}

/// The composites that compact their child slots call this directly, having
/// already established that an address no longer identifies the child it was
/// taken from. It must cost them exactly the entries — a `reset_aggregate`
/// here would take the metrics with it, and a KNN+text union would lose
/// `__vector_score` from its reply.
#[test]
fn clearing_costs_the_entries_and_nothing_else() {
    let mut child0 = child_on_doc([DOC_ID, 30], 3);
    let mut child1 = child_on_doc([DOC_ID, 40], 5);

    let mut suspended = suspended_union(&mut child0, &mut child1);
    let before = Scalars::of(&suspended);
    assert_eq!(
        before.metrics,
        vec![0.25, 0.75],
        "the metrics moved out of the children when the aggregate was built",
    );
    assert_ne!(before.field_mask, 0);

    assert_eq!(
        clear_aggregate_entries(&mut suspended),
        RederiveOutcome::Cleared
    );

    assert_eq!(
        suspended
            .as_aggregate()
            .expect("still an aggregate")
            .num_borrowed(),
        0,
    );
    assert_eq!(Scalars::of(&suspended), before);
}
