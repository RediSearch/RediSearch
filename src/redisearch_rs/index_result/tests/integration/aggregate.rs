/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for [`RawAggregateResult::push_borrowed_ptr_from_ref`], the primitive a
//! suspended composite uses to rebuild its borrowed entries with fresh
//! provenance before it re-narrows them.
//!
//! Run these under miri: the property under test is a Stacked Borrows one, and
//! outside miri a re-narrowed entry with a dead borrow reads back perfectly
//! well.
//!
//! [`RawAggregateResult::push_borrowed_ptr_from_ref`]: index_result::RawAggregateResult::push_borrowed_ptr_from_ref

use index_result::{MetricsVec, RSIndexResult};

/// Hands `child`'s allocation through a by-value [`Box`], reproducing what
/// transitioning a child does to it: the function-entry retag invalidates every
/// borrow taken from that allocation before the call, which is precisely how a
/// composite's aggregate entries lose their provenance across a suspend/resume
/// cycle.
#[inline(never)]
fn retag<T>(child: Box<T>) -> Box<T> {
    child
}

/// Borrows `child` into `parent`'s aggregate the way a composite iterator does:
/// through a raw pointer, because the child is owned by a sibling field and the
/// borrow checker cannot see that it outlives the aggregate.
fn push_borrowed<'a>(
    parent: &mut RSIndexResult<'a>,
    child: &RSIndexResult<'a>,
    metrics: MetricsVec<'a>,
) {
    let child: *const RSIndexResult<'a> = child;
    // SAFETY: every caller below keeps the child alive for as long as `parent`,
    // moving it only through the deliberate `retag` above (which preserves its
    // address).
    let child = unsafe { &*child };
    parent.push_borrowed(child, metrics);
}

/// The property the primitive exists for: after every child has been retagged,
/// rebuilding the entries from those children makes the aggregate readable
/// again — and leaves the metrics alone, which is the part a rebuild cannot put
/// back.
#[test]
fn rebuilt_entries_survive_the_children_being_retagged() {
    let child0 = Box::new(
        RSIndexResult::build_numeric(1.0)
            .doc_id(7)
            .frequency(3)
            .build(),
    );
    let child1 = Box::new(
        RSIndexResult::build_numeric(2.0)
            .doc_id(7)
            .frequency(5)
            .build(),
    );

    let mut parent = RSIndexResult::build_intersect(2).build();
    let mut metrics = MetricsVec::new();
    metrics.push_without_key(0.5);
    push_borrowed(&mut parent, &child0, metrics);
    push_borrowed(&mut parent, &child1, MetricsVec::new());

    let (doc_id, freq, field_mask) = (parent.doc_id, parent.freq, parent.field_mask);
    assert_eq!(freq, 8, "the frequencies of both children accumulated");

    let mut suspended = parent.into_suspended();
    let child0 = retag(child0);
    let child1 = retag(child1);

    let aggregate = suspended
        .as_aggregate_mut()
        .and_then(|agg| agg.as_borrowed_mut())
        .expect("an intersection result carries a borrowed aggregate");
    let kind_mask = aggregate.kind_mask();
    aggregate.reset();
    aggregate.push_borrowed_ptr_from_ref(&child0);
    aggregate.push_borrowed_ptr_from_ref(&child1);
    assert_eq!(
        aggregate.len(),
        2,
        "one entry per child that was pushed back",
    );
    assert_eq!(
        aggregate.kind_mask(),
        kind_mask,
        "`reset` drops the kind mask and the pushes rebuild it",
    );

    // SAFETY: both entries were just written from live children that are still
    // alive and at the same address, and the result holds nothing else that
    // suspension weakened.
    let parent = unsafe { suspended.into_active() };

    assert_eq!(
        parent.get(0).expect("first entry").doc_id,
        7,
        "the first entry reaches its child",
    );
    assert_eq!(
        parent.get(1).expect("second entry").doc_id,
        7,
        "the second entry reaches its child",
    );
    assert_eq!(
        (parent.doc_id, parent.freq, parent.field_mask),
        (doc_id, freq, field_mask),
        "the primitive writes entries; the position and scalars are the \
         caller's to maintain",
    );
    assert_eq!(
        parent.metrics.get(0).map(|m| m.value()),
        Some(0.5),
        "the metrics moved out of the children when the aggregate was first \
         built are still here — `reset_aggregate` would have lost them",
    );
}

/// A rebuild is free to come out shorter than it went in: the entries are
/// whatever the caller pushes, so a child that no longer qualifies simply is not
/// pushed, and no bookkeeping has to recognise its absence.
#[test]
fn a_rebuild_can_drop_a_child_by_not_pushing_it() {
    let survivor = Box::new(RSIndexResult::build_numeric(1.0).doc_id(7).build());
    let departing = Box::new(RSIndexResult::build_numeric(2.0).doc_id(7).build());

    let mut parent = RSIndexResult::build_intersect(2).build();
    push_borrowed(&mut parent, &survivor, MetricsVec::new());
    push_borrowed(&mut parent, &departing, MetricsVec::new());

    let mut suspended = parent.into_suspended();
    let survivor = retag(survivor);
    drop(departing);

    let aggregate = suspended
        .as_aggregate_mut()
        .and_then(|agg| agg.as_borrowed_mut())
        .expect("an intersection result carries a borrowed aggregate");
    aggregate.reset();
    aggregate.push_borrowed_ptr_from_ref(&survivor);
    assert_eq!(
        aggregate.len(),
        1,
        "the departed child left no trace to clean up",
    );

    // SAFETY: the only entry was just written from a live child, and the
    // dropped one was never written back.
    let parent = unsafe { suspended.into_active() };
    assert_eq!(parent.get(0).expect("the surviving entry").doc_id, 7);
    assert!(parent.get(1).is_none(), "nothing stale was left behind");
}

/// An owned aggregate keeps its children in its own allocation, so there is no
/// borrowed payload to push onto — the split between the two aggregate types
/// makes that a compile-time distinction rather than a runtime check, and
/// `as_borrowed_mut` returning `None` is how a rebuild recognises it.
#[test]
fn an_owned_aggregate_has_no_borrowed_payload() {
    let parent = RSIndexResult::build_hybrid_metric().build();
    let mut suspended = parent.into_suspended();
    let aggregate = suspended
        .as_aggregate_mut()
        .expect("a hybrid metric result carries an aggregate");
    assert!(
        aggregate.as_borrowed_mut().is_none(),
        "an owned aggregate has no borrowed payload to push onto",
    );
}
