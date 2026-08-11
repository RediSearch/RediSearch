/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for [`RawAggregateResult::rederive_borrowed`], the primitive a
//! suspended composite uses to give its borrowed entries fresh provenance
//! before it re-narrows them.
//!
//! Run these under miri: the property under test is a Stacked Borrows one, and
//! outside miri a re-narrowed entry with a dead borrow reads back perfectly
//! well.
//!
//! [`RawAggregateResult::rederive_borrowed`]: index_result::RawAggregateResult::rederive_borrowed

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
/// re-deriving the entries makes the aggregate readable again — and leaves
/// everything the entries are *not*, the position and the accumulated scalars
/// and metrics, exactly as it found them.
#[test]
fn rederived_entries_survive_the_children_being_retagged() {
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
        .expect("an intersection result carries an aggregate");
    let rederived = aggregate.rederive_borrowed(&child0) + aggregate.rederive_borrowed(&child1);
    assert_eq!(
        rederived,
        aggregate.num_borrowed(),
        "every entry has a live child to be re-derived from",
    );

    // SAFETY: both entries were just re-derived from live children that are
    // still alive and at the same address, and the result holds nothing else
    // that suspension weakened.
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
        "re-derivation touches the entries and nothing else",
    );
    assert_eq!(
        parent.metrics.get(0).map(|m| m.value()),
        Some(0.5),
        "the metrics moved out of the children when the aggregate was first \
         built are still here — a rebuild would have lost them",
    );
}

/// An entry whose child is gone cannot be re-derived, and the shortfall against
/// [`num_borrowed`] is how the caller finds out. Clearing the aggregate is then
/// the only sound option, and it is available on this side of the cast because
/// nothing here ever dereferences an entry.
///
/// [`num_borrowed`]: index_result::RawAggregateResult::num_borrowed
#[test]
fn a_dropped_child_leaves_a_shortfall_the_caller_can_see() {
    let child = Box::new(RSIndexResult::build_numeric(1.0).doc_id(7).build());
    let survivor = Box::new(RSIndexResult::build_numeric(2.0).doc_id(9).build());

    let mut parent = RSIndexResult::build_union(1).build();
    push_borrowed(&mut parent, &child, MetricsVec::new());

    let mut suspended = parent.into_suspended();
    // The child a resume dropped, because its own resume aborted.
    drop(child);

    let aggregate = suspended
        .as_aggregate_mut()
        .expect("a union result carries an aggregate");
    assert_eq!(
        aggregate.rederive_borrowed(&survivor),
        0,
        "no entry points at the surviving child",
    );
    assert_eq!(
        aggregate.num_borrowed(),
        1,
        "the entry is still there, and still un-derivable",
    );

    suspended.reset_aggregate();
    assert_eq!(
        suspended
            .as_aggregate()
            .expect("still an aggregate")
            .num_borrowed(),
        0,
        "clearing leaves nothing for the cast to re-narrow",
    );
}

/// An owned aggregate keeps its children in its own allocation, so suspension
/// never weakens them and there is nothing to re-derive.
#[test]
fn an_owned_aggregate_borrows_nothing() {
    let stranger = RSIndexResult::build_numeric(2.0).doc_id(9).build();

    let mut parent = RSIndexResult::build_hybrid_metric().build();
    parent.push_boxed(Box::new(RSIndexResult::build_metric(1.0).doc_id(7).build()));
    assert_eq!(
        parent
            .as_aggregate()
            .expect("a hybrid metric result carries an aggregate")
            .num_borrowed(),
        0,
    );

    let mut suspended = parent.into_suspended();
    let aggregate = suspended.as_aggregate_mut().expect("still an aggregate");
    assert_eq!(aggregate.rederive_borrowed(&stranger), 0);
    assert_eq!(aggregate.len(), 1, "the owned child is left alone");
}
