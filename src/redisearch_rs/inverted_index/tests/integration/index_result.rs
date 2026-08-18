/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use index_result::{
    MetricsVec, RSBorrowedAggregateResult, RSIndexResult, RSOffsetSlice, RSOffsetVector,
    RSOwnedAggregateResult, RSResultKind, RSResultKindMask,
};
use query_term::RSQueryTerm;
use rqe_core::RS_FIELDMASK_ALL;

#[test]
fn pushing_to_aggregate_result() {
    let num_first = RSIndexResult::build_numeric(10.0).doc_id(2).build();
    let num_second = RSIndexResult::build_numeric(100.0).doc_id(3).build();
    let virt_first = RSIndexResult::build_virt().doc_id(4).build();

    let mut agg = RSBorrowedAggregateResult::with_capacity(2);

    assert_eq!(agg.kind_mask(), RSResultKindMask::empty());

    agg.push_borrowed(&num_first);

    assert_eq!(
        agg.kind_mask(),
        RSResultKind::Numeric,
        "type mask should be ORed"
    );

    assert_eq!(
        agg.get(0),
        Some(&RSIndexResult::build_numeric(10.0).doc_id(2).build())
    );
    assert_eq!(agg.get(1), None, "This record does not exist yet");

    agg.push_borrowed(&num_second);

    assert_eq!(agg.kind_mask(), RSResultKind::Numeric);

    assert_eq!(
        agg.get(0),
        Some(&RSIndexResult::build_numeric(10.0).doc_id(2).build())
    );
    assert_eq!(
        agg.get(1),
        Some(&RSIndexResult::build_numeric(100.0).doc_id(3).build())
    );
    assert_eq!(agg.get(2), None, "This record does not exist yet");

    agg.push_borrowed(&virt_first);

    assert_eq!(
        agg.kind_mask(),
        RSResultKind::Numeric | RSResultKind::Virtual,
        "types should be combined"
    );

    assert_eq!(
        agg.get(0),
        Some(&RSIndexResult::build_numeric(10.0).doc_id(2).build())
    );
    assert_eq!(
        agg.get(1),
        Some(&RSIndexResult::build_numeric(100.0).doc_id(3).build())
    );
    assert_eq!(
        agg.get(2),
        Some(&RSIndexResult::build_virt().doc_id(4).build())
    );
    assert_eq!(agg.get(3), None, "This record does not exist yet");
}

#[test]
fn pushing_to_index_result() {
    let result_virt = RSIndexResult::build_virt()
        .doc_id(2)
        .frequency(3)
        .field_mask(4)
        .build();
    let result_with_frequency = RSIndexResult::build_numeric(5.0)
        .doc_id(2)
        .frequency(7)
        .build();

    let mut ir = RSIndexResult::build_union(1).doc_id(1).weight(1.0).build();

    assert_eq!(ir.doc_id, 1);
    assert_eq!(ir.kind(), RSResultKind::Union);
    assert_eq!(ir.weight, 1.0);
    assert_eq!(ir.freq, 0);
    assert_eq!(ir.field_mask, 0);

    ir.push_borrowed(&result_virt, MetricsVec::new());
    assert_eq!(ir.doc_id, 2, "should inherit doc id of the child");
    assert_eq!(ir.kind(), RSResultKind::Union);
    assert_eq!(ir.weight, 1.0);
    assert_eq!(ir.freq, 3, "frequency should accumulate");
    assert_eq!(ir.field_mask, 4, "field mask should be ORed");
    assert_eq!(
        ir.get(0),
        Some(
            &RSIndexResult::build_virt()
                .doc_id(2)
                .frequency(3)
                .field_mask(4)
                .build()
        )
    );

    ir.push_borrowed(&result_with_frequency, MetricsVec::new());
    assert_eq!(ir.doc_id, 2);
    assert_eq!(ir.kind(), RSResultKind::Union);
    assert_eq!(ir.weight, 1.0);
    assert_eq!(ir.freq, 10, "frequency should accumulate");
    assert_eq!(ir.field_mask, RS_FIELDMASK_ALL);
}

#[test]
fn to_owned_an_aggregate_index_result() {
    let num_rec = RSIndexResult::build_numeric(5.0).doc_id(10).build();
    let mut ir = RSIndexResult::build_intersect(5)
        .doc_id(10)
        .weight(3.0)
        .build();

    ir.push_borrowed(&num_rec, MetricsVec::new());

    let mut ir_copy = ir.to_owned();

    assert_eq!(ir.doc_id, ir_copy.doc_id);
    assert_eq!(ir.dmd, ir_copy.dmd);
    assert_eq!(ir.field_mask, ir_copy.field_mask);
    assert_eq!(ir.freq, ir_copy.freq);

    let agg = ir.as_aggregate().unwrap();
    let agg_copy = ir_copy.as_aggregate().unwrap();
    assert_eq!(agg.kind_mask(), agg_copy.kind_mask());
    assert_eq!(
        agg_copy.capacity(),
        1,
        "should use as minimal capacity as needed"
    );
    assert_eq!(ir.metrics, ir_copy.metrics);
    assert_eq!(ir.weight, ir_copy.weight);
    assert!(ir_copy.is_copy());

    // Make sure the inner value was cloned too
    {
        let ir_first = ir.get(0).unwrap();
        let ir_clone_first = ir_copy.get(0).unwrap();

        assert_eq!(ir_first.doc_id, ir_clone_first.doc_id);
        assert_eq!(ir_first.dmd, ir_clone_first.dmd);
        assert_eq!(ir_first.field_mask, ir_clone_first.field_mask);
        assert_eq!(ir_first.freq, ir_clone_first.freq);
        ir_first.assert_data(ir_clone_first);
        assert_eq!(ir_first.metrics, ir_clone_first.metrics);
        assert_eq!(ir_first.weight, ir_clone_first.weight);
    }

    // Make sure the inner types are different
    *ir_copy.get_mut(0).unwrap().as_numeric_mut().unwrap() = 1.0;
    assert_eq!(
        ir.get(0).unwrap().as_numeric().unwrap(),
        5.0,
        "cloned value should not have changed"
    )
}

/// Which payload an aggregate answers for: the operations only one kind supports
/// hang off that payload, so this is what decides whether they are reachable at
/// all.
#[test]
fn an_aggregate_answers_for_exactly_one_payload() {
    let mut borrowed = RSIndexResult::build_union(1).build();
    let agg = borrowed
        .as_aggregate_mut()
        .expect("a union result carries an aggregate");
    assert!(agg.as_borrowed().is_some());
    assert!(agg.as_owned().is_none());
    assert!(agg.as_borrowed_mut().is_some());
    assert!(agg.as_owned_mut().is_none());

    let mut owned = RSIndexResult::build_hybrid_metric().build();
    let agg = owned
        .as_aggregate_mut()
        .expect("a hybrid metric result carries an aggregate");
    assert!(agg.as_owned().is_some());
    assert!(agg.as_borrowed().is_none());
    assert!(agg.as_owned_mut().is_some());
    assert!(agg.as_borrowed_mut().is_none());
}

/// `into_records` hands every child to the caller, so dropping what it returns is
/// what frees them. Nothing else exercises that: its only production caller
/// deliberately `mem::forget`s each child to leave the memory with C, so a
/// double-free or a leak on this path would go unobserved. Run under miri.
#[test]
fn into_records_yields_the_children_in_order_and_frees_them_on_drop() {
    let mut owned = RSOwnedAggregateResult::with_capacity(2);
    owned.push_boxed(Box::new(
        RSIndexResult::build_numeric(1.0).doc_id(7).build(),
    ));
    owned.push_boxed(Box::new(
        RSIndexResult::build_numeric(2.0).doc_id(11).build(),
    ));

    let records = owned.into_records();

    assert_eq!(records.len(), 2);
    assert_eq!(
        records.iter().map(|c| c.doc_id).collect::<Vec<_>>(),
        vec![7, 11],
        "the children come back in the order they were pushed",
    );

    // The drop at the end of scope is the point of the test: these boxes are the
    // aggregate's only owners now, so miri sees either a clean free here or a leak.
}

/// An owned aggregate frees its children, so taking a borrowed one would hand it a
/// pointer it must not free.
#[test]
#[should_panic = "Cannot push a borrowed child to an owned aggregate result"]
fn pushing_a_borrowed_child_to_an_owned_aggregate_panics() {
    let child = RSIndexResult::build_metric(1.0).doc_id(7).build();
    let mut owned = RSIndexResult::build_hybrid_metric().build();

    owned.push_borrowed(&child, MetricsVec::new());
}

/// The mirror image: a borrowed aggregate never frees its children, so taking
/// ownership of one would leak it.
#[test]
#[should_panic = "Cannot push an owned child to a borrowed aggregate result"]
fn pushing_an_owned_child_to_a_borrowed_aggregate_panics() {
    let mut borrowed = RSIndexResult::build_union(1).build();

    borrowed.push_boxed(Box::new(
        RSIndexResult::build_numeric(1.0).doc_id(7).build(),
    ));
}

/// A borrowed child is shared with whoever owns it, so a `&mut` to it would alias.
#[test]
#[should_panic = "Cannot get a mutable reference to a borrowed aggregate result"]
fn mutably_reaching_a_borrowed_child_panics() {
    let child = RSIndexResult::build_numeric(1.0).doc_id(7).build();
    let mut borrowed = RSIndexResult::build_union(1).build();
    borrowed.push_borrowed(&child, MetricsVec::new());

    let _ = borrowed.get_mut(0);
}

/// The observers a borrowed payload answers with, and what `reset` leaves behind.
/// A borrowed aggregate never owned its children, so resetting it must drop none
/// of them — only the pointers go. Run under miri, where dropping one here would
/// turn into a double free at the end of scope.
#[test]
fn a_borrowed_aggregate_reports_its_children_and_drops_none_on_reset() {
    let first = RSIndexResult::build_numeric(10.0).doc_id(2).build();
    let second = RSIndexResult::build_virt().doc_id(4).build();

    let mut agg = RSBorrowedAggregateResult::with_capacity(2);
    assert!(agg.is_empty());
    assert_eq!(agg.capacity(), 2);
    assert!(agg.records().is_empty());

    agg.push_borrowed(&first);
    agg.push_borrowed(&second);

    assert!(!agg.is_empty());
    assert_eq!(
        agg.records()
            .iter()
            .map(|r| r.get().doc_id)
            .collect::<Vec<_>>(),
        vec![2, 4],
        "`records` exposes the children in push order"
    );

    agg.reset();

    assert!(agg.is_empty());
    assert_eq!(agg.kind_mask(), RSResultKindMask::empty());
    assert_eq!(agg.capacity(), 2, "reset keeps the allocation");
    assert_eq!(
        (first.doc_id, second.doc_id),
        (2, 4),
        "the children outlive the aggregate that pointed at them"
    );
}

/// The mirror image on the owned payload, whose `reset` *is* what frees the
/// children — it is their only owner. Run under miri.
#[test]
fn an_owned_aggregate_reports_its_children_and_frees_them_on_reset() {
    let mut agg = RSOwnedAggregateResult::with_capacity(2);
    assert!(agg.is_empty());
    assert!(agg.records().is_empty());

    agg.push_boxed(Box::new(
        RSIndexResult::build_numeric(1.0).doc_id(7).build(),
    ));
    agg.push_boxed(Box::new(RSIndexResult::build_virt().doc_id(11).build()));

    assert!(!agg.is_empty());
    assert_eq!(
        agg.records().iter().map(|r| r.doc_id).collect::<Vec<_>>(),
        vec![7, 11],
        "`records` exposes the children in push order"
    );
    assert_eq!(
        agg.kind_mask(),
        RSResultKind::Numeric | RSResultKind::Virtual
    );

    agg.reset();

    assert!(agg.is_empty());
    assert_eq!(agg.kind_mask(), RSResultKindMask::empty());
    assert_eq!(agg.capacity(), 2, "reset keeps the allocation");
}

/// `get_unchecked` is the bounds-check-free twin of `get`. Each payload has its
/// own, and the enum a third that dispatches to them, so all three are distinct
/// code paths that must agree with `get`.
#[test]
fn get_unchecked_agrees_with_get_on_both_payloads() {
    let child = RSIndexResult::build_numeric(10.0).doc_id(2).build();

    let mut borrowed = RSBorrowedAggregateResult::with_capacity(1);
    borrowed.push_borrowed(&child);
    // SAFETY: index 0 is in bounds, one child was just pushed.
    assert_eq!(
        unsafe { borrowed.get_unchecked(0) },
        borrowed.get(0).unwrap()
    );

    let mut owned = RSOwnedAggregateResult::with_capacity(1);
    owned.push_boxed(Box::new(
        RSIndexResult::build_numeric(10.0).doc_id(2).build(),
    ));
    // SAFETY: index 0 is in bounds, one child was just pushed.
    assert_eq!(unsafe { owned.get_unchecked(0) }, owned.get(0).unwrap());

    let mut union = RSIndexResult::build_union(1).build();
    union.push_borrowed(&child, MetricsVec::new());
    let agg = union.as_aggregate().unwrap();
    // SAFETY: index 0 is in bounds, one child was just pushed.
    assert_eq!(unsafe { agg.get_unchecked(0) }, agg.get(0).unwrap());

    let mut hybrid = RSIndexResult::build_hybrid_metric().build();
    hybrid.push_boxed(Box::new(
        RSIndexResult::build_metric(10.0).doc_id(2).build(),
    ));
    let agg = hybrid.as_aggregate().unwrap();
    // SAFETY: index 0 is in bounds, one child was just pushed.
    assert_eq!(unsafe { agg.get_unchecked(0) }, agg.get(0).unwrap());
}

/// The unchecked twin of `get_mut`, which only the owned payload has: a borrowed
/// child is shared with whoever owns it, so handing out a `&mut` to one would
/// alias.
#[test]
fn get_mut_unchecked_reaches_an_owned_child() {
    let mut agg = RSOwnedAggregateResult::with_capacity(1);
    agg.push_boxed(Box::new(
        RSIndexResult::build_numeric(1.0).doc_id(7).build(),
    ));

    // SAFETY: index 0 is in bounds, one child was just pushed.
    let child = unsafe { agg.get_mut_unchecked(0) };
    *child.as_numeric_mut().unwrap() = 42.0;

    assert_eq!(agg.get(0).unwrap().as_numeric(), Some(42.0));
}

/// `to_owned` on an aggregate that already owns its children still has to copy
/// them: sharing them with the copy would give two owners of one allocation.
#[test]
fn to_owned_on_an_owned_aggregate_copies_its_children() {
    let mut agg = RSOwnedAggregateResult::with_capacity(1);
    agg.push_boxed(Box::new(
        RSIndexResult::build_numeric(5.0).doc_id(10).build(),
    ));

    let mut copy = agg.to_owned();
    assert_eq!(copy, agg);
    assert_eq!(
        copy.capacity(),
        1,
        "should use as minimal capacity as needed"
    );

    *copy.get_mut(0).unwrap().as_numeric_mut().unwrap() = 1.0;

    assert_eq!(
        agg.get(0).unwrap().as_numeric(),
        Some(5.0),
        "the original is untouched"
    );
    assert_ne!(copy, agg);
}

/// Borrowed payloads compare by what they point at rather than by the pointers
/// themselves, so two aggregates holding distinct but equal children are equal.
#[test]
fn borrowed_aggregates_compare_by_the_children_they_point_at() {
    let child = RSIndexResult::build_numeric(10.0).doc_id(2).build();
    let same_child = RSIndexResult::build_numeric(10.0).doc_id(2).build();
    let other_child = RSIndexResult::build_numeric(10.0).doc_id(3).build();

    let mut agg = RSBorrowedAggregateResult::with_capacity(1);
    agg.push_borrowed(&child);
    let mut twin = RSBorrowedAggregateResult::with_capacity(1);
    twin.push_borrowed(&same_child);
    let mut different = RSBorrowedAggregateResult::with_capacity(1);
    different.push_borrowed(&other_child);
    let empty = RSBorrowedAggregateResult::with_capacity(1);

    assert_eq!(agg, twin, "distinct children, but equal ones");
    assert_ne!(agg, different);
    assert_ne!(agg, empty, "a length mismatch is enough");
}

/// The two payloads are different types, so an aggregate is never equal to one of
/// the other kind — however alike the children it holds.
#[test]
fn a_borrowed_and_an_owned_aggregate_are_never_equal() {
    let child = RSIndexResult::build_metric(10.0).doc_id(2).build();
    let mut union = RSIndexResult::build_union(1).build();
    union.push_borrowed(&child, MetricsVec::new());

    let mut hybrid = RSIndexResult::build_hybrid_metric().build();
    hybrid.push_boxed(Box::new(
        RSIndexResult::build_metric(10.0).doc_id(2).build(),
    ));

    let borrowed = union.as_aggregate().unwrap();
    let owned = hybrid.as_aggregate().unwrap();

    assert_eq!(borrowed.get(0), owned.get(0), "the same child either way");
    assert_ne!(borrowed, owned, "but not the same aggregate");
}

/// The enum forwards every shared observer to whichever payload it holds. The two
/// arms are separate code paths; this is the borrowed one.
#[test]
fn a_borrowed_backed_enum_forwards_to_its_payload() {
    let child = RSIndexResult::build_numeric(10.0).doc_id(2).build();
    let mut union = RSIndexResult::build_union(3).build();

    assert!(union.as_aggregate().unwrap().is_empty());

    union.push_borrowed(&child, MetricsVec::new());

    let agg = union.as_aggregate().unwrap();
    assert!(!agg.is_empty());
    assert_eq!(agg.capacity(), 3);
    assert_eq!(agg.iter().collect::<Vec<_>>(), vec![&child]);

    let mut twin = RSIndexResult::build_union(3).build();
    twin.push_borrowed(&child, MetricsVec::new());
    assert_eq!(agg, twin.as_aggregate().unwrap());

    let copy = agg.to_owned();
    assert!(
        copy.as_owned().is_some(),
        "`to_owned` yields an owned aggregate whichever kind it started from"
    );
    assert_eq!(copy.get(0), Some(&child));
}

/// The same, for the owned arm.
#[test]
fn an_owned_backed_enum_forwards_to_its_payload() {
    let mut hybrid = RSIndexResult::build_hybrid_metric().build();
    hybrid.push_boxed(Box::new(
        RSIndexResult::build_metric(10.0).doc_id(2).build(),
    ));
    let expected = RSIndexResult::build_metric(10.0).doc_id(2).build();

    let agg = hybrid.as_aggregate().unwrap();
    assert!(!agg.is_empty());
    assert_eq!(agg.iter().collect::<Vec<_>>(), vec![&expected]);

    let copy = agg.to_owned();
    assert_eq!(&copy, agg);
    assert_eq!(copy.get(0), Some(&expected));
}

/// `reset` through the enum reaches both payloads, which differ in what becomes
/// of the children: the borrowed one forgets them, the owned one frees them.
#[test]
fn resetting_through_the_enum_reaches_both_payloads() {
    let child = RSIndexResult::build_numeric(10.0).doc_id(2).build();
    let mut union = RSIndexResult::build_union(1).build();
    union.push_borrowed(&child, MetricsVec::new());

    let agg = union.as_aggregate_mut().unwrap();
    agg.reset();
    assert!(agg.is_empty());
    assert_eq!(agg.kind_mask(), RSResultKindMask::empty());

    let mut hybrid = RSIndexResult::build_hybrid_metric().build();
    hybrid.push_boxed(Box::new(
        RSIndexResult::build_metric(10.0).doc_id(2).build(),
    ));

    let agg = hybrid.as_aggregate_mut().unwrap();
    agg.reset();
    assert!(agg.is_empty());
    assert_eq!(agg.kind_mask(), RSResultKindMask::empty());
}

#[test]
fn to_owned_a_numeric_index_result() {
    let ir = RSIndexResult::build_numeric(8.0).doc_id(3).build();
    let mut ir_copy = ir.to_owned();

    assert_eq!(ir.doc_id, ir_copy.doc_id);
    assert_eq!(ir.dmd, ir_copy.dmd);
    assert_eq!(ir.field_mask, ir_copy.field_mask);
    assert_eq!(ir.freq, ir_copy.freq);
    ir.assert_data(&ir_copy);
    assert_eq!(ir.metrics, ir_copy.metrics);
    assert_eq!(ir.weight, ir_copy.weight);

    // Make sure the values are not linked
    *ir_copy.as_numeric_mut().unwrap() = 1.0;

    assert_eq!(
        ir.as_numeric().unwrap(),
        8.0,
        "cloned value should not have changed"
    );
}

#[test]
fn to_owned_a_virtual_index_result() {
    let ir = RSIndexResult::build_virt()
        .doc_id(8)
        .field_mask(4)
        .weight(2.0)
        .build();
    let ir_copy = ir.to_owned();

    assert_eq!(ir.doc_id, ir_copy.doc_id);
    assert_eq!(ir.dmd, ir_copy.dmd);
    assert_eq!(ir.field_mask, ir_copy.field_mask);
    assert_eq!(ir.freq, ir_copy.freq);
    ir.assert_data(&ir_copy);
    assert_eq!(ir.metrics, ir_copy.metrics);
    assert_eq!(ir.weight, ir_copy.weight);
}

#[test]
fn to_owned_a_term_index_result() {
    let mut term = RSQueryTerm::new("test_term", 2, 3);
    term.set_bm25_idf(4.0);
    term.set_idf(1.0);

    let offsets: [u8; 1] = [0];
    let offsets = RSOffsetSlice::from_slice(&offsets);

    let ir = RSIndexResult::build_term()
        .borrowed_record(Some(term), offsets)
        .doc_id(7)
        .field_mask(1)
        .frequency(1)
        .build();
    let mut ir_copy = ir.to_owned();

    assert_eq!(ir.doc_id, ir_copy.doc_id);
    assert_eq!(ir.dmd, ir_copy.dmd);
    assert_eq!(ir.field_mask, ir_copy.field_mask);
    assert_eq!(ir.freq, ir_copy.freq);
    assert_eq!(
        ir.as_term().unwrap().offsets(),
        ir_copy.as_term().unwrap().offsets()
    );
    assert_eq!(
        ir.as_term().unwrap().query_term(),
        ir_copy.as_term().unwrap().query_term()
    );
    assert_eq!(ir.metrics, ir_copy.metrics);
    assert_eq!(ir.weight, ir_copy.weight);

    // Make sure the values are not linked
    ir_copy
        .as_term_mut()
        .expect("expected term record")
        .set_offsets(RSOffsetSlice::empty());

    assert_eq!(
        ir.as_term().unwrap().offsets().len(),
        1,
        "cloned offsets should not have changed"
    );
}

// ── is_within_range — trivial paths ──────────────────────────────────────

#[test]
fn non_aggregate_always_true() {
    // A term result (not an aggregate) → trivially within range.
    static BYTES: [u8; 1] = [5];
    let ir = RSIndexResult::build_term()
        .borrowed_record(None, RSOffsetSlice::from_slice(&BYTES))
        .doc_id(1)
        .build();
    assert!(ir.is_within_range(Some(0), false));
    assert!(ir.is_within_range(Some(0), true));
}

#[test]
fn single_child_aggregate_always_true() {
    // An intersection with a single numeric child — no proximity check needed.
    let child = RSIndexResult::build_numeric(1.0).doc_id(1).build();
    let mut ir = RSIndexResult::build_intersect(1).build();
    ir.push_borrowed(&child, MetricsVec::new());
    assert!(ir.is_within_range(Some(0), false));
    assert!(ir.is_within_range(Some(0), true));
}

// ── is_within_range — max_slop=None + in_order=true ─────────────────────

#[test]
fn in_order_no_slop_succeeds_when_order_exists() {
    // t1 at pos 3, t2 at pos 7: ordered with any gap → true.
    static T1: [u8; 1] = [3];
    static T2: [u8; 1] = [7];
    let t1: RSIndexResult<'static> = RSIndexResult::build_term()
        .borrowed_record(None, RSOffsetSlice::from_slice(&T1))
        .doc_id(1)
        .build();
    let t2: RSIndexResult<'static> = RSIndexResult::build_term()
        .borrowed_record(None, RSOffsetSlice::from_slice(&T2))
        .doc_id(1)
        .build();
    let mut ir = RSIndexResult::build_intersect(2).build();
    ir.push_borrowed(&t1, MetricsVec::new());
    ir.push_borrowed(&t2, MetricsVec::new());
    assert!(ir.is_within_range(None, true));
}

#[test]
fn in_order_no_slop_fails_when_order_impossible() {
    // t1 is only at position 10, t2 is only at position 5.
    // With in_order=true there is no pair (t1_pos, t2_pos) where t1_pos < t2_pos,
    // so the check must fail regardless of max_slop=None.
    static T1: [u8; 1] = [10]; // pos 10
    static T2: [u8; 1] = [5]; // pos 5 — cannot follow 10
    let t1: RSIndexResult<'static> = RSIndexResult::build_term()
        .borrowed_record(None, RSOffsetSlice::from_slice(&T1))
        .doc_id(1)
        .build();
    let t2: RSIndexResult<'static> = RSIndexResult::build_term()
        .borrowed_record(None, RSOffsetSlice::from_slice(&T2))
        .doc_id(1)
        .build();
    let mut ir = RSIndexResult::build_intersect(2).build();
    ir.push_borrowed(&t1, MetricsVec::new());
    ir.push_borrowed(&t2, MetricsVec::new());
    assert!(!ir.is_within_range(None, true));
}

#[test]
fn purely_numeric_children_always_true() {
    // An intersection of two numeric results has no offsets → trivially within range.
    let child1 = RSIndexResult::build_numeric(1.0).doc_id(1).build();
    let child2 = RSIndexResult::build_numeric(2.0).doc_id(1).build();
    let mut ir = RSIndexResult::build_intersect(2).build();
    ir.push_borrowed(&child1, MetricsVec::new());
    ir.push_borrowed(&child2, MetricsVec::new());
    assert!(ir.is_within_range(Some(0), false));
    assert!(ir.is_within_range(Some(0), true));
}

// ── is_within_range — full integration ───────────────────────────────────

/// vw1 = {1, 9, 13, 16, 22}, vw2 = {4, 7, 32}
#[test]
fn full_test_mirrors_cpp_testdistance() {
    // vw1 = {1, 9, 13, 16, 22} → deltas [1, 8, 4, 3, 6]
    // vw2 = {4, 7, 32}          → deltas [4, 3, 25]
    // Since all values < 128, varint bytes equal the delta values.
    static VW1_BYTES: [u8; 5] = [1, 8, 4, 3, 6];
    static VW2_BYTES: [u8; 3] = [4, 3, 25];

    let t1: RSIndexResult<'static> = RSIndexResult::build_term()
        .borrowed_record(None, RSOffsetSlice::from_slice(&VW1_BYTES))
        .doc_id(1)
        .build();
    let t2: RSIndexResult<'static> = RSIndexResult::build_term()
        .borrowed_record(None, RSOffsetSlice::from_slice(&VW2_BYTES))
        .doc_id(1)
        .build();

    let mut ir = RSIndexResult::build_intersect(2).build();
    ir.push_borrowed(&t1, MetricsVec::new());
    ir.push_borrowed(&t2, MetricsVec::new());

    // Unordered: slop=1 is true because (vw1=9, vw2=7) has span=1.
    assert!(!ir.is_within_range(Some(0), false));
    assert!(ir.is_within_range(Some(1), false));
    assert!(ir.is_within_range(Some(2), false));
    assert!(ir.is_within_range(Some(3), false));
    assert!(ir.is_within_range(Some(4), false));

    // In-order:
    assert!(!ir.is_within_range(Some(0), true));
    assert!(!ir.is_within_range(Some(1), true));
    assert!(ir.is_within_range(Some(2), true));
    assert!(ir.is_within_range(Some(3), true));
    assert!(ir.is_within_range(Some(4), true));
    assert!(ir.is_within_range(Some(5), true));
}

// ── RSTermRecord::FullyOwned ─────────────────────────────────────────────
//
// The `FullyOwned` variant owns both the query term (via `Box`) and the
// offsets (via `RSOffsetVector`), so the resulting `RSIndexResult` is
// independent of the original offset byte source.

/// Build a `FullyOwned`-backed result, drop the source bytes, and verify the
/// record still reads back correctly. Also exercises the `is_copy`,
/// `offsets`, and `query_term` match arms for the `FullyOwned` variant.
#[test]
fn fully_owned_term_result_is_independent_of_source_bytes() {
    let term = RSQueryTerm::new("abc", 1, 0);

    // Allocate the offset bytes on a temporary buffer, copy them into an
    // owned vector, and then explicitly drop the source buffer so any
    // subsequent read must go through the record's own allocation.
    let transient: Vec<u8> = vec![1, 4, 9];
    let offsets_vec = RSOffsetSlice::from_slice(&transient).to_owned();
    drop(transient);

    let ir = RSIndexResult::build_term()
        .fully_owned_record(Some(term), offsets_vec)
        .doc_id(42)
        .field_mask(7)
        .frequency(2)
        .weight(1.5)
        .build();

    let term_rec = ir.as_term().expect("term record");
    assert!(term_rec.is_copy(), "FullyOwned is a copy variant");
    assert!(ir.is_copy(), "FullyOwned bubbles up through RSIndexResult");
    assert_eq!(term_rec.offsets(), &[1, 4, 9]);
    assert_eq!(
        term_rec.query_term().and_then(|t| t.as_bytes()),
        Some(b"abc".as_ref())
    );
    assert_eq!(ir.doc_id, 42);
    assert_eq!(ir.field_mask, 7);
    assert_eq!(ir.freq, 2);
    assert_eq!(ir.weight, 1.5);
}

/// `set_offsets` on a `FullyOwned` record copies the input slice into the
/// record's own allocation (exercising the `FullyOwned` match arm of
/// `set_offsets`, distinct from the `Borrowed` arm covered elsewhere).
#[test]
fn set_offsets_on_fully_owned_copies_slice() {
    static INITIAL: [u8; 2] = [1, 2];
    static REPLACEMENT: [u8; 3] = [9, 8, 7];
    let term = RSQueryTerm::new("t", 1, 0);
    let mut ir = RSIndexResult::build_term()
        .fully_owned_record(Some(term), RSOffsetSlice::from_slice(&INITIAL).to_owned())
        .build();

    ir.as_term_mut()
        .unwrap()
        .set_offsets(RSOffsetSlice::from_slice(&REPLACEMENT));

    assert_eq!(ir.as_term().unwrap().offsets(), &REPLACEMENT);
}

/// `set_offsets_owned` must also work on the `Owned` variant, replacing its
/// offset vector in place.
#[test]
fn set_offsets_owned_on_owned_replaces_data() {
    // Build an `Owned` record via `to_owned()` from a `Borrowed` one.
    let source = RSIndexResult::build_term()
        .borrowed_record(None, RSOffsetSlice::from_slice(&[1u8]))
        .build();
    let mut owned = source.to_owned();
    assert!(owned.is_copy(), "to_owned produces a copy variant");

    let replacement = RSOffsetSlice::from_slice(&[42u8, 43]).to_owned();
    owned.as_term_mut().unwrap().set_offsets_owned(replacement);

    assert_eq!(owned.as_term().unwrap().offsets(), &[42, 43]);
}

/// Calling `set_offsets_owned` on a `Borrowed` record is a programming error:
/// the variant has no home for an owned vector. It must panic.
#[test]
#[should_panic(expected = "set_offsets_owned called on RSTermRecord::Borrowed")]
fn set_offsets_owned_on_borrowed_panics() {
    let mut ir = RSIndexResult::build_term()
        .borrowed_record(None, RSOffsetSlice::empty())
        .build();
    ir.as_term_mut()
        .unwrap()
        .set_offsets_owned(RSOffsetVector::empty());
}
