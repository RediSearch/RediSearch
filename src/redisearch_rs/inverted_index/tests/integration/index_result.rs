/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::RS_FIELDMASK_ALL;
use inverted_index::{
    RSAggregateResult, RSIndexResult, RSOffsetSlice, RSOffsetVector, RSResultKind, RSResultKindMask,
};
use query_term::RSQueryTerm;

#[test]
fn pushing_to_aggregate_result() {
    let num_first = RSIndexResult::build_numeric(10.0).doc_id(2).build();
    let num_second = RSIndexResult::build_numeric(100.0).doc_id(3).build();
    let virt_first = RSIndexResult::build_virt().doc_id(4).build();

    let mut agg = RSAggregateResult::borrowed_with_capacity(2);

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

    ir.push_borrowed(&result_virt);
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

    ir.push_borrowed(&result_with_frequency);
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

    ir.push_borrowed(&num_rec);

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
    ir.push_borrowed(&child);
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
    ir.push_borrowed(&t1);
    ir.push_borrowed(&t2);
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
    ir.push_borrowed(&t1);
    ir.push_borrowed(&t2);
    assert!(!ir.is_within_range(None, true));
}

#[test]
fn purely_numeric_children_always_true() {
    // An intersection of two numeric results has no offsets → trivially within range.
    let child1 = RSIndexResult::build_numeric(1.0).doc_id(1).build();
    let child2 = RSIndexResult::build_numeric(2.0).doc_id(1).build();
    let mut ir = RSIndexResult::build_intersect(2).build();
    ir.push_borrowed(&child1);
    ir.push_borrowed(&child2);
    assert!(ir.is_within_range(Some(0), false));
    assert!(ir.is_within_range(Some(0), true));
}

// ── is_within_range — full integration ───────────────────────────────────

/// C-Code: Mirrors the C++ `testDistance` test using the full `is_within_range` entry point.
///
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
    ir.push_borrowed(&t1);
    ir.push_borrowed(&t2);

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
/// `offsets`, and `query_term` match arms for the new variant.
#[test]
fn fully_owned_term_result_is_independent_of_source_bytes() {
    let term = RSQueryTerm::new("abc", 1, 0);

    // Allocate the offset bytes on a temporary, short-lived buffer and then
    // copy them into an owned vector before the buffer is dropped.
    let offsets_vec = {
        let transient: [u8; 3] = [1, 4, 9];
        RSOffsetSlice::from_slice(&transient).to_owned()
    };

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
