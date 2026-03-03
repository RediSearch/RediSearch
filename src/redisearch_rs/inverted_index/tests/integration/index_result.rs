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
    RSAggregateResult, RSIndexResult, RSOffsetSlice, RSOffsetVector, RSResultData, RSResultKind,
    RSResultKindMask, RSTermRecord,
};
use query_term::RSQueryTerm;

#[test]
fn pushing_to_aggregate_result() {
    let num_first = RSIndexResult::numeric(10.0).doc_id(2);
    let num_second = RSIndexResult::numeric(100.0).doc_id(3);
    let virt_first = RSIndexResult::virt().doc_id(4);

    let mut agg = RSAggregateResult::borrowed_with_capacity(2);

    assert_eq!(agg.kind_mask(), RSResultKindMask::empty());

    agg.push_borrowed(&num_first);

    assert_eq!(
        agg.kind_mask(),
        RSResultKind::Numeric,
        "type mask should be ORed"
    );

    assert_eq!(agg.get(0), Some(&RSIndexResult::numeric(10.0).doc_id(2)));
    assert_eq!(agg.get(1), None, "This record does not exist yet");

    agg.push_borrowed(&num_second);

    assert_eq!(agg.kind_mask(), RSResultKind::Numeric);

    assert_eq!(agg.get(0), Some(&RSIndexResult::numeric(10.0).doc_id(2)));
    assert_eq!(agg.get(1), Some(&RSIndexResult::numeric(100.0).doc_id(3)));
    assert_eq!(agg.get(2), None, "This record does not exist yet");

    agg.push_borrowed(&virt_first);

    assert_eq!(
        agg.kind_mask(),
        RSResultKind::Numeric | RSResultKind::Virtual,
        "types should be combined"
    );

    assert_eq!(agg.get(0), Some(&RSIndexResult::numeric(10.0).doc_id(2)));
    assert_eq!(agg.get(1), Some(&RSIndexResult::numeric(100.0).doc_id(3)));
    assert_eq!(agg.get(2), Some(&RSIndexResult::virt().doc_id(4)));
    assert_eq!(agg.get(3), None, "This record does not exist yet");
}

#[test]
fn pushing_to_index_result() {
    let result_virt = RSIndexResult::virt().doc_id(2).frequency(3).field_mask(4);
    let result_with_frequency = RSIndexResult::numeric(5.0).doc_id(2).frequency(7);

    let mut ir = RSIndexResult::union(1).doc_id(1).weight(1.0);

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
        Some(&RSIndexResult::virt().doc_id(2).frequency(3).field_mask(4))
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
    let num_rec = RSIndexResult::numeric(5.0).doc_id(10);
    let mut ir = RSIndexResult::intersect(5).doc_id(10).weight(3.0);

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
        assert_eq!(ir_first.data, ir_clone_first.data);
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
    let ir = RSIndexResult::numeric(8.0).doc_id(3);
    let mut ir_copy = ir.to_owned();

    assert_eq!(ir.doc_id, ir_copy.doc_id);
    assert_eq!(ir.dmd, ir_copy.dmd);
    assert_eq!(ir.field_mask, ir_copy.field_mask);
    assert_eq!(ir.freq, ir_copy.freq);
    assert_eq!(ir.data, ir_copy.data);
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
    let ir = RSIndexResult::virt().doc_id(8).field_mask(4).weight(2.0);
    let ir_copy = ir.to_owned();

    assert_eq!(ir.doc_id, ir_copy.doc_id);
    assert_eq!(ir.dmd, ir_copy.dmd);
    assert_eq!(ir.field_mask, ir_copy.field_mask);
    assert_eq!(ir.freq, ir_copy.freq);
    assert_eq!(ir.data, ir_copy.data);
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

    let ir = RSIndexResult::with_term(Some(term), offsets, 7, 1, 1);
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
    match &mut ir_copy.data {
        RSResultData::Term(RSTermRecord::Owned { offsets, .. }) => {
            *offsets = RSOffsetVector::empty();
        }
        _ => panic!("expected owned term record"),
    }

    assert_eq!(
        ir.as_term().unwrap().offsets().len(),
        1,
        "cloned offsets should not have changed"
    );
}

/// Tests for [`RSIndexResult::is_within_range`] — the slop/proximity check.
///
/// These tests mirror the Python integration test `testconfigMultiTextOffsetDeltaSlop0`,
/// which verifies how `MULTI_TEXT_SLOP=0` affects cross-element proximity queries on a
/// JSON array field.
///
/// With `MULTI_TEXT_SLOP=0` the array elements are indexed consecutively without
/// positional gaps.  For the document:
///
/// ```json
/// { "category": ["mathematics and computer science", "logic", "programming", "database"] }
/// ```
///
/// Positions (stop words such as "and" are excluded):
/// ```text
/// mathematics=1, computer=2, science=3, logic=4, programming=5, database=6
/// ```
///
/// The span between two in-order terms at positions `p_a` and `p_b` (`p_b > p_a`) is:
/// ```text
/// span = p_b - p_a - 1
/// ```
/// A proximity query with `SLOP N` matches when `span <= N`.
mod proximity {
    use ffi::RS_FIELDMASK_ALL;
    use inverted_index::RSIndexResult;
    use inverted_index::test_utils::TestTermRecord;

    /// Encode absolute token positions as a varint delta-encoded byte sequence,
    /// matching the format stored in `RSOffsetSlice`.
    ///
    /// Positions must be strictly increasing.  All deltas must be < 128 so that
    /// each value fits in a single byte (the common case for small positions used
    /// in unit tests).
    fn encode_positions(positions: &[u32]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(positions.len());
        let mut last = 0u32;
        for &pos in positions {
            let delta = pos - last;
            assert!(
                delta < 128,
                "test helper only supports single-byte varint deltas"
            );
            bytes.push(delta as u8);
            last = pos;
        }
        bytes
    }

    // ── Unordered proximity (in_order = false) ────────────────────────────────

    /// `mathematics`(1) … `database`(6): span = 4.  SLOP 4 is exactly the boundary
    /// and must match.
    ///
    /// Covers the `span <= max_slop → return true` branch in
    /// `within_range_unordered`.
    #[test]
    fn unordered_match_at_slop_boundary() {
        let math_offsets = encode_positions(&[1]);
        let db_offsets = encode_positions(&[6]);
        let math = TestTermRecord::new(1, RS_FIELDMASK_ALL, 1, &math_offsets);
        let db = TestTermRecord::new(1, RS_FIELDMASK_ALL, 1, &db_offsets);
        let mut agg = RSIndexResult::intersect(2);
        agg.push_borrowed(&math.record);
        agg.push_borrowed(&db.record);
        assert!(agg.is_within_range(4, false));
    }

    /// `mathematics`(1) … `database`(6): span = 4.  SLOP 3 is one below the
    /// boundary and must not match.
    ///
    /// Covers the `span > max_slop → advance → EOF → return false` path in
    /// `within_range_unordered`.
    #[test]
    fn unordered_no_match_below_slop_boundary() {
        let math_offsets = encode_positions(&[1]);
        let db_offsets = encode_positions(&[6]);
        let math = TestTermRecord::new(1, RS_FIELDMASK_ALL, 1, &math_offsets);
        let db = TestTermRecord::new(1, RS_FIELDMASK_ALL, 1, &db_offsets);
        let mut agg = RSIndexResult::intersect(2);
        agg.push_borrowed(&math.record);
        agg.push_borrowed(&db.record);
        assert!(!agg.is_within_range(3, false));
    }

    /// Term A has two positions [1, 4]; term B is at [6].
    /// The first candidate pair (1, 6) has span 4 > 1, so the algorithm
    /// advances A to its second position; pair (4, 6) has span 1 == max_slop,
    /// which must match.
    ///
    /// Covers the "outer loop iterates more than once before matching" path in
    /// `within_range_unordered`.
    #[test]
    fn unordered_second_position_satisfies_slop() {
        let a_offsets = encode_positions(&[1, 4]);
        let b_offsets = encode_positions(&[6]);
        let term_a = TestTermRecord::new(1, RS_FIELDMASK_ALL, 1, &a_offsets);
        let term_b = TestTermRecord::new(1, RS_FIELDMASK_ALL, 1, &b_offsets);
        let mut agg = RSIndexResult::intersect(2);
        agg.push_borrowed(&term_a.record);
        agg.push_borrowed(&term_b.record);
        assert!(agg.is_within_range(1, false));
    }

    /// Term A has positions [1, 10]; term B is at [6].
    /// When A (the current minimum) is advanced from 1 to 10, the new position
    /// exceeds the current maximum (6), so the tracked maximum is updated to 10.
    /// No pair within SLOP 2 exists, so the result is no match.
    ///
    /// Covers the `positions[min_pos_idx] > max → max = new_pos` branch in
    /// `within_range_unordered`.
    #[test]
    fn unordered_advancing_min_past_max_updates_max() {
        let a_offsets = encode_positions(&[1, 10]);
        let b_offsets = encode_positions(&[6]);
        let term_a = TestTermRecord::new(1, RS_FIELDMASK_ALL, 1, &a_offsets);
        let term_b = TestTermRecord::new(1, RS_FIELDMASK_ALL, 1, &b_offsets);
        let mut agg = RSIndexResult::intersect(2);
        agg.push_borrowed(&term_a.record);
        agg.push_borrowed(&term_b.record);
        assert!(!agg.is_within_range(2, false));
    }

    // ── Ordered proximity (in_order = true) ──────────────────────────────────

    /// `mathematics`(1) … `database`(6): span = 4.  SLOP 4 with `in_order=true`
    /// must match.
    ///
    /// Covers the `span <= max_slop → return true` branch in
    /// `within_range_in_order`.
    #[test]
    fn ordered_match_at_slop_boundary() {
        let math_offsets = encode_positions(&[1]);
        let db_offsets = encode_positions(&[6]);
        let math = TestTermRecord::new(1, RS_FIELDMASK_ALL, 1, &math_offsets);
        let db = TestTermRecord::new(1, RS_FIELDMASK_ALL, 1, &db_offsets);
        let mut agg = RSIndexResult::intersect(2);
        agg.push_borrowed(&math.record);
        agg.push_borrowed(&db.record);
        assert!(agg.is_within_range(4, true));
    }

    /// `mathematics`(1) … `database`(6): span = 4.  SLOP 3 with `in_order=true`
    /// must not match.
    ///
    /// Covers the `exhausted → return false` branch in `within_range_in_order`:
    /// after span > max_slop on the first outer-loop iteration, the algorithm
    /// retries by advancing term 0; since term 0 has no further positions it is
    /// exhausted.
    #[test]
    fn ordered_no_match_below_slop_boundary() {
        let math_offsets = encode_positions(&[1]);
        let db_offsets = encode_positions(&[6]);
        let math = TestTermRecord::new(1, RS_FIELDMASK_ALL, 1, &math_offsets);
        let db = TestTermRecord::new(1, RS_FIELDMASK_ALL, 1, &db_offsets);
        let mut agg = RSIndexResult::intersect(2);
        agg.push_borrowed(&math.record);
        agg.push_borrowed(&db.record);
        assert!(!agg.is_within_range(3, true));
    }

    /// With `in_order=true`, term A at position 6 and term B at position 1
    /// means B appears *before* A in the document.  The ordering constraint
    /// requires B to appear *after* A, so the algorithm advances B past its only
    /// position and exhausts it → no match, regardless of SLOP.
    ///
    /// Covers the "order-check while-loop advances a subsequent iterator to EOF"
    /// path in `within_range_in_order`.
    #[test]
    fn ordered_terms_in_wrong_order_never_match() {
        let a_offsets = encode_positions(&[6]); // "first" query term appears at position 6
        let b_offsets = encode_positions(&[1]); // "second" query term appears at position 1 (before!)
        let term_a = TestTermRecord::new(1, RS_FIELDMASK_ALL, 1, &a_offsets);
        let term_b = TestTermRecord::new(1, RS_FIELDMASK_ALL, 1, &b_offsets);
        let mut agg = RSIndexResult::intersect(2);
        agg.push_borrowed(&term_a.record);
        agg.push_borrowed(&term_b.record);
        assert!(!agg.is_within_range(100, true));
    }

    /// Three terms at positions [1, 3, 15] with `in_order=true` and SLOP 1.
    /// Processing the inner loop: span(1→3)=1 is within budget, but
    /// span(3→15)=11+1=12 exceeds it → the inner loop breaks early before
    /// processing any further terms.  After retrying the outer loop, term 0 is
    /// exhausted → no match.
    ///
    /// Covers the early-exit `break` inside the inner `for` loop of
    /// `within_range_in_order`.
    #[test]
    fn ordered_early_span_exceeded_triggers_inner_break() {
        let a_offsets = encode_positions(&[1]);
        let b_offsets = encode_positions(&[3]);
        let c_offsets = encode_positions(&[15]);
        let term_a = TestTermRecord::new(1, RS_FIELDMASK_ALL, 1, &a_offsets);
        let term_b = TestTermRecord::new(1, RS_FIELDMASK_ALL, 1, &b_offsets);
        let term_c = TestTermRecord::new(1, RS_FIELDMASK_ALL, 1, &c_offsets);
        let mut agg = RSIndexResult::intersect(3);
        agg.push_borrowed(&term_a.record);
        agg.push_borrowed(&term_b.record);
        agg.push_borrowed(&term_c.record);
        assert!(!agg.is_within_range(1, true));
    }

    // ── Early-return paths in is_within_range ────────────────────────────────

    /// With `max_slop=-1` and `in_order=false`, `is_within_range` returns `true`
    /// immediately without calling any proximity function.
    #[test]
    fn negative_slop_unordered_always_matches() {
        let a_offsets = encode_positions(&[1]);
        let b_offsets = encode_positions(&[100]);
        let term_a = TestTermRecord::new(1, RS_FIELDMASK_ALL, 1, &a_offsets);
        let term_b = TestTermRecord::new(1, RS_FIELDMASK_ALL, 1, &b_offsets);
        let mut agg = RSIndexResult::intersect(2);
        agg.push_borrowed(&term_a.record);
        agg.push_borrowed(&term_b.record);
        assert!(agg.is_within_range(-1, false));
    }

    /// Non-aggregate results (single terms, numeric, virtual) always return
    /// `true` from `is_within_range` because there is nothing to compare.
    #[test]
    fn single_term_result_always_matches() {
        let offsets = encode_positions(&[5]);
        let term = TestTermRecord::new(1, RS_FIELDMASK_ALL, 1, &offsets);
        assert!(term.record.is_within_range(0, true));
    }

    /// Aggregate children with empty offset slices are skipped.  When all
    /// children have empty offsets the iterator list is empty and
    /// `is_within_range` returns `true`.
    #[test]
    fn aggregate_with_empty_offsets_always_matches() {
        let term_a = TestTermRecord::new(1, RS_FIELDMASK_ALL, 1, &[]);
        let term_b = TestTermRecord::new(1, RS_FIELDMASK_ALL, 1, &[]);
        let mut agg = RSIndexResult::intersect(2);
        agg.push_borrowed(&term_a.record);
        agg.push_borrowed(&term_b.record);
        assert!(agg.is_within_range(0, true));
    }
}
