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
    RSAggregateResult, RSIndexResult, RSOffsetSlice, RSResultKind, RSResultKindMask,
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
