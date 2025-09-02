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
    RSAggregateResult, RSIndexResult, RSOffsetVector, RSResultKind, RSResultKindMask,
};

// We can't use the defaults in `c_mocks` for these functions since the tests in this file will call
// some of them. Therefore we are redefining the following function here:
// - Term_Free
#[unsafe(no_mangle)]
pub extern "C" fn ResultMetrics_Free(result: *mut RSIndexResult) {
    if result.is_null() {
        panic!("did not expect `RSIndexResult` to be null");
    }

    let metrics = unsafe { (*result).metrics };
    if metrics.is_null() {
        return;
    }

    panic!(
        "did not expect any test to set metrics, but got: {:?}",
        unsafe { *metrics }
    );
}

#[unsafe(no_mangle)]
pub extern "C" fn RSYieldableMetric_Concat(
    _parent: *mut *mut ffi::RSYieldableMetric,
    _child: *const ffi::RSYieldableMetric,
) {
    // Do nothing since the code will call this
}

#[unsafe(no_mangle)]
pub extern "C" fn Term_Free(_t: *mut ffi::RSQueryTerm) {
    // Making an owned copy of a term test will call this function
}

#[allow(non_snake_case)]
#[unsafe(no_mangle)]
unsafe fn RSYieldableMetrics_Clone(
    _src: *mut ffi::RSYieldableMetric,
) -> *mut ffi::RSYieldableMetric {
    panic!("none of the tests should set any metrics");
}

#[test]
fn pushing_to_aggregate_result() {
    let num_first = RSIndexResult::numeric(10.0).doc_id(2);
    let num_second = RSIndexResult::numeric(100.0).doc_id(3);
    let virt_first = RSIndexResult::virt().doc_id(4);

    let mut agg = RSAggregateResult::with_capacity(2);

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
    assert_eq!(ir.offsets_sz, ir_copy.offsets_sz);

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
        assert_eq!(ir_first.offsets_sz, ir_clone_first.offsets_sz);
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
    assert_eq!(ir.offsets_sz, ir_copy.offsets_sz);
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
    assert_eq!(ir.offsets_sz, ir_copy.offsets_sz);
    assert_eq!(ir.data, ir_copy.data);
    assert_eq!(ir.metrics, ir_copy.metrics);
    assert_eq!(ir.weight, ir_copy.weight);
}

#[test]
fn to_owned_a_term_index_result() {
    const TEST_STR: &str = "test_term";
    let test_str_ptr = TEST_STR.as_ptr() as *mut _;
    let mut term = ffi::RSQueryTerm {
        str_: test_str_ptr,
        len: TEST_STR.len(),
        idf: 1.0,
        id: 2,
        flags: 3,
        bm25_idf: 4.0,
    };

    let mut offsets: [i8; 1] = [0];
    let offsets = RSOffsetVector::with_data(offsets.as_mut_ptr() as _, offsets.len() as _);

    let ir = RSIndexResult::term_with_term_ptr(&mut term, offsets, 7, 1, 1);
    let mut ir_copy = ir.to_owned();

    assert_eq!(ir.doc_id, ir_copy.doc_id);
    assert_eq!(ir.dmd, ir_copy.dmd);
    assert_eq!(ir.field_mask, ir_copy.field_mask);
    assert_eq!(ir.freq, ir_copy.freq);
    assert_eq!(ir.offsets_sz, ir_copy.offsets_sz);
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
    ir_copy.as_term_mut().unwrap().clear_offsets();

    assert_eq!(
        ir.as_term().unwrap().offsets().len(),
        1,
        "cloned offsets should not have changed"
    );
}
