/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ops::DerefMut;

use ffi::RS_FIELDMASK_ALL;
use inverted_index::{
    RSAggregateResult, RSIndexResult, RSOffsetVector, RSResultType, RSResultTypeMask, RSTermRecord,
};

// We can't use the defaults in `c_mocks` for these functions since the tests in this file will call
// some of them. Therefore we are redefining them here with changes.
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
pub extern "C" fn IndexResult_ConcatMetrics(
    _parent: *mut RSIndexResult,
    _child: *const RSIndexResult,
) {
    // Do nothing since the code will call this
}

#[unsafe(no_mangle)]
pub extern "C" fn Term_Offset_Data_Free(_tr: *mut RSTermRecord) {
    // Cloning a term test will call this function
}

#[unsafe(no_mangle)]
pub extern "C" fn Term_Free(_t: *mut ffi::RSQueryTerm) {
    // Cloning a term test will call this function
}

#[unsafe(no_mangle)]
extern "C" fn RSOffsetVector_Copy(src: *const RSOffsetVector, dest: *mut RSOffsetVector) {
    unsafe {
        let src = &*src;
        let mut vec: Vec<u8> = Vec::with_capacity(src.len as _);
        let src_slice = std::slice::from_raw_parts(src.data as *mut u8, src.len as _);
        vec.extend_from_slice(src_slice);

        *dest = RSOffsetVector {
            data: vec.as_mut_ptr() as *mut _,
            len: vec.len() as _,
        }
    }
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
    let mut agg = RSAggregateResult::with_capacity(2);

    assert_eq!(agg.type_mask(), RSResultTypeMask::empty());

    agg.push(&RSIndexResult::numeric(10.0).doc_id(2));

    assert_eq!(
        agg.type_mask(),
        RSResultType::Numeric,
        "type mask should be ORed"
    );

    assert_eq!(agg.get(0), Some(&RSIndexResult::numeric(10.0).doc_id(2)));
    assert_eq!(agg.get(1), None, "This record does not exist yet");

    agg.push(&RSIndexResult::numeric(100.0).doc_id(3));

    assert_eq!(agg.type_mask(), RSResultType::Numeric);

    assert_eq!(agg.get(0), Some(&RSIndexResult::numeric(10.0).doc_id(2)));
    assert_eq!(agg.get(1), Some(&RSIndexResult::numeric(100.0).doc_id(3)));
    assert_eq!(agg.get(2), None, "This record does not exist yet");

    agg.push(&RSIndexResult::virt().doc_id(4));

    assert_eq!(
        agg.type_mask(),
        RSResultType::Numeric | RSResultType::Virtual,
        "types should be combined"
    );

    assert_eq!(agg.get(0), Some(&RSIndexResult::numeric(10.0).doc_id(2)));
    assert_eq!(agg.get(1), Some(&RSIndexResult::numeric(100.0).doc_id(3)));
    assert_eq!(agg.get(2), Some(&RSIndexResult::virt().doc_id(4)));
    assert_eq!(agg.get(3), None, "This record does not exist yet");
}

#[test]
fn pushing_to_index_result() {
    let mut ir = RSIndexResult::union(1).doc_id(1).weight(1.0);

    assert_eq!(ir.doc_id, 1);
    assert_eq!(ir.result_type, RSResultType::Union);
    assert_eq!(ir.weight, 1.0);
    assert_eq!(ir.freq, 0);
    assert_eq!(ir.field_mask, 0);

    ir.push(&RSIndexResult::virt().doc_id(2).frequency(3).field_mask(4));
    assert_eq!(ir.doc_id, 2, "should inherit doc id of the child");
    assert_eq!(ir.result_type, RSResultType::Union);
    assert_eq!(ir.weight, 1.0);
    assert_eq!(ir.freq, 3, "frequency should accumulate");
    assert_eq!(ir.field_mask, 4, "field mask should be ORed");
    assert_eq!(
        ir.get(0),
        Some(&RSIndexResult::virt().doc_id(2).frequency(3).field_mask(4))
    );

    ir.push(&RSIndexResult::numeric(5.0).doc_id(2).frequency(7));
    assert_eq!(ir.doc_id, 2);
    assert_eq!(ir.result_type, RSResultType::Union);
    assert_eq!(ir.weight, 1.0);
    assert_eq!(ir.freq, 10, "frequency should accumulate");
    assert_eq!(ir.field_mask, RS_FIELDMASK_ALL);
}

#[test]
fn cloning_an_aggregate_index_result() {
    let mut ir = RSIndexResult::intersect(5).doc_id(10).weight(3.0);

    ir.push(&mut RSIndexResult::numeric(5.0).doc_id(10));

    let ir_clone = ir.clone();

    assert_eq!(ir.doc_id, ir_clone.doc_id);
    assert_eq!(ir.dmd, ir_clone.dmd);
    assert_eq!(ir.field_mask, ir_clone.field_mask);
    assert_eq!(ir.freq, ir_clone.freq);
    assert_eq!(ir.offsets_sz, ir_clone.offsets_sz);
    unsafe {
        assert_eq!(ir.data.agg.type_mask(), ir_clone.data.agg.type_mask());
        assert_eq!(
            ir_clone.data.agg.capacity(),
            1,
            "should use as minimal capacity as needed"
        );
    }
    assert_eq!(ir.result_type, ir_clone.result_type);
    assert_eq!(ir.metrics, ir_clone.metrics);
    assert_eq!(ir.weight, ir_clone.weight);

    // Make sure the inner value was cloned too
    {
        let ir_first = ir.get(0).unwrap();
        let ir_clone_first = ir_clone.get(0).unwrap();

        assert_eq!(ir_first.doc_id, ir_clone_first.doc_id);
        assert_eq!(ir_first.dmd, ir_clone_first.dmd);
        assert_eq!(ir_first.field_mask, ir_clone_first.field_mask);
        assert_eq!(ir_first.freq, ir_clone_first.freq);
        assert_eq!(ir_first.offsets_sz, ir_clone_first.offsets_sz);
        unsafe {
            assert_eq!(ir_first.data.num, ir_clone_first.data.num);
        }
        assert_eq!(ir_first.result_type, ir_clone_first.result_type);
        assert_eq!(ir_first.metrics, ir_clone_first.metrics);
        assert_eq!(ir_first.weight, ir_clone_first.weight);
        assert!(ir_clone_first.is_copy);
    }
    assert!(ir_clone.is_copy);

    // Make sure the inner types are different
    ir.get_mut(0).unwrap().as_numeric_mut().unwrap().0 = 1.0;
    assert_eq!(
        ir_clone.get(0).unwrap().as_numeric().unwrap().0,
        5.0,
        "cloned value should not have changed"
    )
}

#[test]
fn cloning_a_numeric_index_result() {
    let mut ir = RSIndexResult::numeric(8.0).doc_id(3);
    let ir_clone = ir.clone();

    assert_eq!(ir.doc_id, ir_clone.doc_id);
    assert_eq!(ir.dmd, ir_clone.dmd);
    assert_eq!(ir.field_mask, ir_clone.field_mask);
    assert_eq!(ir.freq, ir_clone.freq);
    assert_eq!(ir.offsets_sz, ir_clone.offsets_sz);
    unsafe {
        assert_eq!(ir.data.num, ir_clone.data.num);
    }
    assert_eq!(ir.result_type, ir_clone.result_type);
    assert_eq!(ir.metrics, ir_clone.metrics);
    assert_eq!(ir.weight, ir_clone.weight);
    assert!(ir_clone.is_copy);

    // Make sure the values are not linked
    ir.as_numeric_mut().unwrap().0 = 1.0;

    unsafe {
        assert_eq!(
            ir_clone.data.num.0, 8.0,
            "cloned value should not have changed"
        );
    }
}

#[test]
fn cloning_a_virtual_index_result() {
    let ir = RSIndexResult::virt().doc_id(8).field_mask(4).weight(2.0);
    let ir_clone = ir.clone();

    assert_eq!(ir.doc_id, ir_clone.doc_id);
    assert_eq!(ir.dmd, ir_clone.dmd);
    assert_eq!(ir.field_mask, ir_clone.field_mask);
    assert_eq!(ir.freq, ir_clone.freq);
    assert_eq!(ir.offsets_sz, ir_clone.offsets_sz);
    assert_eq!(ir.result_type, ir_clone.result_type);
    assert_eq!(ir.metrics, ir_clone.metrics);
    assert_eq!(ir.weight, ir_clone.weight);
    assert!(ir_clone.is_copy);
}

#[test]
fn cloning_a_term_index_result() {
    let mut ir = RSIndexResult::term().doc_id(7).weight(8.0);

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

    const OFFSETS: [u8; 1] = [0];
    let offsets = RSOffsetVector {
        data: OFFSETS.as_ptr() as *mut _,
        len: OFFSETS.len() as _,
    };

    let inner_term = unsafe { &mut ir.data.term };
    inner_term.term = &mut term;
    inner_term.offsets = offsets;
    let ir_clone = ir.clone();

    assert_eq!(ir.doc_id, ir_clone.doc_id);
    assert_eq!(ir.dmd, ir_clone.dmd);
    assert_eq!(ir.field_mask, ir_clone.field_mask);
    assert_eq!(ir.freq, ir_clone.freq);
    assert_eq!(ir.offsets_sz, ir_clone.offsets_sz);
    unsafe {
        assert_eq!(ir.data.term.term, ir_clone.data.term.term);
    }
    assert_eq!(ir.result_type, ir_clone.result_type);
    assert_eq!(ir.metrics, ir_clone.metrics);
    assert_eq!(ir.weight, ir_clone.weight);
    assert!(ir_clone.is_copy);

    // Make sure the values are not linked
    unsafe {
        const NEW_OFFSETS: [u8; 2] = [1, 2];
        ir.data.term.deref_mut().offsets = RSOffsetVector {
            data: NEW_OFFSETS.as_ptr() as *mut _,
            len: NEW_OFFSETS.len() as _,
        };

        assert_eq!(
            ir_clone.data.term.offsets.len, 1,
            "cloned offsets should not have changed"
        );
    }
}
