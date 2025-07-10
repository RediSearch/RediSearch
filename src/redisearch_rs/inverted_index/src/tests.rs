use ffi::{RS_FIELDMASK_ALL, RSQueryTerm};

use crate::{
    RSAggregateResult, RSIndexResult, RSNumericRecord, RSOffsetVector, RSResultType,
    RSResultTypeMask, RSTermRecord,
};

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
pub extern "C" fn Term_Offset_Data_Free(_tr: *mut RSTermRecord) {
    panic!("Nothing should have copied the term record to require this call");
}

#[unsafe(no_mangle)]
pub extern "C" fn Term_Free(_t: *mut RSQueryTerm) {
    panic!("No test created a term record");
}

#[unsafe(no_mangle)]
pub extern "C" fn ResultMetrics_Concat(_parent: *mut RSIndexResult, _child: *mut RSIndexResult) {
    // Do nothing
}

#[unsafe(no_mangle)]
pub extern "C" fn RSOffsetVector_Copy(_src: *const RSOffsetVector, _dest: *mut RSOffsetVector) {
    panic!("there are no term tests yet");
}

impl RSAggregateResult {
    fn get_mut(&self, index: usize) -> Option<&mut RSIndexResult> {
        if index > self.num_children as _ {
            return None;
        }

        let child_ptr = unsafe { *self.children.add(index) };

        if child_ptr.is_null() {
            None
        } else {
            let child = unsafe { &mut *child_ptr };
            Some(child)
        }
    }
}

impl RSIndexResult {
    fn get_mut(&self, index: usize) -> Option<&mut RSIndexResult> {
        if self.is_aggregate() {
            let agg = unsafe { &self.data.agg };

            agg.get_mut(index)
        } else {
            None
        }
    }

    fn as_numeric_mut(&mut self) -> Option<&mut RSNumericRecord> {
        if matches!(
            self.result_type,
            RSResultType::Numeric | RSResultType::Metric,
        ) {
            Some(unsafe { &mut self.data.num })
        } else {
            None
        }
    }
}

#[test]
fn pushing_to_aggregate_result() {
    let mut agg = RSAggregateResult::new(2);

    assert_eq!(agg.children_cap, 2);
    assert_eq!(agg.num_children, 0);
    assert_eq!(agg.type_mask, RSResultTypeMask::empty());

    agg.push(&mut RSIndexResult::numeric(2, 10.0));

    assert_eq!(agg.children_cap, 2);
    assert_eq!(agg.num_children, 1);
    assert_eq!(agg.type_mask, RSResultType::Numeric);

    assert_eq!(agg.get(0), Some(&RSIndexResult::numeric(2, 10.0)));
    assert_eq!(agg.get(1), None, "This record does not exist yet");

    agg.push(&mut RSIndexResult::numeric(3, 100.0));

    assert_eq!(agg.children_cap, 2);
    assert_eq!(agg.num_children, 2);
    assert_eq!(agg.type_mask, RSResultType::Numeric);

    assert_eq!(agg.get(0), Some(&RSIndexResult::numeric(2, 10.0)));
    assert_eq!(agg.get(1), Some(&RSIndexResult::numeric(3, 100.0)));
    assert_eq!(agg.get(2), None, "This record does not exist yet");

    agg.push(&mut RSIndexResult::virt(4, 2, 2.0));

    assert_eq!(agg.children_cap, 4);
    assert_eq!(agg.num_children, 3);
    assert_eq!(
        agg.type_mask,
        RSResultType::Numeric | RSResultType::Virtual,
        "types should be combined"
    );

    assert_eq!(agg.get(0), Some(&RSIndexResult::numeric(2, 10.0)));
    assert_eq!(agg.get(1), Some(&RSIndexResult::numeric(3, 100.0)));
    assert_eq!(agg.get(2), Some(&RSIndexResult::virt(4, 2, 2.0)));
    assert_eq!(agg.get(3), None, "This record does not exist yet");
}

#[test]
fn pushing_to_index_result() {
    let mut ir = RSIndexResult::union(1, 1, 1.0);

    assert_eq!(ir.doc_id, 1);
    assert_eq!(ir.result_type, RSResultType::Union);
    assert_eq!(ir.weight, 1.0);
    assert_eq!(ir.freq, 0);
    assert_eq!(ir.field_mask, 0);

    ir.push(&mut RSIndexResult::virt(2, 4, 2.0));
    assert_eq!(ir.doc_id, 2);
    assert_eq!(ir.result_type, RSResultType::Union);
    assert_eq!(ir.weight, 1.0);
    assert_eq!(ir.freq, 0);
    assert_eq!(ir.field_mask, 4);
    assert_eq!(ir.get(0), Some(&RSIndexResult::virt(2, 4, 2.0)));

    let mut result_with_frequency = RSIndexResult::numeric(2, 5.0);
    result_with_frequency.freq = 7;

    ir.push(&mut result_with_frequency);
    assert_eq!(ir.doc_id, 2);
    assert_eq!(ir.result_type, RSResultType::Union);
    assert_eq!(ir.weight, 1.0);
    assert_eq!(ir.freq, 7);
    assert_eq!(ir.field_mask, RS_FIELDMASK_ALL);
}

#[test]
fn cloning_an_aggregate_index_result() {
    let mut ir = RSIndexResult::intersect(10, 5, 3.0);

    ir.push(&mut RSIndexResult::numeric(10, 5.0));

    let ir_clone = ir.clone();

    assert_eq!(ir.doc_id, ir_clone.doc_id);
    assert_eq!(ir.dmd, ir_clone.dmd);
    assert_eq!(ir.field_mask, ir_clone.field_mask);
    assert_eq!(ir.freq, ir_clone.freq);
    assert_eq!(ir.offsets_sz, ir_clone.offsets_sz);
    unsafe {
        assert_eq!(ir.data.agg.num_children, ir_clone.data.agg.num_children);
        assert_eq!(ir.data.agg.type_mask, ir_clone.data.agg.type_mask);
        assert_eq!(
            ir_clone.data.agg.children_cap, 1,
            "should use as minimal capacity as needed"
        );
    }
    assert_eq!(ir.result_type, ir_clone.result_type);
    assert_eq!(ir.metrics, ir_clone.metrics);
    assert_eq!(ir.weight, ir_clone.weight);
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
    let mut ir = RSIndexResult::numeric(3, 8.0);
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
    let ir = RSIndexResult::virt(8, 4, 2.0);
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
    let mut ir = RSIndexResult::term(7, todo!("how do i make a term"), 8.0);
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
