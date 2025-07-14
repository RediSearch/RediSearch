use std::mem::ManuallyDrop;

use ffi::t_docId;

use crate::{RSAggregateResult, RSIndexResult, RSIndexResultData, RSResultType, RSResultTypeMask};

#[unsafe(no_mangle)]
pub extern "C" fn ResultMetrics_Concat(_parent: *mut RSIndexResult, _child: *mut RSIndexResult) {
    // Do nothing since the code will call this
}

impl RSAggregateResult {
    fn get(&self, index: usize) -> Option<&RSIndexResult> {
        if index > self.num_children as _ {
            return None;
        }

        let child_ptr = unsafe { *self.children.add(index) };

        if child_ptr.is_null() {
            None
        } else {
            let child = unsafe { &*child_ptr };
            Some(child)
        }
    }
}

impl RSIndexResult {
    fn union(doc_id: t_docId, cap: usize, weight: f64) -> Self {
        Self {
            doc_id,
            dmd: std::ptr::null(),
            field_mask: 0,
            freq: 0,
            offsets_sz: 0,
            data: RSIndexResultData {
                agg: ManuallyDrop::new(RSAggregateResult::new(cap)),
            },
            result_type: RSResultType::Union,
            is_copy: false,
            metrics: std::ptr::null_mut(),
            weight,
        }
    }

    fn get(&self, index: usize) -> Option<&RSIndexResult> {
        if self.is_aggregate() {
            let agg = unsafe { &self.data.agg };

            agg.get(index)
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
    assert_eq!(ir.field_mask, 4);
}
