use ffi::RSQueryTerm;

use crate::{RSAggregateResult, RSIndexResult, RSResultType, RSResultTypeMask, RSTermRecord};

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

#[test]
fn pushing_to_aggregate_result() {
    let mut agg = RSAggregateResult::new(2);

    assert_eq!(agg.children_cap, 2);
    assert_eq!(agg.num_children, 0);
    assert_eq!(agg.type_mask, RSResultTypeMask::empty());

    agg.push(RSIndexResult::numeric(2, 10.0));

    assert_eq!(agg.children_cap, 2);
    assert_eq!(agg.num_children, 1);
    assert_eq!(agg.type_mask, RSResultType::Numeric);

    assert_eq!(agg.get(0), Some(&RSIndexResult::numeric(2, 10.0)));
    assert_eq!(agg.get(1), None, "This record does not exist yet");

    agg.push(RSIndexResult::numeric(3, 100.0));

    assert_eq!(agg.children_cap, 2);
    assert_eq!(agg.num_children, 2);
    assert_eq!(agg.type_mask, RSResultType::Numeric);

    assert_eq!(agg.get(0), Some(&RSIndexResult::numeric(2, 10.0)));
    assert_eq!(agg.get(1), Some(&RSIndexResult::numeric(3, 100.0)));
    assert_eq!(agg.get(2), None, "This record does not exist yet");

    agg.push(RSIndexResult::virt(4, 2, 2.0));

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
