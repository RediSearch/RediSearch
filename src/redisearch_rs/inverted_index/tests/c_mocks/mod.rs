use ffi::RSQueryTerm;
use inverted_index::{RSIndexResult, RSTermRecord};

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
    panic!("Nothing should have copied the term record to require this call");
}

#[unsafe(no_mangle)]
pub extern "C" fn Term_Free(t: *mut RSQueryTerm) {
    if !t.is_null() {
        panic!("No test created a term record");
    }
}
