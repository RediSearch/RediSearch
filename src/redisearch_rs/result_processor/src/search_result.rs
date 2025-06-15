use super::RLookupRow;
use ffi::{RSDocumentMetadata, RSIndexResult, RSScoreExplain};

use std::ptr;

pub type DocId = u64;

/// SearchResult - the object all the processing chain is working on.
/// It has the indexResult which is what the index scan brought - scores, vectors, flags, etc,
/// and a list of fields loaded by the chain
/// cbindgen:rename-all=CamelCase
#[repr(C)]
pub struct SearchResult {
    pub doc_id: DocId,
    pub score: f64,
    pub score_explain: *mut RSScoreExplain,
    pub dmd: *const RSDocumentMetadata,
    pub index_result: *mut RSIndexResult,
    pub rowdata: RLookupRow,
    pub flags: u8,
}

#[unsafe(no_mangle)]
unsafe extern "C" fn SearchResult_Clear_cnd(r: *mut SearchResult) {
    let r = unsafe { r.as_mut().unwrap() };

    // This won't affect anything if the result is null
    r.score = 0.0;

    if !r.score_explain.is_null() {
        ffi::SEDestroy(r.score_explain);
        r.score_explain = ptr::null_mut();
    }

    r.index_result = ptr::null_mut();

    r.flags = 0;
    let rlr = &mut r.rowdata as *mut RLookupRow;
    let rlr = rlr as *mut ffi::RLookupRow;

    // Safety: The RLookupRow_Wipe function only deletes the content of the
    // RLookupRow, not the row itself.
    unsafe {
        ffi::RLookupRow_Wipe(rlr);
    }

    if !r.dmd.is_null() {
        // TODO: this is an inline function in the C code, so we cannot call it directly.
        //ffi::DMD_Return(r.dmd);
        r.dmd = ptr::null();
    }
}
