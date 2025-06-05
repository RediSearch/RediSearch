pub type DocId = u64;

use ffi::RSDocumentMetadata;
use ffi::RSIndexResult;
use ffi::RSScoreExplain;
use ffi::RSSortingVector;
use ffi::RSValue;

/// Row data for a lookup key. This abstracts the question of "where" the
/// data comes from.
/// cbindgen:field-names=[sv, dyn, ndyn]
#[repr(C)]
pub struct RLookupRow {
    pub sv: *const RSSortingVector,

    // todo bindgen rename
    pub dyn_: *mut *mut RSValue,

    ndyn: usize,
}

/// SearchResult - the object all the processing chain is working on.
/// It has the indexResult which is what the index scan brought - scores, vectors, flags, etc,
/// and a list of fields loaded by the chain
/// cbindgen:rename-all=CamelCase
#[repr(C)]
pub struct SearchResult {
    pub doc_id: DocId,
    pub score: f64,
    pub score_explain: *mut RSScoreExplain,
    pub dmd: *mut RSDocumentMetadata,
    pub index_result: *mut RSIndexResult,
    pub rowdata: RLookupRow,
    pub flags: u8,
}
