use super::RLookupRow;
use ffi::{RSDocumentMetadata, RSIndexResult, RSScoreExplain};

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
