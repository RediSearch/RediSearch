/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{RSDocumentMetadata, RSIndexResult, RSScoreExplain, RSSortingVector, RSValue};

pub type DocId = u64;

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
