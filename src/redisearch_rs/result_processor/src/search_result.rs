/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use enumflags2::{BitFlags, bitflags};
use rlookup::RLookupRow;
use value::RSValueFFI;

use crate::bindings::DocumentMetadata;

#[bitflags]
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum SearchResultFlag {
    ExpiredDoc = 1 << 0,
}

pub type SearchResultFlags = enumflags2::BitFlags<SearchResultFlag>;

// /*
//  * SearchResult - the object all the processing chain is working on.
//  * It has the indexResult which is what the index scan brought - scores, vectors, flags, etc,
//  * and a list of fields loaded by the chain
//  */
#[derive(Clone)]
pub struct SearchResult<'a> {
    doc_id: ffi::t_docId,
    // not all results have score - TBD
    score: f32,

    // TODO resolve ownership
    score_explain: Option<NonNull<ffi::RSScoreExplain>>,

    document_metadata: Option<DocumentMetadata>,

    // // index result should cover what you need for highlighting,
    // // but we will add a method to duplicate index results to make
    // // them thread safe
    // TODO resolve ownership
    index_result: Option<NonNull<ffi::RSIndexResult>>,

    // // Row data. Use RLookup_* functions to access
    row_data: RLookupRow<'a, RSValueFFI>,

    flags: SearchResultFlags,
}

impl Drop for SearchResult<'_> {
    fn drop(&mut self) {
        self.clear();
        self.row_data.reset_dyn_values();
    }
}

impl<'a> Default for SearchResult<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> SearchResult<'a> {
    pub const fn new() -> Self {
        Self {
            doc_id: 0,
            score: 0.0,
            score_explain: None,
            document_metadata: None,
            index_result: None,
            row_data: RLookupRow::new(),
            flags: SearchResultFlags::from_bits_truncate_c(0, BitFlags::CONST_TOKEN),
        }
    }

    pub fn override_with(&mut self, src: &Self) {
        self.row_data.reset_dyn_values();
        self.clone_from(src);
    }

    // /**
    //  * This function resets the search result, so that it may be reused again.
    //  * Internal caches are reset but not freed
    //  */
    pub fn clear(&mut self) {
        self.score = 0.0;

        if let Some(score_explain) = self.score_explain.take() {
            unsafe {
                ffi::SEDestroy(score_explain.as_ptr());
            }
        }

        // explicitly drop the DMD here to make clear we maintain the same behaviour
        let _ = self.document_metadata.take();

        if let Some(_index_result) = self.index_result.take() {
            // TODO figure out if this should be called
            //   // IndexResult_Free(r->indexResult);
        }

        self.row_data.wipe();

        self.flags = SearchResultFlags::empty();
    }

    pub fn doc_id(&self) -> ffi::t_docId {
        self.doc_id
    }

    pub fn set_doc_id(&mut self, doc_id: ffi::t_docId) {
        self.doc_id = doc_id;
    }

    pub fn score(&self) -> f32 {
        self.score
    }

    pub fn set_score(&mut self, score: f32) {
        self.score = score;
    }

    pub fn score_explain(&self) -> Option<&ffi::RSScoreExplain> {
        self.score_explain.map(|s| unsafe { s.as_ref() })
    }

    pub fn score_explain_mut(&mut self) -> Option<&mut ffi::RSScoreExplain> {
        self.score_explain.map(|mut s| unsafe { s.as_mut() })
    }

    pub fn set_score_explain(&mut self, score_explain: Option<NonNull<ffi::RSScoreExplain>>) {
        self.score_explain = score_explain;
    }

    pub fn document_metadata(&self) -> Option<&ffi::RSDocumentMetadata> {
        self.document_metadata.as_deref()
    }

    pub fn set_document_metadata(&mut self, document_metadata: Option<DocumentMetadata>) {
        self.document_metadata = document_metadata;
    }

    pub fn index_result(&self) -> Option<&ffi::RSIndexResult> {
        self.index_result.map(|s| unsafe { s.as_ref() })
    }

    pub fn index_result_mut(&mut self) -> Option<&mut ffi::RSIndexResult> {
        self.index_result.map(|mut s| unsafe { s.as_mut() })
    }

    pub fn set_index_result(&mut self, index_result: Option<NonNull<ffi::RSIndexResult>>) {
        self.index_result = index_result;
    }

    pub fn row_data(&self) -> &RLookupRow<'a, RSValueFFI> {
        &self.row_data
    }

    pub fn row_data_mut(&mut self) -> &mut RLookupRow<'a, RSValueFFI> {
        &mut self.row_data
    }

    pub fn flags(&self) -> SearchResultFlags {
        self.flags
    }

    pub fn set_flags(&mut self, flags: SearchResultFlags) {
        self.flags = flags;
    }
}
