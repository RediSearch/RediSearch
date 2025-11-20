/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

pub mod bindings;

use enumflags2::{BitFlags, bitflags};
use inverted_index::RSIndexResult;
use std::ptr::{self, NonNull};

use crate::bindings::DocumentMetadata;

#[bitflags]
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum SearchResultFlag {
    ExpiredDoc = 1,
}

pub type SearchResultFlags = enumflags2::BitFlags<SearchResultFlag>;

/// SearchResult - the object all the processing chain is working on.
/// It holds the [`RSIndexResult`] which is what the index scan brought - scores, vectors, flags, etc,
/// and a list of fields loaded by the chain
#[derive(Debug)]
pub struct SearchResult<'index> {
    doc_id: ffi::t_docId,
    // not all results have score - TBD
    score: f64,

    /// Raw pointer to the [`ffi::RSScoreExplain`].
    ///
    /// # Safety
    ///
    /// The pointer must be a [valid] pointer to a [`ffi::RSScoreExplain`] and must
    /// **stay** valid for the entire lifetime of the returned [`SearchResult`].
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    // TODO resolve ownership (this is heap-allocated but owned by this search result??)
    score_explain: Option<NonNull<ffi::RSScoreExplain>>,

    document_metadata: Option<DocumentMetadata>,

    // index result should cover what you need for highlighting,
    // but we will add a method to duplicate index results to make
    // them thread safe
    index_result: Option<&'index RSIndexResult<'index>>,

    // Row data. Use RLookup_* functions to access
    row_data: ffi::RLookupRow,

    flags: SearchResultFlags,
}
unsafe_tools::impl_mimic!(SearchResult<'static>);

impl Drop for SearchResult<'_> {
    fn drop(&mut self) {
        self.clear();

        // Safety: we own (and therefore correctly initialized) the row data struct and have mutable access to it.
        unsafe {
            ffi::RLookupRow_Reset(ptr::from_mut(&mut self.row_data));
        }
    }
}

impl Default for SearchResult<'_> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'index> SearchResult<'index> {
    pub const fn new() -> Self {
        Self {
            doc_id: 0,
            score: 0.0,
            score_explain: None,
            document_metadata: None,
            index_result: None,
            row_data: ffi::RLookupRow {
                sv: ptr::null(),
                dyn_: ptr::null_mut(),
                ndyn: 0,
            },
            flags: SearchResultFlags::from_bits_truncate_c(0, BitFlags::CONST_TOKEN),
        }
    }

    /// Clears the search result, removing all values from the [`RLookupRow`][ffi::RLookupRow].
    /// This has no effect on the allocated capacity of the lookup row.
    pub fn clear(&mut self) {
        self.score = 0.0;

        if let Some(score_explain) = self.score_explain.take() {
            // Safety: the caller of `SearchResult::set_score_explain` promised the pointer is a valid pointer to a `RSScoreExplain`
            unsafe {
                ffi::SEDestroy(score_explain.as_ptr());
            }
        }

        // explicitly drop the DMD here to make clear we maintain the
        // same "drop order" as the old C implementation had.
        let _ = self.document_metadata.take();

        self.index_result = None;

        // Safety: we own (and therefore correctly initialized) the row data struct and have mutable access to it.
        unsafe {
            ffi::RLookupRow_Wipe(ptr::from_mut(&mut self.row_data));
        }

        self.flags = SearchResultFlags::empty();
    }

    /// Sets the document ID of this search result.
    pub const fn doc_id(&self) -> ffi::t_docId {
        self.doc_id
    }

    /// Sets the document ID of this search result.
    pub const fn set_doc_id(&mut self, doc_id: ffi::t_docId) {
        self.doc_id = doc_id;
    }

    /// Returns the score of this search result.
    pub const fn score(&self) -> f64 {
        self.score
    }

    /// Sets the score of this search result.
    pub const fn set_score(&mut self, score: f64) {
        self.score = score;
    }

    /// Returns an immutable reference to the [`ffi::RSScoreExplain`] associated with this search result.
    pub fn score_explain(&self) -> Option<&ffi::RSScoreExplain> {
        self.score_explain.map(|s| {
            // Safety: we expect the RSScoreExplain pointer to be valid (see SearchResult::set_score_explain)
            unsafe { s.as_ref() }
        })
    }

    /// Returns an immutable reference to the [`ffi::RSScoreExplain`] associated with this search result.
    pub fn score_explain_mut(&mut self) -> Option<&mut ffi::RSScoreExplain> {
        self.score_explain.map(|mut s| {
            // Safety: we expect the RSScoreExplain pointer to be valid (see SearchResult::set_score_explain)
            unsafe { s.as_mut() }
        })
    }

    /// Sets the [`ffi::RSScoreExplain`] associated with this search result.
    ///
    /// # Safety
    ///
    /// 1. `index_result` must be a [valid] pointer to a [`ffi::RSScoreExplain`] if non-null.
    /// 2. `index_result` must be [valid] for the entire lifetime of `self`.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub const unsafe fn set_score_explain(
        &mut self,
        score_explain: Option<NonNull<ffi::RSScoreExplain>>,
    ) {
        self.score_explain = score_explain;
    }

    /// Returns an immutable reference to the [`DocumentMetadata`] associated with this search result.
    pub fn document_metadata(&self) -> Option<&ffi::RSDocumentMetadata> {
        self.document_metadata.as_deref()
    }

    /// Sets the [`DocumentMetadata`] associated with this search result.
    pub fn set_document_metadata(&mut self, document_metadata: Option<DocumentMetadata>) {
        self.document_metadata = document_metadata;
    }

    /// Returns an immutable reference to the [`ffi::RSIndexResult`] associated with this search result.
    pub const fn index_result(&self) -> Option<&RSIndexResult<'index>> {
        self.index_result
    }

    /// Sets the [`ffi::RSIndexResult`] associated with this search result.
    pub const fn set_index_result(&mut self, index_result: Option<&'index RSIndexResult<'index>>) {
        self.index_result = index_result;
    }

    /// Returns an immutable reference to the [`RLookupRow`][ffi::RLookupRow] of this search result.
    pub const fn row_data(&self) -> &ffi::RLookupRow {
        &self.row_data
    }

    /// Returns a mutable reference to the [`RLookupRow`][ffi::RLookupRow] of this search result.
    pub const fn row_data_mut(&mut self) -> &mut ffi::RLookupRow {
        &mut self.row_data
    }

    /// Returns the [`SearchResultFlags`] of this search result.
    pub const fn flags(&self) -> SearchResultFlags {
        self.flags
    }

    /// Sets the [`SearchResultFlags`] of this search result.
    pub const fn set_flags(&mut self, flags: SearchResultFlags) {
        self.flags = flags;
    }
}
