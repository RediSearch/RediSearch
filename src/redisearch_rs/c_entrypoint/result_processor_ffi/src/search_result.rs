/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use result_processor::{bindings::DocumentMetadata, search_result::SearchResultFlags};
use std::ptr::{self, NonNull};
use value::RSValueFFI;

pub type SearchResult = result_processor::search_result::SearchResult<'static>;

/// cbindgen:ignore
type RLookupRow = rlookup::RLookupRow<'static, RSValueFFI>;

// /**
//  * This function allocates a new SearchResult, copies the data from `src` to it,
//  * and returns it.
// */
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_Copy(
    res: Option<NonNull<SearchResult>>,
) -> *mut SearchResult {
    let res = unsafe { res.unwrap().as_mut() };

    let dst = Box::new(res.clone());
    Box::into_raw(dst)
}

// // Overwrites the contents of 'dst' with those from 'src'.
// // Ensures proper cleanup of any existing data in 'dst'.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_Override(
    dst: Option<NonNull<SearchResult>>,
    src: Option<NonNull<SearchResult>>,
) {
    let dst = unsafe { dst.unwrap().as_mut() };

    let src = unsafe { src.unwrap().as_ref() };

    dst.override_with(src);
}

// /**
//  * This function resets the search result, so that it may be reused again.
//  * Internal caches are reset but not freed
//  */
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_Clear(res: Option<NonNull<SearchResult>>) {
    let res = unsafe { res.unwrap().as_mut() };

    res.clear();
}

// /**
//  * This function clears the search result, also freeing its internals. Internal
//  * caches are freed. Use this function if `r` will not be used again.
//  */
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_Destroy(res: Option<NonNull<SearchResult>>) {
    let _res = unsafe { res.unwrap().as_mut() };

    todo!()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_GetDocId(res: *const SearchResult) -> ffi::t_docId {
    let res = unsafe { res.as_ref().unwrap() };

    res.doc_id()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_SetDocId(
    res: Option<NonNull<SearchResult>>,
    doc_id: ffi::t_docId,
) {
    let res = unsafe { res.unwrap().as_mut() };

    res.set_doc_id(doc_id);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_GetScore(res: *const SearchResult) -> f32 {
    let res = unsafe { res.as_ref().unwrap() };

    res.score()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_SetScore(res: Option<NonNull<SearchResult>>, score: f32) {
    let res = unsafe { res.unwrap().as_mut() };

    res.set_score(score);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_GetScoreExplain(
    res: *const SearchResult,
) -> *const ffi::RSScoreExplain {
    let res = unsafe { res.as_ref().unwrap() };

    res.score_explain().map_or(ptr::null(), ptr::from_ref)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_GetScoreExplainMut(
    res: Option<NonNull<SearchResult>>,
) -> Option<NonNull<ffi::RSScoreExplain>> {
    let res = unsafe { res.unwrap().as_mut() };

    res.score_explain_mut().map(NonNull::from)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_SetScoreExplain(
    res: Option<NonNull<SearchResult>>,
    score_explain: Option<NonNull<ffi::RSScoreExplain>>,
) {
    let res = unsafe { res.unwrap().as_mut() };

    res.set_score_explain(score_explain);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_GetDocumentMetadata(
    res: *const SearchResult,
) -> *const ffi::RSDocumentMetadata {
    let res = unsafe { res.as_ref().unwrap() };

    res.document_metadata().map_or(ptr::null(), ptr::from_ref)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_SetDocumentMetadata(
    res: Option<NonNull<SearchResult>>,
    document_metadata: *const ffi::RSDocumentMetadata,
) {
    let res = unsafe { res.unwrap().as_mut() };

    let document_metadata = NonNull::new(document_metadata.cast_mut())
        .map(|ptr| unsafe { DocumentMetadata::from_raw(ptr) });

    res.set_document_metadata(document_metadata);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_GetIndexResult(
    res: *const SearchResult,
) -> *const ffi::RSIndexResult {
    let res = unsafe { res.as_ref().unwrap() };

    res.index_result().map_or(ptr::null(), ptr::from_ref)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_GetIndexResultMut(
    res: Option<NonNull<SearchResult>>,
) -> Option<NonNull<ffi::RSIndexResult>> {
    let res = unsafe { res.unwrap().as_mut() };

    res.index_result_mut().map(NonNull::from)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_HasIndexResult(res: *const SearchResult) -> bool {
    let res = unsafe { res.as_ref().unwrap() };

    res.index_result().is_some()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_SetIndexResult(
    res: Option<NonNull<SearchResult>>,
    index_result: Option<NonNull<ffi::RSIndexResult>>,
) {
    let res = unsafe { res.unwrap().as_mut() };

    res.set_index_result(index_result);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_GetRowData(res: *const SearchResult) -> *const RLookupRow {
    let res = unsafe { res.as_ref().unwrap() };

    ptr::from_ref(res.row_data()).cast::<RLookupRow>()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_GetRowDataMut(
    res: Option<NonNull<SearchResult>>,
) -> Option<NonNull<RLookupRow>> {
    let res = unsafe { res.unwrap().as_mut() };

    Some(NonNull::from(res.row_data_mut()))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_GetFlags(res: *const SearchResult) -> u8 {
    let res = unsafe { res.as_ref().unwrap() };

    res.flags().bits()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_SetFlags(res: Option<NonNull<SearchResult>>, flags: u8) {
    let res = unsafe { res.unwrap().as_mut() };

    let flags = SearchResultFlags::from_bits_truncate(flags);

    res.set_flags(flags);
}
