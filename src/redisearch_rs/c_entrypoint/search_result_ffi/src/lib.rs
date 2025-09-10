/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use inverted_index::RSIndexResult;
use search_result::{SearchResultFlags, bindings::DocumentMetadata};
use std::{
    mem,
    ptr::{self, NonNull},
};

/// cbindgen:ignore
pub type SearchResult = search_result::SearchResult<'static>;

/// Construct a new [`SearchResult`].
#[unsafe(no_mangle)]
pub const extern "C" fn SearchResult_New() -> unsafe_tools::Size80Align8 {
    SearchResult::new().into_mimic()
}

/// Overrides the contents of `dst` with those from `src` taking ownership of `src`.
/// Ensures proper cleanup of any existing data in `dst`.
///
/// # Safety
///
/// 1. `dst` must be a [valid], non-null pointer to a [`SearchResult`].
/// 2. `src` must be a [valid], non-null pointer to a [`SearchResult`].
/// 3. `src` must not be used again.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_Override(
    dst: Option<NonNull<SearchResult>>,
    src: Option<NonNull<SearchResult>>,
) {
    // Safety: ensured by caller (1.)
    let dst = unsafe { dst.unwrap().as_mut() };

    // Safety: ensured by caller (2.,3.)
    let _ = mem::replace(dst, unsafe { src.unwrap().read() });
}

/// Clears the [`SearchResult`] pointed to by `res`, removing all values from its [`RLookupRow`][ffi::RLookupRow].
/// This has no effect on the allocated capacity of the lookup row.
///
/// # Safety
///
/// 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_Clear(res: Option<NonNull<SearchResult>>) {
    // Safety: ensured by caller (1.)
    let res = unsafe { res.unwrap().as_mut() };

    res.clear();
}

/// Destroys the [`SearchResult`] pointed to by `res` releasing any resources owned by it.
/// This method takes ownership of the search result, therefore the pointer must **must not** be used again after this function is called.
///
/// # Safety
///
/// 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
/// 2. `res` **must not** be used again after this function is called.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_Destroy(res: Option<NonNull<SearchResult>>) {
    // Safety: ensured by caller (1.,2.)
    unsafe { res.unwrap().drop_in_place() };
}

/// Returns the document ID of `res`.
///
/// # Safety
///
/// 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_GetDocId(res: *const SearchResult) -> ffi::t_docId {
    // Safety: ensured by caller (1.)
    let res = unsafe { res.as_ref().unwrap() };

    res.doc_id()
}

/// Sets the document ID of `res`.
///
/// # Safety
///
/// 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_SetDocId(
    res: Option<NonNull<SearchResult>>,
    doc_id: ffi::t_docId,
) {
    // Safety: ensured by caller (1.)
    let res = unsafe { res.unwrap().as_mut() };

    res.set_doc_id(doc_id);
}

/// Returns the score of `res`.
///
/// # Safety
///
/// 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_GetScore(res: *const SearchResult) -> f64 {
    // Safety: ensured by caller (1.)
    let res = unsafe { res.as_ref().unwrap() };

    res.score()
}

/// Sets the score of `res`.
///
/// # Safety
///
/// 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_SetScore(res: Option<NonNull<SearchResult>>, score: f64) {
    // Safety: ensured by caller (1.)
    let res = unsafe { res.unwrap().as_mut() };

    res.set_score(score);
}

/// Returns an immutable pointer to the [`ffi::RSScoreExplain`] associated with `res`.
///
/// # Safety
///
/// 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_GetScoreExplain(
    res: *const SearchResult,
) -> *const ffi::RSScoreExplain {
    // Safety: ensured by caller (1.)
    let res = unsafe { res.as_ref().unwrap() };

    res.score_explain().map_or(ptr::null(), ptr::from_ref)
}

/// Returns a mutable pointer to the [`ffi::RSScoreExplain`] associated with `res`.
///
/// # Safety
///
/// 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_GetScoreExplainMut(
    res: Option<NonNull<SearchResult>>,
) -> Option<NonNull<ffi::RSScoreExplain>> {
    // Safety: ensured by caller (1.)
    let res = unsafe { res.unwrap().as_mut() };

    res.score_explain_mut().map(NonNull::from)
}

/// Sets the [`ffi::RSScoreExplain`] associated with `res`.
///
/// # Safety
///
/// 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
/// 2. `score_explain` must be a [valid] pointer to a [`ffi::RSScoreExplain`].
/// 3. `score_explain` must be [valid] for the entire lifetime of `res`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_SetScoreExplain(
    res: Option<NonNull<SearchResult>>,
    score_explain: Option<NonNull<ffi::RSScoreExplain>>,
) {
    // Safety: ensured by caller (1.)
    let res = unsafe { res.unwrap().as_mut() };

    // Safety: ensured by caller (2.,3.)
    unsafe {
        res.set_score_explain(score_explain);
    }
}

/// Returns an immutable reference to the [`ffi::RSDocumentMetadata`] associated with `res`.
///
/// # Safety
///
/// 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_GetDocumentMetadata(
    res: *const SearchResult,
) -> *const ffi::RSDocumentMetadata {
    // Safety: ensured by caller (1.)
    let res = unsafe { res.as_ref().unwrap() };

    res.document_metadata().map_or(ptr::null(), ptr::from_ref)
}

/// Sets the [`ffi::RSDocumentMetadata`] associated with `res`.
///
/// # Safety
///
/// 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
/// 2. `document_metadata` must be a [valid] pointer to a [`ffi::RSDocumentMetadata`].
/// 3. `document_metadata` must be not be mutated for the entire lifetime of `res`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_SetDocumentMetadata(
    res: Option<NonNull<SearchResult>>,
    document_metadata: *const ffi::RSDocumentMetadata,
) {
    // Safety: ensured by caller (1.)
    let res = unsafe { res.unwrap().as_mut() };

    let document_metadata = NonNull::new(document_metadata.cast_mut()).map(|ptr| {
        // Safety: ensured by caller (2.,3.)
        unsafe { DocumentMetadata::from_raw(ptr) }
    });

    res.set_document_metadata(document_metadata);
}

/// Returns an immutable pointer to the [`RSIndexResult`] associated with `res`.
///
/// # Safety
///
/// 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_GetIndexResult(
    res: *const SearchResult,
) -> *const RSIndexResult<'static> {
    // Safety: ensured by caller (1.)
    let res = unsafe { res.as_ref().unwrap() };

    res.index_result().map_or(ptr::null(), ptr::from_ref)
}

/// Sets the [`RSIndexResult`] associated with `res`.
///
/// # Safety
///
/// 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_HasIndexResult(res: *const SearchResult) -> bool {
    // Safety: ensured by caller (1.)
    let res = unsafe { res.as_ref().unwrap() };

    res.index_result().is_some()
}

/// Sets the [`RSIndexResult`] associated with `res`.
///
/// # Safety
///
/// 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
/// 2. `index_result` must be a [valid] pointer to a [`ffi::RSIndexResult`].
/// 3. `index_result` must be [valid] for the entire lifetime of `res`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_SetIndexResult(
    res: Option<NonNull<SearchResult>>,
    index_result: *const RSIndexResult<'static>,
) {
    // Safety: ensured by caller (1.)
    let res = unsafe { res.unwrap().as_mut() };

    // Safety: ensured by caller (2.,3.)
    let index_result = unsafe { index_result.as_ref() };

    res.set_index_result(index_result);
}

/// Sets the [`RLookupRow`][ffi::RLookupRow] of `res`.
///
/// # Safety
///
/// 1. `res` must be a correctly initialized [`RLookupRow`][ffi::RLookupRow].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_SetRowData(
    res: Option<NonNull<SearchResult>>,
    row_data: ffi::RLookupRow,
) {
    // Safety: ensured by caller (1.)
    let res = unsafe { res.unwrap().as_mut() };

    res.set_row_data(row_data);
}

/// Returns an immutable pointer to the [`RLookupRow`][ffi::RLookupRow] of `res`.
///
/// # Safety
///
/// 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_GetRowData(
    res: *const SearchResult,
) -> *const ffi::RLookupRow {
    // Safety: ensured by caller (1.)
    let res = unsafe { res.as_ref().unwrap() };

    ptr::from_ref(res.row_data())
}

/// Returns a mutable pointer to the [`RLookupRow`][ffi::RLookupRow] of `res`.
///
/// # Safety
///
/// 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_GetRowDataMut(
    res: Option<NonNull<SearchResult>>,
) -> Option<NonNull<ffi::RLookupRow>> {
    // Safety: ensured by caller (1.)
    let res = unsafe { res.unwrap().as_mut() };

    Some(NonNull::from(res.row_data_mut()))
}

/// Returns the [`SearchResultFlags`] of `res`.
///
/// # Safety
///
/// 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_GetFlags(res: *const SearchResult) -> u8 {
    // Safety: ensured by caller (1.)
    let res = unsafe { res.as_ref().unwrap() };

    res.flags().bits()
}

/// Sets the [`SearchResultFlags`] of `res`.
///
/// # Safety
///
/// 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_SetFlags(res: Option<NonNull<SearchResult>>, flags: u8) {
    // Safety: ensured by caller (1.)
    let res = unsafe { res.unwrap().as_mut() };

    let flags = SearchResultFlags::from_bits_truncate(flags);

    res.set_flags(flags);
}

/// Merge the flags (union) `other` into `res`
///
/// # Safety
///
/// 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
/// 2. `other` must be a [valid], non-null pointer to a [`SearchResult`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_MergeFlags(
    res: Option<NonNull<SearchResult>>,
    other: *const SearchResult,
) {
    // Safety: ensured by caller (1.)
    let res = unsafe { res.expect("`res` must not be null").as_mut() };
    // Safety: ensured by caller (2.)
    let other = unsafe { other.as_ref().expect("`other` must not be null") };

    res.set_flags(res.flags() | other.flags());
}

/// Moves the contents the [`SearchResult`] pointed to by `res` into a new heap allocation.
/// This method takes ownership of the search result, therefore the pointer must **must not** be used again after this function is called.
///
/// # Safety
///
/// 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
/// 2. `res` **must not** be used again after this function is called.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_AllocateMove(
    res: Option<NonNull<SearchResult>>,
) -> *mut SearchResult {
    // Safety: ensured by caller (1.)
    let res = unsafe { res.unwrap().read() };

    let res = Box::new(res);
    Box::into_raw(res)
}

/// Destroys the [`SearchResult`] pointed to by `res` releasing any resources owned by it.
/// This method takes ownership of the search result, therefore the pointer must **must not** be used again after this function is called.
///
/// # Safety
///
/// 1. `res` must be a [valid], non-null pointer to a [`SearchResult`].
/// 2. `res` **must not** be used again after this function is called.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_DeallocateDestroy(res: Option<NonNull<SearchResult>>) {
    // Safety: ensured by caller (1.,2.)
    let res = unsafe { Box::from_raw(res.unwrap().as_ptr()) };
    drop(res);
}
