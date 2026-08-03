/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_MISSING` query nodes.

use std::ptr::NonNull;

use rqe_iterators::inverted_index::new_missing_iterator;

use crate::{EvalResult, QueryEvalContext};

/// `QN_MISSING` — matches documents where a field has no indexed value.
pub(crate) fn eval<'index>(
    ctx: &'index mut QueryEvalContext,
    fs: &ffi::FieldSpec,
) -> Option<EvalResult<'index>> {
    let spec = ctx.spec();

    // SAFETY: `spec` is valid (`QueryEvalContext::new` invariant 2), and any
    // queryable spec has its `missingFieldDict` initialised by
    // `IndexSpec_MakeKeyless`, so the pointer is a valid dict; `fs.fieldName`
    // is a valid `HiddenString` key, matching the C `Query_EvalMissingNode`.
    let ii_ptr = unsafe { ffi::RS_dictFetchValue(spec.missingFieldDict, fs.fieldName as *mut _) };

    if ii_ptr.is_null() {
        // There are no missing values for this field.
        return None;
    }

    let ii_ptr: *const inverted_index::opaque::InvertedIndex = ii_ptr.cast();
    // SAFETY: `ii_ptr` is a valid `InvertedIndex` obtained from the
    // missing-field dict (non-null checked above).
    let ii_ref = unsafe { &*ii_ptr };

    // `ctx.sctx()` is a live reference, so the resulting pointer is never null.
    let sctx_nn = NonNull::from(ctx.sctx());

    // SAFETY: `new_missing_iterator`'s four preconditions hold here:
    // 1. `sctx` is a valid `RedisSearchCtx` with a non-null, valid `spec` —
    //    `QueryEvalContext` invariant (2).
    // 2. `fs.index` is a valid index into `spec.fields`: the query AST node
    //    references a field of this very spec, so its `FieldSpec::index` is in
    //    bounds (mirrors the C `Query_EvalMissingNode` using `fs->index`).
    // 3. `spec.missingFieldDict` is a non-null, valid dict — initialised by
    //    `IndexSpec_MakeKeyless` for every queryable spec; it is also the dict
    //    we just fetched `ii_ptr` from above.
    // 4. `ii_ref` uses `DocIdsOnly`/`RawDocIdsOnly` encoding: the indexer only
    //    ever stores doc-ids-only inverted indexes in `missingFieldDict`.
    Some(unsafe { new_missing_iterator(ii_ref, sctx_nn, fs.index) })
}
