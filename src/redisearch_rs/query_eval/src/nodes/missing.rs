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

use rqe_core::FieldIndex;
use rqe_iterators::inverted_index::new_missing_iterator;

use crate::{EvalResult, QueryEvalContext};

/// `QN_MISSING` — matches documents where a field has no indexed value.
pub(crate) fn eval<'index>(
    ctx: &'index mut QueryEvalContext,
    field_index: FieldIndex,
) -> Option<EvalResult<'index>> {
    let spec = ctx.spec();

    debug_assert!(
        field_index < spec.numFields,
        "field_index must be within the spec's current field count"
    );
    // Re-derive the field's current pointer from the spec's field array via the
    // stable index captured at parse time, rather than trusting a pointer handed
    // in by the caller: under WORKERS>0, evaluation can run on a worker thread
    // well after parsing, and a concurrent FT.ALTER may have since reallocated
    // `IndexSpec.fields`, leaving any such pointer dangling (MOD-18367).
    // SAFETY: `field_index` is within `spec.numFields` (checked above), so this
    // stays within the bounds of the `numFields`-sized array `spec.fields` points to.
    let fs = unsafe { &*spec.fields.add(field_index as usize) };

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
    // 2. `field_index` is within `spec.fields`'s current bounds, checked above.
    // 3. `spec.missingFieldDict` is a non-null, valid dict — initialised by
    //    `IndexSpec_MakeKeyless` for every queryable spec; it is also the dict
    //    we just fetched `ii_ptr` from above.
    // 4. `ii_ref` uses `DocIdsOnly`/`RawDocIdsOnly` encoding: the indexer only
    //    ever stores doc-ids-only inverted indexes in `missingFieldDict`.
    Some(unsafe { new_missing_iterator(ii_ref, sctx_nn, field_index) })
}
