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

use dict::{Dict, MissingFieldDictType};
use hidden_string::HiddenString;
use query_error::QueryErrorCode;
use rqe_core::FieldIndex;
use rqe_iterators::inverted_index::new_missing_iterator;
use search_disk::SearchDiskHandle;

use crate::{EvalResult, QueryEvalContext};

/// `QN_MISSING` — matches documents where a field has no indexed value.
pub(crate) fn eval<'index>(
    ctx: &'index mut QueryEvalContext,
    field_index: FieldIndex,
) -> Option<EvalResult<'index>> {
    // SAFETY: the context owns a live spec whose disk handle, when non-null,
    // remains valid for the query's lifetime (QueryEvalContext invariants 1/2).
    if let Some(disk) = unsafe { SearchDiskHandle::new(ctx.spec().diskSpec) } {
        let snapshot = NonNull::new(ctx.sctx().diskSnapshot)
            .expect("query.sctx.diskSnapshot is null for a disk-backed missing query");
        // SAFETY: the spec and its query snapshot remain valid for `'index`,
        // enterprise iterators are registered for disk specs, and query
        // evaluation has exclusive access to the disk handle.
        return match unsafe { disk.new_missing_iterator(field_index, snapshot) } {
            Ok(it) => Some(it),
            Err(err) => {
                ctx.status()
                    .set_error(QueryErrorCode::DiskIteratorCreation, &err.to_string());
                None
            }
        };
    }

    let spec = ctx.spec();

    // `field_index` is `RS_INVALID_FIELD_INDEX` when this node was parsed with no
    // local spec (e.g. a coordinator shard); nothing upstream stops it from
    // reaching evaluation against a different, spec-bearing context.
    debug_assert!(
        field_index < spec.numFields,
        "field_index must be within the spec's current field count"
    );
    // Re-derive the field's current pointer from the spec's field array via the
    // stable index captured at parse time, rather than trusting a pointer handed
    // in by the caller, which may already be freed.
    // SAFETY: `field_index` is within `spec.numFields` (checked above), so this
    // stays within the bounds of the `numFields`-sized array `spec.fields` points to.
    let field_ptr = unsafe { spec.fields.add(field_index as usize) };
    // SAFETY: `field_ptr` was just derived above and stays within `spec.fields`'s bounds.
    let fs = unsafe { &*field_ptr };

    // SAFETY: the query's spec owns a live missing-field dictionary created
    // with MissingFieldDictType. Indexing finishes its rehashing before readers
    // can access it, and query evaluation holds the spec lock.
    let missing = unsafe { Dict::<MissingFieldDictType>::from_raw(spec.missing.indexes) };
    // SAFETY: `fs` was just re-derived above and is a valid, non-dangling
    // `FieldSpec`, so its `fieldName` is a valid `HiddenString` key.
    let field_name = unsafe { HiddenString::from_raw(fs.fieldName) };
    let ii_ref = missing.fetch(field_name)?;

    // `ctx.sctx()` is a live reference, so the resulting pointer is never null.
    let sctx_nn = NonNull::from(ctx.sctx());

    // SAFETY: `new_missing_iterator`'s four preconditions hold here:
    // 1. `sctx` is a valid `RedisSearchCtx` with a non-null, valid `spec` —
    //    `QueryEvalContext` invariant (2).
    // 2. `field_index` is within `spec.fields`'s current bounds, checked above.
    // 3. `spec.missing.indexes` is a non-null, valid dict — initialised by
    //    `IndexSpec_MakeKeyless` for every queryable spec; it is also the dict
    //    we just fetched `ii_ref` from above.
    // 4. `ii_ref` uses `DocIdsOnly`/`RawDocIdsOnly` encoding: the indexer only
    //    ever stores doc-ids-only inverted indexes in `missing.indexes`.
    Some(unsafe { new_missing_iterator(ii_ref, sctx_nn, field_index) })
}
