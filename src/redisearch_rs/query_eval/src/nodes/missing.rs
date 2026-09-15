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
use rqe_iterators::inverted_index::new_missing_iterator;
use search_disk::SearchDiskHandle;

use crate::{EvalResult, QueryEvalContext};

/// `QN_MISSING` — matches documents where a field has no indexed value.
pub(crate) fn eval<'index>(
    ctx: &'index mut QueryEvalContext,
    fs: &ffi::FieldSpec,
) -> Option<EvalResult<'index>> {
    // SAFETY: the context owns a live spec whose disk handle, when non-null,
    // remains valid for the query's lifetime (QueryEvalContext invariants 1/2).
    if let Some(disk) = unsafe { SearchDiskHandle::new(ctx.spec().diskSpec) } {
        let snapshot = NonNull::new(ctx.sctx().diskSnapshot)
            .expect("query.sctx.diskSnapshot is null for a disk-backed missing query");
        // SAFETY: the spec and its query snapshot remain valid for `'index`,
        // enterprise iterators are registered for disk specs, and query
        // evaluation has exclusive access to the disk handle.
        return match unsafe { disk.new_missing_iterator(fs.index, snapshot) } {
            Ok(it) => Some(it),
            Err(err) => {
                ctx.status()
                    .set_error(QueryErrorCode::DiskIteratorCreation, &err.to_string());
                None
            }
        };
    }

    let spec = ctx.spec();

    // SAFETY: the query's spec owns a live missing-field dictionary created
    // with MissingFieldDictType. Indexing finishes its rehashing before readers
    // can access it, and query evaluation holds the spec lock.
    let missing = unsafe { Dict::<MissingFieldDictType>::from_raw(spec.missing.indexes) };
    // SAFETY: the query node references a schema field whose name remains valid
    // for the lookup; HiddenString is the dictionary's key type.
    let field_name = unsafe { HiddenString::from_raw(fs.fieldName) };
    let ii_ref = missing.fetch(field_name)?;

    // `ctx.sctx()` is a live reference, so the resulting pointer is never null.
    let sctx_nn = NonNull::from(ctx.sctx());

    // SAFETY: `new_missing_iterator`'s four preconditions hold here:
    // 1. `sctx` is a valid `RedisSearchCtx` with a non-null, valid `spec` —
    //    `QueryEvalContext` invariant (2).
    // 2. `fs.index` is a valid index into `spec.fields`: the query AST node
    //    references a field of this very spec, so its `FieldSpec::index` is in
    //    bounds (mirrors the C `Query_EvalMissingNode` using `fs->index`).
    // 3. `spec.missing.indexes` is a non-null, valid dict — initialised by
    //    `IndexSpec_MakeKeyless` for every queryable spec; it is also the dict
    //    we just fetched `ii_ref` from above.
    // 4. `ii_ref` uses `DocIdsOnly`/`RawDocIdsOnly` encoding: the indexer only
    //    ever stores doc-ids-only inverted indexes in `missing.indexes`.
    Some(unsafe { new_missing_iterator(ii_ref, sctx_nn, fs.index) })
}
