/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_GEOMETRY` query nodes.

use std::{ffi::CStr, ptr::NonNull};

use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};

use crate::{Evaluated, QueryEvalContext};

/// `QN_GEOMETRY` — a geometry (WKT/GeoJSON) spatial predicate on a GEOSHAPE
/// field.
///
/// The GEOSHAPE index is a C++ R-tree exposed through a C API
/// ([`GeometryApi`](ffi::GeometryApi)); this evaluator opens the field's index
/// and dispatches to its `query` callback. On a query error the API returns
/// NULL and an error string, which is reported into the query status; the
/// iterator is `None`.
pub(crate) fn eval<'index>(
    ctx: &'index mut QueryEvalContext,
    geomq: *mut ffi::GeometryQuery,
) -> Option<Evaluated<'index>> {
    debug_assert!(
        !geomq.is_null(),
        "geometry node must carry a geometry query"
    );
    // SAFETY: a well-formed geometry node carries a valid, non-null
    // `GeometryQuery` whose `fs` is a valid, non-null `FieldSpec`.
    let gq = unsafe { &*geomq };
    let fs = gq.fs;
    debug_assert!(!fs.is_null(), "geometry query must have a field spec");
    // SAFETY: `fs` is a valid, non-null `FieldSpec`.
    let field_index = unsafe { (*fs).index };

    // TODO: pass `false` (don't create the index if missing) once the query
    // string is validated before reaching this evaluator. Today, if the index
    // has not been created yet and the query is invalid, not creating it would
    // return results as if the index were empty instead of raising an error, so
    // we create it eagerly to force the error path.
    //
    // SAFETY: `fs` is a valid `FieldSpec`. `OpenGeometryIndex` does not keep the
    // pointer; with create-if-missing it may mutate `fs` to lazily attach the
    // index, which is sound here because no live Rust borrow aliases `fs` (`gq`
    // borrows the `GeometryQuery`, a separate allocation).
    let index = unsafe { ffi::OpenGeometryIndex(fs.cast_mut(), true) };
    debug_assert!(
        !index.is_null(),
        "OpenGeometryIndex with create-if-missing must return a valid index"
    );
    // SAFETY: `index` is a valid `GeometryIndex` from `OpenGeometryIndex`.
    let api = unsafe { ffi::GeometryApi_Get(index) };
    debug_assert!(!api.is_null(), "GeometryApi_Get must return a valid api");

    let field_ctx = FieldFilterContext {
        field: FieldMaskOrIndex::Index(field_index),
        predicate: FieldExpirationPredicate::Default,
    };
    let sctx = ctx.sctx_ptr();
    let mut err_msg: *mut redis_module::RedisModuleString = std::ptr::null_mut();

    // SAFETY: `api` is a valid `GeometryApi` and its `query` callback is always
    // populated by `GeometryApi_Get`.
    let query_fn = unsafe { (*api).query }.expect("geometry api `query` must be set");
    // SAFETY: all pointers are valid for the duration of the call: `sctx` and
    // `field_ctx` outlive it, `index` is valid, `gq.str` points to `gq.str_len`
    // bytes, and `err_msg` is a valid out-pointer.
    let ret = unsafe {
        query_fn(
            sctx,
            std::ptr::from_ref(&field_ctx).cast(),
            index,
            gq.query_type,
            gq.format,
            gq.str_,
            gq.str_len,
            &mut err_msg,
        )
    };

    if ret.is_null() {
        let detail = if let Some(err) = NonNull::new(err_msg) {
            // SAFETY: these Redis API function-pointer are set once
            // during module load and never mutated afterwards, so reading them
            // during query evaluation cannot race.
            let string_ptr_len = unsafe { redis_module::RedisModule_StringPtrLen }
                .expect("RedisModule_StringPtrLen unset");
            // SAFETY: set once at module load, never mutated afterwards (see above).
            let free_string = unsafe { redis_module::RedisModule_FreeString }
                .expect("RedisModule_FreeString unset");
            // SAFETY: `err` is a valid `RedisModuleString` returned by the query.
            let str_ptr = unsafe { string_ptr_len(err.as_ptr(), std::ptr::null_mut()) };
            // SAFETY: `str_ptr` is a valid, NUL-terminated C string.
            let s = unsafe { CStr::from_ptr(str_ptr) }
                .to_string_lossy()
                .into_owned();
            // SAFETY: `err` was allocated by the query and is freed exactly once.
            unsafe { free_string(std::ptr::null_mut(), err.as_ptr()) };
            s
        } else {
            String::new()
        };
        ctx.status().set_with_user_data(
            query_error::QueryErrorCode::BadVal,
            "Error querying geoshape index",
            &format!(": {detail}"),
        );
    }

    NonNull::new(ret).map(Evaluated::C)
}
