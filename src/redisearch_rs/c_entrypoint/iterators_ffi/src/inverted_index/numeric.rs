/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr;

use ffi::{FieldSpec, RSGlobalConfig};
use field::FieldFilterContext;
use inverted_index::NumericFilter;
use rqe_iterators::{IteratorsConfig, open_numeric_or_geo_index};

///
/// # Safety
///
/// 1. `spec` must be a valid non-null pointer to an [`ffi::IndexSpec`].
/// 2. `fs` must be a valid non-null pointer to a [`FieldSpec`] for a numeric or geo field.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn openNumericOrGeoIndex(
    spec: *mut ffi::IndexSpec,
    fs: *mut FieldSpec,
    create_if_missing: bool,
) -> *mut ffi::NumericRangeTree {
    debug_assert!(!spec.is_null());
    debug_assert!(!fs.is_null());
    // SAFETY: 1. guarantees spec is valid and non-null.
    let spec = unsafe { &mut *spec };
    // SAFETY: 2. guarantees fs is valid and non-null.
    let fs = unsafe { &mut *fs };

    // SAFETY: RSGlobalConfig is initialised by the time any index is created.
    let compress = unsafe { RSGlobalConfig.numericCompress };
    // SAFETY: 1. and 2. are forwarded from this function's safety contract.
    match unsafe { open_numeric_or_geo_index(spec, fs, create_if_missing, compress) } {
        Some(tree) => std::ptr::from_mut(tree).cast::<ffi::NumericRangeTree>(),
        None => ptr::null_mut(),
    }
}

/// Opens the numeric/geo index and creates an iterator over all matching sub-ranges.
///
/// # Returns
///
/// - `NULL` if the index doesn't exist for this field (i.e., no documents have been indexed
///   for it yet).
/// - `NULL` if no sub-ranges in the tree match the filter.
/// - A single iterator if exactly one sub-range matches.
/// - A union iterator over all matching sub-ranges otherwise.
///
/// # Safety
///
/// 1. `ctx` must be a valid non-NULL pointer to a [`ffi::RedisSearchCtx`], remaining valid
///    for the lifetime of the returned iterator.
/// 2. `ctx.spec` must be a valid non-NULL pointer to an [`ffi::IndexSpec`].
/// 3. `flt` must be a valid non-NULL pointer to a [`NumericFilter`] whose `field_spec` field
///    is a valid non-NULL pointer to a [`FieldSpec`], remaining valid for the lifetime of the
///    returned iterator.
/// 4. `config` must be a valid non-NULL pointer to an [`IteratorsConfig`].
/// 5. `filter_ctx` must be a valid non-NULL pointer to a [`FieldFilterContext`] with a field
///    index (not a field mask).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewNumericFilterIterator(
    ctx: *const ffi::RedisSearchCtx,
    flt: *const NumericFilter,
    _for_type: ffi::FieldType,
    config: *const IteratorsConfig,
    filter_ctx: *const FieldFilterContext,
) -> *mut ffi::QueryIterator {
    debug_assert!(!ctx.is_null());
    debug_assert!(!flt.is_null());

    // SAFETY: 3. guarantees flt is valid and non-null.
    let flt_ref = unsafe { &*flt };
    // SAFETY: 4. guarantees config is valid and non-null.
    let min_union_iter_heap = unsafe { (*config).min_union_iter_heap } as usize;
    // SAFETY: 1. guarantees ctx is valid and non-null.
    let sctx = unsafe { &*ctx };
    // SAFETY: 5. guarantees filter_ctx is valid and non-null.
    let field_ctx = unsafe { &*filter_ctx };

    // SAFETY: preconditions 1–3/5 map directly to those of
    // `build_numeric_filter_iterator`.
    unsafe {
        rqe_iterators::build_numeric_filter_iterator(sctx, flt_ref, min_union_iter_heap, field_ctx)
    }
}
