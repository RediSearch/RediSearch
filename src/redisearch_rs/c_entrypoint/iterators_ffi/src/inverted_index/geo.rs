/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::{self, NonNull};

use ffi::{FieldSpec, GeoFilter, RSGlobalConfig};
use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};
use query_node_type::QueryNodeType;
use rqe_iterators::{build_geo_numeric_filters, c2rust::CRQEIterator, open_numeric_or_geo_index};

use super::numeric::create_numeric_iterator_c;

/// Creates an iterator over all geo-encoded index entries within the radius specified by `gf`.
///
/// Geo fields are stored as sorted numeric geohash values. A radius query maps to up to 9
/// contiguous geohash ranges (the cell containing the centre point and its 8 neighbours).
/// Each range is queried via the numeric range tree; per-record distance filtering is applied
/// by `FilterGeoReader` in the `inverted_index` crate.
///
/// # Safety
///
/// 1. `ctx` must be a valid non-NULL pointer to a `RedisSearchCtx`, remaining valid for the
///    lifetime of all returned iterators.
/// 2. `ctx.spec` must be a valid non-NULL pointer to an `IndexSpec`.
/// 3. `gf` must be a valid non-NULL pointer to a `GeoFilter`.
///    - `gf.fieldSpec` must be a valid non-NULL pointer to a `FieldSpec`.
///    - `gf.numericFilters` must be NULL on entry; it is populated by this function and
///      freed by `GeoFilter_Free`.
/// 4. `config` must be a valid non-NULL pointer to an `IteratorsConfig`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewGeoRangeIterator(
    ctx: *const ffi::RedisSearchCtx,
    gf: *mut GeoFilter,
    config: *const ffi::IteratorsConfig,
) -> *mut ffi::QueryIterator {
    // SAFETY: 3. guarantees gf is non-null and writable; fieldSpec is valid and non-null.
    let geo = unsafe { &mut *gf };
    // Read fieldSpec and field index before the mutable borrow in build_geo_numeric_filters.
    // SAFETY: 3. guarantees geo.fieldSpec is a valid non-null pointer.
    let fs = unsafe { &mut *(geo.fieldSpec as *mut FieldSpec) };
    let field_index = fs.index;

    // SAFETY: 3.
    let filters = match unsafe { build_geo_numeric_filters(geo) } {
        Some(f) => f,
        None => return ptr::null_mut(),
    };

    // Open the numeric/geo index once for all ranges.
    // SAFETY: 1. guarantees ctx is valid and non-null.
    let ctx_ref = unsafe { &*ctx };
    // SAFETY: 2. guarantees ctx.spec is valid and non-null.
    let spec = unsafe { &mut *ctx_ref.spec };
    // SAFETY: RSGlobalConfig is initialised by the time any index is created.
    let compress = unsafe { RSGlobalConfig.numericCompress };
    // SAFETY: 1.–2. are forwarded from this function's safety contract.
    let Some(tree) = (unsafe { open_numeric_or_geo_index(spec, fs, false, compress) }) else {
        return ptr::null_mut();
    };

    let field_ctx = FieldFilterContext {
        field: FieldMaskOrIndex::Index(field_index),
        predicate: FieldExpirationPredicate::Default,
    };
    // SAFETY: 1. guarantees ctx is non-null.
    let sctx = unsafe { NonNull::new_unchecked(ctx as *mut ffi::RedisSearchCtx) };

    // SAFETY: 4. guarantees config is valid and non-null.
    let min_union_iter_heap = unsafe { (*config).minUnionIterHeap } as usize;

    let children: Vec<CRQEIterator> = filters
        .iter()
        .filter_map(|filter| {
            // SAFETY: tree, sctx, filter, and field_ctx all satisfy create_numeric_iterator_c's contract.
            let ptr = unsafe { create_numeric_iterator_c(sctx, tree, filter, config, &field_ctx) };
            NonNull::new(ptr).map(|ptr| {
                // SAFETY: ptr is a valid, uniquely-owned QueryIterator returned by create_numeric_iterator_c.
                unsafe { CRQEIterator::new(ptr) }
            })
        })
        .collect();

    if children.is_empty() {
        return ptr::null_mut();
    }

    crate::union::build_union_from_children(
        children,
        true,
        min_union_iter_heap,
        QueryNodeType::Geo,
        ptr::null(),
        1.0,
    )
}
