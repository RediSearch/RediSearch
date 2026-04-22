/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::{self, NonNull};

use ffi::{GeoFilter, RSGlobalConfig};
use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};
use query_node_type::QueryNodeType;
use rqe_iterators::{
    interop::RQEIteratorWrapper,
    new_geo_range_iterator,
    union_reducer::{NewUnionIterator, new_union_iterator},
};

use crate::inverted_index::numeric::{NumericIterator, into_crqe_from_numeric_iters};

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
    // SAFETY: 3. guarantees gf is non-null and writable.
    let geo = unsafe { &mut *gf };
    // Read field_index before the mutable borrow in new_geo_range_iterator.
    // SAFETY: 3. guarantees geo.fieldSpec is a valid non-null pointer.
    let field_index = unsafe { (*geo.fieldSpec).index };
    let field_ctx = FieldFilterContext {
        field: FieldMaskOrIndex::Index(field_index),
        predicate: FieldExpirationPredicate::Default,
    };
    // SAFETY: 1. guarantees ctx is non-null.
    let sctx = unsafe { NonNull::new_unchecked(ctx as *mut ffi::RedisSearchCtx) };
    // SAFETY: RSGlobalConfig is initialised by the time any index is created.
    let compress = unsafe { RSGlobalConfig.numericCompress };
    // SAFETY: 4. guarantees config is valid and non-null.
    let min_union_iter_heap = unsafe { (*config).minUnionIterHeap } as usize;

    // SAFETY: caller upholds requirements 1–3.
    let Ok(groups) = (unsafe { new_geo_range_iterator(sctx, geo, &field_ctx, compress) }) else {
        return ptr::null_mut();
    };

    // C-Code: each NumericIterator must carry its NumericFilter so that
    // `NumericInvIndIterator_GetNumericFilter` can hand it back to C profiling code, which uses
    // the embedded `geo_filter` pointer to display the geo term as coordinates instead of raw
    // geohash values. Once profiling is fully ported to Rust, this wrapper can be dropped and
    // the variants can be used directly.
    let numeric_iters: Vec<NumericIterator<'_>> = groups
        .into_iter()
        .flat_map(|(filter_nn, variants)| {
            variants
                .into_iter()
                .map(move |v| NumericIterator::with_filter(filter_nn, v))
        })
        .collect();

    // Wrap children into RQEIteratorWrapper so C profiling code can traverse the tree.
    // TODO: simplify once profile.c is ported to Rust.
    match new_union_iterator(numeric_iters, true, min_union_iter_heap) {
        NewUnionIterator::ReducedEmpty(empty) => RQEIteratorWrapper::boxed_new(empty),
        NewUnionIterator::ReducedSingle(v) => RQEIteratorWrapper::boxed_new(v),
        NewUnionIterator::Flat(f) => crate::union::build_union_from_children(
            into_crqe_from_numeric_iters(f.into_children()),
            false,
            min_union_iter_heap,
            QueryNodeType::Geo,
            ptr::null(),
            1.0,
        ),
        NewUnionIterator::FlatQuick(f) => crate::union::build_union_from_children(
            into_crqe_from_numeric_iters(f.into_children()),
            true,
            min_union_iter_heap,
            QueryNodeType::Geo,
            ptr::null(),
            1.0,
        ),
        NewUnionIterator::Heap(h) => crate::union::build_union_from_children(
            into_crqe_from_numeric_iters(h.into_children()),
            false,
            min_union_iter_heap,
            QueryNodeType::Geo,
            ptr::null(),
            1.0,
        ),
        NewUnionIterator::HeapQuick(h) => crate::union::build_union_from_children(
            into_crqe_from_numeric_iters(h.into_children()),
            true,
            min_union_iter_heap,
            QueryNodeType::Geo,
            ptr::null(),
            1.0,
        ),
    }
}
