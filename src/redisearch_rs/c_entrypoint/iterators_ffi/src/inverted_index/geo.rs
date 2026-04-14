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
    interop::RQEIteratorWrapper, new_geo_range_iterator, union_reducer::NewUnionIterator,
};

use crate::inverted_index::numeric::into_crqe_children;

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

    // We're opening the union to wrap children into RQEIteratorWrapper, for profiling support
    // TODO: check if we can implement Profiled to NumericIteratorVariant and simplify this.
    // SAFETY: caller upholds requirements 1–3.
    match unsafe { new_geo_range_iterator(sctx, geo, &field_ctx, min_union_iter_heap, compress) } {
        None | Some(NewUnionIterator::ReducedEmpty(_)) => ptr::null_mut(),
        Some(NewUnionIterator::ReducedSingle(v)) => RQEIteratorWrapper::boxed_new(v),
        Some(NewUnionIterator::Flat(f)) => crate::union::build_union_from_children(
            into_crqe_children(f.into_children()),
            false,
            min_union_iter_heap,
            QueryNodeType::Geo,
            ptr::null(),
            1.0,
        ),
        Some(NewUnionIterator::FlatQuick(f)) => crate::union::build_union_from_children(
            into_crqe_children(f.into_children()),
            true,
            min_union_iter_heap,
            QueryNodeType::Geo,
            ptr::null(),
            1.0,
        ),
        Some(NewUnionIterator::Heap(h)) => crate::union::build_union_from_children(
            into_crqe_children(h.into_children()),
            false,
            min_union_iter_heap,
            QueryNodeType::Geo,
            ptr::null(),
            1.0,
        ),
        Some(NewUnionIterator::HeapQuick(h)) => crate::union::build_union_from_children(
            into_crqe_children(h.into_children()),
            true,
            min_union_iter_heap,
            QueryNodeType::Geo,
            ptr::null(),
            1.0,
        ),
    }
}
