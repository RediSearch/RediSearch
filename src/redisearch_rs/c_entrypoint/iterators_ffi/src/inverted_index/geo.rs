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
use rqe_iterators::{IteratorsConfig, build_geo_range_iterator, free_geo_numeric_filters};

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
    config: *const IteratorsConfig,
) -> *mut ffi::QueryIterator {
    // SAFETY: 3. guarantees gf is non-null and writable.
    let geo = unsafe { &mut *gf };
    // SAFETY: 1. guarantees ctx is non-null.
    let sctx = unsafe { NonNull::new_unchecked(ctx as *mut ffi::RedisSearchCtx) };
    // SAFETY: 4. guarantees config is valid and non-null.
    let min_union_iter_heap = unsafe { (*config).min_union_iter_heap } as usize;
    // SAFETY: `RSGlobalConfig` is initialised by the time any index is created.
    let compress = unsafe { RSGlobalConfig.numericCompress };

    // SAFETY: preconditions 1–3 map directly to those of `build_geo_range_iterator`.
    unsafe { build_geo_range_iterator(sctx, geo, min_union_iter_heap, compress) }
        .map_or(ptr::null_mut(), NonNull::as_ptr)
}

/// Frees the `numericFilters` array that [`NewGeoRangeIterator`] populated on a
/// `GeoFilter`, together with the per-range `NumericFilter`s it owns.
///
/// The array is allocated in Rust (`build_geo_numeric_filters` boxes it), so it
/// must be released with the Rust allocator.
///
/// # Safety
///
/// `filters` must be NULL, or the array stored in `gf.numericFilters` by
/// [`NewGeoRangeIterator`] (i.e. by `build_geo_numeric_filters`) and not yet
/// freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoFilter_FreeNumericFilters(filters: *mut *mut ffi::NumericFilter) {
    // SAFETY: the safety contract is forwarded to `free_geo_numeric_filters`.
    unsafe { free_geo_numeric_filters(filters.cast()) };
}
