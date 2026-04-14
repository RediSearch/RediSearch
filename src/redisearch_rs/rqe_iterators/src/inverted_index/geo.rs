/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ffi::{
    GEO_LAT_MAX, GEO_LAT_MIN, GEO_LONG_MAX, GEO_LONG_MIN, GEO_RANGE_COUNT, GeoDistance, GeoFilter,
    GeoHashRange, calcRanges,
};
use field::FieldFilterContext;
use inverted_index::NumericFilter;

use crate::{
    NumericIteratorVariant, open_numeric_or_geo_index,
    union_reducer::{NewUnionIterator, new_union_iterator},
};

/// Validates `gf`'s parameters, computes the geohash ranges covering the requested circle,
/// allocates a per-range [`NumericFilter`] for each non-trivial range, stores all of them in
/// `gf.numericFilters`, and returns references to the non-trivial filters.
///
/// Returns `None` if `gf`'s parameters are invalid (bad radius, lat, or lon).
///
/// The returned references are valid for `'index` because the filters are owned by
/// `gf.numericFilters`, which lives as long as `gf`.
///
/// # Safety
///
/// 1. `gf.fieldSpec` must be a valid non-null pointer to a [`ffi::FieldSpec`], valid for `'index`.
/// 2. `gf.numericFilters` must be NULL on entry; ownership of the allocated array is transferred
///    to `*gf` and must be released by `GeoFilter_Free`.
pub unsafe fn build_geo_numeric_filters<'index>(
    gf: &'index mut GeoFilter,
) -> Option<Vec<&'index NumericFilter>> {
    if gf.radius <= 0.0
        || gf.lon > f64::from(GEO_LONG_MAX)
        || gf.lon < f64::from(GEO_LONG_MIN)
        || gf.lat > GEO_LAT_MAX
        || gf.lat < GEO_LAT_MIN
    {
        return None;
    }

    let radius_meters = gf.radius * extract_geo_unit_factor(gf.unitType);
    let mut ranges = [GeoHashRange { min: 0.0, max: 0.0 }; GEO_RANGE_COUNT as usize];
    // SAFETY: ranges is a stack array of exactly GEO_RANGE_COUNT elements.
    unsafe { calcRanges(gf.lon, gf.lat, radius_meters, ranges.as_mut_ptr()) };

    // Allocate the numericFilters array and hand ownership to *gf so that
    // GeoFilter_Free → NumericFilter_Free → rm_free can clean up each entry.
    let numeric_filters = Box::into_raw(Box::new(
        [std::ptr::null_mut::<NumericFilter>(); GEO_RANGE_COUNT as usize],
    ));
    // SAFETY: 2. guarantees gf.numericFilters is NULL and writable.
    gf.numericFilters = numeric_filters.cast();

    let mut filters: Vec<&'index NumericFilter> = Vec::new();
    for (ii, range) in ranges.iter().enumerate() {
        if range.min == range.max {
            continue;
        }
        let filt_ptr = Box::into_raw(Box::new(NumericFilter {
            min: range.min,
            max: range.max,
            field_spec: gf.fieldSpec,
            geo_filter: (gf as *const GeoFilter).cast(),
            min_inclusive: true,
            max_inclusive: true,
            ascending: true,
            limit: 0,
            offset: 0,
        }));
        // SAFETY: numeric_filters is a valid array of GEO_RANGE_COUNT elements; ii is in bounds.
        unsafe { (*numeric_filters)[ii] = filt_ptr };
        // SAFETY: filt_ptr is exclusively owned and lives for 'index (stored in gf).
        filters.push(unsafe { &*filt_ptr });
    }
    Some(filters)
}

/// Creates a union iterator over all geo-encoded index entries within the radius in `gf`.
///
/// Geo fields are stored as sorted numeric geohash values. The radius maps to up to
/// [`GEO_RANGE_COUNT`] contiguous geohash ranges; each range is queried via the numeric range
/// tree. All matching [`NumericIteratorVariant`]s across all ranges are collected into a single
/// flat union.
///
/// Returns `None` if the parameters are invalid or the index hasn't been created yet.
///
/// # Safety
///
/// 1. `sctx` must point to a valid [`ffi::RedisSearchCtx`] whose `spec` field is also valid,
///    both remaining so for `'index`.
/// 2. `gf.fieldSpec` must be a valid non-null pointer to a [`ffi::FieldSpec`] for a geo field,
///    remaining valid for `'index`.
/// 3. `gf.numericFilters` must be NULL on entry; it is populated here and must be freed by
///    `GeoFilter_Free`.
/// 4. `field_ctx` must contain a field index (not a field mask).
pub unsafe fn new_geo_range_iterator<'index>(
    sctx: NonNull<ffi::RedisSearchCtx>,
    gf: &'index mut GeoFilter,
    field_ctx: &FieldFilterContext,
    min_union_iter_heap: usize,
    numeric_compress: bool,
) -> Option<NewUnionIterator<'index, NumericIteratorVariant<'index>>> {
    // Read fieldSpec before the mutable borrow in build_geo_numeric_filters.
    // SAFETY: 2. guarantees gf.fieldSpec is valid and non-null.
    let fs = unsafe { &mut *(gf.fieldSpec as *mut ffi::FieldSpec) };

    // SAFETY: 2–3. are forwarded from this function's safety contract.
    let filters = unsafe { build_geo_numeric_filters(gf)? };

    // Open the numeric/geo index once for all ranges.
    // SAFETY: 1. guarantees sctx.spec is valid and non-null.
    let spec = unsafe { &mut *sctx.as_ref().spec };
    // SAFETY: 1–2.
    let tree = unsafe { open_numeric_or_geo_index(spec, fs, false, numeric_compress)? };

    let mut all_variants: Vec<NumericIteratorVariant<'index>> = Vec::new();
    for filter in filters {
        // SAFETY: 1. and 4.
        let variants = unsafe { NumericIteratorVariant::from_tree(tree, sctx, filter, field_ctx) };
        all_variants.extend(variants);
    }
    Some(new_union_iterator(all_variants, true, min_union_iter_heap))
}

/// Convert a [`GeoDistance`] unit to metres.
pub fn extract_geo_unit_factor(unit: GeoDistance) -> f64 {
    match unit {
        ffi::GeoDistance_GEO_DISTANCE_M => 1.0,
        ffi::GeoDistance_GEO_DISTANCE_KM => 1000.0,
        ffi::GeoDistance_GEO_DISTANCE_FT => 0.3048,
        ffi::GeoDistance_GEO_DISTANCE_MI => 1609.34,
        _ => unreachable!("invalid GeoDistance unit"),
    }
}
