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

use crate::{NumericIteratorVariant, open_numeric_or_geo_index};

/// Error returned by [`build_geo_numeric_filters`] when the caller supplies out-of-range
/// coordinates or a non-positive radius.
#[derive(Debug)]
pub struct InvalidGeoInput;

/// Errors returned by [`new_geo_range_iterator`].
#[derive(Debug)]
pub enum GeoRangeError {
    /// The caller supplied out-of-range coordinates or a non-positive radius.
    InvalidInput,
    /// The geo field index has not been created yet.
    IndexNotFound,
    /// The query matched no entries in the index.
    NoMatchingEntries,
}

impl From<InvalidGeoInput> for GeoRangeError {
    fn from(_: InvalidGeoInput) -> Self {
        GeoRangeError::InvalidInput
    }
}

/// Validates `gf`'s parameters, computes the geohash ranges covering the requested circle,
/// allocates a per-range [`NumericFilter`] for each non-trivial range, stores all of them in
/// `gf.numericFilters`, and returns references to the non-trivial filters.
///
/// Returns [`Err(InvalidGeoInput)`](InvalidGeoInput) if `gf`'s parameters are invalid (bad
/// radius, lat, or lon).
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
) -> Result<Vec<&'index NumericFilter>, InvalidGeoInput> {
    if gf.radius <= 0.0
        || gf.lon > GEO_LONG_MAX
        || gf.lon < GEO_LONG_MIN
        || gf.lat > GEO_LAT_MAX
        || gf.lat < GEO_LAT_MIN
    {
        return Err(InvalidGeoInput);
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
        // SAFETY: gf.fieldSpec is valid per the caller's safety contract.
        let filt_ptr = unsafe {
            ffi::NewNumericFilter(
                range.min,
                range.max,
                true, // inclusiveMin
                true, // inclusiveMax
                true, // ascending
                gf.fieldSpec,
                (gf as *const GeoFilter).cast(),
            )
        } as *mut NumericFilter;
        // SAFETY: numeric_filters is a valid array of GEO_RANGE_COUNT elements; ii is in bounds.
        unsafe { (*numeric_filters)[ii] = filt_ptr };
        // SAFETY: filt_ptr is exclusively owned and lives for 'index (stored in gf).
        filters.push(unsafe { &*filt_ptr });
    }
    Ok(filters)
}

/// Mapping to retrieve a [`NumericFilter`] for a vec of [`NumericIteratorVariant`]
type GeoFilterAndRangeIterator<'index> =
    Vec<(NonNull<NumericFilter>, Vec<NumericIteratorVariant<'index>>)>;

/// Creates per-range iterators for all geo-encoded index entries within the radius in `gf`.
///
/// Geo fields are stored as sorted numeric geohash values. The radius maps to up to
/// [`GEO_RANGE_COUNT`] contiguous geohash ranges; each range is queried via the numeric range
/// tree. Returns one `(filter, variants)` pair per non-trivial range so that callers can
/// associate each [`NumericIteratorVariant`] with its [`NumericFilter`] (needed by C profiling;
/// see the comment in `NewGeoRangeIterator`).
///
/// Returns:
/// - `Err(`[`GeoRangeError::InvalidInput`]`)` if `gf`'s parameters are invalid.
/// - `Err(`[`GeoRangeError::IndexNotFound`]`)` if the geo field index hasn't been created yet.
/// - `Err(`[`GeoRangeError::NoMatchingEntries`]`)` if no entries matched the query.
/// - `Ok(_)` on success.
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
    numeric_compress: bool,
) -> Result<GeoFilterAndRangeIterator<'index>, GeoRangeError> {
    // Read fieldSpec before the mutable borrow in build_geo_numeric_filters.
    // SAFETY: 2. guarantees gf.fieldSpec is valid and non-null.
    let fs = unsafe { &mut *(gf.fieldSpec as *mut ffi::FieldSpec) };

    // SAFETY: 2–3. are forwarded from this function's safety contract.
    let filters = unsafe { build_geo_numeric_filters(gf)? };

    // Open the numeric/geo index once for all ranges.
    // SAFETY: 1. guarantees sctx is valid and non-null.
    let sctx_ref = unsafe { sctx.as_ref() };
    // SAFETY: 1. guarantees sctx.spec is valid and non-null.
    let spec = unsafe { &mut *sctx_ref.spec };
    // SAFETY: 1–2.
    let Some(tree) = (unsafe { open_numeric_or_geo_index(spec, fs, false, numeric_compress) })
    else {
        return Err(GeoRangeError::IndexNotFound);
    };

    let mut groups: Vec<(NonNull<NumericFilter>, Vec<NumericIteratorVariant<'index>>)> = Vec::new();
    for filter in filters {
        // SAFETY: 1. and 4.
        let variants = unsafe { NumericIteratorVariant::from_tree(tree, sctx, filter, field_ctx) };
        if !variants.is_empty() {
            // SAFETY: filter is a shared reference stored inside gf, valid for 'index.
            groups.push((NonNull::from(filter), variants));
        }
    }
    if groups.is_empty() {
        Err(GeoRangeError::NoMatchingEntries)
    } else {
        Ok(groups)
    }
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
