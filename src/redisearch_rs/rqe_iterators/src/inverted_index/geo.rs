/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ffi::{GeoDistance, GeoFilter};
use field::FieldFilterContext;
use geo::hash::InvalidWGS84Coordinates;
use inverted_index::NumericFilter;

use crate::{NumericIteratorVariant, open_numeric_or_geo_index};

/// Error returned by [`build_geo_numeric_filters`] when the caller supplies
/// invalid geo parameters.
#[derive(Debug, thiserror::Error)]
pub enum InvalidGeoInput {
    /// The radius is non-positive.
    #[error("non-positive geo radius: {0}")]
    InvalidRadius(f64),
    /// The coordinates are outside WGS-84 bounds.
    #[error(transparent)]
    InvalidCoordinates(#[from] InvalidWGS84Coordinates),
}

/// Errors returned by [`new_geo_range_iterator`].
#[derive(Debug, thiserror::Error)]
pub enum GeoRangeError {
    /// The caller supplied out-of-range coordinates or a non-positive radius.
    #[error(transparent)]
    InvalidInput(#[from] InvalidGeoInput),
    /// The geo field index has not been created yet.
    #[error("geo field index not found")]
    IndexNotFound,
    /// The query matched no entries in the index.
    #[error("no matching entries in geo index")]
    NoMatchingEntries,
}

/// Validates `gf`'s parameters, computes the geohash ranges covering the requested circle,
/// allocates a per-range [`NumericFilter`] for each non-trivial range, stores all of them in
/// `gf.numericFilters`, and returns references to the non-trivial filters.
///
/// Returns `Err(`[`InvalidGeoInput`]`)` if `gf`'s parameters are invalid (non-positive
/// radius or out-of-bounds coordinates).
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
    if gf.radius <= 0.0 {
        return Err(InvalidGeoInput::InvalidRadius(gf.radius));
    }

    let radius_meters = gf.radius * extract_geo_unit_factor(gf.unitType);
    let center = geo::hash::WGS84Coordinates::from_f64(gf.lon, gf.lat)?;
    let ranges = geo::calc_ranges(center, radius_meters);

    // Allocate the `numericFilters` array via the C-side helper so the
    // container is rm_calloc-backed, matching `GeoFilter_Free`'s
    // `rm_free(gf->numericFilters)` cleanup.
    //
    // SAFETY: the helper just calls `rm_calloc(GEO_RANGE_COUNT, sizeof(*))`
    // and returns the resulting pointer; on success the returned array is
    // zero-initialised, so each per-cell slot starts as the null pointer
    // the loop below expects.
    let numeric_filters =
        unsafe { ffi::GeoFilter_AllocNumericFiltersArray() } as *mut *mut NumericFilter;
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
                range.min as f64,
                range.max as f64,
                true, // inclusiveMin
                true, // inclusiveMax
                true, // ascending
                gf.fieldSpec,
                (gf as *const GeoFilter).cast(),
            )
        } as *mut NumericFilter;
        // SAFETY: numeric_filters points at a freshly-allocated array of
        // GEO_RANGE_COUNT pointer-sized slots (see the rm_calloc helper);
        // `ii` is bounded by `ranges.iter()` which has the same length.
        let slot = unsafe { numeric_filters.add(ii) };
        // SAFETY: `slot` is a valid, aligned pointer into the array (see above).
        unsafe { *slot = filt_ptr };
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
/// `GEO_RANGE_COUNT` contiguous geohash ranges; each range is queried via the numeric range
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
