/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ffi::{GeoDistance, GeoFilter, QueryIterator, RedisSearchCtx};
use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};
use geo::hash::InvalidWGS84Coordinates;
use inverted_index::NumericFilter;
use query_types::QueryNodeType;

use crate::{
    NumericIteratorVariant, c2rust::CRQEIterator, open_numeric_or_geo_index,
    union_opaque::build_union,
};

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
///    to `*gf` and must be released by `GeoFilter_Free` (which frees it through
///    [`free_geo_numeric_filters`]).
pub unsafe fn build_geo_numeric_filters<'index>(
    gf: &'index mut GeoFilter,
) -> Result<Vec<&'index NumericFilter>, InvalidGeoInput> {
    if gf.radius <= 0.0 {
        return Err(InvalidGeoInput::InvalidRadius(gf.radius));
    }

    let radius_meters = gf.radius * extract_geo_unit_factor(gf.unitType);
    let center = geo::hash::WGS84Coordinates::from_f64(gf.lon, gf.lat)?;
    let ranges = geo::calc_ranges(center, radius_meters);

    // Allocate the `numericFilters` array in Rust and hand ownership to `*gf`.
    // `GeoFilter_Free` releases it by calling back into Rust
    // ([`free_geo_numeric_filters`], via the `iterators_ffi` wrapper), so the
    // container is always reclaimed with the same Rust allocator that produced
    // it.
    let numeric_filters = Box::into_raw(Box::new(
        [std::ptr::null_mut::<NumericFilter>(); geo::GEO_RANGE_COUNT],
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
                range.min as f64,
                range.max as f64,
                true, // inclusiveMin
                true, // inclusiveMax
                true, // ascending
                gf.fieldSpec,
                (gf as *const GeoFilter).cast(),
            )
        } as *mut NumericFilter;
        // SAFETY: numeric_filters is a valid array of [`geo::GEO_RANGE_COUNT`] elements;
        // `ii` is bounded by `ranges.iter()`, which has the same length.
        unsafe { (*numeric_filters)[ii] = filt_ptr };
        // SAFETY: filt_ptr is exclusively owned and lives for 'index (stored in gf).
        filters.push(unsafe { &*filt_ptr });
    }
    Ok(filters)
}

/// Frees the per-range `numericFilters` array that [`build_geo_numeric_filters`]
/// stored on a [`GeoFilter`], together with every non-null [`NumericFilter`] it
/// owns.
///
/// This is the deallocation counterpart of [`build_geo_numeric_filters`] and
/// must stay in lockstep with it. Each piece is reclaimed with the allocator
/// that produced it: the array container with the Rust allocator (`Box::from_raw`
/// undoing the `Box::into_raw`) and each entry with `NumericFilter_Free` (the C
/// allocator `NewNumericFilter` used). C's `GeoFilter_Free` calls this — through
/// the `iterators_ffi` `GeoFilter_FreeNumericFilters` wrapper — instead of
/// `rm_free`, so the container is freed with the right allocator in every binary.
///
/// Does nothing when `filters` is NULL.
///
/// # Safety
///
/// `filters` must be NULL, or the exact pointer [`build_geo_numeric_filters`]
/// wrote into `gf.numericFilters` (a `Box::into_raw` of
/// `[*mut NumericFilter; GEO_RANGE_COUNT]`), not yet freed. After this call that
/// pointer is dangling and must not be used again.
pub unsafe fn free_geo_numeric_filters(filters: *mut *mut NumericFilter) {
    if filters.is_null() {
        return;
    }
    // SAFETY: per the contract, `filters` is the `Box::into_raw` of a
    // `[*mut NumericFilter; GEO_RANGE_COUNT]`, so reclaiming it with
    // `Box::from_raw` matches the original allocation exactly.
    let array =
        unsafe { Box::from_raw(filters as *mut [*mut NumericFilter; geo::GEO_RANGE_COUNT]) };
    for &filt_ptr in array.iter() {
        if !filt_ptr.is_null() {
            // SAFETY: each non-null entry is a live `NumericFilter` from
            // `NewNumericFilter`, owned by `gf` and freed here exactly once.
            unsafe { ffi::NumericFilter_Free(filt_ptr.cast()) };
        }
    }
    // Dropping `array` releases the container via the Rust allocator.
}

/// Mapping to retrieve a [`NumericFilter`] for a vec of [`NumericIteratorVariant`]
type GeoFilterAndRangeIterator<'index> =
    Vec<(NonNull<NumericFilter>, Vec<NumericIteratorVariant<'index>>)>;

/// Creates per-range iterators for all geo-encoded index entries within the radius in `gf`.
///
/// Geo fields are stored as sorted numeric geohash values. The radius maps to up to
/// [`geo::GEO_RANGE_COUNT`] contiguous geohash ranges; each range is queried via the numeric range
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

/// Build a geo-radius iterator over all geohash sub-ranges matching `gf`.
///
/// A radius query maps to up to [`geo::GEO_RANGE_COUNT`] contiguous geohash ranges;
/// each is queried via the field's numeric range tree (with per-record distance
/// filtering) and the results are combined with [`build_union`].
///
/// Returns [`None`] — an empty (matchless) result, not an error — when `gf`'s
/// parameters are invalid, the geo index has not been created yet, or no entries
/// match.
///
/// # Safety
///
/// 1. `sctx` must be a valid non-null [`RedisSearchCtx`] whose `spec` is valid
///    and non-null; both must remain valid for the lifetime of the returned
///    iterator.
/// 2. `gf.fieldSpec` must be a valid non-null [`FieldSpec`](ffi::FieldSpec) for
///    a geo field, remaining valid for the lifetime of the returned iterator.
/// 3. `gf.numericFilters` must be NULL on entry; it is populated here and must
///    be freed by `GeoFilter_Free`.
pub unsafe fn build_geo_range_iterator(
    sctx: NonNull<RedisSearchCtx>,
    gf: &mut GeoFilter,
    min_union_iter_heap: usize,
    compress: bool,
) -> Option<NonNull<QueryIterator>> {
    debug_assert!(!gf.fieldSpec.is_null(), "geo filter must have a field spec");
    // Read fieldSpec.index before the mutable borrow in `new_geo_range_iterator`.
    // SAFETY: precondition (2) — `gf.fieldSpec` is valid and non-null.
    let field_index = unsafe { (*gf.fieldSpec).index };
    let field_ctx = FieldFilterContext {
        field: FieldMaskOrIndex::Index(field_index),
        predicate: FieldExpirationPredicate::Default,
    };

    // SAFETY: preconditions (1)–(3) map directly to those of
    // `new_geo_range_iterator`.
    let groups = unsafe { new_geo_range_iterator(sctx, gf, &field_ctx, compress) }.ok()?;

    let children: Vec<CRQEIterator> = groups
        .into_iter()
        .flat_map(|(_, variants)| variants.into_iter().map(CRQEIterator::from_rust_leaf))
        .collect();

    let iter = build_union(children, true, min_union_iter_heap, QueryNodeType::Geo, 1.0);
    Some(iter)
}
