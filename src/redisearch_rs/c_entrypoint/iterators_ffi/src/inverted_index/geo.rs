/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rqe_iterators::free_geo_numeric_filters;

/// Frees the `numericFilters` array that
/// [`build_geo_numeric_filters`](rqe_iterators::build_geo_numeric_filters) populated on a
/// [`GeoFilter`](ffi::GeoFilter), together with the per-range `NumericFilter`s it owns.
///
/// The array is allocated in Rust, so it must be released with the Rust allocator.
///
/// # Safety
///
/// The safety contract of [`free_geo_numeric_filters`] applies.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoFilter_FreeNumericFilters(filters: *mut *mut ffi::NumericFilter) {
    // SAFETY: the safety contract is forwarded to `free_geo_numeric_filters`.
    unsafe { free_geo_numeric_filters(filters.cast()) };
}
