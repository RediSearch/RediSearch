/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::{CStr, c_char, c_int};

use query_error::{QueryError, QueryErrorCode, opaque::OpaqueQueryError};
use rqe_core::{FieldIndex, RS_INVALID_FIELD_INDEX};

/// C unit representation. Integers outside the named constants remain representable.
#[cheadergen::config(export)]
pub type GeoDistance = c_int;

#[cheadergen::config(export)]
pub const GEO_DISTANCE_INVALID: GeoDistance = -1;
#[cheadergen::config(export)]
pub const GEO_DISTANCE_KM: GeoDistance = 0;
#[cheadergen::config(export)]
pub const GEO_DISTANCE_M: GeoDistance = 1;
#[cheadergen::config(export)]
pub const GEO_DISTANCE_FT: GeoDistance = 2;
#[cheadergen::config(export)]
pub const GEO_DISTANCE_MI: GeoDistance = 3;

/// C query geometry and iterator-owned runtime cache.
#[derive(Clone, Copy)]
#[repr(C)]
#[cheadergen::config(export)]
#[expect(non_snake_case, reason = "preserve the C field names")]
pub struct GeoFilter {
    /// Stable index, not a potentially stale field-spec pointer.
    pub fieldIndex: FieldIndex,
    pub lat: f64,
    pub lon: f64,
    pub radius: f64,
    pub unitType: GeoDistance,
    /// Released by the iterator adapter, which knows both allocation origins.
    pub numericFilters: *mut *mut ffi::NumericFilter,
}

impl GeoFilter {
    pub(crate) const fn from_core(filter: geo::GeoFilter) -> Self {
        Self {
            fieldIndex: filter.field_index,
            lat: filter.lat,
            lon: filter.lon,
            radius: filter.radius,
            unitType: unit_to_c(filter.unit),
            numericFilters: std::ptr::null_mut(),
        }
    }

    const fn to_core(self) -> geo::GeoFilter {
        geo::GeoFilter {
            field_index: self.fieldIndex,
            lat: self.lat,
            lon: self.lon,
            radius: self.radius,
            unit: unit_from_c(self.unitType),
        }
    }
}

pub(crate) const fn unit_to_c(unit: geo::GeoDistance) -> GeoDistance {
    match unit {
        geo::GeoDistance::Invalid => GEO_DISTANCE_INVALID,
        geo::GeoDistance::Kilometers => GEO_DISTANCE_KM,
        geo::GeoDistance::Meters => GEO_DISTANCE_M,
        geo::GeoDistance::Feet => GEO_DISTANCE_FT,
        geo::GeoDistance::Miles => GEO_DISTANCE_MI,
    }
}

const fn unit_from_c(unit: GeoDistance) -> geo::GeoDistance {
    match unit {
        GEO_DISTANCE_KM => geo::GeoDistance::Kilometers,
        GEO_DISTANCE_M => geo::GeoDistance::Meters,
        GEO_DISTANCE_FT => geo::GeoDistance::Feet,
        GEO_DISTANCE_MI => geo::GeoDistance::Miles,
        _ => geo::GeoDistance::Invalid,
    }
}

/// Parse a NUL-terminated unit.
///
/// # Safety
/// `s` must point to a readable NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoDistance_Parse(s: *const c_char) -> GeoDistance {
    // SAFETY: caller supplies a NUL-terminated string.
    unit_to_c(geo::GeoDistance::parse(
        unsafe { CStr::from_ptr(s) }.to_bytes(),
    ))
}

/// Parse a bounded unit, retaining the legacy buffer limit.
///
/// # Safety
/// `s` must be readable for `len` bytes when `len` is less than the buffer limit.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoDistance_Parse_Buffer(s: *const c_char, len: usize) -> GeoDistance {
    if len >= 16 {
        return GEO_DISTANCE_INVALID;
    }
    let bytes = if len == 0 {
        &[]
    } else {
        // SAFETY: caller guarantees readability for len bytes.
        unsafe { std::slice::from_raw_parts(s.cast(), len) }
    };
    unit_to_c(geo::GeoDistance::parse_buffer(bytes))
}

/// Return a static NUL-terminated unit spelling.
#[unsafe(no_mangle)]
pub const extern "C" fn GeoDistance_ToString(unit: GeoDistance) -> *const c_char {
    match unit_from_c(unit) {
        geo::GeoDistance::Kilometers => c"km",
        geo::GeoDistance::Meters => c"m",
        geo::GeoDistance::Feet => c"ft",
        geo::GeoDistance::Miles => c"mi",
        geo::GeoDistance::Invalid => c"<badunit>",
    }
    .as_ptr()
}

/// Allocate an unvalidated filter, released with [`GeoFilter_Free`].
///
/// # Safety
/// A non-null `unit` must be readable for `unit_len` bytes.
/// Redis allocation functions must be initialized and remain unchanged.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewGeoFilter(
    lon: f64,
    lat: f64,
    radius: f64,
    unit: *const c_char,
    unit_len: usize,
) -> *mut GeoFilter {
    let mut filter = GeoFilter::from_core(geo::GeoFilter::new(lon, lat, radius, None));
    if !unit.is_null() {
        // SAFETY: forwarded caller contract.
        filter.unitType = unsafe { GeoDistance_Parse_Buffer(unit, unit_len) };
    }
    // SAFETY: the Redis allocator is initialized and remains unchanged.
    let alloc =
        unsafe { redis_module::RedisModule_Alloc }.expect("Redis allocator not initialized");
    // SAFETY: the allocation is sized for the C layout.
    let ptr = unsafe { alloc(std::mem::size_of::<GeoFilter>()) }.cast::<GeoFilter>();
    assert!(!ptr.is_null());
    // SAFETY: ptr points to sufficient freshly allocated storage.
    unsafe { ptr.write(filter) };
    ptr
}

/// Resolve or clear the stable field index.
///
/// # Safety
/// `gf` must be writable; a non-null `fs` must point to a live [`ffi::FieldSpec`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoFilter_SetField(gf: *mut GeoFilter, fs: *const ffi::FieldSpec) {
    // SAFETY: a non-null fs points to a live field spec.
    let index = unsafe { fs.as_ref() }.map_or(RS_INVALID_FIELD_INDEX, |fs| fs.index);
    // SAFETY: caller guarantees gf is writable.
    unsafe { (*gf).fieldIndex = index };
}

/// Validate without changing geometry; errors use [`QueryErrorCode::Syntax`].
///
/// # Safety
/// `gf` must be readable and `status` must point to an initialized, writable query error.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoFilter_Validate(
    gf: *const GeoFilter,
    status: *mut OpaqueQueryError,
) -> c_int {
    // SAFETY: caller supplies a readable filter.
    let gf = unsafe { &*gf };
    let mut core = gf.to_core();
    // The C validator only rejects the named invalid sentinel, not other integers.
    if gf.unitType != GEO_DISTANCE_INVALID && core.unit == geo::GeoDistance::Invalid {
        core.unit = geo::GeoDistance::Kilometers;
    }
    if let Err(error) = core.validate() {
        // SAFETY: caller supplies an initialized query error.
        unsafe { QueryError::from_opaque_mut_ptr(status) }
            .expect("status is null")
            .set_error(QueryErrorCode::Syntax, &error.to_string());
        return 0;
    }
    1
}

unsafe extern "C" {
    fn GeoFilter_FreeNumericFilters(filters: *mut *mut ffi::NumericFilter);
}

/// Free a Redis-allocated filter and its iterator-owned cache.
///
/// # Safety
/// `gf` must own its cache and point to a live Redis allocation. Both allocation
/// functions must remain initialized; no references may survive this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoFilter_Free(gf: *mut GeoFilter) {
    // SAFETY: caller supplies a live filter.
    let filters = unsafe { (*gf).numericFilters };
    if !filters.is_null() {
        // SAFETY: caller transfers exclusive ownership of the iterator-owned cache.
        unsafe { GeoFilter_FreeNumericFilters(filters) };
    }
    // SAFETY: the Redis allocator remains initialized and unchanged.
    let free = unsafe { redis_module::RedisModule_Free }.expect("Redis allocator not initialized");
    // SAFETY: gf is a Redis allocation and no references survive this call.
    unsafe { free(gf.cast()) };
}

/// Evaluate a numeric geohash score against the filter.
///
/// # Safety
/// `gf` must be readable; non-null `distance` must be writable.
///
/// # Panics
/// The constraints of [`geo::GeoFilter::is_within_radius`] apply.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn isWithinRadius(
    gf: *const GeoFilter,
    score: f64,
    distance: *mut f64,
) -> c_int {
    // SAFETY: caller supplies a readable filter.
    let (inside, meters) = unsafe { &*gf }.to_core().is_within_radius(score);
    // SAFETY: caller guarantees writability when non-null.
    if let Some(distance) = unsafe { distance.as_mut() } {
        *distance = meters;
    }
    c_int::from(inside)
}
