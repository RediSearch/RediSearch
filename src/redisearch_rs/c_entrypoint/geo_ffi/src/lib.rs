/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI wrappers for geo utilities, exposing them as C-callable functions.
//!
//! This crate bridges the pure Rust [`geo`] crate (including its [`geo::hash`]
//! module), providing the same C API that was previously in `src/rs_geo.c`.

use std::ffi::{CStr, CString, c_char, c_int};

use decorum::R64;
use geo::{
    DistanceUnit, GEO_DISTANCE_INVALID, GeoDistance, GeoFilter, LegacyGeoFilter, ParseGeoError,
};
use query_error::{QueryError, QueryErrorCode, opaque::OpaqueQueryError};
use tracing_log_error::log_error;

/// The number of geohash ranges: the center cell plus its 8 neighbors.
#[cheadergen::config(export)]
pub const GEO_RANGE_COUNT: usize = geo::GEO_RANGE_COUNT;

/// WGS-84 latitude lower bound (EPSG:900913).
#[cheadergen::config(export)]
pub const GEO_LAT_MIN: f64 = geo::hash::GEO_LAT_MIN;

/// WGS-84 latitude upper bound (EPSG:900913).
#[cheadergen::config(export)]
pub const GEO_LAT_MAX: f64 = geo::hash::GEO_LAT_MAX;

/// WGS-84 longitude lower bound.
#[cheadergen::config(export)]
pub const GEO_LONG_MIN: f64 = geo::hash::GEO_LONG_MIN;

/// WGS-84 longitude upper bound.
#[cheadergen::config(export)]
pub const GEO_LONG_MAX: f64 = geo::hash::GEO_LONG_MAX;

/// Encode longitude and latitude into a 52-bit-aligned geohash score,
/// or return `INVALID_GEOHASH` (`-1.0`) on invalid coordinates.
///
/// Valid geohash scores are non-negative, so any negative return value
/// signals an error.
#[unsafe(no_mangle)]
pub extern "C" fn encodeGeo(lon: f64, lat: f64) -> f64 {
    let coords = match geo::hash::WGS84Coordinates::from_f64(lon, lat) {
        Ok(coords) => coords,
        Err(err) => {
            log_error!(err, level: tracing::Level::WARN, "encodeGeo: invalid WGS-84 coordinates");
            return INVALID_GEOHASH;
        }
    };
    let hash = geo::hash::encode_wgs84(coords, geo::hash::GEO_STEP_MAX);
    geo::hash::align_52bits(hash) as f64
}

/// Sentinel returned by [`encodeGeo`] for an unindexable coordinate.
#[cheadergen::config(export)]
pub const INVALID_GEOHASH: f64 = -1.0;

/// Decode a geohash `f64` back into a `[longitude, latitude]` pair.
///
/// Always succeeds — zero is a valid geohash encoding (for the boundary
/// coordinate -180, -85.05112878).
///
/// # Safety
///
/// - `xy` must be a valid, non-null pointer to a writable `[f64; 2]`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn decodeGeo(bits: f64, xy: *mut f64) {
    let raw_bits = bits as u64;
    let hash = geo::hash::GeoHashBits {
        bits: raw_bits,
        step: geo::hash::GEO_STEP_MAX,
    };
    let (lon, lat) = geo::hash::decode_to_lon_lat(hash);
    // SAFETY: caller guarantees `xy` points to at least 2 writable `f64` values.
    unsafe {
        *xy = lon.into_inner();
    }
    // SAFETY: caller guarantees `xy` points to at least 2 writable `f64` values.
    let xy_lat = unsafe { xy.add(1) };
    // SAFETY: `xy_lat` is valid per the above.
    unsafe {
        *xy_lat = lat.into_inner();
    }
}

/// Calculate the haversine great-circle distance between two WGS-84 points.
///
/// All coordinates are in degrees. Returns distance in meters.
#[unsafe(no_mangle)]
pub extern "C" fn geohashGetDistance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let (Ok(lon1), Ok(lat1), Ok(lon2), Ok(lat2)) = (
        R64::try_new(lon1),
        R64::try_new(lat1),
        R64::try_new(lon2),
        R64::try_new(lat2),
    ) else {
        return f64::NAN;
    };
    geo::hash::haversine_distance(lon1, lat1, lon2, lat2)
}

/// Set a [`QueryError`] with a [`ParseGeoError`] message.
///
/// # Panics
///
/// - If `status` is null.
///
/// # Safety
///
/// - `status` must be a valid pointer to an [`OpaqueQueryError`] created by
///   `QueryError_Default`.
unsafe fn set_parse_error(status: *mut OpaqueQueryError, err: ParseGeoError) {
    log_error!(err, level: tracing::Level::WARN, "parseGeo: failed to parse geo string");
    // SAFETY: caller guarantees `status` is valid.
    let query_error = unsafe { QueryError::from_opaque_mut_ptr(status) }.expect("status is null");
    query_error.set_error(QueryErrorCode::ParseArgs, &err.to_string());
}

/// Parse a `"lon,lat"` or `"lon lat"` string into separate longitude and
/// latitude values. Leading and trailing whitespace around each coordinate
/// value is trimmed.
///
/// Returns `REDISMODULE_OK` on success, `REDISMODULE_ERR` on failure (with
/// the error details written to `status`).
///
/// # Safety
///
/// - `c` must be a valid pointer to at least `len` bytes.
/// - `lon` and `lat` must be valid, non-null pointers to writable `f64` values.
/// - `status` must be a valid, non-null pointer to an [`OpaqueQueryError`]
///   created by `QueryError_Default`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn parseGeo(
    c: *const u8,
    len: usize,
    lon: *mut f64,
    lat: *mut f64,
    status: *mut OpaqueQueryError,
) -> c_int {
    // SAFETY: caller guarantees `c` points to at least `len` bytes.
    let bytes = unsafe { std::slice::from_raw_parts(c, len) };
    let s = match std::str::from_utf8(bytes) {
        Ok(s) => s,
        Err(_) => {
            // SAFETY: caller guarantees `status` is valid.
            unsafe {
                set_parse_error(status, ParseGeoError::InvalidUtf8);
            }
            return redis_module::REDISMODULE_ERR as i32;
        }
    };

    match geo::Coordinates::parse_geo(s) {
        Ok(coords) => {
            // SAFETY: caller guarantees `lon` is valid.
            unsafe {
                *lon = coords.lon.into_inner();
            }
            // SAFETY: caller guarantees `lat` is valid.
            unsafe {
                *lat = coords.lat.into_inner();
            }
            redis_module::REDISMODULE_OK as i32
        }
        Err(err) => {
            // SAFETY: caller guarantees `status` is valid.
            unsafe {
                set_parse_error(status, err);
            }
            redis_module::REDISMODULE_ERR as i32
        }
    }
}

/// Parse a case-insensitive unit.
/// # Safety
/// `s` must be a non-null, [valid](std::ptr#safety), NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoDistance_Parse(s: *const c_char) -> GeoDistance {
    debug_assert!(!s.is_null());
    // SAFETY: the caller supplies a live NUL-terminated string.
    let bytes = unsafe { CStr::from_ptr(s) }.to_bytes();
    DistanceUnit::parse(bytes).map_or(GEO_DISTANCE_INVALID, |unit| unit as i32)
}

/// Parse a bounded unit token, accepting an embedded NUL and rejecting lengths >= 16.
/// # Safety
/// `s` must be non-null and [valid](std::ptr#safety) for `len` readable bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoDistance_Parse_Buffer(s: *const c_char, len: usize) -> GeoDistance {
    debug_assert!(!s.is_null());
    if len >= 16 {
        return GEO_DISTANCE_INVALID;
    }
    // SAFETY: the token is readable for the caller-supplied length.
    let bytes = unsafe { std::slice::from_raw_parts(s.cast(), len) };
    let bytes = bytes.split(|&byte| byte == 0).next().unwrap();
    DistanceUnit::parse(bytes).map_or(GEO_DISTANCE_INVALID, |unit| unit as i32)
}

/// Return a static canonical unit abbreviation or `<badunit>`.
#[unsafe(no_mangle)]
pub extern "C" fn GeoDistance_ToString(unit: GeoDistance) -> *const c_char {
    DistanceUnit::from_raw(unit)
        .map_or(c"<badunit>", DistanceUnit::as_c_str)
        .as_ptr()
}

/// Allocate a query filter; a null unit selects kilometers.
/// # Safety
/// A non-null `unit` must be [valid](std::ptr#safety) for `len` bytes. The Redis allocator must be initialized.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewGeoFilter(
    lon: f64,
    lat: f64,
    radius: f64,
    unit: *const c_char,
    len: usize,
) -> *mut GeoFilter {
    let unit = if unit.is_null() {
        Some(DistanceUnit::Kilometers)
    } else {
        // SAFETY: the non-null token has the readable length promised by the caller.
        DistanceUnit::from_raw(unsafe { GeoDistance_Parse_Buffer(unit, len) })
    };
    // SAFETY: Redis initialized its allocator before registering commands.
    let alloc = unsafe { redis_module::RedisModule_Alloc }.unwrap();
    // SAFETY: the allocator returns aligned storage or aborts on OOM.
    let ptr = unsafe { alloc(size_of::<GeoFilter>()) }.cast::<GeoFilter>();
    // SAFETY: the fresh allocation is exclusively owned and sized for this value.
    unsafe { ptr.write(GeoFilter::new(lon, lat, radius, unit)) };
    ptr
}

/// Record the field's stable index; null clears the association.
/// # Safety
/// `gf` must be non-null, [valid](std::ptr#safety) and exclusively writable; `fs` must be null or valid and not alias `gf`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoFilter_SetField(gf: *mut GeoFilter, fs: *const ffi::FieldSpec) {
    debug_assert!(!gf.is_null());
    // SAFETY: fs is null or readable for this call.
    let field = unsafe { fs.as_ref() };
    // SAFETY: gf is exclusively writable and does not alias the field.
    unsafe { (*gf).fieldIndex = field.map_or(rqe_core::RS_INVALID_FIELD_INDEX, |fs| fs.index) };
}

/// Return 1 for a valid filter, otherwise set a syntax error and return 0.
/// # Safety
/// Both pointers must be non-null, [valid](std::ptr#safety) and disjoint; `status` must be initialized and exclusively writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoFilter_Validate(
    gf: *const GeoFilter,
    status: *mut OpaqueQueryError,
) -> c_int {
    debug_assert!(!gf.is_null() && !status.is_null());
    // SAFETY: gf is a live shared filter for this call.
    match unsafe { &*gf }.validate() {
        Ok(_) => 1,
        Err(error) => {
            // SAFETY: status is an initialized, exclusively borrowed query error.
            unsafe { QueryError::from_opaque_mut_ptr(status) }
                .unwrap()
                .set_error(QueryErrorCode::Syntax, error);
            0
        }
    }
}

/// Check a stored geohash, optionally writing distance in meters.
/// # Safety
/// `gf` must be non-null, [valid](std::ptr#safety) and validated. `distance` must be null or valid for exclusive writes, disjoint from `gf`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn isWithinRadius(
    gf: *const GeoFilter,
    score: f64,
    distance: *mut f64,
) -> c_int {
    debug_assert!(!gf.is_null());
    // SAFETY: gf is readable and its query parameters were validated by the caller.
    let (inside, meters) = unsafe { &*gf }
        .validate()
        .expect("validated geo filter")
        .contains(score);
    if !distance.is_null() {
        // SAFETY: a non-null output is exclusively writable, but may be uninitialized.
        unsafe { distance.write(meters) };
    }
    inside.into()
}

/// Release a query-owned filter, including its numeric range filters.
/// # Safety
/// `gf` must be non-null, [valid](std::ptr#safety), uniquely owned and allocated by the initialized Redis allocator.
/// Its range array must satisfy [`rqe_iterators::free_geo_numeric_filters`]'s contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoFilter_Free(gf: *mut GeoFilter) {
    debug_assert!(!gf.is_null());
    // SAFETY: gf owns the range array and is not borrowed elsewhere.
    let ranges = unsafe { (*gf).numericFilters };
    // SAFETY: the query owns exactly the array created by the range builder.
    unsafe { rqe_iterators::free_geo_numeric_filters(ranges.cast()) };
    // SAFETY: the Redis allocator is initialized.
    let free = unsafe { redis_module::RedisModule_Free }.unwrap();
    // SAFETY: the Redis allocation can be larger than GeoFilter for legacy filters.
    unsafe { free(gf.cast()) };
}

/// Release a legacy filter and its borrowed-name wrapper.
/// # Safety
/// `gf` must be non-null, [valid](std::ptr#safety) and uniquely owned; its base obeys [`GeoFilter_Free`]'s contract.
/// Its field is null or an owned [`ffi::NewHiddenString`] wrapper with `takeOwnership=false`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn LegacyGeoFilter_Free(gf: *mut LegacyGeoFilter) {
    debug_assert!(!gf.is_null());
    // SAFETY: the legacy filter is live and exclusively owned.
    let field = unsafe { (*gf).field };
    if !field.is_null() {
        // SAFETY: the wrapper owns no backing bytes and has not been released.
        unsafe { ffi::HiddenString_Free(field.cast(), false) };
    }
    // SAFETY: repr(C) places base at the allocation's start; it owns the ranges.
    unsafe { GeoFilter_Free(gf.cast()) };
}

/// Parse legacy GEOFILTER arguments, leaving failed output safe to free.
/// # Safety
/// All pointers must be non-null, non-aliasing and [valid](std::ptr#safety) for exclusive access.
/// `ac` must be initialized with live arguments, which outlive the filter's field-name resolution;
/// `status` must be initialized. `gf` must not own allocations on entry. The Redis allocator must be initialized.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoFilter_LegacyParse(
    gf: *mut LegacyGeoFilter,
    ac: *mut ffi::ArgsCursor,
    empty: *mut bool,
    status: *mut OpaqueQueryError,
) -> c_int {
    debug_assert!(!gf.is_null() && !ac.is_null() && !empty.is_null() && !status.is_null());
    // SAFETY: gf may be uninitialized but is exclusively writable.
    unsafe {
        gf.write(LegacyGeoFilter {
            base: GeoFilter::new(0.0, 0.0, 0.0, Some(DistanceUnit::Kilometers)),
            field: std::ptr::null_mut(),
        })
    };
    // SAFETY: gf was initialized above and is exclusively writable.
    let gf = unsafe { &mut *gf };
    // SAFETY: ac is initialized and exclusively borrowed for parsing.
    let ac = unsafe { &mut *ac };
    // SAFETY: status is an initialized, exclusively borrowed query error.
    let status = unsafe { QueryError::from_opaque_mut_ptr(status) }.unwrap();
    debug_assert!(ac.offset <= ac.argc);
    if ac.argc - ac.offset < 5 {
        status.set_error(QueryErrorCode::ParseArgs, "GEOFILTER requires 5 arguments");
        return 1;
    }
    // SAFETY: at least five live arguments remain.
    let field = unsafe { ffi::AC_GetStringNC(ac, std::ptr::null_mut()) };
    let (mut lon, mut lat, mut radius) = (0.0, 0.0, 0.0);
    for (name, value) in [
        ("lon", &mut lon),
        ("lat", &mut lat),
        ("radius", &mut radius),
    ] {
        // SAFETY: the next argument exists and value is an exclusive local output.
        if unsafe { ffi::AC_GetDouble(ac, value, ffi::AC_F_NOADVANCE as i32) } != 0 {
            let (mut bytes, mut len) = (std::ptr::null(), 0);
            // SAFETY: the unconsumed argument exists; outputs are distinct locals.
            unsafe { ffi::AC_GetString(ac, &mut bytes, &mut len, ffi::AC_F_NOADVANCE as i32) };
            if len != 0 {
                status.set_with_user_data(
                    QueryErrorCode::ParseArgs,
                    "Bad arguments",
                    &format!(" for <{name}>: Could not convert argument to expected type"),
                );
                return 1;
            }
            // SAFETY: empty is an exclusive caller-provided output flag.
            unsafe { *empty = true };
        }
        ac.offset += 1;
    }
    // SAFETY: the fifth argument exists and its string buffer remains live.
    let unit = unsafe { ffi::AC_GetStringNC(ac, std::ptr::null_mut()) };
    // SAFETY: ArgsCursor strings carry a NUL terminator.
    let unit = unsafe { CStr::from_ptr(unit) };
    let Some(unit_type) = DistanceUnit::parse(unit.to_bytes()) else {
        let message = [
            QueryErrorCode::ParseArgs.prefix_c_str().to_bytes(),
            b"Unknown distance unit ",
            unit.to_bytes(),
        ]
        .concat();
        status.set_code_and_messages(
            QueryErrorCode::ParseArgs,
            Some(c"Unknown distance unit".to_owned()),
            CString::new(message).ok(),
        );
        return 1;
    };
    // SAFETY: the field argument remains live and NUL-terminated.
    let len = unsafe { CStr::from_ptr(field) }.count_bytes();
    // SAFETY: the hidden wrapper borrows the field until context application.
    let field = unsafe { ffi::NewHiddenString(field, len, false) };
    gf.base = GeoFilter::new(lon, lat, radius, Some(unit_type));
    gf.field = field.cast();
    0
}
