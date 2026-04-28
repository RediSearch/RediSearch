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

use std::ffi::{CString, c_char, c_int};

use geo::ParseGeoError;
use query_error::{QueryError, QueryErrorCode, opaque::OpaqueQueryError};

const REDISMODULE_OK: c_int = 0;
const REDISMODULE_ERR: c_int = 1;

// The constants below use literals rather than cross-crate references (e.g.
// `geo::GEO_RANGE_COUNT`, `geo::hash::GEO_STEP_MAX`) because cbindgen cannot
// resolve cross-crate const references and would omit the `#define` from the
// generated C header. Compile-time asserts keep them in sync.

/// The number of geohash ranges computed by [`calcRanges`]: the center cell
/// plus its 8 neighbors.
pub const GEO_RANGE_COUNT: usize = 9;
const _: () = assert!(GEO_RANGE_COUNT == geo::GEO_RANGE_COUNT);

/// Maximum geohash step count. 26 steps * 2 bits = 52 bits of precision.
pub const GEO_STEP_MAX: u8 = 26;
const _: () = assert!(GEO_STEP_MAX == geo::hash::GEO_STEP_MAX);

/// WGS-84 latitude lower bound (EPSG:900913).
pub const GEO_LAT_MIN: f64 = -85.05112878;
const _: () = assert!(GEO_LAT_MIN.to_bits() == geo::GEO_LAT_MIN.to_bits());
/// WGS-84 latitude upper bound (EPSG:900913).
pub const GEO_LAT_MAX: f64 = 85.05112878;
const _: () = assert!(GEO_LAT_MAX.to_bits() == geo::GEO_LAT_MAX.to_bits());
/// WGS-84 longitude lower bound.
pub const GEO_LONG_MIN: f64 = -180.0;
const _: () = assert!(GEO_LONG_MIN.to_bits() == geo::GEO_LONG_MIN.to_bits());
/// WGS-84 longitude upper bound.
pub const GEO_LONG_MAX: f64 = 180.0;
const _: () = assert!(GEO_LONG_MAX.to_bits() == geo::GEO_LONG_MAX.to_bits());

/// A min/max range for a geohash cell, used by [`calcRanges`].
///
/// cbindgen:rename-all=None
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct GeoHashRange {
    /// Minimum score (inclusive).
    pub min: f64,
    /// Maximum score (exclusive).
    pub max: f64,
}

/// Encode longitude and latitude into a single `f64` geohash value.
///
/// Returns non-zero on success, 0 on failure.
///
/// # Safety
///
/// - `bits` must be a valid, non-null pointer to a writable `f64`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn encodeGeo(lon: f64, lat: f64, bits: *mut f64) -> c_int {
    match geo::hash::encode_wgs84(lon, lat, geo::hash::GEO_STEP_MAX) {
        Ok(hash) => {
            // SAFETY: caller guarantees `bits` is valid.
            unsafe {
                *bits = geo::hash::align_52bits(hash) as f64;
            }
            1
        }
        Err(_) => {
            // SAFETY: caller guarantees `bits` is valid.
            // Write 0.0 to match the original C behavior, which always wrote
            // to *bits (geohashAlign52Bits on a zeroed hash yields 0).
            unsafe {
                *bits = 0.0;
            }
            0
        }
    }
}

/// Decode a geohash `f64` back into a `[longitude, latitude]` pair.
///
/// Returns non-zero on success, 0 on failure.
///
/// # Safety
///
/// - `xy` must be a valid, non-null pointer to a writable `[f64; 2]`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn decodeGeo(bits: f64, xy: *mut f64) -> c_int {
    let hash = geo::hash::GeoHashBits {
        bits: bits as u64,
        step: geo::hash::GEO_STEP_MAX,
    };
    match geo::hash::decode_to_lon_lat(hash) {
        Ok((lon, lat)) => {
            // SAFETY: caller guarantees `xy` points to at least 2 writable `f64` values.
            unsafe {
                *xy = lon;
            }
            // SAFETY: caller guarantees `xy` points to at least 2 writable `f64` values.
            let xy_lat = unsafe { xy.add(1) };
            // SAFETY: `xy_lat` is valid per the above.
            unsafe {
                *xy_lat = lat;
            }
            1
        }
        Err(_) => 0,
    }
}

/// Calculate ranges for the 9 geohash cells (center + 8 neighbors) covering
/// a radius around a point.
///
/// # Safety
///
/// - `ranges` must be a valid pointer to a writable array of at least
///   [`geo::GEO_RANGE_COUNT`] [`GeoHashRange`] elements.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn calcRanges(
    longitude: f64,
    latitude: f64,
    radius_meters: f64,
    ranges: *mut GeoHashRange,
) {
    let computed = geo::calc_ranges(longitude, latitude, radius_meters);

    for (i, score_range) in computed.iter().enumerate() {
        // SAFETY: caller guarantees `ranges` has at least geo::GEO_RANGE_COUNT elements.
        // `i` is in 0..GEO_RANGE_COUNT, so `ranges.add(i)` is in bounds.
        let range_ptr = unsafe { ranges.add(i) };
        // SAFETY: `range_ptr` points to a valid, writable `GeoHashRange`.
        let range = unsafe { &mut *range_ptr };
        range.min = score_range.min;
        range.max = score_range.max;
    }
}

/// Calculate the haversine great-circle distance between two WGS-84 points.
///
/// All coordinates are in degrees. Returns distance in meters.
#[unsafe(no_mangle)]
pub extern "C" fn geohashGetDistance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    geo::hash::haversine_distance(lon1, lat1, lon2, lat2)
}

/// Return `true` if the distance between two lon/lat points is within `radius`
/// meters.
///
/// If `distance` is non-null, the actual distance (in meters) is written to it.
///
/// # Safety
///
/// - `distance` must be either null or a valid pointer to a writable `f64`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn isWithinRadiusLonLat(
    lon1: f64,
    lat1: f64,
    lon2: f64,
    lat2: f64,
    radius: f64,
    distance: *mut f64,
) -> bool {
    let dist = geo::hash::haversine_distance(lon1, lat1, lon2, lat2);
    if !distance.is_null() {
        // SAFETY: caller guarantees `distance` is valid when non-null.
        unsafe {
            *distance = dist;
        }
    }
    dist <= radius
}

/// Set a [`QueryError`] with a [`ParseGeoError`] message.
///
/// # Panics
///
/// - If `status` is null.
/// - If the [`ParseGeoError`] display string contains an interior nul byte.
///
/// # Safety
///
/// - `status` must be a valid, non-null pointer to an [`OpaqueQueryError`]
///   created by `QueryError_Default`.
unsafe fn set_parse_error(status: *mut OpaqueQueryError, err: ParseGeoError) {
    let msg = err.to_string();
    // SAFETY: caller guarantees `status` is valid.
    let query_error = unsafe { QueryError::from_opaque_mut_ptr(status) }.expect("status is null");
    let public_message = CString::new(msg.clone()).expect("error message contains nul byte");

    let prefix = QueryErrorCode::ParseArgs
        .prefix_c_str()
        .to_str()
        .unwrap_or("");
    let prefixed = format!("{prefix}{msg}");
    let private_message = CString::new(prefixed).unwrap_or_else(|_| public_message.clone());

    query_error.set_code_and_messages(
        QueryErrorCode::ParseArgs,
        Some(public_message),
        Some(private_message),
    );
}

/// Parse a `"lon,lat"` or `"lon lat"` string into separate longitude and
/// latitude values.
///
/// Returns [`REDISMODULE_OK`] on success, [`REDISMODULE_ERR`] on failure (with
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
    c: *const c_char,
    len: usize,
    lon: *mut f64,
    lat: *mut f64,
    status: *mut OpaqueQueryError,
) -> c_int {
    // SAFETY: caller guarantees `c` points to at least `len` bytes.
    let bytes = unsafe { std::slice::from_raw_parts(c.cast::<u8>(), len) };
    let s = match std::str::from_utf8(bytes) {
        Ok(s) => s,
        Err(_) => {
            // SAFETY: caller guarantees `status` is valid.
            unsafe {
                set_parse_error(status, ParseGeoError::Invalid);
            }
            return REDISMODULE_ERR;
        }
    };

    match geo::parse_geo(s) {
        Ok((parsed_lon, parsed_lat)) => {
            // SAFETY: caller guarantees `lon` is valid.
            unsafe {
                *lon = parsed_lon;
            }
            // SAFETY: caller guarantees `lat` is valid.
            unsafe {
                *lat = parsed_lat;
            }
            REDISMODULE_OK
        }
        Err(err) => {
            // SAFETY: caller guarantees `status` is valid.
            unsafe {
                set_parse_error(status, err);
            }
            REDISMODULE_ERR
        }
    }
}
