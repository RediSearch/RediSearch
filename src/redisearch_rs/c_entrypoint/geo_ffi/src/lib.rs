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

use std::{
    ffi::{CString, c_char, c_int},
    ptr::NonNull,
};

use decorum::R64;
use geo::ParseGeoError;
use query_error::{QueryError, QueryErrorCode, opaque::OpaqueQueryError};

/// The number of geohash ranges: the center cell plus its 8 neighbors.
// This is a literal rather than `geo::GEO_RANGE_COUNT` because cbindgen cannot
// resolve cross-crate const references and would omit the `#define` from the
// generated C header.
pub const GEO_RANGE_COUNT: usize = 9;
const _: () = assert!(GEO_RANGE_COUNT == geo::GEO_RANGE_COUNT);

/// Encode longitude and latitude into a single `f64` geohash value.
///
/// Returns non-zero on success, 0 on failure.
///
/// # Safety
///
/// - `bits` must be a valid, non-null pointer to a writable `f64`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn encodeGeo(lon: f64, lat: f64, bits: *mut f64) -> c_int {
    let coords = match geo::hash::WGS84Coordinates::from_f64(lon, lat) {
        Ok(coords) => coords,
        Err(err) => {
            tracing::warn!(%err, lon, lat, "encodeGeo: invalid WGS-84 coordinates");
            // SAFETY: caller guarantees `bits` is valid.
            unsafe { *bits = 0.0 };
            return 0;
        }
    };
    let hash = geo::hash::encode_wgs84(coords, geo::hash::GEO_STEP_MAX);
    // SAFETY: caller guarantees `bits` is valid.
    unsafe {
        *bits = geo::hash::align_52bits(hash) as f64;
    }
    1
}

/// Decode a geohash `f64` back into a `[longitude, latitude]` pair.
///
/// Always succeeds — zero is a valid geohash encoding (for the boundary
/// coordinate -180, -85.05112878). Returns 1 unconditionally.
///
/// # Safety
///
/// - `xy` must be a valid, non-null pointer to a writable `[f64; 2]`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn decodeGeo(bits: f64, xy: *mut f64) -> c_int {
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
    1
}

/// Return `true` if the distance between two lon/lat points is within `radius`
/// meters.
///
/// If `distance` is non-null, the actual distance (in meters) is written to it.
///
/// # Panics
///
/// If any coordinate is non-finite (NaN or infinity). This cannot happen in
/// practice: `lon1`/`lat1` come from `GeoFilter` which is range-checked by
/// `GeoFilter_Validate`, and `lon2`/`lat2` come from `decodeGeo` which
/// produces finite values by construction (integer bit-arithmetic scaled to
/// WGS-84 bounds).
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
    let lon1 = R64::assert(lon1);
    let lat1 = R64::assert(lat1);
    let lon2 = R64::assert(lon2);
    let lat2 = R64::assert(lat2);
    let dist = geo::hash::haversine_distance(lon1, lat1, lon2, lat2);
    if let Some(d) = NonNull::new(distance) {
        // SAFETY: caller guarantees `distance` is valid when non-null.
        unsafe { d.as_ptr().write(dist) };
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
/// - `status` must be a valid pointer to an [`OpaqueQueryError`] created by
///   `QueryError_Default`.
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
                set_parse_error(status, ParseGeoError::InvalidUtf8);
            }
            return ffi::REDISMODULE_ERR as i32;
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
            ffi::REDISMODULE_OK as i32
        }
        Err(err) => {
            // SAFETY: caller guarantees `status` is valid.
            unsafe {
                set_parse_error(status, err);
            }
            ffi::REDISMODULE_ERR as i32
        }
    }
}
