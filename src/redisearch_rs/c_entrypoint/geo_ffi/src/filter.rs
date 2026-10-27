/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The legacy filter API, retaining C ownership and layout.

use geo::{FilterParameters, GeoDistance};
use query_error::{QueryError, QueryErrorCode, opaque::OpaqueQueryError};
use std::{
    ffi::{CStr, CString, c_char, c_int},
    ptr,
};

unsafe extern "C" {
    fn GeoFilter_FreeNumericFilters(filters: *mut *mut ffi::NumericFilter);
}

const fn empty_filter(lon: f64, lat: f64, radius: f64, unit: i32) -> ffi::GeoFilter {
    ffi::GeoFilter {
        fieldIndex: ffi::t_fieldIndex::MAX,
        lon,
        lat,
        radius,
        unitType: unit,
        numericFilters: ptr::null_mut(),
    }
}

/// Parse a NUL-terminated distance unit.
///
/// # Safety
/// 1. `s` must be [valid] and NUL-terminated.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoDistance_Parse(s: *const c_char) -> ffi::GeoDistance {
    debug_assert!(!s.is_null());
    // SAFETY: the caller guarantees (1).
    GeoDistance::parse(unsafe { CStr::from_ptr(s) }.to_bytes()) as i32
}

/// Parse a bounded distance unit.
///
/// # Safety
/// 1. `s` must be [valid] for `len` bytes when `len` is below the legacy buffer limit.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoDistance_Parse_Buffer(
    s: *const c_char,
    len: usize,
) -> ffi::GeoDistance {
    if len >= 16 {
        return GeoDistance::Invalid as i32;
    }
    if len == 0 {
        return GeoDistance::Invalid as i32;
    }
    debug_assert!(!s.is_null());
    // SAFETY: the caller guarantees (1); the length is nonzero.
    GeoDistance::parse_buffer(unsafe { std::slice::from_raw_parts(s.cast(), len) }) as i32
}

/// Return a static NUL-terminated unit spelling.
#[unsafe(no_mangle)]
pub const extern "C" fn GeoDistance_ToString(unit: ffi::GeoDistance) -> *const c_char {
    GeoDistance::from_raw(unit).as_c_str().as_ptr()
}

/// Allocate a filter. A null unit selects kilometers.
///
/// # Safety
/// 1. A non-null `unit` must be [valid] for `unit_len` bytes.
/// 2. The Redis allocator must be initialized. Release the result with [`GeoFilter_Free`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewGeoFilter(
    lon: f64,
    lat: f64,
    radius: f64,
    unit: *const c_char,
    unit_len: usize,
) -> *mut ffi::GeoFilter {
    let unit = if unit.is_null() {
        GeoDistance::Kilometers as i32
    } else {
        // SAFETY: the caller guarantees (1).
        unsafe { GeoDistance_Parse_Buffer(unit, unit_len) }
    };
    // SAFETY: the allocator is initialized (2).
    let alloc =
        unsafe { redis_module::RedisModule_Alloc.expect("Redis allocator not initialized") };
    // SAFETY: the allocation is nonzero and has the same alignment requirements as C.
    let filter = unsafe { alloc(size_of::<ffi::GeoFilter>()) }.cast::<ffi::GeoFilter>();
    assert!(!filter.is_null());
    // SAFETY: the fresh allocation holds a GeoFilter, exclusively owned here.
    unsafe { filter.write(empty_filter(lon, lat, radius, unit)) };
    filter
}

/// Assign the stable field index, clearing it for a null field.
///
/// # Safety
/// 1. `gf` must be [valid], writable and non-null. A non-null `fs` must be [valid] for reads.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoFilter_SetField(gf: *mut ffi::GeoFilter, fs: *const ffi::FieldSpec) {
    debug_assert!(!gf.is_null());
    // SAFETY: the caller guarantees (1).
    let index = unsafe { fs.as_ref() }.map_or(ffi::t_fieldIndex::MAX, |fs| fs.index);
    // SAFETY: the caller guarantees a writable filter (1).
    unsafe { (*gf).fieldIndex = index };
}

/// Validate a filter, retaining the historical NaN comparisons.
///
/// # Safety
/// 1. `gf` must be [valid] and non-null; `status` must point to an exclusively borrowed initialized query error.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoFilter_Validate(
    gf: *const ffi::GeoFilter,
    status: *mut OpaqueQueryError,
) -> c_int {
    // SAFETY: the caller guarantees (1).
    let gf = unsafe { gf.as_ref().expect("null filter") };
    let error = if gf.unitType == GeoDistance::Invalid as i32 {
        Err("Invalid GeoFilter unit")
    } else {
        FilterParameters {
            lon: gf.lon,
            lat: gf.lat,
            radius: gf.radius,
        }
        .validate()
    };
    if let Err(message) = error {
        // SAFETY: the caller guarantees (1).
        unsafe { QueryError::from_opaque_mut_ptr(status) }
            .expect("null status")
            .set_error(QueryErrorCode::Syntax, message);
        0
    } else {
        1
    }
}

/// Free a filter and its numeric ranges.
///
/// # Safety
/// 1. `gf` must be a [valid], exclusively owned Redis allocation containing a filter.
/// 2. Its ranges must be null or owned allocations from the geo iterator; no references may remain.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoFilter_Free(gf: *mut ffi::GeoFilter) {
    debug_assert!(!gf.is_null());
    // SAFETY: the caller guarantees (1).
    let filters = unsafe { (*gf).numericFilters };
    if !filters.is_null() {
        // SAFETY: the caller transfers the range allocation (2).
        unsafe { GeoFilter_FreeNumericFilters(filters) };
    }
    // SAFETY: the Redis allocator is initialized for the live allocation (1).
    let free = unsafe { redis_module::RedisModule_Free.expect("Redis allocator not initialized") };
    // SAFETY: the caller transfers ownership (1), and the nested ranges were released.
    unsafe { free(gf.cast()) };
}

/// Free a legacy filter, including its borrowed-name wrapper.
///
/// # Safety
/// 1. `gf` must be a [valid], exclusively owned Redis allocation, initialized by [`GeoFilter_LegacyParse`].
/// 2. The range ownership requirements of [`GeoFilter_Free`] apply.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn LegacyGeoFilter_Free(gf: *mut ffi::LegacyGeoFilter) {
    debug_assert!(!gf.is_null());
    // SAFETY: the caller guarantees (1).
    let field = unsafe { (*gf).field };
    if !field.is_null() {
        // SAFETY: the parser created a hidden-string wrapper borrowing its name (1).
        unsafe { ffi::HiddenString_Free(field, false) };
    }
    // SAFETY: base is the first member of the Redis allocation (1, 2).
    unsafe { GeoFilter_Free(gf.cast()) };
}

/// Parse the five legacy GEOFILTER arguments, allocating the field wrapper only on success.
///
/// # Safety
/// 1. `gf` must be [valid] writable storage; `ac` must be an exclusively borrowed initialized cursor.
/// 2. `has_empty` and `status` must be [valid], writable initialized values.
/// 3. Cursor string buffers must be NUL-terminated and remain alive until the filter is freed.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoFilter_LegacyParse(
    gf: *mut ffi::LegacyGeoFilter,
    ac: *mut ffi::ArgsCursor,
    has_empty: *mut bool,
    status: *mut OpaqueQueryError,
) -> c_int {
    debug_assert!(!gf.is_null() && !ac.is_null() && !has_empty.is_null() && !status.is_null());
    // SAFETY: the caller guarantees writable storage and cursor (1).
    unsafe {
        gf.write(ffi::LegacyGeoFilter {
            base: empty_filter(0.0, 0.0, 0.0, 0),
            field: ptr::null_mut(),
        });
    };
    // SAFETY: writable initialized filter storage (1).
    let gf = unsafe { &mut *gf };
    // SAFETY: exclusively borrowed initialized cursor (1).
    let ac = unsafe { &mut *ac };
    // SAFETY: the caller guarantees (2).
    let status = unsafe { QueryError::from_opaque_mut_ptr(status) }.expect("null status");
    if ac.argc - ac.offset < 5 {
        status.set_error(QueryErrorCode::ParseArgs, "GEOFILTER requires 5 arguments");
        return 1;
    }
    let mut field = ptr::null();
    // SAFETY: the cursor is valid and contains at least five arguments (1, 3).
    let rv = unsafe { ffi::AC_GetString(ac, &mut field, ptr::null_mut(), 0) };
    if rv != 0 {
        bad_argument(status, "geo property", rv);
        return 1;
    }
    for (target, name) in [
        (&mut gf.base.lon, "lon"),
        (&mut gf.base.lat, "lat"),
        (&mut gf.base.radius, "radius"),
    ] {
        // SAFETY: the cursor has the next numeric argument (1, 3).
        let rv = unsafe { ffi::AC_GetDouble(ac, target, ffi::AC_F_NOADVANCE as i32) };
        if rv != 0 {
            let mut value = ptr::null();
            let mut len = 0;
            // SAFETY: inspecting the same cursor argument does not advance it (1, 3).
            let string_rv =
                unsafe { ffi::AC_GetString(ac, &mut value, &mut len, ffi::AC_F_NOADVANCE as i32) };
            if string_rv != 0 || len != 0 {
                bad_argument(status, name, rv);
                return 1;
            }
            *target = 0.0;
            // SAFETY: writable initialized flag (2). Non-empty values leave it untouched.
            unsafe { *has_empty = true };
        }
        // SAFETY: one of the five checked arguments is being consumed (1).
        unsafe { ffi::AC_Advance(ac) };
    }
    // SAFETY: the checked fifth argument is NUL-terminated (1, 3).
    let unit = unsafe { ffi::AC_GetStringNC(ac, ptr::null_mut()) };
    // SAFETY: the cursor returned a NUL-terminated string (3).
    gf.base.unitType = unsafe { GeoDistance_Parse(unit) };
    if gf.base.unitType == GeoDistance::Invalid as i32 {
        // Preserve arbitrary user bytes, including non-UTF8, in the private message.
        let public = CString::new("Unknown distance unit").unwrap();
        let mut private = QueryErrorCode::ParseArgs.prefix_c_str().to_bytes().to_vec();
        private.extend_from_slice(public.as_bytes());
        private.push(b' ');
        // SAFETY: the cursor returned a NUL-terminated string (3).
        private.extend_from_slice(unsafe { CStr::from_ptr(unit) }.to_bytes());
        status.set_code_and_messages(
            QueryErrorCode::ParseArgs,
            Some(public),
            Some(CString::new(private).unwrap()),
        );
        return 1;
    }
    // SAFETY: the field name is NUL-terminated and outlives the filter (3).
    let len = unsafe { CStr::from_ptr(field) }.count_bytes();
    // SAFETY: the field name remains borrowed and the wrapper is newly owned (3).
    gf.field = unsafe { ffi::NewHiddenString(field, len, false) };
    0
}

fn bad_argument(status: &mut QueryError, name: &str, rv: c_int) {
    let detail = match rv {
        0 => "SUCCESS",
        1 => "Could not convert argument to expected type",
        2 => "Expected an argument, but none provided",
        3 => "Value is outside acceptable bounds",
        4 => "Unknown argument",
        _ => "(AC: You should not be seeing this message. This is a bug)",
    };
    status.set_with_user_data(
        QueryErrorCode::ParseArgs,
        "Bad arguments",
        &format!(" for <{name}>: {detail}"),
    );
}

/// Test an encoded point against a filter, writing the distance in meters.
///
/// Retains the historical boolean Rust boundary used by the C integer API.
///
/// # Safety
/// 1. `gf` must be [valid], non-null, with finite coordinates and a recognized unit.
/// 2. `distance` must be null or [valid] for writing a distance.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[cheadergen::config(skip)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn isWithinRadius(
    gf: *const ffi::GeoFilter,
    bits: f64,
    distance: *mut f64,
) -> bool {
    debug_assert!(!gf.is_null());
    // SAFETY: the caller guarantees (1).
    let gf = unsafe { &*gf };
    let mut xy = [0.0; 2];
    // SAFETY: xy contains two writable doubles.
    unsafe { crate::decodeGeo(bits, xy.as_mut_ptr()) };
    let radius = gf.radius * GeoDistance::from_raw(gf.unitType).factor();
    // SAFETY: finite coordinates (1) and writable distance if non-null (2).
    unsafe { crate::isWithinRadiusLonLat(gf.lon, gf.lat, xy[0], xy[1], radius, distance) }
}
