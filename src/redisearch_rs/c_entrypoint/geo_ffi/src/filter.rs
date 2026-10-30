/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Geo filter ABI. Filter storage uses the Redis allocator because legacy callers
//! allocate it in C; numeric-filter arrays retain their Rust allocation ownership.

use std::ffi::{CStr, CString, c_char, c_int};
use std::ptr;

use ffi::{ArgsCursor, FieldSpec, GeoDistance, GeoFilter, LegacyGeoFilter};
use geo::DistanceUnit;
use inverted_index::reader::{geo_filter_from_c, geo_unit_from_c};
use query_error::{QueryError, QueryErrorCode, opaque::OpaqueQueryError};
use rqe_core::RS_INVALID_FIELD_INDEX;

/// Encode a parsed unit using the shared C ABI constants.
const fn unit_to_c(unit: Option<DistanceUnit>) -> GeoDistance {
    match unit {
        Some(DistanceUnit::Kilometers) => ffi::GeoDistance_GEO_DISTANCE_KM,
        Some(DistanceUnit::Meters) => ffi::GeoDistance_GEO_DISTANCE_M,
        Some(DistanceUnit::Feet) => ffi::GeoDistance_GEO_DISTANCE_FT,
        Some(DistanceUnit::Miles) => ffi::GeoDistance_GEO_DISTANCE_MI,
        None => ffi::GeoDistance_GEO_DISTANCE_INVALID,
    }
}

/// Parse a NUL-terminated distance unit.
///
/// # Safety
/// `s` must be [valid] for reads through its NUL terminator.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoDistance_Parse(s: *const c_char) -> GeoDistance {
    debug_assert!(!s.is_null());
    // SAFETY: `s` is a terminated input string under this entrypoint's contract.
    let bytes = unsafe { CStr::from_ptr(s) }.to_bytes();
    unit_to_c(DistanceUnit::parse(bytes))
}

/// Parse a length-delimited unit. Buffers shorter than 16 bytes use their NUL
/// prefix; longer buffers are invalid, matching the query parser's buffer ABI.
///
/// # Safety
/// `s` must be non-null and [valid] for `len` readable bytes when `len < 16`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoDistance_Parse_Buffer(s: *const c_char, len: usize) -> GeoDistance {
    if len >= 16 {
        return ffi::GeoDistance_GEO_DISTANCE_INVALID;
    }
    debug_assert!(!s.is_null());
    // SAFETY: the short buffer is readable for `len` bytes by the input contract.
    let bytes = unsafe { std::slice::from_raw_parts(s.cast(), len) };
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    unit_to_c(DistanceUnit::parse(&bytes[..end]))
}

/// Canonical spelling, or `<badunit>` for any unknown integer.
#[unsafe(no_mangle)]
pub extern "C" fn GeoDistance_ToString(unit: GeoDistance) -> *const c_char {
    geo_unit_from_c(unit)
        .map_or(c"<badunit>", DistanceUnit::as_c_str)
        .as_ptr()
}

/// Initialize storage before parsing or parameter substitution.
const fn empty_filter() -> GeoFilter {
    GeoFilter {
        fieldIndex: RS_INVALID_FIELD_INDEX,
        lat: 0.0,
        lon: 0.0,
        radius: 0.0,
        unitType: ffi::GeoDistance_GEO_DISTANCE_KM,
        numericFilters: ptr::null_mut(),
    }
}

/// Allocate an unvalidated filter; a null unit selects kilometers.
///
/// # Safety
/// A non-null `unit` must satisfy [`GeoDistance_Parse_Buffer`]. The Redis
/// allocator API must be initialized and remain unchanged during this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewGeoFilter(
    lon: f64,
    lat: f64,
    radius: f64,
    unit: *const c_char,
    unit_len: usize,
) -> *mut GeoFilter {
    let unit_type = if unit.is_null() {
        ffi::GeoDistance_GEO_DISTANCE_KM
    } else {
        // SAFETY: the non-null unit buffer follows the delegated parsing contract.
        unsafe { GeoDistance_Parse_Buffer(unit, unit_len) }
    };
    // SAFETY: the initialized Redis API remains stable during module calls.
    let alloc =
        unsafe { redis_module::RedisModule_Alloc.expect("Redis allocator not initialized") };
    // SAFETY: the requested size is nonzero and the Redis allocator aligns C structs.
    let gf = unsafe { alloc(size_of::<GeoFilter>()) }.cast::<GeoFilter>();
    // SAFETY: Redis allocation succeeds or aborts; this fresh allocation is exclusively owned.
    unsafe {
        gf.write(GeoFilter {
            lon,
            lat,
            radius,
            unitType: unit_type,
            ..empty_filter()
        })
    };
    gf
}

/// Set the stable field index, or clear it when no local field specification exists.
///
/// # Safety
/// `gf` must be [valid] and exclusively writable. A non-null `fs` must be [valid]
/// for shared reads, without overlapping `gf`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoFilter_SetField(gf: *mut GeoFilter, fs: *const FieldSpec) {
    debug_assert!(!gf.is_null());
    // SAFETY: `fs` is either null or a readable, non-overlapping field specification.
    let index = unsafe { fs.as_ref() }.map_or(RS_INVALID_FIELD_INDEX, |fs| fs.index);
    // SAFETY: the caller provides exclusive writable filter storage.
    unsafe { (*gf).fieldIndex = index };
}

/// Validate a filter, preserving the first error already in `status`.
///
/// # Safety
/// `gf` must be [valid] for shared reads. `status` must be [valid], initialized
/// query-error storage exclusively writable and not overlapping `gf`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoFilter_Validate(
    gf: *const GeoFilter,
    status: *mut OpaqueQueryError,
) -> c_int {
    debug_assert!(!gf.is_null());
    debug_assert!(!status.is_null());
    // SAFETY: the filter is initialized and remains readable for this call.
    let gf = unsafe { &*gf };
    match geo_filter_from_c(gf) {
        Ok(_) => 1,
        Err(error) => {
            // SAFETY: status is initialized, exclusive query-error storage.
            let status = unsafe { QueryError::from_opaque_mut_ptr(status) }.unwrap();
            validation_error(status, error)
        }
    }
}

/// Keep error formatting and allocation off the successful validation path.
#[cold]
fn validation_error(status: &mut QueryError, error: geo::FilterError) -> c_int {
    status.set_error(QueryErrorCode::Syntax, &error.to_string());
    0
}

/// Return radius membership and optionally write the distance in meters.
/// Invalid filters return no match and NaN distance.
///
/// # Safety
/// `gf` must be [valid] for shared reads. `distance` may be null; otherwise it
/// must be [valid] and exclusively writable, without overlapping `gf`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn isWithinRadius(
    gf: *const GeoFilter,
    score: f64,
    distance: *mut f64,
) -> c_int {
    debug_assert!(!gf.is_null());
    // SAFETY: the input filter is initialized and readable for this call.
    let gf = unsafe { &*gf };
    let (within, meters) =
        geo_filter_from_c(gf).map_or((false, f64::NAN), |filter| filter.contains_score(score));
    // SAFETY: a non-null output is exclusively writable and disjoint from the filter.
    if let Some(distance) = unsafe { distance.as_mut() } {
        *distance = meters;
    }
    within.into()
}

/// Release a Redis-allocated filter and its owned numeric-filter array.
///
/// # Safety
/// `gf` must be a live, uniquely owned, [valid] Redis allocation containing a
/// [`GeoFilter`]. Its numeric array must satisfy
/// [`rqe_iterators::free_geo_numeric_filters`]. The Redis allocator API must be
/// initialized and remain unchanged during this call. No references may survive.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoFilter_Free(gf: *mut GeoFilter) {
    debug_assert!(!gf.is_null());
    // SAFETY: gf is initialized, readable storage uniquely owned by the caller.
    let filters = unsafe { (*gf).numericFilters };
    // SAFETY: the live filter owns this array, allocated by the matching Rust owner.
    unsafe { rqe_iterators::free_geo_numeric_filters(filters.cast()) };
    // SAFETY: the initialized Redis allocator API is stable during module calls.
    let free = unsafe { redis_module::RedisModule_Free.expect("Redis allocator not initialized") };
    // SAFETY: `gf` is the uniquely owned base of a Redis allocation, freed exactly once.
    unsafe { free(gf.cast()) };
}

/// Release a legacy filter, including its hidden field-name wrapper.
///
/// # Safety
/// `gf` must be a [valid] Redis allocation containing an initialized, uniquely
/// owned [`LegacyGeoFilter`], including after failed parsing. Its base must
/// satisfy [`GeoFilter_Free`]. The field wrapper, if present, must still
/// borrow a live name buffer and must not have been freed.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn LegacyGeoFilter_Free(gf: *mut LegacyGeoFilter) {
    debug_assert!(!gf.is_null());
    // SAFETY: this live legacy filter exclusively owns its field wrapper.
    let field = unsafe { (*gf).field };
    if !field.is_null() {
        // SAFETY: only the wrapper is owned; the command argument buffer is borrowed.
        unsafe { ffi::HiddenString_Free(field, false) };
    }
    // SAFETY: the base is the first repr(C) field, hence the Redis allocation base.
    unsafe { GeoFilter_Free(gf.cast()) };
}

/// Parse legacy arguments, initializing `gf` even on failure. The field name
/// borrows the command argument buffer. Empty numbers set `has_empty` to true.
///
/// # Safety
/// All pointers must be non-null, [valid], exclusively writable and mutually
/// disjoint. `gf` is uninitialized storage without owned resources. `ac` is an
/// initialized cursor with `offset <= argc` and readable NUL-terminated arguments
/// that outlive the returned filter. `status` is initialized query-error storage.
/// The Redis API must be initialized and remain unchanged during this call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoFilter_LegacyParse(
    gf: *mut LegacyGeoFilter,
    ac: *mut ArgsCursor,
    has_empty: *mut bool,
    status: *mut OpaqueQueryError,
) -> c_int {
    debug_assert!(!gf.is_null());
    debug_assert!(!ac.is_null());
    debug_assert!(!has_empty.is_null());
    debug_assert!(!status.is_null());
    // SAFETY: gf is exclusively writable storage with no resources to discard.
    unsafe {
        gf.write(LegacyGeoFilter {
            base: empty_filter(),
            field: ptr::null_mut(),
        })
    };
    // SAFETY: the cursor is initialized and uniquely borrowed for parsing.
    let ac = unsafe { &mut *ac };
    debug_assert!(ac.offset <= ac.argc);
    // SAFETY: status is initialized and disjoint from the other outputs.
    let status = unsafe { QueryError::from_opaque_mut_ptr(status) }.unwrap();
    if ac.argc - ac.offset < 5 {
        status.set_error(QueryErrorCode::ParseArgs, "GEOFILTER requires 5 arguments");
        return redis_module::REDISMODULE_ERR as c_int;
    }
    // SAFETY: at least five arguments remain; the field token is NUL-terminated.
    let field = unsafe { ffi::AC_GetStringNC(ac, ptr::null_mut()) };
    let mut filter = empty_filter();
    for (label, value) in [
        ("lon", &mut filter.lon),
        ("lat", &mut filter.lat),
        ("radius", &mut filter.radius),
    ] {
        // SAFETY: the current numeric argument exists; value is a disjoint local.
        let result = unsafe { ffi::AC_GetDouble(ac, value, ffi::AC_F_NOADVANCE as c_int) };
        if result != ffi::ACStatus_AC_OK as c_int {
            let mut bytes = ptr::null();
            let mut len = 0;
            // SAFETY: the same argument is still current and both outputs are local.
            unsafe { ffi::AC_GetString(ac, &mut bytes, &mut len, ffi::AC_F_NOADVANCE as c_int) };
            if len != 0 {
                status.set_with_user_data(
                    QueryErrorCode::ParseArgs,
                    "Bad arguments",
                    &format!(" for <{label}>: Could not convert argument to expected type"),
                );
                return redis_module::REDISMODULE_ERR as c_int;
            }
            *value = 0.0;
            // SAFETY: has_empty is a disjoint initialized, exclusively writable bool.
            unsafe { *has_empty = true };
        }
        ac.offset += 1;
    }
    // SAFETY: the fifth argument remains and is NUL-terminated.
    let unit = unsafe { ffi::AC_GetStringNC(ac, ptr::null_mut()) };
    // SAFETY: AC_GetStringNC returned the readable unit argument's NUL-terminated buffer.
    let unit = unsafe { CStr::from_ptr(unit) };
    filter.unitType = unit_to_c(DistanceUnit::parse(unit.to_bytes()));
    if filter.unitType == ffi::GeoDistance_GEO_DISTANCE_INVALID {
        let mut message = QueryErrorCode::ParseArgs.prefix_c_str().to_bytes().to_vec();
        message.extend_from_slice(b"Unknown distance unit ");
        message.extend_from_slice(unit.to_bytes());
        status.set_code_and_messages(
            QueryErrorCode::ParseArgs,
            Some(c"Unknown distance unit".to_owned()),
            Some(CString::new(message).unwrap()),
        );
        return redis_module::REDISMODULE_ERR as c_int;
    }
    // SAFETY: the borrowed field name remains readable through its NUL terminator.
    let len = unsafe { CStr::from_ptr(field) }.to_bytes().len();
    // SAFETY: the name buffer outlives the filter; the wrapper does not own that buffer.
    let field = unsafe { ffi::NewHiddenString(field, len, false) };
    // SAFETY: gf is initialized empty output storage, with no resources to discard.
    unsafe {
        gf.write(LegacyGeoFilter {
            base: filter,
            field,
        })
    };
    redis_module::REDISMODULE_OK as c_int
}
