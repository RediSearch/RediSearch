/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C boundary for [`geo`] filter parameters and legacy command parsing.
//!
//! Filter layouts remain C-owned. Allocations use Redis' allocator, not Rust
//! boxes: the request parser also allocates [`LegacyGeoFilter`] directly.

use std::{
    ffi::{CStr, CString, c_char, c_int},
    ptr,
};

use ffi::{ArgsCursor, FieldSpec, GeoDistance, GeoFilter, LegacyGeoFilter};
use geo::{DistanceUnit, FilterParams};
use query_error::{QueryError, QueryErrorCode, opaque::OpaqueQueryError};
use redis_module::raw::{RedisModule_Alloc, RedisModule_Free};
use rqe_core::RS_INVALID_FIELD_INDEX;

/// Interpret the C unit tag without creating an invalid Rust enum value.
const fn unit_from_c(unit: GeoDistance) -> Option<DistanceUnit> {
    match unit {
        ffi::GeoDistance_GEO_DISTANCE_KM => Some(DistanceUnit::Kilometers),
        ffi::GeoDistance_GEO_DISTANCE_M => Some(DistanceUnit::Meters),
        ffi::GeoDistance_GEO_DISTANCE_FT => Some(DistanceUnit::Feet),
        ffi::GeoDistance_GEO_DISTANCE_MI => Some(DistanceUnit::Miles),
        _ => None,
    }
}

/// Borrow the numeric parameters without borrowing index ownership.
const fn params(gf: &GeoFilter) -> FilterParams {
    FilterParams {
        lon: gf.lon,
        lat: gf.lat,
        radius: gf.radius,
        unit: unit_from_c(gf.unitType),
    }
}

/// Parse a NUL-terminated unit name.
///
/// # Safety
///
/// `s` must be a non-null, [valid] NUL-terminated string for this call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RustGeoDistance_Parse(s: *const c_char) -> GeoDistance {
    debug_assert!(!s.is_null());
    // SAFETY: the caller guarantees a readable NUL-terminated string.
    let s = unsafe { CStr::from_ptr(s) };
    DistanceUnit::parse(s.to_bytes()).map_or(ffi::GeoDistance_GEO_DISTANCE_INVALID, |u| u as i32)
}

/// Parse a length-delimited unit with [`DistanceUnit::parse_buffer`] semantics.
///
/// # Safety
///
/// For lengths 1 through 15, `s` must be non-null and [valid] for `len` readable
/// bytes. Other lengths return invalid without accessing `s`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RustGeoDistance_Parse_Buffer(s: *const c_char, len: usize) -> GeoDistance {
    if len == 0 || len >= 16 {
        return ffi::GeoDistance_GEO_DISTANCE_INVALID;
    }
    debug_assert!(!s.is_null());
    // SAFETY: the caller guarantees `len` readable bytes for these lengths.
    let bytes = unsafe { std::slice::from_raw_parts(s.cast(), len) };
    DistanceUnit::parse_buffer(bytes).map_or(ffi::GeoDistance_GEO_DISTANCE_INVALID, |u| u as i32)
}

/// Return a static lowercase unit name, or `"<badunit>"` for an unknown tag.
#[unsafe(no_mangle)]
pub extern "C" fn RustGeoDistance_ToString(unit: GeoDistance) -> *const c_char {
    unit_from_c(unit)
        .map_or(c"<badunit>", DistanceUnit::as_c_str)
        .as_ptr()
}

/// Allocate a filter, defaulting to kilometers only when `unit` is null.
///
/// # Safety
///
/// 1. A non-null `unit` must satisfy [`RustGeoDistance_Parse_Buffer`].
/// 2. Redis' allocator API must be initialized and remain unchanged during the call.
///
/// The returned allocation belongs to the caller and must be released with
/// [`RustGeoFilter_Free`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RustNewGeoFilter(
    lon: f64,
    lat: f64,
    radius: f64,
    unit: *const c_char,
    unit_len: usize,
) -> *mut GeoFilter {
    let unit_type = if unit.is_null() {
        DistanceUnit::default() as i32
    } else {
        // SAFETY: precondition 1 forwards the unit buffer contract.
        unsafe { RustGeoDistance_Parse_Buffer(unit, unit_len) }
    };
    // SAFETY: precondition 2 guarantees the allocator table is initialized and stable.
    let alloc = unsafe { RedisModule_Alloc.expect("Redis allocator not initialized") };
    // SAFETY: Redis provides aligned storage or aborts on allocation failure.
    let gf = unsafe { alloc(size_of::<GeoFilter>()) }.cast::<GeoFilter>();
    debug_assert!(!gf.is_null());
    // SAFETY: `gf` owns enough aligned, writable storage; write does not read uninitialized data.
    unsafe {
        gf.write(GeoFilter {
            fieldIndex: RS_INVALID_FIELD_INDEX,
            lon,
            lat,
            radius,
            unitType: unit_type,
            numericFilters: ptr::null_mut(),
        })
    };
    gf
}

/// Store the stable field index rather than a pointer into a reallocatable schema.
///
/// # Safety
///
/// `gf` must be non-null and [valid] for exclusive access. `fs` may be null,
/// otherwise it must be [valid] for a shared read during this call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RustGeoFilter_SetField(gf: *mut GeoFilter, fs: *const FieldSpec) {
    debug_assert!(!gf.is_null());
    // SAFETY: the caller permits null or a readable FieldSpec.
    let field = unsafe { fs.as_ref() }.map_or(RS_INVALID_FIELD_INDEX, |fs| fs.index);
    // SAFETY: the caller provides exclusive, initialized filter storage.
    unsafe { (*gf).fieldIndex = field };
}

/// Validate the numeric parameters, retaining the first error in `status`.
///
/// # Safety
///
/// `gf` must be non-null and [valid] for a shared read, with a declared C unit tag.
/// `status` must be non-null and [valid] for exclusive access to an initialized
/// [`OpaqueQueryError`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RustGeoFilter_Validate(
    gf: *const GeoFilter,
    status: *mut OpaqueQueryError,
) -> c_int {
    debug_assert!(!gf.is_null());
    // SAFETY: the caller guarantees an initialized filter.
    let result = params(unsafe { &*gf }).validate();
    if let Err(error) = result {
        debug_assert!(!status.is_null());
        // SAFETY: the caller guarantees an exclusively accessible initialized error.
        let status = unsafe { QueryError::from_opaque_mut_ptr(status) }.expect("status is null");
        status.set_error(QueryErrorCode::Syntax, &error.to_string());
        0
    } else {
        1
    }
}

/// Check the inclusive radius and optionally write the distance in meters.
///
/// # Panics
///
/// The panic conditions of [`FilterParams::within_radius`] apply.
///
/// # Safety
///
/// `gf` must be non-null and [valid] for a shared read. `distance` must be null
/// or [valid] for an exclusive write, without overlapping the filter.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RustIsWithinRadius(
    gf: *const GeoFilter,
    score: f64,
    distance: *mut f64,
) -> c_int {
    debug_assert!(!gf.is_null());
    // SAFETY: the caller guarantees an initialized filter.
    let (found, meters) = params(unsafe { &*gf }).within_radius(score);
    if !distance.is_null() {
        // SAFETY: the caller guarantees exclusive writable storage, which need
        // not already contain an initialized f64.
        unsafe { distance.write(meters) };
    }
    c_int::from(found)
}

/// Release the numeric-filter array and the Redis-allocated filter.
///
/// # Safety
///
/// 1. `gf` must be non-null and [valid], exclusively owned, allocated with Redis'
///    allocator, and not yet freed. No iterator may still borrow it.
/// 2. Its numeric-filter array must satisfy [`rqe_iterators::free_geo_numeric_filters`].
/// 3. Redis' allocator API must be initialized and stable during this call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RustGeoFilter_Free(gf: *mut GeoFilter) {
    debug_assert!(!gf.is_null());
    // SAFETY: precondition 1 guarantees an initialized filter.
    let filters = unsafe { (*gf).numericFilters };
    // SAFETY: precondition 2 forwards ownership of the array and its entries.
    unsafe { rqe_iterators::free_geo_numeric_filters(filters.cast()) };
    // SAFETY: precondition 3 guarantees the allocator table is stable.
    let free = unsafe { RedisModule_Free.expect("Redis allocator not initialized") };
    // SAFETY: precondition 1 transfers the original Redis allocation to its matching free.
    unsafe { free(gf.cast()) };
}

/// Release a legacy filter, including its borrowed-name wrapper.
///
/// # Safety
///
/// `gf` must satisfy [`RustGeoFilter_Free`] for its base, and point to the start
/// of a Redis-allocated [`LegacyGeoFilter`]. Its field must be null or an owned
/// `HiddenString` created with `takeOwnership = false` and not yet freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RustLegacyGeoFilter_Free(gf: *mut LegacyGeoFilter) {
    debug_assert!(!gf.is_null());
    // SAFETY: the caller guarantees an initialized legacy filter.
    let field = unsafe { (*gf).field };
    if !field.is_null() {
        // SAFETY: the caller transfers ownership of this borrowed-name wrapper.
        unsafe { ffi::HiddenString_Free(field, false) };
    }
    // SAFETY: the C layout places the base first. Redis free has no Rust layout
    // argument, so this releases the entire legacy allocation, not a smaller Box.
    unsafe { RustGeoFilter_Free(gf.cast()) };
}

/// Parse the five legacy arguments, leaving the cursor at the failing number
/// or after the unit. On failure `gf` is initialized and can be freed.
///
/// Numeric conversion follows [`ffi::AC_GetDouble`], including its backend
/// differences: an empty Redis string sets `has_empty`, while an empty C string
/// parses as zero without setting it. A previously set `has_empty` stays set.
///
/// # Safety
///
/// 1. `gf` must be non-null, [valid] writable storage for a fresh [`LegacyGeoFilter`].
/// 2. `ac` must be a non-null, exclusively accessible initialized [`ArgsCursor`]
///    with offset no greater than argc and readable, NUL-terminated arguments.
///    On success the field-name argument must stay alive and unmodified until
///    the filter is freed.
/// 3. `has_empty` and `status` must be non-null and [valid] for exclusive access
///    to initialized values; `status` was created as an [`OpaqueQueryError`].
/// 4. Redis' allocator and string APIs must be initialized and stable. All mutable
///    arguments must be disjoint from each other and the cursor's argument buffers.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RustGeoFilter_LegacyParse(
    gf: *mut LegacyGeoFilter,
    ac: *mut ArgsCursor,
    has_empty: *mut bool,
    status: *mut OpaqueQueryError,
) -> c_int {
    debug_assert!(!gf.is_null() && !ac.is_null() && !has_empty.is_null() && !status.is_null());
    // SAFETY: precondition 1 provides fresh writable storage.
    unsafe {
        gf.write(LegacyGeoFilter {
            base: GeoFilter {
                fieldIndex: RS_INVALID_FIELD_INDEX,
                lat: 0.0,
                lon: 0.0,
                radius: 0.0,
                unitType: ffi::GeoDistance_GEO_DISTANCE_KM,
                numericFilters: ptr::null_mut(),
            },
            field: ptr::null_mut(),
        })
    };
    // SAFETY: preconditions 1 and 4 give exclusive access to the newly initialized output.
    let gf = unsafe { &mut *gf };
    // SAFETY: preconditions 2 and 4 give exclusive access to the cursor.
    let ac = unsafe { &mut *ac };
    // SAFETY: preconditions 3 and 4 give exclusive access to the initialized error.
    let status = unsafe { QueryError::from_opaque_mut_ptr(status) }.expect("status is null");
    debug_assert!(ac.offset <= ac.argc);
    if ac.argc - ac.offset < 5 {
        status.set_error(QueryErrorCode::ParseArgs, "GEOFILTER requires 5 arguments");
        return redis_module::REDISMODULE_ERR as c_int;
    }
    let mut field = ptr::null();
    // SAFETY: the argument count was checked and `field` is a disjoint output.
    let rv = unsafe { ffi::AC_GetString(ac, &mut field, ptr::null_mut(), 0) };
    debug_assert_eq!(rv, 0);
    for (name, target) in [
        ("lon", &mut gf.base.lon),
        ("lat", &mut gf.base.lat),
        ("radius", &mut gf.base.radius),
    ] {
        // SAFETY: the five-argument check bounds every read; target is disjoint from the cursor.
        let rv = unsafe { ffi::AC_GetDouble(ac, target, ffi::AC_F_NOADVANCE as c_int) };
        if rv != 0 {
            let mut value = ptr::null();
            let mut len = 0;
            // SAFETY: the cursor has not advanced past this checked argument.
            let string_rv = unsafe {
                ffi::AC_GetString(ac, &mut value, &mut len, ffi::AC_F_NOADVANCE as c_int)
            };
            debug_assert_eq!(string_rv, 0);
            if len != 0 {
                status.set_with_user_data(
                    QueryErrorCode::ParseArgs,
                    "Bad arguments",
                    &format!(" for <{name}>: Could not convert argument to expected type"),
                );
                return redis_module::REDISMODULE_ERR as c_int;
            }
            *target = 0.0;
            // SAFETY: precondition 3 provides a disjoint writable boolean.
            unsafe { *has_empty = true };
        }
        // SAFETY: the current argument exists, and advancing preserves offset <= argc.
        unsafe { ffi::AC_Advance(ac) };
    }
    // SAFETY: the fifth checked argument remains available.
    let unit = unsafe { ffi::AC_GetStringNC(ac, ptr::null_mut()) };
    debug_assert!(!unit.is_null());
    // SAFETY: precondition 2 guarantees the argument's terminating NUL.
    let unit = unsafe { CStr::from_ptr(unit) };
    gf.base.unitType = DistanceUnit::parse(unit.to_bytes())
        .map_or(ffi::GeoDistance_GEO_DISTANCE_INVALID, |u| u as i32);
    if gf.base.unitType == ffi::GeoDistance_GEO_DISTANCE_INVALID {
        // Keep arbitrary unit bytes in the private error, never in the public message.
        let private = [
            QueryErrorCode::ParseArgs.prefix_c_str().to_bytes(),
            b"Unknown distance unit ",
            unit.to_bytes(),
        ]
        .concat();
        status.set_code_and_messages(
            QueryErrorCode::ParseArgs,
            Some(c"Unknown distance unit".to_owned()),
            Some(CString::new(private).expect("CStr contents exclude NUL")),
        );
        return redis_module::REDISMODULE_ERR as c_int;
    }
    debug_assert!(!field.is_null());
    // SAFETY: precondition 2 guarantees the field-name argument's terminating NUL.
    let field_len = unsafe { CStr::from_ptr(field) }.to_bytes().len();
    // SAFETY: the name remains borrowed until filter destruction per precondition 2;
    // its buffer contains field_len bytes followed by NUL and Redis APIs are initialized.
    gf.field = unsafe { ffi::NewHiddenString(field, field_len, false) };
    redis_module::REDISMODULE_OK as c_int
}
