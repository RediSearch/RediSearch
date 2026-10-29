/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::{CStr, CString, c_char, c_int};
use std::ptr;

use ffi::{ArgsCursor, FieldSpec, GeoDistance, GeoFilter, LegacyGeoFilter};
use geo::{DistanceUnit, RadiusFilter};
use query_error::{QueryError, QueryErrorCode, opaque::OpaqueQueryError};
use rqe_core::RS_INVALID_FIELD_INDEX;

/// Parse a case-insensitive, NUL-terminated distance unit.
///
/// # Safety
/// `s` must be [valid] for reads through its NUL terminator.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn GeoDistance_Parse(s: *const c_char) -> GeoDistance {
    debug_assert!(!s.is_null());
    // SAFETY: the caller supplies a readable C string.
    DistanceUnit::parse(unsafe { CStr::from_ptr(s) }) as GeoDistance
}

/// Parse a length-delimited unit with [`DistanceUnit::parse_buffer`] semantics.
///
/// # Safety
/// When `len` is below [`geo::UNIT_BUFFER_SIZE`], `s` must be non-null and [valid]
/// for `len` bytes of reads. Larger lengths are rejected without accessing `s`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoDistance_Parse_Buffer(s: *const c_char, len: usize) -> GeoDistance {
    if len >= geo::UNIT_BUFFER_SIZE {
        return DistanceUnit::Invalid as GeoDistance;
    }
    debug_assert!(!s.is_null());
    // SAFETY: the caller supplies the readable buffer.
    DistanceUnit::parse_buffer(unsafe { std::slice::from_raw_parts(s.cast(), len) }) as GeoDistance
}

/// Return static unit text; unknown discriminants use the invalid-unit placeholder.
#[unsafe(no_mangle)]
pub extern "C" fn GeoDistance_ToString(unit: GeoDistance) -> *const c_char {
    DistanceUnit::from_code(unit)
        .unwrap_or(DistanceUnit::Invalid)
        .as_c_str()
        .as_ptr()
}

/// Allocate a filter without validation, to be freed by [`GeoFilter_Free`].
/// A null `unit` defaults to kilometers regardless of `unit_len`.
///
/// # Safety
/// The Redis allocator must be initialized and remain unchanged during the call.
/// A non-null `unit` must be [valid] for `unit_len` bytes of reads only when
/// `unit_len` is below [`geo::UNIT_BUFFER_SIZE`]; larger lengths are rejected unread.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewGeoFilter(
    lon: f64,
    lat: f64,
    radius: f64,
    unit: *const c_char,
    unit_len: usize,
) -> *mut GeoFilter {
    let unit = if unit.is_null() {
        DistanceUnit::Kilometers as GeoDistance
    } else {
        // SAFETY: non-null unit follows the same length and readability contract.
        unsafe { GeoDistance_Parse_Buffer(unit, unit_len) }
    };
    // SAFETY: the allocator is initialized and not concurrently modified.
    let alloc =
        unsafe { redis_module::RedisModule_Alloc.expect("Redis allocator not initialized") };
    // SAFETY: the allocation size matches the C layout.
    let gf = unsafe { alloc(size_of::<GeoFilter>()) }.cast::<GeoFilter>();
    assert!(!gf.is_null(), "Redis allocator returned null");
    // SAFETY: gf is a newly allocated, aligned, exclusively owned GeoFilter-sized block.
    unsafe {
        gf.write(GeoFilter {
            fieldIndex: RS_INVALID_FIELD_INDEX,
            lon,
            lat,
            radius,
            unitType: unit,
            numericFilters: ptr::null_mut(),
        });
    }
    gf
}

/// Store the stable field index, or [`RS_INVALID_FIELD_INDEX`] for null `fs`.
///
/// # Safety
/// `gf` must be [valid] and exclusively writable; non-null `fs` must be [valid]
/// for reads and not overlap `gf`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoFilter_SetField(gf: *mut GeoFilter, fs: *const FieldSpec) {
    debug_assert!(!gf.is_null());
    // SAFETY: the caller supplies a readable field or null.
    let index = unsafe { fs.as_ref() }.map_or(RS_INVALID_FIELD_INDEX, |fs| fs.index);
    // SAFETY: gf is exclusively writable.
    unsafe { (*gf).fieldIndex = index };
}

/// Return one if valid; otherwise return zero and record the first validation error.
/// As in C, only [`DistanceUnit::Invalid`]'s discriminant is rejected; unnamed unit
/// integers pass this check and use [`isWithinRadius`]'s fallback behavior.
///
/// # Safety
/// `gf` must be [valid] for reads. `status` must be an exclusively writable,
/// initialized [`OpaqueQueryError`], disjoint from `gf`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoFilter_Validate(
    gf: &GeoFilter,
    status: *mut OpaqueQueryError,
) -> c_int {
    let filter = RadiusFilter {
        lon: gf.lon,
        lat: gf.lat,
        radius: gf.radius,
        unit: DistanceUnit::from_code(gf.unitType).unwrap_or(DistanceUnit::Kilometers),
    };
    match filter.validate() {
        Ok(()) => 1,
        Err(error) => {
            // SAFETY: status is initialized and exclusively writable.
            let status =
                unsafe { QueryError::from_opaque_mut_ptr(status) }.expect("status is null");
            set_validation_error(status, error)
        }
    }
}

// Keep formatting and allocation spills off the successful validation path.
#[cold]
#[inline(never)]
fn set_validation_error(status: &mut QueryError, error: geo::FilterError) -> c_int {
    status.set_error(QueryErrorCode::Syntax, &error.to_string());
    0
}

/// Test a decoded score with [`RadiusFilter::within_radius`], optionally writing meters.
/// Invalid and unnamed units retain C's release-mode fallback from [`DistanceUnit::meters_factor`].
///
/// # Panics
/// Aborts the process for non-finite centers.
/// Successful [`GeoFilter_Validate`] does not rule out those inputs.
///
/// # Safety
/// `gf` must be [valid] for reads. Non-null `distance` must be [valid] for an exclusive
/// write and must not overlap `gf`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn isWithinRadius(
    gf: *const GeoFilter,
    score: f64,
    distance: *mut f64,
) -> c_int {
    debug_assert!(!gf.is_null());
    // SAFETY: gf is readable for this call.
    let gf = unsafe { &*gf };
    let filter = RadiusFilter {
        lon: gf.lon,
        lat: gf.lat,
        radius: gf.radius,
        unit: DistanceUnit::from_code(gf.unitType).unwrap_or(DistanceUnit::Invalid),
    };
    let (inside, meters) = filter.within_radius(score);
    if !distance.is_null() {
        // SAFETY: the caller supplies disjoint writable storage; it need not be initialized.
        unsafe { distance.write(meters) };
    }
    c_int::from(inside)
}

/// Release a filter and its owned numeric ranges, returning the array to its Rust owner.
///
/// # Safety
/// `gf` must be a live, uniquely owned Redis-allocated [`GeoFilter`], with numeric
/// ranges null or allocated by [`rqe_iterators::build_geo_numeric_filters`]. No
/// iterators may still borrow those ranges. The Redis allocator must remain initialized
/// and unchanged. The pointer and all its owned ranges become invalid after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoFilter_Free(gf: *mut GeoFilter) {
    debug_assert!(!gf.is_null());
    // SAFETY: gf is live and its numeric array follows the original Rust owner's contract.
    let filters = unsafe { (*gf).numericFilters };
    // SAFETY: ownership of the array and all entries is transferred by the caller.
    unsafe { rqe_iterators::free_geo_numeric_filters(filters.cast()) };
    // SAFETY: the Redis allocator is initialized and not concurrently modified.
    let free = unsafe { redis_module::RedisModule_Free.expect("Redis allocator not initialized") };
    // SAFETY: gf is a uniquely owned Redis allocation.
    unsafe { free(gf.cast()) };
}

/// Release a legacy filter without releasing its borrowed field-name buffer.
///
/// # Safety
/// `gf` must be a live, uniquely owned Redis-allocated [`LegacyGeoFilter`], initialized
/// by [`GeoFilter_LegacyParse`] (including on failure).
/// Its base must satisfy [`GeoFilter_Free`]'s contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn LegacyGeoFilter_Free(gf: *mut LegacyGeoFilter) {
    debug_assert!(!gf.is_null());
    // SAFETY: gf is initialized and uniquely owned.
    let field = unsafe { (*gf).field };
    // SAFETY: field is a nullable wrapper with borrowed storage; false permits null.
    unsafe { ffi::HiddenString_Free(field, false) };
    // SAFETY: repr(C) base is the first field, so its pointer is the allocation's start.
    unsafe { GeoFilter_Free(gf.cast()) };
}

/// Parse the five legacy GEOFILTER arguments using C's numeric conversion semantics.
/// The field name is borrowed only on success. Failed parses retain the cursor's partial
/// progress and leave the filter safe to destroy. Returns the Redis OK/ERR code.
///
/// Failed numeric conversions of empty tokens become zero and set `has_empty`: DIALECT 1
/// accepts these values, while later dialects reject them through the caller's flag check.
/// Successful [`ArgsCursor`] conversions leave the flag unchanged; this parser never clears it.
///
/// # Safety
/// `gf` must be [valid] for exclusive writes and must not already own allocations.
/// `ac` must be a [valid], initialized, exclusively writable cursor with offset at most
/// argc and live arguments. `has_empty` must be [valid] for exclusive reads/writes;
/// `status` must be an initialized, exclusively writable [`OpaqueQueryError`]. All these
/// objects must be disjoint. The Redis API must remain initialized and unchanged.
/// On success the field-name bytes must remain alive and unchanged until the filter is freed.
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
    // SAFETY: the caller supplies exclusive writable storage, possibly uninitialized.
    unsafe {
        gf.write(LegacyGeoFilter {
            base: GeoFilter {
                fieldIndex: RS_INVALID_FIELD_INDEX,
                lat: 0.0,
                lon: 0.0,
                radius: 0.0,
                unitType: DistanceUnit::Kilometers as GeoDistance,
                numericFilters: ptr::null_mut(),
            },
            field: ptr::null_mut(),
        });
    }
    // SAFETY: each object is live, initialized, exclusive, and disjoint per the contract.
    let gf = unsafe { &mut *gf };
    // SAFETY: ac is initialized and exclusive.
    let ac = unsafe { &mut *ac };
    debug_assert!(ac.offset <= ac.argc);
    // SAFETY: status is initialized and exclusive.
    let status = unsafe { QueryError::from_opaque_mut_ptr(status) }.expect("status is null");
    if ac.argc - ac.offset < 5 {
        status.set_error(QueryErrorCode::ParseArgs, "GEOFILTER requires 5 arguments");
        return redis_module::REDISMODULE_ERR as c_int;
    }
    // SAFETY: at least five arguments remain, so the field exists and is a C string.
    let field = unsafe { ffi::AC_GetStringNC(ac, ptr::null_mut()) };
    for (target, name) in [
        (&mut gf.base.lon, "lon"),
        (&mut gf.base.lat, "lat"),
        (&mut gf.base.radius, "radius"),
    ] {
        // SAFETY: an argument remains at each iteration; target is exclusively writable.
        let rv = unsafe { ffi::AC_GetDouble(ac, target, ffi::AC_F_NOADVANCE as c_int) };
        if rv != 0 {
            let mut len = 0;
            let mut value = ptr::null();
            // SAFETY: the failed conversion did not consume this live argument.
            unsafe { ffi::AC_GetString(ac, &mut value, &mut len, ffi::AC_F_NOADVANCE as c_int) };
            if len != 0 {
                // With NOADVANCE alone and an existing argument, AC_GetDouble can only
                // fail with AC_ERR_PARSE; no range-limit flags are supplied.
                status.set_with_user_data(
                    QueryErrorCode::ParseArgs,
                    "Bad arguments",
                    &format!(" for <{name}>: Could not convert argument to expected type"),
                );
                return redis_module::REDISMODULE_ERR as c_int;
            }
            *target = 0.0;
            // SAFETY: has_empty is exclusively writable and disjoint from the filter/cursor.
            unsafe { *has_empty = true };
        }
        ac.offset += 1;
    }
    // SAFETY: the fifth argument remains, with a NUL-terminated backing buffer.
    let unit = unsafe { ffi::AC_GetStringNC(ac, ptr::null_mut()) };
    debug_assert!(!unit.is_null());
    // SAFETY: the argument returned above is readable through its NUL terminator.
    let unit = unsafe { CStr::from_ptr(unit) };
    gf.base.unitType = DistanceUnit::parse(unit) as GeoDistance;
    if gf.base.unitType == DistanceUnit::Invalid as GeoDistance {
        let message = c"Unknown distance unit";
        let detail = [
            QueryErrorCode::ParseArgs.prefix_c_str().to_bytes(),
            message.to_bytes(),
            b" ",
            unit.to_bytes(),
        ]
        .concat();
        // Preserve arbitrary user bytes in the private message, without lossy UTF-8 conversion.
        status.set_code_and_messages(
            QueryErrorCode::ParseArgs,
            Some(message.to_owned()),
            Some(CString::new(detail).expect("C strings have no interior NUL")),
        );
        return redis_module::REDISMODULE_ERR as c_int;
    }
    debug_assert!(!field.is_null());
    // SAFETY: field comes from the live cursor and is NUL-terminated.
    let field_len = unsafe { CStr::from_ptr(field) }.to_bytes().len();
    // SAFETY: the caller keeps the borrowed name alive; the Redis allocator is initialized.
    gf.field = unsafe { ffi::NewHiddenString(field, field_len, false) };
    redis_module::REDISMODULE_OK as c_int
}
