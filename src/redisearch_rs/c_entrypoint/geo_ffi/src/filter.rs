/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use geo::{GEO_DISTANCE_INVALID, GEO_DISTANCE_KM, GeoDistance, GeoFilter, LegacyGeoFilter, Unit};
use query_error::{QueryError, QueryErrorCode, opaque::OpaqueQueryError};
use std::{
    ffi::{CStr, CString, c_char, c_int, c_void},
    ptr,
};

unsafe extern "C" {
    #[link_name = "RedisModule_Alloc"]
    static ALLOC: unsafe extern "C" fn(usize) -> *mut c_void;
    #[link_name = "RedisModule_Free"]
    static FREE: unsafe extern "C" fn(*mut c_void);
}

/// Parses a NUL-terminated unit.
/// # Safety
/// `s` must be a [valid] C string.
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
pub unsafe fn GeoDistance_Parse(s: *const c_char) -> GeoDistance {
    debug_assert!(!s.is_null());
    // SAFETY: the input is a terminated string by the boundary contract.
    Unit::parse(unsafe { CStr::from_ptr(s) }.to_bytes()).map_or(GEO_DISTANCE_INVALID, |u| u as i32)
}

/// Parses a length-delimited unit.
/// # Safety
/// For lengths below sixteen, `s` must be non-null and [valid] for `len` readable bytes.
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
pub unsafe fn GeoDistance_Parse_Buffer(s: *const c_char, len: usize) -> GeoDistance {
    if len >= 16 {
        return GEO_DISTANCE_INVALID;
    }
    debug_assert!(!s.is_null());
    // SAFETY: the length check and boundary contract guarantee this slice is readable.
    Unit::parse(unsafe { std::slice::from_raw_parts(s.cast(), len) })
        .map_or(GEO_DISTANCE_INVALID, |u| u as i32)
}

/// Returns a static spelling, or `<badunit>` for an invalid unit code.
pub fn GeoDistance_ToString(unit: GeoDistance) -> *const c_char {
    Unit::from_raw(unit)
        .map_or(c"<badunit>", Unit::as_c_str)
        .as_ptr()
}

/// Allocates an unbound filter; a null unit selects kilometers.
/// # Safety
/// A non-null `unit` must satisfy [`GeoDistance_Parse_Buffer`]. The C Redis allocator
/// must be initialized and remain unchanged throughout the call.
pub unsafe fn NewGeoFilter(
    lon: f64,
    lat: f64,
    radius: f64,
    unit: *const c_char,
    len: usize,
) -> *mut GeoFilter {
    let unit = if unit.is_null() {
        GEO_DISTANCE_KM
    } else {
        // SAFETY: the non-null buffer's length and readability are supplied at the boundary.
        unsafe { GeoDistance_Parse_Buffer(unit, len) }
    };
    // SAFETY: the initialized C allocator supplies aligned storage of the requested size.
    let gf = unsafe { ALLOC(size_of::<GeoFilter>()) }.cast::<GeoFilter>();
    // SAFETY: Redis allocation either succeeds or aborts; this allocation is exclusively ours.
    unsafe { gf.write(GeoFilter::new(lon, lat, radius, unit)) };
    gf
}

/// Binds the stable field index, or clears it when `fs` is null.
/// # Safety
/// `gf` must be exclusively writable and [valid]; non-null `fs` must be readable and
/// [valid]. They must not overlap.
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
pub unsafe fn GeoFilter_SetField(gf: *mut GeoFilter, fs: *const ffi::FieldSpec) {
    debug_assert!(!gf.is_null());
    // SAFETY: the optional field is readable and disjoint from the exclusively writable filter.
    let index = unsafe { fs.as_ref() }.map_or(rqe_core::RS_INVALID_FIELD_INDEX, |fs| fs.index);
    // SAFETY: the filter is exclusively writable under the boundary contract.
    unsafe { (*gf).fieldIndex = index };
}

/// Validates a filter, setting a syntax error on failure.
/// # Safety
/// `gf` must be readable and [valid]; `status` must be an exclusively writable,
/// initialized, [valid] query error, disjoint from `gf`.
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
pub unsafe fn GeoFilter_Validate(gf: *const GeoFilter, status: *mut OpaqueQueryError) -> c_int {
    debug_assert!(!gf.is_null());
    // SAFETY: the boundary provides a live, readable filter.
    match unsafe { &*gf }.radius_filter() {
        Ok(_) => 1,
        Err(error) => {
            debug_assert!(!status.is_null());
            // SAFETY: status is the caller's exclusive initialized query error.
            unsafe { QueryError::from_opaque_mut_ptr(status) }
                .unwrap()
                .set_error(QueryErrorCode::Syntax, error);
            0
        }
    }
}

/// Checks an indexed score and optionally writes its distance in meters.
/// # Safety
/// `gf` must be readable, [valid], and pass [`GeoFilter_Validate`]. A non-null
/// `distance` must be exclusively writable, [valid], and disjoint from `gf`.
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
pub unsafe fn isWithinRadius(gf: *const GeoFilter, score: f64, distance: *mut f64) -> c_int {
    debug_assert!(!gf.is_null());
    // SAFETY: the caller supplies a live validated filter.
    let filter = unsafe { &*gf }
        .radius_filter()
        .expect("validated geo filter");
    let meters = filter.distance(score);
    if !distance.is_null() {
        // SAFETY: non-null distance is exclusive writable storage, disjoint from the filter.
        unsafe { distance.write(meters) };
    }
    filter.contains(meters).into()
}

/// Releases the filter and its owned numeric ranges.
/// # Safety
/// `gf` must be a [valid] exclusively owned C Redis allocation, containing initialized
/// fields. Its numeric ranges must satisfy [`rqe_iterators::free_geo_numeric_filters`].
/// The C Redis allocator must be initialized and stable during this call.
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
pub unsafe fn GeoFilter_Free(gf: *mut GeoFilter) {
    debug_assert!(!gf.is_null());
    // SAFETY: the exclusively owned filter transfers its ranges to their original allocator.
    let ranges = unsafe { (*gf).numericFilters };
    // SAFETY: these are the owned ranges attached by the iterator subsystem.
    unsafe { rqe_iterators::free_geo_numeric_filters(ranges.cast()) };
    // SAFETY: gf is the base address of a C Redis allocation, even for a legacy filter.
    unsafe { FREE(gf.cast()) };
}

/// Releases a legacy filter, including its borrowed-name wrapper.
/// # Safety
/// `gf` must satisfy [`GeoFilter_Free`] and point to a [valid] initialized
/// [`LegacyGeoFilter`]; any non-null field must be an exclusively owned HiddenString
/// wrapper created with `takeOwnership=false`.
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
pub unsafe fn LegacyGeoFilter_Free(gf: *mut LegacyGeoFilter) {
    debug_assert!(!gf.is_null());
    // SAFETY: the initialized legacy filter owns this optional wrapper.
    let field = unsafe { (*gf).field };
    if !field.is_null() {
        // SAFETY: only the owned wrapper is freed, never the borrowed command argument.
        unsafe { ffi::HiddenString_Free(field.cast(), false) };
    }
    // SAFETY: repr(C) places base at the allocation's start, with its ownership intact.
    unsafe { GeoFilter_Free(gf.cast()) };
}

/// Parses five legacy GEOFILTER tokens; errors leave a freeable filter.
/// # Safety
/// All pointers must be non-null, [valid] and disjoint. `gf` is exclusively writable
/// uninitialized storage; `ac` is an exclusive initialized cursor with offset <= argc
/// and live NUL-terminated argument buffers; `empty` is an initialized exclusive flag;
/// `status` is an initialized exclusive query error. Argument bytes must outlive the filter.
/// The C Redis allocator and the cursor's Redis APIs must be initialized and stable.
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
pub unsafe fn GeoFilter_LegacyParse(
    gf: *mut LegacyGeoFilter,
    ac: *mut ffi::ArgsCursor,
    empty: *mut bool,
    status: *mut OpaqueQueryError,
) -> c_int {
    debug_assert!(!gf.is_null() && !ac.is_null() && !empty.is_null() && !status.is_null());
    // SAFETY: gf is exclusive output storage; no previous value is read or dropped.
    unsafe {
        gf.write(LegacyGeoFilter {
            base: GeoFilter::new(0.0, 0.0, 0.0, GEO_DISTANCE_KM),
            field: ptr::null_mut(),
        })
    };
    // SAFETY: the exclusive output was initialized above.
    let gf = unsafe { &mut *gf };
    // SAFETY: the cursor and error are disjoint exclusive initialized objects.
    let cursor = unsafe { &mut *ac };
    // SAFETY: status is the exclusive initialized query error passed at the boundary.
    let error = unsafe { QueryError::from_opaque_mut_ptr(status) }.unwrap();
    debug_assert!(cursor.offset <= cursor.argc);
    if cursor.argc - cursor.offset < 5 {
        error.set_error(QueryErrorCode::ParseArgs, "GEOFILTER requires 5 arguments");
        return 1;
    }
    // SAFETY: the five-token check guarantees a live, NUL-terminated field name.
    let field = unsafe { ffi::AC_GetStringNC(cursor, ptr::null_mut()) };
    for (value, name) in [
        (&mut gf.base.lon, "lon"),
        (&mut gf.base.lat, "lat"),
        (&mut gf.base.radius, "radius"),
    ] {
        // SAFETY: the numeric token remains available, and value is exclusive output storage.
        if unsafe { ffi::AC_GetDouble(cursor, value, ffi::AC_F_NOADVANCE as c_int) } != 0 {
            let mut len = 0;
            let mut token = ptr::null();
            // SAFETY: the failing token remains at the cursor; both outputs are writable.
            unsafe {
                ffi::AC_GetString(cursor, &mut token, &mut len, ffi::AC_F_NOADVANCE as c_int)
            };
            if len != 0 {
                error.set_with_user_data(
                    QueryErrorCode::ParseArgs,
                    "Bad arguments",
                    &format!(" for <{name}>: Could not convert argument to expected type"),
                );
                return 1;
            }
            // SAFETY: empty is the caller's disjoint initialized output flag.
            unsafe { *empty = true };
        }
        cursor.offset += 1;
    }
    // SAFETY: the final checked token is a live NUL-terminated unit string.
    let unit = unsafe { ffi::AC_GetStringNC(cursor, ptr::null_mut()) };
    // SAFETY: AC_GetStringNC returned the checked unit argument.
    let unit = unsafe { CStr::from_ptr(unit) };
    let Some(unit_type) = Unit::parse(unit.to_bytes()) else {
        let mut private = QueryErrorCode::ParseArgs.prefix_c_str().to_bytes().to_vec();
        private.extend_from_slice(b"Unknown distance unit ");
        private.extend_from_slice(unit.to_bytes());
        error.set_code_and_messages(
            QueryErrorCode::ParseArgs,
            Some(c"Unknown distance unit".into()),
            Some(CString::new(private).unwrap()),
        );
        return 1;
    };
    // SAFETY: field points to the checked NUL-terminated argument, retained by the command.
    let field_len = unsafe { CStr::from_ptr(field) }.to_bytes().len();
    gf.base.unitType = unit_type as i32;
    // SAFETY: initialized stable C allocator; the wrapper borrows bytes that outlive the filter.
    gf.field = unsafe { ffi::NewHiddenString(field, field_len, false) }.cast();
    0
}
