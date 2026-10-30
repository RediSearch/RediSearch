/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Parallel C ABI for geographic filters. Production switches in the final port stage.

use std::{
    ffi::{CStr, c_char, c_int},
    ptr,
};

use ffi::{GeoFilter, LegacyGeoFilter};
use geo::{DistanceUnit, RadiusFilter};
use query_error::{QueryError, QueryErrorCode, opaque::OpaqueQueryError};
use redis_module::raw::{RedisModule_Alloc, RedisModule_Free};
use rqe_core::RS_INVALID_FIELD_INDEX;

unsafe extern "C" {
    fn QueryError_SetWithUserDataFmt(
        status: *mut OpaqueQueryError,
        code: QueryErrorCode,
        message: *const c_char,
        format: *const c_char,
        ...
    );
}

/// Convert the ABI's integer unit into the safe domain.
pub const fn unit(value: c_int) -> Option<DistanceUnit> {
    match value {
        ffi::GeoDistance_GEO_DISTANCE_KM => Some(DistanceUnit::Kilometers),
        ffi::GeoDistance_GEO_DISTANCE_M => Some(DistanceUnit::Meters),
        ffi::GeoDistance_GEO_DISTANCE_FT => Some(DistanceUnit::Feet),
        ffi::GeoDistance_GEO_DISTANCE_MI => Some(DistanceUnit::Miles),
        _ => None,
    }
}

const fn unit_tag(unit: Option<DistanceUnit>) -> c_int {
    match unit {
        Some(DistanceUnit::Kilometers) => ffi::GeoDistance_GEO_DISTANCE_KM,
        Some(DistanceUnit::Meters) => ffi::GeoDistance_GEO_DISTANCE_M,
        Some(DistanceUnit::Feet) => ffi::GeoDistance_GEO_DISTANCE_FT,
        Some(DistanceUnit::Miles) => ffi::GeoDistance_GEO_DISTANCE_MI,
        None => ffi::GeoDistance_GEO_DISTANCE_INVALID,
    }
}

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

/// Parse a NUL-terminated unit name.
///
/// # Safety
/// `s` must be a [valid], non-null C string.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_GeoDistance_Parse(s: *const c_char) -> c_int {
    debug_assert!(!s.is_null());
    // SAFETY: the input string remains readable through its terminating NUL.
    let bytes = unsafe { CStr::from_ptr(s) }.to_bytes();
    unit_tag(DistanceUnit::parse(bytes))
}

/// Parse a unit token without reading beyond its length.
///
/// # Safety
/// `s` must be non-null and [valid] for `len` readable bytes.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_GeoDistance_Parse_Buffer(s: *const c_char, len: usize) -> c_int {
    debug_assert!(!s.is_null());
    // SAFETY: the caller supplies a readable token of exactly this length.
    let bytes = unsafe { std::slice::from_raw_parts(s.cast(), len) };
    unit_tag(DistanceUnit::parse_buffer(bytes))
}

/// Return a static lowercase unit name, or `<badunit>` for unknown integers.
#[unsafe(no_mangle)]
pub extern "C" fn Rust_GeoDistance_ToString(value: c_int) -> *const c_char {
    unit(value)
        .map_or(c"<badunit>", DistanceUnit::as_c_str)
        .as_ptr()
}

/// Allocate an unvalidated filter. A null unit selects kilometers.
///
/// # Safety
/// A non-null `name` must be [valid] for `len` readable bytes. Redis allocation
/// APIs must be initialized and remain unchanged throughout this call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_NewGeoFilter(
    lon: f64,
    lat: f64,
    radius: f64,
    name: *const c_char,
    len: usize,
) -> *mut GeoFilter {
    let mut filter = empty_filter();
    filter.lon = lon;
    filter.lat = lat;
    filter.radius = radius;
    if !name.is_null() {
        // SAFETY: non-null name is a readable token for the supplied length.
        filter.unitType = unsafe { Rust_GeoDistance_Parse_Buffer(name, len) };
    }
    // SAFETY: the initialized Redis allocator provides properly aligned storage.
    let allocate = unsafe { RedisModule_Alloc.unwrap() };
    // SAFETY: the requested layout is the exact GeoFilter allocation size.
    let output = unsafe { allocate(size_of::<GeoFilter>()) }.cast::<GeoFilter>();
    // SAFETY: Redis allocation is infallible and the fresh allocation is exclusively owned.
    unsafe { output.write(filter) };
    output
}

/// Bind a filter to a stable field index; null clears the binding.
///
/// # Safety
/// `filter` must be a [valid] exclusive filter pointer. A non-null `field` must
/// be [valid] for reads and must not overlap `filter`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_GeoFilter_SetField(
    filter: *mut GeoFilter,
    field: *const ffi::FieldSpec,
) {
    debug_assert!(!filter.is_null());
    // SAFETY: a present field is readable and disjoint from the filter.
    let index = unsafe { field.as_ref() }.map_or(RS_INVALID_FIELD_INDEX, |f| f.index);
    // SAFETY: filter is exclusively writable for this call.
    unsafe { (*filter).fieldIndex = index };
}

/// Validate a filter and set its syntax error, returning one on success.
///
/// # Safety
/// `filter` must be [valid] for reads; `status` must be a [valid], exclusive
/// initialized query error, disjoint from `filter`. Neither may be null.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_GeoFilter_Validate(
    filter: *const GeoFilter,
    status: *mut OpaqueQueryError,
) -> c_int {
    debug_assert!(!filter.is_null() && !status.is_null());
    // SAFETY: the filter is initialized and readable for this call.
    let gf = unsafe { &*filter };
    match RadiusFilter::new(gf.lon, gf.lat, gf.radius, unit(gf.unitType)) {
        Ok(_) => 1,
        Err(error) => {
            // SAFETY: status is initialized, exclusive, and does not overlap the filter.
            let status = unsafe { QueryError::from_opaque_mut_ptr(status) }.unwrap();
            status.set_error(QueryErrorCode::Syntax, &error.to_string());
            0
        }
    }
}

/// Compare an indexed score to a validated filter, optionally returning meters.
///
/// # Safety
/// `filter` must be [valid] for reads and pass [`Rust_GeoFilter_Validate`].
/// `distance` must be null or [valid] for an exclusive write, disjoint from `filter`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_isWithinRadius(
    filter: *const GeoFilter,
    score: f64,
    distance: *mut f64,
) -> c_int {
    debug_assert!(!filter.is_null());
    // SAFETY: the filter is readable and was validated before matching begins.
    let gf = unsafe { &*filter };
    let filter = RadiusFilter::new(gf.lon, gf.lat, gf.radius, unit(gf.unitType)).unwrap();
    let meters = filter.distance(score);
    // SAFETY: any present output is exclusively writable and disjoint from filter.
    if let Some(output) = unsafe { distance.as_mut() } {
        *output = meters;
    }
    c_int::from(meters <= filter.radius_meters())
}

/// Release a filter and its numeric ranges.
///
/// # Safety
/// `filter` must be a [valid], exclusively owned Redis allocation of a filter,
/// with null numeric ranges or the live array from `build_geo_numeric_filters`.
/// Redis allocation APIs must be initialized and unchanged during this call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_GeoFilter_Free(filter: *mut GeoFilter) {
    debug_assert!(!filter.is_null());
    // SAFETY: the owned filter remains alive until its ranges have been freed.
    let ranges = unsafe { (*filter).numericFilters };
    // SAFETY: ranges are null or the exclusively owned array created by the range builder.
    unsafe { rqe_iterators::free_geo_numeric_filters(ranges.cast()) };
    // SAFETY: Redis APIs remain initialized and stable during filter destruction.
    let free = unsafe { RedisModule_Free.unwrap() };
    // SAFETY: this is the original Redis allocation, freed exactly once.
    unsafe { free(filter.cast()) };
}

/// Release a legacy filter without freeing the argument buffer it borrows.
///
/// # Safety
/// `filter` must be a [valid], exclusively owned Redis allocation initialized by
/// [`Rust_GeoFilter_LegacyParse`], including failed parses. The allocation APIs
/// and numeric-range requirements of [`Rust_GeoFilter_Free`] apply.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_LegacyGeoFilter_Free(filter: *mut LegacyGeoFilter) {
    debug_assert!(!filter.is_null());
    // SAFETY: the legacy filter is initialized and exclusively owned.
    let field = unsafe { (*filter).field };
    if !field.is_null() {
        // SAFETY: legacy parsing creates a borrowed HiddenString wrapper, owned by this filter.
        unsafe { ffi::HiddenString_Free(field, false) };
    }
    // SAFETY: base is the first field of the original allocation and owns its ranges.
    unsafe { Rust_GeoFilter_Free(filter.cast()) };
}

/// Parse five legacy arguments, retaining cursor progress on failure.
///
/// # Safety
/// All pointers must be non-null, [valid], exclusive and mutually disjoint.
/// `output` must be uninitialized storage without owned resources; `cursor` must
/// be an initialized ArgsCursor with offset ≤ argc and NUL-terminated string
/// arguments. Arguments must outlive the returned filter. `status` and `empty`
/// must be initialized. Redis allocation APIs must remain initialized and stable.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_GeoFilter_LegacyParse(
    output: *mut LegacyGeoFilter,
    cursor: *mut ffi::ArgsCursor,
    empty: *mut bool,
    status: *mut OpaqueQueryError,
) -> c_int {
    debug_assert!(!output.is_null() && !cursor.is_null() && !empty.is_null() && !status.is_null());
    // SAFETY: output is exclusive storage without resources to release.
    unsafe {
        output.write(LegacyGeoFilter {
            base: empty_filter(),
            field: ptr::null_mut(),
        })
    };
    // SAFETY: cursor is exclusively borrowed and initialized.
    let ac = unsafe { &mut *cursor };
    debug_assert!(ac.offset <= ac.argc);
    if ac.argc - ac.offset < 5 {
        // SAFETY: status is initialized and exclusively writable.
        unsafe { QueryError::from_opaque_mut_ptr(status) }
            .unwrap()
            .set_error(QueryErrorCode::ParseArgs, "GEOFILTER requires 5 arguments");
        return 1;
    }
    // SAFETY: five arguments remain; the first is a NUL-terminated field name.
    let field = unsafe { ffi::AC_GetStringNC(ac, ptr::null_mut()) };
    let mut values = [0.0; 3];
    for (value, name) in values.iter_mut().zip(["lon", "lat", "radius"]) {
        // SAFETY: there is a current argument and value is an exclusive local.
        let rv = unsafe { ffi::AC_GetDouble(ac, value, ffi::AC_F_NOADVANCE as c_int) };
        if rv != 0 {
            let mut len = 0;
            let mut text = ptr::null();
            // SAFETY: current argument exists and the local outputs are exclusively writable.
            unsafe { ffi::AC_GetString(ac, &mut text, &mut len, ffi::AC_F_NOADVANCE as c_int) };
            if len != 0 {
                // SAFETY: status is initialized, exclusive and disjoint from the cursor.
                unsafe { QueryError::from_opaque_mut_ptr(status) }
                    .unwrap()
                    .set_with_user_data(
                        QueryErrorCode::ParseArgs,
                        "Bad arguments",
                        &format!(" for <{name}>: Could not convert argument to expected type"),
                    );
                return 1;
            }
            // SAFETY: empty is an exclusive, initialized output flag.
            unsafe { *empty = true };
        }
        ac.offset += 1;
    }
    // SAFETY: the last required argument is a NUL-terminated unit name.
    let name = unsafe { ffi::AC_GetStringNC(ac, ptr::null_mut()) };
    // SAFETY: ArgsCursor returned the current terminated unit string.
    let unit = unsafe { Rust_GeoDistance_Parse(name) };
    if unit == ffi::GeoDistance_GEO_DISTANCE_INVALID {
        // SAFETY: status is exclusive and name points to a live terminated argument.
        unsafe {
            QueryError_SetWithUserDataFmt(
                status,
                QueryErrorCode::ParseArgs,
                c"Unknown distance unit".as_ptr(),
                c" %s".as_ptr(),
                name,
            )
        };
        return 1;
    }
    // SAFETY: field came from the cursor and is a live terminated argument.
    let field_len = unsafe { CStr::from_ptr(field) }.count_bytes();
    // SAFETY: the field buffer outlives the filter and remains borrowed, not owned.
    let field = unsafe { ffi::NewHiddenString(field, field_len, false) };
    // SAFETY: output remains exclusive and its initial empty state owns no resources.
    unsafe {
        *output = LegacyGeoFilter {
            base: GeoFilter {
                lon: values[0],
                lat: values[1],
                radius: values[2],
                unitType: unit,
                ..empty_filter()
            },
            field,
        }
    };
    0
}
