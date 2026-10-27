/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C-owned filter layout and argument-cursor adapters for [`geo`].

use std::{
    cell::Cell,
    ffi::{CStr, CString, c_char, c_int},
    ptr,
};

use geo::{GeoDistance, GeoFilter, LegacyGeoArgs, LegacyGeoParseError};
use query_error::{QueryError, QueryErrorCode, opaque::OpaqueQueryError};
use redis_module::raw::{RedisModule_Alloc, RedisModule_Free};

unsafe extern "C" {
    fn GeoFilter_FreeNumericFilters(filters: *mut *mut ffi::NumericFilter);
}

const fn unit_from_c(unit: ffi::GeoDistance) -> GeoDistance {
    match unit {
        0 => GeoDistance::Kilometers,
        1 => GeoDistance::Meters,
        2 => GeoDistance::Feet,
        3 => GeoDistance::Miles,
        _ => GeoDistance::Invalid,
    }
}

const fn scalars(filter: &ffi::GeoFilter) -> GeoFilter {
    GeoFilter {
        lon: filter.lon,
        lat: filter.lat,
        radius: filter.radius,
        unit: unit_from_c(filter.unitType),
    }
}

const fn c_filter(filter: GeoFilter) -> ffi::GeoFilter {
    ffi::GeoFilter {
        fieldIndex: u16::MAX,
        lat: filter.lat,
        lon: filter.lon,
        radius: filter.radius,
        unitType: filter.unit as ffi::GeoDistance,
        numericFilters: ptr::null_mut(),
    }
}

/// Parse a NUL-terminated unit with [`GeoDistance::parse`].
///
/// # Safety
/// `unit` must be a [valid], non-null NUL-terminated string.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoDistance_Parse(unit: *const c_char) -> ffi::GeoDistance {
    // SAFETY: the caller supplies a NUL-terminated string.
    GeoDistance::parse(unsafe { CStr::from_ptr(unit) }.to_bytes()) as ffi::GeoDistance
}

/// Parse a bounded unit with [`GeoDistance::parse_buffer`].
///
/// # Safety
/// For lengths below the scratch-buffer limit, `unit` must be [valid] for `len` bytes.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoDistance_Parse_Buffer(
    unit: *const c_char,
    len: usize,
) -> ffi::GeoDistance {
    if len >= 16 {
        return GeoDistance::Invalid as ffi::GeoDistance;
    }
    let bytes = if len == 0 {
        &[]
    } else {
        // SAFETY: the caller guarantees the bounded input is readable.
        unsafe { std::slice::from_raw_parts(unit.cast(), len) }
    };
    GeoDistance::parse_buffer(bytes) as ffi::GeoDistance
}

/// Return a static NUL-terminated spelling for a C distance unit.
#[unsafe(no_mangle)]
pub const extern "C" fn GeoDistance_ToString(unit: ffi::GeoDistance) -> *const c_char {
    match unit_from_c(unit) {
        GeoDistance::Kilometers => c"km",
        GeoDistance::Meters => c"m",
        GeoDistance::Feet => c"ft",
        GeoDistance::Miles => c"mi",
        GeoDistance::Invalid => c"<badunit>",
    }
    .as_ptr()
}

/// Allocate a C-compatible filter shell using the Redis allocator.
///
/// Release it with [`GeoFilter_Free`]. A null `unit` selects the default unit.
///
/// # Safety
/// A non-null `unit` must satisfy [`GeoDistance_Parse_Buffer`]'s contract.
/// Redis allocator callbacks must be initialized.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewGeoFilter(
    lon: f64,
    lat: f64,
    radius: f64,
    unit: *const c_char,
    len: usize,
) -> *mut ffi::GeoFilter {
    let mut filter = GeoFilter::new(lon, lat, radius, None);
    if !unit.is_null() {
        // SAFETY: the caller supplies the bounded unit buffer.
        filter.unit = unit_from_c(unsafe { GeoDistance_Parse_Buffer(unit, len) });
    }
    // SAFETY: the caller initializes Redis allocation before creating filters.
    let alloc = unsafe { RedisModule_Alloc }.expect("Redis allocator not initialized");
    // SAFETY: size and alignment match the C shell.
    let result = unsafe { alloc(size_of::<ffi::GeoFilter>()) }.cast::<ffi::GeoFilter>();
    // SAFETY: Redis allocation returns aligned storage of the requested size or aborts.
    unsafe { result.write(c_filter(filter)) };
    result
}

/// Bind a filter to a stable field index, or clear its binding for a null field.
///
/// # Safety
/// `filter` must be [valid], non-null and writable; a non-null `field` must be [valid] to read.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoFilter_SetField(
    filter: *mut ffi::GeoFilter,
    field: *const ffi::FieldSpec,
) {
    // SAFETY: a non-null field is readable by the caller's contract.
    let index = unsafe { field.as_ref() }.map_or(u16::MAX, |field| field.index);
    // SAFETY: the filter is writable by the caller's contract.
    unsafe { (*filter).fieldIndex = index };
}

/// Validate scalar parameters without changing the filter or replacing an earlier error.
///
/// # Safety
/// `filter` must be [valid] and non-null. `status` must point to an initialized,
/// exclusively borrowed [`OpaqueQueryError`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoFilter_Validate(
    filter: *const ffi::GeoFilter,
    status: *mut OpaqueQueryError,
) -> c_int {
    // SAFETY: the caller supplies a readable filter.
    let filter = unsafe { &*filter };
    let mut core = scalars(filter);
    // C rejects only the explicit invalid sentinel during validation.
    if filter.unitType != -1 && core.unit == GeoDistance::Invalid {
        core.unit = GeoDistance::Kilometers;
    }
    match core.validate() {
        Ok(()) => 1,
        Err(error) => {
            // SAFETY: the caller provides an initialized, exclusively borrowed error.
            unsafe { QueryError::from_opaque_mut_ptr(status) }
                .expect("status is null")
                .set_error(QueryErrorCode::Syntax, &error.to_string());
            0
        }
    }
}

/// Free a Redis-allocated filter shell and its Rust-owned numeric filters.
///
/// # Safety
/// `filter` must be a [valid], exclusively owned Redis allocation with an initialized
/// [`ffi::GeoFilter`] prefix. Its numeric filters must obey the existing iterator allocator contract.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoFilter_Free(filter: *mut ffi::GeoFilter) {
    // SAFETY: the caller transfers ownership of the shell and its numeric filters.
    let filters = unsafe { (*filter).numericFilters };
    if !filters.is_null() {
        // SAFETY: numeric filters retain their original matching Rust deallocator.
        unsafe { GeoFilter_FreeNumericFilters(filters) };
    }
    // SAFETY: allocator callbacks are initialized before allocating filters.
    let free = unsafe { RedisModule_Free }.expect("Redis allocator not initialized");
    // SAFETY: this shell was allocated using the Redis allocator.
    unsafe { free(filter.cast()) };
}

/// Free the borrowed field wrapper and the legacy filter shell.
///
/// # Safety
/// `filter` must be a [valid], exclusively owned legacy filter allocated with the Redis
/// allocator. Its field wrapper must borrow its string as created by [`GeoFilter_LegacyParse`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn LegacyGeoFilter_Free(filter: *mut ffi::LegacyGeoFilter) {
    // SAFETY: the caller supplies an initialized legacy filter.
    let field = unsafe { (*filter).field };
    if !field.is_null() {
        // SAFETY: only the wrapper is owned; the argv string remains borrowed.
        unsafe { ffi::HiddenString_Free(field, false) };
    }
    // SAFETY: the base is the first member and retains the same allocation address.
    unsafe { GeoFilter_Free(filter.cast()) };
}

/// Check a geohash score with [`GeoFilter::is_within_radius`], preserving the C integer return ABI.
///
/// # Panics
/// Has the same non-finite-center and invalid-unit panic conditions as the core method.
///
/// # Safety
/// `filter` must be [valid] and non-null. A non-null `distance` must be [valid] to write.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn isWithinRadius(
    filter: *const ffi::GeoFilter,
    score: f64,
    distance: *mut f64,
) -> c_int {
    // SAFETY: the caller provides a readable filter.
    let (inside, actual) = scalars(unsafe { &*filter }).is_within_radius(score);
    // SAFETY: a non-null distance pointer is writable by the caller's contract.
    if let Some(distance) = unsafe { distance.as_mut() } {
        *distance = actual;
    }
    c_int::from(inside)
}

/// Borrow the actual cursor, retaining C numeric conversions and partial scalar writes.
struct CursorArgs<'a> {
    cursor: &'a mut ffi::ArgsCursor,
    start: usize,
    values: [Cell<f64>; 3],
}

impl<'a> LegacyGeoArgs<'a> for CursorArgs<'a> {
    fn remaining(&self) -> usize {
        self.cursor.argc - self.cursor.offset
    }

    fn current(&self) -> &'a [u8] {
        let mut cursor = *self.cursor;
        let mut text = ptr::null();
        let mut len = 0;
        // SAFETY: parsing checks the remaining count and argv storage outlives the call.
        let result = unsafe {
            ffi::AC_GetString(
                &mut cursor,
                &mut text,
                &mut len,
                ffi::AC_F_NOADVANCE as c_int,
            )
        };
        assert_eq!(result, ffi::ACStatus_AC_OK as c_int);
        // SAFETY: the cursor's string storage remains stable across advancement.
        unsafe { std::slice::from_raw_parts(text.cast(), len) }
    }

    fn current_double(&self) -> Option<f64> {
        let mut cursor = *self.cursor;
        let mut value = 0.0;
        // SAFETY: the core only requests numeric conversion of an existing argument.
        let result =
            unsafe { ffi::AC_GetDouble(&mut cursor, &mut value, ffi::AC_F_NOADVANCE as c_int) };
        if result != ffi::ACStatus_AC_OK as c_int {
            return None;
        }
        self.values[self.cursor.offset - self.start - 1].set(value);
        Some(value)
    }

    fn advance(&mut self) {
        self.cursor.offset += 1;
    }
}

/// Parse the legacy filter while preserving cursor progress, borrowed field storage, and errors.
///
/// # Safety
/// All pointers must be non-null and [valid]; outputs and cursor must be exclusively writable.
/// `filter` may point to uninitialized storage; it is initialized before being borrowed.
/// `has_empty` must point to an initialized boolean, even when parsing will not write it:
/// it is borrowed as a mutable reference, and an existing true value is never cleared.
/// `cursor` and `status` must be initialized. The cursor's argv strings must remain stable and NUL-terminated
/// throughout parsing; the field string must also outlive the returned hidden-string wrapper.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GeoFilter_LegacyParse(
    filter: *mut ffi::LegacyGeoFilter,
    cursor: *mut ffi::ArgsCursor,
    has_empty: *mut bool,
    status: *mut OpaqueQueryError,
) -> c_int {
    // SAFETY: the output may be uninitialized, so initialize before forming a reference.
    unsafe {
        filter.write(ffi::LegacyGeoFilter {
            base: c_filter(GeoFilter::new(0.0, 0.0, 0.0, None)),
            field: ptr::null_mut(),
        });
    }
    // SAFETY: the output was initialized above and is exclusively writable.
    let filter = unsafe { &mut *filter };
    // SAFETY: the caller provides an initialized, exclusively writable cursor.
    let cursor = unsafe { &mut *cursor };
    // SAFETY: the caller guarantees an initialized boolean and exclusive access for the
    // borrow, even on paths where parsing leaves the sticky flag untouched.
    let has_empty = unsafe { &mut *has_empty };
    let start = cursor.offset;
    let mut args = CursorArgs {
        cursor,
        start,
        values: [const { Cell::new(0.0) }; 3],
    };
    let field = (args.remaining() >= 5).then(|| args.current().as_ptr());
    let parsed = geo::LegacyGeoFilter::parse(&mut args, has_empty);
    [filter.base.lon, filter.base.lat, filter.base.radius] = args.values.map(Cell::into_inner);
    match parsed {
        Ok(parsed) => {
            filter.base = c_filter(parsed.base);
            // SAFETY: use the original argv pointer, not the core's temporary owned field copy.
            filter.field = unsafe {
                ffi::NewHiddenString(
                    field.expect("field was parsed").cast(),
                    parsed.field.len(),
                    false,
                )
            };
            redis_module::REDISMODULE_OK as c_int
        }
        Err(error) => {
            // SAFETY: the caller supplies an initialized, exclusively borrowed query error.
            let status =
                unsafe { QueryError::from_opaque_mut_ptr(status) }.expect("status is null");
            match error {
                LegacyGeoParseError::UnknownUnit(unit) => {
                    filter.base.unitType = GeoDistance::Invalid as ffi::GeoDistance;
                    let code = QueryErrorCode::ParseArgs;
                    let public = c"Unknown distance unit".to_owned();
                    let mut private = code.prefix_c_str().to_bytes().to_vec();
                    private.extend_from_slice(public.as_bytes());
                    private.push(b' ');
                    private.extend_from_slice(&unit);
                    // The core truncates at NUL, so arbitrary user bytes remain representable.
                    status.set_code_and_messages(
                        code,
                        Some(public),
                        Some(CString::new(private).expect("unit contains no NUL")),
                    );
                }
                LegacyGeoParseError::MissingArguments => {
                    status.set_error(QueryErrorCode::ParseArgs, &error.to_string())
                }
                _ => {
                    let message = error.to_string();
                    status.set_with_user_data(
                        QueryErrorCode::ParseArgs,
                        "Bad arguments",
                        &message["Bad arguments".len()..],
                    );
                }
            }
            redis_module::REDISMODULE_ERR as c_int
        }
    }
}
