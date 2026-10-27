/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::cell::Cell;
use std::ffi::{CString, c_int};

use geo::{LegacyGeoArgs, LegacyGeoParseError};
use query_error::{QueryError, QueryErrorCode, opaque::OpaqueQueryError};

use crate::{GEO_DISTANCE_INVALID, GeoFilter, Rust_GeoFilter_Free};

/// Legacy syntax borrows a field name until schema resolution.
#[repr(C)]
#[cheadergen::config(export)]
pub struct LegacyGeoFilter {
    pub base: GeoFilter,
    pub field: *mut ffi::HiddenString,
}

struct CursorArgs<'a> {
    cursor: &'a ffi::ArgsCursor,
    start: usize,
    // Preserve the C API's partially initialized output on parse failure.
    numbers: Cell<[f64; 3]>,
}

impl CursorArgs<'_> {
    fn at(&self, offset: usize) -> ffi::ArgsCursor {
        assert!(offset < self.cursor.argc);
        ffi::ArgsCursor {
            objs: self.cursor.objs,
            type_: self.cursor.type_,
            argc: self.cursor.argc,
            offset,
        }
    }
}

impl LegacyGeoArgs for CursorArgs<'_> {
    fn len(&self) -> usize {
        self.cursor.argc
    }

    fn string(&self, index: usize) -> Result<&[u8], &'static str> {
        let mut cursor = self.at(index);
        let mut ptr = std::ptr::null();
        let mut len = 0;
        // SAFETY: the borrowed cursor owns live arguments; at checked the index.
        // AC_GetString only changes this local cursor, not its arguments.
        let rv = unsafe { ffi::AC_GetString(&mut cursor, &mut ptr, &mut len, 0) };
        assert_eq!(rv, 0);
        // SAFETY: AC_GetString returns bytes borrowed from the live cursor arguments.
        Ok(unsafe { std::slice::from_raw_parts(ptr.cast(), len) })
    }

    fn number(&self, index: usize) -> Result<f64, &'static str> {
        let mut cursor = self.at(index);
        let mut number = 0.0;
        // SAFETY: at checked the index. The C converter retains the distinct
        // Redis-string and strtod grammars; only the local cursor is mutated.
        let rv = unsafe { ffi::AC_GetDouble(&mut cursor, &mut number, 0) };
        if rv != 0 {
            // With no range flags and an existing argument this is AC_ERR_PARSE.
            return Err("Could not convert argument to expected type");
        }
        let mut numbers = self.numbers.get();
        numbers[index - self.start - 1] = number;
        self.numbers.set(numbers);
        Ok(number)
    }
}

/// Parse legacy arguments, retaining cursor progress and partial output on errors.
///
/// # Safety
/// `gf`, `ac`, `has_empty_value`, and `status` must be non-null and writable.
/// The cursor arguments must remain live, including the field string for the
/// resulting filter's lifetime. `gf` must not already own resources.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_GeoFilter_LegacyParse(
    gf: *mut LegacyGeoFilter,
    ac: *mut ffi::ArgsCursor,
    has_empty_value: *mut bool,
    status: *mut OpaqueQueryError,
) -> c_int {
    // SAFETY: caller supplies exclusive output storage, which may be uninitialized.
    unsafe {
        gf.write(LegacyGeoFilter {
            base: GeoFilter::from_core(geo::GeoFilter::new(0.0, 0.0, 0.0, None)),
            field: std::ptr::null_mut(),
        });
    }
    // SAFETY: gf is now initialized and exclusively borrowed.
    let gf = unsafe { &mut *gf };
    // SAFETY: caller supplies exclusive cursor storage.
    let ac = unsafe { &mut *ac };
    // SAFETY: caller supplies exclusive flag storage.
    let has_empty_value = unsafe { &mut *has_empty_value };
    let start = ac.offset;
    let args = CursorArgs {
        cursor: ac,
        start,
        numbers: Cell::new([0.0; 3]),
    };
    let mut offset = start;
    let parsed = geo::LegacyGeoFilter::parse(&args, &mut offset, has_empty_value);
    let [lon, lat, radius] = args.numbers.get();
    gf.base.lon = lon;
    gf.base.lat = lat;
    gf.base.radius = radius;
    let result = match parsed {
        Ok(parsed) => {
            gf.base = GeoFilter::from_core(parsed.base);
            let field = args.string(start).expect("field was already parsed");
            // SAFETY: AC_GetString's buffer is NUL-terminated and borrowed for the
            // filter lifetime. The core's field length stops at its first NUL.
            gf.field =
                unsafe { ffi::NewHiddenString(field.as_ptr().cast(), parsed.field.len(), false) };
            redis_module::REDISMODULE_OK as c_int
        }
        Err(error) => {
            // SAFETY: caller supplies an initialized writable query error.
            let status =
                unsafe { QueryError::from_opaque_mut_ptr(status) }.expect("status is null");
            match error {
                LegacyGeoParseError::MissingArguments => {
                    status.set_error(QueryErrorCode::ParseArgs, "GEOFILTER requires 5 arguments")
                }
                LegacyGeoParseError::BadArgument { argument, reason } => status.set_with_user_data(
                    QueryErrorCode::ParseArgs,
                    "Bad arguments",
                    &format!(" for <{argument}>: {reason}"),
                ),
                LegacyGeoParseError::UnknownUnit(unit) => {
                    gf.base.unitType = GEO_DISTANCE_INVALID;
                    // Preserve non-UTF-8 user data and its obfuscation boundary.
                    let mut private = QueryErrorCode::ParseArgs.prefix_c_str().to_bytes().to_vec();
                    private.extend_from_slice(b"Unknown distance unit ");
                    private.extend_from_slice(&unit);
                    status.set_code_and_messages(
                        QueryErrorCode::ParseArgs,
                        Some(c"Unknown distance unit".to_owned()),
                        Some(CString::new(private).expect("core truncates unit at NUL")),
                    );
                }
            }
            redis_module::REDISMODULE_ERR as c_int
        }
    };
    ac.offset = offset;
    result
}

/// Release the borrowed field wrapper and Redis-allocated legacy filter.
///
/// # Safety
/// `gf` must be a live Redis allocation with exclusively owned resources, as
/// initialized by [`Rust_GeoFilter_LegacyParse`]. Its borrowed field buffer is not freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Rust_LegacyGeoFilter_Free(gf: *mut LegacyGeoFilter) {
    // SAFETY: caller supplies a live filter.
    let field = unsafe { (*gf).field };
    if !field.is_null() {
        // SAFETY: the filter owns the wrapper but borrows its backing bytes.
        unsafe { ffi::HiddenString_Free(field, false) };
    }
    // SAFETY: base is the first repr(C) member of this exclusively owned Redis allocation.
    unsafe { Rust_GeoFilter_Free(gf.cast()) };
}
