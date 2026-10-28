/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::CStr;
use std::ptr;

use geo_ffi::*;
use query_error::{QueryError, QueryErrorCode};
use rqe_core::RS_INVALID_FIELD_INDEX;

const fn filter(unit: GeoDistance) -> GeoFilter {
    GeoFilter {
        fieldIndex: RS_INVALID_FIELD_INDEX,
        lon: 0.0,
        lat: 0.0,
        radius: 1.0,
        unitType: unit,
        numericFilters: ptr::null_mut(),
    }
}

#[test]
fn unit_adapters_preserve_integer_and_buffer_contracts() {
    for (text, unit, spelling) in [
        (c"kM", GEO_DISTANCE_KM, c"km"),
        (c"M", GEO_DISTANCE_M, c"m"),
        (c"Ft", GEO_DISTANCE_FT, c"ft"),
        (c"mI", GEO_DISTANCE_MI, c"mi"),
        (c"bad", GEO_DISTANCE_INVALID, c"<badunit>"),
    ] {
        // SAFETY: text is NUL-terminated.
        assert_eq!(unsafe { Rust_GeoDistance_Parse(text.as_ptr()) }, unit);
        // SAFETY: text is readable for its byte length.
        let parsed = unsafe { Rust_GeoDistance_Parse_Buffer(text.as_ptr(), text.count_bytes()) };
        assert_eq!(parsed, unit);
        assert_eq!(
            // SAFETY: ToString returns a static NUL-terminated string.
            unsafe { CStr::from_ptr(Rust_GeoDistance_ToString(unit)) },
            spelling
        );
    }
    for len in [0, 16] {
        assert_eq!(
            // SAFETY: zero and over-limit lengths never dereference the pointer.
            unsafe { Rust_GeoDistance_Parse_Buffer(ptr::null(), len) },
            GEO_DISTANCE_INVALID
        );
    }
    assert_eq!(
        // SAFETY: the embedded-NUL buffer is readable for the supplied length.
        unsafe { Rust_GeoDistance_Parse_Buffer(b"m\0x".as_ptr().cast(), 3) },
        GEO_DISTANCE_M
    );
    assert_eq!(
        // SAFETY: ToString returns a static NUL-terminated string even for unknown units.
        unsafe { CStr::from_ptr(Rust_GeoDistance_ToString(123)) },
        c"<badunit>"
    );
}

#[test]
fn validation_maps_errors_and_preserves_unknown_integer_units() {
    for (unit, lon, radius, message) in [
        (
            GEO_DISTANCE_INVALID,
            181.0,
            -1.0,
            Some("Invalid GeoFilter unit"),
        ),
        (
            GEO_DISTANCE_M,
            181.0,
            -1.0,
            Some("Invalid GeoFilter lat/lon"),
        ),
        (GEO_DISTANCE_M, 0.0, -1.0, Some("Invalid GeoFilter radius")),
        (GEO_DISTANCE_M, 0.0, 1.0, None),
        (123, 0.0, 1.0, None),
    ] {
        let mut filter = filter(unit);
        filter.lon = lon;
        filter.radius = radius;
        let mut error = QueryError::default();
        // SAFETY: both locals remain live, and error is initialized and writable.
        let result = unsafe { Rust_GeoFilter_Validate(&filter, error.as_opaque_mut_ptr()) };
        assert_eq!(result, i32::from(message.is_none()));
        if let Some(message) = message {
            assert_eq!(error.code(), QueryErrorCode::Syntax);
            assert!(
                error
                    .private_message()
                    .unwrap()
                    .to_bytes()
                    .ends_with(message.as_bytes())
            );
        } else {
            assert!(error.is_ok());
        }
    }
}

#[test]
fn field_and_radius_adapters_handle_optional_pointers() {
    let mut filter = filter(GEO_DISTANCE_M);
    // SAFETY: FieldSpec consists of C scalar fields, raw pointers, and a union;
    // zero is valid for each. SetField only reads the index.
    let mut field: ffi::FieldSpec = unsafe { std::mem::zeroed() };
    field.index = 7;
    // SAFETY: the filter and field are live locals.
    unsafe { Rust_GeoFilter_SetField(&mut filter, &field) };
    assert_eq!(filter.fieldIndex, 7);
    // SAFETY: filter is writable; a null field clears the index.
    unsafe { Rust_GeoFilter_SetField(&mut filter, ptr::null()) };
    assert_eq!(filter.fieldIndex, RS_INVALID_FIELD_INDEX);
    let score = encodeGeo(0.01, 0.0);
    let mut distance = -1.0;
    for unit in [
        GEO_DISTANCE_KM,
        GEO_DISTANCE_M,
        GEO_DISTANCE_FT,
        GEO_DISTANCE_MI,
    ] {
        filter.unitType = unit;
        filter.radius = 10_000.0;
        assert_eq!(
            // SAFETY: filter is live and distance is writable.
            unsafe { Rust_isWithinRadius(&filter, score, &mut distance) },
            1
        );
        assert!(distance > 1_000.0 && distance < 1_200.0);
        filter.radius = 0.0;
        assert_eq!(
            // SAFETY: filter is live and a null distance output is allowed.
            unsafe { Rust_isWithinRadius(&filter, score, ptr::null_mut()) },
            0
        );
    }
}

#[test]
#[cfg_attr(miri, ignore = "Redis allocator API pointers are extern statics")]
fn allocated_filters_default_or_parse_units_and_free() {
    crate::init_allocator();
    for (unit, len, expected) in [
        (ptr::null(), 0, GEO_DISTANCE_KM),
        (c"FT".as_ptr(), 2, GEO_DISTANCE_FT),
        (c"".as_ptr(), 0, GEO_DISTANCE_INVALID),
    ] {
        // SAFETY: unit buffers are valid and the allocator is initialized.
        let raw = unsafe { Rust_NewGeoFilter(12.5, -34.5, 6.25, unit, len) };
        // SAFETY: NewGeoFilter returned an initialized allocation, borrowed until Free.
        let filter = unsafe { &*raw };
        assert_eq!(filter.unitType, expected);
        assert_eq!(filter.fieldIndex, RS_INVALID_FIELD_INDEX);
        assert!(filter.numericFilters.is_null());
        assert_eq!((filter.lon, filter.lat, filter.radius), (12.5, -34.5, 6.25));
        // SAFETY: the allocation is exclusively owned and no reference is used afterwards.
        unsafe { Rust_GeoFilter_Free(raw) };
    }
}
