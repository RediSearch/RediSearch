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

#[test]
#[cfg_attr(
    miri,
    ignore = "legacy adapter calls the C ArgsCursor and HiddenString APIs"
)]
fn cursor_progress_partial_output_and_owned_wrapper() {
    crate::init_allocator();
    for (args, offset, numbers, unit, message) in [
        (
            vec![c"g", c"12.5", c"-34.5", c"6.25", c"Mi", c"NEXT"],
            5,
            [12.5, -34.5, 6.25],
            GEO_DISTANCE_MI,
            None,
        ),
        (
            vec![c"g", c"0", c"0", c"1"],
            0,
            [0.0, 0.0, 0.0],
            GEO_DISTANCE_KM,
            Some("GEOFILTER requires 5 arguments"),
        ),
        (
            vec![c"g", c"bad", c"0", c"1", c"m"],
            1,
            [0.0, 0.0, 0.0],
            GEO_DISTANCE_KM,
            Some("for <lon>"),
        ),
        (
            vec![c"g", c"12.5", c"bad", c"1", c"m"],
            2,
            [12.5, 0.0, 0.0],
            GEO_DISTANCE_KM,
            Some("for <lat>"),
        ),
        (
            vec![c"g", c"12.5", c"", c"bad", c"m"],
            3,
            [12.5, 0.0, 0.0],
            GEO_DISTANCE_KM,
            Some("for <radius>"),
        ),
        (
            vec![c"g", c"12.5", c"-34.5", c"6.25", c"\xff"],
            5,
            [12.5, -34.5, 6.25],
            GEO_DISTANCE_INVALID,
            Some("Unknown distance unit"),
        ),
    ] {
        let mut pointers: Vec<_> = args
            .iter()
            .map(|arg| arg.as_ptr().cast_mut().cast())
            .collect();
        let mut cursor = ffi::ArgsCursor {
            objs: pointers.as_mut_ptr(),
            type_: ffi::ACType_AC_TYPE_CHAR as i32,
            argc: pointers.len(),
            offset: 0,
        };
        let mut error = QueryError::default();
        let mut empty = false;
        // SAFETY: the mock allocator is initialized and the requested size fits the filter.
        let raw =
            unsafe { crate::rm_alloc_impl(size_of::<LegacyGeoFilter>()) }.cast::<LegacyGeoFilter>();
        // SAFETY: LegacyParse initializes raw; cursor buffers and output locals
        // are live through the call, and arguments outlive the resulting filter.
        let result = unsafe {
            GeoFilter_LegacyParse(raw, &mut cursor, &mut empty, error.as_opaque_mut_ptr())
        };
        assert_eq!(result, i32::from(message.is_some()));
        assert_eq!(cursor.offset, offset);
        assert!(!empty);
        // SAFETY: LegacyParse initialized the entire filter even on failure.
        let filter = unsafe { &*raw };
        let base = &filter.base;
        assert_eq!([base.lon, base.lat, base.radius], numbers);
        assert_eq!(base.unitType, unit);
        assert_eq!(base.fieldIndex, RS_INVALID_FIELD_INDEX);
        assert!(base.numericFilters.is_null());
        if let Some(message) = message {
            assert_eq!(error.code(), QueryErrorCode::ParseArgs);
            let private = error.private_message().unwrap().to_bytes();
            assert!(
                private
                    .windows(message.len())
                    .any(|part| part == message.as_bytes())
            );
            if unit == GEO_DISTANCE_INVALID {
                assert!(private.ends_with(b"\xff"));
            }
            assert!(filter.field.is_null());
        } else {
            assert!(error.is_ok());
            // SAFETY: successful parsing creates a live field wrapper.
            let field = unsafe { ffi::HiddenString_GetUnsafe(filter.field, ptr::null_mut()) };
            assert_eq!(field, args[0].as_ptr());
            // SAFETY: the wrapper borrows the original NUL-terminated argument.
            assert_eq!(unsafe { CStr::from_ptr(field) }, c"g");
        }
        // SAFETY: raw is exclusively owned and neither filter nor field is used afterwards.
        unsafe { LegacyGeoFilter_Free(raw) };
    }
}
