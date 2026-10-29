/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

#[test]
fn oversized_unit_does_not_read_storage() {
    for len in [16, usize::MAX] {
        for ptr in [c"m".as_ptr(), std::ptr::null()] {
            // SAFETY: rejected lengths must not access the pointer, even under Miri.
            let unit = unsafe { geo_ffi::Rust_GeoDistance_Parse_Buffer(ptr, len) };
            assert_eq!(unit, geo::DistanceUnit::Invalid as i32);
        }
    }
}
