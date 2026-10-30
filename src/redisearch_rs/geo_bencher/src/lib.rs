/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Live C symbols used by paired Geo ABI benchmarks.

use std::ffi::{c_char, c_int};

use ffi::{ArgsCursor, GeoDistance, GeoFilter, LegacyGeoFilter};
use query_error::opaque::OpaqueQueryError;

extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

unsafe extern "C" {
    /// C baseline; `s` must be a readable NUL-terminated string.
    pub fn GeoDistance_Parse(s: *const c_char) -> GeoDistance;
    /// C baseline; `s` must be readable for `len` bytes.
    pub fn GeoDistance_Parse_Buffer(s: *const c_char, len: usize) -> GeoDistance;
    /// C baseline; `gf` is readable and `distance` is null or disjoint writable storage.
    pub fn isWithinRadius(gf: *const GeoFilter, score: f64, distance: *mut f64) -> c_int;
    /// C baseline; filter and initialized query-error storage are live and disjoint.
    pub fn GeoFilter_Validate(gf: *const GeoFilter, status: *mut OpaqueQueryError) -> c_int;
    /// C baseline; all pointers are live, disjoint and writable. Arguments outlive `gf`.
    pub fn GeoFilter_LegacyParse(
        gf: *mut LegacyGeoFilter,
        ac: *mut ArgsCursor,
        has_empty: *mut bool,
        status: *mut OpaqueQueryError,
    ) -> c_int;
    /// C baseline; `gf` uniquely owns a Redis-allocated legacy filter and its resources.
    pub fn LegacyGeoFilter_Free(gf: *mut LegacyGeoFilter);
}
