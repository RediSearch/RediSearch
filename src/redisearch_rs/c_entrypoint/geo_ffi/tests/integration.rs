/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use geo_ffi::parseGeo;
use query_error::{QueryError, QueryErrorCode};

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// An empty input never reads `c`, so a NULL buffer is accepted and reported
/// like any other unparsable string.
#[test]
fn an_empty_null_buffer_is_a_parse_error() {
    let (mut lon, mut lat) = (0.0, 0.0);
    let mut status = QueryError::default();

    // SAFETY: (1) `len` is 0, so `c` may be NULL. (2) `lon` and `lat` are live
    // `f64`s. (3) `status` is a live `QueryError`.
    let rc = unsafe {
        parseGeo(
            std::ptr::null(),
            0,
            &mut lon,
            &mut lat,
            status.as_opaque_mut_ptr(),
        )
    };

    assert_eq!(rc, redis_module::REDISMODULE_ERR as i32);
    assert!(status.code() == QueryErrorCode::ParseArgs);
}

#[test]
fn a_comma_separated_pair_is_parsed() {
    let input = b"1.5,2.5";
    let (mut lon, mut lat) = (0.0, 0.0);
    let mut status = QueryError::default();

    // SAFETY: (1) `input` is a live region of `input.len()` bytes. (2) `lon`
    // and `lat` are live `f64`s. (3) `status` is a live `QueryError`.
    let rc = unsafe {
        parseGeo(
            input.as_ptr(),
            input.len(),
            &mut lon,
            &mut lat,
            status.as_opaque_mut_ptr(),
        )
    };

    assert_eq!(rc, redis_module::REDISMODULE_OK as i32);
    assert_eq!((lon, lat), (1.5, 2.5));
    assert!(status.is_ok());
}
