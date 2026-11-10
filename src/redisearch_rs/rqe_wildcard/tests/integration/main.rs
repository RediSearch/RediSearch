/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Debug assertions pull in the C bundle; use its shared FFI link guard as modules are ported.
redis_mock::mock_or_stub_missing_redis_c_symbols!();
extern crate redisearch_rs as _;

mod fmt;
mod matches;
mod parse;
// Disable the proptests when testing with Miri,
// as proptest accesses the file system, which is not supported by Miri
#[cfg(not(miri))]
mod properties;
mod remove_escape;
mod utils;
