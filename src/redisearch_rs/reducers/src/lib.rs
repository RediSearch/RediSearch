/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

pub mod collect;
mod reducer;
mod reducer_options;

pub use reducer::Reducer;
pub use reducer_options::ReducerOptions;

// Link both Rust-provided and C-provided symbols during unit tests.
#[cfg(test)]
extern crate redisearch_rs;
// Mock or stub the ones that aren't provided by the line above.
#[cfg(test)]
redis_mock::mock_or_stub_missing_redis_c_symbols!();
