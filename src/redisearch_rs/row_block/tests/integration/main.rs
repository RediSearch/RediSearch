/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! End-to-end tests for the row block wire format.
//!
//! Everything goes through the public encoder and decoder, driven the way the aggregate reply
//! path drives them: build an `RLookup`, write a schema, append rows, read the bytes back.

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Mock or stub the ones that aren't provided by the line above
redis_mock::mock_or_stub_missing_redis_c_symbols!();

mod bitmap;
mod decode;
mod harness;
mod properties;
mod refusal;
mod schema;
mod trio;
mod values;
