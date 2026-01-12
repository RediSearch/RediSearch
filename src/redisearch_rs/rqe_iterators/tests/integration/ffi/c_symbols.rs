/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Mock implementations of C symbol that aren't provided
//! by the static C libraries we are linking against in build.rs.

redis_mock::bind_redis_alloc_symbols_to_mock_impl!();
extern crate redisearch_rs;
