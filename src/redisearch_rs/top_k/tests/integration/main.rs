/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Provide stubs for C symbols (RedisModule_Free, ResultMetrics_Free, etc.)
// that rqe_iterators and inverted_index reference at link time.
redis_mock::mock_or_stub_missing_redis_c_symbols!();
extern crate redisearch_rs;

mod iterator;
