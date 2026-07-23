/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#![cfg_attr(miri, allow(dead_code, unused_imports))]

redis_mock::mock_or_stub_missing_redis_c_symbols!();
extern crate redisearch_rs;

mod runes_to_string;
mod runes_to_utf8;
mod str_to_lower_runes;
mod tag_normalize;
mod unicode_tolower;
mod unicode_tolower_capped;
mod utf8_to_lower_runes;
mod utf8_to_runes;
