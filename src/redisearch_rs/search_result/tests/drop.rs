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

use rlookup::{RLookupKey, RLookupKeyFlags};
use search_result::SearchResult;
use value::SharedValue;

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn drop_releases_dynamic_row_values() {
    let value = SharedValue::new_num(42.0);
    let key = RLookupKey::new(c"field", RLookupKeyFlags::empty());

    {
        let mut result = SearchResult::new();
        result.row_data_mut().write_key(&key, value.clone());
        assert_eq!(SharedValue::refcount(&value), 2);
    }

    assert_eq!(SharedValue::refcount(&value), 1);
}
