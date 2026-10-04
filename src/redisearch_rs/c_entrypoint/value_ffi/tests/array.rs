/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use value_ffi::array::*;
use value_ffi::constructors::RSValue_NewNumber;
use value_ffi::getters::RSValue_Number_Get;
use value_ffi::shared::RSValue_DecrRef;

// Keep the Rust FFI exports needed by the linked C bundle from being stripped.
extern crate redisearch_rs;
// Mock or stub the C symbols the bundle needs but the line above does not provide.
redis_mock::mock_or_stub_missing_redis_c_symbols!();

#[test]
fn test_array() {
    unsafe {
        let array = RSValue_NewArrayBuilder(3);
        let one = RSValue_NewNumber(1.0);
        let two = RSValue_NewNumber(2.0);
        let three = RSValue_NewNumber(3.0);
        array.add(0).write(one);
        array.add(1).write(two);
        array.add(2).write(three);

        let array = RSValue_NewArrayFromBuilder(array, 3);

        assert_eq!(RSValue_ArrayLen(array), 3);

        let val = RSValue_ArrayItem(array, 0);
        let num = RSValue_Number_Get(val);
        assert_eq!(1.0, num);

        let val = RSValue_ArrayItem(array, 1);
        let num = RSValue_Number_Get(val);
        assert_eq!(2.0, num);

        let val = RSValue_ArrayItem(array, 2);
        let num = RSValue_Number_Get(val);
        assert_eq!(3.0, num);

        RSValue_DecrRef(array);
    }
}
