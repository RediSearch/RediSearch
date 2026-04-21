/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use value_ffi::RSValue;
use value_ffi::constructors::RSValue_NewNumber;
use value_ffi::getters::RSValue_Number_Get;
use value_ffi::map::*;
use value_ffi::shared::RSValue_DecrRef;

#[allow(non_upper_case_globals)]
#[unsafe(no_mangle)]
pub static mut RSDummyContext: *mut redis_mock::ffi::RedisModuleCtx =
    redis_mock::globals::redis_module_ctx();

#[test]
fn test_map() {
    unsafe {
        let map = RSValue_NewMapBuilder(2);
        let key_one = RSValue_NewNumber(1.0);
        let value_one = RSValue_NewNumber(2.0);
        let key_two = RSValue_NewNumber(3.0);
        let value_two = RSValue_NewNumber(4.0);
        RSValue_MapBuilderSetEntry(map, 0, key_one, value_one);
        RSValue_MapBuilderSetEntry(map, 1, key_two, value_two);

        let map = RSValue_NewMapFromBuilder(map);

        assert_eq!(RSValue_Map_Len(map), 2);

        let mut key: *mut RSValue = std::ptr::null_mut();
        let mut value: *mut RSValue = std::ptr::null_mut();

        RSValue_Map_GetEntry(map, 0, &mut key as *mut _, &mut value as *mut _);
        let key_num = RSValue_Number_Get(key);
        assert_eq!(1.0, key_num);
        let value_num = RSValue_Number_Get(value);
        assert_eq!(2.0, value_num);

        RSValue_Map_GetEntry(map, 1, &mut key as *mut _, &mut value as *mut _);
        let key_num = RSValue_Number_Get(key);
        assert_eq!(3.0, key_num);
        let value_num = RSValue_Number_Get(value);
        assert_eq!(4.0, value_num);

        RSValue_DecrRef(map);
    }
}
