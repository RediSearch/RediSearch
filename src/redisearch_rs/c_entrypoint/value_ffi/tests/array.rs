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

#[test]
fn array_distinct_keeps_the_first_of_each_value_in_order() {
    unsafe {
        let numbers = [2.0, 1.0, 2.0, 3.0, 1.0];
        let builder = RSValue_NewArrayBuilder(numbers.len() as u32);
        for (i, num) in numbers.into_iter().enumerate() {
            builder.add(i).write(RSValue_NewNumber(num));
        }
        let array = RSValue_NewArrayFromBuilder(builder, numbers.len() as u32);

        let mut out = [std::ptr::null(); 5];
        let written = RSValue_ArrayDistinct(array, out.as_mut_ptr());

        let distinct: Vec<f64> = out[..written as usize]
            .iter()
            .map(|&value| RSValue_Number_Get(value))
            .collect();
        assert_eq!(distinct, [2.0, 1.0, 3.0]);

        RSValue_DecrRef(array);
    }
}

#[test]
fn array_distinct_of_an_empty_array_writes_nothing() {
    unsafe {
        let array = RSValue_NewArrayFromBuilder(RSValue_NewArrayBuilder(0), 0);
        assert_eq!(RSValue_ArrayDistinct(array, std::ptr::null_mut()), 0);
        RSValue_DecrRef(array);
    }
}

#[test]
fn array_distinct_of_a_long_array() {
    // Longer than the arrays it de-duplicates without allocating.
    unsafe {
        let numbers: Vec<f64> = (0..40).map(|i| f64::from(i % 25)).collect();
        let len = numbers.len() as u32;
        let builder = RSValue_NewArrayBuilder(len);
        for (i, &num) in numbers.iter().enumerate() {
            builder.add(i).write(RSValue_NewNumber(num));
        }
        let array = RSValue_NewArrayFromBuilder(builder, len);

        let mut out = vec![std::ptr::null(); numbers.len()];
        let written = RSValue_ArrayDistinct(array, out.as_mut_ptr());

        let distinct: Vec<f64> = out[..written as usize]
            .iter()
            .map(|&value| RSValue_Number_Get(value))
            .collect();
        assert_eq!(distinct, (0..25).map(f64::from).collect::<Vec<_>>());

        RSValue_DecrRef(array);
    }
}
