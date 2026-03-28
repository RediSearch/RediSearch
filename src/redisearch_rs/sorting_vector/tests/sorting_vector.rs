/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Mock or stub the ones that aren't provided by the line above
redis_mock::mock_or_stub_missing_redis_c_symbols!();

use sorting_vector::{IndexOutOfBounds, RSSortingVector};
use value::shared::{SHARED_VALUE_CONTENT_SIZE, SHARED_VALUE_SIZE};
use value::{SharedValue, Value};

#[test]
#[cfg_attr(miri, ignore = "Calls FFI function `RSValue_NullStatic`")]
fn creation() {
    let vector: RSSortingVector = RSSortingVector::new(10);
    assert_eq!(vector.len(), 10);
    assert_eq!(vector.iter().count(), 10);

    for value in vector {
        assert!(value.is_null_static());
    }
}

fn build_vector() -> Result<RSSortingVector, IndexOutOfBounds> {
    let mut vector = RSSortingVector::new(4);
    vector.try_insert_num(0, 42.0)?;
    vector.try_insert_string(1, b"abcdefg".to_vec())?;
    vector.try_insert_string_normalize(2, "Hello World")?;
    vector.try_insert_null(3)?;
    Ok(vector)
}

#[test]
#[cfg_attr(miri, ignore = "Calls FFI function `RSValue_NullStatic`")]
fn insert() -> Result<(), IndexOutOfBounds> {
    let vector: &mut RSSortingVector = &mut build_vector()?;

    assert!(matches!(*vector[0], Value::Number(42.0)));
    assert_eq!(vector[1].as_str_bytes(), Some("abcdefg".as_bytes()));
    assert_eq!(vector[2].as_str_bytes(), Some("hello world".as_bytes())); // we normalize --> lowercase
    assert!(vector[3].is_null_static());

    Ok(())
}

#[test]
#[cfg_attr(miri, ignore = "Calls FFI function `RSValue_NullStatic`")]
fn out_of_bounds() -> Result<(), IndexOutOfBounds> {
    let mut vector = build_vector()?;

    assert_eq!(vector.len(), 4);
    let reval = vector.try_insert_num(5, 1.0);
    assert!(reval.is_err());
    Ok(())
}

#[test]
#[cfg_attr(miri, ignore = "Calls FFI function `RSValue_NullStatic`")]
fn override_value() -> Result<(), IndexOutOfBounds> {
    let src = build_vector()?;
    let mut dst: RSSortingVector = RSSortingVector::new(1);
    assert!(SharedValue::ptr_eq(&dst[0], &SharedValue::null_static()));

    for (idx, val) in src.iter().enumerate() {
        dst.try_insert_val(0, val.clone())?;
        assert!(SharedValue::ptr_eq(&dst[0], &src[idx]));
    }

    // the following is only possible in Rust API
    let mut dupl = src.clone();
    for val in dupl.iter_mut() {
        *val = SharedValue::new_num(42.0)
    }

    assert!(SharedValue::ptr_eq(&dst[0], &SharedValue::null_static()));
    Ok(())
}

#[test]
#[cfg_attr(miri, ignore = "Calls FFI function `RSValue_NullStatic`")]
fn memory_size() -> Result<(), IndexOutOfBounds> {
    let empty = RSSortingVector::new(0);
    let size = empty.get_memory_size();
    assert!(empty.is_empty());
    assert_eq!(size, 0);

    let mut vec = RSSortingVector::new(1);
    vec.try_insert_num(0, 42.0)?;
    let size = vec.get_memory_size();
    let expected_size = SHARED_VALUE_SIZE + SHARED_VALUE_CONTENT_SIZE;
    assert_eq!(size, expected_size);

    // test with more complex values
    let vector = build_vector()?;
    let size = vector.get_memory_size();
    let expected_size = 4 * SHARED_VALUE_SIZE // 4 RSValue pointers
            + (3 * SHARED_VALUE_CONTENT_SIZE) // 3 actually allocated RSValues (the 4th is a null)
            + "abcdefg".len() // size of the string "abcdefg"
            + "Hello World".len(); // size of the string "Hello World"

    assert_eq!(size, expected_size);
    Ok(())
}

#[test]
#[cfg_attr(
    miri,
    ignore = "icu_casemap causes errors under miri, see <https://github.com/unicode-org/icu4x/issues/6723>"
)]
fn case_folding_aka_normalization() -> Result<(), IndexOutOfBounds> {
    let str = "Straße";
    let mut vec: RSSortingVector = RSSortingVector::new(1);
    vec.try_insert_string_normalize(0, str)?;
    assert_eq!(vec[0].as_str_bytes(), Some("strasse".as_bytes()));
    Ok(())
}
