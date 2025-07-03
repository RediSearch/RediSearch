/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use sorting_vector::normalized_string::NormalizedString;
use sorting_vector::{IndexOutOfBounds, RSSortingVector, RSValueTrait};

#[derive(Clone, Debug, PartialEq)]
enum RSValueMock {
    Null,
    Number(f64),
    String(String),
    Reference(Box<RSValueMock>),
}

impl RSValueTrait for RSValueMock {
    fn create_null() -> Self {
        RSValueMock::Null
    }

    fn create_string(s: NormalizedString) -> Self {
        RSValueMock::String(s.into())
    }

    fn create_num(num: f64) -> Self {
        RSValueMock::Number(num)
    }

    fn create_ref(value: Self) -> Self {
        RSValueMock::Reference(Box::new(value))
    }

    fn is_null(&self) -> bool {
        matches!(self, RSValueMock::Null)
    }

    fn get_ref(&self) -> Option<&Self> {
        if let RSValueMock::Reference(boxed) = self {
            Some(boxed)
        } else {
            None
        }
    }

    fn get_ref_mut(&mut self) -> Option<&mut Self> {
        if let RSValueMock::Reference(boxed) = self {
            Some(boxed.as_mut())
        } else {
            None
        }
    }

    fn as_str(&self) -> Option<&str> {
        if let RSValueMock::String(s) = self {
            Some(s)
        } else {
            None
        }
    }

    fn as_num(&self) -> Option<f64> {
        if let RSValueMock::Number(num) = self {
            Some(*num)
        } else {
            None
        }
    }

    fn get_type(&self) -> ffi::RSValueType {
        // Mock implementation, return a dummy type
        match &self {
            RSValueMock::Null => ffi::RSValueType_RSValue_Null,
            RSValueMock::Number(_) => ffi::RSValueType_RSValue_Number,
            RSValueMock::String(_) => ffi::RSValueType_RSValue_String,
            RSValueMock::Reference(reference) => reference.get_type(),
        }
    }

    fn is_ptr_type() -> bool {
        // Mock implementation, return false
        false
    }
}

#[test]
fn test_creation() {
    let vector: RSSortingVector<RSValueMock> = RSSortingVector::new(10);
    assert_eq!(vector.len(), 10);

    for value in vector {
        assert!(value.is_null());
    }
}

fn build_vector() -> Result<RSSortingVector<RSValueMock>, IndexOutOfBounds> {
    let mut vector = RSSortingVector::new(5);
    vector.try_insert_num(0, 42.0)?;
    vector.try_insert_string(1, "abcdefg")?;
    vector.try_insert_val_as_ref(2, RSValueMock::create_num(3.))?;
    vector.try_insert_val(3, RSValueMock::create_string("Hello World".into()))?;
    vector.try_insert_null(4)?;
    Ok(vector)
}

#[test]
fn test_insert() -> Result<(), IndexOutOfBounds> {
    let vector: &mut RSSortingVector<RSValueMock> = &mut build_vector()?;

    assert_eq!(vector[0].as_num(), Some(42.0));
    assert_eq!(vector[0].get_type(), ffi::RSValueType_RSValue_Number);
    assert_eq!(vector[1].as_str(), Some("abcdefg"));
    assert_eq!(vector[1].get_type(), ffi::RSValueType_RSValue_String);
    assert_eq!(vector[2].get_ref().unwrap().as_num(), Some(3.0));
    assert_eq!(vector[3].as_str(), Some("hello world")); // we normalize --> lowercase
    assert!(vector[4].is_null());

    Ok(())
}

#[test]
fn test_override() -> Result<(), IndexOutOfBounds> {
    let src = build_vector()?;
    let mut dst: RSSortingVector<RSValueMock> = RSSortingVector::new(1);
    assert_eq!(dst[0], RSValueMock::create_null());

    for (idx, val) in src.iter().enumerate() {
        dst.try_insert_val(0, val.clone())?;
        assert_eq!(dst[0], src[idx]);
    }

    // the following is only possible in Rust API
    let mut dupl = src.clone();
    for val in dupl.iter_mut() {
        *val = RSValueMock::Number(42.0)
    }

    assert_eq!(dst[0], RSValueMock::create_null());
    Ok(())
}

#[test]
fn test_memory_size() -> Result<(), IndexOutOfBounds> {
    let empty = RSSortingVector::<RSValueMock>::new(0);
    let size = empty.get_memory_size();
    assert!(empty.is_empty());
    assert_eq!(size, 0);

    let mut vec = RSSortingVector::<RSValueMock>::new(1);
    vec.try_insert_num(0, 42.0)?;
    let size = vec.get_memory_size();
    let expected_size = std::mem::size_of::<RSValueMock>();
    assert_eq!(size, expected_size);

    // test with more complex values
    let vector = build_vector()?;
    let size = vector.get_memory_size();

    let expected_size = 5* std::mem::size_of::<RSValueMock>() // 4 RSValues
            + "abcdefg".len() // size of the string "Hello"
            + "Hello World".len(); // size of the string "World"

    assert_eq!(size, expected_size);
    Ok(())
}

#[test]
#[cfg(not(miri))]
fn test_case_folding_aka_normlization_rust_impl() -> Result<(), IndexOutOfBounds> {
    // not miri because the C implementation `normalizeStr` is called as part of `put_string_and_normalize`
    // and calling C code from Miri is not supported.

    // The underlying implementation still uses C because ICU raised errors over Miri
    // The ICU package is the official package for Unicode, also unicode casing which is called normalization
    // in RediSearch.
    //
    // ```
    // = help: this indicates a potential bug in the program: it performed an invalid operation, but the Stacked Borrows rules it violated are still experimental`
    // ```
    //
    // Further Information:
    // As long as we use SortingVector over the C interface we won't call this function directly, but stick to
    // the Method used in the C API, called `normalizeStr`.
    //
    // Issue opened: https://github.com/unicode-org/icu4x/issues/6723
    //
    // For now the actual implementation binds the C code based on nulib.

    let str = "Straße";
    let mut vec: RSSortingVector<RSValueMock> = RSSortingVector::new(1);
    vec.try_insert_string(0, str)?;
    assert_eq!(vec[0].as_str(), Some("strasse"));
    Ok(())
}
