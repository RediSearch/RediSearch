/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use sorting_vector::{IndexOutOfBounds, RSSortingVector};
use value::RSValueTrait;

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

    fn create_string(s: String) -> Self {
        RSValueMock::String(s)
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

    fn increment(&mut self) {
        todo!("not used in that mock")
    }

    fn decrement(&mut self) {
        todo!("not used in that mock")
    }
}

#[test]
fn test_creation() {
    let vector: RSSortingVector<RSValueMock> = RSSortingVector::new(10);
    assert_eq!(vector.len(), 10);
    assert_eq!(vector.iter().count(), 10);

    for value in vector {
        assert!(value.is_null());
    }
}

fn build_vector() -> Result<RSSortingVector<RSValueMock>, IndexOutOfBounds> {
    let mut vector = RSSortingVector::new(5);
    vector.try_insert_num(0, 42.0)?;
    vector.try_insert_string(1, "abcdefg")?;
    vector.try_insert_val_as_ref(2, RSValueMock::create_num(3.))?;
    vector.try_insert_string(3, "Hello World")?;
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
fn test_out_of_bounds() -> Result<(), IndexOutOfBounds> {
    let mut vector = build_vector()?;

    assert_eq!(vector.len(), 5);
    let reval = vector.try_insert_num(5, 1.0);
    assert!(reval.is_err());
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

    let expected_size = 5 * std::mem::size_of::<RSValueMock>() // 5 RSValues
            + "abcdefg".len() // size of the string "abcdefg"
            + "Hello World".len(); // size of the string "Hello World"

    assert_eq!(size, expected_size);
    Ok(())
}

#[test]
#[cfg(not(miri))]
fn test_case_folding_aka_normalization() -> Result<(), IndexOutOfBounds> {
    // Not in Miri because icu_casemap raised errors over Miri, see https://github.com/unicode-org/icu4x/issues/6723

    let str = "Straße";
    let mut vec: RSSortingVector<RSValueMock> = RSSortingVector::new(1);
    vec.try_insert_string(0, str)?;
    assert_eq!(vec[0].as_str(), Some("strasse"));
    Ok(())
}
