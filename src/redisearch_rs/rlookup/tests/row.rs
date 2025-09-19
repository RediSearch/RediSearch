/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    ops::{Deref, DerefMut},
    rc::Rc,
};

use rlookup::{RLookupKey, RLookupKeyFlags, RLookupRow};
use value::RSValueTrait;

/// Mock implementation of RSValueTrait for testing the RLookupRow functionality.
///
/// Mainly tests the increment and decrement methods but may contain a
/// numeric value for testing purposes.
#[derive(Clone, Debug, PartialEq)]
struct MockRSValue {
    value: Rc<Option<f64>>,

    clone: Option<Rc<Option<f64>>>,
}

impl RSValueTrait for MockRSValue {
    fn create_null() -> Self {
        MockRSValue {
            value: Rc::new(None),
            clone: None,
        }
    }

    fn create_string(_: String) -> Self {
        MockRSValue {
            value: Rc::new(None),
            clone: None,
        }
    }

    fn create_num(val: f64) -> Self {
        MockRSValue {
            value: Rc::new(Some(val)),
            clone: None,
        }
    }

    fn create_ref(value: Self) -> Self {
        value
    }

    fn is_null(&self) -> bool {
        false
    }

    fn get_ref(&self) -> Option<&Self> {
        Some(self)
    }

    fn get_ref_mut(&mut self) -> Option<&mut Self> {
        Some(self)
    }

    fn as_str(&self) -> Option<&str> {
        None
    }

    fn as_num(&self) -> Option<f64> {
        *self.value.as_ref()
    }

    fn get_type(&self) -> ffi::RSValueType {
        unimplemented!("do not use this in tests, it is not implemented")
    }

    fn is_ptr_type() -> bool {
        false
    }

    fn increment(&mut self) {
        self.clone = Some(self.value.clone());
    }

    fn decrement(&mut self) {
        self.clone.take();
    }
}

#[test]
fn test_insert_without_gap() {
    let mut row: RLookupRow<MockRSValue> = RLookupRow::new();

    assert!(row.is_empty());
    assert_eq!(row.len(), 0);
    assert_eq!(row.num_dyn_values(), 0);

    // generate test key at index 0
    let key = RLookupKey::new(c"test", RLookupKeyFlags::empty());

    // insert a key at the first position
    row.write_key(&key, MockRSValue::create_num(42.0));
    assert!(!row.is_empty());
    assert_eq!(row.len(), 1);
    assert_eq!(row.num_dyn_values(), 1);
    assert_eq!(row.dyn_values()[0].as_ref().unwrap().as_num(), Some(42.0));

    // insert a key at the second position
    let mut key = RLookupKey::new(c"test2", RLookupKeyFlags::empty());
    key.dstidx = 1;
    row.write_key(&key, MockRSValue::create_num(84.0));
    assert!(!row.is_empty());
    assert_eq!(row.len(), 2);
    assert_eq!(row.num_dyn_values(), 2);
    assert_eq!(row.dyn_values()[1].as_ref().unwrap().as_num(), Some(84.0));
}

#[test]
fn test_insert_with_gap() {
    let mut row: RLookupRow<MockRSValue> = RLookupRow::new();
    assert!(row.is_empty());
    assert_eq!(row.len(), 0);
    assert_eq!(row.num_dyn_values(), 0);

    // generate test key at index 15
    let mut key = RLookupKey::new(c"test", RLookupKeyFlags::empty());
    key.dstidx = 15;
    row.write_key(&key, MockRSValue::create_num(42.0));

    assert!(!row.is_empty());
    assert_eq!(row.len(), 16); // Length should be 16 due to the gap
    assert_eq!(row.num_dyn_values(), 1);
    assert_eq!(row.dyn_values()[15].as_ref().unwrap().as_num(), Some(42.0));
}

#[test]
fn test_insert_non_owned() {
    let mut row: RLookupRow<MockRSValue> = RLookupRow::new();
    assert!(row.is_empty());
    assert_eq!(row.len(), 0);
    assert_eq!(row.num_dyn_values(), 0);

    // generate test key at index 0
    let key = RLookupKey::new(c"test", RLookupKeyFlags::empty());

    // insert a key at the first position
    let mock = MockRSValue::create_num(42.0);
    row.write_key(&key, mock.clone());

    // We have the key outside of the row, so it should have a ref count of 2
    assert_eq!(
        Rc::strong_count(&row.dyn_values()[0].as_ref().unwrap().value),
        2
    );

    drop(mock);
    // After dropping, the ref count should be back to 1
    assert_eq!(
        Rc::strong_count(&row.dyn_values()[0].as_ref().unwrap().value),
        1
    );
}

#[test]
fn test_insert_overwrite() {
    let mut row: RLookupRow<MockRSValue> = RLookupRow::new();
    assert!(row.is_empty());
    assert_eq!(row.len(), 0);
    assert_eq!(row.num_dyn_values(), 0);

    // generate test key at index 0
    let key = RLookupKey::new(c"test", RLookupKeyFlags::empty());

    // insert a key at the first position
    let mock_to_be_overwritten = MockRSValue::create_num(42.0);
    row.write_key(&key, mock_to_be_overwritten.clone());
    assert_eq!(Rc::strong_count(&mock_to_be_overwritten.value), 2);

    assert!(!row.is_empty());
    assert_eq!(row.len(), 1);
    assert_eq!(row.num_dyn_values(), 1);
    assert_eq!(row.dyn_values()[0].as_ref().unwrap().as_num(), Some(42.0));

    // overwrite the value at the same index
    row.write_key(&key, MockRSValue::create_num(84.0));
    assert_eq!(row.num_dyn_values(), 1);
    assert_eq!(row.dyn_values()[0].as_ref().unwrap().as_num(), Some(84.0));
    // The overwritten value should have been decremented
    assert_eq!(
        Rc::strong_count(&row.dyn_values()[0].as_ref().unwrap().value),
        1
    );
}

struct WriteKeyMock<'a> {
    row: RLookupRow<'a, MockRSValue>,
    num_resize: usize,
}

impl<'a> WriteKeyMock<'a> {
    fn new() -> Self {
        Self {
            row: RLookupRow::new(),
            num_resize: 0,
        }
    }

    fn write_key(&mut self, key: &RLookupKey, val: MockRSValue) {
        if key.dstidx >= self.row.len() as u16 {
            // Simulate resizing the row's dyn_values vector
            self.num_resize += 1;
        }
        self.row.write_key(key, val);
    }
}

impl<'a> Deref for WriteKeyMock<'a> {
    type Target = RLookupRow<'a, MockRSValue>;

    fn deref(&self) -> &Self::Target {
        &self.row
    }
}

impl<'a> DerefMut for WriteKeyMock<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.row
    }
}

#[test]
fn test_wipe() {
    let mut row = WriteKeyMock::new();

    // create 10 entries in the row
    for i in 0..10 {
        let mut key = RLookupKey::new(c"test", RLookupKeyFlags::empty());
        key.dstidx = i as u16;
        row.write_key(&key, MockRSValue::create_num(i as f64 * 2.5));
    }

    assert!(!row.is_empty());
    assert_eq!(row.len(), 10);
    assert_eq!(row.num_dyn_values(), 10);
    assert_eq!(row.num_resize, 10);

    // wipe the row, we expect all values to be cleared but memory to be retained
    row.wipe();
    assert_eq!(row.num_dyn_values(), 0);
    assert_eq!(row.len(), 10);
    assert!(row.dyn_values().iter().all(|v| v.is_none()));

    // create the same 10 entries in the row
    for i in 0..10 {
        let mut key = RLookupKey::new(c"test", RLookupKeyFlags::empty());
        key.dstidx = i as u16;
        row.write_key(&key, MockRSValue::create_num(i as f64 * 2.5));
    }
    // we expect no new resizes
    assert_eq!(row.num_resize, 10);

    // and the same test as after the first insert
    assert!(!row.is_empty());
    assert_eq!(row.len(), 10);
    assert_eq!(row.num_dyn_values(), 10);
    assert_eq!(row.num_resize, 10);
}
