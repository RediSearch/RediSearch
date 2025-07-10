use std::rc::Rc;

use rlookup::{RLookupKey, RLookupKeyFlags, row::RLookupRow};
use value::RSValueTrait;

/// Mock implementation of RSValueTrait for testing purposes
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
        todo!()
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
    let mut row: RLookupRow<MockRSValue> = RLookupRow::new(10);

    assert!(row.is_empty());
    assert_eq!(row.len(), 0);
    assert_eq!(row.num(), 0);

    // generate test key at index 0
    let key = RLookupKey::new(c"test", RLookupKeyFlags::empty());

    // insert a key at the first position
    row.write_own_key(&key, MockRSValue::create_num(42.0));
    assert!(!row.is_empty());
    assert_eq!(row.len(), 1);
    assert_eq!(row.num(), 1);
    assert_eq!(row.values()[0].as_ref().unwrap().as_num(), Some(42.0));

    // insert a key at the second position
    let mut key = RLookupKey::new(c"test2", RLookupKeyFlags::empty());
    key.dstidx = 1;
    row.write_own_key(&key, MockRSValue::create_num(84.0));
    assert!(!row.is_empty());
    assert_eq!(row.len(), 2);
    assert_eq!(row.num(), 2);
    assert_eq!(row.values()[1].as_ref().unwrap().as_num(), Some(84.0));
}

#[test]
fn test_insert_with_gap() {
    let mut row: RLookupRow<MockRSValue> = RLookupRow::new(10);
    assert!(row.is_empty());
    assert_eq!(row.len(), 0);
    assert_eq!(row.num(), 0);

    // generate test key at index 15
    let mut key = RLookupKey::new(c"test", RLookupKeyFlags::empty());
    key.dstidx = 15;
    row.write_own_key(&key, MockRSValue::create_num(42.0));

    assert!(!row.is_empty());
    assert_eq!(row.len(), 16); // Length should be 16 due to the gap
    assert_eq!(row.num(), 1);
    assert_eq!(row.values()[15].as_ref().unwrap().as_num(), Some(42.0));
}

#[test]
fn test_insert_non_owned() {
    let mut row: RLookupRow<MockRSValue> = RLookupRow::new(10);
    assert!(row.is_empty());
    assert_eq!(row.len(), 0);
    assert_eq!(row.num(), 0);

    // generate test key at index 0
    let key = RLookupKey::new(c"test", RLookupKeyFlags::empty());

    // insert a key at the first position
    let mut mock = MockRSValue::create_num(42.0);
    row.write_key(&key, &mut mock);

    // We have the key outside of the row, so it should have a ref count of 2
    assert_eq!(
        Rc::strong_count(&row.values()[0].as_ref().unwrap().value),
        2
    );

    drop(mock);
    // After dropping, the ref count should be back to 1
    assert_eq!(
        Rc::strong_count(&row.values()[0].as_ref().unwrap().value),
        1
    );
}
