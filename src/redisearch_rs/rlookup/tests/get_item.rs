/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::{CStr, CString};

use rlookup::{RLookupKey, RLookupKeyFlag, RLookupKeyFlags, RLookupRow, rlookup_get_item};
use sorting_vector::RSSortingVector;
use value::RSValueTrait;

#[test]
fn test_rlookup_get_item_dynamic_values_success() {
    // Test case 1: Successfully retrieve item from dynamic values
    let mut row = RLookupRow::new();

    let key1 = create_mock_key(0, 0, RLookupKeyFlags::empty());
    let key2 = create_mock_key(1, 0, RLookupKeyFlags::empty());
    row.write_key(&key1, MockRSValue::new("dynamic_value_1"));
    row.write_key(&key2, MockRSValue::new("dynamic_value_2"));

    let result = rlookup_get_item(&key2, &row);
    assert!(result.is_some());
    assert_eq!(result.unwrap().as_str(), Some("dynamic_value_2"));
}

#[test]
fn test_rlookup_get_item_static_values_success() {
    // Test case 2: Successfully retrieve item from sorting vector
    let sv_value1 = MockRSValue::new("static_value_1");
    let sv_value2 = MockRSValue::new("static_value_2");
    let sv = create_sorting_vector(vec![sv_value1, sv_value2]);

    let mut row: RLookupRow<'_, MockRSValue> = RLookupRow::new();
    row.set_sorting_vector(&sv);

    let mut flags = RLookupKeyFlags::empty();
    flags.insert(RLookupKeyFlag::SvSrc);
    let key = create_mock_key(10, 1, flags); // Out of bounds for dynamic values

    let result = rlookup_get_item(&key, &row);
    assert!(result.is_some());
    assert_eq!(result.unwrap().as_str(), Some("static_value_2"));
}

#[test]
fn test_rlookup_get_item_missing_svsrc_flag() {
    // Test case 3: SvSrc flag missing, should return None
    let sv_value = MockRSValue::new("static_value");
    let sv = create_sorting_vector(vec![sv_value]);

    let mut row = RLookupRow::new();
    row.set_sorting_vector(&sv);

    let key = create_mock_key(10, 0, RLookupKeyFlags::empty()); // No SvSrc flag

    let result = rlookup_get_item(&key, &row);
    assert!(result.is_none());
}

#[test]
fn test_rlookup_get_item_dynamic_out_of_bounds() {
    // Test case 4: Dynamic values index out of bounds
    let mut row: RLookupRow<'_, MockRSValue> = RLookupRow::new();
    let k1 = create_mock_key(0, 0, RLookupKeyFlags::empty());
    row.write_key(&k1, MockRSValue::new("dynamic_value"));

    let key_out_of_bounce = create_mock_key(5, 0, RLookupKeyFlags::empty()); // Out of bounds

    let result = rlookup_get_item(&key_out_of_bounce, &row);
    assert!(result.is_none());
}

#[test]
fn test_rlookup_get_item_static_out_of_bounds() {
    // Test case 5: Sorting vector index out of bounds
    let sv_value = MockRSValue::new("static_value");
    let sv = create_sorting_vector(vec![sv_value]);

    let mut row = RLookupRow::new();
    row.set_sorting_vector(&sv);

    let mut flags = RLookupKeyFlags::empty();
    flags.insert(RLookupKeyFlag::SvSrc);
    let key = create_mock_key(10, 5, flags); // Out of bounds for sorting vector

    let result = rlookup_get_item(&key, &row);
    assert!(result.is_none());
}

#[test]
fn test_rlookup_get_item_no_sorting_vector() {
    // Test case 6: No sorting vector available
    let row: RLookupRow<'_, MockRSValue> = RLookupRow::new(); // No sorting vector set

    let mut flags = RLookupKeyFlags::empty();
    flags.insert(RLookupKeyFlag::SvSrc);
    let key = create_mock_key(10, 0, flags);

    let result = rlookup_get_item(&key, &row);
    assert!(result.is_none());
}

#[test]
fn test_rlookup_get_item_empty_dynamic_valid_static() {
    // Test case 7: Empty dynamic values but valid sorting vector access
    let sv_value = MockRSValue::new("static_value");
    let sv = create_sorting_vector(vec![sv_value]);

    let mut row = RLookupRow::new();
    // No dynamic values added
    row.set_sorting_vector(&sv);

    let mut flags = RLookupKeyFlags::empty();
    flags.insert(RLookupKeyFlag::SvSrc);
    let key = create_mock_key(0, 0, flags); //

    let result = rlookup_get_item(&key, &row);
    assert!(result.is_some());
    assert_eq!(result.unwrap().as_str(), Some("static_value"));
}

#[test]
fn test_rlookup_get_item_dynamic_none_value() {
    // Test case 8: Dynamic value slot contains None
    let mut row = RLookupRow::new();

    let k1 = create_mock_key(0, 0, RLookupKeyFlags::empty());
    //row.write_key(&k1,); don't write any value, so it remains None

    let k2 = create_mock_key(1, 0, RLookupKeyFlags::empty());
    row.write_key(&k2, MockRSValue::new("valid_value"));

    let result = rlookup_get_item(&k1, &row);
    assert!(result.is_none());
}

#[test]
fn test_rlookup_get_item_priority_dynamic_over_static() {
    // Test case 9: Dynamic values take priority over sorting vector
    let sv = create_sorting_vector(vec![MockRSValue::new("static_value")]);
    let mut row: RLookupRow<'_, MockRSValue> = RLookupRow::new();
    let creation_flags = RLookupKeyFlags::empty();
    let key = create_mock_key(0, 0, creation_flags);
    // Index 0 created for both
    row.write_key(&key, MockRSValue::new("dynamic_value"));
    row.set_sorting_vector(&sv);

    let mut flags = RLookupKeyFlags::empty();
    flags.insert(RLookupKeyFlag::SvSrc);

    let key = create_mock_key(0, 0, flags);
    // asked for static, but dynamic should take priority
    let result = rlookup_get_item(&key, &row);
    assert!(result.is_some());
    // Should return dynamic value, not static
    assert_eq!(result.unwrap().as_str(), Some("dynamic_value"));
}

#[derive(Clone, Debug, PartialEq)]
pub struct MockRSValue {
    data: String,
}

impl MockRSValue {
    pub fn new(data: &str) -> Self {
        Self {
            data: data.to_string(),
        }
    }
}

impl RSValueTrait for MockRSValue {
    fn create_null() -> Self {
        Self {
            data: String::new(),
        }
    }

    fn create_string(s: String) -> Self {
        Self { data: s }
    }

    fn create_num(num: f64) -> Self {
        Self {
            data: num.to_string(),
        }
    }

    fn create_ref(value: Self) -> Self {
        value
    }

    fn is_null(&self) -> bool {
        self.data.is_empty()
    }

    fn get_ref(&self) -> Option<&Self> {
        Some(self)
    }

    fn get_ref_mut(&mut self) -> Option<&mut Self> {
        Some(self)
    }

    fn as_str(&self) -> Option<&str> {
        Some(&self.data)
    }

    fn as_num(&self) -> Option<f64> {
        self.data.parse().ok()
    }

    fn get_type(&self) -> ffi::RSValueType {
        ffi::RSValueType_RSValue_String
    }

    fn is_ptr_type() -> bool {
        false
    }

    fn increment(&mut self) {
        // Mock implementation - no-op
    }

    fn decrement(&mut self) {
        // Mock implementation - no-op
    }
}

fn create_sorting_vector(values: Vec<MockRSValue>) -> RSSortingVector<MockRSValue> {
    let mut sv = RSSortingVector::new(values.len());
    for (i, value) in values.into_iter().enumerate() {
        sv.try_insert_val(i, value).unwrap();
    }
    sv
}

fn create_mock_key(dstidx: u16, svidx: u16, flags: RLookupKeyFlags) -> RLookupKey<'static> {
    let str = format!("mock_key_{}_{}", dstidx, svidx);
    let cstring = CString::new(str).unwrap();
    let cstr: &'static CStr = Box::leak(cstring.into_boxed_c_str());

    let mut key = RLookupKey::new(cstr, flags);
    key.dstidx = dstidx;
    key.svidx = svidx;

    key
}
