/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rlookup::{RLookup, RLookupKey, RLookupKeyFlag, RLookupKeyFlags, RLookupRow};
use sorting_vector::RSSortingVector;
use std::{
    ffi::CString,
    mem::offset_of,
    ops::{Deref, DerefMut},
    ptr::NonNull,
    sync::atomic::{AtomicUsize, Ordering},
};
use value::{RSValueMock, RSValueTrait};

#[test]
fn test_insert_without_gap() {
    let mut row: RLookupRow<RSValueMock> = RLookupRow::new();

    assert!(row.is_empty());
    assert_eq!(row.len(), 0);
    assert_eq!(row.num_dyn_values(), 0);

    // generate test key at index 0
    let key = RLookupKey::new(c"test", RLookupKeyFlags::empty());

    // insert a key at the first position
    row.write_key(&key, RSValueMock::create_num(42.0));
    assert!(!row.is_empty());
    assert_eq!(row.len(), 1);
    assert_eq!(row.num_dyn_values(), 1);
    assert_eq!(row.dyn_values()[0].as_ref().unwrap().as_num(), Some(42.0));

    // insert a key at the second position
    let mut key = RLookupKey::new(c"test2", RLookupKeyFlags::empty());
    key.dstidx = 1;
    row.write_key(&key, RSValueMock::create_num(84.0));
    assert!(!row.is_empty());
    assert_eq!(row.len(), 2);
    assert_eq!(row.num_dyn_values(), 2);
    assert_eq!(row.dyn_values()[1].as_ref().unwrap().as_num(), Some(84.0));
}

#[test]
fn test_insert_with_gap() {
    let mut row: RLookupRow<RSValueMock> = RLookupRow::new();
    assert!(row.is_empty());
    assert_eq!(row.len(), 0);
    assert_eq!(row.num_dyn_values(), 0);

    // generate test key at index 15
    let mut key = RLookupKey::new(c"test", RLookupKeyFlags::empty());
    key.dstidx = 15;
    row.write_key(&key, RSValueMock::create_num(42.0));

    assert!(!row.is_empty());
    assert_eq!(row.len(), 16); // Length should be 16 due to the gap
    assert_eq!(row.num_dyn_values(), 1);
    assert_eq!(row.dyn_values()[15].as_ref().unwrap().as_num(), Some(42.0));
}

#[test]
fn test_insert_non_owned() {
    let mut row: RLookupRow<RSValueMock> = RLookupRow::new();
    assert!(row.is_empty());
    assert_eq!(row.len(), 0);
    assert_eq!(row.num_dyn_values(), 0);

    // generate test key at index 0
    let key = RLookupKey::new(c"test", RLookupKeyFlags::empty());

    // insert a key at the first position
    let mock = RSValueMock::create_num(42.0);
    row.write_key(&key, mock.clone());

    // We have the key outside of the row, so it should have a ref count of 2
    assert_eq!(row.dyn_values()[0].as_ref().unwrap().strong_count(), 2);

    drop(mock);
    // After dropping, the ref count should be back to 1
    assert_eq!(row.dyn_values()[0].as_ref().unwrap().strong_count(), 1);
}

#[test]
fn insert_overwrite() {
    let mut row: RLookupRow<RSValueMock> = RLookupRow::new();
    assert!(row.is_empty());
    assert_eq!(row.len(), 0);
    assert_eq!(row.num_dyn_values(), 0);

    // generate test key at index 0
    let key = RLookupKey::new(c"test", RLookupKeyFlags::empty());

    // insert a key at the first position
    let mock_to_be_overwritten = RSValueMock::create_num(42.0);

    let prev = row.write_key(&key, mock_to_be_overwritten.clone());
    assert!(prev.is_none());
    assert_eq!(mock_to_be_overwritten.strong_count(), 2);

    assert!(!row.is_empty());
    assert_eq!(row.len(), 1);
    assert_eq!(row.num_dyn_values(), 1);
    assert_eq!(row.dyn_values()[0].as_ref().unwrap().as_num(), Some(42.0));

    // overwrite the value at the same index
    let prev = row.write_key(&key, RSValueMock::create_num(84.0));
    assert!(prev.is_some());

    assert_eq!(row.num_dyn_values(), 1);
    assert_eq!(row.dyn_values()[0].as_ref().unwrap().as_num(), Some(84.0));
    // The overwritten value should have been decremented
    assert_eq!(row.dyn_values()[0].as_ref().unwrap().strong_count(), 1);
    assert_eq!(mock_to_be_overwritten.strong_count(), 2); // we have both mock_to_be_overwritten and prev
}

struct WriteKeyMock<'a> {
    row: RLookupRow<'a, RSValueMock>,
    num_resize: usize,
}

impl<'a> WriteKeyMock<'a> {
    fn new() -> Self {
        Self {
            row: RLookupRow::new(),
            num_resize: 0,
        }
    }

    fn write_key(&mut self, key: &RLookupKey, val: RSValueMock) {
        if key.dstidx >= self.row.len() as u16 {
            // Simulate resizing the row's dyn_values vector
            self.num_resize += 1;
        }
        self.row.write_key(key, val);
    }
}

impl<'a> Deref for WriteKeyMock<'a> {
    type Target = RLookupRow<'a, RSValueMock>;

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
        row.write_key(&key, RSValueMock::create_num(i as f64 * 2.5));
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
        row.write_key(&key, RSValueMock::create_num(i as f64 * 2.5));
    }
    // we expect no new resizes
    assert_eq!(row.num_resize, 10);

    // and the same test as after the first insert
    assert!(!row.is_empty());
    assert_eq!(row.len(), 10);
    assert_eq!(row.num_dyn_values(), 10);
    assert_eq!(row.num_resize, 10);
}

#[test]
fn test_reset() {
    let mut row = WriteKeyMock::new();

    // create 10 entries in the row
    for i in 0..10 {
        let mut key = RLookupKey::new(c"test", RLookupKeyFlags::empty());
        key.dstidx = i as u16;
        row.write_key(&key, RSValueMock::create_num(i as f64 * 2.5));
    }

    assert!(!row.is_empty());
    assert_eq!(row.len(), 10);
    assert_eq!(row.num_dyn_values(), 10);
    assert_eq!(row.num_resize, 10);

    // wipe the row, we expect all values to be cleared but memory to be retained
    row.reset_dyn_values();
    assert_eq!(row.num_dyn_values(), 0);
    assert_eq!(row.len(), 0);
    assert!(row.dyn_values().iter().all(|v| v.is_none()));

    // create the same 10 entries in the row
    for i in 0..10 {
        let mut key = RLookupKey::new(c"test", RLookupKeyFlags::empty());
        key.dstidx = i as u16;
        row.write_key(&key, RSValueMock::create_num(i as f64 * 2.5));
    }
    // we expect new resizes because the vector was replaced with a new allocation
    assert_eq!(row.num_resize, 20);

    // and the same test as after the first insert
    assert!(!row.is_empty());
    assert_eq!(row.len(), 10);
    assert_eq!(row.num_dyn_values(), 10);
    assert_eq!(row.num_resize, 20);
}

#[test]
fn test_rlookup_get_item_dynamic_values_success() {
    // Test case 1: Successfully retrieve item from dynamic values
    let mut row = RLookupRow::new();

    let key1 = create_test_key(0, 0, RLookupKeyFlags::empty());
    let key2 = create_test_key(1, 0, RLookupKeyFlags::empty());
    row.write_key(
        &key1,
        RSValueMock::create_string("dynamic_value_1".to_string()),
    );
    row.write_key(
        &key2,
        RSValueMock::create_string("dynamic_value_2".to_string()),
    );

    let result = row.get(&key2);
    assert!(result.is_some());
    assert_eq!(result.unwrap().as_str(), Some("dynamic_value_2"));

    let result = row.get(&key1);
    assert!(result.is_some());
    assert_eq!(result.unwrap().as_str(), Some("dynamic_value_1"));
}

#[test]
fn test_rlookup_get_item_static_values_success() {
    // Test case 2: Successfully retrieve item from sorting vector
    let sv_value1 = RSValueMock::create_string("static_value_1".to_string());
    let sv_value2 = RSValueMock::create_string("static_value_2".to_string());
    let sv = RSSortingVector::from_iter([sv_value1, sv_value2]);

    let mut row: RLookupRow<'_, RSValueMock> = RLookupRow::new();
    row.set_sorting_vector(&sv);

    let mut flags = RLookupKeyFlags::empty();
    flags.insert(RLookupKeyFlag::SvSrc);
    let key = create_test_key(0, 1, flags);

    let result = row.get(&key);
    assert!(result.is_some());
    assert_eq!(result.unwrap().as_str(), Some("static_value_2"));
}

#[test]
fn test_rlookup_get_item_missing_svsrc_flag() {
    // Test case 3: SvSrc flag missing, should return None
    let sv_value = RSValueMock::create_string("static_value".to_string());
    let sv = RSSortingVector::from_iter([sv_value]);

    let mut row = RLookupRow::new();
    row.set_sorting_vector(&sv);

    let key = create_test_key(0, 0, RLookupKeyFlags::empty()); // No SvSrc flag

    let result = row.get(&key);
    assert!(result.is_none());
}

#[test]
fn test_rlookup_get_item_dynamic_out_of_bounds() {
    // Test case 4: Dynamic values index out of bounds
    let mut row: RLookupRow<'_, RSValueMock> = RLookupRow::new();
    let k1 = create_test_key(0, 0, RLookupKeyFlags::empty());
    row.write_key(&k1, RSValueMock::create_string("dynamic_value".to_string()));

    let key_out_of_bounds = create_test_key(5, 0, RLookupKeyFlags::empty()); // Out of bounds

    let result = row.get(&key_out_of_bounds);
    assert!(result.is_none());
}

#[test]
fn test_rlookup_get_item_static_out_of_bounds() {
    // Test case 5: Sorting vector index out of bounds
    let sv_value = RSValueMock::create_string("static_value".to_string());
    let sv = RSSortingVector::from_iter([sv_value]);

    let mut row = RLookupRow::new();
    row.set_sorting_vector(&sv);

    let mut flags = RLookupKeyFlags::empty();
    flags.insert(RLookupKeyFlag::SvSrc);
    let key = create_test_key(0, 5, flags); // Out of bounds for sorting vector

    let result = row.get(&key);
    assert!(result.is_none());
}

#[test]
fn test_rlookup_get_item_no_sorting_vector() {
    // Test case 6: No sorting vector available
    let row: RLookupRow<'_, RSValueMock> = RLookupRow::new(); // No sorting vector set

    let mut flags = RLookupKeyFlags::empty();
    flags.insert(RLookupKeyFlag::SvSrc);
    let key = create_test_key(0, 0, flags);

    let result = row.get(&key);
    assert!(result.is_none());
}

#[test]
fn test_rlookup_get_item_empty_dynamic_valid_static() {
    // Test case 7: Empty dynamic values but valid sorting vector access
    let sv_value = RSValueMock::create_string("static_value".to_string());
    let sv = RSSortingVector::from_iter([sv_value]);

    let mut row = RLookupRow::new();
    // No dynamic values added
    row.set_sorting_vector(&sv);

    let mut flags = RLookupKeyFlags::empty();
    flags.insert(RLookupKeyFlag::SvSrc);
    let key = create_test_key(0, 0, flags); //

    let result = row.get(&key);
    assert!(result.is_some());
    assert_eq!(result.unwrap().as_str(), Some("static_value"));
}

#[test]
fn test_rlookup_get_item_dynamic_none_value() {
    // Test case 8: Dynamic value slot contains None
    let mut row = RLookupRow::new();

    let k1 = create_test_key(0, 0, RLookupKeyFlags::empty());
    //row.write_key(&k1,); don't write any value, so it remains None

    let k2 = create_test_key(1, 0, RLookupKeyFlags::empty());
    row.write_key(&k2, RSValueMock::create_string("valid_value".to_string()));

    let result = row.get(&k1);
    assert!(result.is_none());
}

#[test]
fn test_rlookup_get_item_priority_dynamic_over_static() {
    // Test case 9: Dynamic values take priority over sorting vector
    let sv = RSSortingVector::from_iter([RSValueMock::create_string("static_value".to_string())]);
    let mut row: RLookupRow<'_, RSValueMock> = RLookupRow::new();
    let key = create_test_key(0, 0, RLookupKeyFlags::empty());
    // Index 0 created for both
    row.write_key(
        &key,
        RSValueMock::create_string("dynamic_value".to_string()),
    );
    row.set_sorting_vector(&sv);

    let mut flags = RLookupKeyFlags::empty();
    flags.insert(RLookupKeyFlag::SvSrc);

    let key = create_test_key(0, 0, flags);
    // asked for static, but dynamic should take priority
    let result = row.get(&key);
    assert!(result.is_some());
    // Should return dynamic value, not static
    assert_eq!(result.unwrap().as_str(), Some("dynamic_value"));
}

#[test]
fn test_write_key_by_name_new_key() {
    // Test case: name is not yet part of the lookup and gets created
    let mut lookup = RLookup::new();
    let mut row = RLookupRow::new();

    let key_name = CString::new("new_key").unwrap();
    let value = RSValueMock::create_string("test_value".to_string());

    // Initially, row should be empty
    assert_eq!(row.len(), 0);

    // Write the key
    row.write_key_by_name(&mut lookup, key_name.to_owned(), value.clone());

    // Verify we can find the key by name
    let cursor = lookup.find_by_name(&key_name);
    assert!(cursor.is_some());

    // Verify the rlookup row is in correct state
    assert_eq!(row.len(), 1);
    assert!(row.dyn_values()[0].is_some());
    assert_eq!(
        row.dyn_values()[0].as_ref().unwrap().as_str(),
        Some("test_value")
    );
}

#[test]
fn test_write_key_by_name_existing_key_overwrite() {
    // Test case: name is part of the lookup and its value gets overwritten
    let mut lookup = RLookup::new();
    let mut row = RLookupRow::new();

    let key_name = CString::new("existing_key").unwrap();
    let initial_value = RSValueMock::create_string("initial_value".to_string());
    let new_value = RSValueMock::create_string("new_value".to_string());

    // Write initial value
    row.write_key_by_name(&mut lookup, key_name.to_owned(), initial_value.clone());

    // Verify initial state
    let cursor = lookup.find_by_name(&key_name).unwrap();
    assert!(cursor.into_current().is_some());
    assert_eq!(row.len(), 1);
    assert_eq!(
        row.dyn_values()[0].as_ref().unwrap().as_str(),
        initial_value.as_str()
    );

    // Overwrite with new value - key count should not increase
    row.write_key_by_name(&mut lookup, key_name.to_owned(), new_value.clone());

    let cursor = lookup.find_by_name(&key_name).unwrap();
    assert!(cursor.into_current().is_some());
    assert_eq!(row.len(), 1);
    assert_eq!(
        row.dyn_values()[0].as_ref().unwrap().as_str(),
        new_value.as_str()
    );
}

#[test]
fn test_write_multiple_different_keys() {
    // Test case: writing multiple different keys
    let mut lookup = RLookup::new();
    let mut row = RLookupRow::new();

    let key1_name = CString::new("key1").unwrap();
    let key2_name = CString::new("key2").unwrap();
    let key3_name = CString::new("key3").unwrap();

    let value1 = RSValueMock::create_string("value1".to_string());
    let value2 = RSValueMock::create_string("value2".to_string());
    let value3 = RSValueMock::create_string("value3".to_string());

    // Write multiple keys
    row.write_key_by_name(&mut lookup, key1_name.to_owned(), value1.clone());
    row.write_key_by_name(&mut lookup, key2_name.to_owned(), value2.clone());
    row.write_key_by_name(&mut lookup, key3_name.to_owned(), value3.clone());

    // Verify all keys were added
    assert_eq!(row.len(), 3);

    for (key_name, value) in [
        (&key1_name, value1),
        (&key2_name, value2),
        (&key3_name, value3),
    ] {
        let cursor = lookup.find_by_name(key_name);
        let key = cursor.unwrap().into_current().unwrap();
        assert!(row.dyn_values()[key.dstidx as usize].is_some());
        assert_eq!(
            row.dyn_values()[key.dstidx as usize]
                .as_ref()
                .unwrap()
                .as_str(),
            value.as_str(),
        );
    }
}

/// Mock implementation of `IndexSpecCache_Decref` from spec.h for testing purposes
#[unsafe(no_mangle)]
extern "C" fn IndexSpecCache_Decref(spcache: Option<NonNull<ffi::IndexSpecCache>>) {
    let spcache = spcache.expect("`spcache` must not be null");
    let refcount = unsafe {
        spcache
            .byte_add(offset_of!(ffi::IndexSpecCache, refcount))
            .cast::<usize>()
    };

    let refcount = unsafe { AtomicUsize::from_ptr(refcount.as_ptr()) };

    if refcount.fetch_sub(1, Ordering::Relaxed) == 1 {
        drop(unsafe { Box::from_raw(spcache.as_ptr()) });
    }
}

fn create_test_key(dstidx: u16, svidx: u16, flags: RLookupKeyFlags) -> RLookupKey<'static> {
    let str = format!("mock_key_{}_{}", dstidx, svidx);
    let cstring = CString::new(str).unwrap();
    let mut key = RLookupKey::new(cstring, flags);
    key.dstidx = dstidx;
    key.svidx = svidx;

    key
}
