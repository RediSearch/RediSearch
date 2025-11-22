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
fn insert_without_gap() {
    let rlookup = RLookup::new();

    let mut row: RLookupRow<RSValueMock> = RLookupRow::new(&rlookup);
    assert!(row.is_empty());
    assert_eq!(row.len(), 0);
    assert_eq!(row.num_dyn_values(), 0);

    // generate test key at index 0
    let key = RLookupKey::new(&rlookup, c"test", RLookupKeyFlags::empty());

    // insert a key at the first position
    row.write_key(&key, RSValueMock::create_num(42.0));
    assert!(!row.is_empty());
    assert_eq!(row.len(), 1);
    assert_eq!(row.num_dyn_values(), 1);
    assert_eq!(row.dyn_values()[0].as_ref().unwrap().as_num(), Some(42.0));

    // insert a key at the second position
    let mut key = RLookupKey::new(&rlookup, c"test2", RLookupKeyFlags::empty());
    key.dstidx = 1;
    row.write_key(&key, RSValueMock::create_num(84.0));
    assert!(!row.is_empty());
    assert_eq!(row.len(), 2);
    assert_eq!(row.num_dyn_values(), 2);
    assert_eq!(row.dyn_values()[1].as_ref().unwrap().as_num(), Some(84.0));
}

#[test]
fn insert_with_gap() {
    let rlookup = RLookup::new();

    let mut row: RLookupRow<RSValueMock> = RLookupRow::new(&rlookup);
    assert!(row.is_empty());
    assert_eq!(row.len(), 0);
    assert_eq!(row.num_dyn_values(), 0);

    // generate test key at index 15
    let mut key = RLookupKey::new(&rlookup, c"test", RLookupKeyFlags::empty());
    key.dstidx = 15;
    row.write_key(&key, RSValueMock::create_num(42.0));

    assert!(!row.is_empty());
    assert_eq!(row.len(), 16); // Length should be 16 due to the gap
    assert_eq!(row.num_dyn_values(), 1);
    assert_eq!(row.dyn_values()[15].as_ref().unwrap().as_num(), Some(42.0));
}

#[test]
fn insert_non_owned() {
    let rlookup = RLookup::new();

    let mut row: RLookupRow<RSValueMock> = RLookupRow::new(&rlookup);
    assert!(row.is_empty());
    assert_eq!(row.len(), 0);
    assert_eq!(row.num_dyn_values(), 0);

    // generate test key at index 0
    let key = RLookupKey::new(&rlookup, c"test", RLookupKeyFlags::empty());

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
    let rlookup = RLookup::new();

    let mut row: RLookupRow<RSValueMock> = RLookupRow::new(&rlookup);
    assert!(row.is_empty());
    assert_eq!(row.len(), 0);
    assert_eq!(row.num_dyn_values(), 0);

    // generate test key at index 0
    let key = RLookupKey::new(&rlookup, c"test", RLookupKeyFlags::empty());

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
    fn new(rlookup: &RLookup<'_>) -> Self {
        Self {
            row: RLookupRow::new(&rlookup),
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
fn wipe() {
    let rlookup = RLookup::new();
    let mut row = WriteKeyMock::new(&rlookup);

    // create 10 entries in the row
    for i in 0..10 {
        let mut key = RLookupKey::new(&rlookup, c"test", RLookupKeyFlags::empty());
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
        let mut key = RLookupKey::new(&rlookup, c"test", RLookupKeyFlags::empty());
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
fn reset() {
    let rlookup = RLookup::new();
    let mut row = WriteKeyMock::new(&rlookup);

    // create 10 entries in the row
    for i in 0..10 {
        let mut key = RLookupKey::new(&rlookup, c"test", RLookupKeyFlags::empty());
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
        let mut key = RLookupKey::new(&rlookup, c"test", RLookupKeyFlags::empty());
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
fn get_item_dynamic_values_success() {
    let rlookup = RLookup::new();

    // Test case 1: Successfully retrieve item from dynamic values
    let mut row = RLookupRow::new(&rlookup);

    let key1 = create_test_key(&rlookup, 0, 0, RLookupKeyFlags::empty());
    let key2 = create_test_key(&rlookup, 1, 0, RLookupKeyFlags::empty());
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
fn get_item_static_values_success() {
    let rlookup = RLookup::new();

    // Test case 2: Successfully retrieve item from sorting vector
    let sv_value1 = RSValueMock::create_string("static_value_1".to_string());
    let sv_value2 = RSValueMock::create_string("static_value_2".to_string());
    let sv = RSSortingVector::from_iter([sv_value1, sv_value2]);

    let mut row: RLookupRow<'_, RSValueMock> = RLookupRow::new(&rlookup);
    row.set_sorting_vector(&sv);

    let mut flags = RLookupKeyFlags::empty();
    flags.insert(RLookupKeyFlag::SvSrc);
    let key = create_test_key(&rlookup, 0, 1, flags);

    let result = row.get(&key);
    assert!(result.is_some());
    assert_eq!(result.unwrap().as_str(), Some("static_value_2"));
}

#[test]
fn get_item_missing_svsrc_flag() {
    let rlookup = RLookup::new();

    // Test case 3: SvSrc flag missing, should return None
    let sv_value = RSValueMock::create_string("static_value".to_string());
    let sv = RSSortingVector::from_iter([sv_value]);

    let mut row = RLookupRow::new(&rlookup);
    row.set_sorting_vector(&sv);

    let key = create_test_key(&rlookup, 0, 0, RLookupKeyFlags::empty()); // No SvSrc flag

    let result = row.get(&key);
    assert!(result.is_none());
}

#[test]
fn get_item_dynamic_out_of_bounds() {
    let rlookup = RLookup::new();

    // Test case 4: Dynamic values index out of bounds
    let mut row: RLookupRow<'_, RSValueMock> = RLookupRow::new(&rlookup);
    let k1 = create_test_key(&rlookup, 0, 0, RLookupKeyFlags::empty());
    row.write_key(&k1, RSValueMock::create_string("dynamic_value".to_string()));

    let key_out_of_bounds = create_test_key(&rlookup, 5, 0, RLookupKeyFlags::empty()); // Out of bounds

    let result = row.get(&key_out_of_bounds);
    assert!(result.is_none());
}

#[test]
fn get_item_static_out_of_bounds() {
    let rlookup = RLookup::new();

    // Test case 5: Sorting vector index out of bounds
    let sv_value = RSValueMock::create_string("static_value".to_string());
    let sv = RSSortingVector::from_iter([sv_value]);

    let mut row = RLookupRow::new(&rlookup);
    row.set_sorting_vector(&sv);

    let mut flags = RLookupKeyFlags::empty();
    flags.insert(RLookupKeyFlag::SvSrc);
    let key = create_test_key(&rlookup, 0, 5, flags); // Out of bounds for sorting vector

    let result = row.get(&key);
    assert!(result.is_none());
}

#[test]
fn get_item_no_sorting_vector() {
    let rlookup = RLookup::new();

    // Test case 6: No sorting vector available
    let row: RLookupRow<'_, RSValueMock> = RLookupRow::new(&rlookup); // No sorting vector set

    let mut flags = RLookupKeyFlags::empty();
    flags.insert(RLookupKeyFlag::SvSrc);
    let key = create_test_key(&rlookup, 0, 0, flags);

    let result = row.get(&key);
    assert!(result.is_none());
}

#[test]
fn get_item_empty_dynamic_valid_static() {
    let rlookup = RLookup::new();

    // Test case 7: Empty dynamic values but valid sorting vector access
    let sv_value = RSValueMock::create_string("static_value".to_string());
    let sv = RSSortingVector::from_iter([sv_value]);

    let mut row = RLookupRow::new(&rlookup);
    // No dynamic values added
    row.set_sorting_vector(&sv);

    let mut flags = RLookupKeyFlags::empty();
    flags.insert(RLookupKeyFlag::SvSrc);
    let key = create_test_key(&rlookup, 0, 0, flags); //

    let result = row.get(&key);
    assert!(result.is_some());
    assert_eq!(result.unwrap().as_str(), Some("static_value"));
}

#[test]
fn get_item_dynamic_none_value() {
    let rlookup = RLookup::new();

    // Test case 8: Dynamic value slot contains None
    let mut row = RLookupRow::new(&rlookup);

    let k1 = create_test_key(&rlookup, 0, 0, RLookupKeyFlags::empty());
    //row.write_key(&k1,); don't write any value, so it remains None

    let k2 = create_test_key(&rlookup, 1, 0, RLookupKeyFlags::empty());
    row.write_key(&k2, RSValueMock::create_string("valid_value".to_string()));

    let result = row.get(&k1);
    assert!(result.is_none());
}

#[test]
fn get_item_priority_dynamic_over_static() {
    let rlookup = RLookup::new();

    // Test case 9: Dynamic values take priority over sorting vector
    let sv = RSSortingVector::from_iter([RSValueMock::create_string("static_value".to_string())]);
    let mut row: RLookupRow<'_, RSValueMock> = RLookupRow::new(&rlookup);
    let key = create_test_key(&rlookup, 0, 0, RLookupKeyFlags::empty());
    // Index 0 created for both
    row.write_key(
        &key,
        RSValueMock::create_string("dynamic_value".to_string()),
    );
    row.set_sorting_vector(&sv);

    let mut flags = RLookupKeyFlags::empty();
    flags.insert(RLookupKeyFlag::SvSrc);

    let key = create_test_key(&rlookup, 0, 0, flags);
    // asked for static, but dynamic should take priority
    let result = row.get(&key);
    assert!(result.is_some());
    // Should return dynamic value, not static
    assert_eq!(result.unwrap().as_str(), Some("dynamic_value"));
}

#[test]
fn write_key_by_name_new_key() {
    // Test case: name is not yet part of the lookup and gets created
    let mut lookup = RLookup::new();
    let mut row = RLookupRow::new(&lookup);

    let key_name = CString::new("new_key").unwrap();
    let value = RSValueMock::create_string("test_value".to_string());

    // Initially, row should be empty
    assert_eq!(row.len(), 0);

    // Write the key
    row.write_key_by_name(&mut lookup, key_name.to_owned(), value.clone());

    // Verify we can find the key by name
    let cursor = lookup.find_key_by_name(&key_name);
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
fn write_key_by_name_existing_key_overwrite() {
    // Test case: name is part of the lookup and its value gets overwritten
    let mut lookup = RLookup::new();
    let mut row = RLookupRow::new(&lookup);

    let key_name = CString::new("existing_key").unwrap();
    let initial_value = RSValueMock::create_string("initial_value".to_string());
    let new_value = RSValueMock::create_string("new_value".to_string());

    // Write initial value
    row.write_key_by_name(&mut lookup, key_name.to_owned(), initial_value.clone());

    // Verify initial state
    let cursor = lookup.find_key_by_name(&key_name).unwrap();
    assert!(cursor.into_current().is_some());
    assert_eq!(row.len(), 1);
    assert_eq!(
        row.dyn_values()[0].as_ref().unwrap().as_str(),
        initial_value.as_str()
    );

    // Overwrite with new value - key count should not increase
    row.write_key_by_name(&mut lookup, key_name.to_owned(), new_value.clone());

    let cursor = lookup.find_key_by_name(&key_name).unwrap();
    assert!(cursor.into_current().is_some());
    assert_eq!(row.len(), 1);
    assert_eq!(
        row.dyn_values()[0].as_ref().unwrap().as_str(),
        new_value.as_str()
    );
}

#[test]
fn write_multiple_different_keys() {
    // Test case: writing multiple different keys
    let mut lookup = RLookup::new();
    let mut row = RLookupRow::new(&lookup);

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
        let cursor = lookup.find_key_by_name(key_name);
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

fn create_test_key(
    rlookup: &RLookup<'_>,
    dstidx: u16,
    svidx: u16,
    flags: RLookupKeyFlags,
) -> RLookupKey<'static> {
    let str = format!("mock_key_{}_{}", dstidx, svidx);
    let cstring = CString::new(str).unwrap();
    let mut key = RLookupKey::new(rlookup, cstring, flags);
    key.dstidx = dstidx;
    key.svidx = svidx;

    key
}

#[test]
#[cfg_attr(debug_assertions, should_panic)]
fn row_panics_on_foreign_lookup() {
    let rlookup_a = RLookup::new();
    let rlookup_b = RLookup::new();

    let mut row: RLookupRow<'_, RSValueMock> = RLookupRow::new(&rlookup_a);

    #[cfg_attr(debug_assertions, allow(unused_mut))]
    let key = RLookupKey::new(&rlookup_b, c"foo", RLookupKeyFlags::empty());

    // This should panic, key has a different RLookupId that Row!
    row.write_key(&key, RSValueMock::create_num(42.0));
}

#[test]
fn write_fields_basic() {
    // Tests basic field writing between lookup rows
    let mut src_lookup = RLookup::new();
    let mut dst_lookup = RLookup::new();

    // Create source keys
    let src_key1_name = CString::new("field1").unwrap();
    let src_key2_name = CString::new("field2").unwrap();

    let mut src_row: RLookupRow<RSValueMock> = RLookupRow::new(&src_lookup);

    // Write values to source row
    let value1 = RSValueMock::create_num(100.0);
    let value2 = RSValueMock::create_num(200.0);

    src_row.write_key_by_name(&mut src_lookup, src_key1_name.to_owned(), value1.clone());
    src_row.write_key_by_name(&mut src_lookup, src_key2_name.to_owned(), value2.clone());

    // Add source keys to destination lookup (simulating RLookup_AddKeysFrom)
    dst_lookup.get_key_write(src_key1_name.to_owned(), RLookupKeyFlags::empty());
    dst_lookup.get_key_write(src_key2_name.to_owned(), RLookupKeyFlags::empty());

    let mut dst_row: RLookupRow<RSValueMock> = RLookupRow::new(&dst_lookup);

    // Write fields from source to destination
    dst_row.copy_fields_from(&dst_lookup, &src_row, &src_lookup);

    // Verify written values are correct and accessible by field names
    let dst_cursor1 = dst_lookup.find_key_by_name(&src_key1_name).unwrap();
    let dst_key1 = dst_cursor1.into_current().unwrap();
    let dst_cursor2 = dst_lookup.find_key_by_name(&src_key2_name).unwrap();
    let dst_key2 = dst_cursor2.into_current().unwrap();

    assert_eq!(dst_row.get(dst_key1).unwrap().as_num(), Some(100.0));
    assert_eq!(dst_row.get(dst_key2).unwrap().as_num(), Some(200.0));

    // Verify shared ownership (reference counts should be increased)
    // value1 and value2 are referenced by: the original vars + src_row + dst_row = 3 total
    assert_eq!(value1.strong_count(), 3); // value1 + src_row + dst_row
    assert_eq!(value2.strong_count(), 3); // value2 + src_row + dst_row

    // Verify source row still contains the values (shared ownership, not moved)
    let src_cursor1 = src_lookup.find_key_by_name(&src_key1_name).unwrap();
    let src_key1 = src_cursor1.into_current().unwrap();
    let src_cursor2 = src_lookup.find_key_by_name(&src_key2_name).unwrap();
    let src_key2 = src_cursor2.into_current().unwrap();

    assert_eq!(src_row.get(src_key1).unwrap().as_num(), Some(100.0));
    assert_eq!(src_row.get(src_key2).unwrap().as_num(), Some(200.0));
}

#[test]
fn write_fields_empty_source() {
    // Tests field writing when source row has no data
    let mut src_lookup = RLookup::new();
    let mut dst_lookup = RLookup::new();

    // Create keys in source lookup but don't add values to row
    let key1_name = CString::new("field1").unwrap();
    let key2_name = CString::new("field2").unwrap();

    src_lookup.get_key_write(key1_name.to_owned(), RLookupKeyFlags::empty());
    src_lookup.get_key_write(key2_name.to_owned(), RLookupKeyFlags::empty());

    // Add source keys to destination lookup
    dst_lookup.get_key_write(key1_name.to_owned(), RLookupKeyFlags::empty());
    dst_lookup.get_key_write(key2_name.to_owned(), RLookupKeyFlags::empty());

    // Create empty rows
    let src_row: RLookupRow<RSValueMock> = RLookupRow::new(&src_lookup);
    let mut dst_row: RLookupRow<RSValueMock> = RLookupRow::new(&dst_lookup);

    // Write from empty source row, will result in error
    dst_row.copy_fields_from(&dst_lookup, &src_row, &src_lookup);

    // Verify destination remains empty
    assert_eq!(dst_row.num_dyn_values(), 0);

    let dst_cursor1 = dst_lookup.find_key_by_name(&key1_name).unwrap();
    let dst_key1 = dst_cursor1.into_current().unwrap();
    let dst_cursor2 = dst_lookup.find_key_by_name(&key2_name).unwrap();
    let dst_key2 = dst_cursor2.into_current().unwrap();

    assert!(dst_row.get(dst_key1).is_none());
    assert!(dst_row.get(dst_key2).is_none());
}

#[test]
fn write_fields_different_mapping() {
    // Tests field writing between schemas with different internal indices
    let mut src_lookup = RLookup::new();
    let mut dst_lookup = RLookup::new();

    // Create source keys in specific order
    let key1_name = CString::new("field1").unwrap();
    let key2_name = CString::new("field2").unwrap();
    let key3_name = CString::new("field3").unwrap();

    let mut src_row: RLookupRow<RSValueMock> = RLookupRow::new(&src_lookup);

    // Add values to source
    let value1 = RSValueMock::create_num(111.0);
    let value2 = RSValueMock::create_num(222.0);
    let value3 = RSValueMock::create_num(333.0);

    src_row.write_key_by_name(&mut src_lookup, key1_name.to_owned(), value1.clone());
    src_row.write_key_by_name(&mut src_lookup, key2_name.to_owned(), value2.clone());
    src_row.write_key_by_name(&mut src_lookup, key3_name.to_owned(), value3.clone());

    // Create some dest keys first to ensure different indices
    let other_key_name = CString::new("other_field").unwrap();
    dst_lookup.get_key_write(other_key_name, RLookupKeyFlags::empty());

    // Add source keys to destination (they'll have different dstidx values)
    dst_lookup.get_key_write(key2_name.to_owned(), RLookupKeyFlags::empty());
    dst_lookup.get_key_write(key3_name.to_owned(), RLookupKeyFlags::empty());
    dst_lookup.get_key_write(key1_name.to_owned(), RLookupKeyFlags::empty());

    let mut dst_row: RLookupRow<RSValueMock> = RLookupRow::new(&dst_lookup);

    // Write fields
    dst_row.copy_fields_from(&dst_lookup, &src_row, &src_lookup);

    // Verify data is readable by field names despite potentially different indices
    let dst_cursor1 = dst_lookup.find_key_by_name(&key1_name).unwrap();
    let dst_key1 = dst_cursor1.into_current().unwrap();
    let dst_cursor2 = dst_lookup.find_key_by_name(&key2_name).unwrap();
    let dst_key2 = dst_cursor2.into_current().unwrap();
    let dst_cursor3 = dst_lookup.find_key_by_name(&key3_name).unwrap();
    let dst_key3 = dst_cursor3.into_current().unwrap();

    assert_eq!(dst_row.get(dst_key1).unwrap().as_num(), Some(111.0));
    assert_eq!(dst_row.get(dst_key2).unwrap().as_num(), Some(222.0));
    assert_eq!(dst_row.get(dst_key3).unwrap().as_num(), Some(333.0));

    // Verify shared ownership (same pointers)
    assert_eq!(value1.strong_count(), 3); // original + src_row + dst_row
    assert_eq!(value2.strong_count(), 3); // original + src_row + dst_row
    assert_eq!(value3.strong_count(), 3); // original + src_row + dst_row
}

#[test]
fn write_fields_multiple_sources_no_overlap() {
    // Tests copy_fields_from with distinct field sets from each source
    let mut src1_lookup = RLookup::new();
    let mut src2_lookup = RLookup::new();
    let mut dst_lookup = RLookup::new();

    // Create distinct field sets: src1["field1", "field2"], src2["field3", "field4"]
    let field1_name = CString::new("field1").unwrap();
    let field2_name = CString::new("field2").unwrap();
    let field3_name = CString::new("field3").unwrap();
    let field4_name = CString::new("field4").unwrap();

    let mut src1_row: RLookupRow<RSValueMock> = RLookupRow::new(&src1_lookup);
    let mut src2_row: RLookupRow<RSValueMock> = RLookupRow::new(&src2_lookup);

    // Create test data and populate source rows
    let value1 = RSValueMock::create_num(10.0);
    let value2 = RSValueMock::create_num(20.0);
    let value3 = RSValueMock::create_num(30.0);
    let value4 = RSValueMock::create_num(40.0);

    src1_row.write_key_by_name(&mut src1_lookup, field1_name.to_owned(), value1.clone());
    src1_row.write_key_by_name(&mut src1_lookup, field2_name.to_owned(), value2.clone());

    src2_row.write_key_by_name(&mut src2_lookup, field3_name.to_owned(), value3.clone());
    src2_row.write_key_by_name(&mut src2_lookup, field4_name.to_owned(), value4.clone());

    // Add keys from both sources to destination
    dst_lookup.get_key_write(field3_name.to_owned(), RLookupKeyFlags::empty());
    dst_lookup.get_key_write(field4_name.to_owned(), RLookupKeyFlags::empty());
    dst_lookup.get_key_write(field2_name.to_owned(), RLookupKeyFlags::empty());
    dst_lookup.get_key_write(field1_name.to_owned(), RLookupKeyFlags::empty());

    let mut dst_row: RLookupRow<RSValueMock> = RLookupRow::new(&dst_lookup);

    // Write data from both sources to single destination row
    dst_row.copy_fields_from(&dst_lookup, &src1_row, &src1_lookup);
    dst_row.copy_fields_from(&dst_lookup, &src2_row, &src2_lookup);

    // Verify all 4 fields are readable from destination using field names
    let dst_cursor1 = dst_lookup.find_key_by_name(&field1_name).unwrap();
    let dst_key1 = dst_cursor1.into_current().unwrap();
    let dst_cursor2 = dst_lookup.find_key_by_name(&field2_name).unwrap();
    let dst_key2 = dst_cursor2.into_current().unwrap();
    let dst_cursor3 = dst_lookup.find_key_by_name(&field3_name).unwrap();
    let dst_key3 = dst_cursor3.into_current().unwrap();
    let dst_cursor4 = dst_lookup.find_key_by_name(&field4_name).unwrap();
    let dst_key4 = dst_cursor4.into_current().unwrap();

    assert_eq!(dst_row.get(dst_key1).unwrap().as_num(), Some(10.0));
    assert_eq!(dst_row.get(dst_key2).unwrap().as_num(), Some(20.0));
    assert_eq!(dst_row.get(dst_key3).unwrap().as_num(), Some(30.0));
    assert_eq!(dst_row.get(dst_key4).unwrap().as_num(), Some(40.0));

    assert_eq!(dst_row.num_dyn_values(), 4);
}

#[test]
fn write_fields_multiple_sources_partial_overlap() {
    // Tests copy_fields_from with overlapping field names (last write wins)
    let mut src1_lookup = RLookup::new();
    let mut src2_lookup = RLookup::new();
    let mut dst_lookup = RLookup::new();

    // Create overlapping field sets: src1["field1", "field2", "field3"], src2["field2", "field4", "field5"]
    let field1_name = CString::new("field1").unwrap();
    let field2_name = CString::new("field2").unwrap(); // This will conflict
    let field3_name = CString::new("field3").unwrap();
    let field4_name = CString::new("field4").unwrap();
    let field5_name = CString::new("field5").unwrap();

    let mut src1_row: RLookupRow<RSValueMock> = RLookupRow::new(&src1_lookup);
    let mut src2_row: RLookupRow<RSValueMock> = RLookupRow::new(&src2_lookup);

    // Create src1 values: field1=1, field2=100, field3=3
    let s1_val1 = RSValueMock::create_num(1.0);
    let s1_val2 = RSValueMock::create_num(100.0); // Will be overwritten
    let s1_val3 = RSValueMock::create_num(3.0);

    // Create src2 values: field2=999 (conflict), field4=4, field5=5
    let s2_val2 = RSValueMock::create_num(999.0); // This should win
    let s2_val4 = RSValueMock::create_num(4.0);
    let s2_val5 = RSValueMock::create_num(5.0);

    // Write values to rows
    src1_row.write_key_by_name(&mut src1_lookup, field1_name.to_owned(), s1_val1.clone());
    src1_row.write_key_by_name(&mut src1_lookup, field2_name.to_owned(), s1_val2.clone());
    src1_row.write_key_by_name(&mut src1_lookup, field3_name.to_owned(), s1_val3.clone());

    src2_row.write_key_by_name(&mut src2_lookup, field2_name.to_owned(), s2_val2.clone());
    src2_row.write_key_by_name(&mut src2_lookup, field4_name.to_owned(), s2_val4.clone());
    src2_row.write_key_by_name(&mut src2_lookup, field5_name.to_owned(), s2_val5.clone());

    // Add keys to destination (first source wins for key creation, but last write wins for data)
    dst_lookup.get_key_write(field1_name.to_owned(), RLookupKeyFlags::empty());
    dst_lookup.get_key_write(field2_name.to_owned(), RLookupKeyFlags::empty());
    dst_lookup.get_key_write(field3_name.to_owned(), RLookupKeyFlags::empty());
    dst_lookup.get_key_write(field4_name.to_owned(), RLookupKeyFlags::empty());
    dst_lookup.get_key_write(field5_name.to_owned(), RLookupKeyFlags::empty());

    let mut dst_row: RLookupRow<RSValueMock> = RLookupRow::new(&dst_lookup);

    // Write src1 first, then src2
    dst_row.copy_fields_from(&dst_lookup, &src1_row, &src1_lookup);

    // After first write, s1_val2 should have refcount 3 (original var + src1Row + destRow)
    assert_eq!(s1_val2.strong_count(), 3); // Shared between source and destination
    assert_eq!(s2_val2.strong_count(), 2); // s2_val2 unchanged yet (original var + src2Row)

    dst_row.copy_fields_from(&dst_lookup, &src2_row, &src2_lookup);

    // After second write, s1_val2 should be decremented (overwritten in dest), s2_val2 should be shared
    assert_eq!(s1_val2.strong_count(), 2); // Back to original var + src1Row (removed from destRow)
    assert_eq!(s2_val2.strong_count(), 3); // Now shared: original var + src2Row + destRow

    // Verify field2 contains src2 data (last write wins)
    let dst_cursor2 = dst_lookup.find_key_by_name(&field2_name).unwrap();
    let dst_key2 = dst_cursor2.into_current().unwrap();
    assert_eq!(dst_row.get(dst_key2).unwrap().as_num(), Some(999.0)); // Should be 999 (src2), last write wins

    // Verify all unique fields written correctly
    let dst_cursor1 = dst_lookup.find_key_by_name(&field1_name).unwrap();
    let dst_key1 = dst_cursor1.into_current().unwrap();
    let dst_cursor4 = dst_lookup.find_key_by_name(&field4_name).unwrap();
    let dst_key4 = dst_cursor4.into_current().unwrap();

    assert_eq!(dst_row.get(dst_key1).unwrap().as_num(), Some(1.0)); // From src1
    assert_eq!(dst_row.get(dst_key4).unwrap().as_num(), Some(4.0)); // From src2
}

#[test]
fn write_fields_multiple_sources_full_overlap() {
    // Tests copy_fields_from with identical field sets (last write wins)
    let mut src1_lookup = RLookup::new();
    let mut src2_lookup = RLookup::new();
    let mut dst_lookup = RLookup::new();

    // Both sources have identical field names: ["field1", "field2", "field3"]
    let field1_name = CString::new("field1").unwrap();
    let field2_name = CString::new("field2").unwrap();
    let field3_name = CString::new("field3").unwrap();

    let mut src1_row: RLookupRow<RSValueMock> = RLookupRow::new(&src1_lookup);
    let mut src2_row: RLookupRow<RSValueMock> = RLookupRow::new(&src2_lookup);

    // Create rows with different data for same field names
    let s1_val1 = RSValueMock::create_num(100.0);
    let s1_val2 = RSValueMock::create_num(200.0);
    let s1_val3 = RSValueMock::create_num(300.0);

    let s2_val1 = RSValueMock::create_num(111.0);
    let s2_val2 = RSValueMock::create_num(222.0);
    let s2_val3 = RSValueMock::create_num(333.0);

    // Populate source rows
    src1_row.write_key_by_name(&mut src1_lookup, field1_name.to_owned(), s1_val1);
    src1_row.write_key_by_name(&mut src1_lookup, field2_name.to_owned(), s1_val2);
    src1_row.write_key_by_name(&mut src1_lookup, field3_name.to_owned(), s1_val3);

    src2_row.write_key_by_name(&mut src2_lookup, field1_name.to_owned(), s2_val1);
    src2_row.write_key_by_name(&mut src2_lookup, field2_name.to_owned(), s2_val2);
    src2_row.write_key_by_name(&mut src2_lookup, field3_name.to_owned(), s2_val3);

    // Add keys to destination
    dst_lookup.get_key_write(field2_name.to_owned(), RLookupKeyFlags::empty());
    dst_lookup.get_key_write(field1_name.to_owned(), RLookupKeyFlags::empty());
    dst_lookup.get_key_write(field3_name.to_owned(), RLookupKeyFlags::empty());

    let mut dst_row: RLookupRow<RSValueMock> = RLookupRow::new(&dst_lookup);

    // Write src1 first, then src2 - src2 should overwrite all values
    dst_row.copy_fields_from(&dst_lookup, &src1_row, &src1_lookup);
    dst_row.copy_fields_from(&dst_lookup, &src2_row, &src2_lookup);

    // Verify all fields contain src2 data (last write wins)
    let dst_cursor1 = dst_lookup.find_key_by_name(&field1_name).unwrap();
    let dst_key1 = dst_cursor1.into_current().unwrap();
    let dst_cursor2 = dst_lookup.find_key_by_name(&field2_name).unwrap();
    let dst_key2 = dst_cursor2.into_current().unwrap();
    let dst_cursor3 = dst_lookup.find_key_by_name(&field3_name).unwrap();
    let dst_key3 = dst_cursor3.into_current().unwrap();

    assert_eq!(dst_row.get(dst_key1).unwrap().as_num(), Some(111.0)); // From src2
    assert_eq!(dst_row.get(dst_key2).unwrap().as_num(), Some(222.0)); // From src2
    assert_eq!(dst_row.get(dst_key3).unwrap().as_num(), Some(333.0)); // From src2

    assert_eq!(dst_row.num_dyn_values(), 3);
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
