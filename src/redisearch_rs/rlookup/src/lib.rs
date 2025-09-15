/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod bindings;
mod lookup;
#[cfg(test)]
mod mock;
mod row;

// Re-export test utilities for usage in test binaries
pub mod test_utils;

use std::ffi::CStr;

pub use bindings::IndexSpecCache;
pub use lookup::{
    RLookup, RLookupKey, RLookupKeyFlag, RLookupKeyFlags, RLookupOption, RLookupOptions,
};
pub use row::RLookupRow;
use value::RSValueTrait;

/// Retrieves an item from the given `RLookupRow` based on the provided `RLookupKey`.
/// The function first checks for dynamic values, and if not found, it checks the sorting vector
/// if the `SvSrc` flag is set in the key.
/// If the item is not found in either location, it returns `None`.
///
/// # Lifetime
/// The returned reference is tied to the lifetime of the input `RLookupRow`.
pub fn rlookup_get_item<'a>(
    key: &RLookupKey,
    row: &'a RLookupRow<impl RSValueTrait>,
) -> Option<&'a impl RSValueTrait> {
    // 1. Check dynamic values first
    if row.len() > key.dstidx as usize {
        return row.dyn_values()[key.dstidx as usize].as_ref();
    }

    // 2. If not found in dynamic values, check the sorting vector if the SvSrc flag is set
    if key.flags.contains(RLookupKeyFlag::SvSrc) {
        let sv = row.sorting_vector()?;
        sv.get(key.svidx as usize)
    } else {
        None
    }
}

pub fn rlookup_write_key_by_name<T: RSValueTrait>(
    lookup: &mut RLookup,
    name: &CStr,
    row: &mut RLookupRow<T>,
    value: T,
) {
    if let Some(cursor) = lookup.keys.find_by_name(name) {
        let key = cursor.into_current().unwrap();
        row.write_key(key, value);
    } else {
        let pinned_key = lookup.keys.push(RLookupKey::new_owned(
            name.to_owned(),
            RLookupKeyFlags::empty(),
        ));
        let key = &pinned_key.as_ref();
        row.write_key(key, value);
    }
}

#[cfg(test)]
mod tests {

    use std::ffi::CString;

    use super::test_utils::*;

    use super::{
        RLookup, RLookupKeyFlag, RLookupKeyFlags, RLookupRow, rlookup_get_item,
        rlookup_write_key_by_name,
    };
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

        let result = rlookup_get_item(&key1, &row);
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_str(), Some("dynamic_value_1"));
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
        let key = create_mock_key(0, 1, flags);

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

        let key = create_mock_key(0, 0, RLookupKeyFlags::empty()); // No SvSrc flag

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
        let key = create_mock_key(0, 5, flags); // Out of bounds for sorting vector

        let result = rlookup_get_item(&key, &row);
        assert!(result.is_none());
    }

    #[test]
    fn test_rlookup_get_item_no_sorting_vector() {
        // Test case 6: No sorting vector available
        let row: RLookupRow<'_, MockRSValue> = RLookupRow::new(); // No sorting vector set

        let mut flags = RLookupKeyFlags::empty();
        flags.insert(RLookupKeyFlag::SvSrc);
        let key = create_mock_key(0, 0, flags);

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
        let key = create_mock_key(0, 0, RLookupKeyFlags::empty());
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
    #[test]
    fn test_write_key_by_name_new_key() {
        // Test case: name is not yet part of the lookup and gets created
        let mut lookup = RLookup::new();
        let mut row = RLookupRow::new();

        let key_name = CString::new("new_key").unwrap();
        let value = MockRSValue::new("test_value");

        // Initially, row should be empty
        assert_eq!(row.len(), 0);

        // Write the key
        rlookup_write_key_by_name(&mut lookup, &key_name, &mut row, value.clone());

        // Verify we can find the key by name
        let cursor = lookup.keys.find_by_name(&key_name);
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
        let initial_value = MockRSValue::new("initial_value");
        let new_value = MockRSValue::new("new_value");

        // Write initial value
        rlookup_write_key_by_name(&mut lookup, &key_name, &mut row, initial_value.clone());

        // Verify initial state
        let cursor = lookup.keys.find_by_name(&key_name).unwrap();
        assert!(cursor.into_current().is_some());
        assert_eq!(row.len(), 1);
        assert_eq!(
            row.dyn_values()[0].as_ref().unwrap().as_str(),
            initial_value.as_str()
        );

        // Overwrite with new value - key count should not increase
        rlookup_write_key_by_name(&mut lookup, &key_name, &mut row, new_value.clone());

        let cursor = lookup.keys.find_by_name(&key_name).unwrap();
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

        let value1 = MockRSValue::new("value1");
        let value2 = MockRSValue::new("value2");
        let value3 = MockRSValue::new("value3");

        // Write multiple keys
        rlookup_write_key_by_name(&mut lookup, &key1_name, &mut row, value1.clone());
        rlookup_write_key_by_name(&mut lookup, &key2_name, &mut row, value2.clone());
        rlookup_write_key_by_name(&mut lookup, &key3_name, &mut row, value3.clone());

        // Verify all keys were added
        assert_eq!(row.len(), 3);

        for (key_name, value) in [
            (&key1_name, value1),
            (&key2_name, value2),
            (&key3_name, value3),
        ] {
            let cursor = lookup.keys.find_by_name(key_name);
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

    #[test]
    fn test_write_key_with_empty_name() {
        // Test case: writing a key with empty name, which is valid in Redis
        let mut lookup = RLookup::new();
        let mut row = RLookupRow::new();

        let empty_key_name = CString::new("").unwrap();
        let value = MockRSValue::new("empty_key_value");

        rlookup_write_key_by_name(&mut lookup, &empty_key_name, &mut row, value.clone());

        // Verify value can be retrieved
        let cursor = lookup.keys.find_by_name(empty_key_name.as_c_str());
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

    #[test]
    fn test_write_key_with_unicode() {
        // Test case: writing keys with Unicode characters, as every byte sequence is valid in Redis
        let mut lookup = RLookup::new();
        let mut row = RLookupRow::new();

        let unicode_key = CString::new("🔍_test_🎯").unwrap();
        let unicode_value = MockRSValue::new("🌟_unicode_value_🌈");

        rlookup_write_key_by_name(&mut lookup, &unicode_key, &mut row, unicode_value.clone());

        // Verify value can be retrieved
        let cursor = lookup.keys.find_by_name(&unicode_key).unwrap();
        let key = cursor.into_current().unwrap();
        assert_eq!(
            row.dyn_values()[key.dstidx as usize]
                .as_ref()
                .unwrap()
                .as_str(),
            unicode_value.as_str()
        );
    }
}
