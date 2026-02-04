/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::util::expect_value;
use libc::size_t;
use std::mem::MaybeUninit;
use value::{Map, RsValue, SharedRsValue};

/// Opaque map structure used during map construction.
/// Holds uninitialized entries that are populated via `RSValueMap_SetEntry`
/// before being finalized into an `RsValue::Map` via `RSValue_NewMap`.
pub struct RSValueMap {
    entries: Vec<MaybeUninit<(*mut RsValue, *mut RsValue)>>,
}

/// Allocates a new, uninitialized [`RSValueMap`] with space for `len` entries.
///
/// The map entries are uninitialized and must be set using [`RSValueMap_SetEntry`]
/// before being finalized into an [`RsValue`] via [`RSValue_NewMap`].
///
/// # SAFETY
///
/// 1. All entries must be initialized via [`RSValueMap_SetEntry`] before
///    passing the map to [`RSValue_NewMap`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValueMap_AllocUninit(len: u32) -> *mut RSValueMap {
    let entries = vec![MaybeUninit::uninit(); len as usize];

    Box::into_raw(Box::new(RSValueMap { entries }))
}

/// Sets a key-value pair at a specific index in the map.
///
/// Takes ownership of both the `key` and `value` [`RsValue`] pointers.
///
/// # SAFETY
///
/// 1. `map` must be a valid pointer to an [`RSValueMap`] created by
///    [`RSValueMap_AllocUninit`].
/// 2. `index` must be less than the map length.
/// 3. `key` and `value` must be valid pointers to [`RsValue`]
///    obtained from an `RSValue_*` function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValueMap_SetEntry(
    map: *mut RSValueMap,
    index: size_t,
    key: *mut RsValue,
    value: *mut RsValue,
) {
    // Safety: ensured by caller (1.)
    let map = unsafe { map.as_mut().expect("map should not be null") };

    // Compatibility: C does an RS_ASSERT on index out of bounds
    map.entries[index] = MaybeUninit::new((key, value));
}

/// Creates a heap-allocated map [`RsValue`] from an [`RSValueMap`].
///
/// Takes ownership of the map structure and all its entries. The [`RSValueMap`]
/// pointer is consumed and must not be used after this call.
///
/// # SAFETY
///
/// 1. `map` must be a valid pointer to an [`RSValueMap`] created by
///    [`RSValueMap_AllocUninit`].
/// 2. All entries in the map must have been initialized via [`RSValueMap_SetEntry`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewMap(map: *mut RSValueMap) -> *mut RsValue {
    // Safety: ensured by caller (1.)
    let map = unsafe { Box::from_raw(map) };

    let map = map
        .entries
        .into_iter()
        .map(|entry| {
            // Safety: ensured by caller (2.)
            let (key, value) = unsafe { entry.assume_init() };
            // Safety: ensured by caller (2.)
            let key = unsafe { SharedRsValue::from_raw(key) };
            // Safety: ensured by caller (2.)
            let value = unsafe { SharedRsValue::from_raw(value) };
            (key, value)
        })
        .collect();

    let value = RsValue::Map(Map::new(map));
    let shared = SharedRsValue::new(value);
    shared.into_raw() as *mut _
}

/// Returns the number of key-value pairs in a map [`RsValue`].
///
/// # SAFETY
///
/// 1. `map` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
///
/// # Panics
///
/// Panics if `map` is not a map value.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Map_Len(map: *const RsValue) -> u32 {
    // Safety: ensured by caller (1.)
    let map = unsafe { expect_value(map) };

    if let RsValue::Map(map) = map {
        map.len_u32()
    } else {
        // Compatibility: C does an RS_ASSERT on incorrect type
        panic!("Expected 'Map' type, got '{}'", map.variant_name())
    }
}

/// Retrieves a key-value pair from a map [`RsValue`] at a specific index.
///
/// The returned key and value pointers are borrowed from the map and must
/// not be freed by the caller.
///
/// # SAFETY
///
/// 1. `map` must point to a valid [`RsValue`] obtained from an `RSValue_*` function.
/// 2. `index` must be less than the map length.
/// 3. `key` and `value` must be valid, non-null pointers to writable
///    `*mut RsValue` locations.
///
/// # Panics
///
/// Panics if `map` is not a map value.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Map_GetEntry(
    map: *const RsValue,
    index: u32,
    key: *mut *mut RsValue,
    value: *mut *mut RsValue,
) {
    // Safety: ensured by caller (1.)
    let map = unsafe { expect_value(map) };

    if let RsValue::Map(map) = map {
        // Compatibility: C does an RS_ASSERT on index out of bounds
        let (shared_key, shared_value) = &map[index as usize];
        // Safety: ensured by caller (3.)
        unsafe { key.write(shared_key.as_ptr() as *mut _) };
        // Safety: ensured by caller (3.)
        unsafe { value.write(shared_value.as_ptr() as *mut _) };
    } else {
        panic!("Expected 'Map' type, got '{}'", map.variant_name())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::constructors::RSValue_NewNumber;
    use crate::getters::RSValue_Number_Get;
    use crate::shared::RSValue_DecrRef;

    #[test]
    fn test_map() {
        unsafe {
            let map = RSValueMap_AllocUninit(2);
            let key_one = RSValue_NewNumber(1.0);
            let value_one = RSValue_NewNumber(2.0);
            let key_two = RSValue_NewNumber(3.0);
            let value_two = RSValue_NewNumber(4.0);
            RSValueMap_SetEntry(map, 0, key_one, value_one);
            RSValueMap_SetEntry(map, 1, key_two, value_two);

            let map = RSValue_NewMap(map);

            assert_eq!(RSValue_Map_Len(map), 2);

            let mut key: *mut RsValue = std::ptr::null_mut();
            let mut value: *mut RsValue = std::ptr::null_mut();

            RSValue_Map_GetEntry(map, 0, &mut key as *mut _, &mut value as *mut _);
            let key_num = RSValue_Number_Get(key);
            assert_eq!(1.0, key_num);
            let value_num = RSValue_Number_Get(value);
            assert_eq!(2.0, value_num);

            RSValue_Map_GetEntry(map, 1, &mut key as *mut _, &mut value as *mut _);
            let key_num = RSValue_Number_Get(key);
            assert_eq!(3.0, key_num);
            let value_num = RSValue_Number_Get(value);
            assert_eq!(4.0, value_num);

            RSValue_DecrRef(map);
        }
    }
}
