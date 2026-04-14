/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::RSValue;
use libc::size_t;
use std::mem::MaybeUninit;
use value::{Map, SharedValue, SharedValueRef, Value};

/// Opaque map structure used during map construction.
/// Holds uninitialized entries that are populated via [`RSValue_MapBuilderSetEntry`]
/// before being finalized into an [`Value::Map`] via [`RSValue_NewMapFromBuilder`].
pub struct RSValueMapBuilder {
    entries: Box<[MaybeUninit<(SharedValue, SharedValue)>]>,
}

/// Allocates a new, uninitialized [`RSValueMapBuilder`] with space for `len` entries.
///
/// The map entries are uninitialized and must be set using [`RSValue_MapBuilderSetEntry`]
/// before being finalized into an [`RSValue`] via [`RSValue_NewMapFromBuilder`].
///
/// # Safety
///
/// 1. All entries must be initialized via [`RSValue_MapBuilderSetEntry`] before
///    passing the map to [`RSValue_NewMapFromBuilder`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewMapBuilder(len: u32) -> *mut RSValueMapBuilder {
    let entries: Vec<MaybeUninit<(*mut RSValue, *mut RSValue)>> =
        vec![MaybeUninit::uninit(); len as usize];
    let entries: Vec<MaybeUninit<(SharedValue, SharedValue)>> =
        unsafe { std::mem::transmute(entries) };

    Box::into_raw(Box::new(RSValueMapBuilder {
        entries: entries.into(),
    }))
}

/// Sets a key-value pair at a specific index in the map.
///
/// Takes ownership of both the `key` and `value` [`RSValue`] pointers.
///
/// # Safety
///
/// 1. `map` must be a valid pointer to an [`RSValueMapBuilder`] created by
///    [`RSValue_NewMapBuilder`].
/// 2. `key` and `value` must be valid pointers to [`RSValue`]
///
/// # Panics
///
/// Panics if `index` is greater than or equal to the map length.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_MapBuilderSetEntry(
    map: *mut RSValueMapBuilder,
    index: size_t,
    key: SharedValue,
    value: SharedValue,
) {
    // Safety: ensured by caller (1.)
    let map = unsafe { map.as_mut().expect("map should not be null") };

    // Compatibility: C does an RS_ASSERT on index out of bounds
    map.entries[index] = MaybeUninit::new((key, value));
}

/// Creates a heap-allocated map [`RSValue`] from an [`RSValueMapBuilder`].
///
/// Takes ownership of the map structure and all its entries. The [`RSValueMapBuilder`]
/// pointer is consumed and must not be used after this call.
///
/// # Safety
///
/// 1. `map` must be a valid pointer to an [`RSValueMapBuilder`] created by
///    [`RSValue_NewMapBuilder`].
/// 2. All entries in the map must have been initialized via [`RSValue_MapBuilderSetEntry`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_NewMapFromBuilder(map: *mut RSValueMapBuilder) -> SharedValue {
    // Safety: ensured by caller (1.)
    let map = unsafe { Box::from_raw(map) };

    let map = map
        .entries
        .into_iter()
        .map(|entry| {
            // Safety: ensured by caller (2.)
            unsafe { entry.assume_init() }
        })
        .collect();

    let value = Value::Map(Map::new(map));
    let shared = SharedValue::new(value);
    shared
}

/// Returns the number of key-value pairs in a map [`RSValue`].
///
/// # Safety
///
/// 1. `map` must point to a valid [`RSValue`].
///
/// # Panics
///
/// Panics if `map` is not a map value.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Map_Len(map: SharedValueRef) -> u32 {
    if let Value::Map(map) = &**map {
        map.len_u32()
    } else {
        // Compatibility: C does an RS_ASSERT on incorrect type
        panic!("Expected 'Map' type, got '{}'", map.variant_name())
    }
}

/// Retrieves a key-value pair from a map [`RSValue`] at a specific index.
///
/// The returned key and value pointers are borrowed from the map and must
/// not be freed by the caller.
///
/// # Safety
///
/// 1. `map` must point to a valid [`RSValue`].
/// 2. `key` and `value` must be valid, non-null pointers to writable
///    `*mut RSValue` locations.
///
/// # Panics
///
/// - Panics if `map` is not a map value.
/// - Panics if `index` is greater or equal to the map length.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Map_GetEntry(
    map: SharedValueRef,
    index: u32,
    key: *mut SharedValueRef,
    value: *mut SharedValueRef,
) {
    if let Value::Map(map) = &**map {
        // Compatibility: C does an RS_ASSERT on index out of bounds
        let (shared_key, shared_value) = &map[index as usize];
        // Safety: ensured by caller (2.)
        unsafe { key.write(shared_key.as_ref()) };
        // Safety: ensured by caller (2.)
        unsafe { value.write(shared_value.as_ref()) };
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
            let map = RSValue_NewMapBuilder(2);
            let key_one = RSValue_NewNumber(1.0);
            let value_one = RSValue_NewNumber(2.0);
            let key_two = RSValue_NewNumber(3.0);
            let value_two = RSValue_NewNumber(4.0);
            RSValue_MapBuilderSetEntry(map, 0, key_one, value_one);
            RSValue_MapBuilderSetEntry(map, 1, key_two, value_two);

            let map = RSValue_NewMapFromBuilder(map);

            assert_eq!(RSValue_Map_Len(map.as_ref()), 2);

            let mut key = MaybeUninit::<SharedValueRef>::uninit();
            let mut value = MaybeUninit::<SharedValueRef>::uninit();

            RSValue_Map_GetEntry(map.as_ref(), 0, key.as_mut_ptr(), value.as_mut_ptr());
            let key_num = RSValue_Number_Get(key.assume_init_read());
            assert_eq!(1.0, key_num);
            let value_num = RSValue_Number_Get(value.assume_init_read());
            assert_eq!(2.0, value_num);

            RSValue_Map_GetEntry(map.as_ref(), 1, key.as_mut_ptr(), value.as_mut_ptr());
            let key_num = RSValue_Number_Get(key.assume_init_read());
            assert_eq!(3.0, key_num);
            let value_num = RSValue_Number_Get(value.assume_init_read());
            assert_eq!(4.0, value_num);

            RSValue_DecrRef(map);
        }
    }
}
