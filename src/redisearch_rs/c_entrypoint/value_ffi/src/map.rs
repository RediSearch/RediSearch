/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::util::{as_rs_value, expect_value, into_rs_value};
use crate::{RSValue, util::into_shared_value};
use libc::size_t;
use std::mem::MaybeUninit;
use value::{Map, SharedValue, Value};

/// Opaque map structure used during map construction.
/// Holds uninitialized entries that are populated via [`RSValue_MapBuilderSetEntry`]
/// before being finalized into an [`Value::Map`] via [`RSValue_NewMapFromBuilder`].
pub struct RSValueMapBuilder {
    entries: Box<[MaybeUninit<(*mut RSValue, *mut RSValue)>]>,
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
    let entries = vec![MaybeUninit::uninit(); len as usize];

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
/// 2. `key` and `value` must be [valid], non-null pointers to [`RSValue`]s.
///
/// # Panics
///
/// Panics if `index` is greater than or equal to the map length.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_MapBuilderSetEntry(
    map: *mut RSValueMapBuilder,
    index: size_t,
    key: *mut RSValue,
    value: *mut RSValue,
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
pub unsafe extern "C" fn RSValue_NewMapFromBuilder(map: *mut RSValueMapBuilder) -> *mut RSValue {
    // Safety: ensured by caller (1.)
    let map = unsafe { Box::from_raw(map) };

    let map = map
        .entries
        .into_iter()
        .map(|entry| {
            // Safety: ensured by caller (2.)
            let (key, value) = unsafe { entry.assume_init() };
            // Safety: ensured by caller (2.)
            let key = unsafe { into_shared_value(key) };
            // Safety: ensured by caller (2.)
            let value = unsafe { into_shared_value(value) };
            (key, value)
        })
        .collect();

    let value = Value::Map(Map::new(map));
    let shared = SharedValue::new(value);
    into_rs_value(shared)
}

/// Returns the number of key-value pairs in a map [`RSValue`].
///
/// # Safety
///
/// 1. `map` must be a [valid], non-null pointer to an [`RSValue`].
///
/// # Panics
///
/// Panics if `map` is not a map value.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Map_Len(map: *const RSValue) -> u32 {
    // Safety: ensured by caller (1.)
    let map = unsafe { expect_value(map) };

    if let Value::Map(map) = map {
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
/// 1. `map` must be a [valid], non-null pointer to an [`RSValue`].
/// 2. `key` and `value` must be valid, non-null pointers to writable
///    `*mut RSValue` locations.
///
/// # Panics
///
/// - Panics if `map` is not a map value.
/// - Panics if `index` is greater or equal to the map length.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSValue_Map_GetEntry(
    map: *const RSValue,
    index: u32,
    key: *mut *mut RSValue,
    value: *mut *mut RSValue,
) {
    // Safety: ensured by caller (1.)
    let map = unsafe { expect_value(map) };

    if let Value::Map(map) = map {
        // Compatibility: C does an RS_ASSERT on index out of bounds
        let (shared_key, shared_value) = &map[index as usize];
        // Safety: ensured by caller (2.)
        unsafe { key.write(as_rs_value(shared_key).cast_mut()) };
        // Safety: ensured by caller (2.)
        unsafe { value.write(as_rs_value(shared_value).cast_mut()) };
    } else {
        panic!("Expected 'Map' type, got '{}'", map.variant_name())
    }
}
