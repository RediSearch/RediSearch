/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use c_ffi_utils::debug_expect;
use libc::size_t;
use value::{
    collection::{RsValueArray, RsValueMap, RsValueMapEntry},
    shared::SharedRsValue,
};

/// Create a new, uninitialized `RsValueMap`, reserving space for `cap`
/// entries. The map entries are uninitialized and must be set using `RSValueMap_SetEntry`.
///
/// # Safety
/// - The all items of the returned `RsValueMap` must be initialized using
///   `RsValueMap_SetEntry`.
///
/// @param cap the number of entries (key and value) of capacity the map needs to get
/// @returns an uninitialized `RsValueMap` of `cap` capacity.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValueMap_AllocUninit(cap: u32) -> RsValueMap {
    // Safety: caller must ensure the `RsValueMap`'s items
    // are initialized using `RsValueMap_SetEntry`.
    unsafe { RsValueMap::reserve_uninit(cap) }
}

/// Set a key-value pair at a specific index in the map.
/// Takes ownership of both the key and value RSValues.
///
/// # Safety
/// - `map` must be a valid pointer to an `RsValueMap` that
///   has been created by `RsValueMap_AllocUninit` and
///   that is valid for writes.
/// - `i` must smaller than the capacity of the `RsValueMap`.
///
/// @param map The map to modify
/// @param i The index where to set the entry (must be < map->len)
/// @param key The key RSValue (ownership is transferred to the map)
/// @param value The value RSValue (ownership is transferred to the map)
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValueMap_SetEntry(
    map: Option<NonNull<RsValueMap>>,
    i: size_t,
    key: SharedRsValue,
    value: SharedRsValue,
) {
    // Safety: caller must ensure `map` is no null
    let map = unsafe { debug_expect!(map) };
    // Safety: caller must ensure `map` originates from a call to
    // `RsValueMap_AllocUninit`, which ensures `map` is dereferencable.
    let map_refm: &mut RsValueMap = unsafe { &mut *map.as_ptr() };

    let entry = RsValueMapEntry::new(key, value);

    // Safety: caller must ensure that `i` is smaller than the capacity
    // of the RsValueMap, which can not exceed `u32::MAX`.
    let i = unsafe { debug_expect!(i.try_into().ok(), "`i` should fit in a u32") };

    // Safety: caller must ensure that `i` is smaller than the capacity
    // of the RsValueMap
    unsafe { map_refm.inner_mut().write_entry(entry, i) };
}

/// Allocates an uninitialized [`RsValueArray`].
///
/// # Safety
/// See [`RsValueCollection::reserve_uninit`]
///
/// @param cap The desired capacity of the `RsValueArray`
/// @return An uninitialized `RsValueArray` of `cap` capacity
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValueArray_AllocUninit(cap: u32) -> RsValueArray {
    // Safety: forwards to `RsValueCollection::reserve_uninit` and
    // has the same safety requirements
    unsafe { RsValueArray::reserve_uninit(cap) }
}

/// Writes a value into the [`RsValueArray`] at `i`.
///
/// # Safety
/// - `arr` must be a non-null pointer to an `RsValueArray` originating from
///   [`RsValueArray_AllocUninit`].
/// - `arr` must be unique
/// - `i` must not exceed the `RsValueArray`'s capacity
///
/// @param arr The array to modify
/// @param i The index at which to write the value
/// @param value the value that is to be written
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RsValueArray_SetEntry(
    arr: Option<NonNull<RsValueArray>>,
    i: size_t,
    value: SharedRsValue,
) {
    // Safety: caller must ensure `arr` is not null
    let arr = unsafe { debug_expect!(arr) };

    // Safety:
    // - Caller must ensure `arr` originates from a call to `RsValueArray_AllocUninit`,
    //   which ensures `arr` points to a valid instance of `RsValueArray`.
    // - Caller must ensure `arr` is unique
    let arr_refm: &mut RsValueArray = unsafe { &mut *arr.as_ptr() };

    // Safety: caller must ensure that `i` is smaller than the capacity
    // of the RsValueMap, which can not exceed `u32::MAX`.
    let i = unsafe { debug_expect!(i.try_into().ok(), "`i` should fit in a u32") };

    // Safety: caller must ensure that `i` is smaller than the capacity
    // of the RsValueArray
    unsafe { arr_refm.inner_mut().write_entry(value, i) };
}
