/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use libc::size_t;
use value::{map::RsValueMap, shared::SharedRsValue};

/// Create a new, uninitialized `RsValueMap`, reserving space for `cap`
/// entries. The map entries are uninitialized and must be set using `RSValueMap_SetEntry`.
/// @param cap the number of entries (key and value) of capacity the map needs to get
/// @returns an uninitialized `RsValueMap` of `cap` capacity.
#[unsafe(no_mangle)]
pub extern "C" fn RsValueMap_AllocUninit(cap: u32) -> RsValueMap {
    todo!()
}

/// Set a key-value pair at a specific index in the map.
/// Takes ownership of both the key and value RSValues.
///
/// # Safety
/// - `map` must be a valid pointer to an `RsValueMap` that
///   has been created by `RsValueMap_AllocUninit`.
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
    todo!()
}
