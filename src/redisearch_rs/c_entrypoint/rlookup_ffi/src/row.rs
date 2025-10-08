/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rlookup::RLookupKey;
use std::{mem::ManuallyDrop, ptr::NonNull};
use value::RSValueFFI;

/// Writes a key to the row but increments the value reference count before writing it thus having shared ownership.
///
/// Safety:
/// 1. `key` must be a valid pointer to an [`RLookupKey`].
/// 2. `row` must be a valid pointer to an [`RLookupRow`].
/// 3. `value` must be a valid pointer to an [`ffi::RSValue`].
#[unsafe(no_mangle)]
unsafe extern "C" fn RLookup_WriteKey<'a>(
    key: *const RLookupKey,
    row: Option<NonNull<rlookup::RLookupRow<'a, RSValueFFI>>>,
    value: Option<NonNull<ffi::RSValue>>,
) {
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupKey
    let key = unsafe { key.as_ref() }.expect("Key must not be null");

    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow
    let row = unsafe { row.expect("row must not be null").as_mut() };

    // this method does not take ownership of `value` so we must take care not to drop it at the end of the scope
    // (therefore the `ManuallyDrop`). Instead we explicitly clone the value before inserting it below.
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RSValue
    let value =
        ManuallyDrop::new(unsafe { RSValueFFI::from_raw(value.expect("value must not be null")) });

    row.write_key(key, ManuallyDrop::into_inner(value.clone()));
}

/// Writes a key to the row without incrementing the value reference count, thus taking ownership of the value.
///
/// Safety:
/// 1. `key` must be a valid pointer to an [`RLookupKey`].
/// 2. `row` must be a valid pointer to an [`RLookupRow`].
/// 3. `value` must be a valid pointer to an [`ffi::RSValue`].
#[unsafe(no_mangle)]
unsafe extern "C" fn RLookup_WriteOwnKey<'a>(
    key: *const RLookupKey,
    row: Option<NonNull<rlookup::RLookupRow<'a, RSValueFFI>>>,
    value: Option<NonNull<ffi::RSValue>>,
) {
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupKey
    let key = unsafe { key.as_ref() }.expect("Key must not be null");

    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow
    let row = unsafe { row.expect("row must not be null").as_mut() };

    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RSValue
    let value = unsafe { RSValueFFI::from_raw(value.expect("value must not be null")) };

    row.write_key(key, value);
}

/// Wipes a RLookupRow by decrementing all values and resetting the row.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RLookupRow`].
#[unsafe(no_mangle)]
unsafe extern "C" fn RLookupRow_Wipe<'a>(
    row: Option<NonNull<rlookup::RLookupRow<'a, RSValueFFI>>>,
) {
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow.
    let row = unsafe { row.expect("row must not be null").as_mut() };
    row.wipe();
}

/// Resets a RLookupRow by wiping it (see [`RLookupRow_Wipe`]) and deallocating the memory of the dynamic values.
///
/// This does not affect the sorting vector.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RLookupRow`].
#[unsafe(no_mangle)]
unsafe extern "C" fn RLookupRow_Reset<'a>(
    row: Option<NonNull<rlookup::RLookupRow<'a, RSValueFFI>>>,
) {
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow.
    let vec = unsafe { row.expect("row must not be null").as_mut() };
    vec.reset_dyn_values();
}
