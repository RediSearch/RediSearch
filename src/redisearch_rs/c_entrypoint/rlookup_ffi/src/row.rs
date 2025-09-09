/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use rlookup::RLookupKey;

use sorting_vector::RSSortingVector;
use value::{RSValueFFI, RSValueTrait};

pub type RLookupRow = rlookup::RLookupRow<'static, RSValueFFI>;

/// Writes a key to the row but increments the value reference count before writing it thus having shared ownership.
///
/// Safety:
/// 1. `key` must be a valid pointer to an [`RLookupKey`].
/// 2. `row` must be a valid pointer to an [`RLookupRow`].
/// 3. `value` must be a valid pointer to an [`ffi::RSValue`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_WriteKey(
    key: *const RLookupKey,
    row: Option<NonNull<RLookupRow>>,
    value: Option<NonNull<ffi::RSValue>>,
) {
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupKey
    let key = unsafe { key.as_ref() }.expect("Key must not be null");

    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow
    let row = unsafe { row.expect("row must not be null").as_mut() };

    let mut value = RSValueFFI(value.expect("value must not be null"));
    value.increment();
    row.write_key(key, value);
}

/// Writes a key to the row without incrementing the value reference count, thus taking ownership of the value.
///
/// Safety:
/// 1. `key` must be a valid pointer to an [`RLookupKey`].
/// 2. `row` must be a valid pointer to an [`RLookupRow`].
/// 3. `value` must be a valid pointer to an [`ffi::RSValue`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_WriteOwnKey(
    key: *const RLookupKey,
    row: Option<NonNull<RLookupRow>>,
    value: Option<NonNull<ffi::RSValue>>,
) {
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupKey
    let key = unsafe { key.as_ref() }.expect("Key must not be null");

    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow
    let row = unsafe { row.expect("row must not be null").as_mut() };

    row.write_key(key, RSValueFFI(value.expect("value must not be null")));
}

/// Wipes a RLookupRow by decrementing all values and resetting the row.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RLookupRow`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookupRow_Wipe(row: Option<NonNull<RLookupRow>>) {
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
pub unsafe extern "C" fn RLookupRow_Reset(row: Option<NonNull<RLookupRow>>) {
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow.
    let vec = unsafe { row.expect("row must not be null").as_mut() };
    vec.reset_dyn_values();
}

/// Sets a sorting vector for the row.
/// Safety:
/// 1. `row` must be a valid pointer to an [`RLookupRow`].
/// 2. `sv` must be a valid pointer to an [`ffi::RSSortingVector`].
#[unsafe(no_mangle)]
unsafe extern "C" fn RLookupRow_SetSortingVector(
    row: Option<NonNull<RLookupRow>>,
    sv: *const ffi::RSSortingVector,
) {
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow.
    let row = unsafe { row.expect("row must not be null").as_mut() };
    let sv: *const RSSortingVector<RSValueFFI> = sv.cast();
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RSSortingVector.
    let sv = unsafe { sv.as_ref() };

    row.set_sorting_vector(sv.unwrap());
}

/// Returns a pointer to the sorting vector if it exists, or null otherwise.
///
/// Safety:
/// The caller does not own the returned pointer and must not attempt to free it.
#[unsafe(no_mangle)]
unsafe extern "C" fn RLookupRow_GetSortingVector(
    row: Option<NonNull<RLookupRow>>,
) -> *const ffi::RSSortingVector {
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow.
    let row = unsafe { row.expect("row must not be null").as_ref() };
    // Safety: The caller has to ensure that the pointer is kept around
    unsafe { row.get_sorting_vector() as *const ffi::RSSortingVector }
}

#[unsafe(no_mangle)]
unsafe extern "C" fn RLookupRow_GetDynLen(row: Option<NonNull<RLookupRow>>) -> u32 {
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow.
    let row = unsafe { row.expect("row must not be null").as_ref() };
    // Safety: The caller has to ensure that the pointer is kept around
    row.num_dyn_values()
}
