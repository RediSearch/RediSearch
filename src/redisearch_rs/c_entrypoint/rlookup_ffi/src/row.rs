/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use rlookup::{RLookupKey, RLookupRow};

use value::{RSValueFFI, RSValueTrait};

#[unsafe(no_mangle)]
#[expect(unused, reason = "implemented by later stacked PRs")]
unsafe extern "C" fn RLookup_WriteKey(
    key: *const RLookupKey,
    mut row: NonNull<RLookupRow<RSValueFFI>>,
    value: NonNull<ffi::RSValue>,
) {
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupKey
    let key = unsafe { key.as_ref() }.expect("Key must not be null");

    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow
    let row = unsafe { row.as_mut() };

    let mut value = RSValueFFI(value);
    value.increment();
    row.write_key(key, value);
}

#[unsafe(no_mangle)]
#[expect(unused, reason = "implemented by later stacked PRs")]
unsafe extern "C" fn RLookupRow_WriteOwnKey(
    key: *const RLookupKey,
    mut row: NonNull<RLookupRow<RSValueFFI>>,
    value: NonNull<ffi::RSValue>,
) {
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupKey
    let key = unsafe { key.as_ref() }.expect("Key must not be null");

    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow
    let row = unsafe { row.as_mut() };

    row.write_key(key, RSValueFFI(value));
}

/// Wipes a RLookupRow by decrementing all values and resetting the row.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RLookupRow`].
#[unsafe(no_mangle)]
#[expect(unused, reason = "implemented by later stacked PRs")]
unsafe extern "C" fn RLookupRow_Wipe(mut vec: NonNull<RLookupRow<RSValueFFI>>) {
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow.
    let vec = unsafe { vec.as_mut() };
    vec.wipe();
}

/// Cleanup a RLookupRow by wiping it (see [`RLookupRow_Wipe`]) and deallocating the memory.
///
/// Safety:
/// 1. The pointer must be a valid pointer to an [`RLookupRow`].
#[unsafe(no_mangle)]
#[expect(unused, reason = "implemented by later stacked PRs")]
unsafe extern "C" fn RLookupRow_Cleanup(vec: NonNull<RLookupRow<RSValueFFI>>) {
    todo!("Implement RLookupRow_Cleanup");
}
