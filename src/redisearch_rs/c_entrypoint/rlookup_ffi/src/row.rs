/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

use rlookup::RLookupKey;

use value::{RSValueFFI, RSValueTrait};

/// As we only need pointers to the RLookupRow in the C code, so we can just use a typedef via cbindgen.toml
/// Thus we don't need to export the full type definition here, which is cumbersome due to the generic type parameter.
/// cbindgen:ignore
pub struct RLookupRow<'a> {
    inner: rlookup::RLookupRow<'a, RSValueFFI>,
}

impl<'a> Deref for RLookupRow<'a> {
    type Target = rlookup::RLookupRow<'a, RSValueFFI>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for RLookupRow<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[unsafe(no_mangle)]
#[expect(unused, reason = "implemented by later stacked PRs")]
unsafe extern "C" fn RLookupRow_WriteKey(
    key: *const RLookupKey,
    mut row: NonNull<RLookupRow>,
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
    mut row: NonNull<RLookupRow>,
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
unsafe extern "C" fn RLookupRow_Wipe(mut vec: NonNull<RLookupRow>) {
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
unsafe extern "C" fn RLookupRow_Cleanup(vec: NonNull<RLookupRow>) {
    todo!("Implement RLookupRow_Cleanup");
}
