/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

pub use rlookup::{RLookupKey, RLookupKeyFlag, RLookupKeyFlags};
use value::RSValueFFI;

mod row;

/// Gets an item from the RLookupRow based on the provided RLookupKey. If the item is not found, it returns a null pointer.
///
/// Safety:
/// 1. `key` must be a valid pointer to an [`RLookupKey`].
/// 2. `row` must be a valid pointer to an [`RLookupRow`].
/// 3. The returned pointer must not be freed by the caller, as it is managed by Rust.
#[unsafe(no_mangle)]
unsafe extern "C" fn RLookup_GetItem<'a>(
    key: *const RLookupKey,
    row: *const rlookup::RLookupRow<'a, value::RSValueFFI>,
) -> *mut ffi::RSValue {
    assert!(
        !key.is_null(),
        "RLookup_GetItem called with null key pointer"
    );
    assert!(
        !row.is_null(),
        "RLookup_GetItem called with null row pointer"
    );

    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupKey
    let key = unsafe { key.as_ref() }.expect("Key must not be null");

    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow
    let row = unsafe { row.as_ref() }.expect("Row must not be null");

    match rlookup::rlookup_get_item(key, row) {
        Some(value) => {
            // Safety: We know that RSValueTrait is always RSValueFFI in the C code
            let value: &RSValueFFI = unsafe { std::mem::transmute(value) };
            value.0.as_ptr()
        }
        None => std::ptr::null_mut(),
    }
}
