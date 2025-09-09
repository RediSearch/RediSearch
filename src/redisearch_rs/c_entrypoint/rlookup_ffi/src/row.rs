/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rlookup::{RLookup, RLookupKey};
use sorting_vector::RSSortingVector;
use std::{mem::ManuallyDrop, ptr::NonNull};
use value::RSValueFFI;

/// cbindgen:no-export
pub type RLookupRow = rlookup::RLookupRow<'static, RSValueFFI>;

/// Writes a key to the row but increments the value reference count before writing it thus having shared ownership.
///
/// # Safety
///
/// 1. `key` must be a [valid], non-null pointer to an [`RLookupKey`].
/// 2. `row` must be a [valid], non-null pointer to an [`RLookupRow`].
/// 3. `value` must be a [valid], non-null pointer to an [`ffi::RSValue`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
unsafe extern "C" fn RLookup_WriteKey(
    key: *const RLookupKey,
    row: Option<NonNull<RLookupRow>>,
    value: Option<NonNull<ffi::RSValue>>,
) {
    // Safety: ensured by caller (1.)
    let key = unsafe { key.as_ref() }.expect("Key must not be null");
    // Safety: ensured by caller (2.)
    let row = unsafe { row.expect("row must not be null").as_mut() };

    // this method does not take ownership of `value` so we must take care not to drop it at the end of the scope
    // (therefore the `ManuallyDrop`). Instead we explicitly clone the value before inserting it below.
    // Safety: ensured by caller (3.)
    let value =
        ManuallyDrop::new(unsafe { RSValueFFI::from_raw(value.expect("value must not be null")) });

    row.write_key(key, ManuallyDrop::into_inner(value.clone()));
}

/// Writes a key to the row without incrementing the value reference count, thus taking ownership of the value.
///
/// # Safety
///
/// 1. `key` must be a [valid], non-null pointer to an [`RLookupKey`].
/// 2. `row` must be a [valid], non-null pointer to an [`RLookupRow`].
/// 3. `value` must be a [valid], non-null pointer to an [`ffi::RSValue`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
unsafe extern "C" fn RLookup_WriteOwnKey(
    key: *const RLookupKey,
    row: Option<NonNull<RLookupRow>>,
    value: Option<NonNull<ffi::RSValue>>,
) {
    // Safety: ensured by caller (1.)
    let key = unsafe { key.as_ref() }.expect("`key` must not be null");
    // Safety: ensured by caller (2.)
    let row = unsafe { row.expect("`row` must not be null").as_mut() };
    // Safety: ensured by caller (3.)
    let value = unsafe { RSValueFFI::from_raw(value.expect("`value` must not be null")) };

    row.write_key(key, value);
}

/// Wipes a RLookupRow by decrementing all values and resetting the row.
///
/// # Safety
///
/// 1. `row` must be a [valid], non-null pointer to an [`RLookupRow`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
unsafe extern "C" fn RLookupRow_Wipe(row: Option<NonNull<RLookupRow>>) {
    // Safety: ensured by caller (1.)
    let row = unsafe { row.expect("`row` must not be null").as_mut() };
    row.wipe();
}

/// Resets a RLookupRow by wiping it (see [`RLookupRow_Wipe`]) and deallocating the memory of the dynamic values.
///
/// This does not affect the sorting vector.
///
/// # Safety
///
/// 1. `row` must be a [valid], non-null pointer to an [`RLookupRow`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
unsafe extern "C" fn RLookupRow_Reset(row: Option<NonNull<RLookupRow>>) {
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow.
    let vec = unsafe { row.expect("`row` must not be null").as_mut() };
    vec.reset_dyn_values();
}

/// Move data from the source row to the destination row. The source row is cleared.
/// The destination row should be pre-cleared (though its cache may still exist).
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to an [`RLookup`].
/// 2. `src` must be a [valid], non-null pointer to an [`RLookupRow`].
/// 3. `dst` must be a [valid], non-null pointer to an [`RLookupRow`].
/// 4. `src` and `dst` must not be the same lookup row.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn RLookupRow_Move(
    lookup: Option<NonNull<RLookup<'_>>>,
    src: Option<NonNull<RLookupRow>>,
    dst: Option<NonNull<RLookupRow>>,
) {
    debug_assert_ne!(src, dst, "`src` and `dst` must not be the same");
    // Safety: ensured by caller (1.)
    let lookup = unsafe { lookup.as_ref().expect("`lookup` must not be null") };
    // Safety: ensured by caller (2.)
    let src = unsafe { src.expect("`src` must not be null").as_mut() };
    // Safety: ensured by caller (3.)
    let dst = unsafe { dst.expect("`dst` must not be null").as_mut() };
    debug_assert!(dst.is_empty(), "expected `dst` to be pre-cleared");
    #[cfg(debug_assertions)]
    {
        assert_eq!(
            src.rlookup_id(),
            lookup.id(),
            "`src` must belong to `rlookup`"
        );
        assert_eq!(
            dst.rlookup_id(),
            lookup.id(),
            "`dst` must belong to `rlookup`"
        );
    }

    for key in lookup.cursor() {
        if let Some(value) = src.get(key) {
            dst.write_key(key, value.to_owned());
        }
    }
    src.wipe();
}

/// Write fields from a source row into a destination row, the fields must exist in both lookups (schemas).
///  
/// Iterate through the source lookup keys, if it finds a corresponding key in the destination
/// lookup by name, then it's value is written to this row as a destination.
///
/// If a source key is not found in the destination lookup the function will panic (same as C behavior).
///
/// If a source key has no value in the source row, it is skipped.
///
/// # Safety
/// 1. `src_lookup` must be a [valid], non-null pointer to an [`RLookup`].
/// 2. `src_row` must be a [valid], non-null pointer to an [`RLookupRow`].
/// 3. `dst_lookup` must be a [valid], non-null pointer to an [`RLookup`].
/// 4. `dst_row` must be a [valid], non-null pointer to an [`RLookupRow`].
#[unsafe(no_mangle)]
unsafe extern "C" fn RLookupRow_WriteFieldsFrom(
    src_row: Option<NonNull<RLookupRow>>,
    src_lookup: Option<NonNull<RLookup>>,
    dst_row: Option<NonNull<RLookupRow>>,
    dst_lookup: Option<NonNull<RLookup>>,
) {
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow.
    let src_row = unsafe { src_row.expect("src_row must not be null").as_ref() };
    let src_lookup = unsafe { src_lookup.expect("src_lookup must not be null").as_ref() };
    let dst_row = unsafe { dst_row.expect("dst_row must not be null").as_mut() };
    let dst_lookup = unsafe { dst_lookup.expect("dst_lookup must not be null").as_ref() };

    // Safety: Caller has to ensure that the pointers are
    unsafe {
        dst_row.copy_fields_from(dst_lookup, src_row, src_lookup);
    }
}

/// Retrieves an item from the given `RLookupRow` based on the provided `RLookupKey`.
/// The function first checks for dynamic values, and if not found, it checks the sorting vector
/// if the `SvSrc` flag is set in the key.
/// If the item is not found in either location, it returns `None`.
///
/// # Safety
/// 1. `key` must be a [valid], non-null pointer to an [`RLookupKey`].
/// 2. `row` must be a [valid], non-null pointer to an [`RLookupRow`].
#[unsafe(no_mangle)]
unsafe extern "C" fn RLookup_GetItem(
    key: Option<NonNull<RLookupKey>>,
    row: Option<NonNull<RLookupRow>>,
) -> *const ffi::RSValue {
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow.
    let row = unsafe { row.expect("row must not be null").as_ref() };
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupKey.
    let key = unsafe { key.expect("key must not be null").as_ref() };

    match row.get(key) {
        Some(value) => value.as_ptr(),
        None => std::ptr::null(),
    }
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

/// Returns the number of dynamic values in the row.
///
/// # Safety
/// 1. `row` must be a [valid], non-null pointer to an [`RLookupRow`].
///
#[unsafe(no_mangle)]
unsafe extern "C" fn RLookupRow_GetDynLen(row: Option<NonNull<RLookupRow>>) -> u32 {
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow.
    let row = unsafe { row.expect("row must not be null").as_ref() };
    // Safety: The caller has to ensure that the pointer is kept around
    row.num_dyn_values()
}
