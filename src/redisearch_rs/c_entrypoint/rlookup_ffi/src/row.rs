/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi;

use rlookup::{RLookup, RLookupKey, RLookupKeyFlags};
use sorting_vector::RSSortingVector;
use std::{mem::ManuallyDrop, ptr::NonNull};
use value::RSValueFFI;

use crate::extension::OpaqueExt;

// see cbindgen.toml parse.expand - there is an option but it doesn't seem to work
/*
impl_opaque_ext!(
    rlookup::RLookupRow<'_, value::RSValueFFI>,
    OpaqueRLookupRow,
    8,
    40,
    48
);
*/

// Recursive expansion of impl_opaque_ext! macro
// ==============================================

#[cfg(not(debug_assertions))]
#[repr(C, align(8))]
pub struct OpaqueRLookupRow(std::mem::MaybeUninit<[u8; 40]>);

#[cfg(debug_assertions)]
#[repr(C, align(8))]
pub struct OpaqueRLookupRow(std::mem::MaybeUninit<[u8; 48]>);

const _: () = {
    #[allow(unreachable_code, clippy::never_loop)]
    loop {
        // Safety: The sizes and alignments are verified by the macro invocation at compile time.
        unsafe {
            std::mem::transmute::<OpaqueRLookupRow, rlookup::RLookupRow<'_, value::RSValueFFI>>(
                break,
            )
        };
    }
};

#[allow(dead_code)]
#[allow(clippy::missing_safety_doc)]
#[allow(clippy::undocumented_unsafe_blocks)]
impl OpaqueExt for rlookup::RLookupRow<'_, value::RSValueFFI> {
    type OpaqueType = OpaqueRLookupRow;
    fn into_opaque(self) -> Self::OpaqueType {
        // Safety: Same size and alignment guaranteed by compile-time checks above
        unsafe { std::mem::transmute(self) }
    }
    fn as_opaque_ptr(&self) -> *const Self::OpaqueType {
        std::ptr::from_ref(self).cast()
    }
    fn as_opaque_mut_ptr(&mut self) -> *mut Self::OpaqueType {
        std::ptr::from_mut(self).cast()
    }
    unsafe fn from_opaque(opaque: Self::OpaqueType) -> Self {
        // Safety: Same size and alignment guaranteed by compile-time checks above
        unsafe { std::mem::transmute(opaque) }
    }
    unsafe fn from_opaque_ptr<'a>(opaque: *const Self::OpaqueType) -> Option<&'a Self> {
        //
        unsafe { opaque.cast::<Self>().as_ref() }
    }
    unsafe fn from_opaque_mut_ptr<'a>(opaque: *mut Self::OpaqueType) -> Option<&'a mut Self> {
        unsafe { opaque.cast::<Self>().as_mut() }
    }
}

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
    row: Option<NonNull<OpaqueRLookupRow>>,
    value: Option<NonNull<ffi::RSValue>>,
) {
    // Safety: ensured by caller (1.)
    let key = unsafe { key.as_ref() }.expect("Key must not be null");
    let row = row.expect("row must not be null").as_ptr();
    // Safety: ensured by caller (2.)
    let row: &mut rlookup::RLookupRow<'_, value::RSValueFFI> =
        unsafe { OpaqueExt::from_opaque_mut_ptr(row as *mut _).expect("row is null") };

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
unsafe extern "C-unwind" fn RLookup_WriteOwnKey(
    key: *const RLookupKey,
    row: Option<NonNull<OpaqueRLookupRow>>,
    value: Option<NonNull<ffi::RSValue>>,
) {
    // Safety: ensured by caller (1.)
    let key = unsafe { key.as_ref() }.expect("`key` must not be null");
    let row = row.expect("`row` must not be null").as_ptr();
    // Safety: ensured by caller (2.)
    let row: &mut rlookup::RLookupRow<'_, value::RSValueFFI> =
        unsafe { OpaqueExt::from_opaque_mut_ptr(row as *mut _).expect("row is null") };
    // Safety: ensured by caller (3.)
    let value = unsafe { RSValueFFI::from_raw(value.expect("`value` must not be null")) };

    row.write_key(key, value);
}

/// Creates a RLookupRow on the stack associated with the given lookup.
///
/// The lookup is only tracked in Debug builds
///
/// # Safety
/// /// 1. `lookup` must be a [valid], non-null pointer to an [`RLookup`].
#[unsafe(no_mangle)]
unsafe extern "C" fn RLookupRow_CreateOnStack(
    lookup: Option<NonNull<RLookup>>,
) -> OpaqueRLookupRow {
    // Safety: Ensured by caller (1.)
    let lookup = unsafe { lookup.expect("lookup must not be null").as_ref() };
    rlookup::RLookupRow::<value::RSValueFFI>::new(lookup).into_opaque()
}

/// Wipes a RLookupRow by decrementing all values and resetting the row.
///
/// # Safety
///
/// 1. `row` must be a [valid], non-null pointer to an [`RLookupRow`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
unsafe extern "C" fn RLookupRow_Wipe(row: Option<NonNull<OpaqueRLookupRow>>) {
    let row = row.expect("row must not be null").as_ptr();
    // Safety: ensured by caller (1.)
    let row: &mut rlookup::RLookupRow<'_, value::RSValueFFI> =
        unsafe { OpaqueExt::from_opaque_mut_ptr(row as *mut _).expect("row is null") };
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
unsafe extern "C" fn RLookupRow_Reset(row: Option<NonNull<OpaqueRLookupRow>>) {
    let row = row.expect("row must not be null").as_ptr();
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow.
    let row: &mut rlookup::RLookupRow<'_, value::RSValueFFI> =
        unsafe { OpaqueExt::from_opaque_mut_ptr(row as *mut _).expect("row is null") };
    row.reset_dyn_values();
}

/// Move data from the source row to the destination row. The source row is cleared.
/// The destination row should be pre-cleared (though its cache may still exist).
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to an [`RLookup`].
/// 2. `src` must be a [valid], non-null pointer to an [`rlookup::RLookupRow`].
/// 3. `dst` must be a [valid], non-null pointer to an [`rlookup::RLookupRow`].
/// 4. `src` and `dst` must not be the same lookup row.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn RLookupRow_Move(
    lookup: Option<NonNull<RLookup<'_>>>,
    src: Option<NonNull<OpaqueRLookupRow>>,
    dst: Option<NonNull<OpaqueRLookupRow>>,
) {
    debug_assert_ne!(src, dst, "`src` and `dst` must not be the same");
    // Safety: ensured by caller (1.)
    let lookup = unsafe { lookup.expect("`lookup` must not be null").as_ref() };
    // Safety: ensured by caller (2.)
    let src = src.expect("`src` must not be null").as_ptr();
    // Safety: ensured by caller (2.)
    let src: &mut rlookup::RLookupRow<'_, value::RSValueFFI> =
        unsafe { OpaqueExt::from_opaque_mut_ptr(src as *mut _).expect("src is null") };
    let dst = dst.expect("`dst` must not be null").as_ptr();
    // Safety: ensured by caller (3.)
    let dst: &mut rlookup::RLookupRow<'_, value::RSValueFFI> =
        unsafe { OpaqueExt::from_opaque_mut_ptr(dst as *mut _).expect("dst is null") };
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
    src_row: Option<NonNull<OpaqueRLookupRow>>,
    src_lookup: Option<NonNull<RLookup>>,
    dst_row: Option<NonNull<OpaqueRLookupRow>>,
    dst_lookup: Option<NonNull<RLookup>>,
) {
    let src_row = src_row.expect("src_row must not be null").as_ptr();
    // Safety: The caller has to ensure (2)
    let src_row: &rlookup::RLookupRow<'_, value::RSValueFFI> =
        unsafe { OpaqueExt::from_opaque_ptr(src_row).expect("src_row is null") };
    // Safety: The caller has to ensure (1)
    let src_lookup = unsafe { src_lookup.expect("src_lookup must not be null").as_ref() };

    let dst_row = dst_row.expect("dst_row must not be null").as_ptr();
    // Safety: The caller has to ensure (4)
    let dst_row: &mut rlookup::RLookupRow<'_, value::RSValueFFI> =
        unsafe { OpaqueExt::from_opaque_mut_ptr(dst_row as *mut _).expect("dst_row is null") };
    // Safety: The caller has to ensure (3)
    let dst_lookup = unsafe { dst_lookup.expect("dst_lookup must not be null").as_ref() };

    // Safety: Caller has to ensure that the pointers are
    unsafe {
        dst_row.copy_fields_from(dst_lookup, src_row, src_lookup);
    }
}

/// Add all on-overridden keys from `src` to `self`.
///
/// For each key in src, check if it already exists *by name*.
/// - If it does the `flag` argument controls the behaviour (skip with `RLookupKeyFlags::empty()`, override with `RLookupKeyFlag::Override`).
/// - If it doesn't a new key will ne created.
///
/// Flag handling:
///  * - Preserves persistent source key properties (F_SVSRC, F_HIDDEN, F_EXPLICITRETURN, etc.)
///  * - Filters out transient flags from source keys (F_OVERRIDE, F_FORCE_LOAD)
///  * - Respects caller's control flags for behavior (F_OVERRIDE, F_FORCE_LOAD, etc.)
///  * - Target flags = caller_flags | (source_flags & ~RLOOKUP_TRANSIENT_FLAGS)
///
/// # Safety:
/// 1. `src` must be a [valid], non-null pointer to an [`RLookup`].
/// 2. `dst` must be a [valid], non-null pointer to an [`RLookup`].
#[unsafe(no_mangle)]
unsafe extern "C" fn RLookup_AddKeysFrom<'a>(
    src: Option<NonNull<RLookup<'a>>>,
    dst: Option<NonNull<RLookup<'a>>>,
    flags: u32,
) {
    // Safety: ensured by caller (1.)
    let src = unsafe { src.expect("`src` must not be null").as_ref() };
    // Safety: ensured by caller (2.)
    let dst = unsafe { dst.expect("`dst` must not be null").as_mut() };

    dst.add_keys_from(src, RLookupKeyFlags::from_bits_truncate(flags));
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
    row: Option<NonNull<OpaqueRLookupRow>>,
) -> *const ffi::RSValue {
    let row = row.expect("row must not be null").as_ptr();
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow.
    let row: &rlookup::RLookupRow<'_, value::RSValueFFI> =
        unsafe { OpaqueExt::from_opaque_ptr(row).expect("row is null") };
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
    row: Option<NonNull<OpaqueRLookupRow>>,
    sv: *const ffi::RSSortingVector,
) {
    let row = row.expect("row must not be null").as_ptr();
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow.
    let row: &mut rlookup::RLookupRow<'_, value::RSValueFFI> =
        unsafe { OpaqueExt::from_opaque_mut_ptr(row as *mut _).expect("row is null") };
    let sv: *const RSSortingVector<RSValueFFI> = sv.cast();
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RSSortingVector.
    let sv = unsafe { sv.as_ref() };

    row.set_sorting_vector(sv.unwrap());
}

/// Returns a pointer to the sorting vector if it exists, or null otherwise.
///
/// Safety:
/// 1. `row` must be a valid pointer to an [`RLookupRow`].
#[unsafe(no_mangle)]
unsafe extern "C" fn RLookupRow_GetSortingVector(
    row: Option<NonNull<OpaqueRLookupRow>>,
) -> *const ffi::RSSortingVector {
    let row = row.expect("row must not be null").as_ptr();
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow.
    let row: &rlookup::RLookupRow<'_, value::RSValueFFI> =
        unsafe { OpaqueExt::from_opaque_ptr(row).expect("row is null") };
    // Safety: The caller has to ensure that the pointer is kept around
    unsafe { row.get_sorting_vector() as *const ffi::RSSortingVector }
}

/// Returns the number of dynamic values in the row.
///
/// # Safety
/// Safety:
/// 1. `row` must be a valid pointer to an [`RLookupRow`].
#[unsafe(no_mangle)]
unsafe extern "C" fn RLookupRow_GetDynLen(row: Option<NonNull<OpaqueRLookupRow>>) -> u32 {
    let row = row.expect("row must not be null").as_ptr();
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow.
    let row: &rlookup::RLookupRow<'_, value::RSValueFFI> =
        unsafe { OpaqueExt::from_opaque_ptr(row).expect("row is null") };
    // Safety: The caller has to ensure that the pointer is kept around
    row.num_dyn_values()
}
