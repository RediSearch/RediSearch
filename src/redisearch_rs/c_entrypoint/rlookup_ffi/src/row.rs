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
use std::{
    mem::{ManuallyDrop, MaybeUninit},
    ptr::NonNull,
};
use value::RSValueFFI;

//pub type RLookupRow = rlookup::RLookupRow<'_, RSValueFFI>;

// cbindgen:no-export
const RLOOKUP_SIZE: u32 = 40;

/// A type with size `N`.
#[repr(transparent)]
pub struct Size<const N: usize>(MaybeUninit<[u8; N]>);

#[cfg(not(debug_assertions))]
#[repr(C, align(8))]
pub struct OpaqueRLookupRow(Size<40>);

/// An opaque RLookupRow which can be passed by value to C.
///
/// The size and alignment of this struct must match the Rust `RLookupRow`
/// structure exactly.
///
/// The size is 40 bytes without the RLookupId field (in debug mode it's 48 bytes).
#[cfg(debug_assertions)]
#[repr(C, align(8))]
pub struct OpaqueRLookupRow(Size<48>);

// If `QueryError` and `OpaqueQueryError` ever differ in size, this code will
// cause a clear error message like:
//
//    = note: source type: `QueryError` (320 bits)
//    = note: target type: `OpaqueQueryError` (256 bits)
//
// Using `assert!(a == b)` is less clear since the values of `a` and `b`
// are not printed. We cannot use `assert_eq` in a const context. We also
// cannot actually run the transmute as that would require that `QueryError` and
// `OpaqueQueryError` have default constant values, so we provide a never type
// (`break`) as the argument.
//
// For alignment, printing a clear error message is more difficult as there
// isn't a magic function like `transmute` that will show the alignments.
const _: () = {
    #[allow(unreachable_code, clippy::never_loop)]
    loop {
        // Safety: this code never runs
        unsafe { std::mem::transmute::<OpaqueRLookupRow, rlookup::RLookupRow<RSValueFFI>>(break) };
    }

    assert!(
        std::mem::align_of::<OpaqueRLookupRow>()
            == std::mem::align_of::<rlookup::RLookupRow<RSValueFFI>>()
    );
};

mod opaque {
    use super::OpaqueRLookupRow;

    /// An extension trait for convenience methods attached to `QueryError` for
    /// using it in an FFI context as an opaque sized type.
    pub trait RLookupRowExt {
        /// Converts a `RLookupRow` into an [`OpaqueRLookupRow`].
        fn into_opaque(self) -> OpaqueRLookupRow;

        /// Converts a [`RLookupRow`] reference into an `*const OpaqueRLookupRow`.
        fn as_opaque_ptr(&self) -> *const OpaqueRLookupRow;

        /// Converts a [`RLookupRow`] mutable reference into an
        /// `*mut OpaqueRLookupRow`.
        fn as_opaque_mut_ptr(&mut self) -> *mut OpaqueRLookupRow;

        /// Converts an [`OpaqueRLookupRow`] back to an [`RLookupRow`].
        ///
        /// # Safety
        ///
        /// This value must have been created via [`QueryErrorExt::into_opaque`].
        unsafe fn from_opaque(opaque: OpaqueRLookupRow) -> Self;

        /// Converts a const pointer to a [`OpaqueRLookupRow`] to a reference to a
        /// [`QueryError`].
        ///
        /// # Safety
        ///
        /// The pointer itself must have been created via
        /// [`RLookupRowExt::as_opaque_ptr`], as the alignment of the value
        /// pointed to by `opaque` must also be an alignment-compatible address for
        /// a [`RLookupRow`].
        unsafe fn from_opaque_ptr<'a>(opaque: *const OpaqueRLookupRow) -> Option<&'a Self>;

        /// Converts a mutable pointer to a [`OpaqueQueryError`] to a mutable
        /// reference to a [`RLookupRow`].
        ///
        /// # Safety
        ///
        /// The pointer itself must have been created via
        /// [`RLookupRowExt::as_opaque_mut_ptr`], as the alignment of the value
        /// pointed to by `opaque` must also be an alignment-compatible address for
        /// a [`RLookupRow`].
        unsafe fn from_opaque_mut_ptr<'a>(opaque: *mut OpaqueRLookupRow) -> Option<&'a mut Self>;
    }

    impl RLookupRowExt for rlookup::RLookupRow<'_, value::RSValueFFI> {
        fn into_opaque(self) -> OpaqueRLookupRow {
            // Safety: `OpaqueRLookupRow` is defined as a `MaybeUninit` slice of
            // bytes with the same size and alignment as `RLookupRow`, so any valid
            // `RLookupRow` has a bit pattern which is a valid `OpaqueRLookupRow`.
            unsafe { std::mem::transmute(self) }
        }

        fn as_opaque_ptr(&self) -> *const OpaqueRLookupRow {
            std::ptr::from_ref(&self).cast()
        }

        fn as_opaque_mut_ptr(&mut self) -> *mut OpaqueRLookupRow {
            std::ptr::from_mut(self).cast()
        }

        unsafe fn from_opaque(opaque: OpaqueRLookupRow) -> Self {
            // Safety: see trait's safety requirement.
            unsafe { std::mem::transmute(opaque) }
        }

        unsafe fn from_opaque_ptr<'a>(opaque: *const OpaqueRLookupRow) -> Option<&'a Self> {
            // Safety: see trait's safety requirement.
            unsafe { opaque.cast::<Self>().as_ref() }
        }

        unsafe fn from_opaque_mut_ptr<'a>(opaque: *mut OpaqueRLookupRow) -> Option<&'a mut Self> {
            // Safety: see trait's safety requirement.
            unsafe { opaque.cast::<Self>().as_mut() }
        }
    }
}

use crate::row::opaque::RLookupRowExt;

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
    //todo Safety: ensured by caller (2.)
    let row = row.expect("row must not be null").as_ptr();
    let row: &mut rlookup::RLookupRow<'_, value::RSValueFFI> =
        unsafe { RLookupRowExt::from_opaque_mut_ptr(row as *mut _).expect("row is null") };

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
    // Safety: ensured by caller (2.)
    let row = row.expect("`row` must not be null").as_ptr();
    let row: &mut rlookup::RLookupRow<'_, value::RSValueFFI> =
        unsafe { RLookupRowExt::from_opaque_mut_ptr(row as *mut _).expect("row is null") };
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
unsafe extern "C" fn RLookupRow_Wipe(row: Option<NonNull<OpaqueRLookupRow>>) {
    // Safety: ensured by caller (1.)
    let row = row.expect("row must not be null").as_ptr();
    let row: &mut rlookup::RLookupRow<'_, value::RSValueFFI> =
        unsafe { RLookupRowExt::from_opaque_mut_ptr(row as *mut _).expect("row is null") };
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
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow.
    let row = row.expect("row must not be null").as_ptr();
    let row: &mut rlookup::RLookupRow<'_, value::RSValueFFI> =
        unsafe { RLookupRowExt::from_opaque_mut_ptr(row as *mut _).expect("row is null") };
    row.reset_dyn_values();
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
    src: Option<NonNull<OpaqueRLookupRow>>,
    dst: Option<NonNull<OpaqueRLookupRow>>,
) {
    debug_assert_ne!(src, dst, "`src` and `dst` must not be the same");
    // Safety: ensured by caller (1.)
    let lookup = unsafe { lookup.as_ref().expect("`lookup` must not be null") };
    // Safety: ensured by caller (2.)
    let src = src.expect("`src` must not be null").as_ptr();
    let src: &mut rlookup::RLookupRow<'_, value::RSValueFFI> =
        unsafe { RLookupRowExt::from_opaque_mut_ptr(src as *mut _).expect("src is null") };
    // Safety: ensured by caller (3.)
    let dst = dst.expect("`dst` must not be null").as_ptr();
    let dst: &mut rlookup::RLookupRow<'_, value::RSValueFFI> =
        unsafe { RLookupRowExt::from_opaque_mut_ptr(dst as *mut _).expect("dst is null") };
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
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow.
    let src_row = src_row.expect("src_row must not be null").as_ptr();
    let src_row: &rlookup::RLookupRow<'_, value::RSValueFFI> =
        unsafe { RLookupRowExt::from_opaque_ptr(src_row).expect("src_row is null") };
    let src_lookup = unsafe { src_lookup.expect("src_lookup must not be null").as_ref() };
    let dst_row = dst_row.expect("dst_row must not be null").as_ptr();
    let dst_row: &mut rlookup::RLookupRow<'_, value::RSValueFFI> =
        unsafe { RLookupRowExt::from_opaque_mut_ptr(dst_row as *mut _).expect("dst_row is null") };
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
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow.
    let row = row.expect("row must not be null").as_ptr();
    let row: &rlookup::RLookupRow<'_, value::RSValueFFI> =
        unsafe { RLookupRowExt::from_opaque_ptr(row).expect("row is null") };
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
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow.
    let row = row.expect("row must not be null").as_ptr();
    let row: &mut rlookup::RLookupRow<'_, value::RSValueFFI> =
        unsafe { RLookupRowExt::from_opaque_mut_ptr(row as *mut _).expect("row is null") };
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
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow.
    let row = row.expect("row must not be null").as_ptr();
    let row: &rlookup::RLookupRow<'_, value::RSValueFFI> =
        unsafe { RLookupRowExt::from_opaque_ptr(row).expect("row is null") };
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
    // Safety: The caller has to ensure that the pointer is valid and points to a properly initialized RLookupRow.
    let row = row.expect("row must not be null").as_ptr();
    let row: &rlookup::RLookupRow<'_, value::RSValueFFI> =
        unsafe { RLookupRowExt::from_opaque_ptr(row).expect("row is null") };
    // Safety: The caller has to ensure that the pointer is kept around
    row.num_dyn_values()
}
