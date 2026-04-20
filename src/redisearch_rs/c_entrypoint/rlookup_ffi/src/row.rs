/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use libc::size_t;
use query_error::QueryError;
use rlookup::{OpaqueRLookupRow, RLookup, RLookupKey, RLookupRow};
use std::{
    cmp::Ordering,
    ffi::{CStr, c_char, c_int},
    mem::{self, ManuallyDrop},
    ptr::NonNull,
    slice,
};
use value::comparison::cmp_fields;
use value::{RsValue, SharedRsValue};

const SORTASCMAP_MAXFIELDS: usize = 8;

/// Returns a newly created [`RLookupRow`].
#[unsafe(no_mangle)]
pub extern "C" fn RLookupRow_New() -> OpaqueRLookupRow {
    RLookupRow::new().into_opaque()
}

/// Writes a key to the row but increments the value reference count before writing it thus having shared ownership.
///
/// # Safety
///
/// 1. `key` must be a [valid], non-null pointer to an [`RLookupKey`].
/// 2. `row` must be a [valid], non-null pointer to an [`RLookupRow`].
/// 3. `value` must be a [valid], non-null pointer to an [`RsValue`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_WriteKey(
    key: *const RLookupKey,
    row: Option<NonNull<OpaqueRLookupRow>>,
    value: Option<NonNull<RsValue>>,
) {
    // Safety: ensured by caller (1.)
    let key = unsafe { key.as_ref() }.expect("Key must not be null");

    // Safety: ensured by caller (2.)
    let row = unsafe { RLookupRow::from_opaque_non_null(row.expect("`row` must not be null")) };

    let value = value.expect("value must not be null").as_ptr().cast_const();

    // this method does not take ownership of `value` so we must take care not to drop it at the end of the scope
    // (therefore the `ManuallyDrop`). Instead we explicitly clone the value before inserting it below.
    // Safety: ensured by caller (3.)
    let value = ManuallyDrop::new(unsafe { SharedRsValue::from_raw(value) });

    row.write_key(key, ManuallyDrop::into_inner(value.clone()));
}

/// Writes a key to the row without incrementing the value reference count, thus taking ownership of the value.
///
/// # Safety
///
/// 1. `key` must be a [valid], non-null pointer to an [`RLookupKey`].
/// 2. `row` must be a [valid], non-null pointer to an [`RLookupRow`].
/// 3. `value` must be a [valid], non-null pointer to an [`RsValue`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_WriteOwnKey(
    key: *const RLookupKey,
    row: Option<NonNull<OpaqueRLookupRow>>,
    value: Option<NonNull<RsValue>>,
) {
    // Safety: ensured by caller (1.)
    let key = unsafe { key.as_ref() }.expect("`key` must not be null");

    // Safety: ensured by caller (2.)
    let row = unsafe { RLookupRow::from_opaque_non_null(row.expect("`row` must not be null")) };

    let value = value.expect("value must not be null").as_ptr().cast_const();

    // Safety: ensured by caller (3.)
    let value = unsafe { SharedRsValue::from_raw(value) };

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
pub unsafe extern "C" fn RLookupRow_Wipe(row: Option<NonNull<OpaqueRLookupRow>>) {
    // Safety: ensured by caller (1.)
    let row = unsafe { RLookupRow::from_opaque_non_null(row.expect("`row` must not be null")) };

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
pub unsafe extern "C" fn RLookupRow_Reset(row: Option<NonNull<OpaqueRLookupRow>>) {
    // Safety: ensured by caller (1.)
    let row = unsafe { RLookupRow::from_opaque_non_null(row.expect("`row` must not be null")) };

    row.reset_dyn_values();
}

/// Move data from the source row to the destination row. The source row is cleared.
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
pub unsafe extern "C-unwind" fn RLookupRow_MoveFieldsFrom(
    lookup: *const RLookup,
    src_row: Option<NonNull<OpaqueRLookupRow>>,
    dst_row: Option<NonNull<OpaqueRLookupRow>>,
) {
    debug_assert_ne!(src_row, dst_row, "`src` and `dst` must not be the same");

    // Safety: ensured by caller (1.)
    let lookup = unsafe { lookup.as_ref().expect("`lookup` must not be null") };

    // Safety: ensured by caller (2.)
    let src = unsafe { RLookupRow::from_opaque_non_null(src_row.expect("`src` must not be null")) };

    // Safety: ensured by caller (3.)
    let dst = unsafe { RLookupRow::from_opaque_non_null(dst_row.expect("`dst` must not be null")) };

    dst.move_fields_from(src, lookup);
}

/// Write a value by-name to the lookup table. This is useful for 'dynamic' keys
/// for which it is not necessary to use the boilerplate of getting an explicit
/// key.
///
/// Ownership of `name` remains with the caller, this function will make a copy if required.
///
/// Like [`RLookupRow_WriteByNameOwned`], but increases the refcount.
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to an [`RLookup`].
/// 2. The memory pointed to by `name` must contain a valid null terminator at the
///    end of the string.
/// 3. `name` must be [valid] for reads of `name_len` bytes up to and including the null terminator.
///    This means in particular:
///     1. `name_len` must be same as `strlen(name)`
///     2. The entire memory range of this cstr must be contained within a single allocation!
///     3. `name` must be non-null even for a zero-length cstr.
/// 4. `row` must be a [valid], non-null pointer to an [`RLookupRow`].
/// 5. `value` must be a [valid], non-null pointer to an [`RsValue`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookupRow_WriteByName<'a>(
    lookup: Option<NonNull<RLookup<'a>>>,
    name: *const c_char,
    name_len: size_t,
    row: Option<NonNull<OpaqueRLookupRow>>,
    value: Option<NonNull<RsValue>>,
) {
    // Safety: ensured by caller (1.)
    let lookup = unsafe { lookup.expect("lookup must not be null").as_mut() };

    // Safety: ensured by caller (2., 3.)
    let name = unsafe {
        // `name_len` is a value as returned by `strlen` and therefore **does not**
        // include the null terminator (that is why we do `name_len + 1` below)
        let bytes = { slice::from_raw_parts(name.cast::<u8>(), name_len + 1) };

        CStr::from_bytes_with_nul(bytes).expect("unable to create cstr from name")
    };

    // Safety: ensured by caller (4.)
    let row = unsafe { RLookupRow::from_opaque_non_null(row.expect("`row` must not be null")) };

    let value = value.expect("value must not be null").as_ptr().cast_const();

    // Safety: ensured by caller (5.)
    let value = unsafe { SharedRsValue::from_raw(value) };

    // In order to increase the refcount, we first clone `value` (which increases the refcount)
    // and move the clone into the function.
    // We then make sure the original `value` is not dropped (which would decrease the refcount again)
    // by giving it to `mem::forget()`.
    row.write_key_by_name(lookup, name, value.clone());
    mem::forget(value);
}

/// Write a value by-name to the lookup table. This is useful for 'dynamic' keys
/// for which it is not necessary to use the boilerplate of getting an explicit
/// key.
///
/// Ownership of `name` remains with the caller, this function will make a copy if required.
///
/// Like [`RLookupRow_WriteByName`], but does not affect the refcount.
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to an [`RLookup`].
/// 2. The memory pointed to by `name` must contain a valid null terminator at the
///    end of the string.
/// 3. `name` must be [valid] for reads of `name_len` bytes up to and including the null terminator.
///    This means in particular:
///     1. `name_len` must be same as `strlen(name)`
///     2. The entire memory range of this cstr must be contained within a single allocation!
///     3. `name` must be non-null even for a zero-length cstr.
/// 4. `row` must be a [valid], non-null pointer to an [`RLookupRow`].
/// 5. `value` must be a [valid], non-null pointer to an [`RsValue`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookupRow_WriteByNameOwned<'a>(
    lookup: Option<NonNull<RLookup<'a>>>,
    name: *const c_char,
    name_len: size_t,
    row: Option<NonNull<OpaqueRLookupRow>>,
    value: Option<NonNull<RsValue>>,
) {
    // Safety: ensured by caller (1.)
    let lookup = unsafe { lookup.expect("lookup must not be null").as_mut() };

    // Safety: ensured by caller (2., 3.)
    let name = unsafe {
        // `name_len` is a value as returned by `strlen` and therefore **does not**
        // include the null terminator (that is why we do `name_len + 1` below)
        let bytes = { slice::from_raw_parts(name.cast::<u8>(), name_len + 1) };

        CStr::from_bytes_with_nul(bytes).expect("unable to create cstr from name")
    };

    // Safety: ensured by caller (4.)
    let row = unsafe { RLookupRow::from_opaque_non_null(row.expect("`row` must not be null")) };

    let value = value.expect("value must not be null").as_ptr().cast_const();

    // Safety: ensured by caller (5.)
    let value = unsafe { SharedRsValue::from_raw(value) };

    // 'value' is moved directly into the function without affecting its refcount.
    row.write_key_by_name(lookup, name, value);
}

/// Write fields from a source row into this row.
///
/// Iterate through the source lookup keys, if it finds a corresponding key in the destination
/// lookup by name, then it's value is written to this row as a destination.
///
/// If a source key has no value in the source row, it is skipped.
///
/// If a source key is not found in the destination lookup the function will either create it or panic
/// depending on the value of `create_missing_keys`.
///
/// # Safety
///
/// 1. `src_row` must be a [valid], non-null pointer to an [`RLookupRow`].
/// 2. `src_lookup` must be a [valid], non-null pointer to an [`RLookup`].
/// 3. `dst_row` must be a [valid], non-null pointer to an [`RLookupRow`].
/// 4. `dst_lookup` must be a [valid], non-null pointer to an [`RLookup`].
/// 5. `src_row` and `dst_row` must not point to the same [`RLookupRow`].
/// 6. `src_lookup` and `dst_lookup` must not point to the same [`RLookup`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C-unwind" fn RLookupRow_WriteFieldsFrom<'a>(
    src_row: *const OpaqueRLookupRow,
    src_lookup: *const RLookup<'a>,
    dst_row: Option<NonNull<OpaqueRLookupRow>>,
    dst_lookup: Option<NonNull<RLookup<'a>>>,
    create_missing_keys: bool,
) {
    let dst_row = dst_row.unwrap();

    // Safety: ensured by caller (4.)
    let dst_lookup = unsafe { dst_lookup.unwrap().as_mut() };

    // We're doing the asserts here in the middle to avoid extra type conversions.
    assert!(
        src_row != dst_row.as_ptr(),
        "`src_row` and `dst_row` must not be the same"
    );
    assert_ne!(
        src_lookup, dst_lookup,
        "`src_lookup` and `dst_lookup` must not be the same"
    );

    // Safety: ensured by caller (1.)
    let src_row = unsafe { RLookupRow::from_opaque_ptr(src_row).unwrap() };

    // Safety: ensured by caller (3.)
    let dst_row = unsafe { RLookupRow::from_opaque_non_null(dst_row) };

    // Safety: ensured by caller (2.)
    let src_lookup = unsafe { src_lookup.as_ref().unwrap() };

    dst_row.copy_fields_from(dst_lookup, src_row, src_lookup, create_missing_keys);
}

/// Retrieves an item from the given `RLookupRow` based on the provided `RLookupKey`.
///
/// The function first checks for dynamic values, and if not found, it checks the sorting vector
/// if the `SvSrc` flag is set in the key.
///
/// If the item is not found in either location, it returns a NULL pointer.
///
/// # Safety
///
/// 1. `key` must be a [valid], non-null pointer to an [`RLookupKey`].
/// 2. `row` must be a [valid], non-null pointer to an [`RLookupRow`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookupRow_Get(
    key: *const RLookupKey,
    row: *const OpaqueRLookupRow,
) -> Option<NonNull<RsValue>> {
    // Safety: ensured by caller (1.)
    let key = unsafe { &*key };

    // Safety: ensured by caller (2.)
    let row = unsafe { RLookupRow::from_opaque_ptr_unchecked(row) };

    row.get(key).map(|x| {
        // Safety: `SharedRsValue` is a valid.
        unsafe { NonNull::new_unchecked(x.as_ptr().cast_mut()) }
    })
}

/// A read-only view of a sorting vector's values, returned by value to C.
///
/// Layout-compatible with [`sorting_vector::RSSortingVector`] but uses `*const` values
/// since this is a borrowed, non-owning view.
#[repr(C)]
pub struct RSSortingVectorSlice {
    /// Pointer to the array of [`SharedRsValue`] values.
    /// When `len == 0` this is a dangling pointer — **not** null. Callers must check `len`.
    pub values: *const *const RsValue,
    /// Number of elements in the array. Zero means no sorting vector is set.
    pub len: size_t,
}

/// Returns a borrowed view of the sorting vector for the row.
///
/// If the row has no sorting vector, returns a slice with `len == 0` and a dangling `values`
/// pointer. Callers must check `len`, not `values`, to detect the empty case.
///
/// # Safety
///
/// 1. `row` must be a [valid], non-null pointer to an [`RLookupRow`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookupRow_GetSortingVector(
    row: *const OpaqueRLookupRow,
) -> RSSortingVectorSlice {
    // Safety: ensured by caller (1.)
    let row = unsafe { RLookupRow::from_opaque_ptr(row).unwrap() };

    let slice = row.sorting_vector();
    RSSortingVectorSlice {
        // Even though slice is a `&[SharedRsValue]`, a `SharedRsValue` is actually a
        // `*const RsValue`. `SharedRsValue` is used within Rust code and
        // `*const RsValue` is used to interface with C. We can safely cast here.
        values: slice.as_ptr().cast(),
        len: slice.len(),
    }
}

/// Sets the sorting vector for the row.
///
/// # Safety
///
/// 1. `row` must be a [valid], non-null pointer to an [`RLookupRow`].
/// 2. `sv` must be either null or a [valid] pointer to an [`sorting_vector::RSSortingVector`].
///    The pointed-to vector must remain valid for the lifetime of the row.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookupRow_SetSortingVector(
    row: Option<NonNull<OpaqueRLookupRow>>,
    sv: *const sorting_vector::RSSortingVector,
) {
    // Safety: ensured by caller (1.)
    let row = unsafe { RLookupRow::from_opaque_non_null(row.expect("`row` must not be null")) };

    // Safety: ensured by caller (2.)
    let sv_ref = unsafe { sv.as_ref() };

    row.set_sorting_vector(sv_ref);
}

/// Compares two search results by the given sort keys, returning a negative, zero, or positive
/// value.
///
/// The comparison loop runs entirely in Rust via [`cmp_fields`], avoiding per-key FFI
/// crossings for value lookups. When all fields are equal, breaks the tie by document ID using
/// the last key's ascending flag.
///
/// # Safety
///
/// 1. `keys` must point to an array of at least `nkeys` valid, non-null `RLookupKey` pointers.
/// 2. `h1` and `h2` must be valid, non-null pointers to a `SearchResult`.
/// 3. `qerr`, when non-null, must be a valid, writable pointer to a `QueryError`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SearchResult_CmpByFields(
    keys: *const *const RLookupKey,
    nkeys: size_t,
    h1: *const search_result::SearchResult<'_>,
    h2: *const search_result::SearchResult<'_>,
    ascend_map: u64,
    qerr: *mut QueryError,
) -> c_int {
    let nkeys = nkeys.min(SORTASCMAP_MAXFIELDS);
    // SAFETY: ensured by caller (1.)
    let keys = unsafe { slice::from_raw_parts(keys, nkeys) };
    // SAFETY: &RLookupKey and *const RLookupKey have identical layout.
    let keys: &[&RLookupKey] = unsafe { mem::transmute(keys) };
    // SAFETY: ensured by caller (2.)
    let h1 = unsafe { &*h1 };
    // SAFETY: ensured by caller (2.)
    let h2 = unsafe { &*h2 };
    // SAFETY: ensured by caller (3.)
    let qerr = unsafe { qerr.as_mut() };

    let row1 = h1.row_data();
    let row2 = h2.row_data();
    let pairs = keys
        .iter()
        .map(|&k| (row1.get(k).map(|v| &**v), row2.get(k).map(|v| &**v)));

    let ord = cmp_fields(pairs, ascend_map, qerr);

    match ord {
        Ordering::Less => -1,
        Ordering::Greater => 1,
        Ordering::Equal => {
            // Tiebreak by docid — ascending uses the last key's flag,
            // matching the original C loop where `ascending` retains its last value.
            let ascending = nkeys > 0 && (ascend_map & (1u64 << (nkeys - 1))) != 0;
            let rc: c_int = if h1.doc_id() < h2.doc_id() { -1 } else { 1 };
            if ascending { -rc } else { rc }
        }
    }
}
