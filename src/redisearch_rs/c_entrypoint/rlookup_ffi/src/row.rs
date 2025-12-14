/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use libc::size_t;
use rlookup::{RLookup, RLookupKey};
use std::{
    ffi::{CStr, c_char},
    mem::{self, ManuallyDrop},
    ptr::NonNull,
    slice,
};
use value::RSValueFFI;

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
    lookup: *const RLookup<'static>,
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

    dst.move_fields_from(src, lookup);
}

/// Write a value by-name to the lookup table. This is useful for 'dynamic' keys
/// for which it is not necessary to use the boilerplate of getting an explicit
/// key.
///
/// Like [`RLookupRow_WriteByNameOwned`], but increases the refcount.
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to an [`RLookup`].
/// 2. The memory pointed to by `name` must contain a valid nul terminator at the
///    end of the string.
/// 3. `name` must be [valid] for reads of `name_len` bytes up to and including the nul terminator.
///    This means in particular:
///     1. `name_len` must be same as `strlen(name)`
///     2. The entire memory range of this cstr must be contained within a single allocation!
///     3. `name` must be non-null even for a zero-length cstr.
/// 4. `row` must be a [valid], non-null pointer to an [`RLookupRow`].
/// 5. `value` must be a [valid], non-null pointer to an [`ffi::RSValue`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookupRow_WriteByName(
    lookup: Option<NonNull<RLookup>>,
    name: *const c_char,
    name_len: size_t,
    row: Option<NonNull<RLookupRow>>,
    value: Option<NonNull<ffi::RSValue>>,
) {
    // Safety: ensured by caller (1.)
    let lookup = unsafe { lookup.unwrap().as_mut() };

    // Safety: ensured by caller (2., 3.)
    let name = unsafe {
        // `name_len` is a value as returned by `strlen` and therefore **does not**
        // include the null terminator (that is why we do `name_len + 1` below)
        let bytes = { slice::from_raw_parts(name.cast::<u8>(), name_len + 1) };

        CStr::from_bytes_with_nul(bytes).unwrap()
    };

    // Safety: ensured by caller (4.)
    let row = unsafe { row.unwrap().as_mut() };

    // Safety: ensured by caller (5.)
    let value = unsafe { RSValueFFI::from_raw(value.unwrap()) };

    // In order to increase the refcount, we first clone `value` (which increases the refcount)
    // and move the clone into the function.
    // We then make sure the original `value` is not dropped (which would decrease the refcount again).
    row.write_key_by_name(lookup, name, value.clone());
    mem::forget(value);
}

/// Write a value by-name to the lookup table. This is useful for 'dynamic' keys
/// for which it is not necessary to use the boilerplate of getting an explicit
/// key.
///
/// Like [`RLookupRow_WriteByName`], but does not affect the refcount.
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to an [`RLookup`].
/// 2. The memory pointed to by `name` must contain a valid nul terminator at the
///    end of the string.
/// 3. `name` must be [valid] for reads of `name_len` bytes up to and including the nul terminator.
///    This means in particular:
///     1. `name_len` must be same as `strlen(name)`
///     2. The entire memory range of this cstr must be contained within a single allocation!
///     3. `name` must be non-null even for a zero-length cstr.
/// 4. `row` must be a [valid], non-null pointer to an [`RLookupRow`].
/// 5. `value` must be a [valid], non-null pointer to an [`ffi::RSValue`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookupRow_WriteByNameOwned(
    lookup: Option<NonNull<RLookup<'_>>>,
    name: *const c_char,
    name_len: size_t,
    row: Option<NonNull<RLookupRow>>,
    value: Option<NonNull<ffi::RSValue>>,
) {
    // Safety: ensured by caller (1.)
    let lookup = unsafe { lookup.unwrap().as_mut() };

    // Safety: ensured by caller (2., 3.)
    let name = unsafe {
        // `name_len` is a value as returned by `strlen` and therefore **does not**
        // include the null terminator (that is why we do `name_len + 1` below)
        let bytes = { slice::from_raw_parts(name.cast::<u8>(), name_len + 1) };

        CStr::from_bytes_with_nul(bytes).unwrap()
    };

    // Safety: ensured by caller (4.)
    let row = unsafe { row.unwrap().as_mut() };

    // Safety: ensured by caller (5.)
    let value = unsafe { RSValueFFI::from_raw(value.unwrap()) };

    // 'value' is moved directly into the function without affecting its refcount.
    row.write_key_by_name(lookup, name, value);
}
