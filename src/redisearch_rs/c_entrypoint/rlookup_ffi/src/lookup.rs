/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use libc::size_t;
use rlookup::{IndexSpecCache, RLookup, RLookupKey, RLookupKeyFlags};
use std::{
    ffi::{CStr, c_char},
    ptr::NonNull,
    slice,
};

/// Get a RLookup key for a given name.
///
/// A key is returned only if it's already in the lookup table (available from the
/// pipeline upstream), it is part of the index schema and is sortable (and then it is created), or
/// if the lookup table accepts unresolved keys.
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to a `RLookup`
/// 2. The memory pointed to by `name` must contain a valid nul terminator at the
///    end of the string.
/// 3. `name` must be [valid] for reads of bytes up to and including the nul terminator.
///    This means in particular:
///     1. The entire memory range of this `CStr` must be contained within a single allocation!
///     2. `name` must be non-null even for a zero-length cstr.
/// 4. The memory referenced by the returned `CStr` must not be mutated for
///    the duration of lifetime `'a`.
/// 5. The nul terminator must be within `isize::MAX` from `name`
/// 6. All bits set in `flags` must correspond to a value of the enum.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_GetKey_Read<'a>(
    lookup: Option<NonNull<RLookup<'a>>>,
    name: *const c_char,
    flags: u32,
) -> Option<NonNull<RLookupKey<'a>>> {
    // Safety: ensured by caller (1.)
    let lookup = unsafe { lookup.unwrap().as_mut() };

    // Safety: ensured by caller (2., 3., 4., 5.)
    let name = unsafe { CStr::from_ptr(name) };

    let flags = RLookupKeyFlags::from_bits(flags).unwrap();

    lookup.get_key_read(name, flags).map(NonNull::from)
}

/// Get a RLookup key for a given name.
///
/// A key is returned only if it's already in the lookup table (available from the
/// pipeline upstream), it is part of the index schema and is sortable (and then it is created), or
/// if the lookup table accepts unresolved keys.
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to a `RLookup`
/// 2. The memory pointed to by `name` must contain a valid nul terminator at the
///    end of the string.
/// 3. `name` must be [valid] for reads of `name_len` bytes up to and including the nul terminator.
///    This means in particular:
///     1. `name_len` must be same as `strlen(name)`
///     2. The entire memory range of this `CStr` must be contained within a single allocation!
///     3. `name` must be non-null even for a zero-length cstr.
/// 4. The memory referenced by the returned `CStr` must not be mutated for
///    the duration of lifetime `'a`.
/// 5. The nul terminator must be within `isize::MAX` from `name`
/// 6. All bits set in `flags` must correspond to a value of the enum.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_GetKey_ReadEx<'a>(
    lookup: Option<NonNull<RLookup<'a>>>,
    name: *const c_char,
    name_len: size_t,
    flags: u32,
) -> Option<NonNull<RLookupKey<'a>>> {
    // Safety: ensured by caller (1.)
    let lookup = unsafe { lookup.unwrap().as_mut() };

    // Safety: ensured by caller (2., 3., 4., 5.)
    let name = unsafe {
        // `name_len` is a value as returned by `strlen` and therefore **does not**
        // include the null terminator (that is why we do `name_len + 1` below)
        let bytes = slice::from_raw_parts(name.cast::<u8>(), name_len + 1);

        CStr::from_bytes_with_nul(bytes).unwrap()
    };

    let flags = RLookupKeyFlags::from_bits(flags).unwrap();

    lookup.get_key_read(name, flags).map(NonNull::from)
}

/// Get a RLookup key for a given name.
///
/// A key is created and returned only if it's NOT in the lookup table, unless the
/// override flag is set.
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to a `RLookup`
/// 2. The memory pointed to by `name` must contain a valid nul terminator at the
///    end of the string.
/// 3. `name` must be [valid] for reads of bytes up to and including the nul terminator.
///    This means in particular:
///     1. The entire memory range of this `CStr` must be contained within a single allocation!
///     2. `name` must be non-null even for a zero-length cstr.
/// 4. The memory referenced by the returned `CStr` must not be mutated for
///    the duration of lifetime `'a`.
/// 5. The nul terminator must be within `isize::MAX` from `name`
/// 6. All bits set in `flags` must correspond to a value of the enum.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_GetKey_Write<'a>(
    lookup: Option<NonNull<RLookup<'a>>>,
    name: *const c_char,
    flags: u32,
) -> Option<NonNull<RLookupKey<'a>>> {
    // Safety: ensured by caller (1.)
    let lookup = unsafe { lookup.unwrap().as_mut() };

    // Safety: ensured by caller (2., 3., 4., 5.)
    let name = unsafe { CStr::from_ptr(name) };

    let flags = RLookupKeyFlags::from_bits(flags).unwrap();

    lookup.get_key_write(name, flags).map(NonNull::from)
}

/// Get a RLookup key for a given name.
///
/// A key is created and returned only if it's NOT in the lookup table, unless the
/// override flag is set.
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to a `RLookup`
/// 2. The memory pointed to by `name` must contain a valid nul terminator at the
///    end of the string.
/// 3. `name` must be [valid] for reads of `name_len` bytes up to and including the nul terminator.
///    This means in particular:
///     1. `name_len` must be same as `strlen(name)`
///     2. The entire memory range of this `CStr` must be contained within a single allocation!
///     3. `name` must be non-null even for a zero-length cstr.
/// 4. The memory referenced by the returned `CStr` must not be mutated for
///    the duration of lifetime `'a`.
/// 5. The nul terminator must be within `isize::MAX` from `name`
/// 6. All bits set in `flags` must correspond to a value of the enum.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_GetKey_WriteEx<'a>(
    lookup: Option<NonNull<RLookup<'a>>>,
    name: *const c_char,
    name_len: size_t,
    flags: u32,
) -> Option<NonNull<RLookupKey<'a>>> {
    // Safety: ensured by caller (1.)
    let lookup = unsafe { lookup.unwrap().as_mut() };

    // Safety: ensured by caller (2., 3., 4., 5.)
    let name = unsafe {
        // `name_len` is a value as returned by `strlen` and therefore **does not**
        // include the null terminator (that is why we do `name_len + 1` below)
        let bytes = slice::from_raw_parts(name.cast::<u8>(), name_len + 1);

        CStr::from_bytes_with_nul(bytes).unwrap()
    };

    let flags = RLookupKeyFlags::from_bits(flags).unwrap();

    lookup.get_key_write(name, flags).map(NonNull::from)
}

/// Get a RLookup key for a given name.
///
/// A key is created and returned only if it's NOT in the lookup table (unless the
/// override flag is set), and it is not already loaded. It will override an existing key if it was
/// created for read out of a sortable field, and the field was normalized. A sortable un-normalized
/// field counts as loaded.
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to a `RLookup`
/// 2. The memory pointed to by `name` and `field_name` must contain a valid nul terminator at the
///    end of the string.
/// 3. `name` and `field_name` must be [valid] for reads of bytes up to and including the nul terminator.
///    This means in particular:
///     1. The entire memory range of these `CStr` must be contained within a single allocation!
///     2. `name` and `field_name` must be non-null even for a zero-length cstr.
/// 4. The memory referenced by the returned `CStr` must not be mutated for
///    the duration of lifetime `'a`.
/// 5. The nul terminator must be within `isize::MAX` from `name` and `field_name`
/// 6. All bits set in `flags` must correspond to a value of the enum.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_GetKey_Load<'a>(
    lookup: Option<NonNull<RLookup<'a>>>,
    name: *const c_char,
    field_name: *const c_char,
    flags: u32,
) -> Option<NonNull<RLookupKey<'a>>> {
    // Safety: ensured by caller (1.)
    let lookup = unsafe { lookup.unwrap().as_mut() };

    // Safety: ensured by caller (2., 3., 4., 5.)
    let name = unsafe { CStr::from_ptr(name) };

    // Safety: ensured by caller (2., 3., 4., 5.)
    let field_name = unsafe { CStr::from_ptr(field_name) };

    let flags = RLookupKeyFlags::from_bits(flags).unwrap();

    lookup
        .get_key_load(name, field_name, flags)
        .map(NonNull::from)
}

/// Get a RLookup key for a given name.
///
/// A key is created and returned only if it's NOT in the lookup table (unless the
/// override flag is set), and it is not already loaded. It will override an existing key if it was
/// created for read out of a sortable field, and the field was normalized. A sortable un-normalized
/// field counts as loaded.
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to a `RLookup`
/// 2. The memory pointed to by `name` and `field_name` must contain a valid nul terminator at the
///    end of the string.
/// 3. `name` and `field_name` must be [valid] for reads of `name_len` bytes up to and including the nul terminator.
///    This means in particular:
///     1. `name_len` must be same as `strlen(name)`
///     2. The entire memory range of these `CStr` must be contained within a single allocation!
///     3. `name` and `field_name` must be non-null even for a zero-length cstr.
/// 4. The memory referenced by the returned `CStr` must not be mutated for
///    the duration of lifetime `'a`.
/// 5. The nul terminator must be within `isize::MAX` from `name` and `field_name`
/// 6. All bits set in `flags` must correspond to a value of the enum.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_GetKey_LoadEx<'a>(
    lookup: Option<NonNull<RLookup<'a>>>,
    name: *const c_char,
    name_len: size_t,
    field_name: *const c_char,
    flags: u32,
) -> Option<NonNull<RLookupKey<'a>>> {
    // Safety: ensured by caller (1.)
    let lookup = unsafe { lookup.unwrap().as_mut() };

    // Safety: ensured by caller (2., 3., 4., 5.)
    let name = unsafe {
        // `name_len` is a value as returned by `strlen` and therefore **does not**
        // include the null terminator (that is why we do `name_len + 1` below)
        let bytes = slice::from_raw_parts(name.cast::<u8>(), name_len + 1);

        CStr::from_bytes_with_nul(bytes).unwrap()
    };

    // Safety: ensured by caller (2., 3., 4., 5.)
    let field_name = unsafe { CStr::from_ptr(field_name) };

    let flags = RLookupKeyFlags::from_bits(flags).unwrap();

    lookup
        .get_key_load(name, field_name, flags)
        .map(NonNull::from)
}

/// Initialize the lookup. If cache is provided, then it will be used as an
/// alternate source for lookups whose fields are absent.
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to a `RLookup`
/// 2. `spcache` must be a [valid] pointer to a [`ffi::IndexSpecCache`]
/// 3. The [`ffi::IndexSpecCache`] being pointed MUST NOT get mutated
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_Init(
    lookup: Option<NonNull<RLookup<'_>>>,
    spcache: Option<NonNull<ffi::IndexSpecCache>>,
) {
    // Safety: ensured by caller (1.)
    let lookup = unsafe { lookup.unwrap().as_mut() };
    let spcache = spcache.map(|spcache| {
        // Safety: ensured by caller (2. & 3.)
        unsafe { IndexSpecCache::from_raw(spcache) }
    });

    lookup.init(spcache);
}

/// Releases any resources created by this lookup object. Note that if there are
/// lookup keys created with RLOOKUP_F_NOINCREF, those keys will no longer be
/// valid after this call!
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to a `RLookup`
/// 2. `lookup` **must not** be used again after this function is called.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_Cleanup(lookup: Option<NonNull<RLookup<'_>>>) {
    // Safety: ensured by caller (1.,2.)
    unsafe { lookup.unwrap().drop_in_place() };
}

/// Add all non-overridden keys from `lookup` to `dest`.
///
/// For each key in `lookup`, check if it already exists *by name*.
/// - If it does the `flag` argument controls the behaviour (skip with `RLookupKeyFlags::empty()`, override with `RLookupKeyFlag::Override`).
/// - If it doesn't a new key will be created.
///
/// Flag handling:
///  * - Preserves persistent source key properties (F_SVSRC, F_HIDDEN, F_EXPLICITRETURN, etc.)
///  * - Filters out transient flags from source keys (F_OVERRIDE, F_FORCE_LOAD)
///  * - Respects caller's control flags for behavior (F_OVERRIDE, F_FORCE_LOAD, etc.)
///  * - Target flags = caller_flags | (source_flags & ~RLOOKUP_TRANSIENT_FLAGS)
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to a [`RLookup`]
/// 2. `dest` must be a [valid], non-null pointer to a [`RLookup`]
/// 3. All bits set in `flags` must correspond to a value of [`RLookupKeyFlags`]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_AddKeysFrom<'a>(
    lookup: Option<NonNull<RLookup<'a>>>,
    dest: Option<NonNull<RLookup<'a>>>,
    flags: u32,
) {
    // Safety: must be ensured by caller (1)
    let lookup = unsafe {
        lookup
            .expect("RLookup_AddKeysFrom: lookup is null")
            .as_ref()
    };

    // Safety: must be ensured by caller (2)
    let dest = unsafe { dest.expect("RLookup_AddKeysFrom: dest is null").as_mut() };

    // Safety: must be ensured by caller (3)
    let flags = RLookupKeyFlags::from_bits(flags).expect("RLookup_AddKeysFrom: invalid flags");

    dest.add_keys_from(lookup, flags);
}

/// Find a [`RLookupKey`] in `lookup`' by its `name`.
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to a [`RLookup`]
/// 2. The memory pointed to by `name` must contain a valid nul terminator at the end of the string
/// 3. `name` must be [valid] for reads of `name_len` bytes up to and including the nul terminator.
///    This means in particular:
///     1. `name_len` must be same as `strlen(name)`
///     2. The entire memory range of this `CStr` must be contained within a single allocation!
///     3. `name` must be non-null even for a zero-length cstr.
/// 4. The memory referenced by the returned `CStr` must not be mutated for
///    the duration of lifetime `'a`.
/// 5. The nul terminator must be within `isize::MAX` from `name` and `field_name`
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_FindKey<'a>(
    lookup: Option<NonNull<RLookup<'a>>>,
    name: *const c_char,
    name_len: size_t,
) -> Option<NonNull<RLookupKey<'a>>> {
    // Safety: must be ensured by caller (1)
    let lookup = unsafe { lookup.expect("RLookup_FindKey: lookup is null").as_ref() };

    // Safety: must be ensured by caller (2, 3, 4, 5)
    let name = unsafe {
        // `name_len` is a value as returned by `strlen` and therefore **does not**
        // include the null terminator (that is why we do `name_len + 1` below)
        let bytes = slice::from_raw_parts(name.cast::<u8>(), name_len + 1);

        CStr::from_bytes_with_nul(bytes).unwrap()
    };

    let cursor = lookup.find_key_by_name(name)?;
    let rlk = cursor.current()?;

    Some(NonNull::from_ref(rlk))
}
