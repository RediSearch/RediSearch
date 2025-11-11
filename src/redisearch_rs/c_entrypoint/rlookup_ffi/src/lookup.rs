/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use c_ffi_utils::{as_mut_unchecked, as_ref_unchecked, canary::CanaryGuarded, expect_unchecked};
use libc::size_t;
use rlookup::{
    IndexSpecCache, RLookup as RLookupInternal, RLookupKey, RLookupKeyFlag, RLookupKeyFlags,
};
use std::{
    borrow::Cow,
    ffi::{CStr, c_char},
    ptr::NonNull,
    slice,
};

type RLookup<'a> = CanaryGuarded<RLookupInternal<'a>>;

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
    let lookup = unsafe { as_mut_unchecked!(lookup, "RLookup_GetKey_Read: lookup is null") };

    // Safety: ensured by caller (2., 3., 4., 5.)
    let name = unsafe { CStr::from_ptr(name) };

    // Safety: ensured by caller (6.)
    let flags = unsafe {
        expect_unchecked!(
            RLookupKeyFlags::from_bits(flags),
            "RLookup_GetKey_Read: Flags are invalid"
        )
    };

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
    let lookup = unsafe { as_mut_unchecked!(lookup, "RLookup_GetKey_ReadEx: lookup is null") };

    // Safety: ensured by caller (3.)
    // `name_len` is a value as returned by `strlen` and therefore **does not**
    // include the null terminator (that is why we do `name_len + 1` below)
    let bytes = unsafe { slice::from_raw_parts(name.cast::<u8>(), name_len + 1) };
    // Safety: ensured by caller (2., 5.)
    let name = unsafe {
        expect_unchecked!(
            CStr::from_bytes_with_nul(bytes),
            "RLookup_GetKey_ReadEx: name either lacks or has a null-terminator before the end of the sequence"
        )
    };

    // Safety: ensured by caller (6.)
    let flags = unsafe {
        expect_unchecked!(
            RLookupKeyFlags::from_bits(flags),
            "RLookup_GetKey_ReadEx: Flags are invalid"
        )
    };

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
    let lookup = unsafe { as_mut_unchecked!(lookup, "RLookup_GetKey_Write: lookup is null") };

    // Safety: ensured by caller (2., 3., 4., 5.)
    let name = unsafe { CStr::from_ptr(name) };

    // Safety: ensured by caller (6.)
    let flags = RLookupKeyFlags::from_bits(flags).expect("RLookup_GetKey_Write: Flags are invalid");
    let name = if flags.contains(RLookupKeyFlag::NameAlloc) {
        Cow::Owned(name.to_owned())
    } else {
        Cow::Borrowed(name)
    };

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
    let lookup = unsafe { as_mut_unchecked!(lookup, "RLookup_GetKey_WriteEx: lookup is null") };

    // Safety: ensured by caller (3.)
    // `name_len` is a value as returned by `strlen` and therefore **does not**
    // include the null terminator (that is why we do `name_len + 1` below)
    let bytes = unsafe { slice::from_raw_parts(name.cast::<u8>(), name_len + 1) };
    // Safety: ensured by caller (2., 5.)
    let name = unsafe {
        expect_unchecked!(
            CStr::from_bytes_with_nul(bytes),
            "RLookup_GetKey_WriteEx: name either lacks or has a null-terminator before the end of the sequence"
        )
    };

    // Safety: ensured by caller (6.)
    let flags = unsafe {
        expect_unchecked!(
            RLookupKeyFlags::from_bits(flags),
            "RLookup_GetKey_WriteEx: Flags are invalid"
        )
    };
    let name = if flags.contains(RLookupKeyFlag::NameAlloc) {
        Cow::Owned(name.to_owned())
    } else {
        Cow::Borrowed(name)
    };

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
    let lookup = unsafe { as_mut_unchecked!(lookup, "RLookup_GetKey_Load: lookup is null") };

    // Safety: ensured by caller (2., 3., 4., 5.)
    let name = unsafe { CStr::from_ptr(name) };

    // Safety: ensured by caller (2., 3., 4., 5.)
    let field_name = unsafe { CStr::from_ptr(field_name) };

    // Safety: ensured by caller (6.)
    let flags = unsafe {
        expect_unchecked!(
            RLookupKeyFlags::from_bits(flags),
            "RLookup_GetKey_Load: Flags are invalid"
        )
    };
    let name = if flags.contains(RLookupKeyFlag::NameAlloc) {
        Cow::Owned(name.to_owned())
    } else {
        Cow::Borrowed(name)
    };

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
    let lookup = unsafe { as_mut_unchecked!(lookup, "RLookup_GetKey_LoadEx: lookup is null") };

    // Safety: ensured by caller (3., 4.)
    // `name_len` is a value as returned by `strlen` and therefore **does not**
    // include the null terminator (that is why we do `name_len + 1` below)
    let bytes = unsafe { slice::from_raw_parts(name.cast::<u8>(), name_len + 1) };
    // Safety: ensured by caller (2., 5.)
    let name = unsafe {
        expect_unchecked!(
            CStr::from_bytes_with_nul(bytes),
            "RLookup_GetKey_LoadEx: name either lacks or has a null-terminator before the end of the sequence"
        )
    };

    // Safety: ensured by caller (2., 3., 4., 5.)
    let field_name = unsafe { CStr::from_ptr(field_name) };

    // Safety: ensured by caller (6.)
    let flags = unsafe {
        expect_unchecked!(
            RLookupKeyFlags::from_bits(flags),
            "RLookup_GetKey_LoadEx: Flags are invalid"
        )
    };
    let name = if flags.contains(RLookupKeyFlag::NameAlloc) {
        Cow::Owned(name.to_owned())
    } else {
        Cow::Borrowed(name)
    };

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
    let lookup = unsafe { as_mut_unchecked!(lookup, "RLookup_Init: lookup is null") };

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
    // Safety: ensured by caller (1.)
    let lookup = unsafe { expect_unchecked!(lookup) };
    // Safety: ensured by caller (2.)
    unsafe { lookup.drop_in_place() };
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
    let lookup = unsafe { as_ref_unchecked!(lookup, "RLookup_AddKeysFrom: lookup is null") };

    // Safety: must be ensured by caller (2)
    let dest = unsafe { as_mut_unchecked!(dest, "RLookup_AddKeysFrom: dest is null") };

    // Safety: must be ensured by caller (3)
    let flags = unsafe {
        expect_unchecked!(
            RLookupKeyFlags::from_bits(flags),
            "RLookup_AddKeysFrom: Flags are invalid"
        )
    };
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
    let lookup = unsafe { as_ref_unchecked!(lookup, "RLookup_FindKey: lookup is null") };

    // Safety: ensured by caller (3., 4.)
    // `name_len` is a value as returned by `strlen` and therefore **does not**
    // include the null terminator (that is why we do `name_len + 1` below)
    let bytes = unsafe { slice::from_raw_parts(name.cast::<u8>(), name_len + 1) };
    // Safety: ensured by caller (2., 5.)
    let name = unsafe {
        expect_unchecked!(
            CStr::from_bytes_with_nul(bytes),
            "RLookup_GetKey_LoadEx: name either lacks or has a null-terminator before the end of the sequence"
        )
    };

    let cursor = lookup.find_key_by_name(name)?;
    let rlk = cursor.current()?;

    Some(NonNull::from_ref(rlk))
}

/// Search the IndexSpecCache for a field by name.
///
/// A FieldSpec is returned if the provided lookup contains an IndexSpecCache and there is a field
/// with the given name.
///
/// # Safety
/// 1. `lookup` must be a [valid], non-null pointer to a `RLookup`
/// 2. The memory pointed to by `field_name` must contain a valid nul terminator at the end of the string.
/// 3. `field_name` must be [valid] for reads of bytes up to and including the nul terminator.
///    This means in particular:
///     1. The entire memory range of this `CStr` must be contained within a single allocation!
///     2. `field_name` must be non-null even for a zero-length cstr.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_FindFieldInSpecCache(
    lookup: Option<NonNull<RLookup>>,
    field_name: *const c_char,
) -> *const ffi::FieldSpec {
    // Safety: ensured by caller (1.)
    let lookup =
        unsafe { as_ref_unchecked!(lookup, "RLookup_FindFieldInSpecCache: lookup is null") };

    // Safety: ensured by caller (2., 3.)
    let field_name = unsafe { CStr::from_ptr(field_name) };

    lookup
        ._ffi_find_field_in_spec_cache(field_name)
        .map_or(std::ptr::null(), |fs| fs as *const ffi::FieldSpec)
}

/// Create a new RLookupKey and add it to the RLookup.
///
/// # Safety
/// 1. `lookup` must be a [valid], non-null pointer to a `RLookup
/// 2. The memory pointed to by `name` must contain a valid nul terminator at the end of the string.
/// 3. `name` must be [valid] for reads of `name_len` bytes up to and including the nul terminator.
///    This means in particular:
///     1. `name_len` must be same as `strlen(name)`
///     2. The entire memory range of this `CStr` must be contained within a single allocation!
///     3. `name` must be non-null even for a zero-length cstr.
/// 4. The given `flags` must correspond to a value of the enum `RLookupKeyFlags`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_CreateKey(
    lookup: Option<NonNull<RLookup>>,
    name: *const c_char,
    name_len: size_t,
    flags: u32,
) -> *mut RLookupKey {
    // Safety: ensured by caller (1.)
    let lookup = unsafe { as_mut_unchecked!(lookup, "RLookup_CreateKey: lookup is null") };

    // Safety: ensured by caller (3.)
    // `name_len` is a value as returned by `strlen` and therefore **does not**
    // include the null terminator (that is why we do `name_len + 1` below)
    let bytes = unsafe { slice::from_raw_parts(name.cast::<u8>(), name_len + 1) };
    // Safety: ensured by caller (2., 3.)
    let name = unsafe {
        expect_unchecked!(
            CStr::from_bytes_with_nul(bytes),
            "RLookup_GetKey_LoadEx: name either lacks or has a null-terminator before the end of the sequence"
        )
    };

    // Safety: must be ensured by caller (4)
    let flags = RLookupKeyFlags::from_bits(flags).unwrap();
    let name = if flags.contains(RLookupKeyFlag::NameAlloc) {
        Cow::Owned(name.to_owned())
    } else {
        Cow::Borrowed(name)
    };

    let rlk = lookup._ffi_new_key(name, flags);
    rlk as *mut RLookupKey
}

/// Create a new RLookup as a value type. We can use this in C code as the size and alignment is known at compile time.
#[allow(improper_ctypes_definitions)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_New_Value<'a>() -> RLookup<'a> {
    CanaryGuarded::new(RLookupInternal::new())
}

/// Create a new RLookup on the heap. The returned pointer must be freed with `RLookup_Free_Heap`.
///
/// # Safety
/// 1. The returned pointer must be freed with `RLookup_Free_Heap` and not used after that.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_New_Heap<'a>() -> *mut RLookup<'a> {
    Box::into_raw(Box::new(CanaryGuarded::new(RLookupInternal::new())))
}

/// Free a RLookup created with `RLookup_New_Heap`.
///
/// # Safety
/// 1. `lookup` must be a [valid] pointe to `RLookup` created with `RLookup_New_Heap`
/// 2. `lookup` **must not** be used again after this function is called.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_Free_Heap<'a>(lookup: Option<NonNull<RLookup<'a>>>) {
    if let Some(lookup) = lookup {
        // Safety: pointer is checked for null
        drop(unsafe { Box::from_raw(lookup.as_ptr()) });
    }
}

/// Set the path of a RLookupKey. This is not doable via direct field access from C, as Rust tracks with _path and path field.
///
/// # Safety
/// 1. `rlk` must be a [valid], non-null pointer to a `RLookupKey`
/// 2. The memory pointed to by `path` must contain a valid nul terminator at the end of the string.
/// 3. `path` must be [valid] for reads of bytes up to and including the nul terminator.
///    This means in particular:
///     1. The entire memory range of this `CStr` must be contained within a single allocation!
///     2. `path` must be non-null even for a zero-length cstr.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_KeySetPath(rlk: Option<NonNull<RLookupKey>>, path: *const c_char) {
    // Safety: ensured by caller (1.)
    let rlk = unsafe { as_mut_unchecked!(rlk, "RLookup_KeySetPath: rlk is null") };

    // Safety: ensured by caller (2., 3.)
    let path = unsafe { CStr::from_ptr(path) };
    rlk._ffi_set_path(path);
}
