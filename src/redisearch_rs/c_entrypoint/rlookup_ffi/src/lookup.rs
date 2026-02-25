/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use libc::size_t;
use rlookup::{
    IndexSpec, IndexSpecCache, OpaqueRLookup, OpaqueRLookupRow, RLookup, RLookupKey,
    RLookupKeyFlag, RLookupKeyFlags, RLookupOptions, RLookupRow, SchemaRule,
};
use std::{
    borrow::Cow,
    ffi::{CStr, c_char},
    ptr::{self, NonNull},
    slice,
};

/// Add all non-overridden keys from `src` to `dest`.
///
/// For each key in `src`, check if it already exists *by name*.
/// - If it does, the `flag` argument controls the behaviour (skip with `RLookupKeyFlags::empty()`, override with `RLookupKeyFlag::Override`).
/// - If it doesn't, a new key will be created.
///
/// Flag handling:
/// - Preserves persistent source key properties (F_SVSRC, F_HIDDEN, F_EXPLICITRETURN, etc.)
/// - Filters out transient flags from source keys (F_OVERRIDE, F_FORCE_LOAD)
/// - Respects caller's control flags for behavior (F_OVERRIDE, F_FORCE_LOAD, etc.)
/// - Target flags = caller_flags | (source_flags & ~RLOOKUP_TRANSIENT_FLAGS)
///
/// # Safety
///
/// 1. `src` must be a [valid], non-null pointer to an [`RLookup`]
/// 2. `dest` must be a [valid], non-null pointer to an [`RLookup`]
/// 3. `src` and `dest` must not point to the same [`RLookup`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_AddKeysFrom(
    src: *const OpaqueRLookup,
    dest: Option<NonNull<OpaqueRLookup>>,
    flags: u32,
) {
    let dest = dest.unwrap();
    assert!(
        !ptr::addr_eq(src, dest.as_ptr().cast_const()),
        "`src` and `dst` must not be the same"
    );

    // Safety: ensured by caller (1.)
    let src = unsafe { RLookup::from_opaque_ptr(src).unwrap() };

    // Safety: ensured by caller (2.)
    let dest = unsafe { RLookup::from_opaque_non_null(dest) };

    let flags = RLookupKeyFlags::from_bits(flags).unwrap();

    dest.add_keys_from(src, flags);
}

/// Disables the given set of `RLookup` options.
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to an `RLookup`.
/// 2. All bits set in `options` must correspond to a value of the `RLookupOptions` enum.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_DisableOptions(
    lookup: Option<NonNull<OpaqueRLookup>>,
    options: u32,
) {
    // Safety: ensured by caller (1.)
    let lookup = unsafe { RLookup::from_opaque_non_null(lookup.unwrap()) };

    let options = RLookupOptions::from_bits(options).unwrap();

    lookup.disable_options(options);
}

/// Enables the given set of `RLookup` options.
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to an `RLookup`.
/// 2. All bits set in `options` must correspond to a value of the `RLookupOptions` enum.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_EnableOptions(
    lookup: Option<NonNull<OpaqueRLookup>>,
    options: u32,
) {
    // Safety: ensured by caller (1.)
    let lookup = unsafe { RLookup::from_opaque_non_null(lookup.unwrap()) };

    let options = RLookupOptions::from_bits(options).unwrap();

    lookup.enable_options(options);
}

/// Find a field in the index spec cache of the lookup.
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to an `RLookup`.
/// 2. The memory pointed to by `name` must contain a valid nul terminator at the
///    end of the string.
/// 3. `name` must be [valid] for reads of bytes up to and including the nul terminator.
///    This means in particular:
///     1. The entire memory range of this cstr must be contained within a single allocation!
///     2. `name` must be non-null even for a zero-length cstr.
/// 4. The nul terminator must be within `isize::MAX` from `name`
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_FindFieldInSpecCache(
    lookup: *const OpaqueRLookup,
    name: *const c_char,
) -> *const ffi::FieldSpec {
    // Safety: ensured by caller (1.)
    let lookup = unsafe { RLookup::from_opaque_ptr(lookup).unwrap() };

    // Safety: ensured by caller (2., 3., 4.)
    let name = unsafe { CStr::from_ptr(name) };

    lookup
        .find_field_in_spec_cache(name)
        .map_or(ptr::null(), ptr::from_ref)
}

/// Get an RLookup key for a given name.
///
/// A key is returned only if it's already in the lookup table (available from the
/// pipeline upstream), it is part of the index schema and is sortable (and then it is created), or
/// if the lookup table accepts unresolved keys.
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to an `RLookup`.
/// 2. The memory pointed to by `name` must contain a valid nul terminator at the
///    end of the string.
/// 3. `name` must be [valid] for reads of bytes up to and including the nul terminator.
///    This means in particular:
///     1. The entire memory range of this `CStr` must be contained within a single allocation!
///     2. `name` must be non-null even for a zero-length cstr.
/// 4. The memory referenced by the returned `CStr` must not be mutated for
///    the lifetime of the returned key.
/// 5. The nul terminator must be within `isize::MAX` from `name`
/// 6. All bits set in `flags` must correspond to a value of the enum.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_GetKey_Read<'a>(
    lookup: Option<NonNull<OpaqueRLookup>>,
    name: *const c_char,
    flags: u32,
) -> Option<NonNull<RLookupKey<'a>>> {
    // Safety: ensured by caller (1.)
    let lookup = unsafe { RLookup::from_opaque_non_null(lookup.unwrap()) };

    // Safety: ensured by caller (2., 3., 4., 5.)
    let name = unsafe { CStr::from_ptr(name) };

    let flags = RLookupKeyFlags::from_bits(flags).unwrap();

    let (name, flags) = handle_name_alloc_flag(name, flags);

    lookup.get_key_read(name, flags).map(NonNull::from)
}

/// Get an RLookup key for a given name.
///
/// A key is returned only if it's already in the lookup table (available from the
/// pipeline upstream), it is part of the index schema and is sortable (and then it is created), or
/// if the lookup table accepts unresolved keys.
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to an `RLookup`.
/// 2. The memory pointed to by `name` must contain a valid nul terminator at the
///    end of the string.
/// 3. `name` must be [valid] for reads of `name_len` bytes up to and including the nul terminator.
///    This means in particular:
///     1. `name_len` must be same as `strlen(name)`
///     2. The entire memory range of this `CStr` must be contained within a single allocation!
///     3. `name` must be non-null even for a zero-length cstr.
/// 4. The memory referenced by the returned `CStr` must not be mutated for
///    the lifetime of the returned key.
/// 5. The nul terminator must be within `isize::MAX` from `name`
/// 6. All bits set in `flags` must correspond to a value of the enum.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_GetKey_ReadEx<'a>(
    lookup: Option<NonNull<OpaqueRLookup>>,
    name: *const c_char,
    name_len: size_t,
    flags: u32,
) -> Option<NonNull<RLookupKey<'a>>> {
    // Safety: ensured by caller (1.)
    let lookup = unsafe { RLookup::from_opaque_non_null(lookup.unwrap()) };

    let name = CStr::from_bytes_with_nul(
        // `name_len` is a value as returned by `strlen` and therefore **does not**
        // include the null terminator (that is why we do `name_len + 1` below)
        // Safety: ensured by caller (2., 3., 4., 5.)
        unsafe { slice::from_raw_parts(name.cast::<u8>(), name_len + 1) },
    )
    .unwrap();

    let flags = RLookupKeyFlags::from_bits(flags).unwrap();

    let (name, flags) = handle_name_alloc_flag(name, flags);

    lookup.get_key_read(name, flags).map(NonNull::from)
}

/// Get an RLookup key for a given name.
///
/// A key is created and returned only if it's NOT in the lookup table, unless the
/// override flag is set.
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to an `RLookup`.
/// 2. The memory pointed to by `name` must contain a valid nul terminator at the
///    end of the string.
/// 3. `name` must be [valid] for reads of bytes up to and including the nul terminator.
///    This means in particular:
///     1. The entire memory range of this `CStr` must be contained within a single allocation!
///     2. `name` must be non-null even for a zero-length cstr.
/// 4. The memory referenced by the returned `CStr` must not be mutated for
///    the lifetime of the returned key.
/// 5. The nul terminator must be within `isize::MAX` from `name`
/// 6. All bits set in `flags` must correspond to a value of the enum.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_GetKey_Write<'a>(
    lookup: Option<NonNull<OpaqueRLookup>>,
    name: *const c_char,
    flags: u32,
) -> Option<NonNull<RLookupKey<'a>>> {
    // Safety: ensured by caller (1.)
    let lookup = unsafe { RLookup::from_opaque_non_null(lookup.unwrap()) };

    // Safety: ensured by caller (2., 3., 4., 5.)
    let name = unsafe { CStr::from_ptr(name) };

    let flags = RLookupKeyFlags::from_bits(flags).unwrap();

    let (name, flags) = handle_name_alloc_flag(name, flags);

    lookup.get_key_write(name, flags).map(NonNull::from)
}

/// Get an RLookup key for a given name.
///
/// A key is created and returned only if it's NOT in the lookup table, unless the
/// override flag is set.
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to an `RLookup`.
/// 2. The memory pointed to by `name` must contain a valid nul terminator at the
///    end of the string.
/// 3. `name` must be [valid] for reads of `name_len` bytes up to and including the nul terminator.
///    This means in particular:
///     1. `name_len` must be same as `strlen(name)`
///     2. The entire memory range of this `CStr` must be contained within a single allocation!
///     3. `name` must be non-null even for a zero-length cstr.
/// 4. The memory referenced by the returned `CStr` must not be mutated for
///    the lifetime of the returned key.
/// 5. The nul terminator must be within `isize::MAX` from `name`
/// 6. All bits set in `flags` must correspond to a value of the enum.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_GetKey_WriteEx<'a>(
    lookup: Option<NonNull<OpaqueRLookup>>,
    name: *const c_char,
    name_len: size_t,
    flags: u32,
) -> Option<NonNull<RLookupKey<'a>>> {
    // Safety: ensured by caller (1.)
    let lookup = unsafe { RLookup::from_opaque_non_null(lookup.unwrap()) };

    let name = CStr::from_bytes_with_nul(
        // `name_len` is a value as returned by `strlen` and therefore **does not**
        // include the null terminator (that is why we do `name_len + 1` below)
        // Safety: ensured by caller (2., 3., 4., 5.)
        unsafe { slice::from_raw_parts(name.cast::<u8>(), name_len + 1) },
    )
    .unwrap();

    let flags = RLookupKeyFlags::from_bits(flags).unwrap();

    let (name, flags) = handle_name_alloc_flag(name, flags);

    lookup.get_key_write(name, flags).map(NonNull::from)
}

/// Get an RLookup key for a given name.
///
/// A key is created and returned only if it's NOT in the lookup table (unless the
/// override flag is set), and it is not already loaded. It will override an existing key if it was
/// created for read out of a sortable field, and the field was normalized. A sortable un-normalized
/// field counts as loaded.
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to an `RLookup`.
/// 2. The memory pointed to by `name` and `field_name` must contain a valid nul terminator at the
///    end of the string.
/// 3. `name` and `field_name` must be [valid] for reads of bytes up to and including the nul terminator.
///    This means in particular:
///     1. The entire memory range of these `CStr` must be contained within a single allocation!
///     2. `name` and `field_name` must be non-null even for a zero-length cstr.
/// 4. The memory referenced by the returned `CStr` must not be mutated for
///    the lifetime of the returned key.
/// 5. The nul terminator must be within `isize::MAX` from `name` and `field_name`
/// 6. All bits set in `flags` must correspond to a value of the enum.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_GetKey_Load<'a>(
    lookup: Option<NonNull<OpaqueRLookup>>,
    name: *const c_char,
    field_name: *const c_char,
    flags: u32,
) -> Option<NonNull<RLookupKey<'a>>> {
    // Safety: ensured by caller (1.)
    let lookup = unsafe { RLookup::from_opaque_non_null(lookup.unwrap()) };

    // Safety: ensured by caller (2., 3., 4., 5.)
    let field_name = unsafe { CStr::from_ptr(field_name) };

    // Safety: ensured by caller (2., 3., 4., 5.)
    let name = unsafe { CStr::from_ptr(name) };

    let flags = RLookupKeyFlags::from_bits(flags).unwrap();

    let (name, flags) = handle_name_alloc_flag(name, flags);

    lookup
        .get_key_load(name, field_name, flags)
        .map(NonNull::from)
}

/// Get an RLookup key for a given name.
///
/// A key is created and returned only if it's NOT in the lookup table (unless the
/// override flag is set), and it is not already loaded. It will override an existing key if it was
/// created for read out of a sortable field, and the field was normalized. A sortable un-normalized
/// field counts as loaded.
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to an `RLookup`.
/// 2. The memory pointed to by `name` and `field_name` must contain a valid nul terminator at the
///    end of the string.
/// 3. `name` and `field_name` must be [valid] for reads of `name_len` bytes up to and including the nul terminator.
///    This means in particular:
///     1. `name_len` must be same as `strlen(name)`
///     2. The entire memory range of these `CStr` must be contained within a single allocation!
///     3. `name` and `field_name` must be non-null even for a zero-length cstr.
/// 4. The memory referenced by the returned `CStr` must not be mutated for
///    the lifetime of the returned key.
/// 5. The nul terminator must be within `isize::MAX` from `name` and `field_name`
/// 6. All bits set in `flags` must correspond to a value of the enum.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_GetKey_LoadEx<'a>(
    lookup: Option<NonNull<OpaqueRLookup>>,
    name: *const c_char,
    name_len: size_t,
    field_name: *const c_char,
    flags: u32,
) -> Option<NonNull<RLookupKey<'a>>> {
    // Safety: ensured by caller (1.)
    let lookup = unsafe { RLookup::from_opaque_non_null(lookup.unwrap()) };

    // Safety: ensured by caller (2., 3., 4., 5.)
    let field_name = unsafe { CStr::from_ptr(field_name) };

    let name = CStr::from_bytes_with_nul(
        // `name_len` is a value as returned by `strlen` and therefore **does not**
        // include the null terminator (that is why we do `name_len + 1` below)
        // Safety: ensured by caller (2., 3., 4., 5.)
        unsafe { slice::from_raw_parts(name.cast::<u8>(), name_len + 1) },
    )
    .unwrap();

    let flags = RLookupKeyFlags::from_bits(flags).unwrap();

    let (name, flags) = handle_name_alloc_flag(name, flags);

    lookup
        .get_key_load(name, field_name, flags)
        .map(NonNull::from)
}

/// Returns the number of visible fields in this RLookupRow.
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to a [`RLookup`]
/// 2. `row` must be a [valid], non-null pointer to a [`RLookupRow`]
/// 3. `skip_field_index` must be a [valid] non-null pointer for reads and writes of `skip_field_index_len` boolean values
/// 4. `rule` must be a [valid], non-null pointer to a [`SchemaRule`] or a null pointer
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_GetLength(
    lookup: *const OpaqueRLookup,
    row: *const OpaqueRLookupRow,
    skip_field_index: Option<NonNull<bool>>,
    skip_field_index_len: size_t,
    required_flags: u32,
    excluded_flags: u32,
    rule: *const ffi::SchemaRule,
) -> size_t {
    // Safety: ensured by caller (1.)
    let lookup = unsafe { RLookup::from_opaque_ptr(lookup).unwrap() };

    // Safety: ensured by caller (2.)
    let row = unsafe { RLookupRow::from_opaque_ptr(row).unwrap() };

    // Safety: ensured by caller (3.)
    let skip_field_index = unsafe {
        slice::from_raw_parts_mut(skip_field_index.unwrap().as_ptr(), skip_field_index_len)
    };

    let required_flags = RLookupKeyFlags::from_bits(required_flags).unwrap();
    let excluded_flags = RLookupKeyFlags::from_bits(excluded_flags).unwrap();

    let rule = if rule.is_null() {
        None
    } else {
        // Safety: ensured by caller (4.)
        Some(unsafe { SchemaRule::from_raw(rule) })
    };

    row.get_length_no_alloc(
        lookup,
        required_flags,
        excluded_flags,
        rule,
        skip_field_index,
    )
}

/// Returns the row len of the [`RLookup`], i.e. the number of keys in its key list not counting the overridden keys.
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to an `RLookup`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_GetRowLen(lookup: *const OpaqueRLookup) -> u32 {
    // Safety: ensured by caller (1.)
    let lookup = unsafe { RLookup::from_opaque_ptr(lookup).unwrap() };

    lookup.get_row_len()
}

/// Returns a newly created [`RLookup`].
#[unsafe(no_mangle)]
pub extern "C" fn RLookup_New() -> OpaqueRLookup {
    RLookup::new().into_opaque()
}

/// Sets the [`ffi::IndexSpecCache`] of the lookup. If spcache is provided, then it will be used as an
/// alternate source for lookups whose fields are absent.
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to an `RLookup`.
/// 2. `spcache` must be a [valid] pointer to a [`ffi::IndexSpecCache`]
/// 3. The [`ffi::IndexSpecCache`] being pointed MUST NOT get mutated
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_SetCache(
    lookup: Option<NonNull<OpaqueRLookup>>,
    spcache: Option<NonNull<ffi::IndexSpecCache>>,
) {
    // Safety: ensured by caller (1.)
    let lookup = unsafe { RLookup::from_opaque_non_null(lookup.unwrap()) };

    let spcache = spcache.map(|spcache| {
        // Safety: ensured by caller (2. & 3.)
        unsafe { IndexSpecCache::from_raw(spcache) }
    });

    lookup.set_cache(spcache);
}

/// Returns `true` if this `RLookup` has an associated [`IndexSpecCache`].
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to an `RLookup`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_HasIndexSpecCache(lookup: *const OpaqueRLookup) -> bool {
    // Safety: ensured by caller (1.)
    let lookup = unsafe { RLookup::from_opaque_ptr(lookup).unwrap() };

    lookup.has_index_spec_cache()
}

/// Releases any resources created by this lookup object. Note that if there are
/// lookup keys created with RLOOKUP_F_NOINCREF, those keys will no longer be
/// valid after this call!
///
/// # Safety
///
/// 1. `lookup` must be a [valid], non-null pointer to an `RLookup`.
/// 2. `lookup` **must not** be used again after this function is called.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_Cleanup(lookup: Option<NonNull<OpaqueRLookup>>) {
    // Safety: ensured by caller (1.,2.)
    unsafe {
        lookup.unwrap().cast::<RLookup>().drop_in_place();
    }
}

/// Initialize the lookup with fields from a Redis hash.
///
/// # Safety
///
/// 1. `search_ctx` must be a [valid], non-null pointer to an `ffi::RedisSearchCtx` that is properly initialized.
/// 2. `lookup` must be a [valid], non-null pointer to an `RLookup` that is properly initialized.
/// 3. `dst_row` must be a [valid], non-null pointer to an `RLookupRow` that is properly initialized.
/// 4. `index_spec` must be a [valid], non-null pointer to an `ffi::IndexSpec` that is properly initialized.
///    This also applies to any of its subfields.
/// 5. The memory pointed to by `key` must contain a valid nul terminator at the
///    end of the string.
/// 6. `key` must be [valid] for reads of bytes up to and including the nul terminator.
///    This means in particular:
///     1. The entire memory range of this `CStr` must be contained within a single allocation!
///     2. `key` must be non-null even for a zero-length cstr.
/// 7. The nul terminator must be within `isize::MAX` from `key`
/// 8. `status` must be a [valid], non-null pointer to an `ffi::QueryError` that is properly initialized.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_LoadRuleFields(
    search_ctx: Option<NonNull<ffi::RedisSearchCtx>>,
    lookup: Option<NonNull<OpaqueRLookup>>,
    dst_row: Option<NonNull<OpaqueRLookupRow>>,
    index_spec: Option<NonNull<ffi::IndexSpec>>,
    key: *const c_char,
    status: Option<NonNull<ffi::QueryError>>,
) -> i32 {
    // Safety: ensured by caller (1.)
    let search_ctx = unsafe { search_ctx.unwrap().as_mut() };

    // Safety: ensured by caller (2.)
    let lookup = unsafe { RLookup::from_opaque_non_null(lookup.unwrap()) };

    // Safety: ensured by caller (3.)
    let dst_row = unsafe { dst_row.unwrap().as_mut() };

    // Safety: ensured by caller (4.)
    let index_spec = unsafe { index_spec.unwrap().as_ref() };
    // Safety: ensured by caller (4.)
    let index_spec = unsafe { IndexSpec::from_raw(index_spec) };

    // Safety: ensured by caller (5., 6., 7.)
    let key = unsafe { CStr::from_ptr(key) };

    // Safety: ensured by caller (8.)
    let status = unsafe { status.unwrap().as_mut() };

    lookup.load_rule_fields(search_ctx, dst_row, index_spec, key, status)
}

/// Turns `name` into an owned allocation if needed, and returns it together with the (cleared) flags.
fn handle_name_alloc_flag(name: &CStr, flags: RLookupKeyFlags) -> (Cow<'_, CStr>, RLookupKeyFlags) {
    if flags.contains(RLookupKeyFlag::NameAlloc) {
        (name.to_owned().into(), flags & !RLookupKeyFlag::NameAlloc)
    } else {
        (name.into(), flags)
    }
}
