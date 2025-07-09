/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rlookup::{IndexSpecCache, RLookup, RLookupKey, RLookupKeyFlags};
use std::{
    ffi::{CStr, c_char},
    pin::Pin,
    ptr::{self, NonNull},
    slice,
};

/// # Panics
/// - if lookup is nullptr
/// - if flags is not a valid RLookupKeyFlags bit-pattern
///
/// # Safety
/// - rlookup must be valid
/// - name must be valid, non-null, and point to a null-terminated string
/// - field_name must be valid and point to a null-terminated string
#[unsafe(no_mangle)]
pub extern "C" fn RLookup_GetKey_Read(
    lookup: Option<NonNull<RLookup>>,
    name: *const c_char,
    flags: u32,
) -> Option<NonNull<RLookupKey>> {
    let mut rlookup = lookup.expect("expected rlookup pointer to be non-null");
    let lookup = unsafe { rlookup.as_mut() };

    assert!(!name.is_null(), "expected name to be non-null");
    let name = unsafe { CStr::from_ptr(name) };

    let flags = RLookupKeyFlags::from_bits(flags).expect("invalid RLookupKeyFlags bit-pattern");

    lookup.get_key_read(name, flags).map(NonNull::from)
}

/// # Panics
/// - if lookup is nullptr
/// - if flags is not a valid RLookupKeyFlags bit-pattern
///
/// # Safety
/// - rlookup must be valid
/// - name must be valid, non-null, and point to a null-terminated string
/// - name_len must match the actual `strlen` length of name
#[unsafe(no_mangle)]
pub extern "C" fn RLookup_GetKey_ReadEx(
    lookup: Option<NonNull<RLookup>>,
    name: *const c_char,
    name_len: usize,
    flags: u32,
) -> Option<NonNull<RLookupKey>> {
    let mut rlookup = lookup.expect("expected rlookup pointer to be non-null");
    let lookup = unsafe { rlookup.as_mut() };

    debug_assert!(!name.is_null(), "expected name to be non-null");
    let name = unsafe { cstr_from_ptr_and_len(name, name_len) };

    let flags = RLookupKeyFlags::from_bits(flags).expect("invalid RLookupKeyFlags bit-pattern");

    lookup.get_key_read(name, flags).map(NonNull::from)
}

/// # Panics
/// - if lookup is nullptr
/// - if flags is not a valid RLookupKeyFlags bit-pattern
///
/// # Safety
/// - rlookup must be valid
/// - name must be valid, non-null, and point to a null-terminated string
/// - field_name must be valid and point to a null-terminated string
#[unsafe(no_mangle)]
pub extern "C" fn RLookup_GetKey_Write(
    lookup: Option<NonNull<RLookup>>,
    name: *const c_char,
    flags: u32,
) -> Option<NonNull<RLookupKey>> {
    let mut rlookup = lookup.expect("expected rlookup pointer to be non-null");
    let lookup = unsafe { rlookup.as_mut() };

    assert!(!name.is_null(), "expected name to be non-null");
    let name = unsafe { CStr::from_ptr(name) };

    let flags = RLookupKeyFlags::from_bits(flags).expect("invalid RLookupKeyFlags bit-pattern");

    lookup.get_key_write(name, flags).map(NonNull::from)
}

/// # Panics
/// - if lookup is nullptr
/// - if flags is not a valid RLookupKeyFlags bit-pattern
///
/// # Safety
/// - rlookup must be valid
/// - name must be valid, non-null, and point to a null-terminated string
/// - name_len must match the actual `strlen` length of name
#[unsafe(no_mangle)]
pub extern "C" fn RLookup_GetKey_WriteEx(
    lookup: Option<NonNull<RLookup>>,
    name: *const c_char,
    name_len: usize,
    flags: u32,
) -> Option<NonNull<RLookupKey>> {
    let mut rlookup = lookup.expect("expected rlookup pointer to be non-null");
    let lookup = unsafe { rlookup.as_mut() };

    debug_assert!(!name.is_null(), "expected name to be non-null");
    let name = unsafe { cstr_from_ptr_and_len(name, name_len) };

    let flags = RLookupKeyFlags::from_bits(flags).expect("invalid RLookupKeyFlags bit-pattern");

    lookup.get_key_write(name, flags).map(NonNull::from)
}

/// # Panics
/// - if lookup is nullptr
/// - if flags is not a valid RLookupKeyFlags bit-pattern
///
/// # Safety
/// - rlookup must be valid
/// - name must be valid, non-null, and point to a null-terminated string
/// - field_name must be valid, non-null, and point to a null-terminated string
#[unsafe(no_mangle)]
pub extern "C" fn RLookup_GetKey_Load(
    lookup: Option<NonNull<RLookup>>,
    name: *const c_char,
    field_name: *const c_char,
    flags: u32,
) -> Option<NonNull<RLookupKey>> {
    let mut rlookup = lookup.expect("expected rlookup pointer to be non-null");
    let lookup = unsafe { rlookup.as_mut() };

    debug_assert!(!name.is_null(), "expected name to be non-null");
    let name = unsafe { CStr::from_ptr(name) };

    debug_assert!(!field_name.is_null(), "expected field_name to be non-null");
    let field_name = unsafe { CStr::from_ptr(field_name) };

    let flags = RLookupKeyFlags::from_bits(flags).expect("invalid RLookupKeyFlags bit-pattern");

    lookup
        .get_key_load(name, field_name, flags)
        .map(NonNull::from)
}

/// # Panics
/// - if name is non-null
/// - if lookup is nullptr
/// - if flags is not a valid RLookupKeyFlags bit-pattern
///
/// # Safety
/// - rlookup must be valid
/// - name must be valid, non-null, and point to a null-terminated string
/// - name_len must match the actual `strlen` length of name
/// - field_name must be valid, non-null, and point to a null-terminated string
#[unsafe(no_mangle)]
pub extern "C" fn RLookup_GetKey_LoadEx(
    lookup: Option<NonNull<RLookup>>,
    name: *const c_char,
    name_len: usize,
    field_name: *const c_char,
    flags: u32,
) -> Option<NonNull<RLookupKey>> {
    let mut rlookup = lookup.expect("expected rlookup pointer to be non-null");
    let lookup = unsafe { rlookup.as_mut() };

    debug_assert!(!name.is_null(), "expected name to be non-null");
    let name = unsafe { cstr_from_ptr_and_len(name, name_len) };

    debug_assert!(!field_name.is_null(), "expected field_name to be non-null");
    let field_name = unsafe { CStr::from_ptr(field_name) };

    let flags = RLookupKeyFlags::from_bits(flags).expect("invalid RLookupKeyFlags bit-pattern");

    lookup
        .get_key_load(name, field_name, flags)
        .map(NonNull::from)
}

/// # Panics
/// - if lookup is nullptr
///
/// # Safety
/// - rlookup must be valid
/// - spcache must be valid
/// - The ffi::IndexSpecCache being pointed MUST NOT get mutated.
#[unsafe(no_mangle)]
pub extern "C" fn RLookup_Init(
    lookup: Option<NonNull<RLookup>>,
    spcache: Option<NonNull<ffi::IndexSpecCache>>,
) {
    let mut rlookup = lookup.expect("expected rlookup pointer to be non-null");
    let lookup = unsafe { rlookup.as_mut() };

    if let Some(spcache) = spcache {
        let spcache = unsafe { IndexSpecCache::from_raw(spcache) };
        lookup.init(spcache);
    }
}

/// # Panics
/// - if lookup is nullptr
///
/// # Safety
/// - rlookup must be valid
/// - after calling this method rlookup is no longer valid and may not be accessed
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_Cleanup(lookup: Option<NonNull<RLookup>>) {
    let lookup = lookup.expect("expected rlookup pointer to be non-null");
    let lookup = unsafe { lookup.read() };
    drop(lookup);
}

/// # Panics
/// - if lookup is nullptr
///
/// # Safety
/// - rlookup must be valid
/// - name must be valid, non-null, and point to a null-terminated string
#[unsafe(no_mangle)]
pub unsafe extern "C" fn findFieldInSpecCache(
    lookup: Option<NonNull<RLookup>>,
    name: *const c_char,
) -> *const ffi::FieldSpec {
    let lookup = lookup.expect("expected rlookup pointer to be non-null");
    let lookup = unsafe { lookup.as_ref() };

    debug_assert!(!name.is_null(), "expected name to be non-null");
    let name = unsafe { CStr::from_ptr(name) };

    if let Some(spcache) = &lookup.spcache {
        if let Some(field) = spcache.find_field(name) {
            return ptr::from_ref(field);
        }
    }

    ptr::null()
}

/// # Safety
/// - str_ptr must be valid for str_len number of characters
/// - str_ptr must be null-terminated
unsafe fn cstr_from_ptr_and_len<'a>(str_ptr: *const c_char, str_len: usize) -> &'a CStr {
    // Safety: We assume the `path_ptr` and `length` information returned by the field spec
    // point to a valid null-terminated C string. Importantly `length` here is value as returned by
    // `strlen` so **does not** include the null terminator (that is why we do `path_len + 1` below)

    let bytes = unsafe { slice::from_raw_parts(str_ptr.cast::<u8>(), str_len + 1) };

    let str = CStr::from_bytes_with_nul(bytes).expect("name is malformed");

    debug_assert_eq!(unsafe { CStr::from_ptr(str_ptr) }, str);

    str
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn createNewKey(
    lookup: Option<NonNull<RLookup>>,
    name: *const c_char,
    name_len: usize,
    flags: u32,
) -> NonNull<RLookupKey> {
    let mut lookup = lookup.expect("expected rlookup pointer to be non-null");
    let lookup = unsafe { lookup.as_mut() };

    debug_assert!(!name.is_null(), "expected name to be non-null");
    let name = unsafe { cstr_from_ptr_and_len(name, name_len) };

    let flags = RLookupKeyFlags::from_bits(flags).expect("invalid RLookupKeyFlags bit-pattern");

    let key = lookup.c_create_new_key(name, flags);
    NonNull::from(unsafe { Pin::into_inner_unchecked(key) })
}

// static RLookupKey *RLookup_FindKey(RLookup *lookup, const char *name, size_t name_len) {
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_FindKey(
    lookup: Option<NonNull<RLookup>>,
    name: *const c_char,
    name_len: usize,
) -> Option<NonNull<RLookupKey>> {
    let lookup = lookup.expect("expected rlookup pointer to be non-null");
    let lookup = unsafe { lookup.as_ref() };

    debug_assert!(!name.is_null(), "expected name to be non-null");
    let name = unsafe { cstr_from_ptr_and_len(name, name_len) };

    lookup.c_find_key(name).map(NonNull::from)
}
