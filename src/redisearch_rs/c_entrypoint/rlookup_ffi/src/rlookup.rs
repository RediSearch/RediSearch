/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use rlookup::{IndexSpecCache, RLookup, RLookupKey, RLookupKeyFlags};
use std::{ffi::CStr, os::raw::c_char, ptr::NonNull, slice};

// /**
//  * Get a RLookup key for a given name.
//  *
//  * 1. On READ mode, a key is returned only if it's already in the lookup table (available from the
//  * pipeline upstream), it is part of the index schema and is sortable (and then it is created), or
//  * if the lookup table accepts unresolved keys.
//  */
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_GetKey_Read<'a>(
    lookup: Option<NonNull<RLookup<'a>>>,
    name: *const c_char,
    flags: u32,
) -> Option<NonNull<RLookupKey<'a>>> {
    let rlookup = unsafe { lookup.unwrap().as_mut() };

    let name = unsafe { CStr::from_ptr(name) };

    let flags = RLookupKeyFlags::from_bits(flags).unwrap();

    rlookup.get_key_read(name, flags).map(NonNull::from)
}

// /**
//  * Get a RLookup key for a given name.
//  *
//  * 1. On READ mode, a key is returned only if it's already in the lookup table (available from the
//  * pipeline upstream), it is part of the index schema and is sortable (and then it is created), or
//  * if the lookup table accepts unresolved keys.
//  */
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_GetKey_ReadEx<'a>(
    lookup: Option<NonNull<RLookup<'a>>>,
    name: *const c_char,
    name_len: libc::size_t,
    flags: u32,
) -> Option<NonNull<RLookupKey<'a>>> {
    let rlookup = unsafe { lookup.unwrap().as_mut() };

    let name = {
        let name_bytes = unsafe { slice::from_raw_parts(name.cast(), name_len) };
        CStr::from_bytes_with_nul(name_bytes).unwrap()
    };

    let flags = RLookupKeyFlags::from_bits(flags).unwrap();

    rlookup.get_key_read(name, flags).map(NonNull::from)
}

// /**
//  * Get a RLookup key for a given name.
//  *
//  * 2. On WRITE mode, a key is created and returned only if it's NOT in the lookup table, unless the
//  * override flag is set.
//  */
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_GetKey_Write<'a>(
    lookup: Option<NonNull<RLookup<'a>>>,
    name: *const c_char,
    flags: u32,
) -> Option<NonNull<RLookupKey<'a>>> {
    let rlookup = unsafe { lookup.unwrap().as_mut() };

    let name = unsafe { CStr::from_ptr(name) };

    let flags = RLookupKeyFlags::from_bits(flags).unwrap();

    rlookup.get_key_write(name, flags).map(NonNull::from)
}

// /**
//  * Get a RLookup key for a given name.
//  *
//  * 2. On WRITE mode, a key is created and returned only if it's NOT in the lookup table, unless the
//  * override flag is set.
//  */
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_GetKey_WriteEx<'a>(
    lookup: Option<NonNull<RLookup<'a>>>,
    name: *const c_char,
    name_len: libc::size_t,
    flags: u32,
) -> Option<NonNull<RLookupKey<'a>>> {
    let rlookup = unsafe { lookup.unwrap().as_mut() };

    let name = {
        let name_bytes = unsafe { slice::from_raw_parts(name.cast(), name_len) };
        CStr::from_bytes_with_nul(name_bytes).unwrap()
    };

    let flags = RLookupKeyFlags::from_bits(flags).unwrap();

    rlookup.get_key_write(name, flags).map(NonNull::from)
}

// /**
//  * Get a RLookup key for a given name.
//  *
//  * 3. On LOAD mode, a key is created and returned only if it's NOT in the lookup table (unless the
//  * override flag is set), and it is not already loaded. It will override an existing key if it was
//  * created for read out of a sortable field, and the field was normalized. A sortable un-normalized
//  * field counts as loaded.
//  */
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_GetKey_Load<'a>(
    lookup: Option<NonNull<RLookup<'a>>>,
    name: *const c_char,
    field_name: *const c_char,
    flags: u32,
) -> Option<NonNull<RLookupKey<'a>>> {
    let rlookup = unsafe { lookup.unwrap().as_mut() };

    let name = unsafe { CStr::from_ptr(name) };

    let field_name = unsafe { CStr::from_ptr(field_name) };

    let flags = RLookupKeyFlags::from_bits(flags).unwrap();

    rlookup
        .get_key_load(name, field_name, flags)
        .map(NonNull::from)
}

// /**
//  * Get a RLookup key for a given name.
//  *
//  * 3. On LOAD mode, a key is created and returned only if it's NOT in the lookup table (unless the
//  * override flag is set), and it is not already loaded. It will override an existing key if it was
//  * created for read out of a sortable field, and the field was normalized. A sortable un-normalized
//  * field counts as loaded.
//  */
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_GetKey_LoadEx<'a>(
    lookup: Option<NonNull<RLookup<'a>>>,
    name: *const c_char,
    name_len: libc::size_t,
    field_name: *const c_char,
    flags: u32,
) -> Option<NonNull<RLookupKey<'a>>> {
    let rlookup = unsafe { lookup.unwrap().as_mut() };

    let name = {
        let name_bytes = unsafe { slice::from_raw_parts(name.cast(), name_len) };
        CStr::from_bytes_with_nul(name_bytes).unwrap()
    };

    let field_name = unsafe { CStr::from_ptr(field_name) };

    let flags = RLookupKeyFlags::from_bits(flags).unwrap();

    rlookup
        .get_key_load(name, field_name, flags)
        .map(NonNull::from)
}

// /**
//  * Initialize the lookup. If cache is provided, then it will be used as an
//  * alternate source for lookups whose fields are absent
//  */
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_Init(
    lookup: Option<NonNull<RLookup<'_>>>,
    spcache: Option<NonNull<ffi::IndexSpecCache>>,
) {
    let lookup = unsafe { lookup.unwrap().as_mut() };
    let spcache = unsafe { IndexSpecCache::from_raw(spcache.unwrap()) };

    lookup.init(spcache);
}

// /**
//  * Releases any resources created by this lookup object. Note that if there are
//  * lookup keys created with RLOOKUP_F_NOINCREF, those keys will no longer be
//  * valid after this call!
//  */
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RLookup_Cleanup(lookup: Option<NonNull<RLookup<'_>>>) {
    drop(unsafe { lookup.unwrap().read() });
}
