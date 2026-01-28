/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI layer for the query_term crate.
//!
//! This module provides C-compatible functions for creating and freeing query terms.

#![allow(non_camel_case_types, non_snake_case)]

use redis_module::raw::{RedisModule_Alloc, RedisModule_Free};
use std::{ffi::c_char, ptr, slice};

/// Flags associated with a token.
/// We support up to 30 user-given flags for each token; flags 1 and 2 are taken by the engine.
pub type RSTokenFlags = u32;

/// A token in the query.
///
/// The expanders receive query tokens and can expand the query with more query tokens.
#[repr(C)]
pub struct RSToken {
    /// The token string - which may or may not be NULL terminated.
    pub str_: *mut c_char,
    /// The token length.
    pub len: usize,
    /// Is this token an expansion?
    pub expanded: u8,
    /// Extension set token flags.
    pub flags: RSTokenFlags,
}

/// A single term being evaluated in query time.
#[repr(C)]
pub struct RSQueryTerm {
    /// The term string, not necessarily NULL terminated, hence the length is given as well.
    pub str_: *mut c_char,
    /// The term length.
    pub len: usize,
    /// Inverse document frequency of the term in the index.
    /// See <https://en.wikipedia.org/wiki/Tf%E2%80%93idf>
    pub idf: f64,
    /// Each term in the query gets an incremental id.
    pub id: i32,
    /// Flags given by the engine or by the query expander.
    pub flags: RSTokenFlags,
    /// Inverse document frequency of the term in the index for computing BM25.
    pub bm25_idf: f64,
}

/// Creates a new query term from a token.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `tok` must either be NULL or point to a valid [`RSToken`].
/// - If `tok->str_` is not NULL, it must point to a valid memory region of at least `tok->len` bytes.
/// - The Redis allocator must be initialized before calling this function.
///
/// # Returns
///
/// A pointer to a newly allocated [`RSQueryTerm`], or NULL if allocation fails.
/// The caller is responsible for freeing this memory using [`Term_Free`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewQueryTerm(tok: *const RSToken, id: i32) -> *mut RSQueryTerm {
    // SAFETY: Caller guarantees tok is NULL or points to a valid RSToken
    let Some(tok) = (unsafe { tok.as_ref() }) else {
        return ptr::null_mut();
    };

    // Get the string bytes from the token
    let str_bytes: Option<&[u8]> = if tok.str_.is_null() || tok.len == 0 {
        None
    } else {
        // SAFETY: Caller guarantees tok->str_ points to valid memory of tok->len bytes
        Some(unsafe { slice::from_raw_parts(tok.str_ as *const u8, tok.len) })
    };

    let flags = tok.flags;

    // Allocate memory for RSQueryTerm using Redis allocator
    // SAFETY: We're calling the Redis allocator which must be initialized per safety docs
    let rm_alloc = unsafe { RedisModule_Alloc.expect("Redis allocator not available") };
    // SAFETY: rm_alloc is a valid function pointer
    let ret = unsafe { rm_alloc(std::mem::size_of::<RSQueryTerm>()) } as *mut RSQueryTerm;
    if ret.is_null() {
        return ptr::null_mut();
    }

    // Allocate and copy the string if present
    let (str_ptr, str_len) = if let Some(bytes) = str_bytes {
        let len = bytes.len();
        // SAFETY: rm_alloc is valid per safety docs
        let str_mem = unsafe { rm_alloc(len + 1) } as *mut c_char; // +1 for null terminator
        if str_mem.is_null() {
            // SAFETY: RedisModule_Free is initialized if we got here
            let rm_free = unsafe { RedisModule_Free.expect("Redis allocator not available") };
            // SAFETY: ret was allocated by rm_alloc above
            unsafe { rm_free(ret as *mut _) };
            return ptr::null_mut();
        }
        // SAFETY: Both pointers are valid and don't overlap
        unsafe {
            ptr::copy_nonoverlapping(bytes.as_ptr(), str_mem as *mut u8, len);
            // Write null terminator
            *str_mem.add(len) = 0;
        }
        (str_mem, len)
    } else {
        (ptr::null_mut(), 0)
    };

    // Fill in the RSQueryTerm structure
    // SAFETY: ret is a valid pointer allocated above, all writes are to valid fields
    unsafe {
        (*ret).str_ = str_ptr;
        (*ret).len = str_len;
        (*ret).idf = 1.0;
        (*ret).id = id;
        (*ret).flags = flags;
        (*ret).bm25_idf = 0.0;
    }

    ret
}

/// Frees a query term allocated by [`NewQueryTerm`].
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must either be NULL or point to a valid [`RSQueryTerm`] allocated by [`NewQueryTerm`].
/// - The Redis allocator must be initialized before calling this function.
/// - `t` must not be used after calling this function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn Term_Free(t: *mut RSQueryTerm) {
    if t.is_null() {
        return;
    }

    // SAFETY: Caller guarantees t points to a valid RSQueryTerm
    let rm_free = unsafe { RedisModule_Free.expect("Redis allocator not available") };

    // Free the string if present
    // SAFETY: t is valid per safety docs
    let str_ptr = unsafe { (*t).str_ };
    if !str_ptr.is_null() {
        // SAFETY: str_ptr was allocated by RedisModule_Alloc in NewQueryTerm
        unsafe { rm_free(str_ptr as *mut _) };
    }

    // Free the RSQueryTerm itself
    // SAFETY: t was allocated by RedisModule_Alloc in NewQueryTerm
    unsafe { rm_free(t as *mut _) };
}
