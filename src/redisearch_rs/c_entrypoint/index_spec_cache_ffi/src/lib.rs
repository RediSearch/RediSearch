/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C entry points for [`IndexSpecCache`].
//!
//! C holds a cache through `const IndexSpecCache *` handles. Each handle is
//! one strong reference of an `Arc<IndexSpecCache>`, in the form
//! [`Arc::into_raw`] returns: [`IndexSpecCache_New`] and
//! [`IndexSpecCache_Incref`] return a new handle, [`IndexSpecCache_Decref`]
//! releases one, and `RLookup_SetCache` takes one over.

use std::{
    ffi::{CStr, c_char},
    slice,
    sync::Arc,
};

use index_spec_cache::CachedField;
pub use index_spec_cache::IndexSpecCache;

/// Builds a cache from the schema's fields and its rule's special field
/// names, and returns the first handle to it.
///
/// # Safety
///
/// 1. If `nfields` is non-zero, `fields` must be [valid] for reads of
///    `nfields` consecutive, properly aligned `FieldSpec`s, none of which is
///    mutated during the call.
/// 2. The `fieldName` and `fieldPath` of each of those fields must be
///    [valid] `HiddenString`s whose buffers are readable for the length they
///    report.
/// 3. `lang_field`, `score_field` and `payload_field` must each be null or a
///    [valid] pointer to a NUL-terminated string.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexSpecCache_New(
    fields: *const ffi::FieldSpec,
    nfields: usize,
    lang_field: *const c_char,
    score_field: *const c_char,
    payload_field: *const c_char,
) -> *const IndexSpecCache {
    let fields: &[ffi::FieldSpec] = if nfields == 0 {
        &[]
    } else {
        debug_assert!(!fields.is_null(), "`fields` must not be null");
        // SAFETY: ensured by caller (1.)
        unsafe { slice::from_raw_parts(fields, nfields) }
    };
    let fields = fields.iter().map(|fs| {
        // SAFETY: ensured by caller (2.)
        unsafe { cached_field(fs) }
    });
    let rule_special_fields = [lang_field, score_field, payload_field]
        .into_iter()
        .filter(|name| !name.is_null())
        .map(|name| {
            // SAFETY: ensured by caller (3.)
            unsafe { CStr::from_ptr(name) }
        });

    Arc::into_raw(Arc::new(IndexSpecCache::new(fields, rule_special_fields)))
}

/// Returns a new handle to the cache `cache` refers to.
///
/// # Safety
///
/// 1. `cache` must be a handle that has not been released.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexSpecCache_Incref(
    cache: *const IndexSpecCache,
) -> *const IndexSpecCache {
    assert!(!cache.is_null(), "`cache` must not be null");
    // SAFETY: ensured by caller (1.)
    unsafe { Arc::increment_strong_count(cache) };
    cache
}

/// Releases `cache`; the cache is freed with its last handle. Does nothing if
/// `cache` is null.
///
/// # Safety
///
/// 1. `cache` must be null or a handle that has not been released.
/// 2. `cache` must not be used after this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexSpecCache_Decref(cache: *const IndexSpecCache) {
    if !cache.is_null() {
        // SAFETY: ensured by caller (1., 2.)
        unsafe { Arc::decrement_strong_count(cache) };
    }
}

/// # Safety
///
/// `fs` must satisfy clause (2.) of [`IndexSpecCache_New`].
unsafe fn cached_field(fs: &ffi::FieldSpec) -> CachedField {
    // SAFETY: ensured by caller
    let name = unsafe { hidden_string_bytes(fs.fieldName) };
    let field = CachedField::new(name)
        .with_types(fs.types())
        .with_options(fs.options())
        .with_sort_idx(fs.sortIdx)
        .with_ft_id(fs.ftId);

    // A field without a path of its own shares one `HiddenString` between its
    // name and path.
    if fs.fieldPath == fs.fieldName {
        field
    } else {
        // SAFETY: ensured by caller
        field.with_path(unsafe { hidden_string_bytes(fs.fieldPath) })
    }
}

/// # Safety
///
/// `hs` must be a [valid] `HiddenString` whose buffer is readable for the
/// length it reports, and stays so for `'a`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe fn hidden_string_bytes<'a>(hs: *const ffi::HiddenString) -> &'a [u8] {
    debug_assert!(!hs.is_null(), "a field's name and path must not be null");
    let mut len = 0;
    // SAFETY: ensured by caller
    let bytes = unsafe { ffi::HiddenString_GetUnsafe(hs, &mut len) };
    debug_assert!(!bytes.is_null(), "a `HiddenString` always has a buffer");
    // SAFETY: ensured by caller
    unsafe { slice::from_raw_parts(bytes.cast::<u8>(), len) }
}
