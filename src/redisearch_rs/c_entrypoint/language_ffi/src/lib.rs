/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI layer to look up, from C, the [`Language`] of a name and the names of a
//! [`Language`].
//!
//! Functions that take a language take it as a raw `u32`, not as [`Language`]:
//! C can store any integer in an `RSLanguage`, and receiving an out-of-range
//! discriminant into a Rust enum is undefined behavior.

use std::{
    ffi::{CStr, c_char},
    ptr, slice,
};

use language::Language;

/// Looks up a supported language by name. The match is ASCII
/// case-insensitive over exactly `len` bytes, so `language` need not be
/// NUL-terminated (see [`Language::from_name`]).
///
/// - If `language` is NULL, returns `DEFAULT_LANGUAGE`
///   ([`Language::default`]), whatever `len` is.
/// - If `len` is 0, returns `RS_LANG_UNSUPPORTED` ([`Language::Unsupported`])
///   without reading `language`.
/// - If no language has that name, returns `RS_LANG_UNSUPPORTED`.
///
/// # Safety
///
/// 1. When `language` is non-null and `len` is non-zero, `language` must point
///    to a [valid] region of memory of length `len`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSLanguage_Find(language: *const c_char, len: usize) -> Language {
    if language.is_null() {
        return Language::default();
    }
    if len == 0 {
        return Language::Unsupported;
    }

    // SAFETY: 1. both early returns above ruled out a null `language` and a
    // zero `len`, so the caller guarantees `len` readable bytes at `language`.
    let name = unsafe { slice::from_raw_parts(language.cast::<u8>(), len) };

    Language::from_name(name).unwrap_or(Language::Unsupported)
}

/// Returns the lowercase name of `language`, such as `"english"`
/// ([`Language::name`]), as a static string the caller must not free.
///
/// Returns NULL for `RS_LANG_UNSUPPORTED`, `RS_LANG_UNSET`, and any value that
/// is not an `RSLanguage`.
#[unsafe(no_mangle)]
pub extern "C" fn RSLanguage_ToString(language: u32) -> *const c_char {
    to_c_str(language, Language::name)
}

/// Returns the name of the libstemmer (snowball) algorithm for `language`, to
/// pass to `sb_stemmer_new`, as a static string the caller must not free.
///
/// The same as [`RSLanguage_ToString`], except for the languages
/// [`Language::snowball_stemmer`] remaps. Returns NULL for the same values as
/// [`RSLanguage_ToString`].
#[unsafe(no_mangle)]
pub extern "C" fn RSLanguage_ToSnowballStemmer(language: u32) -> *const c_char {
    to_c_str(language, Language::snowball_stemmer)
}

/// Applies `f` to `language`, or returns NULL when `language` is not a
/// [`Language`]; the [crate] docs say why it arrives as a raw `u32`.
fn to_c_str(language: u32, f: fn(Language) -> Option<&'static CStr>) -> *const c_char {
    usize::try_from(language)
        .ok()
        .and_then(Language::from_repr)
        .and_then(f)
        .map_or(ptr::null(), CStr::as_ptr)
}
