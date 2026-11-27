/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Indexing stemmers owning the vendored Snowball backend and expansion storage.

#![warn(
    clippy::unwrap_used,
    clippy::dbg_macro,
    clippy::todo,
    clippy::unimplemented,
    clippy::print_stdout,
    clippy::panic,
    clippy::unnecessary_safety_comment,
    clippy::ptr_as_ptr,
    clippy::borrow_as_ptr,
    clippy::ref_as_ptr,
    clippy::indexing_slicing
)]

use language_ffi::RSLanguage;
use std::{
    ffi::{c_char, c_int, c_void},
    ptr::{self, NonNull},
    slice,
};
use stemmer::StemBuffer;

/// C export of [`stemmer::STEM_PREFIX`].
#[cheadergen::config(export)]
pub const STEM_PREFIX: u8 = stemmer::STEM_PREFIX;

/// Available indexing stemmer implementations.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
#[cheadergen::config(export)]
pub enum StemmerType {
    /// The vendored Snowball algorithms.
    SnowballStemmer,
}

/// Caller-facing callbacks; release the owner through [`Free`](Self::Free), never a C allocator.
#[repr(C)]
#[expect(non_snake_case)]
#[cheadergen::config(export)]
pub struct Stemmer {
    /// Exclusively owned backend and output storage.
    pub ctx: *mut c_void,
    /// Returned bytes remain readable until the next call or destruction.
    pub Stem: Option<
        unsafe extern "C" fn(*mut c_void, *const c_char, usize, *mut usize) -> *const c_char,
    >,
    /// Consumes this owner and its context exactly once.
    pub Free: Option<unsafe extern "C" fn(*mut Stemmer)>,
    /// Returns nonzero when the existing backend can serve the requested document.
    pub Reset: Option<unsafe extern "C" fn(*mut Stemmer, StemmerType, RSLanguage) -> c_int>,
    /// Language used to construct the backend.
    pub language: RSLanguage,
    /// Backend implementation.
    pub r#type: StemmerType,
}

struct Snowball {
    backend: NonNull<ffi::sb_stemmer>,
    buffer: StemBuffer,
}

#[track_caller]
fn debug_assert_non_null<T>(pointer: *mut T) -> *mut T {
    debug_assert!(!pointer.is_null());
    pointer
}

/// Create a caller-owned stemmer, or return null when the language has no algorithm.
#[unsafe(no_mangle)]
pub extern "C" fn NewStemmer(r#type: StemmerType, language: RSLanguage) -> *mut Stemmer {
    let Some(algorithm) = language.language() else {
        return ptr::null_mut();
    };
    // SAFETY: the algorithm name is static and NUL-terminated; null selects UTF-8.
    let backend =
        unsafe { ffi::sb_stemmer_new(algorithm.snowball_stemmer().as_ptr(), ptr::null()) };
    let Some(backend) = NonNull::new(backend) else {
        return ptr::null_mut();
    };
    let ctx = Box::into_raw(Box::new(Snowball {
        backend,
        buffer: StemBuffer::default(),
    }))
    .cast();
    Box::into_raw(Box::new(Stemmer {
        ctx,
        Stem: Some(stem),
        Free: Some(free),
        Reset: Some(reset),
        language,
        r#type,
    }))
}

/// Ask the reset callback whether this stemmer can be reused; a missing callback returns zero.
///
/// # Safety
/// 1. `stemmer` must point to a [valid], exclusively accessible [`Stemmer`].
/// 2. Its reset callback, when present, must be [ABI-compatible] with its field and
///    accept this owner, `r#type` and `language`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
/// [ABI-compatible]: https://doc.rust-lang.org/std/primitive.fn.html#abi-compatibility
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ResetStemmer(
    stemmer: *mut Stemmer,
    r#type: StemmerType,
    language: RSLanguage,
) -> c_int {
    // SAFETY: 1. The caller supplies an initialized, exclusively accessible owner.
    let callback = unsafe { debug_assert_non_null(stemmer).as_ref() }
        .expect("non-null stemmer")
        .Reset;
    // SAFETY: 2. The callback accepts this live owner and the requested type and language.
    callback.map_or(0, |callback| unsafe { callback(stemmer, r#type, language) })
}

/// # Safety
/// 1. `ctx` must point to a [valid], exclusively accessible [`Snowball`] from [`NewStemmer`].
/// 2. `word` must be non-null and [valid] for reading `len` initialized bytes, with `len`
///    representable by [`c_int`].
/// 3. `outlen` must be [valid] for writing one aligned length, disjoint from `word` and `ctx`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn stem(
    ctx: *mut c_void,
    word: *const c_char,
    len: usize,
    outlen: *mut usize,
) -> *const c_char {
    // SAFETY: 1. NewStemmer created this exclusively accessible context.
    let ctx = unsafe { debug_assert_non_null(ctx).cast::<Snowball>().as_mut() }
        .expect("non-null context");
    let word = debug_assert_non_null(word.cast_mut());
    let outlen = debug_assert_non_null(outlen);
    let len = c_int::try_from(len).expect("token lengths fit in int");
    // SAFETY: 1, 2. The backend is owned here and word spans len readable bytes.
    let result = unsafe { ffi::sb_stemmer_stem(ctx.backend.as_ptr(), word.cast(), len) };
    if result.is_null() {
        return ptr::null();
    }
    // SAFETY: 1. A successful Snowball call initialized the backend length.
    let result_len = unsafe { ffi::sb_stemmer_length(ctx.backend.as_ptr()) } as usize;
    // SAFETY: 3. The output may be uninitialized and is disjoint from both input and context.
    unsafe { outlen.write(result_len) };
    // SAFETY: Snowball owns result_len initialized bytes until its next update or deletion.
    let result = unsafe { slice::from_raw_parts(result, result_len) };
    // SAFETY: 2. The caller provides len initialized input bytes.
    let original = unsafe { slice::from_raw_parts(word.cast(), len as usize) };
    let Some(expansion) = ctx.buffer.update(original, result) else {
        return ptr::null();
    };
    // SAFETY: 3. Writing the length does not alias the expansion or input.
    unsafe { outlen.write(expansion.len() - 1) };
    expansion.as_ptr().cast()
}

/// # Safety
/// 1. `stemmer` must be a live owner from [`NewStemmer`], exclusively transferred to this call.
///    Neither the owner nor its returned bytes may be used afterwards.
unsafe extern "C" fn free(stemmer: *mut Stemmer) {
    // SAFETY: 1. NewStemmer allocated this owner with Box::into_raw; ownership is transferred once.
    let owner = unsafe { Box::from_raw(debug_assert_non_null(stemmer)) };
    // SAFETY: 1. The owner holds the unique context allocated with Box::into_raw.
    let ctx = unsafe { Box::from_raw(owner.ctx.cast::<Snowball>()) };
    // SAFETY: 1. The unique backend was created with sb_stemmer_new and is no longer borrowed.
    unsafe { ffi::sb_stemmer_delete(ctx.backend.as_ptr()) };
}

/// # Safety
/// 1. `stemmer` must point to a [valid], immutably accessible [`Stemmer`].
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe extern "C" fn reset(
    stemmer: *mut Stemmer,
    r#type: StemmerType,
    language: RSLanguage,
) -> c_int {
    // SAFETY: 1. ResetStemmer passes its live owner to the installed callback.
    let owner = unsafe { debug_assert_non_null(stemmer).as_ref() }.expect("non-null stemmer");
    (owner.r#type == r#type && owner.language == language).into()
}
