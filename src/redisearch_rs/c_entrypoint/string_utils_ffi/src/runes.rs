/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The C rune API, over [`string_utils::runes`].

use std::{cell::RefCell, ffi::c_char, mem::MaybeUninit, ptr::NonNull, slice};

use string_utils::runes;

thread_local! {
    /// The encoding buffer [`runesToStr`] reuses, so each call allocates only
    /// the buffer it returns. Trie searches call it once per result.
    static ENCODED: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// View `len` items at `ptr` as a slice, which may dangle or be null when
/// `len` is zero.
///
/// # Safety
///
/// 1. When `len` is non-zero, `ptr` must point to `len` initialized items that
///    stay unmodified for `'a`.
const unsafe fn slice_or_empty<'a, T>(ptr: *const T, len: usize) -> &'a [T] {
    if len == 0 {
        &[]
    } else {
        // SAFETY: see safety point 1 above.
        unsafe { slice::from_raw_parts(ptr, len) }
    }
}

/// Copy `items` into a new allocation from the Redis module allocator,
/// followed by one zero item, so the C caller can free it with `rm_free`.
///
/// Not a boxed slice: the global allocator matches `rm_free` only in
/// production. Test binaries built with `mock_allocator` use the system
/// allocator, while `rm_free` expects redis_mock's allocation header.
fn to_module_buffer<T: Copy + Default>(items: &[T]) -> NonNull<T> {
    // SAFETY: `RedisModule_Alloc` is set during module init and not mutated
    // afterwards.
    let alloc = unsafe { redis_module::RedisModule_Alloc }.expect("Redis allocator not available");
    // SAFETY: any size may be requested; the result is checked below.
    let buf = unsafe { alloc(size_of_val(items) + size_of::<T>()) }.cast::<T>();
    let buf = NonNull::new(buf).expect("the Redis allocator returned NULL");
    // SAFETY: `buf` is a fresh allocation, aligned for any type, with room for
    // `items.len() + 1` items, so it cannot overlap `items`.
    unsafe { buf.copy_from_nonoverlapping(NonNull::from(items).cast(), items.len()) };
    // SAFETY: the allocation holds `items.len() + 1` items, so the offset
    // stays within it.
    let end = unsafe { buf.add(items.len()) };
    // SAFETY: `end` is the last item slot of the allocation, aligned as above.
    unsafe { end.write(T::default()) };
    buf
}

/// Fold `r` with [`runes::fold_rune`].
#[unsafe(no_mangle)]
pub extern "C" fn runeFold(r: u16) -> u16 {
    runes::fold_rune(r)
}

/// Encode the `len` runes at `in_` as UTF-8 with
/// [`runes::runes_to_bytes_into`], into a NUL-terminated buffer from the Redis
/// module allocator that the caller owns.
///
/// Writes the encoded length, terminator excluded, to `*utflen`. Returns NULL,
/// writing 0, when [`runes::runes_to_bytes_into`] rejects the input.
///
/// # Safety
///
/// 1. When `len` is non-zero, `in_` must point to `len` initialized runes.
/// 2. `utflen` must be a valid pointer to a `size_t`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn runesToStr(
    in_: *const u16,
    len: usize,
    utflen: *mut usize,
) -> *mut c_char {
    // SAFETY: see safety point 1 above.
    let runes = unsafe { slice_or_empty(in_, len) };
    let (ptr, n) =
        ENCODED.with_borrow_mut(|bytes| match runes::runes_to_bytes_into(runes, bytes) {
            Ok(()) => (to_module_buffer(bytes).as_ptr(), bytes.len()),
            Err(_) => (std::ptr::null_mut(), 0),
        });
    // SAFETY: see safety point 2 above.
    unsafe { utflen.write(n) };
    ptr.cast()
}

/// Lowercase the `utf8_len` bytes at `s` into runes with
/// [`runes::lower_runes`], into a zero-terminated buffer from the Redis module
/// allocator that the caller owns.
///
/// Writes the rune count, terminator excluded, to `*unicode_len`. Returns NULL,
/// writing 0, when [`runes::lower_runes`] rejects the input. Nothing past
/// `utf8_len` bytes is read, and the input need not be NUL-terminated.
///
/// # Safety
///
/// 1. When `utf8_len` is non-zero, `s` must point to `utf8_len` readable
///    bytes.
/// 2. `unicode_len` must be a valid pointer to a `size_t`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn strToLowerRunes(
    s: *const c_char,
    utf8_len: usize,
    unicode_len: *mut usize,
) -> *mut u16 {
    // SAFETY: see safety point 1 above.
    let bytes = unsafe { slice_or_empty(s.cast::<u8>(), utf8_len) };
    let (ptr, n) = match runes::lower_runes(bytes) {
        Ok(runes) => (to_module_buffer(&runes).as_ptr(), runes.len()),
        Err(_) => (std::ptr::null_mut(), 0),
    };
    // SAFETY: see safety point 2 above.
    unsafe { unicode_len.write(n) };
    ptr
}

/// Fold the `utf8_len` bytes at `s` into runes with [`runes::fold_runes`],
/// into a zero-terminated buffer from the Redis module allocator that the
/// caller owns.
///
/// Writes the rune count, terminator excluded, to `*len` unless `len` is NULL.
/// Returns NULL, writing 0, when [`runes::fold_runes`] rejects the input.
/// Nothing past `utf8_len` bytes is read, and the input need not be
/// NUL-terminated.
///
/// # Safety
///
/// 1. When `utf8_len` is non-zero, `s` must point to `utf8_len` readable
///    bytes.
/// 2. `len` must be NULL or a valid pointer to a `size_t`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn strToSingleCodepointFoldedRunes(
    s: *const c_char,
    utf8_len: usize,
    len: *mut usize,
) -> *mut u16 {
    // SAFETY: see safety point 1 above.
    let bytes = unsafe { slice_or_empty(s.cast::<u8>(), utf8_len) };
    let (ptr, n) = match runes::fold_runes(bytes) {
        Ok(runes) => (to_module_buffer(&runes).as_ptr(), runes.len()),
        Err(_) => (std::ptr::null_mut(), 0),
    };
    // SAFETY: see safety point 2 above.
    if let Some(len) = unsafe { len.as_mut() } {
        *len = n;
    }
    ptr
}

/// Decode the `slen` bytes at `src` into runes with [`runes::decode_runes`],
/// writing at most `outcap` of them to `out` and never zero-terminating.
///
/// Returns the input's full rune count, which exceeds `outcap` when the output
/// was truncated. Nothing past `slen` bytes is read, and the input need not be
/// NUL-terminated.
///
/// # Safety
///
/// 1. When `slen` is non-zero, `src` must point to `slen` readable bytes.
/// 2. When `outcap` is non-zero, `out` must point to `outcap` writable runes
///    that do not overlap `src`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn strToRunes(
    src: *const c_char,
    slen: usize,
    out: *mut u16,
    outcap: usize,
) -> usize {
    // SAFETY: see safety point 1 above.
    let bytes = unsafe { slice_or_empty(src.cast::<u8>(), slen) };
    // Callers pass uninitialized storage, so the slots are viewed as
    // `MaybeUninit` and only written.
    let out: &mut [MaybeUninit<u16>] = if outcap == 0 {
        &mut []
    } else {
        // SAFETY: see safety point 2 above.
        unsafe { slice::from_raw_parts_mut(out.cast(), outcap) }
    };
    let mut count = 0;
    for rune in runes::decode_runes(bytes) {
        if let Some(slot) = out.get_mut(count) {
            slot.write(rune);
        }
        count += 1;
    }
    count
}
