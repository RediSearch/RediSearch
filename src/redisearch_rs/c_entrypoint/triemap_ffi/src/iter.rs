/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use super::*;
use lending_iterator::LendingIterator;
use libc::timespec;
use std::{
    ffi::{c_char, c_int, c_void},
    time::{Duration, Instant},
};
use trie_rs::iter::{PatternLendingIter, PatternMode};

/// Used by [`TrieMapIterator`] to determine type of query.
#[repr(C)]
#[derive(Debug)]
pub enum tm_iter_mode {
    TM_PREFIX_MODE = 0,
    TM_CONTAINS_MODE = 1,
    TM_SUFFIX_MODE = 2,
    TM_WILDCARD_MODE = 3,
}

/// Opaque type TrieMapIterator. Obtained from calling [`TrieMap_Iterate`] or
/// [`TrieMap_IterateWithFilter`].
pub struct TrieMapIterator<'tm> {
    // The trie and the C-owned pattern come from the same C-side scope, so
    // both lifetimes collapse to `'tm` at the FFI boundary.
    iter: PatternLendingIter<'tm, 'tm, *mut c_void>,
}

/// Iterate over all the entries stored in the trie.
///
/// Invoke [`TrieMapIterator_Next`] to get the results from the iteration. If there are no entries,
/// the first call to next will return 0.
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
/// - `t` must not be freed while the iterator lives.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieMap_Iterate<'tm>(t: *mut TrieMap) -> *mut TrieMapIterator<'tm> {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: Caller is to ensure that the pointer `t` is
    // a valid, non-null pointer to a TrieMap.
    let TrieMap(trie) = unsafe { &*t };

    let iter = Box::new(TrieMapIterator {
        iter: trie.lending_iter().into(),
    });

    Box::into_raw(iter)
}

/// Iterate over the trie entries that match the given predicate.
///
/// Depending on `iter_mode`, they can either be:
/// - All entries with a given key prefix;
/// - All entries with a given key suffix;
/// - All entries with a key that contains the specified string;
/// - All entries with a key matching the specified wildcard pattern.
///
/// This method returns an iterator object. Invoke [`TrieMapIterator_Next`]
/// to get the results from the iteration. If no entry is found,
/// the first call to next will return 0.
///
/// # Safety
/// 1. `t` must point to a valid trie obtained from [`NewTrieMap`] and remain live
///    and immutable until the iterator is freed.
/// 2. If `prefix_len` is nonzero, `prefix` must point to that many initialized
///    bytes that remain live and immutable until the iterator is freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieMap_IterateWithFilter<'tm>(
    t: *mut TrieMap,
    prefix: *const c_char,
    prefix_len: tm_len_t,
    iter_mode: tm_iter_mode,
) -> *mut TrieMapIterator<'tm> {
    // SAFETY: preconditions 1 and 2 cover the iterator's borrows.
    let iter = unsafe { filtered_iter(t, prefix, prefix_len, iter_mode) };
    Box::into_raw(Box::new(TrieMapIterator { iter }))
}

/// Return true to stop a filtered walk before the next match.
pub type TrieMapStopCallback = Option<unsafe extern "C" fn(*const c_void) -> bool>;

/// Consume a matched value, returning false to finish the walk.
pub type TrieMapVisitCallback = unsafe extern "C" fn(*mut c_void, *mut c_void) -> bool;

/// Visit values matching `pattern`, polling `stop` during traversal between matches.
///
/// # Safety
/// 1. The trie and pattern must meet the requirements of [`TrieMap_IterateWithFilter`].
///    Both must stay live and immutable throughout the call, including during callbacks.
/// 2. `visit` must be a non-null C callback that accepts a matched value (which may be
///    null) and `visit_ctx`. `stop`, if supplied, must accept `stop_ctx`. The contexts
///    must satisfy their callbacks' requirements for the whole call.
/// 3. Neither callback may unwind across the FFI boundary.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieMap_VisitWithFilter(
    trie: *mut TrieMap,
    pattern: *const c_char,
    len: tm_len_t,
    mode: tm_iter_mode,
    stop: TrieMapStopCallback,
    stop_ctx: *const c_void,
    visit: TrieMapVisitCallback,
    visit_ctx: *mut c_void,
) {
    struct StopCheck(unsafe extern "C" fn(*const c_void) -> bool, *const c_void);

    // SAFETY: this private wrapper stays in the synchronous visitor call and is
    // used only by the iterator on the calling thread.
    unsafe impl Sync for StopCheck {}

    impl StopCheck {
        fn should_stop(&self) -> bool {
            // SAFETY: preconditions 2 and 3 cover the callback and its context.
            unsafe { (self.0)(self.1) }
        }
    }

    let stop = stop.map(|callback| StopCheck(callback, stop_ctx));
    let should_stop = || stop.as_ref().is_some_and(StopCheck::should_stop);
    // SAFETY: precondition 1 covers the trie and pattern throughout this walk.
    let mut iter = unsafe { filtered_iter(trie, pattern, len, mode) };
    if stop.is_some() {
        iter.set_stop_check(&should_stop);
    }
    while let Some((_, value)) = iter.next() {
        // SAFETY: preconditions 2 and 3 cover the visitor, its value and context.
        if !unsafe { visit(*value, visit_ctx) } {
            break;
        }
    }
}

/// # Safety
/// 1. `t` must point to a live trie, immutably borrowed for `'tm`.
/// 2. If `prefix_len` is nonzero, `prefix` must point to that many initialized
///    bytes, immutably borrowed for `'tm`.
unsafe fn filtered_iter<'tm>(
    t: *mut TrieMap,
    prefix: *const c_char,
    prefix_len: tm_len_t,
    iter_mode: tm_iter_mode,
) -> PatternLendingIter<'tm, 'tm, *mut c_void> {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    let pattern: &[u8] = if prefix_len > 0 {
        debug_assert!(!prefix.is_null(), "prefix cannot be NULL if prefix_len > 0");
        // SAFETY: precondition 2 covers the reconstructed pattern slice.
        unsafe { std::slice::from_raw_parts(prefix.cast(), prefix_len as usize) }
    } else {
        &[]
    };

    // SAFETY: precondition 1 covers the reconstructed trie reference.
    let TrieMap(trie) = unsafe { &*t };

    let mode = match iter_mode {
        tm_iter_mode::TM_PREFIX_MODE => PatternMode::Prefix,
        tm_iter_mode::TM_CONTAINS_MODE => PatternMode::Contains,
        tm_iter_mode::TM_SUFFIX_MODE => PatternMode::Suffix,
        tm_iter_mode::TM_WILDCARD_MODE => PatternMode::Wildcard,
    };
    trie.pattern_lending_iter(pattern, mode)
}

/// Set timeout limit used for affix queries. The deadline is enforced inside
/// the underlying trie iterator's traversal loop; once it is reached
/// [`TrieMapIterator_Next`] returns `0`, as if the iterator were exhausted.
///
/// If the provided timeout is 0, it's interpreted as unlimited.
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `it` must point to a valid [`TrieMapIterator`] obtained from [`TrieMap_Iterate`] or
///   [`TrieMap_IterateWithFilter`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieMapIterator_SetTimeout(it: *mut TrieMapIterator, timeout: timespec) {
    debug_assert!(!it.is_null(), "it cannot be NULL");

    // SAFETY: caller is to ensure `it` points to a valid
    // TrieMapIterator obtained from `TrieMap_Iterate`
    let TrieMapIterator { iter } = unsafe { &mut *it };

    let timeout = if timeout.tv_nsec == 0 && timeout.tv_sec == 0 {
        None
    } else {
        // Convert libc monotonic now into Instant. Both are monotonic so the
        // conversion is exact and immune to wall-clock adjustments.
        let now = timespec_monotonic_now();
        let deadline_since_boot = Duration::new(timeout.tv_sec as u64, timeout.tv_nsec as u32);
        let now_since_boot = Duration::new(now.tv_sec as u64, now.tv_nsec as u32);
        let remaining = deadline_since_boot.saturating_sub(now_since_boot);

        Some(Instant::now() + remaining)
    };
    iter.set_timeout(timeout);
}

/// Free a trie iterator
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `it` must point to a valid [`TrieMapIterator`] obtained from [`TrieMap_Iterate`] or
///   [`TrieMap_IterateWithFilter`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieMapIterator_Free(it: *mut TrieMapIterator) {
    debug_assert!(!it.is_null(), "it cannot be NULL");

    // SAFETY: caller is to ensure `it` points to a valid
    // TrieMapIterator obtained from `TrieMap_Iterate`
    unsafe {
        let _ = Box::from_raw(it);
    };
}

/// Iterate to the next matching entry in the trie. Returns 1 if we can continue,
/// or 0 if we're done and should exit
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `it` must point to a valid [`TrieMapIterator`] obtained from [`TrieMap_Iterate`] or
///   [`TrieMap_IterateWithFilter`] and cannot be NULL.
/// - `ptr` must point to a valid pointer to a byte sequence, which will be set to the current key. This
///   pointer is invalidated upon calling [`TrieMapIterator_Next`] again.
/// - `len` must point to a valid `tm_len_t` which will be set to the length of the current key.
/// - `value` must point to a valid pointer, which will be set to the value of the current key.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieMapIterator_Next(
    it: *mut TrieMapIterator,
    ptr: *mut *mut c_char,
    len: *mut tm_len_t,
    value: *mut *mut c_void,
) -> c_int {
    debug_assert!(!it.is_null(), "it cannot be NULL");
    debug_assert!(!ptr.is_null(), "ptr cannot be NULL");
    debug_assert!(!len.is_null(), "len cannot be NULL");
    debug_assert!(!value.is_null(), "value cannot be NULL");

    // SAFETY: caller is to ensure that the iterator is valid and not null
    let TrieMapIterator { iter } = unsafe { &mut *it };

    let Some((k, v)) = LendingIterator::next(iter) else {
        return 0;
    };

    // SAFETY: caller is to ensure that `ptr` is
    // a mutable, well-aligned pointer to a `c_char` array
    unsafe {
        ptr.write(k.as_ptr().cast::<c_char>().cast_mut());
    }
    // SAFETY: caller is to ensure that `len` is
    // a mutable, well-aligned pointer to a `tm_len_t`
    unsafe {
        len.write(k.len() as tm_len_t);
    }
    // SAFETY: caller is to ensure that `ptr` is
    // a mutable, well-aligned pointer to a `*mut c_void`
    unsafe {
        value.write(*v);
    }

    1
}

/// Get current time from monotonic clock.
/// Calls `clock_gettime` with `clk_id == CLOCK_MONOTONIC_RAW`.
pub fn timespec_monotonic_now() -> timespec {
    let mut ts = std::mem::MaybeUninit::uninit();
    // SAFETY:
    // We have exclusive access to a pointer of the correct type
    let ret = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, ts.as_mut_ptr()) };
    if ret == 0 {
        // SAFETY:
        // `ts` was initialized by before call to `clock_gettime`
        unsafe { ts.assume_init() }
    } else {
        panic!("Couldn't get the current time from the system monotonic clock")
    }
}
