/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use super::iter_types::TrieMapIteratorImpl;
use super::*;
use lending_iterator::LendingIterator;
use libc::timespec;
use std::ffi::{c_char, c_int, c_void};
use wildcard::WildcardPattern;

/// Used by [`TrieMapIterator`] to determine type of query.
#[repr(C)]
#[allow(dead_code)]
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
    iter: TrieMapIteratorImpl<'tm>,
    timeout: Option<IteratorTimeoutState>,
}

struct IteratorTimeoutState {
    deadline: timespec,
    counter: u8,
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
        iter: TrieMapIteratorImpl::Plain(trie.lending_iter()),
        timeout: None,
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
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
/// - `t` must not be freed while the iterator lives.
/// - `prefix` must point to a valid pointer to a byte sequence of length `prefix_len`,
///   which will be set to the current key. It may only be NULL in case `prefix_len == 0`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieMap_IterateWithFilter<'tm>(
    t: *mut TrieMap,
    prefix: *const c_char,
    prefix_len: tm_len_t,
    iter_mode: tm_iter_mode,
) -> *mut TrieMapIterator<'tm> {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    let pattern: &[u8] = if prefix_len > 0 {
        debug_assert!(!prefix.is_null(), "prefix cannot be NULL if prefix_len > 0");
        // SAFETY: Caller is to ensure that the pointer `prefix` is
        // a valid pointer to a byte sequence of length `prefix_len`.
        unsafe { std::slice::from_raw_parts(prefix.cast(), prefix_len as usize) }
    } else {
        &[]
    };

    // SAFETY: Caller is to ensure that the pointer `t` is
    // a valid, non-null pointer to a TrieMap.
    let TrieMap(trie) = unsafe { &*t };

    let iter = match iter_mode {
        tm_iter_mode::TM_PREFIX_MODE => {
            TrieMapIteratorImpl::Plain(trie.prefixed_lending_iter(pattern))
        }
        tm_iter_mode::TM_CONTAINS_MODE => {
            TrieMapIteratorImpl::Contains(Box::new(trie.contains_iter(pattern).into()))
        }
        tm_iter_mode::TM_SUFFIX_MODE => TrieMapIteratorImpl::Filtered(
            trie.lending_iter()
                .filter(Box::new(|(k, _)| k.ends_with(pattern))),
        ),
        tm_iter_mode::TM_WILDCARD_MODE => TrieMapIteratorImpl::Wildcard(
            trie.wildcard_iter(WildcardPattern::parse(pattern)).into(),
        ),
    };

    let iter = TrieMapIterator {
        iter,
        timeout: None,
    };
    let iter = Box::new(iter);

    Box::into_raw(iter)
}

/// Set timeout limit used for affix queries. This timeout is checked in
/// [`TrieMapIterator_Next`], which will return `0` if the timeout is reached.
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
    let TrieMapIterator {
        timeout: it_timeout,
        ..
    } = unsafe { &mut *it };

    *it_timeout = if timeout.tv_nsec == 0 && timeout.tv_sec == 0 {
        None
    } else {
        Some(IteratorTimeoutState {
            deadline: timeout,
            counter: 0,
        })
    };
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
    let TrieMapIterator { iter, timeout } = unsafe { &mut *it };

    if let Some(IteratorTimeoutState { deadline, counter }) = timeout {
        *counter += 1;
        // For optimized builds, we only check the deadline
        // once every 100 iterations. In development,
        // we're checking each iterationn.
        if *counter == 100 || cfg!(debug_assertions) {
            let now = timespec_monotonic_now();

            if now.tv_sec > deadline.tv_sec && now.tv_nsec > deadline.tv_nsec {
                return 0;
            }

            *counter = 0;
        }
    }

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
