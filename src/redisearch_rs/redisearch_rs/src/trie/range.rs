/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use super::TrieMap;
use lending_iterator::LendingIterator as _;
use std::ffi::{c_char, c_int, c_void};
use trie_rs::iter::{RangeBoundary, RangeFilter, RangeLendingIter};

/// Callback type for passing to [`TrieMap_IterateRange`].
pub type TrieMapRangeCallback =
    Option<unsafe extern "C" fn(*const c_char, libc::size_t, *mut c_void, *mut c_void)>;

/// Iterate the trie within the specified key range.
///
/// If `minLen` is 0, `min` is regarded as an empty string. It `minlen` is -1, the itaration starts from the beginning of the trie.
/// If `maxLen` is 0, `max` is regarded as an empty string. If `maxlen` is -1, the iteration goes to the end of the trie.
/// `includeMin` and `includeMax` determine whether the min and max values are included in the iteration.
///
/// The passed [`TrieMapRangeCallback`] function is called for each key found,
/// passing the key and its length, the value, and the `ctx` pointer passed to this
/// function.
///
/// Panics in case the passed callback is NULL.
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `trie` must point to a valid TrieMap obtained from [`NewTrieMap`] and cannot be NULL.
/// - `min` can be NULL only if `minlen == 0` or `minlen == -1`. It is not necessarily NULL-terminated.
/// - `minlen` can be 0. If so, `min` is regarded as an empty string.
/// - `max` can be NULL only if `maxlen == 0` or `maxlen == -1`. It is not necessarily NULL-terminated.
/// - `maxlen` can be 0. If so, `max` is regarded as an empty string.
/// - `callback` must be a valid pointer to a function of type [`TrieMapRangeCallback`]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrieMap_IterateRange(
    trie: *mut TrieMap,
    min: *const c_char, // May be NULL iff minlen == 0
    minlen: c_int,      // if 0, execute special case
    includeMin: bool,
    max: *const c_char, // May be NULL iff minlen == 0
    maxlen: c_int,
    includeMax: bool,
    callback: TrieMapRangeCallback,
    ctx: *mut c_void,
) {
    let Some(callback) = callback else {
        #[cfg(debug_assertions)]
        {
            panic!("TrieMap_IterateRange with a NULL callback");
        }
        #[cfg(not(debug_assertions))]
        {
            return; // It makes no sense to iterate without a callback
        }
    };

    debug_assert!(!trie.is_null(), "trie cannot be NULL");

    let min: Option<&[u8]> = match minlen {
        ..0 => None,
        0 => Some([].as_slice()),
        1.. => {
            debug_assert!(!min.is_null(), "min cannot be NULL if minlen > 0");
            // SAFETY: caller is to ensure that min is not null in case minlen > 0,
            // and that min points to a contiguous slice of bytes of len minlen
            Some(unsafe { std::slice::from_raw_parts(min.cast(), minlen as usize) })
        }
    };

    let max: Option<&[u8]> = match maxlen {
        ..0 => None,
        0 => Some([].as_slice()),
        1.. => {
            debug_assert!(!max.is_null(), "max cannot be NULL if maxlen > 0");
            // SAFETY: caller is to ensure that max is not null in case maxlen > 0,
            // and that max points to a contiguous slice of bytes of len maxlen
            Some(unsafe { std::slice::from_raw_parts(max.cast(), maxlen as usize) })
        }
    };

    // SAFETY: caller is to ensure that `trie` is valid and not null
    let TrieMap(trie) = unsafe { &mut *trie };

    let filter = RangeFilter {
        min: min.map(|m| RangeBoundary {
            value: m,
            is_included: includeMin,
        }),
        max: max.map(|m| RangeBoundary {
            value: m,
            is_included: includeMax,
        }),
    };
    let iter: RangeLendingIter<_> = trie.range_iter(filter).into();
    iter.fuse().for_each(|(key, value)| {
        let key_len = key.len();
        // `u8` and `c_char` can be safely transmuted back and forth.
        let key_ptr = key.as_ptr().cast();
        // Safety: caller is to ensure `callback` be
        // a valid pointer to a function of type [`TrieMapRangeCallback`]
        unsafe {
            (callback)(key_ptr, key_len, ctx, *value);
        }
    });
}
