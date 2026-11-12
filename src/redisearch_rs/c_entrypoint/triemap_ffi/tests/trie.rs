/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
use redis_mock::mock_or_stub_missing_redis_c_symbols;
use std::ffi::{c_char, c_void};
use triemap_ffi::*;

mock_or_stub_missing_redis_c_symbols!();

macro_rules! assert_entries {
    ($pattern:literal, $mode:expr, $expected:expr $(,)?) => {
        with_trie_iter($pattern, $mode, |entries| {
            assert_eq!(
                entries,
                $expected.map(|(k, v)| (k.to_owned(), v)),
                "Pattern {:?} should have yielded entries {:?} in mode {:?}",
                String::from_utf8_lossy($pattern),
                $expected,
                $mode,
            );
        });
    };
}

const unsafe extern "C" fn do_not_free(_val: *mut c_void) {
    // We're using stack-allocated types (i.e. integers) as values,
    // so there's nothing to be freed.
}

/// Create a [`TrieMap`], fill it with entries,
/// call the callback passing the [`TrieMap`] pointer,
/// and free the map.
///
/// Map structure at the point the callback in invoked:
///
/// ```text
/// "" (-)
///  ↳––––"bi" (-)
///        ↳––––"ke" (&0)
///              ↳––––"r" (&1)
///        ↳––––"s" (&2)
///  ↳––––"c" (-)
///        ↳––––"ider" (&3)
///        ↳––––"ool" (&4)
///              ↳––––"er" (&5)
/// ```
fn with_trie_map<F>(f: F)
where
    F: FnOnce(*mut TrieMap),
{
    let t = NewTrieMap();
    let entries = [
        (b"bike".as_slice(), 0u8),
        (b"biker", 1),
        (b"bis", 2),
        (b"cool", 3),
        (b"cooler", 4),
        (b"cider", 5),
    ];
    for (entry, value) in entries.iter() {
        // Safety: We adhere to all the safety requirements of `TrieMap_Add`
        unsafe {
            TrieMap_Add(
                t,
                entry.as_ptr().cast(),
                entry.len().try_into().unwrap(),
                value as *const u8 as *mut c_void,
                None,
            )
        };
    }

    f(t);

    // Safety: We adhere to all the safety requirements of `TrieMap_Free`
    unsafe { TrieMap_Free(t, Some(do_not_free)) };
}

/// Creates a map using [`with_trie_map`],
/// sets up a [`TrieMapIterator`] with the passed
/// config, collects the iteration results in a
/// [`Vec<(String, u8)>`] of which each item
/// corresponds to one entry the iterator yielded.
/// Then, calls the callback, passing the entries
/// and takes care of freeing the iterator.
fn with_trie_iter<F, const N: usize>(pattern: &[u8; N], iter_mode: tm_iter_mode, f: F)
where
    F: FnOnce(Vec<(String, u8)>),
{
    with_trie_map(|t| {
        // Safety: We adhere to all the safety requirements of `TrieMap_Iterate`
        let it = unsafe {
            TrieMap_IterateWithFilter(
                t,
                pattern.as_ptr().cast(),
                pattern.len() as tm_len_t,
                iter_mode,
            )
        };

        let mut char: *mut c_char = std::ptr::null_mut();
        let mut len: tm_len_t = 0;
        let mut value: *mut c_void = std::ptr::null_mut();

        let mut entries = Vec::new();
        // Safety: We adhere to all the safety requirements of `TrieMap_Next`.
        while let 1 = unsafe {
            TrieMapIterator_Next(
                it,
                &mut char as *mut *mut c_char,
                &mut len as *mut tm_len_t,
                &mut value as *mut *mut c_void,
            )
        } {
            // Safety: We're reconstructing the keys and the values created in `with_trie_map`
            let key: &[u8] = unsafe { std::slice::from_raw_parts(char.cast(), len as usize) };
            let key = String::from_utf8(key.to_vec()).unwrap();

            // Safety: We're reconstructing the keys and the values created in `with_trie_map`
            let value = unsafe { *(value as *mut u8) };

            entries.push((key, value));
        }

        f(entries);

        // Safety: We adhere to all the safety requirements of `TrieMapIterator_Free`
        unsafe { TrieMapIterator_Free(it) };
    });
}

#[test]
fn test_trie_find_prefixes() {
    with_trie_map(|t| {
        let prefix = "bistro".as_bytes();

        // Safety: We adhere to all the safety requirements of `TrieMap_FindPrefixes`
        let buf =
            unsafe { TrieMap_FindPrefixes(t, prefix.as_ptr().cast(), prefix.len() as tm_len_t) };
        let mut results = Vec::with_capacity(buf.0.len());
        for &v in &buf.0 {
            // Safety: `v` was created in `with_trie_map`
            // and is a pointer to a `u8` value in disguise.
            let value = unsafe { *(v as *mut u8) };
            results.push(value);
        }

        assert_eq!(results, &[2]);

        TrieMapResultBuf_Free(buf);
    });
}

#[test]
fn test_trie_iter_prefix() {
    assert_entries!(
        b"bi",
        tm_iter_mode::TM_PREFIX_MODE,
        [("bike", 0), ("biker", 1), ("bis", 2)],
    );

    assert_entries!(b"ci", tm_iter_mode::TM_PREFIX_MODE, [("cider", 5)],);

    assert_entries!(
        b"",
        tm_iter_mode::TM_PREFIX_MODE,
        [
            ("bike", 0),
            ("biker", 1),
            ("bis", 2),
            ("cider", 5),
            ("cool", 3),
            ("cooler", 4),
        ],
    );
}

#[test]
fn test_trie_iter_contains() {
    assert_entries!(
        b"ik",
        tm_iter_mode::TM_CONTAINS_MODE,
        [("bike", 0), ("biker", 1)],
    );
}

#[test]
fn test_trie_iter_suffix() {
    assert_entries!(
        b"er",
        tm_iter_mode::TM_SUFFIX_MODE,
        [("biker", 1), ("cider", 5), ("cooler", 4)],
    );
}

#[test]
fn test_trie_iter_wildcard() {
    assert_entries!(
        b"*",
        tm_iter_mode::TM_WILDCARD_MODE,
        [
            ("bike", 0),
            ("biker", 1),
            ("bis", 2),
            ("cider", 5),
            ("cool", 3),
            ("cooler", 4),
        ],
    );

    assert_entries!(
        b"c*",
        tm_iter_mode::TM_WILDCARD_MODE,
        [("cider", 5), ("cool", 3), ("cooler", 4)],
    );

    assert_entries!(
        b"*r",
        tm_iter_mode::TM_WILDCARD_MODE,
        [("biker", 1), ("cider", 5), ("cooler", 4)],
    );

    assert_entries!(
        b"*i*",
        tm_iter_mode::TM_WILDCARD_MODE,
        [("bike", 0), ("biker", 1), ("bis", 2), ("cider", 5)],
    );

    assert_entries!(
        b"*i*",
        tm_iter_mode::TM_WILDCARD_MODE,
        [("bike", 0), ("biker", 1), ("bis", 2), ("cider", 5)],
    );

    assert_entries!(
        b"?i?er",
        tm_iter_mode::TM_WILDCARD_MODE,
        [("biker", 1), ("cider", 5)],
    );

    assert_entries!(
        b"????",
        tm_iter_mode::TM_WILDCARD_MODE,
        [("bike", 0), ("cool", 3)],
    );

    assert_entries!(b"ci???", tm_iter_mode::TM_WILDCARD_MODE, [("cider", 5)],);

    assert_entries!(b"cider", tm_iter_mode::TM_WILDCARD_MODE, [("cider", 5)],);

    assert_entries!(
        b"******?",
        tm_iter_mode::TM_WILDCARD_MODE,
        [
            ("bike", 0),
            ("biker", 1),
            ("bis", 2),
            ("cider", 5),
            ("cool", 3),
            ("cooler", 4),
        ],
    );

    assert_entries!(
        b"*????",
        tm_iter_mode::TM_WILDCARD_MODE,
        [
            ("bike", 0),
            ("biker", 1),
            ("cider", 5),
            ("cool", 3),
            ("cooler", 4),
        ],
    );

    assert_entries!(
        b"?i?er",
        tm_iter_mode::TM_WILDCARD_MODE,
        [("biker", 1), ("cider", 5)],
    );

    assert_entries!(
        b"????",
        tm_iter_mode::TM_WILDCARD_MODE,
        [("bike", 0), ("cool", 3)],
    );

    assert_entries!(b"ci???", tm_iter_mode::TM_WILDCARD_MODE, [("cider", 5)],);

    assert_entries!(b"cider", tm_iter_mode::TM_WILDCARD_MODE, [("cider", 5)],);
}

#[test]
#[cfg_attr(miri, ignore = "Miri does not support the system monotonic clock")]
fn test_trie_iter_timeout() {
    // Prefix mode with an empty pattern matches every entry in the map.
    run_iter_timeout_test(b"", tm_iter_mode::TM_PREFIX_MODE);
}

#[test]
#[cfg_attr(miri, ignore = "Miri does not support the system monotonic clock")]
fn test_trie_wildcard_iter_timeout() {
    // `*` matches every entry in the map, exercising the wildcard iterator's
    // own timeout enforcement (a distinct code path from prefix mode).
    run_iter_timeout_test(b"*", tm_iter_mode::TM_WILDCARD_MODE);
}

/// Number of entries inserted into the map used by the timeout tests.
const TIMEOUT_MAP_ENTRIES: usize = 1_000;

fn run_iter_timeout_test(pattern: &[u8], mode: tm_iter_mode) {
    let t = NewTrieMap();

    let mut value: u8 = 0;
    let value_ptr = &mut value as *mut u8 as *mut c_void;

    for i in 0..TIMEOUT_MAP_ENTRIES {
        let key = format!("{i:08}");
        // Safety: We adhere to all the safety requirements of `TrieMap_Add`
        unsafe {
            TrieMap_Add(
                t,
                key.as_ptr().cast(),
                key.len() as tm_len_t,
                value_ptr,
                None,
            );
        }
    }

    // Safety: We adhere to all the safety requirements of `TrieMap_Iterate`
    let it = unsafe {
        TrieMap_IterateWithFilter(t, pattern.as_ptr().cast(), pattern.len() as tm_len_t, mode)
    };

    let mut char: *mut c_char = std::ptr::null_mut();
    let mut len: tm_len_t = 0;
    let mut value: *mut c_void = std::ptr::null_mut();

    let mut deadline = timespec_monotonic_now();
    let duration_ns = 200_000_000; // 200 ms are 200_000_000 nanoseconds
    deadline.tv_nsec += duration_ns;

    // handle overflow, a second consists of 1_000_000_000 nanoseconds
    deadline.tv_sec += deadline.tv_nsec / 1_000_000_000;
    deadline.tv_nsec %= 1_000_000_000;

    // Safety: We adhere to all the safety requirements of `TrieMapIterator_SetTimeout`
    unsafe { TrieMapIterator_SetTimeout(it, deadline) };

    for _ in 0..2 {
        assert_eq!(
            1,
            // Safety: We adhere to all the safety requirements of `TrieMapIterator_Next`
            unsafe {
                TrieMapIterator_Next(
                    it,
                    &mut char as *mut *mut c_char,
                    &mut len as *mut tm_len_t,
                    &mut value as *mut *mut c_void,
                )
            },
            "Before the deadline passes, next should yield a result"
        );
    }

    // Wait until the deadline has passed.
    // We're using a monotonic timer, so this should not be flaky
    while {
        let now = timespec_monotonic_now();
        now.tv_sec < deadline.tv_sec
            || (now.tv_sec == deadline.tv_sec && now.tv_nsec <= deadline.tv_nsec)
    } {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    let found = unsafe {
        TrieMapIterator_Next(
            it,
            &mut char as *mut *mut c_char,
            &mut len as *mut tm_len_t,
            &mut value as *mut *mut c_void,
        )
    };
    // Expired
    assert_eq!(found, 1);

    // Safety: We adhere to all the safety requirements of `TrieMapIterator_Free`
    unsafe { TrieMapIterator_Free(it) };

    // Safety: We adhere to all the safety requirements of `TrieMap_Free`
    unsafe { TrieMap_Free(t, Some(do_not_free)) };
}
