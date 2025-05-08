/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ffi::{c_char, c_int, c_void};

/// Convert a string to a slice of `c_char`, allocated on the heap, which is the expected input for [crate::RustTrieMap].
pub fn str2c_char(input: &str) -> Box<[c_char]> {
    input.as_bytes().iter().map(|&b| b as c_char).collect()
}

#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
pub static mut RedisModule_Alloc: Option<unsafe extern "C" fn(bytes: usize) -> *mut c_void> =
    Some(redis_mock::allocator::alloc_shim);

#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
pub static mut RedisModule_Realloc: Option<
    unsafe extern "C" fn(ptr: *mut c_void, bytes: usize) -> *mut c_void,
> = Some(redis_mock::allocator::realloc_shim);

#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
pub static mut RedisModule_Free: Option<unsafe extern "C" fn(ptr: *mut c_void)> =
    Some(redis_mock::allocator::free_shim);

#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
pub static mut RedisModule_Calloc: Option<
    unsafe extern "C" fn(count: usize, size: usize) -> *mut c_void,
> = Some(redis_mock::allocator::calloc_shim);

use redisearch_rs::trie::*;

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

pub fn bytes2c_char<const N: usize>(input: &[u8; N]) -> [c_char; N] {
    input.map(|b| b as c_char)
}

unsafe extern "C" fn do_not_free(_val: *mut c_void) {
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
    let entries: Vec<_> = [
        ("bike", 0u8),
        ("biker", 1),
        ("bis", 2),
        ("cool", 3),
        ("cooler", 4),
        ("cider", 5),
    ]
    .into_iter()
    .map(|(k, v)| {
        let k = str2c_char(k);
        let v = Box::into_raw(Box::new(v)) as *mut c_void;
        let k_len = k.len() as tm_len_t;
        (Box::into_raw(k) as *mut c_char, k_len, v)
    })
    .collect();
    for (str, len, value) in entries.iter().copied() {
        // Safety: We adhere to all the safety requirements of `TrieMap_Add`
        unsafe { TrieMap_Add(t, str, len, value, None) };
    }

    f(t);

    // Safety: We adhere to all the safety requirements of `TrieMap_Free`
    unsafe { TrieMap_Free(t, Some(do_not_free)) };

    for (str, len, _) in entries {
        // Safety: we're reconstructing the `Box<[c_char]>`s created earlier
        let k = unsafe { std::slice::from_raw_parts_mut(str, len as usize) };
        // Safety: we're reconstructing the `Box<[c_char]>`s created earlier
        unsafe { std::mem::transmute::<&mut [i8], std::boxed::Box<[i8]>>(k) };
    }
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
        let pattern = bytes2c_char(pattern);
        // Safety: We adhere to all the safety requirements of `TrieMap_Iterate`
        let it =
            unsafe { TrieMap_Iterate(t, pattern.as_ptr(), pattern.len() as tm_len_t, iter_mode) };

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
        let prefix = str2c_char("bistro");

        // Safety: We adhere to all the safety requirements of `TrieMap_FindPrefixes`
        let buf = unsafe { TrieMap_FindPrefixes(t, prefix.as_ptr(), prefix.len() as tm_len_t) };
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
fn test_trie_iter_timeout() {
    with_trie_map(|t| {
        let pattern = bytes2c_char(b"");
        // Safety: We adhere to all the safety requirements of `TrieMap_Iterate`
        let it = unsafe {
            TrieMap_Iterate(
                t,
                pattern.as_ptr(),
                pattern.len() as tm_len_t,
                tm_iter_mode::TM_PREFIX_MODE,
            )
        };

        let mut char: *mut c_char = std::ptr::null_mut();
        let mut len: tm_len_t = 0;
        let mut value: *mut c_void = std::ptr::null_mut();

        let mut deadline = timespec_monotonic_now();
        deadline.tv_nsec += 1000 * 200; // Timeout in 200 ms

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
            now.tv_sec <= deadline.tv_sec || now.tv_nsec <= deadline.tv_nsec
        } {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        for _ in 0..2 {
            assert_eq!(
                0,
                // Safety: We adhere to all the safety requirements of `TrieMapIterator_Next`
                unsafe {
                    TrieMapIterator_Next(
                        it,
                        &mut char as *mut *mut c_char,
                        &mut len as *mut tm_len_t,
                        &mut value as *mut *mut c_void,
                    )
                },
                "After the deadline passes, next should not yield a result"
            );
        }

        // Safety: We adhere to all the safety requirements of `TrieMapIterator_Free`
        unsafe { TrieMapIterator_Free(it) };
    });
}

#[test]
fn test_trie_iter_range() {
    type ResultsVec = Vec<(String, u8)>;

    macro_rules! assert_range {
        (None, None, $expected:expr $(,)?) => {
            assert_range!(None::<&str>, true, None::<&str>, true, $expected)
        };
        ($min:expr, $include_min:expr, None, $expected:expr $(,)?) => {
            assert_range!($min, $include_min, None::<&str>, true, $expected)
        };
        (None, $max:expr, $include_max:expr, $expected:expr $(,)?) => {
            assert_range!(None::<&str>, true, $max, $include_max, $expected)
        };
        ($min: expr, $include_min:expr, $max:expr, $include_max:expr, $expected:expr $(,)?) => {
            let results = do_iterate($min, $include_min, $max, $include_max);
            let range_start = if $include_min { "[" } else { "(" };
            let range_end = if $include_max { "]" } else { ")" };
            let min = $min.map(|m| format!("{m:?}")).unwrap_or("←".to_owned());
            let max = $max.map(|m| format!("{m:?}")).unwrap_or("→".to_owned());

            assert_eq!(
                results,
                $expected.map(|(k, v)| (k.to_owned(), v)),
                "Invalid results for range {range_start} {min} ; {max} {range_end}"
            );
        };
    }

    unsafe extern "C" fn callback(
        key: *const c_char,
        key_len: libc::size_t,
        value: *mut c_void,
        ctx: *mut c_void,
    ) {
        // Safety: the passed context was indeed a `&mut ResultsVec`
        let results = unsafe { &mut *(ctx as *mut ResultsVec) };

        // Safety: We're reconstructing the keys and the values created in `with_trie_map`
        let key = unsafe { std::slice::from_raw_parts(key, key_len) };
        let key = String::from_utf8(key.iter().copied().map(|c| c as u8).collect()).unwrap();

        // Safety: We're reconstructing the keys and the values created in `with_trie_map`
        let value = unsafe { *(value as *mut u8) };

        results.push((key, value));
    }
    fn do_iterate(
        min: Option<&str>,
        include_min: bool,
        max: Option<&str>,
        include_max: bool,
    ) -> ResultsVec {
        let mut results: ResultsVec = Vec::new();
        with_trie_map(|t| {
            let min_c_char = min.map(str2c_char);
            let max_c_char = max.map(str2c_char);

            let (min_ptr, min_len) = min_c_char
                .as_ref()
                .map(|min| (min.as_ptr(), min.len() as c_int))
                .unwrap_or((std::ptr::null(), -1));
            let (max_ptr, max_len) = max_c_char
                .as_ref()
                .map(|max| (max.as_ptr(), max.len() as c_int))
                .unwrap_or((std::ptr::null(), -1));

            // Safety: We adhere to all the safety requirements of `TrieMap_IterateRange`
            unsafe {
                TrieMap_IterateRange(
                    t,
                    min_ptr,
                    min_len,
                    include_min,
                    max_ptr,
                    max_len,
                    include_max,
                    Some(callback),
                    (&mut results) as *mut ResultsVec as *mut _,
                )
            };
        });
        results
    }

    assert_range!(
        Some("biker"),
        true,
        Some("cool"),
        true,
        [("biker", 1), ("bis", 2), ("cider", 5), ("cool", 3)],
    );

    assert_range!(
        Some("biker"),
        false,
        Some("cool"),
        true,
        [("bis", 2), ("cider", 5), ("cool", 3)],
    );

    assert_range!(
        Some("biker"),
        true,
        Some("cool"),
        false,
        [("biker", 1), ("bis", 2), ("cider", 5)],
    );

    assert_range!(
        Some("biker"),
        false,
        Some("cool"),
        false,
        [("bis", 2), ("cider", 5)],
    );

    assert_range!(
        None,
        Some("cool"),
        true,
        [
            ("bike", 0),
            ("biker", 1),
            ("bis", 2),
            ("cider", 5),
            ("cool", 3)
        ],
    );

    assert_range!(
        Some("bike"),
        true,
        None,
        [
            ("bike", 0),
            ("biker", 1),
            ("bis", 2),
            ("cider", 5),
            ("cool", 3),
            ("cooler", 4),
        ],
    );

    assert_range!(
        None,
        None,
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
