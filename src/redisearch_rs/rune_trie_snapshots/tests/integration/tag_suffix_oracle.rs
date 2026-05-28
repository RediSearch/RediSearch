/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C-oracle integration tests for the tag-variant suffix-index port at
//! [`trie_rs::str::tag_suffix::TagSuffixIndex`].
//!
//! The C tag variant builds on a `TrieMap *` (byte-keyed trie) rather
//! than the rune trie used by the text variant. The `NewTrieMap` and
//! `TrieMap_Free` symbols come from the `triemap_ffi` crate's
//! `pub extern "C"` exports — the C TrieMap has already been ported to
//! Rust. We declare them as bare `extern "C"` here rather than pulling
//! `triemap_ffi` in as a dev-dep so the pointer type stays
//! `ffi::TrieMap` (bindgen's opaque struct), matching what the C
//! `addSuffixTrieMap` family expects.
//!
//! ## Output shape: `arrayof(char *)` vs `arrayof(char **)`
//!
//! - `GetList_SuffixTrieMap` returns `arrayof(char **)` — an array of
//!   back-ref arrays (one per matching node). The "result set" is the
//!   union of every inner array's entries.
//! - `GetList_SuffixTrieMap_Wildcard` returns `arrayof(char *)` — a
//!   pre-flattened list of matching source terms.
//!
//! ## Why ASCII inputs
//!
//! Both the C tag variant and the Rust port slice on byte boundaries.
//! Restricting to ASCII keeps the test scenarios easy to reason about.
//! Non-ASCII byte-rotation parity is exercised by the unit test
//! `non_ascii_byte_rotation_lookups_consistent` in
//! `trie_rs::str::tag_suffix`.

use std::collections::HashSet;
use std::ffi::c_void;
use std::slice;

use ffi::{
    GetList_SuffixTrieMap, GetList_SuffixTrieMap_Wildcard, TrieMap, addSuffixTrieMap,
    deleteSuffixTrieMap, suffixTrieMap_freeCallback, timespec,
};
use libc::c_char;
use trie_rs::str::tag_suffix::TagSuffixIndex;

// `NewTrieMap` / `TrieMap_Free` are exported (as `extern "C"`) by the
// `triemap_ffi` Rust crate — the C `TrieMap` was ported and these
// symbols live in `libredisearch_all.a`. Declared here against
// `ffi::TrieMap` (bindgen's opaque-struct stub) because that's the
// type the C suffix-index entry points expect.
unsafe extern "C" {
    fn NewTrieMap() -> *mut TrieMap;
    fn TrieMap_Free(t: *mut TrieMap, func: Option<unsafe extern "C" fn(*mut c_void)>);
    /// Length of an `array.h`-style array, defined in
    /// `src/util/arr/arr.c`. Already exposed by `ffi` as
    /// `array_len_func`, but re-declared here so the test file is
    /// self-contained and doesn't need additional allowlist surface.
    fn array_len_func(arr: *const c_void) -> u32;
    fn array_free(arr: *mut c_void);
}

unsafe fn build_c_index(corpus: &[&str]) -> *mut TrieMap {
    // SAFETY: `NewTrieMap` returns a fresh owned pointer.
    let trie = unsafe { NewTrieMap() };
    for term in corpus {
        // SAFETY: `term.as_ptr()` valid for the call; `addSuffixTrieMap`
        // copies the input internally.
        unsafe {
            addSuffixTrieMap(trie, term.as_ptr() as *const c_char, term.len() as u32);
        }
    }
    trie
}

unsafe fn free_c_index(trie: *mut TrieMap) {
    // SAFETY: `trie` was returned by `NewTrieMap`; the suffix free
    // callback is the canonical one for the payloads we installed.
    unsafe { TrieMap_Free(trie, Some(suffixTrieMap_freeCallback)) };
}

unsafe fn c_iter_suffix(trie: *mut TrieMap, needle: &str) -> HashSet<String> {
    // SAFETY: `needle.as_ptr()` is valid; the C path doesn't retain
    // the pointer past the call.
    let arr_ptr = unsafe {
        GetList_SuffixTrieMap(
            trie,
            needle.as_ptr() as *const c_char,
            needle.len() as u32,
            false,
            timespec {
                tv_sec: 0,
                tv_nsec: 0,
            },
            true,
        )
    };
    let mut result = HashSet::new();
    if arr_ptr.is_null() {
        return result;
    }
    // SAFETY: `arr_ptr` is an `arrayof(char **)` — its length is the
    // number of inner arrays.
    let outer_len = unsafe { array_len_func(arr_ptr as *const c_void) };
    let outer = unsafe { slice::from_raw_parts(arr_ptr, outer_len as usize) };
    for inner_ptr in outer {
        if inner_ptr.is_null() {
            continue;
        }
        // SAFETY: each `*inner_ptr` is an `arrayof(char *)` of known length.
        let inner_len = unsafe { array_len_func(*inner_ptr as *const c_void) };
        let inner = unsafe { slice::from_raw_parts(*inner_ptr, inner_len as usize) };
        for &term_ptr in inner {
            if term_ptr.is_null() {
                continue;
            }
            // SAFETY: back-ref pointers are NUL-terminated C strings.
            let cstr = unsafe { std::ffi::CStr::from_ptr(term_ptr) };
            result.insert(cstr.to_string_lossy().into_owned());
        }
    }
    // SAFETY: the outer array is owned by us per the `GetList_*` contract.
    unsafe { array_free(arr_ptr as *mut c_void) };
    result
}

unsafe fn c_iter_contains(trie: *mut TrieMap, needle: &str) -> HashSet<String> {
    let arr_ptr = unsafe {
        GetList_SuffixTrieMap(
            trie,
            needle.as_ptr() as *const c_char,
            needle.len() as u32,
            true,
            timespec {
                tv_sec: 0,
                tv_nsec: 0,
            },
            true,
        )
    };
    let mut result = HashSet::new();
    if arr_ptr.is_null() {
        return result;
    }
    let outer_len = unsafe { array_len_func(arr_ptr as *const c_void) };
    let outer = unsafe { slice::from_raw_parts(arr_ptr, outer_len as usize) };
    for inner_ptr in outer {
        if inner_ptr.is_null() {
            continue;
        }
        let inner_len = unsafe { array_len_func(*inner_ptr as *const c_void) };
        let inner = unsafe { slice::from_raw_parts(*inner_ptr, inner_len as usize) };
        for &term_ptr in inner {
            if term_ptr.is_null() {
                continue;
            }
            let cstr = unsafe { std::ffi::CStr::from_ptr(term_ptr) };
            result.insert(cstr.to_string_lossy().into_owned());
        }
    }
    unsafe { array_free(arr_ptr as *mut c_void) };
    result
}

/// Returns `(found_token, results)`. The C path returns `BAD_POINTER`
/// (`0xBAAAAAAD`) when no `MIN_SUFFIX`-length literal token is present
/// in the pattern — that's the sentinel telling the caller to fall back
/// to brute-force iteration over the full term trie.
unsafe fn c_iter_wildcard(trie: *mut TrieMap, pattern: &str) -> (bool, HashSet<String>) {
    let arr_ptr = unsafe {
        GetList_SuffixTrieMap_Wildcard(
            trie,
            pattern.as_ptr() as *const c_char,
            pattern.len() as u32,
            timespec {
                tv_sec: 0,
                tv_nsec: 0,
            },
            i64::MAX,
            true,
        )
    };
    // `BAD_POINTER` is `(void *)0xBAAAAAAD` in `src/redisearch.h`.
    if arr_ptr as usize == 0xBAAAAAAD {
        return (false, HashSet::new());
    }
    let mut result = HashSet::new();
    if arr_ptr.is_null() {
        return (true, result);
    }
    let len = unsafe { array_len_func(arr_ptr as *const c_void) };
    let slice = unsafe { slice::from_raw_parts(arr_ptr, len as usize) };
    for &term_ptr in slice {
        if term_ptr.is_null() {
            continue;
        }
        let cstr = unsafe { std::ffi::CStr::from_ptr(term_ptr) };
        result.insert(cstr.to_string_lossy().into_owned());
    }
    unsafe { array_free(arr_ptr as *mut c_void) };
    (true, result)
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

fn rust_iter_suffix(corpus: &[&str], needle: &str) -> HashSet<String> {
    let mut idx = TagSuffixIndex::new();
    for t in corpus {
        idx.add(t);
    }
    idx.iter_suffix(needle)
        .map(|r| r.as_ref().to_string())
        .collect()
}

fn rust_iter_contains(corpus: &[&str], needle: &str) -> HashSet<String> {
    let mut idx = TagSuffixIndex::new();
    for t in corpus {
        idx.add(t);
    }
    idx.iter_contains(needle)
        .map(|r| r.as_ref().to_string())
        .collect()
}

fn rust_iter_wildcard(corpus: &[&str], pattern: &str) -> Option<HashSet<String>> {
    let mut idx = TagSuffixIndex::new();
    for t in corpus {
        idx.add(t);
    }
    idx.iter_wildcard(pattern)
        .map(|it| it.map(|r| r.as_ref().to_string()).collect())
}

#[test]
fn tag_oracle_iter_suffix_basic() {
    let corpus = &["cat", "concat", "scat", "catalog", "category"];
    let trie = unsafe { build_c_index(corpus) };
    for needle in &["cat", "at", "alog", "concat", "missing"] {
        let want = unsafe { c_iter_suffix(trie, needle) };
        let got = rust_iter_suffix(corpus, needle);
        assert_eq!(got, want, "iter_suffix({needle})");
    }
    unsafe { free_c_index(trie) };
}

#[test]
fn tag_oracle_iter_contains_basic() {
    let corpus = &["cat", "concat", "scat", "catalog", "category", "dog"];
    let trie = unsafe { build_c_index(corpus) };
    for needle in &["cat", "at", "og", "ate", "missing"] {
        let want = unsafe { c_iter_contains(trie, needle) };
        let got = rust_iter_contains(corpus, needle);
        assert_eq!(got, want, "iter_contains({needle})");
    }
    unsafe { free_c_index(trie) };
}

#[test]
fn tag_oracle_iter_wildcard_basic() {
    let corpus = &["apple", "happy", "puppet", "banana", "appendix"];
    let trie = unsafe { build_c_index(corpus) };
    for pattern in &["*pp*", "*pen*", "ap*", "*ana*"] {
        let (c_found_token, want) = unsafe { c_iter_wildcard(trie, pattern) };
        let got = rust_iter_wildcard(corpus, pattern);
        match (c_found_token, got) {
            (true, Some(g)) => assert_eq!(g, want, "iter_wildcard({pattern})"),
            (false, None) => {}
            (c, r) => panic!(
                "fallback-signal mismatch for {pattern:?}: C={c}, Rust returned={:?}",
                r.is_some()
            ),
        }
    }
    unsafe { free_c_index(trie) };
}

#[test]
fn tag_oracle_after_delete() {
    let initial = &["cat", "concat", "scat", "catalog"];
    let to_delete = &["concat", "catalog"];

    let trie = unsafe { build_c_index(initial) };
    for t in to_delete {
        unsafe {
            deleteSuffixTrieMap(trie, t.as_ptr() as *const c_char, t.len() as u32);
        }
    }
    let want_suffix = unsafe { c_iter_suffix(trie, "at") };
    let want_contains = unsafe { c_iter_contains(trie, "cat") };
    unsafe { free_c_index(trie) };

    let mut idx = TagSuffixIndex::new();
    for t in initial {
        idx.add(t);
    }
    for t in to_delete {
        idx.remove(t);
    }
    let got_suffix: HashSet<String> = idx
        .iter_suffix("at")
        .map(|r| r.as_ref().to_string())
        .collect();
    let got_contains: HashSet<String> = idx
        .iter_contains("cat")
        .map(|r| r.as_ref().to_string())
        .collect();

    assert_eq!(got_suffix, want_suffix, "iter_suffix(at) after delete");
    assert_eq!(
        got_contains, want_contains,
        "iter_contains(cat) after delete"
    );
}
