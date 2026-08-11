/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for the removal APIs [`TermsTrie::delete`] and
//! [`SuffixTrie::delete`].
//!
//! Each test builds a live C trie through the linked static library, wraps it in
//! a [`TermsTrie`] or [`SuffixTrie`], and mutates it.

// Links the Rust-provided and C-provided symbols of the whole module.
extern crate redisearch_rs;
// Provides the Redis allocator (and stubs) the C trie code relies on.
redis_mock::mock_or_stub_missing_redis_c_symbols!();

use std::{
    collections::HashSet,
    ffi::{c_char, c_void},
    ops::ControlFlow,
    ptr,
};

use c_trie::{SuffixMode, SuffixTrie, TermsTrie};

/// Convert an ASCII/UTF-8 string to the trie's rune (`u16`) key.
fn to_runes(s: &str) -> Vec<ffi::rune> {
    // A UTF-8 string decodes to at most as many runes as bytes; the extra slot
    // gives the conversion room for a trailing rune.
    let mut buf = vec![0 as ffi::rune; s.len() + 1];
    // SAFETY: `s` is valid UTF-8 of `s.len()` bytes, so the decode stays within
    // the slice, and `buf` has room for `s.len() + 1` runes.
    let n = unsafe { ffi::strToRunesN(s.as_ptr().cast::<c_char>(), s.len(), buf.as_mut_ptr()) };
    buf.truncate(n);
    buf
}

/// Build a terms trie holding `terms`, run `f` against an exclusive handle, then
/// free it.
fn with_terms_trie(terms: &[&str], f: impl FnOnce(&mut TermsTrie)) {
    // SAFETY: `NewTrie` returns a fresh, valid, empty terms trie; a terms trie
    // stores no payload, so a null free callback is correct.
    let trie_ptr = unsafe { ffi::NewTrie(None, ffi::TrieSortMode_Trie_Sort_Lex) };
    assert!(!trie_ptr.is_null(), "NewTrie returned null");
    for term in terms {
        // SAFETY: `trie_ptr` is the live trie just created; `term` points to
        // `term.len()` valid UTF-8 bytes; a null payload is accepted.
        unsafe {
            ffi::Trie_InsertStringBuffer(
                trie_ptr,
                term.as_ptr().cast::<c_char>(),
                term.len(),
                1.0,
                0,
                ptr::null_mut(),
                term.chars().count(),
            );
        }
    }
    // SAFETY: `trie_ptr` is a valid trie that stays alive for the whole closure
    // and is not freed until after it returns. No other handle to it exists.
    let trie = unsafe { TermsTrie::from_raw_mut(trie_ptr) };
    f(trie);
    // SAFETY: `trie_ptr` was produced by `NewTrie` and is freed exactly once
    // here, after the last use of `trie`.
    unsafe { ffi::TrieType_Free(trie_ptr.cast::<c_void>()) };
}

/// Build a *suffix* trie holding `terms` (and all their suffixes), run `f`
/// against an exclusive handle, then free it. `terms` must be non-empty strings
/// — `addSuffixTrie` asserts on empty.
fn with_suffix_trie(terms: &[&str], f: impl FnOnce(&mut SuffixTrie)) {
    // SAFETY: `NewTrie` returns a fresh, valid trie; `suffixTrie_freeCallback` is
    // the matching free callback for the payloads `addSuffixTrie` inserts.
    let trie_ptr = unsafe {
        ffi::NewTrie(
            Some(ffi::suffixTrie_freeCallback),
            ffi::TrieSortMode_Trie_Sort_Lex,
        )
    };
    assert!(!trie_ptr.is_null(), "NewTrie returned null");
    for term in terms {
        assert!(!term.is_empty(), "addSuffixTrie rejects empty strings");
        // SAFETY: `trie_ptr` is the live suffix trie; `term` points to
        // `term.len()` valid, non-empty UTF-8 bytes.
        unsafe { ffi::addSuffixTrie(trie_ptr, term.as_ptr().cast::<c_char>(), term.len() as u32) };
    }
    // SAFETY: as in `with_terms_trie` — `trie_ptr` outlives the closure and no
    // other handle to it exists.
    let trie = unsafe { SuffixTrie::from_raw_mut(trie_ptr) };
    f(trie);
    // SAFETY: freed exactly once after the last use of `trie`.
    unsafe { ffi::TrieType_Free(trie_ptr.cast::<c_void>()) };
}

/// Every term currently stored in a terms trie.
fn terms_of(trie: &TermsTrie) -> HashSet<String> {
    trie.iterate_all()
        .map(|term| String::from_utf8(term).expect("term is valid UTF-8"))
        .collect()
}

/// Every term a suffix trie reports for `pattern` under `mode`.
fn suffix_matches(trie: &SuffixTrie, pattern: &str, mode: SuffixMode) -> HashSet<String> {
    let runes = to_runes(pattern);
    let mut found = HashSet::new();
    trie.iterate_contains(&runes, mode, |term| {
        found.insert(String::from_utf8_lossy(term).into_owned());
        ControlFlow::Continue(())
    });
    found
}

fn set(items: &[&str]) -> HashSet<String> {
    items.iter().map(|s| (*s).to_owned()).collect()
}

// --- `TermsTrie::delete` ----------------------------------------------------

#[test]
#[cfg_attr(
    miri,
    ignore = "calling the C trie's foreign functions is not supported by Miri"
)]
fn delete_removes_a_stored_term() {
    with_terms_trie(&["apple", "maple", "grape"], |trie| {
        assert!(trie.delete(b"maple"), "a stored term is removed");
        assert_eq!(terms_of(trie), set(&["apple", "grape"]));
        assert_eq!(trie.num_docs(b"maple"), 0);
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "calling the C trie's foreign functions is not supported by Miri"
)]
fn delete_removes_a_term_that_still_has_documents() {
    // Unlike `decrement_num_docs`, `delete` does not care about the doc count:
    // the term goes away in one step.
    with_terms_trie(&["apple"], |trie| {
        assert_eq!(trie.num_docs(b"apple"), 5, "inserted with numDocs == 5");
        assert!(trie.delete(b"apple"));
        assert_eq!(terms_of(trie), HashSet::new());
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "calling the C trie's foreign functions is not supported by Miri"
)]
fn delete_reports_a_miss_for_an_absent_term() {
    with_terms_trie(&["apple"], |trie| {
        let removed = trie.delete(b"apricot");
        assert!(!removed, "term was never inserted");
        assert_eq!(terms_of(trie), set(&["apple"]), "trie is left untouched");
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "calling the C trie's foreign functions is not supported by Miri"
)]
fn delete_reports_a_miss_for_a_prefix_of_a_stored_term() {
    with_terms_trie(&["apple"], |trie| {
        let removed = trie.delete(b"app");
        assert!(!removed, "a prefix is not an exact match");
        assert_eq!(terms_of(trie), set(&["apple"]));
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "calling the C trie's foreign functions is not supported by Miri"
)]
fn delete_reports_a_miss_for_an_empty_term() {
    // `TrieNode_Add` no-ops on a zero-length key, so the trie can never hold one.
    with_terms_trie(&["apple"], |trie| {
        assert!(!trie.delete(b""));
        assert_eq!(terms_of(trie), set(&["apple"]));
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "calling the C trie's foreign functions is not supported by Miri"
)]
fn delete_reports_a_miss_for_an_over_long_term() {
    let too_long = "a".repeat(ffi::TRIE_INITIAL_STRING_LEN as usize * size_of::<ffi::rune>() + 1);
    with_terms_trie(&["apple"], |trie| {
        let removed = trie.delete(too_long.as_bytes());
        assert!(!removed, "longer than the trie holds");
        assert_eq!(terms_of(trie), set(&["apple"]));
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "calling the C trie's foreign functions is not supported by Miri"
)]
fn delete_of_a_multibyte_term_round_trips() {
    with_terms_trie(&["héllo", "wörld"], |trie| {
        assert!(trie.delete("héllo".as_bytes()));
        assert_eq!(terms_of(trie), set(&["wörld"]));
    });
}

// --- `SuffixTrie::delete` ---------------------------------------------------

#[test]
#[cfg_attr(
    miri,
    ignore = "calling the C trie's foreign functions is not supported by Miri"
)]
fn suffix_delete_removes_the_term_from_every_suffix_it_registered() {
    with_suffix_trie(&["apple", "maple"], |trie| {
        assert_eq!(
            suffix_matches(trie, "ple", SuffixMode::Suffix),
            set(&["apple", "maple"]),
            "both terms end in `ple` to start with"
        );

        trie.delete(b"apple");

        assert_eq!(
            suffix_matches(trie, "ple", SuffixMode::Suffix),
            set(&["maple"]),
            "only the deleted term is gone"
        );
        assert_eq!(
            suffix_matches(trie, "pp", SuffixMode::Contains),
            HashSet::new(),
            "a suffix unique to the deleted term matches nothing"
        );
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "calling the C trie's foreign functions is not supported by Miri"
)]
fn suffix_delete_ignores_an_absent_term() {
    // The suffix trie is shared by every TEXT field in an index, including those
    // that never contributed to it, so a miss is expected rather than an error.
    with_suffix_trie(&["apple"], |trie| {
        trie.delete(b"grape");
        assert_eq!(
            suffix_matches(trie, "ple", SuffixMode::Suffix),
            set(&["apple"]),
            "the trie is left untouched"
        );
    });
}
