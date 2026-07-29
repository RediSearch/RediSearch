/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for the trie-iteration API of [`CTrieRef`].
//!
//! Each test builds a live C trie through the linked static library, wraps it in
//! a [`CTrieRef`], and exercises one of the iteration methods.

// Links the Rust-provided and C-provided symbols of the whole module.
extern crate redisearch_rs;
// Provides the Redis allocator (and stubs) the C trie code relies on.
redis_mock::mock_or_stub_missing_redis_c_symbols!();

use std::{
    collections::HashSet,
    ffi::{c_char, c_void},
    mem,
    ops::ControlFlow,
    ptr::{self, NonNull},
};

use c_trie::{CTrieRef, SuffixMode};
use ffi::{SuffixType, SuffixType_SUFFIX_TYPE_CONTAINS, SuffixType_SUFFIX_TYPE_SUFFIX};

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

/// Render a rune slice back to a `String`. The test corpus is ASCII, so each
/// rune is its own byte.
fn runes_to_string(runes: &[ffi::rune]) -> String {
    runes.iter().map(|&r| char::from(r as u8)).collect()
}

/// Build a terms trie holding `terms`, each keyed with `numDocs == term length`
/// (so tests can assert the document count flows through), run `f`, then free it.
fn with_terms_trie(terms: &[&str], f: impl FnOnce(&CTrieRef)) {
    // SAFETY: `NewTrie` returns a fresh, valid, empty terms trie; a terms trie
    // stores no payload, so a null free callback is correct.
    let ptr = unsafe { ffi::NewTrie(None, ffi::TrieSortMode_Trie_Sort_Lex) };
    assert!(!ptr.is_null(), "NewTrie returned null");
    for term in terms {
        // SAFETY: `ptr` is the live trie just created; `term` points to
        // `term.len()` valid UTF-8 bytes; a null payload is accepted.
        unsafe {
            ffi::Trie_InsertStringBuffer(
                ptr,
                term.as_ptr().cast::<c_char>(),
                term.len(),
                1.0,
                0,
                ptr::null_mut(),
                term.chars().count(),
            );
        }
    }
    // SAFETY: `ptr` is a valid trie that stays alive for the whole closure and
    // is not freed until after it returns.
    let trie = unsafe { CTrieRef::from_raw(ptr) };
    f(&trie);
    // SAFETY: `ptr` was produced by `NewTrie` and is freed exactly once here,
    // after the last use of `trie`.
    unsafe { ffi::TrieType_Free(ptr.cast::<c_void>()) };
}

/// Build a *suffix* trie holding `terms` (and all their suffixes), run `f`, then
/// free it. `terms` must be non-empty strings — `addSuffixTrie` asserts on empty.
fn with_suffix_trie(terms: &[&str], f: impl FnOnce(&CTrieRef)) {
    // SAFETY: `NewTrie` returns a fresh, valid trie; `suffixTrie_freeCallback` is
    // the matching free callback for the payloads `addSuffixTrie` inserts.
    let ptr = unsafe {
        ffi::NewTrie(
            Some(ffi::suffixTrie_freeCallback),
            ffi::TrieSortMode_Trie_Sort_Lex,
        )
    };
    assert!(!ptr.is_null(), "NewTrie returned null");
    for term in terms {
        assert!(!term.is_empty(), "addSuffixTrie rejects empty strings");
        // SAFETY: `ptr` is the live suffix trie; `term` points to `term.len()`
        // valid, non-empty UTF-8 bytes.
        unsafe { ffi::addSuffixTrie(ptr, term.as_ptr().cast::<c_char>(), term.len() as u32) };
    }
    // SAFETY: as in `with_terms_trie` — `ptr` outlives the closure.
    let trie = unsafe { CTrieRef::from_raw(ptr) };
    f(&trie);
    // SAFETY: freed exactly once after the last use of `trie`.
    unsafe { ffi::TrieType_Free(ptr.cast::<c_void>()) };
}

/// Corpus used across the anchoring tests. Every term contains `"ap"`; only
/// `apple`/`apricot` start with it and only `apple`/`maple` end with `"ple"`.
const CORPUS: &[&str] = &["apple", "maple", "grape", "apricot", "map"];

fn set(items: &[&str]) -> HashSet<String> {
    items.iter().map(|s| (*s).to_owned()).collect()
}

// --- `From<SuffixMode>` -----------------------------------------------------

#[test]
fn suffix_mode_maps_to_c_discriminant() {
    assert_eq!(
        SuffixType::from(SuffixMode::Suffix),
        SuffixType_SUFFIX_TYPE_SUFFIX
    );
    assert_eq!(
        SuffixType::from(SuffixMode::Contains),
        SuffixType_SUFFIX_TYPE_CONTAINS
    );
}

// --- `iterate_contains` (terms trie) ----------------------------------------

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_contains_prefix_anchor_reports_terms_and_num_docs() {
    with_terms_trie(CORPUS, |trie| {
        let runes = to_runes("ap");
        let mut got: HashSet<(String, usize)> = HashSet::new();
        // SAFETY: `trie` wraps a live terms trie that is not mutated during the
        // walk; the timeout is `None`; the closure does not touch the trie.
        unsafe {
            trie.iterate_contains(&runes, true, false, None, |term, num_docs| {
                got.insert((runes_to_string(term), num_docs));
                ControlFlow::Continue(())
            });
        }
        // Prefix `ap`: only terms starting with it, and `numDocs` == char length.
        let expected: HashSet<(String, usize)> =
            [("apple".to_owned(), 5), ("apricot".to_owned(), 7)]
                .into_iter()
                .collect();
        assert_eq!(got, expected);
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_contains_suffix_anchor() {
    with_terms_trie(CORPUS, |trie| {
        let runes = to_runes("ple");
        let mut got = HashSet::new();
        // SAFETY: live, un-mutated terms trie; `None` timeout.
        unsafe {
            trie.iterate_contains(&runes, false, true, None, |term, _| {
                got.insert(runes_to_string(term));
                ControlFlow::Continue(())
            });
        }
        assert_eq!(got, set(&["apple", "maple"]));
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_contains_contains_anchor() {
    with_terms_trie(CORPUS, |trie| {
        let runes = to_runes("ap");
        let mut got = HashSet::new();
        // SAFETY: live, un-mutated terms trie; `None` timeout.
        unsafe {
            trie.iterate_contains(&runes, true, true, None, |term, _| {
                got.insert(runes_to_string(term));
                ControlFlow::Continue(())
            });
        }
        // Every corpus term contains `ap`.
        assert_eq!(got, set(CORPUS));
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_contains_break_stops_walk_early() {
    with_terms_trie(CORPUS, |trie| {
        let runes = to_runes("ap");
        let mut count = 0_usize;
        // SAFETY: live, un-mutated terms trie; `None` timeout.
        unsafe {
            trie.iterate_contains(&runes, true, true, None, |_, _| {
                count += 1;
                // Stop after the second match even though five would match.
                if count >= 2 {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            });
        }
        assert_eq!(count, 2, "Break must stop the walk at the second match");
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_contains_empty_pattern_in_suffix_mode_visits_nothing() {
    with_terms_trie(CORPUS, |trie| {
        let mut count = 0_usize;
        // SAFETY: live, un-mutated terms trie; `None` timeout; empty pattern.
        unsafe {
            trie.iterate_contains(&[], false, true, None, |_, _| {
                count += 1;
                ControlFlow::Continue(())
            });
        }
        assert_eq!(count, 0);
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_contains_some_and_none_timeout_agree() {
    // `RS_IsMock` is true in this test binary (`RedisModule_CreateTimer` is
    // unbound), so an actual deadline never fires — deadline *enforcement* is
    // covered by the C and Python suites. This exercises both branches of the
    // wrapper's timeout mapping (`None` → skip checks; `Some` → a valid, live
    // deadline pointer) and confirms they accept input and return the same set.
    with_terms_trie(CORPUS, |trie| {
        let runes = to_runes("ap");

        let mut none_hits = HashSet::new();
        // SAFETY: live, un-mutated terms trie; `None` timeout.
        unsafe {
            trie.iterate_contains(&runes, true, true, None, |term, _| {
                none_hits.insert(runes_to_string(term));
                ControlFlow::Continue(())
            });
        }

        // SAFETY: `timespec` is a plain-old-data struct; an all-zero value is valid.
        let mut deadline: ffi::timespec = unsafe { mem::zeroed() };
        deadline.tv_sec = 1_i64 << 40; // far in the future
        let mut some_hits = HashSet::new();
        // SAFETY: live, un-mutated terms trie; `deadline` is a valid `timespec`
        // that outlives the call.
        unsafe {
            trie.iterate_contains(
                &runes,
                true,
                true,
                Some(NonNull::from(&mut deadline)),
                |term, _| {
                    some_hits.insert(runes_to_string(term));
                    ControlFlow::Continue(())
                },
            );
        }

        assert_eq!(none_hits, set(CORPUS));
        assert_eq!(some_hits, none_hits);
    });
}

// --- `iterate_suffix` (suffix trie) -----------------------------------------

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_suffix_suffix_anchor() {
    with_suffix_trie(CORPUS, |trie| {
        let runes = to_runes("ple");
        let mut got = HashSet::new();
        // SAFETY: `trie` wraps a live suffix trie (matching payload/free
        // callback) that is not mutated during the walk.
        unsafe {
            trie.iterate_suffix(&runes, SuffixMode::Suffix, |bytes| {
                got.insert(String::from_utf8_lossy(bytes).into_owned());
                ControlFlow::Continue(())
            });
        }
        assert_eq!(got, set(&["apple", "maple"]));
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_suffix_contains_anchor() {
    with_suffix_trie(CORPUS, |trie| {
        let runes = to_runes("ap");
        let mut got = HashSet::new();
        // SAFETY: live, un-mutated suffix trie.
        unsafe {
            trie.iterate_suffix(&runes, SuffixMode::Contains, |bytes| {
                got.insert(String::from_utf8_lossy(bytes).into_owned());
                ControlFlow::Continue(())
            });
        }
        assert_eq!(got, set(CORPUS));
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_suffix_break_stops_walk_early() {
    with_suffix_trie(CORPUS, |trie| {
        let runes = to_runes("ap");
        let mut count = 0_usize;
        // SAFETY: live, un-mutated suffix trie.
        unsafe {
            trie.iterate_suffix(&runes, SuffixMode::Contains, |_| {
                count += 1;
                ControlFlow::Break(())
            });
        }
        assert_eq!(count, 1, "Break must stop after the first match");
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_suffix_empty_pattern_visits_nothing() {
    with_suffix_trie(CORPUS, |trie| {
        let mut count = 0_usize;
        // SAFETY: live, un-mutated suffix trie; empty pattern.
        unsafe {
            trie.iterate_suffix(&[], SuffixMode::Contains, |_| {
                count += 1;
                ControlFlow::Continue(())
            });
        }
        assert_eq!(count, 0);
    });
}
