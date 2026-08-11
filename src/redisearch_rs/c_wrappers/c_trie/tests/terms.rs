/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for [`TermsTrie`], the primary term index.
//!
//! Each test builds a live C trie through the linked static library, wraps it in
//! a [`TermsTrie`], and exercises one of its methods.

// Links the Rust-provided and C-provided symbols of the whole module.
extern crate redisearch_rs;
// Provides the Redis allocator (and stubs) the C trie code relies on.
redis_mock::mock_or_stub_missing_redis_c_symbols!();

use std::{
    collections::HashSet,
    ffi::{c_char, c_void},
    mem,
    ops::ControlFlow,
    ptr,
};

use c_trie::{LoweredPattern, TermsTrie};

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

/// Render a rune slice as text, for corpora that are not pure ASCII. Runes are
/// UTF-16 code units and every corpus term stays inside the BMP, so each rune is
/// its own `char`.
fn runes_to_text(runes: &[ffi::rune]) -> String {
    runes
        .iter()
        .map(|&r| char::from_u32(u32::from(r)).expect("corpus runes are BMP scalars"))
        .collect()
}

/// Build a wildcard pattern from an ASCII/UTF-8 string. Every corpus here is
/// lowercase already, so no case folding is needed on the way in.
fn pattern(s: &str) -> LoweredPattern {
    LoweredPattern::new(&to_runes(s)).expect("pattern is short enough to convert")
}

/// Build a terms trie holding `terms`, each keyed with `numDocs == term length`
/// (so tests can assert the document count flows through), run `f` against an
/// exclusive handle, then free it.
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

/// Every term currently stored in a terms trie.
fn terms_of(trie: &TermsTrie) -> HashSet<String> {
    trie.iterate_all()
        .map(|term| String::from_utf8(term).expect("term is valid UTF-8"))
        .collect()
}

/// Corpus used across the anchoring tests. Every term contains `"ap"`; only
/// `apple`/`apricot` start with it and only `apple`/`maple` end with `"ple"`.
const CORPUS: &[&str] = &["apple", "maple", "grape", "apricot", "map"];

fn set(items: &[&str]) -> HashSet<String> {
    items.iter().map(|s| (*s).to_owned()).collect()
}

// --- `iterate_contains` -----------------------------------------------------

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_contains_prefix_anchor_reports_terms_and_num_docs() {
    with_terms_trie(CORPUS, |trie| {
        let runes = to_runes("ap");
        let mut got: HashSet<(String, usize)> = HashSet::new();
        trie.iterate_contains(&runes, true, false, None, |term, num_docs| {
            got.insert((runes_to_string(term), num_docs));
            ControlFlow::Continue(())
        });
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
        trie.iterate_contains(&runes, false, true, None, |term, _| {
            got.insert(runes_to_string(term));
            ControlFlow::Continue(())
        });
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
        trie.iterate_contains(&runes, true, true, None, |term, _| {
            got.insert(runes_to_string(term));
            ControlFlow::Continue(())
        });
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
        trie.iterate_contains(&runes, true, true, None, |_, _| {
            count += 1;
            // Stop after the second match even though five would match.
            if count >= 2 {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        });
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
        trie.iterate_contains(&[], false, true, None, |_, _| {
            count += 1;
            ControlFlow::Continue(())
        });
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
    // wrapper's timeout mapping (`None` → skip checks; `Some` → a deadline the
    // walk copies in) and confirms they accept input and return the same set.
    with_terms_trie(CORPUS, |trie| {
        let runes = to_runes("ap");

        let mut none_hits = HashSet::new();
        trie.iterate_contains(&runes, true, true, None, |term, _| {
            none_hits.insert(runes_to_string(term));
            ControlFlow::Continue(())
        });

        // SAFETY: `timespec` is a plain-old-data struct; an all-zero value is valid.
        let mut deadline: ffi::timespec = unsafe { mem::zeroed() };
        deadline.tv_sec = 1_i64 << 40; // far in the future
        let mut some_hits = HashSet::new();
        trie.iterate_contains(&runes, true, true, Some(deadline), |term, _| {
            some_hits.insert(runes_to_string(term));
            ControlFlow::Continue(())
        });

        assert_eq!(none_hits, set(CORPUS));
        assert_eq!(some_hits, none_hits);
    });
}

// --- `iterate_all` ----------------------------------------------------------

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_all_visits_every_term() {
    with_terms_trie(CORPUS, |trie| {
        let got: HashSet<String> = trie
            .iterate_all()
            .map(|term| String::from_utf8(term).expect("terms are ASCII"))
            .collect();
        assert_eq!(got, set(CORPUS));
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_all_empty_trie_visits_nothing() {
    with_terms_trie(&[], |trie| {
        assert_eq!(trie.iterate_all().count(), 0);
    });
}

/// `for term in &trie` reaches the same walk as `iterate_all`.
#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn into_iter_visits_every_term() {
    with_terms_trie(CORPUS, |trie| {
        let mut got = HashSet::new();
        for term in &*trie {
            got.insert(String::from_utf8(term).expect("terms are ASCII"));
        }
        assert_eq!(got, set(CORPUS));
    });
}

// --- `iterate_wildcard` -----------------------------------------------------

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_wildcard_reports_terms_and_num_docs() {
    with_terms_trie(CORPUS, |trie| {
        let mut got: HashSet<(String, usize)> = HashSet::new();
        trie.iterate_wildcard(&pattern("ap*"), None, |term, num_docs| {
            got.insert((runes_to_string(term), num_docs));
            ControlFlow::Continue(())
        });
        // `numDocs` == char length, as inserted by `with_terms_trie`.
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
fn iterate_wildcard_matches_a_star_that_is_not_front_anchored() {
    with_terms_trie(CORPUS, |trie| {
        let mut got = HashSet::new();
        trie.iterate_wildcard(&pattern("*ple"), None, |term, _| {
            got.insert(runes_to_string(term));
            ControlFlow::Continue(())
        });
        assert_eq!(got, set(&["apple", "maple"]));
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_wildcard_question_mark_matches_exactly_one_rune() {
    with_terms_trie(CORPUS, |trie| {
        let mut got = HashSet::new();
        trie.iterate_wildcard(&pattern("m?p"), None, |term, _| {
            got.insert(runes_to_string(term));
            ControlFlow::Continue(())
        });
        // `map` only: `maple` is longer, and `?` does not span two runes.
        assert_eq!(got, set(&["map"]));
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_wildcard_matches_a_multibyte_pattern() {
    // A pattern whose byte and rune lengths differ, which every ASCII pattern
    // hides. `?` here is one *rune*, so it spans the two-byte `é`.
    with_terms_trie(&["héllo", "hallo", "hxyllo"], |trie| {
        let mut got = HashSet::new();
        trie.iterate_wildcard(&pattern("h?llo"), None, |term, _| {
            got.insert(runes_to_text(term));
            ControlFlow::Continue(())
        });
        assert_eq!(got, set(&["héllo", "hallo"]));
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_wildcard_empty_pattern_visits_nothing() {
    with_terms_trie(CORPUS, |trie| {
        let empty = LoweredPattern::new(&[]).expect("an empty pattern converts");
        let mut count = 0_usize;
        trie.iterate_wildcard(&empty, None, |_, _| {
            count += 1;
            ControlFlow::Continue(())
        });
        // The walk underneath would read the byte before the pattern to decide
        // whether it ends in `*`, so this must not reach it.
        assert_eq!(count, 0);
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_wildcard_break_stops_a_trailing_star_walk_early() {
    with_terms_trie(CORPUS, |trie| {
        let mut count = 0_usize;
        trie.iterate_wildcard(&pattern("ap*"), None, |_, _| {
            count += 1;
            ControlFlow::Break(())
        });
        assert_eq!(count, 1, "Break must stop after the first match");
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_wildcard_break_is_ignored_without_a_trailing_star() {
    // The documented caveat: the walk only honours `Break` on the sub-tree path
    // it takes for a pattern ending in `*`. Otherwise it keeps visiting terms and
    // the caller must make every further callback a no-op itself.
    with_terms_trie(CORPUS, |trie| {
        let mut count = 0_usize;
        trie.iterate_wildcard(&pattern("*ple"), None, |_, _| {
            count += 1;
            ControlFlow::Break(())
        });
        assert_eq!(count, 2, "both matches are still delivered");
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_wildcard_some_and_none_timeout_agree() {
    // As in `iterate_contains_some_and_none_timeout_agree`: a deadline never
    // fires in this binary, so this covers the wrapper's timeout mapping rather
    // than deadline enforcement.
    with_terms_trie(CORPUS, |trie| {
        let mut none_hits = HashSet::new();
        trie.iterate_wildcard(&pattern("ap*"), None, |term, _| {
            none_hits.insert(runes_to_string(term));
            ControlFlow::Continue(())
        });

        // SAFETY: `timespec` is a plain-old-data struct; an all-zero value is valid.
        let mut deadline: ffi::timespec = unsafe { mem::zeroed() };
        deadline.tv_sec = 1_i64 << 40; // far in the future
        let mut some_hits = HashSet::new();
        trie.iterate_wildcard(&pattern("ap*"), Some(deadline), |term, _| {
            some_hits.insert(runes_to_string(term));
            ControlFlow::Continue(())
        });

        assert_eq!(none_hits, set(&["apple", "apricot"]));
        assert_eq!(some_hits, none_hits);
    });
}

// --- `delete` ---------------------------------------------------------------

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
