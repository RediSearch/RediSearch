/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for [`SuffixTrie`], the suffix index.
//!
//! Each test builds a live C trie through the linked static library, wraps it in
//! a [`SuffixTrie`], and exercises one of its methods.

// Links the Rust-provided and C-provided symbols of the whole module.
extern crate redisearch_rs;
// Provides the Redis allocator (and stubs) the C trie code relies on.
redis_mock::mock_or_stub_missing_redis_c_symbols!();

use std::{
    collections::HashSet,
    ffi::{c_char, c_void},
    ops::ControlFlow,
};

use c_trie::{LoweredPattern, SuffixMode, SuffixTrie, SuffixWalk};
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

/// Build a wildcard pattern from an ASCII/UTF-8 string. Every corpus here is
/// lowercase already, so no case folding is needed on the way in.
fn pattern(s: &str) -> LoweredPattern {
    LoweredPattern::new(&to_runes(s)).expect("pattern is short enough to convert")
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
    // SAFETY: `trie_ptr` is a valid trie that stays alive for the whole closure
    // and is not freed until after it returns. No other handle to it exists.
    let trie = unsafe { SuffixTrie::from_raw_mut(trie_ptr) };
    f(trie);
    // SAFETY: freed exactly once after the last use of `trie`.
    unsafe { ffi::TrieType_Free(trie_ptr.cast::<c_void>()) };
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

// --- `iterate_contains` -----------------------------------------------------

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_suffix_suffix_anchor() {
    with_suffix_trie(CORPUS, |trie| {
        let runes = to_runes("ple");
        let mut got = HashSet::new();
        trie.iterate_contains(&runes, SuffixMode::Suffix, |bytes| {
            got.insert(String::from_utf8_lossy(bytes).into_owned());
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
fn iterate_suffix_contains_anchor() {
    with_suffix_trie(CORPUS, |trie| {
        let runes = to_runes("ap");
        let mut got = HashSet::new();
        trie.iterate_contains(&runes, SuffixMode::Contains, |bytes| {
            got.insert(String::from_utf8_lossy(bytes).into_owned());
            ControlFlow::Continue(())
        });
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
        trie.iterate_contains(&runes, SuffixMode::Contains, |_| {
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
fn iterate_suffix_empty_pattern_visits_nothing() {
    with_suffix_trie(CORPUS, |trie| {
        let mut count = 0_usize;
        trie.iterate_contains(&[], SuffixMode::Contains, |_| {
            count += 1;
            ControlFlow::Continue(())
        });
        assert_eq!(count, 0);
    });
}

// --- `iterate_wildcard` -----------------------------------------------------

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn suffix_iterate_wildcard_anchors_on_the_pattern_literal() {
    with_suffix_trie(CORPUS, |trie| {
        let mut got = HashSet::new();
        let outcome = trie.iterate_wildcard(pattern("*ple"), None, |bytes| {
            got.insert(String::from_utf8_lossy(bytes).into_owned());
            ControlFlow::Continue(())
        });
        assert!(
            matches!(outcome, SuffixWalk::Walked),
            "`ple` is a literal the suffix trie can use"
        );
        assert_eq!(got, set(&["apple", "maple"]));
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn suffix_iterate_wildcard_anchor_token_may_absorb_a_trailing_star() {
    // The chosen anchor token `ma` is followed by `*`, so the walk extends the
    // token to cover it and scans the whole sub-tree under `ma` — the only walk
    // shape that honours an early stop.
    with_suffix_trie(CORPUS, |trie| {
        let mut got = HashSet::new();
        let outcome = trie.iterate_wildcard(pattern("ma*"), None, |bytes| {
            got.insert(String::from_utf8_lossy(bytes).into_owned());
            ControlFlow::Continue(())
        });
        assert!(matches!(outcome, SuffixWalk::Walked));
        assert_eq!(got, set(&["maple", "map"]));
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn suffix_iterate_wildcard_matches_a_multibyte_pattern() {
    // A multibyte pattern separates rune count from byte count; the walk operates
    // rune-wise throughout, and every ASCII pattern leaves the two equal and so
    // could not tell.
    with_suffix_trie(&["héllo", "hallo"], |trie| {
        let mut got = HashSet::new();
        let outcome = trie.iterate_wildcard(pattern("*éllo"), None, |bytes| {
            got.insert(String::from_utf8_lossy(bytes).into_owned());
            ControlFlow::Continue(())
        });
        assert!(matches!(outcome, SuffixWalk::Walked));
        assert_eq!(got, set(&["héllo"]));
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn suffix_iterate_wildcard_declines_a_pattern_with_no_literal_to_anchor_on() {
    with_suffix_trie(CORPUS, |trie| {
        let mut count = 0_usize;
        let outcome = trie.iterate_wildcard(pattern("**"), None, |_| {
            count += 1;
            ControlFlow::Continue(())
        });
        let SuffixWalk::NoAnchor(returned) = outcome else {
            panic!("a pattern of only `*` has no token to anchor on");
        };
        assert_eq!(count, 0);
        // The pattern comes back usable for the caller's fallback over the
        // primary terms trie.
        assert_eq!(returned.len(), 2);
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn suffix_iterate_wildcard_declines_an_empty_pattern() {
    with_suffix_trie(CORPUS, |trie| {
        let empty = LoweredPattern::new(&[]).expect("an empty pattern converts");
        let mut count = 0_usize;
        let outcome = trie.iterate_wildcard(empty, None, |_| {
            count += 1;
            ControlFlow::Continue(())
        });
        let SuffixWalk::NoAnchor(returned) = outcome else {
            panic!("an empty pattern has no token to anchor on");
        };
        assert_eq!(count, 0);
        assert!(returned.is_empty());
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn suffix_iterate_wildcard_break_stops_a_star_terminated_anchor_early() {
    // `ma*` extends its anchor token to include the `*`, which is what puts the
    // walk on the sub-tree path — the only path that honours a stop request.
    with_suffix_trie(CORPUS, |trie| {
        let mut count = 0_usize;
        let outcome = trie.iterate_wildcard(pattern("ma*"), None, |_| {
            count += 1;
            ControlFlow::Break(())
        });
        assert!(matches!(outcome, SuffixWalk::Walked));
        assert_eq!(count, 1, "Break must stop after the first match");
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn suffix_iterate_wildcard_break_is_ignored_for_an_anchor_without_a_trailing_star() {
    // The documented caveat, pinned. `*p?e` anchors on `p?e`, which is not
    // `*`-terminated, so the walk is not on the sub-tree path and discards the
    // stop request — it only truncates the matches under the node it is on, then
    // moves to the next matching node.
    //
    // Two suffix keys match `p?e` here: `ple` (from `apple`) and `pie`. One
    // matching key would hide this, since `Break` does end that key's own list.
    with_suffix_trie(&["apple", "pie"], |trie| {
        let mut count = 0_usize;
        let outcome = trie.iterate_wildcard(pattern("*p?e"), None, |_| {
            count += 1;
            ControlFlow::Break(())
        });
        assert!(matches!(outcome, SuffixWalk::Walked));
        assert_eq!(
            count, 2,
            "one callback per matching suffix key, despite Break"
        );
    });
}

// --- `delete` ---------------------------------------------------------------

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
