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

use c_trie::{CTrieRef, LoweredPattern, SuffixMode, SuffixWalk};
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

// --- `LoweredPattern` -------------------------------------------------------

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn lowered_pattern_length_excludes_the_sentinel() {
    let p = pattern("he?l*o");
    assert_eq!(p.len(), 6);
    assert!(!p.is_empty());
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn lowered_pattern_from_no_runes_is_empty() {
    // The sentinel is still appended, but it does not count as content.
    let p = LoweredPattern::new(&[]).expect("an empty pattern converts");
    assert_eq!(p.len(), 0);
    assert!(p.is_empty());
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn lowered_pattern_length_is_in_runes_not_bytes() {
    // A multibyte pattern separates rune count from source-byte count, which
    // every ASCII pattern leaves equal. `len()` must count runes — it is what
    // the walks take as the pattern length.
    let p = pattern("hé*");
    assert_eq!(p.len(), 3, "three runes: h, é, *");
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn lowered_pattern_declines_a_pattern_longer_than_max_rune_str_len() {
    // Term insertion declines anything over `MAX_RUNE_STR_LEN` runes, so such a
    // pattern can name no stored term and there is nothing to walk with.
    let runes = vec![ffi::rune::from(b'a'); ffi::MAX_RUNE_STR_LEN as usize + 1];
    assert!(LoweredPattern::new(&runes).is_none());
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn lowered_pattern_declines_a_pattern_holding_a_zero_rune() {
    // An interior zero collides with the sentinel layout the type guarantees —
    // a consumer scanning for the zero would see only `a`. Declined rather than
    // built.
    assert!(LoweredPattern::new(&[ffi::rune::from(b'a'), 0, ffi::rune::from(b'b')]).is_none());
}

// --- `iterate_wildcard` (terms trie) ----------------------------------------

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_wildcard_reports_terms_and_num_docs() {
    with_terms_trie(CORPUS, |trie| {
        let mut got: HashSet<(String, usize)> = HashSet::new();
        // SAFETY: `trie` wraps a live terms trie that is not mutated during the
        // walk; the timeout is `None`; the closure does not touch the trie.
        unsafe {
            trie.iterate_wildcard(&pattern("ap*"), None, |term, num_docs| {
                got.insert((runes_to_string(term), num_docs));
                ControlFlow::Continue(())
            });
        }
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
        // SAFETY: live, un-mutated terms trie; `None` timeout.
        unsafe {
            trie.iterate_wildcard(&pattern("*ple"), None, |term, _| {
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
fn iterate_wildcard_question_mark_matches_exactly_one_rune() {
    with_terms_trie(CORPUS, |trie| {
        let mut got = HashSet::new();
        // SAFETY: live, un-mutated terms trie; `None` timeout.
        unsafe {
            trie.iterate_wildcard(&pattern("m?p"), None, |term, _| {
                got.insert(runes_to_string(term));
                ControlFlow::Continue(())
            });
        }
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
        // SAFETY: live, un-mutated terms trie; `None` timeout.
        unsafe {
            trie.iterate_wildcard(&pattern("h?llo"), None, |term, _| {
                got.insert(runes_to_text(term));
                ControlFlow::Continue(())
            });
        }
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
        // SAFETY: live, un-mutated terms trie; `None` timeout; empty pattern.
        unsafe {
            trie.iterate_wildcard(&empty, None, |_, _| {
                count += 1;
                ControlFlow::Continue(())
            });
        }
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
        // SAFETY: live, un-mutated terms trie; `None` timeout.
        unsafe {
            trie.iterate_wildcard(&pattern("ap*"), None, |_, _| {
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
fn iterate_wildcard_break_is_ignored_without_a_trailing_star() {
    // The documented caveat: the walk only honours `Break` on the sub-tree path
    // it takes for a pattern ending in `*`. Otherwise it keeps visiting terms and
    // the caller must make every further callback a no-op itself.
    with_terms_trie(CORPUS, |trie| {
        let mut count = 0_usize;
        // SAFETY: live, un-mutated terms trie; `None` timeout.
        unsafe {
            trie.iterate_wildcard(&pattern("*ple"), None, |_, _| {
                count += 1;
                ControlFlow::Break(())
            });
        }
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
        // SAFETY: live, un-mutated terms trie; `None` timeout.
        unsafe {
            trie.iterate_wildcard(&pattern("ap*"), None, |term, _| {
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
            trie.iterate_wildcard(
                &pattern("ap*"),
                Some(NonNull::from(&mut deadline)),
                |term, _| {
                    some_hits.insert(runes_to_string(term));
                    ControlFlow::Continue(())
                },
            );
        }

        assert_eq!(none_hits, set(&["apple", "apricot"]));
        assert_eq!(some_hits, none_hits);
    });
}

// --- `iterate_suffix_wildcard` (suffix trie) --------------------------------

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_suffix_wildcard_anchors_on_the_pattern_literal() {
    with_suffix_trie(CORPUS, |trie| {
        let mut got = HashSet::new();
        // SAFETY: `trie` wraps a live suffix trie (matching payload/free
        // callback) that is not mutated during the walk; `None` timeout.
        let outcome = unsafe {
            trie.iterate_suffix_wildcard(pattern("*ple"), None, |bytes| {
                got.insert(String::from_utf8_lossy(bytes).into_owned());
                ControlFlow::Continue(())
            })
        };
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
fn iterate_suffix_wildcard_anchor_token_may_absorb_a_trailing_star() {
    // The chosen anchor token `ma` is followed by `*`, so the walk extends the
    // token to cover it and scans the whole sub-tree under `ma` — the only walk
    // shape that honours an early stop.
    with_suffix_trie(CORPUS, |trie| {
        let mut got = HashSet::new();
        // SAFETY: live, un-mutated suffix trie; `None` timeout.
        let outcome = unsafe {
            trie.iterate_suffix_wildcard(pattern("ma*"), None, |bytes| {
                got.insert(String::from_utf8_lossy(bytes).into_owned());
                ControlFlow::Continue(())
            })
        };
        assert!(matches!(outcome, SuffixWalk::Walked));
        assert_eq!(got, set(&["maple", "map"]));
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_suffix_wildcard_matches_a_multibyte_pattern() {
    // A multibyte pattern separates rune count from byte count; the walk operates
    // rune-wise throughout, and every ASCII pattern leaves the two equal and so
    // could not tell.
    with_suffix_trie(&["héllo", "hallo"], |trie| {
        let mut got = HashSet::new();
        // SAFETY: live, un-mutated suffix trie; `None` timeout.
        let outcome = unsafe {
            trie.iterate_suffix_wildcard(pattern("*éllo"), None, |bytes| {
                got.insert(String::from_utf8_lossy(bytes).into_owned());
                ControlFlow::Continue(())
            })
        };
        assert!(matches!(outcome, SuffixWalk::Walked));
        assert_eq!(got, set(&["héllo"]));
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_suffix_wildcard_declines_a_pattern_with_no_literal_to_anchor_on() {
    with_suffix_trie(CORPUS, |trie| {
        let mut count = 0_usize;
        // SAFETY: live, un-mutated suffix trie; `None` timeout.
        let outcome = unsafe {
            trie.iterate_suffix_wildcard(pattern("**"), None, |_| {
                count += 1;
                ControlFlow::Continue(())
            })
        };
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
fn iterate_suffix_wildcard_declines_an_empty_pattern() {
    with_suffix_trie(CORPUS, |trie| {
        let empty = LoweredPattern::new(&[]).expect("an empty pattern converts");
        let mut count = 0_usize;
        // SAFETY: live, un-mutated suffix trie; `None` timeout; empty pattern.
        let outcome = unsafe {
            trie.iterate_suffix_wildcard(empty, None, |_| {
                count += 1;
                ControlFlow::Continue(())
            })
        };
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
fn iterate_suffix_wildcard_break_stops_a_star_terminated_anchor_early() {
    // `ma*` extends its anchor token to include the `*`, which is what puts the
    // walk on the sub-tree path — the only path that honours a stop request.
    with_suffix_trie(CORPUS, |trie| {
        let mut count = 0_usize;
        // SAFETY: live, un-mutated suffix trie; `None` timeout.
        let outcome = unsafe {
            trie.iterate_suffix_wildcard(pattern("ma*"), None, |_| {
                count += 1;
                ControlFlow::Break(())
            })
        };
        assert!(matches!(outcome, SuffixWalk::Walked));
        assert_eq!(count, 1, "Break must stop after the first match");
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_suffix_wildcard_break_is_ignored_for_an_anchor_without_a_trailing_star() {
    // The documented caveat, pinned. `*p?e` anchors on `p?e`, which is not
    // `*`-terminated, so the walk is not on the sub-tree path and discards the
    // stop request — it only truncates the matches under the node it is on, then
    // moves to the next matching node.
    //
    // Two suffix keys match `p?e` here: `ple` (from `apple`) and `pie`. One
    // matching key would hide this, since `Break` does end that key's own list.
    with_suffix_trie(&["apple", "pie"], |trie| {
        let mut count = 0_usize;
        // SAFETY: live, un-mutated suffix trie; `None` timeout.
        let outcome = unsafe {
            trie.iterate_suffix_wildcard(pattern("*p?e"), None, |_| {
                count += 1;
                ControlFlow::Break(())
            })
        };
        assert!(matches!(outcome, SuffixWalk::Walked));
        assert_eq!(
            count, 2,
            "one callback per matching suffix key, despite Break"
        );
    });
}
