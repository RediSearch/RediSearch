/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Integration tests for the trie-iteration APIs of [`TermsTrie`] and
//! [`SuffixTrie`].
//!
//! Each test builds a live C trie through the linked static library, wraps it in
//! a [`TermsTrie`] or [`SuffixTrie`], and exercises one of the iteration methods.

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

use c_trie::{
    FuzzyDistance, FuzzyWalk, LoweredPattern, SuffixMode, SuffixTrie, SuffixWalk, TermsTrie,
};
use ffi::{SuffixType, SuffixType_SUFFIX_TYPE_CONTAINS, SuffixType_SUFFIX_TYPE_SUFFIX};

/// Convert an ASCII/UTF-8 string to the trie's rune (`u16`) key.
fn to_runes(s: &str) -> Vec<ffi::rune> {
    // A UTF-8 string decodes to at most as many runes as bytes, so the decode
    // cannot truncate and `n` is a valid truncation point.
    let mut buf = vec![0 as ffi::rune; s.len()];
    // SAFETY: `s` is valid UTF-8 of `s.len()` bytes, so the decode stays within
    // the slice, and `buf.len()` bounds the write.
    let n = unsafe {
        ffi::strToRunes(
            s.as_ptr().cast::<c_char>(),
            s.len(),
            buf.as_mut_ptr(),
            buf.len(),
        )
    };
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
fn with_terms_trie(terms: &[&str], f: impl FnOnce(&TermsTrie)) {
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
    let trie = unsafe { TermsTrie::from_raw(trie_ptr) };
    f(trie);
    // SAFETY: `trie_ptr` was produced by `NewTrie` and is freed exactly once
    // here, after the last use of `trie`.
    unsafe { ffi::TrieType_Free(trie_ptr.cast::<c_void>()) };
}

/// Build a *suffix* trie holding `terms` (and all their suffixes), run `f`, then
/// free it. `terms` must be non-empty strings — `addSuffixTrie` asserts on empty.
fn with_suffix_trie(terms: &[&str], f: impl FnOnce(&SuffixTrie)) {
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
    let trie = unsafe { SuffixTrie::from_raw(trie_ptr) };
    f(trie);
    // SAFETY: freed exactly once after the last use of `trie`.
    unsafe { ffi::TrieType_Free(trie_ptr.cast::<c_void>()) };
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

/// Build a wildcard pattern from an ASCII/UTF-8 string. Every pattern here is
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

// --- `FuzzyDistance` --------------------------------------------------------

#[test]
fn fuzzy_distance_admits_exactly_the_representable_range() {
    // Both ends reach C as the size of a stack VLA, so what this type refuses is
    // what the automaton could not be built for: below zero it is a
    // negative-length allocation, above the cap it exhausts the stack.
    for d in [-1, i32::MIN, FuzzyDistance::MAX + 1, i32::MAX] {
        assert!(FuzzyDistance::new(d).is_err(), "{d} is out of range");
        assert!(FuzzyDistance::try_from(d).is_err(), "{d} is out of range");
    }

    for d in 0..=FuzzyDistance::MAX {
        assert_eq!(
            FuzzyDistance::new(d).map(FuzzyDistance::get),
            Ok(d),
            "an in-range distance round-trips unchanged"
        );
        assert_eq!(FuzzyDistance::try_from(d), FuzzyDistance::new(d));
    }

    // The rejected value is carried, so a caller can name it when reporting.
    assert_eq!(
        FuzzyDistance::new(9).unwrap_err().to_string(),
        format!(
            "fuzzy distance must be in 0..={}, got 9",
            FuzzyDistance::MAX
        )
    );
}

// --- `iterate_fuzzy` (terms trie) -------------------------------------------

/// Corpus for the edit-distance walks: `map`, `maps` and `mop` all sit within
/// one edit of each other while `grape` is far outside them, so moving the
/// distance by one changes the answer.
const FUZZY_CORPUS: &[&str] = &["map", "maps", "mop", "grape"];

/// Wrap a distance the test knows is in range.
fn dist(d: i32) -> FuzzyDistance {
    FuzzyDistance::new(d).expect("test distance is in range")
}

/// Collect every term `iterate_fuzzy` yields for `pattern` at `max_dist`,
/// asserting the walk was not rejected.
fn fuzzy_terms(trie: &TermsTrie, pattern: &[u8], max_dist: i32) -> HashSet<String> {
    let max_dist = dist(max_dist);
    let mut got = HashSet::new();
    // SAFETY: `trie` wraps a live terms trie that is not mutated during the
    // walk; the closure only collects the terms it is handed.
    let walk = unsafe {
        trie.iterate_fuzzy(pattern, max_dist, |term, _| {
            got.insert(runes_to_string(term));
            ControlFlow::Continue(())
        })
    };
    assert!(
        matches!(walk, FuzzyWalk::Walked),
        "the pattern must be short enough to walk"
    );
    got
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_fuzzy_reports_terms_within_the_distance_and_num_docs() {
    with_terms_trie(FUZZY_CORPUS, |trie| {
        let mut got: HashSet<(String, usize)> = HashSet::new();
        // SAFETY: live, un-mutated terms trie; the closure only collects.
        let walk = unsafe {
            trie.iterate_fuzzy(b"map", dist(1), |term, num_docs| {
                got.insert((runes_to_string(term), num_docs));
                ControlFlow::Continue(())
            })
        };
        assert!(matches!(walk, FuzzyWalk::Walked));
        // One edit from `map`: itself, `maps` (insert) and `mop` (substitute);
        // `grape` is far outside. `numDocs` is the term's char length.
        let expected: HashSet<(String, usize)> = [
            ("map".to_owned(), 3),
            ("maps".to_owned(), 4),
            ("mop".to_owned(), 3),
        ]
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
fn iterate_fuzzy_distance_bounds_the_walk() {
    with_terms_trie(FUZZY_CORPUS, |trie| {
        // `mops` is two edits from `map` (substitute `a`, insert `s`), so it is
        // out at distance 1 and in at distance 2 — the bound is honoured in both
        // directions rather than being open-ended.
        assert_eq!(fuzzy_terms(trie, b"mops", 1), set(&["maps", "mop"]));
        assert_eq!(fuzzy_terms(trie, b"mops", 2), set(&["map", "maps", "mop"]));
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_fuzzy_lowercases_the_pattern() {
    with_terms_trie(FUZZY_CORPUS, |trie| {
        // The trie holds folded keys and folds the pattern itself, so an
        // upper-case pattern is not three substitutions away from its own term.
        assert_eq!(fuzzy_terms(trie, b"MAP", 1), set(&["map", "maps", "mop"]));
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_fuzzy_no_match_walks_without_visiting_anything() {
    with_terms_trie(FUZZY_CORPUS, |trie| {
        // Nothing is within one edit of `zzzzzz`, which is still a walk that ran
        // — not a rejected pattern.
        assert_eq!(fuzzy_terms(trie, b"zzzzzz", 1), HashSet::new());
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_fuzzy_empty_pattern_walks_every_term_within_the_distance_of_nothing() {
    with_terms_trie(FUZZY_CORPUS, |trie| {
        // An empty pattern is a walk like any other rather than a rejection: the
        // automaton accepts at the root, so every term reachable in `max_dist`
        // insertions matches — that is, every term no longer than the distance.
        assert_eq!(fuzzy_terms(trie, b"", 3), set(&["map", "mop"]));
        assert_eq!(
            fuzzy_terms(trie, b"", FuzzyDistance::MAX),
            set(&["map", "maps", "mop"])
        );

        // At distance 0 the only term that could match is the empty one, which
        // no trie holds — insertion refuses a zero-length key — so the walk runs
        // and yields nothing rather than yielding a zero-length term.
        assert_eq!(fuzzy_terms(trie, b"", 0), HashSet::new());
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_fuzzy_break_stops_walk_early() {
    with_terms_trie(FUZZY_CORPUS, |trie| {
        let mut count = 0_usize;
        // SAFETY: live, un-mutated terms trie; the closure only counts.
        let walk = unsafe {
            trie.iterate_fuzzy(b"map", dist(1), |_, _| {
                count += 1;
                // Stop at the first match even though three would match.
                ControlFlow::Break(())
            })
        };
        assert!(matches!(walk, FuzzyWalk::Walked));
        assert_eq!(count, 1, "Break must stop the walk at the first match");
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_fuzzy_rejects_a_pattern_over_the_rune_limit() {
    with_terms_trie(FUZZY_CORPUS, |trie| {
        let limit = ffi::TRIE_MAX_PREFIX as usize;
        let mut visited = false;
        // SAFETY: live, un-mutated terms trie; the closure only records.
        let walk = unsafe {
            trie.iterate_fuzzy(&b"a".repeat(limit + 1), dist(1), |_, _| {
                visited = true;
                ControlFlow::Continue(())
            })
        };
        assert!(
            matches!(walk, FuzzyWalk::PatternRejected),
            "a pattern over the rune limit starts no walk"
        );
        assert!(!visited, "a rejected pattern visits nothing");

        // One rune shorter, the walk does run: the limit is an inclusive bound
        // and not an off-by-one that rejects the longest accepted pattern.
        // SAFETY: as above.
        let walk = unsafe {
            trie.iterate_fuzzy(&b"a".repeat(limit), dist(1), |_, _| ControlFlow::Break(()))
        };
        assert!(matches!(walk, FuzzyWalk::Walked));
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_fuzzy_measures_the_limit_in_runes_not_bytes() {
    with_terms_trie(FUZZY_CORPUS, |trie| {
        // The limit applies to the decoded pattern, so a multibyte pattern well
        // over the limit in bytes but under it in runes is still walked. A byte
        // length check would reject it.
        let pattern = "é".repeat(ffi::TRIE_MAX_PREFIX as usize / 2 + 1);
        assert!(pattern.len() > ffi::TRIE_MAX_PREFIX as usize);
        // SAFETY: live, un-mutated terms trie; the closure does nothing.
        let walk = unsafe {
            trie.iterate_fuzzy(
                pattern.as_bytes(),
                dist(1),
                |_, _| ControlFlow::Continue(()),
            )
        };
        assert!(matches!(walk, FuzzyWalk::Walked));
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_fuzzy_accepts_a_pattern_with_a_truncated_multibyte_tail() {
    with_terms_trie(FUZZY_CORPUS, |trie| {
        // A token is a byte string that nothing validates, so a pattern can end
        // mid-sequence. The C decoder reads a fixed 2-4 bytes from a lead byte
        // without checking the input has that many left, so this is the input
        // class that would read past the pattern; under a sanitizer it is what
        // catches an unpadded decode.
        for pattern in [
            &b"ma\xF0"[..],
            b"ma\xF0\x9F",
            b"ma\xE0",
            b"ma\xC3",
            b"ma\x80",
        ] {
            // What the truncated tail decodes to through the zero padding is not
            // uniform: `ma\xF0`, `ma\xE0` and `ma\x80` pad out to codepoint 0,
            // which stops the length pass, so they fold to the two runes `ma`;
            // `ma\xC3` and `ma\xF0\x9F` pad out to a non-zero codepoint and fold
            // to three. Either way `map` is the only term within one edit — an
            // insertion after `ma`, a substitution of the third rune — so what
            // each of these must do is walk and yield it rather than read out of
            // bounds.
            assert_eq!(
                fuzzy_terms(trie, pattern, 1),
                set(&["map"]),
                "a truncated tail must be walked, not read past"
            );
        }
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_fuzzy_byte_cap_admits_the_longest_pattern_that_fits_in_runes() {
    with_terms_trie(FUZZY_CORPUS, |trie| {
        // The over-long pattern is refused on byte length before it is copied,
        // and the widest rune is four bytes — so the densest pattern that still
        // fits the rune limit sits exactly on that byte bound. If the cap were
        // off by one, or measured in anything but the widest rune, this pattern
        // would be refused here despite being one the trie accepts.
        let pattern = "\u{1D11E}".repeat(ffi::TRIE_MAX_PREFIX as usize);
        assert_eq!(pattern.len(), ffi::TRIE_MAX_PREFIX as usize * 4);
        // SAFETY: live, un-mutated terms trie; the closure does nothing.
        let walk = unsafe {
            trie.iterate_fuzzy(
                pattern.as_bytes(),
                dist(1),
                |_, _| ControlFlow::Continue(()),
            )
        };
        assert!(
            matches!(walk, FuzzyWalk::Walked),
            "the byte cap must not refuse a pattern the rune limit admits"
        );
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn iterate_fuzzy_refuses_an_over_long_truncated_tail_before_padding_it() {
    with_terms_trie(FUZZY_CORPUS, |trie| {
        // A truncated trailing sequence is what forces the padded copy, and the
        // token it pads is attacker-supplied and unbounded. Over the byte cap
        // the pattern is refused first, so nothing proportional to it is copied
        // — the same answer C gives, which refuses it on rune length.
        let mut pattern = b"m".repeat(ffi::TRIE_MAX_PREFIX as usize * 4);
        pattern.push(0xF0);
        let mut visited = false;
        // SAFETY: live, un-mutated terms trie; the closure only records.
        let walk = unsafe {
            trie.iterate_fuzzy(&pattern, dist(1), |_, _| {
                visited = true;
                ControlFlow::Continue(())
            })
        };
        assert!(matches!(walk, FuzzyWalk::PatternRejected));
        assert!(!visited, "a refused pattern visits nothing");
    });
}

// --- `iterate_contains` (suffix trie) ---------------------------------------

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

// --- `iterate_wildcard` (suffix trie) ---------------------------------------

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
