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

use c_trie::{FuzzyWalk, LoweredPattern, TermsTrie, TrieTerm};

fn trie_term(term: &str) -> TrieTerm {
    // SAFETY: every test input is the complete byte representation of a
    // non-empty key accepted by the primary terms trie.
    unsafe { TrieTerm::from_bytes_unchecked(Box::from(term.as_bytes())) }
}

fn clock_timeout(deadline: ffi::timespec) -> ffi::QueryRequestTimeout {
    // SAFETY: every field in the C timeout struct accepts an all-zero value.
    let mut timeout: ffi::QueryRequestTimeout = unsafe { mem::zeroed() };
    timeout.kind = ffi::QueryRequestTimeoutKind_QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE;
    timeout.source.clockDeadline = deadline;
    timeout
}

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
        .map(|term| String::from_utf8(term.into_bytes().into_vec()).expect("term is valid UTF-8"))
        .collect()
}

/// Corpus used across the anchoring tests. Every term contains `"ap"`; only
/// `apple`/`apricot` start with it and only `apple`/`maple` end with `"ple"`.
const CORPUS: &[&str] = &["apple", "maple", "grape", "apricot", "map"];

fn set(items: &[&str]) -> HashSet<String> {
    items.iter().map(|s| (*s).to_owned()).collect()
}

// --- `num_docs` -------------------------------------------------------------

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn num_docs_reports_the_stored_count_and_zero_for_absent_terms() {
    with_terms_trie(CORPUS, |trie| {
        // The corpus stores each term's char length as its document count.
        assert_eq!(trie.num_docs(b"apple"), 5);
        assert_eq!(trie.num_docs(b"map"), 3);
        assert_eq!(trie.num_docs(b"pear"), 0);
    });
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn num_docs_of_the_empty_term_is_zero_without_a_lookup() {
    // A zero-length key is refused on insertion, so no trie can answer anything
    // but zero here — and the lookup is skipped rather than handing C an empty
    // slice's pointer to do arithmetic on.
    with_terms_trie(CORPUS, |trie| {
        assert_eq!(trie.num_docs(b""), 0);
    });
    // Including for a trie that was never given any term at all.
    with_terms_trie(&[], |trie| {
        assert_eq!(trie.num_docs(b""), 0);
    });
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
    // wrapper's timeout mapping and confirms both an absent timeout and a
    // request-owned clock timeout return the same set.
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
        let timeout = clock_timeout(deadline);
        let mut some_hits = HashSet::new();
        trie.iterate_contains(&runes, true, true, Some(&timeout), |term, _| {
            some_hits.insert(runes_to_string(term));
            ControlFlow::Continue(())
        });

        assert_eq!(none_hits, set(CORPUS));
        assert_eq!(some_hits, none_hits);
    });
}

// --- `iterate_fuzzy` --------------------------------------------------------

/// Corpus for the edit-distance walks: `map`, `maps` and `mop` all sit within
/// one edit of each other while `grape` is far outside them, so moving the
/// distance by one changes the answer.
const FUZZY_CORPUS: &[&str] = &["map", "maps", "mop", "grape"];

/// Collect every term `iterate_fuzzy` yields for `pattern` at `max_dist`,
/// asserting the distance was accepted and the walk was not rejected.
fn fuzzy_terms(trie: &TermsTrie, pattern: &[u8], max_dist: i32) -> HashSet<String> {
    let mut got = HashSet::new();
    // SAFETY: `trie` wraps a live terms trie that is not mutated during the
    // walk; the closure only collects the terms it is handed.
    let walk = unsafe {
        trie.iterate_fuzzy(pattern, max_dist, |term, _| {
            got.insert(runes_to_string(term));
            ControlFlow::Continue(())
        })
    }
    .expect("test distance is in range");
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
fn iterate_fuzzy_admits_exactly_the_distances_the_automaton_can_be_built_for() {
    let max = ffi::MAX_LEV_DISTANCE as i32;
    with_terms_trie(FUZZY_CORPUS, |trie| {
        // Both ends reach C as the size of a stack VLA, so what the walk refuses
        // is what the automaton could not be built for: below zero it is a
        // negative-length allocation, above the cap it exhausts the stack. The
        // refusal has to come before the walk, so nothing is visited either.
        for d in [-1, i32::MIN, max + 1, i32::MAX] {
            let mut visited = false;
            // SAFETY: live, un-mutated terms trie; the closure only records.
            let walk = unsafe {
                trie.iterate_fuzzy(b"map", d, |_, _| {
                    visited = true;
                    ControlFlow::Continue(())
                })
            };
            assert_eq!(
                walk.unwrap_err().to_string(),
                format!("fuzzy distance must be in 0..={max}, got {d}"),
                "{d} is out of range, and the error must name it"
            );
            assert!(!visited, "a refused distance visits nothing");
        }

        // Every distance in range is walked rather than refused — the cap is an
        // inclusive bound and not an off-by-one that rejects the widest one.
        for d in 0..=max {
            // SAFETY: as above.
            let walk = unsafe { trie.iterate_fuzzy(b"map", d, |_, _| ControlFlow::Continue(())) };
            assert!(
                matches!(walk, Ok(FuzzyWalk::Walked)),
                "{d} is in range and must be walked"
            );
        }
    });
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
            trie.iterate_fuzzy(b"map", 1, |term, num_docs| {
                got.insert((runes_to_string(term), num_docs));
                ControlFlow::Continue(())
            })
        };
        assert!(matches!(walk, Ok(FuzzyWalk::Walked)));
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
            fuzzy_terms(trie, b"", ffi::MAX_LEV_DISTANCE as i32),
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
            trie.iterate_fuzzy(b"map", 1, |_, _| {
                count += 1;
                // Stop at the first match even though three would match.
                ControlFlow::Break(())
            })
        };
        assert!(matches!(walk, Ok(FuzzyWalk::Walked)));
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
            trie.iterate_fuzzy(&b"a".repeat(limit + 1), 1, |_, _| {
                visited = true;
                ControlFlow::Continue(())
            })
        };
        assert!(
            matches!(walk, Ok(FuzzyWalk::PatternRejected)),
            "a pattern over the rune limit starts no walk"
        );
        assert!(!visited, "a rejected pattern visits nothing");

        // One rune shorter, the walk does run: the limit is an inclusive bound
        // and not an off-by-one that rejects the longest accepted pattern.
        // SAFETY: as above.
        let walk =
            unsafe { trie.iterate_fuzzy(&b"a".repeat(limit), 1, |_, _| ControlFlow::Break(())) };
        assert!(matches!(walk, Ok(FuzzyWalk::Walked)));
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
        let walk =
            unsafe { trie.iterate_fuzzy(pattern.as_bytes(), 1, |_, _| ControlFlow::Continue(())) };
        assert!(matches!(walk, Ok(FuzzyWalk::Walked)));
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
        let walk =
            unsafe { trie.iterate_fuzzy(pattern.as_bytes(), 1, |_, _| ControlFlow::Continue(())) };
        assert!(
            matches!(walk, Ok(FuzzyWalk::Walked)),
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
            trie.iterate_fuzzy(&pattern, 1, |_, _| {
                visited = true;
                ControlFlow::Continue(())
            })
        };
        assert!(matches!(walk, Ok(FuzzyWalk::PatternRejected)));
        assert!(!visited, "a refused pattern visits nothing");
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
            .map(|term| String::from_utf8(term.into_bytes().into_vec()).expect("terms are ASCII"))
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
            got.insert(String::from_utf8(term.into_bytes().into_vec()).expect("terms are ASCII"));
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
        let timeout = clock_timeout(deadline);
        let mut some_hits = HashSet::new();
        trie.iterate_wildcard(&pattern("ap*"), Some(&timeout), |term, _| {
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
        assert!(trie.delete(&trie_term("maple")), "a stored term is removed");
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
        assert!(trie.delete(&trie_term("apple")));
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
        let removed = trie.delete(&trie_term("apricot"));
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
        let removed = trie.delete(&trie_term("app"));
        assert!(!removed, "a prefix is not an exact match");
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
        assert!(trie.delete(&trie_term("héllo")));
        assert_eq!(terms_of(trie), set(&["wörld"]));
    });
}
