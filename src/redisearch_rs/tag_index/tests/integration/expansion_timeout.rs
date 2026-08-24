/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Timeout handling of `TagIndex::suffix_expand`, driven entirely through its
//! public `timeout` parameter. On timeout both the wildcard and contains forms
//! stop and return the matches gathered so far (partial results).
//!
//! The tests that actually reach the deadline are `#[cfg_attr(miri, ignore)]`:
//! probing it calls `clock_gettime(CLOCK_MONOTONIC_RAW)`, which miri does not
//! implement. The no-timeout tests stay in miri's reach.
//!
//! [`expansion_deadline`](tag_index)'s own opt-out/elapsed-deadline behavior is
//! a `pub(crate)` internal, covered by the crate's own unit tests instead.

use ffi::timespec;
use tag_index::{InMemoryMode, SuffixQuery, SuffixWildcardPattern, Tag, TagIndex};

use crate::util::commit_mem;

const NO_CAP: u64 = u64::MAX;
/// Comfortably larger than the trie iterators' clock-probe granularity (100
/// traversal steps, a `trie_rs` internal), so a zero-budget deadline is
/// guaranteed to trigger before the corpus is exhausted while still leaving
/// many entries unprocessed.
#[cfg(not(miri))]
const CORPUS: usize = 300;
/// Miri interprets every traversal step, and a 300-term expansion there exceeds
/// `nextest`'s slow-test budget. Only the tests that assert on a *partial*
/// expansion need a corpus above the clock-probe granularity, and those need a
/// real clock, so they are `ignore`d under Miri anyway. What still runs only
/// needs more than one term.
#[cfg(miri)]
const CORPUS: usize = 8;

/// A deadline that has already elapsed. Any `CLOCK_MONOTONIC_RAW` value one
/// second after boot is in the past on a running system, so
/// `duration_from_redis_timespec` maps it to a zero remaining budget.
fn expired() -> timespec {
    timespec {
        tv_sec: 1,
        tv_nsec: 0,
    }
}

/// Build a `WITHSUFFIXTRIE` index over `CORPUS` distinct terms that all
/// share the literal prefix `he`. `he*` (wildcard) visits one full-term
/// suffix entry per term, and the contains-expansion `e` visits one
/// proper-suffix entry per term — so both walks visit exactly `CORPUS`
/// entries, which is what makes `CORPUS` the knob the deadline tests rely on.
fn big_index() -> (TagIndex<InMemoryMode>, usize) {
    let owned: Vec<Vec<u8>> = (0..CORPUS)
        .map(|i| format!("he{i:05}").into_bytes())
        .collect();
    let tags: Vec<&[u8]> = owned.iter().map(Vec::as_slice).collect();
    let mut idx = TagIndex::<InMemoryMode>::new(true);
    commit_mem(&mut idx, &tags);
    (idx, tags.len())
}

/// An uncapped wildcard query over `pattern`: these tests bound the expansion
/// by the deadline, never by the expansion cap.
fn wildcard<'p>(pattern: &'p SuffixWildcardPattern<'p>) -> SuffixQuery<'p> {
    SuffixQuery::Wildcard {
        pattern,
        max_prefix_expansions: NO_CAP,
    }
}

#[test]
fn wildcard_no_timeout_returns_all() {
    let (idx, total) = big_index();
    let pattern = SuffixWildcardPattern::new(b"he*").expect("valid token");
    let got = idx.suffix_expand(wildcard(&pattern), None).count();
    assert_eq!(got, total, "every `he*` term must be expanded");
}

#[test]
#[cfg_attr(miri, ignore)]
fn wildcard_times_out_with_partial_results() {
    let (idx, total) = big_index();
    // An already-elapsed deadline must not panic, and must yield a strict,
    // non-empty subset.
    let pattern = SuffixWildcardPattern::new(b"he*").expect("valid token");
    let got = idx
        .suffix_expand(wildcard(&pattern), Some(expired()))
        .count();
    assert!(got > 0, "the first granularity-1 entries are collected");
    assert!(got < total, "timeout must stop before the full expansion");
}

// `SuffixQuery::Contains` prefix-iterates the suffix trie, probing the
// deadline once per entry.
#[test]
fn contains_no_timeout_returns_all() {
    let (idx, total) = big_index();
    let got = idx
        .suffix_expand(SuffixQuery::Contains(Tag::new(b"e").unwrap()), None)
        .count();
    assert_eq!(got, total, "every term containing `e` must be expanded");
}

#[test]
#[cfg_attr(miri, ignore)]
fn contains_times_out_with_partial_results() {
    let (idx, total) = big_index();
    let got = idx
        .suffix_expand(
            SuffixQuery::Contains(Tag::new(b"e").unwrap()),
            Some(expired()),
        )
        .count();
    assert!(got > 0, "the first granularity-1 entries are collected");
    assert!(got < total, "timeout must stop before the full expansion");
}
