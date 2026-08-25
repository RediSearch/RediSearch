/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! `should_stop` contract: the predicate is polled per traversal step
//! (amortized every [`TIMEOUT_CHECK_GRANULARITY`] steps), not per yielded
//! entry, so it fires even on walks that visit many nodes without yielding
//! anything.

use std::cell::Cell;
use trie_rs::automaton::CodepointWildcard;
use trie_rs::iter::TIMEOUT_CHECK_GRANULARITY;
use trie_rs::str_trie_map::StrTrieMap;

/// Enough keys that any full walk takes well over
/// [`TIMEOUT_CHECK_GRANULARITY`] traversal steps.
const N_KEYS: usize = 300;

fn seeded_trie() -> StrTrieMap<usize> {
    let mut trie = StrTrieMap::new();
    for i in 0..N_KEYS {
        trie.insert(&format!("key{i:04}"), i);
    }
    trie
}

#[test]
fn prefixed_iter_stops_mid_walk() {
    let trie = seeded_trie();

    let mut iter = trie.prefixed_iter_with_should_stop("key", || true);
    let yielded = iter.by_ref().count();

    assert!(
        yielded < N_KEYS,
        "an always-true predicate must cut the walk short, got all {yielded} entries"
    );
    assert!(iter.next().is_none(), "a stopped iterator stays exhausted");
}

#[test]
fn prefixed_iter_polls_predicate_amortized() {
    let trie = seeded_trie();
    let calls = Cell::new(0u32);

    let yielded = trie
        .prefixed_iter_with_should_stop("key", || {
            calls.set(calls.get() + 1);
            false
        })
        .count();

    assert_eq!(yielded, N_KEYS, "a false predicate must not drop entries");
    // Every node contributes at least one traversal step, so a full walk
    // over N_KEYS keys polls the predicate at least N_KEYS / granularity
    // times.
    let min_polls = N_KEYS as u32 / TIMEOUT_CHECK_GRANULARITY;
    assert!(
        calls.get() >= min_polls,
        "expected >= {min_polls} polls over a full walk, got {}",
        calls.get()
    );
}

#[test]
fn suffixed_iter_polls_predicate_on_yieldless_walk() {
    let trie = seeded_trie();
    let calls = Cell::new(0u32);

    // No key ends with "zzz": the walk visits the whole trie and yields
    // nothing, so a per-yield poll would never fire.
    let count = trie
        .suffixed_iter_with_should_stop("zzz", || {
            calls.set(calls.get() + 1);
            false
        })
        .count();

    assert_eq!(count, 0);
    assert!(
        calls.get() > 0,
        "the predicate must be polled per traversal step, not per yield"
    );
}

#[test]
fn suffixed_iter_stops_mid_walk() {
    let trie = seeded_trie();
    let calls = Cell::new(0u32);

    let count = trie
        .suffixed_iter_with_should_stop("zzz", || {
            calls.set(calls.get() + 1);
            true
        })
        .count();

    assert_eq!(count, 0);
    assert_eq!(
        calls.get(),
        1,
        "an always-true predicate must end the walk at its first poll"
    );
}

#[test]
fn contains_iter_polls_predicate_on_yieldless_walk() {
    let trie = seeded_trie();
    let calls = Cell::new(0u32);

    let count = trie
        .contains_iter_with_should_stop("zzz", || {
            calls.set(calls.get() + 1);
            false
        })
        .count();

    assert_eq!(count, 0);
    assert!(
        calls.get() > 0,
        "the predicate must be polled per traversal step, not per yield"
    );
}

#[test]
fn contains_iter_stops_mid_walk() {
    let trie = seeded_trie();

    // Every key contains "key", so a full walk yields everything.
    let count = trie.contains_iter_with_should_stop("key", || true).count();

    assert!(
        count < N_KEYS,
        "an always-true predicate must cut the walk short"
    );
}

#[test]
fn wildcard_iter_nfa_backend_stops_mid_walk() {
    let trie = seeded_trie();

    // Few atoms: NFA backend.
    let count = trie.wildcard_iter_with_should_stop("key*", || true).count();

    assert!(
        count < N_KEYS,
        "an always-true predicate must cut the NFA walk short"
    );
}

#[test]
fn wildcard_iter_filter_backend_polls_predicate() {
    let trie = seeded_trie();
    let calls = Cell::new(0u32);

    // >= 128 atoms forces the per-key filter backend; no stored key is 128
    // codepoints long, so the walk yields nothing.
    let pattern = "?".repeat(128);
    let count = trie
        .wildcard_iter_with_should_stop(&pattern, || {
            calls.set(calls.get() + 1);
            false
        })
        .count();

    assert_eq!(count, 0);
    assert!(
        calls.get() > 0,
        "the filter backend must poll the predicate per traversal step"
    );
}

#[test]
fn wildcard_iter_nfa128_backend_stops_mid_walk() {
    // Long keys so a pattern that matches all of them still needs more
    // NFA positions than a `u64` bitset holds.
    let mut trie = StrTrieMap::new();
    for i in 0..N_KEYS {
        trie.insert(&format!("key{i:04}{}", "z".repeat(70)), i);
    }

    let pattern = format!("key????{}*", "z".repeat(70));
    // The `u64` backend covers up to 64 positions and the accept position
    // sits past the last atom, so this pattern selects the `u128` one.
    let positions = CodepointWildcard::parse(&pattern).atom_count() + 1;
    assert!(
        (65..=128).contains(&positions),
        "pattern must select the u128 NFA backend, got {positions} positions"
    );

    let full = trie
        .wildcard_iter_with_should_stop(&pattern, || false)
        .count();
    assert_eq!(full, N_KEYS, "the pattern must match every key");

    let stopped = trie
        .wildcard_iter_with_should_stop(&pattern, || true)
        .count();
    assert!(
        stopped < N_KEYS,
        "an always-true predicate must cut the u128 NFA walk short"
    );
}
