/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Cover [`TermDictionary::mem_usage`], the Rust replacement for the C
//! `TrieType_MemUsage(sp->terms)` call at `src/spec.c` that feeds
//! `FT.INFO`'s per-spec terms-memory line.
//!
//! The underlying [`TrieMap::mem_usage`] counter is already exhaustively
//! cross-checked against the recursive ground truth in
//! `tests/integration/trie.rs`; this file only asserts the wrapper
//! correctly surfaces the counter and that it moves in the expected
//! direction across the [`TermDictionary`] mutation surface.

use trie_rs::str::term_dict::TermDictionary;

#[test]
fn mem_usage_increases_with_terms() {
    let mut d = TermDictionary::new();
    let initial = d.mem_usage();

    d.add_term("alpha", 1.0, 1);
    let after_one = d.mem_usage();
    assert!(
        after_one > initial,
        "adding a term should grow memory usage: {initial} -> {after_one}",
    );

    d.add_term("beta", 1.0, 1);
    let after_two = d.mem_usage();
    assert!(
        after_two > after_one,
        "adding a second term should grow memory usage: {after_one} -> {after_two}",
    );
}

#[test]
fn mem_usage_returns_to_baseline_after_removal() {
    let mut d = TermDictionary::new();
    let baseline = d.mem_usage();

    d.add_term("gamma", 1.0, 1);
    assert!(d.mem_usage() > baseline);

    d.remove("gamma");
    assert_eq!(
        d.mem_usage(),
        baseline,
        "removing the only term should restore the empty-dictionary footprint",
    );
}

#[test]
fn add_incr_on_existing_term_does_not_grow_storage() {
    let mut d = TermDictionary::new();
    d.add_term("delta", 1.0, 1);
    let after_first = d.mem_usage();

    d.add_term("delta", 1.0, 1);
    assert_eq!(
        d.mem_usage(),
        after_first,
        "ADD_INCR on an existing term only mutates the payload — no new node should be allocated",
    );
}
