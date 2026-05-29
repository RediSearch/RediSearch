/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Snapshot the C trie's response to an insert sequence that walks every
//! `TrieNode_Add` structural path in Lex mode:
//!
//!   1. Empty trie + first insert            -> single leaf
//!   2. Common-prefix divergence             -> splits existing leaf into
//!                                              shared-prefix internal + two
//!                                              suffix leaves
//!   3. Exact-prefix insert at internal node -> turns the split node terminal
//!                                              (offset == len after split)
//!   4. Divergence inside an internal node   -> deep split below an existing
//!                                              internal node
//!   5. Disjoint first rune                  -> new root child, no split
//!   6. Re-insert existing terminal          -> structural no-op; score is
//!                                              replaced, numDocs accumulates

use std::ffi::c_void;
use std::fmt::Write as _;

use ffi::{NewTrie, TrieSortMode_Trie_Sort_Lex, TrieType_Free};

use crate::support::{dump_all, trie_insert_scored};

#[test]
fn lex_insert_sequence_splits() {
    // SAFETY: `NewTrie` with a NULL free-callback is the standard payload-less
    // construction (mirrors `sp->terms` in `src/spec.c`).
    let trie = unsafe { NewTrie(None, TrieSortMode_Trie_Sort_Lex) };

    // Each tuple: (label describing the structural event, term, score, numDocs).
    let steps: &[(&str, &str, f64, usize)] = &[
        ("first insert into empty trie", "apple", 1.0, 1),
        ("split leaf at shared prefix 'appl'", "apply", 2.0, 1),
        (
            "exact-prefix insert -> terminal at internal",
            "appl",
            3.0,
            1,
        ),
        ("deep split below internal 'appl'", "ape", 4.0, 1),
        ("disjoint first rune -> new root child", "b", 5.0, 1),
        ("re-insert existing terminal (REPLACE)", "b", 6.0, 2),
    ];

    let mut out = String::new();
    for (label, term, score, num_docs) in steps {
        unsafe { trie_insert_scored(trie, term, *score, *num_docs) };
        writeln!(
            &mut out,
            "--- after insert({term:?}, score={score}, numDocs={num_docs}) — {label} ---"
        )
        .unwrap();
        out.push_str(&dump_all(trie, 12));
    }

    // SAFETY: `trie` was created by `NewTrie` above.
    unsafe { TrieType_Free(trie as *mut c_void) };

    // Bare snapshot name so the file is shared with `trie_rs`' integration
    // test for the same scenario (see `prepend_module_to_snapshot => false`
    // explanation in `insert_iterate.rs`).
    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_insert_sequence_splits", out); }
    );
}
