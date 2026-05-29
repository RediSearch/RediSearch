/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Snapshot the C rune-trie's lex-mode iteration order over a basic insert
//! scenario.

use std::ffi::c_void;

use ffi::{NewTrie, TrieSortMode_Trie_Sort_Lex, TrieType_Free};

use crate::support::{dump_all, trie_insert_scored};

#[test]
fn lex_basic_insert_iterate_all() {
    // SAFETY: `NewTrie` with a NULL free-callback is the standard pattern for
    // payload-less tries (matches `sp->terms` construction in `src/spec.c`).
    let trie = unsafe { NewTrie(None, TrieSortMode_Trie_Sort_Lex) };

    // A grab-bag of overlapping prefixes to exercise basic radix construction
    // (no splits beyond depth one), plus the score/numDocs columns so we can
    // see they survive insertion intact in Lex mode.
    let terms: &[(&str, f64, usize)] = &[
        ("banana", 2.0, 5),
        ("apple", 1.0, 3),
        ("cherry", 3.0, 4),
        ("apricot", 1.5, 1),
        ("band", 2.5, 2),
        ("date", 1.0, 1),
        ("bandana", 1.0, 1),
    ];
    for (term, score, num_docs) in terms {
        unsafe { trie_insert_scored(trie, term, *score, *num_docs) };
    }

    let dump = dump_all(trie, 10);

    // SAFETY: `trie` was created by `NewTrie` above.
    unsafe { TrieType_Free(trie as *mut c_void) };

    // Explicit snapshot name + `prepend_module_to_snapshot => false` so the
    // resulting file (`lex_basic_insert_iterate_all.snap`) carries no crate-
    // or module-derived prefix, letting `trie_rs`' integration tests assert
    // against the same file via `with_settings!({ snapshot_path => ... })`.
    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_basic_insert_iterate_all", dump); }
    );
}
