/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Assert `RuneTrieMap` reproduces the C trie's Lex-mode behavior over a
//! sequence of inserts that exercise every `TrieNode_Add` structural path.
//!
//! See `rune_trie_snapshots/tests/integration/splits.rs` for the C oracle
//! that owns the shared `.snap` file.
//!
//! Assumed `RuneTrieMap` API (mirrors the C oracle's per-entry payload by
//! storing score + numDocs in the generic `V`; reductions/replace semantics
//! must be handled by the impl):
//!   - `RuneTrieMap::<V>::new() -> Self`
//!   - `insert(&mut self, key: &[Rune], value: V)` — on duplicate key, must
//!     match the C `ADD_REPLACE` path: replace score, accumulate num_docs.
//!   - `len(&self) -> usize`
//!   - `iter(&self) -> impl Iterator<Item = (Vec<Rune>, &V)>` in lex order

use std::fmt::Write as _;

use trie_rs::rune::{Rune, RuneTrieMap};

struct TermEntry {
    score: f32,
    num_docs: usize,
}

fn term_runes(s: &str) -> Vec<Rune> {
    s.encode_utf16().collect()
}

fn dump_all(trie: &RuneTrieMap<TermEntry>) -> String {
    let mut out = String::new();
    writeln!(&mut out, "size: {}", trie.len()).unwrap();
    writeln!(&mut out, "entries:").unwrap();

    for (key, entry) in trie.iter() {
        let term = String::from_utf16(&key).expect("trie runes are valid BMP UTF-16");
        writeln!(
            &mut out,
            "  {term:12}  score={score}  numDocs={num_docs}",
            score = entry.score,
            num_docs = entry.num_docs,
        )
        .unwrap();
    }
    out
}

#[test]
fn lex_insert_sequence_splits() {
    let mut trie = RuneTrieMap::<TermEntry>::new();

    let steps: &[(&str, &str, f32, usize)] = &[
        ("first insert into empty trie", "apple", 1.0, 1),
        ("split leaf at shared prefix 'appl'", "apply", 2.0, 1),
        ("exact-prefix insert -> terminal at internal", "appl", 3.0, 1),
        ("deep split below internal 'appl'", "ape", 4.0, 1),
        ("disjoint first rune -> new root child", "b", 5.0, 1),
        ("re-insert existing terminal (REPLACE)", "b", 6.0, 2),
    ];

    let mut out = String::new();
    for (label, term, score, num_docs) in steps {
        trie.insert(
            &term_runes(term),
            TermEntry {
                score: *score,
                num_docs: *num_docs,
            },
        );
        writeln!(
            &mut out,
            "--- after insert({term:?}, score={score}, numDocs={num_docs}) — {label} ---"
        )
        .unwrap();
        out.push_str(&dump_all(&trie));
    }

    insta::with_settings!(
        {
            snapshot_path => "../../../rune_trie_snapshots/tests/integration/snapshots",
            prepend_module_to_snapshot => false,
        },
        { insta::assert_snapshot!("lex_insert_sequence_splits", out); }
    );
}
