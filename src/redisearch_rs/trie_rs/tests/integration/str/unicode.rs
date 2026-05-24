/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Assert `TermDictionary` reproduces the C trie's Lex iteration order
//! over a mix of ASCII, Latin-1, and CJK BMP terms.
//!
//! See `rune_trie_snapshots/tests/integration/unicode.rs` for the C oracle
//! and the rationale (in short: any byte-wise sort after a little-endian
//! split of `u16` keys reorders runes that cross the 0x0100 boundary).

use std::fmt::Write as _;

use trie_rs::term_dict::TermDictionary;

fn dump_all(trie: &TermDictionary) -> String {
    let mut out = String::new();
    writeln!(&mut out, "size: {}", trie.len()).unwrap();
    writeln!(&mut out, "entries:").unwrap();

    for (term, entry) in trie.iter() {
        let first_cp = term.chars().next().map(u32::from).unwrap_or(0);
        writeln!(
            &mut out,
            "  U+{first_cp:04X}  {term:8}  score={score}  numDocs={num_docs}",
            score = entry.score,
            num_docs = entry.num_docs,
        )
        .unwrap();
    }
    out
}

#[test]
fn lex_unicode_bmp_iteration_order() {
    let mut trie = TermDictionary::new();

    // Same fixture as `rune_trie_snapshots::unicode`; inserts deliberately
    // out of lex order.
    let terms: &[(&str, f32, usize)] = &[
        ("中文", 1.0, 1),
        ("Zoé", 2.0, 1),
        ("apple", 3.0, 1),
        ("naïve", 4.0, 1),
        ("日本", 5.0, 1),
        ("café", 6.0, 1),
        ("Z", 7.0, 1),
    ];
    for (term, score, num_docs) in terms {
        trie.replace_term(term, *score, *num_docs);
    }

    let dump = dump_all(&trie);

    insta::with_settings!(
        {
            snapshot_path => "../../../../rune_trie_snapshots/tests/integration/snapshots",
            prepend_module_to_snapshot => false,
        },
        { insta::assert_snapshot!("lex_unicode_bmp_iteration_order", dump); }
    );
}
