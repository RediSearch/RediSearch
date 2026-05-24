/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Assert `TermDictionary` reproduces the C rune-trie's Lex-mode behavior
//! against the shared snapshot owned by `rune_trie_snapshots`.

use std::fmt::Write as _;

use trie_rs::str::term_dict::TermDictionary;

fn dump_all(trie: &TermDictionary) -> String {
    let mut out = String::new();
    writeln!(&mut out, "size: {}", trie.len()).unwrap();
    writeln!(&mut out, "entries:").unwrap();

    for (term, entry) in trie.iter() {
        writeln!(
            &mut out,
            "  {term:10}  score={score}  numDocs={num_docs}",
            score = entry.score,
            num_docs = entry.num_docs,
        )
        .unwrap();
    }
    out
}

#[test]
fn lex_basic_insert_iterate_all() {
    let mut trie = TermDictionary::new();

    // Same input fixture as `rune_trie_snapshots::insert_iterate` so the two
    // implementations share the snapshot file.
    let terms: &[(&str, f32, usize)] = &[
        ("banana", 2.0, 5),
        ("apple", 1.0, 3),
        ("cherry", 3.0, 4),
        ("apricot", 1.5, 1),
        ("band", 2.5, 2),
        ("date", 1.0, 1),
        ("bandana", 1.0, 1),
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
        { insta::assert_snapshot!("lex_basic_insert_iterate_all", dump); }
    );
}
