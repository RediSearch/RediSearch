/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Snapshot the C trie's Lex iteration order over a mix of ASCII, Latin-1,
//! and CJK BMP terms. The C trie compares runes as `u16` codepoints, so the
//! expected order is by raw codepoint value of the first differing rune.
//!
//! This scenario deliberately includes runes on both sides of the 0x0100
//! boundary (where the high byte transitions from 0x00 to non-zero). Any
//! Rust port that lex-orders runes via byte-wise comparison after a
//! little-endian split will sort these terms incorrectly — for example
//! `中` (0x4E2D) -> LE bytes `[0x2D, 0x4E]`, which a byte-trie sees as
//! starting before `Z` (0x5A). The snapshot diff makes such a divergence
//! immediately visible.

use std::ffi::c_void;
use std::fmt::Write as _;
use std::ptr;

use ffi::{
    NewTrie, RSPayload, Trie, Trie_IterateAll, Trie_Size, TrieIterator_Free, TrieIterator_Next,
    TrieSortMode_Trie_Sort_Lex, TrieType_Free, rune, t_len,
};

use crate::support::{runes_to_string, trie_insert_scored};

fn dump_all(trie: *mut Trie) -> String {
    // SAFETY: `trie` was created via `NewTrie` and has not been freed.
    let size = unsafe { Trie_Size(trie) };
    let it = unsafe { Trie_IterateAll(trie) };

    let mut runes_ptr: *mut rune = ptr::null_mut();
    let mut rune_len: t_len = 0;
    let mut payload = RSPayload {
        data: ptr::null_mut(),
        len: 0,
    };
    let mut score: f32 = 0.0;
    let mut num_docs: usize = 0;

    let mut out = String::new();
    writeln!(&mut out, "size: {size}").unwrap();
    writeln!(&mut out, "entries:").unwrap();

    // SAFETY: all out-pointers are valid for writes; the iterator owns the
    // returned `runes_ptr` buffer and reuses it across iterations.
    while unsafe {
        TrieIterator_Next(
            it,
            &mut runes_ptr,
            &mut rune_len,
            &mut payload,
            &mut score,
            &mut num_docs,
            ptr::null_mut(),
        )
    } != 0
    {
        // SAFETY: iterator hands us a valid rune buffer of length `rune_len`.
        let term = unsafe { runes_to_string(runes_ptr, rune_len as usize) };

        // Print the first rune's codepoint so the lex order is auditable from
        // the snapshot itself without needing a Unicode table on hand.
        let first_cp = term.encode_utf16().next().unwrap_or(0);
        writeln!(
            &mut out,
            "  U+{first_cp:04X}  {term:8}  score={score}  numDocs={num_docs}"
        )
        .unwrap();
    }

    // SAFETY: `it` was just produced by `Trie_IterateAll` and not freed yet.
    unsafe { TrieIterator_Free(it) };
    out
}

#[test]
fn lex_unicode_bmp_iteration_order() {
    // SAFETY: `NewTrie` with a NULL free-callback is the standard payload-less
    // construction.
    let trie = unsafe { NewTrie(None, TrieSortMode_Trie_Sort_Lex) };

    // Inserts are deliberately out of lex order so passing the snapshot
    // actually proves the trie sorts internally — not just that input order
    // happened to be correct.
    //
    // Codepoints (first rune of each term):
    //   'a'  = U+0061     'c'  = U+0063     'n'  = U+006E
    //   'z'  = U+007A     '中' = U+4E2D     '日' = U+65E5
    //
    // Expected lex output order (by first-rune codepoint, ties broken on the
    // next differing rune):
    //   apple, café, naïve, z, zoé, 中文, 日本
    //
    // Keys are pre-lowercased to match the production C tokenizer's contract
    // and the Rust `TermDictionary`'s internal case-folding. This test's
    // signal — that BMP runes sort by codepoint, not by little-endian byte
    // split — is preserved by `z` (0x007A) vs `中` (0x4E2D, LE bytes
    // `[0x2D, 0x4E]`): a byte-trie with LE confusion would still place `中`
    // before `z`, which the snapshot would catch.
    let terms: &[(&str, f64, usize)] = &[
        ("中文", 1.0, 1), // U+4E2D U+6587
        ("zoé", 2.0, 1),  // shares 'z' prefix with bare "z"
        ("apple", 3.0, 1),
        ("naïve", 4.0, 1), // contains U+00EF (still < 0x100)
        ("日本", 5.0, 1),  // U+65E5 U+672C — last in codepoint order
        ("café", 6.0, 1),  // contains U+00E9
        ("z", 7.0, 1),     // single-rune term, exact prefix of "zoé"
    ];
    for (term, score, num_docs) in terms {
        unsafe { trie_insert_scored(trie, term, *score, *num_docs) };
    }

    let dump = dump_all(trie);

    // SAFETY: `trie` was created by `NewTrie` above.
    unsafe { TrieType_Free(trie as *mut c_void) };

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_unicode_bmp_iteration_order", dump); }
    );
}
