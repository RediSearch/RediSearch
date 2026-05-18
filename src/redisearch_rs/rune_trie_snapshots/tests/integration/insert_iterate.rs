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
use std::fmt::Write as _;
use std::ptr;

use ffi::{
    NewTrie, RSPayload, Trie, TrieIterator_Free, TrieIterator_Next, TrieSortMode_Trie_Sort_Lex,
    TrieType_Free, Trie_InsertStringBuffer, Trie_IterateAll, Trie_Size, rune, t_len,
};
use libc::c_char;

/// Decode a slice of runes (BMP-only u16 codepoints) to a Rust String.
/// Mirrors what `runesToStr` in src/trie/rune_util.c does, but doesn't need
/// to be linked through bindgen (the header isn't on the ffi allowlist).
unsafe fn runes_to_string(ptr: *const rune, len: usize) -> String {
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    String::from_utf16(slice).expect("trie runes are valid BMP UTF-16")
}

/// Insert a UTF-8 term into a freshly-created Lex-mode trie.
unsafe fn insert(trie: *mut Trie, term: &str, score: f64, num_docs: usize) {
    // SAFETY: `term.as_bytes()` is a valid borrow for the duration of the call.
    // `Trie_InsertStringBuffer` copies the input internally before returning.
    unsafe {
        Trie_InsertStringBuffer(
            trie,
            term.as_ptr() as *const c_char,
            term.len(),
            score,
            0, // ADD_REPLACE
            ptr::null_mut(),
            num_docs,
        );
    }
}

/// Drive `Trie_IterateAll` to completion and produce a deterministic text dump.
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

        writeln!(
            &mut out,
            "  {term:10}  score={score}  numDocs={num_docs}"
        )
        .unwrap();
    }

    // SAFETY: `it` was just produced by `Trie_IterateAll` and not freed yet.
    unsafe { TrieIterator_Free(it) };
    out
}

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
        unsafe { insert(trie, term, *score, *num_docs) };
    }

    let dump = dump_all(trie);

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
