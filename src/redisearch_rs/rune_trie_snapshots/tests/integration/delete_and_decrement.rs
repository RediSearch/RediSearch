/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Snapshot the C rune-trie's mutation paths in Lex mode.
//!
//! Two scenarios share one base fixture (built by [`build_base`]):
//!
//!   1. `lex_delete_sequence_structural_events` — drives [`Trie_Delete`]
//!      through every observable behavior: leaf delete (with cascade-merge
//!      of the parent's single remaining child), internal-terminal delete
//!      (mark-deleted, children survive), no-op on missing key, no-op on
//!      already-deleted key, re-insert that un-deletes the slot.
//!
//!   2. `lex_decrement_numdocs_return_codes` — drives [`Trie_DecrementNumDocs`]
//!      through every return-code path (NOT_FOUND on missing key, NOT_FOUND on
//!      internal non-terminal exact match, UPDATED below threshold, DELETED at
//!      threshold, DELETED with delta clamped above numDocs, NOT_FOUND on a
//!      previously-deleted key).
//!
//! The mark-deleted-vs-physical-removal split, the optimize-sweep merge logic,
//! and the non-terminal rejection in `Trie_DecrementNumDocs` are exactly the
//! places a naive Rust port (e.g. one that physically removes nodes on delete,
//! or one that decrements numDocs on any node `Trie_GetNode` returns) will
//! diverge — these snapshots pin the observable behavior so the divergence
//! surfaces as a diff.

use std::ffi::c_void;
use std::fmt::Write as _;
use std::ptr;

use ffi::{
    NewTrie, RSPayload, Trie, Trie_DecrementNumDocs, Trie_Delete, Trie_InsertStringBuffer,
    Trie_IterateAll, Trie_Size, TrieDecrResult, TrieDecrResult_TRIE_DECR_DELETED,
    TrieDecrResult_TRIE_DECR_NOT_FOUND, TrieDecrResult_TRIE_DECR_UPDATED, TrieIterator_Free,
    TrieIterator_Next, TrieSortMode_Trie_Sort_Lex, TrieType_Free, rune, t_len,
};
use libc::c_char;

unsafe fn runes_to_string(ptr: *const rune, len: usize) -> String {
    // SAFETY: caller passes a valid rune buffer of length `len` (the iterator
    // hands us its internal buffer; range is `len`).
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    String::from_utf16(slice).expect("trie runes are valid BMP UTF-16")
}

unsafe fn insert(trie: *mut Trie, term: &str, score: f64, num_docs: usize) {
    // SAFETY: `term.as_bytes()` is a valid borrow for the call; C copies the
    // buffer internally before returning.
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

unsafe fn delete(trie: *mut Trie, term: &str) -> i32 {
    // SAFETY: `term.as_bytes()` is a valid borrow for the call.
    unsafe { Trie_Delete(trie, term.as_ptr() as *const c_char, term.len()) }
}

unsafe fn decrement(trie: *mut Trie, term: &str, delta: usize) -> TrieDecrResult {
    // SAFETY: `term.as_bytes()` is a valid borrow for the call.
    unsafe { Trie_DecrementNumDocs(trie, term.as_ptr() as *const c_char, term.len(), delta) }
}

/// Render a `TrieDecrResult` as the same identifier used in `trie.h`, so the
/// snapshot reads against the C source without needing a value table on hand.
const fn decr_name(r: TrieDecrResult) -> &'static str {
    if r == TrieDecrResult_TRIE_DECR_NOT_FOUND {
        "NOT_FOUND"
    } else if r == TrieDecrResult_TRIE_DECR_UPDATED {
        "UPDATED"
    } else if r == TrieDecrResult_TRIE_DECR_DELETED {
        "DELETED"
    } else {
        "UNKNOWN"
    }
}

fn dump_all(trie: *mut Trie) -> String {
    // SAFETY: `trie` was created via `NewTrie` and has not been freed.
    let size = unsafe { Trie_Size(trie) };
    // SAFETY: `trie` was created via `NewTrie` and has not been freed; the
    // returned iterator is freed below.
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
        writeln!(&mut out, "  {term:10}  score={score}  numDocs={num_docs}").unwrap();
    }

    // SAFETY: `it` was just produced by `Trie_IterateAll` and not freed yet.
    unsafe { TrieIterator_Free(it) };
    out
}

/// Seven overlapping terms chosen so the resulting trie has every interesting
/// shape exercised by the two scenarios below:
///
/// ```text
/// root
///  ├─ ap        (non-terminal, created by ape vs appl split)
///  │   ├─ e            (terminal "ape")
///  │   └─ pl           (terminal "appl", numDocs=4)
///  │       ├─ e        (terminal "apple", numDocs=3)
///  │       └─ y        (terminal "apply", numDocs=2)
///  └─ b         (terminal "b", numDocs=5)
///      └─ an    (non-terminal, created by banana vs band split)
///          ├─ ana       (terminal "banana", numDocs=2)
///          └─ d         (terminal "band",   numDocs=1)
/// ```
///
/// Notable structural features the scenarios exploit:
/// - `"appl"` is an **internal terminal** (terminal node with children) — its
///   delete behavior differs from a leaf delete.
/// - `"ape"` is a **leaf with one sibling** under `"ap"` — its delete triggers
///   the optimize-sweep merge of `"ap"` with its surviving single child `"pl"`.
/// - `"ban"` matches the internal non-terminal `"an"` node — `Trie_GetNode`
///   returns it but `Trie_DecrementNumDocs` rejects non-terminals.
unsafe fn build_base(trie: *mut Trie) {
    let terms: &[(&str, f64, usize)] = &[
        ("apple", 1.0, 3),
        ("apply", 1.0, 2),
        ("appl", 1.0, 4),
        ("ape", 1.0, 1),
        ("b", 1.0, 5),
        ("banana", 1.0, 2),
        ("band", 1.0, 1),
    ];
    for (term, score, num_docs) in terms {
        // SAFETY: `trie` is live; term bytes valid for the call.
        unsafe { insert(trie, term, *score, *num_docs) };
    }
}

#[test]
fn lex_delete_sequence_structural_events() {
    // SAFETY: `NewTrie` with a NULL free-callback matches the terms-trie
    // construction in `src/spec.c`.
    let trie = unsafe { NewTrie(None, TrieSortMode_Trie_Sort_Lex) };
    // SAFETY: `trie` is live.
    unsafe { build_base(trie) };

    let mut out = String::new();
    writeln!(&mut out, "=== base ===").unwrap();
    out.push_str(&dump_all(trie));

    // Step 1: delete a leaf whose parent has exactly two children. After the
    // leaf is physically freed by the optimize sweep, the parent has a single
    // child left, which triggers `__trieNode_MergeWithSingleChild`. The merge
    // is invisible in `Trie_IterateAll` output (we only see terminals), so the
    // observable change here is `"ape"` disappearing and `Trie_Size` dropping
    // by 1. A Rust port that skips the merge step still passes this snapshot;
    // a port that fails to remove the leaf entry will not.
    {
        // SAFETY: `trie` is live; "ape" bytes valid.
        let r = unsafe { delete(trie, "ape") };
        writeln!(&mut out, "\n--- delete leaf with sibling \"ape\" — physical removal + parent single-child merge ---").unwrap();
        writeln!(&mut out, "Trie_Delete(\"ape\") -> {r}").unwrap();
        out.push_str(&dump_all(trie));
    }

    // Step 2: delete an internal-terminal node (`"appl"` has children
    // `"apple"`, `"apply"`). The C trie only flips the TRIENODE_TERMINAL bit
    // off and sets TRIENODE_DELETED — the node itself stays so its children
    // remain reachable. Observable: `"appl"` drops out, children survive.
    {
        // SAFETY: `trie` is live.
        let r = unsafe { delete(trie, "appl") };
        writeln!(&mut out, "\n--- delete internal-terminal \"appl\" — children survive (mark-deleted, not physical) ---").unwrap();
        writeln!(&mut out, "Trie_Delete(\"appl\") -> {r}").unwrap();
        out.push_str(&dump_all(trie));
    }

    // Step 3: delete a key that does not exist anywhere in the trie.
    {
        // SAFETY: `trie` is live.
        let r = unsafe { delete(trie, "zzz") };
        writeln!(
            &mut out,
            "\n--- delete non-existent \"zzz\" — returns 0, size unchanged ---"
        )
        .unwrap();
        writeln!(&mut out, "Trie_Delete(\"zzz\") -> {r}").unwrap();
        out.push_str(&dump_all(trie));
    }

    // Step 4: delete the same internal-terminal again. `TrieNode_Delete`
    // checks `__trieNode_isDeleted(n)` and short-circuits to rc=0 if so, so a
    // second delete on an already-deleted node is a no-op. A Rust port that
    // forgets the deleted-flag guard would return 1 here.
    {
        // SAFETY: `trie` is live.
        let r = unsafe { delete(trie, "appl") };
        writeln!(
            &mut out,
            "\n--- delete already-deleted \"appl\" — second delete is a no-op ---"
        )
        .unwrap();
        writeln!(&mut out, "Trie_Delete(\"appl\") -> {r}").unwrap();
        out.push_str(&dump_all(trie));
    }

    // Step 5: re-insert the deleted key. `TrieNode_Add` clears
    // TRIENODE_DELETED and re-sets TRIENODE_TERMINAL on the existing slot
    // (line ~339 of trie_node.c). Note `numDocs` *accumulates* via `+=` even
    // in ADD_REPLACE mode (the deleted node's numDocs was zeroed at delete
    // time, so the result is just the new value). A port that resurrects with
    // a brand-new node and copies-out the previous numDocs would diverge.
    {
        // SAFETY: `trie` is live.
        unsafe { insert(trie, "appl", 9.0, 10) };
        writeln!(
            &mut out,
            "\n--- re-insert \"appl\" with new score/numDocs — un-deletes the existing slot ---"
        )
        .unwrap();
        writeln!(
            &mut out,
            "Trie_InsertStringBuffer(\"appl\", score=9, numDocs=10, ADD_REPLACE)"
        )
        .unwrap();
        out.push_str(&dump_all(trie));
    }

    // SAFETY: `trie` was created by `NewTrie` above; never freed elsewhere.
    unsafe { TrieType_Free(trie as *mut c_void) };

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_delete_sequence_structural_events", out); }
    );
}

#[test]
fn lex_decrement_numdocs_return_codes() {
    // SAFETY: `NewTrie` with a NULL free-callback matches the terms-trie
    // construction in `src/spec.c`.
    let trie = unsafe { NewTrie(None, TrieSortMode_Trie_Sort_Lex) };
    // SAFETY: `trie` is live.
    unsafe { build_base(trie) };

    let mut out = String::new();
    writeln!(&mut out, "=== base ===").unwrap();
    out.push_str(&dump_all(trie));

    // Each step: (description, term, delta).
    // The note is appended to the result line as documentation of the path the
    // C trie takes — the snapshot will fail if the code stops matching it.
    let steps: &[(&str, &str, usize, &str)] = &[
        (
            "decrement non-existent \"zzz\"",
            "zzz",
            1,
            "TrieNode_Get returns NULL",
        ),
        (
            "decrement \"ban\" — exact match on internal non-terminal split node",
            "ban",
            1,
            "Trie_DecrementNumDocs rejects non-terminals (not a real term)",
        ),
        (
            "decrement \"apple\" by 1 (numDocs 3 -> 2)",
            "apple",
            1,
            "delta < numDocs",
        ),
        (
            "decrement \"apple\" by 2 (numDocs 2 -> 0, triggers delete)",
            "apple",
            2,
            "delta == numDocs",
        ),
        (
            "decrement \"apply\" by 100 (delta clamped to numDocs, triggers delete)",
            "apply",
            100,
            "delta > numDocs — clamped to 0, not underflow",
        ),
        (
            "decrement \"apple\" again — node was just deleted",
            "apple",
            1,
            "TrieNode_Get skips deleted nodes (returns NULL)",
        ),
    ];

    for (label, term, delta, note) in steps {
        // SAFETY: `trie` is live; term bytes valid for the call.
        let r = unsafe { decrement(trie, term, *delta) };
        writeln!(&mut out, "\n--- {label} ---").unwrap();
        writeln!(
            &mut out,
            "Trie_DecrementNumDocs(\"{term}\", delta={delta}) -> {} ({note})",
            decr_name(r)
        )
        .unwrap();
        out.push_str(&dump_all(trie));
    }

    // SAFETY: `trie` was created by `NewTrie` above.
    unsafe { TrieType_Free(trie as *mut c_void) };

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_decrement_numdocs_return_codes", out); }
    );
}
