/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Assert `RuneTrieMap` reproduces the C trie's Lex-mode delete and
//! decrement-numDocs behavior against the snapshots owned by
//! `rune_trie_snapshots::delete_and_decrement`.
//!
//! See that crate's docs for the structural diagram of the shared base
//! fixture and the per-op rationale.
//!
//! # Expected API surface (not all present yet — see compile errors)
//!
//! This test calls the API as the port plans to expose it once delete /
//! decrement support lands. Items the compiler will currently flag:
//!
//! - `trie_rs::rune::TermEntry` — a `{score: f32, num_docs: usize}` value
//!   type with public fields. The terms-trie payload, hoisted out of this
//!   test so a `RuneTrieMap<TermEntry>`-specific impl can attach methods
//!   to it (next item). Could equivalently be a newtype wrapper around
//!   the trie if a constrained `impl` block is preferred.
//!
//! - `trie_rs::rune::DecrResult` — three-arm enum mirroring `TrieDecrResult`
//!   in `src/trie/trie.h`: `NotFound`, `Updated`, `Deleted`.
//!
//! - `RuneTrieMap::<Data>::remove(&mut self, key: &[Rune]) -> Option<Data>` —
//!   returns the removed value (or `None` if absent). Two things the current
//!   `remove(&mut self, key)` -> () signature doesn't give us:
//!   1. Found/not-found signal needed to print `Trie_Delete -> 1/0`.
//!   2. The removed `num_docs` value, which the delete-then-reinsert step
//!      adds back into the next insert to mirror the C trie's "deleted
//!      slot keeps numDocs" quirk (4 + 10 = 14, not 10).
//!
//! - `impl RuneTrieMap<TermEntry> { fn decrement_num_docs(&mut self, key,
//!   delta) -> DecrResult }` — encapsulates the C
//!   `Trie_DecrementNumDocs` policy (saturating subtract, delete on zero,
//!   reject keys that don't resolve to a terminal). Lives on the
//!   `TermEntry`-specialized impl because the trie itself is value-agnostic.

use std::fmt::Write as _;

use trie_rs::rune::{DecrResult, Rune, RuneTrieMap};

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
            "  {term:10}  score={score}  numDocs={num_docs}",
            score = entry.score,
            num_docs = entry.num_docs,
        )
        .unwrap();
    }
    out
}

/// Render a `DecrResult` as the same identifier used in `trie.h`, so the
/// snapshot reads against the C source without needing a value table on hand.
const fn decr_name(r: DecrResult) -> &'static str {
    match r {
        DecrResult::NotFound => "NOT_FOUND",
        DecrResult::Updated => "UPDATED",
        DecrResult::Deleted => "DELETED",
    }
}

/// Same 7-term fixture as `rune_trie_snapshots::delete_and_decrement::build_base`.
fn build_base(trie: &mut RuneTrieMap<TermEntry>) {
    let terms: &[(&str, f32, usize)] = &[
        ("apple", 1.0, 3),
        ("apply", 1.0, 2),
        ("appl", 1.0, 4),
        ("ape", 1.0, 1),
        ("b", 1.0, 5),
        ("banana", 1.0, 2),
        ("band", 1.0, 1),
    ];
    for (term, score, num_docs) in terms {
        trie.insert(
            &term_runes(term),
            TermEntry {
                score: *score,
                num_docs: *num_docs,
            },
        );
    }
}

#[test]
fn lex_delete_sequence_structural_events() {
    let mut trie = RuneTrieMap::<TermEntry>::new();
    build_base(&mut trie);

    let mut out = String::new();
    writeln!(&mut out, "=== base ===").unwrap();
    out.push_str(&dump_all(&trie));

    // Step 1: leaf delete. Rust just physically removes — no cascade-merge to
    // observe in iteration output (the C optimize-sweep merge is invisible in
    // `Trie_IterateAll` anyway).
    {
        let r = i32::from(trie.remove(&term_runes("ape")).is_some());
        writeln!(&mut out, "\n--- delete leaf with sibling \"ape\" — physical removal + parent single-child merge ---").unwrap();
        writeln!(&mut out, "Trie_Delete(\"ape\") -> {r}").unwrap();
        out.push_str(&dump_all(&trie));
    }

    // Step 2: internal-terminal delete. Capture the removed `num_docs` so
    // step 5 can mirror the C trie's "deleted slot keeps numDocs" quirk on
    // re-insert. Rust has no deleted-slot concept; the policy moves to the
    // call site (this test).
    let appl_preserved_num_docs;
    {
        let removed = trie.remove(&term_runes("appl"));
        appl_preserved_num_docs = removed.as_ref().map(|e| e.num_docs).unwrap_or(0);
        let r = i32::from(removed.is_some());
        writeln!(&mut out, "\n--- delete internal-terminal \"appl\" — children survive (mark-deleted, not physical) ---").unwrap();
        writeln!(&mut out, "Trie_Delete(\"appl\") -> {r}").unwrap();
        out.push_str(&dump_all(&trie));
    }

    // Step 3: missing key.
    {
        let r = i32::from(trie.remove(&term_runes("zzz")).is_some());
        writeln!(
            &mut out,
            "\n--- delete non-existent \"zzz\" — returns 0, size unchanged ---"
        )
        .unwrap();
        writeln!(&mut out, "Trie_Delete(\"zzz\") -> {r}").unwrap();
        out.push_str(&dump_all(&trie));
    }

    // Step 4: re-delete the already-removed key. Falls out naturally because
    // the previous `remove` physically dropped the entry.
    {
        let r = i32::from(trie.remove(&term_runes("appl")).is_some());
        writeln!(
            &mut out,
            "\n--- delete already-deleted \"appl\" — second delete is a no-op ---"
        )
        .unwrap();
        writeln!(&mut out, "Trie_Delete(\"appl\") -> {r}").unwrap();
        out.push_str(&dump_all(&trie));
    }

    // Step 5: re-insert. Mirror C's `n->numDocs += numDocs` over the deleted
    // node's preserved-numDocs (=4) plus the new numDocs (=10) -> 14.
    {
        let key = term_runes("appl");
        trie.insert(
            &key,
            TermEntry {
                score: 9.0,
                num_docs: appl_preserved_num_docs + 10,
            },
        );
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
        out.push_str(&dump_all(&trie));
    }

    insta::with_settings!(
        {
            snapshot_path => "../../../rune_trie_snapshots/tests/integration/snapshots",
            prepend_module_to_snapshot => false,
        },
        { insta::assert_snapshot!("lex_delete_sequence_structural_events", out); }
    );
}

#[test]
fn lex_decrement_numdocs_return_codes() {
    let mut trie = RuneTrieMap::<TermEntry>::new();
    build_base(&mut trie);

    let mut out = String::new();
    writeln!(&mut out, "=== base ===").unwrap();
    out.push_str(&dump_all(&trie));

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
        let r = trie.decrement_num_docs(&term_runes(term), *delta);
        writeln!(&mut out, "\n--- {label} ---").unwrap();
        writeln!(
            &mut out,
            "Trie_DecrementNumDocs(\"{term}\", delta={delta}) -> {} ({note})",
            decr_name(r)
        )
        .unwrap();
        out.push_str(&dump_all(&trie));
    }

    insta::with_settings!(
        {
            snapshot_path => "../../../rune_trie_snapshots/tests/integration/snapshots",
            prepend_module_to_snapshot => false,
        },
        { insta::assert_snapshot!("lex_decrement_numdocs_return_codes", out); }
    );
}
