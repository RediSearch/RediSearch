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
//! # Why this test mirrors C semantics at the call site
//!
//! `RuneTrieMap` is a value-agnostic radix map — every present key is a
//! terminal, deletions are physical, and the trie owns no notion of
//! `numDocs`, `score`, deletion flags, or the C "Trie_DecrementNumDocs"
//! result codes. The terms-trie merge policy (numDocs accumulates across
//! REPLACE-mode inserts, numDocs survives `Trie_Delete` and re-emerges on
//! re-insert, etc.) lives in the C trie because that's the only consumer
//! that needs it. A Rust port plans to keep the policy at the call site
//! the same way `rune_splits.rs` already does for `ADD_REPLACE`.
//!
//! Where the C trie does it implicitly, this test does it explicitly:
//!
//! - **`Trie_Delete -> 1/0`** — Rust's `remove` returns `()`. Mirror via a
//!   `get` probe before `remove`.
//!
//! - **Re-insert after delete preserves numDocs (4 + 10 = 14, not 10)** —
//!   The C trie keeps the deleted node's `numDocs` field intact (only score
//!   and flags are touched in `TrieNode_Delete`), so `n->numDocs += numDocs`
//!   on re-insert accumulates onto the stale value. We mirror by capturing
//!   the numDocs *before* removing and adding it back on the next insert.
//!
//! - **`Trie_DecrementNumDocs` return codes** — Implemented inline in
//!   [`decrement`] below as `get` + `insert_with`/`remove`. The
//!   "non-terminal exact match returns NOT_FOUND" case collapses naturally
//!   because `RuneTrieMap` has no notion of an internal-only exact match —
//!   `get("ban")` returns `None` when "ban" was never inserted, which is
//!   observably the same as the C trie's two-step `GetNode` + `IsTerminal`
//!   rejection.
//!
//! When the Rust port grows a dedicated `decrement_num_docs` API (likely
//! also a `numDocs`-aware `TermEntry` newtype to make the merge policy
//! reusable across call sites), this test can be retargeted at that API
//! without changing the snapshot.

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
            "  {term:10}  score={score}  numDocs={num_docs}",
            score = entry.score,
            num_docs = entry.num_docs,
        )
        .unwrap();
    }
    out
}

/// Same 7-term fixture as `rune_trie_snapshots::delete_and_decrement::build_base`.
/// Uses plain `insert` because every key is brand new on its insert step
/// (no merge needed for the base build).
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

/// Mirror `Trie_Delete`: returns 1 if the key was present, 0 otherwise.
/// Rust's `RuneTrieMap::remove` returns `()`, so we probe with `get` first.
fn delete(trie: &mut RuneTrieMap<TermEntry>, key: &[Rune]) -> i32 {
    if trie.get(key).is_some() {
        trie.remove(key);
        1
    } else {
        0
    }
}

/// The three-way return type matching `TrieDecrResult` from `trie.h`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecrResult {
    NotFound,
    Updated,
    Deleted,
}

impl DecrResult {
    const fn name(self) -> &'static str {
        match self {
            Self::NotFound => "NOT_FOUND",
            Self::Updated => "UPDATED",
            Self::Deleted => "DELETED",
        }
    }
}

/// Mirror `Trie_DecrementNumDocs` (`src/trie/trie.c`):
///   - key absent (or only matches an internal split node in C) -> NOT_FOUND
///   - `delta` clamps to `num_docs` (no underflow)
///   - resulting `num_docs == 0` -> remove, return DELETED
///   - else -> rewrite the entry with decremented num_docs, return UPDATED
fn decrement(trie: &mut RuneTrieMap<TermEntry>, key: &[Rune], delta: usize) -> DecrResult {
    let Some(entry) = trie.get(key) else {
        return DecrResult::NotFound;
    };
    let new_num_docs = entry.num_docs.saturating_sub(delta);
    if new_num_docs == 0 {
        trie.remove(key);
        DecrResult::Deleted
    } else {
        trie.insert_with(key, |prev| {
            // The closure runs *after* the get borrow above has been released,
            // so we look up the previous score from `prev` (which must be Some
            // because we just verified the key exists and never removed it).
            let prev = prev.expect("key existed at get() above and was not removed");
            TermEntry {
                score: prev.score,
                num_docs: new_num_docs,
            }
        });
        DecrResult::Updated
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
        let r = delete(&mut trie, &term_runes("ape"));
        writeln!(&mut out, "\n--- delete leaf with sibling \"ape\" — physical removal + parent single-child merge ---").unwrap();
        writeln!(&mut out, "Trie_Delete(\"ape\") -> {r}").unwrap();
        out.push_str(&dump_all(&trie));
    }

    // Step 2: internal-terminal delete. We capture `appl`'s numDocs before
    // removal so step 5 can mirror the C trie's "deleted node keeps numDocs"
    // quirk on re-insert. Rust has no deleted-slot concept; the policy moves
    // to the call site (this test).
    let appl_preserved_num_docs;
    {
        let key = term_runes("appl");
        appl_preserved_num_docs = trie
            .get(&key)
            .map(|e| e.num_docs)
            .expect("appl was just inserted by build_base");
        let r = delete(&mut trie, &key);
        writeln!(&mut out, "\n--- delete internal-terminal \"appl\" — children survive (mark-deleted, not physical) ---").unwrap();
        writeln!(&mut out, "Trie_Delete(\"appl\") -> {r}").unwrap();
        out.push_str(&dump_all(&trie));
    }

    // Step 3: missing key.
    {
        let r = delete(&mut trie, &term_runes("zzz"));
        writeln!(&mut out, "\n--- delete non-existent \"zzz\" — returns 0, size unchanged ---")
            .unwrap();
        writeln!(&mut out, "Trie_Delete(\"zzz\") -> {r}").unwrap();
        out.push_str(&dump_all(&trie));
    }

    // Step 4: re-delete the already-removed key. Falls out naturally because
    // `get` returns `None` after step 2's `remove`.
    {
        let r = delete(&mut trie, &term_runes("appl"));
        writeln!(&mut out, "\n--- delete already-deleted \"appl\" — second delete is a no-op ---")
            .unwrap();
        writeln!(&mut out, "Trie_Delete(\"appl\") -> {r}").unwrap();
        out.push_str(&dump_all(&trie));
    }

    // Step 5: re-insert. Mirror C's `n->numDocs += numDocs` over the deleted
    // node's preserved-numDocs (=4) plus the new numDocs (=10) -> 14.
    {
        let key = term_runes("appl");
        trie.insert_with(&key, |_prev| TermEntry {
            score: 9.0,
            num_docs: appl_preserved_num_docs + 10,
        });
        writeln!(&mut out, "\n--- re-insert \"appl\" with new score/numDocs — un-deletes the existing slot ---").unwrap();
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
        let r = decrement(&mut trie, &term_runes(term), *delta);
        writeln!(&mut out, "\n--- {label} ---").unwrap();
        writeln!(
            &mut out,
            "Trie_DecrementNumDocs(\"{term}\", delta={delta}) -> {} ({note})",
            r.name()
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
