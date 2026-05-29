/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Assert `TermDictionary` reproduces the C trie's `ADD_INCR` insert-mode
//! behavior against the snapshots owned by `rune_trie_snapshots::incr_mode`.
//!
//! The C trie's `ADD_INCR` path lives inside `__trieNode_Add`
//! (`src/trie/trie_node.c:282-310`); see the C-side test's docstring for the
//! per-field arithmetic the snapshots pin. The scenarios in this file
//! exercise [`TermDictionary::add_term`] / [`TermDictionary::replace_term`]
//! directly. The C oracle's payload-bearing scenario is intentionally not
//! mirrored here: `sp->terms` never carries a payload, so the Rust port
//! has no consumer for that behavior (the future `SuggestionDictionary`
//! that would is out of scope for this port).
//!
//! # Mapping per scenario
//!
//! - **score/numDocs accumulation** — `add_term` and `replace_term`
//!   encapsulate the per-mode arithmetic; the [`InsertOutcome`] return
//!   distinguishes new terminals (rc=OK_NEW) from updated ones
//!   (rc=OK_UPDATED).
//!
//! - **INCR over deleted node** — Rust has no mark-deleted slot; `remove`
//!   physically drops the entry, and the subsequent `add_term` is
//!   from-fresh. The observable score=5/numDocs=10 (vs C's
//!   preserved-then-zeroed slot) matches because in both cases the
//!   addition baseline is 0.
//!
//! - **INCR over non-terminal split** — Rust has no non-terminal nodes.
//!   Inserting "apple" + "apply" gives a 2-entry map; inserting "appl"
//!   adds a third entry. The C trie's invisible non-terminal "appl" is a
//!   non-issue here because `Trie_IterateAll` skips non-terminals too.
//!
//! - **Mixed INCR + REPLACE** — alternating `add_term` / `replace_term`
//!   on the same key. Pins that REPLACE overwrites score while numDocs
//!   always accumulates.

use std::fmt::Write as _;

use trie_rs::str::term_dict::{InsertOutcome, TermDictionary};

/// Mirror C `TRIE_OK_*` rendering (`trie_node.h:29-32`).
const fn rc_name(rc: i32) -> &'static str {
    match rc {
        0 => "OK_UPDATED",
        1 => "OK_NEW",
        -1 => "ERR_PAYLOAD_OVERFLOW",
        _ => "UNKNOWN",
    }
}

/// Bridge [`InsertOutcome`] back to the C `TRIE_OK_*` integer rc so the
/// `TermDictionary`-backed scenarios feed the same [`rc_name`] formatter
/// the C-side snapshots were captured against.
const fn outcome_rc(o: InsertOutcome) -> i32 {
    match o {
        InsertOutcome::New => 1,
        InsertOutcome::Updated => 0,
    }
}

/// Field widths chosen to byte-match the C-side `dump_all` so the shared
/// snapshot aligns column-for-column.
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

const ADD_INCR: i32 = 1;
const ADD_REPLACE: i32 = 0;

const fn mode_name(incr: i32) -> &'static str {
    if incr == ADD_INCR { "INCR" } else { "REPLACE" }
}

#[test]
fn lex_incr_score_and_numdocs_accumulation() {
    let mut trie = TermDictionary::new();

    // Parallel pair — same triples on "foo" with INCR and "bar" with REPLACE.
    // INCR: score accumulates 1.0 + 0.5 + 2.0 = 3.5.
    // REPLACE: last score wins -> 2.0.
    // Both: numDocs accumulates 1 + 1 + 1 = 3 (mode-independent per
    // trie_node.c:296).
    let steps: &[(&str, i32, f32, usize, &str)] = &[
        (
            "foo",
            ADD_INCR,
            1.0,
            1,
            "first insert — creates leaf (rc=OK_NEW)",
        ),
        (
            "foo",
            ADD_INCR,
            0.5,
            1,
            "score += 0.5 -> 1.5; numDocs += 1 -> 2",
        ),
        (
            "foo",
            ADD_INCR,
            2.0,
            1,
            "score += 2.0 -> 3.5; numDocs += 1 -> 3",
        ),
        (
            "bar",
            ADD_REPLACE,
            1.0,
            1,
            "first insert — creates leaf (rc=OK_NEW)",
        ),
        (
            "bar",
            ADD_REPLACE,
            0.5,
            1,
            "score = 0.5 (overwrite); numDocs += 1 -> 2",
        ),
        (
            "bar",
            ADD_REPLACE,
            2.0,
            1,
            "score = 2.0 (overwrite); numDocs += 1 -> 3",
        ),
    ];

    let mut out = String::new();
    for (term, incr, score, num_docs, note) in steps {
        let rc = outcome_rc(if *incr == ADD_INCR {
            trie.add_term(term, *score, *num_docs)
        } else {
            trie.replace_term(term, *score, *num_docs)
        });
        writeln!(
            &mut out,
            "--- insert({term:?}, mode={}, score={score}, numDocs={num_docs}) -> {} — {note} ---",
            mode_name(*incr),
            rc_name(rc),
        )
        .unwrap();
        out.push_str(&dump_all(&trie));
    }

    insta::with_settings!(
        {
            snapshot_path => "../../../../rune_trie_snapshots/tests/integration/snapshots",
            prepend_module_to_snapshot => false,
        },
        { insta::assert_snapshot!("lex_incr_score_and_numdocs_accumulation", out); }
    );
}

#[test]
fn lex_incr_over_deleted_node() {
    let mut trie = TermDictionary::new();

    let mut out = String::new();

    // Step 1: build an internal-terminal "foo" in C terms — i.e. "foo" plus a
    // child "foobar". In Rust there are no internal terminals (every key is
    // independent), but the snapshot only cares about the iterator output
    // (which already skips non-terminals on the C side), so the two views
    // agree.
    writeln!(
        &mut out,
        "=== step 1: build internal-terminal \"foo\" via \"foo\" + \"foobar\" ==="
    )
    .unwrap();
    let rc = outcome_rc(trie.replace_term("foo", 1.0, 2));
    writeln!(
        &mut out,
        "insert(\"foo\", REPLACE, score=1, numDocs=2) -> {}",
        rc_name(rc)
    )
    .unwrap();
    let rc = outcome_rc(trie.replace_term("foobar", 1.0, 1));
    writeln!(
        &mut out,
        "insert(\"foobar\", REPLACE, score=1, numDocs=1) -> {}",
        rc_name(rc)
    )
    .unwrap();
    out.push_str(&dump_all(&trie));

    // Step 2: delete "foo". In Rust this is a physical removal. The C trie
    // mark-deletes (keeps the node, zeroes score/numDocs); the observable
    // iterator output is the same — "foo" disappears, "foobar" survives.
    writeln!(
        &mut out,
        "\n=== step 2: Trie_Delete(\"foo\") — mark-delete internal terminal ==="
    )
    .unwrap();
    let r = i32::from(trie.remove("foo").is_some());
    writeln!(&mut out, "Trie_Delete(\"foo\") -> {r}").unwrap();
    out.push_str(&dump_all(&trie));

    // Step 3: INCR-resurrect. In C, the deleted slot has score=0/numDocs=0
    // (TrieNode_Delete zeroes both since becd799e9f), so INCR(5, 10) gives
    // 5/10. In Rust, the slot was physically removed at step 2, so INCR is
    // a from-fresh insert — also 5/10. rc=OK_NEW in both because the key
    // wasn't a live terminal pre-call.
    writeln!(
        &mut out,
        "\n=== step 3: INCR over deleted node — score/numDocs reset to new values ==="
    )
    .unwrap();
    let rc = outcome_rc(trie.add_term("foo", 5.0, 10));
    writeln!(
        &mut out,
        "insert(\"foo\", INCR, score=5, numDocs=10) -> {}",
        rc_name(rc)
    )
    .unwrap();
    out.push_str(&dump_all(&trie));

    insta::with_settings!(
        {
            snapshot_path => "../../../../rune_trie_snapshots/tests/integration/snapshots",
            prepend_module_to_snapshot => false,
        },
        { insta::assert_snapshot!("lex_incr_over_deleted_node", out); }
    );
}

#[test]
fn lex_incr_over_non_terminal_split() {
    let mut trie = TermDictionary::new();

    let mut out = String::new();

    // Step 1: C side creates a non-terminal "appl" internally via
    // __trie_SplitNode. The non-terminal is invisible to Trie_IterateAll.
    // Rust has no non-terminals, but the iterator output is identical: just
    // apple + apply.
    writeln!(
        &mut out,
        "=== step 1: \"apple\" + \"apply\" — creates non-terminal \"appl\" ==="
    )
    .unwrap();
    let rc = outcome_rc(trie.replace_term("apple", 1.0, 1));
    writeln!(
        &mut out,
        "insert(\"apple\", REPLACE, score=1, numDocs=1) -> {}",
        rc_name(rc)
    )
    .unwrap();
    let rc = outcome_rc(trie.replace_term("apply", 2.0, 1));
    writeln!(
        &mut out,
        "insert(\"apply\", REPLACE, score=2, numDocs=1) -> {}",
        rc_name(rc)
    )
    .unwrap();
    out.push_str(&dump_all(&trie));

    // Step 2: INCR("appl", 7, 3). C side: the existing non-terminal becomes
    // terminal, score = 0 + 7, numDocs = 0 + 3 (the split-parent's
    // score/numDocs were zeroed by __trie_SplitNode). Rust side: brand-new
    // key, so score=7, numDocs=3 directly. rc=OK_NEW in both.
    writeln!(
        &mut out,
        "\n=== step 2: INCR-insert \"appl\" — non-terminal becomes terminal ==="
    )
    .unwrap();
    let rc = outcome_rc(trie.add_term("appl", 7.0, 3));
    writeln!(
        &mut out,
        "insert(\"appl\", INCR, score=7, numDocs=3) -> {}",
        rc_name(rc)
    )
    .unwrap();
    out.push_str(&dump_all(&trie));

    insta::with_settings!(
        {
            snapshot_path => "../../../../rune_trie_snapshots/tests/integration/snapshots",
            prepend_module_to_snapshot => false,
        },
        { insta::assert_snapshot!("lex_incr_over_non_terminal_split", out); }
    );
}

#[test]
fn lex_incr_mixed_with_replace() {
    let mut trie = TermDictionary::new();

    let mut out = String::new();

    // INCR(1, 1) → new leaf, score=1, numDocs=1, rc=OK_NEW.
    // REPLACE(10, 5) → score=10 (overwrite), numDocs += 5 = 6, rc=OK_UPDATED.
    // INCR(2, 1) → score += 2 = 12, numDocs += 1 = 7, rc=OK_UPDATED.
    let steps: &[(i32, f32, usize, &str)] = &[
        (ADD_INCR, 1.0, 1, "first insert — creates leaf"),
        (
            ADD_REPLACE,
            10.0,
            5,
            "score overwritten to 10; numDocs += 5 -> 6 (always additive)",
        ),
        (ADD_INCR, 2.0, 1, "score += 2 -> 12; numDocs += 1 -> 7"),
    ];

    for (incr, score, num_docs, note) in steps {
        let rc = outcome_rc(if *incr == ADD_INCR {
            trie.add_term("foo", *score, *num_docs)
        } else {
            trie.replace_term("foo", *score, *num_docs)
        });
        writeln!(
            &mut out,
            "--- insert(\"foo\", mode={}, score={score}, numDocs={num_docs}) -> {} — {note} ---",
            mode_name(*incr),
            rc_name(rc),
        )
        .unwrap();
        out.push_str(&dump_all(&trie));
    }

    insta::with_settings!(
        {
            snapshot_path => "../../../../rune_trie_snapshots/tests/integration/snapshots",
            prepend_module_to_snapshot => false,
        },
        { insta::assert_snapshot!("lex_incr_mixed_with_replace", out); }
    );
}
