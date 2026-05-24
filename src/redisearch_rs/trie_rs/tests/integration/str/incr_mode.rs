/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Assert `StrTrieMap` reproduces the C trie's `ADD_INCR` insert-mode
//! behavior against the snapshots owned by `rune_trie_snapshots::incr_mode`.
//!
//! The C trie's `ADD_INCR` path lives inside `__trieNode_Add`
//! (`src/trie/trie_node.c:282-310`); see the C-side test's docstring for the
//! per-field arithmetic the snapshots pin. This file maps each scenario onto
//! the Rust port's primitives.
//!
//! # How the Rust port models score/numDocs/payload
//!
//! `StrTrieMap` is value-agnostic — score, numDocs and payload bytes are
//! NOT stored on the node. Instead, this test (and others in the crate)
//! defines a local `TermEntry` and instantiates `StrTrieMap<TermEntry>`.
//! The terms-trie production path will hoist this struct out and impl
//! `Trie_InsertStringBuffer`-style entry points on a constrained
//! `StrTrieMap<TermEntry>` (or a newtype around it); the mode-aware
//! score/numDocs arithmetic lives ON THE CALLER, not inside the trie.
//!
//! That's why `insert_incr` / `insert_replace` below do
//! `remove → modify → insert`: the C trie's `n->score += score` happens
//! atomically inside `__trieNode_Add`; the Rust port externalises it. The
//! observable result is identical, which is what the shared snapshot pins.
//!
//! # Mapping per scenario
//!
//! - **score/numDocs accumulation** — `insert_incr` / `insert_replace` do
//!   the per-mode arithmetic at the call site. `Option<V>` from `remove`
//!   distinguishes "no prior value" (rc=OK_NEW) from "existing terminal"
//!   (rc=OK_UPDATED).
//!
//! - **INCR over deleted node** — Rust has no mark-deleted slot; `remove`
//!   physically drops the entry, and the subsequent `insert` is from-fresh.
//!   The observable score=5/numDocs=10 (vs C's preserved-then-zeroed slot)
//!   matches because in both cases the addition baseline is 0.
//!
//! - **INCR over non-terminal split** — Rust has no non-terminal nodes.
//!   Inserting "apple" + "apply" gives a 2-entry map; inserting "appl"
//!   adds a third entry. The C trie's invisible non-terminal "appl" is a
//!   non-issue here because `Trie_IterateAll` skips non-terminals too.
//!
//! - **INCR with payload** — `LoggingPayload`'s `Drop` records the freecb
//!   moment. The C trie's payload-replace runs `triePayload_Free` on the
//!   old bytes inside `__trieNode_Add` regardless of `op`; the Rust mirror
//!   replaces the entire `TermEntry`, so the old payload drops at the end
//!   of the `match` arm — before the next `insert` and before the log is
//!   drained for the snapshot.
//!
//! - **Mixed INCR + REPLACE** — alternating calls to `insert_incr` /
//!   `insert_replace` on the same key. Pins that REPLACE overwrites score
//!   while numDocs always accumulates.
//!
//! # Expected API surface
//!
//! - `StrTrieMap::<V>::insert(&mut self, key, value) -> Option<V>` —
//!   needed so the test can tell NEW from UPDATED on payload-less
//!   scenarios that don't go through the `remove → modify → insert`
//!   round-trip.
//! - `StrTrieMap::<V>::remove(&mut self, key) -> Option<V>` — same;
//!   plus the returned `Some(prev)` drops at the call site so
//!   `LoggingPayload::drop` records the freecb moment in payload scenarios.

use std::cell::RefCell;
use std::fmt::Write as _;

use trie_rs::str::StrTrieMap;

thread_local! {
    /// Rust analog of the C-side `FREECB_LOG`. Each `LoggingPayload::drop`
    /// pushes its label here; the test drains the log between steps and
    /// renders it into the snapshot trace.
    static FREECB_LOG: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

struct TermEntry {
    score: f32,
    num_docs: usize,
}

/// Payload-carrying variant of `TermEntry`, used only by the payload
/// scenario. `LoggingPayload::drop` records when the C trie's freecb
/// would have fired.
struct TermEntryWithPayload {
    score: f32,
    num_docs: usize,
    payload: LoggingPayload,
}

struct LoggingPayload {
    label: String,
}

impl Drop for LoggingPayload {
    fn drop(&mut self) {
        FREECB_LOG.with(|log| log.borrow_mut().push(self.label.clone()));
    }
}

/// Mirror C `TRIE_OK_*` rendering (`trie_node.h:29-32`).
const fn rc_name(rc: i32) -> &'static str {
    match rc {
        0 => "OK_UPDATED",
        1 => "OK_NEW",
        -1 => "ERR_PAYLOAD_OVERFLOW",
        _ => "UNKNOWN",
    }
}

/// ADD_INCR emulation: `n->score += score`, `n->numDocs += numDocs`.
/// Returns the C rc (OK_NEW=1 if the key wasn't present, OK_UPDATED=0
/// otherwise).
fn insert_incr(
    trie: &mut StrTrieMap<TermEntry>,
    term: &str,
    score: f32,
    num_docs: usize,
) -> i32 {
    let prev = trie.remove(term);
    let was_present = prev.is_some();
    let new_entry = match prev {
        Some(p) => TermEntry {
            score: p.score + score,
            num_docs: p.num_docs + num_docs,
        },
        None => TermEntry { score, num_docs },
    };
    trie.insert(term, new_entry);
    if was_present { 0 } else { 1 }
}

/// ADD_REPLACE emulation: `n->score = score` (overwrite), `n->numDocs +=
/// numDocs` (accumulates regardless of mode — verified against
/// `trie_node.c:296`).
fn insert_replace(
    trie: &mut StrTrieMap<TermEntry>,
    term: &str,
    score: f32,
    num_docs: usize,
) -> i32 {
    let prev = trie.remove(term);
    let was_present = prev.is_some();
    let new_entry = match prev {
        Some(p) => TermEntry {
            score,
            num_docs: p.num_docs + num_docs,
        },
        None => TermEntry { score, num_docs },
    };
    trie.insert(term, new_entry);
    if was_present { 0 } else { 1 }
}

/// As `insert_incr` but carries a payload. The old `LoggingPayload` drops
/// at the end of the `Some` arm — before `trie.insert` runs — mirroring
/// the C trie's "free old payload, install new one" order inside
/// `__trieNode_Add`.
fn insert_incr_with_payload(
    trie: &mut StrTrieMap<TermEntryWithPayload>,
    term: &str,
    score: f32,
    num_docs: usize,
    label: &str,
) -> i32 {
    let prev = trie.remove(term);
    let was_present = prev.is_some();
    let new_entry = match prev {
        Some(p) => TermEntryWithPayload {
            score: p.score + score,
            num_docs: p.num_docs + num_docs,
            payload: LoggingPayload {
                label: label.to_string(),
            },
        },
        None => TermEntryWithPayload {
            score,
            num_docs,
            payload: LoggingPayload {
                label: label.to_string(),
            },
        },
    };
    trie.insert(term, new_entry);
    if was_present { 0 } else { 1 }
}

/// ADD_REPLACE with payload — same as `insert_incr_with_payload` but
/// score is overwritten rather than added.
fn insert_replace_with_payload(
    trie: &mut StrTrieMap<TermEntryWithPayload>,
    term: &str,
    score: f32,
    num_docs: usize,
    label: &str,
) -> i32 {
    let prev = trie.remove(term);
    let was_present = prev.is_some();
    let new_entry = match prev {
        Some(p) => TermEntryWithPayload {
            score,
            num_docs: p.num_docs + num_docs,
            payload: LoggingPayload {
                label: label.to_string(),
            },
        },
        None => TermEntryWithPayload {
            score,
            num_docs,
            payload: LoggingPayload {
                label: label.to_string(),
            },
        },
    };
    trie.insert(term, new_entry);
    if was_present { 0 } else { 1 }
}

/// Field widths chosen to byte-match the C-side `dump_all` so the shared
/// snapshot aligns column-for-column.
fn dump_all(trie: &StrTrieMap<TermEntry>) -> String {
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

fn dump_with_payloads(trie: &StrTrieMap<TermEntryWithPayload>) -> String {
    let mut out = String::new();
    writeln!(&mut out, "size: {}", trie.len()).unwrap();
    writeln!(&mut out, "entries:").unwrap();
    for (term, entry) in trie.iter() {
        writeln!(
            &mut out,
            "  {term:6}  score={score}  numDocs={num_docs}  payload={payload:?}",
            score = entry.score,
            num_docs = entry.num_docs,
            payload = entry.payload.label,
        )
        .unwrap();
    }
    out
}

fn drain_freecb_log() -> String {
    let entries: Vec<String> = FREECB_LOG.with(|log| std::mem::take(&mut *log.borrow_mut()));
    if entries.is_empty() {
        return "freecb fired: <none>\n".into();
    }
    let mut out = String::new();
    writeln!(&mut out, "freecb fired ({}x):", entries.len()).unwrap();
    for label in entries {
        writeln!(&mut out, "  {label:?}").unwrap();
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
    let mut trie = StrTrieMap::<TermEntry>::new();

    // Parallel pair — same triples on "foo" with INCR and "bar" with REPLACE.
    // INCR: score accumulates 1.0 + 0.5 + 2.0 = 3.5.
    // REPLACE: last score wins -> 2.0.
    // Both: numDocs accumulates 1 + 1 + 1 = 3 (mode-independent per
    // trie_node.c:296).
    let steps: &[(&str, i32, f32, usize, &str)] = &[
        ("foo", ADD_INCR, 1.0, 1, "first insert — creates leaf (rc=OK_NEW)"),
        ("foo", ADD_INCR, 0.5, 1, "score += 0.5 -> 1.5; numDocs += 1 -> 2"),
        ("foo", ADD_INCR, 2.0, 1, "score += 2.0 -> 3.5; numDocs += 1 -> 3"),
        ("bar", ADD_REPLACE, 1.0, 1, "first insert — creates leaf (rc=OK_NEW)"),
        ("bar", ADD_REPLACE, 0.5, 1, "score = 0.5 (overwrite); numDocs += 1 -> 2"),
        ("bar", ADD_REPLACE, 2.0, 1, "score = 2.0 (overwrite); numDocs += 1 -> 3"),
    ];

    let mut out = String::new();
    for (term, incr, score, num_docs, note) in steps {
        let rc = if *incr == ADD_INCR {
            insert_incr(&mut trie, term, *score, *num_docs)
        } else {
            insert_replace(&mut trie, term, *score, *num_docs)
        };
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
    let mut trie = StrTrieMap::<TermEntry>::new();

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
    let rc = insert_replace(&mut trie, "foo", 1.0, 2);
    writeln!(
        &mut out,
        "insert(\"foo\", REPLACE, score=1, numDocs=2) -> {}",
        rc_name(rc)
    )
    .unwrap();
    let rc = insert_replace(&mut trie, "foobar", 1.0, 1);
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
    let rc = insert_incr(&mut trie, "foo", 5.0, 10);
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
    let mut trie = StrTrieMap::<TermEntry>::new();

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
    let rc = insert_replace(&mut trie, "apple", 1.0, 1);
    writeln!(
        &mut out,
        "insert(\"apple\", REPLACE, score=1, numDocs=1) -> {}",
        rc_name(rc)
    )
    .unwrap();
    let rc = insert_replace(&mut trie, "apply", 2.0, 1);
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
    let rc = insert_incr(&mut trie, "appl", 7.0, 3);
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
fn lex_incr_with_payload_vs_replace_with_payload() {
    FREECB_LOG.with(|log| log.borrow_mut().clear());

    let mut trie = StrTrieMap::<TermEntryWithPayload>::new();
    let mut out = String::new();

    // Step 1: install "v1". No prior payload, no Drop fires.
    writeln!(
        &mut out,
        "=== step 1: REPLACE-insert \"foo\" with payload \"v1\" ==="
    )
    .unwrap();
    let rc = insert_replace_with_payload(&mut trie, "foo", 1.0, 1, "v1");
    writeln!(
        &mut out,
        "insert(\"foo\", REPLACE, score=1, numDocs=1, payload=\"v1\") -> {}",
        rc_name(rc)
    )
    .unwrap();
    out.push_str(&dump_with_payloads(&trie));
    out.push_str(&drain_freecb_log());

    // Step 2: INCR with new payload "v2". The old `TermEntryWithPayload`
    // moves out via `remove`, gets destructured inside the `Some` arm, and
    // its `LoggingPayload` field drops at end of arm — firing the log push
    // on "v1" BEFORE the next `trie.insert` runs. Mirrors C's
    // `triePayload_Free(n->payload, freecb)` followed by `triePayload_New`.
    writeln!(
        &mut out,
        "\n=== step 2: INCR-insert \"foo\" with payload \"v2\" — old payload still replaced ==="
    )
    .unwrap();
    let rc = insert_incr_with_payload(&mut trie, "foo", 1.0, 1, "v2");
    writeln!(
        &mut out,
        "insert(\"foo\", INCR, score=1, numDocs=1, payload=\"v2\") -> {}",
        rc_name(rc)
    )
    .unwrap();
    out.push_str(&dump_with_payloads(&trie));
    out.push_str(&drain_freecb_log());

    // Step 3: REPLACE with "v3". Same mechanism — old payload "v2" drops
    // before the new entry is inserted.
    writeln!(
        &mut out,
        "\n=== step 3: REPLACE-insert \"foo\" with payload \"v3\" — freecb fires on survivor ==="
    )
    .unwrap();
    let rc = insert_replace_with_payload(&mut trie, "foo", 3.0, 1, "v3");
    writeln!(
        &mut out,
        "insert(\"foo\", REPLACE, score=3, numDocs=1, payload=\"v3\") -> {}",
        rc_name(rc)
    )
    .unwrap();
    out.push_str(&dump_with_payloads(&trie));
    out.push_str(&drain_freecb_log());

    // Step 4: drop the trie. Only one entry survives ("foo" with "v3"); its
    // payload's Drop pushes to the log.
    writeln!(
        &mut out,
        "\n=== step 4: TrieType_Free — surviving payload freed ==="
    )
    .unwrap();
    drop(trie);
    out.push_str(&drain_freecb_log());

    insta::with_settings!(
        {
            snapshot_path => "../../../../rune_trie_snapshots/tests/integration/snapshots",
            prepend_module_to_snapshot => false,
        },
        { insta::assert_snapshot!("lex_incr_with_payload_vs_replace_with_payload", out); }
    );
}

#[test]
fn lex_incr_mixed_with_replace() {
    let mut trie = StrTrieMap::<TermEntry>::new();

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
        (
            ADD_INCR,
            2.0,
            1,
            "score += 2 -> 12; numDocs += 1 -> 7",
        ),
    ];

    for (incr, score, num_docs, note) in steps {
        let rc = if *incr == ADD_INCR {
            insert_incr(&mut trie, "foo", *score, *num_docs)
        } else {
            insert_replace(&mut trie, "foo", *score, *num_docs)
        };
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
