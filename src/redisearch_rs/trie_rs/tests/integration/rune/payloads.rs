/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Assert `RuneTrieMap` reproduces the C trie's payload roundtrip and
//! per-physical-free callback ordering against the snapshot owned by
//! `rune_trie_snapshots::payloads`.
//!
//! The Rust port doesn't expose a `TrieFreeCallback` — it doesn't need
//! one, because dropping the trie (or a value pulled out of it) runs the
//! payload's `Drop` impl. To compare snapshots byte-for-byte against the
//! C oracle we wrap the test payload in a type whose `Drop` pushes to a
//! thread-local log, then drain that log between steps. This makes the
//! Rust trie's "physical free moments" externally observable in the same
//! way the C trie's freecb invocations are.
//!
//! Mapping from C events to Rust events:
//!
//!   - C `ADD_REPLACE` frees the old payload bytes via freecb when the
//!     C trie's `TrieNode_Add` replaces an existing terminal. The Rust
//!     equivalent: `insert(..)` returns `Option<V>` whose `Some(prev)`
//!     is dropped at the end of the call expression. The drop fires
//!     the log push.
//!
//!   - C `Trie_Delete` on a leaf physically frees the node and runs
//!     freecb via the `__trieNode_optimizeChildren` sweep. The Rust
//!     equivalent: `remove(..)` returns `Option<V>`; we hold it briefly
//!     to read the return code, then drop it.
//!
//!   - C `TrieType_Free` cascades freecb over every surviving payload.
//!     The Rust equivalent: dropping the trie runs every payload's
//!     `Drop` in iteration order.
//!
//! # Expected API surface
//!
//! Items the compiler will currently flag (same as `rune_boundary_cases`):
//!
//! - `RuneTrieMap::<V>::insert(&mut self, key, value) -> Option<V>` so
//!   the test can tell NEW from UPDATED and so the returned `Some(prev)`
//!   drops at the call site (mirroring the freecb timing in the snapshot).

use std::cell::RefCell;
use std::fmt::Write as _;

use trie_rs::rune::{Rune, RuneTrieMap};

thread_local! {
    /// The Rust-side analog of `FREECB_LOG` in the C oracle. Each entry
    /// is the payload label captured by `LoggingPayload::drop`. Drained
    /// and rendered between test steps so the snapshot reads as a trace.
    static FREECB_LOG: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

/// Payload wrapper whose `Drop` records the label, mirroring the C
/// freecb's "invoked once per physical free with the payload bytes"
/// semantic. The C oracle stores `payload-apple` as raw bytes plus a
/// NUL we appended at the call site; here we don't need the NUL because
/// `String` carries its own length.
struct LoggingPayload {
    label: String,
}

impl Drop for LoggingPayload {
    fn drop(&mut self) {
        FREECB_LOG.with(|log| log.borrow_mut().push(self.label.clone()));
    }
}

fn term_runes(s: &str) -> Vec<Rune> {
    s.encode_utf16().collect()
}

/// Insert `term` carrying `label` and return the C rc: 1 = NEW, 0 = UPDATED.
/// The `prev` Option dropping at the end of the function body is what
/// fires the freecb log push on replacement — matching the C trie's
/// "free old payload, install new one" order in `TrieNode_Add`.
fn insert_with_payload(
    trie: &mut RuneTrieMap<LoggingPayload>,
    term: &str,
    label: &str,
) -> i32 {
    let key = term_runes(term);
    let prev = trie.insert(
        &key,
        LoggingPayload {
            label: label.to_string(),
        },
    );
    if prev.is_none() { 1 } else { 0 }
}

fn delete(trie: &mut RuneTrieMap<LoggingPayload>, term: &str) -> i32 {
    // The returned `Option<LoggingPayload>` drops at the end of this
    // function, firing the freecb log push when present. Mirrors the
    // C trie's `__trieNode_optimizeChildren` calling freecb on the
    // physical-free path.
    let removed = trie.remove(&term_runes(term));
    i32::from(removed.is_some())
}

/// Dump every terminal: term + payload label (we control all inputs so
/// strings decode cleanly). Field widths chosen to match the C-side
/// `dump_with_payloads` so the snapshots align column-for-column.
fn dump_with_payloads(trie: &RuneTrieMap<LoggingPayload>) -> String {
    let mut out = String::new();
    writeln!(&mut out, "size: {}", trie.len()).unwrap();
    writeln!(&mut out, "entries:").unwrap();
    for (key, payload) in trie.iter() {
        let term = String::from_utf16(&key).expect("trie runes are valid BMP UTF-16");
        writeln!(&mut out, "  {term:8}  payload={:?}", payload.label).unwrap();
    }
    out
}

/// Drain and render the freecb log since the last call. Matches the
/// C-side wording exactly so the shared snapshot byte-matches both
/// oracles.
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

#[test]
fn lex_payload_roundtrip_and_freecb_lifecycle() {
    // Insurance against thread-local bleed from a prior test on the
    // same thread (cargo nextest runs each test in a fresh process by
    // default, but this is cheap).
    FREECB_LOG.with(|log| log.borrow_mut().clear());

    let mut trie = RuneTrieMap::<LoggingPayload>::new();
    let mut out = String::new();

    // Step 1 — three distinct payloads, none should be freed.
    writeln!(
        &mut out,
        "=== step 1: insert three terms with distinct payloads ==="
    )
    .unwrap();
    for (term, label) in [
        ("apple", "payload-apple"),
        ("ape", "payload-ape"),
        ("banana", "payload-banana"),
    ] {
        let rc = insert_with_payload(&mut trie, term, label);
        writeln!(&mut out, "insert({term:?}, {label:?}) -> rc={rc}").unwrap();
    }
    out.push_str(&dump_with_payloads(&trie));
    out.push_str(&drain_freecb_log());

    // Step 2 — replace "apple". Old payload `LoggingPayload` drops
    // inside `insert_with_payload`, pushing "payload-apple" to the log.
    writeln!(
        &mut out,
        "\n=== step 2: ADD_REPLACE over existing terminal — old payload freed ==="
    )
    .unwrap();
    let rc = insert_with_payload(&mut trie, "apple", "payload-apple-v2");
    writeln!(
        &mut out,
        "insert({:?}, {:?}) -> rc={rc}",
        "apple", "payload-apple-v2"
    )
    .unwrap();
    out.push_str(&dump_with_payloads(&trie));
    out.push_str(&drain_freecb_log());

    // Step 3 — leaf delete; removed payload drops inside `delete()`.
    writeln!(
        &mut out,
        "\n=== step 3: delete leaf \"ape\" — physical removal fires freecb ==="
    )
    .unwrap();
    let r = delete(&mut trie, "ape");
    writeln!(&mut out, "Trie_Delete(\"ape\") -> {r}").unwrap();
    out.push_str(&dump_with_payloads(&trie));
    out.push_str(&drain_freecb_log());

    // Step 4 — leaf delete; v2 payload from step 2 now drops.
    writeln!(
        &mut out,
        "\n=== step 4: delete leaf \"apple\" — physical removal fires freecb on v2 payload ==="
    )
    .unwrap();
    let r = delete(&mut trie, "apple");
    writeln!(&mut out, "Trie_Delete(\"apple\") -> {r}").unwrap();
    out.push_str(&dump_with_payloads(&trie));
    out.push_str(&drain_freecb_log());

    // Step 5 — drop the trie. Only "banana" remains; its payload drops
    // in whatever iteration order the trie chooses. With a single entry
    // there is no ordering ambiguity to pin.
    writeln!(
        &mut out,
        "\n=== step 5: TrieType_Free — all remaining payloads freed in tree order ==="
    )
    .unwrap();
    drop(trie);
    out.push_str(&drain_freecb_log());

    insta::with_settings!(
        {
            snapshot_path => "../../../../rune_trie_snapshots/tests/integration/snapshots",
            prepend_module_to_snapshot => false,
        },
        { insta::assert_snapshot!("lex_payload_roundtrip_and_freecb_lifecycle", out); }
    );
}
