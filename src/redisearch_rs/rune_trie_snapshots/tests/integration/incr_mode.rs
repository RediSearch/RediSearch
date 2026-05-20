/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Snapshot the C rune-trie's ADD_INCR insert mode — the path taken by the
//! production terms-trie at `src/spec.c:1958` on every doc indexed:
//!
//! ```c
//! Trie_InsertStringBuffer(sp->terms, term, len, 1, /*incr=*/1, NULL, /*numDocs=*/1)
//! ```
//!
//! `Trie_InsertRune` translates `incr=1` into `ADD_INCR` for `TrieNode_Add`.
//! All other scenarios in this crate exercise `ADD_REPLACE`. The two paths
//! diverge in `__trieNode_Add` (`src/trie/trie_node.c:282-310`):
//!
//! - **Score** is the only field with mode-dependent handling:
//!     - `ADD_INCR`:    `n->score += score` (additive)
//!     - `ADD_REPLACE`: `n->score = score` (overwrite)
//! - **numDocs** is `n->numDocs += numDocs` in BOTH modes (line 296). The
//!   "REPLACE overwrites numDocs" intuition is wrong — verified against the C
//!   source — the field always accumulates per call.
//! - **Payload** replacement is mode-agnostic too (line 297-303): if the new
//!   payload is non-null with non-zero length, BOTH modes free the existing
//!   payload via freecb and install the new one. INCR does NOT keep the old
//!   payload — it just leaves the score additive while still replacing the
//!   payload bytes.
//! - **Previously-deleted node**: `TrieNode_Delete` zeroes both `score` and
//!   `numDocs` on the mark-deleted node (line 488-489), so an INCR resurrect
//!   yields `score=new` and `numDocs=new` (the addition baseline is 0), with
//!   `TRIENODE_DELETED` cleared and `TRIENODE_TERMINAL` re-set.
//! - **Non-terminal split node**: when an insert exactly matches a prior split
//!   prefix's path AND the non-terminal already exists (i.e. we re-enter
//!   `__trieNode_Add` on the existing node rather than triggering a fresh
//!   `__trie_SplitNode`), the offset==len branch (line 282) runs with mode-
//!   aware score handling — but since the non-terminal's existing
//!   `score`/`numDocs` are 0 (set by `__trie_SplitNode` at line 182-183), INCR
//!   produces the same numbers as REPLACE would.
//!
//! These snapshots pin the per-mode arithmetic for the production terms-trie
//! path. A Rust port that conflates the two modes — e.g. forgets the score
//! addition, or accidentally makes numDocs mode-dependent — will surface as a
//! snapshot diff against any of the five scenarios below.

use std::cell::RefCell;
use std::ffi::{CStr, c_void};
use std::fmt::Write as _;
use std::ptr;

use ffi::{
    NewTrie, RSPayload, Trie, TrieIterator_Free, TrieIterator_Next, TrieSortMode_Trie_Sort_Lex,
    TrieType_Free, Trie_Delete, Trie_InsertStringBuffer, Trie_IterateAll, Trie_Size, rune, t_len,
};
use libc::c_char;

const ADD_REPLACE: i32 = 0;
const ADD_INCR: i32 = 1;

thread_local! {
    /// Mirrors the freecb log used by `payloads.rs` — each payload free
    /// pushes its decoded label here so the test can render the sequence of
    /// freecb invocations between operations.
    static FREECB_LOG: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

unsafe extern "C" fn capture_freecb(data: *mut c_void) {
    if data.is_null() {
        FREECB_LOG.with(|log| log.borrow_mut().push("<null>".into()));
        return;
    }
    // SAFETY: callers always insert null-terminated payload bytes via
    // `insert_with_payload` below, so the trie's internal copy is also
    // null-terminated. The pointer is live for the duration of the call.
    let label = unsafe { CStr::from_ptr(data as *const c_char) }
        .to_string_lossy()
        .into_owned();
    FREECB_LOG.with(|log| log.borrow_mut().push(label));
}

unsafe fn runes_to_string(ptr: *const rune, len: usize) -> String {
    // SAFETY: caller passes a valid rune buffer of length `len`.
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    String::from_utf16(slice).expect("trie runes are valid BMP UTF-16")
}

unsafe fn insert(trie: *mut Trie, term: &str, score: f64, incr: i32, num_docs: usize) -> i32 {
    // SAFETY: `term.as_bytes()` is a valid borrow for the call; C copies the
    // buffer internally before returning.
    unsafe {
        Trie_InsertStringBuffer(
            trie,
            term.as_ptr() as *const c_char,
            term.len(),
            score,
            incr,
            ptr::null_mut(),
            num_docs,
        )
    }
}

/// `Trie_InsertStringBuffer` returns `TRIE_OK_NEW` (1) when the call created a
/// new entry (or un-deleted a previously-deleted slot), `TRIE_OK_UPDATED` (0)
/// when an existing terminal was updated. Rendering the symbolic name makes
/// the snapshot read against the C header (`trie_node.h:29-32`).
const fn rc_name(rc: i32) -> &'static str {
    match rc {
        0 => "OK_UPDATED",
        1 => "OK_NEW",
        -1 => "ERR_PAYLOAD_OVERFLOW",
        _ => "UNKNOWN",
    }
}

const fn mode_name(incr: i32) -> &'static str {
    if incr == ADD_INCR { "INCR" } else { "REPLACE" }
}

unsafe fn insert_with_payload(
    trie: *mut Trie,
    term: &str,
    score: f64,
    incr: i32,
    num_docs: usize,
    label: &str,
) -> i32 {
    let mut bytes: Vec<u8> = label.as_bytes().to_vec();
    bytes.push(0); // NUL terminator so `capture_freecb`'s CStr decode is safe.

    let mut payload = RSPayload {
        data: bytes.as_mut_ptr() as *mut c_char,
        // `TriePayload::len` excludes the NUL — matching how `Trie_Insert`
        // paths in the suffix/suggest code set `payload.len`.
        len: label.len(),
    };

    // SAFETY: `term` and `bytes` are valid for the duration of the call; the
    // C trie copies both before returning.
    unsafe {
        Trie_InsertStringBuffer(
            trie,
            term.as_ptr() as *const c_char,
            term.len(),
            score,
            incr,
            &mut payload,
            num_docs,
        )
    }
}

unsafe fn delete(trie: *mut Trie, term: &str) -> i32 {
    // SAFETY: `term.as_bytes()` is a valid borrow for the call.
    unsafe { Trie_Delete(trie, term.as_ptr() as *const c_char, term.len()) }
}

fn dump_all(trie: *mut Trie) -> String {
    // SAFETY: `trie` was created via `NewTrie` and has not been freed.
    let size = unsafe { Trie_Size(trie) };
    // SAFETY: `trie` is live; the iterator is freed below.
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

    // SAFETY: `it` was produced by `Trie_IterateAll` above and not freed yet.
    unsafe { TrieIterator_Free(it) };
    out
}

/// As `dump_all`, but also renders each entry's payload bytes — used by the
/// payload-interaction scenario where the freecb log needs to be cross-
/// referenced against the surviving payload to interpret each step.
fn dump_with_payloads(trie: *mut Trie) -> String {
    // SAFETY: `trie` is live for the duration of this function.
    let size = unsafe { Trie_Size(trie) };
    // SAFETY: `trie` is live; the iterator is freed below.
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
    // returned `runes_ptr` buffer and the `payload.data` slice.
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
        let payload_repr = if payload.data.is_null() {
            "<none>".to_string()
        } else {
            // SAFETY: `payload.data` points into the trie's owned buffer for
            // exactly `payload.len` bytes (excluding our NUL terminator).
            let bytes = unsafe {
                std::slice::from_raw_parts(payload.data as *const u8, payload.len as usize)
            };
            String::from_utf8(bytes.to_vec()).expect("test payloads are ASCII")
        };
        writeln!(
            &mut out,
            "  {term:6}  score={score}  numDocs={num_docs}  payload={payload_repr:?}"
        )
        .unwrap();
    }

    // SAFETY: `it` was just produced by `Trie_IterateAll` and not freed yet.
    unsafe { TrieIterator_Free(it) };
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

#[test]
fn lex_incr_score_and_numdocs_accumulation() {
    // SAFETY: `NewTrie` with a NULL free-callback matches the terms-trie
    // construction in `src/spec.c` — the production caller of ADD_INCR.
    let trie = unsafe { NewTrie(None, TrieSortMode_Trie_Sort_Lex) };

    // Each row: (term, mode, score, numDocs, structural note for the snapshot).
    //
    // Parallel pair: "foo" uses ADD_INCR three times, "bar" uses ADD_REPLACE
    // three times with the same (score, numDocs) triples. Per the C source:
    //   - foo: score accumulates -> 1.0 + 0.5 + 2.0 = 3.5
    //   - bar: score overwritten -> last call wins -> 2.0
    //   - both: numDocs accumulates -> 1 + 1 + 1 = 3
    // A Rust port that handles numDocs mode-dependently would diverge on
    // "bar"'s numDocs.
    let steps: &[(&str, i32, f64, usize, &str)] = &[
        ("foo", ADD_INCR, 1.0, 1, "first insert — creates leaf (rc=OK_NEW)"),
        ("foo", ADD_INCR, 0.5, 1, "score += 0.5 -> 1.5; numDocs += 1 -> 2"),
        ("foo", ADD_INCR, 2.0, 1, "score += 2.0 -> 3.5; numDocs += 1 -> 3"),
        ("bar", ADD_REPLACE, 1.0, 1, "first insert — creates leaf (rc=OK_NEW)"),
        ("bar", ADD_REPLACE, 0.5, 1, "score = 0.5 (overwrite); numDocs += 1 -> 2"),
        ("bar", ADD_REPLACE, 2.0, 1, "score = 2.0 (overwrite); numDocs += 1 -> 3"),
    ];

    let mut out = String::new();
    for (term, incr, score, num_docs, note) in steps {
        // SAFETY: `trie` is live; term bytes valid for the call.
        let rc = unsafe { insert(trie, term, *score, *incr, *num_docs) };
        writeln!(
            &mut out,
            "--- insert({term:?}, mode={}, score={score}, numDocs={num_docs}) -> {} — {note} ---",
            mode_name(*incr),
            rc_name(rc),
        )
        .unwrap();
        out.push_str(&dump_all(trie));
    }

    // SAFETY: `trie` was created by `NewTrie` above; never freed elsewhere.
    unsafe { TrieType_Free(trie as *mut c_void) };

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_incr_score_and_numdocs_accumulation", out); }
    );
}

#[test]
fn lex_incr_over_deleted_node() {
    // SAFETY: `NewTrie` with a NULL free-callback matches the production
    // terms-trie construction.
    let trie = unsafe { NewTrie(None, TrieSortMode_Trie_Sort_Lex) };

    let mut out = String::new();

    // Step 1: build the fixture so that "foo" is an *internal terminal* (has a
    // child). Inserting "foo" then "foobar" yields:
    //   root -> [foo] (terminal, score=1, numDocs=2) -> [bar] (terminal)
    // The internal-terminal shape is what makes the mark-delete path
    // observable: a leaf "foo" would be physically removed by the
    // optimize-sweep at the end of `TrieNode_Delete`, and the subsequent
    // re-insert would create a brand-new node — which would only test the
    // "first insert" path, not the un-delete path.
    writeln!(&mut out, "=== step 1: build internal-terminal \"foo\" via \"foo\" + \"foobar\" ===").unwrap();
    // SAFETY: `trie` is live; term bytes valid for the call.
    let rc = unsafe { insert(trie, "foo", 1.0, ADD_REPLACE, 2) };
    writeln!(&mut out, "insert(\"foo\", REPLACE, score=1, numDocs=2) -> {}", rc_name(rc)).unwrap();
    // SAFETY: `trie` is live.
    let rc = unsafe { insert(trie, "foobar", 1.0, ADD_REPLACE, 1) };
    writeln!(&mut out, "insert(\"foobar\", REPLACE, score=1, numDocs=1) -> {}", rc_name(rc)).unwrap();
    out.push_str(&dump_all(trie));

    // Step 2: mark-delete the internal terminal. `TrieNode_Delete` flips
    // TRIENODE_TERMINAL off, sets TRIENODE_DELETED, and zeroes
    // `score`/`numDocs` (trie_node.c:486-489). The child "foobar" survives —
    // the node itself is NOT physically freed.
    writeln!(&mut out, "\n=== step 2: Trie_Delete(\"foo\") — mark-delete internal terminal ===").unwrap();
    // SAFETY: `trie` is live; term bytes valid for the call.
    let r = unsafe { delete(trie, "foo") };
    writeln!(&mut out, "Trie_Delete(\"foo\") -> {r}").unwrap();
    out.push_str(&dump_all(trie));

    // Step 3: INCR-resurrect. The path through `__trieNode_Add`:
    //   - offset==len==3, hits the in-place update branch (line 282).
    //   - ADD_INCR: `n->score += 5`. n->score was zeroed at delete time, so
    //     the result is just 5 (NOT 1+5 from the pre-delete state).
    //   - `n->numDocs += 10`. Was zeroed -> 10.
    //   - TRIENODE_TERMINAL re-set, TRIENODE_DELETED cleared.
    //   - rc returns OK_NEW because the pre-call state was `!term || deleted`.
    writeln!(&mut out, "\n=== step 3: INCR over deleted node — score/numDocs reset to new values ===").unwrap();
    // SAFETY: `trie` is live; term bytes valid for the call.
    let rc = unsafe { insert(trie, "foo", 5.0, ADD_INCR, 10) };
    writeln!(&mut out, "insert(\"foo\", INCR, score=5, numDocs=10) -> {}", rc_name(rc)).unwrap();
    out.push_str(&dump_all(trie));

    // SAFETY: `trie` was created by `NewTrie` above; never freed elsewhere.
    unsafe { TrieType_Free(trie as *mut c_void) };

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_incr_over_deleted_node", out); }
    );
}

#[test]
fn lex_incr_over_non_terminal_split() {
    // SAFETY: `NewTrie` with a NULL free-callback matches the production
    // terms-trie construction.
    let trie = unsafe { NewTrie(None, TrieSortMode_Trie_Sort_Lex) };

    let mut out = String::new();

    // Step 1: create a non-terminal "appl" via the apple/apply split. Per
    // `__trie_SplitNode` (trie_node.c:166-193), the freshly-created parent
    // gets `score=0`, `numDocs=0` (line 182-183), TERMINAL/DELETED flags
    // cleared. The children inherit their original scores/numDocs.
    writeln!(&mut out, "=== step 1: \"apple\" + \"apply\" — creates non-terminal \"appl\" ===").unwrap();
    // SAFETY: `trie` is live; term bytes valid for the call.
    let rc = unsafe { insert(trie, "apple", 1.0, ADD_REPLACE, 1) };
    writeln!(&mut out, "insert(\"apple\", REPLACE, score=1, numDocs=1) -> {}", rc_name(rc)).unwrap();
    // SAFETY: `trie` is live.
    let rc = unsafe { insert(trie, "apply", 2.0, ADD_REPLACE, 1) };
    writeln!(&mut out, "insert(\"apply\", REPLACE, score=2, numDocs=1) -> {}", rc_name(rc)).unwrap();
    // The non-terminal "appl" exists in the tree but is NOT visible in
    // `Trie_IterateAll` output (the iterator only emits terminals).
    out.push_str(&dump_all(trie));

    // Step 2: INCR-insert "appl". Path through `__trieNode_Add`:
    //   - We walk to the existing "appl" non-terminal (NOT triggering a fresh
    //     split — the structure already exists).
    //   - offset==len==4 on the existing node, hits the in-place branch.
    //   - ADD_INCR: `n->score += 7`. Pre-state from `__trie_SplitNode`: 0.
    //     Result = 7. (REPLACE would have given the same number on this exact
    //     fixture because the split-parent's pre-state is 0.)
    //   - `n->numDocs += 3`. Pre-state: 0. Result = 3.
    //   - TRIENODE_TERMINAL set; node now appears in `Trie_IterateAll`.
    //   - rc=OK_NEW because pre-state was `!term`.
    writeln!(&mut out, "\n=== step 2: INCR-insert \"appl\" — non-terminal becomes terminal ===").unwrap();
    // SAFETY: `trie` is live; term bytes valid for the call.
    let rc = unsafe { insert(trie, "appl", 7.0, ADD_INCR, 3) };
    writeln!(&mut out, "insert(\"appl\", INCR, score=7, numDocs=3) -> {}", rc_name(rc)).unwrap();
    out.push_str(&dump_all(trie));

    // SAFETY: `trie` was created by `NewTrie` above.
    unsafe { TrieType_Free(trie as *mut c_void) };

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_incr_over_non_terminal_split", out); }
    );
}

#[test]
fn lex_incr_with_payload_vs_replace_with_payload() {
    // Clear any leftover state (cheap insurance — each #[test] runs on a
    // fresh thread, but the freecb log is thread-local).
    FREECB_LOG.with(|log| log.borrow_mut().clear());

    // SAFETY: `NewTrie` accepts a non-NULL freecb; the trie invokes it on
    // every physical payload free.
    let trie = unsafe { NewTrie(Some(capture_freecb), TrieSortMode_Trie_Sort_Lex) };

    let mut out = String::new();

    // Step 1: install payload "v1" via ADD_REPLACE. No prior payload to free,
    // so freecb does NOT fire here.
    writeln!(&mut out, "=== step 1: REPLACE-insert \"foo\" with payload \"v1\" ===").unwrap();
    // SAFETY: `trie` is live; term + label bytes valid for the call.
    let rc = unsafe { insert_with_payload(trie, "foo", 1.0, ADD_REPLACE, 1, "v1") };
    writeln!(&mut out, "insert(\"foo\", REPLACE, score=1, numDocs=1, payload=\"v1\") -> {}", rc_name(rc)).unwrap();
    out.push_str(&dump_with_payloads(trie));
    out.push_str(&drain_freecb_log());

    // Step 2: INCR-insert "foo" with payload "v2".
    //
    // The C source (`__trieNode_Add` line 297-303) handles payloads the SAME
    // way regardless of `op` (INCR vs REPLACE): if the new payload is
    // non-null with non-zero length, free the old via freecb and install the
    // new. So freecb fires once on "v1" and the surviving payload is "v2".
    //
    // The score arithmetic is the only thing INCR changes here: 1.0 + 1.0 = 2.0
    // (vs REPLACE which would yield 1.0).
    writeln!(&mut out, "\n=== step 2: INCR-insert \"foo\" with payload \"v2\" — old payload still replaced ===").unwrap();
    // SAFETY: `trie` is live; term + label bytes valid for the call.
    let rc = unsafe { insert_with_payload(trie, "foo", 1.0, ADD_INCR, 1, "v2") };
    writeln!(&mut out, "insert(\"foo\", INCR, score=1, numDocs=1, payload=\"v2\") -> {}", rc_name(rc)).unwrap();
    out.push_str(&dump_with_payloads(trie));
    out.push_str(&drain_freecb_log());

    // Step 3: REPLACE-insert "foo" with payload "v3". Same mode-agnostic
    // payload replacement — freecb fires on whatever payload survived step 2.
    writeln!(&mut out, "\n=== step 3: REPLACE-insert \"foo\" with payload \"v3\" — freecb fires on survivor ===").unwrap();
    // SAFETY: `trie` is live; term + label bytes valid for the call.
    let rc = unsafe { insert_with_payload(trie, "foo", 3.0, ADD_REPLACE, 1, "v3") };
    writeln!(&mut out, "insert(\"foo\", REPLACE, score=3, numDocs=1, payload=\"v3\") -> {}", rc_name(rc)).unwrap();
    out.push_str(&dump_with_payloads(trie));
    out.push_str(&drain_freecb_log());

    // Step 4: tear down the trie. `TrieType_Free -> TrieNode_Free` cascades
    // through every surviving payload — here, just the v3 bytes on "foo".
    writeln!(&mut out, "\n=== step 4: TrieType_Free — surviving payload freed ===").unwrap();
    // SAFETY: `trie` was created by `NewTrie` above; never freed elsewhere.
    unsafe { TrieType_Free(trie as *mut c_void) };
    out.push_str(&drain_freecb_log());

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_incr_with_payload_vs_replace_with_payload", out); }
    );
}

#[test]
fn lex_incr_mixed_with_replace() {
    // SAFETY: `NewTrie` with a NULL free-callback matches the production
    // terms-trie construction.
    let trie = unsafe { NewTrie(None, TrieSortMode_Trie_Sort_Lex) };

    let mut out = String::new();

    // Three calls on the same key, alternating modes. The C semantics
    // (verified against `__trieNode_Add` line 282-309):
    //
    //   1. INCR, score=1, numDocs=1
    //      - First insert. Creates new leaf via `__trie_AddChildIdx` with the
    //        requested score/numDocs verbatim. (INCR vs REPLACE makes no
    //        difference when there's no existing node to update against.)
    //        Result: score=1, numDocs=1, rc=OK_NEW.
    //
    //   2. REPLACE, score=10, numDocs=5
    //      - Existing terminal, offset==len. REPLACE: `n->score = 10`
    //        (overwrites). `n->numDocs += 5` (accumulates regardless of
    //        mode). Result: score=10, numDocs=6, rc=OK_UPDATED.
    //
    //   3. INCR, score=2, numDocs=1
    //      - Existing terminal, offset==len. INCR: `n->score += 2 -> 12`.
    //        `n->numDocs += 1 -> 7`. Result: score=12, numDocs=7, rc=OK_UPDATED.
    let steps: &[(i32, f64, usize, &str)] = &[
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
        // SAFETY: `trie` is live; term bytes valid for the call.
        let rc = unsafe { insert(trie, "foo", *score, *incr, *num_docs) };
        writeln!(
            &mut out,
            "--- insert(\"foo\", mode={}, score={score}, numDocs={num_docs}) -> {} — {note} ---",
            mode_name(*incr),
            rc_name(rc),
        )
        .unwrap();
        out.push_str(&dump_all(trie));
    }

    // SAFETY: `trie` was created by `NewTrie` above.
    unsafe { TrieType_Free(trie as *mut c_void) };

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_incr_mixed_with_replace", out); }
    );
}
