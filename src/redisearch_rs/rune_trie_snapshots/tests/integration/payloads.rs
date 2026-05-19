/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Snapshot the C rune-trie's payload lifecycle: roundtrip on iterate-back,
//! free-callback invocation timing on physical removal, replacement semantics
//! on ADD over an existing terminal, and the bulk-free at trie destruction.
//!
//! The C trie copies the user's payload bytes into its own `TriePayload` on
//! insert (see `triePayload_New` in `src/trie/trie_node.c`), then on physical
//! free invokes the trie's `TrieFreeCallback` with a pointer to those internal
//! bytes — giving the user a chance to do additional cleanup (the suffix trie
//! uses this to free a heap-allocated `suffixData` referenced *by* the bytes,
//! see `suffixTrie_freeCallback` in `src/suffix.c`). The mark-delete path used
//! by `Trie_Delete` on internal terminals does NOT physically remove the
//! node, and therefore does NOT fire the freecb — only leaf removals (via the
//! optimize sweep at the end of `TrieNode_Delete`) and `TrieType_Free` do.
//!
//! These are exactly the spots where a Rust port that frees aggressively
//! (e.g. always physically removes on delete) or never (e.g. forgets to
//! cascade-free during `Drop`) will diverge — the freecb log captured below
//! pins the observable behavior so divergences surface as a snapshot diff.

use std::cell::RefCell;
use std::ffi::{CStr, c_void};
use std::fmt::Write as _;
use std::ptr;

use ffi::{
    NewTrie, RSPayload, Trie, TrieIterator_Free, TrieIterator_Next, TrieSortMode_Trie_Sort_Lex,
    TrieType_Free, Trie_Delete, Trie_InsertStringBuffer, Trie_IterateAll, Trie_Size, rune, t_len,
};
use libc::c_char;

thread_local! {
    /// Each payload byte-string handed to the C trie is null-terminated by us
    /// before insertion (the trie itself is byte-agnostic, but our `freecb`
    /// reads the payload as a C string so we have a printable identifier for
    /// the log). The freecb pushes the decoded label here each time it fires.
    /// The test consumes + asserts against this log between operations.
    static FREECB_LOG: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

/// C trie passes `payload->data` (a pointer into its internal `TriePayload`
/// buffer) — not the wrapper struct — to the freecb. We treat those bytes as
/// a null-terminated label and append it to `FREECB_LOG`.
unsafe extern "C" fn capture_freecb(data: *mut c_void) {
    if data.is_null() {
        FREECB_LOG.with(|log| log.borrow_mut().push("<null>".into()));
        return;
    }
    // SAFETY: callers always insert null-terminated payload bytes (see
    // `insert_with_payload` below), so the trie's internal copy is also
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

/// Insert `term` carrying `label` as its payload. The label is copied into a
/// fresh `Vec<u8>` with a trailing NUL so the freecb can decode it later.
/// `Trie_InsertStringBuffer` copies both the term and the payload bytes into
/// trie-owned storage before returning, so the temporary buffer's lifetime
/// only needs to outlast the call.
unsafe fn insert_with_payload(trie: *mut Trie, term: &str, label: &str) -> i32 {
    let mut bytes: Vec<u8> = label.as_bytes().to_vec();
    bytes.push(0); // NUL terminator for `capture_freecb`'s CStr decode.

    let mut payload = RSPayload {
        data: bytes.as_mut_ptr() as *mut c_char,
        // The recorded `TriePayload::len` excludes the NUL — matching how
        // `Trie_Insert` paths in the suffix/suggest code set `payload.len`.
        len: label.len(),
    };

    // SAFETY: `term` and `bytes` are valid for the duration of the call; the
    // C trie copies both before returning.
    unsafe {
        Trie_InsertStringBuffer(
            trie,
            term.as_ptr() as *const c_char,
            term.len(),
            1.0,
            0, // ADD_REPLACE
            &mut payload,
            1,
        )
    }
}

unsafe fn delete(trie: *mut Trie, term: &str) -> i32 {
    // SAFETY: `term.as_bytes()` is a valid borrow for the call.
    unsafe { Trie_Delete(trie, term.as_ptr() as *const c_char, term.len()) }
}

/// Dump every terminal: term + payload-bytes-as-utf8 (we control the inputs
/// so they decode cleanly). Used as the "what survives" half of each step.
fn dump_with_payloads(trie: *mut Trie) -> String {
    // SAFETY: `trie` is live for the duration of this function.
    let size = unsafe { Trie_Size(trie) };
    // SAFETY: `trie` is live; the returned iterator is freed at the end of
    // this function.
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
    // returned `runes_ptr` buffer and the `payload.data` slice (the C trie
    // hands us pointers into its internal copy on each iteration).
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

        // Payload bytes are the user-supplied label without the trailing NUL.
        // `payload.data` is null when the entry has no payload — print "<none>".
        let payload_repr = if payload.data.is_null() {
            "<none>".to_string()
        } else {
            // SAFETY: `payload.data` points into the trie's owned buffer for
            // exactly `payload.len` bytes (does NOT include our NUL terminator).
            let bytes = unsafe {
                std::slice::from_raw_parts(payload.data as *const u8, payload.len as usize)
            };
            String::from_utf8(bytes.to_vec()).expect("test payloads are ASCII")
        };

        writeln!(&mut out, "  {term:8}  payload={payload_repr:?}").unwrap();
    }

    // SAFETY: `it` was produced by `Trie_IterateAll` above and not freed yet.
    unsafe { TrieIterator_Free(it) };
    out
}

/// Drain and render the freecb log since the last call. Producing the lines
/// in invocation order is the point of the snapshot — they form an order-of-
/// freeing trace that pins the trie's internal cleanup choreography.
fn drain_freecb_log() -> String {
    let entries: Vec<String> =
        FREECB_LOG.with(|log| std::mem::take(&mut *log.borrow_mut()));
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
    // Make sure no leftover state from a prior test on the same thread
    // bleeds in (cargo test runs each test on a fresh thread by default, but
    // explicit clear is cheap insurance).
    FREECB_LOG.with(|log| log.borrow_mut().clear());

    // SAFETY: `NewTrie` accepts a non-NULL freecb; the C trie stores it and
    // invokes it on every physical payload free.
    let trie = unsafe { NewTrie(Some(capture_freecb), TrieSortMode_Trie_Sort_Lex) };

    let mut out = String::new();

    // Step 1 — insert three terms each carrying a distinct payload. No
    // payload should be freed yet, since none are evicted/replaced.
    writeln!(&mut out, "=== step 1: insert three terms with distinct payloads ===").unwrap();
    for (term, label) in [
        ("apple", "payload-apple"),
        ("ape", "payload-ape"),
        ("banana", "payload-banana"),
    ] {
        // SAFETY: `trie` is live; `term`/`label` valid for the call.
        let rc = unsafe { insert_with_payload(trie, term, label) };
        writeln!(&mut out, "insert({term:?}, {label:?}) -> rc={rc}").unwrap();
    }
    out.push_str(&dump_with_payloads(trie));
    out.push_str(&drain_freecb_log());

    // Step 2 — re-insert "apple" with a new payload. The C trie's
    // `TrieNode_Add` ADD_REPLACE path explicitly frees the old payload and
    // installs a new one (see `trie_node.c` line ~331). So freecb must fire
    // exactly once with the *old* label.
    writeln!(
        &mut out,
        "\n=== step 2: ADD_REPLACE over existing terminal — old payload freed ==="
    )
    .unwrap();
    // SAFETY: `trie` is live; term/label bytes valid for the call.
    let rc = unsafe { insert_with_payload(trie, "apple", "payload-apple-v2") };
    writeln!(&mut out, "insert({:?}, {:?}) -> rc={rc}", "apple", "payload-apple-v2").unwrap();
    out.push_str(&dump_with_payloads(trie));
    out.push_str(&drain_freecb_log());

    // Step 3 — delete leaf "ape". The optimize sweep at the end of
    // `TrieNode_Delete` (`__trieNode_optimizeChildren` -> `TrieNode_Free`)
    // physically frees the leaf node, which fires freecb on its payload.
    // The parent ("ap" non-terminal) then single-child-merges with "ple",
    // but neither of those nodes carries a payload — so the freecb count
    // here is exactly 1.
    writeln!(
        &mut out,
        "\n=== step 3: delete leaf \"ape\" — physical removal fires freecb ==="
    )
    .unwrap();
    // SAFETY: `trie` is live; term bytes valid for the call.
    let r = unsafe { delete(trie, "ape") };
    writeln!(&mut out, "Trie_Delete(\"ape\") -> {r}").unwrap();
    out.push_str(&dump_with_payloads(trie));
    out.push_str(&drain_freecb_log());

    // Step 4 — delete "apple". After step 3's optimize sweep, "apple" is now
    // a child under a merged node, but importantly its delete is still a
    // *leaf* delete (it has no children of its own), so it too physically
    // frees and fires freecb on the v2 payload installed in step 2.
    writeln!(
        &mut out,
        "\n=== step 4: delete leaf \"apple\" — physical removal fires freecb on v2 payload ==="
    )
    .unwrap();
    // SAFETY: `trie` is live; term bytes valid for the call.
    let r = unsafe { delete(trie, "apple") };
    writeln!(&mut out, "Trie_Delete(\"apple\") -> {r}").unwrap();
    out.push_str(&dump_with_payloads(trie));
    out.push_str(&drain_freecb_log());

    // Step 5 — destroy the trie. `TrieType_Free -> TrieNode_Free` recursively
    // frees every surviving payload (here: just "banana"). The freecb log
    // captures that one surviving payload going away as the trie tears down.
    writeln!(
        &mut out,
        "\n=== step 5: TrieType_Free — all remaining payloads freed in tree order ==="
    )
    .unwrap();
    // SAFETY: `trie` was created by `NewTrie` and not freed elsewhere.
    unsafe { TrieType_Free(trie as *mut c_void) };
    out.push_str(&drain_freecb_log());

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_payload_roundtrip_and_freecb_lifecycle", out); }
    );
}
