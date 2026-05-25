/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Snapshot the C rune-trie's behavior at the input-length and codepoint
//! boundaries that a Rust port is likely to handle differently.
//!
//! Length guards live in two places (`src/trie/trie.c`):
//!
//!   - `Trie_InsertStringBuffer` rejects `len > TRIE_INITIAL_STRING_LEN * sizeof(rune)`
//!     i.e. > 512 *bytes*, before UTF-8 → rune conversion.
//!   - `Trie_InsertRune` (called after conversion) requires `len > 0 &&
//!     len < TRIE_INITIAL_STRING_LEN`, i.e. 1 ≤ runes < 256.
//!
//! Each guard rejects independently:
//!   - empty input  -> rejected by `len > 0` in `Trie_InsertRune`
//!   - 255 ASCII    -> accepted (255 bytes / 255 runes)
//!   - 256 ASCII    -> accepted byte-check (256 ≤ 512), rejected rune-check (`< 256` is false)
//!   - 513 ASCII    -> rejected by byte-check before rune conversion ever runs
//!
//! Rejection returns 0 (TRIE_OK_UPDATED), accepted-new returns 1 (TRIE_OK_NEW).
//! The snapshot prints the rc so a port that returns a different code (e.g.
//! `-1` on overflow, or panics) will produce a diff.
//!
//! Non-BMP codepoints are **truncated**, not surrogate-pair-encoded. The C
//! `rune` typedef is `uint16_t` and `strToRunesN` (rune_util.c) does a plain
//! `(rune)cp` cast on each decoded codepoint, dropping the high bits. So
//! `"🦀"` (U+1F980, UTF-8: F0 9F A6 80) decodes to codepoint 0x1F980 and is
//! then stored as the single rune 0xF980 — which happens to be a valid BMP
//! codepoint (CJK Compatibility Ideograph, rendered "呂"). Two consequences:
//!
//!   - **Aliasing**: any non-BMP codepoint whose low 16 bits collide with a
//!     real BMP codepoint becomes indistinguishable from it. The second
//!     scenario inserts `"🦀"` (U+1F980) and `"\u{F980}"` (the actual BMP
//!     compat ideograph) and snapshots that the second insert is treated as
//!     an UPDATE (rc=0), not a NEW (rc=1).
//!   - **Surrogate halves are unreachable**: a Rust port using `char`
//!     (Unicode scalar values) cannot produce isolated surrogate values
//!     (D800–DFFF are not valid scalars), but the C trie can store them
//!     fine — they're just `u16` keys.
//!
//! A Rust port keying on `char` would either:
//!
//!   - Store U+1F980 verbatim (no aliasing) — differs from C.
//!   - Truncate explicitly (matches C) — needs a `(c as u32 as u16)` step.
//!
//! Either way, this snapshot pins the choice as a deliberate decision.

use std::ffi::c_void;
use std::fmt::Write as _;
use std::ptr;

use ffi::{
    NewTrie, RSPayload, Trie, Trie_InsertStringBuffer, Trie_IterateAll, Trie_Size,
    TrieIterator_Free, TrieIterator_Next, TrieSortMode_Trie_Sort_Lex, TrieType_Free, rune, t_len,
};
use libc::c_char;

unsafe fn runes_to_string(ptr: *const rune, len: usize) -> String {
    // SAFETY: caller passes a valid rune buffer of length `len`.
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    // `from_utf16` handles surrogate pairs by combining them into the
    // corresponding non-BMP Unicode scalar value. Lone surrogates would
    // error — none of our test inputs produce those.
    String::from_utf16(slice).expect("trie runes form valid UTF-16")
}

/// Insert a UTF-8 term with a trivial score/numDocs. Returns the C `rc`
/// (1 = TRIE_OK_NEW, 0 = TRIE_OK_UPDATED / rejected).
unsafe fn insert(trie: *mut Trie, term: &str) -> i32 {
    // SAFETY: `term.as_bytes()` valid for the call; the C trie copies the
    // buffer internally before returning.
    unsafe {
        Trie_InsertStringBuffer(
            trie,
            term.as_ptr() as *const c_char,
            term.len(),
            1.0,
            0, // ADD_REPLACE
            ptr::null_mut(),
            1,
        )
    }
}

/// Dump every terminal entry. To keep snapshots reviewable we print the
/// term's *rune length* and a short head/tail preview rather than the full
/// content when the term is long.
fn dump_all(trie: *mut Trie) -> String {
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

        // For terms ≤ 16 chars, print verbatim. Beyond that, summarize with
        // length + head/tail — full content would make the snapshot noisy
        // without adding signal.
        let display = if term.chars().count() <= 16 {
            format!("{term:?}")
        } else {
            let head: String = term.chars().take(4).collect();
            let tail: String = term
                .chars()
                .rev()
                .take(4)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect();
            format!("<{}-rune term: {head:?}..{tail:?}>", rune_len)
        };

        writeln!(&mut out, "  runeLen={rune_len:3}  {display}").unwrap();
    }

    // SAFETY: `it` was produced by `Trie_IterateAll` above and not freed yet.
    unsafe { TrieIterator_Free(it) };
    out
}

#[test]
fn lex_input_length_boundaries() {
    // SAFETY: NULL freecb matches the terms-trie construction in `src/spec.c`
    // (these terms carry no payload).
    let trie = unsafe { NewTrie(None, TrieSortMode_Trie_Sort_Lex) };

    let mut out = String::new();

    // Each case: (label describing the boundary, term, expected guard outcome).
    // The "expected" string is for snapshot readers — the actual outcome is
    // taken from the C-returned rc.
    struct Case {
        label: &'static str,
        term: String,
        expected: &'static str,
    }

    let cases: Vec<Case> = vec![
        Case {
            label: "empty string",
            term: String::new(),
            expected: "rejected by Trie_InsertRune (len > 0 required)",
        },
        Case {
            label: "single ASCII rune",
            term: "a".to_string(),
            expected: "accepted (minimum non-empty input)",
        },
        Case {
            label: "255-rune ASCII (one below TRIE_INITIAL_STRING_LEN)",
            term: "x".repeat(255),
            expected: "accepted (255 < 256)",
        },
        Case {
            label: "256-rune ASCII (at TRIE_INITIAL_STRING_LEN — strict `<`)",
            term: "y".repeat(256),
            expected: "rejected by Trie_InsertRune (256 not strictly < 256)",
        },
        Case {
            label: "513-byte ASCII (one above TRIE_INITIAL_STRING_LEN * sizeof(rune))",
            term: "z".repeat(513),
            expected: "rejected by Trie_InsertStringBuffer (513 > 512 bytes)",
        },
    ];

    for case in &cases {
        // SAFETY: `trie` is live; term bytes valid for the call.
        let rc = unsafe { insert(trie, &case.term) };
        writeln!(
            &mut out,
            "--- {} (bytes={}, expected: {}) ---",
            case.label,
            case.term.len(),
            case.expected,
        )
        .unwrap();
        writeln!(&mut out, "rc = {rc}").unwrap();
    }

    // One consolidated dump at the end so the snapshot shows which inputs
    // actually landed. With per-step dumps the file would balloon — the only
    // structural signal here is which terms survived.
    writeln!(&mut out, "\n--- final trie contents ---").unwrap();
    out.push_str(&dump_all(trie));

    // SAFETY: `trie` was created by `NewTrie` above.
    unsafe { TrieType_Free(trie as *mut c_void) };

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_input_length_boundaries", out); }
    );
}

#[test]
fn lex_non_bmp_codepoint_truncation_and_aliasing() {
    // SAFETY: NULL freecb.
    let trie = unsafe { NewTrie(None, TrieSortMode_Trie_Sort_Lex) };

    // Each tuple: (UTF-8 term, label describing what the C trie does with it).
    //
    // Codepoints involved:
    //   "a"          = U+0061              (BMP, 1 rune)
    //   "🦀"         = U+1F980              (non-BMP, truncated to 1 rune = 0xF980)
    //   "\u{F980}"   = U+F980               (BMP CJK compat ideograph, 1 rune = 0xF980)
    //   "ab"         = U+0061 + U+0062     (2 runes)
    //   "a🦀"        = U+0061 + U+1F980    (truncated to 2 runes: 0061 F980)
    //
    // The 🦀 vs \u{F980} pair is the punch line: they map to the same rune
    // and therefore *alias* in the C trie. A Rust port keying on USVs
    // (`char`) would store them as distinct entries.
    let cases: &[(&str, &str)] = &[
        ("🦀", "non-BMP — codepoint U+1F980 truncated to rune 0xF980"),
        ("a", "ASCII baseline"),
        (
            "\u{F980}",
            "BMP U+F980 directly — collides with truncated 🦀",
        ),
        ("ab", "ASCII baseline 2"),
        ("a🦀", "BMP + non-BMP — second rune truncated to 0xF980"),
    ];

    let mut out = String::new();
    writeln!(&mut out, "--- inserts ---").unwrap();
    for (term, why) in cases {
        // SAFETY: `trie` is live; term bytes valid for the call.
        let rc = unsafe { insert(trie, term) };
        // `utf16Runes` is what a UTF-16-keyed (surrogate-pair) port would
        // produce — *not* what the C trie actually stores. Printing it
        // alongside lets the snapshot reader see the divergence in one glance.
        let utf16_runes = term.encode_utf16().count();
        writeln!(
            &mut out,
            "insert({term:?})  utf8bytes={}  rustUtf16Runes={}  rc={rc}  ({why})",
            term.len(),
            utf16_runes,
        )
        .unwrap();
    }

    writeln!(
        &mut out,
        "\n--- iteration (entries are by raw u16; 🦀 and U+F980 collapse) ---"
    )
    .unwrap();
    out.push_str(&dump_all(trie));

    // SAFETY: `trie` was created by `NewTrie` above.
    unsafe { TrieType_Free(trie as *mut c_void) };

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_non_bmp_codepoint_truncation_and_aliasing", out); }
    );
}
