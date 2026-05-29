/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Snapshot the C rune-trie's wildcard iteration (`Trie_IterateWildcard`).
//!
//! Pattern syntax (from `Wildcard_MatchRune` in `src/wildcard/wildcard.c`):
//!
//!   - `*` matches zero or more runes.
//!   - `?` matches exactly one rune.
//!   - Every other rune is treated as a literal — **including** `\\`. The
//!     suffix-trie path in `src/suffix.c` strips backslashes with
//!     `Wildcard_RemoveEscape` *before* calling the matcher, but the matcher
//!     itself has no escape handling. So in this iterator a `\*` pattern means
//!     "literal backslash, then star-wildcard" (anything starting with `\`),
//!     not "literal `*`". The snapshot pins that observable behavior.
//!
//! Per-node trie integration is in `wildcardIterate` (`src/trie/trie_node.c`):
//! a depth-first walk where each node calls `Wildcard_MatchRune(pattern, buf)`
//! on the accumulated path. `FULL_MATCH` on a terminal fires the callback;
//! `PARTIAL_MATCH` recurses into children; `NO_MATCH` prunes the subtree.
//!
//! The `prefix` flag in the iterator's `RangeCtx` is set when the *last* rune
//! of the pattern is `*` (line 1204) — in that case, the matcher's
//! `FULL_MATCH` triggers a full `rangeIterateSubTree` rather than just the
//! single node, so descendants under the matched prefix all fire too.

use std::cell::RefCell;
use std::ffi::c_void;
use std::fmt::Write as _;
use std::ptr;

use ffi::{
    NewTrie, Trie, Trie_IterateWildcard, Trie_Size, TrieSortMode_Trie_Sort_Lex, TrieType_Free, rune,
};
use libc::{c_int};

use crate::support::{encode_runes, runes_to_string, trie_insert};

struct WildcardRecord {
    term: String,
    num_docs: usize,
}

thread_local! {
    /// Push log. `wildcardIterate` recurses depth-first; the order of records
    /// is part of the snapshot's contract.
    static WILDCARD_LOG: RefCell<Vec<WildcardRecord>> = const { RefCell::new(Vec::new()) };
}

/// `TrieRangeCallback`. Wildcard matches in non-`prefix` mode pass `n->payload`
/// rather than `NULL` (line 1172 of `trie_node.c`); we don't insert payloads
/// here so the field is unused. Returns `REDISEARCH_OK` (0) to keep iterating.
unsafe extern "C" fn capture_wildcard_cb(
    str_ptr: *const rune,
    len: usize,
    _ctx: *mut c_void,
    _payload: *mut c_void,
    num_docs: usize,
) -> c_int {
    // SAFETY: callers in the C trie pass a valid rune buffer of `len` runes.
    let term = unsafe { runes_to_string(str_ptr, len) };
    WILDCARD_LOG.with(|log| log.borrow_mut().push(WildcardRecord { term, num_docs }));
    0
}

fn drain_wildcard_log() -> String {
    let entries: Vec<WildcardRecord> =
        WILDCARD_LOG.with(|log| std::mem::take(&mut *log.borrow_mut()));
    if entries.is_empty() {
        return "  <no matches>\n".into();
    }
    let mut out = String::new();
    for r in entries {
        writeln!(&mut out, "  {:10}  numDocs={}", r.term, r.num_docs).unwrap();
    }
    out
}

/// Fixture: three-letter `?at` candidates (`cat`/`bat`/`rat`), the longer
/// `category`/`concat` to differentiate `*at` vs `?at`, and `car`/`cab` so
/// `ca*` has multiple hits.
fn build_fixture(trie: *mut Trie) {
    for term in ["cat", "car", "cab", "bat", "rat", "category", "concat"] {
        // SAFETY: `trie` is live for the duration of this function.
        unsafe { trie_insert(trie, term) };
    }
}

/// Run one `Trie_IterateWildcard` query and append a labeled block. Patterns
/// here must be non-empty: `Trie_IterateWildcard` reads `str[nstr - 1]` at line
/// 1204 of `src/trie/trie_node.c` to decide its `prefix` flag, so a zero-length
/// pattern would read `str[-1]` (UB).
fn run_wildcard_query(trie: *mut Trie, label: &str, pattern: &str, out: &mut String) {
    assert!(
        !pattern.is_empty(),
        "wildcard pattern must be non-empty (str[-1] read)"
    );

    writeln!(out, "query: {label}  pattern={pattern:?}").unwrap();

    let buf = encode_runes(pattern);
    let (ptr_runes, len_runes) = (buf.as_ptr(), buf.len() as c_int);

    // SAFETY: `trie` is live; `buf` outlives the call; `timeout=NULL` +
    // `skipTimeoutChecks=true` bypasses the timeout codepath (the C trie reads
    // `REDISEARCH_UNINITIALIZED` from the counter and skips `clock_gettime`).
    unsafe {
        Trie_IterateWildcard(
            trie,
            ptr_runes,
            len_runes,
            Some(capture_wildcard_cb),
            ptr::null_mut(),
            ptr::null_mut(),
            true,
        );
    }

    out.push_str(&drain_wildcard_log());
}

fn run(queries: &[(&str, &str)]) -> String {
    // SAFETY: NULL freecb is fine — no payloads in this fixture.
    let trie = unsafe { NewTrie(None, TrieSortMode_Trie_Sort_Lex) };
    build_fixture(trie);

    // SAFETY: `trie` is live.
    let size = unsafe { Trie_Size(trie) };
    let mut out = format!("size: {size}\n\n");

    for (i, (label, pattern)) in queries.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        run_wildcard_query(trie, label, pattern, &mut out);
    }

    // SAFETY: `trie` was created by `NewTrie` above and not freed elsewhere.
    unsafe { TrieType_Free(trie as *mut c_void) };
    out
}

#[test]
fn lex_wildcard_star_only() {
    // Pure `*` — every node `FULL_MATCH`'s; with the `prefix` flag set (last
    // rune is `*`) this routes through `rangeIterateSubTree` for descendants
    // too. Should yield every terminal in lex order.
    let dump = run(&[("*", "*")]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_wildcard_star_only", dump); }
    );
}

#[test]
fn lex_wildcard_leading_star() {
    // `*at` — matches anything ending in `at`. `Wildcard_MatchRune` backtracks
    // via the `np_itr`/`ns_itr` pointers (line 94-95 of `wildcard.c`) so longer
    // hits like `concat` should still surface.
    let dump = run(&[("*at", "*at")]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_wildcard_leading_star", dump); }
    );
}

#[test]
fn lex_wildcard_trailing_star() {
    // `ca*` — anything starting with `ca`. Last-rune-is-`*` activates the
    // `prefix` shortcut, so `rangeIterateSubTree` walks the whole `ca`-rooted
    // subtree on the first FULL_MATCH.
    let dump = run(&[("ca*", "ca*")]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_wildcard_trailing_star", dump); }
    );
}

#[test]
fn lex_wildcard_middle_star() {
    // `c*t` — starts with `c`, ends with `t`, anything between. Should yield
    // `cat`, `concat`; `category` ends in `y` so it should drop.
    let dump = run(&[("c*t", "c*t")]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_wildcard_middle_star", dump); }
    );
}

#[test]
fn lex_wildcard_question_mark() {
    // `?at` — exactly one rune before `at`. Only the 3-rune terms qualify:
    // `cat`, `bat`, `rat`. `concat` is 6 runes so it should drop.
    let dump = run(&[("?at", "?at")]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_wildcard_question_mark", dump); }
    );
}

#[test]
fn lex_wildcard_multiple_questions() {
    // `??t` — same length filter as `?at` for this fixture, but without
    // constraining the middle character. The snapshot pins that both
    // patterns happen to yield the same set on this term list.
    let dump = run(&[("??t", "??t")]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_wildcard_multiple_questions", dump); }
    );
}

#[test]
fn lex_wildcard_escape() {
    // `\*` — the matcher has no escape handling (`Wildcard_RemoveEscape` is
    // applied only by suffix-trie callers, never by `Trie_IterateWildcard`).
    // So this means "literal `\`, then `*` (wildcard)". No fixture term starts
    // with `\` → empty. The snapshot pins that the matcher is escape-agnostic.
    let dump = run(&[("\\*", "\\*")]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_wildcard_escape", dump); }
    );
}

#[test]
fn lex_wildcard_no_match() {
    // No fixture term starts with `x`.
    let dump = run(&[("xyz", "xyz")]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_wildcard_no_match", dump); }
    );
}

// NOTE: `lex_wildcard_empty_pattern` is deliberately omitted. The first thing
// `Trie_IterateWildcard` does is `str[nstr - 1] == '*'` (`trie_node.c:1204`).
// With `nstr=0` that's a read of `str[-1]` — undefined behavior regardless of
// whether `str` points to a real allocation or to `NonNull::dangling()`. We
// pin "callers must pass a non-empty pattern" via the assertion in
// `run_wildcard_query` above instead of snapshotting UB.
