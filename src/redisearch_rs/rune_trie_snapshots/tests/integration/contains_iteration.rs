/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Snapshot the C rune-trie's "contains" iteration (`Trie_IterateContains`).
//!
//! The `(prefix, suffix)` flag pair selects one of four modes; in production
//! the four are used through distinct call sites:
//!
//!   - `(true, false)`  — pure prefix walk ("str*"), used by every prefix
//!     iterator over a regular term trie.
//!   - `(false, true)`  — pure suffix walk ("*str"); only meaningful on a
//!     *suffix* trie (see `src/suffix.c`) because the matching loop expects
//!     reversed children to expose suffix paths. On a plain terms trie this
//!     mode only fires the callback for nodes whose stored string ends with
//!     the pattern *as a node label* — typically nothing.
//!   - `(true, true)`   — contains ("*str*"), also a suffix-trie construct.
//!   - `(false, false)` — exact match; short-circuits to `TrieNode_Get` and a
//!     single callback fire if the term exists and `score != 0`.
//!
//! The latter three modes share a recursive `containsIterate` walk that
//! supports a `struct timespec` timeout; we pass `NULL`/`skipTimeoutChecks=true`
//! everywhere here so the timeout codepath is bypassed (see
//! `REDISEARCH_UNINITIALIZED` branch at line 1051 of `src/trie/trie_node.c`).

use std::cell::RefCell;
use std::ffi::c_void;
use std::fmt::Write as _;
use std::ptr;

use ffi::{
    NewTrie, Trie, Trie_IterateContains, Trie_Size, TrieSortMode_Trie_Sort_Lex, TrieType_Free, rune,
};
use libc::c_int;

use crate::support::{encode_runes, runes_to_string, trie_insert};

struct ContainsRecord {
    term: String,
    num_docs: usize,
}

thread_local! {
    /// Push log. Order matters — `containsIterate` recurses depth-first, and
    /// that order is what the snapshot pins.
    static CONTAINS_LOG: RefCell<Vec<ContainsRecord>> = const { RefCell::new(Vec::new()) };
}

/// `TrieRangeCallback`. The contains path passes `NULL` payload for prefix-mode
/// matches and `n->payload` for wildcard/suffix-mode matches; we don't insert
/// payloads here so the field is always `NULL`. Returns `REDISEARCH_OK` (0) so
/// iteration runs to completion.
unsafe extern "C" fn capture_contains_cb(
    str_ptr: *const rune,
    len: usize,
    _ctx: *mut c_void,
    _payload: *mut c_void,
    num_docs: usize,
) -> c_int {
    // SAFETY: callers in the C trie pass a valid rune buffer of `len` runes.
    let term = unsafe { runes_to_string(str_ptr, len) };
    CONTAINS_LOG.with(|log| log.borrow_mut().push(ContainsRecord { term, num_docs }));
    0
}

fn drain_contains_log() -> String {
    let entries: Vec<ContainsRecord> =
        CONTAINS_LOG.with(|log| std::mem::take(&mut *log.borrow_mut()));
    if entries.is_empty() {
        return "  <no matches>\n".into();
    }
    let mut out = String::new();
    for r in entries {
        writeln!(&mut out, "  {:10}  numDocs={}", r.term, r.num_docs).unwrap();
    }
    out
}

/// Fixture chosen so each contains-mode bucket gets a distinct set of hits:
///
///   - prefix `"cat"` → `cat`, `catalog`, `category`
///   - suffix `"cat"` → `cat`, `concat`, `scat` (only if you reversed the trie)
///   - contains `"cat"` → all six terms
///
/// On a plain terms trie the suffix/contains modes don't actually traverse the
/// reverse direction; the snapshot pins whatever the recursive walk surfaces.
fn build_fixture(trie: *mut Trie) {
    for term in ["cat", "catalog", "category", "concat", "scat", "scatter"] {
        // SAFETY: `trie` is live for the duration of this function.
        unsafe { trie_insert(trie, term) };
    }
}

/// Run one `Trie_IterateContains` query and append a labeled block. All four
/// `(prefix, suffix)` combinations route through this helper.
fn run_contains_query(
    trie: *mut Trie,
    label: &str,
    pattern: &str,
    prefix: bool,
    suffix: bool,
    out: &mut String,
) {
    writeln!(
        out,
        "query: {label}  str={pattern:?}  prefix={prefix}  suffix={suffix}"
    )
    .unwrap();

    let buf = encode_runes(pattern);
    let (ptr_runes, len_runes) = (buf.as_ptr(), buf.len() as c_int);

    // SAFETY: `trie` is live; `buf` outlives the call; we pass `timeout=NULL`
    // and `skipTimeoutChecks=true` to bypass the timeout codepath entirely
    // (the C trie reads `REDISEARCH_UNINITIALIZED` from the counter and skips
    // every `clock_gettime` call).
    unsafe {
        Trie_IterateContains(
            trie,
            ptr_runes,
            len_runes,
            prefix,
            suffix,
            Some(capture_contains_cb),
            ptr::null_mut(),
            ptr::null_mut(),
            true,
        );
    }

    out.push_str(&drain_contains_log());
}

/// `(label, pattern, prefix, suffix)` — runs each tuple back-to-back against a
/// freshly-built fixture and concatenates the blocks for one snapshot.
fn run(queries: &[(&str, &str, bool, bool)]) -> String {
    // SAFETY: NULL freecb is fine — no payloads in this fixture.
    let trie = unsafe { NewTrie(None, TrieSortMode_Trie_Sort_Lex) };
    build_fixture(trie);

    // SAFETY: `trie` is live.
    let size = unsafe { Trie_Size(trie) };
    let mut out = format!("size: {size}\n\n");

    for (i, (label, pattern, prefix, suffix)) in queries.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        run_contains_query(trie, label, pattern, *prefix, *suffix, &mut out);
    }

    // SAFETY: `trie` was created by `NewTrie` above and not freed elsewhere.
    unsafe { TrieType_Free(trie as *mut c_void) };
    out
}

#[test]
fn lex_contains_pure_prefix() {
    // `(prefix=true, suffix=false)` — pure "starts-with" walk. The path goes
    // through `TrieNode_Get` to descend to the subtree root, then
    // `rangeIterateSubTree` for the full sub-lex traversal.
    let dump = run(&[("starts-with \"cat\"", "cat", true, false)]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_contains_pure_prefix", dump); }
    );
}

#[test]
fn lex_contains_pure_suffix() {
    // `(prefix=false, suffix=true)` — production callers only invoke this
    // mode on a suffix trie where children are stored reversed. On a plain
    // terms trie the walk still recurses, but the "is suffix match" check at
    // line 1125 (`TrieNode_IsTerminal(n) && localOffset + 1 == n->len`) only
    // fires for nodes that happen to terminate exactly at the pattern's last
    // rune in a forward orientation. The snapshot pins whatever the walk
    // actually surfaces.
    let dump = run(&[("ends-with \"cat\"", "cat", false, true)]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_contains_pure_suffix", dump); }
    );
}

#[test]
fn lex_contains_substring() {
    // `(prefix=true, suffix=true)` — contains mode. On a suffix trie this
    // would match every term containing "cat" anywhere; on a plain terms trie
    // it covers prefix-style hits and whatever the matching state-machine
    // surfaces beyond those. Snapshot, don't predict.
    let dump = run(&[("substring \"cat\"", "cat", true, true)]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_contains_substring", dump); }
    );
}

#[test]
fn lex_contains_exact() {
    // `(prefix=false, suffix=false)` — the fast-path at line 1038-1044: a
    // single `TrieNode_Get` + one callback fire iff the node exists and
    // `score != 0`. `"cat"` is a terminal (inserted with score=1.0) so it
    // surfaces; `"catalo"` is an internal non-terminal so it shouldn't.
    let dump = run(&[
        ("exact \"cat\"", "cat", false, false),
        ("exact \"catalo\"", "catalo", false, false),
        ("exact \"nope\"", "nope", false, false),
    ]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_contains_exact", dump); }
    );
}

#[test]
fn lex_contains_no_match() {
    // No fixture term contains "zzz" anywhere, in any orientation.
    let dump = run(&[
        ("starts-with \"zzz\"", "zzz", true, false),
        ("ends-with \"zzz\"", "zzz", false, true),
        ("substring \"zzz\"", "zzz", true, true),
        ("exact \"zzz\"", "zzz", false, false),
    ]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_contains_no_match", dump); }
    );
}

#[test]
fn lex_contains_empty_pattern() {
    // Empty-prefix is the only well-defined empty-pattern case — the suffix
    // and contains modes read `origStr[0]` unconditionally (line 1115 of
    // `trie_node.c`), which is UB on an empty buffer.
    //
    // The snapshot pins an unexpected outcome: `TrieNode_Get` with `len=0`
    // never enters its `while (n && offset < len)` loop and falls through to
    // `return NULL` (line 411 of `trie_node.c`). The `if (res)` guard in
    // `TrieNode_IterateContains` then skips the subtree walk entirely. So
    // empty-prefix → zero matches, NOT "every term" as a naive read of the
    // prefix semantics would suggest. A port that returns "all terms" on
    // empty input is wrong by this contract.
    let dump = run(&[("empty-prefix", "", true, false)]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_contains_empty_pattern", dump); }
    );
}
