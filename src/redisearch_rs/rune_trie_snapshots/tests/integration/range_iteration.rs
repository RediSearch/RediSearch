/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Snapshot the C rune-trie's lexicographic-range iteration
//! (`Trie_IterateRange`).
//!
//! Unlike `Trie_IterateAll` / `Trie_Iterate`, the range iterator is *push*-style:
//! it invokes a user `TrieRangeCallback` per matched terminal and unwinds on a
//! non-zero return (see `rangeIterateSubTree` in `src/trie/trie_node.c`, which
//! propagates `REDISEARCH_ERR` back up the recursion). This is the entry point
//! used by `[lex foo bar]` query nodes (`src/query.c:912`).
//!
//! Parameter conventions (from `TrieNode_IterateRange`'s doc comment):
//!   - `min`/`max` are rune buffers; `minlen`/`maxlen` are signed ints.
//!   - `minlen == -1` (with `min == NULL`) means "no lower bound"; same for max.
//!   - `includeMin`/`includeMax` toggle whether the endpoint itself is yielded
//!     when it happens to be a terminal in the trie.
//!
//! The result *order* is part of the contract: the C implementation walks the
//! tree in lex order via `rangeIterate` + `rangeIterateSubTree`, so the
//! snapshot's row order is what we're pinning. A port that yields the right set
//! but in a different order is a regression.

use std::cell::RefCell;
use std::ffi::c_void;
use std::fmt::Write as _;
use std::ptr;

use ffi::{
    NewTrie, Trie, TrieSortMode_Trie_Sort_Lex, TrieType_Free, Trie_InsertStringBuffer,
    Trie_IterateRange, Trie_Size, rune,
};
use libc::{c_char, c_int};

/// Per-invocation record. The dump preserves push order (no sorting) because
/// that traversal order is part of what the snapshot pins.
struct RangeRecord {
    term: String,
    num_docs: usize,
}

thread_local! {
    /// Push log. Each entry is one `TrieRangeCallback` invocation.
    static RANGE_LOG: RefCell<Vec<RangeRecord>> = const { RefCell::new(Vec::new()) };
    /// Optional halt threshold: if `Some(n)`, the callback returns non-zero on
    /// its `n`-th invocation. Used by the stop-semantics scenario.
    static STOP_AFTER: RefCell<Option<usize>> = const { RefCell::new(None) };
}

/// Decode a slice of runes (BMP-only u16 codepoints) to a Rust String.
unsafe fn runes_to_string(ptr: *const rune, len: usize) -> String {
    // SAFETY: caller passes a valid rune buffer of length `len`.
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    String::from_utf16(slice).expect("trie runes are valid BMP UTF-16")
}

/// The `TrieRangeCallback` we hand to every `Trie_IterateRange` call. We
/// ignore `ctx` and `payload` (the range path always passes `NULL` for
/// payload — see `rangeIterate` lines 877-880 in `src/trie/trie_node.c`).
/// Return convention: `REDISEARCH_OK` (= 0) to continue, anything else to halt.
unsafe extern "C" fn capture_range_cb(
    str_ptr: *const rune,
    len: usize,
    _ctx: *mut c_void,
    _payload: *mut c_void,
    num_docs: usize,
) -> c_int {
    // SAFETY: callers in the C trie pass a valid rune buffer of `len` runes.
    let term = unsafe { runes_to_string(str_ptr, len) };
    RANGE_LOG.with(|log| log.borrow_mut().push(RangeRecord { term, num_docs }));
    // Decide whether to halt by reading the threshold *after* recording the
    // entry, so the "stop after Nth call" scenario logs exactly N entries
    // before the iterator unwinds.
    STOP_AFTER.with(|stop| match *stop.borrow() {
        Some(n) if RANGE_LOG.with(|log| log.borrow().len()) >= n => 1,
        _ => 0,
    })
}

/// Drain the log and render it in invocation order. Empty input emits a
/// distinct line so the snapshot still pins "no matches" outcomes.
fn drain_range_log() -> String {
    let entries: Vec<RangeRecord> = RANGE_LOG.with(|log| std::mem::take(&mut *log.borrow_mut()));
    if entries.is_empty() {
        return "  <no matches>\n".into();
    }
    let mut out = String::new();
    for r in entries {
        writeln!(&mut out, "  {:10}  numDocs={}", r.term, r.num_docs).unwrap();
    }
    out
}

/// Insert a UTF-8 term into a freshly-created Lex-mode trie. No payload, no
/// score variation — the range iterator yields lex-sorted hits regardless.
unsafe fn insert(trie: *mut Trie, term: &str) {
    // SAFETY: `term.as_bytes()` is a valid borrow for the duration of the call.
    // `Trie_InsertStringBuffer` copies the input internally before returning.
    unsafe {
        Trie_InsertStringBuffer(
            trie,
            term.as_ptr() as *const c_char,
            term.len(),
            1.0,
            0, // ADD_REPLACE
            ptr::null_mut(),
            1,
        );
    }
}

/// Build the shared fixture: a mix of overlapping prefixes (`band`/`bandana`/
/// `banana`, `apple`/`apricot`) plus standalone terms (`cherry`, `date`). The
/// internal-terminal `band` is the interesting case for inclusive/exclusive
/// boundary handling.
fn build_fixture(trie: *mut Trie) {
    for term in [
        "apple", "apricot", "banana", "band", "bandana", "cherry", "date",
    ] {
        // SAFETY: `trie` is live for the duration of this function.
        unsafe { insert(trie, term) };
    }
}

/// Encode a Rust `&str` as a heap-allocated rune (UTF-16) buffer plus length,
/// suitable for passing as `(min, minlen)` / `(max, maxlen)` to
/// `Trie_IterateRange`. Returns `None` for the sentinel "no bound" case so the
/// caller can pass `(NULL, -1)` instead.
fn encode_runes(s: Option<&str>) -> Option<Vec<rune>> {
    s.map(|s| s.encode_utf16().collect::<Vec<_>>())
}

/// Run one `Trie_IterateRange` query and append a labeled block to `out`.
///
/// `min`/`max` of `None` translates to the `(NULL, -1)` sentinel for that side.
fn run_range_query(
    trie: *mut Trie,
    label: &str,
    min: Option<&str>,
    include_min: bool,
    max: Option<&str>,
    include_max: bool,
    out: &mut String,
) {
    writeln!(
        out,
        "query: {label}  min={:?} (inc={include_min})  max={:?} (inc={include_max})",
        min.unwrap_or("<none>"),
        max.unwrap_or("<none>"),
    )
    .unwrap();

    let min_buf = encode_runes(min);
    let max_buf = encode_runes(max);

    let (min_ptr, min_len) = match &min_buf {
        Some(b) => (b.as_ptr(), b.len() as c_int),
        None => (ptr::null(), -1),
    };
    let (max_ptr, max_len) = match &max_buf {
        Some(b) => (b.as_ptr(), b.len() as c_int),
        None => (ptr::null(), -1),
    };

    // SAFETY: `trie` is live; the rune buffers (if any) outlive the call; the
    // callback is a `'static` extern "C" function with no lifetime concerns.
    unsafe {
        Trie_IterateRange(
            trie,
            min_ptr,
            min_len,
            include_min,
            max_ptr,
            max_len,
            include_max,
            Some(capture_range_cb),
            ptr::null_mut(),
        );
    }

    out.push_str(&drain_range_log());
}

/// Render the fixture's size + a blank-separated batch of range queries.
fn run(queries: &[(&str, Option<&str>, bool, Option<&str>, bool)]) -> String {
    // SAFETY: NULL freecb is fine — no payloads in this fixture.
    let trie = unsafe { NewTrie(None, TrieSortMode_Trie_Sort_Lex) };
    build_fixture(trie);

    // SAFETY: `trie` is live for the duration of this function.
    let size = unsafe { Trie_Size(trie) };
    let mut out = format!("size: {size}\n\n");

    for (i, (label, min, inc_min, max, inc_max)) in queries.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        run_range_query(trie, label, *min, *inc_min, *max, *inc_max, &mut out);
    }

    // SAFETY: `trie` was created by `NewTrie` above and not freed elsewhere.
    unsafe { TrieType_Free(trie as *mut c_void) };
    out
}

#[test]
fn lex_range_inclusive_both_ends() {
    // Closed interval [apple, cherry]. Endpoints are both terms in the fixture,
    // so the snapshot pins that both surface.
    let dump = run(&[(
        "[apple, cherry] inclusive",
        Some("apple"),
        true,
        Some("cherry"),
        true,
    )]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_range_inclusive_both_ends", dump); }
    );
}

#[test]
fn lex_range_exclusive_both_ends() {
    // Open interval (apple, cherry). `apple` and `cherry` themselves must drop;
    // `apricot`, `banana`, `band`, `bandana` should remain.
    let dump = run(&[(
        "(apple, cherry) exclusive",
        Some("apple"),
        false,
        Some("cherry"),
        false,
    )]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_range_exclusive_both_ends", dump); }
    );
}

#[test]
fn lex_range_mixed_inclusivity() {
    // Asymmetric — one endpoint inclusive, the other exclusive. Two queries
    // back-to-back so the snapshot shows both halves of the mirror.
    let dump = run(&[
        (
            "[apple, cherry)",
            Some("apple"),
            true,
            Some("cherry"),
            false,
        ),
        (
            "(apple, cherry]",
            Some("apple"),
            false,
            Some("cherry"),
            true,
        ),
    ]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_range_mixed_inclusivity", dump); }
    );
}

#[test]
fn lex_range_one_sided_min_only() {
    // Half-open from "b" upward. `(NULL, -1)` on the max side disables the
    // upper bound entirely — see `nmax > 0` guard in `rangeIterate`.
    let dump = run(&[("[b, +inf)", Some("b"), true, None, true)]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_range_one_sided_min_only", dump); }
    );
}

#[test]
fn lex_range_one_sided_max_only() {
    // Mirror of the previous test: bounded above, unbounded below.
    let dump = run(&[("(-inf, b]", None, true, Some("b"), true)]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_range_one_sided_max_only", dump); }
    );
}

#[test]
fn lex_range_unbounded() {
    // Both sides `(NULL, -1)`. Should walk the entire trie in lex order —
    // structurally identical to `Trie_IterateAll`'s output, but exercised
    // through the range path's recursion.
    let dump = run(&[("(-inf, +inf)", None, true, None, true)]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_range_unbounded", dump); }
    );
}

#[test]
fn lex_range_empty() {
    // Range "[ze, zz]" contains no fixture terms. The callback should never
    // fire; the snapshot's `<no matches>` line pins that.
    let dump = run(&[("[ze, zz]", Some("ze"), true, Some("zz"), true)]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_range_empty", dump); }
    );
}

#[test]
fn lex_range_min_equals_max() {
    // Degenerate range — single point. `Trie_IterateRange` short-circuits at
    // line 1005-1013 of `trie_node.c`: it `TrieNode_Get`'s the exact key and
    // fires the callback iff that node is a real terminal *and* at least one
    // of the inclusivity flags is set.
    let dump = run(&[
        ("[band, band]", Some("band"), true, Some("band"), true),
        ("(band, band)", Some("band"), false, Some("band"), false),
        ("[nope, nope]", Some("nope"), true, Some("nope"), true),
    ]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_range_min_equals_max", dump); }
    );
}

#[test]
fn lex_range_min_greater_than_max() {
    // Inverted range. `Trie_IterateRange` returns immediately when `runecmp(min,
    // max) > 0` (line 1000-1003) — we snapshot to confirm "silent empty"
    // rather than a panic or garbage callback fire.
    let dump = run(&[("[cherry, apple] inverted", Some("cherry"), true, Some("apple"), true)]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_range_min_greater_than_max", dump); }
    );
}

#[test]
fn lex_range_callback_stops_early() {
    // Unbounded range so the iterator would visit every fixture term, but we
    // halt the callback after its 3rd fire. The snapshot pins:
    //   - exactly 3 records logged
    //   - the iterator unwinds cleanly (no panic, no leak)
    // The 3rd return-non-zero propagates `REDISEARCH_ERR` up through
    // `rangeIterateSubTree`, which short-circuits all further sibling/child
    // recursion (see line 855-857).
    STOP_AFTER.with(|s| *s.borrow_mut() = Some(3));
    let dump = run(&[("(-inf, +inf) stop after 3rd", None, true, None, true)]);
    // Reset for the next test on this thread (cargo gives each test its own
    // thread by default, but explicit reset is cheap insurance).
    STOP_AFTER.with(|s| *s.borrow_mut() = None);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_range_callback_stops_early", dump); }
    );
}
