/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Snapshot the C rune-trie's DFA-filtered iteration path (`Trie_Iterate`).
//!
//! `Trie_Iterate(t, prefix, len, maxDist, prefixMode)` builds a `DFAFilter`
//! over the lowered runes of `prefix` and hands it to `TrieNode_Iterate` as
//! the per-edge `StepFilter` (`LoweringFilterFunc`) plus the matching
//! `StackPop` callback (see `src/trie/trie.c:212-227`). This is the entry
//! point for FT.SEARCH prefix/fuzzy queries (`src/query.c:617`), spell-check
//! (`src/spell_check.c:149`), `FT.DICTDUMP` (`src/dictionary.c:170`), and the
//! fork-GC terms walk (`src/fork_gc/terms.c:20`). All other rune_trie_snapshots
//! scenarios use `Trie_IterateAll`, which leaves this whole path uncovered.
//!
//! Four `(maxDist, prefixMode)` regimes:
//!
//!   - `(0, 1)` → pure prefix match
//!   - `(0, 0)` → exact match (at most one terminal)
//!   - `(d, 0)` with d>0 → Levenshtein fuzzy within edit distance d
//!   - `(d, 1)` with d>0 → fuzzy match followed by prefix continuation
//!
//! `TrieIterator_Next`'s `matchCtx` parameter is interpreted by the DFA filter
//! as `int *pdist`: on every terminal match the filter writes the matching
//! `dfaNode->distance` (clamped by `minDist` from the dist-stack), giving the
//! caller the edit distance of the term that was just yielded
//! (`src/trie/levenshtein.c:258-278`). We pass an `&mut i32` pre-seeded to
//! `maxDist + 1` — the same sentinel `Trie_Search` uses (`src/trie/trie.c:256`)
//! — and dump it alongside each result so a port whose DFA scoring diverges
//! produces a snapshot diff. For the `maxDist=0` regimes the column is still
//! captured: every match should land at distance 0, and capturing the value
//! pins that for the port.

use std::ffi::{CString, c_void};
use std::fmt::Write as _;
use std::ptr;

use ffi::{
    NewTrie, RSPayload, Trie, Trie_Iterate, Trie_Size, TrieIterator_Free, TrieIterator_Next,
    TrieSortMode_Trie_Sort_Lex, TrieType_Free, rune, t_len,
};
use libc::{c_char, c_int};

use crate::support::{runes_to_string, trie_insert};

/// Build the shared fixture trie. The term set is chosen to exercise:
///
///   - shared prefix with an internal terminal (`appl` is itself a term *and*
///     a parent of `apple`/`apply`)
///   - sibling diverging on a single edit (`ape` vs `appl*`)
///   - a one-character term (`b`) sitting on a parent edge that also leads to
///     longer terms — DFA must yield it at distance 0 for `""` queries
///   - shared-prefix triple where the prefix itself is also a term
///     (`band`/`bandana`/`banana`)
///   - a term whose prefix is itself a term (`cat`/`category`) — distance-1
///     queries near either should pick the closer
fn build_fixture(trie: *mut Trie) {
    // Sorted alphabetically here, but the trie's structure (and the DFA's
    // traversal of it) is independent of insertion order. The snapshot's
    // ordering is the trie's own lex traversal, which is what we're pinning.
    for term in [
        "apple", "apply", "appl", "ape", "banana", "band", "bandana", "b", "cat", "category",
    ] {
        // SAFETY: `trie` is live for the duration of this function.
        unsafe { trie_insert(trie, term) };
    }
}

/// Run a single `Trie_Iterate` query and append its results to `out`.
///
/// Format:
///
/// ```text
/// query: prefix="..", maxDist=N, prefixMode=N
///   <term>   dist=<d>  score=<s>  numDocs=<n>
///   ...
///   <empty body line if no matches>
/// ```
///
/// The `dist` column is the value the DFA wrote into `matchCtx` for that
/// match — see this module's doc comment for the seed (`maxDist + 1`).
fn dump_filtered(trie: *mut Trie, prefix: &str, max_dist: i32, prefix_mode: i32, out: &mut String) {
    writeln!(
        out,
        "query: prefix={prefix:?}, maxDist={max_dist}, prefixMode={prefix_mode}"
    )
    .unwrap();

    // `Trie_Iterate` -> `strToLowerRunes` -> `nu_readstr` walks the input
    // until it hits a NUL terminator (`src/trie/rune_util.c:82`), ignoring the
    // explicit `len` parameter that bounds the *outer* loop. Production
    // callers happen to pass NUL-terminated buffers; we have to do the same
    // or the libnu transform overruns the input and corrupts memory.
    let c_prefix = CString::new(prefix).expect("test prefixes contain no NULs");

    // SAFETY: `trie` is live for the duration of this function; `c_prefix`
    // lives until the iterator is constructed (the C trie copies / consumes
    // the runes before returning).
    let it = unsafe {
        Trie_Iterate(
            trie,
            c_prefix.as_ptr() as *const c_char,
            prefix.len(),
            max_dist,
            prefix_mode,
        )
    };

    if it.is_null() {
        writeln!(out, "  <Trie_Iterate returned NULL>").unwrap();
        return;
    }

    let mut runes_ptr: *mut rune = ptr::null_mut();
    let mut rune_len: t_len = 0;
    let mut payload = RSPayload {
        data: ptr::null_mut(),
        len: 0,
    };
    let mut score: f32 = 0.0;
    let mut num_docs: usize = 0;
    // Seed identically to `Trie_Search` (src/trie/trie.c:256). The filter only
    // writes on match; reading it back on a miss would be undefined, but we
    // only read it inside the loop body where a match just fired.
    let mut dist: c_int = max_dist + 1;

    let mut matches = 0usize;
    // SAFETY: all out-pointers are valid for writes; the iterator owns the
    // returned `runes_ptr` buffer and reuses it across iterations. The
    // `matchCtx` pointer is interpreted by `LoweringFilterFunc` as `int *`.
    while unsafe {
        TrieIterator_Next(
            it,
            &mut runes_ptr,
            &mut rune_len,
            &mut payload,
            &mut score,
            &mut num_docs,
            &mut dist as *mut c_int as *mut c_void,
        )
    } != 0
    {
        // SAFETY: iterator hands us a valid rune buffer of length `rune_len`.
        let term = unsafe { runes_to_string(runes_ptr, rune_len as usize) };
        writeln!(
            out,
            "  {term:10}  dist={dist}  score={score}  numDocs={num_docs}"
        )
        .unwrap();
        matches += 1;
    }
    if matches == 0 {
        writeln!(out, "  <no matches>").unwrap();
    }

    // SAFETY: `it` was produced by `Trie_Iterate` above and not freed yet.
    unsafe { TrieIterator_Free(it) };
}

/// Render a per-test header so the snapshot reads top-down: fixture size,
/// then each query block separated by a blank line.
fn header(trie: *mut Trie) -> String {
    // SAFETY: `trie` is live for the duration of this function.
    let size = unsafe { Trie_Size(trie) };
    format!("size: {size}\n\n")
}

/// Build a fresh fixture trie, run the supplied query batch against it, free
/// the trie, and return the rendered dump. Each test only differs in the list
/// of `(prefix, maxDist, prefixMode)` triples it passes here, so the
/// fixture-construction and teardown plumbing lives in one place.
fn run(queries: &[(&str, i32, i32)]) -> String {
    // SAFETY: `NewTrie` with a NULL free-callback matches `sp->terms`
    // construction in `src/spec.c`. We free via `TrieType_Free` below.
    let trie = unsafe { NewTrie(None, TrieSortMode_Trie_Sort_Lex) };
    build_fixture(trie);

    let mut out = header(trie);
    for (i, (prefix, max_dist, prefix_mode)) in queries.iter().enumerate() {
        if i > 0 {
            out.push('\n');
        }
        dump_filtered(trie, prefix, *max_dist, *prefix_mode, &mut out);
    }

    // SAFETY: `trie` was created by `NewTrie` above and not freed elsewhere.
    unsafe { TrieType_Free(trie as *mut c_void) };
    out
}

#[test]
fn lex_iterate_pure_prefix() {
    // `(maxDist=0, prefixMode=1)`: the DFA degenerates to "match this exact
    // prefix, then accept anything beneath it". The empty-prefix case should
    // walk the whole trie in lex order — pinning that it behaves identically
    // to `Trie_IterateAll` is the point of including it here.
    let dump = run(&[
        ("", 0, 1),
        ("ap", 0, 1),
        ("ban", 0, 1),
        ("z", 0, 1),
        ("appl", 0, 1),
    ]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_iterate_pure_prefix", dump); }
    );
}

#[test]
fn lex_iterate_exact_match() {
    // `(maxDist=0, prefixMode=0)`: the DFA accepts exactly the input string,
    // so at most one terminal is yielded. The interesting case is `"appl"` —
    // it's an *internal* terminal (a parent of `apple`/`apply`) and the DFA
    // must still surface it.
    let dump = run(&[("apple", 0, 0), ("appl", 0, 0), ("apples", 0, 0)]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_iterate_exact_match", dump); }
    );
}

#[test]
fn lex_iterate_fuzzy_distance_1() {
    // `(maxDist=1, prefixMode=0)`: standard Levenshtein within edit distance 1.
    // The DFA's notion of "edit" is insertion/deletion/substitution (the
    // classic Levenshtein DFA) — a transposition costs 2, not 1, so `"appel"`
    // → `"apple"` is borderline depending on the construction. We don't
    // predict; we snapshot.
    let dump = run(&[
        ("appel", 1, 0),
        ("banan", 1, 0),
        ("aple", 1, 0),
        ("bandz", 1, 0),
        ("ca", 1, 0),
    ]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_iterate_fuzzy_distance_1", dump); }
    );
}

#[test]
fn lex_iterate_fuzzy_distance_2() {
    // `(maxDist=2, prefixMode=0)`: doubles the slack. `"aplez"` differs from
    // `apple` by an insertion + substitution (2 edits), from `apply` by 2
    // edits as well, from `appl` by 1, and from `ape` by 2 (insert+sub).
    // Whether the DFA yields all four — and what distance each lands at —
    // is the snapshot.
    let dump = run(&[("aplez", 2, 0), ("bnan", 2, 0)]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_iterate_fuzzy_distance_2", dump); }
    );
}

#[test]
fn lex_iterate_fuzzy_prefix() {
    // `(maxDist>0, prefixMode=1)`: the DFA accepts any string within edit
    // distance d of the input *prefix*, then accepts anything beneath. So
    // `"apl"` at d=1 should match a fuzzy `app`-ish prefix and then expand,
    // pulling in `apple`/`apply`/`appl`/`ape` and their descendants.
    let dump = run(&[("apl", 1, 1), ("bnd", 1, 1), ("cta", 1, 1)]);

    insta::with_settings!(
        { prepend_module_to_snapshot => false },
        { insta::assert_snapshot!("lex_iterate_fuzzy_prefix", dump); }
    );
}
