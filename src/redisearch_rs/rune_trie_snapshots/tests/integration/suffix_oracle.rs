/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C-oracle integration tests for the text-variant suffix-index port at
//! [`trie_rs::str::suffix::SuffixIndex`].
//!
//! The strategy: feed the same operation stream (add/remove + iteration
//! queries) to the Rust port and to the C implementation in `src/suffix.c`,
//! and assert that the **set** of yielded source terms matches. Iteration
//! order is not part of the contract: C walks the rune trie depth-first in
//! score-ordered child order, the Rust port walks the StrTrieMap in
//! byte-lex order — both must agree on membership.
//!
//! ## Why ASCII-lowercase inputs only
//!
//! The C `addSuffixTrie` runs `strToFoldedRunes` over its input before
//! indexing — case folding plus BMP-only rune conversion. The Rust port
//! at `trie_rs::str::suffix` is dormant code (no callers yet) and operates
//! on raw UTF-8 without folding, on the assumption that production callers
//! pre-lowercase. To compare apples-to-apples here we feed both
//! implementations ASCII lowercase, which is the identity for
//! `strToFoldedRunes`.
//!
//! Non-ASCII codepoint-vs-byte handling is pinned by the Rust-only fuzz
//! at `trie_rs/tests/integration/str/suffix.rs` and by the unit test
//! `multibyte_utf8_terms_round_trip` in `trie_rs::str::suffix`.

use std::cell::RefCell;
use std::collections::HashSet;
use std::ffi::c_void;
use std::ptr;

use ffi::{
    NewTrie, Suffix_IterateContains, Suffix_IterateWildcard, SuffixCtx,
    SuffixType_SUFFIX_TYPE_CONTAINS, SuffixType_SUFFIX_TYPE_SUFFIX,
    SuffixType_SUFFIX_TYPE_WILDCARD, Trie, TrieSortMode_Trie_Sort_Lex, TrieType_Free,
    addSuffixTrie, deleteSuffixTrie, strToLowerRunes,
};
use libc::{c_char, c_int};

use trie_rs::str::suffix::SuffixIndex;

/// Free a `rm_malloc`-allocated buffer via the `RedisModule_Free` hook —
/// in this test binary that's `redis_mock::allocator::free_shim`, which
/// understands the header layout `rm_malloc` produces. Returns silently
/// when the pointer is null.
unsafe fn rm_free(p: *mut c_void) {
    if p.is_null() {
        return;
    }
    // SAFETY: `redis_mock::mock_or_stub_missing_redis_c_symbols!()` in
    // `main.rs` initializes `RedisModule_Free` to a real allocator hook
    // before any test runs.
    let free_fn =
        unsafe { ffi::RedisModule_Free }.expect("redis_mock initializes RedisModule_Free");
    // SAFETY: caller guarantees `p` came from `rm_malloc` / `rm_strndup`.
    unsafe { free_fn(p) };
}

thread_local! {
    /// Push log used by `capture_terms_cb` — the C side fires the
    /// callback once per matched source term, with `char *` (not `rune *`)
    /// arguments because every suffix-index callsite uses the
    /// "char-mode" callback. We just record each emitted term as a
    /// `String`.
    static SUFFIX_LOG: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

/// `TrieSuffixCallback` signature: `(const char *, size_t, void *, void *) -> int`.
/// Returns 0 to keep iteration going (REDISMODULE_OK).
unsafe extern "C" fn capture_terms_cb(
    s: *const c_char,
    len: usize,
    _ctx: *mut c_void,
    _payload: *mut c_void,
) -> c_int {
    // SAFETY: the C path passes a valid `char` buffer of `len` bytes
    // (it's the same `term` pointer that `addSuffixTrie` recorded into
    // `suffixData.term` / `suffixData.array`).
    let slice = unsafe { std::slice::from_raw_parts(s as *const u8, len) };
    let term = String::from_utf8(slice.to_vec()).expect("ASCII corpus");
    SUFFIX_LOG.with(|log| log.borrow_mut().push(term));
    0
}

fn drain_log() -> HashSet<String> {
    SUFFIX_LOG.with(|log| std::mem::take(&mut *log.borrow_mut()).into_iter().collect())
}

/// Build a fresh C suffix trie from a corpus.
///
/// Returns the owning `Trie *`; caller must free with
/// `TrieType_Free(trie, suffixTrie_freeCallback)` once done.
unsafe fn build_c_index(corpus: &[&str]) -> *mut Trie {
    // SAFETY: `NewTrie` accepts a NULL free-callback and the lex-mode
    // sort enum; matches `sp->suffix` construction in `src/spec.c:3473`.
    let trie = unsafe { NewTrie(None, TrieSortMode_Trie_Sort_Lex) };
    for term in corpus {
        // SAFETY: `term.as_ptr()` valid for the call; `addSuffixTrie`
        // copies the input internally.
        unsafe {
            addSuffixTrie(trie, term.as_ptr() as *const c_char, term.len() as u32);
        }
    }
    trie
}

/// Free a C suffix trie. Mirrors what `IndexSpec_FreeSync` does for
/// `sp->suffix`: `TrieType_Free` walks each node and invokes the payload
/// free callback (`suffixTrie_freeCallback`) on its `suffixData`.
unsafe fn free_c_index(trie: *mut Trie) {
    // SAFETY: `trie` was produced by `build_c_index`, which only ever
    // returns valid pointers; the free callback is the canonical one
    // exported from `suffix.c`.
    unsafe {
        TrieType_Free(trie as *mut c_void);
    }
    // Note: `TrieType_Free` ignores the payload-free callback hook for
    // the *root* node; the per-node payload frees rely on the callback
    // registered via `NewTrie`. We constructed the trie with `None`
    // because the production callsite passes the free callback to
    // `NewTrie` instead — but for this oracle we accept the residual
    // payload leak in exchange for not needing a separate free-cb
    // type-erased binding. The leak is bounded by test-corpus size and
    // doesn't affect set-equality assertions.
}

/// Convert a UTF-8 needle to the C-side rune buffer via `strToLowerRunes`.
///
/// Returns `(rune *, unicode_len)`. The caller owns the `rune *` and must
/// free it with [`rm_free`] — the buffer is `rm_malloc`-allocated, which
/// in this test binary routes through `redis_mock::allocator` (a header-
/// prefixed allocator). Calling `libc::free` on it would assert in
/// `libmalloc` because the user pointer is `HEADER_SIZE` past the
/// malloc-block base.
unsafe fn lower_runes(needle: &str) -> (*mut ffi::rune, usize) {
    let mut unicode_len: usize = 0;
    // SAFETY: input pointer/length are valid; `strToLowerRunes` allocates
    // and returns a `rune *` whose length is written into `unicode_len`.
    let p = unsafe {
        strToLowerRunes(
            needle.as_ptr() as *const c_char,
            needle.len(),
            &mut unicode_len,
        )
    };
    (p, unicode_len)
}

/// Drive the C `Suffix_IterateContains` for `needle` against `trie` and
/// return the **set** of source terms it emits.
unsafe fn c_iter_suffix(trie: *mut Trie, needle: &str) -> HashSet<String> {
    drain_log(); // clear any residue
    let (rune_ptr, rune_len) = unsafe { lower_runes(needle) };
    if rune_ptr.is_null() {
        return HashSet::new();
    }
    let mut ctx = SuffixCtx {
        trie,
        rune: rune_ptr,
        runelen: rune_len,
        cstr: needle.as_ptr() as *const c_char,
        cstrlen: needle.len(),
        type_: SuffixType_SUFFIX_TYPE_SUFFIX,
        callback: Some(capture_terms_cb),
        cbCtx: ptr::null_mut(),
        timeout: ptr::null_mut(),
        skipTimeoutChecks: true,
    };
    // SAFETY: `ctx` lives for the duration of the call; all pointers are
    // valid; the trie was constructed via `addSuffixTrie` so its payloads
    // are `suffixData`.
    unsafe {
        Suffix_IterateContains(&mut ctx);
        rm_free(rune_ptr as *mut c_void);
    }
    drain_log()
}

unsafe fn c_iter_contains(trie: *mut Trie, needle: &str) -> HashSet<String> {
    drain_log();
    let (rune_ptr, rune_len) = unsafe { lower_runes(needle) };
    if rune_ptr.is_null() {
        return HashSet::new();
    }
    let mut ctx = SuffixCtx {
        trie,
        rune: rune_ptr,
        runelen: rune_len,
        cstr: needle.as_ptr() as *const c_char,
        cstrlen: needle.len(),
        type_: SuffixType_SUFFIX_TYPE_CONTAINS,
        callback: Some(capture_terms_cb),
        cbCtx: ptr::null_mut(),
        timeout: ptr::null_mut(),
        skipTimeoutChecks: true,
    };
    // SAFETY: same invariants as `c_iter_suffix`.
    unsafe {
        Suffix_IterateContains(&mut ctx);
        rm_free(rune_ptr as *mut c_void);
    }
    drain_log()
}

/// Drive the C `Suffix_IterateWildcard` for `pattern`. Returns
/// `(found_token, results)`: `found_token == false` mirrors the C
/// return-0 sentinel (no `MIN_SUFFIX`-length token in the pattern).
unsafe fn c_iter_wildcard(trie: *mut Trie, pattern: &str) -> (bool, HashSet<String>) {
    drain_log();
    // The C path requires a NUL-terminated, mutable rune buffer — it
    // writes a `(rune)'\0'` after the chosen token to terminate it for
    // `Trie_IterateWildcard`. Allocate an extra slot beyond what
    // `strToLowerRunes` returns so the in-place NUL termination has
    // somewhere to land.
    let (base, rune_len) = unsafe { lower_runes(pattern) };
    if base.is_null() {
        return (false, HashSet::new());
    }
    // Copy into a Vec<rune> with one extra slot for the trailing NUL the
    // C code writes during token termination. (The buffer
    // `strToLowerRunes` returns is exactly `rune_len * sizeof(rune)`
    // bytes, no headroom — writing past it would corrupt the heap.)
    let mut owned: Vec<ffi::rune> = Vec::with_capacity(rune_len + 1);
    // SAFETY: `base` points to `rune_len` valid runes.
    let slice = unsafe { std::slice::from_raw_parts(base, rune_len) };
    owned.extend_from_slice(slice);
    owned.push(0);
    // SAFETY: `base` is `rm_malloc`-allocated; `rm_free` routes through
    // the redis_mock allocator hook installed by `main.rs`.
    unsafe { rm_free(base as *mut c_void) };

    let mut ctx = SuffixCtx {
        trie,
        rune: owned.as_mut_ptr(),
        runelen: rune_len,
        cstr: pattern.as_ptr() as *const c_char,
        cstrlen: pattern.len(),
        type_: SuffixType_SUFFIX_TYPE_WILDCARD,
        callback: Some(capture_terms_cb),
        cbCtx: ptr::null_mut(),
        timeout: ptr::null_mut(),
        skipTimeoutChecks: true,
    };
    // SAFETY: `ctx` and its pointers are valid for the call; `owned`
    // stays alive across the `Suffix_IterateWildcard` invocation.
    let ret = unsafe { Suffix_IterateWildcard(&mut ctx) };
    (ret != 0, drain_log())
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

fn rust_iter_suffix(corpus: &[&str], needle: &str) -> HashSet<String> {
    let mut idx = SuffixIndex::new();
    for t in corpus {
        idx.add(t);
    }
    idx.iter_suffix(needle)
        .map(|r| r.as_ref().to_string())
        .collect()
}

fn rust_iter_contains(corpus: &[&str], needle: &str) -> HashSet<String> {
    let mut idx = SuffixIndex::new();
    for t in corpus {
        idx.add(t);
    }
    idx.iter_contains(needle)
        .map(|r| r.as_ref().to_string())
        .collect()
}

fn rust_iter_wildcard(corpus: &[&str], pattern: &str) -> Option<HashSet<String>> {
    let mut idx = SuffixIndex::new();
    for t in corpus {
        idx.add(t);
    }
    idx.iter_wildcard(pattern)
        .map(|it| it.map(|r| r.as_ref().to_string()).collect())
}

#[test]
fn oracle_iter_suffix_basic() {
    let corpus = &["cat", "concat", "scat", "catalog", "category"];
    // SAFETY: `build_c_index` / `free_c_index` are paired.
    let trie = unsafe { build_c_index(corpus) };
    for needle in &["cat", "at", "alog", "concat", "missing"] {
        let want = unsafe { c_iter_suffix(trie, needle) };
        let got = rust_iter_suffix(corpus, needle);
        assert_eq!(got, want, "iter_suffix({needle})");
    }
    unsafe { free_c_index(trie) };
}

#[test]
fn oracle_iter_contains_basic() {
    let corpus = &["cat", "concat", "scat", "catalog", "category", "dog"];
    let trie = unsafe { build_c_index(corpus) };
    for needle in &["cat", "at", "og", "ate", "missing"] {
        let want = unsafe { c_iter_contains(trie, needle) };
        let got = rust_iter_contains(corpus, needle);
        assert_eq!(got, want, "iter_contains({needle})");
    }
    unsafe { free_c_index(trie) };
}

#[test]
fn oracle_iter_wildcard_basic() {
    let corpus = &["apple", "happy", "puppet", "banana", "appendix"];
    let trie = unsafe { build_c_index(corpus) };
    for pattern in &["*pp*", "*pen*", "ap*", "*ana*"] {
        let (c_found_token, want) = unsafe { c_iter_wildcard(trie, pattern) };
        let got = rust_iter_wildcard(corpus, pattern);
        match (c_found_token, got) {
            (true, Some(g)) => assert_eq!(g, want, "iter_wildcard({pattern})"),
            (false, None) => { /* both signal fallback */ }
            (c, r) => panic!(
                "fallback-signal mismatch for {pattern:?}: C returned token={c}, Rust returned {:?}",
                r.is_some()
            ),
        }
    }
    unsafe { free_c_index(trie) };
}

#[test]
fn oracle_with_overlapping_suffixes() {
    // "concat" shares the "at" suffix with "scat", "cat", "category" —
    // the C side iterates the subtree under "at" and emits each back-ref
    // once per rotation. The Rust port matches by construction.
    let corpus = &["cat", "concat", "scat", "category", "automat"];
    let trie = unsafe { build_c_index(corpus) };

    let want = unsafe { c_iter_suffix(trie, "at") };
    let got = rust_iter_suffix(corpus, "at");
    assert_eq!(got, want, "iter_suffix(at)");

    let want = unsafe { c_iter_contains(trie, "at") };
    let got = rust_iter_contains(corpus, "at");
    assert_eq!(got, want, "iter_contains(at)");

    unsafe { free_c_index(trie) };
}

#[test]
fn oracle_after_delete() {
    // Insert, then delete a subset, then query — verifies that the
    // delete path on both sides leaves the same set of back-refs.
    let initial = &["cat", "concat", "scat", "catalog"];
    let to_delete = &["concat", "catalog"];

    // C side.
    let trie = unsafe { build_c_index(initial) };
    for t in to_delete {
        // SAFETY: each `t` is ASCII; `deleteSuffixTrie` is the canonical
        // mirror of `addSuffixTrie` and tolerates absent rotations.
        unsafe {
            deleteSuffixTrie(trie, t.as_ptr() as *const c_char, t.len() as u32);
        }
    }
    let want_suffix = unsafe { c_iter_suffix(trie, "at") };
    let want_contains = unsafe { c_iter_contains(trie, "cat") };
    unsafe { free_c_index(trie) };

    // Rust side.
    let mut idx = SuffixIndex::new();
    for t in initial {
        idx.add(t);
    }
    for t in to_delete {
        idx.remove(t);
    }
    let got_suffix: HashSet<String> = idx
        .iter_suffix("at")
        .map(|r| r.as_ref().to_string())
        .collect();
    let got_contains: HashSet<String> = idx
        .iter_contains("cat")
        .map(|r| r.as_ref().to_string())
        .collect();

    assert_eq!(got_suffix, want_suffix, "iter_suffix(at) after delete");
    assert_eq!(
        got_contains, want_contains,
        "iter_contains(cat) after delete"
    );
}
