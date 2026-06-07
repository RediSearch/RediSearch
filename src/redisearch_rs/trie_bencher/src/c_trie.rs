/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Bench-only thin wrapper around the C `Trie` (`src/trie/trie.h`) configured
//! in `Trie_Sort_Lex` mode — the sort mode used by `sp->terms` (see
//! `src/spec.c:2403`).
//!
//! Exists so the Criterion harness in [`crate::bencher`] can mirror every
//! Rust `TermDictionary` bench with an equivalent C call inside the same
//! benchmark group, producing one chart per (corpus, operation) with `Rust`
//! and `C` bars side-by-side.
//!
//! Mirrored surface (production call shapes from `src/spec.c`,
//! `src/query.c`, `src/fork_gc/terms.c`):
//!
//! - [`CTrie::insert`] → `Trie_InsertStringBuffer(t, s, len, 1.0, ADD_INCR=1, NULL, 1)`
//! - [`CTrie::remove`] → `Trie_Delete(t, s, len)`
//! - [`CTrie::iterate_all`] → `Trie_IterateAll(t)` (full lex walk; fork-GC hot path)
//! - [`CTrie::iterate_dfa`] → `Trie_Iterate(t, prefix, len, maxDist, prefixMode)`
//!   (FT.SEARCH prefix/fuzzy hot path; see memory `project_dfa_filter_dist_semantics`
//!   on why we read `matchCtx` per match — running-min distance is load-bearing)
//! - [`CTrie::iterate_range`] → `Trie_IterateRange(t, min, ..., max, ...)` (lex-range walk)
//! - [`CTrie::iterate_wildcard`] → `Trie_IterateWildcard(t, runes, n, ...)`
//! - [`CTrie::iterate_contains`] → `Trie_IterateContains(t, runes, n, prefix=false, suffix=false, ...)`
//!
//! Out of scope:
//!
//! - Point-lookup ("find") — `sp->terms` has no C `Trie_Find` primitive;
//!   `Trie_InsertStringBuffer` does the read+modify atomically. The bench
//!   harness deliberately runs the Rust `TermDictionary::get` benches
//!   without a C mirror rather than synthesizing a fake one via
//!   `Trie_Iterate(..., 0, 0)`.
//! - `Clone` — the C trie has no clone API. Mutation benches must rebuild
//!   from a [`Vec<String>`] per iteration via Criterion's `iter_batched`.

use std::cell::RefCell;
use std::ffi::c_void;
use std::ptr::{self, NonNull};

use ffi::{
    NewTrie, RSPayload, Trie, TrieIterator_Free, TrieIterator_Next, TrieSortMode_Trie_Sort_Lex,
    TrieType_Free, Trie_Delete, Trie_InsertStringBuffer, Trie_IterateAll, Trie_IterateContains,
    Trie_IterateRange, Trie_IterateWildcard, Trie_Iterate, Trie_Size, rune, t_len,
};
use libc::{c_char, c_int};

/// `ADD_INCR` insert mode (see `src/redisearch.h`). Matches `sp->terms`
/// production call shape in `src/spec.c:1928`.
const ADD_INCR: i32 = 1;

/// Owning handle to a `Trie_Sort_Lex` C trie. Drops via `TrieType_Free`.
pub struct CTrie(NonNull<Trie>);

// SAFETY: `Trie` is a plain Redis-allocated heap struct with no
// thread-local state; the C side never mutates from another thread during
// a bench iteration. Criterion may move benches between threads at setup
// time, so we promise `Send`. We deliberately do NOT impl `Sync` — there
// is no read-only-shared usage in the harness and the C trie's internal
// counters mutate even on reads.
unsafe impl Send for CTrie {}

impl CTrie {
    pub fn new() -> Self {
        // SAFETY: `NewTrie` always returns a non-null pointer when the
        // module allocator hooks are wired, which `trie_bencher::lib.rs`
        // ensures via `mock_or_stub_missing_redis_c_symbols!()`.
        let raw = unsafe { NewTrie(None, TrieSortMode_Trie_Sort_Lex) };
        Self(NonNull::new(raw).expect("NewTrie returned NULL"))
    }

    /// `ADD_INCR` insert at score=1.0, numDocs=1 — mirrors `Trie_InsertStringBuffer`
    /// call in `src/spec.c:1928`.
    ///
    /// `term` is fed as UTF-8 bytes; the C trie lowers via libnu internally.
    pub fn insert(&mut self, term: &[u8]) {
        // SAFETY: `term` is borrowed for the duration of the call. The C
        // trie copies into its own rune-keyed storage before returning.
        unsafe {
            Trie_InsertStringBuffer(
                self.0.as_ptr(),
                term.as_ptr() as *const c_char,
                term.len(),
                1.0,
                ADD_INCR,
                ptr::null_mut(),
                1,
            );
        }
    }

    /// Mirrors `Trie_Delete(t, term, len)`. Returns whether a node was removed.
    pub fn remove(&mut self, term: &[u8]) -> bool {
        // SAFETY: `term` is borrowed for the duration of the call.
        let rc = unsafe {
            Trie_Delete(
                self.0.as_ptr(),
                term.as_ptr() as *const c_char,
                term.len(),
            )
        };
        rc != 0
    }

    pub fn size(&self) -> usize {
        // SAFETY: pointer is owned and live.
        unsafe { Trie_Size(self.0.as_ptr()) }
    }

    /// Walk every terminal (`Trie_IterateAll`). Drains the iterator and
    /// frees it. Used for full-walk benches (`sp->terms` fork-GC hot path,
    /// `src/fork_gc/terms.c`).
    pub fn iterate_all(&self) -> usize {
        // SAFETY: `self.0` is a live trie pointer produced by `NewTrie`;
        // `Trie_IterateAll` returns a heap-allocated iterator that this
        // function frees via `TrieIterator_Free` below.
        let it = unsafe { Trie_IterateAll(self.0.as_ptr()) };
        let mut count = 0usize;
        let mut runes_ptr: *mut rune = ptr::null_mut();
        let mut rune_len: t_len = 0;
        let mut payload = RSPayload {
            data: ptr::null_mut(),
            len: 0,
        };
        let mut score: f32 = 0.0;
        let mut num_docs: usize = 0;
        // SAFETY: every out-pointer is a valid stack write target. The
        // iterator owns and reuses `runes_ptr`'s buffer across calls.
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
            count += 1;
        }
        // SAFETY: `it` was produced by `Trie_IterateAll` above and not yet freed.
        unsafe { TrieIterator_Free(it) };
        count
    }

    /// Walk every terminal accepted by a Levenshtein DFA built over
    /// `prefix`. `max_dist` is the DFA edit budget; `prefix_mode=true`
    /// freezes the DFA at the end of the prefix and yields every term
    /// extending it within budget.
    ///
    /// Reads `matchCtx` as `int*` per match and accumulates the running-min
    /// distance — see memory `project_dfa_filter_dist_semantics`. The
    /// running-min is load-bearing for FT.SUGGET FUZZY ranking and must be
    /// measured, not skipped, for a fair comparison.
    pub fn iterate_dfa(&self, prefix: &[u8], max_dist: i32, prefix_mode: bool) -> (usize, i64) {
        // SAFETY: `self.0` is a live trie pointer; `prefix` is borrowed
        // for the duration of the call and the C trie copies what it
        // needs before returning. `Trie_Iterate` returns a heap iterator
        // freed via `TrieIterator_Free` below.
        let it = unsafe {
            Trie_Iterate(
                self.0.as_ptr(),
                prefix.as_ptr() as *const c_char,
                prefix.len(),
                max_dist,
                if prefix_mode { 1 } else { 0 },
            )
        };
        let mut count = 0usize;
        let mut dist_sum: i64 = 0;
        let mut runes_ptr: *mut rune = ptr::null_mut();
        let mut rune_len: t_len = 0;
        let mut payload = RSPayload {
            data: ptr::null_mut(),
            len: 0,
        };
        let mut score: f32 = 0.0;
        let mut num_docs: usize = 0;
        // The DFA filter writes the matched term's edit distance into
        // `match_ctx`. We seed with `max_dist + 1` (same sentinel as
        // `Trie_Search`, `src/trie/trie.c:256`) so an unwritten value
        // does not get silently summed.
        let mut match_dist: c_int = max_dist + 1;
        let match_ctx = &mut match_dist as *mut c_int as *mut c_void;
        // SAFETY: same invariants as `iterate_all` plus `match_ctx` is a
        // live writable `int*` for the duration of the loop.
        while unsafe {
            TrieIterator_Next(
                it,
                &mut runes_ptr,
                &mut rune_len,
                &mut payload,
                &mut score,
                &mut num_docs,
                match_ctx,
            )
        } != 0
        {
            count += 1;
            dist_sum += match_dist as i64;
            match_dist = max_dist + 1;
        }
        // SAFETY: `it` was produced by `Trie_Iterate` above and not yet freed.
        unsafe { TrieIterator_Free(it) };
        (count, dist_sum)
    }

    /// Lex-range walk via `Trie_IterateRange`. `min` / `max` are UTF-8
    /// strings; we ASCII-lowercase then UTF-16 encode them before calling.
    /// The lowering mirrors production's `strToLowerRunes` step on the C
    /// side of query.c; without it, mixed-case bounds would never match
    /// the already-lowered keys stored in the trie. `None` on either side
    /// disables that bound.
    pub fn iterate_range(
        &self,
        min: Option<&str>,
        include_min: bool,
        max: Option<&str>,
        include_max: bool,
    ) -> usize {
        let min_owned = min.map(str::to_lowercase);
        let max_owned = max.map(str::to_lowercase);
        let min_runes: Option<Vec<rune>> =
            min_owned.as_deref().map(encode_runes);
        let max_runes: Option<Vec<rune>> =
            max_owned.as_deref().map(encode_runes);

        // The C signature uses `-1` (with NULL ptr) to mean "unbounded".
        let (min_ptr, min_len): (*const rune, c_int) = match &min_runes {
            Some(v) => (v.as_ptr(), v.len() as c_int),
            None => (ptr::null(), -1),
        };
        let (max_ptr, max_len): (*const rune, c_int) = match &max_runes {
            Some(v) => (v.as_ptr(), v.len() as c_int),
            None => (ptr::null(), -1),
        };

        RANGE_COUNT.with(|c| *c.borrow_mut() = 0);
        // SAFETY:
        // - `self.0.as_ptr()` is a live trie pointer produced by `NewTrie`.
        // - `min_ptr` / `max_ptr` are valid for `min_len` / `max_len`
        //   runes by construction: each `Some(v)` branch hands the
        //   pointer + length of the same `Vec<rune>`, owned for the
        //   duration of this call by `min_runes` / `max_runes`. The
        //   `None` branch uses the C convention `(NULL, -1)` to mean
        //   "unbounded", which `Trie_IterateRange` accepts.
        // - `count_range_cb` runs synchronously on this thread, so the
        //   `RANGE_COUNT` thread-local is consistent across all callback
        //   invocations during the walk.
        unsafe {
            Trie_IterateRange(
                self.0.as_ptr(),
                min_ptr,
                min_len,
                include_min,
                max_ptr,
                max_len,
                include_max,
                Some(count_range_cb),
                ptr::null_mut(),
            );
        }
        RANGE_COUNT.with(|c| *c.borrow())
    }

    /// `Trie_IterateWildcard` mirror. ASCII-lowercases then UTF-16 encodes
    /// `pattern` to mirror production's `strToLowerRunes` step.
    pub fn iterate_wildcard(&self, pattern: &str) -> usize {
        let lowered = pattern.to_lowercase();
        let runes = encode_runes(&lowered);
        RANGE_COUNT.with(|c| *c.borrow_mut() = 0);
        // SAFETY: see `iterate_range`.
        unsafe {
            Trie_IterateWildcard(
                self.0.as_ptr(),
                runes.as_ptr(),
                runes.len() as c_int,
                Some(count_range_cb),
                ptr::null_mut(),
                ptr::null_mut(),
                true,
            );
        }
        RANGE_COUNT.with(|c| *c.borrow())
    }

    /// `Trie_IterateContains` mirror with `prefix=false, suffix=false`
    /// — i.e. "contains anywhere", which is the byte-trie's
    /// `contains_iter` semantic. ASCII-lowercases `target` before
    /// UTF-16 encoding to mirror `strToLowerRunes`.
    pub fn iterate_contains(&self, target: &str) -> usize {
        let lowered = target.to_lowercase();
        let runes = encode_runes(&lowered);
        RANGE_COUNT.with(|c| *c.borrow_mut() = 0);
        // SAFETY: see `iterate_range`.
        unsafe {
            Trie_IterateContains(
                self.0.as_ptr(),
                runes.as_ptr(),
                runes.len() as c_int,
                false,
                false,
                Some(count_range_cb),
                ptr::null_mut(),
                ptr::null_mut(),
                true,
            );
        }
        RANGE_COUNT.with(|c| *c.borrow())
    }
}

impl Default for CTrie {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for CTrie {
    fn drop(&mut self) {
        // SAFETY: pointer was produced by `NewTrie` and not yet freed.
        unsafe { TrieType_Free(self.0.as_ptr() as *mut c_void) };
    }
}

thread_local! {
    /// Counter consumed by [`count_range_cb`]. Cheaper than a heap-allocated
    /// `Vec` and avoids skewing the measured window with allocation cost.
    static RANGE_COUNT: RefCell<usize> = const { RefCell::new(0) };
}

/// Range/wildcard/contains callback that just counts matches. Returns 0
/// to signal "keep walking".
///
/// # Safety
///
/// The callback ignores every pointer argument — none are dereferenced —
/// so callers do not need to guarantee pointer validity. The only
/// requirement is that this function is invoked on the same thread that
/// initialized the [`RANGE_COUNT`] thread-local. The C trie's
/// `Trie_IterateRange` / `Trie_IterateWildcard` / `Trie_IterateContains`
/// all run synchronously on the calling thread, so this holds by
/// construction in [`CTrie::iterate_range`], [`CTrie::iterate_wildcard`],
/// and [`CTrie::iterate_contains`].
unsafe extern "C" fn count_range_cb(
    _str_ptr: *const rune,
    _len: usize,
    _ctx: *mut c_void,
    _payload: *mut c_void,
    _num_docs: usize,
) -> c_int {
    RANGE_COUNT.with(|c| *c.borrow_mut() += 1);
    0
}

/// UTF-16 encode `s` (BMP-only `u16` runes) for FFI calls that take
/// `(*const rune, len)`. Same convention as `rune_trie_snapshots::support`.
fn encode_runes(s: &str) -> Vec<rune> {
    s.encode_utf16().collect()
}

/// Bulk-build a [`CTrie`] from a slice of UTF-8 keys. Used as the setup
/// closure for `iter_batched` mutation benches and for one-shot
/// immutable-bench preloads.
pub fn build_from_terms(keys: &[&[u8]]) -> CTrie {
    let mut t = CTrie::new();
    for k in keys {
        t.insert(k);
    }
    t
}
