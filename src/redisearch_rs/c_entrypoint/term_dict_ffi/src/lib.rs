/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C-callable surface for [`trie_rs::str::term_dict::TermDictionary`].
//!
//! Backs the optional Rust path of the `sp->terms` term dictionary, gated
//! by the `USE_RUST_TERM_DICT` cmake option. PR2 of the RDB-delegation
//! stack: it provides the construct / iterate / raw-insert primitives that
//! the callback-shaped C codec (`TermStream_RdbSave` / `TermStream_RdbLoad`
//! in `src/trie/term_stream_codec.{c,h}`) needs to delegate save and load to
//! a Rust-backed dictionary. The production indexing surface
//! (`add_term`/`replace_term`/`get`/`decrement_num_docs`) is not exposed
//! here yet — it lands as call sites in `src/spec.c` are converted.
//!
//! ## Iterator lifetime
//!
//! [`TermDict_IterNext`] yields a `*const c_char` that points into a
//! per-step `String` owned by the iterator (see
//! [`trie_rs::str::iter::Iter::next`]). The pointer is invalidated by the
//! next call to [`TermDict_IterNext`] on the same iterator, or by
//! [`TermDict_IterFree`]. Callers must copy the bytes if they need them
//! past the next step.
//!
//! ## Payload discard
//!
//! Legacy RDBs at `encver >= TRIE_ENCVER_PAYLOADS` may carry payload bytes
//! per term. The C codec reads them off the stream regardless; the Rust
//! sink ([`TermDict_InsertRaw`]) ignores them by design —
//! [`trie_rs::str::term_dict::TermEntry`] has no payload field.
//!
//! ## Case-folding
//!
//! [`TermDict_InsertRaw`] forwards to
//! [`trie_rs::str::term_dict::TermDictionary::insert`], which lowercases
//! via [`str::to_lowercase`]. Old C-trie RDBs were written after the libnu
//! ASCII fold (`runeBufFill` in `src/trie/trie.c`) — bytes round-trip
//! identically on ASCII; non-ASCII keys may diverge on NFKC/ligature
//! decomposition. Documented hazard, not a bug.

use std::ffi::{c_char, c_int, c_void};

use libc::size_t;
use trie_rs::str::iter::Iter;
use trie_rs::str::term_dict::{DecrResult, InsertOutcome, TermDictionary, TermEntry};

/// Opaque C handle wrapping a [`TermDictionary`].
///
/// Allocated with [`TermDict_New`] and freed with [`TermDict_Free`]. The
/// tuple-struct form keeps the field name out of the cheadergen-generated
/// C header — only `struct TermDict` is exposed.
pub struct TermDict(TermDictionary);

/// Iterator handle yielded by [`TermDict_IterNew`]. Holds the active
/// [`trie_rs::str::iter::Iter`] plus a one-slot stash for the most-recently
/// yielded key, so the `*const c_char` exposed to C stays valid until the
/// next [`TermDict_IterNext`] step.
pub struct TermDictIter<'d> {
    inner: Iter<'d, TermEntry>,
    current_term: Option<String>,
}

/// Allocate an empty [`TermDictionary`] on the heap and hand ownership to C.
///
/// The returned pointer must be released exactly once via [`TermDict_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn TermDict_New() -> *mut TermDict {
    Box::into_raw(Box::new(TermDict(TermDictionary::new())))
}

/// Free a [`TermDict`] previously returned by [`TermDict_New`].
///
/// # Safety
/// - `d` must either be NULL or a pointer obtained from [`TermDict_New`]
///   that has not yet been freed.
/// - Any [`TermDictIter`] borrowing `d` must have been freed via
///   [`TermDict_IterFree`] before this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDict_Free(d: *mut TermDict) {
    if d.is_null() {
        return;
    }
    // SAFETY: Caller guarantees `d` was returned by `TermDict_New` and has
    // not yet been freed. We reclaim the `Box` and drop it.
    unsafe {
        let _ = Box::from_raw(d);
    }
}

/// Number of terms currently held.
///
/// # Safety
/// - `d` must point to a valid [`TermDict`] obtained from [`TermDict_New`]
///   and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDict_Len(d: *const TermDict) -> size_t {
    debug_assert!(!d.is_null(), "d cannot be NULL");
    // SAFETY: Caller guarantees `d` points to a valid `TermDict`.
    let TermDict(dict) = unsafe { &*d };
    dict.len() as size_t
}

/// Open a forward iterator over every `(term, score, num_docs)` tuple in
/// `d`. The returned handle must be freed with [`TermDict_IterFree`].
///
/// The iterator borrows `d`. The caller must not free or mutate `d` for
/// the iterator's lifetime.
///
/// # Safety
/// - `d` must point to a valid [`TermDict`] obtained from [`TermDict_New`]
///   and cannot be NULL.
/// - `d` must outlive the returned iterator.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDict_IterNew<'d>(d: *const TermDict) -> *mut TermDictIter<'d> {
    debug_assert!(!d.is_null(), "d cannot be NULL");
    // SAFETY: Caller guarantees `d` points to a valid `TermDict` that
    // outlives the iterator's lifetime `'d`.
    let TermDict(dict) = unsafe { &*d };
    let it = Box::new(TermDictIter {
        inner: dict.iter(),
        current_term: None,
    });
    Box::into_raw(it)
}

/// Advance the iterator. On `true`, writes the next tuple to the out-pointers:
/// - `*term`: UTF-8 bytes, NOT NUL-terminated. Valid only until the next
///   call to [`TermDict_IterNext`] on this iterator, OR until
///   [`TermDict_IterFree`], whichever comes first. The C side must copy
///   the bytes if it needs to retain them across steps. The per-step
///   ownership is required because the underlying [`Iter`] yields a fresh
///   `String` per step (see `src/redisearch_rs/trie_rs/src/str/iter/iter_.rs:24`).
/// - `*term_len`: byte length of the term.
/// - `*score`: the entry's score, widened from the stored `f32` to `f64`
///   to match the codec's `RedisModule_SaveDouble` wire shape.
/// - `*num_docs`: the entry's document count.
///
/// On `false`, the stream is exhausted and out-pointers are not touched.
///
/// # Safety
/// - `it` must point to a valid [`TermDictIter`] obtained from
///   [`TermDict_IterNew`] and cannot be NULL.
/// - `term`, `term_len`, `score`, `num_docs` must each point to writable
///   storage of the appropriate type.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDict_IterNext(
    it: *mut TermDictIter<'_>,
    term: *mut *const c_char,
    term_len: *mut size_t,
    score: *mut f64,
    num_docs: *mut size_t,
) -> bool {
    debug_assert!(!it.is_null(), "it cannot be NULL");
    debug_assert!(!term.is_null(), "term cannot be NULL");
    debug_assert!(!term_len.is_null(), "term_len cannot be NULL");
    debug_assert!(!score.is_null(), "score cannot be NULL");
    debug_assert!(!num_docs.is_null(), "num_docs cannot be NULL");

    // SAFETY: Caller guarantees `it` points to a valid `TermDictIter`.
    let iter = unsafe { &mut *it };

    let Some((k, entry)) = iter.inner.next() else {
        return false;
    };

    // Stash the fresh `String` on the iterator so the exposed `*const c_char`
    // stays valid until the next step (which drops the old slot) or until
    // `TermDict_IterFree`.
    iter.current_term = Some(k);
    let stashed = iter.current_term.as_ref().expect("just stored");
    let bytes = stashed.as_bytes();

    // SAFETY: Caller guarantees `term` is writable.
    unsafe { term.write(bytes.as_ptr().cast::<c_char>()) };
    // SAFETY: Caller guarantees `term_len` is writable.
    unsafe { term_len.write(bytes.len() as size_t) };
    // SAFETY: Caller guarantees `score` is writable.
    unsafe { score.write(entry.score as f64) };
    // SAFETY: Caller guarantees `num_docs` is writable.
    unsafe { num_docs.write(entry.num_docs as size_t) };
    true
}

/// Free a [`TermDictIter`] previously returned by [`TermDict_IterNew`].
///
/// Drops the iterator's per-step stash, invalidating any `*const c_char`
/// previously yielded by [`TermDict_IterNext`].
///
/// # Safety
/// - `it` must either be NULL or a pointer obtained from
///   [`TermDict_IterNew`] that has not yet been freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDict_IterFree(it: *mut TermDictIter<'_>) {
    if it.is_null() {
        return;
    }
    // SAFETY: Caller guarantees `it` was returned by `TermDict_IterNew`
    // and has not yet been freed.
    unsafe {
        let _ = Box::from_raw(it);
    }
}

/// Raw overwrite insert — the RDB-load sink primitive.
///
/// Discards any prior entry for the same (case-folded) key. The term bytes
/// must be valid UTF-8; returns `false` without inserting on UTF-8 failure
/// so the caller can route to its `_IOError` path (see
/// `src/trie/term_stream_codec.c`).
///
/// Payload bytes from legacy RDBs are NOT plumbed into this function —
/// [`TermEntry`] has no payload field by design. The C codec reads them off
/// the stream and discards.
///
/// # Safety
/// - `d` must point to a valid [`TermDict`] obtained from [`TermDict_New`]
///   and cannot be NULL.
/// - `term` must point to a readable byte sequence of length `term_len`.
///   It may only be NULL when `term_len == 0`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDict_InsertRaw(
    d: *mut TermDict,
    term: *const c_char,
    term_len: size_t,
    score: f64,
    num_docs: size_t,
) -> bool {
    debug_assert!(!d.is_null(), "d cannot be NULL");

    let bytes: &[u8] = if term_len > 0 {
        debug_assert!(!term.is_null(), "term cannot be NULL when term_len > 0");
        // SAFETY: Caller guarantees `term` points to a readable byte
        // sequence of length `term_len`.
        unsafe { std::slice::from_raw_parts(term.cast::<u8>(), term_len) }
    } else {
        &[]
    };

    let Ok(s) = std::str::from_utf8(bytes) else {
        return false;
    };

    // SAFETY: Caller guarantees `d` points to a valid `TermDict`.
    let TermDict(dict) = unsafe { &mut *d };
    dict.insert(
        s,
        TermEntry {
            score: score as f32,
            num_docs,
        },
    );
    true
}

/// ABI-mirror of `TrieDecrResult` in `src/trie/trie.h:103-107`.
///
/// Variants must keep their explicit discriminants in sync with the C
/// enum: [`TermDict_DecrementNumDocs`]'s return value is cast directly to
/// `TrieDecrResult` at the `spec.c` call site. Reordering or renumbering
/// any variant here is an ABI break.
///
/// `prefix_with_name` keeps the C-side variant names namespaced
/// (`TermDictDecrResult_NotFound` etc.) so they don't collide with
/// arbitrary `NotFound` / `Updated` / `Deleted` identifiers elsewhere in
/// the C codebase.
#[repr(C)]
#[cheadergen::config(prefix_with_name)]
pub enum TermDictDecrResult {
    /// Term not present — no entry to decrement.
    NotFound = 0,
    /// `num_docs` was decremented and is still `> 0`.
    Updated = 1,
    /// `num_docs` reached `0`; the entry was removed.
    Deleted = 2,
}

/// Estimated heap bytes held by the dictionary.
///
/// Backs `TrieType_MemUsage(sp->terms)` at `src/spec.c:504`, reached
/// unconditionally from `IndexSpec_collect_text_overhead` — including on
/// freshly-constructed or torn-down specs where `sp->terms` may be NULL.
/// Returns `0` on NULL to match `TrieType_MemUsage`'s NULL-tolerance.
///
/// # Safety
/// - `d` must either be NULL or point to a valid [`TermDict`] obtained
///   from [`TermDict_New`].
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn TermDict_MemUsage(d: *const TermDict) -> size_t {
    if d.is_null() {
        return 0;
    }
    // SAFETY: Caller guarantees `d` points to a valid `TermDict`.
    let TermDict(dict) = unsafe { &*d };
    dict.mem_usage() as size_t
}

/// ADD_INCR insert — production indexing primitive backing
/// `Trie_InsertStringBuffer(sp->terms, term, len, score=1, incr=1,
/// payload=NULL, numDocs=1)` at `src/spec.c:1971`.
///
/// Returns `true` if a new terminal was created (mirrors `TRIE_OK_NEW`),
/// `false` if an existing entry was updated in place (mirrors
/// `TRIE_OK_UPDATED`). `spec.c:1972` only branches on the NEW case, so
/// the full `InsertOutcome` enum is not exposed across FFI. A `false`
/// return is **not** an error signal.
///
/// `term` bytes must be valid UTF-8; on failure the function returns
/// `false` *without* inserting, matching [`TermDict_InsertRaw`]'s shape.
/// Callers that need to distinguish UTF-8 failure from "updated existing"
/// must validate upstream — production indexing already lowercases via
/// `runeBufFill`, so the failure mode is unreachable in practice.
///
/// Case-folding happens inside [`TermDictionary::add_term`]; callers must
/// not pre-fold.
///
/// # Safety
/// - `d` must point to a valid [`TermDict`] obtained from [`TermDict_New`]
///   and cannot be NULL.
/// - `term` must point to a readable byte sequence of length `term_len`.
///   It may only be NULL when `term_len == 0`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDict_AddTerm(
    d: *mut TermDict,
    term: *const c_char,
    term_len: size_t,
    score: f64,
    num_docs: size_t,
) -> bool {
    debug_assert!(!d.is_null(), "d cannot be NULL");

    let bytes: &[u8] = if term_len > 0 {
        debug_assert!(!term.is_null(), "term cannot be NULL when term_len > 0");
        // SAFETY: Caller guarantees `term` points to a readable byte
        // sequence of length `term_len`.
        unsafe { std::slice::from_raw_parts(term.cast::<u8>(), term_len) }
    } else {
        &[]
    };

    let Ok(s) = std::str::from_utf8(bytes) else {
        return false;
    };

    // SAFETY: Caller guarantees `d` points to a valid `TermDict`.
    let TermDict(dict) = unsafe { &mut *d };
    matches!(dict.add_term(s, score as f32, num_docs), InsertOutcome::New)
}

/// Decrement the `num_docs` count for `term` by `delta`. Mirrors
/// `Trie_DecrementNumDocs` at `src/trie/trie.c:130` and backs
/// `IndexSpec_DecrementTrieTermCount` at `src/spec.c:4862`.
///
/// NULL `d` returns [`TermDictDecrResult::NotFound`], matching the C
/// `Trie_DecrementNumDocs(NULL, …)` branch at `src/trie/trie.c:132-135`.
/// Non-UTF-8 `term` also returns [`TermDictDecrResult::NotFound`] —
/// the same outcome the C path produces when `runeBufFill` fails.
///
/// Case-folding happens inside [`TermDictionary::decrement_num_docs`];
/// callers must not pre-fold.
///
/// # Safety
/// - `d` must either be NULL or point to a valid [`TermDict`] obtained
///   from [`TermDict_New`].
/// - `term` must point to a readable byte sequence of length `term_len`.
///   It may only be NULL when `term_len == 0`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDict_DecrementNumDocs(
    d: *mut TermDict,
    term: *const c_char,
    term_len: size_t,
    delta: size_t,
) -> TermDictDecrResult {
    if d.is_null() {
        return TermDictDecrResult::NotFound;
    }

    let bytes: &[u8] = if term_len > 0 {
        debug_assert!(!term.is_null(), "term cannot be NULL when term_len > 0");
        // SAFETY: Caller guarantees `term` points to a readable byte
        // sequence of length `term_len`.
        unsafe { std::slice::from_raw_parts(term.cast::<u8>(), term_len) }
    } else {
        &[]
    };

    let Ok(s) = std::str::from_utf8(bytes) else {
        return TermDictDecrResult::NotFound;
    };

    // SAFETY: Caller guarantees `d` points to a valid `TermDict`.
    let TermDict(dict) = unsafe { &mut *d };
    match dict.decrement_num_docs(s, delta) {
        DecrResult::NotFound => TermDictDecrResult::NotFound,
        DecrResult::Updated => TermDictDecrResult::Updated,
        DecrResult::Deleted => TermDictDecrResult::Deleted,
    }
}

/// Return the document count for `term`, or `0` if the term is absent.
/// Backs the `Trie_GetNode(...)->numDocs` reads in `src/query.c` (the
/// fuzzy / lex-range / suffix-trie sites at q-c:568, q-c:651, q-c:871),
/// which all collapse "node not found" into "zero docs" via the
/// `trienode ? trienode->numDocs : 0` idiom.
///
/// NULL `d` returns `0`. Non-UTF-8 `term` also returns `0` — matching
/// the C path's outcome when `runeBufFill` fails on the lookup.
///
/// Case-folding happens inside [`TermDictionary::get`]; callers must not
/// pre-fold. (Production query paths already lowercase via `runeBufFill`,
/// so the fold is effectively a no-op for ASCII — see the case-folding
/// hazard note on [`TermDictionary`].)
///
/// # Safety
/// - `d` must either be NULL or point to a valid [`TermDict`] obtained
///   from [`TermDict_New`].
/// - `term` must point to a readable byte sequence of length `term_len`.
///   It may only be NULL when `term_len == 0`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDict_GetNumDocs(
    d: *const TermDict,
    term: *const c_char,
    term_len: size_t,
) -> size_t {
    if d.is_null() {
        return 0;
    }

    let bytes: &[u8] = if term_len > 0 {
        debug_assert!(!term.is_null(), "term cannot be NULL when term_len > 0");
        // SAFETY: Caller guarantees `term` points to a readable byte
        // sequence of length `term_len`.
        unsafe { std::slice::from_raw_parts(term.cast::<u8>(), term_len) }
    } else {
        &[]
    };

    let Ok(s) = std::str::from_utf8(bytes) else {
        return 0;
    };

    // SAFETY: Caller guarantees `d` points to a valid `TermDict`.
    let TermDict(dict) = unsafe { &*d };
    dict.get(s).map_or(0, |e| e.num_docs as size_t)
}

/// Remove the entry for `term`. Mirrors `Trie_Delete` at
/// `src/trie/trie.c:89`. Backs the fork-GC delete site at
/// `src/fork_gc/terms.c`.
///
/// Returns `true` if an entry was removed, `false` otherwise (term absent
/// or `d` NULL or `term` not valid UTF-8). The two false-modes are not
/// distinguished — matching the C path, which folds an oversized rune
/// buffer into the same `0` return as a genuine miss.
///
/// Case-folding happens inside [`TermDictionary::remove`]; callers must
/// not pre-fold.
///
/// # Safety
/// - `d` must either be NULL or point to a valid [`TermDict`] obtained
///   from [`TermDict_New`].
/// - `term` must point to a readable byte sequence of length `term_len`.
///   It may only be NULL when `term_len == 0`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDict_Delete(
    d: *mut TermDict,
    term: *const c_char,
    term_len: size_t,
) -> bool {
    if d.is_null() {
        return false;
    }

    let bytes: &[u8] = if term_len > 0 {
        debug_assert!(!term.is_null(), "term cannot be NULL when term_len > 0");
        // SAFETY: Caller guarantees `term` points to a readable byte
        // sequence of length `term_len`.
        unsafe { std::slice::from_raw_parts(term.cast::<u8>(), term_len) }
    } else {
        &[]
    };

    let Ok(s) = std::str::from_utf8(bytes) else {
        return false;
    };

    // SAFETY: Caller guarantees `d` points to a valid `TermDict`.
    let TermDict(dict) = unsafe { &mut *d };
    dict.remove(s).is_some()
}

/// Per-match callback for the lex-range / contains / wildcard iterators.
///
/// Mirrors the slim shape of `TrieRangeCallback` in
/// `src/trie/trie_node.h:233` — the `payload` parameter is dropped because
/// `sp->terms` never carries one. Receives the term as UTF-8 bytes
/// (`term` / `term_len`), the caller-supplied `ctx`, and the entry's
/// `num_docs`.
///
/// Return `0` to continue iteration, non-zero to break early. Mirrors the
/// `REDISEARCH_OK` / `REDISEARCH_ERR` convention the C callers already use
/// to honour `maxPrefixExpansions` caps.
///
/// `None` means "no callback supplied" — the iterate call short-circuits
/// without traversal. Matches the `cb: TrieMapReplaceFunc` pattern.
pub type TermDictRangeCallback = Option<
    unsafe extern "C" fn(
        term: *const c_char,
        term_len: size_t,
        ctx: *mut c_void,
        num_docs: size_t,
    ) -> c_int,
>;

/// Per-match callback for the DFA (fuzzy / prefix-fuzzy) iterator.
///
/// Mirrors the columns `TrieIterator_Next` writes out at
/// `src/trie/trie.c` — `(term, term_len, score, num_docs, dist)` — minus
/// the always-NULL payload pointer. Receives the term as UTF-8 bytes.
///
/// `score` is widened from the stored `f32` to `f64` to match the C-side
/// `float score` slot at the `iterateExpandedTerms` call site in
/// `src/query.c:633`, where the value is consumed as a double for IDF
/// math.
///
/// Return `0` to continue, non-zero to break early.
pub type TermDictDfaCallback = Option<
    unsafe extern "C" fn(
        term: *const c_char,
        term_len: size_t,
        ctx: *mut c_void,
        score: f64,
        num_docs: size_t,
        dist: u32,
    ) -> c_int,
>;

/// Resolve a `(ptr, len)` pair from C into an optional `&str`.
///
/// - NULL pointer → `Ok(None)` (the unbounded / no-input sentinel for
///   `TermDict_IterateRange`).
/// - Non-NULL pointer with non-UTF-8 contents → `Err(())`. The iterate
///   functions collapse this into a silent no-op, mirroring the C path
///   where `runeBufFill` failure produces an empty traversal.
/// - Non-NULL pointer with valid UTF-8 → `Ok(Some(&str))`.
///
/// # Safety
/// - When `ptr` is non-NULL, it must point to a readable byte sequence of
///   length `len`. NULL is accepted regardless of `len`.
unsafe fn ptr_to_utf8<'a>(ptr: *const c_char, len: size_t) -> Result<Option<&'a str>, ()> {
    if ptr.is_null() {
        return Ok(None);
    }
    let bytes = if len > 0 {
        // SAFETY: Caller guarantees `ptr` points to a readable byte sequence
        // of length `len`.
        unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len) }
    } else {
        &[]
    };
    std::str::from_utf8(bytes).map(Some).map_err(|_| ())
}

/// Iterate every term whose key begins with, ends with, or contains
/// `pattern`. The `prefix` and `suffix` flags select the variant, matching
/// the C `Trie_IterateContains(t, str, nstr, prefix, suffix, …)` shape at
/// `src/trie/trie.c`:
///
/// | prefix | suffix | semantic                          |
/// | ------ | ------ | --------------------------------- |
/// | true   | false  | prefix match (term starts with X) |
/// | false  | true   | suffix match (term ends with X)   |
/// | true   | true   | contains match (substring)        |
/// | false  | false  | contains match (same as true,true)|
///
/// The last row is reachable from the C side but never used in practice —
/// query.c always sets at least one flag. Treating `(false, false)` as
/// "contains" keeps the FFI total without an error path.
///
/// NULL `d`, NULL `cb`, or non-UTF-8 `pattern` collapse to a no-op — the
/// same outcome the C path produces when `runeBufFill` fails or the trie
/// is missing.
///
/// Case-folding happens inside `TermDictionary`'s iter constructors;
/// callers must not pre-fold. (Production paths receive already-lowercased
/// ASCII, so the fold is a no-op for them; the contract is pinned for
/// future non-ASCII work — see [`TermDictionary`].)
///
/// # Safety
/// - `d` must either be NULL or point to a valid [`TermDict`] obtained
///   from [`TermDict_New`].
/// - `pattern` must point to a readable byte sequence of length
///   `pattern_len`. NULL is accepted only when `pattern_len == 0`.
/// - `cb`, when `Some`, must have the documented signature and must not
///   capture pointers handed to it past the call (the `term` pointer is
///   invalidated after the callback returns).
/// - `ctx` is opaque — passed through to `cb` unchanged.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDict_IterateContains(
    d: *const TermDict,
    pattern: *const c_char,
    pattern_len: size_t,
    prefix: bool,
    suffix: bool,
    cb: TermDictRangeCallback,
    ctx: *mut c_void,
) {
    if d.is_null() {
        return;
    }
    let Some(cb) = cb else { return };

    // SAFETY: Caller guarantees `pattern` honours the `(ptr, len)` contract.
    let pattern_str = match unsafe { ptr_to_utf8(pattern, pattern_len) } {
        Ok(Some(s)) => s,
        Ok(None) => "",
        Err(()) => return,
    };

    // SAFETY: Caller guarantees `d` points to a valid `TermDict`.
    let TermDict(dict) = unsafe { &*d };

    match (prefix, suffix) {
        (true, false) => {
            for (k, e) in dict.prefixed_iter(pattern_str) {
                // SAFETY: `cb` honours the documented signature; `k`'s bytes
                // are valid through the callback call.
                let rc = unsafe {
                    cb(
                        k.as_ptr().cast::<c_char>(),
                        k.len() as size_t,
                        ctx,
                        e.num_docs as size_t,
                    )
                };
                if rc != 0 {
                    break;
                }
            }
        }
        (false, true) => {
            for (k, e) in dict.suffixed_iter(pattern_str) {
                // SAFETY: see prefix branch.
                let rc = unsafe {
                    cb(
                        k.as_ptr().cast::<c_char>(),
                        k.len() as size_t,
                        ctx,
                        e.num_docs as size_t,
                    )
                };
                if rc != 0 {
                    break;
                }
            }
        }
        _ => {
            for (k, e) in dict.contains_iter(pattern_str) {
                // SAFETY: see prefix branch.
                let rc = unsafe {
                    cb(
                        k.as_ptr().cast::<c_char>(),
                        k.len() as size_t,
                        ctx,
                        e.num_docs as size_t,
                    )
                };
                if rc != 0 {
                    break;
                }
            }
        }
    }
}

/// Iterate every term matching the wildcard `pattern` (`?` = one byte,
/// `*` = zero or more bytes). Backs `Trie_IterateWildcard` at
/// `src/trie/trie.c`.
///
/// NULL `d`, NULL `cb`, or non-UTF-8 `pattern` collapse to a no-op.
///
/// Note on non-ASCII patterns: the underlying [`TermDictionary::wildcard_iter`]
/// operates byte-wise. `?` matches exactly one byte, not one codepoint —
/// so a multi-byte codepoint matched by `?` will surface a partial-codepoint
/// mismatch later in the search. Production patterns are ASCII; the caveat
/// is documented for future work.
///
/// # Safety
/// Same contract as [`TermDict_IterateContains`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDict_IterateWildcard(
    d: *const TermDict,
    pattern: *const c_char,
    pattern_len: size_t,
    cb: TermDictRangeCallback,
    ctx: *mut c_void,
) {
    if d.is_null() {
        return;
    }
    let Some(cb) = cb else { return };

    // SAFETY: Caller guarantees `pattern` honours the `(ptr, len)` contract.
    let pattern_str = match unsafe { ptr_to_utf8(pattern, pattern_len) } {
        Ok(Some(s)) => s,
        Ok(None) => "",
        Err(()) => return,
    };

    // SAFETY: Caller guarantees `d` points to a valid `TermDict`.
    let TermDict(dict) = unsafe { &*d };

    for (k, e) in dict.wildcard_iter(pattern_str) {
        // SAFETY: `cb` honours the documented signature; `k`'s bytes are
        // valid through the callback call.
        let rc = unsafe {
            cb(
                k.as_ptr().cast::<c_char>(),
                k.len() as size_t,
                ctx,
                e.num_docs as size_t,
            )
        };
        if rc != 0 {
            break;
        }
    }
}

/// Iterate every term within the lex range `[min, max]`. Backs
/// `Trie_IterateRange` at `src/trie/trie.c`.
///
/// NULL `min` (or NULL `max`) means "unbounded on that side", matching
/// the C `(NULL, -1)` sentinel. The inclusivity flags control whether
/// the endpoints themselves are admitted.
///
/// NULL `d`, NULL `cb`, or non-UTF-8 bound collapse to a no-op.
///
/// # Safety
/// - `d` must either be NULL or point to a valid [`TermDict`] obtained
///   from [`TermDict_New`].
/// - `min` may be NULL (unbounded) or must point to a readable byte
///   sequence of length `min_len`. Same for `max` / `max_len`.
/// - `cb` and `ctx`: same contract as [`TermDict_IterateContains`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDict_IterateRange(
    d: *const TermDict,
    min: *const c_char,
    min_len: size_t,
    min_inclusive: bool,
    max: *const c_char,
    max_len: size_t,
    max_inclusive: bool,
    cb: TermDictRangeCallback,
    ctx: *mut c_void,
) {
    if d.is_null() {
        return;
    }
    let Some(cb) = cb else { return };

    // SAFETY: Caller guarantees the `(ptr, len)` contracts for both bounds.
    let min_str = match unsafe { ptr_to_utf8(min, min_len) } {
        Ok(opt) => opt,
        Err(()) => return,
    };
    // SAFETY: see above.
    let max_str = match unsafe { ptr_to_utf8(max, max_len) } {
        Ok(opt) => opt,
        Err(()) => return,
    };

    // SAFETY: Caller guarantees `d` points to a valid `TermDict`.
    let TermDict(dict) = unsafe { &*d };

    for (k, e) in dict.range_iter(min_str, min_inclusive, max_str, max_inclusive) {
        // SAFETY: `cb` honours the documented signature; `k`'s bytes are
        // valid through the callback call.
        let rc = unsafe {
            cb(
                k.as_ptr().cast::<c_char>(),
                k.len() as size_t,
                ctx,
                e.num_docs as size_t,
            )
        };
        if rc != 0 {
            break;
        }
    }
}

/// Iterate every term within Levenshtein distance `max_dist` of `prefix`.
/// When `prefix_mode` is true, matched terms may have any suffix beyond
/// the prefix-anchored match — backs the fuzzy and prefix-fuzzy paths at
/// `src/query.c:617` (`iterateExpandedTerms`).
///
/// Yields `(term, score, num_docs, dist)` per match — the same columns
/// the C `TrieIterator_Next` writes (minus the always-NULL payload).
/// `score` is widened to `f64` at the FFI boundary.
///
/// NULL `d`, NULL `cb`, or non-UTF-8 `prefix` collapse to a no-op.
///
/// # Safety
/// - `d` must either be NULL or point to a valid [`TermDict`] obtained
///   from [`TermDict_New`].
/// - `prefix` must point to a readable byte sequence of length
///   `prefix_len`. NULL is accepted only when `prefix_len == 0`.
/// - `cb` and `ctx`: same contract as [`TermDict_IterateContains`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TermDict_IterateDfa(
    d: *const TermDict,
    prefix: *const c_char,
    prefix_len: size_t,
    max_dist: u32,
    prefix_mode: bool,
    cb: TermDictDfaCallback,
    ctx: *mut c_void,
) {
    if d.is_null() {
        return;
    }
    let Some(cb) = cb else { return };

    // SAFETY: Caller guarantees `prefix` honours the `(ptr, len)` contract.
    let prefix_str = match unsafe { ptr_to_utf8(prefix, prefix_len) } {
        Ok(Some(s)) => s,
        Ok(None) => "",
        Err(()) => return,
    };

    // SAFETY: Caller guarantees `d` points to a valid `TermDict`.
    let TermDict(dict) = unsafe { &*d };

    for (k, entry, dist) in dict.iterate_dfa(prefix_str, max_dist, prefix_mode) {
        // SAFETY: `cb` honours the documented signature; `k`'s bytes are
        // valid through the callback call.
        let rc = unsafe {
            cb(
                k.as_ptr().cast::<c_char>(),
                k.len() as size_t,
                ctx,
                entry.score as f64,
                entry.num_docs as size_t,
                dist,
            )
        };
        if rc != 0 {
            break;
        }
    }
}
