/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C-callable surface for [`trie_rs::str::TermDictionary`].
//!
//! Backs the optional Rust path of the `sp->terms` term dictionary, gated
//! by the `USE_RUST_TERM_DICT` cmake option. PR2 of the RDB-delegation
//! stack: it provides the construct / iterate / raw-insert primitives that
//! the callback-shaped C codec ([`TermStream_RdbSave`] / `TermStream_RdbLoad`
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
//! [`trie_rs::str::TermEntry`] has no payload field.
//!
//! ## Case-folding
//!
//! [`TermDict_InsertRaw`] forwards to [`trie_rs::str::TermDictionary::insert`],
//! which lowercases via [`str::to_lowercase`]. Old C-trie RDBs were written
//! after the libnu ASCII fold (`runeBufFill` in `src/trie/trie.c`) — bytes
//! round-trip identically on ASCII; non-ASCII keys may diverge on
//! NFKC/ligature decomposition. Documented hazard, not a bug.

use std::ffi::c_char;

use libc::size_t;
use trie_rs::str::iter::Iter;
use trie_rs::str::term_dict::{TermDictionary, TermEntry};

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
