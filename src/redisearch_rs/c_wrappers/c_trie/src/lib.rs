/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Rust wrapper for the C Trie API.
//!
//! This crate provides a safe Rust interface to the C Trie implementation,

use std::{
    ffi::{c_char, c_int, c_void},
    ops::ControlFlow,
    ptr::NonNull,
};

use ffi::{SuffixCtx, SuffixType, SuffixType_SUFFIX_TYPE_CONTAINS, SuffixType_SUFFIX_TYPE_SUFFIX};

/// Which side(s) of a term a [`CTrieRef::iterate_suffix`] walk anchors on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuffixMode {
    /// Match terms that end with the pattern (`*llo`).
    Suffix,
    /// Match terms that contain the pattern anywhere (`*ell*`).
    Contains,
}

impl From<SuffixMode> for SuffixType {
    /// The [`SuffixType`] discriminant for the given [`SuffixMode`].
    fn from(mode: SuffixMode) -> Self {
        match mode {
            SuffixMode::Suffix => SuffixType_SUFFIX_TYPE_SUFFIX,
            SuffixMode::Contains => SuffixType_SUFFIX_TYPE_CONTAINS,
        }
    }
}

/// Adapts a [`ffi::TrieRangeCallback`] to a Rust closure handed back through
/// the opaque `ctx` pointer for every matching term.
///
/// A panic escaping this `extern "C"` function aborts the process rather than
/// unwinding across the FFI boundary, keeping that boundary sound.
///
/// # Safety
///
/// - `ctx` must be the `&mut F` passed as the iterator's `ctx` argument,
///   exclusively borrowed for the duration of the walk.
/// - `runes` must point to `len` valid runes, or `len` must be `0` (in which
///   case `runes` is ignored).
///
/// Both hold when the trie invokes this through the function pointer installed
/// by [`CTrieRef::iterate_contains`].
unsafe extern "C" fn range_trampoline<F>(
    runes: *const ffi::rune,
    len: usize,
    ctx: *mut c_void,
    _payload: *mut c_void,
    num_docs: usize,
) -> c_int
where
    F: FnMut(&[ffi::rune], usize) -> ControlFlow<()>,
{
    // SAFETY: `ctx` is the `&mut F` forwarded unchanged by the trie; the closure
    // outlives every callback invocation of a single iteration call.
    let callback = unsafe { &mut *(ctx as *mut F) };
    let runes = if len == 0 {
        // The pointer may be dangling/null for an empty key.
        &[][..]
    } else {
        // SAFETY: the trie passes `len` valid, contiguous runes.
        unsafe { std::slice::from_raw_parts(runes, len) }
    };

    match callback(runes, num_docs) {
        // 0 continues the walk; any other value stops it.
        ControlFlow::Continue(()) => 0,
        ControlFlow::Break(()) => 1,
    }
}

/// Adapts a [`ffi::TrieSuffixCallback`] to a Rust closure handed back through
/// the opaque `ctx` pointer for every matching term.
///
/// A panic escaping this `extern "C"` function aborts the process rather than
/// unwinding across the FFI boundary, keeping that boundary sound.
///
/// # Safety
///
/// - `ctx` must be the `&mut F` passed as the suffix iterator's context pointer,
///   exclusively borrowed for the duration of the walk.
/// - `s` must point to `len` valid bytes, or `len` must be `0`.
///
/// Both hold when the suffix trie invokes this through the function pointer
/// installed by [`CTrieRef::iterate_suffix`].
unsafe extern "C" fn suffix_trampoline<F>(
    s: *const c_char,
    len: usize,
    ctx: *mut c_void,
    _payload: *mut c_void,
) -> c_int
where
    F: FnMut(&[u8]) -> ControlFlow<()>,
{
    // SAFETY: `ctx` is the `&mut F` forwarded unchanged by the suffix trie.
    let callback = unsafe { &mut *(ctx as *mut F) };
    let bytes = if len == 0 {
        &[][..]
    } else {
        // SAFETY: the suffix trie passes `len` valid, contiguous bytes.
        unsafe { std::slice::from_raw_parts(s.cast::<u8>(), len) }
    };
    match callback(bytes) {
        ControlFlow::Continue(()) => 0,
        ControlFlow::Break(()) => 1,
    }
}

/// Result of a decrement operation on the C Trie.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::FromRepr)]
#[repr(u32)]
pub enum CTrieDecrResult {
    /// Term not found in the trie.
    NotFound = 0,
    /// numDocs decremented, term still has documents.
    Updated = 1,
    /// numDocs reached 0, term was deleted from the trie.
    Deleted = 2,
}

/// Wrapper around the C Trie pointer for safe FFI operations.
///
/// This struct does NOT own the C Trie - it's just a wrapper for
/// calling FFI functions. The caller is responsible for managing
/// the lifetime of the C Trie.
#[derive(Debug)]
pub struct CTrieRef {
    ptr: *mut ffi::Trie,
}

impl CTrieRef {
    /// Create a new wrapper around an existing C Trie pointer.
    ///
    /// # Safety
    ///
    /// The caller must ensure that:
    /// - `ptr` is a valid pointer to a C `Trie` struct
    /// - The C Trie outlives this wrapper
    /// - No other code frees the C Trie while this wrapper exists
    pub unsafe fn from_raw(ptr: *mut ffi::Trie) -> Self {
        debug_assert!(!ptr.is_null(), "C Trie pointer cannot be null");
        Self { ptr }
    }

    /// Decrement the numDocs count for a term in the C Trie.
    ///
    /// # Arguments
    ///
    /// * `term` - The UTF-8 encoded term bytes
    /// * `delta` - The amount to decrement numDocs by
    ///
    /// # Returns
    ///
    /// * `CTrieDecrResult::NotFound` - Term not found in trie
    /// * `CTrieDecrResult::Updated` - numDocs decremented, still > 0
    /// * `CTrieDecrResult::Deleted` - numDocs reached 0, term deleted
    ///
    /// # Safety
    ///
    /// This function is safe to call if the `CTrieRef` was created safely.
    /// The C function handles UTF-8 to rune conversion internally.
    pub fn decrement_num_docs(&mut self, term: &[u8], delta: u64) -> CTrieDecrResult {
        // SAFETY: We're calling the C function with valid parameters.
        // The term is passed as a UTF-8 byte slice, and the C function
        // handles the conversion to runes internally via runeBufFill.
        // The C function mutates the Trie by decrementing numDocs and
        // potentially deleting nodes.
        let result = unsafe {
            ffi::Trie_DecrementNumDocs(
                self.ptr,
                term.as_ptr() as *const c_char,
                term.len(),
                delta as usize,
            )
        };
        CTrieDecrResult::from_repr(result).unwrap_or(CTrieDecrResult::NotFound)
    }

    /// Number of documents indexed under `term`, or `0` if the term is absent
    /// from the trie.
    ///
    /// `term` is UTF-8 encoded; it is converted to runes internally and looked
    /// up as an exact match. Used to compute a term's inverse document
    /// frequency (IDF).
    ///
    /// Returns `0` for a term longer than the trie can hold, since such a term
    /// can never have been inserted.
    ///
    /// `term` is decoded as the (possibly WTF-8) byte string the key was stored
    /// under, so a term carrying a lone surrogate — a non-BMP codepoint
    /// truncated to a rune at index time — still resolves its real count rather
    /// than being rejected as invalid UTF-8.
    ///
    /// # Safety
    ///
    /// This function is safe to call if the `CTrieRef` was created safely.
    pub fn num_docs(&self, term: &[u8]) -> usize {
        // Terms longer than the trie can store are never present, so report zero
        // without a lookup (mirrors the C insertion/decrement guards). This also
        // bounds the rune count to `term.len()`, keeping it within `t_len` so the
        // narrowing cast below cannot wrap and match a shorter term by mistake.
        if term.len() > ffi::TRIE_INITIAL_STRING_LEN as usize * std::mem::size_of::<ffi::rune>() {
            return 0;
        }

        // Decode to runes with a bounds-checked WTF-8 decoder (never reading past
        // `term`), which preserves surrogate-bearing keys instead of rejecting
        // them, and yields at most `term.len()` runes (so `rlen` fits `t_len`,
        // guarded above).
        let runes = string_utils::runes::utf8_to_runes(term);
        // SAFETY: `self.ptr` is a valid `Trie` (`CTrieRef` invariant); `runes`
        // describes a valid rune slice whose length fits `t_len`.
        let node = unsafe {
            ffi::Trie_GetNode(
                self.ptr,
                runes.as_ptr(),
                runes.len() as ffi::t_len,
                true,
                std::ptr::null_mut(),
            )
        };

        if node.is_null() {
            0
        } else {
            // SAFETY: `node` is a valid, non-null `TrieNode` returned by the
            // lookup above.
            unsafe { ffi::TrieNode_NumDocs(node) }
        }
    }

    /// Visit every term that contains `pattern` (or begins/ends with it), in the
    /// trie's iteration order.
    ///
    /// `pattern` is a rune key. `prefix` and `suffix` together select the match
    /// anchoring: `prefix` alone matches terms starting with `pattern`, `suffix`
    /// alone matches terms ending with it, and both set matches terms containing
    /// it anywhere. With neither set the walk degenerates to an exact-match
    /// lookup of `pattern` itself, which is not a useful way to call this method.
    /// For each match the callback receives the term's runes and the number of
    /// documents indexed under it, and returns [`ControlFlow`] to continue or
    /// stop the walk early (e.g. once an expansion cap is reached).
    ///
    /// An empty `pattern` in suffix/contains mode has nothing to anchor on and
    /// visits nothing, as does a `pattern` longer than the trie's maximum term
    /// length (no stored term can match it).
    ///
    /// `timeout` bounds the walk: `Some(deadline)` aborts it once the deadline
    /// passes, while `None` runs it to completion with no deadline.
    ///
    /// # Safety
    ///
    /// - A `Some` `timeout` must point to a valid [`timespec`](ffi::timespec)
    ///   that stays valid for the duration of the call.
    /// - The wrapped trie must not be mutated, freed, or iterated again for the
    ///   duration of the call — including from within `callback`. The walk caches
    ///   raw node and child pointers and reuses them after each callback
    ///   invocation, so mutating the trie (e.g. deleting a term, or decrementing
    ///   one to zero, through another handle) can free a node mid-walk and leave
    ///   those pointers dangling.
    pub unsafe fn iterate_contains<F>(
        &self,
        pattern: &[ffi::rune],
        prefix: bool,
        suffix: bool,
        timeout: Option<NonNull<ffi::timespec>>,
        mut callback: F,
    ) where
        F: FnMut(&[ffi::rune], usize) -> ControlFlow<()>,
    {
        // No trie term is longer than `t_len` (`u16`) runes, and the prefix and
        // exact walks narrow the pattern length to `t_len` when looking up nodes,
        // truncating anything longer (e.g. 65537 runes becomes 1).
        if pattern.len() > ffi::t_len::MAX as usize {
            return;
        }
        // There is nothing to anchor on, and nothing to visit, so bail out.
        if suffix && pattern.is_empty() {
            return;
        }
        // The iterator only honours a deadline when timeout checks are enabled,
        // and treats a null deadline as already-expired, so the two must move
        // together: a deadline enables the checks, its absence disables them.
        let (timeout, skip_timeout_checks) = match timeout {
            Some(deadline) => (deadline.as_ptr(), false),
            None => (std::ptr::null_mut(), true),
        };
        // SAFETY: `self.ptr` is a valid `Trie` (`CTrieRef` invariant); `pattern`
        // points to `pattern.len()` runes; `&mut callback` stays alive for the
        // whole call, so the `ctx` the trampoline reconstitutes is valid; and
        // `timeout` is null or the valid pointer the caller supplied.
        unsafe {
            ffi::Trie_IterateContains(
                self.ptr,
                pattern.as_ptr(),
                pattern.len() as c_int,
                prefix,
                suffix,
                Some(range_trampoline::<F>),
                std::ptr::from_mut(&mut callback).cast(),
                timeout,
                skip_timeout_checks,
            );
        }
    }

    /// Visit every term matched by `pattern` through the *suffix* trie, which
    /// indexes terms by their suffixes to answer contains/suffix queries without
    /// a full scan.
    ///
    /// `self` must wrap a suffix trie, not the primary terms trie. `pattern` is
    /// the rune key and `mode` selects a suffix or contains match. Each matching
    /// term is delivered to `callback` as a UTF-8 byte string — already converted
    /// from runes by the suffix trie — with no document count; the callback
    /// returns [`ControlFlow`] to continue or stop.
    ///
    /// The suffix-trie walk is recursive and does not check for a timeout, so
    /// this method takes no deadline. An empty `pattern` visits nothing.
    ///
    /// # Safety
    ///
    /// - `self` must wrap a suffix trie, whose nodes carry a suffix-data payload.
    ///   The iterator reinterprets each visited node's payload as that type and
    ///   dereferences it, so passing the primary terms trie (or any trie with a
    ///   different payload) is type confusion and undefined behavior.
    /// - The wrapped trie must not be mutated, freed, or iterated again for the
    ///   duration of the call — including from within `callback`. The walk caches
    ///   raw node and child pointers and reuses them after each callback
    ///   invocation, so mutating the trie through another handle can free a node
    ///   mid-walk and leave those pointers dangling.
    pub unsafe fn iterate_suffix<F>(&self, pattern: &[ffi::rune], mode: SuffixMode, mut callback: F)
    where
        F: FnMut(&[u8]) -> ControlFlow<()>,
    {
        // An empty pattern has no suffix/contains anchor.
        if pattern.is_empty() {
            return;
        }
        // The suffix-trie node lookup narrows the pattern length to `t_len`
        // (`u16`), so an over-long pattern would be truncated and match a shorter
        // key. No stored key can be this long, so nothing can match: bail out
        // rather than look up a truncated pattern.
        if pattern.len() > ffi::t_len::MAX as usize {
            return;
        }
        let mut suffix_ctx = SuffixCtx {
            trie: self.ptr,
            rune: pattern.as_ptr().cast_mut(),
            runelen: pattern.len(),
            cstr: std::ptr::null(),
            cstrlen: 0,
            type_: mode.into(),
            callback: Some(suffix_trampoline::<F>),
            cbCtx: std::ptr::from_mut(&mut callback).cast(),
            // The suffix walk ignores these; keep them zeroed.
            timeout: std::ptr::null_mut(),
            skipTimeoutChecks: false,
        };
        // SAFETY: every `suffix_ctx` field is initialised above with a valid
        // value — `self.ptr` is a valid suffix `Trie` (caller contract), the
        // pattern pointer/len describe a live rune slice, and the callback
        // closure outlives the call.
        unsafe {
            ffi::Suffix_IterateContains(std::ptr::from_mut(&mut suffix_ctx));
        }
    }

    /// Get the raw pointer to the C Trie.
    ///
    /// This is useful when you need to pass the pointer to other C functions.
    pub const fn as_ptr(&self) -> *mut ffi::Trie {
        self.ptr
    }
}
