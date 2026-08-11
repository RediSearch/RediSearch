/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The suffix trie, which indexes terms by their suffixes so that a query that
//! is not front-anchored can be answered without a full scan.

use std::{
    ffi::{c_char, c_int, c_void},
    marker::PhantomData,
    ops::ControlFlow,
    ptr,
};

use ffi::{
    SuffixCtx, SuffixType, SuffixType_SUFFIX_TYPE_CONTAINS, SuffixType_SUFFIX_TYPE_SUFFIX,
    SuffixType_SUFFIX_TYPE_WILDCARD,
};

use crate::LoweredPattern;

/// Which side(s) of a term a [`SuffixTrie::iterate_contains`] walk anchors on.
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
/// installed by [`SuffixTrie::iterate_contains`] or
/// [`SuffixTrie::iterate_wildcard`].
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

/// Outcome of [`SuffixTrie::iterate_wildcard`].
///
/// Neither variant is a failure: declining a pattern the suffix trie cannot
/// anchor on is a routine handover to the primary terms trie. The distinction is
/// carried in the type because it also decides who owns the pattern afterwards.
#[derive(Debug)]
#[must_use = "a declined walk leaves the pattern un-walked, and dropping it here \
              silently skips the terms-trie fallback"]
pub enum SuffixWalk {
    /// The walk ran and every matching term was delivered to the callback.
    ///
    /// The pattern is not returned: the walk answered it, so the caller has
    /// nothing left to do with it.
    Walked,
    /// The pattern held no literal run to anchor on, so nothing was visited.
    ///
    /// The pattern comes back ready for a fallback walk over the primary terms
    /// trie with [`crate::TermsTrie::iterate_wildcard`].
    NoAnchor(LoweredPattern),
}

/// A safe wrapper around a C [`ffi::Trie`] used as a *suffix* index: its keys
/// are the suffixes of the indexed terms, and each node's payload lists the
/// terms carrying that suffix.
#[derive(Debug)]
#[repr(transparent)]
pub struct SuffixTrie {
    inner: ffi::Trie,
    // [`ffi::Trie`] is an opaque ZST, which would make `SuffixTrie` `Send + Sync`
    // by default. The C trie is neither: it is mutated under the owning spec's lock.
    // This `PhantomData` removes the auto traits.
    _phantom: PhantomData<*mut ffi::Trie>,
}

impl SuffixTrie {
    /// Borrow an existing C Suffix Trie pointer as a shared reference.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a valid, non-null pointer to an `ffi::Trie`, must
    ///    remain live for `'a`, and must not be mutated for the duration.
    /// 2. Every node payload in that trie must be a valid `suffixData`.
    pub const unsafe fn from_raw<'a>(ptr: *const ffi::Trie) -> &'a Self {
        debug_assert!(!ptr.is_null(), "C Suffix Trie pointer cannot be null");
        // SAFETY: guaranteed by caller (1., 2.)
        unsafe { &*ptr.cast::<Self>() }
    }

    /// Borrow an existing C Suffix Trie pointer as an exclusive reference.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a valid, non-null pointer to an `ffi::Trie`, must remain
    ///    live for `'a`, and must have no other aliasing references for the duration.
    /// 2. Every node payload in that trie must be a valid `suffixData`.
    /// 3. The trie's free callback must point to [`ffi::suffixTrie_freeCallback`].
    pub const unsafe fn from_raw_mut<'a>(ptr: *mut ffi::Trie) -> &'a mut Self {
        debug_assert!(!ptr.is_null(), "C Suffix Trie pointer cannot be null");
        // SAFETY: guaranteed by caller (1., 2., 3.)
        unsafe { &mut *ptr.cast::<Self>() }
    }

    /// Return a raw pointer to the underlying [`ffi::Trie`].
    pub const fn as_ptr(&self) -> *mut ffi::Trie {
        ptr::from_ref(self).cast_mut().cast::<ffi::Trie>()
    }

    /// Visit every term matched by `pattern` through the *suffix* trie, which
    /// indexes terms by their suffixes to answer contains/suffix queries without
    /// a full scan.
    ///
    /// `pattern` is the rune key and `mode` selects a suffix or contains match.
    /// Each matching term is delivered to `callback` as a UTF-8 byte string —
    /// already converted from runes by the suffix trie — with no document count;
    /// the callback returns [`ControlFlow`] to continue or stop.
    ///
    /// The suffix-trie walk is recursive and does not check for a timeout, so
    /// this method takes no deadline. An empty `pattern` visits nothing.
    pub fn iterate_contains<F>(&self, pattern: &[ffi::rune], mode: SuffixMode, mut callback: F)
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
            trie: self.as_ptr(),
            rune: pattern.as_ptr().cast_mut(),
            runelen: pattern.len(),
            type_: mode.into(),
            callback: Some(suffix_trampoline::<F>),
            cbCtx: std::ptr::from_mut(&mut callback).cast(),
            // The suffix walk ignores these; keep them zeroed.
            timeout: std::ptr::null_mut(),
            skipTimeoutChecks: false,
        };
        // SAFETY: every `suffix_ctx` field is initialised above with a valid
        // value, `self` borrows a valid `Trie` whose payloads the walk may cast
        // to `suffixData`, the pattern pointer/len describe a live rune slice,
        // and the callback closure outlives the call.
        unsafe {
            ffi::Suffix_IterateContains(std::ptr::from_mut(&mut suffix_ctx));
        }
    }

    /// Visit every term matching a wildcard `pattern` through the *suffix* trie,
    /// which indexes terms by their suffixes so a pattern that is not
    /// front-anchored can be answered without a full scan.
    ///
    /// Each matching term is delivered to `callback` as a byte string — already
    /// converted from runes by the suffix trie — with no document count; the
    /// callback returns [`ControlFlow`] to continue or stop. A term reachable
    /// under several matching suffix keys is delivered once per key, so
    /// `callback` may see duplicates.
    ///
    /// A [`Break`](ControlFlow::Break) carries the same weak guarantee as in
    /// [`crate::TermsTrie::iterate_wildcard`]: it is honoured outright only when
    /// the anchor token the walk settles on is `*`-terminated, and otherwise ends
    /// just the current suffix key's terms. Which token is chosen is not the
    /// caller's to predict, so treat the weaker guarantee as the contract.
    ///
    /// The suffix trie can only answer a pattern that contains a literal run to
    /// anchor on. When it does not, nothing is visited and the pattern is handed
    /// back as [`SuffixWalk::NoAnchor`] so the caller can fall back to
    /// [`crate::TermsTrie::iterate_wildcard`].
    ///
    /// `pattern` is consumed so that a declined walk can hand it back through
    /// [`SuffixWalk::NoAnchor`] for the fallback.
    ///
    /// `timeout` bounds the walk, as for [`crate::TermsTrie::iterate_wildcard`].
    pub fn iterate_wildcard<F>(
        &self,
        mut pattern: LoweredPattern,
        mut timeout: Option<ffi::timespec>,
        mut callback: F,
    ) -> SuffixWalk
    where
        F: FnMut(&[u8]) -> ControlFlow<()>,
    {
        let (timeout_ptr, skip_timeout_checks) = match &mut timeout {
            Some(timeout) => (ptr::from_mut(timeout), false),
            None => (ptr::null_mut(), true),
        };

        let mut suffix_ctx = SuffixCtx {
            trie: self.as_ptr(),
            rune: pattern.runes.as_mut_ptr(),
            runelen: pattern.len(),
            // The wildcard walk never reads this back, but a stale `Contains`
            // would misdescribe the context in a debugger.
            type_: SuffixType_SUFFIX_TYPE_WILDCARD,
            callback: Some(suffix_trampoline::<F>),
            cbCtx: std::ptr::from_mut(&mut callback).cast(),
            timeout: timeout_ptr,
            skipTimeoutChecks: skip_timeout_checks,
        };
        // SAFETY: every `suffix_ctx` field is initialised above with a valid
        // value — `self` borrows a valid `Trie` whose payloads the walk may cast
        // to `suffixData`, the rune pointer describes
        // a live pattern followed by the sentinel the walk reads
        // (`LoweredPattern` invariant), the callback closure outlives the call,
        // and `timeout` is null or points to a valid `timeout` argument.
        let used = unsafe { ffi::Suffix_IterateWildcard(std::ptr::from_mut(&mut suffix_ctx)) };
        if used == 0 {
            SuffixWalk::NoAnchor(pattern)
        } else {
            SuffixWalk::Walked
        }
    }

    /// Remove `term` and all of its suffixes from the suffix trie.
    ///
    /// An empty `term` is a caller-level mistake and is ignored after
    /// a debug assertion.
    pub fn delete(&mut self, term: &[u8]) {
        debug_assert!(
            !term.is_empty(),
            "an empty term is never inserted into a suffix trie"
        );

        // The C side asserts on a term that decodes to no runes rather than
        // handling one, and an empty term is the reachable way to get there.
        if term.is_empty() {
            return;
        }

        let Ok(term_len) = u32::try_from(term.len()) else {
            // The C API takes a `uint32_t` length. A term this long was never
            // inserted, and truncating would delete an unrelated entry.
            return;
        };

        // SAFETY: `self` borrows a valid `Trie` whose payloads are `suffixData`
        // and whose free callback releases them, so unregistering the term and
        // freeing the nodes it empties is sound. `term`/`term_len` describe a
        // readable byte slice for the call.
        unsafe {
            ffi::deleteSuffixTrie(self.as_ptr(), term.as_ptr() as *const c_char, term_len);
        }
    }
}
