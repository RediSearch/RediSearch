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
    marker::PhantomData,
    ops::ControlFlow,
    ptr::{self, NonNull},
};

use ffi::{
    SuffixCtx, SuffixType, SuffixType_SUFFIX_TYPE_CONTAINS, SuffixType_SUFFIX_TYPE_SUFFIX,
    SuffixType_SUFFIX_TYPE_WILDCARD,
};

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
/// by [`TermsTrie::iterate_contains`] or [`TermsTrie::iterate_wildcard`].
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

/// A lowercased wildcard pattern, in both encodings the wildcard walks need.
///
/// The lowercasing is the *caller's* job and is not checked here. A pattern
/// built from unfolded runes matches case-sensitively rather than erroring,
/// because the tries store folded keys and nothing on the way in can tell an
/// already-folded rune from one that was never folded.
///
/// The walks read one element *past* the pattern — its value decides the match
/// for a pattern ending in `*` — so the runes carry a zero sentinel past their
/// content, which a plain `Vec` of the converted pattern would not have. This
/// type owns that layout instead of leaving each call site to remember it.
#[derive(Debug)]
pub struct LoweredPattern {
    /// The content runes followed by one zero sentinel, so `len()` is
    /// `runes.len() - 1`.
    runes: Vec<ffi::rune>,
}

impl LoweredPattern {
    /// Build a pattern from its lowercased runes, appending the sentinel.
    ///
    /// `runes` must hold only the content — the sentinel is added here.
    ///
    /// Returns [`None`], which every caller treats as matching nothing, rather
    /// than building a pattern that could not name a stored term:
    ///
    /// - a rune slice longer than [`MAX_RUNE_STR_LEN`](ffi::MAX_RUNE_STR_LEN),
    ///   which term insertion declines the same way. A pattern that *is* built
    ///   therefore fits the `int` length the walks take;
    /// - a slice holding a zero rune, which collides with the sentinel layout
    ///   this type guarantees — a consumer scanning for the zero would see a
    ///   truncated pattern — and which no stored term contains.
    pub fn new(runes: &[ffi::rune]) -> Option<Self> {
        if runes.contains(&0) {
            return None;
        }
        if runes.len() > ffi::MAX_RUNE_STR_LEN as usize {
            return None;
        }

        let mut runes = runes.to_vec();
        runes.push(0);
        Some(Self { runes })
    }

    /// The number of content runes, excluding the sentinel.
    pub const fn len(&self) -> usize {
        self.runes.len() - 1
    }

    /// Whether the pattern has no content.
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
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
    /// trie with [`TermsTrie::iterate_wildcard`].
    NoAnchor(LoweredPattern),
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
    /// Term too long/unconvertible for the trie; never inserted.
    Unsupported = 3,
}

/// A safe wrapper around a C [`ffi::Trie`] used for terms tries.
#[derive(Debug)]
#[repr(transparent)]
pub struct TermsTrie {
    inner: ffi::Trie,
    // [`ffi::Trie`] is an opaque ZST, which would make `TermsTrie` `Send + Sync` by
    // default. The C trie is neither: it is mutated under the owning spec's lock.
    // This `PhantomData` removes the auto traits.
    _phantom: PhantomData<*mut ffi::Trie>,
}

impl TermsTrie {
    /// Borrow an existing C Terms Trie pointer as a shared reference.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a valid, non-null pointer to a terms `ffi::Trie`, must
    ///    remain live for `'a`, and must not be mutated for the duration.
    pub const unsafe fn from_raw<'a>(ptr: *const ffi::Trie) -> &'a Self {
        debug_assert!(!ptr.is_null(), "C Trie pointer cannot be null");
        // SAFETY: guaranteed by caller (1.)
        unsafe { &*ptr.cast::<Self>() }
    }

    /// Borrow an existing C Terms Trie pointer as an exclusive reference.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a valid, non-null pointer to a terms `ffi::Trie`, must remain
    ///    live for `'a`, and must have no other aliasing references for the duration.
    pub const unsafe fn from_raw_mut<'a>(ptr: *mut ffi::Trie) -> &'a mut Self {
        debug_assert!(!ptr.is_null(), "C Trie pointer cannot be null");
        // SAFETY: guaranteed by caller (1.)
        unsafe { &mut *ptr.cast::<Self>() }
    }

    /// Return a raw pointer to the underlying terms [`ffi::Trie`].
    pub const fn as_ptr(&self) -> *mut ffi::Trie {
        ptr::from_ref(self).cast_mut().cast::<ffi::Trie>()
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
    /// * `CTrieDecrResult::Unsupported` - term too long/unconvertible; never inserted
    pub fn decrement_num_docs(&mut self, term: &[u8], delta: u64) -> CTrieDecrResult {
        // SAFETY: We're calling the C function with valid parameters.
        // The term is passed as a UTF-8 byte slice, and the C function
        // handles the conversion to runes internally via runeBufFill.
        // The C function mutates the Trie by decrementing numDocs and
        // potentially deleting nodes.
        let result = unsafe {
            ffi::Trie_DecrementNumDocs(
                self.as_ptr(),
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
    /// Returns `0` for input that cannot correspond to a stored term — invalid
    /// UTF-8, or a term longer than the trie can hold — since such a term can
    /// never have been inserted.
    ///
    /// # Safety
    ///
    /// This function is safe to call if the `TermsTrie` was created safely.
    pub fn num_docs(&self, term: &[u8]) -> usize {
        // Terms longer than the trie can store are never present, so report zero
        // without a lookup (mirrors the C insertion/decrement guards). This also
        // bounds the rune count to `term.len()`, keeping it within `t_len` so the
        // narrowing cast below cannot wrap and match a shorter term by mistake.
        if term.len() > ffi::TRIE_INITIAL_STRING_LEN as usize * std::mem::size_of::<ffi::rune>() {
            return 0;
        }

        // The rune conversion decodes UTF-8 without bounds-checking multibyte
        // sequences against the slice end, so a truncated or invalid sequence
        // could read past `term`. Reject non-UTF-8 input up front; such a term
        // cannot match a stored (rune-decoded) term anyway.
        if std::str::from_utf8(term).is_err() {
            return 0;
        }

        // A UTF-8 string yields at most as many runes as bytes; the extra slot
        // leaves room for the conversion to write a trailing rune.
        let mut runes = vec![0 as ffi::rune; term.len() + 1];
        // SAFETY: `term` is valid UTF-8 of `term.len()` bytes, so the decode
        // stays within the slice, and `runes` has room for `term.len() + 1`
        // runes, so the conversion writes within bounds.
        let rlen = unsafe {
            ffi::strToRunesN(
                term.as_ptr() as *const c_char,
                term.len(),
                runes.as_mut_ptr(),
            )
        };
        // SAFETY: `self` borrows a valid `Trie` (`TermsTrie` invariant); `runes`/
        // `rlen` describe a valid rune slice, and `rlen <= term.len()` fits
        // `t_len` (guarded above).
        let node = unsafe {
            ffi::Trie_GetNode(
                self.as_ptr(),
                runes.as_ptr(),
                rlen as ffi::t_len,
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
    pub fn iterate_contains<F>(
        &self,
        pattern: &[ffi::rune],
        prefix: bool,
        suffix: bool,
        mut timeout: Option<ffi::timespec>,
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
        let (timeout, skip_timeout_checks) = match &mut timeout {
            Some(timeout) => (ptr::from_mut(timeout), false),
            None => (ptr::null_mut(), true),
        };
        // SAFETY: `self` borrows a valid terms `Trie`; `pattern` points to
        // `pattern.len()` runes; `&mut callback` stays alive for the whole
        // call, so the `ctx` the trampoline reconstitutes is valid; and
        // `timeout` is null or points to a valid `timeout` argument.
        unsafe {
            ffi::Trie_IterateContains(
                self.as_ptr(),
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

    /// Visit every term matching a wildcard `pattern` — one admitting `*` (any
    /// run of characters) and `?` (exactly one) — by walking the primary terms
    /// trie.
    ///
    /// For each match the callback receives the term's runes and the number of
    /// documents indexed under it, and returns [`ControlFlow`] to continue or stop
    /// the walk early (e.g. once an expansion cap is reached). The walk honours a
    /// [`Break`] only on the sub-tree path it takes for a pattern ending in `*`;
    /// otherwise it keeps visiting terms, so a caller enforcing a cap must make
    /// every further callback a no-op itself.
    ///
    /// An empty pattern visits nothing: it can match only the empty term, which
    /// this trie never holds — a zero-length key is refused on insertion, so an
    /// indexed empty value exists only as an inverted index. A caller that wants
    /// to match it has to open that index itself.
    ///
    /// `timeout` bounds the walk: `Some(deadline)` aborts it once the deadline
    /// passes, while `None` runs it to completion with no deadline.
    ///
    /// [`Break`]: ControlFlow::Break
    ///
    pub fn iterate_wildcard<F>(
        &self,
        pattern: &LoweredPattern,
        mut timeout: Option<ffi::timespec>,
        mut callback: F,
    ) where
        F: FnMut(&[ffi::rune], usize) -> ControlFlow<()>,
    {
        // An empty pattern can match only the empty term, which this trie never
        // holds (see the doc comment), so skip the walk rather than have it
        // discover that.
        if pattern.is_empty() {
            return;
        }
        // As in `iterate_contains`: the walk only honours a deadline when timeout
        // checks are enabled and treats a null deadline as already-expired, so the
        // two must move together.
        let (timeout, skip_timeout_checks) = match &mut timeout {
            Some(timeout) => (ptr::from_mut(timeout), false),
            None => (ptr::null_mut(), true),
        };
        // SAFETY: `self` borrows a valid terms `Trie`; `pattern` addresses its
        // content runes followed by the readable zero sentinel the matcher
        // requires (`LoweredPattern` invariant); `&mut callback` stays alive for
        // the whole call, so the `ctx` the trampoline reconstitutes is valid; and
        // `timeout` is null or points to a valid `timeout` argument.
        unsafe {
            ffi::Trie_IterateWildcard(
                self.as_ptr(),
                pattern.runes.as_ptr(),
                pattern.len() as c_int,
                Some(range_trampoline::<F>),
                std::ptr::from_mut(&mut callback).cast(),
                timeout,
                skip_timeout_checks,
            );
        }
    }

    /// Iterate every term in the trie, in the trie's natural iteration order,
    /// with no filter or distance constraint.
    pub fn iterate_all(&self) -> CTrieAllIterator<'_> {
        CTrieAllIterator::new(self)
    }

    /// Remove `term` from the trie.
    ///
    /// Returns whether an entry was actually removed.
    pub fn delete(&mut self, term: &[u8]) -> bool {
        // Terms longer than the trie can store are never present, so report a
        // miss without a lookup. The C function applies the same cap, but only
        // after the rune conversion below has already run.
        if term.len() > ffi::TRIE_INITIAL_STRING_LEN as usize * std::mem::size_of::<ffi::rune>() {
            return false;
        }

        // SAFETY: `self` borrows a valid Terms `Trie`, and `term`/`term_len`
        // describe a valid byte slice for the duration of the call.
        let removed =
            unsafe { ffi::Trie_Delete(self.as_ptr(), term.as_ptr() as *const c_char, term.len()) };

        removed != 0
    }
}

/// A safe wrapper around a C [`ffi::Trie`] used for suffix tries.
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
    /// 1. `ptr` must be a valid, non-null pointer to a suffix `ffi::Trie`, must
    ///    remain live for `'a`, and must not be mutated for the duration.
    pub const unsafe fn from_raw<'a>(ptr: *const ffi::Trie) -> &'a Self {
        debug_assert!(!ptr.is_null(), "C Suffix Trie pointer cannot be null");
        // SAFETY: guaranteed by caller (1.)
        unsafe { &*ptr.cast::<Self>() }
    }

    /// Borrow an existing C Suffix Trie pointer as an exclusive reference.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a valid, non-null pointer to a suffix `ffi::Trie`, must remain
    ///    live for `'a`, and must have no other aliasing references for the duration.
    pub const unsafe fn from_raw_mut<'a>(ptr: *mut ffi::Trie) -> &'a mut Self {
        debug_assert!(!ptr.is_null(), "C Suffix Trie pointer cannot be null");
        // SAFETY: guaranteed by caller (1.)
        unsafe { &mut *ptr.cast::<Self>() }
    }

    /// Return a raw pointer to the underlying suffix [`ffi::Trie`].
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
        // value, `self` borrows a valid suffix `Trie`, the pattern pointer/len
        // describe a live rune slice, and the callback closure outlives the call.
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
    /// [`TermsTrie::iterate_wildcard`]: it is honoured outright only when the
    /// anchor token the walk settles on is `*`-terminated, and otherwise ends just
    /// the current suffix key's terms. Which token is chosen is not the caller's
    /// to predict, so treat the weaker guarantee as the contract.
    ///
    /// The suffix trie can only answer a pattern that contains a literal run to
    /// anchor on. When it does not, nothing is visited and the pattern is handed
    /// back as [`SuffixWalk::NoAnchor`] so the caller can fall back to
    /// [`TermsTrie::iterate_wildcard`].
    ///
    /// `pattern` is consumed so that a declined walk can hand it back through
    /// [`SuffixWalk::NoAnchor`] for the fallback.
    ///
    /// `timeout` bounds the walk, as for [`TermsTrie::iterate_wildcard`].
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
        // value — `self` borrows a valid suffix `Trie`, the rune pointer describes
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

        // SAFETY: `self` borrows a valid Suffix `Trie`, and `term`/`term_len`
        // describe a valid byte slice for the duration of the call.
        unsafe {
            ffi::deleteSuffixTrie(self.as_ptr(), term.as_ptr() as *const c_char, term_len);
        }
    }
}

/// Iterator over every term in a trie.
///
/// Reached through [`TermsTrie::iterate_all`], the public entry point.
pub struct CTrieAllIterator<'a> {
    ptr: NonNull<ffi::TrieIterator>,
    _borrow: PhantomData<&'a TermsTrie>,
}

impl<'a> CTrieAllIterator<'a> {
    fn new(trie: &'a TermsTrie) -> Self {
        // SAFETY: `trie` borrows a valid `Trie`.
        let ptr = unsafe { ffi::Trie_IterateAll(trie.as_ptr()) };
        Self {
            // `Trie_IterateAll` allocates via `rm_calloc`, which aborts the
            // process on allocation failure rather than returning null.
            ptr: NonNull::new(ptr).expect("Trie_IterateAll returned null"),
            _borrow: PhantomData,
        }
    }

    /// Advance the walk, returning the next term's rune key, or `None` once
    /// every term has been visited.
    fn advance(&mut self) -> Option<&[ffi::rune]> {
        let mut ptr: *mut ffi::rune = std::ptr::null_mut();
        let mut len: ffi::t_len = 0;
        let mut score: f32 = 0.0;

        // SAFETY: `self.ptr` is a valid, non-exhausted `TrieIterator`. `score`
        // is a valid `&mut f32` the C function may write through unconditionally
        // on a match; `numDocs`, `payload` and `matchCtx` are unused, and the C
        // side skips each of them when null.
        let has_next = unsafe {
            ffi::TrieIterator_Next(
                self.ptr.as_ptr(),
                &mut ptr,
                &mut len,
                std::ptr::null_mut(),
                &mut score,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };

        if has_next == 0 {
            return None;
        }

        // SAFETY: on a match, the C function sets `ptr`/`len` to describe
        // `len` valid, contiguous runes owned by the iterator's internal
        // buffer, borrowed for `self`'s lifetime until the next call.
        Some(unsafe { std::slice::from_raw_parts(ptr, len as usize) })
    }
}

impl Iterator for CTrieAllIterator<'_> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let runes = self.advance()?;
            // A key that cannot be converted back is skipped rather than
            // ending the walk: `runes_to_bytes` only fails for a key longer
            // than the trie can represent, which no stored key can be.
            if let Some(term) = runes_to_bytes(runes) {
                return Some(term);
            }
        }
    }
}

impl<'a> IntoIterator for &'a TermsTrie {
    type Item = Vec<u8>;
    type IntoIter = CTrieAllIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iterate_all()
    }
}

impl Drop for CTrieAllIterator<'_> {
    fn drop(&mut self) {
        // SAFETY: `self.ptr` was allocated by `Trie_IterateAll` and has not
        // been freed yet (owned exclusively by this iterator).
        unsafe { ffi::TrieIterator_Free(self.ptr.as_ptr()) };
    }
}

/// Convert a rune slice back to its original representation.
///
/// Returns `None` if `runes` is longer than the trie can represent; this
/// can only happen for a key that was never actually stored in a trie.
pub fn runes_to_bytes(runes: &[ffi::rune]) -> Option<Vec<u8>> {
    let mut utflen: usize = 0;
    // SAFETY: `runes` points to `runes.len()` valid runes; `utflen` is a
    // valid `&mut usize` out-param.
    let ptr = unsafe { ffi::runesToStr(runes.as_ptr(), runes.len(), &mut utflen) };
    if ptr.is_null() {
        return None;
    }
    // SAFETY: on success `ptr` points to `utflen` initialised bytes in a
    // buffer allocated by `runesToStr` via `rm_calloc` (`RedisModule_Calloc`).
    let bytes = unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), utflen) }.to_vec();
    // SAFETY: `RedisModule_Free` is set during module init and not mutated
    // afterwards.
    let rm_free = unsafe { redis_module::RedisModule_Free.expect("Redis allocator not available") };
    // SAFETY: `ptr` was allocated by `runesToStr` via the Redis allocator and
    // is freed exactly once here, matching the C side's `rm_free`.
    unsafe { rm_free(ptr.cast()) };
    Some(bytes)
}
