/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The primary terms trie, which maps a term to the documents indexing it.

use std::{
    ffi::{c_char, c_int, c_void},
    marker::PhantomData,
    ops::ControlFlow,
    ptr::{self, NonNull},
};

use string_utils::runes::runes_to_bytes;

use crate::LoweredPattern;

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

/// Result of a decrement operation on the C Trie.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::FromRepr)]
#[repr(u32)]
pub enum TermsTrieDecrResult {
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
    /// * `TermsTrieDecrResult::NotFound` - Term not found in trie
    /// * `TermsTrieDecrResult::Updated` - numDocs decremented, still > 0
    /// * `TermsTrieDecrResult::Deleted` - numDocs reached 0, term deleted
    /// * `TermsTrieDecrResult::Unsupported` - term too long/unconvertible; never inserted
    pub fn decrement_num_docs(&mut self, term: &[u8], delta: u64) -> TermsTrieDecrResult {
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
        TermsTrieDecrResult::from_repr(result).unwrap_or(TermsTrieDecrResult::NotFound)
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
    pub fn iterate_all(&self) -> TermsTrieAllIterator<'_> {
        TermsTrieAllIterator::new(self)
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

/// Iterator over every term in a trie.
///
/// Reached through [`TermsTrie::iterate_all`], the public entry point.
pub struct TermsTrieAllIterator<'a> {
    ptr: NonNull<ffi::TrieIterator>,
    _borrow: PhantomData<&'a TermsTrie>,
}

impl<'a> TermsTrieAllIterator<'a> {
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

impl Iterator for TermsTrieAllIterator<'_> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let runes = self.advance()?;
            // A key that cannot be converted back is skipped rather than
            // ending the walk: `runes_to_bytes` only fails for a key longer
            // than the trie can represent, which no stored key can be.
            if let Ok(term) = runes_to_bytes(runes) {
                return Some(term);
            }
        }
    }
}

impl<'a> IntoIterator for &'a TermsTrie {
    type Item = Vec<u8>;
    type IntoIter = TermsTrieAllIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iterate_all()
    }
}

impl Drop for TermsTrieAllIterator<'_> {
    fn drop(&mut self) {
        // SAFETY: `self.ptr` was allocated by `Trie_IterateAll` and has not
        // been freed yet (owned exclusively by this iterator).
        unsafe { ffi::TrieIterator_Free(self.ptr.as_ptr()) };
    }
}
