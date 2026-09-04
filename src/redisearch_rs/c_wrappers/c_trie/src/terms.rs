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

use string_utils::{
    libnu::{NU_MAX_READAHEAD, tail_may_overread},
    runes::runes_to_bytes,
};

use crate::{LoweredPattern, QueryRequestTimeoutHandle, TrieTerm};

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

/// Outcome of [`TermsTrie::iterate_fuzzy`].
///
/// Neither variant is a failure: a pattern the trie will not walk is a routine
/// answer, not an error. The distinction matters to the caller because the two
/// are not interchangeable — a walk that visited nothing has *answered* the
/// pattern (no term is within the distance), while a rejected one never asked.
#[derive(Debug)]
#[must_use = "a rejected pattern was never walked, and treating it as an empty \
              walk claims the index holds no term within the distance"]
pub enum FuzzyWalk {
    /// The walk ran and every term within the distance was delivered to the
    /// callback (up to an early [`ControlFlow::Break`]).
    Walked,
    /// The pattern could not start a walk and nothing was visited, because it
    /// exceeds [`TRIE_MAX_PREFIX`](ffi::TRIE_MAX_PREFIX) runes once decoded.
    PatternRejected,
}

/// Error returned by [`TermsTrie::iterate_fuzzy`] when the distance it was asked
/// for is outside `0..=`[`MAX_LEV_DISTANCE`](ffi::MAX_LEV_DISTANCE).
///
/// The distance reaches C as the size of a stack variable-length array, so a
/// negative one is a negative-length allocation and a large one exhausts the
/// stack before the walk begins — both before any bound the caller could apply
/// afterwards. Refusing them up front is what keeps the walk from being asked
/// for an automaton that cannot be built.
///
/// The offending value is carried so a caller can name it when reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("fuzzy distance must be in 0..={max}, got {value}", value = .0, max = ffi::MAX_LEV_DISTANCE)]
pub struct InvalidFuzzyDistance(i32);

/// Outcome of [`TermsTrie::decrement_num_docs`].
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

/// A safe wrapper around a C [`ffi::Trie`] holding an index's terms.
#[derive(Debug)]
#[repr(transparent)]
pub struct TermsTrie {
    inner: ffi::Trie,
    // `ffi::Trie` is an opaque ZST, which would make `TermsTrie` `Send + Sync` by
    // default. The C trie is neither: it is mutated under the owning spec's lock.
    // This `PhantomData` removes the auto traits.
    _phantom: PhantomData<*mut ffi::Trie>,
}

impl TermsTrie {
    /// Borrow an existing C Terms Trie pointer as a shared reference.
    ///
    /// # Safety
    ///
    /// 1. `ptr` must be a valid, non-null pointer to an [`ffi::Trie`], must
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
    /// 1. `ptr` must be a valid, non-null pointer to an [`ffi::Trie`], must
    ///    remain live for `'a`, and must have no other aliasing references for
    ///    the duration.
    pub const unsafe fn from_raw_mut<'a>(ptr: *mut ffi::Trie) -> &'a mut Self {
        debug_assert!(!ptr.is_null(), "C Trie pointer cannot be null");
        // SAFETY: guaranteed by caller (1.)
        unsafe { &mut *ptr.cast::<Self>() }
    }

    /// Return a raw const pointer to the underlying [`ffi::Trie`].
    pub const fn as_ptr(&self) -> *const ffi::Trie {
        ptr::from_ref(self).cast::<ffi::Trie>()
    }

    /// Return a raw mutable pointer to the underlying [`ffi::Trie`].
    pub const fn as_mut_ptr(&mut self) -> *mut ffi::Trie {
        ptr::from_mut(self).cast::<ffi::Trie>()
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
        // SAFETY: `self` borrows a valid `ffi::Trie`, and `term`/`term.len()`
        // describe a readable byte slice for the duration of the call.
        let result = unsafe {
            ffi::Trie_DecrementNumDocs(
                self.as_mut_ptr(),
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
    /// Returns `0` for input that cannot correspond to a stored term — empty,
    /// invalid UTF-8, or longer than the trie can hold — since such a term can
    /// never have been inserted.
    pub fn num_docs(&self, term: &[u8]) -> usize {
        // A zero-length key is refused on insertion, so the trie cannot hold the
        // empty term and the lookup could only answer zero. Skipping it also
        // keeps an empty slice's pointer — which need not point into an
        // allocation — away from the decode below, which forms `src + slen`
        // before testing that the decode loop is empty. That arithmetic is only
        // defined on a pointer into an object.
        if term.is_empty() {
            return 0;
        }

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

        // A UTF-8 string yields at most as many runes as bytes, so the decode
        // cannot truncate and `rlen` below indexes within `runes`. The extra
        // slot keeps the pointer non-dangling for an empty term.
        let mut runes = vec![0 as ffi::rune; term.len() + 1];
        // SAFETY: `term` is valid UTF-8 of `term.len()` bytes, so the decode
        // stays within the slice, and `runes.len()` bounds the write.
        let rlen = unsafe {
            ffi::strToRunes(
                term.as_ptr() as *const c_char,
                term.len(),
                runes.as_mut_ptr(),
                runes.len(),
            )
        };
        // SAFETY: `self` borrows a valid `ffi::Trie`; `runes`/`rlen` describe a
        // valid rune slice, and `rlen <= term.len()` fits `t_len` (guarded above).
        // `Trie_GetNode` takes a non-const `Trie *` but only traverses it, so
        // nothing is written through the shared pointer.
        let node = unsafe {
            ffi::Trie_GetNode(
                self.as_ptr().cast_mut(),
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
    /// `timeout` supplies the request-owned timeout state for the walk.
    pub fn iterate_contains<F>(
        &self,
        pattern: &[ffi::rune],
        prefix: bool,
        suffix: bool,
        timeout: Option<&QueryRequestTimeoutHandle>,
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
        let timeout = timeout.map_or(ptr::null_mut(), QueryRequestTimeoutHandle::as_mut_ptr);
        // SAFETY: `self` borrows a valid `ffi::Trie`; `pattern` points to
        // `pattern.len()` runes; `&mut callback` stays alive for the whole
        // call, so the `ctx` the trampoline reconstitutes is valid; and
        // `timeout` is null or points to valid request-owned timeout state.
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
            );
        }
    }

    /// Visit every term within `max_dist` edits of `pattern`, in the trie's
    /// iteration order.
    ///
    /// `pattern` is the raw token bytes — it need not be valid UTF-8: unlike the
    /// rune-keyed walks above, the trie decodes and lowercases them itself, the
    /// same way the indexer stored the terms, and it is also there that the
    /// length limit is applied — a pattern longer than
    /// [`TRIE_MAX_PREFIX`](ffi::TRIE_MAX_PREFIX) runes yields
    /// [`FuzzyWalk::PatternRejected`] and visits nothing. The distance is a
    /// Levenshtein distance counted in runes, so a multibyte pattern is measured
    /// by what it decodes to rather than by its byte length.
    ///
    /// For each match the callback receives the term's runes and the number of
    /// documents indexed under it, and returns [`ControlFlow`] to continue or
    /// stop the walk early (e.g. once an expansion cap is reached).
    ///
    /// The walk never yields the empty term: a zero-length key is refused on
    /// insertion, so it exists only as an inverted index and a caller that wants
    /// it has to open that index itself.
    ///
    /// `max_dist` is taken as an [`i32`] because that is the width it travels at
    /// in a parsed query, so the range is checked once, here, rather than at
    /// every call site.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidFuzzyDistance`], having walked nothing, if `max_dist` is
    /// negative or above [`MAX_LEV_DISTANCE`](ffi::MAX_LEV_DISTANCE).
    ///
    /// # Safety
    ///
    /// The wrapped trie must not be mutated, freed, or iterated again for the
    /// duration of the call — including from within `callback`. The walk holds a
    /// stack of raw node pointers across callback invocations, so mutating the
    /// trie (e.g. deleting a term, or decrementing one to zero, through another
    /// handle) can free a node mid-walk and leave those pointers dangling.
    pub unsafe fn iterate_fuzzy<F>(
        &self,
        pattern: &[u8],
        max_dist: i32,
        mut callback: F,
    ) -> Result<FuzzyWalk, InvalidFuzzyDistance>
    where
        F: FnMut(&[ffi::rune], usize) -> ControlFlow<()>,
    {
        // Checked before anything else: an out-of-range distance is a stack VLA
        // C cannot allocate, so it must not reach the automaton at all.
        if max_dist < 0 || max_dist > ffi::MAX_LEV_DISTANCE as i32 {
            return Err(InvalidFuzzyDistance(max_dist));
        }

        // Reject an over-long pattern before copying it. This bounds the padded
        // copy below, which would otherwise duplicate the whole token before C
        // had looked at its length.
        const MAX_UTF8_SEQUENCE_LEN: usize = 4;
        if pattern.len() > ffi::TRIE_MAX_PREFIX as usize * MAX_UTF8_SEQUENCE_LEN {
            return Ok(FuzzyWalk::PatternRejected);
        }

        // The trie lowercases the pattern with `strToLowerRunes`, whose decoder
        // (`nu_utf8_read`) reads a fixed 2–4 bytes from a multibyte lead byte
        // with no bounds check — so a pattern ending in a truncated sequence
        // (e.g. a lone `0xF0`) would read past the end. A token is a byte string
        // that nothing validates as UTF-8, so that is reachable input. Where it
        // can happen, hand C a private copy padded with `NU_MAX_READAHEAD`
        // trailing zero bytes instead; the length passed stays the pattern's own,
        // so the decode, and with it the rune-length threshold, is unchanged.
        //
        // The common case needs no copy: `tail_may_overread` is exact, so every
        // well-formed pattern is decoded in place.
        let padded = tail_may_overread(pattern).then(|| {
            let mut buf = Vec::with_capacity(pattern.len() + NU_MAX_READAHEAD);
            buf.extend_from_slice(pattern);
            buf.resize(pattern.len() + NU_MAX_READAHEAD, 0);
            buf
        });
        // An empty slice carries no allocation, so its pointer is a well-aligned
        // dangling address rather than one into an object. C forms `str + len`
        // to bound its decode loops before testing that the loop is empty, and
        // that arithmetic is only defined on a pointer into an object — so hand
        // it a real one. Nothing is read through it: the length passed is still
        // zero, so every such loop compares equal at the first test.
        static EMPTY_PATTERN: [u8; 1] = [0];
        let pattern_ptr = if pattern.is_empty() {
            EMPTY_PATTERN.as_ptr()
        } else {
            padded.as_deref().unwrap_or(pattern).as_ptr()
        };

        // SAFETY: `self` borrows a valid `ffi::Trie` and
        // `pattern_ptr`/`pattern.len()` describe a live byte slice, which the
        // call only reads (it decodes it into an owned filter). Its decoder can
        // read up to `NU_MAX_READAHEAD` bytes past a truncated trailing sequence,
        // which the padding above put inside the allocation. The returned
        // iterator is owned by us and freed below. `Trie_IterateFuzzy` takes a
        // non-const `Trie *` but only traverses it, so nothing is written
        // through the shared pointer.
        let it = unsafe {
            ffi::Trie_IterateFuzzy(
                self.as_ptr().cast_mut(),
                pattern_ptr.cast::<c_char>(),
                pattern.len(),
                max_dist,
                ffi::TrieMatchMode_TRIE_MATCH_EDIT_DISTANCE,
            )
        };
        // The pattern is decoded into the filter during the call above and not
        // referenced afterwards, so the copy has served its purpose here.
        drop(padded);
        let Some(it) = NonNull::new(it) else {
            return Ok(FuzzyWalk::PatternRejected);
        };
        // The loop below is driven from Rust rather than from a C trampoline, so
        // an unwinding panic in `callback` would otherwise skip the free and leak
        // the iterator together with its edit-distance filter.
        struct OwnedIterator(NonNull<ffi::TrieIterator>);
        impl Drop for OwnedIterator {
            fn drop(&mut self) {
                // SAFETY: we own the iterator returned by `Trie_IterateFuzzy`,
                // it is freed exactly once here and not used afterwards.
                unsafe { ffi::TrieIterator_Free(self.0.as_ptr()) };
            }
        }
        let it = OwnedIterator(it);

        let mut runes: *mut ffi::rune = std::ptr::null_mut();
        let mut len: ffi::t_len = 0;
        // Written on every match whether or not anyone wants it, so it needs a
        // slot; a fuzzy expansion scores its terms through their readers, not
        // through the trie, so the value is dropped.
        let mut score: f32 = 0.0;
        let mut num_docs: usize = 0;

        loop {
            // SAFETY: `it` is the live iterator returned above; the out-params
            // are valid, exclusively borrowed slots. The payload and the
            // per-match context are optional and passed as null, which the
            // iterator and the edit-distance filter both check for before
            // writing.
            let has_match = unsafe {
                ffi::TrieIterator_Next(
                    it.0.as_ptr(),
                    &mut runes,
                    &mut len,
                    std::ptr::null_mut(),
                    &mut score,
                    &mut num_docs,
                    std::ptr::null_mut(),
                )
            };
            if has_match == 0 {
                break;
            }
            let matched = if len == 0 {
                // The pointer may be dangling for a zero-length key.
                &[][..]
            } else {
                // SAFETY: a match writes `runes`/`len` as the iterator's buffer
                // and the number of valid runes in it.
                unsafe { std::slice::from_raw_parts(runes, len as usize) }
            };
            if callback(matched, num_docs).is_break() {
                break;
            }
        }

        drop(it);
        Ok(FuzzyWalk::Walked)
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
    /// When supplied, `timeout` aborts the walk when the active request timeout
    /// expires; otherwise the walk runs to completion with no timeout.
    ///
    /// [`Break`]: ControlFlow::Break
    ///
    pub fn iterate_wildcard<F>(
        &self,
        pattern: &LoweredPattern,
        timeout: Option<&QueryRequestTimeoutHandle>,
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
        let timeout = timeout.map_or(ptr::null_mut(), QueryRequestTimeoutHandle::as_mut_ptr);
        // SAFETY: `self` borrows a valid `ffi::Trie`; `pattern` addresses its
        // content runes followed by the readable zero sentinel the matcher
        // requires (`LoweredPattern` invariant); `&mut callback` stays alive for
        // the whole call, so the `ctx` the trampoline reconstitutes is valid; and
        // `timeout` is null or points to valid request-owned timeout state.
        unsafe {
            ffi::Trie_IterateWildcard(
                self.as_ptr(),
                pattern.runes.as_ptr(),
                pattern.len() as c_int,
                Some(range_trampoline::<F>),
                std::ptr::from_mut(&mut callback).cast(),
                timeout,
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
    pub fn delete(&mut self, term: &TrieTerm) -> bool {
        // SAFETY: `self` borrows a valid `ffi::Trie`; `TrieTerm` guarantees that
        // the C decoder can consume `term` without reading beyond the slice or
        // treating an interior zero codepoint as its end.
        let removed = unsafe {
            ffi::Trie_Delete(
                self.as_mut_ptr(),
                term.as_ptr() as *const c_char,
                term.len(),
            )
        };

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
        // SAFETY: `trie` borrows a valid `ffi::Trie`. `Trie_IterateAll` takes a
        // non-const `Trie *` but only reads it to seed the iterator's own stack,
        // so nothing is written through the shared pointer.
        let ptr = unsafe { ffi::Trie_IterateAll(trie.as_ptr().cast_mut()) };
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
    type Item = TrieTerm;

    fn next(&mut self) -> Option<Self::Item> {
        let runes = self.advance()?;
        // terms trie insertion caps a trie key at `TRIE_INITIAL_STRING_LEN`
        // runes, well inside the `MAX_RUNE_STR_LEN` which `runes_to_bytes`
        // requires, so every key this iterator yields converts.
        let bytes = runes_to_bytes(runes).expect("a stored trie key fits within MAX_RUNE_STR_LEN");
        // SAFETY: the iterator only yields non-empty keys accepted by the terms
        // trie. `runes_to_bytes` emits a complete encoding of every rune, with
        // no zero rune, and the key is shorter than `TRIE_INITIAL_STRING_LEN`.
        Some(unsafe { TrieTerm::from_bytes_unchecked(bytes.into_boxed_slice()) })
    }
}

impl<'a> IntoIterator for &'a TermsTrie {
    type Item = TrieTerm;
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
