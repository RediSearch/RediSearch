/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe wrapper around [`ffi::RSToken`].

use std::{ffi::CStr, ptr::NonNull};

use query_term::RSTokenFlags;
use string_utils::runes::{RuneStrTooLong, utf8_to_lower_runes};

/// Safe, read-only, [`Copy`] handle borrowing a query-node's [`ffi::RSToken`].
///
/// An [`ffi::RSToken`] is a plain-data struct — a `(string, length)` pair plus a
/// packed flags bitfield. This handle borrows one for the lifetime `'a` and
/// exposes the string as a byte slice and the flags as a scalar, keeping the raw
/// pointer handling behind a safe surface. It is a thin, [`Copy`] wrapper around a
/// shared reference, so it is passed by value rather than behind another
/// reference.
///
/// The `NUL_TERMINATED` const parameter is a typestate flag recording whether the
/// token string is NUL-terminated:
///
/// - [`RSTokenRef<'a, true>`] additionally guarantees the string is
///   NUL-terminated, so it exposes a *safe* [`as_c_str`](RSTokenRef::as_c_str).
///   Query-node tokens produced by the parser for prefix, fuzzy, and verbatim
///   nodes are NUL-terminated and use this variant.
/// - [`RSTokenRef<'a, false>`] (the default) makes no such promise: tokens built
///   from a raw `(pointer, length)` slice — e.g. tag values or trie-expansion
///   terms — may not be NUL-terminated, so `as_c_str` is not available on it.
///   Plain term (`QN_TOKEN`) nodes also use this variant, because token
///   expansion can replace their string with a length-delimited one.
#[derive(Clone, Copy)]
pub struct RSTokenRef<'a, const NUL_TERMINATED: bool = false>(&'a ffi::RSToken);

/// An [`RSTokenRef`] whose string is known to be NUL-terminated.
pub type RSTokenRefNulTerminated<'a> = RSTokenRef<'a, true>;

impl<'a, const NUL_TERMINATED: bool> RSTokenRef<'a, NUL_TERMINATED> {
    /// Wrap a borrowed [`ffi::RSToken`] as a [`Copy`] handle carrying its `'a`
    /// borrow. Upholding the `str_`/`len` — and, for the NUL-terminated variant,
    /// NUL-termination — invariants is the job of the public `unsafe`
    /// constructors that wrap it.
    const fn new(tok: &'a ffi::RSToken) -> Self {
        Self(tok)
    }

    /// A pointer to the underlying raw [`ffi::RSToken`], for the few call sites
    /// that must hand the token back to C. The pointer is valid for `'a`.
    pub const fn as_ptr(&self) -> *const ffi::RSToken {
        std::ptr::from_ref(self.0)
    }

    /// The length in bytes of the token string.
    pub const fn len(&self) -> usize {
        self.0.len
    }

    /// Whether the token string is empty.
    pub const fn is_empty(&self) -> bool {
        self.0.len == 0
    }

    /// The token's per-term flags (stemming, phonetic, expansion, …).
    pub fn flags(&self) -> RSTokenFlags {
        self.0.flags()
    }

    /// The token string as a byte slice, or `None` when the token carries no
    /// string (a null `str_` pointer).
    pub fn as_bytes(&self) -> Option<&'a [u8]> {
        let ptr = NonNull::new(self.0.str_)?;
        // SAFETY: the constructors' contract guarantees a non-null `str_`
        // addresses `len` valid, initialized bytes for `'a`. The returned slice
        // borrows `'a`, so it cannot outlive that guarantee.
        Some(unsafe { std::slice::from_raw_parts(ptr.as_ptr().cast::<u8>(), self.0.len) })
    }

    /// Lowercase the token string and convert it to runes, e.g. for a trie
    /// lookup.
    ///
    /// A token is a byte string and need not be valid UTF-8, so the bytes are
    /// decoded by [`utf8_to_lower_runes`] — the same decoding a term is indexed
    /// under — rather than validated. A token carrying malformed bytes therefore
    /// resolves the runes the index stored them as, instead of a key built from
    /// replacement characters that was never stored.
    ///
    /// Returns `None` when the token carries no string (a null `str_` pointer),
    /// and `Some(Err(_))` when the lowercased string exceeds the maximum
    /// rune-string length.
    pub fn as_lower_runes(&self) -> Option<Result<Vec<u16>, RuneStrTooLong>> {
        let bytes = self.as_bytes()?;
        Some(utf8_to_lower_runes(bytes))
    }
}

impl<'a> RSTokenRef<'a, false> {
    /// Wrap a borrowed [`ffi::RSToken`] whose string is *not* guaranteed to be
    /// NUL-terminated (e.g. a token built from a raw `(pointer, length)` slice).
    ///
    /// If the string *is* known to be NUL-terminated, use
    /// [`from_nul_terminated_ffi`](RSTokenRef::from_nul_terminated_ffi) instead.
    ///
    /// # Safety
    ///
    /// The token's `str_`/`len` pair must describe a valid byte range for the
    /// whole of the handle's `'a` lifetime: when `str_` is non-null it must point
    /// to `len` initialized, immutable bytes. (A null `str_` carries no string and
    /// needs no backing bytes.)
    pub const unsafe fn from_ffi(tok: &'a ffi::RSToken) -> Self {
        Self::new(tok)
    }
}

impl<'a> RSTokenRef<'a, true> {
    /// Wrap a borrowed [`ffi::RSToken`] whose string is known to be
    /// NUL-terminated, such as a query-node token produced by the parser. This
    /// unlocks the safe [`as_c_str`](RSTokenRef::as_c_str) accessor.
    ///
    /// # Safety
    ///
    /// In addition to [`from_ffi`](RSTokenRef::from_ffi)'s `str_`/`len`
    /// requirement, a non-null `str_` must be terminated by a NUL byte at
    /// index `len` — i.e. `str_[len]` is readable and equal to `0`, so the
    /// allocation spans at least `len + 1` bytes (a C string of content
    /// length `len`).
    pub const unsafe fn from_nul_terminated_ffi(tok: &'a ffi::RSToken) -> Self {
        // In debug builds, sanity-check the NUL-termination the caller promised.
        #[cfg(debug_assertions)]
        if !tok.str_.is_null() {
            // SAFETY: the caller guarantees `str_` points to a NUL-terminated
            // string of content length `len`, so index `len` (the terminator)
            // is in bounds.
            let terminator = unsafe { tok.str_.add(tok.len) };
            // SAFETY: `terminator` addresses the in-bounds terminator byte,
            // valid to read for the duration of this call.
            let terminator = unsafe { *terminator };
            assert!(terminator == 0, "token string must be NUL-terminated");
        }
        Self::new(tok)
    }

    /// The token string as a NUL-terminated [`CStr`], or `None` when the token
    /// carries no string (a null `str_` pointer).
    ///
    /// This is safe: the NUL-termination requirement is discharged once, at
    /// construction time, by
    /// [`from_nul_terminated_ffi`](RSTokenRef::from_nul_terminated_ffi)'s
    /// `unsafe` contract.
    pub fn as_c_str(&self) -> Option<&'a CStr> {
        let ptr = NonNull::new(self.0.str_)?;
        // SAFETY: this is an `RSTokenRef<true>`, so its constructor guaranteed a
        // non-null `str_` points to a NUL-terminated string that stays valid and
        // unmutated for `'a`, which the returned `CStr` borrows.
        Some(unsafe { CStr::from_ptr(ptr.as_ptr()) })
    }
}
