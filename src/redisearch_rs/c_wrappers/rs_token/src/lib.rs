/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Safe wrapper around [`ffi::RSToken`].

use std::{
    ffi::{CStr, c_char},
    ptr::NonNull,
};

use query_term::RSTokenFlags;

/// The most bytes the C `strToLowerRunes` decoder (`nu_utf8_read`) reads past a
/// multibyte lead byte. A UTF-8 sequence is at most four bytes, so the decoder
/// touches at most three bytes beyond the lead — and it does so without any
/// bounds check. Padding a decoder input with this many trailing zero bytes
/// keeps a truncated trailing lead byte from reading out of bounds.
const NU_MAX_READAHEAD: usize = 3;

/// Upper bound on how many token bytes are copied for a rune conversion. A term
/// is stored under at most [`ffi::MAX_RUNE_STR_LEN`] runes, and every UTF-8
/// codepoint — at most four bytes — folds to at least one rune, so any input
/// longer than this decodes to more runes than `MAX_RUNE_STR_LEN`. Such a token
/// is rejected by the C `strToLowerRunes` and can match no stored term either
/// way, so capping the copy here keeps a huge token from forcing an equally huge
/// transient allocation before that rejection.
const MAX_DECODE_BYTES: usize = 4 * (ffi::MAX_RUNE_STR_LEN as usize + 1);

/// How many bytes the C decoder (`nu_utf8_read`) consumes for the lead byte `b`,
/// mirroring its branches exactly — including that it treats a stray
/// continuation byte (`0x80..=0xBF`) as a two-byte lead rather than rejecting it.
const fn nu_seq_len(b: u8) -> usize {
    match b {
        0x00..=0x7F => 1,
        0x80..=0xDF => 2,
        0xE0..=0xEF => 3,
        0xF0..=0xFF => 4,
    }
}

/// Whether handing `bytes` to the C decoder would read past its end.
///
/// The decoder walks the input one sequence at a time, reading [`nu_seq_len`]
/// bytes from each position it lands on without checking that many remain. This
/// replays that same walk — the lengths alone decide where it lands, so no
/// decoding is needed — and reports whether any step would run off the end. It
/// is exact rather than conservative: a well-formed input, whatever its last
/// character, never trips it, and only a genuinely truncated trailing sequence
/// does.
///
/// `false` means no read can escape `bytes`, so it may be decoded in place; the
/// padded copy is only needed when this returns `true`.
fn tail_may_overread(bytes: &[u8]) -> bool {
    let mut pos = 0;
    while pos < bytes.len() {
        let seq_len = nu_seq_len(bytes[pos]);
        if pos + seq_len > bytes.len() {
            return true;
        }
        pos += seq_len;
    }
    false
}

/// Safe, read-only, [`Copy`] handle borrowing a query-node's [`ffi::RSToken`].
///
/// An [`ffi::RSToken`] is a plain-data struct — a `(string, length)` pair plus a
/// packed flags bitfield. This handle borrows one for the lifetime `'a` and
/// exposes the string as a byte slice and the flags as a scalar, keeping the raw
/// pointer handling behind a safe surface. It is a thin, [`Copy`] wrapper, so it
/// is passed by value rather than behind another reference.
///
/// # Why the accessors are safe
///
/// A query node's token is **not immutable**: evaluation rewrites it in place
/// (escape removal, case folding) and can even `free` its backing buffer and
/// install a new one. What keeps a view of the string from dangling is the
/// lifetime `'a`: a handle is only ever minted from a shared borrow of the
/// owning node, and every mutation path — including handing the node to the C
/// evaluator — requires an exclusive borrow of it. So while a handle, or
/// anything derived from it, is live, no mutation of the token can be
/// expressed. Constructing a handle whose `'a` does *not* come from such a
/// borrow is what the `unsafe` constructors guard.
///
/// `'a` being a no-mutation window is also what lets the handle simply hold a
/// `&'a `[`ffi::RSToken`]: the freeze a shared reference asserts is exactly the
/// window's guarantee, so there is nothing left for a raw pointer to buy.
///
/// The `NUL_TERMINATED` const parameter is a typestate flag recording whether the
/// token string is NUL-terminated:
///
/// - [`RSTokenRef<'a, true>`] additionally guarantees the string is
///   NUL-terminated, which is what unlocks
///   [`as_c_str`](RSTokenRef::as_c_str). Query-node tokens produced by the parser
///   for prefix, fuzzy, and verbatim nodes are NUL-terminated and use this
///   variant.
/// - [`RSTokenRef<'a, false>`] (the default) makes no such promise: tokens built
///   from a raw `(pointer, length)` slice — e.g. tag values or trie-expansion
///   terms — may not be NUL-terminated, so `as_c_str` is not available on it.
///   Plain term (`QN_TOKEN`) nodes also use this variant, because token
///   expansion can replace their string with a length-delimited one.
#[derive(Clone, Copy)]
pub struct RSTokenRef<'a, const NUL_TERMINATED: bool = false> {
    tok: &'a ffi::RSToken,
}

/// An [`RSTokenRef`] whose string is known to be NUL-terminated.
pub type RSTokenRefNulTerminated<'a> = RSTokenRef<'a, true>;

impl<'a, const NUL_TERMINATED: bool> RSTokenRef<'a, NUL_TERMINATED> {
    /// A pointer to the underlying [`ffi::RSToken`], for the few call sites that
    /// must hand the token back to C. The pointer is valid for `'a`, and — like
    /// everything else reachable through this handle — C may only read through
    /// it: `'a` is a no-mutation window.
    pub const fn as_ptr(&self) -> *const ffi::RSToken {
        std::ptr::from_ref(self.tok)
    }

    /// The length in bytes of the token string.
    pub const fn len(&self) -> usize {
        self.tok.len
    }

    /// Whether the token string is empty.
    pub const fn is_empty(&self) -> bool {
        self.tok.len == 0
    }

    /// The token's per-term flags (stemming, phonetic, expansion, …).
    pub fn flags(&self) -> RSTokenFlags {
        self.tok.flags()
    }

    /// The token string as a byte slice, or `None` when the token carries no
    /// string (a null `str_` pointer).
    ///
    /// The slice borrows for `'a`, not for `&self`: the handle is [`Copy`], so
    /// tying the slice to a particular copy of it would be arbitrary. `'a` is the
    /// borrow of the owning node, which is exactly as long as the string is
    /// guaranteed to stay put — see [the type's docs](RSTokenRef).
    pub fn as_bytes(&self) -> Option<&'a [u8]> {
        let ptr = NonNull::new(self.tok.str_)?;
        // SAFETY: the constructors' contract guarantees a non-null `str_`
        // addresses `len` initialized bytes, and that they stay readable and
        // unmutated for `'a` — no mutation of the token can be expressed while a
        // borrow of the owning node is outstanding.
        Some(unsafe { std::slice::from_raw_parts(ptr.as_ptr().cast::<u8>(), self.tok.len) })
    }

    /// Lowercase the token string and convert it to runes, e.g. for a trie
    /// lookup.
    ///
    /// A token is a byte string and need not be valid UTF-8, so the bytes are
    /// decoded the same way a term is indexed — each codepoint folded to
    /// lowercase before being truncated to a rune — rather than validated. A
    /// token carrying malformed bytes therefore resolves the runes the index
    /// stored them as, instead of a key built from replacement characters that
    /// was never stored.
    ///
    /// Content after a first interior NUL byte is ignored, matching the
    /// up-to-NUL rune sequence the indexer stores for such a term.
    ///
    /// Returns `None` when the token carries no string (a null `str_` pointer) or
    /// when the lowercased string exceeds the maximum rune-string length, in
    /// which case it can name no stored term.
    pub fn as_lower_runes(&self) -> Option<Vec<u16>> {
        let bytes = self.as_bytes()?;

        // `strToLowerRunes` stops at the first NUL its decoder yields — a literal
        // `0` byte or an overlong encoding of one — and reports the runes before
        // it, matching the indexer, so we needn't strip interior NULs ourselves.
        //
        // But its decoder (`nu_utf8_read`) reads a fixed 2–4 bytes from a
        // multibyte lead byte with no bounds check, so a token ending in a
        // truncated lead (e.g. a lone `0xF0`) would read past the end. Where that
        // can happen, decode a private copy instead: one capped at
        // `MAX_DECODE_BYTES`, since a longer token decodes to more runes than any
        // stored term can have and is rejected regardless, so the cap only avoids
        // a needless huge transient copy; and padded with `NU_MAX_READAHEAD`
        // trailing zero bytes so the decoder can never read past the content.
        //
        // The common case needs neither: a token short enough to escape the cap
        // and not ending in a truncated sequence — which is every well-formed
        // one — is handed to C in place, with no copy at all.
        if bytes.len() <= MAX_DECODE_BYTES && !tail_may_overread(bytes) {
            // SAFETY: `tail_may_overread` ruled out every read past `bytes`, so
            // the decoder stays within it (see that function's contract).
            return unsafe { Self::to_lower_runes(bytes.as_ptr(), bytes.len()) };
        }

        let decode_len = bytes.len().min(MAX_DECODE_BYTES);
        let mut buf = Vec::with_capacity(decode_len + NU_MAX_READAHEAD);
        buf.extend_from_slice(&bytes[..decode_len]);
        buf.resize(decode_len + NU_MAX_READAHEAD, 0);

        // SAFETY: `buf` holds `decode_len` content bytes followed by
        // `NU_MAX_READAHEAD` zero bytes; since `nu_utf8_read` reads at most
        // `NU_MAX_READAHEAD` bytes past any lead byte it decodes, every read stays
        // within `buf`.
        unsafe { Self::to_lower_runes(buf.as_ptr(), decode_len) }
    }

    /// Lowercase `len` bytes at `ptr` to runes via the C converter, returning
    /// `None` when it declines the conversion because the result would exceed the
    /// maximum rune-string length.
    ///
    /// # Safety
    ///
    /// `ptr` must address `len` initialized bytes, *and* the C decoder must not
    /// read past them: `nu_utf8_read` consumes a fixed 1–4 bytes from whatever
    /// lead byte it lands on with no bounds check, so the caller must either pad
    /// the allocation with [`NU_MAX_READAHEAD`] trailing bytes or establish that
    /// no such over-read can occur (see [`tail_may_overread`]).
    unsafe fn to_lower_runes(ptr: *const u8, len: usize) -> Option<Vec<u16>> {
        let mut nrunes: usize = 0;
        // SAFETY: the caller guarantees the decoder's reads stay in bounds. The
        // call returns a freshly allocated buffer of `nrunes` runes, or NULL when
        // the folded length exceeds the maximum.
        let ptr = unsafe { ffi::strToLowerRunes(ptr.cast::<c_char>(), len, &mut nrunes) };
        let ptr = NonNull::new(ptr)?;
        // SAFETY: `ptr` points to `nrunes` valid runes written by the call above.
        let runes = unsafe { std::slice::from_raw_parts(ptr.as_ptr(), nrunes) }.to_vec();
        // SAFETY: `RedisModule_Free` is set during module init and not mutated
        // afterwards.
        let rm_free = unsafe { ffi::RedisModule_Free.expect("Redis allocator not available") };
        // SAFETY: `ptr` was allocated by the module allocator inside
        // `strToLowerRunes`.
        unsafe { rm_free(ptr.as_ptr().cast::<std::ffi::c_void>()) };
        Some(runes)
    }
}

impl<'a> RSTokenRef<'a, false> {
    /// Wrap a raw pointer to an [`ffi::RSToken`] whose string is *not* guaranteed
    /// to be NUL-terminated (e.g. a token built from a raw `(pointer, length)`
    /// slice).
    ///
    /// If the string *is* known to be NUL-terminated, use
    /// [`from_nul_terminated_ffi`](RSTokenRef::from_nul_terminated_ffi) instead.
    ///
    /// # Safety
    ///
    /// - `tok` must be non-null and point to a valid [`ffi::RSToken`] that stays
    ///   allocated and readable for the whole of the handle's `'a` lifetime; when
    ///   its `str_` is non-null it must address `len` initialized bytes.
    /// - Neither the token nor the bytes its `str_` addresses may be mutated or
    ///   freed for `'a`. This is what makes the accessors safe, so `'a` must be
    ///   chosen to make it true — in practice by deriving it from a borrow that
    ///   every mutation path already conflicts with — for a query-node token,
    ///   the shared borrow of the owning node it was read out of. A `'a` picked
    ///   freely (e.g. inferred as `'static`) would let a view outlive the bytes.
    pub const unsafe fn from_ffi(tok: *const ffi::RSToken) -> Self {
        debug_assert!(!tok.is_null(), "token pointer must not be null");
        // SAFETY: the caller guarantees `tok` is non-null, points to a valid
        // `RSToken`, and that it is neither mutated nor freed for `'a`, which is
        // what the shared reference asserts.
        let tok = unsafe { &*tok };
        Self { tok }
    }
}

impl<'a> RSTokenRef<'a, true> {
    /// Wrap a raw pointer to an [`ffi::RSToken`] whose string is known to be
    /// NUL-terminated, such as a query-node token produced by the parser. This
    /// unlocks the safe [`as_c_str`](RSTokenRef::as_c_str) accessor.
    ///
    /// # Safety
    ///
    /// In addition to [`from_ffi`](RSTokenRef::from_ffi)'s requirements, a
    /// non-null `str_` must be terminated by a NUL byte at index `len` — i.e.
    /// `str_[len]` is readable and equal to `0`, so the allocation spans at least
    /// `len + 1` bytes (a C string of content length `len`).
    pub const unsafe fn from_nul_terminated_ffi(tok: *const ffi::RSToken) -> Self {
        debug_assert!(!tok.is_null(), "token pointer must not be null");
        // SAFETY: as for `from_ffi` — the caller guarantees `tok` is non-null and
        // points to a valid `RSToken` that is neither mutated nor freed for `'a`.
        let tok = unsafe { &*tok };
        // In debug builds, sanity-check the NUL-termination the caller promised.
        #[cfg(debug_assertions)]
        if !tok.str_.is_null() {
            // SAFETY: the caller guarantees `str_` is a NUL-terminated string of
            // content length `len`, so `str_.add(len)` is the in-bounds
            // terminator address.
            let terminator_ptr = unsafe { tok.str_.add(tok.len) };
            // SAFETY: `terminator_ptr` addresses the in-bounds terminator byte.
            let terminator = unsafe { *terminator_ptr };
            assert!(terminator == 0, "token string must be NUL-terminated");
        }
        Self { tok }
    }

    /// The token string as a NUL-terminated [`CStr`], or `None` when the token
    /// carries no string (a null `str_` pointer).
    ///
    /// The NUL-termination requirement is discharged once, at construction time,
    /// by [`from_nul_terminated_ffi`](RSTokenRef::from_nul_terminated_ffi)'s
    /// `unsafe` contract, so this accessor exists only on this variant.
    ///
    /// Content after a first interior NUL byte is ignored, as for
    /// [`as_lower_runes`](RSTokenRef::as_lower_runes): the result ends at that
    /// NUL, so it is shorter than [`len`](RSTokenRef::len) and covers less than
    /// [`as_bytes`](RSTokenRef::as_bytes).
    ///
    /// As for [`as_bytes`](RSTokenRef::as_bytes), the result borrows for `'a`.
    pub fn as_c_str(&self) -> Option<&'a CStr> {
        let ptr = NonNull::new(self.tok.str_)?;
        // SAFETY: this is an `RSTokenRef<true>`, so its constructor guaranteed a
        // non-null `str_` points to a NUL-terminated string that stays readable
        // and unmutated for `'a` — no mutation of the token can be expressed
        // while a borrow of the owning node is outstanding.
        Some(unsafe { CStr::from_ptr(ptr.as_ptr()) })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn well_formed_input_needs_no_padding() {
        // Nothing here can over-read, so all of it decodes in place: ASCII, and
        // sequences of every width sitting flush against the end.
        assert!(!tail_may_overread(b""));
        assert!(!tail_may_overread(b"hello"));
        assert!(!tail_may_overread(b"ab\0cd"));
        assert!(!tail_may_overread("é".as_bytes()));
        assert!(!tail_may_overread("日本".as_bytes()));
        assert!(!tail_may_overread("ab😀".as_bytes()));
    }

    #[test]
    fn truncated_trailing_sequence_needs_padding() {
        // A lead byte announcing more bytes than remain: the decoder would read
        // past the end, so these must take the padded-copy path.
        assert!(tail_may_overread(b"ab\xF0"));
        assert!(tail_may_overread(b"ab\xF0\x9F"));
        assert!(tail_may_overread(b"ab\xF0\x9F\x98"));
        assert!(tail_may_overread(b"ab\xE0"));
        assert!(tail_may_overread(b"ab\xC3"));
        // A stray continuation byte at the end is read as a two-byte lead.
        assert!(tail_may_overread(b"ab\x80"));
    }

    #[test]
    fn earlier_malformed_bytes_do_not_force_padding() {
        // The damage is mid-string: the walk resynchronises past it and still
        // lands inside the input, so no over-read is possible.
        assert!(!tail_may_overread(b"\xC3(ab"));
    }
}
