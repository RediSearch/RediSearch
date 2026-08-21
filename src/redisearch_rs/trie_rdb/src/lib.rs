/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! RDB serialization for the [`trie_rs`] trie maps.
//!
//! Mirrors the wire format produced by the C functions `TrieType_GenericSave`
//! and `TrieType_GenericLoad`. This crate owns the shared substrate —
//! [`RdbOpts`], [`RdbError`], the
//! NUL-framing helpers, and the [`read_entries`] entry-stream reader — plus
//! the [`TrieEntry`] value type the wire fields are modeled on. The two
//! serializer flavors live alongside it:
//!
//! - [`trie_map`] — for the byte-keyed [`trie_rs::TrieMap`].
//! - [`str_trie_map`] — for the UTF-8-keyed [`trie_rs::str_trie_map::StrTrieMap`],
//!   a thin wrapper that delegates to [`trie_map`].
//!
//! Each flavor is generic over the map's payload type ([`trie_map::save_with`] /
//! [`trie_map::load_with`] and their [`str_trie_map`] counterparts, with a per-entry
//! mapping to and from the wire fields) and offers [`trie_map::save`] /
//! [`trie_map::load`] shorthands for maps that store [`TrieEntry`] itself.
//!
//! IO is abstracted behind the [`RdbIO`] trait, so this crate makes no direct
//! use of the Redis module API: [`rdb_io`] implements the trait over
//! `RedisModuleIO`, and pure-Rust callers can implement it over any buffer.
//!
//! # Wire format
//!
//! ```text
//! u64  count                            // map.n_unique_keys()
//! [ bytes(key + '\0')
//!   f64  score
//!   bytes(payload + '\0')               // only if RdbOpts::payloads
//!   u64  num_docs                       // only if RdbOpts::num_docs
//! ] * count
//! ```
//!
//! The diagram lists the framed primitives passed to [`RdbIO`]; the actual
//! on-wire bytes include length prefixes added by `RedisModule_Save*`, which
//! are opaque to this layer.
//!
//! # Trailing-NUL framing
//!
//! Both keys and payloads are written with a trailing NUL byte, so a saved
//! buffer is one byte longer than the value it carries — matching C's
//! `SaveStringBuffer(..., len + 1)` — and the loader strips that byte. Framing
//! is applied in the algorithm body via [`save_nul_terminated`],
//! [`load_nul_terminated`] and [`load_payload`]; the [`RdbIO`] trait surface
//! stays neutral — it just writes and reads raw length-prefixed buffers.
//!
//! The two field kinds differ in how much of that framing the loader trusts.
//! A key's terminator is verified to be NUL. A payload's is not: RDB files and
//! replication streams written before `triePayload_New` assigned the
//! terminator carry whatever heap byte occupied that slot, and they must stay
//! loadable. Only the byte's presence is guaranteed, so [`load_payload`]
//! discards it unread and rejects nothing but the zero-length buffer.
//!
//! # Empty-payload normalization
//!
//! When [`RdbOpts::payloads`] is `true`, both `payload: None` and
//! `payload: Some(vec![])` emit a single-NUL buffer (`"\0"`) and load back as
//! `None`. This mirrors the C-side collapse `payload.len ? &payload : NULL`.
//!
//! # Key domain
//!
//! C's `Trie_InsertStringBuffer` — the insertion path `TrieType_GenericLoad`
//! feeds every loaded entry through — accepts only a narrow set of keys, and
//! silently ignores the rest: a key must be non-empty, at most
//! [`MAX_KEY_BYTES`] bytes long, and decode to at most [`MAX_KEY_RUNES`]
//! runes. C's decoder additionally stops at the first *zero codepoint* it
//! decodes — produced by a literal NUL byte, but also by byte patterns no NUL
//! scan finds, such as the overlong sequence `C0 80` or the continuation pair
//! `80 80` — so a key carrying one would enter the C trie truncated to its
//! prefix, or not at all. The entry count is written ahead of the entries and
//! is not revised for any of this, so a stream holding such a key loads into
//! C as a trie that has fewer entries than its own header claims. A key
//! ending in a truncated multibyte sequence is worse still: C's decoder
//! strides a fixed 1–4 bytes per step with no bounds check, so completing the
//! final sequence would read past the end of the loaded buffer.
//!
//! The savers therefore reject those keys instead of emitting a stream C
//! would mis-load. Every key is checked before the first byte is written, so
//! a save that returns [`SaveError`] has left `writer` untouched and the
//! caller is free to write something else in its place.
//!
//! Passing the check makes a key loadable by C, not byte-stable through it:
//! C holds keys as `u16` runes, so a key that is not valid UTF-8, or that
//! carries codepoints beyond the Basic Multilingual Plane, re-emerges from a
//! pass through C with different bytes — and two such keys can even collapse
//! into one when their decodes coincide. Byte identity through C is
//! guaranteed only for valid UTF-8 keys confined to that plane, which every
//! [`str_trie_map`]-flavor key without astral codepoints is.
//!
//! Loading applies no such check, deliberately: a stream written by C cannot
//! contain an out-of-domain key, and enforcing the domain on load would only
//! turn streams C itself accepts into load failures.
//!
//! # Score domain
//!
//! The C trie stores each score as a `float`: its saver widens that `float`
//! to the wire's `f64`, and its loader narrows the wire value back on
//! insert, so no score survives a pass through C with more than `f32`
//! precision. The serializers here collapse every score to that domain on
//! both save and load, so a map round-trips to the same scores whichever
//! implementation writes or reads the stream.
//!
//! # IO model
//!
//! The only way a save can fail is the key domain above — the write
//! primitives are the void-returning C `RedisModule_Save*` and report nothing
//! back. IO errors therefore surface only on load, through [`RdbError`].
//! Payloads in particular are not validated: they are opaque bytes at this
//! layer, but C's loader aborts the whole load on a payload whose size (plus
//! terminator) does not fit its `u32` length field, so a payload approaching
//! 4 GiB produces a stream C refuses to load.

pub mod entry;
pub mod str_trie_map;
#[cfg(feature = "test-utils")]
pub mod test_utils;
pub mod trie_map;

use std::io;

use rdb_io::RdbIO;
use string_utils::libnu::{nu_decodes_a_zero_codepoint, nu_rune_count, tail_may_overread};

pub use entry::{TrieEntry, WireFields};

/// Read the entry stream shared by both key flavors and feed each decoded
/// entry to `insert`.
///
/// Owns the count framing and the per-entry field layout so the byte- and
/// str-keyed loaders cannot drift apart. The two flavors differ only in how
/// raw key bytes become a key: `key_from_bytes` maps the NUL-stripped buffer
/// into the caller's key type (identity for bytes, UTF-8 validation for str),
/// and `insert` places the finished `(key, entry)` into the caller's map.
pub(crate) fn read_entries<IO: RdbIO, K>(
    reader: &mut IO,
    opts: RdbOpts,
    mut key_from_bytes: impl FnMut(Vec<u8>) -> Result<K, RdbError>,
    mut insert: impl FnMut(K, TrieEntry),
) -> Result<(), RdbError> {
    let count = reader.read_u64()?;
    for _ in 0..count {
        let key = key_from_bytes(load_nul_terminated(reader)?)?;
        let score = quantize_score(reader.read_f64()?);
        let payload = opts
            .payloads
            .then(|| load_payload(reader))
            .transpose()?
            .filter(|b| !b.is_empty());
        let num_docs = if opts.num_docs { reader.read_u64()? } else { 0 };
        insert(
            key,
            TrieEntry {
                score,
                payload,
                num_docs,
            },
        );
    }
    Ok(())
}

/// Write `b` followed by one trailing NUL byte as a single length-prefixed
/// record, reusing `scratch` as the temporary contiguous buffer.
///
/// `scratch` is borrowed from the caller so one allocation can amortize
/// across an entire save loop.
pub(crate) fn save_nul_terminated<IO: RdbIO>(writer: &mut IO, scratch: &mut Vec<u8>, b: &[u8]) {
    scratch.clear();
    scratch.reserve(b.len() + 1);
    scratch.extend_from_slice(b);
    scratch.push(0);
    writer.write_buffer(scratch);
}

/// Read one length-prefixed key buffer and return its contents with the
/// trailing NUL stripped. Returns [`RdbError::MissingTrailingNul`] when the
/// wire buffer is empty or does not end in `0x00`.
pub(crate) fn load_nul_terminated<IO: RdbIO>(reader: &mut IO) -> Result<Vec<u8>, RdbError> {
    let mut buf = reader.read_buffer()?;
    if buf.pop() != Some(0) {
        return Err(RdbError::MissingTrailingNul);
    }
    Ok(buf)
}

/// Read one length-prefixed payload buffer and return its contents with the
/// trailing byte dropped, whatever that byte is — see
/// [trailing-NUL framing](crate#trailing-nul-framing) for why a payload's
/// terminator value is not checked. Returns
/// [`RdbError::MissingTrailingNul`] only when the wire buffer is empty, and
/// so has no terminator slot at all.
pub(crate) fn load_payload<IO: RdbIO>(reader: &mut IO) -> Result<Vec<u8>, RdbError> {
    let mut buf = reader.read_buffer()?;
    if buf.pop().is_none() {
        return Err(RdbError::MissingTrailingNul);
    }
    Ok(buf)
}

/// The fixed capacity the C trie sizes its key limits against.
const TRIE_INITIAL_STRING_LEN: usize = ffi::TRIE_INITIAL_STRING_LEN as usize;

/// Longest key, in bytes, that C's `Trie_InsertStringBuffer` accepts.
///
/// It rejects anything longer outright, before decoding — see
/// [key domain](crate#key-domain).
pub const MAX_KEY_BYTES: usize = TRIE_INITIAL_STRING_LEN * size_of::<ffi::rune>();

/// Most runes a key may decode to for C's `Trie_InsertRune` to accept it.
///
/// One below [`TRIE_INITIAL_STRING_LEN`], since C compares with a strict
/// `<` to leave room for the terminator its rune buffers carry.
#[expect(rustdoc::private_intra_doc_links)]
pub const MAX_KEY_RUNES: usize = TRIE_INITIAL_STRING_LEN - 1;

/// Reasons a key makes a map unserializable — see
/// [key domain](crate#key-domain) for why each one is fatal.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SaveError {
    /// The map holds the empty key.
    #[error("the empty key cannot be stored by the C trie")]
    EmptyKey,
    /// A key exceeds [`MAX_KEY_BYTES`].
    #[error("key of {bytes} bytes exceeds the C trie's {MAX_KEY_BYTES}-byte limit")]
    KeyTooLong {
        /// Length of the offending key, in bytes.
        bytes: usize,
    },
    /// A key decodes to more than [`MAX_KEY_RUNES`] runes.
    #[error("key of {runes} runes exceeds the C trie's {MAX_KEY_RUNES}-rune limit")]
    TooManyRunes {
        /// Rune count of the offending key, per [`nu_rune_count`].
        runes: usize,
    },
    /// A key decodes to a zero codepoint before its end, where C's decoder
    /// would stop — truncating the key, or dropping it entirely.
    #[error("key decodes to a zero codepoint, at which the C trie would cut it short")]
    DecodesToNul,
    /// A key ends in a truncated multibyte sequence, which C's decoder would
    /// read past the end of the loaded buffer to complete.
    #[error("key ends in a truncated multibyte sequence, which the C trie would over-read")]
    TruncatedSequence,
}

/// Collapse `score` to the value it would hold after a pass through the C
/// trie's `float` storage — see [score domain](crate#score-domain).
pub(crate) const fn quantize_score(score: f64) -> f64 {
    score as f32 as f64
}

/// Reject a key the C trie could not store faithfully.
pub(crate) fn validate_key(key: &[u8]) -> Result<(), SaveError> {
    if key.is_empty() {
        return Err(SaveError::EmptyKey);
    }
    if key.len() > MAX_KEY_BYTES {
        return Err(SaveError::KeyTooLong { bytes: key.len() });
    }
    if tail_may_overread(key) {
        return Err(SaveError::TruncatedSequence);
    }
    if nu_decodes_a_zero_codepoint(key) {
        return Err(SaveError::DecodesToNul);
    }
    // With both walks above clean, the rune count replays C's decode without
    // hitting either condition it cannot model, so it cannot over-count.
    let runes = nu_rune_count(key);
    if runes > MAX_KEY_RUNES {
        return Err(SaveError::TooManyRunes { runes });
    }
    Ok(())
}

/// Controls which optional fields are present on the wire.
///
/// The same value must be used at save and load time. Mismatches misalign
/// the wire layout, so subsequent reads either fail with an [`RdbError`] or
/// silently parse the wrong bytes as the next field.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RdbOpts {
    /// Persist each entry's payload (with trailing NUL).
    pub payloads: bool,
    /// Persist each entry's `num_docs`.
    pub num_docs: bool,
}

/// Errors that can occur while reading a trie RDB payload.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RdbError {
    /// The underlying RDB read failed (EOF, corrupted stream, etc.).
    ///
    /// The originating [`std::io::Error`] is intentionally not retained:
    /// dropping it keeps [`RdbError`] `Clone + PartialEq + Eq`, which the
    /// wire-shape tests rely on to assert exact error values.
    #[error("rdb io error")]
    Io,
    /// A framed buffer had no terminator byte at all, or — for keys, whose
    /// terminator value is checked — ended in something other than `0x00`.
    #[error("rdb bytes buffer missing trailing NUL")]
    MissingTrailingNul,
    /// A key buffer was not valid UTF-8 when loaded through the
    /// [`crate::str_trie_map`] wrapper that requires UTF-8 keys.
    #[error("rdb key bytes not valid UTF-8")]
    InvalidUtf8,
}

impl From<io::Error> for RdbError {
    /// Lift an IO failure from the [`RdbIO`] load primitives into the framing
    /// error type, so `?` threads `io::Result` through the framing helpers.
    fn from(_: io::Error) -> Self {
        RdbError::Io
    }
}
