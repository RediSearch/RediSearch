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
//! replication streams written earlier by C carry whatever heap byte occupied
//! that slot, and they must stay loadable. Only the byte's presence is guaranteed,
//! so [`load_payload`] discards it unread and rejects nothing but the zero-length buffer.
//!
//! # Empty-payload normalization
//!
//! When [`RdbOpts::payloads`] is `true`, both `payload: None` and
//! `payload: Some(vec![])` emit a single-NUL buffer (`"\0"`) and load back as
//! `None`.
//!
//! # IO model
//!
//! Save is infallible at the Rust API level, matching the void-returning C
//! `RedisModule_Save*` primitives. Errors only surface on load through
//! [`RdbError`].

pub mod entry;
pub mod str_trie_map;
#[cfg(feature = "test-utils")]
pub mod test_utils;
pub mod trie_map;

use std::io;

use rdb_io::RdbIO;

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
        let score = reader.read_f64()?;
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
