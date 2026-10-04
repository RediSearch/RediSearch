/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! RDB serialization for [`StrTrieMap`].
//!
//! Wraps the byte-keyed [`crate::trie_map`] surface for callers whose keys are
//! UTF-8 by type; the wire output is byte-identical. Each loaded key buffer
//! is validated with [`String::from_utf8`], so non-UTF-8 input surfaces as
//! [`RdbError::InvalidUtf8`] rather than silently materializing as an
//! ill-formed `String`.
//!
//! That makes this wrapper fit only for tries whose keys are UTF-8 by
//! construction — the lexicographic dictionaries. C's trie keys are `rune`
//! arrays that libnu produces from arbitrary bytes without ever validating
//! them, so a trie fed from user input directly (`FT.SUGADD`'s) can hold keys
//! that re-encode to byte sequences no UTF-8 decoder accepts. Serializing one
//! of those needs a rune-keyed flavor alongside [`crate::trie_map`], not this
//! wrapper.

use super::{RdbError, RdbOpts, SaveError, read_entries, trie_map};
use crate::{TrieEntry, WireFields};
use rdb_io::RdbIO;
use trie_rs::str_trie_map::StrTrieMap;

/// Serialize a [`StrTrieMap`] with an arbitrary payload type to `writer`
/// in the trie RDB wire format.
///
/// `fields` produces the wire fields for each entry's payload, as in
/// [`crate::trie_map::save_with`]. Delegates to it on the inner byte-keyed
/// [`trie_rs::TrieMap`], and so enforces the same
/// [key domain](crate#key-domain): a `String` key is UTF-8, but that alone
/// does not put it in range of the C trie.
pub fn save_with<P, IO: RdbIO>(
    map: &StrTrieMap<P>,
    writer: &mut IO,
    opts: RdbOpts,
    fields: impl for<'a> FnMut(&'a P) -> WireFields<'a>,
) -> Result<(), SaveError> {
    trie_map::save_with(map.byte_trie(), writer, opts, fields)
}

/// Serialize a [`StrTrieMap<TrieEntry>`] to `writer` in the trie RDB wire
/// format.
///
/// Shorthand for [`save_with`] with the identity field mapping.
pub fn save<IO: RdbIO>(
    map: &StrTrieMap<TrieEntry>,
    writer: &mut IO,
    opts: RdbOpts,
) -> Result<(), SaveError> {
    trie_map::save(map.byte_trie(), writer, opts)
}

/// Stream the entries of a serialized trie from `reader`, in stream order,
/// into `sink`.
///
/// `opts` must match the [`RdbOpts`] used at save time. Each loaded key
/// buffer is UTF-8 validated; on failure the load aborts with
/// [`RdbError::InvalidUtf8`].
///
/// This is the building block for callers that store the entries in their
/// own structure (possibly transforming keys on the way in) and would
/// otherwise pay for an intermediate [`StrTrieMap`] build.
pub fn load_entries<IO: RdbIO>(
    reader: &mut IO,
    opts: RdbOpts,
    sink: impl FnMut(String, TrieEntry),
) -> Result<(), RdbError> {
    read_entries(
        reader,
        opts,
        |bytes| String::from_utf8(bytes).map_err(|_| RdbError::InvalidUtf8),
        sink,
    )
}

/// Deserialize a [`StrTrieMap`] with an arbitrary payload type from
/// `reader`.
///
/// `opts` must match the [`RdbOpts`] used at save time. `payload` builds
/// each stored payload from the decoded wire fields, as in
/// [`crate::trie_map::load_with`]. Each loaded key buffer is UTF-8 validated;
/// on failure the load aborts with [`RdbError::InvalidUtf8`].
pub fn load_with<P, IO: RdbIO>(
    reader: &mut IO,
    opts: RdbOpts,
    mut payload: impl FnMut(TrieEntry) -> P,
) -> Result<StrTrieMap<P>, RdbError> {
    let mut map = StrTrieMap::new();
    load_entries(reader, opts, |key, entry| {
        map.insert(&key, payload(entry));
    })?;
    Ok(map)
}

/// Deserialize a [`StrTrieMap<TrieEntry>`] from `reader`.
///
/// Shorthand for [`load_with`] with the identity payload mapping.
pub fn load<IO: RdbIO>(reader: &mut IO, opts: RdbOpts) -> Result<StrTrieMap<TrieEntry>, RdbError> {
    load_with(reader, opts, |entry| entry)
}
