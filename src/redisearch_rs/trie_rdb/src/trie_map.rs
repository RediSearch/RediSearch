/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! RDB serialization for the byte-keyed [`TrieMap`].
//!
//! This is the canonical serializer; the UTF-8-keyed [`crate::str_trie_map`]
//! wrapper delegates to it. The wire format and framing rules are documented
//! on the crate root.

use super::{RdbError, RdbOpts, read_entries, save_nul_terminated};
use crate::{TrieEntry, WireFields};
use lending_iterator::LendingIterator;
use rdb_io::RdbIO;
use trie_rs::TrieMap;

/// Serialize a [`TrieMap`] with an arbitrary payload type to `writer` in
/// the trie RDB wire format.
///
/// `fields` produces the wire fields ([`WireFields`]) for each entry's
/// payload; which of them actually reach the wire is governed by `opts`.
/// Payload types that carry no wire data map to constants (e.g. a `()`
/// payload saving as score 1).
///
/// Iterates entries in lexicographic key order; the NUL framing applied to
/// each field is documented on the crate root.
pub fn save_with<P, IO: RdbIO>(
    map: &TrieMap<P>,
    writer: &mut IO,
    opts: RdbOpts,
    mut fields: impl for<'a> FnMut(&'a P) -> WireFields<'a>,
) {
    writer.write_u64(map.n_unique_keys() as u64);
    let mut scratch = Vec::new();
    let mut entries = map.lending_iter();
    while let Some((key, payload)) = entries.next() {
        let entry = fields(payload);
        save_nul_terminated(writer, &mut scratch, key);
        writer.write_f64(entry.score);
        if opts.payloads {
            save_nul_terminated(writer, &mut scratch, entry.payload.unwrap_or(&[]));
        }
        if opts.num_docs {
            writer.write_u64(entry.num_docs);
        }
    }
}

/// Serialize a [`TrieMap<TrieEntry>`] to `writer` in the trie RDB wire
/// format.
///
/// Shorthand for [`save_with`] with the identity field mapping.
pub fn save<IO: RdbIO>(map: &TrieMap<TrieEntry>, writer: &mut IO, opts: RdbOpts) {
    save_with(map, writer, opts, |entry| entry.into());
}

/// Deserialize a [`TrieMap`] with an arbitrary payload type from `reader`.
///
/// `opts` must match the [`RdbOpts`] used at save time. `payload` builds
/// each stored payload from the decoded wire fields; payload types that
/// carry no wire data simply discard them (e.g. `|_| ()`).
pub fn load_with<P, IO: RdbIO>(
    reader: &mut IO,
    opts: RdbOpts,
    mut payload: impl FnMut(TrieEntry) -> P,
) -> Result<TrieMap<P>, RdbError> {
    let mut map = TrieMap::new();
    read_entries(reader, opts, Ok, |key, entry| {
        map.insert(&key, payload(entry));
    })?;
    Ok(map)
}

/// Deserialize a [`TrieMap<TrieEntry>`] from `reader`.
///
/// Shorthand for [`load_with`] with the identity payload mapping.
pub fn load<IO: RdbIO>(reader: &mut IO, opts: RdbOpts) -> Result<TrieMap<TrieEntry>, RdbError> {
    load_with(reader, opts, |entry| entry)
}
