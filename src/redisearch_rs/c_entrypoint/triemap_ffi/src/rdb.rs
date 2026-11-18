/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! RDB persistence for disk TAG dictionaries whose value slots hold document counts.

use std::ffi::c_void;

use rdb_io::RdbIO;
use trie_rdb::{RdbError, RdbOpts, WireFields, trie_map};

use crate::{TrieMap, tm_len_t};

const COUNT_OPTS: RdbOpts = RdbOpts {
    payloads: false,
    num_docs: true,
};

/// Save the count-valued entries in `map` through [`RdbIO`].
///
/// Every value must encode a positive document count in its pointer address.
/// Uses the existing trie RDB layout with [`RdbOpts::num_docs`]; its mandatory
/// score field is zero and [`RdbOpts::payloads`] is disabled.
pub fn save(map: &TrieMap, io: &mut impl RdbIO) {
    trie_map::save_with(&map.0, io, COUNT_OPTS, |count| WireFields {
        score: 0.0,
        payload: None,
        num_docs: count.addr() as u64,
    });
}

/// Load a count-valued [`TrieMap`] through [`RdbIO`].
///
/// Rejects malformed key framing, keys longer than [`tm_len_t::MAX`], and
/// counts that are zero or do not fit in a pointer address. The score is ignored.
pub fn load(io: &mut impl RdbIO) -> Result<TrieMap, RdbError> {
    trie_map::try_load_with(io, COUNT_OPTS, |key, entry| {
        if key.len() > usize::from(tm_len_t::MAX) {
            return Err(RdbError::InvalidEntry);
        }
        let count = usize::try_from(entry.num_docs).map_err(|_| RdbError::InvalidEntry)?;
        if count == 0 {
            return Err(RdbError::InvalidEntry);
        }
        Ok(std::ptr::without_provenance_mut::<c_void>(count))
    })
    .map(TrieMap)
}
