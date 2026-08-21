/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The value type stored in a trie map.

/// Borrowed view of the wire fields of one entry, handed to the generic
/// save path.
///
/// The payload-generic serializers ([`crate::trie_map::save_with`],
/// [`crate::str_trie_map::save_with`]) ask the caller to produce one of these per
/// entry, so any payload type can be persisted without first materializing
/// a [`TrieEntry`] (and cloning its payload bytes). Which of these fields
/// actually reach the wire is governed by [`crate::RdbOpts`], same as for
/// [`TrieEntry`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct WireFields<'a> {
    /// See [`TrieEntry::score`].
    pub score: f64,
    /// See [`TrieEntry::payload`].
    pub payload: Option<&'a [u8]>,
    /// See [`TrieEntry::num_docs`].
    pub num_docs: u64,
}

impl<'a> From<&'a TrieEntry> for WireFields<'a> {
    /// Project an owned [`TrieEntry`] into its borrowed wire fields — the
    /// identity field mapping the save shorthands pass to the payload-generic
    /// serializers for maps that store [`TrieEntry`] itself.
    fn from(entry: &'a TrieEntry) -> Self {
        WireFields {
            score: entry.score,
            payload: entry.payload.as_deref(),
            num_docs: entry.num_docs,
        }
    }
}

/// One trie entry: score, optional opaque payload, and a per-entry counter.
///
/// This is the payload a [`trie_rs::TrieMap`] / [`trie_rs::str_trie_map::StrTrieMap`]
/// holds for each key. Its persistence behavior is governed by
/// [`crate::RdbOpts`]; the type itself carries no IO concern.
///
/// Empty and absent payloads are wire-indistinguishable; see
/// [empty-payload normalization](crate#empty-payload-normalization).
#[derive(Clone, Debug, PartialEq)]
pub struct TrieEntry {
    /// Score associated with the entry. Semantics are caller-defined (e.g.
    /// suggestion weight, or a constant for index-term tries) and may be
    /// mutated by callers after the initial insert. The C trie stores this
    /// as `float`; the RDB wire format widens it to `f64`, and the
    /// serializers collapse it back — see
    /// [score domain](crate#score-domain).
    pub score: f64,
    /// Optional opaque payload bytes.
    pub payload: Option<Vec<u8>>,
    /// Per-entry counter, persisted only when [`crate::RdbOpts::num_docs`]
    /// is set. Semantics are caller-defined (e.g. document frequency for an
    /// index's term trie); this type does not enforce a meaning. Loads with
    /// [`crate::RdbOpts::num_docs`] unset materialize this as `0`.
    pub num_docs: u64,
}
