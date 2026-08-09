/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The value type stored in a trie map.

/// One trie entry: score, optional opaque payload, and a per-entry counter.
///
/// This is the payload a [`trie_rs::TrieMap`] / [`trie_rs::str_trie_map::StrTrieMap`]
/// holds for each key. Its persistence behavior is governed by
/// [`crate::RdbOpts`]; the type itself carries no IO concern.
///
/// `payload: None` and `payload: Some(vec![])` are wire-indistinguishable
/// when payloads are persisted — both round-trip as `None`. See
/// [`crate::RdbOpts::payloads`].
/// Borrowed view of the wire fields of one entry, handed to the generic
/// save path.
///
/// The payload-generic serializers ([`crate::byte::save_with`],
/// [`crate::str::save_with`]) ask the caller to produce one of these per
/// entry, so any payload type can be persisted without first materializing
/// a [`TrieEntry`] (and cloning its payload bytes). Which of these fields
/// actually reach the wire is governed by [`crate::RdbOpts`], same as for
/// [`TrieEntry`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EntryFields<'a> {
    /// See [`TrieEntry::score`].
    pub score: f64,
    /// See [`TrieEntry::payload`]. `None` and `Some(&[])` are
    /// wire-indistinguishable, like the owned form.
    pub payload: Option<&'a [u8]>,
    /// See [`TrieEntry::num_docs`].
    pub num_docs: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TrieEntry {
    /// Score associated with the entry. Semantics are caller-defined (e.g.
    /// suggestion weight, or a constant for index-term tries) and may be
    /// mutated by callers after the initial insert. The C trie stores this
    /// as `float`; the RDB wire format widens it to `f64` (via
    /// `RedisModule_SaveDouble`).
    pub score: f64,
    /// Optional opaque payload bytes.
    pub payload: Option<Vec<u8>>,
    /// Per-entry counter, persisted only when [`crate::RdbOpts::num_docs`]
    /// is set. Semantics are caller-defined (e.g. document frequency for an
    /// index's term trie); this type does not enforce a meaning. Loads with
    /// `num_docs = false` materialize this as `0`.
    pub num_docs: u64,
}
