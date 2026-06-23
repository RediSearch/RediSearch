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
