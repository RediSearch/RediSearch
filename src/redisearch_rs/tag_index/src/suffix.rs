/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`TagSuffixIndex`] is created when `WITHSUFFIXTRIE` option is given.
//!
//! For each tags, [`TagSuffixIndex`] stores all the suffixes as key.
//!
//! # Memory model
//!
//! The values of this index are [`SuffixData`] which stores:
//! - owned term (one occurrence)
//! - borrowed term (N occurrence, one for each suffix)
//!
//! [`OwnedTerm`] holds the owned memory, while [`TermPtr`] points to the owned memory.
//! [`OwnedTerm`] frees the memory on drop, [`TermPtr`] doesn't.
//! Adding and removing items from this trie require order-aware operations.
//!
//! For instance, during the insertion:
//! - insert owned term under tag key
//! - for each suffix:
//!   - insert borrowed term under the suffix key
//!
//! [`OwnedTerm`] uses:
//! - the first two bytes (the size of u16) as header to store the string length
//! - the remaining bytes as the string
//!

use std::ptr::NonNull;

use thin_vec::{AlignedU32, ThinVec};
use trie_rs::TrieMap;

/// Owning handle to a tag term allocation.
///
/// Dropping it frees the allocation.
#[derive(Debug)]
struct OwnedTerm(NonNull<u8>);

/// Weak, thin (8-byte) pointer to a term allocation.
///
/// The pointee is owned by the [`OwnedTerm`] of the member's own trie entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TermPtr(NonNull<u8>);

/// Payload of one trie entry.
#[derive(Debug, Default)]
struct SuffixData {
    /// `Some` iff this entry's key is itself a member: the owning handle of
    /// that member's tag term allocation.
    full_term: Option<OwnedTerm>,
    /// Every member this entry's key is a suffix of.
    refs: ThinVec<TermPtr, AlignedU32>,
}

#[derive(Debug, Default)]
pub struct TagSuffixIndex {
    /// The suffix entries
    entries: TrieMap<SuffixData>,
}
