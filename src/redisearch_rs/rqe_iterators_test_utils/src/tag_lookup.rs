/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A [`TagLookup`] the tests own, standing in for the one the tag index hands
//! its readers in production.
//!
//! `rqe_iterators` cannot use `tag_index::TrieLookup` — `tag_index` depends on
//! this crate's dependent, so the edge would be a cycle. This is the same shape
//! over a bare [`TrieMapOpaque`], which lets a test remove or replace an entry
//! to stand in for the garbage collector.

use std::ptr::NonNull;

use inverted_index::{InvertedIndex, opaque::OpaqueEncoding};
use rqe_iterators::inverted_index::TagLookup;
use trie_rs::TrieMapOpaque;

/// A [`TagLookup`] over a tag values trie the caller owns.
pub struct TrieMapTagLookup(NonNull<TrieMapOpaque>);

impl TrieMapTagLookup {
    /// Create a lookup over the given values trie.
    ///
    /// # Safety
    ///
    /// 1. `trie` must be the pointer the trie's owner holds, and not one derived
    ///    from a reference to it. A test that mutates the trie while an iterator
    ///    holds this lookup takes a `&mut` through the owner's pointer, which
    ///    would revoke anything derived above it — the same discipline
    ///    `tag_index::TrieLookup` documents.
    /// 2. `trie` must stay valid for this lookup and any iterator holding it.
    /// 3. Its entries must point to [`InvertedIndex`]es whose encoding matches
    ///    the `E` this lookup is used with.
    pub const unsafe fn new(trie: NonNull<TrieMapOpaque>) -> Self {
        Self(trie)
    }
}

impl<E> TagLookup<E> for TrieMapTagLookup
where
    E: OpaqueEncoding<Storage = InvertedIndex<E>>,
{
    fn find(&self, tag: &[u8]) -> Option<&InvertedIndex<E>> {
        // SAFETY: contracts 1 and 2 — the pointer carries the owner's provenance
        // and the trie outlives any iterator holding this lookup.
        let trie = unsafe { self.0.as_ref() };
        let idx = trie.find(tag)?;
        let opaque = idx.cast::<inverted_index::opaque::InvertedIndex>().as_ptr();
        // SAFETY: contract 3 — `from_opaque` panics when the encoding does not
        // match `E`.
        Some(E::from_opaque(unsafe { &*opaque }))
    }
}
