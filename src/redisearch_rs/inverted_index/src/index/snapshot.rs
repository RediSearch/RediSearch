/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Borrowed point-in-time view of an inverted index's block storage.
//!
//! [`InvertedIndexSnapshot`] is a thin wrapper around the index's block list — it
//! lets readers and GC walk blocks through a unified, version-stable API while the
//! underlying storage is free to evolve. Today it borrows directly from the index's
//! `blocks` field; a follow-up PR will swap the storage for a multi-region layout
//! and make the snapshot own its data. Consumers of the snapshot API don't have to
//! change when that happens.

use rqe_core::DocId;

use super::core::IndexBlock;

/// A point-in-time view of an inverted index's block storage. Currently borrows
/// directly from the index; the follow-up storage refactor will replace the borrow
/// with owned, multi-region data without changing this public surface.
pub struct InvertedIndexSnapshot<'a> {
    blocks: &'a [IndexBlock],
}

impl<'a> InvertedIndexSnapshot<'a> {
    /// Construct from a borrowed slice of blocks.
    pub(crate) const fn from_slice(blocks: &'a [IndexBlock]) -> Self {
        Self { blocks }
    }

    /// Total number of blocks visible in the snapshot.
    pub const fn block_count(&self) -> usize {
        self.blocks.len()
    }

    /// Borrow the block at logical index `idx`, or [`None`] if out of range.
    /// The returned reference inherits the snapshot's underlying lifetime, so it
    /// stays valid as long as the snapshot's source data does.
    pub fn block_ref(&self, idx: usize) -> Option<&'a IndexBlock> {
        self.blocks.get(idx)
    }

    /// First block in the snapshot.
    pub const fn first_block(&self) -> Option<&'a IndexBlock> {
        self.blocks.first()
    }

    /// Last block in the snapshot.
    pub const fn last_block(&self) -> Option<&'a IndexBlock> {
        self.blocks.last()
    }

    /// Find the logical index of the first block whose `last_doc_id >= target`,
    /// searching from logical index `start` onward. Returns [`Self::block_count`]
    /// if no such block exists.
    pub fn find_block_for_doc_id(&self, start: usize, target: DocId) -> usize {
        if start >= self.blocks.len() {
            return self.blocks.len();
        }
        let rel = self.blocks[start..]
            .binary_search_by_key(&target, |b| b.last_doc_id)
            .unwrap_or_else(|insertion_point| insertion_point);
        let abs = start + rel;
        if abs >= self.blocks.len() {
            self.blocks.len()
        } else {
            abs
        }
    }
}
