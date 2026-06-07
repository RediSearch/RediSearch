/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Owned point-in-time view of an inverted index's block storage.
//!
//! [`InvertedIndexSnapshot`] holds an [`Arc`] clone of the index's block list, taken
//! at snapshot time. The reader and GC walk this view independently of subsequent
//! writes — once the snapshot is taken, the writer is free to mutate its own copy
//! (copy-on-write through `Arc::make_mut`) without affecting the snapshot.

use std::sync::Arc;

use rqe_core::DocId;
use thin_vec::ThinVec;

use super::core::IndexBlock;
use crate::BlockCapacity;

/// A point-in-time view of an inverted index's block storage.
///
/// Owns an [`Arc`] clone of the underlying `ThinVec` taken at snapshot time, so the
/// returned blocks stay valid even after the writer mutates the live index. Follow-up
/// PRs split the storage into `sealed`/`pending`/`in_progress` regions; the public
/// API stays the same.
pub struct InvertedIndexSnapshot {
    blocks: Arc<ThinVec<IndexBlock, BlockCapacity>>,
}

impl InvertedIndexSnapshot {
    /// Construct a snapshot from a refcount-cloned blocks Arc.
    pub(crate) const fn new(blocks: Arc<ThinVec<IndexBlock, BlockCapacity>>) -> Self {
        Self { blocks }
    }

    /// Total number of blocks visible in the snapshot.
    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    /// Borrow the block at logical index `idx`, or [`None`] if out of range.
    /// The returned reference borrows from the snapshot.
    pub fn block_ref(&self, idx: usize) -> Option<&IndexBlock> {
        self.blocks.get(idx)
    }

    /// First block in the snapshot.
    pub fn first_block(&self) -> Option<&IndexBlock> {
        self.blocks.first()
    }

    /// Last block in the snapshot.
    pub fn last_block(&self) -> Option<&IndexBlock> {
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
