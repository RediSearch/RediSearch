/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Owned, point-in-time snapshot of an inverted index's block storage.
//!
//! `sealed` and `pending` both live as direct fields on
//! [`InvertedIndex`](super::core::InvertedIndex). Writers and GC mutate them under
//! `&mut self` plus the spec write lock; readers take a snapshot under the spec read
//! lock, which captures both at once:
//!
//! - `sealed`: cheap [`Arc::clone`] (refcount bump only, no data copy).
//! - `pending`: shallow [`Vec::clone`] (clones the pointer slots; the [`Arc<IndexBlock>`]s
//!   they point to are refcount-bumped, not deep-copied). This includes the writable tail
//!   block (the last slot); a later append to it copies-on-write, so the snapshot keeps
//!   observing the frozen tail via its `Arc`.
//!
//! The snapshot then walks blocks lock-free for the rest of the query — writers and GC
//! can proceed without affecting it.
//!
//! See [`InvertedIndexSnapshot`] for the public API.
//!
//! [`InvertedIndex`]: super::core::InvertedIndex
//! [`Arc::clone`]: std::sync::Arc::clone

use std::sync::Arc;

use rqe_core::DocId;
use thin_vec::ThinVec;

use super::core::IndexBlock;
use crate::BlockCapacity;

/// An owned, point-in-time snapshot of an [`InvertedIndex`](super::core::InvertedIndex).
///
/// Combines:
/// - An [`Arc`] clone of the index's `sealed` blocks (data shared via refcount).
/// - A shallow clone of `pending`: the [`ThinVec`] of [`Arc<IndexBlock>`] pointer slots
///   is copied, but the block data behind each `Arc` is shared — including the writable
///   tail block (the last `pending` slot). A writer that later appends to the tail
///   copies-on-write, so this snapshot's `Arc` keeps observing the frozen tail.
///
/// Both regions are captured together while the caller holds the spec read lock, so no
/// concurrent writer/GC can split the snapshot across an inconsistent moment. After the
/// lock is released the snapshot is fully owned and can be walked without coordination.
#[derive(Debug)]
pub struct InvertedIndexSnapshot {
    sealed: Arc<[IndexBlock]>,
    pending: ThinVec<Arc<IndexBlock>, BlockCapacity>,
}

impl InvertedIndexSnapshot {
    /// Construct from the two regions of an [`InvertedIndex`](super::core::InvertedIndex).
    /// Caller is responsible for capturing both under the same spec-read-lock acquisition.
    pub(crate) const fn new(
        sealed: Arc<[IndexBlock]>,
        pending: ThinVec<Arc<IndexBlock>, BlockCapacity>,
    ) -> Self {
        Self { sealed, pending }
    }

    /// The `sealed` region `Arc` captured when this snapshot was taken.
    ///
    /// Its pointer identity is the GC signal: GC is the *only* thing that replaces
    /// [`InvertedIndex::sealed`](super::core::InvertedIndex), so a reader whose captured
    /// `sealed` still points at the index's current `sealed` has not been invalidated by a
    /// compaction. See [`needs_revalidation`](crate::reader::core).
    pub(crate) const fn sealed_arc(&self) -> &Arc<[IndexBlock]> {
        &self.sealed
    }

    /// Total number of blocks visible in the snapshot.
    pub fn block_count(&self) -> usize {
        self.sealed.len() + self.pending.len()
    }

    /// Borrow the block at logical index `idx`. The logical index is flat across
    /// sealed → pending (the writable tail is the last `pending` slot).
    pub fn block_ref(&self, idx: usize) -> Option<&IndexBlock> {
        if let Some(b) = self.sealed.get(idx) {
            return Some(b);
        }
        let idx = idx.checked_sub(self.sealed.len())?;
        self.pending.get(idx).map(|arc| &**arc)
    }

    /// First block in the snapshot.
    pub fn first_block(&self) -> Option<&IndexBlock> {
        self.block_ref(0)
    }

    /// Last block in the snapshot.
    pub fn last_block(&self) -> Option<&IndexBlock> {
        let total = self.block_count();
        if total == 0 {
            None
        } else {
            self.block_ref(total - 1)
        }
    }

    /// Find the logical index of the first block whose `last_doc_id >= target`, searching
    /// from logical index `start` onward. Returns [`Self::block_count`] if no such block
    /// exists.
    pub fn find_block_for_doc_id(&self, start: usize, target: DocId) -> usize {
        let n_sealed = self.sealed.len();
        let n_pending = self.pending.len();

        if start < n_sealed {
            let rel = self.sealed[start..]
                .binary_search_by_key(&target, |b| b.last_doc_id)
                .unwrap_or_else(|insertion_point| insertion_point);
            let abs = start + rel;
            if abs < n_sealed {
                return abs;
            }
        }

        let pending_start = start.saturating_sub(n_sealed);
        if pending_start < n_pending {
            let rel = self.pending[pending_start..]
                .binary_search_by_key(&target, |b| b.last_doc_id)
                .unwrap_or_else(|insertion_point| insertion_point);
            let abs = n_sealed + pending_start + rel;
            if abs < n_sealed + n_pending {
                return abs;
            }
        }

        self.block_count()
    }

    /// Access to the writable tail block (last `pending` slot), for test-only inspection.
    #[cfg(test)]
    pub(crate) fn tail(&self) -> Option<&IndexBlock> {
        self.pending.last().map(|arc| &**arc)
    }
}
