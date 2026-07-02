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
//! lock, which captures both regions plus the tail block's entry count at once:
//!
//! - `sealed`: cheap [`Arc::clone`] (refcount bump only, no data copy).
//! - `pending`: shallow [`Vec::clone`] (clones the pointer slots; the [`Arc<IndexBlock>`]s
//!   they point to are refcount-bumped, not deep-copied).
//! - `tail_num_entries`: a snapshot-time copy of the live tail block's `num_entries`. The
//!   field is captured *before* the `pending` Vec clone, so it bounds the snapshot's view
//!   of the tail against any in-place append the writer might race ahead with (the
//!   transient refcount = 1 window between the snapshot reading the count and bumping the
//!   tail Arc's refcount via Vec clone). The atomic release on the writer's `num_entries`
//!   store pairs with the snapshot's acquire read here.
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
/// - A shallow clone of `pending`: the [`Vec`] of [`Arc<IndexBlock>`] pointer slots is
///   copied, but the block data behind each `Arc` is shared.
/// - A captured `tail_num_entries`: the value of the tail block's `num_entries` at
///   snapshot time, used to bound iteration on the tail block against any concurrent
///   writer-side in-place append.
///
/// All three are captured together while the caller holds the spec read lock, so no
/// concurrent writer/GC can split the snapshot across an inconsistent moment. After the
/// lock is released the snapshot is fully owned and can be walked without coordination.
#[derive(Debug)]
pub struct InvertedIndexSnapshot {
    sealed: Arc<ThinVec<IndexBlock, BlockCapacity>>,
    pending: Vec<Arc<IndexBlock>>,
    /// `num_entries` of the tail block as observed at snapshot construction time.
    /// `0` when both `sealed` and `pending` are empty (i.e. no tail block).
    /// Iterators on the tail should not exceed this count — see the module docs for
    /// the writer/snapshot ordering contract.
    tail_num_entries: u16,
}

impl InvertedIndexSnapshot {
    /// Construct from the two regions of an [`InvertedIndex`](super::core::InvertedIndex)
    /// plus the captured tail entry count. Caller is responsible for capturing all three
    /// under the same spec-read-lock acquisition (and for reading `tail_num_entries`
    /// before cloning the tail Arc — see module docs).
    pub(crate) const fn new(
        sealed: Arc<ThinVec<IndexBlock, BlockCapacity>>,
        pending: Vec<Arc<IndexBlock>>,
        tail_num_entries: u16,
    ) -> Self {
        Self {
            sealed,
            pending,
            tail_num_entries,
        }
    }

    /// Total number of blocks visible in the snapshot.
    pub fn block_count(&self) -> usize {
        self.sealed.len() + self.pending.len()
    }

    /// Borrow the block at logical index `idx`. The logical index is flat across:
    /// sealed → pending.
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

    /// Number of entries in the tail block at snapshot time. `0` when the snapshot is
    /// empty. Iterators that walk the tail block should not decode more than this many
    /// records — see the module docs.
    pub const fn tail_num_entries(&self) -> u16 {
        self.tail_num_entries
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
                .binary_search_by_key(&target, |arc| arc.last_doc_id)
                .unwrap_or_else(|insertion_point| insertion_point);
            let abs = n_sealed + pending_start + rel;
            if abs < n_sealed + n_pending {
                return abs;
            }
        }

        self.block_count()
    }
}
