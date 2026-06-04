/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Lock-free snapshot of an inverted index's block storage.
//!
//! See the design doc for FT.HYBRID Workers Pool Consolidation (Epic 1) for context.
//! In short: readers should be able to walk an inverted index's blocks without holding
//! any lock. We expose a consistent snapshot of the block storage via a single
//! [`ArcSwap`](arc_swap::ArcSwap) on the index, so a reader can take one atomic load
//! and walk the result for the lifetime of its iteration.
//!
//! The snapshot is split into three regions to keep the per-write cost small:
//!
//! - [`State::sealed`] — compacted blocks committed by garbage collection. Only GC
//!   writes to this. Reading from it is contiguous and cache-friendly.
//! - [`State::pending`] — full blocks added since the last GC pass. Each block is
//!   `Arc`'d so that a write that closes a new block only allocates a small `Vec`
//!   header rather than copying the whole sealed region.
//! - [`State::in_progress`] — the partial block currently being written to, or
//!   `None` if the index is empty.
//!
//! During Epic 1's dual-write phase (Story 1.1), the canonical block storage is still
//! the [`InvertedIndex::blocks`](super::core::InvertedIndex::blocks) `ThinVec` and
//! the state is a parallel mirror. Stories 1.2-1.3 will switch reads and writes over
//! to the state and remove `blocks`.

use std::sync::Arc;

use rqe_core::DocId;
use thin_vec::ThinVec;

use super::core::IndexBlock;
use crate::BlockCapacity;

/// A consistent snapshot of an inverted index's block storage.
///
/// Constructed only via [`State::empty`] or by cloning Arcs out of an existing state
/// during a `state.rcu(...)` call.
#[derive(Debug)]
pub(crate) struct State {
    /// Compacted blocks committed by GC. Only GC writes to this field. The underlying
    /// [`ThinVec`] is shared via [`Arc`] so that non-GC writes can construct a new
    /// [`State`] without copying the sealed region.
    pub(crate) sealed: Arc<ThinVec<IndexBlock, BlockCapacity>>,

    /// Full blocks added since the last GC compaction. Each block is `Arc`'d so a
    /// write that fills a new block only allocates a small [`Vec`] header and bumps
    /// the new block's `Arc` refcount, without copying the others.
    pub(crate) pending: Arc<Vec<Arc<IndexBlock>>>,

    /// The current partial block being written to, or `None` if the index has no
    /// blocks yet.
    pub(crate) in_progress: Option<Arc<IndexBlock>>,
}

impl State {
    /// An empty state: no sealed blocks, no pending blocks, no in-progress block.
    pub(crate) fn empty() -> Self {
        Self {
            sealed: Arc::new(ThinVec::new()),
            pending: Arc::new(Vec::new()),
            in_progress: None,
        }
    }

    /// Total number of blocks visible in this snapshot.
    pub(crate) fn block_count(&self) -> usize {
        self.sealed.len() + self.pending.len() + usize::from(self.in_progress.is_some())
    }

    /// Alias for [`Self::block_count`], kept for symmetry with `Vec::len`.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.block_count()
    }

    /// Get the block at logical index `idx`. The logical index is flat across the three
    /// regions: `sealed[0..n_sealed]` then `pending[0..n_pending]` then `in_progress`
    /// (if present, occupying the last slot).
    pub(crate) fn get_block(&self, idx: usize) -> Option<&IndexBlock> {
        if let Some(b) = self.sealed.get(idx) {
            return Some(b);
        }
        let idx = idx.checked_sub(self.sealed.len())?;
        if let Some(b) = self.pending.get(idx) {
            return Some(&**b);
        }
        let idx = idx.checked_sub(self.pending.len())?;
        if idx == 0 {
            self.in_progress.as_deref()
        } else {
            None
        }
    }

    /// First block in the snapshot.
    pub(crate) fn first_block(&self) -> Option<&IndexBlock> {
        self.get_block(0)
    }

    /// Last block in the snapshot.
    pub(crate) fn last_block(&self) -> Option<&IndexBlock> {
        let total = self.block_count();
        if total == 0 {
            None
        } else {
            self.get_block(total - 1)
        }
    }

    /// Find the logical index of the first block whose `last_doc_id >= target`, searching
    /// from logical index `start` onward. Returns [`Self::block_count`] if no such block
    /// exists (i.e. `target` is past the end of the index).
    ///
    /// Each of `sealed` and `pending` is sorted by `last_doc_id`, so we can binary-search
    /// within each region. `in_progress` is the single trailing partial block.
    pub(crate) fn find_block_for_doc_id(&self, start: usize, target: DocId) -> usize {
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

        if start <= n_sealed + n_pending
            && let Some(ip) = self.in_progress.as_ref()
            && ip.last_doc_id >= target
        {
            return n_sealed + n_pending;
        }

        self.block_count()
    }
}

/// An owned snapshot of an [`InvertedIndex`](super::core::InvertedIndex)'s block storage.
///
/// Holds an internal [`Arc`] to the [`State`] that was current when the snapshot was
/// taken, so the contained blocks remain alive for the snapshot's entire lifetime —
/// regardless of writes to the source index. Use this to hand block references across
/// boundaries where the borrow checker can't prove the source index outlives the
/// reference (FFI, threads, callbacks).
///
/// The snapshot does **not** see writes made after it was taken; if a fresh view is
/// needed, take a new snapshot.
pub struct InvertedIndexSnapshot {
    state: Arc<State>,
}

impl InvertedIndexSnapshot {
    /// Construct from an `Arc<State>`. Intentionally `pub(crate)` so only the parent
    /// `InvertedIndex::snapshot` constructor — which has access to `self.state` — can
    /// call it.
    pub(crate) fn from_arc(state: Arc<State>) -> Self {
        Self { state }
    }

    /// Total number of blocks visible in the snapshot.
    pub fn block_count(&self) -> usize {
        self.state.block_count()
    }

    /// Borrow the block at the given logical index, or `None` if out of bounds. The
    /// borrow lives as long as `&self`, which is safe: the snapshot keeps the
    /// underlying [`State`] alive via its `Arc`.
    pub fn block_ref(&self, idx: usize) -> Option<&super::core::IndexBlock> {
        self.state.get_block(idx)
    }
}
