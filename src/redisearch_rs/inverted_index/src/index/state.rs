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
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.sealed.len() + self.pending.len() + usize::from(self.in_progress.is_some())
    }
}
