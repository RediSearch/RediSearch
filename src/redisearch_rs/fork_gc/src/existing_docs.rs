/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! GC collection and application for the `existingDocs` inverted index.

use std::io::{self, Read, Write};

use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::GcScanDelta;
use serde::Serialize as _;

use crate::{ForkGC, Frame};

/// Result returned by [`handle_existing_docs`].
///
/// Mirrors the `FGCError` variants relevant to a single parent-side receive
/// iteration (mapped to `FGCError` in the FFI layer).
#[derive(Debug)]
pub enum HandleResult {
    /// Delta received and applied; iteration may continue.
    Collected,
    /// Child sent no data; iteration is complete.
    Done,
    /// Pipe read failed; child likely crashed.
    ChildError,
    /// The index spec was deleted before the delta could be applied.
    SpecDeleted,
}

/// Statistics produced by [`apply_existing_docs`], forwarded by
/// [`handle_existing_docs`] to [`ForkGC::update_gc_stats`].
pub struct ApplyInfo {
    /// Net bytes freed (freed minus allocated) by this GC pass.
    pub bytes_freed: usize,
    /// Bytes allocated during compaction (new block overhead).
    pub bytes_allocated: usize,
    /// Whether the last block was skipped to avoid data races.
    pub ignored_last_block: bool,
}

/// Collect the GC delta for the spec's `existingDocs` inverted index and write
/// it to `writer`.
///
/// If the spec has no existing-docs index, or the scan produces no delta, only
/// the terminator is written. Otherwise an empty header followed by the
/// serialised [`GcScanDelta`] is written before the terminator.
///
/// Scan errors are silently ignored (same block is retried on the next GC
/// cycle). Write errors are surfaced to the caller so they can terminate the
/// child process.
///
/// [`GcScanDelta`]: inverted_index::GcScanDelta
pub fn collect_existing_docs(writer: &mut impl Write, spec: &IndexSpecReadGuard) -> io::Result<()> {
    let doc_exists = |id| spec.doc_exists(id);

    if let Some(ii) = spec.existing_docs()
        && let Ok(Some(deltas)) = ii.scan_gc(doc_exists)
    {
        Frame::Empty.encode(writer)?;
        deltas
            .serialize(&mut rmp_serde::Serializer::new(&mut *writer))
            .map_err(io::Error::other)?;
    }

    Frame::Terminator.encode(writer)
}

/// Decode one existing-docs message from `reader`.
///
/// Returns `Ok(Some(delta))` when the child sent a GC delta to apply,
/// `Ok(None)` when the child sent only a terminator (nothing to collect),
/// or `Err(HandleResult::ChildError)` on any read or deserialisation failure.
pub fn receive_existing_docs(
    reader: &mut impl Read,
) -> Result<Option<GcScanDelta>, HandleResult> {
    match Frame::decode(reader) {
        Ok(Frame::Terminator) => return Ok(None),
        Ok(Frame::Empty) => (),
        _ => return Err(HandleResult::ChildError),
    }

    rmp_serde::from_read::<_, GcScanDelta>(reader)
        .map(Some)
        .map_err(|_| HandleResult::ChildError)
}

/// Apply a pre-decoded GC delta to the spec's `existingDocs` inverted index.
///
/// Updates the spec's block-count and GC statistics. Returns [`ApplyInfo`]
/// with counters the caller can forward to [`ForkGC::update_gc_stats`].
///
/// Returns `Err(HandleResult::ChildError)` when the spec has no
/// `existingDocs` index, which can happen if the index was removed between
/// the child's scan and the parent's apply.
pub fn apply_existing_docs(
    delta: GcScanDelta,
    guard: &mut IndexSpecWriteGuard<'_>,
) -> Result<ApplyInfo, HandleResult> {
    let Some(ii) = guard.existing_docs_mut() else {
        return Err(HandleResult::ChildError);
    };

    let info = ii.apply_gc(delta);

    let (extra, remaining_blocks) = if ii.unique_docs() == 0 {
        let extra = ii.memory_usage();
        let remaining_blocks = ii.number_of_blocks();
        guard.clear_existing_docs();
        (extra, remaining_blocks)
    } else {
        (0, 0)
    };

    guard.add_block_count(info.block_count_delta - remaining_blocks as i64);
    guard.update_gc_stats(0, info.bytes_freed + extra, info.bytes_allocated);

    Ok(ApplyInfo {
        bytes_freed: info.bytes_freed + extra,
        bytes_allocated: info.bytes_allocated,
        ignored_last_block: info.ignored_last_block,
    })
}

/// Parent-side handler for the `existingDocs` GC protocol.
///
/// Reads a [`GcScanDelta`] from the pipe, applies it to the spec's
/// `existingDocs` inverted index under the write lock, and updates
/// statistics on both the spec and the [`ForkGC`].
///
/// Returns [`HandleResult::Done`] when the child sent no data (empty
/// index or no GC needed).
pub fn handle_existing_docs(fgc: &mut ForkGC) -> HandleResult {
    // Phase 1: read from the pipe. The reader borrow on `fgc` ends with this
    // block, before the spec borrow begins in phase 2.
    let delta = match receive_existing_docs(&mut fgc.reader()) {
        Ok(None) => return HandleResult::Done,
        Ok(Some(d)) => d,
        Err(r) => return r,
    };

    // Phase 2: promote spec and apply delta. No pipe access from here on.
    let Some(mut spec_ref) = fgc.index_spec().promote() else {
        return HandleResult::SpecDeleted;
    };

    let mut guard = spec_ref.write();

    let info = match apply_existing_docs(delta, &mut guard) {
        Ok(i) => i,
        Err(r) => return r,
    };

    // in the old code ForkGC stats are updated before the write guard and strong ref are dropped,
    // but in Rust that is impossible because we can't mutably borrow fgc while fgc.index_spec is
    // mutably borrowed.
    drop(guard);
    drop(spec_ref);

    fgc.update_gc_stats(info.bytes_freed, info.bytes_allocated, info.ignored_last_block);

    HandleResult::Collected
}
