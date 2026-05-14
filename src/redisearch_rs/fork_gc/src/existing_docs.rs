/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! GC collection and application for the `existingDocs` inverted index.

use std::io::{self, Write};

use index_spec::IndexSpecReadGuard;
use inverted_index::GcScanDelta;
use serde::Serialize as _;

use crate::{ForkGC, Frame};

/// Result returned by [`handle_existing_docs`].
///
/// Mirrors the `FGCError` variants relevant to a single parent-side receive
/// iteration (mapped to `FGCError` in the FFI layer).
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

/// Parent-side handler for the `existingDocs` GC protocol.
///
/// Reads a [`GcScanDelta`] from the pipe, applies it to the spec's
/// `existingDocs` inverted index under the write lock, and updates
/// statistics on both the spec and the [`ForkGC`].
///
/// Returns [`HandleResult::Done`] when the child sent no data (empty
/// index or no GC needed).
pub fn handle_existing_docs(fgc: &mut ForkGC) -> HandleResult {
    match Frame::decode(&mut fgc.reader()) {
        Ok(Frame::Terminator) => return HandleResult::Done,
        Ok(Frame::Empty) => (),
        _ => return HandleResult::ChildError,
    };

    let Ok(delta) = rmp_serde::from_read::<_, GcScanDelta>(&mut fgc.reader()) else {
        return HandleResult::ChildError;
    };

    let Some(mut spec_ref) = fgc.index_spec().promote() else {
        return HandleResult::SpecDeleted;
    };

    let mut guard = spec_ref.write();

    let Some(ii) = guard.existing_docs_mut() else {
        return HandleResult::ChildError;
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

    // in the old code ForkGC stats are updated before the write guard and strong ref are dropped,
    // but in Rust that is impossible because we can't mutably borrow fgc while fgc.index_spec is
    // mutably borrowed.
    drop(guard);
    drop(spec_ref);

    fgc.update_gc_stats(
        info.bytes_freed + extra,
        info.bytes_allocated,
        info.ignored_last_block,
    );

    HandleResult::Collected
}
