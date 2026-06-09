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

use crate::{ForkGC, ForkGCLite, Frame};

/// Successful outcome of [`handle_existing_docs`].
pub enum HandleOutcome {
    /// Delta received and applied; iteration may continue.
    Collected,
    /// Child sent no data; iteration is complete.
    Done,
}

/// Error returned by [`handle_existing_docs`] and its sub-functions.
///
/// Mapped to `FGCError` variants at the FFI layer.
#[derive(Debug)]
pub enum HandleError {
    /// Pipe read failed; child likely crashed.
    ChildError,
    /// The index spec was deleted before the delta could be applied.
    SpecDeleted,
    /// The `existingDocs` inverted index was removed between the child's scan
    /// and the parent's apply (race between GC and a concurrent index drop).
    ExistingDocsDeleted,
}

/// Statistics produced by [`apply_existing_docs`], forwarded by
/// [`handle_existing_docs`] to [`crate::fork_gc::ForkGCLite::update_gc_stats`].
pub struct ApplyInfo {
    /// Added block count
    pub added_block_count: i64,
    /// Bytes freed by this GC pass.
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
/// or `Err(HandleError::ChildError)` on any read or deserialisation failure.
pub fn receive_existing_docs(reader: &mut impl Read) -> Result<Option<GcScanDelta>, HandleError> {
    match Frame::decode(reader) {
        Ok(Frame::Empty) => rmp_serde::from_read::<_, GcScanDelta>(reader)
            .map(Some)
            .map_err(|_| HandleError::ChildError),
        Ok(Frame::Terminator) => Ok(None),
        _ => Err(HandleError::ChildError),
    }
}

/// Apply a pre-decoded GC delta to the spec's `existingDocs` inverted index.
///
/// Returns [`ApplyInfo`] with counters the caller can forward to
/// [`crate::fork_gc::ForkGCLite::update_gc_stats`].
///
/// Returns `Err(HandleError::ExistingDocsDeleted)` when the spec has no
/// `existingDocs` index, which can happen if the index was removed between
/// the child's scan and the parent's apply.
pub fn apply_existing_docs(
    delta: GcScanDelta,
    guard: &mut IndexSpecWriteGuard<'_>,
) -> Result<ApplyInfo, HandleError> {
    let Some(ii) = guard.existing_docs_mut() else {
        return Err(HandleError::ExistingDocsDeleted);
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

    Ok(ApplyInfo {
        added_block_count: info.block_count_delta - remaining_blocks as i64,
        bytes_freed: info.bytes_freed + extra,
        bytes_allocated: info.bytes_allocated,
        ignored_last_block: info.ignored_last_block,
    })
}

/// Update both the spec-level and GC-level statistics after applying a GC delta.
///
/// Combines the spec stats update (done under the write lock) and the GC stats
/// update (done via [`ForkGCLite`]) that always go together after a successful
/// [`apply_existing_docs`] call.
pub fn update_stats(info: &ApplyInfo, guard: &mut IndexSpecWriteGuard<'_>, lite: &mut ForkGCLite) {
    guard.add_block_count(info.added_block_count);
    // entries_removed is 0: existingDocs entries are not counted on insertion
    // (they are internal duplicates), so we do not count them on removal either.
    guard.update_gc_stats(0, info.bytes_freed, info.bytes_allocated);
    lite.update_gc_stats(
        info.bytes_freed,
        info.bytes_allocated,
        info.ignored_last_block,
    );
}

/// Parent-side handler for the `existingDocs` GC protocol.
///
/// Reads a [`GcScanDelta`] from the pipe, applies it to the spec's
/// `existingDocs` inverted index under the write lock, and updates
/// statistics on both the spec and the GC.
///
/// Returns `Ok(HandleOutcome::Done)` when the child sent no data (empty
/// index or no GC needed).
pub fn handle_existing_docs(fgc: &mut ForkGC) -> Result<HandleOutcome, HandleError> {
    let (lite, index_spec, mut reader) = fgc.split();

    let Some(delta) = receive_existing_docs(&mut reader)? else {
        return Ok(HandleOutcome::Done);
    };

    let mut spec_ref = index_spec.promote().ok_or(HandleError::SpecDeleted)?;
    let mut guard = spec_ref.write();

    let info = apply_existing_docs(delta, &mut guard)?;
    update_stats(&info, &mut guard, lite);

    Ok(HandleOutcome::Collected)
}
