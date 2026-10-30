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

use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::{DocLiveness, GcScanDelta, GcSnapshotScope, opaque::InvertedIndex};

use crate::util::{deserialize, serialize};
use crate::{ForkGC, GcApplyStats, HandleError, HandleOutcome};

/// Collect the GC delta for the spec's `existingDocs` inverted index and write
/// it to `writer`.
///
/// When a scan produces a delta, it is serialised in `Some`. A serialised
/// `None` always follows to terminate the stream.
///
/// Scan errors are silently ignored (same block is retried on the next GC
/// cycle). Write errors are surfaced to the caller so they can terminate the
/// child process.
///
/// [`GcScanDelta`]: inverted_index::GcScanDelta
pub fn collect_existing_docs(writer: &mut impl Write, spec: &IndexSpecReadGuard) -> io::Result<()> {
    if let Some(deltas) = scan_existing_docs(spec, |id| spec.doc_exists(id)) {
        serialize(writer, Some(deltas))?;
    }

    serialize(writer, None::<GcScanDelta>)
}

/// Run the GC scan of the spec's `existingDocs` inverted index.
///
/// Returns `None` when the spec has no such index, the scan found nothing to
/// collect, or the scan failed; a failed scan is retried on the next cycle.
pub fn scan_existing_docs(
    spec: &IndexSpecReadGuard,
    live: impl DocLiveness + Copy,
) -> Option<GcScanDelta> {
    let ii = spec.existing_docs()?;
    ii.scan_gc(live).ok().flatten()
}

/// A private copy of what a GC scan of the `existingDocs` index has to decode;
/// see [`InvertedIndex::gc_snapshot`].
pub fn snapshot_existing_docs(
    spec: &IndexSpecReadGuard,
    live: &impl DocLiveness,
) -> Option<InvertedIndex> {
    spec.existing_docs()?
        .gc_snapshot(live, GcSnapshotScope::DirtyBlocks)
}

/// [`apply_existing_docs_with`] without a liveness source: a grown last block is left for the next cycle.
pub fn apply_existing_docs(
    delta: GcScanDelta,
    guard: &mut IndexSpecWriteGuard<'_>,
) -> Result<GcApplyStats, HandleError> {
    apply_existing_docs_with(delta, None, guard)
}

/// Apply a pre-decoded GC delta to the spec's `existingDocs` inverted index.
///
/// Returns [`GcApplyStats`] the caller flushes to the spec and the GC via
/// [`GcApplyStats::apply`].
///
/// Returns [`HandleError::ApplyError`] when the spec has no `existingDocs`
/// index, which can happen if the index was removed between
/// the child's scan and the parent's apply.
///
/// With `live`, a last block that grew since the scan is rescanned against it under
/// the write lock instead of being denied; it must be the source the scan used, see
/// [`InvertedIndex::gc_snapshot`](inverted_index::InvertedIndex::gc_snapshot).
pub fn apply_existing_docs_with(
    delta: GcScanDelta,
    live: Option<&dyn DocLiveness>,
    guard: &mut IndexSpecWriteGuard<'_>,
) -> Result<GcApplyStats, HandleError> {
    let Some(ii) = guard.existing_docs_mut() else {
        return Err(HandleError::ApplyError(
            "the existingDocs inverted index was removed before the delta could be applied",
        ));
    };

    let info = ii.apply_gc_with(delta, live);

    let (extra, remaining_blocks) = if ii.unique_docs() == 0 {
        let extra = ii.memory_usage();
        let remaining_blocks = ii.number_of_blocks();
        guard.clear_existing_docs();
        (extra, remaining_blocks)
    } else {
        (0, 0)
    };

    Ok(GcApplyStats {
        // `records_removed` is 0: existingDocs entries are not counted on insertion
        // (they are internal duplicates), so we do not count them on removal either.
        records_removed: 0,
        bytes_collected: info.bytes_freed + extra,
        bytes_allocated: info.bytes_allocated,
        block_count_delta: info.block_count_delta - remaining_blocks as i64,
        blocks_denied: info.ignored_last_block as u64,
        ..GcApplyStats::default()
    })
}

/// Parent-side handler for the `existingDocs` GC protocol.
///
/// Reads an optional [`GcScanDelta`] from the pipe, applies it to the spec's
/// `existingDocs` inverted index under the write lock, and updates statistics
/// on both the spec and the GC. A serialised `None` returns
/// `Ok(HandleOutcome::Done)`.
pub fn handle_existing_docs(fgc: &mut ForkGC) -> Result<HandleOutcome, HandleError> {
    crate::util::handle_one(
        fgc,
        |reader| deserialize(reader, "decoding existing-docs entry"),
        apply_existing_docs,
    )
}
