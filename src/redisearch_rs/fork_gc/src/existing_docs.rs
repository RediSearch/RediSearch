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
use inverted_index::GcScanDelta;

use crate::util::{deserialize, serialize};
use crate::{ForkGC, GcApplyStats, HandleError, HandleOutcome};

/// The `existingDocs` inverted index was removed between the child's scan and
/// the parent's apply (a race between GC and a concurrent index drop).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the existingDocs inverted index was removed before the delta could be applied")]
pub struct ExistingDocsDeleted;

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
    let doc_exists = |id| spec.doc_exists(id);

    if let Some(ii) = spec.existing_docs()
        && let Ok(Some(deltas)) = ii.scan_gc(doc_exists)
    {
        serialize(writer, Some(deltas))?;
    }

    serialize(writer, None::<GcScanDelta>)
}

/// Apply a pre-decoded GC delta to the spec's `existingDocs` inverted index.
///
/// Returns [`GcApplyStats`] the caller flushes to the spec and the GC via
/// [`GcApplyStats::apply`].
///
/// Returns `Err(HandleError::Custom(ExistingDocsDeleted))` when the spec has no
/// `existingDocs` index, which can happen if the index was removed between
/// the child's scan and the parent's apply.
pub fn apply_existing_docs(
    delta: GcScanDelta,
    guard: &mut IndexSpecWriteGuard<'_>,
) -> Result<GcApplyStats, HandleError<ExistingDocsDeleted>> {
    let Some(ii) = guard.existing_docs_mut() else {
        return Err(HandleError::Custom(ExistingDocsDeleted));
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
pub fn handle_existing_docs(
    fgc: &mut ForkGC,
) -> Result<HandleOutcome, HandleError<ExistingDocsDeleted>> {
    crate::util::handle_one(
        fgc,
        |reader| deserialize(reader, "decoding existing-docs entry"),
        apply_existing_docs,
    )
}
