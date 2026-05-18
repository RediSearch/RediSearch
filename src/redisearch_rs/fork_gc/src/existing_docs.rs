/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Child-side GC collection for the `existingDocs` inverted index.

use std::io;

use inverted_index::ii_dispatch;
use search_ctx::SearchCtx;
use serde::Serialize as _;

use crate::{ForkGC, Frame};

/// Collect the GC delta for the spec's `existingDocs` inverted index and write
/// it to the parent process.
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
pub fn collect_existing_docs(fgc: &mut ForkGC, sctx: &SearchCtx) -> io::Result<()> {
    let spec = sctx.spec();
    let doc_exists = |id| spec.doc_exists(id);
    let mut writer = fgc.writer();

    if let Some(ii) = spec.existing_docs()
        && let Ok(Some(deltas)) = ii_dispatch!(ii, scan_gc, doc_exists, None::<fn(&_, &_)>)
    {
        writer.write_frame(Frame::Empty)?;
        deltas
            .serialize(&mut rmp_serde::Serializer::new(&mut writer))
            .map_err(io::Error::other)?;
    }

    writer.write_frame(Frame::Terminator)
}
