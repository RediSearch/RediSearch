/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Child-side GC collection for the `missingFieldDict` inverted indexes.

use std::io::{self, Write};

use index_spec::IndexSpecReadGuard;
use serde::Serialize as _;

use crate::Frame;

/// Collect GC deltas for every entry in the spec's `missingFieldDict` and write
/// them to the parent process.
///
/// Iterates the dict and, for each entry with a non-null inverted index,
/// attempts a GC scan. When a scan produces a delta the field-name header
/// followed by the serialised [`GcScanDelta`] is written. Entries that produce
/// no delta or fail the scan are skipped. A terminator is written once all
/// entries are processed.
///
/// Scan errors are silently ignored (same block is retried on the next GC
/// cycle). Write errors are surfaced to the caller so they can terminate the
/// child process.
///
/// [`GcScanDelta`]: inverted_index::GcScanDelta
pub fn collect_missing_docs(writer: &mut impl Write, spec: &IndexSpecReadGuard) -> io::Result<()> {
    let doc_exists = |id| spec.doc_exists(id);

    for entry in spec.missing_field_dict2().iter() {
        let Some(ii) = entry.val() else { continue };

        let Ok(Some(deltas)) = ii.scan_gc(doc_exists) else {
            continue;
        };

        let field_name = entry.key().into_secret_value();
        Frame::data(field_name.to_bytes()).encode(writer)?;
        deltas
            .serialize(&mut rmp_serde::Serializer::new(&mut *writer))
            .map_err(io::Error::other)?;
    }

    Frame::Terminator.encode(writer)
}
