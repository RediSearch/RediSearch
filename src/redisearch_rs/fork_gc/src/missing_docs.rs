/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! GC collection and application for the `missingFieldDict` inverted indexes.

use std::ffi::{CStr, CString};
use std::io::{self, Read, Write};

use crate::{ForkGC, GcApplyStats, HandleError, HandleOutcome};
use hidden_string::OwnedHiddenString;
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::GcScanDelta;
use serde::{Deserialize, Serialize};

use crate::util::{deserialize, serialize};
/// The field was removed from `missingFieldDict` between the child's scan and
/// the parent's apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the field was removed from missingFieldDict before the delta could be applied")]
pub struct FieldNotFound;

/// One missing-field inverted index's GC work, as it travels the pipe.
#[derive(Debug, Serialize, Deserialize)]
pub struct MissingDocsEntry<T = Vec<u8>> {
    /// NUL-terminated name of the missing field whose inverted index the child scanned.
    pub field_name: T,
    /// What the scan found to collect.
    pub delta: GcScanDelta,
}

/// Collect GC deltas for every entry in the spec's `missingFieldDict` and write
/// them to the parent process.
///
/// Iterates the dict and, for each entry with a non-null inverted index,
/// attempts a GC scan. When a scan produces a delta, a serialised
/// [`MissingDocsEntry`] is written. Entries that produce no delta or fail the
/// scan are skipped. A serialised `None` terminates the stream.
///
/// Scan errors are silently ignored (same block is retried on the next GC
/// cycle). Write errors are surfaced to the caller so they can terminate the
/// child process.
///
/// [`GcScanDelta`]: inverted_index::GcScanDelta
pub fn collect_missing_docs(writer: &mut impl Write, spec: &IndexSpecReadGuard) -> io::Result<()> {
    let doc_exists = |id| spec.doc_exists(id);

    for entry in spec.missing_field_dict().iter() {
        let Ok(Some(deltas)) = entry.val().scan_gc(doc_exists) else {
            continue;
        };

        let field_name = entry.key().secret_value();
        serialize(
            writer,
            Some(MissingDocsEntry {
                field_name: field_name.to_bytes_with_nul(),
                delta: deltas,
            }),
        )?;
    }

    serialize(writer, None::<MissingDocsEntry<&[u8]>>)
}

/// Decode one missing-docs message from `reader`.
pub fn receive_missing_docs(
    reader: &mut impl Read,
) -> Result<Option<(CString, GcScanDelta)>, HandleError<FieldNotFound>> {
    deserialize::<Option<MissingDocsEntry>, FieldNotFound>(reader, "decoding missing-docs entry")?
        .map(|MissingDocsEntry { field_name, delta }| {
            CString::from_vec_with_nul(field_name)
                .map(|field_name| (field_name, delta))
                .map_err(|e| HandleError::codec("decoding missing-docs field name", e))
        })
        .transpose()
}

/// Apply a pre-decoded GC delta to the field's inverted index.
///
/// The field's inverted index is removed from the dict once it has no unique docs left.
///
/// Returns [`GcApplyStats`] the caller flushes to the spec and the GC via
/// [`GcApplyStats::apply`].
pub fn apply_missing_docs(
    field_name: &CStr,
    delta: GcScanDelta,
    guard: &mut IndexSpecWriteGuard<'_>,
) -> Result<GcApplyStats, HandleError<FieldNotFound>> {
    let hidden = OwnedHiddenString::new(field_name);

    let Some(ii) = guard.missing_field_dict_mut().fetch_mut(&hidden) else {
        return Err(HandleError::Custom(FieldNotFound));
    };

    let gc_info = ii.apply_gc(delta);

    let (extra, remaining_blocks) = if ii.unique_docs() == 0 {
        let extra = ii.memory_usage();
        let remaining_blocks = ii.number_of_blocks();
        let removed = guard.missing_field_dict_mut().remove(&hidden);
        debug_assert!(
            removed,
            "`fetch_mut` found this field in the same dict, under the write lock we still hold"
        );
        (extra, remaining_blocks)
    } else {
        (0, 0)
    };

    Ok(GcApplyStats {
        // `records_removed` is 0: missingFieldDict entries are not counted
        // on insertion (they are internal bookkeeping), so we do not count
        // them on removal either.
        records_removed: 0,
        bytes_collected: gc_info.bytes_freed + extra,
        bytes_allocated: gc_info.bytes_allocated,
        block_count_delta: gc_info.block_count_delta - remaining_blocks as i64,
        blocks_denied: gc_info.ignored_last_block as u64,
        ..GcApplyStats::default()
    })
}

/// Parent-side handler for one iteration of the missing-docs GC protocol.
///
/// Reads one [`MissingDocsEntry`] from the pipe, applies its delta, updates
/// stats, and returns `Ok(HandleOutcome::Collected)`. A serialised `None`
/// returns `Ok(HandleOutcome::Done)`.
///
/// Errors map to corresponding `FGCError` variants at the FFI layer.
pub fn handle_missing_docs(fgc: &mut ForkGC) -> Result<HandleOutcome, HandleError<FieldNotFound>> {
    crate::util::handle_one(
        fgc,
        |reader| receive_missing_docs(reader),
        |(field_name, delta), guard| apply_missing_docs(&field_name, delta, guard),
    )
}
