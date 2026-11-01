/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! GC collection and application for the `missing.indexes` inverted indexes.

use std::ffi::{CStr, CString};
use std::io::{self, Read, Write};

use crate::{ForkPipe, GcApplyStats, GcError, HandleOutcome};
use hidden_string::OwnedHiddenString;
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::{DocLiveness, GcScanDelta, GcSnapshotScope, opaque::InvertedIndex};
use serde::{Deserialize, Serialize};

use crate::fork::wire::{deserialize, serialize};
/// One missing-field inverted index's GC work, as it travels the pipe.
#[derive(Debug, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T: serde_bytes::Serialize",
    deserialize = "T: serde_bytes::Deserialize<'de>"
))]
pub struct MissingDocsEntry<T = Vec<u8>> {
    /// NUL-terminated name of the missing field whose inverted index the child scanned.
    #[serde(with = "serde_bytes")]
    pub field_name: T,
    /// Unique id of the inverted index scanned, so the delta is not applied to a
    /// replacement created for the same field after the scan.
    pub unique_id: u32,
    /// What the scan found to collect.
    pub delta: GcScanDelta,
}

/// Collect GC deltas for every entry in the spec's `missing.indexes` and write
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
/// `live` says which documents are gone.
///
/// [`GcScanDelta`]: inverted_index::GcScanDelta
pub fn collect_missing_docs(
    writer: &mut impl Write,
    spec: &IndexSpecReadGuard,
    live: impl DocLiveness + Copy,
) -> io::Result<()> {
    for entry in spec.missing_field_dict().iter() {
        let ii = entry.val();
        let Ok(Some(deltas)) = ii.scan_gc(live) else {
            continue;
        };

        let field_name = entry.key().secret_value();
        serialize(
            writer,
            Some(MissingDocsEntry {
                field_name: field_name.to_bytes_with_nul(),
                unique_id: u32::from(ii.unique_id()),
                delta: deltas,
            }),
        )?;
    }

    serialize(writer, None::<MissingDocsEntry<&[u8]>>)
}

/// Name every missing field that has an inverted index, in dict order.
pub fn missing_field_names(spec: &IndexSpecReadGuard) -> Vec<CString> {
    spec.missing_field_dict()
        .iter()
        .map(|entry| entry.key().secret_value().to_owned())
        .collect()
}

/// Run the GC scan of `field_name`'s missing-docs inverted index, returning the
/// index's unique id with the delta.
///
/// Returns `None` when the field has no such index, the scan found nothing to
/// collect, or the scan failed; a failed scan is retried on the next cycle.
pub fn scan_missing_field(
    spec: &IndexSpecReadGuard,
    field_name: &CStr,
    live: impl DocLiveness + Copy,
) -> Option<(u32, GcScanDelta)> {
    let ii = spec
        .missing_field_dict()
        .fetch(&OwnedHiddenString::new(field_name))?;
    Some((u32::from(ii.unique_id()), ii.scan_gc(live).ok().flatten()?))
}

/// A private copy of what a GC scan of `field_name`'s missing-docs index has to
/// decode; see [`InvertedIndex::gc_snapshot`].
pub fn snapshot_missing_field(
    spec: &IndexSpecReadGuard,
    field_name: &CStr,
    live: &impl DocLiveness,
) -> Option<InvertedIndex> {
    spec.missing_field_dict()
        .fetch(&OwnedHiddenString::new(field_name))?
        .gc_snapshot(live, GcSnapshotScope::DirtyBlocks)
}

/// Decode one missing-docs message from `reader`.
pub fn receive_missing_docs(
    reader: &mut impl Read,
) -> Result<Option<MissingDocsEntry<CString>>, GcError> {
    deserialize::<Option<MissingDocsEntry>>(reader, "decoding missing-docs entry")?
        .map(
            |MissingDocsEntry {
                 field_name,
                 unique_id,
                 delta,
             }| {
                CString::from_vec_with_nul(field_name)
                    .map(|field_name| MissingDocsEntry {
                        field_name,
                        unique_id,
                        delta,
                    })
                    .map_err(|e| GcError::codec("decoding missing-docs field name", e))
            },
        )
        .transpose()
}

/// [`apply_missing_docs_with`] without a liveness source: a grown last block is left for the next cycle.
pub fn apply_missing_docs(
    field_name: &CStr,
    unique_id: u32,
    delta: GcScanDelta,
    guard: &mut IndexSpecWriteGuard<'_>,
) -> Result<GcApplyStats, GcError> {
    apply_missing_docs_with(field_name, unique_id, delta, None, guard)
}

/// Apply a pre-decoded GC delta to the field's inverted index.
///
/// The field's inverted index is removed from the dict once it has no unique docs left.
/// Returns [`GcError::ApplyError`] when the field has no inverted index, or one
/// other than the index scanned: it can be removed, and recreated, between the scan
/// and the apply.
///
/// Returns [`GcApplyStats`] the caller flushes to the spec and the GC via
/// [`GcApplyStats::apply`].
///
/// With `live`, a last block that grew since the scan is rescanned against it under
/// the write lock instead of being denied; it must be the source the scan used, see
/// [`InvertedIndex::gc_snapshot`](inverted_index::InvertedIndex::gc_snapshot).
pub fn apply_missing_docs_with(
    field_name: &CStr,
    unique_id: u32,
    delta: GcScanDelta,
    live: Option<&dyn DocLiveness>,
    guard: &mut IndexSpecWriteGuard<'_>,
) -> Result<GcApplyStats, GcError> {
    let hidden = OwnedHiddenString::new(field_name);

    let Some(ii) = guard.missing_field_dict_mut().fetch_mut(&hidden) else {
        return Err(GcError::ApplyError(
            "the field was removed from missing.indexes before the delta could be applied",
        ));
    };
    if u32::from(ii.unique_id()) != unique_id {
        return Err(GcError::ApplyError(
            "the field's missing-docs inverted index is not the one scanned",
        ));
    }

    let gc_info = ii.apply_gc_with(delta, live);

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
        // `records_removed` is 0: missing.indexes entries are not counted
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
/// Errors map to corresponding `ForkGCError` variants at the FFI layer.
pub fn handle_missing_docs(fgc: &mut ForkPipe) -> Result<HandleOutcome, GcError> {
    crate::fork::wire::handle_one(
        fgc,
        |reader| receive_missing_docs(reader),
        |entry, guard| apply_missing_docs(&entry.field_name, entry.unique_id, entry.delta, guard),
    )
}
