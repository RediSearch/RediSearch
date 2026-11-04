/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! GC collection and application for the `missing.indexes` inverted indexes.

use std::ffi::CString;
use std::io::{self, Read, Write};

use crate::{ForkPipe, GcApplyStats, GcError, HandleOutcome};
use hidden_string::OwnedHiddenString;
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::{DocLiveness, GcScanDelta, GcSnapshotScope, opaque::InvertedIndex};
use serde::{Deserialize, Serialize};

use crate::fork::wire::{deserialize, serialize};
/// One missing-field inverted index's GC work, as it travels the pipe.
#[derive(Debug, Serialize, Deserialize)]
pub struct MissingDocsEntry {
    /// Position of the field, in the spec's field list, whose missing-docs
    /// inverted index was scanned.
    pub field_index: ffi::t_fieldIndex,
    /// Unique id of the inverted index scanned, so the delta is not applied to a
    /// replacement created for the same field after the scan.
    pub unique_id: u32,
    /// What the scan found to collect.
    pub delta: GcScanDelta,
}

/// Collect GC deltas for every entry in the spec's `missing.indexes` and write
/// them to the parent process.
///
/// Walks the spec's fields and, for each one with a missing-docs inverted index,
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
    for field_index in missing_field_indexes(spec) {
        let Some(ii) = missing_index(spec, field_index) else {
            continue;
        };
        let Ok(Some(delta)) = ii.scan_gc(live) else {
            continue;
        };

        serialize(
            writer,
            Some(MissingDocsEntry {
                field_index,
                unique_id: u32::from(ii.unique_id()),
                delta,
            }),
        )?;
    }

    serialize(writer, None::<MissingDocsEntry>)
}

/// The name the missing-docs dict keys the field at `field_index` by, or `None`
/// when there is no such field. Owned, so the dict can be borrowed mutably while
/// it is in use.
fn missing_key(
    field_specs: &[field_spec::FieldSpec],
    field_index: ffi::t_fieldIndex,
) -> Option<CString> {
    let fs = field_specs.get(usize::from(field_index))?;
    Some(fs.field_name().secret_value().to_owned())
}

fn missing_index<'a>(
    spec: &'a IndexSpecReadGuard<'_>,
    field_index: ffi::t_fieldIndex,
) -> Option<&'a InvertedIndex> {
    let key = missing_key(spec.field_specs(), field_index)?;
    spec.missing_field_dict()
        .fetch(&OwnedHiddenString::new(&key))
}

/// Position of every field that has a missing-docs inverted index, in field order.
pub fn missing_field_indexes(spec: &IndexSpecReadGuard) -> Vec<ffi::t_fieldIndex> {
    let dict = spec.missing_field_dict();
    spec.field_specs()
        .iter()
        .enumerate()
        .filter(|(_, fs)| {
            dict.fetch(&OwnedHiddenString::new(fs.field_name().secret_value()))
                .is_some()
        })
        .map(|(i, _)| {
            ffi::t_fieldIndex::try_from(i)
                .expect("a spec has fewer fields than a field index can count")
        })
        .collect()
}

/// Run the GC scan of the missing-docs inverted index of the field at
/// `field_index`, returning the index's unique id with the delta.
///
/// Returns `None` when the field has no such index, the scan found nothing to
/// collect, or the scan failed; a failed scan is retried on the next cycle.
pub fn scan_missing_field(
    spec: &IndexSpecReadGuard,
    field_index: ffi::t_fieldIndex,
    live: impl DocLiveness + Copy,
) -> Option<(u32, GcScanDelta)> {
    let ii = missing_index(spec, field_index)?;
    Some((u32::from(ii.unique_id()), ii.scan_gc(live).ok().flatten()?))
}

/// A private copy of what a GC scan of the missing-docs index of the field at
/// `field_index` has to decode; see [`InvertedIndex::gc_snapshot`].
pub fn snapshot_missing_field(
    spec: &IndexSpecReadGuard,
    field_index: ffi::t_fieldIndex,
    live: &impl DocLiveness,
) -> Option<InvertedIndex> {
    missing_index(spec, field_index)?.gc_snapshot(live, GcSnapshotScope::DirtyBlocks)
}

/// Decode one missing-docs message from `reader`.
pub fn receive_missing_docs(reader: &mut impl Read) -> Result<Option<MissingDocsEntry>, GcError> {
    deserialize(reader, "decoding missing-docs entry")
}

/// [`apply_missing_docs_with`] without a liveness source: a grown last block is left for the next cycle.
pub fn apply_missing_docs(
    field_index: ffi::t_fieldIndex,
    unique_id: u32,
    delta: GcScanDelta,
    guard: &mut IndexSpecWriteGuard<'_>,
) -> Result<GcApplyStats, GcError> {
    apply_missing_docs_with(field_index, unique_id, delta, None, guard)
}

/// Apply a pre-decoded GC delta to the missing-docs inverted index of the field at `field_index`.
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
    field_index: ffi::t_fieldIndex,
    unique_id: u32,
    delta: GcScanDelta,
    live: Option<&dyn DocLiveness>,
    guard: &mut IndexSpecWriteGuard<'_>,
) -> Result<GcApplyStats, GcError> {
    let Some(key) = missing_key(guard.field_specs(), field_index) else {
        return Err(GcError::ApplyError(
            "no field in the spec is at the scanned field index",
        ));
    };
    let hidden = OwnedHiddenString::new(&key);

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
        |entry, guard| apply_missing_docs(entry.field_index, entry.unique_id, entry.delta, guard),
    )
}
