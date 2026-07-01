/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! GC collection and application for the `missingFieldDict` inverted indexes.

use std::io::{self, Read, Write};

use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::GcScanDelta;
use serde::Serialize as _;

use crate::util::with_hidden_string_ref;
use crate::{ForkGC, Frame};

/// Successful outcome of [`handle_missing_docs`].
pub enum HandleOutcome {
    /// Delta received and applied; iteration may continue.
    Collected,
    /// Child sent a terminator; all missing-docs entries have been processed.
    Done,
}

/// Error returned by [`handle_missing_docs`] and its sub-functions.
///
/// Mapped to `FGCError` variants at the FFI layer.
#[derive(Debug)]
pub enum HandleError {
    /// Pipe read or deserialisation failed; child likely crashed.
    ChildError,
    /// The index spec was deleted before the delta could be applied.
    SpecDeleted,
    /// The field was removed from `missingFieldDict` between the child's scan
    /// and the parent's apply (race between GC and a concurrent field removal).
    FieldNotFound,
}

/// Statistics produced by [`apply_missing_docs`], forwarded by
/// [`handle_missing_docs`] to [`update_stats`].
pub struct ApplyInfo {
    /// Net change in block count for this GC pass.
    pub added_block_count: i64,
    /// Bytes freed by this GC pass.
    pub bytes_freed: usize,
    /// Bytes allocated during compaction (new block overhead).
    pub bytes_allocated: usize,
    /// Number of entries removed from the inverted index.
    pub entries_removed: usize,
    /// Whether the last block was skipped to avoid data races.
    pub ignored_last_block: bool,
}

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

    for entry in spec.missing_field_dict().iter() {
        let Ok(Some(deltas)) = entry.val().scan_gc(doc_exists) else {
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

/// Decode one missing-docs message from `reader`.
///
/// Returns `Ok(Some((field_name, delta)))` when the child sent a GC delta for a
/// field, `Ok(None)` when the child sent a terminator (all fields processed), or
/// `Err(HandleError::ChildError)` on any read or deserialisation failure.
pub fn receive_missing_docs(
    reader: &mut impl Read,
) -> Result<Option<(Box<[u8]>, GcScanDelta)>, HandleError> {
    match Frame::decode(reader).map_err(|_| HandleError::ChildError)? {
        Frame::Terminator => Ok(None),
        Frame::Data(field_name) => {
            let delta = rmp_serde::from_read::<_, GcScanDelta>(reader)
                .map_err(|_| HandleError::ChildError)?;
            Ok(Some((field_name.into_inner(), delta)))
        }
        _ => Err(HandleError::ChildError),
    }
}

/// Apply a pre-decoded GC delta to the field's inverted index in `missingFieldDict`.
///
/// Looks up `field_name` in `missingFieldDict` and applies `delta`. If the inverted
/// index has no remaining documents after the apply, removes the entry from the dict
/// (the dict's `valDestructor`, `InvIndFreeCb`, frees the inverted index on removal).
///
/// Returns [`ApplyInfo`] with counters the caller can forward to [`update_stats`].
///
/// Returns `Err(HandleError::FieldNotFound)` when the field is absent from
/// `missingFieldDict`, which can happen if it was removed between the child's scan
/// and the parent's apply.
pub fn apply_missing_docs(
    field_name: &[u8],
    delta: GcScanDelta,
    guard: &mut IndexSpecWriteGuard<'_>,
) -> Result<ApplyInfo, HandleError> {
    with_hidden_string_ref(field_name, |key| {
        let Some(ii) = guard.missing_field_dict_mut().fetch_mut(key) else {
            return Err(HandleError::FieldNotFound);
        };

        let gc_info = ii.apply_gc(delta);

        let (extra, remaining_blocks) = if ii.unique_docs() == 0 {
            let extra = ii.memory_usage();
            let remaining_blocks = ii.number_of_blocks();
            guard.missing_field_dict_mut().remove(key);
            (extra, remaining_blocks)
        } else {
            (0, 0)
        };

        Ok(ApplyInfo {
            added_block_count: gc_info.block_count_delta - remaining_blocks as i64,
            bytes_freed: gc_info.bytes_freed + extra,
            bytes_allocated: gc_info.bytes_allocated,
            entries_removed: gc_info.entries_removed,
            ignored_last_block: gc_info.ignored_last_block,
        })
    })
}

/// Update both the spec-level and GC-level statistics after applying a GC delta.
///
/// Combines the spec stats update (done under the write lock) and the GC stats
/// update that always go together after a successful [`apply_missing_docs`] call.
pub fn update_stats(info: &ApplyInfo, guard: &mut IndexSpecWriteGuard<'_>, fgc: &mut ForkGC) {
    guard.add_block_count(info.added_block_count);
    // entries_removed is 0: existingDocs entries are not counted on insertion
    // (they are internal duplicates), so we do not count them on removal either.
    guard.update_gc_stats(0, info.bytes_freed, info.bytes_allocated);
    fgc.update_gc_stats(
        info.bytes_freed,
        info.bytes_allocated,
        info.ignored_last_block,
    );
}

/// Parent-side handler for one iteration of the missing-docs GC protocol.
///
/// Reads one frame from the pipe:
/// - A [`Frame::Data`] carrying a field name, followed by a [`GcScanDelta`] →
///   applies the delta, updates stats, returns `Ok(HandleOutcome::Collected)`.
/// - A [`Frame::Terminator`] → all fields processed, returns `Ok(HandleOutcome::Done)`.
///
/// Errors map to corresponding `FGCError` variants at the FFI layer.
pub fn handle_missing_docs(fgc: &mut ForkGC) -> Result<HandleOutcome, HandleError> {
    let Some((field_name, delta)) = receive_missing_docs(&mut fgc.reader())? else {
        return Ok(HandleOutcome::Done);
    };

    let mut spec_ref = fgc.index_spec().promote().ok_or(HandleError::SpecDeleted)?;
    let mut guard = spec_ref.write();

    let info = apply_missing_docs(&field_name, delta, &mut guard)?;
    update_stats(&info, &mut guard, fgc);

    Ok(HandleOutcome::Collected)
}
