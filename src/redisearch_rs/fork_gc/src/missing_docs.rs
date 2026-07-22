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

use crate::{ForkGC, Frame, GcApplyStats, HandleError, HandleOutcome};
use hidden_string::OwnedHiddenString;
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::GcScanDelta;
use serde::Serialize as _;

/// The field was removed from `missingFieldDict` between the child's scan and
/// the parent's apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the field was removed from missingFieldDict before the delta could be applied")]
pub struct FieldNotFound;

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

        let field_name = entry.key().secret_value();
        Frame::data(field_name.to_bytes()).encode(writer)?;
        deltas
            .serialize(&mut rmp_serde::Serializer::new(&mut *writer))
            .map_err(io::Error::other)?;
    }

    Frame::Terminator.encode(writer)
}

/// Decode one missing-docs message from `reader`.
pub fn receive_missing_docs(
    reader: &mut impl Read,
) -> Result<Option<(CString, GcScanDelta)>, HandleError<FieldNotFound>> {
    let frame = Frame::decode_nul_terminated(reader)
        .map_err(|e| HandleError::codec("reading the missing-docs field-name frame", e))?;

    match frame {
        Frame::Terminator => Ok(None),
        Frame::Data(field_name) => {
            let delta = rmp_serde::from_read::<_, GcScanDelta>(reader)
                .map_err(|e| HandleError::codec("decoding the missing-docs delta", e))?;
            let field_name = field_name
                .into_inner()
                .into_c_string()
                .expect("child always sends a field name that is a valid C string");
            Ok(Some((field_name, delta)))
        }
        Frame::Empty => Err(HandleError::codec(
            "expected a field-name or terminator frame for missing-docs",
            "got an empty frame",
        )),
    }
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
        guard.missing_field_dict_mut().remove(&hidden);
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
    })
}

/// Parent-side handler for one iteration of the missing-docs GC protocol.
///
/// Reads one frame from the pipe:
/// - A [`Frame::Data`] carrying a field name, followed by a [`GcScanDelta`] →
///   applies the delta, updates stats, returns `Ok(HandleOutcome::Collected)`.
/// - A [`Frame::Terminator`] → all fields processed, returns `Ok(HandleOutcome::Done)`.
///
/// Errors map to corresponding `FGCError` variants at the FFI layer.
pub fn handle_missing_docs(fgc: &mut ForkGC) -> Result<HandleOutcome, HandleError<FieldNotFound>> {
    crate::util::handle_one(
        fgc,
        |reader| receive_missing_docs(reader),
        |(field_name, delta), guard| apply_missing_docs(&field_name, delta, guard),
    )
}
