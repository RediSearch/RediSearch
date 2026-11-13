/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! GC collection and application for the per-dimension posting lists of every
//! SPARSEVECTOR field in a spec.
//!
//! A dimension whose posting list GC empties is removed from its index, so
//! the memory a deleted document held is returned and `FT.INFO` stops counting
//! the dimension. Query iterators re-resolve their dimensions when they
//! revalidate, which is what makes freeing a list safe.

use std::io::{self, Write};

use field_spec::{FieldSpec, FieldSpecType};
use index_result::RSIndexResult;
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::{DocLiveness, GcScanDelta, GcSnapshotScope, RepairContext};
use rqe_core::FieldIndex;
use serde::{Deserialize, Serialize};
use sparse_vector_index::{PostingList, SparseVectorIndex};

use crate::fork::wire::{deserialize, serialize};
use crate::{ForkPipe, GcApplyStats, GcError, HandleOutcome};

/// Sparse posting lists need no repair callback: they carry no per-document
/// aggregate that a collected entry could leave stale.
const NO_REPAIR: Option<fn(&RSIndexResult<'_>, &RepairContext<'_>)> = None;

/// The sparse vector index behind `fs`, or `None` when the field is not a
/// sparse vector field or no document has been written to it yet.
fn fetch_sparse_index(fs: &FieldSpec) -> Option<&SparseVectorIndex> {
    if !fs.types().contains(FieldSpecType::SparseVector) {
        return None;
    }
    let raw_fs = std::ptr::from_ref(fs).cast::<ffi::FieldSpec>();
    // SAFETY: `FieldSpec` is a `repr(transparent)` wrapper of `ffi::FieldSpec`.
    let raw_fs = unsafe { &*raw_fs };
    // SAFETY: the field is a sparse vector field, so `sparseVecOpts` is the
    // active union member.
    let index = unsafe { raw_fs.__bindgen_anon_1.sparseVecOpts.sparseVecIndex };
    // SAFETY: a non-null index is the `SparseVectorIndex` the FFI allocated for
    // this field, owned by it and kept alive by the caller's spec lock.
    unsafe { index.cast::<SparseVectorIndex>().as_ref() }
}

/// [`fetch_sparse_index()`], exclusively.
fn fetch_sparse_index_mut(fs: &mut FieldSpec) -> Option<&mut SparseVectorIndex> {
    if !fs.types().contains(FieldSpecType::SparseVector) {
        return None;
    }
    let raw_fs = std::ptr::from_mut(fs).cast::<ffi::FieldSpec>();
    // SAFETY: as in `fetch_sparse_index`.
    let raw_fs = unsafe { &mut *raw_fs };
    // SAFETY: as in `fetch_sparse_index`.
    let index = unsafe { raw_fs.__bindgen_anon_1.sparseVecOpts.sparseVecIndex };
    // SAFETY: as in `fetch_sparse_index`; the caller's write lock makes the
    // borrow unique.
    unsafe { index.cast::<SparseVectorIndex>().as_mut() }
}

/// One dimension's worth of GC work, as it travels the pipe.
///
/// Each message locates its field and dimension, so the parent can route and
/// check it on its own.
#[derive(Debug, Serialize, Deserialize)]
pub struct SparseVectorEntry {
    /// Position of the field in the spec's fields, which only grow.
    pub field_index: FieldIndex,
    /// [`SparseVectorIndex::unique_id`] of the field's index.
    pub index_unique_id: u32,
    /// The dimension whose posting list was scanned.
    pub dim_id: u32,
    /// Unique id of that posting list.
    pub list_unique_id: u32,
    /// What the scan found to collect.
    pub delta: GcScanDelta,
}

/// A [`SparseVectorEntry`] whose scan has not run yet: a private copy of the
/// posting list, taken under the lock so the scan itself needs none.
pub struct SparseVectorSnapshot {
    /// As [`SparseVectorEntry::field_index`].
    pub field_index: FieldIndex,
    /// As [`SparseVectorEntry::index_unique_id`].
    pub index_unique_id: u32,
    /// As [`SparseVectorEntry::dim_id`].
    pub dim_id: u32,
    /// As [`SparseVectorEntry::list_unique_id`].
    pub list_unique_id: u32,
    /// The copy to scan; see [`PostingList::gc_snapshot`].
    pub snapshot: PostingList,
}

impl SparseVectorSnapshot {
    /// Run the GC scan on the copy; `Ok(None)` when it finds nothing to collect,
    /// the error when the copy would not decode.
    pub fn scan(self, live: impl DocLiveness) -> io::Result<Option<SparseVectorEntry>> {
        let Some(delta) = self.snapshot.scan_gc(live, NO_REPAIR)? else {
            return Ok(None);
        };
        Ok(Some(SparseVectorEntry {
            field_index: self.field_index,
            index_unique_id: self.index_unique_id,
            dim_id: self.dim_id,
            list_unique_id: self.list_unique_id,
            delta,
        }))
    }
}

fn field_index_of(position: usize) -> FieldIndex {
    FieldIndex::try_from(position).expect("a spec has fewer fields than a field index can count")
}

/// Collect GC deltas for every dimension of every SPARSEVECTOR field in the
/// spec and write them to the parent process.
///
/// Scan failures skip the dimension, leaving it for the next GC cycle. Write
/// errors are surfaced so the caller can terminate the child process.
pub fn collect_sparse_vectors(
    writer: &mut impl Write,
    spec: &IndexSpecReadGuard,
    live: impl DocLiveness + Copy,
) -> io::Result<()> {
    for (position, fs) in spec.field_specs().iter().enumerate() {
        let Some(index) = fetch_sparse_index(fs) else {
            continue;
        };
        for dim_id in index.dimension_ids() {
            let list = index
                .get_dimension(dim_id)
                .expect("an ID the index listed has a posting list");
            let Some(delta) = list.scan_gc(live, NO_REPAIR).ok().flatten() else {
                continue;
            };
            serialize(
                writer,
                Some(SparseVectorEntry {
                    field_index: field_index_of(position),
                    index_unique_id: index.unique_id(),
                    dim_id,
                    list_unique_id: u32::from(list.unique_id()),
                    delta,
                }),
            )?;
        }
    }
    serialize(writer, None::<SparseVectorEntry>)
}

/// Where a [`scan_sparse_vectors_after`] walk resumes: a field, and the last
/// dimension of that field it visited.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SparseVectorsCursor {
    /// Position of the field in the spec's field array, which only grows.
    field_index: FieldIndex,
    /// The last dimension visited in that field; `None` before the first.
    after: Option<u32>,
}

/// Snapshot the dimensions of every sparse vector field, in field then
/// dimension-ID order, starting strictly after `cursor`, until `should_stop`
/// returns `true` after a dimension or none is left.
///
/// Appends a [`SparseVectorSnapshot`] of each dimension whose posting list
/// `live` finds a gone document in to `found`, and moves `cursor` past every
/// dimension visited. Returns whether `should_stop` ended the walk.
pub fn scan_sparse_vectors_after(
    spec: &IndexSpecReadGuard,
    cursor: &mut SparseVectorsCursor,
    live: impl DocLiveness + Copy,
    mut should_stop: impl FnMut() -> bool,
    found: &mut Vec<SparseVectorSnapshot>,
) -> bool {
    let fields = spec.field_specs().iter().enumerate();
    for (position, fs) in fields.skip(usize::from(cursor.field_index)) {
        let field_index = field_index_of(position);
        if let Some(index) = fetch_sparse_index(fs) {
            for dim_id in index.dimension_ids_after(cursor.after) {
                let list = index
                    .get_dimension(dim_id)
                    .expect("an ID the index listed has a posting list");
                if let Some(snapshot) = list.gc_snapshot(&live, GcSnapshotScope::DirtyBlocks) {
                    found.push(SparseVectorSnapshot {
                        field_index,
                        index_unique_id: index.unique_id(),
                        dim_id,
                        list_unique_id: u32::from(list.unique_id()),
                        snapshot,
                    });
                }
                cursor.after = Some(dim_id);
                if should_stop() {
                    return true;
                }
            }
        }
        cursor.field_index = field_index
            .checked_add(1)
            .expect("a spec has fewer fields than a field index can count");
        cursor.after = None;
    }
    false
}

/// [`apply_sparse_vector_entry_with`] without a liveness source: a grown last
/// block is left for the next cycle.
pub fn apply_sparse_vector_entry(
    entry: SparseVectorEntry,
    guard: &mut IndexSpecWriteGuard<'_>,
) -> Result<GcApplyStats, GcError> {
    apply_sparse_vector_entry_with(entry, None, guard)
}

/// Apply one decoded message to its field's sparse vector index, removing the
/// dimension if its posting list is left empty.
///
/// With `live`, a last block that grew since the scan is rescanned against it
/// under the write lock instead of being denied; it must be the source the scan
/// used.
pub fn apply_sparse_vector_entry_with(
    entry: SparseVectorEntry,
    live: Option<&dyn DocLiveness>,
    guard: &mut IndexSpecWriteGuard<'_>,
) -> Result<GcApplyStats, GcError> {
    let fs = guard
        .field_specs_mut()
        .get_mut(usize::from(entry.field_index))
        .ok_or(GcError::TargetGone(
            "the spec has no field at the scanned position",
        ))?;
    let index = fetch_sparse_index_mut(fs).ok_or(GcError::TargetGone(
        "the field no longer has a sparse vector index",
    ))?;
    if index.unique_id() != entry.index_unique_id {
        return Err(GcError::TargetGone(
            "the field's sparse vector index is not the one the scan saw",
        ));
    }
    let outcome = index
        .apply_gc(entry.dim_id, entry.list_unique_id, entry.delta, live)
        .ok_or(GcError::TargetGone(
            "the dimension's posting list was removed or replaced since the scan",
        ))?;

    let (extra_bytes, remaining_blocks) = outcome
        .removed
        .map_or((0, 0), |removed| (removed.bytes, removed.blocks));
    Ok(GcApplyStats {
        records_removed: outcome.info.entries_removed,
        bytes_collected: outcome.info.bytes_freed + extra_bytes,
        bytes_allocated: outcome.info.bytes_allocated,
        block_count_delta: outcome.info.block_count_delta - remaining_blocks as i64,
        blocks_denied: outcome.info.ignored_last_block as u64,
        ..GcApplyStats::default()
    })
}

/// Parent-side handler for one iteration of the sparse vector GC protocol.
///
/// Reads one message from the pipe: an entry is applied and reported as
/// [`HandleOutcome::Collected`]; a terminator ends the iteration with
/// [`HandleOutcome::Done`].
pub fn handle_sparse_vectors(fgc: &mut ForkPipe) -> Result<HandleOutcome, GcError> {
    crate::fork::wire::handle_one(
        fgc,
        |reader| deserialize(reader, "decoding sparse vector entry"),
        apply_sparse_vector_entry,
    )
}
