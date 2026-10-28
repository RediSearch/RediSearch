/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The backend contract for an inverted index.
//!
//! An [`IndexBackend`] captures everything the rest of the module needs from an inverted
//! index — writes, reads, GC, and introspection — independent of *how* the blocks are
//! reached. That lets wrappers such as
//! [`EntriesTrackingIndex`](crate::EntriesTrackingIndex) and
//! [`FieldMaskTrackingIndex`](crate::FieldMaskTrackingIndex) forward the same
//! storage surface while preserving their own accounting.

use ffi::IndexFlags;
use index_result::RSIndexResult;
use rqe_core::DocId;

use crate::{
    AddRecordOutcome, DecodedBy, Encoder, GcApplyInfo, GcScanDelta, IndexReader, IndexReaderCore,
    InvertedIndex, RepairContext,
    debug::{BlockSummary, Summary},
    numeric::{NumericEncoder, PreparedValue},
};

/// Operations every inverted-index storage type provides. Implementers can be the core
/// index or wrappers that add bookkeeping while delegating the actual storage.
///
/// Not object-safe (generic GC-closure params + a GAT reader) — that is intentional:
/// generic callers are statically dispatched, so there is no vtable cost.
pub trait IndexBackend {
    /// The reader this backend hands out. Must implement [`IndexReader`].
    type Reader<'index>: IndexReader<'index>
    where
        Self: 'index;

    /// Encode and append one record. Returns how the index's memory changed.
    fn add_record(&mut self, record: &RSIndexResult) -> std::io::Result<AddRecordOutcome>;

    /// Total heap bytes owned by this index (blocks + buffers + bookkeeping).
    fn memory_usage(&self) -> usize;

    /// Index flags (encoding options).
    fn flags(&self) -> IndexFlags;

    /// Number of unique documents indexed.
    fn unique_docs(&self) -> u32;

    /// Number of blocks currently in the index.
    fn number_of_blocks(&self) -> usize;

    /// A summary of the index for introspection (`FT.INFO`).
    fn summary(&self) -> Summary;

    /// Per-block summaries for introspection (`FT.DEBUG`).
    fn blocks_summary(&self) -> Vec<BlockSummary>;

    /// The highest document id in the index, or `None` if empty.
    fn last_doc_id(&self) -> Option<DocId>;

    /// A reader positioned at the start of the index.
    fn reader(&self) -> Self::Reader<'_>;

    /// Scan for garbage: `doc_exist` reports whether a doc id is still live; `repair`, if
    /// given, is invoked per surviving record. Returns the delta to apply, or `None` if the
    /// index is unchanged.
    fn scan_gc(
        &self,
        doc_exist: impl Fn(DocId) -> bool,
        repair: Option<impl for<'call> FnMut(&RSIndexResult<'call>, &RepairContext<'call>)>,
    ) -> std::io::Result<Option<GcScanDelta>>;

    /// Apply a previously-scanned GC delta to this index.
    fn apply_gc(&mut self, delta: GcScanDelta) -> GcApplyInfo;
}

/// Prepared numeric writes required by numeric range indexes.
pub trait NumericIndexBackend: IndexBackend {
    /// Add an entry whose numeric representation the caller already prepared.
    fn add_prepared_record(
        &mut self,
        doc_id: DocId,
        prepared: PreparedValue,
        has_field_expiration: bool,
    ) -> std::io::Result<AddRecordOutcome>;
}

impl<E: Encoder + DecodedBy> IndexBackend for InvertedIndex<E> {
    type Reader<'index>
        = IndexReaderCore<'index, E>
    where
        Self: 'index;

    fn add_record(&mut self, record: &RSIndexResult) -> std::io::Result<AddRecordOutcome> {
        self.add_record(record)
    }

    fn memory_usage(&self) -> usize {
        self.memory_usage()
    }

    fn flags(&self) -> IndexFlags {
        self.flags()
    }

    fn unique_docs(&self) -> u32 {
        self.unique_docs()
    }

    fn number_of_blocks(&self) -> usize {
        self.number_of_blocks()
    }

    fn summary(&self) -> Summary {
        self.summary()
    }

    fn blocks_summary(&self) -> Vec<BlockSummary> {
        self.blocks_summary()
    }

    fn last_doc_id(&self) -> Option<DocId> {
        self.last_doc_id()
    }

    fn reader(&self) -> Self::Reader<'_> {
        self.reader()
    }

    fn scan_gc(
        &self,
        doc_exist: impl Fn(DocId) -> bool,
        repair: Option<impl for<'call> FnMut(&RSIndexResult<'call>, &RepairContext<'call>)>,
    ) -> std::io::Result<Option<GcScanDelta>> {
        self.scan_gc(doc_exist, repair)
    }

    fn apply_gc(&mut self, delta: GcScanDelta) -> GcApplyInfo {
        self.apply_gc(delta)
    }
}

impl<E: NumericEncoder + DecodedBy> NumericIndexBackend for InvertedIndex<E> {
    fn add_prepared_record(
        &mut self,
        doc_id: DocId,
        prepared: PreparedValue,
        has_field_expiration: bool,
    ) -> std::io::Result<AddRecordOutcome> {
        self.add_prepared_record(doc_id, prepared, has_field_expiration)
    }
}
