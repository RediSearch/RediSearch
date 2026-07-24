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
//! stored or read. Today the only implementer is the in-place index ([`InvertedIndex`],
//! aliased [`InPlaceInvertedIndex`]): a single mutable block vector, in-place GC repair,
//! and lock-held reads. A future copy-on-write / snapshot backend can implement the same
//! trait, letting callers (and the FFI) be written once against the contract and select the
//! concrete backend at compile time — no runtime dispatch on the hot path.

use ffi::IndexFlags;
use index_result::RSIndexResult;
use rqe_core::DocId;

use crate::{
    AddRecordOutcome, DecodedBy, Decoder, Encoder, GcApplyInfo, GcScanDelta, InPlaceInvertedIndex,
    IndexReader, IndexReaderCore, InvertedIndex, RepairContext,
    debug::{BlockSummary, Summary},
};

/// Operations every inverted-index backend provides. Implementers differ in block storage,
/// read strategy, and GC mechanism, but present this one contract so the FFI layer and query
/// engine are backend-agnostic.
///
/// Not object-safe (generic GC-closure params + a GAT reader) — that is intentional: the
/// active backend is chosen at compile time, so calls monomorphize with zero vtable cost.
pub trait IndexBackend {
    /// The reader this backend hands out. Must implement [`IndexReader`].
    type Reader<'a>: IndexReader<'a>
    where
        Self: 'a;

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

impl<E: Encoder + DecodedBy<Decoder = D>, D: Decoder> IndexBackend for InPlaceInvertedIndex<E> {
    type Reader<'a>
        = IndexReaderCore<'a, E>
    where
        Self: 'a;

    fn add_record(&mut self, record: &RSIndexResult) -> std::io::Result<AddRecordOutcome> {
        InvertedIndex::add_record(self, record)
    }

    fn memory_usage(&self) -> usize {
        InvertedIndex::memory_usage(self)
    }

    fn flags(&self) -> IndexFlags {
        self.flags
    }

    fn unique_docs(&self) -> u32 {
        InvertedIndex::unique_docs(self)
    }

    fn number_of_blocks(&self) -> usize {
        InvertedIndex::number_of_blocks(self)
    }

    fn summary(&self) -> Summary {
        InvertedIndex::summary(self)
    }

    fn blocks_summary(&self) -> Vec<BlockSummary> {
        InvertedIndex::blocks_summary(self)
    }

    fn last_doc_id(&self) -> Option<DocId> {
        InvertedIndex::last_doc_id(self)
    }

    fn reader(&self) -> Self::Reader<'_> {
        InvertedIndex::reader(self)
    }

    fn scan_gc(
        &self,
        doc_exist: impl Fn(DocId) -> bool,
        repair: Option<impl for<'call> FnMut(&RSIndexResult<'call>, &RepairContext<'call>)>,
    ) -> std::io::Result<Option<GcScanDelta>> {
        InvertedIndex::scan_gc(self, doc_exist, repair)
    }

    fn apply_gc(&mut self, delta: GcScanDelta) -> GcApplyInfo {
        InvertedIndex::apply_gc(self, delta)
    }
}
