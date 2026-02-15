/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{
    DecodedBy, Encoder, GcApplyInfo, GcScanDelta, IndexBlock, InvertedIndex, RSIndexResult,
    debug::{BlockSummary, Summary},
    reader::IndexReaderCore,
};
use ffi::{IndexFlags, t_docId};

/// A wrapper around the inverted index to track the total number of entries in the index.
/// Unlike [`InvertedIndex::unique_docs()`], this counts all entries, including duplicates.
#[derive(Debug)]
pub struct EntriesTrackingIndex<E> {
    /// The underlying inverted index that stores the entries.
    index: InvertedIndex<E>,

    /// The total number of entries in the index. This is not the number of unique documents, but
    /// rather the total number of entries added to the index.
    number_of_entries: usize,
}

impl<E: Encoder> EntriesTrackingIndex<E> {
    /// Create a new entries tracking index with the given encoder.
    pub fn new(flags: IndexFlags) -> Self {
        Self {
            index: InvertedIndex::new(flags),
            number_of_entries: 0,
        }
    }

    /// Add a new record to the index and return by how much memory grew. It is expected that
    /// the document ID of the record is greater than or equal the last document ID in the index.
    ///
    /// The total number of entries in the index is incremented by one.
    pub fn add_record(&mut self, record: &RSIndexResult) -> std::io::Result<usize> {
        let mem_growth = self.index.add_record(record)?;

        self.number_of_entries += 1;

        Ok(mem_growth)
    }

    /// The memory size of the index in bytes.
    pub fn memory_usage(&self) -> usize {
        self.index.memory_usage() + std::mem::size_of::<usize>()
    }

    /// The total number of entries in the index. This is not the number of unique documents, but
    /// rather the total number of entries added to the index.
    pub const fn number_of_entries(&self) -> usize {
        self.number_of_entries
    }

    /// Returns the last document ID in the index, if any.
    pub fn last_doc_id(&self) -> Option<t_docId> {
        self.index.last_doc_id()
    }

    /// Returns the number of unique documents in the index.
    pub const fn unique_docs(&self) -> u32 {
        self.index.unique_docs()
    }

    /// Returns the flags of this index.
    pub const fn flags(&self) -> IndexFlags {
        self.index.flags()
    }

    /// Return the debug summary for this inverted index.
    pub fn summary(&self) -> Summary {
        let mut summary = self.index.summary();

        summary.number_of_entries = self.number_of_entries;
        summary.has_efficiency = true;

        if summary.number_of_blocks > 0 {
            summary.block_efficiency =
                summary.number_of_entries as f64 / summary.number_of_blocks as f64
        }

        summary
    }

    /// Return basic information about the blocks in this inverted index.
    pub fn blocks_summary(&self) -> Vec<BlockSummary> {
        self.index.blocks_summary()
    }

    /// Returns the number of blocks in this index.
    pub fn number_of_blocks(&self) -> usize {
        self.index.number_of_blocks()
    }

    /// Get a reference to the block at the given index, if it exists. This is only used by some C tests.
    pub fn block_ref(&self, index: usize) -> Option<&IndexBlock> {
        self.index.block_ref(index)
    }

    /// Get the current GC marker of this index. This is only used by the some C tests.
    pub fn gc_marker(&self) -> u32 {
        self.index.gc_marker()
    }

    /// Increment the GC marker of this index. This is only used by the some C tests.
    pub fn gc_marker_inc(&self) {
        self.index.gc_marker_inc();
    }

    /// Get a reference to the inner inverted index.
    pub const fn inner(&self) -> &InvertedIndex<E> {
        &self.index
    }

    /// Get a mutable reference to the inner inverted index.
    pub const fn inner_mut(&mut self) -> &mut InvertedIndex<E> {
        &mut self.index
    }
}

impl<E: Encoder + DecodedBy> EntriesTrackingIndex<E> {
    /// Create a new [`crate::reader::IndexReader`] for this inverted index.
    pub fn reader(&self) -> IndexReaderCore<'_, E> {
        self.index.reader()
    }

    /// Scan the index for blocks that can be garbage collected. A block can be garbage collected
    /// if any of its records point to documents that no longer exist. The `doc_exist`
    /// callback is used to check if a document exists. It should return `true` if the document
    /// exists and `false` otherwise.
    ///
    /// If a doc does exist, then `repair` is called with it to run any repair calculations needed.
    ///
    /// This function returns a delta if GC is needed, or `None` if no GC is needed.
    pub fn scan_gc<'index>(
        &'index self,
        doc_exist: impl Fn(t_docId) -> bool,
        repair: Option<impl FnMut(&RSIndexResult<'index>, &IndexBlock)>,
    ) -> std::io::Result<Option<GcScanDelta>> {
        self.index.scan_gc(doc_exist, repair)
    }

    /// Apply the deltas of a garbage collection scan to the index. This will modify the index
    /// by deleting or repairing blocks as needed.
    pub fn apply_gc(&mut self, delta: GcScanDelta) -> GcApplyInfo {
        let info = self.index.apply_gc(delta);

        self.number_of_entries -= info.entries_removed;

        info
    }
}
