/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::{
    DecodedBy, Encoder, FilterMaskReader, GcApplyInfo, GcScanDelta, IndexBlock, InvertedIndex,
    RSIndexResult,
    debug::{BlockSummary, Summary},
    reader::IndexReaderCore,
};
use ffi::{IndexFlags, IndexFlags_Index_StoreFieldFlags, t_docId, t_fieldMask};

/// A wrapper around the inverted index which tracks the fields for all the records in the index
/// using a mask. This makes is easy to know if the index has any records for a specific field.
#[derive(Debug)]
pub struct FieldMaskTrackingIndex<E> {
    /// The underlying inverted index that stores the records.
    index: InvertedIndex<E>,

    /// A field mask of all the entries in the index. This is used to quickly determine if a
    /// record with a specific field mask exists in the index.
    field_mask: t_fieldMask,
}

impl<E: Encoder> FieldMaskTrackingIndex<E> {
    /// Create a new field mask tracking index with the given encoder.
    pub fn new(flags: IndexFlags) -> Self {
        debug_assert!(
            flags & IndexFlags_Index_StoreFieldFlags > 1,
            "FieldMaskTrackingIndex should only be used with indices that store field flags"
        );

        Self {
            index: InvertedIndex::new(flags),
            field_mask: 0,
        }
    }

    /// Add a new record to the index and return by how much memory grew. It is expected that
    /// the document ID of the record is greater than or equal the last document ID in the index.
    pub fn add_record(&mut self, record: &RSIndexResult) -> std::io::Result<usize> {
        let mem_growth = self.index.add_record(record)?;

        self.field_mask |= record.field_mask;

        Ok(mem_growth)
    }

    /// The memory size of the index in bytes.
    pub fn memory_usage(&self) -> usize {
        self.index.memory_usage() + std::mem::size_of::<t_fieldMask>()
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

    /// Get the combined field mask of all records in the index.
    pub const fn field_mask(&self) -> t_fieldMask {
        self.field_mask
    }

    /// Return the debug summary for this inverted index.
    pub fn summary(&self) -> Summary {
        self.index.summary()
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
    #[cfg(feature = "test_utils")]
    pub const fn inner_mut(&mut self) -> &mut InvertedIndex<E> {
        &mut self.index
    }
}

impl<E: Encoder + DecodedBy> FieldMaskTrackingIndex<E> {
    /// Create a new [`crate::reader::IndexReader`] for this inverted index.
    pub fn reader(&self, mask: t_fieldMask) -> FilterMaskReader<IndexReaderCore<'_, E>> {
        FilterMaskReader::new(mask, self.index.reader())
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
        self.index.apply_gc(delta)
    }
}
