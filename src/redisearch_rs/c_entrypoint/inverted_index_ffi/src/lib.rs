/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! This module contains the inverted index implementation for the RediSearch module.
#![allow(non_upper_case_globals)]

mod fork_gc;

use std::{
    ffi::{c_char, c_void},
    fmt::Debug,
};

use ffi::{
    DocTable_Exists, IndexFlags, IndexFlags_Index_DocIdsOnly, IndexFlags_Index_StoreFieldFlags,
    IndexFlags_Index_StoreFreqs, IndexFlags_Index_StoreNumeric, IndexFlags_Index_StoreTermOffsets,
    IndexFlags_Index_WideSchema, RedisSearchCtx, t_docId, t_fieldMask,
};

use fork_gc::{InvertedIndexGCCallback, InvertedIndexGCReader, InvertedIndexGCWriter};
use inverted_index::{
    EntriesTrackingIndex, FieldMaskTrackingIndex, FilterGeoReader, FilterMaskReader,
    FilterNumericReader, GcApplyInfo, GcScanDelta, IndexBlock, IndexReader as _, NumericFilter,
    RSIndexResult, ReadFilter,
    debug::{BlockSummary, Summary},
    doc_ids_only::DocIdsOnly,
    fields_offsets::{FieldsOffsets, FieldsOffsetsWide},
    fields_only::{FieldsOnly, FieldsOnlyWide},
    freqs_fields::{FreqsFields, FreqsFieldsWide},
    freqs_offsets::FreqsOffsets,
    freqs_only::FreqsOnly,
    full::{Full, FullWide},
    numeric::{Numeric, NumericFloatCompression},
    offsets_only::OffsetsOnly,
    raw_doc_ids_only::RawDocIdsOnly,
};
use serde::{Deserialize, Serialize};

/// Get the total number of index blocks allocated across all inverted index instances.
#[unsafe(no_mangle)]
pub extern "C" fn TotalIIBlocks() -> usize {
    IndexBlock::total_blocks()
}

/// An opaque inverted index structure. The actual implementation is determined at runtime based on
/// the index flags provided when creating the index. This allows us to have a single interface for
/// all index types while still being able to optimize the storage and performance for each index
/// type.
pub enum InvertedIndex {
    // Needs to track the field masks because it has the `StoreFieldFlags` flag set
    Full(FieldMaskTrackingIndex<Full>),
    // Needs to track the field masks because it has the `StoreFieldFlags` flag set
    FullWide(FieldMaskTrackingIndex<FullWide>),
    // Needs to track the field masks because it has the `StoreFieldFlags` flag set
    FreqsFields(FieldMaskTrackingIndex<FreqsFields>),
    // Needs to track the field masks because it has the `StoreFieldFlags` flag set
    FreqsFieldsWide(FieldMaskTrackingIndex<FreqsFieldsWide>),
    FreqsOnly(inverted_index::InvertedIndex<FreqsOnly>),
    // Needs to track the field masks because it has the `StoreFieldFlags` flag set
    FieldsOnly(FieldMaskTrackingIndex<FieldsOnly>),
    // Needs to track the field masks because it has the `StoreFieldFlags` flag set
    FieldsOnlyWide(FieldMaskTrackingIndex<FieldsOnlyWide>),
    // Needs to track the field masks because it has the `StoreFieldFlags` flag set
    FieldsOffsets(FieldMaskTrackingIndex<FieldsOffsets>),
    // Needs to track the field masks because it has the `StoreFieldFlags` flag set
    FieldsOffsetsWide(FieldMaskTrackingIndex<FieldsOffsetsWide>),
    OffsetsOnly(inverted_index::InvertedIndex<OffsetsOnly>),
    FreqsOffsets(inverted_index::InvertedIndex<FreqsOffsets>),
    DocumentIdOnly(inverted_index::InvertedIndex<DocIdsOnly>),
    RawDocumentIdOnly(inverted_index::InvertedIndex<RawDocIdsOnly>),
    // Needs to track the entries count because it has the `StoreNumeric` flag set
    Numeric(EntriesTrackingIndex<Numeric>),
    NumericFloatCompression(EntriesTrackingIndex<NumericFloatCompression>),
}

impl Debug for InvertedIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full(_) => f.debug_tuple("Full").finish(),
            Self::FullWide(_) => f.debug_tuple("FullWide").finish(),
            Self::FreqsFields(_) => f.debug_tuple("FreqsFields").finish(),
            Self::FreqsFieldsWide(_) => f.debug_tuple("FreqsFieldsWide").finish(),
            Self::FreqsOnly(_) => f.debug_tuple("FreqsOnly").finish(),
            Self::FieldsOnly(_) => f.debug_tuple("FieldsOnly").finish(),
            Self::FieldsOnlyWide(_) => f.debug_tuple("FieldsOnlyWide").finish(),
            Self::FieldsOffsets(_) => f.debug_tuple("FieldsOffsets").finish(),
            Self::FieldsOffsetsWide(_) => f.debug_tuple("FieldsOffsetsWide").finish(),
            Self::OffsetsOnly(_) => f.debug_tuple("OffsetsOnly").finish(),
            Self::FreqsOffsets(_) => f.debug_tuple("FreqsOffsets").finish(),
            Self::DocumentIdOnly(_) => f.debug_tuple("DocumentIdOnly").finish(),
            Self::RawDocumentIdOnly(_) => f.debug_tuple("RawDocumentIdOnly").finish(),
            Self::Numeric(_) => f.debug_tuple("Numeric").finish(),
            Self::NumericFloatCompression(_) => f.debug_tuple("NumericFloatCompression").finish(),
        }
    }
}

// Macro to make calling the methods on the inner index easier
macro_rules! ii_dispatch {
    ($self:expr, $method:ident $(, $args:expr)*) => {
        match $self {
            InvertedIndex::Full(ii) => ii.$method($($args),*),
            InvertedIndex::FullWide(ii) => ii.$method($($args),*),
            InvertedIndex::FreqsFields(ii) => ii.$method($($args),*),
            InvertedIndex::FreqsFieldsWide(ii) => ii.$method($($args),*),
            InvertedIndex::FreqsOnly(ii) => ii.$method($($args),*),
            InvertedIndex::FieldsOnly(ii) => ii.$method($($args),*),
            InvertedIndex::FieldsOnlyWide(ii) => ii.$method($($args),*),
            InvertedIndex::FieldsOffsets(ii) => ii.$method($($args),*),
            InvertedIndex::FieldsOffsetsWide(ii) => ii.$method($($args),*),
            InvertedIndex::OffsetsOnly(ii) => ii.$method($($args),*),
            InvertedIndex::FreqsOffsets(ii) => ii.$method($($args),*),
            InvertedIndex::DocumentIdOnly(ii) => ii.$method($($args),*),
            InvertedIndex::RawDocumentIdOnly(ii) => ii.$method($($args),*),
            InvertedIndex::Numeric(ii) => ii.$method($($args),*),
            InvertedIndex::NumericFloatCompression(ii) => ii.$method($($args),*),
        }
    };
}

/// The mask of flags that determine the index storage type. This includes all flags that affect
/// the storage format of the index.
const INDEX_STORAGE_MASK: IndexFlags = IndexFlags_Index_StoreFreqs
    | IndexFlags_Index_StoreFieldFlags
    | IndexFlags_Index_StoreTermOffsets
    | IndexFlags_Index_StoreNumeric
    | IndexFlags_Index_WideSchema
    | IndexFlags_Index_DocIdsOnly;

const FULL_MASK: IndexFlags = IndexFlags_Index_StoreFreqs
    | IndexFlags_Index_StoreTermOffsets
    | IndexFlags_Index_StoreFieldFlags;
const FULL_WIDE_MASK: IndexFlags = IndexFlags_Index_StoreFreqs
    | IndexFlags_Index_StoreTermOffsets
    | IndexFlags_Index_StoreFieldFlags
    | IndexFlags_Index_WideSchema;
const FREQS_FIELDS_MASK: IndexFlags =
    IndexFlags_Index_StoreFreqs | IndexFlags_Index_StoreFieldFlags;
const FREQS_FIELDS_WIDE_MASK: IndexFlags =
    IndexFlags_Index_StoreFreqs | IndexFlags_Index_StoreFieldFlags | IndexFlags_Index_WideSchema;
const FREQS_ONLY_MASK: IndexFlags = IndexFlags_Index_StoreFreqs;
const FIELDS_ONLY_MASK: IndexFlags = IndexFlags_Index_StoreFieldFlags;
const FIELDS_ONLY_WIDE_MASK: IndexFlags =
    IndexFlags_Index_StoreFieldFlags | IndexFlags_Index_WideSchema;
const FIELDS_OFFSETS_MASK: IndexFlags =
    IndexFlags_Index_StoreFieldFlags | IndexFlags_Index_StoreTermOffsets;
const FIELDS_OFFSETS_WIDE_MASK: IndexFlags = IndexFlags_Index_StoreFieldFlags
    | IndexFlags_Index_StoreTermOffsets
    | IndexFlags_Index_WideSchema;
const OFFSETS_ONLY_MASK: IndexFlags = IndexFlags_Index_StoreTermOffsets;
const FREQS_OFFSETS_MASK: IndexFlags =
    IndexFlags_Index_StoreFreqs | IndexFlags_Index_StoreTermOffsets;
const DOC_IDS_ONLY_MASK: IndexFlags = IndexFlags_Index_DocIdsOnly;
const NUMERIC_MASK: IndexFlags = IndexFlags_Index_StoreNumeric;

/// Create a new inverted index instance based on the provided flags and options. `raw_doc_encoding`
/// controls whether document IDs only encoding should use raw encoding (true) or varint encoding
/// (false). `compress_floats` controls whether numeric encoding should have its floating point
/// numbers compressed (true) or not (false). Compressing floating point numbers saves memory
/// but lowers precision.
///
/// The output parameter `mem_size` will be set to the memory usage of the created index. The
/// inverted index should be freed using [`InvertedIndex_Free`] when no longer needed.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `mem_size` must be a valid pointer to a `usize`.
///
/// # Panics
/// This function will panic if the provided flags does not set at least one of the following
/// storage flags:
/// - `StoreFreqs`
/// - `StoreFieldFlags`
/// - `StoreTermOffsets`
/// - `StoreNumeric`
/// - `DocIdsOnly`
#[unsafe(no_mangle)]
pub extern "C" fn NewInvertedIndex_Ex(
    flags: IndexFlags,
    raw_doc_id_encoding: bool,
    compress_floats: bool,
    mem_size: &mut usize,
) -> *mut InvertedIndex {
    let ii = match (
        flags & INDEX_STORAGE_MASK,
        raw_doc_id_encoding,
        compress_floats,
    ) {
        (FULL_MASK, _, _) => InvertedIndex::Full(FieldMaskTrackingIndex::<Full>::new(flags)),
        (FULL_WIDE_MASK, _, _) => {
            InvertedIndex::FullWide(FieldMaskTrackingIndex::<FullWide>::new(flags))
        }
        (FREQS_FIELDS_MASK, _, _) => {
            InvertedIndex::FreqsFields(FieldMaskTrackingIndex::<FreqsFields>::new(flags))
        }
        (FREQS_FIELDS_WIDE_MASK, _, _) => {
            InvertedIndex::FreqsFieldsWide(FieldMaskTrackingIndex::<FreqsFieldsWide>::new(flags))
        }
        (FREQS_ONLY_MASK, _, _) => {
            InvertedIndex::FreqsOnly(inverted_index::InvertedIndex::<FreqsOnly>::new(flags))
        }
        (FIELDS_ONLY_MASK, _, _) => {
            InvertedIndex::FieldsOnly(FieldMaskTrackingIndex::<FieldsOnly>::new(flags))
        }
        (FIELDS_ONLY_WIDE_MASK, _, _) => {
            InvertedIndex::FieldsOnlyWide(FieldMaskTrackingIndex::<FieldsOnlyWide>::new(flags))
        }
        (FIELDS_OFFSETS_MASK, _, _) => {
            InvertedIndex::FieldsOffsets(FieldMaskTrackingIndex::<FieldsOffsets>::new(flags))
        }
        (FIELDS_OFFSETS_WIDE_MASK, _, _) => InvertedIndex::FieldsOffsetsWide(
            FieldMaskTrackingIndex::<FieldsOffsetsWide>::new(flags),
        ),
        (OFFSETS_ONLY_MASK, _, _) => {
            InvertedIndex::OffsetsOnly(inverted_index::InvertedIndex::<OffsetsOnly>::new(flags))
        }
        (FREQS_OFFSETS_MASK, _, _) => {
            InvertedIndex::FreqsOffsets(inverted_index::InvertedIndex::<FreqsOffsets>::new(flags))
        }
        (DOC_IDS_ONLY_MASK, false, _) => {
            InvertedIndex::DocumentIdOnly(inverted_index::InvertedIndex::<DocIdsOnly>::new(flags))
        }
        (DOC_IDS_ONLY_MASK, true, _) => InvertedIndex::RawDocumentIdOnly(
            inverted_index::InvertedIndex::<RawDocIdsOnly>::new(flags),
        ),
        (NUMERIC_MASK, _, false) => {
            InvertedIndex::Numeric(EntriesTrackingIndex::<Numeric>::new(flags))
        }
        (NUMERIC_MASK, _, true) => InvertedIndex::NumericFloatCompression(EntriesTrackingIndex::<
            NumericFloatCompression,
        >::new(flags)),
        // We generally don't panic in Rust code and would have a match were we cover all the cases.
        // However, the `flags` value stores more than just the storage flags and it is not clear
        // that the C code won't call this function without any of the storage flags set.
        //
        _ => panic!("Unsupported index flags: {flags:?}"),
    };

    *mem_size = ii_dispatch!(&ii, memory_usage);

    let ii_boxed = Box::new(ii);
    Box::into_raw(ii_boxed)
}

/// Free the memory associated with an inverted index instance created using [`NewInvertedIndex_Ex`].
///
/// # Safety
/// The following invariant must be upheld when calling this function:
/// - `ii` must be a valid, non NULL, pointer to an `InvertedIndex` instance created using
///   [`NewInvertedIndex_Ex`] or `NewInvertedIndex`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndex_Free(ii: *mut InvertedIndex) {
    debug_assert!(!ii.is_null(), "ii must not be null");

    // SAFETY: The caller must ensure that `ii` is a valid pointer to an `InvertedIndex`
    let _ = unsafe { Box::from_raw(ii) };
}

/// Get the memory usage of the inverted index instance in bytes.
///
/// # Safety
/// The following invariant must be upheld when calling this function:
/// - `ii` must be a valid pointer to an `InvertedIndex` instance and must not be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndex_MemUsage(ii: *const InvertedIndex) -> usize {
    debug_assert!(!ii.is_null(), "ii must not be null");

    // SAFETY: The caller must ensure that `ii` is a valid pointer to an `InvertedIndex`
    let ii = unsafe { &*ii };
    ii_dispatch!(ii, memory_usage)
}

/// Write a new numeric entry to the inverted index. This is only valid for numeric indexes created
/// with the `StoreNumeric` flag. The function returns the number of bytes the memory usage of the
/// index grew by.
///
/// # Safety
/// The following invariant must be upheld when calling this function:
/// - `ii` must be a valid pointer to an `InvertedIndex` instance and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndex_WriteNumericEntry(
    ii: *mut InvertedIndex,
    doc_id: t_docId,
    value: f64,
) -> usize {
    debug_assert!(!ii.is_null(), "ii must not be null");

    let record = RSIndexResult::numeric(value).doc_id(doc_id);

    // SAFETY: The caller must ensure that `ii` is a valid pointer to an `InvertedIndex`
    let ii = unsafe { &mut *ii };
    ii_dispatch!(ii, add_record, &record).unwrap()
}

/// Write a new entry to the inverted index. The function returns the number of bytes the memory
/// usage of the index grew by.
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `ii` must be a valid pointer to an `InvertedIndex` instance and cannot be NULL.
/// - `record` must be a valid pointer to an `RSIndexResult` instance and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndex_WriteEntryGeneric(
    ii: *mut InvertedIndex,
    record: *const RSIndexResult,
) -> usize {
    debug_assert!(!ii.is_null(), "ii must not be null");
    debug_assert!(!record.is_null(), "record must not be null");

    // SAFETY: The caller must ensure that `ii` is a valid pointer to an `InvertedIndex`
    let ii = unsafe { &mut *ii };

    // SAFETY: The caller must ensure that `record` is a valid pointer to an `RSIndexResult`
    let record = unsafe { &*record };

    ii_dispatch!(ii, add_record, record).unwrap()
}

/// Return the number of blocks in the inverted index.
///
/// # Safety
/// The following invariant must be upheld when calling this function:
/// - `ii` must be a valid pointer to an `InvertedIndex` instance and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndex_NumBlocks(ii: *const InvertedIndex) -> usize {
    debug_assert!(!ii.is_null(), "ii must not be null");

    // SAFETY: The caller must ensure that `ii` is a valid pointer to an `InvertedIndex`
    let ii = unsafe { &*ii };
    ii_dispatch!(ii, number_of_blocks)
}

/// Get the flags used to create the inverted index.
///
/// # Safety
/// The following invariant must be upheld when calling this function:
/// - `ii` must be a valid pointer to an `InvertedIndex` instance and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndex_Flags(ii: *const InvertedIndex) -> IndexFlags {
    debug_assert!(!ii.is_null(), "ii must not be null");

    // SAFETY: The caller must ensure that `ii` is a valid pointer to an `InvertedIndex`
    let ii = unsafe { &*ii };
    ii_dispatch!(ii, flags)
}

/// Get the number of unique documents in the inverted index.
///
/// # Safety
/// The following invariant must be upheld when calling this function:
/// - `ii` must be a valid pointer to an `InvertedIndex` instance and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndex_NumDocs(ii: *const InvertedIndex) -> u32 {
    debug_assert!(!ii.is_null(), "ii must not be null");

    // SAFETY: The caller must ensure that `ii` is a valid pointer to an `InvertedIndex`
    let ii = unsafe { &*ii };
    ii_dispatch!(ii, unique_docs)
}

/// Get a summary of the inverted index for debugging purposes.
///
/// # Safety
/// The following invariant must be upheld when calling this function:
/// - `ii` must be a valid pointer to an `InvertedIndex` instance and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndex_Summary(ii: *const InvertedIndex) -> Summary {
    debug_assert!(!ii.is_null(), "ii must not be null");

    // SAFETY: The caller must ensure that `ii` is a valid pointer to an `InvertedIndex`
    let ii = unsafe { &*ii };
    ii_dispatch!(ii, summary)
}

/// Get an array of summaries of all blocks in the inverted index. The output parameter `count` will
/// be set to the number of blocks in the index. The returned pointer must be freed using
/// [`InvertedIndex_BlocksSummaryFree`].
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `ii` must be a valid pointer to an `InvertedIndex` instance and cannot be NULL.
/// - `count` must be a valid pointer to a `usize` and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndex_BlocksSummary(
    ii: *const InvertedIndex,
    count: *mut usize,
) -> *mut BlockSummary {
    debug_assert!(!ii.is_null(), "ii must not be null");
    debug_assert!(!count.is_null(), "count must not be null");

    // SAFETY: The caller must ensure that `ii` is a valid pointer to an `InvertedIndex`
    let ii = unsafe { &*ii };

    let blocks_summary = ii_dispatch!(ii, blocks_summary);

    // SAFETY: The caller must ensure that `count` is a valid pointer to a `usize`
    unsafe {
        *count = blocks_summary.len();
    }

    Box::leak(blocks_summary.into_boxed_slice()).as_mut_ptr()
}

/// Free the memory associated with the array of block summaries returned by [`InvertedIndex_BlocksSummary`].
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `blocks` must be a valid pointer to an array of `BlockSummary` instances returned by
///   [`InvertedIndex_BlocksSummary`].
/// - `count` must have the same value as the `count` output parameter passed to
///   [`InvertedIndex_BlocksSummary`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndex_BlocksSummaryFree(blocks: *mut BlockSummary, count: usize) {
    debug_assert!(!blocks.is_null(), "blocks must not be null");

    // SAFETY: The caller must ensure that `blocks` is a valid pointer to an array of `BlockSummary`
    // and that `count` is the correct length of the array
    let blocks = unsafe { std::slice::from_raw_parts_mut(blocks, count) };

    // SAFETY: We can safely convert the slice back to a boxed slice and drop it to free the memory
    let _ = unsafe { Box::from_raw(blocks) };
}

/// Get the field mask used in the inverted index. This is only valid for indexes created with the
/// `StoreFieldFlags` flag. For other index types, this function will return 0.
///
/// # Safety
/// The following invariant must be upheld when calling this function:
/// - `ii` must be a valid pointer to an `InvertedIndex` instance and cannot be NULL.
#[allow(improper_ctypes_definitions)] // `t_fieldMask` is type `u128`
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndex_FieldMask(ii: *const InvertedIndex) -> t_fieldMask {
    debug_assert!(!ii.is_null(), "ii must not be null");

    // SAFETY: The caller must ensure that `ii` is a valid pointer to an `InvertedIndex`
    let ii = unsafe { &*ii };

    match ii {
        InvertedIndex::Full(ii) => ii.field_mask(),
        InvertedIndex::FullWide(ii) => ii.field_mask(),
        InvertedIndex::FreqsFields(ii) => ii.field_mask(),
        InvertedIndex::FreqsFieldsWide(ii) => ii.field_mask(),
        InvertedIndex::FieldsOnly(ii) => ii.field_mask(),
        InvertedIndex::FieldsOnlyWide(ii) => ii.field_mask(),
        InvertedIndex::FieldsOffsets(ii) => ii.field_mask(),
        InvertedIndex::FieldsOffsetsWide(ii) => ii.field_mask(),
        InvertedIndex::FreqsOnly(_)
        | InvertedIndex::OffsetsOnly(_)
        | InvertedIndex::FreqsOffsets(_)
        | InvertedIndex::DocumentIdOnly(_)
        | InvertedIndex::RawDocumentIdOnly(_)
        | InvertedIndex::Numeric(_)
        | InvertedIndex::NumericFloatCompression(_) => 0,
    }
}

/// Get the number of entries in the inverted index. This is only valid for numeric indexes created
/// with the `StoreNumeric` flag. For other index types, this function will return 0.
///
/// # Safety
/// The following invariant must be upheld when calling this function:
/// - `ii` must be a valid pointer to an `InvertedIndex` instance and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndex_NumEntries(ii: *const InvertedIndex) -> usize {
    debug_assert!(!ii.is_null(), "ii must not be null");

    // SAFETY: The caller must ensure that `ii` is a valid pointer to an `InvertedIndex`
    let ii = unsafe { &*ii };

    match ii {
        InvertedIndex::Numeric(ii) => ii.number_of_entries(),
        InvertedIndex::NumericFloatCompression(ii) => ii.number_of_entries(),
        InvertedIndex::Full(_)
        | InvertedIndex::FullWide(_)
        | InvertedIndex::FreqsFields(_)
        | InvertedIndex::FreqsFieldsWide(_)
        | InvertedIndex::FreqsOnly(_)
        | InvertedIndex::FieldsOnly(_)
        | InvertedIndex::FieldsOnlyWide(_)
        | InvertedIndex::FieldsOffsets(_)
        | InvertedIndex::FieldsOffsetsWide(_)
        | InvertedIndex::OffsetsOnly(_)
        | InvertedIndex::FreqsOffsets(_)
        | InvertedIndex::DocumentIdOnly(_)
        | InvertedIndex::RawDocumentIdOnly(_) => 0,
    }
}

/// Get a reference to the block at the specified index. Returns NULL if the index is out of bounds.
/// This is used by some C tests.
///
/// # Safety
/// The following invariant must be upheld when calling this function:
/// - `ii` must be a valid pointer to an `InvertedIndex` instance and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndex_BlockRef<'index>(
    ii: *const InvertedIndex,
    block_idx: usize,
) -> Option<&'index IndexBlock> {
    debug_assert!(!ii.is_null(), "ii must not be null");

    // SAFETY: The caller must ensure that `ii` is a valid pointer to an `InvertedIndex`
    let ii: &'index _ = unsafe { &*ii };
    ii_dispatch!(ii, block_ref, block_idx)
}

/// Get ID of the last document in the index. Returns 0 if the index is empty.
/// This is used by some C tests.
///
/// # Safety
/// The following invariant must be upheld when calling this function:
/// - `ii` must be a valid pointer to an `InvertedIndex` instance and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndex_LastId(ii: *const InvertedIndex) -> t_docId {
    debug_assert!(!ii.is_null(), "ii must not be null");

    // SAFETY: The caller must ensure that `ii` is a valid pointer to an `InvertedIndex`
    let ii = unsafe { &*ii };
    ii_dispatch!(ii, last_doc_id).unwrap_or(0)
}

/// Get the garbage collector marker of the inverted index. This is used by some C tests.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `ii` must be a valid, non NULL, pointer to an `InvertedIndex` instance.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndex_GcMarker(ii: *const InvertedIndex) -> u32 {
    debug_assert!(!ii.is_null(), "ii must not be null");

    // SAFETY: The caller must ensure that `ii` is a valid pointer to an `InvertedIndex`
    let ii = unsafe { &*ii };

    ii_dispatch!(ii, gc_marker)
}

/// Increment the garbage collector marker of the inverted index. This is used by some C tests.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `ii` must be a valid, non NULL, pointer to an `InvertedIndex` instance.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndex_GcMarkerInc(ii: *mut InvertedIndex) {
    debug_assert!(!ii.is_null(), "ii must not be null");

    // SAFETY: The caller must ensure that `ii` is a valid pointer to an `InvertedIndex`
    let ii = unsafe { &*ii };

    ii_dispatch!(ii, gc_marker_inc);
}

/// Setting to pass to the GC scan function
#[repr(C)]
pub struct IndexRepairParams {
    /// Callback to call for each entry that is still valid
    pub repair_callback:
        Option<extern "C" fn(res: *const RSIndexResult, ib: *const IndexBlock, *mut c_void)>,

    /// Argument to pass to the repair callback
    pub repair_arg: *mut c_void,
}

/// Scan the inverted index for garbage and write the GC delta to the provided writer. The function
/// returns true if the scan was successful and false otherwise.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `wr` must be a valid, non NULL, pointer to an `InvertedIndexGCWriter` instance.
/// - `sctx` must be a valid, non NULL, pointer to a `RedisSearchCtx` instance.
/// - `idx` must be a valid, non NULL, pointer to an `InvertedIndex` instance.
/// - `cb` must be a valid, non NULL, pointer to an `InvertedIndexGCCallback` instance.
/// - `params` must be a valid, NULLable, pointer to an `IndexRepairParams` instance.
/// - The `spec` field of the `RedisSearchCtx` must be a valid, non NULL, pointer to an
///   `IndexSpec` instance.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndex_GcDelta_Scan(
    wr: *mut InvertedIndexGCWriter,
    sctx: *mut RedisSearchCtx,
    idx: *mut InvertedIndex,
    cb: *mut InvertedIndexGCCallback,
    params: *mut IndexRepairParams,
) -> bool {
    debug_assert!(!sctx.is_null(), "sctx must not be null");
    debug_assert!(!idx.is_null(), "idx must not be null");
    debug_assert!(!cb.is_null(), "cb must not be null");
    debug_assert!(!wr.is_null(), "wr must not be null");

    // SAFETY: The caller must ensure `sctx` is a valid pointer to a `RedisSearchCtx`
    let sctx = unsafe { &*sctx };

    debug_assert!(!sctx.spec.is_null(), "sctx.spec must not be null");

    // SAFETY: The caller must ensure the `spec` field of the `RedisSearchCtx` is a valid
    // pointer to an `IndexSpec`
    let spec = unsafe { &*sctx.spec };
    let doc_table = spec.docs;

    // SAFETY: We know `doc_table` is a valid `DocTable` because it just got it off the spec
    let doc_exists = |id| unsafe { DocTable_Exists(&doc_table, id) };

    let repair = if params.is_null() {
        None
    } else {
        // SAFETY: The caller must ensure `params` is a valid pointer to a `IndexRepairParams` and
        // we just checked it is not NULL
        let params = unsafe { &*params };
        params
            .repair_callback
            .map(|cb| move |res: &RSIndexResult, ib: &IndexBlock| cb(res, ib, params.repair_arg))
    };

    // SAFETY: The caller must ensure `idx` is a valid pointer to an `InvertedIndex`
    let ii = unsafe { &*idx };

    let Ok(deltas) = ii_dispatch!(ii, scan_gc, doc_exists, repair) else {
        return false;
    };

    let Some(deltas) = deltas else {
        return false;
    };

    // SAFETY: The caller must ensure `cb` is a valid pointer to an `InvertedIndexGCCallback`
    let cb = unsafe { &*cb };
    let cb_call = cb.call;
    cb_call(cb.ctx);

    // SAFETY: The caller must ensure `wr` is a valid pointer to a `InvertedIndexGCWriter`
    let wr = unsafe { &mut *wr };

    deltas
        .serialize(&mut rmp_serde::Serializer::new(wr))
        .is_ok()
}

/// Read a GC delta from the provided reader. The returned pointer must be freed using
/// [`InvertedIndex_GcDelta_Free`] or should be passed to [`InvertedIndex_ApplyGcDelta`].
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `rd` must be a valid, non NULL, pointer to an `InvertedIndexGCReader` instance.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndex_GcDelta_Read(
    rd: *mut InvertedIndexGCReader,
) -> *mut GcScanDelta {
    debug_assert!(!rd.is_null(), "rd must not be null");

    // SAFETY: The caller must ensure `rd` is a valid pointer to a `InvertedIndexGCReader`
    let rt = unsafe { &mut *rd };

    let deltas = GcScanDelta::deserialize(&mut rmp_serde::Deserializer::new(rt)).unwrap();

    let deltas = Box::new(deltas);

    Box::into_raw(deltas)
}

/// Free the memory associated with a GC delta instance created using [`InvertedIndex_GcDelta_Read`].
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `deltas` must be a valid, non NULL, pointer to a `GcScanDelta` instance created using
///   [`InvertedIndex_GcDelta_Read`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndex_GcDelta_Free(deltas: *mut GcScanDelta) {
    debug_assert!(!deltas.is_null(), "deltas must not be null");

    // SAFETY: The caller must ensure that `deltas` is a valid pointer to a `GcScanDelta`
    let _deltas = unsafe { Box::from_raw(deltas) };
}

/// Apply a GC delta to the inverted index. The output parameter `apply_info` will be set to
/// information about the applied delta.
///
/// This will take ownership of the `deltas` pointer and free it. Therefore, it should not be
/// used or freed after calling this function.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `ii` must be a valid, non NULL, pointer to an `InvertedIndex` instance.
/// - `deltas` must be a valid, non NULL, pointer to a `GcScanDelta` instance created using
///   [`InvertedIndex_GcDelta_Read`].
/// - `apply_info` must be a valid, non NULL, pointer to a `GcApplyInfo` instance.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndex_ApplyGcDelta(
    ii: *mut InvertedIndex,
    deltas: *mut GcScanDelta,
    apply_info: *mut GcApplyInfo,
) {
    debug_assert!(!ii.is_null(), "ii must not be null");
    debug_assert!(!deltas.is_null(), "deltas must not be null");
    debug_assert!(!apply_info.is_null(), "apply_info must not be null");

    // SAFETY: The caller must ensure that `ii` is a valid pointer to an `InvertedIndex`
    let ii = unsafe { &mut *ii };

    // SAFETY: The caller must ensure `deltas` is a valid pointer to a `GcScanDelta`
    let deltas = unsafe { Box::from_raw(deltas) };
    let deltas = *deltas;

    let info = ii_dispatch!(ii, apply_gc, deltas);

    // SAFETY: The caller must ensure `apply_info` is a valid pointer to a `GcApplyInfo`
    unsafe { *apply_info = info };
}

/// Get the index of the last block in the GC delta.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `gc_scan_delta` must be a valid, non NULL, pointer to a `GcScanDelta` instance.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GcScanDelta_LastBlockIdx(gc_scan_delta: *const GcScanDelta) -> usize {
    debug_assert!(!gc_scan_delta.is_null(), "gc_scan_delta must not be null");

    // SAFETY: The caller must ensure `gc_scan_delta` is a valid pointer to a `GcScanDelta`
    let gc_scan_delta = unsafe { &*gc_scan_delta };

    gc_scan_delta.last_block_idx()
}

/// Get ID of the first document in the index block. This is used by some C tests.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `ib` must be a valid pointer to an `IndexBlock` instance and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexBlock_FirstId(ib: *const IndexBlock) -> t_docId {
    debug_assert!(!ib.is_null(), "ib must not be null");

    // SAFETY: The caller must ensure that `ib` is a valid pointer to an `IndexBlock`
    let ib = unsafe { &*ib };

    ib.first_block_id()
}

/// Get ID of the last document in the index block. This is used by some C tests.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `ib` must be a valid pointer to an `IndexBlock` instance and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexBlock_LastId(ib: *const IndexBlock) -> t_docId {
    debug_assert!(!ib.is_null(), "ib must not be null");

    // SAFETY: The caller must ensure that `ib` is a valid pointer to an `IndexBlock`
    let ib = unsafe { &*ib };

    ib.last_block_id()
}

/// Get the number of entries in the index block. This is used by some C tests.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `ib` must be a valid pointer to an `IndexBlock` instance and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexBlock_NumEntries(ib: *const IndexBlock) -> u16 {
    debug_assert!(!ib.is_null(), "ib must not be null");

    // SAFETY: The caller must ensure that `ib` is a valid pointer to an `IndexBlock`
    let ib = unsafe { &*ib };

    ib.num_entries()
}

/// Get a pointer to the raw data of the index block. This is used by some C tests.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `ib` must be a valid pointer to an `IndexBlock` instance and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexBlock_Data(ib: *const IndexBlock) -> *const c_char {
    debug_assert!(!ib.is_null(), "ib must not be null");

    // SAFETY: The caller must ensure that `ib` is a valid pointer to an `IndexBlock`
    let ib = unsafe { &*ib };

    ib.data().as_ptr() as *const _
}

/// An opaque inverted index reader structure. The actual implementation is determined at runtime
/// based on the index type and filter provided when creating the reader. This allows us to have a
/// single interface for all index reader types while still being able to optimize the storage
/// and performance for each index reader type.
pub enum IndexReader<'index_and_filter> {
    Full(FilterMaskReader<inverted_index::IndexReaderCore<'index_and_filter, Full>>),
    FullWide(FilterMaskReader<inverted_index::IndexReaderCore<'index_and_filter, FullWide>>),
    FreqsFields(FilterMaskReader<inverted_index::IndexReaderCore<'index_and_filter, FreqsFields>>),
    FreqsFieldsWide(
        FilterMaskReader<inverted_index::IndexReaderCore<'index_and_filter, FreqsFieldsWide>>,
    ),
    FreqsOnly(inverted_index::IndexReaderCore<'index_and_filter, FreqsOnly>),
    FieldsOnly(FilterMaskReader<inverted_index::IndexReaderCore<'index_and_filter, FieldsOnly>>),
    FieldsOnlyWide(
        FilterMaskReader<inverted_index::IndexReaderCore<'index_and_filter, FieldsOnlyWide>>,
    ),
    FieldsOffsets(
        FilterMaskReader<inverted_index::IndexReaderCore<'index_and_filter, FieldsOffsets>>,
    ),
    FieldsOffsetsWide(
        FilterMaskReader<inverted_index::IndexReaderCore<'index_and_filter, FieldsOffsetsWide>>,
    ),
    OffsetsOnly(inverted_index::IndexReaderCore<'index_and_filter, OffsetsOnly>),
    FreqsOffsets(inverted_index::IndexReaderCore<'index_and_filter, FreqsOffsets>),
    DocumentIdOnly(inverted_index::IndexReaderCore<'index_and_filter, DocIdsOnly>),
    RawDocumentIdOnly(inverted_index::IndexReaderCore<'index_and_filter, RawDocIdsOnly>),
    Numeric(inverted_index::IndexReaderCore<'index_and_filter, Numeric>),
    NumericFiltered(
        FilterNumericReader<
            'index_and_filter,
            inverted_index::IndexReaderCore<'index_and_filter, Numeric>,
        >,
    ),
    NumericGeoFiltered(
        FilterGeoReader<
            'index_and_filter,
            inverted_index::IndexReaderCore<'index_and_filter, Numeric>,
        >,
    ),
    NumericFloatCompression(
        inverted_index::IndexReaderCore<'index_and_filter, NumericFloatCompression>,
    ),
    NumericFilteredFloatCompression(
        FilterNumericReader<
            'index_and_filter,
            inverted_index::IndexReaderCore<'index_and_filter, NumericFloatCompression>,
        >,
    ),
    NumericGeoFilteredFloatCompression(
        FilterGeoReader<
            'index_and_filter,
            inverted_index::IndexReaderCore<'index_and_filter, NumericFloatCompression>,
        >,
    ),
}

// Macro to make calling the methods on the inner index reader easier
macro_rules! ir_dispatch {
    ($self:expr, $method:ident $(, $args:expr)*) => {
        match $self {
            IndexReader::Full(ii) => ii.$method($($args),*),
            IndexReader::FullWide(ii) => ii.$method($($args),*),
            IndexReader::FreqsFields(ii) => ii.$method($($args),*),
            IndexReader::FreqsFieldsWide(ii) => ii.$method($($args),*),
            IndexReader::FreqsOnly(ii) => ii.$method($($args),*),
            IndexReader::FieldsOnly(ii) => ii.$method($($args),*),
            IndexReader::FieldsOnlyWide(ii) => ii.$method($($args),*),
            IndexReader::FieldsOffsets(ii) => ii.$method($($args),*),
            IndexReader::FieldsOffsetsWide(ii) => ii.$method($($args),*),
            IndexReader::OffsetsOnly(ii) => ii.$method($($args),*),
            IndexReader::FreqsOffsets(ii) => ii.$method($($args),*),
            IndexReader::DocumentIdOnly(ii) => ii.$method($($args),*),
            IndexReader::RawDocumentIdOnly(ii) => ii.$method($($args),*),
            IndexReader::Numeric(ii) => ii.$method($($args),*),
            IndexReader::NumericFiltered(ii) => ii.$method($($args),*),
            IndexReader::NumericGeoFiltered(ii) => ii.$method($($args),*),
            IndexReader::NumericFloatCompression(ii) => ii.$method($($args),*),
            IndexReader::NumericFilteredFloatCompression(ii) => ii.$method($($args),*),
            IndexReader::NumericGeoFilteredFloatCompression(ii) => ii.$method($($args),*),
        }
    };
}

/// Create a new inverted index reader for the given inverted index and filter. The returned pointer
/// must be freed using [`IndexReader_Free`] when no longer needed.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `ii` must be a valid, non NULL, pointer to an `InvertedIndex` instance.
///
/// # Panics
/// This function will panic if the provided filter is not compatible with the `InvertedIndex` type.
#[allow(improper_ctypes_definitions)] // `ctx` might contain `t_fieldMask` which is a `u128` type
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewIndexReader(
    ii: *const InvertedIndex,
    ctx: ReadFilter,
) -> *mut IndexReader {
    debug_assert!(!ii.is_null(), "ii must not be null");

    // SAFETY: The caller must ensure that `ii` is a valid pointer to an `InvertedIndex`
    let ii = unsafe { &*ii };

    let reader = match (ii, ctx) {
        (InvertedIndex::Full(ii), ReadFilter::FieldMask(mask)) => {
            IndexReader::Full(ii.reader(mask))
        }
        (InvertedIndex::FullWide(ii), ReadFilter::FieldMask(mask)) => {
            IndexReader::FullWide(ii.reader(mask))
        }
        (InvertedIndex::FreqsFields(ii), ReadFilter::FieldMask(mask)) => {
            IndexReader::FreqsFields(ii.reader(mask))
        }
        (InvertedIndex::FreqsFieldsWide(ii), ReadFilter::FieldMask(mask)) => {
            IndexReader::FreqsFieldsWide(ii.reader(mask))
        }
        (InvertedIndex::FreqsOnly(ii), _) => IndexReader::FreqsOnly(ii.reader()),
        (InvertedIndex::FieldsOnly(ii), ReadFilter::FieldMask(mask)) => {
            IndexReader::FieldsOnly(ii.reader(mask))
        }
        (InvertedIndex::FieldsOnlyWide(ii), ReadFilter::FieldMask(mask)) => {
            IndexReader::FieldsOnlyWide(ii.reader(mask))
        }
        (InvertedIndex::FieldsOffsets(ii), ReadFilter::FieldMask(mask)) => {
            IndexReader::FieldsOffsets(ii.reader(mask))
        }
        (InvertedIndex::FieldsOffsetsWide(ii), ReadFilter::FieldMask(mask)) => {
            IndexReader::FieldsOffsetsWide(ii.reader(mask))
        }
        (InvertedIndex::OffsetsOnly(ii), _) => IndexReader::OffsetsOnly(ii.reader()),
        (InvertedIndex::FreqsOffsets(ii), _) => IndexReader::FreqsOffsets(ii.reader()),
        (InvertedIndex::DocumentIdOnly(ii), _) => IndexReader::DocumentIdOnly(ii.reader()),
        (InvertedIndex::RawDocumentIdOnly(ii), _) => IndexReader::RawDocumentIdOnly(ii.reader()),
        (InvertedIndex::Numeric(ii), ReadFilter::None) => IndexReader::Numeric(ii.reader()),
        (InvertedIndex::Numeric(ii), ReadFilter::Numeric(filter)) if filter.is_numeric_filter() => {
            IndexReader::NumericFiltered(FilterNumericReader::new(filter, ii.reader()))
        }
        (InvertedIndex::Numeric(ii), ReadFilter::Numeric(filter)) => {
            IndexReader::NumericGeoFiltered(FilterGeoReader::new(filter, ii.reader()))
        }
        (InvertedIndex::NumericFloatCompression(ii), ReadFilter::None) => {
            IndexReader::NumericFloatCompression(ii.reader())
        }
        (InvertedIndex::NumericFloatCompression(ii), ReadFilter::Numeric(filter))
            if filter.is_numeric_filter() =>
        {
            IndexReader::NumericFilteredFloatCompression(FilterNumericReader::new(
                filter,
                ii.reader(),
            ))
        }
        (InvertedIndex::NumericFloatCompression(ii), ReadFilter::Numeric(filter)) => {
            IndexReader::NumericGeoFilteredFloatCompression(FilterGeoReader::new(
                filter,
                ii.reader(),
            ))
        }
        // In normal Rust we would not panic, but would rather design the type system in such a way
        // that it would be impossible to get the reader for an index with an unsupported filter.
        // But for now we still have to interface with some C code and can't have this type
        // system design yet. So it is okay to panic, but only because we are in an FFI layer.
        (index, filter) => panic!("Unsupported filter ({filter:?}) for inverted index ({index:?})"),
    };

    let reader_boxed = Box::new(reader);
    Box::into_raw(reader_boxed)
}

/// Free the memory associated with an index reader instance created using [`NewIndexReader`].
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `ir` must be a valid, non NULL, pointer to an `IndexReader` instance created using
///   [`NewIndexReader`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexReader_Free(ir: *mut IndexReader) {
    debug_assert!(!ir.is_null(), "ir must not be null");

    // SAFETY: The caller must ensure that `ir` is a valid pointer to an `IndexReader`
    let _ = unsafe { Box::from_raw(ir) };
}

/// Reset the index reader to the beginning of the index.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `ir` must be a valid, non NULL, pointer to an `IndexReader` instance.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexReader_Reset(ir: *mut IndexReader) {
    debug_assert!(!ir.is_null(), "ir must not be null");

    // SAFETY: The caller must ensure that `ir` is a valid pointer to an `IndexReader`
    let ir = unsafe { &mut *ir };

    ir_dispatch!(ir, reset);
}

/// Get the estimated number of documents in the index reader.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `ir` must be a valid, non NULL, pointer to an `IndexReader` instance.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexReader_NumEstimated(ir: *const IndexReader) -> u32 {
    debug_assert!(!ir.is_null(), "ir must not be null");

    // SAFETY: The caller must ensure that `ir` is a valid pointer to an `IndexReader`
    let ir = unsafe { &*ir };

    ir_dispatch!(ir, unique_docs)
}

/// Check if the index reader can read from the given inverted index. This is true if the index
/// reader was created for the same type of index as the given inverted index.
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `ir` must be a valid, non NULL, pointer to an `IndexReader` instance.
/// - `ii` must be a valid, non NULL, pointer to an `InvertedIndex` instance.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexReader_IsIndex(
    ir: *const IndexReader,
    ii: *const InvertedIndex,
) -> bool {
    debug_assert!(!ir.is_null(), "ir must not be null");
    debug_assert!(!ii.is_null(), "ii must not be null");

    // SAFETY: The caller must ensure that `ir` is a valid pointer to an `IndexReader`
    let ir = unsafe { &*ir };

    // SAFETY: The caller must ensure that `ii` is a valid pointer to an `InvertedIndex`
    let ii = unsafe { &*ii };

    match (ir, ii) {
        (IndexReader::Full(ir), InvertedIndex::Full(ii)) => ir.is_index(ii.inner()),
        (IndexReader::FullWide(ir), InvertedIndex::FullWide(ii)) => ir.is_index(ii.inner()),
        (IndexReader::FreqsFields(ir), InvertedIndex::FreqsFields(ii)) => ir.is_index(ii.inner()),
        (IndexReader::FreqsFieldsWide(ir), InvertedIndex::FreqsFieldsWide(ii)) => {
            ir.is_index(ii.inner())
        }
        (IndexReader::FreqsOnly(ir), InvertedIndex::FreqsOnly(ii)) => ir.is_index(ii),
        (IndexReader::FieldsOnly(ir), InvertedIndex::FieldsOnly(ii)) => ir.is_index(ii.inner()),
        (IndexReader::FieldsOnlyWide(ir), InvertedIndex::FieldsOnlyWide(ii)) => {
            ir.is_index(ii.inner())
        }
        (IndexReader::FieldsOffsets(ir), InvertedIndex::FieldsOffsets(ii)) => {
            ir.is_index(ii.inner())
        }
        (IndexReader::FieldsOffsetsWide(ir), InvertedIndex::FieldsOffsetsWide(ii)) => {
            ir.is_index(ii.inner())
        }
        (IndexReader::OffsetsOnly(ir), InvertedIndex::OffsetsOnly(ii)) => ir.is_index(ii),
        (IndexReader::FreqsOffsets(ir), InvertedIndex::FreqsOffsets(ii)) => ir.is_index(ii),
        (IndexReader::DocumentIdOnly(ir), InvertedIndex::DocumentIdOnly(ii)) => ir.is_index(ii),
        (IndexReader::RawDocumentIdOnly(ir), InvertedIndex::RawDocumentIdOnly(ii)) => {
            ir.is_index(ii)
        }
        (IndexReader::Numeric(ir), InvertedIndex::Numeric(ii)) => ir.is_index(ii.inner()),
        (IndexReader::NumericFiltered(ir), InvertedIndex::Numeric(ii)) => ir.is_index(ii.inner()),
        (IndexReader::NumericGeoFiltered(ir), InvertedIndex::Numeric(ii)) => {
            ir.is_index(ii.inner())
        }
        (IndexReader::NumericFloatCompression(ir), InvertedIndex::NumericFloatCompression(ii)) => {
            ir.is_index(ii.inner())
        }
        (
            IndexReader::NumericFilteredFloatCompression(ir),
            InvertedIndex::NumericFloatCompression(ii),
        ) => ir.is_index(ii.inner()),
        (
            IndexReader::NumericGeoFilteredFloatCompression(ir),
            InvertedIndex::NumericFloatCompression(ii),
        ) => ir.is_index(ii.inner()),
        _ => false,
    }
}

/// Advance the index reader to the next entry in the index. If there is a next entry, it will be
/// written to the output parameter `res` and the function will return true. If there are no more
/// entries, the function will return false.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `ir` must be a valid, non NULL, pointer to an `IndexReader` instance.
/// - `res` must be a valid pointer to an `RSIndexResult` instance.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexReader_Next<'index_and_filter>(
    ir: *mut IndexReader<'index_and_filter>,
    res: *mut RSIndexResult<'index_and_filter>,
) -> bool {
    debug_assert!(!ir.is_null(), "ir must not be null");
    debug_assert!(!res.is_null(), "res must not be null");

    // SAFETY: The caller must ensure that `ir` is a valid pointer to an `IndexReader`
    let ir = unsafe { &mut *ir };

    // SAFETY: The caller must ensure that `res` is a valid pointer to a `RSIndexResult`
    let res = unsafe { &mut *res };

    ir_dispatch!(ir, next_record, res).unwrap_or_default()
}

/// Skip the internal block of the inverted index reader to the block that may contain the given
/// document ID. If such a block exists, the function returns true and the next call to
/// `IndexReader_Seek` will return the entry for the given document ID or the next higher document
/// ID. If the document ID is beyond the last document in the index, the function returns false.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `ir` must be a valid, non NULL, pointer to an `IndexReader` instance.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexReader_SkipTo(ir: *mut IndexReader, doc_id: t_docId) -> bool {
    debug_assert!(!ir.is_null(), "ir must not be null");

    // SAFETY: The caller must ensure that `ir` is a valid pointer to an `IndexReader`
    let ir = unsafe { &mut *ir };

    ir_dispatch!(ir, skip_to, doc_id)
}

/// Seek the index reader to the entry with the given document ID. If such an entry exists, it will be
/// written to the output parameter `res` and the function will return true. If there is no entry
/// with the given document ID, but there are entries with higher document IDs, the next higher
/// entry will be written to `res` and the function will return true. If there are no more entries
/// with document IDs greater than or equal to the given document ID, the function will return false.
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// - `ir` must be a valid, non NULL, pointer to an `IndexReader` instance.
/// - `res` must be a valid pointer to an `RSIndexResult` instance.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexReader_Seek<'index_and_filter>(
    ir: *mut IndexReader<'index_and_filter>,
    doc_id: t_docId,
    res: *mut RSIndexResult<'index_and_filter>,
) -> bool {
    debug_assert!(!ir.is_null(), "ir must not be null");
    debug_assert!(!res.is_null(), "res must not be null");

    // SAFETY: The caller must ensure that `ir` is a valid pointer to an `IndexReader`
    let ir = unsafe { &mut *ir };

    // SAFETY: The caller must ensure that `res` is a valid pointer to a `RSIndexResult`
    let res = unsafe { &mut *res };

    ir_dispatch!(ir, seek_record, doc_id, res).unwrap_or_default()
}

/// Check if the index reader can return multiple entries for the same document ID.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `ir` must be a valid, non NULL, pointer to an `IndexReader` instance.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexReader_HasMulti(ir: *const IndexReader) -> bool {
    debug_assert!(!ir.is_null(), "ir must not be null");

    // SAFETY: The caller must ensure that `ir` is a valid pointer to an `IndexReader`
    let ir = unsafe { &*ir };

    ir_dispatch!(ir, has_duplicates)
}

/// Get the flags used to create the inverted index of the reader.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `ir` must be a valid, non NULL, pointer to an `IndexReader` instance.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexReader_Flags(ir: *const IndexReader) -> IndexFlags {
    debug_assert!(!ir.is_null(), "ir must not be null");

    // SAFETY: The caller must ensure that `ir` is a valid pointer to an `IndexReader`
    let ir = unsafe { &*ir };

    ir_dispatch!(ir, flags)
}

/// Get a pointer to the numeric filter used by the index reader. If the index reader does not use
/// a numeric filter, the function will return NULL.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `ir` must be a valid, non NULL, pointer to an `IndexReader` instance.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexReader_NumericFilter(ir: *const IndexReader) -> *const NumericFilter {
    debug_assert!(!ir.is_null(), "ir must not be null");

    // SAFETY: The caller must ensure that `ir` is a valid pointer to an `IndexReader`
    let ir = unsafe { &*ir };

    match ir {
        IndexReader::NumericFiltered(ir) => ir.filter(),
        IndexReader::NumericGeoFiltered(ir) => ir.filter(),
        IndexReader::NumericFilteredFloatCompression(ir) => ir.filter(),
        IndexReader::NumericGeoFilteredFloatCompression(ir) => ir.filter(),
        IndexReader::Numeric(_)
        | IndexReader::NumericFloatCompression(_)
        | IndexReader::Full(_)
        | IndexReader::FullWide(_)
        | IndexReader::FreqsFields(_)
        | IndexReader::FreqsFieldsWide(_)
        | IndexReader::FreqsOnly(_)
        | IndexReader::FieldsOnly(_)
        | IndexReader::FieldsOnlyWide(_)
        | IndexReader::FieldsOffsets(_)
        | IndexReader::FieldsOffsetsWide(_)
        | IndexReader::OffsetsOnly(_)
        | IndexReader::FreqsOffsets(_)
        | IndexReader::DocumentIdOnly(_)
        | IndexReader::RawDocumentIdOnly(_) => std::ptr::null(),
    }
}

/// Swap the inverted index of the reader with the given inverted index. This is only used by some
/// C tests to trigger revalidation on the reader.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `ir` must be a valid, non NULL, pointer to an `IndexReader` instance.
/// - `ii` must be a valid, non NULL, pointer to an `InvertedIndex` instance.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexReader_SwapIndex(ir: *mut IndexReader, ii: *const InvertedIndex) {
    debug_assert!(!ir.is_null(), "ir must not be null");
    debug_assert!(!ii.is_null(), "ii must not be null");

    // SAFETY: The caller must ensure that `ir` is a valid pointer to an `IndexReader`
    let ir = unsafe { &mut *ir };

    // SAFETY: The caller must ensure that `ii` is a valid pointer to an `InvertedIndex`
    let ii = unsafe { &*ii };

    match (ir, ii) {
        (IndexReader::Full(ir), InvertedIndex::Full(ii)) => ir.swap_index(&mut ii.inner()),
        (IndexReader::FullWide(ir), InvertedIndex::FullWide(ii)) => ir.swap_index(&mut ii.inner()),
        (IndexReader::FreqsFields(ir), InvertedIndex::FreqsFields(ii)) => {
            ir.swap_index(&mut ii.inner())
        }
        (IndexReader::FreqsFieldsWide(ir), InvertedIndex::FreqsFieldsWide(ii)) => {
            ir.swap_index(&mut ii.inner())
        }
        (IndexReader::FreqsOnly(ir), InvertedIndex::FreqsOnly(ii)) => {
            let mut ii = ii;
            ir.swap_index(&mut ii)
        }
        (IndexReader::FieldsOnly(ir), InvertedIndex::FieldsOnly(ii)) => {
            ir.swap_index(&mut ii.inner())
        }
        (IndexReader::FieldsOnlyWide(ir), InvertedIndex::FieldsOnlyWide(ii)) => {
            ir.swap_index(&mut ii.inner())
        }
        (IndexReader::FieldsOffsets(ir), InvertedIndex::FieldsOffsets(ii)) => {
            ir.swap_index(&mut ii.inner())
        }
        (IndexReader::FieldsOffsetsWide(ir), InvertedIndex::FieldsOffsetsWide(ii)) => {
            ir.swap_index(&mut ii.inner())
        }
        (IndexReader::OffsetsOnly(ir), InvertedIndex::OffsetsOnly(ii)) => {
            let mut ii = ii;
            ir.swap_index(&mut ii)
        }
        (IndexReader::FreqsOffsets(ir), InvertedIndex::FreqsOffsets(ii)) => {
            let mut ii = ii;
            ir.swap_index(&mut ii)
        }
        (IndexReader::DocumentIdOnly(ir), InvertedIndex::DocumentIdOnly(ii)) => {
            let mut ii = ii;
            ir.swap_index(&mut ii)
        }
        (IndexReader::RawDocumentIdOnly(ir), InvertedIndex::RawDocumentIdOnly(ii)) => {
            let mut ii = ii;
            ir.swap_index(&mut ii)
        }
        (IndexReader::Numeric(ir), InvertedIndex::Numeric(ii)) => ir.swap_index(&mut ii.inner()),
        (IndexReader::NumericFiltered(ir), InvertedIndex::Numeric(ii)) => {
            ir.swap_index(&mut ii.inner())
        }
        (IndexReader::NumericGeoFiltered(ir), InvertedIndex::Numeric(ii)) => {
            ir.swap_index(&mut ii.inner())
        }
        (IndexReader::NumericFloatCompression(ir), InvertedIndex::NumericFloatCompression(ii)) => {
            ir.swap_index(&mut ii.inner())
        }
        (
            IndexReader::NumericFilteredFloatCompression(ir),
            InvertedIndex::NumericFloatCompression(ii),
        ) => ir.swap_index(&mut ii.inner()),
        (
            IndexReader::NumericGeoFilteredFloatCompression(ir),
            InvertedIndex::NumericFloatCompression(ii),
        ) => ir.swap_index(&mut ii.inner()),
        _ => {}
    }
}

/// Revalidate the index reader against its inverted index. This is only needed if the inverted index
/// has been modified since the last time the reader was used. The function returns true if the
/// reader needs revalidation, false otherwise.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `ir` must be a valid, non NULL, pointer to an `IndexReader` instance.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn IndexReader_Revalidate(ir: *const IndexReader) -> bool {
    debug_assert!(!ir.is_null(), "ir must not be null");

    // SAFETY: The caller must ensure that `ir` is a valid pointer to an `IndexReader`
    let ir = unsafe { &*ir };

    ir_dispatch!(ir, needs_revalidation)
}
