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

use ffi::{
    IndexFlags, IndexFlags_Index_DocIdsOnly, IndexFlags_Index_StoreFieldFlags,
    IndexFlags_Index_StoreFreqs, IndexFlags_Index_StoreNumeric, IndexFlags_Index_StoreTermOffsets,
    IndexFlags_Index_WideSchema, t_docId, t_fieldMask,
};

use inverted_index::{
    EntriesTrackingIndex, FieldMaskTrackingIndex, IndexBlock, RSIndexResult,
    debug::{BlockSummary, Summary},
    doc_ids_only::DocIdsOnly,
    fields_offsets::{FieldsOffsets, FieldsOffsetsWide},
    fields_only::{FieldsOnly, FieldsOnlyWide},
    freqs_fields::{FreqsFields, FreqsFieldsWide},
    freqs_offsets::FreqsOffsets,
    freqs_only::FreqsOnly,
    full::{Full, FullWide},
    numeric::Numeric,
    offsets_only::OffsetsOnly,
    raw_doc_ids_only::RawDocIdsOnly,
};

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
        (FULL_MASK, _, _) => InvertedIndex::Full(FieldMaskTrackingIndex::new(flags, Full)),
        (FULL_WIDE_MASK, _, _) => {
            InvertedIndex::FullWide(FieldMaskTrackingIndex::new(flags, FullWide))
        }
        (FREQS_FIELDS_MASK, _, _) => {
            InvertedIndex::FreqsFields(FieldMaskTrackingIndex::new(flags, FreqsFields))
        }
        (FREQS_FIELDS_WIDE_MASK, _, _) => {
            InvertedIndex::FreqsFieldsWide(FieldMaskTrackingIndex::new(flags, FreqsFieldsWide))
        }
        (FREQS_ONLY_MASK, _, _) => {
            InvertedIndex::FreqsOnly(inverted_index::InvertedIndex::new(flags, FreqsOnly))
        }
        (FIELDS_ONLY_MASK, _, _) => {
            InvertedIndex::FieldsOnly(FieldMaskTrackingIndex::new(flags, FieldsOnly))
        }
        (FIELDS_ONLY_WIDE_MASK, _, _) => {
            InvertedIndex::FieldsOnlyWide(FieldMaskTrackingIndex::new(flags, FieldsOnlyWide))
        }
        (FIELDS_OFFSETS_MASK, _, _) => {
            InvertedIndex::FieldsOffsets(FieldMaskTrackingIndex::new(flags, FieldsOffsets))
        }
        (FIELDS_OFFSETS_WIDE_MASK, _, _) => {
            InvertedIndex::FieldsOffsetsWide(FieldMaskTrackingIndex::new(flags, FieldsOffsetsWide))
        }
        (OFFSETS_ONLY_MASK, _, _) => {
            InvertedIndex::OffsetsOnly(inverted_index::InvertedIndex::new(flags, OffsetsOnly))
        }
        (FREQS_OFFSETS_MASK, _, _) => {
            InvertedIndex::FreqsOffsets(inverted_index::InvertedIndex::new(flags, FreqsOffsets))
        }
        (DOC_IDS_ONLY_MASK, false, _) => {
            InvertedIndex::DocumentIdOnly(inverted_index::InvertedIndex::new(flags, DocIdsOnly))
        }
        (DOC_IDS_ONLY_MASK, true, _) => InvertedIndex::RawDocumentIdOnly(
            inverted_index::InvertedIndex::new(flags, RawDocIdsOnly),
        ),
        (NUMERIC_MASK, _, false) => {
            InvertedIndex::Numeric(EntriesTrackingIndex::new(flags, Numeric::new()))
        }
        (NUMERIC_MASK, _, true) => InvertedIndex::Numeric(EntriesTrackingIndex::new(
            flags,
            Numeric::new().with_float_compression(),
        )),
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
pub unsafe extern "C" fn InvertedIndex_NumDocs(ii: *const InvertedIndex) -> usize {
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
        | InvertedIndex::Numeric(_) => 0,
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
