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
    IndexFlags_Index_WideSchema, t_docId,
};

use inverted_index::{
    EntriesTrackingIndex, FieldMaskTrackingIndex, InvertedIndex as II, RSIndexResult,
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
/// the index flags provided when creating the index. However, C does not support generics, so we
/// need to use an enum to represent the different implementations.
pub enum InvertedIndex {
    // Needs to track the field masks because it has the `StoreFieldFlags` flag set
    Full(FieldMaskTrackingIndex<Full>),
    // Needs to track the field masks because it has the `StoreFieldFlags` flag set
    FullWide(FieldMaskTrackingIndex<FullWide>),
    // Needs to track the field masks because it has the `StoreFieldFlags` flag set
    FreqsFields(FieldMaskTrackingIndex<FreqsFields>),
    // Needs to track the field masks because it has the `StoreFieldFlags` flag set
    FreqsFieldsWide(FieldMaskTrackingIndex<FreqsFieldsWide>),
    FreqsOnly(II<FreqsOnly>),
    // Needs to track the field masks because it has the `StoreFieldFlags` flag set
    FieldsOnly(FieldMaskTrackingIndex<FieldsOnly>),
    // Needs to track the field masks because it has the `StoreFieldFlags` flag set
    FieldsOnlyWide(FieldMaskTrackingIndex<FieldsOnlyWide>),
    // Needs to track the field masks because it has the `StoreFieldFlags` flag set
    FieldsOffsets(FieldMaskTrackingIndex<FieldsOffsets>),
    // Needs to track the field masks because it has the `StoreFieldFlags` flag set
    FieldsOffsetsWide(FieldMaskTrackingIndex<FieldsOffsetsWide>),
    OffsetsOnly(II<OffsetsOnly>),
    FreqsOffsets(II<FreqsOffsets>),
    DocumentIdOnly(II<DocIdsOnly>),
    RawDocumentIdOnly(II<RawDocIdsOnly>),
    // Needs to track the entries count because it has the `StoreNumeric` flag set
    Numeric(EntriesTrackingIndex<Numeric>),
}

impl InvertedIndex {
    /// Add a record to the inverted index. Returns by how many bytes the memory grew by.
    fn add_record(&mut self, record: &RSIndexResult) -> std::io::Result<usize> {
        use InvertedIndex::*;

        match self {
            Full(ii) => ii.add_record(record),
            FullWide(ii) => ii.add_record(record),
            FreqsFields(ii) => ii.add_record(record),
            FreqsFieldsWide(ii) => ii.add_record(record),
            FreqsOnly(ii) => ii.add_record(record),
            FieldsOnly(ii) => ii.add_record(record),
            FieldsOnlyWide(ii) => ii.add_record(record),
            FieldsOffsets(ii) => ii.add_record(record),
            FieldsOffsetsWide(ii) => ii.add_record(record),
            OffsetsOnly(ii) => ii.add_record(record),
            FreqsOffsets(ii) => ii.add_record(record),
            DocumentIdOnly(ii) => ii.add_record(record),
            RawDocumentIdOnly(ii) => ii.add_record(record),
            Numeric(ii) => ii.add_record(record),
        }
    }

    /// Get the memory usage of the inverted index in bytes.
    fn memory_usage(&self) -> usize {
        use InvertedIndex::*;

        match self {
            Full(ii) => ii.memory_usage(),
            FullWide(ii) => ii.memory_usage(),
            FreqsFields(ii) => ii.memory_usage(),
            FreqsFieldsWide(ii) => ii.memory_usage(),
            FreqsOnly(ii) => ii.memory_usage(),
            FieldsOnly(ii) => ii.memory_usage(),
            FieldsOnlyWide(ii) => ii.memory_usage(),
            FieldsOffsets(ii) => ii.memory_usage(),
            FieldsOffsetsWide(ii) => ii.memory_usage(),
            OffsetsOnly(ii) => ii.memory_usage(),
            FreqsOffsets(ii) => ii.memory_usage(),
            DocumentIdOnly(ii) => ii.memory_usage(),
            RawDocumentIdOnly(ii) => ii.memory_usage(),
            Numeric(ii) => ii.memory_usage(),
        }
    }
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

/// Create a new inverted index instance based on the provided flags and options. The output
/// parameter `mem_size` will be set to the memory usage of the created index. The inverted index
/// should be freed using [`InvertedIndex_Free`] when no longer needed.
///
/// # Safety
///
/// The following invariant must be upheld when calling this function:
/// - `mem_size` must be a valid pointer to a `usize`.
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
        (FREQS_ONLY_MASK, _, _) => InvertedIndex::FreqsOnly(II::new(flags, FreqsOnly)),
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
        (OFFSETS_ONLY_MASK, _, _) => InvertedIndex::OffsetsOnly(II::new(flags, OffsetsOnly)),
        (FREQS_OFFSETS_MASK, _, _) => InvertedIndex::FreqsOffsets(II::new(flags, FreqsOffsets)),
        (DOC_IDS_ONLY_MASK, false, _) => InvertedIndex::DocumentIdOnly(II::new(flags, DocIdsOnly)),
        (DOC_IDS_ONLY_MASK, true, _) => {
            InvertedIndex::RawDocumentIdOnly(II::new(flags, RawDocIdsOnly))
        }
        (NUMERIC_MASK, _, false) => {
            InvertedIndex::Numeric(EntriesTrackingIndex::new(flags, Numeric::new()))
        }
        (NUMERIC_MASK, _, true) => InvertedIndex::Numeric(EntriesTrackingIndex::new(
            flags,
            Numeric::new().with_float_compression(),
        )),
        _ => panic!("Unsupported index flags: {flags:?}"),
    };

    *mem_size = ii.memory_usage();

    let ii_boxed = Box::new(ii);
    Box::into_raw(ii_boxed)
}

/// Free the memory associated with the inverted index instance created using [`NewInvertedIndex_Ex`].
///
/// # Safety
/// The following invariant must be upheld when calling this function:
/// - `ii` must be a valid pointer to an `InvertedIndex` instance created using
///   [`NewInvertedIndex_Ex`] or `NewInvertedIndex`.
/// - `ii` must not be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndex_Free(ii: *mut InvertedIndex) {
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
    // SAFETY: The caller must ensure that `ii` is a valid pointer to an `InvertedIndex`
    let ii = unsafe { &*ii };
    ii.memory_usage()
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
    let record = RSIndexResult::numeric(value).doc_id(doc_id);

    // SAFETY: The caller must ensure that `ii` is a valid pointer to an `InvertedIndex`
    let ii = unsafe { &mut *ii };
    ii.add_record(&record).unwrap()
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
    // SAFETY: The caller must ensure that `ii` is a valid pointer to an `InvertedIndex`
    let ii = unsafe { &mut *ii };

    // SAFETY: The caller must ensure that `record` is a valid pointer to an `RSIndexResult`
    let record = unsafe { &*record };

    ii.add_record(record).unwrap()
}
