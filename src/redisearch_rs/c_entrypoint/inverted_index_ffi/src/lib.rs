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
    IndexFlags_Index_WideSchema,
};

use inverted_index::{
    EntriesTrackingIndex, FieldMaskTrackingIndex, InvertedIndex as II,
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
        (FULL_MASK, _, _) => {
            let ii = FieldMaskTrackingIndex::new(flags, Full);

            *mem_size = ii.memory_usage();
            InvertedIndex::Full(ii)
        }
        (FULL_WIDE_MASK, _, _) => {
            let ii = FieldMaskTrackingIndex::new(flags, FullWide);

            *mem_size = ii.memory_usage();
            InvertedIndex::FullWide(ii)
        }
        (FREQS_FIELDS_MASK, _, _) => {
            let ii = FieldMaskTrackingIndex::new(flags, FreqsFields);

            *mem_size = ii.memory_usage();
            InvertedIndex::FreqsFields(ii)
        }
        (FREQS_FIELDS_WIDE_MASK, _, _) => {
            let ii = FieldMaskTrackingIndex::new(flags, FreqsFieldsWide);

            *mem_size = ii.memory_usage();
            InvertedIndex::FreqsFieldsWide(ii)
        }
        (FREQS_ONLY_MASK, _, _) => {
            let ii = II::new(flags, FreqsOnly);

            *mem_size = ii.memory_usage();
            InvertedIndex::FreqsOnly(ii)
        }
        (FIELDS_ONLY_MASK, _, _) => {
            let ii = FieldMaskTrackingIndex::new(flags, FieldsOnly);

            *mem_size = ii.memory_usage();
            InvertedIndex::FieldsOnly(ii)
        }
        (FIELDS_ONLY_WIDE_MASK, _, _) => {
            let ii = FieldMaskTrackingIndex::new(flags, FieldsOnlyWide);

            *mem_size = ii.memory_usage();
            InvertedIndex::FieldsOnlyWide(ii)
        }
        (FIELDS_OFFSETS_MASK, _, _) => {
            let ii = FieldMaskTrackingIndex::new(flags, FieldsOffsets);

            *mem_size = ii.memory_usage();
            InvertedIndex::FieldsOffsets(ii)
        }
        (FIELDS_OFFSETS_WIDE_MASK, _, _) => {
            let ii = FieldMaskTrackingIndex::new(flags, FieldsOffsetsWide);

            *mem_size = ii.memory_usage();
            InvertedIndex::FieldsOffsetsWide(ii)
        }
        (OFFSETS_ONLY_MASK, _, _) => {
            let ii = II::new(flags, OffsetsOnly);

            *mem_size = ii.memory_usage();
            InvertedIndex::OffsetsOnly(ii)
        }
        (FREQS_OFFSETS_MASK, _, _) => {
            let ii = II::new(flags, FreqsOffsets);

            *mem_size = ii.memory_usage();
            InvertedIndex::FreqsOffsets(ii)
        }
        (DOC_IDS_ONLY_MASK, false, _) => {
            let ii = II::new(flags, DocIdsOnly);

            *mem_size = ii.memory_usage();
            InvertedIndex::DocumentIdOnly(ii)
        }
        (DOC_IDS_ONLY_MASK, true, _) => {
            let ii = II::new(flags, RawDocIdsOnly);

            *mem_size = ii.memory_usage();
            InvertedIndex::RawDocumentIdOnly(ii)
        }
        (NUMERIC_MASK, _, false) => {
            let ii = EntriesTrackingIndex::new(flags, Numeric::new());

            *mem_size = ii.memory_usage();
            InvertedIndex::Numeric(ii)
        }
        (NUMERIC_MASK, _, true) => {
            let ii = EntriesTrackingIndex::new(flags, Numeric::new().with_float_compression());

            *mem_size = ii.memory_usage();
            InvertedIndex::Numeric(ii)
        }
        _ => panic!("Unsupported index flags: {:?}", flags),
    };

    let ii_boxed = Box::new(ii);
    Box::into_raw(ii_boxed)
}

#[unsafe(no_mangle)]
pub extern "C" fn InvertedIndex_Free(ii: *mut InvertedIndex) {
    let _ = unsafe { Box::from_raw(ii) };
}

#[unsafe(no_mangle)]
pub extern "C" fn InvertedIndex_MemUsage(ii: *const InvertedIndex) -> usize {
    use InvertedIndex::*;

    let ii = unsafe { &*ii };
    match ii {
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
