/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI-facing wrapper enum that dispatches to the correct inverted index type at runtime.

use std::fmt::Debug;

use crate::{
    EntriesTrackingIndex, FieldMaskTrackingIndex, InvertedIndex as InvertedIndexInner,
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

/// Encoding types that correspond to a variant of the opaque [`InvertedIndex`] enum.
///
/// Each encoding type knows how to extract its storage from the opaque wrapper,
/// enabling type-safe access without manual matching.
pub trait OpaqueEncoding: Sized {
    /// The storage type wrapping this encoding in the opaque [`InvertedIndex`] enum.
    type Storage;

    /// Try to extract a reference to this encoding's storage from the opaque wrapper.
    fn extract_ii_from(opaque: &InvertedIndex) -> Option<&Self::Storage>;

    /// Try to extract a mutable reference to this encoding's storage from the opaque wrapper.
    fn extract_ii_from_mut(opaque: &mut InvertedIndex) -> Option<&mut Self::Storage>;
}

macro_rules! impl_opaque_encoding {
    ($encoding:ty, $variant:ident, $storage:ty) => {
        impl OpaqueEncoding for $encoding {
            type Storage = $storage;

            fn extract_ii_from(opaque: &InvertedIndex) -> Option<&Self::Storage> {
                match opaque {
                    InvertedIndex::$variant(ii) => Some(ii),
                    _ => None,
                }
            }

            fn extract_ii_from_mut(opaque: &mut InvertedIndex) -> Option<&mut Self::Storage> {
                match opaque {
                    InvertedIndex::$variant(ii) => Some(ii),
                    _ => None,
                }
            }
        }
    };
}

impl_opaque_encoding!(Full, Full, FieldMaskTrackingIndex<Full>);
impl_opaque_encoding!(FullWide, FullWide, FieldMaskTrackingIndex<FullWide>);
impl_opaque_encoding!(FreqsFields, FreqsFields, FieldMaskTrackingIndex<FreqsFields>);
impl_opaque_encoding!(FreqsFieldsWide, FreqsFieldsWide, FieldMaskTrackingIndex<FreqsFieldsWide>);
impl_opaque_encoding!(FreqsOnly, FreqsOnly, InvertedIndexInner<FreqsOnly>);
impl_opaque_encoding!(FieldsOnly, FieldsOnly, FieldMaskTrackingIndex<FieldsOnly>);
impl_opaque_encoding!(FieldsOnlyWide, FieldsOnlyWide, FieldMaskTrackingIndex<FieldsOnlyWide>);
impl_opaque_encoding!(FieldsOffsets, FieldsOffsets, FieldMaskTrackingIndex<FieldsOffsets>);
impl_opaque_encoding!(FieldsOffsetsWide, FieldsOffsetsWide, FieldMaskTrackingIndex<FieldsOffsetsWide>);
impl_opaque_encoding!(OffsetsOnly, OffsetsOnly, InvertedIndexInner<OffsetsOnly>);
impl_opaque_encoding!(FreqsOffsets, FreqsOffsets, InvertedIndexInner<FreqsOffsets>);
impl_opaque_encoding!(DocIdsOnly, DocumentIdOnly, InvertedIndexInner<DocIdsOnly>);
impl_opaque_encoding!(RawDocIdsOnly, RawDocumentIdOnly, InvertedIndexInner<RawDocIdsOnly>);
impl_opaque_encoding!(Numeric, Numeric, EntriesTrackingIndex<Numeric>);
impl_opaque_encoding!(NumericFloatCompression, NumericFloatCompression, EntriesTrackingIndex<NumericFloatCompression>);

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
    FreqsOnly(InvertedIndexInner<FreqsOnly>),
    // Needs to track the field masks because it has the `StoreFieldFlags` flag set
    FieldsOnly(FieldMaskTrackingIndex<FieldsOnly>),
    // Needs to track the field masks because it has the `StoreFieldFlags` flag set
    FieldsOnlyWide(FieldMaskTrackingIndex<FieldsOnlyWide>),
    // Needs to track the field masks because it has the `StoreFieldFlags` flag set
    FieldsOffsets(FieldMaskTrackingIndex<FieldsOffsets>),
    // Needs to track the field masks because it has the `StoreFieldFlags` flag set
    FieldsOffsetsWide(FieldMaskTrackingIndex<FieldsOffsetsWide>),
    OffsetsOnly(InvertedIndexInner<OffsetsOnly>),
    FreqsOffsets(InvertedIndexInner<FreqsOffsets>),
    DocumentIdOnly(InvertedIndexInner<DocIdsOnly>),
    RawDocumentIdOnly(InvertedIndexInner<RawDocIdsOnly>),
    // Needs to track the entries count because it has the `StoreNumeric` flag set
    Numeric(EntriesTrackingIndex<Numeric>),
    NumericFloatCompression(EntriesTrackingIndex<NumericFloatCompression>),
}

impl Debug for InvertedIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full(ii) => f.debug_tuple("Full").field(ii).finish(),
            Self::FullWide(ii) => f.debug_tuple("FullWide").field(ii).finish(),
            Self::FreqsFields(ii) => f.debug_tuple("FreqsFields").field(ii).finish(),
            Self::FreqsFieldsWide(ii) => f.debug_tuple("FreqsFieldsWide").field(ii).finish(),
            Self::FreqsOnly(ii) => f.debug_tuple("FreqsOnly").field(ii).finish(),
            Self::FieldsOnly(ii) => f.debug_tuple("FieldsOnly").field(ii).finish(),
            Self::FieldsOnlyWide(ii) => f.debug_tuple("FieldsOnlyWide").field(ii).finish(),
            Self::FieldsOffsets(ii) => f.debug_tuple("FieldsOffsets").field(ii).finish(),
            Self::FieldsOffsetsWide(ii) => f.debug_tuple("FieldsOffsetsWide").field(ii).finish(),
            Self::OffsetsOnly(ii) => f.debug_tuple("OffsetsOnly").field(ii).finish(),
            Self::FreqsOffsets(ii) => f.debug_tuple("FreqsOffsets").field(ii).finish(),
            Self::DocumentIdOnly(ii) => f.debug_tuple("DocumentIdOnly").field(ii).finish(),
            Self::RawDocumentIdOnly(ii) => f.debug_tuple("RawDocumentIdOnly").field(ii).finish(),
            Self::Numeric(ii) => f.debug_tuple("Numeric").field(ii).finish(),
            Self::NumericFloatCompression(ii) => {
                f.debug_tuple("NumericFloatCompression").field(ii).finish()
            }
        }
    }
}
