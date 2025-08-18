/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{ffi::c_char, fmt::Debug, marker::PhantomData};

use enumflags2::{BitFlags, bitflags};
use ffi::{RSDocumentMetadata, RSQueryTerm, RSYieldableMetric, t_docId, t_fieldMask};
use low_memory_thin_vec::LowMemoryThinVec;

/// Represents a numeric value in an index record.
/// cbindgen:field-names=[value]
#[allow(rustdoc::broken_intra_doc_links)] // The field rename above breaks the intra-doc link
#[repr(C)]
#[derive(Debug, PartialEq)]
pub struct RSNumericRecord(pub f64);

/// Represents the encoded offsets of a term in a document. You can read the offsets by iterating
/// over it with RSIndexResult_IterateOffsets
#[repr(C)]
#[derive(Eq, PartialEq)]
pub struct RSOffsetVector<'index> {
    /// At this point the data ownership is still managed by the caller.
    // TODO: switch to a Cow once the caller code has been ported to Rust.
    pub data: *mut c_char,
    pub len: u32,
    /// data may be borrowed from the reader.
    /// The data pointer does not allow lifetime so use a PhantomData to carry the lifetime for it instead.
    pub _phantom: PhantomData<&'index ()>,
}

/// Represents a single record of a document inside a term in the inverted index
/// cbindgen:rename-all=CamelCase
#[repr(C)]
#[derive(Eq, PartialEq)]
pub struct RSTermRecord<'index> {
    /// We mark copied terms so we can treat them a bit differently on delete.
    pub is_copy: bool,

    /// The term that brought up this record
    pub term: *mut RSQueryTerm,

    /// The encoded offsets in which the term appeared in the document
    pub offsets: RSOffsetVector<'index>,
}

pub type RSResultKindMask = BitFlags<RSResultKind, u8>;

/// Represents an aggregate array of values in an index record.
///
/// The C code should always use `AggregateResult_New` to construct a new instance of this type
/// using Rust since the internals cannot be constructed directly in C. The reason is because of
/// the `LowMemoryThinVec` which needs to exist in Rust's memory space to ensure its memory is
/// managed correctly.
/// cbindgen:rename-all=CamelCase
#[repr(C)]
#[derive(Debug, Eq, PartialEq)]
pub struct RSAggregateResult<'index, 'children> {
    /// We mark copied aggregates so we can treat them a bit differently on delete.
    pub is_copy: bool,

    /// The records making up this aggregate result
    ///
    /// The `RSAggregateResult` is part of a union in [`RSResultData`], so it needs to have a
    /// known size. The std `Vec` won't have this since it is not `#[repr(C)]`, so we use our
    /// own `LowMemoryThinVec` type which is `#[repr(C)]` and has a known size instead.
    pub records: LowMemoryThinVec<*const RSIndexResult<'index, 'children>>,

    /// A map of the aggregate kind of the underlying records
    pub kind_mask: RSResultKindMask,

    /// The lifetime is actually on the `*const RSIndexResult` children stored in the `records`
    /// field. But since these are stored as a pointers which do not support lifetimes, we need to
    /// use a PhantomData to carry the lifetime for each child record instead.
    pub _phantom: PhantomData<&'children ()>,
}

/// An iterator over the results in an [`RSAggregateResult`].
pub struct RSAggregateResultIter<'index, 'aggregate_children> {
    pub agg: &'index RSAggregateResult<'index, 'aggregate_children>,
    pub index: usize,
}

impl<'index, 'aggregate_children> Iterator for RSAggregateResultIter<'index, 'aggregate_children> {
    type Item = &'index RSIndexResult<'index, 'aggregate_children>;

    /// Get the next item in the iterator
    ///
    /// # Safety
    /// The caller must ensure that all memory pointers in the aggregate result are still valid.
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(result) = self.agg.get(self.index) {
            self.index += 1;
            Some(result)
        } else {
            None
        }
    }
}

/// Represents a virtual result in an index record.
#[repr(C)]
#[derive(Debug, Eq, PartialEq)]
pub struct RSVirtualResult;

/// A C-style discriminant for [`RSResultData`].
///
/// # Implementation notes
///
/// We need a standalone C-style discriminant to get `bitflags` to generate a
/// dedicated bitmask type. Unfortunately, we can't apply `#[bitflags]` directly
/// on [`RSResultData`] since `bitflags` doesn't support enum with data in
/// their variants, nor lifetime parameters.
///
/// The discriminant values must match *exactly* the ones specified
/// on [`RSResultData`].
#[bitflags]
#[repr(u8)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum RSResultKind {
    Union = 1,
    Intersection = 2,
    Term = 4,
    Virtual = 8,
    Numeric = 16,
    Metric = 32,
    HybridMetric = 64,
}

/// Holds the actual data of an ['IndexResult']
///
/// These enum values should stay in sync with [`RSResultKind`], so that the C union generated matches
/// the bitflags on [`RSResultKindMask`]
///
/// The `'index` lifetime is linked to the [`crate::IndexBlock`] when decoding borrows from the block.
/// While the `'aggregate_children` lifetime is linked to [`RSAggregateResult`] that is holding
/// raw pointers to results.
#[repr(u8)]
#[derive(Debug, PartialEq)]
/// cbindgen:prefix-with-name=true
pub enum RSResultData<'index, 'aggregate_children> {
    Union(RSAggregateResult<'index, 'aggregate_children>) = 1,
    Intersection(RSAggregateResult<'index, 'aggregate_children>) = 2,
    Term(RSTermRecord<'index>) = 4,
    Virtual(RSVirtualResult) = 8,
    Numeric(RSNumericRecord) = 16,
    Metric(RSNumericRecord) = 32,
    HybridMetric(RSAggregateResult<'index, 'aggregate_children>) = 64,
}

/// The result of an inverted index
/// cbindgen:rename-all=CamelCase
#[repr(C)]
#[derive(Debug, PartialEq)]
pub struct RSIndexResult<'index, 'aggregate_children> {
    /// The document ID of the result
    pub doc_id: t_docId,

    /// Some metadata about the result document
    pub dmd: *const RSDocumentMetadata,

    /// The aggregate field mask of all the records in this result
    pub field_mask: t_fieldMask,

    /// The total frequency of all the records in this result
    pub freq: u32,

    /// For term records only. This is used as an optimization, allowing the result to be loaded
    /// directly into memory
    pub offsets_sz: u32,

    /// The actual data of the result
    pub data: RSResultData<'index, 'aggregate_children>,

    /// Holds an array of metrics yielded by the different iterators in the AST
    pub metrics: *mut RSYieldableMetric,

    /// Relative weight for scoring calculations. This is derived from the result's iterator weight
    pub weight: f64,
}
