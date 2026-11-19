/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Where an [`ElementFilter`](super::ElementFilter) reads the values of array elements.

use std::ptr::NonNull;

use document_metadata::OwnedDocumentMetadata;
use rqe_core::DocId;
use sorting_vector::RSSortingVector;
use value::ElementNumbers;

/// The numeric values that the documents of an index hold per array element.
pub trait ElementValues {
    /// The values of one document.
    type Document: DocumentElementValues;

    /// The values of document `doc_id`, or `None` when the index no longer has the document.
    fn document(&self, doc_id: DocId) -> Option<Self::Document>;
}

/// The numeric values that one document holds per array element.
pub trait DocumentElementValues {
    /// The value of the field in `slot` for the array element `element`, or `None` when that
    /// element has no value for the field.
    fn value(&self, slot: usize, element: u32) -> Option<f64>;

    /// The number of array elements that the field in `slot` has an entry for, with or
    /// without a value. Zero when the document has nothing for the field.
    fn num_elements(&self, slot: usize) -> u32;
}

/// [`ElementValues`] read from the sorting vectors of the documents in a
/// [`DocTable`](ffi::DocTable).
///
/// A slot is an index into the [`RSSortingVector`], whose value there is the stored form of
/// [`ElementNumbers`].
pub struct SortableElementValues {
    doc_table: NonNull<ffi::DocTable>,
}

impl SortableElementValues {
    /// # Safety
    ///
    /// 1. `doc_table` must be a [valid] pointer to a [`DocTable`](ffi::DocTable) for as long
    ///    as the returned value is alive.
    /// 2. The lock of the index that owns the table must be held for every call to
    ///    [`ElementValues::document`] on the returned value.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub const unsafe fn new(doc_table: NonNull<ffi::DocTable>) -> Self {
        Self { doc_table }
    }
}

impl ElementValues for SortableElementValues {
    type Document = OwnedDocumentMetadata;

    fn document(&self, doc_id: DocId) -> Option<Self::Document> {
        // SAFETY: the table is valid and the index lock is held (`new`'s 1 and 2).
        let dmd = unsafe { ffi::DocTable_Borrow(self.doc_table.as_ptr(), doc_id) };
        let dmd = NonNull::new(dmd.cast_mut())?;
        // SAFETY: `DocTable_Borrow` returned valid metadata and took the reference that the
        // handle now owns.
        Some(unsafe { OwnedDocumentMetadata::from_raw(dmd) })
    }
}

impl DocumentElementValues for OwnedDocumentMetadata {
    fn value(&self, slot: usize, element: u32) -> Option<f64> {
        self.sorting_vector().value(slot, element)
    }

    fn num_elements(&self, slot: usize) -> u32 {
        self.sorting_vector().num_elements(slot)
    }
}

impl DocumentElementValues for RSSortingVector {
    fn value(&self, slot: usize, element: u32) -> Option<f64> {
        ElementNumbers::stored_in(self.get(slot)?)?.get(element)
    }

    fn num_elements(&self, slot: usize) -> u32 {
        self.get(slot)
            .and_then(|value| ElementNumbers::stored_in(value))
            .map_or(0, |numbers| numbers.num_elements())
    }
}
