/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/// Why [`NestedIndex::index_document`](crate::NestedIndex::index_document) rejected a document.
/// A rejected document leaves the index unchanged.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IndexError {
    /// An element's path does not route.
    #[error("element {element}: {error}")]
    Path {
        /// The position of the element in the document.
        element: usize,
        /// Why its path does not route.
        error: compound_index::PathError,
    },
    /// The document's paths together need more distinct values under one node than its level's
    /// cardinality limit allows. No single element is at fault.
    #[error("the document needs more than {max} distinct values on level {level} under one path")]
    CardinalityExceeded {
        /// The position of the level.
        level: usize,
        /// The level's limit.
        max: usize,
    },
    /// The document's id is not greater than the last one indexed.
    #[error("doc id {doc_id} is not greater than the last one indexed, {last}")]
    DocIdNotIncreasing {
        /// The document's id.
        doc_id: rqe_core::DocId,
        /// The last doc id indexed.
        last: rqe_core::DocId,
    },
    /// An element's terminal value is numeric for a TAG terminal, or the reverse.
    #[error("element {element} has a value of the wrong type for this index")]
    ValueMismatch {
        /// The position of the element in the document.
        element: usize,
    },
    /// An element's numeric terminal value is NaN.
    #[error("element {element} has a NaN value")]
    NotANumber {
        /// The position of the element in the document.
        element: usize,
    },
}
