/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use compound_index::InsertError;

/// Why [`NestedIndex::index_document`](crate::NestedIndex::index_document) rejected a document.
/// A rejected document leaves the index unchanged.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IndexError {
    /// An element's path does not route, or the document's paths together break a cardinality
    /// limit.
    #[error(transparent)]
    Path(#[from] InsertError),
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
