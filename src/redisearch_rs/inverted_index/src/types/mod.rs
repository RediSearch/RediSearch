/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::RSQueryTerm;
use std::ffi::c_char;

/// Represents a numeric value in an index record.
/// cbindgen:field-names=[value]
#[allow(rustdoc::broken_intra_doc_links)] // The field rename above breaks the intra-doc link
#[repr(C)]
pub struct RSNumericRecord(pub f64);

/// Represents the encoded offsets of a term in a document. You can read the offsets by iterating
/// over it with RSOffsetVector_Iterator
#[repr(C)]
pub struct RSOffsetVector {
    pub data: *mut c_char,
    pub len: u32,
}

/// Represents a single record of a document inside a term in the inverted index
#[repr(C)]
pub struct RSTermRecord {
    /// The term that brought up this record
    pub term: *mut RSQueryTerm,

    /// The encoded offsets in which the term appeared in the document
    pub offsets: RSOffsetVector,
}
