/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! This module contains the debug information for an inverted index.

use ffi::t_docId;

/// Summary information about an inverted index containing all key metrics
#[repr(C)]
pub struct Summary {
    number_of_docs: usize,
    number_of_entries: usize,
    last_doc_id: t_docId,
    flags: u64,
    number_of_blocks: usize,
    block_efficiency: f64,
    has_efficiency: bool,
}

/// Summary information about the key metrics of a block in an inverted index
#[repr(C)]
pub struct BlockSummary {
    first_doc_id: t_docId,
    last_doc_id: t_docId,
    number_of_entries: usize,
}
