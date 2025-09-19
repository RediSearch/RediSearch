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
#[derive(Debug, PartialEq)]
pub struct Summary {
    pub number_of_docs: usize,
    pub number_of_entries: usize,
    pub last_doc_id: t_docId,
    pub flags: u64,
    pub number_of_blocks: usize,
    pub block_efficiency: f64,
    pub has_efficiency: bool,
}

/// Summary information about the key metrics of a block in an inverted index
#[repr(C)]
#[derive(Debug, PartialEq)]
pub struct BlockSummary {
    pub first_doc_id: t_docId,
    pub last_doc_id: t_docId,
    pub number_of_entries: usize,
}
