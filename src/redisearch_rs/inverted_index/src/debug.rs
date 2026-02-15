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
use redis_reply::ArrayBuilder;

/// Summary information about an inverted index containing all key metrics
#[repr(C)]
#[derive(Debug, PartialEq)]
pub struct Summary {
    pub number_of_docs: u32,
    pub number_of_entries: usize,
    pub last_doc_id: t_docId,
    pub flags: u64,
    pub number_of_blocks: usize,
    pub block_efficiency: f64,
    pub has_efficiency: bool,
}

impl Summary {
    pub fn reply_with_inverted_index_header(&self, parent: &mut ArrayBuilder<'_>) {
        let mut header_arr = parent.array();

        header_arr.simple_string(c"numDocs");
        header_arr.long_long(self.number_of_docs as i64);
        header_arr.simple_string(c"numEntries");
        header_arr.long_long(self.number_of_entries as i64);
        header_arr.simple_string(c"lastId");
        header_arr.long_long(self.last_doc_id as i64);
        header_arr.simple_string(c"flags");
        header_arr.long_long(self.flags as i64);
        header_arr.simple_string(c"numberOfBlocks");
        header_arr.long_long(self.number_of_blocks as i64);

        if self.has_efficiency {
            header_arr.simple_string(c"blocks_efficiency (numEntries/numberOfBlocks)");
            header_arr.double(self.block_efficiency);
        }
    }
}

/// Summary information about the key metrics of a block in an inverted index
#[repr(C)]
#[derive(Debug, PartialEq)]
pub struct BlockSummary {
    pub first_doc_id: t_docId,
    pub last_doc_id: t_docId,
    pub number_of_entries: u16,
}
