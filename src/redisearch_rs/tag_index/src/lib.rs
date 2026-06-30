/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Rust port of the TAG index (`src/tag_index.c`), memory mode.
//!
//! A [`TagIndex`] maps every tag value of one TAG field to the postings list
//! (an [`InvertedIndex`] of document ids) of the documents carrying it, plus
//! an optional [`TagSuffixIndex`] powering `WITHSUFFIXTRIE` queries.
//!
//! Tag values are raw byte strings: separator splitting, whitespace trimming
//! and case folding happen upstream (C's `TagIndex_Preprocess`), exactly as
//! in the C module. Disk mode (`diskSpec`) stays on the C side for now — it
//! is a thin dispatcher to `SearchDisk_*` and carries no data-structure
//! logic to port.
//!
//! This change introduces only the data structures; the methods, examples and
//! tests follow in a later change, so every field is temporarily unread.
#![expect(dead_code, reason = "read by methods added in the follow-up change")]

pub mod suffix;

use index_result::RSIndexResult;
use inverted_index::{IndexReaderCore, InvertedIndex, doc_ids_only::DocIdsOnly};
pub use suffix::TagSuffixIndex;
use trie_rs::TrieMap;

/// See the [crate documentation](self) for an overview.
pub struct TagIndex {
    unique_id: u32,

    /// tag value → postings. Tag postings only need document ids, so the
    /// inverted indexes always use the [`DocIdsOnly`] encoding, like C's
    /// `NewInvertedIndex(Index_DocIdsOnly)`.
    values: TrieMap<InvertedIndex<DocIdsOnly>>,

    /// Suffix index, present only for fields created `WITHSUFFIXTRIE`.
    suffix: Option<TagSuffixIndex>,

    /// Documents indexed so far (C: `stats->numRecords`).
    num_records: usize,

    /// Total heap held by the postings (C: `stats->invertedSize`).
    inverted_size: usize,
}

/// Sequential reader over one tag's postings; yields document ids in
/// increasing order. Obtained from [`TagIndex::open_reader`].
pub struct TagReader<'index> {
    inner: IndexReaderCore<'index, DocIdsOnly>,
    /// Scratch record the underlying reader decodes into.
    result: RSIndexResult<'index>,
}
