/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Live document frequency for IDF when RAM inverted lists still hold stale internal doc ids.

use super::{FilterMaskReader, IndexReader, IndexReaderCore, TermReader};
use crate::{
    DecodedBy, Decoder, Encoder, HasInnerIndex, TermDecoder,
    opaque::OpaqueEncoding,
};
use ffi::{DocTable_Exists, IndexSpec, t_docId};

/// [`IndexSpec`] gates for using a posting scan + [`DocTable_Exists`] as term document frequency.
#[inline]
pub fn use_live_df_for_idf(spec: &IndexSpec) -> bool {
    spec.diskSpec.is_null() && !spec.keysDict.is_null()
}

/// Term DF for IDF/BM25: live count in production RAM indexes, encoded [`unique_docs`](TermReader::unique_docs) otherwise.
pub trait IdfTermDocs<'index>: TermReader<'index> {
    /// Number of documents that should feed IDF for this reader and index spec.
    ///
    /// On I/O error, callers should fall back to [`TermReader::unique_docs`].
    fn idf_term_docs(&self, spec: &IndexSpec) -> std::io::Result<u32>;
}

impl<'index, E> IdfTermDocs<'index> for IndexReaderCore<'index, E>
where
    E: Encoder + Decoder + DecodedBy<Decoder = E> + OpaqueEncoding + TermDecoder,
    E::Storage: HasInnerIndex<E>,
{
    fn idf_term_docs(&self, spec: &IndexSpec) -> std::io::Result<u32> {
        if !use_live_df_for_idf(spec) {
            return Ok(self.ii.unique_docs());
        }
        let mut doc_exists =
            |id: t_docId| unsafe { DocTable_Exists(&spec.docs, id) };
        self.ii.count_live_unique_docs(&mut doc_exists)
    }
}

impl<'index, E> IdfTermDocs<'index> for FilterMaskReader<IndexReaderCore<'index, E>>
where
    E: Encoder + Decoder + DecodedBy<Decoder = E> + OpaqueEncoding + TermDecoder,
    E::Storage: HasInnerIndex<E>,
{
    fn idf_term_docs(&self, spec: &IndexSpec) -> std::io::Result<u32> {
        if !use_live_df_for_idf(spec) {
            return Ok(IndexReader::unique_docs(self) as u32);
        }
        let mut doc_exists =
            |id: t_docId| unsafe { DocTable_Exists(&spec.docs, id) };
        self.internal_index()
            .count_live_unique_docs_for_query_mask(self.filter_mask(), &mut doc_exists)
    }
}
