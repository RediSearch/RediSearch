/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::sync::Arc;

use index_result::RSIndexResult;
use inverted_index::IndexReader;

use super::NumericRangeTree;

impl NumericRangeTree {
    /// Values after numeric encoding, in document ID order, for a dense single-value field.
    /// Each entry belongs to its original DocId and cannot be reused for a later DocId.
    ///
    /// Callers must use the same index snapshot and exclude expired or deleted documents.
    pub fn dense_values(&self, docs: usize) -> Option<Arc<[f64]>> {
        if self.has_multivalued_docs
            || self.last_doc_id as usize != docs
            || self.num_entries() != docs
        {
            return None;
        }
        self.dense_values
            .get_or_init(|| {
                let mut values = vec![f64::NAN; docs + 1];
                let mut record = RSIndexResult::build_numeric(0.0).build();
                for node in self.iter().filter(|node| node.is_leaf()) {
                    let mut reader = node.range()?.reader();
                    while reader.next_record(&mut record).ok()? {
                        let slot = values.get_mut(record.doc_id as usize)?;
                        if !slot.is_nan() {
                            return None;
                        }
                        *slot = record.as_numeric()?;
                    }
                }
                Some(values.into())
            })
            .clone()
    }
}
