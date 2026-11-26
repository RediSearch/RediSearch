/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::sync::{Arc, Mutex};

use index_result::RSIndexResult;
use inverted_index::IndexReader;

use super::NumericRangeTree;
use crate::TreeUniqueId;

// At most this many million-document value arrays can be cached per process.
// An array held by a running query cannot be evicted to make room for a copy.
const MAX_CACHED_FIELDS: usize = 4;
type CachedField = (TreeUniqueId, usize, Arc<[f64]>);
static CACHE: Mutex<Vec<CachedField>> = Mutex::new(Vec::new());

impl NumericRangeTree {
    /// Values after numeric encoding, in document ID order, for a dense single-value field.
    /// Each entry belongs to its original DocId and cannot be reused for a later DocId.
    ///
    /// Callers must use the same index snapshot and exclude expired or deleted documents.
    pub fn dense_values(
        &self,
        docs: usize,
        mut can_continue: impl FnMut() -> bool,
    ) -> Option<Arc<[f64]>> {
        if self.has_multivalued_docs
            || self.last_doc_id as usize != docs
            || self.num_entries() != docs
            || docs > 1_000_000
        {
            return None;
        }
        // Never block another query on building a cold field.
        let mut cache = CACHE.try_lock().ok()?;
        let id = self.unique_id();
        if let Some(pos) = cache
            .iter()
            .position(|entry| (entry.0, entry.1) == (id, docs))
        {
            let entry = cache.remove(pos);
            let values = Arc::clone(&entry.2);
            cache.push(entry);
            return Some(values);
        }
        if cache.len() == MAX_CACHED_FIELDS {
            let pos = cache
                .iter()
                .position(|entry| Arc::strong_count(&entry.2) == 1)?;
            cache.remove(pos);
        }
        let mut values = vec![f64::NAN; docs + 1];
        let mut entries = 0;
        let mut record = RSIndexResult::build_numeric(0.0).build();
        for node in self.iter().filter(|node| node.is_leaf()) {
            let mut reader = node.range()?.reader();
            while reader.next_record(&mut record).ok()? {
                if !can_continue() {
                    return None;
                }
                let value = record.as_numeric()?;
                let slot = values.get_mut(record.doc_id as usize)?;
                if record.doc_id == 0 || value.is_nan() || !slot.is_nan() {
                    return None;
                }
                *slot = value;
                entries += 1;
            }
        }
        if entries != docs {
            return None;
        }
        let values: Arc<[f64]> = values.into();
        cache.push((id, docs, Arc::clone(&values)));
        Some(values)
    }
}
