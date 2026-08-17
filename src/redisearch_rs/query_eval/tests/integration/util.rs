/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared helpers for the query_eval integration tests.

use index_result::{RSIndexResult, RSOffsetSlice};
use query_term::RSQueryTerm;
use rqe_core::FieldMask;

/// All (low 32) field-mask bits set, so a reader's field-mask filter never
/// excludes a document unless a test narrows the mask deliberately.
pub const ALL_INDEXED_FIELDS: FieldMask = u32::MAX as FieldMask;

/// Build term postings for the given document IDs, each indexed under
/// `field_mask`. `write_forward_index_entry` only consumes each record's doc id,
/// frequency, and field mask, so a single dummy offset is enough to form a
/// well-formed record.
pub fn term_records(doc_ids: &[u64], field_mask: FieldMask) -> Vec<RSIndexResult<'static>> {
    const OFFSETS: &[u8] = &[0];
    doc_ids
        .iter()
        .map(|&doc_id| {
            let mut term = RSQueryTerm::new("t", 1, 0);
            term.set_idf(5.0);
            term.set_bm25_idf(10.0);
            RSIndexResult::build_term()
                .borrowed_record(Some(term), RSOffsetSlice::from_slice(OFFSETS))
                .doc_id(doc_id)
                .field_mask(field_mask)
                .frequency(1)
                .build()
        })
        .collect()
}

/// Owned mock `RedisModuleString` keys for an id-filter node. The node
/// borrows the pointer array (mirroring production, where the keys are a
/// window into the request's held argv); this owner must outlive the
/// evaluation and frees the strings on drop.
pub struct MockKeys(Vec<*mut redis_module::raw::RedisModuleString>);

impl MockKeys {
    pub fn new(names: &[&str]) -> Self {
        // The key-resolution path (`DocTable_GetIdR`) reads the keys through
        // the `RedisModule_StringPtrLen` function pointer — wire it (and the
        // rest of the module API) to the mock implementations.
        redis_mock::init_redis_module_mock();
        Self(
            names
                .iter()
                .map(|name| {
                    redis_mock::string::create_string(name)
                        .cast::<redis_module::raw::RedisModuleString>()
                })
                .collect(),
        )
    }

    /// Placeholder (null) keys for tests that never read them (pre-resolved
    /// doc ids).
    pub fn nulls(n: usize) -> Self {
        Self(vec![std::ptr::null_mut(); n])
    }

    pub fn as_ptr(&self) -> *mut *mut redis_module::raw::RedisModuleString {
        self.0.as_ptr().cast_mut()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl Drop for MockKeys {
    fn drop(&mut self) {
        for &key in &self.0 {
            if !key.is_null() {
                // SAFETY: created by the mock in `new`, freed exactly once here.
                unsafe { redis_mock::string::free_string(key.cast()) };
            }
        }
    }
}
