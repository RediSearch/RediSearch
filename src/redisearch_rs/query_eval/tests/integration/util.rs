/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared helpers for the query_eval integration tests.

/// Owned mock `RedisModuleString` keys for an id-filter node. The node
/// borrows the pointer array (mirroring production, where the keys are a
/// window into the request's held argv); this owner must outlive the
/// evaluation and frees the strings on drop.
pub struct MockKeys(Vec<*mut redis_module::raw::RedisModuleString>);

impl MockKeys {
    pub fn new(names: &[&str]) -> Self {
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
