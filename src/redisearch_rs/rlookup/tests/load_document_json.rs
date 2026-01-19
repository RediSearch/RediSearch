/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![allow(clippy::missing_safety_doc, clippy::undocumented_unsafe_blocks)]

//! Integration tests for [`rlookup::HashFormat`].

//!
//! Coverage gaps deferred to pytest:
//! - `HashDoc::load_field` paths (HashGet is variadic; the redis_mock crate
//!   does not currently provide a HashGet mock — see also
//!   `tests/pytests/test_aggregate.py` for end-to-end RETURN/LOAD coverage).
//! - `force_string` interaction with the `Numeric` flag — exercising it
//!   requires a populated `IndexSpecCache` fixture, since `get_key_load`
//!   strips the flag (`Numeric` is not in `GET_KEY_FLAGS`).

extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

use proptest::prelude::{Strategy, any};
use proptest::proptest;
use redis_module::{KeyType, RedisString};
use rlookup::{
    DocumentFormat, HashFormat, LoadFieldError, RLookup, RLookupKeyFlag, RLookupKeyFlags,
    RLookupRow,
};
use std::ffi::CString;
use std::ptr::NonNull;

/// Build a [`redis_mock::TestContext`] with the given key type and
/// `(field, value)` pairs (which back `RedisModule_ScanKey` iteration), leak
/// it, and return a `NonNull<RedisModuleCtx>` pointing at it.
///
/// Leaking is acceptable for tests: the context is alive for the duration of
/// the test process and the mock APIs assume the pointer remains valid.
fn make_ctx(key_type: KeyType, fields: &[(CString, CString)]) -> NonNull<ffi::RedisModuleCtx> {
    redis_mock::init_redis_module_mock();

    let mut builder = redis_mock::TestContext::builder();
    builder.with_key_type(&key_type);
    for (k, v) in fields {
        builder.inject_key_value(k.clone(), v.clone());
    }
    let ctx = Box::leak(Box::new(builder.build())) as *mut redis_mock::TestContext;
    NonNull::new(ctx.cast::<ffi::RedisModuleCtx>()).unwrap()
}

/// Construct a `RedisString` from a `CString` that the caller keeps alive.
///
/// The mock `RedisModule_CreateString` stores the pointer rather than copying,
/// so `bytes` must outlive the returned `RedisString`.
fn make_redis_string(bytes: &CString) -> RedisString {
    unsafe { RedisString::from_raw_parts(None, bytes.as_ptr(), bytes.as_bytes().len()) }
}

/// Strategy yielding hash-style field/value pairs with **distinct keys**.
///
/// `get_key_load` / `get_key_write` return `None` on duplicate names, so every
/// field-driven test needs unique keys. Generating via `HashMap` bakes the
/// invariant into the strategy itself, so shrinking preserves it instead of
/// fighting a runtime filter.
fn arb_unique_fields() -> impl Strategy<Value = Vec<(CString, CString)>> {
    any::<std::collections::HashMap<CString, CString>>().prop_map(|m| m.into_iter().collect())
}

// ── HashFormat::open ─────────────────────────────────────────────────────

proptest! {}
