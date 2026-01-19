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

proptest! {
    #[test]
    fn open_hash_key_returns_doc(key_name_bytes: CString) {
        let ctx = make_ctx(KeyType::Hash, &[]);
        let format = HashFormat::new(ctx, false);
        let key_name = make_redis_string(&key_name_bytes);

        let _doc = format.open(&key_name).unwrap();
    }

    #[test]
    fn open_empty_key(key_name_bytes: CString) {
        let ctx = make_ctx(KeyType::Empty, &[]);
        let format = HashFormat::new(ctx, false);
        let key_name = make_redis_string(&key_name_bytes);

        let err = format.open(&key_name).unwrap_err();
        assert!(matches!(err, LoadFieldError::WrongHashKeyType));
    }

    #[test]
    fn open_wrong_type(key_name_bytes: CString) {
        let ctx = make_ctx(KeyType::String, &[]);
        let format = HashFormat::new(ctx, false);
        let key_name = make_redis_string(&key_name_bytes);

        let err = format.open(&key_name).unwrap_err();
        assert!(matches!(err, LoadFieldError::WrongHashKeyType));
    }

    // ── HashFormat::load_all ─────────────────────────────────────────────────

    #[test]
    fn load_all_writes_existing_keys(
        key_name_bytes: CString,
        fields in arb_unique_fields(),
    ) {
        let ctx = make_ctx(KeyType::Hash, &fields);
        let format = HashFormat::new(ctx, false);
        let key_name = make_redis_string(&key_name_bytes);

        let mut rlookup = RLookup::new();

        let fields_dstidx: Vec<_> = fields
            .iter()
            .map(|(key, value)| {
                // Use `get_key_load` (not `get_key_write`) — `get_key_write` would mark
                // the keys as `QuerySrc`, which load_all is required to skip.
                let dstidx = rlookup
                    .get_key_load(key.clone(), key.as_c_str(), RLookupKeyFlags::empty())
                    .unwrap()
                    .dstidx;

                (value, dstidx)
            })
            .collect();

        let mut row = RLookupRow::new();
        format
            .load_all(&mut rlookup, &mut row, &key_name)
            .expect("load_all should succeed");


        for (value, dstidx) in fields_dstidx {
            assert_eq!(
                row.dyn_values()[dstidx as usize]
                    .as_ref()
                    .unwrap()
                    .as_str_bytes(),
                Some(value.as_bytes()),
            );
        }
    }

    #[test]
    fn load_all_skips_query_src_keys(
        key_name_bytes: CString,
        fields in arb_unique_fields(),
    ) {
        // Every field is pre-registered by the query (QuerySrc) — load_all must
        // NOT overwrite any of them even though the hash contains all fields.
        let ctx = make_ctx(KeyType::Hash, &fields);
        let format = HashFormat::new(ctx, false);
        let key_name = make_redis_string(&key_name_bytes);

        let mut rlookup = RLookup::new();
        let query_dstidxs: Vec<usize> = fields
            .iter()
            .map(|(field_name, _)| {
                let key = rlookup
                    .get_key_write(field_name.clone(), RLookupKeyFlags::empty())
                    .unwrap();
                assert!(key.flags.contains(RLookupKeyFlag::QuerySrc));
                key.dstidx as usize
            })
            .collect();

        let mut row = RLookupRow::new();
        format.load_all(&mut rlookup, &mut row, &key_name).unwrap();

        for dstidx in query_dstidxs {
            assert!(
                row.dyn_values().get(dstidx).is_none_or(Option::is_none),
                "QuerySrc key should not be written by load_all",
            );
        }
    }

    #[test]
    fn load_all_creates_keys_for_unknown_fields(
        key_name_bytes: CString,
        fields in arb_unique_fields(),
    ) {
        let ctx = make_ctx(KeyType::Hash, &fields);
        let format = HashFormat::new(ctx, false);
        let key_name = make_redis_string(&key_name_bytes);

        let mut rlookup = RLookup::new();
        for (field_name, _) in &fields {
            assert!(rlookup.find_key_by_name(field_name).is_none());
        }

        let mut row = RLookupRow::new();
        format.load_all(&mut rlookup, &mut row, &key_name).unwrap();

        for (field_name, field_value) in &fields {
            let cursor = rlookup
                .find_key_by_name(field_name)
                .expect("load_all should have created the key on the fly");
            let new_key = cursor.current().unwrap();
            // `ForceLoad` is transient and is stripped on persistence; what we
            // expect to see on the freshly-created key are the get_key_load-
            // applied `DocSrc | IsLoaded` flags.
            assert!(
                new_key
                    .flags
                    .contains(RLookupKeyFlag::DocSrc | RLookupKeyFlag::IsLoaded),
                "expected new key to be DocSrc|IsLoaded, got {:?}",
                new_key.flags,
            );
            assert!(
                !new_key.flags.contains(RLookupKeyFlag::QuerySrc),
                "newly-created load key must not be QuerySrc",
            );
            assert_eq!(
                row.dyn_values()[new_key.dstidx as usize]
                    .as_ref()
                    .unwrap()
                    .as_str_bytes(),
                Some(field_value.as_bytes()),
            );
        }
    }
}

#[test]
fn load_all_returns_err_without_setting_status_for_wrong_type() {
    // Mirrors `RLookup_HGETALL` in C, which goes straight to `done` without
    // setting any QueryError on a wrong-type / missing key.
    let ctx = make_ctx(KeyType::String, &[]);
    let format = HashFormat::new(ctx, false);
    let key_name_bytes = CString::new("doc:1").unwrap();
    let key_name = make_redis_string(&key_name_bytes);

    let mut rlookup = RLookup::new();
    let mut row = RLookupRow::new();
    let res = format.load_all(&mut rlookup, &mut row, &key_name);

    assert!(res.is_err());
}
