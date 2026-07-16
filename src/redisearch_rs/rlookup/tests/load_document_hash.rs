/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![allow(clippy::missing_safety_doc, clippy::undocumented_unsafe_blocks)]

extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

use proptest::prelude::{Strategy, any};
use proptest::proptest;
use redis_module::{KeyType, RedisString};
use rlookup::{
    DocumentFormat, FieldSpecBuilder, FieldSpecType, HashDocumentFormat, IndexSpecCache,
    LoadFieldError, RLookup, RLookupKeyFlag, RLookupKeyFlags, RLookupRow,
};
use std::ffi::CString;
use std::ptr::NonNull;

/// Build a [`redis_mock::TestContext`] with the given key type and
/// `(field, value)` pairs (which back `RedisModule_ScanKey` iteration).
///
/// The given context lives only for the duration of the callback.
fn with_ctx<F, R>(key_type: KeyType, fields: &[(CString, CString)], f: F) -> R
where
    F: FnOnce(NonNull<ffi::RedisModuleCtx>) -> R,
{
    redis_mock::init_redis_module_mock();

    let mut builder = redis_mock::TestContext::builder();
    builder.with_key_type(&key_type);
    for (k, v) in fields {
        builder.inject_key_value(k.clone(), v.clone());
    }

    let mut ctx = builder.build();

    f(NonNull::from_mut(&mut ctx).cast::<ffi::RedisModuleCtx>())
}

/// Construct a `RedisString` from a `CString`.
///
/// The mock `RedisModule_CreateString` copies its input, so `bytes` need not
/// outlive the returned `RedisString`.
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

proptest! {
    #[test]
    #[cfg_attr(miri, ignore)] // can't call FFI function RedisModule_CreateString under miri
    fn open_hash_key_returns_doc(key_name_bytes: CString) {
        redis_mock::init_redis_module_mock();

        with_ctx(KeyType::Hash, &[], |ctx| {
            let format = HashDocumentFormat::new(ctx, false);
            let key_name = make_redis_string(&key_name_bytes);

            let _doc = format.open(&key_name).unwrap();
        })
    }

    #[test]
    #[cfg_attr(miri, ignore)] // can't call FFI function RedisModule_CreateString under miri
    fn open_empty_key(key_name_bytes: CString) {
        redis_mock::init_redis_module_mock();

        with_ctx(KeyType::Empty, &[], |ctx| {
            let format = HashDocumentFormat::new(ctx, false);
            let key_name = make_redis_string(&key_name_bytes);

            let err = format.open(&key_name).unwrap_err();
            assert!(matches!(err, LoadFieldError::WrongKeyType));
        })
    }

    #[test]
    #[cfg_attr(miri, ignore)] // can't call FFI function RedisModule_CreateString under miri
    fn open_wrong_type(key_name_bytes: CString) {
        redis_mock::init_redis_module_mock();

        with_ctx(KeyType::String, &[], |ctx| {
            let format = HashDocumentFormat::new(ctx, false);
            let key_name = make_redis_string(&key_name_bytes);

            let err = format.open(&key_name).unwrap_err();
            assert!(matches!(err, LoadFieldError::WrongKeyType));
        })
    }

    #[test]
    #[cfg_attr(miri, ignore)] // can't call FFI function RedisModule_CreateString under miri
    fn load_all_writes_existing_keys(
        key_name_bytes: CString,
        fields in arb_unique_fields(),
    ) {
        redis_mock::init_redis_module_mock();

        with_ctx(KeyType::Hash, &fields, |ctx| {
            let format = HashDocumentFormat::new(ctx, false);
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
        })
    }

    #[test]
    #[cfg_attr(miri, ignore)] // can't call FFI function RedisModule_CreateString under miri
    fn load_all_skips_query_src_keys(
        key_name_bytes: CString,
        fields in arb_unique_fields(),
    ) {
        redis_mock::init_redis_module_mock();

        // Every field is pre-registered by the query (QuerySrc) — load_all must
        // NOT overwrite any of them even though the hash contains all fields.
        with_ctx(KeyType::Hash, &fields, |ctx| {
            let format = HashDocumentFormat::new(ctx, false);
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
        })
    }

    #[test]
    #[cfg_attr(miri, ignore)] // can't call FFI function RedisModule_CreateString under miri
    fn load_all_creates_keys_for_unknown_fields(
        key_name_bytes: CString,
        fields in arb_unique_fields(),
    ) {
        redis_mock::init_redis_module_mock();

        with_ctx(KeyType::Hash, &fields, |ctx| {
            let format = HashDocumentFormat::new(ctx, false);
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
        })
    }
}

#[test]
#[cfg_attr(miri, ignore)] // can't call FFI function RedisModule_CreateString under miri
fn load_all_composes_branches_in_one_pass() {
    redis_mock::init_redis_module_mock();

    // A single scan pass must compose all three callback branches: an existing
    // load key is written, a QuerySrc key is skipped, and an unknown field gets
    // a key created on the fly.
    let fields = [
        (CString::new("load").unwrap(), CString::new("L").unwrap()),
        (CString::new("query").unwrap(), CString::new("Q").unwrap()),
        (CString::new("unknown").unwrap(), CString::new("U").unwrap()),
    ];

    with_ctx(KeyType::Hash, &fields, |ctx| {
        let format = HashDocumentFormat::new(ctx, false);
        let key_name_bytes = CString::new("doc:1").unwrap();
        let key_name = make_redis_string(&key_name_bytes);

        let mut rlookup = RLookup::new();
        let load_dst = rlookup
            .get_key_load(
                fields[0].0.clone(),
                fields[0].0.as_c_str(),
                RLookupKeyFlags::empty(),
            )
            .unwrap()
            .dstidx;
        let query_dst = rlookup
            .get_key_write(fields[1].0.clone(), RLookupKeyFlags::empty())
            .unwrap()
            .dstidx;

        let mut row = RLookupRow::new();
        format.load_all(&mut rlookup, &mut row, &key_name).unwrap();

        // Existing load key -> written.
        assert_eq!(
            row.dyn_values()[load_dst as usize]
                .as_ref()
                .unwrap()
                .as_str_bytes(),
            Some(fields[0].1.as_bytes()),
        );
        // QuerySrc key -> skipped.
        assert!(
            row.dyn_values()
                .get(query_dst as usize)
                .is_none_or(Option::is_none),
            "QuerySrc key should not be written by load_all",
        );
        // Unknown field -> key created on the fly and written.
        let cursor = rlookup
            .find_key_by_name(&fields[2].0)
            .expect("load_all should have created the key on the fly");
        let new_key = cursor.current().unwrap();
        assert_eq!(
            row.dyn_values()[new_key.dstidx as usize]
                .as_ref()
                .unwrap()
                .as_str_bytes(),
            Some(fields[2].1.as_bytes()),
        );
    })
}

#[test]
#[cfg_attr(miri, ignore)] // can't call FFI function RedisModule_CreateString under miri
fn load_all_coerces_numeric_keys_unless_force_string() {
    redis_mock::init_redis_module_mock();

    let fields = [(CString::new("n").unwrap(), CString::new("42.5").unwrap())];

    // `Numeric` cannot be set through a caller flag — both the C `RLOOKUP_GET_KEY_FLAGS`
    // mask and the Rust `GET_KEY_FLAGS` mask strip it. The flag is only ever applied
    // from the schema, so the coercion branch is driven through a numeric field spec.
    let numeric_spec_cache = || {
        IndexSpecCache::from_fields([FieldSpecBuilder::new(fields[0].0.as_c_str())
            .with_types(FieldSpecType::Numeric.into())
            .finish()])
    };

    // force_string = false: a Numeric key coerces the value to a number.
    with_ctx(KeyType::Hash, &fields, |ctx| {
        let format = HashDocumentFormat::new(ctx, false);
        let key_name_bytes = CString::new("doc:1").unwrap();
        let key_name = make_redis_string(&key_name_bytes);

        let mut rlookup = RLookup::new();
        rlookup.set_cache(Some(numeric_spec_cache()));
        let key = rlookup
            .get_key_load(
                fields[0].0.clone(),
                fields[0].0.as_c_str(),
                RLookupKeyFlags::empty(),
            )
            .unwrap();
        assert!(key.flags.contains(RLookupKeyFlag::Numeric));
        let dstidx = key.dstidx;

        let mut row = RLookupRow::new();
        format.load_all(&mut rlookup, &mut row, &key_name).unwrap();

        assert_eq!(
            row.dyn_values()[dstidx as usize].as_ref().unwrap().as_num(),
            Some(42.5),
        );
    });

    // force_string = true: even a Numeric key keeps the raw Redis string.
    with_ctx(KeyType::Hash, &fields, |ctx| {
        let format = HashDocumentFormat::new(ctx, true);
        let key_name_bytes = CString::new("doc:1").unwrap();
        let key_name = make_redis_string(&key_name_bytes);

        let mut rlookup = RLookup::new();
        rlookup.set_cache(Some(numeric_spec_cache()));
        let dstidx = rlookup
            .get_key_load(
                fields[0].0.clone(),
                fields[0].0.as_c_str(),
                RLookupKeyFlags::empty(),
            )
            .unwrap()
            .dstidx;

        let mut row = RLookupRow::new();
        format.load_all(&mut rlookup, &mut row, &key_name).unwrap();

        assert_eq!(
            row.dyn_values()[dstidx as usize]
                .as_ref()
                .unwrap()
                .as_str_bytes(),
            Some(fields[0].1.as_bytes()),
        );
    })
}

/// `DocumentFormat::borrow` must not take ownership of the caller's key handle:
/// dropping the loader must leave the handle open for the caller to close. This
/// is the invariant that lets the AsyncScan `key_cb` reuse its pinned handle.
#[test]
#[cfg_attr(miri, ignore)] // can't call FFI function RedisModule_CreateString under miri
fn borrow_does_not_close_caller_handle() {
    redis_mock::init_redis_module_mock();

    let key_name_bytes = CString::new("doc:1").unwrap();
    with_ctx(KeyType::Hash, &[], |ctx| {
        let format = HashDocumentFormat::new(ctx, false);
        let key_name = make_redis_string(&key_name_bytes);

        // Open a raw handle ourselves so we (not the loader) own it.
        // Safety: the mock accepts any mode; `key_name.inner` is a mock string.
        let raw_key = unsafe {
            redis_module::raw::RedisModule_OpenKey.unwrap()(ctx.cast().as_ptr(), key_name.inner, 0)
        };

        {
            // Safety: `raw_key` is a valid, open handle that outlives the loader.
            let open_key = unsafe { raw_key.cast::<ffi::RedisModuleKey>().as_ref().unwrap() };
            let _loader = format.borrow(open_key, &key_name).unwrap();
            // `_loader` is dropped here; a borrowed handle must NOT be closed.
        }

        // We still own the handle, so we can close it exactly once. A double close
        // (had `borrow` taken ownership) would abort the process.
        // Safety: `raw_key` was opened above and has not been closed.
        unsafe { redis_module::raw::RedisModule_CloseKey.unwrap()(raw_key) };
    })
}
