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
    F: FnOnce(NonNull<redis_module::RedisModuleCtx>) -> R,
{
    redis_mock::init_redis_module_mock();

    let mut builder = redis_mock::TestContext::builder();
    builder.with_key_type(&key_type);
    for (k, v) in fields {
        builder.inject_key_value(k.clone(), v.clone());
    }

    let mut ctx = builder.build();

    f(NonNull::from_mut(&mut ctx).cast::<redis_module::RedisModuleCtx>())
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
            let open_key = unsafe {
                raw_key
                    .cast::<redis_module::RedisModuleKey>()
                    .as_ref()
                    .unwrap()
            };
            let _loader = format.borrow(open_key, &key_name).unwrap();
            // `_loader` is dropped here; a borrowed handle must NOT be closed.
        }

        // We still own the handle, so we can close it exactly once. A double close
        // (had `borrow` taken ownership) would abort the process.
        // Safety: `raw_key` was opened above and has not been closed.
        unsafe { redis_module::raw::RedisModule_CloseKey.unwrap()(raw_key) };
    })
}

/// `load_field` fetches one field per lookup key through `RedisModule_HashGet`, resolving
/// the field name through the loader's [`HashFieldNames`] cache.
mod load_field {
    use super::*;
    use rlookup::{FieldLoader, HashFieldNames, RLookupKey};

    fn field(name: &str, value: &str) -> (CString, CString) {
        (CString::new(name).unwrap(), CString::new(value).unwrap())
    }

    fn load_one<'a>(
        format: &HashDocumentFormat<'_>,
        key_name: &RedisString,
        key: &RLookupKey<'a>,
        row: &mut RLookupRow<'a>,
    ) {
        format
            .open(key_name)
            .expect("hash key opens")
            .load_field(key, row)
            .expect("load_field succeeds");
    }

    #[test]
    #[cfg_attr(miri, ignore)] // can't call FFI function RedisModule_CreateString under miri
    fn loads_present_field_and_leaves_missing_slot_empty() {
        let fields = [field("title", "hello"), field("body", "world")];
        with_ctx(KeyType::Hash, &fields, |ctx| {
            let format = HashDocumentFormat::new(ctx, false);
            let key_name = make_redis_string(&CString::new("doc:1").unwrap());
            let mut rlookup = RLookup::new();
            let title = rlookup
                .get_key_load(c"title", c"title", RLookupKeyFlags::empty())
                .unwrap()
                .dstidx;
            let missing = rlookup
                .get_key_load(c"missing", c"missing", RLookupKeyFlags::empty())
                .unwrap()
                .dstidx;
            let mut row = RLookupRow::new();
            for name in [c"title", c"missing"] {
                let key = rlookup
                    .get_key_read(name, RLookupKeyFlags::empty())
                    .unwrap();
                load_one(&format, &key_name, key, &mut row);
            }
            assert_eq!(
                row.dyn_values()[title as usize]
                    .as_ref()
                    .unwrap()
                    .as_str_bytes(),
                Some(b"hello".as_slice())
            );
            assert!(
                row.dyn_values()
                    .get(missing as usize)
                    .is_none_or(Option::is_none)
            );
        })
    }

    #[test]
    #[cfg_attr(miri, ignore)] // can't call FFI function RedisModule_CreateString under miri
    fn resolves_the_key_path_not_its_name() {
        let fields = [field("price_usd", "12.5")];
        with_ctx(KeyType::Hash, &fields, |ctx| {
            let format = HashDocumentFormat::new(ctx, false);
            let key_name = make_redis_string(&CString::new("doc:1").unwrap());
            let mut rlookup = RLookup::new();
            // The key is named `price` but loads the hash field `price_usd`.
            let dstidx = rlookup
                .get_key_load(c"price", c"price_usd", RLookupKeyFlags::empty())
                .unwrap()
                .dstidx;
            let mut row = RLookupRow::new();
            let key = rlookup
                .get_key_read(c"price", RLookupKeyFlags::empty())
                .unwrap();
            load_one(&format, &key_name, key, &mut row);
            assert_eq!(
                row.dyn_values()[dstidx as usize]
                    .as_ref()
                    .unwrap()
                    .as_str_bytes(),
                Some(b"12.5".as_slice())
            );
        })
    }

    #[test]
    #[cfg_attr(miri, ignore)] // can't call FFI function RedisModule_CreateString under miri
    fn coerces_numeric_schema_fields() {
        let fields = [field("n", "42.5")];
        with_ctx(KeyType::Hash, &fields, |ctx| {
            let format = HashDocumentFormat::new(ctx, false);
            let key_name = make_redis_string(&CString::new("doc:1").unwrap());
            let mut rlookup = RLookup::new();
            rlookup.set_cache(Some(IndexSpecCache::from_fields([FieldSpecBuilder::new(
                c"n",
            )
            .with_types(FieldSpecType::Numeric.into())
            .finish()])));
            let key = rlookup
                .get_key_load(c"n", c"n", RLookupKeyFlags::empty())
                .unwrap();
            assert!(key.flags.contains(RLookupKeyFlag::Numeric));
            let dstidx = key.dstidx;
            let mut row = RLookupRow::new();
            let key = rlookup
                .get_key_read(c"n", RLookupKeyFlags::empty())
                .unwrap();
            load_one(&format, &key_name, key, &mut row);
            assert_eq!(
                row.dyn_values()[dstidx as usize].as_ref().unwrap().as_num(),
                Some(42.5)
            );
        })
    }

    #[test]
    #[cfg_attr(miri, ignore)] // can't call FFI function RedisModule_CreateString under miri
    fn underscore_key_loads_the_document_key_name() {
        with_ctx(KeyType::Hash, &[], |ctx| {
            let format = HashDocumentFormat::new(ctx, false);
            let key_name = make_redis_string(&CString::new("doc:42").unwrap());
            let mut rlookup = RLookup::new();
            let dstidx = rlookup
                .get_key_load(c"__key", c"__key", RLookupKeyFlags::empty())
                .unwrap()
                .dstidx;
            let mut row = RLookupRow::new();
            let key = rlookup
                .get_key_read(c"__key", RLookupKeyFlags::empty())
                .unwrap();
            load_one(&format, &key_name, key, &mut row);
            assert_eq!(
                row.dyn_values()[dstidx as usize]
                    .as_ref()
                    .unwrap()
                    .as_str_bytes(),
                Some(b"doc:42".as_slice())
            );
        })
    }

    #[test]
    #[cfg_attr(miri, ignore)] // can't call FFI function RedisModule_CreateString under miri
    fn shared_field_names_are_built_once_per_key_across_documents() {
        let fields = [field("a", "1"), field("b", "2")];
        with_ctx(KeyType::Hash, &fields, |ctx| {
            let names = HashFieldNames::new();
            let format = HashDocumentFormat::new(ctx, false).with_field_names(&names);
            let mut rlookup = RLookup::new();
            for name in [c"a", c"b"] {
                rlookup
                    .get_key_load(name, name, RLookupKeyFlags::empty())
                    .unwrap();
            }
            assert!(names.is_empty());
            for doc in ["doc:1", "doc:2", "doc:3"] {
                let key_name = make_redis_string(&CString::new(doc).unwrap());
                let mut row = RLookupRow::new();
                for name in [c"a", c"b"] {
                    let key = rlookup
                        .get_key_read(name, RLookupKeyFlags::empty())
                        .unwrap();
                    load_one(&format, &key_name, key, &mut row);
                }
                assert_eq!(
                    row.dyn_values()[1].as_ref().unwrap().as_str_bytes(),
                    Some(b"2".as_slice())
                );
            }
            // One name per key, however many documents were loaded.
            assert_eq!(names.len(), 2);
        })
    }

    #[test]
    #[cfg_attr(miri, ignore)] // can't call FFI function RedisModule_CreateString under miri
    fn field_names_fill_out_of_order_without_disturbing_earlier_slots() {
        let fields = [field("a", "1"), field("b", "2"), field("c", "3")];
        with_ctx(KeyType::Hash, &fields, |ctx| {
            let names = HashFieldNames::new();
            let format = HashDocumentFormat::new(ctx, false).with_field_names(&names);
            let key_name = make_redis_string(&CString::new("doc:1").unwrap());
            let mut rlookup = RLookup::new();
            for name in [c"a", c"b", c"c"] {
                rlookup
                    .get_key_load(name, name, RLookupKeyFlags::empty())
                    .unwrap();
            }
            let mut row = RLookupRow::new();
            // Highest slot first, then the lowest, then the highest again: the hole at
            // slot 1 stays empty and slot 2 is not rebuilt.
            for name in [c"c", c"a", c"c"] {
                let key = rlookup
                    .get_key_read(name, RLookupKeyFlags::empty())
                    .unwrap();
                load_one(&format, &key_name, key, &mut row);
            }
            assert_eq!(names.len(), 2);
            assert_eq!(
                row.dyn_values()[0].as_ref().unwrap().as_str_bytes(),
                Some(b"1".as_slice())
            );
            assert!(row.dyn_values()[1].is_none());
            assert_eq!(
                row.dyn_values()[2].as_ref().unwrap().as_str_bytes(),
                Some(b"3".as_slice())
            );
        })
    }
}
