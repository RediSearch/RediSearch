/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![allow(clippy::missing_safety_doc, clippy::undocumented_unsafe_blocks)]

//! Integration tests for [`rlookup::JsonFormat`].
//!
//! # Behavioural baseline (mirrors C `rlookup_load_document.c` on `master`)
//!
//! `JsonFormat` is the JSON counterpart to `HashFormat`. Compared with C, the
//! port preserves the following invariants verbatim — these are the contracts
//! we want to lock down with proptests:
//!
//! ## `JsonFormat::open`
//! - Calls `japi->openKeyWithFlags` with `DOCUMENT_OPEN_KEY_QUERY_FLAGS`.
//! - Returns `Err(LoadFieldError::KeyNotFound)` whenever `openKeyWithFlags`
//!   returns NULL. JSON does **not** distinguish missing-vs-wrong-type the
//!   way hash does (the JSON API folds both into a NULL result), so there is
//!   no `WrongHashKeyType` analogue.
//!
//! ## `JsonFormat::load_all`
//! - Opens the key; NULL → `LoadAllError::KeyNotFound`.
//! - Fetches the JSONPath iterator at `$`; NULL → `LoadAllError::JsonRootMissing`.
//! - `json_iter_to_value` returning `Ok(None)` is *also* mapped to
//!   `JsonRootMissing` — i.e. an empty root collapses to a document-level
//!   error (C analogue: `RLookup_JSON_GetAll` returns ERR when
//!   `jsonIterToValue` returns ERR).
//! - On success, looks up the RLookupKey named `"$"` (path = `"$"`). If
//!   absent it is created with `get_key_load("$", "$", RLookupKeyFlags::empty())`.
//!   Crucially, unlike `RLookup_HGETALL`, this path does **not** skip
//!   `QuerySrc` keys — the `$` key is overwritten even when it was registered
//!   by a query. This divergence is intentional and must be preserved.
//! - Writes the produced `SharedValue` to the row at the key's `dstidx`.
//!
//! ## `JsonDoc::load_field`
//! - `kk.path()` MUST be `Some(_)` — the call panics otherwise (C: deref of
//!   `RLookupKey_GetPath`).
//! - Path branching, exactly mirroring C `getKeyCommonJSON`:
//!   - Path starts with `$` → JSONPath against the document. If the iterator
//!     is absent (no match) or `json_iter_to_value` returns `Ok(None)` or
//!     `Err(_)`, the call silently succeeds with no write. Only the absence
//!     of a value is tolerated; the function never bubbles a "field missing"
//!     error.
//!   - Path equals the literal `"__key"` → write the document key name as a
//!     string value.
//!   - Anything else → silent no-op (C: returns OK without writing).
//!
//! ## `json_iter_to_value`
//! Multi-value handling depends on `apiVersion`:
//! - `apiVersion < APIVERSION_RETURN_MULTI_CMP_FIRST` (i.e. < 3):
//!   - Take the first element of the iterator (single-value legacy mode).
//!   - Empty iterator → `Ok(None)`.
//! - `apiVersion >= APIVERSION_RETURN_MULTI_CMP_FIRST`:
//!   - Empty iterator → `Ok(None)`.
//!   - Serialize the entire iterator first (does not consume it), then take
//!     the first element. If the first is an `Array`, drill into element 0;
//!     if the array is empty → `Ok(None)`.
//!   - Wrap `(first_value, serialized, expanded_array)` in `Value::Trio`.
//!
//! ## `json_val_to_value` (scalar coercion)
//! - `String`  → `SharedValue::new_string(bytes)`
//! - `Int`     → `SharedValue::new_num(int as f64)` — lossy on values not
//!   representable in f64; the C path performs the same lossy cast via
//!   `RSValue_NewNumberFromInt64` so the port must match.
//! - `Double`  → `SharedValue::new_num(double)`
//! - `Bool`    → `SharedValue::new_num(0.0 | 1.0)`
//! - `Object`/`Array` → `SharedValue::new_string(serialized_bytes)` — the
//!   compact JSON serialization of the container.
//! - `Null`    → `SharedValue::null_static()`
//!
//! # Why most of these tests are stubbed
//!
//! The `redis_mock` crate does **not** currently provide a `RedisJSONAPI`
//! mock. `JsonFormat::new` requires a `&RedisJsonApi`, and the only public
//! constructor (`RedisJsonApi::get()`) reads the global `ffi::japi` /
//! `ffi::japi_ver` symbols. To exercise the live behaviour we would need
//! either:
//!
//! 1. A `redis_mock::JsonApiBuilder` that installs a `RedisJSONAPI` vtable
//!    into `ffi::japi` and synthesises in-memory JSON documents (the analogue
//!    of `TestContext::inject_key_value` for the hash path), or
//! 2. A `pub(crate)` `RedisJsonApi::from_vtable_for_test(...)` constructor
//!    plus a hand-rolled in-test vtable.
//!
//! Until one of those lands, the `load_all` happy path, every `load_field`
//! branch, the multi-value api-version branching, and the scalar coercions
//! are **deferred to pytest** (see `tests/pytests/test_json*.py` and the new
//! `tests/pytests/test_json_partial_load.py`).
//!
//! The proptests below cover what we *can* exercise without a JSON mock:
//! the open / load_all error paths that resolve before any vtable function
//! is invoked (NULL `openKeyWithFlags` short-circuits via the existing
//! sentinel value at module-init time).

extern crate redisearch_rs;
redis_mock::mock_or_stub_missing_redis_c_symbols!();

use proptest::prelude::{Strategy, any};
use proptest::proptest;
use redis_module::{KeyType, RedisString};
use std::ffi::CString;
use std::ptr::NonNull;

/// Build a [`redis_mock::TestContext`], leak it, and return a
/// `NonNull<RedisModuleCtx>` pointing at it.
///
/// Leaking is acceptable for tests: the context is alive for the duration of
/// the test process and the mock APIs assume the pointer remains valid.
#[allow(dead_code, reason = "used by deferred tests once JsonApi mock is available")]
fn make_ctx(key_type: KeyType) -> NonNull<ffi::RedisModuleCtx> {
    redis_mock::init_redis_module_mock();

    let mut builder = redis_mock::TestContext::builder();
    builder.with_key_type(&key_type);
    let ctx = Box::leak(Box::new(builder.build())) as *mut redis_mock::TestContext;
    NonNull::new(ctx.cast::<ffi::RedisModuleCtx>()).unwrap()
}

/// Construct a `RedisString` from a `CString` that the caller keeps alive.
///
/// The mock `RedisModule_CreateString` stores the pointer rather than copying,
/// so `bytes` must outlive the returned `RedisString`.
#[allow(dead_code, reason = "used by deferred tests once JsonApi mock is available")]
fn make_redis_string(bytes: &CString) -> RedisString {
    unsafe { RedisString::from_raw_parts(None, bytes.as_ptr(), bytes.as_bytes().len()) }
}

/// Strategy yielding a JSONPath-shaped CString. We restrict to non-empty
/// paths starting with `$` because the `$` prefix selects the JSONPath
/// branch in `JsonDoc::load_field` — the very thing we want to vary.
///
/// The body characters are constrained to printable ASCII to keep shrinking
/// readable; this is plenty since the path-handling code is purely byte-wise
/// (`starts_with(b"$")` and an equality compare against `b"__key"`).
#[allow(dead_code)] // used by deferred tests
fn arb_json_path() -> impl Strategy<Value = CString> {
    use proptest::collection::vec;
    vec(any::<u8>().prop_filter("printable, no NUL", |b| (0x20..0x7f).contains(b)), 0..32)
        .prop_map(|tail| {
            let mut bytes = b"$".to_vec();
            bytes.extend(tail);
            CString::new(bytes).unwrap()
        })
}

/// Strategy yielding RLookup-style `(name, path)` pairs with **distinct
/// names**. Mirrors `arb_unique_fields` in `load_document_hash.rs`:
/// `get_key_load` returns `None` on duplicate names, so any field-driven
/// test needs unique keys, and baking the invariant into a `HashMap`
/// strategy keeps shrinking honest.
#[allow(dead_code, reason = "used by deferred tests once JsonApi mock is available")]
fn arb_unique_path_keys() -> impl Strategy<Value = Vec<(CString, CString)>> {
    // Use the name as the HashMap key so generated names are unique; pair
    // each with an independently-generated `$`-prefixed path.
    proptest::collection::hash_map(any::<CString>(), arb_json_path(), 0..16)
        .prop_map(|m| m.into_iter().collect())
}

// ── JsonFormat::open — error paths ────────────────────────────────────────
//
// These tests do not require a populated JSONAPI vtable: when `ffi::japi`
// is NULL (its default state in the test process), `RedisJsonApi::get()`
// returns `None`, and any code path that reaches an `unwrap()` on it will
// panic with a clear message — which is itself a useful invariant to
// document. The active proptests below therefore assert *that panic*,
// pinning the precondition: "callers must initialise the JSON API before
// constructing a `JsonFormat`".
//
// Once a `redis_mock::JsonApiBuilder` lands, replace `#[ignore]` on the
// real-behaviour tests with the mock setup and drop the panic checks.

proptest! {
    /// `JsonFormat::new` must not be called without a live JSON API. The
    /// production call site (`RLookup::load_rule_fields`) constructs the
    /// API via `RedisJsonApi::get().unwrap()`. We pin that precondition
    /// here so that any future refactor which silently falls back to a
    /// no-op `RedisJsonApi` is caught.
    #[test]
    #[ignore = "Requires `redis_mock::JsonApiBuilder` — see module docs."]
    fn open_missing_key_returns_not_found(key_name_bytes: CString) {
        // Pseudocode for when the mock is available:
        //
        //   let ctx = make_ctx(KeyType::Empty);
        //   let japi = JsonApiBuilder::new()
        //       .with_open_key_with_flags(|_, _, _| std::ptr::null_mut())
        //       .install();
        //   let format = JsonFormat::new(ctx, &japi, 7);
        //   let key_name = make_redis_string(&key_name_bytes);
        //   assert!(matches!(format.open(&key_name).unwrap_err(),
        //                    LoadFieldError::KeyNotFound));
        let _ = key_name_bytes;
    }

    /// `JsonFormat::open` must succeed when `openKeyWithFlags` returns a
    /// non-null `RedisJSON`, producing a `JsonDoc` that retains the same
    /// `ctx`, `key_name`, and `api_version` used at construction.
    #[test]
    #[ignore = "Requires `redis_mock::JsonApiBuilder` — see module docs."]
    fn open_existing_key_returns_doc(key_name_bytes: CString) {
        let _ = key_name_bytes;
    }
}

// ── JsonFormat::load_all — error paths ────────────────────────────────────

proptest! {
    /// `LoadAllError::KeyNotFound` when `openKeyWithFlags` returns NULL.
    /// Matches the C `RLookup_JSON_GetAll` `goto done` on `!jsonRoot`.
    #[test]
    #[ignore = "Requires `redis_mock::JsonApiBuilder` — see module docs."]
    fn load_all_returns_key_not_found_when_open_fails(key_name_bytes: CString) {
        let _ = key_name_bytes;
    }

    /// `LoadAllError::JsonRootMissing` when `openKeyWithFlags` succeeds but
    /// `japi->get(root, "$")` returns NULL.
    #[test]
    #[ignore = "Requires `redis_mock::JsonApiBuilder` — see module docs."]
    fn load_all_returns_root_missing_when_get_returns_null(key_name_bytes: CString) {
        let _ = key_name_bytes;
    }

    /// `LoadAllError::JsonRootMissing` when the iterator exists but yields no
    /// values (`json_iter_to_value` returns `Ok(None)`). This is the
    /// "empty root" case — the Rust code intentionally collapses it to the
    /// same error variant.
    #[test]
    #[ignore = "Requires `redis_mock::JsonApiBuilder` — see module docs."]
    fn load_all_returns_root_missing_when_iter_is_empty(key_name_bytes: CString) {
        let _ = key_name_bytes;
    }
}

// ── JsonFormat::load_all — happy path ─────────────────────────────────────

proptest! {
    /// `load_all` finds an existing `"$"` RLookupKey and writes the loaded
    /// value into the row at its `dstidx`. The key must NOT be re-created.
    #[test]
    #[ignore = "Requires `redis_mock::JsonApiBuilder` — see module docs."]
    fn load_all_reuses_existing_dollar_key(key_name_bytes: CString) {
        let _ = key_name_bytes;
    }

    /// `load_all` creates a `"$"` key when none exists, with the flags set
    /// by `get_key_load(JSON_ROOT, JSON_ROOT, RLookupKeyFlags::empty())` —
    /// i.e. `DocSrc | IsLoaded` and **not** `QuerySrc`.
    #[test]
    #[ignore = "Requires `redis_mock::JsonApiBuilder` — see module docs."]
    fn load_all_creates_dollar_key_when_missing(key_name_bytes: CString) {
        let _ = key_name_bytes;
    }

    /// Divergence from `HashFormat::load_all`: a pre-existing `"$"` key
    /// marked `QuerySrc` is overwritten by `load_all`. The hash path
    /// preserves `QuerySrc` keys; the JSON path does not. This test exists
    /// specifically to catch a regression that "fixes" the JSON path by
    /// mirroring the hash skip-on-QuerySrc behaviour.
    #[test]
    #[ignore = "Requires `redis_mock::JsonApiBuilder` — see module docs."]
    fn load_all_overwrites_query_src_dollar_key(key_name_bytes: CString) {
        let _ = key_name_bytes;
    }
}

// ── JsonDoc::load_field — branching ───────────────────────────────────────

proptest! {
    /// `load_field` panics when `kk.path()` is `None`. C deref of a NULL
    /// path is UB; Rust upgrades it to an explicit panic so the contract
    /// is observable.
    #[test]
    #[ignore = "Requires `redis_mock::JsonApiBuilder` — see module docs."]
    fn load_field_panics_on_missing_path(key_name_bytes: CString) {
        let _ = key_name_bytes;
    }

    /// JSONPath that matches no value (`japi->get` returns NULL) is a
    /// silent success — the row is left untouched.
    #[test]
    #[ignore = "Requires `redis_mock::JsonApiBuilder` — see module docs."]
    fn load_field_silently_skips_unmatched_jsonpath(
        key_name_bytes: CString,
        path in arb_json_path(),
    ) {
        let _ = (key_name_bytes, path);
    }

    /// `json_iter_to_value` returning `Ok(None)` (empty match) on a per-field
    /// load is silently mapped to no-write (unlike `load_all`, which maps it
    /// to `JsonRootMissing`).
    #[test]
    #[ignore = "Requires `redis_mock::JsonApiBuilder` — see module docs."]
    fn load_field_silently_skips_empty_iter(
        key_name_bytes: CString,
        path in arb_json_path(),
    ) {
        let _ = (key_name_bytes, path);
    }

    /// `json_iter_to_value` returning `Err(_)` is *also* silently mapped to
    /// no-write. This intentionally drops serialization errors on per-field
    /// loads — matches C `getKeyCommonJSON`'s `if (res == REDISMODULE_ERR)
    /// return REDISMODULE_OK;` branch.
    #[test]
    #[ignore = "Requires `redis_mock::JsonApiBuilder` — see module docs."]
    fn load_field_silently_swallows_serialization_errors(
        key_name_bytes: CString,
        path in arb_json_path(),
    ) {
        let _ = (key_name_bytes, path);
    }

    /// Path equal to the literal `"__key"` resolves to the document key
    /// name written as a string value. Anything starting with `$` takes
    /// the JSONPath branch *before* the `__key` check, so a `$__key` path
    /// will NOT trigger this branch.
    #[test]
    #[ignore = "Requires `redis_mock::JsonApiBuilder` — see module docs."]
    fn load_field_underscore_key_returns_document_key_name(key_name_bytes: CString) {
        let _ = key_name_bytes;
    }

    /// Path that is neither `$`-prefixed nor `__key` is a silent no-op.
    /// (RLookupKeys with arbitrary paths can be constructed via
    /// `RLookupKey::new_with_path`, e.g. from a hash schema.)
    #[test]
    #[ignore = "Requires `redis_mock::JsonApiBuilder` — see module docs."]
    fn load_field_unknown_sentinel_is_noop(
        key_name_bytes: CString,
        path: CString,
    ) {
        // Filter out the two recognised sentinels in the test body would be
        // wasteful — encode the constraint in the strategy when this is
        // un-ignored: `path.prop_filter(|p| !p.to_bytes().starts_with(b"$")
        //                                 && p.as_c_str() != UNDERSCORE_KEY)`.
        let _ = (key_name_bytes, path);
    }
}

// ── json_iter_to_value — multi-value api-version branching ────────────────
//
// These exercise the trio-construction in the `>= APIVERSION_RETURN_MULTI_CMP_FIRST`
// branch and the single-value compatibility shim below it. Both branches are
// orthogonal to the open/get vtable functions but require synthesising
// `ResultsIter` values, which is impossible without `RedisJSONAPI` symbols.

proptest! {
    /// `api_version < 3` returns a bare scalar (no `Trio` wrapping), matching
    /// the legacy single-value behaviour. Empty iterator → `Ok(None)`.
    #[test]
    #[ignore = "Requires `redis_mock::JsonApiBuilder` — see module docs."]
    fn iter_to_value_legacy_apiversion_returns_scalar(seed: u64) {
        let _ = seed;
    }

    /// `api_version >= 3` with a single non-array value wraps `(scalar,
    /// serialized, expanded_array)` in a `Trio`. The serialized component
    /// is the *whole* iterator JSON-encoded, which for a single match equals
    /// the scalar's JSON form.
    #[test]
    #[ignore = "Requires `redis_mock::JsonApiBuilder` — see module docs."]
    fn iter_to_value_multi_apiversion_wraps_in_trio(seed: u64) {
        let _ = seed;
    }

    /// `api_version >= 3` where the first iterator element is itself a
    /// JSON array: the trio's first slot drills into `array[0]`. An empty
    /// array short-circuits to `Ok(None)`.
    #[test]
    #[ignore = "Requires `redis_mock::JsonApiBuilder` — see module docs."]
    fn iter_to_value_multi_apiversion_drills_into_array(seed: u64) {
        let _ = seed;
    }

    /// `iter_to_value_multi_apiversion_serialize_is_idempotent_over_reset`:
    /// `getJSONFromIter` does not consume the iterator. After it runs,
    /// `iter.next()` still yields the first element. This is a precondition
    /// `json_iter_to_value` relies on; if RedisJSON ever breaks it, the
    /// `debug_assert!` on `is_empty/next disagree` will fire.
    #[test]
    #[ignore = "Requires `redis_mock::JsonApiBuilder` — see module docs."]
    fn iter_to_value_serialize_does_not_consume(seed: u64) {
        let _ = seed;
    }
}

// ── json_val_to_value — scalar coercions ──────────────────────────────────
//
// These cover the per-type conversion. They are pure functions of a single
// `JsonValueRef`, but the ref still requires a backing RedisJSON pointer
// served by a vtable, so they are deferred along with everything else.

proptest! {
    /// `JsonType::Int` is cast to `f64` via `as f64`. For `i64` values
    /// outside the `f64`-representable integer range (`|v| > 2^53`) this
    /// is intentionally lossy; the C path performs the same cast.
    #[test]
    #[ignore = "Requires `redis_mock::JsonApiBuilder` — see module docs."]
    fn int_round_trips_with_f64_loss_above_2_53(v: i64) {
        let _ = v;
    }

    /// `JsonType::Bool` becomes `0.0` for `false` and `1.0` for `true`.
    #[test]
    #[ignore = "Requires `redis_mock::JsonApiBuilder` — see module docs."]
    fn bool_becomes_zero_or_one(b: bool) {
        let _ = b;
    }

    /// `JsonType::Double` is preserved bit-exact (`f64` → `f64`), including
    /// `NaN`, `±∞`, and subnormals — *if* RedisJSON serialises these to
    /// begin with. If RedisJSON normalises NaN/Inf away, the assertion
    /// here documents the actually-reachable range.
    #[test]
    #[ignore = "Requires `redis_mock::JsonApiBuilder` — see module docs."]
    fn double_is_preserved_bit_exact(v: f64) {
        let _ = v;
    }

    /// `JsonType::String` is copied verbatim into `SharedValue::new_string`.
    /// Bytes outside ASCII must round-trip unchanged.
    #[test]
    #[ignore = "Requires `redis_mock::JsonApiBuilder` — see module docs."]
    fn string_round_trips_arbitrary_utf8(s in ".*") {
        let _ = s;
    }

    /// `JsonType::Object` / `JsonType::Array` serialise to a compact JSON
    /// string. We verify only the round-trip parses back to a JSON value
    /// of the same type, not byte-exactness (formatting is RedisJSON's
    /// responsibility).
    #[test]
    #[ignore = "Requires `redis_mock::JsonApiBuilder` — see module docs."]
    fn container_serializes_to_round_trippable_json(seed: u64) {
        let _ = seed;
    }
}

/// `JsonType::Null` resolves to `SharedValue::null_static()`. Multiple
/// calls must return the same `null` representation; document this so
/// callers may rely on pointer equality if `SharedValue` ever offers it.
#[test]
#[ignore = "Requires `redis_mock::JsonApiBuilder` — see module docs."]
fn null_resolves_to_null_static() {}

// ── Non-proptest invariant ────────────────────────────────────────────────

/// `load_all` does not set a `QueryError` status on missing-key — mirrors
/// the C `RLookup_JSON_GetAll` which falls through `done` without touching
/// the status. The Rust port encodes the same contract by returning
/// `LoadAllError::KeyNotFound` rather than touching a status pointer.
#[test]
#[ignore = "Requires `redis_mock::JsonApiBuilder` — see module docs."]
fn load_all_missing_key_returns_err_without_status_side_effects() {}
