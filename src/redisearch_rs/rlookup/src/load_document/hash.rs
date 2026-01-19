/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::load_document::{
    DOCUMENT_OPEN_KEY_QUERY_FLAGS, DocumentFormat, LoadAllError, LoadFieldError, OpenDocument,
    UNDERSCORE_KEY,
};
use crate::{RLookup, RLookupKey, RLookupKeyFlag, RLookupRow};
use redis_module::RedisString;
use redis_module::key::RedisKey;
use redis_module::{KeyType, ScanKeyCursor, key::KeyFlags};
use std::ffi::CStr;
use std::ptr::NonNull;
use value::SharedValue;

pub struct HashFormat {
    ctx: NonNull<ffi::RedisModuleCtx>,
    force_string: bool,
}

#[derive(Debug)]
pub struct HashDoc<'a> {
    key: RedisKey,
    key_name: &'a RedisString,
}

/// Why opening a hash key failed.
enum HashOpenError {
    /// `RedisModule_OpenKey` returned NULL — no document at this key.
    NotFound,
    /// The key exists but is not a hash.
    WrongType,
}

impl HashFormat {
    pub const fn new(ctx: NonNull<ffi::RedisModuleCtx>, force_string: bool) -> Self {
        Self { ctx, force_string }
    }

    /// Open `key_name` and verify it points to a hash.
    fn open_hash_key(&self, key_name: &RedisString) -> Result<RedisKey, HashOpenError> {
        let key = RedisKey::open_with_flags(
            self.ctx.cast().as_ptr(),
            key_name,
            DOCUMENT_OPEN_KEY_QUERY_FLAGS,
        );

        if key.is_null() {
            return Err(HashOpenError::NotFound);
        }
        if key.key_type() != KeyType::Hash {
            return Err(HashOpenError::WrongType);
        }
        Ok(key)
    }
}

impl DocumentFormat for HashFormat {
    type Document<'a> = HashDoc<'a>;

    fn open<'key>(
        &'key self,
        key_name: &'key RedisString,
    ) -> Result<Self::Document<'key>, LoadFieldError> {
        let key = self.open_hash_key(key_name).map_err(|e| match e {
            HashOpenError::NotFound => LoadFieldError::KeyNotFound,
            HashOpenError::WrongType => LoadFieldError::WrongHashKeyType,
        })?;

        Ok(HashDoc { key, key_name })
    }

    fn load_all(
        &self,
        rlookup: &mut RLookup,
        dst_row: &mut RLookupRow,
        key_name: &RedisString,
    ) -> Result<(), LoadAllError> {
        // Matches `RLookup_HGETALL` in C: returns ERR without setting `QueryError`
        // when the key is missing or has the wrong type.
        let key = self
            .open_hash_key(key_name)
            .map_err(|_| LoadAllError::KeyNotFound)?;

        let scan_cursor = ScanKeyCursor::new(key);

        scan_cursor.for_each(|_key, field, value| {
            let (field_ptr, _field_len) = field.as_cstr_ptr_and_len();

            // SAFETY: `RedisModule_StringPtrLen` returns a pointer to a null-terminated
            // SDS string. `CStr::from_ptr` reads until the first null byte.
            let field_cstr = unsafe { CStr::from_ptr(field_ptr) };

            let key = if let Some(c) = rlookup.find_key_by_name(field_cstr) {
                if c.current()
                    .unwrap()
                    .flags
                    .contains(RLookupKeyFlag::QuerySrc)
                {
                    // Key name is already taken by a query key.
                    return;
                } else {
                    c.into_current().unwrap()
                }
            } else {
                // First returned document, create the key.
                rlookup
                    .get_key_load(
                        field_cstr.to_owned(),
                        field_cstr,
                        RLookupKeyFlag::ForceLoad.into(),
                    )
                    .unwrap()
            };

            let coerce = if key.flags.contains(RLookupKeyFlag::Numeric) && !self.force_string {
                HashValueCoerce::Dbl
            } else {
                HashValueCoerce::Str
            };
            let value = hval_to_value(value, coerce);

            dst_row.write_key(key, value);
        });

        Ok(())
    }
}

impl OpenDocument for HashDoc<'_> {
    fn load_field(&self, kk: &RLookupKey, dst_row: &mut RLookupRow) -> Result<(), LoadFieldError> {
        let path = match kk.path() {
            Some(p) => p.as_ref(),
            // No path set — nothing to load.
            None => return Ok(()),
        };

        // `hash_get` requires a `&str`. Hash field paths in RediSearch schemas are always
        // valid UTF-8 (they come from user-defined schema field names).
        let field_name = path.to_str().expect("hash field path is not valid UTF-8");

        let val = if let Some(val) = self.key.hash_get(field_name)? {
            let coerce = if kk.flags.contains(RLookupKeyFlag::Numeric) {
                HashValueCoerce::Dbl
            } else {
                HashValueCoerce::Str
            };
            hval_to_value(&val, coerce)
        } else if path.to_bytes() == UNDERSCORE_KEY.to_bytes() {
            // The field path is "__key" — load the document's key name as a string value.
            //
            // FIXME: Ideally we would call `RedisModule_GetKeyNameFromModuleKey` here
            // to match the C code exactly. However, `RedisKey` in the `redismodule-rs`
            // crate does not expose a `key_name()` method, and its internal fields
            // (`ctx`, `key_inner`) are `pub(crate)` — inaccessible from outside.
            // `to_raw_parts()` consumes the key, which we can't do with a `&RedisKey`.
            // Reading the fields via raw pointer cast would be UB (`RedisKey` is not
            // `#[repr(C)]`).
            //
            // As a workaround we use the `key_name` parameter passed in by the caller
            // (the same document key name used to open this Redis key handle).
            // `GetKeyNameFromModuleKey` reads from `RedisModuleKey::key`, which is set
            // by `OpenKey` to the name passed in — so the two are always identical.
            //
            // Upstream fix needed: add `pub fn key_name(&self) -> RedisString` to
            // `RedisKey` in `redismodule-rs`, wrapping `RedisModule_GetKeyNameFromModuleKey`.
            // Once available, replace this with: `hval_to_value(&key.key_name(), HashValueCoerce::Str)`
            hval_to_value(self.key_name, HashValueCoerce::Str)
        } else {
            // Field doesn't exist in the hash — silently succeed (no value to load).
            return Ok(());
        };

        dst_row.write_key(kk, val);

        Ok(())
    }
}

/// How to coerce a Redis hash field value into an [`SharedValue`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HashValueCoerce {
    Str,
    Int,
    Dbl,
    Bool,
}

/// Converts a Redis hash field value to a [`SharedValue`].
///
/// **Note:** the C version retains the `RedisModuleString` for the string case
/// (zero-copy, refcount bump via `RedisModule_RetainString`). This version
/// copies the bytes via `as_slice().to_vec()`. Correct but allocating; a
/// zero-copy path would require wrapping `RSValue_NewRedisString`, which is
/// not yet available in the `value` crate.
fn hval_to_value(src: &RedisString, coerce: HashValueCoerce) -> SharedValue {
    match coerce {
        HashValueCoerce::Bool | HashValueCoerce::Int => {
            // C calls `RSValue_NewNumberFromInt64`, which casts to `f64`.
            let ll = src.parse_integer().unwrap_or(0);
            SharedValue::new_num(ll as f64)
        }
        HashValueCoerce::Dbl => {
            let dd = src.parse_float().unwrap_or(0.0);
            SharedValue::new_num(dd)
        }
        HashValueCoerce::Str => SharedValue::new_string(src.as_slice().to_vec()),
    }
}

#[cfg(all(test, feature = "unittest"))]
mod tests {
    use super::*;
    use std::ffi::CString;

    /// Build a `RedisString` whose backing bytes live in the caller's `CString`.
    ///
    /// The mock `RedisModule_CreateString` stores the pointer, not a copy, so
    /// `bytes` must outlive the returned `RedisString`.
    fn make_string(bytes: &CString) -> RedisString {
        // Safety: `bytes` outlives the returned `RedisString` (caller-controlled).
        unsafe { RedisString::from_raw_parts(None, bytes.as_ptr(), bytes.as_bytes().len()) }
    }

    fn expect_num(v: &SharedValue) -> f64 {
        v.as_num().expect("expected Number variant")
    }

    fn expect_string(v: &SharedValue) -> Vec<u8> {
        v.as_str_bytes()
            .map(<[u8]>::to_vec)
            .expect("expected String/RedisString variant")
    }

    #[test]
    fn str_round_trips_arbitrary_bytes() {
        redis_mock::init_redis_module_mock();

        // Non-UTF-8 bytes (excluding NUL) must round-trip unchanged.
        let bytes = CString::new(b"\xff\xfe\x80hello".to_vec()).unwrap();
        let s = make_string(&bytes);

        let v = hval_to_value(&s, HashValueCoerce::Str);
        assert_eq!(expect_string(&v), b"\xff\xfe\x80hello");
    }

    #[test]
    fn dbl_parses_and_falls_back_to_zero() {
        redis_mock::init_redis_module_mock();

        let ok = CString::new("3.5").unwrap();
        assert_eq!(
            expect_num(&hval_to_value(&make_string(&ok), HashValueCoerce::Dbl)),
            3.5
        );

        let bad = CString::new("not a number").unwrap();
        assert_eq!(
            expect_num(&hval_to_value(&make_string(&bad), HashValueCoerce::Dbl)),
            0.0
        );
    }

    #[test]
    fn int_parses_and_falls_back_to_zero() {
        redis_mock::init_redis_module_mock();

        let ok = CString::new("42").unwrap();
        assert_eq!(
            expect_num(&hval_to_value(&make_string(&ok), HashValueCoerce::Int)),
            42.0
        );

        let bad = CString::new("3.5").unwrap(); // not an int — must fall back
        assert_eq!(
            expect_num(&hval_to_value(&make_string(&bad), HashValueCoerce::Int)),
            0.0
        );
    }

    #[test]
    fn bool_uses_integer_branch() {
        redis_mock::init_redis_module_mock();

        // C's hvalToValue treats BOOL identically to INT: parses with
        // StringToLongLong then casts the i64 to f64.
        let ok = CString::new("1").unwrap();
        assert_eq!(
            expect_num(&hval_to_value(&make_string(&ok), HashValueCoerce::Bool)),
            1.0
        );

        let zero = CString::new("0").unwrap();
        assert_eq!(
            expect_num(&hval_to_value(&make_string(&zero), HashValueCoerce::Bool)),
            0.0
        );
    }

    #[test]
    fn int_large_value_loses_precision_via_f64_cast() {
        redis_mock::init_redis_module_mock();

        // i64::MAX is 9_223_372_036_854_775_807 — not exactly representable
        // as f64. The C path (`RSValue_NewNumberFromInt64`) does the same
        // lossy cast, so the Rust port must match.
        let s = CString::new(i64::MAX.to_string()).unwrap();
        let v = expect_num(&hval_to_value(&make_string(&s), HashValueCoerce::Int));
        assert_eq!(v, i64::MAX as f64);
    }
}
