/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::load_document::{
    DOCUMENT_OPEN_KEY_QUERY_FLAGS, DocumentFormat, FieldLoader, LoadAllError, LoadFieldError,
    UNDERSCORE_KEY,
};
use crate::{RLookup, RLookupKey, RLookupKeyFlag, RLookupRow};
use redis_module::RedisString;
use redis_module::key::RedisKey;
use redis_module::{KeyType, ScanKeyCursor};
use std::ffi::CStr;
use std::ptr::{self, NonNull};
use std::slice;
use value::{SharedValue, Value};

/// Document loading support for the hash format
pub struct HashDocumentFormat {
    ctx: NonNull<redis_module::RedisModuleCtx>,
    force_string: bool,
}

/// Represents an open hash document ready to load values from.
#[derive(Debug)]
pub struct HashFieldLoader<'a> {
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

impl HashDocumentFormat {
    pub const fn new(ctx: NonNull<redis_module::RedisModuleCtx>, force_string: bool) -> Self {
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

impl DocumentFormat for HashDocumentFormat {
    type FieldLoader<'a> = HashFieldLoader<'a>;

    fn open<'key>(
        &'key self,
        key_name: &'key RedisString,
    ) -> Result<Self::FieldLoader<'key>, LoadFieldError> {
        let key = self.open_hash_key(key_name).map_err(|e| match e {
            HashOpenError::NotFound => LoadFieldError::KeyNotFound,
            HashOpenError::WrongType => LoadFieldError::WrongKeyType,
        })?;

        Ok(HashFieldLoader { key, key_name })
    }

    fn load_all(
        &self,
        rlookup: &mut RLookup,
        dst_row: &mut RLookupRow,
        key_name: &RedisString,
    ) -> Result<(), LoadAllError> {
        let key = self
            .open_hash_key(key_name)
            .map_err(|_| LoadAllError::OpenKeyFailed)?;

        let scan_cursor = ScanKeyCursor::new(key);

        scan_cursor.for_each(|_key, field, value| {
            let (field_ptr, field_len) = field.as_cstr_ptr_and_len();

            // Field names are treated as C strings here: a name with an interior NUL is
            // truncated at the first NUL. This matches RediSearch as a whole: schema and
            // document field names are built with `strlen` (`IndexSpec_CreateField`,
            // `addFieldCommon`) and HASH lookups use `REDISMODULE_HASH_CFIELDS`.
            let field_cstr = {
                // SAFETY: `RedisModule_StringPtrLen` returns a pointer to a null-terminated
                // SDS string.
                let bytes = unsafe { slice::from_raw_parts(field_ptr.cast::<u8>(), field_len + 1) };

                CStr::from_bytes_until_nul(bytes).expect("SDS string must contain null-terminator")
            };

            let key = if let Some(c) = rlookup.find_key_by_name(field_cstr) {
                if c.current()
                    .unwrap() // NB: if `find_key_by_name` returns Some the cursor always points at a valid element
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
                HashCoerceType::Double
            } else {
                HashCoerceType::Str
            };
            let value = hval_to_value(value, coerce);

            dst_row.write_key(key, value);
        });

        Ok(())
    }
}

impl FieldLoader for HashFieldLoader<'_> {
    fn load_field(&self, kk: &RLookupKey, dst_row: &mut RLookupRow) -> Result<(), LoadFieldError> {
        let path = match kk.path() {
            Some(p) => p.as_ref(),
            // No path set — nothing to load.
            None => return Ok(()),
        };

        let val = if let Some(val) = self.key.hash_get(path.to_bytes())? {
            let coerce = if kk.flags.contains(RLookupKeyFlag::Numeric) {
                HashCoerceType::Double
            } else {
                HashCoerceType::Str
            };
            hval_to_value(&val, coerce)
        } else if path.to_bytes() == UNDERSCORE_KEY.to_bytes() {
            // The field path is "__key" — load the document's key name as a string value.
            //
            // workaround `RedisModule_GetKeyNameFromModuleKey` not being exposed by upstream
            // redismodule-rs. we use the `key_name` parameter passed in by the caller
            // (the same document key name used to open this Redis key handle).
            // `GetKeyNameFromModuleKey` reads from `RedisModuleKey::key`, which is set
            // by `OpenKey` to the name passed in therefore the two are identical and this
            // is correct.
            hval_to_value(self.key_name, HashCoerceType::Str)
        } else {
            // Field doesn't exist in the hash — silently succeed (no value to load).
            return Ok(());
        };

        dst_row.write_key(kk, val);

        Ok(())
    }
}

/// How to coerce a Redis hash field value into a [`SharedValue`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HashCoerceType {
    Str,
    Double,
}

/// Converts a Redis hash field value to a [`SharedValue`].
fn hval_to_value(src: &RedisString, coerce: HashCoerceType) -> SharedValue {
    match coerce {
        HashCoerceType::Double => {
            let dd = src.parse_float().unwrap_or(0.0);
            SharedValue::new_num(dd)
        }
        HashCoerceType::Str => {
            redis_module::raw::string_retain_string(ptr::null_mut(), src.inner);
            // Safety: `src` is guaranteed to be a valid redis string by the type wrapper AND we called retain above
            // to bump the reference count
            let src = unsafe { value::RedisString::from_raw(src.inner.cast()) };
            SharedValue::new(Value::RedisString(src))
        }
    }
}

#[cfg(all(test, feature = "unittest"))]
mod tests {
    use super::*;
    use std::ffi::CString;

    /// Build a [`RedisString`] whose backing bytes live in the caller's `CString`.
    ///
    /// The mock `RedisModule_CreateString` stores the pointer, not a copy, so
    /// `bytes` must outlive the returned [`RedisString`].
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
    #[cfg_attr(miri, ignore)] // can't call FFI function RedisModule_CreateString under miri
    fn str_round_trips_arbitrary_bytes() {
        redis_mock::init_redis_module_mock();

        // Non-UTF-8 bytes (excluding NUL) must round-trip unchanged.
        let bytes = CString::new(b"\xff\xfe\x80hello".to_vec()).unwrap();
        let s = make_string(&bytes);

        let v = hval_to_value(&s, HashCoerceType::Str);
        assert_eq!(expect_string(&v), b"\xff\xfe\x80hello");
    }

    #[test]
    #[cfg_attr(miri, ignore)] // can't call FFI function RedisModule_CreateString under miri
    fn invalid_fallback_to_zero() {
        redis_mock::init_redis_module_mock();

        let bad = CString::new("foo").unwrap();
        assert_eq!(
            expect_num(&hval_to_value(&make_string(&bad), HashCoerceType::Double)),
            0.0
        );
    }
}
