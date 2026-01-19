/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use crate::load_document::{DocumentFormat, LoadDocumentError, OpenDocument, UNDERSCORE_KEY};
use crate::{RLookup, RLookupKey, RLookupKeyFlag, RLookupRow};
use query_error::{QueryError, QueryErrorCode};
use redis_module::{
    KeyType, RedisString, ScanKeyCursor,
    key::{KeyFlags, RedisKey},
};
use std::ffi::CStr;
use std::ptr::NonNull;
use value::SharedRsValue;

/// Equivalent to `DOCUMENT_OPEN_KEY_QUERY_FLAGS` from `document.h`.
///
/// FIXME: `ACCESS_TRIMMED` (bit 22) is hardcoded because the `redis-module` crate
/// does not expose it yet. This MUST be fixed by upstreaming the flag to
/// `redismodule-rs` (`KeyFlags::ACCESS_TRIMMED`) and then replacing the raw bit
/// here.
const DOCUMENT_OPEN_KEY_QUERY_FLAGS: KeyFlags = KeyFlags::from_bits_retain(
    KeyFlags::NOEFFECTS.bits()
        | KeyFlags::NOEXPIRE.bits()
        | KeyFlags::ACCESS_EXPIRED.bits()
        | (1 << 22), // ACCESS_TRIMMED
);

pub struct HashFormat {
    ctx: NonNull<ffi::RedisModuleCtx>,
    force_string: bool,
}

pub struct HashDoc<'a> {
    key: RedisKey,
    key_name: &'a RedisString,
}

impl DocumentFormat for HashFormat {
    type Document<'a> = HashDoc<'a>;

    fn open<'key>(
        &'key self,
        key_name: &'key RedisString,
        status: &mut QueryError,
    ) -> Result<Self::Document<'key>, LoadDocumentError> {
        let key = RedisKey::open_with_flags(
            self.ctx.cast().as_ptr(),
            key_name,
            DOCUMENT_OPEN_KEY_QUERY_FLAGS,
        );

        if key.is_null() {
            status.set_code(QueryErrorCode::NoDoc);
            return Err(LoadDocumentError {});
        }

        if key.key_type() != KeyType::Hash {
            status.set_code(QueryErrorCode::RedisKeyType);
            return Err(LoadDocumentError {});
        }

        Ok(HashDoc { key, key_name })
    }

    fn load_all(
        &self,
        rlookup: &mut RLookup,
        dst_row: &mut RLookupRow,
        key_name: &RedisString,
        _status: &mut QueryError,
    ) -> Result<(), LoadDocumentError> {
        let key = RedisKey::open_with_flags(
            self.ctx.cast().as_ptr(),
            key_name,
            DOCUMENT_OPEN_KEY_QUERY_FLAGS,
        );

        if key.is_null() || key.key_type() != KeyType::Hash {
            return Err(LoadDocumentError {});
        }

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
                        RLookupKeyFlag::ForceLoad | RLookupKeyFlag::NameAlloc,
                    )
                    .unwrap()
            };

            let numeric = key.flags.contains(RLookupKeyFlag::Numeric) && !self.force_string;
            let value = hval_to_value(value, numeric);

            dst_row.write_key(key, value);
        });

        Ok(())
    }
}

impl OpenDocument for HashDoc<'_> {
    fn load_field(
        &self,
        kk: &RLookupKey,
        dst_row: &mut RLookupRow,
    ) -> Result<(), LoadDocumentError> {
        let path = match kk.path() {
            Some(p) => p.as_ref(),
            // No path set — nothing to load.
            None => return Ok(()),
        };

        // `hash_get` requires a `&str`. Hash field paths in RediSearch schemas are always
        // valid UTF-8 (they come from user-defined schema field names).
        let field_name = path.to_str().expect("hash field path is not valid UTF-8");

        let val = if let Some(val) = self.key.hash_get(field_name)? {
            hval_to_value(&val, kk.flags.contains(RLookupKeyFlag::Numeric))
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
            // Once available, replace this with: `hval_to_value(&key.key_name(), false)`
            hval_to_value(self.key_name, false)
        } else {
            // Field doesn't exist in the hash — silently succeed (no value to load).
            return Ok(());
        };

        dst_row.write_key(kk, val);

        Ok(())
    }
}

/// Converts a Redis hash field value to an [`SharedRsValue`].
///
/// Equivalent to the C function `hvalToValue` in `rlookup_load_document.c`.
///
/// **Note:** The C version retains the `RedisModuleString` for the string case
/// (zero-copy, refcount bump via `RedisModule_RetainString`). This Rust version
/// copies the bytes via `as_slice().to_vec()`. This is safe and correct but
/// performs an extra allocation. A zero-copy path would require wrapping
/// `RSValue_NewRedisString` which is not yet available in the `value` crate.
fn hval_to_value(src: &RedisString, numeric: bool) -> SharedRsValue {
    if numeric {
        let mut dd: f64 = 0.0;
        // SAFETY: `RedisModule_StringToDouble` is always available after module init.
        // `src.inner` is a valid `RedisModuleString` pointer owned by `src`.
        let string_to_double = unsafe { ffi::RedisModule_StringToDouble.unwrap() };
        // SAFETY: `src.inner` is a valid `RedisModuleString` pointer owned by `src`.
        let ret = unsafe { string_to_double(src.inner.cast(), &mut dd) };
        debug_assert_eq!(ret, ffi::REDISMODULE_OK as i32);

        SharedRsValue::new_num(dd)
    } else {
        SharedRsValue::new_string(src.as_slice().to_vec())
    }
}
