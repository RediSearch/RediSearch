/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! This module contains the functions that load documents into the lookup table.
//! This functionality depends on the Redis Module API and the Redis JSON API.
//!
//! The function [`load_document`] is the main entry point for loading documents.

use core::panic;
use std::borrow::Cow;
use std::ffi::{CStr, CString};
use std::ptr::NonNull;

use crate::bindings::{DocumentType, RLookupCoerceType, RLookupLoadMode};
use crate::row::RLookupRow;
use crate::{RLookup, RLookupKey, RLookupKeyFlag};
use enumflags2::make_bitflags;
use ffi::{QueryError, REDISMODULE_OK, RSDocumentMetadata, RedisSearchCtx};
use redis_module::key::{KeyFlags, RedisKey};
use redis_module::{
    CallOptionResp, CallOptionsBuilder, CallReply, CallResult, Context, KeyType, RedisString,
    ScanKeyCursor,
};
use sorting_vector::RSSortingVector;
use value::RSValueFFI;

/// The options data structure as used by the `RLookup_Load` function. Needed for interoperability with C code.
#[repr(C)]
pub struct RLookupLoadOptions<'a> {
    /// The RedisSearch context, needed to access the RedisModuleCtx and other context data. Must be locked for usage outside the main thread.
    pub sctx: *mut RedisSearchCtx,

    /// Provides the key name and the sorting vector
    pub dmd: &'a RSDocumentMetadata,

    /// Needed for rule filter where dmd does not exist
    pub key_ptr: *const std::ffi::c_char,

    /// Keys to load. If present, then loadNonCached and loadAllFields is ignored
    pub keys: *const *const ffi::RLookupKey,

    /// Number of keys in keys array
    pub n_keys: libc::size_t,

    /// The following mode controls the loading behavior of fields
    pub mode: RLookupLoadMode,

    /// Don't use sortables when loading documents. This will enforce the loader to load
    /// the fields from the document itself, even if they are sortables and un-normalized.
    pub force_load: bool,

    /// Force string return; don't coerce to native type
    pub force_string: bool,

    /// Error status, if any
    pub status: *mut QueryError,
}

impl RLookupLoadOptions<'_> {
    /// Get a mutable pointer to the RedisModuleCtx from the RedisSearchCtx.
    fn ctx_mut(&self) -> Option<NonNull<redis_module::raw::RedisModuleCtx>> {
        let rs_ctx = self.sctx;

        // Safety: The RedisSearchCtxPtr is expected to be a valid pointer under
        // the assumption that the caller has provided valid option data structure.
        let rs_ctx = rs_ctx.cast::<redis_module::raw::RedisModuleCtx>();

        NonNull::new(rs_ctx)
    }
}

/// Error type for loading documents into the lookup table.
///
/// We extend the error code from the Redis Resource API to include more specific errors.
#[derive(Debug, thiserror::Error)]
pub enum LoadDocumentError {
    /// The provided key does not exist in the Redis database
    #[cfg(debug_assertions)]
    #[error("Key does not exist: {0}")]
    KeyDoesNotExist(String),
    #[cfg(not(debug_assertions))]
    #[error("Key does not exist")]
    KeyDoesNotExist,

    /// The provided key is not a hash
    #[cfg(debug_assertions)]
    #[error("Key is not a hash document: {0}")]
    KeyIsNoHash(String),
    #[cfg(not(debug_assertions))]
    #[error("Key is not a hash document")]
    KeyIsNoHash,

    /// Neither the scan API nor the fallback Call API is available
    #[error("Neither the `Scan` nor the `Call` API is available for hash based documents")]
    FallbackAPINotAvailable,

    /// A temporary error that occurred in the C code, e.g. C was called and returned [`ffi::REDISMODULE_ERR`].
    #[error("An error occurred in the c code")]
    FromCCode,
}

/// Populate the provided `dst_row` by loading a document (either a [Redis hash] or JSON object).
/// Either all keys are loaded or only the individual keys given in `options.keys`.
///
/// If the key given in `options.dmd.keyPtr` does not exist it will be created in the lookup table.
///
/// ## Arguments
///
/// If `options.mode` is `RLookupLoadMode::AllKeys` the hash or JSON object is loaded in its entirety
/// otherwise only the individual keys given in `options.keys` are loaded.
///
/// ## Status of Porting
///
/// There are four code paths for loading documents, depending on the document type and the loading mode:
///
/// 1. Hash document, load all fields, Scan-Cursor-API: This code path is fully implemented in Rust using the `h_get_all` and `h_get_all_scan` function.
/// 2. Hash document, load all fields, fallback Call-API: This code path is fully implemented in Rust using the `h_get_all` and `h_get_all_fallback` function.
/// 3. JSON document, load all fields: This code path is not yet ported to Rust and still uses the C implementation via FFI, see MOD-11050.
/// 4. Individual keys (either hash or JSON document): This code path is not yet ported to Rust and still uses the C implementation via FFI, see MOD-11051.
///
/// ## References
///
/// [Redis hash]: https://redis.io/docs/latest/develop/data-types/hashes/
pub fn load_document(
    it: &mut RLookup<'_>,
    dst_row: &mut RLookupRow<'_, value::RSValueFFI>,
    options: &RLookupLoadOptions,
) -> Result<(), LoadDocumentError> {
    let sv = options.dmd.sortVector as *const RSSortingVector<RSValueFFI>;

    // Safety: The sorting vector pointer is expected to be valid, i.e. the caller must ensure that the sorting vector is valid.
    let sv = unsafe { sv.as_ref().unwrap() };
    dst_row.set_sorting_vector(sv);

    let rc = match options.mode {
        RLookupLoadMode::AllKeys => {
            if options.dmd.type_() == DocumentType::Hash as u32 {
                h_get_all(it, dst_row, options)?;
                REDISMODULE_OK
            } else {
                let it = (it as *mut RLookup<'_>).cast::<ffi::RLookup>();
                let dst = (dst_row as *mut RLookupRow<RSValueFFI>).cast::<ffi::RLookupRow>();
                let options = (options as *const RLookupLoadOptions)
                    .cast::<ffi::RLookupLoadOptions>()
                    .cast_mut();

                // Safety: The caller must ensure that the `it`, `dst`, and `options` pointers are valid.
                unsafe { ffi::RLookup_JSON_GetAll(it, dst, options) as u32 }
            }
        }
        RLookupLoadMode::KeyList | RLookupLoadMode::SortingVectorKeys => {
            let it = (it as *mut RLookup<'_>).cast::<ffi::RLookup>();
            let dst = (dst_row as *mut RLookupRow<RSValueFFI>).cast::<ffi::RLookupRow>();
            let options = (options as *const RLookupLoadOptions)
                .cast::<ffi::RLookupLoadOptions>()
                .cast_mut();

            // Safety: The caller must ensure that the `it`, `dst`, and `options` pointers are valid.
            unsafe { ffi::loadIndividualKeys(it, dst, options) as u32 }
        }
        _ => {
            panic!("Invalid load mode");
        }
    };

    if rc != REDISMODULE_OK {
        Err(LoadDocumentError::FromCCode)
    } else {
        Ok(())
    }
}

/// Populate the provided `dst_row` by loading all field-value pairs from the [Redis hash] with the key given in `options.dmd.keyPtr`.
///
/// Internally either a scan cursor API or a fallback Call API is used, depending on the Redis version and deployment type. In the CRDT
/// deployment the scan cursor API is not available, so we always use the fallback Call API. The scan cursor API is available
/// from Redis v6.0.6 and above.
///
/// [Redis hash]: https://redis.io/docs/latest/develop/data-types/hashes/
fn h_get_all(
    lookup: &mut RLookup<'_>,
    dst_row: &mut RLookupRow<'_, RSValueFFI>,
    options: &RLookupLoadOptions,
) -> Result<(), LoadDocumentError> {
    // get the key pointer from the options
    let key_ptr = options.dmd.keyPtr;

    // Safety: The key pointer is expected to be a valid C string.
    let sds_len = unsafe { ffi::sdslen__(key_ptr) };

    let key_str = RedisString::from_raw_parts(options.ctx_mut(), key_ptr, sds_len);

    // Safety: We access the global config, which is setup during module initialization, we readonly access the serverVersion field here.
    // which is safe as it is never changed after initialization.
    let server_version = unsafe { ffi::RSGlobalConfig.serverVersion };
    let feature = ffi::RM_SCAN_KEY_API_FIX as i32;
    let feature_supported: bool = feature <= server_version;

    // We can only use the scan API from Redis version 6.0.6 and above
    // and when the deployment is not enterprise-crdt
    // Safety: `isCrdt` is written at module startup and never changed afterwards, therefore now its readable.
    let is_crdt = unsafe { ffi::isCrdt };
    if feature_supported && !is_crdt {
        h_get_all_scan(lookup, dst_row, options, key_str)
    } else {
        h_get_all_fallback(lookup, dst_row, options, key_str)
    }
}

/// Populate the provided `dst_row` by loading all field-value pairs from the [Redis hash] with the key given in `key_str`
///
/// HGETALL implementation using the scan cursor API that is available
/// from Redis v6.0.6 and above.
///
/// [Redis hash]: https://redis.io/docs/latest/develop/data-types/hashes/
fn h_get_all_scan(
    lookup: &mut RLookup<'_>,
    dst_row: &mut RLookupRow<'_, RSValueFFI>,
    options: &RLookupLoadOptions,
    key_str: RedisString,
) -> Result<(), LoadDocumentError> {
    // 1. Open the key
    let key = RedisKey::open_with_flags(
        options.ctx_mut().unwrap().as_ptr(),
        &key_str,
        KeyFlags::NOEFFECTS | KeyFlags::NOEXPIRE | KeyFlags::ACCESS_EXPIRED,
    );

    // Check if the key is a hash
    if key.key_type() != KeyType::Hash {
        // If not a hash, return an error
        #[cfg(debug_assertions)]
        {
            return Err(LoadDocumentError::KeyIsNoHash(key_str.to_string()));
        }
        #[cfg(not(debug_assertions))]
        {
            return Err(LoadDocumentError::KeyIsNoHash);
        }
    }

    // 2. Create a scan cursor
    let scan_cursor = ScanKeyCursor::new(key);

    // Iterator over cursor
    scan_cursor.foreach(|_key, field, value| {
        let (field_cstr, _field_len) = field.as_cstr_ptr_and_len();
        let field_cstr = field_cstr.cast::<u8>();
        let mut vec: Vec<u8> =
            unsafe { Vec::from_raw_parts(field_cstr.cast_mut(), _field_len, _field_len + 1) };
        vec.push(b'\0');
        let field_cstr = unsafe { CStr::from_bytes_with_nul_unchecked(vec.as_slice()) };

        // To work around the borrow checker, we check for key existence and create a new key if needed.
        let flags = make_bitflags!(RLookupKeyFlag::{DocSrc | QuerySrc | NameAlloc});
        let new_needed = lookup.keys.find_by_name(field_cstr).is_none();
        if new_needed {
            let name = Cow::Owned(field_cstr.to_owned());
            lookup.keys.push(RLookupKey::new_with_cow(name, flags));
        }

        // now we can assume the key is there and we get a reference to the name with the correct lifetime
        let name_str: Cow<'_, CStr> = {
            let c = lookup.keys.find_by_name(field_cstr).unwrap();
            Cow::Owned(c.into_current().unwrap().name().to_owned())
        };

        let rlk = lookup
            .get_key_load(name_str.clone(), name_str, flags)
            .unwrap();

        let mut coerce_type = RLookupCoerceType::Str;
        if options.force_string && rlk.flags.contains(RLookupKeyFlag::QuerySrc) {
            coerce_type = RLookupCoerceType::Dbl;
        }

        // This function will retain the value if it's a string. This is thread-safe because
        // the value was created just before calling this callback and will be freed right after
        // the callback returns, so this is a thread-local operation that will take ownership of
        // the string value.
        dst_row.write_key(rlk, {
            // Safety: value is expected to be valid RedisModuleString that is a hash and
            // `ffi::hvalToValue` is expected to return a valid RSValue pointer.
            let inner = unsafe {
                ffi::hvalToValue(value.inner.cast(), coerce_type as ffi::RLookupCoerceType)
            };
            RSValueFFI(NonNull::new(inner).unwrap())
        });
    });

    Ok(())
}

/// Populate the provided `row` by loading all field-value pairs from the [Redis hash] with the key given in `key_str`
///
/// HGETALL implementation using HGETALL as a fallback for Redis versions older than v6.0.6 AND
/// for the enterprise-crdt deployment (which also doesn't support the scan cursor API).
///
/// [Redis hash]: https://redis.io/docs/latest/develop/data-types/hashes/
fn h_get_all_fallback(
    it: &mut RLookup<'_>,
    row: &mut RLookupRow<'_, RSValueFFI>,
    options: &RLookupLoadOptions,
    key_str: RedisString,
) -> Result<(), LoadDocumentError> {
    let call_options = CallOptionsBuilder::new()
        .script_mode()
        .resp(CallOptionResp::Resp3)
        .errors_as_replies()
        .build();
    let ctx = Context::new(options.ctx_mut().unwrap().as_ptr());
    let Ok(reply) = ctx.call_ext::<_, CallResult>("HGETALL", &call_options, &[&key_str]) else {
        return Err(LoadDocumentError::FallbackAPINotAvailable);
    };

    let redis_module::CallReply::Array(reply) = reply else {
        #[cfg(debug_assertions)]
        {
            return Err(LoadDocumentError::KeyDoesNotExist(key_str.to_string()));
        }
        #[cfg(not(debug_assertions))]
        {
            return Err(LoadDocumentError::KeyDoesNotExist);
        }
    };

    let len = reply.len();
    if len == 0 {
        #[cfg(debug_assertions)]
        {
            return Err(LoadDocumentError::KeyDoesNotExist(key_str.to_string()));
        }
        #[cfg(not(debug_assertions))]
        {
            return Err(LoadDocumentError::KeyDoesNotExist);
        }
    }

    for i in (0..len).step_by(2) {
        let k = reply.get(i).unwrap().unwrap();
        let CallReply::String(k) = k else {
            panic!("Expected a string reply for the key, but got something else");
        };
        let value = reply.get(i + 1).unwrap().unwrap();
        let CallReply::String(value) = value else {
            panic!("Expected a string reply for the value, but got something else");
        };

        let k = k.to_string().unwrap();
        let k = CString::new(k).unwrap();
        let field_cstr = k.as_c_str();

        let cursor = it.keys.find_by_name(field_cstr);

        let (_, rlk) = if let Some(cursor) = &cursor {
            let rlk = cursor.current().unwrap();
            if rlk.flags.contains(RLookupKeyFlag::QuerySrc) {
                /* || (rlk->flags & RLOOKUP_F_ISLOADED) TODO: skip loaded keys, EXCLUDING keys that were opened by this function*/
                continue; // Key name is already taken by a query key, or it's already loaded.
            }
            (Some(cursor), rlk)
        } else {
            // To work around the borrow checker, we check for key existence and create a new key if needed.
            let flags = make_bitflags!(RLookupKeyFlag::{DocSrc | QuerySrc | NameAlloc});
            let new_needed = it.keys.find_by_name(field_cstr).is_none();
            if new_needed {
                let name = Cow::Owned(field_cstr.to_owned());
                it.keys.push(RLookupKey::new_with_cow(name, flags));
            }

            // now we can assume the key is there and we get a reference to the name with the correct lifetime
            let name_str: Cow<'_, CStr> = {
                let c = it.keys.find_by_name(field_cstr).unwrap();
                Cow::Owned(c.into_current().unwrap().name().to_owned())
            };

            let rlk = it.get_key_load(name_str.clone(), name_str, flags);
            (None, rlk.unwrap())
        };

        let mut coerce_ty = RLookupCoerceType::Str;
        if options.force_string && rlk.flags.contains(RLookupKeyFlag::QuerySrc) {
            coerce_ty = RLookupCoerceType::Dbl;
        }

        // This function will retain the value if it's a string. This is thread-safe because
        // the value was created just before calling this callback and will be freed right after
        // the callback returns, so this is a thread-local operation that will take ownership of
        // the string value.
        row.write_key(rlk, {
            // Safety: The prvoided value pointer is valid and the ctype is of the correct type.
            let ptr = unsafe {
                ffi::replyElemToValue(value.get_raw().cast(), coerce_ty as ffi::RLookupCoerceType)
            };
            RSValueFFI(NonNull::new(ptr).unwrap())
        });
    }

    Ok(())
}
