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
use std::ptr::NonNull;

use crate::bindings::call_hgetall;
use crate::bindings::{
    DocumentType, KeyMode, KeyTypes, RLookupCoerceType, RLookupLoadMode, RedisKey, RedisScanCursor,
    RedisString, ReplyTypes,
};
use crate::row::RLookupRow;
use crate::{RLookup, RLookupKey, RLookupKeyFlag};
use enumflags2::make_bitflags;
use ffi::{QueryError, REDISMODULE_OK, RSDocumentMetadata, RedisSearchCtx};
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
    ///
    /// # Safety
    /// This function does not ensure that the module context is locked.
    /// The caller must ensure that the context is locked before using the context pointer.
    unsafe fn ctx_mut(&self) -> *mut ffi::RedisModuleCtx {
        let rs_ctx = self.sctx;

        // Safety: The RedisSearchCtxPtr is expected to be a valid pointer under
        // the assumption that the caller has provided valid option data structure.
        let rs_ctx = unsafe { &*rs_ctx };

        rs_ctx.redisCtx
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
    // Safety: The caller
    let key_ptr = options.dmd.keyPtr;

    // Safety: The key pointer is expected to be a valid C string.
    let sds_len = unsafe { ffi::sdslen__(key_ptr) };

    // Safety: We assume the context has been locked by the caller
    let key_str = RedisString::from_raw_parts(unsafe { options.ctx_mut() }, key_ptr, sds_len);

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
    mut key_str: RedisString,
) -> Result<(), LoadDocumentError> {
    // 1. Open the key
    // Safety: The RedisString pointer is only used inside the RedisModule API, which we assume to be safe.
    let key_str_ptr = unsafe { key_str.as_ptr() };

    // Safety: We assume the context has been locked by the caller
    let key = RedisKey::open(
        unsafe { options.ctx_mut() },
        key_str_ptr,
        KeyMode::Read | KeyMode::NoEffects | KeyMode::NoExpire | KeyMode::AccessExpired,
    );

    // Check if the key is a hash
    if key.ty() != KeyTypes::Hash {
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
    let scan_cursor = RedisScanCursor::new_from_key(key);

    // Iterator over cursor
    for (_key, field, value) in scan_cursor {
        let field_cstr = RedisString::try_raw_into_cstr(field).unwrap();

        // To work around the borrow checker, we check for key existence and create a new key if needed.
        let flags = make_bitflags!(RLookupKeyFlag::{DocSrc | QuerySrc | NameAlloc});
        let new_needed = lookup.keys.find_by_name(field_cstr).is_none();
        if new_needed {
            let name = Cow::Owned(field_cstr.to_owned());
            lookup.keys.push(RLookupKey::new_with_cow(name, flags));
        }

        let rlk = lookup.get_key_load(field_cstr, field_cstr, flags).unwrap();

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
            let inner = unsafe { ffi::hvalToValue(value, coerce_type as ffi::RLookupCoerceType) };
            // Safety: We assume that `hvalToValue` returns a valid pointer.
            unsafe {
                RSValueFFI::from_raw(
                    NonNull::new(inner).expect("hvalToValue returned a null pointer"),
                )
            }
        });
    }

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
    // Safety: We assume the context has been locked by the caller
    let Some(reply) = call_hgetall(unsafe { options.ctx_mut() }, &key_str) else {
        return Err(LoadDocumentError::FallbackAPINotAvailable);
    };

    if reply.ty() != ReplyTypes::Array {
        #[cfg(debug_assertions)]
        {
            return Err(LoadDocumentError::KeyDoesNotExist(key_str.to_string()));
        }
        #[cfg(not(debug_assertions))]
        {
            return Err(LoadDocumentError::KeyDoesNotExist);
        }
    }

    let len = reply.length();
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
        let Some(k) = reply.array_element(i) else {
            panic!("The call reply should be accessible, this indicates a bug.");
        };
        let Some(value) = reply.array_element(i + 1) else {
            panic!("The call reply should be accessible, this indicates a bug.");
        };

        // Safety: The k is expected to be a valid C string obtained from Redis Module API.
        let field_cstr = k.try_as_cstr().unwrap_or_else(|| {
            panic!("Expected a string pointer for the key, but got None");
        });

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

            let rlk = it.get_key_load(field_cstr, field_cstr, flags);
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
            // Safety: We use the pointer only to obtain the RSValue.
            let in_ptr = unsafe { value.get_ptr() };
            // Safety: The prvoided value pointer is valid and the ctype is of the correct type.
            let ptr = unsafe { ffi::replyElemToValue(in_ptr, coerce_ty as ffi::RLookupCoerceType) };
            // Safety: We assume that `replyElemToValue` returns a valid pointer.
            unsafe {
                RSValueFFI::from_raw(
                    NonNull::new(ptr).expect("replyElemToValue returned a null pointer"),
                )
            }
        });
    }

    Ok(())
}
