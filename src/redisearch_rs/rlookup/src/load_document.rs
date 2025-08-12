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
use std::ffi::CStr;
use std::ptr::NonNull;

use crate::RLookup;
use crate::RLookupKey;
use crate::RLookupKeyFlag;
use crate::bindings::DocumentType;
use crate::bindings::KeyMode;
use crate::bindings::KeyTypes;
use crate::bindings::RLookupCoerceType;
use crate::bindings::RLookupLoadMode;
use crate::bindings::RedisKey;
use crate::bindings::RedisScanCursor;
use crate::bindings::RedisString;
use crate::bindings::ReplyTypes;
use crate::bindings::call_hgetall;
use crate::row::RLookupRow;
use enumflags2::make_bitflags;
use ffi::QueryError;
use ffi::REDISMODULE_OK;
use ffi::RSDocumentMetadata;
use ffi::RedisSearchCtx;
use sorting_vector::RSSortingVector;
use value::RSValueFFI;

/// Variables that are global in the C API and required for loading documents.
///
/// This is a temporary solution until we have a proper Rust port of the globals in the C API.
pub struct GlobalOptions {
    /// A server-wide flag if CRDTs (Conflict-free replicated data type) are used.
    /// [See further details](https://redis.io/docs/latest/operate/rs/databases/active-active/)
    is_crdt: bool,

    /// The server version, which is used to determine the available features (using Scan API or Call API).
    server_version: u32,
}

/// The options data structure as used by the `RLookup_Load` function. Needed for interoperability with C code.
#[repr(C)]
pub struct RLookupLoadOptions<'a> {
    pub sctx: *mut RedisSearchCtx,

    /** Needed for the key name, and perhaps the sortable */
    pub dmd: &'a RSDocumentMetadata,

    /// Needed for rule filter where dmd does not exist
    pub key_ptr: *const std::ffi::c_char,

    /// Type of document to load, either Hash or JSON.
    pub doc_type: DocumentType,

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

    pub status: *mut QueryError,
}

impl RLookupLoadOptions<'_> {
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
///
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

    /// Neither the scan API nor the fallback, Call, API is available
    #[error("Neither the `Scan` nor the `Call` API is available for hash based documents")]
    FallbackAPINotAvailable,

    /// A temporary error that occurred in the C code, e.g. C was called and returned [`ffi::REDISMODULE_ERR`].
    #[error("An error occurred in the c code")]
    FromCCode,
}

/// Populate a the provided `row` by loading a document (either a [Redis hash] or JSON object) from the key given in `options.dmd.keyPtr`.
///
/// [Redis hash]: https://redis.io/docs/latest/develop/data-types/hashes/
pub fn load_document(
    it: &mut RLookup<'_>,
    dst: &mut RLookupRow<'_, value::RSValueFFI>,
    options: &RLookupLoadOptions,
    globals: &GlobalOptions,
) -> Result<(), LoadDocumentError> {
    let sv = options.dmd.sortVector as *const RSSortingVector<RSValueFFI>;

    // Safety: The sorting vector pointer is expected to be valid, i.e. the caller must ensure that the sorting vector is valid.
    let sv = unsafe { sv.as_ref().unwrap() };
    dst.set_sorting_vector(sv);

    let rc = if options.mode == RLookupLoadMode::AllKeys {
        if options.dmd.type_() == DocumentType::Hash as u32 {
            h_get_all(it, dst, options, globals)?;
            REDISMODULE_OK
        } else {
            let it = (it as *mut RLookup<'_>).cast::<ffi::RLookup>();
            let dst = (dst as *mut RLookupRow<RSValueFFI>).cast::<ffi::RLookupRow>();
            let options = (options as *const RLookupLoadOptions)
                .cast::<ffi::RLookupLoadOptions>()
                .cast_mut();

            // Safety: The caller must ensure that the `it`, `dst`, and `options` pointers are valid.
            unsafe { ffi::RLookup_JSON_GetAll(it, dst, options) as u32 }
        }
    } else {
        let it = (it as *mut RLookup<'_>).cast::<ffi::RLookup>();
        let dst = (dst as *mut RLookupRow<RSValueFFI>).cast::<ffi::RLookupRow>();
        let options = (options as *const RLookupLoadOptions)
            .cast::<ffi::RLookupLoadOptions>()
            .cast_mut();

        // Safety: The caller must ensure that the `it`, `dst`, and `options` pointers are valid.
        unsafe { ffi::loadIndividualKeys(it, dst, options) as u32 }
    };

    if rc != REDISMODULE_OK {
        Err(LoadDocumentError::FromCCode)
    } else {
        Ok(())
    }
}

/// Populate the provided `row` by loading all field-value pairs from the [Redis hash] with the key given in `options.dmd.keyPtr`
///
/// [Redis hash]: https://redis.io/docs/latest/develop/data-types/hashes/
fn h_get_all(
    it: &mut RLookup<'_>,
    row: &mut RLookupRow<'_, RSValueFFI>,
    options: &RLookupLoadOptions,
    globals: &GlobalOptions,
) -> Result<(), LoadDocumentError> {
    // get the key pointer from the options
    // Safety: The caller
    let key_ptr = options.dmd.keyPtr;

    // Safety: The key pointer is expected to be a valid C string.
    let sds_len = unsafe { ffi::sdslen__(key_ptr) };

    // Safety: We assume the context has been locked by the caller
    let key_str = RedisString::from_raw_parts(unsafe { options.ctx_mut() }, key_ptr, sds_len);

    let feature = ffi::RM_SCAN_KEY_API_FIX;
    let feature_supported: bool = feature <= globals.server_version;

    // We can only use the scan API from Redis version 6.0.6 and above
    // and when the deployment is not enterprise-crdt
    if feature_supported && !globals.is_crdt {
        h_get_all_scan(it, row, options, key_str)
    } else {
        h_get_all_fallback(it, row, options, key_str)
    }
}

/// Populate the provided `row` by loading all field-value pairs from the [Redis hash] with the key given in `key_str`
///
/// HGETALL implementation using the scan cursor API that is available
/// from Redis v6.0.6 and above.
///
/// [Redis hash]: https://redis.io/docs/latest/develop/data-types/hashes/
fn h_get_all_scan(
    it: &mut RLookup<'_>,
    row: &mut RLookupRow<'_, RSValueFFI>,
    options: &RLookupLoadOptions,
    mut key_str: RedisString,
) -> Result<(), LoadDocumentError> {
    // 1. Open the key
    // Safety: The RedisString pointer is only used inside the RedisModule API, which we assume to be safe.
    let key_str = unsafe { key_str.as_ptr() };

    // Safety: We assume the context has been locked by the caller
    let key = RedisKey::open(
        unsafe { options.ctx_mut() },
        key_str,
        KeyMode::Read | KeyMode::NoEffects | KeyMode::NoExpire | KeyMode::AccessExpired,
    );

    // Check if the key is a hash
    if key.ty() != KeyTypes::Hash {
        // If not a hash, return an error
        #[cfg(debug_assertions)]
        {
            let str_err = "kstr.to_string()".to_string();
            return Err(LoadDocumentError::KeyIsNoHash(str_err));
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
        let new_needed = it.keys.find_by_name(field_cstr).is_none();
        if new_needed {
            let name = Cow::Owned(field_cstr.to_owned());
            it.keys.push(RLookupKey::new_with_cow(name, flags));
        }

        let rlk = it.get_key_load(field_cstr, field_cstr, flags).unwrap();

        let mut coerce_type = RLookupCoerceType::Str;
        if options.force_string && rlk.flags.contains(RLookupKeyFlag::QuerySrc) {
            coerce_type = RLookupCoerceType::Dbl;
        }

        // This function will retain the value if it's a string. This is thread-safe because
        // the value was created just before calling this callback and will be freed right after
        // the callback returns, so this is a thread-local operation that will take ownership of
        // the string value.
        row.write_key(rlk, {
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
            let str_err = "todo: kstr.to_string()".to_string();
            return Err(LoadDocumentError::KeyDoesNotExist(str_err));
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
            let str_err = "todo kstr.to_string()".to_string();
            return Err(LoadDocumentError::KeyDoesNotExist(str_err));
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
