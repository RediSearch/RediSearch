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
use crate::bindings::RLookupLoadOptions;
use crate::bindings::RedisKey;
use crate::bindings::RedisScanCursor;
use crate::bindings::RedisString;
use crate::bindings::ReplyTypes;
use crate::bindings::call_hgetall;
use crate::lookup::CBCow;
use crate::row::RLookupRow;
use enumflags2::make_bitflags;
use ffi::REDISMODULE_OK;
use ffi::{RSDocumentMetadata_s, RedisModuleKey, RedisModuleString};
use sorting_vector::RSSortingVector;
use value::RSValueFFI;

/// Variables that are global in the C API and required for loading documents.
///
/// This is a temporary solution until we have a proper Rust port of the globals in the C API.
pub struct GlobalCVariables {
    /// A server-wide flag if CRDTs (Conflict-free replicated data type) are used.
    /// [See further details](https://redis.io/docs/latest/operate/rs/databases/active-active/)
    is_crdt: bool,

    /// The server version, which is used to determine the available features (using Scan API or Call API).
    server_version: u32,
}

/// Extraction of the Options for loading a document from [`RLookupLoadOptions`] and [`GlobalCVariables`].
///
/// The [`LoadDocumentOptions::new`] function to create an instance of this struct from the parameters fed
/// to the [`load_document`] function from the c-side calling.
struct LoadDocumentOptions<'a> {
    /// The document metadata, which contains the key pointer and other metadata.
    dmd: &'a RSDocumentMetadata_s,

    /// The Redis context, which is used to access the Redis Module API.
    ctx: &'a mut ffi::RedisModuleCtx,

    /// The mode of loading the document, which can be one of the [`RLookupLoadMode`].
    mode: RLookupLoadMode,

    /// Whether to force the loading of string values.
    force_string: bool,

    /// A server-wide flag if CRDTs (Conflict-free replicated data type) are used.
    /// [See further details](https://redis.io/docs/latest/operate/rs/databases/active-active/)
    is_crdt: bool,

    /// The server version, which is used to determine the available features (using Scan API or Call API).
    server_version: u32,
}

impl LoadDocumentOptions<'_> {
    /// Creates a new `LoadDocumentParams` from the given [`RLookupLoadOptions`] and [`GlobalCVariables`].
    ///
    /// # Safety
    /// The given `options` must be valid, i.e. the caller must ensure that the
    /// pointers to RSDocumentMetadata and RedisModuleCtx are valid.
    pub unsafe fn new(options: &RLookupLoadOptions, additional_params: &GlobalCVariables) -> Self {
        // Safety: The RSDocumentMetadataPtr is expected to be a valid pointer under
        // the assumption that the caller has provided valid option data structure.
        let dmd = unsafe { &*options.dmd };

        let ctx = {
            let rs_ctx = options.sctx;

            // Safety: The RedisSearchCtxPtr is expected to be a valid pointer under
            // the assumption that the caller has provided valid option data structure.
            let rs_ctx = unsafe { &*rs_ctx };

            // Safety: The RedisModuleCtx is expected to be a valid pointer under
            // the assumption that the caller has provided valid option data structure.
            unsafe { &mut *rs_ctx.redisCtx }
        };

        LoadDocumentOptions {
            dmd,
            ctx,
            mode: options.mode,
            force_string: options.force_string,
            is_crdt: additional_params.is_crdt,
            server_version: additional_params.server_version,
        }
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

pub fn load_document<'a>(
    it: &'a mut RLookup<'a>,
    dst: &'a mut RLookupRow<'a, value::RSValueFFI>,
    options: &'a mut RLookupLoadOptions,
    additional_params: &'a GlobalCVariables,
) -> Result<(), LoadDocumentError> {
    // Safety: The options are expected to be valid, i.e. the caller must ensure that the options are valid.
    let mut options_safe = unsafe { LoadDocumentOptions::new(options, additional_params) };
    let mode = options_safe.mode;

    let sv = options_safe.dmd.sortVector as *const RSSortingVector<RSValueFFI>;

    // Safety: The sorting vector pointer is expected to be valid, i.e. the caller must ensure that the sorting vector is valid.
    let sv = unsafe { &*sv };
    dst.set_sorting_vector(sv);

    let rc = if mode == RLookupLoadMode::AllKeys {
        if options_safe.dmd.type_() == DocumentType::Hash as u32 {
            h_get_all(it, dst, &mut options_safe)?;
            REDISMODULE_OK
        } else {
            let it = (it as *mut RLookup<'a>).cast::<ffi::RLookup>();
            let dst = (dst as *mut RLookupRow<RSValueFFI>).cast::<ffi::RLookupRow>();
            let options = (options as *mut RLookupLoadOptions).cast::<ffi::RLookupLoadOptions>();

            // Safety: The caller must ensure that the `it`, `dst`, and `options` pointers are valid.
            unsafe { ffi::RLookup_JSON_GetAll(it, dst, options) as u32 }
        }
    } else {
        let it = (it as *mut RLookup<'a>).cast::<ffi::RLookup>();
        let dst = (dst as *mut RLookupRow<RSValueFFI>).cast::<ffi::RLookupRow>();
        let options = (options as *mut RLookupLoadOptions).cast::<ffi::RLookupLoadOptions>();

        // Safety: The caller must ensure that the `it`, `dst`, and `options` pointers are valid.
        unsafe { ffi::loadIndividualKeys(it, dst, options) as u32 }
    };

    if rc != REDISMODULE_OK {
        Err(LoadDocumentError::FromCCode)
    } else {
        Ok(())
    }
}

// / Private data structure for the callback function `h_get_all_scan_callback`.
struct HgetallPrivateData<'a, 'b> {
    pub it: &'a mut RLookup<'a>,
    pub dst: &'a mut RLookupRow<'a, RSValueFFI>,
    pub options: &'b LoadDocumentOptions<'b>,
}

fn h_get_all<'a>(
    it: &'a mut RLookup<'a>,
    dst: &'a mut RLookupRow<'a, RSValueFFI>,
    options: &mut LoadDocumentOptions<'_>,
) -> Result<(), LoadDocumentError> {
    // get the key pointer from the options
    let key_ptr = options.dmd.keyPtr;

    // Safety: The key pointer is expected to be a valid C string.
    let sds_len = unsafe { ffi::sdslen__(key_ptr) };
    let mut krstr = RedisString::from_raw_parts(options.ctx, key_ptr, sds_len);

    let feature = ffi::RM_SCAN_KEY_API_FIX;
    let feature_supported: bool = feature <= options.server_version;

    if feature_supported && !options.is_crdt {
        // 1. Open the key
        let mode = KeyMode::Read | KeyMode::NoEffects | KeyMode::NoExpire | KeyMode::AccessExpired;

        // Safety: The RedisString pointer is only used inside the RedisModule API, which we assume to be safe.
        let key = RedisKey::open(options.ctx, unsafe { krstr.as_ptr() }, mode);
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
        let mut scan_cursor = RedisScanCursor::new_from_key(key);

        // 3. Iterate over the scan cursors using a callback
        // Safety: See [h_get_all_scan_callback] for more safety details.
        unsafe { scan_cursor.scan_key_loop_unsafe_callback(h_get_all_scan_callback) };
    } else {
        let Some(reply) = call_hgetall(options.ctx, &krstr) else {
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

        let mut priv_data = HgetallPrivateData { it, dst, options };
        for i in (0..len).step_by(2) {
            let Some(k) = reply.array_element(i) else {
                panic!("The call reply should be accessible, this indicates a bug.");
            };
            let Some(value) = reply.array_element(i + 1) else {
                panic!("The call reply should be accessible, this indicates a bug.");
            };

            // Safety: The k is expected to be a valid C string obtained from Redis Module API.
            let kstr = k.try_as_cstr().unwrap_or_else(|| {
                panic!("Expected a string pointer for the key, but got None");
            });

            process_rlookup_key_value(kstr, &mut priv_data, |ctype| {
                // todo access raw value

                // Safety: value is expected to be a valid RedisModuleCallReply pointer
                // and `ffi::replyElemToValue` is expected to return a valid RSValue

                // todo: handle mutability with CallReply

                let in_ptr = unsafe { value.get_ptr() };
                // Safety: The prvoided value pointer is valid and the ctype is of the correct type.
                let ptr = unsafe { ffi::replyElemToValue(in_ptr, ctype) };
                RSValueFFI(NonNull::new(ptr).unwrap())
            });
        }
    }

    Ok(())
}

/// Callback function for the RedisModule `scan_key` function
///
/// Safety: This function is called by the RedisModule API `RedisModule_ScanKey` function,
/// which guarantees that the `priv_data` pointer is valid.
/// We initialize `priv_data` before calling `scan_key_loop_unsafe_callback`.
unsafe extern "C" fn h_get_all_scan_callback(
    _key: *mut RedisModuleKey,
    field: *mut RedisModuleString,
    value: *mut RedisModuleString,
    priv_data: *mut std::ffi::c_void,
) {
    // Safety: cast priv_data back to HgetallPrivateData, it's safe as this is generated in
    // `h_get_all` function and given to this callback.
    let priv_data = unsafe { &mut *(priv_data as *mut HgetallPrivateData) };

    let field_cstr = RedisString::try_raw_into_cstr(field).unwrap();

    process_rlookup_key_value(field_cstr, priv_data, |ctype| {
        // Safety: value is expected to be valid RedisModuleString that is a hash and
        // `ffi::hvalToValue` is expected to return a valid RSValue pointer.
        let inner = unsafe { ffi::hvalToValue(value, ctype) };
        RSValueFFI(NonNull::new(inner).unwrap())
    });
}

/// Processes a key-value pair from the Redis lookup table and writes it to the [`RLookupRow`].
/// This function is used in the [`h_get_all_scan_callback`] and [`h_get_all`] functions to
/// write the key-value pairs to the [`RLookupRow`]'s `dyn_values`.
fn process_rlookup_key_value<F>(
    key_str: &CStr,
    priv_data: &mut HgetallPrivateData<'_, '_>,
    rsvalue_create: F,
) where
    F: FnOnce(u32) -> RSValueFFI,
{
    let cursor = priv_data.it.keys.find_by_name(key_str);

    let (_, rlk) = if let Some(cursor) = &cursor {
        let rlk = cursor.current().unwrap();
        if rlk.flags.contains(RLookupKeyFlag::QuerySrc) {
            /* || (rlk->flags & RLOOKUP_F_ISLOADED) TODO: skip loaded keys, EXCLUDING keys that were opened by this function*/
            return; // Key name is already taken by a query key, or it's already loaded.
        }
        (Some(cursor), rlk)
    } else {
        // To work around the borrow checker, we check for key existence and create a new key if needed.
        let flags = make_bitflags!(RLookupKeyFlag::{DocSrc | QuerySrc | NameAlloc});
        let new_needed = priv_data.it.keys.find_by_name(key_str).is_none();
        if new_needed {
            let name = CBCow::Owned(key_str.to_owned());
            priv_data
                .it
                .keys
                .push(RLookupKey::new_with_cow(name, flags));
        }

        // now we can assume the key is there and we get a reference to the name with the correct lifetime
        let name_str = {
            let c = priv_data.it.keys.find_by_name(key_str).unwrap();
            c.into_current().unwrap().name_ref()
        };

        let rlk = priv_data.it.get_key_load(name_str, name_str, flags);
        (None, rlk.unwrap())
    };

    let mut ctype = RLookupCoerceType::Str;
    if priv_data.options.force_string && rlk.flags.contains(RLookupKeyFlag::QuerySrc) {
        ctype = RLookupCoerceType::Dbl;
    }

    // This function will retain the value if it's a string. This is thread-safe because
    // the value was created just before calling this callback and will be freed right after
    // the callback returns, so this is a thread-local operation that will take ownership of
    // the string value.
    let vptr = rsvalue_create(ctype as u32);
    priv_data.dst.write_key(rlk, vptr);
}
