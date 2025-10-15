/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{borrow::Cow, ffi::CStr, ptr::NonNull};

use enumflags2::make_bitflags;
use redis_module::{
    CallOptionResp, CallOptionsBuilder, CallReply, CallResult, Context, KeyType, RedisString,
    ScanKeyCursor,
    key::{KeyFlags, RedisKey},
};
use value::RSValueFFI;

use crate::{
    RLookup, RLookupKey, RLookupKeyFlag, RLookupRow,
    bindings::RLookupCoerceType,
    load_document::{LoadDocumentError, LoadDocumentOptions},
};

#[inline(always)]
pub(super) fn is_crdt() -> bool {
    // Safety: `isCrdt` is written at module startup and never changed afterwards, therefore it is safe to read it here.
    unsafe { ffi::isCrdt }
}

#[inline(always)]
pub(super) fn has_scan_key_feature() -> bool {
    // Safety: We access the global config, which is setup during module initialization, we readonly access the serverVersion field here.
    // which is safe as it is never changed after initialization.
    let server_version = unsafe { ffi::RSGlobalConfig.serverVersion };
    let feature = ffi::RM_SCAN_KEY_API_FIX as i32;
    feature <= server_version
}

pub(super) fn provide_keystr(
    options: &LoadDocumentOptions<'_>,
) -> Result<RedisString, LoadDocumentError> {
    let key_ptr = options
        .key_ptr
        .ok_or(LoadDocumentError::invalid_arguments(Some(
            "Key pointer is null".to_string(),
        )))?;

    // Safety: We assume the caller provided options with a key pointer containing a sds string.
    let sds_len = unsafe { ffi::sdslen__(key_ptr.as_ptr()) };

    Ok(
        // Safety: The sds string is prefixed with its length, key_ptr directly points to the string data.
        unsafe { RedisString::from_raw_parts(Some(options.context), key_ptr.as_ptr(), sds_len) },
    )
}

pub(super) fn get_all_fallback(
    lookup: &mut RLookup<'_>,
    dst_row: &mut RLookupRow<'_, RSValueFFI>,
    options: &LoadDocumentOptions<'_>,
    key_str: RedisString,
) -> Result<(), LoadDocumentError> {
    // generate call options to use CALL API from `redis_module` crate.
    let call_options = CallOptionsBuilder::new()
        .script_mode()
        .resp(CallOptionResp::Resp3)
        .errors_as_replies()
        .build();

    // create a context from the RedisModuleCtx
    let ctx = Context::new(options.context.as_ptr());

    // call HGETALL using the CALL API with `key_str` as argument
    let Ok(reply) = ctx.call_ext::<_, CallResult>("HGETALL", &call_options, &[&key_str]) else {
        return Err(LoadDocumentError::FallbackAPINotAvailable);
    };

    // Check if the reply is an array and return an error if not
    let redis_module::CallReply::Array(reply) = reply else {
        return Err(LoadDocumentError::key_does_not_exist(Some(
            key_str.to_string(),
        )));
    };

    // If the array is empty, the key does not exist
    let len = reply.len();
    if len == 0 {
        return Err(LoadDocumentError::key_does_not_exist(Some(
            key_str.to_string(),
        )));
    }

    // The array must have an even number of elements (field-value pairs)
    for i in (0..len).step_by(2) {
        let k = reply.get(i).unwrap().unwrap();
        let CallReply::String(k) = k else {
            panic!("Expected a string reply for the key, but got something else");
        };
        let value = reply.get(i + 1).unwrap().unwrap();
        let CallReply::String(value) = value else {
            panic!("Expected a string reply for the value, but got something else");
        };

        // the following is like the c strndup function, so we need to add the trailing zero:
        let key_bytes = k.as_bytes();

        // Safety: We create a CString from null terminated byte slice, which is safe because redis strings have a trailing zero.
        let field_cstr = unsafe { CStr::from_ptr(key_bytes.as_ptr().cast()) };

        with_or_create_key(
            lookup,
            field_cstr,
            |rlk| !rlk.flags.contains(RLookupKeyFlag::QuerySrc),
            |rlk| {
                let mut coerce_ty = RLookupCoerceType::Str;
                if !options.force_string && rlk.flags.contains(RLookupKeyFlag::Numeric) {
                    coerce_ty = RLookupCoerceType::Dbl;
                }

                // This function will retain the value if it's a string. This is thread-safe because
                // the value was created just before calling this callback and will be freed right after
                // the callback returns, so this is a thread-local operation that will take ownership of
                // the string value.
                dst_row.write_key(rlk, {
                    // Safety: The provided value pointer is valid and the ctype is of the correct type.
                    let ptr = unsafe {
                        ffi::replyElemToValue(
                            value.get_raw().cast(),
                            coerce_ty as ffi::RLookupCoerceType,
                        )
                    };
                    // Safety: We assume that `replyElemToValue` returns a valid pointer.
                    unsafe {
                        RSValueFFI::from_raw(
                            NonNull::new(ptr).expect("replyElemToValue returned a null pointer"),
                        )
                    }
                });
            },
        );
    }

    Ok(())
}

pub(super) fn get_all_scan(
    lookup: &mut RLookup<'_>,
    dst_row: &mut RLookupRow<'_, RSValueFFI>,
    options: &LoadDocumentOptions<'_>,
    key_str: RedisString,
) -> Result<(), LoadDocumentError> {
    // 1. Open the key
    let key = RedisKey::open_with_flags(
        options.context.as_ptr(),
        &key_str,
        KeyFlags::NOEFFECTS | KeyFlags::NOEXPIRE | KeyFlags::ACCESS_EXPIRED,
    );

    // Check if the key is a hash
    if key.key_type() != KeyType::Hash {
        return Err(LoadDocumentError::key_is_no_hash(Some(key_str.to_string())));
    }

    // 2. Create a scan cursor
    let scan_cursor = ScanKeyCursor::new(key);

    // Iterator over cursor
    scan_cursor.for_each(|_key, field, value| {
        let (raw_field_cstr, _field_len) = field.as_cstr_ptr_and_len();

        // Safety: We crate a CStr from the bytes we received from Redis SCAN API:
        let field_cstr = unsafe { CStr::from_ptr(raw_field_cstr) };

        with_or_create_key(
            lookup,
            field_cstr,
            |key| !key.flags.contains(RLookupKeyFlag::QuerySrc),
            |key| {
                // Decide on the coerce type
                let mut coerce_type = RLookupCoerceType::Str;
                if !options.force_string && key.flags.contains(RLookupKeyFlag::Numeric) {
                    coerce_type = RLookupCoerceType::Dbl;
                }

                // This function will retain the value if it's a string. This is thread-safe because
                // the value was created just before calling this callback and will be freed right after
                // the callback returns, so this is a thread-local operation that will take ownership of
                // the string value.
                dst_row.write_key(key, {
                    // Safety: value is a valid RedisModuleString provided by the scan cursor API.
                    let inner = unsafe {
                        ffi::hvalToValue(value.inner.cast(), coerce_type as ffi::RLookupCoerceType)
                    };
                    // Safety: We assume that `hvalToValue` returns a valid pointer.
                    unsafe {
                        RSValueFFI::from_raw(
                            NonNull::new(inner).expect("hvalToValue returned a null pointer"),
                        )
                    }
                });
            },
        );
    });

    Ok(())
}

/// Helper function to find or create a key in the lookup table and apply operations on it if the key holds a condition.
///
/// If the key does not exist, it is created with the provided flags. If closure `c` returns true, the operations defined in
/// closure `f` are applied on the key.
///
/// This works around lifetime issues as we don't know if we have to create a new key or can use an existing one.
#[inline(always)]
fn with_or_create_key<'a, F, C>(lookup: &mut RLookup<'a>, field_cstr: &'a CStr, c: C, f: F)
where
    F: FnOnce(&RLookupKey<'_>),
    C: FnOnce(&RLookupKey<'_>) -> bool,
{
    let new_required = lookup.find_by_name(field_cstr).is_none();
    if new_required {
        let flags = make_bitflags!(RLookupKeyFlag::{ForceLoad | NameAlloc});
        let name = Cow::from(field_cstr.to_owned());
        lookup.get_key_write(name, flags);
    }

    let cursor = lookup.find_by_name(field_cstr).expect("key must exist now");

    let key = cursor.current().expect(
        "the cursor returned by `Keys::find_by_name` must have a current key. This is a bug!",
    );

    if new_required || c(key) {
        f(key)
    }
}
