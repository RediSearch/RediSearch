/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{borrow::Cow, ffi::CStr, ptr::NonNull};

use redis_module::{
    CallOptionsBuilder, CallReply, CallResult, Context, KeyType, RedisString, ScanKeyCursor,
    key::{KeyFlags, RedisKey},
};
use value::RSValueFFI;

use crate::{
    RLookup, RLookupKey, RLookupKeyFlag, RLookupKeyFlags, RLookupRow,
    bindings::RLookupCoerceType,
    load_document::{LoadDocumentContext, LoadDocumentError, LoadDocumentOptions, ValueSrc},
};

pub(super) struct LoadDocumentImpl;

impl LoadDocumentContext for LoadDocumentImpl {
    type V = RSValueFFI;

    fn is_crdt(&self) -> bool {
        // use redis_mock crate as temporary place for globals
        redis_mock::globals::is_crdt()
    }

    fn has_scan_key_feature(&self) -> bool {
        // use redis_mock crate as temporary place for globals
        redis_mock::globals::has_scan_key_feature()
    }

    fn generate_value(&self, src: ValueSrc, ct: RLookupCoerceType) -> Self::V {
        let ptr = match src {
            // Safety: The replyElemToValue function is safe on RedisModuleCallReply pointers.
            ValueSrc::ReplyElem(reply) => unsafe {
                ffi::replyElemToValue(reply.cast(), ct as ffi::RLookupCoerceType)
            },
            // Safety: The hvalToValue function is safe on RedisString pointers.
            ValueSrc::HVal(_hval) => unsafe {
                ffi::hvalToValue(_hval.as_ptr().cast(), ct as ffi::RLookupCoerceType)
            },
        };
        // Safety: We expect the FFI functions to return a valid pointer
        unsafe {
            RSValueFFI::from_raw(
                NonNull::new(ptr).expect("FFI returned a null pointer in generate_value"),
            )
        }
    }

    fn load_json(
        &self,
        _lookup: &mut RLookup<'_>,
        _dst_row: &mut RLookupRow<'_, Self::V>,
        _options: &LoadDocumentOptions<'_, Self::V>,
    ) -> Result<(), LoadDocumentError> {
        todo!("ccalls::json_get_all(lookup, dst_row, options)")
    }

    fn load_individual_keys(
        &self,
        _lookup: &mut RLookup<'_>,
        _dst_row: &mut RLookupRow<'_, Self::V>,
        _options: &LoadDocumentOptions<'_, Self::V>,
    ) -> Result<(), LoadDocumentError> {
        todo!("ccalls::load_individual_keys(lookup, dst_row, options)")
    }
}

pub(super) fn get_all_fallback<C>(
    lookup: &mut RLookup<'_>,
    dst_row: &mut RLookupRow<'_, C::V>,
    options: &LoadDocumentOptions<'_, C::V>,
    key_str: RedisString,
    context: &C,
) -> Result<(), LoadDocumentError>
where
    C: LoadDocumentContext,
{
    // generate call options to use CALL API from `redis_module` crate.
    let call_options = CallOptionsBuilder::new().build();

    // create a context from the RedisModuleCtx
    let ctx = if let Some(ptr) = options.context {
        Context::new(ptr.as_ptr())
    } else {
        Context::dummy()
    };

    // call HGETALL using the CALL API with `key_str` as argument
    let Ok(reply) = ctx.call_ext::<_, CallResult>("HGETALL", &call_options, &[&key_str]) else {
        return Err(LoadDocumentError::CallAPIError);
    };

    // Check if the reply is an array and return an error if not
    let redis_module::CallReply::Array(reply) = reply else {
        return Err(LoadDocumentError::key_does_not_exist(Some(
            key_str.to_string(),
        )));
    };

    // If the array is empty, either does not exist or is no hash
    let len = reply.len();
    if len == 0 {
        return Err(LoadDocumentError::key_is_somehow_invalid(Some(
            key_str.to_string(),
        )));
    }

    // The array must have an even number of elements (field-value pairs)
    for i in (0..len).step_by(2) {
        let k = reply.get(i).unwrap().unwrap();
        let CallReply::String(k) = k else {
            return Err(LoadDocumentError::invalid_arguments(Some(
                "Expected a string reply for the key, but got something else".to_string(),
            )));
        };
        let value_call = reply.get(i + 1).unwrap().unwrap();
        let CallReply::String(_value) = &value_call else {
            return Err(LoadDocumentError::invalid_arguments(Some(
                "Expected a string reply for the value, but got something else".to_string(),
            )));
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
                let mut coerce_type = RLookupCoerceType::Str;
                if !options.force_string && rlk.flags.contains(RLookupKeyFlag::Numeric) {
                    coerce_type = RLookupCoerceType::Dbl;
                }

                let input = value_call.get_raw().unwrap_or(std::ptr::null_mut());
                let value = context.generate_value(ValueSrc::ReplyElem(input), coerce_type);

                // This function will retain the value if it's a string. This is thread-safe because
                // the value was created just before calling this callback and will be freed right after
                // the callback returns, so this is a thread-local operation that will take ownership of
                // the string value.
                dst_row.write_key(rlk, value);
            },
        );
    }

    Ok(())
}

pub(super) fn get_all_scan<C: LoadDocumentContext>(
    lookup: &mut RLookup<'_>,
    dst_row: &mut RLookupRow<'_, C::V>,
    options: &LoadDocumentOptions<'_, C::V>,
    key_str: RedisString,
    context: &C,
) -> Result<(), LoadDocumentError> {
    // 1. Open the key
    let redis_ctx = options.context.map_or(std::ptr::null_mut(), |c| c.as_ptr());
    let key = RedisKey::open_with_flags(
        redis_ctx,
        &key_str,
        KeyFlags::NOEFFECTS | KeyFlags::NOEXPIRE | KeyFlags::ACCESS_EXPIRED,
    );

    // Check for non-existing key
    if key.key_type() == KeyType::Empty {
        return Err(LoadDocumentError::key_does_not_exist(Some(
            key_str.to_string(),
        )));
    }

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

                let value = context.generate_value(ValueSrc::HVal(value), coerce_type);

                // This function will retain the value if it's a string. This is thread-safe because
                // the value was created just before calling this callback and will be freed right after
                // the callback returns, so this is a thread-local operation that will take ownership of
                // the string value.
                dst_row.write_key(key, value);
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
    let new_required = lookup.find_key_by_name(field_cstr).is_none();
    if new_required {
        let name = Cow::from(field_cstr.to_owned());
        lookup.get_key_write(name, RLookupKeyFlags::empty());
    }

    let cursor = lookup
        .find_key_by_name(field_cstr)
        .expect("key must exist now");

    let key = cursor.current().expect(
        "the cursor returned by `Keys::find_by_name` must have a current key. This is a bug!",
    );

    if new_required || c(key) {
        f(key)
    }
}
