/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    borrow::Cow,
    ffi::CStr,
    ops::{Deref, DerefMut},
    ptr::NonNull,
};

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
    load_document::{LoadDocumentContext, LoadDocumentError, LoadDocumentOptions, ValueSrc},
};

pub(super) struct LoadDocumentImpl {
    pub(super) ctx: redis_module::Context,
}

impl Deref for LoadDocumentImpl {
    type Target = redis_module::Context;
    fn deref(&self) -> &Self::Target {
        &self.ctx
    }
}

impl DerefMut for LoadDocumentImpl {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ctx
    }
}

impl LoadDocumentContext for LoadDocumentImpl {
    type V = RSValueFFI;

    fn is_crdt(&self) -> bool {
        // Safety: `isCrdt` is written at module startup and never changed afterwards, therefore it is safe to read it here.
        unsafe { ffi::isCrdt }
    }

    fn has_scan_key_feature(&self) -> bool {
        // Safety: We access the global config, which is setup during module initialization, we readonly access the serverVersion field here.
        // which is safe as it is never changed after initialization.
        let server_version = unsafe { ffi::RSGlobalConfig.serverVersion };
        let feature = ffi::RM_SCAN_KEY_API_FIX as i32;
        feature <= server_version
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
        lookup: &mut RLookup<'_>,
        dst_row: &mut RLookupRow<'_, Self::V>,
        options: &LoadDocumentOptions<'_, Self::V>,
    ) -> Result<(), LoadDocumentError> {
        todo!("ccalls::json_get_all(lookup, dst_row, options)")
    }

    fn load_individual_keys(
        &self,
        lookup: &mut RLookup<'_>,
        dst_row: &mut RLookupRow<'_, Self::V>,
        options: &LoadDocumentOptions<'_, Self::V>,
    ) -> Result<(), LoadDocumentError> {
        todo!("ccalls::load_individual_keys(lookup, dst_row, options)")
    }
}

pub(super) fn get_all_scan<C: LoadDocumentContext>(
    lookup: &mut RLookup<'_>,
    dst_row: &mut RLookupRow<'_, C::V>,
    options: &LoadDocumentOptions<'_, C::V>,
    key_str: RedisString,
    context: &C,
) -> Result<(), LoadDocumentError> {
    // 1. Open the key
    let ctx = options.context.map_or(std::ptr::null_mut(), |c| c.as_ptr());
    let key = RedisKey::open_with_flags(
        ctx,
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
