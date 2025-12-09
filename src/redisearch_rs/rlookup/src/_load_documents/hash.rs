/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use core::fmt;
use std::{
    borrow::Cow,
    ffi::{CStr, c_char},
    marker::PhantomData,
    ops::{self, Deref},
    ptr::{self, NonNull},
    slice,
    str::Utf8Error,
};

use document::UNDERSCORE_KEY;
use redis_module::{
    KeyType, RedisString, ScanKeyCursor,
    key::{KeyFlags, RedisKey},
};
use value::RSValueFFI;

use crate::{
    _load_documents::{LoadDocumentError, LoadDocumentFlag, LoadDocumentOptions},
    RLookup, RLookupKey, RLookupKeyFlag, RLookupRow,
};

// // When loading the document we are after the iterators phase, where we already verified the expiration time of the field and document
// // We don't allow any lazy expiration to happen here
// const DOCUMENT_OPEN_KEY_QUERY_FLAGS: KeyFlags = /* KeyFlags::READ | */
pub fn get_all(
    rlookup: &mut RLookup,
    dst_row: &mut RLookupRow<'_, RSValueFFI>,
    options: &LoadDocumentOptions<'_>,
) -> Result<(), LoadDocumentError> {
    let ctx = options.context.as_ptr();

    // Safety: We assume the caller provided options with a key pointer containing a sds string.
    let sds_len = unsafe { ffi::sdslen__(options.dmd.as_ref().unwrap().keyPtr) };

    // Safety: The sds string is prefixed with its length, key_ptr directly points to the string data.
    let key_str =
        unsafe { RedisString::from_raw_parts(None, options.dmd.as_ref().unwrap().keyPtr, sds_len) };

    let key = RedisKey::open_with_flags(
        ctx,
        &key_str,
        // TODO make this a constant
        KeyFlags::NOEFFECTS | KeyFlags::NOEXPIRE | KeyFlags::ACCESS_EXPIRED,
    );
    // TODO this is stupid why would you do it like this??
    if key.is_null() {
        return Err(());
    }

    if key.key_type() != KeyType::Hash {
        return Err(()); // TODO error
    }

    // 2. Create a scan cursor
    let scan_cursor = ScanKeyCursor::new(key);

    scan_cursor.for_each(|_key, field, value| {
        // REDISMODULE_NOT_USED(key);

        let (raw_field_cstr, _field_len) = field.as_cstr_ptr_and_len();

        // Safety: We crate a CStr from the bytes we received from Redis SCAN API:
        let field_cstr = unsafe { CStr::from_ptr(raw_field_cstr) };

        let key = if let Some(c) = rlookup.find_key_by_name(field_cstr) {
            // Key name is already taken by a query key, or it's already loaded.
            if c.current()
                .unwrap()
                .flags
                .contains(RLookupKeyFlag::QuerySrc)
            {
                return;
            }

            c.into_current().unwrap()
        } else {
            rlookup
                .get_key_load(
                    field_cstr.to_owned(),
                    field_cstr,
                    RLookupKeyFlag::ForceLoad | RLookupKeyFlag::NameAlloc,
                )
                .unwrap()
        };

        let coerce_to_type = if key.flags.contains(RLookupKeyFlag::Numeric)
            && !options.flags.contains(LoadDocumentFlag::ForceString)
        {
            ffi::RLookupCoerceType_RLOOKUP_C_DBL
        } else {
            ffi::RLookupCoerceType_RLOOKUP_C_STR
        };

        // This function will retain the value if it's a string. This is thread-safe because
        // the value was created just before calling this callback and will be freed right after
        // the callback returns, so this is a thread-local operation that will take ownership of
        // the string value.

        let value = rsvalue_from_hash_value(value, coerce_to_type);

        dst_row.write_key(key, value);
    });

    Ok(())
}

// returns true if the value of the key is already available
// avoids the need to call to redis api to get the value
// i.e we can use the sorting vector as a cache
fn is_value_available(
    rlookup: &RLookupKey,
    dst_row: &RLookupRow<'_, RSValueFFI>,
    options: &LoadDocumentOptions<'_>,
) -> bool {
    // return (!options->forceLoad && (
    //       // No need to "write" this key. It's always implicitly loaded!
    //       (kk->flags & RLOOKUP_F_VAL_AVAILABLE) ||
    //       // There is no value in the sorting vector, and we don't need to load it from the document.
    //       ((kk->flags & RLOOKUP_F_SVSRC) && (RLookup_GetItem(kk, dst) == NULL))
    //   ));

    todo!()
}

// ASSUMES:
// - rlookup_key.path is SOME
// - rlookup_key.path is UTF8 string
// - options.dmd is SOME
pub fn get_one(
    rlookup_key: &RLookupKey,
    dst_row: &mut RLookupRow<'_, RSValueFFI>,
    options: &LoadDocumentOptions<'_>,
) -> Result<(), LoadDocumentError> {
    if is_value_available(rlookup_key, dst_row, options) {
        return Ok(());
    }

    let ctx = options.context.as_ptr();

    //   // In this case, the flag must be obtained via HGET
    //   if (!*keyobj) {
    let key_ptr = if let Some(dmd) = options.dmd.as_ref() {
        dmd.keyPtr
    } else {
        options.key_ptr
    };

    // Safety: We assume the caller provided options with a key pointer containing a sds string.
    let sds_len = unsafe { ffi::sdslen__(key_ptr) };

    // Safety: The sds string is prefixed with its length, key_ptr directly points to the string data.
    let key_str = unsafe { RedisString::from_raw_parts(None, key_ptr, sds_len) };

    let key = RedisKey::open_with_flags(
        ctx,
        &key_str,
        // TODO make this a constant
        KeyFlags::NOEFFECTS | KeyFlags::NOEXPIRE | KeyFlags::ACCESS_EXPIRED,
    );
    // TODO this is stupid why would you do it like this??
    if key.is_null() {
        // QueryError_SetCode(options->status, QUERY_ERROR_CODE_NO_DOC);
        return Err(());
    }

    if key.key_type() != KeyType::Hash {
        //       QueryError_SetCode(options->status, QUERY_ERROR_CODE_REDIS_KEY_TYPE);
        return Err(());
    }
    //   }

    //                         key          flags                 field name    val   NULL
    //   RedisModule_HashGet(*keyobj, REDISMODULE_HASH_CFIELDS, kk->path,       &val, NULL);
    let maybe_value = key
        .hash_get(rlookup_key.path().unwrap().to_str().unwrap())
        .unwrap();

    let value = if let Some(value) = maybe_value {
        // `val` was created by `RedisModule_HashGet` and is owned by us.
        // This function might retain it, but it's thread-safe to free it afterwards without any locks
        // as it will hold the only reference to it after the next line.

        let coerce_to_type = if rlookup_key.flags.contains(RLookupKeyFlag::Numeric)
        /* && !options.flags.contains(LoadDocumentFlag::ForceString) */
        // TODO this is present in get_all but should it also be present here?
        {
            ffi::RLookupCoerceType_RLOOKUP_C_DBL
        } else {
            ffi::RLookupCoerceType_RLOOKUP_C_STR
        };

        rsvalue_from_hash_value(&value, coerce_to_type)
    } else {
        if let Some(path) = rlookup_key.path()
            && path != document::UNDERSCORE_KEY
        {
            let (_, key) = key.to_raw_parts();
            let name = unsafe { (redis_module::RedisModule_GetKeyNameFromModuleKey.unwrap())(key) };

            let value = unsafe {
                ffi::hvalToValue(
                    name.cast::<ffi::RedisModuleString>(),
                    ffi::RLookupCoerceType_RLOOKUP_C_STR,
                )
            };
            unsafe { RSValueFFI::from_raw(NonNull::new(value).unwrap()) }
        } else {
            return Ok(());
        }
    };

    dst_row.write_key(rlookup_key, value);

    Ok(())
}

fn rsvalue_from_hash_value(
    value: &RedisString,
    coerce_to_type: ffi::RLookupCoerceType,
) -> RSValueFFI {
    let value = unsafe {
        ffi::hvalToValue(
            value.inner.cast::<ffi::RedisModuleString>().cast_const(),
            coerce_to_type,
        )
    };
    unsafe { RSValueFFI::from_raw(NonNull::new(value).unwrap()) }
}

struct _RedisString<'ctx> {
    ctx: Option<NonNull<ffi::RedisModuleCtx>>,
    _ctx: PhantomData<&'ctx ffi::RedisModuleCtx>,
    inner: NonNull<ffi::RedisModuleString>,
}

impl Drop for _RedisString<'_> {
    fn drop(&mut self) {
        unsafe {
            ffi::RedisModule_FreeString.unwrap()(self.ctx_as_ptr(), self.inner.as_ptr());
        }
    }
}

impl Clone for _RedisString<'_> {
    fn clone(&self) -> Self {
        let inner = unsafe {
            ffi::RedisModule_CreateStringFromString.unwrap()(self.ctx_as_ptr(), self.inner.as_ptr())
        };
        let inner = NonNull::new(inner).unwrap();

        unsafe { Self::from_raw(self.ctx, inner) }
    }
}

impl<'a> _RedisString<'a> {
    pub const unsafe fn from_raw(
        ctx: Option<NonNull<ffi::RedisModuleCtx>>,
        inner: NonNull<ffi::RedisModuleString>,
    ) -> Self {
        Self {
            ctx,
            inner,
            _ctx: PhantomData,
        }
    }
}

impl _RedisString<'_> {
    fn ctx_as_ptr(&self) -> *mut ffi::RedisModuleCtx {
        self.ctx.map_or(ptr::null_mut(), |ptr| ptr.as_ptr())
    }

    pub fn as_redis_str(&self) -> &_RedisStr {
        let mut len = 0;
        let ptr = unsafe {
            ffi::RedisModule_StringPtrLen.unwrap()(self.inner.as_ptr(), ptr::from_mut(&mut len))
        };

        unsafe { &*(ptr::slice_from_raw_parts(ptr.cast::<c_char>(), len) as *const _RedisStr) }
    }

    pub fn push_str(&mut self, s: &str) -> Result<(), ()> {
        let status = unsafe {
            ffi::RedisModule_StringAppendBuffer.unwrap()(
                self.ctx_as_ptr(),
                self.inner.as_ptr(),
                s.as_ptr().cast::<c_char>(),
                s.len(),
            )
        };

        if status == ffi::REDISMODULE_OK as i32 {
            Ok(())
        } else {
            Err(())
        }
    }
}

impl ops::Deref for _RedisString<'_> {
    type Target = _RedisStr;

    #[inline]
    fn deref(&self) -> &_RedisStr {
        self.as_redis_str()
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct _RedisStr {
    inner: [c_char],
}

impl fmt::Debug for &_RedisStr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(FmtRedisStr::from_bytes(self.to_bytes()), f)
    }
}

impl _RedisStr {
    pub fn as_ptr(&self) -> *const c_char {
        self.inner.as_ptr().cast()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn to_bytes(&self) -> &[u8] {
        unsafe { &*((&raw const self.inner) as *const [u8]) }
    }

    pub fn to_str(&self) -> Result<&str, Utf8Error> {
        str::from_utf8(self.to_bytes())
    }

    pub fn to_string_lossy(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(self.to_bytes())
    }

    pub fn display(&self) -> impl fmt::Display {
        FmtRedisStr::from_bytes(self.to_bytes())
    }
}

struct FmtRedisStr([u8]);

impl FmtRedisStr {
    pub const fn from_bytes(slice: &[u8]) -> &Self {
        unsafe { &*(slice as *const [u8] as *const Self) }
    }
}

impl fmt::Debug for FmtRedisStr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "\"")?;
        for chunk in self.0.utf8_chunks() {
            for c in chunk.valid().chars() {
                match c {
                    '\0' => write!(f, "\\0")?,
                    '\x01'..='\x7f' => write!(f, "{}", (c as u8).escape_ascii())?,
                    _ => write!(f, "{}", c.escape_debug())?,
                }
            }
            write!(f, "{}", chunk.invalid().escape_ascii())?;
        }
        write!(f, "\"")?;
        Ok(())
    }
}

impl fmt::Display for FmtRedisStr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fn fmt_nopad(this: &FmtRedisStr, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            for chunk in this.0.utf8_chunks() {
                f.write_str(chunk.valid())?;
                if !chunk.invalid().is_empty() {
                    f.write_str("\u{FFFD}")?;
                }
            }
            Ok(())
        }

        let Some(align) = f.align() else {
            return fmt_nopad(self, f);
        };
        let nchars: usize = self
            .0
            .utf8_chunks()
            .map(|chunk| {
                chunk.valid().chars().count() + if chunk.invalid().is_empty() { 0 } else { 1 }
            })
            .sum();
        let padding = f.width().unwrap_or(0).saturating_sub(nchars);
        let fill = f.fill();
        let (lpad, rpad) = match align {
            fmt::Alignment::Left => (0, padding),
            fmt::Alignment::Right => (padding, 0),
            fmt::Alignment::Center => {
                let half = padding / 2;
                (half, half + padding % 2)
            }
        };
        for _ in 0..lpad {
            write!(f, "{fill}")?;
        }
        fmt_nopad(self, f)?;
        for _ in 0..rpad {
            write!(f, "{fill}")?;
        }

        Ok(())
    }
}
