/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Bindings and wrapper types for as-of-yet unported types and modules. This makes the actual RLookup code cleaner and safer
//! isolating much of the unsafe FFI code.

use core::slice;
use enumflags2::{BitFlags, bitflags};
use std::{
    ffi::{CStr, c_char, c_void},
    mem::MaybeUninit,
    ptr::{self, NonNull, addr_of_mut},
    sync::atomic::{AtomicUsize, Ordering},
};

use ffi::{
    REDISMODULE_OK, RedisModule_CallReplyArrayElement, RedisModule_CallReplyLength,
    RedisModule_CallReplyStringPtr, RedisModule_CallReplyType, RedisModule_CreateString,
    RedisModule_FreeString, RedisModule_KeyType, RedisModule_ScanCursorCreate,
    RedisModule_ScanCursorDestroy, RedisModule_ScanKey, RedisModule_StringPtrLen,
    RedisModule_StringToDouble, RedisModule_StringToLongLong, RedisModuleCallReply, RedisModuleCtx,
    RedisModuleKey, RedisModuleString,
};

// TODO [MOD-10333] remove once FieldSpec is ported to Rust
#[bitflags]
#[repr(u32)] // should be c_unit
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum FieldSpecOption {
    Sortable = 0x01,
    NoStemming = 0x02,
    NotIndexable = 0x04,
    Phonetics = 0x08,
    Dynamic = 0x10,
    Unf = 0x20,
    WithSuffixTrie = 0x40,
    UndefinedOrder = 0x80,
    IndexEmpty = 0x100,   // Index empty values (i.e., empty strings)
    IndexMissing = 0x200, // Index missing values (non-existing field)
}
pub type FieldSpecOptions = BitFlags<FieldSpecOption>;

// TODO [MOD-10333] remove once FieldSpec is ported to Rust
#[bitflags]
#[repr(u32)] // should be c_unit
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum FieldSpecType {
    Fulltext = 1,
    Numeric = 2,
    Geo = 4,
    Tag = 8,
    Vector = 16,
    Geometry = 32,
}
pub type FieldSpecTypes = BitFlags<FieldSpecType>;

// TODO [MOD-10342] remove once IndexSpecCache is ported to Rust
#[derive(Debug)]
pub struct IndexSpecCache(NonNull<ffi::IndexSpecCache>);

impl IndexSpecCache {
    /// Creates an [`IndexSpecCache`] from a slice of [`ffi::FieldSpec`].
    ///
    /// # Safety
    ///
    /// The caller must ensure the slice outlived the [`IndexSpecCache`].
    pub unsafe fn from_slice(slice: &[ffi::FieldSpec]) -> Self {
        let spcache = Box::new(ffi::IndexSpecCache {
            fields: slice.as_ptr().cast_mut(),
            nfields: slice.len(),
            refcount: 1,
        });

        // Safety: we just allocated it, the ptr cannot be null
        let ptr = unsafe { NonNull::new_unchecked(Box::into_raw(spcache)) };

        // Safety: we allocate the `ffi::IndexSpecCache` here, and free it in Free which ensured the it remains valid
        unsafe { IndexSpecCache::from_raw(ptr) }
    }

    /// # Safety
    ///
    /// The caller must ensure the following invariants are upheld for the *entire lifetime* of this [`crate::RLookup`]:
    /// 1. The `spcache` pointer MUST point to a valid [`ffi::IndexSpecCache`].
    /// 2. The [`ffi::IndexSpecCache`] being pointed MUST NOT get mutated.
    pub unsafe fn from_raw(ptr: NonNull<ffi::IndexSpecCache>) -> Self {
        debug_assert!(ptr.is_aligned());

        Self(ptr)
    }

    pub fn fields(&self) -> &[ffi::FieldSpec] {
        // Safety: The caller promised - on construction of this type - that this pointer is valid, and alias rules for immutable access are obeyed.
        // Furthermore, we maintain the refcount ourselves giving us extra confidence that this pointer is safe to access.
        let me = unsafe { self.0.as_ref() };
        debug_assert!(!me.fields.is_null());

        // Safety: we have to trust that these two values are correct
        unsafe { slice::from_raw_parts(me.fields, me.nfields) }
    }

    pub fn find_field(&self, name: &CStr) -> Option<&ffi::FieldSpec> {
        self.fields().iter().find(|field| {
            debug_assert!(!field.fieldName.is_null());
            // Safety: we have to trust that the `fieldName` pointer is valid
            unsafe {
                ffi::HiddenString_CompareC(field.fieldName, name.as_ptr(), name.count_bytes()) == 0
            }
        })
    }
}

impl Clone for IndexSpecCache {
    fn clone(&self) -> Self {
        // Safety: The caller promised - on construction of this type - that this pointer is valid, and alias rules for immutable access are obeyed.
        // Furthermore, we maintain the refcount ourselves giving us extra confidence that this pointer is safe to access.
        let refcount = unsafe { &raw const self.0.as_ref().refcount };

        // Safety: See above
        let refcount = unsafe { AtomicUsize::from_ptr(refcount.cast_mut()) };

        refcount.fetch_add(1, Ordering::Relaxed);

        Self(self.0)
    }
}

impl Drop for IndexSpecCache {
    fn drop(&mut self) {
        // Safety: The caller promised - on construction of this type - that this pointer is valid, and alias rules for immutable access are obeyed.
        // Furthermore, we maintain the refcount ourselves giving us extra confidence that this pointer is safe to access.

        unsafe {
            ffi::IndexSpecCache_Decref(self.0.as_ptr());
        }
    }
}

impl AsRef<ffi::IndexSpecCache> for IndexSpecCache {
    fn as_ref(&self) -> &ffi::IndexSpecCache {
        // Safety: The caller promised - on construction of this type - that this pointer is valid, and alias rules for immutable access are obeyed.
        // Furthermore, we maintain the refcount ourselves giving us extra confidence that this pointer is safe to access.
        unsafe { self.0.as_ref() }
    }
}

// ===== RedisString =====

#[derive(Debug)]
pub struct RedisString {
    ctx: *mut RedisModuleCtx,
    str: NonNull<ffi::RedisModuleString>,
}

impl Drop for RedisString {
    fn drop(&mut self) {
        unsafe { RedisModule_FreeString.unwrap()(self.ctx, self.str.as_ptr()) }
    }
}

impl RedisString {
    pub fn from_raw_parts(ctx: *mut RedisModuleCtx, ptr: *const c_char, len: libc::size_t) -> Self {
        let ptr =
            unsafe { RedisModule_CreateString.unwrap()(ctx, ptr, len).cast::<RedisModuleString>() };
        Self {
            ctx,
            str: NonNull::new(ptr).unwrap(),
        }
    }

    pub fn as_c_str(&self) -> Option<&CStr> {
        let mut len = 0;

        let ptr = unsafe {
            RedisModule_StringPtrLen.unwrap()(self.str.as_ptr(), ptr::from_mut(&mut len))
        };

        if ptr.is_null() {
            None
        } else {
            let bytes = unsafe { slice::from_raw_parts(ptr.cast(), len) };
            Some(CStr::from_bytes_with_nul(bytes).unwrap())
        }
    }

    #[inline]
    pub fn to_i64(&self) -> Option<i64> {
        let mut out: i64 = 0;

        let ret = unsafe {
            RedisModule_StringToLongLong.unwrap()(self.str.as_ptr(), ptr::from_mut(&mut out))
        };

        if ret as u32 == REDISMODULE_OK {
            Some(out)
        } else {
            None
        }
    }

    #[inline]
    pub fn to_f64(&self) -> Option<f64> {
        let mut out: f64 = 0.0;

        let ret = unsafe {
            RedisModule_StringToDouble.unwrap()(self.str.as_ptr(), ptr::from_mut(&mut out))
        };

        if ret as u32 == REDISMODULE_OK {
            Some(out)
        } else {
            None
        }
    }
}

// ===== RedisKey =====

#[derive(Debug)]
pub struct RedisKey(NonNull<ffi::RedisModuleKey>);

impl Drop for RedisKey {
    fn drop(&mut self) {
        unsafe { ffi::RedisModule_CloseKey.unwrap()(self.0.as_ptr()) }
    }
}

impl RedisKey {
    pub fn open(ctx: *mut RedisModuleCtx, name: *mut RedisModuleString, mode: i32) -> Self {
        let ptr = unsafe {
            ffi::RedisModule_OpenKey.unwrap()(ctx, name, mode).cast::<ffi::RedisModuleKey>()
        };
        Self(NonNull::new(ptr).unwrap())
    }

    pub fn ty(&self) -> i32 {
        unsafe { RedisModule_KeyType.unwrap()(self.0.as_ptr()) }
    }
}

// ===== RedisScanCursor =====

/// Scan api that allows a module to scan the elements in a hash, set or sorted set key
#[derive(Debug)]
pub struct RedisScanCursor {
    cursor: NonNull<ffi::RedisModuleScanCursor>,
    key: RedisKey,
}

impl Drop for RedisScanCursor {
    fn drop(&mut self) {
        unsafe { RedisModule_ScanCursorDestroy.unwrap()(self.cursor.as_ptr()) }
    }
}

impl RedisScanCursor {
    pub fn new_from_key(key: RedisKey) -> Self {
        let ptr =
            unsafe { RedisModule_ScanCursorCreate.unwrap()().cast::<ffi::RedisModuleScanCursor>() };
        Self {
            cursor: NonNull::new(ptr).unwrap(),
            key,
        }
    }
}

impl Iterator for RedisScanCursor {
    type Item = (RedisKey, *mut RedisModuleString, *mut RedisModuleString);

    fn next(&mut self) -> Option<Self::Item> {
        type Data = MaybeUninit<(
            *mut RedisModuleKey,
            *mut RedisModuleString,
            *mut RedisModuleString,
        )>;

        unsafe extern "C" fn callback(
            key: *mut RedisModuleKey,
            field: *mut RedisModuleString,
            value: *mut RedisModuleString,
            data: *mut c_void,
        ) {
            // SAFETY: this is the responsibility of the caller, see above.
            unsafe {
                let data = data.cast::<Data>();
                let data = &mut (*data);

                data.write((key, field, value));
            }
        }

        let mut data: Data = MaybeUninit::uninit();
        let data_ptr = addr_of_mut!(data).cast::<c_void>();

        let ret = unsafe {
            RedisModule_ScanKey.unwrap()(
                self.key.0.as_ptr(),
                self.cursor.as_ptr(),
                Some(callback),
                data_ptr,
            )
        };

        // From the docs:
        // > The function will return 1 if there are more elements to scan and 0 otherwise,
        // > possibly setting errno if the call failed.
        // <https://redis.io/docs/latest/develop/reference/modules/modules-api-ref/#redismodule_scankey>
        if ret == 1 {
            let (key, field, value) = unsafe { MaybeUninit::assume_init(data) };
            let key = RedisKey(NonNull::new(key).unwrap());

            Some((key, field, value))
        } else {
            None
        }
    }
}

// ===== RedisCallReply_Hgetall =====

#[derive(Debug)]
#[repr(transparent)]
pub struct RedisCallReply_Hgetall(NonNull<ffi::RedisModuleCallReply>);

pub fn call_HGETCALL(ctx: *mut RedisModuleCtx, krstr: &RedisString) -> RedisCallReply_Hgetall {
    let ptr = unsafe {
        ffi::RedisModule_Call.unwrap()(ctx, c"HGETALL".as_ptr(), c"s".as_ptr(), krstr)
            .cast::<ffi::RedisModuleCallReply>()
    };
    RedisCallReply_Hgetall(NonNull::new(ptr).unwrap())
}

impl Drop for RedisCallReply_Hgetall {
    fn drop(&mut self) {
        unsafe { ffi::RedisModule_FreeCallReply.unwrap()(self.0.as_ptr()) }
    }
}

impl RedisCallReply_Hgetall {
    // call_reply_type
    pub fn ty(&self) -> i32 {
        unsafe { RedisModule_CallReplyType.unwrap()(self.0.as_ptr()) }
    }

    // call_reply_length
    pub fn length(&self) -> libc::size_t {
        unsafe { RedisModule_CallReplyLength.unwrap()(self.0.as_ptr()) }
    }

    // call_reply_array_element
    pub fn array_element(&self, idx: libc::size_t) -> Option<&RedisCallReply_Hgetall> {
        let ptr = unsafe {
            RedisModule_CallReplyArrayElement.unwrap()(self.0.as_ptr(), idx)
                .cast::<RedisModuleCallReply>()
        };

        NonNull::new(ptr).map(|ptr| unsafe { ptr.cast().as_ref() })
    }

    // call_reply_array_element
    pub fn array_element_mut(&mut self, idx: libc::size_t) -> Option<&mut RedisCallReply_Hgetall> {
        let ptr = unsafe {
            RedisModule_CallReplyArrayElement.unwrap()(self.0.as_ptr(), idx)
                .cast::<RedisModuleCallReply>()
        };

        NonNull::new(ptr).map(|ptr| unsafe { ptr.cast().as_mut() })
    }

    // call_reply_string_ptr
    pub fn string_ptr(&self) -> Option<&CStr> {
        let mut len = 0;

        let ptr = unsafe {
            RedisModule_CallReplyStringPtr.unwrap()(self.0.as_ptr(), ptr::from_mut(&mut len))
        };

        if ptr.is_null() {
            None
        } else {
            let bytes = unsafe { slice::from_raw_parts(ptr.cast(), len) };
            Some(CStr::from_bytes_with_nul(bytes).unwrap())
        }
    }
}

#[cfg(not(miri))]
#[cfg(test)]
mod tests {
    use super::*;

    /// We have to turn off miri for this test. As stacked borrows generate a false positive on the underlying code.
    ///
    /// ```
    /// fn clone(&self) -> Self {
    ///   // ...  
    ///   let refcount = unsafe { &raw const self.0.as_ref().refcount };
    ///   let refcount = unsafe { AtomicUsize::from_ptr(refcount.cast_mut()) };
    ///   //...
    /// ```
    ///
    /// This code generates the error message:  
    ///
    /// ```
    /// error: Undefined Behavior: trying to retag from <182444> for SharedReadWrite permission at alloc76385[0x10], but that tag only grants SharedReadOnly permission for this location
    /// ```
    ///
    /// This happens because, self is a read-only reference, and the second line is trying to retag it to a read-write reference. Which is fine as
    /// a refcount mutable even in const instances. For this the keyword mutable has been introduced to C++. It allows the change of specific variables in const c++ structs or objects.
    /// Here the same safety conditions hold, so we are sound.
    ///
    /// An upcoming alternative for stacked borrows with less false positives are [tree borrows](https://pldi25.sigplan.org/details/pldi-2025-papers/42/Tree-Borrows).
    #[test]
    fn index_spec_cache_refcount() {
        let spcache = Box::new(ffi::IndexSpecCache {
            fields: std::ptr::null_mut(),
            nfields: 0,
            refcount: 1,
        });

        let spcache =
            unsafe { IndexSpecCache::from_raw(NonNull::new_unchecked(Box::into_raw(spcache))) };
        let ffi_ptr_pre = spcache.0;

        assert_eq!(spcache.as_ref().refcount, 1);

        {
            let _clone = spcache.clone();
            assert_eq!(spcache.as_ref().refcount, 2);
        }

        assert_eq!(spcache.as_ref().refcount, 1);
        assert_eq!(ffi_ptr_pre, spcache.0);
    }
}
