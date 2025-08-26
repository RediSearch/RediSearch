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
//!
//! It also contains wrappers for some RedisModule types, using new-type pattern to ensure proper memory/lock management.
//!
//! - [RedisString]: A wrapper around `RedisModuleString` that ensures proper memory management.
//! - [RedisKey]: A wrapper around `RedisModuleKey` that ensures proper resource management (open/close)
//! - [RedisScanCursor]: A wrapper around `RedisModuleScanCursor` that ensures proper resource management (create/destroy).

use core::slice;
use enumflags2::{BitFlags, bitflags};
use redis_module::{KeyMode, KeyType, ReplyType};
use std::{
    ffi::{CStr, c_char, c_void},
    fmt::Display,
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

/// Three Loading modes for RLookup
#[repr(u32)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum RLookupLoadMode {
    /// Use keylist to load a number of [RLookupLoadOptions::n_keys] from [RLookupLoadOptions::keys]
    KeyList = 0,

    /// Load only cached keys from the [sorting_vector::RSSortingVector] and do not load from [crate::row::RLookupRow]
    SortingVectorKeys = 1,

    /// Load all keys from both the [sorting_vector::RSSortingVector] and from the [crate::row::RLookupRow]
    AllKeys = 2,

    /// Load all the keys in the RLookup object
    // TODO: possible unused, Not used in RLookup but listed for completeness with C-side, candidate for removal
    LkKeys = 3,
}

#[repr(u32)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub(crate) enum RLookupCoerceType {
    Str = 0,

    #[expect(
        unused,
        reason = "Not used in RLookup but listed for completeness with C-side, candidate for removal"
    )]
    /// Type is an integer
    // TODO: possible unused, Not used in RLookup but listed for completeness with C-side, candidate for removal
    Int = 1,

    /// Type is a floating point number of double precision
    Dbl = 2,

    #[expect(
        unused,
        reason = "Not used in RLookup but listed for completeness with C-side, candidate for removal"
    )]
    /// Type is a boolean
    // TODO: possible unused, Not used in RLookup but listed for completeness with C-side, candidate for removal
    Bool = 3,
}

#[repr(u32)]
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum DocumentType {
    Hash = 0,

    #[expect(unused, reason = "Used by follow-up PRs")]
    Json = 1,

    #[expect(unused, reason = "Used by follow-up PRs")]
    Unsupported = 2,
}

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

// ===== RedisModule Wrapping ====

// ===== RedisString =====

/// A new-type wrapper around `RedisModuleString` that ensures proper memory management.
/// It is designed to be used with the RAII pattern, where the string is automatically freed when it goes out of scope.
///
/// Use [`RedisString::from_raw_parts`] to create an instance of a `RedisModuleString`. Accessing the `RedisModuleString` differs
/// depending on the caller-side, so far we identified two access patterns:
///
/// 1. Using both the pointer and the len assuming non null-terminated data stored in the `RedisModuleString`.
/// 2. Using only the pointer and assuming a null-terminated C string stored in the `RedisModuleString`, e.g. `Document_LoadPairwiseArgs()` in document_basic.c
///
/// The c string is accessible via [`RedisString::try_as_c_str`].
/// The methods [`RedisString::to_i64`] and [`RedisString::to_f64`] can be used to convert the string to integers or floating-point numbers, respectively.
///
/// Safety:
/// If the context is not null it is the caller's responsibility to ensure that the context is valid until the string is freed correctly.
/// The [`RedisString::ctx`] may be null for a `RedisModuleString` in that case `RedisModule_FreeString` will be called with a null context, which is safe.
/// This pattern can be used if a string shall live longer than it's context.
#[derive(Debug)]
pub struct RedisString {
    // the context in which the string was created, used for freeing the string
    ctx: *mut RedisModuleCtx,
    // the actual string, wrapped in a NonNull pointer for safety
    str: NonNull<ffi::RedisModuleString>,
}

impl Drop for RedisString {
    fn drop(&mut self) {
        // Safety: Assumption: c-side initialized the function ptr and it is is never changed,
        // i.e. after module initialization the function pointers stay valid till the end of the program.
        let gs = unsafe { RedisModule_FreeString }.unwrap();

        // Safety: RAII pattern ensures that free is called only once
        unsafe { gs(self.ctx, self.str.as_ptr()) }
    }
}

impl RedisString {
    /// Creates a new `RedisString` from raw parts, i.e. a context, a pointer to a memory buffer, and its length.
    #[inline]
    pub fn from_raw_parts(ctx: *mut RedisModuleCtx, ptr: *const c_char, len: libc::size_t) -> Self {
        debug_assert!(
            !ptr.is_null(),
            "RedisString::from_raw_parts called with null pointer"
        );

        // Safety: Assumption: c-side initialized the function ptr and it is is never changed,
        // i.e. after module initialization the function pointers stay valid till the end of the program.
        let gs = unsafe { RedisModule_CreateString.unwrap() };

        // Safety: String allocations are done by the C side, so we assume that the pointer is valid.
        let ptr = unsafe { gs(ctx, ptr, len).cast::<RedisModuleString>() };
        Self {
            ctx,
            str: NonNull::new(ptr).unwrap(),
        }
    }

    /// Returns a pointer to the underlying `RedisModuleString` object.
    ///
    /// This is useful for passing the string to C functions that expect a `RedisModuleString`
    ///
    /// Safety: The returned pointer is still managed by this object and should only be used with RedisModule functions.
    #[inline]
    pub unsafe fn as_ptr(&mut self) -> *mut ffi::RedisModuleString {
        self.str.as_ptr()
    }

    /// Directly wraps the `RedisModule_StringPtrLen` function from the C API to get the pointer and length of the string. Prefer use
    /// different methods like [`RedisString::try_as_c_str`] or [`RedisString::to_i64`] if possible.
    #[inline]
    #[expect(unused, reason = "Used by follow-up PRs")]
    pub fn string_ptr_len(&self) -> (*const c_char, libc::size_t) {
        // Safety: Assumption: c-side initialized the function ptr and it is is never changed,
        // i.e. after module initialization the function pointers stay valid till the end of the program.
        let gs = unsafe { RedisModule_StringPtrLen.unwrap() };

        // Safety: By RAII we assure to provide a valid pointer to the string and we assume then the c-side will be safe to call.
        let mut len = 0;

        // Safety: By RAII we assure to provide a valid pointer to the string and we assume then the c-side will be safe to call.
        let ptr = unsafe { gs(self.str.as_ptr(), ptr::from_mut(&mut len)) };

        (ptr, len)
    }

    /// Converts a raw `RedisModuleString` pointer to a C-style string reference [`&CStr`].
    ///
    /// This is useful at points where the Rust wrapper is not accessible, e.g. callbacks.
    /// If possible prefer [RedisString::try_as_c_str] which is safer and more idiomatic.
    ///
    /// It uses the `RedisModule_StringPtrLen` function from the C API to convert the string to a C-style string.
    #[inline]
    pub fn try_raw_into_cstr(raw_str: *mut RedisModuleString) -> Option<&'static CStr> {
        let mut len = 0;
        if raw_str.is_null() {
            return None;
        }

        // Safety: Assumption: c-side initialized the function ptr and it is is never changed,
        // i.e. after module initialization the function pointers stay valid till the end of the program.
        let gs = unsafe { RedisModule_StringPtrLen.unwrap() };

        // Safety: By RAII we assure to provide a valid pointer to the string and we assume then the c-side will be safe to call.
        let ptr = unsafe { gs(raw_str, ptr::from_mut(&mut len)) };

        if ptr.is_null() {
            None
        } else {
            // Safety: We assume the ptr and len given by the c-side are valid.
            let bytes = unsafe { slice::from_raw_parts(ptr.cast(), len) };
            CStr::from_bytes_with_nul(bytes).ok()
        }
    }

    /// Converts the string to a C-style string reference [&CStr], uses the `RedisModule_StringPtrLen` function from the C API.
    #[inline]
    #[expect(unused, reason = "Used by follow-up PRs")]
    pub fn try_as_c_str(&self) -> Option<&CStr> {
        let mut len = 0;

        // Safety: Assumption: c-side initialized the function ptr and it is is never changed,
        // i.e. after module initialization the function pointers stay valid till the end of the program.
        let gs = unsafe { RedisModule_StringPtrLen.unwrap() };

        // Safety: By RAII we assure to provide a valid pointer to the string and we assume then the c-side will be safe to call.
        let ptr = unsafe { gs(self.str.as_ptr(), ptr::from_mut(&mut len)) };

        if ptr.is_null() {
            None
        } else {
            // Safety: We assume the ptr and len given by the c-side are valid.
            let bytes = unsafe { slice::from_raw_parts(ptr.cast(), len) };
            Some(CStr::from_bytes_with_nul(bytes).unwrap())
        }
    }

    /// Converts the string to an integer (i64), uses the `RedisModule_StringToLongLong` function from the C API.
    #[inline]
    #[expect(unused, reason = "Used by follow-up PRs")]
    pub fn to_i64(&self) -> Option<i64> {
        let mut out: i64 = 0;

        // Safety: Assumption: c-side initialized the function ptr and it is is never changed,
        // i.e. after module initialization the function pointers stay valid till the end of the program.
        let gs = unsafe { RedisModule_StringToLongLong.unwrap() };

        // Safety: By RAII we assure to provide a valid pointer to the string and we assume then the c-side will be safe to call.
        let ret = unsafe { gs(self.str.as_ptr(), ptr::from_mut(&mut out)) };

        if ret as u32 == REDISMODULE_OK {
            Some(out)
        } else {
            None
        }
    }

    /// Converts the string to a floating-point number (f64), uses the `RedisModule_StringToDouble` function from the C API.
    #[inline]
    #[expect(unused, reason = "Used by follow-up PRs")]
    pub fn to_f64(&self) -> Option<f64> {
        let mut out: f64 = 0.0;

        // Safety: Assumption: c-side initialized the function ptr and it is is never changed,
        // i.e. after module initialization the function pointers stay valid till the end of the program.
        let gs = unsafe { RedisModule_StringToDouble.unwrap() };

        // Safety: By RAII we assure to provide a valid pointer to the string and we assume then the c-side will be safe to call.
        let ret = unsafe { gs(self.str.as_ptr(), ptr::from_mut(&mut out)) };

        if ret as u32 == REDISMODULE_OK {
            Some(out)
        } else {
            None
        }
    }
}

// this also implements the to_string trait, which is useful for debug error handling, such that we can attach the string to an error message
impl Display for RedisString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Safety: Assumption: c-side initialized the function ptr and it is is never changed,
        // i.e. after module initialization the function pointers stay valid till the end of the program.
        let gs = unsafe { RedisModule_StringPtrLen.unwrap() };

        let mut len = 0;
        // Safety: We assume it is safe to call RedisModule_StringPtrLen with a valid RedisString.
        let ptr = unsafe { gs(self.str.as_ptr(), ptr::from_mut(&mut len)) };

        if ptr.is_null() {
            debug_assert!(false, "RedisModlue::StringPtrLen returned a null pointer.");
            write!(f, "<null RedisString>")
        } else {
            // Safety: We assume the ptr and len given by the c-side are valid.
            let bytes = unsafe { slice::from_raw_parts(ptr.cast(), len) };
            write!(f, "{}", String::from_utf8_lossy(bytes))
        }
    }
}

// ===== RedisKey =====

/// A new-type wrapper around `RedisModuleKey` that ensures proper resource management.
/// It is designed to be used with the RAII pattern, where the key is automatically closed when it goes out of scope.
///
/// Use [`RedisKey::open`] to create an instance from a context and a key name. The key type can be checked using [`RedisKey::ty`].
#[derive(Debug)]
pub struct RedisKey(NonNull<ffi::RedisModuleKey>);

impl Drop for RedisKey {
    fn drop(&mut self) {
        // Safety: Assumption: c-side initialized the function ptr and it is is never changed,
        // i.e. after module initialization the function pointers stay valid till the end of the program.
        let gs = unsafe { ffi::RedisModule_CloseKey }.unwrap();
        // Safety: RAII pattern ensures that close is called only once
        unsafe { gs(self.0.as_ptr()) }
    }
}

impl RedisKey {
    /// Opens a key with the given context and name, using the specified [KeyModes] mode.
    /// Uses the `RedisModule_OpenKey` function from the C API.
    #[inline]
    pub fn open(ctx: *mut RedisModuleCtx, name: *mut RedisModuleString, mode: KeyMode) -> Self {
        debug_assert!(!name.is_null(), "RedisKey::open called with null pointer");

        // Safety: Assumption: c-side initialized the function ptr and it is is never changed,
        // i.e. after module initialization the function pointers stay valid till the end of the program.
        let gs = unsafe { ffi::RedisModule_OpenKey.unwrap() };

        // Safety:
        let ptr = unsafe { gs(ctx, name, mode.bits() as i32).cast::<ffi::RedisModuleKey>() };
        Self(NonNull::new(ptr).unwrap())
    }

    /// Returns the type of the key, using the `RedisModule_KeyType` function from the C API.
    #[inline]
    pub fn ty(&self) -> KeyType {
        // Safety: Assumption: c-side initialized the function ptr and it is is never changed,
        // i.e. after module initialization the function pointers stay valid till the end of the program.
        let gs = unsafe { RedisModule_KeyType.unwrap() };

        // Safety: RAII pattern ensures that the ptr is created with the right c function
        let raw = unsafe { gs(self.0.as_ptr()) } as u32;

        // If the c function returns an invalid value, we assume the type is empty.
        KeyType::from_repr(raw).unwrap_or(KeyType::Empty)
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
        // Safety: Assumption: c-side initialized the function ptr and it is is never changed,
        // i.e. after module initialization the function pointers stay valid till the end of the program.
        let gs = unsafe { RedisModule_ScanCursorDestroy.unwrap() };

        // Safety: RAII pattern ensures that destroy is called only once and that `RedisModule_ScanCursorCreate`` was called for creation.
        unsafe { gs(self.cursor.as_ptr()) }
    }
}

impl RedisScanCursor {
    /// Creates a new scan cursor for the given key.
    #[inline]
    pub fn new_from_key(key: RedisKey) -> Self {
        // Safety: Assumption: c-side initialized the function ptr and it is is never changed,
        // i.e. after module initialization the function pointers stay valid till the end of the program.
        let gs = unsafe { RedisModule_ScanCursorCreate.unwrap() };

        // Safety: Assumption: it is safe to call RedisModule_ScanCursorCreate with a valid RedisKey.
        let ptr = unsafe { gs().cast::<ffi::RedisModuleScanCursor>() };
        Self {
            cursor: NonNull::new(ptr).unwrap(),
            key,
        }
    }
}

// Implements an iterator for RedisScanCursor that yields (RedisKey, *mut RedisModuleString, *mut RedisModuleString) in a Rust for loop.
// This is a wrapper around the RedisModule_ScanKey function from the C API and uses a pattern to get the values from the callback that
// is also used in stack unwinding scenarios. There is not common term for that but here we can think of it as a "stack slot" pattern.
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
            // SAFETY: this is the responsibility of the caller, see only usage below in `next()`
            // `data` is a stack slot of type Data
            unsafe {
                let data = data.cast::<Data>();
                let data = &mut (*data);

                data.write((key, field, value));
            }
        }

        let mut data: Data = MaybeUninit::uninit();
        let data_ptr = addr_of_mut!(data).cast::<c_void>();

        // Safety: Assumption: c-side initialized the function ptr and it is is never changed,
        // i.e. after module initialization the function pointers stay valid till the end of the program.
        let scan_key = unsafe { RedisModule_ScanKey.unwrap() };

        // Safety: All pointers we pass here are guaranteed to remain valid during the `scan_key` call.
        let ret = unsafe {
            scan_key(
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
            // Safety: As stated above, we know that a return-code of 1 means we successfully scanned, which in-turn
            // means `scan_key` called our callback, which wrote the pointers to our stack slot. Its therefore safe to
            // read the data out of the slot here.
            let (key, field, value) = unsafe { MaybeUninit::assume_init(data) };
            let key = RedisKey(NonNull::new(key).unwrap());

            Some((key, field, value))
        } else {
            None
        }
    }
}

// ===== RedisCallReply_Hgetall =====

/// A wrapper around `RedisModuleCallReply` that represents the reply from a Redis command.
#[derive(Debug)]
#[repr(transparent)]
pub struct RedisCallReply(NonNull<ffi::RedisModuleCallReply>);

/// Calls the `HGETALL` command on the given key and returns a `Option<RedisCallReplyHgetall>` instance.
#[inline]
pub fn call_hgetall(ctx: *mut RedisModuleCtx, krstr: &RedisString) -> Option<RedisCallReply> {
    // Safety: Assumption: c-side initialized the function ptr and it is is never changed,
    // i.e. after module initialization the function pointers stay valid till the end of the program.
    let gs = unsafe { ffi::RedisModule_Call.unwrap() };

    // Safety: We assume it is safe to call RedisModule_Call with a valid context
    let ptr = unsafe {
        gs(ctx, c"HGETALL".as_ptr(), c"s".as_ptr(), krstr).cast::<ffi::RedisModuleCallReply>()
    };

    if ptr.is_null() {
        None
    } else {
        // Safety: This branch of if-else is only reached if the pointer is not null.
        Some(RedisCallReply(unsafe { NonNull::new_unchecked(ptr) }))
    }
}

impl Drop for RedisCallReply {
    fn drop(&mut self) {
        // Safety: Assumption: c-side initialized the function ptr and it is is never changed,
        // i.e. after module initialization the function pointers stay valid till the end of the program.
        let gs = unsafe { ffi::RedisModule_FreeCallReply.unwrap() };

        // Safety: RAII pattern ensures that free is called only once
        unsafe { gs(self.0.as_ptr()) }
    }
}

impl RedisCallReply {
    /// Returns a pointer to the underlying `RedisModuleCallReply` object.
    /// This is useful for passing the call reply to C functions that expect a `RedisModuleCallReply
    /// Safety: The returned pointer should only be used with RedisModule functions.
    #[inline]
    pub unsafe fn get_ptr(&self) -> *mut ffi::RedisModuleCallReply {
        self.0.as_ptr()
    }

    /// Returns the type of the reply, using the `RedisModule_CallReplyType` function from the C API.
    #[inline]
    pub fn ty(&self) -> ReplyType {
        // Safety: Assumption: c-side initialized the function ptr and it is is never changed,
        // i.e. after module initialization the function pointers stay valid till the end of the program.
        let gs = unsafe { RedisModule_CallReplyType.unwrap() };

        // Safety: RAII pattern ensures that the ptr is valid (created by "RedisModule_Call" with HGETCALL params)
        let raw = unsafe { gs(self.0.as_ptr()) };

        // If the c function returns an invalid value, we assume the type is unknown.
        ReplyType::from_repr(raw).unwrap_or(ReplyType::Unknown)
    }

    /// Returns the length of the reply if it is an array, using the `RedisModule_CallReplyLength` function from the C API.
    #[inline]
    pub fn length(&self) -> libc::size_t {
        // Safety: Assumption: c-side initialized the function ptr and it is is never changed,
        // i.e. after module initialization the function pointers stay valid till the end of the program.
        let gs = unsafe { RedisModule_CallReplyLength.unwrap() };

        // Safety: RAII pattern ensures that the ptr is valid (created by "RedisModule_Call" with HGETCALL params)
        unsafe { gs(self.0.as_ptr()) }
    }

    /// Returns the array element at the given index, using the `RedisModule_CallReplyArrayElement` function from the C API.
    /// Returns none if the index is out of bounds, or the result is not an array.
    #[inline]
    pub fn array_element(&self, idx: libc::size_t) -> Option<&RedisCallReply> {
        // Safety: Assumption: c-side initialized the function ptr and it is is never changed,
        // i.e. after module initialization the function pointers stay valid till the end of the program.
        let gs = unsafe { RedisModule_CallReplyArrayElement.unwrap() };

        // Safety: RAII pattern ensures that the ptr is valid (created by "RedisModule_Call" with HGETCALL params)
        let ptr = unsafe { gs(self.0.as_ptr(), idx).cast::<RedisModuleCallReply>() };

        if ptr.is_null() {
            None
        } else {
            // Safety: This branch of if-else is only reached if the pointer is not null.
            let ptr = unsafe { NonNull::new_unchecked(ptr) };

            // Safety: It's safe because we only use read-only access to the pointer.
            // If in the future we need to mutate the reply, we must implement a mutable
            // version of this function.
            Some(unsafe { ptr.cast().as_ref() })
        }
    }

    /// Returns the string pointer and length of the reply if it is a string or an error. If not it returns `None`.
    #[inline]
    #[expect(unused, reason = "Used by follow-up PRs")]
    pub fn str_ptr_len(&self) -> Option<(*const c_char, libc::size_t)> {
        // Safety: Assumption: c-side initialized the function ptr and it is is never changed,
        // i.e. after module initialization the function pointers stay valid till the end of the program.
        let gs = unsafe { RedisModule_CallReplyStringPtr.unwrap() };

        // Safety: RAII pattern ensures that the ptr is valid (created by "RedisModule_Call" with HGETCALL params)
        let mut len = 0;

        // Safety: We assume it is safe to call RedisModule_CallReplyStringPtr and that zero
        // is returned when the reply is not a string or an error.
        let ptr = unsafe { gs(self.0.as_ptr(), ptr::from_mut(&mut len)) };

        if ptr.is_null() {
            None
        } else {
            Some((ptr, len))
        }
    }

    /// Tries to generate a string slice from the reply if it is a string.
    ///
    /// Call this to retrieve null-terminated C strings. It checks if the RedisModule String ends with a null byte
    /// and if it does not, it returns `None`.
    ///
    /// Uses the `RedisModule_CallReplyStringPtr` function from the C API.
    #[inline]
    pub fn try_as_cstr(&self) -> Option<&CStr> {
        let mut len = 0;

        // Safety: Assumption: c-side initialized the function ptr and it is is never changed,
        // i.e. after module initialization the function pointers stay valid till the end of the program.
        let gs = unsafe { RedisModule_CallReplyStringPtr.unwrap() };

        // Safety: We assume it is safe to call RedisModule_CallReplyStringPtr and that zero
        // is returned when the reply is not a string or an error.
        let ptr = unsafe { gs(self.0.as_ptr(), ptr::from_mut(&mut len)) };

        if ptr.is_null() {
            None
        } else {
            // Safety: The c-side, RedisModule_CallReplyStringPtr, must returns a valid pointer to a c string.
            let bytes = unsafe { slice::from_raw_parts(ptr.cast(), len) };

            // Either we hold the caller assumption and return a valid CStr, or we return None.
            CStr::from_bytes_with_nul(bytes).ok()
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
