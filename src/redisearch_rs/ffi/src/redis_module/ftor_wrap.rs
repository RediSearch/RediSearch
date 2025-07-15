/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! This module encapsulates the global function pointers of the redis module API

use std::ffi::{c_char, c_void};

use super::*;

// -- String API

// create string
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[inline]
pub fn create_string(
    ctx: *mut RedisModuleCtx,
    ptr: *const c_char,
    len: libc::size_t,
) -> *mut RedisModuleString {
    unsafe { RedisModule_CreateString.unwrap()(ctx, ptr, len).cast::<RedisModuleString>() }
}

// Returns pointer to the C string, and sets len to its length
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[inline]
pub fn string_ptr_len(s: *const RedisModuleString, len: *mut libc::size_t) -> *const c_char {
    unsafe { RedisModule_StringPtrLen.unwrap()(s, len) }
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[inline]
pub fn string_to_longlong(s: *const RedisModuleString, len: *mut i64) -> Status {
    unsafe { RedisModule_StringToLongLong.unwrap()(s, len).into() }
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[inline]
pub fn string_to_double(s: *const RedisModuleString, len: *mut f64) -> Status {
    unsafe { RedisModule_StringToDouble.unwrap()(s, len).into() }
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[inline]
pub fn free_string(ctx: *mut RedisModuleCtx, s: *mut RedisModuleString) {
    unsafe { RedisModule_FreeString.unwrap()(ctx, s) }
}

// -- Call API

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[inline]
pub fn call0(
    ctx: *mut RedisModuleCtx,
    cmd: *const c_char,
    fmt: *const c_char,
) -> *mut RedisModuleCallReply {
    unsafe { RedisModule_Call.unwrap()(ctx, cmd, fmt).cast::<RedisModuleCallReply>() }
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[inline]
pub fn call1<P1>(
    ctx: *mut RedisModuleCtx,
    cmd: *const c_char,
    fmt: *const c_char,
    p1: P1,
) -> *mut RedisModuleCallReply {
    unsafe { RedisModule_Call.unwrap()(ctx, cmd, fmt, p1).cast::<RedisModuleCallReply>() }
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[inline]
pub fn call2<P1, P2>(
    ctx: *mut RedisModuleCtx,
    cmd: *const c_char,
    fmt: *const c_char,
    p1: P1,
    p2: P2,
) -> *mut RedisModuleCallReply {
    unsafe { RedisModule_Call.unwrap()(ctx, cmd, fmt, p1, p2).cast::<RedisModuleCallReply>() }
}

// call_reply_type
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[inline]
pub fn call_reply_type(reply: *mut RedisModuleCallReply) -> i32 {
    unsafe { RedisModule_CallReplyType.unwrap()(reply) }
}

// call_reply_length
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[inline]
pub fn call_reply_length(reply: *mut RedisModuleCallReply) -> libc::size_t {
    unsafe { RedisModule_CallReplyLength.unwrap()(reply) }
}

// call_reply_array_element
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[inline]
pub fn call_reply_array_element(
    reply: *mut RedisModuleCallReply,
    idx: libc::size_t,
) -> *mut RedisModuleCallReply {
    unsafe { RedisModule_CallReplyArrayElement.unwrap()(reply, idx).cast::<RedisModuleCallReply>() }
}

// call_reply_string_ptr
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[inline]
pub fn call_reply_string_ptr(
    reply: *mut RedisModuleCallReply,
    len: *mut libc::size_t,
) -> *const c_char {
    unsafe { RedisModule_CallReplyStringPtr.unwrap()(reply, len) }
}

// call_reply_free
#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[inline]
pub fn call_reply_free(reply: *mut RedisModuleCallReply) {
    unsafe { RedisModule_FreeCallReply.unwrap()(reply) }
}

// -- Key API

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[inline]
pub fn open_key(
    ctx: *mut RedisModuleCtx,
    keyname: *mut RedisModuleString,
    mode: i32,
) -> *mut RedisModuleKey {
    unsafe { RedisModule_OpenKey.unwrap()(ctx, keyname, mode).cast::<RedisModuleKey>() }
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[inline]
pub fn key_type(key: *mut RedisModuleKey) -> i32 {
    unsafe { RedisModule_KeyType.unwrap()(key) }
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[inline]
pub fn close_key(kp: *mut RedisModuleKey) {
    unsafe { RedisModule_CloseKey.unwrap()(kp) }
}

// -- Scan Cursor API

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[inline]
pub fn scan_cursor_create() -> *mut RedisModuleScanCursor {
    unsafe { RedisModule_ScanCursorCreate.unwrap()().cast::<RedisModuleScanCursor>() }
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[inline]
pub fn scan_key(
    key: *mut RedisModuleKey,
    cursor: *mut RedisModuleScanCursor,
    callback: RedisModuleScanKeyCB,
    private_data: *mut c_void,
) -> i32 {
    unsafe { RedisModule_ScanKey.unwrap()(key, cursor, callback, private_data) }
}

#[allow(clippy::not_unsafe_ptr_arg_deref)]
#[inline]
pub fn scan_cursor_destroy(cursor: *mut RedisModuleScanCursor) {
    unsafe { RedisModule_ScanCursorDestroy.unwrap()(cursor) }
}
