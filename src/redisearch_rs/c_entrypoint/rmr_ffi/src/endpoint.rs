/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The [`MREndpoint`] of a cluster node: its address and the strings it owns.
//!
//! The strings are allocated with `RedisModule_Alloc` and freed with
//! `RedisModule_Free`, as the C code that assigns them directly (such as
//! `unixSock`) does.

use std::ffi::{CStr, c_char, c_int};
use std::ptr::{self, NonNull};

use ffi::{MREndpoint, REDIS_ERR, REDIS_OK};

/// The parts of a `[password@]host:port` address, as [`parse_address`] splits it.
#[derive(Debug, PartialEq, Eq)]
pub struct Address<'a> {
    /// Everything before the first `@`, possibly empty, if the address has one.
    pub password: Option<&'a [u8]>,
    /// The host, without the brackets of a bracketed one.
    pub host: &'a [u8],
    pub port: u16,
}

/// Splits a `[password@]host:port` address as [`MREndpoint_Parse`] describes, or
/// returns `None` if it is malformed.
pub fn parse_address(addr: &[u8]) -> Option<Address<'_>> {
    let (password, addr) = match addr.iter().position(|&byte| byte == b'@') {
        Some(at) => (Some(&addr[..at]), &addr[at + 1..]),
        None => (None, addr),
    };
    let (bracketed, addr) = match addr.strip_prefix(b"[") {
        Some(rest) => (true, rest),
        None => (false, addr),
    };

    let colon = addr.iter().rposition(|&byte| byte == b':')?;
    let host = &addr[..colon];
    let host = if bracketed {
        host.strip_suffix(b"]")?
    } else {
        host
    };

    let port = u16::try_from(atoi(&addr[colon + 1..]))
        .ok()
        .filter(|&port| port != 0)?;
    Some(Address {
        password,
        host,
        port,
    })
}

/// Reads the leading integer of `digits` as C's `atoi` does: after optional C
/// whitespace and a sign, up to the first non-digit, and 0 if there is none. A
/// value beyond [`i64`] saturates rather than wrapping.
fn atoi(digits: &[u8]) -> i64 {
    let digits = match digits
        .iter()
        .position(|&byte| !matches!(byte, b' ' | b'\t' | b'\n' | 0x0B | 0x0C | b'\r'))
    {
        Some(start) => &digits[start..],
        None => return 0,
    };
    let (negative, digits) = match digits.split_first() {
        Some((b'-', rest)) => (true, rest),
        Some((b'+', rest)) => (false, rest),
        _ => (false, digits),
    };
    let magnitude =
        digits
            .iter()
            .take_while(|byte| byte.is_ascii_digit())
            .fold(0i64, |value, &digit| {
                value
                    .saturating_mul(10)
                    .saturating_add(i64::from(digit - b'0'))
            });
    if negative { -magnitude } else { magnitude }
}

/// Parses `addr`, a `[password@]host:port` address, into `ep`, as a TCP endpoint.
///
/// The password ends at the first `@`, and the port starts after the last `:`, so
/// an unbracketed IPv6 host works too. A host in brackets has them removed. The
/// port is read like C's `atoi` and must be in `1..=65535`.
///
/// Returns [`REDIS_OK`], or [`REDIS_ERR`] with every field of `ep` zeroed if
/// `addr` is malformed.
///
/// # Safety
///
/// 1. `addr` must be a [valid] pointer to a nul-terminated string.
/// 2. `ep` must be [valid] for writes. Its previous content is overwritten, not
///    freed.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn MREndpoint_Parse(addr: *const c_char, ep: *mut MREndpoint) -> c_int {
    debug_assert!(!addr.is_null() && !ep.is_null());

    // SAFETY: ensured by caller (1.)
    let addr = unsafe { CStr::from_ptr(addr) };
    let parsed = parse_address(addr.to_bytes());

    let endpoint = match &parsed {
        Some(address) => MREndpoint {
            host: rm_strdup(address.host),
            port: c_int::from(address.port),
            isTls: false,
            unixSock: ptr::null_mut(),
            password: address.password.map_or(ptr::null_mut(), rm_strdup),
        },
        None => empty(),
    };
    // SAFETY: ensured by caller (2.)
    unsafe { ep.write(endpoint) };

    if parsed.is_some() {
        REDIS_OK as c_int
    } else {
        REDIS_ERR as c_int
    }
}

/// Copies `src` into `dst`, with copies of its strings, so that freeing either
/// endpoint leaves the other intact.
///
/// # Safety
///
/// 1. `src` must be a [valid] pointer to an [`MREndpoint`] whose non-null strings
///    are nul-terminated.
/// 2. `dst` must be [valid] for writes and not overlap `src`. Its previous content
///    is overwritten, not freed.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn MREndpoint_Copy(dst: *mut MREndpoint, src: *const MREndpoint) {
    debug_assert!(!dst.is_null() && !src.is_null());

    // SAFETY: ensured by caller (1.)
    let src = unsafe { src.read() };
    // SAFETY: the strings of `src` are nul-terminated or null (1.)
    let host = unsafe { rm_strdup_c(src.host) };
    // SAFETY: as above.
    let unix_sock = unsafe { rm_strdup_c(src.unixSock) };
    // SAFETY: as above.
    let password = unsafe { rm_strdup_c(src.password) };
    let copy = MREndpoint {
        host,
        unixSock: unix_sock,
        password,
        ..src
    };
    // SAFETY: ensured by caller (2.)
    unsafe { dst.write(copy) };
}

/// Frees the strings of `ep` and resets them to null. The endpoint itself, usually
/// on the stack or embedded in another struct, is not freed.
///
/// # Safety
///
/// 1. `ep` must be a [valid] pointer to an [`MREndpoint`] whose non-null strings
///    were allocated with `RedisModule_Alloc` and are not referenced elsewhere.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn MREndpoint_Free(ep: *mut MREndpoint) {
    debug_assert!(!ep.is_null());

    // SAFETY: ensured by caller (1.)
    let ep = unsafe { &mut *ep };
    for string in [&mut ep.host, &mut ep.unixSock, &mut ep.password] {
        // SAFETY: the string was allocated with `RedisModule_Alloc` and has no other
        // owner (1.)
        unsafe { rm_free(*string) };
        *string = ptr::null_mut();
    }
}

/// Returns whether `a` and `b` describe the same endpoint: the same port, TLS flag,
/// host, Unix socket and password, where a null string only equals null. Two null
/// endpoints are equal; a null and a non-null one are not.
///
/// # Safety
///
/// 1. `a` and `b` must each be null or a [valid] pointer to an [`MREndpoint`]
///    whose non-null strings are nul-terminated.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn MREndpoint_Equal(a: *const MREndpoint, b: *const MREndpoint) -> bool {
    if ptr::eq(a, b) {
        return true;
    }
    // SAFETY: ensured by caller (1.)
    let a = unsafe { a.as_ref() };
    // SAFETY: ensured by caller (1.)
    let b = unsafe { b.as_ref() };
    let (Some(a), Some(b)) = (a, b) else {
        return false;
    };
    // SAFETY: the strings of both endpoints are nul-terminated or null (1.)
    let strings_eq = |x, y| unsafe { c_str_eq(x, y) };
    a.port == b.port
        && a.isTls == b.isTls
        && strings_eq(a.host, b.host)
        && strings_eq(a.unixSock, b.unixSock)
        && strings_eq(a.password, b.password)
}

const fn empty() -> MREndpoint {
    MREndpoint {
        host: ptr::null_mut(),
        port: 0,
        isTls: false,
        unixSock: ptr::null_mut(),
        password: ptr::null_mut(),
    }
}

/// Returns a nul-terminated copy of `bytes`, allocated with `RedisModule_Alloc`.
fn rm_strdup(bytes: &[u8]) -> *mut c_char {
    // SAFETY: the Redis Module API is initialized before any endpoint is created.
    let alloc = unsafe { redis_module::RedisModule_Alloc }.expect("Redis allocator not available");
    // SAFETY: the size is non-zero.
    let copy = NonNull::new(unsafe { alloc(bytes.len() + 1) })
        .expect("RedisModule_Alloc returned NULL")
        .cast::<u8>();
    // SAFETY: `copy` was just allocated with room for `bytes` and a terminator, and
    // cannot overlap `bytes`.
    unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), copy.as_ptr(), bytes.len()) };
    // SAFETY: the terminator's byte is within the allocation.
    let terminator = unsafe { copy.add(bytes.len()) };
    // SAFETY: as above.
    unsafe { terminator.write(0) };
    copy.as_ptr().cast()
}

/// Returns a copy of the C string `string`, or null if it is null.
///
/// # Safety
///
/// 1. `string` must be null or a [valid] pointer to a nul-terminated string.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe fn rm_strdup_c(string: *const c_char) -> *mut c_char {
    if string.is_null() {
        return ptr::null_mut();
    }
    // SAFETY: ensured by caller (1.)
    rm_strdup(unsafe { CStr::from_ptr(string) }.to_bytes())
}

/// Frees `string` with `RedisModule_Free`, if it is not null.
///
/// # Safety
///
/// 1. `string` must be null or allocated with `RedisModule_Alloc` and not used
///    afterwards.
unsafe fn rm_free(string: *mut c_char) {
    if string.is_null() {
        return;
    }
    // SAFETY: the Redis Module API is initialized before any endpoint is created.
    let free = unsafe { redis_module::RedisModule_Free }.expect("Redis allocator not available");
    // SAFETY: ensured by caller (1.)
    unsafe { free(string.cast()) };
}

/// Returns whether two C strings are equal, where null only equals null.
///
/// # Safety
///
/// 1. `a` and `b` must each be null or a [valid] pointer to a nul-terminated
///    string.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
unsafe fn c_str_eq(a: *const c_char, b: *const c_char) -> bool {
    match (a.is_null(), b.is_null()) {
        (true, true) => true,
        (false, false) => {
            // SAFETY: `a` is non-null and nul-terminated (1.)
            let a = unsafe { CStr::from_ptr(a) };
            // SAFETY: `b` is non-null and nul-terminated (1.)
            let b = unsafe { CStr::from_ptr(b) };
            a == b
        }
        _ => false,
    }
}
