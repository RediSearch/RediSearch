//! The module entry point exported by `redisearch.so`.
//!
//! Redis loads a module by `dlsym(handle, "RedisModule_OnLoad")`. A Rust
//! `cdylib`'s auto-generated version script exports only Rust `#[no_mangle]`
//! symbols and localizes everything else — so the C core's `RedisModule_OnLoad`
//! (renamed to `RediSearch_OnLoad_Impl`) would be hidden. This Rust shim carries
//! the exported `RedisModule_OnLoad` name and forwards to the C implementation.

use std::ffi::{c_int, c_void};

unsafe extern "C" {
    /// The real module init in the C core (renamed from `RedisModule_OnLoad`).
    fn RediSearch_OnLoad_Impl(ctx: *mut c_void, argv: *mut *mut c_void, argc: c_int) -> c_int;
}

/// `redisearch.so` module entry point. Forwards to the C core init.
///
/// # Safety
/// Called by Redis with a valid module context and argument vector.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RedisModule_OnLoad(
    ctx: *mut c_void,
    argv: *mut *mut c_void,
    argc: c_int,
) -> c_int {
    // SAFETY: forwarding Redis' own arguments unchanged to the C implementation,
    // which has the identical signature.
    unsafe { RediSearch_OnLoad_Impl(ctx, argv, argc) }
}
