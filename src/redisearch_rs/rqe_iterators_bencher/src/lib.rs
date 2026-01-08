/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#![allow(
    clippy::undocumented_unsafe_blocks,
    clippy::missing_safety_doc,
    clippy::multiple_unsafe_ops_per_block
)]

pub mod benchers;
pub mod ffi;

// Some of the missing C symbols are actually Rust-provided.
pub use redisearch_rs;

redis_mock::bind_redis_alloc_symbols_to_mock_impl!();

/// Define an empty stub function for each given symbols.
/// This is used to define C functions the linker requires but which are not actually used by the benchers.
macro_rules! stub_c_fn {
    ($($fn_name:ident),* $(,)?) => {
        $(
            #[unsafe(no_mangle)]
            pub extern "C" fn $fn_name() {
                panic!(concat!(stringify!($fn_name), " should not be called by any of the benchmarks"));
            }
        )*
    };
}

// Those C symbols are required for the c benchmarking code to build and run.
// They have been added by adding them until it runs fine.
stub_c_fn! {
  ERR_clear_error,
  ERR_peek_last_error,
  ERR_reason_error_string,
  SSL_connect,
  SSL_ctrl,
  SSL_CTX_ctrl,
  SSL_CTX_free,
  SSL_CTX_load_verify_locations,
  SSL_CTX_new,
  SSL_CTX_set_default_passwd_cb,
  SSL_CTX_set_default_passwd_cb_userdata,
  SSL_CTX_set_default_verify_paths,
  SSL_CTX_set_options,
  SSL_CTX_set_verify,
  SSL_CTX_use_certificate_chain_file,
  SSL_CTX_use_PrivateKey_file,
  SSL_free,
  SSL_get_error,
  SSL_new,
  SSL_read,
  SSL_set_connect_state,
  SSL_set_fd,
  SSL_write,
  TLS_client_method,
}
