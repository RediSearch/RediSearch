/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! This module encapsulates Resources from the Redis API in RAII manners.
//!
//! It provides a guard and a closure way to use Redis resources safely.

use std::ops::{Deref, DerefMut};

use super::ftor_wrap;

type RedisModuleCtxPtr = *mut crate::RedisModuleCtx;
type RedisModuleStringPtr = *mut crate::RedisModuleString;

#[derive(Debug, Default, thiserror::Error)]
pub enum RedisResourceError {
    #[default]
    #[error("Redis resource create/access error")]
    CreateOrAccessError,
}

/// Error type for the [`RedisResource::use_with`] method.
#[derive(Debug, thiserror::Error)]
pub enum WithError<E> {
    /// Error from using Resources over the Redis API
    Resource(RedisResourceError),

    /// Error from the user code in the closure, i.e. the rust code
    Closure(E),
}

pub trait RedisResource<T, P>: Deref<Target = *mut T> + DerefMut<Target = *mut T> {
    fn new(params: P) -> Result<Self, RedisResourceError>
    where
        Self: Sized;

    fn use_with<F, R, E>(params: P, f: F) -> Result<R, WithError<E>>
    where
        Self: Sized,
        F: FnOnce(*mut T) -> Result<R, E>,
    {
        let obj = Self::new(params).map_err(WithError::Resource)?;
        let ret = f(*obj).map_err(WithError::Closure)?;
        Ok(ret)
    }
}

macro_rules! impl_deref_traits_for_redis_resource {
    (
        $struct_name:ident,
        $c_type:ty$(,)?                   // Underlying C type (e.g., ffi::RedisModuleKey)
    ) => {
        // Implement Deref for pointer access
        impl Deref for $struct_name {
            type Target = *mut $c_type;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        // Implement DerefMut for $struct_name {
        impl DerefMut for $struct_name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }
    };
}

pub struct RedisString(*mut crate::RedisModuleString, *mut crate::RedisModuleCtx);

impl RedisResource<crate::RedisModuleString, (RedisModuleCtxPtr, *const std::ffi::c_char, usize)>
    for RedisString
{
    fn new(
        params: (RedisModuleCtxPtr, *const std::ffi::c_char, usize),
    ) -> Result<Self, RedisResourceError> {
        let (ctx, str, strlen) = params;
        let resource = ftor_wrap::create_string(ctx, str, strlen);
        if resource.is_null() {
            Err(RedisResourceError::CreateOrAccessError)
        } else {
            Ok(RedisString(resource, ctx))
        }
    }
}

impl_deref_traits_for_redis_resource!(RedisString, crate::RedisModuleString);

macro_rules! define_redis_resource {
    (
        $struct_name:ident,           // Name of the struct (e.g., RedisKey)
        $c_type:ty,                   // Underlying C type (e.g., ffi::RedisModuleKey)
        $create_fn:expr,              // Function to create the resource (e.g., redis_module::open_key)
        $params_type:ty,              // Parameter tuple types for creation function (e.g., RedisKeyParams)
        ( $($param_name:ident),+ ),   // Tuple destructuring names for creation function params (e.g., ctx, keyname, mode)
        $cleanup_fn:expr$(,)?           // Function to free the resource (e.g., redis_module::close_key)
    ) => {
        // Define the struct to wrap the raw pointer
        #[derive(Debug)]
        pub struct $struct_name(*mut $c_type);

        // Implement the RedisResource trait
        impl RedisResource<$c_type, $params_type> for $struct_name {
            fn new(params: $params_type) -> Result<Self, RedisResourceError> {
                let ( $($param_name),* ) = params; // Destructure the tuple
                let resource = $create_fn( $($param_name),* ); // Call create_fn with unpacked params
                if resource.is_null() {
                    Err(Default::default()) // Assumes error type implements Default
                } else {
                    Ok($struct_name(resource))
                }
            }
        }

        // Implement Drop for cleanup
        impl Drop for $struct_name {
            fn drop(&mut self) {
                $cleanup_fn(self.0);
            }
        }

        impl_deref_traits_for_redis_resource!($struct_name, $c_type);
    };

    (
        $struct_name:ident,           // Name of the struct (e.g., RedisKey)
        $c_type:ty,                   // Underlying C type (e.g., ffi::RedisModuleKey)
        $create_fn:expr,              // Function to create the resource (e.g., redis_module::open_key)
        $close_fn:expr$(,)?           // Function to free the resource (e.g., redis_module::close_key)
    ) => {
        // Define the struct to wrap the raw pointer
        #[derive(Debug)]
        pub struct $struct_name(*mut $c_type);

        // Implement the RedisResource trait
        impl RedisResource<$c_type, ()> for $struct_name {
            fn new(_: ()) -> Result<Self, RedisResourceError> {
                let resource = $create_fn(); // Call create_fn with unpacked params
                if resource.is_null() {
                    Err(Default::default()) // Assumes error type implements Default
                } else {
                    Ok($struct_name(resource))
                }
            }
        }

        // Implement Drop for cleanup
        impl Drop for $struct_name {
            fn drop(&mut self) {
                $close_fn(self.0);
            }
        }

        impl_deref_traits_for_redis_resource!($struct_name, $c_type);
    };
}

define_redis_resource!(
    RedisKey,                                       // Struct name
    crate::RedisModuleKey,                          // C type
    ftor_wrap::open_key,                            // Creation function
    (RedisModuleCtxPtr, RedisModuleStringPtr, i32), // Params (adjust based on actual API)
    (ctx, keyname, mode),                           // Tuple destructuring names
    ftor_wrap::close_key,                           // Cleanup function
);

define_redis_resource!(
    RedisScanCursor,                // Struct name
    crate::RedisModuleScanCursor,   // C type
    ftor_wrap::scan_cursor_create,  // Creation function
    ftor_wrap::scan_cursor_destroy  // Cleanup function
);

define_redis_resource!(
    RedisCallReply_Hgetall,      // Struct name
    crate::RedisModuleCallReply, // C type
    ftor_wrap::call1,            // Creation function
    (
        RedisModuleCtxPtr,
        c_cchar_ptr,
        c_cchar_ptr,
        RedisModuleStringPtr
    ), // Param types
    (ctx, cmd, fmt, keyname),    // Tuple destructuring names
    ftor_wrap::call_reply_free   // Cleanup function
);

// example for a call with zero arguments:
type c_cchar_ptr = *const std::ffi::c_char;
define_redis_resource!(
    RedisCallReply0,                               // Struct name
    crate::RedisModuleCallReply,                   // C type
    ftor_wrap::call0,                              // Creation function
    (RedisModuleCtxPtr, c_cchar_ptr, c_cchar_ptr), // Param types
    (ctx, cmd, fmt),                               // Tuple destructuring names
    ftor_wrap::call_reply_free                     // Cleanup function
);
