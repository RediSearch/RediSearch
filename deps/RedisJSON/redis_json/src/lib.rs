/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

extern crate redis_module;

use redis_module::native_types::RedisType;
use redis_module::raw::RedisModuleTypeMethods;
#[cfg(not(feature = "as-library"))]
use redis_module::AclCategory;
#[cfg(not(feature = "as-library"))]
use redis_module::InfoContext;

#[cfg(not(feature = "as-library"))]
use redis_module::Status;
#[cfg(not(feature = "as-library"))]
use redis_module::{Context, RedisResult};

#[cfg(not(feature = "as-library"))]
use redis_module::key::KeyFlags;

#[cfg(not(feature = "as-library"))]
use crate::c_api::{
    get_llapi_ctx, json_api_alloc_json, json_api_free_iter, json_api_free_json,
    json_api_free_key_values_iter, json_api_get, json_api_get_at, json_api_get_boolean,
    json_api_get_double, json_api_get_int, json_api_get_json, json_api_get_json_from_iter,
    json_api_get_key_value, json_api_get_len, json_api_get_string, json_api_get_type,
    json_api_is_json, json_api_len, json_api_next, json_api_next_key_value,
    json_api_open_key_internal, json_api_open_key_with_flags_internal, json_api_reset_iter,
    LLAPI_CTX,
};

use crate::commands::{
    json_arr_append_command_impl, json_arr_index_command_impl, json_arr_insert_command_impl,
    json_arr_len_command_impl, json_arr_pop_command_impl, json_arr_trim_command_impl,
    json_bool_toggle_command_impl, json_clear_command_impl, json_debug_command_impl,
    json_del_command_impl, json_get_command_impl, json_merge_command_impl, json_mget_command_impl,
    json_mset_command_impl, json_num_incrby_command_impl, json_num_multby_command_impl,
    json_num_powby_command_impl, json_obj_keys_command_impl, json_obj_len_command_impl,
    json_resp_command_impl, json_set_command_impl, json_str_append_command_impl,
    json_str_len_command_impl, json_type_command_impl,
};
use crate::redisjson::Format;

mod array_index;
mod backward;
pub mod c_api;
pub mod commands;
pub mod defrag;
pub mod error;
mod formatter;
pub mod ivalue_manager;
mod key_value;
pub mod manager;
pub mod redisjson;

pub const GIT_SHA: Option<&str> = std::option_env!("GIT_SHA");
pub const GIT_BRANCH: Option<&str> = std::option_env!("GIT_BRANCH");
pub const MODULE_NAME: &str = "ReJSON";
pub const MODULE_TYPE_NAME: &str = "ReJSON-RL";

pub const REDIS_JSON_TYPE_VERSION: i32 = 3;

pub static REDIS_JSON_TYPE: RedisType = RedisType::new(
    MODULE_TYPE_NAME,
    REDIS_JSON_TYPE_VERSION,
    RedisModuleTypeMethods {
        version: redis_module::TYPE_METHOD_VERSION,

        rdb_load: Some(redisjson::type_methods::rdb_load),
        rdb_save: Some(redisjson::type_methods::rdb_save),
        aof_rewrite: None, // TODO add support
        free: Some(redisjson::type_methods::free),

        // Currently unused by Redis
        mem_usage: Some(redisjson::type_methods::mem_usage),
        digest: None,

        // Auxiliary data (v2)
        aux_load: None,
        aux_save: None,
        aux_save_triggers: 0,

        free_effort: None,
        unlink: None,
        copy: Some(redisjson::type_methods::copy),
        defrag: Some(defrag::defrag),

        free_effort2: None,
        unlink2: None,
        copy2: None,
        mem_usage2: None,
        aux_save2: None,
    },
);
/////////////////////////////////////////////////////

#[macro_export]
macro_rules! run_on_manager {
    (
    pre_command: $pre_command_expr:expr,
    get_manage: {
        $( $condition:expr => $manager_ident:ident { $($field:ident: $value:expr),* $(,)? } ),* $(,)?
        _ => $default_manager:expr $(,)?
    },
    run: $run_expr:expr,
    ) => {{
        $pre_command_expr();

        $(
            if $condition {
                let mngr = $manager_ident {
                    $( $field: $value, )*
                };
                return $run_expr(mngr);
            }
        )*

        // Handle default case (Option<Manager>)
        match $default_manager {
            Some(mngr) => $run_expr(mngr),
            None => {
                let mngr = $crate::ivalue_manager::RedisIValueJsonKeyManager {
                    phantom: PhantomData,
                };
                $run_expr(mngr)
            }
        }
    }};
}

#[macro_export]
macro_rules! redis_json_module_create {
    (
        data_types: [
            $($data_type:ident),* $(,)*
        ],
        pre_command_function: $pre_command_function_expr:expr,
        get_manage: {
            $( $condition:expr => $manager_ident:ident { $($field:ident: $value:expr),* $(,)? } ),* $(,)?
            _ => $default_manager:expr $(,)?
        },
        version: $version:expr,
        init: $init_func:expr,
        info: $info_func:ident,
    ) => {

        use redis_module::RedisString;
        use std::marker::PhantomData;
        use std::os::raw::{c_double, c_int, c_longlong};
        use redis_module::raw as rawmod;
        use rawmod::ModuleOptions;
        use redis_module::redis_module;
        use redis_module::logging::RedisLogLevel;

        use std::{
            ffi::{CStr, CString},
            os::raw::{c_char, c_void},
        };
        use libc::size_t;
        use std::collections::HashMap;
        use $crate::c_api::create_rmstring;

        macro_rules! json_command {
            ($cmd:ident) => {
                |ctx: &Context, args: Vec<RedisString>| -> RedisResult {
                    run_on_manager!(
                        pre_command: ||$pre_command_function_expr(ctx, &args),
                        get_manage: {
                            $( $condition => $manager_ident { $($field: $value),* } ),*
                            _ => $default_manager
                        },
                        run: |mngr|$cmd(mngr, ctx, args),
                    )
                }
            };
        }

        // Wrap the commands in the macro that will generate the command info and register the command itself
        // We can't call json_command from commands.rs

        json_get_command!(
            pub fn json_get(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
                json_command!(json_get_command_impl)(ctx, args)
            }
        );

        json_set_command!(
            pub fn json_set(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
                json_command!(json_set_command_impl)(ctx, args)
            }
        );

        json_merge_command!(
            pub fn json_merge(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
                json_command!(json_merge_command_impl)(ctx, args)
            }
        );

        json_mset_command!(
            pub fn json_mset(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
                json_command!(json_mset_command_impl)(ctx, args)
            }
        );

        json_mget_command!(
            pub fn json_mget(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
                json_command!(json_mget_command_impl)(ctx, args)
            }
        );

        json_type_command!(
            pub fn json_type(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
                json_command!(json_type_command_impl)(ctx, args)
            }
        );

        json_del_command!(
            "json.del",
            pub fn json_del(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
                json_command!(json_del_command_impl)(ctx, args)
            }
        );

        json_del_command!(
            "json.forget",
            pub fn json_forget(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
                json_del(ctx, args)
            }
        );

        json_numincrby_command!(
            pub fn json_num_incrby(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
                json_command!(json_num_incrby_command_impl)(ctx, args)
            }
        );

        json_nummultby_command!(
            pub fn json_num_multby(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
                json_command!(json_num_multby_command_impl)(ctx, args)
            }
        );

        json_numpowby_command!(
            pub fn json_num_powby(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
                json_command!(json_num_powby_command_impl)(ctx, args)
            }
        );

        json_toggle_command!(
            pub fn json_toggle(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
                json_command!(json_bool_toggle_command_impl)(ctx, args)
            }
        );

        json_strappend_command!(
            pub fn json_strappend(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
                json_command!(json_str_append_command_impl)(ctx, args)
            }
        );

        json_strlen_command!(
            pub fn json_strlen(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
                json_command!(json_str_len_command_impl)(ctx, args)
            }
        );

        json_arrappend_command!(
            pub fn json_arrappend(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
                json_command!(json_arr_append_command_impl)(ctx, args)
            }
        );

        json_arrindex_command!(
            pub fn json_arrindex(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
                json_command!(json_arr_index_command_impl)(ctx, args)
            }
        );

        json_arrinsert_command!(
            pub fn json_arrinsert(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
                json_command!(json_arr_insert_command_impl)(ctx, args)
            }
        );

        json_arrlen_command!(
            pub fn json_arrlen(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
                json_command!(json_arr_len_command_impl)(ctx, args)
            }
        );

        json_arrpop_command!(
            pub fn json_arrpop(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
                json_command!(json_arr_pop_command_impl)(ctx, args)
            }
        );

        json_arrtrim_command!(
            pub fn json_arrtrim(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
                json_command!(json_arr_trim_command_impl)(ctx, args)
            }
        );

        json_objkeys_command!(
            pub fn json_objkeys(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
                json_command!(json_obj_keys_command_impl)(ctx, args)
            }
        );

        json_objlen_command!(
            pub fn json_objlen(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
                json_command!(json_obj_len_command_impl)(ctx, args)
            }
        );

        json_clear_command!(
            pub fn json_clear(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
                json_command!(json_clear_command_impl)(ctx, args)
            }
        );

        json_debug_command!(
            pub fn json_debug(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
                json_command!(json_debug_command_impl)(ctx, args)
            }
        );

        json_resp_command!(
            pub fn json_resp(ctx: &Context, args: Vec<RedisString>) -> RedisResult {
                json_command!(json_resp_command_impl)(ctx, args)
            }
        );

        #[cfg(not(test))]
        macro_rules! get_allocator {
            () => {
                redis_module::alloc::RedisAlloc
            };
        }

        #[cfg(test)]
        macro_rules! get_allocator {
            () => {
                std::alloc::System
            };
        }

        redis_json_module_export_shared_api! {
            get_manage: {
                $( $condition => $manager_ident { $($field: $value),* } ),*
                _ => $default_manager
            },
            pre_command_function: $pre_command_function_expr,
        }

        fn initialize(ctx: &Context, args: &[RedisString]) -> Status {
            $crate::setup_panic_handler();

            ctx.log_notice(&format!("version: {} git sha: {} branch: {}",
                $version,
                match GIT_SHA { Some(val) => val, _ => "unknown"},
                match GIT_BRANCH { Some(val) => val, _ => "unknown"},
                ));
            export_shared_api(ctx);
            ctx.set_module_options(ModuleOptions::HANDLE_IO_ERRORS);
            ctx.log_notice("Enabled diskless replication");
            // Always enable thread-safe cache for ASM support
            // RwLock has zero overhead when there's no contention
            if let Err(e) = $crate::init_ijson_shared_string_cache(true) {
                ctx.log(RedisLogLevel::Warning, &format!("Failed initializing shared string cache, {e}."));
                return Status::Err;
            }
            ctx.log_notice("Initialized shared string cache, thread safe: true.");
            $init_func(ctx, args)
        }

        fn json_init_config(ctx: &Context, args: &[RedisString]) -> Status{
            if args.len() % 2 != 0 {
                ctx.log(RedisLogLevel::Warning, "RedisJson arguments must be key:value pairs");
                return Status::Err;
            }
            let mut args_map = HashMap::<String, String>::new();
            for i in (0..args.len()).step_by(2) {
                args_map.insert(args[i].to_string_lossy(), args[i + 1].to_string_lossy());
            }

            Status::Ok
        }

        use AclCategory as ACL;
        redis_module! {
            name: $crate::MODULE_NAME,
            version: $version,
            allocator: (get_allocator!(), get_allocator!()),
            data_types: [$($data_type,)*],
            acl_categories: [ACL::from("json"), ],
            init: json_init_config,
            init: initialize,
            info: $info_func,
        }
    }
}

#[cfg(not(feature = "as-library"))]
const fn pre_command(_ctx: &Context, _args: &[RedisString]) {}

#[cfg(not(feature = "as-library"))]
const fn dummy_init(_ctx: &Context, _args: &[RedisString]) -> Status {
    Status::Ok
}

pub fn init_ijson_shared_string_cache(is_bigredis: bool) -> Result<(), String> {
    ijson::init_shared_string_cache(is_bigredis)
}

pub fn setup_panic_handler() {
    use redis_module::logging::log_warning;
    use std::panic;

    let default_hook = panic::take_hook();

    panic::set_hook(Box::new(move |panic_info| {
        let payload = if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "Unknown panic payload".to_string()
        };

        let location = panic_info
            .location()
            .map(|location| {
                format!(
                    " at {}:{}:{}",
                    location.file(),
                    location.line(),
                    location.column()
                )
            })
            .unwrap_or("UNKNOWN PANIC LOCATION".to_string());

        let message = format!("PANIC in RedisJSON module: {payload}{location}");

        log_warning(&message);
        default_hook(panic_info);
    }));
}

#[cfg(not(feature = "as-library"))]
const fn dummy_info(_ctx: &InfoContext, _for_crash_report: bool) {}

const fn version() -> i32 {
    let string = env!("CARGO_PKG_VERSION");
    let mut bytes = string.as_bytes();
    let mut value: i32 = 0;
    let mut result = 0;
    let mut multiplier = 10000;

    while let [byte, rest @ ..] = bytes {
        bytes = rest;
        match byte {
            b'0'..=b'9' => {
                value = value * 10 + (*byte - b'0') as i32;
            }
            b'.' => {
                result += value * multiplier;
                multiplier /= 100;
                value = 0;
            }
            _ => {
                // The provided string is not a valid version specification.
                unreachable!()
            }
        }
    }

    result + value
}

#[cfg(not(feature = "as-library"))]
redis_json_module_create! {
    data_types: [REDIS_JSON_TYPE],
    pre_command_function: pre_command,
    get_manage: {
    _ => Some(crate::ivalue_manager::RedisIValueJsonKeyManager {
        phantom: PhantomData,
    })
    },
    version: version(),
    init: dummy_init,
    info: dummy_info,
}
