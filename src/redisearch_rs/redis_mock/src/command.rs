/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{
    collections::HashMap,
    ffi::c_char,
    sync::{LazyLock, RwLock},
};

use crate::mock::REDISMODULE_OK;

pub(crate) struct RedisModuleCommand {
    _name: String,
    handler: ffi::RedisModuleCmdFunc,
    sub_commands: HashMap<String, RedisModuleCommand>,
}

static MY_API_CMDS: LazyLock<RwLock<HashMap<String, RedisModuleCommand>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
pub(crate) unsafe extern "C" fn RedisModule_CreateCommand(
    _ctx: *mut ffi::RedisModuleCtx,
    name: *const c_char,
    handler: ffi::RedisModuleCmdFunc,
    _strflags: *const c_char,
    _firstkey: i32,
    _lastkey: i32,
    _keystep: i32,
) -> i32 {
    // Safety: The caller must ensure that `name` is a valid C string.
    let slice = unsafe { std::ffi::CStr::from_ptr(name.cast()) };
    let name = String::from_utf8_lossy(slice.to_bytes()).to_string();
    let cmd = RedisModuleCommand {
        _name: name.clone(),
        handler,
        sub_commands: HashMap::new(),
    };

    let mut commands = MY_API_CMDS
        .write()
        .expect("Failed to acquire write lock on MY_API_CMDS");
    commands.insert(name, cmd);

    crate::mock::REDISMODULE_OK
}

#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
pub(crate) unsafe extern "C" fn RedisModule_GetCommand(
    _ctx: *mut ffi::RedisModuleCtx,
    name: *const c_char,
) -> *mut RedisModuleCommand {
    // Safety: The caller must ensure that `name` is a valid C string.
    let slice = unsafe { std::ffi::CStr::from_ptr(name.cast()) };
    let name = String::from_utf8_lossy(slice.to_bytes()).to_string();
    {
        let mut commands = MY_API_CMDS
            .write()
            .expect("Failed to acquire read lock on MY_API_CMDS");

        match commands.get_mut(name.as_str()) {
            Some(cmd) => {
                // Convert Command to ffi::RedisModuleCommand
                // This is a placeholder, as the actual conversion depends on the ffi definition
                // and how RedisModuleCommand is structured.
                // You would need to implement this conversion based on your actual ffi definitions.
                cmd as *const RedisModuleCommand as *mut RedisModuleCommand // Placeholder for conversion logic
                //return cmd.handler as *mut ffi::RedisModuleCommand; // Assuming handler is the command pointer
            }
            None => std::ptr::null_mut(), // Return null if command not found
        }
    }
}

#[unsafe(no_mangle)]
#[allow(non_upper_case_globals)]
pub(crate) unsafe extern "C" fn RedisModule_CreateSubcommand(
    parent: *mut RedisModuleCommand,
    name: *const c_char,
    handler: ffi::RedisModuleCmdFunc,
    _strflags: *const c_char,
    _firstkey: i32,
    _lastkey: i32,
    _keystep: i32,
) -> i32 {
    if parent.is_null() {
        return crate::mock::REDISMODULE_ERR; // Return error if parent command is null
    }

    // Safety: The caller must ensure that `name` is a valid C string.
    let slice = unsafe { std::ffi::CStr::from_ptr(name.cast()) };
    let name = String::from_utf8_lossy(slice.to_bytes()).to_string();

    // Safety: The caller must ensure that parent is a valid ptr
    let parent: &mut RedisModuleCommand = unsafe { &mut *parent };
    if parent.handler.is_some() || parent.sub_commands.contains_key(&name) {
        return crate::mock::REDISMODULE_ERR;
    }

    let sub_command = RedisModuleCommand {
        _name: name.clone(),
        handler,
        sub_commands: HashMap::new(),
    };
    parent.sub_commands.insert(name, sub_command);

    REDISMODULE_OK
}
