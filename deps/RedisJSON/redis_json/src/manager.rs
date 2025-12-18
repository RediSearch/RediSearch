/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

use json_path::select_value::SelectValue;
use redis_module::key::KeyFlags;
use serde_json::Number;

use redis_module::raw::RedisModuleKey;
use redis_module::rediserror::RedisError;
use redis_module::{Context, RedisResult, RedisString};

use crate::Format;

use crate::error::Error;

use crate::key_value::KeyValue;

pub struct SetUpdateInfo {
    pub path: Vec<String>,
}

pub struct AddUpdateInfo {
    pub path: Vec<String>,
    pub key: String,
}

pub enum UpdateInfo {
    SUI(SetUpdateInfo),
    AUI(AddUpdateInfo),
}

pub trait ReadHolder<V: SelectValue> {
    fn get_value(&self) -> Result<Option<&V>, RedisError>;
}

pub trait WriteHolder<O: Clone, V: SelectValue> {
    fn delete(&mut self) -> Result<(), RedisError>;
    fn get_value(&mut self) -> Result<Option<&mut V>, RedisError>;
    fn set_value(&mut self, path: Vec<String>, v: O) -> Result<bool, RedisError>;
    fn merge_value(&mut self, path: Vec<String>, v: O) -> Result<bool, RedisError>;
    fn dict_add(&mut self, path: Vec<String>, key: &str, v: O) -> Result<bool, RedisError>;
    fn delete_path(&mut self, path: Vec<String>) -> Result<bool, RedisError>;
    fn incr_by(&mut self, path: Vec<String>, num: &str) -> Result<Number, RedisError>;
    fn mult_by(&mut self, path: Vec<String>, num: &str) -> Result<Number, RedisError>;
    fn pow_by(&mut self, path: Vec<String>, num: &str) -> Result<Number, RedisError>;
    fn bool_toggle(&mut self, path: Vec<String>) -> Result<bool, RedisError>;
    fn str_append(&mut self, path: Vec<String>, val: String) -> Result<usize, RedisError>;
    fn arr_append(&mut self, path: Vec<String>, args: Vec<O>) -> Result<usize, RedisError>;
    fn arr_insert(
        &mut self,
        path: Vec<String>,
        args: &[O],
        index: i64,
    ) -> Result<usize, RedisError>;
    fn arr_pop<C: FnOnce(Option<&V>) -> RedisResult>(
        &mut self,
        path: Vec<String>,
        index: i64,
        serialize_callback: C,
    ) -> RedisResult;
    fn arr_trim(&mut self, path: Vec<String>, start: i64, stop: i64) -> Result<usize, RedisError>;
    fn clear(&mut self, path: Vec<String>) -> Result<usize, RedisError>;
    fn notify_keyspace_event(&mut self, ctx: &Context, command: &str) -> Result<(), RedisError>;
}

pub trait Manager {
    /* V - SelectValue that the json path library can work on
     * O - SelectValue Holder
     * Naive implementation is that V and O are from the same type but its not
     * always possible so they are separated
     */
    type V: SelectValue;
    type O: Clone;
    type WriteHolder: WriteHolder<Self::O, Self::V>;
    type ReadHolder: ReadHolder<Self::V>;
    fn open_key_read(
        &self,
        ctx: &Context,
        key: &RedisString,
    ) -> Result<Self::ReadHolder, RedisError>;
    fn open_key_read_with_flags(
        &self,
        ctx: &Context,
        key: &RedisString,
        flags: KeyFlags,
    ) -> Result<Self::ReadHolder, RedisError>;
    fn open_key_write(
        &self,
        ctx: &Context,
        key: RedisString,
    ) -> Result<Self::WriteHolder, RedisError>;
    fn apply_changes(&self, ctx: &Context);
    #[allow(clippy::wrong_self_convention)]
    fn from_str(&self, val: &str, format: Format, limit_depth: bool) -> Result<Self::O, Error>;
    fn get_memory(v: &Self::V) -> Result<usize, RedisError>;
    fn is_json(&self, key: *mut RedisModuleKey) -> Result<bool, RedisError>;
}

pub(crate) fn err_json<V: SelectValue>(value: &V, expected_value: &'static str) -> Error {
    Error::from(err_msg_json_expected(
        expected_value,
        KeyValue::value_name(value),
    ))
}

pub(crate) fn err_msg_json_expected(expected_value: &'static str, found: &str) -> String {
    format!("WRONGTYPE wrong type of path value - expected {expected_value} but found {found}")
}

pub(crate) fn err_msg_json_path_doesnt_exist_with_param(path: &str) -> String {
    format!("ERR Path '{path}' does not exist")
}

pub(crate) fn err_msg_json_path_doesnt_exist() -> String {
    "ERR Path does not exist".to_string()
}

pub(crate) fn err_msg_json_path_doesnt_exist_with_param_or(path: &str, or: &str) -> String {
    format!("ERR Path '{path}' does not exist or {or}")
}
