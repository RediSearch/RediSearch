/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

use crate::defrag::defrag_info;
use crate::error::Error;
use crate::formatter::ReplyFormatOptions;
use crate::key_value::KeyValue;
use crate::manager::{
    err_msg_json_path_doesnt_exist_with_param, err_msg_json_path_doesnt_exist_with_param_or,
    Manager, ReadHolder, UpdateInfo, WriteHolder,
};
use crate::redisjson::{Format, Path, ReplyFormat, SetOptions, JSON_ROOT_PATH};
use json_path::select_value::{SelectValue, SelectValueType, ValueRef};
use redis_module::{Context, RedisValue};
use redis_module::{NextArg, RedisError, RedisResult, RedisString, REDIS_OK};
use std::cmp::Ordering;
use std::str::FromStr;

use json_path::{calc_once_with_paths, compile, json_path::UserPathTracker};

use serde_json::{Number, Value};

use itertools::FoldWhile::{Continue, Done};
use itertools::{EitherOrBoth, Itertools};
use serde::{Serialize, Serializer};

const CMD_ARG_NOESCAPE: &str = "NOESCAPE";
const CMD_ARG_INDENT: &str = "INDENT";
const CMD_ARG_NEWLINE: &str = "NEWLINE";
const CMD_ARG_SPACE: &str = "SPACE";
const CMD_ARG_FORMAT: &str = "FORMAT";

// Compile time evaluation of the max len() of all elements of the array
const fn max_strlen(arr: &[&str]) -> usize {
    let mut max_strlen = 0;
    let arr_len = arr.len();
    if arr_len < 1 {
        return max_strlen;
    }
    let mut pos = 0;
    while pos < arr_len {
        let curr_strlen = arr[pos].len();
        if max_strlen < curr_strlen {
            max_strlen = curr_strlen;
        }
        pos += 1;
    }
    max_strlen
}

// We use this constant to further optimize json_get command, by calculating the max subcommand length
// Any subcommand added to JSON.GET should be included on the following array
const JSONGET_SUBCOMMANDS_MAXSTRLEN: usize = max_strlen(&[
    CMD_ARG_NOESCAPE,
    CMD_ARG_INDENT,
    CMD_ARG_NEWLINE,
    CMD_ARG_SPACE,
    CMD_ARG_FORMAT,
]);

pub enum Values<'a, V: SelectValue> {
    Single(ValueRef<'a, V>),
    Multi(Vec<ValueRef<'a, V>>),
}

impl<'a, V: SelectValue> Serialize for Values<'a, V> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Values::Single(v) => v.serialize(serializer),
            Values::Multi(v) => v.serialize(serializer),
        }
    }
}

fn is_resp3(ctx: &Context) -> bool {
    ctx.get_flags()
        .contains(redis_module::ContextFlags::FLAGS_RESP3)
}

///
/// JSON.GET <key>
///         [INDENT indentation-string]
///         [NEWLINE line-break-string]
///         [SPACE space-string]
///         [FORMAT {STRING|EXPAND1|EXPAND}]      /* default is STRING */
///         [path ...]
///
#[macro_export]
macro_rules! json_get_command {
    ($item:item) => {
        #[::redis_module_macros::command(
            {
                name: "json.get",
                flags: [ReadOnly],
                acl_categories: [Read, Single("json")],
                arity: -2,
                complexity: "O(N) where N is the size of the JSON",
                since: "1.0.0",
                summary: "Get JSON value at path",
                key_spec: [
                    {
                        flags: [ReadOnly],
                        begin_search: Index({ index: 1 }),
                        find_keys: Range({ last_key: 0, steps: 1, limit: 0 }),
                    }
                ],
                args: [
                    {
                        name: "key",
                        arg_type: Key,
                        key_spec_index: 0,
                    },
                    {
                        name: "indent",
                        token: "INDENT",
                        arg_type: Block,
                        flags: [Optional],
                        subargs: [
                            {
                                name: "indent",
                                arg_type: String,
                            }
                        ]
                    },
                    {
                        name: "newline",
                        token: "NEWLINE",
                        arg_type: Block,
                        flags: [Optional],
                        subargs: [
                            {
                                name: "newline",
                                arg_type: String,
                            }
                        ]
                    },
                    {
                        name: "space",
                        token: "SPACE",
                        arg_type: Block,
                        flags: [Optional],
                        subargs: [
                            {
                                name: "space",
                                arg_type: String,
                            }
                        ]
                    },
                    {
                        name: "format",
                        token: "FORMAT",
                        arg_type: Block,
                        flags: [Optional],
                        subargs: [
                            {
                                name: "format-token",
                                arg_type: OneOf,
                                subargs: [
                                    {
                                        name: "STRING",
                                        arg_type: PureToken,
                                        token: "STRING",
                                    },
                                    {
                                        name: "EXPAND1",
                                        arg_type: PureToken,
                                        token: "EXPAND1",
                                    },
                                    {
                                        name: "EXPAND",
                                        arg_type: PureToken,
                                        token: "EXPAND",
                                    }
                                ]

                            }
                        ]
                    },
                    {
                        name: "path",
                        arg_type: String,
                        flags: [Optional, Multiple],
                    }
                ]
            }
        )]
        $item
    };
}
pub fn json_get_command_impl<M: Manager>(
    manager: M,
    ctx: &Context,
    args: Vec<RedisString>,
) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;

    // Set Capacity to 1 assuming the common case has one path
    let mut paths = Vec::with_capacity(1);

    let mut format_options = ReplyFormatOptions::new(is_resp3(ctx), ReplyFormat::STRING);

    while let Ok(arg) = args.next_str() {
        match arg {
            // fast way to consider arg a path by using the max length of all possible subcommands
            // See #390 for the comparison of this function with/without this optimization
            arg if arg.len() > JSONGET_SUBCOMMANDS_MAXSTRLEN => paths.push(Path::new(arg)),
            arg if arg.eq_ignore_ascii_case(CMD_ARG_FORMAT) => {
                if !format_options.resp3 && paths.is_empty() {
                    return Err(RedisError::Str(
                        "ERR FORMAT argument is not supported on RESP2",
                    ));
                }
                // Temporary fix until STRINGS is also supported
                let next = args.next_str()?;
                if next.eq_ignore_ascii_case("STRINGS") {
                    return Err(RedisError::Str("ERR wrong reply format"));
                }
                format_options.format = ReplyFormat::from_str(next)?;
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_INDENT) => {
                format_options.indent = Some(args.next_str()?)
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_NEWLINE) => {
                format_options.newline = Some(args.next_str()?)
            }
            arg if arg.eq_ignore_ascii_case(CMD_ARG_SPACE) => {
                format_options.space = Some(args.next_str()?)
            }
            // Silently ignore. Compatibility with ReJSON v1.0 which has this option. See #168 TODO add support
            arg if arg.eq_ignore_ascii_case(CMD_ARG_NOESCAPE) => continue,
            _ => paths.push(Path::new(arg)),
        };
    }

    // path is optional -> no path found we use legacy root "."
    if paths.is_empty() {
        paths.push(Path::default());
    }

    let key = manager.open_key_read(ctx, &key)?;
    let value = match key.get_value()? {
        Some(doc) => KeyValue::new(doc).to_json(paths, &format_options)?,
        None => RedisValue::Null,
    };

    Ok(value)
}

///
/// JSON.SET <key> <path> <json> [NX | XX | FORMAT <format>]
///
#[macro_export]
macro_rules! json_set_command {
    ($item:item) => {
        #[::redis_module_macros::command(
            {
                name: "json.set",
                flags: [Write, DenyOOM],
                acl_categories: [Write, Single("json")],
                arity: -4,
                complexity: "O(M+N) where M is the size of the original value (if it exists) and N is the size of the new value",
                since: "1.0.0",
                summary: "Set the JSON value at path in key",
                key_spec: [
                    {
                        flags: [ReadWrite],
                        begin_search: Index({ index: 1 }),
                        find_keys: Range({ last_key: 0, steps: 1, limit: 0 }),
                    }
                ],
                args: [
                    {
                        name: "key",
                        arg_type: Key,
                        key_spec_index: 0,
                    },
                    {
                        name: "path",
                        arg_type: String,
                    },
                    {
                        name: "json",
                        arg_type: String,
                    },
                    {
                        name: "condition",
                        arg_type: OneOf,
                        flags: [Optional],
                        subargs: [
                            {
                                name: "nx",
                                arg_type: PureToken,
                                token: "NX",
                            },
                            {
                                name: "xx",
                                arg_type: PureToken,
                                token: "XX",
                            }]
                        },
                        {
                            name: "format",
                            token: "FORMAT",
                            arg_type: Block,
                            flags: [Optional],
                            subargs: [
                                {
                                    name: "format-token",
                                    arg_type: OneOf,
                                    subargs: [
                                        {
                                            name: "STRING",
                                            arg_type: PureToken,
                                            token: "STRING",
                                        },
                                        {
                                            name: "JSON",
                                            arg_type: PureToken,
                                            token: "JSON",
                                        },
                                        {
                                            name: "BSON",
                                            arg_type: PureToken,
                                            token: "BSON",
                                        }
                                    ]

                                }
                            ]
                        }
                ],
            }
        )]
        $item
    };
}
pub fn json_set_command_impl<M: Manager>(
    manager: M,
    ctx: &Context,
    args: Vec<RedisString>,
) -> RedisResult {
    let mut args = args.into_iter().skip(1);

    let key = args.next_arg()?;
    let path = Path::new(args.next_str()?);
    let value = args.next_str()?;

    let mut format = Format::JSON;
    let mut set_option = SetOptions::None;

    while let Some(s) = args.next() {
        match s.try_as_str()? {
            arg if arg.eq_ignore_ascii_case("NX") && set_option == SetOptions::None => {
                set_option = SetOptions::NotExists
            }
            arg if arg.eq_ignore_ascii_case("XX") && set_option == SetOptions::None => {
                set_option = SetOptions::AlreadyExists
            }
            arg if arg.eq_ignore_ascii_case("FORMAT") => {
                format = Format::from_str(args.next_str()?)?;
            }
            _ => return Err(RedisError::Str("ERR syntax error")),
        };
    }

    let mut redis_key = manager.open_key_write(ctx, key)?;
    let current = redis_key.get_value()?;

    let val = manager.from_str(value, format, true)?;

    match (current, set_option) {
        (Some(doc), op) => {
            if path == JSON_ROOT_PATH {
                if op != SetOptions::NotExists {
                    redis_key.set_value(vec![], val)?;
                    redis_key.notify_keyspace_event(ctx, "json.set")?;
                    manager.apply_changes(ctx);
                    REDIS_OK
                } else {
                    Ok(RedisValue::Null)
                }
            } else {
                let update_info = KeyValue::new(doc).find_paths(path.get_path(), op)?;
                if !update_info.is_empty() && apply_updates::<M>(&mut redis_key, val, update_info) {
                    redis_key.notify_keyspace_event(ctx, "json.set")?;
                    manager.apply_changes(ctx);
                    REDIS_OK
                } else {
                    Ok(RedisValue::Null)
                }
            }
        }
        (None, SetOptions::AlreadyExists) => Ok(RedisValue::Null),
        _ => {
            if path == JSON_ROOT_PATH {
                redis_key.set_value(Vec::new(), val)?;
                redis_key.notify_keyspace_event(ctx, "json.set")?;
                manager.apply_changes(ctx);
                REDIS_OK
            } else {
                Err(RedisError::Str(
                    "ERR new objects must be created at the root",
                ))
            }
        }
    }
}

///
/// JSON.MERGE <key> <path> <json> [FORMAT <format>]
///
#[macro_export]
macro_rules! json_merge_command {
    ($item:item) => {
        #[::redis_module_macros::command(
            {
                name: "json.merge",
                flags: [Write, DenyOOM],
                acl_categories: [Write, Single("json")],
                arity: -4,
                complexity: "O(M+N) when path is evaluated to a single value where M is the size of the original value (if it exists) and N is the size of the new value, O(M+N) when path is evaluated to multiple values where M is the size of the key and N is the size of the new value * the number of original values in the key",
                since: "2.6.0",
                summary: "Merge a given JSON value into matching paths. Consequently, JSON values at matching paths are updated, deleted, or expanded with new children",
                key_spec: [
                    {
                        flags: [ReadWrite],
                        begin_search: Index({ index: 1 }),
                        find_keys: Range({ last_key: 0, steps: 1, limit: 0 }),
                    }
                ],
                args: [
                    {
                        name: "key",
                        arg_type: Key,
                        key_spec_index: 0,
                    },
                    {
                        name: "path",
                        arg_type: String,
                    },
                    {
                        name: "json",
                        arg_type: String,
                    },
                    {
                        name: "format",
                        token: "FORMAT",
                        arg_type: Block,
                        flags: [Optional],
                        subargs: [
                            {
                                name: "format-token",
                                arg_type: OneOf,
                                subargs: [
                                    {
                                        name: "STRING",
                                        arg_type: PureToken,
                                        token: "STRING",
                                    },
                                    {
                                        name: "JSON",
                                        arg_type: PureToken,
                                        token: "JSON",
                                    },
                                    {
                                        name: "BSON",
                                        arg_type: PureToken,
                                        token: "BSON",
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        )]
        $item
    };
}

pub fn json_merge_command_impl<M: Manager>(
    manager: M,
    ctx: &Context,
    args: Vec<RedisString>,
) -> RedisResult {
    let mut args = args.into_iter().skip(1);

    let key = args.next_arg()?;
    let path = Path::new(args.next_str()?);
    let value = args.next_str()?;

    let mut format = Format::JSON;

    while let Some(s) = args.next() {
        match s.try_as_str()? {
            arg if arg.eq_ignore_ascii_case("FORMAT") => {
                format = Format::from_str(args.next_str()?)?;
            }
            _ => return Err(RedisError::Str("ERR syntax error")),
        };
    }

    let mut redis_key = manager.open_key_write(ctx, key)?;
    let current = redis_key.get_value()?;

    let val = manager.from_str(value, format, true)?;

    match current {
        Some(doc) => {
            if path == JSON_ROOT_PATH {
                redis_key.merge_value(Vec::new(), val)?;
                redis_key.notify_keyspace_event(ctx, "json.merge")?;
                manager.apply_changes(ctx);
                REDIS_OK
            } else {
                let mut update_info =
                    KeyValue::new(doc).find_paths(path.get_path(), SetOptions::MergeExisting)?;
                if !update_info.is_empty() {
                    let mut res = false;
                    if update_info.len() == 1 {
                        res = match update_info.pop().unwrap() {
                            UpdateInfo::SUI(sui) => redis_key.merge_value(sui.path, val)?,
                            UpdateInfo::AUI(aui) => redis_key.dict_add(aui.path, &aui.key, val)?,
                        }
                    } else {
                        for ui in update_info {
                            res = match ui {
                                UpdateInfo::SUI(sui) => {
                                    redis_key.merge_value(sui.path, val.clone())?
                                }
                                UpdateInfo::AUI(aui) => {
                                    redis_key.dict_add(aui.path, &aui.key, val.clone())?
                                }
                            } || res; // If any of the updates succeed, return true
                        }
                    }
                    if res {
                        redis_key.notify_keyspace_event(ctx, "json.merge")?;
                        manager.apply_changes(ctx);
                        REDIS_OK
                    } else {
                        Ok(RedisValue::Null)
                    }
                } else {
                    Ok(RedisValue::Null)
                }
            }
        }
        None => {
            if path == JSON_ROOT_PATH {
                // Nothing to merge with it's a new doc
                redis_key.set_value(Vec::new(), val)?;
                redis_key.notify_keyspace_event(ctx, "json.merge")?;
                manager.apply_changes(ctx);
                REDIS_OK
            } else {
                Err(RedisError::Str(
                    "ERR new objects must be created at the root",
                ))
            }
        }
    }
}

///
/// JSON.MSET <key> <path> <json> [[<key> <path> <json>]...]
///
#[macro_export]
macro_rules! json_mset_command {
    ($item:item) => {
        #[::redis_module_macros::command(
            {
                name: "json.mset",
                flags: [Write, DenyOOM],
                acl_categories: [Write, Single("json")],
                arity: -4,
                complexity: "O(K*(M+N)) where k is the number of keys in the command, when path is evaluated to a single value where M is the size of the original value (if it exists) and N is the size of the new value, or O(K*(M+N)) when path is evaluated to multiple values where M is the size of the key and N is the size of the new value * the number of original values in the key",
                since: "2.6.0",
                summary: "Set or update one or more JSON values according to the specified key-path-value triplets",
                key_spec: [
                    {
                        flags: [ReadWrite],
                        begin_search: Index({ index: 1 }),
                        find_keys: Range({ last_key: -1, steps: 3, limit: 0 }),
                    }
                ],
                args: [
                    {
                        name: "triplet",
                        arg_type: Block,
                        flags: [Multiple],
                        subargs: [
                            {
                                name: "key",
                                arg_type: Key,
                                key_spec_index: 0,
                            },
                            {
                                name: "path",
                                arg_type: String,
                            },
                            {
                                name: "json",
                                arg_type: String,
                            }
                        ]
                    }
                ]
            }
        )]
        $item
    };
}

pub fn json_mset_command_impl<M: Manager>(
    manager: M,
    ctx: &Context,
    args: Vec<RedisString>,
) -> RedisResult {
    let mut args = args.into_iter().skip(1);

    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }

    // Collect all the actions from the args (redis_key, update_info, value)
    let mut actions = Vec::new();
    while let Ok(key) = args.next_arg() {
        let mut redis_key = manager.open_key_write(ctx, key)?;

        // Verify the key is a JSON type
        let key_value = redis_key.get_value()?;

        // Verify the path is valid and get all the update info
        let path = Path::new(args.next_str()?);
        let update_info = if path == JSON_ROOT_PATH {
            None
        } else if let Some(value) = key_value {
            Some(KeyValue::new(value).find_paths(path.get_path(), SetOptions::None)?)
        } else {
            return Err(RedisError::Str(
                "ERR new objects must be created at the root",
            ));
        };

        // Parse the input and validate it's valid JSON
        let value_str = args.next_str()?;
        let value = manager.from_str(value_str, Format::JSON, true)?;

        actions.push((redis_key, update_info, value));
    }

    let res = actions
        .into_iter()
        .fold(REDIS_OK, |res, (mut redis_key, update_info, value)| {
            let updated = if let Some(update_info) = update_info {
                !update_info.is_empty() && apply_updates::<M>(&mut redis_key, value, update_info)
            } else {
                // In case it is a root path
                redis_key.set_value(Vec::new(), value)?
            };
            if updated {
                redis_key.notify_keyspace_event(ctx, "json.mset")?
            }
            res
        });

    manager.apply_changes(ctx);
    res
}

fn apply_updates<M: Manager>(
    redis_key: &mut M::WriteHolder,
    value: M::O,
    mut update_info: Vec<UpdateInfo>,
) -> bool {
    // If there is only one update info, we can avoid cloning the value
    if update_info.len() == 1 {
        match update_info.pop().unwrap() {
            UpdateInfo::SUI(sui) => redis_key.set_value(sui.path, value),
            UpdateInfo::AUI(aui) => redis_key.dict_add(aui.path, &aui.key, value),
        }
        .unwrap_or(false)
    } else {
        update_info.into_iter().fold(false, |updated, ui| {
            match ui {
                UpdateInfo::SUI(sui) => redis_key.set_value(sui.path, value.clone()),
                UpdateInfo::AUI(aui) => redis_key.dict_add(aui.path, &aui.key, value.clone()),
            }
            .unwrap_or(updated)
        })
    }
}

fn find_paths<T: SelectValue, F: Fn(&ValueRef<'_, T>) -> bool>(
    path: &str,
    doc: &T,
    f: F,
) -> Result<Vec<Vec<String>>, RedisError> {
    let query = match compile(path) {
        Ok(q) => q,
        Err(e) => return Err(RedisError::String(e.to_string())),
    };
    let res = calc_once_with_paths(query, doc);
    Ok(res
        .into_iter()
        .filter_map(|e| f(&e.res).then_some(e.path_tracker.unwrap().to_string_path()))
        .collect())
}

/// Returns tuples of Value and its concrete path which match the given `path`
fn get_all_values_and_paths<'a, T: SelectValue>(
    path: &str,
    doc: &'a T,
) -> Result<Vec<(ValueRef<'a, T>, Vec<String>)>, RedisError> {
    let query = match compile(path) {
        Ok(q) => q,
        Err(e) => return Err(RedisError::String(e.to_string())),
    };
    let res = calc_once_with_paths(query, doc);
    Ok(res
        .into_iter()
        .map(|e| (e.res, e.path_tracker.unwrap().to_string_path()))
        .collect())
}

/// Returns a Vec of paths with `None` for Values that do not match the filter
fn filter_paths<T: SelectValue, F>(
    values_and_paths: Vec<(ValueRef<'_, T>, Vec<String>)>,
    f: F,
) -> Vec<Option<Vec<String>>>
where
    F: Fn(ValueRef<'_, T>) -> bool,
{
    values_and_paths
        .into_iter()
        .map(|(v, p)| f(v).then_some(p))
        .collect()
}

/// Returns a Vec of Values with `None` for Values that do not match the filter
fn filter_values<T: SelectValue, F>(
    values_and_paths: Vec<(ValueRef<'_, T>, Vec<String>)>,
    f: F,
) -> Vec<Option<ValueRef<'_, T>>>
where
    F: Fn(ValueRef<'_, T>) -> bool,
{
    values_and_paths
        .into_iter()
        .map(|(v, _)| f(v.clone()).then_some(v))
        .collect()
}

fn find_all_paths<T: SelectValue, F>(
    path: &str,
    doc: &T,
    f: F,
) -> Result<Vec<Option<Vec<String>>>, RedisError>
where
    F: Fn(ValueRef<'_, T>) -> bool,
{
    let res = get_all_values_and_paths(path, doc)?;
    match res.is_empty() {
        false => Ok(filter_paths(res, f)),
        _ => Ok(vec![]),
    }
}

fn find_all_values<'a, T: SelectValue, F>(
    path: &str,
    doc: &'a T,
    f: F,
) -> Result<Vec<Option<ValueRef<'a, T>>>, RedisError>
where
    F: Fn(ValueRef<'_, T>) -> bool,
{
    let res = get_all_values_and_paths(path, doc)?;
    match res.is_empty() {
        false => Ok(filter_values(res, f)),
        _ => Ok(vec![]),
    }
}

fn to_json_value<T>(values: Vec<Option<T>>, none_value: Value) -> Vec<Value>
where
    Value: From<T>,
{
    values
        .into_iter()
        .map(|n| n.map_or_else(|| none_value.clone(), |t| t.into()))
        .collect()
}

/// Sort the paths so higher indices precede lower indices on the same array,
/// And longer paths precede shorter paths
/// And if a path is a sub-path of the other, then only paths with shallower hierarchy (closer to the top-level) remain
pub fn prepare_paths_for_updating(paths: &mut Vec<Vec<String>>) {
    if paths.len() < 2 {
        // No need to reorder when there are less than 2 paths
        return;
    }
    paths.sort_by(|v1, v2| {
        v1.iter()
            .zip_longest(v2.iter())
            .fold_while(Ordering::Equal, |_acc, v| {
                match v {
                    EitherOrBoth::Left(_) => Done(Ordering::Less), // Shorter paths after longer paths
                    EitherOrBoth::Right(_) => Done(Ordering::Greater), // Shorter paths after longer paths
                    EitherOrBoth::Both(p1, p2) => {
                        let i1 = p1.parse::<usize>();
                        let i2 = p2.parse::<usize>();
                        match (i1, i2) {
                            (Err(_), Err(_)) => match p1.cmp(p2) {
                                // String compare
                                Ordering::Less => Done(Ordering::Less),
                                Ordering::Equal => Continue(Ordering::Equal),
                                Ordering::Greater => Done(Ordering::Greater),
                            },
                            (Ok(_), Err(_)) => Done(Ordering::Greater), //String before Numeric
                            (Err(_), Ok(_)) => Done(Ordering::Less),    //String before Numeric
                            (Ok(i1), Ok(i2)) => {
                                // Numeric compare - higher indices before lower ones
                                match i2.cmp(&i1) {
                                    Ordering::Greater => Done(Ordering::Greater),
                                    Ordering::Less => Done(Ordering::Less),
                                    Ordering::Equal => Continue(Ordering::Equal),
                                }
                            }
                        }
                    }
                }
            })
            .into_inner()
    });
    // Remove paths which are nested by others (on each sub-tree only top most ancestor should be deleted)
    // (TODO: Add a mode in which the jsonpath selector will already skip nested paths)
    let mut string_paths = paths.iter().map(|v| v.join(",")).collect_vec();
    string_paths.sort_by(|a, b| {
        let i_a = a.parse::<usize>();
        let i_b = b.parse::<usize>();
        match (i_a, i_b) {
            (Ok(i1), Ok(i2)) => i1.cmp(&i2),
            _ => a.cmp(b),
        }
    });

    paths.retain(|v| {
        let path = v.join(",");
        string_paths
            .iter()
            .skip_while(|p| {
                // Check if path is a proper nested path of p
                // A path is nested if it starts with p followed by a comma, or if it equals p
                !path.starts_with(*p) || (path.len() > p.len() && !path[p.len()..].starts_with(","))
            })
            .next()
            .map(|found| path == *found)
            .unwrap_or(false)
    });
}

///
/// JSON.DEL <key> [path]
///
///
#[macro_export]
macro_rules! json_del_command {
    ($name:literal, $item:item) => {
        #[::redis_module_macros::command(
            {
                name: $name,
                flags: [Write],
                acl_categories: [Write, Single("json")],
                arity: -2,
                complexity: "O(N) when path is evaluated to a single value where N is the size of the deleted value, O(N) when path is evaluated to multiple values, where N is the size of the key",
                since: "1.0.0",
                summary: "Delete a value",
                key_spec: [
                    {
                        flags: [ReadWrite],
                        begin_search: Index({ index: 1 }),
                        find_keys: Range({ last_key: 0, steps: 1, limit: 0 }),
                    }
                ],
                args: [
                    { name: "key",  arg_type: Key,    key_spec_index: 0 },
                    { name: "path", arg_type: String, flags: [Optional] }
                ]
            }
        )]
        $item
    };
}

pub fn json_del_command_impl<M: Manager>(
    manager: M,
    ctx: &Context,
    args: Vec<RedisString>,
) -> RedisResult {
    let mut args = args.into_iter().skip(1);

    let key = args.next_arg()?;
    let path = match args.next() {
        None => Path::default(),
        Some(s) => Path::new(s.try_as_str()?),
    };

    let mut redis_key = manager.open_key_write(ctx, key)?;
    let deleted = if let Some(doc) = redis_key.get_value()? {
        if path != JSON_ROOT_PATH {
            let mut paths = find_paths(path.get_path(), doc, |_| true)?;
            prepare_paths_for_updating(&mut paths);
            paths
                .into_iter()
                .try_fold(0, |acc, p| redis_key.delete_path(p).map(|v| acc + v as i64))?
        } else {
            1
        }
    } else {
        0
    };

    if deleted > 0 {
        let is_empty = redis_key
            .get_value()?
            .and_then(|v| v.is_empty())
            .unwrap_or(false);
        if is_empty || path == JSON_ROOT_PATH {
            redis_key.delete()?;
        }
        redis_key.notify_keyspace_event(ctx, "json.del")?;
        manager.apply_changes(ctx);
    }

    Ok(deleted.into())
}

///
/// JSON.MGET <key> [key ...] path
///
#[macro_export]
macro_rules! json_mget_command {
    ($item:item) => {
        #[::redis_module_macros::command(
            {
                name: "json.mget",
                flags: [ReadOnly],
                acl_categories: [Read, Single("json")],
                arity: -3,
                complexity: "O(M*N) when path is evaluated to a single value where M is the number of keys and N is the size of the value, O(N1+N2+...+Nm) when path is evaluated to multiple values where m is the number of keys and Ni is the size of the i-th key",
                since: "1.0.0",
                summary: "Return the values at path from multiple key arguments",
                key_spec: [
                    {
                        notes: "The key containing the JSON document",
                        flags: [ReadOnly],
                        begin_search: Index({ index: 1 }),
                        find_keys: Range({ last_key: 0, steps: 1, limit: 0 }),
                    }
                ],
                args: [
                    {
                        name: "key",
                        arg_type: Key,
                        key_spec_index: 0,
                        flags: [Multiple],
                    },
                    {
                        name: "path",
                        arg_type: String,
                    }
                ]
            }
        )]
        $item
    };
}

pub fn json_mget_command_impl<M: Manager>(
    manager: M,
    ctx: &Context,
    args: Vec<RedisString>,
) -> RedisResult {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }

    args.last().ok_or(RedisError::WrongArity).and_then(|path| {
        let path = Path::new(path.try_as_str()?);
        let keys = &args[1..args.len() - 1];

        let format_options = ReplyFormatOptions::new(is_resp3(ctx), ReplyFormat::STRING);

        // Verify that at least one key exists
        if keys.is_empty() {
            return Err(RedisError::WrongArity);
        }

        let results: Result<Vec<RedisValue>, RedisError> = keys
            .iter()
            .map(|key| {
                manager
                    .open_key_read(ctx, key)
                    .map_or(Ok(RedisValue::Null), |json_key| {
                        json_key.get_value().map_or(Ok(RedisValue::Null), |value| {
                            value.map_or(Ok(RedisValue::Null), |doc| {
                                let key_value = KeyValue::new(doc);
                                let res = if !path.is_legacy() {
                                    key_value.to_string_multi(path.get_path(), &format_options)
                                } else {
                                    key_value.to_string_single(path.get_path(), &format_options)
                                };
                                Ok(res.map_or(RedisValue::Null, |v| v.into()))
                            })
                        })
                    })
            })
            .collect();

        Ok(results?.into())
    })
}
///
/// JSON.TYPE <key> [path]
///
#[macro_export]
macro_rules! json_type_command {
    ($item:item) => {
        #[::redis_module_macros::command(
            {
                name: "json.type",
                flags: [ReadOnly],
                acl_categories: [Read, Single("json")],
                arity: -2,
                complexity: "O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values, where N is the size of the key",
                since: "1.0.0",
                summary: "Report the type of JSON value at path",
                key_spec: [
                    {
                        flags: [ReadOnly],
                        begin_search: Index({ index: 1 }),
                        find_keys: Range({ last_key: 0, steps: 1, limit: 0 }),
                    }
                ],
                args: [
                    {
                        name: "key",
                        arg_type: Key,
                        key_spec_index: 0,
                    },
                    {
                        name: "path",
                        arg_type: String,
                        flags: [Optional],
                    }
                ]
            }
        )]
        $item
    };
}

pub fn json_type_command_impl<M: Manager>(
    manager: M,
    ctx: &Context,
    args: Vec<RedisString>,
) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;
    let path = args.next_str().map(Path::new).unwrap_or_default();

    let key = manager.open_key_read(ctx, &key)?;

    let value = if path.is_legacy() {
        json_type_legacy::<M>(&key, path.get_path())?
    } else {
        json_type_impl::<M>(&key, path.get_path())?
    };

    // Check context flags to see if RESP3 is enabled and return the appropriate result
    if is_resp3(ctx) {
        Ok(vec![value].into())
    } else {
        Ok(value)
    }
}

pub fn json_type_impl<M>(redis_key: &M::ReadHolder, path: &str) -> RedisResult
where
    M: Manager,
{
    let root = redis_key.get_value()?;
    let value = match root {
        Some(root) => KeyValue::new(root)
            .get_values(path)?
            .iter()
            .map(|v| RedisValue::from(KeyValue::value_name(v.as_ref())))
            .collect_vec()
            .into(),
        None => RedisValue::Null,
    };
    Ok(value)
}

fn json_type_legacy<M>(redis_key: &M::ReadHolder, path: &str) -> RedisResult
where
    M: Manager,
{
    let value = redis_key.get_value()?.map_or(RedisValue::Null, |doc| {
        KeyValue::new(doc)
            .get_type(path)
            .map_or(RedisValue::Null, |s| s.into())
    });
    Ok(value)
}

enum NumOp {
    Incr,
    Mult,
    Pow,
}

fn json_num_op<M>(
    manager: M,
    ctx: &Context,
    args: Vec<RedisString>,
    cmd: &str,
    op: NumOp,
) -> RedisResult
where
    M: Manager,
{
    let mut args = args.into_iter().skip(1);

    let key = args.next_arg()?;
    let path = Path::new(args.next_str()?);
    let number = args.next_str()?;

    let mut redis_key = manager.open_key_write(ctx, key)?;

    // check context flags to see if RESP3 is enabled
    if is_resp3(ctx) {
        let res = json_num_op_impl::<M>(
            manager,
            &mut redis_key,
            ctx,
            path.get_path(),
            number,
            op,
            cmd,
        )?
        .into_iter()
        .map(|v| {
            v.map_or(RedisValue::Null, |v| {
                if let Some(i) = v.as_i64() {
                    RedisValue::Integer(i)
                } else {
                    RedisValue::Float(v.as_f64().unwrap_or_default())
                }
            })
        })
        .collect_vec()
        .into();
        Ok(res)
    } else if path.is_legacy() {
        json_num_op_legacy::<M>(
            manager,
            &mut redis_key,
            ctx,
            path.get_path(),
            number,
            op,
            cmd,
        )
    } else {
        let results = json_num_op_impl::<M>(
            manager,
            &mut redis_key,
            ctx,
            path.get_path(),
            number,
            op,
            cmd,
        )?;

        // Convert to RESP2 format return as one JSON array
        let values = to_json_value::<Number>(results, Value::Null);
        Ok(KeyValue::<M::V>::serialize_object(&values, &ReplyFormatOptions::default()).into())
    }
}

fn json_num_op_impl<M>(
    manager: M,
    redis_key: &mut M::WriteHolder,
    ctx: &Context,
    path: &str,
    number: &str,
    op: NumOp,
    cmd: &str,
) -> Result<Vec<Option<Number>>, RedisError>
where
    M: Manager,
{
    let root = redis_key
        .get_value()?
        .ok_or_else(RedisError::nonexistent_key)?;
    let paths = find_all_paths(path, root, |v| {
        matches!(
            v.get_type(),
            SelectValueType::Double | SelectValueType::Long
        )
    })?;

    let mut need_notify = false;
    let res = paths
        .into_iter()
        .map(|p| {
            p.map(|p| {
                need_notify = true;
                match op {
                    NumOp::Incr => redis_key.incr_by(p, number),
                    NumOp::Mult => redis_key.mult_by(p, number),
                    NumOp::Pow => redis_key.pow_by(p, number),
                }
            })
            .transpose()
        })
        .try_collect()?;
    if need_notify {
        redis_key.notify_keyspace_event(ctx, cmd)?;
        manager.apply_changes(ctx);
    }
    Ok(res)
}

fn json_num_op_legacy<M>(
    manager: M,
    redis_key: &mut M::WriteHolder,
    ctx: &Context,
    path: &str,
    number: &str,
    op: NumOp,
    cmd: &str,
) -> RedisResult
where
    M: Manager,
{
    let root = redis_key
        .get_value()?
        .ok_or_else(RedisError::nonexistent_key)?;
    let paths = find_paths(path, root, |v| {
        v.get_type() == SelectValueType::Double || v.get_type() == SelectValueType::Long
    })?;
    if !paths.is_empty() {
        let mut res = None;
        for p in paths {
            res = Some(match op {
                NumOp::Incr => redis_key.incr_by(p, number)?,
                NumOp::Mult => redis_key.mult_by(p, number)?,
                NumOp::Pow => redis_key.pow_by(p, number)?,
            });
        }
        redis_key.notify_keyspace_event(ctx, cmd)?;
        manager.apply_changes(ctx);
        Ok(res.unwrap().to_string().into())
    } else {
        Err(RedisError::String(
            err_msg_json_path_doesnt_exist_with_param_or(path, "does not contains a number"),
        ))
    }
}

///
/// JSON.NUMINCRBY <key> <path> <number>
///
#[macro_export]
macro_rules! json_numincrby_command {
    ($item:item) => {
        #[::redis_module_macros::command(
            {
                name: "json.numincrby",
                flags: [Write],
                acl_categories: [Write, Single("json")],
                arity: 4,
                complexity: "O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values, where N is the size of the key",
                since: "1.0.0",
                summary: "Increment the number value stored at path by number",
                key_spec: [
                    {
                        flags: [ReadWrite],
                        begin_search: Index({ index: 1 }),
                        find_keys: Range({ last_key: 0, steps: 1, limit: 0 }),
                    }
                ],
                args: [
                    {
                        name: "key",
                        arg_type: Key,
                        key_spec_index: 0,
                    },
                    {
                        name: "path",
                        arg_type: String,
                    },
                    {
                        name: "number",
                        arg_type: Double,
                    },
                ]
            }
        )]
        $item
    };
}

pub fn json_num_incrby_command_impl<M: Manager>(
    manager: M,
    ctx: &Context,
    args: Vec<RedisString>,
) -> RedisResult {
    json_num_op(manager, ctx, args, "json.numincrby", NumOp::Incr)
}

///
/// JSON.NUMMULTBY <key> <path> <number>
///
#[macro_export]
macro_rules! json_nummultby_command {
    ($item:item) => {
        #[::redis_module_macros::command(
            {
                name: "json.nummultby",
                flags: [Write],
                acl_categories: [Write, Single("json")],
                arity: 4,
                complexity: "O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values, where N is the size of the key",
                since: "1.0.0",
                summary: "Multiply the number value stored at path by number",
                key_spec: [
                    {
                        flags: [ReadWrite],
                        begin_search: Index({ index: 1 }),
                        find_keys: Range({ last_key: 0, steps: 1, limit: 0 }),
                    }
                ],
                args: [
                    {
                        name: "key",
                        arg_type: Key,
                        key_spec_index: 0,
                    },
                    {
                        name: "path",
                        arg_type: String,
                    },
                    {
                        name: "number",
                        arg_type: Double,
                    },
                ]
            }
        )]
        $item
    };
}

pub fn json_num_multby_command_impl<M: Manager>(
    manager: M,
    ctx: &Context,
    args: Vec<RedisString>,
) -> RedisResult {
    json_num_op(manager, ctx, args, "json.nummultby", NumOp::Mult)
}

///
/// JSON.NUMPOWBY <key> <path> <number>
///
#[macro_export]
macro_rules! json_numpowby_command {
    ($item:item) => {
        #[::redis_module_macros::command(
            {
                name: "json.numpowby",
                flags: [Write],
                acl_categories: [Write, Single("json")],
                arity: 4,
                complexity: "O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values, where N is the size of the key",
                since: "1.0.0",
                summary: "Raise the number value stored at path to the power of number",
                key_spec: [
                    {
                        flags: [ReadWrite],
                        begin_search: Index({ index: 1 }),
                        find_keys: Range({ last_key: 0, steps: 1, limit: 0 }),
                    }
                ],
                args: [
                    {
                        name: "key",
                        arg_type: Key,
                        key_spec_index: 0,
                    },
                    {
                        name: "path",
                        arg_type: String,
                    },
                    {
                        name: "number",
                        arg_type: Double,
                    },
                ]
            }
        )]
        $item
    };
}

pub fn json_num_powby_command_impl<M: Manager>(
    manager: M,
    ctx: &Context,
    args: Vec<RedisString>,
) -> RedisResult {
    json_num_op(manager, ctx, args, "json.numpowby", NumOp::Pow)
}

//
/// JSON.TOGGLE <key> <path>
///
#[macro_export]
macro_rules! json_toggle_command {
    ($item:item) => {
        #[::redis_module_macros::command(
            {
                name: "json.toggle",
                flags: [Write],
                acl_categories: [Write, Single("json")],
                arity: 3,
                complexity: "O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values, where N is the size of the key",
                since: "2.0.0",
                summary: "Toggle the boolean value stored at path",
                key_spec: [
                    {
                        flags: [ReadWrite],
                        begin_search: Index({ index: 1 }),
                        find_keys: Range({ last_key: 0, steps: 1, limit: 0 }),
                    }
                ],
                args: [
                    {
                        name: "key",
                        arg_type: Key,
                        key_spec_index: 0,
                    },
                    {
                        name: "path",
                        arg_type: String,
                    },
                ]
            }
        )]
        $item
    };
}

pub fn json_bool_toggle_command_impl<M: Manager>(
    manager: M,
    ctx: &Context,
    args: Vec<RedisString>,
) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;
    let path = Path::new(args.next_str()?);
    let mut redis_key = manager.open_key_write(ctx, key)?;

    if path.is_legacy() {
        json_bool_toggle_legacy::<M>(manager, &mut redis_key, ctx, path.get_path())
    } else {
        json_bool_toggle_impl::<M>(manager, &mut redis_key, ctx, path.get_path())
    }
}

fn json_bool_toggle_impl<M>(
    manager: M,
    redis_key: &mut M::WriteHolder,
    ctx: &Context,
    path: &str,
) -> RedisResult
where
    M: Manager,
{
    let root = redis_key
        .get_value()?
        .ok_or_else(RedisError::nonexistent_key)?;
    let paths = find_all_paths(path, root, |v| v.get_type() == SelectValueType::Bool)?;
    let mut res: Vec<RedisValue> = vec![];
    let mut need_notify = false;
    for p in paths {
        res.push(match p {
            Some(p) => {
                need_notify = true;
                RedisValue::Integer((redis_key.bool_toggle(p)?).into())
            }
            None => RedisValue::Null,
        });
    }
    if need_notify {
        redis_key.notify_keyspace_event(ctx, "json.toggle")?;
        manager.apply_changes(ctx);
    }
    Ok(res.into())
}

fn json_bool_toggle_legacy<M>(
    manager: M,
    redis_key: &mut M::WriteHolder,
    ctx: &Context,
    path: &str,
) -> RedisResult
where
    M: Manager,
{
    let root = redis_key
        .get_value()?
        .ok_or_else(RedisError::nonexistent_key)?;
    let paths = find_paths(path, root, |v| v.get_type() == SelectValueType::Bool)?;
    if !paths.is_empty() {
        let mut res = false;
        for p in paths {
            res = redis_key.bool_toggle(p)?;
        }
        redis_key.notify_keyspace_event(ctx, "json.toggle")?;
        manager.apply_changes(ctx);
        Ok(res.to_string().into())
    } else {
        Err(RedisError::String(
            err_msg_json_path_doesnt_exist_with_param_or(path, "not a bool"),
        ))
    }
}

///
/// JSON.STRAPPEND <key> [path] <json-string>
///
#[macro_export]
macro_rules! json_strappend_command {
    ($item:item) => {
        #[::redis_module_macros::command(
            {
                name: "json.strappend",
                flags: [Write, DenyOOM],
                acl_categories: [Write, Single("json")],
                arity: -3,
                complexity: "O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values, where N is the size of the key",
                since: "1.0.0",
                summary: "Append the json-string values to the string at path",
                key_spec: [
                    {
                        flags: [ReadWrite],
                        begin_search: Index({ index: 1 }),
                        find_keys: Range({ last_key: 0, steps: 1, limit: 0 }),
                    }
                ],
                args: [
                    {
                        name: "key",
                        arg_type: Key,
                        key_spec_index: 0,
                    },
                    {
                        name: "path",
                        arg_type: String,
                        flags: [Optional],
                    },
                    {
                        name: "json-string",
                        arg_type: String,
                    }
                ]
            }
        )]
        $item
    };
}

pub fn json_str_append_command_impl<M: Manager>(
    manager: M,
    ctx: &Context,
    args: Vec<RedisString>,
) -> RedisResult {
    let mut args = args.into_iter().skip(1);

    let key = args.next_arg()?;
    let path_or_json = args.next_str()?;

    let path;
    let json;

    // path is optional
    if let Ok(val) = args.next_arg() {
        path = Path::new(path_or_json);
        json = val.try_as_str()?;
    } else {
        path = Path::default();
        json = path_or_json;
    }

    let mut redis_key = manager.open_key_write(ctx, key)?;

    if path.is_legacy() {
        json_str_append_legacy::<M>(manager, &mut redis_key, ctx, path.get_path(), json)
    } else {
        json_str_append_impl::<M>(manager, &mut redis_key, ctx, path.get_path(), json)
    }
}

fn json_str_append_impl<M>(
    manager: M,
    redis_key: &mut M::WriteHolder,
    ctx: &Context,
    path: &str,
    json: &str,
) -> RedisResult
where
    M: Manager,
{
    let root = redis_key
        .get_value()?
        .ok_or_else(RedisError::nonexistent_key)?;

    let paths = find_all_paths(path, root, |v| v.get_type() == SelectValueType::String)?;

    let mut res: Vec<RedisValue> = vec![];
    let mut need_notify = false;
    for p in paths {
        res.push(match p {
            Some(p) => {
                need_notify = true;
                (redis_key.str_append(p, json.to_string())?).into()
            }
            _ => RedisValue::Null,
        });
    }
    if need_notify {
        redis_key.notify_keyspace_event(ctx, "json.strappend")?;
        manager.apply_changes(ctx);
    }
    Ok(res.into())
}

fn json_str_append_legacy<M>(
    manager: M,
    redis_key: &mut M::WriteHolder,
    ctx: &Context,
    path: &str,
    json: &str,
) -> RedisResult
where
    M: Manager,
{
    let root = redis_key
        .get_value()?
        .ok_or_else(RedisError::nonexistent_key)?;

    let paths = find_paths(path, root, |v| v.get_type() == SelectValueType::String)?;
    if !paths.is_empty() {
        let mut res = None;
        for p in paths {
            res = Some(redis_key.str_append(p, json.to_string())?);
        }
        redis_key.notify_keyspace_event(ctx, "json.strappend")?;
        manager.apply_changes(ctx);
        Ok(res.unwrap().into())
    } else {
        Err(RedisError::String(
            err_msg_json_path_doesnt_exist_with_param_or(path, "not a string"),
        ))
    }
}

///
/// JSON.STRLEN <key> [path]
///
#[macro_export]
macro_rules! json_strlen_command {
    ($item:item) => {
        #[::redis_module_macros::command(
            {
                name: "json.strlen",
                flags: [ReadOnly],
                acl_categories: [Read, Single("json")],
                arity: -2,
                complexity: "O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values, where N is the size of the key",
                since: "1.0.0",
                summary: "Report the length of the JSON String at path in key",
                key_spec: [
                    {
                        flags: [ReadOnly],
                        begin_search: Index({ index: 1 }),
                        find_keys: Range({ last_key: 0, steps: 1, limit: 0 }),
                    }
                ],
                args: [
                    {
                        name: "key",
                        arg_type: Key,
                        key_spec_index: 0,
                    },
                    {
                        name: "path",
                        arg_type: String,
                        flags: [Optional],
                    }
                ]
            }
        )]
        $item
    };
}

pub fn json_str_len_command_impl<M: Manager>(
    manager: M,
    ctx: &Context,
    args: Vec<RedisString>,
) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;
    let path = args.next_str().map(Path::new).unwrap_or_default();

    let key = manager.open_key_read(ctx, &key)?;

    if path.is_legacy() {
        json_str_len_legacy::<M>(&key, path.get_path())
    } else {
        json_str_len_impl::<M>(&key, path.get_path())
    }
}

fn json_str_len_impl<M>(redis_key: &M::ReadHolder, path: &str) -> RedisResult
where
    M: Manager,
{
    let root = redis_key
        .get_value()?
        .ok_or_else(RedisError::nonexistent_key)?;
    let values = find_all_values(path, root, |v| v.get_type() == SelectValueType::String)?;
    let mut res: Vec<RedisValue> = vec![];
    for v in values {
        res.push(v.map_or(RedisValue::Null, |v| (v.get_str().len() as i64).into()));
    }
    Ok(res.into())
}

fn json_str_len_legacy<M>(redis_key: &M::ReadHolder, path: &str) -> RedisResult
where
    M: Manager,
{
    match redis_key.get_value()? {
        Some(doc) => Ok(RedisValue::Integer(KeyValue::new(doc).str_len(path)? as i64)),
        None => Ok(RedisValue::Null),
    }
}

///
/// JSON.ARRAPPEND <key> <path> <json> [json ...]
///
#[macro_export]
macro_rules! json_arrappend_command {
    ($item:item) => {
        #[::redis_module_macros::command(
            {
                name: "json.arrappend",
                flags: [Write, DenyOOM],
                acl_categories: [Write, Single("json")],
                arity: -3,
                complexity: "O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values, where N is the size of the key",
                since: "1.0.0",
                summary: "Append the JSON values into the array at path after the last element in it",
                key_spec: [
                    {
                        flags: [ReadWrite],
                        begin_search: Index({ index: 1 }),
                        find_keys: Range({ last_key: 0, steps: 1, limit: 0 }),
                    }
                ],
                args: [
                    {
                        name: "key",
                        arg_type: Key,
                        key_spec_index: 0,
                    },
                    {
                        name: "path",
                        arg_type: String,
                    },
                    {
                        name: "json",
                        arg_type: String,
                        flags: [Multiple],
                    }
                ]
            }
        )]
        $item
    };
}

pub fn json_arr_append_command_impl<M: Manager>(
    manager: M,
    ctx: &Context,
    args: Vec<RedisString>,
) -> RedisResult {
    let mut args = args.into_iter().skip(1).peekable();

    let key = args.next_arg()?;
    let path = Path::new(args.next_str()?);

    // We require at least one JSON item to append
    args.peek().ok_or(RedisError::WrongArity)?;

    let args = args.try_fold::<_, _, Result<_, RedisError>>(
        Vec::with_capacity(args.len()),
        |mut acc, arg| {
            let json = arg.try_as_str()?;
            acc.push(manager.from_str(json, Format::JSON, true)?);
            Ok(acc)
        },
    )?;

    let mut redis_key = manager.open_key_write(ctx, key)?;

    if path.is_legacy() {
        json_arr_append_legacy::<M>(manager, &mut redis_key, ctx, &path, args)
    } else {
        json_arr_append_impl::<M>(manager, &mut redis_key, ctx, path.get_path(), args)
    }
}

fn json_arr_append_legacy<M>(
    manager: M,
    redis_key: &mut M::WriteHolder,
    ctx: &Context,
    path: &Path,
    args: Vec<M::O>,
) -> RedisResult
where
    M: Manager,
{
    let root = redis_key
        .get_value()?
        .ok_or_else(RedisError::nonexistent_key)?;
    let mut paths = find_paths(path.get_path(), root, |v| {
        v.get_type() == SelectValueType::Array
    })?;
    if paths.is_empty() {
        Err(RedisError::String(
            err_msg_json_path_doesnt_exist_with_param_or(path.get_original(), "not an array"),
        ))
    } else if paths.len() == 1 {
        let res = redis_key.arr_append(paths.pop().unwrap(), args)?;
        redis_key.notify_keyspace_event(ctx, "json.arrappend")?;
        manager.apply_changes(ctx);
        Ok(res.into())
    } else {
        let mut res = 0;
        for p in paths {
            res = redis_key.arr_append(p, args.clone())?;
        }
        redis_key.notify_keyspace_event(ctx, "json.arrappend")?;
        manager.apply_changes(ctx);
        Ok(res.into())
    }
}

fn json_arr_append_impl<M>(
    manager: M,
    redis_key: &mut M::WriteHolder,
    ctx: &Context,
    path: &str,
    args: Vec<M::O>,
) -> RedisResult
where
    M: Manager,
{
    let root = redis_key
        .get_value()?
        .ok_or_else(RedisError::nonexistent_key)?;
    let paths = find_all_paths(path, root, |v| v.get_type() == SelectValueType::Array)?;

    let mut res = vec![];
    let mut need_notify = false;
    for p in paths {
        res.push(match p {
            Some(p) => {
                need_notify = true;
                (redis_key.arr_append(p, args.clone())? as i64).into()
            }
            _ => RedisValue::Null,
        });
    }
    if need_notify {
        redis_key.notify_keyspace_event(ctx, "json.arrappend")?;
        manager.apply_changes(ctx);
    }
    Ok(res.into())
}

pub enum FoundIndex {
    Index(i64),
    NotFound,
    NotArray,
}

impl From<FoundIndex> for RedisValue {
    fn from(e: FoundIndex) -> Self {
        match e {
            FoundIndex::NotFound => Self::Integer(-1),
            FoundIndex::NotArray => Self::Null,
            FoundIndex::Index(i) => Self::Integer(i),
        }
    }
}

pub enum ObjectLen {
    Len(usize),
    NoneExisting,
    NotObject,
}

///
/// JSON.ARRINDEX <key> <path> <json-value> [start [stop]]
///
#[macro_export]
macro_rules! json_arrindex_command {
    ($item:item) => {
        #[::redis_module_macros::command(
            {
                name: "json.arrindex",
                flags: [ReadOnly],
                acl_categories: [Read, Single("json")],
                arity: -4,
                complexity: "O(N) when path is evaluated to a single value where N is the size of the array, O(N) when path is evaluated to multiple values, where N is the size of the key",
                since: "1.0.0",
                summary: "Search for the first occurrence of a JSON value in an array",
                key_spec: [
                    {
                        flags: [ReadOnly],
                        begin_search: Index({ index: 1 }),
                        find_keys: Range({ last_key: 0, steps: 1, limit: 0 }),
                    }
                ],
                args: [
                    {
                        name: "key",
                        arg_type: Key,
                        key_spec_index: 0,
                    },
                    {
                        name: "path",
                        arg_type: String,
                    },
                    {
                        name: "json-value",
                        arg_type: String,
                    },
                    {
                        name: "range",
                        arg_type: Block,
                        flags: [Optional],
                        subargs: [
                            {
                                name: "start",
                                arg_type: Integer,
                            },
                            {
                                name: "stop",
                                arg_type: Integer,
                                flags: [Optional],
                            }
                        ]
                    }
                ]
            }
        )]
        $item
    };
}

pub fn json_arr_index_command_impl<M: Manager>(
    manager: M,
    ctx: &Context,
    args: Vec<RedisString>,
) -> RedisResult {
    let mut args = args.into_iter().skip(1);

    let key = args.next_arg()?;
    let path = Path::new(args.next_str()?);
    let value = args.next_str()?;
    let start: i64 = args.next().map_or(Ok(0), |v| v.parse_integer())?;
    let end: i64 = args.next().map_or(Ok(0), |v| v.parse_integer())?;

    args.done()?; // TODO: Add to other functions as well to terminate args list

    let key = manager.open_key_read(ctx, &key)?;

    let json_value: Value = serde_json::from_str(value)?;

    let res = key.get_value()?.map_or_else(
        || {
            Err(Error::from(err_msg_json_path_doesnt_exist_with_param(
                path.get_original(),
            )))
        },
        |doc| {
            if path.is_legacy() {
                KeyValue::new(doc).arr_index_legacy(path.get_path(), json_value, start, end)
            } else {
                KeyValue::new(doc).arr_index(path.get_path(), json_value, start, end)
            }
        },
    )?;

    Ok(res)
}

///
/// JSON.ARRINSERT <key> <path> <index> <json> [json ...]
///

#[macro_export]
macro_rules! json_arrinsert_command {
    ($item:item) => {
        #[::redis_module_macros::command(
            {
                name: "json.arrinsert",
                flags: [Write, DenyOOM],
                acl_categories: [Write, Single("json")],
                arity: -5,
                complexity: "O(N) when path is evaluated to a single value where N is the size of the array, O(N) when path is evaluated to multiple values, where N is the size of the key",
                since: "1.0.0",
                summary: "Insert the json values into the array at path before the index (shifts to the right)",
                key_spec: [
                    {
                        flags: [ReadWrite],
                        begin_search: Index({ index: 1 }),
                        find_keys: Range({ last_key: 0, steps: 1, limit: 0 }),
                    }
                ],
                args: [
                    {
                        name: "key",
                        arg_type: Key,
                        key_spec_index: 0,
                    },
                    {
                        name: "path",
                        arg_type: String,
                    },
                    {
                        name: "index",
                        arg_type: Integer,
                    },
                    {
                        name: "json",
                        arg_type: String,
                        flags: [Multiple],
                    }
                ]
            }
        )]
        $item
    };
}

pub fn json_arr_insert_command_impl<M: Manager>(
    manager: M,
    ctx: &Context,
    args: Vec<RedisString>,
) -> RedisResult {
    let mut args = args.into_iter().skip(1).peekable();

    let key = args.next_arg()?;
    let path = Path::new(args.next_str()?);
    let index = args.next_i64()?;

    // We require at least one JSON item to insert
    args.peek().ok_or(RedisError::WrongArity)?;
    let args = args.try_fold::<_, _, Result<_, RedisError>>(
        Vec::with_capacity(args.len()),
        |mut acc, arg| {
            let json = arg.try_as_str()?;
            acc.push(manager.from_str(json, Format::JSON, true)?);
            Ok(acc)
        },
    )?;
    let mut redis_key = manager.open_key_write(ctx, key)?;
    if path.is_legacy() {
        json_arr_insert_legacy::<M>(manager, &mut redis_key, ctx, path.get_path(), index, args)
    } else {
        json_arr_insert_impl::<M>(manager, &mut redis_key, ctx, path.get_path(), index, args)
    }
}

fn json_arr_insert_impl<M>(
    manager: M,
    redis_key: &mut M::WriteHolder,
    ctx: &Context,
    path: &str,
    index: i64,
    args: Vec<M::O>,
) -> RedisResult
where
    M: Manager,
{
    let root = redis_key
        .get_value()?
        .ok_or_else(RedisError::nonexistent_key)?;

    let paths = find_all_paths(path, root, |v| v.get_type() == SelectValueType::Array)?;

    let mut res: Vec<RedisValue> = vec![];
    let mut need_notify = false;
    for p in paths {
        res.push(match p {
            Some(p) => {
                need_notify = true;
                (redis_key.arr_insert(p, &args, index)? as i64).into()
            }
            _ => RedisValue::Null,
        });
    }

    if need_notify {
        redis_key.notify_keyspace_event(ctx, "json.arrinsert")?;
        manager.apply_changes(ctx);
    }
    Ok(res.into())
}

fn json_arr_insert_legacy<M>(
    manager: M,
    redis_key: &mut M::WriteHolder,
    ctx: &Context,
    path: &str,
    index: i64,
    args: Vec<M::O>,
) -> RedisResult
where
    M: Manager,
{
    let root = redis_key
        .get_value()?
        .ok_or_else(RedisError::nonexistent_key)?;

    let paths = find_paths(path, root, |v| v.get_type() == SelectValueType::Array)?;
    if paths.is_empty() {
        return Err(RedisError::String(
            err_msg_json_path_doesnt_exist_with_param_or(path, "not an array"),
        ));
    }
    let res = paths
        .into_iter()
        .try_fold(0, |_, p| redis_key.arr_insert(p, &args, index))?;
    redis_key.notify_keyspace_event(ctx, "json.arrinsert")?;
    manager.apply_changes(ctx);
    Ok(res.into())
}

///
/// JSON.ARRLEN <key> [path]
///
#[macro_export]
macro_rules! json_arrlen_command {
    ($item:item) => {
        #[::redis_module_macros::command(
            {
                name: "json.arrlen",
                flags: [ReadOnly],
                acl_categories: [Read, Single("json")],
                arity: -2,
                complexity: "O(1) where path is evaluated to a single value, O(N) where path is evaluated to multiple values, where N is the size of the key",
                since: "1.0.0",
                summary: "Report the length of the JSON array at path in key",
                key_spec: [
                    {
                        flags: [ReadOnly],
                        begin_search: Index({ index: 1 }),
                        find_keys: Range({ last_key: 0, steps: 1, limit: 0 }),
                    }
                ],
                args: [
                    {
                        name: "key",
                        arg_type: Key,
                        key_spec_index: 0,
                    },
                    {
                        name: "path",
                        arg_type: String,
                        flags: [Optional],
                    }
                ]
            }
        )]
        $item
    };
}

pub fn json_arr_len_command_impl<M: Manager>(
    manager: M,
    ctx: &Context,
    args: Vec<RedisString>,
) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;
    let path = args.next_str().map(Path::new).unwrap_or_default();
    let is_legacy = path.is_legacy();
    let key = manager.open_key_read(ctx, &key)?;
    let root = match key.get_value()? {
        Some(k) => k,
        None if is_legacy => {
            return Ok(RedisValue::Null);
        }
        None => {
            return Err(RedisError::nonexistent_key());
        }
    };
    let values = find_all_values(path.get_path(), root, |v| {
        v.get_type() == SelectValueType::Array
    })?;
    if is_legacy && values.is_empty() {
        return Err(RedisError::String(
            err_msg_json_path_doesnt_exist_with_param(path.get_original()),
        ));
    }
    let mut res = vec![];
    for v in values {
        let cur_val: RedisValue = match v {
            Some(v) => (v.len().unwrap() as i64).into(),
            _ => {
                if is_legacy {
                    return Err(RedisError::String(
                        err_msg_json_path_doesnt_exist_with_param_or(
                            path.get_original(),
                            "not an array",
                        ),
                    ));
                }
                RedisValue::Null
            }
        };
        if is_legacy {
            return Ok(cur_val);
        }
        res.push(cur_val);
    }
    Ok(res.into())
}

///
/// JSON.ARRPOP <key>
///         [FORMAT {STRINGS|EXPAND1|EXPAND}]   /* default is STRINGS */
///         [path [index]]
///

#[macro_export]
macro_rules! json_arrpop_command {
    ($item:item) => {
        #[::redis_module_macros::command(
            {
                name: "json.arrpop",
                flags: [Write],
                acl_categories: [Write, Single("json")],
                arity: -2,
                complexity: "O(N) when path is evaluated to a single value where N is the size of the array and the specified index is not the last element, O(1) when path is evaluated to a single value and the specified index is the last element, or O(N) when path is evaluated to multiple values, where N is the size of the key",
                since: "1.0.0",
                summary: "Remove and return the element at the specified index in the array at path",
                key_spec: [
                    {
                        flags: [ReadWrite],
                        begin_search: Index({ index: 1 }),
                        find_keys: Range({ last_key: 0, steps: 1, limit: 0 }),
                    }
                ],
                args: [
                    {
                        name: "key",
                        arg_type: Key,
                        key_spec_index: 0,
                    },
                    {
                        name: "format",
                        token: "FORMAT",
                        arg_type: Block,
                        flags: [Optional],
                        subargs: [
                            {
                                name: "format-token",
                                arg_type: OneOf,
                                subargs: [
                                    {
                                        name: "STRINGS",
                                        arg_type: PureToken,
                                        token: "STRINGS",
                                    },
                                    {
                                        name: "EXPAND1",
                                        arg_type: PureToken,
                                        token: "EXPAND1",
                                    },
                                    {
                                        name: "EXPAND",
                                        arg_type: PureToken,
                                        token: "EXPAND",
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        name: "path_index",
                        arg_type: Block,
                        flags: [Optional],
                        subargs: [
                            {
                                name: "path",
                                arg_type: String,
                            },
                            {
                                name: "index",
                                arg_type: Integer,
                                flags: [Optional],
                            }
                        ]
                    }
                ]
            }
        )]
        $item
    };
}

pub fn json_arr_pop_command_impl<M: Manager>(
    manager: M,
    ctx: &Context,
    args: Vec<RedisString>,
) -> RedisResult {
    let mut args = args.into_iter().skip(1);

    let key = args.next_arg()?;

    let is_resp3 = is_resp3(ctx);
    let mut format_options = ReplyFormatOptions::new(is_resp3, ReplyFormat::STRINGS);

    let path = if let Some(arg) = args.next() {
        if arg.try_as_str()?.eq_ignore_ascii_case(CMD_ARG_FORMAT) {
            if let Ok(next) = args.next_str() {
                format_options.format = ReplyFormat::from_str(next)?;
                if format_options.format == ReplyFormat::STRING {
                    // ARRPOP FORMAT STRING is not supported
                    return Err(RedisError::Str("ERR wrong reply format"));
                }
                if !format_options.resp3 {
                    return Err(RedisError::Str(
                        "ERR FORMAT argument is not supported on RESP2",
                    ));
                }
                args.next()
            } else {
                // If only the FORMAT subcommand is provided, then it's the path
                Some(arg)
            }
        } else {
            // if it's not FORMAT, then it's the path
            Some(arg)
        }
    } else {
        None
    };

    // Try to retrieve the optional arguments [path [index]]
    let (path, index) = match path {
        None => (Path::default(), i64::MAX),
        Some(s) => {
            let path = Path::new(s.try_as_str()?);
            let index = args.next_i64().unwrap_or(-1);
            (path, index)
        }
    };
    //args.done()?;

    let mut redis_key = manager.open_key_write(ctx, key)?;
    if path.is_legacy() {
        if format_options.format != ReplyFormat::STRINGS {
            return Err(RedisError::Str(
                "Legacy paths are supported only with FORMAT STRINGS",
            ));
        }

        json_arr_pop_legacy::<M>(manager, &mut redis_key, ctx, path.get_path(), index)
    } else {
        json_arr_pop_impl::<M>(
            manager,
            &mut redis_key,
            ctx,
            path.get_path(),
            index,
            &format_options,
        )
    }
}

fn json_arr_pop_impl<M>(
    manager: M,
    redis_key: &mut M::WriteHolder,
    ctx: &Context,
    path: &str,
    index: i64,
    format_options: &ReplyFormatOptions,
) -> RedisResult
where
    M: Manager,
{
    let root = redis_key
        .get_value()?
        .ok_or_else(RedisError::nonexistent_key)?;

    let paths = find_all_paths(path, root, |v| v.get_type() == SelectValueType::Array)?;
    let mut res: Vec<RedisValue> = vec![];
    let mut need_notify = false;
    for p in paths {
        res.push(match p {
            Some(p) => redis_key.arr_pop(p, index, |v| {
                v.map_or(Ok(RedisValue::Null), |v| {
                    need_notify = true;
                    if format_options.is_resp3_reply() {
                        Ok(KeyValue::value_to_resp3(v, format_options))
                    } else {
                        Ok(serde_json::to_string(&v)?.into())
                    }
                })
            })?,
            _ => RedisValue::Null, // Not an array
        });
    }
    if need_notify {
        redis_key.notify_keyspace_event(ctx, "json.arrpop")?;
        manager.apply_changes(ctx);
    }
    Ok(res.into())
}

fn json_arr_pop_legacy<M>(
    manager: M,
    redis_key: &mut M::WriteHolder,
    ctx: &Context,
    path: &str,
    index: i64,
) -> RedisResult
where
    M: Manager,
{
    let root = redis_key
        .get_value()?
        .ok_or_else(RedisError::nonexistent_key)?;

    let paths = find_paths(path, root, |v| v.get_type() == SelectValueType::Array)?;
    if !paths.is_empty() {
        let mut res = Ok(().into());
        for p in paths {
            res = Ok(redis_key.arr_pop(p, index, |v| match v {
                Some(r) => Ok(serde_json::to_string(&r)?.into()),
                None => Ok(().into()),
            })?);
        }
        redis_key.notify_keyspace_event(ctx, "json.arrpop")?;
        manager.apply_changes(ctx);
        res
    } else {
        Err(RedisError::String(
            err_msg_json_path_doesnt_exist_with_param_or(path, "not an array"),
        ))
    }
}

///
/// JSON.ARRTRIM <key> <path> <start> <stop>
///
#[macro_export]
macro_rules! json_arrtrim_command {
    ($item:item) => {
        #[::redis_module_macros::command(
            {
                name: "json.arrtrim",
                flags: [Write],
                acl_categories: [Write, Single("json")],
                arity: 5,
                complexity: "O(N) when path is evaluated to a single value where N is the size of the array, O(N) when path is evaluated to multiple values, where N is the size of the key",
                since: "1.0.0",
                summary: "Trim an array so that it contains only the specified inclusive range of elements",
                key_spec: [
                    {
                        flags: [ReadWrite],
                        begin_search: Index({ index: 1 }),
                        find_keys: Range({ last_key: 0, steps: 1, limit: 0 }),
                    }
                ],
                args: [
                    {
                        name: "key",
                        arg_type: Key,
                        key_spec_index: 0,
                    },
                    {
                        name: "path",
                        arg_type: String,
                    },
                    {
                        name: "start",
                        arg_type: Integer,
                    },
                    {
                        name: "stop",
                        arg_type: Integer,
                    }
                ]
            }
        )]
        $item
    };
}
pub fn json_arr_trim_command_impl<M: Manager>(
    manager: M,
    ctx: &Context,
    args: Vec<RedisString>,
) -> RedisResult {
    let mut args = args.into_iter().skip(1);

    let key = args.next_arg()?;
    let path = Path::new(args.next_str()?);
    let start = args.next_i64()?;
    let stop = args.next_i64()?;

    let mut redis_key = manager.open_key_write(ctx, key)?;

    if path.is_legacy() {
        json_arr_trim_legacy::<M>(manager, &mut redis_key, ctx, path.get_path(), start, stop)
    } else {
        json_arr_trim_impl::<M>(manager, &mut redis_key, ctx, path.get_path(), start, stop)
    }
}
fn json_arr_trim_impl<M>(
    manager: M,
    redis_key: &mut M::WriteHolder,
    ctx: &Context,
    path: &str,
    start: i64,
    stop: i64,
) -> RedisResult
where
    M: Manager,
{
    let root = redis_key
        .get_value()?
        .ok_or_else(RedisError::nonexistent_key)?;

    let paths = find_all_paths(path, root, |v| v.get_type() == SelectValueType::Array)?;
    let mut res: Vec<RedisValue> = vec![];
    let mut need_notify = false;
    for p in paths {
        res.push(match p {
            Some(p) => {
                need_notify = true;
                (redis_key.arr_trim(p, start, stop)?).into()
            }
            _ => RedisValue::Null,
        });
    }
    if need_notify {
        redis_key.notify_keyspace_event(ctx, "json.arrtrim")?;
        manager.apply_changes(ctx);
    }
    Ok(res.into())
}

fn json_arr_trim_legacy<M>(
    manager: M,
    redis_key: &mut M::WriteHolder,
    ctx: &Context,
    path: &str,
    start: i64,
    stop: i64,
) -> RedisResult
where
    M: Manager,
{
    let root = redis_key
        .get_value()?
        .ok_or_else(RedisError::nonexistent_key)?;

    let paths = find_paths(path, root, |v| v.get_type() == SelectValueType::Array)?;
    if paths.is_empty() {
        Err(RedisError::String(
            err_msg_json_path_doesnt_exist_with_param_or(path, "not an array"),
        ))
    } else {
        let mut res = None;
        for p in paths {
            res = Some(redis_key.arr_trim(p, start, stop)?);
        }
        redis_key.notify_keyspace_event(ctx, "json.arrtrim")?;
        manager.apply_changes(ctx);
        Ok(res.unwrap().into())
    }
}

///
/// JSON.OBJKEYS <key> [path]
///
#[macro_export]
macro_rules! json_objkeys_command {
    ($item:item) => {
        #[::redis_module_macros::command(
            {
                name: "json.objkeys",
                flags: [ReadOnly],
                acl_categories: [Read, Single("json")],
                arity: -2,
                complexity: "O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values, where N is the size of the key",
                since: "1.0.0",
                summary: "Return the keys in the object that's referenced by path",
                key_spec: [
                    {
                        flags: [ReadOnly],
                        begin_search: Index({ index: 1 }),
                        find_keys: Range({ last_key: 0, steps: 1, limit: 0 }),
                    }
                ],
                args: [
                    {
                        name: "key",
                        arg_type: Key,
                        key_spec_index: 0,
                    },
                    {
                        name: "path",
                        arg_type: String,
                        flags: [Optional],
                    }
                ]
            }
        )]
        $item
    };
}
pub fn json_obj_keys_command_impl<M: Manager>(
    manager: M,
    ctx: &Context,
    args: Vec<RedisString>,
) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;
    let path = args.next_str().map(Path::new).unwrap_or_default();

    let mut key = manager.open_key_read(ctx, &key)?;
    if path.is_legacy() {
        json_obj_keys_legacy::<M>(&mut key, path.get_path())
    } else {
        json_obj_keys_impl::<M>(&mut key, path.get_path())
    }
}

fn json_obj_keys_impl<M>(redis_key: &mut M::ReadHolder, path: &str) -> RedisResult
where
    M: Manager,
{
    let root = redis_key
        .get_value()?
        .ok_or_else(RedisError::nonexistent_key)?;
    let res: RedisValue = {
        let values = find_all_values(path, root, |v| v.get_type() == SelectValueType::Object)?;
        let mut res: Vec<RedisValue> = vec![];
        for v in values {
            res.push(v.map_or(RedisValue::Null, |v| v.keys().unwrap().collect_vec().into()));
        }
        res.into()
    };
    Ok(res)
}

fn json_obj_keys_legacy<M>(redis_key: &mut M::ReadHolder, path: &str) -> RedisResult
where
    M: Manager,
{
    let root = match redis_key.get_value()? {
        Some(v) => v,
        _ => return Ok(RedisValue::Null),
    };
    let value = match KeyValue::new(root).get_first(path) {
        Ok(v) => match v.get_type() {
            SelectValueType::Object => v.keys().unwrap().collect_vec().into(),
            _ => {
                return Err(RedisError::String(
                    err_msg_json_path_doesnt_exist_with_param_or(path, "not an object"),
                ))
            }
        },
        _ => RedisValue::Null,
    };
    Ok(value)
}

///
/// JSON.OBJLEN <key> [path]
///
#[macro_export]
macro_rules! json_objlen_command {
    ($item:item) => {
        #[::redis_module_macros::command(
            {
                name: "json.objlen",
                flags: [ReadOnly],
                acl_categories: [Read, Single("json")],
                arity: -2,
                complexity: "O(1) when path is evaluated to a single value, O(N) when path is evaluated to multiple values, where N is the size of the key",
                since: "1.0.0",
                summary: "Report the number of keys in the JSON object at path in key",
                key_spec: [
                    {
                        flags: [ReadOnly],
                        begin_search: Index({ index: 1 }),
                        find_keys: Range({ last_key: 0, steps: 1, limit: 0 }),
                    }
                ],
                args: [
                    {
                        name: "key",
                        arg_type: Key,
                        key_spec_index: 0,
                    },
                    {
                        name: "path",
                        arg_type: String,
                        flags: [Optional],
                    }
                ]
            }
        )]
        $item
    };
}

pub fn json_obj_len_command_impl<M: Manager>(
    manager: M,
    ctx: &Context,
    args: Vec<RedisString>,
) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;

    let path = args.next_str().map(Path::new).unwrap_or_default();

    let key = manager.open_key_read(ctx, &key)?;
    if path.is_legacy() {
        json_obj_len_legacy::<M>(&key, path.get_path())
    } else {
        json_obj_len_impl::<M>(&key, path.get_path())
    }
}

fn json_obj_len_impl<M>(redis_key: &M::ReadHolder, path: &str) -> RedisResult
where
    M: Manager,
{
    let root = redis_key.get_value()?;
    let res = match root {
        Some(root) => find_all_values(path, root, |v| v.get_type() == SelectValueType::Object)?
            .iter()
            .map(|v| {
                v.as_ref().map_or(RedisValue::Null, |v| {
                    RedisValue::Integer(v.len().unwrap() as i64)
                })
            })
            .collect_vec()
            .into(),
        None => {
            return Err(RedisError::String(
                err_msg_json_path_doesnt_exist_with_param_or(path, "not an object"),
            ))
        }
    };
    Ok(res)
}

fn json_obj_len_legacy<M>(redis_key: &M::ReadHolder, path: &str) -> RedisResult
where
    M: Manager,
{
    match redis_key.get_value()? {
        Some(doc) => match KeyValue::new(doc).obj_len(path)? {
            ObjectLen::Len(l) => Ok(RedisValue::Integer(l as i64)),
            _ => Ok(RedisValue::Null),
        },
        None => Ok(RedisValue::Null),
    }
}

///
/// JSON.CLEAR <key> [path ...]
///
#[macro_export]
macro_rules! json_clear_command {
    ($item:item) => {
        #[::redis_module_macros::command(
            {
                name: "json.clear",
                flags: [Write],
                acl_categories: [Write, Single("json")],
                arity: -2,
                complexity: "O(N) when path is evaluated to a single value where N is the size of the values, O(N) when path is evaluated to multiple values, where N is the size of the key",
                since: "2.0.0",
                summary: "Clear container values (arrays/objects) and set numeric values to 0",
                key_spec: [
                    {
                        flags: [ReadWrite],
                        begin_search: Index({ index: 1 }),
                        find_keys: Range({ last_key: 0, steps: 1, limit: 0 }),
                    }
                ],
                args: [
                    {
                        name: "key",
                        arg_type: Key,
                        key_spec_index: 0,
                    },
                    {
                        name: "path",
                        arg_type: String,
                        flags: [Optional, Multiple],
                    }
                ]
            }
        )]
        $item
    };
}

pub fn json_clear_command_impl<M: Manager>(
    manager: M,
    ctx: &Context,
    args: Vec<RedisString>,
) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    let key = args.next_arg()?;
    let paths = args.try_fold::<_, _, Result<Vec<Path>, RedisError>>(
        Vec::with_capacity(args.len()),
        |mut acc, arg| {
            let s = arg.try_as_str()?;
            acc.push(Path::new(s));
            Ok(acc)
        },
    )?;

    let paths = if paths.is_empty() {
        vec![Path::default()]
    } else {
        paths
    };

    let path = paths.first().unwrap().get_path();

    let mut redis_key = manager.open_key_write(ctx, key)?;

    let root = redis_key
        .get_value()?
        .ok_or_else(RedisError::nonexistent_key)?;

    let paths = find_paths(path, root, |v| match v.get_type() {
        SelectValueType::Array | SelectValueType::Object => v.len().unwrap() > 0,
        SelectValueType::Long => v.get_long() != 0,
        SelectValueType::Double => v.get_double() != 0.0,
        _ => false,
    })?;
    let cleared = paths
        .into_iter()
        .try_fold(0, |acc, p| redis_key.clear(p).map(|v| acc + v))?;
    if cleared > 0 {
        redis_key.notify_keyspace_event(ctx, "json.clear")?;
        manager.apply_changes(ctx);
    }
    Ok(cleared.into())
}

///
/// JSON.DEBUG <subcommand & arguments>
///
/// subcommands:
/// MEMORY <key> [path]
/// HELP
///
#[macro_export]
macro_rules! json_debug_command {
    ($item:item) => {
        #[::redis_module_macros::command(
                            {
                                name: "json.debug",
                                flags: [ReadOnly],
                                acl_categories: [Read, Single("json")],
                                arity: -2,
                                complexity: "N/A",
                                since: "1.0.0",
                                summary: "This is a container command for debugging related tasks",
                                key_spec: [
                                ],
                            }
                        )]
        $item
    };
}

pub fn json_debug_command_impl<M: Manager>(
    manager: M,
    ctx: &Context,
    args: Vec<RedisString>,
) -> RedisResult {
    let mut args = args.into_iter().skip(1);
    match args.next_str()?.to_uppercase().as_str() {
        "MEMORY" => {
            let key = args.next_arg()?;
            let path = args.next_str().map(Path::new).unwrap_or_default();

            let key = manager.open_key_read(ctx, &key)?;
            if path.is_legacy() {
                Ok(match key.get_value()? {
                    Some(doc) => {
                        M::get_memory(KeyValue::new(doc).get_first(path.get_path())?.as_ref())?
                    }
                    None => 0,
                }
                .into())
            } else {
                Ok(match key.get_value()? {
                    Some(doc) => KeyValue::new(doc)
                        .get_values(path.get_path())?
                        .into_iter()
                        .map(|v| M::get_memory(v.as_ref()))
                        .try_collect()?,
                    None => vec![],
                }
                .into())
            }
        }
        "DEFRAG_INFO" => defrag_info(ctx),
        "HELP" => {
            let results = vec![
                "MEMORY <key> [path] - reports memory usage",
                "HELP                - this message",
            ];
            Ok(results.into())
        }
        _ => Err(RedisError::Str(
            "ERR unknown subcommand - try `JSON.DEBUG HELP`",
        )),
    }
}

///
/// JSON.RESP <key> [path]
///
#[macro_export]
macro_rules! json_resp_command {
    ($item:item) => {
        #[::redis_module_macros::command(
            {
                name: "json.resp",
                flags: [ReadOnly],
                acl_categories: [Read, Single("json")],
                arity: -2,
                complexity: "O(N) when path is evaluated to a single value, where N is the size of the value, O(N) when path is evaluated to multiple values, where N is the size of the key",
                since: "1.0.0",
                summary: "Return the JSON in key in Redis serialization protocol specification form",
                key_spec: [
                    {
                        flags: [ReadOnly],
                        begin_search: Index({ index: 1 }),
                        find_keys: Range({ last_key: 0, steps: 1, limit: 0 }),
                    }
                ],
                args: [
                    {
                        name: "key",
                        arg_type: Key,
                        key_spec_index: 0,
                    },
                    {
                        name: "path",
                        arg_type: String,
                        flags: [Optional],
                    }
                ]
            }
        )]
        $item
    };
}

pub fn json_resp_command_impl<M: Manager>(
    manager: M,
    ctx: &Context,
    args: Vec<RedisString>,
) -> RedisResult {
    let mut args = args.into_iter().skip(1);

    let key = args.next_arg()?;
    let path = match args.next() {
        None => Path::default(),
        Some(s) => Path::new(s.try_as_str()?),
    };

    let key = manager.open_key_read(ctx, &key)?;
    key.get_value()?.map_or(Ok(RedisValue::Null), |doc| {
        KeyValue::new(doc).resp_serialize(path)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prepare_paths_for_updating_with_numeric_pathes() {
        let mut pathes = vec![
            vec!["0".to_string()],
            vec!["1".to_string()],
            vec!["2".to_string()],
            vec!["3".to_string()],
            vec!["4".to_string()],
            vec!["5".to_string()],
            vec!["6".to_string()],
            vec!["7".to_string()],
            vec!["8".to_string()],
            vec!["9".to_string()],
            vec!["10".to_string()],
            vec!["20".to_string()],
            vec!["30".to_string()],
            vec!["40".to_string()],
            vec!["50".to_string()],
            vec!["60".to_string()],
            vec!["100".to_string()],
        ];
        let pathes_expected = pathes.clone().into_iter().rev().collect::<Vec<_>>();
        prepare_paths_for_updating(&mut pathes);
        assert_eq!(pathes, pathes_expected);
    }
}
