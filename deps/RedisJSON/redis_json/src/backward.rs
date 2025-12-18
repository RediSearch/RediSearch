/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

use std::vec::Vec;

use redis_module::raw;
use serde_json::map::Map;
use serde_json::Number;
use serde_json::Value;

use crate::error::Error;

#[derive(Debug, PartialEq)]
enum NodeType {
    Null,
    // used in masks and consistent type checking
    String,
    Number,
    Integer,
    Boolean,
    Dict,
    Array,
    KeyVal,
    // N_DATETIME = 0x100
    // N_BINARY = 0x200
}

impl From<u64> for NodeType {
    fn from(n: u64) -> Self {
        match n {
            0x1u64 => Self::Null,
            0x2u64 => Self::String,
            0x4u64 => Self::Number,
            0x8u64 => Self::Integer,
            0x10u64 => Self::Boolean,
            0x20u64 => Self::Dict,
            0x40u64 => Self::Array,
            0x80u64 => Self::KeyVal,
            _ => panic!("Can't load old RedisJSON RDB1"),
        }
    }
}

pub fn json_rdb_load(rdb: *mut raw::RedisModuleIO) -> Result<Value, Error> {
    let node_type = raw::load_unsigned(rdb)?.into();
    match node_type {
        NodeType::Null => Ok(Value::Null),
        NodeType::Boolean => {
            let buffer = raw::load_string_buffer(rdb)?;
            Ok(Value::Bool(buffer.as_ref()[0] == b'1'))
        }
        NodeType::Integer => {
            let n = raw::load_signed(rdb)?;
            Ok(Value::Number(n.into()))
        }
        NodeType::Number => {
            let n = raw::load_double(rdb)?;
            Ok(Value::Number(
                Number::from_f64(n).ok_or_else(|| Error::from("Can't load as float"))?,
            ))
        }
        NodeType::String => {
            let buffer = raw::load_string_buffer(rdb)?;
            Ok(Value::String(buffer.to_string()?))
        }
        NodeType::Dict => {
            let len = raw::load_unsigned(rdb)?;
            let mut m = Map::with_capacity(len as usize);
            for _ in 0..len {
                let t: NodeType = raw::load_unsigned(rdb)?.into();
                if t != NodeType::KeyVal {
                    return Err(Error::from("Can't load old RedisJSON RDB"));
                }
                let buffer = raw::load_string_buffer(rdb)?;
                m.insert(buffer.to_string()?, json_rdb_load(rdb)?);
            }
            Ok(Value::Object(m))
        }
        NodeType::Array => {
            let len = raw::load_unsigned(rdb)?;
            let mut v = Vec::with_capacity(len as usize);
            for _ in 0..len {
                let nested = json_rdb_load(rdb)?;
                v.push(nested);
            }
            Ok(Value::Array(v))
        }
        NodeType::KeyVal => Err(Error::from("Can't load old RedisJSON RDB")),
    }
}
