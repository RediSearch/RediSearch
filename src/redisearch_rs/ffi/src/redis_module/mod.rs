/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use super::*;

mod ftor_wrap;
mod resources;

pub mod prelude {
    use super::*;

    pub use resources::{RedisResource, RedisResourceError, WithError};

    // Re-export the Redis Key API
    pub use ftor_wrap::key_type;
    pub use resources::RedisKey;

    // Re-export the Redis Scan API
    pub use ftor_wrap::scan_key;
    pub use resources::RedisScanCursor;

    // Re-export the Redis String API
    pub use ftor_wrap::string_ptr_len;
    pub use ftor_wrap::string_to_double;
    pub use ftor_wrap::string_to_longlong;
    pub use resources::RedisString;

    // Re-export the Redis Call API
    pub use ftor_wrap::call_reply_array_element;
    pub use ftor_wrap::call_reply_length;
    pub use ftor_wrap::call_reply_string_ptr;
    pub use ftor_wrap::call_reply_type;
    pub use resources::RedisCallReply_Hgetall;
}

#[derive(Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum Status {
    Ok = REDISMODULE_OK,
    Err = REDISMODULE_ERR,
}

impl From<u32> for Status {
    fn from(value: u32) -> Self {
        match value {
            REDISMODULE_OK => Status::Ok,
            REDISMODULE_ERR => Status::Err,
            _ => panic!("Unknown status code: {}", value),
        }
    }
}

impl From<i32> for Status {
    fn from(value: i32) -> Self {
        match value as u32 {
            REDISMODULE_OK => Status::Ok,
            REDISMODULE_ERR => Status::Err,
            _ => panic!("Unknown status code: {}", value),
        }
    }
}
