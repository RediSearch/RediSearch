/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

use json_path::json_path::QueryCompilationError;
use redis_module::RedisError;
use std::num::ParseIntError;

#[derive(Debug)]
pub struct Error {
    pub msg: String,
}

impl From<String> for Error {
    fn from(e: String) -> Self {
        Self { msg: e }
    }
}

impl From<QueryCompilationError> for Error {
    fn from(e: QueryCompilationError) -> Self {
        Self { msg: e.to_string() }
    }
}

impl From<redis_module::error::GenericError> for Error {
    fn from(err: redis_module::error::GenericError) -> Self {
        err.to_string().into()
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(err: std::string::FromUtf8Error) -> Self {
        err.to_string().into()
    }
}

impl From<ParseIntError> for Error {
    fn from(err: ParseIntError) -> Self {
        err.to_string().into()
    }
}

impl From<redis_module::error::Error> for Error {
    fn from(e: redis_module::error::Error) -> Self {
        match e {
            redis_module::error::Error::Generic(err) => err.into(),
            redis_module::error::Error::FromUtf8(err) => err.into(),
            redis_module::error::Error::ParseInt(err) => err.into(),
        }
    }
}

impl From<RedisError> for Error {
    fn from(e: RedisError) -> Self {
        Self::from(format!("ERR {e}"))
    }
}

impl From<&str> for Error {
    fn from(e: &str) -> Self {
        Self { msg: e.to_string() }
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self { msg: e.to_string() }
    }
}

impl From<Error> for redis_module::RedisError {
    fn from(e: Error) -> Self {
        Self::String(e.msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Result, Value};

    #[test]
    fn test_from_string() {
        let err: Error = String::from("error from String").into();
        assert_eq!(err.msg, "error from String");
    }

    #[test]
    fn test_from_generic_error() {
        let err: Error = redis_module::error::GenericError::new("error from GenericError").into();
        assert_eq!(err.msg, "Store error: error from GenericError");
    }

    #[test]
    fn test_from_utf8_error() {
        let err: Error = String::from_utf8(vec![128, 0, 0]).unwrap_err().into();
        assert_eq!(err.msg, "invalid utf-8 sequence of 1 bytes from index 0");
    }

    #[test]
    fn test_from_parse_int_error() {
        let err: Error = "a12".parse::<i32>().unwrap_err().into();
        assert_eq!(err.msg, "invalid digit found in string");
    }

    #[test]
    fn test_from_redis_error() {
        let err: Error = RedisError::short_read().into();
        assert_eq!(err.msg, "ERR ERR short read or OOM loading DB");
    }

    #[test]
    fn test_from_error() {
        let err: Error = redis_module::error::Error::generic("error from Generic").into();
        assert_eq!(err.msg, "Store error: error from Generic");

        let utf8_err: redis_module::error::Error =
            String::from_utf8(vec![128, 0, 0]).unwrap_err().into();
        let err: Error = utf8_err.into();
        assert_eq!(err.msg, "invalid utf-8 sequence of 1 bytes from index 0");

        let parse_int_error: redis_module::error::Error = "a12".parse::<i32>().unwrap_err().into();
        let err: Error = parse_int_error.into();
        assert_eq!(err.msg, "invalid digit found in string");
    }

    #[test]
    fn test_from_serde_json_error() {
        let res: Result<Value> = serde_json::from_str("{");
        let err: Error = res.unwrap_err().into();
        assert_eq!(err.msg, "EOF while parsing an object at line 1 column 1");
    }

    #[test]
    fn test_from_str() {
        let err: Error = "error from str".into();
        assert_eq!(err.msg, "error from str");
    }

    #[test]
    fn test_to_redis_error() {
        let err: redis_module::RedisError = Error {
            msg: "to RedisError".to_string(),
        }
        .into();
        assert_eq!(err.to_string(), "to RedisError".to_string());
    }
}
